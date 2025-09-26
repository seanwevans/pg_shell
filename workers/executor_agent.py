#!/usr/bin/env python3
"""pg_shell executor agent.

Polls the ``commands`` table for pending commands and executes them. This
implementation uses ``psycopg2`` and ``subprocess``. It also listens for the
``new_command`` channel so commands can run in near real time.

Set ``DATABASE_URL`` or ``PG_CONN`` to the PostgreSQL DSN before running this
script.
"""

import json
import logging
import os
import select
import shlex
import subprocess
import time
from typing import Any, Dict

from psycopg2 import sql, errors
from psycopg2.extras import RealDictCursor

from workers.db import get_conn

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1"))
DEFAULT_LISTEN_CHANNEL = "new_command"
LISTEN_CHANNEL_ENV = os.getenv("LISTEN_CHANNEL")
LISTEN_CHANNEL = LISTEN_CHANNEL_ENV or DEFAULT_LISTEN_CHANNEL
COMMAND_TIMEOUT = int(os.getenv("COMMAND_TIMEOUT", "30"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_OUTPUT_BYTES = int(os.getenv("MAX_OUTPUT_BYTES", "65536"))
TRUNCATION_SUFFIX = "...[truncated]"


def _update_channel_config(conn, channel: str) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pg_shell_config(key, value)
                VALUES ('listen_channel', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (channel,),
            )
    except errors.UndefinedTable:
        if not conn.autocommit:
            conn.rollback()
        logging.warning(
            "pg_shell_config table missing when updating listen channel; using %s",
            channel,
        )


def _fetch_channel_from_config(conn) -> str:
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value FROM pg_shell_config WHERE key = %s",
                ("listen_channel",),
            )
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
    except errors.UndefinedTable:
        if not conn.autocommit:
            conn.rollback()
        logging.warning(
            "pg_shell_config table missing when fetching listen channel; using default",
        )
    return DEFAULT_LISTEN_CHANNEL


def resolve_listen_channel(conn) -> str:
    global LISTEN_CHANNEL
    if LISTEN_CHANNEL_ENV:
        channel = LISTEN_CHANNEL_ENV
        _update_channel_config(conn, channel)
    else:
        channel = _fetch_channel_from_config(conn)
    LISTEN_CHANNEL = channel
    return channel


def setup_listener(conn):
    channel = resolve_listen_channel(conn)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
    conn.commit()


def wait_for_notify(conn, timeout: float) -> None:
    if select.select([conn], [], [], timeout) != ([], [], []):
        conn.poll()
        conn.notifies.clear()


def fetch_pending(conn) -> Dict[str, Any] | None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("BEGIN;")
        cur.execute(
            """
            SELECT id, user_id, command, cwd_snapshot, env_snapshot
            FROM commands
            WHERE status = 'pending'
            ORDER BY submitted_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if row:
            cur.execute("UPDATE commands SET status='running' WHERE id=%s", (row["id"],))
            logging.info("Fetched command %s for user %s", row["id"], row["user_id"])
        conn.commit()
        return row


def update_command(conn, cmd_id: int, status: str, output: str, exit_code: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE commands SET status=%s, output=%s, exit_code=%s, completed_at=now() WHERE id=%s",
            (status, output, exit_code, cmd_id),
        )
    conn.commit()


def update_cwd(conn, user_id, cwd: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE environments SET cwd=%s, updated_at=now() WHERE user_id=%s",
            (cwd, user_id),
        )
    conn.commit()


def run_subprocess(command: str, cwd: str, env_snapshot: Any) -> tuple[int, str]:
    env: Dict[str, str] = os.environ.copy()
    if env_snapshot:
        if isinstance(env_snapshot, str):
            env.update(json.loads(env_snapshot))
        else:
            env.update(env_snapshot)

    cmd_list = shlex.split(command)
    proc = subprocess.Popen(
        cmd_list,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    deadline = time.time() + COMMAND_TIMEOUT
    output = bytearray()
    limit_exceeded = False
    timed_out = False
    fds = [proc.stdout, proc.stderr]

    while fds:
        if time.time() > deadline and not timed_out:
            proc.kill()
            timed_out = True
        ready, _, _ = select.select(fds, [], [], 0.1)
        for fd in ready:
            chunk = fd.read1(4096)
            if not chunk:
                fds.remove(fd)
                continue
            if len(output) < MAX_OUTPUT_BYTES:
                remaining = MAX_OUTPUT_BYTES - len(output)
                if len(chunk) > remaining:
                    output.extend(chunk[:remaining])
                    limit_exceeded = True
                else:
                    output.extend(chunk)
            else:
                limit_exceeded = True

    proc.wait()
    exit_code = proc.returncode
    text = output.decode(errors="replace")
    if timed_out:
        exit_code = 124
        text = f"Timed out after {COMMAND_TIMEOUT}s\n" + text
    if limit_exceeded:
        text += TRUNCATION_SUFFIX
    return exit_code, text


def handle_command(conn, row: Dict[str, Any]) -> None:
    command = row["command"].strip()
    logging.info(
        "Executing command %s for user %s: %s",
        row["id"],
        row["user_id"],
        command,
    )
    try:
        tokens = shlex.split(command)
    except ValueError as exc:
        logging.error(
            "Command %s for user %s failed: %s",
            row["id"],
            row["user_id"],
            exc,
        )
        update_command(conn, row["id"], "failed", str(exc), 1)
        return

    if len(tokens) == 2 and tokens[0] == "cd":
        path = tokens[1]
        if os.path.isabs(path):
            new_cwd = os.path.normpath(path)
        else:
            new_cwd = os.path.normpath(os.path.join(row["cwd_snapshot"], path))
        if not os.path.isdir(new_cwd):
            error = f"cd: {path}: No such file or directory"
            update_command(conn, row["id"], "failed", error, 1)
            logging.error(
                "Command %s for user %s failed: directory %s not found",
                row["id"],
                row["user_id"],
                path,
            )
            return
        update_cwd(conn, row["user_id"], new_cwd)
        update_command(conn, row["id"], "done", "", 0)
        logging.info("Command %s for user %s completed", row["id"], row["user_id"])
        return

    try:
        exit_code, output = run_subprocess(
            command, row["cwd_snapshot"], row["env_snapshot"]
        )
    except Exception as exc:
        logging.exception(
            "Command %s for user %s failed: %s",
            row["id"],
            row["user_id"],
            exc,
        )
        update_command(conn, row["id"], "failed", str(exc), 1)
        return

    status = "done" if exit_code == 0 else "failed"
    update_command(conn, row["id"], status, output, exit_code)
    if status == "done":
        logging.info("Command %s for user %s completed", row["id"], row["user_id"])
    else:
        logging.error(
            "Command %s for user %s failed with exit code %s",
            row["id"],
            row["user_id"],
            exit_code,
        )


def main() -> None:
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(message)s")

    conn = get_conn()
    try:
        setup_listener(conn)
        while True:
            row = fetch_pending(conn)
            if row:
                handle_command(conn, row)
                continue
            wait_for_notify(conn, POLL_INTERVAL)
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
