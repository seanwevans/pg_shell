#!/usr/bin/env python3
"""pg_shell executor agent.

Polls the ``commands`` table for pending commands and executes them. This
implementation uses ``psycopg2`` and ``subprocess``. It also listens for the
``new_command`` channel so commands can run in near real time.

Set ``DATABASE_URL`` or ``PG_CONN`` to the PostgreSQL DSN before running this
script.
"""

import json
import os
import select
import shlex
import subprocess
import sys
import time
from typing import Any, Dict
import logging

import psycopg2
from psycopg2.extras import RealDictCursor

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1"))
LISTEN_CHANNEL = os.getenv("LISTEN_CHANNEL", "new_command")
COMMAND_TIMEOUT = int(os.getenv("COMMAND_TIMEOUT", "30"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


def get_conn():
    dsn = os.environ.get("DATABASE_URL") or os.environ.get("PG_CONN")
    if not dsn:
        logging.error("DATABASE_URL or PG_CONN environment variable required")
        sys.exit(1)
    return psycopg2.connect(dsn)


def setup_listener(conn):
    with conn.cursor() as cur:
        cur.execute("LISTEN %s;" % LISTEN_CHANNEL)
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
    try:
        proc = subprocess.run(
            cmd_list,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT,
        )
        output = (proc.stdout or "") + (proc.stderr or "")
        return proc.returncode, output
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        output = stdout + stderr
        return 124, f"Timed out after {COMMAND_TIMEOUT}s\n" + output


def handle_command(conn, row: Dict[str, Any]) -> None:
    command = row["command"].strip()
    logging.info(
        "Executing command %s for user %s: %s",
        row["id"],
        row["user_id"],
        command,
    )

    tokens = shlex.split(command)
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
    setup_listener(conn)
    while True:
        row = fetch_pending(conn)
        if row:
            handle_command(conn, row)
            continue
        wait_for_notify(conn, POLL_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
