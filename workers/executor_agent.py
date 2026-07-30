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
import signal
import socket
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
CLAIM_LEASE_SECONDS = int(os.getenv("CLAIM_LEASE_SECONDS", str(COMMAND_TIMEOUT + 10)))
MAX_COMMAND_ATTEMPTS = int(os.getenv("MAX_COMMAND_ATTEMPTS", "3"))
WORKER_ID = os.getenv("EXECUTOR_WORKER_ID") or f"{socket.gethostname()}:{os.getpid()}"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_OUTPUT_BYTES = int(os.getenv("MAX_OUTPUT_BYTES", "65536"))
TRUNCATION_SUFFIX = "...[truncated]"
TERMINATION_GRACE_SECONDS = 0.5
PIPE_DRAIN_TIMEOUT_SECONDS = 0.5


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
    """Atomically claim pending or lease-expired work for this worker.

    An expired claim consumes an attempt. Once the configured limit has been
    reached, it is terminally failed instead of being executed indefinitely.
    """
    if CLAIM_LEASE_SECONDS <= 0:
        raise ValueError("CLAIM_LEASE_SECONDS must be greater than zero")
    if MAX_COMMAND_ATTEMPTS <= 0:
        raise ValueError("MAX_COMMAND_ATTEMPTS must be greater than zero")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if conn.autocommit:
            raise ValueError("fetch_pending requires autocommit to be disabled")
        cur.execute(
            """
            UPDATE commands
            SET status = 'failed',
                output = %s,
                exit_code = 1,
                completed_at = now(),
                claimed_at = NULL,
                lease_expires_at = NULL,
                claimed_by = NULL
            WHERE status = 'running'
              AND (lease_expires_at IS NULL OR lease_expires_at <= now())
              AND attempt_count >= %s
            """,
            (
                f"Execution claim expired after {MAX_COMMAND_ATTEMPTS} attempts; "
                "command was not retried again",
                MAX_COMMAND_ATTEMPTS,
            ),
        )
        cur.execute(
            """
            WITH candidate AS (
              SELECT id
              FROM commands
              WHERE (status = 'pending'
                     OR (status = 'running' AND
                         (lease_expires_at IS NULL OR lease_expires_at <= now())))
                AND attempt_count < %s
              ORDER BY submitted_at
              FOR UPDATE SKIP LOCKED
              LIMIT 1
            )
            UPDATE commands AS command
            SET status = 'running',
                claimed_at = now(),
                lease_expires_at = now() + (%s * interval '1 second'),
                claimed_by = %s,
                attempt_count = command.attempt_count + 1,
                completed_at = NULL
            FROM candidate
            WHERE command.id = candidate.id
            RETURNING command.id, command.user_id, command.command,
                      command.cwd_snapshot, command.env_snapshot,
                      command.claimed_at, command.lease_expires_at,
                      command.claimed_by, command.attempt_count
            """,
            (MAX_COMMAND_ATTEMPTS, CLAIM_LEASE_SECONDS, WORKER_ID),
        )
        row = cur.fetchone()
        if row:
            logging.info("Fetched command %s for user %s", row["id"], row["user_id"])
        conn.commit()
        return row


def update_command(conn, cmd_id: int, status: str, output: str, exit_code: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE commands
               SET status=%s, output=%s, exit_code=%s, completed_at=now(),
                   claimed_at=NULL, lease_expires_at=NULL, claimed_by=NULL
               WHERE id=%s""",
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
        start_new_session=os.name == "posix",
    )

    deadline = time.monotonic() + COMMAND_TIMEOUT
    output = bytearray()
    limit_exceeded = False
    timed_out = False
    termination_deadline = None
    drain_deadline = None
    fds = [proc.stdout, proc.stderr]

    while fds:
        now = time.monotonic()
        if now >= deadline and not timed_out:
            if os.name == "posix":
                try:
                    os.killpg(proc.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            else:
                proc.terminate()
            timed_out = True
            termination_deadline = now + TERMINATION_GRACE_SECONDS
        elif (
            termination_deadline is not None
            and now >= termination_deadline
            and drain_deadline is None
        ):
            if os.name == "posix":
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            elif proc.poll() is None:
                proc.kill()
            drain_deadline = now + PIPE_DRAIN_TIMEOUT_SECONDS

        if drain_deadline is not None and now >= drain_deadline:
            break

        next_event = deadline if not timed_out else termination_deadline
        if drain_deadline is not None:
            next_event = drain_deadline
        select_timeout = max(0.0, min(0.1, next_event - now))
        ready, _, _ = select.select(fds, [], [], select_timeout)
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

    for fd in fds:
        fd.close()
    if timed_out:
        try:
            proc.wait(timeout=TERMINATION_GRACE_SECONDS + PIPE_DRAIN_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    else:
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
        shell_root = os.path.realpath(os.getenv("SHELL_ROOT", "/home/sandbox"))
        if os.path.isabs(path):
            new_cwd = os.path.realpath(path)
        else:
            new_cwd = os.path.realpath(os.path.join(row["cwd_snapshot"], path))
        try:
            in_root = os.path.commonpath([shell_root, new_cwd]) == shell_root
        except ValueError:
            in_root = False
        if not in_root:
            error = f"cd: {path}: Permission denied"
            update_command(conn, row["id"], "failed", error, 1)
            logging.error(
                "Command %s for user %s failed: path %s escapes shell root %s",
                row["id"],
                row["user_id"],
                new_cwd,
                shell_root,
            )
            return
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
