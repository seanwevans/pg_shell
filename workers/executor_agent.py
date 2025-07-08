#!/usr/bin/env python3
"""pg_shell executor agent.

Polls the ``commands`` table for pending commands and executes them. This
implementation uses ``psycopg2`` and ``subprocess``. It also listens for the
``new_command`` channel so commands can run in near real time.
"""

import json
import os
import select
import subprocess
import time
from typing import Any, Dict

import psycopg2
from psycopg2.extras import RealDictCursor

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1"))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/postgres")
LISTEN_CHANNEL = os.getenv("LISTEN_CHANNEL", "new_command")


def get_conn():
    return psycopg2.connect(DATABASE_URL)


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
        conn.commit()
        return row


def update_command(conn, cmd_id: int, status: str, output: str, exit_code: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE commands SET status=%s, output=%s, exit_code=%s WHERE id=%s",
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
    proc = subprocess.Popen(
        command,
        shell=True,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    out, _ = proc.communicate()
    return proc.returncode, out.decode()


def handle_command(conn, row: Dict[str, Any]) -> None:
    command = row["command"].strip()
    if command.startswith("cd "):
        path = command[3:].strip()
        if os.path.isabs(path):
            new_cwd = os.path.normpath(path)
        else:
            new_cwd = os.path.normpath(os.path.join(row["cwd_snapshot"], path))
        update_cwd(conn, row["user_id"], new_cwd)
        update_command(conn, row["id"], "done", "", 0)
        return

    exit_code, output = run_subprocess(command, row["cwd_snapshot"], row["env_snapshot"])
    status = "done" if exit_code == 0 else "failed"
    update_command(conn, row["id"], status, output, exit_code)


def main() -> None:
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
