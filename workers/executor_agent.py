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
import pwd
import select
import shlex
import shutil
import signal
import socket
import subprocess
import time
import uuid
from typing import Any, Callable, Dict

from psycopg2 import sql, errors
from psycopg2.extras import Json, RealDictCursor

from workers.db import get_conn

POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "1"))
DEFAULT_LISTEN_CHANNEL = "new_command"
LISTEN_CHANNEL_ENV = os.getenv("LISTEN_CHANNEL")
LISTEN_CHANNEL = LISTEN_CHANNEL_ENV or DEFAULT_LISTEN_CHANNEL
COMMAND_TIMEOUT = int(os.getenv("COMMAND_TIMEOUT", "30"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
MAX_OUTPUT_BYTES = int(os.getenv("MAX_OUTPUT_BYTES", "65536"))
TRUNCATION_SUFFIX = "...[truncated]"
TERMINATION_GRACE_SECONDS = 0.5
PIPE_DRAIN_TIMEOUT_SECONDS = 0.5
RESERVED_COMMAND_ENV = frozenset({"DATABASE_URL", "PG_CONN"})
DEFAULT_COMMAND_PATH = "/usr/local/bin:/usr/bin:/bin"
LEASE_SECONDS = float(os.getenv("COMMAND_LEASE_SECONDS", "60"))
LEASE_REFRESH_SECONDS = float(
    os.getenv("COMMAND_LEASE_REFRESH_SECONDS", str(max(1.0, LEASE_SECONDS / 3)))
)
WORKER_ID = os.getenv("EXECUTOR_WORKER_ID") or (
    f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4()}"
)


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


def recover_worker_commands(conn, worker_id: str = WORKER_ID) -> int:
    """Release commands belonging to a previous instance of this worker.

    ``EXECUTOR_WORKER_ID`` should be a stable, unique deployment identity. At
    startup, a new instance with that identity makes work left by its dead
    predecessor eligible immediately rather than waiting for lease expiry.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE commands
               SET status = 'pending', claimed_at = NULL,
                   lease_expires_at = NULL, worker_id = NULL
             WHERE status = 'running' AND worker_id = %s
            """,
            (worker_id,),
        )
        recovered = cur.rowcount
    conn.commit()
    if recovered:
        logging.warning("Recovered %s command(s) left by worker %s", recovered, worker_id)
    return recovered


def fetch_pending(conn, worker_id: str = WORKER_ID) -> Dict[str, Any] | None:
    """Claim the next command while holding a session-level session lock.

    The advisory lock remains held after this function commits and is released
    by :func:`handle_command`.  Consequently another executor connection cannot
    run a command for the same user concurrently.  Snapshots are refreshed from
    the user's effective environment at claim time, after all preceding
    commands have reached a terminal state.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("BEGIN;")
        # Requeue abandoned work first so it no longer blocks later commands
        # in the same session. NULL leases are rows created by older workers.
        cur.execute(
            """
            UPDATE commands
               SET status = 'pending', claimed_at = NULL,
                   lease_expires_at = NULL, worker_id = NULL
             WHERE status = 'running'
               AND (lease_expires_at IS NULL OR lease_expires_at <= now())
            """
        )
        cur.execute("""
            SELECT c.id, c.user_id, c.session_id, c.command, e.cwd, e.env,
                   hashtextextended(c.session_id::text, 0) AS claim_lock_key
            FROM commands AS c
            JOIN environments AS e
              ON e.session_id = c.session_id AND e.user_id = c.user_id
            WHERE c.status = 'pending'
              AND NOT EXISTS (
                    SELECT 1
                    FROM commands AS earlier
                    WHERE earlier.session_id = c.session_id
                      AND earlier.status IN ('pending', 'running')
                      AND (earlier.submitted_at, earlier.id)
                          < (c.submitted_at, c.id)
              )
            ORDER BY c.submitted_at, c.id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """)
        row = cur.fetchone()
        if row:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (row["claim_lock_key"],))
            if not cur.fetchone()["pg_try_advisory_lock"]:
                conn.commit()
                return None
            cur.execute(
                """
                UPDATE commands
                   SET status = 'running', cwd_snapshot = %s, env_snapshot = %s,
                       claimed_at = now(),
                       lease_expires_at = now() + %s * interval '1 second',
                       worker_id = %s
                 WHERE id = %s
                """,
                (row["cwd"], Json(row["env"]), LEASE_SECONDS, worker_id, row["id"]),
            )
            row["cwd_snapshot"] = row.pop("cwd")
            row["env_snapshot"] = row.pop("env")
            logging.info("Fetched command %s for user %s", row["id"], row["user_id"])
        conn.commit()
        return row


def update_command(conn, cmd_id: int, status: str, output: str, exit_code: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE commands
                  SET status=%s, output=%s, exit_code=%s, completed_at=now(),
                      claimed_at=NULL, lease_expires_at=NULL, worker_id=NULL
                WHERE id=%s""",
            (status, output, exit_code, cmd_id),
        )
    conn.commit()


def update_cwd(conn, user_id, session_id, cwd: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE environments SET cwd=%s, updated_at=now()
                 WHERE user_id=%s AND session_id=%s""",
            (cwd, user_id, session_id),
        )
    conn.commit()


def refresh_lease(conn, cmd_id: int, worker_id: str = WORKER_ID) -> bool:
    """Extend a command lease, returning false if this worker lost ownership."""
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE commands
                  SET lease_expires_at = now() + %s * interval '1 second'
                WHERE id = %s AND status = 'running' AND worker_id = %s""",
            (LEASE_SECONDS, cmd_id, worker_id),
        )
        refreshed = cur.rowcount == 1
    conn.commit()
    return refreshed


def run_subprocess(
    command: str,
    cwd: str,
    env_snapshot: Any,
    lease_refresh: Callable[[], bool] | None = None,
) -> tuple[int, str]:
    # Never inherit the worker's environment: it contains the database DSN and
    # may contain unrelated orchestration credentials.
    env: Dict[str, str] = {
        "PATH": os.getenv("EXECUTOR_PATH", DEFAULT_COMMAND_PATH),
        "LANG": os.getenv("EXECUTOR_LANG", "C.UTF-8"),
        "LC_ALL": os.getenv("EXECUTOR_LC_ALL", "C.UTF-8"),
    }
    if env_snapshot:
        if isinstance(env_snapshot, str):
            snapshot = json.loads(env_snapshot)
        else:
            snapshot = env_snapshot
        if not isinstance(snapshot, dict):
            raise ValueError("env_snapshot must be a JSON object")
        reserved = RESERVED_COMMAND_ENV.intersection(snapshot)
        if reserved:
            names = ", ".join(sorted(reserved))
            raise ValueError(f"reserved environment variable(s): {names}")
        if not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in snapshot.items()
        ):
            raise ValueError("env_snapshot keys and values must be strings")
        env.update(snapshot)

    cmd_list = shlex.split(command)
    if not cmd_list:
        raise ValueError("command must not be empty")

    allowed_setting = os.getenv("EXECUTOR_ALLOWED_COMMANDS", "")
    allowed = {
        os.path.realpath(path)
        for path in allowed_setting.split(os.pathsep)
        if path
    }
    executable = shutil.which(cmd_list[0], path=env["PATH"])
    resolved_executable = os.path.realpath(executable) if executable else None
    if resolved_executable not in allowed:
        raise PermissionError(
            f"command is not in EXECUTOR_ALLOWED_COMMANDS: {cmd_list[0]}"
        )
    cmd_list[0] = resolved_executable

    executor_user = os.getenv("EXECUTOR_USER")
    if not executor_user:
        raise RuntimeError("EXECUTOR_USER must name a dedicated unprivileged account")
    account = pwd.getpwnam(executor_user)
    if account.pw_uid == 0:
        raise RuntimeError("EXECUTOR_USER must be unprivileged")
    if os.geteuid() != 0 and os.geteuid() != account.pw_uid:
        raise PermissionError("worker cannot switch to EXECUTOR_USER")

    def demote() -> None:
        if os.geteuid() == 0:
            os.setgroups([])
            os.setgid(account.pw_gid)
            os.setuid(account.pw_uid)

    env.update(
        {"HOME": account.pw_dir, "USER": account.pw_name, "LOGNAME": account.pw_name}
    )
    proc = subprocess.Popen(
        cmd_list,
        cwd=cwd,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=os.name == "posix",
        preexec_fn=demote if os.name == "posix" else None,
    )

    deadline = time.monotonic() + COMMAND_TIMEOUT
    output = bytearray()
    limit_exceeded = False
    timed_out = False
    termination_deadline = None
    drain_deadline = None
    fds = [proc.stdout, proc.stderr]
    next_lease_refresh = time.monotonic() + LEASE_REFRESH_SECONDS

    # A process can close both output streams and continue doing work. Keep
    # driving its timeout and lease heartbeat until it actually exits rather
    # than blocking in ``wait()`` with an unrefreshed lease.
    while fds or proc.poll() is None:
        now = time.monotonic()
        if lease_refresh is not None and now >= next_lease_refresh:
            if not lease_refresh():
                logging.error("Command lease was lost while subprocess was active")
            next_lease_refresh = now + LEASE_REFRESH_SECONDS
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
        if fds:
            ready, _, _ = select.select(fds, [], [], select_timeout)
        else:
            time.sleep(select_timeout)
            ready = []
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


def _release_user_claim(conn, row: Dict[str, Any]) -> None:
    lock_key = row.get("claim_lock_key")
    if lock_key is None:
        return
    # A command-side database error may have left the transaction aborted;
    # advisory unlock must still be allowed to run before this worker polls.
    conn.rollback()
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s)", (lock_key,))
    conn.commit()


def handle_command(conn, row: Dict[str, Any]) -> None:
    """Execute a claimed command and always release its per-user claim."""
    try:
        _handle_command(conn, row)
    finally:
        _release_user_claim(conn, row)


def _handle_command(conn, row: Dict[str, Any]) -> None:
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
        update_cwd(conn, row["user_id"], row["session_id"], new_cwd)
        update_command(conn, row["id"], "done", "", 0)
        logging.info("Command %s for user %s completed", row["id"], row["user_id"])
        return

    try:
        exit_code, output = run_subprocess(
            command,
            row["cwd_snapshot"],
            row["env_snapshot"],
            lambda: refresh_lease(conn, row["id"]),
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
        recover_worker_commands(conn)
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
