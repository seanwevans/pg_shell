"""End-to-end executor tests against a real database.

These exercise the executor's database helpers (``fetch_pending``,
``update_command``, ``update_cwd``) together with command execution, which
the unit tests in ``test_executor_agent.py`` stub out. They require
``TEST_DATABASE_URL`` and are skipped otherwise.
"""

import uuid
from pathlib import Path

import psycopg2

from workers.executor_agent import (
    WORKER_HOST,
    fetch_pending,
    handle_command,
    recover_dead_workers,
)


def _create_user_with_env(conn, cwd: str) -> str:
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users(id, username) VALUES (%s, %s)",
            (user_id, f"exec-{user_id[:8]}"),
        )
        cur.execute(
            "INSERT INTO environments(user_id, cwd) VALUES (%s, %s)",
            (user_id, cwd),
        )
    return user_id


def test_executor_runs_pending_command_end_to_end(db_conn):
    user_id = _create_user_with_env(db_conn, "/tmp")
    with db_conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "echo integration-ok"))
        cmd_id = cur.fetchone()[0]

    # fetch_pending claims the row and flips it to 'running'.
    db_conn.autocommit = False
    try:
        row = fetch_pending(db_conn)
        assert row is not None
        assert row["id"] == cmd_id
        handle_command(db_conn, row)
    finally:
        db_conn.autocommit = True

    with db_conn.cursor() as cur:
        cur.execute(
            """SELECT status, output, exit_code, completed_at,
                      claimed_at, lease_expires_at, worker_id
                 FROM commands WHERE id = %s""",
            (cmd_id,),
        )
        (
            status,
            output,
            exit_code,
            completed_at,
            claimed_at,
            lease_expires_at,
            worker_id,
        ) = cur.fetchone()

    assert status == "done"
    assert "integration-ok" in output
    assert exit_code == 0
    assert completed_at is not None
    assert claimed_at is None
    assert lease_expires_at is None
    assert worker_id is None


def test_executor_reclaims_abandoned_command_and_unblocks_user(db_conn):
    user_id = _create_user_with_env(db_conn, "/tmp")
    with db_conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "echo recovered"))
        abandoned_id = cur.fetchone()[0]
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "echo next"))
        next_id = cur.fetchone()[0]
        cur.execute(
            """
            UPDATE commands
               SET status='running', claimed_at=now() - interval '2 minutes',
                   lease_expires_at=now() + interval '1 hour',
                   worker_id=%s
             WHERE id=%s
            """,
            (f"{WORKER_HOST}:2147483647:old-worker", abandoned_id),
        )

    worker = psycopg2.connect(db_conn.dsn)
    try:
        # Startup recovery does not need to wait for the lease when its local
        # owner PID can be identified conclusively as dead.
        assert recover_dead_workers(worker) == 1
        recovered = fetch_pending(worker)
        assert recovered["id"] == abandoned_id
        handle_command(worker, recovered)

        following = fetch_pending(worker)
        assert following["id"] == next_id
        handle_command(worker, following)
    finally:
        worker.close()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT id, status, output FROM commands WHERE id IN (%s, %s) ORDER BY id",
            (abandoned_id, next_id),
        )
        rows = cur.fetchall()
    assert [(row[0], row[1]) for row in rows] == [
        (abandoned_id, "done"),
        (next_id, "done"),
    ]
    assert "recovered" in rows[0][2]
    assert "next" in rows[1][2]


def test_executor_cd_updates_environment(db_conn, monkeypatch, tmp_path):
    sub = tmp_path / "workdir"
    sub.mkdir()
    monkeypatch.setenv("SHELL_ROOT", str(tmp_path))

    user_id = _create_user_with_env(db_conn, str(tmp_path))
    with db_conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "cd workdir"))
        cmd_id = cur.fetchone()[0]

    db_conn.autocommit = False
    try:
        row = fetch_pending(db_conn)
        assert row["id"] == cmd_id
        handle_command(db_conn, row)
    finally:
        db_conn.autocommit = True

    with db_conn.cursor() as cur:
        cur.execute("SELECT status FROM commands WHERE id = %s", (cmd_id,))
        assert cur.fetchone()[0] == "done"
        cur.execute("SELECT cwd FROM environments WHERE user_id = %s", (user_id,))
        assert cur.fetchone()[0] == str(sub)


def test_fetch_pending_returns_none_when_idle(db_conn):
    # No pending rows -> fetch_pending yields nothing rather than raising.
    db_conn.autocommit = False
    try:
        assert fetch_pending(db_conn) is None
    finally:
        db_conn.autocommit = True


def test_fetch_pending_reclaims_expired_lease(db_conn):
    user_id = _create_user_with_env(db_conn, "/tmp")
    with db_conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "echo expired"))
        cmd_id = cur.fetchone()[0]
        cur.execute(
            """UPDATE commands SET status='running', claimed_at=now(),
                      lease_expires_at=now() - interval '1 second', worker_id='gone'
                 WHERE id=%s""",
            (cmd_id,),
        )

    worker = psycopg2.connect(db_conn.dsn)
    try:
        row = fetch_pending(worker)
        assert row["id"] == cmd_id
        handle_command(worker, row)
    finally:
        worker.close()

    with db_conn.cursor() as cur:
        cur.execute("SELECT status FROM commands WHERE id=%s", (cmd_id,))
        assert cur.fetchone()[0] == "done"


def test_command_lease_migration_is_idempotent(db_conn):
    migration = Path("sql/migrate_command_leases.sql").read_text()
    with db_conn.cursor() as cur:
        cur.execute(migration)
        cur.execute(migration)


def test_queued_commands_use_sequential_per_user_environment(
    db_conn, monkeypatch, tmp_path
):
    """A second worker cannot overtake cd or use its submission snapshot."""
    child = tmp_path / "child"
    child.mkdir()
    monkeypatch.setenv("SHELL_ROOT", str(tmp_path))
    user_id = _create_user_with_env(db_conn, str(tmp_path))
    with db_conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "cd child"))
        cd_id = cur.fetchone()[0]
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "pwd"))
        pwd_id = cur.fetchone()[0]

    worker_one = psycopg2.connect(db_conn.dsn)
    worker_two = psycopg2.connect(db_conn.dsn)
    try:
        first = fetch_pending(worker_one)
        assert first["id"] == cd_id

        # This worker is eligible, but cannot claim the same user's later row.
        assert fetch_pending(worker_two) is None
        handle_command(worker_one, first)

        second = fetch_pending(worker_two)
        assert second["id"] == pwd_id
        assert second["cwd_snapshot"] == str(child)
        handle_command(worker_two, second)
    finally:
        worker_one.close()
        worker_two.close()

    with db_conn.cursor() as cur:
        cur.execute("SELECT status, output FROM commands WHERE id = %s", (pwd_id,))
        status, output = cur.fetchone()
    assert status == "done"
    assert output.strip() == str(child)
