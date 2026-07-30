"""End-to-end executor tests against a real database.

These exercise the executor's database helpers (``fetch_pending``,
``update_command``, ``update_cwd``) together with command execution, which
the unit tests in ``test_executor_agent.py`` stub out. They require
``TEST_DATABASE_URL`` and are skipped otherwise.
"""

import uuid
from pathlib import Path

import psycopg2

from workers.executor_agent import fetch_pending, handle_command


def test_execution_claim_migration_is_idempotent(db_conn):
    migration = Path("sql/migrate_execution_claims.sql").read_text()
    with db_conn.cursor() as cur:
        cur.execute(migration)
        cur.execute(migration)
        cur.execute(
            """SELECT column_name
               FROM information_schema.columns
               WHERE table_name = 'commands'
                 AND column_name IN
                     ('claimed_at', 'lease_expires_at', 'claimed_by', 'attempt_count')"""
        )
        assert {row[0] for row in cur.fetchall()} == {
            "claimed_at",
            "lease_expires_at",
            "claimed_by",
            "attempt_count",
        }


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
            "SELECT status, output, exit_code, completed_at FROM commands WHERE id = %s",
            (cmd_id,),
        )
        status, output, exit_code, completed_at = cur.fetchone()

    assert status == "done"
    assert "integration-ok" in output
    assert exit_code == 0
    assert completed_at is not None


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


def test_concurrent_executors_claim_different_commands(db_conn, tmp_path):
    user_id = _create_user_with_env(db_conn, str(tmp_path))
    with db_conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, 'echo first')", (user_id,))
        first_id = cur.fetchone()[0]
        cur.execute("SELECT submit_command(%s, 'echo second')", (user_id,))
        second_id = cur.fetchone()[0]

    other = psycopg2.connect(db_conn.dsn)
    db_conn.autocommit = False
    other.autocommit = False
    try:
        first = fetch_pending(db_conn)
        second = fetch_pending(other)
        assert {first["id"], second["id"]} == {first_id, second_id}
    finally:
        db_conn.rollback()
        db_conn.autocommit = True
        other.close()


def test_expired_claim_is_retried_then_fails_at_attempt_limit(
    db_conn, monkeypatch, tmp_path
):
    monkeypatch.setattr("workers.executor_agent.MAX_COMMAND_ATTEMPTS", 2)
    user_id = _create_user_with_env(db_conn, str(tmp_path))
    with db_conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, 'echo stale')", (user_id,))
        cmd_id = cur.fetchone()[0]
        cur.execute(
            """UPDATE commands
               SET status='running', attempt_count=1,
                   claimed_at=now() - interval '2 minutes',
                   lease_expires_at=now() - interval '1 minute',
                   claimed_by='dead-worker'
               WHERE id=%s""",
            (cmd_id,),
        )

    db_conn.autocommit = False
    try:
        reclaimed = fetch_pending(db_conn)
        assert reclaimed["id"] == cmd_id
        assert reclaimed["attempt_count"] == 2
        with db_conn.cursor() as cur:
            cur.execute(
                "UPDATE commands SET lease_expires_at=now() - interval '1 second' WHERE id=%s",
                (cmd_id,),
            )
        assert fetch_pending(db_conn) is None
    finally:
        db_conn.autocommit = True

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, output, claimed_at, lease_expires_at, claimed_by FROM commands WHERE id=%s",
            (cmd_id,),
        )
        status, output, claimed_at, lease_expires_at, claimed_by = cur.fetchone()
    assert status == "failed"
    assert "2 attempts" in output
    assert claimed_at is None
    assert lease_expires_at is None
    assert claimed_by is None
