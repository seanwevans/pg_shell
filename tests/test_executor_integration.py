"""End-to-end executor tests against a real database.

These exercise the executor's database helpers (``fetch_pending``,
``update_command``, ``update_cwd``) together with command execution, which
the unit tests in ``test_executor_agent.py`` stub out. They require
``TEST_DATABASE_URL`` and are skipped otherwise.
"""

import os
import pwd
import shutil
import uuid

import psycopg2
import psycopg2.extensions
import pytest

import workers.executor_agent
from workers.executor_agent import (
    _release_user_claim,
    fetch_pending,
    handle_command,
    recover_worker_commands,
)


@pytest.fixture(autouse=True)
def configured_executor(monkeypatch, tmp_path):
    """Configure the executor's account and allowlist for real command runs."""
    account = pwd.getpwuid(os.geteuid())
    if account.pw_uid == 0:
        account = pwd.getpwnam("nobody")
        tmp_path.chmod(0o777)
        parent = tmp_path.parent
        while parent != parent.parent and parent != parent.parent.parent:
            parent.chmod(0o755)
            if parent == tmp_path.parents[2]:
                break
            parent = parent.parent
    monkeypatch.setenv("EXECUTOR_USER", account.pw_name)
    command_path = workers.executor_agent.DEFAULT_COMMAND_PATH
    commands = [
        shutil.which(name, path=command_path)
        for name in ("echo", "pwd", "python3", "sleep")
    ]
    monkeypatch.setenv(
        "EXECUTOR_ALLOWED_COMMANDS", os.pathsep.join(filter(None, commands))
    )


def _create_user_with_env(conn, cwd: str) -> tuple[str, str]:
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users(id, username) VALUES (%s, %s)",
            (user_id, f"exec-{user_id[:8]}"),
        )
        cur.execute(
            "INSERT INTO environments(user_id, cwd) VALUES (%s, %s) RETURNING session_id",
            (user_id, cwd),
        )
        session_id = str(cur.fetchone()[0])
    return user_id, session_id


def test_executor_runs_pending_command_end_to_end(db_conn):
    user_id, session_id = _create_user_with_env(db_conn, "/tmp")
    with db_conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, %s, %s)", (user_id, session_id, "echo integration-ok"))
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

    user_id, session_id = _create_user_with_env(db_conn, str(tmp_path))
    with db_conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, %s, %s)", (user_id, session_id, "cd workdir"))
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
        cur.execute("SELECT cwd FROM environments WHERE session_id = %s", (session_id,))
        assert cur.fetchone()[0] == str(sub)


def test_fetch_pending_skips_a_session_whose_claim_lock_is_held(db_conn):
    """A busy session must not hide every session queued behind it.

    SPEC.md section 3 requires per-user serialization "while still allowing
    different users to execute concurrently". fetch_pending used to look at a
    single candidate row and return None when its advisory lock was taken, so
    one session whose lock was held by a peer worker (for instance after its
    lease expired but before it finished) starved every other session for as
    long as that worker held the lock.
    """
    busy_user, busy_session = _create_user_with_env(db_conn, "/tmp")
    free_user, free_session = _create_user_with_env(db_conn, "/tmp")

    with db_conn.cursor() as cur:
        # The blocked session's command is submitted first, so it sorts ahead.
        cur.execute(
            "SELECT submit_command(%s, %s, %s)", (busy_user, busy_session, "echo busy")
        )
        busy_id = cur.fetchone()[0]
        cur.execute(
            "SELECT submit_command(%s, %s, %s)", (free_user, free_session, "echo free")
        )
        free_id = cur.fetchone()[0]
        cur.execute("SELECT hashtextextended(%s, 0)", (busy_session,))
        busy_lock_key = cur.fetchone()[0]

    # A peer worker on its own connection holds the busy session's claim lock.
    peer = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
    peer.autocommit = True
    try:
        with peer.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (busy_lock_key,))

        db_conn.autocommit = False
        row = None
        try:
            row = fetch_pending(db_conn)
            assert row is not None, "poll gave up instead of trying the next session"
            assert row["id"] == free_id
            assert str(row["session_id"]) == free_session
        finally:
            if row is not None:
                _release_user_claim(db_conn, row)
            db_conn.autocommit = True
    finally:
        peer.close()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT id, status FROM commands WHERE id IN (%s, %s) ORDER BY id",
            (busy_id, free_id),
        )
        statuses = dict(cur.fetchall())

    # The blocked session is left untouched for the worker that owns it.
    assert statuses[busy_id] == "pending"
    assert statuses[free_id] == "running"


def test_fetch_pending_claims_atomically_without_server_warnings(db_conn):
    """The claim is one transaction, and it does not warn on every poll.

    fetch_pending used to open its transaction with an explicit ``BEGIN``,
    which psycopg2 had already issued implicitly. PostgreSQL answered every
    single poll with ``WARNING: there is already a transaction in progress``,
    so a worker polling once a second wrote 86,400 warnings a day into the
    server log.
    """
    user_id, session_id = _create_user_with_env(db_conn, "/tmp")
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT submit_command(%s, %s, %s)", (user_id, session_id, "echo warn")
        )
        cmd_id = cur.fetchone()[0]

    db_conn.autocommit = False
    del db_conn.notices[:]
    try:
        row = fetch_pending(db_conn)
        assert row is not None and row["id"] == cmd_id
        # Still a single transaction: the claim is not visible until commit,
        # and fetch_pending commits before returning.
        assert db_conn.get_transaction_status() == (
            psycopg2.extensions.TRANSACTION_STATUS_IDLE
        )
        notices = list(db_conn.notices)
    finally:
        _release_user_claim(db_conn, row)
        db_conn.autocommit = True

    assert not [n for n in notices if "already a transaction in progress" in n], notices

    with db_conn.cursor() as cur:
        cur.execute("SELECT status, worker_id FROM commands WHERE id = %s", (cmd_id,))
        status, worker_id = cur.fetchone()
    assert status == "running"
    assert worker_id is not None


def test_fetch_pending_returns_none_when_idle(db_conn):
    # No pending rows -> fetch_pending yields nothing rather than raising.
    db_conn.autocommit = False
    try:
        assert fetch_pending(db_conn) is None
    finally:
        db_conn.autocommit = True


def test_queued_commands_use_sequential_per_user_environment(
    db_conn, monkeypatch, tmp_path
):
    """A second worker cannot overtake cd or use its submission snapshot."""
    child = tmp_path / "child"
    child.mkdir()
    monkeypatch.setenv("SHELL_ROOT", str(tmp_path))
    user_id, session_id = _create_user_with_env(db_conn, str(tmp_path))
    with db_conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, %s, %s)", (user_id, session_id, "cd child"))
        cd_id = cur.fetchone()[0]
        cur.execute("SELECT submit_command(%s, %s, %s)", (user_id, session_id, "pwd"))
        pwd_id = cur.fetchone()[0]

    worker_one = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
    worker_two = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
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


def test_expired_command_is_recovered_and_no_longer_blocks_session(db_conn):
    user_id, session_id = _create_user_with_env(db_conn, "/tmp")
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT submit_command(%s, %s, %s)",
            (user_id, session_id, "echo recovered"),
        )
        abandoned_id = cur.fetchone()[0]
        cur.execute(
            "SELECT submit_command(%s, %s, %s)",
            (user_id, session_id, "echo subsequent"),
        )
        subsequent_id = cur.fetchone()[0]
        cur.execute(
            """UPDATE commands
                  SET status='running', claimed_at=now() - interval '2 minutes',
                      lease_expires_at=now() - interval '1 minute', worker_id='dead'
                WHERE id=%s""",
            (abandoned_id,),
        )

    worker = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
    try:
        recovered = fetch_pending(worker, "fresh")
        assert recovered["id"] == abandoned_id
        handle_command(worker, recovered)

        following = fetch_pending(worker, "fresh")
        assert following["id"] == subsequent_id
        handle_command(worker, following)
    finally:
        worker.close()

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT id, status, worker_id, lease_expires_at FROM commands "
            "WHERE id IN (%s, %s) ORDER BY id",
            (abandoned_id, subsequent_id),
        )
        rows = cur.fetchall()
    assert [(row[0], row[1]) for row in rows] == [
        (abandoned_id, "done"),
        (subsequent_id, "done"),
    ]
    assert all(row[2] is None and row[3] is None for row in rows)


def test_startup_recovers_commands_from_same_worker_identity(db_conn):
    user_id, session_id = _create_user_with_env(db_conn, "/tmp")
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT submit_command(%s, %s, %s)",
            (user_id, session_id, "echo restart"),
        )
        cmd_id = cur.fetchone()[0]
        cur.execute(
            """UPDATE commands SET status='running', claimed_at=now(),
                      lease_expires_at=now() + interval '1 hour', worker_id='executor-a'
                WHERE id=%s""",
            (cmd_id,),
        )

    assert recover_worker_commands(db_conn, "executor-a") == 1
    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT status, claimed_at, lease_expires_at, worker_id FROM commands WHERE id=%s",
            (cmd_id,),
        )
        assert cur.fetchone() == ("pending", None, None, None)
