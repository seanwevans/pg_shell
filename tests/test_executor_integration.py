"""End-to-end executor tests against a real database.

These exercise the executor's database helpers (``fetch_pending``,
``update_command``, ``update_cwd``) together with command execution, which
the unit tests in ``test_executor_agent.py`` stub out. They require
``TEST_DATABASE_URL`` and are skipped otherwise.
"""

import uuid

from workers.executor_agent import fetch_pending, handle_command


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
