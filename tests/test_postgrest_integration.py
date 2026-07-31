"""Integration coverage for requests sent to a live PostgREST instance."""

import os
import uuid

import pytest

from cli.shell_cli import tail_output


def test_tail_output_invokes_latest_output_through_postgrest(db_conn, capsys):
    """A CLI-generated query resolves latest_output(UUID, INTEGER)."""
    base_url = os.environ.get("TEST_POSTGREST_URL")
    if not base_url:
        pytest.skip("TEST_POSTGREST_URL not set")

    user_id = str(uuid.uuid4())
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users(id, username) VALUES (%s, %s)",
            (user_id, f"postgrest-{user_id}"),
        )
        cur.execute("INSERT INTO environments(user_id) VALUES (%s) RETURNING session_id", (user_id,))
        session_id = str(cur.fetchone()[0])
        cur.execute("SELECT submit_command(%s, %s, %s)", (user_id, session_id, "echo integration"))
        command_id = cur.fetchone()[0]
        cur.execute(
            """
            UPDATE commands
               SET output = 'integration', exit_code = 0, status = 'done',
                   completed_at = now()
             WHERE id = %s
            """,
            (command_id,),
        )

    assert tail_output(
        base_url.rstrip("/"), user_id, session_id, since=0, interval=0, max_polls=1
    ) == 0
    assert capsys.readouterr().out.splitlines() == [
        "$ echo integration",
        "integration",
        "(exit 0)",
    ]
