import os
import uuid
from pathlib import Path

import psycopg2
import pytest

INIT_SQL = Path('sql/init_schema.sql').read_text()
SUBMIT_SQL = Path('sql/submit_command.sql').read_text()
LATEST_SQL = Path('sql/latest_output.sql').read_text()
FORK_SQL = Path('sql/fork_session.sql').read_text()


@pytest.fixture(scope="module")
def conn():
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not set")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS commands, environments, users CASCADE;")
    cur.execute(INIT_SQL)
    cur.execute(SUBMIT_SQL)
    cur.execute(LATEST_SQL)
    cur.execute(FORK_SQL)
    cur.close()
    yield conn
    cur = conn.cursor()
    cur.execute("DROP TABLE commands, environments, users CASCADE;")
    cur.close()
    conn.close()


def test_submit_and_latest_output(conn):
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (user_id, "testuser"))
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "echo hello"))
        cmd_id = cur.fetchone()[0]
        cur.execute("UPDATE commands SET output='hello', exit_code=0, status='done' WHERE id=%s", (cmd_id,))
        cur.execute("SELECT * FROM latest_output(%s)", (user_id,))
        row = cur.fetchone()
        assert row[0] == cmd_id
        assert row[2] == 'hello'


def test_fork_session(conn):
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (user_id, "u2"))
        cur.execute(
            "INSERT INTO environments(user_id, cwd, env) VALUES (%s, %s, %s)",
            (user_id, '/home/start', '{"FOO":"BAR"}'),
        )
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "ls"))
        cmd_id = cur.fetchone()[0]
        cur.execute(
            "UPDATE commands SET cwd_snapshot=%s, env_snapshot=%s::jsonb WHERE id=%s",
            ('/home/start', '{"FOO":"BAR"}', cmd_id),
        )
        cur.execute("SELECT fork_session(%s, %s)", (user_id, cmd_id))
        cur.fetchone()
        cur.execute("SELECT cwd FROM environments WHERE user_id=%s", (user_id,))
        cwd = cur.fetchone()[0]
        assert cwd == '/home/start'
