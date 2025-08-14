import os
import uuid
from pathlib import Path

import psycopg2
import pytest

from workers.cleanup_agent import cleanup_once


INIT_SQL = Path("sql/init_schema.sql").read_text()


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
    cur.close()
    yield conn
    cur = conn.cursor()
    cur.execute("DROP TABLE commands, environments, users CASCADE;")
    cur.close()
    conn.close()


def test_cleanup_once_resets_env(conn):
    uid_str = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users (id, username) VALUES (%s, %s)", (uid_str, "testuser"))
        cur.execute(
            "INSERT INTO environments (user_id, cwd, env, updated_at) VALUES (%s, %s, %s::jsonb, now() - interval '100 days')",
            (uid_str, '/tmp', '{"k":1}'),
        )
        cur.execute(
            "INSERT INTO commands (user_id, command, submitted_at, status) VALUES (%s, 'ls', now() - interval '100 days', 'done')",
            (uid_str,),
        )

    cleanup_once(conn, 90)

    with conn.cursor() as cur:
        cur.execute("SELECT cwd, env FROM environments WHERE user_id = %s", (uid_str,))
        cwd, env = cur.fetchone()

    assert cwd == '/home/sandbox'
    assert env == {}

