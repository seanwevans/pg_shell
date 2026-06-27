import os
import uuid
import logging
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
    cur.execute(
        "DROP TABLE IF EXISTS commands, environments, users, pg_shell_config CASCADE;"
    )
    cur.execute(INIT_SQL)
    cur.close()
    yield conn
    cur = conn.cursor()
    cur.execute(
        "DROP TABLE commands, environments, users, pg_shell_config CASCADE;"
    )
    cur.close()
    conn.close()


def test_cleanup_once_deletes_only_old_completed_commands(conn):
    uid_str = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users (id, username) VALUES (%s, %s)", (uid_str, "deluser")
        )
        # Old + done -> deleted
        cur.execute(
            "INSERT INTO commands (user_id, command, submitted_at, status) "
            "VALUES (%s, 'old-done', now() - interval '100 days', 'done') RETURNING id",
            (uid_str,),
        )
        old_done_id = cur.fetchone()[0]
        # Old but not done -> preserved (cleanup only targets completed history)
        cur.execute(
            "INSERT INTO commands (user_id, command, submitted_at, status) "
            "VALUES (%s, 'old-failed', now() - interval '100 days', 'failed') RETURNING id",
            (uid_str,),
        )
        old_failed_id = cur.fetchone()[0]
        # Recent + done -> preserved (still within retention window)
        cur.execute(
            "INSERT INTO commands (user_id, command, submitted_at, status) "
            "VALUES (%s, 'recent-done', now(), 'done') RETURNING id",
            (uid_str,),
        )
        recent_done_id = cur.fetchone()[0]

    cleanup_once(conn, 90)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM commands WHERE user_id = %s ORDER BY id", (uid_str,)
        )
        remaining = [row[0] for row in cur.fetchall()]

    assert old_done_id not in remaining
    assert old_failed_id in remaining
    assert recent_done_id in remaining


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


def test_cleanup_once_resets_multiple_envs(conn, caplog):
    ids = [str(uuid.uuid4()) for _ in range(2)]
    with conn.cursor() as cur:
        for i, uid in enumerate(ids):
            cur.execute(
                "INSERT INTO users (id, username) VALUES (%s, %s)",
                (uid, f"multiuser{i}"),
            )
            cur.execute(
                "INSERT INTO environments (user_id, cwd, env, updated_at) VALUES (%s, %s, %s::jsonb, now() - interval '100 days')",
                (uid, '/tmp', '{"k":1}')
            )
            cur.execute(
                "INSERT INTO commands (user_id, command, submitted_at, status) VALUES (%s, 'ls', now() - interval '100 days', 'done')",
                (uid,)
            )
    caplog.set_level(logging.INFO)
    cleanup_once(conn, 90)
    assert "Reset 2 stale environments" in caplog.text
    with conn.cursor() as cur:
        cur.execute(
            "SELECT cwd, env FROM environments WHERE user_id = ANY(%s::uuid[]) ORDER BY user_id",
            (ids,)
        )
        rows = cur.fetchall()
    for cwd, env in rows:
        assert cwd == '/home/sandbox'
        assert env == {}

