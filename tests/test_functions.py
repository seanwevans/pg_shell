import os
import select
import time
import uuid
from pathlib import Path

import psycopg2
from psycopg2 import sql
import pytest

INSTALL_SCRIPT = Path('sql/install.sql').read_text()


def run_install(cur):
    for raw_line in INSTALL_SCRIPT.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        if stripped.startswith("\\i"):
            include = stripped[2:].strip().split()[0]
            sql_text = Path(include).read_text()
            cur.execute(sql_text)


def wait_for_notification(conn, timeout: float = 2.0):
    deadline = time.monotonic() + timeout
    while True:
        conn.poll()
        if conn.notifies:
            return conn.notifies.pop(0)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        select.select([conn], [], [], min(0.1, remaining))
    return None


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
    run_install(cur)
    cur.close()
    yield conn
    cur = conn.cursor()
    cur.execute(
        "DROP TABLE commands, environments, users, pg_shell_config CASCADE;"
    )
    cur.close()
    conn.close()


def test_submit_and_latest_output(conn):
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (user_id, "testuser"))
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "echo hello"))
        cmd_id = cur.fetchone()[0]
        cur.execute("UPDATE commands SET output='hello', exit_code=0, status='done', completed_at=now() WHERE id=%s", (cmd_id,))
        cur.execute("SELECT * FROM latest_output(%s)", (user_id,))
        row = cur.fetchone()
        assert row[0] == cmd_id
        assert row[2] == 'hello'
        assert row[6] is not None


def test_submit_command_notifies(conn):
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (user_id, "notify"))
        cur.execute("LISTEN new_command;")
    conn.notifies.clear()
    with conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "echo ping"))
        cmd_id = cur.fetchone()[0]
    notification = wait_for_notification(conn)
    with conn.cursor() as cur:
        cur.execute("UNLISTEN *;")
    assert notification is not None
    assert notification.channel == 'new_command'
    assert notification.payload == str(cmd_id)


def test_submit_command_respects_configured_channel(conn):
    user_id = str(uuid.uuid4())
    alt_channel = 'custom_command_channel'
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (user_id, "config"))
        cur.execute(
            """
            INSERT INTO pg_shell_config(key, value)
            VALUES ('listen_channel', %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            (alt_channel,),
        )
        cur.execute("UNLISTEN *;")
        cur.execute(sql.SQL("LISTEN {}").format(sql.Identifier(alt_channel)))
    conn.notifies.clear()
    with conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, %s)", (user_id, "echo config"))
        cmd_id = cur.fetchone()[0]
    notification = wait_for_notification(conn)
    with conn.cursor() as cur:
        cur.execute("UNLISTEN *;")
        cur.execute(
            "UPDATE pg_shell_config SET value='new_command' WHERE key='listen_channel'"
        )
    assert notification is not None
    assert notification.channel == alt_channel
    assert notification.payload == str(cmd_id)


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


def test_latest_output_since_id(conn):
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (user_id, "u3"))
        ids = []
        for cmd in ("echo one", "echo two", "echo three"):
            cur.execute("SELECT submit_command(%s, %s)", (user_id, cmd))
            cmd_id = cur.fetchone()[0]
            cur.execute(
                "UPDATE commands SET output=%s, exit_code=0, status='done', completed_at=now() WHERE id=%s",
                (cmd.split()[1], cmd_id),
            )
            ids.append(cmd_id)
        cur.execute("SELECT * FROM latest_output(%s, %s)", (user_id, ids[1]))
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == ids[2]
