import json
import os
import uuid
from pathlib import Path

import psycopg2
import pytest

INIT_SQL = Path('sql/init_schema.sql').read_text()
SUBMIT_SQL = Path('sql/submit_command.sql').read_text()
LATEST_SQL = Path('sql/latest_output.sql').read_text()
FORK_SQL = Path('sql/fork_session.sql').read_text()


def _collect_index_names(plan_node):
    indexes = []

    def _traverse(node):
        index_name = node.get("Index Name")
        if index_name:
            indexes.append(index_name)
        for child in node.get("Plans", []):
            _traverse(child)

    _traverse(plan_node)
    return indexes


def _fetch_plan_root(cur):
    raw = cur.fetchone()[0]
    if isinstance(raw, str):
        raw = json.loads(raw)
    if isinstance(raw, list):
        raw = raw[0]
    return raw["Plan"]


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
        cur.execute("UPDATE commands SET output='hello', exit_code=0, status='done', completed_at=now() WHERE id=%s", (cmd_id,))
        cur.execute("SELECT * FROM latest_output(%s)", (user_id,))
        row = cur.fetchone()
        assert row[0] == cmd_id
        assert row[2] == 'hello'
        assert row[6] is not None


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


def test_command_indexes_query_plans(conn):
    primary_user = str(uuid.uuid4())
    secondary_user = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (primary_user, "planner"))
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (secondary_user, "other"))

        # Populate enough data to give the planner a strong preference for the new indexes
        for i in range(50):
            cur.execute(
                "INSERT INTO commands(user_id, command, status, submitted_at)"
                " VALUES (%s, %s, %s, now() - (%s * INTERVAL '1 minute'))",
                (primary_user, f'cmd {i}', 'pending' if i % 3 else 'done', i),
            )
        for i in range(10):
            cur.execute(
                "INSERT INTO commands(user_id, command, status, submitted_at)"
                " VALUES (%s, %s, %s, now() - (%s * INTERVAL '1 minute'))",
                (secondary_user, f'spare {i}', 'pending', i),
            )

        cur.execute(
            "EXPLAIN (FORMAT JSON) "
            "SELECT id FROM commands WHERE status = 'pending' ORDER BY submitted_at LIMIT 5"
        )
        pending_plan = _fetch_plan_root(cur)
        pending_indexes = _collect_index_names(pending_plan)
        assert "commands_status_submitted_at_idx" in pending_indexes

        cur.execute(
            "EXPLAIN (FORMAT JSON) "
            "SELECT id FROM commands WHERE user_id = %s ORDER BY id DESC LIMIT 1",
            (primary_user,),
        )
        latest_plan = _fetch_plan_root(cur)
        latest_indexes = _collect_index_names(latest_plan)
        assert "commands_user_id_id_idx" in latest_indexes
