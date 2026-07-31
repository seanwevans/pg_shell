import json
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


def _create_session(cur, user_id):
    cur.execute(
        "INSERT INTO environments(user_id) VALUES (%s) RETURNING session_id",
        (user_id,),
    )
    return cur.fetchone()[0]


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
        session_id = _create_session(cur, user_id)
        cmd_ids = []
        for idx in range(25):
            cur.execute("SELECT submit_command(%s, %s, %s)", (user_id, session_id, f"echo msg{idx}"))
            cmd_id = cur.fetchone()[0]
            cur.execute(
                "UPDATE commands SET output=%s, exit_code=0, status='done', completed_at=now() WHERE id=%s",
                (f"msg{idx}", cmd_id),
            )
            cmd_ids.append(cmd_id)

        cur.execute("SELECT * FROM latest_output(%s, %s)", (user_id, session_id))
        rows = cur.fetchall()

        assert len(rows) == 20
        expected_ids = list(reversed(cmd_ids))[:20]
        assert [row[0] for row in rows] == expected_ids
        assert rows[0][2] == "msg24"
        assert rows[0][6] is not None


def test_submit_command_notifies(conn):
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (user_id, "notify"))
        session_id = _create_session(cur, user_id)
        cur.execute("LISTEN new_command;")
    conn.notifies.clear()
    with conn.cursor() as cur:
        cur.execute("SELECT submit_command(%s, %s, %s)", (user_id, session_id, "echo ping"))
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
        session_id = _create_session(cur, user_id)
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
        cur.execute("SELECT submit_command(%s, %s, %s)", (user_id, session_id, "echo config"))
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

def test_submit_command_requires_existing_user(conn):
    missing_user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        with pytest.raises(psycopg2.Error) as exc_info:
            cur.execute("SELECT submit_command(%s, %s, %s)", (missing_user_id, str(uuid.uuid4()), "echo nope"))
    err = exc_info.value
    assert "Unknown user_id" in str(err)
    assert err.pgcode == '22023'


def test_fork_session_creates_independent_session_without_changing_source(conn):
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (user_id, "u2"))
        cur.execute(
            """INSERT INTO environments(user_id, cwd, env)
                 VALUES (%s, %s, %s) RETURNING session_id""",
            (user_id, '/home/source-current', '{"LIVE":"YES"}'),
        )
        source_session_id = cur.fetchone()[0]
        cur.execute(
            "SELECT submit_command(%s, %s, %s)",
            (user_id, source_session_id, "ls"),
        )
        cmd_id = cur.fetchone()[0]
        cur.execute(
            "UPDATE commands SET cwd_snapshot=%s, env_snapshot=%s::jsonb WHERE id=%s",
            ('/home/start', '{"FOO":"BAR"}', cmd_id),
        )
        cur.execute("SELECT fork_session(%s, %s)", (user_id, cmd_id))
        fork_session_id = cur.fetchone()[0]
        assert fork_session_id != source_session_id

        cur.execute(
            "SELECT cwd, env FROM environments WHERE session_id=%s",
            (source_session_id,),
        )
        assert cur.fetchone() == ('/home/source-current', {"LIVE": "YES"})
        cur.execute(
            "SELECT cwd, env FROM environments WHERE session_id=%s",
            (fork_session_id,),
        )
        assert cur.fetchone() == ('/home/start', {"FOO": "BAR"})

        # Commands submitted to the fork use its state and do not affect the
        # source session's command stream.
        cur.execute(
            "SELECT submit_command(%s, %s, %s)",
            (user_id, fork_session_id, "pwd"),
        )
        fork_command_id = cur.fetchone()[0]
        cur.execute(
            "SELECT session_id, cwd_snapshot FROM commands WHERE id=%s",
            (fork_command_id,),
        )
        assert cur.fetchone() == (fork_session_id, '/home/start')


def test_fork_session_different_user_source_command_fails(conn):
    source_user_id = str(uuid.uuid4())
    target_user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users(id, username) VALUES (%s, %s)",
            (source_user_id, "fork-src"),
        )
        cur.execute(
            "INSERT INTO users(id, username) VALUES (%s, %s)",
            (target_user_id, "fork-dst"),
        )
        cur.execute(
            "INSERT INTO environments(user_id) VALUES (%s) RETURNING session_id",
            (source_user_id,),
        )
        source_session_id = cur.fetchone()[0]
        cur.execute(
            "SELECT submit_command(%s, %s, %s)",
            (source_user_id, source_session_id, "pwd"),
        )
        source_cmd_id = cur.fetchone()[0]
        cur.execute(
            "UPDATE commands SET cwd_snapshot=%s, env_snapshot=%s::jsonb WHERE id=%s",
            ('/home/source', '{"SRC":"1"}', source_cmd_id),
        )

        with pytest.raises(psycopg2.errors.RaiseException, match="not found or forbidden"):
            cur.execute("SELECT fork_session(%s, %s)", (target_user_id, source_cmd_id))


def test_latest_output_since_id(conn):
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (user_id, "u3"))
        session_id = _create_session(cur, user_id)
        ids = []
        for idx in range(25):
            cur.execute("SELECT submit_command(%s, %s, %s)", (user_id, session_id, f"echo seq{idx}"))
            cmd_id = cur.fetchone()[0]
            cur.execute(
                "UPDATE commands SET output=%s, exit_code=0, status='done', completed_at=now() WHERE id=%s",
                (f"seq{idx}", cmd_id),
            )
            ids.append(cmd_id)

        since_id = ids[4]
        cur.execute("SELECT * FROM latest_output(%s, %s, %s)", (user_id, session_id, since_id))
        rows = cur.fetchall()

        expected_ids = ids[5:]
        assert len(rows) == len(expected_ids)
        assert [row[0] for row in rows] == expected_ids


def test_html_rpcs_render_escaped_fragments(conn):
    user_id = str(uuid.uuid4())
    command = '<button title="quoted">run & go</button>'
    output = "<img src=x onerror='bad'>"
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users(id, username) VALUES (%s, %s)",
            (user_id, "html-renderer"),
        )
        session_id = _create_session(cur, user_id)
        cur.execute(
            "SELECT submit_command_html(%s, %s, %s)",
            (user_id, session_id, command),
        )
        submitted = cur.fetchone()[0]
        cur.execute(
            "UPDATE commands SET output=%s, exit_code=0, status='done' "
            "WHERE user_id=%s AND session_id=%s",
            (output, user_id, session_id),
        )
        cur.execute(
            "SELECT latest_output_html(%s, %s)",
            (user_id, session_id),
        )
        latest = cur.fetchone()[0]

    assert '&lt;button title=&quot;quoted&quot;&gt;run &amp; go&lt;/button&gt;' in submitted
    assert '<button' not in submitted
    assert "command-status\">pending" in submitted
    assert "&lt;img src=x onerror=&#39;bad&#39;&gt;" in latest
    assert "command-status\">done" in latest
    assert "exit-code\">0" in latest


def test_replay_session_requeues_from_start(conn):
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (user_id, "replayer"))
        session_id = _create_session(cur, user_id)
        original_ids = []
        for idx in range(3):
            cur.execute("SELECT submit_command(%s, %s, %s)", (user_id, session_id, f"echo r{idx}"))
            original_ids.append(cur.fetchone()[0])

        start_id = original_ids[1]
        cur.execute("SELECT replay_session(%s, %s, %s)", (user_id, session_id, start_id))
        run_id = cur.fetchone()[0]
        assert run_id is not None

        # Replayed rows reference the originals at/after start_id, share the
        # returned run id, and are queued as fresh pending work.
        cur.execute(
            """
            SELECT command, replay_of_command_id, replay_run_id, status
              FROM commands
             WHERE replay_run_id = %s
             ORDER BY id
            """,
            (run_id,),
        )
        replayed = cur.fetchall()

    assert [r[0] for r in replayed] == ["echo r1", "echo r2"]
    assert [r[1] for r in replayed] == original_ids[1:]
    assert all(str(r[2]) == str(run_id) for r in replayed)
    assert all(r[3] == "pending" for r in replayed)


def test_replay_session_only_replays_originals(conn):
    user_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (user_id, "replayer2"))
        session_id = _create_session(cur, user_id)
        cur.execute("SELECT submit_command(%s, %s, %s)", (user_id, session_id, "echo once"))
        original_id = cur.fetchone()[0]

        # First replay creates one replayed row.
        cur.execute("SELECT replay_session(%s, %s, %s)", (user_id, session_id, original_id))
        cur.fetchone()

        # Second replay from the same start must re-queue only the original,
        # not the row produced by the first replay.
        cur.execute("SELECT replay_session(%s, %s, %s)", (user_id, session_id, original_id))
        second_run = cur.fetchone()[0]

        cur.execute(
            "SELECT replay_of_command_id FROM commands WHERE replay_run_id = %s",
            (second_run,),
        )
        rows = cur.fetchall()

    assert len(rows) == 1
    assert rows[0][0] == original_id


def test_replay_session_unknown_user_raises(conn):
    missing_user = str(uuid.uuid4())
    with conn.cursor() as cur:
        with pytest.raises(psycopg2.Error) as exc_info:
            cur.execute("SELECT replay_session(%s, %s, %s)", (missing_user, str(uuid.uuid4()), 1))
    assert exc_info.value.pgcode == "22023"


def test_command_indexes_query_plans(conn):
    primary_user = str(uuid.uuid4())
    secondary_user = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (primary_user, "planner"))
        cur.execute("INSERT INTO users(id, username) VALUES (%s, %s)", (secondary_user, "other"))
        primary_session = _create_session(cur, primary_user)
        secondary_session = _create_session(cur, secondary_user)

        # Populate enough data to give the planner a strong preference for the new indexes
        for i in range(50):
            cur.execute(
                "INSERT INTO commands(user_id, session_id, command, status, submitted_at)"
                " VALUES (%s, %s, %s, %s, now() - (%s * INTERVAL '1 minute'))",
                (primary_user, primary_session, f'cmd {i}', 'pending' if i % 3 else 'done', i),
            )
        for i in range(10):
            cur.execute(
                "INSERT INTO commands(user_id, session_id, command, status, submitted_at)"
                " VALUES (%s, %s, %s, %s, now() - (%s * INTERVAL '1 minute'))",
                (secondary_user, secondary_session, f'spare {i}', 'pending', i),
            )

        # Refresh planner statistics so index selection reflects the data we
        # just inserted; without this the planner works from empty-table stats
        # and may pick an unrelated index.
        cur.execute("ANALYZE commands")

        cur.execute(
            "EXPLAIN (FORMAT JSON) "
            "SELECT id FROM commands WHERE status = 'pending' ORDER BY submitted_at LIMIT 5"
        )
        pending_plan = _fetch_plan_root(cur)
        pending_indexes = _collect_index_names(pending_plan)
        assert "commands_status_submitted_at_idx" in pending_indexes

        cur.execute(
            "EXPLAIN (FORMAT JSON) "
            "SELECT id FROM commands WHERE session_id = %s ORDER BY id DESC LIMIT 1",
            (primary_session,),
        )
        latest_plan = _fetch_plan_root(cur)
        latest_indexes = _collect_index_names(latest_plan)
        assert "commands_session_id_id_idx" in latest_indexes
