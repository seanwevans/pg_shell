import os
from datetime import datetime

import csv

import psycopg2

import workers.monitor_agent as monitor_agent


def test_collect_metrics_streams(monkeypatch):
    rows = [
        ("u1", "2024-01-01", 1, 0.5),
        ("u2", "2024-01-01", 2, 1.0),
        ("u1", "2024-01-02", 3, 1.5),
    ]
    fetch_calls = []

    class FakeCursor:
        def __init__(self):
            self.idx = 0
            self.sql = None
            self.params = None

        def execute(self, sql, params=None):
            self.sql = sql
            self.params = params

        def fetchone(self):
            fetch_calls.append(self.idx)
            if self.idx >= len(rows):
                return None
            row = rows[self.idx]
            self.idx += 1
            return row

        def fetchall(self):  # pragma: no cover - should not run
            raise AssertionError("fetchall should not be called")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

    cursor = FakeCursor()

    class FakeConn:
        def cursor(self, name=None):
            # ensure server-side cursor is requested
            assert name is not None
            return cursor

    conn = FakeConn()

    gen = monitor_agent.collect_metrics(conn)
    # consume one row to ensure generator yields incrementally
    first = next(gen)
    assert first == rows[0]
    assert fetch_calls == [0]

    captured = []

    def fake_print(line):
        captured.append(line)

    monkeypatch.setattr("builtins.print", fake_print)

    # process remaining rows
    monitor_agent.output_metrics(gen, None)

    # fetchone called for remaining rows plus final sentinel None
    assert fetch_calls == [0, 1, 2, 3]
    # only remaining two rows printed
    assert len(captured) == 2
    assert "u2" in captured[0]
    assert "u1" in captured[1]
    assert "ORDER BY day, commands.user_id" in cursor.sql


def test_collect_metrics_uses_incremental_filter_when_state_present():
    class FakeCursor:
        def __init__(self):
            self.sql = None
            self.params = None
            self.calls = 0

        def execute(self, sql, params=None):
            self.sql = sql
            self.params = params

        def fetchone(self):
            self.calls += 1
            return None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            pass

    cursor = FakeCursor()

    class FakeConn:
        def cursor(self, name=None):
            return cursor

    last_completed = datetime(2024, 1, 1, 10, 0, 0)
    list(
        monitor_agent.collect_metrics(
            FakeConn(),
            last_completed_at=last_completed,
            last_command_id=42,
        )
    )

    assert "completed_at > %s OR (completed_at = %s AND id > %s)" in cursor.sql
    assert cursor.params == [last_completed, last_completed, 42]
    assert "WITH changed_groups" in cursor.sql
    assert "JOIN changed_groups" in cursor.sql
    # The watermark predicate belongs only to group discovery, while the outer
    # query includes every terminal command in each changed daily group.
    assert cursor.sql.count("completed_at > %s") == 1


def test_second_poll_emits_complete_daily_aggregate():
    """Commands arriving in later polls replace, rather than delta, a day."""

    first_poll = iter([("u1", "2024-01-01", 1, 2.0)])
    second_poll = iter([("u1", "2024-01-01", 2, 4.0)])

    # This models the result of the complete-group SQL after a second command
    # taking six seconds arrives: count=2 and weighted average=(2+6)/2=4.
    from tempfile import TemporaryDirectory

    with TemporaryDirectory() as directory:
        path = f"{directory}/metrics.csv"
        monitor_agent.upsert_csv_metrics(path, first_poll)
        monitor_agent.upsert_csv_metrics(path, second_poll)

        with open(path, newline="") as csv_file:
            rows = list(csv.DictReader(csv_file))

    assert rows == [
        {
            "user_id": "u1",
            "day": "2024-01-01",
            "command_count": "2",
            "avg_seconds": "4.0",
        }
    ]


def test_output_metrics_flushes_immediately(tmp_path):
    rows = iter([("u1", "2024-01-01", 1, 0.5)])
    csv_path = tmp_path / "metrics.csv"

    with open(csv_path, "a", newline="") as csv_file:
        writer = csv.writer(csv_file)
        monitor_agent.output_metrics(rows, writer, csv_file.flush)

        with open(csv_path, "r", newline="") as reader:
            contents = reader.read()

    assert "u1" in contents


def test_compute_since_timestamp_rejects_dual_window_args():
    class Args:
        since_hours = 1
        since_days = 1

    try:
        monitor_agent.compute_since_timestamp(Args())
    except ValueError:
        pass
    else:  # pragma: no cover - defensive
        raise AssertionError("Expected ValueError when both windows are set")


def test_ensure_monitor_state_table_commits(monkeypatch):
    """The CREATE TABLE must be committed, not left to a later save.

    Runs that never reach save_monitor_state -- every --since-hours or
    --since-days run, and any run with nothing new -- otherwise rolled the
    table back on close, so the watermark could never be kept.
    """
    calls = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, query, params=None):
            calls.append("execute")

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            calls.append("commit")

    monitor_agent.ensure_monitor_state_table(FakeConn())

    assert calls == ["execute", "commit"]


def test_ensure_monitor_state_table_persists_without_a_watermark(db_conn):
    """End to end: the table survives a run that saves no watermark."""
    with db_conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS monitor_state")

    conn = psycopg2.connect(os.environ["TEST_DATABASE_URL"])
    try:
        monitor_agent.ensure_monitor_state_table(conn)
    finally:
        conn.close()  # rolls back anything left uncommitted

    with db_conn.cursor() as cur:
        cur.execute("SELECT to_regclass('monitor_state') IS NOT NULL")
        assert cur.fetchone()[0] is True


def test_module_exposes_its_docstring():
    """A __future__ import above the docstring demotes it to a dead literal."""
    assert monitor_agent.__doc__ is not None
    assert "pg_shell monitor agent" in monitor_agent.__doc__
