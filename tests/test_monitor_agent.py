from datetime import datetime

import csv

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
    assert "ORDER BY day, user_id" in cursor.sql


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
