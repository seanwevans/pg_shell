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

        def execute(self, sql):
            pass

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
