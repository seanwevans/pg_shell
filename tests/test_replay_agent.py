import logging
import math

import workers.replay_agent as replay_agent


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.index = 0
        self.fetchmany_calls = []
        self.execute_calls = []
        self.next_id = 1000

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    def execute(self, query, params=None):
        self.execute_calls.append((query, params))

    def fetchmany(self, size):
        self.fetchmany_calls.append(size)
        if self.index >= len(self.rows):
            return []
        batch = self.rows[self.index : self.index + size]
        self.index += size
        return batch

    def fetchone(self):
        self.next_id += 1
        return [self.next_id]

    def fetchall(self):  # pragma: no cover - should not be used
        raise AssertionError("fetchall should not be called")


class FakeConn:
    def __init__(self, rows):
        self.cursor_obj = FakeCursor(rows)
        self.commit_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    def cursor(self, cursor_factory=None):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1


def test_replay_commands_no_commands_logs_message(monkeypatch, caplog):
    fake_conn = FakeConn([])
    monkeypatch.setattr(replay_agent, "get_conn", lambda: fake_conn)
    with caplog.at_level(logging.INFO):
        replay_agent.replay_commands("u1", 1)
    assert "no commands to replay" in caplog.text
    assert fake_conn.commit_count == 0
    assert len(fake_conn.cursor_obj.fetchmany_calls) == 1


def test_replay_commands_streams_large_command_set(monkeypatch):
    rows = [{"id": i, "command": f"cmd{i}"} for i in range(250)]
    fake_conn = FakeConn(rows)
    monkeypatch.setattr(replay_agent, "get_conn", lambda: fake_conn)
    replay_agent.replay_commands("u1", 1)
    assert fake_conn.commit_count == len(rows)
    expected_calls = math.ceil(len(rows) / 100) + 1
    assert len(fake_conn.cursor_obj.fetchmany_calls) == expected_calls
