import logging
import math

import workers.replay_agent as replay_agent


class FakeHistoryCursor:
    def __init__(self, rows):
        self.rows = rows
        self.index = 0
        self.fetchmany_calls = []
        self.execute_calls = []

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


class FakeSubmitCursor:
    def __init__(self):
        self.execute_calls = []
        self.next_id = 1000

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    def execute(self, query, params=None):
        self.execute_calls.append((query, params))

    def fetchone(self):
        self.next_id += 1
        return [self.next_id]


class FakeHistoryConn:
    def __init__(self, rows):
        self.cursor_obj = FakeHistoryCursor(rows)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    def cursor(self, cursor_factory=None):
        return self.cursor_obj


class FakeSubmitConn:
    def __init__(self):
        self.cursor_obj = FakeSubmitCursor()
        self.commit_count = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    def cursor(self, cursor_factory=None):
        return self.cursor_obj

    def commit(self):
        self.commit_count += 1


class FakeConnFactory:
    def __init__(self, *connections):
        self._connections = list(connections)

    def __call__(self):
        if not self._connections:
            raise AssertionError("get_conn called more times than expected")
        return self._connections.pop(0)


def test_replay_commands_no_commands_logs_message(monkeypatch, caplog):
    history_conn = FakeHistoryConn([])
    submit_conn = FakeSubmitConn()
    monkeypatch.setattr(
        replay_agent, "get_conn", FakeConnFactory(history_conn, submit_conn)
    )
    with caplog.at_level(logging.INFO):
        replay_agent.replay_commands("u1", 1)
    assert "no commands to replay" in caplog.text
    assert submit_conn.commit_count == 0
    assert len(history_conn.cursor_obj.fetchmany_calls) == 1


def test_replay_commands_streams_large_command_set(monkeypatch):
    rows = [{"id": i, "command": f"cmd{i}"} for i in range(250)]
    history_conn = FakeHistoryConn(rows)
    submit_conn = FakeSubmitConn()
    monkeypatch.setattr(
        replay_agent, "get_conn", FakeConnFactory(history_conn, submit_conn)
    )
    replay_agent.replay_commands("u1", 1)
    assert submit_conn.commit_count == len(rows)
    expected_calls = math.ceil(len(rows) / 100) + 1
    assert len(history_conn.cursor_obj.fetchmany_calls) == expected_calls


def test_replay_commands_replays_full_history(monkeypatch):
    rows = [{"id": i, "command": f"cmd{i}"} for i in range(150)]
    history_conn = FakeHistoryConn(rows)
    submit_conn = FakeSubmitConn()
    monkeypatch.setattr(
        replay_agent, "get_conn", FakeConnFactory(history_conn, submit_conn)
    )
    replay_agent.replay_commands("u1", 1)
    assert len(submit_conn.cursor_obj.execute_calls) == len(rows)
    submitted_commands = [params for _, params in submit_conn.cursor_obj.execute_calls]
    assert submitted_commands[0] == ("u1", "cmd0")
    assert submitted_commands[-1] == ("u1", "cmd149")
