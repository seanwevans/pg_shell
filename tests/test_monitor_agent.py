import logging
import sys

import pytest

import workers.monitor_agent as monitor_agent


def test_main_handles_connection_error(monkeypatch, caplog):
    def fake_get_conn():
        raise RuntimeError("boom")

    monkeypatch.setattr(monitor_agent, "get_conn", fake_get_conn)
    monkeypatch.setattr(sys, "argv", ["monitor_agent", "--once"])

    with caplog.at_level(logging.ERROR):
        code = monitor_agent.main()

    assert code == 1
    assert "boom" in caplog.text

