import pytest
import workers.executor_agent
from workers.executor_agent import run_subprocess, handle_command


def test_run_subprocess_combines_output(tmp_path):
    cmd = "python3 -c 'import sys;print(\"out\");print(\"err\", file=sys.stderr)'"
    exit_code, output = run_subprocess(cmd, str(tmp_path), None)
    assert exit_code == 0
    assert "out" in output
    assert "err" in output


def test_handle_command_uses_combined_output(monkeypatch):
    captured = {}

    def fake_run_subprocess(command, cwd, env):
        return 1, "stdout+stderr"

    def fake_update_command(conn, cmd_id, status, output, exit_code):
        captured['status'] = status
        captured['output'] = output
        captured['exit_code'] = exit_code
        captured['cmd_id'] = cmd_id

    monkeypatch.setattr('workers.executor_agent.run_subprocess', fake_run_subprocess)
    monkeypatch.setattr('workers.executor_agent.update_command', fake_update_command)

    row = {
        'id': 42,
        'user_id': 'u1',
        'command': 'ls',
        'cwd_snapshot': '.',
        'env_snapshot': None,
    }
    handle_command(None, row)

    assert captured['output'] == "stdout+stderr"
    assert captured['status'] == 'failed'
    assert captured['exit_code'] == 1
    assert captured['cmd_id'] == 42


def test_handle_command_cd_changes_directory(tmp_path, monkeypatch):
    captured: dict = {}

    def fake_update_cwd(conn, user_id, cwd):
        captured['cwd'] = cwd

    def fake_update_command(conn, cmd_id, status, output, exit_code):
        captured['status'] = status
        captured['output'] = output
        captured['exit_code'] = exit_code

    def fake_run_subprocess(*args, **kwargs):  # pragma: no cover - should not run
        raise AssertionError('run_subprocess should not be called')

    monkeypatch.setattr('workers.executor_agent.update_cwd', fake_update_cwd)
    monkeypatch.setattr('workers.executor_agent.update_command', fake_update_command)
    monkeypatch.setattr('workers.executor_agent.run_subprocess', fake_run_subprocess)

    subdir = tmp_path / 'sub'
    subdir.mkdir()
    row = {
        'id': 1,
        'user_id': 'u1',
        'command': f'cd {subdir}',
        'cwd_snapshot': str(tmp_path),
        'env_snapshot': None,
    }

    handle_command(None, row)

    assert captured['cwd'] == str(subdir)
    assert captured['status'] == 'done'
    assert captured['output'] == ''
    assert captured['exit_code'] == 0


def test_handle_command_cd_nonexistent_path(tmp_path, monkeypatch):
    captured: dict = {}

    def fake_update_cwd(*args, **kwargs):  # pragma: no cover - should not run
        raise AssertionError('update_cwd should not be called')

    def fake_update_command(conn, cmd_id, status, output, exit_code):
        captured['status'] = status
        captured['output'] = output
        captured['exit_code'] = exit_code

    def fake_run_subprocess(*args, **kwargs):  # pragma: no cover - should not run
        raise AssertionError('run_subprocess should not be called')

    monkeypatch.setattr('workers.executor_agent.update_cwd', fake_update_cwd)
    monkeypatch.setattr('workers.executor_agent.update_command', fake_update_command)
    monkeypatch.setattr('workers.executor_agent.run_subprocess', fake_run_subprocess)

    row = {
        'id': 2,
        'user_id': 'u1',
        'command': 'cd missing',
        'cwd_snapshot': str(tmp_path),
        'env_snapshot': None,
    }

    handle_command(None, row)

    assert captured['status'] == 'failed'
    assert captured['exit_code'] == 1
    assert 'No such file or directory' in captured['output']


def test_handle_command_cd_with_extra_args_runs_subprocess(monkeypatch):
    captured: dict = {}

    def fake_run_subprocess(command, cwd, env):
        captured['command'] = command
        return 0, ''

    def fake_update_command(conn, cmd_id, status, output, exit_code):
        captured['status'] = status
        captured['output'] = output
        captured['exit_code'] = exit_code

    def fake_update_cwd(*args, **kwargs):  # pragma: no cover - should not run
        raise AssertionError('update_cwd should not be called')

    monkeypatch.setattr('workers.executor_agent.run_subprocess', fake_run_subprocess)
    monkeypatch.setattr('workers.executor_agent.update_command', fake_update_command)
    monkeypatch.setattr('workers.executor_agent.update_cwd', fake_update_cwd)

    row = {
        'id': 3,
        'user_id': 'u1',
        'command': 'cd /tmp extra',
        'cwd_snapshot': '.',
        'env_snapshot': None,
    }

    handle_command(None, row)

    assert captured['command'] == 'cd /tmp extra'
    assert captured['status'] == 'done'
    assert captured['exit_code'] == 0


def test_main_closes_connection_on_keyboard_interrupt(monkeypatch):
    closed = False

    class FakeConn:
        def close(self):
            nonlocal closed
            closed = True

    def fake_get_conn():
        return FakeConn()

    def fake_fetch_pending(conn):
        raise KeyboardInterrupt

    monkeypatch.setattr('workers.executor_agent.get_conn', fake_get_conn)
    monkeypatch.setattr('workers.executor_agent.setup_listener', lambda conn: None)
    monkeypatch.setattr('workers.executor_agent.fetch_pending', fake_fetch_pending)
    monkeypatch.setattr('workers.executor_agent.wait_for_notify', lambda conn, timeout: None)

    with pytest.raises(KeyboardInterrupt):
        workers.executor_agent.main()

    assert closed
