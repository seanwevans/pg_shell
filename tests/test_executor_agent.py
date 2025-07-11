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
