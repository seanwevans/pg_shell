import os
import pwd
import shutil
import time

import pytest
import workers.executor_agent
from workers.executor_agent import run_subprocess, handle_command


@pytest.fixture(autouse=True)
def configured_executor(monkeypatch, tmp_path):
    """Use the test runner account while exercising the production controls."""
    account = pwd.getpwuid(os.geteuid())
    if account.pw_uid == 0:
        account = pwd.getpwnam("nobody")
        tmp_path.chmod(0o777)
        parent = tmp_path.parent
        while parent != parent.parent and parent != parent.parent.parent:
            parent.chmod(0o755)
            if parent == tmp_path.parents[2]:
                break
            parent = parent.parent
    monkeypatch.setenv("EXECUTOR_USER", account.pw_name)
    command_path = workers.executor_agent.DEFAULT_COMMAND_PATH
    commands = [
        shutil.which("python3", path=command_path),
        shutil.which("sleep", path=command_path),
    ]
    monkeypatch.setenv(
        "EXECUTOR_ALLOWED_COMMANDS", os.pathsep.join(filter(None, commands))
    )


def test_run_subprocess_combines_output(tmp_path):
    cmd = "python3 -c 'import sys;print(\"out\");print(\"err\", file=sys.stderr)'"
    exit_code, output = run_subprocess(cmd, str(tmp_path), None)
    assert exit_code == 0
    assert "out" in output
    assert "err" in output


def test_run_subprocess_truncates_output(monkeypatch, tmp_path):
    monkeypatch.setattr(workers.executor_agent, "MAX_OUTPUT_BYTES", 100)
    cmd = "python3 -c 'import sys;sys.stdout.write(\"a\"*200)'"
    exit_code, output = run_subprocess(cmd, str(tmp_path), None)
    assert exit_code == 0
    assert output.endswith("...[truncated]")
    assert output.startswith("a" * 100)
    assert len(output) == 100 + len("...[truncated]")


def test_run_subprocess_preserves_exit_code(monkeypatch, tmp_path):
    monkeypatch.setattr(workers.executor_agent, "MAX_OUTPUT_BYTES", 50)
    cmd = "python3 -c 'import sys;sys.stdout.write(\"b\"*100);sys.exit(3)'"
    exit_code, output = run_subprocess(cmd, str(tmp_path), None)
    assert exit_code == 3
    assert output.endswith("...[truncated]")


def test_run_subprocess_times_out(monkeypatch, tmp_path):
    # SPEC section 7 requires command timeouts to be enforced. A command that
    # outruns COMMAND_TIMEOUT is killed and reported with exit code 124.
    monkeypatch.setattr(workers.executor_agent, "COMMAND_TIMEOUT", 1)
    exit_code, output = run_subprocess("sleep 5", str(tmp_path), None)
    assert exit_code == 124
    assert "Timed out after 1s" in output


@pytest.mark.skipif(os.name != "posix", reason="process-group signaling is POSIX-specific")
def test_run_subprocess_timeout_kills_descendant_with_inherited_pipe(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(workers.executor_agent, "COMMAND_TIMEOUT", 1)
    pid_file = tmp_path / "descendant.pid"
    script = (
        "import pathlib, subprocess, time; "
        "child = subprocess.Popen(['sleep', '30']); "
        f"pathlib.Path({str(pid_file)!r}).write_text(str(child.pid)); "
        "time.sleep(30)"
    )

    started = time.monotonic()
    exit_code, output = run_subprocess(
        f"python3 -c {script!r}", str(tmp_path), None
    )
    elapsed = time.monotonic() - started

    assert exit_code == 124
    assert "Timed out after 1s" in output
    assert elapsed < 3
    descendant_pid = int(pid_file.read_text())
    status_file = f"/proc/{descendant_pid}/status"
    for _ in range(20):
        if not os.path.exists(status_file):
            break
        if "State:\tZ" in open(status_file).read():
            break
        time.sleep(0.05)
    else:
        pytest.fail(f"descendant process {descendant_pid} is still running")


def test_run_subprocess_passes_env_snapshot(monkeypatch, tmp_path):
    cmd = "python3 -c 'import os;print(os.environ[\"PGS_TEST_VAR\"])'"
    exit_code, output = run_subprocess(cmd, str(tmp_path), {"PGS_TEST_VAR": "hello"})
    assert exit_code == 0
    assert "hello" in output


def test_run_subprocess_does_not_expose_worker_secrets(monkeypatch, tmp_path):
    monkeypatch.setenv("WORKER_API_SECRET", "worker-only")
    monkeypatch.setenv("DATABASE_URL", "postgresql://secret")
    script = (
        'import os;print(os.getenv("WORKER_API_SECRET"));'
        'print(os.getenv("DATABASE_URL"))'
    )
    cmd = f"python3 -c '{script}'"

    exit_code, output = run_subprocess(cmd, str(tmp_path), None)

    assert exit_code == 0
    assert output.splitlines() == ["None", "None"]


@pytest.mark.parametrize("reserved", ["DATABASE_URL", "PG_CONN"])
def test_run_subprocess_rejects_reserved_snapshot_variables(
    monkeypatch, tmp_path, reserved
):
    with pytest.raises(ValueError, match="reserved environment variable"):
        run_subprocess("python3 -c 'pass'", str(tmp_path), {reserved: "secret"})


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


def test_handle_command_cd_relative_within_root_succeeds(tmp_path, monkeypatch):
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

    monkeypatch.setenv('SHELL_ROOT', str(tmp_path))

    subdir = tmp_path / 'sub'
    subdir.mkdir()
    row = {
        'id': 1,
        'user_id': 'u1',
        'command': 'cd sub',
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

    monkeypatch.setenv('SHELL_ROOT', str(tmp_path))

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


def test_handle_command_cd_dotdot_escaping_root_fails(tmp_path, monkeypatch):
    captured: dict = {}

    def fake_update_cwd(*args, **kwargs):  # pragma: no cover - should not run
        raise AssertionError('update_cwd should not be called')

    def fake_update_command(conn, cmd_id, status, output, exit_code):
        captured['status'] = status
        captured['output'] = output
        captured['exit_code'] = exit_code

    def fake_run_subprocess(*args, **kwargs):  # pragma: no cover - should not run
        raise AssertionError('run_subprocess should not be called')

    monkeypatch.setenv('SHELL_ROOT', str(tmp_path))
    monkeypatch.setattr('workers.executor_agent.update_cwd', fake_update_cwd)
    monkeypatch.setattr('workers.executor_agent.update_command', fake_update_command)
    monkeypatch.setattr('workers.executor_agent.run_subprocess', fake_run_subprocess)

    row = {
        'id': 5,
        'user_id': 'u1',
        'command': 'cd ..',
        'cwd_snapshot': str(tmp_path),
        'env_snapshot': None,
    }

    handle_command(None, row)

    assert captured['status'] == 'failed'
    assert captured['exit_code'] == 1
    assert 'Permission denied' in captured['output']


def test_handle_command_cd_absolute_outside_root_fails(tmp_path, monkeypatch):
    captured: dict = {}

    def fake_update_cwd(*args, **kwargs):  # pragma: no cover - should not run
        raise AssertionError('update_cwd should not be called')

    def fake_update_command(conn, cmd_id, status, output, exit_code):
        captured['status'] = status
        captured['output'] = output
        captured['exit_code'] = exit_code

    def fake_run_subprocess(*args, **kwargs):  # pragma: no cover - should not run
        raise AssertionError('run_subprocess should not be called')

    monkeypatch.setenv('SHELL_ROOT', str(tmp_path))
    monkeypatch.setattr('workers.executor_agent.update_cwd', fake_update_cwd)
    monkeypatch.setattr('workers.executor_agent.update_command', fake_update_command)
    monkeypatch.setattr('workers.executor_agent.run_subprocess', fake_run_subprocess)

    row = {
        'id': 6,
        'user_id': 'u1',
        'command': 'cd /tmp',
        'cwd_snapshot': str(tmp_path),
        'env_snapshot': None,
    }

    handle_command(None, row)

    assert captured['status'] == 'failed'
    assert captured['exit_code'] == 1
    assert 'Permission denied' in captured['output']


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


def test_handle_command_malformed_command(monkeypatch):
    captured: dict = {}

    def fake_update_command(conn, cmd_id, status, output, exit_code):
        captured['status'] = status
        captured['output'] = output
        captured['exit_code'] = exit_code

    def fake_run_subprocess(*args, **kwargs):  # pragma: no cover - should not run
        raise AssertionError('run_subprocess should not be called')

    def fake_update_cwd(*args, **kwargs):  # pragma: no cover - should not run
        raise AssertionError('update_cwd should not be called')

    monkeypatch.setattr('workers.executor_agent.update_command', fake_update_command)
    monkeypatch.setattr('workers.executor_agent.run_subprocess', fake_run_subprocess)
    monkeypatch.setattr('workers.executor_agent.update_cwd', fake_update_cwd)

    row = {
        'id': 4,
        'user_id': 'u1',
        'command': 'echo "unterminated',
        'cwd_snapshot': '.',
        'env_snapshot': None,
    }

    handle_command(None, row)

    assert captured['status'] == 'failed'
    assert captured['exit_code'] == 1
    assert 'No closing quotation' in captured['output']


def test_handle_command_missing_binary_marks_failed(monkeypatch, tmp_path):
    captured: dict = {}

    def fake_update_command(conn, cmd_id, status, output, exit_code):
        captured['status'] = status
        captured['output'] = output
        captured['exit_code'] = exit_code

    monkeypatch.setattr('workers.executor_agent.update_command', fake_update_command)

    row = {
        'id': 7,
        'user_id': 'u1',
        'command': 'definitely_not_a_real_binary_xyz',
        'cwd_snapshot': str(tmp_path),
        'env_snapshot': None,
    }

    handle_command(None, row)

    assert captured['status'] == 'failed'
    assert captured['exit_code'] == 1
    assert 'not in EXECUTOR_ALLOWED_COMMANDS' in captured['output']


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
