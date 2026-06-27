import cli.shell_cli as sc


def test_exec_command_posts_with_prefer_headers(monkeypatch, capsys):
    class Resp:
        text = ""
        status_code = 200
        reason = "OK"

        def raise_for_status(self):
            pass

        def json(self):
            return {"status": "ok"}

    def fake_post(url, json, timeout, headers):
        assert url == "http://example/rpc/submit_command"
        # PostgREST maps JSON body keys to function argument names, so the
        # keys must match submit_command(p_user_id, p_command).
        assert json == {"p_user_id": "u1", "p_command": "ls"}
        assert timeout == 5
        assert headers["Prefer"] == "return=representation"
        assert headers["Accept"] == "application/json"
        return Resp()

    monkeypatch.setattr(sc.requests, "post", fake_post)

    rc = sc.exec_command("http://example", "u1", "ls", timeout=5)
    assert rc == 0
    captured = capsys.readouterr()
    assert captured.out.strip() == "{'status': 'ok'}"


def test_fork_session_posts_prefixed_arguments(monkeypatch, capsys):
    class Resp:
        text = ""
        status_code = 200
        reason = "OK"

        def raise_for_status(self):
            pass

        def json(self):
            return {"fork_session": "u1"}

    def fake_post(url, json, timeout, headers):
        assert url == "http://example/rpc/fork_session"
        assert json == {"p_user_id": "u1", "p_source_command_id": 7}
        return Resp()

    monkeypatch.setattr(sc.requests, "post", fake_post)

    rc = sc.fork_session("http://example", "u1", 7)
    assert rc == 0


def test_replay_session_posts_prefixed_arguments(monkeypatch, capsys):
    class Resp:
        text = ""
        status_code = 200
        reason = "OK"

        def raise_for_status(self):
            pass

        def json(self):
            return {"replay_session": "00000000-0000-0000-0000-000000000000"}

    def fake_post(url, json, timeout, headers):
        assert url == "http://example/rpc/replay_session"
        assert json == {"p_user_id": "u1", "p_start_id": 5}
        return Resp()

    monkeypatch.setattr(sc.requests, "post", fake_post)

    rc = sc.replay_session("http://example", "u1", 5)
    assert rc == 0


def test_main_replay_dispatches_user_and_start(monkeypatch):
    captured = {}

    def fake_replay(base_url, user_id, start_id, timeout):
        captured["args"] = (base_url, user_id, start_id)
        return 0

    monkeypatch.setattr(sc, "replay_session", fake_replay)
    rc = sc.main(
        ["--base-url", "http://example", "replay", "--user", "u1", "--start", "5"]
    )
    assert rc == 0
    assert captured["args"] == ("http://example", "u1", 5)


def test_exec_command_handles_empty_response(monkeypatch, capsys):
    class Resp:
        text = ""
        status_code = 204
        reason = "No Content"

        def raise_for_status(self):
            pass

        def json(self):  # pragma: no cover - exercised in except branch
            raise ValueError("No JSON")

    monkeypatch.setattr(
        sc.requests,
        "post",
        lambda *args, **kwargs: Resp(),
    )

    rc = sc.exec_command("http://example", "u1", "ls")
    assert rc == 0
    captured = capsys.readouterr()
    assert captured.out.strip() == "204 No Content"

def test_tail_output_stops_after_max_polls(monkeypatch):
    calls = []

    class Resp:
        def raise_for_status(self):
            pass
        def json(self):
            return []

    def fake_get(*args, **kwargs):
        calls.append(1)
        return Resp()

    monkeypatch.setattr(sc.requests, 'get', fake_get)
    monkeypatch.setattr(sc.time, 'sleep', lambda _: None)

    rc = sc.tail_output('http://example', 'u1', interval=0, max_polls=2)
    assert rc == 0
    assert len(calls) == 2


def test_tail_output_prints_text_when_json_missing(monkeypatch, capsys):
    class Resp:
        text = "plain text"
        status_code = 200
        reason = "OK"

        def raise_for_status(self):
            pass

        def json(self):  # pragma: no cover - exercised in except branch
            raise ValueError("not json")

    monkeypatch.setattr(sc.requests, 'get', lambda *args, **kwargs: Resp())
    monkeypatch.setattr(sc.time, 'sleep', lambda _: None)

    rc = sc.tail_output('http://example', 'u1', interval=0, max_polls=1)
    assert rc == 0
    captured = capsys.readouterr()
    assert captured.out.strip() == 'plain text'


def test_tail_output_waits_for_terminal_status(monkeypatch, capsys):
    responses = [
        [
            {
                "id": 1,
                "command": "echo hi",
                "status": "pending",
            }
        ],
        [
            {
                "id": 1,
                "command": "echo hi",
                "status": "done",
                "output": "hi",
                "exit_code": 0,
            }
        ],
        [],
    ]

    call_params: list[dict[str, object]] = []

    class Resp:
        def __init__(self, idx: int) -> None:
            self._idx = idx

        def raise_for_status(self):
            pass

        def json(self):
            return responses[self._idx]

    def fake_get(url, params, timeout):
        idx = len(call_params)
        call_params.append(dict(params))
        return Resp(idx if idx < len(responses) else len(responses) - 1)

    monkeypatch.setattr(sc.requests, 'get', fake_get)
    monkeypatch.setattr(sc.time, 'sleep', lambda _: None)

    rc = sc.tail_output('http://example', 'u1', interval=0, max_polls=3)
    assert rc == 0

    captured = capsys.readouterr()
    assert captured.out.splitlines() == [
        '$ echo hi',
        'hi',
        '(exit 0)',
    ]

    assert call_params == [
        {'p_user_id': 'eq.u1'},
        {'p_user_id': 'eq.u1'},
        {'p_user_id': 'eq.u1', 'p_since_id': 1},
    ]
