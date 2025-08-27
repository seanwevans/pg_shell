import cli.shell_cli as sc

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
