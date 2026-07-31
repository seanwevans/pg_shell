"""Integration coverage for requests sent to a PostgREST-compatible endpoint.

When ``TEST_POSTGREST_URL`` points at a live PostgREST instance the test uses
it directly. Otherwise an in-process, PostgREST-compatible shim resolves
``GET /rpc/latest_output`` by invoking the real ``latest_output`` SQL function,
so the CLI's request construction and the function's contract are exercised
without provisioning an external gateway.
"""

import json
import os
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import psycopg2
from psycopg2.extras import RealDictCursor
import pytest

from cli.shell_cli import tail_output


def _make_handler(dsn):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path != "/rpc/latest_output":
                self.send_error(404)
                return
            params = parse_qs(parsed.query)
            try:
                user_id = params["p_user_id"][0]
                session_id = params["p_session_id"][0]
            except (KeyError, IndexError):
                self.send_error(400)
                return
            since_id = int(params.get("p_since_id", ["0"])[0])

            conn = psycopg2.connect(dsn)
            try:
                conn.autocommit = True
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT * FROM latest_output(%s, %s, %s)",
                        (user_id, session_id, since_id),
                    )
                    rows = cur.fetchall()
            finally:
                conn.close()

            body = json.dumps(rows, default=str).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # pragma: no cover - silence test server
            pass

    return Handler


@pytest.fixture
def postgrest_base_url():
    """Yield a base URL exposing PostgREST's ``/rpc`` interface.

    Prefers a live PostgREST instance via ``TEST_POSTGREST_URL``; falls back to
    an in-process shim backed by the test database.
    """
    configured = os.environ.get("TEST_POSTGREST_URL")
    if configured:
        yield configured.rstrip("/")
        return

    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not set")

    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def test_tail_output_invokes_latest_output_through_postgrest(
    db_conn, postgrest_base_url, capsys
):
    """A CLI-generated query resolves latest_output(UUID, UUID, INTEGER)."""
    user_id = str(uuid.uuid4())
    with db_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO users(id, username) VALUES (%s, %s)",
            (user_id, f"postgrest-{user_id}"),
        )
        cur.execute(
            "INSERT INTO environments(user_id) VALUES (%s) RETURNING session_id",
            (user_id,),
        )
        session_id = str(cur.fetchone()[0])
        cur.execute(
            "SELECT submit_command(%s, %s, %s)",
            (user_id, session_id, "echo integration"),
        )
        command_id = cur.fetchone()[0]
        cur.execute(
            """
            UPDATE commands
               SET output = 'integration', exit_code = 0, status = 'done',
                   completed_at = now()
             WHERE id = %s
            """,
            (command_id,),
        )

    assert tail_output(
        postgrest_base_url, user_id, session_id, since=0, interval=0, max_polls=1
    ) == 0
    assert capsys.readouterr().out.splitlines() == [
        "$ echo integration",
        "integration",
        "(exit 0)",
    ]
