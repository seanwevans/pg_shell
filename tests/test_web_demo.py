"""End-to-end coverage for the browser-hosted pg_shell instance in ``web/``.

The demo installs this repository's SQL into PostgreSQL compiled to
WebAssembly and drives it through htmx, so these tests exercise the real
schema, the real RPCs and the real HTML fragments -- only the executor and
the HTTP transport are browser stand-ins.

Requires the site to have been built (``node web/build.mjs``); the tests skip
when it has not been, so a checkout without Node still runs the suite.
"""

import contextlib
import functools
import glob
import http.server
import os
import threading
from pathlib import Path

import pytest

playwright_sync_api = pytest.importorskip(
    "playwright.sync_api",
    reason="Playwright is required for browser-level UI tests",
)
expect = playwright_sync_api.expect
sync_playwright = playwright_sync_api.sync_playwright


SITE_DIR = Path(__file__).parents[1] / "_site"
# Booting a PostgreSQL build in a headless browser is slow on a cold cache.
BOOT_TIMEOUT_MS = 180_000
COMMAND_TIMEOUT_MS = 30_000


class _SiteHandler(http.server.SimpleHTTPRequestHandler):
    """Static handler that serves the media types PGlite needs.

    ``instantiateStreaming`` rejects ``pglite.wasm`` unless it arrives as
    ``application/wasm``, which Python's table does not know about.
    """

    extensions_map = {
        **http.server.SimpleHTTPRequestHandler.extensions_map,
        ".js": "text/javascript",
        ".wasm": "application/wasm",
        ".data": "application/octet-stream",
        ".sql": "text/plain",
    }

    def log_message(self, *args):  # noqa: D102 - keep pytest output readable
        pass


def _launch_chromium(playwright):
    """Launch Chromium, tolerating a browser build that predates this
    Playwright release's pinned revision (see tests/test_html_ui.py)."""
    try:
        return playwright.chromium.launch()
    except Exception:
        root = os.environ.get("PLAYWRIGHT_BROWSERS_PATH", "/opt/pw-browsers")
        patterns = (
            "chromium-*/chrome-linux/chrome",
            "chromium_headless_shell-*/chrome-linux/headless_shell",
            "chromium",
        )
        for pattern in patterns:
            for path in sorted(glob.glob(os.path.join(root, pattern))):
                if os.access(path, os.X_OK):
                    return playwright.chromium.launch(executable_path=path)
        raise


@pytest.fixture(scope="module")
def site_base_url():
    if not (SITE_DIR / "index.html").exists():
        pytest.skip("run `node web/build.mjs` to build the demo before these tests")
    handler = functools.partial(_SiteHandler, directory=SITE_DIR)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.fixture(scope="module")
def browser():
    with sync_playwright() as playwright:
        try:
            launched = _launch_chromium(playwright)
        except Exception as error:
            pytest.skip(f"Playwright Chromium is unavailable: {error}")
        with contextlib.closing(launched):
            yield launched


def _boot(browser, base_url, **context_options):
    """Open the demo in a fresh context and wait for it to come up.

    A fresh context means an empty IndexedDB, so the page always installs the
    schema from ``sql/`` rather than reusing a database a previous run
    persisted.
    """
    page = browser.new_context(**context_options).new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.on(
        "console",
        lambda message: errors.append(message.text) if message.type == "error" else None,
    )
    page.goto(f"{base_url}/index.html")
    page.wait_for_selector("#command-input:not([disabled])", timeout=BOOT_TIMEOUT_MS)
    return page, errors


@pytest.fixture(scope="module")
def demo(browser, site_base_url):
    """A booted demo page shared by the tests in this module.

    ``bypass_csp`` is needed because Playwright compiles ``wait_for_function``
    predicates in the page's main world, which the page's own
    ``script-src 'self' 'wasm-unsafe-eval'`` refuses. The policy itself is
    covered by ``test_page_boots_under_its_own_content_security_policy``, and
    dropping it here makes the escaping test stricter: an unescaped fragment
    would actually execute rather than being stopped by the policy.
    """
    page, errors = _boot(browser, site_base_url, bypass_csp=True)
    yield page, errors
    page.context.close()


def _run(page, command):
    """Submit a command and wait for the executor to reach a terminal status.

    The newest command is always the last entry in the transcript, because
    ``latest_output_html`` returns its page oldest-first.
    """
    page.fill("#command-input", command)
    page.press("#command-input", "Enter")
    page.wait_for_function(
        """(text) => {
             const rows = document.querySelectorAll('#output .command-result');
             const row = rows[rows.length - 1];
             return Boolean(
               row &&
               row.querySelector('.command').textContent === text &&
               ['done', 'failed'].includes(row.dataset.status)
             );
           }""",
        arg=command,
        timeout=COMMAND_TIMEOUT_MS,
    )
    return page.locator("#output .command-result").last


def _query(page, sql, params=None):
    """Run SQL against the page's own database and return the rows."""
    return page.evaluate(
        "async ([sql, params]) => (await window.pgShell.db.query(sql, params)).rows",
        [sql, params or []],
    )


def test_page_boots_a_real_postgres_and_seeds_a_session(demo):
    page, errors = demo
    expect(page.locator("#fact-version")).to_contain_text("wasm")
    # The banner is PostgreSQL's own, so a major version proves a server booted.
    assert page.locator("#fact-version").inner_text().split(".")[0].isdigit()
    expect(page.locator("#fact-session")).not_to_have_text("—")
    assert errors == [], f"console errors during boot: {errors}"


def test_page_boots_under_its_own_content_security_policy(browser, site_base_url):
    """The published page enforces a CSP, and a policy that blocked the
    WebAssembly build or a module would break it for every visitor."""
    page, errors = _boot(browser, site_base_url)
    try:
        assert errors == [], f"console errors under the page's own CSP: {errors}"
        policy = page.get_attribute(
            'meta[http-equiv="Content-Security-Policy"]', "content"
        )
        assert "script-src 'self' 'wasm-unsafe-eval'" in policy
        assert "unsafe-inline" not in policy
        assert "'unsafe-eval'" not in policy
    finally:
        page.context.close()


def test_command_round_trips_through_the_database(demo):
    page, _ = demo
    entry = _run(page, "echo hello from postgres")
    expect(entry.locator(".command")).to_have_text("echo hello from postgres")
    expect(entry.locator(".command-output")).to_have_text("hello from postgres")
    expect(entry.locator(".exit-code")).to_have_text("0")
    expect(entry).to_have_attribute("data-status", "done")


def test_transcript_is_rebuilt_from_the_database_after_a_reload(demo):
    page, _ = demo
    _run(page, "echo persisted")
    page.reload()
    page.wait_for_selector("#command-input:not([disabled])", timeout=BOOT_TIMEOUT_MS)
    expect(page.locator("#output")).to_contain_text("echo persisted")


def test_failing_command_is_recorded_as_failed(demo):
    page, _ = demo
    entry = _run(page, "cat missing.txt")
    expect(entry).to_have_attribute("data-status", "failed")
    expect(entry.locator(".command-output")).to_contain_text(
        "cat: missing.txt: No such file or directory"
    )
    expect(entry.locator(".exit-code")).to_have_text("1")


def test_command_outside_the_allowlist_is_refused(demo):
    page, _ = demo
    entry = _run(page, "curl https://example.com")
    expect(entry).to_have_attribute("data-status", "failed")
    expect(entry.locator(".command-output")).to_contain_text(
        "command is not in EXECUTOR_ALLOWED_COMMANDS: curl"
    )


def test_cd_persists_in_the_environments_row(demo):
    page, _ = demo
    _run(page, "cd notes")
    entry = _run(page, "pwd")
    expect(entry.locator(".command-output")).to_have_text("/home/sandbox/notes")
    _run(page, "cd ..")


def test_cd_cannot_escape_the_shell_root(demo):
    page, _ = demo
    entry = _run(page, "cd /etc")
    expect(entry).to_have_attribute("data-status", "failed")
    expect(entry.locator(".command-output")).to_contain_text("Permission denied")


def test_untrusted_command_text_is_never_rendered_as_markup(demo):
    page, _ = demo
    _run(page, "echo '<img src=x onerror=window.pwned=true>'")
    assert page.locator("#output img").count() == 0
    assert page.evaluate("Boolean(window.pwned)") is False


def test_audit_log_shows_the_command_rows(demo):
    page, _ = demo
    page.click("#tabbtn-audit")
    expect(page.locator("#audit-table")).to_contain_text("echo hello from postgres")
    expect(page.locator("#audit-table td.status-done").first).to_be_visible()


def test_sql_console_queries_the_same_database(demo):
    page, _ = demo
    page.click("#tabbtn-sql")
    page.fill(
        "#sql-input",
        "SELECT count(*) AS commands, count(*) FILTER (WHERE status = 'failed') AS failed FROM commands",
    )
    page.click("#action-run-sql")
    expect(page.locator("#sql-status")).to_contain_text("1 row(s)")
    expect(page.locator("#sql-table")).to_contain_text("commands")


def test_inspector_controls_do_not_queue_a_command(demo):
    """The prompt triggers on "submit from:form", which htmx resolves against
    every form in the document -- so the inspector must not contain one."""
    page, _ = demo
    session = page.evaluate("window.pgShell.session.sessionId")
    before = _query(
        page, "SELECT count(*) AS n FROM commands WHERE session_id = $1", [session]
    )[0]["n"]

    page.click("#tabbtn-sql")
    page.fill("#sql-input", "SELECT 1 AS one;")
    page.click("#action-run-sql")
    expect(page.locator("#sql-status")).to_contain_text("1 row(s)")

    after = _query(
        page, "SELECT count(*) AS n FROM commands WHERE session_id = $1", [session]
    )[0]["n"]
    assert after == before
    assert page.locator("form").count() == 1


def test_replay_requeues_the_session_history(demo):
    page, _ = demo
    session = page.evaluate("window.pgShell.session.sessionId")
    replays = _query(
        page,
        "SELECT count(*) AS n FROM commands WHERE session_id = $1 AND replay_of_command_id IS NOT NULL",
        [session],
    )
    assert replays[0]["n"] == 0

    page.click("#tabbtn-sessions")
    page.click("#action-replay")

    page.wait_for_function(
        """async (session) => {
             const result = await window.pgShell.db.query(
               `SELECT count(*) AS n, count(DISTINCT replay_run_id) AS runs
                  FROM commands
                 WHERE session_id = $1 AND replay_of_command_id IS NOT NULL`,
               [session]
             );
             return Number(result.rows[0].n) > 0 && Number(result.rows[0].runs) === 1;
           }""",
        arg=session,
        timeout=COMMAND_TIMEOUT_MS,
    )


def test_fork_starts_a_new_session_from_a_command_snapshot(demo):
    page, _ = demo
    original = page.evaluate("window.pgShell.session.sessionId")
    source = _query(
        page,
        "SELECT id, cwd_snapshot FROM commands WHERE session_id = $1 ORDER BY id DESC LIMIT 1",
        [original],
    )[0]

    page.click("#tabbtn-sessions")
    page.click("#action-fork")
    page.wait_for_function(
        "(previous) => window.pgShell.session.sessionId !== previous",
        arg=original,
        timeout=COMMAND_TIMEOUT_MS,
    )

    forked = page.evaluate("window.pgShell.session.sessionId")
    environment = _query(
        page, "SELECT cwd FROM environments WHERE session_id = $1", [forked]
    )
    # fork_session() rebuilds the environment from the source command's
    # snapshot rather than from the session's current state.
    assert environment[0]["cwd"] == source["cwd_snapshot"]
    expect(page.locator("#output .command-result")).to_have_count(0)

    entry = _run(page, "pwd")
    expect(entry.locator(".command-output")).to_have_text(source["cwd_snapshot"])
