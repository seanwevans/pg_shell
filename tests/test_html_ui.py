"""Browser-level security coverage for the htmx UI."""

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


HTML_DIR = Path(__file__).parents[1] / "html"


def _launch_chromium(playwright):
    """Launch Chromium, tolerating a browser build that predates this
    Playwright release's pinned revision.

    ``playwright.chromium.launch()`` resolves an exact browser build number and
    fails when only a differently numbered Chromium is present in the image (a
    common situation when the browser is provisioned separately from the pinned
    Python package). Fall back to any executable Chromium found under
    ``PLAYWRIGHT_BROWSERS_PATH`` before giving up.
    """
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
def ui_base_url():
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler,
        directory=HTML_DIR,
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


def _escape_fragment(value):
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def test_database_fragments_are_swapped_and_untrusted_values_remain_text(ui_base_url):
    command = '<button onclick="window.commandRan=true">run me</button>'
    command_output = '<img src="missing" onerror="window.outputRan=true">'
    submission = '<svg onload="window.submissionRan=true"></svg>'

    with sync_playwright() as playwright:
        try:
            browser = _launch_chromium(playwright)
        except Exception as error:
            pytest.skip(f"Playwright Chromium is unavailable: {error}")

        with contextlib.closing(browser):
            page = browser.new_page()
            page.route(
                "**/rpc/latest_output_html**",
                lambda route: route.fulfill(
                    content_type="text/html",
                    body=(
                        '<article class="command-result">'
                        f'<pre class="command">{_escape_fragment(command)}</pre>'
                        '<pre class="command-output">'
                        f'{_escape_fragment(command_output)}</pre>'
                        '<span class="command-status">done</span>'
                        '<span class="exit-code">0</span></article>'
                    ),
                ),
            )
            page.route(
                "**/rpc/submit_command_html",
                lambda route: route.fulfill(
                    content_type="text/html",
                    body=(
                        '<article class="command-result">'
                        f'<pre class="command">{_escape_fragment(submission)}</pre>'
                        '<span class="command-status">pending</span></article>'
                    ),
                ),
            )

            page.goto(f"{ui_base_url}/index.html")
            expect(page.locator(".command")).to_have_text(command)
            expect(page.locator(".command-output")).to_have_text(command_output)

            assert page.locator("#output button, #output img").count() == 0
            assert page.evaluate("Boolean(window.commandRan || window.outputRan)") is False

            page.locator("input[name=p_command]").fill(command)
            page.locator("form").press("Enter")
            expect(page.locator(".command")).to_have_count(2)
            expect(page.locator(".command").last).to_have_text(submission)

            assert page.locator("#output svg").count() == 0
            assert page.evaluate("Boolean(window.submissionRan)") is False


def test_ui_uses_native_htmx_swaps_without_rendering_javascript():
    index = (HTML_DIR / "index.html").read_text()

    assert "latest_output_html" in index
    assert "submit_command_html" in index
    assert 'hx-swap="innerHTML"' in index
    assert 'hx-swap="beforeend"' in index
    assert index.count('hx-headers=\'{"Accept": "text/html"}\'') == 2
    assert "app.js" not in index
    assert "json-enc" not in index
