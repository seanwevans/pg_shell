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


def test_command_and_response_markup_is_rendered_only_as_text(ui_base_url):
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
                "**/rpc/latest_output**",
                lambda route: route.fulfill(
                    json=[
                        {
                            "command": command,
                            "output": command_output,
                            "status": "done",
                            "exit_code": 0,
                        }
                    ]
                ),
            )
            page.route(
                "**/rpc/submit_command",
                lambda route: route.fulfill(json=submission),
            )

            page.goto(f"{ui_base_url}/index.html")
            expect(page.locator(".command")).to_have_text(command)
            expect(page.locator(".command-output")).to_have_text(command_output)

            assert page.locator("#output button, #output img").count() == 0
            assert page.evaluate("Boolean(window.commandRan || window.outputRan)") is False

            page.locator("input[name=p_command]").fill(command)
            page.locator("form").press("Enter")
            expect(page.locator(".submission-response")).to_have_text(submission)

            assert page.locator("#output svg").count() == 0
            assert page.evaluate("Boolean(window.submissionRan)") is False
