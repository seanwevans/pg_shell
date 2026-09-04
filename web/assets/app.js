// Wires the page together: bring up PostgreSQL, install pg_shell into it, put
// the PostgREST shim in front of htmx, start the browser executor, and point
// the UI at the seeded session.

import { boot, reset } from "./db.js";
import { installPostgrestShim } from "./postgrest.js";
import { BrowserExecutor, ALLOWED_COMMANDS } from "./executor.js";
import { Inspector } from "./inspector.js";

const bootOverlay = document.getElementById("boot");
const bootStatus = document.getElementById("boot-status");

function progress(message) {
  bootStatus.textContent = message;
}

function fail(error) {
  progress(`failed: ${error.message ?? error}`);
  bootOverlay.querySelector(".boot-bar").style.animation = "none";
  console.error(error);
}

// The transcript fragments come from the database with a fixed shape, so the
// status is copied onto the article where CSS can key off it.
function markStatuses(root) {
  for (const article of root.querySelectorAll(".command-result")) {
    const status = article.querySelector(".command-status")?.textContent.trim();
    if (status) article.dataset.status = status;
  }
}

// htmx captures an element's request path when it sets up that element's
// triggers, so pointing the transcript at another session means giving htmx a
// new element rather than editing the attribute in place. Replacing the node
// also drops the old poll: htmx stops a poll whose element has left the
// document, and cloning without children empties the transcript.
function pointAtSession(session) {
  const current = document.getElementById("output");
  const replacement = current.cloneNode(false);
  const query = new URLSearchParams({
    p_user_id: session.userId,
    p_session_id: session.sessionId,
  });
  replacement.setAttribute("hx-get", `/rpc/latest_output_html?${query}`);
  current.replaceWith(replacement);
  window.htmx?.process(replacement);

  document.querySelector('#prompt input[name="p_user_id"]').value = session.userId;
  document.querySelector('#prompt input[name="p_session_id"]').value = session.sessionId;
  document.getElementById("fact-session").textContent = session.sessionId.slice(0, 8);
}

// htmx is held off with hx-disable until the database is up, so its `load`
// trigger cannot fire a poll against the placeholder ids in the markup.
function enableHtmx() {
  for (const element of document.querySelectorAll("[hx-disable]")) {
    element.removeAttribute("hx-disable");
  }
  const input = document.getElementById("command-input");
  input.disabled = false;
  input.placeholder = "try: ls -l   ·   cat README.md   ·   help";
  document.querySelector('#prompt button[type="submit"]').disabled = false;
  window.htmx.process(document.body);
}

async function main() {
  let inspector;
  let context;
  try {
    context = await boot({ onProgress: progress });
  } catch (error) {
    fail(error);
    return;
  }

  const { db, version } = context;
  let session = { userId: context.userId, sessionId: context.sessionId };

  progress("starting the executor");
  installPostgrestShim(db);

  const executor = new BrowserExecutor(db, {
    onEvent: (event) => {
      if (event.kind === "completed") inspector?.refresh();
      if (event.kind === "error") console.error(event.message);
    },
  });
  await executor.start();

  document.getElementById("fact-version").textContent =
    version.replace(/^PostgreSQL ([0-9.]+).*$/, "$1") + " · wasm";
  document.getElementById("fact-executor").textContent =
    `browser · ${ALLOWED_COMMANDS.length} commands`;

  inspector = new Inspector(db, session, {
    onSwitchSession(next) {
      session = next;
      pointAtSession(session);
      inspector.setSession(session);
    },
    async onReset() {
      const seeded = await reset(db);
      executor.fs.reset();
      session = seeded;
      pointAtSession(session);
      inspector.setSession(session);
    },
  });

  pointAtSession(session);
  inspector.attach();
  enableHtmx();

  document.body.addEventListener("htmx:afterSwap", (event) => {
    markStatuses(event.target);
    const output = document.getElementById("output");
    if (event.target === output || output.contains(event.target)) {
      output.scrollTop = output.scrollHeight;
    }
  });

  const commandInput = document.getElementById("command-input");
  document.getElementById("prompt").addEventListener("htmx:afterRequest", () => {
    commandInput.value = "";
    commandInput.focus();
    inspector.refresh();
  });

  const rpcError = document.getElementById("rpc-error");
  document.body.addEventListener("htmx:responseError", (event) => {
    let message = `RPC failed with HTTP ${event.detail.xhr.status}`;
    try {
      message = JSON.parse(event.detail.xhr.response).message;
    } catch {
      /* keep the status-code message */
    }
    rpcError.textContent = message;
    rpcError.hidden = false;
  });
  document.body.addEventListener("htmx:afterRequest", (event) => {
    if (event.detail.successful) rpcError.hidden = true;
  });

  // Poll the inspector while commands are in flight so the audit log tracks
  // the transcript rather than lagging a keystroke behind.
  setInterval(() => inspector.refresh(), 1000);

  // The database is deliberately reachable from the console: the point of the
  // page is that pg_shell has no state anywhere else, and `pgShell.db.query()`
  // is the shortest way to see that for yourself. The browser tests use it too.
  window.pgShell = {
    db,
    executor,
    get session() {
      return session;
    },
  };

  bootOverlay.classList.add("done");
  setTimeout(() => {
    bootOverlay.hidden = true;
    commandInput.focus();
  }, 300);
}

main();
