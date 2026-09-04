# web/ — pg_shell in a browser tab

The site published to GitHub Pages is a complete, working pg_shell instance.
It is not a screenshot, a recording, or a reimplementation: PostgreSQL runs in
the page, this repository's SQL is installed into it at load time, and htmx
swaps the HTML those RPCs return.

**Live: <https://seanwevans.github.io/pg_shell/>**

## What is real, and what is not

| Layer | Deployed pg_shell | This page |
| --- | --- | --- |
| Database | PostgreSQL | PostgreSQL compiled to WebAssembly ([PGlite](https://pglite.dev)) |
| Schema, RPCs, HTML fragments | `sql/` | `sql/`, fetched and installed unmodified at load |
| HTTP gateway | PostgREST | `assets/postgrest.js`, answering the same two RPC calls from the tab |
| Frontend | htmx + `html/index.html` | htmx, with the same `hx-*` attributes |
| Executor | `workers/executor_agent.py`, one OS process per command | `assets/executor.js`, an allowlist of commands over an in-memory filesystem |

Only the last two rows are stand-ins, and both are stand-ins for the same
reason: a browser tab has neither a socket to a database server nor the ability
to fork a process.

The executor stand-in keeps the worker's contract rather than approximating it.
It claims a command by copying the session's live `cwd`/`env` into that row's
snapshot columns under a lease, interprets `cd` itself and writes the result
back to `environments`, refuses paths that escape `SHELL_ROOT`, rebuilds the
command environment from scratch and rejects the same reserved variable names
(`LD_*`, `DATABASE_URL`, `GCONV_PATH`, …), caps output at 64 KiB with the same
`...[truncated]` marker, times out at 30 seconds with exit code 124, and
refuses anything outside its allowlist with the worker's own
`command is not in EXECUTOR_ALLOWED_COMMANDS` error. Its tokenizer is a port of
`shlex.split(posix=True)`, verified against Python's across several thousand
fuzzed inputs.

## Poking at it

The page keeps nothing in JavaScript state, so the database is the only place
to look — and it is on `window`:

```js
(await pgShell.db.query("SELECT id, command, status, exit_code FROM commands ORDER BY id")).rows
pgShell.session          // { userId, sessionId }
```

The **Database** panel does the same thing without a console: the audit log is
`commands` for the current session, **Sessions** lists `environments` and calls
`fork_session()` and `replay_session()`, and **SQL** runs whatever you type.

## Layout

```
web/
  index.html          the page; hx-* attributes match html/index.html
  build.mjs           assembles ../_site from web/, sql/, html/vendor/ and PGlite
  assets/
    app.js            boot order, session switching, htmx glue
    db.js             starts PGlite, installs sql/*.sql, seeds a user + session
    postgrest.js      PostgREST stand-in behind XMLHttpRequest
    executor.js       the claim/execute/complete cycle
    commands.js       the command allowlist
    vfs.js            in-memory filesystem confined to SHELL_ROOT
    shlex.js          POSIX tokenizer matching Python's shlex
    inspector.js      the Database panel
  test/               node --test unit tests, run against real PGlite
```

`build.mjs` copies `sql/` and `html/vendor/htmx-1.9.12.min.js` into the output
rather than keeping duplicates here, so the published demo cannot drift from
the source it claims to run.

## Building and running locally

```bash
cd web
npm install
npm test          # unit tests for the executor, filesystem and tokenizer
node build.mjs    # writes ../_site
npm run serve     # build, then serve ../_site on http://localhost:8080
```

Any static server works, but it must send `application/wasm` for `.wasm`;
`python3 -m http.server` does not, and PGlite will fail to start behind it.

The browser end-to-end tests live with the rest of the suite and skip until the
site has been built:

```bash
node web/build.mjs && python -m pytest tests/test_web_demo.py
```

## Deployment

`.github/workflows/pages.yml` builds and publishes on pushes to `main` that
touch `web/`, `sql/`, or the vendored htmx, and on manual dispatch. The first
load transfers about 17 MB of PostgreSQL; after that the browser caches it, and
the database itself is persisted to IndexedDB so a reload resumes the same
session.
