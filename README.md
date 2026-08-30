# pg_shell
<img width="256" alt="shell" src="https://github.com/user-attachments/assets/59f007f2-571d-42ed-889b-4369d936bfba" />
<img width="256" alt="shell" src="https://github.com/user-attachments/assets/8e5b3ea6-adc7-4067-908c-67badf9bc675" />

A stateless, auditable, replayable command shell powered entirely by PostgreSQL and htmx.

---

## 🚀 What is **pg_shell**?

**pg_shell** lets you run terminal-like sessions via HTTP—backed by Postgres tables, stored procedures, and audit logs. Every session, command, and output is recorded, making it easy to **replay**, **audit**, or **inspect** later.  
The frontend is a simple HTML+htmx app; no JavaScript-heavy terminal emulators, no persistent processes—everything lives in the database.

---

## 💡 Key Features

- **Stateless per HTTP request**: Commands are queued and executed server-side
- **Auditable**: Full command history, timestamps, user IDs, environment states
- **Replayable**: Reconstruct sessions deterministically from database snapshots
- **Database-only backend**: Environment stored in `environments` table
- **Sandboxable**: Command execution results via C extension or worker binary
- **PostgREST-compatible**: Interacts cleanly via REST/RPC endpoints
- **Easy to embed**: Plain HTML + htmx frontend—no JS bundle

---

## 📦 Install & Quickstart

**Requirements:**
- PostgreSQL ≥13
- PostgREST or compatible HTTP gateway
- htmx (version-pinned assets are included under `html/vendor/`)
- Shell execution C binary or trusted extension

**1. Create database schema & extensions**
```sql
\i sql/init_schema.sql
-- Installs pg_shell PL/pgSQL functions and extensions
```

**2. Install Python requirements**
```bash
pip install -r requirements.txt
```
The `requirements.txt` file pins the following versions:

- `pytest==8.4.1`
- `psycopg2-binary==2.9.10`
- `requests==2.32.4`

**3. Run the executor agent**
```bash
# Either DATABASE_URL or PG_CONN may be used for the PostgreSQL DSN
DATABASE_URL=postgresql://localhost/postgres python workers/executor_agent.py
```
The executor agent will exit with an error if neither `DATABASE_URL` nor
`PG_CONN` is set.
Set `COMMAND_TIMEOUT` (seconds) to limit how long each command may run.
Executors claim work with renewable leases. `COMMAND_LEASE_SECONDS` controls
the lease lifetime (60 seconds by default), and
`COMMAND_LEASE_REFRESH_SECONDS` controls the heartbeat interval. Set a stable,
unique `EXECUTOR_WORKER_ID` for a singleton worker deployment to let its next
instance immediately recover commands left by the previous instance at
startup; otherwise a process-unique identifier is generated and abandoned
work is recovered when its lease expires. Each poll considers up to
`COMMAND_CLAIM_CANDIDATES` sessions (10 by default) and takes the first whose
per-session claim is free, so one busy session does not delay the others.
Commands are parsed with `shlex.split` before execution, so quoting rules follow
POSIX shells but features like glob expansion are not performed.

### Executor trust model and sandbox configuration

Database users who can submit commands are **untrusted**. The executor worker,
its PostgreSQL credentials, and the host outside `SHELL_ROOT` are trusted and
must not be accessible to submitted commands. `run_subprocess` therefore builds
a new environment containing only a fixed `PATH`, locale, the command account's
identity variables, and values saved in `env_snapshot`. It never copies the
worker environment, and rejects reserved variables even if a snapshot contains
them: the worker credentials `DATABASE_URL` and `PG_CONN`, every `LD_*`,
`DYLD_*`, and `BASH_FUNC_*` name, and the glibc path overrides `GCONV_PATH`,
`HOSTALIASES`, `LOCPATH`, `MALLOC_TRACE`, `NLSPATH`, and `RESOLV_HOST_CONF`.
Those loader variables would otherwise run attacker-supplied code inside an
allowlisted binary without executing anything new, defeating
`EXECUTOR_ALLOWED_COMMANDS`. Matching is case-insensitive.

Production deployments must create a dedicated, unprivileged OS account and
configure an absolute-path executable allowlist. The allowlist is the required
process isolation boundary in the default deployment; commands not on it are
rejected before execution. Run the worker as root only when it must switch to
the command account (supplementary groups are cleared before `setuid`), or run
the worker itself as that account:

```bash
useradd --system --create-home --home-dir /home/pg-shell-command pg-shell-command
export EXECUTOR_USER=pg-shell-command
export EXECUTOR_ALLOWED_COMMANDS=/usr/bin/printf:/usr/bin/python3
export SHELL_ROOT=/home/pg-shell-command
DATABASE_URL=postgresql://localhost/postgres python workers/executor_agent.py
```

Keep the allowlist minimal and ensure every allowed program and its libraries
are not writable by the command account. Programs that can launch other
processes, load arbitrary code, or read arbitrary paths (including Python) are
not a security boundary and must not be allowed for untrusted tenants. A
deployment needing those tools must add a stronger boundary (for example a
rootless container or namespace sandbox with a read-only root filesystem,
network disabled, capability dropping, resource limits, and only `SHELL_ROOT`
mounted writable). `EXECUTOR_PATH`, `EXECUTOR_LANG`, and `EXECUTOR_LC_ALL` may
override command defaults; these worker settings are used to construct the
command environment rather than inherited wholesale.

You can run `cleanup_agent.py` periodically. Command retention applies only to
terminal statuses: `done` and `failed` commands older than `CLEANUP_DAYS` are
deleted, while recent terminal commands and all `pending` or `running` commands
are retained. Sessions untouched for `CLEANUP_DAYS` also have their `cwd` and
`env` reset, so give the cleanup agent the same `SHELL_ROOT` as the executor
(both default to `/home/sandbox`); a session reset to a `cwd` outside the
executor's `SHELL_ROOT` cannot run anything. Use `replay_agent.py` for session replays. The optional
`monitor_agent.py` emits usage metrics like
command counts and average run time to stdout or CSV.

## Serving the HTML UI

The `html/` directory contains a minimal `index.html` using htmx. Any
static web server can host it:

```bash
cd html && python3 -m http.server 8080
```

When running PostgREST you can also point `server-static-path` to this
folder so the UI is served alongside your RPC endpoints.

The browser calls the `latest_output_html` and `submit_command_html` RPCs,
which return a PostgreSQL `"text/html"` domain. PostgREST serves that media
type directly, so htmx swaps server-rendered, HTML-escaped fragments without
client-side JSON parsing or DOM construction. Poll a user's output with:

```bash
curl -H 'Accept: text/html' "http://localhost:3000/rpc/latest_output_html?p_user_id=00000000-0000-0000-0000-000000000000&p_session_id=00000000-0000-0000-0000-000000000000&p_since_id=0"
```

## Running Tests

Tests require a PostgreSQL database. Set `TEST_DATABASE_URL` to a DSN with privileges to create tables. Then run:
```bash
pip install -r requirements.txt
pytest
```
