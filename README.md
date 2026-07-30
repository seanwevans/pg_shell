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
- htmx (via CDN)
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
Commands are parsed with `shlex.split` before execution, so quoting rules follow
POSIX shells but features like glob expansion are not performed.

### Executor command and isolation policy

The executor does **not** provide an unrestricted shell. `ALLOWED_EXECUTABLES`
is a comma-separated allowlist of executable names (not paths); it defaults to
`cat,echo,false,head,ls,printf,pwd,python3,sleep,tail,true,wc`. Commands using an
unsupported name, an absolute/relative executable path, or an explicit resource
path outside `SHELL_ROOT` are rejected before process creation and recorded as
`failed` with exit code 126. `EXECUTOR_PATH` (default `/usr/bin:/bin`) is trusted
administrator configuration and cannot be overridden by a stored environment
snapshot. `cd` is the only built-in and is confined to `SHELL_ROOT`.

Each child receives address-space and process-count limits, configurable with
`COMMAND_MEMORY_BYTES` (default 256 MiB) and `COMMAND_MAX_PROCESSES` (default
32). In production, set `EXECUTOR_UID` and `EXECUTOR_GID` to a dedicated,
unprivileged OS account; UID 0 is refused. `EXECUTOR_CHROOT` optionally enters a
prepared chroot containing the allowlisted binaries and their libraries before
dropping privileges.

The allowlist and argument checks are defense in depth, **not a complete
sandbox**: an allowed program (especially an interpreter) can access paths that
do not appear as plain command arguments. Deploy the worker in a dedicated
container or VM with a read-only root filesystem, only `SHELL_ROOT` mounted
writable, no host mounts or credentials, no-new-privileges/capabilities dropped,
seccomp syscall filtering, cgroup CPU/memory/PID limits, and network access
denied unless explicitly required. For stronger local isolation, populate and
enable `EXECUTOR_CHROOT`; container/VM isolation is still required for hostile
multi-tenant workloads.

You can run `cleanup_agent.py` periodically and use `replay_agent.py` for

session replays. The optional `monitor_agent.py` emits usage metrics like
command counts and average run time to stdout or CSV.

## Serving the HTML UI

The `html/` directory contains a minimal `index.html` using htmx. Any
static web server can host it:

```bash
cd html && python3 -m http.server 8080
```

When running PostgREST you can also point `server-static-path` to this
folder so the UI is served alongside your RPC endpoints.

Poll a user's output by passing the RPC arguments directly. PostgREST casts
the plain UUID and integer values to the `latest_output(UUID, INTEGER)`
function parameters:

```bash
curl "http://localhost:3000/rpc/latest_output?p_user_id=00000000-0000-0000-0000-000000000000&p_since_id=0"
```

## Running Tests

Tests require a PostgreSQL database. Set `TEST_DATABASE_URL` to a DSN with privileges to create tables. Then run:
```bash
pip install -r requirements.txt
pytest
```

## User Provisioning Contract

`submit_command(p_user_id UUID, p_command TEXT)` requires that `p_user_id` already
exists in the `users` table. The function validates this up front and raises a
clear SQL error (`22023`) for unknown users instead of relying on downstream
foreign-key failures.


## License

This project is licensed under the [MIT License](LICENSE).
