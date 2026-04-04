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
