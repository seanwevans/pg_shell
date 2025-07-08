# pg_shell

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

## Serving the HTML UI

The `html/` directory contains a minimal `index.html` using htmx. Any
static web server can host it:

```bash
cd html && python3 -m http.server 8080
```

When running PostgREST you can also point `server-static-path` to this
folder so the UI is served alongside your RPC endpoints.
