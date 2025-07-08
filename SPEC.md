# pg_shell Spec

## Overview

pg_shell uses Postgres as the sole authority for stateful terminal sessions. Users submit commands via REST (/rpc/submit_command), sessions are auditable and deterministic, and execution occurs via a C-based extension or standalone binary.

---

## 1. Database Schema

```sql
CREATE TABLE users (
  id UUID PRIMARY KEY,
  username TEXT UNIQUE NOT NULL
);

CREATE TABLE environments (
  user_id UUID PRIMARY KEY REFERENCES users(id),
  cwd TEXT NOT NULL DEFAULT '/home/sandbox',
  env JSONB NOT NULL DEFAULT '{}',
  updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE commands (
  id SERIAL PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES users(id),
  command TEXT NOT NULL,
  submitted_at TIMESTAMP NOT NULL DEFAULT now(),
  status TEXT NOT NULL DEFAULT 'pending',
  output TEXT,
  exit_code INT,
  cwd_snapshot TEXT,
  env_snapshot JSONB,
  CONSTRAINT status_enum CHECK (status IN ('pending','running','done','failed'))
);
```

---

## 2. RPC / Stored Procedures

### `submit_command(user_id UUID, command TEXT) RETURNS INTEGER`

- Inserts a new `commands` row with snapshots from `environments`
- Returns `commands.id`

### `latest_output(user_id UUID) RETURNS TABLE(id, command, output, exit_code, status, submitted_at)`

- Returns last *N* commands for user

### `fork_session(user_id, source_command_id) RETURNS UUID`

- Creates a new `environments` entry based on historical snapshot

---

## 3. Execution Backend

- **Preferred**: C extension `run_command(cwd TEXT, env JSONB, command TEXT) RETURNS RECORD (exit_code INT, output TEXT)`
- Alternatively: Polling binary (Python/Go) using `LISTEN/NOTIFY`

Execution must:

- chdir to cwd_snapshot
- set ENV from env_snapshot
- run command via `execve()`
- capture stdout, stderr, and exit status
- update `commands` row and optionally modify `environments` (e.g., cd path)

---

## 4. REST Interface (via PostgREST)

- `POST /rpc/submit_command`
- `GET /rpc/latest_output?user_id=eq.{uuid}`
- `POST /rpc/fork_session`

Requests/responses in JSON.

---

## 5. Frontend (HTML + htmx)

### UI structure

```html
<div id="output"
  hx-get="/rpc/latest_output?user_id=eq.USER_ID"
  hx-trigger="load, every 1s"
  hx-swap="innerHTML">
</div>

<form
  hx-post="/rpc/submit_command"
  hx-trigger="submit from:form"
  hx-target="#output"
  hx-swap="beforeend"
>
  <input type="hidden" name="user_id" value="USER_ID" />
  <input name="command" autocomplete="off" autofocus placeholder="Enter command…" />
</form>
```

- On load/polling: fetch recent output
- On submit: append new output

---

## 6. Auditing & Replay

- `commands` table is full audit log
- Session replay: pull all commands ≥ session start, apply in order
- Use `fork_session()` to snapshot state and replay from that point

---

## 7. Security

- `run_command()` must sanitize inputs or limit to whitelist
- Enforce timeouts, memory limits, chroot/fork
- Role-based access: Postgres policy for users table

---

## 8. Scalability

- Horizontal: multiple exec workers share DB
- Stateless: workers are ephemeral
- Metrics: per-command timing, output size, usage per user

---

## 9. Troubleshooting

- Use LISTEN/NOTIFY on `commands`
- Monitor rows with `status = 'pending'`
- Logs for failed commands accessible via `output` field

---

## 10. Versioning

- Schema migrations via SQL scripts
- Extension version embedded in `pg_extension`
