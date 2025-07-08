# pg_shell Agents

## What are Agents?

Agents are automated scripts or daemons that interact with `pg_shell` to execute commands, replay sessions, monitor, or assist users. Agents can be part of the backend or external tools.

---

## 1. `executor_agent`

**Role**: Polls the `commands` table, executes pending commands, and updates output.

**Behaviour**:

- LISTEN/NOTIFY or periodic poll
- For each `status = 'pending'`:
  - Mark status → `running`
  - Call `run_command()` C extension
  - Update `commands.status`, `commands.output`, `exit_code`
  - If command is `cd ...`, update `environments.cwd`

---

## 2. `replay_agent`

**Role**: Replays a past session from a snapshot.

**Use cases**:

- Live demo
- Debugging reproductions
- CI or QA environments

**Behaviour**:

- Accepts `user_id` + `start_command_id`
- Iterates commands in ascending order
- Re-applies `submit_command()` or `run_command()` under a replay mode
- Writes outputs to a replay log or alternate environment

---

## 3. `cleanup_agent`

**Role**: Periodically clean stale sessions, prune outputs, enforce limits.

**Behaviour**:

- Removes old `commands` (`status = 'done'`, older than 90 days)
- Resets or archives `environments` for users inactive for X days

---

## 4. `monitor_agent`

**Role**: Generates usage metrics and alerts

**Behaviour**:

- Counts command executions/user/per day
- Tracks execution time, output size
- Alerts via email/webhook on failures or abuse

---

## 5. `shell_cli` (Optional External Agent)

**Role**: CLI tool for power users

**Commands**:

- `pg_shell exec --user UUID --cmd "ls -la"`
- `pg_shell replay --session 2025-07-08T10:00:00Z`
- `pg_shell fork --user UUID --at command_id`
- `pg_shell tail --user UUID`

---

## Integration & Security

- Agents authenticate via DB credentials or PostgREST tokens
- Run executor_agent under separate Linux user
- Agents may enforce resource limits (memory, CPU, I/O)

---

## Running As a FaaS or Job

Agents can be deployed as:

- Kubernetes cron/deployment
- Systemd services
- Serverless functions (triggered by events)

Each agent logs to stdout/stderr; aggregated in central log system.
