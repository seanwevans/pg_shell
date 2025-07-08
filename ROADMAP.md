# pg_shell Roadmap

## 🚀 v0.1 (MVP)

- Core DB tables: `users`, `environments`, `commands`
- PL/pgSQL functions: `submit_command(user_id, command)`, `latest_output(user_id)`
- C exec binary + extension for sandboxed commands
- Basic HTML+htmx frontend
- PostgREST integration + demo

## 🛡️ v0.2 (Hardening & UX)

- Input sanitizing & SQL injection protection
- ANSI → HTML color support
- Command timeout & resource limit
- Frontend improvements: scrolling, command history, autocomplete
- Docker container for all-in-one deployment

## 📊 v0.3 (Enterprise/Collab)

- `fork_session()` and `replay_session(session_id)`
- LISTEN/NOTIFY API for live UI updates
- Role-based access control (RBAC), audit policy
- CLI client (Go, Python) for headless use

## 🔐 v1.0 (Production Grade)

- Secure sandbox via chroot/cgroups/seccomp
- Multi-tenant isolation, schema separation
- Audit signing & tamper-evident logs
- Helm chart + Kubernetes deployment

## 🧠 Future Ideas (v2.x+)

- SQL REPL, Python REPL, custom executors
- Collaboration (shared sessions, co-edit)
- Analytics dashboard: heatmaps, frequency stats
- Plugin API: add custom command sets
