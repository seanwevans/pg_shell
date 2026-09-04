// The Database panel: a read-only window onto the tables the shell is running
// on, plus the session RPCs (fork_session, replay_session) that make pg_shell
// more than a command log.

const AUDIT_COLUMNS = [
  { key: "id", label: "id" },
  { key: "command", label: "command", wrap: true },
  { key: "status", label: "status" },
  { key: "exit_code", label: "exit" },
  { key: "submitted_at", label: "submitted" },
  { key: "completed_at", label: "completed" },
];

function clock(value) {
  if (value === null || value === undefined) return "";
  const at = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(at.getTime())) return String(value);
  return at.toISOString().slice(11, 19);
}

function cell(value) {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) return value.toISOString().replace("T", " ").slice(0, 19);
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function renderTable(table, columns, rows, emptyMessage) {
  const head = columns.map((column) => `<th scope="col">${column.label}</th>`).join("");
  if (!rows.length) {
    table.innerHTML =
      `<thead><tr>${head}</tr></thead>` +
      `<tbody><tr><td class="empty" colspan="${columns.length}">${emptyMessage}</td></tr></tbody>`;
    return;
  }
  const body = rows
    .map(
      (row) =>
        "<tr>" +
        columns
          .map((column) => {
            const raw = column.render ? column.render(row) : cell(row[column.key]);
            const classes = [column.wrap ? "wrap" : null, column.className?.(row) ?? null]
              .filter(Boolean)
              .join(" ");
            return `<td${classes ? ` class="${classes}"` : ""}>${escapeHtml(raw)}</td>`;
          })
          .join("") +
        "</tr>"
    )
    .join("");
  table.innerHTML = `<thead><tr>${head}</tr></thead><tbody>${body}</tbody>`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

export class Inspector {
  constructor(db, session, { onSwitchSession, onReset } = {}) {
    this.db = db;
    this.session = session;
    this.onSwitchSession = onSwitchSession ?? (() => {});
    this.onReset = onReset ?? (() => {});
    this.auditTable = document.getElementById("audit-table");
    this.sessionsTable = document.getElementById("sessions-table");
    this.sqlTable = document.getElementById("sql-table");
    this.sqlStatus = document.getElementById("sql-status");
    this.active = "audit";
  }

  attach() {
    for (const tab of document.querySelectorAll('.tabs [role="tab"]')) {
      tab.addEventListener("click", () => this.select(tab));
    }

    this.sessionsTable.addEventListener("click", (event) => {
      const row = event.target.closest("tr[data-session-id]");
      if (row) this.switchTo(row.dataset.sessionId);
    });

    document.getElementById("action-new").addEventListener("click", () => this.newSession());
    document.getElementById("action-fork").addEventListener("click", () => this.fork());
    document.getElementById("action-replay").addEventListener("click", () => this.replay());
    document.getElementById("action-reset").addEventListener("click", () => this.reset());
    document.getElementById("action-run-sql").addEventListener("click", () => this.runSql());
    document.getElementById("sql-input").addEventListener("keydown", (event) => {
      if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
        event.preventDefault();
        this.runSql();
      }
    });

    this.refresh();
  }

  select(tab) {
    for (const other of document.querySelectorAll('.tabs [role="tab"]')) {
      const selected = other === tab;
      other.setAttribute("aria-selected", String(selected));
      document.getElementById(other.getAttribute("aria-controls")).hidden = !selected;
    }
    this.active = tab.id.replace("tabbtn-", "");
    this.refresh();
  }

  setSession(session) {
    this.session = session;
    this.refresh();
  }

  async refresh() {
    if (this.active === "audit") await this.refreshAudit();
    else if (this.active === "sessions") await this.refreshSessions();
  }

  async refreshAudit() {
    const result = await this.db.query(
      `SELECT id, command, status, exit_code, submitted_at, completed_at
         FROM commands
        WHERE session_id = $1
        ORDER BY id DESC
        LIMIT 200`,
      [this.session.sessionId]
    );
    renderTable(
      this.auditTable,
      AUDIT_COLUMNS.map((column) =>
        column.key === "status"
          ? { ...column, className: (row) => `status-${row.status}` }
          : column.key.endsWith("_at")
            ? { ...column, render: (row) => clock(row[column.key]) }
            : column
      ),
      result.rows,
      "No commands yet — run something in the terminal."
    );
  }

  async refreshSessions() {
    const result = await this.db.query(
      `SELECT e.session_id, e.cwd, e.updated_at,
              count(c.id) AS commands,
              count(c.id) FILTER (WHERE c.status = 'failed') AS failed
         FROM environments AS e
         LEFT JOIN commands AS c ON c.session_id = e.session_id
        WHERE e.user_id = $1
        GROUP BY e.session_id, e.cwd, e.updated_at
        ORDER BY e.updated_at DESC`,
      [this.session.userId]
    );
    renderTable(
      this.sessionsTable,
      [
        {
          key: "session_id",
          label: "session",
          render: (row) =>
            (row.session_id === this.session.sessionId ? "● " : "  ") + row.session_id.slice(0, 8),
          className: (row) => (row.session_id === this.session.sessionId ? "is-current" : ""),
        },
        { key: "cwd", label: "cwd" },
        { key: "commands", label: "cmds" },
        { key: "failed", label: "failed" },
        { key: "updated_at", label: "updated", render: (row) => clock(row.updated_at) },
      ],
      result.rows,
      "No sessions."
    );
    for (const row of this.sessionsTable.querySelectorAll("tbody tr")) {
      const index = [...row.parentNode.children].indexOf(row);
      const record = result.rows[index];
      if (record) row.dataset.sessionId = record.session_id;
    }
  }

  switchTo(sessionId) {
    if (sessionId === this.session.sessionId) return;
    this.onSwitchSession({ ...this.session, sessionId });
  }

  async newSession() {
    const created = await this.db.query(
      "INSERT INTO environments(user_id) VALUES ($1) RETURNING session_id",
      [this.session.userId]
    );
    this.switchTo(created.rows[0].session_id);
  }

  async fork() {
    const source = await this.db.query(
      `SELECT id FROM commands
        WHERE session_id = $1 AND user_id = $2
        ORDER BY id DESC LIMIT 1`,
      [this.session.sessionId, this.session.userId]
    );
    if (!source.rows.length) {
      this.report("Run a command first — fork_session() copies a command's snapshot.", true);
      return;
    }
    const forked = await this.db.query("SELECT fork_session($1, $2) AS session_id", [
      this.session.userId,
      source.rows[0].id,
    ]);
    this.switchTo(forked.rows[0].session_id);
  }

  async replay() {
    const first = await this.db.query(
      "SELECT min(id) AS start_id FROM commands WHERE session_id = $1 AND replay_of_command_id IS NULL",
      [this.session.sessionId]
    );
    const startId = first.rows[0]?.start_id;
    if (startId === null || startId === undefined) {
      this.report("Nothing to replay in this session yet.", true);
      return;
    }
    await this.db.query("SELECT replay_session($1, $2, $3) AS run_id", [
      this.session.userId,
      this.session.sessionId,
      startId,
    ]);
    await this.refresh();
  }

  async reset() {
    await this.onReset();
  }

  async runSql() {
    const statement = document.getElementById("sql-input").value.trim();
    if (!statement) return;
    try {
      const result = await this.db.query(statement);
      const fields = (result.fields ?? []).map((field) => ({ key: field.name, label: field.name, wrap: true }));
      if (!fields.length) {
        renderTable(this.sqlTable, [{ key: "result", label: "result" }], [], "Statement ran; no rows returned.");
      } else {
        renderTable(this.sqlTable, fields, result.rows, "0 rows.");
      }
      this.report(`${result.rows.length} row(s), ${result.affectedRows ?? 0} affected`, false);
    } catch (error) {
      renderTable(this.sqlTable, [{ key: "error", label: "error" }], [], "Query failed.");
      this.report(error.message, true);
    }
  }

  report(message, isError) {
    this.sqlStatus.textContent = message;
    this.sqlStatus.classList.toggle("error", Boolean(isError));
  }
}
