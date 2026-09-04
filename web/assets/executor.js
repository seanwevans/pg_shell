// Browser stand-in for workers/executor_agent.py.
//
// The claim/execute/complete cycle and the SQL behind it are the worker's: a
// command is claimed with the session's live cwd and env copied into its
// snapshot columns, `cd` is handled by the executor itself and writes back to
// `environments`, and the terminal update clears the lease. What changes is
// the execution step -- a tab cannot fork a process, so commands run against
// an in-memory filesystem from an allowlist instead of an execve.

import { COMMANDS, ALLOWED_COMMANDS, CommandError } from "./commands.js";
import { Vfs, SHELL_ROOT } from "./vfs.js";
import { split, TokenizeError } from "./shlex.js";

const DEFAULTS = {
  workerId: "browser",
  leaseSeconds: 60,
  commandTimeout: 30,
  maxOutputBytes: 65536,
  pollInterval: 500,
};

const TRUNCATION_SUFFIX = "...[truncated]";

// Mirrors RESERVED_COMMAND_ENV / RESERVED_COMMAND_ENV_PREFIXES in
// executor_agent.py. There is no dynamic loader here, but keeping the check
// means the demo rejects exactly the snapshots the real executor rejects.
const RESERVED_ENV = new Set([
  "DATABASE_URL",
  "PG_CONN",
  "GCONV_PATH",
  "HOSTALIASES",
  "LOCPATH",
  "MALLOC_TRACE",
  "NLSPATH",
  "RESOLV_HOST_CONF",
]);
const RESERVED_ENV_PREFIXES = ["LD_", "DYLD_", "BASH_FUNC_"];

function isReservedEnv(name) {
  const upper = name.toUpperCase();
  return RESERVED_ENV.has(upper) || RESERVED_ENV_PREFIXES.some((prefix) => upper.startsWith(prefix));
}

function truncate(text, limit) {
  const encoded = new TextEncoder().encode(text);
  if (encoded.length <= limit) return text;
  return new TextDecoder().decode(encoded.slice(0, limit)) + TRUNCATION_SUFFIX;
}

export class BrowserExecutor {
  constructor(db, options = {}) {
    this.db = db;
    this.options = { ...DEFAULTS, ...options };
    this.fs = options.fs ?? new Vfs();
    this.onEvent = options.onEvent ?? (() => {});
    this.running = false;
    this.busy = false;
    this.wakeup = null;
    this.timer = null;
    this.unlisten = null;
  }

  async start() {
    if (this.running) return;
    this.running = true;
    await this.recover();
    // submit_command() ends with pg_notify on the channel named in
    // pg_shell_config, exactly as the deployed worker expects, so the demo is
    // driven by NOTIFY and only falls back to polling.
    const channel = await this.listenChannel();
    try {
      this.unlisten = await this.db.listen(channel, () => this.poke());
    } catch (error) {
      this.onEvent({ kind: "warning", message: `LISTEN unavailable, polling only: ${error.message}` });
    }
    this.timer = setInterval(() => this.poke(), this.options.pollInterval);
    this.poke();
  }

  async stop() {
    this.running = false;
    if (this.timer !== null) clearInterval(this.timer);
    this.timer = null;
    if (this.unlisten) await this.unlisten();
    this.unlisten = null;
  }

  async listenChannel() {
    const result = await this.db.query(
      "SELECT value FROM pg_shell_config WHERE key = 'listen_channel'"
    );
    return result.rows[0]?.value || "new_command";
  }

  // Release work left running by a previous instance of this worker id, the
  // way recover_worker_commands() does at worker startup. A reloaded tab is
  // exactly the "dead predecessor" case that exists for.
  async recover() {
    const result = await this.db.query(
      `UPDATE commands
          SET status = 'pending', claimed_at = NULL,
              lease_expires_at = NULL, worker_id = NULL
        WHERE status = 'running' AND worker_id = $1`,
      [this.options.workerId]
    );
    if (result.affectedRows) {
      this.onEvent({
        kind: "recovered",
        message: `Recovered ${result.affectedRows} command(s) left by a previous page load`,
      });
    }
  }

  poke() {
    if (!this.running || this.busy) return;
    this.busy = true;
    this.drain().finally(() => {
      this.busy = false;
    });
  }

  async drain() {
    while (this.running) {
      let row;
      try {
        row = await this.fetchPending();
      } catch (error) {
        this.onEvent({ kind: "error", message: `claim failed: ${error.message}` });
        return;
      }
      if (!row) return;
      try {
        await this.handle(row);
      } catch (error) {
        await this.complete(row.id, "failed", String(error.message ?? error), 1);
      }
    }
  }

  // fetch_pending() in executor_agent.py, minus the per-session advisory lock:
  // a page has a single connection, so it can never race itself. The requeue
  // of expired leases and the "no earlier unfinished command in this session"
  // guard are what keep a session's commands strictly ordered, so both stay.
  async fetchPending() {
    await this.db.query(
      `UPDATE commands
          SET status = 'pending', claimed_at = NULL,
              lease_expires_at = NULL, worker_id = NULL
        WHERE status = 'running'
          AND (lease_expires_at IS NULL OR lease_expires_at <= now())`
    );
    const claimed = await this.db.query(
      `UPDATE commands AS target
          SET status = 'running',
              cwd_snapshot = source.cwd,
              env_snapshot = source.env,
              claimed_at = now(),
              lease_expires_at = now() + $1 * interval '1 second',
              worker_id = $2
         FROM (
              SELECT c.id, e.cwd, e.env
                FROM commands AS c
                JOIN environments AS e
                  ON e.session_id = c.session_id AND e.user_id = c.user_id
               WHERE c.status = 'pending'
                 AND NOT EXISTS (
                       SELECT 1
                         FROM commands AS earlier
                        WHERE earlier.session_id = c.session_id
                          AND earlier.status IN ('pending', 'running')
                          AND (earlier.submitted_at, earlier.id) < (c.submitted_at, c.id)
                 )
               ORDER BY c.submitted_at, c.id
               LIMIT 1
         ) AS source
        WHERE target.id = source.id
    RETURNING target.id, target.user_id, target.session_id, target.command,
              target.cwd_snapshot, target.env_snapshot`,
      [this.options.leaseSeconds, this.options.workerId]
    );
    return claimed.rows[0] ?? null;
  }

  async complete(id, status, output, exitCode) {
    await this.db.query(
      `UPDATE commands
          SET status = $1, output = $2, exit_code = $3, completed_at = now(),
              claimed_at = NULL, lease_expires_at = NULL, worker_id = NULL
        WHERE id = $4`,
      [status, output, exitCode, id]
    );
    this.onEvent({ kind: "completed", id, status, exitCode });
  }

  async updateCwd(userId, sessionId, cwd) {
    await this.db.query(
      `UPDATE environments SET cwd = $1, updated_at = now()
        WHERE user_id = $2 AND session_id = $3`,
      [cwd, userId, sessionId]
    );
  }

  async handle(row) {
    const command = row.command.trim();
    let tokens;
    try {
      tokens = split(command);
    } catch (error) {
      if (!(error instanceof TokenizeError)) throw error;
      await this.complete(row.id, "failed", error.message, 1);
      return;
    }

    // `cd` is the one command the worker interprets itself, because a child
    // process could not persist a directory change back to the session.
    if (tokens.length === 2 && tokens[0] === "cd") {
      await this.changeDirectory(row, tokens[1]);
      return;
    }

    const [exitCode, output] = await this.run(command, row.cwd_snapshot, row.env_snapshot, tokens);
    await this.complete(row.id, exitCode === 0 ? "done" : "failed", output, exitCode);
  }

  async changeDirectory(row, target) {
    const resolved = this.fs.resolve(target, row.cwd_snapshot);
    if (!this.fs.inRoot(resolved)) {
      await this.complete(row.id, "failed", `cd: ${target}: Permission denied`, 1);
      return;
    }
    if (!this.fs.isDir(resolved)) {
      await this.complete(row.id, "failed", `cd: ${target}: No such file or directory`, 1);
      return;
    }
    await this.updateCwd(row.user_id, row.session_id, resolved);
    await this.complete(row.id, "done", "", 0);
  }

  // run_subprocess()'s contract: build the command environment from scratch,
  // refuse reserved names, refuse anything off the allowlist, and cap both the
  // wall clock and the captured output.
  async run(command, cwd, envSnapshot, tokens) {
    const env = {
      PATH: "/usr/local/bin:/usr/bin:/bin",
      LANG: "C.UTF-8",
      LC_ALL: "C.UTF-8",
    };
    const snapshot = typeof envSnapshot === "string" ? JSON.parse(envSnapshot) : envSnapshot;
    if (snapshot) {
      if (typeof snapshot !== "object" || Array.isArray(snapshot)) {
        throw new Error("env_snapshot must be a JSON object");
      }
      const reserved = Object.keys(snapshot).filter(isReservedEnv).sort();
      if (reserved.length) {
        throw new Error(`reserved environment variable(s): ${reserved.join(", ")}`);
      }
      if (!Object.entries(snapshot).every(([key, value]) => typeof key === "string" && typeof value === "string")) {
        throw new Error("env_snapshot keys and values must be strings");
      }
      Object.assign(env, snapshot);
    }

    if (!tokens.length) throw new Error("command must not be empty");

    const [name, ...argv] = tokens;
    if (!Object.hasOwn(COMMANDS, name)) {
      throw new Error(`command is not in EXECUTOR_ALLOWED_COMMANDS: ${name}`);
    }

    const context = { name, argv, cwd, env, fs: this.fs };
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject({ timedOut: true }), this.options.commandTimeout * 1000);
    });

    try {
      const output = await Promise.race([Promise.resolve(COMMANDS[name](context)), timeout]);
      return [0, truncate(output ?? "", this.options.maxOutputBytes)];
    } catch (error) {
      if (error && error.timedOut) {
        return [124, `Timed out after ${this.options.commandTimeout}s\n`];
      }
      if (error instanceof CommandError) {
        return [error.exitCode, truncate(error.message ? error.message + "\n" : "", this.options.maxOutputBytes)];
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }
}

export { SHELL_ROOT, ALLOWED_COMMANDS };
