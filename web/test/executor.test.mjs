// Unit tests for the browser executor, run against a real PGlite database in
// Node so the SQL under test is the repository's own.
//
//   cd web && npm install && node --test test/

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { after, before, beforeEach, describe, it } from "node:test";
import { fileURLToPath } from "node:url";

import { PGlite } from "@electric-sql/pglite";

import { BrowserExecutor } from "../assets/executor.js";
import { Vfs, SHELL_ROOT } from "../assets/vfs.js";
import { split, TokenizeError } from "../assets/shlex.js";
import { ALLOWED_COMMANDS } from "../assets/commands.js";

const repo = resolve(dirname(fileURLToPath(import.meta.url)), "..", "..");

// sql/install.sql, in order.
const SQL_FILES = [
  "init_schema.sql",
  "migrate_session_identity.sql",
  "migrate_replay_provenance_retention.sql",
  "migrate_command_leases.sql",
  "submit_command.sql",
  "latest_output.sql",
  "html_fragments.sql",
  "fork_session.sql",
  "replay_session.sql",
];

let db;
let session;

async function installSchema() {
  await db.exec(
    "DROP TABLE IF EXISTS commands, environments, users, pg_shell_config, monitor_state CASCADE;"
  );
  for (const name of SQL_FILES) {
    await db.exec(await readFile(join(repo, "sql", name), "utf8"));
  }
}

async function openSession() {
  const user = await db.query(
    "INSERT INTO users(id, username) VALUES (gen_random_uuid(), 'sandbox') RETURNING id"
  );
  const environment = await db.query(
    "INSERT INTO environments(user_id) VALUES ($1) RETURNING session_id",
    [user.rows[0].id]
  );
  return { userId: user.rows[0].id, sessionId: environment.rows[0].session_id };
}

// Submit through the real RPC, then let the executor drain the queue, so each
// case covers submit_command() and the claim cycle rather than hand-written
// inserts.
async function run(executor, command) {
  const submitted = await db.query("SELECT submit_command($1, $2, $3) AS id", [
    session.userId,
    session.sessionId,
    command,
  ]);
  await executor.drain();
  const result = await db.query(
    "SELECT command, status, output, exit_code FROM commands WHERE id = $1",
    [submitted.rows[0].id]
  );
  return result.rows[0];
}

function makeExecutor(options = {}) {
  const executor = new BrowserExecutor(db, { pollInterval: 0, ...options });
  executor.running = true; // drain() is driven directly; no timers or LISTEN.
  return executor;
}

before(async () => {
  db = await PGlite.create();
});

after(async () => {
  await db.close();
});

beforeEach(async () => {
  await installSchema();
  session = await openSession();
});

describe("shlex", () => {
  it("splits POSIX quoting the way Python's shlex does", () => {
    assert.deepEqual(split("echo hello world"), ["echo", "hello", "world"]);
    assert.deepEqual(split('echo "a  b" c'), ["echo", "a  b", "c"]);
    assert.deepEqual(split("echo 'it'\\''s'"), ["echo", "it's"]);
    assert.deepEqual(split('echo ""'), ["echo", ""]);
    // Inside double quotes a backslash is only stripped before " and \.
    assert.deepEqual(split('echo "dollar \\$HOME"'), ["echo", "dollar \\$HOME"]);
    assert.deepEqual(split('echo "quote \\" here"'), ["echo", 'quote " here']);
  });

  it("performs no glob expansion", () => {
    assert.deepEqual(split("ls *.txt"), ["ls", "*.txt"]);
  });

  it("rejects unbalanced quotes and dangling escapes", () => {
    assert.throws(() => split('echo "open'), TokenizeError);
    assert.throws(() => split("echo trailing\\"), TokenizeError);
  });
});

describe("filesystem confinement", () => {
  it("refuses paths that resolve outside the shell root", () => {
    const fs = new Vfs();
    assert.throws(() => fs.confine("/etc/passwd", SHELL_ROOT), /Permission denied/);
    assert.throws(() => fs.confine("../../etc", SHELL_ROOT), /Permission denied/);
    assert.equal(fs.confine("notes/../hello.txt", SHELL_ROOT), `${SHELL_ROOT}/hello.txt`);
  });

  it("refuses to remove the shell root itself", () => {
    const fs = new Vfs();
    assert.throws(() => fs.remove(SHELL_ROOT, { recursive: true }), /Permission denied/);
  });
});

describe("executor", () => {
  it("runs a command and records output and exit code", async () => {
    const row = await run(makeExecutor(), "echo hello");
    assert.equal(row.status, "done");
    assert.equal(row.output, "hello\n");
    assert.equal(row.exit_code, 0);
  });

  it("marks a non-zero exit as failed", async () => {
    const row = await run(makeExecutor(), "cat nope.txt");
    assert.equal(row.status, "failed");
    assert.equal(row.exit_code, 1);
    assert.match(row.output, /cat: nope\.txt: No such file or directory/);
  });

  it("refuses a command that is not on the allowlist", async () => {
    const row = await run(makeExecutor(), "curl https://example.com");
    assert.equal(row.status, "failed");
    assert.equal(row.output, "command is not in EXECUTOR_ALLOWED_COMMANDS: curl");
    assert.ok(!ALLOWED_COMMANDS.includes("curl"));
  });

  it("reports a tokenizer error as the command output", async () => {
    const row = await run(makeExecutor(), 'echo "unterminated');
    assert.equal(row.status, "failed");
    assert.equal(row.output, "No closing quotation");
  });

  it("writes a successful cd back to the session environment", async () => {
    const executor = makeExecutor();
    const row = await run(executor, "cd notes");
    assert.equal(row.status, "done");
    const environment = await db.query(
      "SELECT cwd FROM environments WHERE session_id = $1",
      [session.sessionId]
    );
    assert.equal(environment.rows[0].cwd, `${SHELL_ROOT}/notes`);

    // The next command is claimed with the new cwd in its snapshot.
    const after = await run(executor, "pwd");
    assert.equal(after.output, `${SHELL_ROOT}/notes\n`);
  });

  it("refuses a cd that escapes the shell root and leaves the environment alone", async () => {
    const row = await run(makeExecutor(), "cd /etc");
    assert.equal(row.status, "failed");
    assert.equal(row.output, "cd: /etc: Permission denied");
    const environment = await db.query(
      "SELECT cwd FROM environments WHERE session_id = $1",
      [session.sessionId]
    );
    assert.equal(environment.rows[0].cwd, SHELL_ROOT);
  });

  it("rejects an env_snapshot carrying a reserved variable", async () => {
    await db.query("UPDATE environments SET env = $1 WHERE session_id = $2", [
      JSON.stringify({ LD_PRELOAD: "/tmp/evil.so" }),
      session.sessionId,
    ]);
    const row = await run(makeExecutor(), "env");
    assert.equal(row.status, "failed");
    assert.match(row.output, /reserved environment variable\(s\): LD_PRELOAD/);
  });

  it("passes a benign env_snapshot through to the command", async () => {
    await db.query("UPDATE environments SET env = $1 WHERE session_id = $2", [
      JSON.stringify({ GREETING: "hi" }),
      session.sessionId,
    ]);
    const row = await run(makeExecutor(), "env");
    assert.match(row.output, /^GREETING=hi$/m);
    assert.match(row.output, /^PATH=/m);
  });

  it("truncates output beyond the byte limit", async () => {
    const executor = makeExecutor({ maxOutputBytes: 64 });
    const row = await run(executor, `echo ${"x".repeat(200)}`);
    assert.ok(row.output.endsWith("...[truncated]"));
    assert.equal(row.output.replace("...[truncated]", "").length, 64);
  });

  it("times a command out with exit code 124", async () => {
    const executor = makeExecutor({ commandTimeout: 0.05 });
    const row = await run(executor, "sleep 5");
    assert.equal(row.status, "failed");
    assert.equal(row.exit_code, 124);
    assert.match(row.output, /^Timed out after 0\.05s/);
  });

  it("keeps a session's commands in submission order", async () => {
    const executor = makeExecutor();
    for (const command of ["mkdir one", "cd one", "pwd"]) {
      await db.query("SELECT submit_command($1, $2, $3)", [
        session.userId,
        session.sessionId,
        command,
      ]);
    }
    await executor.drain();
    const rows = await db.query(
      "SELECT command, status, output FROM commands ORDER BY id"
    );
    assert.deepEqual(
      rows.rows.map((row) => row.status),
      ["done", "done", "done"]
    );
    assert.equal(rows.rows[2].output, `${SHELL_ROOT}/one\n`);
  });

  it("requeues work left running by a previous instance of the same worker", async () => {
    await db.query("SELECT submit_command($1, $2, $3)", [
      session.userId,
      session.sessionId,
      "echo recovered",
    ]);
    await db.query(
      `UPDATE commands SET status = 'running', worker_id = 'browser',
              claimed_at = now(), lease_expires_at = now() + interval '1 hour'`
    );

    const executor = makeExecutor();
    await executor.recover();
    await executor.drain();

    const row = await db.query("SELECT status, output FROM commands LIMIT 1");
    assert.equal(row.rows[0].status, "done");
    assert.equal(row.rows[0].output, "recovered\n");
  });

  it("reclaims a command whose lease has expired", async () => {
    await db.query("SELECT submit_command($1, $2, $3)", [
      session.userId,
      session.sessionId,
      "echo expired",
    ]);
    await db.query(
      `UPDATE commands SET status = 'running', worker_id = 'someone-else',
              claimed_at = now(), lease_expires_at = now() - interval '1 minute'`
    );

    await makeExecutor().drain();

    const row = await db.query("SELECT status, output FROM commands LIMIT 1");
    assert.equal(row.rows[0].status, "done");
    assert.equal(row.rows[0].output, "expired\n");
  });

  it("leaves another worker's live lease alone", async () => {
    await db.query("SELECT submit_command($1, $2, $3)", [
      session.userId,
      session.sessionId,
      "echo not-mine",
    ]);
    await db.query(
      `UPDATE commands SET status = 'running', worker_id = 'someone-else',
              claimed_at = now(), lease_expires_at = now() + interval '1 hour'`
    );

    await makeExecutor().drain();

    const row = await db.query("SELECT status, worker_id FROM commands LIMIT 1");
    assert.equal(row.rows[0].status, "running");
    assert.equal(row.rows[0].worker_id, "someone-else");
  });

  it("renders completed commands through the HTML fragment RPC", async () => {
    await run(makeExecutor(), "echo <b>hi</b>");
    const fragment = await db.query("SELECT latest_output_html($1, $2, 0) AS html", [
      session.userId,
      session.sessionId,
    ]);
    // The RPC escapes untrusted text, which is what lets htmx swap it directly.
    assert.match(fragment.rows[0].html, /&lt;b&gt;hi&lt;\/b&gt;/);
    assert.ok(!fragment.rows[0].html.includes("<b>hi</b>"));
  });
});
