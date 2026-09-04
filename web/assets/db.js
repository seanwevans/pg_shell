// Boots PostgreSQL (PGlite -- the real server compiled to WebAssembly) and
// installs pg_shell into it from the repository's own SQL files.
//
// Nothing here is a reimplementation: the files fetched below are the same
// ones sql/install.sql feeds to psql, in the same order.

import { PGlite } from "../vendor/pglite/index.js";

// sql/install.sql, with its \i meta-commands expanded and its trailing index
// statements inlined -- psql only understands \i, so the browser reads the
// list instead.
export const SQL_FILES = [
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

const INSTALL_TAIL = `
CREATE INDEX IF NOT EXISTS commands_status_submitted_at_idx ON commands (status, submitted_at);
CREATE INDEX IF NOT EXISTS commands_session_id_id_idx ON commands (session_id, id);
`;

// Bumped whenever the installed SQL changes shape in a way an already
// persisted database cannot migrate into. A mismatch rebuilds from scratch.
const SCHEMA_VERSION = "1";
const DATA_DIR = "idb://pg-shell-demo";

async function fetchSql(name) {
  const response = await fetch(`sql/${name}`, { cache: "no-cache" });
  if (!response.ok) throw new Error(`could not load sql/${name}: HTTP ${response.status}`);
  return response.text();
}

async function installedVersion(db) {
  try {
    const result = await db.query(
      "SELECT value FROM pg_shell_config WHERE key = 'demo_schema_version'"
    );
    return result.rows[0]?.value ?? null;
  } catch {
    return null; // pg_shell_config does not exist yet: a fresh database.
  }
}

async function install(db, onProgress) {
  for (const name of SQL_FILES) {
    onProgress?.(`installing sql/${name}`);
    await db.exec(await fetchSql(name));
  }
  await db.exec(INSTALL_TAIL);
  await db.query(
    `INSERT INTO pg_shell_config(key, value) VALUES ('demo_schema_version', $1)
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
    [SCHEMA_VERSION]
  );
}

// The demo's user and session live in the database like everything else, so a
// reload picks up the same session rather than starting a new one.
async function seed(db) {
  const existing = await db.query(
    "SELECT key, value FROM pg_shell_config WHERE key IN ('demo_user_id', 'demo_session_id')"
  );
  const config = Object.fromEntries(existing.rows.map((row) => [row.key, row.value]));

  if (config.demo_user_id && config.demo_session_id) {
    const session = await db.query(
      "SELECT 1 FROM environments WHERE session_id = $1 AND user_id = $2",
      [config.demo_session_id, config.demo_user_id]
    );
    if (session.rows.length) {
      return { userId: config.demo_user_id, sessionId: config.demo_session_id };
    }
  }

  const user = await db.query(
    `INSERT INTO users(id, username) VALUES (gen_random_uuid(), 'sandbox')
     ON CONFLICT (username) DO UPDATE SET username = EXCLUDED.username
     RETURNING id`
  );
  const userId = user.rows[0].id;
  const environment = await db.query(
    "INSERT INTO environments(user_id) VALUES ($1) RETURNING session_id",
    [userId]
  );
  const sessionId = environment.rows[0].session_id;

  await db.query(
    `INSERT INTO pg_shell_config(key, value) VALUES ('demo_user_id', $1), ('demo_session_id', $2)
     ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
    [userId, sessionId]
  );
  return { userId, sessionId };
}

export async function boot({ onProgress, persist = true } = {}) {
  onProgress?.("starting PostgreSQL (WebAssembly)");
  let db = await PGlite.create(persist ? DATA_DIR : undefined);

  const version = await installedVersion(db);
  if (version !== null && version !== SCHEMA_VERSION) {
    onProgress?.("schema changed since this browser last visited; rebuilding");
    await db.close();
    if (persist) await indexedDB.deleteDatabase("/pglite/pg-shell-demo");
    db = await PGlite.create(persist ? DATA_DIR : undefined);
  }

  await install(db, onProgress);
  onProgress?.("opening a session");
  const identity = await seed(db);

  const server = await db.query("SELECT version() AS version");
  return { db, ...identity, version: server.rows[0].version };
}

export async function reset(db) {
  await db.exec(`
    DROP TABLE IF EXISTS commands, environments, users, pg_shell_config, monitor_state CASCADE;
  `);
  await install(db);
  return seed(db);
}
