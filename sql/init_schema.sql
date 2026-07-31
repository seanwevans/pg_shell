-- Schema initialization for pg_shell
-- Tables defined per SPEC.md

CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY,
  username TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS environments (
  session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID NOT NULL REFERENCES users(id),
  cwd TEXT NOT NULL DEFAULT '/home/sandbox',
  env JSONB NOT NULL DEFAULT '{}',
  updated_at TIMESTAMP NOT NULL DEFAULT now(),
  UNIQUE (session_id, user_id)
);

CREATE TABLE IF NOT EXISTS commands (
  id SERIAL PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES users(id),
  session_id UUID NOT NULL,
  command TEXT NOT NULL,
  -- Replay rows are retained independently of their source command. Once the
  -- source ages out, its nullable provenance link is cleared.
  replay_of_command_id INT REFERENCES commands(id) ON DELETE SET NULL,
  replay_run_id UUID,
  submitted_at TIMESTAMP NOT NULL DEFAULT now(),
  status TEXT NOT NULL DEFAULT 'pending',
  output TEXT,
  exit_code INT,
  cwd_snapshot TEXT,
  env_snapshot JSONB,
  completed_at TIMESTAMP,
  CONSTRAINT status_enum CHECK (status IN ('pending','running','done','failed')),
  CONSTRAINT commands_session_owner_fkey
    FOREIGN KEY (session_id, user_id)
    REFERENCES environments(session_id, user_id)
);

CREATE TABLE IF NOT EXISTS pg_shell_config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS monitor_state (
  agent_name TEXT PRIMARY KEY,
  last_completed_at TIMESTAMP,
  last_command_id INT,
  updated_at TIMESTAMP NOT NULL DEFAULT now()
);


INSERT INTO pg_shell_config(key, value)
VALUES ('listen_channel', 'new_command')
ON CONFLICT (key) DO NOTHING;

CREATE INDEX IF NOT EXISTS commands_status_submitted_at_idx
  ON commands (status, submitted_at);

CREATE INDEX IF NOT EXISTS commands_status_completed_at_idx
  ON commands (status, completed_at, id);
