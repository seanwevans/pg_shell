-- Schema initialization for pg_shell
-- Tables defined per SPEC.md

CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY,
  username TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS environments (
  user_id UUID PRIMARY KEY REFERENCES users(id),
  cwd TEXT NOT NULL DEFAULT '/home/sandbox',
  env JSONB NOT NULL DEFAULT '{}',
  updated_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS commands (
  id SERIAL PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES users(id),
  command TEXT NOT NULL,
  submitted_at TIMESTAMP NOT NULL DEFAULT now(),
  status TEXT NOT NULL DEFAULT 'pending',
  output TEXT,
  exit_code INT,
  cwd_snapshot TEXT,
  env_snapshot JSONB,
  completed_at TIMESTAMP,
  CONSTRAINT status_enum CHECK (status IN ('pending','running','done','failed'))
);

CREATE TABLE IF NOT EXISTS pg_shell_config (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

INSERT INTO pg_shell_config(key, value)
VALUES ('listen_channel', 'new_command')
ON CONFLICT (key) DO NOTHING;

CREATE INDEX IF NOT EXISTS commands_status_submitted_at_idx
  ON commands (status, submitted_at);

CREATE INDEX IF NOT EXISTS commands_user_id_id_idx
  ON commands (user_id, id);

