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
  replay_of_command_id INT REFERENCES commands(id),
  replay_run_id UUID,
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

CREATE INDEX IF NOT EXISTS commands_user_id_id_idx
  ON commands (user_id, id);

CREATE INDEX IF NOT EXISTS commands_status_completed_at_idx
  ON commands (status, completed_at, id);
  
CREATE INDEX IF NOT EXISTS commands_user_replay_source_idx
  ON commands (user_id, replay_of_command_id)
  WHERE replay_of_command_id IS NOT NULL;
