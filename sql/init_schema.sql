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
  CONSTRAINT status_enum CHECK (status IN ('pending','running','done','failed'))
);
