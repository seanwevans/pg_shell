-- Add renewable executor leases without disturbing existing command rows.
-- Safe to run repeatedly during upgrades.
ALTER TABLE commands ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP;
ALTER TABLE commands ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMP;
ALTER TABLE commands ADD COLUMN IF NOT EXISTS lease_worker_id TEXT;

CREATE INDEX IF NOT EXISTS commands_running_lease_idx
  ON commands (lease_expires_at)
  WHERE status = 'running';
