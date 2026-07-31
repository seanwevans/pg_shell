-- Add executor lease metadata to installations created before command leases.
-- Every statement is safe to run repeatedly.
ALTER TABLE commands ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP;
ALTER TABLE commands ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMP;
ALTER TABLE commands ADD COLUMN IF NOT EXISTS worker_id TEXT;

CREATE INDEX IF NOT EXISTS commands_running_lease_idx
  ON commands (lease_expires_at)
  WHERE status = 'running';
