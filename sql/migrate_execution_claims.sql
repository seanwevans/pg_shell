-- Add bounded execution-claim metadata to existing pg_shell installations.
-- Every statement is safe to run repeatedly.
ALTER TABLE commands ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ;
ALTER TABLE commands ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE commands ADD COLUMN IF NOT EXISTS claimed_by TEXT;
ALTER TABLE commands ADD COLUMN IF NOT EXISTS attempt_count INT;

UPDATE commands SET attempt_count = 0 WHERE attempt_count IS NULL;
ALTER TABLE commands ALTER COLUMN attempt_count SET DEFAULT 0;
ALTER TABLE commands ALTER COLUMN attempt_count SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'commands_attempt_count_nonnegative'
      AND conrelid = 'commands'::regclass
  ) THEN
    ALTER TABLE commands
      ADD CONSTRAINT commands_attempt_count_nonnegative
      CHECK (attempt_count >= 0);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS commands_running_lease_expires_at_idx
  ON commands (lease_expires_at)
  WHERE status = 'running';
