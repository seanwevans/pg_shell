-- Idempotently upgrade the original one-environment-per-user schema.
-- Every legacy user environment becomes one session and all of that user's
-- existing commands are attached to it.

ALTER TABLE environments
  ADD COLUMN IF NOT EXISTS session_id UUID DEFAULT gen_random_uuid();
UPDATE environments SET session_id = gen_random_uuid() WHERE session_id IS NULL;
ALTER TABLE environments ALTER COLUMN session_id SET NOT NULL;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'environments'::regclass
       AND contype = 'p'
       AND conname = 'environments_pkey'
       AND pg_get_constraintdef(oid) = 'PRIMARY KEY (user_id)'
  ) THEN
    ALTER TABLE environments DROP CONSTRAINT environments_pkey;
  END IF;
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'environments'::regclass AND contype = 'p'
  ) THEN
    ALTER TABLE environments ADD CONSTRAINT environments_pkey PRIMARY KEY (session_id);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS environments_session_user_idx
  ON environments(session_id, user_id);
CREATE INDEX IF NOT EXISTS environments_user_id_idx ON environments(user_id);

ALTER TABLE commands ADD COLUMN IF NOT EXISTS session_id UUID;
UPDATE commands c
   SET session_id = e.session_id
  FROM environments e
 WHERE c.session_id IS NULL AND e.user_id = c.user_id;
ALTER TABLE commands ALTER COLUMN session_id SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
     WHERE conrelid = 'commands'::regclass
       AND conname = 'commands_session_owner_fkey'
  ) THEN
    ALTER TABLE commands ADD CONSTRAINT commands_session_owner_fkey
      FOREIGN KEY (session_id, user_id)
      REFERENCES environments(session_id, user_id);
  END IF;
END $$;

DROP INDEX IF EXISTS commands_user_id_id_idx;
DROP INDEX IF EXISTS commands_user_replay_source_idx;
CREATE INDEX IF NOT EXISTS commands_session_id_id_idx ON commands(session_id, id);
CREATE INDEX IF NOT EXISTS commands_session_replay_source_idx
  ON commands(session_id, replay_of_command_id)
  WHERE replay_of_command_id IS NOT NULL;
