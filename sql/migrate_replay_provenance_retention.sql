-- Replay command data has its own retention lifetime.  Expiring an original
-- command therefore clears the replay's provenance link instead of deleting
-- the replay or preventing cleanup of the original.
--
-- This block is safe to run repeatedly and also tolerates a non-default name
-- for the existing single-column foreign-key constraint.
DO $$
DECLARE
  replay_fk_name TEXT;
  replay_fk_delete_action "char";
BEGIN
  SELECT constraint_row.conname, constraint_row.confdeltype
    INTO replay_fk_name, replay_fk_delete_action
    FROM pg_constraint AS constraint_row
   WHERE constraint_row.conrelid = 'commands'::regclass
     AND constraint_row.contype = 'f'
     AND constraint_row.conkey = ARRAY[
           (SELECT column_row.attnum
              FROM pg_attribute AS column_row
             WHERE column_row.attrelid = 'commands'::regclass
               AND column_row.attname = 'replay_of_command_id'
               AND NOT column_row.attisdropped)
         ];

  IF replay_fk_name IS NOT NULL AND replay_fk_delete_action <> 'n' THEN
    EXECUTE format(
      'ALTER TABLE commands DROP CONSTRAINT %I',
      replay_fk_name
    );
    replay_fk_name := NULL;
  END IF;

  IF replay_fk_name IS NULL THEN
    ALTER TABLE commands
      ADD CONSTRAINT commands_replay_of_command_id_fkey
      FOREIGN KEY (replay_of_command_id)
      REFERENCES commands(id)
      ON DELETE SET NULL;
  END IF;
END
$$;
