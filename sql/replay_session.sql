-- replay_session: re-queues a user's command history from a starting point.
--
-- Implements the replay flow described in SPEC.md section 6 ("pull all
-- commands >= session start, apply in order") as a callable RPC, mirroring
-- workers/replay_agent.py. Each replayed command is queued through
-- submit_command so it is tagged with replay_of_command_id (the original
-- command) and a shared replay_run_id, and the executor is notified.
--
-- Only original commands (replay_of_command_id IS NULL) are replayed so that
-- replaying a range which already contains prior replays cannot snowball.
-- Returns the replay_run_id grouping the newly queued commands.

CREATE OR REPLACE FUNCTION replay_session(
  p_user_id UUID,
  p_start_id INTEGER
)
RETURNS UUID LANGUAGE plpgsql AS $$
DECLARE
  run_id UUID := gen_random_uuid();
  rec RECORD;
BEGIN
  -- Validate the user up front, matching submit_command's contract.
  PERFORM 1 FROM users WHERE id = p_user_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Unknown user_id: %. Create the user before replaying.', p_user_id
      USING ERRCODE = '22023';
  END IF;

  FOR rec IN
    SELECT id, command
      FROM commands
     WHERE user_id = p_user_id
       AND id >= p_start_id
       AND replay_of_command_id IS NULL
     ORDER BY id ASC
  LOOP
    PERFORM submit_command(p_user_id, rec.command, rec.id, run_id);
  END LOOP;

  RETURN run_id;
END;
$$;
