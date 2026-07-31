-- submit_command: queues a command for execution
-- References SPEC.md (RPC function); consumed by workers/executor_agent.py

DROP FUNCTION IF EXISTS submit_command(UUID, TEXT);
DROP FUNCTION IF EXISTS submit_command(UUID, TEXT, INT, UUID);

CREATE OR REPLACE FUNCTION submit_command(
  p_user_id UUID,
  p_session_id UUID,
  p_command TEXT,
  p_replay_of_command_id INT DEFAULT NULL,
  p_replay_run_id UUID DEFAULT NULL
)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
  user_exists INTEGER;
  env_row environments%ROWTYPE;
  new_id INTEGER;
  channel TEXT;
BEGIN
  -- validate user exists before creating dependent records
  SELECT 1 INTO user_exists FROM users WHERE id = p_user_id;
  IF user_exists IS NULL THEN
    RAISE EXCEPTION 'Unknown user_id: %. Create the user before submitting commands.', p_user_id
      USING ERRCODE = '22023';
  END IF;

  -- A session is explicit and must belong to the requesting user.
  SELECT * INTO env_row FROM environments
   WHERE session_id = p_session_id AND user_id = p_user_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Unknown session_id or session not owned by user: %', p_session_id
      USING ERRCODE = '22023';
  END IF;

  INSERT INTO commands(
    user_id,
    session_id,
    command,
    cwd_snapshot,
    env_snapshot,
    replay_of_command_id,
    replay_run_id
  )
    VALUES (
      p_user_id,
      p_session_id,
      p_command,
      env_row.cwd,
      env_row.env,
      p_replay_of_command_id,
      p_replay_run_id
    )
    RETURNING id INTO new_id;

  SELECT value INTO channel FROM pg_shell_config WHERE key = 'listen_channel';
  PERFORM pg_notify(COALESCE(channel, 'new_command'), new_id::text);

  RETURN new_id;
END;
$$;
