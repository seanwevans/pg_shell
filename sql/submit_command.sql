-- submit_command: queues a command for execution
-- References SPEC.md (RPC function); consumed by workers/executor_agent.py

CREATE OR REPLACE FUNCTION submit_command(
  p_user_id UUID,
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

  -- ensure environment row exists
  SELECT * INTO env_row FROM environments WHERE user_id = p_user_id;
  IF NOT FOUND THEN
    INSERT INTO environments(user_id) VALUES (p_user_id)
      RETURNING * INTO env_row;
  END IF;

  INSERT INTO commands(
    user_id,
    command,
    cwd_snapshot,
    env_snapshot,
    replay_of_command_id,
    replay_run_id
  )
    VALUES (
      p_user_id,
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
