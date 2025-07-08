-- submit_command: queues a command for execution
-- References SPEC.md (RPC function) and executor_agent in AGENTS.md

CREATE OR REPLACE FUNCTION submit_command(p_user_id UUID, p_command TEXT)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
  env_row environments%ROWTYPE;
  new_id INTEGER;
BEGIN
  -- ensure environment row exists
  SELECT * INTO env_row FROM environments WHERE user_id = p_user_id;
  IF NOT FOUND THEN
    INSERT INTO environments(user_id) VALUES (p_user_id)
      RETURNING * INTO env_row;
  END IF;

  INSERT INTO commands(user_id, command, cwd_snapshot, env_snapshot)
    VALUES (p_user_id, p_command, env_row.cwd, env_row.env)
    RETURNING id INTO new_id;

  RETURN new_id;
END;
$$;
