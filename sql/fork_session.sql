-- fork_session: creates a new environment from a command snapshot
-- Refers to SPEC.md for snapshot fields; used by workers/replay_agent.py

CREATE OR REPLACE FUNCTION fork_session(p_user_id UUID, p_source_command_id INTEGER)
RETURNS UUID LANGUAGE plpgsql AS $$
DECLARE
  src RECORD;
  new_session_id UUID := gen_random_uuid();
BEGIN
  SELECT cwd_snapshot, env_snapshot
    INTO src
    FROM commands
    WHERE id = p_source_command_id
      AND user_id = p_user_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'source command not found or forbidden';
  END IF;

  INSERT INTO environments(session_id, user_id, cwd, env)
    VALUES (new_session_id, p_user_id,
            COALESCE(src.cwd_snapshot, '/home/sandbox'),
            COALESCE(src.env_snapshot, '{}'::jsonb));

  RETURN new_session_id;
END;
$$;
