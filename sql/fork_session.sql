-- fork_session: creates a new environment from a command snapshot
-- Refers to SPEC.md for snapshot fields and AGENTS.md for replay_agent usage

CREATE OR REPLACE FUNCTION fork_session(p_user_id UUID, p_source_command_id INTEGER)
RETURNS UUID LANGUAGE plpgsql AS $$
DECLARE
  src RECORD;
BEGIN
  SELECT cwd_snapshot, env_snapshot
    INTO src
    FROM commands
    WHERE id = p_source_command_id;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'source command % not found', p_source_command_id;
  END IF;

  INSERT INTO environments(user_id, cwd, env)
    VALUES (p_user_id,
            COALESCE(src.cwd_snapshot, '/home/sandbox'),
            COALESCE(src.env_snapshot, '{}'::jsonb))
    ON CONFLICT (user_id) DO UPDATE
      SET cwd = EXCLUDED.cwd,
          env = EXCLUDED.env,
          updated_at = now();

  RETURN p_user_id;
END;
$$;
