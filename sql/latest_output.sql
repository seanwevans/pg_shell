-- latest_output: returns recent commands and outputs
-- Based on SPEC.md RPC definition

DROP FUNCTION IF EXISTS latest_output(UUID);

CREATE OR REPLACE FUNCTION latest_output(p_user_id UUID)
RETURNS TABLE(
    id INTEGER,
    command TEXT,
    output TEXT,
    exit_code INT,
    status TEXT,
    submitted_at TIMESTAMP,
    completed_at TIMESTAMP
) LANGUAGE sql AS $$
    SELECT id, command, output, exit_code, status, submitted_at, completed_at
    FROM commands
    WHERE user_id = $1
    ORDER BY id DESC
    LIMIT 20;
$$;
