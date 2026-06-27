-- latest_output: returns recent commands and outputs
-- Based on SPEC.md RPC definition

DROP FUNCTION IF EXISTS latest_output(UUID);
DROP FUNCTION IF EXISTS latest_output(UUID, INTEGER);

CREATE OR REPLACE FUNCTION latest_output(p_user_id UUID, p_since_id INTEGER DEFAULT 0)
RETURNS TABLE(
    id INTEGER,
    command TEXT,
    output TEXT,
    exit_code INT,
    status TEXT,
    submitted_at TIMESTAMP,
    completed_at TIMESTAMP
) LANGUAGE plpgsql AS $$
BEGIN
    -- Columns are qualified with the table alias so they are not confused
    -- with the identically named RETURNS TABLE output columns, which would
    -- otherwise raise "column reference \"id\" is ambiguous".
    IF p_since_id = 0 THEN
        RETURN QUERY
        SELECT c.id, c.command, c.output, c.exit_code, c.status,
               c.submitted_at, c.completed_at
        FROM commands c
        WHERE c.user_id = p_user_id AND c.id > p_since_id
        ORDER BY c.id DESC
        LIMIT 20;
    ELSE
        RETURN QUERY
        SELECT c.id, c.command, c.output, c.exit_code, c.status,
               c.submitted_at, c.completed_at
        FROM commands c
        WHERE c.user_id = p_user_id AND c.id > p_since_id
        ORDER BY c.id ASC;
    END IF;
END;
$$;
