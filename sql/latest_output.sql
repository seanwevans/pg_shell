-- latest_output: returns recent commands and outputs
-- Based on SPEC.md RPC definition

DROP FUNCTION IF EXISTS latest_output(UUID);
DROP FUNCTION IF EXISTS latest_output(UUID, INTEGER);
DROP FUNCTION IF EXISTS latest_output(UUID, UUID, INTEGER);

CREATE OR REPLACE FUNCTION latest_output(
  p_user_id UUID, p_session_id UUID, p_since_id INTEGER DEFAULT 0
)
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
    -- Rows are always returned oldest-first so a caller can render them as a
    -- transcript. The p_since_id = 0 backfill still selects the *newest* 20
    -- commands (SPEC.md: "last N commands"), but the DESC + LIMIT is confined
    -- to an inner query and the result is re-sorted ascending. Returning that
    -- page newest-first would make the initial page load read bottom-to-top
    -- and then flip as soon as incremental polling took over.
    --
    -- Columns are qualified with the table alias so they are not confused
    -- with the identically named RETURNS TABLE output columns, which would
    -- otherwise raise "column reference \"id\" is ambiguous".
    IF p_since_id = 0 THEN
        RETURN QUERY
        SELECT recent.id, recent.command, recent.output, recent.exit_code,
               recent.status, recent.submitted_at, recent.completed_at
        FROM (
            SELECT c.id, c.command, c.output, c.exit_code, c.status,
                   c.submitted_at, c.completed_at
            FROM commands c
            WHERE c.user_id = p_user_id AND c.session_id = p_session_id
              AND c.id > p_since_id
            ORDER BY c.id DESC
            LIMIT 20
        ) AS recent
        ORDER BY recent.id ASC;
    ELSE
        RETURN QUERY
        SELECT c.id, c.command, c.output, c.exit_code, c.status,
               c.submitted_at, c.completed_at
        FROM commands c
        WHERE c.user_id = p_user_id AND c.session_id = p_session_id
          AND c.id > p_since_id
        ORDER BY c.id ASC;
    END IF;
END;
$$;
