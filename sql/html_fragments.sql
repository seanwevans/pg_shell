-- HTML-producing RPCs for the htmx frontend.
--
-- PostgREST uses a domain named after a media type for custom media type
-- responses.  Returning "text/html" lets it send these values as HTML rather
-- than JSON-encoding a text scalar.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
      FROM pg_type t
      JOIN pg_namespace n ON n.oid = t.typnamespace
     WHERE t.typname = 'text/html'
       AND n.nspname = current_schema()
  ) THEN
    CREATE DOMAIN "text/html" AS TEXT;
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION pg_shell_html_escape(p_value TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT replace(
           replace(
             replace(
               replace(
                 replace(COALESCE(p_value, ''), '&', '&amp;'),
                 '<', '&lt;'),
               '>', '&gt;'),
             '"', '&quot;'),
           '''', '&#39;')
$$;

CREATE OR REPLACE FUNCTION latest_output_html(
  p_user_id UUID, p_session_id UUID, p_since_id INTEGER DEFAULT 0
)
RETURNS "text/html"
LANGUAGE sql
STABLE
AS $$
  SELECT COALESCE(
    string_agg(
      format(
        '<article class="command-result" data-command-id="%s"><pre class="command">%s</pre><pre class="command-output">%s</pre><span class="command-status">%s</span><span class="exit-code">%s</span></article>',
        result.id,
        pg_shell_html_escape(result.command),
        pg_shell_html_escape(result.output),
        pg_shell_html_escape(result.status),
        pg_shell_html_escape(result.exit_code::TEXT)
      ),
      '' ORDER BY result.id
    ),
    ''
  )::"text/html"
  -- Ordering by id keeps the fragment a chronological transcript. A bare
  -- row_number() OVER () would instead depend on the order the planner
  -- happened to emit latest_output's rows in.
  FROM latest_output(p_user_id, p_session_id, p_since_id) AS result
$$;

CREATE OR REPLACE FUNCTION submit_command_html(
  p_user_id UUID, p_session_id UUID, p_command TEXT
)
RETURNS "text/html"
LANGUAGE plpgsql
AS $$
DECLARE
  new_id INTEGER;
BEGIN
  new_id := submit_command(p_user_id, p_session_id, p_command);

  RETURN format(
    '<article class="command-result" data-command-id="%s"><pre class="command">%s</pre><pre class="command-output"></pre><span class="command-status">pending</span><span class="exit-code"></span></article>',
    new_id,
    pg_shell_html_escape(p_command)
  )::"text/html";
END;
$$;
