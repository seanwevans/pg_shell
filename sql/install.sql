-- install.sql: convenience script to install pg_shell schema and functions
\i sql/init_schema.sql
\i sql/submit_command.sql
\i sql/latest_output.sql -- updated latest_output with optional since_id filter
\i sql/fork_session.sql

-- Ensure supporting indexes exist on commands for executor and query performance
CREATE INDEX IF NOT EXISTS commands_status_submitted_at_idx ON commands (status, submitted_at);
CREATE INDEX IF NOT EXISTS commands_user_id_id_idx ON commands (user_id, id);
