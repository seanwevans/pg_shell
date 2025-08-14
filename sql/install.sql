-- install.sql: convenience script to install pg_shell schema and functions
\i sql/init_schema.sql
\i sql/submit_command.sql
\i sql/latest_output.sql -- updated latest_output with optional since_id filter
\i sql/fork_session.sql
