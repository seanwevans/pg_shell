"""Shared pytest fixtures for pg_shell tests.

Provides a reusable, schema-installed PostgreSQL connection so individual
test modules do not have to re-implement schema setup. Tests that need a
database are skipped automatically when ``TEST_DATABASE_URL`` is unset.
"""

import os
from pathlib import Path

import psycopg2
import pytest

# Function/procedure definitions installed on top of the base schema, in
# dependency order.
SQL_FILES = (
    "sql/init_schema.sql",
    "sql/submit_command.sql",
    "sql/latest_output.sql",
    "sql/fork_session.sql",
)

# Tables dropped before (re)installing the schema for a clean slate.
_MANAGED_TABLES = "commands, environments, users, pg_shell_config, monitor_state"


def install_schema(cur) -> None:
    """Drop managed tables and (re)install schema + functions on ``cur``."""
    cur.execute(f"DROP TABLE IF EXISTS {_MANAGED_TABLES} CASCADE;")
    for path in SQL_FILES:
        cur.execute(Path(path).read_text())


@pytest.fixture
def db_conn():
    """Yield an autocommit connection with a freshly installed schema.

    Skips the test when ``TEST_DATABASE_URL`` is not configured.
    """
    dsn = os.environ.get("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not set")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    with conn.cursor() as cur:
        install_schema(cur)
    try:
        yield conn
    finally:
        conn.close()
