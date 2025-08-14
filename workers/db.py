import logging
import os
import sys

import psycopg2


def get_conn():
    """Return a PostgreSQL connection using DATABASE_URL or PG_CONN env vars."""
    dsn = os.environ.get("DATABASE_URL") or os.environ.get("PG_CONN")
    if not dsn:
        logging.error("DATABASE_URL or PG_CONN environment variable required")
        sys.exit(1)
    try:
        return psycopg2.connect(dsn)
    except Exception:
        logging.exception("Failed to connect to database")
        sys.exit(1)
