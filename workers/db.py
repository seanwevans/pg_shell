import logging
import os

import psycopg2


def get_conn():
    """Return a PostgreSQL connection using DATABASE_URL or PG_CONN env vars."""
    dsn = os.environ.get("DATABASE_URL") or os.environ.get("PG_CONN")
    if not dsn:
        msg = "DATABASE_URL or PG_CONN environment variable required"
        logging.error(msg)
        raise RuntimeError(msg)
    try:
        return psycopg2.connect(dsn)
    except Exception as exc:
        logging.exception("Failed to connect to database")
        raise RuntimeError("Failed to connect to database") from exc
