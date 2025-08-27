#!/usr/bin/env python3
import argparse
import logging
import os
import time

from workers.db import get_conn


def cleanup_once(conn, days: int) -> None:
    """Remove commands and reset environments older than ``days`` days."""
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM commands
             WHERE status = 'done'
               AND submitted_at < now() - %s * interval '1 day'
            RETURNING id
            """,
            (days,),
        )
        deleted = cur.rowcount
        logging.info("Deleted %d old commands", deleted)

        cur.execute(
            """
            UPDATE environments
               SET cwd = '/home/sandbox', env = '{}'::jsonb, updated_at = now()
             WHERE updated_at < now() - %s * interval '1 day'
            """,
            (days,),
        )
        reset = cur.rowcount
        logging.info("Reset %d stale environments", reset)
    conn.commit()


def main():
    parser = argparse.ArgumentParser(description="Cleanup old commands and environments")
    parser.add_argument(
        "--interval", type=int, default=3600,
        help="Seconds between cleanup runs (default: 3600)"
    )
    parser.add_argument(
        "--once", action="store_true", help="Run cleanup once and exit"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=int(os.getenv("CLEANUP_DAYS", "90")),
        help="Age threshold in days (default: 90)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    while True:
        try:
            conn = get_conn()
        except RuntimeError as exc:
            logging.error("Cleanup agent failed to connect to database: %s", exc)
            break
        try:
            cleanup_once(conn, args.days)
        finally:
            conn.close()
        if args.once:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
