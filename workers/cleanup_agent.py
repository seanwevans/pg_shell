#!/usr/bin/env python3
import argparse
import logging
import os
import time

from workers.db import get_conn


def cleanup_once(conn, days: int) -> None:
    """Remove commands and reset environments older than ``days`` days.

    Replay rows have an independent retention lifetime. Deleting an expired
    source command clears their ``replay_of_command_id`` through the schema's
    ``ON DELETE SET NULL`` foreign key, while preserving replay command data.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM commands
             WHERE status IN ('done', 'failed')
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
    parser = argparse.ArgumentParser(
        description="Cleanup old commands and environments"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Seconds between cleanup runs (default: 3600)",
    )
    parser.add_argument("--once", action="store_true", help="Run cleanup once and exit")
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Age threshold in days (default: 90)",
    )
    args = parser.parse_args()

    if args.days is None:
        cleanup_days = os.getenv("CLEANUP_DAYS", "90")
        try:
            args.days = int(cleanup_days)
        except ValueError:
            parser.error(f"CLEANUP_DAYS must be an integer (got {cleanup_days!r})")

    if args.days <= 0:
        parser.error("--days must be a positive integer")
    if args.interval <= 0:
        parser.error("--interval must be a positive integer")

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
