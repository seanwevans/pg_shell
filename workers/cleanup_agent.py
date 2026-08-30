#!/usr/bin/env python3
import argparse
import logging
import os
import time

from workers.db import get_conn

RETRY_DELAY_SECONDS = 5
DEFAULT_SHELL_ROOT = "/home/sandbox"


def shell_root() -> str:
    """Return the sandbox root a reset session should start from.

    Must agree with executor_agent's ``SHELL_ROOT``: a reset session whose cwd
    sits outside that root cannot run anything, because the executor passes
    the cwd straight to the subprocess.
    """
    return os.getenv("SHELL_ROOT", DEFAULT_SHELL_ROOT)


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
               SET cwd = %s, env = '{}'::jsonb, updated_at = now()
             WHERE updated_at < now() - %s * interval '1 day'
            """,
            (shell_root(), days),
        )
        reset = cur.rowcount
        logging.info("Reset %d stale environments", reset)
    conn.commit()


def main() -> int:
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
        # A database outage must not end the daemon: it is the periodic
        # retention job, so giving up means retention silently stops until
        # somebody notices. --once still reports the failure to its caller.
        try:
            conn = get_conn()
        except RuntimeError as exc:
            logging.error("Cleanup agent failed to connect to database: %s", exc)
            if args.once:
                return 1
            time.sleep(min(args.interval, RETRY_DELAY_SECONDS))
            continue

        try:
            cleanup_once(conn, args.days)
        except Exception:
            logging.exception("Cleanup run failed")
            failed = True
        else:
            failed = False
        finally:
            conn.close()

        if args.once:
            return 1 if failed else 0
        time.sleep(min(args.interval, RETRY_DELAY_SECONDS) if failed else args.interval)


if __name__ == "__main__":
    raise SystemExit(main())
