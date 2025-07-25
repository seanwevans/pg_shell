#!/usr/bin/env python3
"""pg_shell monitor agent.

Collects usage statistics from the ``commands`` table. Metrics include
command counts per user per day and the approximate time between
submission and completion. Results are printed to stdout or optionally
written to a CSV file.
"""

import argparse
import csv
import logging
import os
import sys
import time
from typing import Iterable, Tuple

import psycopg2


Row = Tuple[str, str, int, float]


def get_conn():
    dsn = os.environ.get("DATABASE_URL") or os.environ.get("PG_CONN")
    if not dsn:
        logging.error("DATABASE_URL or PG_CONN environment variable required")
        sys.exit(1)
    return psycopg2.connect(dsn)


def collect_metrics(conn) -> Iterable[Row]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT user_id,
                   DATE(submitted_at) AS day,
                   COUNT(*) AS command_count,
                   AVG(EXTRACT(EPOCH FROM completed_at - submitted_at)) AS avg_seconds
             FROM commands
             WHERE status IN ('done', 'failed') AND completed_at IS NOT NULL
          GROUP BY user_id, day
          ORDER BY day, user_id
            """
        )
        return cur.fetchall()


def output_metrics(rows: Iterable[Row], csv_writer: csv.writer | None) -> None:
    for row in rows:
        user_id, day, count, avg_seconds = row
        avg_seconds = round(avg_seconds or 0.0, 3)
        if csv_writer:
            csv_writer.writerow([user_id, day, count, avg_seconds])
        else:
            print(f"{day} user={user_id} commands={count} avg_s={avg_seconds}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect usage metrics")
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Seconds between metric output (default: 300)",
    )
    parser.add_argument("--csv", help="Write metrics to CSV file")
    parser.add_argument("--once", action="store_true", help="Run once then exit")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    csv_writer = None
    csv_file = None
    if args.csv:
        csv_file = open(args.csv, "a", newline="")
        csv_writer = csv.writer(csv_file)
        if csv_file.tell() == 0:
            csv_writer.writerow(["user_id", "day", "command_count", "avg_seconds"])

    while True:
        conn = get_conn()
        try:
            rows = collect_metrics(conn)
            output_metrics(rows, csv_writer)
        finally:
            conn.close()

        if args.once:
            break
        time.sleep(args.interval)

    if csv_file:
        csv_file.close()


if __name__ == "__main__":
    main()
