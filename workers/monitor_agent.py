#!/usr/bin/env python3
from __future__ import annotations

"""pg_shell monitor agent.

Collects usage statistics from the ``commands`` table. Metrics include
command counts per user per day and the approximate time between
submission and completion. Results are printed to stdout or optionally
written to a CSV file.
"""

import argparse
import csv
import logging
import time
from typing import Iterable, Tuple

from workers.db import get_conn


Row = Tuple[str, str, int, float]


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

    def run_loop(csv_writer: csv.writer | None) -> int:
        while True:
            try:
                conn = get_conn()
                try:
                    rows = collect_metrics(conn)
                    output_metrics(rows, csv_writer)
                finally:
                    conn.close()
            except RuntimeError as exc:
                logging.error("Error collecting metrics: %s", exc)
                if args.once:
                    return 1
                time.sleep(args.interval)
                continue

            if args.once:
                return 0
            time.sleep(args.interval)

    if args.csv:

        with open(args.csv, "a", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            if csv_file.tell() == 0:
                csv_writer.writerow(["user_id", "day", "command_count", "avg_seconds"])
            return run_loop(csv_writer)
    else:
        return run_loop(None)

if __name__ == "__main__":
    raise SystemExit(main())
