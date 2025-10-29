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
from typing import Callable, Iterator, Tuple

from workers.db import get_conn


Row = Tuple[str, str, int, float]


def collect_metrics(conn) -> Iterator[Row]:
    """Yield metric rows one by one using a server-side cursor."""
    with conn.cursor(name="monitor_agent_metrics") as cur:
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
        for row in iter(cur.fetchone, None):
            yield row


def output_metrics(
    rows: Iterator[Row],
    csv_writer: csv.writer | None,
    flush: Callable[[], None] | None = None,
) -> None:
    for row in rows:
        user_id, day, count, avg_seconds = row
        avg_seconds = round(avg_seconds or 0.0, 3)
        if csv_writer:
            csv_writer.writerow([user_id, day, count, avg_seconds])
            if flush is not None:
                flush()
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

    def run_loop(
        csv_writer: csv.writer | None,
        flush: Callable[[], None] | None,
    ) -> int:
        while True:
            try:
                conn = get_conn()
            except RuntimeError as exc:
                logging.error("Failed to connect to database: %s", exc)
                if args.once:
                    return 1
                time.sleep(min(args.interval, 5))
                continue

            try:
                output_metrics(collect_metrics(conn), csv_writer, flush)
            finally:
                conn.close()

            if csv_writer and flush is not None:
                flush()

            if args.once:
                return 0
            time.sleep(args.interval)

    if args.csv:

        with open(args.csv, "a", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            flush = csv_file.flush
            if csv_file.tell() == 0:
                csv_writer.writerow(["user_id", "day", "command_count", "avg_seconds"])
                flush()
            return run_loop(csv_writer, flush)
    else:
        return run_loop(None, None)

if __name__ == "__main__":
    raise SystemExit(main())
