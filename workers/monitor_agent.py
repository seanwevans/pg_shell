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
from datetime import datetime, timedelta
from typing import Callable, Iterator, Tuple

from workers.db import get_conn


Row = Tuple[str, str, int, float]
STATE_AGENT_NAME = "monitor_agent"


def ensure_monitor_state_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS monitor_state (
              agent_name TEXT PRIMARY KEY,
              last_completed_at TIMESTAMP,
              last_command_id INT,
              updated_at TIMESTAMP NOT NULL DEFAULT now()
            )
            """
        )


def load_monitor_state(conn) -> tuple[datetime | None, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_completed_at, COALESCE(last_command_id, 0)
              FROM monitor_state
             WHERE agent_name = %s
            """,
            (STATE_AGENT_NAME,),
        )
        row = cur.fetchone()
    if not row:
        return None, 0
    return row[0], row[1]


def save_monitor_state(conn, completed_at: datetime, command_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO monitor_state (agent_name, last_completed_at, last_command_id, updated_at)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (agent_name)
            DO UPDATE SET
              last_completed_at = EXCLUDED.last_completed_at,
              last_command_id = EXCLUDED.last_command_id,
              updated_at = now()
            """,
            (STATE_AGENT_NAME, completed_at, command_id),
        )
    conn.commit()


def compute_since_timestamp(args: argparse.Namespace) -> datetime | None:
    if args.since_hours is not None and args.since_days is not None:
        raise ValueError("--since-hours and --since-days are mutually exclusive")
    if args.since_hours is not None:
        return datetime.utcnow() - timedelta(hours=args.since_hours)
    if args.since_days is not None:
        return datetime.utcnow() - timedelta(days=args.since_days)
    return None


def collect_metrics(
    conn,
    *,
    since_timestamp: datetime | None = None,
    last_completed_at: datetime | None = None,
    last_command_id: int = 0,
) -> Iterator[Row]:
    """Yield metric rows one by one using a server-side cursor."""

    predicates = ["status IN ('done', 'failed')", "completed_at IS NOT NULL"]
    params: list[object] = []

    if since_timestamp is not None:
        predicates.append("completed_at >= %s")
        params.append(since_timestamp)
    elif last_completed_at is not None:
        predicates.append("(completed_at > %s OR (completed_at = %s AND id > %s))")
        params.extend([last_completed_at, last_completed_at, last_command_id])

    where_sql = " AND ".join(predicates)

    with conn.cursor(name="monitor_agent_metrics") as cur:
        cur.execute(
            f"""
            SELECT user_id,
                   DATE(submitted_at) AS day,
                   COUNT(*) AS command_count,
                   AVG(EXTRACT(EPOCH FROM completed_at - submitted_at)) AS avg_seconds
              FROM commands
             WHERE {where_sql}
          GROUP BY user_id, day
          ORDER BY day, user_id
            """,
            params,
        )
        for row in iter(cur.fetchone, None):
            yield row


def get_watermark(
    conn,
    *,
    since_timestamp: datetime | None = None,
    last_completed_at: datetime | None = None,
    last_command_id: int = 0,
) -> tuple[datetime, int] | None:
    predicates = ["status IN ('done', 'failed')", "completed_at IS NOT NULL"]
    params: list[object] = []

    if since_timestamp is not None:
        predicates.append("completed_at >= %s")
        params.append(since_timestamp)
    elif last_completed_at is not None:
        predicates.append("(completed_at > %s OR (completed_at = %s AND id > %s))")
        params.extend([last_completed_at, last_completed_at, last_command_id])

    where_sql = " AND ".join(predicates)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT completed_at, id
              FROM commands
             WHERE {where_sql}
          ORDER BY completed_at DESC, id DESC
             LIMIT 1
            """,
            params,
        )
        return cur.fetchone()


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
    parser.add_argument(
        "--since-hours",
        type=int,
        help="Only consider commands completed in the last N hours",
    )
    parser.add_argument(
        "--since-days",
        type=int,
        help="Only consider commands completed in the last N days",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    try:
        since_timestamp = compute_since_timestamp(args)
    except ValueError as exc:
        logging.error("%s", exc)
        return 1

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
                ensure_monitor_state_table(conn)
                last_completed_at, last_command_id = load_monitor_state(conn)
                output_metrics(
                    collect_metrics(
                        conn,
                        since_timestamp=since_timestamp,
                        last_completed_at=last_completed_at,
                        last_command_id=last_command_id,
                    ),
                    csv_writer,
                    flush,
                )
                watermark = get_watermark(
                    conn,
                    since_timestamp=since_timestamp,
                    last_completed_at=last_completed_at,
                    last_command_id=last_command_id,
                )
                if watermark is not None and since_timestamp is None:
                    save_monitor_state(conn, watermark[0], watermark[1])
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
