#!/usr/bin/env python3
import argparse
import logging
import os
import sys

import psycopg2
from psycopg2.extras import RealDictCursor


def get_conn():
    dsn = os.environ.get("DATABASE_URL") or os.environ.get("PG_CONN")
    if not dsn:
        logging.error("DATABASE_URL or PG_CONN environment variable required")
        sys.exit(1)
    return psycopg2.connect(dsn)


def replay_commands(user_id: str, start_id: int) -> None:
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, command
              FROM commands
             WHERE user_id = %s AND id >= %s
          ORDER BY id ASC
            """,
            (user_id, start_id),
        )
        rows = cur.fetchall()
        logging.info("Found %d commands to replay", len(rows))

        for row in rows:
            cmd_id = row["id"]
            command = row["command"]
            logging.info("Replaying command %s: %s", cmd_id, command)
            cur.execute("SELECT submit_command(%s, %s)", (user_id, command))
            new_id = cur.fetchone()[0]
            conn.commit()
            logging.info("Queued as command %s", new_id)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay user commands")
    parser.add_argument("--user", required=True, help="User ID")
    parser.add_argument(
        "--start", type=int, required=True, help="Starting command ID"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    replay_commands(args.user, args.start)


if __name__ == "__main__":
    main()
