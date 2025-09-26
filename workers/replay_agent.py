#!/usr/bin/env python3
import argparse
import logging

from psycopg2.extras import RealDictCursor

from workers.db import get_conn


def replay_commands(user_id: str, start_id: int) -> None:
    with get_conn() as history_conn:
        with history_conn.cursor(cursor_factory=RealDictCursor) as history_cur:
            history_cur.execute(
                """
                SELECT id, command
                  FROM commands
                 WHERE user_id = %s AND id >= %s
              ORDER BY id ASC
                """,
                (user_id, start_id),
            )
            batch_size = 100
            total = 0
            with get_conn() as submit_conn:
                with submit_conn.cursor() as submit_cur:
                    while True:
                        rows = history_cur.fetchmany(batch_size)
                        if not rows:
                            break
                        for row in rows:
                            cmd_id = row["id"]
                            command = row["command"]
                            logging.info(
                                "Replaying command %s: %s", cmd_id, command
                            )
                            submit_cur.execute(
                                "SELECT submit_command(%s, %s)",
                                (user_id, command),
                            )
                            new_id = submit_cur.fetchone()[0]
                            submit_conn.commit()
                            logging.info("Queued as command %s", new_id)
                            total += 1
            if total == 0:
                logging.info("no commands to replay")
            else:
                logging.info("Replayed %d commands", total)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay user commands")
    parser.add_argument("--user", required=True, help="User ID")
    parser.add_argument(
        "--start", type=int, required=True, help="Starting command ID"
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    try:
        replay_commands(args.user, args.start)
    except RuntimeError as exc:
        logging.error("Replay agent failed to connect to database: %s", exc)


if __name__ == "__main__":
    main()
