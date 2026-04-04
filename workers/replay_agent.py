#!/usr/bin/env python3
import argparse
import logging
import uuid

from psycopg2.extras import RealDictCursor

from workers.db import get_conn


def replay_commands(
    user_id: str,
    start_id: int,
    *,
    resume: bool = False,
    force: bool = False,
    batch_commit_size: int = 100,
) -> None:
    if resume and force:
        raise ValueError("Cannot use --resume and --force together")

    replay_run_id = str(uuid.uuid4())
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
            skipped = 0
            pending_commits = 0
            with get_conn() as submit_conn:
                with submit_conn.cursor() as submit_cur:
                    while True:
                        rows = history_cur.fetchmany(batch_size)
                        if not rows:
                            break
                        for row in rows:
                            cmd_id = row["id"]
                            command = row["command"]
                            if resume and not force:
                                submit_cur.execute(
                                    """
                                    SELECT 1
                                      FROM commands
                                     WHERE user_id = %s
                                       AND replay_of_command_id = %s
                                     LIMIT 1
                                    """,
                                    (user_id, cmd_id),
                                )
                                if submit_cur.fetchone() is not None:
                                    logging.info(
                                        "Skipping already replayed command %s", cmd_id
                                    )
                                    skipped += 1
                                    continue
                            logging.info(
                                "Replaying command %s: %s", cmd_id, command
                            )
                            submit_cur.execute(
                                "SELECT submit_command(%s, %s, %s, %s)",
                                (user_id, command, cmd_id, replay_run_id),
                            )
                            new_id = submit_cur.fetchone()[0]
                            pending_commits += 1
                            if pending_commits >= batch_commit_size:
                                submit_conn.commit()
                                pending_commits = 0
                            logging.info("Queued as command %s", new_id)
                            total += 1
                    if pending_commits:
                        submit_conn.commit()
            if total == 0:
                logging.info("no commands to replay")
            else:
                logging.info("Replay run %s queued %d commands", replay_run_id, total)
            if skipped:
                logging.info("Skipped %d commands already replayed", skipped)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay user commands")
    parser.add_argument("--user", required=True, help="User ID")
    parser.add_argument(
        "--start", type=int, required=True, help="Starting command ID"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip commands already replayed for this user",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow duplicate replay rows even if already replayed",
    )
    parser.add_argument(
        "--commit-every",
        type=int,
        default=100,
        help="Commit after this many submitted replay rows",
    )
    args = parser.parse_args()
    if args.commit_every <= 0:
        parser.error("--commit-every must be greater than zero")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    try:
        replay_commands(
            args.user,
            args.start,
            resume=args.resume,
            force=args.force,
            batch_commit_size=args.commit_every,
        )
    except ValueError as exc:
        logging.error("Replay agent argument error: %s", exc)
    except RuntimeError as exc:
        logging.error("Replay agent failed to connect to database: %s", exc)


if __name__ == "__main__":
    main()
