import argparse
import os
import sys
import time
from typing import Any

import requests


def exec_command(base_url: str, user_id: str, cmd: str) -> None:
    resp = requests.post(f"{base_url}/rpc/submit_command", json={"user_id": user_id, "command": cmd})
    resp.raise_for_status()
    data = resp.json()
    print(data)


def fork_session(base_url: str, user_id: str, command_id: int) -> None:
    resp = requests.post(f"{base_url}/rpc/fork_session", json={"user_id": user_id, "source_command_id": command_id})
    resp.raise_for_status()
    data = resp.json()
    print(data)


def replay_session(base_url: str, session: str) -> None:
    resp = requests.post(f"{base_url}/rpc/replay_session", json={"session": session})
    resp.raise_for_status()
    data = resp.json()
    print(data)


def tail_output(
    base_url: str, user_id: str, interval: float = 1.0, since: int | None = None
) -> None:
    """Poll for new output and print it continuously."""
    last_id: Any = since
    try:
        while True:
            params = {"user_id": f"eq.{user_id}"}
            resp = requests.get(f"{base_url}/rpc/latest_output", params=params)
            resp.raise_for_status()
            rows = resp.json()
            for row in rows:
                if last_id is None or row["id"] > last_id:
                    print(f"$ {row['command']}")
                    if row.get('output'):
                        print(row['output'])
                    print(f"(exit {row.get('exit_code')})")
                    last_id = row["id"]
            time.sleep(interval)
    except KeyboardInterrupt:
        pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="pg_shell CLI")
    parser.add_argument("--base-url", default=os.getenv("PG_SHELL_API", "http://localhost:3000"), help="PostgREST base URL")
    sub = parser.add_subparsers(dest="command", required=True)

    exec_p = sub.add_parser("exec", help="Submit a command")
    exec_p.add_argument("--user", required=True, help="User ID")
    exec_p.add_argument("--cmd", required=True, help="Command text")

    replay_p = sub.add_parser("replay", help="Replay a session")
    replay_p.add_argument("--session", required=True, help="Session timestamp or ID")

    fork_p = sub.add_parser("fork", help="Fork session at a command")
    fork_p.add_argument("--user", required=True, help="User ID")
    fork_p.add_argument("--at", type=int, required=True, help="Command ID to fork at")

    tail_p = sub.add_parser("tail", help="Tail latest output")
    tail_p.add_argument("--user", required=True, help="User ID")
    tail_p.add_argument("--interval", type=float, default=1.0, help="Polling interval seconds")
    tail_p.add_argument("--since", type=int, help="Start tailing after this command ID")

    args = parser.parse_args(argv)

    if args.command == "exec":
        exec_command(args.base_url, args.user, args.cmd)
    elif args.command == "replay":
        replay_session(args.base_url, args.session)
    elif args.command == "fork":
        fork_session(args.base_url, args.user, args.at)
    elif args.command == "tail":
        tail_output(args.base_url, args.user, args.interval, args.since)
    else:
        parser.print_help()
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
