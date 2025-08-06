"""Command line interface for interacting with a pg_shell deployment.

This module provides helper functions to submit commands, fork or replay
sessions, and stream command output. It is primarily a thin wrapper around the
HTTP API exposed by the server.
"""

import argparse
import os
import sys
import time
from typing import Any

import requests


DEFAULT_TIMEOUT = float(os.getenv("PG_SHELL_TIMEOUT", "30"))


def exec_command(base_url: str, user_id: str, cmd: str, timeout: float = DEFAULT_TIMEOUT) -> None:
    """Submit a command for execution.

    Parameters
    ----------
    base_url : str
        PostgREST base URL.
    user_id : str
        Identifier of the target user.
    cmd : str
        Command text to queue for execution.
    timeout : float, optional
        HTTP request timeout seconds.

    Returns
    -------
    None
    """

    resp = requests.post(
        f"{base_url}/rpc/submit_command",
        json={"user_id": user_id, "command": cmd},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    print(data)


def fork_session(base_url: str, user_id: str, command_id: int, timeout: float = DEFAULT_TIMEOUT) -> None:
    """Create a new session starting from a previous command.

    Parameters
    ----------
    base_url : str
        PostgREST base URL.
    user_id : str
        User owning the new session.
    command_id : int
        Identifier of the command to fork at.
    timeout : float, optional
        HTTP request timeout seconds.

    Returns
    -------
    None
    """

    resp = requests.post(
        f"{base_url}/rpc/fork_session",
        json={"user_id": user_id, "source_command_id": command_id},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    print(data)


def replay_session(base_url: str, session: str, timeout: float = DEFAULT_TIMEOUT) -> None:
    """Request a replay of a past session.

    Parameters
    ----------
    base_url : str
        PostgREST base URL.
    session : str
        Timestamp or session ID to replay.
    timeout : float, optional
        HTTP request timeout seconds.

    Returns
    -------
    None
    """

    resp = requests.post(
        f"{base_url}/rpc/replay_session",
        json={"session": session},
        timeout=timeout,
    )
    resp.raise_for_status()
    data = resp.json()
    print(data)


def tail_output(
    base_url: str,
    user_id: str,
    interval: float = 1.0,
    since: int | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> None:
    """Poll for new output and print it continuously."""
    last_id: Any = since
    try:
        while True:
            params = {"user_id": f"eq.{user_id}"}
            resp = requests.get(
                f"{base_url}/rpc/latest_output", params=params, timeout=timeout
            )
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
    """Entry point for the CLI.

    Parameters
    ----------
    argv : list[str] | None, optional
        Argument list to parse instead of ``sys.argv``.

    Returns
    -------
    int
        Exit status code.
    """

    parser = argparse.ArgumentParser(description="pg_shell CLI")
    parser.add_argument(
        "--base-url",
        default=os.getenv("PG_SHELL_API", "http://localhost:3000"),
        help="PostgREST base URL",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help="HTTP request timeout seconds",
    )
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
        exec_command(args.base_url, args.user, args.cmd, args.timeout)
    elif args.command == "replay":
        replay_session(args.base_url, args.session, args.timeout)
    elif args.command == "fork":
        fork_session(args.base_url, args.user, args.at, args.timeout)
    elif args.command == "tail":
        tail_output(args.base_url, args.user, args.interval, args.since, args.timeout)
    else:
        parser.print_help()
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
