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


def exec_command(
    base_url: str, user_id: str, cmd: str, timeout: float = DEFAULT_TIMEOUT
) -> int:
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
    int
        Status code, ``0`` on success, non-zero on failure.
    """

    try:
        resp = requests.post(
            f"{base_url}/rpc/submit_command",
            json={"user_id": user_id, "command": cmd},
            timeout=timeout,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - network failure
        print(f"submit failed: {exc}", file=sys.stderr)
        return 1

    data = resp.json()
    print(data)
    return 0


def fork_session(
    base_url: str, user_id: str, command_id: int, timeout: float = DEFAULT_TIMEOUT
) -> int:
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
    int
        Status code, ``0`` on success, non-zero on failure.
    """

    try:
        resp = requests.post(
            f"{base_url}/rpc/fork_session",
            json={"user_id": user_id, "source_command_id": command_id},
            timeout=timeout,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - network failure
        print(f"fork failed: {exc}", file=sys.stderr)
        return 1

    data = resp.json()
    print(data)
    return 0


def replay_session(base_url: str, session: str, timeout: float = DEFAULT_TIMEOUT) -> int:
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
    int
        Status code, ``0`` on success, non-zero on failure.
    """

    try:
        resp = requests.post(
            f"{base_url}/rpc/replay_session",
            json={"session": session},
            timeout=timeout,
        )
        resp.raise_for_status()
    except requests.RequestException as exc:  # pragma: no cover - network failure
        print(f"replay failed: {exc}", file=sys.stderr)
        return 1

    data = resp.json()
    print(data)
    return 0


def tail_output(
    base_url: str,
    user_id: str,
    interval: float = 1.0,
    since: int | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_polls: int | None = None,
) -> int:
    """Poll for new output and print it continuously.

    Returns
    -------
    int
        Status code, ``0`` on success, non-zero on failure.
    """
    if max_polls is not None and max_polls <= 0:
        return 0

    last_id: Any = since
    polls_remaining = max_polls
    try:
        while True:
            params = {"p_user_id": f"eq.{user_id}"}

            if last_id is not None:
                params["p_since_id"] = last_id

            try:
                resp = requests.get(
                    f"{base_url}/rpc/latest_output", params=params, timeout=timeout
                )
                resp.raise_for_status()
            except requests.RequestException as exc:  # pragma: no cover - network failure
                print(f"tail failed: {exc}", file=sys.stderr)
                return 1

            rows = resp.json()
            for row in rows:
                print(f"$ {row['command']}")
                if row.get('output'):
                    print(row['output'])
                print(f"(exit {row.get('exit_code')})")
                if last_id is None or row["id"] > last_id:
                    last_id = row["id"]

            if polls_remaining is not None:
                polls_remaining -= 1
                if polls_remaining <= 0:
                    break

            time.sleep(interval)
    except KeyboardInterrupt:
        return 0

    return 0


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
    tail_p.add_argument("--max-polls", type=int, help="Maximum number of polls before exiting")

    args = parser.parse_args(argv)

    if args.command == "exec":
        rc = exec_command(args.base_url, args.user, args.cmd, args.timeout)
    elif args.command == "replay":
        rc = replay_session(args.base_url, args.session, args.timeout)
    elif args.command == "fork":
        rc = fork_session(args.base_url, args.user, args.at, args.timeout)
    elif args.command == "tail":
        rc = tail_output(
            args.base_url,
            args.user,
            args.interval,
            args.since,
            args.timeout,
            args.max_polls,
        )
    else:
        parser.print_help()
        return 1

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
