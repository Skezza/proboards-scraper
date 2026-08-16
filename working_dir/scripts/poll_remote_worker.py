#!/usr/bin/env python3
"""Poll a remote user-level scraper service through SSH."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone


REMOTE_CHECK = r'''
set -o pipefail
service_state=$(systemctl --user is-active oatcake-worker.service 2>/dev/null || true)
health=$(curl -fsS --max-time 8 http://127.0.0.1:18080/health 2>/dev/null || true)
printf '%s\n%s\n' "$service_state" "$health"
'''


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", action="append", required=True, help="SSH host to try, in order")
    parser.add_argument("--user", default="joe")
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument("--timeout", type=float, default=12.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def poll(args: argparse.Namespace) -> tuple[str | None, dict[str, object]]:
    last_error = "no hosts configured"
    for host in args.host:
        try:
            result = subprocess.run(
                [
                    "ssh",
                    "-o",
                    "BatchMode=yes",
                    "-o",
                    f"ConnectTimeout={max(1, int(args.timeout))}",
                    f"{args.user}@{host}",
                    "bash",
                    "-s",
                ],
                input=REMOTE_CHECK,
                text=True,
                capture_output=True,
                timeout=args.timeout + 3,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            last_error = str(exc)
            continue

        if result.returncode != 0:
            last_error = result.stderr.strip() or f"ssh exit {result.returncode}"
            continue

        lines = result.stdout.splitlines()
        service_state = lines[0].strip() if lines else "unknown"
        health: object = None
        if len(lines) > 1 and lines[1].strip():
            try:
                health = json.loads(lines[1])
            except json.JSONDecodeError:
                health = {"raw": lines[1].strip()}
        return host, {"service": service_state, "health": health}

    return None, {"error": last_error}


def main() -> int:
    args = parse_args()
    previous_key: object = object()
    while True:
        host, result = poll(args)
        state: object = {"host": host, **result}
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        line = json.dumps({"timestamp": timestamp, **state}, sort_keys=True)
        print(line, flush=True)

        health = result.get("health")
        progress = health.get("progress", {}) if isinstance(health, dict) else {}
        state_key = (
            host,
            result.get("service"),
            health.get("status") if isinstance(health, dict) else None,
            health.get("last_worker_state") if isinstance(health, dict) else None,
            progress.get("threads_remaining_estimated") if isinstance(progress, dict) else None,
            progress.get("page_coverage_ratio") if isinstance(progress, dict) else None,
        )
        if state_key != previous_key:
            print(f"remote worker state changed: {line}", file=sys.stderr, flush=True)
            previous_key = state_key
        if args.once:
            return 0 if host is not None else 1
        time.sleep(max(1.0, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
