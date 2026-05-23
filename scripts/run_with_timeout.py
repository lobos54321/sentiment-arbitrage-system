#!/usr/bin/env python3
"""Run a subprocess with a hard timeout and optional append-only log capture."""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path


def _write_log(path: str | None, message: str) -> None:
    if not path:
        return
    log_path = Path(path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(message)
        if not message.endswith("\n"):
            fh.write("\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout-sec", type=float, default=45.0)
    parser.add_argument("--log")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    command = list(args.command)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        parser.error("command is required after --")

    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _write_log(args.log, f"[timeout-wrapper] {started} starting timeout={args.timeout_sec}s command={' '.join(command)}")
    try:
        result = subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=max(1.0, float(args.timeout_sec)),
        )
    except subprocess.TimeoutExpired as exc:
        if exc.stdout:
            _write_log(args.log, exc.stdout if isinstance(exc.stdout, str) else exc.stdout.decode("utf-8", "replace"))
        if exc.stderr:
            _write_log(args.log, exc.stderr if isinstance(exc.stderr, str) else exc.stderr.decode("utf-8", "replace"))
        ended = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _write_log(args.log, f"[timeout-wrapper] {ended} timed out after {args.timeout_sec}s")
        return 124

    if result.stdout:
        _write_log(args.log, result.stdout)
    if result.stderr:
        _write_log(args.log, result.stderr)
    ended = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _write_log(args.log, f"[timeout-wrapper] {ended} exit status={result.returncode}")
    return int(result.returncode or 0)


if __name__ == "__main__":
    raise SystemExit(main())
