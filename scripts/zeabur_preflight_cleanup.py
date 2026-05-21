#!/usr/bin/env python3
"""Lightweight Zeabur volume preflight for paper-only runtime.

The goal is to recover observability when the persistent volume is under
pressure. Keep this script dependency-free so it can run before Node/Python
sidecars start.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
from pathlib import Path


DATA_DIR = Path(os.environ.get("ZEABUR_DATA_DIR", "/app/data"))
MAX_LOG_BYTES = int(float(os.environ.get("ZEABUR_LOG_TRIM_MAX_MB", "256")) * 1024 * 1024)
KEEP_LOG_BYTES = int(float(os.environ.get("ZEABUR_LOG_TRIM_KEEP_MB", "64")) * 1024 * 1024)
DELETE_LARGE_TMP = os.environ.get("ZEABUR_DELETE_LARGE_TMP", "false").lower() == "true"
TMP_DELETE_BYTES = int(float(os.environ.get("ZEABUR_TMP_DELETE_MIN_MB", "256")) * 1024 * 1024)
DISK_WARN_FREE_BYTES = int(float(os.environ.get("ZEABUR_DISK_WARN_FREE_MB", "256")) * 1024 * 1024)

LOG_NAMES = [
    "node.log",
    "runtime.log",
    "paper-trader.log",
    "paper-fast-lane.log",
    "paper-review-snapshot.log",
    "source-resonance.log",
    "gmgn-scout.log",
    "lifecycle.log",
    "social-service.log",
]

DB_NAMES = [
    "paper_trades.db",
    "sentiment_arb.db",
    "kline_cache.db",
    "lifecycle_tracks.db",
]


def log(message: str) -> None:
    print(f"[preflight] {message}", flush=True)


def disk_report(label: str) -> None:
    try:
        usage = shutil.disk_usage(DATA_DIR)
        log(
            f"{label} disk total={usage.total // (1024 * 1024)}MB "
            f"used={usage.used // (1024 * 1024)}MB free={usage.free // (1024 * 1024)}MB"
        )
        if usage.free < DISK_WARN_FREE_BYTES:
            log(f"WARN low disk free={usage.free // (1024 * 1024)}MB")
    except Exception as exc:
        log(f"WARN disk usage failed: {exc}")


def trim_file(path: Path, *, max_bytes: int = MAX_LOG_BYTES, keep_bytes: int = KEEP_LOG_BYTES) -> None:
    try:
        if not path.exists() or not path.is_file():
            return
        size = path.stat().st_size
        if size <= max_bytes:
            return
        tmp = path.with_suffix(path.suffix + ".trim")
        try:
            with path.open("rb") as src:
                src.seek(max(0, size - keep_bytes))
                data = src.read()
            with tmp.open("wb") as dst:
                dst.write(data)
            os.replace(tmp, path)
            log(f"trimmed {path} {size // (1024 * 1024)}MB -> {path.stat().st_size // (1024 * 1024)}MB")
        except Exception as exc:
            log(f"WARN trim-copy failed for {path}: {exc}; leaving original log intact")
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception as exc:
        log(f"WARN trim failed for {path}: {exc}")


def remove_large_temp_files() -> None:
    if not DELETE_LARGE_TMP:
        log("large temp deletion disabled (set ZEABUR_DELETE_LARGE_TMP=true to enable)")
        return
    if not DATA_DIR.exists():
        return
    for pattern in ("*.tmp", "*.download", "*.partial", "*.trim"):
        for path in DATA_DIR.rglob(pattern):
            try:
                if path.is_file() and path.stat().st_size >= TMP_DELETE_BYTES:
                    size = path.stat().st_size
                    path.unlink()
                    log(f"removed temp {path} size={size // (1024 * 1024)}MB")
            except Exception as exc:
                log(f"WARN remove temp failed for {path}: {exc}")


def checkpoint_db(path: Path) -> None:
    if not path.exists():
        return
    try:
        conn = sqlite3.connect(str(path), timeout=5)
        try:
            conn.execute("PRAGMA busy_timeout=5000")
            row = conn.execute("PRAGMA quick_check").fetchone()
            status = row[0] if row else "unknown"
            if status != "ok":
                log(f"WARN quick_check {path.name}: {status}")
                marker = path.with_suffix(path.suffix + ".integrity_error")
                marker.write_text(str(status)[:2000], encoding="utf-8")
                return
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            log(f"checkpoint ok {path.name}")
        finally:
            conn.close()
    except Exception as exc:
        log(f"WARN checkpoint failed {path.name}: {exc}")


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    disk_report("before")
    for name in LOG_NAMES:
        trim_file(DATA_DIR / name)
    remove_large_temp_files()
    for name in DB_NAMES:
        checkpoint_db(DATA_DIR / name)
    disk_report("after")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
