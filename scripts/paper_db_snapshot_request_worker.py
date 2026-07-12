#!/usr/bin/env python3
"""Process an approved paper DB snapshot request from the long-lived supervisor."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import shutil
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

try:
    from scripts.sqlite_evidence_utils import atomic_write_json, sha256_file
    from scripts.zeabur_preflight_cleanup import create_consistent_sqlite_snapshot
except ImportError:
    from sqlite_evidence_utils import atomic_write_json, sha256_file
    from zeabur_preflight_cleanup import create_consistent_sqlite_snapshot


SCHEMA_VERSION = "paper_db_snapshot_request_worker.v1"
REQUEST_SCHEMA_VERSION = "paper_db_snapshot_request.v1"
DEFAULT_DATA_DIR = Path(os.environ.get("ZEABUR_DATA_DIR", "/app/data"))
DEFAULT_RECOVERY_DIR = Path(os.environ.get("ZEABUR_RECOVERY_DIR", str(DEFAULT_DATA_DIR / "recovery")))
CRITICAL_TABLES = (
    "candidate_shadow_observations",
    "candidate_shadow_virtual_trades",
    "paper_trades",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_request_id(value: object) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip()).strip("-.")
    if not normalized or len(normalized) > 96:
        raise ValueError("request_id must be 1-96 safe filename characters")
    return normalized


def read_json_object(path: Path) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def acquire_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("w", encoding="utf-8")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    handle.write(str(os.getpid()))
    handle.flush()
    return handle


def snapshot_table_counts(path: Path) -> dict[str, int | None]:
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    counts: dict[str, int | None] = {}
    try:
        available = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        for table in CRITICAL_TABLES:
            counts[table] = int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]) if table in available else None
    finally:
        connection.close()
    return counts


def status_payload(*, request_id: str | None, state: str, attempt_count: int, **extra) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "request_id": request_id,
        "state": state,
        "attempt_count": attempt_count,
        "source_read_only": True,
        "deletes_backups": False,
        "promotion_allowed": False,
        **extra,
    }


def run_once(
    *,
    request_path: Path,
    status_path: Path,
    source_path: Path,
    recovery_dir: Path,
    archive_dir: Path,
    max_attempts: int = 3,
) -> dict:
    if not request_path.exists():
        return status_payload(request_id=None, state="idle_no_request", attempt_count=0)

    request = read_json_object(request_path)
    if request.get("schema_version") != REQUEST_SCHEMA_VERSION:
        raise ValueError(f"unsupported request schema: {request.get('schema_version')}")
    request_id = safe_request_id(request.get("request_id"))
    previous = {}
    if status_path.exists():
        try:
            previous = read_json_object(status_path)
        except Exception:
            previous = {}
    previous_attempts = int(previous.get("attempt_count") or 0) if previous.get("request_id") == request_id else 0
    if previous_attempts >= max(1, int(max_attempts)) and previous.get("state") == "failed":
        exhausted = status_payload(
            request_id=request_id,
            state="attempts_exhausted",
            attempt_count=previous_attempts,
            error=previous.get("error"),
        )
        atomic_write_json(status_path, exhausted)
        return exhausted

    recovery_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    attempt_count = previous_attempts + 1
    final_dir = recovery_dir / f"paper_trades_verified_{request_id}"
    snapshot_path = final_dir / "paper_trades.db"
    manifest_path = final_dir / "manifest.json"
    if snapshot_path.is_file() and manifest_path.is_file():
        archive_path = archive_dir / f"completed_{request_id}.json"
        archive_error = None
        try:
            os.replace(request_path, archive_path)
        except Exception as exc:
            archive_error = f"{type(exc).__name__}: {exc}"
        completed = status_payload(
            request_id=request_id,
            state="completed_existing",
            attempt_count=previous_attempts,
            snapshot_path=str(snapshot_path),
            manifest_path=str(manifest_path),
            request_archive_error=archive_error,
        )
        atomic_write_json(status_path, completed)
        return completed
    if final_dir.exists():
        raise RuntimeError(f"snapshot destination exists without complete manifest: {final_dir}")

    partial_dir = recovery_dir / f".paper_trades_verified_{request_id}.{os.getpid()}.{time.time_ns()}.partial"
    running = status_payload(
        request_id=request_id,
        state="running",
        attempt_count=attempt_count,
        source_path=str(source_path),
        partial_dir=str(partial_dir),
        snapshot_path=str(snapshot_path),
        manifest_path=str(manifest_path),
        reason=request.get("reason"),
    )
    atomic_write_json(status_path, running)
    partial_dir.mkdir(parents=True, exist_ok=False)
    try:
        partial_snapshot = partial_dir / "paper_trades.db"
        snapshot = create_consistent_sqlite_snapshot(source_path, partial_snapshot)
        snapshot["sha256"] = sha256_file(partial_snapshot)
        snapshot["critical_table_counts"] = snapshot_table_counts(partial_snapshot)
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": utc_now_iso(),
            "request": request,
            "source_path": str(source_path),
            "source_read_only": True,
            "snapshot": snapshot,
            "promotion_allowed": False,
        }
        atomic_write_json(partial_dir / "manifest.json", manifest)
        os.replace(partial_dir, final_dir)
        archive_path = archive_dir / f"completed_{request_id}.json"
        archive_error = None
        try:
            os.replace(request_path, archive_path)
        except Exception as exc:
            archive_error = f"{type(exc).__name__}: {exc}"
        completed = status_payload(
            request_id=request_id,
            state="completed",
            attempt_count=attempt_count,
            snapshot_path=str(snapshot_path),
            manifest_path=str(manifest_path),
            snapshot_sha256=snapshot["sha256"],
            quick_check=snapshot["quick_check"],
            critical_table_counts=snapshot["critical_table_counts"],
            request_archive_error=archive_error,
        )
        atomic_write_json(status_path, completed)
        return completed
    except Exception as exc:
        shutil.rmtree(partial_dir, ignore_errors=True)
        failed = status_payload(
            request_id=request_id,
            state="failed",
            attempt_count=attempt_count,
            error=f"{type(exc).__name__}: {exc}",
        )
        atomic_write_json(status_path, failed)
        return failed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", default=str(DEFAULT_RECOVERY_DIR / "paper_db_snapshot_request.json"))
    parser.add_argument("--status", default=str(DEFAULT_RECOVERY_DIR / "paper_db_snapshot_status.json"))
    parser.add_argument("--source", default=os.environ.get("PAPER_DB", str(DEFAULT_DATA_DIR / "paper_trades.db")))
    parser.add_argument("--recovery-dir", default=str(DEFAULT_RECOVERY_DIR))
    parser.add_argument("--archive-dir", default=str(DEFAULT_RECOVERY_DIR / "paper_db_snapshot_requests"))
    parser.add_argument("--lock-file", default="/tmp/paper_db_snapshot_request_worker.lock")
    parser.add_argument("--max-attempts", type=int, default=3)
    args = parser.parse_args()

    lock_handle = acquire_lock(Path(args.lock_file))
    if lock_handle is None:
        print(json.dumps(status_payload(request_id=None, state="lock_held", attempt_count=0), sort_keys=True))
        return 0
    try:
        result = run_once(
            request_path=Path(args.request),
            status_path=Path(args.status),
            source_path=Path(args.source),
            recovery_dir=Path(args.recovery_dir),
            archive_dir=Path(args.archive_dir),
            max_attempts=args.max_attempts,
        )
        print(json.dumps(result, sort_keys=True))
        return 1 if result.get("state") in {"failed", "attempts_exhausted"} else 0
    finally:
        lock_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
