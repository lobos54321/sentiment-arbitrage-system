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
CHECKPOINT_SCHEMA_VERSION = "paper_db_snapshot_checkpoint.v1"
DEFAULT_DATA_DIR = Path(os.environ.get("ZEABUR_DATA_DIR", "/app/data"))
DEFAULT_RECOVERY_DIR = Path(os.environ.get("ZEABUR_RECOVERY_DIR", str(DEFAULT_DATA_DIR / "recovery")))
CRITICAL_TABLES = (
    "candidate_shadow_observations",
    "candidate_shadow_virtual_trades",
    "paper_trades",
)
LOCAL_VERIFY_FREE_SPACE_MARGIN_BYTES = 4 * 1024 * 1024 * 1024


class LocalVerificationUnavailable(RuntimeError):
    """Local verification could not run; the persistent snapshot remains reusable."""


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


def snapshot_quick_check(path: Path) -> list[str]:
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA mmap_size=0")
        connection.execute("PRAGMA cache_size=-8192")
        result = [str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()]
    finally:
        connection.close()
    if result != ["ok"]:
        raise RuntimeError(f"snapshot quick_check failed: {result[:20]}")
    return result


def snapshot_quick_check_evidence(
    path: Path,
    *,
    local_verify_dir: Path | None = None,
    require_distinct_filesystem: bool = True,
) -> dict:
    if local_verify_dir is None:
        return {
            "quick_check": snapshot_quick_check(path),
            "quick_check_method": "direct_snapshot_full_quick_check",
        }

    try:
        local_verify_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise LocalVerificationUnavailable(f"cannot create local verify directory: {exc}") from exc
    source_stat = path.stat()
    try:
        local_dir_stat = local_verify_dir.stat()
    except OSError as exc:
        raise LocalVerificationUnavailable(f"cannot inspect local verify directory: {exc}") from exc
    distinct_filesystem = source_stat.st_dev != local_dir_stat.st_dev
    if require_distinct_filesystem and not distinct_filesystem:
        raise LocalVerificationUnavailable(
            "local verify directory must be on a different filesystem from the snapshot"
        )
    safe_parent = re.sub(r"[^A-Za-z0-9_.-]+", "-", path.parent.name).strip("-.") or "snapshot"
    local_copy = local_verify_dir / f"{safe_parent}-{path.name}.verify"
    try:
        local_copy.unlink(missing_ok=True)
        free_bytes = shutil.disk_usage(local_verify_dir).free
    except OSError as exc:
        raise LocalVerificationUnavailable(f"cannot prepare local verify directory: {exc}") from exc
    required_bytes = source_stat.st_size + LOCAL_VERIFY_FREE_SPACE_MARGIN_BYTES
    if free_bytes < required_bytes:
        raise LocalVerificationUnavailable(
            f"local verify directory has insufficient space: free={free_bytes} required={required_bytes}"
        )

    evidence = None
    primary_error: BaseException | None = None
    try:
        try:
            shutil.copyfile(path, local_copy)
        except OSError as exc:
            raise LocalVerificationUnavailable(f"cannot create local verification copy: {exc}") from exc
        try:
            local_size = local_copy.stat().st_size
        except OSError as exc:
            raise LocalVerificationUnavailable(f"cannot inspect local verification copy: {exc}") from exc
        if local_size != source_stat.st_size:
            raise LocalVerificationUnavailable(
                f"local verification copy size mismatch: source={source_stat.st_size} local={local_size}"
            )
        try:
            quick_check = snapshot_quick_check(local_copy)
        except sqlite3.OperationalError as exc:
            raise LocalVerificationUnavailable(f"local quick_check I/O failure: {exc}") from exc
        try:
            local_sha256 = sha256_file(local_copy)
        except OSError as exc:
            raise LocalVerificationUnavailable(f"cannot hash local verification copy: {exc}") from exc
        evidence = {
            "quick_check": quick_check,
            "quick_check_method": "ephemeral_local_copy_full_quick_check",
            "quick_check_local_copy_sha256": local_sha256,
            "quick_check_local_copy_size_bytes": local_size,
            "quick_check_snapshot_device": source_stat.st_dev,
            "quick_check_local_device": local_dir_stat.st_dev,
            "quick_check_distinct_filesystem": distinct_filesystem,
        }
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        try:
            local_copy.unlink(missing_ok=True)
        except OSError as exc:
            if primary_error is None:
                raise LocalVerificationUnavailable(f"cannot remove local verification copy: {exc}") from exc
    evidence["quick_check_local_copy_removed"] = not local_copy.exists()
    return evidence


def checkpoint_payload(*, request_id: str, attempt_count: int, stage: str, snapshot: dict) -> dict:
    return {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "request_id": request_id,
        "attempt_count": attempt_count,
        "stage": stage,
        "snapshot": snapshot,
    }


def read_checkpoint(path: Path, *, request_id: str) -> dict | None:
    if not path.is_file():
        return None
    checkpoint = read_json_object(path)
    if checkpoint.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
        raise ValueError(f"unsupported checkpoint schema: {checkpoint.get('schema_version')}")
    if checkpoint.get("request_id") != request_id:
        raise ValueError("checkpoint request_id mismatch")
    if not isinstance(checkpoint.get("snapshot"), dict):
        raise ValueError("checkpoint snapshot metadata missing")
    return checkpoint


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
    local_verify_dir: Path | None = None,
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
    recovery_dir.mkdir(parents=True, exist_ok=True)
    archive_dir.mkdir(parents=True, exist_ok=True)
    final_dir = recovery_dir / f"paper_trades_verified_{request_id}"
    snapshot_path = final_dir / "paper_trades.db"
    manifest_path = final_dir / "manifest.json"
    partial_dir = recovery_dir / f".paper_trades_verified_{request_id}.partial"
    partial_snapshot = partial_dir / "paper_trades.db"
    checkpoint_path = partial_dir / "checkpoint.json"
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
    checkpoint = read_checkpoint(checkpoint_path, request_id=request_id) if partial_dir.exists() else None
    if checkpoint is None and previous_attempts >= max(1, int(max_attempts)):
        exhausted = status_payload(
            request_id=request_id,
            state="attempts_exhausted",
            attempt_count=previous_attempts,
            error=previous.get("error"),
        )
        atomic_write_json(status_path, exhausted)
        return exhausted
    integrity_marker = Path(f"{source_path}.integrity_error")
    if checkpoint is None and integrity_marker.exists():
        blocked = status_payload(
            request_id=request_id,
            state="blocked_source_integrity_marker",
            attempt_count=previous_attempts,
            integrity_marker=str(integrity_marker),
        )
        atomic_write_json(status_path, blocked)
        return blocked
    if final_dir.exists():
        raise RuntimeError(f"snapshot destination exists without complete manifest: {final_dir}")

    if checkpoint is None:
        attempt_count = previous_attempts + 1
        if partial_dir.exists():
            shutil.rmtree(partial_dir)
        running = status_payload(
            request_id=request_id,
            state="running_snapshot_copy",
            stage="snapshot_copy",
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
            snapshot = create_consistent_sqlite_snapshot(source_path, partial_snapshot, verify=False)
            checkpoint = checkpoint_payload(
                request_id=request_id,
                attempt_count=attempt_count,
                stage="snapshot_copied",
                snapshot=snapshot,
            )
            atomic_write_json(checkpoint_path, checkpoint)
            copied = status_payload(
                request_id=request_id,
                state="running_snapshot_copied",
                stage="snapshot_copied",
                attempt_count=attempt_count,
                source_path=str(source_path),
                partial_dir=str(partial_dir),
                snapshot_path=str(snapshot_path),
                manifest_path=str(manifest_path),
                reason=request.get("reason"),
            )
            atomic_write_json(status_path, copied)
            return copied
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

    attempt_count = max(previous_attempts, int(checkpoint.get("attempt_count") or 0))
    snapshot = dict(checkpoint["snapshot"])
    stage = str(checkpoint.get("stage") or "")
    if not partial_snapshot.is_file():
        raise RuntimeError(f"checkpoint snapshot missing: {partial_snapshot}")

    running_fields = {
        "attempt_count": attempt_count,
        "source_path": str(source_path),
        "partial_dir": str(partial_dir),
        "snapshot_path": str(snapshot_path),
        "manifest_path": str(manifest_path),
        "reason": request.get("reason"),
    }
    try:
        if stage == "snapshot_copied":
            running = status_payload(
                request_id=request_id,
                state="running_quick_check",
                stage="quick_check",
                **running_fields,
            )
            atomic_write_json(status_path, running)
            snapshot.update(
                snapshot_quick_check_evidence(
                    partial_snapshot,
                    local_verify_dir=local_verify_dir,
                )
            )
            checkpoint = checkpoint_payload(
                request_id=request_id,
                attempt_count=attempt_count,
                stage="quick_check_complete",
                snapshot=snapshot,
            )
            atomic_write_json(checkpoint_path, checkpoint)
            completed_stage = status_payload(
                request_id=request_id,
                state="running_quick_check_complete",
                stage="quick_check_complete",
                **running_fields,
            )
            atomic_write_json(status_path, completed_stage)
            return completed_stage

        if stage == "quick_check_complete":
            running = status_payload(
                request_id=request_id,
                state="running_sha256",
                stage="sha256",
                **running_fields,
            )
            atomic_write_json(status_path, running)
            snapshot["sha256"] = sha256_file(partial_snapshot)
            local_copy_sha256 = snapshot.get("quick_check_local_copy_sha256")
            if local_copy_sha256 is not None and local_copy_sha256 != snapshot["sha256"]:
                raise RuntimeError(
                    "snapshot sha256 does not match the byte-identical local copy used for quick_check"
                )
            snapshot["quick_check_sha256_match"] = (
                local_copy_sha256 == snapshot["sha256"] if local_copy_sha256 is not None else None
            )
            checkpoint = checkpoint_payload(
                request_id=request_id,
                attempt_count=attempt_count,
                stage="sha256_complete",
                snapshot=snapshot,
            )
            atomic_write_json(checkpoint_path, checkpoint)
            completed_stage = status_payload(
                request_id=request_id,
                state="running_sha256_complete",
                stage="sha256_complete",
                **running_fields,
            )
            atomic_write_json(status_path, completed_stage)
            return completed_stage

        if stage == "sha256_complete":
            running = status_payload(
                request_id=request_id,
                state="running_critical_counts",
                stage="critical_counts",
                **running_fields,
            )
            atomic_write_json(status_path, running)
            snapshot["critical_table_counts"] = snapshot_table_counts(partial_snapshot)
            missing = [name for name, count in snapshot["critical_table_counts"].items() if count is None]
            if missing:
                raise RuntimeError(f"critical tables missing from snapshot: {missing}")
            checkpoint = checkpoint_payload(
                request_id=request_id,
                attempt_count=attempt_count,
                stage="critical_counts_complete",
                snapshot=snapshot,
            )
            atomic_write_json(checkpoint_path, checkpoint)
            completed_stage = status_payload(
                request_id=request_id,
                state="running_critical_counts_complete",
                stage="critical_counts_complete",
                **running_fields,
            )
            atomic_write_json(status_path, completed_stage)
            return completed_stage

        if stage != "critical_counts_complete":
            raise ValueError(f"unsupported checkpoint stage: {stage}")

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
    except LocalVerificationUnavailable as exc:
        blocked = status_payload(
            request_id=request_id,
            state="blocked_local_verification",
            stage=stage,
            attempt_count=attempt_count,
            partial_dir=str(partial_dir),
            error=f"{type(exc).__name__}: {exc}",
        )
        atomic_write_json(status_path, blocked)
        return blocked
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
    parser.add_argument("--local-verify-dir", default=os.environ.get("PAPER_DB_SNAPSHOT_LOCAL_VERIFY_DIR"))
    parser.add_argument("--lock-file", default="/tmp/paper_db_snapshot_request_worker.lock")
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=float, default=300.0)
    parser.add_argument("--active-interval", type=float, default=5.0)
    args = parser.parse_args()

    lock_handle = acquire_lock(Path(args.lock_file))
    if lock_handle is None:
        print(json.dumps(status_payload(request_id=None, state="lock_held", attempt_count=0), sort_keys=True))
        return 0
    try:
        while True:
            result = run_once(
                request_path=Path(args.request),
                status_path=Path(args.status),
                source_path=Path(args.source),
                recovery_dir=Path(args.recovery_dir),
                archive_dir=Path(args.archive_dir),
                max_attempts=args.max_attempts,
                local_verify_dir=Path(args.local_verify_dir) if args.local_verify_dir else None,
            )
            print(json.dumps(result, sort_keys=True), flush=True)
            if not args.loop:
                if str(result.get("state") or "").startswith("running_"):
                    continue
                return 1 if result.get("state") in {"failed", "attempts_exhausted"} else 0
            sleep_seconds = args.active_interval if str(result.get("state") or "").startswith("running_") else args.interval
            time.sleep(max(1.0, float(sleep_seconds)))
    finally:
        lock_handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
