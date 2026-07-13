#!/usr/bin/env python3
"""Process an approved paper DB snapshot request from the long-lived supervisor."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
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
CHECKPOINT_SCHEMA_VERSION = "paper_db_snapshot_checkpoint.v2"
DEFAULT_DATA_DIR = Path(os.environ.get("ZEABUR_DATA_DIR", "/app/data"))
DEFAULT_RECOVERY_DIR = Path(os.environ.get("ZEABUR_RECOVERY_DIR", str(DEFAULT_DATA_DIR / "recovery")))
CRITICAL_TABLES = (
    "candidate_shadow_observations",
    "candidate_shadow_virtual_trades",
    "paper_trades",
)
LOCAL_VERIFY_FREE_SPACE_MARGIN_BYTES = 4 * 1024 * 1024 * 1024
REPAIR_MODE_NONE = "none"
REPAIR_MODE_VACUUM_REBUILD = "vacuum_rebuild"
ALLOWED_REPAIR_MODES = {REPAIR_MODE_NONE, REPAIR_MODE_VACUUM_REBUILD}
SAFE_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def request_fingerprint(request: dict) -> str:
    encoded = json.dumps(request, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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


def request_repair_mode(request: dict) -> str:
    mode = str(request.get("repair_mode") or REPAIR_MODE_NONE).strip().lower()
    if mode not in ALLOWED_REPAIR_MODES:
        raise ValueError(f"unsupported repair_mode: {mode}")
    if mode == REPAIR_MODE_VACUUM_REBUILD and request.get("allow_rebuild_corrupt_snapshot") is not True:
        raise ValueError("vacuum_rebuild requires allow_rebuild_corrupt_snapshot=true")
    return mode


def request_required_tables(request: dict) -> tuple[str, ...]:
    raw = request.get("required_tables")
    if raw is None:
        return CRITICAL_TABLES
    if not isinstance(raw, list) or not raw or len(raw) > 32:
        raise ValueError("required_tables must be a non-empty list with at most 32 entries")
    tables: list[str] = []
    for value in raw:
        name = str(value or "").strip()
        if not SAFE_TABLE_NAME_RE.fullmatch(name):
            raise ValueError(f"unsafe required table name: {name!r}")
        if name not in tables:
            tables.append(name)
    return tuple(tables)


def snapshot_table_counts(path: Path, tables: tuple[str, ...] = CRITICAL_TABLES) -> dict[str, int | None]:
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
        for table in tables:
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


def snapshot_quick_check_result(path: Path, *, max_errors: int = 100) -> list[str]:
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        connection.execute("PRAGMA mmap_size=0")
        connection.execute("PRAGMA cache_size=-8192")
        return [
            str(row[0])
            for row in connection.execute(f"PRAGMA quick_check({max(1, min(int(max_errors), 1000))})").fetchall()
        ]
    finally:
        connection.close()


def fsync_file(path: Path) -> None:
    flags = os.O_RDONLY | (getattr(os, "O_NOFOLLOW", 0))
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def fsync_dir(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def sqlite_sidecar_paths(path: Path) -> tuple[Path, ...]:
    return tuple(Path(f"{path}{suffix}") for suffix in ("-wal", "-shm", "-journal"))


def secure_regular_file_identity(path: Path, *, expected_owner: int | None = None) -> dict:
    item = path.lstat()
    if not stat.S_ISREG(item.st_mode):
        raise RuntimeError(f"expected regular file without symlink: {path}")
    owner = os.geteuid() if expected_owner is None else expected_owner
    if item.st_uid != owner:
        raise RuntimeError(f"unexpected file owner for {path}: {item.st_uid} != {owner}")
    if item.st_nlink != 1:
        raise RuntimeError(f"expected single-link worker file: {path} nlink={item.st_nlink}")
    return {
        "st_dev": item.st_dev,
        "st_ino": item.st_ino,
        "size_bytes": item.st_size,
        "mtime_ns": item.st_mtime_ns,
        "uid": item.st_uid,
        "nlink": item.st_nlink,
    }


def secure_private_dir(path: Path) -> dict:
    item = path.lstat()
    if not stat.S_ISDIR(item.st_mode):
        raise RuntimeError(f"expected real directory without symlink: {path}")
    if item.st_uid != os.geteuid():
        raise RuntimeError(f"unexpected directory owner for {path}: {item.st_uid}")
    if stat.S_IMODE(item.st_mode) & 0o077:
        raise RuntimeError(f"worker directory must not grant group/other access: {path}")
    return {"st_dev": item.st_dev, "st_ino": item.st_ino, "mode": stat.S_IMODE(item.st_mode)}


def assert_worker_snapshot_isolated(snapshot: Path, *, partial_dir: Path, live_source: Path) -> dict:
    secure_private_dir(partial_dir)
    identity = secure_regular_file_identity(snapshot)
    live = live_source.stat()
    if (identity["st_dev"], identity["st_ino"]) == (live.st_dev, live.st_ino):
        raise RuntimeError("worker snapshot aliases the live source database")
    return identity


def copy_file_exclusive(source: Path, destination: Path) -> None:
    secure_regular_file_identity(source)
    source_fd = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    destination_created = False
    try:
        try:
            destination_fd = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            destination_created = True
            try:
                with os.fdopen(os.dup(source_fd), "rb", closefd=True) as source_handle:
                    with os.fdopen(os.dup(destination_fd), "wb", closefd=True) as destination_handle:
                        shutil.copyfileobj(source_handle, destination_handle, length=8 * 1024 * 1024)
                        destination_handle.flush()
                        os.fsync(destination_handle.fileno())
            finally:
                os.close(destination_fd)
        except Exception:
            if destination_created:
                destination.unlink(missing_ok=True)
            raise
    finally:
        os.close(source_fd)
    secure_regular_file_identity(destination)


def vacuum_rebuild_snapshot(
    source_snapshot: Path,
    *,
    local_verify_dir: Path,
    required_tables: tuple[str, ...],
    require_distinct_filesystem: bool = True,
) -> dict:
    """Rebuild only a worker-owned snapshot; the live source is never opened here."""
    local_verify_dir.mkdir(parents=True, exist_ok=True)
    source_file_identity = secure_regular_file_identity(source_snapshot)
    source_before = source_snapshot.lstat()
    local_dir_stat = local_verify_dir.stat()
    distinct_filesystem = source_before.st_dev != local_dir_stat.st_dev
    if require_distinct_filesystem and not distinct_filesystem:
        raise LocalVerificationUnavailable(
            "vacuum rebuild directory must be on a different filesystem from the persistent snapshot"
        )
    required_bytes = source_before.st_size + LOCAL_VERIFY_FREE_SPACE_MARGIN_BYTES
    free_bytes = shutil.disk_usage(local_verify_dir).free
    if free_bytes < required_bytes:
        raise LocalVerificationUnavailable(
            f"vacuum rebuild directory has insufficient space: free={free_bytes} required={required_bytes}"
        )

    preexisting_sidecars = [str(path) for path in sqlite_sidecar_paths(source_snapshot) if path.exists()]
    if preexisting_sidecars:
        raise RuntimeError(f"worker-owned source snapshot has unexpected SQLite sidecars: {preexisting_sidecars}")

    source_quick_check = snapshot_quick_check_result(source_snapshot)
    source_counts = snapshot_table_counts(source_snapshot, required_tables)
    missing = [name for name, count in source_counts.items() if count is None]
    if missing:
        raise RuntimeError(f"required tables missing before vacuum rebuild: {missing}")
    source_sha256 = sha256_file(source_snapshot)
    source_identity_before = {
        "size_bytes": source_before.st_size,
        "mtime_ns": source_before.st_mtime_ns,
        "st_dev": source_before.st_dev,
        "st_ino": source_before.st_ino,
        "sha256": source_sha256,
    }
    local_work_dir = Path(
        tempfile.mkdtemp(prefix=f".{source_snapshot.parent.name}-vacuum-", dir=local_verify_dir)
    )
    os.chmod(local_work_dir, 0o700)
    secure_private_dir(local_work_dir)
    local_rebuilt = local_work_dir / "rebuilt.sqlite"
    persistent_rebuilt = source_snapshot.with_name(f".{source_snapshot.name}.rebuilt")
    persistent_created = False
    try:
        connection = sqlite3.connect(source_snapshot, timeout=60)
        try:
            connection.execute("PRAGMA busy_timeout=60000")
            connection.execute("PRAGMA mmap_size=0")
            connection.execute("PRAGMA cache_size=-32768")
            connection.execute("PRAGMA temp_store=FILE")
            quoted = str(local_rebuilt).replace("'", "''")
            connection.execute(f"VACUUM INTO '{quoted}'")
        finally:
            connection.close()
        generated_sidecars = {
            str(path): path.stat().st_size
            for path in sqlite_sidecar_paths(source_snapshot)
            if path.exists()
        }
        for sidecar in sqlite_sidecar_paths(source_snapshot):
            sidecar.unlink(missing_ok=True)
        fsync_file(local_rebuilt)

        local_quick_check = snapshot_quick_check(local_rebuilt)
        local_counts = snapshot_table_counts(local_rebuilt, required_tables)
        if local_counts != source_counts:
            raise RuntimeError(
                f"vacuum rebuild required table count mismatch: source={source_counts} rebuilt={local_counts}"
            )
        local_sha256 = sha256_file(local_rebuilt)

        secure_regular_file_identity(local_rebuilt)
        copy_file_exclusive(local_rebuilt, persistent_rebuilt)
        persistent_created = True
        persistent_sha256 = sha256_file(persistent_rebuilt)
        if persistent_sha256 != local_sha256:
            raise RuntimeError("persistent rebuilt snapshot SHA-256 mismatch")
        persistent_quick_check = snapshot_quick_check(persistent_rebuilt)
        persistent_counts = snapshot_table_counts(persistent_rebuilt, required_tables)
        if persistent_counts != source_counts:
            raise RuntimeError("persistent rebuilt snapshot required table count mismatch")

        source_after = source_snapshot.stat()
        if (
            source_after.st_dev != source_before.st_dev
            or source_after.st_ino != source_before.st_ino
            or source_after.st_dev != source_file_identity["st_dev"]
            or source_after.st_ino != source_file_identity["st_ino"]
            or source_after.st_size != source_before.st_size
            or source_after.st_mtime_ns != source_before.st_mtime_ns
            or sha256_file(source_snapshot) != source_sha256
        ):
            raise RuntimeError("worker-owned source snapshot changed during vacuum rebuild")

        os.replace(persistent_rebuilt, source_snapshot)
        fsync_dir(source_snapshot.parent)
        return {
            "repair_mode": REPAIR_MODE_VACUUM_REBUILD,
            "repair_method": "vacuum_into_distinct_filesystem_then_verified_persistent_replace",
            "source_snapshot": source_identity_before,
            "source_quick_check": source_quick_check[:20],
            "source_quick_check_error_count": 0 if source_quick_check == ["ok"] else len(source_quick_check),
            "source_required_table_counts": source_counts,
            "worker_generated_source_sidecars_removed": generated_sidecars,
            "local_verify_distinct_filesystem": distinct_filesystem,
            "local_rebuilt_quick_check": local_quick_check,
            "persistent_rebuilt_quick_check": persistent_quick_check,
            "rebuilt_sha256": persistent_sha256,
            "rebuilt_size_bytes": source_snapshot.stat().st_size,
            "rebuilt_required_table_counts": persistent_counts,
        }
    finally:
        if persistent_created:
            persistent_rebuilt.unlink(missing_ok=True)
        shutil.rmtree(local_work_dir, ignore_errors=True)


def validate_repair_evidence(
    snapshot: dict,
    path: Path,
    *,
    required_tables: tuple[str, ...],
    verify_file: bool,
) -> None:
    repair = snapshot.get("repair")
    if not isinstance(repair, dict) or repair.get("repair_mode") != REPAIR_MODE_VACUUM_REBUILD:
        raise RuntimeError("vacuum rebuild evidence is missing")
    if repair.get("local_rebuilt_quick_check") != ["ok"]:
        raise RuntimeError("local rebuilt quick_check evidence is not healthy")
    if repair.get("persistent_rebuilt_quick_check") != ["ok"]:
        raise RuntimeError("persistent rebuilt quick_check evidence is not healthy")
    expected_counts = repair.get("source_required_table_counts")
    rebuilt_counts = repair.get("rebuilt_required_table_counts")
    if not isinstance(expected_counts, dict) or rebuilt_counts != expected_counts:
        raise RuntimeError("vacuum rebuild required-table evidence mismatch")
    if set(expected_counts) != set(required_tables):
        raise RuntimeError("vacuum rebuild required-table evidence has the wrong scope")
    expected_sha256 = str(repair.get("rebuilt_sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise RuntimeError("vacuum rebuild SHA-256 evidence is invalid")
    if snapshot.get("sha256") is not None and snapshot.get("sha256") != expected_sha256:
        raise RuntimeError("final snapshot SHA-256 differs from rebuild evidence")
    if verify_file:
        secure_regular_file_identity(path)
        if sha256_file(path) != expected_sha256:
            raise RuntimeError("vacuum rebuild checkpoint file SHA-256 mismatch")
        if snapshot_quick_check(path) != ["ok"]:
            raise RuntimeError("vacuum rebuild checkpoint file failed quick_check")
        if snapshot_table_counts(path, required_tables) != expected_counts:
            raise RuntimeError("vacuum rebuild checkpoint file required-table counts mismatch")


def validate_completed_manifest(
    manifest_path: Path,
    snapshot_path: Path,
    *,
    request: dict,
    repair_mode: str,
    required_tables: tuple[str, ...],
) -> dict:
    manifest = read_json_object(manifest_path)
    if manifest.get("request") != request:
        raise RuntimeError("existing snapshot manifest does not match the current request")
    snapshot = manifest.get("snapshot")
    if not isinstance(snapshot, dict):
        raise RuntimeError("existing snapshot manifest is missing snapshot evidence")
    if str(snapshot.get("repair_mode") or REPAIR_MODE_NONE) != repair_mode:
        raise RuntimeError("existing snapshot repair_mode does not match the current request")
    if tuple(snapshot.get("required_tables") or CRITICAL_TABLES) != required_tables:
        raise RuntimeError("existing snapshot required_tables do not match the current request")
    expected_sha256 = str(snapshot.get("sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise RuntimeError("existing snapshot manifest SHA-256 is invalid")
    secure_regular_file_identity(snapshot_path)
    if sha256_file(snapshot_path) != expected_sha256:
        raise RuntimeError("existing snapshot does not match its manifest SHA-256")
    if repair_mode == REPAIR_MODE_VACUUM_REBUILD:
        validate_repair_evidence(
            snapshot,
            snapshot_path,
            required_tables=required_tables,
            verify_file=True,
        )
    return manifest


def validate_pre_promotion_snapshot(
    snapshot: dict,
    path: Path,
    *,
    repair_mode: str,
    required_tables: tuple[str, ...],
) -> None:
    if snapshot.get("quick_check") != ["ok"]:
        raise RuntimeError("snapshot quick_check evidence is missing before promotion")
    expected_sha256 = str(snapshot.get("sha256") or "")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise RuntimeError("snapshot SHA-256 evidence is invalid before promotion")
    expected_counts = snapshot.get("critical_table_counts")
    if not isinstance(expected_counts, dict) or set(expected_counts) != set(required_tables):
        raise RuntimeError("snapshot required-table evidence is incomplete before promotion")
    secure_regular_file_identity(path)
    if snapshot_quick_check(path) != ["ok"]:
        raise RuntimeError("snapshot failed final quick_check before promotion")
    if sha256_file(path) != expected_sha256:
        raise RuntimeError("snapshot failed final SHA-256 verification before promotion")
    if snapshot_table_counts(path, required_tables) != expected_counts:
        raise RuntimeError("snapshot failed final required-table verification before promotion")
    if repair_mode == REPAIR_MODE_VACUUM_REBUILD:
        validate_repair_evidence(
            snapshot,
            path,
            required_tables=required_tables,
            verify_file=True,
        )


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


def checkpoint_payload(
    *,
    request_id: str,
    request_sha256: str,
    attempt_count: int,
    stage: str,
    snapshot: dict,
) -> dict:
    return {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "request_id": request_id,
        "request_sha256": request_sha256,
        "attempt_count": attempt_count,
        "stage": stage,
        "snapshot": snapshot,
    }


def read_checkpoint(path: Path, *, request_id: str, request_sha256: str) -> dict | None:
    if not path.is_file():
        return None
    checkpoint = read_json_object(path)
    if checkpoint.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
        raise ValueError(f"unsupported checkpoint schema: {checkpoint.get('schema_version')}")
    if checkpoint.get("request_id") != request_id:
        raise ValueError("checkpoint request_id mismatch")
    if checkpoint.get("request_sha256") != request_sha256:
        raise ValueError("checkpoint request fingerprint mismatch")
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
    request_sha256 = request_fingerprint(request)
    repair_mode = request_repair_mode(request)
    required_tables = request_required_tables(request)
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
        validate_completed_manifest(
            manifest_path,
            snapshot_path,
            request=request,
            repair_mode=repair_mode,
            required_tables=required_tables,
        )
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
    checkpoint = (
        read_checkpoint(checkpoint_path, request_id=request_id, request_sha256=request_sha256)
        if partial_dir.exists()
        else None
    )
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
    if checkpoint is None and integrity_marker.exists() and repair_mode == REPAIR_MODE_NONE:
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
            repair_mode=repair_mode,
            required_tables=list(required_tables),
        )
        atomic_write_json(status_path, running)
        partial_dir.mkdir(mode=0o700, parents=True, exist_ok=False)
        os.chmod(partial_dir, 0o700)
        try:
            snapshot = create_consistent_sqlite_snapshot(source_path, partial_snapshot, verify=False)
            os.chmod(partial_snapshot, 0o600)
            snapshot["repair_mode"] = repair_mode
            snapshot["required_tables"] = list(required_tables)
            checkpoint = checkpoint_payload(
                request_id=request_id,
                request_sha256=request_sha256,
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
                repair_mode=repair_mode,
                required_tables=list(required_tables),
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
    checkpoint_repair_mode = str(snapshot.get("repair_mode") or REPAIR_MODE_NONE)
    if checkpoint_repair_mode != repair_mode:
        raise ValueError(
            f"checkpoint repair_mode mismatch: checkpoint={checkpoint_repair_mode} request={repair_mode}"
        )
    checkpoint_required_tables = tuple(snapshot.get("required_tables") or CRITICAL_TABLES)
    if checkpoint_required_tables != required_tables:
        raise ValueError(
            "checkpoint required_tables mismatch: "
            f"checkpoint={checkpoint_required_tables} request={required_tables}"
        )
    if not partial_snapshot.is_file():
        raise RuntimeError(f"checkpoint snapshot missing: {partial_snapshot}")

    running_fields = {
        "attempt_count": attempt_count,
        "source_path": str(source_path),
        "partial_dir": str(partial_dir),
        "snapshot_path": str(snapshot_path),
        "manifest_path": str(manifest_path),
        "reason": request.get("reason"),
        "repair_mode": repair_mode,
        "required_tables": list(required_tables),
    }
    try:
        if stage == "infrastructure_exhausted":
            exhausted = status_payload(
                request_id=request_id,
                state="attempts_exhausted",
                stage=stage,
                infrastructure_retry_count=int(snapshot.get("infrastructure_retry_count") or 0),
                partial_preserved=True,
                **running_fields,
            )
            atomic_write_json(status_path, exhausted)
            return exhausted

        if repair_mode == REPAIR_MODE_VACUUM_REBUILD and stage != "snapshot_copied":
            validate_repair_evidence(
                snapshot,
                partial_snapshot,
                required_tables=required_tables,
                verify_file=True,
            )

        if stage == "snapshot_copied" and repair_mode == REPAIR_MODE_VACUUM_REBUILD:
            if local_verify_dir is None:
                raise LocalVerificationUnavailable("vacuum_rebuild requires --local-verify-dir")
            assert_worker_snapshot_isolated(
                partial_snapshot,
                partial_dir=partial_dir,
                live_source=source_path,
            )
            running = status_payload(
                request_id=request_id,
                state="running_vacuum_rebuild",
                stage="vacuum_rebuild",
                **running_fields,
            )
            atomic_write_json(status_path, running)
            repair_evidence = vacuum_rebuild_snapshot(
                partial_snapshot,
                local_verify_dir=local_verify_dir,
                required_tables=required_tables,
            )
            snapshot["repair"] = repair_evidence
            snapshot["method"] = "sqlite_online_backup_then_vacuum_rebuild"
            snapshot["snapshot_size_bytes"] = partial_snapshot.stat().st_size
            checkpoint = checkpoint_payload(
                request_id=request_id,
                request_sha256=request_sha256,
                attempt_count=attempt_count,
                stage="rebuild_complete",
                snapshot=snapshot,
            )
            atomic_write_json(checkpoint_path, checkpoint)
            completed_stage = status_payload(
                request_id=request_id,
                state="running_vacuum_rebuild_complete",
                stage="rebuild_complete",
                **running_fields,
            )
            atomic_write_json(status_path, completed_stage)
            return completed_stage

        if stage in {"snapshot_copied", "rebuild_complete"}:
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
                request_sha256=request_sha256,
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
                request_sha256=request_sha256,
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
            snapshot["critical_table_counts"] = snapshot_table_counts(partial_snapshot, required_tables)
            missing = [name for name, count in snapshot["critical_table_counts"].items() if count is None]
            if missing:
                raise RuntimeError(f"critical tables missing from snapshot: {missing}")
            checkpoint = checkpoint_payload(
                request_id=request_id,
                request_sha256=request_sha256,
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

        validate_pre_promotion_snapshot(
            snapshot,
            partial_snapshot,
            repair_mode=repair_mode,
            required_tables=required_tables,
        )

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
        fsync_file(partial_snapshot)
        fsync_dir(partial_dir)
        os.replace(partial_dir, final_dir)
        fsync_dir(recovery_dir)
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
        infrastructure_retry_count = int(snapshot.get("infrastructure_retry_count") or 0) + 1
        snapshot["infrastructure_retry_count"] = infrastructure_retry_count
        exhausted = infrastructure_retry_count >= max(1, int(max_attempts))
        checkpoint_stage = "infrastructure_exhausted" if exhausted else stage
        checkpoint = checkpoint_payload(
            request_id=request_id,
            request_sha256=request_sha256,
            attempt_count=attempt_count,
            stage=checkpoint_stage,
            snapshot=snapshot,
        )
        atomic_write_json(checkpoint_path, checkpoint)
        blocked = status_payload(
            request_id=request_id,
            state="attempts_exhausted" if exhausted else "blocked_local_verification",
            stage=checkpoint_stage,
            attempt_count=attempt_count,
            infrastructure_retry_count=infrastructure_retry_count,
            partial_dir=str(partial_dir),
            partial_preserved=True,
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
