#!/usr/bin/env python3
"""Lightweight Zeabur volume preflight for paper-only runtime.

The goal is to recover observability when the persistent volume is under
pressure. Keep this script dependency-free so it can run before Node/Python
sidecars start.
"""

from __future__ import annotations

import fcntl
import gzip
import hashlib
import json
import os
import re
import shutil
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote


DATA_DIR = Path(os.environ.get("ZEABUR_DATA_DIR", "/app/data"))
MAX_LOG_BYTES = int(float(os.environ.get("ZEABUR_LOG_TRIM_MAX_MB", "256")) * 1024 * 1024)
KEEP_LOG_BYTES = int(float(os.environ.get("ZEABUR_LOG_TRIM_KEEP_MB", "64")) * 1024 * 1024)
JSONL_TRIM_ENABLED = os.environ.get("ZEABUR_JSONL_TRIM_ENABLED", "true").lower() != "false"
GMGN_JSONL_MAX_BYTES = int(float(os.environ.get("ZEABUR_GMGN_JSONL_TRIM_MAX_MB", "256")) * 1024 * 1024)
GMGN_JSONL_KEEP_BYTES = int(float(os.environ.get("ZEABUR_GMGN_JSONL_TRIM_KEEP_MB", "64")) * 1024 * 1024)
PAPER_EVIDENCE_JSONL_MAX_BYTES = int(float(os.environ.get("ZEABUR_PAPER_EVIDENCE_JSONL_TRIM_MAX_MB", "256")) * 1024 * 1024)
PAPER_EVIDENCE_JSONL_KEEP_BYTES = int(float(os.environ.get("ZEABUR_PAPER_EVIDENCE_JSONL_TRIM_KEEP_MB", "128")) * 1024 * 1024)
PAPER_EVIDENCE_JSONL_ARCHIVE_ENABLED = os.environ.get(
    "ZEABUR_PAPER_EVIDENCE_JSONL_ARCHIVE_ENABLED", "true"
).lower() != "false"
PAPER_EVIDENCE_JSONL_HOT_DAYS = max(
    1, int(float(os.environ.get("ZEABUR_PAPER_EVIDENCE_JSONL_HOT_DAYS", "7")))
)
PAPER_EVIDENCE_JSONL_ARCHIVE_MAX_FILES = max(
    0,
    int(
        float(
            os.environ.get(
                "ZEABUR_PAPER_EVIDENCE_JSONL_ARCHIVE_MAX_FILES", "4"
            )
        )
    ),
)
PAPER_EVIDENCE_JSONL_COMPRESSION_LEVEL = min(
    9,
    max(
        1,
        int(
            float(
                os.environ.get(
                    "ZEABUR_PAPER_EVIDENCE_JSONL_COMPRESSION_LEVEL", "1"
                )
            )
        ),
    ),
)
PAPER_EVIDENCE_JSONL_ARCHIVE_TIMEOUT_SEC = max(
    1.0,
    float(os.environ.get("ZEABUR_PAPER_EVIDENCE_JSONL_ARCHIVE_TIMEOUT_SEC", "40")),
)
V27_EVENT_JSONL_MAX_BYTES = int(float(os.environ.get("ZEABUR_V27_EVENT_JSONL_TRIM_MAX_MB", "512")) * 1024 * 1024)
V27_EVENT_JSONL_KEEP_BYTES = int(float(os.environ.get("ZEABUR_V27_EVENT_JSONL_TRIM_KEEP_MB", "128")) * 1024 * 1024)
DELETE_LARGE_TMP = os.environ.get("ZEABUR_DELETE_LARGE_TMP", "false").lower() == "true"
TMP_DELETE_BYTES = int(float(os.environ.get("ZEABUR_TMP_DELETE_MIN_MB", "256")) * 1024 * 1024)
DISK_WARN_FREE_BYTES = int(float(os.environ.get("ZEABUR_DISK_WARN_FREE_MB", "256")) * 1024 * 1024)
QUARANTINE_MALFORMED_PAPER_DB = os.environ.get("ZEABUR_QUARANTINE_MALFORMED_PAPER_DB", "true").lower() != "false"
RECOVERY_DIR = Path(os.environ.get("ZEABUR_RECOVERY_DIR", str(DATA_DIR / "recovery")))
QUICK_CHECK_MAX_BYTES = int(float(os.environ.get("ZEABUR_PREFLIGHT_QUICK_CHECK_MAX_MB", "64")) * 1024 * 1024)
DB_CHECK_ENABLED = os.environ.get("ZEABUR_PREFLIGHT_DB_CHECK_ENABLED", "true").lower() != "false"
# A full paper DB snapshot is too large for the bounded startup preflight. Run
# it explicitly outside the startup timeout instead of copying on every boot.
PAPER_DB_BACKUP_ENABLED = os.environ.get("ZEABUR_PREFLIGHT_PAPER_DB_BACKUP_ENABLED", "false").lower() == "true"
PAPER_DB_BACKUP_DIR = Path(os.environ.get("ZEABUR_PAPER_DB_BACKUP_DIR", str(DATA_DIR / "backup" / "paper-db-family")))
PAPER_DB_BACKUP_KEEP = int(os.environ.get("ZEABUR_PAPER_DB_BACKUP_KEEP", "12"))
PAPER_DB_BACKUP_MIN_INTERVAL_SEC = int(os.environ.get("ZEABUR_PAPER_DB_BACKUP_MIN_INTERVAL_SEC", "3600"))
PAPER_DB_BACKUP_PARTIAL_MAX_AGE_SEC = int(os.environ.get("ZEABUR_PAPER_DB_BACKUP_PARTIAL_MAX_AGE_SEC", "86400"))
PAPER_EVIDENCE_DAILY_RE = re.compile(r"^paper-events-(\d{8})\.jsonl$")
ARCHIVE_COPY_CHUNK_BYTES = 1024 * 1024

LOG_NAMES = [
    "dashboard.log",
    "node.log",
    "maintenance.log",
    "runtime.log",
    "paper-trader.log",
    "paper-fast-lane.log",
    "paper-db-snapshot-worker.log",
    "paper-db-retention.log",
    "paper-review-snapshot.log",
    "source-resonance.log",
    "gmgn-scout.log",
    "lifecycle.log",
    "social-service.log",
    "raw-path-observer.log",
    "raw-dog-discovery-observer.log",
    "candidate-shadow-observer.log",
    "agent-capture-discovery.log",
    "pump-fun-shadow-worker.log",
]

DB_NAMES = [
    "paper_trades.db",
    "sentiment_arb.db",
    "kline_cache.db",
    "lifecycle_tracks.db",
    "raw_signal_outcomes.db",
    "pump_fun_shadow_signals.db",
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
        tmp = path.with_suffix(path.suffix + f".trim.{os.getpid()}")
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


def trim_jsonl_tail(path: Path, *, max_bytes: int, keep_bytes: int) -> None:
    try:
        if not JSONL_TRIM_ENABLED or not path.exists() or not path.is_file():
            return
        size = path.stat().st_size
        if size <= max_bytes:
            return
        tmp = path.with_suffix(path.suffix + f".trim.{os.getpid()}")
        try:
            with path.open("rb") as src:
                src.seek(max(0, size - keep_bytes))
                data = src.read()
            first_newline = data.find(b"\n")
            if first_newline >= 0:
                data = data[first_newline + 1 :]
            if data and not data.endswith(b"\n"):
                data += b"\n"
            with tmp.open("wb") as dst:
                dst.write(data)
            os.replace(tmp, path)
            log(f"trimmed jsonl {path} {size // (1024 * 1024)}MB -> {path.stat().st_size // (1024 * 1024)}MB")
        except Exception as exc:
            log(f"WARN jsonl trim-copy failed for {path}: {exc}; leaving original intact")
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception as exc:
        log(f"WARN jsonl trim failed for {path}: {exc}")


def stream_sha256(stream, *, deadline: float | None = None) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    while True:
        if archive_deadline_expired(deadline):
            raise TimeoutError("paper evidence archive work deadline exceeded")
        chunk = stream.read(ARCHIVE_COPY_CHUNK_BYTES)
        if not chunk:
            break
        digest.update(chunk)
        size += len(chunk)
    return size, digest.hexdigest()


def gzip_matches_source(
    source: Path,
    archive: Path,
    *,
    deadline: float | None = None,
) -> bool:
    try:
        with source.open("rb") as source_fh:
            source_identity = stream_sha256(source_fh, deadline=deadline)
        with gzip.open(archive, "rb") as archive_fh:
            archive_identity = stream_sha256(archive_fh, deadline=deadline)
        return source_identity == archive_identity
    except TimeoutError:
        raise
    except Exception:
        return False


def gzip_matches_archive_plus_source(
    existing_archive: Path,
    source: Path,
    candidate_archive: Path,
    *,
    deadline: float | None = None,
) -> bool:
    try:
        digest = hashlib.sha256()
        expected_size = 0
        for path, opener in ((existing_archive, gzip.open), (source, open)):
            with opener(path, "rb") as stream:
                while True:
                    if archive_deadline_expired(deadline):
                        raise TimeoutError("paper evidence archive work deadline exceeded")
                    chunk = stream.read(ARCHIVE_COPY_CHUNK_BYTES)
                    if not chunk:
                        break
                    digest.update(chunk)
                    expected_size += len(chunk)
        with gzip.open(candidate_archive, "rb") as candidate_fh:
            candidate_size, candidate_hash = stream_sha256(
                candidate_fh,
                deadline=deadline,
            )
        return (expected_size, digest.hexdigest()) == (candidate_size, candidate_hash)
    except TimeoutError:
        raise
    except Exception:
        return False


def gzip_ends_with_source(
    source: Path,
    archive: Path,
    *,
    deadline: float | None = None,
) -> bool:
    try:
        with source.open("rb") as source_fh:
            source_size, source_hash = stream_sha256(source_fh, deadline=deadline)
        with gzip.open(archive, "rb") as archive_fh:
            archive_size, _archive_hash = stream_sha256(archive_fh, deadline=deadline)
        if archive_size < source_size:
            return False
        remaining = archive_size - source_size
        digest = hashlib.sha256()
        with gzip.open(archive, "rb") as archive_fh:
            while remaining > 0:
                if archive_deadline_expired(deadline):
                    raise TimeoutError("paper evidence archive work deadline exceeded")
                discarded = archive_fh.read(min(ARCHIVE_COPY_CHUNK_BYTES, remaining))
                if not discarded:
                    return False
                remaining -= len(discarded)
            tail_size = 0
            while True:
                if archive_deadline_expired(deadline):
                    raise TimeoutError("paper evidence archive work deadline exceeded")
                chunk = archive_fh.read(ARCHIVE_COPY_CHUNK_BYTES)
                if not chunk:
                    break
                digest.update(chunk)
                tail_size += len(chunk)
        return (tail_size, digest.hexdigest()) == (source_size, source_hash)
    except TimeoutError:
        raise
    except Exception:
        return False


def copy_stream_before_deadline(source_fh, destination_fh, *, deadline: float | None) -> None:
    while True:
        if archive_deadline_expired(deadline):
            raise TimeoutError("paper evidence archive work deadline exceeded")
        chunk = source_fh.read(ARCHIVE_COPY_CHUNK_BYTES)
        if not chunk:
            return
        destination_fh.write(chunk)


def write_gzip_archive(
    source: Path,
    temporary: Path,
    *,
    existing_archive: Path | None = None,
    deadline: float | None = None,
) -> None:
    with temporary.open("wb") as raw_fh:
        with gzip.GzipFile(
            filename=source.name,
            mode="wb",
            fileobj=raw_fh,
            compresslevel=PAPER_EVIDENCE_JSONL_COMPRESSION_LEVEL,
            mtime=0,
        ) as archive_fh:
            if existing_archive is not None:
                with gzip.open(existing_archive, "rb") as existing_fh:
                    copy_stream_before_deadline(
                        existing_fh,
                        archive_fh,
                        deadline=deadline,
                    )
            with source.open("rb") as source_fh:
                copy_stream_before_deadline(
                    source_fh,
                    archive_fh,
                    deadline=deadline,
                )
        raw_fh.flush()
        os.fsync(raw_fh.fileno())


def fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(path, flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def paper_evidence_jsonl_day(path: Path):
    match = PAPER_EVIDENCE_DAILY_RE.fullmatch(path.name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def oldest_hot_paper_evidence_day(*, now_ts: float | None = None):
    now = float(time.time() if now_ts is None else now_ts)
    return datetime.fromtimestamp(now, timezone.utc).date() - timedelta(
        days=PAPER_EVIDENCE_JSONL_HOT_DAYS - 1
    )


def archive_deadline_expired(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def acquire_lock_before_deadline(lock_fh, *, deadline: float | None) -> None:
    while True:
        try:
            fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if archive_deadline_expired(deadline):
                raise TimeoutError("paper evidence archive lock deadline exceeded")
            time.sleep(0.05)


def archive_paper_evidence_jsonl_file(path: Path, *, deadline: float | None = None) -> dict:
    archive = Path(f"{path}.gz")
    temporary = path.with_name(f"{path.name}.gz.tmp.{os.getpid()}")
    try:
        source_bytes = path.stat().st_size
        previous_archive_bytes = archive.stat().st_size if archive.exists() else 0
        if archive_deadline_expired(deadline):
            raise TimeoutError("paper evidence archive work deadline exceeded")
        if archive.exists():
            if not gzip_matches_source(path, archive, deadline=deadline) and not gzip_ends_with_source(
                path,
                archive,
                deadline=deadline,
            ):
                write_gzip_archive(
                    path,
                    temporary,
                    existing_archive=archive,
                    deadline=deadline,
                )
                if not gzip_matches_archive_plus_source(
                    archive,
                    path,
                    temporary,
                    deadline=deadline,
                ):
                    raise RuntimeError("merged gzip archive verification failed")
                if archive_deadline_expired(deadline):
                    raise TimeoutError("paper evidence archive work deadline exceeded")
                os.replace(temporary, archive)
                fsync_directory(path.parent)
        else:
            write_gzip_archive(path, temporary, deadline=deadline)
            if not gzip_matches_source(path, temporary, deadline=deadline):
                raise RuntimeError("gzip archive verification failed")
            if archive_deadline_expired(deadline):
                raise TimeoutError("paper evidence archive work deadline exceeded")
            os.replace(temporary, archive)
            fsync_directory(path.parent)
        archive_bytes = archive.stat().st_size
        archive_growth_bytes = max(0, archive_bytes - previous_archive_bytes)
        path.unlink()
        try:
            fsync_directory(path.parent)
        except Exception as exc:
            log(f"WARN jsonl source removal directory sync failed for {path}: {exc}")
        result = {
            "source": str(path),
            "archive": str(archive),
            "source_bytes": source_bytes,
            "archive_bytes": archive_bytes,
            "archive_growth_bytes": archive_growth_bytes,
            "reclaimed_bytes": max(0, source_bytes - archive_growth_bytes),
        }
        log(
            f"archived jsonl {path} {source_bytes // (1024 * 1024)}MB -> "
            f"{archive_bytes // (1024 * 1024)}MB"
        )
        return result
    except Exception as exc:
        try:
            temporary.unlink(missing_ok=True)
        except Exception:
            pass
        log(f"WARN jsonl archive failed for {path}: {exc}; leaving source intact")
        return {
            "source": str(path),
            "archive": str(archive),
            "error": f"{type(exc).__name__}:{exc}",
        }


def archive_paper_evidence_jsonl_files(
    *,
    now_ts: float | None = None,
    deadline: float | None = None,
) -> dict:
    summary = {
        "enabled": PAPER_EVIDENCE_JSONL_ARCHIVE_ENABLED,
        "hot_days": PAPER_EVIDENCE_JSONL_HOT_DAYS,
        "max_files": PAPER_EVIDENCE_JSONL_ARCHIVE_MAX_FILES,
        "archived": [],
        "errors": [],
        "reclaimed_bytes": 0,
    }
    if (
        not PAPER_EVIDENCE_JSONL_ARCHIVE_ENABLED
        or PAPER_EVIDENCE_JSONL_ARCHIVE_MAX_FILES <= 0
    ):
        return summary
    evidence_dir = DATA_DIR / "paper_evidence_log"
    if not evidence_dir.exists():
        return summary
    oldest_hot_day = oldest_hot_paper_evidence_day(now_ts=now_ts)
    candidates = []
    for path in evidence_dir.glob("paper-events-*.jsonl"):
        day = paper_evidence_jsonl_day(path)
        if day is None:
            continue
        if day < oldest_hot_day:
            candidates.append((day, path))
    candidates.sort(key=lambda item: (item[0], item[1].name))
    lock_path = evidence_dir / ".append.lock"
    if deadline is None:
        deadline = time.monotonic() + PAPER_EVIDENCE_JSONL_ARCHIVE_TIMEOUT_SEC
    try:
        with lock_path.open("a+", encoding="utf-8") as lock_fh:
            acquire_lock_before_deadline(lock_fh, deadline=deadline)
            try:
                for stale in evidence_dir.glob("paper-events-*.jsonl.gz.tmp.*"):
                    try:
                        stale.unlink(missing_ok=True)
                    except Exception as exc:
                        log(f"WARN stale jsonl archive cleanup failed for {stale}: {exc}")
                for _day, path in candidates[:PAPER_EVIDENCE_JSONL_ARCHIVE_MAX_FILES]:
                    if archive_deadline_expired(deadline):
                        result = {
                            "source": str(path),
                            "error": "TimeoutError:paper evidence archive work deadline exceeded",
                        }
                        summary["errors"].append(result)
                        break
                    result = archive_paper_evidence_jsonl_file(path, deadline=deadline)
                    if result.get("error"):
                        summary["errors"].append(result)
                    else:
                        summary["archived"].append(result)
                        summary["reclaimed_bytes"] += int(
                            result.get("reclaimed_bytes") or 0
                        )
            finally:
                fcntl.flock(lock_fh, fcntl.LOCK_UN)
    except Exception as exc:
        result = {
            "source": str(evidence_dir),
            "error": f"{type(exc).__name__}:{exc}",
        }
        summary["errors"].append(result)
        log(f"WARN jsonl archive pass failed for {evidence_dir}: {exc}")
    return summary


def trim_runtime_jsonl_files() -> None:
    trim_jsonl_tail(
        DATA_DIR / "gmgn_candidates.jsonl",
        max_bytes=GMGN_JSONL_MAX_BYTES,
        keep_bytes=GMGN_JSONL_KEEP_BYTES,
    )
    trim_jsonl_tail(
        DATA_DIR / "v27_event_log" / "events.jsonl",
        max_bytes=V27_EVENT_JSONL_MAX_BYTES,
        keep_bytes=V27_EVENT_JSONL_KEEP_BYTES,
    )
    evidence_dir = DATA_DIR / "paper_evidence_log"
    if evidence_dir.exists():
        deadline = time.monotonic() + PAPER_EVIDENCE_JSONL_ARCHIVE_TIMEOUT_SEC
        archive_paper_evidence_jsonl_files(deadline=deadline)
        oldest_hot_day = oldest_hot_paper_evidence_day()
        lock_path = evidence_dir / ".append.lock"
        try:
            with lock_path.open("a+", encoding="utf-8") as lock_fh:
                acquire_lock_before_deadline(lock_fh, deadline=deadline)
                try:
                    for path in evidence_dir.glob("*.jsonl"):
                        day = paper_evidence_jsonl_day(path)
                        if (
                            PAPER_EVIDENCE_JSONL_ARCHIVE_ENABLED
                            and PAPER_EVIDENCE_JSONL_ARCHIVE_MAX_FILES > 0
                            and day is not None
                            and day < oldest_hot_day
                        ):
                            # Preserve cold shards for a later bounded archive pass instead
                            # of tail-trimming evidence that should remain replayable.
                            continue
                        trim_jsonl_tail(
                            path,
                            max_bytes=PAPER_EVIDENCE_JSONL_MAX_BYTES,
                            keep_bytes=PAPER_EVIDENCE_JSONL_KEEP_BYTES,
                        )
                finally:
                    fcntl.flock(lock_fh, fcntl.LOCK_UN)
        except TimeoutError as exc:
            log(f"WARN paper evidence trim skipped: {exc}")


def remove_large_temp_files() -> None:
    if not DELETE_LARGE_TMP:
        log("large temp deletion disabled (set ZEABUR_DELETE_LARGE_TMP=true to enable)")
        return
    if not DATA_DIR.exists():
        return
    for pattern in ("*.tmp", "*.download", "*.partial", "*.trim", "*.trim.*"):
        for path in DATA_DIR.rglob(pattern):
            try:
                if path.is_file() and path.stat().st_size >= TMP_DELETE_BYTES:
                    size = path.stat().st_size
                    path.unlink()
                    log(f"removed temp {path} size={size // (1024 * 1024)}MB")
            except Exception as exc:
                log(f"WARN remove temp failed for {path}: {exc}")


def write_integrity_marker(path: Path, status: str) -> None:
    marker = path.with_suffix(path.suffix + ".integrity_error")
    try:
        marker.write_text(str(status)[:4000], encoding="utf-8")
    except Exception as exc:
        log(f"WARN write integrity marker failed {marker}: {exc}")


def should_quarantine(path: Path, reason: str) -> bool:
    if not QUARANTINE_MALFORMED_PAPER_DB:
        return False
    if path.name != "paper_trades.db":
        return False
    reason_l = str(reason or "").lower()
    return (
        "malformed" in reason_l
        or "database disk image" in reason_l
        or "file is not a database" in reason_l
        or "quick_check" in reason_l
        or "zero-byte" in reason_l
    )


def quarantine_db_family(path: Path, reason: str) -> None:
    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    dest_dir = RECOVERY_DIR / f"{path.stem}_corrupt_{ts}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    moved = []
    for suffix in ("", "-wal", "-shm", ".integrity_error"):
        src = Path(f"{path}{suffix}") if suffix.startswith("-") else path.with_suffix(path.suffix + suffix) if suffix else path
        if not src.exists():
            continue
        dest = dest_dir / src.name
        try:
            os.replace(src, dest)
            moved.append({"from": str(src), "to": str(dest), "size_bytes": dest.stat().st_size})
        except Exception as exc:
            log(f"WARN quarantine move failed {src}: {exc}")
    manifest = {
        "created_at": ts,
        "reason": str(reason)[:4000],
        "db": str(path),
        "moved": moved,
        "note": "Original malformed paper DB files were preserved here; live path was cleared so paper services can recreate a clean DB.",
    }
    try:
        (dest_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    except Exception as exc:
        log(f"WARN quarantine manifest failed {dest_dir}: {exc}")
    log(f"quarantined malformed {path.name} -> {dest_dir} files={len(moved)}")


def complete_paper_db_backups() -> list[Path]:
    if not PAPER_DB_BACKUP_DIR.exists():
        return []
    return sorted(
        path
        for path in PAPER_DB_BACKUP_DIR.glob("paper_trades_*")
        if path.is_dir() and (path / "paper_trades.db").is_file() and (path / "manifest.json").is_file()
    )


def cleanup_stale_backup_partials() -> None:
    if not PAPER_DB_BACKUP_DIR.exists():
        return
    now = time.time()
    for partial in PAPER_DB_BACKUP_DIR.glob(".paper_trades_*.partial"):
        try:
            age_sec = now - partial.stat().st_mtime
            if age_sec < max(0, PAPER_DB_BACKUP_PARTIAL_MAX_AGE_SEC):
                continue
            shutil.rmtree(partial)
            log(f"removed stale partial backup {partial} age_sec={int(age_sec)}")
        except Exception as exc:
            log(f"WARN partial backup cleanup failed {partial}: {exc}")


def create_consistent_sqlite_snapshot(source: Path, destination: Path, *, verify: bool = True) -> dict:
    source_before = source.stat()
    source_uri = f"file:{quote(str(source.resolve()), safe='/')}?mode=ro"
    source_connection = sqlite3.connect(source_uri, uri=True, timeout=30)
    destination_connection = sqlite3.connect(str(destination), timeout=30)
    try:
        source_connection.execute("PRAGMA query_only=ON")
        source_connection.execute("PRAGMA busy_timeout=30000")
        destination_connection.execute("PRAGMA busy_timeout=30000")
        source_connection.backup(destination_connection, pages=4096, sleep=0.05)
        destination_connection.commit()
    finally:
        destination_connection.close()
        source_connection.close()
    quick_check = None
    if verify:
        verify_uri = f"file:{quote(str(destination.resolve()), safe='/')}?mode=ro"
        verify_connection = sqlite3.connect(verify_uri, uri=True, timeout=30)
        try:
            verify_connection.execute("PRAGMA query_only=ON")
            verify_connection.execute("PRAGMA busy_timeout=30000")
            verify_connection.execute("PRAGMA mmap_size=0")
            verify_connection.execute("PRAGMA cache_size=-8192")
            quick_check = [str(row[0]) for row in verify_connection.execute("PRAGMA quick_check").fetchall()]
            if quick_check != ["ok"]:
                raise RuntimeError(f"snapshot quick_check failed: {quick_check[:20]}")
        finally:
            verify_connection.close()
    source_after = source.stat()
    return {
        "method": "sqlite_online_backup",
        "quick_check": quick_check,
        "source_size_before": source_before.st_size,
        "source_size_after": source_after.st_size,
        "source_mtime_before": source_before.st_mtime,
        "source_mtime_after": source_after.st_mtime,
        "source_changed_during_snapshot": (
            source_before.st_size != source_after.st_size or source_before.st_mtime != source_after.st_mtime
        ),
        "snapshot_size_bytes": destination.stat().st_size,
    }


def prune_complete_paper_db_backups() -> None:
    backups = complete_paper_db_backups()
    for old in backups[: max(0, len(backups) - max(1, PAPER_DB_BACKUP_KEEP))]:
        shutil.rmtree(old)
        log(f"backup pruned {old}")


def backup_db_family(path: Path) -> None:
    if not PAPER_DB_BACKUP_ENABLED or path.name != "paper_trades.db" or not path.exists():
        return
    try:
        PAPER_DB_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
        cleanup_stale_backup_partials()
        existing = complete_paper_db_backups()
        if existing and PAPER_DB_BACKUP_MIN_INTERVAL_SEC > 0:
            latest = existing[-1]
            try:
                age_sec = time.time() - latest.stat().st_mtime
                if age_sec < PAPER_DB_BACKUP_MIN_INTERVAL_SEC:
                    log(f"backup skipped {path.name}: latest_age_sec={int(age_sec)}")
                    return
            except Exception:
                pass
        ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        dest_dir = PAPER_DB_BACKUP_DIR / f"paper_trades_{ts}"
        partial_dir = PAPER_DB_BACKUP_DIR / f".paper_trades_{ts}.{os.getpid()}.{time.time_ns()}.partial"
        partial_dir.mkdir(parents=True, exist_ok=False)
        snapshot_path = partial_dir / path.name
        snapshot = create_consistent_sqlite_snapshot(path, snapshot_path)
        manifest = {
            "created_at": ts,
            "db": str(path),
            "snapshot": snapshot,
            "note": "Explicit consistent paper DB snapshot created outside the bounded startup preflight.",
        }
        (partial_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
        os.replace(partial_dir, dest_dir)
        log(f"backup ok {path.name} -> {dest_dir} method={snapshot['method']}")
        prune_complete_paper_db_backups()
    except Exception as exc:
        log(f"WARN backup failed {path.name}: {exc}")


def sqlite_header_invalid(path: Path) -> bool:
    try:
        if path.stat().st_size == 0:
            return False
        with path.open("rb") as fh:
            header = fh.read(16)
        return header != b"SQLite format 3\x00"
    except Exception:
        return False


def checkpoint_db(path: Path) -> None:
    if not path.exists():
        return
    marker = path.with_suffix(path.suffix + ".integrity_error")
    if path.name == "paper_trades.db" and marker.exists():
        try:
            marker_status = marker.read_text(encoding="utf-8", errors="replace")[:4000]
        except Exception as exc:
            marker_status = f"integrity marker present but unreadable: {exc}"
        if should_quarantine(path, marker_status):
            log(f"WARN existing integrity marker {path.name}: {marker_status.splitlines()[0] if marker_status else 'unknown'}")
            quarantine_db_family(path, marker_status)
            return
    if path.name == "paper_trades.db" and sqlite_header_invalid(path):
        reason = "file is not a database: invalid sqlite header"
        log(f"WARN checkpoint failed {path.name}: {reason}")
        write_integrity_marker(path, reason)
        if should_quarantine(path, reason):
            quarantine_db_family(path, reason)
        return
    if path.name == "paper_trades.db" and path.exists() and path.stat().st_size == 0:
        reason = "zero-byte paper DB: live path must be recreated from schema"
        log(f"WARN checkpoint failed {path.name}: {reason}")
        write_integrity_marker(path, reason)
        if should_quarantine(path, reason):
            quarantine_db_family(path, reason)
        return
    try:
        conn = sqlite3.connect(str(path), timeout=5)
        try:
            conn.execute("PRAGMA busy_timeout=5000")
            try:
                conn.execute("PRAGMA mmap_size=0")
            except sqlite3.OperationalError:
                pass
            size = path.stat().st_size
            if size <= QUICK_CHECK_MAX_BYTES:
                row = conn.execute("PRAGMA quick_check").fetchone()
                status = row[0] if row else "unknown"
                if status != "ok":
                    log(f"WARN quick_check {path.name}: {status}")
                    write_integrity_marker(path, status)
                    if should_quarantine(path, f"quick_check: {status}"):
                        conn.close()
                        quarantine_db_family(path, f"quick_check: {status}")
                    return
            else:
                log(f"quick_check skipped {path.name} size={size // (1024 * 1024)}MB max={QUICK_CHECK_MAX_BYTES // (1024 * 1024)}MB")
            checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint and int(checkpoint[0] or 0) != 0:
                log(f"WARN checkpoint busy {path.name} result={tuple(checkpoint)}")
            else:
                log(f"checkpoint ok {path.name} result={tuple(checkpoint) if checkpoint else None}")
        finally:
            conn.close()
    except Exception as exc:
        log(f"WARN checkpoint failed {path.name}: {exc}")
        if should_quarantine(path, str(exc)):
            write_integrity_marker(path, str(exc))
            quarantine_db_family(path, str(exc))


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    disk_report("before")
    cleanup_stale_backup_partials()
    for name in LOG_NAMES:
        trim_file(DATA_DIR / name)
    trim_runtime_jsonl_files()
    remove_large_temp_files()
    if DB_CHECK_ENABLED:
        for name in DB_NAMES:
            path = DATA_DIR / name
            backup_db_family(path)
            checkpoint_db(path)
    else:
        log("db checkpoint disabled for this preflight run")
    disk_report("after")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
