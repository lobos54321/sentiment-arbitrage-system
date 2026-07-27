#!/usr/bin/env python3
"""NFS-safe SQLite durability and cross-language writer coordination for Kline data."""

from __future__ import annotations

from contextlib import contextmanager
import json
import os
from pathlib import Path
import tempfile
import time
import uuid


DEFAULT_LOCK_FILE = Path(
    os.environ.get(
        "KLINE_SQLITE_WRITER_LOCK_FILE",
        str(Path(tempfile.gettempdir()) / "kline_sqlite_single_writer.lock"),
    )
)
DEFAULT_TIMEOUT_SEC = float(os.environ.get("KLINE_SQLITE_WRITER_LOCK_TIMEOUT_SEC", "30"))
POLL_SEC = float(os.environ.get("KLINE_SQLITE_WRITER_LOCK_POLL_SEC", "0.025"))
INVALID_LOCK_GRACE_SEC = 5.0


def _process_is_alive(pid: object) -> bool:
    try:
        parsed = int(pid)
    except (TypeError, ValueError):
        return False
    if parsed <= 0:
        return False
    try:
        os.kill(parsed, 0)
        return True
    except PermissionError:
        return True
    except ProcessLookupError:
        return False


def _read_owner(lock_file: Path) -> dict | None:
    try:
        value = json.loads(lock_file.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _stale_reason(lock_file: Path, owner: dict | None) -> str | None:
    if owner and owner.get("pid") and not _process_is_alive(owner.get("pid")):
        return "owner_process_not_alive"
    if owner:
        return None
    try:
        if time.time() - lock_file.stat().st_mtime >= INVALID_LOCK_GRACE_SEC:
            return "invalid_owner_record"
    except FileNotFoundError:
        return None
    return None


def _same_owner(expected: dict | None, current: dict | None) -> bool:
    if (expected or {}).get("token") or (current or {}).get("token"):
        return (expected or {}).get("token") == (current or {}).get("token")
    if (expected or {}).get("pid") or (current or {}).get("pid"):
        return (
            (expected or {}).get("pid") == (current or {}).get("pid")
            and (expected or {}).get("owner") == (current or {}).get("owner")
        )
    return expected is None and current is None


def _try_cleanup_gate(lock_file: Path) -> tuple[Path, int, str] | None:
    cleanup = Path(f"{lock_file}.cleanup")
    token = str(uuid.uuid4())
    fd = None
    try:
        fd = os.open(cleanup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        payload = json.dumps(
            {
                "schema_version": "kline_sqlite_cleanup_gate.v1",
                "token": token,
                "pid": os.getpid(),
                "acquired_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
            sort_keys=True,
        ).encode("utf-8")
        os.write(fd, payload)
        os.fsync(fd)
        return cleanup, fd, token
    except FileExistsError:
        return None
    except BaseException:
        if fd is not None:
            os.close(fd)
            try:
                cleanup.unlink()
            except FileNotFoundError:
                pass
        raise


def _release_cleanup_gate(gate: tuple[Path, int, str] | None) -> None:
    if not gate:
        return
    cleanup, fd, token = gate
    try:
        owner = _read_owner(cleanup)
        if owner and owner.get("token") == token:
            try:
                cleanup.unlink()
            except FileNotFoundError:
                pass
    finally:
        os.close(fd)


def _remove_stale_lock(lock_file: Path, observed_owner: dict | None) -> bool:
    gate = _try_cleanup_gate(lock_file)
    if not gate:
        return False
    try:
        current_owner = _read_owner(lock_file)
        reason = _stale_reason(lock_file, current_owner)
        if not reason or not _same_owner(observed_owner, current_owner):
            return False

        stale = lock_file.with_name(
            f"{lock_file.name}.stale-{int(time.time() * 1000)}-{os.getpid()}-{uuid.uuid4()}"
        )
        try:
            lock_file.replace(stale)
        except FileNotFoundError:
            return False
        try:
            stale.unlink()
        except FileNotFoundError:
            pass
        print(
            f"[kline-sqlite] removed stale writer lock ({reason}): {lock_file}",
            file=os.sys.stderr,
        )
        return True
    finally:
        _release_cleanup_gate(gate)


@contextmanager
def kline_single_writer(
    name: str,
    *,
    lock_file: str | os.PathLike | None = None,
    timeout_sec: float | None = None,
):
    """Coordinate with Node Kline writers through one atomic local lock file."""

    path = Path(
        lock_file
        or os.environ.get("KLINE_SQLITE_WRITER_LOCK_FILE")
        or DEFAULT_LOCK_FILE
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    timeout = DEFAULT_TIMEOUT_SEC if timeout_sec is None else max(0.001, float(timeout_sec))
    deadline = time.monotonic() + timeout
    token = str(uuid.uuid4())
    fd = None

    while True:
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            payload = json.dumps(
                {
                    "schema_version": "kline_sqlite_writer_lock.v1",
                    "token": token,
                    "pid": os.getpid(),
                    "owner": str(name or "unknown"),
                    "acquired_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                },
                sort_keys=True,
            ).encode("utf-8")
            os.write(fd, payload)
            os.fsync(fd)
            break
        except FileExistsError:
            owner = _read_owner(path)
            stale_reason = _stale_reason(path, owner)
            if stale_reason and _remove_stale_lock(path, owner):
                continue
            if time.monotonic() >= deadline:
                holder = (
                    f"pid={owner.get('pid', 'unknown')} owner={owner.get('owner', 'unknown')}"
                    if owner
                    else "owner=initializing"
                )
                raise TimeoutError(
                    f"timed out waiting for Kline SQLite writer lock {path} ({holder})"
                )
            time.sleep(min(POLL_SEC, max(0.001, deadline - time.monotonic())))

    try:
        yield
    finally:
        try:
            owner = _read_owner(path)
            if owner and owner.get("token") == token:
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
        finally:
            if fd is not None:
                os.close(fd)


def configure_kline_sqlite_connection(db, *, busy_timeout_ms: int | None = None) -> dict:
    """Apply and verify the Kline rollback-journal contract on one writer connection."""

    timeout_ms = int(
        busy_timeout_ms
        if busy_timeout_ms is not None
        else os.environ.get("KLINE_SQLITE_BUSY_TIMEOUT_MS", "30000")
    )
    db.execute(f"PRAGMA busy_timeout = {max(1, timeout_ms)}")
    db.execute("PRAGMA mmap_size = 0")
    row = db.execute("PRAGMA journal_mode = DELETE").fetchone()
    journal_mode = str(row[0] if row else "").strip().upper()
    db.execute("PRAGMA synchronous = FULL")
    synchronous = int(db.execute("PRAGMA synchronous").fetchone()[0])
    mmap_size = int(db.execute("PRAGMA mmap_size").fetchone()[0])
    if journal_mode != "DELETE" or synchronous != 2 or mmap_size != 0:
        raise RuntimeError(
            "unsafe Kline SQLite settings: "
            f"journal_mode={journal_mode or 'unknown'} "
            f"synchronous={synchronous} mmap_size={mmap_size}"
        )
    return {
        "journal_mode": journal_mode,
        "synchronous": "FULL",
        "mmap_size": mmap_size,
        "busy_timeout_ms": max(1, timeout_ms),
    }
