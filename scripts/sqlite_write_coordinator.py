#!/usr/bin/env python3
"""Cross-process SQLite durability and write coordination for paper services.

The production paper database lives on a mounted volume.  Rollback-journal
mode is the safe default there; WAL relies on shared-memory semantics that are
not reliable on network filesystems.  The file lock also serializes the short
write transactions issued by separate paper workers.
"""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import threading
import time


DEFAULT_LOCK_FILE = Path(os.environ.get("PAPER_SQLITE_WRITER_LOCK_FILE", "/tmp/paper_sqlite_single_writer.lock"))
DEFAULT_TIMEOUT_SEC = float(os.environ.get("PAPER_SQLITE_WRITER_LOCK_TIMEOUT_SEC", "90"))
POLL_SEC = float(os.environ.get("PAPER_SQLITE_WRITER_LOCK_POLL_SEC", "0.025"))
_PROCESS_WRITE_LOCK = threading.RLock()
ALLOWED_JOURNAL_MODES = {"DELETE", "TRUNCATE", "PERSIST", "MEMORY", "WAL", "OFF"}
ALLOWED_SYNCHRONOUS_MODES = {"OFF", "NORMAL", "FULL", "EXTRA"}


def configure_paper_sqlite_connection(
    db,
    *,
    journal_mode: str | None = None,
    synchronous: str | None = None,
    busy_timeout_ms: int | None = None,
):
    """Apply and verify the durability contract for a writable paper DB."""

    requested_journal = str(
        journal_mode or os.environ.get("PAPER_SQLITE_JOURNAL_MODE", "DELETE")
    ).strip().upper()
    requested_sync = str(
        synchronous or os.environ.get("PAPER_SQLITE_SYNCHRONOUS", "FULL")
    ).strip().upper()
    if requested_journal not in ALLOWED_JOURNAL_MODES:
        raise ValueError(f"unsupported paper SQLite journal mode: {requested_journal}")
    if requested_sync not in ALLOWED_SYNCHRONOUS_MODES:
        raise ValueError(f"unsupported paper SQLite synchronous mode: {requested_sync}")

    timeout_ms = int(
        busy_timeout_ms
        if busy_timeout_ms is not None
        else os.environ.get("PAPER_SQLITE_BUSY_TIMEOUT_MS", "30000")
    )
    db.execute(f"PRAGMA busy_timeout = {max(0, timeout_ms)}")
    try:
        db.execute("PRAGMA mmap_size = 0")
    except Exception:
        pass

    row = db.execute(f"PRAGMA journal_mode = {requested_journal}").fetchone()
    applied_journal = str(row[0] if row else "").strip().upper()
    if applied_journal != requested_journal:
        raise RuntimeError(
            "paper SQLite journal mode mismatch: "
            f"requested={requested_journal} applied={applied_journal or 'unknown'}"
        )
    db.execute(f"PRAGMA synchronous = {requested_sync}")
    applied_sync = int(db.execute("PRAGMA synchronous").fetchone()[0])
    return {
        "journal_mode": applied_journal,
        "synchronous": applied_sync,
        "busy_timeout_ms": max(0, timeout_ms),
    }


class SQLiteSingleWriterLock:
    """A re-usable context manager combining thread and process locks."""

    def __init__(self, name: str = "paper", *, lock_file: str | os.PathLike | None = None, timeout_sec: float | None = None):
        self.name = str(name or "paper")
        self.lock_file = Path(lock_file or DEFAULT_LOCK_FILE)
        self.timeout_sec = DEFAULT_TIMEOUT_SEC if timeout_sec is None else float(timeout_sec)
        self._fh_stack = []

    def __enter__(self):
        deadline = time.time() + max(0.0, self.timeout_sec)
        acquired_process_lock = _PROCESS_WRITE_LOCK.acquire(timeout=max(0.0, deadline - time.time()))
        if not acquired_process_lock:
            raise TimeoutError(
                f"sqlite single-writer process lock timeout name={self.name} "
                f"file={self.lock_file}"
            )
        fh = None
        try:
            self.lock_file.parent.mkdir(parents=True, exist_ok=True)
            fh = self.lock_file.open("a+", encoding="utf-8")
            while True:
                try:
                    fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    fh.seek(0)
                    fh.truncate()
                    fh.write(f"{os.getpid()} {self.name} {time.time():.3f}\n")
                    fh.flush()
                    self._fh_stack.append(fh)
                    return self
                except BlockingIOError:
                    if time.time() >= deadline:
                        try:
                            holder = self.lock_file.read_text(encoding="utf-8").strip()
                        except OSError:
                            holder = "unknown"
                        fh.close()
                        fh = None
                        _PROCESS_WRITE_LOCK.release()
                        acquired_process_lock = False
                        raise TimeoutError(
                            f"sqlite single-writer lock timeout name={self.name} "
                            f"file={self.lock_file} holder={holder[:160]}"
                        )
                    time.sleep(POLL_SEC)
        except BaseException:
            if fh is not None:
                fh.close()
            if acquired_process_lock:
                _PROCESS_WRITE_LOCK.release()
            raise

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._fh_stack:
                fh = self._fh_stack.pop()
                try:
                    fcntl.flock(fh, fcntl.LOCK_UN)
                finally:
                    fh.close()
        finally:
            _PROCESS_WRITE_LOCK.release()
        return False


@contextmanager
def sqlite_single_writer(name: str = "paper", *, lock_file: str | os.PathLike | None = None, timeout_sec: float | None = None):
    lock = SQLiteSingleWriterLock(name, lock_file=lock_file, timeout_sec=timeout_sec)
    with lock:
        yield lock


def coordinated_sqlite_write(writer, *, name: str = "paper", lock_file: str | os.PathLike | None = None, timeout_sec: float | None = None):
    """Run a synchronous writer callable under the single-writer lock."""
    with sqlite_single_writer(name, lock_file=lock_file, timeout_sec=timeout_sec):
        return writer()
