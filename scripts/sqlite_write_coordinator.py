#!/usr/bin/env python3
"""Cross-process SQLite write coordination for paper services.

SQLite WAL allows concurrent readers, but only one writer can commit at a
time. The paper trader has several Python workers, so process-local locks are
not enough. This module provides one file-lock-backed writer gate that can be
used as a drop-in context manager around short write transactions.
"""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import threading
import time


DEFAULT_LOCK_FILE = Path(os.environ.get("PAPER_SQLITE_WRITER_LOCK_FILE", "/tmp/paper_sqlite_single_writer.lock"))
DEFAULT_TIMEOUT_SEC = float(os.environ.get("PAPER_SQLITE_WRITER_LOCK_TIMEOUT_SEC", "10"))
POLL_SEC = float(os.environ.get("PAPER_SQLITE_WRITER_LOCK_POLL_SEC", "0.025"))
_PROCESS_WRITE_LOCK = threading.RLock()


class SQLiteSingleWriterLock:
    """A re-usable context manager combining thread and process locks."""

    def __init__(self, name: str = "paper", *, lock_file: str | os.PathLike | None = None, timeout_sec: float | None = None):
        self.name = str(name or "paper")
        self.lock_file = Path(lock_file or DEFAULT_LOCK_FILE)
        self.timeout_sec = DEFAULT_TIMEOUT_SEC if timeout_sec is None else float(timeout_sec)
        self._fh_stack = []

    def __enter__(self):
        _PROCESS_WRITE_LOCK.acquire()
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        fh = self.lock_file.open("a+", encoding="utf-8")
        deadline = time.time() + max(0.0, self.timeout_sec)
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
                    fh.close()
                    _PROCESS_WRITE_LOCK.release()
                    raise TimeoutError(f"sqlite single-writer lock timeout name={self.name} file={self.lock_file}")
                time.sleep(POLL_SEC)

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
