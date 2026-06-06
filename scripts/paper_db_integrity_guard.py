#!/usr/bin/env python3
"""Fail-closed helpers for the shared paper SQLite database.

The paper DB is the canonical evidence ledger.  Once a malformed marker exists,
workers must stop touching it and let Zeabur preflight quarantine/recreate the DB
family.  Continuing to read/write a marked DB keeps the runtime in a split-brain
state and prevents clean denominator accumulation.
"""

from __future__ import annotations

from pathlib import Path


class PaperDBIntegrityMarked(RuntimeError):
    """Raised when a worker attempts to open a paper DB marked as malformed."""


def paper_db_integrity_marker_path(db_path: str | Path) -> Path:
    path = Path(db_path)
    return Path(f"{path}.integrity_error")


def paper_db_integrity_marker_present(db_path: str | Path) -> bool:
    return paper_db_integrity_marker_path(db_path).exists()


def is_paper_db_path(db_path: str | Path) -> bool:
    return Path(db_path).name == "paper_trades.db"


def read_paper_db_integrity_marker(db_path: str | Path, *, limit: int = 4000) -> str:
    marker = paper_db_integrity_marker_path(db_path)
    try:
        return marker.read_text(encoding="utf-8", errors="replace")[:limit]
    except Exception as exc:
        return f"integrity marker present but unreadable: {exc}"


def require_unmarked_paper_db(db_path: str | Path, *, component: str = "paper_db") -> None:
    if not is_paper_db_path(db_path):
        return
    if not paper_db_integrity_marker_present(db_path):
        return
    marker_text = read_paper_db_integrity_marker(db_path)
    first_line = marker_text.splitlines()[0] if marker_text else "integrity marker present"
    raise PaperDBIntegrityMarked(
        f"{component}: refusing to open marked paper DB {db_path}: {first_line}"
    )
