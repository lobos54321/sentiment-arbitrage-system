#!/usr/bin/env python3
"""Shared read-only SQLite inspection helpers for Phase 4 evidence recovery."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
from typing import Any, Iterable
from urllib.parse import quote


SQLITE_HEADER = b"SQLite format 3\x00"
TIMESTAMP_COLUMNS = (
    "timestamp",
    "timestamp_sec",
    "ts",
    "sample_ts",
    "block_time",
    "fetched_at",
    "updated_at",
    "created_at",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def atomic_write_json(path: str | os.PathLike[str], payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True, ensure_ascii=True)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, target)
        fsync_directory(target.parent)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def fsync_directory(path: str | os.PathLike[str]) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    fd = os.open(Path(path), flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def sha256_file(path: str | os.PathLike[str]) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sqlite_readonly_uri(path: str | os.PathLike[str]) -> str:
    absolute = Path(path).expanduser().resolve()
    return f"file:{quote(str(absolute), safe='/')}?mode=ro"


def open_sqlite_readonly(path: str | os.PathLike[str], *, timeout_sec: float = 5.0) -> sqlite3.Connection:
    connection = sqlite3.connect(sqlite_readonly_uri(path), uri=True, timeout=timeout_sec)
    connection.execute("PRAGMA query_only = ON")
    connection.execute(f"PRAGMA busy_timeout = {max(0, int(timeout_sec * 1000))}")
    return connection


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _file_metadata(path: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return {
            "exists": False,
            "size_bytes": None,
            "inode": None,
            "mtime_epoch": None,
            "mtime": None,
        }
    return {
        "exists": True,
        "size_bytes": stat.st_size,
        "inode": stat.st_ino,
        "mtime_epoch": stat.st_mtime,
        "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def _table_summary(connection: sqlite3.Connection, table_name: str) -> dict[str, Any]:
    quoted = quote_identifier(table_name)
    columns = [row[1] for row in connection.execute(f"PRAGMA table_info({quoted})").fetchall()]
    row_count = int(connection.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0])
    timestamp_column = next((name for name in TIMESTAMP_COLUMNS if name in columns), None)
    min_timestamp = None
    max_timestamp = None
    if timestamp_column:
        timestamp_quoted = quote_identifier(timestamp_column)
        min_timestamp, max_timestamp = connection.execute(
            f"SELECT MIN({timestamp_quoted}), MAX({timestamp_quoted}) FROM {quoted}"
        ).fetchone()
    return {
        "name": table_name,
        "row_count": row_count,
        "columns": columns,
        "timestamp_column": timestamp_column,
        "min_timestamp": min_timestamp,
        "max_timestamp": max_timestamp,
    }


def inspect_sqlite(
    path: str | os.PathLike[str],
    *,
    include_hash: bool = True,
    include_tables: bool = True,
    timeout_sec: float = 5.0,
) -> dict[str, Any]:
    """Inspect a SQLite file without creating or mutating it."""
    target = Path(path).expanduser().resolve()
    metadata = _file_metadata(target)
    result: dict[str, Any] = {
        "path": str(target),
        **metadata,
        "classification": "MISSING",
        "sqlite_header_valid": False,
        "first_64_bytes_hex": None,
        "all_zero_first_64_bytes": None,
        "sha256": None,
        "quick_check": None,
        "tables": [],
        "wal": _file_metadata(Path(f"{target}-wal")),
        "shm": _file_metadata(Path(f"{target}-shm")),
        "error": None,
        "read_only": True,
    }
    if not metadata["exists"]:
        return result

    try:
        with target.open("rb") as fh:
            first_64 = fh.read(64)
    except OSError as error:
        result["classification"] = "LOCKED" if "busy" in str(error).lower() else "MALFORMED"
        result["error"] = str(error)
        return result

    result["first_64_bytes_hex"] = first_64.hex()
    result["all_zero_first_64_bytes"] = bool(first_64) and not any(first_64)
    result["sqlite_header_valid"] = first_64.startswith(SQLITE_HEADER)
    if include_hash:
        try:
            result["sha256"] = sha256_file(target)
        except OSError as error:
            result["error"] = f"sha256_failed: {error}"

    if not result["sqlite_header_valid"]:
        result["classification"] = "INVALID_HEADER"
        return result

    connection = None
    try:
        connection = open_sqlite_readonly(target, timeout_sec=timeout_sec)
        connection.execute("BEGIN")
        quick_rows = connection.execute("PRAGMA quick_check").fetchall()
        quick_values = [str(row[0]) for row in quick_rows]
        result["quick_check"] = quick_values
        if quick_values != ["ok"]:
            result["classification"] = "MALFORMED"
            result["error"] = "; ".join(quick_values[:20])
            return result
        if include_tables:
            table_names = [
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
                ).fetchall()
            ]
            for table_name in table_names:
                try:
                    result["tables"].append(_table_summary(connection, table_name))
                except sqlite3.DatabaseError as error:
                    result["tables"].append({"name": table_name, "error": str(error)})
        result["classification"] = "HEALTHY"
    except sqlite3.OperationalError as error:
        message = str(error)
        result["classification"] = "LOCKED" if any(term in message.lower() for term in ("locked", "busy")) else "MALFORMED"
        result["error"] = message
    except sqlite3.DatabaseError as error:
        result["classification"] = "MALFORMED"
        result["error"] = str(error)
    finally:
        if connection is not None:
            try:
                connection.rollback()
            except sqlite3.DatabaseError:
                pass
            connection.close()
    return result


def find_process_references(
    path: str | os.PathLike[str],
    *,
    proc_root: str | os.PathLike[str] = "/proc",
    exclude_pids: Iterable[int] = (),
) -> dict[str, Any]:
    """Return Linux /proc processes that currently hold the target or sidecars."""
    root = Path(proc_root)
    target = Path(path).expanduser().resolve()
    target_strings = {str(target), f"{target}-wal", f"{target}-shm"}
    excluded = {int(pid) for pid in exclude_pids}
    output: dict[str, Any] = {
        "available": root.exists(),
        "proc_root": str(root),
        "target": str(target),
        "references": [],
        "errors": [],
    }
    if not root.exists():
        return output
    for pid_dir in sorted(root.iterdir(), key=lambda item: item.name):
        if not pid_dir.name.isdigit() or int(pid_dir.name) in excluded:
            continue
        fd_dir = pid_dir / "fd"
        try:
            fd_paths = list(fd_dir.iterdir())
        except FileNotFoundError:
            # Processes can exit while /proc is being scanned.
            continue
        except OSError as error:
            output["errors"].append(f"pid={pid_dir.name} fd_dir={fd_dir}: {error}")
            continue
        matched_fds = []
        for fd_path in fd_paths:
            try:
                resolved = os.readlink(fd_path)
            except FileNotFoundError:
                # Individual descriptors can close during the scan.
                continue
            except OSError as error:
                output["errors"].append(
                    f"pid={pid_dir.name} fd={fd_path.name} readlink_failed: {error}"
                )
                continue
            normalized = resolved.removesuffix(" (deleted)")
            try:
                normalized = str(Path(normalized).resolve())
            except OSError:
                pass
            if normalized in target_strings:
                matched_fds.append({"fd": fd_path.name, "path": resolved})
        if not matched_fds:
            continue
        command = None
        try:
            command = (pid_dir / "cmdline").read_bytes().replace(b"\x00", b" ").decode("utf-8", "replace").strip()
        except OSError:
            pass
        output["references"].append({
            "pid": int(pid_dir.name),
            "command": command,
            "fds": matched_fds,
        })
    return output


def parse_time(value: Any) -> datetime | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 1_000_000_000_000:
            timestamp /= 1000.0
        try:
            return datetime.fromtimestamp(timestamp, timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    try:
        numeric = float(text)
    except ValueError:
        numeric = None
    if numeric is not None:
        return parse_time(numeric)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat().replace("+00:00", "Z") if value else None
