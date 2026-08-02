#!/usr/bin/env python3
"""Build and atomically publish a bounded cross-database evaluator snapshot."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
from typing import Any
from urllib.parse import quote


SCHEMA_VERSION = "cross_db_evaluator_snapshot.v1"
SNAPSHOT_NAME_RE = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{8}$")
PARTIAL_SNAPSHOT_NAME_RE = re.compile(r"^\.\d{8}T\d{6}Z-[0-9a-f]{8}\.partial$")
DATABASE_SPECS = {
    "signal": {
        "filename": "signal.db",
        "required_tables": ("premium_signals",),
        "watermarks": {"premium_signals": ("id", "source_message_ts", "timestamp", "receive_ts")},
    },
    "paper": {
        "filename": "paper_evidence.db",
        "required_tables": (
            "candidate_shadow_observations",
            "candidate_shadow_virtual_trades",
            "paper_decision_events",
            "a_class_decision_events",
            "a_class_mode_runtime_state",
            "paper_trades",
            "opportunity_events",
        ),
        "watermarks": {
            "candidate_shadow_observations": ("signal_id", "signal_ts", "observed_at"),
            "candidate_shadow_virtual_trades": ("signal_id", "observed_at", "closed_at"),
            "paper_decision_events": ("id", "event_ts", "created_at"),
            "a_class_decision_events": ("id", "event_ts", "created_at"),
            "a_class_mode_runtime_state": ("id", "updated_at", "evaluated_at", "created_at"),
            "paper_trades": ("id", "entry_time", "exit_time", "created_at"),
            "opportunity_events": ("id", "event_ts", "created_at"),
        },
    },
    "raw": {
        "filename": "raw.db",
        "required_tables": ("raw_signal_outcomes",),
        "watermarks": {"raw_signal_outcomes": ("id", "signal_id", "signal_ts", "updated_at")},
    },
    "kline": {
        "filename": "kline.db",
        "required_tables": ("kline_1m",),
        "watermarks": {"kline_1m": ("timestamp", "fetched_at", "updated_at")},
    },
}


def utc_iso(epoch: float | None = None) -> str:
    value = time.time() if epoch is None else epoch
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextmanager
def exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"evaluator_snapshot_lock_held:{path}") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"{os.getpid()} {utc_iso()}\n")
        handle.flush()
        yield
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def readonly_connection(path: Path) -> sqlite3.Connection:
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def database_metadata(connection: sqlite3.Connection, spec: dict[str, Any]) -> dict[str, Any]:
    table_rows = connection.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    table_sql = {str(row["name"]): str(row["sql"] or "") for row in table_rows}
    table_names = set(table_sql)
    missing_required = [name for name in spec["required_tables"] if name not in table_names]
    missing_required_watermarks = []
    watermarks = {}
    for table, candidates in spec["watermarks"].items():
        if table not in table_names:
            continue
        columns = {
            str(row["name"])
            for row in connection.execute(f"PRAGMA table_info({quote_identifier(table)})")
        }
        selected = [name for name in candidates if name in columns]
        if not selected:
            if table in spec["required_tables"]:
                missing_required_watermarks.append(table)
            continue
        expressions = ", ".join(
            f"MAX({quote_identifier(column)}) AS {quote_identifier(column)}"
            for column in selected
        )
        row = connection.execute(
            f"SELECT {expressions} FROM {quote_identifier(table)}"
        ).fetchone()
        watermarks[table] = {column: row[column] for column in selected}
    schema_text = "\n".join(f"{name}\n{table_sql[name]}" for name in sorted(table_sql))
    return {
        "schema_version": int(connection.execute("PRAGMA schema_version").fetchone()[0]),
        "user_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
        "application_id": int(connection.execute("PRAGMA application_id").fetchone()[0]),
        "page_size": int(connection.execute("PRAGMA page_size").fetchone()[0]),
        "page_count": int(connection.execute("PRAGMA page_count").fetchone()[0]),
        "freelist_count": int(connection.execute("PRAGMA freelist_count").fetchone()[0]),
        "table_schema_sha256": hashlib.sha256(schema_text.encode()).hexdigest(),
        "table_count": len(table_names),
        "missing_required_tables": missing_required,
        "missing_required_watermarks": missing_required_watermarks,
        "upper_watermarks": watermarks,
    }


def source_page_stats(connection: sqlite3.Connection, source: Path) -> dict[str, int]:
    page_size = int(connection.execute("PRAGMA page_size").fetchone()[0] or 0)
    page_count = int(connection.execute("PRAGMA page_count").fetchone()[0] or 0)
    freelist_count = int(connection.execute("PRAGMA freelist_count").fetchone()[0] or 0)
    return {
        "source_size_bytes": int(source.stat().st_size),
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "estimated_compact_bytes": max(page_size, (page_count - freelist_count) * page_size),
    }


def inspect_source_page_reports(source_paths: dict[str, Path]) -> dict[str, dict[str, int]]:
    reports = {}
    for name, source in source_paths.items():
        if not source.is_file():
            raise FileNotFoundError(source)
        connection = readonly_connection(source)
        try:
            reports[name] = source_page_stats(connection, source)
        finally:
            connection.close()
    return reports


def snapshot_one(
    source: Path,
    destination: Path,
    spec: dict[str, Any],
    source_connection: sqlite3.Connection,
    pin_report: dict[str, Any],
) -> dict[str, Any]:
    if not source.is_file():
        raise FileNotFoundError(source)
    started = time.time()
    source_stat_before = source.stat()
    backup_path = destination.with_name(f".{destination.name}.full-backup.tmp")
    destination_connection = None
    try:
        data_version_before = int(source_connection.execute("PRAGMA data_version").fetchone()[0])
        destination_connection = sqlite3.connect(backup_path)
        source_connection.backup(destination_connection, pages=4096, sleep=0.01)
        destination_connection.commit()
        data_version_after = int(source_connection.execute("PRAGMA data_version").fetchone()[0])
    finally:
        if destination_connection is not None:
            destination_connection.close()
    backup_finished = time.time()
    source_stat_after = source.stat()
    source_connection.rollback()
    temporary_backup_size = backup_path.stat().st_size
    compact_source = sqlite3.connect(backup_path)
    try:
        compact_source.execute("PRAGMA busy_timeout=30000")
        compact_source.execute("VACUUM INTO ?", (str(destination),))
    finally:
        compact_source.close()
    backup_path.unlink()
    finished = time.time()
    check = sqlite3.connect(destination)
    check.row_factory = sqlite3.Row
    try:
        quick_check = [str(row[0]) for row in check.execute("PRAGMA quick_check").fetchall()]
        metadata = database_metadata(check, spec)
    finally:
        check.close()
    if quick_check != ["ok"]:
        raise RuntimeError(f"snapshot quick_check failed for {source}: {quick_check[:20]}")
    if metadata["missing_required_tables"]:
        raise RuntimeError(
            f"snapshot missing required tables for {source}: {metadata['missing_required_tables']}"
        )
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())
    return {
        "source_path": str(source.resolve()),
        "snapshot_path": str(destination.resolve()),
        "started_at": utc_iso(started),
        "finished_at": utc_iso(finished),
        "source_read_view_released_at": utc_iso(backup_finished),
        "started_epoch": started,
        "finished_epoch": finished,
        "midpoint_epoch": (started + finished) / 2,
        "duration_sec": round(finished - started, 6),
        "source_read_lock_duration_sec": round(backup_finished - started, 6),
        "source_size_bytes_before": source_stat_before.st_size,
        "source_size_bytes_after": source_stat_after.st_size,
        "source_mtime_before": source_stat_before.st_mtime,
        "source_mtime_after": source_stat_after.st_mtime,
        "source_data_version_before": data_version_before,
        "source_data_version_after": data_version_after,
        "source_connection_total_changes": int(source_connection.total_changes),
        "source_mutated_by_snapshot_process": bool(source_connection.total_changes),
        "source_changed_during_backup": (
            data_version_before != data_version_after
            or source_stat_before.st_mtime_ns != source_stat_after.st_mtime_ns
            or source_stat_before.st_size != source_stat_after.st_size
        ),
        "temporary_full_backup_size_bytes": temporary_backup_size,
        "snapshot_size_bytes": destination.stat().st_size,
        "compaction_removed_bytes": max(0, temporary_backup_size - destination.stat().st_size),
        "snapshot_sha256": sha256_file(destination),
        "quick_check": quick_check,
        "pinned_read_view": pin_report,
        **metadata,
    }


def snapshot_all_concurrently(
    source_paths: dict[str, Path],
    partial_dir: Path,
    source_page_reports: dict[str, dict[str, int]],
) -> dict[str, dict[str, Any]]:
    names = tuple(DATABASE_SPECS)
    start_barrier = threading.Barrier(len(names))
    pinned_barrier = threading.Barrier(len(names))
    reports: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    result_lock = threading.Lock()

    def worker(name: str) -> None:
        connection = None
        try:
            source = source_paths[name]
            connection = readonly_connection(source)
            start_barrier.wait(timeout=30)
            pin_started = time.time()
            connection.execute("BEGIN")
            connection.execute("SELECT COUNT(*) FROM sqlite_master").fetchone()
            pin_finished = time.time()
            pin_report = {
                "pinned_started_at": utc_iso(pin_started),
                "pinned_finished_at": utc_iso(pin_finished),
                "pinned_started_epoch": pin_started,
                "pinned_finished_epoch": pin_finished,
                "pinned_midpoint_epoch": (pin_started + pin_finished) / 2,
                **source_page_reports[name],
            }
            pinned_barrier.wait(timeout=30)
            report = snapshot_one(
                source,
                partial_dir / DATABASE_SPECS[name]["filename"],
                DATABASE_SPECS[name],
                connection,
                pin_report,
            )
            with result_lock:
                reports[name] = report
        except Exception as exc:
            for barrier in (start_barrier, pinned_barrier):
                try:
                    barrier.abort()
                except threading.BrokenBarrierError:
                    pass
            with result_lock:
                errors[name] = f"{type(exc).__name__}:{exc}"
        finally:
            if connection is not None:
                try:
                    connection.rollback()
                except sqlite3.Error:
                    pass
                connection.close()

    threads = [threading.Thread(target=worker, args=(name,), name=f"snapshot-{name}") for name in names]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise RuntimeError(f"concurrent evaluator snapshot failed: {errors}")
    if set(reports) != set(names):
        raise RuntimeError(f"concurrent evaluator snapshot incomplete: {sorted(reports)}")
    return reports


def detected_commit(repo_root: Path) -> str | None:
    for name in ("ZEABUR_GIT_COMMIT_SHA", "ZEABUR_GIT_COMMIT", "GIT_COMMIT", "COMMIT_SHA"):
        value = os.environ.get(name)
        if value:
            return value
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            text=True,
            timeout=5,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return None


def disk_preflight(
    root: Path,
    pin_reports: dict[str, dict[str, Any]],
    min_free_after_gib: float,
) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(root)
    estimated_compact = sum(int(row["estimated_compact_bytes"]) for row in pin_reports.values())
    concurrent_full_backups = sum(int(row["source_size_bytes"]) for row in pin_reports.values())
    estimated_peak = estimated_compact + concurrent_full_backups
    reserve = int(float(min_free_after_gib) * 1024**3)
    accepted = usage.free >= estimated_peak + reserve
    return {
        "free_bytes": usage.free,
        "estimated_compact_snapshot_bytes": estimated_compact,
        "concurrent_temporary_full_backup_bytes": concurrent_full_backups,
        "estimated_peak_working_bytes": estimated_peak,
        "required_reserve_bytes": reserve,
        "estimated_free_after_bytes": usage.free - estimated_compact,
        "estimated_free_at_peak_bytes": usage.free - estimated_peak,
        "accepted": accepted,
    }


def publish_current(root: Path, snapshot_dir: Path) -> None:
    current = root / "current"
    if current.exists() and not current.is_symlink():
        raise RuntimeError(f"current path must be a symlink or absent: {current}")
    temporary = root / f".current.{snapshot_dir.name}.tmp"
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(Path("snapshots") / snapshot_dir.name, target_is_directory=True)
    os.replace(temporary, current)


def prune_old_snapshots(root: Path, current_name: str, keep_previous: int) -> list[str]:
    snapshots = root / "snapshots"
    valid = []
    for path in snapshots.iterdir():
        if not path.is_dir() or not SNAPSHOT_NAME_RE.fullmatch(path.name):
            continue
        manifest_path = path / "manifest.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if manifest.get("schema_version") == SCHEMA_VERSION and manifest.get("accepted") is True:
            valid.append(path)
    valid.sort(key=lambda item: item.name, reverse=True)
    protected = {current_name, *[path.name for path in valid if path.name != current_name][:keep_previous]}
    removed = []
    for path in valid:
        if path.name in protected:
            continue
        shutil.rmtree(path)
        removed.append(str(path))
    return removed


def cleanup_interrupted_partials(root: Path) -> list[str]:
    snapshots = root / "snapshots"
    if not snapshots.is_dir():
        return []
    removed = []
    for path in snapshots.iterdir():
        if path.is_dir() and PARTIAL_SNAPSHOT_NAME_RE.fullmatch(path.name):
            shutil.rmtree(path)
            removed.append(str(path))
    return removed


def build_snapshot_bundle(
    *,
    sources: dict[str, str],
    out_root: str,
    repo_root: str,
    max_skew_sec: float = 300,
    min_free_after_gib: float = 5,
    keep_previous: int = 0,
    snapshot_id: str | None = None,
) -> dict[str, Any]:
    root = Path(out_root).expanduser().resolve()
    source_paths = {name: Path(path).expanduser().resolve() for name, path in sources.items()}
    if set(source_paths) != set(DATABASE_SPECS):
        raise ValueError(f"sources must be exactly {sorted(DATABASE_SPECS)}")
    sid = snapshot_id or f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{os.urandom(4).hex()}"
    if not SNAPSHOT_NAME_RE.fullmatch(sid):
        raise ValueError(f"invalid snapshot id: {sid}")
    snapshots_root = root / "snapshots"
    snapshots_root.mkdir(parents=True, exist_ok=True)
    final_dir = snapshots_root / sid
    partial_dir = snapshots_root / f".{sid}.partial"
    if final_dir.exists() or partial_dir.exists():
        raise FileExistsError(sid)
    partial_dir.mkdir()
    started = time.time()
    try:
        source_page_reports = inspect_source_page_reports(source_paths)
        preflight = disk_preflight(root, source_page_reports, min_free_after_gib)
        if not preflight["accepted"]:
            raise RuntimeError(f"insufficient disk for evaluator snapshot: {preflight}")
        database_reports = snapshot_all_concurrently(
            source_paths,
            partial_dir,
            source_page_reports,
        )
        pin_midpoints = [
            float(report["pinned_read_view"]["pinned_midpoint_epoch"])
            for report in database_reports.values()
        ]
        skew = max(pin_midpoints) - min(pin_midpoints)
        source_mutation_free = all(
            not report["source_mutated_by_snapshot_process"]
            for report in database_reports.values()
        )
        quick_checks_passed = all(
            report["quick_check"] == ["ok"] for report in database_reports.values()
        )
        required_tables_present = all(
            not report["missing_required_tables"] for report in database_reports.values()
        )
        required_watermarks_present = all(
            not report["missing_required_watermarks"] for report in database_reports.values()
        )
        git_commit = detected_commit(Path(repo_root).expanduser().resolve())
        accepted = bool(
            quick_checks_passed
            and required_tables_present
            and required_watermarks_present
            and source_mutation_free
            and skew <= max_skew_sec
            and git_commit
        )
        for name, report in database_reports.items():
            report["snapshot_filename"] = DATABASE_SPECS[name]["filename"]
            report["snapshot_path"] = str((final_dir / DATABASE_SPECS[name]["filename"]).resolve())
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "snapshot_id": sid,
            "generated_at": utc_iso(),
            "snapshot_started_at": utc_iso(started),
            "snapshot_completed_at": utc_iso(),
            "snapshot_ts": min(pin_midpoints),
            "git_commit": git_commit,
            "git_commit_present": bool(git_commit),
            "method": "concurrent_read_view_pin_then_parallel_backup_and_compact",
            "read_views_pinned_before_copy": True,
            "source_mutation_free": source_mutation_free,
            "copy_mode": "parallel_per_database_release_on_completion",
            "cross_database_time_skew_sec": round(skew, 6),
            "max_allowed_cross_database_time_skew_sec": float(max_skew_sec),
            "cross_database_time_skew_passed": skew <= max_skew_sec,
            "quick_checks_passed": quick_checks_passed,
            "required_tables_present": required_tables_present,
            "required_watermarks_present": required_watermarks_present,
            "disk_preflight": preflight,
            "databases": database_reports,
            "accepted": accepted,
            "immutable": True,
            "promotion_allowed": False,
        }
        if not accepted:
            raise RuntimeError(f"cross-database snapshot acceptance failed: {manifest}")
        atomic_json(partial_dir / "manifest.json", manifest)
        os.replace(partial_dir, final_dir)
        fsync_directory(snapshots_root)
        publish_current(root, final_dir)
        fsync_directory(root)
        removed = prune_old_snapshots(root, final_dir.name, max(0, int(keep_previous)))
        latest_manifest = {
            **manifest,
            "retention": {
            "keep_previous": max(0, int(keep_previous)),
            "removed_snapshots": removed,
            },
        }
        atomic_json(root / "latest_manifest.json", latest_manifest)
        return latest_manifest
    except BaseException:
        if partial_dir.exists():
            shutil.rmtree(partial_dir)
        raise


def self_test() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        sources = {}
        definitions = {
            "signal": "CREATE TABLE premium_signals(id INTEGER, source_message_ts INTEGER)",
            "paper": (
                "CREATE TABLE candidate_shadow_observations(signal_id INTEGER, observed_at INTEGER);"
                "CREATE TABLE candidate_shadow_virtual_trades(signal_id INTEGER, observed_at INTEGER);"
                "CREATE TABLE paper_decision_events(id INTEGER, event_ts INTEGER);"
                "CREATE TABLE a_class_decision_events(id INTEGER, event_ts INTEGER);"
                "CREATE TABLE a_class_mode_runtime_state(id INTEGER, updated_at INTEGER);"
                "CREATE TABLE paper_trades(id INTEGER, entry_time INTEGER);"
                "CREATE TABLE opportunity_events(id INTEGER, event_ts INTEGER)"
            ),
            "raw": "CREATE TABLE raw_signal_outcomes(id INTEGER, signal_id INTEGER, updated_at INTEGER)",
            "kline": "CREATE TABLE kline_1m(token_ca TEXT, timestamp INTEGER)",
        }
        for name, ddl in definitions.items():
            path = root / f"{name}-source.db"
            connection = sqlite3.connect(path)
            connection.executescript(ddl)
            connection.commit()
            connection.close()
            sources[name] = str(path)
        out_root = root / "evidence"
        previous_commit = os.environ.get("ZEABUR_GIT_COMMIT_SHA")
        os.environ["ZEABUR_GIT_COMMIT_SHA"] = "self-test-commit"
        try:
            manifest = build_snapshot_bundle(
                sources=sources,
                out_root=str(out_root),
                repo_root=str(Path(__file__).resolve().parent.parent),
                max_skew_sec=30,
                min_free_after_gib=0,
                snapshot_id="20260101T000000Z-1234abcd",
            )
        finally:
            if previous_commit is None:
                os.environ.pop("ZEABUR_GIT_COMMIT_SHA", None)
            else:
                os.environ["ZEABUR_GIT_COMMIT_SHA"] = previous_commit
        assert manifest["accepted"] is True
        assert (out_root / "current" / "paper_evidence.db").is_file()
        assert json.loads((out_root / "current" / "manifest.json").read_text())["accepted"] is True
    print("SELF_TEST_PASS cross_db_evaluator_snapshot")


def run_snapshot_once(args: argparse.Namespace) -> dict[str, Any]:
    started = utc_iso()
    try:
        with exclusive_lock(Path(args.lock_file).expanduser().resolve()):
            interrupted_partials_removed = cleanup_interrupted_partials(
                Path(args.out_root).expanduser().resolve()
            )
            manifest = build_snapshot_bundle(
                sources={
                    "signal": args.signal_db,
                    "paper": args.paper_db,
                    "raw": args.raw_db,
                    "kline": args.kline_db,
                },
                out_root=args.out_root,
                repo_root=args.repo_root,
                max_skew_sec=args.max_skew_sec,
                min_free_after_gib=args.min_free_after_gib,
                keep_previous=args.keep_previous,
                snapshot_id=args.snapshot_id,
            )
        status = {
            "schema_version": "cross_db_evaluator_snapshot_worker_status.v1",
            "started_at": started,
            "finished_at": utc_iso(),
            "status": "completed",
            "snapshot_id": manifest["snapshot_id"],
            "accepted": True,
            "current": str(Path(args.out_root).resolve() / "current"),
            "interrupted_partials_removed": interrupted_partials_removed,
            "promotion_allowed": False,
        }
    except Exception as exc:
        status = {
            "schema_version": "cross_db_evaluator_snapshot_worker_status.v1",
            "started_at": started,
            "finished_at": utc_iso(),
            "status": "failed",
            "accepted": False,
            "error": f"{type(exc).__name__}:{exc}",
            "promotion_allowed": False,
        }
    if args.status_out:
        atomic_json(Path(args.status_out).expanduser().resolve(), status)
    print(json.dumps(status, sort_keys=True), flush=True)
    return status


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signal-db", default="/app/data/sentiment_arb.db")
    parser.add_argument("--paper-db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument("--out-root", default="/app/data/agent_evidence")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parent.parent))
    parser.add_argument("--max-skew-sec", type=float, default=300)
    parser.add_argument("--min-free-after-gib", type=float, default=5)
    parser.add_argument("--keep-previous", type=int, default=0)
    parser.add_argument("--snapshot-id")
    parser.add_argument("--lock-file", default="/tmp/cross-db-evaluator-snapshot.lock")
    parser.add_argument("--status-out", default="/app/data/agent_evidence/snapshot_status.json")
    parser.add_argument("--max-runs", type=int, default=1)
    parser.add_argument("--interval-sec", type=int, default=21600)
    parser.add_argument("--initial-delay-sec", type=int, default=0)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if args.initial_delay_sec > 0:
        time.sleep(args.initial_delay_sec)
    max_runs = int(args.max_runs)
    run_count = 0
    last_status = None
    while max_runs <= 0 or run_count < max_runs:
        run_count += 1
        last_status = run_snapshot_once(args)
        if max_runs > 0 and run_count >= max_runs:
            break
        time.sleep(max(1, int(args.interval_sec)))
    return 0 if last_status and last_status["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
