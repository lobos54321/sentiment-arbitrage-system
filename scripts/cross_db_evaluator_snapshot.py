#!/usr/bin/env python3
"""Build and atomically publish a bounded cross-database evaluator snapshot."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import itertools
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


SCHEMA_VERSION = "cross_db_evaluator_snapshot.v3"
PRUNABLE_SCHEMA_VERSIONS = {
    SCHEMA_VERSION,
    "cross_db_evaluator_snapshot.v2",
    "cross_db_evaluator_snapshot.v1",
}
SELECTION_SCHEMA_VERSION = "evaluator_snapshot_selection.v1"
BUDGET_SCHEMA_VERSION = "evaluator_snapshot_budget.v2"
PAYLOAD_PROJECTION_SCHEMA_VERSION = "candidate_observation_payload_projection.v1"
WORKER_STATUS_SCHEMA_VERSION = "cross_db_evaluator_snapshot_worker_status.v1"
DEFAULT_REVIEW_HISTORY_HOURS = 96.0
DEFAULT_LONG_HISTORY_HOURS = 24.0 * 35.0
DEFAULT_MAX_OUTPUT_GIB = 10.0
DEFAULT_MAX_SOURCE_READ_LOCK_SEC = 300.0
DEFAULT_FAILURE_RETRY_SEC = 60
MIN_FAILURE_RETRY_SEC = 60
SECOND_FAILURE_RETRY_SEC = 900
THIRD_FAILURE_RETRY_SEC = 3600
SUSTAINED_FAILURE_RETRY_SEC = 21600
SNAPSHOT_NAME_RE = re.compile(r"^\d{8}T\d{6}Z-[0-9a-f]{8}$")
PARTIAL_SNAPSHOT_NAME_RE = re.compile(r"^\.\d{8}T\d{6}Z-[0-9a-f]{8}\.partial$")
DATABASE_BUDGET_SHARES = {
    "signal": 0.08,
    "paper": 0.68,
    "raw": 0.18,
    "kline": 0.06,
}
DYNAMIC_BUDGET_HEADROOM_NUMERATOR = 5
DYNAMIC_BUDGET_HEADROOM_DENOMINATOR = 4


def recent(
    *time_columns: str,
    horizon: str = "review",
    required: bool = False,
    indexed_epoch_seconds_anchor: str | None = None,
    epoch_seconds_columns: tuple[str, ...] = (),
) -> dict[str, Any]:
    if indexed_epoch_seconds_anchor and indexed_epoch_seconds_anchor not in time_columns:
        raise ValueError("indexed epoch-seconds anchor must be a registered time column")
    rule = {
        "mode": "recent",
        "time_columns": tuple(time_columns),
        "horizon": horizon,
        "required": required,
    }
    if indexed_epoch_seconds_anchor:
        rule["indexed_epoch_seconds_anchor"] = indexed_epoch_seconds_anchor
        rule["epoch_seconds_columns"] = tuple(epoch_seconds_columns)
    return rule


def full(*, required: bool = False) -> dict[str, Any]:
    return {
        "mode": "full",
        "required": required,
        "time_semantics": "timeless_reference",
    }


def through_upper(*time_columns: str, required: bool = False) -> dict[str, Any]:
    return {
        "mode": "through_upper",
        "time_columns": tuple(time_columns),
        "required": required,
        "time_semantics": "event_time",
    }


DATABASE_SPECS = {
    "signal": {
        "filename": "signal.db",
        "required_tables": ("premium_signals",),
        "watermarks": {"premium_signals": ("id", "source_message_ts", "timestamp", "receive_ts")},
        "tables": {
            "premium_signals": recent(
                "timestamp", "source_message_ts", "receive_ts", "created_at",
                horizon="long", required=True
            ),
            "token_motion_events": recent("ts_ms", horizon="long"),
            "tokens": through_upper("first_seen_at", "created_at", "decision_timestamp"),
        },
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
            "candidate_shadow_virtual_trades": (
                "signal_id", "signal_ts", "observed_at", "exit_ts"
            ),
            "paper_decision_events": ("id", "event_ts", "created_at"),
            "a_class_decision_events": ("id", "event_ts", "created_at"),
            "a_class_mode_runtime_state": ("id", "updated_at", "evaluated_at", "created_at"),
            "paper_trades": (
                "id", "signal_ts", "entry_ts", "entry_time", "exit_ts", "created_at"
            ),
            "opportunity_events": ("id", "event_ts", "created_at"),
        },
        "tables": {
            "candidate_shadow_observations": recent(
                "observed_at",
                "signal_ts",
                required=True,
                indexed_epoch_seconds_anchor="observed_at",
                epoch_seconds_columns=("observed_at",),
            ),
            "candidate_shadow_virtual_trades": recent(
                "observed_at",
                "signal_ts",
                "entry_ts",
                "exit_ts",
                required=True,
                indexed_epoch_seconds_anchor="observed_at",
                epoch_seconds_columns=("observed_at",),
            ),
            "paper_decision_events": recent(
                "event_ts", "signal_ts", "created_at", required=True
            ),
            "a_class_decision_events": recent(
                "event_ts", "signal_ts", "opportunity_ts", "created_at", required=True
            ),
            "a_class_mode_runtime_state": through_upper(
                "updated_at", "created_at", "last_breach_ts", required=True
            ),
            "paper_trades": through_upper(
                "entry_ts", "entry_time", "signal_ts", "exit_ts", "last_ath_ts",
                "trigger_ts", "armed_ts", "rolling_low_ts",
                "stage3_qualifying_exit_ts", "created_at", required=True
            ),
            "opportunity_events": recent(
                "event_ts", "raw_signal_ts", "opportunity_ts", "created_at", "updated_at",
                required=True
            ),
            "canonical_trade_ledger": through_upper(
                "entry_ts", "exit_ts", "created_at", "updated_at"
            ),
            "paper_missed_signal_attribution": recent(
                "signal_ts", "created_event_ts", "baseline_ts", "first_tradable_ts",
                "created_at", "updated_at", horizon="long"
            ),
            "opportunity_event_path_samples": recent(
                "sample_ts", "created_at", "updated_at", horizon="long"
            ),
            "paper_trade_path_samples": recent("sample_ts", "created_at", horizon="long"),
            "candidate_shadow_kline_fetch_attempts": through_upper("last_attempt_at"),
            "lotto_not_ath_watch_shadow_snapshots": recent(
                "captured_at", "signal_ts", "snapshot_ts", "first_seen_ts", "created_at",
                horizon="long"
            ),
            "external_alpha_snapshots": recent("captured_at", "created_at", horizon="long"),
            "external_alpha_state": through_upper("updated_at", "last_seen_ts", "first_seen_ts"),
            "external_alpha_health": through_upper("updated_at", "last_run_ts", "last_success_ts"),
        },
    },
    "raw": {
        "filename": "raw.db",
        "required_tables": ("raw_signal_outcomes",),
        "watermarks": {"raw_signal_outcomes": ("id", "signal_id", "signal_ts", "updated_at")},
        "tables": {
            "raw_signal_outcomes": recent(
                "signal_ts", "matured_at_ts", "baseline_ts", "first_bar_ts",
                "created_at", "updated_at", horizon="long", required=True
            ),
            "raw_signal_observations": recent(
                "signal_ts", "matured_at_ts", "first_bar_ts", "created_at", "updated_at",
                horizon="long"
            ),
            "raw_price_bars_1m": recent(
                "timestamp", "first_trade_ts", "last_trade_ts", "fetched_at", "created_at",
                "updated_at", horizon="long"
            ),
            "raw_path_observer_provider_state": through_upper("updated_at"),
        },
    },
    "kline": {
        "filename": "kline.db",
        "required_tables": ("kline_1m",),
        "watermarks": {"kline_1m": ("timestamp", "fetched_at", "updated_at")},
        "tables": {
            "kline_1m": recent("timestamp", "fetched_at", horizon="long", required=True),
            "pool_mapping": through_upper("fetched_at"),
            "helius_trades": recent("block_time", "ingested_at", horizon="long"),
            "history_backfill_cursor": through_upper(
                "last_backfill_at", "newest_block_time", "oldest_block_time"
            ),
        },
    },
}
SNAPSHOT_DATABASE_FILENAMES = {
    spec["filename"] for spec in DATABASE_SPECS.values()
}
CANDIDATE_OBSERVATION_TABLE = "candidate_shadow_observations"
CANDIDATE_OBSERVATION_ROW_TABLE = "__a3_candidate_shadow_observation_rows"
CANDIDATE_OBSERVATION_CONTEXT_TABLE = "__a3_candidate_shadow_observation_contexts"
CANDIDATE_OBSERVATION_PROJECTION_REQUIRED_COLUMNS = {
    "id",
    "signal_id",
    "candidate_id",
    "payload_json",
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


def read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def bounded_error_text(exc: BaseException, limit: int = 4096) -> str:
    text = f"{type(exc).__name__}:{exc}"
    return text if len(text) <= limit else f"{text[:limit]}…"


class ConcurrentSnapshotError(RuntimeError):
    """Bounded multi-database failure with public-safe component details."""

    def __init__(self, errors: dict[str, dict[str, Any]]):
        self.errors = {
            str(name): {
                "error_code": str((details or {}).get("error_code") or "snapshot_component_failed"),
                "error_type": str((details or {}).get("error_type") or "Exception"),
                "stage": str((details or {}).get("stage") or "unknown"),
            }
            for name, details in sorted((errors or {}).items())
        }
        summary = ",".join(
            f"{details['error_code']}:{name}:{details['stage']}"
            for name, details in self.errors.items()
        )
        super().__init__(f"concurrent evaluator snapshot failed: {summary}")


def snapshot_component_failure_code(exc: BaseException) -> str:
    text = str(exc)
    known = (
        "source_read_lock_budget_exceeded",
        "selective_snapshot_source_query_plan_not_indexed",
        "selective_snapshot_source_index_missing",
        "selective_snapshot_epoch_seconds_type_invalid",
        "selective_snapshot_index_anchor_missing",
        "selective_snapshot_index_anchor_unit_missing",
        "snapshot_source_read_lock_timeout",
        "selective snapshot exceeded database budget",
        "snapshot source inspection failed",
        "snapshot missing required tables",
        "snapshot missing required watermarks",
        "candidate_observation_payload_projection_semantic_mismatch",
    )
    for marker in known:
        if marker in text:
            return marker.replace(" ", "_").replace("-", "_")
    return type(exc).__name__


def snapshot_failure_details(exc: BaseException) -> dict[str, dict[str, str]]:
    if isinstance(exc, ConcurrentSnapshotError):
        return {name: dict(details) for name, details in exc.errors.items()}
    return {
        "worker": {
            "error_code": snapshot_component_failure_code(exc),
            "error_type": type(exc).__name__,
            "stage": "run_snapshot_once",
        }
    }


def snapshot_next_attempt_delay_sec(
    status: dict[str, Any],
    *,
    interval_sec: int,
    failure_retry_sec: int,
) -> int:
    success_interval = max(1, int(interval_sec))
    if status.get("accepted") is True:
        return success_interval
    if status.get("last_failure_code") == "evaluator_snapshot_lock_held":
        return max(SUSTAINED_FAILURE_RETRY_SEC, success_interval)
    first_retry = max(MIN_FAILURE_RETRY_SEC, int(failure_retry_sec))
    try:
        consecutive_failures = max(1, int(status.get("consecutive_failure_count") or 1))
    except (TypeError, ValueError):
        consecutive_failures = 1
    if consecutive_failures == 1:
        return first_retry
    if consecutive_failures == 2:
        return max(first_retry, SECOND_FAILURE_RETRY_SEC)
    if consecutive_failures == 3:
        return max(first_retry, THIRD_FAILURE_RETRY_SEC)
    return max(first_retry, SUSTAINED_FAILURE_RETRY_SEC, success_interval)


def snapshot_failure_code(exc: BaseException) -> str:
    text = str(exc)
    if isinstance(exc, ConcurrentSnapshotError):
        component_codes = {
            str(details.get("error_code") or "snapshot_component_failed")
            for details in exc.errors.values()
        }
        if len(component_codes) == 1:
            return next(iter(component_codes))
        return "concurrent_evaluator_snapshot_failed"
    known = (
        "evaluator_snapshot_lock_held",
        "source_read_lock_budget_exceeded",
        "selective_snapshot_source_index_missing",
        "selective_snapshot_epoch_seconds_type_invalid",
        "selective_snapshot_index_anchor_missing",
        "selective_snapshot_index_anchor_unit_missing",
        "snapshot_source_read_lock_timeout",
        "insufficient disk for evaluator snapshot",
        "concurrent evaluator snapshot failed",
        "cross-database snapshot acceptance failed",
        "selective snapshot exceeded bundle output cap",
        "snapshot manifest size did not converge",
    )
    for marker in known:
        if marker in text:
            return marker.replace(" ", "_").replace("-", "_")
    return type(exc).__name__


def snapshot_manifest_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    databases = manifest.get("databases") if isinstance(manifest.get("databases"), dict) else {}
    lock_durations = [
        float(report.get("source_read_lock_duration_sec") or 0.0)
        for report in databases.values()
        if isinstance(report, dict)
    ]
    paper = databases.get("paper") if isinstance(databases.get("paper"), dict) else {}
    selected = paper.get("selected_tables") if isinstance(paper.get("selected_tables"), dict) else {}
    indexed_selection = {}
    for table in (
        "candidate_shadow_observations",
        "candidate_shadow_virtual_trades",
    ):
        report = selected.get(table) if isinstance(selected.get(table), dict) else {}
        indexed_selection[table] = {
            "predicate_strategy": report.get("predicate_strategy"),
            "indexed_time_anchor": report.get("indexed_time_anchor"),
            "source_index_name": report.get("source_index_name"),
            "source_index_columns": report.get("source_index_columns") or [],
            "source_index_partial": report.get("source_index_partial"),
            "source_query_plan": report.get("source_query_plan") or [],
            "source_query_plan_uses_index": report.get("source_query_plan_uses_index"),
            "source_query_plan_uses_range_search": report.get("source_query_plan_uses_range_search"),
            "source_query_plan_full_table_scan_detected": report.get(
                "source_query_plan_full_table_scan_detected"
            ),
            "rows_copied": report.get("rows_copied"),
        }
    disk = manifest.get("disk_preflight") if isinstance(manifest.get("disk_preflight"), dict) else {}
    return {
        "schema_version": manifest.get("schema_version"),
        "snapshot_id": manifest.get("snapshot_id"),
        "snapshot_ts": manifest.get("snapshot_ts"),
        "generated_at": manifest.get("generated_at"),
        "git_commit": manifest.get("git_commit"),
        "accepted": manifest.get("accepted") is True,
        "quick_checks_passed": manifest.get("quick_checks_passed") is True,
        "required_tables_present": manifest.get("required_tables_present") is True,
        "required_watermarks_present": manifest.get("required_watermarks_present") is True,
        "cross_database_time_skew_sec": manifest.get("cross_database_time_skew_sec"),
        "cross_database_time_skew_passed": manifest.get("cross_database_time_skew_passed") is True,
        "source_read_lock_budget_passed": manifest.get("source_read_lock_budget_passed") is True,
        "max_source_read_lock_sec": manifest.get("max_source_read_lock_sec"),
        "max_source_read_lock_duration_sec": round(max(lock_durations), 6) if lock_durations else None,
        "output_size_bytes": manifest.get("output_size_bytes"),
        "output_cap_bytes": manifest.get("output_cap_bytes"),
        "output_cap_passed": manifest.get("output_cap_passed") is True,
        "disk_preflight": {
            "accepted": disk.get("accepted") is True,
            "free_bytes": disk.get("free_bytes"),
            "required_reserve_bytes": disk.get("required_reserve_bytes"),
            "estimated_free_after_bytes": disk.get("estimated_free_after_bytes"),
        },
        "indexed_selection": indexed_selection,
        "promotion_allowed": False,
    }


def atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
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


def snapshot_directory_report(path: Path, *, include_manifest: bool) -> dict[str, Any]:
    allowed = set(SNAPSHOT_DATABASE_FILENAMES)
    if include_manifest:
        allowed.add("manifest.json")
    entries = list(path.iterdir()) if path.is_dir() else []
    files = [item for item in entries if item.is_file()]
    actual_names = {item.name for item in files}
    unexpected = sorted(
        item.name
        for item in entries
        if not item.is_file() or item.name not in allowed
    )
    missing = sorted(allowed - actual_names)
    sizes = {item.name: int(item.stat().st_size) for item in files}
    return {
        "files": sizes,
        "total_size_bytes": sum(sizes.values()),
        "unexpected_entries": unexpected,
        "missing_entries": missing,
        "accepted": not unexpected and not missing,
    }


def write_bounded_manifest(
    partial_dir: Path,
    manifest: dict[str, Any],
    *,
    output_cap_bytes: int,
) -> dict[str, Any]:
    manifest_path = partial_dir / "manifest.json"
    manifest["manifest_size_bytes"] = 0
    manifest["output_size_bytes"] = int(manifest.get("database_payload_size_bytes") or 0)
    for _attempt in range(8):
        atomic_json(manifest_path, manifest)
        directory = snapshot_directory_report(partial_dir, include_manifest=True)
        if not directory["accepted"]:
            raise RuntimeError(f"snapshot bundle contains unexpected files: {directory}")
        manifest_size = int(directory["files"]["manifest.json"])
        total_size = int(directory["total_size_bytes"])
        if total_size > int(output_cap_bytes):
            raise RuntimeError(
                f"selective snapshot exceeded bundle output cap: {total_size}>{output_cap_bytes}"
            )
        if (
            int(manifest.get("manifest_size_bytes") or 0) == manifest_size
            and int(manifest.get("output_size_bytes") or 0) == total_size
        ):
            manifest["output_cap_passed"] = True
            return directory
        manifest["manifest_size_bytes"] = manifest_size
        manifest["output_size_bytes"] = total_size
        manifest["output_cap_passed"] = True
    raise RuntimeError("snapshot manifest size did not converge")


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


def readonly_connection(path: Path, *, busy_timeout_ms: int = 30000) -> sqlite3.Connection:
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro"
    timeout_sec = max(0.001, float(busy_timeout_ms) / 1000.0)
    connection = sqlite3.connect(uri, uri=True, timeout=timeout_sec)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute(f"PRAGMA busy_timeout={max(0, int(busy_timeout_ms))}")
    return connection


def _schema_prefix(schema: str) -> str:
    if schema not in {"main", "src"}:
        raise ValueError(f"unsupported SQLite schema: {schema}")
    return schema


def database_metadata(
    connection: sqlite3.Connection,
    spec: dict[str, Any],
    *,
    schema: str = "main",
    include_views: bool = False,
) -> dict[str, Any]:
    schema = _schema_prefix(schema)
    object_types = "('table','view')" if include_views else "('table')"
    table_rows = connection.execute(
        f"SELECT name, sql FROM {schema}.sqlite_master "
        f"WHERE type IN {object_types} ORDER BY name"
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
            for row in connection.execute(
                f"PRAGMA {schema}.table_info({quote_identifier(table)})"
            )
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
            f"SELECT {expressions} FROM {schema}.{quote_identifier(table)}"
        ).fetchone()
        watermarks[table] = {column: row[column] for column in selected}
    schema_text = "\n".join(f"{name}\n{table_sql[name]}" for name in sorted(table_sql))
    return {
        "schema_version": int(connection.execute(f"PRAGMA {schema}.schema_version").fetchone()[0]),
        "user_version": int(connection.execute(f"PRAGMA {schema}.user_version").fetchone()[0]),
        "application_id": int(connection.execute(f"PRAGMA {schema}.application_id").fetchone()[0]),
        "page_size": int(connection.execute(f"PRAGMA {schema}.page_size").fetchone()[0]),
        "page_count": int(connection.execute(f"PRAGMA {schema}.page_count").fetchone()[0]),
        "freelist_count": int(connection.execute(f"PRAGMA {schema}.freelist_count").fetchone()[0]),
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


def inspect_source_page_reports(
    source_paths: dict[str, Path],
    *,
    busy_timeout_ms: int = 30000,
) -> dict[str, dict[str, int]]:
    reports = {}
    for name, source in source_paths.items():
        if not source.is_file():
            raise FileNotFoundError(source)
        connection = readonly_connection(source, busy_timeout_ms=busy_timeout_ms)
        try:
            try:
                reports[name] = source_page_stats(connection, source)
            except sqlite3.Error as exc:
                raise RuntimeError(
                    f"snapshot source inspection failed:{name}:{type(exc).__name__}:{exc}"
                ) from exc
        finally:
            connection.close()
    return reports


def normalized_timestamp_sql(column: str) -> str:
    value = quote_identifier(column)
    text_value = f"TRIM(CAST({value} AS TEXT))"
    numeric = f"CAST({value} AS REAL)"
    normalized_numeric = (
        f"CASE WHEN ABS({numeric}) >= 100000000000 THEN {numeric} / 1000.0 ELSE {numeric} END"
    )
    return (
        "CASE "
        f"WHEN {value} IS NULL THEN NULL "
        f"WHEN typeof({value}) IN ('integer','real') THEN {normalized_numeric} "
        f"WHEN {text_value} != '' AND {text_value} NOT GLOB '*[^0-9.-]*' "
        f"THEN {normalized_numeric} "
        f"ELSE CAST(strftime('%s', {value}) AS REAL) END"
    )


def declared_numeric_timestamp_type(declared_type: str) -> bool:
    normalized = str(declared_type or "").strip().upper()
    return bool(
        "INT" in normalized
        or any(token in normalized for token in ("REAL", "FLOA", "DOUB", "NUM"))
    )


def source_index_for_column(
    connection: sqlite3.Connection,
    table: str,
    column: str,
) -> str | None:
    candidates: list[tuple[int, str]] = []
    for row in connection.execute(
        f"PRAGMA src.index_list({quote_identifier(table)})"
    ):
        if len(row) > 4 and int(row[4] or 0):
            continue
        name = str(row[1])
        columns = [
            str(index_row[2])
            for index_row in connection.execute(
                f"PRAGMA src.index_info({quote_identifier(name)})"
            )
        ]
        if columns and columns[0] == column:
            candidates.append((len(columns), name))
    if not candidates:
        return None
    return sorted(candidates)[0][1]


def source_table_reference(table: str, selection: dict[str, Any]) -> str:
    reference = f"src.{quote_identifier(table)}"
    source_index_name = selection.get("source_index_name")
    if source_index_name:
        reference += f" INDEXED BY {quote_identifier(str(source_index_name))}"
    return reference


def source_index_columns(
    connection: sqlite3.Connection,
    index_name: str | None,
) -> list[str]:
    if not index_name:
        return []
    return [
        str(row[2])
        for row in connection.execute(
            f"PRAGMA src.index_info({quote_identifier(str(index_name))})"
        )
    ]


def source_query_plan_evidence(
    connection: sqlite3.Connection,
    table: str,
    selection: dict[str, Any],
) -> dict[str, Any]:
    index_name = selection.get("source_index_name")
    indexed_anchor = selection.get("indexed_time_anchor")
    if not index_name or not indexed_anchor:
        return {
            "required": False,
            "details": [],
            "uses_declared_index": None,
            "uses_range_search": None,
            "full_table_scan_detected": None,
        }
    query = (
        f"EXPLAIN QUERY PLAN SELECT {quote_identifier(str(indexed_anchor))} "
        f"FROM {source_table_reference(table, selection)} "
        f"WHERE {selection['predicate_sql']} LIMIT 1"
    )
    rows = connection.execute(query, selection["parameters"]).fetchall()
    details = [str(row[3]) for row in rows]
    index_token = str(index_name).lower()
    table_token = str(table).lower()
    uses_declared_index = any(index_token in detail.lower() for detail in details)
    uses_range_search = any(
        "search" in detail.lower() and index_token in detail.lower()
        for detail in details
    )
    full_table_scan_detected = any(
        "scan" in detail.lower()
        and table_token in detail.lower()
        and index_token not in detail.lower()
        for detail in details
    )
    return {
        "required": True,
        "details": details,
        "uses_declared_index": uses_declared_index,
        "uses_range_search": uses_range_search,
        "full_table_scan_detected": full_table_scan_detected,
    }


def selection_for_table(
    connection: sqlite3.Connection,
    table: str,
    rule: dict[str, Any],
    *,
    review_lower_epoch: float,
    long_lower_epoch: float,
    upper_epoch: float,
) -> dict[str, Any]:
    column_rows = list(
        connection.execute(f"PRAGMA src.table_info({quote_identifier(table)})")
    )
    columns = {str(row["name"]) for row in column_rows}
    declared_types = {str(row["name"]): str(row["type"] or "") for row in column_rows}
    if rule["mode"] == "full":
        return {
            "mode": "full",
            "predicate_sql": "1=1",
            "parameters": [],
            "time_column": None,
            "lower_epoch": None,
            "upper_epoch": upper_epoch,
            "future_bound_enforced": False,
            "time_semantics": rule.get("time_semantics"),
        }
    time_columns = [name for name in rule.get("time_columns", ()) if name in columns]
    if not time_columns:
        raise RuntimeError(f"selective_snapshot_time_column_missing:{table}")
    indexed_anchor = rule.get("indexed_epoch_seconds_anchor")
    epoch_seconds_columns = {
        str(name) for name in rule.get("epoch_seconds_columns", ()) if name in columns
    }
    source_index_name = None
    if indexed_anchor:
        indexed_anchor = str(indexed_anchor)
        if indexed_anchor not in time_columns:
            raise RuntimeError(
                f"selective_snapshot_index_anchor_missing:{table}:{indexed_anchor}"
            )
        if indexed_anchor not in epoch_seconds_columns:
            raise RuntimeError(
                f"selective_snapshot_index_anchor_unit_missing:{table}:{indexed_anchor}"
            )
        invalid_types = sorted(
            name
            for name in epoch_seconds_columns
            if not declared_numeric_timestamp_type(declared_types.get(name, ""))
        )
        if invalid_types:
            raise RuntimeError(
                f"selective_snapshot_epoch_seconds_type_invalid:{table}:"
                f"{','.join(invalid_types)}"
            )
        source_index_name = source_index_for_column(connection, table, indexed_anchor)
        if not source_index_name:
            raise RuntimeError(
                f"selective_snapshot_source_index_missing:{table}:{indexed_anchor}"
            )
        source_index_column_names = source_index_columns(connection, source_index_name)
        if not source_index_column_names or source_index_column_names[0] != indexed_anchor:
            raise RuntimeError(
                f"selective_snapshot_source_index_column_mismatch:"
                f"{table}:{source_index_name}:{indexed_anchor}"
            )
    else:
        source_index_column_names = []
    normalized_columns = [
        quote_identifier(column)
        if column in epoch_seconds_columns
        else normalized_timestamp_sql(column)
        for column in time_columns
    ]
    if indexed_anchor:
        anchor = quote_identifier(indexed_anchor)
    else:
        anchor = (
            normalized_columns[0]
            if len(normalized_columns) == 1
            else "COALESCE(" + ", ".join(normalized_columns) + ")"
        )
    upper_checks = " AND ".join(
        f"({normalized} IS NULL OR {normalized} <= ?)"
        for normalized in normalized_columns
    )
    if rule["mode"] == "through_upper":
        return {
            "mode": "through_upper",
            "predicate_sql": f"{anchor} IS NOT NULL AND {upper_checks}",
            "parameters": [float(upper_epoch)] * len(normalized_columns),
            "time_column": time_columns[0],
            "time_columns": time_columns,
            "upper_bound_columns": time_columns,
            "lower_epoch": None,
            "upper_epoch": float(upper_epoch),
            "future_bound_enforced": True,
            "time_semantics": rule.get("time_semantics", "event_time"),
            "predicate_strategy": (
                "indexed_epoch_seconds" if source_index_name else "normalized_timestamp"
            ),
            "indexed_time_anchor": indexed_anchor,
            "source_index_name": source_index_name,
            "source_index_columns": source_index_column_names,
            "source_index_partial": False if source_index_name else None,
        }
    lower_epoch = long_lower_epoch if rule.get("horizon") == "long" else review_lower_epoch
    return {
        "mode": "recent",
        "predicate_sql": f"{anchor} IS NOT NULL AND {anchor} >= ? AND {upper_checks}",
        "parameters": [float(lower_epoch)] + [float(upper_epoch)] * len(normalized_columns),
        "time_column": time_columns[0],
        "time_columns": time_columns,
        "upper_bound_columns": time_columns,
        "lower_epoch": float(lower_epoch),
        "upper_epoch": float(upper_epoch),
        "future_bound_enforced": True,
        "time_semantics": rule.get("time_semantics", "event_time"),
        "predicate_strategy": (
            "indexed_epoch_seconds" if source_index_name else "normalized_timestamp"
        ),
        "indexed_time_anchor": indexed_anchor,
        "source_index_name": source_index_name,
        "source_index_columns": source_index_column_names,
        "source_index_partial": False if source_index_name else None,
    }


def canonical_json_object(raw: Any, *, table: str, signal_id: Any) -> tuple[dict[str, Any], str]:
    try:
        value = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"snapshot_payload_json_invalid:{table}:signal_id={signal_id}"
        ) from exc
    if not isinstance(value, dict):
        raise RuntimeError(
            f"snapshot_payload_json_not_object:{table}:signal_id={signal_id}"
        )
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return value, canonical


def update_payload_semantic_digest(
    digest: Any,
    *,
    signal_id: Any,
    candidate_id: Any,
    payload_json: str,
) -> None:
    digest.update(str(signal_id).encode("utf-8"))
    digest.update(b"\x00")
    digest.update(str(candidate_id).encode("utf-8"))
    digest.update(b"\x00")
    digest.update(payload_json.encode("utf-8"))
    digest.update(b"\n")


def column_definition(row: sqlite3.Row) -> str:
    name = str(row["name"])
    parts = [quote_identifier(name)]
    declared_type = str(row["type"] or "").strip()
    if declared_type:
        parts.append(declared_type)
    if int(row["pk"] or 0):
        parts.append("PRIMARY KEY")
    if int(row["notnull"] or 0):
        parts.append("NOT NULL")
    if row["dflt_value"] is not None:
        parts.extend(("DEFAULT", str(row["dflt_value"])))
    return " ".join(parts)


def candidate_observation_projection_supported(
    connection: sqlite3.Connection,
) -> tuple[bool, list[sqlite3.Row]]:
    columns = list(
        connection.execute(
            f"PRAGMA src.table_info({quote_identifier(CANDIDATE_OBSERVATION_TABLE)})"
        )
    )
    names = {str(row["name"]) for row in columns}
    id_columns = [row for row in columns if str(row["name"]) == "id"]
    id_is_rowid_alias = bool(
        len(id_columns) == 1
        and str(id_columns[0]["type"] or "").strip().upper() == "INTEGER"
        and int(id_columns[0]["pk"] or 0) == 1
    )
    return (
        CANDIDATE_OBSERVATION_PROJECTION_REQUIRED_COLUMNS.issubset(names)
        and id_is_rowid_alias,
        columns,
    )


def candidate_payload_expression() -> str:
    common = f"c.{quote_identifier('common_payload_json')}"
    delta = f"r.{quote_identifier('payload_delta_json')}"
    return (
        "CASE "
        f"WHEN {common}='{{}}' THEN {delta} "
        f"WHEN {delta}='{{}}' THEN {common} "
        f"ELSE substr({common},1,length({common})-1) || ',' || substr({delta},2) "
        "END"
    )


def create_candidate_observation_projection(
    connection: sqlite3.Connection,
    source_columns: list[sqlite3.Row],
) -> list[str]:
    source_names = [str(row["name"]) for row in source_columns]
    for internal in (
        CANDIDATE_OBSERVATION_ROW_TABLE,
        CANDIDATE_OBSERVATION_CONTEXT_TABLE,
        "payload_delta_json",
        "common_payload_json",
    ):
        if internal in source_names:
            raise RuntimeError(f"candidate_observation_projection_name_collision:{internal}")
    row_columns = [row for row in source_columns if str(row["name"]) != "payload_json"]
    definitions = [column_definition(row) for row in row_columns]
    definitions.extend(
        (
            f"{quote_identifier('context_id')} INTEGER NOT NULL",
            f"{quote_identifier('payload_delta_json')} TEXT NOT NULL",
            f"UNIQUE({quote_identifier('signal_id')},{quote_identifier('candidate_id')})",
        )
    )
    connection.execute(
        f"CREATE TABLE {quote_identifier(CANDIDATE_OBSERVATION_ROW_TABLE)} "
        f"({', '.join(definitions)})"
    )
    connection.execute(
        f"CREATE TABLE {quote_identifier(CANDIDATE_OBSERVATION_CONTEXT_TABLE)} ("
        f"{quote_identifier('context_id')} INTEGER PRIMARY KEY, "
        f"{quote_identifier('signal_id')} TEXT NOT NULL, "
        f"{quote_identifier('common_payload_json')} TEXT NOT NULL)"
    )
    projected_columns = []
    for name in source_names:
        if name == "payload_json":
            projected_columns.append(
                f"{candidate_payload_expression()} AS {quote_identifier(name)}"
            )
        else:
            projected_columns.append(f"r.{quote_identifier(name)}")
    projected_columns.append(f"r.{quote_identifier('id')} AS {quote_identifier('rowid')}")
    connection.execute(
        f"CREATE VIEW {quote_identifier(CANDIDATE_OBSERVATION_TABLE)} AS "
        f"SELECT {', '.join(projected_columns)} "
        f"FROM {quote_identifier(CANDIDATE_OBSERVATION_ROW_TABLE)} AS r "
        f"JOIN {quote_identifier(CANDIDATE_OBSERVATION_CONTEXT_TABLE)} AS c "
        f"ON c.{quote_identifier('context_id')}=r.{quote_identifier('context_id')}"
    )
    return source_names


def copy_candidate_observation_projection(
    connection: sqlite3.Connection,
    source_columns: list[sqlite3.Row],
    selection: dict[str, Any],
) -> dict[str, Any]:
    source_names = create_candidate_observation_projection(connection, source_columns)
    selected_columns = ", ".join(quote_identifier(name) for name in source_names)
    cursor = connection.execute(
        f"SELECT {selected_columns} "
        f"FROM {source_table_reference(CANDIDATE_OBSERVATION_TABLE, selection)} "
        f"WHERE {selection['predicate_sql']} "
        f"ORDER BY {quote_identifier('signal_id')}, {quote_identifier('candidate_id')}",
        selection["parameters"],
    )
    source_digest = hashlib.sha256()
    rows_copied = 0
    context_count = 0
    source_payload_bytes = 0
    projected_payload_bytes = 0
    row_insert_columns = [
        name for name in source_names if name != "payload_json"
    ] + ["context_id", "payload_delta_json"]
    row_insert_sql = (
        f"INSERT INTO {quote_identifier(CANDIDATE_OBSERVATION_ROW_TABLE)} "
        f"({', '.join(quote_identifier(name) for name in row_insert_columns)}) "
        f"VALUES ({', '.join('?' for _ in row_insert_columns)})"
    )
    context_insert_sql = (
        f"INSERT INTO {quote_identifier(CANDIDATE_OBSERVATION_CONTEXT_TABLE)} "
        f"({quote_identifier('context_id')},{quote_identifier('signal_id')},"
        f"{quote_identifier('common_payload_json')}) VALUES (?,?,?)"
    )
    for signal_id, grouped in itertools.groupby(cursor, key=lambda row: row["signal_id"]):
        rows = list(grouped)
        payloads: list[dict[str, Any]] = []
        for row in rows:
            payload, canonical = canonical_json_object(
                row["payload_json"],
                table=CANDIDATE_OBSERVATION_TABLE,
                signal_id=signal_id,
            )
            payloads.append(payload)
            source_payload_bytes += len(str(row["payload_json"]).encode("utf-8"))
            update_payload_semantic_digest(
                source_digest,
                signal_id=signal_id,
                candidate_id=row["candidate_id"],
                payload_json=canonical,
            )
        common = dict(payloads[0])
        common_encodings = {
            key: json.dumps(value, sort_keys=True, separators=(",", ":"))
            for key, value in common.items()
        }
        for payload in payloads[1:]:
            for key in list(common):
                if (
                    key not in payload
                    or json.dumps(
                        payload[key], sort_keys=True, separators=(",", ":")
                    )
                    != common_encodings[key]
                ):
                    del common[key]
                    del common_encodings[key]
        common_json = json.dumps(common, sort_keys=True, separators=(",", ":"))
        context_count += 1
        connection.execute(
            context_insert_sql,
            (context_count, str(signal_id), common_json),
        )
        projected_payload_bytes += len(common_json.encode("utf-8"))
        insert_rows = []
        for row, payload in zip(rows, payloads):
            delta = {key: value for key, value in payload.items() if key not in common}
            delta_json = json.dumps(delta, sort_keys=True, separators=(",", ":"))
            projected_payload_bytes += len(delta_json.encode("utf-8"))
            values = [row[name] for name in source_names if name != "payload_json"]
            values.extend((context_count, delta_json))
            insert_rows.append(values)
        connection.executemany(row_insert_sql, insert_rows)
        rows_copied += len(insert_rows)
    return {
        "schema_version": PAYLOAD_PROJECTION_SCHEMA_VERSION,
        "applied": True,
        "logical_object_type": "view",
        "row_storage_table": CANDIDATE_OBSERVATION_ROW_TABLE,
        "context_storage_table": CANDIDATE_OBSERVATION_CONTEXT_TABLE,
        "rows_copied": rows_copied,
        "context_rows": context_count,
        "source_payload_bytes": source_payload_bytes,
        "projected_payload_bytes": projected_payload_bytes,
        "payload_storage_ratio": (
            projected_payload_bytes / source_payload_bytes
            if source_payload_bytes > 0
            else 0.0
        ),
        "source_payload_semantic_sha256": source_digest.hexdigest(),
        "payload_reconstruction": "common_object_plus_disjoint_per_candidate_delta",
        "unknown_payload_keys_preserved": True,
        "missing_and_null_keys_preserved": True,
    }


def candidate_projection_indexes() -> list[tuple[str, str]]:
    table = quote_identifier(CANDIDATE_OBSERVATION_ROW_TABLE)
    return [
        (
            "idx_a3_candidate_shadow_obs_signal",
            f"CREATE INDEX idx_a3_candidate_shadow_obs_signal ON {table}(signal_id)",
        ),
        (
            "idx_a3_candidate_shadow_obs_candidate",
            f"CREATE INDEX idx_a3_candidate_shadow_obs_candidate "
            f"ON {table}(candidate_id, observed_at)",
        ),
        (
            "idx_a3_candidate_shadow_obs_observed",
            f"CREATE INDEX idx_a3_candidate_shadow_obs_observed ON {table}(observed_at)",
        ),
    ]


def verify_candidate_observation_projection(
    connection: sqlite3.Connection,
    report: dict[str, Any],
) -> None:
    projection = report.get("storage_projection") or {}
    if projection.get("applied") is not True:
        return
    destination_digest = hashlib.sha256()
    rows_verified = 0
    cursor = connection.execute(
        f"SELECT signal_id, candidate_id, payload_json "
        f"FROM {quote_identifier(CANDIDATE_OBSERVATION_TABLE)} "
        f"ORDER BY signal_id, candidate_id"
    )
    for row in cursor:
        _payload, canonical = canonical_json_object(
            row["payload_json"],
            table=CANDIDATE_OBSERVATION_TABLE,
            signal_id=row["signal_id"],
        )
        update_payload_semantic_digest(
            destination_digest,
            signal_id=row["signal_id"],
            candidate_id=row["candidate_id"],
            payload_json=canonical,
        )
        rows_verified += 1
    projection["destination_payload_semantic_sha256"] = destination_digest.hexdigest()
    projection["semantic_rows_verified"] = rows_verified
    projection["payload_semantics_preserved"] = bool(
        rows_verified == int(report.get("rows_copied") or 0)
        and destination_digest.hexdigest()
        == projection.get("source_payload_semantic_sha256")
    )
    if projection["payload_semantics_preserved"] is not True:
        raise RuntimeError("candidate_observation_payload_projection_semantic_mismatch")


def create_deferred_indexes(
    connection: sqlite3.Connection,
    deferred_indexes: list[tuple[str, str, str]],
    table_reports: dict[str, dict[str, Any]],
) -> None:
    for table, name, sql in deferred_indexes:
        connection.execute(sql)
        table_reports[table].setdefault("indexes_created", []).append(name)


def copy_selected_tables(
    connection: sqlite3.Connection,
    spec: dict[str, Any],
    *,
    review_lower_epoch: float,
    long_lower_epoch: float,
    upper_epoch: float,
    progress: dict[str, str] | None = None,
) -> tuple[
    dict[str, dict[str, Any]],
    list[dict[str, str]],
    list[tuple[str, str, str]],
]:
    source_rows = connection.execute(
        "SELECT name, sql FROM src.sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    source_table_sql = {str(row["name"]): str(row["sql"] or "") for row in source_rows}
    source_tables = set(source_table_sql)
    table_reports: dict[str, dict[str, Any]] = {}
    deferred_indexes: list[tuple[str, str, str]] = []
    for table, rule in spec["tables"].items():
        if progress is not None:
            progress["stage"] = f"copy_table:{table}"
        required = bool(rule.get("required"))
        if table not in source_tables:
            if required:
                raise RuntimeError(f"snapshot missing required tables: {table}")
            table_reports[table] = {
                "included": False,
                "required": False,
                "reason": "optional_source_table_missing",
            }
            continue
        create_sql = source_table_sql[table]
        if not create_sql:
            raise RuntimeError(f"snapshot source table schema missing: {table}")
        selection = selection_for_table(
            connection,
            table,
            rule,
            review_lower_epoch=review_lower_epoch,
            long_lower_epoch=long_lower_epoch,
            upper_epoch=upper_epoch,
        )
        query_plan = source_query_plan_evidence(connection, table, selection)
        if query_plan["required"] and (
            query_plan["uses_declared_index"] is not True
            or query_plan["uses_range_search"] is not True
            or query_plan["full_table_scan_detected"] is True
        ):
            raise RuntimeError(
                f"selective_snapshot_source_query_plan_not_indexed:{table}:"
                f"{selection.get('source_index_name')}:{query_plan['details']}"
            )
        projection_supported = False
        source_columns: list[sqlite3.Row] = []
        if table == CANDIDATE_OBSERVATION_TABLE:
            projection_supported, source_columns = candidate_observation_projection_supported(
                connection
            )
        storage_projection: dict[str, Any]
        if projection_supported:
            storage_projection = copy_candidate_observation_projection(
                connection,
                source_columns,
                selection,
            )
            copied_rows = int(storage_projection["rows_copied"])
            for name, sql in candidate_projection_indexes():
                deferred_indexes.append((table, name, sql))
        else:
            connection.execute(create_sql)
            connection.execute(
                f"INSERT INTO {quote_identifier(table)} "
                f"SELECT * FROM {source_table_reference(table, selection)} "
                f"WHERE {selection['predicate_sql']}",
                selection["parameters"],
            )
            copied_rows = int(connection.execute("SELECT changes()").fetchone()[0])
            storage_projection = {
                "schema_version": PAYLOAD_PROJECTION_SCHEMA_VERSION,
                "applied": False,
                "reason": "source_schema_not_projection_compatible",
                "payload_semantics_preserved": True,
            }
        table_reports[table] = {
            "included": True,
            "required": required,
            "rows_copied": copied_rows,
            "selection_mode": selection["mode"],
            "time_column": selection["time_column"],
            "time_columns": selection.get("time_columns") or [],
            "upper_bound_columns": selection.get("upper_bound_columns") or [],
            "lower_epoch": selection["lower_epoch"],
            "upper_epoch": selection["upper_epoch"],
            "future_bound_enforced": selection["future_bound_enforced"],
            "time_semantics": selection["time_semantics"],
            "predicate_strategy": selection.get("predicate_strategy"),
            "indexed_time_anchor": selection.get("indexed_time_anchor"),
            "source_index_name": selection.get("source_index_name"),
            "source_index_columns": selection.get("source_index_columns") or [],
            "source_index_partial": selection.get("source_index_partial"),
            "source_query_plan": query_plan["details"],
            "source_query_plan_uses_index": query_plan["uses_declared_index"],
            "source_query_plan_uses_range_search": query_plan["uses_range_search"],
            "source_query_plan_full_table_scan_detected": query_plan["full_table_scan_detected"],
            "horizon": rule.get("horizon") if selection["mode"] == "recent" else None,
            "storage_projection": storage_projection,
            "indexes_created": [],
        }
    for table, report in table_reports.items():
        if not report.get("included"):
            continue
        if (report.get("storage_projection") or {}).get("applied") is True:
            continue
        indexes = connection.execute(
            "SELECT name, sql FROM src.sqlite_master "
            "WHERE type='index' AND tbl_name=? AND sql IS NOT NULL ORDER BY name",
            (table,),
        ).fetchall()
        for index in indexes:
            deferred_indexes.append(
                (table, str(index["name"]), str(index["sql"]))
            )
    omitted = [
        {
            "table": table,
            "reason": "not_required_by_evaluator_selection_contract",
        }
        for table in sorted(source_tables - set(spec["tables"]) - {"sqlite_sequence"})
    ]
    return table_reports, omitted, deferred_indexes


def snapshot_one(
    source: Path,
    destination: Path,
    spec: dict[str, Any],
    connection: sqlite3.Connection,
    pin_report: dict[str, Any],
    *,
    review_lower_epoch: float,
    long_lower_epoch: float,
    upper_epoch: float,
    budget_bytes: int,
    progress: dict[str, str] | None = None,
) -> dict[str, Any]:
    if not source.is_file():
        raise FileNotFoundError(source)
    started = time.time()
    source_stat_before = source.stat()
    data_version_before = int(connection.execute("PRAGMA src.data_version").fetchone()[0])
    if progress is not None:
        progress["stage"] = "source_metadata"
    source_metadata = database_metadata(connection, spec, schema="src")
    if source_metadata["missing_required_tables"]:
        raise RuntimeError(
            f"snapshot missing required tables for {source}: "
            f"{source_metadata['missing_required_tables']}"
        )
    if source_metadata["missing_required_watermarks"]:
        raise RuntimeError(
            f"snapshot missing required watermarks for {source}: "
            f"{source_metadata['missing_required_watermarks']}"
        )
    table_reports, omitted_tables, deferred_indexes = copy_selected_tables(
        connection,
        spec,
        review_lower_epoch=review_lower_epoch,
        long_lower_epoch=long_lower_epoch,
        upper_epoch=upper_epoch,
        progress=progress,
    )
    if progress is not None:
        progress["stage"] = "release_source_read_view"
    connection.commit()
    read_view_released = time.time()
    connection.set_progress_handler(None, 0)
    data_version_after = int(connection.execute("PRAGMA src.data_version").fetchone()[0])
    source_stat_after = source.stat()
    connection.execute("DETACH DATABASE src")
    index_build_started = time.time()
    if progress is not None:
        progress["stage"] = "build_snapshot_indexes"
    create_deferred_indexes(connection, deferred_indexes, table_reports)
    connection.commit()
    index_build_finished = time.time()
    candidate_report = table_reports.get(CANDIDATE_OBSERVATION_TABLE)
    if candidate_report:
        if progress is not None:
            progress["stage"] = "verify_candidate_projection"
        verify_candidate_observation_projection(connection, candidate_report)
    connection.execute("PRAGMA journal_mode=DELETE")
    connection.commit()
    finished = time.time()
    if progress is not None:
        progress["stage"] = "snapshot_quick_check"
    check = sqlite3.connect(destination)
    check.row_factory = sqlite3.Row
    try:
        quick_check = [str(row[0]) for row in check.execute("PRAGMA quick_check").fetchall()]
        metadata = database_metadata(check, spec, include_views=True)
    finally:
        check.close()
    if quick_check != ["ok"]:
        raise RuntimeError(f"snapshot quick_check failed for {source}: {quick_check[:20]}")
    if metadata["missing_required_tables"]:
        raise RuntimeError(
            f"snapshot missing required tables for {source}: {metadata['missing_required_tables']}"
        )
    if progress is not None:
        progress["stage"] = "snapshot_budget_and_sha256"
    snapshot_size = destination.stat().st_size
    if snapshot_size > budget_bytes:
        raise RuntimeError(
            f"selective snapshot exceeded database budget for {source}: "
            f"{snapshot_size}>{budget_bytes}"
        )
    with destination.open("rb") as handle:
        os.fsync(handle.fileno())
    return {
        "source_path": str(source.resolve()),
        "snapshot_path": str(destination.resolve()),
        "started_at": utc_iso(started),
        "finished_at": utc_iso(finished),
        "source_read_view_released_at": utc_iso(read_view_released),
        "started_epoch": started,
        "finished_epoch": finished,
        "midpoint_epoch": (started + finished) / 2,
        "duration_sec": round(finished - started, 6),
        "source_read_lock_duration_sec": round(
            read_view_released - float(pin_report["pinned_started_epoch"]), 6
        ),
        "source_read_lock_limit_sec": float(
            pin_report["source_read_lock_limit_sec"]
        ),
        "source_read_lock_budget_passed": (
            read_view_released - float(pin_report["pinned_started_epoch"])
            <= float(pin_report["source_read_lock_limit_sec"]) + 1.0
        ),
        "source_read_lock_released_before_index_build": (
            index_build_started >= read_view_released
        ),
        "deferred_index_build_started_at": utc_iso(index_build_started),
        "deferred_index_build_finished_at": utc_iso(index_build_finished),
        "deferred_index_build_duration_sec": round(
            index_build_finished - index_build_started, 6
        ),
        "source_size_bytes_before": source_stat_before.st_size,
        "source_size_bytes_after": source_stat_after.st_size,
        "source_mtime_before": source_stat_before.st_mtime,
        "source_mtime_after": source_stat_after.st_mtime,
        "source_data_version_before": data_version_before,
        "source_data_version_after": data_version_after,
        "destination_connection_total_changes": int(connection.total_changes),
        "source_open_mode": "read_only_attached_uri",
        "source_mutated_by_snapshot_process": False,
        "source_changed_during_backup": (
            data_version_before != data_version_after
            or source_stat_before.st_mtime_ns != source_stat_after.st_mtime_ns
            or source_stat_before.st_size != source_stat_after.st_size
        ),
        "temporary_full_backup_size_bytes": 0,
        "snapshot_size_bytes": snapshot_size,
        "database_budget_bytes": int(budget_bytes),
        "database_budget_passed": snapshot_size <= budget_bytes,
        "snapshot_sha256": sha256_file(destination),
        "quick_check": quick_check,
        "pinned_read_view": pin_report,
        "selection_upper_epoch": float(upper_epoch),
        "selection_review_lower_epoch": float(review_lower_epoch),
        "selection_long_lower_epoch": float(long_lower_epoch),
        "selected_tables": table_reports,
        "omitted_source_tables": omitted_tables,
        "source_schema_version": source_metadata["schema_version"],
        "source_table_schema_sha256": source_metadata["table_schema_sha256"],
        "source_upper_watermarks": source_metadata["upper_watermarks"],
        **metadata,
    }


def snapshot_all_concurrently(
    source_paths: dict[str, Path],
    partial_dir: Path,
    source_page_reports: dict[str, dict[str, int]],
    *,
    review_lower_epoch: float,
    long_lower_epoch: float,
    upper_epoch: float,
    database_budgets: dict[str, int],
    busy_timeout_ms: int,
    max_source_read_lock_sec: float,
) -> dict[str, dict[str, Any]]:
    names = tuple(DATABASE_SPECS)
    start_barrier = threading.Barrier(len(names))
    pinned_barrier = threading.Barrier(len(names))
    reports: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    result_lock = threading.Lock()

    def worker(name: str) -> None:
        connection = None
        progress = {"stage": "open_destination"}
        try:
            source = source_paths[name]
            destination = partial_dir / DATABASE_SPECS[name]["filename"]
            timeout_sec = max(0.001, float(busy_timeout_ms) / 1000.0)
            connection = sqlite3.connect(destination, timeout=timeout_sec, uri=True)
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout={max(0, int(busy_timeout_ms))}")
            connection.execute("PRAGMA journal_mode=OFF")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute("PRAGMA temp_store=FILE")
            page_size = max(512, int(source_page_reports[name]["page_size"] or 4096))
            connection.execute(f"PRAGMA page_size={page_size}")
            max_pages = max(1, int(database_budgets[name]) // page_size)
            connection.execute(f"PRAGMA max_page_count={max_pages}")
            source_uri = f"file:{quote(str(source.resolve()), safe='/')}?mode=ro"
            progress["stage"] = "attach_source"
            connection.execute("ATTACH DATABASE ? AS src", (source_uri,))
            progress["stage"] = "start_barrier"
            start_barrier.wait(timeout=30)
            pin_started = time.time()
            lock_deadline = time.monotonic() + float(max_source_read_lock_sec)

            def interrupt_expired_read_view() -> int:
                return 1 if time.monotonic() >= lock_deadline else 0

            connection.set_progress_handler(interrupt_expired_read_view, 10000)
            progress["stage"] = "pin_source_read_view"
            connection.execute("BEGIN")
            connection.execute("SELECT COUNT(*) FROM src.sqlite_master").fetchone()
            pin_finished = time.time()
            pin_report = {
                "pinned_started_at": utc_iso(pin_started),
                "pinned_finished_at": utc_iso(pin_finished),
                "pinned_started_epoch": pin_started,
                "pinned_finished_epoch": pin_finished,
                "pinned_midpoint_epoch": (pin_started + pin_finished) / 2,
                "source_read_lock_limit_sec": float(max_source_read_lock_sec),
                **source_page_reports[name],
            }
            progress["stage"] = "pinned_barrier"
            pinned_barrier.wait(timeout=30)
            report = snapshot_one(
                source,
                partial_dir / DATABASE_SPECS[name]["filename"],
                DATABASE_SPECS[name],
                connection,
                pin_report,
                review_lower_epoch=review_lower_epoch,
                long_lower_epoch=long_lower_epoch,
                upper_epoch=upper_epoch,
                budget_bytes=database_budgets[name],
                progress=progress,
            )
            with result_lock:
                reports[name] = report
        except Exception as exc:
            if connection is not None:
                connection.set_progress_handler(None, 0)
            if (
                isinstance(exc, sqlite3.OperationalError)
                and "interrupted" in str(exc).lower()
                and time.monotonic()
                >= locals().get("lock_deadline", float("inf"))
            ):
                exc = RuntimeError(
                    f"source_read_lock_budget_exceeded:{name}:"
                    f"{progress.get('stage') or 'unknown'}:"
                    f"{float(max_source_read_lock_sec):.3f}s"
                )
            for barrier in (start_barrier, pinned_barrier):
                try:
                    barrier.abort()
                except threading.BrokenBarrierError:
                    pass
            with result_lock:
                errors[name] = {
                    "error_code": snapshot_component_failure_code(exc),
                    "error_type": type(exc).__name__,
                    "stage": str(progress.get("stage") or "unknown"),
                }
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
        raise ConcurrentSnapshotError(errors)
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
    min_free_after_gib: float,
    max_output_gib: float,
) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(root)
    bounded_output = int(float(max_output_gib) * 1024**3)
    reserve = int(float(min_free_after_gib) * 1024**3)
    accepted = bounded_output > 0 and usage.free >= bounded_output + reserve
    return {
        "free_bytes": usage.free,
        "selective_snapshot_output_cap_bytes": bounded_output,
        "temporary_full_backup_bytes": 0,
        "estimated_peak_working_bytes": bounded_output,
        "required_reserve_bytes": reserve,
        "estimated_free_after_bytes": usage.free - bounded_output,
        "estimated_free_at_peak_bytes": usage.free - bounded_output,
        "fail_closed_on_insufficient_space": True,
        "accepted": accepted,
    }


def static_database_output_budgets(max_output_gib: float) -> dict[str, int]:
    total = int(float(max_output_gib) * 1024**3)
    if total <= 0:
        raise ValueError("max_output_gib must be positive")
    budgets = {
        name: int(total * DATABASE_BUDGET_SHARES[name])
        for name in DATABASE_SPECS
    }
    budgets["paper"] += total - sum(budgets.values())
    return budgets


def strict_positive_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def database_output_budget_plan(
    max_output_gib: float,
    source_page_reports: dict[str, dict[str, int]],
) -> dict[str, Any]:
    """Reclaim unused non-paper reserves without changing the bundle cap."""

    static_budgets = static_database_output_budgets(max_output_gib)
    budgets = dict(static_budgets)
    compact_estimates: dict[str, int | None] = {}
    padded_estimates: dict[str, int | None] = {}
    static_fallback_databases: list[str] = []
    reclaimed = 0

    for name in ("signal", "raw", "kline"):
        report = source_page_reports.get(name) or {}
        compact_bytes = strict_positive_int(report.get("estimated_compact_bytes"))
        page_size = strict_positive_int(report.get("page_size")) or 4096
        page_size = max(512, page_size)
        if compact_bytes is None:
            compact_estimates[name] = None
            padded_estimates[name] = None
            static_fallback_databases.append(name)
            continue

        padded_bytes = (
            compact_bytes * DYNAMIC_BUDGET_HEADROOM_NUMERATOR
            + DYNAMIC_BUDGET_HEADROOM_DENOMINATOR
            - 1
        ) // DYNAMIC_BUDGET_HEADROOM_DENOMINATOR
        padded_bytes = max(
            page_size,
            ((padded_bytes + page_size - 1) // page_size) * page_size,
        )
        adaptive_budget = min(static_budgets[name], padded_bytes)
        compact_estimates[name] = compact_bytes
        padded_estimates[name] = padded_bytes
        budgets[name] = adaptive_budget
        reclaimed += static_budgets[name] - adaptive_budget

    paper_report = source_page_reports.get("paper") or {}
    compact_estimates["paper"] = strict_positive_int(
        paper_report.get("estimated_compact_bytes")
    )

    budgets["paper"] = static_budgets["paper"] + reclaimed
    total_cap_bytes = sum(static_budgets.values())
    if sum(budgets.values()) != total_cap_bytes:
        raise RuntimeError("dynamic evaluator database budgets do not preserve the bundle cap")
    if any(int(value) <= 0 for value in budgets.values()):
        raise RuntimeError(f"dynamic evaluator database budget is non-positive: {budgets}")

    return {
        "schema_version": BUDGET_SCHEMA_VERSION,
        "mode": "reclaim_unused_non_paper_static_reserves_to_paper",
        "total_output_cap_bytes": total_cap_bytes,
        "headroom_ratio": (
            DYNAMIC_BUDGET_HEADROOM_NUMERATOR / DYNAMIC_BUDGET_HEADROOM_DENOMINATOR
        ),
        "static_share_budget_bytes": static_budgets,
        "source_compact_estimate_bytes": compact_estimates,
        "padded_non_paper_estimate_bytes": padded_estimates,
        "static_fallback_databases": static_fallback_databases,
        "reclaimed_to_paper_bytes": reclaimed,
        "database_budget_bytes": budgets,
        "total_budget_bytes": sum(budgets.values()),
        "bundle_cap_unchanged": True,
    }


def publish_current(root: Path, snapshot_dir: Path) -> None:
    current = root / "current"
    if current.exists() and not current.is_symlink():
        raise RuntimeError(f"current path must be a symlink or absent: {current}")
    temporary = root / f".current.{snapshot_dir.name}.tmp"
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(Path("snapshots") / snapshot_dir.name, target_is_directory=True)
    os.replace(temporary, current)


def current_symlink_target(root: Path) -> str | None:
    current = root / "current"
    if not current.exists() and not current.is_symlink():
        return None
    if not current.is_symlink():
        raise RuntimeError(f"current path must be a symlink or absent: {current}")
    return os.readlink(current)


def restore_current(root: Path, target: str | None) -> None:
    current = root / "current"
    if target is None:
        current.unlink(missing_ok=True)
        fsync_directory(root)
        return
    temporary = root / ".current.rollback.tmp"
    temporary.unlink(missing_ok=True)
    temporary.symlink_to(target, target_is_directory=True)
    os.replace(temporary, current)
    fsync_directory(root)


def prune_old_snapshots(root: Path, current_name: str, keep_previous: int) -> dict[str, list[str]]:
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
        if (
            manifest.get("schema_version") in PRUNABLE_SCHEMA_VERSIONS
            and manifest.get("accepted") is True
        ):
            valid.append(path)
    valid.sort(key=lambda item: item.name, reverse=True)
    protected = {current_name, *[path.name for path in valid if path.name != current_name][:keep_previous]}
    removed = []
    errors = []
    for path in valid:
        if path.name in protected:
            continue
        try:
            shutil.rmtree(path)
            removed.append(str(path))
        except OSError as exc:
            errors.append(f"{path}:{type(exc).__name__}:{exc}")
    return {"removed_snapshots": removed, "removal_errors": errors}


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
    max_output_gib: float = DEFAULT_MAX_OUTPUT_GIB,
    review_history_hours: float = DEFAULT_REVIEW_HISTORY_HOURS,
    long_history_hours: float = DEFAULT_LONG_HISTORY_HOURS,
    source_busy_timeout_ms: int = 30000,
    max_source_read_lock_sec: float = DEFAULT_MAX_SOURCE_READ_LOCK_SEC,
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
    started = time.time()
    selection_upper_epoch = started
    review_history_hours = float(review_history_hours)
    long_history_hours = float(long_history_hours)
    if review_history_hours < 72:
        raise ValueError("review_history_hours must cover at least 72 hours")
    if long_history_hours < review_history_hours:
        raise ValueError("long_history_hours must be >= review_history_hours")
    if int(source_busy_timeout_ms) < 0:
        raise ValueError("source_busy_timeout_ms must be non-negative")
    if float(max_source_read_lock_sec) <= 0:
        raise ValueError("max_source_read_lock_sec must be positive")
    review_lower_epoch = selection_upper_epoch - review_history_hours * 3600
    long_lower_epoch = selection_upper_epoch - long_history_hours * 3600
    snapshots_root = root / "snapshots"
    snapshots_root.mkdir(parents=True, exist_ok=True)
    final_dir = snapshots_root / sid
    partial_dir = snapshots_root / f".{sid}.partial"
    if final_dir.exists() or partial_dir.exists():
        raise FileExistsError(sid)
    partial_dir.mkdir()
    try:
        source_page_reports = inspect_source_page_reports(
            source_paths,
            busy_timeout_ms=int(source_busy_timeout_ms),
        )
        preflight = disk_preflight(root, min_free_after_gib, max_output_gib)
        if not preflight["accepted"]:
            raise RuntimeError(f"insufficient disk for evaluator snapshot: {preflight}")
        budget_plan = database_output_budget_plan(max_output_gib, source_page_reports)
        output_budgets = budget_plan["database_budget_bytes"]
        database_reports = snapshot_all_concurrently(
            source_paths,
            partial_dir,
            source_page_reports,
            review_lower_epoch=review_lower_epoch,
            long_lower_epoch=long_lower_epoch,
            upper_epoch=selection_upper_epoch,
            database_budgets=output_budgets,
            busy_timeout_ms=int(source_busy_timeout_ms),
            max_source_read_lock_sec=float(max_source_read_lock_sec),
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
        selection_upper_bounds_consistent = all(
            float(report["selection_upper_epoch"]) == float(selection_upper_epoch)
            for report in database_reports.values()
        )
        source_read_lock_budget_passed = all(
            report.get("source_read_lock_budget_passed") is True
            for report in database_reports.values()
        )
        indexes_built_after_source_read_lock_release = all(
            report.get("source_read_lock_released_before_index_build") is True
            for report in database_reports.values()
        )
        database_payload_size_bytes = sum(
            int(report["snapshot_size_bytes"]) for report in database_reports.values()
        )
        output_cap_bytes = int(preflight["selective_snapshot_output_cap_bytes"])
        payload_directory = snapshot_directory_report(partial_dir, include_manifest=False)
        if not payload_directory["accepted"]:
            raise RuntimeError(f"snapshot payload contains unexpected files: {payload_directory}")
        if int(payload_directory["total_size_bytes"]) != database_payload_size_bytes:
            raise RuntimeError(
                "snapshot payload size accounting mismatch: "
                f"{payload_directory['total_size_bytes']}!={database_payload_size_bytes}"
            )
        output_cap_passed = database_payload_size_bytes < output_cap_bytes
        git_commit = detected_commit(Path(repo_root).expanduser().resolve())
        accepted = bool(
            quick_checks_passed
            and required_tables_present
            and required_watermarks_present
            and source_mutation_free
            and skew <= max_skew_sec
            and selection_upper_bounds_consistent
            and source_read_lock_budget_passed
            and indexes_built_after_source_read_lock_release
            and output_cap_passed
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
            "snapshot_ts": selection_upper_epoch,
            "git_commit": git_commit,
            "git_commit_present": bool(git_commit),
            "method": "coordinated_read_view_pin_then_compact_bounded_selective_extract",
            "bounded_selective_snapshot": True,
            "read_views_pinned_before_copy": True,
            "max_source_read_lock_sec": float(max_source_read_lock_sec),
            "source_read_lock_budget_passed": source_read_lock_budget_passed,
            "indexes_built_after_source_read_lock_release": (
                indexes_built_after_source_read_lock_release
            ),
            "source_mutation_free": source_mutation_free,
            "copy_mode": "parallel_direct_extract_no_full_database_intermediate",
            "cross_database_time_skew_sec": round(skew, 6),
            "max_allowed_cross_database_time_skew_sec": float(max_skew_sec),
            "cross_database_time_skew_passed": skew <= max_skew_sec,
            "quick_checks_passed": quick_checks_passed,
            "required_tables_present": required_tables_present,
            "required_watermarks_present": required_watermarks_present,
            "selection_upper_bounds_consistent": selection_upper_bounds_consistent,
            "selection_contract": {
                "schema_version": SELECTION_SCHEMA_VERSION,
                "common_upper_epoch": selection_upper_epoch,
                "common_upper_at": utc_iso(selection_upper_epoch),
                "review_history_hours": review_history_hours,
                "review_lower_epoch": review_lower_epoch,
                "long_history_hours": long_history_hours,
                "long_lower_epoch": long_lower_epoch,
                "supported_capture_windows_hours": [24, 48, 72],
                "future_rows_excluded": True,
                "table_rules_are_explicit": True,
            },
            "database_payload_size_bytes": database_payload_size_bytes,
            "database_budget_plan": budget_plan,
            "output_size_bytes": database_payload_size_bytes,
            "output_cap_bytes": output_cap_bytes,
            "output_cap_passed": output_cap_passed,
            "disk_preflight": preflight,
            "databases": database_reports,
            "accepted": accepted,
            "immutable": True,
            "partial_artifacts_absent": True,
            "active_database_reads_allowed_for_autoloop": False,
            "promotion_allowed": False,
        }
        if not accepted:
            raise RuntimeError(f"cross-database snapshot acceptance failed: {manifest}")
        write_bounded_manifest(
            partial_dir,
            manifest,
            output_cap_bytes=output_cap_bytes,
        )
        if manifest["output_cap_passed"] is not True:
            raise RuntimeError(f"cross-database snapshot output cap failed: {manifest}")
        previous_current_target = current_symlink_target(root)
        latest_path = root / "latest_manifest.json"
        previous_latest_bytes = latest_path.read_bytes() if latest_path.is_file() else None
        manifest["retention"] = {
            "keep_previous": max(0, int(keep_previous)),
            "removed_snapshots": [],
            "removal_errors": [],
        }
        try:
            os.replace(partial_dir, final_dir)
            fsync_directory(snapshots_root)
            publish_current(root, final_dir)
            fsync_directory(root)
            atomic_json(latest_path, manifest)
            fsync_directory(root)
        except BaseException:
            restore_current(root, previous_current_target)
            if previous_latest_bytes is None:
                latest_path.unlink(missing_ok=True)
            else:
                atomic_bytes(latest_path, previous_latest_bytes)
            fsync_directory(root)
            if final_dir.exists():
                shutil.rmtree(final_dir)
            raise
        retention = prune_old_snapshots(root, final_dir.name, max(0, int(keep_previous)))
        manifest["retention"].update(retention)
        try:
            atomic_json(latest_path, manifest)
        except Exception as exc:
            manifest["retention"]["status_write_error"] = f"{type(exc).__name__}:{exc}"
        latest_manifest = manifest
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
                "CREATE INDEX idx_candidate_shadow_obs_observed "
                "ON candidate_shadow_observations(observed_at);"
                "CREATE TABLE candidate_shadow_virtual_trades(signal_id INTEGER, observed_at INTEGER);"
                "CREATE INDEX idx_candidate_shadow_virtual_observed "
                "ON candidate_shadow_virtual_trades(observed_at);"
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
                max_output_gib=0.1,
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
    status_path = (
        Path(args.status_out).expanduser().resolve()
        if args.status_out
        else None
    )
    previous = read_json_object(status_path) if status_path else {}
    continuous_worker = int(args.max_runs) <= 0
    interval_sec = max(1, int(getattr(args, "interval_sec", 21600)))
    failure_retry_sec = max(
        MIN_FAILURE_RETRY_SEC,
        int(getattr(args, "failure_retry_sec", DEFAULT_FAILURE_RETRY_SEC)),
    )
    current_path = str(Path(args.out_root).expanduser().resolve() / "current")
    base_status = {
        "schema_version": WORKER_STATUS_SCHEMA_VERSION,
        "pid": os.getpid(),
        "worker_mode": "continuous" if continuous_worker else "bounded",
        "running": True,
        "attempt_running": True,
        "started_at": started,
        "finished_at": None,
        "last_attempt_at": started,
        "last_success_at": previous.get("last_success_at"),
        "last_failure_at": previous.get("last_failure_at"),
        "last_failure_code": previous.get("last_failure_code"),
        "last_failure_details": previous.get("last_failure_details"),
        "last_error": previous.get("last_error"),
        "error_count": int(previous.get("error_count") or 0),
        "consecutive_failure_count": int(previous.get("consecutive_failure_count") or 0),
        "success_interval_sec": interval_sec,
        "failure_retry_sec": failure_retry_sec,
        "next_attempt_delay_sec": None,
        "next_attempt_at": None,
        "status": "running",
        "accepted": False,
        "snapshot_id": None,
        "current": current_path,
        "last_accepted_snapshot": previous.get("last_accepted_snapshot"),
        "promotion_allowed": False,
    }
    lock_acquired = False
    try:
        with exclusive_lock(Path(args.lock_file).expanduser().resolve()):
            lock_acquired = True
            if status_path:
                atomic_json(status_path, base_status)
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
                max_output_gib=args.max_output_gib,
                review_history_hours=args.review_history_hours,
                long_history_hours=args.long_history_hours,
                source_busy_timeout_ms=args.source_busy_timeout_ms,
                max_source_read_lock_sec=args.max_source_read_lock_sec,
                keep_previous=args.keep_previous,
                snapshot_id=args.snapshot_id,
            )
            finished = utc_iso()
            accepted_summary = snapshot_manifest_summary(manifest)
            accepted_manifest_path = (
                Path(args.out_root).expanduser().resolve()
                / "snapshots"
                / str(manifest["snapshot_id"])
                / "manifest.json"
            )
            accepted_summary["manifest_path"] = str(accepted_manifest_path)
            accepted_summary["manifest_sha256"] = sha256_file(accepted_manifest_path)
            next_attempt_delay = snapshot_next_attempt_delay_sec(
                {"accepted": True, "consecutive_failure_count": 0},
                interval_sec=interval_sec,
                failure_retry_sec=failure_retry_sec,
            )
            status = {
                **base_status,
                "running": continuous_worker,
                "attempt_running": False,
                "finished_at": finished,
                "last_success_at": finished,
                "last_failure_code": None,
                "last_failure_details": None,
                "last_error": None,
                "status": "completed",
                "accepted": True,
                "consecutive_failure_count": 0,
                "snapshot_id": manifest["snapshot_id"],
                "interrupted_partials_removed": interrupted_partials_removed,
                "last_accepted_snapshot": accepted_summary,
                "next_attempt_delay_sec": next_attempt_delay if continuous_worker else None,
                "next_attempt_at": (
                    utc_iso(time.time() + next_attempt_delay)
                    if continuous_worker
                    else None
                ),
            }
            if status_path:
                atomic_json(status_path, status)
    except Exception as exc:
        finished = utc_iso()
        failure_code = snapshot_failure_code(exc)
        failure_details = snapshot_failure_details(exc)
        consecutive_failure_count = int(base_status["consecutive_failure_count"]) + 1
        retry_status = {
            "accepted": False,
            "last_failure_code": failure_code,
            "consecutive_failure_count": consecutive_failure_count,
        }
        next_attempt_delay = snapshot_next_attempt_delay_sec(
            retry_status,
            interval_sec=interval_sec,
            failure_retry_sec=failure_retry_sec,
        )
        status = {
            **base_status,
            "running": continuous_worker if lock_acquired else False,
            "attempt_running": False,
            "finished_at": finished,
            "last_failure_at": finished,
            "last_failure_code": failure_code,
            "last_failure_details": failure_details,
            "last_error": bounded_error_text(exc),
            "error_count": int(base_status["error_count"]) + 1,
            "status": "failed",
            "accepted": False,
            "consecutive_failure_count": consecutive_failure_count,
            "error": bounded_error_text(exc),
            "status_artifact_preserved": not lock_acquired,
            "next_attempt_delay_sec": next_attempt_delay if continuous_worker else None,
            "next_attempt_at": (
                utc_iso(time.time() + next_attempt_delay)
                if continuous_worker
                else None
            ),
        }
        if status_path and lock_acquired:
            atomic_json(status_path, status)
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
    parser.add_argument("--max-output-gib", type=float, default=DEFAULT_MAX_OUTPUT_GIB)
    parser.add_argument("--review-history-hours", type=float, default=DEFAULT_REVIEW_HISTORY_HOURS)
    parser.add_argument("--long-history-hours", type=float, default=DEFAULT_LONG_HISTORY_HOURS)
    parser.add_argument("--source-busy-timeout-ms", type=int, default=30000)
    parser.add_argument(
        "--max-source-read-lock-sec",
        type=float,
        default=DEFAULT_MAX_SOURCE_READ_LOCK_SEC,
    )
    parser.add_argument("--keep-previous", type=int, default=0)
    parser.add_argument("--snapshot-id")
    parser.add_argument("--lock-file", default="/tmp/cross-db-evaluator-snapshot.lock")
    parser.add_argument("--status-out", default="/app/data/agent_evidence/snapshot_status.json")
    parser.add_argument("--max-runs", type=int, default=1)
    parser.add_argument("--interval-sec", type=int, default=21600)
    parser.add_argument(
        "--failure-retry-sec",
        type=int,
        default=DEFAULT_FAILURE_RETRY_SEC,
    )
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
        time.sleep(
            snapshot_next_attempt_delay_sec(
                last_status or {},
                interval_sec=args.interval_sec,
                failure_retry_sec=args.failure_retry_sec,
            )
        )
    return 0 if last_status and last_status["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
