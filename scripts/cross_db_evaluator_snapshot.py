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
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import sqlite3
import stat as stat_module
import struct
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any
from urllib.parse import quote
import zlib

from evaluator_evidence_schema import (
    EVIDENCE_SCHEMA_SHA256_FIELD,
    EVIDENCE_SCHEMA_VALIDATED_FIELD,
    EVIDENCE_SCHEMA_VERSION_FIELD,
    bind_numeric_evidence_schema,
    require_numeric_evidence_schema,
)


SCHEMA_VERSION = "cross_db_evaluator_snapshot.v3"
PRUNABLE_SCHEMA_VERSIONS = {
    SCHEMA_VERSION,
    "cross_db_evaluator_snapshot.v2",
    "cross_db_evaluator_snapshot.v1",
}
SELECTION_SCHEMA_VERSION = "evaluator_snapshot_selection.v1"
BUDGET_SCHEMA_VERSION = "evaluator_snapshot_budget.v2"
PAYLOAD_PROJECTION_SCHEMA_VERSION = "candidate_observation_payload_projection.v1"
CANDIDATE_STAGE_SCHEMA_VERSION = "candidate_observation_selective_stage.v1"
CANDIDATE_STAGE_SCHEMA = "candidate_stage"
CANDIDATE_STAGE_TABLE = "__a3_candidate_shadow_observation_stage"
CANDIDATE_STAGE_ORDER_INDEX = "idx_a3_candidate_stage_signal"
MIN_CANDIDATE_STAGE_CAP_BYTES = 3 * 4096
CANDIDATE_STAGE_BUDGET_MODE = "shared_stage_budget_coordinator"
PARALLEL_PAPER_STAGE_SCHEMA_VERSION = "parallel_paper_event_stage.v4"
PARALLEL_PAPER_STAGE_STORAGE_MODE = "lossless_compressed_chunk_spool"
PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION = "sqlite_value_tlv_stream.v2"
PARALLEL_PAPER_STAGE_COMPRESSION = "zlib_level_1"
PARALLEL_PAPER_STAGE_COMPRESSION_LEVEL = 1
PARALLEL_PAPER_STAGE_CHUNK_TARGET_BYTES = 4 * 1024**2
PARALLEL_PAPER_STAGE_MAX_CHUNK_RAW_BYTES = PARALLEL_PAPER_STAGE_CHUNK_TARGET_BYTES
PARALLEL_PAPER_STAGE_MAX_COMPRESSED_CHUNK_BYTES = (
    PARALLEL_PAPER_STAGE_MAX_CHUNK_RAW_BYTES + 64 * 1024
)
PARALLEL_PAPER_STAGE_METADATA_TABLE = "__a3_parallel_stage_metadata"
PARALLEL_PAPER_STAGE_CHUNK_TABLE = "__a3_parallel_stage_chunks"
MIN_PARALLEL_PAPER_STAGE_CAP_BYTES = 3 * 4096
SHARED_STAGE_BUDGET_SCHEMA_VERSION = "shared_stage_budget.v2"
SHARED_STAGE_BUDGET_ALLOCATION_MODE = (
    "history_high_water_plus_advisory_source_demand"
)
SHARED_STAGE_TARGET_CANDIDATE = "candidate_shadow_observations"
SHARED_STAGE_PAGE_SIZE = 4096
PARALLEL_PAPER_STAGE_BULK_PAGE_SIZE = 65536
# Production's 494-506 MiB wide-row stage grants remained I/O-bound on 4 KiB
# overflow pages. Keep small-stage compatibility, but leave enough headroom
# below those observed grants to absorb normal retention-window drift.
PARALLEL_PAPER_STAGE_BULK_PAGE_MIN_BUDGET_BYTES = 384 * 1024**2
PARALLEL_PAPER_STAGE_PAGE_SIZES = frozenset(
    {SHARED_STAGE_PAGE_SIZE, PARALLEL_PAPER_STAGE_BULK_PAGE_SIZE}
)
SHARED_STAGE_MAX_SAFE_INTEGER = 9_007_199_254_740_991
SHARED_STAGE_ESTIMATE_SAMPLE_ROWS = 256
# Keep the indexed count/sample path bounded below the independent 300-second
# source-read-lock budget. Full-table DBSTAT has its own much shorter window.
SHARED_STAGE_ESTIMATE_TIMEOUT_SEC = 180.0
# Exact indexed COUNT(*) is also only allocation evidence.  On production-scale
# ranges it must not consume the entire estimate/read-lock budget before the
# bounded advisory fallback can run.
SHARED_STAGE_INDEXED_COUNT_TIMEOUT_SEC = 20.0
# Full-table DBSTAT is only an allocation hint. Bound its contribution so an
# advisory scan cannot consume the source-read-lock budget needed by the copy.
SHARED_STAGE_DBSTAT_ADVISORY_TIMEOUT_SEC = 20.0
# Source measurements are advisory allocation evidence, not a physical size
# proof. SQLite record packing can legitimately make a 4096-byte destination
# larger than any table-level DBSTAT extrapolation. Safety is therefore enforced
# only by the global disk cap plus per-file max_page_count; a low advisory demand
# may fail SQLITE_FULL, persist a high-water mark, and converge on the next run.
SHARED_STAGE_ADVISORY_ROW_OVERHEAD_BYTES = 32
SHARED_STAGE_ADVISORY_INDEX_OVERHEAD_BYTES = 32
SHARED_STAGE_ADVISORY_ROOT_RESERVE_PAGES = 2
SHARED_STAGE_ADVISORY_SCHEMA_VERSION = "sqlite_dbstat_advisory_demand.v1"
SHARED_STAGE_ADVISORY_FORMULA = (
    "source_physical_times_selected_row_fraction_plus_per_row_overhead_"
    "plus_root_reserve_plus_candidate_signal_index_fraction"
)
SHARED_STAGE_SAMPLE_ADVISORY_SCHEMA_VERSION = (
    "bounded_index_sample_advisory_demand.v1"
)
SHARED_STAGE_SAMPLE_ADVISORY_FORMULA = (
    "selected_rows_times_bounded_sample_max_plus_per_row_overhead_"
    "plus_root_reserve_plus_candidate_signal_index_overhead"
)
SHARED_STAGE_SAMPLE_ADVISORY_STRATEGY = (
    "bounded_index_sample_advisory_fallback"
)
SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_SCHEMA_VERSION = (
    "bounded_index_count_timeout_advisory_demand.v1"
)
SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_FORMULA = (
    "bounded_edge_sample_rows_times_sample_max_plus_per_row_overhead_"
    "plus_root_reserve_plus_candidate_signal_index_overhead"
)
SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_STRATEGY = (
    "bounded_index_count_timeout_advisory_fallback"
)
SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ROW_BINDING_MODE = (
    "copy_report_exact_after_indexed_count_timeout"
)
SHARED_STAGE_HASH_CANONICALIZATION = "json_sorted_float64_bits.v1"
SHARED_STAGE_HISTORY_ANCHOR_SCHEMA_VERSION = (
    "shared_stage_budget_history_anchor.v1"
)
SHARED_STAGE_HISTORY_ANCHOR_DIRECTORY = "shared_stage_budget_anchors"
SHARED_STAGE_ATTEMPT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")
SOURCE_DBSTAT_VIRTUAL_TABLE = "__a3_source_dbstat"
SHARED_STAGE_COMPLETED_HISTORY_HEADROOM = 1.10
SHARED_STAGE_INCOMPLETE_HISTORY_HEADROOM = 1.20
SHARED_STAGE_CAP_HIT_EXTRA_PAGES = 1
SHARED_STAGE_HIDDEN_FILE_RE = re.compile(r"^\..+-stage\.db(?:-(?:journal|wal|shm))?$")
PARALLEL_PAPER_STAGE_CONFIGS = {
    "paper_decision_events": {
        "schema": "paper_decision_stage",
        "filename": ".paper-decision-events-stage.db",
        "role": "paper_decision_events_parallel_stage",
        "required": True,
    },
    "a_class_decision_events": {
        "schema": "a_class_decision_stage",
        "filename": ".a-class-decision-events-stage.db",
        "role": "a_class_decision_events_parallel_stage",
        "required": True,
    },
    "opportunity_events": {
        "schema": "opportunity_events_stage",
        "filename": ".opportunity-events-stage.db",
        "role": "opportunity_events_parallel_stage",
        "required": True,
    },
    "opportunity_event_path_samples": {
        "schema": "opportunity_path_samples_stage",
        "filename": ".opportunity-event-path-samples-stage.db",
        "role": "opportunity_event_path_samples_parallel_stage",
        "required": False,
    },
}
PARALLEL_PAPER_STAGE_TABLES = tuple(PARALLEL_PAPER_STAGE_CONFIGS)
HEAVY_PARALLEL_ROWID_RANGE_TABLES = frozenset(
    {
        "paper_decision_events",
        "a_class_decision_events",
    }
)
PARALLEL_PAPER_REQUIRED_STAGE_TABLES = tuple(
    table
    for table, config in PARALLEL_PAPER_STAGE_CONFIGS.items()
    if config.get("required") is True
)
PARALLEL_PAPER_OPTIONAL_STAGE_TABLES = tuple(
    table
    for table, config in PARALLEL_PAPER_STAGE_CONFIGS.items()
    if config.get("required") is not True
)
PAPER_DECISION_STAGE_SCHEMA_VERSION = PARALLEL_PAPER_STAGE_SCHEMA_VERSION
PAPER_DECISION_STAGE_SCHEMA = PARALLEL_PAPER_STAGE_CONFIGS["paper_decision_events"]["schema"]
PAPER_DECISION_STAGE_TABLE = "paper_decision_events"
MIN_PAPER_DECISION_STAGE_CAP_BYTES = MIN_PARALLEL_PAPER_STAGE_CAP_BYTES
WORKER_STATUS_SCHEMA_VERSION = "cross_db_evaluator_snapshot_worker_status.v1"
WORKER_OWNER_SCHEMA_VERSION = "cross_db_evaluator_snapshot_worker_owner.v1"
WORKER_PROCESS_IDENTITY_SCHEMA_VERSION = "process_identity.v1"
WORKER_OWNER_FILENAME = ".snapshot-worker-owner.json"
WORKER_OWNER_LOCK_FILENAME = ".snapshot-worker-owner.lock"
WORKER_OWNER_MAX_BYTES = 16 * 1024
PARTIAL_OWNER_SCHEMA_VERSION = "cross_db_evaluator_snapshot_partial_owner.v1"
PARTIAL_OWNER_FILENAME = ".snapshot-partial-owner.json"
PARTIAL_OWNER_MAX_BYTES = 16 * 1024
WORKER_PROCESS_INSTANCE_ENV = "_CROSS_DB_EVALUATOR_SNAPSHOT_PROCESS_INSTANCE_ID"
_worker_process_instance_id = os.environ.get(WORKER_PROCESS_INSTANCE_ENV, "")
if not re.fullmatch(r"[a-f0-9]{32}", _worker_process_instance_id):
    _worker_process_instance_id = secrets.token_hex(16)
    os.environ[WORKER_PROCESS_INSTANCE_ENV] = _worker_process_instance_id
WORKER_PROCESS_INSTANCE_ID = _worker_process_instance_id
# importlib.reload reuses the module dictionary but reexecutes assignments.
# Preserve process-lifecycle guards so reload cannot bypass restart poison or
# create a second lock while the original worker is still active.
if "_WORKER_RESTART_POISONED_OUT_ROOTS" not in globals():
    _WORKER_RESTART_POISONED_OUT_ROOTS: dict[str, dict[str, Any]] = {}
if "_RUN_SNAPSHOT_ONCE_LOCK" not in globals():
    _RUN_SNAPSHOT_ONCE_LOCK = threading.RLock()
if "_WORKER_OWNER_LEASES" not in globals():
    _WORKER_OWNER_LEASES: dict[str, Any] = {}
DEFAULT_REVIEW_HISTORY_HOURS = 72.0
MAX_RESEARCH_HISTORY_HOURS = 24.0 * 30.0
DEFAULT_LONG_HISTORY_HOURS = MAX_RESEARCH_HISTORY_HOURS
DEFAULT_MAX_OUTPUT_GIB = 10.0
DEFAULT_MAX_SOURCE_READ_LOCK_SEC = 300.0
DEFAULT_FAILURE_RETRY_SEC = 60
PARALLEL_STAGE_CANCEL_GRACE_SEC = 2.0
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
            "opportunity_event_path_samples": (
                "id",
                "sample_ts",
                "created_at",
                "updated_at",
            ),
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
                "event_ts",
                "signal_ts",
                "created_at",
                required=True,
                indexed_epoch_seconds_anchor="event_ts",
                epoch_seconds_columns=("event_ts",),
            ),
            "a_class_decision_events": recent(
                "event_ts",
                "signal_ts",
                "opportunity_ts",
                "created_at",
                required=True,
                indexed_epoch_seconds_anchor="event_ts",
                epoch_seconds_columns=("event_ts",),
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
                "event_ts",
                "raw_signal_ts",
                "opportunity_ts",
                "created_at",
                "updated_at",
                required=True,
                indexed_epoch_seconds_anchor="event_ts",
                epoch_seconds_columns=("event_ts",),
            ),
            "canonical_trade_ledger": through_upper(
                "entry_ts", "exit_ts", "created_at", "updated_at"
            ),
            "paper_missed_signal_attribution": recent(
                "signal_ts", "created_event_ts", "baseline_ts", "first_tradable_ts",
                "created_at", "updated_at", horizon="long"
            ),
            "opportunity_event_path_samples": recent(
                "sample_ts",
                "created_at",
                "updated_at",
                horizon="long",
                indexed_epoch_seconds_anchor="sample_ts",
                epoch_seconds_columns=("sample_ts",),
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


def parallel_paper_stage_tables_for_schema(
    connection: sqlite3.Connection,
    *,
    schema: str = "main",
) -> tuple[str, ...]:
    schema = _schema_prefix(schema)
    source_tables = {
        str(row[0])
        for row in connection.execute(
            f"SELECT name FROM {schema}.sqlite_master WHERE type='table'"
        ).fetchall()
    }
    missing_required = [
        table
        for table in PARALLEL_PAPER_REQUIRED_STAGE_TABLES
        if table not in source_tables
    ]
    if missing_required:
        raise RuntimeError(
            "snapshot missing required tables: " + ",".join(missing_required)
        )
    return tuple(
        table for table in PARALLEL_PAPER_STAGE_TABLES if table in source_tables
    )


def active_parallel_paper_stage_tables(
    connection: sqlite3.Connection,
) -> tuple[str, ...]:
    return parallel_paper_stage_tables_for_schema(connection, schema="src")


def parallel_paper_stage_inventory_valid(tables: Any) -> bool:
    if not isinstance(tables, (list, tuple)):
        return False
    normalized = tuple(str(table) for table in tables)
    expected_order = tuple(
        table for table in PARALLEL_PAPER_STAGE_TABLES if table in normalized
    )
    return bool(
        normalized == expected_order
        and len(set(normalized)) == len(normalized)
        and set(PARALLEL_PAPER_REQUIRED_STAGE_TABLES).issubset(normalized)
    )


def shared_stage_target_names(
    parallel_stage_tables: tuple[str, ...] | list[str],
) -> tuple[str, ...]:
    tables = tuple(str(table) for table in parallel_stage_tables)
    if not parallel_paper_stage_inventory_valid(tables):
        raise ValueError(f"invalid shared stage inventory: {tables}")
    return (SHARED_STAGE_TARGET_CANDIDATE, *tables)


def shared_stage_target_filename(target: str) -> str:
    target = str(target)
    if target == SHARED_STAGE_TARGET_CANDIDATE:
        return ".candidate-observation-stage.db"
    config = PARALLEL_PAPER_STAGE_CONFIGS.get(target)
    if not config:
        raise ValueError(f"unknown shared stage target: {target}")
    return str(config["filename"])


def shared_stage_target_minimum_bytes(target: str) -> int:
    return (
        MIN_CANDIDATE_STAGE_CAP_BYTES
        if str(target) == SHARED_STAGE_TARGET_CANDIDATE
        else MIN_PARALLEL_PAPER_STAGE_CAP_BYTES
    )


def shared_stage_target_storage_schema_version(target: str) -> str:
    return (
        CANDIDATE_STAGE_SCHEMA_VERSION
        if str(target) == SHARED_STAGE_TARGET_CANDIDATE
        else PARALLEL_PAPER_STAGE_SCHEMA_VERSION
    )


def shared_stage_failure_aliases(target: str) -> tuple[str, ...]:
    target = str(target)
    if target == SHARED_STAGE_TARGET_CANDIDATE:
        return (
            SHARED_STAGE_TARGET_CANDIDATE,
            CANDIDATE_OBSERVATION_TABLE,
            CANDIDATE_STAGE_TABLE,
            CANDIDATE_STAGE_SCHEMA,
            shared_stage_target_filename(target),
        )
    if target in PARALLEL_PAPER_STAGE_CONFIGS:
        config = PARALLEL_PAPER_STAGE_CONFIGS[target]
        return (
            target,
            str(config["schema"]),
            str(config["filename"]),
        )
    raise ValueError(f"unknown shared stage target: {target}")


def shared_stage_failure_targets_target(
    target: str,
    details: dict[str, Any],
) -> bool:
    """Map a public-safe failure stage/current-table back to one stage target."""
    aliases = set(shared_stage_failure_aliases(target))
    stage = str(details.get("stage") or "")
    copy_timing = details.get("copy_timing")
    current_table = (
        str(copy_timing.get("current_table") or "")
        if isinstance(copy_timing, dict)
        else ""
    )
    return bool(
        current_table in aliases
        or any(alias and alias in stage for alias in aliases)
    )


def round_up_stage_page(value: Any) -> int:
    try:
        numeric = max(0, int(math.ceil(float(value))))
    except (TypeError, ValueError, OverflowError):
        return 0
    return (
        (numeric + SHARED_STAGE_PAGE_SIZE - 1)
        // SHARED_STAGE_PAGE_SIZE
        * SHARED_STAGE_PAGE_SIZE
    )


def parallel_paper_stage_page_size(budget_bytes: Any) -> int:
    """Use wider pages only for temporary stages large enough to benefit."""
    try:
        normalized_budget = max(0, int(budget_bytes or 0))
    except (TypeError, ValueError, OverflowError):
        normalized_budget = 0
    if normalized_budget >= PARALLEL_PAPER_STAGE_BULK_PAGE_MIN_BUDGET_BYTES:
        return PARALLEL_PAPER_STAGE_BULK_PAGE_SIZE
    return SHARED_STAGE_PAGE_SIZE


def remaining_source_read_lock_wait(
    *,
    deadline_monotonic: float,
    max_wait_sec: float | None = None,
    database: str,
    stage: str,
    limit_sec: float,
) -> float:
    remaining = float(deadline_monotonic) - time.monotonic()
    if remaining <= 0:
        raise RuntimeError(
            f"source_read_lock_budget_exceeded:{database}:{stage}:"
            f"{float(limit_sec):.3f}s"
        )
    if max_wait_sec is None:
        return remaining
    return min(max(0.001, float(max_wait_sec)), remaining)


def utc_iso(epoch: float | None = None) -> str:
    value = time.time() if epoch is None else epoch
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def json_safe_integer(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a JSON safe integer")
    if isinstance(value, int) and abs(value) <= 9_007_199_254_740_991:
        return value
    if (
        isinstance(value, float)
        and math.isfinite(value)
        and value.is_integer()
        and abs(value) <= 9_007_199_254_740_991
    ):
        return int(value)
    raise ValueError(f"{field} must be a JSON safe integer")


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


class PriorSnapshotWorkerActiveError(RuntimeError):
    """Fail closed while a prior worker can still own live stage inodes."""

    def __init__(self, owner: dict[str, Any] | None, liveness: str):
        super().__init__("evaluator_snapshot_prior_worker_active")
        self.worker_restart_required = True
        self.cleanup_deferred_until_worker_restart = True
        owner = owner if isinstance(owner, dict) else {}
        self.prior_worker_pid = _positive_process_pid(owner.get("pid"))
        identity = owner.get("process_identity")
        self.prior_worker_identity_source = str(
            identity.get("source") if isinstance(identity, dict) else "unknown"
        )
        self.prior_worker_liveness = str(liveness)


class SnapshotWorkerOwnerInvalidError(RuntimeError):
    """Fail closed when safe ownership transfer cannot be proven."""

    def __init__(self, reason: str):
        super().__init__(f"evaluator_snapshot_worker_owner_invalid:{reason}")
        self.worker_restart_required = True
        self.cleanup_deferred_until_worker_restart = True


def snapshot_worker_owner_path(out_root: Path) -> Path:
    return out_root.expanduser().resolve() / WORKER_OWNER_FILENAME


def snapshot_worker_owner_lock_path(out_root: Path) -> Path:
    return out_root.expanduser().resolve() / WORKER_OWNER_LOCK_FILENAME


def _acquire_snapshot_worker_lease(out_root: Path) -> None:
    """Hold a filesystem lease until this worker process really exits."""
    out_root_key = str(out_root.expanduser().resolve())
    existing = _WORKER_OWNER_LEASES.get(out_root_key)
    if existing is not None:
        try:
            fcntl.flock(existing.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except (OSError, ValueError):
            _WORKER_OWNER_LEASES.pop(out_root_key, None)
            try:
                existing.close()
            except Exception:
                pass
    lease_path = snapshot_worker_owner_lock_path(out_root)
    lease_path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_RDWR
    flags |= getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    descriptor = None
    try:
        descriptor = os.open(lease_path, flags, 0o600)
        if not stat_module.S_ISREG(os.fstat(descriptor).st_mode):
            os.close(descriptor)
            descriptor = None
            raise SnapshotWorkerOwnerInvalidError("lease_file_type")
        handle = os.fdopen(descriptor, "r+", encoding="utf-8")
        descriptor = None
    except (OSError, ValueError) as exc:
        if descriptor is not None:
            os.close(descriptor)
        raise SnapshotWorkerOwnerInvalidError("lease_open_failed") from exc
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        handle.close()
        try:
            active_owner = _load_snapshot_worker_owner(
                snapshot_worker_owner_path(out_root)
            )
        except SnapshotWorkerOwnerInvalidError:
            active_owner = None
        raise PriorSnapshotWorkerActiveError(
            active_owner,
            "process_lifetime_lease_held",
        ) from exc
    except OSError as exc:
        handle.close()
        raise SnapshotWorkerOwnerInvalidError("lease_lock_failed") from exc
    try:
        handle.seek(0)
        handle.truncate()
        handle.write(f"{os.getpid()} {WORKER_PROCESS_INSTANCE_ID} {utc_iso()}\n")
        handle.flush()
        os.fsync(handle.fileno())
        fsync_directory(lease_path.parent)
    except Exception as exc:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
        raise SnapshotWorkerOwnerInvalidError("lease_persist_failed") from exc
    _WORKER_OWNER_LEASES[out_root_key] = handle


def _release_snapshot_worker_lease(out_root: Path) -> None:
    out_root_key = str(out_root.expanduser().resolve())
    handle = _WORKER_OWNER_LEASES.pop(out_root_key, None)
    if handle is None:
        return
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def _release_direct_snapshot_worker_owner(
    out_root: Path,
    owner: dict[str, Any],
) -> None:
    if _interrupted_snapshot_partial_exists(out_root):
        raise SnapshotWorkerOwnerInvalidError(
            "direct_owner_release_with_partials"
        )
    if owner.get("lease_identity") != _snapshot_worker_lease_identity(out_root):
        raise SnapshotWorkerOwnerInvalidError(
            "direct_owner_release_lease_mismatch"
        )
    owner_path = snapshot_worker_owner_path(out_root)
    if _load_snapshot_worker_owner(owner_path) != owner:
        raise SnapshotWorkerOwnerInvalidError(
            "direct_owner_release_record_mismatch"
        )
    owner_path.unlink()
    fsync_directory(owner_path.parent)
    _release_snapshot_worker_lease(out_root)


def _snapshot_worker_lease_identity(out_root: Path) -> dict[str, int]:
    out_root_key = str(out_root.expanduser().resolve())
    handle = _WORKER_OWNER_LEASES.get(out_root_key)
    if handle is None:
        raise SnapshotWorkerOwnerInvalidError("lease_not_held")
    try:
        file_stat = os.fstat(handle.fileno())
    except (OSError, ValueError) as exc:
        raise SnapshotWorkerOwnerInvalidError("lease_identity_unavailable") from exc
    if not stat_module.S_ISREG(file_stat.st_mode):
        raise SnapshotWorkerOwnerInvalidError("lease_file_type")
    lease_path = snapshot_worker_owner_lock_path(out_root)
    try:
        path_stat = lease_path.lstat()
    except OSError as exc:
        raise SnapshotWorkerOwnerInvalidError("lease_path_unavailable") from exc
    if (
        not stat_module.S_ISREG(path_stat.st_mode)
        or int(path_stat.st_dev) != int(file_stat.st_dev)
        or int(path_stat.st_ino) != int(file_stat.st_ino)
    ):
        raise SnapshotWorkerOwnerInvalidError("lease_path_replaced")
    return {
        "device": int(file_stat.st_dev),
        "inode": int(file_stat.st_ino),
    }


def _valid_worker_lease_identity(identity: Any) -> bool:
    if not isinstance(identity, dict):
        return False
    device = identity.get("device")
    inode = identity.get("inode")
    return bool(
        isinstance(device, int)
        and not isinstance(device, bool)
        and device >= 0
        and isinstance(inode, int)
        and not isinstance(inode, bool)
        and inode > 0
    )


def _positive_process_pid(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        pid = int(value)
    except (TypeError, ValueError):
        return None
    return pid if 0 < pid <= 2_147_483_647 else None


def _pid_liveness(pid: int) -> str:
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return "exited"
    except PermissionError:
        return "alive"
    except OSError:
        return "unknown"
    return "alive"


def _valid_worker_process_identity(identity: Any) -> bool:
    if not isinstance(identity, dict):
        return False
    if identity.get("schema_version") != WORKER_PROCESS_IDENTITY_SCHEMA_VERSION:
        return False
    source = identity.get("source")
    if source == "linux_proc_stat":
        boot_id = str(identity.get("boot_id") or "")
        start_ticks = identity.get("start_time_ticks")
        return bool(
            re.fullmatch(
                r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}",
                boot_id,
            )
            and isinstance(start_ticks, int)
            and not isinstance(start_ticks, bool)
            and start_ticks > 0
        )
    if source == "ps_lstart":
        start_time = str(identity.get("start_time") or "")
        return bool(start_time and len(start_time) <= 128)
    return False


def snapshot_process_identity(pid: int) -> dict[str, Any]:
    """Return PID liveness plus a start-identity that survives PID reuse."""
    normalized_pid = _positive_process_pid(pid)
    if normalized_pid is None:
        return {"state": "exited", "identity": None}
    if sys.platform.startswith("linux"):
        stat_path = Path(f"/proc/{normalized_pid}/stat")
        try:
            stat_text = stat_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return {"state": "exited", "identity": None}
        except (OSError, UnicodeError):
            return {"state": _pid_liveness(normalized_pid), "identity": None}
        closing_parenthesis = stat_text.rfind(")")
        fields = (
            stat_text[closing_parenthesis + 2 :].split()
            if closing_parenthesis >= 0
            else []
        )
        if len(fields) <= 19:
            return {"state": _pid_liveness(normalized_pid), "identity": None}
        if fields[0] in {"X", "Z"}:
            return {"state": "exited", "identity": None}
        try:
            start_ticks = int(fields[19])
            boot_id = (
                Path("/proc/sys/kernel/random/boot_id")
                .read_text(encoding="utf-8")
                .strip()
                .lower()
            )
        except (OSError, TypeError, UnicodeError, ValueError):
            return {"state": _pid_liveness(normalized_pid), "identity": None}
        identity = {
            "schema_version": WORKER_PROCESS_IDENTITY_SCHEMA_VERSION,
            "source": "linux_proc_stat",
            "boot_id": boot_id,
            "start_time_ticks": start_ticks,
        }
        if not _valid_worker_process_identity(identity):
            return {"state": _pid_liveness(normalized_pid), "identity": None}
        return {"state": "alive", "identity": identity}
    if _pid_liveness(normalized_pid) == "exited":
        return {"state": "exited", "identity": None}
    try:
        completed = subprocess.run(
            ["ps", "-o", "stat=", "-o", "lstart=", "-p", str(normalized_pid)],
            check=False,
            capture_output=True,
            text=True,
            timeout=1.0,
            env={**os.environ, "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError):
        return {"state": _pid_liveness(normalized_pid), "identity": None}
    normalized_output = " ".join(completed.stdout.split())
    output_fields = normalized_output.split(maxsplit=1)
    if completed.returncode != 0 or len(output_fields) != 2:
        return {"state": _pid_liveness(normalized_pid), "identity": None}
    if output_fields[0].startswith(("X", "Z")):
        return {"state": "exited", "identity": None}
    identity = {
        "schema_version": WORKER_PROCESS_IDENTITY_SCHEMA_VERSION,
        "source": "ps_lstart",
        "start_time": output_fields[1],
    }
    if not _valid_worker_process_identity(identity):
        return {"state": _pid_liveness(normalized_pid), "identity": None}
    return {"state": "alive", "identity": identity}


def _worker_owner_record_valid(owner: Any) -> bool:
    if not isinstance(owner, dict):
        return False
    if owner.get("schema_version") != WORKER_OWNER_SCHEMA_VERSION:
        return False
    if _positive_process_pid(owner.get("pid")) is None:
        return False
    worker_instance_id = str(owner.get("worker_instance_id") or "")
    legacy_recovered = owner.get("legacy_status_recovered") is True
    if not re.fullmatch(r"[a-f0-9]{32}", worker_instance_id):
        if not (legacy_recovered and worker_instance_id == ""):
            return False
    identity = owner.get("process_identity")
    if identity is None:
        if not legacy_recovered:
            return False
    elif not _valid_worker_process_identity(identity):
        return False
    return _valid_worker_lease_identity(owner.get("lease_identity"))


def _worker_owner_record(
    *,
    pid: int,
    worker_instance_id: str,
    process_identity: dict[str, Any] | None,
    lease_identity: dict[str, int],
    legacy_status_recovered: bool,
) -> dict[str, Any]:
    normalized_instance = str(worker_instance_id or "")
    if legacy_status_recovered and not re.fullmatch(
        r"[a-f0-9]{32}", normalized_instance
    ):
        normalized_instance = ""
    record = {
        "schema_version": WORKER_OWNER_SCHEMA_VERSION,
        "pid": int(pid),
        "worker_instance_id": normalized_instance,
        "process_identity": process_identity,
        "lease_identity": lease_identity,
        "legacy_status_recovered": bool(legacy_status_recovered),
        "acquired_at": utc_iso(),
    }
    if not _worker_owner_record_valid(record):
        raise SnapshotWorkerOwnerInvalidError("record_contract")
    return record


def _write_snapshot_worker_owner(path: Path, owner: dict[str, Any]) -> None:
    if not _worker_owner_record_valid(owner):
        raise SnapshotWorkerOwnerInvalidError("write_contract")
    atomic_json(path, owner)
    fsync_directory(path.parent)


def _load_snapshot_worker_owner(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise SnapshotWorkerOwnerInvalidError("file_type")
    try:
        if path.stat().st_size > WORKER_OWNER_MAX_BYTES:
            raise SnapshotWorkerOwnerInvalidError("oversized")
        owner = json.loads(path.read_text(encoding="utf-8"))
    except SnapshotWorkerOwnerInvalidError:
        raise
    except Exception as exc:
        raise SnapshotWorkerOwnerInvalidError("unreadable") from exc
    if not _worker_owner_record_valid(owner):
        raise SnapshotWorkerOwnerInvalidError("contract")
    return owner


def _legacy_worker_owner_candidate(status: Any) -> dict[str, Any] | None:
    if not isinstance(status, dict):
        return None
    if status.get("schema_version") != WORKER_STATUS_SCHEMA_VERSION:
        return None
    if not any(
        status.get(field) is True
        for field in (
            "worker_restart_required",
            "cleanup_deferred_until_worker_restart",
            "running",
            "attempt_running",
        )
    ):
        return None
    pid = _positive_process_pid(status.get("pid"))
    if pid is None:
        return None
    worker_instance_id = str(status.get("worker_instance_id") or "")
    if pid == os.getpid() and worker_instance_id == WORKER_PROCESS_INSTANCE_ID:
        return None
    return {"pid": pid, "worker_instance_id": worker_instance_id}


def _interrupted_snapshot_partial_exists(out_root: Path) -> bool:
    snapshots = out_root / "snapshots"
    if not snapshots.is_dir():
        return False
    return any(
        PARTIAL_SNAPSHOT_NAME_RE.fullmatch(path.name)
        for path in snapshots.iterdir()
    )


def snapshot_partial_owner_path(partial_dir: Path) -> Path:
    return partial_dir / PARTIAL_OWNER_FILENAME


def _snapshot_id_from_partial_dir(partial_dir: Path) -> str | None:
    match = PARTIAL_SNAPSHOT_NAME_RE.fullmatch(partial_dir.name)
    if match is None:
        return None
    return partial_dir.name[1 : -len(".partial")]


def _partial_owner_record_valid(
    record: Any,
    *,
    expected_snapshot_id: str | None = None,
) -> bool:
    if not isinstance(record, dict):
        return False
    if record.get("schema_version") != PARTIAL_OWNER_SCHEMA_VERSION:
        return False
    snapshot_id = str(record.get("snapshot_id") or "")
    if not SNAPSHOT_NAME_RE.fullmatch(snapshot_id):
        return False
    if expected_snapshot_id is not None and snapshot_id != expected_snapshot_id:
        return False
    owner = record.get("owner")
    return bool(
        _worker_owner_record_valid(owner)
        and owner.get("legacy_status_recovered") is False
        and _valid_worker_process_identity(owner.get("process_identity"))
    )


def _write_snapshot_partial_owner(
    partial_dir: Path,
    *,
    snapshot_id: str,
    owner: dict[str, Any],
    bootstrap: bool = False,
) -> dict[str, Any]:
    if partial_dir.is_symlink() or not partial_dir.is_dir():
        raise SnapshotWorkerOwnerInvalidError("partial_owner_directory")
    if partial_dir.parent.name != "snapshots":
        raise SnapshotWorkerOwnerInvalidError("partial_owner_location")
    if bootstrap:
        expected_prefix = f".{snapshot_id}.partial-bootstrap-"
        if not (
            partial_dir.name.startswith(expected_prefix)
            and re.fullmatch(r"[a-f0-9]{8}", partial_dir.name[len(expected_prefix) :])
        ):
            raise SnapshotWorkerOwnerInvalidError(
                "partial_owner_bootstrap_name"
            )
    elif _snapshot_id_from_partial_dir(partial_dir) != snapshot_id:
        raise SnapshotWorkerOwnerInvalidError("partial_owner_snapshot_id")
    if not _worker_owner_record_valid(owner):
        raise SnapshotWorkerOwnerInvalidError("partial_owner_root_contract")
    current = snapshot_process_identity(os.getpid())
    current_identity = current.get("identity")
    if (
        current.get("state") != "alive"
        or not _valid_worker_process_identity(current_identity)
        or owner.get("pid") != os.getpid()
        or owner.get("worker_instance_id") != WORKER_PROCESS_INSTANCE_ID
        or owner.get("process_identity") != current_identity
        or owner.get("legacy_status_recovered") is not False
    ):
        raise SnapshotWorkerOwnerInvalidError("partial_owner_creator_mismatch")
    out_root = partial_dir.parent.parent.resolve()
    if owner.get("lease_identity") != _snapshot_worker_lease_identity(out_root):
        raise SnapshotWorkerOwnerInvalidError("partial_owner_lease_mismatch")
    record = {
        "schema_version": PARTIAL_OWNER_SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "owner": json.loads(json.dumps(owner)),
        "created_at": utc_iso(),
    }
    if not _partial_owner_record_valid(
        record,
        expected_snapshot_id=snapshot_id,
    ):
        raise SnapshotWorkerOwnerInvalidError("partial_owner_write_contract")
    path = snapshot_partial_owner_path(partial_dir)
    atomic_json(path, record)
    fsync_directory(partial_dir)
    return record


def _load_snapshot_partial_owner(partial_dir: Path) -> dict[str, Any]:
    snapshot_id = _snapshot_id_from_partial_dir(partial_dir)
    if snapshot_id is None:
        raise SnapshotWorkerOwnerInvalidError("partial_name_contract")
    marker_path = snapshot_partial_owner_path(partial_dir)
    if not marker_path.exists():
        raise SnapshotWorkerOwnerInvalidError("partial_owner_missing")
    if marker_path.is_symlink() or not marker_path.is_file():
        raise SnapshotWorkerOwnerInvalidError("partial_owner_file_type")
    try:
        if marker_path.stat().st_size > PARTIAL_OWNER_MAX_BYTES:
            raise SnapshotWorkerOwnerInvalidError("partial_owner_oversized")
        record = json.loads(marker_path.read_text(encoding="utf-8"))
    except SnapshotWorkerOwnerInvalidError:
        raise
    except Exception as exc:
        raise SnapshotWorkerOwnerInvalidError("partial_owner_unreadable") from exc
    if not _partial_owner_record_valid(
        record,
        expected_snapshot_id=snapshot_id,
    ):
        raise SnapshotWorkerOwnerInvalidError("partial_owner_contract")
    return record


def _authorize_snapshot_partial_cleanup(
    partial_dir: Path,
    *,
    expected_lease_identity: dict[str, int],
) -> tuple[dict[str, Any], tuple[int, int]]:
    if partial_dir.is_symlink() or not partial_dir.is_dir():
        raise SnapshotWorkerOwnerInvalidError("partial_file_type")
    directory_stat = partial_dir.stat()
    record = _load_snapshot_partial_owner(partial_dir)
    owner = record["owner"]
    if owner.get("lease_identity") != expected_lease_identity:
        raise SnapshotWorkerOwnerInvalidError("partial_owner_lease_mismatch")
    owner_pid = int(owner["pid"])
    stored_identity = owner["process_identity"]
    observed = snapshot_process_identity(owner_pid)
    observed_state = str(observed.get("state") or "unknown")
    observed_identity = observed.get("identity")
    identity_mismatch = bool(
        observed_state == "alive"
        and _valid_worker_process_identity(observed_identity)
        and observed_identity != stored_identity
    )
    if observed_state != "exited" and not identity_mismatch:
        liveness = (
            "partial_owner_alive_matching_identity"
            if observed_state == "alive"
            and observed_identity == stored_identity
            else "partial_owner_identity_unavailable"
        )
        raise PriorSnapshotWorkerActiveError(owner, liveness)
    return record, (int(directory_stat.st_dev), int(directory_stat.st_ino))


def ensure_snapshot_worker_owner(
    out_root: Path,
    *,
    legacy_statuses: tuple[dict[str, Any], ...] = (),
) -> dict[str, Any]:
    """Claim cleanup ownership only after any prior process has exited."""
    _acquire_snapshot_worker_lease(out_root)
    owner_path = snapshot_worker_owner_path(out_root)
    owner = _load_snapshot_worker_owner(owner_path)
    lease_identity = _snapshot_worker_lease_identity(out_root)
    if owner is not None:
        if owner.get("lease_identity") != lease_identity:
            raise SnapshotWorkerOwnerInvalidError("lease_identity_mismatch")
        owner_pid = int(owner["pid"])
        owner_instance = str(owner.get("worker_instance_id") or "")
        if (
            owner_pid == os.getpid()
            and owner_instance == WORKER_PROCESS_INSTANCE_ID
        ):
            current = snapshot_process_identity(os.getpid())
            current_identity = current.get("identity")
            if current.get("state") != "alive" or not _valid_worker_process_identity(
                current_identity
            ):
                raise RuntimeError("evaluator_snapshot_process_identity_unavailable")
            stored_current_identity = owner.get("process_identity")
            if (
                stored_current_identity is not None
                and stored_current_identity != current_identity
            ):
                raise SnapshotWorkerOwnerInvalidError(
                    "current_identity_mismatch"
                )
            if stored_current_identity is None:
                owner = _worker_owner_record(
                    pid=os.getpid(),
                    worker_instance_id=WORKER_PROCESS_INSTANCE_ID,
                    process_identity=current_identity,
                    lease_identity=lease_identity,
                    legacy_status_recovered=False,
                )
                _write_snapshot_worker_owner(owner_path, owner)
            return owner
        observed = snapshot_process_identity(owner_pid)
        observed_state = str(observed.get("state") or "unknown")
        observed_identity = observed.get("identity")
        stored_identity = owner.get("process_identity")
        identity_mismatch = bool(
            observed_state == "alive"
            and _valid_worker_process_identity(observed_identity)
            and _valid_worker_process_identity(stored_identity)
            and observed_identity != stored_identity
        )
        if observed_state != "exited" and not identity_mismatch:
            liveness = (
                "alive_matching_identity"
                if observed_state == "alive"
                and observed_identity == stored_identity
                and _valid_worker_process_identity(stored_identity)
                else "identity_unavailable"
            )
            raise PriorSnapshotWorkerActiveError(owner, liveness)
    else:
        if _interrupted_snapshot_partial_exists(out_root):
            raise SnapshotWorkerOwnerInvalidError(
                "missing_with_interrupted_partials"
            )
        for legacy_status in legacy_statuses:
            candidate = _legacy_worker_owner_candidate(legacy_status)
            if candidate is None:
                continue
            observed = snapshot_process_identity(int(candidate["pid"]))
            if observed.get("state") == "exited":
                continue
            identity = observed.get("identity")
            legacy_owner = _worker_owner_record(
                pid=int(candidate["pid"]),
                worker_instance_id=str(candidate.get("worker_instance_id") or ""),
                process_identity=(
                    identity if _valid_worker_process_identity(identity) else None
                ),
                lease_identity=lease_identity,
                legacy_status_recovered=True,
            )
            _write_snapshot_worker_owner(owner_path, legacy_owner)
            raise PriorSnapshotWorkerActiveError(
                legacy_owner,
                (
                    "alive_identity_captured"
                    if _valid_worker_process_identity(identity)
                    else "identity_unavailable"
                ),
            )
    current = snapshot_process_identity(os.getpid())
    current_identity = current.get("identity")
    if current.get("state") != "alive" or not _valid_worker_process_identity(
        current_identity
    ):
        raise RuntimeError("evaluator_snapshot_process_identity_unavailable")
    current_owner = _worker_owner_record(
        pid=os.getpid(),
        worker_instance_id=WORKER_PROCESS_INSTANCE_ID,
        process_identity=current_identity,
        lease_identity=lease_identity,
        legacy_status_recovered=False,
    )
    _write_snapshot_worker_owner(owner_path, current_owner)
    return current_owner


def shared_stage_budget_anchor_path(
    status_path: Path,
    attempt_id: Any,
) -> Path:
    normalized_attempt_id = str(attempt_id or "")
    if not SHARED_STAGE_ATTEMPT_ID_RE.fullmatch(normalized_attempt_id):
        raise ValueError("shared stage anchor attempt id invalid")
    return (
        status_path.parent
        / SHARED_STAGE_HISTORY_ANCHOR_DIRECTORY
        / f"{normalized_attempt_id}.json"
    )


def shared_stage_budget_anchor_payload(
    evidence: dict[str, Any],
) -> dict[str, Any]:
    attempt_id = str(evidence.get("attempt_id") or "")
    evidence_sha256 = str(evidence.get("evidence_sha256") or "")
    if not SHARED_STAGE_ATTEMPT_ID_RE.fullmatch(attempt_id):
        raise ValueError("shared stage anchor attempt id invalid")
    if (
        not re.fullmatch(r"[a-f0-9]{64}", evidence_sha256)
        or shared_stage_budget_evidence_sha256(evidence) != evidence_sha256
        or evidence.get("captured_before_cleanup") is not True
        or evidence.get("cleanup_completed") is not True
        or evidence.get("stage_files_removed") is not True
        or evidence.get("no_unregistered_stage_files") is not True
    ):
        raise ValueError("shared stage anchor evidence invalid")
    return {
        "schema_version": SHARED_STAGE_HISTORY_ANCHOR_SCHEMA_VERSION,
        "attempt_id": attempt_id,
        "evidence_sha256": evidence_sha256,
        "anchor_source": "atomic_worker_attempt_sidecar",
        "immutable": True,
    }


def write_shared_stage_budget_anchor(
    status_path: Path,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    anchor = shared_stage_budget_anchor_payload(evidence)
    anchor_path = shared_stage_budget_anchor_path(
        status_path,
        anchor["attempt_id"],
    )
    if anchor_path.exists():
        if read_json_object(anchor_path) != anchor:
            raise RuntimeError("shared_stage_history_anchor_collision")
        return anchor
    atomic_json(anchor_path, anchor)
    return anchor


def validated_shared_stage_budget_history(
    payload: Any,
    *,
    trusted_anchor: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {"accepted": False, "reason": "history_missing", "targets": {}}
    if payload.get("schema_version") != SHARED_STAGE_BUDGET_SCHEMA_VERSION:
        return {
            "accepted": False,
            "reason": "history_schema_mismatch",
            "targets": {},
        }
    if payload.get("hash_canonicalization") != SHARED_STAGE_HASH_CANONICALIZATION:
        return {
            "accepted": False,
            "reason": "history_hash_canonicalization_mismatch",
            "targets": {},
        }
    plan_sha256 = str(payload.get("plan_sha256") or "")
    try:
        computed_plan_sha256 = shared_stage_budget_plan_sha256(payload)
    except (TypeError, ValueError, OverflowError):
        computed_plan_sha256 = None
    if (
        not re.fullmatch(r"[a-f0-9]{64}", plan_sha256)
        or computed_plan_sha256 != plan_sha256
    ):
        return {
            "accepted": False,
            "reason": "history_plan_hash_invalid",
            "targets": {},
        }
    evidence_sha256 = str(payload.get("evidence_sha256") or "")
    try:
        computed_evidence_sha256 = shared_stage_budget_evidence_sha256(
            payload
        )
    except (TypeError, ValueError, OverflowError):
        computed_evidence_sha256 = None
    if (
        not re.fullmatch(r"[a-f0-9]{64}", evidence_sha256)
        or computed_evidence_sha256 != evidence_sha256
    ):
        return {
            "accepted": False,
            "reason": "history_evidence_hash_invalid",
            "targets": {},
        }
    attempt_id = str(payload.get("attempt_id") or "")
    if not SHARED_STAGE_ATTEMPT_ID_RE.fullmatch(attempt_id):
        return {
            "accepted": False,
            "reason": "history_attempt_id_invalid",
            "targets": {},
        }
    if not isinstance(trusted_anchor, dict):
        return {
            "accepted": False,
            "reason": "history_anchor_missing",
            "targets": {},
        }
    if (
        trusted_anchor.get("schema_version")
        != SHARED_STAGE_HISTORY_ANCHOR_SCHEMA_VERSION
        or trusted_anchor.get("anchor_source")
        != "atomic_worker_attempt_sidecar"
        or trusted_anchor.get("immutable") is not True
        or str(trusted_anchor.get("attempt_id") or "") != attempt_id
        or str(trusted_anchor.get("evidence_sha256") or "")
        != evidence_sha256
    ):
        return {
            "accepted": False,
            "reason": "history_anchor_mismatch",
            "targets": {},
        }
    try:
        history_total_cap = json_safe_integer(
            payload.get("total_cap_bytes"),
            field="history.total_cap_bytes",
        )
        history_total_granted = json_safe_integer(
            payload.get("total_granted_bytes"),
            field="history.total_granted_bytes",
        )
    except (TypeError, ValueError, OverflowError):
        return {
            "accepted": False,
            "reason": "history_total_invalid",
            "targets": {},
        }
    if (
        payload.get("allocation_mode")
        != SHARED_STAGE_BUDGET_ALLOCATION_MODE
        or payload.get("capacity_sufficient") is not True
        or payload.get("grants_sum_matches_total_cap") is not True
        or payload.get("all_advisory_queries_bounded") is not True
        or payload.get("physical_upper_bound_claimed") is not False
        or payload.get("global_hard_cap_enforced") is not True
        or payload.get("per_target_max_page_count_enforced") is not True
        or payload.get("fixed_percentage_allocation_used") is not False
        or history_total_cap <= 0
        or history_total_granted != history_total_cap
    ):
        return {
            "accepted": False,
            "reason": "history_plan_not_usable",
            "targets": {},
        }
    raw_targets = payload.get("targets")
    raw_active_targets = payload.get("active_targets")
    if not isinstance(raw_targets, dict) or not isinstance(raw_active_targets, list):
        return {
            "accepted": False,
            "reason": "history_shape_invalid",
            "targets": {},
        }
    known_targets = {
        SHARED_STAGE_TARGET_CANDIDATE,
        *PARALLEL_PAPER_STAGE_TABLES,
    }
    active_targets = tuple(str(target) for target in raw_active_targets)
    active_parallel_tables = active_targets[1:]
    if (
        not active_targets
        or active_targets[0] != SHARED_STAGE_TARGET_CANDIDATE
        or not parallel_paper_stage_inventory_valid(active_parallel_tables)
        or active_targets
        != shared_stage_target_names(active_parallel_tables)
        or len(active_targets) != len(set(active_targets))
        or any(target not in known_targets for target in active_targets)
        or set(raw_targets) != set(active_targets)
    ):
        return {
            "accepted": False,
            "reason": "history_inventory_invalid",
            "targets": {},
        }
    sanitized_targets: dict[str, dict[str, Any]] = {}
    for target in active_targets:
        raw = raw_targets.get(target)
        if not isinstance(raw, dict):
            return {
                "accepted": False,
                "reason": "history_target_invalid",
                "targets": {},
            }
        try:
            granted = json_safe_integer(
                raw.get("granted_cap_bytes"),
                field=f"history.targets.{target}.granted_cap_bytes",
            )
            raw_high_water = raw.get("high_water_bytes")
            if raw_high_water is None:
                raw_high_water = raw.get("actual_usage_bytes")
            high_water = json_safe_integer(
                raw_high_water,
                field=f"history.targets.{target}.high_water_bytes",
            )
        except (TypeError, ValueError, OverflowError):
            return {
                "accepted": False,
                "reason": "history_numeric_invalid",
                "targets": {},
            }
        copy_completed = raw.get("copy_completed") is True
        cap_hit = raw.get("cap_hit") is True
        sqlite_full_observed = raw.get("sqlite_full_observed") is True
        if (
            granted < shared_stage_target_minimum_bytes(target)
            or granted % SHARED_STAGE_PAGE_SIZE != 0
            or high_water < 0
            or high_water > granted
            or (
                cap_hit
                and (
                    copy_completed
                    or high_water
                    < max(0, granted - SHARED_STAGE_PAGE_SIZE)
                    and not sqlite_full_observed
                )
            )
        ):
            return {
                "accepted": False,
                "reason": "history_capacity_invalid",
                "targets": {},
            }
        sanitized_targets[target] = {
            "granted_cap_bytes": granted,
            "high_water_bytes": high_water,
            "copy_completed": copy_completed,
            "cap_hit": cap_hit,
            "sqlite_full_observed": sqlite_full_observed,
            "storage_schema_version": str(
                raw.get("storage_schema_version") or ""
            )[:80],
            "evidence_source": str(raw.get("evidence_source") or "")[:80],
        }
    if sum(
        int(report["granted_cap_bytes"])
        for report in sanitized_targets.values()
    ) != history_total_granted:
        return {
            "accepted": False,
            "reason": "history_total_invalid",
            "targets": {},
        }
    cleanup_completed = payload.get("cleanup_completed") is True
    stage_files_removed = payload.get("stage_files_removed") is True
    no_unregistered = payload.get("no_unregistered_stage_files") is True
    if payload.get("accepted") is True:
        cleanup_completed = payload.get("cleanup_completed") is True
        stage_files_removed = payload.get("stage_files_removed") is True
        no_unregistered = payload.get("no_unregistered_stage_files") is True
    accepted = bool(
        cleanup_completed
        and stage_files_removed
        and no_unregistered
    )
    evidence_sources = {
        str(report.get("evidence_source") or "")
        for report in sanitized_targets.values()
    }
    source_invariants_valid = bool(
        payload.get("captured_before_cleanup") is True
        and str(payload.get("captured_at") or "")
        and (
            (
                payload.get("accepted") is True
                and evidence_sources == {"accepted_producer_stage_report"}
                and all(
                    report.get("copy_completed") is True
                    and report.get("cap_hit") is not True
                    for report in sanitized_targets.values()
                )
            )
            or (
                payload.get("accepted") is not True
                and evidence_sources
                == {"partial_stage_files_before_cleanup"}
                and str(payload.get("failure_code") or "")
                and isinstance(payload.get("failure_components"), list)
                and bool(payload.get("failure_components"))
            )
        )
    )
    accepted = bool(accepted and source_invariants_valid)
    return {
        "accepted": accepted,
        "reason": (
            "history_accepted"
            if accepted
            else (
                "history_source_invariants_invalid"
                if cleanup_completed and stage_files_removed and no_unregistered
                else "history_cleanup_invalid"
            )
        ),
        "attempt_id": attempt_id,
        "evidence_sha256": evidence_sha256,
        "anchor_schema_version": trusted_anchor.get("schema_version"),
        "active_targets": list(active_targets),
        "targets": sanitized_targets if accepted else {},
    }


def shared_stage_budget_evidence_from_exception(
    exc: BaseException,
) -> dict[str, Any] | None:
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        evidence = getattr(current, "shared_stage_budget", None)
        if isinstance(evidence, dict):
            return evidence
        current = current.__cause__ or current.__context__
    return None


def bounded_error_text(exc: BaseException, limit: int = 4096) -> str:
    text = f"{type(exc).__name__}:{exc}"
    return text if len(text) <= limit else f"{text[:limit]}…"


def sqlite_error_identity(exc: BaseException) -> tuple[int | None, str | None]:
    """Return the first SQLite error identity in a bounded exception chain."""
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, sqlite3.Error):
            raw_code = getattr(current, "sqlite_errorcode", None)
            try:
                code = int(raw_code) if raw_code is not None else None
            except (TypeError, ValueError):
                code = None
            raw_name = getattr(current, "sqlite_errorname", None)
            name = str(raw_name) if raw_name else None
            return code, name
        current = current.__cause__ or current.__context__
    return None, None


def sqlite_primary_error_code(exc: BaseException) -> int | None:
    code, _name = sqlite_error_identity(exc)
    return (code & 0xFF) if code is not None else None


def sqlite_busy_or_locked(exc: BaseException) -> bool:
    return sqlite_primary_error_code(exc) in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}


def sqlite_full_error(exc: BaseException) -> bool:
    return sqlite_primary_error_code(exc) == sqlite3.SQLITE_FULL


class ConcurrentSnapshotError(RuntimeError):
    """Bounded multi-database failure with public-safe component details."""

    def __init__(self, errors: dict[str, dict[str, Any]]):
        self.errors = {}
        self.worker_restart_required = False
        for name, details in sorted((errors or {}).items()):
            details = details or {}
            bounded = {
                "error_code": str(details.get("error_code") or "snapshot_component_failed"),
                "error_type": str(details.get("error_type") or "Exception"),
                "stage": str(details.get("stage") or "unknown"),
            }
            copy_timing = details.get("copy_timing")
            if isinstance(copy_timing, dict):
                bounded["copy_timing"] = copy_timing
            sqlite_errorcode = details.get("sqlite_errorcode")
            sqlite_errorname = details.get("sqlite_errorname")
            if isinstance(sqlite_errorcode, int):
                bounded["sqlite_errorcode"] = sqlite_errorcode
            if isinstance(sqlite_errorname, str) and sqlite_errorname:
                bounded["sqlite_errorname"] = sqlite_errorname
            if details.get("worker_restart_required") is True:
                bounded["worker_restart_required"] = True
                self.worker_restart_required = True
            self.errors[str(name)] = bounded
        summary = ",".join(
            f"{details['error_code']}:{name}:{details['stage']}"
            for name, details in self.errors.items()
        )
        super().__init__(f"concurrent evaluator snapshot failed: {summary}")


def exception_requires_worker_restart(exc: BaseException) -> bool:
    if getattr(exc, "worker_restart_required", False) is True:
        return True
    if isinstance(exc, ConcurrentSnapshotError):
        return any(
            details.get("worker_restart_required") is True
            for details in exc.errors.values()
        )
    return False


def snapshot_component_failure_code(exc: BaseException) -> str:
    text = str(exc)
    known = (
        "evaluator_snapshot_prior_worker_active",
        "evaluator_snapshot_worker_owner_invalid",
        "evaluator_snapshot_process_identity_unavailable",
        "paper_source_journal_mode_mismatch",
        "paper_source_journal_mode_unavailable",
        "source_read_lock_budget_exceeded",
        "shared_stage_capacity_insufficient",
        "shared_stage_estimate_timeout",
        "shared_stage_estimate_query_plan_not_indexed",
        "shared_stage_estimate_columns_missing",
        "shared_stage_estimate_dbstat_unavailable",
        "shared_stage_estimate_dbstat_missing",
        "shared_stage_estimate_dbstat_invalid",
        "shared_stage_estimate_row_count_missing",
        "shared_stage_estimate_read_view_not_pinned",
        "shared_stage_estimate_read_view_identity_invalid",
        "shared_stage_estimate_duplicate_target",
        "shared_stage_estimate_barrier_broken",
        "shared_stage_estimate_candidate_signal_id_type_invalid",
        "shared_stage_estimate_candidate_order_index_missing",
        "shared_stage_estimate_candidate_order_index_invalid",
        "shared_stage_budget_plan_missing",
        "shared_stage_cleanup_failed",
        "parallel_paper_stage_column_contract_mismatch",
        "parallel_paper_stage_destination_schema_invalid",
        "parallel_paper_stage_destination_schema_mismatch",
        "parallel_paper_stage_generated_columns_unsupported",
        "parallel_paper_stage_integer_out_of_range",
        "parallel_paper_stage_non_finite_float",
        "parallel_paper_stage_value_type_unsupported",
        "parallel_paper_stage_row_column_count_mismatch",
        "parallel_paper_stage_encoded_row_too_large",
        "parallel_paper_stage_chunk_truncated",
        "parallel_paper_stage_text_invalid_utf8",
        "parallel_paper_stage_value_tag_invalid",
        "parallel_paper_stage_row_trailing_bytes",
        "parallel_paper_stage_chunk_trailing_bytes",
        "parallel_paper_stage_chunk_size_invalid",
        "parallel_paper_stage_chunk_decompression_invalid",
        "parallel_paper_stage_chunk_decompression_failed",
        "parallel_paper_stage_storage_contract_mismatch",
        "parallel_paper_stage_metadata_invalid",
        "parallel_paper_stage_producer_evidence_mismatch",
        "parallel_paper_stage_chunk_sequence_invalid",
        "parallel_paper_stage_chunk_integrity_failed",
        "parallel_paper_stage_row_digest_mismatch",
        "parallel_stage_table_columns_missing",
        "parallel_stage_duplicate_columns",
        "parallel_stage_table_missing",
        "parallel_stage_destination_collision",
        "selective_snapshot_source_query_plan_not_indexed",
        "selective_snapshot_source_index_missing",
        "selective_snapshot_epoch_seconds_type_invalid",
        "selective_snapshot_index_anchor_missing",
        "selective_snapshot_index_anchor_unit_missing",
        "snapshot_source_read_lock_timeout",
        "selective snapshot exceeded database budget",
        "snapshot source inspection failed",
        "snapshot_source_watermark_query_plan_not_indexed",
        "snapshot missing required tables",
        "snapshot missing required watermarks",
        "candidate_observation_payload_projection_semantic_mismatch",
        "parallel_paper_stage_start_timeout",
        "parallel_paper_stage_cancelled",
        "parallel_paper_stage_barrier_broken",
        "parallel_paper_stage_timeout",
        "parallel_paper_stage_missing",
        "parallel_paper_stage_budget_exceeded",
        "parallel_paper_stage_quick_check_failed",
        "parallel_paper_stage_row_count_mismatch",
        "parallel_paper_stage_cleanup_failed",
        "parallel_paper_stage_failed",
        "paper_decision_parallel_stage_start_timeout",
        "paper_decision_parallel_stage_cancelled",
        "paper_decision_parallel_stage_barrier_broken",
        "paper_decision_parallel_stage_timeout",
        "paper_decision_parallel_stage_missing",
        "paper_decision_parallel_stage_budget_exceeded",
        "paper_decision_parallel_stage_quick_check_failed",
        "paper_decision_parallel_stage_row_count_mismatch",
        "paper_decision_parallel_stage_cleanup_failed",
        "paper_decision_parallel_stage_failed",
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
    failure_count_value = status.get("consecutive_failure_code_count")
    if failure_count_value is None:
        failure_count_value = status.get("consecutive_failure_count")
    try:
        consecutive_failures = max(1, int(failure_count_value or 1))
    except (TypeError, ValueError):
        consecutive_failures = 1
    if consecutive_failures == 1:
        return first_retry
    if consecutive_failures == 2:
        return max(first_retry, SECOND_FAILURE_RETRY_SEC)
    if consecutive_failures == 3:
        return max(first_retry, THIRD_FAILURE_RETRY_SEC)
    return max(first_retry, SUSTAINED_FAILURE_RETRY_SEC, success_interval)


CONCURRENT_SNAPSHOT_FALLOUT_CODES = frozenset(
    {
        "BrokenBarrierError",
        "parallel_paper_stage_barrier_broken",
        "parallel_paper_stage_cancelled",
        "paper_decision_parallel_stage_barrier_broken",
        "paper_decision_parallel_stage_cancelled",
    }
)


def snapshot_failure_code(exc: BaseException) -> str:
    text = str(exc)
    if isinstance(exc, ConcurrentSnapshotError):
        component_codes = [
            str(details.get("error_code") or "snapshot_component_failed")
            for details in exc.errors.values()
        ]
        causal_codes = {
            code
            for code in component_codes
            if code not in CONCURRENT_SNAPSHOT_FALLOUT_CODES
        }
        if len(causal_codes) == 1:
            return next(iter(causal_codes))
        return "concurrent_evaluator_snapshot_failed"
    known = (
        "evaluator_snapshot_lock_held",
        "evaluator_snapshot_prior_worker_active",
        "evaluator_snapshot_worker_owner_invalid",
        "evaluator_snapshot_process_identity_unavailable",
        "paper_source_journal_mode_mismatch",
        "paper_source_journal_mode_unavailable",
        "source_read_lock_budget_exceeded",
        "shared_stage_capacity_insufficient",
        "shared_stage_estimate_timeout",
        "shared_stage_estimate_query_plan_not_indexed",
        "shared_stage_estimate_columns_missing",
        "shared_stage_estimate_dbstat_unavailable",
        "shared_stage_estimate_dbstat_missing",
        "shared_stage_estimate_dbstat_invalid",
        "shared_stage_estimate_row_count_missing",
        "shared_stage_estimate_read_view_not_pinned",
        "shared_stage_estimate_read_view_identity_invalid",
        "shared_stage_estimate_duplicate_target",
        "shared_stage_estimate_barrier_broken",
        "shared_stage_estimate_candidate_signal_id_type_invalid",
        "shared_stage_estimate_candidate_order_index_missing",
        "shared_stage_estimate_candidate_order_index_invalid",
        "shared_stage_budget_plan_missing",
        "shared_stage_cleanup_failed",
        "parallel_paper_stage_column_contract_mismatch",
        "parallel_paper_stage_destination_schema_invalid",
        "parallel_paper_stage_destination_schema_mismatch",
        "parallel_paper_stage_generated_columns_unsupported",
        "parallel_paper_stage_integer_out_of_range",
        "parallel_paper_stage_non_finite_float",
        "parallel_paper_stage_value_type_unsupported",
        "parallel_paper_stage_row_column_count_mismatch",
        "parallel_paper_stage_encoded_row_too_large",
        "parallel_paper_stage_chunk_truncated",
        "parallel_paper_stage_text_invalid_utf8",
        "parallel_paper_stage_value_tag_invalid",
        "parallel_paper_stage_row_trailing_bytes",
        "parallel_paper_stage_chunk_trailing_bytes",
        "parallel_paper_stage_chunk_size_invalid",
        "parallel_paper_stage_chunk_decompression_invalid",
        "parallel_paper_stage_chunk_decompression_failed",
        "parallel_paper_stage_storage_contract_mismatch",
        "parallel_paper_stage_metadata_invalid",
        "parallel_paper_stage_producer_evidence_mismatch",
        "parallel_paper_stage_chunk_sequence_invalid",
        "parallel_paper_stage_chunk_integrity_failed",
        "parallel_paper_stage_row_digest_mismatch",
        "parallel_stage_table_columns_missing",
        "parallel_stage_duplicate_columns",
        "parallel_stage_table_missing",
        "parallel_stage_destination_collision",
        "selective_snapshot_source_index_missing",
        "selective_snapshot_epoch_seconds_type_invalid",
        "selective_snapshot_index_anchor_missing",
        "selective_snapshot_index_anchor_unit_missing",
        "snapshot_source_read_lock_timeout",
        "snapshot source inspection failed",
        "snapshot_source_inspection_failed",
        "insufficient disk for evaluator snapshot",
        "concurrent evaluator snapshot failed",
        "cross-database snapshot acceptance failed",
        "selective snapshot exceeded bundle output cap",
        "snapshot manifest size did not converge",
        "parallel_paper_stage_start_timeout",
        "parallel_paper_stage_cancelled",
        "parallel_paper_stage_barrier_broken",
        "parallel_paper_stage_timeout",
        "parallel_paper_stage_missing",
        "parallel_paper_stage_budget_exceeded",
        "parallel_paper_stage_quick_check_failed",
        "parallel_paper_stage_row_count_mismatch",
        "parallel_paper_stage_cleanup_failed",
        "parallel_paper_stage_failed",
        "paper_decision_parallel_stage_start_timeout",
        "paper_decision_parallel_stage_cancelled",
        "paper_decision_parallel_stage_barrier_broken",
        "paper_decision_parallel_stage_timeout",
        "paper_decision_parallel_stage_missing",
        "paper_decision_parallel_stage_budget_exceeded",
        "paper_decision_parallel_stage_quick_check_failed",
        "paper_decision_parallel_stage_row_count_mismatch",
        "paper_decision_parallel_stage_cleanup_failed",
        "paper_decision_parallel_stage_failed",
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
    shared_budget = (
        manifest.get("shared_stage_budget")
        if isinstance(manifest.get("shared_stage_budget"), dict)
        else {}
    )
    shared_targets = (
        shared_budget.get("targets")
        if isinstance(shared_budget.get("targets"), dict)
        else {}
    )
    shared_budget_summary = {
        "schema_version": shared_budget.get("schema_version"),
        "attempt_id": shared_budget.get("attempt_id"),
        "allocation_mode": shared_budget.get("allocation_mode"),
        "plan_sha256": shared_budget.get("plan_sha256"),
        "active_targets": shared_budget.get("active_targets") or [],
        "total_cap_bytes": shared_budget.get("total_cap_bytes"),
        "total_granted_bytes": shared_budget.get("total_granted_bytes"),
        "actual_total_bytes": shared_budget.get("actual_total_bytes"),
        "unconsumed_bytes": shared_budget.get("unconsumed_bytes"),
        "capacity_sufficient": shared_budget.get("capacity_sufficient") is True,
        "history_used": shared_budget.get("history_used") is True,
        "fixed_percentage_allocation_used": (
            shared_budget.get("fixed_percentage_allocation_used") is True
        ),
        "cleanup_completed": shared_budget.get("cleanup_completed") is True,
        "no_unregistered_stage_files": (
            shared_budget.get("no_unregistered_stage_files") is True
        ),
        "targets": {
            str(target): {
                "granted_cap_bytes": report.get("granted_cap_bytes"),
                "actual_usage_bytes": report.get("actual_usage_bytes"),
                "high_water_bytes": report.get("high_water_bytes"),
                "copy_completed": report.get("copy_completed") is True,
                "cap_hit": report.get("cap_hit") is True,
                "within_grant": report.get("within_grant") is True,
            }
            for target, report in shared_targets.items()
            if isinstance(report, dict)
        },
    }
    return {
        "schema_version": manifest.get("schema_version"),
        EVIDENCE_SCHEMA_VERSION_FIELD: manifest.get(
            EVIDENCE_SCHEMA_VERSION_FIELD
        ),
        EVIDENCE_SCHEMA_SHA256_FIELD: manifest.get(
            EVIDENCE_SCHEMA_SHA256_FIELD
        ),
        EVIDENCE_SCHEMA_VALIDATED_FIELD: manifest.get(
            EVIDENCE_SCHEMA_VALIDATED_FIELD
        )
        is True,
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
        "shared_stage_budget": shared_budget_summary,
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


def snapshot_directory_report(
    path: Path,
    *,
    include_manifest: bool,
    allow_partial_owner: bool = False,
) -> dict[str, Any]:
    allowed = set(SNAPSHOT_DATABASE_FILENAMES)
    if include_manifest:
        allowed.add("manifest.json")
    entries = list(path.iterdir()) if path.is_dir() else []
    files = [
        item
        for item in entries
        if item.is_file()
        and not (
            allow_partial_owner and item.name == PARTIAL_OWNER_FILENAME
        )
    ]
    actual_names = {item.name for item in files}
    unexpected = sorted(
        item.name
        for item in entries
        if not (
            allow_partial_owner
            and item.is_file()
            and not item.is_symlink()
            and item.name == PARTIAL_OWNER_FILENAME
        )
        and (not item.is_file() or item.name not in allowed)
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


def stage_file_usage(path: Path) -> dict[str, Any]:
    logical_size = 0
    allocated_size = 0
    sidecars: dict[str, int] = {}
    for suffix in ("", "-journal", "-wal", "-shm"):
        candidate = Path(f"{path}{suffix}")
        if not candidate.is_file():
            continue
        details = candidate.stat()
        size = max(0, int(details.st_size))
        logical_size += size
        allocated = max(
            0,
            int(getattr(details, "st_blocks", 0) or 0) * 512,
        )
        allocated_size += allocated or size
        sidecars[candidate.name] = size
    return {
        "file_present": bool(sidecars),
        "logical_size_bytes": logical_size,
        "allocated_size_bytes": allocated_size,
        "high_water_bytes": max(logical_size, allocated_size),
        "file_count": len(sidecars),
        "sidecar_sizes": sidecars,
    }


def capture_shared_stage_budget_failure(
    partial_dir: Path,
    plan: dict[str, Any] | None,
    exc: BaseException,
) -> dict[str, Any] | None:
    if not isinstance(plan, dict):
        return None
    targets = plan.get("targets")
    active_targets = plan.get("active_targets")
    if not isinstance(targets, dict) or not isinstance(active_targets, list):
        return None
    evidence = json.loads(json.dumps(plan))
    evidence["accepted"] = False
    evidence["captured_at"] = utc_iso()
    evidence["captured_before_cleanup"] = True
    evidence["cleanup_completed"] = False
    failure_code = snapshot_failure_code(exc)
    failure_details = snapshot_failure_details(exc)
    evidence["failure_code"] = failure_code
    evidence["failure_components"] = sorted(failure_details)
    expected_stage_files: set[str] = set()
    actual_total = 0
    for target in active_targets:
        report = evidence["targets"].get(target) or {}
        filename = str(report.get("stage_filename") or "")
        if not filename:
            continue
        expected_stage_files.update(
            {filename, f"{filename}-journal", f"{filename}-wal", f"{filename}-shm"}
        )
        usage = stage_file_usage(partial_dir / filename)
        high_water = int(usage["high_water_bytes"])
        actual_total += high_water
        grant = max(0, int(report.get("granted_cap_bytes") or 0))
        advisory = max(0, int(report.get("advisory_required_bytes") or 0))
        copy_completed = False
        explicit_budget_failure = False
        targeted_sqlite_full = False
        for details in failure_details.values():
            code = str(details.get("error_code") or "")
            component_targets_stage = shared_stage_failure_targets_target(
                target,
                details,
            )
            try:
                sqlite_errorcode = int(details.get("sqlite_errorcode"))
            except (TypeError, ValueError, OverflowError):
                sqlite_errorcode = None
            if (
                component_targets_stage
                and sqlite_errorcode is not None
                and sqlite_errorcode & 0xFF == sqlite3.SQLITE_FULL
            ):
                targeted_sqlite_full = True
            copy_timing = details.get("copy_timing")
            completed = (
                copy_timing.get("completed_tables")
                if isinstance(copy_timing, dict)
                else None
            )
            completed_parallel = (
                copy_timing.get("completed_parallel_stages")
                if isinstance(copy_timing, dict)
                else None
            )
            if (
                target == SHARED_STAGE_TARGET_CANDIDATE
                and isinstance(completed, dict)
                and CANDIDATE_OBSERVATION_TABLE in completed
            ):
                copy_completed = True
            if (
                target != SHARED_STAGE_TARGET_CANDIDATE
                and isinstance(completed_parallel, list)
                and target in completed_parallel
            ):
                copy_completed = True
            if component_targets_stage and code in {
                "parallel_paper_stage_budget_exceeded",
                "paper_decision_parallel_stage_budget_exceeded",
                "selective_snapshot_exceeded_database_budget",
            }:
                explicit_budget_failure = True
        report.update(
            {
                "actual_usage_bytes": high_water,
                "high_water_bytes": high_water,
                "logical_high_water_bytes": int(usage["logical_size_bytes"]),
                "allocated_high_water_bytes": int(
                    usage["allocated_size_bytes"]
                ),
                "file_present": usage["file_present"],
                "file_count": usage["file_count"],
                "copy_completed": copy_completed,
                "sqlite_full_observed": targeted_sqlite_full,
                "advisory_exceeded": high_water > advisory,
                "advisory_delta_bytes": high_water - advisory,
                "cap_hit": bool(
                    not copy_completed
                    and explicit_budget_failure
                    and grant > 0
                    and (
                        targeted_sqlite_full
                        or high_water
                        >= max(0, grant - SHARED_STAGE_PAGE_SIZE)
                    )
                ),
                "evidence_source": "partial_stage_files_before_cleanup",
                "within_grant": high_water <= grant if grant > 0 else False,
                "utilization_ratio": (
                    round(high_water / grant, 6) if grant > 0 else None
                ),
            }
        )
        evidence["targets"][target] = report
    unregistered = sorted(
        item.name
        for item in partial_dir.iterdir()
        if item.is_file()
        and SHARED_STAGE_HIDDEN_FILE_RE.fullmatch(item.name)
        and item.name not in expected_stage_files
    ) if partial_dir.is_dir() else []
    evidence["actual_total_bytes"] = actual_total
    evidence["unconsumed_bytes"] = max(
        0,
        int(evidence.get("total_cap_bytes") or 0) - actual_total,
    )
    evidence["all_targets_within_grant"] = all(
        (report or {}).get("within_grant") is True
        for report in evidence["targets"].values()
    )
    evidence["targets_exceeding_advisory"] = [
        target
        for target in active_targets
        if (evidence["targets"].get(target) or {}).get(
            "advisory_exceeded"
        )
        is True
    ]
    evidence["advisory_miss_count"] = len(
        evidence["targets_exceeding_advisory"]
    )
    evidence["unregistered_stage_files"] = unregistered
    evidence["no_unregistered_stage_files"] = not unregistered
    return evidence


def finalize_shared_stage_budget_success(
    plan: dict[str, Any],
    paper_report: dict[str, Any],
) -> dict[str, Any]:
    evidence = json.loads(json.dumps(plan))
    targets = evidence.get("targets") or {}
    candidate_usage = max(
        0,
        int(paper_report.get("temporary_candidate_stage_size_bytes") or 0),
    )
    actual_total = 0
    for target in evidence.get("active_targets") or []:
        report = targets.get(target) or {}
        if target == SHARED_STAGE_TARGET_CANDIDATE:
            actual = candidate_usage
            actual_rows_copied = max(
                0,
                int(
                    ((paper_report.get("selected_tables") or {}).get(
                        CANDIDATE_OBSERVATION_TABLE
                    ) or {}).get("rows_copied")
                    or 0
                ),
            )
        else:
            stage_report = (
                (paper_report.get("parallel_paper_stages") or {}).get(target)
                or {}
            )
            actual = max(
                0,
                int(stage_report.get("stage_size_bytes") or 0),
            )
            actual_rows_copied = max(
                0,
                int(stage_report.get("rows_copied") or 0),
            )
        grant = max(0, int(report.get("granted_cap_bytes") or 0))
        advisory = max(0, int(report.get("advisory_required_bytes") or 0))
        advisory_evidence = report.get("advisory_evidence") or {}
        advisory_rows = max(
            0,
            int(advisory_evidence.get("selected_row_count") or 0),
        )
        row_count_binding_mode = str(
            advisory_evidence.get("row_count_binding_mode") or ""
        )
        row_count_bound = bool(
            (
                row_count_binding_mode == "exact_selected_rows"
                and actual_rows_copied == advisory_rows
            )
            or (
                row_count_binding_mode == "full_source_row_upper"
                and actual_rows_copied <= advisory_rows
            )
            or (
                row_count_binding_mode
                == SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ROW_BINDING_MODE
                and actual_rows_copied >= 0
            )
        )
        actual_total += actual
        report.update(
            {
                "actual_usage_bytes": actual,
                "high_water_bytes": actual,
                "actual_rows_copied": actual_rows_copied,
                "row_count_bound_to_snapshot": row_count_bound,
                "advisory_exceeded": actual > advisory,
                "advisory_delta_bytes": actual - advisory,
                "copy_completed": True,
                "cap_hit": False,
                "within_grant": grant > 0 and actual <= grant,
                "utilization_ratio": (
                    round(actual / grant, 6) if grant > 0 else None
                ),
                "evidence_source": "accepted_producer_stage_report",
            }
        )
        targets[target] = report
    evidence["targets"] = targets
    evidence["actual_total_bytes"] = actual_total
    evidence["unconsumed_bytes"] = max(
        0,
        int(evidence.get("total_cap_bytes") or 0) - actual_total,
    )
    evidence["all_targets_within_grant"] = all(
        (report or {}).get("within_grant") is True
        for report in targets.values()
    )
    evidence["targets_exceeding_advisory"] = [
        target
        for target in evidence.get("active_targets") or []
        if (targets.get(target) or {}).get("advisory_exceeded") is True
    ]
    evidence["advisory_miss_count"] = len(
        evidence["targets_exceeding_advisory"]
    )
    evidence["all_target_row_counts_bound_to_snapshot"] = all(
        (report or {}).get("row_count_bound_to_snapshot") is True
        for report in targets.values()
    )
    evidence["captured_at"] = utc_iso()
    evidence["captured_before_cleanup"] = True
    evidence["cleanup_completed"] = True
    evidence["stage_files_removed"] = True
    evidence["unregistered_stage_files"] = []
    evidence["no_unregistered_stage_files"] = True
    evidence["accepted"] = bool(
        plan.get("accepted") is True
        and evidence["all_targets_within_grant"] is True
        and evidence["all_target_row_counts_bound_to_snapshot"] is True
        and evidence["actual_total_bytes"]
        <= int(evidence.get("total_cap_bytes") or 0)
    )
    evidence["evidence_sha256"] = shared_stage_budget_evidence_sha256(
        evidence
    )
    return evidence


def write_bounded_manifest(
    partial_dir: Path,
    manifest: dict[str, Any],
    *,
    output_cap_bytes: int,
    allow_partial_owner: bool = False,
) -> dict[str, Any]:
    manifest_path = partial_dir / "manifest.json"
    manifest["manifest_size_bytes"] = 0
    manifest["output_size_bytes"] = int(manifest.get("database_payload_size_bytes") or 0)
    for _attempt in range(8):
        require_numeric_evidence_schema(manifest, require_binding=True)
        atomic_json(manifest_path, manifest)
        directory = snapshot_directory_report(
            partial_dir,
            include_manifest=True,
            allow_partial_owner=allow_partial_owner,
        )
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
            require_numeric_evidence_schema(manifest, require_binding=True)
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


def source_journal_mode(
    path: Path,
    *,
    busy_timeout_ms: int = 30000,
) -> str:
    connection = readonly_connection(path, busy_timeout_ms=busy_timeout_ms)
    try:
        row = connection.execute("PRAGMA journal_mode").fetchone()
        mode = str(row[0] if row else "").strip().upper()
    finally:
        connection.close()
    if not mode:
        raise RuntimeError("paper_source_journal_mode_unavailable")
    return mode


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
    indexed_watermark_anchors: bool = False,
    progress: dict[str, str] | None = None,
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
    watermark_query_evidence = {}
    for table, candidates in spec["watermarks"].items():
        if progress is not None:
            progress["stage"] = f"source_metadata:{table}"
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
        rule = (spec.get("tables") or {}).get(table) or {}
        indexed_anchor = rule.get("indexed_epoch_seconds_anchor")
        if indexed_watermark_anchors:
            if not indexed_anchor:
                watermarks[table] = {}
                watermark_query_evidence[table] = {
                    "strategy": "deferred_to_frozen_snapshot",
                    "columns": selected,
                    "source_query_executed": False,
                }
                continue
            if indexed_anchor not in selected:
                if table in spec["required_tables"]:
                    missing_required_watermarks.append(table)
                watermark_query_evidence[table] = {
                    "strategy": "indexed_anchor_column_missing",
                    "column": indexed_anchor,
                    "source_index_name": None,
                    "query_plan": [],
                    "uses_declared_index": False,
                    "full_table_scan_detected": None,
                }
                continue
            source_index_name = source_index_for_column(
                connection,
                table,
                str(indexed_anchor),
                schema=schema,
            )
            if not source_index_name:
                if table in spec["required_tables"]:
                    missing_required_watermarks.append(table)
                watermark_query_evidence[table] = {
                    "strategy": "indexed_anchor_missing",
                    "column": indexed_anchor,
                    "source_index_name": None,
                    "query_plan": [],
                    "uses_declared_index": False,
                    "full_table_scan_detected": None,
                }
                continue
            table_reference = (
                f"{schema}.{quote_identifier(table)} "
                f"INDEXED BY {quote_identifier(source_index_name)}"
            )
            query = (
                f"SELECT MAX({quote_identifier(str(indexed_anchor))}) AS value "
                f"FROM {table_reference}"
            )
            query_plan = [
                str(row[3])
                for row in connection.execute(f"EXPLAIN QUERY PLAN {query}")
            ]
            uses_index = any(source_index_name in detail for detail in query_plan)
            full_scan = any(
                "SCAN" in detail.upper() and source_index_name not in detail
                for detail in query_plan
            )
            if not uses_index or full_scan:
                raise RuntimeError(
                    f"snapshot_source_watermark_query_plan_not_indexed:{table}:"
                    f"{indexed_anchor}:{source_index_name}"
                )
            row = connection.execute(query).fetchone()
            watermarks[table] = {str(indexed_anchor): row["value"]}
            watermark_query_evidence[table] = {
                "strategy": "indexed_anchor_max",
                "column": str(indexed_anchor),
                "source_index_name": source_index_name,
                "query_plan": query_plan,
                "uses_declared_index": True,
                "full_table_scan_detected": False,
            }
            continue
        expressions = ", ".join(
            f"MAX({quote_identifier(column)}) AS {quote_identifier(column)}"
            for column in selected
        )
        row = connection.execute(
            f"SELECT {expressions} FROM {schema}.{quote_identifier(table)}"
        ).fetchone()
        watermarks[table] = {column: row[column] for column in selected}
        watermark_query_evidence[table] = {
            "strategy": "aggregate_max",
            "columns": selected,
        }
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
        "watermark_query_evidence": watermark_query_evidence,
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
) -> dict[str, dict[str, Any]]:
    reports = {}
    for name, source in source_paths.items():
        if not source.is_file():
            raise FileNotFoundError(source)
        connection = readonly_connection(source, busy_timeout_ms=busy_timeout_ms)
        try:
            try:
                report: dict[str, Any] = source_page_stats(connection, source)
                if name == "paper":
                    report["parallel_paper_stage_tables"] = list(
                        parallel_paper_stage_tables_for_schema(
                            connection,
                            schema="main",
                        )
                    )
                reports[name] = report
            except RuntimeError as exc:
                raise ConcurrentSnapshotError({
                    name: {
                        "error_code": snapshot_component_failure_code(exc),
                        "error_type": type(exc).__name__,
                        "stage": "source_metadata",
                    }
                }) from exc
            except sqlite3.Error as exc:
                sqlite_errorcode, sqlite_errorname = sqlite_error_identity(exc)
                details: dict[str, Any] = {
                    "error_code": (
                        "snapshot_source_read_lock_timeout"
                        if sqlite_busy_or_locked(exc)
                        else "snapshot_source_inspection_failed"
                    ),
                    "error_type": type(exc).__name__,
                    "stage": "source_page_stats",
                }
                if sqlite_errorcode is not None:
                    details["sqlite_errorcode"] = sqlite_errorcode
                if sqlite_errorname:
                    details["sqlite_errorname"] = sqlite_errorname
                raise ConcurrentSnapshotError({name: details}) from exc
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
    *,
    schema: str = "src",
) -> str | None:
    candidates: list[tuple[int, str]] = []
    schema = _schema_prefix(schema)
    for row in connection.execute(
        f"PRAGMA {schema}.index_list({quote_identifier(table)})"
    ):
        if len(row) > 4 and int(row[4] or 0):
            continue
        name = str(row[1])
        columns = [
            str(index_row[2])
            for index_row in connection.execute(
                f"PRAGMA {schema}.index_info({quote_identifier(name)})"
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


def integer_primary_key_rowid_alias(
    connection: sqlite3.Connection,
    table: str,
) -> str | None:
    if str(table) not in HEAVY_PARALLEL_ROWID_RANGE_TABLES:
        return None
    schema_row = connection.execute(
        "SELECT sql FROM src.sqlite_master WHERE type='table' AND name=?",
        (str(table),),
    ).fetchone()
    create_sql = str(schema_row[0] or "") if schema_row is not None else ""
    if "WITHOUT ROWID" in create_sql.upper():
        return None
    primary_key_rows = [
        row
        for row in connection.execute(
            f"PRAGMA src.table_info({quote_identifier(str(table))})"
        )
        if int(row["pk"] or 0) > 0
    ]
    if len(primary_key_rows) != 1:
        return None
    row = primary_key_rows[0]
    if str(row["type"] or "").strip().upper() != "INTEGER":
        return None
    return str(row["name"])


def indexed_time_bounds_rowid_copy_plan(
    connection: sqlite3.Connection,
    table: str,
    selection: dict[str, Any],
) -> dict[str, Any] | None:
    rowid_alias = integer_primary_key_rowid_alias(connection, table)
    if not rowid_alias or not selection.get("source_index_name"):
        return None
    alias = quote_identifier(rowid_alias)
    boundary_row = connection.execute(
        f"SELECT MIN({alias}), MAX({alias}) "
        f"FROM {source_table_reference(table, selection)} "
        f"WHERE {selection['predicate_sql']}",
        selection["parameters"],
    ).fetchone()
    if (
        boundary_row is None
        or boundary_row[0] is None
        or boundary_row[1] is None
    ):
        return None
    lower = int(boundary_row[0])
    upper = int(boundary_row[1])
    if lower > upper:
        raise RuntimeError(
            f"selective_snapshot_source_query_plan_not_indexed:{table}:"
            "invalid_rowid_bounds"
        )
    relation = f"src.{quote_identifier(str(table))} NOT INDEXED"
    predicate_sql = (
        f"{alias} >= ? AND {alias} <= ? "
        f"AND ({selection['predicate_sql']})"
    )
    parameters = [lower, upper, *selection["parameters"]]
    plan_rows = connection.execute(
        f"EXPLAIN QUERY PLAN SELECT {alias} FROM {relation} "
        f"WHERE {predicate_sql} LIMIT 1",
        parameters,
    ).fetchall()
    details = [str(row[3]) for row in plan_rows]
    table_token = str(table).lower()
    uses_integer_primary_key_range = any(
        "search" in detail.lower()
        and "integer primary key" in detail.lower()
        and "rowid>" in detail.lower()
        and "rowid<" in detail.lower()
        for detail in details
    )
    full_table_scan_detected = any(
        "scan" in detail.lower() and table_token in detail.lower()
        for detail in details
    )
    if not uses_integer_primary_key_range or full_table_scan_detected:
        raise RuntimeError(
            f"selective_snapshot_source_query_plan_not_indexed:{table}:"
            f"rowid_range:{details}"
        )
    return {
        "strategy": "indexed_time_bounds_then_rowid_range",
        "relation": relation,
        "predicate_sql": predicate_sql,
        "parameters": parameters,
        "rowid_alias": rowid_alias,
        "rowid_lower": lower,
        "rowid_upper": upper,
        "rowid_span": upper - lower + 1,
        "query_plan": details,
        "query_plan_uses_integer_primary_key_range": True,
        "query_plan_full_table_scan_detected": False,
        "time_predicate_rechecked": True,
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


def bounded_sqlite_value_bytes(value: Any) -> int:
    if value is None:
        return 1
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return 8
    return len(str(value).encode("utf-8"))


def ensure_source_dbstat(connection: sqlite3.Connection) -> str:
    """Bind DBSTAT explicitly to the attached ``src`` schema.

    The eponymous ``dbstat('src', 1)`` form can report the main database page
    size for an attached database on some SQLite builds. A named TEMP virtual
    table created with ``USING dbstat(src)`` keeps the schema binding and page
    accounting unambiguous.
    """
    table_name = SOURCE_DBSTAT_VIRTUAL_TABLE
    exists = connection.execute(
        "SELECT 1 FROM temp.sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    if exists is None:
        connection.execute(
            f"CREATE VIRTUAL TABLE temp.{quote_identifier(table_name)} "
            "USING dbstat(src)"
        )
    return table_name


def source_table_storage_report(
    connection: sqlite3.Connection,
    table: str,
) -> dict[str, int]:
    """Return an explicitly aggregated DBSTAT physical upper-bound basis."""
    dbstat_table = ensure_source_dbstat(connection)
    row = connection.execute(
        "SELECT COUNT(*) AS page_count, "
        "COALESCE(SUM(payload), 0) AS payload_bytes, "
        "COALESCE(SUM(unused), 0) AS unused_bytes, "
        "COALESCE(MAX(mx_payload), 0) AS max_payload_bytes, "
        "COALESCE(SUM(pgsize), 0) AS physical_bytes, "
        "COALESCE(SUM(ncell), 0) AS cell_upper_count, "
        "COALESCE(MIN(pgsize), 0) AS min_page_size, "
        "COALESCE(MAX(pgsize), 0) AS max_page_size "
        f"FROM temp.{quote_identifier(dbstat_table)} WHERE name=?",
        (table,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"shared_stage_estimate_dbstat_missing:{table}")
    page_count = max(0, int(row["page_count"] or 0))
    payload_bytes = max(0, int(row["payload_bytes"] or 0))
    unused_bytes = max(0, int(row["unused_bytes"] or 0))
    max_payload_bytes = max(0, int(row["max_payload_bytes"] or 0))
    physical_bytes = max(0, int(row["physical_bytes"] or 0))
    cell_upper_count = max(0, int(row["cell_upper_count"] or 0))
    min_page_size = max(0, int(row["min_page_size"] or 0))
    max_page_size = max(0, int(row["max_page_size"] or 0))
    source_page_size = int(
        connection.execute("PRAGMA src.page_size").fetchone()[0] or 0
    )
    if physical_bytes <= 0 or page_count <= 0:
        raise RuntimeError(
            f"shared_stage_estimate_dbstat_invalid:{table}:empty_btree"
        )
    if (
        source_page_size < 512
        or source_page_size > 65536
        or source_page_size & (source_page_size - 1)
        or min_page_size != source_page_size
        or max_page_size != source_page_size
        or physical_bytes != page_count * source_page_size
    ):
        raise RuntimeError(
            f"shared_stage_estimate_dbstat_invalid:{table}:page_accounting"
        )
    if payload_bytes > physical_bytes or unused_bytes > physical_bytes:
        raise RuntimeError(
            f"shared_stage_estimate_dbstat_invalid:{table}:payload_exceeds_physical"
        )
    if max_payload_bytes > payload_bytes:
        raise RuntimeError(
            f"shared_stage_estimate_dbstat_invalid:{table}:max_payload_invalid"
        )
    return {
        "page_count": page_count,
        "page_size": source_page_size,
        "physical_bytes": physical_bytes,
        "payload_bytes": payload_bytes,
        "unused_bytes": unused_bytes,
        "max_payload_bytes": max_payload_bytes,
        "cell_upper_count": cell_upper_count,
        "structural_overhead_bytes": physical_bytes - payload_bytes,
    }


def source_table_storage_bytes(
    connection: sqlite3.Connection,
    table: str,
) -> int:
    return source_table_storage_report(connection, table)["physical_bytes"]


def shared_stage_advisory_demand(
    *,
    target: str,
    selected_row_count: int,
    source_row_count_upper: int,
    storage: dict[str, int],
    candidate_order_index_storage: dict[str, int] | None = None,
) -> dict[str, int]:
    """Return a deterministic allocation hint, never a physical size proof.

    The source btree physical size is scaled by the selected-row fraction and
    padded with explicit per-row and root-page allowances. SQLite record packing
    can still make the destination larger, so callers must enforce safety with
    the global stage cap and each file's ``max_page_count``. A cap hit is valid
    fail-closed evidence for the next attempt's high-water allocation.
    """
    rows = max(0, int(selected_row_count))
    source_rows = max(0, int(source_row_count_upper))
    if rows > source_rows:
        raise RuntimeError("shared_stage_advisory_row_count_invalid")
    minimum = shared_stage_target_minimum_bytes(target)

    def scaled_physical(physical_bytes: int) -> int:
        physical = max(0, int(physical_bytes))
        if rows <= 0 or source_rows <= 0:
            return 0
        return (physical * rows + source_rows - 1) // source_rows

    root_reserve = (
        SHARED_STAGE_PAGE_SIZE * SHARED_STAGE_ADVISORY_ROOT_RESERVE_PAGES
    )
    table_scaled_physical = scaled_physical(storage["physical_bytes"])
    table_row_overhead = rows * SHARED_STAGE_ADVISORY_ROW_OVERHEAD_BYTES
    table_advisory = round_up_stage_page(
        max(minimum, table_scaled_physical + table_row_overhead + root_reserve)
    )

    candidate_index_scaled_physical = 0
    candidate_index_row_overhead = 0
    candidate_index_advisory = 0
    if target == SHARED_STAGE_TARGET_CANDIDATE:
        if not isinstance(candidate_order_index_storage, dict):
            raise RuntimeError(
                "shared_stage_estimate_candidate_order_index_missing"
            )
        index_cells = max(
            0,
            int(candidate_order_index_storage["cell_upper_count"]),
        )
        if index_cells != source_rows:
            raise RuntimeError(
                "shared_stage_estimate_candidate_order_index_invalid"
            )
        candidate_index_scaled_physical = scaled_physical(
            candidate_order_index_storage["physical_bytes"]
        )
        candidate_index_row_overhead = (
            rows * SHARED_STAGE_ADVISORY_INDEX_OVERHEAD_BYTES
        )
        candidate_index_advisory = round_up_stage_page(
            candidate_index_scaled_physical
            + candidate_index_row_overhead
            + root_reserve
        )

    advisory_required = round_up_stage_page(
        max(minimum, table_advisory + candidate_index_advisory)
    )
    return {
        "source_row_fraction_numerator": rows,
        "source_row_fraction_denominator": source_rows,
        "table_scaled_physical_advisory_bytes": table_scaled_physical,
        "table_row_overhead_advisory_bytes": table_row_overhead,
        "table_root_reserve_advisory_bytes": root_reserve,
        "table_advisory_bytes": table_advisory,
        "candidate_order_index_scaled_physical_advisory_bytes": (
            candidate_index_scaled_physical
        ),
        "candidate_order_index_row_overhead_advisory_bytes": (
            candidate_index_row_overhead
        ),
        "candidate_order_index_advisory_bytes": candidate_index_advisory,
        "advisory_required_bytes": advisory_required,
    }


def shared_stage_sample_advisory_demand(
    *,
    target: str,
    selected_row_count: int,
    sample_rows: int,
    sample_max_row_bytes: int | None,
) -> dict[str, int]:
    """Return a bounded sample allocation hint, never a size proof."""
    rows = max(0, int(selected_row_count))
    sampled = max(0, int(sample_rows))
    sample_basis = max(0, int(sample_max_row_bytes or 0))
    if sampled > SHARED_STAGE_ESTIMATE_SAMPLE_ROWS:
        raise RuntimeError("shared_stage_sample_advisory_sample_limit_invalid")
    if rows > 0 and (sampled <= 0 or sample_basis <= 0):
        raise RuntimeError("shared_stage_sample_advisory_sample_missing")
    minimum = shared_stage_target_minimum_bytes(target)
    root_reserve = (
        SHARED_STAGE_PAGE_SIZE * SHARED_STAGE_ADVISORY_ROOT_RESERVE_PAGES
    )
    table_sample_payload = rows * sample_basis
    table_row_overhead = rows * SHARED_STAGE_ADVISORY_ROW_OVERHEAD_BYTES
    table_advisory = round_up_stage_page(
        max(
            minimum,
            table_sample_payload + table_row_overhead + root_reserve,
        )
    )
    candidate_index_row_overhead = (
        rows * SHARED_STAGE_ADVISORY_INDEX_OVERHEAD_BYTES
        if str(target) == SHARED_STAGE_TARGET_CANDIDATE
        else 0
    )
    candidate_index_advisory = (
        round_up_stage_page(candidate_index_row_overhead + root_reserve)
        if str(target) == SHARED_STAGE_TARGET_CANDIDATE
        else 0
    )
    advisory_required = round_up_stage_page(
        max(minimum, table_advisory + candidate_index_advisory)
    )
    values = {
        "sample_row_bytes_basis": sample_basis,
        "table_sample_payload_advisory_bytes": table_sample_payload,
        "table_scaled_physical_advisory_bytes": 0,
        "table_row_overhead_advisory_bytes": table_row_overhead,
        "table_root_reserve_advisory_bytes": root_reserve,
        "table_advisory_bytes": table_advisory,
        "candidate_order_index_scaled_physical_advisory_bytes": 0,
        "candidate_order_index_row_overhead_advisory_bytes": (
            candidate_index_row_overhead
        ),
        "candidate_order_index_advisory_bytes": candidate_index_advisory,
        "advisory_required_bytes": advisory_required,
    }
    if any(value > SHARED_STAGE_MAX_SAFE_INTEGER for value in values.values()):
        raise RuntimeError("shared_stage_sample_advisory_numeric_overflow")
    return values


def exact_indexed_selected_row_count(
    connection: sqlite3.Connection,
    relation: str,
    selection: dict[str, Any],
) -> int:
    """Count the selected index range; the caller supplies the hard deadline."""
    return int(
        connection.execute(
            f"SELECT COUNT(*) FROM {relation} "
            f"WHERE {selection['predicate_sql']}",
            selection["parameters"],
        ).fetchone()[0]
    )


def estimate_shared_stage_target_requirement(
    connection: sqlite3.Connection,
    target: str,
    *,
    review_lower_epoch: float,
    long_lower_epoch: float,
    upper_epoch: float,
    pinned_read_view: dict[str, Any] | None = None,
    lock_deadline_monotonic: float | None = None,
    cancel_event: threading.Event | None = None,
) -> dict[str, Any]:
    """Estimate one stage from the same pinned source view used for its copy.

    When ``pinned_read_view`` is supplied, the caller must already hold a read
    transaction on the attached ``src`` database.  The returned evidence is
    bound to that read-view identity and is therefore not a pre-copy guess from
    a different database state.
    """
    target = str(target)
    table = (
        CANDIDATE_OBSERVATION_TABLE
        if target == SHARED_STAGE_TARGET_CANDIDATE
        else target
    )
    if target != SHARED_STAGE_TARGET_CANDIDATE and target not in PARALLEL_PAPER_STAGE_CONFIGS:
        raise ValueError(f"unknown shared stage estimate target: {target}")
    if pinned_read_view is not None:
        if not connection.in_transaction:
            raise RuntimeError("shared_stage_estimate_read_view_not_pinned")
        read_view_id = str(pinned_read_view.get("read_view_id") or "")
        read_view_role = str(pinned_read_view.get("role") or "")
        if not re.fullmatch(r"[a-f0-9]{32}", read_view_id) or not read_view_role:
            raise RuntimeError("shared_stage_estimate_read_view_identity_invalid")
    else:
        read_view_id = None
        read_view_role = None

    started = time.monotonic()
    estimate_deadline = started + SHARED_STAGE_ESTIMATE_TIMEOUT_SEC
    indexed_count_deadline: float | None = None
    dbstat_deadline: float | None = None

    def estimate_interrupted() -> int:
        if cancel_event is not None and cancel_event.is_set():
            return 1
        now = time.monotonic()
        if lock_deadline_monotonic is not None and now >= lock_deadline_monotonic:
            return 1
        if (
            indexed_count_deadline is not None
            and now >= indexed_count_deadline
        ):
            return 1
        if dbstat_deadline is not None and now >= dbstat_deadline:
            return 1
        return 1 if now >= estimate_deadline else 0

    connection.set_progress_handler(estimate_interrupted, 10000)
    try:
        rule = DATABASE_SPECS["paper"]["tables"][table]
        selection = selection_for_table(
            connection,
            table,
            rule,
            review_lower_epoch=review_lower_epoch,
            long_lower_epoch=long_lower_epoch,
            upper_epoch=upper_epoch,
        )
        plan = source_query_plan_evidence(connection, table, selection)
        if plan["required"] and (
            plan["uses_declared_index"] is not True
            or plan["uses_range_search"] is not True
            or plan["full_table_scan_detected"] is True
        ):
            raise RuntimeError(
                f"shared_stage_estimate_query_plan_not_indexed:{table}"
            )
        column_rows = list(
            connection.execute(
                f"PRAGMA src.table_info({quote_identifier(table)})"
            ).fetchall()
        )
        columns = [str(row["name"]) for row in column_rows]
        declared_types = {
            str(row["name"]): str(row["type"] or "")
            for row in column_rows
        }
        if not columns:
            raise RuntimeError(f"shared_stage_estimate_columns_missing:{table}")

        candidate_order_source_index_name = None
        candidate_order_source_index_columns: list[str] = []
        candidate_order_index_storage: dict[str, int] | None = None
        if target == SHARED_STAGE_TARGET_CANDIDATE:
            if not declared_numeric_timestamp_type(
                declared_types.get("signal_id", "")
            ):
                raise RuntimeError(
                    "shared_stage_estimate_candidate_signal_id_type_invalid"
                )
            candidate_order_source_index_name = source_index_for_column(
                connection,
                table,
                "signal_id",
            )
            if not candidate_order_source_index_name:
                raise RuntimeError(
                    "shared_stage_estimate_candidate_order_index_missing"
                )
            candidate_order_source_index_columns = source_index_columns(
                connection,
                candidate_order_source_index_name,
            )
            if candidate_order_source_index_columns != ["signal_id"]:
                raise RuntimeError(
                    "shared_stage_estimate_candidate_order_index_invalid"
                )

        relation = source_table_reference(table, selection)
        selected_rows: int | None = None
        total_rows: int | None = None
        row_count_upper_basis = "table_dbstat_cell_upper"
        sample_rows = 0
        average_row_bytes: float | None = None
        sample_max_row_bytes: int | None = None
        indexed_count_timed_out = False
        indexed_count_elapsed_sec: float | None = None
        if selection.get("source_index_name"):
            column_sql = ", ".join(
                quote_identifier(column) for column in columns
            )
            anchor = quote_identifier(str(selection["indexed_time_anchor"]))
            per_edge_limit = max(
                1,
                SHARED_STAGE_ESTIMATE_SAMPLE_ROWS // 2,
            )
            sampled = []
            for direction in ("ASC", "DESC"):
                sampled.extend(
                    connection.execute(
                        f"SELECT {column_sql} FROM {relation} "
                        f"WHERE {selection['predicate_sql']} "
                        f"ORDER BY {anchor} {direction} "
                        f"LIMIT {per_edge_limit}",
                        selection["parameters"],
                    ).fetchall()
                )
            sample_rows = len(sampled)
            if sample_rows:
                diagnostic_sizes = [
                    sum(bounded_sqlite_value_bytes(value) for value in row)
                    + len(columns) * 2
                    + 24
                    for row in sampled
                ]
                average_row_bytes = sum(diagnostic_sizes) / sample_rows
                sample_max_row_bytes = max(diagnostic_sizes)
            indexed_count_started = time.monotonic()
            indexed_count_deadline = min(
                estimate_deadline,
                indexed_count_started
                + SHARED_STAGE_INDEXED_COUNT_TIMEOUT_SEC,
                (
                    float(lock_deadline_monotonic)
                    if lock_deadline_monotonic is not None
                    else estimate_deadline
                ),
            )
            try:
                selected_rows = exact_indexed_selected_row_count(
                    connection,
                    relation,
                    selection,
                )
            except sqlite3.OperationalError as exc:
                now = time.monotonic()
                if (
                    "interrupted" in str(exc).lower()
                    and now >= indexed_count_deadline
                    and now < estimate_deadline
                    and (
                        lock_deadline_monotonic is None
                        or now < lock_deadline_monotonic
                    )
                    and not (
                        cancel_event is not None and cancel_event.is_set()
                    )
                ):
                    indexed_count_timed_out = True
                    selected_rows = None
                    row_count_upper_basis = (
                        "unavailable_after_bounded_index_count_timeout"
                    )
                else:
                    raise
            finally:
                indexed_count_elapsed_sec = round(
                    time.monotonic() - indexed_count_started,
                    6,
                )
                indexed_count_deadline = None
                connection.set_progress_handler(
                    estimate_interrupted,
                    10000,
                )

        storage: dict[str, int] | None = None
        dbstat_timed_out = False
        dbstat_completed = False
        dbstat_elapsed_sec = 0.0
        dbstat_skipped_reason: str | None = None
        if indexed_count_timed_out:
            dbstat_skipped_reason = "indexed_count_timeout"
        else:
            dbstat_started = time.monotonic()
            dbstat_deadline = min(
                estimate_deadline,
                dbstat_started + SHARED_STAGE_DBSTAT_ADVISORY_TIMEOUT_SEC,
                (
                    float(lock_deadline_monotonic)
                    if lock_deadline_monotonic is not None
                    else estimate_deadline
                ),
            )
            try:
                storage = source_table_storage_report(connection, table)
                if candidate_order_source_index_name:
                    candidate_order_index_storage = source_table_storage_report(
                        connection,
                        candidate_order_source_index_name,
                    )
                    total_rows = int(
                        candidate_order_index_storage["cell_upper_count"]
                    )
                    row_count_upper_basis = "exact_signal_index_entry_count"
                else:
                    total_rows = int(storage["cell_upper_count"])
            except sqlite3.OperationalError as exc:
                now = time.monotonic()
                if (
                    "interrupted" in str(exc).lower()
                    and selection.get("source_index_name")
                    and now >= dbstat_deadline
                    and now < estimate_deadline
                    and (
                        lock_deadline_monotonic is None
                        or now < lock_deadline_monotonic
                    )
                    and not (
                        cancel_event is not None and cancel_event.is_set()
                    )
                ):
                    dbstat_timed_out = True
                    storage = None
                    candidate_order_index_storage = None
                    total_rows = None
                    row_count_upper_basis = (
                        "not_required_for_bounded_index_sample_advisory"
                    )
                else:
                    raise
            finally:
                dbstat_elapsed_sec = round(
                    time.monotonic() - dbstat_started,
                    6,
                )
                dbstat_deadline = None
                connection.set_progress_handler(
                    estimate_interrupted,
                    10000,
                )
            dbstat_completed = not dbstat_timed_out

        if indexed_count_timed_out:
            estimate_strategy = (
                SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_STRATEGY
            )
            advisory_schema_version = (
                SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_SCHEMA_VERSION
            )
            advisory_formula = (
                SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_FORMULA
            )
            capacity_sample_used = True
        elif dbstat_timed_out:
            estimate_strategy = SHARED_STAGE_SAMPLE_ADVISORY_STRATEGY
            advisory_schema_version = (
                SHARED_STAGE_SAMPLE_ADVISORY_SCHEMA_VERSION
            )
            advisory_formula = SHARED_STAGE_SAMPLE_ADVISORY_FORMULA
            capacity_sample_used = True
        else:
            if not isinstance(storage, dict):
                raise RuntimeError(
                    f"shared_stage_estimate_dbstat_missing:{table}"
                )
            if selection.get("source_index_name"):
                estimate_strategy = (
                    "dbstat_proportional_advisory_with_indexed_row_count"
                )
            else:
                estimate_strategy = "dbstat_full_btree_advisory_demand"
                selected_rows = total_rows
            advisory_schema_version = SHARED_STAGE_ADVISORY_SCHEMA_VERSION
            advisory_formula = SHARED_STAGE_ADVISORY_FORMULA
            capacity_sample_used = False

        if selected_rows is None and not indexed_count_timed_out:
            raise RuntimeError(
                f"shared_stage_estimate_row_count_missing:{table}"
            )
        if indexed_count_timed_out:
            advisory = shared_stage_sample_advisory_demand(
                target=target,
                selected_row_count=sample_rows,
                sample_rows=sample_rows,
                sample_max_row_bytes=sample_max_row_bytes,
            )
        elif dbstat_timed_out:
            advisory = shared_stage_sample_advisory_demand(
                target=target,
                selected_row_count=int(selected_rows or 0),
                sample_rows=sample_rows,
                sample_max_row_bytes=sample_max_row_bytes,
            )
        else:
            advisory = shared_stage_advisory_demand(
                target=target,
                selected_row_count=int(selected_rows or 0),
                source_row_count_upper=int(total_rows or 0),
                storage=storage,
                candidate_order_index_storage=candidate_order_index_storage,
            )
        advisory_bytes = int(advisory["advisory_required_bytes"])
        minimum = shared_stage_target_minimum_bytes(target)
        return {
            "target": target,
            "source_table": table,
            "strategy": estimate_strategy,
            "query_bounded": True,
            "physical_upper_bound_claimed": False,
            "advisory_schema_version": advisory_schema_version,
            "advisory_formula": advisory_formula,
            "capacity_sample_used": capacity_sample_used,
            "indexed_count_completed": (
                selection.get("source_index_name") is not None
                and not indexed_count_timed_out
            ),
            "indexed_count_timed_out": indexed_count_timed_out,
            "indexed_count_timeout_sec": (
                SHARED_STAGE_INDEXED_COUNT_TIMEOUT_SEC
            ),
            "indexed_count_elapsed_sec": indexed_count_elapsed_sec,
            "dbstat_completed": dbstat_completed,
            "dbstat_timed_out": dbstat_timed_out,
            "dbstat_timeout_sec": SHARED_STAGE_DBSTAT_ADVISORY_TIMEOUT_SEC,
            "dbstat_elapsed_sec": dbstat_elapsed_sec,
            "dbstat_skipped_reason": dbstat_skipped_reason,
            "source_measurement_trust_boundary": (
                "same_pinned_read_view_as_copy"
                if pinned_read_view is not None
                else "standalone_diagnostic_connection"
            ),
            "pinned_read_view_id": read_view_id,
            "pinned_read_view_role": read_view_role,
            "estimate_started_after_pin": pinned_read_view is not None,
            "estimate_completed_before_copy": pinned_read_view is not None,
            "row_count_binding_mode": (
                SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ROW_BINDING_MODE
                if indexed_count_timed_out
                else (
                    "exact_selected_rows"
                    if selection.get("source_index_name")
                    else "full_source_row_upper"
                )
            ),
            "sample_limit_rows": SHARED_STAGE_ESTIMATE_SAMPLE_ROWS,
            "selected_row_count": selected_rows,
            "sample_row_count_advisory_basis": (
                sample_rows if indexed_count_timed_out else None
            ),
            "source_row_count_upper": total_rows,
            "source_row_count_upper_basis": row_count_upper_basis,
            "sample_rows": sample_rows,
            "average_row_bytes_diagnostic": (
                round(average_row_bytes, 3)
                if average_row_bytes is not None
                else None
            ),
            "sample_max_row_bytes_diagnostic": sample_max_row_bytes,
            "source_dbstat_page_count": (
                storage["page_count"] if storage else None
            ),
            "source_dbstat_page_size": (
                storage["page_size"] if storage else None
            ),
            "source_dbstat_physical_bytes": (
                storage["physical_bytes"] if storage else None
            ),
            "source_dbstat_payload_bytes": (
                storage["payload_bytes"] if storage else None
            ),
            "source_dbstat_unused_bytes": (
                storage["unused_bytes"] if storage else None
            ),
            "source_dbstat_max_payload_bytes": (
                storage["max_payload_bytes"] if storage else None
            ),
            "source_dbstat_cell_upper_count": (
                storage["cell_upper_count"] if storage else None
            ),
            "advisory_row_overhead_bytes": (
                SHARED_STAGE_ADVISORY_ROW_OVERHEAD_BYTES
            ),
            "advisory_index_overhead_bytes": (
                SHARED_STAGE_ADVISORY_INDEX_OVERHEAD_BYTES
            ),
            "advisory_root_reserve_pages": (
                SHARED_STAGE_ADVISORY_ROOT_RESERVE_PAGES
            ),
            **advisory,
            "candidate_order_source_index_name": (
                candidate_order_source_index_name
            ),
            "candidate_order_source_index_columns": (
                candidate_order_source_index_columns
            ),
            "candidate_order_source_index_partial": (
                False if candidate_order_source_index_name else None
            ),
            "candidate_order_source_index_dbstat_page_count": (
                candidate_order_index_storage["page_count"]
                if candidate_order_index_storage
                else None
            ),
            "candidate_order_source_index_dbstat_page_size": (
                candidate_order_index_storage["page_size"]
                if candidate_order_index_storage
                else None
            ),
            "candidate_order_source_index_dbstat_physical_bytes": (
                candidate_order_index_storage["physical_bytes"]
                if candidate_order_index_storage
                else None
            ),
            "candidate_order_source_index_dbstat_payload_bytes": (
                candidate_order_index_storage["payload_bytes"]
                if candidate_order_index_storage
                else None
            ),
            "candidate_order_source_index_dbstat_unused_bytes": (
                candidate_order_index_storage["unused_bytes"]
                if candidate_order_index_storage
                else None
            ),
            "candidate_order_source_index_dbstat_max_payload_bytes": (
                candidate_order_index_storage["max_payload_bytes"]
                if candidate_order_index_storage
                else None
            ),
            "candidate_order_source_index_dbstat_cell_upper_count": (
                candidate_order_index_storage["cell_upper_count"]
                if candidate_order_index_storage
                else None
            ),
            "candidate_order_source_index_structural_overhead_bytes": (
                candidate_order_index_storage["structural_overhead_bytes"]
                if candidate_order_index_storage
                else None
            ),
            "advisory_required_bytes": advisory_bytes,
            "minimum_cap_bytes": minimum,
            "source_index_name": selection.get("source_index_name"),
            "source_query_plan": plan["details"],
            "source_query_plan_uses_index": plan["uses_declared_index"],
            "source_query_plan_uses_range_search": plan["uses_range_search"],
            "source_query_plan_full_table_scan_detected": plan[
                "full_table_scan_detected"
            ],
            "elapsed_sec": round(time.monotonic() - started, 6),
        }
    except sqlite3.OperationalError as exc:
        error_text = str(exc).lower()
        if cancel_event is not None and cancel_event.is_set():
            raise RuntimeError("parallel_paper_stage_cancelled") from exc
        if (
            lock_deadline_monotonic is not None
            and time.monotonic() >= lock_deadline_monotonic
        ):
            raise RuntimeError(
                f"source_read_lock_budget_exceeded:paper:"
                f"shared_stage_estimate:{table}"
            ) from exc
        if "dbstat" in error_text and (
            "no such table" in error_text or "no such module" in error_text
        ):
            raise RuntimeError(
                f"shared_stage_estimate_dbstat_unavailable:{table}"
            ) from exc
        if "interrupted" in error_text:
            raise RuntimeError(
                f"shared_stage_estimate_timeout:{table}"
            ) from exc
        if sqlite_busy_or_locked(exc):
            raise RuntimeError(
                f"snapshot_source_read_lock_timeout:paper:"
                f"shared_stage_estimate:{table}"
            ) from exc
        raise
    finally:
        connection.set_progress_handler(None, 0)


def estimate_shared_stage_requirements(
    source: Path,
    *,
    parallel_stage_tables: tuple[str, ...],
    review_lower_epoch: float,
    long_lower_epoch: float,
    upper_epoch: float,
    busy_timeout_ms: int,
) -> dict[str, Any]:
    active_targets = shared_stage_target_names(parallel_stage_tables)
    timeout_sec = max(0.001, float(busy_timeout_ms) / 1000.0)
    connection = sqlite3.connect(":memory:", timeout=timeout_sec, uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute(f"PRAGMA busy_timeout={max(0, int(busy_timeout_ms))}")
    source_uri = f"file:{quote(str(source.resolve()), safe='/')}?mode=ro"
    connection.execute("ATTACH DATABASE ? AS src", (source_uri,))
    estimates: dict[str, dict[str, Any]] = {}
    try:
        for target in active_targets:
            table = (
                CANDIDATE_OBSERVATION_TABLE
                if target == SHARED_STAGE_TARGET_CANDIDATE
                else target
            )
            rule = DATABASE_SPECS["paper"]["tables"][table]
            selection = selection_for_table(
                connection,
                table,
                rule,
                review_lower_epoch=review_lower_epoch,
                long_lower_epoch=long_lower_epoch,
                upper_epoch=upper_epoch,
            )
            plan = source_query_plan_evidence(connection, table, selection)
            if plan["required"] and (
                plan["uses_declared_index"] is not True
                or plan["uses_range_search"] is not True
                or plan["full_table_scan_detected"] is True
            ):
                raise RuntimeError(
                    f"shared_stage_estimate_query_plan_not_indexed:{table}"
                )
            column_rows = list(
                connection.execute(
                    f"PRAGMA src.table_info({quote_identifier(table)})"
                ).fetchall()
            )
            columns = [str(row["name"]) for row in column_rows]
            declared_types = {
                str(row["name"]): str(row["type"] or "")
                for row in column_rows
            }
            if not columns:
                raise RuntimeError(f"shared_stage_estimate_columns_missing:{table}")
            candidate_order_source_index_name = None
            candidate_order_source_index_columns: list[str] = []
            candidate_order_index_storage: dict[str, int] | None = None
            if target == SHARED_STAGE_TARGET_CANDIDATE:
                if not declared_numeric_timestamp_type(
                    declared_types.get("signal_id", "")
                ):
                    raise RuntimeError(
                        "shared_stage_estimate_candidate_signal_id_type_invalid"
                    )
                candidate_order_source_index_name = source_index_for_column(
                    connection,
                    table,
                    "signal_id",
                )
                if not candidate_order_source_index_name:
                    raise RuntimeError(
                        "shared_stage_estimate_candidate_order_index_missing"
                    )
                candidate_order_source_index_columns = source_index_columns(
                    connection,
                    candidate_order_source_index_name,
                )
                if candidate_order_source_index_columns != ["signal_id"]:
                    raise RuntimeError(
                        "shared_stage_estimate_candidate_order_index_invalid"
                    )
            started = time.monotonic()
            deadline = started + SHARED_STAGE_ESTIMATE_TIMEOUT_SEC

            def estimate_timeout() -> int:
                return 1 if time.monotonic() >= deadline else 0

            connection.set_progress_handler(estimate_timeout, 10000)
            relation = source_table_reference(table, selection)
            selected_rows: int | None = None
            total_rows: int | None = None
            sample_rows = 0
            average_row_bytes: float | None = None
            sample_max_row_bytes: int | None = None
            estimate_strategy = "dbstat_proportional_advisory_with_indexed_row_count"
            storage: dict[str, int] = {}
            try:
                storage = source_table_storage_report(connection, table)
                if candidate_order_source_index_name:
                    candidate_order_index_storage = source_table_storage_report(
                        connection,
                        candidate_order_source_index_name,
                    )
                    # Rowid-table DBSTAT ncell includes interior separator cells.
                    # A single-column ordinary index has exactly one entry per
                    # source row across its full btree, so use that entry count
                    # as the candidate source-row upper/exact count.
                    total_rows = int(
                        candidate_order_index_storage["cell_upper_count"]
                    )
                    row_count_upper_basis = "exact_signal_index_entry_count"
                else:
                    total_rows = int(storage["cell_upper_count"])
                    row_count_upper_basis = "table_dbstat_cell_upper"
                if selection.get("source_index_name"):
                    selected_rows = int(
                        connection.execute(
                            f"SELECT COUNT(*) FROM {relation} "
                            f"WHERE {selection['predicate_sql']}",
                            selection["parameters"],
                        ).fetchone()[0]
                    )
                    column_sql = ", ".join(
                        quote_identifier(column) for column in columns
                    )
                    anchor = quote_identifier(
                        str(selection["indexed_time_anchor"])
                    )
                    per_edge_limit = max(
                        1,
                        SHARED_STAGE_ESTIMATE_SAMPLE_ROWS // 2,
                    )
                    sampled = []
                    for direction in ("ASC", "DESC"):
                        sampled.extend(
                            connection.execute(
                                f"SELECT {column_sql} FROM {relation} "
                                f"WHERE {selection['predicate_sql']} "
                                f"ORDER BY {anchor} {direction} "
                                f"LIMIT {per_edge_limit}",
                                selection["parameters"],
                            ).fetchall()
                        )
                    sample_rows = len(sampled)
                    if sample_rows:
                        diagnostic_sizes = [
                            sum(bounded_sqlite_value_bytes(value) for value in row)
                            + len(columns) * 2
                            + 24
                            for row in sampled
                        ]
                        average_row_bytes = sum(diagnostic_sizes) / sample_rows
                        sample_max_row_bytes = max(diagnostic_sizes)
                else:
                    estimate_strategy = "dbstat_full_btree_advisory_demand"
                    # Without a validated range index, the advisory demand uses
                    # the full source btree and its cell-count upper bound.
                    selected_rows = total_rows
            except sqlite3.OperationalError as exc:
                error_text = str(exc).lower()
                if "dbstat" in error_text and (
                    "no such table" in error_text
                    or "no such module" in error_text
                ):
                    raise RuntimeError(
                        f"shared_stage_estimate_dbstat_unavailable:{table}"
                    ) from exc
                if "interrupted" in error_text:
                    raise RuntimeError(
                        f"shared_stage_estimate_timeout:{table}"
                    ) from exc
                if sqlite_busy_or_locked(exc):
                    raise RuntimeError(
                        f"snapshot_source_read_lock_timeout:paper:"
                        f"shared_stage_estimate:{table}"
                    ) from exc
                raise
            finally:
                connection.set_progress_handler(None, 0)
            minimum = shared_stage_target_minimum_bytes(target)
            if selected_rows is None:
                raise RuntimeError(
                    f"shared_stage_estimate_row_count_missing:{table}"
                )
            advisory = shared_stage_advisory_demand(
                target=target,
                selected_row_count=selected_rows,
                source_row_count_upper=int(total_rows or 0),
                storage=storage,
                candidate_order_index_storage=candidate_order_index_storage,
            )
            advisory_bytes = int(advisory["advisory_required_bytes"])
            estimates[target] = {
                "target": target,
                "source_table": table,
                "strategy": estimate_strategy,
                "query_bounded": True,
                "physical_upper_bound_claimed": False,
                "advisory_schema_version": (
                    SHARED_STAGE_ADVISORY_SCHEMA_VERSION
                ),
                "advisory_formula": SHARED_STAGE_ADVISORY_FORMULA,
                "capacity_sample_used": False,
                "source_measurement_trust_boundary": (
                    "standalone_diagnostic_connection"
                ),
                "pinned_read_view_id": None,
                "pinned_read_view_role": None,
                "estimate_started_after_pin": False,
                "estimate_completed_before_copy": False,
                "row_count_binding_mode": (
                    "exact_selected_rows"
                    if selection.get("source_index_name")
                    else "full_source_row_upper"
                ),
                "sample_limit_rows": SHARED_STAGE_ESTIMATE_SAMPLE_ROWS,
                "selected_row_count": selected_rows,
                "source_row_count_upper": total_rows,
                "source_row_count_upper_basis": row_count_upper_basis,
                "sample_rows": sample_rows,
                "average_row_bytes_diagnostic": (
                    round(average_row_bytes, 3)
                    if average_row_bytes is not None
                    else None
                ),
                "sample_max_row_bytes_diagnostic": sample_max_row_bytes,
                "source_dbstat_page_count": storage["page_count"],
                "source_dbstat_page_size": storage["page_size"],
                "source_dbstat_physical_bytes": storage["physical_bytes"],
                "source_dbstat_payload_bytes": storage["payload_bytes"],
                "source_dbstat_unused_bytes": storage["unused_bytes"],
                "source_dbstat_max_payload_bytes": storage[
                    "max_payload_bytes"
                ],
                "source_dbstat_cell_upper_count": storage[
                    "cell_upper_count"
                ],
                "advisory_row_overhead_bytes": (
                    SHARED_STAGE_ADVISORY_ROW_OVERHEAD_BYTES
                ),
                "advisory_index_overhead_bytes": (
                    SHARED_STAGE_ADVISORY_INDEX_OVERHEAD_BYTES
                ),
                "advisory_root_reserve_pages": (
                    SHARED_STAGE_ADVISORY_ROOT_RESERVE_PAGES
                ),
                **advisory,
                "candidate_order_source_index_name": (
                    candidate_order_source_index_name
                ),
                "candidate_order_source_index_columns": (
                    candidate_order_source_index_columns
                ),
                "candidate_order_source_index_partial": (
                    False if candidate_order_source_index_name else None
                ),
                "candidate_order_source_index_dbstat_page_count": (
                    candidate_order_index_storage["page_count"]
                    if candidate_order_index_storage
                    else None
                ),
                "candidate_order_source_index_dbstat_page_size": (
                    candidate_order_index_storage["page_size"]
                    if candidate_order_index_storage
                    else None
                ),
                "candidate_order_source_index_dbstat_physical_bytes": (
                    candidate_order_index_storage["physical_bytes"]
                    if candidate_order_index_storage
                    else None
                ),
                "candidate_order_source_index_dbstat_payload_bytes": (
                    candidate_order_index_storage["payload_bytes"]
                    if candidate_order_index_storage
                    else None
                ),
                "candidate_order_source_index_dbstat_unused_bytes": (
                    candidate_order_index_storage["unused_bytes"]
                    if candidate_order_index_storage
                    else None
                ),
                "candidate_order_source_index_dbstat_max_payload_bytes": (
                    candidate_order_index_storage["max_payload_bytes"]
                    if candidate_order_index_storage
                    else None
                ),
                "candidate_order_source_index_dbstat_cell_upper_count": (
                    candidate_order_index_storage["cell_upper_count"]
                    if candidate_order_index_storage
                    else None
                ),
                "candidate_order_source_index_structural_overhead_bytes": (
                    candidate_order_index_storage[
                        "structural_overhead_bytes"
                    ]
                    if candidate_order_index_storage
                    else None
                ),
                "advisory_required_bytes": advisory_bytes,
                "minimum_cap_bytes": minimum,
                "source_index_name": selection.get("source_index_name"),
                "source_query_plan": plan["details"],
                "source_query_plan_uses_index": plan["uses_declared_index"],
                "source_query_plan_uses_range_search": plan["uses_range_search"],
                "source_query_plan_full_table_scan_detected": plan[
                    "full_table_scan_detected"
                ],
                "elapsed_sec": round(time.monotonic() - started, 6),
            }
    finally:
        try:
            connection.execute("DETACH DATABASE src")
        except sqlite3.Error:
            pass
        connection.close()
    return {
        "schema_version": SHARED_STAGE_BUDGET_SCHEMA_VERSION,
        "generated_at": utc_iso(),
        "active_targets": list(active_targets),
        "targets": estimates,
        "all_advisory_queries_bounded": all(
            report.get("query_bounded") is True
            for report in estimates.values()
        ),
        "physical_upper_bound_claimed": any(
            report.get("physical_upper_bound_claimed") is True
            for report in estimates.values()
        ),
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
    declared_types = {
        str(row["name"]): str(row["type"] or "")
        for row in columns
    }
    id_columns = [row for row in columns if str(row["name"]) == "id"]
    id_is_rowid_alias = bool(
        len(id_columns) == 1
        and str(id_columns[0]["type"] or "").strip().upper() == "INTEGER"
        and int(id_columns[0]["pk"] or 0) == 1
    )
    return (
        CANDIDATE_OBSERVATION_PROJECTION_REQUIRED_COLUMNS.issubset(names)
        and id_is_rowid_alias
        and declared_numeric_timestamp_type(
            declared_types.get("signal_id", "")
        ),
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
    *,
    source_relation: str,
    where_sql: str = "1=1",
    parameters: tuple[Any, ...] | list[Any] = (),
) -> dict[str, Any]:
    source_names = create_candidate_observation_projection(connection, source_columns)
    selected_columns = ", ".join(quote_identifier(name) for name in source_names)
    cursor = connection.execute(
        f"SELECT {selected_columns} "
        f"FROM {source_relation} "
        f"WHERE {where_sql} "
        f"ORDER BY {quote_identifier('signal_id')}",
        tuple(parameters),
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
        rows = sorted(
            list(grouped),
            key=lambda row: str(row["candidate_id"]),
        )
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


def stage_candidate_observation_rows(
    connection: sqlite3.Connection,
    selection: dict[str, Any],
) -> int:
    stage_relation = (
        f"{quote_identifier(CANDIDATE_STAGE_SCHEMA)}."
        f"{quote_identifier(CANDIDATE_STAGE_TABLE)}"
    )
    source_relation = source_table_reference(CANDIDATE_OBSERVATION_TABLE, selection)
    connection.execute(
        f"CREATE TABLE {stage_relation} AS "
        f"SELECT * FROM {source_relation} WHERE 0"
    )
    # Build the ordering index while the staging table is empty. Keep only the
    # numeric signal_id in the SQLite key: candidate_id can be arbitrarily wide
    # and is sorted within each signal group in Python after the source lock is
    # released. Subsequent pages are maintained incrementally by INSERT and
    # remain bounded by candidate_stage.max_page_count.
    connection.execute(
        f"CREATE INDEX {quote_identifier(CANDIDATE_STAGE_SCHEMA)}."
        f"{quote_identifier(CANDIDATE_STAGE_ORDER_INDEX)} "
        f"ON {quote_identifier(CANDIDATE_STAGE_TABLE)}(signal_id)"
    )
    connection.execute(
        f"INSERT INTO {stage_relation} "
        f"SELECT * FROM {source_relation} WHERE {selection['predicate_sql']}",
        selection["parameters"],
    )
    return int(connection.execute("SELECT changes()").fetchone()[0])


def candidate_stage_relation() -> str:
    return (
        f"{quote_identifier(CANDIDATE_STAGE_SCHEMA)}."
        f"{quote_identifier(CANDIDATE_STAGE_TABLE)}"
    )


def prepare_candidate_stage_for_projection(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    plan_rows = connection.execute(
        f"EXPLAIN QUERY PLAN SELECT signal_id,candidate_id,payload_json "
        f"FROM {candidate_stage_relation()} "
        f"ORDER BY signal_id"
    ).fetchall()
    details = [str(row[3]) for row in plan_rows]
    uses_order_index = any(CANDIDATE_STAGE_ORDER_INDEX in detail for detail in details)
    temp_btree_detected = any("TEMP B-TREE" in detail.upper() for detail in details)
    if not uses_order_index or temp_btree_detected:
        raise RuntimeError(
            "candidate_observation_payload_projection_semantic_mismatch:"
            "stage_order_index_not_used"
        )
    return {
        "stage_order_index_name": CANDIDATE_STAGE_ORDER_INDEX,
        "stage_query_plan": details,
        "stage_query_plan_uses_order_index": uses_order_index,
        "stage_query_plan_temp_btree_detected": temp_btree_detected,
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


def table_column_contract(
    connection: sqlite3.Connection,
    table: str,
    *,
    schema: str = "main",
) -> dict[str, Any]:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(schema)):
        raise ValueError(f"unsupported SQLite schema: {schema}")
    rows = list(
        connection.execute(
            f"PRAGMA {schema}.table_xinfo({quote_identifier(table)})"
        )
    )
    if not rows:
        raise RuntimeError(f"parallel_stage_table_columns_missing:{table}")
    hidden_columns = [
        str(row["name"])
        for row in rows
        if int(row["hidden"] or 0) != 0
    ]
    if hidden_columns:
        raise RuntimeError(
            "parallel_paper_stage_generated_columns_unsupported:"
            f"{table}:{','.join(hidden_columns)}"
        )
    columns = [
        {
            "name": str(row["name"]),
            "declared_type": str(row["type"] or ""),
        }
        for row in rows
    ]
    names = [column["name"] for column in columns]
    if len(names) != len(set(names)):
        raise RuntimeError(f"parallel_stage_duplicate_columns:{table}")
    canonical = json.dumps(
        columns,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "columns": columns,
        "column_names": names,
        "column_count": len(columns),
        "sha256": sha256_text(canonical),
    }


def compressed_stage_storage_sql() -> tuple[str, str]:
    return (
        (
            f"CREATE TABLE {quote_identifier(PARALLEL_PAPER_STAGE_METADATA_TABLE)} ("
            "singleton INTEGER PRIMARY KEY CHECK(singleton = 1), "
            "stage_schema_version TEXT NOT NULL, "
            "codec_schema_version TEXT NOT NULL, "
            "compression TEXT NOT NULL, "
            "source_table TEXT NOT NULL, "
            "source_create_sql TEXT NOT NULL, "
            "source_create_sql_sha256 TEXT NOT NULL, "
            "source_column_contract_json TEXT NOT NULL, "
            "source_column_contract_sha256 TEXT NOT NULL, "
            "deferred_indexes_json TEXT NOT NULL, "
            "row_count INTEGER NOT NULL CHECK(row_count >= 0), "
            "chunk_count INTEGER NOT NULL CHECK(chunk_count >= 0), "
            "raw_size_bytes INTEGER NOT NULL CHECK(raw_size_bytes >= 0), "
            "compressed_size_bytes INTEGER NOT NULL CHECK(compressed_size_bytes >= 0), "
            "rows_sha256 TEXT NOT NULL, "
            "storage_contract_sha256 TEXT NOT NULL)"
        ),
        (
            f"CREATE TABLE {quote_identifier(PARALLEL_PAPER_STAGE_CHUNK_TABLE)} ("
            "sequence INTEGER PRIMARY KEY CHECK(sequence >= 0), "
            "row_count INTEGER NOT NULL CHECK(row_count >= 0), "
            "raw_size_bytes INTEGER NOT NULL CHECK(raw_size_bytes > 0), "
            "compressed_size_bytes INTEGER NOT NULL CHECK(compressed_size_bytes > 0), "
            "raw_sha256 TEXT NOT NULL, "
            "compressed_sha256 TEXT NOT NULL, "
            "payload BLOB NOT NULL)"
        ),
    )


def compressed_stage_storage_contract_sha256() -> str:
    return sha256_text(
        json.dumps(
            list(compressed_stage_storage_sql()),
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )


def _encode_sqlite_stage_value_parts(value: Any) -> tuple[bytes, ...]:
    if value is None:
        return (b"\x00",)
    if isinstance(value, bool):
        value = int(value)
    if isinstance(value, int):
        try:
            return (b"\x01" + struct.pack(">q", value),)
        except struct.error as exc:
            raise RuntimeError("parallel_paper_stage_integer_out_of_range") from exc
    if isinstance(value, float):
        if not math.isfinite(value):
            raise RuntimeError("parallel_paper_stage_non_finite_float")
        return (b"\x02" + struct.pack(">d", value),)
    if isinstance(value, str):
        payload = value.encode("utf-8")
        header = b"\x03" + struct.pack(">Q", len(payload))
        return (header, payload) if payload else (header,)
    if isinstance(value, (bytes, bytearray, memoryview)):
        payload = bytes(value)
        header = b"\x04" + struct.pack(">Q", len(payload))
        return (header, payload) if payload else (header,)
    raise RuntimeError(
        f"parallel_paper_stage_value_type_unsupported:{type(value).__name__}"
    )


def encode_sqlite_stage_value(value: Any) -> bytes:
    return b"".join(_encode_sqlite_stage_value_parts(value))


def encode_sqlite_stage_row_parts(
    row: Any,
    column_count: int,
) -> tuple[bytes, ...]:
    if len(row) != column_count:
        raise RuntimeError("parallel_paper_stage_row_column_count_mismatch")
    value_parts = tuple(
        part
        for value in row
        for part in _encode_sqlite_stage_value_parts(value)
    )
    payload_size = sum(len(part) for part in value_parts)
    return (struct.pack(">Q", payload_size), *value_parts)


def encode_sqlite_stage_row(row: Any, column_count: int) -> bytes:
    return b"".join(encode_sqlite_stage_row_parts(row, column_count))


def _decode_sqlite_stage_value(
    payload: memoryview,
    offset: int,
) -> tuple[Any, int]:
    if offset >= len(payload):
        raise RuntimeError("parallel_paper_stage_chunk_truncated")
    tag = int(payload[offset])
    offset += 1
    if tag == 0:
        return None, offset
    if tag in {1, 2}:
        end = offset + 8
        if end > len(payload):
            raise RuntimeError("parallel_paper_stage_chunk_truncated")
        value = struct.unpack(">q" if tag == 1 else ">d", payload[offset:end])[0]
        if tag == 2 and not math.isfinite(value):
            raise RuntimeError("parallel_paper_stage_non_finite_float")
        return value, end
    if tag in {3, 4}:
        length_end = offset + 8
        if length_end > len(payload):
            raise RuntimeError("parallel_paper_stage_chunk_truncated")
        length = int(struct.unpack(">Q", payload[offset:length_end])[0])
        value_end = length_end + length
        if value_end > len(payload):
            raise RuntimeError("parallel_paper_stage_chunk_truncated")
        raw = bytes(payload[length_end:value_end])
        if tag == 3:
            try:
                return raw.decode("utf-8"), value_end
            except UnicodeDecodeError as exc:
                raise RuntimeError("parallel_paper_stage_text_invalid_utf8") from exc
        return raw, value_end
    raise RuntimeError(f"parallel_paper_stage_value_tag_invalid:{tag}")


class SQLiteStageRowStreamDecoder:
    def __init__(
        self,
        *,
        column_count: int,
        max_encoded_row_bytes: int,
    ) -> None:
        self.column_count = int(column_count)
        self.max_encoded_row_bytes = int(max_encoded_row_bytes)
        self.pending = bytearray()

    def feed(self, payload: bytes) -> list[tuple[Any, ...]]:
        self.pending.extend(payload)
        view = memoryview(self.pending)
        offset = 0
        rows: list[tuple[Any, ...]] = []
        while offset + 8 <= len(view):
            length_end = offset + 8
            row_size = int(struct.unpack(">Q", view[offset:length_end])[0])
            if row_size > self.max_encoded_row_bytes:
                raise RuntimeError("parallel_paper_stage_encoded_row_too_large")
            row_end = length_end + row_size
            if row_end > len(view):
                break
            row_view = view[length_end:row_end]
            row_offset = 0
            values: list[Any] = []
            for _column in range(self.column_count):
                value, row_offset = _decode_sqlite_stage_value(
                    row_view,
                    row_offset,
                )
                values.append(value)
            if row_offset != len(row_view):
                raise RuntimeError("parallel_paper_stage_row_trailing_bytes")
            rows.append(tuple(values))
            offset = row_end
            del row_view
        del view
        if offset:
            del self.pending[:offset]
        return rows

    def finish(self) -> None:
        if self.pending:
            raise RuntimeError("parallel_paper_stage_chunk_truncated")


def decompress_sqlite_stage_chunk(
    payload: bytes,
    *,
    expected_raw_size: int,
) -> bytes:
    if not 0 < expected_raw_size <= PARALLEL_PAPER_STAGE_MAX_CHUNK_RAW_BYTES:
        raise RuntimeError("parallel_paper_stage_chunk_size_invalid")
    decompressor = zlib.decompressobj()
    try:
        raw = decompressor.decompress(payload, expected_raw_size + 1)
        if decompressor.unconsumed_tail or len(raw) > expected_raw_size:
            raise RuntimeError("parallel_paper_stage_chunk_decompression_invalid")
        raw += decompressor.flush(expected_raw_size + 1 - len(raw))
    except zlib.error as exc:
        raise RuntimeError("parallel_paper_stage_chunk_decompression_failed") from exc
    if (
        len(raw) != expected_raw_size
        or not decompressor.eof
        or decompressor.unused_data
        or decompressor.unconsumed_tail
    ):
        raise RuntimeError("parallel_paper_stage_chunk_decompression_invalid")
    return raw


def stage_single_source_table(
    connection: sqlite3.Connection,
    table: str,
    rule: dict[str, Any],
    *,
    review_lower_epoch: float,
    long_lower_epoch: float,
    upper_epoch: float,
    progress: dict[str, Any] | None = None,
    lock_started_monotonic: float | None = None,
    lock_limit_sec: float | None = None,
    cancel_event: threading.Event | None = None,
) -> tuple[
    dict[str, Any],
    list[tuple[str, str, str]],
    dict[str, Any] | None,
]:
    table_started_monotonic = time.monotonic()
    if progress is not None:
        progress["stage"] = f"copy_table:{table}"
        progress["current_table"] = table
        progress["current_table_started_monotonic"] = table_started_monotonic
    source_row = connection.execute(
        "SELECT sql FROM src.sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if source_row is None:
        if rule.get("required"):
            raise RuntimeError(f"snapshot missing required tables: {table}")
        return {
            "included": False,
            "required": False,
            "reason": "optional_source_table_missing",
        }, [], None
    create_sql = str(source_row["sql"] or "")
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
    rowid_copy_plan = indexed_time_bounds_rowid_copy_plan(
        connection,
        table,
        selection,
    )
    source_column_contract = table_column_contract(
        connection,
        table,
        schema="src",
    )
    storage_sql = compressed_stage_storage_sql()
    for statement in storage_sql:
        connection.execute(statement)
    storage_contract_sha256 = compressed_stage_storage_contract_sha256()
    column_sql = ", ".join(
        quote_identifier(name)
        for name in source_column_contract["column_names"]
    )
    copy_relation = (
        rowid_copy_plan["relation"]
        if rowid_copy_plan is not None
        else source_table_reference(table, selection)
    )
    copy_predicate_sql = (
        rowid_copy_plan["predicate_sql"]
        if rowid_copy_plan is not None
        else selection["predicate_sql"]
    )
    copy_parameters = (
        rowid_copy_plan["parameters"]
        if rowid_copy_plan is not None
        else selection["parameters"]
    )
    deferred_indexes = [
        (table, str(row["name"]), str(row["sql"]))
        for row in connection.execute(
            "SELECT name, sql FROM src.sqlite_master "
            "WHERE type='index' AND tbl_name=? AND sql IS NOT NULL ORDER BY name",
            (table,),
        ).fetchall()
    ]
    source_cursor = connection.execute(
        f"SELECT {column_sql} FROM {copy_relation} "
        f"WHERE {copy_predicate_sql}",
        copy_parameters,
    )
    chunk_sequence = 0
    chunk_rows = 0
    copied_rows = 0
    raw_size_bytes = 0
    compressed_size_bytes = 0
    rows_sha256 = hashlib.sha256()
    chunk_buffer = bytearray()

    def flush_chunk() -> None:
        nonlocal chunk_sequence, chunk_rows, raw_size_bytes, compressed_size_bytes
        if not chunk_buffer:
            return
        raw = bytes(chunk_buffer)
        compressed = zlib.compress(
            raw,
            level=PARALLEL_PAPER_STAGE_COMPRESSION_LEVEL,
        )
        connection.execute(
            f"INSERT INTO {quote_identifier(PARALLEL_PAPER_STAGE_CHUNK_TABLE)} "
            "(sequence, row_count, raw_size_bytes, compressed_size_bytes, "
            "raw_sha256, compressed_sha256, payload) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                chunk_sequence,
                chunk_rows,
                len(raw),
                len(compressed),
                hashlib.sha256(raw).hexdigest(),
                hashlib.sha256(compressed).hexdigest(),
                compressed,
            ),
        )
        chunk_sequence += 1
        raw_size_bytes += len(raw)
        compressed_size_bytes += len(compressed)
        chunk_buffer.clear()
        chunk_rows = 0

    def ensure_stage_copy_active() -> None:
        if cancel_event is not None and cancel_event.is_set():
            raise RuntimeError("parallel_paper_stage_cancelled")
        if (
            lock_started_monotonic is not None
            and lock_limit_sec is not None
            and time.monotonic() >= lock_started_monotonic + float(lock_limit_sec)
        ):
            raise RuntimeError(
                f"source_read_lock_budget_exceeded:paper:copy_table:{table}"
            )

    def append_row_parts(parts: tuple[bytes, ...]) -> None:
        nonlocal chunk_rows
        final_part_index = len(parts) - 1
        for part_index, part in enumerate(parts):
            part_view = memoryview(part)
            part_offset = 0
            while part_offset < len(part_view):
                capacity = (
                    PARALLEL_PAPER_STAGE_CHUNK_TARGET_BYTES
                    - len(chunk_buffer)
                )
                take = min(capacity, len(part_view) - part_offset)
                next_offset = part_offset + take
                piece = part_view[part_offset:next_offset]
                chunk_buffer.extend(piece)
                rows_sha256.update(piece)
                part_offset = next_offset
                row_completed = bool(
                    part_index == final_part_index
                    and part_offset == len(part_view)
                )
                if row_completed:
                    chunk_rows += 1
                if len(chunk_buffer) == PARALLEL_PAPER_STAGE_CHUNK_TARGET_BYTES:
                    flush_chunk()
                    ensure_stage_copy_active()

    for row in source_cursor:
        ensure_stage_copy_active()
        encoded_parts = encode_sqlite_stage_row_parts(
            row,
            source_column_contract["column_count"],
        )
        append_row_parts(encoded_parts)
        copied_rows += 1
        if progress is not None and copied_rows % 256 == 0:
            progress["current_table_rows_copied"] = copied_rows
            progress["current_table_raw_bytes"] = (
                raw_size_bytes + len(chunk_buffer)
            )
            progress["current_table_compressed_bytes"] = compressed_size_bytes
    flush_chunk()
    ensure_stage_copy_active()
    if progress is not None:
        progress["current_table_rows_copied"] = copied_rows
        progress["current_table_raw_bytes"] = raw_size_bytes
        progress["current_table_compressed_bytes"] = compressed_size_bytes
    source_column_contract_json = json.dumps(
        source_column_contract["columns"],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    deferred_indexes_json = json.dumps(
        deferred_indexes,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    connection.execute(
        f"INSERT INTO {quote_identifier(PARALLEL_PAPER_STAGE_METADATA_TABLE)} "
        "(singleton, stage_schema_version, codec_schema_version, compression, "
        "source_table, source_create_sql, source_create_sql_sha256, "
        "source_column_contract_json, source_column_contract_sha256, "
        "deferred_indexes_json, row_count, chunk_count, raw_size_bytes, "
        "compressed_size_bytes, rows_sha256, storage_contract_sha256) "
        "VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            PARALLEL_PAPER_STAGE_SCHEMA_VERSION,
            PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION,
            PARALLEL_PAPER_STAGE_COMPRESSION,
            table,
            create_sql,
            sha256_text(create_sql),
            source_column_contract_json,
            source_column_contract["sha256"],
            deferred_indexes_json,
            copied_rows,
            chunk_sequence,
            raw_size_bytes,
            compressed_size_bytes,
            rows_sha256.hexdigest(),
            storage_contract_sha256,
        ),
    )
    table_finished_monotonic = time.monotonic()
    table_duration_sec = max(0.0, table_finished_monotonic - table_started_monotonic)
    source_lock_elapsed_sec = (
        max(0.0, table_finished_monotonic - lock_started_monotonic)
        if lock_started_monotonic is not None
        else None
    )
    source_lock_remaining_sec = (
        max(0.0, float(lock_limit_sec) - source_lock_elapsed_sec)
        if source_lock_elapsed_sec is not None and lock_limit_sec is not None
        else None
    )
    report = {
        "included": True,
        "required": bool(rule.get("required")),
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
        "source_copy_strategy": (
            rowid_copy_plan["strategy"]
            if rowid_copy_plan is not None
            else "indexed_time_range"
        ),
        "source_copy_rowid_alias": (
            rowid_copy_plan["rowid_alias"]
            if rowid_copy_plan is not None
            else None
        ),
        "source_copy_rowid_lower": (
            rowid_copy_plan["rowid_lower"]
            if rowid_copy_plan is not None
            else None
        ),
        "source_copy_rowid_upper": (
            rowid_copy_plan["rowid_upper"]
            if rowid_copy_plan is not None
            else None
        ),
        "source_copy_rowid_span": (
            rowid_copy_plan["rowid_span"]
            if rowid_copy_plan is not None
            else None
        ),
        "source_copy_query_plan": (
            rowid_copy_plan["query_plan"]
            if rowid_copy_plan is not None
            else query_plan["details"]
        ),
        "source_copy_query_plan_uses_integer_primary_key_range": (
            rowid_copy_plan[
                "query_plan_uses_integer_primary_key_range"
            ]
            if rowid_copy_plan is not None
            else False
        ),
        "source_copy_query_plan_full_table_scan_detected": (
            rowid_copy_plan["query_plan_full_table_scan_detected"]
            if rowid_copy_plan is not None
            else query_plan["full_table_scan_detected"]
        ),
        "source_copy_time_predicate_rechecked": (
            rowid_copy_plan["time_predicate_rechecked"]
            if rowid_copy_plan is not None
            else False
        ),
        "horizon": rule.get("horizon") if selection["mode"] == "recent" else None,
        "storage_projection": {
            "schema_version": PAPER_DECISION_STAGE_SCHEMA_VERSION,
            "applied": False,
            "reason": "parallel_lossless_compressed_chunk_spool",
            "payload_semantics_preserved": True,
        },
        "stage_schema_mode": PARALLEL_PAPER_STAGE_STORAGE_MODE,
        "source_create_sql_sha256": sha256_text(create_sql),
        "source_column_contract_sha256": source_column_contract["sha256"],
        "stage_storage_contract_sha256": storage_contract_sha256,
        "stage_codec_schema_version": PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION,
        "stage_compression": PARALLEL_PAPER_STAGE_COMPRESSION,
        "stage_chunk_target_bytes": PARALLEL_PAPER_STAGE_CHUNK_TARGET_BYTES,
        "stage_chunk_count": chunk_sequence,
        "stage_raw_size_bytes": raw_size_bytes,
        "stage_compressed_payload_size_bytes": compressed_size_bytes,
        "stage_rows_sha256": rows_sha256.hexdigest(),
        "stage_column_count": source_column_contract["column_count"],
        "stage_storage_contract_passed": True,
        "stage_index_count": 0,
        "source_constraints_deferred_off_source_lock": True,
        "indexes_created": [],
        "source_copy_duration_sec": round(table_duration_sec, 6),
        "source_lock_elapsed_after_table_sec": (
            round(source_lock_elapsed_sec, 6)
            if source_lock_elapsed_sec is not None
            else None
        ),
        "source_lock_remaining_after_table_sec": (
            round(source_lock_remaining_sec, 6)
            if source_lock_remaining_sec is not None
            else None
        ),
    }
    if progress is not None:
        progress.setdefault("completed_table_timings", {})[table] = {
            "duration_sec": round(table_duration_sec, 6),
            "rows_copied": copied_rows,
            "source_lock_elapsed_sec": report["source_lock_elapsed_after_table_sec"],
            "source_lock_remaining_sec": report["source_lock_remaining_after_table_sec"],
        }
    destination_schema = {
        "create_sql": create_sql,
        "create_sql_sha256": sha256_text(create_sql),
        "column_names": list(source_column_contract["column_names"]),
        "column_contract_sha256": source_column_contract["sha256"],
        "deferred_indexes_json": deferred_indexes_json,
        "stage_schema_version": PARALLEL_PAPER_STAGE_SCHEMA_VERSION,
        "stage_codec_schema_version": PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION,
        "stage_compression": PARALLEL_PAPER_STAGE_COMPRESSION,
        "stage_storage_contract_sha256": storage_contract_sha256,
        "stage_row_count": copied_rows,
        "stage_chunk_count": chunk_sequence,
        "stage_raw_size_bytes": raw_size_bytes,
        "stage_compressed_size_bytes": compressed_size_bytes,
        "stage_rows_sha256": rows_sha256.hexdigest(),
    }
    return report, deferred_indexes, destination_schema


def merge_staged_table(
    connection: sqlite3.Connection,
    *,
    stage_schema: str,
    table: str,
    destination_schema: dict[str, Any],
) -> dict[str, Any]:
    schema = quote_identifier(stage_schema)
    if connection.execute(
        "SELECT 1 FROM main.sqlite_master WHERE name=?",
        (table,),
    ).fetchone():
        raise RuntimeError(f"parallel_stage_destination_collision:{table}")
    stage_tables = {
        str(row["name"]): str(row["sql"] or "")
        for row in connection.execute(
            f"SELECT name, sql FROM {schema}.sqlite_master "
            "WHERE type='table' ORDER BY name"
        ).fetchall()
    }
    expected_stage_tables = {
        PARALLEL_PAPER_STAGE_METADATA_TABLE,
        PARALLEL_PAPER_STAGE_CHUNK_TABLE,
    }
    if set(stage_tables) != expected_stage_tables:
        raise RuntimeError(f"parallel_stage_table_missing:{table}")
    storage_contract_sha256 = compressed_stage_storage_contract_sha256()
    if any(
        not stage_tables.get(name)
        for name in expected_stage_tables
    ) or sha256_text(
        json.dumps(
            [
                stage_tables[PARALLEL_PAPER_STAGE_METADATA_TABLE],
                stage_tables[PARALLEL_PAPER_STAGE_CHUNK_TABLE],
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        )
    ) != storage_contract_sha256:
        raise RuntimeError(f"parallel_paper_stage_storage_contract_mismatch:{table}")
    stage_index_count = int(
        connection.execute(
            f"SELECT COUNT(*) FROM {schema}.sqlite_master WHERE type='index'"
        ).fetchone()[0]
    )
    if stage_index_count != 0:
        raise RuntimeError(f"parallel_paper_stage_storage_contract_mismatch:{table}")
    metadata = connection.execute(
        f"SELECT * FROM {schema}."
        f"{quote_identifier(PARALLEL_PAPER_STAGE_METADATA_TABLE)}"
    ).fetchall()
    if len(metadata) != 1:
        raise RuntimeError(f"parallel_paper_stage_metadata_invalid:{table}")
    metadata_row = metadata[0]
    create_sql = str(destination_schema.get("create_sql") or "")
    expected_create_sql_sha256 = str(
        destination_schema.get("create_sql_sha256") or ""
    )
    column_names = [
        str(name) for name in destination_schema.get("column_names") or []
    ]
    trusted_stage_rows_sha256 = str(
        destination_schema.get("stage_rows_sha256") or ""
    )
    try:
        trusted_stage_row_count = int(destination_schema["stage_row_count"])
        trusted_stage_chunk_count = int(
            destination_schema["stage_chunk_count"]
        )
        trusted_stage_raw_size = int(
            destination_schema["stage_raw_size_bytes"]
        )
        trusted_stage_compressed_size = int(
            destination_schema["stage_compressed_size_bytes"]
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(
            f"parallel_paper_stage_producer_evidence_mismatch:{table}"
        ) from exc
    if (
        not create_sql
        or not column_names
        or sha256_text(create_sql) != expected_create_sql_sha256
        or destination_schema.get("stage_schema_version")
        != PARALLEL_PAPER_STAGE_SCHEMA_VERSION
        or destination_schema.get("stage_codec_schema_version")
        != PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION
        or destination_schema.get("stage_compression")
        != PARALLEL_PAPER_STAGE_COMPRESSION
        or destination_schema.get("stage_storage_contract_sha256")
        != storage_contract_sha256
        or trusted_stage_row_count < 0
        or trusted_stage_chunk_count < 0
        or trusted_stage_raw_size < 0
        or trusted_stage_compressed_size < 0
        or not re.fullmatch(r"[a-f0-9]{64}", trusted_stage_rows_sha256)
        or str(metadata_row["stage_schema_version"])
        != PARALLEL_PAPER_STAGE_SCHEMA_VERSION
        or str(metadata_row["codec_schema_version"])
        != PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION
        or str(metadata_row["compression"])
        != PARALLEL_PAPER_STAGE_COMPRESSION
        or str(metadata_row["source_table"]) != table
        or str(metadata_row["source_create_sql"]) != create_sql
        or str(metadata_row["source_create_sql_sha256"])
        != expected_create_sql_sha256
        or str(metadata_row["source_column_contract_sha256"])
        != str(destination_schema.get("column_contract_sha256") or "")
        or str(metadata_row["deferred_indexes_json"])
        != str(destination_schema.get("deferred_indexes_json") or "")
        or str(metadata_row["storage_contract_sha256"])
        != storage_contract_sha256
    ):
        raise RuntimeError(
            f"parallel_paper_stage_destination_schema_invalid:{table}"
        )
    try:
        metadata_columns = json.loads(
            str(metadata_row["source_column_contract_json"])
        )
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"parallel_paper_stage_column_contract_mismatch:{table}"
        ) from exc
    metadata_column_contract = json.dumps(
        metadata_columns,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    if (
        not isinstance(metadata_columns, list)
        or any(not isinstance(column, dict) for column in metadata_columns)
        or [str(column.get("name") or "") for column in metadata_columns]
        != column_names
        or sha256_text(metadata_column_contract)
        != str(destination_schema.get("column_contract_sha256") or "")
    ):
        raise RuntimeError(
            f"parallel_paper_stage_column_contract_mismatch:{table}"
        )
    try:
        expected_row_count = int(metadata_row["row_count"])
        expected_chunk_count = int(metadata_row["chunk_count"])
        expected_raw_size = int(metadata_row["raw_size_bytes"])
        expected_compressed_size = int(metadata_row["compressed_size_bytes"])
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"parallel_paper_stage_metadata_invalid:{table}"
        ) from exc
    expected_rows_sha256 = str(metadata_row["rows_sha256"])
    if (
        expected_row_count < 0
        or expected_chunk_count < 0
        or expected_raw_size < 0
        or expected_compressed_size < 0
        or not re.fullmatch(r"[a-f0-9]{64}", expected_rows_sha256)
        or (expected_row_count == 0) != (expected_chunk_count == 0)
    ):
        raise RuntimeError(f"parallel_paper_stage_metadata_invalid:{table}")
    if (
        expected_row_count != trusted_stage_row_count
        or expected_chunk_count != trusted_stage_chunk_count
        or expected_raw_size != trusted_stage_raw_size
        or expected_compressed_size != trusted_stage_compressed_size
        or expected_rows_sha256 != trusted_stage_rows_sha256
    ):
        raise RuntimeError(
            f"parallel_paper_stage_producer_evidence_mismatch:{table}"
        )
    connection.execute(create_sql)
    destination_row = connection.execute(
        "SELECT sql FROM main.sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    destination_create_sql = str(
        destination_row["sql"] if destination_row is not None else ""
    )
    destination_contract = table_column_contract(
        connection,
        table,
        schema="main",
    )
    destination_schema_restored = bool(
        sha256_text(destination_create_sql) == expected_create_sql_sha256
        and destination_contract["sha256"]
        == destination_schema.get("column_contract_sha256")
        and destination_contract["column_names"] == column_names
    )
    if not destination_schema_restored:
        raise RuntimeError(
            f"parallel_paper_stage_destination_schema_mismatch:{table}"
        )
    column_sql = ", ".join(quote_identifier(name) for name in column_names)
    placeholders = ", ".join("?" for _ in column_names)
    insert_sql = (
        f"INSERT INTO {quote_identifier(table)} ({column_sql}) "
        f"VALUES ({placeholders})"
    )
    chunk_rows_total = 0
    raw_size_total = 0
    compressed_size_total = 0
    rows_sha256 = hashlib.sha256()
    chunk_count = 0
    maximum_encoded_row_bytes = int(
        connection.getlimit(sqlite3.SQLITE_LIMIT_LENGTH)
    ) + 9 * len(column_names)
    row_decoder = SQLiteStageRowStreamDecoder(
        column_count=len(column_names),
        max_encoded_row_bytes=maximum_encoded_row_bytes,
    )
    before_changes = int(connection.total_changes)
    for chunk in connection.execute(
        f"SELECT sequence, row_count, raw_size_bytes, compressed_size_bytes, "
        f"raw_sha256, compressed_sha256, payload FROM {schema}."
        f"{quote_identifier(PARALLEL_PAPER_STAGE_CHUNK_TABLE)} "
        "ORDER BY sequence"
    ):
        if int(chunk["sequence"]) != chunk_count:
            raise RuntimeError(f"parallel_paper_stage_chunk_sequence_invalid:{table}")
        row_count = int(chunk["row_count"])
        raw_size = int(chunk["raw_size_bytes"])
        compressed_size = int(chunk["compressed_size_bytes"])
        compressed = bytes(chunk["payload"])
        if (
            row_count < 0
            or raw_size <= 0
            or raw_size > PARALLEL_PAPER_STAGE_MAX_CHUNK_RAW_BYTES
            or compressed_size <= 0
            or compressed_size > PARALLEL_PAPER_STAGE_MAX_COMPRESSED_CHUNK_BYTES
            or len(compressed) != compressed_size
            or hashlib.sha256(compressed).hexdigest()
            != str(chunk["compressed_sha256"])
        ):
            raise RuntimeError(f"parallel_paper_stage_chunk_integrity_failed:{table}")
        raw = decompress_sqlite_stage_chunk(
            compressed,
            expected_raw_size=raw_size,
        )
        if hashlib.sha256(raw).hexdigest() != str(chunk["raw_sha256"]):
            raise RuntimeError(f"parallel_paper_stage_chunk_integrity_failed:{table}")
        rows = row_decoder.feed(raw)
        if len(rows) != row_count:
            raise RuntimeError(f"parallel_paper_stage_chunk_integrity_failed:{table}")
        if rows:
            connection.executemany(insert_sql, rows)
        rows_sha256.update(raw)
        chunk_rows_total += row_count
        raw_size_total += raw_size
        compressed_size_total += compressed_size
        chunk_count += 1
    row_decoder.finish()
    rows_merged = int(connection.total_changes) - before_changes
    final_row_count = int(
        connection.execute(
            f"SELECT COUNT(*) FROM {quote_identifier(table)}"
        ).fetchone()[0]
    )
    row_digest_matched = bool(
        rows_sha256.hexdigest() == expected_rows_sha256
        and rows_sha256.hexdigest() == trusted_stage_rows_sha256
        and chunk_rows_total == expected_row_count
        and rows_merged == expected_row_count
        and final_row_count == expected_row_count
        and chunk_count == expected_chunk_count
        and raw_size_total == expected_raw_size
        and compressed_size_total == expected_compressed_size
    )
    if not row_digest_matched:
        raise RuntimeError(f"parallel_paper_stage_row_digest_mismatch:{table}")
    return {
        "rows_merged": rows_merged,
        "destination_create_sql_sha256": sha256_text(destination_create_sql),
        "destination_column_contract_sha256": destination_contract["sha256"],
        "destination_schema_restored": destination_schema_restored,
        "source_constraints_rebuilt_after_source_read_lock_release": True,
        "stage_storage_contract_sha256": storage_contract_sha256,
        "stage_storage_contract_passed": True,
        "stage_index_count": stage_index_count,
        "stage_chunk_count": chunk_count,
        "stage_raw_size_bytes": raw_size_total,
        "stage_compressed_payload_size_bytes": compressed_size_total,
        "stage_rows_sha256": expected_rows_sha256,
        "hydrated_rows_sha256": rows_sha256.hexdigest(),
        "stage_chunk_integrity_passed": True,
        "stage_row_digest_matched": row_digest_matched,
        "final_row_count": final_row_count,
    }


def copy_selected_tables(
    connection: sqlite3.Connection,
    spec: dict[str, Any],
    *,
    review_lower_epoch: float,
    long_lower_epoch: float,
    upper_epoch: float,
    progress: dict[str, Any] | None = None,
    lock_started_monotonic: float | None = None,
    lock_limit_sec: float | None = None,
    externally_staged_tables: set[str] | None = None,
) -> tuple[
    dict[str, dict[str, Any]],
    list[dict[str, str]],
    list[tuple[str, str, str]],
    list[sqlite3.Row] | None,
]:
    source_rows = connection.execute(
        "SELECT name, sql FROM src.sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    source_table_sql = {str(row["name"]): str(row["sql"] or "") for row in source_rows}
    source_tables = set(source_table_sql)
    table_reports: dict[str, dict[str, Any]] = {}
    deferred_indexes: list[tuple[str, str, str]] = []
    candidate_source_columns: list[sqlite3.Row] | None = None
    externally_staged_tables = set(externally_staged_tables or ())
    if progress is not None:
        progress.setdefault("completed_table_timings", {})
    for table, rule in spec["tables"].items():
        if table in externally_staged_tables:
            continue
        table_started_monotonic = time.monotonic()
        if progress is not None:
            progress["stage"] = f"copy_table:{table}"
            progress["current_table"] = table
            progress["current_table_started_monotonic"] = table_started_monotonic
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
            if not projection_supported:
                raise RuntimeError(
                    "candidate_observation_payload_projection_semantic_mismatch:"
                    "source_schema_not_projection_compatible"
                )
        storage_projection: dict[str, Any]
        if projection_supported:
            candidate_source_columns = source_columns
            copied_rows = stage_candidate_observation_rows(connection, selection)
            storage_projection = {
                "schema_version": PAYLOAD_PROJECTION_SCHEMA_VERSION,
                "applied": False,
                "deferred_off_source_lock": True,
                "stage_schema_version": CANDIDATE_STAGE_SCHEMA_VERSION,
                "stage_rows_copied": copied_rows,
                "payload_semantics_preserved": False,
            }
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
        table_finished_monotonic = time.monotonic()
        table_duration_sec = max(0.0, table_finished_monotonic - table_started_monotonic)
        source_lock_elapsed_sec = (
            max(0.0, table_finished_monotonic - lock_started_monotonic)
            if lock_started_monotonic is not None
            else None
        )
        source_lock_remaining_sec = (
            max(0.0, float(lock_limit_sec) - source_lock_elapsed_sec)
            if source_lock_elapsed_sec is not None and lock_limit_sec is not None
            else None
        )
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
            "source_copy_duration_sec": round(table_duration_sec, 6),
            "source_lock_elapsed_after_table_sec": (
                round(source_lock_elapsed_sec, 6)
                if source_lock_elapsed_sec is not None
                else None
            ),
            "source_lock_remaining_after_table_sec": (
                round(source_lock_remaining_sec, 6)
                if source_lock_remaining_sec is not None
                else None
            ),
        }
        if progress is not None:
            progress["completed_table_timings"][table] = {
                "duration_sec": round(table_duration_sec, 6),
                "rows_copied": copied_rows,
                "source_lock_elapsed_sec": (
                    round(source_lock_elapsed_sec, 6)
                    if source_lock_elapsed_sec is not None
                    else None
                ),
                "source_lock_remaining_sec": (
                    round(source_lock_remaining_sec, 6)
                    if source_lock_remaining_sec is not None
                    else None
                ),
            }
    for table, report in table_reports.items():
        if not report.get("included"):
            continue
        projection = report.get("storage_projection") or {}
        if (
            projection.get("applied") is True
            or projection.get("deferred_off_source_lock") is True
        ):
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
    return table_reports, omitted, deferred_indexes, candidate_source_columns


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
    candidate_stage_path: Path | None = None,
    parallel_paper_stage_states: dict[str, dict[str, Any]] | None = None,
    progress: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not source.is_file():
        raise FileNotFoundError(source)
    started = time.time()
    source_stat_before = source.stat()
    data_version_before = int(connection.execute("PRAGMA src.data_version").fetchone()[0])
    if progress is not None:
        progress["stage"] = "source_metadata"
    source_metadata = database_metadata(
        connection,
        spec,
        schema="src",
        indexed_watermark_anchors=True,
        progress=progress,
    )
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
    table_reports, omitted_tables, deferred_indexes, candidate_source_columns = copy_selected_tables(
        connection,
        spec,
        review_lower_epoch=review_lower_epoch,
        long_lower_epoch=long_lower_epoch,
        upper_epoch=upper_epoch,
        progress=progress,
        lock_started_monotonic=pin_report.get("pinned_started_monotonic"),
        lock_limit_sec=pin_report.get("source_read_lock_limit_sec"),
        externally_staged_tables=(
            set(parallel_paper_stage_states)
            if parallel_paper_stage_states
            else None
        ),
    )
    if progress is not None:
        progress["stage"] = "release_source_read_view"
    connection.commit()
    read_view_released = time.time()
    connection.set_progress_handler(None, 0)
    data_version_after = int(connection.execute("PRAGMA src.data_version").fetchone()[0])
    source_stat_after = source.stat()
    connection.execute("DETACH DATABASE src")
    candidate_report = table_reports.get(CANDIDATE_OBSERVATION_TABLE)
    candidate_projection_started = None
    candidate_projection_finished = None
    candidate_stage_size_bytes = 0
    candidate_stage_removed = True
    parallel_paper_stage_results: dict[str, dict[str, Any]] = {}
    parallel_paper_stage_reports: dict[str, dict[str, Any]] = {}
    parallel_stage_tables = tuple(parallel_paper_stage_states or ())
    if (
        parallel_paper_stage_states is not None
        and not parallel_paper_stage_inventory_valid(parallel_stage_tables)
    ):
        raise RuntimeError("parallel_paper_stage_failed:inventory_invalid")
    pinned_read_views = [pin_report]
    all_source_read_views_released_at = read_view_released
    if parallel_paper_stage_states:
        main_lock_deadline = (
            float(pin_report["pinned_started_monotonic"])
            + float(pin_report["source_read_lock_limit_sec"])
        )
        for table in parallel_stage_tables:
            runtime = parallel_paper_stage_states.get(table)
            if runtime is None:
                raise RuntimeError(f"parallel_paper_stage_missing:{table}")
            stage_thread = runtime["thread"]
            runtime_lock_deadline = runtime.get("state", {}).get(
                "lock_deadline_monotonic"
            )
            stage_lock_deadline = (
                float(runtime_lock_deadline)
                if isinstance(runtime_lock_deadline, (int, float))
                else main_lock_deadline
            )
            remaining = max(0.0, stage_lock_deadline - time.monotonic())
            if remaining > 0:
                stage_thread.join(timeout=remaining)
            if stage_thread.is_alive():
                unreaped = cancel_parallel_stage_runtimes(
                    parallel_paper_stage_states,
                    grace_sec=PARALLEL_STAGE_CANCEL_GRACE_SEC,
                )
                stage_exception = runtime["state"].get("exception")
                if not unreaped and isinstance(
                    stage_exception,
                    BaseException,
                ):
                    raise stage_exception
                timeout_exc = RuntimeError(
                    f"parallel_paper_stage_timeout:{table}"
                )
                if unreaped:
                    setattr(timeout_exc, "worker_restart_required", True)
                raise timeout_exc
        for table in parallel_stage_tables:
            runtime = parallel_paper_stage_states[table]
            stage_state = runtime["state"]
            if stage_state.get("exception") is not None:
                if progress is not None:
                    progress["stage"] = f"copy_table:{table}"
                    progress["current_table"] = table
                raise stage_state["exception"]
            result = stage_state.get("result")
            if not isinstance(result, dict) or result.get("accepted") is not True:
                raise RuntimeError(f"parallel_paper_stage_failed:{table}")
            stage_path = Path(result["stage_path"])
            if not stage_path.is_file():
                raise RuntimeError(f"parallel_paper_stage_missing:{table}")
            parallel_paper_stage_results[table] = result
            pinned_read_views.append(result["pinned_read_view"])
            all_source_read_views_released_at = max(
                all_source_read_views_released_at,
                float(result["source_read_view_released_epoch"]),
            )
        for table in parallel_stage_tables:
            result = parallel_paper_stage_results[table]
            config = PARALLEL_PAPER_STAGE_CONFIGS[table]
            stage_schema = str(config["schema"])
            stage_path = Path(result["stage_path"])
            current_stage_size = int(stage_path.stat().st_size)
            if (
                current_stage_size != int(result.get("stage_size_bytes") or 0)
                or current_stage_size
                > int(result.get("stage_budget_bytes") or 0)
            ):
                raise RuntimeError(
                    f"parallel_paper_stage_producer_evidence_mismatch:{table}"
                )
            if progress is not None:
                progress["stage"] = f"merge_parallel_stage:{table}"
                progress["current_table"] = table
            connection.execute(
                f"ATTACH DATABASE ? AS {quote_identifier(stage_schema)}",
                (str(stage_path),),
            )
            merge_started = time.time()
            merge_result = merge_staged_table(
                connection,
                stage_schema=stage_schema,
                table=table,
                destination_schema=result.get("destination_schema") or {},
            )
            if (
                int(merge_result.get("stage_chunk_count") or 0)
                != int(result.get("stage_chunk_count") or 0)
                or int(merge_result.get("stage_raw_size_bytes") or 0)
                != int(result.get("stage_raw_size_bytes") or 0)
                or int(
                    merge_result.get(
                        "stage_compressed_payload_size_bytes"
                    )
                    or 0
                )
                != int(
                    result.get("stage_compressed_payload_size_bytes")
                    or 0
                )
                or str(merge_result.get("stage_rows_sha256") or "")
                != str(result.get("stage_rows_sha256") or "")
            ):
                raise RuntimeError(
                    f"parallel_paper_stage_producer_evidence_mismatch:{table}"
                )
            rows_merged = int(merge_result.get("rows_merged") or 0)
            expected_rows = int(
                (result.get("table_report") or {}).get("rows_copied") or 0
            )
            if rows_merged != expected_rows:
                raise RuntimeError(f"parallel_paper_stage_row_count_mismatch:{table}")
            destination_schema_restored_after_source_read_lock_release = bool(
                merge_started >= all_source_read_views_released_at
                and merge_result.get("destination_schema_restored") is True
                and merge_result.get("stage_storage_contract_passed") is True
                and merge_result.get("stage_chunk_integrity_passed") is True
                and merge_result.get("stage_row_digest_matched") is True
                and merge_result.get(
                    "source_constraints_rebuilt_after_source_read_lock_release"
                )
                is True
            )
            if not destination_schema_restored_after_source_read_lock_release:
                raise RuntimeError(
                    f"parallel_paper_stage_destination_schema_mismatch:{table}"
                )
            connection.commit()
            merge_finished = time.time()
            connection.execute(f"DETACH DATABASE {quote_identifier(stage_schema)}")
            table_report = dict(result["table_report"])
            table_report["parallel_stage"] = {
                "schema_version": PARALLEL_PAPER_STAGE_SCHEMA_VERSION,
                "role": config["role"],
                "full_fidelity_row_copy": True,
                "payload_semantics_preserved": True,
                "stage_schema_mode": result.get("stage_schema_mode"),
                "source_create_sql_sha256": result.get(
                    "source_create_sql_sha256"
                ),
                "destination_create_sql_sha256": merge_result.get(
                    "destination_create_sql_sha256"
                ),
                "source_column_contract_sha256": result.get(
                    "source_column_contract_sha256"
                ),
                "destination_column_contract_sha256": merge_result.get(
                    "destination_column_contract_sha256"
                ),
                "stage_column_count": result.get("stage_column_count"),
                "stage_storage_contract_sha256": result.get(
                    "stage_storage_contract_sha256"
                ),
                "stage_storage_contract_passed": merge_result.get(
                    "stage_storage_contract_passed"
                ) is True,
                "stage_codec_schema_version": result.get(
                    "stage_codec_schema_version"
                ),
                "stage_compression": result.get("stage_compression"),
                "stage_chunk_target_bytes": result.get(
                    "stage_chunk_target_bytes"
                ),
                "stage_chunk_count": merge_result.get("stage_chunk_count"),
                "stage_raw_size_bytes": merge_result.get(
                    "stage_raw_size_bytes"
                ),
                "stage_compressed_payload_size_bytes": merge_result.get(
                    "stage_compressed_payload_size_bytes"
                ),
                "stage_rows_sha256": merge_result.get("stage_rows_sha256"),
                "hydrated_rows_sha256": merge_result.get(
                    "hydrated_rows_sha256"
                ),
                "stage_chunk_integrity_passed": merge_result.get(
                    "stage_chunk_integrity_passed"
                ) is True,
                "stage_row_digest_matched": merge_result.get(
                    "stage_row_digest_matched"
                ) is True,
                "stage_index_count": result.get("stage_index_count"),
                "source_constraints_deferred_off_source_lock": result.get(
                    "source_constraints_deferred_off_source_lock"
                )
                is True,
                "destination_schema_restored_after_source_read_lock_release": (
                    destination_schema_restored_after_source_read_lock_release
                ),
                "source_constraints_rebuilt_after_source_read_lock_release": (
                    merge_result.get(
                        "source_constraints_rebuilt_after_source_read_lock_release"
                    )
                    is True
                ),
                "stage_rows_copied": expected_rows,
                "rows_merged": rows_merged,
                "row_count_matched": rows_merged == expected_rows,
                "compressed_during_source_read_lock": True,
                "hydrated_after_source_read_lock_release": True,
                "quick_check": result.get("quick_check") or [],
                "stage_page_size": result.get("stage_page_size"),
                "stage_size_bytes": result.get("stage_size_bytes"),
                "stage_budget_bytes": result.get("stage_budget_bytes"),
                "source_read_lock_duration_sec": result.get(
                    "source_read_lock_duration_sec"
                ),
                "source_read_lock_budget_passed": result.get(
                    "source_read_lock_budget_passed"
                )
                is True,
                "merge_started_after_source_read_view_release": (
                    merge_started >= float(result["source_read_view_released_epoch"])
                ),
                "merge_duration_sec": round(merge_finished - merge_started, 6),
            }
            table_reports[table] = table_report
            deferred_indexes.extend(list(result.get("deferred_indexes") or []))
            stage_path.unlink(missing_ok=True)
            for suffix in ("-journal", "-wal", "-shm"):
                Path(f"{stage_path}{suffix}").unlink(missing_ok=True)
            removed = not stage_path.exists()
            if not removed:
                raise RuntimeError(f"parallel_paper_stage_cleanup_failed:{table}")
            parallel_paper_stage_reports[table] = {
                "schema_version": PARALLEL_PAPER_STAGE_SCHEMA_VERSION,
                "role": config["role"],
                "stage_size_bytes": int(result.get("stage_size_bytes") or 0),
                "stage_budget_bytes": int(result.get("stage_budget_bytes") or 0),
                "stage_page_size": int(result.get("stage_page_size") or 0),
                "rows_copied": expected_rows,
                "rows_merged": rows_merged,
                "merge_duration_sec": round(merge_finished - merge_started, 6),
                "source_read_lock_duration_sec": float(
                    result.get("source_read_lock_duration_sec") or 0.0
                ),
                "source_read_lock_budget_passed": result.get(
                    "source_read_lock_budget_passed"
                )
                is True,
                "merged_after_source_read_lock_release": (
                    merge_started >= float(result["source_read_view_released_epoch"])
                ),
                "removed_before_publish": removed,
                "quick_check": result.get("quick_check") or [],
                "full_fidelity_row_copy": True,
                "payload_semantics_preserved": True,
                "stage_schema_mode": result.get("stage_schema_mode"),
                "source_create_sql_sha256": result.get(
                    "source_create_sql_sha256"
                ),
                "destination_create_sql_sha256": merge_result.get(
                    "destination_create_sql_sha256"
                ),
                "source_column_contract_sha256": result.get(
                    "source_column_contract_sha256"
                ),
                "destination_column_contract_sha256": merge_result.get(
                    "destination_column_contract_sha256"
                ),
                "stage_column_count": int(result.get("stage_column_count") or 0),
                "stage_storage_contract_sha256": result.get(
                    "stage_storage_contract_sha256"
                ),
                "stage_storage_contract_passed": merge_result.get(
                    "stage_storage_contract_passed"
                ) is True,
                "stage_codec_schema_version": result.get(
                    "stage_codec_schema_version"
                ),
                "stage_compression": result.get("stage_compression"),
                "stage_chunk_target_bytes": int(
                    result.get("stage_chunk_target_bytes") or 0
                ),
                "stage_chunk_count": int(
                    merge_result.get("stage_chunk_count") or 0
                ),
                "stage_raw_size_bytes": int(
                    merge_result.get("stage_raw_size_bytes") or 0
                ),
                "stage_compressed_payload_size_bytes": int(
                    merge_result.get("stage_compressed_payload_size_bytes") or 0
                ),
                "stage_rows_sha256": merge_result.get("stage_rows_sha256"),
                "hydrated_rows_sha256": merge_result.get(
                    "hydrated_rows_sha256"
                ),
                "stage_chunk_integrity_passed": merge_result.get(
                    "stage_chunk_integrity_passed"
                ) is True,
                "stage_row_digest_matched": merge_result.get(
                    "stage_row_digest_matched"
                ) is True,
                "stage_index_count": int(result.get("stage_index_count") or 0),
                "compressed_during_source_read_lock": True,
                "hydrated_after_source_read_lock_release": True,
                "source_constraints_deferred_off_source_lock": result.get(
                    "source_constraints_deferred_off_source_lock"
                )
                is True,
                "destination_schema_restored_after_source_read_lock_release": (
                    destination_schema_restored_after_source_read_lock_release
                ),
                "source_constraints_rebuilt_after_source_read_lock_release": (
                    merge_result.get(
                        "source_constraints_rebuilt_after_source_read_lock_release"
                    )
                    is True
                ),
            }
    if candidate_source_columns is not None:
        if candidate_stage_path is None or not candidate_stage_path.is_file():
            raise RuntimeError("candidate_observation_stage_missing_after_source_release")
        candidate_stage_plan = prepare_candidate_stage_for_projection(connection)
        candidate_stage_size_bytes = int(candidate_stage_path.stat().st_size)
        candidate_projection_started = time.time()
        if progress is not None:
            progress["stage"] = "project_candidate_observations_off_source_lock"
        storage_projection = copy_candidate_observation_projection(
            connection,
            candidate_source_columns,
            source_relation=candidate_stage_relation(),
        )
        candidate_projection_finished = time.time()
        storage_projection.update(
            {
                "deferred_off_source_lock": False,
                "source_stage_schema_version": CANDIDATE_STAGE_SCHEMA_VERSION,
                "source_stage_size_bytes": candidate_stage_size_bytes,
                **candidate_stage_plan,
                "projection_started_after_source_read_view_release": (
                    candidate_projection_started >= all_source_read_views_released_at
                ),
                "off_source_lock_projection_duration_sec": round(
                    candidate_projection_finished - candidate_projection_started, 6
                ),
            }
        )
        if candidate_report is not None:
            candidate_report["storage_projection"] = storage_projection
        connection.commit()
    try:
        connection.execute(f"DETACH DATABASE {quote_identifier(CANDIDATE_STAGE_SCHEMA)}")
    except sqlite3.OperationalError as exc:
        if "no such database" not in str(exc).lower():
            raise
    if candidate_stage_path is not None:
        candidate_stage_path.unlink(missing_ok=True)
        for suffix in ("-journal", "-wal", "-shm"):
            Path(f"{candidate_stage_path}{suffix}").unlink(missing_ok=True)
        candidate_stage_removed = not candidate_stage_path.exists()
        if not candidate_stage_removed:
            raise RuntimeError("candidate_observation_stage_cleanup_failed")
    index_build_started = time.time()
    if progress is not None:
        progress["stage"] = "build_snapshot_indexes"
    create_deferred_indexes(connection, deferred_indexes, table_reports)
    connection.commit()
    index_build_finished = time.time()
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
    main_source_read_lock_duration_sec = (
        read_view_released - float(pin_report["pinned_started_epoch"])
    )
    parallel_source_read_lock_durations = {
        table: float(result.get("source_read_lock_duration_sec") or 0.0)
        for table, result in parallel_paper_stage_results.items()
    }
    max_source_read_lock_duration_sec = max(
        [main_source_read_lock_duration_sec, *parallel_source_read_lock_durations.values()]
    )
    all_source_read_lock_budgets_passed = bool(
        main_source_read_lock_duration_sec
        <= float(pin_report["source_read_lock_limit_sec"]) + 1.0
        and all(
            result.get("source_read_lock_budget_passed") is True
            for result in parallel_paper_stage_results.values()
        )
    )
    all_parallel_stages_pinned = bool(
        not parallel_paper_stage_states
        or (
            tuple(parallel_paper_stage_results) == parallel_stage_tables
            and all(
                bool(result.get("pinned_read_view"))
                for result in parallel_paper_stage_results.values()
            )
        )
    )
    all_parallel_stages_merged_after_release = bool(
        not parallel_paper_stage_states
        or all(
            report.get("merged_after_source_read_lock_release") is True
            for report in parallel_paper_stage_reports.values()
        )
    )
    all_parallel_stages_removed = bool(
        not parallel_paper_stage_states
        or all(
            report.get("removed_before_publish") is True
            for report in parallel_paper_stage_reports.values()
        )
    )
    paper_decision_alias = parallel_paper_stage_reports.get(
        PAPER_DECISION_STAGE_TABLE,
        {},
    )
    return {
        "source_path": str(source.resolve()),
        "snapshot_path": str(destination.resolve()),
        "started_at": utc_iso(started),
        "finished_at": utc_iso(finished),
        "source_read_view_released_at": utc_iso(all_source_read_views_released_at),
        "main_source_read_view_released_at": utc_iso(read_view_released),
        "started_epoch": started,
        "finished_epoch": finished,
        "midpoint_epoch": (started + finished) / 2,
        "duration_sec": round(finished - started, 6),
        "source_read_lock_duration_sec": round(
            max_source_read_lock_duration_sec, 6
        ),
        "main_source_read_lock_duration_sec": round(
            main_source_read_lock_duration_sec, 6
        ),
        "parallel_paper_source_read_lock_duration_sec": {
            table: round(duration, 6)
            for table, duration in parallel_source_read_lock_durations.items()
        },
        "paper_decision_source_read_lock_duration_sec": round(
            parallel_source_read_lock_durations.get(PAPER_DECISION_STAGE_TABLE, 0.0),
            6,
        ),
        "source_read_lock_limit_sec": float(
            pin_report["source_read_lock_limit_sec"]
        ),
        "source_read_lock_budget_passed": all_source_read_lock_budgets_passed,
        "source_read_lock_released_before_index_build": (
            index_build_started >= all_source_read_views_released_at
        ),
        "parallel_paper_stages": parallel_paper_stage_reports,
        "parallel_paper_stage_tables": list(parallel_stage_tables),
        "parallel_paper_stage_count": len(parallel_paper_stage_reports),
        "parallel_paper_stages_all_pinned": all_parallel_stages_pinned,
        "parallel_paper_stages_all_merged_after_source_read_lock_release": (
            all_parallel_stages_merged_after_release
        ),
        "parallel_paper_stages_all_removed_before_publish": (
            all_parallel_stages_removed
        ),
        "paper_decision_parallel_stage_used": bool(paper_decision_alias),
        "paper_decision_parallel_stage_schema_version": paper_decision_alias.get(
            "schema_version"
        ),
        "paper_decision_parallel_read_view_pinned": bool(paper_decision_alias),
        "paper_decision_parallel_stage_merged_after_source_read_lock_release": (
            paper_decision_alias.get("merged_after_source_read_lock_release") is True
        ),
        "paper_decision_parallel_stage_size_bytes": int(
            paper_decision_alias.get("stage_size_bytes") or 0
        ),
        "paper_decision_parallel_stage_page_size": int(
            paper_decision_alias.get("stage_page_size") or 0
        ),
        "paper_decision_parallel_stage_budget_bytes": int(
            paper_decision_alias.get("stage_budget_bytes") or 0
        ),
        "paper_decision_parallel_stage_rows_merged": int(
            paper_decision_alias.get("rows_merged") or 0
        ),
        "paper_decision_parallel_stage_merge_duration_sec": float(
            paper_decision_alias.get("merge_duration_sec") or 0.0
        ),
        "paper_decision_parallel_stage_removed_before_publish": (
            paper_decision_alias.get("removed_before_publish") is True
        ),
        "candidate_projection_after_source_read_lock_release": (
            candidate_projection_started is None
            or candidate_projection_started >= all_source_read_views_released_at
        ),
        "candidate_projection_duration_sec": (
            round(candidate_projection_finished - candidate_projection_started, 6)
            if candidate_projection_started is not None
            and candidate_projection_finished is not None
            else 0.0
        ),
        "temporary_candidate_stage_size_bytes": candidate_stage_size_bytes,
        "temporary_candidate_stage_removed_before_publish": candidate_stage_removed,
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
        "pinned_read_views": pinned_read_views,
        "selection_upper_epoch": float(upper_epoch),
        "selection_review_lower_epoch": float(review_lower_epoch),
        "selection_long_lower_epoch": float(long_lower_epoch),
        "selected_tables": table_reports,
        "omitted_source_tables": omitted_tables,
        "source_schema_version": source_metadata["schema_version"],
        "source_table_schema_sha256": source_metadata["table_schema_sha256"],
        "source_upper_watermarks": source_metadata["upper_watermarks"],
        "source_watermark_query_evidence": source_metadata.get(
            "watermark_query_evidence"
        ) or {},
        **metadata,
    }


class SharedStageBudgetCoordinator:
    """Build one global stage plan from the exact pinned views being copied."""

    def __init__(
        self,
        *,
        total_cap_bytes: int,
        parallel_stage_tables: tuple[str, ...],
        history: dict[str, Any] | None,
        attempt_id: str,
        history_anchor: dict[str, Any] | None = None,
    ) -> None:
        self.active_targets = shared_stage_target_names(parallel_stage_tables)
        self.total_cap_bytes = int(total_cap_bytes)
        self.parallel_stage_tables = tuple(parallel_stage_tables)
        self.history = history
        self.history_anchor = history_anchor
        self.attempt_id = str(attempt_id)
        self._lock = threading.Lock()
        self._estimates: dict[str, dict[str, Any]] = {}
        self._plan: dict[str, Any] | None = None
        self._error: BaseException | None = None
        self._error_target: str | None = None
        self._barrier = threading.Barrier(
            len(self.active_targets),
            action=self._finalize_plan,
        )

    def _finalize_plan(self) -> None:
        try:
            with self._lock:
                estimates = {
                    "schema_version": SHARED_STAGE_BUDGET_SCHEMA_VERSION,
                    "generated_at": utc_iso(),
                    "active_targets": list(self.active_targets),
                    "all_advisory_queries_bounded": all(
                        report.get("query_bounded") is True
                        for report in self._estimates.values()
                    ),
                    "physical_upper_bound_claimed": any(
                        report.get("physical_upper_bound_claimed") is True
                        for report in self._estimates.values()
                    ),
                    "all_advisory_estimates_pinned_read_view_bound": all(
                        report.get("source_measurement_trust_boundary")
                        == "same_pinned_read_view_as_copy"
                        and report.get("estimate_started_after_pin") is True
                        and report.get("estimate_completed_before_copy") is True
                        and bool(report.get("pinned_read_view_id"))
                        and bool(report.get("pinned_read_view_role"))
                        for report in self._estimates.values()
                    ),
                    "targets": json.loads(json.dumps(self._estimates)),
                }
            plan = build_shared_stage_budget_plan(
                total_cap_bytes=self.total_cap_bytes,
                parallel_stage_tables=self.parallel_stage_tables,
                estimates=estimates,
                history=self.history,
                history_anchor=self.history_anchor,
                attempt_id=self.attempt_id,
                require_pinned_view_binding=True,
            )
            if plan.get("accepted") is not True:
                raise RuntimeError("shared_stage_capacity_insufficient")
            with self._lock:
                self._plan = plan
        except BaseException as exc:
            with self._lock:
                self._error = exc

    def submit_estimate(
        self,
        target: str,
        estimate: dict[str, Any],
        *,
        timeout_sec: float,
    ) -> int:
        target = str(target)
        if target not in self.active_targets:
            raise ValueError(f"shared stage target not active: {target}")
        with self._lock:
            if target in self._estimates:
                raise RuntimeError(
                    f"shared_stage_estimate_duplicate_target:{target}"
                )
            self._estimates[target] = json.loads(json.dumps(estimate))
        try:
            self._barrier.wait(timeout=max(0.001, float(timeout_sec)))
        except threading.BrokenBarrierError as exc:
            with self._lock:
                error = self._error
            if error is not None:
                raise error
            raise RuntimeError(
                "shared_stage_estimate_barrier_broken"
            ) from exc
        with self._lock:
            error = self._error
            plan = self._plan
        if error is not None:
            raise error
        if not isinstance(plan, dict):
            raise RuntimeError("shared_stage_budget_plan_missing")
        return int(plan["targets"][target]["granted_cap_bytes"])

    def abort(
        self,
        exc: BaseException | None = None,
        *,
        target: str | None = None,
    ) -> None:
        with self._lock:
            if self._error is None and exc is not None:
                self._error = exc
                self._error_target = str(target) if target else None
        try:
            self._barrier.abort()
        except threading.BrokenBarrierError:
            pass

    def root_error(self) -> tuple[str | None, BaseException | None]:
        with self._lock:
            return self._error_target, self._error

    def error_target(self) -> str | None:
        return self.root_error()[0]

    def plan(self) -> dict[str, Any] | None:
        with self._lock:
            return (
                json.loads(json.dumps(self._plan))
                if isinstance(self._plan, dict)
                else None
            )


def build_parallel_table_stage(
    *,
    source: Path,
    destination: Path,
    table: str,
    role: str,
    rule: dict[str, Any],
    source_page_report: dict[str, int],
    review_lower_epoch: float,
    long_lower_epoch: float,
    upper_epoch: float,
    budget_bytes: int | None = None,
    budget_coordinator: SharedStageBudgetCoordinator | None = None,
    busy_timeout_ms: int,
    max_source_read_lock_sec: float,
    start_event: threading.Event,
    pinned_barrier: threading.Barrier,
    copy_start_event: threading.Event,
    cancel_event: threading.Event,
    runtime_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    connection: sqlite3.Connection | None = None
    progress: dict[str, Any] = {"stage": "open_parallel_stage"}
    pin_started_monotonic: float | None = None
    lock_deadline: float | None = None
    try:
        timeout_sec = max(0.001, float(busy_timeout_ms) / 1000.0)
        connection = sqlite3.connect(destination, timeout=timeout_sec, uri=True)
        if runtime_state is not None:
            runtime_state["connection"] = connection
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={max(0, int(busy_timeout_ms))}")
        connection.execute("PRAGMA journal_mode=OFF")
        connection.execute("PRAGMA synchronous=OFF")
        connection.execute("PRAGMA temp_store=FILE")
        resolved_budget_bytes = max(0, int(budget_bytes or 0))
        source_uri = f"file:{quote(str(source.resolve()), safe='/')}?mode=ro"
        progress["stage"] = "attach_source"
        connection.execute("ATTACH DATABASE ? AS src", (source_uri,))
        if not start_event.wait(timeout=30):
            raise RuntimeError("parallel_paper_stage_start_timeout")
        if cancel_event.is_set():
            raise RuntimeError("parallel_paper_stage_cancelled")
        pin_started = time.time()
        pin_started_monotonic = time.monotonic()
        lock_deadline = pin_started_monotonic + float(max_source_read_lock_sec)
        if runtime_state is not None:
            runtime_state["pin_started_monotonic"] = pin_started_monotonic
            runtime_state["lock_deadline_monotonic"] = lock_deadline

        def interrupt_expired_or_cancelled() -> int:
            return 1 if cancel_event.is_set() or time.monotonic() >= lock_deadline else 0

        connection.set_progress_handler(interrupt_expired_or_cancelled, 10000)
        progress["stage"] = "pin_source_read_view"
        connection.execute("BEGIN")
        connection.execute("SELECT COUNT(*) FROM src.sqlite_master").fetchone()
        pin_finished = time.time()
        pin_report = {
            "role": role,
            "table": table,
            "read_view_id": os.urandom(16).hex(),
            "pinned_started_at": utc_iso(pin_started),
            "pinned_finished_at": utc_iso(pin_finished),
            "pinned_started_epoch": pin_started,
            "pinned_started_monotonic": pin_started_monotonic,
            "pinned_finished_epoch": pin_finished,
            "pinned_midpoint_epoch": (pin_started + pin_finished) / 2,
            "source_read_lock_limit_sec": float(max_source_read_lock_sec),
            **source_page_report,
        }
        if budget_coordinator is not None:
            progress["stage"] = f"shared_stage_estimate:{table}"
            estimate = estimate_shared_stage_target_requirement(
                connection,
                table,
                review_lower_epoch=review_lower_epoch,
                long_lower_epoch=long_lower_epoch,
                upper_epoch=upper_epoch,
                pinned_read_view=pin_report,
                lock_deadline_monotonic=lock_deadline,
                cancel_event=cancel_event,
            )
            connection.set_progress_handler(
                interrupt_expired_or_cancelled,
                10000,
            )
            resolved_budget_bytes = budget_coordinator.submit_estimate(
                table,
                estimate,
                timeout_sec=remaining_source_read_lock_wait(
                    deadline_monotonic=lock_deadline,
                    database="paper",
                    stage=f"shared_stage_estimate_coordinator:{table}",
                    limit_sec=float(max_source_read_lock_sec),
                ),
            )
        if resolved_budget_bytes < MIN_PARALLEL_PAPER_STAGE_CAP_BYTES:
            raise RuntimeError(
                f"shared_stage_capacity_insufficient:{table}"
            )
        page_size = parallel_paper_stage_page_size(resolved_budget_bytes)
        connection.execute(f"PRAGMA page_size={page_size}")
        actual_page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        if actual_page_size != page_size:
            raise RuntimeError(
                f"parallel_paper_stage_failed:page_size_invalid:{table}:"
                f"{actual_page_size}:{page_size}"
            )
        connection.execute(
            f"PRAGMA max_page_count={max(1, resolved_budget_bytes // page_size)}"
        )
        progress["stage"] = "parallel_pinned_barrier"
        try:
            pinned_barrier.wait(
                timeout=remaining_source_read_lock_wait(
                    deadline_monotonic=lock_deadline,
                    database="paper",
                    stage=f"parallel_pinned_barrier:{table}",
                    limit_sec=float(max_source_read_lock_sec),
                )
            )
        except threading.BrokenBarrierError as exc:
            if time.monotonic() >= lock_deadline:
                raise RuntimeError(
                    f"source_read_lock_budget_exceeded:paper:"
                    f"parallel_pinned_barrier:{table}:"
                    f"{float(max_source_read_lock_sec):.3f}s"
                ) from exc
            raise
        while not copy_start_event.wait(timeout=0.1):
            if cancel_event.is_set():
                raise RuntimeError("parallel_paper_stage_cancelled")
            if time.monotonic() >= lock_deadline:
                raise RuntimeError(
                    f"source_read_lock_budget_exceeded:paper:copy_table:{table}:"
                    f"{float(max_source_read_lock_sec):.3f}s"
                )
        if cancel_event.is_set():
            raise RuntimeError("parallel_paper_stage_cancelled")
        table_report, deferred_indexes, destination_schema = stage_single_source_table(
            connection,
            table,
            rule,
            review_lower_epoch=review_lower_epoch,
            long_lower_epoch=long_lower_epoch,
            upper_epoch=upper_epoch,
            progress=progress,
            lock_started_monotonic=pin_started_monotonic,
            lock_limit_sec=max_source_read_lock_sec,
            cancel_event=cancel_event,
        )
        if not isinstance(destination_schema, dict):
            raise RuntimeError(f"parallel_paper_stage_destination_schema_invalid:{table}")
        if cancel_event.is_set():
            raise RuntimeError("parallel_paper_stage_cancelled")
        if time.monotonic() >= lock_deadline:
            raise RuntimeError(
                f"source_read_lock_budget_exceeded:paper:copy_table:{table}:"
                f"{float(max_source_read_lock_sec):.3f}s"
            )
        progress["stage"] = "release_parallel_source_read_view"
        connection.commit()
        read_view_released = time.time()
        connection.set_progress_handler(None, 0)
        connection.execute("DETACH DATABASE src")
        connection.close()
        connection = None
        stage_size_bytes = int(destination.stat().st_size)
        if stage_size_bytes <= 0 or stage_size_bytes > resolved_budget_bytes:
            raise RuntimeError("parallel_paper_stage_budget_exceeded")
        quick_check = sqlite3.connect(destination)
        quick_check.row_factory = sqlite3.Row
        try:
            quick_check_rows = [
                str(row[0]) for row in quick_check.execute("PRAGMA quick_check").fetchall()
            ]
            persisted_stage_tables = {
                str(row["name"]): str(row["sql"] or "")
                for row in quick_check.execute(
                    "SELECT name, sql FROM sqlite_master "
                    "WHERE type='table' ORDER BY name"
                ).fetchall()
            }
            persisted_stage_index_count = int(
                quick_check.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='index'"
                ).fetchone()[0]
            )
            persisted_metadata = quick_check.execute(
                f"SELECT * FROM "
                f"{quote_identifier(PARALLEL_PAPER_STAGE_METADATA_TABLE)}"
            ).fetchall()
        finally:
            quick_check.close()
        if quick_check_rows != ["ok"]:
            raise RuntimeError("parallel_paper_stage_quick_check_failed")
        persisted_storage_contract_sha256 = sha256_text(
            json.dumps(
                [
                    persisted_stage_tables.get(
                        PARALLEL_PAPER_STAGE_METADATA_TABLE,
                        "",
                    ),
                    persisted_stage_tables.get(
                        PARALLEL_PAPER_STAGE_CHUNK_TABLE,
                        "",
                    ),
                ],
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        if (
            persisted_stage_index_count != 0
            or set(persisted_stage_tables)
            != {
                PARALLEL_PAPER_STAGE_METADATA_TABLE,
                PARALLEL_PAPER_STAGE_CHUNK_TABLE,
            }
            or persisted_storage_contract_sha256
            != compressed_stage_storage_contract_sha256()
            or len(persisted_metadata) != 1
            or str(persisted_metadata[0]["source_table"]) != table
            or str(persisted_metadata[0]["source_create_sql_sha256"])
            != destination_schema["create_sql_sha256"]
            or str(persisted_metadata[0]["source_column_contract_sha256"])
            != destination_schema["column_contract_sha256"]
            or str(persisted_metadata[0]["storage_contract_sha256"])
            != persisted_storage_contract_sha256
            or int(persisted_metadata[0]["row_count"])
            != int(table_report["rows_copied"])
            or int(persisted_metadata[0]["chunk_count"])
            != int(table_report["stage_chunk_count"])
            or int(persisted_metadata[0]["raw_size_bytes"])
            != int(table_report["stage_raw_size_bytes"])
            or int(persisted_metadata[0]["compressed_size_bytes"])
            != int(table_report["stage_compressed_payload_size_bytes"])
            or str(persisted_metadata[0]["rows_sha256"])
            != str(table_report["stage_rows_sha256"])
        ):
            raise RuntimeError(
                f"parallel_paper_stage_storage_contract_mismatch:{table}"
            )
        duration_sec = read_view_released - pin_started
        return {
            "schema_version": PARALLEL_PAPER_STAGE_SCHEMA_VERSION,
            "accepted": True,
            "table": table,
            "stage_path": str(destination.resolve()),
            "stage_size_bytes": stage_size_bytes,
            "stage_budget_bytes": resolved_budget_bytes,
            "stage_page_size": page_size,
            "stage_budget_passed": stage_size_bytes <= resolved_budget_bytes,
            "quick_check": quick_check_rows,
            "table_report": table_report,
            "deferred_indexes": deferred_indexes,
            "destination_schema": destination_schema,
            "stage_schema_mode": PARALLEL_PAPER_STAGE_STORAGE_MODE,
            "source_create_sql_sha256": destination_schema["create_sql_sha256"],
            "source_column_contract_sha256": destination_schema[
                "column_contract_sha256"
            ],
            "stage_storage_contract_sha256": (
                persisted_storage_contract_sha256
            ),
            "stage_codec_schema_version": (
                PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION
            ),
            "stage_compression": PARALLEL_PAPER_STAGE_COMPRESSION,
            "stage_chunk_target_bytes": PARALLEL_PAPER_STAGE_CHUNK_TARGET_BYTES,
            "stage_chunk_count": int(table_report["stage_chunk_count"]),
            "stage_raw_size_bytes": int(table_report["stage_raw_size_bytes"]),
            "stage_compressed_payload_size_bytes": int(
                table_report["stage_compressed_payload_size_bytes"]
            ),
            "stage_rows_sha256": str(table_report["stage_rows_sha256"]),
            "stage_column_count": int(table_report["stage_column_count"]),
            "stage_storage_contract_passed": True,
            "stage_index_count": persisted_stage_index_count,
            "source_constraints_deferred_off_source_lock": True,
            "pinned_read_view": pin_report,
            "source_read_view_released_at": utc_iso(read_view_released),
            "source_read_view_released_epoch": read_view_released,
            "source_read_lock_duration_sec": round(duration_sec, 6),
            "source_read_lock_limit_sec": float(max_source_read_lock_sec),
            "source_read_lock_budget_passed": (
                duration_sec <= float(max_source_read_lock_sec) + 1.0
            ),
            "source_open_mode": "read_only_attached_uri",
            "source_mutated_by_snapshot_process": False,
            "full_fidelity_row_copy": True,
            "payload_semantics_preserved": True,
        }
    except threading.BrokenBarrierError as exc:
        raise RuntimeError("parallel_paper_stage_barrier_broken") from exc
    except sqlite3.OperationalError as exc:
        if (
            "interrupted" in str(exc).lower()
            and lock_deadline is not None
            and time.monotonic() >= lock_deadline
        ):
            raise RuntimeError(
                f"source_read_lock_budget_exceeded:paper:"
                f"{progress.get('stage') or f'copy_table:{table}'}:"
                f"{float(max_source_read_lock_sec):.3f}s"
            ) from exc
        if "interrupted" in str(exc).lower() and cancel_event.is_set():
            raise RuntimeError("parallel_paper_stage_cancelled") from exc
        if sqlite_busy_or_locked(exc):
            raise RuntimeError(
                f"snapshot_source_read_lock_timeout:paper:"
                f"{progress.get('stage') or f'copy_table:{table}'}:{table}"
            ) from exc
        if sqlite_full_error(exc):
            raise RuntimeError(f"parallel_paper_stage_budget_exceeded:{table}") from exc
        raise
    finally:
        if connection is not None:
            connection.set_progress_handler(None, 0)
            try:
                connection.rollback()
            except sqlite3.Error:
                pass
            connection.close()
        if runtime_state is not None:
            runtime_state["connection"] = None


def cancel_parallel_stage_runtimes(
    runtimes: dict[str, dict[str, Any]],
    *,
    grace_sec: float = PARALLEL_STAGE_CANCEL_GRACE_SEC,
) -> tuple[str, ...]:
    active = {
        str(table): runtime
        for table, runtime in runtimes.items()
        if runtime.get("thread") is not None
        and runtime["thread"].is_alive()
    }
    if not active:
        return ()
    now = time.monotonic()
    requested_deadline = now + max(0.0, float(grace_sec))
    prior_deadlines = [
        float(runtime["cancel_deadline_monotonic"])
        for runtime in active.values()
        if isinstance(runtime.get("cancel_deadline_monotonic"), (int, float))
    ]
    shared_deadline = min([requested_deadline, *prior_deadlines])
    for runtime in active.values():
        runtime.setdefault("cancel_deadline_monotonic", shared_deadline)
        runtime["cancel_event"].set()
        runtime["start_event"].set()
        runtime["copy_start_event"].set()
        connection = runtime.get("state", {}).get("connection")
        if connection is not None:
            try:
                connection.interrupt()
            except sqlite3.Error:
                pass
        try:
            runtime["pin_barrier"].abort()
        except threading.BrokenBarrierError:
            pass
    for runtime in active.values():
        remaining = max(0.0, shared_deadline - time.monotonic())
        if remaining > 0:
            runtime["thread"].join(timeout=remaining)
    return tuple(
        table
        for table, runtime in active.items()
        if runtime["thread"].is_alive()
    )


def snapshot_all_concurrently(
    source_paths: dict[str, Path],
    partial_dir: Path,
    source_page_reports: dict[str, dict[str, Any]],
    *,
    review_lower_epoch: float,
    long_lower_epoch: float,
    upper_epoch: float,
    database_budgets: dict[str, int],
    expected_parallel_paper_stage_tables: tuple[str, ...],
    busy_timeout_ms: int,
    max_source_read_lock_sec: float,
    candidate_stage_budget_bytes: int | None = None,
    parallel_paper_stage_budget_bytes: dict[str, int] | None = None,
    shared_stage_total_cap_bytes: int | None = None,
    shared_stage_history: dict[str, Any] | None = None,
    shared_stage_history_anchor: dict[str, Any] | None = None,
    shared_stage_attempt_id: str | None = None,
) -> dict[str, dict[str, Any]]:
    names = tuple(DATABASE_SPECS)
    start_barrier = threading.Barrier(len(names))
    pinned_barrier = threading.Barrier(len(names))
    reports: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}
    result_lock = threading.Lock()
    dynamic_shared_budget = shared_stage_total_cap_bytes is not None
    paper_plan_event = threading.Event()
    paper_plan_lock = threading.Lock()
    paper_plan_state: dict[str, Any] = {
        "ready": not dynamic_shared_budget,
        "error": None,
    }
    if not dynamic_shared_budget:
        paper_plan_event.set()

    def publish_paper_plan_ready() -> None:
        with paper_plan_lock:
            if paper_plan_state["error"] is None:
                paper_plan_state["ready"] = True
        paper_plan_event.set()

    def publish_paper_plan_error(exc: BaseException) -> None:
        with paper_plan_lock:
            if not paper_plan_state["ready"] and paper_plan_state["error"] is None:
                paper_plan_state["error"] = exc
        paper_plan_event.set()

    def await_paper_plan_before_nonpaper_pin(name: str) -> None:
        if not dynamic_shared_budget or name == "paper":
            return
        if not paper_plan_event.wait(timeout=max(30.0, float(max_source_read_lock_sec))):
            raise RuntimeError("paper_shared_stage_plan_timeout")
        with paper_plan_lock:
            ready = paper_plan_state["ready"] is True
            failed = paper_plan_state["error"] is not None
        if failed or not ready:
            raise RuntimeError("paper_shared_stage_plan_failed")

    fixed_parallel_stage_budgets = dict(
        parallel_paper_stage_budget_bytes or {}
    )
    shared_budget_coordinator_holder: dict[
        str,
        SharedStageBudgetCoordinator,
    ] = {}

    def worker(name: str) -> None:
        connection = None
        parallel_stage_runtimes: dict[str, dict[str, Any]] = {}
        shared_budget_coordinator: SharedStageBudgetCoordinator | None = None
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
            candidate_stage_path = None
            active_parallel_stage_tables: tuple[str, ...] = ()
            if name == "paper":
                active_parallel_stage_tables = active_parallel_paper_stage_tables(
                    connection
                )
                if active_parallel_stage_tables != expected_parallel_paper_stage_tables:
                    raise RuntimeError(
                        "parallel_paper_stage_failed:source_inventory_drift"
                    )
                if dynamic_shared_budget:
                    if (
                        shared_stage_total_cap_bytes is None
                        or int(shared_stage_total_cap_bytes) <= 0
                        or not shared_stage_attempt_id
                    ):
                        raise RuntimeError(
                            "shared_stage_budget_plan_missing"
                        )
                    shared_budget_coordinator = SharedStageBudgetCoordinator(
                        total_cap_bytes=int(shared_stage_total_cap_bytes),
                        parallel_stage_tables=active_parallel_stage_tables,
                        history=shared_stage_history,
                        history_anchor=shared_stage_history_anchor,
                        attempt_id=str(shared_stage_attempt_id),
                    )
                    shared_budget_coordinator_holder["paper"] = (
                        shared_budget_coordinator
                    )
                elif set(fixed_parallel_stage_budgets) != set(
                    active_parallel_stage_tables
                ):
                    raise RuntimeError(
                        "parallel_paper_stage_failed:disk_budget_inventory_mismatch"
                    )
                candidate_stage_path = partial_dir / ".candidate-observation-stage.db"
                connection.execute(
                    f"ATTACH DATABASE ? AS {quote_identifier(CANDIDATE_STAGE_SCHEMA)}",
                    (str(candidate_stage_path),),
                )
                connection.execute(
                    f"PRAGMA {quote_identifier(CANDIDATE_STAGE_SCHEMA)}.journal_mode=OFF"
                )
                connection.execute(
                    f"PRAGMA {quote_identifier(CANDIDATE_STAGE_SCHEMA)}.synchronous=OFF"
                )
                stage_page_size = 4096
                connection.execute(
                    f"PRAGMA {quote_identifier(CANDIDATE_STAGE_SCHEMA)}.page_size={stage_page_size}"
                )
                resolved_candidate_stage_budget = max(
                    0,
                    int(candidate_stage_budget_bytes or 0),
                )
                if not dynamic_shared_budget:
                    if (
                        resolved_candidate_stage_budget
                        < MIN_CANDIDATE_STAGE_CAP_BYTES
                    ):
                        raise RuntimeError(
                            "shared_stage_capacity_insufficient:"
                            f"{SHARED_STAGE_TARGET_CANDIDATE}"
                        )
                    stage_max_pages = max(
                        1,
                        resolved_candidate_stage_budget // stage_page_size,
                    )
                    connection.execute(
                        f"PRAGMA {quote_identifier(CANDIDATE_STAGE_SCHEMA)}."
                        f"max_page_count={stage_max_pages}"
                    )
                parallel_pin_barrier = threading.Barrier(
                    1 + len(active_parallel_stage_tables)
                )
                for table in active_parallel_stage_tables:
                    config = PARALLEL_PAPER_STAGE_CONFIGS[table]
                    stage_path = partial_dir / str(config["filename"])
                    start_event = threading.Event()
                    copy_start_event = threading.Event()
                    cancel_event = threading.Event()
                    stage_state: dict[str, Any] = {}

                    def parallel_stage_worker(
                        *,
                        stage_table: str = table,
                        stage_config: dict[str, Any] = config,
                        destination_path: Path = stage_path,
                        stage_start: threading.Event = start_event,
                        stage_copy_start: threading.Event = copy_start_event,
                        stage_cancel: threading.Event = cancel_event,
                        state: dict[str, Any] = stage_state,
                    ) -> None:
                        try:
                            state["result"] = build_parallel_table_stage(
                                source=source,
                                destination=destination_path,
                                table=stage_table,
                                role=str(stage_config["role"]),
                                rule=DATABASE_SPECS["paper"]["tables"][stage_table],
                                source_page_report=source_page_reports["paper"],
                                review_lower_epoch=review_lower_epoch,
                                long_lower_epoch=long_lower_epoch,
                                upper_epoch=upper_epoch,
                                budget_bytes=(
                                    None
                                    if dynamic_shared_budget
                                    else int(
                                        fixed_parallel_stage_budgets[
                                            stage_table
                                        ]
                                    )
                                ),
                                budget_coordinator=shared_budget_coordinator,
                                busy_timeout_ms=busy_timeout_ms,
                                max_source_read_lock_sec=max_source_read_lock_sec,
                                start_event=stage_start,
                                pinned_barrier=parallel_pin_barrier,
                                copy_start_event=stage_copy_start,
                                cancel_event=stage_cancel,
                                runtime_state=state,
                            )
                        except Exception as stage_exc:
                            # Publish the concrete stage error before aborting
                            # the coordinator; waiters must never wake to an
                            # error target whose shared state is still empty.
                            state["exception"] = stage_exc
                            state["error"] = {
                                "error_code": snapshot_component_failure_code(
                                    stage_exc
                                ),
                                "error_type": type(stage_exc).__name__,
                                "stage": stage_table,
                            }
                            if shared_budget_coordinator is not None:
                                shared_budget_coordinator.abort(
                                    stage_exc,
                                    target=stage_table,
                                )
                            try:
                                parallel_pin_barrier.abort()
                            except threading.BrokenBarrierError:
                                pass

                    stage_thread = threading.Thread(
                        target=parallel_stage_worker,
                        name=f"snapshot-{table}-stage",
                        daemon=True,
                    )
                    parallel_stage_runtimes[table] = {
                        "thread": stage_thread,
                        "state": stage_state,
                        "path": stage_path,
                        "start_event": start_event,
                        "copy_start_event": copy_start_event,
                        "cancel_event": cancel_event,
                        "pin_barrier": parallel_pin_barrier,
                    }
                    stage_thread.start()
            progress["stage"] = "start_barrier"
            start_barrier.wait(timeout=30)
            for runtime in parallel_stage_runtimes.values():
                runtime["start_event"].set()
            if dynamic_shared_budget and name != "paper":
                progress["stage"] = "wait_for_paper_shared_stage_plan"
                await_paper_plan_before_nonpaper_pin(name)
            pin_started = time.time()
            pin_started_monotonic = time.monotonic()
            lock_deadline = pin_started_monotonic + float(max_source_read_lock_sec)

            def interrupt_expired_read_view() -> int:
                return 1 if time.monotonic() >= lock_deadline else 0

            connection.set_progress_handler(interrupt_expired_read_view, 10000)
            progress["stage"] = "pin_source_read_view"
            connection.execute("BEGIN")
            connection.execute("SELECT COUNT(*) FROM src.sqlite_master").fetchone()
            pin_finished = time.time()
            pin_report = {
                "role": f"{name}_main_selective_copy",
                "read_view_id": os.urandom(16).hex(),
                "pinned_started_at": utc_iso(pin_started),
                "pinned_finished_at": utc_iso(pin_finished),
                "pinned_started_epoch": pin_started,
                "pinned_started_monotonic": pin_started_monotonic,
                "pinned_finished_epoch": pin_finished,
                "pinned_midpoint_epoch": (pin_started + pin_finished) / 2,
                "source_read_lock_limit_sec": float(max_source_read_lock_sec),
                **source_page_reports[name],
            }
            if name == "paper" and shared_budget_coordinator is not None:
                progress["stage"] = (
                    f"shared_stage_estimate:"
                    f"{SHARED_STAGE_TARGET_CANDIDATE}"
                )
                candidate_estimate = estimate_shared_stage_target_requirement(
                    connection,
                    SHARED_STAGE_TARGET_CANDIDATE,
                    review_lower_epoch=review_lower_epoch,
                    long_lower_epoch=long_lower_epoch,
                    upper_epoch=upper_epoch,
                    pinned_read_view=pin_report,
                    lock_deadline_monotonic=lock_deadline,
                )
                connection.set_progress_handler(
                    interrupt_expired_read_view,
                    10000,
                )
                try:
                    resolved_candidate_stage_budget = (
                        shared_budget_coordinator.submit_estimate(
                            SHARED_STAGE_TARGET_CANDIDATE,
                            candidate_estimate,
                            timeout_sec=remaining_source_read_lock_wait(
                                deadline_monotonic=lock_deadline,
                                database="paper",
                                stage=(
                                    "shared_stage_estimate_coordinator:"
                                    f"{SHARED_STAGE_TARGET_CANDIDATE}"
                                ),
                                limit_sec=float(max_source_read_lock_sec),
                            ),
                        )
                    )
                except BaseException:
                    root_target, root_exception = (
                        shared_budget_coordinator.root_error()
                    )
                    if root_exception is not None:
                        if root_target:
                            progress["stage"] = (
                                f"copy_table:{root_target}"
                            )
                            progress["current_table"] = root_target
                        raise root_exception
                    for table, runtime in parallel_stage_runtimes.items():
                        stage_exception = runtime["state"].get(
                            "exception"
                        )
                        if stage_exception is not None:
                            progress["stage"] = f"copy_table:{table}"
                            progress["current_table"] = table
                            raise stage_exception
                    raise
                if (
                    resolved_candidate_stage_budget
                    < MIN_CANDIDATE_STAGE_CAP_BYTES
                ):
                    raise RuntimeError(
                        "shared_stage_capacity_insufficient:"
                        f"{SHARED_STAGE_TARGET_CANDIDATE}"
                    )
                stage_max_pages = max(
                    1,
                    resolved_candidate_stage_budget // stage_page_size,
                )
                connection.execute(
                    f"PRAGMA {quote_identifier(CANDIDATE_STAGE_SCHEMA)}."
                    f"max_page_count={stage_max_pages}"
                )
            if parallel_stage_runtimes:
                progress["stage"] = "paper_parallel_pinned_barrier"
                try:
                    next(iter(parallel_stage_runtimes.values()))[
                        "pin_barrier"
                    ].wait(
                        timeout=remaining_source_read_lock_wait(
                            deadline_monotonic=lock_deadline,
                            database=name,
                            stage="paper_parallel_pinned_barrier",
                            limit_sec=float(max_source_read_lock_sec),
                        )
                    )
                except threading.BrokenBarrierError as barrier_exc:
                    if shared_budget_coordinator is not None:
                        root_target, root_exception = (
                            shared_budget_coordinator.root_error()
                        )
                        if root_exception is not None:
                            if root_target:
                                progress["stage"] = (
                                    f"copy_table:{root_target}"
                                )
                                progress["current_table"] = root_target
                            raise root_exception
                    for table, runtime in parallel_stage_runtimes.items():
                        stage_exception = runtime["state"].get("exception")
                        if stage_exception is not None:
                            progress["stage"] = f"copy_table:{table}"
                            progress["current_table"] = table
                            raise stage_exception
                    if time.monotonic() >= lock_deadline:
                        raise RuntimeError(
                            "source_read_lock_budget_exceeded:paper:"
                            "paper_parallel_pinned_barrier:"
                            f"{float(max_source_read_lock_sec):.3f}s"
                        ) from barrier_exc
                    raise RuntimeError("parallel_paper_stage_barrier_broken") from barrier_exc
            if name == "paper" and dynamic_shared_budget:
                # The paper main view and every parallel paper stage are now
                # pinned and the shared-stage plan is final. Only now allow the
                # smaller signal/raw/kline databases to pin. This keeps their
                # rollback-journal read locks out of the 20-second allocation
                # estimate phase without weakening the cross-database barrier.
                publish_paper_plan_ready()
            progress["stage"] = "pinned_barrier"
            try:
                pinned_barrier.wait(
                    timeout=remaining_source_read_lock_wait(
                        deadline_monotonic=lock_deadline,
                        database=name,
                        stage="pinned_barrier",
                        limit_sec=float(max_source_read_lock_sec),
                    )
                )
            except threading.BrokenBarrierError as barrier_exc:
                if time.monotonic() >= lock_deadline:
                    raise RuntimeError(
                        f"source_read_lock_budget_exceeded:{name}:"
                        f"pinned_barrier:{float(max_source_read_lock_sec):.3f}s"
                    ) from barrier_exc
                raise
            for runtime in parallel_stage_runtimes.values():
                runtime["copy_start_event"].set()
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
                candidate_stage_path=candidate_stage_path,
                parallel_paper_stage_states=(
                    parallel_stage_runtimes or None
                ),
                progress=progress,
            )
            if name == "paper" and shared_budget_coordinator is not None:
                pinned_plan = shared_budget_coordinator.plan()
                if not isinstance(pinned_plan, dict):
                    raise RuntimeError("shared_stage_budget_plan_missing")
                report["shared_stage_budget_plan"] = pinned_plan
                report[
                    "shared_stage_estimates_bound_to_copy_read_views"
                ] = True
            with result_lock:
                reports[name] = report
        except Exception as exc:
            if name == "paper" and dynamic_shared_budget:
                publish_paper_plan_error(exc)
            sqlite_errorcode, sqlite_errorname = sqlite_error_identity(exc)
            if shared_budget_coordinator is not None:
                shared_budget_coordinator.abort(exc)
            worker_restart_required = exception_requires_worker_restart(exc)
            if parallel_stage_runtimes:
                unreaped = cancel_parallel_stage_runtimes(
                    parallel_stage_runtimes,
                    grace_sec=PARALLEL_STAGE_CANCEL_GRACE_SEC,
                )
                worker_restart_required = bool(
                    worker_restart_required or unreaped
                )
            completed_parallel_stages = [
                table
                for table, runtime in parallel_stage_runtimes.items()
                if isinstance(runtime["state"].get("result"), dict)
                and runtime["state"]["result"].get("accepted") is True
            ]
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
            elif isinstance(exc, sqlite3.OperationalError) and sqlite_busy_or_locked(exc):
                exc = RuntimeError(
                    f"snapshot_source_read_lock_timeout:{name}:"
                    f"{progress.get('stage') or 'unknown'}"
                )
            elif isinstance(exc, sqlite3.OperationalError) and sqlite_full_error(exc):
                exc = RuntimeError(
                    f"selective snapshot exceeded database budget for {name}"
                )
            if worker_restart_required:
                setattr(exc, "worker_restart_required", True)
            for barrier in (start_barrier, pinned_barrier):
                try:
                    barrier.abort()
                except threading.BrokenBarrierError:
                    pass
            now_monotonic = time.monotonic()
            current_started = progress.get("current_table_started_monotonic")
            pin_started_monotonic = locals().get("pin_started_monotonic")
            completed_timings = progress.get("completed_table_timings")
            copy_timing = {
                "current_table": progress.get("current_table"),
                "current_table_elapsed_sec": (
                    round(max(0.0, now_monotonic - float(current_started)), 6)
                    if isinstance(current_started, (int, float))
                    else None
                ),
                "source_lock_elapsed_sec": (
                    round(max(0.0, now_monotonic - float(pin_started_monotonic)), 6)
                    if isinstance(pin_started_monotonic, (int, float))
                    else None
                ),
                "source_lock_remaining_sec": (
                    round(
                        max(
                            0.0,
                            float(max_source_read_lock_sec)
                            - max(0.0, now_monotonic - float(pin_started_monotonic)),
                        ),
                        6,
                    )
                    if isinstance(pin_started_monotonic, (int, float))
                    else None
                ),
                "completed_tables": (
                    dict(completed_timings)
                    if isinstance(completed_timings, dict)
                    else {}
                ),
                "completed_parallel_stages": completed_parallel_stages,
            }
            error_details: dict[str, Any] = {
                "error_code": snapshot_component_failure_code(exc),
                "error_type": type(exc).__name__,
                "stage": str(progress.get("stage") or "unknown"),
                "copy_timing": copy_timing,
            }
            if sqlite_errorcode is not None:
                error_details["sqlite_errorcode"] = sqlite_errorcode
            if sqlite_errorname:
                error_details["sqlite_errorname"] = sqlite_errorname
            if worker_restart_required:
                error_details["worker_restart_required"] = True
            with result_lock:
                errors[name] = error_details
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
        concurrent_error = ConcurrentSnapshotError(errors)
        coordinator = shared_budget_coordinator_holder.get("paper")
        if coordinator is not None:
            pinned_plan = coordinator.plan()
            if isinstance(pinned_plan, dict):
                setattr(
                    concurrent_error,
                    "shared_stage_budget_plan",
                    pinned_plan,
                )
        raise concurrent_error
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


def shared_stage_budget_plan_hash_payload(
    plan: dict[str, Any],
) -> dict[str, Any]:
    payload = json.loads(json.dumps(plan))
    for key in (
        "generated_at",
        "plan_sha256",
        "evidence_sha256",
        "captured_at",
        "captured_before_cleanup",
        "failure_code",
        "failure_components",
        "unregistered_stage_files",
        "stage_files_removed",
    ):
        payload.pop(key, None)
    for key in (
        "actual_total_bytes",
        "unconsumed_bytes",
        "all_targets_within_grant",
        "targets_exceeding_advisory",
        "advisory_miss_count",
        "all_target_row_counts_bound_to_snapshot",
        "no_unregistered_stage_files",
        "cleanup_completed",
    ):
        payload[key] = None
    try:
        total_granted = json_safe_integer(
            payload.get("total_granted_bytes"),
            field="shared_stage.total_granted_bytes",
        )
        total_cap = json_safe_integer(
            payload.get("total_cap_bytes"),
            field="shared_stage.total_cap_bytes",
        )
    except ValueError as exc:
        raise ValueError(
            "shared stage plan totals must be safe integers"
        ) from exc
    payload["accepted"] = bool(
        payload.get("capacity_sufficient") is True
        and payload.get("grants_sum_matches_total_cap") is True
        and int(total_granted) == int(total_cap)
    )
    for report in (payload.get("targets") or {}).values():
        if not isinstance(report, dict):
            continue
        for key in (
            "actual_usage_bytes",
            "high_water_bytes",
            "actual_rows_copied",
            "row_count_bound_to_snapshot",
            "advisory_exceeded",
            "advisory_delta_bytes",
            "logical_high_water_bytes",
            "allocated_high_water_bytes",
            "file_present",
            "file_count",
            "copy_completed",
            "cap_hit",
            "sqlite_full_observed",
            "within_grant",
            "utilization_ratio",
            "evidence_source",
        ):
            report.pop(key, None)
    return payload


def shared_stage_hash_normalize(value: Any) -> Any:
    """Normalize JSON values identically in Python and JavaScript.

    JSON parsers do not preserve the distinction between ``1`` and ``1.0``.
    Integral floats in the JavaScript-safe range are therefore normalized to
    integers. Non-integral floats are represented by their exact IEEE-754
    binary64 bit pattern so both runtimes hash the same value without relying
    on language-specific decimal formatting.
    """
    if value is None or isinstance(value, (bool, str)):
        return value
    if isinstance(value, int):
        if abs(value) > 9_007_199_254_740_991:
            raise ValueError("shared stage hash contains unsafe integer")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("shared stage hash contains non-finite float")
        if value.is_integer():
            if abs(value) > 9_007_199_254_740_991:
                raise ValueError("shared stage hash contains unsafe integer")
            return int(value)
        return {"__float64__": struct.pack(">d", value).hex()}
    if isinstance(value, list):
        return [shared_stage_hash_normalize(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): shared_stage_hash_normalize(item)
            for key, item in value.items()
        }
    raise TypeError(
        f"unsupported shared stage hash value: {type(value).__name__}"
    )


def shared_stage_hash_json(value: Any) -> str:
    return json.dumps(
        shared_stage_hash_normalize(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def shared_stage_budget_plan_sha256(plan: dict[str, Any]) -> str:
    return sha256_text(
        shared_stage_hash_json(shared_stage_budget_plan_hash_payload(plan))
    )


def shared_stage_budget_evidence_hash_payload(
    evidence: dict[str, Any],
) -> dict[str, Any]:
    payload = json.loads(json.dumps(evidence))
    payload.pop("evidence_sha256", None)
    return payload


def shared_stage_budget_evidence_sha256(evidence: dict[str, Any]) -> str:
    return sha256_text(
        shared_stage_hash_json(shared_stage_budget_evidence_hash_payload(evidence))
    )


def shared_stage_history_required_bytes(
    target: str,
    previous: dict[str, Any] | None,
) -> tuple[int, str]:
    """Return the hard baseline justified by verified prior high-water evidence."""
    minimum = shared_stage_target_minimum_bytes(target)
    if not isinstance(previous, dict) or not previous:
        return minimum, "none"
    high_water = round_up_stage_page(previous.get("high_water_bytes") or 0)
    granted = round_up_stage_page(previous.get("granted_cap_bytes") or 0)
    if previous.get("cap_hit") is True:
        required = round_up_stage_page(
            max(minimum, high_water, granted)
            + SHARED_STAGE_CAP_HIT_EXTRA_PAGES * SHARED_STAGE_PAGE_SIZE
        )
        return required, "cap_hit"
    if previous.get("copy_completed") is True:
        required = round_up_stage_page(
            max(minimum, high_water * SHARED_STAGE_COMPLETED_HISTORY_HEADROOM)
        )
        return required, "completed"
    required = round_up_stage_page(
        max(minimum, high_water * SHARED_STAGE_INCOMPLETE_HISTORY_HEADROOM)
    )
    return required, "incomplete"


def allocate_shared_stage_residual(
    *,
    residual_bytes: int,
    priority_targets: tuple[str, ...] | list[str],
    weights: dict[str, int],
) -> dict[str, int]:
    """Allocate every residual page deterministically by integer weights."""
    targets = tuple(str(target) for target in priority_targets)
    residual = int(residual_bytes)
    if residual < 0 or residual % SHARED_STAGE_PAGE_SIZE != 0:
        raise ValueError("shared stage residual must be non-negative and page-aligned")
    if len(targets) != len(set(targets)):
        raise ValueError("shared stage allocation priority contains duplicates")
    if not targets:
        if residual:
            raise ValueError("shared stage residual has no allocation target")
        return {}
    normalized_weights = {
        target: max(SHARED_STAGE_PAGE_SIZE, int(weights.get(target) or 0))
        for target in targets
    }
    total_weight = sum(normalized_weights.values())
    total_pages = residual // SHARED_STAGE_PAGE_SIZE
    page_allocations: dict[str, int] = {}
    remainders: list[tuple[int, int, str]] = []
    allocated_pages = 0
    for index, target in enumerate(targets):
        numerator = total_pages * normalized_weights[target]
        pages = numerator // total_weight
        remainder = numerator % total_weight
        page_allocations[target] = pages
        allocated_pages += pages
        remainders.append((remainder, -index, target))
    remaining_pages = total_pages - allocated_pages
    for _remainder, _negative_index, target in sorted(
        remainders,
        reverse=True,
    )[:remaining_pages]:
        page_allocations[target] += 1
    return {
        target: page_allocations[target] * SHARED_STAGE_PAGE_SIZE
        for target in targets
    }


def build_shared_stage_budget_plan(
    *,
    total_cap_bytes: int,
    parallel_stage_tables: tuple[str, ...],
    estimates: dict[str, Any],
    history: dict[str, Any] | None = None,
    history_anchor: dict[str, Any] | None = None,
    attempt_id: str | None = None,
    require_pinned_view_binding: bool = False,
) -> dict[str, Any]:
    active_targets = shared_stage_target_names(parallel_stage_tables)
    estimate_targets = (
        estimates.get("targets") if isinstance(estimates, dict) else None
    )
    if (
        not isinstance(estimate_targets, dict)
        or tuple(estimates.get("active_targets") or ()) != active_targets
        or set(estimate_targets) != set(active_targets)
        or estimates.get("schema_version") != SHARED_STAGE_BUDGET_SCHEMA_VERSION
        or estimates.get("all_advisory_queries_bounded") is not True
        or estimates.get("physical_upper_bound_claimed") is not False
    ):
        raise ValueError("shared stage estimate inventory invalid")
    history_report = validated_shared_stage_budget_history(
        history,
        trusted_anchor=history_anchor,
    )
    if (
        history_report.get("accepted") is True
        and str(history_report.get("attempt_id") or "")
        == str(attempt_id or "")
    ):
        history_report = {
            "accepted": False,
            "reason": "history_attempt_id_reused",
            "targets": {},
        }
    history_validation_required = history is not None
    history_targets = (
        history_report.get("targets")
        if history_report.get("accepted") is True
        else {}
    )
    target_reports: dict[str, dict[str, Any]] = {}
    baseline_total = 0
    cap_hit_targets: list[str] = []
    for target in active_targets:
        estimate = estimate_targets.get(target) or {}
        minimum = shared_stage_target_minimum_bytes(target)
        try:
            advisory_required = round_up_stage_page(
                max(
                    minimum,
                    int(estimate.get("advisory_required_bytes") or 0),
                )
            )
        except (TypeError, ValueError, OverflowError):
            raise ValueError(
                f"shared stage advisory invalid for {target}"
            ) from None
        advisory_contract = (
            estimate.get("advisory_schema_version"),
            estimate.get("advisory_formula"),
        )
        if (
            estimate.get("query_bounded") is not True
            or estimate.get("physical_upper_bound_claimed") is not False
            or advisory_contract
            not in {
                (
                    SHARED_STAGE_ADVISORY_SCHEMA_VERSION,
                    SHARED_STAGE_ADVISORY_FORMULA,
                ),
                (
                    SHARED_STAGE_SAMPLE_ADVISORY_SCHEMA_VERSION,
                    SHARED_STAGE_SAMPLE_ADVISORY_FORMULA,
                ),
                (
                    SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_SCHEMA_VERSION,
                    SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_FORMULA,
                ),
            }
        ):
            raise ValueError(f"shared stage advisory contract invalid for {target}")
        target_storage_schema_version = (
            shared_stage_target_storage_schema_version(target)
        )
        raw_previous = history_targets.get(target) or {}
        previous_storage_schema_version = str(
            raw_previous.get("storage_schema_version") or ""
        )
        history_storage_compatible = bool(
            previous_storage_schema_version == target_storage_schema_version
        )
        previous = raw_previous if history_storage_compatible else {}
        previous_high_water = round_up_stage_page(
            previous.get("high_water_bytes") or 0
        )
        previous_grant = round_up_stage_page(
            previous.get("granted_cap_bytes") or 0
        )
        history_required, history_state = shared_stage_history_required_bytes(
            target,
            previous,
        )
        cap_hit = history_state == "cap_hit"
        completed = history_state == "completed"
        if cap_hit:
            cap_hit_targets.append(target)
        baseline_required = round_up_stage_page(
            max(minimum, history_required)
        )
        baseline_total += baseline_required
        target_reports[target] = {
            "target": target,
            "source_table": str(
                (estimate.get("source_table") or target)
            ),
            "stage_filename": shared_stage_target_filename(target),
            "required": (
                target == SHARED_STAGE_TARGET_CANDIDATE
                or PARALLEL_PAPER_STAGE_CONFIGS.get(target, {}).get("required")
                is True
            ),
            "storage_schema_version": target_storage_schema_version,
            "minimum_cap_bytes": minimum,
            "advisory_required_bytes": advisory_required,
            "advisory_strategy": estimate.get("strategy"),
            "advisory_query_bounded": estimate.get("query_bounded") is True,
            "physical_upper_bound_claimed": False,
            "advisory_evidence": {
                key: estimate.get(key)
                for key in (
                    "advisory_schema_version",
                    "advisory_formula",
                    "query_bounded",
                    "physical_upper_bound_claimed",
                    "capacity_sample_used",
                    "indexed_count_completed",
                    "indexed_count_timed_out",
                    "indexed_count_timeout_sec",
                    "indexed_count_elapsed_sec",
                    "dbstat_completed",
                    "dbstat_timed_out",
                    "dbstat_timeout_sec",
                    "dbstat_elapsed_sec",
                    "dbstat_skipped_reason",
                    "source_measurement_trust_boundary",
                    "pinned_read_view_id",
                    "pinned_read_view_role",
                    "estimate_started_after_pin",
                    "estimate_completed_before_copy",
                    "row_count_binding_mode",
                    "sample_limit_rows",
                    "selected_row_count",
                    "sample_row_count_advisory_basis",
                    "source_row_count_upper",
                    "source_row_count_upper_basis",
                    "sample_rows",
                    "average_row_bytes_diagnostic",
                    "sample_max_row_bytes_diagnostic",
                    "sample_row_bytes_basis",
                    "source_dbstat_page_count",
                    "source_dbstat_page_size",
                    "source_dbstat_physical_bytes",
                    "source_dbstat_payload_bytes",
                    "source_dbstat_unused_bytes",
                    "source_dbstat_max_payload_bytes",
                    "source_dbstat_cell_upper_count",
                    "advisory_row_overhead_bytes",
                    "advisory_index_overhead_bytes",
                    "advisory_root_reserve_pages",
                    "source_row_fraction_numerator",
                    "source_row_fraction_denominator",
                    "table_sample_payload_advisory_bytes",
                    "table_scaled_physical_advisory_bytes",
                    "table_row_overhead_advisory_bytes",
                    "table_root_reserve_advisory_bytes",
                    "table_advisory_bytes",
                    "candidate_order_index_scaled_physical_advisory_bytes",
                    "candidate_order_index_row_overhead_advisory_bytes",
                    "candidate_order_index_advisory_bytes",
                    "advisory_required_bytes",
                    "candidate_order_source_index_name",
                    "candidate_order_source_index_columns",
                    "candidate_order_source_index_partial",
                    "candidate_order_source_index_dbstat_page_count",
                    "candidate_order_source_index_dbstat_page_size",
                    "candidate_order_source_index_dbstat_physical_bytes",
                    "candidate_order_source_index_dbstat_payload_bytes",
                    "candidate_order_source_index_dbstat_unused_bytes",
                    "candidate_order_source_index_dbstat_max_payload_bytes",
                    "candidate_order_source_index_dbstat_cell_upper_count",
                    "candidate_order_source_index_structural_overhead_bytes",
                    "source_index_name",
                    "source_query_plan",
                    "source_query_plan_uses_index",
                    "source_query_plan_uses_range_search",
                    "source_query_plan_full_table_scan_detected",
                )
            },
            "history_state": history_state,
            "history_storage_schema_version": (
                previous_storage_schema_version or None
            ),
            "history_storage_compatible": history_storage_compatible,
            "history_high_water_bytes": previous_high_water,
            "history_granted_cap_bytes": previous_grant,
            "history_cap_hit": cap_hit,
            "history_copy_completed": completed,
            "baseline_required_bytes": baseline_required,
            "allocation_weight_bytes": 0,
            "granted_cap_bytes": 0,
            "borrowed_shared_pool_bytes": 0,
            "advisory_shortfall_bytes": 0,
            "evidence_sources": [
                "advisory_source_demand",
                *( ["previous_worker_high_water"] if previous else [] ),
            ],
        }
    total_cap = max(0, int(total_cap_bytes))
    all_advisory_estimates_pinned_read_view_bound = all(
        (report.get("advisory_evidence") or {}).get(
            "source_measurement_trust_boundary"
        )
        == "same_pinned_read_view_as_copy"
        and (report.get("advisory_evidence") or {}).get(
            "estimate_started_after_pin"
        )
        is True
        and (report.get("advisory_evidence") or {}).get(
            "estimate_completed_before_copy"
        )
        is True
        and bool(
            (report.get("advisory_evidence") or {}).get(
                "pinned_read_view_id"
            )
        )
        and bool(
            (report.get("advisory_evidence") or {}).get(
                "pinned_read_view_role"
            )
        )
        for report in target_reports.values()
    )
    all_advisory_queries_bounded = all(
        report["advisory_query_bounded"] is True
        for report in target_reports.values()
    )
    physical_upper_bound_claimed = any(
        report["physical_upper_bound_claimed"] is True
        for report in target_reports.values()
    )
    capacity_sufficient = bool(
        total_cap > 0
        and baseline_total <= total_cap
        and (
            not history_validation_required
            or history_report.get("accepted") is True
        )
        and all_advisory_queries_bounded
        and not physical_upper_bound_claimed
        and (
            not require_pinned_view_binding
            or all_advisory_estimates_pinned_read_view_bound
        )
    )
    residual_pool = max(0, total_cap - baseline_total)
    grants = {
        target: int(report["baseline_required_bytes"])
        for target, report in target_reports.items()
    }
    borrowing_priority = tuple(cap_hit_targets or active_targets)
    allocation_weights = {
        target: max(
            SHARED_STAGE_PAGE_SIZE,
            int(target_reports[target]["advisory_required_bytes"]),
            int(target_reports[target]["history_granted_cap_bytes"]),
        )
        for target in borrowing_priority
    }
    residual_allocations = (
        allocate_shared_stage_residual(
            residual_bytes=residual_pool,
            priority_targets=borrowing_priority,
            weights=allocation_weights,
        )
        if capacity_sufficient
        else {target: 0 for target in borrowing_priority}
    )
    for target in active_targets:
        baseline = int(target_reports[target]["baseline_required_bytes"])
        extra = int(residual_allocations.get(target, 0))
        grant = baseline + extra
        grants[target] = grant
        target_reports[target]["allocation_weight_bytes"] = int(
            allocation_weights.get(target, 0)
        )
        target_reports[target]["granted_cap_bytes"] = grant
        target_reports[target]["borrowed_shared_pool_bytes"] = extra
        target_reports[target]["advisory_shortfall_bytes"] = max(
            0,
            int(target_reports[target]["advisory_required_bytes"]) - grant,
        )
    total_granted = sum(grants.values())
    advisory_demand_total = sum(
        int(report["advisory_required_bytes"])
        for report in target_reports.values()
    )
    allocation_weight_total = sum(allocation_weights.values())
    history_used = any(
        report.get("history_storage_compatible") is True
        for report in target_reports.values()
    )
    plan = {
        "schema_version": SHARED_STAGE_BUDGET_SCHEMA_VERSION,
        "allocation_mode": SHARED_STAGE_BUDGET_ALLOCATION_MODE,
        "hash_canonicalization": SHARED_STAGE_HASH_CANONICALIZATION,
        "attempt_id": str(attempt_id or "")[:80] or None,
        "generated_at": utc_iso(),
        "page_size": SHARED_STAGE_PAGE_SIZE,
        "total_cap_bytes": total_cap,
        "active_targets": list(active_targets),
        "minimum_total_bytes": sum(
            shared_stage_target_minimum_bytes(target)
            for target in active_targets
        ),
        "baseline_required_total_bytes": baseline_total,
        "advisory_demand_total_bytes": advisory_demand_total,
        "residual_pool_bytes": residual_pool,
        "borrowing_priority_targets": list(borrowing_priority),
        "allocation_weight_total_bytes": allocation_weight_total,
        "history_used": history_used,
        "history_reason": (
            history_report.get("reason")
            if history_used
            else (
                "history_storage_schema_incompatible"
                if history_report.get("accepted") is True
                else history_report.get("reason")
            )
        ),
        "history_attempt_id": (
            history_report.get("attempt_id") if history_used else None
        ),
        "history_evidence_sha256": (
            history_report.get("evidence_sha256") if history_used else None
        ),
        "history_anchor_schema_version": (
            history_report.get("anchor_schema_version")
            if history_used
            else None
        ),
        "history_lineage_validated": bool(
            history_used and history_report.get("accepted") is True
        ),
        "fixed_percentage_allocation_used": False,
        "pinned_read_view_binding_required": bool(
            require_pinned_view_binding
        ),
        "all_advisory_estimates_pinned_read_view_bound": (
            all_advisory_estimates_pinned_read_view_bound
        ),
        "all_advisory_queries_bounded": all_advisory_queries_bounded,
        "physical_upper_bound_claimed": physical_upper_bound_claimed,
        "global_hard_cap_enforced": True,
        "per_target_max_page_count_enforced": True,
        "capacity_sufficient_basis": (
            "minimum_and_verified_history_high_water"
        ),
        "targets": target_reports,
        "total_granted_bytes": total_granted,
        "grants_sum_matches_total_cap": total_granted == total_cap,
        "capacity_sufficient": capacity_sufficient,
        "accepted": bool(
            capacity_sufficient
            and total_granted == total_cap
            and (
                not require_pinned_view_binding
                or all_advisory_estimates_pinned_read_view_bound
            )
            and all(
                int(report["granted_cap_bytes"])
                >= int(report["baseline_required_bytes"])
                >= int(report["minimum_cap_bytes"])
                for report in target_reports.values()
            )
        ),
        "actual_total_bytes": None,
        "unconsumed_bytes": None,
        "all_targets_within_grant": None,
        "targets_exceeding_advisory": None,
        "advisory_miss_count": None,
        "no_unregistered_stage_files": None,
        "cleanup_completed": None,
    }
    plan["plan_sha256"] = shared_stage_budget_plan_sha256(plan)
    return plan


def disk_preflight(
    root: Path,
    min_free_after_gib: float,
    max_output_gib: float,
    *,
    parallel_stage_tables: tuple[str, ...] | list[str] | None = None,
    shared_stage_estimates: dict[str, Any] | None = None,
    shared_stage_history: dict[str, Any] | None = None,
    shared_stage_history_anchor: dict[str, Any] | None = None,
    attempt_id: str | None = None,
) -> dict[str, Any]:
    root.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(root)
    bounded_output = int(float(max_output_gib) * 1024**3)
    reserve = int(float(min_free_after_gib) * 1024**3)
    active_stage_tables = tuple(
        PARALLEL_PAPER_STAGE_TABLES
        if parallel_stage_tables is None
        else (str(table) for table in parallel_stage_tables)
    )
    if not parallel_paper_stage_inventory_valid(active_stage_tables):
        raise ValueError(
            f"invalid parallel paper stage inventory for disk preflight: "
            f"{active_stage_tables}"
        )
    omitted_optional_stages = tuple(
        table
        for table in PARALLEL_PAPER_OPTIONAL_STAGE_TABLES
        if table not in active_stage_tables
    )
    raw_stage_cap = max(0, int(usage.free) - bounded_output - reserve)
    total_stage_cap = (
        raw_stage_cap // SHARED_STAGE_PAGE_SIZE * SHARED_STAGE_PAGE_SIZE
    )
    alignment_reserve = raw_stage_cap - total_stage_cap
    if shared_stage_estimates is None:
        active_targets = shared_stage_target_names(active_stage_tables)
        shared_stage_estimates = {
            "schema_version": SHARED_STAGE_BUDGET_SCHEMA_VERSION,
            "generated_at": utc_iso(),
            "active_targets": list(active_targets),
            "all_advisory_queries_bounded": True,
            "physical_upper_bound_claimed": False,
            "targets": {
                target: {
                    "target": target,
                    "source_table": target,
                    "strategy": "minimum_only_advisory_fallback",
                    "query_bounded": True,
                    "physical_upper_bound_claimed": False,
                    "advisory_schema_version": (
                        SHARED_STAGE_ADVISORY_SCHEMA_VERSION
                    ),
                    "advisory_formula": SHARED_STAGE_ADVISORY_FORMULA,
                    "advisory_required_bytes": (
                        shared_stage_target_minimum_bytes(target)
                    ),
                    "minimum_cap_bytes": (
                        shared_stage_target_minimum_bytes(target)
                    ),
                }
                for target in active_targets
            },
        }
    shared_budget = build_shared_stage_budget_plan(
        total_cap_bytes=total_stage_cap,
        parallel_stage_tables=active_stage_tables,
        estimates=shared_stage_estimates,
        history=shared_stage_history,
        history_anchor=shared_stage_history_anchor,
        attempt_id=attempt_id,
    )
    candidate_stage_cap = int(
        shared_budget["targets"][SHARED_STAGE_TARGET_CANDIDATE][
            "granted_cap_bytes"
        ]
    )
    parallel_stage_caps = {
        table: int(
            shared_budget["targets"][table]["granted_cap_bytes"]
        )
        for table in active_stage_tables
    }
    estimated_peak = bounded_output + int(shared_budget["total_granted_bytes"])
    estimated_free_at_peak = int(usage.free) - estimated_peak
    accepted = bool(
        bounded_output > 0
        and shared_budget.get("accepted") is True
        and int(shared_budget["total_granted_bytes"]) == total_stage_cap
        and candidate_stage_cap >= MIN_CANDIDATE_STAGE_CAP_BYTES
        and all(
            parallel_stage_caps.get(table, 0)
            >= MIN_PARALLEL_PAPER_STAGE_CAP_BYTES
            for table in active_stage_tables
        )
        and estimated_free_at_peak >= reserve + alignment_reserve
    )
    return {
        "free_bytes": int(usage.free),
        "selective_snapshot_output_cap_bytes": bounded_output,
        "temporary_full_backup_bytes": 0,
        "temporary_stage_raw_cap_bytes": raw_stage_cap,
        "temporary_stage_alignment_reserve_bytes": alignment_reserve,
        "temporary_stage_total_cap_bytes": total_stage_cap,
        "temporary_candidate_stage_cap_bytes": candidate_stage_cap,
        "temporary_parallel_paper_stage_cap_bytes": parallel_stage_caps,
        "temporary_paper_decision_stage_cap_bytes": parallel_stage_caps.get(
            PAPER_DECISION_STAGE_TABLE,
            0,
        ),
        "configured_parallel_paper_stage_tables": list(
            PARALLEL_PAPER_STAGE_TABLES
        ),
        "parallel_paper_stage_tables": list(active_stage_tables),
        "omitted_optional_parallel_paper_stage_tables": list(
            omitted_optional_stages
        ),
        "candidate_stage_budget_mode": CANDIDATE_STAGE_BUDGET_MODE,
        "candidate_stage_minimum_cap_bytes": MIN_CANDIDATE_STAGE_CAP_BYTES,
        "parallel_paper_stage_minimum_cap_bytes": MIN_PARALLEL_PAPER_STAGE_CAP_BYTES,
        "paper_decision_stage_minimum_cap_bytes": MIN_PAPER_DECISION_STAGE_CAP_BYTES,
        "shared_stage_budget": shared_budget,
        "fixed_percentage_allocation_used": False,
        "estimated_peak_working_bytes": estimated_peak,
        "required_reserve_bytes": reserve,
        "estimated_free_after_bytes": int(usage.free) - bounded_output,
        "estimated_free_at_peak_bytes": estimated_free_at_peak,
        "fail_closed_on_insufficient_space": True,
        "accepted": accepted,
    }


def apply_shared_stage_budget_to_preflight(
    preflight: dict[str, Any],
    shared_budget: dict[str, Any],
    *,
    active_stage_tables: tuple[str, ...],
) -> dict[str, Any]:
    """Replace provisional minimum grants with the pinned-view stage plan."""
    if not isinstance(preflight, dict) or not isinstance(shared_budget, dict):
        raise ValueError("shared stage preflight evidence missing")
    expected_targets = shared_stage_target_names(active_stage_tables)
    targets = shared_budget.get("targets")
    total_cap = int(preflight.get("temporary_stage_total_cap_bytes") or 0)
    if (
        tuple(shared_budget.get("active_targets") or ()) != expected_targets
        or not isinstance(targets, dict)
        or set(targets) != set(expected_targets)
        or int(shared_budget.get("total_cap_bytes") or 0) != total_cap
        or int(shared_budget.get("total_granted_bytes") or 0) != total_cap
        or shared_budget.get("grants_sum_matches_total_cap") is not True
    ):
        raise ValueError("shared stage pinned plan does not match disk cap")
    candidate_cap = int(
        targets[SHARED_STAGE_TARGET_CANDIDATE]["granted_cap_bytes"]
    )
    parallel_caps = {
        table: int(targets[table]["granted_cap_bytes"])
        for table in active_stage_tables
    }
    output_cap = int(
        preflight.get("selective_snapshot_output_cap_bytes") or 0
    )
    free_bytes = int(preflight.get("free_bytes") or 0)
    reserve = int(preflight.get("required_reserve_bytes") or 0)
    alignment_reserve = int(
        preflight.get("temporary_stage_alignment_reserve_bytes") or 0
    )
    estimated_peak = output_cap + total_cap
    estimated_free_at_peak = free_bytes - estimated_peak
    preflight.update(
        {
            "temporary_candidate_stage_cap_bytes": candidate_cap,
            "temporary_parallel_paper_stage_cap_bytes": parallel_caps,
            "temporary_paper_decision_stage_cap_bytes": parallel_caps.get(
                PAPER_DECISION_STAGE_TABLE,
                0,
            ),
            "shared_stage_budget": json.loads(json.dumps(shared_budget)),
            "estimated_peak_working_bytes": estimated_peak,
            "estimated_free_at_peak_bytes": estimated_free_at_peak,
            "accepted": bool(
                output_cap > 0
                and shared_budget.get("accepted") is True
                and candidate_cap >= MIN_CANDIDATE_STAGE_CAP_BYTES
                and all(
                    parallel_caps.get(table, 0)
                    >= MIN_PARALLEL_PAPER_STAGE_CAP_BYTES
                    for table in active_stage_tables
                )
                and estimated_free_at_peak
                >= reserve + alignment_reserve
            ),
        }
    )
    return preflight


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
    expected_lease_identity = _snapshot_worker_lease_identity(root)
    authorized: list[tuple[Path, dict[str, Any], tuple[int, int]]] = []
    for path in sorted(snapshots.iterdir(), key=lambda item: item.name):
        if not PARTIAL_SNAPSHOT_NAME_RE.fullmatch(path.name):
            continue
        record, directory_identity = _authorize_snapshot_partial_cleanup(
            path,
            expected_lease_identity=expected_lease_identity,
        )
        authorized.append((path, record, directory_identity))
    if _snapshot_worker_lease_identity(root) != expected_lease_identity:
        raise SnapshotWorkerOwnerInvalidError(
            "lease_changed_after_partial_authorization"
        )
    expected_inventory = {
        path.name: directory_identity
        for path, _record, directory_identity in authorized
    }
    current_inventory: dict[str, tuple[int, int]] = {}
    for candidate in snapshots.iterdir():
        if not PARTIAL_SNAPSHOT_NAME_RE.fullmatch(candidate.name):
            continue
        if candidate.is_symlink() or not candidate.is_dir():
            raise SnapshotWorkerOwnerInvalidError(
                "partial_inventory_file_type"
            )
        candidate_stat = candidate.stat()
        current_inventory[candidate.name] = (
            int(candidate_stat.st_dev),
            int(candidate_stat.st_ino),
        )
    if current_inventory != expected_inventory:
        raise SnapshotWorkerOwnerInvalidError(
            "partial_inventory_changed_after_authorization"
        )
    for path, expected_record, expected_directory_identity in authorized:
        if path.is_symlink() or not path.is_dir():
            raise SnapshotWorkerOwnerInvalidError(
                "partial_changed_after_authorization"
            )
        current_stat = path.stat()
        if (
            int(current_stat.st_dev),
            int(current_stat.st_ino),
        ) != expected_directory_identity:
            raise SnapshotWorkerOwnerInvalidError(
                "partial_replaced_after_authorization"
            )
        if _load_snapshot_partial_owner(path) != expected_record:
            raise SnapshotWorkerOwnerInvalidError(
                "partial_owner_changed_after_authorization"
            )
    if _snapshot_worker_lease_identity(root) != expected_lease_identity:
        raise SnapshotWorkerOwnerInvalidError(
            "lease_changed_before_partial_cleanup"
        )
    removed = []
    for index, (
        path,
        expected_record,
        expected_directory_identity,
    ) in enumerate(authorized):
        expected_remaining = {
            candidate.name: directory_identity
            for candidate, _record, directory_identity in authorized[index:]
        }
        current_remaining: dict[str, tuple[int, int]] = {}
        for candidate in snapshots.iterdir():
            if not PARTIAL_SNAPSHOT_NAME_RE.fullmatch(candidate.name):
                continue
            if candidate.is_symlink() or not candidate.is_dir():
                raise SnapshotWorkerOwnerInvalidError(
                    "partial_inventory_file_type"
                )
            candidate_stat = candidate.stat()
            current_remaining[candidate.name] = (
                int(candidate_stat.st_dev),
                int(candidate_stat.st_ino),
            )
        if current_remaining != expected_remaining:
            raise SnapshotWorkerOwnerInvalidError(
                "partial_inventory_changed_during_cleanup"
            )
        if path.is_symlink() or not path.is_dir():
            raise SnapshotWorkerOwnerInvalidError("partial_changed_before_cleanup")
        current_stat = path.stat()
        current_directory_identity = (
            int(current_stat.st_dev),
            int(current_stat.st_ino),
        )
        if current_directory_identity != expected_directory_identity:
            raise SnapshotWorkerOwnerInvalidError("partial_replaced_before_cleanup")
        if _load_snapshot_partial_owner(path) != expected_record:
            raise SnapshotWorkerOwnerInvalidError("partial_owner_changed_before_cleanup")
        if _snapshot_worker_lease_identity(root) != expected_lease_identity:
            raise SnapshotWorkerOwnerInvalidError(
                "lease_changed_before_partial_delete"
            )
        shutil.rmtree(path)
        removed.append(str(path))
    if _interrupted_snapshot_partial_exists(root):
        raise SnapshotWorkerOwnerInvalidError(
            "partial_inventory_changed_during_cleanup"
        )
    return removed


def _build_snapshot_bundle_owned(
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
    previous_shared_stage_budget: dict[str, Any] | None = None,
    previous_shared_stage_budget_anchor: dict[str, Any] | None = None,
    partial_owner: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if partial_owner is None:
        raise SnapshotWorkerOwnerInvalidError("partial_owner_required")
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
    if long_history_hours > MAX_RESEARCH_HISTORY_HOURS:
        raise ValueError("long_history_hours must not exceed the 30-day research retention cap")
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
    bootstrap_dir = snapshots_root / (
        f".{sid}.partial-bootstrap-{os.urandom(4).hex()}"
    )
    bootstrap_dir.mkdir()
    try:
        _write_snapshot_partial_owner(
            bootstrap_dir,
            snapshot_id=sid,
            owner=partial_owner,
            bootstrap=True,
        )
        os.replace(bootstrap_dir, partial_dir)
        fsync_directory(snapshots_root)
    except BaseException:
        if bootstrap_dir.exists():
            shutil.rmtree(bootstrap_dir)
        if partial_dir.exists():
            shutil.rmtree(partial_dir)
        raise
    preflight: dict[str, Any] | None = None
    shared_budget_plan: dict[str, Any] | None = None
    try:
        source_page_reports = inspect_source_page_reports(
            source_paths,
            busy_timeout_ms=int(source_busy_timeout_ms),
        )
        inspected_parallel_stage_tables = tuple(
            source_page_reports["paper"].get(
                "parallel_paper_stage_tables"
            )
            or ()
        )
        if not parallel_paper_stage_inventory_valid(
            inspected_parallel_stage_tables
        ):
            raise RuntimeError(
                "parallel_paper_stage_failed:source_inventory_invalid"
            )
        # This first pass proves only the global disk cap and minimum stage
        # inventory.  Per-target grants are deliberately deferred until the
        # exact paper read views are pinned; no production capacity decision is
        # taken from an earlier, independently opened connection.
        preflight = disk_preflight(
            root,
            min_free_after_gib,
            max_output_gib,
            parallel_stage_tables=inspected_parallel_stage_tables,
            shared_stage_history=previous_shared_stage_budget,
            shared_stage_history_anchor=(
                previous_shared_stage_budget_anchor
            ),
            attempt_id=sid,
        )
        if not preflight["accepted"]:
            raise RuntimeError(f"insufficient disk for evaluator snapshot: {preflight}")
        budget_plan = database_output_budget_plan(max_output_gib, source_page_reports)
        output_budgets = budget_plan["database_budget_bytes"]
        try:
            database_reports = snapshot_all_concurrently(
                source_paths,
                partial_dir,
                source_page_reports,
                review_lower_epoch=review_lower_epoch,
                long_lower_epoch=long_lower_epoch,
                upper_epoch=selection_upper_epoch,
                database_budgets=output_budgets,
                expected_parallel_paper_stage_tables=(
                    inspected_parallel_stage_tables
                ),
                busy_timeout_ms=int(source_busy_timeout_ms),
                max_source_read_lock_sec=float(max_source_read_lock_sec),
                shared_stage_total_cap_bytes=int(
                    preflight["temporary_stage_total_cap_bytes"]
                ),
                shared_stage_history=previous_shared_stage_budget,
                shared_stage_history_anchor=(
                    previous_shared_stage_budget_anchor
                ),
                shared_stage_attempt_id=sid,
            )
        except BaseException as snapshot_exc:
            pinned_plan = getattr(
                snapshot_exc,
                "shared_stage_budget_plan",
                None,
            )
            if isinstance(pinned_plan, dict):
                shared_budget_plan = pinned_plan
            raise
        paper_plan_report = database_reports.get("paper") or {}
        shared_budget_plan = paper_plan_report.pop(
            "shared_stage_budget_plan",
            None,
        )
        if not isinstance(shared_budget_plan, dict):
            raise RuntimeError("shared_stage_budget_plan_missing")
        pinned_read_views = [
            view
            for report in database_reports.values()
            for view in (
                report.get("pinned_read_views")
                or [report["pinned_read_view"]]
            )
        ]
        pin_midpoints = [
            float(view["pinned_midpoint_epoch"])
            for view in pinned_read_views
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
        candidate_projection_after_source_read_lock_release = all(
            report.get("candidate_projection_after_source_read_lock_release") is True
            for report in database_reports.values()
        )
        candidate_stage_removed_before_publish = all(
            report.get("temporary_candidate_stage_removed_before_publish") is True
            for report in database_reports.values()
        )
        paper_report = database_reports.get("paper") or {}
        if not isinstance(shared_budget_plan, dict):
            raise RuntimeError("shared_stage_budget_plan_missing")
        shared_stage_budget = finalize_shared_stage_budget_success(
            shared_budget_plan,
            paper_report,
        )
        apply_shared_stage_budget_to_preflight(
            preflight,
            shared_stage_budget,
            active_stage_tables=inspected_parallel_stage_tables,
        )
        shared_stage_budget_passed = bool(
            shared_stage_budget.get("accepted") is True
            and shared_stage_budget.get(
                "pinned_read_view_binding_required"
            )
            is True
            and shared_stage_budget.get(
                "all_advisory_estimates_pinned_read_view_bound"
            )
            is True
            and paper_report.get(
                "shared_stage_estimates_bound_to_copy_read_views"
            )
            is True
            and preflight.get("accepted") is True
        )
        paper_selected_tables = paper_report.get("selected_tables") or {}
        active_parallel_stage_tables = tuple(
            paper_report.get("parallel_paper_stage_tables") or ()
        )
        parallel_paper_stage_count = int(
            paper_report.get("parallel_paper_stage_count") or 0
        )
        optional_parallel_stage_absence_valid = all(
            table in active_parallel_stage_tables
            or (
                (paper_selected_tables.get(table) or {}).get("included") is False
                and (paper_selected_tables.get(table) or {}).get("required") is False
                and (paper_selected_tables.get(table) or {}).get("reason")
                == "optional_source_table_missing"
            )
            for table in PARALLEL_PAPER_OPTIONAL_STAGE_TABLES
        )
        parallel_paper_stage_inventory_passed = bool(
            parallel_paper_stage_inventory_valid(active_parallel_stage_tables)
            and parallel_paper_stage_count == len(active_parallel_stage_tables)
            and tuple((paper_report.get("parallel_paper_stages") or {}).keys())
            == active_parallel_stage_tables
            and optional_parallel_stage_absence_valid
        )
        parallel_paper_stages_all_pinned = (
            paper_report.get("parallel_paper_stages_all_pinned") is True
        )
        parallel_paper_stages_all_merged_after_source_read_lock_release = (
            paper_report.get(
                "parallel_paper_stages_all_merged_after_source_read_lock_release"
            )
            is True
        )
        parallel_paper_stages_all_removed_before_publish = (
            paper_report.get("parallel_paper_stages_all_removed_before_publish")
            is True
        )
        paper_decision_parallel_read_view_pinned = (
            paper_report.get("paper_decision_parallel_read_view_pinned") is True
        )
        paper_decision_parallel_stage_merged_after_source_read_lock_release = (
            paper_report.get(
                "paper_decision_parallel_stage_merged_after_source_read_lock_release"
            )
            is True
        )
        paper_decision_parallel_stage_removed_before_publish = (
            paper_report.get("paper_decision_parallel_stage_removed_before_publish")
            is True
        )
        database_payload_size_bytes = sum(
            int(report["snapshot_size_bytes"]) for report in database_reports.values()
        )
        output_cap_bytes = int(preflight["selective_snapshot_output_cap_bytes"])
        payload_directory = snapshot_directory_report(
            partial_dir,
            include_manifest=False,
            allow_partial_owner=True,
        )
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
            and candidate_projection_after_source_read_lock_release
            and candidate_stage_removed_before_publish
            and parallel_paper_stage_inventory_passed
            and parallel_paper_stages_all_pinned
            and parallel_paper_stages_all_merged_after_source_read_lock_release
            and parallel_paper_stages_all_removed_before_publish
            and paper_decision_parallel_read_view_pinned
            and paper_decision_parallel_stage_merged_after_source_read_lock_release
            and paper_decision_parallel_stage_removed_before_publish
            and shared_stage_budget_passed
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
            "method": "coordinated_pinned_view_estimate_then_compact_bounded_selective_extract",
            "bounded_selective_snapshot": True,
            "read_views_pinned_before_copy": True,
            "shared_stage_estimates_bound_to_copy_read_views": (
                paper_report.get(
                    "shared_stage_estimates_bound_to_copy_read_views"
                )
                is True
            ),
            "max_source_read_lock_sec": float(max_source_read_lock_sec),
            "source_read_lock_budget_passed": source_read_lock_budget_passed,
            "indexes_built_after_source_read_lock_release": (
                indexes_built_after_source_read_lock_release
            ),
            "candidate_projection_after_source_read_lock_release": (
                candidate_projection_after_source_read_lock_release
            ),
            "candidate_stage_removed_before_publish": candidate_stage_removed_before_publish,
            "parallel_paper_stage_schema_version": (
                PARALLEL_PAPER_STAGE_SCHEMA_VERSION
            ),
            "parallel_paper_stage_tables": list(active_parallel_stage_tables),
            "parallel_paper_stage_count": parallel_paper_stage_count,
            "parallel_paper_stage_inventory_passed": (
                parallel_paper_stage_inventory_passed
            ),
            "parallel_paper_stages_all_pinned": parallel_paper_stages_all_pinned,
            "parallel_paper_stages_all_merged_after_source_read_lock_release": (
                parallel_paper_stages_all_merged_after_source_read_lock_release
            ),
            "parallel_paper_stages_all_removed_before_publish": (
                parallel_paper_stages_all_removed_before_publish
            ),
            "paper_decision_parallel_read_view_pinned": (
                paper_decision_parallel_read_view_pinned
            ),
            "paper_decision_parallel_stage_merged_after_source_read_lock_release": (
                paper_decision_parallel_stage_merged_after_source_read_lock_release
            ),
            "paper_decision_parallel_stage_removed_before_publish": (
                paper_decision_parallel_stage_removed_before_publish
            ),
            "source_mutation_free": source_mutation_free,
            "copy_mode": "parallel_pinned_estimate_and_heavy_paper_stages_plus_bounded_selective_extract",
            "pinned_read_view_count": len(pinned_read_views),
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
            "shared_stage_budget": shared_stage_budget,
            "shared_stage_budget_passed": shared_stage_budget_passed,
            "databases": database_reports,
            "accepted": accepted,
            "immutable": True,
            "partial_artifacts_absent": True,
            "active_database_reads_allowed_for_autoloop": False,
            "promotion_allowed": False,
        }
        if not accepted:
            raise RuntimeError(f"cross-database snapshot acceptance failed: {manifest}")
        bind_numeric_evidence_schema(manifest)
        require_numeric_evidence_schema(manifest, require_binding=True)
        write_bounded_manifest(
            partial_dir,
            manifest,
            output_cap_bytes=output_cap_bytes,
            allow_partial_owner=True,
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
        require_numeric_evidence_schema(manifest, require_binding=True)
        try:
            os.replace(partial_dir, final_dir)
            fsync_directory(snapshots_root)
            final_marker_path = snapshot_partial_owner_path(final_dir)
            final_marker_path.unlink()
            fsync_directory(final_dir)
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
            require_numeric_evidence_schema(manifest, require_binding=True)
            atomic_json(latest_path, manifest)
        except Exception as exc:
            manifest["retention"]["status_write_error"] = f"{type(exc).__name__}:{exc}"
        latest_manifest = manifest
        return latest_manifest
    except BaseException as exc:
        shared_failure_evidence = capture_shared_stage_budget_failure(
            partial_dir,
            shared_budget_plan,
            exc,
        )
        worker_restart_required = exception_requires_worker_restart(exc)
        cleanup_deferred = bool(
            worker_restart_required and partial_dir.exists()
        )
        cleanup_completed = not partial_dir.exists()
        if not worker_restart_required:
            cleanup_completed = True
            try:
                if partial_dir.exists():
                    shutil.rmtree(partial_dir)
            except Exception:
                cleanup_completed = False
        if isinstance(shared_failure_evidence, dict):
            shared_failure_evidence["cleanup_completed"] = cleanup_completed
            shared_failure_evidence["stage_files_removed"] = bool(
                cleanup_completed and not partial_dir.exists()
            )
            shared_failure_evidence["evidence_sha256"] = (
                shared_stage_budget_evidence_sha256(
                    shared_failure_evidence
                )
            )
            setattr(exc, "shared_stage_budget", shared_failure_evidence)
        if cleanup_deferred:
            setattr(exc, "cleanup_deferred_until_worker_restart", True)
        if not cleanup_completed:
            if worker_restart_required:
                raise
            cleanup_exc = RuntimeError("shared_stage_cleanup_failed")
            if isinstance(shared_failure_evidence, dict):
                setattr(
                    cleanup_exc,
                    "shared_stage_budget",
                    shared_failure_evidence,
                )
            raise cleanup_exc from exc
        raise


def _build_snapshot_bundle_entry(
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
    previous_shared_stage_budget: dict[str, Any] | None = None,
    previous_shared_stage_budget_anchor: dict[str, Any] | None = None,
    partial_owner: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if set(sources) != set(DATABASE_SPECS):
        raise ValueError(f"sources must be exactly {sorted(DATABASE_SPECS)}")
    normalized_review_hours = float(review_history_hours)
    normalized_long_hours = float(long_history_hours)
    if normalized_review_hours < 72:
        raise ValueError("review_history_hours must cover at least 72 hours")
    if normalized_long_hours < normalized_review_hours:
        raise ValueError("long_history_hours must be >= review_history_hours")
    if normalized_long_hours > MAX_RESEARCH_HISTORY_HOURS:
        raise ValueError(
            "long_history_hours must not exceed the 30-day research retention cap"
        )
    if int(source_busy_timeout_ms) < 0:
        raise ValueError("source_busy_timeout_ms must be non-negative")
    if float(max_source_read_lock_sec) <= 0:
        raise ValueError("max_source_read_lock_sec must be positive")
    if snapshot_id is not None and not SNAPSHOT_NAME_RE.fullmatch(snapshot_id):
        raise ValueError(f"invalid snapshot id: {snapshot_id}")
    root = Path(out_root).expanduser().resolve()
    direct_owner = partial_owner is None
    root_key = str(root)
    prior_lease_handle = _WORKER_OWNER_LEASES.get(root_key)
    owner = partial_owner
    if direct_owner:
        owner = ensure_snapshot_worker_owner(root)
        cleanup_interrupted_partials(root)
    direct_lease_handle = _WORKER_OWNER_LEASES.get(root_key)
    release_direct_lease = bool(
        direct_owner
        and direct_lease_handle is not None
        and direct_lease_handle is not prior_lease_handle
    )
    try:
        manifest = _build_snapshot_bundle_owned(
            sources=sources,
            out_root=out_root,
            repo_root=repo_root,
            max_skew_sec=max_skew_sec,
            min_free_after_gib=min_free_after_gib,
            max_output_gib=max_output_gib,
            review_history_hours=normalized_review_hours,
            long_history_hours=normalized_long_hours,
            source_busy_timeout_ms=source_busy_timeout_ms,
            max_source_read_lock_sec=max_source_read_lock_sec,
            keep_previous=keep_previous,
            snapshot_id=snapshot_id,
            previous_shared_stage_budget=previous_shared_stage_budget,
            previous_shared_stage_budget_anchor=(
                previous_shared_stage_budget_anchor
            ),
            partial_owner=owner,
        )
    except BaseException as exc:
        if release_direct_lease and not exception_requires_worker_restart(exc):
            _release_direct_snapshot_worker_owner(root, owner)
        raise
    if release_direct_lease:
        _release_direct_snapshot_worker_owner(root, owner)
    return manifest


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
    previous_shared_stage_budget: dict[str, Any] | None = None,
    previous_shared_stage_budget_anchor: dict[str, Any] | None = None,
    partial_owner: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with _RUN_SNAPSHOT_ONCE_LOCK:
        return _build_snapshot_bundle_entry(
            sources=sources,
            out_root=out_root,
            repo_root=repo_root,
            max_skew_sec=max_skew_sec,
            min_free_after_gib=min_free_after_gib,
            max_output_gib=max_output_gib,
            review_history_hours=review_history_hours,
            long_history_hours=long_history_hours,
            source_busy_timeout_ms=source_busy_timeout_ms,
            max_source_read_lock_sec=max_source_read_lock_sec,
            keep_previous=keep_previous,
            snapshot_id=snapshot_id,
            previous_shared_stage_budget=previous_shared_stage_budget,
            previous_shared_stage_budget_anchor=(
                previous_shared_stage_budget_anchor
            ),
            partial_owner=partial_owner,
        )


def self_test() -> None:
    assert parallel_paper_stage_page_size(
        PARALLEL_PAPER_STAGE_BULK_PAGE_MIN_BUDGET_BYTES - SHARED_STAGE_PAGE_SIZE
    ) == SHARED_STAGE_PAGE_SIZE
    assert parallel_paper_stage_page_size(
        PARALLEL_PAPER_STAGE_BULK_PAGE_MIN_BUDGET_BYTES
    ) == PARALLEL_PAPER_STAGE_BULK_PAGE_SIZE
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        sources = {}
        definitions = {
            "signal": "CREATE TABLE premium_signals(id INTEGER, source_message_ts INTEGER)",
            "paper": (
                "CREATE TABLE candidate_shadow_observations("
                "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                "signal_id INTEGER NOT NULL,"
                "token_ca TEXT NOT NULL,"
                "signal_ts INTEGER,"
                "candidate_id TEXT NOT NULL,"
                "family TEXT,"
                "matched INTEGER NOT NULL,"
                "reason TEXT,"
                "observed_at INTEGER NOT NULL,"
                "payload_json TEXT NOT NULL,"
                "UNIQUE(signal_id,candidate_id));"
                "CREATE INDEX idx_candidate_shadow_obs_observed "
                "ON candidate_shadow_observations(observed_at);"
                "CREATE INDEX idx_candidate_shadow_obs_signal "
                "ON candidate_shadow_observations(signal_id);"
                "CREATE TABLE candidate_shadow_virtual_trades(signal_id INTEGER, observed_at INTEGER);"
                "CREATE INDEX idx_candidate_shadow_virtual_observed "
                "ON candidate_shadow_virtual_trades(observed_at);"
                "CREATE TABLE paper_decision_events(id INTEGER, event_ts INTEGER);"
                "CREATE INDEX idx_pde_event_ts ON paper_decision_events(event_ts);"
                "CREATE TABLE a_class_decision_events(id INTEGER, event_ts INTEGER);"
                "CREATE INDEX idx_a_class_decision_recent ON a_class_decision_events(event_ts);"
                "CREATE TABLE a_class_mode_runtime_state(id INTEGER, updated_at INTEGER);"
                "CREATE TABLE paper_trades(id INTEGER, entry_time INTEGER);"
                "CREATE TABLE opportunity_events(id INTEGER, event_ts INTEGER);"
                "CREATE INDEX idx_opportunity_events_recent ON opportunity_events(event_ts);"
                "CREATE TABLE opportunity_event_path_samples("
                "id INTEGER PRIMARY KEY, opportunity_key TEXT, sample_ts REAL, "
                "raw_payload_json TEXT, created_at REAL, updated_at REAL);"
                "CREATE INDEX idx_opportunity_path_samples_key_ts "
                "ON opportunity_event_path_samples(opportunity_key, sample_ts);"
                "CREATE INDEX idx_opportunity_path_samples_sample_ts "
                "ON opportunity_event_path_samples(sample_ts)"
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


def _run_snapshot_once(args: argparse.Namespace) -> dict[str, Any]:
    started = utc_iso()
    out_root_path = Path(args.out_root).expanduser().resolve()
    out_root_key = str(out_root_path)
    status_path = (
        Path(args.status_out).expanduser().resolve()
        if args.status_out
        else None
    )
    previous = read_json_object(status_path) if status_path else {}
    canonical_status_path = out_root_path / "snapshot_status.json"
    canonical_previous = (
        previous
        if status_path == canonical_status_path
        else read_json_object(canonical_status_path)
    )
    legacy_statuses = tuple(
        status
        for status in (canonical_previous, previous)
        if isinstance(status, dict) and status
    )
    poisoned_status = _WORKER_RESTART_POISONED_OUT_ROOTS.get(out_root_key)
    previous_worker_instance_id = str(
        previous.get("worker_instance_id") or ""
    )
    same_process_instance = (
        previous_worker_instance_id == WORKER_PROCESS_INSTANCE_ID
        if previous_worker_instance_id
        else previous.get("pid") == os.getpid()
    )
    if (
        poisoned_status is None
        and previous.get("worker_restart_required") is True
        and same_process_instance
    ):
        poisoned_status = previous
    if isinstance(poisoned_status, dict):
        status = json.loads(json.dumps(poisoned_status))
        status.update(
            {
                "running": False,
                "attempt_running": False,
                "status": "failed",
                "accepted": False,
                "worker_restart_required": True,
                "next_attempt_delay_sec": None,
                "next_attempt_at": None,
                "promotion_allowed": False,
            }
        )
        _WORKER_RESTART_POISONED_OUT_ROOTS[out_root_key] = status
        if status_path and read_json_object(status_path) != status:
            atomic_json(status_path, status)
        print(json.dumps(status, sort_keys=True), flush=True)
        return status
    previous_history_candidate = (
        previous.get("shared_stage_budget")
        if isinstance(previous.get("shared_stage_budget"), dict)
        else None
    )
    previous_history_anchor: dict[str, Any] | None = None
    if status_path and isinstance(previous_history_candidate, dict):
        try:
            previous_anchor_path = shared_stage_budget_anchor_path(
                status_path,
                previous_history_candidate.get("attempt_id"),
            )
            loaded_anchor = read_json_object(previous_anchor_path)
            previous_history_anchor = loaded_anchor or None
        except ValueError:
            previous_history_anchor = None
    legacy_unanchored_history = bool(
        previous_history_candidate
        and previous_history_anchor is None
        and previous.get("shared_stage_budget_anchor_required") is not True
    )
    previous_shared_stage_budget = (
        None if legacy_unanchored_history else previous_history_candidate
    )
    continuous_worker = int(args.max_runs) <= 0
    interval_sec = max(1, int(getattr(args, "interval_sec", 21600)))
    failure_retry_sec = max(
        MIN_FAILURE_RETRY_SEC,
        int(getattr(args, "failure_retry_sec", DEFAULT_FAILURE_RETRY_SEC)),
    )
    current_path = str(Path(args.out_root).expanduser().resolve() / "current")
    required_paper_journal_mode = str(
        getattr(args, "required_paper_journal_mode", "") or ""
    ).strip().upper()
    if required_paper_journal_mode not in {"", "DELETE", "WAL"}:
        raise ValueError(
            "required_paper_journal_mode must be DELETE, WAL, or empty"
        )
    try:
        previous_failure_count = max(
            0,
            int(previous.get("consecutive_failure_count") or 0),
        )
    except (TypeError, ValueError):
        previous_failure_count = 0
    previous_code_count_raw = previous.get("consecutive_failure_code_count")
    if previous_code_count_raw is None:
        previous_failure_code_count = (
            previous_failure_count if previous.get("last_failure_code") else 0
        )
    else:
        try:
            previous_failure_code_count = max(
                0,
                int(previous_code_count_raw or 0),
            )
        except (TypeError, ValueError):
            previous_failure_code_count = 0
    base_status = {
        "schema_version": WORKER_STATUS_SCHEMA_VERSION,
        "pid": os.getpid(),
        "worker_instance_id": WORKER_PROCESS_INSTANCE_ID,
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
        "consecutive_failure_count": previous_failure_count,
        "consecutive_failure_code_count": previous_failure_code_count,
        "success_interval_sec": interval_sec,
        "failure_retry_sec": failure_retry_sec,
        "next_attempt_delay_sec": None,
        "next_attempt_at": None,
        "status": "running",
        "accepted": False,
        "snapshot_id": None,
        "current": current_path,
        "last_accepted_snapshot": previous.get("last_accepted_snapshot"),
        "shared_stage_budget": previous_shared_stage_budget,
        "shared_stage_budget_anchor_required": True,
        "shared_stage_budget_anchor": (
            previous_history_anchor
            if previous_shared_stage_budget is not None
            else None
        ),
        "worker_restart_required": False,
        "cleanup_deferred_until_worker_restart": False,
        "worker_owner_path": str(snapshot_worker_owner_path(out_root_path)),
        "required_paper_journal_mode": (
            required_paper_journal_mode or None
        ),
        "paper_journal_mode": None,
        "promotion_allowed": False,
    }
    lock_acquired = False
    try:
        with exclusive_lock(Path(args.lock_file).expanduser().resolve()):
            lock_acquired = True
            worker_owner = ensure_snapshot_worker_owner(
                out_root_path,
                legacy_statuses=legacy_statuses,
            )
            paper_journal_mode = source_journal_mode(
                Path(args.paper_db).expanduser().resolve(),
                busy_timeout_ms=int(args.source_busy_timeout_ms),
            )
            base_status["paper_journal_mode"] = paper_journal_mode
            if status_path:
                atomic_json(status_path, base_status)
            if (
                required_paper_journal_mode
                and paper_journal_mode != required_paper_journal_mode
            ):
                raise RuntimeError(
                    "paper_source_journal_mode_mismatch:"
                    f"required={required_paper_journal_mode}:"
                    f"actual={paper_journal_mode}"
                )
            interrupted_partials_removed = cleanup_interrupted_partials(
                out_root_path
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
                previous_shared_stage_budget=previous_shared_stage_budget,
                previous_shared_stage_budget_anchor=previous_history_anchor,
                partial_owner=worker_owner,
            )
            finished = utc_iso()
            current_shared_stage_budget = manifest.get("shared_stage_budget")
            current_shared_stage_anchor = (
                write_shared_stage_budget_anchor(
                    status_path,
                    current_shared_stage_budget,
                )
                if status_path
                and isinstance(current_shared_stage_budget, dict)
                else None
            )
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
                "consecutive_failure_code_count": 0,
                "snapshot_id": manifest["snapshot_id"],
                "interrupted_partials_removed": interrupted_partials_removed,
                "last_accepted_snapshot": accepted_summary,
                "shared_stage_budget": current_shared_stage_budget,
                "shared_stage_budget_anchor": current_shared_stage_anchor,
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
        worker_restart_required = exception_requires_worker_restart(exc)
        cleanup_deferred = bool(
            getattr(exc, "cleanup_deferred_until_worker_restart", False)
        )
        current_failure_stage_budget = (
            shared_stage_budget_evidence_from_exception(exc)
        )
        failure_stage_budget = (
            current_failure_stage_budget or previous_shared_stage_budget
        )
        failure_stage_anchor = (
            None
            if isinstance(current_failure_stage_budget, dict)
            else previous_history_anchor
        )
        if (
            status_path
            and lock_acquired
            and isinstance(current_failure_stage_budget, dict)
            and current_failure_stage_budget.get("captured_before_cleanup") is True
            and current_failure_stage_budget.get("cleanup_completed") is True
            and current_failure_stage_budget.get("stage_files_removed") is True
            and current_failure_stage_budget.get("no_unregistered_stage_files") is True
        ):
            failure_stage_anchor = write_shared_stage_budget_anchor(
                status_path,
                current_failure_stage_budget,
            )
        consecutive_failure_count = int(base_status["consecutive_failure_count"]) + 1
        previous_failure_code = str(base_status.get("last_failure_code") or "")
        previous_failure_code_count = int(
            base_status.get("consecutive_failure_code_count") or 0
        )
        consecutive_failure_code_count = (
            previous_failure_code_count + 1
            if previous_failure_code == failure_code
            else 1
        )
        retry_status = {
            "accepted": False,
            "last_failure_code": failure_code,
            "consecutive_failure_count": consecutive_failure_count,
            "consecutive_failure_code_count": consecutive_failure_code_count,
        }
        next_attempt_delay = snapshot_next_attempt_delay_sec(
            retry_status,
            interval_sec=interval_sec,
            failure_retry_sec=failure_retry_sec,
        )
        status = {
            **base_status,
            "running": bool(
                continuous_worker
                and lock_acquired
                and not worker_restart_required
            ),
            "attempt_running": False,
            "finished_at": finished,
            "last_failure_at": finished,
            "last_failure_code": failure_code,
            "last_failure_details": failure_details,
            "shared_stage_budget": failure_stage_budget,
            "shared_stage_budget_anchor": failure_stage_anchor,
            "last_error": bounded_error_text(exc),
            "error_count": int(base_status["error_count"]) + 1,
            "status": "failed",
            "accepted": False,
            "consecutive_failure_count": consecutive_failure_count,
            "consecutive_failure_code_count": consecutive_failure_code_count,
            "error": bounded_error_text(exc),
            "status_artifact_preserved": not lock_acquired,
            "worker_restart_required": worker_restart_required,
            "cleanup_deferred_until_worker_restart": cleanup_deferred,
            "next_attempt_delay_sec": (
                next_attempt_delay
                if continuous_worker and not worker_restart_required
                else None
            ),
            "next_attempt_at": (
                utc_iso(time.time() + next_attempt_delay)
                if continuous_worker and not worker_restart_required
                else None
            ),
        }
        prior_worker_pid = _positive_process_pid(
            getattr(exc, "prior_worker_pid", None)
        )
        if prior_worker_pid is not None:
            status["prior_worker_pid"] = prior_worker_pid
            status["prior_worker_identity_source"] = str(
                getattr(exc, "prior_worker_identity_source", "unknown")
            )
        if hasattr(exc, "prior_worker_liveness"):
            status["prior_worker_liveness"] = str(
                getattr(exc, "prior_worker_liveness", "unknown")
            )
        if worker_restart_required:
            _WORKER_RESTART_POISONED_OUT_ROOTS[out_root_key] = json.loads(
                json.dumps(status)
            )
        if status_path and lock_acquired:
            atomic_json(status_path, status)
    if not continuous_worker and status.get("worker_restart_required") is not True:
        _release_snapshot_worker_lease(out_root_path)
    print(json.dumps(status, sort_keys=True), flush=True)
    return status


def run_snapshot_once(args: argparse.Namespace) -> dict[str, Any]:
    with _RUN_SNAPSHOT_ONCE_LOCK:
        return _run_snapshot_once(args)


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
        "--required-paper-journal-mode",
        choices=("DELETE", "WAL"),
        default="",
    )
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
        if last_status.get("worker_restart_required") is True:
            break
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
