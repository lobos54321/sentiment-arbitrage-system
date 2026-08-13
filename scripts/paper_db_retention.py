#!/usr/bin/env python3
"""Bound hot paper DB growth using value-aware retention.

Recent row-level evidence remains in SQLite for 24h/48h/72h reviews. Older
research rows are either fully archived, compacted, or reduced to manifest
summaries. Permanent trade evidence is never eligible for automated pruning.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import time
import uuid

from sqlite_write_coordinator import sqlite_single_writer
from paper_db_integrity_guard import require_unmarked_paper_db


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_ARCHIVE_DIR = PROJECT_ROOT / "data" / "archive" / "paper-db-retention"
MAX_RESEARCH_RETENTION_DAYS = 30.0
RETENTION_SCHEDULER_SCHEMA_VERSION = "paper_db_retention_scheduler.v1"
RETENTION_SCHEDULER_STATE_NAME = ".retention-scheduler.json"
RETENTION_SCHEDULER_INITIAL_TABLE = "opportunity_events"


def _env_bool(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.environ.get(name, str(default))))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class RetentionPolicy:
    table: str
    ts_expr: str
    days_env: str
    default_days: float
    description: str
    extra_where: str = "1=1"
    storage_class: str = "bounded_full_archive"
    archive_kind: str = "full"
    archive_days_env: str = "PAPER_DB_RETENTION_ARCHIVE_DAYS"
    default_archive_days: float = 30.0
    pressure_days: float | None = None
    compact_columns: tuple[str, ...] = ()
    summary_dimensions: tuple[str, ...] = ()
    required_archive_columns: tuple[str, ...] = ()
    maximum_days: float = MAX_RESEARCH_RETENTION_DAYS

    def days(self) -> float:
        return min(
            MAX_RESEARCH_RETENTION_DAYS,
            max(0.0, float(self.maximum_days)),
            max(0.0, _env_float(self.days_env, self.default_days)),
        )

    def archive_days(self) -> float:
        return min(
            MAX_RESEARCH_RETENTION_DAYS,
            max(0.0, _env_float(self.archive_days_env, self.default_archive_days)),
        )

    def effective_days(self, pressure_level: str) -> float:
        configured = self.days()
        if pressure_level not in {"hard", "critical"} or self.pressure_days is None:
            return configured
        return min(configured, max(0.0, float(self.pressure_days)))


PROTECTED_TABLES = frozenset(
    {
        "paper_trades",
        "canonical_trade_ledger",
        "paper_trade_entries",
        "paper_trade_exits",
        "paper_trade_fills",
        "a_class_mode_events",
        "a_class_mode_state",
        "a_class_mode_runtime_state",
        "a_class_mode_operator_audit",
        "runtime_mode_events",
        "final_entry_contract_events",
        "human_approvals",
        "operator_approvals",
        "oos_freeze_registry",
        "strategy_proposals",
    }
)


A_CLASS_COMPACT_COLUMNS = (
    "id",
    "event_ts",
    "token_ca",
    "symbol",
    "lifecycle_id",
    "route_bucket",
    "normalized_mode",
    "source_table",
    "source_id",
    "source_component",
    "source_reason",
    "opportunity_key",
    "source_dedup_key",
    "is_duplicate",
    "duplicate_of_id",
    "signal_ts",
    "opportunity_ts",
    "action",
    "grade",
    "size_sol",
    "score",
    "reason",
    "hard_blockers_json",
    "would_action",
    "expected_rr",
    "expected_upside_pct",
    "defined_risk_pct",
    "expected_rr_detail_json",
    "controller_action_json",
    "denominator_key",
    "block_cause",
    "recoverability",
    "classification_reason",
    "blocker_classifications_json",
    "quote_available",
    "quote_executable",
    "quote_clean",
    "route_available",
    "quote_source",
    "quote_age_sec",
    "data_confidence",
    "provider_reason",
    "evidence_status",
    "quote_failure_reason",
    "route_failure_reason",
    "liquidity_usd",
    "spread_pct",
    "provider_hydrate_outcome",
    "created_at",
)


PAPER_DECISION_COMPACT_COLUMNS = (
    "id",
    "event_ts",
    "signal_id",
    "token_ca",
    "symbol",
    "lifecycle_id",
    "trade_id",
    "signal_ts",
    "strategy_stage",
    "route",
    "component",
    "event_type",
    "decision",
    "reason",
    "data_source",
    "lifecycle_state",
    "vitality_score",
    "entry_bias",
    "created_at",
)


RETENTION_POLICIES = [
    RetentionPolicy(
        table="a_class_decision_events",
        ts_expr="event_ts",
        days_env="PAPER_DB_RETENTION_A_CLASS_DECISION_DAYS",
        default_days=4.0,
        pressure_days=3.0,
        description="high-volume A_CLASS decision evidence compacted after the 72h review floor",
        storage_class="bounded_compact_archive",
        archive_kind="compact",
        archive_days_env="PAPER_DB_RETENTION_A_CLASS_ARCHIVE_DAYS",
        default_archive_days=30.0,
        compact_columns=A_CLASS_COMPACT_COLUMNS,
        summary_dimensions=("action", "block_cause", "route_bucket", "source_component"),
        required_archive_columns=("event_ts", "token_ca", "action"),
    ),
    RetentionPolicy(
        table="paper_decision_events",
        ts_expr="event_ts",
        days_env="PAPER_DB_RETENTION_DECISION_DAYS",
        default_days=4.0,
        pressure_days=3.0,
        description="paper decision evidence compacted after the 72h review floor",
        storage_class="bounded_compact_archive",
        archive_kind="compact",
        archive_days_env="PAPER_DB_RETENTION_DECISION_ARCHIVE_DAYS",
        default_archive_days=30.0,
        compact_columns=PAPER_DECISION_COMPACT_COLUMNS,
        summary_dimensions=("component", "event_type", "decision", "reason"),
        required_archive_columns=("event_ts", "decision"),
    ),
    RetentionPolicy(
        table="candidate_shadow_observations",
        ts_expr="observed_at",
        days_env="PAPER_DB_RETENTION_CANDIDATE_OBSERVATION_DAYS",
        default_days=4.0,
        pressure_days=3.0,
        description="84-candidate row-level observations used by capture-first review",
        storage_class="bounded_full_archive",
        archive_kind="full",
        archive_days_env="PAPER_DB_RETENTION_CANDIDATE_OBSERVATION_ARCHIVE_DAYS",
        default_archive_days=30.0,
        summary_dimensions=("family", "candidate_id", "matched", "reason"),
        required_archive_columns=("signal_id", "candidate_id", "observed_at", "payload_json"),
    ),
    RetentionPolicy(
        table="candidate_shadow_virtual_trades",
        ts_expr="observed_at",
        days_env="PAPER_DB_RETENTION_CANDIDATE_VIRTUAL_DAYS",
        default_days=7.0,
        pressure_days=3.0,
        description="candidate virtual outcomes retained longer for secondary PnL and OOS checks",
        storage_class="bounded_full_archive",
        archive_kind="full",
        archive_days_env="PAPER_DB_RETENTION_CANDIDATE_VIRTUAL_ARCHIVE_DAYS",
        default_archive_days=30.0,
        summary_dimensions=("family", "candidate_id", "status", "exit_reason"),
        required_archive_columns=("signal_id", "candidate_id", "status", "observed_at", "payload_json"),
    ),
    RetentionPolicy(
        table="paper_missed_signal_attribution",
        ts_expr="COALESCE(created_event_ts, signal_ts, baseline_ts, 0)",
        days_env="PAPER_DB_RETENTION_MISSED_DAYS",
        default_days=7.0,
        pressure_days=3.0,
        description="missed-dog attribution and forward outcomes",
        storage_class="bounded_full_archive",
        archive_kind="full",
        archive_days_env="PAPER_DB_RETENTION_MISSED_ARCHIVE_DAYS",
        default_archive_days=30.0,
        summary_dimensions=("component", "decision", "reject_reason", "status"),
        required_archive_columns=("token_ca", "status", "payload_json"),
    ),
    RetentionPolicy(
        table="paper_trade_path_samples",
        ts_expr="sample_ts",
        days_env="PAPER_DB_RETENTION_PATH_SAMPLE_DAYS",
        default_days=7.0,
        pressure_days=3.0,
        description="per-position path samples for closed/old trades",
        extra_where=(
            "trade_id NOT IN ("
            "SELECT id FROM paper_trades WHERE exit_reason IS NULL OR exit_ts IS NULL"
            ")"
        ),
        storage_class="bounded_full_archive",
        archive_kind="full",
        archive_days_env="PAPER_DB_RETENTION_PATH_SAMPLE_ARCHIVE_DAYS",
        default_archive_days=30.0,
        summary_dimensions=("trade_id",),
        required_archive_columns=("trade_id", "sample_ts"),
    ),
    RetentionPolicy(
        table="lotto_not_ath_watch_shadow_snapshots",
        ts_expr="snapshot_ts",
        days_env="PAPER_DB_RETENTION_WATCH_SHADOW_DAYS",
        default_days=3.0,
        pressure_days=3.0,
        description="watch-shadow quote-clean snapshots",
        storage_class="summary_only",
        archive_kind="summary",
        archive_days_env="PAPER_DB_RETENTION_WATCH_SHADOW_SUMMARY_DAYS",
        default_archive_days=30.0,
        summary_dimensions=("parent_blocker", "horizon_sec", "quote_clean", "snapshot_pass", "reason"),
        required_archive_columns=("parent_blocker",),
    ),
    RetentionPolicy(
        table="external_alpha_snapshots",
        ts_expr="captured_at",
        days_env="PAPER_DB_RETENTION_EXTERNAL_ALPHA_DAYS",
        default_days=7.0,
        pressure_days=3.0,
        description="raw external alpha snapshots",
        storage_class="bounded_compact_archive",
        archive_kind="compact",
        archive_days_env="PAPER_DB_RETENTION_EXTERNAL_ALPHA_ARCHIVE_DAYS",
        default_archive_days=30.0,
        compact_columns=(
            "id",
            "captured_at",
            "token_ca",
            "symbol",
            "source",
            "category",
            "chain",
            "market_cap",
            "liquidity",
            "volume",
            "swaps",
            "buys",
            "sells",
            "price_change_5m",
            "price_change_1h",
            "created_at",
        ),
        summary_dimensions=("source", "category", "chain"),
        required_archive_columns=("captured_at", "token_ca", "source"),
    ),
    RetentionPolicy(
        table="source_resonance_candidates",
        ts_expr="signal_ts",
        days_env="PAPER_DB_RETENTION_SOURCE_RESONANCE_DAYS",
        default_days=7.0,
        pressure_days=3.0,
        description="source resonance candidate rows",
        storage_class="bounded_compact_archive",
        archive_kind="compact",
        archive_days_env="PAPER_DB_RETENTION_SOURCE_RESONANCE_ARCHIVE_DAYS",
        default_archive_days=30.0,
        compact_columns=(
            "id",
            "signal_ts",
            "token_ca",
            "symbol",
            "signal_type",
            "gmgn_pre_seen",
            "gmgn_seen_count",
            "gmgn_momentum_confirmed",
            "gmgn_volume_confirmed",
            "quote_clean_seen",
            "two_quote_clean_snapshots",
            "entry_quote_success_seen",
            "entry_quote_fail_seen",
            "source_count",
            "resonance_level",
            "resonance_score",
            "cohort",
            "updated_at",
        ),
        summary_dimensions=("cohort", "resonance_level", "gmgn_pre_seen", "quote_clean_seen"),
        required_archive_columns=("signal_ts", "token_ca", "cohort"),
    ),
    RetentionPolicy(
        table="latency_audit_events",
        ts_expr="COALESCE(event_ts, signal_ts, 0)",
        days_env="PAPER_DB_RETENTION_LATENCY_AUDIT_DAYS",
        default_days=3.0,
        pressure_days=3.0,
        description="latency audit events",
        storage_class="summary_only",
        archive_kind="summary",
        archive_days_env="PAPER_DB_RETENTION_LATENCY_SUMMARY_DAYS",
        default_archive_days=30.0,
        summary_dimensions=("source", "stage"),
        required_archive_columns=("source", "stage"),
    ),
    RetentionPolicy(
        table="paper_fast_entry_queue",
        ts_expr="created_at",
        days_env="PAPER_DB_RETENTION_FAST_QUEUE_DAYS",
        default_days=7.0,
        pressure_days=3.0,
        description="terminal fast-lane queue rows",
        extra_where="COALESCE(status, '') NOT IN ('queued', 'claimed', 'pending', 'running')",
        storage_class="bounded_compact_archive",
        archive_kind="compact",
        archive_days_env="PAPER_DB_RETENTION_FAST_QUEUE_ARCHIVE_DAYS",
        default_archive_days=30.0,
        compact_columns=(
            "id",
            "created_at",
            "updated_at",
            "token_ca",
            "symbol",
            "status",
            "source_type",
            "entry_mode_hint",
            "entry_branch",
            "hard_gate_status",
            "source_resonance_cohort",
            "priority",
            "attempt_count",
            "last_error",
            "first_error",
            "market_session",
        ),
        summary_dimensions=("status", "source_type", "entry_branch", "last_error"),
        required_archive_columns=("created_at", "token_ca", "status"),
    ),
    RetentionPolicy(
        table="opportunity_events",
        ts_expr="event_ts",
        days_env="PAPER_DB_RETENTION_OPPORTUNITY_EVENT_DAYS",
        default_days=7.0,
        pressure_days=3.0,
        description=(
            "high-volume executable opportunity evidence retained for the 168h "
            "research window"
        ),
        storage_class="bounded_full_archive",
        archive_kind="full",
        archive_days_env="PAPER_DB_RETENTION_OPPORTUNITY_EVENT_ARCHIVE_DAYS",
        default_archive_days=30.0,
        summary_dimensions=(
            "source_type",
            "source_component",
            "quote_clean",
            "would_enter_a_class",
            "did_enter",
        ),
        required_archive_columns=("opportunity_key", "event_ts", "token_ca"),
        maximum_days=7.0,
    ),
    RetentionPolicy(
        table="opportunity_event_path_samples",
        ts_expr="sample_ts",
        days_env="PAPER_DB_RETENTION_OPPORTUNITY_PATH_DAYS",
        default_days=7.0,
        pressure_days=3.0,
        description=(
            "high-volume executable opportunity path samples retained for the "
            "168h research window"
        ),
        storage_class="bounded_full_archive",
        archive_kind="full",
        archive_days_env="PAPER_DB_RETENTION_OPPORTUNITY_PATH_ARCHIVE_DAYS",
        default_archive_days=30.0,
        summary_dimensions=(
            "quote_clean",
            "quote_executable",
            "route_available",
            "no_route_flag",
            "trapped_flag",
        ),
        required_archive_columns=("opportunity_key", "sample_ts"),
        maximum_days=7.0,
    ),
]


def validate_policy_contract(policies: list[RetentionPolicy] | tuple[RetentionPolicy, ...]) -> None:
    tables = [policy.table for policy in policies]
    protected = sorted(set(tables) & PROTECTED_TABLES)
    if protected:
        raise RuntimeError(f"protected tables must never be retained automatically: {protected}")
    duplicates = sorted(table for table, count in Counter(tables).items() if count > 1)
    if duplicates:
        raise RuntimeError(f"duplicate retention policies: {duplicates}")
    invalid_kinds = sorted({policy.archive_kind for policy in policies} - {"full", "compact", "summary"})
    if invalid_kinds:
        raise RuntimeError(f"unsupported archive kinds: {invalid_kinds}")


validate_policy_contract(RETENTION_POLICIES)


def connect_db(db_path: str | os.PathLike) -> sqlite3.Connection:
    require_unmarked_paper_db(db_path, component="paper_db_retention")
    db = sqlite3.connect(str(db_path), timeout=_env_float("PAPER_DB_RETENTION_SQLITE_TIMEOUT_SEC", 30.0))
    db.execute(f"PRAGMA busy_timeout={_env_int('PAPER_DB_RETENTION_BUSY_TIMEOUT_MS', 30000)}")
    db.row_factory = sqlite3.Row
    return db


def table_exists(db: sqlite3.Connection, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return bool(row)


def table_columns(db: sqlite3.Connection, table: str) -> list[str]:
    return [str(row["name"]) for row in db.execute(f"PRAGMA table_info({table})").fetchall()]


def eligible_where(policy: RetentionPolicy) -> str:
    return f"({policy.ts_expr}) > 0 AND ({policy.ts_expr}) < ? AND ({policy.extra_where})"


def count_eligible(db: sqlite3.Connection, policy: RetentionPolicy, cutoff_ts: float) -> int:
    row = db.execute(
        f"SELECT COUNT(*) AS n FROM {policy.table} WHERE {eligible_where(policy)}",
        (cutoff_ts,),
    ).fetchone()
    return int(row["n"] or 0)


def row_to_jsonable(row: sqlite3.Row) -> dict:
    result = {}
    for key in row.keys():
        value = row[key]
        if isinstance(value, bytes):
            value = value.hex()
        result[key] = value
    return result


def archive_columns_for_policy(policy: RetentionPolicy, available_columns: list[str]) -> list[str]:
    available = set(available_columns)
    if policy.archive_kind == "full":
        return list(available_columns)
    requested = list(policy.compact_columns or policy.summary_dimensions)
    return [column for column in requested if column in available]


def summarize_dimensions(rows: list[sqlite3.Row], dimensions: tuple[str, ...]) -> dict:
    available = set(rows[0].keys()) if rows else set()
    usable = [dimension for dimension in dimensions if dimension in available]
    result = {}
    for dimension in usable:
        counts = Counter()
        for row in rows:
            value = row[dimension]
            key = "<null>" if value is None else str(value)
            counts[key] += 1
        result[dimension] = dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:100])
    return result


def verify_archive_payload(path: Path, expected_sha256: str, expected_rows: int) -> dict:
    hasher = hashlib.sha256()
    row_count = 0
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            hasher.update(line.encode("utf-8"))
            row_count += 1
    actual_sha = hasher.hexdigest()
    if actual_sha != expected_sha256:
        raise RuntimeError(
            f"archive sha256 mismatch path={path} expected={expected_sha256} actual={actual_sha}"
        )
    if row_count != expected_rows:
        raise RuntimeError(
            f"archive row count mismatch path={path} expected={expected_rows} actual={row_count}"
        )
    return {"verified": True, "row_count": row_count, "uncompressed_sha256": actual_sha}


def archive_rows(
    *,
    rows: list[sqlite3.Row],
    archive_dir: Path,
    policy: RetentionPolicy,
    cutoff_ts: float,
    run_id: str,
    batch_no: int,
    mode: str,
    selected_columns: list[str],
    now_ts: float,
) -> dict:
    archive_dir.mkdir(parents=True, exist_ok=True)
    month = time.strftime("%Y-%m", time.gmtime(cutoff_ts))
    table_dir = archive_dir / policy.table / month
    table_dir.mkdir(parents=True, exist_ok=True)
    base = f"{policy.table}_{run_id}_batch{batch_no:05d}"
    final_path = table_dir / f"{base}.jsonl.gz"
    tmp_path = table_dir / f"{base}.jsonl.gz.tmp"
    manifest_path = table_dir / f"{base}.manifest.json"
    manifest_tmp = table_dir / f"{base}.manifest.json.tmp"
    hasher = hashlib.sha256()
    min_ts = None
    max_ts = None
    min_rowid = None
    max_rowid = None
    archive_file = None
    if policy.archive_kind != "summary":
        with gzip.open(tmp_path, "wt", encoding="utf-8") as fh:
            for row in rows:
                row_dict = row_to_jsonable(row)
                row_dict = {
                    key: value
                    for key, value in row_dict.items()
                    if key in {"_retention_ts_", "_retention_rowid_"} or key in selected_columns
                }
                row_ts = row_dict.get("_retention_ts_")
                rowid = row_dict.get("_retention_rowid_")
                try:
                    min_ts = row_ts if min_ts is None else min(float(min_ts), float(row_ts))
                    max_ts = row_ts if max_ts is None else max(float(max_ts), float(row_ts))
                except (TypeError, ValueError):
                    pass
                try:
                    min_rowid = rowid if min_rowid is None else min(int(min_rowid), int(rowid))
                    max_rowid = rowid if max_rowid is None else max(int(max_rowid), int(rowid))
                except (TypeError, ValueError):
                    pass
                line = json.dumps(row_dict, ensure_ascii=False, sort_keys=True, default=str) + "\n"
                hasher.update(line.encode("utf-8"))
                fh.write(line)
        os.replace(tmp_path, final_path)
        archive_file = str(final_path)
        verification = verify_archive_payload(final_path, hasher.hexdigest(), len(rows))
    else:
        verification = {"verified": True, "row_count": len(rows), "summary_only": True}
        for row in rows:
            row_dict = row_to_jsonable(row)
            row_ts = row_dict.get("_retention_ts_")
            rowid = row_dict.get("_retention_rowid_")
            try:
                min_ts = row_ts if min_ts is None else min(float(min_ts), float(row_ts))
                max_ts = row_ts if max_ts is None else max(float(max_ts), float(row_ts))
            except (TypeError, ValueError):
                pass
            try:
                min_rowid = rowid if min_rowid is None else min(int(min_rowid), int(rowid))
                max_rowid = rowid if max_rowid is None else max(int(max_rowid), int(rowid))
            except (TypeError, ValueError):
                pass
    created_at_ts = float(now_ts)
    configured_gc_after_ts = created_at_ts + policy.archive_days() * 86400.0
    # Keep the archive until even its newest row has reached the global age cap.
    # Using the oldest row here could erase newer rows in the same batch early.
    retention_basis_ts = float(max_ts) if max_ts is not None else created_at_ts
    hard_gc_after_ts = retention_basis_ts + MAX_RESEARCH_RETENTION_DAYS * 86400.0
    gc_after_ts = min(configured_gc_after_ts, hard_gc_after_ts)
    manifest = {
        "schema_version": "paper_db_retention_archive.v2",
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(created_at_ts)),
        "created_at_ts": created_at_ts,
        "mode": mode,
        "table": policy.table,
        "storage_class": policy.storage_class,
        "archive_kind": policy.archive_kind,
        "archive_file": archive_file,
        "selected_columns": selected_columns,
        "row_count": len(rows),
        "rowid_min": min_rowid,
        "rowid_max": max_rowid,
        "retention_ts_min": min_ts,
        "retention_ts_max": max_ts,
        "cutoff_ts": cutoff_ts,
        "cutoff_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff_ts)),
        "uncompressed_sha256": hasher.hexdigest() if archive_file else None,
        "verification": verification,
        "dimension_counts": summarize_dimensions(rows, policy.summary_dimensions),
        "archive_retention_days": policy.archive_days(),
        "max_total_research_retention_days": MAX_RESEARCH_RETENTION_DAYS,
        "hard_gc_after_ts": hard_gc_after_ts,
        "gc_after_ts": gc_after_ts,
        "gc_after_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(gc_after_ts)),
        "source_delete_status": "pending" if mode == "apply" else "not_requested",
        "source_deleted_rows": 0,
    }
    manifest_tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(manifest_tmp, manifest_path)
    manifest["manifest_file"] = str(manifest_path)
    return manifest


def rewrite_manifest_atomic(manifest: dict) -> None:
    manifest_path = Path(manifest["manifest_file"])
    payload = {key: value for key, value in manifest.items() if key != "manifest_file"}
    tmp = manifest_path.with_name(manifest_path.name + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, manifest_path)


def delete_rowids(db: sqlite3.Connection, table: str, rowids: list[int]) -> int:
    if not rowids:
        return 0
    placeholders = ",".join("?" for _ in rowids)
    cur = db.execute(f"DELETE FROM {table} WHERE rowid IN ({placeholders})", tuple(rowids))
    return int(cur.rowcount or 0)


def apply_policy(
    db: sqlite3.Connection,
    policy: RetentionPolicy,
    *,
    archive_dir: Path,
    now_ts: float,
    mode: str,
    batch_size: int,
    max_rows: int,
    deadline_ts: float | None,
    run_id: str,
    pressure_level: str,
) -> dict:
    effective_days = policy.effective_days(pressure_level)
    summary = {
        "table": policy.table,
        "description": policy.description,
        "storage_class": policy.storage_class,
        "archive_kind": policy.archive_kind,
        "retention_days": policy.days(),
        "effective_retention_days": effective_days,
        "archive_retention_days": policy.archive_days(),
        "exists": table_exists(db, policy.table),
        "eligible": 0,
        "archived": 0,
        "deleted": 0,
        "batches": [],
        "stopped_reason": None,
    }
    if not summary["exists"]:
        summary["stopped_reason"] = "table_missing"
        return summary
    cutoff_ts = now_ts - effective_days * 86400.0
    summary["cutoff_ts"] = cutoff_ts
    summary["cutoff_iso"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(cutoff_ts))
    summary["eligible"] = count_eligible(db, policy, cutoff_ts)
    if mode == "report" or summary["eligible"] <= 0:
        summary["stopped_reason"] = "report_only" if mode == "report" else "no_eligible_rows"
        return summary

    columns = table_columns(db, policy.table)
    missing_required = sorted(set(policy.required_archive_columns) - set(columns))
    if missing_required:
        summary["missing_required_archive_columns"] = missing_required
        summary["stopped_reason"] = "required_archive_columns_missing"
        return summary
    selected_columns = archive_columns_for_policy(policy, columns)
    if policy.archive_kind == "compact" and not selected_columns:
        summary["stopped_reason"] = "compact_archive_columns_missing"
        return summary
    summary["selected_columns"] = selected_columns
    select_columns = ", ".join(f'"{name}"' for name in selected_columns)
    if not select_columns:
        select_columns = "NULL AS _retention_summary_placeholder_"
    last_rowid = 0
    batch_no = 0
    max_rows = max(0, int(max_rows))
    rows_remaining = min(summary["eligible"], max_rows) if max_rows else summary["eligible"]
    while rows_remaining > 0:
        if deadline_ts is not None and time.time() >= deadline_ts:
            summary["stopped_reason"] = "time_budget_exhausted"
            break
        limit = min(batch_size, rows_remaining)
        with sqlite_single_writer(
            f"paper_db_retention:{policy.table}",
            timeout_sec=_env_float("PAPER_DB_RETENTION_LOCK_TIMEOUT_SEC", 120.0),
        ):
            rows = db.execute(
                f"""
                SELECT rowid AS _retention_rowid_,
                       ({policy.ts_expr}) AS _retention_ts_,
                       {select_columns}
                FROM {policy.table}
                WHERE rowid > ? AND {eligible_where(policy)}
                ORDER BY rowid ASC
                LIMIT ?
                """,
                (last_rowid, cutoff_ts, limit),
            ).fetchall()
            if not rows:
                summary["stopped_reason"] = "scan_complete"
                break
            batch_no += 1
            last_rowid = int(rows[-1]["_retention_rowid_"])
            manifest = archive_rows(
                rows=rows,
                archive_dir=archive_dir,
                policy=policy,
                cutoff_ts=cutoff_ts,
                run_id=run_id,
                batch_no=batch_no,
                mode=mode,
                selected_columns=selected_columns,
                now_ts=now_ts,
            )
            summary["archived"] += len(rows)
            if mode == "apply":
                deleted = delete_rowids(
                    db,
                    policy.table,
                    [int(row["_retention_rowid_"]) for row in rows],
                )
                if deleted != len(rows):
                    db.rollback()
                    raise RuntimeError(
                        f"retention delete count mismatch table={policy.table} "
                        f"expected={len(rows)} actual={deleted}"
                    )
                summary["deleted"] += deleted
                manifest["deleted"] = deleted
                manifest["source_deleted_rows"] = deleted
                manifest["source_delete_status"] = "verified"
                manifest["source_delete_verified_at"] = time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ",
                    time.gmtime(),
                )
                rewrite_manifest_atomic(manifest)
                db.commit()
            else:
                manifest["deleted"] = 0
        summary["batches"].append(
            {
                "manifest_file": manifest.get("manifest_file"),
                "archive_file": manifest.get("archive_file"),
                "row_count": manifest.get("row_count"),
                "deleted": manifest.get("deleted"),
                "archive_kind": manifest.get("archive_kind"),
                "retention_ts_min": manifest.get("retention_ts_min"),
                "retention_ts_max": manifest.get("retention_ts_max"),
                "verified": bool((manifest.get("verification") or {}).get("verified")),
                "gc_after_ts": manifest.get("gc_after_ts"),
            }
        )
        rows_remaining -= len(rows)
    if summary["stopped_reason"] is None:
        summary["stopped_reason"] = "row_budget_exhausted" if max_rows and summary["archived"] >= max_rows else "complete"
    return summary


def storage_health(path: Path, override: dict | None = None) -> dict:
    if override is None:
        usage = shutil.disk_usage(path)
        total_bytes = int(usage.total)
        used_bytes = int(usage.used)
        free_bytes = int(usage.free)
    else:
        total_bytes = int(override["total_bytes"])
        used_bytes = int(override["used_bytes"])
        free_bytes = int(override["free_bytes"])
    used_pct = (used_bytes / total_bytes * 100.0) if total_bytes > 0 else 0.0
    soft_pct = _env_float("PAPER_DB_RETENTION_DISK_SOFT_PCT", 70.0)
    hard_pct = _env_float("PAPER_DB_RETENTION_DISK_HARD_PCT", 82.0)
    critical_pct = _env_float("PAPER_DB_RETENTION_DISK_CRITICAL_PCT", 90.0)
    reserve_gib = _env_float("PAPER_DB_RETENTION_MIN_FREE_GIB", 8.0)
    reserve_bytes = int(reserve_gib * 1024**3)
    if used_pct >= critical_pct or free_bytes <= reserve_bytes:
        level = "critical"
    elif used_pct >= hard_pct:
        level = "hard"
    elif used_pct >= soft_pct:
        level = "soft"
    else:
        level = "normal"
    return {
        "total_bytes": total_bytes,
        "used_bytes": used_bytes,
        "free_bytes": free_bytes,
        "used_pct": round(used_pct, 3),
        "pressure_level": level,
        "soft_pct": soft_pct,
        "hard_pct": hard_pct,
        "critical_pct": critical_pct,
        "min_free_gib": reserve_gib,
    }


def sqlite_storage_stats(db: sqlite3.Connection, db_path: Path) -> dict:
    page_size = int(db.execute("PRAGMA page_size").fetchone()[0] or 0)
    page_count = int(db.execute("PRAGMA page_count").fetchone()[0] or 0)
    freelist_count = int(db.execute("PRAGMA freelist_count").fetchone()[0] or 0)
    return {
        "db_file_bytes": db_path.stat().st_size if db_path.exists() else 0,
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "freelist_bytes": freelist_count * page_size,
        "reusable_page_pct": round(freelist_count / page_count * 100.0, 3) if page_count else 0.0,
    }


def path_is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def garbage_collect_archives(
    *,
    archive_dir: Path,
    now_ts: float,
    max_manifests: int,
    verify_payloads: bool,
    pressure_level: str = "normal",
    critical_max_age_days: float = 14.0,
    max_total_retention_days: float = MAX_RESEARCH_RETENTION_DAYS,
) -> dict:
    summary = {
        "enabled": True,
        "seen": 0,
        "scanned": 0,
        "eligible": 0,
        "hard_retention_cap_eligible": 0,
        "critical_pressure_eligible": 0,
        "deleted_manifests": 0,
        "deleted_archives": 0,
        "freed_bytes": 0,
        "refused_count": 0,
        "refused": [],
    }
    def refuse(manifest_path: Path, reason: str) -> None:
        summary["refused_count"] += 1
        if len(summary["refused"]) < 100:
            summary["refused"].append({"manifest": str(manifest_path), "reason": reason})

    if not archive_dir.exists():
        summary["stopped_reason"] = "archive_dir_missing"
        return summary
    manifests = sorted(archive_dir.rglob("*.manifest.json"))
    eligible_manifests = []
    for manifest_path in manifests:
        summary["seen"] += 1
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception as exc:
            refuse(manifest_path, f"invalid_manifest:{exc}")
            continue
        if manifest.get("schema_version") != "paper_db_retention_archive.v2":
            refuse(manifest_path, "unmanaged_schema")
            continue
        try:
            gc_after_ts = float(manifest.get("gc_after_ts"))
        except (TypeError, ValueError):
            refuse(manifest_path, "missing_gc_after_ts")
            continue
        try:
            created_at_ts = float(manifest.get("created_at_ts"))
        except (TypeError, ValueError):
            created_at_ts = 0.0
        try:
            retention_ts_max = float(manifest.get("retention_ts_max"))
        except (TypeError, ValueError):
            retention_ts_max = 0.0
        retention_basis_ts = retention_ts_max if retention_ts_max > 0 else created_at_ts
        hard_gc_after_ts = (
            retention_basis_ts + max(0.0, float(max_total_retention_days)) * 86400.0
            if retention_basis_ts > 0
            else None
        )
        declared_expired = gc_after_ts <= now_ts
        hard_retention_cap_expired = bool(
            hard_gc_after_ts is not None and hard_gc_after_ts <= now_ts
        )
        critical_expired = bool(
            pressure_level == "critical"
            and manifest.get("archive_kind") != "summary"
            and created_at_ts > 0
            and created_at_ts <= now_ts - max(0.0, critical_max_age_days) * 86400.0
        )
        if not declared_expired and not critical_expired and not hard_retention_cap_expired:
            continue
        summary["eligible"] += 1
        if hard_retention_cap_expired and not declared_expired:
            summary["hard_retention_cap_eligible"] += 1
        if critical_expired and not declared_expired:
            summary["critical_pressure_eligible"] += 1
        eligible_candidates = [gc_after_ts]
        if critical_expired:
            eligible_candidates.append(
                created_at_ts + max(0.0, critical_max_age_days) * 86400.0
            )
        if hard_retention_cap_expired and hard_gc_after_ts is not None:
            eligible_candidates.append(hard_gc_after_ts)
        eligible_since_ts = min(eligible_candidates)
        eligible_manifests.append(
            (eligible_since_ts, str(manifest_path), manifest_path, manifest)
        )

    # The budget limits successful removals, not discovery. Otherwise a table
    # with many unexpired manifests sorted first can starve expired archives
    # belonging to every later table forever.
    delete_budget = max(0, max_manifests)
    for _eligible_since, _path_key, manifest_path, manifest in sorted(eligible_manifests):
        if summary["deleted_manifests"] >= delete_budget:
            summary["stopped_reason"] = "eligible_delete_budget_exhausted"
            break
        summary["scanned"] += 1
        if manifest.get("mode") == "apply" and manifest.get("source_delete_status") != "verified":
            refuse(manifest_path, "source_delete_not_verified")
            continue
        archive_file = manifest.get("archive_file")
        archive_path = Path(archive_file) if archive_file else None
        if archive_path is not None:
            if not path_is_within(archive_path, archive_dir):
                refuse(manifest_path, "archive_outside_root")
                continue
            if not archive_path.exists():
                refuse(manifest_path, "archive_missing")
                continue
            if verify_payloads:
                try:
                    verify_archive_payload(
                        archive_path,
                        str(manifest.get("uncompressed_sha256") or ""),
                        int(manifest.get("row_count") or 0),
                    )
                except Exception as exc:
                    refuse(manifest_path, f"archive_verify_failed:{exc}")
                    continue
        tombstone = manifest_path.with_name(manifest_path.name + ".deleting")
        try:
            os.replace(manifest_path, tombstone)
            if archive_path is not None:
                archive_bytes = archive_path.stat().st_size
                archive_path.unlink()
                summary["freed_bytes"] += archive_bytes
                summary["deleted_archives"] += 1
            tombstone.unlink()
            summary["deleted_manifests"] += 1
        except Exception as exc:
            refuse(manifest_path, f"delete_failed:{exc}")
            if tombstone.exists() and not manifest_path.exists() and (archive_path is None or archive_path.exists()):
                try:
                    os.replace(tombstone, manifest_path)
                except OSError:
                    pass
    if "stopped_reason" not in summary:
        summary["stopped_reason"] = "complete"
    return summary


def update_bounded_growth_history(
    path: str | os.PathLike,
    summary: dict,
    *,
    max_entries: int,
) -> dict:
    history_path = Path(path)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    previous_entries = []
    if history_path.exists():
        try:
            previous_entries = [
                json.loads(line)
                for line in history_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        except Exception:
            previous_entries = []
    now_ts = float(summary.get("finished_at_ts") or time.time())
    current = {
        "ts": now_ts,
        "run_id": summary.get("run_id"),
        "db_file_bytes": int((summary.get("sqlite_after") or {}).get("db_file_bytes") or 0),
        "freelist_bytes": int((summary.get("sqlite_after") or {}).get("freelist_bytes") or 0),
        "volume_used_bytes": int((summary.get("storage") or {}).get("used_bytes") or 0),
        "volume_free_bytes": int((summary.get("storage") or {}).get("free_bytes") or 0),
        "volume_total_bytes": int((summary.get("storage") or {}).get("total_bytes") or 0),
        "pressure_level": (summary.get("storage") or {}).get("pressure_level"),
        "deleted_rows": int(summary.get("total_deleted") or 0),
        "archive_gc_freed_bytes": int((summary.get("archive_gc") or {}).get("freed_bytes") or 0),
    }
    projection = {
        "history_path": str(history_path),
        "history_max_entries": max(1, int(max_entries)),
        "previous_point_available": False,
        "db_growth_bytes_per_day": None,
        "volume_growth_bytes_per_day": None,
        "estimated_days_to_hard_watermark": None,
    }
    if previous_entries:
        previous = previous_entries[-1]
        elapsed = now_ts - float(previous.get("ts") or 0)
        if elapsed > 0:
            scale = 86400.0 / elapsed
            db_growth = (current["db_file_bytes"] - int(previous.get("db_file_bytes") or 0)) * scale
            volume_growth = (current["volume_used_bytes"] - int(previous.get("volume_used_bytes") or 0)) * scale
            projection["previous_point_available"] = True
            projection["elapsed_sec"] = elapsed
            projection["db_growth_bytes_per_day"] = round(db_growth, 3)
            projection["volume_growth_bytes_per_day"] = round(volume_growth, 3)
            hard_pct = float((summary.get("storage") or {}).get("hard_pct") or 82.0)
            hard_bytes = current["volume_total_bytes"] * hard_pct / 100.0
            if volume_growth > 0 and hard_bytes > current["volume_used_bytes"]:
                projection["estimated_days_to_hard_watermark"] = round(
                    (hard_bytes - current["volume_used_bytes"]) / volume_growth,
                    3,
                )
    entries = (previous_entries + [current])[-max(1, int(max_entries)) :]
    tmp = history_path.with_name(history_path.name + f".tmp.{os.getpid()}")
    tmp.write_text(
        "".join(json.dumps(entry, sort_keys=True) + "\n" for entry in entries),
        encoding="utf-8",
    )
    os.replace(tmp, history_path)
    projection["history_entries"] = len(entries)
    return projection


def retention_scheduler_state_path(archive_dir: Path) -> Path:
    return archive_dir / RETENTION_SCHEDULER_STATE_NAME


def load_retention_scheduler_state(archive_dir: Path) -> dict:
    tables = [policy.table for policy in RETENTION_POLICIES]
    initial_table = (
        RETENTION_SCHEDULER_INITIAL_TABLE
        if RETENTION_SCHEDULER_INITIAL_TABLE in tables
        else tables[0]
    )
    state_path = retention_scheduler_state_path(archive_dir)
    result = {
        "schema_version": RETENTION_SCHEDULER_SCHEMA_VERSION,
        "state_path": str(state_path),
        "next_table": initial_table,
        "loaded": False,
        "load_error": None,
    }
    if not state_path.exists():
        return result
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        if payload.get("schema_version") != RETENTION_SCHEDULER_SCHEMA_VERSION:
            raise ValueError("scheduler schema mismatch")
        next_table = str(payload.get("next_table") or "")
        if next_table not in tables:
            raise ValueError("scheduler next_table is not a retention policy")
        result["next_table"] = next_table
        result["loaded"] = True
    except Exception as exc:
        result["load_error"] = f"{type(exc).__name__}:{exc}"
    return result


def rotated_retention_policies(next_table: str) -> list[RetentionPolicy]:
    policies = list(RETENTION_POLICIES)
    start = next(
        (index for index, policy in enumerate(policies) if policy.table == next_table),
        0,
    )
    return policies[start:] + policies[:start]


def write_retention_scheduler_state(
    archive_dir: Path,
    *,
    next_table: str,
    run_id: str,
    now_ts: float,
) -> Path:
    if next_table not in {policy.table for policy in RETENTION_POLICIES}:
        raise ValueError(f"invalid retention scheduler table: {next_table}")
    archive_dir.mkdir(parents=True, exist_ok=True)
    state_path = retention_scheduler_state_path(archive_dir)
    temporary = state_path.with_name(
        state_path.name + f".tmp.{os.getpid()}.{uuid.uuid4().hex[:8]}"
    )
    payload = {
        "schema_version": RETENTION_SCHEDULER_SCHEMA_VERSION,
        "next_table": next_table,
        "updated_at_ts": float(now_ts),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts)),
        "run_id": run_id,
    }
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, state_path)
    return state_path


def run_retention(
    *,
    db_path: str | os.PathLike,
    archive_dir: str | os.PathLike,
    mode: str = "report",
    batch_size: int = 5000,
    max_rows_per_table: int = 50000,
    max_rows_total: int = 200000,
    max_seconds: float = 60.0,
    vacuum: bool = False,
    now_ts: float | None = None,
    storage_usage_override: dict | None = None,
) -> dict:
    if mode not in {"report", "archive", "apply"}:
        raise ValueError(f"unsupported mode: {mode}")
    now_ts = float(now_ts if now_ts is not None else time.time())
    deadline_ts = None if max_seconds <= 0 else time.time() + max_seconds
    archive_path = Path(archive_dir)
    db_path = Path(db_path)
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(now_ts)) + "_" + uuid.uuid4().hex[:8]
    storage_root = db_path.parent if db_path.parent.exists() else Path.cwd()
    storage = storage_health(storage_root, storage_usage_override)
    scheduler = (
        load_retention_scheduler_state(archive_path)
        if mode == "apply"
        else {
            "schema_version": RETENTION_SCHEDULER_SCHEMA_VERSION,
            "enabled": False,
            "next_table": None,
        }
    )
    scheduler["enabled"] = mode == "apply"
    scheduler["start_table"] = scheduler.get("next_table")
    scheduler["advance_count"] = 0
    scheduler["last_advanced_from"] = None
    policy_sequence = (
        rotated_retention_policies(str(scheduler["next_table"]))
        if mode == "apply"
        else list(RETENTION_POLICIES)
    )
    summary = {
        "schema_version": "paper_db_retention.v2",
        "run_id": run_id,
        "mode": mode,
        "db_path": str(db_path),
        "archive_dir": str(archive_path),
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "started_at_ts": time.time(),
        "storage": storage,
        "storage_before": storage,
        "protected_tables": sorted(PROTECTED_TABLES),
        "protected_tables_selected": [],
        "max_total_research_retention_days": MAX_RESEARCH_RETENTION_DAYS,
        "scheduler": scheduler,
        "policies": [],
        "total_eligible": 0,
        "total_archived": 0,
        "total_deleted": 0,
        "vacuum": {"requested": bool(vacuum), "ran": False},
    }
    if not db_path.exists():
        summary["status"] = "db_missing"
        return summary
    db = connect_db(db_path)
    try:
        summary["sqlite_before"] = sqlite_storage_stats(db, db_path)
        if mode == "apply":
            db.execute("PRAGMA foreign_keys=OFF")
        total_budget_left = max_rows_total
        for policy_index, policy in enumerate(policy_sequence):
            if deadline_ts is not None and time.time() >= deadline_ts:
                summary["stopped_reason"] = "time_budget_exhausted"
                break
            if max_rows_total and total_budget_left <= 0:
                summary["stopped_reason"] = "total_row_budget_exhausted"
                break
            if mode == "apply":
                next_policy = policy_sequence[(policy_index + 1) % len(policy_sequence)]
                # Advance before table work. If the outer timeout kills this run
                # mid-policy, the next run still moves on instead of starving
                # every later table forever.
                with sqlite_single_writer(
                    "paper_db_retention:scheduler",
                    timeout_sec=_env_float("PAPER_DB_RETENTION_LOCK_TIMEOUT_SEC", 120.0),
                ):
                    state_path = write_retention_scheduler_state(
                        archive_path,
                        next_table=next_policy.table,
                        run_id=run_id,
                        now_ts=now_ts,
                    )
                scheduler["next_table"] = next_policy.table
                scheduler["state_path"] = str(state_path)
                scheduler["advance_count"] += 1
                scheduler["last_advanced_from"] = policy.table
            policy_budget = max_rows_per_table
            if max_rows_total:
                policy_budget = min(policy_budget, total_budget_left)
            policy_summary = apply_policy(
                db,
                policy,
                archive_dir=archive_path,
                now_ts=now_ts,
                mode=mode,
                batch_size=max(1, batch_size),
                max_rows=max(0, policy_budget),
                deadline_ts=deadline_ts,
                run_id=run_id,
                pressure_level=str(storage["pressure_level"]),
            )
            summary["policies"].append(policy_summary)
            summary["total_eligible"] += int(policy_summary.get("eligible") or 0)
            summary["total_archived"] += int(policy_summary.get("archived") or 0)
            summary["total_deleted"] += int(policy_summary.get("deleted") or 0)
            if max_rows_total:
                total_budget_left -= int(policy_summary.get("archived") or 0)
        archive_gc_enabled = _env_bool("PAPER_DB_RETENTION_ARCHIVE_GC_ENABLED", "true")
        if mode == "apply" and archive_gc_enabled:
            summary["archive_gc"] = garbage_collect_archives(
                archive_dir=archive_path,
                now_ts=now_ts,
                max_manifests=_env_int("PAPER_DB_RETENTION_ARCHIVE_GC_MAX_MANIFESTS", 10),
                verify_payloads=_env_bool("PAPER_DB_RETENTION_ARCHIVE_GC_VERIFY", "true"),
                pressure_level=str(storage["pressure_level"]),
                critical_max_age_days=_env_float(
                    "PAPER_DB_RETENTION_CRITICAL_ARCHIVE_MAX_AGE_DAYS",
                    14.0,
                ),
            )
        else:
            summary["archive_gc"] = {
                "enabled": archive_gc_enabled,
                "stopped_reason": "not_apply_mode" if mode != "apply" else "disabled",
            }
        try:
            checkpoint = db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            summary["wal_checkpoint"] = list(checkpoint) if checkpoint is not None else None
        except Exception as exc:
            summary["wal_checkpoint_error"] = str(exc)
        if vacuum and mode == "apply" and summary["total_deleted"] > 0:
            try:
                with sqlite_single_writer(
                    "paper_db_retention:vacuum",
                    timeout_sec=_env_float("PAPER_DB_RETENTION_LOCK_TIMEOUT_SEC", 120.0),
                ):
                    db.execute("VACUUM")
                summary["vacuum"]["ran"] = True
                summary["vacuum"]["single_writer_coordinated"] = True
            except Exception as exc:
                summary["vacuum"]["error"] = str(exc)
        summary["sqlite_after"] = sqlite_storage_stats(db, db_path)
        summary["storage_after"] = storage_health(storage_root, storage_usage_override)
        summary["storage"] = summary["storage_after"]
        summary["bounded_growth_contract"] = {
            "hot_window_floor_hours": 72,
            "max_total_research_retention_days": MAX_RESEARCH_RETENTION_DAYS,
            "archives_expire": True,
            "unknown_archives_auto_deleted": False,
            "protected_trade_evidence_pruned": False,
            "protected_trade_evidence_retention": "permanent",
            "promotion_allowed": False,
        }
        summary["status"] = "ok"
        summary["finished_at_ts"] = time.time()
        summary["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return summary
    finally:
        db.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive and prune hot paper DB audit rows.")
    parser.add_argument("--db", default=os.environ.get("PAPER_DB") or str(DEFAULT_DB))
    parser.add_argument("--archive-dir", default=os.environ.get("PAPER_DB_RETENTION_ARCHIVE_DIR") or str(DEFAULT_ARCHIVE_DIR))
    parser.add_argument("--mode", choices=["report", "archive", "apply"], default=os.environ.get("PAPER_DB_RETENTION_MODE", "report"))
    parser.add_argument("--batch-size", type=int, default=_env_int("PAPER_DB_RETENTION_BATCH_ROWS", 5000))
    parser.add_argument("--max-rows-per-table", type=int, default=_env_int("PAPER_DB_RETENTION_MAX_ROWS_PER_TABLE", 50000))
    parser.add_argument("--max-rows-total", type=int, default=_env_int("PAPER_DB_RETENTION_MAX_ROWS_TOTAL", 200000))
    parser.add_argument("--max-seconds", type=float, default=_env_float("PAPER_DB_RETENTION_MAX_SECONDS", 60.0))
    parser.add_argument("--vacuum", action="store_true", default=_env_bool("PAPER_DB_RETENTION_VACUUM", "false"))
    parser.add_argument("--out", default=os.environ.get("PAPER_DB_RETENTION_STATUS_PATH"))
    parser.add_argument("--history", default=os.environ.get("PAPER_DB_RETENTION_HISTORY_PATH"))
    parser.add_argument(
        "--history-max-entries",
        type=int,
        default=_env_int("PAPER_DB_RETENTION_HISTORY_MAX_ENTRIES", 720),
    )
    return parser.parse_args()


def write_json_atomic(path: str | os.PathLike, payload: dict) -> None:
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_name(output.name + f".tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, output)


def main() -> int:
    args = parse_args()
    summary = run_retention(
        db_path=args.db,
        archive_dir=args.archive_dir,
        mode=args.mode,
        batch_size=args.batch_size,
        max_rows_per_table=args.max_rows_per_table,
        max_rows_total=args.max_rows_total,
        max_seconds=args.max_seconds,
        vacuum=args.vacuum,
    )
    if args.history:
        summary["growth_projection"] = update_bounded_growth_history(
            args.history,
            summary,
            max_entries=args.history_max_entries,
        )
    if args.out:
        write_json_atomic(args.out, summary)
    print(json.dumps(summary, indent=2, sort_keys=True), flush=True)
    return 0 if summary.get("status") in {"ok", "db_missing"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
