#!/usr/bin/env python3
"""Fail-closed evaluator database source contract."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import time
from urllib.parse import quote

from cross_db_evaluator_snapshot import (
    CANDIDATE_STAGE_BUDGET_MODE,
    CANDIDATE_STAGE_RESIDUAL_SHARE,
    DATABASE_SPECS,
    MIN_CANDIDATE_STAGE_CAP_BYTES,
    MIN_PARALLEL_PAPER_STAGE_CAP_BYTES,
    PARALLEL_PAPER_OPTIONAL_STAGE_TABLES,
    PARALLEL_PAPER_REQUIRED_STAGE_TABLES,
    PARALLEL_PAPER_STAGE_CONFIGS,
    PARALLEL_PAPER_STAGE_SCHEMA_VERSION,
    PARALLEL_PAPER_STAGE_STORAGE_MODE,
    PARALLEL_PAPER_STAGE_TABLES,
    PAPER_DECISION_STAGE_TABLE,
    parallel_paper_stage_inventory_valid,
    normalized_timestamp_sql,
    quote_identifier,
)


SCHEMA_VERSION = "evaluator_db_source_contract.v1"
SNAPSHOT_SCHEMA_VERSION = "cross_db_evaluator_snapshot.v3"
SELECTION_SCHEMA_VERSION = "evaluator_snapshot_selection.v1"
PROVENANCE_SCHEMA_VERSION = "evaluator_snapshot_provenance.v1"
PRODUCER_STATUS_SCHEMA_VERSION = "cross_db_evaluator_snapshot_worker_status.v1"
SNAPSHOT_FILES = {
    "signal": "signal.db",
    "paper": "paper_evidence.db",
    "raw": "raw.db",
    "kline": "kline.db",
}


def file_identity(path: Path) -> tuple[int, int] | None:
    details = path.stat()
    if not stat.S_ISREG(details.st_mode):
        return None
    return int(details.st_dev), int(details.st_ino)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def valid_sha256_hex(value: object) -> bool:
    text = str(value or "").lower()
    return len(text) == 64 and all(
        character in "0123456789abcdef" for character in text
    )


def sqlite_table_schema_evidence(
    path: Path,
    tables: tuple[str, ...] | list[str],
) -> dict[str, dict]:
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    evidence: dict[str, dict] = {}
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        for table in tables:
            row = connection.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            if row is None or not row["sql"]:
                continue
            columns = list(
                connection.execute(
                    f"PRAGMA table_xinfo({quote_identifier(table)})"
                )
            )
            visible_columns = [
                {
                    "name": str(column["name"]),
                    "declared_type": str(column["type"] or ""),
                }
                for column in columns
                if int(column["hidden"] or 0) == 0
            ]
            canonical_columns = json.dumps(
                visible_columns,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            evidence[str(table)] = {
                "create_sql_sha256": sha256_text(str(row["sql"])),
                "column_contract_sha256": sha256_text(canonical_columns),
                "column_count": len(visible_columns),
                "hidden_column_count": len(columns) - len(visible_columns),
                "columns": visible_columns,
            }
    finally:
        connection.close()
    return evidence


def sqlite_quick_check(path: Path) -> list[str]:
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return [str(row[0]) for row in connection.execute("PRAGMA quick_check").fetchall()]
    finally:
        connection.close()


def sqlite_temporal_bounds(
    path: Path,
    spec: dict,
    *,
    upper_epoch: float,
) -> dict[str, dict]:
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    reports: dict[str, dict] = {}
    try:
        connection.execute("PRAGMA query_only=ON")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table','view')"
            )
        }
        for table, rule in spec["tables"].items():
            if table not in tables or rule["mode"] == "full":
                continue
            columns = {
                str(row["name"])
                for row in connection.execute(
                    f"PRAGMA table_info({quote_identifier(table)})"
                )
            }
            time_columns = [
                name for name in rule.get("time_columns", ()) if name in columns
            ]
            if not time_columns:
                reports[table] = {
                    "time_columns": [],
                    "missing_time_columns": True,
                    "upper_bound_passed": False,
                }
                continue
            expressions = []
            for index, column in enumerate(time_columns):
                normalized = normalized_timestamp_sql(column)
                expressions.append(
                    f"MAX({normalized}) AS max_{index}"
                )
                expressions.append(
                    "SUM(CASE WHEN "
                    f"{quote_identifier(column)} IS NOT NULL AND {normalized} IS NULL "
                    f"THEN 1 ELSE 0 END) AS invalid_{index}"
                )
            row = connection.execute(
                f"SELECT {', '.join(expressions)} FROM {quote_identifier(table)}"
            ).fetchone()
            maxima = {
                column: row[f"max_{index}"]
                for index, column in enumerate(time_columns)
            }
            invalid_counts = {
                column: int(row[f"invalid_{index}"] or 0)
                for index, column in enumerate(time_columns)
            }
            reports[table] = {
                "time_columns": time_columns,
                "maxima": maxima,
                "invalid_timestamp_counts": invalid_counts,
                "missing_time_columns": False,
                "upper_bound_passed": all(
                    value is None or float(value) <= float(upper_epoch) + 0.001
                    for value in maxima.values()
                ),
                "timestamps_parseable": all(value == 0 for value in invalid_counts.values()),
            }
    finally:
        connection.close()
    return reports


def evaluator_db_source_status(
    paper_db: str,
    data_dir: str,
) -> dict:
    candidate = Path(paper_db).expanduser().resolve()
    live = (Path(data_dir).expanduser().resolve() / "paper_trades.db").resolve()
    blockers = []
    try:
        candidate_identity = file_identity(candidate)
    except OSError:
        candidate_identity = None
    try:
        live_identity = file_identity(live)
    except OSError:
        live_identity = None
    exists = candidate_identity is not None
    is_live = bool(
        candidate == live
        or (
            candidate_identity is not None
            and live_identity is not None
            and candidate_identity == live_identity
        )
    )
    if not exists:
        blockers.append("evaluator_db_missing")
    if is_live:
        blockers.append("active_paper_db_forbidden_for_evaluator")
    return {
        "schema_version": SCHEMA_VERSION,
        "paper_db": str(candidate),
        "live_paper_db": str(live),
        "exists": exists,
        "is_live_paper_db": is_live,
        "accepted": not blockers,
        "blockers": blockers,
        "promotion_allowed": False,
    }


def require_evaluator_db_source(
    paper_db: str,
    data_dir: str,
) -> dict:
    status = evaluator_db_source_status(paper_db, data_dir)
    if not status["accepted"]:
        raise RuntimeError(
            "evaluator_db_source_blocked "
            + ",".join(status["blockers"])
            + f" paper_db={status['paper_db']}"
        )
    return status


def producer_status_path_for_manifest(
    manifest_file: Path,
    explicit_path: str | None = None,
) -> Path:
    configured = explicit_path or os.environ.get("EVALUATOR_SNAPSHOT_STATUS")
    if configured:
        return Path(configured).expanduser().resolve(strict=False)
    snapshot_dir = manifest_file.parent
    if snapshot_dir.parent.name == "snapshots":
        evidence_root = snapshot_dir.parent.parent
    elif snapshot_dir.name == "current":
        evidence_root = snapshot_dir.parent
    else:
        evidence_root = snapshot_dir.parent
    return (evidence_root / "snapshot_status.json").resolve(strict=False)


def evaluator_snapshot_bundle_status(
    *,
    signal_db: str,
    paper_db: str,
    raw_db: str,
    kline_db: str,
    data_dir: str,
    manifest_path: str | None = None,
    producer_status_path: str | None = None,
    max_age_sec: float = 28800,
    now_ts: float | None = None,
    live_databases: dict[str, str] | None = None,
) -> dict:
    candidates = {
        "signal": Path(signal_db).expanduser().resolve(),
        "paper": Path(paper_db).expanduser().resolve(),
        "raw": Path(raw_db).expanduser().resolve(),
        "kline": Path(kline_db).expanduser().resolve(),
    }
    data_root = Path(data_dir).expanduser().resolve()
    default_live = {
        "signal": data_root / "sentiment_arb.db",
        "paper": data_root / "paper_trades.db",
        "raw": data_root / "raw_signal_outcomes.db",
        "kline": data_root / "kline_cache.db",
    }
    live = {
        name: Path((live_databases or {}).get(name, default_path)).expanduser().resolve()
        for name, default_path in default_live.items()
    }
    manifest_file = (
        Path(manifest_path).expanduser().resolve()
        if manifest_path
        else (Path(paper_db).expanduser().parent / "manifest.json").resolve()
    )
    producer_status_file = producer_status_path_for_manifest(
        manifest_file,
        producer_status_path,
    )
    blockers: list[str] = []
    candidate_identities: dict[str, tuple[int, int]] = {}
    live_identities: dict[str, tuple[int, int]] = {}
    for name, candidate in candidates.items():
        try:
            identity = file_identity(candidate)
        except OSError:
            identity = None
            blockers.append(f"evaluator_snapshot_{name}_identity_unavailable")
        if identity is None:
            blockers.append(f"evaluator_snapshot_{name}_db_missing")
        else:
            candidate_identities[name] = identity
    for name, active in live.items():
        try:
            identity = file_identity(active)
        except OSError:
            identity = None
            if active.exists():
                blockers.append(f"active_{name}_db_identity_unavailable")
        if identity is not None:
            live_identities[name] = identity
    for candidate_name, candidate in candidates.items():
        for live_name, active in live.items():
            same_path = candidate == active
            same_identity = (
                candidate_identities.get(candidate_name) is not None
                and candidate_identities.get(candidate_name) == live_identities.get(live_name)
            )
            if same_path or same_identity:
                blockers.append(
                    f"active_{live_name}_db_forbidden_for_{candidate_name}_evaluator"
                )
                if candidate_name == live_name:
                    blockers.append(f"active_{candidate_name}_db_forbidden_for_evaluator")
    manifest: dict = {}
    manifest_loaded = False
    manifest_sha256_value: str | None = None
    producer_status: dict = {}
    producer_status_loaded = False
    producer_acceptance: dict = {}
    snapshot_age_sec_value: float | None = None
    snapshot_upper_epoch: float | None = None
    manifest_parallel_stage_tables: tuple[str, ...] = ()
    safe_manifest_parallel_stage_tables: tuple[str, ...] = ()
    verified_integrity: dict[str, dict] = {}
    if not manifest_file.is_file():
        blockers.append("evaluator_snapshot_manifest_missing")
    else:
        try:
            manifest_sha256_value = sha256_file(manifest_file)
            parsed_manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
            if not isinstance(parsed_manifest, dict) or not parsed_manifest:
                blockers.append("evaluator_snapshot_manifest_invalid_structure")
            else:
                manifest = parsed_manifest
                manifest_loaded = True
        except Exception:
            blockers.append("evaluator_snapshot_manifest_invalid_json")
    if not producer_status_file.is_file():
        blockers.append("evaluator_snapshot_producer_status_missing")
    else:
        try:
            parsed_status = json.loads(producer_status_file.read_text(encoding="utf-8"))
            if not isinstance(parsed_status, dict) or not parsed_status:
                blockers.append("evaluator_snapshot_producer_status_invalid_structure")
            else:
                producer_status = parsed_status
                producer_status_loaded = True
                producer_acceptance = (
                    producer_status.get("last_accepted_snapshot")
                    if isinstance(producer_status.get("last_accepted_snapshot"), dict)
                    else {}
                )
        except Exception:
            blockers.append("evaluator_snapshot_producer_status_invalid_json")
    if producer_status_loaded:
        if producer_status.get("schema_version") != PRODUCER_STATUS_SCHEMA_VERSION:
            blockers.append("evaluator_snapshot_producer_status_schema_invalid")
        if producer_status.get("promotion_allowed") is not False:
            blockers.append("evaluator_snapshot_producer_promotion_boundary_invalid")
        if not producer_acceptance:
            blockers.append("evaluator_snapshot_producer_acceptance_missing")
    if manifest_loaded and producer_status_loaded and producer_acceptance:
        if producer_acceptance.get("snapshot_id") != manifest.get("snapshot_id"):
            blockers.append("evaluator_snapshot_producer_snapshot_id_mismatch")
        if producer_acceptance.get("manifest_sha256") != manifest_sha256_value:
            blockers.append("evaluator_snapshot_producer_manifest_sha256_mismatch")
        producer_manifest_path = producer_acceptance.get("manifest_path")
        try:
            producer_manifest_file = Path(str(producer_manifest_path)).expanduser().resolve()
        except Exception:
            producer_manifest_file = None
        if producer_manifest_file != manifest_file:
            blockers.append("evaluator_snapshot_producer_manifest_path_mismatch")
    if manifest_loaded:
        if manifest.get("schema_version") != SNAPSHOT_SCHEMA_VERSION:
            blockers.append("evaluator_snapshot_schema_version_invalid")
        if manifest.get("accepted") is not True:
            blockers.append("evaluator_snapshot_not_accepted")
        if manifest.get("quick_checks_passed") is not True:
            blockers.append("evaluator_snapshot_quick_check_not_passed")
        if manifest.get("required_tables_present") is not True:
            blockers.append("evaluator_snapshot_required_tables_missing")
        if manifest.get("required_watermarks_present") is not True:
            blockers.append("evaluator_snapshot_required_watermarks_missing")
        if manifest.get("cross_database_time_skew_passed") is not True:
            blockers.append("evaluator_snapshot_cross_database_time_skew_failed")
        if manifest.get("read_views_pinned_before_copy") is not True:
            blockers.append("evaluator_snapshot_read_views_not_pinned")
        if manifest.get("source_read_lock_budget_passed") is not True:
            blockers.append("evaluator_snapshot_source_read_lock_budget_failed")
        if manifest.get("indexes_built_after_source_read_lock_release") is not True:
            blockers.append("evaluator_snapshot_index_build_lock_order_invalid")
        if manifest.get("candidate_projection_after_source_read_lock_release") is not True:
            blockers.append("evaluator_snapshot_candidate_projection_lock_order_invalid")
        if manifest.get("candidate_stage_removed_before_publish") is not True:
            blockers.append("evaluator_snapshot_candidate_stage_cleanup_invalid")
        raw_parallel_stage_tables = manifest.get("parallel_paper_stage_tables")
        if isinstance(raw_parallel_stage_tables, list):
            manifest_parallel_stage_tables = tuple(
                str(table) for table in raw_parallel_stage_tables
            )
            safe_manifest_parallel_stage_tables = tuple(
                table
                for table in manifest_parallel_stage_tables
                if table in PARALLEL_PAPER_STAGE_CONFIGS
            )
        if (
            manifest.get("parallel_paper_stage_schema_version")
            != PARALLEL_PAPER_STAGE_SCHEMA_VERSION
            or not parallel_paper_stage_inventory_valid(
                manifest_parallel_stage_tables
            )
            or int(manifest.get("parallel_paper_stage_count") or 0)
            != len(manifest_parallel_stage_tables)
            or manifest.get("parallel_paper_stage_inventory_passed") is not True
        ):
            blockers.append("evaluator_snapshot_parallel_paper_stage_inventory_invalid")
        if manifest.get("parallel_paper_stages_all_pinned") is not True:
            blockers.append("evaluator_snapshot_parallel_paper_stage_pin_invalid")
        if (
            manifest.get(
                "parallel_paper_stages_all_merged_after_source_read_lock_release"
            )
            is not True
        ):
            blockers.append("evaluator_snapshot_parallel_paper_stage_merge_order_invalid")
        if (
            manifest.get("parallel_paper_stages_all_removed_before_publish")
            is not True
        ):
            blockers.append("evaluator_snapshot_parallel_paper_stage_cleanup_invalid")
        if manifest.get("paper_decision_parallel_read_view_pinned") is not True:
            blockers.append("evaluator_snapshot_paper_decision_parallel_pin_invalid")
        if (
            manifest.get(
                "paper_decision_parallel_stage_merged_after_source_read_lock_release"
            )
            is not True
        ):
            blockers.append("evaluator_snapshot_paper_decision_merge_lock_order_invalid")
        if (
            manifest.get("paper_decision_parallel_stage_removed_before_publish")
            is not True
        ):
            blockers.append("evaluator_snapshot_paper_decision_stage_cleanup_invalid")
        try:
            manifest_read_lock_limit = float(manifest.get("max_source_read_lock_sec"))
            if manifest_read_lock_limit <= 0:
                raise ValueError("non-positive")
        except (TypeError, ValueError):
            manifest_read_lock_limit = None
            blockers.append("evaluator_snapshot_source_read_lock_limit_invalid")
        if manifest.get("source_mutation_free") is not True:
            blockers.append("evaluator_snapshot_source_mutation_contract_failed")
        if manifest.get("bounded_selective_snapshot") is not True:
            blockers.append("evaluator_snapshot_not_bounded_selective")
        if manifest.get("selection_upper_bounds_consistent") is not True:
            blockers.append("evaluator_snapshot_selection_upper_bounds_inconsistent")
        if manifest.get("output_cap_passed") is not True:
            blockers.append("evaluator_snapshot_output_cap_failed")
        disk_preflight = manifest.get("disk_preflight") or {}
        if not isinstance(disk_preflight, dict) or disk_preflight.get("accepted") is not True:
            blockers.append("evaluator_snapshot_disk_preflight_failed")
        else:
            try:
                disk_free_bytes = int(disk_preflight.get("free_bytes"))
                disk_reserve_bytes = int(disk_preflight.get("required_reserve_bytes"))
                disk_free_after_bytes = int(disk_preflight.get("estimated_free_after_bytes"))
                disk_cap_bytes = int(disk_preflight.get("selective_snapshot_output_cap_bytes"))
                total_stage_cap_bytes = int(
                    disk_preflight.get("temporary_stage_total_cap_bytes")
                )
                candidate_stage_cap_bytes = int(
                    disk_preflight.get("temporary_candidate_stage_cap_bytes")
                )
                raw_parallel_stage_caps = disk_preflight.get(
                    "temporary_parallel_paper_stage_cap_bytes"
                )
                if not isinstance(raw_parallel_stage_caps, dict):
                    raise ValueError("parallel stage caps missing")
                parallel_stage_cap_bytes = {
                    str(table): int(value)
                    for table, value in raw_parallel_stage_caps.items()
                }
                candidate_stage_minimum_cap_bytes = int(
                    disk_preflight.get("candidate_stage_minimum_cap_bytes")
                )
                parallel_stage_minimum_cap_bytes = int(
                    disk_preflight.get("parallel_paper_stage_minimum_cap_bytes")
                )
                estimated_peak_working_bytes = int(
                    disk_preflight.get("estimated_peak_working_bytes")
                )
                estimated_free_at_peak_bytes = int(
                    disk_preflight.get("estimated_free_at_peak_bytes")
                )
                expected_total_stage_cap_bytes = max(
                    0,
                    disk_free_bytes - disk_cap_bytes - disk_reserve_bytes,
                )
                active_stage_tables = safe_manifest_parallel_stage_tables
                raw_disk_active_tables = disk_preflight.get(
                    "parallel_paper_stage_tables"
                )
                raw_disk_configured_tables = disk_preflight.get(
                    "configured_parallel_paper_stage_tables"
                )
                raw_disk_omitted_tables = disk_preflight.get(
                    "omitted_optional_parallel_paper_stage_tables"
                )
                if (
                    not isinstance(raw_disk_active_tables, list)
                    or not isinstance(raw_disk_configured_tables, list)
                    or not isinstance(raw_disk_omitted_tables, list)
                ):
                    raise ValueError("parallel stage inventory evidence missing")
                disk_active_tables = tuple(
                    str(table) for table in raw_disk_active_tables
                )
                disk_configured_tables = tuple(
                    str(table) for table in raw_disk_configured_tables
                )
                disk_omitted_tables = tuple(
                    str(table) for table in raw_disk_omitted_tables
                )
                expected_omitted_tables = tuple(
                    table
                    for table in PARALLEL_PAPER_OPTIONAL_STAGE_TABLES
                    if table not in active_stage_tables
                )
                expected_stage_shares = {
                    table: float(
                        PARALLEL_PAPER_STAGE_CONFIGS[table]["residual_share"]
                    )
                    for table in active_stage_tables
                }
                expected_active_weight_total = float(
                    CANDIDATE_STAGE_RESIDUAL_SHARE
                    + sum(expected_stage_shares.values())
                )
                expected_candidate_normalized_share = (
                    CANDIDATE_STAGE_RESIDUAL_SHARE
                    / expected_active_weight_total
                )
                expected_normalized_stage_shares = {
                    table: share / expected_active_weight_total
                    for table, share in expected_stage_shares.items()
                }
                minimum_stage_total = (
                    MIN_CANDIDATE_STAGE_CAP_BYTES
                    + len(active_stage_tables)
                    * MIN_PARALLEL_PAPER_STAGE_CAP_BYTES
                )
                expected_parallel_stage_cap_bytes: dict[str, int] = {
                    table: 0 for table in active_stage_tables
                }
                if expected_total_stage_cap_bytes >= minimum_stage_total:
                    residual_after_minimums = (
                        expected_total_stage_cap_bytes - minimum_stage_total
                    )
                    expected_candidate_stage_cap_bytes = (
                        MIN_CANDIDATE_STAGE_CAP_BYTES
                    )
                    for table in active_stage_tables:
                        expected_parallel_stage_cap_bytes[table] = (
                            MIN_PARALLEL_PAPER_STAGE_CAP_BYTES
                        )
                    allocation_targets = ("candidate", *active_stage_tables)
                    allocated = (
                        expected_candidate_stage_cap_bytes
                        + sum(expected_parallel_stage_cap_bytes.values())
                    )
                    for target in allocation_targets[:-1]:
                        normalized_share = (
                            expected_candidate_normalized_share
                            if target == "candidate"
                            else expected_normalized_stage_shares[target]
                        )
                        extra = (
                            int(residual_after_minimums * normalized_share)
                            // 4096
                            * 4096
                        )
                        if target == "candidate":
                            expected_candidate_stage_cap_bytes += extra
                        else:
                            expected_parallel_stage_cap_bytes[target] += extra
                        allocated += extra
                    final_target = allocation_targets[-1]
                    final_extra = expected_total_stage_cap_bytes - allocated
                    if final_target == "candidate":
                        expected_candidate_stage_cap_bytes += final_extra
                    else:
                        expected_parallel_stage_cap_bytes[final_target] += (
                            final_extra
                        )
                else:
                    remaining = expected_total_stage_cap_bytes
                    expected_candidate_stage_cap_bytes = min(
                        remaining,
                        MIN_CANDIDATE_STAGE_CAP_BYTES,
                    )
                    remaining -= expected_candidate_stage_cap_bytes
                    for table in active_stage_tables:
                        expected_parallel_stage_cap_bytes[table] = min(
                            remaining,
                            MIN_PARALLEL_PAPER_STAGE_CAP_BYTES,
                        )
                        remaining -= expected_parallel_stage_cap_bytes[table]
                raw_stage_shares = disk_preflight.get(
                    "parallel_paper_stage_residual_shares"
                )
                raw_normalized_stage_shares = disk_preflight.get(
                    "parallel_paper_stage_normalized_shares"
                )
                if (
                    not isinstance(raw_stage_shares, dict)
                    or not isinstance(raw_normalized_stage_shares, dict)
                ):
                    raise ValueError("parallel stage shares missing")
                stage_shares = {
                    str(table): float(value)
                    for table, value in raw_stage_shares.items()
                }
                normalized_stage_shares = {
                    str(table): float(value)
                    for table, value in raw_normalized_stage_shares.items()
                }
                if (
                    disk_free_bytes <= 0
                    or disk_reserve_bytes < 0
                    or disk_free_after_bytes < disk_reserve_bytes
                    or disk_cap_bytes <= 0
                    or total_stage_cap_bytes != expected_total_stage_cap_bytes
                    or candidate_stage_cap_bytes
                    != expected_candidate_stage_cap_bytes
                    or disk_active_tables != active_stage_tables
                    or disk_configured_tables
                    != tuple(PARALLEL_PAPER_STAGE_TABLES)
                    or disk_omitted_tables != expected_omitted_tables
                    or set(parallel_stage_cap_bytes)
                    != set(active_stage_tables)
                    or parallel_stage_cap_bytes
                    != expected_parallel_stage_cap_bytes
                    or candidate_stage_cap_bytes
                    + sum(parallel_stage_cap_bytes.values())
                    != total_stage_cap_bytes
                    or candidate_stage_cap_bytes < MIN_CANDIDATE_STAGE_CAP_BYTES
                    or any(
                        parallel_stage_cap_bytes[table]
                        < MIN_PARALLEL_PAPER_STAGE_CAP_BYTES
                        for table in active_stage_tables
                    )
                    or candidate_stage_minimum_cap_bytes
                    != MIN_CANDIDATE_STAGE_CAP_BYTES
                    or parallel_stage_minimum_cap_bytes
                    != MIN_PARALLEL_PAPER_STAGE_CAP_BYTES
                    or abs(
                        float(disk_preflight.get("candidate_stage_residual_share"))
                        - CANDIDATE_STAGE_RESIDUAL_SHARE
                    )
                    > 1e-9
                    or set(stage_shares) != set(active_stage_tables)
                    or any(
                        abs(stage_shares[table] - expected_stage_shares[table])
                        > 1e-9
                        for table in active_stage_tables
                    )
                    or set(normalized_stage_shares)
                    != set(active_stage_tables)
                    or any(
                        abs(
                            normalized_stage_shares[table]
                            - expected_normalized_stage_shares[table]
                        )
                        > 1e-9
                        for table in active_stage_tables
                    )
                    or abs(
                        float(
                            disk_preflight.get(
                                "parallel_paper_stage_active_weight_total"
                            )
                        )
                        - expected_active_weight_total
                    )
                    > 1e-9
                    or abs(
                        float(
                            disk_preflight.get(
                                "candidate_stage_normalized_share"
                            )
                        )
                        - expected_candidate_normalized_share
                    )
                    > 1e-9
                    or int(
                        disk_preflight.get(
                            "temporary_paper_decision_stage_cap_bytes"
                        )
                    )
                    != parallel_stage_cap_bytes.get(
                        PAPER_DECISION_STAGE_TABLE,
                        0,
                    )
                    or disk_preflight.get("candidate_stage_budget_mode")
                    != CANDIDATE_STAGE_BUDGET_MODE
                    or estimated_peak_working_bytes
                    != (
                        disk_cap_bytes
                        + candidate_stage_cap_bytes
                        + sum(parallel_stage_cap_bytes.values())
                    )
                    or estimated_free_at_peak_bytes
                    != disk_free_bytes - estimated_peak_working_bytes
                    or estimated_free_at_peak_bytes < disk_reserve_bytes
                    or int(disk_preflight.get("temporary_full_backup_bytes") or 0) != 0
                    or disk_preflight.get("fail_closed_on_insufficient_space") is not True
                ):
                    raise ValueError("invalid disk preflight evidence")
            except (TypeError, ValueError):
                blockers.append("evaluator_snapshot_disk_preflight_contract_invalid")
        try:
            output_size_bytes = int(manifest.get("output_size_bytes"))
            output_cap_bytes = int(manifest.get("output_cap_bytes"))
            if output_size_bytes <= 0 or output_cap_bytes <= 0 or output_size_bytes > output_cap_bytes:
                blockers.append("evaluator_snapshot_output_size_contract_invalid")
            if isinstance(disk_preflight, dict) and disk_preflight.get("accepted") is True:
                try:
                    if int(disk_preflight.get("selective_snapshot_output_cap_bytes")) != output_cap_bytes:
                        blockers.append("evaluator_snapshot_disk_output_cap_mismatch")
                except (TypeError, ValueError):
                    blockers.append("evaluator_snapshot_disk_output_cap_mismatch")
        except (TypeError, ValueError):
            output_size_bytes = None
            output_cap_bytes = None
            blockers.append("evaluator_snapshot_output_size_contract_invalid")
        if manifest.get("partial_artifacts_absent") is not True:
            blockers.append("evaluator_snapshot_partial_artifact_contract_failed")
        if manifest.get("active_database_reads_allowed_for_autoloop") is not False:
            blockers.append("evaluator_snapshot_active_database_read_contract_invalid")
        selection = manifest.get("selection_contract") or {}
        if selection.get("schema_version") != SELECTION_SCHEMA_VERSION:
            blockers.append("evaluator_snapshot_selection_contract_invalid")
        if selection.get("future_rows_excluded") is not True:
            blockers.append("evaluator_snapshot_future_row_contract_invalid")
        if selection.get("table_rules_are_explicit") is not True:
            blockers.append("evaluator_snapshot_table_selection_contract_invalid")
        supported_windows = set(selection.get("supported_capture_windows_hours") or [])
        if not {24, 48, 72}.issubset(supported_windows):
            blockers.append("evaluator_snapshot_capture_window_coverage_invalid")
        try:
            snapshot_upper_epoch = float(manifest.get("snapshot_ts"))
            if abs(float(selection.get("common_upper_epoch")) - snapshot_upper_epoch) > 0.001:
                blockers.append("evaluator_snapshot_common_upper_timestamp_mismatch")
        except (TypeError, ValueError):
            blockers.append("evaluator_snapshot_common_upper_timestamp_invalid")
        if not manifest.get("git_commit"):
            blockers.append("evaluator_snapshot_git_commit_missing")
        if manifest.get("snapshot_ts") is None:
            blockers.append("evaluator_snapshot_timestamp_missing")
        else:
            try:
                snapshot_age_sec_value = (
                    float(now_ts if now_ts is not None else time.time())
                    - float(manifest["snapshot_ts"])
                )
                if snapshot_age_sec_value < -60:
                    blockers.append("evaluator_snapshot_timestamp_in_future")
                if max_age_sec > 0 and snapshot_age_sec_value > float(max_age_sec):
                    blockers.append("evaluator_snapshot_stale")
            except (TypeError, ValueError):
                blockers.append("evaluator_snapshot_timestamp_invalid")
        reports = manifest.get("databases") or {}
        all_pinned_read_views: list[dict] = []
        for name, candidate in candidates.items():
            report = reports.get(name) or {}
            expected_path = report.get("snapshot_path")
            if not expected_path or Path(expected_path).expanduser().resolve() != candidate:
                blockers.append(f"evaluator_snapshot_{name}_path_mismatch")
            if report.get("quick_check") != ["ok"]:
                blockers.append(f"evaluator_snapshot_{name}_quick_check_invalid")
            expected_sha = report.get("snapshot_sha256")
            if not expected_sha:
                blockers.append(f"evaluator_snapshot_{name}_sha256_missing")
            if report.get("schema_version") is None:
                blockers.append(f"evaluator_snapshot_{name}_schema_version_missing")
            if not report.get("upper_watermarks"):
                blockers.append(f"evaluator_snapshot_{name}_watermarks_missing")
            if report.get("missing_required_watermarks"):
                blockers.append(f"evaluator_snapshot_{name}_required_watermarks_missing")
            if report.get("database_budget_passed") is not True:
                blockers.append(f"evaluator_snapshot_{name}_database_budget_failed")
            if report.get("source_open_mode") != "read_only_attached_uri":
                blockers.append(f"evaluator_snapshot_{name}_source_open_mode_invalid")
            if report.get("source_read_lock_budget_passed") is not True:
                blockers.append(
                    f"evaluator_snapshot_{name}_source_read_lock_budget_failed"
                )
            if report.get("source_read_lock_released_before_index_build") is not True:
                blockers.append(
                    f"evaluator_snapshot_{name}_index_build_lock_order_invalid"
                )
            try:
                report_read_lock_limit = float(report.get("source_read_lock_limit_sec"))
                report_read_lock_duration = float(
                    report.get("source_read_lock_duration_sec")
                )
                if (
                    report_read_lock_limit <= 0
                    or report_read_lock_duration < 0
                    or report_read_lock_duration > report_read_lock_limit + 1.0
                    or (
                        manifest_read_lock_limit is not None
                        and abs(report_read_lock_limit - manifest_read_lock_limit) > 0.001
                    )
                ):
                    raise ValueError("lock budget mismatch")
            except (TypeError, ValueError):
                blockers.append(
                    f"evaluator_snapshot_{name}_source_read_lock_contract_invalid"
                )
            if int(report.get("temporary_full_backup_size_bytes") or 0) != 0:
                blockers.append(f"evaluator_snapshot_{name}_full_backup_intermediate_detected")
            try:
                if abs(float(report.get("selection_upper_epoch")) - float(manifest.get("snapshot_ts"))) > 0.001:
                    blockers.append(f"evaluator_snapshot_{name}_selection_upper_mismatch")
            except (TypeError, ValueError):
                blockers.append(f"evaluator_snapshot_{name}_selection_upper_invalid")
            report_pinned_read_views = report.get("pinned_read_views")
            if (
                not isinstance(report_pinned_read_views, list)
                or not report_pinned_read_views
                or not all(isinstance(view, dict) for view in report_pinned_read_views)
            ):
                blockers.append(
                    f"evaluator_snapshot_{name}_pinned_read_views_invalid"
                )
            else:
                all_pinned_read_views.extend(report_pinned_read_views)
            selected_tables = report.get("selected_tables") or {}
            source_watermark_evidence = report.get("source_watermark_query_evidence") or {}
            for watermark_table in (DATABASE_SPECS[name].get("watermarks") or {}):
                watermark_rule = (
                    (DATABASE_SPECS[name].get("tables") or {}).get(watermark_table)
                    or {}
                )
                if watermark_rule.get("indexed_epoch_seconds_anchor"):
                    continue
                watermark_report = source_watermark_evidence.get(watermark_table) or {}
                if (
                    watermark_report.get("strategy")
                    != "deferred_to_frozen_snapshot"
                    or watermark_report.get("source_query_executed") is not False
                ):
                    blockers.append(
                        f"evaluator_snapshot_{name}_source_watermark_not_deferred:"
                        f"{watermark_table}"
                    )
            if name == "paper":
                pinned_read_views = report.get("pinned_read_views") or []
                parallel_stage_reports = report.get("parallel_paper_stages") or {}
                raw_report_parallel_stage_tables = report.get(
                    "parallel_paper_stage_tables"
                )
                active_parallel_stage_tables = safe_manifest_parallel_stage_tables
                try:
                    final_stage_schema_evidence = sqlite_table_schema_evidence(
                        candidate,
                        active_parallel_stage_tables,
                    )
                    if not isinstance(
                        raw_report_parallel_stage_tables,
                        list,
                    ):
                        raise ValueError("parallel stage table inventory missing")
                    report_parallel_stage_tables = tuple(
                        str(table)
                        for table in raw_report_parallel_stage_tables
                    )
                    if not isinstance(parallel_stage_reports, dict):
                        raise ValueError("parallel stage reports missing")
                    parallel_stage_caps = disk_preflight.get(
                        "temporary_parallel_paper_stage_cap_bytes"
                    )
                    if not isinstance(parallel_stage_caps, dict):
                        raise ValueError("parallel stage caps missing")
                    parallel_lock_durations = report.get(
                        "parallel_paper_source_read_lock_duration_sec"
                    )
                    if not isinstance(parallel_lock_durations, dict):
                        raise ValueError("parallel lock durations missing")
                    main_lock_duration_sec = float(
                        report.get("main_source_read_lock_duration_sec")
                    )
                    reported_max_lock_duration_sec = float(
                        report.get("source_read_lock_duration_sec")
                    )
                    roles = {
                        str(view.get("role"))
                        for view in pinned_read_views
                        if isinstance(view, dict)
                    }
                    expected_paper_roles = {
                        "paper_main_selective_copy",
                        *{
                            str(PARALLEL_PAPER_STAGE_CONFIGS[table]["role"])
                            for table in active_parallel_stage_tables
                        },
                    }
                    optional_absence_valid = all(
                        table in active_parallel_stage_tables
                        or (
                            (selected_tables.get(table) or {}).get("included")
                            is False
                            and (selected_tables.get(table) or {}).get("required")
                            is False
                            and (selected_tables.get(table) or {}).get("reason")
                            == "optional_source_table_missing"
                        )
                        for table in PARALLEL_PAPER_OPTIONAL_STAGE_TABLES
                    )
                    if (
                        not parallel_paper_stage_inventory_valid(
                            active_parallel_stage_tables
                        )
                        or report_parallel_stage_tables
                        != active_parallel_stage_tables
                        or report.get("parallel_paper_stage_count")
                        != len(active_parallel_stage_tables)
                        or set(parallel_stage_reports)
                        != set(active_parallel_stage_tables)
                        or set(parallel_lock_durations)
                        != set(active_parallel_stage_tables)
                        or set(final_stage_schema_evidence)
                        != set(active_parallel_stage_tables)
                        or not optional_absence_valid
                        or report.get("parallel_paper_stages_all_pinned") is not True
                        or report.get(
                            "parallel_paper_stages_all_merged_after_source_read_lock_release"
                        )
                        is not True
                        or report.get(
                            "parallel_paper_stages_all_removed_before_publish"
                        )
                        is not True
                        or not isinstance(pinned_read_views, list)
                        or len(pinned_read_views)
                        != 1 + len(active_parallel_stage_tables)
                        or roles != expected_paper_roles
                        or main_lock_duration_sec < 0
                        or any(
                            not math.isfinite(float(parallel_lock_durations[table]))
                            or float(parallel_lock_durations[table]) < 0
                            for table in active_parallel_stage_tables
                        )
                        or abs(
                            reported_max_lock_duration_sec
                            - max(
                                main_lock_duration_sec,
                                *[
                                    float(parallel_lock_durations[table])
                                    for table in active_parallel_stage_tables
                                ],
                            )
                        )
                        > 0.001
                    ):
                        raise ValueError("invalid parallel paper stage inventory")
                    for table in active_parallel_stage_tables:
                        config = PARALLEL_PAPER_STAGE_CONFIGS[table]
                        stage_report = parallel_stage_reports.get(table) or {}
                        selection_report = selected_tables.get(table) or {}
                        nested_stage = selection_report.get("parallel_stage") or {}
                        stage_size_bytes = int(stage_report.get("stage_size_bytes"))
                        stage_budget_bytes = int(stage_report.get("stage_budget_bytes"))
                        stage_page_size = int(stage_report.get("stage_page_size"))
                        rows_copied = int(stage_report.get("rows_copied"))
                        rows_merged = int(stage_report.get("rows_merged"))
                        merge_duration_sec = float(
                            stage_report.get("merge_duration_sec")
                        )
                        lock_duration_sec = float(
                            stage_report.get("source_read_lock_duration_sec")
                        )
                        final_schema = final_stage_schema_evidence.get(table) or {}
                        final_columns = final_schema.get("columns") or []
                        expected_stage_definitions = []
                        for column in final_columns:
                            column_name = quote_identifier(str(column["name"]))
                            declared_type = str(
                                column.get("declared_type") or ""
                            ).strip()
                            expected_stage_definitions.append(
                                f"{column_name} {declared_type}"
                                if declared_type
                                else column_name
                            )
                        expected_stage_create_sql_sha256 = sha256_text(
                            f"CREATE TABLE {quote_identifier(table)} "
                            f"({', '.join(expected_stage_definitions)})"
                        )
                        source_create_sql_sha256 = str(
                            stage_report.get("source_create_sql_sha256") or ""
                        )
                        stage_create_sql_sha256 = str(
                            stage_report.get("stage_create_sql_sha256") or ""
                        )
                        destination_create_sql_sha256 = str(
                            stage_report.get("destination_create_sql_sha256")
                            or ""
                        )
                        source_column_contract_sha256 = str(
                            stage_report.get("source_column_contract_sha256")
                            or ""
                        )
                        stage_column_contract_sha256 = str(
                            stage_report.get("stage_column_contract_sha256")
                            or ""
                        )
                        destination_column_contract_sha256 = str(
                            stage_report.get(
                                "destination_column_contract_sha256"
                            )
                            or ""
                        )
                        stage_column_count = int(
                            stage_report.get("stage_column_count")
                        )
                        stage_index_count = int(
                            stage_report.get("stage_index_count")
                        )
                        if (
                            stage_report.get("schema_version")
                            != PARALLEL_PAPER_STAGE_SCHEMA_VERSION
                            or stage_report.get("role") != config["role"]
                            or stage_report.get("stage_schema_mode")
                            != PARALLEL_PAPER_STAGE_STORAGE_MODE
                            or not all(
                                valid_sha256_hex(value)
                                for value in (
                                    source_create_sql_sha256,
                                    stage_create_sql_sha256,
                                    destination_create_sql_sha256,
                                    source_column_contract_sha256,
                                    stage_column_contract_sha256,
                                    destination_column_contract_sha256,
                                )
                            )
                            or source_create_sql_sha256
                            != destination_create_sql_sha256
                            or source_create_sql_sha256
                            != final_schema.get("create_sql_sha256")
                            or stage_create_sql_sha256
                            != expected_stage_create_sql_sha256
                            or source_column_contract_sha256
                            != stage_column_contract_sha256
                            or source_column_contract_sha256
                            != destination_column_contract_sha256
                            or source_column_contract_sha256
                            != final_schema.get("column_contract_sha256")
                            or stage_column_count
                            != int(final_schema.get("column_count"))
                            or int(final_schema.get("hidden_column_count")) != 0
                            or stage_report.get("stage_column_contract_passed")
                            is not True
                            or stage_index_count != 0
                            or stage_report.get(
                                "source_constraints_deferred_off_source_lock"
                            )
                            is not True
                            or stage_report.get(
                                "destination_schema_restored_after_source_read_lock_release"
                            )
                            is not True
                            or stage_report.get(
                                "source_constraints_rebuilt_after_source_read_lock_release"
                            )
                            is not True
                            or stage_size_bytes <= 0
                            or stage_budget_bytes != int(parallel_stage_caps[table])
                            or stage_size_bytes > stage_budget_bytes
                            or stage_page_size != 4096
                            or rows_copied != int(selection_report.get("rows_copied"))
                            or rows_merged != rows_copied
                            or merge_duration_sec < 0
                            or lock_duration_sec
                            != float(parallel_lock_durations[table])
                            or stage_report.get("source_read_lock_budget_passed")
                            is not True
                            or stage_report.get(
                                "merged_after_source_read_lock_release"
                            )
                            is not True
                            or stage_report.get("removed_before_publish") is not True
                            or stage_report.get("quick_check") != ["ok"]
                            or stage_report.get("full_fidelity_row_copy") is not True
                            or stage_report.get("payload_semantics_preserved") is not True
                            or nested_stage.get("schema_version")
                            != PARALLEL_PAPER_STAGE_SCHEMA_VERSION
                            or nested_stage.get("role") != config["role"]
                            or nested_stage.get("stage_schema_mode")
                            != PARALLEL_PAPER_STAGE_STORAGE_MODE
                            or nested_stage.get("source_create_sql_sha256")
                            != source_create_sql_sha256
                            or nested_stage.get("stage_create_sql_sha256")
                            != stage_create_sql_sha256
                            or nested_stage.get("destination_create_sql_sha256")
                            != destination_create_sql_sha256
                            or nested_stage.get(
                                "source_column_contract_sha256"
                            )
                            != source_column_contract_sha256
                            or nested_stage.get(
                                "stage_column_contract_sha256"
                            )
                            != stage_column_contract_sha256
                            or nested_stage.get(
                                "destination_column_contract_sha256"
                            )
                            != destination_column_contract_sha256
                            or int(nested_stage.get("stage_column_count"))
                            != stage_column_count
                            or nested_stage.get("stage_column_contract_passed")
                            is not True
                            or int(nested_stage.get("stage_index_count")) != 0
                            or nested_stage.get(
                                "source_constraints_deferred_off_source_lock"
                            )
                            is not True
                            or nested_stage.get(
                                "destination_schema_restored_after_source_read_lock_release"
                            )
                            is not True
                            or nested_stage.get(
                                "source_constraints_rebuilt_after_source_read_lock_release"
                            )
                            is not True
                            or nested_stage.get("full_fidelity_row_copy") is not True
                            or nested_stage.get("payload_semantics_preserved") is not True
                            or nested_stage.get("row_count_matched") is not True
                            or int(nested_stage.get("stage_rows_copied"))
                            != rows_copied
                            or int(nested_stage.get("rows_merged")) != rows_merged
                            or nested_stage.get("quick_check") != ["ok"]
                            or int(nested_stage.get("stage_page_size")) != 4096
                            or int(nested_stage.get("stage_size_bytes"))
                            != stage_size_bytes
                            or int(nested_stage.get("stage_budget_bytes"))
                            != stage_budget_bytes
                            or nested_stage.get("source_read_lock_budget_passed")
                            is not True
                            or nested_stage.get(
                                "merge_started_after_source_read_view_release"
                            )
                            is not True
                        ):
                            raise ValueError(
                                f"invalid parallel paper stage evidence:{table}"
                            )
                    paper_alias = parallel_stage_reports[PAPER_DECISION_STAGE_TABLE]
                    if (
                        report.get("paper_decision_parallel_stage_used") is not True
                        or report.get("paper_decision_parallel_stage_schema_version")
                        != paper_alias.get("schema_version")
                        or report.get("paper_decision_parallel_read_view_pinned")
                        is not True
                        or report.get(
                            "paper_decision_parallel_stage_merged_after_source_read_lock_release"
                        )
                        is not True
                        or report.get(
                            "paper_decision_parallel_stage_removed_before_publish"
                        )
                        is not True
                        or int(report.get("paper_decision_parallel_stage_size_bytes"))
                        != int(paper_alias.get("stage_size_bytes"))
                        or int(report.get("paper_decision_parallel_stage_page_size"))
                        != int(paper_alias.get("stage_page_size"))
                        or int(report.get("paper_decision_parallel_stage_budget_bytes"))
                        != int(paper_alias.get("stage_budget_bytes"))
                        or int(report.get("paper_decision_parallel_stage_rows_merged"))
                        != int(paper_alias.get("rows_merged"))
                    ):
                        raise ValueError("paper decision compatibility alias mismatch")
                except (KeyError, TypeError, ValueError, sqlite3.Error):
                    blockers.append(
                        "evaluator_snapshot_parallel_paper_stage_contract_invalid"
                    )
                if report.get("candidate_projection_after_source_read_lock_release") is not True:
                    blockers.append(
                        "evaluator_snapshot_paper_candidate_projection_lock_order_invalid"
                    )
                if report.get("temporary_candidate_stage_removed_before_publish") is not True:
                    blockers.append(
                        "evaluator_snapshot_paper_candidate_stage_cleanup_invalid"
                    )
                candidate_projection = (
                    selected_tables.get("candidate_shadow_observations") or {}
                ).get("storage_projection") or {}
                if candidate_projection.get("applied") is not True:
                    blockers.append("evaluator_snapshot_candidate_payload_projection_required")
                try:
                    stage_size_bytes = int(
                        report.get("temporary_candidate_stage_size_bytes")
                    )
                    stage_cap_bytes = int(
                        disk_preflight.get("temporary_candidate_stage_cap_bytes")
                    )
                    projection_duration_sec = float(
                        report.get("candidate_projection_duration_sec")
                    )
                    stage_plan = candidate_projection.get("stage_query_plan") or []
                    if (
                        candidate_projection.get("applied") is not True
                        or stage_size_bytes <= 0
                        or stage_cap_bytes <= 0
                        or stage_size_bytes > stage_cap_bytes
                        or projection_duration_sec < 0
                        or candidate_projection.get(
                            "projection_started_after_source_read_view_release"
                        )
                        is not True
                        or candidate_projection.get("source_stage_schema_version")
                        != "candidate_observation_selective_stage.v1"
                        or int(candidate_projection.get("source_stage_size_bytes") or 0)
                        != stage_size_bytes
                        or candidate_projection.get("stage_order_index_name")
                        != "idx_a3_candidate_stage_signal_candidate"
                        or candidate_projection.get("stage_query_plan_uses_order_index")
                        is not True
                        or candidate_projection.get("stage_query_plan_temp_btree_detected")
                        is not False
                        or not isinstance(stage_plan, list)
                        or not stage_plan
                        or not any(
                            "idx_a3_candidate_stage_signal_candidate" in str(item)
                            for item in stage_plan
                        )
                        or any("TEMP B-TREE" in str(item).upper() for item in stage_plan)
                    ):
                        raise ValueError("invalid off-lock candidate projection evidence")
                except (TypeError, ValueError):
                    blockers.append(
                        "evaluator_snapshot_candidate_stage_projection_contract_invalid"
                    )
                if (
                    candidate_projection.get("schema_version")
                    != "candidate_observation_payload_projection.v1"
                    or candidate_projection.get("payload_semantics_preserved") is not True
                    or candidate_projection.get("unknown_payload_keys_preserved") is not True
                    or candidate_projection.get("missing_and_null_keys_preserved") is not True
                ):
                    blockers.append(
                        "evaluator_snapshot_candidate_payload_projection_invalid"
                    )
            for required_table in (
                ("premium_signals",) if name == "signal" else
                (
                    "candidate_shadow_observations",
                    "candidate_shadow_virtual_trades",
                    "paper_decision_events",
                    "a_class_decision_events",
                    "a_class_mode_runtime_state",
                    "paper_trades",
                    "opportunity_events",
                ) if name == "paper" else
                ("raw_signal_outcomes",) if name == "raw" else
                ("kline_1m",)
            ):
                if (selected_tables.get(required_table) or {}).get("included") is not True:
                    blockers.append(
                        f"evaluator_snapshot_{name}_required_selection_missing:{required_table}"
                    )
            for table, selection_report in selected_tables.items():
                if not isinstance(selection_report, dict) or selection_report.get("included") is not True:
                    continue
                rule = (DATABASE_SPECS[name].get("tables") or {}).get(table) or {}
                indexed_anchor = rule.get("indexed_epoch_seconds_anchor")
                if indexed_anchor:
                    source_index_name = selection_report.get("source_index_name")
                    source_index_columns = selection_report.get("source_index_columns")
                    watermark_evidence = source_watermark_evidence.get(table) or {}
                    watermark_plan = watermark_evidence.get("query_plan")
                    if (
                        watermark_evidence.get("strategy") != "indexed_anchor_max"
                        or watermark_evidence.get("column") != indexed_anchor
                        or watermark_evidence.get("source_index_name") != source_index_name
                        or not isinstance(source_index_name, str)
                        or not source_index_name
                        or watermark_evidence.get("uses_declared_index") is not True
                        or watermark_evidence.get("full_table_scan_detected") is not False
                        or not isinstance(watermark_plan, list)
                        or not watermark_plan
                        or not all(
                            isinstance(value, str)
                            and value
                            and source_index_name in value
                            for value in watermark_plan
                        )
                    ):
                        blockers.append(
                            f"evaluator_snapshot_{name}_indexed_watermark_invalid:{table}"
                        )
                    if (
                        selection_report.get("predicate_strategy") != "indexed_epoch_seconds"
                        or selection_report.get("indexed_time_anchor") != indexed_anchor
                        or not isinstance(source_index_name, str)
                        or not source_index_name.strip()
                        or not isinstance(source_index_columns, list)
                        or not source_index_columns
                        or source_index_columns[0] != indexed_anchor
                        or selection_report.get("source_index_partial") is not False
                    ):
                        blockers.append(
                            f"evaluator_snapshot_{name}_indexed_time_selection_invalid:{table}"
                        )
                    query_plan = selection_report.get("source_query_plan")
                    if (
                        not isinstance(query_plan, list)
                        or not query_plan
                        or not all(isinstance(value, str) and value for value in query_plan)
                        or selection_report.get("source_query_plan_uses_index") is not True
                        or selection_report.get("source_query_plan_uses_range_search") is not True
                        or selection_report.get(
                            "source_query_plan_full_table_scan_detected"
                        ) is not False
                    ):
                        blockers.append(
                            f"evaluator_snapshot_{name}_indexed_query_plan_invalid:{table}"
                        )
                time_semantics = selection_report.get("time_semantics")
                if time_semantics == "event_time" and selection_report.get("future_bound_enforced") is not True:
                    blockers.append(
                        f"evaluator_snapshot_{name}_future_bound_missing:{table}"
                    )
                if time_semantics not in {"event_time", "timeless_reference"}:
                    blockers.append(
                        f"evaluator_snapshot_{name}_time_semantics_invalid:{table}"
                    )
            if candidate.is_file() and int(report.get("snapshot_size_bytes") or -1) != candidate.stat().st_size:
                blockers.append(f"evaluator_snapshot_{name}_size_mismatch")
            if candidate.is_file():
                try:
                    actual_sha = sha256_file(candidate)
                    actual_quick_check = sqlite_quick_check(candidate)
                    temporal_bounds = (
                        sqlite_temporal_bounds(
                            candidate,
                            DATABASE_SPECS[name],
                            upper_epoch=snapshot_upper_epoch,
                        )
                        if snapshot_upper_epoch is not None
                        else {}
                    )
                    verified_integrity[name] = {
                        "sha256": actual_sha,
                        "sha256_matches_manifest": bool(expected_sha and actual_sha == expected_sha),
                        "quick_check": actual_quick_check,
                        "temporal_bounds": temporal_bounds,
                    }
                    if not expected_sha or actual_sha != expected_sha:
                        blockers.append(f"evaluator_snapshot_{name}_sha256_mismatch")
                    if actual_quick_check != ["ok"]:
                        blockers.append(f"evaluator_snapshot_{name}_quick_check_revalidation_failed")
                    for table, temporal in temporal_bounds.items():
                        selection_report = selected_tables.get(table) or {}
                        expected_mode = DATABASE_SPECS[name]["tables"][table]["mode"]
                        if selection_report.get("selection_mode") != expected_mode:
                            blockers.append(
                                f"evaluator_snapshot_{name}_selection_mode_mismatch:{table}"
                            )
                        if selection_report.get("time_columns") != temporal.get("time_columns"):
                            blockers.append(
                                f"evaluator_snapshot_{name}_time_columns_mismatch:{table}"
                            )
                        if selection_report.get("upper_bound_columns") != temporal.get("time_columns"):
                            blockers.append(
                                f"evaluator_snapshot_{name}_upper_bound_columns_mismatch:{table}"
                            )
                        if temporal.get("missing_time_columns") is True:
                            blockers.append(
                                f"evaluator_snapshot_{name}_time_columns_missing:{table}"
                            )
                        if temporal.get("upper_bound_passed") is not True:
                            blockers.append(
                                f"evaluator_snapshot_{name}_future_rows_detected:{table}"
                            )
                        if temporal.get("timestamps_parseable") is not True:
                            blockers.append(
                                f"evaluator_snapshot_{name}_timestamp_parse_failed:{table}"
                            )
                except Exception as exc:
                    verified_integrity[name] = {
                        "error": f"{type(exc).__name__}:{exc}",
                    }
                    blockers.append(f"evaluator_snapshot_{name}_integrity_revalidation_failed")
        expected_pinned_roles = {
            "signal_main_selective_copy",
            "paper_main_selective_copy",
            *{
                str(PARALLEL_PAPER_STAGE_CONFIGS[table]["role"])
                for table in safe_manifest_parallel_stage_tables
            },
            "raw_main_selective_copy",
            "kline_main_selective_copy",
        }
        try:
            pinned_roles = {
                str(view.get("role")) for view in all_pinned_read_views
            }
            pinned_midpoints = [
                float(view.get("pinned_midpoint_epoch"))
                for view in all_pinned_read_views
            ]
            pinned_limits = [
                float(view.get("source_read_lock_limit_sec"))
                for view in all_pinned_read_views
            ]
            recomputed_skew = max(pinned_midpoints) - min(pinned_midpoints)
            manifest_skew = float(manifest.get("cross_database_time_skew_sec"))
            manifest_max_skew = float(
                manifest.get("max_allowed_cross_database_time_skew_sec")
            )
            if (
                len(all_pinned_read_views)
                != 4 + len(safe_manifest_parallel_stage_tables)
                or int(manifest.get("pinned_read_view_count"))
                != 4 + len(safe_manifest_parallel_stage_tables)
                or pinned_roles != expected_pinned_roles
                or len(pinned_roles) != len(all_pinned_read_views)
                or any(
                    not math.isfinite(value)
                    for value in [
                        *pinned_midpoints,
                        *pinned_limits,
                        recomputed_skew,
                        manifest_skew,
                        manifest_max_skew,
                    ]
                )
                or any(limit <= 0 for limit in pinned_limits)
                or (
                    manifest_read_lock_limit is not None
                    and any(
                        abs(limit - manifest_read_lock_limit) > 0.001
                        for limit in pinned_limits
                    )
                )
                or recomputed_skew < 0
                or abs(recomputed_skew - manifest_skew) > 0.001
                or manifest_max_skew < 0
                or recomputed_skew > manifest_max_skew
                or manifest.get("cross_database_time_skew_passed") is not True
            ):
                raise ValueError("pinned read view lineage mismatch")
        except (TypeError, ValueError):
            blockers.append("evaluator_snapshot_pinned_read_view_lineage_invalid")
        if manifest_file.parent.is_dir():
            expected_names = {"manifest.json", *SNAPSHOT_FILES.values()}
            entries = list(manifest_file.parent.iterdir())
            leftovers = [
                item.name
                for item in entries
                if not item.is_file()
                or item.name not in expected_names
                or item.name.startswith(".")
                or item.name.endswith((".tmp", ".partial", "-journal", "-wal", "-shm"))
            ]
            if leftovers:
                blockers.append("evaluator_snapshot_partial_artifacts_present")
            missing_names = expected_names - {item.name for item in entries if item.is_file()}
            if missing_names:
                blockers.append("evaluator_snapshot_bundle_files_missing")
            actual_bundle_size = sum(
                int(item.stat().st_size) for item in entries if item.is_file()
            )
            if output_size_bytes is None or actual_bundle_size != output_size_bytes:
                blockers.append("evaluator_snapshot_bundle_size_mismatch")
            if output_cap_bytes is None or actual_bundle_size > output_cap_bytes:
                blockers.append("evaluator_snapshot_bundle_output_cap_exceeded")
            snapshots_root = manifest_file.parent.parent
            if snapshots_root.name == "snapshots":
                interrupted = [
                    item.name
                    for item in snapshots_root.iterdir()
                    if item.is_dir() and item.name.startswith(".") and item.name.endswith(".partial")
                ]
                if interrupted:
                    blockers.append("evaluator_snapshot_interrupted_partials_present")
    blockers = list(dict.fromkeys(blockers))
    return {
        "schema_version": "evaluator_snapshot_bundle_contract.v1",
        "manifest_path": str(manifest_file),
        "manifest_sha256": manifest_sha256_value,
        "producer_status_path": str(producer_status_file),
        "producer_status_schema_version": (
            producer_status.get("schema_version") if producer_status else None
        ),
        "producer_status": (
            {
                "status": producer_status.get("status"),
                "accepted": producer_status.get("accepted") is True,
                "last_success_at": producer_status.get("last_success_at"),
                "last_failure_at": producer_status.get("last_failure_at"),
                "last_failure_code": producer_status.get("last_failure_code"),
                "last_accepted_snapshot": producer_acceptance,
                "promotion_allowed": False,
            }
            if producer_status_loaded
            else None
        ),
        "snapshot_id": manifest.get("snapshot_id") if manifest else None,
        "snapshot_ts": manifest.get("snapshot_ts") if manifest else None,
        "snapshot_age_sec": (
            round(snapshot_age_sec_value, 6) if snapshot_age_sec_value is not None else None
        ),
        "max_snapshot_age_sec": float(max_age_sec),
        "git_commit": manifest.get("git_commit") if manifest else None,
        "databases": {name: str(path) for name, path in candidates.items()},
        "verified_integrity": verified_integrity,
        "live_databases": {name: str(path) for name, path in live.items()},
        "accepted": not blockers,
        "blockers": blockers,
        "promotion_allowed": False,
    }


def evaluator_snapshot_provenance(status: dict) -> dict:
    """Return a bounded immutable consumer record for one accepted bundle.

    The provenance record is safe to copy into AutoLoop artifacts. It contains
    bundle identity and integrity evidence, never source rows or token-level
    payloads, and it cannot authorize promotion.
    """
    payload = status if isinstance(status, dict) else {}
    databases = payload.get("databases") if isinstance(payload.get("databases"), dict) else {}
    verified = payload.get("verified_integrity") if isinstance(payload.get("verified_integrity"), dict) else {}
    database_evidence = {}
    for name in sorted(SNAPSHOT_FILES):
        integrity = verified.get(name) if isinstance(verified.get(name), dict) else {}
        database_evidence[name] = {
            "path": databases.get(name),
            "sha256": integrity.get("sha256"),
            "sha256_matches_manifest": integrity.get("sha256_matches_manifest") is True,
            "quick_check": integrity.get("quick_check") if isinstance(integrity.get("quick_check"), list) else [],
        }
    return {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "consumer_verified_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "contract_schema_version": payload.get("schema_version"),
        "accepted": payload.get("accepted") is True,
        "snapshot_id": payload.get("snapshot_id"),
        "snapshot_ts": payload.get("snapshot_ts"),
        "snapshot_age_sec": payload.get("snapshot_age_sec"),
        "max_snapshot_age_sec": payload.get("max_snapshot_age_sec"),
        "git_commit": payload.get("git_commit"),
        "manifest_path": payload.get("manifest_path"),
        "manifest_sha256": payload.get("manifest_sha256"),
        "producer_status_path": payload.get("producer_status_path"),
        "producer_status_schema_version": payload.get(
            "producer_status_schema_version"
        ),
        "producer_manifest_sha256": (
            (payload.get("producer_status") or {})
            .get("last_accepted_snapshot", {})
            .get("manifest_sha256")
        ),
        "databases": database_evidence,
        "blockers": [str(value) for value in (payload.get("blockers") or [])],
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def require_evaluator_snapshot_bundle(**kwargs) -> dict:
    status = evaluator_snapshot_bundle_status(**kwargs)
    if not status["accepted"]:
        raise RuntimeError(
            "evaluator_snapshot_bundle_blocked "
            + ",".join(status["blockers"])
            + f" manifest={status['manifest_path']}"
        )
    return status


@contextmanager
def evaluator_snapshot_bundle_lease(*, lock_file: str, lock_timeout_sec: float = 300, **kwargs):
    """Hold a shared lease while one evaluator run uses an immutable bundle."""
    lock_path = Path(lock_file).expanduser().resolve()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+", encoding="utf-8")
    deadline = time.monotonic() + max(0.0, float(lock_timeout_sec))
    try:
        while True:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"evaluator_snapshot_lease_timeout:{lock_path}")
                time.sleep(0.1)
        status = require_evaluator_snapshot_bundle(**kwargs)
        try:
            yield status
        except BaseException:
            post_status = require_evaluator_snapshot_bundle(**kwargs)
            if post_status.get("snapshot_id") != status.get("snapshot_id"):
                raise RuntimeError("evaluator_snapshot_changed_during_lease")
            raise
        else:
            post_status = require_evaluator_snapshot_bundle(**kwargs)
            if post_status.get("snapshot_id") != status.get("snapshot_id"):
                raise RuntimeError("evaluator_snapshot_changed_during_lease")
    finally:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signal-db", required=True)
    parser.add_argument("--paper-db", required=True)
    parser.add_argument("--raw-db", required=True)
    parser.add_argument("--kline-db", required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--producer-status-path")
    parser.add_argument("--max-age-sec", type=float, default=28800)
    parser.add_argument("--live-signal-db")
    parser.add_argument("--live-paper-db")
    parser.add_argument("--live-raw-db")
    parser.add_argument("--live-kline-db")
    args = parser.parse_args()
    try:
        status = evaluator_snapshot_bundle_status(
            signal_db=args.signal_db,
            paper_db=args.paper_db,
            raw_db=args.raw_db,
            kline_db=args.kline_db,
            data_dir=args.data_dir,
            manifest_path=args.manifest_path,
            producer_status_path=args.producer_status_path,
            max_age_sec=args.max_age_sec,
            live_databases={
                name: value
                for name, value in {
                    "signal": args.live_signal_db,
                    "paper": args.live_paper_db,
                    "raw": args.live_raw_db,
                    "kline": args.live_kline_db,
                }.items()
                if value
            },
        )
    except Exception as exc:
        status = {
            "schema_version": "evaluator_snapshot_bundle_contract.v1",
            "accepted": False,
            "blockers": [f"evaluator_snapshot_preflight_exception:{type(exc).__name__}:{exc}"],
            "promotion_allowed": False,
        }
    print(json.dumps(status, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
