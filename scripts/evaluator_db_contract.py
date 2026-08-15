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
import re
import sqlite3
import stat
import time
from urllib.parse import quote

from evaluator_evidence_schema import (
    EVIDENCE_SCHEMA_SHA256,
    EVIDENCE_SCHEMA_VERSION,
    is_json_finite_number,
    is_json_safe_integer,
    numeric_evidence_schema_binding_valid,
    numeric_evidence_schema_valid,
)

from cross_db_evaluator_snapshot import (
    CANDIDATE_OBSERVATION_ROW_TABLE,
    CANDIDATE_STAGE_BUDGET_MODE,
    DATABASE_SPECS,
    MIN_CANDIDATE_STAGE_CAP_BYTES,
    MIN_PARALLEL_PAPER_STAGE_CAP_BYTES,
    PARALLEL_PAPER_STAGE_BULK_PAGE_MIN_BUDGET_BYTES,
    PARALLEL_PAPER_STAGE_BULK_PAGE_SIZE,
    PARALLEL_PAPER_STAGE_CHUNK_TARGET_BYTES,
    PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION,
    PARALLEL_PAPER_STAGE_COMPRESSION,
    PARALLEL_PAPER_OPTIONAL_STAGE_TABLES,
    PARALLEL_PAPER_REQUIRED_STAGE_TABLES,
    PARALLEL_PAPER_STAGE_CONFIGS,
    PARALLEL_PAPER_STAGE_PAGE_SIZES,
    PARALLEL_PAPER_STAGE_SCHEMA_VERSION,
    PARALLEL_PAPER_STAGE_STORAGE_MODE,
    PARALLEL_PAPER_STAGE_TABLES,
    PAPER_DECISION_STAGE_TABLE,
    SHARED_STAGE_ADVISORY_FORMULA,
    SHARED_STAGE_ADVISORY_INDEX_OVERHEAD_BYTES,
    SHARED_STAGE_ADVISORY_ROOT_RESERVE_PAGES,
    SHARED_STAGE_ADVISORY_ROW_OVERHEAD_BYTES,
    SHARED_STAGE_ADVISORY_SCHEMA_VERSION,
    SHARED_STAGE_BUDGET_ALLOCATION_MODE,
    SHARED_STAGE_BUDGET_SCHEMA_VERSION,
    SHARED_STAGE_DBSTAT_ADVISORY_TIMEOUT_SEC,
    SHARED_STAGE_ESTIMATE_SAMPLE_ROWS,
    SHARED_STAGE_ESTIMATE_TIMEOUT_SEC,
    SHARED_STAGE_HASH_CANONICALIZATION,
    SHARED_STAGE_HISTORY_ANCHOR_SCHEMA_VERSION,
    SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_FORMULA,
    SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_SCHEMA_VERSION,
    SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_STRATEGY,
    SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ROW_BINDING_MODE,
    SHARED_STAGE_INDEXED_COUNT_TIMEOUT_SEC,
    SHARED_STAGE_PAGE_SIZE,
    SHARED_STAGE_SAMPLE_ADVISORY_FORMULA,
    SHARED_STAGE_SAMPLE_ADVISORY_SCHEMA_VERSION,
    SHARED_STAGE_SAMPLE_ADVISORY_STRATEGY,
    SHARED_STAGE_TARGET_CANDIDATE,
    allocate_shared_stage_residual,
    compressed_stage_storage_contract_sha256,
    parallel_paper_stage_inventory_valid,
    read_json_object,
    normalized_timestamp_sql,
    quote_identifier,
    shared_stage_budget_evidence_sha256,
    shared_stage_budget_anchor_path,
    shared_stage_budget_plan_sha256,
    shared_stage_advisory_demand,
    shared_stage_sample_advisory_demand,
    shared_stage_history_required_bytes,
    shared_stage_target_filename,
    shared_stage_target_minimum_bytes,
    shared_stage_target_names,
    shared_stage_target_storage_schema_version,
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


def json_safe_integer(value: object, *, field: str) -> int:
    if not is_json_safe_integer(value):
        raise ValueError(f"{field} must be a JSON safe integer")
    return int(value)


def json_finite_number(value: object, *, field: str) -> float:
    if not is_json_finite_number(value):
        raise ValueError(f"{field} must be a finite JSON number")
    return float(value)


def json_numeric_evidence_contract_sha256() -> str:
    return EVIDENCE_SCHEMA_SHA256


def json_numeric_evidence_types_valid(payload: object) -> bool:
    """Validate all numeric evidence using the shared declarative schema."""

    return numeric_evidence_schema_valid(payload)


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


def sqlite_table_row_counts(
    path: Path,
    tables: tuple[str, ...] | list[str],
) -> dict[str, int]:
    uri = f"file:{quote(str(path.resolve()), safe='/')}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    counts: dict[str, int] = {}
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        for table in tables:
            row = connection.execute(
                "SELECT type FROM sqlite_master WHERE name=? "
                "AND type IN ('table','view')",
                (table,),
            ).fetchone()
            if row is None:
                continue
            counts[str(table)] = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {quote_identifier(table)}"
                ).fetchone()[0]
            )
    finally:
        connection.close()
    return counts


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


def validate_shared_stage_sample_estimate_contract(
    target: str,
    report: dict,
    evidence: dict,
) -> int:
    """Validate the indexed-sample fallback as an allocation hint only."""

    def nonnegative_int(name: str) -> int:
        try:
            value = json_safe_integer(
                evidence.get(name),
                field=f"{target}.advisory_evidence.{name}",
            )
        except ValueError:
            raise ValueError(
                f"shared stage sample advisory numeric invalid:{target}:{name}"
            ) from None
        if value < 0:
            raise ValueError(
                f"shared stage sample advisory numeric invalid:{target}:{name}"
            )
        return value

    indexed_count_timeout_contract = bool(
        evidence.get("advisory_schema_version")
        == SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_SCHEMA_VERSION
        or evidence.get("advisory_formula")
        == SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_FORMULA
    )
    if indexed_count_timeout_contract:
        if evidence.get("selected_row_count") is not None:
            raise ValueError(
                f"shared stage indexed count advisory row claim invalid:{target}"
            )
        selected_rows = nonnegative_int("sample_row_count_advisory_basis")
    else:
        selected_rows = nonnegative_int("selected_row_count")
    sample_limit = nonnegative_int("sample_limit_rows")
    sample_rows = nonnegative_int("sample_rows")
    sample_basis = nonnegative_int("sample_row_bytes_basis")
    pinned_read_view_id = str(evidence.get("pinned_read_view_id") or "")
    pinned_read_view_role = str(evidence.get("pinned_read_view_role") or "")
    dbstat_timeout = json_finite_number(
        evidence.get("dbstat_timeout_sec"),
        field=f"{target}.advisory_evidence.dbstat_timeout_sec",
    )
    dbstat_elapsed = json_finite_number(
        evidence.get("dbstat_elapsed_sec"),
        field=f"{target}.advisory_evidence.dbstat_elapsed_sec",
    )
    indexed_count_timeout = evidence.get("indexed_count_timeout_sec")
    indexed_count_elapsed = evidence.get("indexed_count_elapsed_sec")
    expected_schema = (
        SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_SCHEMA_VERSION
        if indexed_count_timeout_contract
        else SHARED_STAGE_SAMPLE_ADVISORY_SCHEMA_VERSION
    )
    expected_formula = (
        SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_FORMULA
        if indexed_count_timeout_contract
        else SHARED_STAGE_SAMPLE_ADVISORY_FORMULA
    )
    expected_strategy = (
        SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_STRATEGY
        if indexed_count_timeout_contract
        else SHARED_STAGE_SAMPLE_ADVISORY_STRATEGY
    )
    expected_binding_mode = (
        SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ROW_BINDING_MODE
        if indexed_count_timeout_contract
        else "exact_selected_rows"
    )
    expected_row_upper_basis = (
        "unavailable_after_bounded_index_count_timeout"
        if indexed_count_timeout_contract
        else "not_required_for_bounded_index_sample_advisory"
    )
    indexed_count_row_basis_invalid = bool(
        indexed_count_timeout_contract and selected_rows != sample_rows
    )
    if (
        evidence.get("source_measurement_trust_boundary")
        != "same_pinned_read_view_as_copy"
        or not re.fullmatch(r"[a-f0-9]{32}", pinned_read_view_id)
        or not pinned_read_view_role
        or evidence.get("estimate_started_after_pin") is not True
        or evidence.get("estimate_completed_before_copy") is not True
        or evidence.get("query_bounded") is not True
        or evidence.get("physical_upper_bound_claimed") is not False
        or evidence.get("advisory_schema_version") != expected_schema
        or evidence.get("advisory_formula") != expected_formula
        or report.get("advisory_strategy") != expected_strategy
        or evidence.get("capacity_sample_used") is not True
        or dbstat_timeout != SHARED_STAGE_DBSTAT_ADVISORY_TIMEOUT_SEC
        or dbstat_elapsed < 0
        or evidence.get("row_count_binding_mode") != expected_binding_mode
        or evidence.get("source_row_count_upper") is not None
        or evidence.get("source_row_count_upper_basis")
        != expected_row_upper_basis
        or evidence.get("advisory_row_overhead_bytes")
        != SHARED_STAGE_ADVISORY_ROW_OVERHEAD_BYTES
        or evidence.get("advisory_index_overhead_bytes")
        != SHARED_STAGE_ADVISORY_INDEX_OVERHEAD_BYTES
        or evidence.get("advisory_root_reserve_pages")
        != SHARED_STAGE_ADVISORY_ROOT_RESERVE_PAGES
        or sample_limit != SHARED_STAGE_ESTIMATE_SAMPLE_ROWS
        or sample_rows > sample_limit
        or indexed_count_row_basis_invalid
        or evidence.get("source_row_fraction_numerator") is not None
        or evidence.get("source_row_fraction_denominator") is not None
    ):
        raise ValueError(f"shared stage sample advisory evidence invalid:{target}")

    if indexed_count_timeout_contract:
        indexed_count_timeout = json_finite_number(
            indexed_count_timeout,
            field=f"{target}.advisory_evidence.indexed_count_timeout_sec",
        )
        indexed_count_elapsed = json_finite_number(
            indexed_count_elapsed,
            field=f"{target}.advisory_evidence.indexed_count_elapsed_sec",
        )
        if (
            evidence.get("indexed_count_completed") is not False
            or evidence.get("indexed_count_timed_out") is not True
            or indexed_count_timeout
            != SHARED_STAGE_INDEXED_COUNT_TIMEOUT_SEC
            or indexed_count_elapsed + 0.000001 < indexed_count_timeout
            or indexed_count_elapsed >= SHARED_STAGE_ESTIMATE_TIMEOUT_SEC
            or evidence.get("dbstat_completed") is not False
            or evidence.get("dbstat_timed_out") is not False
            or dbstat_elapsed != 0.0
            or evidence.get("dbstat_skipped_reason")
            != "indexed_count_timeout"
        ):
            raise ValueError(
                f"shared stage indexed count timeout evidence invalid:{target}"
            )
    elif (
        evidence.get("dbstat_completed") is not False
        or evidence.get("dbstat_timed_out") is not True
        or dbstat_elapsed + 0.000001 < dbstat_timeout
    ):
        raise ValueError(f"shared stage sample dbstat evidence invalid:{target}")

    source_dbstat_fields = (
        "source_dbstat_page_count",
        "source_dbstat_page_size",
        "source_dbstat_physical_bytes",
        "source_dbstat_payload_bytes",
        "source_dbstat_unused_bytes",
        "source_dbstat_max_payload_bytes",
        "source_dbstat_cell_upper_count",
    )
    if any(evidence.get(field) is not None for field in source_dbstat_fields):
        raise ValueError(f"unexpected source dbstat evidence:{target}")

    average_diagnostic = evidence.get("average_row_bytes_diagnostic")
    max_diagnostic = evidence.get("sample_max_row_bytes_diagnostic")
    if average_diagnostic is not None:
        try:
            average_diagnostic = json_finite_number(
                average_diagnostic,
                field=(
                    f"{target}.advisory_evidence."
                    "average_row_bytes_diagnostic"
                ),
            )
        except ValueError:
            raise ValueError(
                f"shared stage sample diagnostic invalid:{target}:average"
            ) from None
        if average_diagnostic < 0:
            raise ValueError(
                f"shared stage sample diagnostic invalid:{target}:average"
            )
    if max_diagnostic is not None:
        try:
            max_diagnostic = json_safe_integer(
                max_diagnostic,
                field=(
                    f"{target}.advisory_evidence."
                    "sample_max_row_bytes_diagnostic"
                ),
            )
        except ValueError:
            raise ValueError(
                f"shared stage sample diagnostic invalid:{target}:maximum"
            ) from None
        if max_diagnostic < 0:
            raise ValueError(
                f"shared stage sample diagnostic invalid:{target}:maximum"
            )
    if selected_rows > 0:
        if (
            sample_rows <= 0
            or sample_basis <= 0
            or max_diagnostic != sample_basis
            or average_diagnostic is None
            or average_diagnostic <= 0
        ):
            raise ValueError(f"shared stage sample basis invalid:{target}")
    elif (
        sample_rows != 0
        or sample_basis != 0
        or average_diagnostic is not None
        or max_diagnostic is not None
    ):
        raise ValueError(f"shared stage empty sample invalid:{target}")

    source_index_name = evidence.get("source_index_name")
    query_plan = evidence.get("source_query_plan")
    if (
        not isinstance(source_index_name, str)
        or not source_index_name
        or not isinstance(query_plan, list)
        or not query_plan
        or evidence.get("source_query_plan_uses_index") is not True
        or evidence.get("source_query_plan_uses_range_search") is not True
        or evidence.get("source_query_plan_full_table_scan_detected") is not False
    ):
        raise ValueError(f"shared stage sample index evidence invalid:{target}")

    candidate_order_dbstat_fields = (
        "candidate_order_source_index_dbstat_page_count",
        "candidate_order_source_index_dbstat_page_size",
        "candidate_order_source_index_dbstat_physical_bytes",
        "candidate_order_source_index_dbstat_payload_bytes",
        "candidate_order_source_index_dbstat_unused_bytes",
        "candidate_order_source_index_dbstat_max_payload_bytes",
        "candidate_order_source_index_dbstat_cell_upper_count",
        "candidate_order_source_index_structural_overhead_bytes",
    )
    if any(
        evidence.get(field) is not None
        for field in candidate_order_dbstat_fields
    ):
        raise ValueError(f"unexpected candidate dbstat evidence:{target}")
    if target == SHARED_STAGE_TARGET_CANDIDATE:
        if (
            not isinstance(
                evidence.get("candidate_order_source_index_name"),
                str,
            )
            or not evidence.get("candidate_order_source_index_name")
            or evidence.get("candidate_order_source_index_columns")
            != ["signal_id"]
            or evidence.get("candidate_order_source_index_partial") is not False
        ):
            raise ValueError("candidate signal-order sample index evidence invalid")
    else:
        candidate_identity_fields = (
            "candidate_order_source_index_name",
            "candidate_order_source_index_columns",
            "candidate_order_source_index_partial",
        )
        if any(
            evidence.get(field) not in (None, [], {})
            for field in candidate_identity_fields
        ):
            raise ValueError("unexpected candidate sample index evidence")

    expected = shared_stage_sample_advisory_demand(
        target=target,
        selected_row_count=selected_rows,
        sample_rows=sample_rows,
        sample_max_row_bytes=max_diagnostic,
    )
    for field, expected_value in expected.items():
        if nonnegative_int(field) != int(expected_value):
            raise ValueError(
                f"shared stage sample advisory mismatch:{target}:{field}"
            )
    if json_safe_integer(
        report.get("advisory_required_bytes"),
        field=f"{target}.advisory_required_bytes",
    ) != int(expected["advisory_required_bytes"]):
        raise ValueError(f"shared stage sample advisory total mismatch:{target}")
    return int(expected["advisory_required_bytes"])


def validate_shared_stage_estimate_contract(
    target: str,
    report: dict,
) -> int:
    """Validate advisory demand without treating it as a capacity proof."""
    evidence = report.get("advisory_evidence")
    if not isinstance(evidence, dict):
        raise ValueError(f"shared stage advisory evidence missing:{target}")
    if (
        evidence.get("advisory_schema_version")
        == SHARED_STAGE_SAMPLE_ADVISORY_SCHEMA_VERSION
        or evidence.get("advisory_formula")
        == SHARED_STAGE_SAMPLE_ADVISORY_FORMULA
        or evidence.get("advisory_schema_version")
        == SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_SCHEMA_VERSION
        or evidence.get("advisory_formula")
        == SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ADVISORY_FORMULA
    ):
        return validate_shared_stage_sample_estimate_contract(
            target,
            report,
            evidence,
        )

    def nonnegative_int(name: str) -> int:
        try:
            value = json_safe_integer(
                evidence.get(name),
                field=f"{target}.advisory_evidence.{name}",
            )
        except ValueError:
            raise ValueError(
                f"shared stage advisory numeric invalid:{target}:{name}"
            ) from None
        if value < 0:
            raise ValueError(
                f"shared stage advisory numeric invalid:{target}:{name}"
            )
        return value

    selected_rows = nonnegative_int("selected_row_count")
    source_rows_upper = nonnegative_int("source_row_count_upper")
    page_count = nonnegative_int("source_dbstat_page_count")
    page_size = nonnegative_int("source_dbstat_page_size")
    physical_bytes = nonnegative_int("source_dbstat_physical_bytes")
    payload_bytes = nonnegative_int("source_dbstat_payload_bytes")
    unused_bytes = nonnegative_int("source_dbstat_unused_bytes")
    max_payload_bytes = nonnegative_int("source_dbstat_max_payload_bytes")
    cell_upper_count = nonnegative_int("source_dbstat_cell_upper_count")
    sample_limit = nonnegative_int("sample_limit_rows")
    sample_rows = nonnegative_int("sample_rows")
    pinned_read_view_id = str(evidence.get("pinned_read_view_id") or "")
    pinned_read_view_role = str(evidence.get("pinned_read_view_role") or "")
    dbstat_timeout = json_finite_number(
        evidence.get("dbstat_timeout_sec"),
        field=f"{target}.advisory_evidence.dbstat_timeout_sec",
    )
    dbstat_elapsed = json_finite_number(
        evidence.get("dbstat_elapsed_sec"),
        field=f"{target}.advisory_evidence.dbstat_elapsed_sec",
    )

    if (
        evidence.get("source_measurement_trust_boundary")
        != "same_pinned_read_view_as_copy"
        or not re.fullmatch(r"[a-f0-9]{32}", pinned_read_view_id)
        or not pinned_read_view_role
        or evidence.get("estimate_started_after_pin") is not True
        or evidence.get("estimate_completed_before_copy") is not True
        or evidence.get("query_bounded") is not True
        or evidence.get("physical_upper_bound_claimed") is not False
        or evidence.get("advisory_schema_version")
        != SHARED_STAGE_ADVISORY_SCHEMA_VERSION
        or evidence.get("advisory_formula") != SHARED_STAGE_ADVISORY_FORMULA
        or evidence.get("capacity_sample_used") is not False
        or evidence.get("dbstat_completed") is not True
        or evidence.get("dbstat_timed_out") is not False
        or dbstat_timeout != SHARED_STAGE_DBSTAT_ADVISORY_TIMEOUT_SEC
        or dbstat_elapsed < 0
        or evidence.get("sample_row_bytes_basis") is not None
        or evidence.get("table_sample_payload_advisory_bytes") is not None
        or evidence.get("advisory_row_overhead_bytes")
        != SHARED_STAGE_ADVISORY_ROW_OVERHEAD_BYTES
        or evidence.get("advisory_index_overhead_bytes")
        != SHARED_STAGE_ADVISORY_INDEX_OVERHEAD_BYTES
        or evidence.get("advisory_root_reserve_pages")
        != SHARED_STAGE_ADVISORY_ROOT_RESERVE_PAGES
        or sample_limit != SHARED_STAGE_ESTIMATE_SAMPLE_ROWS
        or sample_rows > sample_limit
        or page_count <= 0
        or page_size < 512
        or page_size > 65536
        or page_size & (page_size - 1)
        or physical_bytes != page_count * page_size
        or payload_bytes > physical_bytes
        or unused_bytes > physical_bytes
        or max_payload_bytes > payload_bytes
        or selected_rows > source_rows_upper
    ):
        raise ValueError(f"shared stage advisory evidence invalid:{target}")

    average_diagnostic = evidence.get("average_row_bytes_diagnostic")
    max_diagnostic = evidence.get("sample_max_row_bytes_diagnostic")
    if average_diagnostic is not None:
        try:
            average_diagnostic = json_finite_number(
                average_diagnostic,
                field=(
                    f"{target}.advisory_evidence."
                    "average_row_bytes_diagnostic"
                ),
            )
        except ValueError:
            raise ValueError(
                f"shared stage advisory diagnostic invalid:{target}:average"
            ) from None
        if average_diagnostic < 0:
            raise ValueError(
                f"shared stage advisory diagnostic invalid:{target}:average"
            )
    if max_diagnostic is not None:
        try:
            max_diagnostic = json_safe_integer(
                max_diagnostic,
                field=(
                    f"{target}.advisory_evidence."
                    "sample_max_row_bytes_diagnostic"
                ),
            )
        except ValueError:
            raise ValueError(
                f"shared stage advisory diagnostic invalid:{target}:maximum"
            ) from None
        if max_diagnostic < 0:
            raise ValueError(
                f"shared stage advisory diagnostic invalid:{target}:maximum"
            )

    candidate_order_index_storage: dict[str, int] | None = None
    if target == SHARED_STAGE_TARGET_CANDIDATE:
        candidate_order_name = evidence.get("candidate_order_source_index_name")
        candidate_order_columns = evidence.get(
            "candidate_order_source_index_columns"
        )
        candidate_order_page_count = nonnegative_int(
            "candidate_order_source_index_dbstat_page_count"
        )
        candidate_order_page_size = nonnegative_int(
            "candidate_order_source_index_dbstat_page_size"
        )
        candidate_order_physical = nonnegative_int(
            "candidate_order_source_index_dbstat_physical_bytes"
        )
        candidate_order_payload = nonnegative_int(
            "candidate_order_source_index_dbstat_payload_bytes"
        )
        candidate_order_unused = nonnegative_int(
            "candidate_order_source_index_dbstat_unused_bytes"
        )
        candidate_order_max_payload = nonnegative_int(
            "candidate_order_source_index_dbstat_max_payload_bytes"
        )
        candidate_order_cells = nonnegative_int(
            "candidate_order_source_index_dbstat_cell_upper_count"
        )
        candidate_order_structural = nonnegative_int(
            "candidate_order_source_index_structural_overhead_bytes"
        )
        if (
            not isinstance(candidate_order_name, str)
            or not candidate_order_name
            or candidate_order_columns != ["signal_id"]
            or evidence.get("candidate_order_source_index_partial") is not False
            or candidate_order_page_count <= 0
            or candidate_order_page_size < 512
            or candidate_order_page_size > 65536
            or candidate_order_page_size & (candidate_order_page_size - 1)
            or candidate_order_physical
            != candidate_order_page_count * candidate_order_page_size
            or candidate_order_payload > candidate_order_physical
            or candidate_order_unused > candidate_order_physical
            or candidate_order_max_payload > candidate_order_payload
            or candidate_order_structural
            != candidate_order_physical - candidate_order_payload
            or candidate_order_cells != source_rows_upper
            or source_rows_upper > cell_upper_count
            or evidence.get("source_row_count_upper_basis")
            != "exact_signal_index_entry_count"
        ):
            raise ValueError("candidate signal-order index evidence invalid")
        candidate_order_index_storage = {
            "page_count": candidate_order_page_count,
            "page_size": candidate_order_page_size,
            "physical_bytes": candidate_order_physical,
            "payload_bytes": candidate_order_payload,
            "unused_bytes": candidate_order_unused,
            "max_payload_bytes": candidate_order_max_payload,
            "cell_upper_count": candidate_order_cells,
            "structural_overhead_bytes": candidate_order_structural,
        }
    else:
        if (
            source_rows_upper != cell_upper_count
            or evidence.get("source_row_count_upper_basis")
            != "table_dbstat_cell_upper"
        ):
            raise ValueError("shared stage source-row upper invalid")
        candidate_order_fields = (
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
        )
        if any(
            evidence.get(field) not in (None, [], {})
            for field in candidate_order_fields
        ):
            raise ValueError("unexpected candidate index evidence")

    source_index_name = evidence.get("source_index_name")
    query_plan = evidence.get("source_query_plan")
    if source_index_name:
        if (
            report.get("advisory_strategy")
            != "dbstat_proportional_advisory_with_indexed_row_count"
            or not isinstance(source_index_name, str)
            or not isinstance(query_plan, list)
            or not query_plan
            or evidence.get("source_query_plan_uses_index") is not True
            or evidence.get("source_query_plan_uses_range_search") is not True
            or evidence.get("source_query_plan_full_table_scan_detected")
            is not False
        ):
            raise ValueError(f"shared stage indexed advisory invalid:{target}")
    else:
        if (
            report.get("advisory_strategy")
            != "dbstat_full_btree_advisory_demand"
            or selected_rows != source_rows_upper
            or query_plan not in ([], None)
            or evidence.get("source_query_plan_uses_index") is not None
            or evidence.get("source_query_plan_uses_range_search") is not None
            or evidence.get("source_query_plan_full_table_scan_detected")
            is not None
        ):
            raise ValueError(f"shared stage full-btree advisory invalid:{target}")

    expected = shared_stage_advisory_demand(
        target=target,
        selected_row_count=selected_rows,
        source_row_count_upper=source_rows_upper,
        storage={
            "page_count": page_count,
            "page_size": page_size,
            "physical_bytes": physical_bytes,
            "payload_bytes": payload_bytes,
            "unused_bytes": unused_bytes,
            "max_payload_bytes": max_payload_bytes,
            "cell_upper_count": cell_upper_count,
            "structural_overhead_bytes": physical_bytes - payload_bytes,
        },
        candidate_order_index_storage=candidate_order_index_storage,
    )
    for field, expected_value in expected.items():
        if nonnegative_int(field) != int(expected_value):
            raise ValueError(f"shared stage advisory mismatch:{target}:{field}")
    if json_safe_integer(
        report.get("advisory_required_bytes"),
        field=f"{target}.advisory_required_bytes",
    ) != int(expected["advisory_required_bytes"]):
        raise ValueError(f"shared stage advisory total mismatch:{target}")
    return int(expected["advisory_required_bytes"])


def validate_shared_stage_budget_contract(
    manifest: dict,
    disk_preflight: dict,
    active_stage_tables: tuple[str, ...],
    trusted_history_anchor: dict | None,
) -> None:
    shared = manifest.get("shared_stage_budget")
    disk_shared = disk_preflight.get("shared_stage_budget")
    if not isinstance(shared, dict) or not isinstance(disk_shared, dict):
        raise ValueError("shared stage budget missing")
    if shared != disk_shared:
        raise ValueError("shared stage budget copies diverged")

    def contract_int(value: object, field: str) -> int:
        return json_safe_integer(value, field=f"shared_stage.{field}")

    def contract_number(value: object, field: str) -> float:
        return json_finite_number(value, field=f"shared_stage.{field}")

    if (
        shared.get("schema_version") != SHARED_STAGE_BUDGET_SCHEMA_VERSION
        or shared.get("allocation_mode")
        != SHARED_STAGE_BUDGET_ALLOCATION_MODE
        or shared.get("hash_canonicalization")
        != SHARED_STAGE_HASH_CANONICALIZATION
        or shared.get("accepted") is not True
        or shared.get("capacity_sufficient") is not True
        or shared.get("all_advisory_queries_bounded") is not True
        or shared.get("physical_upper_bound_claimed") is not False
        or shared.get("global_hard_cap_enforced") is not True
        or shared.get("per_target_max_page_count_enforced") is not True
        or shared.get("capacity_sufficient_basis")
        != "minimum_and_verified_history_high_water"
        or shared.get("pinned_read_view_binding_required") is not True
        or shared.get(
            "all_advisory_estimates_pinned_read_view_bound"
        )
        is not True
        or manifest.get(
            "shared_stage_estimates_bound_to_copy_read_views"
        )
        is not True
        or shared.get("fixed_percentage_allocation_used") is not False
        or str(shared.get("attempt_id") or "")
        != str(manifest.get("snapshot_id") or "")
        or manifest.get("shared_stage_budget_passed") is not True
    ):
        raise ValueError("shared stage budget contract invalid")
    legacy_share_fields = {
        "candidate_stage_residual_share",
        "parallel_paper_stage_residual_shares",
        "parallel_paper_stage_active_weight_total",
        "candidate_stage_normalized_share",
        "parallel_paper_stage_normalized_shares",
        "paper_decision_stage_residual_share",
    }
    if any(field in disk_preflight for field in legacy_share_fields):
        raise ValueError("legacy fixed stage allocation evidence present")
    expected_targets = shared_stage_target_names(active_stage_tables)
    expected_omitted_tables = tuple(
        table
        for table in PARALLEL_PAPER_OPTIONAL_STAGE_TABLES
        if table not in active_stage_tables
    )
    if (
        tuple(disk_preflight.get("parallel_paper_stage_tables") or ())
        != active_stage_tables
        or tuple(
            disk_preflight.get("configured_parallel_paper_stage_tables")
            or ()
        )
        != tuple(PARALLEL_PAPER_STAGE_TABLES)
        or tuple(
            disk_preflight.get(
                "omitted_optional_parallel_paper_stage_tables"
            )
            or ()
        )
        != expected_omitted_tables
        or contract_int(
            disk_preflight.get("candidate_stage_minimum_cap_bytes"),
            "disk.candidate_stage_minimum_cap_bytes",
        )
        != MIN_CANDIDATE_STAGE_CAP_BYTES
        or contract_int(
            disk_preflight.get("parallel_paper_stage_minimum_cap_bytes"),
            "disk.parallel_paper_stage_minimum_cap_bytes",
        )
        != MIN_PARALLEL_PAPER_STAGE_CAP_BYTES
    ):
        raise ValueError("shared stage disk inventory invalid")
    raw_targets = shared.get("targets")
    if (
        tuple(shared.get("active_targets") or ()) != expected_targets
        or not isinstance(raw_targets, dict)
        or set(raw_targets) != set(expected_targets)
    ):
        raise ValueError("shared stage target inventory invalid")
    disk_free = contract_int(disk_preflight.get("free_bytes"), "disk.free_bytes")
    output_cap = contract_int(
        disk_preflight.get("selective_snapshot_output_cap_bytes"),
        "disk.selective_snapshot_output_cap_bytes",
    )
    reserve = contract_int(
        disk_preflight.get("required_reserve_bytes"),
        "disk.required_reserve_bytes",
    )
    raw_cap = max(0, disk_free - output_cap - reserve)
    aligned_cap = raw_cap // SHARED_STAGE_PAGE_SIZE * SHARED_STAGE_PAGE_SIZE
    alignment_reserve = raw_cap - aligned_cap
    total_cap = contract_int(shared.get("total_cap_bytes"), "total_cap_bytes")
    total_granted = contract_int(
        shared.get("total_granted_bytes"),
        "total_granted_bytes",
    )
    actual_total = contract_int(
        shared.get("actual_total_bytes"),
        "actual_total_bytes",
    )
    unconsumed = contract_int(
        shared.get("unconsumed_bytes"),
        "unconsumed_bytes",
    )
    if (
        contract_int(
            disk_preflight.get("temporary_stage_raw_cap_bytes"),
            "disk.temporary_stage_raw_cap_bytes",
        ) != raw_cap
        or contract_int(
            disk_preflight.get("temporary_stage_alignment_reserve_bytes"),
            "disk.temporary_stage_alignment_reserve_bytes",
        )
        != alignment_reserve
        or contract_int(
            disk_preflight.get("temporary_stage_total_cap_bytes"),
            "disk.temporary_stage_total_cap_bytes",
        )
        != aligned_cap
        or total_cap != aligned_cap
        or total_granted != total_cap
        or shared.get("grants_sum_matches_total_cap") is not True
        or actual_total < 0
        or actual_total > total_cap
        or unconsumed != total_cap - actual_total
        or shared.get("all_targets_within_grant") is not True
        or shared.get("all_target_row_counts_bound_to_snapshot") is not True
        or shared.get("cleanup_completed") is not True
        or shared.get("stage_files_removed") is not True
        or shared.get("no_unregistered_stage_files") is not True
        or shared.get("unregistered_stage_files") not in ([], None)
        or not valid_sha256_hex(shared.get("plan_sha256"))
        or shared_stage_budget_plan_sha256(shared)
        != str(shared.get("plan_sha256"))
        or not valid_sha256_hex(shared.get("evidence_sha256"))
        or shared_stage_budget_evidence_sha256(shared)
        != str(shared.get("evidence_sha256"))
    ):
        raise ValueError("shared stage totals invalid")
    borrowing_priority = shared.get("borrowing_priority_targets")
    if (
        not isinstance(borrowing_priority, list)
        or len(borrowing_priority) != len(set(map(str, borrowing_priority)))
        or any(str(target) not in expected_targets for target in borrowing_priority)
    ):
        raise ValueError("shared stage borrowing priority invalid")
    grants: dict[str, int] = {}
    actuals: dict[str, int] = {}
    baselines: dict[str, int] = {}
    advisories: dict[str, int] = {}
    history_grants: dict[str, int] = {}
    allocation_weights: dict[str, int] = {}
    borrowed_total = 0
    advisory_exceeded_targets: list[str] = []
    cap_hit_targets: list[str] = []
    for target in expected_targets:
        report = raw_targets.get(target)
        if not isinstance(report, dict):
            raise ValueError(f"shared stage target missing:{target}")
        minimum = shared_stage_target_minimum_bytes(target)
        validated_advisory = validate_shared_stage_estimate_contract(
            target,
            report,
        )
        advisory = contract_int(
            report.get("advisory_required_bytes"),
            f"targets.{target}.advisory_required_bytes",
        )
        baseline = contract_int(
            report.get("baseline_required_bytes"),
            f"targets.{target}.baseline_required_bytes",
        )
        grant = contract_int(
            report.get("granted_cap_bytes"),
            f"targets.{target}.granted_cap_bytes",
        )
        actual = contract_int(
            report.get("actual_usage_bytes"),
            f"targets.{target}.actual_usage_bytes",
        )
        high_water = contract_int(
            report.get("high_water_bytes"),
            f"targets.{target}.high_water_bytes",
        )
        actual_rows_copied = contract_int(
            report.get("actual_rows_copied"),
            f"targets.{target}.actual_rows_copied",
        )
        borrowed = contract_int(
            report.get("borrowed_shared_pool_bytes"),
            f"targets.{target}.borrowed_shared_pool_bytes",
        )
        allocation_weight = contract_int(
            report.get("allocation_weight_bytes"),
            f"targets.{target}.allocation_weight_bytes",
        )
        advisory_shortfall = contract_int(
            report.get("advisory_shortfall_bytes"),
            f"targets.{target}.advisory_shortfall_bytes",
        )
        history_high_water = contract_int(
            report.get("history_high_water_bytes"),
            f"targets.{target}.history_high_water_bytes",
        )
        history_grant = contract_int(
            report.get("history_granted_cap_bytes"),
            f"targets.{target}.history_granted_cap_bytes",
        )
        history_cap_hit = report.get("history_cap_hit") is True
        history_copy_completed = report.get("history_copy_completed") is True
        history_state = str(report.get("history_state") or "")
        storage_schema_version = str(
            report.get("storage_schema_version") or ""
        )
        history_storage_schema_version = str(
            report.get("history_storage_schema_version") or ""
        )
        history_storage_compatible = (
            report.get("history_storage_compatible") is True
        )
        previous = (
            {
                "high_water_bytes": history_high_water,
                "granted_cap_bytes": history_grant,
                "cap_hit": history_cap_hit,
                "copy_completed": history_copy_completed,
            }
            if history_state != "none"
            else {}
        )
        expected_baseline, expected_history_state = (
            shared_stage_history_required_bytes(target, previous)
        )
        advisory_evidence = report.get("advisory_evidence") or {}
        row_count_binding_mode = str(
            advisory_evidence.get("row_count_binding_mode") or ""
        )
        raw_advisory_rows = advisory_evidence.get("selected_row_count")
        advisory_rows = (
            contract_int(
                raw_advisory_rows,
                f"targets.{target}.advisory_evidence.selected_row_count",
            )
            if raw_advisory_rows is not None
            else None
        )
        row_count_bound = bool(
            (
                row_count_binding_mode == "exact_selected_rows"
                and advisory_rows is not None
                and actual_rows_copied == advisory_rows
            )
            or (
                row_count_binding_mode == "full_source_row_upper"
                and advisory_rows is not None
                and actual_rows_copied <= advisory_rows
            )
            or (
                row_count_binding_mode
                == SHARED_STAGE_INDEXED_COUNT_TIMEOUT_ROW_BINDING_MODE
                and actual_rows_copied >= 0
            )
        )
        utilization = contract_number(
            report.get("utilization_ratio"),
            f"targets.{target}.utilization_ratio",
        )
        advisory_exceeded = actual > advisory
        advisory_delta = actual - advisory
        if history_cap_hit:
            cap_hit_targets.append(target)
        if advisory_exceeded:
            advisory_exceeded_targets.append(target)
        if (
            report.get("stage_filename") != shared_stage_target_filename(target)
            or storage_schema_version
            != shared_stage_target_storage_schema_version(target)
            or history_storage_compatible != (history_state != "none")
            or (
                history_storage_compatible
                and history_storage_schema_version != storage_schema_version
            )
            or (
                not history_storage_compatible
                and history_storage_schema_version == storage_schema_version
            )
            or contract_int(
                report.get("minimum_cap_bytes"),
                f"targets.{target}.minimum_cap_bytes",
            ) != minimum
            or report.get("advisory_query_bounded") is not True
            or report.get("physical_upper_bound_claimed") is not False
            or advisory != validated_advisory
            or advisory < minimum
            or advisory % SHARED_STAGE_PAGE_SIZE != 0
            or history_state != expected_history_state
            or (
                history_state == "none"
                and (
                    history_high_water != 0
                    or history_grant != 0
                    or history_cap_hit
                    or history_copy_completed
                )
            )
            or baseline != expected_baseline
            or baseline < minimum
            or grant < baseline
            or grant % SHARED_STAGE_PAGE_SIZE != 0
            or borrowed != grant - baseline
            or advisory_shortfall != max(0, advisory - grant)
            or actual <= 0
            or actual > grant
            or high_water != actual
            or actual_rows_copied < 0
            or not row_count_bound
            or report.get("row_count_bound_to_snapshot") is not True
            or report.get("advisory_exceeded") is not advisory_exceeded
            or contract_int(
                report.get("advisory_delta_bytes"),
                f"targets.{target}.advisory_delta_bytes",
            ) != advisory_delta
            or report.get("copy_completed") is not True
            or report.get("cap_hit") is not False
            or report.get("within_grant") is not True
            or not math.isfinite(utilization)
            or abs(utilization - (actual / grant)) > 1e-6
            or not isinstance(report.get("evidence_sources"), list)
            or not report.get("evidence_sources")
        ):
            raise ValueError(f"shared stage target contract invalid:{target}")
        grants[target] = grant
        actuals[target] = actual
        baselines[target] = baseline
        advisories[target] = advisory
        history_grants[target] = history_grant
        allocation_weights[target] = allocation_weight
        borrowed_total += borrowed
    minimum_total = sum(
        shared_stage_target_minimum_bytes(target)
        for target in expected_targets
    )
    baseline_total = sum(baselines.values())
    advisory_total = sum(advisories.values())
    residual_pool = total_cap - baseline_total
    expected_priority = cap_hit_targets or list(expected_targets)
    expected_weights = {
        target: max(
            SHARED_STAGE_PAGE_SIZE,
            advisories[target],
            history_grants[target],
        )
        for target in expected_priority
    }
    expected_allocations = allocate_shared_stage_residual(
        residual_bytes=residual_pool,
        priority_targets=expected_priority,
        weights=expected_weights,
    )
    for target in expected_targets:
        expected_weight = int(expected_weights.get(target, 0))
        expected_extra = int(expected_allocations.get(target, 0))
        if (
            allocation_weights[target] != expected_weight
            or grants[target] != baselines[target] + expected_extra
        ):
            raise ValueError(
                f"shared stage allocation mismatch:{target}"
            )
    raw_advisory_exceeded = shared.get("targets_exceeding_advisory")
    history_used = any(
        str((raw_targets.get(target) or {}).get("history_state") or "")
        != "none"
        for target in expected_targets
    )
    history_attempt_id = str(shared.get("history_attempt_id") or "")
    history_evidence_sha256 = str(
        shared.get("history_evidence_sha256") or ""
    )
    history_lineage_valid = bool(
        history_used
        and shared.get("history_lineage_validated") is True
        and shared.get("history_reason") == "history_accepted"
        and shared.get("history_anchor_schema_version")
        == SHARED_STAGE_HISTORY_ANCHOR_SCHEMA_VERSION
        and history_attempt_id
        and history_attempt_id != str(shared.get("attempt_id") or "")
        and valid_sha256_hex(history_evidence_sha256)
        and isinstance(trusted_history_anchor, dict)
        and trusted_history_anchor.get("schema_version")
        == SHARED_STAGE_HISTORY_ANCHOR_SCHEMA_VERSION
        and trusted_history_anchor.get("anchor_source")
        == "atomic_worker_attempt_sidecar"
        and trusted_history_anchor.get("immutable") is True
        and str(trusted_history_anchor.get("attempt_id") or "")
        == history_attempt_id
        and str(trusted_history_anchor.get("evidence_sha256") or "")
        == history_evidence_sha256
    )
    if (
        borrowing_priority != expected_priority
        or not isinstance(raw_advisory_exceeded, list)
        or [str(target) for target in raw_advisory_exceeded]
        != advisory_exceeded_targets
        or contract_int(
            shared.get("advisory_miss_count"),
            "advisory_miss_count",
        )
        != len(advisory_exceeded_targets)
        or shared.get("history_used") is not history_used
        or (
            history_used
            and not history_lineage_valid
        )
        or (
            not history_used
            and (
                shared.get("history_lineage_validated") is not False
                or shared.get("history_attempt_id") is not None
                or shared.get("history_evidence_sha256") is not None
                or shared.get("history_anchor_schema_version") is not None
            )
        )
        or sum(grants.values()) != total_cap
        or sum(actuals.values()) != actual_total
        or contract_int(
            shared.get("minimum_total_bytes"),
            "minimum_total_bytes",
        ) != minimum_total
        or contract_int(
            shared.get("baseline_required_total_bytes"),
            "baseline_required_total_bytes",
        )
        != baseline_total
        or contract_int(
            shared.get("advisory_demand_total_bytes"),
            "advisory_demand_total_bytes",
        )
        != advisory_total
        or contract_int(
            shared.get("allocation_weight_total_bytes"),
            "allocation_weight_total_bytes",
        )
        != sum(expected_weights.values())
        or contract_int(
            shared.get("residual_pool_bytes"),
            "residual_pool_bytes",
        ) != residual_pool
        or residual_pool < 0
        or residual_pool % SHARED_STAGE_PAGE_SIZE != 0
        or borrowed_total != residual_pool
    ):
        raise ValueError("shared stage target totals mismatch")
    candidate_grant = grants[SHARED_STAGE_TARGET_CANDIDATE]
    parallel_grants = {table: grants[table] for table in active_stage_tables}
    if (
        contract_int(
            disk_preflight.get("temporary_candidate_stage_cap_bytes"),
            "disk.temporary_candidate_stage_cap_bytes",
        )
        != candidate_grant
        or {
            str(table): contract_int(
                value,
                f"disk.temporary_parallel_paper_stage_cap_bytes.{table}",
            )
            for table, value in (
                disk_preflight.get("temporary_parallel_paper_stage_cap_bytes")
                or {}
            ).items()
        }
        != parallel_grants
        or contract_int(
            disk_preflight.get("temporary_paper_decision_stage_cap_bytes"),
            "disk.temporary_paper_decision_stage_cap_bytes",
        )
        != parallel_grants.get(PAPER_DECISION_STAGE_TABLE, 0)
        or disk_preflight.get("candidate_stage_budget_mode")
        != CANDIDATE_STAGE_BUDGET_MODE
        or disk_preflight.get("fixed_percentage_allocation_used") is not False
    ):
        raise ValueError("shared stage aliases mismatch")
    estimated_peak = contract_int(
        disk_preflight.get("estimated_peak_working_bytes"),
        "disk.estimated_peak_working_bytes",
    )
    estimated_free_at_peak = contract_int(
        disk_preflight.get("estimated_free_at_peak_bytes"),
        "disk.estimated_free_at_peak_bytes",
    )
    if (
        estimated_peak != output_cap + total_cap
        or estimated_free_at_peak != disk_free - estimated_peak
        or estimated_free_at_peak < reserve
        or contract_int(
            disk_preflight.get("estimated_free_after_bytes"),
            "disk.estimated_free_after_bytes",
        )
        != disk_free - output_cap
        or contract_int(
            disk_preflight.get("temporary_full_backup_bytes"),
            "disk.temporary_full_backup_bytes",
        ) != 0
        or disk_preflight.get("fail_closed_on_insufficient_space") is not True
    ):
        raise ValueError("shared stage disk reserve invalid")


def validate_shared_stage_snapshot_row_counts(
    manifest: dict,
    frozen_counts: dict[str, int],
    active_stage_tables: tuple[str, ...],
) -> None:
    shared = manifest.get("shared_stage_budget") or {}
    targets = shared.get("targets") or {}
    expected_targets = shared_stage_target_names(active_stage_tables)
    paper_report = (manifest.get("databases") or {}).get("paper") or {}
    selected_tables = paper_report.get("selected_tables") or {}
    if set(targets) != set(expected_targets):
        raise ValueError("shared stage frozen row inventory invalid")
    for target in expected_targets:
        frozen_table = (
            CANDIDATE_OBSERVATION_ROW_TABLE
            if target == SHARED_STAGE_TARGET_CANDIDATE
            else target
        )
        if frozen_table not in frozen_counts:
            raise ValueError(
                f"shared stage frozen row table missing:{target}"
            )
        report = targets.get(target) or {}
        selection = selected_tables.get(target) or {}
        actual_rows = json_safe_integer(
            report.get("actual_rows_copied"),
            field=f"{target}.actual_rows_copied",
        )
        selected_rows = json_safe_integer(
            selection.get("rows_copied"),
            field=f"{target}.selection.rows_copied",
        )
        if (
            actual_rows != selected_rows
            or actual_rows != int(frozen_counts[frozen_table])
        ):
            raise ValueError(
                f"shared stage frozen row count mismatch:{target}"
            )
        if target == SHARED_STAGE_TARGET_CANDIDATE:
            projection = selection.get("storage_projection") or {}
            if json_safe_integer(
                projection.get("rows_copied"),
                field="candidate_projection.rows_copied",
            ) != actual_rows:
                raise ValueError(
                    "candidate projection row count mismatch"
                )


def validate_shared_stage_estimate_read_view_bindings(
    manifest: dict,
    paper_report: dict,
    active_stage_tables: tuple[str, ...],
) -> None:
    shared = manifest.get("shared_stage_budget") or {}
    targets = shared.get("targets") or {}
    pinned_views = paper_report.get("pinned_read_views")
    if (
        manifest.get("shared_stage_estimates_bound_to_copy_read_views")
        is not True
        or paper_report.get(
            "shared_stage_estimates_bound_to_copy_read_views"
        )
        is not True
        or not isinstance(pinned_views, list)
    ):
        raise ValueError("shared stage pinned estimate binding missing")
    views_by_role: dict[str, dict] = {}
    read_view_ids: set[str] = set()
    for view in pinned_views:
        if not isinstance(view, dict):
            raise ValueError("shared stage pinned view invalid")
        role = str(view.get("role") or "")
        read_view_id = str(view.get("read_view_id") or "")
        if (
            not role
            or role in views_by_role
            or not re.fullmatch(r"[a-f0-9]{32}", read_view_id)
            or read_view_id in read_view_ids
        ):
            raise ValueError("shared stage pinned view identity invalid")
        views_by_role[role] = view
        read_view_ids.add(read_view_id)
    expected_roles = {
        SHARED_STAGE_TARGET_CANDIDATE: "paper_main_selective_copy",
        **{
            table: str(PARALLEL_PAPER_STAGE_CONFIGS[table]["role"])
            for table in active_stage_tables
        },
    }
    if set(targets) != set(expected_roles):
        raise ValueError("shared stage pinned target inventory invalid")
    for target, expected_role in expected_roles.items():
        report = targets.get(target) or {}
        evidence = report.get("advisory_evidence") or {}
        pinned_view = views_by_role.get(expected_role) or {}
        if (
            evidence.get("source_measurement_trust_boundary")
            != "same_pinned_read_view_as_copy"
            or evidence.get("estimate_started_after_pin") is not True
            or evidence.get("estimate_completed_before_copy") is not True
            or evidence.get("pinned_read_view_role") != expected_role
            or evidence.get("pinned_read_view_id")
            != pinned_view.get("read_view_id")
        ):
            raise ValueError(
                f"shared stage advisory read-view mismatch:{target}"
            )


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
    producer_shared_stage_history_anchor: dict = {}
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
        if not numeric_evidence_schema_binding_valid(producer_acceptance):
            blockers.append(
                "evaluator_snapshot_producer_numeric_evidence_schema_invalid"
            )
    if manifest_loaded:
        shared_budget = manifest.get("shared_stage_budget") or {}
        if shared_budget.get("history_used") is True:
            try:
                producer_shared_stage_history_anchor_file = (
                    shared_stage_budget_anchor_path(
                        producer_status_file,
                        shared_budget.get("history_attempt_id"),
                    )
                )
            except (TypeError, ValueError):
                producer_shared_stage_history_anchor_file = None
            if (
                producer_shared_stage_history_anchor_file is None
                or not producer_shared_stage_history_anchor_file.is_file()
            ):
                blockers.append(
                    "evaluator_snapshot_shared_stage_history_anchor_missing"
                )
            else:
                producer_shared_stage_history_anchor = read_json_object(
                    producer_shared_stage_history_anchor_file
                )
                if not producer_shared_stage_history_anchor:
                    blockers.append(
                        "evaluator_snapshot_shared_stage_history_anchor_invalid"
                    )
    if manifest_loaded:
        if not numeric_evidence_schema_binding_valid(manifest):
            blockers.append("evaluator_snapshot_numeric_evidence_schema_invalid")
        if not json_numeric_evidence_types_valid(manifest):
            blockers.append("evaluator_snapshot_numeric_evidence_type_invalid")
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
            or not is_json_safe_integer(
                manifest.get("parallel_paper_stage_count")
            )
            or manifest.get("parallel_paper_stage_count")
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
            manifest_read_lock_limit = json_finite_number(
                manifest.get("max_source_read_lock_sec"),
                field="manifest.max_source_read_lock_sec",
            )
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
                validate_shared_stage_budget_contract(
                    manifest,
                    disk_preflight,
                    safe_manifest_parallel_stage_tables,
                    producer_shared_stage_history_anchor,
                )
            except (KeyError, TypeError, ValueError, OverflowError):
                blockers.append(
                    "evaluator_snapshot_shared_stage_budget_contract_invalid"
                )
        try:
            output_size_bytes = json_safe_integer(
                manifest.get("output_size_bytes"),
                field="manifest.output_size_bytes",
            )
            output_cap_bytes = json_safe_integer(
                manifest.get("output_cap_bytes"),
                field="manifest.output_cap_bytes",
            )
            if output_size_bytes <= 0 or output_cap_bytes <= 0 or output_size_bytes > output_cap_bytes:
                blockers.append("evaluator_snapshot_output_size_contract_invalid")
            if isinstance(disk_preflight, dict) and disk_preflight.get("accepted") is True:
                try:
                    if json_safe_integer(
                        disk_preflight.get("selective_snapshot_output_cap_bytes"),
                        field="disk.selective_snapshot_output_cap_bytes",
                    ) != output_cap_bytes:
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
            snapshot_upper_epoch = json_finite_number(
                manifest.get("snapshot_ts"),
                field="manifest.snapshot_ts",
            )
            if abs(
                json_finite_number(
                    selection.get("common_upper_epoch"),
                    field="selection.common_upper_epoch",
                )
                - snapshot_upper_epoch
            ) > 0.001:
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
                    - json_finite_number(
                        manifest["snapshot_ts"],
                        field="manifest.snapshot_ts",
                    )
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
                report_read_lock_limit = json_finite_number(
                    report.get("source_read_lock_limit_sec"),
                    field=f"databases.{name}.source_read_lock_limit_sec",
                )
                report_read_lock_duration = json_finite_number(
                    report.get("source_read_lock_duration_sec"),
                    field=f"databases.{name}.source_read_lock_duration_sec",
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
            if json_safe_integer(
                report.get("temporary_full_backup_size_bytes"),
                field=f"databases.{name}.temporary_full_backup_size_bytes",
            ) != 0:
                blockers.append(f"evaluator_snapshot_{name}_full_backup_intermediate_detected")
            try:
                if abs(
                    json_finite_number(
                        report.get("selection_upper_epoch"),
                        field=f"databases.{name}.selection_upper_epoch",
                    )
                    - json_finite_number(
                        manifest.get("snapshot_ts"),
                        field="manifest.snapshot_ts",
                    )
                ) > 0.001:
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
                    frozen_shared_stage_row_counts = sqlite_table_row_counts(
                        candidate,
                        (
                            CANDIDATE_OBSERVATION_ROW_TABLE,
                            *active_parallel_stage_tables,
                        ),
                    )
                    validate_shared_stage_snapshot_row_counts(
                        manifest,
                        frozen_shared_stage_row_counts,
                        active_parallel_stage_tables,
                    )
                    validate_shared_stage_estimate_read_view_bindings(
                        manifest,
                        report,
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
                    main_lock_duration_sec = json_finite_number(
                        report.get("main_source_read_lock_duration_sec"),
                        field="paper.main_source_read_lock_duration_sec",
                    )
                    reported_max_lock_duration_sec = json_finite_number(
                        report.get("source_read_lock_duration_sec"),
                        field="paper.source_read_lock_duration_sec",
                    )
                    parallel_lock_duration_values = {
                        table: json_finite_number(
                            parallel_lock_durations.get(table),
                            field=(
                                "paper.parallel_source_read_lock_duration_sec."
                                f"{table}"
                            ),
                        )
                        for table in active_parallel_stage_tables
                    }
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
                        or not is_json_safe_integer(
                            report.get("parallel_paper_stage_count")
                        )
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
                            parallel_lock_duration_values[table] < 0
                            for table in active_parallel_stage_tables
                        )
                        or abs(
                            reported_max_lock_duration_sec
                            - max(
                                main_lock_duration_sec,
                                *[
                                    parallel_lock_duration_values[table]
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
                        stage_size_bytes = json_safe_integer(
                            stage_report.get("stage_size_bytes"),
                            field=f"{table}.stage_size_bytes",
                        )
                        stage_budget_bytes = json_safe_integer(
                            stage_report.get("stage_budget_bytes"),
                            field=f"{table}.stage_budget_bytes",
                        )
                        stage_page_size = json_safe_integer(
                            stage_report.get("stage_page_size"),
                            field=f"{table}.stage_page_size",
                        )
                        rows_copied = json_safe_integer(
                            stage_report.get("rows_copied"),
                            field=f"{table}.rows_copied",
                        )
                        rows_merged = json_safe_integer(
                            stage_report.get("rows_merged"),
                            field=f"{table}.rows_merged",
                        )
                        selection_rows_copied = json_safe_integer(
                            selection_report.get("rows_copied"),
                            field=f"{table}.selection.rows_copied",
                        )
                        merge_duration_sec = json_finite_number(
                            stage_report.get("merge_duration_sec"),
                            field=f"{table}.merge_duration_sec",
                        )
                        lock_duration_sec = json_finite_number(
                            stage_report.get("source_read_lock_duration_sec"),
                            field=f"{table}.source_read_lock_duration_sec",
                        )
                        final_schema = final_stage_schema_evidence.get(table) or {}
                        source_create_sql_sha256 = str(
                            stage_report.get("source_create_sql_sha256") or ""
                        )
                        destination_create_sql_sha256 = str(
                            stage_report.get("destination_create_sql_sha256")
                            or ""
                        )
                        source_column_contract_sha256 = str(
                            stage_report.get("source_column_contract_sha256")
                            or ""
                        )
                        destination_column_contract_sha256 = str(
                            stage_report.get(
                                "destination_column_contract_sha256"
                            )
                            or ""
                        )
                        stage_column_count = json_safe_integer(
                            stage_report.get("stage_column_count"),
                            field=f"{table}.stage_column_count",
                        )
                        stage_index_count = json_safe_integer(
                            stage_report.get("stage_index_count"),
                            field=f"{table}.stage_index_count",
                        )
                        stage_storage_contract_sha256 = str(
                            stage_report.get("stage_storage_contract_sha256")
                            or ""
                        )
                        stage_chunk_target_bytes = json_safe_integer(
                            stage_report.get("stage_chunk_target_bytes"),
                            field=f"{table}.stage_chunk_target_bytes",
                        )
                        stage_chunk_count = json_safe_integer(
                            stage_report.get("stage_chunk_count"),
                            field=f"{table}.stage_chunk_count",
                        )
                        stage_raw_size_bytes = json_safe_integer(
                            stage_report.get("stage_raw_size_bytes"),
                            field=f"{table}.stage_raw_size_bytes",
                        )
                        stage_compressed_payload_size_bytes = json_safe_integer(
                            stage_report.get(
                                "stage_compressed_payload_size_bytes"
                            ),
                            field=(
                                f"{table}."
                                "stage_compressed_payload_size_bytes"
                            ),
                        )
                        parallel_stage_cap_bytes = json_safe_integer(
                            parallel_stage_caps[table],
                            field=f"{table}.shared_stage_cap_bytes",
                        )
                        selection_stage_integers = {
                            field: json_safe_integer(
                                selection_report.get(field),
                                field=f"{table}.selection.{field}",
                            )
                            for field in (
                                "stage_column_count",
                                "stage_index_count",
                                "stage_chunk_target_bytes",
                                "stage_chunk_count",
                                "stage_raw_size_bytes",
                                "stage_compressed_payload_size_bytes",
                            )
                        }
                        nested_stage_column_count = json_safe_integer(
                            nested_stage.get("stage_column_count"),
                            field=f"{table}.nested.stage_column_count",
                        )
                        nested_stage_chunk_target_bytes = json_safe_integer(
                            nested_stage.get("stage_chunk_target_bytes"),
                            field=f"{table}.nested.stage_chunk_target_bytes",
                        )
                        nested_stage_chunk_count = json_safe_integer(
                            nested_stage.get("stage_chunk_count"),
                            field=f"{table}.nested.stage_chunk_count",
                        )
                        nested_stage_raw_size_bytes = json_safe_integer(
                            nested_stage.get("stage_raw_size_bytes"),
                            field=f"{table}.nested.stage_raw_size_bytes",
                        )
                        nested_stage_compressed_size_bytes = json_safe_integer(
                            nested_stage.get(
                                "stage_compressed_payload_size_bytes"
                            ),
                            field=(
                                f"{table}.nested."
                                "stage_compressed_payload_size_bytes"
                            ),
                        )
                        nested_stage_index_count = json_safe_integer(
                            nested_stage.get("stage_index_count"),
                            field=f"{table}.nested.stage_index_count",
                        )
                        nested_stage_rows_copied = json_safe_integer(
                            nested_stage.get("stage_rows_copied"),
                            field=f"{table}.nested.stage_rows_copied",
                        )
                        nested_rows_merged = json_safe_integer(
                            nested_stage.get("rows_merged"),
                            field=f"{table}.nested.rows_merged",
                        )
                        nested_stage_page_size = json_safe_integer(
                            nested_stage.get("stage_page_size"),
                            field=f"{table}.nested.stage_page_size",
                        )
                        nested_stage_size_bytes = json_safe_integer(
                            nested_stage.get("stage_size_bytes"),
                            field=f"{table}.nested.stage_size_bytes",
                        )
                        nested_stage_budget_bytes = json_safe_integer(
                            nested_stage.get("stage_budget_bytes"),
                            field=f"{table}.nested.stage_budget_bytes",
                        )
                        nested_merge_duration_sec = json_finite_number(
                            nested_stage.get("merge_duration_sec"),
                            field=f"{table}.nested.merge_duration_sec",
                        )
                        nested_lock_duration_sec = json_finite_number(
                            nested_stage.get("source_read_lock_duration_sec"),
                            field=(
                                f"{table}.nested."
                                "source_read_lock_duration_sec"
                            ),
                        )
                        stage_rows_sha256 = str(
                            stage_report.get("stage_rows_sha256") or ""
                        )
                        hydrated_rows_sha256 = str(
                            stage_report.get("hydrated_rows_sha256") or ""
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
                                    destination_create_sql_sha256,
                                    source_column_contract_sha256,
                                    destination_column_contract_sha256,
                                    stage_storage_contract_sha256,
                                    stage_rows_sha256,
                                    hydrated_rows_sha256,
                                )
                            )
                            or source_create_sql_sha256
                            != destination_create_sql_sha256
                            or source_create_sql_sha256
                            != final_schema.get("create_sql_sha256")
                            or source_column_contract_sha256
                            != destination_column_contract_sha256
                            or source_column_contract_sha256
                            != final_schema.get("column_contract_sha256")
                            or stage_column_count
                            != int(final_schema.get("column_count"))
                            or int(final_schema.get("hidden_column_count")) != 0
                            or stage_storage_contract_sha256
                            != compressed_stage_storage_contract_sha256()
                            or stage_report.get("stage_storage_contract_passed")
                            is not True
                            or stage_report.get("stage_codec_schema_version")
                            != PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION
                            or stage_report.get("stage_compression")
                            != PARALLEL_PAPER_STAGE_COMPRESSION
                            or stage_chunk_target_bytes
                            != PARALLEL_PAPER_STAGE_CHUNK_TARGET_BYTES
                            or stage_chunk_count < 0
                            or stage_raw_size_bytes < 0
                            or stage_compressed_payload_size_bytes < 0
                            or stage_compressed_payload_size_bytes
                            > stage_size_bytes
                            or (rows_copied == 0) != (stage_chunk_count == 0)
                            or (rows_copied == 0) != (stage_raw_size_bytes == 0)
                            or (rows_copied == 0) != (
                                stage_compressed_payload_size_bytes == 0
                            )
                            or stage_rows_sha256 != hydrated_rows_sha256
                            or stage_report.get(
                                "stage_chunk_integrity_passed"
                            ) is not True
                            or stage_report.get("stage_row_digest_matched")
                            is not True
                            or stage_report.get(
                                "compressed_during_source_read_lock"
                            ) is not True
                            or stage_report.get(
                                "hydrated_after_source_read_lock_release"
                            ) is not True
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
                            or stage_budget_bytes != parallel_stage_cap_bytes
                            or stage_size_bytes > stage_budget_bytes
                            or stage_page_size not in PARALLEL_PAPER_STAGE_PAGE_SIZES
                            or stage_size_bytes % stage_page_size != 0
                            or (
                                stage_page_size
                                == PARALLEL_PAPER_STAGE_BULK_PAGE_SIZE
                                and stage_budget_bytes
                                < PARALLEL_PAPER_STAGE_BULK_PAGE_MIN_BUDGET_BYTES
                            )
                            or rows_copied != selection_rows_copied
                            or rows_merged != rows_copied
                            or merge_duration_sec < 0
                            or lock_duration_sec
                            != parallel_lock_duration_values[table]
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
                            or selection_report.get("stage_schema_mode")
                            != PARALLEL_PAPER_STAGE_STORAGE_MODE
                            or selection_report.get("source_create_sql_sha256")
                            != source_create_sql_sha256
                            or selection_report.get(
                                "source_column_contract_sha256"
                            )
                            != source_column_contract_sha256
                            or selection_report.get(
                                "source_constraints_deferred_off_source_lock"
                            )
                            is not True
                            or selection_report.get(
                                "stage_storage_contract_sha256"
                            )
                            != stage_storage_contract_sha256
                            or selection_report.get(
                                "stage_storage_contract_passed"
                            )
                            is not True
                            or selection_report.get(
                                "stage_codec_schema_version"
                            )
                            != PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION
                            or selection_report.get("stage_compression")
                            != PARALLEL_PAPER_STAGE_COMPRESSION
                            or selection_stage_integers["stage_column_count"]
                            != stage_column_count
                            or selection_stage_integers["stage_index_count"]
                            != stage_index_count
                            or selection_stage_integers[
                                "stage_chunk_target_bytes"
                            ]
                            != stage_chunk_target_bytes
                            or selection_stage_integers["stage_chunk_count"]
                            != stage_chunk_count
                            or selection_stage_integers[
                                "stage_raw_size_bytes"
                            ]
                            != stage_raw_size_bytes
                            or selection_stage_integers[
                                "stage_compressed_payload_size_bytes"
                            ]
                            != stage_compressed_payload_size_bytes
                            or selection_report.get("stage_rows_sha256")
                            != stage_rows_sha256
                            or nested_stage.get("schema_version")
                            != PARALLEL_PAPER_STAGE_SCHEMA_VERSION
                            or nested_stage.get("role") != config["role"]
                            or nested_stage.get("stage_schema_mode")
                            != PARALLEL_PAPER_STAGE_STORAGE_MODE
                            or nested_stage.get("source_create_sql_sha256")
                            != source_create_sql_sha256
                            or nested_stage.get("destination_create_sql_sha256")
                            != destination_create_sql_sha256
                            or nested_stage.get(
                                "source_column_contract_sha256"
                            )
                            != source_column_contract_sha256
                            or nested_stage.get(
                                "destination_column_contract_sha256"
                            )
                            != destination_column_contract_sha256
                            or nested_stage_column_count != stage_column_count
                            or nested_stage.get(
                                "stage_storage_contract_sha256"
                            ) != stage_storage_contract_sha256
                            or nested_stage.get("stage_storage_contract_passed")
                            is not True
                            or nested_stage.get("stage_codec_schema_version")
                            != PARALLEL_PAPER_STAGE_CODEC_SCHEMA_VERSION
                            or nested_stage.get("stage_compression")
                            != PARALLEL_PAPER_STAGE_COMPRESSION
                            or nested_stage_chunk_target_bytes
                            != PARALLEL_PAPER_STAGE_CHUNK_TARGET_BYTES
                            or nested_stage_chunk_count != stage_chunk_count
                            or nested_stage_raw_size_bytes
                            != stage_raw_size_bytes
                            or nested_stage_compressed_size_bytes
                            != stage_compressed_payload_size_bytes
                            or nested_stage.get("stage_rows_sha256")
                            != stage_rows_sha256
                            or nested_stage.get("hydrated_rows_sha256")
                            != hydrated_rows_sha256
                            or nested_stage.get(
                                "stage_chunk_integrity_passed"
                            ) is not True
                            or nested_stage.get("stage_row_digest_matched")
                            is not True
                            or nested_stage.get(
                                "compressed_during_source_read_lock"
                            ) is not True
                            or nested_stage.get(
                                "hydrated_after_source_read_lock_release"
                            ) is not True
                            or nested_stage_index_count != 0
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
                            or nested_stage_rows_copied != rows_copied
                            or nested_rows_merged != rows_merged
                            or nested_merge_duration_sec != merge_duration_sec
                            or nested_lock_duration_sec != lock_duration_sec
                            or nested_stage.get("quick_check") != ["ok"]
                            or nested_stage_page_size != stage_page_size
                            or nested_stage_size_bytes != stage_size_bytes
                            or nested_stage_budget_bytes != stage_budget_bytes
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
                    alias_stage_size_bytes = json_safe_integer(
                        paper_alias.get("stage_size_bytes"),
                        field="paper_decision_events.alias.stage_size_bytes",
                    )
                    alias_stage_page_size = json_safe_integer(
                        paper_alias.get("stage_page_size"),
                        field="paper_decision_events.alias.stage_page_size",
                    )
                    alias_stage_budget_bytes = json_safe_integer(
                        paper_alias.get("stage_budget_bytes"),
                        field="paper_decision_events.alias.stage_budget_bytes",
                    )
                    alias_rows_merged = json_safe_integer(
                        paper_alias.get("rows_merged"),
                        field="paper_decision_events.alias.rows_merged",
                    )
                    alias_merge_duration_sec = json_finite_number(
                        paper_alias.get("merge_duration_sec"),
                        field=(
                            "paper_decision_events.alias.merge_duration_sec"
                        ),
                    )
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
                        or json_safe_integer(
                            report.get(
                                "paper_decision_parallel_stage_size_bytes"
                            ),
                            field="paper.paper_decision_stage_size_bytes",
                        ) != alias_stage_size_bytes
                        or json_safe_integer(
                            report.get(
                                "paper_decision_parallel_stage_page_size"
                            ),
                            field="paper.paper_decision_stage_page_size",
                        ) != alias_stage_page_size
                        or json_safe_integer(
                            report.get(
                                "paper_decision_parallel_stage_budget_bytes"
                            ),
                            field="paper.paper_decision_stage_budget_bytes",
                        ) != alias_stage_budget_bytes
                        or json_safe_integer(
                            report.get(
                                "paper_decision_parallel_stage_rows_merged"
                            ),
                            field="paper.paper_decision_stage_rows_merged",
                        ) != alias_rows_merged
                        or json_finite_number(
                            report.get(
                                "paper_decision_parallel_stage_merge_duration_sec"
                            ),
                            field=(
                                "paper.paper_decision_stage_merge_duration_sec"
                            ),
                        ) != alias_merge_duration_sec
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
                    stage_size_bytes = json_safe_integer(
                        report.get("temporary_candidate_stage_size_bytes"),
                        field="paper.temporary_candidate_stage_size_bytes",
                    )
                    stage_cap_bytes = json_safe_integer(
                        disk_preflight.get("temporary_candidate_stage_cap_bytes"),
                        field="disk.temporary_candidate_stage_cap_bytes",
                    )
                    projection_duration_sec = json_finite_number(
                        report.get("candidate_projection_duration_sec"),
                        field="paper.candidate_projection_duration_sec",
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
                        or json_safe_integer(
                            candidate_projection.get("source_stage_size_bytes"),
                            field=(
                                "candidate_projection.source_stage_size_bytes"
                            ),
                        )
                        != stage_size_bytes
                        or candidate_projection.get("stage_order_index_name")
                        != "idx_a3_candidate_stage_signal"
                        or candidate_projection.get("stage_query_plan_uses_order_index")
                        is not True
                        or candidate_projection.get("stage_query_plan_temp_btree_detected")
                        is not False
                        or not isinstance(stage_plan, list)
                        or not stage_plan
                        or not any(
                            "idx_a3_candidate_stage_signal" in str(item)
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
            reported_snapshot_size = report.get("snapshot_size_bytes")
            if candidate.is_file() and (
                not is_json_safe_integer(reported_snapshot_size)
                or reported_snapshot_size != candidate.stat().st_size
            ):
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
                json_finite_number(
                    view.get("pinned_midpoint_epoch"),
                    field="pinned_read_view.pinned_midpoint_epoch",
                )
                for view in all_pinned_read_views
            ]
            pinned_limits = [
                json_finite_number(
                    view.get("source_read_lock_limit_sec"),
                    field="pinned_read_view.source_read_lock_limit_sec",
                )
                for view in all_pinned_read_views
            ]
            recomputed_skew = max(pinned_midpoints) - min(pinned_midpoints)
            manifest_skew = json_finite_number(
                manifest.get("cross_database_time_skew_sec"),
                field="manifest.cross_database_time_skew_sec",
            )
            manifest_max_skew = json_finite_number(
                manifest.get("max_allowed_cross_database_time_skew_sec"),
                field="manifest.max_allowed_cross_database_time_skew_sec",
            )
            if (
                len(all_pinned_read_views)
                != 4 + len(safe_manifest_parallel_stage_tables)
                or json_safe_integer(
                    manifest.get("pinned_read_view_count"),
                    field="manifest.pinned_read_view_count",
                )
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
        "numeric_evidence_schema_version": EVIDENCE_SCHEMA_VERSION,
        "numeric_evidence_schema_sha256": EVIDENCE_SCHEMA_SHA256,
        "numeric_evidence_schema_binding_valid": bool(
            numeric_evidence_schema_binding_valid(manifest)
            and numeric_evidence_schema_binding_valid(producer_acceptance)
        ),
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
        "numeric_evidence_schema_version": payload.get(
            "numeric_evidence_schema_version"
        ),
        "numeric_evidence_schema_sha256": payload.get(
            "numeric_evidence_schema_sha256"
        ),
        "numeric_evidence_schema_binding_valid": (
            payload.get("numeric_evidence_schema_binding_valid") is True
        ),
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
