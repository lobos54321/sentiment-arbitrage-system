#!/usr/bin/env python3
"""Audit Telegram signal identity, dual denominators, and outcome label coverage.

This evaluator is read-only. It never changes strategy, gates, runtime mode,
execution, canary, wallet, or risk settings.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import tempfile
import time
from typing import Any


SCHEMA_VERSION = "telegram_signal_identity_audit.v1"
EXPECTED_CONTRACT_SCHEMA_VERSION = "telegram_outcome_contract.v1"
EXPECTED_OUTCOME_SCHEMA_VERSION = "telegram_signal_outcome.v1"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONTRACT = PROJECT_ROOT / "docs" / "agents" / "contracts" / "telegram-outcome-contract.v1.json"
MESSAGE_ID_COLUMNS = ("telegram_message_id", "source_message_id", "message_id")
CHANNEL_ID_COLUMNS = ("telegram_channel_id", "source_channel_id", "channel_id", "chat_id")
LIFECYCLE_COLUMNS = ("downstream_lifecycle_id", "lifecycle_id")
SOL_MINT = "So11111111111111111111111111111111111111112"
EXECUTABLE_QUOTE_EVIDENCE_SCHEMA_VERSION = "raw_executable_quote_evidence.v1"
EXECUTABLE_QUOTE_RECORD_SCHEMA_VERSION = "raw_executable_quote_record.v1"
EXECUTABLE_QUOTE_ENTRY_MAX_LAG_SEC = 600
EXECUTABLE_QUOTE_EXIT_MAX_LAG_SEC = 600
EXECUTABLE_QUOTE_REQUIRED_COLUMNS = {
    "executable_quote_evidence_version",
    "executable_quote_size_sol",
    "executable_entry_quote_json",
    "executable_exit_quote_json",
    "executable_quote_return_pct",
    "executable_quote_evidence_status",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + f".{time.time_ns()}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(target)


def load_json(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"JSON object required: {path}")
    return data


def open_readonly(path: str | Path) -> sqlite3.Connection:
    resolved = Path(path).expanduser().resolve()
    connection = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def table_exists(db: sqlite3.Connection, table: str) -> bool:
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone() is not None


def table_columns(db: sqlite3.Connection, table: str) -> set[str]:
    if not table_exists(db, table):
        return set()
    return {str(row["name"]) for row in db.execute(f"PRAGMA table_info({table})")}


def first_value(row: dict[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        value = row.get(name)
        if value is not None and str(value).strip() != "":
            return value
    return None


def normalize_ts_sec(value: Any) -> int | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        text = str(value).strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    if numeric <= 0:
        return None
    return int(numeric / 1000) if numeric > 1_000_000_000_000 else int(numeric)


def normalized_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def round_rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def signal_timestamp(row: dict[str, Any]) -> int | None:
    return normalize_ts_sec(
        first_value(row, ("source_message_ts", "timestamp", "receive_ts", "created_at"))
    )


def classify_identity(row: dict[str, Any]) -> dict[str, Any]:
    signal_id = normalized_text(row.get("id") or row.get("signal_id"))
    token = normalized_text(row.get("token_ca"))
    signal_type = normalized_text(row.get("signal_type")) or ("ATH" if row.get("is_ath") else "UNKNOWN")
    channel_id = normalized_text(first_value(row, CHANNEL_ID_COLUMNS))
    message_id = normalized_text(first_value(row, MESSAGE_ID_COLUMNS))
    source_event_id = normalized_text(row.get("source_event_id"))
    source_ts = normalize_ts_sec(first_value(row, ("source_message_ts", "timestamp", "receive_ts")))

    common = {
        "signal_id": signal_id,
        "token_ca": token,
        "telegram_message_id_present": bool(message_id),
        "telegram_channel_id_present": bool(channel_id),
        "message_id_without_channel": bool(message_id and not channel_id),
    }
    if message_id and channel_id:
        return {
            "namespace": "telegram_message_id",
            "key": f"telegram:{channel_id}:{message_id}",
            "confidence": "exact",
            **common,
        }
    if source_event_id:
        return {
            "namespace": "source_event_id",
            "key": f"source_event:{source_event_id}",
            "confidence": "deterministic_fallback",
            **common,
        }
    if token and source_ts is not None:
        return {
            "namespace": "token_source_ts_signal_type",
            "key": f"alias:{token}:{source_ts}:{signal_type}",
            "confidence": "deterministic_alias",
            **common,
        }
    if signal_id:
        return {
            "namespace": "signal_id_only",
            "key": f"signal:{signal_id}",
            "confidence": "internal_only",
            **common,
        }
    return {
        "namespace": "unknown",
        "key": None,
        "confidence": "unknown",
        **common,
    }


def select_signal_rows(
    db: sqlite3.Connection,
    *,
    hours: int,
    now_ts: int,
    limit: int,
) -> tuple[list[dict[str, Any]], set[str]]:
    columns = table_columns(db, "premium_signals")
    if not columns:
        return [], columns
    selected = [
        name
        for name in (
            "id",
            "token_ca",
            "symbol",
            "timestamp",
            "source_message_ts",
            "receive_ts",
            "signal_type",
            "is_ath",
            "signal_source",
            "source_event_id",
            "parse_status",
            "downstream_lifecycle_id",
            "lifecycle_id",
            "ath_stage",
            "created_at",
            *MESSAGE_ID_COLUMNS,
            *CHANNEL_ID_COLUMNS,
        )
        if name in columns
    ]
    timestamp_column = next(
        (name for name in ("source_message_ts", "timestamp", "receive_ts") if name in columns),
        None,
    )
    if not selected:
        return [], columns
    where = ""
    params: list[Any] = []
    if timestamp_column and hours > 0:
        since_sec = now_ts - hours * 3600
        since_ms = since_sec * 1000
        where = (
            f"WHERE (({timestamp_column} > 1000000000000 AND {timestamp_column} >= ?) "
            f"OR ({timestamp_column} <= 1000000000000 AND {timestamp_column} >= ?))"
        )
        params.extend([since_ms, since_sec])
    order = "id DESC" if "id" in columns else (
        f"{timestamp_column} DESC" if timestamp_column else "rowid DESC"
    )
    params.append(max(1, int(limit)))
    sql = f"SELECT {', '.join(selected)} FROM premium_signals {where} ORDER BY {order} LIMIT ?"
    rows = [dict(row) for row in db.execute(sql, params)]
    return rows, columns


def select_raw_outcomes(
    db: sqlite3.Connection | None,
    signal_ids: set[str],
) -> tuple[list[dict[str, Any]], set[str]]:
    if db is None or not signal_ids:
        return [], set()
    columns = table_columns(db, "raw_signal_outcomes")
    if not columns or "signal_id" not in columns:
        return [], columns
    selected = [
        name
        for name in (
            "id",
            "signal_id",
            "token_ca",
            "signal_ts",
            "observation_status",
            "right_censored",
            "horizon_sec",
            "raw_wick_tier",
            "raw_sustained_tier",
            "raw_primary_tier",
            "max_wick_peak_pct",
            "max_sustained_peak_pct",
            "executable_quote_evidence_version",
            "executable_quote_size_sol",
            "executable_entry_quote_json",
            "executable_exit_quote_json",
            "executable_quote_return_pct",
            "executable_quote_evidence_status",
            "executable_quote_failure_reason",
            "executable_quote_last_attempt_ts",
            "executable_quote_attempt_count",
            "sustained_evaluable",
            "baseline_confidence",
            "updated_at",
        )
        if name in columns
    ]
    rows: list[dict[str, Any]] = []
    ordered_ids = sorted(signal_ids)
    for offset in range(0, len(ordered_ids), 800):
        chunk = ordered_ids[offset: offset + 800]
        placeholders = ",".join("?" for _ in chunk)
        sql = (
            f"SELECT {', '.join(selected)} FROM raw_signal_outcomes "
            f"WHERE CAST(signal_id AS TEXT) IN ({placeholders})"
        )
        rows.extend(dict(row) for row in db.execute(sql, chunk))
    return rows, columns


def tier_for_pct(value: Any, contract: dict[str, Any]) -> str:
    try:
        pct = float(value)
    except (TypeError, ValueError):
        return "unknown"
    thresholds = contract.get("thresholds") or {}
    ordered = ("100x", "10x", "gold", "silver", "bronze")
    for name in ordered:
        try:
            threshold = float((thresholds.get(name) or {}).get("min_return_pct"))
        except (TypeError, ValueError):
            continue
        if pct >= threshold:
            return name
    return "sub_bronze"


def raw_outcome_maturity(row: dict[str, Any], snapshot_ts: int) -> dict[str, Any]:
    status = (normalized_text(row.get("observation_status")) or "unknown").lower()
    explicit_right_censored = bool(row.get("right_censored"))
    signal_ts = normalize_ts_sec(row.get("signal_ts"))
    try:
        horizon_sec = int(row.get("horizon_sec"))
    except (TypeError, ValueError):
        horizon_sec = None
    horizon_elapsed = (
        signal_ts is not None
        and horizon_sec is not None
        and horizon_sec >= 0
        and signal_ts + horizon_sec <= snapshot_ts
    )
    status_mature = status in {"matured", "complete", "completed", "closed"}
    mature = bool(status_mature and horizon_elapsed and not explicit_right_censored)
    reasons = []
    if not status_mature:
        reasons.append("observation_status_not_mature")
    if signal_ts is None:
        reasons.append("signal_ts_missing")
    if horizon_sec is None:
        reasons.append("horizon_sec_missing")
    elif not horizon_elapsed:
        reasons.append("horizon_not_elapsed")
    if explicit_right_censored:
        reasons.append("explicit_right_censored")
    return {
        "mature": mature,
        "status": status,
        "signal_ts": signal_ts,
        "horizon_sec": horizon_sec,
        "horizon_elapsed": horizon_elapsed,
        "explicit_right_censored": explicit_right_censored,
        "reasons": reasons,
    }


def _strict_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _positive_integer_text(value: Any) -> str | None:
    if not isinstance(value, str) or re.fullmatch(r"[1-9][0-9]*", value) is None:
        return None
    return value


def _parse_quote_record(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def validate_executable_quote_evidence(row: dict[str, Any]) -> dict[str, Any]:
    def reject(reason: str) -> dict[str, Any]:
        return {"valid": False, "reason": reason, "return_pct": None}

    if row.get("executable_quote_evidence_version") != EXECUTABLE_QUOTE_EVIDENCE_SCHEMA_VERSION:
        return reject("evidence_version_missing_or_invalid")
    if row.get("executable_quote_evidence_status") != "complete":
        return reject("evidence_status_not_complete")
    signal_id = normalized_text(row.get("signal_id"))
    token_ca = normalized_text(row.get("token_ca"))
    signal_ts = _strict_int(row.get("signal_ts"))
    horizon_sec = _strict_int(row.get("horizon_sec"))
    if not signal_id or not token_ca or signal_ts is None or horizon_sec is None or horizon_sec < 0:
        return reject("row_identity_or_horizon_invalid")
    size_sol = row.get("executable_quote_size_sol")
    if (not isinstance(size_sol, (int, float)) or isinstance(size_sol, bool)
            or not math.isfinite(float(size_sol)) or not 0 < float(size_sol) <= 1):
        return reject("quote_size_invalid")
    stored_return_pct = row.get("executable_quote_return_pct")
    if (not isinstance(stored_return_pct, (int, float)) or isinstance(stored_return_pct, bool)
            or not math.isfinite(float(stored_return_pct))):
        return reject("stored_return_pct_invalid")
    entry = _parse_quote_record(row.get("executable_entry_quote_json"))
    exit_quote = _parse_quote_record(row.get("executable_exit_quote_json"))
    if entry is None or exit_quote is None:
        return reject("entry_or_exit_quote_json_invalid")

    def validate_record(record: dict[str, Any], side: str) -> str | None:
        if record.get("schema_version") != EXECUTABLE_QUOTE_RECORD_SCHEMA_VERSION:
            return f"{side}_record_version_invalid"
        if record.get("side") != side:
            return f"{side}_side_invalid"
        if normalized_text(record.get("signal_id")) != signal_id:
            return f"{side}_signal_id_mismatch"
        if normalized_text(record.get("token_ca")) != token_ca:
            return f"{side}_token_ca_mismatch"
        if _strict_int(record.get("signal_ts")) != signal_ts:
            return f"{side}_signal_ts_mismatch"
        if _strict_int(record.get("horizon_sec")) != horizon_sec:
            return f"{side}_horizon_sec_mismatch"
        if record.get("provider") != "jupiter-ultra" or record.get("source") != "shared-quote-client":
            return f"{side}_provider_invalid"
        if record.get("executable") is not True:
            return f"{side}_executable_flag_invalid"
        if not normalized_text(record.get("provider_request_id")):
            return f"{side}_request_id_missing"
        if _positive_integer_text(record.get("input_amount_raw")) is None:
            return f"{side}_input_amount_invalid"
        if _positive_integer_text(record.get("output_amount_raw")) is None:
            return f"{side}_output_amount_invalid"
        route_plan_json = record.get("route_plan_json")
        if not isinstance(route_plan_json, str) or not route_plan_json:
            return f"{side}_route_plan_missing"
        if hashlib.sha256(route_plan_json.encode("utf-8")).hexdigest() != record.get("route_plan_sha256"):
            return f"{side}_route_plan_hash_mismatch"
        try:
            route_plan = json.loads(route_plan_json)
        except (TypeError, ValueError):
            return f"{side}_route_plan_json_invalid"
        hop_count = _strict_int(record.get("route_plan_hop_count"))
        if not isinstance(route_plan, list) or not route_plan or hop_count != len(route_plan):
            return f"{side}_route_plan_hop_count_invalid"
        fetched_at_ms = _strict_int(record.get("provider_fetched_at_ms"))
        captured_at_ms = _strict_int(record.get("captured_at_ms"))
        if fetched_at_ms is None or captured_at_ms is None or fetched_at_ms <= 0 or captured_at_ms <= 0:
            return f"{side}_timestamp_invalid"
        if fetched_at_ms > captured_at_ms + 5_000 or captured_at_ms - fetched_at_ms > 60_000:
            return f"{side}_provider_timestamp_unbound"
        return None

    for record, side in ((entry, "entry"), (exit_quote, "exit")):
        reason = validate_record(record, side)
        if reason:
            return reject(reason)

    if entry.get("input_mint") != SOL_MINT or entry.get("output_mint") != token_ca:
        return reject("entry_mint_direction_invalid")
    if exit_quote.get("input_mint") != token_ca or exit_quote.get("output_mint") != SOL_MINT:
        return reject("exit_mint_direction_invalid")
    expected_entry_input = round(float(size_sol) * 1_000_000_000)
    if int(entry["input_amount_raw"]) != expected_entry_input:
        return reject("entry_size_amount_mismatch")
    if exit_quote["input_amount_raw"] != entry["output_amount_raw"]:
        return reject("entry_exit_amount_chain_mismatch")
    entry_captured_ms = int(entry["captured_at_ms"])
    exit_captured_ms = int(exit_quote["captured_at_ms"])
    if not signal_ts * 1000 <= entry_captured_ms <= (signal_ts + EXECUTABLE_QUOTE_ENTRY_MAX_LAG_SEC) * 1000 + 999:
        return reject("entry_capture_outside_time_legal_window")
    exit_target_ts = signal_ts + horizon_sec
    if not exit_target_ts * 1000 <= exit_captured_ms <= (exit_target_ts + EXECUTABLE_QUOTE_EXIT_MAX_LAG_SEC) * 1000 + 999:
        return reject("exit_capture_outside_time_legal_window")
    recomputed_return_pct = (
        (int(exit_quote["output_amount_raw"]) / int(entry["input_amount_raw"])) - 1
    ) * 100
    if not math.isfinite(recomputed_return_pct):
        return reject("recomputed_return_pct_invalid")
    tolerance = max(1e-9, abs(recomputed_return_pct) * 1e-12)
    if abs(float(stored_return_pct) - recomputed_return_pct) > tolerance:
        return reject("stored_return_pct_mismatch")
    return {"valid": True, "reason": None, "return_pct": recomputed_return_pct}


def file_metadata(path: str | Path | None) -> dict[str, Any]:
    if not path:
        return {"available": False, "path": None}
    target = Path(path).expanduser().resolve()
    if not target.exists():
        return {"available": False, "path": str(target)}
    stat = target.stat()
    return {
        "available": True,
        "path": str(target),
        "size_bytes": stat.st_size,
        "mtime_epoch": stat.st_mtime,
        "mtime_iso": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def max_column_value(db: sqlite3.Connection, table: str, columns: tuple[str, ...]) -> dict[str, Any]:
    available = table_columns(db, table)
    result: dict[str, Any] = {}
    for column in columns:
        if column in available:
            result[column] = db.execute(f"SELECT MAX({column}) FROM {table}").fetchone()[0]
    return result


def build_audit(
    *,
    signal_db_path: str,
    raw_db_path: str | None,
    contract_path: str,
    hours: int,
    now_ts: int,
    limit: int,
) -> dict[str, Any]:
    contract = load_json(contract_path)
    signal_read_started = time.time()
    signal_db = open_readonly(signal_db_path)
    raw_db = None
    try:
        signal_rows, signal_columns = select_signal_rows(
            signal_db,
            hours=hours,
            now_ts=now_ts,
            limit=limit,
        )
        signal_watermark = max_column_value(
            signal_db,
            "premium_signals",
            ("id", "timestamp", "source_message_ts", "receive_ts"),
        )
    finally:
        signal_db.close()
    signal_read_finished = time.time()

    identities = [classify_identity(row) for row in signal_rows]
    signal_ids = {identity["signal_id"] for identity in identities if identity.get("signal_id")}
    raw_read_started = time.time()
    raw_rows: list[dict[str, Any]] = []
    raw_columns: set[str] = set()
    raw_watermark: dict[str, Any] = {}
    if raw_db_path and Path(raw_db_path).expanduser().exists():
        raw_db = open_readonly(raw_db_path)
        try:
            raw_rows, raw_columns = select_raw_outcomes(raw_db, signal_ids)
            raw_watermark = max_column_value(
                raw_db,
                "raw_signal_outcomes",
                ("id", "signal_ts", "updated_at"),
            )
        finally:
            raw_db.close()
    raw_read_finished = time.time()

    identity_counts = Counter(identity["namespace"] for identity in identities)
    identity_key_counts = Counter(identity["key"] for identity in identities if identity.get("key"))
    duplicate_identity_rows = sum(count - 1 for count in identity_key_counts.values() if count > 1)
    unknown_rows = [
        {
            "signal_id": identity.get("signal_id"),
            "token_ca": identity.get("token_ca"),
            "reason": "no_message_id_source_event_id_token_timestamp_or_signal_id",
        }
        for identity in identities
        if identity["namespace"] == "unknown"
    ]
    signal_id_only_rows = [
        {
            "signal_id": identity.get("signal_id"),
            "token_ca": identity.get("token_ca"),
            "reason": "source_identity_not_persisted_signal_id_only",
        }
        for identity in identities
        if identity["namespace"] == "signal_id_only"
    ]

    canonical_event_keys = {identity["key"] for identity in identities if identity.get("key")}
    tokens = [normalized_text(row.get("token_ca")) for row in signal_rows]
    unique_tokens = {token for token in tokens if token}
    token_event_counts = Counter(token for token in tokens if token)
    repeated_token_event_rows = sum(count - 1 for count in token_event_counts.values() if count > 1)
    signal_type_counts = Counter(
        normalized_text(row.get("signal_type")) or ("ATH" if row.get("is_ath") else "UNKNOWN")
        for row in signal_rows
    )
    lifecycle_values = [
        normalized_text(first_value(row, LIFECYCLE_COLUMNS))
        for row in signal_rows
    ]
    lifecycle_present = sum(value is not None for value in lifecycle_values)
    exact_message_present = identity_counts["telegram_message_id"]
    source_identity_present = sum(
        identity_counts[name]
        for name in ("telegram_message_id", "source_event_id", "token_source_ts_signal_type")
    )
    canonical_identity_present = len(signal_rows) - identity_counts["unknown"]
    full_lineage = sum(
        bool(identity.get("key") and identity.get("signal_id") and identity.get("token_ca") and lifecycle)
        for identity, lifecycle in zip(identities, lifecycle_values)
    )

    raw_by_signal: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in raw_rows:
        signal_id = normalized_text(row.get("signal_id"))
        if signal_id:
            raw_by_signal[signal_id].append(row)
    joined_signal_ids = set(raw_by_signal)
    raw_joined_signal_events = sum(
        1 for identity in identities if identity.get("signal_id") in joined_signal_ids
    )
    raw_duplicate_rows = sum(max(0, len(rows) - 1) for rows in raw_by_signal.values())
    raw_status_counts = Counter(
        normalized_text(row.get("observation_status")) or "unknown"
        for row in raw_rows
    )
    horizon_counts = Counter(
        str(row.get("horizon_sec") if row.get("horizon_sec") is not None else "unknown")
        for row in raw_rows
    )
    maturity_rows = [raw_outcome_maturity(row, now_ts) for row in raw_rows]
    matured_rows = [row for row, maturity in zip(raw_rows, maturity_rows) if maturity["mature"]]
    pending_rows = [row for row, maturity in zip(raw_rows, maturity_rows) if not maturity["mature"]]
    pending_reason_counts = Counter(
        reason
        for maturity in maturity_rows
        if not maturity["mature"]
        for reason in maturity["reasons"]
    )
    wick_tiers = Counter(tier_for_pct(row.get("max_wick_peak_pct"), contract) for row in matured_rows)
    sustained_tiers = Counter(tier_for_pct(row.get("max_sustained_peak_pct"), contract) for row in matured_rows)
    executable_tier_available = EXECUTABLE_QUOTE_REQUIRED_COLUMNS.issubset(raw_columns)
    executable_validations = [
        validate_executable_quote_evidence(row)
        for row in matured_rows
    ] if executable_tier_available else []
    executable_rows = [
        (row, validation)
        for row, validation in zip(matured_rows, executable_validations)
        if validation["valid"]
    ]
    executable_invalid_reason_counts = Counter(
        validation["reason"]
        for validation in executable_validations
        if not validation["valid"]
    )
    executable_tiers = Counter(
        tier_for_pct(validation["return_pct"], contract)
        for _row, validation in executable_rows
    )
    executable_coverage_rate = round_rate(len(executable_rows), len(matured_rows))

    contract_schema_pinned = (
        contract.get("contract_schema_version") == EXPECTED_CONTRACT_SCHEMA_VERSION
    )
    outcome_schema_pinned = (
        contract.get("outcome_schema_version") == EXPECTED_OUTCOME_SCHEMA_VERSION
    )
    dual_denominators_emitted = bool(signal_rows and canonical_event_keys and unique_tokens)
    raw_outcome_join_rate = round_rate(raw_joined_signal_events, len(signal_rows))
    right_censor_inputs_available = all(
        column in raw_columns
        for column in ("signal_ts", "horizon_sec", "observation_status")
    )
    right_censoring_enforced = bool(right_censor_inputs_available)
    executable_tier_calculated = bool(
        executable_tier_available
        and matured_rows
        and executable_coverage_rate is not None
        and executable_coverage_rate >= 0.99
    )

    unresolved_reasons_complete = all(bool(row.get("reason")) for row in unknown_rows)
    source_identity_coverage_rate = round_rate(source_identity_present, len(signal_rows))
    canonical_identity_coverage_rate = round_rate(canonical_identity_present, len(signal_rows))
    acceptance = {
        "identity_coverage_target": 0.99,
        "canonical_identity_coverage_rate": canonical_identity_coverage_rate,
        "source_identity_coverage_rate": source_identity_coverage_rate,
        "full_lineage_coverage_rate": round_rate(full_lineage, len(signal_rows)),
        "unknown_identity_count": len(unknown_rows),
        "unknown_identity_reasons_complete": unresolved_reasons_complete,
        "signal_id_only_is_not_source_identity": True,
        "contract_schema_pinned": contract_schema_pinned,
        "outcome_schema_pinned": outcome_schema_pinned,
        "dual_denominators_emitted": dual_denominators_emitted,
        "raw_outcome_join_target": 0.99,
        "raw_outcome_join_rate": raw_outcome_join_rate,
        "right_censor_inputs_available": right_censor_inputs_available,
        "right_censoring_enforced": right_censoring_enforced,
        "mature_outcome_count": len(matured_rows),
        "right_censored_or_pending_count": len(pending_rows),
        "executable_tier_coverage_target": 0.99,
        "executable_tier_coverage_rate": executable_coverage_rate,
        "executable_tier_calculated": executable_tier_calculated,
        "passed": bool(
            signal_rows
            and source_identity_coverage_rate is not None
            and source_identity_coverage_rate >= 0.99
            and unresolved_reasons_complete
            and contract_schema_pinned
            and outcome_schema_pinned
            and dual_denominators_emitted
            and raw_outcome_join_rate is not None
            and raw_outcome_join_rate >= 0.99
            and right_censoring_enforced
            and matured_rows
            and executable_tier_calculated
        ),
    }
    blockers = []
    warnings = []
    if not signal_rows:
        blockers.append("premium_signal_rows_unavailable")
    if exact_message_present == 0 and signal_rows:
        warnings.append("telegram_message_id_not_persisted")
    if any(identity.get("message_id_without_channel") for identity in identities):
        warnings.append("telegram_message_id_without_channel_not_exact")
    if source_identity_present < len(signal_rows):
        warnings.append("source_identity_fallback_incomplete")
    if lifecycle_present < len(signal_rows):
        warnings.append("lifecycle_identity_incomplete")
    if raw_db_path and not raw_rows:
        warnings.append("raw_outcome_join_empty")
    if duplicate_identity_rows:
        warnings.append("duplicate_canonical_identity_rows_present")
    if not executable_tier_available:
        blockers.append("executable_tier_evidence_unavailable")
    elif not executable_tier_calculated:
        blockers.append("executable_tier_evidence_incomplete")
    if raw_outcome_join_rate is None or raw_outcome_join_rate < 0.99:
        blockers.append("raw_outcome_join_below_99pct")
    if not right_censor_inputs_available:
        blockers.append("right_censoring_inputs_incomplete")
    if not matured_rows:
        blockers.append("mature_outcome_denominator_empty")
    if not contract_schema_pinned or not outcome_schema_pinned:
        blockers.append("outcome_schema_not_pinned")
    if not dual_denominators_emitted:
        blockers.append("dual_denominator_contract_incomplete")

    return {
        "schema_version": SCHEMA_VERSION,
        "outcome_schema_version": contract.get("outcome_schema_version"),
        "generated_at": utc_now_iso(),
        "snapshot_ts": now_ts,
        "window_hours": hours,
        "read_only": True,
        "evidence_level": "discovery_only",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "outcome_contract": contract,
        "inputs": {
            "signal_db": file_metadata(signal_db_path),
            "raw_db": file_metadata(raw_db_path),
            "signal_db_columns": sorted(signal_columns),
            "raw_db_columns": sorted(raw_columns),
            "signal_read_started_at": signal_read_started,
            "signal_read_finished_at": signal_read_finished,
            "raw_read_started_at": raw_read_started,
            "raw_read_finished_at": raw_read_finished,
            "cross_db_read_start_skew_ms": round(abs(raw_read_started - signal_read_started) * 1000, 3),
            "frozen_cross_db_snapshot": False,
            "watermarks": {
                "premium_signals": signal_watermark,
                "raw_signal_outcomes": raw_watermark,
            },
        },
        "identity_namespace_report": {
            "rows": len(signal_rows),
            "namespace_counts": dict(sorted(identity_counts.items())),
            "telegram_message_id_present_rate": round_rate(exact_message_present, len(signal_rows)),
            "telegram_message_id_without_channel_count": sum(
                bool(identity.get("message_id_without_channel")) for identity in identities
            ),
            "source_identity_present_rate": round_rate(source_identity_present, len(signal_rows)),
            "canonical_identity_present_rate": round_rate(canonical_identity_present, len(signal_rows)),
            "signal_id_present_rate": round_rate(
                sum(bool(identity.get("signal_id")) for identity in identities),
                len(signal_rows),
            ),
            "token_present_rate": round_rate(
                sum(bool(identity.get("token_ca")) for identity in identities),
                len(signal_rows),
            ),
            "lifecycle_present_rate": round_rate(lifecycle_present, len(signal_rows)),
            "full_message_signal_token_lifecycle_rate": round_rate(full_lineage, len(signal_rows)),
            "unknown_examples": unknown_rows[:20],
            "signal_id_only_examples": signal_id_only_rows[:20],
        },
        "denominators": {
            "signal_event": {
                "raw_rows": len(signal_rows),
                "canonical_events": len(canonical_event_keys),
                "duplicate_identity_rows": duplicate_identity_rows,
                "signal_type_counts": dict(sorted(signal_type_counts.items())),
                "repeated_token_event_rows": repeated_token_event_rows,
            },
            "unique_token": {
                "unique_tokens": len(unique_tokens),
                "tokens_with_multiple_events": sum(count > 1 for count in token_event_counts.values()),
                "max_events_per_token": max(token_event_counts.values(), default=0),
            },
            "business_primary": "unique_token",
            "timing_secondary": "signal_event",
        },
        "raw_outcome_join": {
            "signal_events_with_outcome": raw_joined_signal_events,
            "signal_events_without_outcome": max(0, len(signal_rows) - raw_joined_signal_events),
            "signal_event_join_rate": raw_outcome_join_rate,
            "raw_rows_loaded": len(raw_rows),
            "raw_event_duplicate_count": raw_duplicate_rows,
            "observation_status_counts": dict(sorted(raw_status_counts.items())),
            "horizon_sec_counts": dict(sorted(horizon_counts.items())),
            "mature_rows": len(matured_rows),
            "right_censored_or_pending_rows": len(pending_rows),
            "right_censored_or_pending_reason_counts": dict(sorted(pending_reason_counts.items())),
            "tier_counts_include_only_mature_rows": True,
            "wick_tier_counts": dict(sorted(wick_tiers.items())),
            "sustained_tier_counts": dict(sorted(sustained_tiers.items())),
            "executable_tier": {
                "available": executable_tier_available,
                "mature_rows_with_executable_label": len(executable_rows),
                "coverage_rate": executable_coverage_rate,
                "tier_counts": dict(sorted(executable_tiers.items())),
                "calculated": executable_tier_calculated,
                "evidence_schema_version": EXECUTABLE_QUOTE_EVIDENCE_SCHEMA_VERSION,
                "record_schema_version": EXECUTABLE_QUOTE_RECORD_SCHEMA_VERSION,
                "invalid_reason_counts": dict(sorted(executable_invalid_reason_counts.items())),
                "invalid_examples": [
                    {
                        "signal_id": normalized_text(row.get("signal_id")),
                        "token_ca": normalized_text(row.get("token_ca")),
                        "reason": validation["reason"],
                    }
                    for row, validation in zip(matured_rows, executable_validations)
                    if not validation["valid"]
                ][:20],
                "rule": "Require time-legal Jupiter entry and exit quote records; never infer executable tier from wick or OHLCV high.",
            },
        },
        "earliest_legal_capture_contract": contract.get("earliest_legal_capture"),
        "acceptance": acceptance,
        "blockers": blockers,
        "warnings": sorted(set(warnings)),
        "next_stage": "A3_frozen_cross_db_snapshot" if acceptance["passed"] and not blockers else "resolve_A1_input_blockers",
    }


def _self_test_executable_quote_evidence(
    signal_id: str,
    token_ca: str,
    signal_ts: int,
    horizon_sec: int,
    return_pct: float,
) -> tuple[Any, ...]:
    entry_input = 3_000_000
    token_amount = 12_345_678 + int(signal_id)
    exit_output = round(entry_input * (1 + return_pct / 100))
    route_plan_json = '[{"percent":100}]'
    route_hash = hashlib.sha256(route_plan_json.encode()).hexdigest()

    def record(
        side: str,
        input_mint: str,
        output_mint: str,
        input_amount: int,
        output_amount: int,
        captured_at_ms: int,
    ) -> dict[str, Any]:
        return {
            "schema_version": EXECUTABLE_QUOTE_RECORD_SCHEMA_VERSION,
            "side": side,
            "signal_id": signal_id,
            "token_ca": token_ca,
            "signal_ts": signal_ts,
            "horizon_sec": horizon_sec,
            "provider": "jupiter-ultra",
            "source": "shared-quote-client",
            "input_mint": input_mint,
            "output_mint": output_mint,
            "input_amount_raw": str(input_amount),
            "output_amount_raw": str(output_amount),
            "provider_request_id": f"self-test-{signal_id}-{side}",
            "route_plan_json": route_plan_json,
            "route_plan_sha256": route_hash,
            "route_plan_hop_count": 1,
            "provider_fetched_at_ms": captured_at_ms,
            "captured_at_ms": captured_at_ms,
            "executable": True,
        }

    entry = record(
        "entry", SOL_MINT, token_ca, entry_input, token_amount, (signal_ts + 60) * 1000,
    )
    exit_quote = record(
        "exit", token_ca, SOL_MINT, token_amount, exit_output,
        (signal_ts + horizon_sec + 60) * 1000,
    )
    exact_return = ((exit_output / entry_input) - 1) * 100
    return (
        EXECUTABLE_QUOTE_EVIDENCE_SCHEMA_VERSION,
        0.003,
        json.dumps(entry, sort_keys=True),
        json.dumps(exit_quote, sort_keys=True),
        exact_return,
        "complete",
    )


def self_test() -> None:
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        signal_db = root / "signals.db"
        raw_db = root / "raw.db"
        signal = sqlite3.connect(signal_db)
        signal.execute(
            """
            CREATE TABLE premium_signals(
              id INTEGER PRIMARY KEY,
              message_id TEXT,
              channel_id TEXT,
              token_ca TEXT,
              timestamp INTEGER,
              source_message_ts INTEGER,
              signal_type TEXT,
              is_ath INTEGER,
              source_event_id TEXT,
              downstream_lifecycle_id TEXT
            )
            """
        )
        now = int(time.time())
        signal.executemany(
            "INSERT INTO premium_signals VALUES (?,?,?,?,?,?,?,?,?,?)",
            [
                (1, "100", "chan", "TOKEN_A", now - 8300, now - 8300, "NEW_TRENDING", 0, "evt-1", "life-a"),
                (2, "101", "chan", "TOKEN_A", now - 8200, now - 8200, "ATH", 1, "evt-2", "life-a"),
                (3, "102", "chan", "TOKEN_B", now - 8100, now - 8100, "ATH", 1, "evt-3", "life-b"),
            ],
        )
        signal.commit()
        signal.close()

        raw = sqlite3.connect(raw_db)
        raw.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              id INTEGER PRIMARY KEY,
              signal_id TEXT,
              token_ca TEXT,
              signal_ts INTEGER,
              observation_status TEXT,
              right_censored INTEGER,
              horizon_sec INTEGER,
              max_wick_peak_pct REAL,
              max_sustained_peak_pct REAL,
              executable_quote_evidence_version TEXT,
              executable_quote_size_sol REAL,
              executable_entry_quote_json TEXT,
              executable_exit_quote_json TEXT,
              executable_quote_return_pct REAL,
              executable_quote_evidence_status TEXT,
              updated_at INTEGER
            )
            """
        )
        raw.executemany(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (1, "1", "TOKEN_A", now - 8300, "matured", 0, 7200, 1200.0, 950.0,
                 *_self_test_executable_quote_evidence("1", "TOKEN_A", now - 8300, 7200, 900.0), now),
                (2, "2", "TOKEN_A", now - 8200, "matured", 0, 7200, 60.0, 55.0,
                 *_self_test_executable_quote_evidence("2", "TOKEN_A", now - 8200, 7200, 52.0), now),
                (3, "3", "TOKEN_B", now - 8100, "matured", 0, 7200, 30.0, 27.0,
                 *_self_test_executable_quote_evidence("3", "TOKEN_B", now - 8100, 7200, 26.0), now),
            ],
        )
        raw.commit()
        raw.close()

        report = build_audit(
            signal_db_path=str(signal_db),
            raw_db_path=str(raw_db),
            contract_path=str(DEFAULT_CONTRACT),
            hours=24,
            now_ts=now,
            limit=100,
        )
        assert report["identity_namespace_report"]["namespace_counts"]["telegram_message_id"] == 3
        assert report["denominators"]["signal_event"]["canonical_events"] == 3
        assert report["denominators"]["unique_token"]["unique_tokens"] == 2
        assert report["denominators"]["unique_token"]["tokens_with_multiple_events"] == 1
        assert report["raw_outcome_join"]["signal_event_join_rate"] == 1.0
        assert report["raw_outcome_join"]["sustained_tier_counts"]["10x"] == 1
        assert report["raw_outcome_join"]["executable_tier"]["available"] is True
        assert report["raw_outcome_join"]["executable_tier"]["calculated"] is True
        assert report["acceptance"]["passed"] is True

        signal_only_db = root / "signal-only.db"
        signal_only = sqlite3.connect(signal_only_db)
        signal_only.execute(
            """
            CREATE TABLE premium_signals(
              id INTEGER PRIMARY KEY,
              token_ca TEXT,
              timestamp INTEGER,
              signal_type TEXT
            )
            """
        )
        signal_only.execute(
            "INSERT INTO premium_signals VALUES (?,?,?,?)",
            (1, "TOKEN_C", now - 10, "ATH"),
        )
        signal_only.commit()
        signal_only.close()
        signal_only_report = build_audit(
            signal_db_path=str(signal_only_db),
            raw_db_path=None,
            contract_path=str(DEFAULT_CONTRACT),
            hours=24,
            now_ts=now,
            limit=100,
        )
        assert signal_only_report["identity_namespace_report"]["namespace_counts"]["token_source_ts_signal_type"] == 1
        assert signal_only_report["acceptance"]["passed"] is False
    print("SELF_TEST_PASS telegram_signal_identity_audit")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--signal-db", default="/app/data/sentiment_arb.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--contract", default=str(DEFAULT_CONTRACT))
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--now-ts", type=int, default=0)
    parser.add_argument("--limit", type=int, default=20000)
    parser.add_argument("--out")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    if not args.out:
        raise SystemExit("--out is required")
    report = build_audit(
        signal_db_path=args.signal_db,
        raw_db_path=args.raw_db,
        contract_path=args.contract,
        hours=max(1, int(args.hours)),
        now_ts=int(args.now_ts or time.time()),
        limit=max(1, int(args.limit)),
    )
    write_json(args.out, report)
    print(json.dumps({
        "schema_version": report["schema_version"],
        "out": str(args.out),
        "canonical_events": report["denominators"]["signal_event"]["canonical_events"],
        "unique_tokens": report["denominators"]["unique_token"]["unique_tokens"],
        "identity_acceptance_passed": report["acceptance"]["passed"],
        "promotion_allowed": False,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
