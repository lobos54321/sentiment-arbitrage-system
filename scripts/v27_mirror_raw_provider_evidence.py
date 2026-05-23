#!/usr/bin/env python3
"""Mirror raw provider request/response evidence into v2.7 events."""

import argparse
import fcntl
import json
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, V27EventLogError, sha256_hex  # noqa: E402
from v27_mirror_realtime_clean import _nested_get, _parse_json_object, _quote_ts_seconds, _row_dict  # noqa: E402
from v27_mirror_trade_outcomes import _as_float, _as_int, _signal_context, _timestamp_to_iso  # noqa: E402


DEFAULT_PAPER_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_SIGNAL_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"
DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_raw_provider_evidence_mirror.lock")
RAW_PROVIDER_EVIDENCE_EVENT_TYPE = "raw_provider_evidence_recorded"
DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION = "legacy_paper_raw_provider_evidence_v0.1"
DEFAULT_PROVIDER = "jupiter_ultra"
DEFAULT_ENDPOINT = "/ultra/v1/order"
DEFAULT_CURSOR_OVERLAP_IDS = 100
REQUIRED_PAPER_COLUMNS = {"id", "token_ca"}
SIDES = {
    "entry": {
        "audit_field": "entry_execution_audit_json",
        "execution_field": "entry_execution_json",
        "default_side": "buy",
    },
    "exit": {
        "audit_field": "exit_execution_audit_json",
        "execution_field": "exit_execution_json",
        "default_side": "sell",
    },
}


def _connect(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    return db


def _table_exists(db, table):
    row = db.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone()
    return row is not None


def _table_columns(db, table):
    if not _table_exists(db, table):
        return set()
    return {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}


def _paper_trade_schema_error(db, table):
    if not _table_exists(db, table):
        return f"{table} table missing"
    missing = sorted(REQUIRED_PAPER_COLUMNS - _table_columns(db, table))
    if missing:
        return f"{table} missing required columns: {', '.join(missing)}"
    return None


def _row_filters(since_id=None, until_id=None):
    clauses = []
    params = []
    if since_id is not None:
        clauses.append("id >= ?")
        params.append(since_id)
    if until_id is not None:
        clauses.append("id <= ?")
        params.append(until_id)
    return f" WHERE {' AND '.join(clauses)}" if clauses else "", params


def iter_paper_trade_rows(db, *, since_id=None, until_id=None, limit=None, table="paper_trades"):
    if _paper_trade_schema_error(db, table):
        return
    where, params = _row_filters(since_id=since_id, until_id=until_id)
    sql = f"SELECT * FROM {table}{where} ORDER BY id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    yield from db.execute(sql, params)


def _first_value(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def _text(value, default=None):
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def _available_row_value(row, key):
    return row.get(key) if key in row else None


def _provider_from_material(audit, execution, default_provider=DEFAULT_PROVIDER):
    return _text(
        _first_value(
            audit.get("provider"),
            audit.get("providerName"),
            audit.get("quoteProvider"),
            execution.get("provider"),
            execution.get("providerName"),
            execution.get("quoteProvider"),
            execution.get("executor"),
            default_provider,
        )
    )


def _endpoint_from_material(audit, execution, default_endpoint=DEFAULT_ENDPOINT):
    return _text(
        _first_value(
            audit.get("endpoint"),
            audit.get("requestEndpoint"),
            audit.get("providerEndpoint"),
            execution.get("endpoint"),
            execution.get("requestEndpoint"),
            execution.get("providerEndpoint"),
            default_endpoint,
        )
    )


def _request_id_from_material(audit, execution):
    return _text(
        _first_value(
            execution.get("requestId"),
            execution.get("providerRequestId"),
            execution.get("quoteRequestId"),
            _nested_get(execution, ("_rawOrder", "requestId")),
            _nested_get(execution, ("rawResponse", "requestId")),
            _nested_get(execution, ("providerResponse", "requestId")),
            audit.get("requestId"),
            audit.get("providerRequestId"),
            audit.get("quoteRequestId"),
        )
    )


def _latency_ms_from_material(audit, execution):
    for value in (
        audit.get("latencyMs"),
        audit.get("providerLatencyMs"),
        audit.get("quoteLatencyMs"),
        execution.get("latencyMs"),
        execution.get("providerLatencyMs"),
        execution.get("quoteLatencyMs"),
        _nested_get(audit, ("entryLatencyAudit", "signal_to_quote_latency_ms")),
        _nested_get(execution, ("entryLatencyAudit", "signal_to_quote_latency_ms")),
    ):
        parsed = _as_float(value)
        if parsed is not None and parsed >= 0:
            return parsed
    return 0.0


def _raw_response_material(audit, execution):
    candidates = [
        ("execution._rawOrder", execution.get("_rawOrder")),
        ("execution.rawResponse", execution.get("rawResponse")),
        ("execution.raw_response", execution.get("raw_response")),
        ("execution.providerResponse", execution.get("providerResponse")),
        ("execution.provider_response", execution.get("provider_response")),
        ("audit.rawResponse", audit.get("rawResponse")),
        ("audit.raw_response", audit.get("raw_response")),
        ("audit.providerResponse", audit.get("providerResponse")),
        ("audit.provider_response", audit.get("provider_response")),
    ]
    for material_type, material in candidates:
        if material is None:
            continue
        if isinstance(material, str) and not material.strip():
            continue
        return material, material_type, True
    if execution:
        return execution, "execution_json_projection", False
    if audit:
        return audit, "execution_audit_projection", False
    return {}, "missing_response_material", False


def _request_parameters(row, audit, execution, side, default_side):
    token_ca = row.get("token_ca")
    return {
        "paper_trade_id": row.get("id"),
        "side": _text(_first_value(audit.get("side"), execution.get("side"), default_side)),
        "token_ca": token_ca,
        "input_mint": _first_value(audit.get("inputMint"), execution.get("inputMint")),
        "output_mint": _first_value(audit.get("outputMint"), execution.get("outputMint"), token_ca if side == "entry" else "SOL"),
        "input_amount": _first_value(audit.get("inputAmount"), execution.get("inputAmount"), row.get("position_size_sol")),
        "input_amount_raw": _first_value(audit.get("inputAmountRaw"), execution.get("inputAmountRaw")),
        "quoted_out_amount": _first_value(audit.get("quotedOutAmount"), execution.get("quotedOutAmount")),
        "quoted_out_amount_raw": _first_value(audit.get("quotedOutAmountRaw"), execution.get("quotedOutAmountRaw")),
        "route": _first_value(audit.get("route"), audit.get("routeId"), execution.get("route"), execution.get("routeId"), row.get("signal_route"), row.get("entry_mode")),
        "pool": _first_value(audit.get("pool"), audit.get("poolAddress"), execution.get("pool"), execution.get("poolAddress"), row.get("lifecycle_id")),
        "slippage_bps": _first_value(audit.get("slippageBps"), execution.get("slippageBps")),
        "quote_ts": _quote_ts_seconds(_first_value(audit.get("quoteTs"), execution.get("quoteTs"), row.get("entry_ts" if side == "entry" else "exit_ts"))),
    }


def _raw_provider_evidence_payload(
    row,
    signal_context,
    *,
    side,
    evidence_version=DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION,
    default_provider=DEFAULT_PROVIDER,
    default_endpoint=DEFAULT_ENDPOINT,
):
    side_config = SIDES[side]
    audit = _parse_json_object(_available_row_value(row, side_config["audit_field"]))
    execution = _parse_json_object(_available_row_value(row, side_config["execution_field"]))
    if not audit and not execution:
        return None

    provider = _provider_from_material(audit, execution, default_provider=default_provider)
    endpoint = _endpoint_from_material(audit, execution, default_endpoint=default_endpoint)
    request_id = _request_id_from_material(audit, execution)
    latency_ms = _latency_ms_from_material(audit, execution)
    request_parameters = _request_parameters(row, audit, execution, side, side_config["default_side"])
    request_metadata = {
        "raw_provider_evidence_version": evidence_version,
        "paper_trade_id": row.get("id"),
        "side": side,
        "provider": provider,
        "endpoint": endpoint,
        "request_id": request_id,
        "request_parameters": request_parameters,
    }
    response_material, response_material_type, raw_response_available = _raw_response_material(audit, execution)
    request_hash = sha256_hex(request_metadata)
    response_hash = sha256_hex(response_material)
    request_metadata_hash = sha256_hex(request_metadata)
    raw_response_hash = response_hash if raw_response_available else None
    request_metadata_available = bool(request_parameters)
    provider_evidence_trusted = bool(
        provider
        and endpoint
        and request_id
        and request_metadata_available
        and raw_response_available
        and response_hash
        and latency_ms is not None
        and latency_ms >= 0
    )
    observed_ts = _first_value(request_parameters.get("quote_ts"), row.get("entry_ts" if side == "entry" else "exit_ts"), row.get("signal_ts"))
    observed_at = _timestamp_to_iso(observed_ts)
    chain = signal_context.get("chain") or row.get("chain") or "solana"
    pool = signal_context.get("canonical_pool_group") or request_parameters.get("pool") or row.get("lifecycle_id") or "unknown_pool"
    lifecycle_epoch = signal_context.get("lifecycle_epoch") or row.get("lifecycle_epoch") or 0
    proof_level = (
        "provider_request_id_with_raw_response_hash"
        if provider_evidence_trusted
        else "legacy_execution_projection_without_raw_provider_response"
        if request_id
        else "legacy_execution_projection_without_provider_request_id"
    )
    return {
        "paper_trade_id": row.get("id"),
        "premium_signal_id": signal_context.get("premium_signal_id"),
        "telegram_signal_id": signal_context.get("telegram_signal_id"),
        "token_ca": row.get("token_ca"),
        "symbol": row.get("symbol"),
        "chain": chain,
        "canonical_pool_group": pool,
        "lifecycle_epoch": lifecycle_epoch,
        "raw_provider_evidence_version": evidence_version,
        "provider_evidence_version": evidence_version,
        "provider": provider,
        "endpoint": endpoint,
        "request_id": request_id,
        "provider_request_id": request_id,
        "side": side,
        "latency_ms": latency_ms,
        "request_parameters": request_parameters,
        "request_metadata": request_metadata,
        "request_metadata_available": request_metadata_available,
        "request_metadata_hash": request_metadata_hash,
        "request_hash": request_hash,
        "response_hash": response_hash,
        "raw_response_hash": raw_response_hash,
        "raw_response_available": raw_response_available,
        "response_material_type": response_material_type,
        "hash_algorithm": "sha256(canonical_json)",
        "evidence_source": f"paper_trades.{side_config['execution_field']}+{side_config['audit_field']}",
        "provider_evidence_proof_level": proof_level,
        "provider_evidence_trusted": provider_evidence_trusted,
        "decision_available_at": observed_at,
        "observed_at": observed_at,
        "audit_hash": sha256_hex(audit) if audit else None,
        "execution_hash": sha256_hex(execution) if execution else None,
        "legacy_execution_audit": audit,
        "legacy_execution": execution,
    }


def mirror_raw_provider_evidence(
    paper_db_path,
    signal_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    dry_run=False,
    table="paper_trades",
    signal_table="premium_signals",
    default_chain="solana",
    evidence_version=DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION,
    default_provider=DEFAULT_PROVIDER,
    default_endpoint=DEFAULT_ENDPOINT,
    trusted_only=False,
):
    summary = {
        "paper_db": str(paper_db_path),
        "signal_db": str(signal_db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "read_rows": 0,
        "candidate_provider_evidence": 0,
        "trusted_provider_evidence": 0,
        "skipped_untrusted_provider_evidence": 0,
        "appended": 0,
        "duplicate": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
        "failures": [],
        "raw_provider_evidence_version": evidence_version,
        "trusted_only": bool(trusted_only),
    }
    with _connect(paper_db_path) as paper_db, _connect(signal_db_path) as signal_db:
        schema_error = _paper_trade_schema_error(paper_db, table)
        if schema_error:
            summary["failed"] += 1
            summary["failures"].append({"reason": schema_error})
            return summary
        for row in iter_paper_trade_rows(paper_db, since_id=since_id, until_id=until_id, limit=limit, table=table):
            row = _row_dict(row)
            summary["read_rows"] += 1
            try:
                signal_context = _signal_context(signal_db, row, signal_table=signal_table, default_chain=default_chain)
                payloads = [
                    _raw_provider_evidence_payload(
                        row,
                        signal_context,
                        side=side,
                        evidence_version=evidence_version,
                        default_provider=default_provider,
                        default_endpoint=default_endpoint,
                    )
                    for side in SIDES
                ]
                payloads = [payload for payload in payloads if payload]
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"paper_trade_id": row.get("id"), "reason": str(exc)})
                continue
            if not payloads:
                continue
            summary["candidate_provider_evidence"] += len(payloads)
            trusted_payloads = [payload for payload in payloads if payload.get("provider_evidence_trusted") is True]
            summary["trusted_provider_evidence"] += len(trusted_payloads)
            if trusted_only:
                summary["skipped_untrusted_provider_evidence"] += len(payloads) - len(trusted_payloads)
                payloads = trusted_payloads
                if not payloads:
                    continue
            if dry_run:
                continue
            for payload in payloads:
                try:
                    aggregate_id = ":".join(
                        [
                            "raw_provider_evidence",
                            str(payload.get("chain") or "unknown_chain"),
                            str(payload.get("token_ca")),
                            str(payload.get("canonical_pool_group") or "unknown_pool"),
                            str(payload.get("lifecycle_epoch", 0)),
                            str(payload.get("paper_trade_id")),
                            str(payload.get("side")),
                        ]
                    )
                    result = V27EventLog(event_log_dir).append_event(
                        event_type=RAW_PROVIDER_EVIDENCE_EVENT_TYPE,
                        aggregate_id=aggregate_id,
                        payload=payload,
                        source="paper_trades",
                        idempotency_key=f"raw_provider_evidence:{row.get('id')}:{payload.get('side')}:{evidence_version}",
                        observed_at=payload.get("observed_at"),
                        available_at=payload.get("decision_available_at") or payload.get("observed_at"),
                    )
                except Exception as exc:
                    summary["failed"] += 1
                    summary["failures"].append({"paper_trade_id": row.get("id"), "side": payload.get("side"), "reason": str(exc)})
                    continue
                status = result.get("status")
                if status == "appended":
                    summary["appended"] += 1
                elif status == "duplicate":
                    summary["duplicate"] += 1
                else:
                    summary["failed"] += 1
                    summary["failures"].append({"paper_trade_id": row.get("id"), "side": payload.get("side"), "reason": f"unexpected status {status}"})
    return summary


def _paper_trade_provider_keys(
    db,
    *,
    signal_db=None,
    since_id=None,
    until_id=None,
    limit=None,
    table="paper_trades",
    signal_table="premium_signals",
    default_chain="solana",
    evidence_version=DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION,
    default_provider=DEFAULT_PROVIDER,
    default_endpoint=DEFAULT_ENDPOINT,
    trusted_only=False,
):
    keys = []
    for row in iter_paper_trade_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table):
        row = _row_dict(row)
        signal_context = (
            _signal_context(signal_db, row, signal_table=signal_table, default_chain=default_chain)
            if signal_db is not None
            else {}
        )
        for side, config in SIDES.items():
            audit = _parse_json_object(_available_row_value(row, config["audit_field"]))
            execution = _parse_json_object(_available_row_value(row, config["execution_field"]))
            if audit or execution:
                if trusted_only:
                    payload = _raw_provider_evidence_payload(
                        row,
                        signal_context,
                        side=side,
                        evidence_version=evidence_version,
                        default_provider=default_provider,
                        default_endpoint=default_endpoint,
                    )
                    if not payload or payload.get("provider_evidence_trusted") is not True:
                        continue
                keys.append((int(row["id"]), side))
    return keys


def _mirrored_raw_provider_keys(event_log_dir, *, evidence_version=None, trusted_only=False):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != RAW_PROVIDER_EVIDENCE_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        if evidence_version and payload.get("raw_provider_evidence_version") != evidence_version:
            continue
        if trusted_only and payload.get("provider_evidence_trusted") is not True:
            continue
        paper_trade_id = payload.get("paper_trade_id")
        side = payload.get("side")
        if paper_trade_id is None or side is None:
            continue
        key = (int(paper_trade_id), str(side))
        counts[key] = counts.get(key, 0) + 1
    return counts


def max_mirrored_raw_provider_evidence_id(event_log_dir, *, evidence_version=DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION):
    mirrored = _mirrored_raw_provider_keys(event_log_dir, evidence_version=evidence_version)
    return max([trade_id for trade_id, _side in mirrored] or [None])


def next_unmirrored_raw_provider_since_id(event_log_dir, configured_since_id=None, *, evidence_version=DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION, cursor_overlap_ids=DEFAULT_CURSOR_OVERLAP_IDS):
    max_id = max_mirrored_raw_provider_evidence_id(event_log_dir, evidence_version=evidence_version)
    if max_id is None:
        return configured_since_id
    next_id = max(1, int(max_id) - max(0, int(cursor_overlap_ids)) + 1)
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def verify_raw_provider_evidence_mirror_parity(
    paper_db_path,
    event_log_dir,
    *,
    signal_db_path=None,
    since_id=None,
    until_id=None,
    limit=None,
    table="paper_trades",
    signal_table="premium_signals",
    default_chain="solana",
    evidence_version=DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION,
    default_provider=DEFAULT_PROVIDER,
    default_endpoint=DEFAULT_ENDPOINT,
    trusted_only=False,
):
    summary = {
        "paper_db": str(paper_db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "db_provider_evidence": 0,
        "mirrored_events": 0,
        "missing_provider_evidence": [],
        "duplicate_provider_evidence": [],
        "orphan_mirrored_provider_evidence": [],
        "event_log_verify": None,
        "event_log_error": None,
        "parity_ok": False,
        "raw_provider_evidence_version": evidence_version,
        "trusted_only": bool(trusted_only),
    }
    with _connect(paper_db_path) as db:
        if trusted_only and signal_db_path:
            with _connect(signal_db_path) as signal_db:
                db_keys = _paper_trade_provider_keys(
                    db,
                    signal_db=signal_db,
                    since_id=since_id,
                    until_id=until_id,
                    limit=limit,
                    table=table,
                    signal_table=signal_table,
                    default_chain=default_chain,
                    evidence_version=evidence_version,
                    default_provider=default_provider,
                    default_endpoint=default_endpoint,
                    trusted_only=True,
                )
        else:
            db_keys = _paper_trade_provider_keys(
                db,
                since_id=since_id,
                until_id=until_id,
                limit=limit,
                table=table,
                evidence_version=evidence_version,
                default_provider=default_provider,
                default_endpoint=default_endpoint,
                trusted_only=trusted_only,
            )
    mirrored_counts = _mirrored_raw_provider_keys(
        event_log_dir,
        evidence_version=evidence_version,
        trusted_only=trusted_only,
    )
    db_key_set = set(db_keys)
    mirrored_key_set = set(mirrored_counts)
    scoped = since_id is not None or until_id is not None or limit is not None
    summary["db_provider_evidence"] = len(db_keys)
    summary["mirrored_events"] = sum(mirrored_counts.get(key, 0) for key in db_key_set) if scoped else sum(mirrored_counts.values())
    summary["missing_provider_evidence"] = [
        {"paper_trade_id": trade_id, "side": side}
        for trade_id, side in sorted(db_key_set - mirrored_key_set)
    ]
    summary["duplicate_provider_evidence"] = [
        {"paper_trade_id": trade_id, "side": side}
        for trade_id, side in sorted(key for key in db_key_set if mirrored_counts.get(key, 0) > 1)
    ]
    summary["orphan_mirrored_provider_evidence"] = [] if scoped else [
        {"paper_trade_id": trade_id, "side": side}
        for trade_id, side in sorted(mirrored_key_set - db_key_set)
    ]
    try:
        summary["event_log_verify"] = V27EventLog(event_log_dir).verify()
    except V27EventLogError as exc:
        summary["event_log_error"] = str(exc)
    summary["parity_ok"] = (
        not summary["missing_provider_evidence"]
        and not summary["duplicate_provider_evidence"]
        and summary["event_log_error"] is None
    )
    return summary


def acquire_loop_lock(lock_file):
    lock_file = Path(lock_file)
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fh = lock_file.open("a+", encoding="utf-8")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fh.close()
        return None
    return fh


def run_mirror_once(args):
    since_id = args.since_id
    if getattr(args, "new_only", False):
        since_id = next_unmirrored_raw_provider_since_id(
            args.event_log_dir,
            configured_since_id=since_id,
            evidence_version=args.evidence_version,
            cursor_overlap_ids=args.cursor_overlap_ids,
        )
    mirror_summary = mirror_raw_provider_evidence(
        args.paper_db,
        args.signal_db,
        args.event_log_dir,
        since_id=since_id,
        until_id=args.until_id,
        limit=args.limit,
        dry_run=args.dry_run,
        table=args.table,
        signal_table=args.signal_table,
        default_chain=args.default_chain,
        evidence_version=args.evidence_version,
        default_provider=args.default_provider,
        default_endpoint=args.default_endpoint,
        trusted_only=getattr(args, "trusted_only", False),
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_raw_provider_evidence_mirror_parity(
            args.paper_db,
            args.event_log_dir,
            signal_db_path=args.signal_db,
            since_id=since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
            signal_table=args.signal_table,
            default_chain=args.default_chain,
            evidence_version=args.evidence_version,
            default_provider=args.default_provider,
            default_endpoint=args.default_endpoint,
            trusted_only=getattr(args, "trusted_only", False),
        )
    return {
        "cursor": {
            "new_only": bool(getattr(args, "new_only", False)),
            "since_id": since_id,
            "until_id": args.until_id,
            "limit": args.limit,
            "cursor_overlap_ids": args.cursor_overlap_ids,
            "max_mirrored_paper_trade_id": max_mirrored_raw_provider_evidence_id(
                args.event_log_dir,
                evidence_version=args.evidence_version,
            ),
        },
        "mirror": mirror_summary,
        "verify": verify_summary,
    }


def _install_signal_handlers(stop):
    def _handler(_signum, _frame):
        stop["stop"] = True

    signal.signal(signal.SIGTERM, _handler)
    signal.signal(signal.SIGINT, _handler)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-db", default=str(DEFAULT_PAPER_DB))
    parser.add_argument("--signal-db", default=str(DEFAULT_SIGNAL_DB))
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--since-id", type=int)
    parser.add_argument("--until-id", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--table", default="paper_trades")
    parser.add_argument("--signal-table", default="premium_signals")
    parser.add_argument("--default-chain", default="solana")
    parser.add_argument("--evidence-version", default=os.environ.get("V27_RAW_PROVIDER_EVIDENCE_VERSION", DEFAULT_RAW_PROVIDER_EVIDENCE_VERSION))
    parser.add_argument("--default-provider", default=os.environ.get("V27_RAW_PROVIDER_DEFAULT_PROVIDER", DEFAULT_PROVIDER))
    parser.add_argument("--default-endpoint", default=os.environ.get("V27_RAW_PROVIDER_DEFAULT_ENDPOINT", DEFAULT_ENDPOINT))
    parser.add_argument("--cursor-overlap-ids", type=int, default=int(os.environ.get("V27_RAW_PROVIDER_CURSOR_OVERLAP_IDS", DEFAULT_CURSOR_OVERLAP_IDS)))
    parser.add_argument("--trusted-only", action="store_true")
    parser.add_argument("--new-only", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--initial-delay", type=float, default=0.0)
    parser.add_argument("--lock-file", default=str(DEFAULT_LOCK_FILE))
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    lock_fh = acquire_loop_lock(args.lock_file)
    if lock_fh is None:
        print(json.dumps({"status": "lock_busy", "lock_file": args.lock_file}, sort_keys=True))
        return
    stop = {"stop": False}
    _install_signal_handlers(stop)
    try:
        if args.initial_delay > 0:
            time.sleep(args.initial_delay)
        while True:
            result = run_mirror_once(args)
            print(json.dumps(result, ensure_ascii=False, sort_keys=True))
            sys.stdout.flush()
            failed = result.get("mirror", {}).get("failed", 0)
            failed += 0 if (result.get("verify") or {}).get("parity_ok", True) else 1
            if args.strict and failed:
                raise SystemExit(1)
            if not args.loop or stop["stop"]:
                break
            time.sleep(max(5.0, args.interval))
    finally:
        try:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
        finally:
            lock_fh.close()


if __name__ == "__main__":
    main()
