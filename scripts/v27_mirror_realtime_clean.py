#!/usr/bin/env python3
"""Mirror realtime clean detector evidence into v2.7 events."""

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

from v27_event_log import V27EventLog, V27EventLogError  # noqa: E402
from v27_mirror_trade_outcomes import _as_float, _as_int, _signal_context, _timestamp_to_iso  # noqa: E402


DEFAULT_PAPER_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_SIGNAL_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"
DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_realtime_clean_mirror.lock")
REALTIME_CLEAN_EVENT_TYPE = "realtime_clean_detector_recorded"
REQUIRED_PAPER_COLUMNS = {
    "id",
    "token_ca",
    "entry_price",
    "entry_ts",
    "exit_price",
    "exit_ts",
}
DEFAULT_CLEAN_STANDARD_VERSION = "legacy_round_trip_quote_clean_v0.1"
DEFAULT_QUOTE_SOURCE = "paper_trade_round_trip_quote"


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


def _row_dict(row):
    return {key: row[key] for key in row.keys()}


def _paper_trade_schema_error(db, table):
    if not _table_exists(db, table):
        return f"{table} table missing"
    missing = sorted(REQUIRED_PAPER_COLUMNS - _table_columns(db, table))
    if missing:
        return f"{table} missing required columns: {', '.join(missing)}"
    return None


def _row_filters(since_id=None, until_id=None):
    clauses = [
        "entry_price IS NOT NULL",
        "entry_price > 0",
        "entry_ts IS NOT NULL",
        "exit_price IS NOT NULL",
        "exit_ts IS NOT NULL",
    ]
    params = []
    if since_id is not None:
        clauses.append("id >= ?")
        params.append(since_id)
    if until_id is not None:
        clauses.append("id <= ?")
        params.append(until_id)
    return f" WHERE {' AND '.join(clauses)}", params


def iter_paper_trade_rows(db, *, since_id=None, until_id=None, limit=None, table="paper_trades"):
    if _paper_trade_schema_error(db, table):
        return
    where, params = _row_filters(since_id=since_id, until_id=until_id)
    sql = f"SELECT * FROM {table}{where} ORDER BY id"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    yield from db.execute(sql, params)


def _parse_json_object(value):
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
    except Exception as exc:
        return {
            "_raw_json": value,
            "_json_parse_error": str(exc),
        }
    if isinstance(parsed, dict):
        return parsed
    return {
        "_raw_json": value,
        "_json_parse_error": "json value is not an object",
    }


def _nested_get(mapping, path, default=None):
    cursor = mapping
    for key in path:
        if not isinstance(cursor, dict) or key not in cursor:
            return default
        cursor = cursor.get(key)
    return cursor


def _quote_ts_seconds(value):
    if value is None:
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    return ts / 1000.0 if ts > 1_000_000_000_000 else ts


def _clean_quote_available(audit):
    return bool(
        audit.get("success") is True
        and _as_float(audit.get("effectivePrice")) is not None
        and _quote_ts_seconds(audit.get("quoteTs")) is not None
    )


def _quote_age_sec_from_entry_audit(audit, row):
    latency = _nested_get(audit, ("entryLatencyAudit", "signal_to_quote_latency_ms"))
    latency_sec = _as_float(latency)
    if latency_sec is not None:
        return max(0.0, latency_sec / 1000.0)
    quote_ts = _quote_ts_seconds(audit.get("quoteTs"))
    signal_ts = _quote_ts_seconds(row.get("signal_ts"))
    if quote_ts is not None and signal_ts is not None:
        return max(0.0, quote_ts - signal_ts)
    return None


def _quote_age_sec_from_exit_audit(audit):
    freshness = audit.get("quoteFreshness") if isinstance(audit.get("quoteFreshness"), dict) else {}
    quote_age = _as_float(freshness.get("quote_age_sec"))
    if quote_age is not None:
        return max(0.0, quote_age)
    quote_ts = _quote_ts_seconds(freshness.get("quote_ts") or audit.get("quoteTs"))
    now_ts = _quote_ts_seconds(freshness.get("now_ts"))
    if quote_ts is not None and now_ts is not None:
        return max(0.0, now_ts - quote_ts)
    return None


def _realtime_clean_payload(row, signal_context, *, clean_standard_version=DEFAULT_CLEAN_STANDARD_VERSION, quote_source=DEFAULT_QUOTE_SOURCE):
    entry_audit = _parse_json_object(row.get("entry_execution_audit_json"))
    exit_audit = _parse_json_object(row.get("exit_execution_audit_json"))
    monitor_state = _parse_json_object(row.get("monitor_state_json"))
    execution_availability = str(row.get("execution_availability") or "available").strip().lower()
    execution_available = execution_availability != "unavailable"
    entry_quote_audit_available = _clean_quote_available(entry_audit)
    exit_quote_audit_available = _clean_quote_available(exit_audit)
    entry_quote_ts = _quote_ts_seconds(entry_audit.get("quoteTs")) or _quote_ts_seconds(row.get("entry_ts"))
    exit_quote_ts = _quote_ts_seconds(exit_audit.get("quoteTs")) or _quote_ts_seconds(row.get("exit_ts"))
    entry_quote_price = _as_float(entry_audit.get("effectivePrice") or row.get("entry_price"))
    exit_quote_price = _as_float(exit_audit.get("effectivePrice") or row.get("exit_price"))
    entry_quote_available = bool(
        entry_quote_audit_available
        or (execution_available and entry_quote_price is not None and entry_quote_price > 0 and entry_quote_ts is not None)
    )
    exit_quote_available = bool(
        exit_quote_audit_available
        or (execution_available and exit_quote_price is not None and exit_quote_price > 0 and exit_quote_ts is not None)
    )
    entry_quote_age_sec = _quote_age_sec_from_entry_audit(entry_audit, row)
    if entry_quote_age_sec is None and entry_quote_available:
        entry_quote_age_sec = 0.0
    exit_quote_age_sec = _quote_age_sec_from_exit_audit(exit_audit)
    if exit_quote_age_sec is None and exit_quote_available:
        exit_quote_age_sec = 0.0
    quote_age_sec = max(
        [value for value in (entry_quote_age_sec, exit_quote_age_sec) if value is not None],
        default=None,
    )
    if quote_age_sec is None:
        quote_age_sec = 0.0
    entry_slippage_bps = _as_float(entry_audit.get("slippageBps"))
    exit_slippage_bps = _as_float(exit_audit.get("slippageBps"))
    slippage_bps = max(
        [abs(value) for value in (entry_slippage_bps, exit_slippage_bps) if value is not None],
        default=None,
    )
    quote_mint = (
        entry_audit.get("inputMint")
        or exit_audit.get("outputMint")
        or row.get("symbol")
        or "unknown_quote_mint"
    )
    decision_available_at = max(
        [value for value in (entry_quote_ts, exit_quote_ts, _quote_ts_seconds(row.get("entry_ts")), _quote_ts_seconds(row.get("exit_ts"))) if value is not None],
        default=None,
    )
    decision_available_at_iso = _timestamp_to_iso(decision_available_at)
    quote_source_used = quote_source if entry_quote_audit_available and exit_quote_audit_available else "legacy_paper_trade_entry_exit_price_proxy"
    clean_observation_type = (
        "TRADABLE_CLEAN_OBSERVED"
        if entry_quote_available and exit_quote_available and execution_available
        else "QUOTE_DIRTY_OBSERVED"
    )
    realtime_clean = clean_observation_type == "TRADABLE_CLEAN_OBSERVED"
    liquidity_usd = _nested_get(monitor_state, ("entryExecutionEligibility", "observed", "liquidity_usd"))
    if liquidity_usd is None:
        liquidity_usd = _nested_get(monitor_state, ("entryExecutionEligibility", "observed", "liquidity_usd"))
    route = row.get("signal_route") or row.get("entry_mode") or signal_context.get("canonical_pool_group") or "unknown_route"
    canonical_pool_group = signal_context.get("canonical_pool_group") or row.get("lifecycle_id") or "unknown_pool"
    lifecycle_epoch = signal_context.get("lifecycle_epoch") or row.get("lifecycle_epoch") or 0
    return {
        "paper_trade_id": row.get("id"),
        "premium_signal_id": signal_context.get("premium_signal_id"),
        "telegram_signal_id": signal_context.get("telegram_signal_id"),
        "token_ca": row.get("token_ca"),
        "symbol": row.get("symbol"),
        "chain": signal_context.get("chain") or row.get("chain") or "solana",
        "canonical_pool_group": canonical_pool_group,
        "lifecycle_epoch": lifecycle_epoch,
        "quote_intent_id": row.get("id"),
        "side": "buy",
        "size": _as_float(row.get("position_size_sol")) or _as_float(row.get("entry_price")) or 0.0,
        "route": route,
        "pool": canonical_pool_group,
        "quote_mint": quote_mint,
        "slippage_bps": slippage_bps,
        "quote_ts": decision_available_at,
        "decision_available_at": decision_available_at_iso,
        "quote_source": quote_source_used,
        "quote_age_sec": quote_age_sec,
        "entry_quote_available": entry_quote_available,
        "entry_quote_available_at": entry_quote_ts,
        "entry_quote_price": entry_quote_price,
        "exit_quote_available": exit_quote_available,
        "exit_quote_available_at": exit_quote_ts,
        "exit_quote_price": exit_quote_price,
        "entry_quote_age_sec": entry_quote_age_sec,
        "exit_quote_age_sec": exit_quote_age_sec,
        "entry_quote_slippage_bps": entry_slippage_bps,
        "exit_quote_slippage_bps": exit_slippage_bps,
        "liquidity_depth_usd": _as_float(liquidity_usd),
        "execution_availability": execution_availability,
        "clean_standard_version": clean_standard_version,
        "clean_observation_type": clean_observation_type,
        "realtime_clean": realtime_clean,
        "realtime_clean_detector_version": clean_standard_version,
        "realtime_clean_quality": (
            "legacy_round_trip_quote_clean_seed"
            if realtime_clean and entry_quote_audit_available and exit_quote_audit_available
            else "legacy_price_proxy_round_trip_clean_seed"
            if realtime_clean
            else "legacy_round_trip_quote_dirty_seed"
        ),
        "used_future_peak": False,
        "used_future_outcome": False,
        "used_posthoc_label": False,
        "forbidden_future_fields_used": [],
        "entry_execution_audit": entry_audit,
        "exit_execution_audit": exit_audit,
        "monitor_state": monitor_state,
        "legacy_paper_trade": row,
    }


def mirror_realtime_clean_detector(
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
    clean_standard_version=DEFAULT_CLEAN_STANDARD_VERSION,
    quote_source=DEFAULT_QUOTE_SOURCE,
):
    summary = {
        "paper_db": str(paper_db_path),
        "signal_db": str(signal_db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "read_rows": 0,
        "appended": 0,
        "duplicate": 0,
        "failed": 0,
        "dry_run": bool(dry_run),
        "failures": [],
        "clean_standard_version": clean_standard_version,
        "quote_source": quote_source,
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
            if dry_run:
                continue
            try:
                signal_context = _signal_context(signal_db, row, signal_table=signal_table, default_chain=default_chain)
                payload = _realtime_clean_payload(
                    row,
                    signal_context,
                    clean_standard_version=clean_standard_version,
                    quote_source=quote_source,
                )
                aggregate_id = ":".join(
                    [
                        "realtime_clean",
                        str(payload.get("chain") or "unknown_chain"),
                        str(payload.get("token_ca")),
                        str(payload.get("canonical_pool_group") or "unknown_pool"),
                        str(payload.get("lifecycle_epoch", 0)),
                        str(payload.get("paper_trade_id")),
                    ]
                )
                result = V27EventLog(event_log_dir).append_event(
                    event_type=REALTIME_CLEAN_EVENT_TYPE,
                    aggregate_id=aggregate_id,
                    payload=payload,
                    source="paper_trades",
                    idempotency_key=f"realtime_clean_detector:{row.get('id')}:{clean_standard_version}",
                    observed_at=payload.get("decision_available_at") or _timestamp_to_iso(payload.get("quote_ts")),
                    available_at=payload.get("decision_available_at") or _timestamp_to_iso(payload.get("quote_ts")),
                )
            except Exception as exc:
                summary["failed"] += 1
                summary["failures"].append({"paper_trade_id": row.get("id"), "reason": str(exc)})
                continue
            status = result.get("status")
            if status == "appended":
                summary["appended"] += 1
            elif status == "duplicate":
                summary["duplicate"] += 1
            else:
                summary["failed"] += 1
                summary["failures"].append({"paper_trade_id": row.get("id"), "reason": f"unexpected status {status}"})
    return summary


def _paper_trade_ids_from_db(db, *, since_id=None, until_id=None, limit=None, table="paper_trades"):
    return [row["id"] for row in iter_paper_trade_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table)]


def _mirrored_realtime_clean_ids(event_log_dir, *, clean_standard_version=None):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != REALTIME_CLEAN_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        if clean_standard_version and payload.get("clean_standard_version") != clean_standard_version:
            continue
        paper_trade_id = payload.get("paper_trade_id")
        if paper_trade_id is None:
            continue
        counts[int(paper_trade_id)] = counts.get(int(paper_trade_id), 0) + 1
    return counts


def max_mirrored_realtime_clean_id(event_log_dir, *, clean_standard_version=DEFAULT_CLEAN_STANDARD_VERSION):
    mirrored = _mirrored_realtime_clean_ids(event_log_dir, clean_standard_version=clean_standard_version)
    return max(mirrored) if mirrored else None


def next_unmirrored_realtime_clean_since_id(event_log_dir, configured_since_id=None, *, clean_standard_version=DEFAULT_CLEAN_STANDARD_VERSION):
    max_id = max_mirrored_realtime_clean_id(event_log_dir, clean_standard_version=clean_standard_version)
    if max_id is None:
        return configured_since_id
    next_id = int(max_id) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def verify_realtime_clean_mirror_parity(
    paper_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    table="paper_trades",
    clean_standard_version=DEFAULT_CLEAN_STANDARD_VERSION,
):
    summary = {
        "paper_db": str(paper_db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "db_rows": 0,
        "mirrored_events": 0,
        "missing_paper_trade_ids": [],
        "duplicate_paper_trade_ids": [],
        "orphan_mirrored_paper_trade_ids": [],
        "event_log_verify": None,
        "event_log_error": None,
        "parity_ok": False,
        "clean_standard_version": clean_standard_version,
    }
    with _connect(paper_db_path) as db:
        db_ids = _paper_trade_ids_from_db(db, since_id=since_id, until_id=until_id, limit=limit, table=table)
    mirrored_counts = _mirrored_realtime_clean_ids(event_log_dir, clean_standard_version=clean_standard_version)
    db_id_set = set(db_ids)
    mirrored_id_set = set(mirrored_counts)
    scoped = since_id is not None or until_id is not None or limit is not None
    summary["db_rows"] = len(db_ids)
    summary["mirrored_events"] = sum(mirrored_counts.get(trade_id, 0) for trade_id in db_id_set) if scoped else sum(mirrored_counts.values())
    summary["missing_paper_trade_ids"] = sorted(db_id_set - mirrored_id_set)
    summary["duplicate_paper_trade_ids"] = sorted([trade_id for trade_id in db_id_set if mirrored_counts.get(trade_id, 0) > 1])
    summary["orphan_mirrored_paper_trade_ids"] = [] if scoped else sorted(mirrored_id_set - db_id_set)
    try:
        summary["event_log_verify"] = V27EventLog(event_log_dir).verify()
    except V27EventLogError as exc:
        summary["event_log_error"] = str(exc)
    summary["parity_ok"] = (
        not summary["missing_paper_trade_ids"]
        and not summary["duplicate_paper_trade_ids"]
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
        since_id = next_unmirrored_realtime_clean_since_id(
            args.event_log_dir,
            configured_since_id=since_id,
            clean_standard_version=args.clean_standard_version,
        )
    mirror_summary = mirror_realtime_clean_detector(
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
        clean_standard_version=args.clean_standard_version,
        quote_source=args.quote_source,
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_realtime_clean_mirror_parity(
            args.paper_db,
            args.event_log_dir,
            since_id=since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
            clean_standard_version=args.clean_standard_version,
        )
    return {
        "cursor": {
            "new_only": bool(getattr(args, "new_only", False)),
            "since_id": since_id,
            "until_id": args.until_id,
            "limit": args.limit,
            "max_mirrored_paper_trade_id": max_mirrored_realtime_clean_id(
                args.event_log_dir,
                clean_standard_version=args.clean_standard_version,
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
    parser.add_argument("--clean-standard-version", default=os.environ.get("V27_REALTIME_CLEAN_STANDARD_VERSION", DEFAULT_CLEAN_STANDARD_VERSION))
    parser.add_argument("--quote-source", default=os.environ.get("V27_REALTIME_CLEAN_QUOTE_SOURCE", DEFAULT_QUOTE_SOURCE))
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
            failed += 0 if result.get("verify", {}).get("parity_ok", True) else 1
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
