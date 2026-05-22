#!/usr/bin/env python3
"""Mirror legacy paper-entry ex-ante feasibility proof into v2.7 events."""

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
DEFAULT_LOCK_FILE = Path("/tmp/v27_ex_ante_feasibility_mirror.lock")
EX_ANTE_FEASIBILITY_EVENT_TYPE = "ex_ante_feasibility_recorded"
REQUIRED_PAPER_COLUMNS = {"id", "token_ca", "entry_price", "entry_ts", "execution_availability"}
DEFAULT_FEASIBILITY_POLICY_VERSION = "legacy_actual_paper_entry_feasibility_v0.1"


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
        "COALESCE(execution_availability, 'available') != 'unavailable'",
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


def _to_epoch_seconds(value):
    if value is None:
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    return ts / 1000.0 if ts > 1_000_000_000_000 else ts


def _entry_delay_sec(signal_ts, decision_ts):
    signal = _to_epoch_seconds(signal_ts)
    decision = _to_epoch_seconds(decision_ts)
    if signal is None or decision is None:
        return None
    return max(0.0, decision - signal)


def _ex_ante_payload(row, signal_context, *, feasibility_policy_version, default_chain):
    decision_ts = row.get("trigger_ts") or row.get("armed_ts") or row.get("entry_ts")
    entry_price = _as_float(row.get("entry_price"))
    chain = signal_context.get("chain") or row.get("chain") or default_chain
    canonical_pool_group = signal_context.get("canonical_pool_group") or row.get("lifecycle_id") or "unknown_pool"
    lifecycle_epoch = signal_context.get("lifecycle_epoch") or row.get("lifecycle_epoch") or 0
    quote_available = entry_price is not None and entry_price > 0
    return {
        "paper_trade_id": row.get("id"),
        "premium_signal_id": signal_context.get("premium_signal_id"),
        "telegram_signal_id": signal_context.get("telegram_signal_id"),
        "token_ca": row.get("token_ca"),
        "symbol": row.get("symbol"),
        "chain": chain,
        "canonical_pool_group": canonical_pool_group,
        "lifecycle_epoch": lifecycle_epoch,
        "decision_ts": decision_ts,
        "decision_available_at": decision_ts,
        "counterfactual_entry_ts": row.get("entry_ts"),
        "feasibility_policy_version": feasibility_policy_version,
        "ex_ante_feasible": True,
        "feasibility_class": "legacy_actual_paper_entry",
        "feasibility_quality": "legacy_entry_quote_available_no_future_outcome",
        "system_min_decision_latency_sec": 0,
        "system_min_entry_latency_sec": 0,
        "entry_delay_from_signal_sec": _entry_delay_sec(row.get("signal_ts"), decision_ts),
        "entry_quote_available": quote_available,
        "entry_quote_available_at": decision_ts,
        "entry_quote_price": entry_price,
        "current_quote_availability": quote_available,
        "current_pool_resolution": canonical_pool_group,
        "current_provider_health": "legacy_not_recorded",
        "current_risk_availability": "legacy_not_recorded",
        "current_reclaim_state": "legacy_not_recorded",
        "current_queue_delay_sec": 0,
        "feature_max_available_at": decision_ts,
        "used_future_peak": False,
        "used_future_outcome": False,
        "used_posthoc_label": False,
        "forbidden_future_fields_used": [],
        "execution_availability": row.get("execution_availability"),
        "legacy_policy_seed": True,
        "legacy_paper_trade": {
            "id": row.get("id"),
            "signal_ts": row.get("signal_ts"),
            "entry_ts": row.get("entry_ts"),
            "trigger_ts": row.get("trigger_ts"),
            "armed_ts": row.get("armed_ts"),
            "execution_availability": row.get("execution_availability"),
            "entry_mode": row.get("entry_mode"),
            "signal_route": row.get("signal_route"),
        },
    }


def mirror_ex_ante_feasibility(
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
    feasibility_policy_version=DEFAULT_FEASIBILITY_POLICY_VERSION,
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
        "feasibility_policy_version": feasibility_policy_version,
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
                payload = _ex_ante_payload(
                    row,
                    signal_context,
                    feasibility_policy_version=feasibility_policy_version,
                    default_chain=default_chain,
                )
                aggregate_id = ":".join(
                    [
                        "ex_ante_feasibility",
                        str(payload.get("chain") or "unknown_chain"),
                        str(payload.get("token_ca")),
                        str(payload.get("canonical_pool_group") or "unknown_pool"),
                        str(payload.get("lifecycle_epoch", 0)),
                        str(payload.get("paper_trade_id")),
                    ]
                )
                result = V27EventLog(event_log_dir).append_event(
                    event_type=EX_ANTE_FEASIBILITY_EVENT_TYPE,
                    aggregate_id=aggregate_id,
                    payload=payload,
                    source="paper_trades",
                    idempotency_key=f"ex_ante_feasibility:{row.get('id')}:{feasibility_policy_version}",
                    observed_at=_timestamp_to_iso(payload.get("decision_available_at")),
                    available_at=_timestamp_to_iso(payload.get("decision_available_at")),
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


def _mirrored_ex_ante_ids(event_log_dir, *, feasibility_policy_version=None):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != EX_ANTE_FEASIBILITY_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        if feasibility_policy_version and payload.get("feasibility_policy_version") != feasibility_policy_version:
            continue
        paper_trade_id = payload.get("paper_trade_id")
        if paper_trade_id is None:
            continue
        counts[int(paper_trade_id)] = counts.get(int(paper_trade_id), 0) + 1
    return counts


def max_mirrored_ex_ante_id(event_log_dir, *, feasibility_policy_version=DEFAULT_FEASIBILITY_POLICY_VERSION):
    mirrored = _mirrored_ex_ante_ids(event_log_dir, feasibility_policy_version=feasibility_policy_version)
    return max(mirrored) if mirrored else None


def next_unmirrored_ex_ante_since_id(event_log_dir, configured_since_id=None, *, feasibility_policy_version=DEFAULT_FEASIBILITY_POLICY_VERSION):
    max_id = max_mirrored_ex_ante_id(event_log_dir, feasibility_policy_version=feasibility_policy_version)
    if max_id is None:
        return configured_since_id
    next_id = int(max_id) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def verify_ex_ante_mirror_parity(
    paper_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    table="paper_trades",
    feasibility_policy_version=DEFAULT_FEASIBILITY_POLICY_VERSION,
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
        "feasibility_policy_version": feasibility_policy_version,
    }
    with _connect(paper_db_path) as db:
        db_ids = _paper_trade_ids_from_db(db, since_id=since_id, until_id=until_id, limit=limit, table=table)
    mirrored_counts = _mirrored_ex_ante_ids(event_log_dir, feasibility_policy_version=feasibility_policy_version)
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
        since_id = next_unmirrored_ex_ante_since_id(
            args.event_log_dir,
            configured_since_id=since_id,
            feasibility_policy_version=args.feasibility_policy_version,
        )
    mirror_summary = mirror_ex_ante_feasibility(
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
        feasibility_policy_version=args.feasibility_policy_version,
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_ex_ante_mirror_parity(
            args.paper_db,
            args.event_log_dir,
            since_id=since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
            feasibility_policy_version=args.feasibility_policy_version,
        )
    return {
        "cursor": {
            "new_only": bool(getattr(args, "new_only", False)),
            "since_id": since_id,
            "until_id": args.until_id,
            "limit": args.limit,
            "max_mirrored_paper_trade_id": max_mirrored_ex_ante_id(
                args.event_log_dir,
                feasibility_policy_version=args.feasibility_policy_version,
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
    parser.add_argument("--feasibility-policy-version", default=os.environ.get("V27_EX_ANTE_FEASIBILITY_POLICY_VERSION", DEFAULT_FEASIBILITY_POLICY_VERSION))
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
    if args.initial_delay:
        time.sleep(args.initial_delay)
    try:
        while True:
            report = run_mirror_once(args)
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
            sys.stdout.flush()
            failed = int((report.get("mirror") or {}).get("failed") or 0)
            if args.strict and failed:
                raise SystemExit(1)
            if not args.loop or stop["stop"]:
                break
            time.sleep(args.interval)
    finally:
        lock_fh.close()


if __name__ == "__main__":
    main()
