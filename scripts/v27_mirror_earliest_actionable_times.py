#!/usr/bin/env python3
"""Mirror legacy earliest-actionable-time proof into v2.7 events."""

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
from v27_mirror_trade_outcomes import _as_float, _signal_context, _timestamp_to_iso  # noqa: E402


DEFAULT_PAPER_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_SIGNAL_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"
DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_earliest_actionable_time_mirror.lock")
EARLIEST_ACTIONABLE_EVENT_TYPE = "earliest_actionable_time_recorded"
EARLIEST_ACTIONABLE_REJECTED_EVENT_TYPE = "earliest_actionable_time_rejected"
REQUIRED_PAPER_COLUMNS = {"id", "token_ca", "entry_price", "entry_ts", "exit_ts", "execution_availability"}
DEFAULT_EARLIEST_ACTIONABLE_POLICY_VERSION = "legacy_actual_paper_entry_actionable_time_v0.1"


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
        "exit_ts IS NOT NULL",
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


def _max_present_timestamp(*values):
    parsed = [(_to_epoch_seconds(value), value) for value in values if value is not None]
    parsed = [item for item in parsed if item[0] is not None]
    if not parsed:
        return None
    return max(parsed, key=lambda item: item[0])[1]


def _entry_before_peak(row):
    entry_sec = _to_epoch_seconds(row.get("entry_ts"))
    peak_sec = _to_epoch_seconds(row.get("exit_ts"))
    if entry_sec is None or peak_sec is None:
        return False
    return entry_sec <= peak_sec


def _decision_ts(row):
    return row.get("trigger_ts") or row.get("armed_ts") or row.get("entry_ts")


def _earliest_actionable_ts(row):
    return _max_present_timestamp(row.get("signal_ts"), _decision_ts(row), row.get("entry_ts"))


def _actionable_window_valid(row):
    earliest_sec = _to_epoch_seconds(_earliest_actionable_ts(row))
    entry_sec = _to_epoch_seconds(row.get("entry_ts"))
    peak_sec = _to_epoch_seconds(row.get("exit_ts"))
    if earliest_sec is None or entry_sec is None or peak_sec is None:
        return False
    return earliest_sec <= entry_sec <= peak_sec


def _earliest_actionable_payload(row, signal_context, *, policy_version, default_chain):
    decision_ts = _decision_ts(row)
    entry_ts = row.get("entry_ts")
    peak_ts = row.get("exit_ts")
    signal_ts = row.get("signal_ts")
    chain = signal_context.get("chain") or row.get("chain") or default_chain
    canonical_pool_group = signal_context.get("canonical_pool_group") or row.get("lifecycle_id") or "unknown_pool"
    lifecycle_epoch = signal_context.get("lifecycle_epoch") or row.get("lifecycle_epoch") or 0
    earliest_actionable_ts = _earliest_actionable_ts(row)
    required_inputs_available_at = {
        "telegram_anchor_available_at": signal_ts,
        "pool_resolved_available_at": decision_ts,
        "entry_quote_executable_available_at": decision_ts,
        "exit_quote_executable_available_at": decision_ts,
        "critical_risk_not_bad_available_at": decision_ts,
        "liquidity_ok_available_at": decision_ts,
        "decision_engine_available_at": decision_ts,
    }
    return {
        "paper_trade_id": row.get("id"),
        "premium_signal_id": signal_context.get("premium_signal_id"),
        "telegram_signal_id": signal_context.get("telegram_signal_id"),
        "token_ca": row.get("token_ca"),
        "symbol": row.get("symbol"),
        "chain": chain,
        "canonical_pool_group": canonical_pool_group,
        "lifecycle_epoch": lifecycle_epoch,
        "earliest_actionable_policy_version": policy_version,
        "earliest_actionable_ts": earliest_actionable_ts,
        "required_inputs_available_at": required_inputs_available_at,
        "missing_inputs_before_ts": [],
        "peak_ts": peak_ts,
        "peak_ts_quality": "legacy_outcome_window_close_proxy",
        "peak_ts_source": "paper_trade_exit_ts",
        "counterfactual_entry_ts": entry_ts,
        "actionable_before_peak": _actionable_window_valid(row),
        "earliest_actionable_reason": "legacy_actual_paper_entry_inputs_available_by_decision",
        "actionability_quality": "legacy_actual_paper_entry_window_proof",
        "decision_ts": decision_ts,
        "decision_available_at": decision_ts,
        "entry_quote_price": _as_float(row.get("entry_price")),
        "execution_availability": row.get("execution_availability"),
        "legacy_policy_seed": True,
        "legacy_paper_trade": {
            "id": row.get("id"),
            "signal_ts": row.get("signal_ts"),
            "entry_ts": row.get("entry_ts"),
            "exit_ts": row.get("exit_ts"),
            "trigger_ts": row.get("trigger_ts"),
            "armed_ts": row.get("armed_ts"),
            "execution_availability": row.get("execution_availability"),
            "entry_mode": row.get("entry_mode"),
            "signal_route": row.get("signal_route"),
        },
    }


def _earliest_actionable_rejected_payload(row, signal_context, *, policy_version, default_chain, reject_reason):
    chain = signal_context.get("chain") or row.get("chain") or default_chain
    canonical_pool_group = signal_context.get("canonical_pool_group") or row.get("lifecycle_id") or "unknown_pool"
    lifecycle_epoch = signal_context.get("lifecycle_epoch") or row.get("lifecycle_epoch") or 0
    return {
        "paper_trade_id": row.get("id"),
        "premium_signal_id": signal_context.get("premium_signal_id"),
        "telegram_signal_id": signal_context.get("telegram_signal_id"),
        "token_ca": row.get("token_ca"),
        "symbol": row.get("symbol"),
        "chain": chain,
        "canonical_pool_group": canonical_pool_group,
        "lifecycle_epoch": lifecycle_epoch,
        "earliest_actionable_policy_version": policy_version,
        "reject_reason": reject_reason,
        "earliest_actionable_ts": _earliest_actionable_ts(row),
        "counterfactual_entry_ts": row.get("entry_ts"),
        "peak_ts": row.get("exit_ts"),
        "peak_ts_quality": "legacy_outcome_window_close_proxy",
        "decision_ts": _decision_ts(row),
        "decision_available_at": _decision_ts(row),
        "actionable_before_peak": False,
        "legacy_policy_seed": True,
    }


def _append_rejected_event(event_log_dir, row, signal_context, *, policy_version, default_chain, reject_reason):
    payload = _earliest_actionable_rejected_payload(
        row,
        signal_context,
        policy_version=policy_version,
        default_chain=default_chain,
        reject_reason=reject_reason,
    )
    aggregate_id = ":".join(
        [
            "earliest_actionable_time_rejected",
            str(payload.get("chain") or "unknown_chain"),
            str(payload.get("token_ca")),
            str(payload.get("canonical_pool_group") or "unknown_pool"),
            str(payload.get("lifecycle_epoch", 0)),
            str(payload.get("paper_trade_id")),
        ]
    )
    return V27EventLog(event_log_dir).append_event(
        event_type=EARLIEST_ACTIONABLE_REJECTED_EVENT_TYPE,
        aggregate_id=aggregate_id,
        payload=payload,
        source="paper_trades",
        idempotency_key=f"earliest_actionable_time_rejected:{row.get('id')}:{policy_version}",
        observed_at=_timestamp_to_iso(payload.get("decision_available_at")),
        available_at=_timestamp_to_iso(payload.get("decision_available_at")),
    )


def mirror_earliest_actionable_times(
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
    policy_version=DEFAULT_EARLIEST_ACTIONABLE_POLICY_VERSION,
):
    summary = {
        "paper_db": str(paper_db_path),
        "signal_db": str(signal_db_path),
        "event_log_dir": str(event_log_dir),
        "table": table,
        "read_rows": 0,
        "appended": 0,
        "duplicate": 0,
        "rejected_appended": 0,
        "rejected_duplicate": 0,
        "failed": 0,
        "skipped_invalid_time_order": 0,
        "dry_run": bool(dry_run),
        "failures": [],
        "earliest_actionable_policy_version": policy_version,
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
                if not _actionable_window_valid(row):
                    result = _append_rejected_event(
                        event_log_dir,
                        row,
                        signal_context,
                        policy_version=policy_version,
                        default_chain=default_chain,
                        reject_reason="legacy_time_order_invalid",
                    )
                    summary["skipped_invalid_time_order"] += 1
                    if result.get("status") == "appended":
                        summary["rejected_appended"] += 1
                    elif result.get("status") == "duplicate":
                        summary["rejected_duplicate"] += 1
                    else:
                        summary["failed"] += 1
                        summary["failures"].append({"paper_trade_id": row.get("id"), "reason": f"unexpected reject status {result.get('status')}"})
                    continue
                payload = _earliest_actionable_payload(
                    row,
                    signal_context,
                    policy_version=policy_version,
                    default_chain=default_chain,
                )
                aggregate_id = ":".join(
                    [
                        "earliest_actionable_time",
                        str(payload.get("chain") or "unknown_chain"),
                        str(payload.get("token_ca")),
                        str(payload.get("canonical_pool_group") or "unknown_pool"),
                        str(payload.get("lifecycle_epoch", 0)),
                        str(payload.get("paper_trade_id")),
                    ]
                )
                result = V27EventLog(event_log_dir).append_event(
                    event_type=EARLIEST_ACTIONABLE_EVENT_TYPE,
                    aggregate_id=aggregate_id,
                    payload=payload,
                    source="paper_trades",
                    idempotency_key=f"earliest_actionable_time:{row.get('id')}:{policy_version}",
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
    ids = []
    for row in iter_paper_trade_rows(db, since_id=since_id, until_id=until_id, limit=limit, table=table):
        row = _row_dict(row)
        if _actionable_window_valid(row):
            ids.append(row["id"])
    return ids


def _mirrored_earliest_actionable_ids(event_log_dir, *, policy_version=None, include_rejected=False):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != EARLIEST_ACTIONABLE_EVENT_TYPE and not (
            include_rejected and event.get("event_type") == EARLIEST_ACTIONABLE_REJECTED_EVENT_TYPE
        ):
            continue
        payload = event.get("payload") or {}
        if policy_version and payload.get("earliest_actionable_policy_version") != policy_version:
            continue
        paper_trade_id = payload.get("paper_trade_id")
        if paper_trade_id is None:
            continue
        counts[int(paper_trade_id)] = counts.get(int(paper_trade_id), 0) + 1
    return counts


def max_mirrored_earliest_actionable_id(event_log_dir, *, policy_version=DEFAULT_EARLIEST_ACTIONABLE_POLICY_VERSION):
    mirrored = _mirrored_earliest_actionable_ids(event_log_dir, policy_version=policy_version, include_rejected=True)
    return max(mirrored) if mirrored else None


def next_unmirrored_earliest_actionable_since_id(event_log_dir, configured_since_id=None, *, policy_version=DEFAULT_EARLIEST_ACTIONABLE_POLICY_VERSION):
    max_id = max_mirrored_earliest_actionable_id(event_log_dir, policy_version=policy_version)
    if max_id is None:
        return configured_since_id
    next_id = int(max_id) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def verify_earliest_actionable_mirror_parity(
    paper_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    table="paper_trades",
    policy_version=DEFAULT_EARLIEST_ACTIONABLE_POLICY_VERSION,
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
        "earliest_actionable_policy_version": policy_version,
    }
    with _connect(paper_db_path) as db:
        db_ids = _paper_trade_ids_from_db(db, since_id=since_id, until_id=until_id, limit=limit, table=table)
    mirrored_counts = _mirrored_earliest_actionable_ids(event_log_dir, policy_version=policy_version)
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
        since_id = next_unmirrored_earliest_actionable_since_id(
            args.event_log_dir,
            configured_since_id=since_id,
            policy_version=args.earliest_actionable_policy_version,
        )
    mirror_summary = mirror_earliest_actionable_times(
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
        policy_version=args.earliest_actionable_policy_version,
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_earliest_actionable_mirror_parity(
            args.paper_db,
            args.event_log_dir,
            since_id=since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
            policy_version=args.earliest_actionable_policy_version,
        )
    return {
        "cursor": {
            "new_only": bool(getattr(args, "new_only", False)),
            "since_id": since_id,
            "until_id": args.until_id,
            "limit": args.limit,
            "max_mirrored_paper_trade_id": max_mirrored_earliest_actionable_id(
                args.event_log_dir,
                policy_version=args.earliest_actionable_policy_version,
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
    parser.add_argument(
        "--earliest-actionable-policy-version",
        default=os.environ.get("V27_EARLIEST_ACTIONABLE_POLICY_VERSION", DEFAULT_EARLIEST_ACTIONABLE_POLICY_VERSION),
    )
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
