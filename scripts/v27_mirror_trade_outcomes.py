#!/usr/bin/env python3
"""Mirror legacy paper trade outcomes into the v2.7 event log."""

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
from v27_mirror_telegram_signals import _signal_payload  # noqa: E402


DEFAULT_PAPER_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_SIGNAL_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"
DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_LOCK_FILE = Path("/tmp/v27_trade_outcome_mirror.lock")
TRADE_OUTCOME_EVENT_TYPE = "trade_outcome_label_recorded"
REQUIRED_PAPER_COLUMNS = {"id", "token_ca", "entry_price", "entry_ts", "peak_pnl"}


def _connect(db_path):
    db = sqlite3.connect(str(db_path))
    db.row_factory = sqlite3.Row
    return db


def _table_exists(db, table):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(db, table):
    if not _table_exists(db, table):
        return set()
    return {row["name"] for row in db.execute(f"PRAGMA table_info({table})")}


def _row_dict(row):
    return {key: row[key] for key in row.keys()}


def _value(row, *names):
    for name in names:
        if name in row and row.get(name) is not None:
            return row.get(name)
    return None


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_epoch_seconds(value):
    if value is None:
        return None
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    return ts / 1000.0 if ts > 1_000_000_000_000 else ts


def _timestamp_to_iso(value):
    ts = _to_epoch_seconds(value)
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


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


def _signal_row_by_id(signal_db, signal_id, *, table="premium_signals"):
    if signal_id is None or not _table_exists(signal_db, table):
        return None
    row = signal_db.execute(f"SELECT * FROM {table} WHERE id = ? LIMIT 1", (signal_id,)).fetchone()
    return _row_dict(row) if row else None


def _exit_capture_ratio(realized_pnl, peak_pnl):
    if realized_pnl is None or peak_pnl is None or peak_pnl <= 0:
        return None
    return realized_pnl / peak_pnl


def _signal_context(signal_db, row, *, signal_table="premium_signals", default_chain="solana"):
    signal_id = _as_int(_value(row, "premium_signal_id", "signal_id"))
    signal_row = _signal_row_by_id(signal_db, signal_id, table=signal_table)
    if not signal_row:
        return {
            "premium_signal_id": signal_id,
            "telegram_signal_id": signal_id,
            "chain": default_chain,
            "canonical_pool_group": _value(row, "lifecycle_id") or "unknown_pool",
            "lifecycle_epoch": _value(row, "lifecycle_epoch") or 0,
            "signal_row_found": False,
        }
    signal_payload = _signal_payload(signal_row, default_chain=default_chain)
    return {
        "premium_signal_id": signal_id,
        "telegram_signal_id": signal_payload.get("telegram_signal_id"),
        "chain": signal_payload.get("chain") or default_chain,
        "canonical_pool_group": signal_payload.get("canonical_pool_group") or "unknown_pool",
        "lifecycle_epoch": signal_payload.get("lifecycle_epoch") or 0,
        "signal_row_found": True,
    }


def _trade_outcome_payload(row, signal_context):
    entry_price = _as_float(row.get("entry_price"))
    peak_pnl = _as_float(row.get("peak_pnl"))
    realized_pnl = _as_float(row.get("pnl_pct"))
    exit_ts = _value(row, "exit_ts", "updated_at")
    entry_ts = _value(row, "entry_ts", "signal_ts")
    label_available_at = exit_ts or entry_ts
    token_ca = row.get("token_ca")
    chain = signal_context.get("chain") or row.get("chain") or "solana"
    canonical_pool_group = signal_context.get("canonical_pool_group") or row.get("lifecycle_id") or "unknown_pool"
    lifecycle_epoch = signal_context.get("lifecycle_epoch") or row.get("lifecycle_epoch") or 0
    return {
        "paper_trade_id": row.get("id"),
        "premium_signal_id": signal_context.get("premium_signal_id"),
        "telegram_signal_id": signal_context.get("telegram_signal_id"),
        "token_ca": token_ca,
        "symbol": row.get("symbol"),
        "chain": chain,
        "canonical_pool_group": canonical_pool_group,
        "lifecycle_epoch": lifecycle_epoch,
        "trade_outcome_label_version": "legacy_paper_trade_outcome_v0.1",
        "trade_outcome_label_quality": "legacy_paper_trade_view",
        "trade_outcome_label_source": "paper_trades",
        "ledger_authority_proven": False,
        "counterfactual_entry_ts": entry_ts,
        "counterfactual_entry_reason": "legacy_paper_trade_entry",
        "fill_time_anchor": "simulated_fill_ts",
        "simulated_fill_ts": entry_ts,
        "simulated_fill_price": entry_price,
        "net_delayed_executable_peak_3s": peak_pnl,
        "realized_pnl": realized_pnl,
        "exit_capture_ratio": _exit_capture_ratio(realized_pnl, peak_pnl),
        "trade_label_available_at": label_available_at,
        "exit_ts": exit_ts,
        "exit_price": _as_float(row.get("exit_price")),
        "exit_reason": row.get("exit_reason"),
        "entry_mode": row.get("entry_mode"),
        "position_size_sol": _as_float(row.get("position_size_sol")),
        "synthetic_close": bool(row.get("synthetic_close")) if row.get("synthetic_close") is not None else None,
        "accounting_outcome": row.get("accounting_outcome"),
        "legacy_paper_trade": row,
    }


def mirror_trade_outcomes(
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
                payload = _trade_outcome_payload(row, signal_context)
                aggregate_id = ":".join(
                    [
                        "trade_outcome",
                        str(payload.get("chain") or "unknown_chain"),
                        str(payload.get("token_ca")),
                        str(payload.get("canonical_pool_group") or "unknown_pool"),
                        str(payload.get("lifecycle_epoch", 0)),
                        str(payload.get("paper_trade_id")),
                    ]
                )
                result = V27EventLog(event_log_dir).append_event(
                    event_type=TRADE_OUTCOME_EVENT_TYPE,
                    aggregate_id=aggregate_id,
                    payload=payload,
                    source="paper_trades",
                    idempotency_key=f"paper_trade_outcome_label:{row.get('id')}",
                    observed_at=_timestamp_to_iso(payload.get("trade_label_available_at")),
                    available_at=_timestamp_to_iso(payload.get("trade_label_available_at")),
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


def _mirrored_trade_outcome_ids(event_log_dir):
    counts = {}
    if not (Path(event_log_dir) / "events.jsonl").exists():
        return counts
    for event in V27EventLog(event_log_dir).iter_events():
        if event.get("event_type") != TRADE_OUTCOME_EVENT_TYPE:
            continue
        payload = event.get("payload") or {}
        paper_trade_id = payload.get("paper_trade_id")
        if paper_trade_id is None:
            continue
        counts[int(paper_trade_id)] = counts.get(int(paper_trade_id), 0) + 1
    return counts


def max_mirrored_trade_outcome_id(event_log_dir):
    mirrored = _mirrored_trade_outcome_ids(event_log_dir)
    return max(mirrored) if mirrored else None


def next_unmirrored_trade_since_id(event_log_dir, configured_since_id=None):
    max_id = max_mirrored_trade_outcome_id(event_log_dir)
    if max_id is None:
        return configured_since_id
    next_id = int(max_id) + 1
    if configured_since_id is not None:
        return max(next_id, int(configured_since_id))
    return next_id


def verify_trade_outcome_mirror_parity(
    paper_db_path,
    event_log_dir,
    *,
    since_id=None,
    until_id=None,
    limit=None,
    table="paper_trades",
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
    }
    with _connect(paper_db_path) as db:
        db_ids = _paper_trade_ids_from_db(db, since_id=since_id, until_id=until_id, limit=limit, table=table)

    mirrored_counts = _mirrored_trade_outcome_ids(event_log_dir)
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
        since_id = next_unmirrored_trade_since_id(args.event_log_dir, configured_since_id=since_id)
    mirror_summary = mirror_trade_outcomes(
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
    )
    verify_summary = None
    if not args.dry_run:
        verify_summary = verify_trade_outcome_mirror_parity(
            args.paper_db,
            args.event_log_dir,
            since_id=since_id,
            until_id=args.until_id,
            limit=args.limit,
            table=args.table,
        )
    return {
        "cursor": {
            "new_only": bool(getattr(args, "new_only", False)),
            "since_id": since_id,
            "until_id": args.until_id,
            "limit": args.limit,
            "max_mirrored_paper_trade_id": max_mirrored_trade_outcome_id(args.event_log_dir),
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
