#!/usr/bin/env python3
"""Check whether paper trade rows preserve entry/execution truth.

This is a read-only deployment sanity check. It answers the questions that were
previously reconstructed from logs: did we store seconds timestamps, can we
recover trigger->quote/fill spread, did 2-5% bad fills enter, and did partial
locks duplicate.
"""

import argparse
import json
import sqlite3
from collections import Counter


DEFAULT_DB = "data/paper_trades.db"


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def load_json(value):
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def table_columns(db, table):
    rows = db.execute(f"PRAGMA table_info({table})").fetchall()
    return {row["name"] for row in rows}


def pct(numerator, denominator):
    if not denominator:
        return 0.0
    return numerator / denominator * 100.0


def entry_spread_pct(row, audit, state):
    spread = safe_float(state.get("entrySpreadPct"))
    if spread is None:
        spread = safe_float(audit.get("entrySpreadPct"))
    if spread is not None:
        return spread
    trigger = safe_float(row["trigger_price"])
    entry = safe_float(row["entry_price"])
    if trigger and trigger > 0 and entry and entry > 0:
        return (entry - trigger) / trigger * 100.0
    return None


def route_for(row, audit, state):
    for source in (state, audit):
        route = source.get("signalRoute") or source.get("route")
        if route:
            return str(route).upper()
    signal_type = row["signal_type"] if "signal_type" in row.keys() else None
    replay_source = row["replay_source"] if "replay_source" in row.keys() else None
    if signal_type:
        return str(signal_type).upper()
    if replay_source and "lotto" in str(replay_source).lower():
        return "LOTTO"
    return "UNKNOWN"


def partial_duplicate_count(state):
    history = state.get("partialLockHistory")
    if not isinstance(history, list):
        return 0
    seen = set()
    duplicates = 0
    for event in history:
        if not isinstance(event, dict):
            continue
        key = (
            event.get("reason"),
            round(safe_float(event.get("soldPctAfter"), -1.0), 6),
        )
        if key in seen:
            duplicates += 1
        seen.add(key)
    return duplicates


def analyze(db_path, *, since_id=None, limit=None):
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    columns = table_columns(db, "paper_trades")
    required = {"id", "signal_ts", "entry_ts", "entry_price", "trigger_price"}
    missing = required - columns
    if missing:
        raise SystemExit(f"paper_trades missing required columns: {sorted(missing)}")

    optional = [
        "symbol", "token_ca", "exit_ts", "pnl_pct", "peak_pnl", "exit_reason",
        "replay_source", "signal_type", "entry_execution_audit_json",
        "monitor_state_json",
    ]
    selected = ["id", "signal_ts", "entry_ts", "entry_price", "trigger_price"]
    selected.extend(col for col in optional if col in columns)

    where = []
    params = []
    if since_id is not None:
        where.append("id >= ?")
        params.append(since_id)
    sql = f"SELECT {', '.join(selected)} FROM paper_trades"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)

    rows = db.execute(sql, params).fetchall()
    counters = Counter()
    worst_spreads = []
    duplicate_partials = []

    for row in rows:
        counters["rows"] += 1
        signal_ts = safe_float(row["signal_ts"], 0)
        entry_ts = safe_float(row["entry_ts"], 0)
        if signal_ts and signal_ts > 2_000_000_000:
            counters["signal_ts_ms_like"] += 1
        if signal_ts and entry_ts and entry_ts - signal_ts < -60:
            counters["negative_signal_age"] += 1

        audit = load_json(row["entry_execution_audit_json"] if "entry_execution_audit_json" in row.keys() else None)
        state = load_json(row["monitor_state_json"] if "monitor_state_json" in row.keys() else None)
        route = route_for(row, audit, state)
        spread = entry_spread_pct(row, audit, state)
        if spread is not None:
            counters["spread_known"] += 1
            if 2.0 < spread <= 5.0:
                counters["spread_2_to_5_filled"] += 1
            if spread > 5.0:
                counters["spread_gt_5_filled"] += 1
            if spread > 2.0 and route == "LOTTO":
                counters["lotto_spread_gt_2_filled"] += 1
            symbol = row["symbol"] if "symbol" in row.keys() else "?"
            pnl = safe_float(row["pnl_pct"])
            worst_spreads.append((spread, row["id"], symbol, route, pnl))

        duplicates = partial_duplicate_count(state)
        if duplicates:
            counters["duplicate_partial_history"] += duplicates
            symbol = row["symbol"] if "symbol" in row.keys() else "?"
            duplicate_partials.append((row["id"], symbol, duplicates))

    return counters, sorted(worst_spreads, reverse=True)[:10], duplicate_partials[:10]


def print_report(counters, worst_spreads, duplicate_partials):
    rows = counters["rows"]
    print("Entry Truth Healthcheck")
    print(f"rows: {rows}")
    print(
        "signal_ts_ms_like: "
        f"{counters['signal_ts_ms_like']} ({pct(counters['signal_ts_ms_like'], rows):.1f}%)"
    )
    print(f"negative_signal_age: {counters['negative_signal_age']}")
    print(f"spread_known: {counters['spread_known']}")
    print(f"spread_2_to_5_filled: {counters['spread_2_to_5_filled']}")
    print(f"spread_gt_5_filled: {counters['spread_gt_5_filled']}")
    print(f"lotto_spread_gt_2_filled: {counters['lotto_spread_gt_2_filled']}")
    print(f"duplicate_partial_history: {counters['duplicate_partial_history']}")

    if worst_spreads:
        print("\nWorst spreads:")
        for spread, trade_id, symbol, route, pnl in worst_spreads:
            pnl_text = "n/a" if pnl is None else f"{pnl:+.2%}"
            print(f"  #{trade_id} {symbol} {route} spread={spread:+.2f}% pnl={pnl_text}")

    if duplicate_partials:
        print("\nDuplicate partial histories:")
        for trade_id, symbol, duplicates in duplicate_partials:
            print(f"  #{trade_id} {symbol} duplicates={duplicates}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=DEFAULT_DB)
    parser.add_argument("--since-id", type=int)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    print_report(*analyze(args.db, since_id=args.since_id, limit=args.limit))


if __name__ == "__main__":
    main()
