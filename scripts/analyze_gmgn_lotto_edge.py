#!/usr/bin/env python3
"""Analyze GMGN LOTTO policy buckets against paper trade outcomes."""

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import sqlite3


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "data" / "paper_trades.db"


def safe_json(value):
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception:
        return {}


def safe_float(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def bucket_rate(value, cuts):
    value = safe_float(value)
    for label, upper in cuts:
        if value <= upper:
            return label
    return f">{cuts[-1][1]}"


def bucket_count(value, labels):
    value = int(safe_float(value))
    for label, upper in labels:
        if value <= upper:
            return label
    return f">={labels[-1][1] + 1}"


def extract_gmgn_from_payload(payload):
    payload = safe_json(payload)
    gmgn = payload.get("gmgn_readonly") or {}
    policy = payload.get("gmgn_policy") or {}
    if not gmgn and isinstance(payload.get("lotto_state"), dict):
        entry = (payload["lotto_state"].get("entryDecision") or {})
        gmgn = entry.get("gmgn_readonly") or {}
        policy = entry.get("gmgn_policy") or {}
    return gmgn, policy


def load_gmgn_events(db):
    events = {}
    try:
        rows = db.execute(
            """
            SELECT lifecycle_id, token_ca, signal_ts, event_ts, payload_json
            FROM paper_decision_events
            WHERE route = 'LOTTO'
              AND payload_json LIKE '%gmgn_policy%'
            ORDER BY event_ts ASC
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return events
    for row in rows:
        gmgn, policy = extract_gmgn_from_payload(row["payload_json"])
        key = row["lifecycle_id"] or f"{row['token_ca']}:{row['signal_ts']}"
        events[key] = {"gmgn": gmgn, "policy": policy}
    return events


def load_lotto_trades(db, min_id=0):
    try:
        return db.execute(
            """
            SELECT id, lifecycle_id, token_ca, symbol, signal_ts, signal_route,
                   strategy_stage, entry_ts, exit_ts, exit_reason, pnl_pct, peak_pnl,
                   position_size_sol, lotto_state_json
            FROM paper_trades
            WHERE id >= ?
              AND (signal_route = 'LOTTO' OR lotto_state_json IS NOT NULL)
            ORDER BY id ASC
            """,
            (min_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return []


def add_metric(groups, name, pnl, peak):
    g = groups[name]
    g["n"] += 1
    if pnl > 0:
        g["wins"] += 1
    if pnl < 0:
        g["losses"] += 1
    g["sum_pnl"] += pnl
    g["sum_peak"] += peak
    g["max_loss"] = min(g["max_loss"], pnl)


def print_groups(title, groups):
    print(f"\n== {title} ==")
    print(f"{'bucket':<36} {'n':>4} {'win':>6} {'avg':>8} {'peak':>8} {'sum':>8} {'maxloss':>8}")
    for name, g in sorted(groups.items(), key=lambda kv: (-kv[1]["n"], kv[0])):
        n = g["n"]
        win = g["wins"] / n if n else 0
        avg = g["sum_pnl"] / n if n else 0
        peak = g["sum_peak"] / n if n else 0
        print(
            f"{name:<36} {n:>4} {win:>5.0%} {avg:>7.1%} {peak:>7.1%} "
            f"{g['sum_pnl']:>7.1%} {g['max_loss']:>7.1%}"
        )


def analyze(db_path, min_id=0):
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    events = load_gmgn_events(db)
    trades = load_lotto_trades(db, min_id=min_id)

    groups = {
        "policy_action": defaultdict(lambda: {"n": 0, "wins": 0, "losses": 0, "sum_pnl": 0.0, "sum_peak": 0.0, "max_loss": 0.0}),
        "bundler_rate": defaultdict(lambda: {"n": 0, "wins": 0, "losses": 0, "sum_pnl": 0.0, "sum_peak": 0.0, "max_loss": 0.0}),
        "smart_degen_count": defaultdict(lambda: {"n": 0, "wins": 0, "losses": 0, "sum_pnl": 0.0, "sum_peak": 0.0, "max_loss": 0.0}),
        "renowned_count": defaultdict(lambda: {"n": 0, "wins": 0, "losses": 0, "sum_pnl": 0.0, "sum_peak": 0.0, "max_loss": 0.0}),
        "rat_trader_rate": defaultdict(lambda: {"n": 0, "wins": 0, "losses": 0, "sum_pnl": 0.0, "sum_peak": 0.0, "max_loss": 0.0}),
        "creator_close": defaultdict(lambda: {"n": 0, "wins": 0, "losses": 0, "sum_pnl": 0.0, "sum_peak": 0.0, "max_loss": 0.0}),
    }

    matched = 0
    for trade in trades:
        key = trade["lifecycle_id"] or f"{trade['token_ca']}:{trade['signal_ts']}"
        event = events.get(key)
        gmgn = (event or {}).get("gmgn") or {}
        policy = (event or {}).get("policy") or {}
        if not gmgn:
            state = safe_json(trade["lotto_state_json"])
            entry = state.get("entryDecision") or {}
            gmgn = entry.get("gmgn_readonly") or {}
            policy = entry.get("gmgn_policy") or {}
        if not gmgn:
            continue
        matched += 1
        pnl = safe_float(trade["pnl_pct"])
        peak = safe_float(trade["peak_pnl"])
        add_metric(groups["policy_action"], policy.get("action") or "no_policy", pnl, peak)
        add_metric(groups["bundler_rate"], bucket_rate(gmgn.get("bundler_rate"), [("<=0.30", 0.30), ("0.30-0.60", 0.60)]), pnl, peak)
        add_metric(groups["smart_degen_count"], bucket_count(gmgn.get("smart_degen_count"), [("0", 0), ("1-2", 2)]), pnl, peak)
        add_metric(groups["renowned_count"], bucket_count(gmgn.get("renowned_count"), [("0", 0), ("1", 1)]), pnl, peak)
        add_metric(groups["rat_trader_rate"], bucket_rate(gmgn.get("rat_trader_amount_rate"), [("<=0.05", 0.05), ("0.05-0.30", 0.30)]), pnl, peak)
        add_metric(groups["creator_close"], str(bool(gmgn.get("creator_close"))), pnl, peak)

    print(f"trades={len(trades)} matched_gmgn={matched} db={db_path}")
    for name, group in groups.items():
        print_groups(name, group)


def main():
    parser = argparse.ArgumentParser(description="Analyze GMGN LOTTO fields against paper outcomes")
    parser.add_argument("--db", default=os.environ.get("PAPER_DB", str(DEFAULT_DB)))
    parser.add_argument("--min-id", type=int, default=0)
    args = parser.parse_args()
    analyze(args.db, min_id=args.min_id)


if __name__ == "__main__":
    main()
