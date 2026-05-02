#!/usr/bin/env python3
"""Analyze whether shadow external-alpha features predict paper outcomes."""

import argparse
from collections import defaultdict
import json
import os
from pathlib import Path
import sqlite3


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "data" / "paper_trades.db"


def f(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def pct(value):
    return f"{f(value) * 100:+.1f}%"


def load_json(raw):
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {}


def bucket_for(alpha):
    if not alpha or not alpha.get("available"):
        return "no_external_alpha"
    if alpha.get("gmgn_momentum_confirmed"):
        return "gmgn_momentum_confirmed"
    if alpha.get("gmgn_pre_seen"):
        rounds = int(f(alpha.get("gmgn_momentum_rounds"), 0))
        if rounds >= 2:
            return "gmgn_pre_seen_rounds_2"
        return "gmgn_pre_seen_rounds_1"
    return "external_alpha_other"


def summarize(rows, value_key):
    vals = [f(row.get(value_key)) for row in rows if row.get(value_key) is not None]
    if not vals:
        return "n=0"
    wins = sum(1 for value in vals if value > 0)
    avg = sum(vals) / len(vals)
    best = max(vals)
    worst = min(vals)
    return f"n={len(vals)} win={wins / len(vals):.0%} avg={pct(avg)} best={pct(best)} worst={pct(worst)}"


def load_alpha_events(db, limit):
    if not table_exists(db, "paper_decision_events"):
        return []
    rows = db.execute(
        """
        SELECT id, event_ts, token_ca, symbol, lifecycle_id, signal_ts, route,
               component, decision, reason, payload_json
        FROM paper_decision_events
        WHERE payload_json LIKE '%external_alpha%'
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    events = []
    for row in rows:
        payload = load_json(row["payload_json"])
        alpha = payload.get("external_alpha") or {}
        events.append({**dict(row), "payload": payload, "external_alpha": alpha, "bucket": bucket_for(alpha)})
    return events


def attach_trade_outcomes(db, events):
    if not table_exists(db, "paper_trades"):
        return events
    for event in events:
        trade = db.execute(
            """
            SELECT id, pnl_pct, peak_pnl, exit_reason, strategy_stage
            FROM paper_trades
            WHERE lifecycle_id = ?
               OR (token_ca = ? AND signal_ts = ?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (event["lifecycle_id"], event["token_ca"], event["signal_ts"]),
        ).fetchone()
        if trade:
            event["trade_id"] = trade["id"]
            event["pnl_pct"] = trade["pnl_pct"]
            event["peak_pnl"] = trade["peak_pnl"]
            event["exit_reason"] = trade["exit_reason"]
            event["strategy_stage"] = trade["strategy_stage"]
    return events


def attach_missed_outcomes(db, events):
    if not table_exists(db, "paper_missed_signal_attribution"):
        return events
    for event in events:
        missed = db.execute(
            """
            SELECT tradable_missed, tradability_status, tradable_peak_pnl,
                   max_pnl_recorded, pnl_60m, reject_reason
            FROM paper_missed_signal_attribution
            WHERE lifecycle_id = ?
               OR (token_ca = ? AND signal_ts = ?)
            ORDER BY id DESC
            LIMIT 1
            """,
            (event["lifecycle_id"], event["token_ca"], event["signal_ts"]),
        ).fetchone()
        if missed:
            event["missed_peak_pnl"] = (
                missed["tradable_peak_pnl"]
                if missed["tradable_peak_pnl"] is not None
                else missed["max_pnl_recorded"]
            )
            if event["missed_peak_pnl"] is None:
                event["missed_peak_pnl"] = missed["pnl_60m"]
            event["tradable_missed"] = missed["tradable_missed"]
            event["tradability_status"] = missed["tradability_status"]
            event["missed_reject_reason"] = missed["reject_reason"]
    return events


def table_exists(db, table_name):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return bool(row)


def print_grouped(title, events, value_key):
    print(f"\n{title}")
    grouped = defaultdict(list)
    for event in events:
        grouped[event["bucket"]].append(event)
    for bucket, rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        print(f"  {bucket:<28} {summarize(rows, value_key)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default=os.environ.get("PAPER_DB") or str(DEFAULT_DB))
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()

    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    try:
        events = load_alpha_events(db, args.limit)
        attach_trade_outcomes(db, events)
        attach_missed_outcomes(db, events)
    finally:
        db.close()

    trade_events = [event for event in events if event.get("pnl_pct") is not None]
    missed_events = [event for event in events if event.get("missed_peak_pnl") is not None]

    print(f"external_alpha_events={len(events)} db={args.db}")
    print_grouped("Closed trade PnL by external alpha bucket", trade_events, "pnl_pct")
    print_grouped("Closed trade peak by external alpha bucket", trade_events, "peak_pnl")
    print_grouped("Missed peak by external alpha bucket", missed_events, "missed_peak_pnl")

    confirmed = [event for event in events if event["bucket"] == "gmgn_momentum_confirmed"]
    if confirmed:
        print("\nRecent GMGN momentum-confirmed events")
        for event in confirmed[:10]:
            alpha = event["external_alpha"]
            print(
                f"  {event['symbol'] or event['token_ca'][:8]:<12} "
                f"{event['component']}/{event['decision']} "
                f"rounds={alpha.get('gmgn_momentum_rounds')} "
                f"gain={f(alpha.get('gmgn_momentum_gain_pct')):.1f}% "
                f"lead={alpha.get('gmgn_lead_time_sec')}s "
                f"pnl={pct(event.get('pnl_pct')) if event.get('pnl_pct') is not None else '-'} "
                f"missed_peak={pct(event.get('missed_peak_pnl')) if event.get('missed_peak_pnl') is not None else '-'}"
            )


if __name__ == "__main__":
    main()
