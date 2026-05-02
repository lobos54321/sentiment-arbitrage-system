#!/usr/bin/env python3
"""Replay recent paper entry decisions from audit tables.

This is a lightweight diagnosis harness. It does not try to simulate every live
API call; it checks whether known bad entries and missed dogs would be visible
to the current audit data.
"""

from __future__ import annotations

import argparse
import sqlite3
import time


def table_exists(db, name):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row)


def best_expr():
    return (
        "MAX(COALESCE(pnl_5m,0),COALESCE(pnl_15m,0),"
        "COALESCE(pnl_60m,0),COALESCE(tradable_peak_pnl,0))"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="data/paper_trades.db")
    parser.add_argument("--since", type=float, default=None, help="Unix seconds; default last 10h from DB max event")
    parser.add_argument("--dog", type=float, default=0.50, help="Missed-dog threshold as decimal pnl")
    args = parser.parse_args()

    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    if not table_exists(db, "paper_decision_events"):
        raise SystemExit("paper_decision_events table missing")
    if not table_exists(db, "paper_missed_signal_attribution"):
        raise SystemExit("paper_missed_signal_attribution table missing")

    max_event = db.execute("SELECT MAX(event_ts) FROM paper_decision_events").fetchone()[0] or time.time()
    since = args.since if args.since is not None else float(max_event) - 10 * 3600
    print(f"DB: {args.db}")
    print(f"Window: event_ts >= {since:.0f}")

    trade_summary = db.execute(
        """
        SELECT COUNT(*) n,
               SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) wins,
               SUM(CASE WHEN peak_pnl <= 0.000001 AND pnl_pct < 0 THEN 1 ELSE 0 END) peak0_losses,
               ROUND(AVG(pnl_pct)*100,2) avg_pnl,
               ROUND(SUM(pnl_pct)*100,2) sum_pnl
        FROM paper_trades
        WHERE entry_ts >= ?
        """,
        (since,),
    ).fetchone()
    print(
        "Trades: "
        f"n={trade_summary['n'] or 0} wins={trade_summary['wins'] or 0} "
        f"peak0_losses={trade_summary['peak0_losses'] or 0} "
        f"avg={trade_summary['avg_pnl']}% sum={trade_summary['sum_pnl']}%"
    )

    print("\nDecision Hotspots")
    for row in db.execute(
        """
        SELECT route, component, event_type, decision, reason,
               COUNT(DISTINCT token_ca) tokens, COUNT(*) events
        FROM paper_decision_events
        WHERE event_ts >= ?
        GROUP BY route, component, event_type, decision, reason
        ORDER BY events DESC
        LIMIT 20
        """,
        (since,),
    ):
        print(
            f"  events={row['events']:4d} tokens={row['tokens']:3d} "
            f"{row['route'] or '-'} {row['component']}/{row['event_type']} "
            f"{row['decision']} {row['reason']}"
        )

    print("\nMissed Dogs")
    for row in db.execute(
        f"""
        WITH best AS (
            SELECT token_ca, COALESCE(symbol,'') symbol, COALESCE(route,'') route,
                   MAX({best_expr()}) best_pnl,
                   MAX(COALESCE(would_stop_before_peak,0)) stopfirst
            FROM paper_missed_signal_attribution
            WHERE signal_ts >= ?
            GROUP BY token_ca, route
        )
        SELECT route,
               COUNT(*) tokens,
               SUM(CASE WHEN best_pnl >= ? THEN 1 ELSE 0 END) dogs,
               SUM(CASE WHEN stopfirst THEN 1 ELSE 0 END) stopfirst,
               ROUND(MAX(best_pnl)*100,1) max_pnl
        FROM best
        GROUP BY route
        """,
        (since, args.dog),
    ):
        print(
            f"  {row['route'] or '-'} tokens={row['tokens']} "
            f"dog{int(args.dog*100)}={row['dogs']} stopfirst={row['stopfirst']} "
            f"max={row['max_pnl']}%"
        )

    print("\nTop Missed")
    for row in db.execute(
        f"""
        WITH best AS (
            SELECT token_ca, COALESCE(symbol,'') symbol, COALESCE(route,'') route,
                   MAX({best_expr()}) best_pnl,
                   MAX(COALESCE(would_stop_before_peak,0)) stopfirst
            FROM paper_missed_signal_attribution
            WHERE signal_ts >= ?
            GROUP BY token_ca, route
        )
        SELECT symbol, route, ROUND(best_pnl*100,1) best_pnl, stopfirst
        FROM best
        ORDER BY best_pnl DESC
        LIMIT 12
        """,
        (since,),
    ):
        print(f"  {row['symbol']:<12} {row['route']:<6} best={row['best_pnl']:>7}% stopfirst={row['stopfirst']}")


if __name__ == "__main__":
    main()
