#!/usr/bin/env python3
"""Summarize paper trader decision audit events."""

import argparse
import json
import os
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_PAPER_DB = os.environ.get("PAPER_DB", str(DATA_DIR / "paper_trades.db"))


def load_json(value):
    if not value:
        return {}
    try:
        return json.loads(value)
    except Exception:
        return {}


def main():
    parser = argparse.ArgumentParser(description="Print paper decision audit summary")
    parser.add_argument("--db", default=DEFAULT_PAPER_DB)
    parser.add_argument("--since-id", type=int, default=0)
    parser.add_argument("--token", default="")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--timeline", action="store_true")
    args = parser.parse_args()

    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    has_table = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'paper_decision_events'"
    ).fetchone()
    if not has_table:
        print(f"events=0 db={args.db}")
        print("paper_decision_events table not found; run the paper monitor once after deploying audit support.")
        return

    where = ["id > ?"]
    params = [args.since_id]
    if args.token:
        where.append("token_ca = ?")
        params.append(args.token)
    where_sql = " AND ".join(where)

    rows = db.execute(
        f"""
        SELECT *
        FROM paper_decision_events
        WHERE {where_sql}
        ORDER BY id ASC
        """,
        params,
    ).fetchall()

    print(f"events={len(rows)} db={args.db}")
    if not rows:
        return

    by_component = Counter()
    by_component_decision = Counter()
    by_reason = Counter()
    by_route = Counter()
    by_token = defaultdict(Counter)

    for row in rows:
        component = row["component"] or "-"
        decision = row["decision"] or "-"
        reason = row["reason"] or "-"
        route = row["route"] or "-"
        token = row["symbol"] or row["token_ca"] or "-"
        by_component[component] += 1
        by_component_decision[(component, decision)] += 1
        by_reason[(component, reason)] += 1
        by_route[route] += 1
        by_token[token][(component, decision, reason)] += 1

    print("\nBy route:")
    for route, count in by_route.most_common(args.limit):
        print(f"  {count:5d} {route}")

    print("\nBy component:")
    for component, count in by_component.most_common(args.limit):
        print(f"  {count:5d} {component}")

    print("\nBy component/decision:")
    for (component, decision), count in by_component_decision.most_common(args.limit):
        print(f"  {count:5d} {component:18s} {decision}")

    print("\nTop reasons:")
    for (component, reason), count in by_reason.most_common(args.limit):
        print(f"  {count:5d} {component:18s} {reason}")

    print("\nTrade outcomes by route/stage:")
    try:
        outcome_rows = db.execute(
            """
            SELECT
                COALESCE(signal_route, strategy_stage, '-') AS route,
                COUNT(*) AS n,
                SUM(CASE WHEN exit_reason IS NULL THEN 1 ELSE 0 END) AS open_n,
                AVG(CASE WHEN exit_reason IS NOT NULL THEN pnl_pct END) AS avg_pnl,
                AVG(CASE WHEN exit_reason IS NOT NULL THEN peak_pnl END) AS avg_peak,
                SUM(CASE WHEN exit_reason IS NOT NULL AND pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN exit_reason IS NOT NULL THEN 1 ELSE 0 END) AS closed_n
            FROM paper_trades
            GROUP BY COALESCE(signal_route, strategy_stage, '-')
            ORDER BY n DESC
            """
        ).fetchall()
        for row in outcome_rows:
            closed = row["closed_n"] or 0
            win_rate = (row["wins"] or 0) / closed * 100 if closed else 0
            avg_pnl = row["avg_pnl"] or 0
            avg_peak = row["avg_peak"] or 0
            print(
                f"  {row['route']:12s} n={row['n']:4d} open={row['open_n']:3d} "
                f"closed={closed:4d} win={win_rate:5.1f}% avg={avg_pnl*100:+6.2f}% peak={avg_peak*100:+6.2f}%"
            )
    except sqlite3.OperationalError as exc:
        print(f"  unavailable: {exc}")

    if args.timeline:
        print("\nTimeline:")
        for row in rows[-args.limit:]:
            payload = load_json(row["payload_json"])
            extra = ""
            if payload.get("market_cap") is not None:
                extra += f" mc={payload.get('market_cap')}"
            if payload.get("current_pnl") is not None:
                extra += f" pnl={payload.get('current_pnl')}"
            print(
                f"  #{row['id']} {row['symbol'] or row['token_ca']} "
                f"{row['component']} {row['decision']} reason={row['reason']}{extra}"
            )

    has_missed = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'paper_missed_signal_attribution'"
    ).fetchone()
    if has_missed:
        print("\nMissed-dog attribution:")
        missed_rows = db.execute(
            """
            SELECT
                COALESCE(route, '-') AS route,
                component,
                reject_reason,
                COUNT(*) AS n,
                SUM(CASE WHEN pnl_60m >= 0.5 THEN 1 ELSE 0 END) AS dog50_60m,
                SUM(CASE WHEN pnl_60m >= 1.0 THEN 1 ELSE 0 END) AS dog100_60m,
                AVG(pnl_5m) AS avg_5m,
                AVG(pnl_15m) AS avg_15m,
                AVG(pnl_60m) AS avg_60m,
                AVG(pnl_24h) AS avg_24h,
                SUM(CASE WHEN status = 'complete' THEN 1 ELSE 0 END) AS complete_n
            FROM paper_missed_signal_attribution
            GROUP BY COALESCE(route, '-'), component, reject_reason
            ORDER BY dog100_60m DESC, dog50_60m DESC, n DESC
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()
        for row in missed_rows:
            def fmt(value):
                return "na" if value is None else f"{value * 100:+.1f}%"
            print(
                f"  n={row['n']:4d} complete={row['complete_n']:4d} "
                f"60m>=50%={row['dog50_60m'] or 0:3d} 60m>=100%={row['dog100_60m'] or 0:3d} "
                f"avg5={fmt(row['avg_5m'])} avg15={fmt(row['avg_15m'])} "
                f"avg60={fmt(row['avg_60m'])} avg24h={fmt(row['avg_24h'])} "
                f"{row['route']} {row['component']} reason={row['reject_reason']}"
            )

        print("\nTop missed dogs:")
        dog_rows = db.execute(
            """
            SELECT
                symbol,
                token_ca,
                route,
                component,
                reject_reason,
                pnl_5m,
                pnl_15m,
                pnl_60m,
                pnl_24h,
                max_pnl_recorded,
                status
            FROM paper_missed_signal_attribution
            WHERE COALESCE(max_pnl_recorded, pnl_60m, pnl_24h, pnl_15m, pnl_5m) >= 0.5
            ORDER BY COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m) DESC
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()
        for row in dog_rows:
            token = row["symbol"] or (row["token_ca"] or "")[:8]
            print(
                f"  {token:12s} max={fmt(row['max_pnl_recorded'])} "
                f"5m={fmt(row['pnl_5m'])} 15m={fmt(row['pnl_15m'])} "
                f"60m={fmt(row['pnl_60m'])} 24h={fmt(row['pnl_24h'])} "
                f"{row['route']} {row['component']} reason={row['reject_reason']} status={row['status']}"
            )


if __name__ == "__main__":
    main()
