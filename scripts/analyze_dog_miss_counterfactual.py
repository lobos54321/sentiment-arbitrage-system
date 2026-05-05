#!/usr/bin/env python3
"""
Summarize missed dog counterfactuals from paper_missed_signal_attribution.

The report is deliberately based on clean tradable rows first, then overlays
execution/spread and scout coverage. It avoids judging opportunities from mark
peak alone.
"""

import argparse
import json
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone


DEFAULT_DB = os.environ.get("PAPER_DB", "data/paper_trades.db")


def _connect(path):
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    return db


def _table_exists(db, table):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _column_exists(db, table, column):
    return any(row["name"] == column for row in db.execute(f"PRAGMA table_info({table})"))


def _best_pnl(row):
    for key in ("tradable_peak_pnl", "max_pnl_recorded", "pnl_24h", "pnl_60m", "pnl_15m", "pnl_5m"):
        value = row[key]
        if value is not None:
            return float(value)
    return 0.0


def _reclaim_pnl(row):
    for key in ("first_tradable_pnl", "pnl_15m", "pnl_5m"):
        value = row[key]
        if value is not None:
            return float(value)
    return 0.0


def _tier(pnl):
    if pnl >= 1.0:
        return "gold"
    if pnl >= 0.5:
        return "silver"
    if pnl >= 0.25:
        return "bronze"
    return "sub25"


def _pct(value):
    return f"{float(value) * 100:+.1f}%"


def _fmt_ts(ts):
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat(timespec="seconds")


def _json_dict(value):
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _event_rows(db, since_ts):
    return db.execute(
        """
        SELECT *
        FROM paper_missed_signal_attribution
        WHERE created_event_ts >= ?
          AND baseline_price IS NOT NULL
        """,
        (since_ts,),
    ).fetchall()


def _unique_by_token(rows):
    unique = {}
    for row in rows:
        token = row["token_ca"]
        if not token:
            continue
        best = _best_pnl(row)
        current = unique.get(token)
        if current is None or best > current["best_pnl"]:
            unique[token] = {"row": row, "best_pnl": best, "reclaim_pnl": _reclaim_pnl(row)}
    return list(unique.values())


def _tier_counts(items):
    counts = {"gold": 0, "silver": 0, "bronze": 0, "sub25": 0}
    clean_tradable = 0
    tradable = 0
    stop_before_peak = 0
    for item in items:
        row = item["row"] if isinstance(item, dict) else item
        pnl = item["best_pnl"] if isinstance(item, dict) else _best_pnl(row)
        counts[_tier(pnl)] += 1
        if row["tradable_missed"]:
            tradable += 1
        if row["tradable_missed"] and not row["would_stop_before_peak"]:
            clean_tradable += 1
        if row["would_stop_before_peak"]:
            stop_before_peak += 1
    counts["total"] = len(items)
    counts["tradable"] = tradable
    counts["clean_tradable"] = clean_tradable
    counts["stop_before_peak"] = stop_before_peak
    return counts


def _print_counts(label, counts):
    print(
        f"{label}: total={counts['total']} gold={counts['gold']} silver={counts['silver']} "
        f"bronze={counts['bronze']} sub25={counts['sub25']} tradable={counts['tradable']} "
        f"clean_tradable={counts['clean_tradable']} stop_before_peak={counts['stop_before_peak']}"
    )


def _spread_abort_tokens(db, since_ts):
    if not _table_exists(db, "paper_decision_events"):
        return set()
    rows = db.execute(
        """
        SELECT DISTINCT token_ca
        FROM paper_decision_events
        WHERE event_ts >= ?
          AND component = 'execution_guard'
          AND event_type = 'entry_abort'
          AND reason = 'entry_edge_spread_too_high'
          AND token_ca IS NOT NULL
        """,
        (since_ts,),
    ).fetchall()
    return {row["token_ca"] for row in rows}


def _probe_coverage(db, since_ts):
    if not _table_exists(db, "paper_decision_events"):
        return {}
    rows = db.execute(
        """
        SELECT component, event_type, reason, COUNT(*) AS n
        FROM paper_decision_events
        WHERE event_ts >= ?
          AND component IN ('lotto_upstream_probe_live', 'lotto_probe_live', 'ath_probe_live')
          AND event_type IN ('pending_entry', 'reentry_armed', 'scan', 'wait_reclaim', 'skip')
        GROUP BY component, event_type, reason
        ORDER BY component, n DESC
        """,
        (since_ts,),
    ).fetchall()
    return rows


def _blocker_summary(unique_items, spread_tokens):
    groups = defaultdict(lambda: {"n": 0, "gold": 0, "silver": 0, "bronze": 0, "clean": 0, "spread_abort": 0, "best": None})
    for item in unique_items:
        row = item["row"]
        best = item["best_pnl"]
        if best < 0.25:
            continue
        key = (row["route"] or "UNKNOWN", row["component"] or "UNKNOWN", row["reject_reason"] or "UNKNOWN")
        group = groups[key]
        tier = _tier(best)
        group["n"] += 1
        if tier in ("gold", "silver", "bronze"):
            group[tier] += 1
        if row["tradable_missed"] and not row["would_stop_before_peak"]:
            group["clean"] += 1
        if row["token_ca"] in spread_tokens:
            group["spread_abort"] += 1
        if group["best"] is None or best > group["best"]["best_pnl"]:
            group["best"] = item
    return sorted(groups.items(), key=lambda kv: (kv[1]["gold"], kv[1]["silver"], kv[1]["bronze"], kv[1]["n"]), reverse=True)


def main():
    parser = argparse.ArgumentParser(description="Analyze missed gold/silver/bronze dog counterfactuals.")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite paper trades DB path")
    parser.add_argument("--hours", type=float, default=48.0, help="Lookback window")
    parser.add_argument("--limit", type=int, default=40, help="Rows to print per section")
    args = parser.parse_args()

    db = _connect(args.db)
    if not _table_exists(db, "paper_missed_signal_attribution"):
        raise SystemExit("paper_missed_signal_attribution table not found")
    for column in ("tradable_missed", "would_stop_before_peak", "tradable_peak_pnl", "first_tradable_pnl"):
        if not _column_exists(db, "paper_missed_signal_attribution", column):
            raise SystemExit(f"missing required column: {column}")

    now_ts = int(time.time())
    since_ts = now_ts - int(args.hours * 3600)
    rows = _event_rows(db, since_ts)
    unique = _unique_by_token(rows)
    spread_tokens = _spread_abort_tokens(db, since_ts)

    print(f"Dog-miss counterfactual report db={args.db}")
    print(f"window={args.hours:.1f}h since={_fmt_ts(since_ts)} generated={_fmt_ts(now_ts)}")
    _print_counts("event_rows", _tier_counts(rows))
    _print_counts("unique_tokens", _tier_counts(unique))
    print(f"quote_spread_abort_tokens={len(spread_tokens)}")

    print("\nTop unique missed dogs (clean tradable first):")
    ranked = sorted(
        unique,
        key=lambda item: (
            1 if item["row"]["tradable_missed"] and not item["row"]["would_stop_before_peak"] else 0,
            item["best_pnl"],
        ),
        reverse=True,
    )
    for item in ranked[: args.limit]:
        row = item["row"]
        spread = " spread_abort" if row["token_ca"] in spread_tokens else ""
        clean = "clean" if row["tradable_missed"] and not row["would_stop_before_peak"] else row["tradability_status"] or "not_clean"
        print(
            f"- {row['symbol'] or row['token_ca'][:8]} {row['route']}/{row['component']} "
            f"best={_pct(item['best_pnl'])} reclaim={_pct(item['reclaim_pnl'])} {clean}{spread} "
            f"reason={row['reject_reason']}"
        )

    print("\nBlockers on >=25% unique dogs:")
    for (route, component, reason), group in _blocker_summary(unique, spread_tokens)[: args.limit]:
        best = group["best"]
        row = best["row"]
        print(
            f"- {route}/{component}/{reason}: n={group['n']} gold={group['gold']} silver={group['silver']} "
            f"bronze={group['bronze']} clean={group['clean']} spread_abort={group['spread_abort']} "
            f"top={row['symbol'] or row['token_ca'][:8]}:{_pct(best['best_pnl'])}"
        )

    coverage = _probe_coverage(db, since_ts)
    if coverage:
        print("\nProbe coverage:")
        for row in coverage:
            print(f"- {row['component']} {row['event_type']} {row['reason']}: n={row['n']}")


if __name__ == "__main__":
    main()
