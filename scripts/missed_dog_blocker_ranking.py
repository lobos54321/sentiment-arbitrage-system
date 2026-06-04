#!/usr/bin/env python3
"""Missed-dog blocker ranking from attribution evidence."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = PROJECT_ROOT / "data" / "paper_trades.db"
SECURITY_BLOCKER_MARKERS = (
    "security",
    "rug",
    "creator",
    "bundler",
    "rat",
    "entrapment",
    "honeypot",
    "top10",
)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "tradable", "clean"}
    return bool(value)


def _table_exists(db, table: str) -> bool:
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def _columns(db, table: str) -> set[str]:
    if not _table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def _optional(cols: set[str], name: str, fallback: str = "NULL") -> str:
    return name if name in cols else f"{fallback} AS {name}"


def _peak_expr(cols: set[str]) -> str:
    names = [name for name in ("max_pnl_recorded", "executable_peak_pnl", "quote_clean_peak_pnl", "pnl_60m", "pnl_15m", "pnl_5m") if name in cols]
    return f"COALESCE({', '.join(names)}, 0)" if names else "0"


def _is_security_blocker(route: str, component: str, reason: str) -> bool:
    text = " ".join([route or "", component or "", reason or ""]).lower()
    return any(marker in text for marker in SECURITY_BLOCKER_MARKERS)


def _recommendation(row: dict) -> str:
    if row.get("security_blocker_count", 0) > 0:
        return "keep_hard_block"
    if row.get("simulated_no_route_count", 0) > 0:
        return "investigate_data_quality"
    if row.get("missed_quote_clean_count", 0) <= 0:
        return "no_action"
    if row.get("dog100_n", 0) > 0 or row.get("dog50_n", 0) >= 2:
        return "allow_a_class_only"
    if row.get("avg_best_pnl_pct", 0.0) >= 50.0:
        return "downgrade_to_soft_block"
    return "no_action"


def build_missed_dog_blocker_ranking(
    db,
    *,
    since_ts: float | None = None,
    limit: int = 50,
    silver_threshold: float = 0.50,
    gold_threshold: float = 1.00,
) -> dict:
    if not _table_exists(db, "paper_missed_signal_attribution"):
        return {"available": False, "reason": "paper_missed_signal_attribution_missing", "rows": []}
    cols = _columns(db, "paper_missed_signal_attribution")
    peak_expr = _peak_expr(cols)
    ts_expr = "created_event_ts" if "created_event_ts" in cols else ("signal_ts" if "signal_ts" in cols else "0")
    filters = []
    params: dict[str, Any] = {}
    if since_ts is not None:
        filters.append(f"{ts_expr} >= :since_ts")
        params["since_ts"] = float(since_ts)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = db.execute(
        f"""
        SELECT
          {_optional(cols, 'route', "'UNKNOWN'")},
          {_optional(cols, 'component', "'UNKNOWN'")},
          {_optional(cols, 'reject_reason', "'UNKNOWN'")},
          {_optional(cols, 'tradable_missed', '0')},
          {_optional(cols, 'tradability_status', "''")},
          {_optional(cols, 'tradability_reason', "''")},
          {peak_expr} AS best_pnl
        FROM paper_missed_signal_attribution
        {where}
        """,
        params,
    ).fetchall()
    groups: dict[tuple[str, str, str], dict] = {}
    for row in rows:
        row = dict(row) if hasattr(row, "keys") else {
            "route": row[0],
            "component": row[1],
            "reject_reason": row[2],
            "tradable_missed": row[3],
            "tradability_status": row[4],
            "tradability_reason": row[5],
            "best_pnl": row[6],
        }
        route = str(row.get("route") or "UNKNOWN")
        component = str(row.get("component") or "UNKNOWN")
        reason = str(row.get("reject_reason") or "UNKNOWN")
        key = (route, component, reason)
        group = groups.setdefault(
            key,
            {
                "route": route,
                "component": component,
                "reject_reason": reason,
                "candidate_count": 0,
                "missed_quote_clean_count": 0,
                "simulated_no_route_count": 0,
                "simulated_DOA_count": 0,
                "security_blocker_count": 0,
                "dog50_n": 0,
                "dog100_n": 0,
                "max_peak_pct": 0.0,
                "_sum_peak_pct": 0.0,
            },
        )
        best = _safe_float(row.get("best_pnl"), 0.0) or 0.0
        best_pct = best * 100.0
        tradable = _truthy(row.get("tradable_missed"))
        status = str(row.get("tradability_status") or "").lower()
        group["candidate_count"] += 1
        group["missed_quote_clean_count"] += 1 if tradable else 0
        group["simulated_no_route_count"] += 1 if "no_route" in status else 0
        group["simulated_DOA_count"] += 1 if best <= 0 else 0
        group["security_blocker_count"] += 1 if _is_security_blocker(route, component, reason) else 0
        group["dog50_n"] += 1 if best >= silver_threshold else 0
        group["dog100_n"] += 1 if best >= gold_threshold else 0
        group["max_peak_pct"] = max(group["max_peak_pct"], best_pct)
        group["_sum_peak_pct"] += best_pct

    result_rows = []
    for group in groups.values():
        count = int(group["candidate_count"] or 0)
        group["avg_best_pnl_pct"] = group["_sum_peak_pct"] / count if count else 0.0
        group["counterfactual_ev_sol"] = 0.001 * (group["avg_best_pnl_pct"] / 100.0)
        group["recommendation"] = _recommendation(group)
        group.pop("_sum_peak_pct", None)
        result_rows.append(group)
    result_rows.sort(
        key=lambda row: (
            row["dog100_n"],
            row["dog50_n"],
            row["missed_quote_clean_count"],
            row["max_peak_pct"],
        ),
        reverse=True,
    )
    return {
        "available": True,
        "generated_at": time.time(),
        "since_ts": since_ts,
        "rows": result_rows[: int(limit)],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Rank missed dog blockers.")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args(argv)
    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    try:
        since = time.time() - args.hours * 3600.0 if args.hours else None
        print(json.dumps(build_missed_dog_blocker_ranking(db, since_ts=since, limit=args.limit), indent=2, sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
