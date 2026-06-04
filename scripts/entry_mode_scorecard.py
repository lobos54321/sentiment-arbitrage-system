#!/usr/bin/env python3
"""Entry-mode scorecard from canonical SOL-accounting ledger."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from statistics import median
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = PROJECT_ROOT / "data" / "paper_trades.db"


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _rows_as_dicts(rows) -> list[dict]:
    out = []
    for row in rows:
        if hasattr(row, "keys"):
            out.append({key: row[key] for key in row.keys()})
        else:
            out.append(dict(row))
    return out


def _table_exists(db, table: str) -> bool:
    try:
        return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None
    except Exception:
        return False


def _columns(db, table: str) -> set[str]:
    if not _table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def _rate(n: int, d: int) -> float | None:
    return None if d <= 0 else n / d


def _avg(values: list[float]) -> float | None:
    return None if not values else sum(values) / len(values)


def _last_ev(rows: list[dict], n: int) -> float | None:
    values = [
        _safe_float(row.get("realized_pnl_sol"), None)
        for row in sorted(rows, key=lambda row: _safe_float(row.get("entry_ts"), 0.0) or 0.0, reverse=True)[:n]
        if _safe_float(row.get("realized_pnl_sol"), None) is not None
    ]
    return _avg(values)


def _mode_status(row: dict, *, min_sample_to_live: int = 30, max_no_route_rate: float = 0.10, max_loss_pct: float = -0.20) -> str:
    trades = int(row.get("trades") or 0)
    if trades < min_sample_to_live:
        return "SHADOW"
    if (row.get("no_route_rate") or 0.0) > max_no_route_rate or (row.get("trapped_rate") or 0.0) > max_no_route_rate:
        return "DISABLED"
    if row.get("max_loss_pct") is not None and row["max_loss_pct"] < max_loss_pct:
        return "TINY_ONLY"
    if row.get("last20_ev_sol") is not None and row["last20_ev_sol"] < 0:
        return "TINY_ONLY"
    return "LIVE"


def build_entry_mode_scorecard(
    db,
    *,
    since_ts: float | None = None,
    a_class_only: bool = False,
    min_sample_to_live: int = 30,
) -> dict:
    if not _table_exists(db, "canonical_trade_ledger"):
        return {
            "available": False,
            "reason": "canonical_trade_ledger_missing",
            "rows": [],
        }
    cols = _columns(db, "canonical_trade_ledger")
    required = {"entry_ts", "normalized_mode", "realized_pnl_sol", "realized_pnl_pct"}
    missing = sorted(required - cols)
    if missing:
        return {
            "available": False,
            "reason": "canonical_trade_ledger_missing_columns",
            "missing_columns": missing,
            "rows": [],
        }

    filters = []
    params: dict[str, Any] = {}
    if since_ts is not None:
        filters.append("COALESCE(entry_ts, exit_ts, created_at, 0) >= :since_ts")
        params["since_ts"] = float(since_ts)
    if a_class_only:
        filters.append("is_a_class_fastlane = 1")
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = _rows_as_dicts(
        db.execute(
            f"""
            SELECT *
            FROM canonical_trade_ledger
            {where}
            ORDER BY COALESCE(entry_ts, exit_ts, created_at, 0) DESC, id DESC
            """,
            params,
        ).fetchall()
    )
    by_mode: dict[str, list[dict]] = {}
    for row in rows:
        mode = str(row.get("normalized_mode") or row.get("entry_mode") or "UNKNOWN")
        if a_class_only:
            mode = str(row.get("a_class_grade") or mode or "A_CLASS")
        by_mode.setdefault(mode, []).append(row)

    score_rows = []
    for mode, mode_rows in sorted(by_mode.items(), key=lambda item: len(item[1]), reverse=True):
        closed = [row for row in mode_rows if row.get("exit_ts") is not None]
        pnl_sol = [_safe_float(row.get("realized_pnl_sol"), None) for row in closed]
        pnl_sol = [value for value in pnl_sol if value is not None]
        pnl_pct = [_safe_float(row.get("realized_pnl_pct"), None) for row in closed]
        pnl_pct = [value for value in pnl_pct if value is not None]
        non_outlier = [row for row in closed if not _truthy(row.get("outlier_flag"))]
        non_outlier_pnl = [
            _safe_float(row.get("realized_pnl_sol"), None)
            for row in non_outlier
            if _safe_float(row.get("realized_pnl_sol"), None) is not None
        ]
        peaks = [_safe_float(row.get("peak_quote_pnl_pct"), 0.0) or 0.0 for row in mode_rows]
        losses_pct = [value for value in pnl_pct if value is not None]
        result = {
            "mode": mode,
            "trades": len(mode_rows),
            "closed_trades": len(closed),
            "win_rate": _rate(sum(1 for value in pnl_sol if value > 0), len(closed)),
            "avg_pnl_sol": _avg(pnl_sol),
            "median_pnl_sol": median(pnl_sol) if pnl_sol else None,
            "avg_pnl_pct": _avg(pnl_pct),
            "median_pnl_pct": median(pnl_pct) if pnl_pct else None,
            "total_pnl_sol": sum(pnl_sol) if pnl_sol else 0.0,
            "outlier_adjusted_total_pnl_sol": sum(non_outlier_pnl) if non_outlier_pnl else 0.0,
            "outlier_adjusted_avg_pnl_sol": _avg(non_outlier_pnl),
            "doa_rate": _rate(sum(1 for value in peaks if value <= 0), len(mode_rows)),
            "peak20_rate": _rate(sum(1 for value in peaks if value >= 0.20), len(mode_rows)),
            "peak50_rate": _rate(sum(1 for value in peaks if value >= 0.50), len(mode_rows)),
            "peak100_rate": _rate(sum(1 for value in peaks if value >= 1.00), len(mode_rows)),
            "no_route_rate": _rate(sum(1 for row in mode_rows if _truthy(row.get("no_route_flag"))), len(mode_rows)),
            "trapped_rate": _rate(sum(1 for row in mode_rows if _truthy(row.get("trapped_flag"))), len(mode_rows)),
            "fast_stop_rate": _rate(sum(1 for value in pnl_pct if value <= -0.15), len(closed)),
            "max_loss_sol": min(pnl_sol) if pnl_sol else None,
            "max_loss_pct": min(losses_pct) if losses_pct else None,
            "median_hold_time_sec": median([
                _safe_float(row.get("time_held_sec"), None)
                for row in closed
                if _safe_float(row.get("time_held_sec"), None) is not None
            ]) if closed else None,
            "last20_ev_sol": _last_ev(closed, 20),
            "last50_ev_sol": _last_ev(closed, 50),
        }
        result["status"] = _mode_status(result, min_sample_to_live=min_sample_to_live)
        result["allowed_max_size_sol"] = 0.0 if result["status"] == "DISABLED" else (0.001 if result["status"] in {"SHADOW", "TINY_ONLY"} else 0.003)
        score_rows.append(result)
    return {
        "available": True,
        "generated_at": time.time(),
        "since_ts": since_ts,
        "a_class_only": a_class_only,
        "rows": score_rows,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build entry-mode scorecard from canonical_trade_ledger.")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--hours", type=float, default=168)
    parser.add_argument("--a-class-only", action="store_true")
    args = parser.parse_args(argv)
    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    try:
        since = time.time() - args.hours * 3600.0 if args.hours else None
        print(json.dumps(build_entry_mode_scorecard(db, since_ts=since, a_class_only=args.a_class_only), indent=2, sort_keys=True))
    finally:
        db.close()


if __name__ == "__main__":
    main()
