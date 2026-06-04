#!/usr/bin/env python3
"""Paper-trade integrity audit.

Use this before making strategy claims.  It separates trusted SOL-accounted
rows from legacy/synthetic/unavailable rows and flags price-unit pollution.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from typing import Any


def _columns(db, table: str) -> set[str]:
    try:
        return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _row_dict(row: Any) -> dict:
    if row is None:
        return {}
    return dict(row) if hasattr(row, "keys") else dict(row)


def _fetchone(db, sql: str, params: dict | None = None) -> dict:
    return _row_dict(db.execute(sql, params or {}).fetchone())


def _fetchall(db, sql: str, params: dict | None = None) -> list[dict]:
    return [_row_dict(row) for row in db.execute(sql, params or {}).fetchall()]


def _where_time(cols: set[str], since_ts: float | None, until_ts: float | None) -> tuple[str, dict]:
    if "entry_ts" not in cols:
        return "", {}
    clauses = []
    params: dict[str, float] = {}
    if since_ts is not None:
        clauses.append("entry_ts >= @since_ts")
        params["since_ts"] = float(since_ts)
    if until_ts is not None:
        clauses.append("entry_ts <= @until_ts")
        params["until_ts"] = float(until_ts)
    return ("WHERE " + " AND ".join(clauses)) if clauses else "", params


def build_paper_trade_integrity_audit(
    db,
    *,
    since_ts: float | None = None,
    until_ts: float | None = None,
    pollution_peak_ratio: float = 50.0,
    suspicious_entry_price_lt: float = 1e-12,
) -> dict:
    cols = _columns(db, "paper_trades")
    if not cols:
        return {"error": "paper_trades_missing", "table": "paper_trades"}
    where, params = _where_time(cols, since_ts, until_ts)
    where_and = f"{where} AND" if where else "WHERE"

    total = _fetchone(
        db,
        f"""
        SELECT COUNT(*) AS rows,
               MIN(entry_ts) AS first_entry_ts,
               MAX(entry_ts) AS last_entry_ts
        FROM paper_trades
        {where}
        """,
        params,
    )
    classification = _fetchall(
        db,
        f"""
        SELECT COALESCE(replay_source, 'NULL') AS replay_source,
               COALESCE(CAST(synthetic_close AS TEXT), 'NULL') AS synthetic_close,
               COALESCE(accounting_outcome, 'NULL') AS accounting_outcome,
               COALESCE(execution_availability, 'NULL') AS execution_availability,
               COUNT(*) AS n,
               ROUND(AVG(CASE WHEN pnl_pct > 0 THEN 1.0 ELSE 0 END) * 100.0, 3) AS win_pct,
               ROUND(AVG(pnl_pct) * 100.0, 6) AS avg_pnl_pct,
               ROUND(MIN(pnl_pct) * 100.0, 6) AS min_pnl_pct,
               ROUND(MAX(pnl_pct) * 100.0, 6) AS max_pnl_pct
        FROM paper_trades
        {where}
        GROUP BY replay_source, synthetic_close, accounting_outcome, execution_availability
        ORDER BY n DESC
        """,
        params,
    )
    trusted_where = (
        f"{where_and} accounting_outcome = 'closed_real' "
        "AND execution_availability = 'available' "
        "AND COALESCE(synthetic_close, 0) = 0"
    )
    trusted = _fetchone(
        db,
        f"""
        SELECT COUNT(*) AS n,
               ROUND(AVG(CASE WHEN pnl_pct > 0 THEN 1.0 ELSE 0 END) * 100.0, 3) AS win_pct,
               ROUND(AVG(pnl_pct) * 100.0, 6) AS avg_pnl_pct,
               SUM(CASE WHEN pnl_pct >= 0.50 THEN 1 ELSE 0 END) AS silver_n,
               SUM(CASE WHEN pnl_pct >= 1.00 THEN 1 ELSE 0 END) AS gold_n,
               ROUND(MIN(pnl_pct) * 100.0, 6) AS min_pnl_pct,
               ROUND(MAX(pnl_pct) * 100.0, 6) AS max_pnl_pct
        FROM paper_trades
        {trusted_where}
        """,
        params,
    )
    legacy = _fetchone(
        db,
        f"""
        SELECT COUNT(*) AS n,
               ROUND(AVG(CASE WHEN pnl_pct > 0 THEN 1.0 ELSE 0 END) * 100.0, 3) AS win_pct,
               ROUND(AVG(pnl_pct) * 100.0, 6) AS avg_pnl_pct,
               SUM(CASE WHEN pnl_pct >= 0.50 THEN 1 ELSE 0 END) AS silver_n,
               SUM(CASE WHEN pnl_pct >= 1.00 THEN 1 ELSE 0 END) AS gold_n,
               ROUND(MAX(pnl_pct) * 100.0, 6) AS max_pnl_pct
        FROM paper_trades
        {where_and} accounting_outcome IS NULL AND execution_availability IS NULL
        """,
        params,
    )
    pollution = _fetchone(
        db,
        f"""
        SELECT SUM(CASE WHEN peak_pnl > @pollution_peak_ratio THEN 1 ELSE 0 END) AS polluted_peak_rows,
               SUM(CASE WHEN entry_price > 0 AND entry_price < @suspicious_entry_price_lt THEN 1 ELSE 0 END) AS suspicious_tiny_entry_rows,
               ROUND(MAX(peak_pnl) * 100.0, 6) AS max_peak_pct
        FROM paper_trades
        {where}
        """,
        {**params, "pollution_peak_ratio": pollution_peak_ratio, "suspicious_entry_price_lt": suspicious_entry_price_lt},
    )
    trusted_coverage = 0.0
    if total.get("rows"):
        trusted_coverage = float(trusted.get("n") or 0) / float(total["rows"])

    by_day = []
    if "entry_ts" in cols:
        by_day = _fetchall(
            db,
            f"""
            SELECT date(entry_ts, 'unixepoch') AS day,
                   COUNT(*) AS n,
                   SUM(CASE WHEN accounting_outcome IS NULL OR execution_availability IS NULL THEN 1 ELSE 0 END) AS null_accounting_rows,
                   SUM(CASE WHEN peak_pnl > @pollution_peak_ratio THEN 1 ELSE 0 END) AS polluted_peak_rows,
                   SUM(CASE WHEN entry_price > 0 AND entry_price < @suspicious_entry_price_lt THEN 1 ELSE 0 END) AS suspicious_tiny_entry_rows,
                   ROUND(MAX(peak_pnl) * 100.0, 6) AS max_peak_pct
            FROM paper_trades
            {where}
            GROUP BY day
            ORDER BY day
            """,
            {**params, "pollution_peak_ratio": pollution_peak_ratio, "suspicious_entry_price_lt": suspicious_entry_price_lt},
        )
    return {
        "table": "paper_trades",
        "columns_present": sorted(cols),
        "total": total,
        "trusted_coverage": round(trusted_coverage, 6),
        "trusted_real_available": trusted,
        "legacy_null_accounting": legacy,
        "classification": classification,
        "pollution": pollution,
        "by_day": by_day,
        "thresholds": {
            "pollution_peak_ratio": pollution_peak_ratio,
            "suspicious_entry_price_lt": suspicious_entry_price_lt,
        },
        "notes": [
            "Use trusted_real_available for strategy claims.",
            "Legacy NULL-accounting rows are not valid for win-rate/dog-capture conclusions.",
            "peak_pnl pollution threshold is ratio decimal; 50 means +5000%.",
        ],
    }


def build_audit_from_path(path: str, **kwargs) -> dict:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    try:
        return build_paper_trade_integrity_audit(db, **kwargs)
    finally:
        db.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit paper_trades data quality before strategy analysis.")
    parser.add_argument("--db", required=True, help="Path to a SQLite database containing paper_trades")
    parser.add_argument("--since-ts", type=float)
    parser.add_argument("--until-ts", type=float)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args(argv)
    audit = build_audit_from_path(args.db, since_ts=args.since_ts, until_ts=args.until_ts)
    print(json.dumps(audit, ensure_ascii=False, indent=2 if args.pretty else None, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
