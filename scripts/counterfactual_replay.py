#!/usr/bin/env python3
"""Counterfactual replay labels for missed/opportunity candidates.

The replay is deliberately conservative.  When decision-time path samples are
available, it uses deterministic triple-barrier labels.  When only missed
attribution summary columns exist, it reports `summary_only` data quality
instead of pretending to know exact event order.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from triple_barrier_label import TripleBarrierConfig, TripleBarrierLabel, label_triple_barrier_path


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
        return value.strip().lower() in {"1", "true", "yes", "on", "tradable", "clean", "ok"}
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
    names = [
        name
        for name in ("max_pnl_recorded", "executable_peak_pnl", "quote_clean_peak_pnl", "pnl_60m", "pnl_15m", "pnl_5m")
        if name in cols
    ]
    return f"COALESCE({', '.join(names)}, 0)" if names else "0"


def _rows_as_dicts(rows) -> list[dict]:
    out = []
    for row in rows:
        out.append({key: row[key] for key in row.keys()} if hasattr(row, "keys") else dict(row))
    return out


def _summary_label(row: dict, *, config: TripleBarrierConfig) -> TripleBarrierLabel:
    best = _safe_float(row.get("best_pnl"), 0.0) or 0.0
    mae = _safe_float(row.get("mae_before_peak_pnl"), None)
    status = str(row.get("tradability_status") or "").lower()
    event_ts = _safe_float(row.get("event_ts"), None)
    time_to_peak = _safe_float(row.get("time_to_peak_sec"), None)
    if "trapped" in status:
        return TripleBarrierLabel(
            label="TRAPPED",
            terminal_reason="summary_trapped_status",
            terminal_ts=event_ts,
            time_to_terminal_sec=0.0,
            max_pnl_pct=best,
            min_pnl_pct=mae,
            hit_upper=None,
            hit_lower=None,
            sample_count=0,
            quote_clean_sample_count=0,
            trapped_seen=True,
            data_quality="summary_only",
        )
    if "no_route" in status:
        return TripleBarrierLabel(
            label="NO_ROUTE",
            terminal_reason="summary_no_route_status",
            terminal_ts=event_ts,
            time_to_terminal_sec=0.0,
            max_pnl_pct=best,
            min_pnl_pct=mae,
            hit_upper=None,
            hit_lower=None,
            sample_count=0,
            quote_clean_sample_count=0,
            no_route_seen=True,
            data_quality="summary_only",
        )
    if status == "would_stop_before_peak" or (mae is not None and mae <= config.lower_barrier):
        return TripleBarrierLabel(
            label="LOWER",
            terminal_reason="summary_would_stop_before_peak",
            terminal_ts=event_ts,
            time_to_terminal_sec=0.0,
            max_pnl_pct=best,
            min_pnl_pct=mae,
            hit_upper=None,
            hit_lower=config.lower_barrier,
            sample_count=0,
            quote_clean_sample_count=0,
            data_quality="summary_only",
        )
    hit_upper = next((barrier for barrier in sorted(config.upper_barriers) if best >= barrier), None)
    if hit_upper is not None:
        return TripleBarrierLabel(
            label="UPPER",
            terminal_reason=f"summary_upper_{int(hit_upper * 100)}pct_seen",
            terminal_ts=event_ts + time_to_peak if event_ts is not None and time_to_peak is not None else event_ts,
            time_to_terminal_sec=time_to_peak,
            max_pnl_pct=best,
            min_pnl_pct=mae,
            hit_upper=hit_upper,
            hit_lower=None,
            sample_count=0,
            quote_clean_sample_count=0,
            data_quality="summary_only",
        )
    if best <= config.lower_barrier:
        return TripleBarrierLabel(
            label="LOWER",
            terminal_reason="summary_lower_barrier_seen",
            terminal_ts=event_ts,
            time_to_terminal_sec=0.0,
            max_pnl_pct=best,
            min_pnl_pct=mae,
            hit_upper=None,
            hit_lower=config.lower_barrier,
            sample_count=0,
            quote_clean_sample_count=0,
            data_quality="summary_only",
        )
    return TripleBarrierLabel(
        label="TIMEOUT",
        terminal_reason="summary_no_barrier_seen",
        terminal_ts=event_ts + config.horizon_sec if event_ts is not None else None,
        time_to_terminal_sec=config.horizon_sec,
        max_pnl_pct=best,
        min_pnl_pct=mae,
        hit_upper=None,
        hit_lower=None,
        sample_count=0,
        quote_clean_sample_count=0,
        data_quality="summary_only",
    )


def _opportunity_path_samples(db, opportunity_key: str, *, entry_ts: float, horizon_sec: int, limit: int = 500) -> list[dict]:
    if not _table_exists(db, "opportunity_event_path_samples"):
        return []
    cols = _columns(db, "opportunity_event_path_samples")
    if "opportunity_key" not in cols:
        return []
    ts_col = "sample_ts" if "sample_ts" in cols else ("event_ts" if "event_ts" in cols else "ts")
    if ts_col not in cols:
        return []
    samples = db.execute(
        f"""
        SELECT *
        FROM opportunity_event_path_samples
        WHERE opportunity_key = ?
          AND {ts_col} >= ?
          AND {ts_col} <= ?
        ORDER BY {ts_col} ASC
        LIMIT ?
        """,
        (opportunity_key, float(entry_ts), float(entry_ts) + float(horizon_sec), int(limit)),
    ).fetchall()
    return _rows_as_dicts(samples)


def _fetch_opportunity_rows(db, *, since_ts: float | None, limit: int) -> list[dict]:
    if not _table_exists(db, "opportunity_events"):
        return []
    cols = _columns(db, "opportunity_events")
    ts_expr = "event_ts" if "event_ts" in cols else "created_at"
    filters = []
    params: list[Any] = []
    if since_ts is not None:
        filters.append(f"{ts_expr} >= ?")
        params.append(float(since_ts))
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = db.execute(
        f"""
        SELECT *
        FROM opportunity_events
        {where}
        ORDER BY {ts_expr} DESC
        LIMIT ?
        """,
        params + [int(limit)],
    ).fetchall()
    return _rows_as_dicts(rows)


def _fetch_missed_rows(db, *, since_ts: float | None, limit: int) -> list[dict]:
    if not _table_exists(db, "paper_missed_signal_attribution"):
        return []
    cols = _columns(db, "paper_missed_signal_attribution")
    peak_expr = _peak_expr(cols)
    ts_expr = "created_event_ts" if "created_event_ts" in cols else ("signal_ts" if "signal_ts" in cols else "0")
    filters = []
    params: list[Any] = []
    if since_ts is not None:
        filters.append(f"{ts_expr} >= ?")
        params.append(float(since_ts))
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    rows = db.execute(
        f"""
        SELECT
          {_optional(cols, 'id', 'NULL')},
          {_optional(cols, 'symbol', "''")},
          {_optional(cols, 'token_ca', "''")},
          {_optional(cols, 'route', "'UNKNOWN'")},
          {_optional(cols, 'component', "'UNKNOWN'")},
          {_optional(cols, 'reject_reason', "'UNKNOWN'")},
          {_optional(cols, 'tradable_missed', '0')},
          {_optional(cols, 'tradability_status', "''")},
          {_optional(cols, 'mae_before_peak_pnl', 'NULL')},
          {_optional(cols, 'time_to_peak_sec', 'NULL')},
          {ts_expr} AS event_ts,
          {peak_expr} AS best_pnl
        FROM paper_missed_signal_attribution
        {where}
        ORDER BY {ts_expr} DESC
        LIMIT ?
        """,
        params + [int(limit)],
    ).fetchall()
    return _rows_as_dicts(rows)


def build_counterfactual_replay_report(
    db,
    *,
    since_ts: float | None = None,
    limit: int = 500,
    config: TripleBarrierConfig | dict | None = None,
) -> dict:
    if isinstance(config, dict):
        config = TripleBarrierConfig(
            upper_barriers=tuple(sorted(float(value) for value in config.get("upper_barriers", (0.50, 1.00, 2.00)))),
            lower_barrier=float(config.get("lower_barrier", -0.20)),
            horizon_sec=int(config.get("horizon_sec", 3600)),
            require_quote_clean=bool(config.get("require_quote_clean", True)),
        )
    config = config or TripleBarrierConfig()
    rows = []

    for event in _fetch_opportunity_rows(db, since_ts=since_ts, limit=limit):
        event_ts = _safe_float(event.get("event_ts"), _safe_float(event.get("created_at"), time.time())) or time.time()
        opportunity_key = str(event.get("opportunity_key") or "")
        samples = _opportunity_path_samples(db, opportunity_key, entry_ts=event_ts, horizon_sec=config.horizon_sec)
        label = label_triple_barrier_path(samples, entry_ts=event_ts, config=config) if samples else TripleBarrierLabel(
            label="DATA_MISSING",
            terminal_reason="no_opportunity_path_samples",
            terminal_ts=None,
            time_to_terminal_sec=None,
            max_pnl_pct=None,
            min_pnl_pct=None,
            hit_upper=None,
            hit_lower=None,
            sample_count=0,
            quote_clean_sample_count=0,
            data_quality="missing",
        )
        rows.append({
            "source": "opportunity_events",
            "source_id": event.get("id"),
            "opportunity_key": opportunity_key,
            "token_ca": event.get("token_ca"),
            "symbol": event.get("symbol"),
            "route": event.get("route_bucket"),
            "would_enter_a_class": _truthy(event.get("would_enter_a_class")),
            "quote_clean_denominator": _truthy(event.get("quote_clean")) and _truthy(event.get("quote_executable")) and _truthy(event.get("route_available")),
            **label.to_dict(),
        })

    remaining = max(0, int(limit) - len(rows))
    if remaining:
        for missed in _fetch_missed_rows(db, since_ts=since_ts, limit=remaining):
            label = _summary_label(missed, config=config)
            best = _safe_float(missed.get("best_pnl"), 0.0) or 0.0
            rows.append({
                "source": "paper_missed_signal_attribution",
                "source_id": missed.get("id"),
                "opportunity_key": f"missed:{missed.get('id') or missed.get('token_ca') or missed.get('symbol')}",
                "token_ca": missed.get("token_ca"),
                "symbol": missed.get("symbol"),
                "route": missed.get("route"),
                "component": missed.get("component"),
                "reject_reason": missed.get("reject_reason"),
                "would_enter_a_class": False,
                "quote_clean_denominator": _truthy(missed.get("tradable_missed")),
                "best_pnl": best,
                **label.to_dict(),
            })

    label_counts: dict[str, int] = {}
    for row in rows:
        label_counts[row["label"]] = label_counts.get(row["label"], 0) + 1
    quote_clean_gold_silver = [
        row
        for row in rows
        if row.get("quote_clean_denominator")
        and (_safe_float(row.get("max_pnl_pct"), _safe_float(row.get("best_pnl"), 0.0)) or 0.0) >= 0.50
    ]
    would_enter_gold_silver = [
        row
        for row in quote_clean_gold_silver
        if row.get("would_enter_a_class") or (row.get("label") == "UPPER" and row.get("data_quality") == "summary_only")
    ]
    return {
        "available": bool(rows),
        "generated_at": time.time(),
        "since_ts": since_ts,
        "config": {
            "upper_barriers": list(config.upper_barriers),
            "lower_barrier": config.lower_barrier,
            "horizon_sec": config.horizon_sec,
            "require_quote_clean": config.require_quote_clean,
        },
        "input_count": len(rows),
        "label_counts": label_counts,
        "quote_clean_gold_silver_seen_count": len(quote_clean_gold_silver),
        "quote_clean_gold_silver_would_enter_count": len(would_enter_gold_silver),
        "data_quality_counts": {
            key: sum(1 for row in rows if row.get("data_quality") == key)
            for key in sorted({row.get("data_quality") for row in rows})
        },
        "rows": rows,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build counterfactual triple-barrier replay labels.")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args(argv)
    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    try:
        since = time.time() - args.hours * 3600.0 if args.hours else None
        print(json.dumps(build_counterfactual_replay_report(db, since_ts=since, limit=args.limit), ensure_ascii=False, sort_keys=True, indent=2))
    finally:
        db.close()


if __name__ == "__main__":
    main()
