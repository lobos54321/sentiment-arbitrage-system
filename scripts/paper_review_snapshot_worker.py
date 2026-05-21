#!/usr/bin/env python3
"""Materialize live-safe paper review snapshots.

This worker keeps small JSON summaries for 2h/8h/24h review windows so the
dashboard does not need to scan the live SQLite database for heavy reviews.
It is paper-only observability; it never changes trades or decisions.
"""

import argparse
import datetime as dt
import fcntl
import json
import os
import sqlite3
import tempfile
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PAPER_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_OUT_DIR = PROJECT_ROOT / "data" / "review-artifacts" / "live"
PEAK_UNTRUSTED_MARK_GAP_PCT = float(os.environ.get("PEAK_UNTRUSTED_MARK_GAP_PCT", "0.25"))
ENTRY_MODE_PERFORMANCE_ROW_LIMIT = int(os.environ.get("ENTRY_MODE_PERFORMANCE_ROW_LIMIT", "20000"))


def connect(path):
    db = sqlite3.connect(path, timeout=float(os.environ.get("PAPER_REVIEW_SQLITE_TIMEOUT_SEC", "30")))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA busy_timeout = 30000")
    return db


def table_exists(db, table):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() is not None


def columns(db, table):
    if not table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def col_expr(cols, name, fallback="NULL", alias=None):
    target = alias or name
    return name if name in cols else f"{fallback} AS {target}"


def coalesce_expr(cols, names, fallback="0"):
    available = [name for name in names if name in cols]
    available.append(fallback)
    return f"COALESCE({', '.join(available)})"


def since_predicate(cols, names, param=":since", *, include_null=None):
    """Build an index-friendly recent-window predicate.

    Avoid wrapping timestamp columns in COALESCE in WHERE clauses. On the live
    paper DB those tables are large enough that COALESCE(ts, ...) can turn a
    small recent-window query into a full scan.
    """
    parts = [f"{name} >= {param}" for name in names if name in cols]
    if include_null and include_null in cols:
        parts.append(f"{include_null} IS NULL")
    if not parts:
        return f"0 >= {param}"
    return "(" + " OR ".join(parts) + ")"


def missed_since_predicate(cols, param=":since"):
    if "created_event_ts" in cols:
        return f"created_event_ts >= {param}"
    return since_predicate(cols, ["signal_ts", "baseline_ts"], param)


def rows_as_dicts(rows):
    return [dict(row) for row in rows]


def one_as_dict(row):
    return dict(row) if row else {}


def as_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def as_int(value, default=0):
    try:
        if value is None:
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def market_session_for_ts(value):
    try:
        hour = dt.datetime.fromtimestamp(int(float(value or 0)), dt.timezone.utc).hour
    except (TypeError, ValueError, OSError, OverflowError):
        return "unknown"
    if 0 <= hour < 8:
        return "asia"
    if 8 <= hour < 14:
        return "europe"
    if 14 <= hour < 22:
        return "us"
    return "quiet"


def percentile(values, pct):
    if not values:
        return None
    values = sorted(float(v) for v in values)
    if len(values) == 1:
        return values[0]
    pos = (len(values) - 1) * pct
    lo = int(pos)
    hi = min(lo + 1, len(values) - 1)
    frac = pos - lo
    return values[lo] * (1 - frac) + values[hi] * frac


def parse_json_object(raw):
    try:
        data = json.loads(raw or "{}")
        return data if isinstance(data, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def first_value(*values):
    for value in values:
        if value not in (None, ""):
            return value
    return None


def entry_mode_bucket(entry_mode, size_sol):
    mode = str(entry_mode or "").lower()
    size = as_float(size_sol, 0.0)
    if "gmgn" in mode and "tiny_scout" in mode:
        return "gmgn_tiny_scout"
    if "tiny_scout" in mode or "tiny_probe" in mode:
        return "tiny_scout"
    if "probe" in mode and 0 < size <= 0.005:
        return "tiny_scout"
    if "scout" in mode and 0 < size <= 0.005:
        return "tiny_scout"
    if "scout" in mode or "probe" in mode:
        return "scout"
    return "primary"


def infer_entry_mode(row):
    monitor_state = parse_json_object(row.get("monitor_state_json"))
    lotto_state = parse_json_object(row.get("lotto_state_json"))
    entry_audit = parse_json_object(row.get("entry_execution_audit_json"))
    entry_decision = lotto_state.get("entryDecision") or {}
    monitor_contract = monitor_state.get("entryDecisionContract") or {}
    audit_contract = entry_audit.get("entryDecisionContract") or {}
    return str(first_value(
        row.get("entry_mode"),
        monitor_state.get("entryMode"),
        monitor_state.get("entry_mode"),
        monitor_state.get("smartEntryReason"),
        monitor_state.get("passReason"),
        monitor_contract.get("entry_mode"),
        audit_contract.get("entry_mode"),
        entry_decision.get("entry_mode"),
        lotto_state.get("entry_mode"),
        f"{str(row.get('signal_route')).lower()}_unknown" if row.get("signal_route") else None,
        row.get("strategy_stage"),
        "unknown",
    ))


def missed_summary(db, since_ts, limit):
    if not table_exists(db, "paper_missed_signal_attribution"):
        return {"available": False, "reason": "paper_missed_signal_attribution_missing"}
    cols = columns(db, "paper_missed_signal_attribution")
    event_ts_expr = coalesce_expr(cols, ["created_event_ts", "signal_ts", "baseline_ts"], "0")
    trusted_peak_cols = [
        name for name in ("executable_peak_pnl", "quote_clean_peak_pnl", "tradable_peak_pnl")
        if name in cols
    ]
    if not trusted_peak_cols:
        trusted_peak_cols = [
            name for name in ("max_pnl_recorded", "pnl_24h", "pnl_60m", "pnl_15m", "pnl_5m")
            if name in cols
        ]
    max_pnl_expr = f"COALESCE({', '.join(trusted_peak_cols + ['0'])})"
    mark_pnl_expr = coalesce_expr(
        cols,
        [
            "theoretical_peak_pnl",
            "max_pnl_recorded",
            "pnl_24h",
            "pnl_60m",
            "pnl_15m",
            "pnl_5m",
        ],
        "0",
    )
    quote_exec_expr = (
        """
          CASE
            WHEN COALESCE(tradable_missed, 0) = 1
             AND COALESCE(would_stop_before_peak, 0) != 1
            THEN 1 ELSE 0
          END
        """
        if "tradable_missed" in cols else "0"
    )
    tradable_expr = "COALESCE(tradable_missed, 0)" if "tradable_missed" in cols else "0"
    stop_before_expr = "COALESCE(would_stop_before_peak, 0)" if "would_stop_before_peak" in cols else "0"
    params = {"since": since_ts, "limit": limit}
    recent_where = missed_since_predicate(cols)
    rows = rows_as_dicts(db.execute(
        """
        SELECT
          token_ca,
          COALESCE({symbol_expr}, substr(token_ca, 1, 8), '?') AS symbol,
          COALESCE({route_expr}, '-') AS route,
          COALESCE({component_expr}, '-') AS component,
          COALESCE({reject_reason_expr}, '-') AS reject_reason,
          {event_ts_expr} AS event_ts,
          {max_pnl_expr} AS max_pnl,
          {mark_pnl_expr} AS mark_pnl,
          {quote_exec_expr} AS quote_exec,
          {tradable_expr} AS tradable_missed,
          {stop_before_expr} AS would_stop_before_peak
        FROM paper_missed_signal_attribution
        WHERE {recent_where}
          AND token_ca IS NOT NULL
          AND token_ca != ''
        """.format(
            symbol_expr="symbol" if "symbol" in cols else "NULL",
            route_expr="route" if "route" in cols else "NULL",
            component_expr="component" if "component" in cols else "NULL",
            reject_reason_expr="reject_reason" if "reject_reason" in cols else "NULL",
            event_ts_expr=event_ts_expr,
            recent_where=recent_where,
            max_pnl_expr=max_pnl_expr,
            mark_pnl_expr=mark_pnl_expr,
            quote_exec_expr=quote_exec_expr,
            tradable_expr=tradable_expr,
            stop_before_expr=stop_before_expr,
        ),
        params,
    ).fetchall())

    def peak_trust_status(rec):
        if rec["max_pnl"] >= 0.25:
            return "trusted_peak"
        if rec["mark_pnl"] >= 0.25:
            return "mark_only_peak_untrusted"
        return "sub25_or_unknown"

    def blank_summary(extra=None):
        data = {
            "unique_tokens": 0,
            "gold_unique": 0,
            "silver_unique": 0,
            "bronze_unique": 0,
            "mark_only_gold_unique": 0,
            "mark_only_silver_unique": 0,
            "mark_only_bronze_unique": 0,
            "quote_executable_unique": 0,
            "tradable_unique": 0,
            "stop_before_peak_unique": 0,
            "max_pnl": None,
        }
        if extra:
            data.update(extra)
        return data

    def update_rollup(summary, rec):
        max_pnl = rec["max_pnl"]
        mark_pnl = rec["mark_pnl"]
        summary["unique_tokens"] += 1
        if max_pnl >= 1.0:
            summary["gold_unique"] += 1
        elif max_pnl >= 0.5:
            summary["silver_unique"] += 1
        elif max_pnl >= 0.25:
            summary["bronze_unique"] += 1
        if max_pnl < 1.0 and mark_pnl >= 1.0:
            summary["mark_only_gold_unique"] += 1
        elif max_pnl < 0.5 and 0.5 <= mark_pnl < 1.0:
            summary["mark_only_silver_unique"] += 1
        elif max_pnl < 0.25 and 0.25 <= mark_pnl < 0.5:
            summary["mark_only_bronze_unique"] += 1
        if rec["quote_exec"]:
            summary["quote_executable_unique"] += 1
        if rec["tradable_missed"]:
            summary["tradable_unique"] += 1
        if rec["would_stop_before_peak"]:
            summary["stop_before_peak_unique"] += 1
        if summary["max_pnl"] is None:
            summary["max_pnl"] = max_pnl
        else:
            summary["max_pnl"] = max(max_pnl, as_float(summary["max_pnl"]))

    def merge_record(target, row):
        target["symbol"] = max(str(target.get("symbol") or ""), str(row.get("symbol") or "?")) or "?"
        target["route"] = max(str(target.get("route") or ""), str(row.get("route") or "-")) or "-"
        target["component"] = max(str(target.get("component") or ""), str(row.get("component") or "-")) or "-"
        target["reject_reason"] = max(str(target.get("reject_reason") or ""), str(row.get("reject_reason") or "-")) or "-"
        target["max_pnl"] = max(as_float(target.get("max_pnl")), as_float(row.get("max_pnl")))
        target["mark_pnl"] = max(as_float(target.get("mark_pnl")), as_float(row.get("mark_pnl")))
        target["quote_exec"] = max(as_int(target.get("quote_exec")), as_int(row.get("quote_exec")))
        target["tradable_missed"] = max(as_int(target.get("tradable_missed")), as_int(row.get("tradable_missed")))
        target["would_stop_before_peak"] = max(
            as_int(target.get("would_stop_before_peak")),
            as_int(row.get("would_stop_before_peak")),
        )
        return target

    per_token = {}
    per_blocker_token = {}
    for row in rows:
        token = str(row.get("token_ca") or "")
        rec = per_token.setdefault(token, {"token_ca": token})
        merge_record(rec, row)
        blocker_key = (
            str(row.get("route") or "-"),
            str(row.get("component") or "-"),
            str(row.get("reject_reason") or "-"),
            token,
        )
        blocker_rec = per_blocker_token.setdefault(
            blocker_key,
            {
                "route": blocker_key[0],
                "component": blocker_key[1],
                "reject_reason": blocker_key[2],
                "token_ca": token,
            },
        )
        merge_record(blocker_rec, row)

    overall = blank_summary()
    for rec in per_token.values():
        update_rollup(overall, rec)
    if overall["max_pnl"] is None:
        overall["max_pnl"] = None

    gate_map = {}
    for rec in per_blocker_token.values():
        key = (rec["route"], rec["component"], rec["reject_reason"])
        gate = gate_map.setdefault(
            key,
            blank_summary({"route": key[0], "component": key[1], "reject_reason": key[2]}),
        )
        update_rollup(gate, rec)
    by_gate = sorted(
        gate_map.values(),
        key=lambda item: (
            item["gold_unique"],
            item["silver_unique"],
            item["bronze_unique"],
            item["quote_executable_unique"],
            item["unique_tokens"],
            as_float(item["max_pnl"]),
        ),
        reverse=True,
    )[:limit]
    top = []
    for rec in per_token.values():
        rec["peak_trust_status"] = peak_trust_status(rec)
        if rec["max_pnl"] >= 0.25 or rec["mark_pnl"] >= 0.25:
            top.append({
                "symbol": rec.get("symbol"),
                "token_ca": rec.get("token_ca"),
                "route": rec.get("route"),
                "component": rec.get("component"),
                "reject_reason": rec.get("reject_reason"),
                "max_pnl": rec.get("max_pnl"),
                "mark_pnl": rec.get("mark_pnl"),
                "peak_trust_status": rec.get("peak_trust_status"),
                "quote_exec": rec.get("quote_exec"),
                "tradable_missed": rec.get("tradable_missed"),
                "would_stop_before_peak": rec.get("would_stop_before_peak"),
            })
    top = sorted(
        top,
        key=lambda item: (
            as_int(item["quote_exec"]),
            as_float(item["max_pnl"]),
            as_float(item["mark_pnl"]),
        ),
        reverse=True,
    )[:limit]
    return {
        "available": True,
        "overall": overall,
        "by_gate": by_gate,
        "top_dogs": top,
    }


def trade_summary(db, since_ts, limit):
    if not table_exists(db, "paper_trades"):
        return {"available": False, "reason": "paper_trades_missing"}
    cols = columns(db, "paper_trades")
    exit_ts_expr = "exit_ts" if "exit_ts" in cols else "NULL"
    pnl_expr = "pnl_pct" if "pnl_pct" in cols else "0"
    if "trusted_peak_pnl" in cols and "quote_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(trusted_peak_pnl, 0), NULLIF(quote_peak_pnl, 0), 0)"
    elif "trusted_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(trusted_peak_pnl, 0), 0)"
    elif "quote_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(quote_peak_pnl, 0), 0)"
    else:
        peak_expr = "peak_pnl" if "peak_pnl" in cols else "0"
    mark_peak_expr = (
        "mark_peak_pnl"
        if "mark_peak_pnl" in cols
        else "peak_pnl" if "peak_pnl" in cols else "0"
    )
    size_expr = "position_size_sol" if "position_size_sol" in cols else "0"
    mode_expr = "entry_mode" if "entry_mode" in cols else "strategy_stage" if "strategy_stage" in cols else "NULL"
    branch_expr = "entry_branch" if "entry_branch" in cols else "NULL"
    params = {"since": since_ts, "limit": limit}
    where = "WHERE " + since_predicate(cols, ["entry_ts", "signal_ts", "exit_ts"], include_null="exit_ts")
    totals = one_as_dict(db.execute(
        f"""
        SELECT
          COUNT(*) AS total,
          SUM(CASE WHEN {exit_ts_expr} IS NOT NULL THEN 1 ELSE 0 END) AS closed,
          SUM(CASE WHEN {exit_ts_expr} IS NULL THEN 1 ELSE 0 END) AS open,
          SUM(CASE WHEN {pnl_expr} > 0 THEN 1 ELSE 0 END) AS wins,
          AVG({pnl_expr}) AS avg_pnl,
          AVG({peak_expr}) AS avg_peak,
          AVG({mark_peak_expr}) AS avg_mark_peak,
          SUM(CASE
            WHEN {mark_peak_expr} - {peak_expr} >= {PEAK_UNTRUSTED_MARK_GAP_PCT}
             AND {mark_peak_expr} >= 0.25
            THEN 1 ELSE 0 END
          ) AS mark_only_peak_spikes,
          SUM(COALESCE({size_expr}, 0) * COALESCE({pnl_expr}, 0)) AS est_pnl_sol
        FROM paper_trades
        {where}
        """,
        params,
    ).fetchone())
    by_mode = rows_as_dicts(db.execute(
        f"""
        SELECT
          COALESCE({mode_expr}, 'unknown') AS entry_mode,
          COALESCE({branch_expr}, '') AS entry_branch,
          COUNT(*) AS total,
          SUM(CASE WHEN {exit_ts_expr} IS NOT NULL THEN 1 ELSE 0 END) AS closed,
          SUM(CASE WHEN {pnl_expr} > 0 THEN 1 ELSE 0 END) AS wins,
          AVG({pnl_expr}) AS avg_pnl,
          AVG({peak_expr}) AS avg_peak,
          AVG({mark_peak_expr}) AS avg_mark_peak,
          SUM(CASE
            WHEN {mark_peak_expr} - {peak_expr} >= {PEAK_UNTRUSTED_MARK_GAP_PCT}
             AND {mark_peak_expr} >= 0.25
            THEN 1 ELSE 0 END
          ) AS mark_only_peak_spikes,
          SUM(COALESCE({size_expr}, 0) * COALESCE({pnl_expr}, 0)) AS est_pnl_sol
        FROM paper_trades
        {where}
        GROUP BY COALESCE({mode_expr}, 'unknown'), COALESCE({branch_expr}, '')
        ORDER BY total DESC
        LIMIT :limit
        """,
        params,
    ).fetchall())
    return {"available": True, "totals": totals, "by_mode": by_mode}


def _trade_rows_for_window(db, since_ts):
    if not table_exists(db, "paper_trades"):
        return []
    cols = columns(db, "paper_trades")
    peak_expr = "peak_pnl" if "peak_pnl" in cols else "0"
    if "trusted_peak_pnl" in cols and "quote_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(trusted_peak_pnl, 0), NULLIF(quote_peak_pnl, 0), 0)"
    elif "trusted_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(trusted_peak_pnl, 0), 0)"
    elif "quote_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(quote_peak_pnl, 0), 0)"
    mark_peak_expr = "mark_peak_pnl" if "mark_peak_pnl" in cols else peak_expr
    select_cols = [
        "id",
        col_expr(cols, "symbol"),
        col_expr(cols, "token_ca"),
        col_expr(cols, "entry_ts", "0"),
        col_expr(cols, "exit_ts"),
        col_expr(cols, "exit_reason"),
        col_expr(cols, "pnl_pct", "0"),
        f"{peak_expr} AS peak_pnl",
        f"{mark_peak_expr} AS mark_peak_pnl",
        col_expr(cols, "position_size_sol", "0"),
        col_expr(cols, "signal_route"),
        col_expr(cols, "signal_type"),
        col_expr(cols, "strategy_stage"),
        col_expr(cols, "entry_mode"),
        col_expr(cols, "entry_branch"),
        col_expr(cols, "monitor_state_json"),
        col_expr(cols, "lotto_state_json"),
        col_expr(cols, "entry_execution_audit_json"),
    ]
    # Entry-mode and route-health windows should describe trades whose own
    # lifecycle touched the requested window. Old open rows are handled by the
    # general trade summary; including every NULL exit_ts here pollutes recent
    # route EV with historical positions.
    where = "WHERE " + since_predicate(cols, ["entry_ts", "signal_ts", "exit_ts"])
    return rows_as_dicts(db.execute(
        f"""
        SELECT {', '.join(select_cols)}
        FROM paper_trades
        {where}
        ORDER BY entry_ts DESC, id DESC
        LIMIT :row_limit
        """,
        {"since": since_ts, "row_limit": ENTRY_MODE_PERFORMANCE_ROW_LIMIT},
    ).fetchall())


def entry_mode_performance_summary(db, since_ts, limit):
    if not table_exists(db, "paper_trades"):
        return {"available": False, "reason": "paper_trades_missing"}
    rows = _trade_rows_for_window(db, since_ts)
    groups = {}
    recent = []
    for row in rows:
        entry_audit = parse_json_object(row.get("entry_execution_audit_json"))
        monitor_state = parse_json_object(row.get("monitor_state_json"))
        entry_mode = infer_entry_mode(row)
        bucket = entry_mode_bucket(entry_mode, row.get("position_size_sol"))
        key = (bucket, entry_mode)
        group = groups.setdefault(key, {
            "bucket": bucket,
            "entry_mode": entry_mode,
            "total": 0,
            "open": 0,
            "closed": 0,
            "wins": 0,
            "losses": 0,
            "pnls": [],
            "peaks": [],
            "position_sizes": [],
            "est_pnl_sol": 0.0,
            "entry_quote_success_n": 0,
            "entry_quote_failure_n": 0,
            "parent_block_reasons": {},
            "recovery_probe_reasons": {},
        })
        closed = row.get("exit_ts") is not None or row.get("exit_reason") is not None
        pnl = as_float(row.get("pnl_pct"), None)
        peak = as_float(row.get("peak_pnl"), None)
        size = as_float(row.get("position_size_sol"), None)
        group["total"] += 1
        group["closed" if closed else "open"] += 1
        if pnl is not None:
            group["pnls"].append(pnl)
            if closed and pnl > 0:
                group["wins"] += 1
            if closed and pnl <= 0:
                group["losses"] += 1
            if size is not None:
                group["est_pnl_sol"] += pnl * size
        if peak is not None:
            group["peaks"].append(peak)
        if size is not None:
            group["position_sizes"].append(size)
        if entry_audit.get("success") is True or entry_audit.get("routeAvailable") is True:
            group["entry_quote_success_n"] += 1
        if entry_audit.get("failureReason") or entry_audit.get("success") is False or entry_audit.get("routeAvailable") is False:
            group["entry_quote_failure_n"] += 1
        parent_reason = first_value(monitor_state.get("parentBlockReason"), monitor_state.get("parent_block_reason"))
        recovery_reason = first_value(monitor_state.get("recoveryProbeReason"), monitor_state.get("recovery_probe_reason"))
        if parent_reason:
            group["parent_block_reasons"][str(parent_reason)] = group["parent_block_reasons"].get(str(parent_reason), 0) + 1
        if recovery_reason:
            group["recovery_probe_reasons"][str(recovery_reason)] = group["recovery_probe_reasons"].get(str(recovery_reason), 0) + 1
        if len(recent) < limit:
            recent.append({
                "id": row.get("id"),
                "symbol": row.get("symbol"),
                "token_ca": row.get("token_ca"),
                "entry_ts": row.get("entry_ts"),
                "exit_ts": row.get("exit_ts"),
                "exit_reason": row.get("exit_reason"),
                "signal_route": row.get("signal_route"),
                "strategy_stage": row.get("strategy_stage"),
                "entry_mode": entry_mode,
                "bucket": bucket,
                "position_size_sol": size,
                "pnl_pct": round(pnl * 100.0, 2) if pnl is not None else None,
                "peak_pnl_pct": round(peak * 100.0, 2) if peak is not None else None,
                "mark_peak_pnl_pct": round(as_float(row.get("mark_peak_pnl"), 0.0) * 100.0, 2),
            })

    by_mode = []
    bucket_summary = {}
    for group in groups.values():
        pnls = group["pnls"]
        peaks = group["peaks"]
        sizes = group["position_sizes"]
        quote_total = group["entry_quote_success_n"] + group["entry_quote_failure_n"]
        record = {
            "bucket": group["bucket"],
            "entry_mode": group["entry_mode"],
            "total": group["total"],
            "open": group["open"],
            "closed": group["closed"],
            "wins": group["wins"],
            "losses": group["losses"],
            "win_rate_pct": round((group["wins"] / group["closed"]) * 100.0, 1) if group["closed"] else None,
            "avg_pnl_pct": round((sum(pnls) / len(pnls)) * 100.0, 2) if pnls else None,
            "p10_pnl_pct": round(percentile(pnls, 0.10) * 100.0, 2) if pnls else None,
            "p90_pnl_pct": round(percentile(pnls, 0.90) * 100.0, 2) if pnls else None,
            "max_loss_pct": round(min(pnls) * 100.0, 2) if pnls else None,
            "avg_peak_pnl_pct": round((sum(peaks) / len(peaks)) * 100.0, 2) if peaks else None,
            "avg_position_size_sol": round(sum(sizes) / len(sizes), 4) if sizes else None,
            "est_pnl_sol": round(group["est_pnl_sol"], 6),
            "avg_ev_sol_per_trade": round(group["est_pnl_sol"] / group["total"], 6) if group["total"] else None,
            "entry_quote_success_n": group["entry_quote_success_n"],
            "entry_quote_failure_n": group["entry_quote_failure_n"],
            "entry_quote_success_rate_pct": round((group["entry_quote_success_n"] / quote_total) * 100.0, 1) if quote_total else None,
            "parent_block_reasons": group["parent_block_reasons"],
            "recovery_probe_reasons": group["recovery_probe_reasons"],
        }
        by_mode.append(record)
        bucket = bucket_summary.setdefault(group["bucket"], {"total": 0, "closed": 0, "open": 0, "est_pnl_sol": 0.0})
        bucket["total"] += group["total"]
        bucket["closed"] += group["closed"]
        bucket["open"] += group["open"]
        bucket["est_pnl_sol"] += group["est_pnl_sol"]
    for summary in bucket_summary.values():
        summary["est_pnl_sol"] = round(summary["est_pnl_sol"], 6)
    by_mode.sort(key=lambda item: (item["bucket"], -item["total"]))
    return {
        "available": True,
        "row_limit": ENTRY_MODE_PERFORMANCE_ROW_LIMIT,
        "row_count": len(rows),
        "bucket_summary": bucket_summary,
        "by_entry_mode": by_mode[:limit],
        "recent": recent,
    }


def fast_lane_summary(db, since_ts, limit):
    if not table_exists(db, "paper_fast_entry_queue"):
        return {"available": False, "reason": "paper_fast_entry_queue_missing"}
    cols = columns(db, "paper_fast_entry_queue")
    created_expr = "created_at" if "created_at" in cols else "0"
    updated_expr = "updated_at" if "updated_at" in cols else created_expr
    status_expr = "status" if "status" in cols else "'unknown'"
    branch_expr = "entry_branch" if "entry_branch" in cols else "source_type" if "source_type" in cols else "NULL"
    first_error_expr = coalesce_expr(cols, ["first_error", "last_error"], "'none'")
    session_expr = "market_session" if "market_session" in cols else "'unknown'"
    params = {"since": since_ts, "limit": limit}
    recent_where = since_predicate(cols, ["created_at", "updated_at"])
    status = rows_as_dicts(db.execute(
        f"""
        SELECT {status_expr} AS status, COUNT(*) AS n, MAX({updated_expr}) AS latest_updated_at
        FROM paper_fast_entry_queue
        WHERE {recent_where}
        GROUP BY {status_expr}
        ORDER BY n DESC
        """,
        params,
    ).fetchall())
    branches = rows_as_dicts(db.execute(
        f"""
        SELECT COALESCE({branch_expr}, 'unknown') AS entry_branch,
               {status_expr} AS status,
               COUNT(*) AS n
        FROM paper_fast_entry_queue
        WHERE {recent_where}
        GROUP BY COALESCE({branch_expr}, 'unknown'), {status_expr}
        ORDER BY n DESC
        LIMIT :limit
        """,
        params,
    ).fetchall())
    reasons = rows_as_dicts(db.execute(
        f"""
        SELECT {status_expr} AS status,
               {first_error_expr} AS reason,
               COUNT(*) AS n
        FROM paper_fast_entry_queue
        WHERE {recent_where}
        GROUP BY {status_expr}, {first_error_expr}
        ORDER BY n DESC
        LIMIT :limit
        """,
        params,
    ).fetchall())
    sessions = rows_as_dicts(db.execute(
        f"""
        SELECT COALESCE({session_expr}, 'unknown') AS market_session,
               {status_expr} AS status,
               COUNT(*) AS n
        FROM paper_fast_entry_queue
        WHERE {recent_where}
        GROUP BY COALESCE({session_expr}, 'unknown'), {status_expr}
        ORDER BY n DESC
        LIMIT :limit
        """,
        params,
    ).fetchall())
    branch_ev = branch_ev_summary(db, since_ts, limit)
    return {
        "available": True,
        "queue_status": status,
        "branch_summary": branches,
        "reason_summary": reasons,
        "session_summary": sessions,
        "branch_ev_summary": branch_ev,
    }


def branch_ev_summary(db, since_ts, limit):
    if not table_exists(db, "paper_trades"):
        return []
    cols = columns(db, "paper_trades")
    if "entry_branch" not in cols or "pnl_pct" not in cols:
        return []
    ts_col = next((name for name in ("entry_ts", "signal_ts", "exit_ts") if name in cols), None)
    filter_ts_expr = ts_col or "0"
    session_ts_expr = ts_col or "0"
    mode_expr = "entry_mode" if "entry_mode" in cols else "'unknown'"
    peak_expr = "peak_pnl" if "peak_pnl" in cols else "0"
    if "trusted_peak_pnl" in cols and "quote_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(trusted_peak_pnl, 0), NULLIF(quote_peak_pnl, 0), 0)"
    elif "trusted_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(trusted_peak_pnl, 0), 0)"
    elif "quote_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(quote_peak_pnl, 0), 0)"
    rows = rows_as_dicts(db.execute(
        f"""
        SELECT COALESCE(entry_branch, 'unknown') AS entry_branch,
               COALESCE({mode_expr}, 'unknown') AS entry_mode,
               {session_ts_expr} AS session_ts,
               COALESCE(pnl_pct, 0) AS pnl_pct,
               {peak_expr} AS trusted_peak_pnl
        FROM paper_trades
        WHERE COALESCE(pnl_pct, NULL) IS NOT NULL
          AND {filter_ts_expr} >= :since
        """,
        {"since": since_ts},
    ).fetchall())
    groups = {}
    for row in rows:
        session = market_session_for_ts(row.get("session_ts"))
        key = (row.get("entry_branch") or "unknown", row.get("entry_mode") or "unknown", session)
        group = groups.setdefault(key, {"pnls": [], "wins": 0, "trusted_dogs": 0})
        pnl = float(row.get("pnl_pct") or 0.0)
        group["pnls"].append(pnl)
        if pnl > 0:
            group["wins"] += 1
        if float(row.get("trusted_peak_pnl") or 0.0) >= 0.25:
            group["trusted_dogs"] += 1
    out = []
    for (branch, mode, session), group in groups.items():
        pnls = group["pnls"]
        closed_n = len(pnls)
        avg_pnl = sum(pnls) / closed_n if closed_n else 0.0
        out.append({
            "entry_branch": branch,
            "entry_mode": mode,
            "market_session": session,
            "closed_n": closed_n,
            "wins": group["wins"],
            "win_rate_pct": round((group["wins"] / closed_n) * 100.0, 2) if closed_n else None,
            "avg_pnl_pct": round(avg_pnl * 100.0, 2),
            "p10_pnl_pct": round(percentile(pnls, 0.10) * 100.0, 2) if pnls else None,
            "p90_pnl_pct": round(percentile(pnls, 0.90) * 100.0, 2) if pnls else None,
            "max_loss_pct": round(min(pnls) * 100.0, 2) if pnls else None,
            "trusted_dog_capture_n": group["trusted_dogs"],
            "auto_action": "downgrade_to_watch_only" if closed_n >= 20 and avg_pnl < -0.03 else "allow_or_observe",
        })
    out.sort(key=lambda item: (item["closed_n"], abs(item["avg_pnl_pct"] or 0)), reverse=True)
    return out[:limit]


def route_health_summary(db, since_ts, limit):
    queue_available = table_exists(db, "paper_fast_entry_queue")
    trade_available = table_exists(db, "paper_trades")
    routes = {}

    if queue_available:
        q_cols = columns(db, "paper_fast_entry_queue")
        created_expr = "created_at" if "created_at" in q_cols else "0"
        updated_expr = "updated_at" if "updated_at" in q_cols else created_expr
        branch_expr = "entry_branch" if "entry_branch" in q_cols else "source_type" if "source_type" in q_cols else "NULL"
        mode_expr = "entry_mode_hint" if "entry_mode_hint" in q_cols else "NULL"
        status_expr = "status" if "status" in q_cols else "'unknown'"
        payload_expr = "payload_json" if "payload_json" in q_cols else "NULL"
        reason_expr = coalesce_expr(q_cols, ["first_error", "last_error"], "'none'")
        queue_rows = rows_as_dicts(db.execute(
            f"""
            SELECT COALESCE({branch_expr}, 'unknown') AS entry_branch,
                   COALESCE({mode_expr}, '') AS entry_mode,
                   {status_expr} AS status,
                   {payload_expr} AS payload_json,
                   {reason_expr} AS reason
            FROM paper_fast_entry_queue
            WHERE {created_expr} >= :since OR {updated_expr} >= :since
            LIMIT :row_limit
            """,
            {"since": since_ts, "row_limit": ENTRY_MODE_PERFORMANCE_ROW_LIMIT},
        ).fetchall())
        for row in queue_rows:
            branch = row.get("entry_branch") or "unknown"
            mode = row.get("entry_mode") or ""
            key = (branch, mode)
            route = routes.setdefault(key, {
                "entry_branch": branch,
                "entry_mode": mode,
                "candidates": 0,
                "entered": 0,
                "watch_only": 0,
                "counterfactual": 0,
                "stale": 0,
                "quote_clean_n": 0,
                "status_counts": {},
                "reason_counts": {},
                "pnls": [],
                "wins": 0,
                "est_pnl_sol": 0.0,
            })
            status = str(row.get("status") or "unknown")
            reason = str(row.get("reason") or "none")
            payload = parse_json_object(row.get("payload_json"))
            route["candidates"] += 1
            route["status_counts"][status] = route["status_counts"].get(status, 0) + 1
            route["reason_counts"][reason] = route["reason_counts"].get(reason, 0) + 1
            if status in {"entered", "filled", "filled_paper"}:
                route["entered"] += 1
            if status == "watch_only":
                route["watch_only"] += 1
            if status == "counterfactual_only":
                route["counterfactual"] += 1
            if "stale" in reason or "stale" in status:
                route["stale"] += 1
            if (
                payload.get("quote_clean_seen")
                or payload.get("two_quote_clean_snapshots")
                or payload.get("recovery_quote_clean")
                or payload.get("final_reclaim_quote_executable")
            ):
                route["quote_clean_n"] += 1

    if trade_available:
        t_cols = columns(db, "paper_trades")
        if "pnl_pct" in t_cols:
            trade_rows = _trade_rows_for_window(db, since_ts)
            for row in trade_rows:
                branch = row.get("entry_branch") or "unknown"
                mode = infer_entry_mode(row)
                if branch == "unknown" and str(mode).lower() in {"stage1", "stage2a", "stage3"}:
                    continue
                key = (branch, mode)
                route = routes.setdefault(key, {
                    "entry_branch": branch,
                    "entry_mode": mode,
                    "candidates": 0,
                    "entered": 0,
                    "watch_only": 0,
                    "counterfactual": 0,
                    "stale": 0,
                    "quote_clean_n": 0,
                    "status_counts": {},
                    "reason_counts": {},
                    "pnls": [],
                    "wins": 0,
                    "est_pnl_sol": 0.0,
                })
                pnl = as_float(row.get("pnl_pct"), None)
                size = as_float(row.get("position_size_sol"), 0.0)
                if pnl is not None:
                    route["pnls"].append(pnl)
                    if pnl > 0:
                        route["wins"] += 1
                    route["est_pnl_sol"] += pnl * size
                route["entered"] += 1

    out = []
    totals = {
        "routes": 0,
        "candidates": 0,
        "entered": 0,
        "watch_only": 0,
        "counterfactual": 0,
        "stale": 0,
    }
    for route in routes.values():
        pnls = route.pop("pnls")
        closed_n = len(pnls)
        p10 = percentile(pnls, 0.10) if pnls else None
        max_loss = min(pnls) if pnls else None
        avg = (sum(pnls) / closed_n) if closed_n else None
        kill_reason = "route_health_observe"
        kill_status = "observe"
        if max_loss is not None and max_loss < -0.80:
            kill_status = "tripped"
            kill_reason = "route_health_catastrophic_loss"
        elif p10 is not None and p10 < -0.30:
            kill_status = "tripped"
            kill_reason = "route_health_tail_loss"
        elif closed_n >= 20 and avg is not None and avg < 0:
            kill_status = "tripped"
            kill_reason = "route_health_negative_ev"
        candidates = int(route["candidates"] or 0)
        record = {
            **route,
            "closed_n": closed_n,
            "wins": route["wins"],
            "win_rate_pct": round((route["wins"] / closed_n) * 100.0, 2) if closed_n else None,
            "avg_pnl_pct": round(avg * 100.0, 2) if avg is not None else None,
            "p10_pnl_pct": round(p10 * 100.0, 2) if p10 is not None else None,
            "p90_pnl_pct": round(percentile(pnls, 0.90) * 100.0, 2) if pnls else None,
            "max_loss_pct": round(max_loss * 100.0, 2) if max_loss is not None else None,
            "est_pnl_sol": round(route["est_pnl_sol"], 6),
            "quote_clean_rate_pct": round((route["quote_clean_n"] / candidates) * 100.0, 2) if candidates else None,
            "kill_switch": {
                "status": kill_status,
                "reason": kill_reason,
                "auto_action": "downgrade_to_watch_only" if kill_status == "tripped" else "allow_or_observe",
            },
        }
        out.append(record)
        totals["routes"] += 1
        for name in ("candidates", "entered", "watch_only", "counterfactual", "stale"):
            totals[name] += int(record.get(name) or 0)
    out.sort(key=lambda item: (
        1 if (item.get("kill_switch") or {}).get("status") == "tripped" else 0,
        item.get("candidates") or 0,
        item.get("closed_n") or 0,
    ), reverse=True)
    return {
        "available": queue_available or trade_available,
        "totals": totals,
        "routes": out[:limit],
        "notes": {
            "kill_switch_rule": "tripped when max_loss<-80%, p10<-30%, or closed>=20 and avg_pnl<0",
            "quote_clean_rate": "derived from fast-lane payload quote_clean/reclaim/final quote flags",
        },
    }


def build_snapshot(db, hours, limit):
    now_ts = int(time.time())
    since_ts = now_ts - int(hours * 3600)
    generated_at = dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    section_query_ms = {}

    def timed_section(name, fn):
        started = time.time()
        result = fn()
        section_query_ms[name] = int((time.time() - started) * 1000)
        return result

    return {
        "schema_version": 1,
        "snapshot_id": f"paper_live_{hours}h_{now_ts}",
        "generated_at": generated_at,
        "window": {
            "hours": hours,
            "since_ts": since_ts,
            "until_ts": now_ts,
            "since_iso": dt.datetime.fromtimestamp(since_ts, dt.timezone.utc).isoformat().replace("+00:00", "Z"),
            "until_iso": dt.datetime.fromtimestamp(now_ts, dt.timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "section_query_ms": section_query_ms,
        "missed": timed_section("missed", lambda: missed_summary(db, since_ts, limit)),
        "trades": timed_section("trades", lambda: trade_summary(db, since_ts, limit)),
        "fast_lane": timed_section("fast_lane", lambda: fast_lane_summary(db, since_ts, limit)),
        "entry_mode_performance": timed_section(
            "entry_mode_performance",
            lambda: entry_mode_performance_summary(db, since_ts, max(limit, 120)),
        ),
        "route_health": timed_section("route_health", lambda: route_health_summary(db, since_ts, max(limit, 120))),
    }


def write_atomic(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass


def acquire_lock(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = path.open("w", encoding="utf-8")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(f"paper review snapshot lock held at {path}; duplicate worker idling", flush=True)
        return None
    fh.write(str(os.getpid()))
    fh.flush()
    return fh


def run_once(args):
    db = connect(args.paper_db)
    try:
        for hours in args.windows:
            started = time.time()
            snapshot = build_snapshot(db, hours, args.limit)
            snapshot["query_ms"] = int((time.time() - started) * 1000)
            out = Path(args.out_dir) / f"paper_review_{hours}h.json"
            write_atomic(out, snapshot)
            print(f"wrote {out} query_ms={snapshot['query_ms']}", flush=True)
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-db", default=os.environ.get("PAPER_DB", str(DEFAULT_PAPER_DB)))
    parser.add_argument("--out-dir", default=os.environ.get("PAPER_REVIEW_LIVE_DIR", str(DEFAULT_OUT_DIR)))
    parser.add_argument("--windows", default=os.environ.get("PAPER_REVIEW_WINDOWS", "2,8,12,24"))
    parser.add_argument("--limit", type=int, default=int(os.environ.get("PAPER_REVIEW_SNAPSHOT_LIMIT", "40")))
    parser.add_argument("--interval", type=float, default=float(os.environ.get("PAPER_REVIEW_SNAPSHOT_INTERVAL_SEC", "300")))
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--lock-file", default=os.environ.get("PAPER_REVIEW_SNAPSHOT_LOCK_FILE", "/tmp/paper_review_snapshot.lock"))
    args = parser.parse_args()
    args.windows = [int(item.strip()) for item in str(args.windows).split(",") if item.strip()]

    lock_fh = acquire_lock(Path(args.lock_file))
    if lock_fh is None:
        while True:
            time.sleep(300)

    while True:
        try:
            run_once(args)
        except Exception as exc:
            print(f"paper review snapshot worker failed: {exc}", flush=True)
        if not args.loop:
            return
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
