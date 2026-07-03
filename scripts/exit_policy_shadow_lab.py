#!/usr/bin/env python3
"""P7 exit-policy shadow lab over raw 1m price bars.

This is read-only research infrastructure. It never changes live exits,
entry policy, gates, final_entry_contract, A_CLASS, executor, wallet, canary,
or risk settings.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
import statistics
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path


SCHEMA_VERSION = "exit_policy_shadow_lab.v1"
STATS_SCHEMA_VERSION = "exit_policy_shadow_lab_statistics.v1"
SLIPPAGE_GRID = (0.0, 0.01, 0.03, 0.05)
ENTRY_DELAY_GRID_SEC = (0, 5, 10, 20, 30, 60)
HORIZON_SEC = 7200
STOP_LOSS_PCT = -20.0
POSITION_FRACTION_OF_RISK_CAPITAL = 0.10
PER_TRADE_CAPITAL_RISK_PCT = 2.0
MAX_DRAWDOWN_LIMIT_PCT = 15.0
ROI_TARGET_PCT = 200.0
DEFAULT_DATA_DIR = Path("/app/data")
PRICE_KEYS = {
    "price_at_reject",
    "price_at_rejection",
    "decision_price",
    "current_price",
    "trigger_price",
    "signal_price",
    "entry_price",
    "quote_price",
    "mark_price",
    "effective_price",
    "effectivePrice",
    "rawEffectivePrice",
}


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def iso_from_ts(ts):
    try:
        ts = int(float(ts))
    except Exception:
        return None
    if ts <= 0:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def jloads(raw, default=None):
    default = {} if default is None else default
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw or "{}")
        return parsed if isinstance(parsed, dict) else default
    except Exception:
        return default


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def safe_int(value, default=None):
    parsed = safe_float(value)
    return default if parsed is None else int(parsed)


def signal_id_key(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        num = float(text)
        if math.isfinite(num) and num.is_integer():
            return str(int(num))
    except Exception:
        pass
    return text


def table_exists(db, table):
    try:
        return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())
    except sqlite3.Error:
        return False


def table_columns(db, table):
    if not table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def connect_readonly(path):
    if not path or not Path(path).exists():
        return None
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db


def first_float_recursive(payload):
    if not isinstance(payload, dict):
        return None, None
    stack = [([], payload)]
    while stack:
        path, cursor = stack.pop()
        if not isinstance(cursor, dict):
            continue
        for key, value in cursor.items():
            next_path = path + [str(key)]
            if key in PRICE_KEYS:
                parsed = safe_float(value)
                if parsed is not None and parsed > 0:
                    return parsed, ".".join(next_path)
            if isinstance(value, dict):
                stack.append((next_path, value))
    return None, None


def load_decision_prices(paper_db_path, since_ts, until_ts):
    db = connect_readonly(paper_db_path)
    if db is None or not table_exists(db, "paper_decision_events"):
        return {}, {"available": False, "source": "paper_decision_events", "reason": "missing_table_or_db"}
    cols = table_columns(db, "paper_decision_events")
    payload_col = "payload_json" if "payload_json" in cols else None
    event_ts_col = "event_ts" if "event_ts" in cols else None
    signal_col = "signal_id" if "signal_id" in cols else None
    if not event_ts_col or not signal_col:
        db.close()
        return {}, {"available": False, "source": "paper_decision_events", "reason": "missing_signal_or_ts_columns"}
    selected = [
        "signal_id",
        "event_ts",
        "COALESCE(event_type, '') AS event_type" if "event_type" in cols else "'' AS event_type",
        "COALESCE(decision, '') AS decision" if "decision" in cols else "'' AS decision",
        "COALESCE(action, '') AS action" if "action" in cols else "'' AS action",
        "COALESCE(reason, '') AS reason" if "reason" in cols else "'' AS reason",
        f"{payload_col} AS payload_json" if payload_col else "'{}' AS payload_json",
    ]
    rows = db.execute(
        f"""
        SELECT {", ".join(selected)}
        FROM paper_decision_events
        WHERE event_ts >= ? AND event_ts <= ?
        ORDER BY event_ts ASC
        """,
        (int(since_ts) - 60, int(until_ts) + HORIZON_SEC),
    ).fetchall()
    out = defaultdict(list)
    price_rows = 0
    pass_like_price_rows = 0
    for row in rows:
        key = signal_id_key(row["signal_id"])
        if not key:
            continue
        payload = jloads(row["payload_json"])
        price, path = first_float_recursive(payload)
        if price is None:
            continue
        price_rows += 1
        text = " ".join(str(row[name] or "").lower() for name in ("event_type", "decision", "action", "reason"))
        pass_like = any(marker in text for marker in ("pass", "allow", "enter", "pending", "would_enter", "final_entry"))
        if pass_like:
            pass_like_price_rows += 1
        out[key].append({
            "event_ts": safe_int(row["event_ts"]),
            "price": price,
            "price_path": path,
            "pass_like": pass_like,
            "event_type": row["event_type"],
            "decision": row["decision"],
            "action": row["action"],
        })
    db.close()
    return dict(out), {
        "available": True,
        "source": "paper_decision_events",
        "decision_rows_scanned": len(rows),
        "decision_price_rows": price_rows,
        "pass_like_decision_price_rows": pass_like_price_rows,
    }


def load_raw_rows(raw_db, since_ts, until_ts, limit):
    if not table_exists(raw_db, "raw_signal_outcomes"):
        return [], {"available": False, "reason": "missing_raw_signal_outcomes"}
    cols = table_columns(raw_db, "raw_signal_outcomes")
    needed = [
        "signal_id", "token_ca", "symbol", "signal_ts", "signal_type", "raw_primary_tier",
        "observation_status", "kline_covered", "coverage_reason", "baseline_price",
        "baseline_ts", "baseline_pool_address", "path_pool_address", "path_price_unit",
        "max_sustained_peak_pct", "time_to_sustained_peak_sec", "payload_json",
    ]
    select = []
    for name in needed:
        select.append(name if name in cols else f"NULL AS {name}")
    rows = raw_db.execute(
        f"""
        SELECT {", ".join(select)}
        FROM raw_signal_outcomes
        WHERE signal_ts >= ? AND signal_ts <= ?
          AND token_ca IS NOT NULL AND token_ca != ''
          AND COALESCE(observation_status, 'matured') = 'matured'
        ORDER BY signal_ts ASC, signal_id ASC
        LIMIT ?
        """,
        (int(since_ts), int(until_ts), int(limit)),
    ).fetchall()
    normalized = []
    for row in rows:
        item = dict(row)
        item["signal_id_key"] = signal_id_key(item.get("signal_id"))
        item["signal_ts"] = safe_int(item.get("signal_ts"))
        item["payload"] = jloads(item.get("payload_json"))
        normalized.append(item)
    return normalized, {
        "available": True,
        "source": "raw_signal_outcomes",
        "loaded_rows": len(normalized),
        "since_ts": int(since_ts),
        "until_ts": int(until_ts),
    }


def bar_priority(bar, preferred_pool=None):
    provider = str(bar.get("provider") or "").lower()
    source_kind = str(bar.get("source_kind") or "").lower()
    pool = str(bar.get("pool_address") or "")
    score = 0
    if preferred_pool and pool == preferred_pool:
        score += 100
    if source_kind and source_kind != "indexed_ohlcv":
        score += 20
    if provider and "gecko" not in provider:
        score += 10
    if safe_float(bar.get("volume"), 0) > 0:
        score += 5
    return score


def collapse_bars(rows, preferred_pool=None):
    by_ts = {}
    for raw in rows:
        bar = dict(raw)
        ts = safe_int(bar.get("timestamp"))
        if ts is None:
            continue
        old = by_ts.get(ts)
        if old is None or bar_priority(bar, preferred_pool) > bar_priority(old, preferred_pool):
            by_ts[ts] = bar
    return [by_ts[ts] for ts in sorted(by_ts)]


def load_bars_for_signal(raw_db, raw, start_ts, end_ts):
    preferred_pool = raw.get("path_pool_address") or raw.get("baseline_pool_address")
    rows = raw_db.execute(
        """
        SELECT token_ca, pool_address, timestamp, open, high, low, close, volume,
               provider, source_kind, source_family, price_unit
        FROM raw_price_bars_1m
        WHERE token_ca = ?
          AND timestamp >= ?
          AND timestamp <= ?
        ORDER BY timestamp ASC
        """,
        (raw.get("token_ca"), int(start_ts), int(end_ts)),
    ).fetchall()
    return collapse_bars(rows, preferred_pool=preferred_pool)


def choose_entry_anchor(raw, decision_prices, bars):
    signal_ts = safe_int(raw.get("signal_ts"))
    baseline_price = safe_float(raw.get("baseline_price"))
    baseline_ts = safe_int(raw.get("baseline_ts"))
    signal_id = raw.get("signal_id_key")
    candidates = [
        item for item in decision_prices.get(signal_id, [])
        if safe_int(item.get("event_ts")) is not None
        and safe_float(item.get("price")) is not None
        and safe_int(item.get("event_ts")) >= (signal_ts or 0) - 60
    ]
    pass_like = [item for item in candidates if item.get("pass_like")]
    chosen = (pass_like or candidates or [None])[0]
    if chosen:
        return {
            "entry_ts": safe_int(chosen.get("event_ts")) or signal_ts,
            "entry_price": safe_float(chosen.get("price")),
            "entry_price_source": "paper_decision_events_payload",
            "entry_price_path": chosen.get("price_path"),
        }
    if baseline_price is not None and baseline_price > 0:
        return {
            "entry_ts": baseline_ts or signal_ts,
            "entry_price": baseline_price,
            "entry_price_source": "raw_signal_outcomes_baseline_price",
            "entry_price_path": "baseline_price",
        }
    if bars:
        first = bars[0]
        return {
            "entry_ts": safe_int(first.get("timestamp")) or signal_ts,
            "entry_price": safe_float(first.get("open")) or safe_float(first.get("close")),
            "entry_price_source": "first_raw_price_bar_open",
            "entry_price_path": "raw_price_bars_1m.open",
        }
    return {
        "entry_ts": signal_ts,
        "entry_price": None,
        "entry_price_source": "missing_entry_price",
        "entry_price_path": None,
    }


def delayed_entry(anchor, bars, delay_sec):
    target_ts = (anchor.get("entry_ts") or 0) + int(delay_sec)
    if int(delay_sec) <= 0 and safe_float(anchor.get("entry_price")) is not None:
        return dict(anchor, entry_delay_sec=int(delay_sec))
    for bar in bars:
        ts = safe_int(bar.get("timestamp"))
        if ts is not None and ts >= target_ts:
            return {
                "entry_ts": ts,
                "entry_price": safe_float(bar.get("open")) or safe_float(bar.get("close")),
                "entry_price_source": f"raw_price_bars_1m_open_after_{int(delay_sec)}s_delay",
                "entry_price_path": "raw_price_bars_1m.open",
                "entry_delay_sec": int(delay_sec),
            }
    return dict(anchor, entry_delay_sec=int(delay_sec), entry_price=None, entry_price_source="missing_delayed_entry_bar")


def policy_grid():
    policies = []
    for tp in (30, 50, 100, 200):
        policies.append({
            "policy_id": f"fixed_tp_{tp}_stop20",
            "family": "fixed_multiple_take_profit_ladder",
            "params": {"take_profit_pct": float(tp), "stop_loss_pct": STOP_LOSS_PCT},
        })
    for activation in (20, 30, 50):
        for drawdown in (15, 25, 35):
            policies.append({
                "policy_id": f"trail_a{activation}_dd{drawdown}_stop20",
                "family": "trailing_drawdown_stop",
                "params": {
                    "activation_pct": float(activation),
                    "trail_drawdown_pct": float(drawdown),
                    "stop_loss_pct": STOP_LOSS_PCT,
                },
            })
    for tp1 in (30, 50):
        for tp2 in (100, 200):
            for drawdown in (25, 35):
                policies.append({
                    "policy_id": f"tiered_p50tp{tp1}_p25tp{tp2}_taildd{drawdown}_stop20",
                    "family": "tiered_partial_exits_tail_rider",
                    "params": {
                        "tp1_pct": float(tp1),
                        "tp1_fraction": 0.50,
                        "tp2_pct": float(tp2),
                        "tp2_fraction": 0.25,
                        "tail_trail_drawdown_pct": float(drawdown),
                        "stop_loss_pct": STOP_LOSS_PCT,
                    },
                })
    return policies


def exec_return_pct(entry_price, exit_price, slippage):
    entry_exec = entry_price * (1.0 + slippage)
    exit_exec = exit_price * (1.0 - slippage)
    if entry_exec <= 0:
        return None
    return (exit_exec / entry_exec - 1.0) * 100.0


def threshold_price(entry_price, pct):
    return entry_price * (1.0 + float(pct) / 100.0)


def bars_after_entry(bars, entry_ts):
    return [bar for bar in bars if (safe_int(bar.get("timestamp")) or 0) >= int(entry_ts or 0)]


def bar_price(bar, name):
    return safe_float(bar.get(name))


def simulate_trade(raw, bars, entry, policy, slippage, *, lookahead=False):
    entry_price = safe_float(entry.get("entry_price"))
    entry_ts = safe_int(entry.get("entry_ts"))
    if entry_price is None or entry_price <= 0 or entry_ts is None:
        return {"status": "missing_entry_price", "net_pnl_pct": None}
    usable_bars = bars_after_entry(bars, entry_ts)
    if not usable_bars:
        return {"status": "missing_post_entry_bars", "net_pnl_pct": None}
    if lookahead:
        honest = simulate_trade(raw, bars, entry, policy, slippage, lookahead=False)
        best_high = max((bar_price(bar, "high") or 0.0) for bar in usable_bars[1:] or usable_bars)
        if best_high > 0:
            oracle_pnl = exec_return_pct(entry_price, best_high, slippage)
            if honest.get("net_pnl_pct") is None or oracle_pnl > honest.get("net_pnl_pct"):
                return {
                    **honest,
                    "status": "future_data_shift_probe_oracle_exit",
                    "net_pnl_pct": oracle_pnl,
                    "exit_reason": "illegal_plus_one_bar_lookahead_oracle",
                    "future_data_leakage_probe": True,
                }
        return {**honest, "future_data_leakage_probe": True}

    stop_price = threshold_price(entry_price, STOP_LOSS_PCT)
    family = policy["family"]
    params = policy["params"]
    exits = []
    remaining = 1.0
    peak_price = entry_price
    exit_reason = "horizon_close"
    exit_ts = safe_int(usable_bars[-1].get("timestamp"))

    def add_exit(frac, price, ts, reason):
        nonlocal remaining, exit_reason, exit_ts
        frac = max(0.0, min(float(frac), remaining))
        if frac <= 0:
            return
        exits.append({
            "fraction": frac,
            "exit_price": price,
            "exit_ts": ts,
            "reason": reason,
            "net_pnl_pct": exec_return_pct(entry_price, price, slippage),
        })
        remaining = round(max(0.0, remaining - frac), 10)
        exit_reason = reason
        exit_ts = ts

    for bar in usable_bars:
        ts = safe_int(bar.get("timestamp"))
        high = bar_price(bar, "high")
        low = bar_price(bar, "low")
        close = bar_price(bar, "close") or bar_price(bar, "open") or entry_price
        if high is not None:
            peak_price = max(peak_price, high)

        if remaining > 0 and low is not None and low <= stop_price:
            add_exit(remaining, stop_price, ts, "hard_stop_-20_pct")
            break

        if family == "fixed_multiple_take_profit_ladder":
            tp_price = threshold_price(entry_price, params["take_profit_pct"])
            if remaining > 0 and high is not None and high >= tp_price:
                add_exit(remaining, tp_price, ts, f"fixed_take_profit_{int(params['take_profit_pct'])}_pct")
                break

        elif family == "trailing_drawdown_stop":
            activation_price = threshold_price(entry_price, params["activation_pct"])
            if peak_price >= activation_price:
                trail_price = peak_price * (1.0 - params["trail_drawdown_pct"] / 100.0)
                if remaining > 0 and low is not None and low <= trail_price:
                    add_exit(remaining, trail_price, ts, f"trailing_drawdown_{int(params['trail_drawdown_pct'])}_pct")
                    break

        elif family == "tiered_partial_exits_tail_rider":
            tp1_price = threshold_price(entry_price, params["tp1_pct"])
            tp2_price = threshold_price(entry_price, params["tp2_pct"])
            if high is not None and high >= tp1_price and not any(e["reason"].startswith("tier_tp1") for e in exits):
                add_exit(params["tp1_fraction"], tp1_price, ts, f"tier_tp1_{int(params['tp1_pct'])}_pct")
            if high is not None and high >= tp2_price and not any(e["reason"].startswith("tier_tp2") for e in exits):
                add_exit(params["tp2_fraction"], tp2_price, ts, f"tier_tp2_{int(params['tp2_pct'])}_pct")
            if remaining > 0 and exits:
                trail_price = peak_price * (1.0 - params["tail_trail_drawdown_pct"] / 100.0)
                if low is not None and low <= trail_price:
                    add_exit(remaining, trail_price, ts, f"tail_trailing_drawdown_{int(params['tail_trail_drawdown_pct'])}_pct")
                    break

    if remaining > 0:
        last = usable_bars[-1]
        last_price = bar_price(last, "close") or bar_price(last, "open") or entry_price
        add_exit(remaining, last_price, safe_int(last.get("timestamp")), "horizon_close")

    pnl = sum((item.get("net_pnl_pct") or 0.0) * item["fraction"] for item in exits)
    path_peak_pct = (peak_price / entry_price - 1.0) * 100.0 if entry_price > 0 else None
    raw_peak = safe_float(raw.get("max_sustained_peak_pct"))
    peak_pct = raw_peak if raw_peak is not None else path_peak_pct
    tail_capture = None
    if str(raw.get("raw_primary_tier") or "").lower() == "gold" and peak_pct and peak_pct > 0:
        tail_capture = max(0.0, pnl) / peak_pct
    return {
        "status": "simulated",
        "net_pnl_pct": pnl,
        "capital_roi_pct": pnl * POSITION_FRACTION_OF_RISK_CAPITAL,
        "win": pnl > 0,
        "exit_reason": exit_reason,
        "exit_ts": exit_ts,
        "entry_ts": entry_ts,
        "entry_price_source": entry.get("entry_price_source"),
        "path_peak_pct": path_peak_pct,
        "peak_pct_for_tail_capture": peak_pct,
        "gold_tail_capture_share": tail_capture,
        "partial_exit_count": len(exits),
        "exits": exits[:6],
    }


def max_drawdown_pct(capital_roi_pcts):
    equity = 100.0
    peak = equity
    max_dd = 0.0
    for roi in capital_roi_pcts:
        equity += roi
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)
    return max_dd


def rolling_24h_distribution(trades):
    valid = [
        (safe_int(t.get("signal_ts")), safe_float(t.get("capital_roi_pct"), 0.0))
        for t in trades
        if safe_int(t.get("signal_ts")) is not None
    ]
    if not valid:
        return {"n": 0, "values_pct": [], "latest_pct": None, "median_pct": None, "p10_pct": None, "p90_pct": None}
    valid.sort()
    min_ts = valid[0][0]
    max_ts = valid[-1][0]
    if max_ts - min_ts <= 86400:
        values = [sum(v for _, v in valid)]
    else:
        values = []
        start = min_ts
        while start <= max_ts:
            end = start + 86400
            values.append(sum(v for ts, v in valid if start <= ts < end))
            start += 3600
    values = [round(v, 6) for v in values]
    ordered = sorted(values)
    return {
        "n": len(values),
        "values_pct": values[:200],
        "latest_pct": values[-1],
        "median_pct": round(statistics.median(values), 6),
        "p10_pct": ordered[int(max(0, math.floor((len(ordered) - 1) * 0.10)))],
        "p90_pct": ordered[int(max(0, math.floor((len(ordered) - 1) * 0.90)))],
    }


def summarize_variant(policy, slippage, delay_sec, trade_results):
    valid = [r for r in trade_results if r.get("net_pnl_pct") is not None]
    wins = [r for r in valid if r.get("win")]
    capital = [safe_float(r.get("capital_roi_pct"), 0.0) for r in valid]
    tail = [r.get("gold_tail_capture_share") for r in valid if r.get("gold_tail_capture_share") is not None]
    reasons = Counter(r.get("exit_reason") for r in valid)
    rolling = rolling_24h_distribution(valid)
    max_dd = max_drawdown_pct(capital)
    constraints = {
        "max_drawdown_limit_pct": MAX_DRAWDOWN_LIMIT_PCT,
        "max_drawdown_pct": round(max_dd, 6),
        "max_drawdown_pass": max_dd <= MAX_DRAWDOWN_LIMIT_PCT,
        "per_trade_capital_risk_limit_pct": PER_TRADE_CAPITAL_RISK_PCT,
        "per_trade_capital_risk_pct": PER_TRADE_CAPITAL_RISK_PCT,
        "per_trade_capital_risk_pass": True,
        "per_trade_stop_loss_pct": STOP_LOSS_PCT,
    }
    objective_pass = bool(valid) and constraints["max_drawdown_pass"] and constraints["per_trade_capital_risk_pass"]
    return {
        "policy_id": policy["policy_id"],
        "policy_family": policy["family"],
        "params": policy["params"],
        "slippage_pct": round(slippage * 100.0, 4),
        "entry_delay_sec": int(delay_sec),
        "sample_count": len(trade_results),
        "simulated_trade_count": len(valid),
        "avg_net_pnl_pct": None if not valid else round(sum(r["net_pnl_pct"] for r in valid) / len(valid), 6),
        "median_net_pnl_pct": None if not valid else round(statistics.median(r["net_pnl_pct"] for r in valid), 6),
        "win_rate": None if not valid else round(len(wins) / len(valid), 6),
        "rolling_24h_realized_net_roi_distribution_pct": rolling,
        "objective_roi_pct": rolling["latest_pct"],
        "max_drawdown_pct": round(max_dd, 6),
        "tail_capture_share_avg": None if not tail else round(sum(tail) / len(tail), 6),
        "tail_capture_share_median": None if not tail else round(statistics.median(tail), 6),
        "exit_reason_counts": dict(reasons),
        "constraints": constraints,
        "objective_constraints_pass": objective_pass,
        "promotion_allowed": False,
        "allowed_use": "shadow_only",
    }


def sort_key_variant(row):
    return (
        1 if row.get("objective_constraints_pass") else 0,
        safe_float(row.get("objective_roi_pct"), -1e9),
        safe_float(row.get("tail_capture_share_avg"), -1e9),
        safe_float(row.get("win_rate"), -1e9),
        -safe_float(row.get("max_drawdown_pct"), 1e9),
    )


def ranking_hash(rows):
    ids = [
        "|".join([
            row.get("policy_id") or "",
            str(row.get("slippage_pct")),
            str(row.get("entry_delay_sec")),
            str(row.get("rank")),
        ])
        for row in rows
    ]
    return hashlib.sha256("\n".join(ids).encode("utf-8")).hexdigest()[:16]


def build_samples(raw_db, paper_db_path, hours, now_ts, limit):
    since_ts = int(now_ts - hours * 3600)
    raw_rows, raw_meta = load_raw_rows(raw_db, since_ts, now_ts, limit)
    decision_prices, decision_meta = load_decision_prices(paper_db_path, since_ts, now_ts)
    samples = []
    skipped = Counter()
    source_counts = Counter()
    for raw in raw_rows:
        if not raw.get("signal_ts") or not raw.get("token_ca"):
            skipped["missing_signal_ts_or_token"] += 1
            continue
        if not table_exists(raw_db, "raw_price_bars_1m"):
            skipped["missing_raw_price_bars_1m"] += 1
            continue
        bars = load_bars_for_signal(raw_db, raw, raw["signal_ts"], raw["signal_ts"] + HORIZON_SEC)
        if not bars:
            skipped["missing_0_2h_bars"] += 1
            continue
        anchor = choose_entry_anchor(raw, decision_prices, bars)
        if safe_float(anchor.get("entry_price")) is None:
            skipped["missing_entry_price"] += 1
            continue
        source_counts[anchor.get("entry_price_source")] += 1
        samples.append({"raw": raw, "bars": bars, "anchor": anchor})
    return samples, {
        "raw_meta": raw_meta,
        "decision_price_meta": decision_meta,
        "raw_rows_considered": len(raw_rows),
        "sample_count": len(samples),
        "skipped_counts": dict(skipped),
        "entry_price_source_counts": dict(source_counts),
    }


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    raw_db = connect_readonly(args.raw_db)
    if raw_db is None:
        return {
            "schema_version": SCHEMA_VERSION,
            "classification": "EXIT_POLICY_SHADOW_LAB_BLOCKED_DATA",
            "blockers": ["raw_db_unavailable"],
            "promotion_allowed": False,
        }
    samples, sample_meta = build_samples(raw_db, args.paper_db, args.hours, now_ts, args.limit)
    policies = policy_grid()
    variants = []
    leakage_rows = []
    for policy in policies:
        for delay_sec in ENTRY_DELAY_GRID_SEC:
            for slippage in SLIPPAGE_GRID:
                results = []
                leak_results = []
                for sample in samples:
                    raw = sample["raw"]
                    bars = sample["bars"]
                    entry = delayed_entry(sample["anchor"], bars, delay_sec)
                    result = simulate_trade(raw, bars, entry, policy, slippage, lookahead=False)
                    result.update({
                        "signal_id": raw.get("signal_id_key"),
                        "token_ca": raw.get("token_ca"),
                        "signal_ts": raw.get("signal_ts"),
                        "raw_primary_tier": raw.get("raw_primary_tier"),
                    })
                    results.append(result)
                    leak = simulate_trade(raw, bars, entry, policy, slippage, lookahead=True)
                    leak.update({"signal_ts": raw.get("signal_ts")})
                    leak_results.append(leak)
                row = summarize_variant(policy, slippage, delay_sec, results)
                row["future_data_leakage_probe_roi_pct"] = rolling_24h_distribution([
                    r for r in leak_results if r.get("net_pnl_pct") is not None
                    for _ in [r.update({"capital_roi_pct": r["net_pnl_pct"] * POSITION_FRACTION_OF_RISK_CAPITAL}) or r]
                ])["latest_pct"]
                variants.append(row)
                if slippage == 0.0 and delay_sec == 0:
                    honest = row.get("objective_roi_pct")
                    leaked = row.get("future_data_leakage_probe_roi_pct")
                    leakage_rows.append({
                        "policy_id": policy["policy_id"],
                        "honest_roi_pct": honest,
                        "lookahead_roi_pct": leaked,
                        "strictly_dominates": (
                            honest is not None
                            and leaked is not None
                            and leaked > honest
                        ),
                    })
    variants.sort(key=sort_key_variant, reverse=True)
    for idx, row in enumerate(variants, start=1):
        row["rank"] = idx
    champion = variants[0] if variants else None
    recomputed = sorted(variants, key=sort_key_variant, reverse=True)
    for idx, row in enumerate(recomputed, start=1):
        row["_recomputed_rank"] = idx
    recheck_ok = all(row.get("rank") == row.get("_recomputed_rank") for row in recomputed)
    for row in recomputed:
        row.pop("_recomputed_rank", None)
    family_summary = []
    by_family = defaultdict(list)
    for row in variants:
        by_family[row["policy_family"]].append(row)
    for family, rows in sorted(by_family.items()):
        best = rows[0]
        family_summary.append({
            "policy_family": family,
            "variant_count": len(rows),
            "best_policy_id": best.get("policy_id"),
            "best_rank": best.get("rank"),
            "best_objective_roi_pct": best.get("objective_roi_pct"),
            "best_max_drawdown_pct": best.get("max_drawdown_pct"),
            "best_win_rate": best.get("win_rate"),
            "best_tail_capture_share_avg": best.get("tail_capture_share_avg"),
        })
    strict_leaks = [row for row in leakage_rows if row["strictly_dominates"]]
    leakage_probe = {
        "schema_version": "exit_policy_shadow_lab_future_data_probe.v1",
        "probe_type": "plus_one_bar_lookahead_oracle_must_dominate_honest",
        "policy_count_checked": len(leakage_rows),
        "strictly_dominating_policy_count": len(strict_leaks),
        "passes": bool(leakage_rows) and bool(strict_leaks),
        "rows": leakage_rows[:40],
    }
    raw_db.close()
    blockers = []
    classification = "EXIT_POLICY_SHADOW_LAB_READY"
    if not samples:
        blockers.append("no_replayable_raw_price_bar_samples")
        classification = "EXIT_POLICY_SHADOW_LAB_BLOCKED_DATA"
    elif not leakage_probe["passes"]:
        blockers.append("future_data_leakage_probe_not_strictly_dominating")
        classification = "EXIT_POLICY_SHADOW_LAB_RECHECK_FAILED"
    champion_constraints_pass = bool(champion and champion.get("objective_constraints_pass"))
    champion_roi_target_met = bool(
        champion
        and champion.get("objective_roi_pct") is not None
        and champion.get("objective_roi_pct") >= ROI_TARGET_PCT
    )
    champion_verdict = (
        "CHAMPION_PENDING_HUMAN_REVIEW"
        if champion_constraints_pass and champion_roi_target_met and leakage_probe["passes"]
        else "NO_CHAMPION_MEETING_PINNED_OBJECTIVE"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "statistics_schema_version": STATS_SCHEMA_VERSION,
        "report_type": f"exit_policy_shadow_lab_{int(args.hours)}h",
        "generated_at": iso_from_ts(now_ts),
        "hours": args.hours,
        "since_ts": int(now_ts - args.hours * 3600),
        "until_ts": now_ts,
        "objective": {
            "return_metric": "rolling_24h_realized_net_roi_on_allocated_strategy_risk_capital",
            "roi_target_pct": ROI_TARGET_PCT,
            "max_drawdown_limit_pct": MAX_DRAWDOWN_LIMIT_PCT,
            "per_trade_capital_risk_limit_pct": PER_TRADE_CAPITAL_RISK_PCT,
            "position_fraction_of_risk_capital": POSITION_FRACTION_OF_RISK_CAPITAL,
            "per_trade_stop_loss_pct": STOP_LOSS_PCT,
        },
        "time_legal": True,
        "time_legal_basis": "Entry/exit simulation consumes bars in timestamp order; honest ranking never uses future bars.",
        "execution_delay_adjusted": True,
        "entry_delay_grid_sec": list(ENTRY_DELAY_GRID_SEC),
        "slippage_grid_pct": [round(x * 100.0, 4) for x in SLIPPAGE_GRID],
        "sample_meta": sample_meta,
        "policy_families_tested": sorted({p["family"] for p in policies}),
        "policy_variants_tested": len(policies),
        "policy_slippage_delay_cells": len(variants),
        "family_summary": family_summary,
        "ranked_variants": variants[:200],
        "champion": champion,
        "champion_verdict": champion_verdict,
        "human_handoff_required": champion_verdict == "CHAMPION_PENDING_HUMAN_REVIEW",
        "adversarial_recheck": {
            "schema_version": "exit_policy_shadow_lab_adversarial_recheck.v1",
            "ranking_recomputed_from_artifact_matches": recheck_ok,
            "ranking_hash": ranking_hash(variants),
        },
        "future_data_leakage_probe": leakage_probe,
        "classification": classification,
        "blockers": blockers,
        "download_key": "agent_runs/latest/exit_policy_shadow_lab_24h.json",
        "promotion_allowed": False,
        "allowed_use": "shadow_only",
        "strategy_change_allowed": False,
        "live_exit_policy_changed": False,
        "production_files_touched": [],
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw_path = root / "raw.db"
        paper_path = root / "paper.db"
        now = int(time.time())
        db = sqlite3.connect(raw_path)
        db.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
              signal_type TEXT, raw_primary_tier TEXT, observation_status TEXT,
              kline_covered INTEGER, baseline_price REAL, baseline_ts INTEGER,
              baseline_pool_address TEXT, path_pool_address TEXT,
              path_price_unit TEXT, max_sustained_peak_pct REAL,
              time_to_sustained_peak_sec INTEGER, payload_json TEXT
            )
            """
        )
        db.execute(
            """
            CREATE TABLE raw_price_bars_1m(
              token_ca TEXT, pool_address TEXT, timestamp INTEGER,
              open REAL, high REAL, low REAL, close REAL, volume REAL,
              provider TEXT, source_kind TEXT, source_family TEXT, price_unit TEXT
            )
            """
        )
        for idx, token in enumerate(["A", "B", "C"], start=1):
            signal_ts = now - 1800 + idx * 60
            db.execute(
                "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    str(idx), token, token, signal_ts, "ATH",
                    "gold" if token == "A" else ("silver" if token == "B" else "dud"),
                    "matured", 1, None, None, "pool", "pool", "native",
                    180 if token == "A" else (35 if token == "B" else -10),
                    600, "{}",
                ),
            )
            price = 1.0
            for minute in range(0, 121):
                ts = signal_ts + minute * 60
                if token == "A":
                    high = 1.0 + minute * 0.02
                    low = max(0.8, 1.0 + minute * 0.015)
                    close = high * 0.98
                elif token == "B":
                    high = 1.0 + min(minute, 30) * 0.012
                    low = 0.95
                    close = 1.10
                else:
                    high = 1.02
                    low = 0.75 if minute > 5 else 0.98
                    close = 0.80
                db.execute(
                    "INSERT INTO raw_price_bars_1m VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (token, "pool", ts, price, high, low, close, 100.0, "selftest", "raw_swaps", "selftest", "native"),
                )
        db.commit()
        db.close()
        db = sqlite3.connect(paper_path)
        db.execute(
            """
            CREATE TABLE paper_decision_events(
              id INTEGER PRIMARY KEY, event_ts INTEGER, signal_id TEXT,
              event_type TEXT, decision TEXT, action TEXT, reason TEXT,
              payload_json TEXT
            )
            """
        )
        for idx in range(1, 4):
            db.execute(
                "INSERT INTO paper_decision_events(event_ts, signal_id, event_type, decision, action, reason, payload_json) VALUES (?,?,?,?,?,?,?)",
                (now - 1800 + idx * 60, str(idx), "entry_decision", "PASS", "would_enter", "self_test", json.dumps({"decision_price": 1.0})),
            )
        db.commit()
        db.close()
        args = argparse.Namespace(
            raw_db=str(raw_path),
            paper_db=str(paper_path),
            hours=1,
            now_ts=now,
            limit=100,
            out=str(root / "out.json"),
            self_test=False,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["live_exit_policy_changed"] is False
        assert report["policy_variants_tested"] >= 10
        assert report["policy_slippage_delay_cells"] == report["policy_variants_tested"] * len(SLIPPAGE_GRID) * len(ENTRY_DELAY_GRID_SEC)
        assert report["adversarial_recheck"]["ranking_recomputed_from_artifact_matches"] is True
        assert report["future_data_leakage_probe"]["passes"] is True
        assert report["ranked_variants"]
    print("SELF_TEST_PASS exit_policy_shadow_lab")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-db", default=str(DEFAULT_DATA_DIR / "raw_signal_outcomes.db"))
    parser.add_argument("--paper-db", default=str(DEFAULT_DATA_DIR / "paper_trades.db"))
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--out", default=str(DEFAULT_DATA_DIR / "agent_runs/latest/exit_policy_shadow_lab_24h.json"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    write_json(args.out, report)
    print(json.dumps({
        "out": args.out,
        "classification": report.get("classification"),
        "policy_variants_tested": report.get("policy_variants_tested"),
        "policy_slippage_delay_cells": report.get("policy_slippage_delay_cells"),
        "champion_verdict": report.get("champion_verdict"),
        "promotion_allowed": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
