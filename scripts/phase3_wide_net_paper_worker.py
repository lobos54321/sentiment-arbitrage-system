#!/usr/bin/env python3
"""Run the Phase 3 wide-net paper experiment ledger worker.

This is an independent paper-level experiment writer. It never calls the live or
paper executor, never inserts into production paper_trades, and never changes
strategy, gates, final_entry_contract, A_CLASS, canary, wallet, or risk. Its only
write target is the Phase 3 experiment ledger DB/table.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import tempfile
import time
from collections import Counter
from pathlib import Path


SCHEMA_VERSION = "phase3_wide_net_paper_worker.v1"
DEFAULT_DATA_DIR = Path("/app/data")
DEFAULT_RUN_DIR = DEFAULT_DATA_DIR / "agent_runs/latest"
DEFAULT_CONTRACT = DEFAULT_RUN_DIR / "phase3_wide_net_paper_contract.json"
DEFAULT_RAW_DB = DEFAULT_DATA_DIR / "raw_signal_outcomes.db"
DEFAULT_PAPER_DB = DEFAULT_DATA_DIR / "paper_trades.db"
DEFAULT_LEDGER_DB = DEFAULT_DATA_DIR / "phase3_wide_net_paper_contract.db"
DEFAULT_OUT = DEFAULT_RUN_DIR / "phase3_wide_net_paper_experiment_summary.json"
P7_POLICY_ID = "trail_a50_dd15_stop20"
HORIZON_SEC = 7200
STOP_LOSS_PCT = -20.0


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def load_json(path):
    try:
        if not path or not Path(path).exists():
            return {}
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def safe_int(value, default=None):
    parsed = safe_float(value)
    return default if parsed is None else int(parsed)


def jloads(raw, default=None):
    default = {} if default is None else default
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(raw or "{}")
        return parsed if isinstance(parsed, dict) else default
    except Exception:
        return default


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


def load_raw_rows(raw_db, since_ts, until_ts, limit):
    if not table_exists(raw_db, "raw_signal_outcomes"):
        return [], {"available": False, "reason": "missing_raw_signal_outcomes"}
    cols = table_columns(raw_db, "raw_signal_outcomes")
    needed = [
        "signal_id", "token_ca", "symbol", "signal_ts", "signal_type", "raw_primary_tier",
        "raw_sustained_tier", "observation_status", "kline_covered", "coverage_reason",
        "baseline_price", "baseline_ts", "baseline_pool_address", "path_pool_address",
        "path_price_unit", "max_sustained_peak_pct", "time_to_sustained_peak_sec",
        "payload_json",
    ]
    select = [name if name in cols else f"NULL AS {name}" for name in needed]
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


def choose_entry_anchor(raw, bars):
    signal_ts = safe_int(raw.get("signal_ts"))
    baseline_price = safe_float(raw.get("baseline_price"))
    baseline_ts = safe_int(raw.get("baseline_ts"))
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


def threshold_price(entry_price, pct):
    return entry_price * (1.0 + float(pct) / 100.0)


def exec_return_pct(entry_price, exit_price, slippage):
    entry_exec = entry_price * (1.0 + slippage)
    exit_exec = exit_price * (1.0 - slippage)
    if entry_exec <= 0:
        return None
    return (exit_exec / entry_exec - 1.0) * 100.0


def bars_after_entry(bars, entry_ts):
    return [bar for bar in bars if (safe_int(bar.get("timestamp")) or 0) >= int(entry_ts or 0)]


def simulate_trade(raw, bars, entry, policy, slippage):
    entry_price = safe_float(entry.get("entry_price"))
    entry_ts = safe_int(entry.get("entry_ts"))
    if entry_price is None or entry_price <= 0 or entry_ts is None:
        return {"status": "missing_entry_price", "net_pnl_pct": None}
    usable_bars = bars_after_entry(bars, entry_ts)
    if not usable_bars:
        return {"status": "missing_post_entry_bars", "net_pnl_pct": None}
    stop_price = threshold_price(entry_price, STOP_LOSS_PCT)
    activation_price = threshold_price(entry_price, policy["params"]["activation_pct"])
    peak_price = entry_price
    exit_reason = "horizon_close"
    exit_ts = safe_int(usable_bars[-1].get("timestamp"))
    exit_price = safe_float(usable_bars[-1].get("close")) or safe_float(usable_bars[-1].get("open")) or entry_price
    for bar in usable_bars:
        ts = safe_int(bar.get("timestamp"))
        high = safe_float(bar.get("high"))
        low = safe_float(bar.get("low"))
        if high is not None:
            peak_price = max(peak_price, high)
        if low is not None and low <= stop_price:
            exit_price = stop_price
            exit_ts = ts
            exit_reason = "hard_stop_-20_pct"
            break
        if peak_price >= activation_price:
            trail_price = peak_price * (1.0 - policy["params"]["trail_drawdown_pct"] / 100.0)
            if low is not None and low <= trail_price:
                exit_price = trail_price
                exit_ts = ts
                exit_reason = "trailing_drawdown_15_pct"
                break
    pnl = exec_return_pct(entry_price, exit_price, slippage)
    return {
        "status": "simulated",
        "net_pnl_pct": pnl,
        "win": pnl is not None and pnl > 0,
        "exit_reason": exit_reason,
        "exit_ts": exit_ts,
        "entry_ts": entry_ts,
        "entry_price_source": entry.get("entry_price_source"),
        "path_peak_pct": ((peak_price / entry_price - 1.0) * 100.0) if entry_price > 0 else None,
    }


def connect_ledger(path):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(target, timeout=20)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS phase3_wide_net_paper_ledger (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          signal_id TEXT,
          token_ca TEXT,
          signal_ts INTEGER,
          intended_size_sol REAL,
          quote_executable INTEGER,
          entry_intent_ts INTEGER,
          entry_result TEXT,
          exit_policy_id TEXT,
          hard_stop_pct REAL,
          fees_sol REAL,
          slippage_pct REAL,
          failed_quote_reason TEXT,
          no_fill_reason TEXT,
          timeout_reason TEXT,
          realized_pnl_pct REAL,
          payload_json TEXT,
          created_at INTEGER NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_phase3_wide_net_paper_ledger_signal
        ON phase3_wide_net_paper_ledger(signal_id, token_ca, signal_ts)
        """
    )
    db.commit()
    return db


def p7_policy():
    return {
        "policy_id": P7_POLICY_ID,
        "family": "trailing_drawdown_stop",
        "params": {
            "activation_pct": 50.0,
            "trail_drawdown_pct": 15.0,
            "stop_loss_pct": STOP_LOSS_PCT,
        },
    }


def contract_enabled(contract):
    enablement = contract.get("enablement") if isinstance(contract.get("enablement"), dict) else {}
    return bool(
        contract.get("classification") == "WIDE_NET_PAPER_ENABLED_BY_HUMAN_APPROVAL"
        and enablement.get("paper_experiment_enablement_allowed") is True
    )


def raw_tier(raw):
    return str(raw.get("raw_primary_tier") or raw.get("raw_sustained_tier") or "").lower()


def bar_ts(bar):
    return safe_int(bar.get("timestamp"))


def summarize_closed(values):
    clean = [safe_float(v) for v in values if safe_float(v) is not None]
    if not clean:
        return {
            "closed_count": 0,
            "win_count": 0,
            "loss_count": 0,
            "median_realized_pnl_pct": None,
            "avg_realized_pnl_pct": None,
        }
    return {
        "closed_count": len(clean),
        "win_count": sum(1 for value in clean if value > 0),
        "loss_count": sum(1 for value in clean if value <= 0),
        "median_realized_pnl_pct": round(statistics.median(clean), 6),
        "avg_realized_pnl_pct": round(sum(clean) / len(clean), 6),
    }


def ledger_upsert(db, row):
    db.execute(
        """
        DELETE FROM phase3_wide_net_paper_ledger
        WHERE COALESCE(signal_id, '') = COALESCE(?, '')
          AND COALESCE(token_ca, '') = COALESCE(?, '')
          AND COALESCE(signal_ts, 0) = COALESCE(?, 0)
        """,
        (row.get("signal_id"), row.get("token_ca"), row.get("signal_ts")),
    )
    db.execute(
        """
        INSERT INTO phase3_wide_net_paper_ledger (
          signal_id, token_ca, signal_ts, intended_size_sol, quote_executable,
          entry_intent_ts, entry_result, exit_policy_id, hard_stop_pct, fees_sol,
          slippage_pct, failed_quote_reason, no_fill_reason, timeout_reason,
          realized_pnl_pct, payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row.get("signal_id"),
            row.get("token_ca"),
            row.get("signal_ts"),
            row.get("intended_size_sol"),
            row.get("quote_executable"),
            row.get("entry_intent_ts"),
            row.get("entry_result"),
            row.get("exit_policy_id"),
            row.get("hard_stop_pct"),
            row.get("fees_sol"),
            row.get("slippage_pct"),
            row.get("failed_quote_reason"),
            row.get("no_fill_reason"),
            row.get("timeout_reason"),
            row.get("realized_pnl_pct"),
            json.dumps(row.get("payload") or {}, sort_keys=True),
            int(time.time()),
        ),
    )


def skipped_row(raw, reason, args, payload_extra=None):
    payload = {
        "schema_version": SCHEMA_VERSION,
        "paper_level_only": True,
        "production_impact": "zero",
        "signal_id": raw.get("signal_id_key"),
        "symbol": raw.get("symbol"),
        "raw_primary_tier": raw.get("raw_primary_tier"),
        "reason": reason,
    }
    payload.update(payload_extra or {})
    return {
        "signal_id": raw.get("signal_id_key"),
        "token_ca": raw.get("token_ca"),
        "signal_ts": raw.get("signal_ts"),
        "intended_size_sol": float(args.paper_size_sol),
        "quote_executable": 0,
        "entry_intent_ts": None,
        "entry_result": reason,
        "exit_policy_id": P7_POLICY_ID,
        "hard_stop_pct": -20.0,
        "fees_sol": None,
        "slippage_pct": float(args.slippage_pct),
        "failed_quote_reason": reason if "quote" in reason or "price" in reason else None,
        "no_fill_reason": reason if "bar" in reason or "path" in reason else None,
        "timeout_reason": None,
        "realized_pnl_pct": None,
        "payload": payload,
    }


def build_ledger_row(raw, bars, anchor, result, entry, args):
    last_bar = bars[-1] if bars else {}
    last_bar_ts = bar_ts(last_bar)
    entry_ts = safe_int(entry.get("entry_ts"))
    path_observed_sec = None
    if last_bar_ts is not None and entry_ts is not None:
        path_observed_sec = max(0, last_bar_ts - entry_ts)
    exit_reason = str(result.get("exit_reason") or "")
    matured_path = path_observed_sec is not None and path_observed_sec >= HORIZON_SEC - 60
    closed = bool(exit_reason and (exit_reason != "horizon_close" or matured_path))
    if result.get("status") != "simulated":
        entry_result = f"skipped_{result.get('status') or 'simulation_unavailable'}"
        realized = None
    elif closed:
        entry_result = "paper_closed_shadow"
        realized = result.get("net_pnl_pct")
    else:
        entry_result = "paper_open_collecting_path"
        realized = None

    payload = {
        "schema_version": SCHEMA_VERSION,
        "paper_level_only": True,
        "production_impact": "zero",
        "signal_id": raw.get("signal_id_key"),
        "symbol": raw.get("symbol"),
        "raw_primary_tier": raw.get("raw_primary_tier"),
        "raw_tier": raw_tier(raw),
        "entry_delay_sec": int(args.entry_delay_sec),
        "entry_price": entry.get("entry_price"),
        "entry_price_source": entry.get("entry_price_source"),
        "entry_price_path": entry.get("entry_price_path"),
        "path_bar_count": len(bars),
        "path_observed_sec": path_observed_sec,
        "expected_horizon_sec": HORIZON_SEC,
        "matured_path": matured_path,
        "result": result,
    }
    return {
        "signal_id": raw.get("signal_id_key"),
        "token_ca": raw.get("token_ca"),
        "signal_ts": raw.get("signal_ts"),
        "intended_size_sol": float(args.paper_size_sol),
        "quote_executable": 1,
        "entry_intent_ts": entry_ts,
        "entry_result": entry_result,
        "exit_policy_id": P7_POLICY_ID,
        "hard_stop_pct": -20.0,
        "fees_sol": None,
        "slippage_pct": float(args.slippage_pct),
        "failed_quote_reason": None,
        "no_fill_reason": None,
        "timeout_reason": "path_still_collecting" if entry_result == "paper_open_collecting_path" else None,
        "realized_pnl_pct": realized,
        "payload": payload,
    }


def run(args):
    generated_at = utc_now()
    now_ts = int(args.now_ts or time.time())
    contract = load_json(args.contract)
    if not contract_enabled(contract):
        summary = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at,
            "classification": "WIDE_NET_PAPER_EXPERIMENT_DISABLED",
            "contract_path": str(args.contract),
            "contract_classification": contract.get("classification"),
            "enabled": False,
            "promotion_allowed": False,
            "production_impact": "zero",
            "next_action": "wait_for_enabled_phase3_wide_net_paper_contract",
        }
        write_json(args.out, summary)
        return summary

    raw_db = connect_readonly(args.raw_db)
    if raw_db is None:
        summary = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at,
            "classification": "WIDE_NET_PAPER_EXPERIMENT_BLOCKED_DATA",
            "blockers": ["raw_signal_outcomes_db_unavailable"],
            "enabled": True,
            "promotion_allowed": False,
            "production_impact": "zero",
        }
        write_json(args.out, summary)
        return summary

    since_ts = now_ts - int(float(args.lookback_hours) * 3600)
    raw_rows, raw_meta = load_raw_rows(raw_db, since_ts, now_ts, int(args.limit))
    decision_meta = {
        "available": False,
        "source": "paper_decision_events",
        "reason": "phase3_wide_net_worker_uses_raw_baseline_or_first_bar_entry_anchor",
    }
    policy = p7_policy()
    slippage = float(args.slippage_pct) / 100.0
    ledger_db = connect_ledger(args.ledger_db)
    counters = Counter()
    realized_values = []
    raw_tier_counts = Counter()
    first_signal_ts = None
    last_signal_ts = None

    for raw in raw_rows:
        counters["raw_rows_considered"] += 1
        raw_tier_counts[raw_tier(raw) or "unknown"] += 1
        signal_ts = safe_int(raw.get("signal_ts"))
        if signal_ts is not None:
            first_signal_ts = signal_ts if first_signal_ts is None else min(first_signal_ts, signal_ts)
            last_signal_ts = signal_ts if last_signal_ts is None else max(last_signal_ts, signal_ts)
        if not table_exists(raw_db, "raw_price_bars_1m"):
            row = skipped_row(raw, "skipped_missing_raw_price_bars_1m", args)
        else:
            bars = load_bars_for_signal(raw_db, raw, raw.get("signal_ts"), raw.get("signal_ts") + HORIZON_SEC)
            if not bars:
                row = skipped_row(raw, "skipped_missing_0_2h_bars", args)
            else:
                anchor = choose_entry_anchor(raw, bars)
                entry = delayed_entry(anchor, bars, int(args.entry_delay_sec))
                if safe_float(entry.get("entry_price")) is None:
                    row = skipped_row(raw, "skipped_missing_entry_price", args, {
                        "entry_price_source": entry.get("entry_price_source"),
                    })
                else:
                    result = simulate_trade(raw, bars, entry, policy, slippage)
                    row = build_ledger_row(raw, bars, anchor, result, entry, args)
        ledger_upsert(ledger_db, row)
        counters[row["entry_result"]] += 1
        if row.get("quote_executable"):
            counters["quote_executable_shadow_rows"] += 1
        if row.get("realized_pnl_pct") is not None:
            realized_values.append(row.get("realized_pnl_pct"))

    ledger_db.commit()
    ledger_db.close()
    raw_db.close()
    closed = summarize_closed(realized_values)
    classification = "WIDE_NET_PAPER_EXPERIMENT_RUNNING"
    if counters["raw_rows_considered"] == 0:
        classification = "WIDE_NET_PAPER_EXPERIMENT_WAITING_FOR_SIGNALS"
    summary = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "classification": classification,
        "enabled": True,
        "contract_path": str(args.contract),
        "contract_classification": contract.get("classification"),
        "ledger_db": str(args.ledger_db),
        "ledger_table": "phase3_wide_net_paper_ledger",
        "lookback_hours": float(args.lookback_hours),
        "since_ts": int(since_ts),
        "until_ts": int(now_ts),
        "first_signal_ts": first_signal_ts,
        "last_signal_ts": last_signal_ts,
        "experiment_defaults": {
            "entry_scope": "all_eligible_matured_raw_signals",
            "paper_size_sol": float(args.paper_size_sol),
            "exit_policy_id": P7_POLICY_ID,
            "entry_delay_sec": int(args.entry_delay_sec),
            "slippage_pct": float(args.slippage_pct),
            "hard_stop_pct": -20.0,
            "horizon_sec": HORIZON_SEC,
        },
        "raw_source": raw_meta,
        "decision_price_source": decision_meta,
        "counts": dict(counters),
        "raw_tier_counts": dict(raw_tier_counts),
        "realized_summary": closed,
        "guardrails": {
            "promotion_allowed": False,
            "production_strategy_change_allowed": False,
            "entry_policy_change_allowed": False,
            "gate_change_allowed": False,
            "final_entry_contract_change_allowed": False,
            "executor_change_allowed": False,
            "canary_or_risk_change_allowed": False,
            "production_impact": "zero",
        },
        "next_action": "continue_collecting_phase3_wide_net_paper_evidence",
    }
    write_json(args.out, summary)
    return summary


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw_db_path = root / "raw.db"
        paper_db_path = root / "paper.db"
        ledger_db_path = root / "ledger.db"
        contract_path = root / "contract.json"
        out_path = root / "summary.json"
        contract = {
            "classification": "WIDE_NET_PAPER_ENABLED_BY_HUMAN_APPROVAL",
            "enablement": {"paper_experiment_enablement_allowed": True},
        }
        write_json(contract_path, contract)
        raw_db = sqlite3.connect(raw_db_path)
        raw_db.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
              signal_type TEXT, raw_primary_tier TEXT, observation_status TEXT,
              kline_covered INTEGER, coverage_reason TEXT, baseline_price REAL,
              baseline_ts INTEGER, baseline_pool_address TEXT, path_pool_address TEXT,
              path_price_unit TEXT, max_sustained_peak_pct REAL,
              time_to_sustained_peak_sec INTEGER, payload_json TEXT
            )
            """
        )
        raw_db.execute(
            """
            CREATE TABLE raw_price_bars_1m(
              token_ca TEXT, pool_address TEXT, timestamp INTEGER, open REAL, high REAL,
              low REAL, close REAL, volume REAL, provider TEXT, source_kind TEXT,
              source_family TEXT, price_unit TEXT
            )
            """
        )
        now_ts = 1_800_000_000
        raw_db.execute(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "sig-1", "token-a", "AAA", now_ts - 3600, "premium", "gold", "matured",
                1, None, 1.0, now_ts - 3600, "pool-a", "pool-a", "usd", 70.0, 120,
                json.dumps({}),
            ),
        )
        bars = [
            (0, 1.00, 1.10, 0.95, 1.00),
            (60, 1.00, 1.55, 1.00, 1.50),
            (120, 1.50, 1.70, 1.30, 1.35),
        ]
        for offset, open_, high, low, close in bars:
            raw_db.execute(
                "INSERT INTO raw_price_bars_1m VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                ("token-a", "pool-a", now_ts - 3600 + offset, open_, high, low, close, 100.0, "test", "dex", "test", "usd"),
            )
        raw_db.commit()
        raw_db.close()
        sqlite3.connect(paper_db_path).close()
        args = argparse.Namespace(
            contract=str(contract_path),
            raw_db=str(raw_db_path),
            paper_db=str(paper_db_path),
            ledger_db=str(ledger_db_path),
            out=str(out_path),
            lookback_hours=24,
            limit=100,
            paper_size_sol=0.001,
            entry_delay_sec=5,
            slippage_pct=0.0,
            now_ts=now_ts,
        )
        summary = run(args)
        assert summary["classification"] == "WIDE_NET_PAPER_EXPERIMENT_RUNNING"
        assert summary["counts"]["raw_rows_considered"] == 1
        assert summary["realized_summary"]["closed_count"] == 1
        db = sqlite3.connect(ledger_db_path)
        count = db.execute("SELECT COUNT(*) FROM phase3_wide_net_paper_ledger").fetchone()[0]
        db.close()
        assert count == 1
    print("SELF_TEST_PASS phase3_wide_net_paper_worker")


def parse_args():
    parser = argparse.ArgumentParser(description="Run Phase 3 wide-net paper experiment ledger worker.")
    parser.add_argument("--contract", default=str(DEFAULT_CONTRACT))
    parser.add_argument("--raw-db", default=str(DEFAULT_RAW_DB))
    parser.add_argument("--paper-db", default=str(DEFAULT_PAPER_DB))
    parser.add_argument("--ledger-db", default=str(DEFAULT_LEDGER_DB))
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--lookback-hours", default=24, type=float)
    parser.add_argument("--limit", default=2000, type=int)
    parser.add_argument("--paper-size-sol", default=0.001, type=float)
    parser.add_argument("--entry-delay-sec", default=5, type=int)
    parser.add_argument("--slippage-pct", default=3.0, type=float)
    parser.add_argument("--now-ts", default=0, type=int)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    summary = run(args)
    print(json.dumps({
        "schema_version": summary.get("schema_version"),
        "classification": summary.get("classification"),
        "enabled": summary.get("enabled"),
        "counts": summary.get("counts"),
        "realized_summary": summary.get("realized_summary"),
        "promotion_allowed": False,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
