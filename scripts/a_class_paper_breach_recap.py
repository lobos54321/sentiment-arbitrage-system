#!/usr/bin/env python3
"""Build a read-only recap artifact for a paper-only A_CLASS circuit breach.

The recap explains the trade timeline and why a paper-only loss should feed the
P2.1 recovery contract without changing strategy, gates, A_CLASS mode, executor,
wallet, canary, or risk.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import tempfile
import time
from pathlib import Path


STABLECOIN_SYMBOLS = {"USDC", "USDT", "USD", "DAI", "FDUSD", "PYUSD"}
SCHEMA_VERSION = "a_class_paper_breach_recap.v1"


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def safe_int(value, default=None):
    parsed = safe_float(value)
    return default if parsed is None else int(parsed)


def iso(ts):
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(ts)))
    except Exception:
        return None


def table_exists(db, table):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def table_columns(db, table):
    if not table_exists(db, table):
        return set()
    return {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def jloads(raw, default=None):
    default = {} if default is None else default
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, (dict, list)) else default
    except Exception:
        return default


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def select_trade(db, trade_id):
    if not table_exists(db, "paper_trades"):
        return None
    cols = table_columns(db, "paper_trades")
    selected = []
    for col in (
        "id", "token_ca", "symbol", "premium_signal_id", "signal_ts", "entry_ts", "exit_ts",
        "entry_price", "exit_price", "pnl_pct", "exit_reason", "paper_only", "entry_mode",
        "signal_type", "signal_route", "position_size_sol", "entry_execution_json",
        "exit_execution_json", "monitor_state_json", "entry_execution_audit_json",
        "exit_execution_audit_json", "intervention_flags_json", "created_at",
    ):
        selected.append(col if col in cols else f"NULL AS {col}")
    row = db.execute(
        f"SELECT {', '.join(selected)} FROM paper_trades WHERE id=?",
        (int(trade_id),),
    ).fetchone()
    return dict(row) if row else None


def load_path_samples(db, trade_id):
    if not table_exists(db, "paper_trade_path_samples"):
        return []
    cols = table_columns(db, "paper_trade_path_samples")
    selected = []
    for col in (
        "sample_ts", "action", "reason", "mark_price", "mark_pnl", "quote_price",
        "quote_pnl", "peak_pnl", "sold_pct", "mark_source", "quote_success",
        "quote_failure_reason", "quote_out_sol", "payload_json",
    ):
        selected.append(col if col in cols else f"NULL AS {col}")
    rows = db.execute(
        f"""
        SELECT {', '.join(selected)}
        FROM paper_trade_path_samples
        WHERE trade_id=?
        ORDER BY sample_ts ASC
        """,
        (int(trade_id),),
    ).fetchall()
    return [dict(row) for row in rows]


def load_decision_events(db, trade):
    if not table_exists(db, "paper_decision_events"):
        return []
    cols = table_columns(db, "paper_decision_events")
    if "event_ts" not in cols:
        return []
    selected = []
    for col in (
        "event_ts", "signal_id", "component", "event_type", "decision", "action",
        "reason", "token_ca", "symbol", "trade_id", "data_source", "payload_json",
    ):
        selected.append(col if col in cols else f"NULL AS {col}")
    clause_params = []
    clauses = []
    token = str(trade.get("token_ca") or "")
    signal_id = trade.get("premium_signal_id")
    entry_ts = safe_int(trade.get("entry_ts"), 0) or 0
    exit_ts = safe_int(trade.get("exit_ts"), entry_ts) or entry_ts
    if token and "token_ca" in cols:
        clauses.append("token_ca=?")
        clause_params.append(token)
    if signal_id is not None and "signal_id" in cols:
        clauses.append("CAST(signal_id AS TEXT)=?")
        clause_params.append(str(signal_id))
    window_start = entry_ts - 900
    window_end = exit_ts + 300
    params = [window_start, window_end] + clause_params
    if clauses:
        where = f"event_ts BETWEEN ? AND ? AND ({' OR '.join(clauses)})"
    else:
        where = "event_ts BETWEEN ? AND ?"
    rows = db.execute(
        f"""
        SELECT {', '.join(selected)}
        FROM paper_decision_events
        WHERE {where}
        ORDER BY event_ts ASC
        LIMIT 200
        """,
        tuple(params),
    ).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        payload = jloads(item.get("payload_json"), {})
        item["payload_keys"] = sorted(payload.keys())[:40] if isinstance(payload, dict) else []
        item.pop("payload_json", None)
        out.append(item)
    return out


def load_runtime_state(db):
    if not table_exists(db, "a_class_mode_runtime_state"):
        return {"available": False}
    row = db.execute(
        "SELECT * FROM a_class_mode_runtime_state WHERE mode_key='A_CLASS_FASTLANE'",
    ).fetchone()
    if not row:
        return {"available": False, "reason": "a_class_fastlane_row_missing"}
    item = dict(row)
    item["detail"] = jloads(item.get("detail_json"), {})
    item.pop("detail_json", None)
    return {"available": True, "state": item}


def quote_collapse_timeline(trade, samples):
    entry_price = safe_float(trade.get("entry_price"))
    exit_price = safe_float(trade.get("exit_price"))
    points = []
    if entry_price:
        points.append({
            "ts": safe_int(trade.get("entry_ts")),
            "iso": iso(trade.get("entry_ts")),
            "stage": "entry",
            "price": entry_price,
            "pnl_pct_from_entry": 0.0,
        })
    for sample in samples:
        price = safe_float(sample.get("quote_price")) or safe_float(sample.get("mark_price"))
        if entry_price and price:
            points.append({
                "ts": safe_int(sample.get("sample_ts")),
                "iso": iso(sample.get("sample_ts")),
                "stage": sample.get("action") or "path_sample",
                "price": price,
                "pnl_pct_from_entry": round(price / entry_price - 1.0, 6),
                "quote_success": sample.get("quote_success"),
                "quote_failure_reason": sample.get("quote_failure_reason"),
            })
    if entry_price and exit_price:
        points.append({
            "ts": safe_int(trade.get("exit_ts")),
            "iso": iso(trade.get("exit_ts")),
            "stage": "exit",
            "price": exit_price,
            "pnl_pct_from_entry": round(exit_price / entry_price - 1.0, 6),
            "exit_reason": trade.get("exit_reason"),
        })
    return points


def build_recap(db_path, trade_id, now_ts=None):
    now_ts = float(now_ts or time.time())
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    try:
        trade = select_trade(db, trade_id)
        if not trade:
            return {
                "schema_version": SCHEMA_VERSION,
                "available": False,
                "reason": "trade_not_found",
                "trade_id": str(trade_id),
                "promotion_allowed": False,
            }
        samples = load_path_samples(db, trade_id)
        runtime = load_runtime_state(db)
        symbol = str(trade.get("symbol") or "").upper()
        token = str(trade.get("token_ca") or "")
        spoof_risk = bool(symbol in STABLECOIN_SYMBOLS and token and token.lower() not in {
            "epjfwdd5aufqssqe4xqdibkmojfoe7zl5yyjvytdt1v",
        })
        entry_price = safe_float(trade.get("entry_price"))
        exit_price = safe_float(trade.get("exit_price"))
        quote_collapse_pct = None if not entry_price or not exit_price else round(exit_price / entry_price - 1.0, 6)
        cooldown_until = None
        clean_required = None
        if runtime.get("available"):
            state = runtime.get("state") or {}
            cooldown_until = safe_float(state.get("cooldown_until_ts"))
            clean_required = safe_int(state.get("clean_windows_required"))
        return {
            "schema_version": SCHEMA_VERSION,
            "available": True,
            "generated_at_ts": now_ts,
            "generated_at": iso(now_ts),
            "trade_id": str(trade_id),
            "paper_only": bool(safe_int(trade.get("paper_only"), 1)),
            "breach_class_expected": "PAPER_MARKET",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "runtime_change_performed": False,
            "trade": {
                "token_ca": trade.get("token_ca"),
                "symbol": trade.get("symbol"),
                "premium_signal_id": trade.get("premium_signal_id"),
                "signal_ts": trade.get("signal_ts"),
                "entry_ts": trade.get("entry_ts"),
                "entry_iso": iso(trade.get("entry_ts")),
                "exit_ts": trade.get("exit_ts"),
                "exit_iso": iso(trade.get("exit_ts")),
                "seconds_held": None if not trade.get("entry_ts") or not trade.get("exit_ts") else safe_int(trade.get("exit_ts")) - safe_int(trade.get("entry_ts")),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl_pct": safe_float(trade.get("pnl_pct")),
                "exit_reason": trade.get("exit_reason"),
                "entry_mode": trade.get("entry_mode"),
                "signal_type": trade.get("signal_type"),
                "signal_route": trade.get("signal_route"),
                "position_size_sol": safe_float(trade.get("position_size_sol")),
            },
            "spoofed_symbol_review": {
                "symbol": trade.get("symbol"),
                "stablecoin_like_symbol": bool(symbol in STABLECOIN_SYMBOLS),
                "known_real_usdc_mint": "EPjFWdd5AufqSSqeM2qQDibkMqFoE7zL5yYyJvytdT1v",
                "token_is_known_real_usdc_mint": token.lower() == "epjfwdd5aufqssqe4xqdibkmojfoe7zl5yyjvytdt1v",
                "spoofed_symbol_risk": spoof_risk,
                "review_note": "Symbol resembles a stablecoin but token_ca is not the canonical USDC mint." if spoof_risk else None,
            },
            "quote_collapse": {
                "entry_to_exit_price_return_pct": quote_collapse_pct,
                "stop_target_pct": -0.20,
                "stop_overshoot_pp": None if quote_collapse_pct is None else round(abs(quote_collapse_pct) - 0.20, 6),
                "timeline": quote_collapse_timeline(trade, samples),
            },
            "decision_events": load_decision_events(db, trade),
            "runtime_state": runtime,
            "paper_recovery_eta": {
                "cooldown_until_ts": cooldown_until,
                "cooldown_until_iso": iso(cooldown_until),
                "clean_windows_required": clean_required,
                "eta_rule": "paper auto-resume is eligible after cooldown elapsed and required clean windows pass; LIVE remains human-operated",
            },
            "required_followups": [
                "Persist breach_class=PAPER_MARKET in a_class_mode_runtime_state.detail_json.",
                "Keep paper auto-resume gated by clean-window SLA.",
                "Keep LIVE re-enable routed through the human operator script.",
                "Add P7 hard-stop fill stress at -25%/-30%.",
            ],
        }
    finally:
        db.close()


def self_test():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "paper.db"
        db = sqlite3.connect(path)
        db.execute(
            """
            CREATE TABLE paper_trades(
              id INTEGER PRIMARY KEY, token_ca TEXT, symbol TEXT, premium_signal_id INTEGER,
              signal_ts INTEGER, entry_ts INTEGER, exit_ts INTEGER, entry_price REAL,
              exit_price REAL, pnl_pct REAL, exit_reason TEXT, paper_only INTEGER,
              entry_mode TEXT, signal_type TEXT, signal_route TEXT, position_size_sol REAL
            )
            """
        )
        db.execute(
            "INSERT INTO paper_trades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (71, "FakeMint", "USDC", 49535, 1000, 1010, 1110, 1.0, 0.7025, -0.2975, "probe_quote_guard_stop", 1, "A_CLASS_FASTLANE", "ATH", "ATH", 0.001),
        )
        db.execute(
            """
            CREATE TABLE paper_trade_path_samples(
              trade_id INTEGER, sample_ts INTEGER, action TEXT, reason TEXT,
              mark_price REAL, mark_pnl REAL, quote_price REAL, quote_pnl REAL,
              peak_pnl REAL, sold_pct REAL, mark_source TEXT, quote_success INTEGER,
              quote_failure_reason TEXT, quote_out_sol REAL, payload_json TEXT
            )
            """
        )
        db.execute("INSERT INTO paper_trade_path_samples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (71, 1060, "monitor", "sample", 0.9, -0.1, 0.9, -0.1, 0, 0, "self", 1, None, 0.0009, "{}"))
        db.commit()
        db.close()
        report = build_recap(str(path), 71, now_ts=1200)
        assert report["available"] is True
        assert report["paper_only"] is True
        assert report["breach_class_expected"] == "PAPER_MARKET"
        assert report["spoofed_symbol_review"]["spoofed_symbol_risk"] is True
        assert report["quote_collapse"]["stop_overshoot_pp"] > 0
    print("SELF_TEST_PASS a_class_paper_breach_recap")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--trade-id")
    parser.add_argument("--out", default="/app/data/agent_runs/latest/a_class_paper_breach_recap.json")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    if not args.trade_id:
        raise SystemExit("--trade-id is required unless --self-test is set")
    report = build_recap(args.db, args.trade_id)
    write_json(args.out, report)
    print(json.dumps({
        "out": args.out,
        "available": report.get("available"),
        "trade_id": report.get("trade_id"),
        "breach_class_expected": report.get("breach_class_expected"),
        "paper_only": report.get("paper_only"),
        "promotion_allowed": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
