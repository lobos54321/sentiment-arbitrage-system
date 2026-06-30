#!/usr/bin/env python3
"""Read-only A_CLASS_FASTLANE mode/final-entry readiness audit.

This report explains whether A_CLASS remaining in SHADOW is expected or stuck.
It reads runtime safety state, final_entry_contract decision events, and the
current context/coverage reports. It never resets SHADOW, enables A_CLASS,
changes final_entry_contract, enables paper/live execution, or changes risk.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import tempfile
import time
from collections import Counter
from pathlib import Path


SCHEMA_VERSION = "a_class_fastlane_mode_readiness_audit.v2"
MODE_KEY = "A_CLASS_FASTLANE"


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def safe_int(value, default=0):
    parsed = safe_float(value)
    return default if parsed is None else int(parsed)


def truthy(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "live", "available"}


def rate(num, den):
    return None if not den else round(float(num) / float(den), 6)


def table_exists(db, table):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def jloads(raw, default=None):
    default = {} if default is None else default
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, (dict, list)) else default
    except Exception:
        return default


def load_json(path):
    if not path:
        return {}
    target = Path(path)
    if not target.exists():
        return {}
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def row_value(row, key, default=None):
    try:
        return row[key]
    except Exception:
        return default


def effective_runtime_state(row, now_ts):
    if row is None:
        return {
            "mode_key": MODE_KEY,
            "available": False,
            "status": "LIVE",
            "action": "LIVE",
            "circuit_broken": False,
            "stored_status": "LIVE",
            "stored_circuit_broken": False,
            "reason": None,
            "cooldown_until_ts": None,
            "cooldown_remaining_sec": 0,
            "recovery_required": False,
            "clean_windows_required": 0,
            "detail": {},
            "evaluated_at": now_ts,
        }
    cooldown_until = safe_float(row_value(row, "cooldown_until_ts"))
    stored_status = str(row_value(row, "status", "LIVE") or "LIVE").upper()
    stored_action = str(row_value(row, "action", stored_status) or stored_status).upper()
    stored_circuit = truthy(row_value(row, "circuit_broken", False))
    in_cooldown = cooldown_until is not None and cooldown_until > now_ts
    status = stored_status
    action = stored_action
    circuit_broken = stored_circuit
    recovery_required = False
    reason = row_value(row, "reason")
    if stored_circuit and in_cooldown:
        status = "CIRCUIT_BROKEN"
        action = "SHADOW"
        circuit_broken = True
    elif stored_circuit:
        status = "SHADOW"
        action = "SHADOW"
        circuit_broken = False
        recovery_required = True
        reason = "cooldown_elapsed_requires_clean_windows"
    return {
        "mode_key": str(row_value(row, "mode_key", MODE_KEY) or MODE_KEY),
        "available": True,
        "status": status,
        "action": action,
        "circuit_broken": bool(circuit_broken),
        "stored_status": stored_status,
        "stored_circuit_broken": bool(stored_circuit),
        "reason": reason,
        "source_trade_id": row_value(row, "source_trade_id"),
        "token_ca": row_value(row, "token_ca"),
        "symbol": row_value(row, "symbol"),
        "last_realized_pnl_pct": safe_float(row_value(row, "last_realized_pnl_pct")),
        "last_realized_pnl_sol": safe_float(row_value(row, "last_realized_pnl_sol")),
        "loss_cap_pct": safe_float(row_value(row, "loss_cap_pct")),
        "breach_count": safe_int(row_value(row, "breach_count"), 0),
        "last_breach_ts": safe_float(row_value(row, "last_breach_ts")),
        "cooldown_until_ts": cooldown_until,
        "cooldown_remaining_sec": max(0.0, (cooldown_until or 0.0) - now_ts) if cooldown_until is not None else 0,
        "recovery_required": recovery_required,
        "clean_windows_required": safe_int(row_value(row, "clean_windows_required"), 4),
        "detail": jloads(row_value(row, "detail_json"), {}),
        "evaluated_at": now_ts,
    }


def load_runtime_state(db, now_ts):
    if not table_exists(db, "a_class_mode_runtime_state"):
        state = effective_runtime_state(None, now_ts)
        return {
            "available": False,
            "reason": "a_class_mode_runtime_state_table_missing",
            "mode_state": state,
        }
    row = db.execute(
        "SELECT * FROM a_class_mode_runtime_state WHERE mode_key = ?",
        (MODE_KEY,),
    ).fetchone()
    state = effective_runtime_state(row, now_ts)
    return {
        "available": row is not None,
        "reason": None if row is not None else "a_class_fastlane_row_missing",
        "mode_state": state,
    }


def extract_hard_blockers(payload):
    blockers = payload.get("hard_blockers")
    if blockers is None and isinstance(payload.get("final_entry_contract"), dict):
        blockers = payload.get("final_entry_contract", {}).get("hard_blockers")
    if isinstance(blockers, str):
        decoded = jloads(blockers, None)
        blockers = decoded if isinstance(decoded, list) else [blockers]
    if isinstance(blockers, (list, tuple, set)):
        return [str(item) for item in blockers if str(item or "")]
    return []


def mode_state_from_payload(payload):
    state = payload.get("mode_state")
    if state is None and isinstance(payload.get("final_entry_contract"), dict):
        state = payload.get("final_entry_contract", {}).get("mode_state")
    return state if isinstance(state, dict) else {}


def counter_value(value):
    if value is None or value == "":
        return "UNKNOWN"
    return str(value)


def load_final_entry_contract_events(db, since_ts, until_ts):
    if not table_exists(db, "paper_decision_events"):
        return {
            "available": False,
            "reason": "paper_decision_events_missing",
            "rows": 0,
            "hard_blockers": {},
            "mode_status": {},
            "mode_action": {},
            "mode_reason": {},
            "event_type_decision_reason_counts": [],
            "sample_blocks": [],
        }
    rows = db.execute(
        """
        SELECT event_ts, signal_id, token_ca, symbol, lifecycle_id, event_type,
               decision, reason, payload_json
        FROM paper_decision_events
        WHERE event_ts >= ? AND event_ts <= ?
          AND component = 'final_entry_contract'
        ORDER BY event_ts DESC
        """,
        (since_ts, until_ts),
    ).fetchall()
    hard_blockers = Counter()
    mode_status = Counter()
    mode_action = Counter()
    mode_reason = Counter()
    normalized_modes = Counter()
    group_counts = Counter()
    expected_rr = Counter()
    spread = Counter()
    samples = []
    for row in rows:
        payload = jloads(row_value(row, "payload_json"))
        group_counts[(row_value(row, "event_type"), row_value(row, "decision"), row_value(row, "reason"))] += 1
        for blocker in extract_hard_blockers(payload):
            hard_blockers[blocker] += 1
        state = mode_state_from_payload(payload)
        mode_status[counter_value(state.get("status"))] += 1
        mode_action[counter_value(state.get("action"))] += 1
        mode_reason[counter_value(state.get("reason"))] += 1
        normalized_modes[counter_value(payload.get("normalized_mode"))] += 1
        rr = safe_float(payload.get("expected_rr"))
        expected_rr["missing" if rr is None else ("lt_2" if rr < 2 else "gte_2")] += 1
        sp = safe_float(payload.get("spread_pct"))
        spread["missing" if sp is None else ("gt_20pct" if sp > 20 else "le_20pct")] += 1
        if len(samples) < 20:
            samples.append(
                {
                    "event_ts": row_value(row, "event_ts"),
                    "signal_id": row_value(row, "signal_id"),
                    "token_ca": row_value(row, "token_ca"),
                    "symbol": row_value(row, "symbol"),
                    "lifecycle_id": row_value(row, "lifecycle_id"),
                    "event_type": row_value(row, "event_type"),
                    "decision": row_value(row, "decision"),
                    "reason": row_value(row, "reason"),
                    "hard_blockers": extract_hard_blockers(payload),
                    "normalized_mode": payload.get("normalized_mode"),
                    "expected_rr": payload.get("expected_rr"),
                    "spread_pct": payload.get("spread_pct"),
                    "mode_state": {
                        "status": state.get("status"),
                        "action": state.get("action"),
                        "reason": state.get("reason"),
                        "circuit_broken": state.get("circuit_broken"),
                    },
                }
            )
    return {
        "available": True,
        "rows": len(rows),
        "hard_blockers": dict(hard_blockers.most_common()),
        "mode_status": dict(mode_status.most_common()),
        "mode_action": dict(mode_action.most_common()),
        "mode_reason": dict(mode_reason.most_common()),
        "normalized_modes": dict(normalized_modes.most_common()),
        "expected_rr_bucket_counts": dict(expected_rr.most_common()),
        "spread_bucket_counts": dict(spread.most_common()),
        "event_type_decision_reason_counts": [
            {"event_type": key[0], "decision": key[1], "reason": key[2], "count": count}
            for key, count in group_counts.most_common(30)
        ],
        "sample_blocks": samples,
    }


def context_failed_conditions(context, volume_kline):
    failed = []
    blockers = set((context.get("blockers") or []))
    for blocker in sorted(blockers):
        if blocker in {
            "source_quote_clean_coverage_below_80pct",
            "source_quote_executable_coverage_below_80pct",
            "volume_profile_coverage_below_80pct",
            "kline_coverage_below_80pct",
            "schema_mixed_quote_sensitive_slices_blocked",
            "context_schema_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
            "quote_clean_definition_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
        }:
            failed.append({"condition": blocker, "source": "context_coverage_audit"})
    volume = volume_kline.get("volume_context") or {}
    if volume.get("blocker") and not any(row["condition"] == volume.get("blocker") for row in failed):
        failed.append({"condition": volume.get("blocker"), "source": "volume_kline_coverage_audit"})
    kline = volume_kline.get("raw_gold_silver_kline") or {}
    if kline.get("blocker") and not any(row["condition"] == kline.get("blocker") for row in failed):
        failed.append({"condition": kline.get("blocker"), "source": "volume_kline_coverage_audit"})
    return failed


def raw_funnel_snapshot(raw_funnel):
    summary = raw_funnel.get("summary") or {}
    raw = summary.get("raw_denominator") or {}
    decision = summary.get("decision_layer") or {}
    bridge = summary.get("entry_bridge_layer") or {}
    raw_bridge = bridge.get("raw_signal_decision_bridge") or {}
    return {
        "raw_gold_silver_events": raw.get("raw_all_gold_silver_event_rows"),
        "evaluable_gold_silver_events": raw.get("evaluable_gold_silver_event_rows"),
        "raw_gold_silver_entered_events": raw.get("entered_events"),
        "decision_record_rate": decision.get("decision_record_rate"),
        "would_enter_rate": decision.get("would_enter_rate"),
        "entered_rate": decision.get("entered_rate"),
        "events_with_decision_record": decision.get("events_with_decision_record"),
        "would_enter_events": decision.get("would_enter_events"),
        "entered_events": decision.get("entered_events"),
        "paper_trades_entry_ts_window_count": bridge.get("paper_trades_entry_ts_window_count"),
        "raw_signals_with_pass_or_allow": raw_bridge.get("raw_signals_with_pass_or_allow"),
        "raw_signals_with_pending_entry": raw_bridge.get("raw_signals_with_pending_entry"),
        "raw_signals_with_final_entry_contract": raw_bridge.get("raw_signals_with_final_entry_contract"),
        "raw_signals_with_final_entry_block": raw_bridge.get("raw_signals_with_final_entry_block"),
    }


def classify(runtime, final_contract, failed_conditions):
    state = runtime.get("mode_state") or {}
    hard_blockers = final_contract.get("hard_blockers") or {}
    mode_disabled_count = int(hard_blockers.get("mode_disabled") or 0)
    final_rows = int(final_contract.get("rows") or 0)
    cooldown_remaining = safe_float(state.get("cooldown_remaining_sec"), 0) or 0
    status = str(state.get("status") or state.get("action") or "LIVE").upper()
    recovery_required = truthy(state.get("recovery_required"))
    clean_windows_passed = not failed_conditions
    human = False
    if status == "CIRCUIT_BROKEN" or cooldown_remaining > 0:
        verdict = "FUNNEL_BLOCKED_EXPECTED"
        reason = "a_class_runtime_cooldown_active"
        current_capture_stage = "final_entry_cooldown"
    elif (mode_disabled_count or status == "SHADOW" or recovery_required) and not clean_windows_passed:
        verdict = "FUNNEL_BLOCKED_EXPECTED"
        reason = "cooldown_elapsed_requires_clean_windows"
        current_capture_stage = "mode_disabled_clean_window_pending"
    elif (mode_disabled_count or status == "SHADOW" or recovery_required) and clean_windows_passed:
        verdict = "FUNNEL_BLOCKED_STUCK"
        reason = "mode_disabled_after_clean_windows_passed"
        human = True
        current_capture_stage = "mode_disabled_stuck_requires_human_review"
    elif final_rows and not mode_disabled_count:
        verdict = "FUNNEL_READY_FOR_PAPER_PROPOSAL"
        reason = "final_entry_contract_no_mode_disabled_blocker"
        human = True
        current_capture_stage = "paper_proposal_ready_requires_human_approval"
    else:
        verdict = "READINESS_AUDIT_ONLY"
        reason = "no_final_entry_contract_mode_evidence_in_window"
        current_capture_stage = "insufficient_final_entry_contract_evidence"
    return {
        "final_entry_status": verdict,
        "reason": reason,
        "human_action_required": human,
        "clean_windows_passed": clean_windows_passed,
        "current_capture_stage": current_capture_stage,
    }


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    raw_funnel = load_json(args.raw_funnel)
    context = load_json(args.context_coverage)
    volume_kline = load_json(args.volume_kline_audit)
    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    try:
        runtime = load_runtime_state(db, now_ts)
        final_contract = load_final_entry_contract_events(db, since_ts, now_ts)
    finally:
        db.close()
    failed = context_failed_conditions(context, volume_kline)
    classification = classify(runtime, final_contract, failed)
    raw_snapshot = raw_funnel_snapshot(raw_funnel)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "a_class_fastlane_mode_audit_24h",
        "generated_at": utc_now(),
        "window": {"hours": args.hours, "since_ts": since_ts, "until_ts": now_ts},
        "inputs": {
            "paper_db": args.db,
            "raw_funnel": args.raw_funnel,
            "context_coverage": args.context_coverage,
            "volume_kline_audit": args.volume_kline_audit,
        },
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "canary_increase_allowed": False,
        "paper_enablement_allowed": False,
        "runtime_safety": runtime,
        "A_CLASS_mode_status": final_contract.get("mode_status") or {},
        "effective_runtime_mode_state": runtime.get("mode_state") or {},
        "final_entry_status": classification["final_entry_status"],
        "reason": classification["reason"],
        "human_action_required": classification["human_action_required"],
        "current_capture_stage": classification["current_capture_stage"],
        "clean_window_conditions": {
            "passed": classification["clean_windows_passed"],
            "failed_conditions": failed,
            "context_coverage_loaded": bool(context),
            "volume_kline_audit_loaded": bool(volume_kline),
        },
        "raw_funnel_snapshot": raw_snapshot,
        "raw_gold_silver_entered_events": raw_snapshot.get("raw_gold_silver_entered_events"),
        "decision_layer": (raw_funnel.get("summary") or {}).get("decision_layer") or {},
        "entry_bridge_layer_summary": {
            key: raw_snapshot.get(key)
            for key in (
                "paper_trades_entry_ts_window_count",
                "raw_signals_with_pass_or_allow",
                "raw_signals_with_pending_entry",
                "raw_signals_with_final_entry_contract",
                "raw_signals_with_final_entry_block",
            )
        },
        "final_entry_contract_blocker_breakdown": final_contract.get("hard_blockers") or {},
        "final_entry_contract": final_contract,
        "stop_conditions": {
            "requires_human_approval_before_mode_change": True,
            "must_not_reset_shadow_automatically": True,
            "must_not_enable_paper_or_live_automatically": True,
            "must_not_change_final_entry_contract_automatically": True,
        },
    }


def compact_summary(report):
    return {
        "final_entry_status": report.get("final_entry_status"),
        "reason": report.get("reason"),
        "current_capture_stage": report.get("current_capture_stage"),
        "human_action_required": report.get("human_action_required"),
        "promotion_allowed": False,
        "clean_windows_passed": (report.get("clean_window_conditions") or {}).get("passed"),
        "failed_clean_window_conditions": (report.get("clean_window_conditions") or {}).get("failed_conditions"),
        "effective_runtime_mode_state": report.get("effective_runtime_mode_state"),
        "final_entry_contract_blocker_breakdown": report.get("final_entry_contract_blocker_breakdown"),
        "raw_funnel_snapshot": report.get("raw_funnel_snapshot"),
    }


def self_test():
    now = 2_000_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "paper.db"
        raw_path = root / "raw_funnel.json"
        context_path = root / "context.json"
        volume_path = root / "volume.json"
        db = sqlite3.connect(db_path)
        db.execute(
            """
            CREATE TABLE a_class_mode_runtime_state(
              mode_key TEXT PRIMARY KEY, status TEXT, action TEXT, circuit_broken INTEGER,
              reason TEXT, source_trade_id TEXT, token_ca TEXT, symbol TEXT,
              last_realized_pnl_pct REAL, last_realized_pnl_sol REAL, loss_cap_pct REAL,
              breach_count INTEGER, last_breach_ts REAL, cooldown_until_ts REAL,
              clean_windows_required INTEGER, detail_json TEXT, created_at REAL, updated_at REAL
            )
            """
        )
        db.execute(
            "INSERT INTO a_class_mode_runtime_state VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (MODE_KEY, "SHADOW", "SHADOW", 1, "loss_cap_breach", "t1", "TOK", "TOK", -21, -0.001, 20, 1, now - 90000, now - 1, 4, "{}", now - 90000, now - 1),
        )
        db.execute(
            """
            CREATE TABLE paper_decision_events(
              event_ts INTEGER, signal_id TEXT, token_ca TEXT, symbol TEXT, lifecycle_id TEXT,
              component TEXT, event_type TEXT, decision TEXT, reason TEXT, payload_json TEXT
            )
            """
        )
        payload = {
            "hard_blockers": ["mode_disabled", "expected_rr_below_2"],
            "mode_state": {"status": "SHADOW", "action": "SHADOW", "reason": "cooldown_elapsed_requires_clean_windows"},
            "normalized_mode": MODE_KEY,
            "expected_rr": 1.5,
            "spread_pct": 4,
        }
        db.execute(
            "INSERT INTO paper_decision_events VALUES (?,?,?,?,?,?,?,?,?,?)",
            (now - 60, "1", "TOK", "TOK", "lc", "final_entry_contract", "entry_block", "BLOCK", "final_entry_hard_block", json.dumps(payload)),
        )
        db.commit()
        db.close()
        write_json(raw_path, {
            "summary": {
                "raw_denominator": {"raw_all_gold_silver_event_rows": 2, "entered_events": 0},
                "decision_layer": {"would_enter_events": 1, "would_enter_rate": 0.5, "entered_events": 0, "entered_rate": 0.0},
                "entry_bridge_layer": {
                    "paper_trades_entry_ts_window_count": 0,
                    "raw_signal_decision_bridge": {
                        "raw_signals_with_pass_or_allow": 1,
                        "raw_signals_with_pending_entry": 1,
                        "raw_signals_with_final_entry_contract": 1,
                        "raw_signals_with_final_entry_block": 1,
                    },
                },
            }
        })
        write_json(context_path, {"blockers": ["source_quote_clean_coverage_below_80pct"]})
        write_json(volume_path, {"overall": {}, "volume_context": {}, "raw_gold_silver_kline": {}})
        args = argparse.Namespace(
            db=str(db_path),
            raw_funnel=str(raw_path),
            context_coverage=str(context_path),
            volume_kline_audit=str(volume_path),
            hours=24,
            now_ts=now,
            out=None,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["final_entry_status"] == "FUNNEL_BLOCKED_EXPECTED"
        assert report["reason"] == "cooldown_elapsed_requires_clean_windows"
        assert report["clean_window_conditions"]["passed"] is False
        assert report["human_action_required"] is False
        write_json(context_path, {"blockers": []})
        report = build_report(args)
        assert report["final_entry_status"] == "FUNNEL_BLOCKED_STUCK"
        assert report["human_action_required"] is True
        assert report["clean_window_conditions"]["passed"] is True
    print("SELF_TEST_PASS a_class_fastlane_mode_readiness_audit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-funnel", default=None)
    parser.add_argument("--context-coverage", default=None)
    parser.add_argument("--volume-kline-audit", default=None)
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--out")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    if args.out:
        write_json(args.out, report)
    print(json.dumps(compact_summary(report), sort_keys=True))


if __name__ == "__main__":
    main()
