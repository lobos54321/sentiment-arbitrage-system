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


SCHEMA_VERSION = "a_class_fastlane_mode_readiness_audit.v3"
MODE_KEY = "A_CLASS_FASTLANE"
CLEAN_WINDOW_COUNTER_SCHEMA_VERSION = "a_class_clean_window_counter.v2"
CLEAN_WINDOW_COUNTER_BUCKET_SEC = 3600
RECOVERY_SLA_SCHEMA_VERSION = "a_class_circuit_recovery_sla.v1"
DATA_INFRA_CLEAN_WINDOWS_REQUIRED = 6
MARKET_CLEAN_WINDOWS_REQUIRED = 24
CIRCUIT_COOLDOWN_SEC = 24 * 60 * 60
HUMAN_ESCALATION_SEC = 48 * 60 * 60
DAILY_REMINDER_SEC = 24 * 60 * 60


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
        "created_at": safe_float(row_value(row, "created_at")),
        "updated_at": safe_float(row_value(row, "updated_at")),
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


def clean_condition_code(row):
    if isinstance(row, dict):
        return str(row.get("condition") or row.get("blocker") or row.get("reason") or "unknown")
    return str(row or "unknown")


def window_bucket(now_ts, bucket_sec=CLEAN_WINDOW_COUNTER_BUCKET_SEC):
    bucket_sec = max(1, int(bucket_sec or CLEAN_WINDOW_COUNTER_BUCKET_SEC))
    bucket = int(float(now_ts) // bucket_sec)
    return {
        "bucket": bucket,
        "bucket_sec": bucket_sec,
        "window_key": f"{bucket_sec}s:{bucket}",
        "previous_bucket": bucket - 1,
    }


def clean_window_counter_from_detail(detail):
    if not isinstance(detail, dict):
        return {}
    value = detail.get("clean_window_counter")
    return value if isinstance(value, dict) else {}


def normalize_breach_class(value):
    text = str(value or "").strip().upper()
    if text in {"DATA_INFRA", "INFRA", "PROVIDER", "QUOTE_INFRA", "NO_ROUTE_INFRA"}:
        return "DATA_INFRA"
    if text in {"MARKET", "REAL_LOSS", "EXECUTION_MARKET"}:
        return "MARKET"
    return None


def compact_detail_text(*values):
    return " ".join(str(value or "").strip().lower() for value in values if str(value or "").strip())


def infer_breach_class(state):
    """Classify the last circuit trip into the P2.1 recovery SLA classes.

    DATA_INFRA means the breach evidence points at quote corruption, no-route/trap,
    provider outage, or other execution-data failure. MARKET is the conservative
    default for a normal realized loss with no infra evidence.
    """
    state = state if isinstance(state, dict) else {}
    detail = state.get("detail") if isinstance(state.get("detail"), dict) else {}
    persisted = normalize_breach_class(
        detail.get("breach_class")
        or detail.get("circuit_breaker_breach_class")
        or detail.get("recovery_breach_class")
        or detail.get("sla_breach_class")
    )
    if persisted:
        return {
            "breach_class": persisted,
            "source": "persisted_runtime_detail",
            "evidence": ["persisted_breach_class"],
        }

    evidence_text = compact_detail_text(
        detail.get("exit_reason"),
        detail.get("reason"),
        detail.get("quote_failure_reason"),
        detail.get("route_failure_reason"),
        detail.get("provider_reason"),
        detail.get("evidence_status"),
        detail.get("data_confidence"),
        state.get("reason"),
        state.get("symbol"),
        state.get("token_ca"),
    )
    infra_evidence = []
    for key in (
        "no_route_flag",
        "trapped_flag",
        "quote_corruption_flag",
        "provider_outage_flag",
        "provider_timeout_flag",
    ):
        if truthy(detail.get(key)):
            infra_evidence.append(key)
    if any(
        marker in evidence_text
        for marker in (
            "quote corruption",
            "quote_corruption",
            "no_route",
            "no route",
            "route unavailable",
            "route_unavailable",
            "route failed",
            "route_failure",
            "trapped",
            "provider outage",
            "provider_outage",
            "provider failed",
            "provider_failed",
            "rate limited",
            "rate_limited",
            "429",
            "timeout",
            "quote failed",
            "quote_failed",
            "quote unavailable",
            "quote_unavailable",
        )
    ):
        infra_evidence.append("infra_text_match")
    if infra_evidence:
        return {
            "breach_class": "DATA_INFRA",
            "source": "runtime_detail_inference",
            "evidence": sorted(set(infra_evidence)),
        }
    return {
        "breach_class": "MARKET",
        "source": "default_realized_loss_without_data_infra_evidence",
        "evidence": ["loss_cap_breach_without_infra_markers"],
    }


def clean_windows_required_for_breach_class(breach_class):
    return DATA_INFRA_CLEAN_WINDOWS_REQUIRED if breach_class == "DATA_INFRA" else MARKET_CLEAN_WINDOWS_REQUIRED


def build_motion_trace_review_artifact(state, breach_class, now_ts):
    state = state if isinstance(state, dict) else {}
    detail = state.get("detail") if isinstance(state.get("detail"), dict) else {}
    required = breach_class == "MARKET"
    trade_id = state.get("source_trade_id") or detail.get("trade_id")
    token = state.get("token_ca") or detail.get("token_ca")
    symbol = state.get("symbol") or detail.get("symbol")
    pnl_pct = state.get("last_realized_pnl_pct")
    loss_cap_pct = state.get("loss_cap_pct")
    available = bool(not required or trade_id)
    summary = (
        f"A_CLASS MARKET circuit review for trade {trade_id or 'UNKNOWN'} "
        f"({symbol or token or 'UNKNOWN'}): realized_pnl_pct={pnl_pct}, "
        f"loss_cap_pct={loss_cap_pct}. Review confirms the recovery SLA is "
        "24 clean hourly buckets before paper auto-resume; LIVE still requires "
        "human operator action."
    )
    return {
        "schema_version": "a_class_motion_trace_review.v1",
        "required": required,
        "available": available,
        "auto_generated": True,
        "artifact_scope": "readiness_audit_inline",
        "artifact_id": f"a_class_motion_trace:{trade_id or token or 'unknown'}",
        "generated_at_ts": float(now_ts),
        "source_trade_id": trade_id,
        "token_ca": token,
        "symbol": symbol,
        "last_realized_pnl_pct": pnl_pct,
        "loss_cap_pct": loss_cap_pct,
        "human_readable_summary": summary,
        "review_questions": [
            "Was the loss caused by normal market movement rather than quote/provider corruption?",
            "Was the -20% loss cap enforced and recorded?",
            "Are 24 clean hourly buckets present before paper auto-resume?",
            "Is LIVE re-enable still routed to the human operator script?",
        ] if required else [],
        "satisfies_market_recovery_requirement": bool(required and available),
    }


def build_circuit_recovery_sla(state, now_ts):
    state = state if isinstance(state, dict) else {}
    inferred = infer_breach_class(state)
    breach_class = inferred["breach_class"]
    required = clean_windows_required_for_breach_class(breach_class)
    motion_trace = build_motion_trace_review_artifact(state, breach_class, now_ts)
    cooldown_remaining = safe_float(state.get("cooldown_remaining_sec"), 0) or 0
    return {
        "schema_version": RECOVERY_SLA_SCHEMA_VERSION,
        "mode_key": MODE_KEY,
        "breach_class": breach_class,
        "breach_class_source": inferred.get("source"),
        "breach_class_evidence": inferred.get("evidence") or [],
        "class_definitions": {
            "DATA_INFRA": {
                "examples": ["quote_corruption", "no_route_trap", "provider_outage"],
                "clean_windows_required": DATA_INFRA_CLEAN_WINDOWS_REQUIRED,
                "motion_trace_review_required": False,
            },
            "MARKET": {
                "examples": ["real_loss_with_normal_data_quality"],
                "clean_windows_required": MARKET_CLEAN_WINDOWS_REQUIRED,
                "motion_trace_review_required": True,
            },
        },
        "cooldown_required_sec": CIRCUIT_COOLDOWN_SEC,
        "cooldown_remaining_sec": cooldown_remaining,
        "cooldown_elapsed": cooldown_remaining <= 0,
        "legacy_clean_windows_required": safe_int(state.get("clean_windows_required"), 4),
        "effective_clean_windows_required": required,
        "clean_window_bucket_sec": CLEAN_WINDOW_COUNTER_BUCKET_SEC,
        "motion_trace_review": motion_trace,
        "live_reenable_contract": {
            "live_auto_reenable_allowed": False,
            "human_operator_required": True,
            "operator_script": "scripts/a_class_mode_reenable_operator.py",
        },
        "paper_recovery_contract": {
            "paper_auto_resume_after_sla_allowed": True,
            "real_capital_risk": False,
            "live_canary_reenable_allowed": False,
        },
        "promotion_allowed": False,
    }


def apply_recovery_sla_to_runtime(runtime, now_ts):
    runtime = dict(runtime or {})
    state = dict(runtime.get("mode_state") or {})
    policy = build_circuit_recovery_sla(state, now_ts)
    state["legacy_clean_windows_required"] = state.get("clean_windows_required")
    state["clean_windows_required"] = policy["effective_clean_windows_required"]
    state["circuit_recovery_sla"] = policy
    runtime["mode_state"] = state
    return runtime, policy


def clean_window_counter_summary(state, clean_windows_passed, failed_conditions, now_ts):
    required = max(0, safe_int(state.get("clean_windows_required"), 4))
    detail = state.get("detail") if isinstance(state, dict) else {}
    previous = clean_window_counter_from_detail(detail)
    bucket = window_bucket(now_ts)
    current_key = bucket["window_key"]
    current_bucket = bucket["bucket"]
    previous_key = previous.get("last_window_key")
    previous_bucket = safe_int(previous.get("last_window_bucket"), None)
    previous_streak = safe_int(previous.get("streak"), 0)
    previous_passed = truthy(previous.get("last_passed"))
    failed_codes = [clean_condition_code(row) for row in failed_conditions or []]
    if clean_windows_passed:
        if previous_key == current_key:
            streak = max(1, previous_streak)
        elif previous_passed and previous_bucket == current_bucket - 1:
            streak = previous_streak + 1
        else:
            streak = 1
    else:
        streak = 0
    sufficient = bool(required <= 0 or streak >= required)
    return {
        "schema_version": CLEAN_WINDOW_COUNTER_SCHEMA_VERSION,
        "mode_key": MODE_KEY,
        "breach_class": ((state.get("circuit_recovery_sla") or {}).get("breach_class") if isinstance(state, dict) else None),
        "required_source": "circuit_recovery_sla",
        "counter_bucket_sec": bucket["bucket_sec"],
        "last_window_key": current_key,
        "last_window_bucket": current_bucket,
        "last_passed": bool(clean_windows_passed),
        "streak": int(streak),
        "required": int(required),
        "sufficient": sufficient,
        "failed_condition_codes": failed_codes,
        "updated_at": float(now_ts),
        "promotion_allowed": False,
        "automatic_runtime_change_allowed": False,
    }


def persist_clean_window_counter(db, state, clean_windows_passed, failed_conditions, now_ts):
    result = {
        "schema_version": CLEAN_WINDOW_COUNTER_SCHEMA_VERSION,
        "attempted": True,
        "available": False,
        "mode_key": MODE_KEY,
        "promotion_allowed": False,
        "changes_runtime_mode": False,
        "changes_circuit_breaker": False,
    }
    if not table_exists(db, "a_class_mode_runtime_state"):
        result["reason"] = "a_class_mode_runtime_state_table_missing"
        return result
    row = db.execute(
        "SELECT detail_json FROM a_class_mode_runtime_state WHERE mode_key = ?",
        (MODE_KEY,),
    ).fetchone()
    if row is None:
        result["reason"] = "a_class_fastlane_row_missing"
        return result
    detail = jloads(row_value(row, "detail_json"), {})
    if not isinstance(detail, dict):
        detail = {}
    counter = clean_window_counter_summary(state, clean_windows_passed, failed_conditions, now_ts)
    detail["clean_window_counter"] = counter
    db.execute(
        """
        UPDATE a_class_mode_runtime_state
        SET detail_json = ?, updated_at = ?
        WHERE mode_key = ?
        """,
        (json.dumps(detail, sort_keys=True), float(now_ts), MODE_KEY),
    )
    db.commit()
    result.update({
        "available": True,
        "reason": "persisted",
        "counter": counter,
    })
    return result


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


def signal_key(value):
    if value is None or value == "":
        return None
    return str(value)


def load_final_entry_contract_events(db, since_ts, until_ts):
    if not table_exists(db, "paper_decision_events"):
        return {
            "available": False,
            "reason": "paper_decision_events_missing",
            "rows": 0,
            "unique_signal_ids": 0,
            "hard_blockers": {},
            "non_mode_hard_blockers": {},
            "mode_disabled_rows": 0,
            "mode_disabled_unique_signal_ids": 0,
            "mode_disabled_only_rows": 0,
            "mode_disabled_only_unique_signal_ids": 0,
            "mode_disabled_plus_other_rows": 0,
            "mode_disabled_plus_other_unique_signal_ids": 0,
            "rows_without_hard_blockers": 0,
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
    non_mode_hard_blockers = Counter()
    mode_status = Counter()
    mode_action = Counter()
    mode_reason = Counter()
    normalized_modes = Counter()
    group_counts = Counter()
    expected_rr = Counter()
    spread = Counter()
    final_entry_signal_ids = set()
    mode_disabled_signal_ids = set()
    mode_disabled_only_signal_ids = set()
    mode_disabled_plus_other_signal_ids = set()
    mode_disabled_rows = 0
    mode_disabled_only_rows = 0
    mode_disabled_plus_other_rows = 0
    rows_without_hard_blockers = 0
    samples = []
    for row in rows:
        payload = jloads(row_value(row, "payload_json"))
        sig_key = signal_key(row_value(row, "signal_id"))
        if sig_key:
            final_entry_signal_ids.add(sig_key)
        group_counts[(row_value(row, "event_type"), row_value(row, "decision"), row_value(row, "reason"))] += 1
        blockers = extract_hard_blockers(payload)
        non_mode_blockers = [blocker for blocker in blockers if blocker != "mode_disabled"]
        if not blockers:
            rows_without_hard_blockers += 1
        for blocker in blockers:
            hard_blockers[blocker] += 1
        for blocker in non_mode_blockers:
            non_mode_hard_blockers[blocker] += 1
        if "mode_disabled" in blockers:
            mode_disabled_rows += 1
            if sig_key:
                mode_disabled_signal_ids.add(sig_key)
            if not non_mode_blockers:
                mode_disabled_only_rows += 1
                if sig_key:
                    mode_disabled_only_signal_ids.add(sig_key)
            else:
                mode_disabled_plus_other_rows += 1
                if sig_key:
                    mode_disabled_plus_other_signal_ids.add(sig_key)
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
                    "hard_blockers": blockers,
                    "non_mode_hard_blockers": non_mode_blockers,
                    "mode_disabled_only": "mode_disabled" in blockers and not non_mode_blockers,
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
        "unique_signal_ids": len(final_entry_signal_ids),
        "hard_blockers": dict(hard_blockers.most_common()),
        "non_mode_hard_blockers": dict(non_mode_hard_blockers.most_common()),
        "mode_disabled_rows": mode_disabled_rows,
        "mode_disabled_unique_signal_ids": len(mode_disabled_signal_ids),
        "mode_disabled_only_rows": mode_disabled_only_rows,
        "mode_disabled_only_unique_signal_ids": len(mode_disabled_only_signal_ids),
        "mode_disabled_plus_other_rows": mode_disabled_plus_other_rows,
        "mode_disabled_plus_other_unique_signal_ids": len(mode_disabled_plus_other_signal_ids),
        "rows_without_hard_blockers": rows_without_hard_blockers,
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


def add_failed_condition(failed, condition, source, **extra):
    if not condition:
        return
    if any(row.get("condition") == condition for row in failed):
        return
    row = {"condition": condition, "source": source}
    row.update({key: value for key, value in extra.items() if value is not None})
    failed.append(row)


def quote_window_pending_reconciliation(context_monitor):
    context_monitor = context_monitor or {}
    monitor_overall = context_monitor.get("overall_verdict") or {}
    monitor_task_b = (
        context_monitor.get("task_b_rolling_24h_quote_clean_window")
        or context_monitor.get("task_b_clean_window_monitor")
        or context_monitor.get("task_b")
        or {}
    )
    quote_coverage = monitor_task_b.get("quote_coverage_rolling24") or {}
    clean_rate = safe_float(quote_coverage.get("source_quote_clean_present_rate"))
    executable_rate = safe_float(quote_coverage.get("source_quote_executable_present_rate"))
    clean_missing = safe_int(quote_coverage.get("source_quote_clean_missing_rows"), 0)
    executable_missing = safe_int(quote_coverage.get("source_quote_executable_missing_rows"), 0)
    rows_scanned = safe_int(quote_coverage.get("rows_scanned"), 0)
    post_fix_rows = safe_int(monitor_task_b.get("post_fix_rows"), 0)
    writer_verified = monitor_overall.get("quote_writer_fix") == "VERIFIED_POST_DEPLOY" or (
        monitor_overall.get("context_field_writer_fix") == "VERIFIED_POST_DEPLOY"
    )
    coverage_clean = bool(
        rows_scanned > 0
        and clean_rate is not None
        and executable_rate is not None
        and clean_rate >= 0.8
        and executable_rate >= 0.8
        and clean_missing == 0
        and executable_missing == 0
    )
    reconciled = bool(writer_verified and coverage_clean)
    return {
        "reconciled": reconciled,
        "reason": "quote_pending_reconciled_by_current_rolling24_field_coverage"
        if reconciled
        else None,
        "writer_verified": writer_verified,
        "rows_scanned": rows_scanned,
        "post_fix_rows": post_fix_rows,
        "source_quote_clean_present_rate": clean_rate,
        "source_quote_executable_present_rate": executable_rate,
        "source_quote_clean_missing_rows": clean_missing,
        "source_quote_executable_missing_rows": executable_missing,
    }


def context_failed_conditions(context, volume_kline, context_monitor=None):
    failed = []
    context_monitor = context_monitor or {}
    monitor_overall = context_monitor.get("overall_verdict") or {}
    monitor_task_b = (
        context_monitor.get("task_b_rolling_24h_quote_clean_window")
        or context_monitor.get("task_b_clean_window_monitor")
        or context_monitor.get("task_b")
        or {}
    )
    monitor_task_d = (
        context_monitor.get("task_d_context_field_coverage_audit")
        or context_monitor.get("task_d")
        or {}
    )
    monitor_blockers = set(monitor_task_d.get("blockers") or [])
    quote_window_pending = monitor_overall.get("rolling24_quote_status") == "QUOTE_CLEAN_WINDOW_PENDING"
    quote_pending_reconciliation = quote_window_pending_reconciliation(context_monitor)
    context_writer_verified = monitor_overall.get("context_field_writer_fix") == "VERIFIED_POST_DEPLOY"
    if quote_window_pending and not quote_pending_reconciliation.get("reconciled"):
        add_failed_condition(
            failed,
            "quote_clean_window_pending",
            "context_blocker_monitor",
            estimated_clean_at_iso=monitor_task_b.get("estimated_clean_at_iso"),
            pre_fix_rows_remaining=monitor_task_b.get("pre_fix_rows_remaining"),
        )
    if context_writer_verified:
        for blocker, pending_condition in (
            ("lifecycle_profile_coverage_below_80pct", "lifecycle_profile_clean_window_pending"),
            ("source_component_coverage_below_80pct", "source_component_clean_window_pending"),
        ):
            if blocker in monitor_blockers:
                add_failed_condition(
                    failed,
                    pending_condition,
                    "context_blocker_monitor",
                    original_blocker=blocker,
                )
    blockers = set((context.get("blockers") or []))
    volume = volume_kline.get("volume_context") or {}
    kline = volume_kline.get("raw_gold_silver_kline") or {}
    p1_volume_cleared = bool(
        volume.get("coverage_method_version") == "p1_matured_recompute_v2"
        and not volume.get("blocker")
    )
    p1_kline_cleared = bool(
        kline.get("coverage_method_version") == "p1_confidence_time_legal_v2"
        and not kline.get("blocker")
    )
    for blocker in sorted(blockers):
        if blocker in {
            "source_quote_clean_coverage_below_80pct",
            "source_quote_executable_coverage_below_80pct",
            "lifecycle_profile_coverage_below_80pct",
            "source_component_coverage_below_80pct",
            "volume_profile_coverage_below_80pct",
            "kline_coverage_below_80pct",
            "schema_mixed_quote_sensitive_slices_blocked",
            "context_schema_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
            "quote_clean_definition_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
        }:
            if quote_window_pending and blocker in {
                "source_quote_clean_coverage_below_80pct",
                "source_quote_executable_coverage_below_80pct",
                "schema_mixed_quote_sensitive_slices_blocked",
                "context_schema_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
                "quote_clean_definition_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
            }:
                continue
            if context_writer_verified and blocker in {
                "lifecycle_profile_coverage_below_80pct",
                "source_component_coverage_below_80pct",
            }:
                continue
            if blocker == "volume_profile_coverage_below_80pct" and p1_volume_cleared:
                continue
            if blocker == "kline_coverage_below_80pct" and p1_kline_cleared:
                continue
            add_failed_condition(failed, blocker, "context_coverage_audit")
    if volume.get("blocker"):
        add_failed_condition(failed, volume.get("blocker"), "volume_kline_coverage_audit")
    if kline.get("blocker"):
        add_failed_condition(failed, kline.get("blocker"), "volume_kline_coverage_audit")
    return failed


def raw_funnel_snapshot(raw_funnel):
    summary = raw_funnel.get("summary") or {}
    raw = summary.get("raw_denominator") or {}
    decision = summary.get("decision_layer") or {}
    bridge = summary.get("entry_bridge_layer") or {}
    raw_bridge = bridge.get("raw_signal_decision_bridge") or {}
    paper_evidence_events = (bridge.get("paper_evidence_log") or {}).get("event_type_counts") or {}
    return {
        "raw_gold_silver_events": raw.get("raw_all_gold_silver_event_rows"),
        "evaluable_gold_silver_events": raw.get("evaluable_gold_silver_event_rows"),
        "raw_gold_silver_entered_events": raw.get("entered_events"),
        "candidate_matched_any_events": (summary.get("candidate_layer") or {}).get("candidate_matched_any_events"),
        "candidate_match_any_rate": (summary.get("candidate_layer") or {}).get("candidate_match_any_rate"),
        "decision_record_rate": decision.get("decision_record_rate"),
        "would_enter_rate": decision.get("would_enter_rate"),
        "entered_rate": decision.get("entered_rate"),
        "events_with_decision_record": decision.get("events_with_decision_record"),
        "would_enter_events": decision.get("would_enter_events"),
        "entered_events": decision.get("entered_events"),
        "realized_events": decision.get("realized_events"),
        "realized_rate": decision.get("realized_rate"),
        "paper_trades_entry_ts_window_count": bridge.get("paper_trades_entry_ts_window_count"),
        "paper_evidence_event_counts": paper_evidence_events,
        "paper_trade_entry_intent_events": safe_int(paper_evidence_events.get("paper_trade_entry_intent"), 0),
        "raw_signal_ids": raw_bridge.get("raw_signal_ids"),
        "raw_signals_with_decision_record": raw_bridge.get("raw_signals_with_decision_record"),
        "raw_signals_without_decision_record": raw_bridge.get("raw_signals_without_decision_record"),
        "raw_signals_with_pass_or_allow": raw_bridge.get("raw_signals_with_pass_or_allow"),
        "raw_signals_with_pending_entry": raw_bridge.get("raw_signals_with_pending_entry"),
        "raw_signals_with_final_entry_contract": raw_bridge.get("raw_signals_with_final_entry_contract"),
        "raw_signals_with_final_entry_block": raw_bridge.get("raw_signals_with_final_entry_block"),
        "raw_signals_with_final_entry_mode_disabled": raw_bridge.get("raw_signals_with_final_entry_mode_disabled"),
        "raw_signals_with_final_entry_mode_disabled_only": raw_bridge.get("raw_signals_with_final_entry_mode_disabled_only"),
        "raw_signals_with_final_entry_mode_disabled_plus_other": raw_bridge.get("raw_signals_with_final_entry_mode_disabled_plus_other"),
        "no_decision_record_root_cause_counts": raw_bridge.get("no_decision_record_root_cause_counts"),
        "no_decision_record_subroot_cause_counts": raw_bridge.get("no_decision_record_subroot_cause_counts"),
        "no_decision_record_examples": raw_bridge.get("no_decision_record_examples"),
        "shadow_no_decision_entry_hypothesis_family_counts": raw_bridge.get(
            "shadow_no_decision_entry_hypothesis_family_counts"
        ),
        "shadow_no_decision_entry_hypothesis_candidate_counts": raw_bridge.get(
            "shadow_no_decision_entry_hypothesis_candidate_counts"
        ),
        "shadow_no_decision_entry_hypothesis_reason_counts": raw_bridge.get(
            "shadow_no_decision_entry_hypothesis_reason_counts"
        ),
        "shadow_no_decision_entry_hypothesis_signal_examples": raw_bridge.get(
            "shadow_no_decision_entry_hypothesis_signal_examples"
        ),
        "no_decision_token_time_decision_without_exact_signal_id": raw_bridge.get(
            "no_decision_token_time_decision_without_exact_signal_id"
        ),
        "no_decision_candidate_shadow_observed_no_decision_event": raw_bridge.get(
            "no_decision_candidate_shadow_observed_no_decision_event"
        ),
        "no_decision_partial_candidate_observation_no_decision_event": raw_bridge.get(
            "no_decision_partial_candidate_observation_no_decision_event"
        ),
        "no_decision_no_candidate_observation_or_decision_event": raw_bridge.get(
            "no_decision_no_candidate_observation_or_decision_event"
        ),
        "no_decision_raw_event_missing_signal_id": raw_bridge.get("no_decision_raw_event_missing_signal_id"),
        "raw_signals_with_decision_no_pass_or_allow": raw_bridge.get("raw_signals_with_decision_no_pass_or_allow"),
        "decision_no_pass_or_allow_reason_counts": raw_bridge.get("decision_no_pass_or_allow_reason_counts"),
        "decision_no_pass_or_allow_examples": raw_bridge.get("decision_no_pass_or_allow_examples"),
        "raw_signals_pass_or_allow_without_pending_entry": raw_bridge.get("raw_signals_pass_or_allow_without_pending_entry"),
        "pass_or_allow_without_pending_entry_reason_counts": raw_bridge.get("pass_or_allow_without_pending_entry_reason_counts"),
        "pass_or_allow_without_pending_entry_examples": raw_bridge.get("pass_or_allow_without_pending_entry_examples"),
        "raw_signals_pending_without_final_entry_contract": raw_bridge.get("raw_signals_pending_without_final_entry_contract"),
        "pending_without_final_entry_reason_counts": raw_bridge.get("pending_without_final_entry_reason_counts"),
        "pending_without_final_entry_examples": raw_bridge.get("pending_without_final_entry_examples"),
        "raw_scoped_final_entry_hard_blockers": raw_bridge.get("raw_scoped_final_entry_hard_blockers"),
    }


PENDING_GAP_CATEGORY_META = {
    "POLICY_OR_SOURCE_PREREQUISITE": {
        "description": "A direct-entry or source prerequisite prevented the pending signal from reaching final_entry_contract.",
        "automatic_allowed_scope": "data/source-prerequisite audit only",
        "human_approval_required_if_fix_requires": "enabling direct entry or relaxing prerequisite policy",
    },
    "DATA_OR_MARKET_CONTEXT_BLOCK": {
        "description": "Missing or unfavorable kline/volume/context evidence prevented final-entry evaluation.",
        "automatic_allowed_scope": "data coverage, writer, evaluator, or shadow-only context audit",
        "human_approval_required_if_fix_requires": "relaxing market/context gates",
    },
    "MODE_SHADOW_OR_RATE_LIMIT": {
        "description": "A shadow/rate-limit/canary mode condition prevented final-entry evaluation.",
        "automatic_allowed_scope": "read-only mode readiness audit",
        "human_approval_required_if_fix_requires": "resetting SHADOW, changing rate limits, or enabling paper/live execution",
    },
    "QUALITY_OR_TIMING_REJECT": {
        "description": "The signal was rejected by quality or timing logic before final_entry_contract.",
        "automatic_allowed_scope": "shadow-only candidate/evaluator analysis",
        "human_approval_required_if_fix_requires": "changing strategy thresholds or timing policy",
    },
    "SIGNAL_SUPERSEDED_OR_ABORTED": {
        "description": "The pending signal was superseded or aborted before final_entry_contract.",
        "automatic_allowed_scope": "read-only duplicate/refresh attribution audit",
        "human_approval_required_if_fix_requires": "changing signal refresh or abort policy",
    },
    "UNKNOWN_PENDING_TO_FINAL_GAP": {
        "description": "The audit could not classify the pending-to-final gap deterministically.",
        "automatic_allowed_scope": "instrumentation and evaluator audit",
        "human_approval_required_if_fix_requires": "runtime behavior changes",
    },
}


UPSTREAM_GAP_CATEGORY_META = {
    "NO_DECISION_RECORD": {
        "description": "The raw gold/silver signal had no decision event in the audited window.",
        "automatic_allowed_scope": "instrumentation, join, or observer audit",
        "human_approval_required_if_fix_requires": "changing runtime signal routing or executor behavior",
    },
    "POLICY_OR_SOURCE_PREREQUISITE": PENDING_GAP_CATEGORY_META["POLICY_OR_SOURCE_PREREQUISITE"],
    "DATA_OR_MARKET_CONTEXT_BLOCK": PENDING_GAP_CATEGORY_META["DATA_OR_MARKET_CONTEXT_BLOCK"],
    "MODE_SHADOW_OR_RATE_LIMIT": PENDING_GAP_CATEGORY_META["MODE_SHADOW_OR_RATE_LIMIT"],
    "QUALITY_OR_TIMING_REJECT": PENDING_GAP_CATEGORY_META["QUALITY_OR_TIMING_REJECT"],
    "SIGNAL_SUPERSEDED_OR_ABORTED": PENDING_GAP_CATEGORY_META["SIGNAL_SUPERSEDED_OR_ABORTED"],
    "UNKNOWN_UPSTREAM_GAP": {
        "description": "The audit could not classify the upstream decision/pass/pending gap deterministically.",
        "automatic_allowed_scope": "instrumentation and evaluator audit",
        "human_approval_required_if_fix_requires": "runtime behavior changes",
    },
}


def classify_pending_gap_reason(row):
    component = str(row.get("component") or "").lower()
    event_type = str(row.get("event_type") or "").lower()
    decision = str(row.get("decision") or "").lower()
    reason = str(row.get("reason") or "").lower()
    if "gmgn_pre_seen_required" in reason or "direct_entry_disabled" in reason:
        return "POLICY_OR_SOURCE_PREREQUISITE"
    if "no_kline" in reason or "low_volume" in reason or "kline" in reason:
        return "DATA_OR_MARKET_CONTEXT_BLOCK"
    if (
        "shadow" in reason
        or "rate_limited" in reason
        or "canary" in reason
        or component in {"entry_mode_quality", "revival_canary"}
    ):
        return "MODE_SHADOW_OR_RATE_LIMIT"
    if event_type == "entry_abort" or decision == "supersede" or "refresh" in reason:
        return "SIGNAL_SUPERSEDED_OR_ABORTED"
    if component in {"smart_entry", "scout_quality"} or decision in {"reject", "block", "watch_only"}:
        return "QUALITY_OR_TIMING_REJECT"
    return "UNKNOWN_PENDING_TO_FINAL_GAP"


def classify_upstream_gap_reason(row, stage):
    if stage == "no_decision_record":
        return "NO_DECISION_RECORD"
    component = str(row.get("component") or "").lower()
    event_type = str(row.get("event_type") or "").lower()
    decision = str(row.get("decision") or "").lower()
    reason = str(row.get("reason") or "").lower()
    if "gmgn_pre_seen_required" in reason or "direct_entry_disabled" in reason or "policy" in reason:
        return "POLICY_OR_SOURCE_PREREQUISITE"
    if (
        "no_kline" in reason
        or "low_volume" in reason
        or "kline" in reason
        or "quote" in reason
        or "route" in reason
        or "spread" in reason
        or "stale" in reason
    ):
        return "DATA_OR_MARKET_CONTEXT_BLOCK"
    if "shadow" in reason or "rate_limited" in reason or "canary" in reason or "mode" in reason:
        return "MODE_SHADOW_OR_RATE_LIMIT"
    if event_type == "entry_abort" or decision == "supersede" or "refresh" in reason:
        return "SIGNAL_SUPERSEDED_OR_ABORTED"
    if (
        component in {"smart_entry", "scout_quality", "matrix_evaluator"}
        or "quality" in reason
        or "timing" in reason
        or decision in {"reject", "block", "watch_only", "wait", "skip"}
    ):
        return "QUALITY_OR_TIMING_REJECT"
    return "UNKNOWN_UPSTREAM_GAP"


def categorize_pending_to_final_gap(reason_counts):
    categories = {}
    for row in reason_counts or []:
        category = classify_pending_gap_reason(row)
        bucket = categories.setdefault(
            category,
            {
                "category": category,
                "count": 0,
                **PENDING_GAP_CATEGORY_META[category],
                "top_reasons": [],
            },
        )
        count = safe_int(row.get("count"), 0) or 0
        bucket["count"] += count
        if len(bucket["top_reasons"]) < 8:
            bucket["top_reasons"].append(
                {
                    "component": row.get("component"),
                    "event_type": row.get("event_type"),
                    "decision": row.get("decision"),
                    "reason": row.get("reason"),
                    "count": count,
                }
            )
    total = sum(item["count"] for item in categories.values())
    rows = []
    for item in categories.values():
        item["share_of_pending_without_final"] = rate(item["count"], total)
        rows.append(item)
    rows.sort(key=lambda item: (-item["count"], item["category"]))
    return {
        "total_classified": total,
        "categories": rows,
        "automatic_runtime_change_allowed": False,
        "strategy_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def categorize_upstream_funnel_gap(
    no_decision_count,
    decision_no_pass_counts,
    pass_without_pending_counts,
    no_decision_root_cause_counts=None,
    no_decision_subroot_cause_counts=None,
):
    categories = {}

    def add_category(category, count, reason_row=None, stage=None):
        if count <= 0:
            return
        meta = UPSTREAM_GAP_CATEGORY_META[category]
        bucket = categories.setdefault(
            category,
            {
                "category": category,
                "count": 0,
                **meta,
                "top_reasons": [],
            },
        )
        bucket["count"] += count
        if reason_row is not None and len(bucket["top_reasons"]) < 8:
            bucket["top_reasons"].append(
                {
                    "stage": stage,
                    "component": reason_row.get("component"),
                    "event_type": reason_row.get("event_type"),
                    "decision": reason_row.get("decision"),
                    "reason": reason_row.get("reason"),
                    "count": count,
                }
            )

    add_category("NO_DECISION_RECORD", safe_int(no_decision_count, 0) or 0, stage="no_decision_record")
    no_decision_bucket = categories.get("NO_DECISION_RECORD")
    if no_decision_bucket is not None:
        for row in (no_decision_subroot_cause_counts or no_decision_root_cause_counts or []):
            count = safe_int(row.get("count"), 0) or 0
            if count <= 0 or len(no_decision_bucket["top_reasons"]) >= 8:
                continue
            no_decision_bucket["top_reasons"].append(
                {
                    "stage": "no_decision_record",
                    "component": "raw_signal_decision_bridge",
                    "event_type": "no_decision_record",
                    "decision": "MISSING",
                    "reason": row.get("root_cause"),
                    "description": row.get("description"),
                    "count": count,
                }
            )
    for row in decision_no_pass_counts or []:
        count = safe_int(row.get("count"), 0) or 0
        add_category(
            classify_upstream_gap_reason(row, "decision_no_pass_or_allow"),
            count,
            reason_row=row,
            stage="decision_no_pass_or_allow",
        )
    for row in pass_without_pending_counts or []:
        count = safe_int(row.get("count"), 0) or 0
        add_category(
            classify_upstream_gap_reason(row, "pass_or_allow_without_pending_entry"),
            count,
            reason_row=row,
            stage="pass_or_allow_without_pending_entry",
        )

    total = sum(item["count"] for item in categories.values())
    rows = []
    for item in categories.values():
        item["share_of_upstream_gap"] = rate(item["count"], total)
        rows.append(item)
    rows.sort(key=lambda item: (-item["count"], item["category"]))
    return {
        "total_classified": total,
        "categories": rows,
        "automatic_runtime_change_allowed": False,
        "strategy_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def readiness_gap_priority(raw_signals, mode_disabled_only, final_entry_contract, pending_gap_categories):
    target_count = int(math.ceil(float(raw_signals or 0) * 0.6)) if raw_signals else None
    current_shortfall = None if target_count is None else max(0, target_count - int(mode_disabled_only or 0))
    categories = []
    for item in (pending_gap_categories or {}).get("categories") or []:
        count = safe_int(item.get("count"), 0) or 0
        optimistic_mode_adjusted = int(mode_disabled_only or 0) + count
        optimistic_final_contract = int(final_entry_contract or 0) + count
        categories.append(
            {
                "category": item.get("category"),
                "count": count,
                "share_of_pending_without_final": item.get("share_of_pending_without_final"),
                "current_mode_disabled_adjusted_final_eligibility_count": int(mode_disabled_only or 0),
                "optimistic_mode_adjusted_final_eligibility_count_if_all_bridged": optimistic_mode_adjusted,
                "optimistic_mode_adjusted_final_eligibility_rate_if_all_bridged": rate(optimistic_mode_adjusted, raw_signals),
                "optimistic_final_entry_contract_count_if_all_bridged": optimistic_final_contract,
                "optimistic_final_entry_contract_rate_if_all_bridged": rate(optimistic_final_contract, raw_signals),
                "remaining_shortfall_to_60_if_all_bridged": (
                    None if target_count is None else max(0, target_count - optimistic_mode_adjusted)
                ),
                "can_reach_60_alone_under_optimistic_assumption": (
                    False if target_count is None else optimistic_mode_adjusted >= target_count
                ),
                "evidence_level": "optimistic_readiness_upper_bound_not_policy_change",
                "requires_final_entry_contract_eval": True,
                "promotion_allowed": False,
                "automatic_allowed_scope": item.get("automatic_allowed_scope"),
                "human_approval_required_if_fix_requires": item.get("human_approval_required_if_fix_requires"),
                "top_reasons": item.get("top_reasons") or [],
            }
        )
    categories.sort(
        key=lambda item: (
            item["remaining_shortfall_to_60_if_all_bridged"] if item["remaining_shortfall_to_60_if_all_bridged"] is not None else 10**9,
            -item["count"],
            item.get("category") or "",
        )
    )
    return {
        "target": "mode_disabled_adjusted_final_eligibility_rate >= 0.60",
        "denominator_raw_signal_ids": raw_signals,
        "target_count_60pct": target_count,
        "current_mode_disabled_adjusted_final_eligibility_count": mode_disabled_only,
        "current_final_entry_contract_count": final_entry_contract,
        "current_shortfall_to_60": current_shortfall,
        "categories_ranked_by_optimistic_readiness_gain": categories,
        "interpretation": "Upper bounds only. Bridging a category would still require final_entry_contract evaluation; strategy, mode, gate, executor, and risk changes require human approval.",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "paper_enablement_allowed": False,
        "automatic_runtime_change_allowed": False,
    }


def upstream_gap_priority(raw_signals, pending, upstream_gap_categories):
    target_count = int(math.ceil(float(raw_signals or 0) * 0.6)) if raw_signals else None
    current_shortfall = None if target_count is None else max(0, target_count - int(pending or 0))
    categories = []
    for item in (upstream_gap_categories or {}).get("categories") or []:
        count = safe_int(item.get("count"), 0) or 0
        optimistic_pending = int(pending or 0) + count
        categories.append(
            {
                "category": item.get("category"),
                "count": count,
                "share_of_upstream_gap": item.get("share_of_upstream_gap"),
                "current_pending_entry_count": int(pending or 0),
                "optimistic_pending_entry_count_if_all_bridged": optimistic_pending,
                "optimistic_pending_capture_rate_if_all_bridged": rate(optimistic_pending, raw_signals),
                "remaining_shortfall_to_60_pending_if_all_bridged": (
                    None if target_count is None else max(0, target_count - optimistic_pending)
                ),
                "can_reach_60_pending_alone_under_optimistic_assumption": (
                    False if target_count is None else optimistic_pending >= target_count
                ),
                "evidence_level": "optimistic_readiness_upper_bound_not_policy_change",
                "requires_final_entry_contract_eval": True,
                "promotion_allowed": False,
                "automatic_allowed_scope": item.get("automatic_allowed_scope"),
                "human_approval_required_if_fix_requires": item.get("human_approval_required_if_fix_requires"),
                "top_reasons": item.get("top_reasons") or [],
            }
        )
    categories.sort(
        key=lambda item: (
            item["remaining_shortfall_to_60_pending_if_all_bridged"]
            if item["remaining_shortfall_to_60_pending_if_all_bridged"] is not None
            else 10**9,
            -item["count"],
            item.get("category") or "",
        )
    )
    return {
        "target": "pending_capture_rate >= 0.60 before final_entry_contract readiness",
        "denominator_raw_signal_ids": raw_signals,
        "target_count_60pct": target_count,
        "current_pending_entry_count": pending,
        "current_shortfall_to_60_pending": current_shortfall,
        "categories_ranked_by_optimistic_pending_gain": categories,
        "interpretation": "Upper bounds only. Bridging upstream gaps would still require final_entry_contract evaluation; strategy, mode, gate, executor, and risk changes require human approval.",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "paper_enablement_allowed": False,
        "automatic_runtime_change_allowed": False,
    }


def build_capture_stage_rates(raw_snapshot, final_contract):
    raw_events = safe_int(raw_snapshot.get("raw_gold_silver_events"), 0)
    raw_signals = safe_int(raw_snapshot.get("raw_signal_ids"), 0) or raw_events
    decision_records = safe_int(raw_snapshot.get("raw_signals_with_decision_record"), 0)
    if not decision_records:
        decision_records = safe_int(raw_snapshot.get("events_with_decision_record"), 0)
    pass_or_allow = safe_int(raw_snapshot.get("raw_signals_with_pass_or_allow"), 0)
    pending = safe_int(raw_snapshot.get("raw_signals_with_pending_entry"), 0)
    raw_final_entry_contract = raw_snapshot.get("raw_signals_with_final_entry_contract")
    final_entry_contract = safe_int(raw_final_entry_contract, 0)
    if raw_final_entry_contract is None:
        final_entry_contract = safe_int(final_contract.get("unique_signal_ids"), 0)
    scoped_mode_disabled_only_available = raw_snapshot.get("raw_signals_with_final_entry_mode_disabled_only") is not None
    mode_disabled_only = (
        safe_int(raw_snapshot.get("raw_signals_with_final_entry_mode_disabled_only"), 0)
        if scoped_mode_disabled_only_available
        else 0
    )
    entered = safe_int(raw_snapshot.get("entered_events"), 0)
    realized = safe_int(raw_snapshot.get("realized_events"), 0)
    paper_committed = safe_int(raw_snapshot.get("paper_trades_entry_ts_window_count"), 0)
    paper_intent = safe_int(raw_snapshot.get("paper_trade_entry_intent_events"), 0)
    candidate_matched_any = safe_int(raw_snapshot.get("candidate_matched_any_events"), 0)
    detector_rate = raw_snapshot.get("candidate_match_any_rate")
    if detector_rate is None:
        detector_rate = rate(candidate_matched_any, raw_events)
    exact_pending_without_final = safe_int(raw_snapshot.get("raw_signals_pending_without_final_entry_contract"), None)
    pending_without_final = (
        exact_pending_without_final
        if exact_pending_without_final is not None
        else max(0, pending - final_entry_contract)
    )
    final_without_mode_adjusted = max(0, final_entry_contract - mode_disabled_only)
    mode_adjusted_rate = rate(mode_disabled_only, raw_signals) if scoped_mode_disabled_only_available else None
    pending_reason_counts = raw_snapshot.get("pending_without_final_entry_reason_counts") or []
    pending_gap_categories = categorize_pending_to_final_gap(pending_reason_counts)
    readiness_priority = readiness_gap_priority(
        raw_signals,
        mode_disabled_only,
        final_entry_contract,
        pending_gap_categories,
    )
    no_decision = safe_int(raw_snapshot.get("raw_signals_without_decision_record"), 0)
    decision_no_pass = safe_int(raw_snapshot.get("raw_signals_with_decision_no_pass_or_allow"), 0)
    pass_without_pending = safe_int(raw_snapshot.get("raw_signals_pass_or_allow_without_pending_entry"), 0)
    upstream_categories = categorize_upstream_funnel_gap(
        no_decision,
        raw_snapshot.get("decision_no_pass_or_allow_reason_counts") or [],
        raw_snapshot.get("pass_or_allow_without_pending_entry_reason_counts") or [],
        raw_snapshot.get("no_decision_record_root_cause_counts") or [],
        raw_snapshot.get("no_decision_record_subroot_cause_counts") or [],
    )
    upstream_priority = upstream_gap_priority(raw_signals, pending, upstream_categories)
    readiness_status = "SCOPED_FINAL_ENTRY_BLOCKERS_MISSING"
    if scoped_mode_disabled_only_available:
        readiness_status = (
            "CAPTURE_READINESS_60_REACHED"
            if mode_adjusted_rate is not None and mode_adjusted_rate >= 0.6
            else "CAPTURE_READINESS_BELOW_60"
        )
    return {
        "denominator_raw_gold_silver_events": raw_events,
        "denominator_raw_signal_ids": raw_signals,
        "detector_capture_rate": detector_rate,
        "decision_record_capture_rate": rate(decision_records, raw_signals),
        "pass_allow_capture_rate": rate(pass_or_allow, raw_signals),
        "pending_capture_rate": rate(pending, raw_signals),
        "final_entry_contract_reach_rate": rate(final_entry_contract, raw_signals),
        "mode_disabled_adjusted_final_eligibility_rate": mode_adjusted_rate,
        "paper_capture_rate": rate(paper_committed, raw_signals),
        "paper_trade_intent_rate": rate(paper_intent, raw_signals),
        "actual_entered_rate": rate(entered, raw_signals),
        "realized_capture_rate": (
            raw_snapshot.get("realized_rate")
            if raw_snapshot.get("realized_rate") is not None
            else rate(realized, raw_signals)
        ),
        "events": {
            "candidate_matched_any": candidate_matched_any,
            "decision_records": decision_records,
            "pass_or_allow": pass_or_allow,
            "pending_entry": pending,
            "final_entry_contract": final_entry_contract,
            "mode_disabled_only_final_entry": mode_disabled_only,
            "mode_disabled_final_entry": safe_int(raw_snapshot.get("raw_signals_with_final_entry_mode_disabled"), 0),
            "mode_disabled_plus_other_final_entry": safe_int(raw_snapshot.get("raw_signals_with_final_entry_mode_disabled_plus_other"), 0),
            "entered": entered,
            "realized": realized,
            "paper_trade_intent": paper_intent,
            "paper_committed": paper_committed,
        },
        "pending_to_final_entry_gap": {
            "pending_entry_signal_ids": pending,
            "final_entry_contract_signal_ids": final_entry_contract,
            "pending_without_final_entry_contract": pending_without_final,
            "pending_to_final_entry_contract_rate": rate(final_entry_contract, pending),
            "pending_to_mode_adjusted_final_eligibility_rate": rate(mode_disabled_only, pending),
            "pending_without_final_entry_contract_attributed": raw_snapshot.get(
                "raw_signals_pending_without_final_entry_contract"
            ),
            "pending_without_final_entry_reason_counts": raw_snapshot.get(
                "pending_without_final_entry_reason_counts"
            )
            or [],
            "pending_without_final_entry_examples": raw_snapshot.get(
                "pending_without_final_entry_examples"
            )
            or [],
            "pending_without_final_entry_category_counts": pending_gap_categories,
            "readiness_gap_priority": readiness_priority,
        },
        "upstream_funnel_gap": {
            "raw_signal_ids": raw_signals,
            "decision_record_signal_ids": decision_records,
            "pass_or_allow_signal_ids": pass_or_allow,
            "pending_entry_signal_ids": pending,
            "no_decision_record": no_decision,
            "no_decision_record_root_cause_counts": raw_snapshot.get("no_decision_record_root_cause_counts") or [],
            "no_decision_record_subroot_cause_counts": raw_snapshot.get("no_decision_record_subroot_cause_counts") or [],
            "no_decision_record_examples": raw_snapshot.get("no_decision_record_examples") or [],
            "shadow_no_decision_entry_hypothesis_family_counts": raw_snapshot.get(
                "shadow_no_decision_entry_hypothesis_family_counts"
            )
            or [],
            "shadow_no_decision_entry_hypothesis_candidate_counts": raw_snapshot.get(
                "shadow_no_decision_entry_hypothesis_candidate_counts"
            )
            or [],
            "shadow_no_decision_entry_hypothesis_reason_counts": raw_snapshot.get(
                "shadow_no_decision_entry_hypothesis_reason_counts"
            )
            or [],
            "shadow_no_decision_entry_hypothesis_signal_examples": raw_snapshot.get(
                "shadow_no_decision_entry_hypothesis_signal_examples"
            )
            or [],
            "no_decision_token_time_decision_without_exact_signal_id": safe_int(
                raw_snapshot.get("no_decision_token_time_decision_without_exact_signal_id"), 0
            ),
            "no_decision_candidate_shadow_observed_no_decision_event": safe_int(
                raw_snapshot.get("no_decision_candidate_shadow_observed_no_decision_event"), 0
            ),
            "no_decision_partial_candidate_observation_no_decision_event": safe_int(
                raw_snapshot.get("no_decision_partial_candidate_observation_no_decision_event"), 0
            ),
            "no_decision_no_candidate_observation_or_decision_event": safe_int(
                raw_snapshot.get("no_decision_no_candidate_observation_or_decision_event"), 0
            ),
            "no_decision_raw_event_missing_signal_id": safe_int(
                raw_snapshot.get("no_decision_raw_event_missing_signal_id"), 0
            ),
            "decision_no_pass_or_allow": decision_no_pass,
            "pass_or_allow_without_pending_entry": pass_without_pending,
            "total_upstream_gap": no_decision + decision_no_pass + pass_without_pending,
            "decision_record_capture_rate": rate(decision_records, raw_signals),
            "pass_allow_capture_rate": rate(pass_or_allow, raw_signals),
            "pending_capture_rate": rate(pending, raw_signals),
            "decision_no_pass_or_allow_reason_counts": raw_snapshot.get("decision_no_pass_or_allow_reason_counts") or [],
            "decision_no_pass_or_allow_examples": raw_snapshot.get("decision_no_pass_or_allow_examples") or [],
            "pass_or_allow_without_pending_entry_reason_counts": raw_snapshot.get(
                "pass_or_allow_without_pending_entry_reason_counts"
            )
            or [],
            "pass_or_allow_without_pending_entry_examples": raw_snapshot.get(
                "pass_or_allow_without_pending_entry_examples"
            )
            or [],
            "upstream_gap_category_counts": upstream_categories,
            "upstream_gap_priority": upstream_priority,
            "automatic_runtime_change_allowed": False,
            "strategy_change_allowed": False,
            "paper_enablement_allowed": False,
        },
        "mode_disabled_adjusted_final_eligibility": {
            "status": readiness_status,
            "raw_scoped_blockers_available": scoped_mode_disabled_only_available,
            "mode_disabled_only_unique_signal_ids": mode_disabled_only,
            "final_entry_contract_unique_signal_ids": final_entry_contract,
            "final_entry_contract_not_mode_adjusted_signal_ids": final_without_mode_adjusted,
            "rate": mode_adjusted_rate,
            "denominator_raw_signal_ids": raw_signals,
            "definition": "raw gold/silver signals that reached final_entry_contract with mode_disabled as the only hard blocker",
            "raw_scoped_final_entry_hard_blockers": raw_snapshot.get("raw_scoped_final_entry_hard_blockers") or {},
            "unscoped_window_mode_disabled_only_unique_signal_ids": safe_int(
                final_contract.get("mode_disabled_only_unique_signal_ids"), 0
            ),
        },
    }


def build_readiness_shortfall_summary(capture_stage_rates):
    raw_signals = safe_int(capture_stage_rates.get("denominator_raw_signal_ids"), 0)
    target_count = int(math.ceil(float(raw_signals) * 0.6)) if raw_signals else None
    events = capture_stage_rates.get("events") or {}
    mode_adjusted = capture_stage_rates.get("mode_disabled_adjusted_final_eligibility") or {}
    pending_gap = capture_stage_rates.get("pending_to_final_entry_gap") or {}
    upstream_gap = capture_stage_rates.get("upstream_funnel_gap") or {}
    readiness_priority = pending_gap.get("readiness_gap_priority") or {}
    upstream_priority = upstream_gap.get("upstream_gap_priority") or {}
    final_eligible = safe_int(mode_adjusted.get("mode_disabled_only_unique_signal_ids"), 0)
    pending_count = safe_int(events.get("pending_entry"), 0)
    paper_intent = safe_int(events.get("paper_trade_intent"), 0)
    paper_committed = safe_int(events.get("paper_committed"), 0)
    best_pending_to_final = (readiness_priority.get("categories_ranked_by_optimistic_readiness_gain") or [None])[0]
    best_upstream = (upstream_priority.get("categories_ranked_by_optimistic_pending_gain") or [None])[0]
    return {
        "target": "mode_disabled_adjusted_final_eligibility_rate >= 0.60 while A_CLASS is SHADOW",
        "denominator_raw_signal_ids": raw_signals,
        "target_count_60pct": target_count,
        "current_mode_disabled_adjusted_final_eligibility_count": final_eligible,
        "current_mode_disabled_adjusted_final_eligibility_rate": capture_stage_rates.get(
            "mode_disabled_adjusted_final_eligibility_rate"
        ),
        "current_pending_entry_count": pending_count,
        "current_pending_capture_rate": capture_stage_rates.get("pending_capture_rate"),
        "current_paper_trade_intent_count": paper_intent,
        "current_paper_trade_intent_rate": capture_stage_rates.get("paper_trade_intent_rate"),
        "current_paper_committed_count": paper_committed,
        "current_paper_capture_rate": capture_stage_rates.get("paper_capture_rate"),
        "shortfall_to_60_final_eligibility": (
            None if target_count is None else max(0, target_count - final_eligible)
        ),
        "shortfall_to_60_pending": (
            None if target_count is None else max(0, target_count - pending_count)
        ),
        "readiness_status": mode_adjusted.get("status"),
        "largest_pending_to_final_gap_category": best_pending_to_final,
        "largest_upstream_gap_category": best_upstream,
        "interpretation": (
            "Read-only readiness summary. Counts are evidence for where the funnel loses raw gold/silver; "
            "bridging categories that require strategy, mode, gate, executor, or risk changes requires human approval."
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "paper_enablement_allowed": False,
        "automatic_runtime_change_allowed": False,
    }


def recovery_sla_requirements_satisfied(recovery_sla, clean_counter, failed_conditions):
    recovery_sla = recovery_sla if isinstance(recovery_sla, dict) else {}
    clean_ok = bool(not failed_conditions and (clean_counter or {}).get("sufficient"))
    motion = recovery_sla.get("motion_trace_review") or {}
    motion_ok = bool(not motion.get("required") or motion.get("available"))
    cooldown_ok = bool(recovery_sla.get("cooldown_elapsed"))
    return bool(clean_ok and motion_ok and cooldown_ok)


def paper_ready_tracker_from_detail(detail):
    if not isinstance(detail, dict):
        return {}
    value = (
        detail.get("paper_entry_ready_tracker")
        or detail.get("paper_auto_resume_ready_tracker")
        or detail.get("paper_entry_proposal_ready_tracker")
    )
    return value if isinstance(value, dict) else {}


def build_handoff_escalation_sla(state, paper_status, now_ts):
    state = state if isinstance(state, dict) else {}
    tracker = paper_ready_tracker_from_detail(state.get("detail") or {})
    ready = str(paper_status or "").upper() in {
        "PAPER_AUTO_RESUME_READY_LIVE_REENABLE_REQUIRES_HUMAN",
        "PAPER_ENTRY_PROPOSAL_READY_REQUIRES_HUMAN_APPROVAL",
    }
    ready_since = safe_float(tracker.get("ready_since_ts"))
    if ready and ready_since is None:
        ready_since = float(now_ts)
    elapsed = None if not ready or ready_since is None else max(0.0, float(now_ts) - ready_since)
    high_priority = bool(elapsed is not None and elapsed > HUMAN_ESCALATION_SEC)
    last_reminder_ts = safe_float(tracker.get("last_daily_reminder_ts"))
    reminder_due = bool(
        high_priority
        and (last_reminder_ts is None or float(now_ts) - last_reminder_ts >= DAILY_REMINDER_SEC)
    )
    return {
        "schema_version": "a_class_handoff_escalation_sla.v1",
        "tracked_status": paper_status,
        "ready": ready,
        "ready_since_ts": ready_since,
        "ready_elapsed_sec": elapsed,
        "threshold_sec": HUMAN_ESCALATION_SEC,
        "high_priority": high_priority,
        "daily_reminder_due": reminder_due,
        "daily_reminder_field": "a_class_live_human_review_daily_reminder",
        "last_daily_reminder_ts": last_reminder_ts,
        "promotion_allowed": False,
    }


def persist_paper_ready_tracker(db_path, paper_status, now_ts):
    result = {
        "attempted": True,
        "available": False,
        "mode_key": MODE_KEY,
        "changes_runtime_mode": False,
        "changes_circuit_breaker": False,
    }
    try:
        db = sqlite3.connect(db_path)
        db.row_factory = sqlite3.Row
    except Exception as exc:
        result["reason"] = "db_open_failed"
        result["error"] = str(exc)
        return result
    try:
        if not table_exists(db, "a_class_mode_runtime_state"):
            result["reason"] = "a_class_mode_runtime_state_table_missing"
            return result
        row = db.execute(
            "SELECT detail_json FROM a_class_mode_runtime_state WHERE mode_key = ?",
            (MODE_KEY,),
        ).fetchone()
        if row is None:
            result["reason"] = "a_class_fastlane_row_missing"
            return result
        detail = jloads(row_value(row, "detail_json"), {})
        if not isinstance(detail, dict):
            detail = {}
        ready = str(paper_status or "").upper() in {
            "PAPER_AUTO_RESUME_READY_LIVE_REENABLE_REQUIRES_HUMAN",
            "PAPER_ENTRY_PROPOSAL_READY_REQUIRES_HUMAN_APPROVAL",
        }
        tracker = paper_ready_tracker_from_detail(detail)
        if ready:
            tracker.setdefault("ready_since_ts", float(now_ts))
            tracker["last_status"] = paper_status
            tracker["last_seen_ts"] = float(now_ts)
        else:
            tracker = {
                "last_status": paper_status,
                "last_seen_ts": float(now_ts),
                "ready_since_ts": None,
            }
        tracker["schema_version"] = "a_class_paper_ready_tracker.v1"
        detail["paper_entry_ready_tracker"] = tracker
        db.execute(
            """
            UPDATE a_class_mode_runtime_state
            SET detail_json = ?, updated_at = ?
            WHERE mode_key = ?
            """,
            (json.dumps(detail, sort_keys=True), float(now_ts), MODE_KEY),
        )
        db.commit()
        result.update({"available": True, "reason": "persisted", "tracker": tracker})
        return result
    finally:
        db.close()


def build_paper_entry_proposal_readiness(classification, capture_stage_rates, failed_conditions, clean_counter, recovery_sla=None):
    mode_adjusted = capture_stage_rates.get("mode_disabled_adjusted_final_eligibility") or {}
    rate_value = capture_stage_rates.get("mode_disabled_adjusted_final_eligibility_rate")
    events = capture_stage_rates.get("events") or {}
    reasons = []
    if failed_conditions:
        reasons.append("clean_window_conditions_not_passed")
    if clean_counter and not clean_counter.get("sufficient"):
        reasons.append("clean_window_streak_below_required")
    recovery_sla = recovery_sla if isinstance(recovery_sla, dict) else {}
    motion = recovery_sla.get("motion_trace_review") or {}
    if motion.get("required") and not motion.get("available"):
        reasons.append("motion_trace_review_artifact_missing")
    if recovery_sla and not recovery_sla.get("cooldown_elapsed"):
        reasons.append("cooldown_not_elapsed")
    final_status = classification.get("final_entry_status")
    paper_auto_resume_allowed = bool(
        final_status in {"FUNNEL_READY_FOR_PAPER_AUTO_RESUME", "FUNNEL_BLOCKED_STUCK", "FUNNEL_READY_FOR_PAPER_PROPOSAL"}
        and not reasons
        and recovery_sla_requirements_satisfied(recovery_sla, clean_counter, failed_conditions)
    )
    if paper_auto_resume_allowed:
        status = "PAPER_AUTO_RESUME_READY_LIVE_REENABLE_REQUIRES_HUMAN"
    elif final_status == "FUNNEL_BLOCKED_STUCK" and not reasons:
        status = "PAPER_ENTRY_PROPOSAL_READY_REQUIRES_HUMAN_APPROVAL"
    elif final_status == "FUNNEL_READY_FOR_PAPER_PROPOSAL" and not reasons:
        status = "PAPER_ENTRY_PROPOSAL_READY_REQUIRES_HUMAN_APPROVAL"
    else:
        status = "NOT_READY_FOR_PAPER_ENTRY_PROPOSAL"
    return {
        "status": status,
        "ready": bool(status in {
            "PAPER_AUTO_RESUME_READY_LIVE_REENABLE_REQUIRES_HUMAN",
            "PAPER_ENTRY_PROPOSAL_READY_REQUIRES_HUMAN_APPROVAL",
        }),
        "paper_auto_resume_allowed": paper_auto_resume_allowed,
        "paper_auto_resume_status": (
            "ELIGIBLE_AFTER_SLA" if paper_auto_resume_allowed else "NOT_ELIGIBLE"
        ),
        "final_entry_status": final_status,
        "current_capture_stage": classification.get("current_capture_stage"),
        "mode_disabled_adjusted_final_eligibility_rate": rate_value,
        "mode_disabled_adjusted_final_eligibility_status": mode_adjusted.get("status"),
        "mode_disabled_adjusted_final_eligibility_target_met": bool(rate_value is not None and rate_value >= 0.6),
        "paper_trade_intent_count": safe_int(events.get("paper_trade_intent"), 0),
        "paper_committed_count": safe_int(events.get("paper_committed"), 0),
        "paper_intent_required_while_shadow": False,
        "paper_committed_required_while_shadow": False,
        "clean_window_counter": clean_counter or {},
        "circuit_recovery_sla": recovery_sla,
        "blocking_reasons": reasons,
        "human_action_required_before_enabling_live": True,
        "human_action_required_before_enabling": not paper_auto_resume_allowed,
        "promotion_allowed": False,
        "paper_enablement_allowed": paper_auto_resume_allowed,
        "live_enablement_allowed": False,
        "automatic_paper_resume_allowed": paper_auto_resume_allowed,
        "automatic_runtime_change_allowed": False,
    }


def classify(runtime, final_contract, failed_conditions, clean_counter=None, recovery_sla=None):
    state = runtime.get("mode_state") or {}
    hard_blockers = final_contract.get("hard_blockers") or {}
    mode_disabled_count = int(hard_blockers.get("mode_disabled") or 0)
    final_rows = int(final_contract.get("rows") or 0)
    cooldown_remaining = safe_float(state.get("cooldown_remaining_sec"), 0) or 0
    status = str(state.get("status") or state.get("action") or "LIVE").upper()
    recovery_required = truthy(state.get("recovery_required"))
    clean_windows_passed = not failed_conditions
    clean_window_streak_sufficient = bool(clean_windows_passed and (clean_counter or {}).get("sufficient"))
    recovery_sla_sufficient = recovery_sla_requirements_satisfied(recovery_sla, clean_counter, failed_conditions)
    motion = (recovery_sla or {}).get("motion_trace_review") or {}
    human = False
    if status == "CIRCUIT_BROKEN" or cooldown_remaining > 0:
        verdict = "FUNNEL_BLOCKED_EXPECTED"
        reason = "a_class_runtime_cooldown_active"
        current_capture_stage = "final_entry_cooldown"
    elif (
        motion.get("required")
        and not motion.get("available")
        and (mode_disabled_count or status == "SHADOW" or recovery_required)
    ):
        verdict = "FUNNEL_BLOCKED_EXPECTED"
        reason = "motion_trace_review_artifact_missing"
        current_capture_stage = "mode_disabled_motion_trace_review_pending"
    elif (mode_disabled_count or status == "SHADOW" or recovery_required) and not clean_window_streak_sufficient:
        verdict = "FUNNEL_BLOCKED_EXPECTED"
        reason = "clean_window_streak_below_required" if clean_windows_passed else "cooldown_elapsed_requires_clean_windows"
        current_capture_stage = "mode_disabled_clean_window_pending"
    elif (mode_disabled_count or status == "SHADOW" or recovery_required) and recovery_sla_sufficient:
        verdict = "FUNNEL_READY_FOR_PAPER_AUTO_RESUME"
        reason = "paper_auto_resume_ready_live_requires_human_operator"
        human = True
        current_capture_stage = "paper_auto_resume_ready_live_requires_human_review"
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
        "clean_window_streak_sufficient": clean_window_streak_sufficient,
        "recovery_sla_sufficient": recovery_sla_sufficient,
        "current_capture_stage": current_capture_stage,
    }


def build_stage2_flat_summary(runtime, classification, failed_conditions, capture_stage_rates, final_contract, clean_counter=None):
    state = runtime.get("mode_state") or {}
    events = capture_stage_rates.get("events") or {}
    hard_blockers = final_contract.get("hard_blockers") or {}
    spread_block_count = sum(
        safe_int(hard_blockers.get(key), 0)
        for key in ("spread_above_route_limit", "spread_extreme")
    )
    action = str(state.get("action") or state.get("status") or "").upper()
    status = str(state.get("status") or action or "").upper()
    shadow_entered_ts = None
    shadow_entered_ts_source = None
    if action == "SHADOW" or status == "SHADOW":
        shadow_entered_ts = state.get("last_breach_ts") or state.get("updated_at") or state.get("created_at")
        shadow_entered_ts_source = (
            "last_breach_ts"
            if state.get("last_breach_ts") is not None
            else ("updated_at" if state.get("updated_at") is not None else "created_at")
        )
    clean_windows_passed = classification.get("clean_windows_passed")
    recovery_sla = state.get("circuit_recovery_sla") or {}
    required_clean_window = {
        "condition": "context_coverage_clean_window",
        "clean_windows_required": state.get("clean_windows_required"),
        "breach_class": recovery_sla.get("breach_class"),
        "required_source": "circuit_recovery_sla",
        "passed": clean_windows_passed,
        "streak": (clean_counter or {}).get("streak"),
        "streak_sufficient": (clean_counter or {}).get("sufficient"),
        "counter_bucket_sec": (clean_counter or {}).get("counter_bucket_sec"),
    }
    return {
        "mode_status": status or None,
        "mode_action": action or None,
        "mode_reason": state.get("reason") or classification.get("reason"),
        "shadow_entered_ts": shadow_entered_ts,
        "shadow_entered_ts_source": shadow_entered_ts_source,
        "cooldown_elapsed": (safe_float(state.get("cooldown_remaining_sec"), 0) or 0) <= 0,
        "cooldown_remaining_sec": safe_float(state.get("cooldown_remaining_sec"), 0),
        "clean_window_required_conditions": [required_clean_window],
        "clean_window_passed_conditions": [required_clean_window] if clean_windows_passed else [],
        "clean_window_failed_conditions": failed_conditions,
        "clean_window_counter": clean_counter or {},
        "raw_gs_events": capture_stage_rates.get("denominator_raw_gold_silver_events"),
        "raw_gs_signal_ids": capture_stage_rates.get("denominator_raw_signal_ids"),
        "candidate_matched_any": events.get("candidate_matched_any"),
        "has_decision_record": events.get("decision_records"),
        "pass_allow": events.get("pass_or_allow"),
        "pending_entry": events.get("pending_entry"),
        "reached_final_entry_contract": events.get("final_entry_contract"),
        "final_entry_block_mode_disabled": events.get("mode_disabled_final_entry"),
        "final_entry_block_mode_disabled_only": events.get("mode_disabled_only_final_entry"),
        "final_entry_block_expected_rr": safe_int(hard_blockers.get("expected_rr_below_2"), 0),
        "final_entry_block_spread": spread_block_count,
        "paper_trade_intent": events.get("paper_trade_intent"),
        "paper_trade_committed": events.get("paper_committed"),
        "entered": events.get("entered"),
        "realized": events.get("realized"),
        "promotion_allowed": False,
        "paper_enablement_allowed": False,
        "automatic_paper_resume_allowed": False,
        "automatic_runtime_change_allowed": False,
        "strategy_change_allowed": False,
    }


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    raw_funnel = load_json(args.raw_funnel)
    context = load_json(args.context_coverage)
    volume_kline = load_json(args.volume_kline_audit)
    context_monitor = load_json(args.context_blocker_monitor)
    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    try:
        runtime = load_runtime_state(db, now_ts)
        runtime, recovery_sla = apply_recovery_sla_to_runtime(runtime, now_ts)
        final_contract = load_final_entry_contract_events(db, since_ts, now_ts)
        failed = context_failed_conditions(context, volume_kline, context_monitor)
        current_clean_passed = not failed
        clean_counter_persistence = persist_clean_window_counter(
            db,
            runtime.get("mode_state") or {},
            current_clean_passed,
            failed,
            now_ts,
        )
        runtime = load_runtime_state(db, now_ts)
        runtime, recovery_sla = apply_recovery_sla_to_runtime(runtime, now_ts)
    finally:
        db.close()
    clean_counter = clean_window_counter_from_detail((runtime.get("mode_state") or {}).get("detail") or {})
    if not clean_counter and clean_counter_persistence.get("counter"):
        clean_counter = clean_counter_persistence.get("counter") or {}
    quote_reconciliation = quote_window_pending_reconciliation(context_monitor)
    classification = classify(runtime, final_contract, failed, clean_counter, recovery_sla)
    raw_snapshot = raw_funnel_snapshot(raw_funnel)
    capture_stage_rates = build_capture_stage_rates(raw_snapshot, final_contract)
    readiness_shortfall = build_readiness_shortfall_summary(capture_stage_rates)
    paper_proposal_readiness = build_paper_entry_proposal_readiness(
        classification,
        capture_stage_rates,
        failed,
        clean_counter,
        recovery_sla,
    )
    paper_tracker_persistence = persist_paper_ready_tracker(
        args.db,
        paper_proposal_readiness.get("status"),
        now_ts,
    )
    tracker_state = dict(runtime.get("mode_state") or {})
    tracker_detail = dict(tracker_state.get("detail") or {})
    if paper_tracker_persistence.get("tracker"):
        tracker_detail["paper_entry_ready_tracker"] = paper_tracker_persistence.get("tracker")
        tracker_state["detail"] = tracker_detail
    handoff_escalation_sla = build_handoff_escalation_sla(
        tracker_state,
        paper_proposal_readiness.get("status"),
        now_ts,
    )
    paper_proposal_readiness["handoff_escalation_sla"] = handoff_escalation_sla
    paper_proposal_readiness["paper_ready_tracker_persistence"] = paper_tracker_persistence
    flat_summary = build_stage2_flat_summary(
        runtime,
        classification,
        failed,
        capture_stage_rates,
        final_contract,
        clean_counter,
    )
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
            "context_blocker_monitor": args.context_blocker_monitor,
        },
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "canary_increase_allowed": False,
        "paper_enablement_allowed": False,
        "automatic_paper_resume_allowed": paper_proposal_readiness.get("automatic_paper_resume_allowed"),
        "live_enablement_allowed": False,
        "mode_status": flat_summary["mode_status"],
        "mode_action": flat_summary["mode_action"],
        "mode_reason": flat_summary["mode_reason"],
        "shadow_entered_ts": flat_summary["shadow_entered_ts"],
        "shadow_entered_ts_source": flat_summary["shadow_entered_ts_source"],
        "cooldown_elapsed": flat_summary["cooldown_elapsed"],
        "cooldown_remaining_sec": flat_summary["cooldown_remaining_sec"],
        "clean_window_required_conditions": flat_summary["clean_window_required_conditions"],
        "clean_window_passed_conditions": flat_summary["clean_window_passed_conditions"],
        "clean_window_failed_conditions": flat_summary["clean_window_failed_conditions"],
        "clean_window_counter": flat_summary["clean_window_counter"],
        "clean_window_counter_persistence": clean_counter_persistence,
        "circuit_recovery_sla": recovery_sla,
        "breach_class": recovery_sla.get("breach_class"),
        "effective_clean_windows_required": recovery_sla.get("effective_clean_windows_required"),
        "paper_auto_resume_readiness": {
            "status": paper_proposal_readiness.get("paper_auto_resume_status"),
            "allowed": paper_proposal_readiness.get("paper_auto_resume_allowed"),
            "automatic_paper_resume_allowed": paper_proposal_readiness.get("automatic_paper_resume_allowed"),
            "paper_enablement_allowed": paper_proposal_readiness.get("paper_enablement_allowed"),
            "real_capital_risk": False,
            "live_enablement_allowed": False,
        },
        "live_reenable_contract": recovery_sla.get("live_reenable_contract") or {},
        "handoff_escalation_sla": handoff_escalation_sla,
        "paper_ready_tracker_persistence": paper_tracker_persistence,
        "raw_gs_events": flat_summary["raw_gs_events"],
        "raw_gs_signal_ids": flat_summary["raw_gs_signal_ids"],
        "candidate_matched_any": flat_summary["candidate_matched_any"],
        "has_decision_record": flat_summary["has_decision_record"],
        "pass_allow": flat_summary["pass_allow"],
        "pending_entry": flat_summary["pending_entry"],
        "reached_final_entry_contract": flat_summary["reached_final_entry_contract"],
        "final_entry_block_mode_disabled": flat_summary["final_entry_block_mode_disabled"],
        "final_entry_block_mode_disabled_only": flat_summary["final_entry_block_mode_disabled_only"],
        "final_entry_block_expected_rr": flat_summary["final_entry_block_expected_rr"],
        "final_entry_block_spread": flat_summary["final_entry_block_spread"],
        "paper_trade_intent": flat_summary["paper_trade_intent"],
        "paper_trade_committed": flat_summary["paper_trade_committed"],
        "entered": flat_summary["entered"],
        "realized": flat_summary["realized"],
        "stage2_flat_summary": flat_summary,
        "runtime_safety": runtime,
        "A_CLASS_mode_status": final_contract.get("mode_status") or {},
        "effective_runtime_mode_state": runtime.get("mode_state") or {},
        "final_entry_status": classification["final_entry_status"],
        "reason": classification["reason"],
        "human_action_required": classification["human_action_required"],
        "current_capture_stage": classification["current_capture_stage"],
        "clean_window_conditions": {
            "passed": classification["clean_windows_passed"],
            "streak_sufficient": classification.get("clean_window_streak_sufficient"),
            "recovery_sla_sufficient": classification.get("recovery_sla_sufficient"),
            "clean_window_counter": clean_counter,
            "clean_window_counter_persistence": clean_counter_persistence,
            "failed_conditions": failed,
            "reconciled_conditions": [
                {
                    "condition": "quote_clean_window_pending",
                    "source": "context_blocker_monitor",
                    **quote_reconciliation,
                }
            ] if quote_reconciliation.get("reconciled") else [],
            "context_coverage_loaded": bool(context),
            "volume_kline_audit_loaded": bool(volume_kline),
            "context_blocker_monitor_loaded": bool(context_monitor),
        },
        "clean_window_monitor": {
            "overall_verdict": context_monitor.get("overall_verdict") or {},
            "quote_clean_window": (
                context_monitor.get("task_b_rolling_24h_quote_clean_window")
                or context_monitor.get("task_b_clean_window_monitor")
                or context_monitor.get("task_b")
                or {}
            ),
            "quote_clean_window_reconciliation": quote_reconciliation,
            "context_field_coverage": (
                context_monitor.get("task_d_context_field_coverage_audit")
                or context_monitor.get("task_d")
                or {}
            ),
            "post_deploy_context_smoke": (
                context_monitor.get("task_e_context_field_post_deploy_smoke")
                or context_monitor.get("task_e_post_deploy_context_field_smoke_test")
                or context_monitor.get("task_e")
                or {}
            ),
        },
        "raw_funnel_snapshot": raw_snapshot,
        "capture_stage_rates": capture_stage_rates,
        "readiness_shortfall_summary": readiness_shortfall,
        "paper_entry_proposal_readiness": paper_proposal_readiness,
        "upstream_funnel_gap": capture_stage_rates["upstream_funnel_gap"],
        "pending_to_final_entry_gap": capture_stage_rates["pending_to_final_entry_gap"],
        "mode_disabled_adjusted_final_eligibility": capture_stage_rates["mode_disabled_adjusted_final_eligibility"],
        "raw_gold_silver_entered_events": raw_snapshot.get("raw_gold_silver_entered_events"),
        "decision_layer": (raw_funnel.get("summary") or {}).get("decision_layer") or {},
        "entry_bridge_layer_summary": {
            key: raw_snapshot.get(key)
            for key in (
                "paper_trades_entry_ts_window_count",
                "raw_signals_with_decision_record",
                "raw_signals_without_decision_record",
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
            "paper_auto_resume_after_sla_allowed": paper_proposal_readiness.get("paper_auto_resume_allowed"),
            "must_not_enable_live_automatically": True,
            "must_not_change_final_entry_contract_automatically": True,
        },
    }


def compact_summary(report):
    return {
        "final_entry_status": report.get("final_entry_status"),
        "reason": report.get("reason"),
        "current_capture_stage": report.get("current_capture_stage"),
        "mode_status": report.get("mode_status"),
        "mode_reason": report.get("mode_reason"),
        "raw_gs_events": report.get("raw_gs_events"),
        "candidate_matched_any": report.get("candidate_matched_any"),
        "has_decision_record": report.get("has_decision_record"),
        "pass_allow": report.get("pass_allow"),
        "pending_entry": report.get("pending_entry"),
        "reached_final_entry_contract": report.get("reached_final_entry_contract"),
        "final_entry_block_mode_disabled": report.get("final_entry_block_mode_disabled"),
        "paper_trade_intent": report.get("paper_trade_intent"),
        "paper_trade_committed": report.get("paper_trade_committed"),
        "human_action_required": report.get("human_action_required"),
        "promotion_allowed": False,
        "clean_windows_passed": (report.get("clean_window_conditions") or {}).get("passed"),
        "clean_window_streak_sufficient": (report.get("clean_window_conditions") or {}).get("streak_sufficient"),
        "clean_window_counter": report.get("clean_window_counter"),
        "breach_class": report.get("breach_class"),
        "effective_clean_windows_required": report.get("effective_clean_windows_required"),
        "paper_auto_resume_readiness": report.get("paper_auto_resume_readiness"),
        "handoff_escalation_sla": report.get("handoff_escalation_sla"),
        "failed_clean_window_conditions": (report.get("clean_window_conditions") or {}).get("failed_conditions"),
        "effective_runtime_mode_state": report.get("effective_runtime_mode_state"),
        "final_entry_contract_blocker_breakdown": report.get("final_entry_contract_blocker_breakdown"),
        "raw_funnel_snapshot": report.get("raw_funnel_snapshot"),
        "capture_stage_rates": report.get("capture_stage_rates"),
        "readiness_shortfall_summary": report.get("readiness_shortfall_summary"),
        "paper_entry_proposal_readiness": report.get("paper_entry_proposal_readiness"),
        "upstream_funnel_gap": report.get("upstream_funnel_gap"),
        "pending_to_final_entry_gap": report.get("pending_to_final_entry_gap"),
        "mode_disabled_adjusted_final_eligibility": report.get("mode_disabled_adjusted_final_eligibility"),
    }


def self_test():
    now = 2_000_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        db_path = root / "paper.db"
        raw_path = root / "raw_funnel.json"
        context_path = root / "context.json"
        context_monitor_path = root / "context_monitor.json"
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
        payload_mode_only = {
            "hard_blockers": ["mode_disabled"],
            "mode_state": {"status": "SHADOW", "action": "SHADOW", "reason": "cooldown_elapsed_requires_clean_windows"},
            "normalized_mode": MODE_KEY,
            "expected_rr": 2.4,
            "spread_pct": 3,
        }
        db.execute(
            "INSERT INTO paper_decision_events VALUES (?,?,?,?,?,?,?,?,?,?)",
            (now - 30, "2", "TOK2", "TOK2", "lc2", "final_entry_contract", "entry_block", "BLOCK", "final_entry_hard_block", json.dumps(payload_mode_only)),
        )
        db.commit()
        db.close()
        write_json(raw_path, {
            "summary": {
                "raw_denominator": {"raw_all_gold_silver_event_rows": 4, "entered_events": 0},
                "candidate_layer": {"candidate_matched_any_events": 4, "candidate_match_any_rate": 1.0},
                "decision_layer": {
                    "events_with_decision_record": 3,
                    "decision_record_rate": 0.75,
                    "would_enter_events": 2,
                    "would_enter_rate": 0.5,
                    "entered_events": 0,
                    "entered_rate": 0.0,
                    "realized_events": 0,
                    "realized_rate": 0.0,
                },
                "entry_bridge_layer": {
                    "paper_trades_entry_ts_window_count": 0,
                    "paper_evidence_log": {
                        "event_type_counts": {
                            "paper_trade_entry_intent": 0,
                        }
                    },
                    "raw_signal_decision_bridge": {
                        "raw_signal_ids": 4,
                        "raw_signals_with_decision_record": 3,
                        "raw_signals_without_decision_record": 1,
                        "no_decision_record_root_cause_counts": [
                            {
                                "root_cause": "candidate_shadow_observed_no_decision_event",
                                "description": "Candidate shadow observations exist with full candidate coverage, but no decision event was written.",
                                "count": 1,
                            }
                        ],
                        "no_decision_record_subroot_cause_counts": [
                            {
                                "root_cause": "shadow_entry_hypotheses_matched_no_decision_bridge",
                                "description": "Full candidate mesh observed the signal and one or more shadow entry hypotheses matched, but no decision event was written.",
                                "count": 1,
                            }
                        ],
                        "shadow_no_decision_entry_hypothesis_family_counts": [
                            {"family": "base", "count": 1}
                        ],
                        "shadow_no_decision_entry_hypothesis_candidate_counts": [
                            {"candidate_id": "notath_quote_clean", "family": "base", "count": 1}
                        ],
                        "shadow_no_decision_entry_hypothesis_reason_counts": [
                            {
                                "candidate_id": "notath_quote_clean",
                                "family": "base",
                                "reason": "runtime_source_quote_clean",
                                "count": 1,
                            }
                        ],
                        "no_decision_candidate_shadow_observed_no_decision_event": 1,
                        "raw_signals_with_pass_or_allow": 2,
                        "raw_signals_with_pending_entry": 2,
                        "raw_signals_with_final_entry_contract": 2,
                        "raw_signals_with_final_entry_block": 2,
                        "raw_signals_with_final_entry_mode_disabled": 2,
                        "raw_signals_with_final_entry_mode_disabled_only": 1,
                        "raw_signals_with_final_entry_mode_disabled_plus_other": 1,
                        "raw_signals_with_decision_no_pass_or_allow": 1,
                        "decision_no_pass_or_allow_reason_counts": [
                            {
                                "component": "smart_entry",
                                "event_type": "timing_decision",
                                "decision": "WATCH_ONLY",
                                "reason": "timing_not_ready",
                                "count": 1,
                            }
                        ],
                        "raw_signals_pass_or_allow_without_pending_entry": 0,
                        "pass_or_allow_without_pending_entry_reason_counts": [],
                        "raw_scoped_final_entry_hard_blockers": {
                            "mode_disabled": 2,
                            "expected_rr_below_2": 1,
                        },
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
            context_blocker_monitor=None,
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
        assert report["mode_status"] == "SHADOW"
        assert report["mode_action"] == "SHADOW"
        assert report["mode_reason"] == "cooldown_elapsed_requires_clean_windows"
        assert report["shadow_entered_ts"] == now - 90000
        assert report["cooldown_elapsed"] is True
        assert report["clean_window_required_conditions"][0]["condition"] == "context_coverage_clean_window"
        assert report["clean_window_passed_conditions"] == []
        assert report["clean_window_failed_conditions"][0]["condition"] == "source_quote_clean_coverage_below_80pct"
        assert report["raw_gs_events"] == 4
        assert report["candidate_matched_any"] == 4
        assert report["has_decision_record"] == 3
        assert report["pass_allow"] == 2
        assert report["pending_entry"] == 2
        assert report["reached_final_entry_contract"] == 2
        assert report["final_entry_block_mode_disabled"] == 2
        assert report["final_entry_block_mode_disabled_only"] == 1
        assert report["final_entry_block_expected_rr"] == 1
        assert report["final_entry_block_spread"] == 0
        assert report["paper_trade_intent"] == 0
        assert report["paper_trade_committed"] == 0
        assert report["entered"] == 0
        assert report["realized"] == 0
        assert report["stage2_flat_summary"]["paper_enablement_allowed"] is False
        assert report["final_entry_contract"]["mode_disabled_rows"] == 2
        assert report["final_entry_contract"]["mode_disabled_only_rows"] == 1
        assert report["final_entry_contract"]["mode_disabled_plus_other_rows"] == 1
        assert report["capture_stage_rates"]["detector_capture_rate"] == 1.0
        assert report["capture_stage_rates"]["decision_record_capture_rate"] == 0.75
        assert report["capture_stage_rates"]["pending_capture_rate"] == 0.5
        assert report["capture_stage_rates"]["paper_trade_intent_rate"] == 0.0
        assert report["capture_stage_rates"]["events"]["paper_trade_intent"] == 0
        assert report["capture_stage_rates"]["events"]["paper_committed"] == 0
        assert report["capture_stage_rates"]["realized_capture_rate"] == 0.0
        assert report["pending_to_final_entry_gap"]["pending_to_final_entry_contract_rate"] == 1.0
        assert report["upstream_funnel_gap"]["no_decision_record"] == 1
        assert report["upstream_funnel_gap"]["no_decision_candidate_shadow_observed_no_decision_event"] == 1
        assert report["upstream_funnel_gap"]["no_decision_record_subroot_cause_counts"][0]["root_cause"] == "shadow_entry_hypotheses_matched_no_decision_bridge"
        assert report["upstream_funnel_gap"]["shadow_no_decision_entry_hypothesis_candidate_counts"][0]["candidate_id"] == "notath_quote_clean"
        assert report["upstream_funnel_gap"]["upstream_gap_category_counts"]["categories"][0]["top_reasons"]
        assert report["upstream_funnel_gap"]["decision_no_pass_or_allow"] == 1
        assert report["upstream_funnel_gap"]["total_upstream_gap"] == 2
        assert report["upstream_funnel_gap"]["upstream_gap_priority"]["current_shortfall_to_60_pending"] == 1
        assert report["mode_disabled_adjusted_final_eligibility"]["mode_disabled_only_unique_signal_ids"] == 1
        assert report["mode_disabled_adjusted_final_eligibility"]["rate"] == 0.25
        assert report["mode_disabled_adjusted_final_eligibility"]["status"] == "CAPTURE_READINESS_BELOW_60"
        assert report["readiness_shortfall_summary"]["target_count_60pct"] == 3
        assert report["readiness_shortfall_summary"]["current_mode_disabled_adjusted_final_eligibility_count"] == 1
        assert report["readiness_shortfall_summary"]["shortfall_to_60_final_eligibility"] == 2
        assert report["readiness_shortfall_summary"]["shortfall_to_60_pending"] == 1
        assert report["paper_entry_proposal_readiness"]["status"] == "NOT_READY_FOR_PAPER_ENTRY_PROPOSAL"
        assert "clean_window_conditions_not_passed" in report["paper_entry_proposal_readiness"]["blocking_reasons"]
        assert "paper_trade_entry_intent_zero" not in report["paper_entry_proposal_readiness"]["blocking_reasons"]
        assert "paper_trade_committed_zero" not in report["paper_entry_proposal_readiness"]["blocking_reasons"]
        assert report["clean_window_counter"]["streak"] == 0
        assert report["breach_class"] == "MARKET"
        assert report["effective_clean_windows_required"] == 24
        assert report["circuit_recovery_sla"]["motion_trace_review"]["required"] is True
        assert report["circuit_recovery_sla"]["motion_trace_review"]["available"] is True
        assert report["clean_window_counter_persistence"]["changes_runtime_mode"] is False
        write_json(context_monitor_path, {
            "overall_verdict": {
                "rolling24_quote_status": "QUOTE_CLEAN_WINDOW_PENDING",
                "context_field_writer_fix": "VERIFIED_POST_DEPLOY",
            },
            "task_b_rolling_24h_quote_clean_window": {
                "estimated_clean_at_iso": "2030-01-01T00:00:00Z",
                "pre_fix_rows_remaining": 10,
            },
            "task_d_context_field_coverage_audit": {
                "blockers": [
                    "source_component_coverage_below_80pct",
                    "lifecycle_profile_coverage_below_80pct",
                ]
            },
        })
        args.context_blocker_monitor = str(context_monitor_path)
        report = build_report(args)
        failed_conditions = {row["condition"] for row in report["clean_window_failed_conditions"]}
        assert "quote_clean_window_pending" in failed_conditions
        assert "source_component_clean_window_pending" in failed_conditions
        assert "lifecycle_profile_clean_window_pending" in failed_conditions
        assert "source_quote_clean_coverage_below_80pct" not in failed_conditions
        assert report["clean_window_conditions"]["context_blocker_monitor_loaded"] is True
        assert report["clean_window_monitor"]["overall_verdict"]["rolling24_quote_status"] == "QUOTE_CLEAN_WINDOW_PENDING"
        write_json(context_monitor_path, {
            "overall_verdict": {
                "rolling24_quote_status": "QUOTE_CLEAN_WINDOW_PENDING",
                "context_field_writer_fix": "VERIFIED_POST_DEPLOY",
                "quote_writer_fix": "VERIFIED_POST_DEPLOY",
            },
            "task_b": {
                "estimated_clean_at_iso": "2030-01-01T00:00:00Z",
                "pre_fix_rows_remaining": 10,
                "post_fix_rows": 10,
                "quote_coverage_rolling24": {
                    "rows_scanned": 100,
                    "source_quote_clean_present_rate": 1.0,
                    "source_quote_executable_present_rate": 1.0,
                    "source_quote_clean_missing_rows": 0,
                    "source_quote_executable_missing_rows": 0,
                },
            },
            "task_d_context_field_coverage_audit": {
                "blockers": [
                    "source_component_coverage_below_80pct",
                ]
            },
        })
        report = build_report(args)
        failed_conditions = {row["condition"] for row in report["clean_window_failed_conditions"]}
        reconciled_conditions = {
            row["condition"] for row in report["clean_window_conditions"]["reconciled_conditions"]
        }
        assert "quote_clean_window_pending" not in failed_conditions
        assert "quote_clean_window_pending" in reconciled_conditions
        assert "source_component_clean_window_pending" in failed_conditions
        assert report["clean_window_monitor"]["quote_clean_window_reconciliation"]["reconciled"] is True
        write_json(context_monitor_path, {
            "overall_verdict": {
                "rolling24_quote_status": "QUOTE_CLEAN_WINDOW_PENDING",
                "context_field_writer_fix": "VERIFIED_POST_DEPLOY",
                "quote_writer_fix": "VERIFIED_POST_DEPLOY",
            },
            "task_b_clean_window_monitor": {
                "estimated_clean_at_iso": "2030-01-01T00:00:00Z",
                "pre_fix_rows_remaining": 10,
                "post_fix_rows": 10,
                "quote_coverage_rolling24": {
                    "rows_scanned": 100,
                    "source_quote_clean_present_rate": 1.0,
                    "source_quote_executable_present_rate": 1.0,
                    "source_quote_clean_missing_rows": 0,
                    "source_quote_executable_missing_rows": 0,
                },
            },
            "task_d_context_field_coverage_audit": {
                "blockers": [
                    "lifecycle_profile_coverage_below_80pct",
                ]
            },
        })
        report = build_report(args)
        failed_conditions = {row["condition"] for row in report["clean_window_failed_conditions"]}
        assert "quote_clean_window_pending" not in failed_conditions
        assert "lifecycle_profile_clean_window_pending" in failed_conditions
        assert report["clean_window_monitor"]["quote_clean_window_reconciliation"]["rows_scanned"] == 100
        args.context_blocker_monitor = None
        write_json(context_path, {"blockers": []})
        bucket = window_bucket(now)
        previous_counter = {
            "schema_version": CLEAN_WINDOW_COUNTER_SCHEMA_VERSION,
            "mode_key": MODE_KEY,
            "counter_bucket_sec": bucket["bucket_sec"],
            "last_window_key": f"{bucket['bucket_sec']}s:{bucket['previous_bucket']}",
            "last_window_bucket": bucket["previous_bucket"],
            "last_passed": True,
            "streak": 5,
            "required": 6,
            "sufficient": False,
            "failed_condition_codes": [],
            "updated_at": now - bucket["bucket_sec"],
            "promotion_allowed": False,
            "automatic_runtime_change_allowed": False,
        }
        db = sqlite3.connect(db_path)
        db.execute(
            "UPDATE a_class_mode_runtime_state SET detail_json=? WHERE mode_key=?",
            (json.dumps({"breach_class": "DATA_INFRA", "clean_window_counter": previous_counter}, sort_keys=True), MODE_KEY),
        )
        db.commit()
        db.close()
        report = build_report(args)
        assert report["final_entry_status"] == "FUNNEL_READY_FOR_PAPER_AUTO_RESUME"
        assert report["human_action_required"] is True
        assert report["clean_window_conditions"]["passed"] is True
        assert report["clean_window_conditions"]["streak_sufficient"] is True
        assert report["clean_window_conditions"]["recovery_sla_sufficient"] is True
        assert report["breach_class"] == "DATA_INFRA"
        assert report["effective_clean_windows_required"] == 6
        assert report["clean_window_counter"]["streak"] == 6
        assert report["paper_entry_proposal_readiness"]["status"] == "PAPER_AUTO_RESUME_READY_LIVE_REENABLE_REQUIRES_HUMAN"
        assert report["paper_entry_proposal_readiness"]["paper_auto_resume_allowed"] is True
        assert report["paper_entry_proposal_readiness"]["paper_enablement_allowed"] is True
        assert report["paper_entry_proposal_readiness"]["live_enablement_allowed"] is False
        assert report["paper_entry_proposal_readiness"]["paper_intent_required_while_shadow"] is False
        assert report["paper_entry_proposal_readiness"]["mode_disabled_adjusted_final_eligibility_target_met"] is False
        assert report["paper_auto_resume_readiness"]["allowed"] is True
        assert report["live_reenable_contract"]["live_auto_reenable_allowed"] is False
        previous_counter["streak"] = 23
        previous_counter["required"] = 24
        db = sqlite3.connect(db_path)
        db.execute(
            "UPDATE a_class_mode_runtime_state SET detail_json=? WHERE mode_key=?",
            (
                json.dumps(
                    {
                        "breach_class": "MARKET",
                        "clean_window_counter": previous_counter,
                        "paper_entry_ready_tracker": {
                            "schema_version": "a_class_paper_ready_tracker.v1",
                            "ready_since_ts": now - HUMAN_ESCALATION_SEC - 60,
                            "last_seen_ts": now - 60,
                            "last_status": "PAPER_AUTO_RESUME_READY_LIVE_REENABLE_REQUIRES_HUMAN",
                        },
                    },
                    sort_keys=True,
                ),
                MODE_KEY,
            ),
        )
        db.commit()
        db.close()
        report = build_report(args)
        assert report["breach_class"] == "MARKET"
        assert report["effective_clean_windows_required"] == 24
        assert report["clean_window_counter"]["streak"] == 24
        assert report["final_entry_status"] == "FUNNEL_READY_FOR_PAPER_AUTO_RESUME"
        assert report["paper_entry_proposal_readiness"]["paper_auto_resume_allowed"] is True
        assert report["circuit_recovery_sla"]["motion_trace_review"]["required"] is True
        assert report["circuit_recovery_sla"]["motion_trace_review"]["available"] is True
        assert report["handoff_escalation_sla"]["high_priority"] is True
        assert report["handoff_escalation_sla"]["daily_reminder_due"] is True
    print("SELF_TEST_PASS a_class_fastlane_mode_readiness_audit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-funnel", default=None)
    parser.add_argument("--context-coverage", default=None)
    parser.add_argument("--volume-kline-audit", default=None)
    parser.add_argument("--context-blocker-monitor", default=None)
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
