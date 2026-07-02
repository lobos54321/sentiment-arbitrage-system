#!/usr/bin/env python3
"""Read-only post-freeze OOS validation for final-eligibility 60% definitions.

This report validates frozen same-window final-eligibility closure definitions
against raw gold/silver rows that arrived after the freeze timestamp. It is
discovery/readiness evidence only: it never changes strategy, entry policy,
gates, final_entry_contract, A_CLASS, executor, wallet, canary, or risk
settings.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

from capture_cross_post_freeze_oos_validation import (
    SUPPORTED_STAGES,
    build_observation_indexes,
    build_oos_data_availability,
    context_value,
    iso_from_ts,
    load_matured_volume_contexts,
    norm_value,
    quality_timing_value,
    signal_stage_rates,
)
from offline_raw_gold_silver_funnel_audit import (
    attach_records,
    decision_would_enter,
    load_candidate_observations,
    load_paper_decisions,
    load_paper_trades,
    load_raw_dogs,
    make_raw_indexes,
    normalize_ts,
    rate,
    signal_id_key,
)
from pass_allow_60_post_freeze_oos_validation import (
    build_post_freeze_source_activity,
    load_json,
    parse_utc_ts,
    table_columns,
    table_exists,
    write_json,
)
from quality_timing_reject_research_audit import stage_quality_timing_events


SCHEMA_VERSION = "final_eligibility_60_post_freeze_oos_validation.v1"
DEFAULT_EXPECTED_CANDIDATES = 84
DEFAULT_MIN_RAW_EVENTS = 10
DEFAULT_MIN_SELECTED_EVENTS = 3
DEFAULT_SAFETY_SEC = 120

FINAL_POSITIVE_STAGES = {
    "final_eligibility",
    "mode_disabled_adjusted_final_eligibility",
    "paper_capture",
    "realized_capture",
}

BLOCKER_SOURCES = {
    "pending_to_final_entry_audit",
    "pending_to_final_entry_audit.dropoff_counts",
}


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def truthy(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "enter", "would_enter"}


def jloads(value):
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(value)
        return decoded if isinstance(decoded, dict) else {}
    except Exception:
        return {}


def optional(cols, name, default="NULL"):
    return name if name in cols else f"{default} AS {name}"


def extract_hard_blockers(payload):
    payload = payload if isinstance(payload, dict) else {}
    blockers = payload.get("hard_blockers")
    if blockers is None and isinstance(payload.get("final_entry_contract"), dict):
        blockers = payload.get("final_entry_contract", {}).get("hard_blockers")
    if blockers is None:
        blockers = payload.get("hard_blockers_json")
    if isinstance(blockers, str):
        try:
            decoded = json.loads(blockers)
            blockers = decoded
        except Exception:
            blockers = [blockers]
    if isinstance(blockers, (list, tuple, set)):
        return [str(item) for item in blockers if item is not None and str(item)]
    return []


def blocker_category_from_text(text, default="missing_final_contract_record"):
    blob = str(text or "").lower()
    if "stale" in blob:
        return "stale_before_final"
    if "quote" in blob:
        return "quote_missing"
    if "route" in blob:
        return "route_missing"
    if "spread" in blob:
        return "spread_above_route_limit"
    if "expected_rr" in blob or "rr" in blob:
        return "expected_rr_below_policy"
    if "hour" in blob or "cap" in blob or "rate_limit" in blob:
        return "hourly_cap_block"
    if "lifecycle" in blob or "cancel" in blob:
        return "lifecycle_cancelled"
    if "duplicate" in blob or "existing" in blob:
        return "duplicate_or_existing_position"
    if "expired" in blob or "timeout" in blob:
        return "pending_expired"
    if "shadow" in blob or "mode_disabled" in blob or "mode" in blob:
        return "mode_shadow_preblocked"
    if blob:
        return default
    return "unknown" if default == "unknown" else default


def category_from_decision(decision, fallback="missing_final_contract_record"):
    if not decision:
        return fallback
    payload = jloads(decision.get("payload_json"))
    if decision.get("hard_blockers_json"):
        payload["hard_blockers_json"] = decision.get("hard_blockers_json")
    blockers = extract_hard_blockers(payload)
    if blockers:
        non_mode = [item for item in blockers if item != "mode_disabled"]
        if not non_mode and "mode_disabled" in blockers:
            return "mode_shadow_preblocked"
        return blocker_category_from_text(" ".join(non_mode or blockers), default=fallback)
    text = " ".join(
        str(decision.get(key) or "")
        for key in (
            "reason",
            "block_cause",
            "quote_failure_reason",
            "route_failure_reason",
            "source_component",
            "event_type",
            "decision",
            "action",
            "would_action",
        )
    )
    return blocker_category_from_text(text, default=fallback)


def is_final_entry_decision(decision):
    component = str(decision.get("source_component") or decision.get("component") or "").lower()
    event_type = str(decision.get("event_type") or "").lower()
    reason = str(decision.get("reason") or "").lower()
    return (
        component == "final_entry_contract"
        or "final_entry_contract" in component
        or "final_entry_contract" in event_type
        or "final_entry_contract" in reason
    )


def build_decisions_by_signal(raw_rows, decisions):
    by_token = defaultdict(list)
    by_signal = defaultdict(list)
    for row in decisions or []:
        if row.get("token_ca"):
            by_token[str(row.get("token_ca"))].append(row)
        if row.get("signal_id_key"):
            by_signal[row.get("signal_id_key")].append(row)
    out = {}
    for raw in raw_rows:
        signal_id = raw.get("signal_id_key")
        if not signal_id:
            continue
        token = str(raw.get("token_ca") or "")
        signal_ts = raw.get("signal_ts_norm") or 0
        peak_sec = raw.get("time_to_sustained_peak_sec") or 900
        try:
            peak_sec = float(peak_sec)
        except Exception:
            peak_sec = 900
        start = signal_ts - 60
        end = signal_ts + max(60.0, min(900.0, peak_sec))
        rows = []
        seen = set()
        for decision in by_signal.get(signal_id, []):
            key = (decision.get("source_kind"), decision.get("id"), decision.get("event_ts_norm"))
            seen.add(key)
            rows.append(decision)
        for decision in by_token.get(token, []):
            ts = decision.get("event_ts_norm")
            if ts is None or ts < start or ts > end:
                continue
            key = (decision.get("source_kind"), decision.get("id"), decision.get("event_ts_norm"))
            if key in seen:
                continue
            seen.add(key)
            rows.append(decision)
        rows.sort(key=lambda item: (item.get("event_ts_norm") or 0, item.get("source_kind") or ""))
        out[signal_id] = rows
    return out


def load_final_entry_contract_events(paper_db, raw_rows, eval_start_ts, now_ts):
    signal_ids = {row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}
    tokens = {str(row.get("token_ca")) for row in raw_rows if row.get("token_ca")}
    rows = []
    if table_exists(paper_db, "paper_decision_events"):
        cols = table_columns(paper_db, "paper_decision_events")
        if "component" in cols:
            rows.extend(
                dict(row)
                for row in paper_db.execute(
                    """
                    SELECT event_ts, signal_id, token_ca, component AS source_component,
                           event_type, decision, reason, payload_json,
                           NULL AS action, NULL AS would_action, NULL AS block_cause,
                           NULL AS hard_blockers_json, NULL AS quote_failure_reason,
                           NULL AS route_failure_reason, 'paper_decision_events' AS source_kind
                    FROM paper_decision_events
                    WHERE event_ts >= ? AND event_ts <= ?
                      AND LOWER(COALESCE(component, '')) = 'final_entry_contract'
                    """,
                    (eval_start_ts - 60, now_ts + 900),
                ).fetchall()
            )
    if table_exists(paper_db, "a_class_decision_events"):
        cols = table_columns(paper_db, "a_class_decision_events")
        if "source_component" in cols:
            rows.extend(
                dict(row)
                for row in paper_db.execute(
                    f"""
                    SELECT event_ts, NULL AS signal_id, token_ca,
                           source_component, NULL AS event_type, NULL AS decision,
                           {optional(cols, 'reason')},
                           NULL AS payload_json,
                           {optional(cols, 'action')},
                           {optional(cols, 'would_action')},
                           {optional(cols, 'block_cause')},
                           {optional(cols, 'hard_blockers_json', "'[]'")},
                           {optional(cols, 'quote_failure_reason')},
                           {optional(cols, 'route_failure_reason')},
                           'a_class_decision_events' AS source_kind
                    FROM a_class_decision_events
                    WHERE event_ts >= ? AND event_ts <= ?
                      AND LOWER(COALESCE(source_component, '')) = 'final_entry_contract'
                    """,
                    (eval_start_ts - 60, now_ts + 900),
                ).fetchall()
            )
    out = []
    for row in rows:
        row = dict(row)
        row["signal_id_key"] = signal_id_key(row.get("signal_id"))
        row["event_ts_norm"] = normalize_ts(row.get("event_ts"))
        if row.get("signal_id_key") and row.get("signal_id_key") not in signal_ids:
            continue
        if not row.get("signal_id_key") and str(row.get("token_ca") or "") not in tokens:
            continue
        out.append(row)
    return out


def build_final_events_by_signal(raw_rows, final_events):
    by_signal = defaultdict(list)
    by_token = defaultdict(list)
    for row in final_events or []:
        if row.get("signal_id_key"):
            by_signal[row.get("signal_id_key")].append(row)
        if row.get("token_ca"):
            by_token[str(row.get("token_ca"))].append(row)
    out = {}
    for raw in raw_rows:
        signal_id = raw.get("signal_id_key")
        if not signal_id:
            continue
        token = str(raw.get("token_ca") or "")
        signal_ts = raw.get("signal_ts_norm") or 0
        peak_sec = raw.get("time_to_sustained_peak_sec") or 900
        try:
            peak_sec = float(peak_sec)
        except Exception:
            peak_sec = 900
        start = signal_ts - 60
        end = signal_ts + max(60.0, min(900.0, peak_sec))
        rows = []
        seen = set()
        for event in by_signal.get(signal_id, []):
            key = (event.get("source_kind"), event.get("event_ts_norm"), event.get("reason"))
            seen.add(key)
            rows.append(event)
        for event in by_token.get(token, []):
            ts = event.get("event_ts_norm")
            if ts is None or ts < start or ts > end:
                continue
            key = (event.get("source_kind"), event.get("event_ts_norm"), event.get("reason"))
            if key in seen:
                continue
            seen.add(key)
            rows.append(event)
        rows.sort(key=lambda item: item.get("event_ts_norm") or 0)
        out[signal_id] = rows
    return out


def build_signal_state(raw_rows, audits, decisions_by_signal, final_events_by_signal, trades):
    audit_by_signal = {row.get("signal_id"): row for row in audits if row.get("signal_id")}
    trade_signals = {row.get("signal_id_key") for row in trades or [] if row.get("signal_id_key")}
    states = {}
    stage_sets = {stage: set() for stage in SUPPORTED_STAGES}
    category_counts = Counter()
    transition_counts = Counter()
    for raw in raw_rows:
        signal_id = raw.get("signal_id_key")
        if not signal_id:
            continue
        audit = audit_by_signal.get(signal_id) or {}
        decisions = decisions_by_signal.get(signal_id) or []
        final_events = final_events_by_signal.get(signal_id) or []
        matched = int(audit.get("matched_candidate_count") or 0) > 0
        decision_count = int(audit.get("decision_record_count") or 0)
        would_enter = int(audit.get("would_enter_count") or 0) > 0 or any(
            decision_would_enter(row) for row in decisions
        )
        pending = any(str(row.get("event_type") or "").lower() == "pending_entry" for row in decisions)
        final_entry = bool(final_events)
        paper = bool(audit.get("entered") or int(audit.get("paper_trade_count") or 0) > 0 or signal_id in trade_signals)
        realized = bool(audit.get("raw_dog_realized"))
        if matched:
            stage_sets["detector_capture"].add(signal_id)
        if decision_count > 0 or decisions:
            stage_sets["decision_capture"].add(signal_id)
        if would_enter:
            stage_sets["pass_allow_capture"].add(signal_id)
        if pending:
            stage_sets["pending_capture"].add(signal_id)
        if final_entry:
            stage_sets["final_eligibility"].add(signal_id)
            non_mode_blocked = False
            for event in final_events:
                blockers = extract_hard_blockers(jloads(event.get("payload_json")))
                if event.get("hard_blockers_json"):
                    blockers.extend(extract_hard_blockers({"hard_blockers_json": event.get("hard_blockers_json")}))
                non_mode_blocked = non_mode_blocked or any(blocker != "mode_disabled" for blocker in blockers)
            if not non_mode_blocked:
                stage_sets["mode_disabled_adjusted_final_eligibility"].add(signal_id)
        if paper:
            stage_sets["paper_capture"].add(signal_id)
        if realized:
            stage_sets["realized_capture"].add(signal_id)

        if not matched:
            transition = "no_candidate_match"
            category = "no_candidate_match"
        elif not (decision_count > 0 or decisions):
            transition = "no_decision"
            category = "no_decision"
        elif not would_enter:
            transition = "decision_no_pass_allow"
            category = category_from_decision(decisions[-1] if decisions else None, "unknown")
        elif not pending:
            transition = "pass_allow_no_pending"
            category = category_from_decision(decisions[-1] if decisions else None, "unknown")
        elif not final_entry:
            transition = "pending_no_final_entry"
            category = category_from_decision(decisions[-1] if decisions else None, "missing_final_contract_record")
        elif not paper:
            transition = "final_entry_no_paper"
            category = category_from_decision(final_events[-1] if final_events else None, "mode_shadow_preblocked")
        elif not realized:
            transition = "paper_no_realized"
            category = "entered_not_realized"
        else:
            transition = "realized_gold_silver"
            category = "realized_gold_silver"
        transition_counts[transition] += 1
        category_counts[category] += 1
        states[signal_id] = {
            "signal_id": signal_id,
            "matched": matched,
            "decision": decision_count > 0 or bool(decisions),
            "pass_allow": would_enter,
            "pending": pending,
            "final_eligibility": final_entry,
            "mode_disabled_adjusted_final_eligibility": signal_id in stage_sets["mode_disabled_adjusted_final_eligibility"],
            "paper": paper,
            "realized": realized,
            "transition": transition,
            "final_blocker_category": category,
            "final_entry_event_count": len(final_events),
            "decision_record_count": decision_count or len(decisions),
        }
    return states, stage_sets, dict(transition_counts), dict(category_counts)


def selected_status(selected_count, selected_stage_rate, global_stage_rate, min_selected_events, role):
    lift = (
        None
        if selected_stage_rate is None or global_stage_rate is None
        else round(float(selected_stage_rate) - float(global_stage_rate), 6)
    )
    if selected_count < min_selected_events:
        verdict = "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_TOO_SMALL"
    elif role == "blocker_repeat":
        verdict = "FINAL_ELIGIBILITY_60_POST_FREEZE_REPEATED_BLOCKER_WATCH"
    elif lift is not None and lift > 0:
        verdict = "FINAL_ELIGIBILITY_60_POST_FREEZE_REPEAT_WATCH"
    else:
        verdict = "FINAL_ELIGIBILITY_60_POST_FREEZE_NO_REPEAT"
    return lift, verdict


def row_matches_definition(
    signal_id,
    definition,
    matched_candidates,
    context_by_signal,
    matured_volume_by_signal,
    quality_timing_by_signal,
    signal_states,
):
    candidate_id = definition.get("candidate_id")
    if candidate_id and candidate_id not in matched_candidates.get(signal_id, set()):
        return False
    transition = definition.get("transition")
    if transition and (signal_states.get(signal_id) or {}).get("transition") != transition:
        return False
    category = definition.get("category")
    if category and (signal_states.get(signal_id) or {}).get("final_blocker_category") != category:
        return False
    dimension = definition.get("dimension")
    if dimension:
        if dimension in {"quality_timing_cluster", "quality_timing_reason"}:
            observed = quality_timing_value((quality_timing_by_signal or {}).get(signal_id), dimension)
        elif dimension == "matured_volume_profile":
            observed = context_value((matured_volume_by_signal or {}).get(signal_id) or {}, dimension)
        else:
            observed = context_value(context_by_signal.get(signal_id) or {}, dimension)
        if norm_value(observed) != norm_value(definition.get("slice_value")):
            return False
    return True


def validation_role_for_item(item):
    source = item.get("source")
    definition = item.get("freeze_definition") or {}
    if source in BLOCKER_SOURCES or definition.get("transition") or definition.get("category"):
        return "final_eligibility_blocker_repeat_oos"
    if definition.get("candidate_id") and definition.get("dimension"):
        return "candidate_context_final_eligibility_oos"
    if definition.get("candidate_id"):
        return "candidate_only_final_eligibility_oos"
    return "unsupported_final_eligibility_definition"


def validate_item(
    item,
    raw_rows,
    matched_candidates,
    context_by_signal,
    matured_volume_by_signal,
    quality_timing_by_signal,
    signal_states,
    stage_sets,
    global_stage_rates,
    min_selected_events,
):
    definition = item.get("freeze_definition") or {}
    role = validation_role_for_item(item)
    expected_stage = (
        item.get("expected_capture_stage_improved")
        or definition.get("expected_capture_stage_improved")
        or "final_eligibility"
    )
    if role == "unsupported_final_eligibility_definition":
        return {
            "freeze_id": item.get("freeze_id"),
            "source": item.get("source"),
            "definition_fingerprint": item.get("definition_fingerprint"),
            "frozen_at": item.get("frozen_at"),
            "validation_role": role,
            "expected_capture_stage_improved": expected_stage,
            "selected_raw_gold_silver_events": 0,
            "expected_stage_rate": None,
            "global_expected_stage_rate": None,
            "expected_stage_lift_vs_post_freeze_global": None,
            "selected_stage_rates": {},
            "global_stage_rates": global_stage_rates,
            "current_window_evidence": item.get("current_window_evidence") or {},
            "verdict": "FINAL_ELIGIBILITY_60_POST_FREEZE_UNSUPPORTED_DEFINITION",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        }
    selected = []
    for row in raw_rows:
        signal_id = row.get("signal_id_key")
        if not signal_id:
            continue
        if row_matches_definition(
            signal_id,
            definition,
            matched_candidates,
            context_by_signal,
            matured_volume_by_signal,
            quality_timing_by_signal,
            signal_states,
        ):
            selected.append(signal_id)
    selected_rates = signal_stage_rates(selected, stage_sets)
    if expected_stage not in SUPPORTED_STAGES:
        stage_lift = None
        verdict = "FINAL_ELIGIBILITY_60_POST_FREEZE_UNSUPPORTED_EXPECTED_STAGE"
    else:
        selected_stage_rate = selected_rates.get(f"{expected_stage}_rate")
        global_stage_rate = global_stage_rates.get(f"{expected_stage}_rate")
        stage_lift, verdict = selected_status(
            len(selected),
            selected_stage_rate,
            global_stage_rate,
            min_selected_events,
            "blocker_repeat" if role == "final_eligibility_blocker_repeat_oos" else "positive_lift",
        )
    return {
        "freeze_id": item.get("freeze_id"),
        "source": item.get("source"),
        "definition_fingerprint": item.get("definition_fingerprint"),
        "frozen_at": item.get("frozen_at"),
        "validation_role": role,
        "source_plan_item_id": item.get("source_plan_item_id"),
        "priority_rank": item.get("priority_rank"),
        "priority_bucket": item.get("priority_bucket"),
        "candidate_id": definition.get("candidate_id"),
        "dimension": definition.get("dimension"),
        "slice_value": definition.get("slice_value"),
        "category": definition.get("category"),
        "transition": definition.get("transition"),
        "expected_capture_stage_improved": expected_stage,
        "selected_raw_gold_silver_events": len(selected),
        "expected_stage_rate": (
            selected_rates.get(f"{expected_stage}_rate")
            if expected_stage in SUPPORTED_STAGES
            else None
        ),
        "global_expected_stage_rate": (
            global_stage_rates.get(f"{expected_stage}_rate")
            if expected_stage in SUPPORTED_STAGES
            else None
        ),
        "expected_stage_lift_vs_post_freeze_global": stage_lift,
        "selected_stage_rates": selected_rates,
        "global_stage_rates": global_stage_rates,
        "current_window_evidence": item.get("current_window_evidence") or {},
        "verdict": verdict,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def classify_items(raw_count, items, min_raw_events):
    if raw_count < min_raw_events:
        return "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_TOO_SMALL"
    supported = [
        row for row in items
        if row.get("verdict") not in {
            "FINAL_ELIGIBILITY_60_POST_FREEZE_UNSUPPORTED_DEFINITION",
            "FINAL_ELIGIBILITY_60_POST_FREEZE_UNSUPPORTED_EXPECTED_STAGE",
        }
    ]
    if not supported:
        return "FINAL_ELIGIBILITY_60_POST_FREEZE_NO_SUPPORTED_DEFINITIONS"
    blockers = [
        row for row in supported
        if row.get("verdict") == "FINAL_ELIGIBILITY_60_POST_FREEZE_REPEATED_BLOCKER_WATCH"
    ]
    repeated = [
        row for row in supported
        if row.get("verdict") == "FINAL_ELIGIBILITY_60_POST_FREEZE_REPEAT_WATCH"
    ]
    sufficient = [
        row for row in supported
        if row.get("verdict") != "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_TOO_SMALL"
    ]
    if repeated:
        return "FINAL_ELIGIBILITY_60_POST_FREEZE_REPEAT_WATCH"
    if blockers:
        return "FINAL_ELIGIBILITY_60_POST_FREEZE_REPEATED_BLOCKER_WATCH"
    if sufficient:
        return "FINAL_ELIGIBILITY_60_POST_FREEZE_NO_REPEAT"
    return "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_TOO_SMALL"


def refine_classification(base_classification, oos_data_availability):
    if base_classification != "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_TOO_SMALL":
        return base_classification
    mapping = {
        "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_SIGNALS": (
            "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_WAITING_FOR_RAW_SIGNALS"
        ),
        "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER": (
            "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_WAITING_FOR_RAW_GOLD_SILVER"
        ),
        "OOS_DATA_BELOW_MIN_RAW_EVENTS": (
            "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_BELOW_MIN_RAW_EVENTS"
        ),
        "OOS_DATA_OBSERVATION_JOIN_BLOCKED": (
            "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_OBSERVATION_JOIN_BLOCKED"
        ),
    }
    return mapping.get((oos_data_availability or {}).get("classification"), base_classification)


def next_action_for_classification(classification, oos_data_availability):
    if classification in {
        "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_WAITING_FOR_RAW_SIGNALS",
        "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_WAITING_FOR_RAW_GOLD_SILVER",
        "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_BELOW_MIN_RAW_EVENTS",
        "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_OBSERVATION_JOIN_BLOCKED",
    }:
        return (
            (oos_data_availability or {}).get("next_action")
            or "continue_collecting_final_eligibility_oos_window"
        )
    if classification == "FINAL_ELIGIBILITY_60_POST_FREEZE_REPEAT_WATCH":
        return "review_repeated_final_eligibility_oos_evidence_without_promotion"
    if classification == "FINAL_ELIGIBILITY_60_POST_FREEZE_REPEATED_BLOCKER_WATCH":
        return "review_repeated_pending_to_final_blockers_without_runtime_changes"
    if classification == "FINAL_ELIGIBILITY_60_POST_FREEZE_NO_REPEAT":
        return "keep_final_eligibility_definitions_watch_only_or_retire_if_repeated_no_repeat"
    return "continue_collecting_final_eligibility_oos_window"


def build_report(args):
    registry = load_json(args.freeze_registry, {})
    frozen_at = registry.get("definition_set_frozen_at") or registry.get("generated_at")
    frozen_ts = parse_utc_ts(frozen_at)
    if frozen_ts is None:
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "final_eligibility_60_post_freeze_oos_validation",
            "generated_at": utc_now(),
            "classification": "FINAL_ELIGIBILITY_60_POST_FREEZE_FREEZE_TS_MISSING",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "items": [],
        }
    eval_start_ts = int(frozen_ts) + int(args.safety_sec)
    now_ts = int(time.time())
    raw_db = sqlite3.connect(args.raw_db)
    raw_db.row_factory = sqlite3.Row
    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    try:
        post_freeze_source_activity = build_post_freeze_source_activity(raw_db, eval_start_ts, now_ts)
        raw_rows = load_raw_dogs(raw_db, eval_start_ts)
        raw_rows = [
            row for row in raw_rows
            if row.get("signal_ts_norm") is not None
            and row.get("signal_ts_norm") >= eval_start_ts
            and row.get("signal_ts_norm") <= now_ts
        ]
        raw_signal_ids, tokens, _by_signal, _by_token = make_raw_indexes(raw_rows)
        observations, observation_meta = load_candidate_observations(paper_db, raw_signal_ids, eval_start_ts)
        decisions = load_paper_decisions(paper_db, tokens, eval_start_ts, now_ts)
        trades = load_paper_trades(paper_db, tokens, eval_start_ts, now_ts)
        audits = attach_records(raw_rows, observations, decisions, trades, int(args.expected_candidates))
        final_events = load_final_entry_contract_events(paper_db, raw_rows, eval_start_ts, now_ts)
    finally:
        raw_db.close()
        paper_db.close()

    _obs_by_signal, matched_candidates, context_by_signal, candidate_sets, _full = build_observation_indexes(observations)
    quality_timing_by_signal = stage_quality_timing_events(raw_rows, decisions)
    matured_volume_by_signal, matured_volume_meta = load_matured_volume_contexts(
        getattr(args, "kline_db", None),
        raw_rows,
    )
    decisions_by_signal = build_decisions_by_signal(raw_rows, decisions)
    final_events_by_signal = build_final_events_by_signal(raw_rows, final_events)
    signal_states, stage_sets, transition_counts, category_counts = build_signal_state(
        raw_rows,
        audits,
        decisions_by_signal,
        final_events_by_signal,
        trades,
    )
    raw_signal_set = {row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}
    observed_signal_set = set(candidate_sets)
    full_coverage_signal_count = sum(
        1
        for signal_id in raw_signal_set
        if len(candidate_sets.get(signal_id) or set()) >= int(args.expected_candidates)
    )
    raw_count = len(raw_rows)
    global_stage_rates = signal_stage_rates(raw_signal_set, stage_sets)
    items = [
        validate_item(
            item,
            raw_rows,
            matched_candidates,
            context_by_signal,
            matured_volume_by_signal,
            quality_timing_by_signal,
            signal_states,
            stage_sets,
            global_stage_rates,
            int(args.min_selected_events),
        )
        for item in (registry.get("items") or [])
        if isinstance(item, dict)
    ]
    status_counts = Counter(row.get("verdict") for row in items)
    source_counts = Counter(row.get("source") for row in items)
    stage_counts = Counter(row.get("expected_capture_stage_improved") for row in items)
    role_counts = Counter(row.get("validation_role") for row in items)
    repeat_watch = [
        row for row in items
        if row.get("verdict") == "FINAL_ELIGIBILITY_60_POST_FREEZE_REPEAT_WATCH"
    ]
    repeated_blockers = [
        row for row in items
        if row.get("verdict") == "FINAL_ELIGIBILITY_60_POST_FREEZE_REPEATED_BLOCKER_WATCH"
    ]
    positive_lift = [
        row for row in items
        if row.get("expected_stage_lift_vs_post_freeze_global") is not None
        and row.get("expected_stage_lift_vs_post_freeze_global") > 0
    ]
    post_freeze_usable_hours = round(max(0, now_ts - eval_start_ts) / 3600.0, 4)
    oos_data_availability = build_oos_data_availability(
        raw_count,
        int(args.min_raw_events),
        observation_meta,
        post_freeze_source_activity,
    )
    legacy_classification = classify_items(raw_count, items, int(args.min_raw_events))
    classification = refine_classification(legacy_classification, oos_data_availability)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "final_eligibility_60_post_freeze_oos_validation",
        "generated_at": utc_now(),
        "phase": "discovery_readiness",
        "evidence_level": "post_freeze_oos_readiness_probe",
        "usage": "read_only_validation_only",
        "classification": classification,
        "legacy_classification": legacy_classification,
        "oos_data_availability_classification": oos_data_availability.get("classification"),
        "oos_data_root_causes": oos_data_availability.get("root_causes") or [],
        "next_action": next_action_for_classification(classification, oos_data_availability),
        "freeze_registry_available": bool(registry),
        "definition_set_frozen_at": registry.get("definition_set_frozen_at"),
        "freeze_generated_at": registry.get("generated_at"),
        "eval_start_ts": eval_start_ts,
        "eval_start_iso": iso_from_ts(eval_start_ts),
        "now_ts": now_ts,
        "post_freeze_usable_hours": post_freeze_usable_hours,
        "post_freeze_safety_sec": int(args.safety_sec),
        "raw_gold_silver_event_rows": raw_count,
        "raw_gold_silver_rows_since_eval_start_unfiltered": (
            post_freeze_source_activity.get("raw_gold_silver_rows_since_eval_start_unfiltered")
        ),
        "all_raw_rows_since_eval_start": post_freeze_source_activity.get("all_raw_rows_since_eval_start"),
        "latest_raw_signal_age_sec": post_freeze_source_activity.get("latest_raw_signal_age_sec"),
        "latest_raw_gold_silver_age_sec": post_freeze_source_activity.get("latest_raw_gold_silver_age_sec"),
        "latest_raw_gold_silver_lag_sec_before_eval_start": (
            post_freeze_source_activity.get("latest_raw_gold_silver_lag_sec_before_eval_start")
        ),
        "min_raw_events_for_oos_judgment": int(args.min_raw_events),
        "minimum_raw_gold_silver_event_rows": int(args.min_raw_events),
        "minimum_raw_gold_silver_event_rows_for_oos_judgment": int(args.min_raw_events),
        "raw_gold_silver_event_rows_needed_for_min": (
            oos_data_availability.get("raw_gold_silver_event_rows_needed_for_min")
        ),
        "raw_gold_silver_event_rows_needed_to_minimum": (
            oos_data_availability.get("raw_gold_silver_event_rows_needed_for_min")
        ),
        "raw_signal_rows_seen_after_freeze": oos_data_availability.get("raw_signal_rows_seen_after_freeze"),
        "candidate_observation_meta": observation_meta,
        "candidate_observation_effective_status": (
            oos_data_availability.get("candidate_observation_effective_status")
        ),
        "candidate_observation_join_blocked": (
            oos_data_availability.get("classification") == "OOS_DATA_OBSERVATION_JOIN_BLOCKED"
        ),
        "post_freeze_oos_wait_reason": oos_data_availability.get("classification"),
        "post_freeze_wait_reason": oos_data_availability.get("post_freeze_wait_reason"),
        "oos_data_next_action": oos_data_availability.get("next_action"),
        "oos_data_availability": oos_data_availability,
        "post_freeze_source_activity": post_freeze_source_activity,
        "post_freeze_matured_volume_context": matured_volume_meta,
        "post_freeze_global_stage_rates": global_stage_rates,
        "post_freeze_transition_counts": transition_counts,
        "post_freeze_final_blocker_category_counts": category_counts,
        "post_freeze_signal_observation_coverage": {
            "raw_signal_count": len(raw_signal_set),
            "observed_signal_count": len(observed_signal_set & raw_signal_set),
            "observed_signal_rate": rate(len(observed_signal_set & raw_signal_set), len(raw_signal_set)),
            "full_candidate_coverage_signal_count": full_coverage_signal_count,
            "full_candidate_coverage_signal_rate": rate(full_coverage_signal_count, len(raw_signal_set)),
            "expected_candidates": int(args.expected_candidates),
        },
        "frozen_definition_count": len(registry.get("items") or []),
        "validated_definition_count": len(items),
        "supported_definition_count": sum(
            1 for row in items
            if row.get("verdict") not in {
                "FINAL_ELIGIBILITY_60_POST_FREEZE_UNSUPPORTED_DEFINITION",
                "FINAL_ELIGIBILITY_60_POST_FREEZE_UNSUPPORTED_EXPECTED_STAGE",
            }
        ),
        "repeat_watch_count": len(repeat_watch),
        "positive_lift_count": len(positive_lift),
        "repeated_blocker_count": len(repeated_blockers),
        "too_small_definition_count": status_counts.get("FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_TOO_SMALL", 0),
        "status_counts": dict(status_counts),
        "source_counts": dict(source_counts),
        "stage_counts": dict(stage_counts),
        "validation_role_counts": dict(role_counts),
        "top_repeat_watch_items": sorted(
            repeat_watch,
            key=lambda row: (
                row.get("selected_raw_gold_silver_events") or 0,
                row.get("expected_stage_lift_vs_post_freeze_global") or 0,
            ),
            reverse=True,
        )[:20],
        "top_repeated_blocker_items": sorted(
            repeated_blockers,
            key=lambda row: (
                row.get("selected_raw_gold_silver_events") or 0,
                row.get("priority_rank") or 10**9,
            ),
            reverse=True,
        )[:20],
        "items": items,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "notes": [
            "This report validates frozen final-eligibility definitions only on post-freeze raw gold/silver rows.",
            "Repeated blockers and repeated lift are readiness evidence only and do not authorize promotion or runtime behavior changes.",
        ],
    }


def create_self_test_inputs(root):
    now = int(time.time())
    frozen_ts = now - 900
    registry_path = root / "final_eligibility_freeze.json"
    write_json(registry_path, {
        "schema_version": "final_eligibility_60_oos_freeze_registry.v1",
        "generated_at": iso_from_ts(frozen_ts),
        "definition_set_frozen_at": iso_from_ts(frozen_ts),
        "items": [
            {
                "freeze_id": "final-cat-1",
                "source": "pending_to_final_entry_audit",
                "definition_fingerprint": "cat",
                "frozen_at": iso_from_ts(frozen_ts),
                "expected_capture_stage_improved": "final_eligibility",
                "freeze_definition": {
                    "plan_item_id": "pending_to_final:stale_before_final",
                    "category": "stale_before_final",
                    "expected_capture_stage_improved": "final_eligibility",
                },
            },
            {
                "freeze_id": "final-transition-1",
                "source": "pending_to_final_entry_audit.dropoff_counts",
                "definition_fingerprint": "transition",
                "frozen_at": iso_from_ts(frozen_ts),
                "expected_capture_stage_improved": "final_eligibility",
                "freeze_definition": {
                    "plan_item_id": "upstream_transition:pending_no_final_entry",
                    "transition": "pending_no_final_entry",
                    "expected_capture_stage_improved": "final_eligibility",
                },
            },
            {
                "freeze_id": "final-candidate-1",
                "source": "shadow_candidate_improvement_queue",
                "definition_fingerprint": "candidate",
                "frozen_at": iso_from_ts(frozen_ts),
                "expected_capture_stage_improved": "mode_disabled_adjusted_final_eligibility",
                "freeze_definition": {
                    "candidate_id": "notath_quote_clean",
                    "expected_capture_stage_improved": "mode_disabled_adjusted_final_eligibility",
                },
            },
            {
                "freeze_id": "final-cross-1",
                "source": "capture_cross_validity_24h",
                "definition_fingerprint": "cross",
                "frozen_at": iso_from_ts(frozen_ts),
                "expected_capture_stage_improved": "mode_disabled_adjusted_final_eligibility",
                "freeze_definition": {
                    "candidate_id": "notath_quote_clean",
                    "dimension": "source_component",
                    "slice_value": "matrix_evaluator",
                    "expected_capture_stage_improved": "mode_disabled_adjusted_final_eligibility",
                },
            },
        ],
    })
    raw_path = root / "raw.db"
    paper_path = root / "paper.db"
    kline_path = root / "kline.db"
    raw = sqlite3.connect(raw_path)
    raw.execute(
        """
        CREATE TABLE raw_signal_outcomes (
          signal_id INTEGER, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
          signal_type TEXT, source TEXT, observation_status TEXT,
          kline_covered INTEGER, coverage_reason TEXT, baseline_confidence TEXT,
          same_source_path INTEGER, outlier_flag INTEGER, outlier_reason TEXT,
          sustained_evaluable INTEGER, sustained_reason TEXT,
          raw_sustained_tier TEXT, raw_primary_tier TEXT,
          max_sustained_peak_pct REAL, max_wick_peak_pct REAL,
          time_to_sustained_peak_sec REAL, did_enter INTEGER,
          entered_before_peak INTEGER, held_to_silver INTEGER, held_to_gold INTEGER,
          raw_dog_entered INTEGER, raw_dog_realized INTEGER,
          sold_before_silver INTEGER, sold_before_gold INTEGER,
          exit_reason TEXT, payload_json TEXT, source_kind TEXT, source_family TEXT
        )
        """
    )
    raw.executemany(
        """
        INSERT INTO raw_signal_outcomes VALUES (
          ?, ?, ?, ?, 'premium', 'selftest', 'matured', 1, NULL, 'high', 1, 0,
          NULL, 1, NULL, ?, ?, 150.0, 160.0, 300, 0, 0, 0, 0, 0, 0, 0, 0,
          NULL, '{}', 'selftest', 'selftest'
        )
        """,
        [
            (101, "token-a", "A", now - 500, "silver", "silver"),
            (102, "token-b", "B", now - 490, "gold", "gold"),
            (103, "token-c", "C", now - 480, "silver", "silver"),
        ],
    )
    raw.commit()
    raw.close()

    paper = sqlite3.connect(paper_path)
    paper.execute(
        """
        CREATE TABLE candidate_shadow_observations (
          signal_id INTEGER, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT,
          family TEXT, matched INTEGER, reason TEXT, observed_at INTEGER,
          payload_json TEXT
        )
        """
    )
    rows = []
    for signal_id, token, component, matched in [
        (101, "token-a", "matrix_evaluator", 1),
        (102, "token-b", "matrix_evaluator", 1),
        (103, "token-c", "other_component", 0),
    ]:
        rows.append((
            signal_id, token, now - 500, "current_all", "base", 1, "baseline",
            now - 470, json.dumps({"source_component": component}),
        ))
        rows.append((
            signal_id, token, now - 500, "notath_quote_clean", "base", matched,
            "matched" if matched else "no_match", now - 470, None,
        ))
    paper.executemany(
        "INSERT INTO candidate_shadow_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    paper.execute(
        """
        CREATE TABLE paper_decision_events (
          id INTEGER PRIMARY KEY, event_ts INTEGER, signal_id INTEGER, token_ca TEXT,
          symbol TEXT, lifecycle_id TEXT, component TEXT, reason TEXT, event_type TEXT,
          decision TEXT, route TEXT, data_source TEXT, lifecycle_state TEXT,
          payload_json TEXT
        )
        """
    )
    paper.executemany(
        """
        INSERT INTO paper_decision_events
        (event_ts, signal_id, token_ca, symbol, lifecycle_id, component, reason,
         event_type, decision, route, data_source, lifecycle_state, payload_json)
        VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, NULL, 'selftest', NULL, ?)
        """,
        [
            (now - 460, 101, "token-a", "A", "entry_engine", "pass", "decision", "PASS", "{}"),
            (now - 450, 101, "token-a", "A", "entry_engine", "pending", "pending_entry", "PENDING", "{}"),
            (now - 440, 101, "token-a", "A", "entry_engine", "stale_before_final", "entry_block", "BLOCK", "{}"),
            (now - 460, 102, "token-b", "B", "entry_engine", "pass", "decision", "PASS", "{}"),
            (now - 450, 102, "token-b", "B", "entry_engine", "pending", "pending_entry", "PENDING", "{}"),
            (
                now - 440, 102, "token-b", "B", "final_entry_contract",
                "mode disabled", "entry_block", "BLOCK", json.dumps({"hard_blockers": ["mode_disabled"]}),
            ),
            (now - 450, 103, "token-c", "C", "matrix_evaluator", "reject", "timing_decision", "REJECT", "{}"),
        ],
    )
    paper.commit()
    paper.close()

    kline = sqlite3.connect(kline_path)
    kline.execute(
        """
        CREATE TABLE kline_1m (
          token_ca TEXT, timestamp INTEGER, open REAL, high REAL, low REAL,
          close REAL, volume REAL
        )
        """
    )
    kline.commit()
    kline.close()
    return raw_path, paper_path, registry_path, kline_path


def run_self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw_path, paper_path, registry_path, kline_path = create_self_test_inputs(root)
        out = root / "out.json"
        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
            kline_db=str(kline_path),
            freeze_registry=str(registry_path),
            out=str(out),
            expected_candidates=2,
            safety_sec=120,
            min_raw_events=1,
            min_selected_events=1,
        )
        payload = build_report(args)
        write_json(out, payload)
        assert payload["schema_version"] == SCHEMA_VERSION
        assert payload["promotion_allowed"] is False
        assert payload["raw_gold_silver_event_rows"] == 3
        assert payload["oos_data_availability_classification"] == "OOS_DATA_AVAILABLE_FOR_JUDGMENT"
        assert payload["validated_definition_count"] == 4
        assert payload["repeated_blocker_count"] == 2
        assert payload["repeat_watch_count"] == 2
        assert payload["positive_lift_count"] >= 2
        assert payload["source_counts"]["pending_to_final_entry_audit"] == 1
        assert payload["source_counts"]["pending_to_final_entry_audit.dropoff_counts"] == 1
        assert payload["source_counts"]["shadow_candidate_improvement_queue"] == 1
        assert payload["source_counts"]["capture_cross_validity_24h"] == 1
        assert payload["post_freeze_global_stage_rates"]["pending_capture_rate"] == 0.666667
        assert (
            payload["post_freeze_global_stage_rates"][
                "mode_disabled_adjusted_final_eligibility_rate"
            ]
            == 0.333333
        )
        blocker = [
            row for row in payload["items"]
            if row.get("source") == "pending_to_final_entry_audit"
        ][0]
        assert blocker["selected_raw_gold_silver_events"] == 1
        assert blocker["verdict"] == "FINAL_ELIGIBILITY_60_POST_FREEZE_REPEATED_BLOCKER_WATCH"
        candidate = [
            row for row in payload["items"]
            if row.get("source") == "shadow_candidate_improvement_queue"
        ][0]
        assert candidate["selected_raw_gold_silver_events"] == 2
        assert candidate["expected_stage_lift_vs_post_freeze_global"] > 0
        assert out.exists()
        availability = build_oos_data_availability(
            0,
            1,
            {"available": False},
            {"available": True, "all_raw_rows_since_eval_start": 7},
        )
        assert availability["classification"] == "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER"
        assert refine_classification(
            "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_TOO_SMALL",
            availability,
        ) == "FINAL_ELIGIBILITY_60_POST_FREEZE_OOS_WAITING_FOR_RAW_GOLD_SILVER"
    print("self-test passed")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument(
        "--freeze-registry",
        default="/app/data/agent_runs/latest/final_eligibility_60_oos_freeze_registry.json",
    )
    parser.add_argument(
        "--out",
        default="/app/data/agent_runs/latest/final_eligibility_60_post_freeze_oos_validation.json",
    )
    parser.add_argument("--expected-candidates", type=int, default=DEFAULT_EXPECTED_CANDIDATES)
    parser.add_argument("--safety-sec", type=int, default=DEFAULT_SAFETY_SEC)
    parser.add_argument("--min-raw-events", type=int, default=DEFAULT_MIN_RAW_EVENTS)
    parser.add_argument("--min-selected-events", type=int, default=DEFAULT_MIN_SELECTED_EVENTS)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        run_self_test()
        return 0
    payload = build_report(args)
    write_json(args.out, payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
