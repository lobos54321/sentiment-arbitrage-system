#!/usr/bin/env python3
"""Read-only post-freeze OOS validation for capture-first 2D cross definitions.

This report validates frozen same-window capture-cross definitions against raw
gold/silver rows that arrived after the freeze timestamp. It is discovery /
readiness evidence only: it never changes strategy, entry policy, gates,
final_entry_contract, A_CLASS, executor, wallet, canary, or risk settings.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

from offline_raw_gold_silver_funnel_audit import (
    attach_records,
    load_candidate_observations,
    load_paper_decisions,
    load_paper_trades,
    load_raw_dogs,
    make_raw_indexes,
    rate,
    signal_id_key,
)
from pass_allow_60_post_freeze_oos_validation import (
    build_post_freeze_source_activity,
    context_value,
    load_json,
    norm_value,
    parse_utc_ts,
    table_exists,
    write_json,
)


SCHEMA_VERSION = "capture_cross_post_freeze_oos_validation.v1"
DEFAULT_EXPECTED_CANDIDATES = 84
DEFAULT_MIN_RAW_EVENTS = 10
DEFAULT_MIN_SELECTED_EVENTS = 3
DEFAULT_SAFETY_SEC = 120

SUPPORTED_STAGES = {
    "detector_capture",
    "decision_capture",
    "pass_allow_capture",
    "pending_capture",
    "final_eligibility",
    "mode_disabled_adjusted_final_eligibility",
    "paper_capture",
    "realized_capture",
}


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def iso_from_ts(value):
    if value is None:
        return None
    try:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(value)))
    except Exception:
        return None


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


def extract_hard_blockers(payload):
    payload = payload if isinstance(payload, dict) else {}
    blockers = payload.get("hard_blockers")
    if blockers is None and isinstance(payload.get("final_entry_contract"), dict):
        blockers = payload.get("final_entry_contract", {}).get("hard_blockers")
    if isinstance(blockers, str):
        try:
            decoded = json.loads(blockers)
            blockers = decoded
        except Exception:
            blockers = [blockers]
    if isinstance(blockers, (list, tuple, set)):
        return [str(item) for item in blockers if item is not None and str(item)]
    return []


def build_observation_indexes(observations):
    by_signal = defaultdict(list)
    matched_candidates = defaultdict(set)
    context_by_signal = {}
    full_coverage_signals = set()
    candidate_sets = defaultdict(set)
    for row in observations or []:
        key = row.get("signal_id_key")
        if not key:
            continue
        by_signal[key].append(row)
        candidate_id = row.get("candidate_id")
        if candidate_id:
            candidate_sets[key].add(candidate_id)
        if row.get("matched"):
            matched_candidates[key].add(candidate_id)
        if candidate_id == "current_all":
            context_by_signal[key] = row.get("payload") or {}
    return by_signal, matched_candidates, context_by_signal, candidate_sets, full_coverage_signals


def build_stage_sets(paper_db, raw_rows, audits, trades, eval_start_ts, now_ts):
    raw_signal_ids = {row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}
    stages = {
        "detector_capture": set(),
        "decision_capture": set(),
        "pass_allow_capture": set(),
        "pending_capture": set(),
        "final_eligibility": set(),
        "mode_disabled_adjusted_final_eligibility": set(),
        "paper_capture": set(),
        "realized_capture": set(),
    }
    for audit in audits or []:
        signal_id = audit.get("signal_id")
        if not signal_id:
            continue
        if int(audit.get("matched_candidate_count") or 0) > 0:
            stages["detector_capture"].add(signal_id)
        if int(audit.get("decision_record_count") or 0) > 0:
            stages["decision_capture"].add(signal_id)
        if int(audit.get("would_enter_count") or 0) > 0:
            stages["pass_allow_capture"].add(signal_id)
        if audit.get("entered") or int(audit.get("paper_trade_count") or 0) > 0:
            stages["paper_capture"].add(signal_id)
        if audit.get("raw_dog_realized"):
            stages["realized_capture"].add(signal_id)

    for trade in trades or []:
        signal_id = trade.get("signal_id_key")
        if signal_id:
            stages["paper_capture"].add(signal_id)

    if table_exists(paper_db, "paper_decision_events") and raw_signal_ids:
        rows = paper_db.execute(
            """
            SELECT event_ts, signal_id, component, event_type, decision, reason, payload_json
            FROM paper_decision_events
            WHERE event_ts >= ? AND event_ts <= ?
              AND signal_id IS NOT NULL
            """,
            (eval_start_ts - 60, now_ts + 900),
        ).fetchall()
        for row in rows:
            signal_id = signal_id_key(row["signal_id"])
            if signal_id not in raw_signal_ids:
                continue
            event_type = str(row["event_type"] or "").lower()
            decision = str(row["decision"] or "").upper()
            component = str(row["component"] or "")
            stages["decision_capture"].add(signal_id)
            if decision in {"PASS", "ALLOW", "WOULD_ENTER", "ENTER"} or event_type in {"would_enter", "enter"}:
                stages["pass_allow_capture"].add(signal_id)
            if event_type == "pending_entry":
                stages["pending_capture"].add(signal_id)
            if component == "final_entry_contract":
                stages["final_eligibility"].add(signal_id)
                blockers = extract_hard_blockers(jloads(row["payload_json"]))
                non_mode_blockers = [blocker for blocker in blockers if blocker != "mode_disabled"]
                if not non_mode_blockers:
                    stages["mode_disabled_adjusted_final_eligibility"].add(signal_id)
    return stages


def build_oos_data_availability(raw_count, min_raw_events, observation_meta, source_activity):
    all_raw_since_freeze = (source_activity or {}).get("all_raw_rows_since_eval_start")
    root_causes = []
    if all_raw_since_freeze == 0:
        root_causes.append("no_post_freeze_raw_signal_rows")
    if raw_count == 0:
        root_causes.append("no_post_freeze_raw_gold_silver_events")
    elif raw_count < min_raw_events:
        root_causes.append("post_freeze_raw_gold_silver_event_rows_below_min")
    if raw_count > 0 and not (observation_meta or {}).get("available"):
        root_causes.append("candidate_observations_unavailable_for_post_freeze_signal_ids")
    candidate_observation_effective_status = (
        "not_applicable_no_raw_signal_ids"
        if raw_count == 0
        else "available"
        if (observation_meta or {}).get("available")
        else "unavailable"
    )
    classification = (
        "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_SIGNALS"
        if all_raw_since_freeze == 0
        else "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER"
        if raw_count == 0
        else "OOS_DATA_BELOW_MIN_RAW_EVENTS"
        if raw_count < min_raw_events
        else "OOS_DATA_OBSERVATION_JOIN_BLOCKED"
        if candidate_observation_effective_status == "unavailable"
        else "OOS_DATA_AVAILABLE_FOR_JUDGMENT"
    )
    next_action = (
        "wait_for_post_freeze_raw_signal_rows"
        if classification == "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_SIGNALS"
        else "continue_collecting_post_freeze_raw_gold_silver_events"
        if classification == "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER"
        else "continue_collecting_until_min_oos_raw_events"
        if classification == "OOS_DATA_BELOW_MIN_RAW_EVENTS"
        else "inspect_post_freeze_candidate_observation_join"
        if classification == "OOS_DATA_OBSERVATION_JOIN_BLOCKED"
        else "judge_capture_cross_post_freeze_oos_repeat_evidence"
    )
    return {
        "classification": classification,
        "root_causes": root_causes,
        "raw_gold_silver_event_rows": raw_count,
        "min_raw_events_for_oos_judgment": min_raw_events,
        "raw_gold_silver_event_rows_needed_for_min": max(0, int(min_raw_events) - int(raw_count or 0)),
        "all_raw_rows_since_eval_start": all_raw_since_freeze,
        "raw_signal_rows_seen_after_freeze": (
            None if all_raw_since_freeze is None else int(all_raw_since_freeze or 0)
        ),
        "post_freeze_source_activity": source_activity or {},
        "candidate_observation_meta": observation_meta or {},
        "candidate_observation_effective_status": candidate_observation_effective_status,
        "next_action": next_action,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def signal_stage_rates(signal_ids, stages):
    selected = set(signal_ids or [])
    denom = len(selected)
    out = {}
    for stage in sorted(SUPPORTED_STAGES):
        count = len(selected & (stages.get(stage) or set()))
        out[f"{stage}_count"] = count
        out[f"{stage}_rate"] = rate(count, denom)
    return out


def selected_status(selected_count, selected_stage_rate, global_stage_rate, min_selected_events):
    lift = (
        None
        if selected_stage_rate is None or global_stage_rate is None
        else round(float(selected_stage_rate) - float(global_stage_rate), 6)
    )
    if selected_count < min_selected_events:
        verdict = "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL"
    elif lift is not None and lift > 0:
        verdict = "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"
    else:
        verdict = "CAPTURE_CROSS_POST_FREEZE_NO_REPEAT"
    return lift, verdict


def validate_capture_cross_item(
    item,
    raw_rows,
    matched_candidates,
    context_by_signal,
    stages,
    global_stage_rates,
    min_selected_events,
):
    definition = item.get("freeze_definition") or {}
    candidate_id = definition.get("candidate_id")
    dimension = definition.get("dimension")
    slice_value = definition.get("slice_value")
    expected_stage = (
        item.get("expected_capture_stage_improved")
        or definition.get("expected_capture_stage_improved")
        or "detector_capture"
    )
    selected = []
    for row in raw_rows:
        signal_id = row.get("signal_id_key")
        if not signal_id:
            continue
        if candidate_id and candidate_id not in matched_candidates.get(signal_id, set()):
            continue
        if dimension:
            observed_value = context_value(context_by_signal.get(signal_id) or {}, dimension)
            if norm_value(observed_value) != norm_value(slice_value):
                continue
        selected.append(signal_id)
    selected_rates = signal_stage_rates(selected, stages)
    selected_count = len(selected)
    if expected_stage not in SUPPORTED_STAGES:
        stage_lift = None
        verdict = "CAPTURE_CROSS_POST_FREEZE_UNSUPPORTED_EXPECTED_STAGE"
    else:
        selected_stage_rate = selected_rates.get(f"{expected_stage}_rate")
        global_stage_rate = global_stage_rates.get(f"{expected_stage}_rate")
        stage_lift, verdict = selected_status(
            selected_count,
            selected_stage_rate,
            global_stage_rate,
            min_selected_events,
        )
    return {
        "freeze_id": item.get("freeze_id"),
        "source": item.get("source"),
        "definition_fingerprint": item.get("definition_fingerprint"),
        "frozen_at": item.get("frozen_at"),
        "validation_role": "capture_first_2d_cross_post_freeze_oos",
        "candidate_id": candidate_id,
        "dimension": dimension,
        "slice_value": slice_value,
        "expected_capture_stage_improved": expected_stage,
        "selected_raw_gold_silver_events": selected_count,
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
        return "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL"
    supported = [
        row for row in items
        if row.get("verdict") != "CAPTURE_CROSS_POST_FREEZE_UNSUPPORTED_EXPECTED_STAGE"
    ]
    if not supported:
        return "CAPTURE_CROSS_POST_FREEZE_NO_SUPPORTED_DEFINITIONS"
    repeated = [row for row in supported if row.get("verdict") == "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"]
    sufficient = [row for row in supported if row.get("verdict") != "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL"]
    if repeated:
        return "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"
    if sufficient:
        return "CAPTURE_CROSS_POST_FREEZE_NO_REPEAT"
    return "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL"


def refine_classification(base_classification, oos_data_availability):
    if base_classification != "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL":
        return base_classification
    mapping = {
        "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_SIGNALS": (
            "CAPTURE_CROSS_POST_FREEZE_OOS_WAITING_FOR_RAW_SIGNALS"
        ),
        "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER": (
            "CAPTURE_CROSS_POST_FREEZE_OOS_WAITING_FOR_RAW_GOLD_SILVER"
        ),
        "OOS_DATA_BELOW_MIN_RAW_EVENTS": (
            "CAPTURE_CROSS_POST_FREEZE_OOS_BELOW_MIN_RAW_EVENTS"
        ),
        "OOS_DATA_OBSERVATION_JOIN_BLOCKED": (
            "CAPTURE_CROSS_POST_FREEZE_OOS_OBSERVATION_JOIN_BLOCKED"
        ),
    }
    return mapping.get((oos_data_availability or {}).get("classification"), base_classification)


def next_action_for_classification(classification, oos_data_availability):
    if classification in {
        "CAPTURE_CROSS_POST_FREEZE_OOS_WAITING_FOR_RAW_SIGNALS",
        "CAPTURE_CROSS_POST_FREEZE_OOS_WAITING_FOR_RAW_GOLD_SILVER",
        "CAPTURE_CROSS_POST_FREEZE_OOS_BELOW_MIN_RAW_EVENTS",
        "CAPTURE_CROSS_POST_FREEZE_OOS_OBSERVATION_JOIN_BLOCKED",
    }:
        return (oos_data_availability or {}).get("next_action") or "continue_collecting_capture_cross_oos_window"
    if classification == "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH":
        return "review_repeated_capture_cross_oos_evidence_without_promotion"
    if classification == "CAPTURE_CROSS_POST_FREEZE_NO_REPEAT":
        return "keep_frozen_capture_cross_definitions_watch_only_or_retire_if_repeated_no_repeat"
    return "continue_collecting_capture_cross_oos_window"


def build_report(args):
    registry = load_json(args.freeze_registry, {})
    frozen_at = registry.get("definition_set_frozen_at") or registry.get("generated_at")
    frozen_ts = parse_utc_ts(frozen_at)
    if frozen_ts is None:
        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "capture_cross_post_freeze_oos_validation",
            "generated_at": utc_now(),
            "classification": "CAPTURE_CROSS_POST_FREEZE_FREEZE_TS_MISSING",
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
        stages = build_stage_sets(paper_db, raw_rows, audits, trades, eval_start_ts, now_ts)
    finally:
        raw_db.close()
        paper_db.close()

    _obs_by_signal, matched_candidates, context_by_signal, candidate_sets, _full = build_observation_indexes(observations)
    raw_signal_set = {row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}
    observed_signal_set = set(candidate_sets)
    full_coverage_signal_count = sum(
        1
        for signal_id in raw_signal_set
        if len(candidate_sets.get(signal_id) or set()) >= int(args.expected_candidates)
    )
    raw_count = len(raw_rows)
    global_stage_rates = signal_stage_rates(raw_signal_set, stages)
    items = [
        validate_capture_cross_item(
            item,
            raw_rows,
            matched_candidates,
            context_by_signal,
            stages,
            global_stage_rates,
            int(args.min_selected_events),
        )
        for item in (registry.get("items") or [])
        if isinstance(item, dict) and item.get("source") == "capture_first_2d_cross"
    ]
    status_counts = Counter(row.get("verdict") for row in items)
    repeat_watch = [row for row in items if row.get("verdict") == "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"]
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
        "report_type": "capture_cross_post_freeze_oos_validation",
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
        "latest_raw_gold_silver_age_sec": (
            post_freeze_source_activity.get("latest_raw_gold_silver_age_sec")
        ),
        "latest_raw_gold_silver_lag_sec_before_eval_start": (
            post_freeze_source_activity.get("latest_raw_gold_silver_lag_sec_before_eval_start")
        ),
        "min_raw_events_for_oos_judgment": int(args.min_raw_events),
        "raw_gold_silver_event_rows_needed_for_min": (
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
        "oos_data_next_action": oos_data_availability.get("next_action"),
        "oos_data_availability": oos_data_availability,
        "post_freeze_source_activity": post_freeze_source_activity,
        "post_freeze_global_stage_rates": global_stage_rates,
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
            if row.get("verdict") != "CAPTURE_CROSS_POST_FREEZE_UNSUPPORTED_EXPECTED_STAGE"
        ),
        "repeat_watch_count": len(repeat_watch),
        "positive_lift_count": len(positive_lift),
        "too_small_definition_count": status_counts.get("CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL", 0),
        "status_counts": dict(status_counts),
        "source_counts": dict(Counter(row.get("source") for row in items)),
        "stage_counts": dict(Counter(row.get("expected_capture_stage_improved") for row in items)),
        "top_repeat_watch_items": sorted(
            repeat_watch,
            key=lambda row: (
                row.get("selected_raw_gold_silver_events") or 0,
                row.get("expected_stage_lift_vs_post_freeze_global") or 0,
            ),
            reverse=True,
        )[:20],
        "items": items,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "notes": [
            "This report validates frozen capture-first 2D cross definitions only on post-freeze raw gold/silver rows.",
            "Repeat evidence does not authorize promotion or runtime behavior changes.",
        ],
    }


def create_self_test_inputs(root):
    now = int(time.time())
    frozen_ts = now - 600
    registry_path = root / "capture_cross_freeze.json"
    write_json(registry_path, {
        "schema_version": "capture_cross_oos_freeze_registry.v1",
        "generated_at": iso_from_ts(frozen_ts),
        "definition_set_frozen_at": iso_from_ts(frozen_ts),
        "items": [
            {
                "freeze_id": "capture-cross-1",
                "source": "capture_first_2d_cross",
                "definition_fingerprint": "abc",
                "frozen_at": iso_from_ts(frozen_ts),
                "expected_capture_stage_improved": "pending_capture",
                "freeze_definition": {
                    "candidate_id": "notath_quote_clean",
                    "dimension": "source_component",
                    "slice_value": "matrix_evaluator",
                    "expected_capture_stage_improved": "pending_capture",
                },
            }
        ],
    })
    raw_path = root / "raw.db"
    paper_path = root / "paper.db"
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
            (101, "token-a", "A", now - 200, "silver", "silver"),
            (102, "token-b", "B", now - 190, "gold", "gold"),
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
    for signal_id, token, component in [
        (101, "token-a", "matrix_evaluator"),
        (102, "token-b", "other_component"),
    ]:
        rows.append((
            signal_id, token, now - 200, "current_all", "base", 1, "baseline",
            now - 180, json.dumps({"source_component": component}),
        ))
        rows.append((
            signal_id, token, now - 200, "notath_quote_clean", "base", 1,
            "matched", now - 180, None,
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
            (now - 180, 101, "token-a", "A", "entry_engine", "selftest pass", "decision", "PASS", "{}"),
            (now - 170, 101, "token-a", "A", "entry_engine", "pending", "pending_entry", "PENDING", "{}"),
            (
                now - 160, 101, "token-a", "A", "final_entry_contract",
                "mode disabled", "entry_block", "BLOCK", json.dumps({"hard_blockers": ["mode_disabled"]}),
            ),
            (now - 165, 102, "token-b", "B", "matrix_evaluator", "wait", "timing_decision", "WAIT", "{}"),
        ],
    )
    paper.commit()
    paper.close()
    return raw_path, paper_path, registry_path


def run_self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        raw_path, paper_path, registry_path = create_self_test_inputs(root)
        out = root / "out.json"
        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
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
        assert payload["raw_gold_silver_event_rows"] == 2
        assert payload["oos_data_availability_classification"] == "OOS_DATA_AVAILABLE_FOR_JUDGMENT"
        assert payload["validated_definition_count"] == 1
        assert payload["repeat_watch_count"] == 1
        assert payload["classification"] == "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"
        assert payload["post_freeze_global_stage_rates"]["decision_capture_rate"] == 1.0
        assert payload["post_freeze_global_stage_rates"]["pending_capture_rate"] == 0.5
        item = payload["items"][0]
        assert item["selected_raw_gold_silver_events"] == 1
        assert item["selected_stage_rates"]["pending_capture_rate"] == 1.0
        assert item["expected_stage_lift_vs_post_freeze_global"] == 0.5
        assert item["verdict"] == "CAPTURE_CROSS_POST_FREEZE_REPEAT_WATCH"
        assert out.exists()
        availability = build_oos_data_availability(
            0,
            1,
            {"available": False},
            {"available": True, "all_raw_rows_since_eval_start": 7},
        )
        assert availability["classification"] == "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER"
        assert refine_classification(
            "CAPTURE_CROSS_POST_FREEZE_OOS_TOO_SMALL",
            availability,
        ) == "CAPTURE_CROSS_POST_FREEZE_OOS_WAITING_FOR_RAW_GOLD_SILVER"
    print("self-test passed")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument(
        "--freeze-registry",
        default="/app/data/agent_runs/latest/capture_cross_oos_freeze_registry.json",
    )
    parser.add_argument(
        "--out",
        default="/app/data/agent_runs/latest/capture_cross_post_freeze_oos_validation.json",
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
