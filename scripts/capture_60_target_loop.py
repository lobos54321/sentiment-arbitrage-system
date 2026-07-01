#!/usr/bin/env python3
"""Assemble the Gold/Silver 60% Capture AutoLoop v3 target artifacts.

Read-only target layer. This script consumes already materialized AutoLoop
reports and writes the v3 target-driven capture artifacts. It never reads or
writes production strategy, gates, final_entry_contract, A_CLASS mode,
executors, wallets, canary settings, or risk settings.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import tempfile
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

try:
    from review_agent_verdict import build_oos_readiness_summary
except Exception:  # pragma: no cover - self-contained fallback for import edge cases.
    build_oos_readiness_summary = None


SCHEMA_VERSION = "capture_60_target_loop.v1"
TARGET_RATE = 0.60
STATUS_CLEAN = "CLEAN"
STATUS_PENDING = "CLEAN_WINDOW_PENDING"
STATUS_WRITER_BUG = "WRITER_BUG_PERSISTS"
STATUS_NA = "NOT_APPLICABLE"
STATUS_BLOCKED = "BLOCKED_UNKNOWN"

REQUIRED_FILES = {
    "capture": "capture_discovery_24h.json",
    "raw_funnel": "raw_gold_silver_funnel_audit_24h.json",
    "shadow_decision_bridge": "shadow_decision_bridge_audit_24h.json",
    "candidate_downstream": "candidate_downstream_readiness_24h.json",
    "context_coverage": "context_coverage_audit_24h.json",
    "context_blocker_monitor": "context_blocker_monitor_24h.json",
    "a_class": "a_class_fastlane_mode_audit_24h.json",
    "strategy_memory_validation": "strategy_memory_validation_24h.json",
    "strategy_memory_ingestion": "strategy_memory_ingestion_summary.json",
    "strategy_memory_filtered_winner_bridge": "strategy_memory_filtered_winner_bridge.json",
    "strategy_memory_exit_shadow_summary": "strategy_memory_exit_shadow_summary.json",
    "strategy_memory_delay_replay_summary": "strategy_memory_delay_replay_summary.json",
    "candidate_improvement": "candidate_improvement_opportunities_24h.json",
    "quality_timing_reject_research": "quality_timing_reject_research_audit_24h.json",
    "quality_timing_candidate_probe_validation": "quality_timing_candidate_probe_validation_24h.json",
    "decision_no_pass_quality_timing_watch_validation": "decision_no_pass_quality_timing_watch_validation_24h.json",
    "capture_cross": "capture_cross_validity_24h.json",
    "markov_effectiveness": "markov_effectiveness_24h.json",
    "volume_kline_coverage_audit": "volume_kline_coverage_audit_24h.json",
    "pnl_secondary": "pnl_cross_secondary_24h.json",
    "oos_refresh": "oos_readiness_probe_refresh.json",
}

V3_OUTPUT_FILES = {
    "capture_60_gap_report": "capture_60_gap_report.json",
    "capture_stage_metrics": "capture_stage_metrics.json",
    "context_dimension_eligibility": "context_dimension_eligibility.json",
    "pass_allow_capture_gap_audit": "pass_allow_capture_gap_audit.json",
    "decision_no_pass_quality_timing_review": "decision_no_pass_quality_timing_review.json",
    "pass_allow_60_closure_plan": "pass_allow_60_closure_plan.json",
    "pass_allow_60_oos_freeze_registry": "pass_allow_60_oos_freeze_registry.json",
    "pending_to_final_entry_audit": "pending_to_final_entry_audit.json",
    "final_entry_readiness_audit": "final_entry_readiness_audit.json",
    "strategy_memory_capture_validation": "strategy_memory_capture_validation.json",
    "shadow_candidate_improvement_queue": "shadow_candidate_improvement_queue.json",
    "oos_readiness_summary": "oos_readiness_summary.json",
}


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path, default=None):
    if not path:
        return default if default is not None else {}
    target = Path(path)
    if not target.exists():
        return default if default is not None else {}
    try:
        with target.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if data is not None else (default if default is not None else {})
    except Exception:
        return default if default is not None else {}


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def safe_int(value, default=0):
    parsed = safe_float(value, None)
    return default if parsed is None else int(parsed)


def boolish(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_utc_ts(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        pass
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(datetime.fromisoformat(text).timestamp())
    except Exception:
        return None


def rate(num, den):
    den = safe_float(den, 0)
    if not den:
        return None
    return round(float(num or 0) / den, 6)


def coverage_rate(row):
    row = row or {}
    for key in ("coverage_rate", "effective_present_rate", "present_rate", "known_rate"):
        value = safe_float(row.get(key))
        if value is not None:
            return value
    pct = safe_float(row.get("coverage_pct") or row.get("effective_present_pct"))
    if pct is not None:
        return pct / 100.0 if pct > 1 else pct
    return None


def collect_reports(run_dir):
    run_dir = Path(run_dir)
    return {
        key: load_json(run_dir / filename, {})
        for key, filename in REQUIRED_FILES.items()
    }


def target_count(denominator):
    return None if not denominator else int(math.ceil(float(denominator) * TARGET_RATE))


def compact_stage_counts(a_class):
    rates = (a_class or {}).get("capture_stage_rates") or {}
    events = rates.get("events") or {}
    denominator = safe_int(rates.get("denominator_raw_signal_ids"), 0)
    raw_event_denominator = safe_int(rates.get("denominator_raw_gold_silver_events"), 0)
    mode_adjusted = rates.get("mode_disabled_adjusted_final_eligibility") or {}
    return {
        "raw_gold_silver_denominator": denominator,
        "raw_gold_silver_event_denominator": raw_event_denominator,
        "target_60_count": target_count(denominator),
        "detector_capture": {
            "count": safe_int(events.get("candidate_matched_any"), 0),
            "rate": rates.get("detector_capture_rate"),
        },
        "decision_capture": {
            "count": safe_int(events.get("decision_records"), 0),
            "rate": rates.get("decision_record_capture_rate"),
        },
        "pass_allow_capture": {
            "count": safe_int(events.get("pass_or_allow"), 0),
            "rate": rates.get("pass_allow_capture_rate"),
        },
        "pending_capture": {
            "count": safe_int(events.get("pending_entry"), 0),
            "rate": rates.get("pending_capture_rate"),
        },
        "final_eligibility": {
            "count": safe_int(events.get("final_entry_contract"), 0),
            "rate": rates.get("final_entry_contract_reach_rate"),
        },
        "mode_disabled_adjusted_final_eligibility": {
            "count": safe_int(mode_adjusted.get("mode_disabled_only_unique_signal_ids"), safe_int(events.get("mode_disabled_only_final_entry"), 0)),
            "rate": rates.get("mode_disabled_adjusted_final_eligibility_rate"),
            "status": mode_adjusted.get("status"),
            "definition": mode_adjusted.get("definition"),
        },
        "paper_trade_intent": {
            "count": safe_int(events.get("paper_trade_intent"), 0),
            "rate": rates.get("paper_trade_intent_rate"),
        },
        "paper_capture": {
            "count": safe_int(events.get("paper_committed"), 0),
            "rate": rates.get("paper_capture_rate"),
        },
        "realized_capture": {
            "count": safe_int(events.get("realized"), 0),
            "rate": rates.get("realized_capture_rate"),
        },
        "actual_entered": {
            "count": safe_int(events.get("entered"), 0),
            "rate": rates.get("actual_entered_rate"),
        },
    }


def stage_sequence(stage_counts):
    return [
        ("detector_capture", stage_counts["detector_capture"]),
        ("decision_capture", stage_counts["decision_capture"]),
        ("pass_allow_capture", stage_counts["pass_allow_capture"]),
        ("pending_capture", stage_counts["pending_capture"]),
        ("final_eligibility", stage_counts["final_eligibility"]),
        ("mode_disabled_adjusted_final_eligibility", stage_counts["mode_disabled_adjusted_final_eligibility"]),
        ("paper_capture", stage_counts["paper_capture"]),
        ("realized_capture", stage_counts["realized_capture"]),
    ]


def first_stage_below_target(stage_counts):
    target = stage_counts.get("target_60_count")
    if target is None:
        return None, None
    for name, row in stage_sequence(stage_counts):
        count = safe_int(row.get("count"), 0)
        if count < target:
            return name, max(0, target - count)
    return "target_reached", 0


def largest_stage_dropoff(stage_counts):
    rows = []
    previous_name = "raw_gold_silver_denominator"
    previous_count = safe_int(stage_counts.get("raw_gold_silver_denominator"), 0)
    for name, row in stage_sequence(stage_counts):
        count = safe_int(row.get("count"), 0)
        rows.append({
            "from_stage": previous_name,
            "to_stage": name,
            "from_count": previous_count,
            "to_count": count,
            "drop_count": max(0, previous_count - count),
            "drop_rate_of_previous": rate(max(0, previous_count - count), previous_count),
        })
        previous_name = name
        previous_count = count
    rows.sort(key=lambda item: (item["drop_count"], item["from_stage"]), reverse=True)
    return rows[0] if rows else {}


def stage_specific_allowed_action(biggest_gap_stage, pending_audit):
    if biggest_gap_stage in {"detector_capture"}:
        return "generate_shadow_candidate_improvement_queue"
    if biggest_gap_stage in {"decision_capture", "pass_allow_capture"}:
        return "audit_decision_bridge_and_quality_timing_shadow_only"
    if biggest_gap_stage in {"pending_capture"}:
        return "audit_pass_allow_to_pending_bridge_shadow_only"
    if biggest_gap_stage in {"final_eligibility", "mode_disabled_adjusted_final_eligibility"}:
        dominant = ((pending_audit.get("pending_no_final_entry_classification") or {}).get("dominant_category"))
        if dominant:
            return f"audit_pending_to_final_entry_{dominant}_shadow_only"
        return "audit_pending_to_final_entry_dropoff_shadow_only"
    if biggest_gap_stage in {"paper_capture", "realized_capture"}:
        return "human_approval_required_before_paper_or_exit_runtime_changes"
    return "continue_shadow_oos_collection"


def transition_dropoff_allowed_action(dropoff, pending_audit):
    to_stage = (dropoff or {}).get("to_stage")
    if not to_stage:
        return "continue_shadow_oos_collection"
    return stage_specific_allowed_action(to_stage, pending_audit)


def gap_interpretation(stage_metrics, biggest_gap_stage, additional_needed, pending_audit):
    largest_dropoff = stage_metrics.get("largest_stage_dropoff") or {}
    target_track = {
        "track": "target_shortfall_to_60",
        "stage": biggest_gap_stage,
        "additional_count_needed_to_60": additional_needed,
        "meaning": (
            "First capture stage below the 60% target. This determines the next "
            "target-driven shadow/evaluator action."
        ),
        "next_action": stage_specific_allowed_action(biggest_gap_stage, pending_audit),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }
    transition_track = {
        "track": "largest_transition_dropoff",
        "from_stage": largest_dropoff.get("from_stage"),
        "to_stage": largest_dropoff.get("to_stage"),
        "drop_count": largest_dropoff.get("drop_count"),
        "drop_rate_of_previous": largest_dropoff.get("drop_rate_of_previous"),
        "meaning": (
            "Largest count loss between adjacent funnel stages. This is diagnostic "
            "dropoff evidence and may require a parallel audit even when it is not "
            "the first stage below 60%."
        ),
        "next_action": transition_dropoff_allowed_action(largest_dropoff, pending_audit),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }
    return {
        "target_shortfall_stage": biggest_gap_stage,
        "largest_transition_dropoff_stage": largest_dropoff.get("to_stage"),
        "largest_transition_dropoff": largest_dropoff,
        "tracks": [target_track, transition_track],
        "promotion_allowed": False,
    }


def next_best_allowed_action(biggest_gap_stage, context_eligibility, pending_audit):
    blocked_dimensions = [
        name for name, row in (context_eligibility.get("dimensions") or {}).items()
        if row.get("status") not in {STATUS_CLEAN, STATUS_NA}
    ]
    stage_action = stage_specific_allowed_action(biggest_gap_stage, pending_audit)
    if blocked_dimensions:
        if any((context_eligibility.get("dimensions") or {}).get(name, {}).get("status") == STATUS_WRITER_BUG for name in blocked_dimensions):
            return "fix_context_writer_path_before_using_blocked_dimensions"
        if biggest_gap_stage in {
            "decision_capture",
            "pass_allow_capture",
            "pending_capture",
            "final_eligibility",
            "mode_disabled_adjusted_final_eligibility",
        }:
            return f"{stage_action}_with_blocked_context_dimensions_excluded"
        if biggest_gap_stage == "detector_capture":
            return "generate_shadow_candidate_improvement_queue_using_clean_dimensions_only"
        if any((context_eligibility.get("dimensions") or {}).get(name, {}).get("status") == STATUS_PENDING for name in blocked_dimensions):
            return "wait_for_context_clean_window_and_continue_shadow_oos_collection"
        return "run_context_coverage_audit_and_block_dirty_dimensions"
    return stage_action


def build_capture_stage_metrics(a_class):
    stage_counts = compact_stage_counts(a_class)
    shortfall = (a_class or {}).get("readiness_shortfall_summary") or {}
    payload = {
        "schema_version": "capture_stage_metrics.v1",
        "report_type": "capture_stage_metrics",
        "generated_at": utc_now(),
        "target_capture_rate": TARGET_RATE,
        "target_stage_while_shadow": "mode_disabled_adjusted_final_eligibility",
        "promotion_allowed": False,
        "stage_counts": stage_counts,
        "readiness_shortfall_summary": shortfall,
        "largest_stage_dropoff": largest_stage_dropoff(stage_counts),
        "notes": [
            "Stage counts are read-only funnel evidence.",
            "While A_CLASS is SHADOW, mode-disabled-adjusted final eligibility is readiness evidence, not paper capture.",
        ],
    }
    payload.update({
        "raw_gold_silver_denominator": stage_counts.get("raw_gold_silver_denominator"),
        "raw_gold_silver_event_denominator": stage_counts.get("raw_gold_silver_event_denominator"),
        "target_60_count": stage_counts.get("target_60_count"),
        "detector_capture_count": (stage_counts.get("detector_capture") or {}).get("count"),
        "detector_capture_rate": (stage_counts.get("detector_capture") or {}).get("rate"),
        "decision_capture_count": (stage_counts.get("decision_capture") or {}).get("count"),
        "decision_capture_rate": (stage_counts.get("decision_capture") or {}).get("rate"),
        "pass_allow_capture_count": (stage_counts.get("pass_allow_capture") or {}).get("count"),
        "pass_allow_capture_rate": (stage_counts.get("pass_allow_capture") or {}).get("rate"),
        "pending_capture_count": (stage_counts.get("pending_capture") or {}).get("count"),
        "pending_capture_rate": (stage_counts.get("pending_capture") or {}).get("rate"),
        "final_eligibility_count": (stage_counts.get("final_eligibility") or {}).get("count"),
        "final_eligibility_rate": (stage_counts.get("final_eligibility") or {}).get("rate"),
        "final_eligibility_capture_count": (stage_counts.get("final_eligibility") or {}).get("count"),
        "final_eligibility_capture_rate": (stage_counts.get("final_eligibility") or {}).get("rate"),
        "mode_disabled_adjusted_final_eligibility_count": (
            stage_counts.get("mode_disabled_adjusted_final_eligibility") or {}
        ).get("count"),
        "mode_disabled_adjusted_final_eligibility_rate": (
            stage_counts.get("mode_disabled_adjusted_final_eligibility") or {}
        ).get("rate"),
        "paper_trade_intent_count": (stage_counts.get("paper_trade_intent") or {}).get("count"),
        "paper_trade_intent_rate": (stage_counts.get("paper_trade_intent") or {}).get("rate"),
        "paper_capture_count": (stage_counts.get("paper_capture") or {}).get("count"),
        "paper_capture_rate": (stage_counts.get("paper_capture") or {}).get("rate"),
        "realized_capture_count": (stage_counts.get("realized_capture") or {}).get("count"),
        "realized_capture_rate": (stage_counts.get("realized_capture") or {}).get("rate"),
        "actual_entered_count": (stage_counts.get("actual_entered") or {}).get("count"),
        "actual_entered_rate": (stage_counts.get("actual_entered") or {}).get("rate"),
    })
    return payload


def build_pending_to_final_entry_audit(a_class):
    rates = (a_class or {}).get("capture_stage_rates") or {}
    upstream = rates.get("upstream_funnel_gap") or (a_class or {}).get("upstream_funnel_gap") or {}
    pending_gap = rates.get("pending_to_final_entry_gap") or (a_class or {}).get("pending_to_final_entry_gap") or {}
    events = rates.get("events") or {}
    pending_without_final = safe_int(pending_gap.get("pending_without_final_entry_contract"), 0)
    final_count = safe_int(events.get("final_entry_contract"), 0)
    paper_count = safe_int(events.get("paper_committed"), 0)
    category_rows = ((pending_gap.get("pending_without_final_entry_category_counts") or {}).get("categories") or [])
    required_categories = {
        "stale_before_final": 0,
        "quote_missing": 0,
        "route_missing": 0,
        "spread_above_route_limit": 0,
        "expected_rr_below_policy": 0,
        "hourly_cap_block": 0,
        "lifecycle_cancelled": 0,
        "duplicate_or_existing_position": 0,
        "pending_expired": 0,
        "missing_final_contract_record": 0,
        "mode_shadow_preblocked": 0,
        "unknown": 0,
    }
    top_reason_rows = []
    bucket_source_rows = {category: [] for category in required_categories}
    for row in category_rows:
        count = safe_int(row.get("count"), 0)
        category = str(row.get("category") or "")
        reasons = row.get("top_reasons") or []
        text = " ".join(str(reason.get("reason") or "") for reason in reasons).lower()
        if "stale" in text or category == "QUALITY_OR_TIMING_REJECT":
            bucket = "stale_before_final"
        elif "quote" in text:
            bucket = "quote_missing"
        elif "route" in text:
            bucket = "route_missing"
        elif "spread" in text:
            bucket = "spread_above_route_limit"
        elif "expected_rr" in text or "rr" in text:
            bucket = "expected_rr_below_policy"
        elif "hour" in text or "cap" in text or "rate_limited" in text:
            bucket = "hourly_cap_block"
        elif "lifecycle" in text or "cancel" in text:
            bucket = "lifecycle_cancelled"
        elif "duplicate" in text or "existing" in text:
            bucket = "duplicate_or_existing_position"
        elif "expired" in text or "timeout" in text:
            bucket = "pending_expired"
        elif "shadow" in text or "mode" in text or category == "MODE_SHADOW_OR_RATE_LIMIT":
            bucket = "mode_shadow_preblocked"
        elif category == "DATA_OR_MARKET_CONTEXT_BLOCK":
            bucket = "quote_missing"
        elif category == "SIGNAL_SUPERSEDED_OR_ABORTED":
            bucket = "lifecycle_cancelled"
        elif category == "UNKNOWN_PENDING_TO_FINAL_GAP":
            bucket = "unknown"
        else:
            bucket = "missing_final_contract_record"
        required_categories[bucket] += count
        bucket_source_rows.setdefault(bucket, []).append({
            "source_category": category,
            "count": count,
            "description": row.get("description"),
            "share_of_pending_without_final": row.get("share_of_pending_without_final"),
            "top_reasons": reasons[:8],
            "automatic_allowed_scope": row.get("automatic_allowed_scope"),
            "human_approval_required_if_fix_requires": row.get(
                "human_approval_required_if_fix_requires"
            ),
        })
        top_reason_rows.extend(reasons[:5])
    if pending_without_final and not any(required_categories.values()):
        required_categories["unknown"] = pending_without_final
        bucket_source_rows.setdefault("unknown", []).append({
            "source_category": "UNKNOWN_PENDING_TO_FINAL_GAP",
            "count": pending_without_final,
            "description": "Pending entries did not reach final_entry_contract and no more specific attribution was available.",
            "top_reasons": [],
        })
    dominant = max(required_categories.items(), key=lambda item: item[1])[0] if required_categories else "unknown"
    pending_count = safe_int(events.get("pending_entry"), 0)
    adjacent_drop_count = max(0, pending_count - final_count)
    largest_transition_review = build_pending_to_final_transition_review(
        category_counts=required_categories,
        bucket_source_rows=bucket_source_rows,
        pending_count=pending_count,
        final_count=final_count,
        adjacent_drop_count=adjacent_drop_count,
        pending_without_final=pending_without_final,
    )
    return {
        "schema_version": "pending_to_final_entry_audit.v1",
        "report_type": "pending_to_final_entry_audit",
        "generated_at": utc_now(),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "dropoff_counts": {
            "no_decision": safe_int(upstream.get("no_decision_record"), 0),
            "decision_no_pass_allow": safe_int(upstream.get("decision_no_pass_or_allow"), 0),
            "pass_allow_no_pending": safe_int(upstream.get("pass_or_allow_without_pending_entry"), 0),
            "pending_no_final_entry": pending_without_final,
            "final_entry_no_paper": max(0, final_count - paper_count),
        },
        "pending_no_final_entry_classification": {
            "dominant_category": dominant,
            "categories": required_categories,
            "source_category_counts": category_rows,
            "top_reasons": top_reason_rows[:20],
        },
        "largest_transition_dropoff_review": largest_transition_review,
        "upstream_funnel_gap": upstream,
        "pending_to_final_entry_gap": pending_gap,
    }


def pending_to_final_category_action(category):
    actions = {
        "stale_before_final": {
            "next_action": "audit_quality_timing_staleness_before_final_shadow_only",
            "allowed_scope": "shadow-only timing attribution and evaluator reporting",
            "human_approval_required_if_fix_requires": "changing signal freshness, timing thresholds, entry policy, or final_entry_contract",
        },
        "quote_missing": {
            "next_action": "audit_quote_context_before_final_data_path_only",
            "allowed_scope": "quote data/evaluator attribution only",
            "human_approval_required_if_fix_requires": "relaxing quote, route, spread, or execution requirements",
        },
        "route_missing": {
            "next_action": "audit_route_context_before_final_data_path_only",
            "allowed_scope": "route data/evaluator attribution only",
            "human_approval_required_if_fix_requires": "relaxing route availability or executor requirements",
        },
        "spread_above_route_limit": {
            "next_action": "audit_spread_distribution_before_final_shadow_only",
            "allowed_scope": "spread distribution reporting only",
            "human_approval_required_if_fix_requires": "changing spread limits or route policy",
        },
        "expected_rr_below_policy": {
            "next_action": "audit_expected_rr_before_final_shadow_only",
            "allowed_scope": "expected-RR distribution reporting only",
            "human_approval_required_if_fix_requires": "changing expected-RR policy",
        },
        "hourly_cap_block": {
            "next_action": "audit_capacity_and_hourly_cap_shadow_only",
            "allowed_scope": "capacity/cap attribution only",
            "human_approval_required_if_fix_requires": "changing hourly cap, risk, canary, or executor settings",
        },
        "lifecycle_cancelled": {
            "next_action": "audit_lifecycle_cancellation_before_final_shadow_only",
            "allowed_scope": "lifecycle attribution and candidate context reporting only",
            "human_approval_required_if_fix_requires": "changing lifecycle state machine or entry eligibility",
        },
        "duplicate_or_existing_position": {
            "next_action": "audit_duplicate_existing_position_attribution_shadow_only",
            "allowed_scope": "duplicate/existing-position attribution only",
            "human_approval_required_if_fix_requires": "changing position limits or duplicate suppression",
        },
        "pending_expired": {
            "next_action": "audit_pending_ttl_and_expiry_shadow_only",
            "allowed_scope": "pending TTL/timing attribution only",
            "human_approval_required_if_fix_requires": "changing pending TTL or retry policy",
        },
        "missing_final_contract_record": {
            "next_action": "audit_final_entry_contract_recording_bridge",
            "allowed_scope": "instrumentation/join/reporting audit only",
            "human_approval_required_if_fix_requires": "changing final_entry_contract behavior",
        },
        "mode_shadow_preblocked": {
            "next_action": "audit_a_class_shadow_preblock_readiness_only",
            "allowed_scope": "A_CLASS readiness attribution only",
            "human_approval_required_if_fix_requires": "resetting SHADOW, enabling A_CLASS, or changing runtime mode",
        },
        "unknown": {
            "next_action": "improve_pending_to_final_attribution_instrumentation",
            "allowed_scope": "read-only attribution instrumentation",
            "human_approval_required_if_fix_requires": "changing strategy, gates, final_entry_contract, A_CLASS, executor, or risk",
        },
    }
    return actions.get(category, actions["unknown"])


def build_pending_to_final_transition_review(
    *,
    category_counts,
    bucket_source_rows,
    pending_count,
    final_count,
    adjacent_drop_count,
    pending_without_final,
):
    target_to_explain = adjacent_drop_count or pending_without_final
    rows = []
    for category, count in sorted(
        (category_counts or {}).items(),
        key=lambda item: (safe_int(item[1], 0), str(item[0])),
        reverse=True,
    ):
        count = safe_int(count, 0)
        if count <= 0:
            continue
        action = pending_to_final_category_action(category)
        rows.append({
            "category": category,
            "event_count": count,
            "share_of_pending_without_final": rate(count, pending_without_final),
            "share_of_adjacent_transition_dropoff_upper_bound": rate(
                min(count, target_to_explain),
                target_to_explain,
            ),
            "source_categories": (bucket_source_rows or {}).get(category) or [],
            "next_action": action["next_action"],
            "allowed_scope": action["allowed_scope"],
            "human_approval_required_if_fix_requires": action[
                "human_approval_required_if_fix_requires"
            ],
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    selected = []
    cumulative = 0
    for row in rows:
        if target_to_explain and cumulative >= target_to_explain:
            break
        contribution = min(
            safe_int(row.get("event_count"), 0),
            max(0, target_to_explain - cumulative),
        )
        cumulative += contribution
        selected.append({
            "category": row.get("category"),
            "event_count": row.get("event_count"),
            "events_contributing_to_largest_transition_dropoff_upper_bound": contribution,
            "cumulative_events_contributing_to_largest_transition_dropoff_upper_bound": cumulative,
            "next_action": row.get("next_action"),
            "allowed_scope": row.get("allowed_scope"),
            "human_approval_required_if_fix_requires": row.get(
                "human_approval_required_if_fix_requires"
            ),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    return {
        "schema_version": "pending_to_final_transition_dropoff_review.v1",
        "report_type": "pending_to_final_transition_dropoff_review",
        "evidence_level": "discovery_same_window_transition_attribution",
        "pending_capture_count": pending_count,
        "final_eligibility_count": final_count,
        "adjacent_count_loss_pending_to_final": adjacent_drop_count,
        "pending_without_final_entry_contract": pending_without_final,
        "target_to_explain_count": target_to_explain,
        "category_count": len(rows),
        "categories": rows,
        "selected_categories_to_cover_largest_transition_dropoff_upper_bound": selected,
        "selected_upper_bound_event_count": cumulative,
        "covers_largest_transition_dropoff_upper_bound": (
            cumulative >= target_to_explain if target_to_explain else True
        ),
        "next_action": (
            selected[0]["next_action"]
            if selected
            else "continue_pending_to_final_transition_monitoring"
        ),
        "interpretation": (
            "Upper-bound attribution only. It explains where pending entries disappeared before final_entry_contract; "
            "it does not prove any pending signal should bypass timing, quote, lifecycle, mode, or final-entry controls."
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def quality_timing_cluster_action(cluster):
    actions = {
        "momentum_fading_or_negative_trend": {
            "next_action": "audit_pending_quality_timing_momentum_decay_shadow_only",
            "human_approval_required_if_fix_requires": (
                "changing momentum, trend, timing, entry policy, or final_entry_contract"
            ),
        },
        "chasing_top_timing_reject": {
            "next_action": "audit_pending_chasing_top_timing_window_shadow_only",
            "human_approval_required_if_fix_requires": (
                "changing top-chasing rejection policy, timing thresholds, or final_entry_contract"
            ),
        },
        "buy_pressure_weak": {
            "next_action": "audit_pending_buy_pressure_decay_shadow_only",
            "human_approval_required_if_fix_requires": (
                "changing buy-pressure thresholds, quality gates, or final_entry_contract"
            ),
        },
        "score_or_quality_too_low": {
            "next_action": "audit_pending_quality_score_decay_shadow_only",
            "human_approval_required_if_fix_requires": (
                "changing quality-score thresholds, entry policy, or final_entry_contract"
            ),
        },
        "entry_timing_timeout_or_retry": {
            "next_action": "audit_pending_entry_timing_timeout_shadow_only",
            "human_approval_required_if_fix_requires": (
                "changing pending TTL, retry policy, timing thresholds, or final_entry_contract"
            ),
        },
        "newborn_pullback_timing_reject": {
            "next_action": "audit_pending_newborn_pullback_timing_shadow_only",
            "human_approval_required_if_fix_requires": (
                "changing newborn pullback timing policy or final_entry_contract"
            ),
        },
        "other_quality_timing_reject": {
            "next_action": "decompose_pending_quality_timing_other_rejects_shadow_only",
            "human_approval_required_if_fix_requires": (
                "changing strategy, thresholds, gates, final_entry_contract, A_CLASS, executor, or risk"
            ),
        },
        "unknown_quality_timing_reject": {
            "next_action": "improve_pending_quality_timing_attribution_instrumentation",
            "human_approval_required_if_fix_requires": (
                "changing strategy, thresholds, gates, final_entry_contract, A_CLASS, executor, or risk"
            ),
        },
    }
    return actions.get(cluster, actions["other_quality_timing_reject"])


def median_value(values):
    nums = sorted(
        safe_float(value)
        for value in values
        if safe_float(value) is not None
    )
    if not nums:
        return None
    mid = len(nums) // 2
    if len(nums) % 2:
        return nums[mid]
    return round((nums[mid - 1] + nums[mid]) / 2, 6)


def peak_lag_band(seconds):
    value = safe_float(seconds)
    if value is None:
        return "unknown_peak_lag"
    if value <= 60:
        return "peak_within_60s"
    if value <= 300:
        return "peak_1m_to_5m"
    if value <= 900:
        return "peak_5m_to_15m"
    return "peak_after_15m"


def build_pending_momentum_decay_review(clusters, quality_timing, context_eligibility):
    cluster_row = next(
        (
            row for row in clusters or []
            if row.get("cluster") == "momentum_fading_or_negative_trend"
        ),
        {},
    )
    opportunities = {
        row.get("cluster"): row
        for row in ((quality_timing.get("shadow_only_review") or {}).get("top_research_opportunities") or [])
        if isinstance(row, dict) and row.get("cluster")
    }
    opportunity = opportunities.get("momentum_fading_or_negative_trend") or {}
    examples = [
        row for row in (quality_timing.get("top_examples") or [])
        if isinstance(row, dict)
        and (
            row.get("shadow_review_cluster") == "momentum_fading_or_negative_trend"
            or ((row.get("attribution") or {}).get("reason") in {"momentum_fading", "negative_trend"})
        )
    ]
    example_peak_lags = [
        row.get("time_to_sustained_peak_sec")
        for row in examples
        if safe_float(row.get("time_to_sustained_peak_sec")) is not None
    ]
    median_peak_lag = (
        safe_float(opportunity.get("time_to_sustained_peak_sec_median"))
        if opportunity.get("time_to_sustained_peak_sec_median") is not None
        else median_value(example_peak_lags)
    )
    band_counts = Counter(peak_lag_band(value) for value in example_peak_lags)
    if median_peak_lag is None:
        recheck_window = "PEAK_LAG_EVIDENCE_INCOMPLETE"
        next_action = "collect_pending_momentum_decay_peak_lag_examples"
    elif median_peak_lag > 300:
        recheck_window = "RECHECK_WINDOW_EXISTS_BEFORE_SUSTAINED_PEAK"
        next_action = "validate_pending_momentum_decay_recheck_window_shadow_only"
    elif median_peak_lag > 60:
        recheck_window = "SHORT_RECHECK_WINDOW_BEFORE_SUSTAINED_PEAK"
        next_action = "validate_short_pending_momentum_decay_recheck_window_shadow_only"
    else:
        recheck_window = "LIKELY_TOO_LATE_FOR_RECHECK"
        next_action = "monitor_momentum_decay_rejects_without_policy_change"

    top_candidates = (opportunity.get("top_candidates") or cluster_row.get("top_candidates") or [])
    top_families = Counter(
        str(row.get("family") or "unknown")
        for row in top_candidates
        if isinstance(row, dict)
    )
    selected_probes = []
    base_evidence = {
        "cluster": "momentum_fading_or_negative_trend",
        "event_count": safe_int(cluster_row.get("event_count") or opportunity.get("event_count"), 0),
        "time_to_sustained_peak_sec_median": median_peak_lag,
        "recheck_window_classification": recheck_window,
    }
    if recheck_window in {
        "RECHECK_WINDOW_EXISTS_BEFORE_SUSTAINED_PEAK",
        "SHORT_RECHECK_WINDOW_BEFORE_SUSTAINED_PEAK",
    }:
        selected_probes.append({
            "probe_id": "pending_momentum_decay:timeboxed_recheck_window",
            "expected_capture_stage_improved": "final_eligibility",
            "required_features": [
                "pending_entry_ts",
                "momentum_decay_reason",
                "recheck_ts",
                "time_to_sustained_peak_sec",
            ],
            "next_action": "track_timeboxed_momentum_decay_recheck_shadow_only",
            "evidence": base_evidence,
        })
    if top_families.get("kline"):
        selected_probes.append({
            "probe_id": "pending_momentum_decay:kline_confirmation_recheck",
            "expected_capture_stage_improved": "final_eligibility",
            "required_features": [
                "pending_entry_ts",
                "momentum_decay_reason",
                "kline_candidate_id",
                "feature_available_at_ts",
            ],
            "next_action": "track_kline_confirmed_momentum_decay_recheck_shadow_only",
            "evidence": {**base_evidence, "kline_candidate_family_count": top_families.get("kline")},
        })
    if top_families.get("entry_mode_registry"):
        selected_probes.append({
            "probe_id": "pending_momentum_decay:entry_mode_registry_recheck",
            "expected_capture_stage_improved": "final_eligibility",
            "required_features": [
                "pending_entry_ts",
                "momentum_decay_reason",
                "entry_mode_candidate_id",
                "lifecycle_profile",
            ],
            "next_action": "track_entry_mode_momentum_decay_recheck_shadow_only",
            "evidence": {
                **base_evidence,
                "entry_mode_registry_candidate_family_count": top_families.get("entry_mode_registry"),
            },
        })

    for probe in selected_probes:
        probe.update({
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "human_approval_required_if_fix_requires": (
                "changing momentum/trend thresholds, entry policy, final_entry_contract, "
                "A_CLASS mode, executor, paper enablement, or risk"
            ),
        })

    return {
        "schema_version": "pending_momentum_decay_review.v1",
        "report_type": "pending_momentum_decay_review",
        "evidence_level": "discovery_same_window_shadow_timing_attribution",
        "cluster": "momentum_fading_or_negative_trend",
        "event_count": base_evidence["event_count"],
        "unique_tokens": opportunity.get("unique_tokens"),
        "share_of_raw_all_gold_silver": opportunity.get("share_of_raw_all_gold_silver"),
        "share_of_quality_timing_rejects": opportunity.get("share_of_quality_timing_rejects"),
        "time_to_sustained_peak_sec_median": median_peak_lag,
        "max_sustained_peak_pct_max": opportunity.get("max_sustained_peak_pct_max"),
        "example_count": len(examples),
        "example_peak_lag_band_counts": dict(band_counts),
        "recheck_window_classification": recheck_window,
        "top_candidates": top_candidates[:12],
        "top_families": [
            {"family": family, "count": count}
            for family, count in top_families.most_common()
        ],
        "top_lifecycle_source_contexts": (
            opportunity.get("top_lifecycle_source_contexts")
            or cluster_row.get("top_lifecycle_source_contexts")
            or []
        )[:8],
        "context_blockers": quality_timing_context_blockers(opportunity, context_eligibility)
        if opportunity
        else [],
        "selected_shadow_probes": selected_probes,
        "next_action": next_action,
        "interpretation": (
            "This only identifies whether momentum-fading gold/silver misses had a possible shadow recheck window. "
            "It does not prove the original momentum/trend reject was wrong or safe to trade."
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def build_stale_before_final_review(pending_audit, reports, context_eligibility):
    """Decompose the stale/timing pending->final bucket using read-only QT evidence."""

    category_counts = (
        (pending_audit.get("pending_no_final_entry_classification") or {}).get("categories")
        or {}
    )
    stale_count = safe_int(category_counts.get("stale_before_final"), 0)
    pending_without_final = safe_int(
        ((pending_audit.get("dropoff_counts") or {}).get("pending_no_final_entry")),
        0,
    )
    transition_review = pending_audit.get("largest_transition_dropoff_review") or {}
    adjacent_drop = safe_int(transition_review.get("adjacent_count_loss_pending_to_final"), 0)

    quality_timing = reports.get("quality_timing_reject_research") or {}
    reason_rows = (quality_timing.get("stage_attribution") or {}).get("reason_counts") or []
    opportunity_by_cluster = {
        row.get("cluster"): row
        for row in ((quality_timing.get("shadow_only_review") or {}).get("top_research_opportunities") or [])
        if isinstance(row, dict) and row.get("cluster")
    }
    cluster_counts = {}
    cluster_reasons = {}
    for row in reason_rows:
        if row.get("stage") != "pending_without_final_entry_contract":
            continue
        cluster = classify_quality_timing_reason_cluster(row)
        count = safe_int(row.get("count"), 0)
        cluster_counts[cluster] = cluster_counts.get(cluster, 0) + count
        cluster_reasons.setdefault(cluster, []).append(row)

    clusters = []
    for cluster, count in sorted(
        cluster_counts.items(),
        key=lambda item: (safe_int(item[1], 0), str(item[0])),
        reverse=True,
    ):
        if count <= 0:
            continue
        action = quality_timing_cluster_action(cluster)
        opportunity = opportunity_by_cluster.get(cluster) or {}
        clusters.append({
            "cluster": cluster,
            "event_count": count,
            "share_of_stale_before_final": rate(count, stale_count),
            "share_of_pending_without_final": rate(count, pending_without_final),
            "share_of_adjacent_transition_dropoff_upper_bound": rate(
                min(count, adjacent_drop or stale_count),
                adjacent_drop or stale_count,
            ),
            "reason_counts": (cluster_reasons.get(cluster) or [])[:10],
            "top_candidates": (opportunity.get("top_candidates") or [])[:8],
            "top_lifecycle_source_contexts": (
                opportunity.get("top_lifecycle_source_contexts") or []
            )[:8],
            "context_blockers": quality_timing_context_blockers(opportunity, context_eligibility)
            if opportunity
            else [],
            "next_action": action["next_action"],
            "human_approval_required_if_fix_requires": action[
                "human_approval_required_if_fix_requires"
            ],
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })

    selected = []
    cumulative = 0
    for row in clusters:
        if stale_count and cumulative >= stale_count:
            break
        contribution = min(
            safe_int(row.get("event_count"), 0),
            max(0, stale_count - cumulative),
        )
        cumulative += contribution
        selected.append({
            "cluster": row.get("cluster"),
            "event_count": row.get("event_count"),
            "events_contributing_to_stale_before_final_upper_bound": contribution,
            "cumulative_events_contributing_to_stale_before_final_upper_bound": cumulative,
            "next_action": row.get("next_action"),
            "context_blockers": row.get("context_blockers") or [],
            "human_approval_required_if_fix_requires": row.get(
                "human_approval_required_if_fix_requires"
            ),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })

    examples = []
    selected_clusters = {row.get("cluster") for row in selected}
    for example in quality_timing.get("top_examples") or []:
        if not isinstance(example, dict):
            continue
        if example.get("stage") != "pending_without_final_entry_contract":
            continue
        if selected_clusters and example.get("shadow_review_cluster") not in selected_clusters:
            continue
        examples.append(example)
        if len(examples) >= 8:
            break

    momentum_decay_review = build_pending_momentum_decay_review(
        clusters,
        quality_timing,
        context_eligibility,
    )
    return {
        "schema_version": "pending_to_final_stale_before_final_review.v1",
        "report_type": "pending_to_final_stale_before_final_review",
        "evidence_level": "discovery_same_window_quality_timing_attribution",
        "usage": "read_only_shadow_timing_attribution",
        "stale_before_final_event_count": stale_count,
        "pending_without_final_entry_contract": pending_without_final,
        "adjacent_count_loss_pending_to_final": adjacent_drop,
        "quality_timing_pending_without_final_event_count": sum(cluster_counts.values()),
        "cluster_count": len(clusters),
        "clusters": clusters,
        "selected_clusters_to_cover_stale_before_final_upper_bound": selected,
        "selected_upper_bound_event_count": cumulative,
        "covers_stale_before_final_upper_bound": cumulative >= stale_count if stale_count else True,
        "top_examples": examples,
        "momentum_decay_review": momentum_decay_review,
        "next_action": (
            momentum_decay_review.get("next_action")
            if momentum_decay_review.get("event_count")
            else selected[0]["next_action"]
            if selected
            else "continue_pending_stale_before_final_monitoring"
        ),
        "interpretation": (
            "Upper-bound attribution only. These are pending gold/silver events whose quality/timing state "
            "worsened before final_entry_contract. This does not prove the rejects were wrong or safe to trade."
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def dimension_status_from_rate(value, *, pending=False, writer_bug=False, not_applicable=False):
    if not_applicable:
        return STATUS_NA
    if writer_bug:
        return STATUS_WRITER_BUG
    if pending:
        return STATUS_PENDING
    if value is None:
        return STATUS_BLOCKED
    return STATUS_CLEAN if value >= 0.8 else STATUS_BLOCKED


def build_context_dimension_eligibility(reports):
    capture = reports.get("capture") or {}
    context_report = reports.get("context_coverage") or {}
    context_monitor = reports.get("context_blocker_monitor") or {}
    volume_kline = reports.get("volume_kline_coverage_audit") or {}
    markov = reports.get("markov_effectiveness") or {}
    strategy_memory = reports.get("strategy_memory_validation") or {}
    report_health = capture.get("report_health") or {}
    blockers = set(report_health.get("promotion_blockers") or [])
    blockers.update(context_report.get("blockers") or [])
    quote_cov = context_report.get("quote_context_coverage") or capture.get("quote_context_coverage") or {}
    quote_root = context_report.get("quote_missing_root_cause") or capture.get("quote_missing_root_cause") or {}
    monitor_overall = context_monitor.get("overall_verdict") or {}
    quote_smoke = context_monitor.get("task_a_post_deploy_quote_smoke_test") or {}
    clean_window = context_monitor.get("task_b_clean_window_monitor") or {}
    field_smoke = context_monitor.get("task_e_post_deploy_context_field_smoke_test") or {}
    quote_rate = min(
        safe_float(quote_cov.get("source_quote_clean_present_rate"), 0),
        safe_float(quote_cov.get("source_quote_executable_present_rate"), 0),
    )
    quote_writer_verified = (
        monitor_overall.get("quote_writer_fix") == "VERIFIED_POST_DEPLOY"
        or quote_smoke.get("classification") == "VERIFIED_POST_DEPLOY"
    )
    quote_monitor_pending = (
        monitor_overall.get("rolling24_quote_status") == "QUOTE_CLEAN_WINDOW_PENDING"
        or clean_window.get("classification") == "QUOTE_CLEAN_WINDOW_PENDING"
    )
    quote_writer_bug = quote_root.get("dominant_root_cause") == "v2_writer_path_missing_quote_fields"
    quote_pending_reconciled = bool(quote_monitor_pending and quote_rate >= 0.8 and not quote_writer_bug)
    quote_pending = bool(
        (quote_monitor_pending and quote_rate < 0.8)
        or (quote_writer_verified and quote_rate < 0.8)
    )
    field_writer_verified = field_smoke.get("classification") == "VERIFIED_POST_DEPLOY" or monitor_overall.get("context_field_writer_fix") == "VERIFIED_POST_DEPLOY"

    source_rate = coverage_rate(context_report.get("source_component_coverage") or {})
    lifecycle_rate = coverage_rate(context_report.get("lifecycle_profile_coverage") or {})
    volume_context = volume_kline.get("volume_context") or {}
    raw_gs_kline = volume_kline.get("raw_gold_silver_kline") or {}
    volume_rate = coverage_rate(volume_context) if volume_context else coverage_rate(context_report.get("volume_profile_coverage") or {})
    kline_rate = (
        safe_float(raw_gs_kline.get("kline_coverage_rate"))
        if raw_gs_kline
        else coverage_rate(context_report.get("kline_coverage") or {})
    )
    markov_diag = markov.get("profile_diagnostics") or {}
    markov_informative_profiles = {
        name: row for name, row in markov_diag.items()
        if (row or {}).get("informative_bucket_count", 0)
    }
    markov_blocked_profiles = {
        name: reason for name, reason in (markov.get("non_informative_reasons") or {}).items()
        if reason
    }
    markov_context_blockers = list(markov.get("context_blockers") or [])
    markov_informative = bool(markov_informative_profiles)
    markov_blocked = bool(markov_context_blockers) or (bool(markov_blocked_profiles) and not markov_informative)
    quote_status = dimension_status_from_rate(quote_rate, pending=quote_pending, writer_bug=quote_writer_bug)
    quote_blockers = sorted(blocker for blocker in blockers if "quote" in blocker or "schema" in blocker)
    quote_evidence = dict(quote_cov)
    quote_evidence.update({
        "quote_writer_verified": bool(quote_writer_verified),
        "quote_monitor_pending": bool(quote_monitor_pending),
        "quote_clean_window_pending_reconciled_by_current_coverage": quote_pending_reconciled,
        "active_quote_context_blockers": quote_blockers if quote_status != STATUS_CLEAN else [],
        "stale_or_reconciled_quote_context_blockers": quote_blockers if quote_status == STATUS_CLEAN else [],
    })
    volume_blockers = set(blocker for blocker in blockers if "volume" in blocker)
    if volume_context.get("blocker"):
        volume_blockers.add(volume_context.get("blocker"))
    volume_evidence = dict(context_report.get("volume_profile_coverage") or {})
    if volume_context:
        volume_evidence.update({
            "context_field_present_rate": (context_report.get("volume_profile_coverage") or {}).get("coverage_rate"),
            "field_present_rate": volume_context.get("field_present_rate"),
            "known_rate": volume_context.get("known_rate"),
            "unknown_rate": volume_context.get("unknown_rate"),
            "missing_rate": volume_context.get("missing_rate"),
            "not_applicable_rate": volume_context.get("not_applicable_rate"),
            "root_causes": volume_context.get("root_causes") or [],
            "unknown_diagnostics": volume_context.get("unknown_diagnostics") or {},
            "coverage_denominator_type": volume_context.get("coverage_denominator_type"),
            "blocker": volume_context.get("blocker"),
        })
    kline_blockers = set(blocker for blocker in blockers if "kline" in blocker)
    if raw_gs_kline.get("blocker"):
        kline_blockers.add(raw_gs_kline.get("blocker"))
    kline_evidence = dict(context_report.get("kline_coverage") or {})
    if raw_gs_kline:
        kline_evidence.update({
            "coverage_denominator_type": raw_gs_kline.get("coverage_denominator_type") or "raw_all_gold_silver",
            "raw_all_gold_silver_event_rows": raw_gs_kline.get("raw_all_gold_silver_event_rows"),
            "kline_covered_rows": raw_gs_kline.get("kline_covered_rows"),
            "kline_uncovered_rows": raw_gs_kline.get("kline_uncovered_rows"),
            "coverage_rate": raw_gs_kline.get("kline_coverage_rate"),
            "kline_coverage_pct": raw_gs_kline.get("kline_coverage_pct"),
            "blocker": raw_gs_kline.get("blocker"),
            "kline_uncovered_root_cause_counts": raw_gs_kline.get("kline_uncovered_root_cause_counts") or {},
            "low_confidence_research_audit": raw_gs_kline.get("low_confidence_research_audit") or {},
        })
    dimensions = {
        "quote-sensitive": {
            "status": quote_status,
            "eligible_for_capture_cross": quote_rate >= 0.8 and not quote_writer_bug,
            "coverage_rate": quote_rate,
            "blockers": quote_blockers if quote_status != STATUS_CLEAN else [],
            "evidence": quote_evidence,
        },
        "source_component": {
            "status": dimension_status_from_rate(source_rate, pending=field_writer_verified and source_rate is not None and source_rate < 0.8),
            "eligible_for_capture_cross": source_rate is not None and source_rate >= 0.8,
            "coverage_rate": source_rate,
            "blockers": sorted(blocker for blocker in blockers if "source_component" in blocker),
            "evidence": context_report.get("source_component_coverage") or {},
        },
        "lifecycle": {
            "status": dimension_status_from_rate(lifecycle_rate, pending=field_writer_verified and lifecycle_rate is not None and lifecycle_rate < 0.8),
            "eligible_for_capture_cross": lifecycle_rate is not None and lifecycle_rate >= 0.8,
            "coverage_rate": lifecycle_rate,
            "blockers": sorted(blocker for blocker in blockers if "lifecycle" in blocker),
            "evidence": context_report.get("lifecycle_profile_coverage") or {},
        },
        "volume": {
            "status": dimension_status_from_rate(volume_rate),
            "eligible_for_capture_cross": volume_rate is not None and volume_rate >= 0.8,
            "coverage_rate": volume_rate,
            "blockers": sorted(volume_blockers),
            "evidence": volume_evidence,
        },
        "kline": {
            "status": dimension_status_from_rate(kline_rate),
            "eligible_for_capture_cross": kline_rate is not None and kline_rate >= 0.8,
            "coverage_rate": kline_rate,
            "blockers": sorted(kline_blockers),
            "evidence": kline_evidence,
        },
        "Markov": {
            "status": STATUS_CLEAN if markov_informative and not markov_blocked else (STATUS_NA if not markov else STATUS_BLOCKED),
            "eligible_for_capture_cross": bool(markov_informative and not markov_blocked),
            "coverage_rate": None,
            "blockers": sorted(set(markov_context_blockers if markov_blocked else [])),
            "evidence": {
                "classification": markov.get("classification"),
                "profile_count": len(markov_diag),
                "informative_profile_count": len(markov_informative_profiles),
                "informative_profiles": sorted(markov_informative_profiles),
                "non_informative_reasons": markov_blocked_profiles,
                "partial_profile_blockers": markov_blocked_profiles if markov_informative else {},
                "status_note": (
                    "informative_profiles_available_discovery_only"
                    if markov_informative and markov_blocked_profiles and not markov_context_blockers
                    else None
                ),
            },
        },
        "Strategy Memory": {
            "status": STATUS_CLEAN if strategy_memory.get("strategy_memory_enabled") or strategy_memory.get("hypotheses_count") else STATUS_NA,
            "eligible_for_capture_cross": bool(strategy_memory.get("strategy_memory_enabled") or strategy_memory.get("hypotheses_count")),
            "coverage_rate": None,
            "blockers": [],
            "evidence": {
                "hypotheses_count": strategy_memory.get("hypotheses_count"),
                "status_counts": strategy_memory.get("status_counts") or {},
                "promotion_allowed": False,
            },
        },
    }
    status_counts = Counter(row["status"] for row in dimensions.values())
    return {
        "schema_version": "context_dimension_eligibility.v1",
        "report_type": "context_dimension_eligibility",
        "generated_at": utc_now(),
        "promotion_allowed": False,
        "status_counts": dict(status_counts),
        "dimensions": dimensions,
        "clean_dimensions": [name for name, row in dimensions.items() if row["status"] == STATUS_CLEAN],
        "blocked_dimensions": [name for name, row in dimensions.items() if row["status"] not in {STATUS_CLEAN, STATUS_NA}],
        "rule": "Only dimensions with status CLEAN may contribute capture-cross or OOS evidence.",
    }


def build_capture_60_gap_report(stage_metrics, context_eligibility, pending_audit):
    stage_counts = stage_metrics["stage_counts"]
    biggest_gap_stage, additional_needed = first_stage_below_target(stage_counts)
    interpretation = gap_interpretation(stage_metrics, biggest_gap_stage, additional_needed, pending_audit)
    return {
        "schema_version": "capture_60_gap_report.v1",
        "report_type": "capture_60_gap_report",
        "generated_at": utc_now(),
        "phase": "discovery_readiness",
        "target_capture_rate": TARGET_RATE,
        "promotion_allowed": False,
        "raw_gold_silver_denominator": stage_counts.get("raw_gold_silver_denominator"),
        "target_60_count": stage_counts.get("target_60_count"),
        "detector_capture_count": stage_counts["detector_capture"]["count"],
        "detector_capture_rate": stage_counts["detector_capture"]["rate"],
        "decision_capture_count": stage_counts["decision_capture"]["count"],
        "decision_capture_rate": stage_counts["decision_capture"]["rate"],
        "pass_allow_capture_count": stage_counts["pass_allow_capture"]["count"],
        "pass_allow_capture_rate": stage_counts["pass_allow_capture"]["rate"],
        "pending_capture_count": stage_counts["pending_capture"]["count"],
        "pending_capture_rate": stage_counts["pending_capture"]["rate"],
        "final_eligibility_count": stage_counts["final_eligibility"]["count"],
        "final_eligibility_rate": stage_counts["final_eligibility"]["rate"],
        "mode_disabled_adjusted_final_eligibility_count": stage_counts["mode_disabled_adjusted_final_eligibility"]["count"],
        "mode_disabled_adjusted_final_eligibility_rate": stage_counts["mode_disabled_adjusted_final_eligibility"]["rate"],
        "paper_capture_count": stage_counts["paper_capture"]["count"],
        "paper_capture_rate": stage_counts["paper_capture"]["rate"],
        "realized_capture_count": stage_counts["realized_capture"]["count"],
        "realized_capture_rate": stage_counts["realized_capture"]["rate"],
        "biggest_gap_stage": biggest_gap_stage,
        "target_shortfall_stage": biggest_gap_stage,
        "largest_stage_dropoff": stage_metrics.get("largest_stage_dropoff") or {},
        "largest_transition_dropoff": stage_metrics.get("largest_stage_dropoff") or {},
        "additional_count_needed_to_60": additional_needed,
        "next_best_allowed_action": next_best_allowed_action(biggest_gap_stage, context_eligibility, pending_audit),
        "gap_interpretation": interpretation,
        "recommended_parallel_tracks": interpretation.get("tracks") or [],
        "human_approval_required_before_runtime_change": True,
        "notes": [
            "This is a target-gap report, not a promotion report.",
            "target_shortfall_stage is the first stage below 60%; largest_transition_dropoff is the biggest adjacent funnel loss. They can differ and should be audited separately.",
            "All suggested actions are constrained to evaluator, data, shadow-only, or human-approval handoff paths.",
        ],
    }


def count_stage(rows, stage_name):
    return sum(
        safe_int(row.get("count"), 0)
        for row in rows or []
        if row.get("stage") == stage_name
    )


def top_stage_reasons(rows, stage_name, limit=12):
    return [
        row for row in rows or []
        if row.get("stage") == stage_name
    ][:limit]


def classify_quality_timing_reason_cluster(row):
    component = str((row or {}).get("component") or "").lower()
    reason = str((row or {}).get("reason") or "").lower()
    decision = str((row or {}).get("decision") or "").lower()
    event_type = str((row or {}).get("event_type") or "").lower()
    stage = str((row or {}).get("stage") or "").lower()
    text = " ".join([stage, component, event_type, decision, reason])
    if "matrix" in component or "matrices not yet aligned" in reason:
        return "matrix_alignment_wait"
    if "lotto_observe_low_mc_vol" in reason or "low_volume" in reason or "low_vol" in reason:
        return "low_volume_observe"
    if "risky_newborn_pullback" in reason:
        return "newborn_pullback_timing_reject"
    if "not_ath" in reason:
        return "notath_upstream_skip"
    if "buy_pressure" in reason or "weak_buying_pressure" in reason:
        return "buy_pressure_weak"
    if "chasing_top" in reason:
        return "chasing_top_timing_reject"
    if "score_too_low" in reason or "quality_score" in reason:
        return "score_or_quality_too_low"
    if "negative_trend" in reason or "momentum_fading" in reason:
        return "momentum_fading_or_negative_trend"
    if "entry_node_timeout" in reason or "retry_watch_scheduled" in reason:
        return "entry_timing_timeout_or_retry"
    if "final_entry" in component or "final_entry" in reason:
        return "final_entry_contract_research_block"
    if "top10_pct" in reason or "concentration" in reason:
        return "holder_concentration_quality_reject"
    if text.strip():
        return "other_quality_timing_reject"
    return "unknown_quality_timing_reject"


def stage_count(row, stage_name):
    return sum(
        safe_int(item.get("count"), 0)
        for item in (row or {}).get("stage_counts") or []
        if item.get("stage") == stage_name
    )


def build_pass_allow_capture_gap_audit(stage_metrics, pending_audit, reports, context_eligibility):
    """Explain the current gap from raw gold/silver events to pass/allow.

    This is deliberately an audit artifact, not a policy artifact. It merges
    the shadow decision bridge report with decision-no-pass quality/timing
    evidence so the loop can target the first below-60% stage without touching
    production decision logic.
    """
    stage_counts = stage_metrics.get("stage_counts") or {}
    raw_den = safe_int(stage_counts.get("raw_gold_silver_denominator"), 0)
    target = safe_int(stage_counts.get("target_60_count"), 0)
    pass_allow_count = safe_int((stage_counts.get("pass_allow_capture") or {}).get("count"), 0)
    additional_needed = max(0, target - pass_allow_count)
    pending_dropoff = pending_audit.get("dropoff_counts") or {}
    upstream = pending_audit.get("upstream_funnel_gap") or {}

    shadow_bridge = reports.get("shadow_decision_bridge") or {}
    shadow_den = shadow_bridge.get("denominator") or {}
    shadow_bridge_count = safe_int(
        shadow_den.get("shadow_entry_hypotheses_matched_no_decision_bridge"),
        0,
    )
    shadow_review_items = [
        {
            "candidate_id": row.get("candidate_id"),
            "candidate_family": row.get("family"),
            "event_count": row.get("count"),
            "status": "SHADOW_MATCHED_NO_DECISION_BRIDGE",
            "next_action": "review_shadow_decision_bridge_instrumentation_without_entry_policy_change",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        }
        for row in (shadow_bridge.get("candidate_counts") or [])[:12]
        if isinstance(row, dict)
    ]

    quality_timing = reports.get("quality_timing_reject_research") or {}
    stage_attribution = quality_timing.get("stage_attribution") or {}
    qt_stage_rows = stage_attribution.get("stage_counts") or []
    qt_reason_rows = stage_attribution.get("reason_counts") or []
    qt_review = (quality_timing.get("shadow_only_review") or {})
    qt_decision_no_pass = count_stage(qt_stage_rows, "decision_no_pass_or_allow")
    qt_pass_no_pending = count_stage(qt_stage_rows, "pass_or_allow_without_pending_entry")
    qt_pending_no_final = count_stage(qt_stage_rows, "pending_without_final_entry_contract")
    qt_review_items = []
    for row in qt_review.get("top_research_opportunities") or []:
        if not isinstance(row, dict):
            continue
        dominant_stage = (row.get("stage_counts") or [{}])[0].get("stage")
        if dominant_stage not in {"decision_no_pass_or_allow", "pass_or_allow_without_pending_entry"}:
            continue
        qt_review_items.append({
            "cluster": row.get("cluster"),
            "dominant_stage": dominant_stage,
            "event_count": row.get("event_count"),
            "share_of_raw_all_gold_silver": row.get("share_of_raw_all_gold_silver"),
            "unique_tokens": row.get("unique_tokens"),
            "top_candidates": (row.get("top_candidates") or [])[:5],
            "top_lifecycle_source_contexts": (row.get("top_lifecycle_source_contexts") or [])[:5],
            "suggested_shadow_only_action": row.get("suggested_shadow_only_action"),
            "human_approval_required_if_fix_requires": row.get("human_approval_required_if_fix_requires"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })

    candidate_downstream = reports.get("candidate_downstream") or {}
    downstream_candidates = []
    for row in candidate_downstream.get("top_candidates") or []:
        if not isinstance(row, dict):
            continue
        downstream_candidates.append({
            "candidate_id": row.get("candidate_id"),
            "candidate_family": row.get("family"),
            "classification": row.get("classification"),
            "matched_raw_gs_signals": row.get("matched_raw_gs_signals"),
            "raw_gs_recall": row.get("raw_gs_recall"),
            "match_precision": row.get("match_precision"),
            "decision_record_rate_after_match": row.get("decision_record_rate_after_match"),
            "pass_allow_rate_after_match": row.get("pass_allow_rate_after_match"),
            "pending_rate_after_match": row.get("pending_rate_after_match"),
            "mode_disabled_adjusted_final_eligibility_rate_after_match": (
                row.get("mode_disabled_adjusted_final_eligibility_rate_after_match")
            ),
            "promotion_allowed": False,
        })

    potential_sources = {
        "shadow_matched_no_decision_bridge_upper_bound": shadow_bridge_count,
        "decision_no_pass_or_allow_upper_bound": safe_int(
            upstream.get("decision_no_pass_or_allow")
            or pending_dropoff.get("decision_no_pass_allow"),
            0,
        ),
        "quality_timing_decision_no_pass_or_allow_upper_bound": qt_decision_no_pass,
        "pass_allow_without_pending_not_part_of_pass_allow_shortfall": safe_int(
            upstream.get("pass_or_allow_without_pending_entry")
            or pending_dropoff.get("pass_allow_no_pending"),
            0,
        ),
    }
    explained_upper_bound = (
        potential_sources["shadow_matched_no_decision_bridge_upper_bound"]
        + potential_sources["decision_no_pass_or_allow_upper_bound"]
    )
    if shadow_bridge_count and shadow_bridge.get("status") != "SHADOW_DECISION_BRIDGE_MIRROR_COMPLETE":
        next_action = "complete_shadow_decision_bridge_read_only_mirror"
    elif qt_decision_no_pass:
        next_action = "review_decision_no_pass_quality_timing_clusters_shadow_only"
    elif potential_sources["decision_no_pass_or_allow_upper_bound"]:
        next_action = "decompose_decision_no_pass_reasons_shadow_only"
    else:
        next_action = "continue_pass_allow_capture_monitoring"

    if additional_needed <= 0:
        classification = "PASS_ALLOW_CAPTURE_60_REACHED_DISCOVERY_ONLY"
        dominant_gap_stage = None
        dominant_blocker = None
        next_action = "pass_allow_capture_target_reached_continue_downstream_gap_audit"
    elif shadow_bridge_count and shadow_bridge.get("status") != "SHADOW_DECISION_BRIDGE_MIRROR_COMPLETE":
        classification = "PASS_ALLOW_CAPTURE_GAP_SHADOW_DECISION_BRIDGE_INCOMPLETE"
        dominant_gap_stage = "decision_capture_to_pass_allow_capture"
        dominant_blocker = "shadow_matched_no_decision_bridge"
    elif qt_decision_no_pass:
        classification = "PASS_ALLOW_CAPTURE_GAP_DECISION_NO_PASS_QUALITY_TIMING"
        dominant_gap_stage = "decision_capture_to_pass_allow_capture"
        dominant_blocker = "quality_timing_decision_no_pass_or_allow"
    elif potential_sources["decision_no_pass_or_allow_upper_bound"]:
        classification = "PASS_ALLOW_CAPTURE_GAP_DECISION_NO_PASS_REASON_DECOMPOSITION_NEEDED"
        dominant_gap_stage = "decision_capture_to_pass_allow_capture"
        dominant_blocker = "decision_no_pass_or_allow"
    else:
        classification = "PASS_ALLOW_CAPTURE_GAP_UNATTRIBUTED_MONITOR"
        dominant_gap_stage = "decision_capture_to_pass_allow_capture"
        dominant_blocker = "unattributed_pass_allow_shortfall"

    blocked_dimensions = context_eligibility.get("blocked_dimensions") or []
    clean_dimensions = context_eligibility.get("clean_dimensions") or []
    return {
        "schema_version": "pass_allow_capture_gap_audit.v1",
        "report_type": "pass_allow_capture_gap_audit",
        "generated_at": utc_now(),
        "classification": classification,
        "dominant_gap_stage": dominant_gap_stage,
        "dominant_blocker": dominant_blocker,
        "next_action": next_action,
        "phase": "discovery_readiness",
        "evidence_level": "discovery_same_window",
        "usage": "read_only_pass_allow_gap_targeting",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "runtime_effect": "none",
        "target_gap": {
            "raw_gold_silver_denominator": raw_den,
            "target_capture_rate": TARGET_RATE,
            "target_60_count": target,
            "current_pass_allow_count": pass_allow_count,
            "current_pass_allow_rate": (stage_counts.get("pass_allow_capture") or {}).get("rate"),
            "additional_pass_allow_events_needed_to_60": additional_needed,
        },
        "dropoff_counts": {
            "no_decision": safe_int(
                upstream.get("no_decision_record") or pending_dropoff.get("no_decision"),
                0,
            ),
            "decision_no_pass_allow": potential_sources["decision_no_pass_or_allow_upper_bound"],
            "pass_allow_no_pending": potential_sources["pass_allow_without_pending_not_part_of_pass_allow_shortfall"],
            "pending_no_final_entry": safe_int(pending_dropoff.get("pending_no_final_entry"), 0),
            "final_entry_no_paper": safe_int(pending_dropoff.get("final_entry_no_paper"), 0),
        },
        "gap_source_upper_bounds": potential_sources,
        "gap_explainability": {
            "combined_shadow_bridge_and_decision_no_pass_upper_bound": explained_upper_bound,
            "covers_current_pass_allow_gap_if_all_resolved": explained_upper_bound >= additional_needed if additional_needed else True,
            "events_contributing_to_60pct_gap_upper_bound": min(explained_upper_bound, additional_needed),
            "residual_pass_allow_gap_after_upper_bound": max(0, additional_needed - explained_upper_bound),
            "interpretation": (
                "Upper bound only. It does not prove any rejected or missing-decision event should be traded."
            ),
        },
        "shadow_decision_bridge": {
            "status": shadow_bridge.get("status"),
            "root_cause": shadow_bridge.get("root_cause"),
            "shadow_bridge_count": shadow_bridge_count,
            "mirror_event_coverage_vs_shadow_bridge_gap": shadow_den.get("mirror_event_coverage_vs_shadow_bridge_gap"),
            "mirror_event_truncated": shadow_den.get("mirror_event_truncated"),
            "review_queue": shadow_review_items,
        },
        "decision_no_pass_quality_timing": {
            "quality_timing_report_verdict": quality_timing.get("verdict"),
            "quality_timing_decision_no_pass_or_allow_events": qt_decision_no_pass,
            "quality_timing_pass_or_allow_without_pending_events": qt_pass_no_pending,
            "quality_timing_pending_without_final_events": qt_pending_no_final,
            "decision_no_pass_reason_counts": top_stage_reasons(
                qt_reason_rows,
                "decision_no_pass_or_allow",
            ),
            "pass_allow_without_pending_reason_counts": top_stage_reasons(
                qt_reason_rows,
                "pass_or_allow_without_pending_entry",
            ),
            "review_queue": qt_review_items[:12],
        },
        "candidate_downstream_watch": downstream_candidates[:12],
        "context_constraints": {
            "clean_dimensions": clean_dimensions,
            "blocked_dimensions": blocked_dimensions,
            "blocked_dimensions_excluded_from_this_audit": blocked_dimensions,
            "rule": "Do not use blocked quote/kline/Markov dimensions for promotion or OOS evidence.",
        },
        "allowed_scope": [
            "read-only evaluator/report improvements",
            "shadow-only candidate or context instrumentation",
            "hypothesis registry entries for clean-window/OOS validation",
        ],
        "forbidden_scope": [
            "strategy change",
            "entry policy change",
            "hard gate relaxation",
            "exit gate change",
            "final_entry_contract change",
            "A_CLASS mode reset or enablement",
            "paper/live executor enablement",
            "canary or risk increase",
        ],
        "notes": [
            "This audit targets the first below-60% stage: pass_allow_capture.",
            "All counts are discovery/readiness evidence. No row authorizes a runtime policy change.",
        ],
    }


def build_decision_no_pass_quality_timing_review(stage_metrics, pass_allow_gap_audit, reports, context_eligibility):
    """Prioritize decision-no-pass quality/timing clusters against the 60% target.

    This narrows the previous quality/timing audit to the first below-target
    stage, pass_allow_capture. It provides a shadow-only review plan for the
    exact clusters that could close the current pass/allow gap if future clean
    windows and OOS validation prove they are false negatives.
    """
    quality_timing = reports.get("quality_timing_reject_research") or {}
    stage_attribution = quality_timing.get("stage_attribution") or {}
    reason_rows = [
        row for row in stage_attribution.get("reason_counts") or []
        if row.get("stage") == "decision_no_pass_or_allow"
    ]
    cluster_reason_rows = {}
    for row in reason_rows:
        cluster = classify_quality_timing_reason_cluster(row)
        cluster_reason_rows.setdefault(cluster, []).append(row)

    opportunities = {
        row.get("cluster"): row
        for row in ((quality_timing.get("shadow_only_review") or {}).get("top_research_opportunities") or [])
        if isinstance(row, dict) and row.get("cluster")
    }
    target_gap = pass_allow_gap_audit.get("target_gap") or {}
    additional_needed = safe_int(target_gap.get("additional_pass_allow_events_needed_to_60"), 0)
    raw_den = safe_int(target_gap.get("raw_gold_silver_denominator"), 0)
    rows = []
    for cluster in sorted(set(cluster_reason_rows) | set(opportunities)):
        opportunity = opportunities.get(cluster) or {}
        dnp_count = stage_count(opportunity, "decision_no_pass_or_allow")
        reason_count = sum(safe_int(row.get("count"), 0) for row in cluster_reason_rows.get(cluster) or [])
        event_count = dnp_count or reason_count
        if event_count <= 0:
            continue
        review_blockers = quality_timing_context_blockers(opportunity, context_eligibility) if opportunity else []
        rows.append({
            "cluster": cluster,
            "decision_no_pass_event_count": event_count,
            "share_of_raw_gold_silver": rate(event_count, raw_den),
            "share_of_current_pass_allow_gap_upper_bound": rate(
                min(event_count, additional_needed),
                additional_needed,
            ),
            "unique_tokens": opportunity.get("unique_tokens"),
            "candidate_matched_any_rate": opportunity.get("candidate_matched_any_rate"),
            "max_sustained_peak_pct_max": opportunity.get("max_sustained_peak_pct_max"),
            "time_to_sustained_peak_sec_median": opportunity.get("time_to_sustained_peak_sec_median"),
            "reason_counts": cluster_reason_rows.get(cluster) or [],
            "top_candidates": (opportunity.get("top_candidates") or [])[:8],
            "top_lifecycle_source_contexts": (opportunity.get("top_lifecycle_source_contexts") or [])[:8],
            "suggested_shadow_only_action": (
                opportunity.get("suggested_shadow_only_action")
                or "continue_reason_level_shadow_review"
            ),
            "human_approval_required_if_fix_requires": (
                opportunity.get("human_approval_required_if_fix_requires")
                or "changing strategy, entry policy, gate, final_entry, or runtime behavior"
            ),
            "context_blockers": review_blockers,
            "review_status": "SHADOW_REVIEW_READY" if not review_blockers else "BLOCKED_CONTEXT_COVERAGE",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    rows = sorted(
        rows,
        key=lambda row: (
            safe_int(row.get("decision_no_pass_event_count"), 0),
            safe_float(row.get("candidate_matched_any_rate"), 0) or 0,
            str(row.get("cluster") or ""),
        ),
        reverse=True,
    )
    cumulative = 0
    selected = []
    for row in rows:
        if additional_needed and cumulative >= additional_needed:
            break
        contribution = min(
            safe_int(row.get("decision_no_pass_event_count"), 0),
            max(0, additional_needed - cumulative),
        )
        cumulative += contribution
        selected.append({
            "cluster": row.get("cluster"),
            "decision_no_pass_event_count": row.get("decision_no_pass_event_count"),
            "events_contributing_to_gap_upper_bound": contribution,
            "cumulative_events_contributing_to_gap_upper_bound": cumulative,
            "suggested_shadow_only_action": row.get("suggested_shadow_only_action"),
            "human_approval_required_if_fix_requires": row.get("human_approval_required_if_fix_requires"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    classification = (
        "DECISION_NO_PASS_QUALITY_TIMING_REVIEW_READY"
        if rows
        else "DECISION_NO_PASS_QUALITY_TIMING_REVIEW_EMPTY"
    )
    if rows and any(row.get("review_status") == "BLOCKED_CONTEXT_COVERAGE" for row in rows):
        classification = "DECISION_NO_PASS_QUALITY_TIMING_REVIEW_CONTEXT_BLOCKED"
    return {
        "schema_version": "decision_no_pass_quality_timing_review.v1",
        "report_type": "decision_no_pass_quality_timing_review",
        "generated_at": utc_now(),
        "phase": "discovery_readiness",
        "evidence_level": "discovery_same_window",
        "usage": "read_only_shadow_review_decision_no_pass_quality_timing",
        "classification": classification,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "runtime_effect": "none",
        "target_gap": target_gap,
        "decision_no_pass_quality_timing_event_count": sum(
            safe_int(row.get("decision_no_pass_event_count"), 0)
            for row in rows
        ),
        "cluster_count": len(rows),
        "clusters": rows,
        "selected_clusters_to_cover_current_pass_allow_gap_upper_bound": selected,
        "selected_cluster_count": len(selected),
        "selected_upper_bound_event_count": cumulative,
        "covers_current_pass_allow_gap_upper_bound": cumulative >= additional_needed if additional_needed else True,
        "context_constraints": {
            "clean_dimensions": context_eligibility.get("clean_dimensions") or [],
            "blocked_dimensions": context_eligibility.get("blocked_dimensions") or [],
            "rule": "Blocked quote/kline/Markov dimensions are excluded from this review.",
        },
        "next_action": (
            "track_selected_decision_no_pass_quality_timing_clusters_shadow_only_then_clean_window_oos"
            if rows
            else "continue_pass_allow_gap_monitoring"
        ),
        "allowed_scope": [
            "read-only evaluator/report improvements",
            "shadow-only candidate/context instrumentation",
            "hypothesis registry entries for clean-window/OOS validation",
        ],
        "forbidden_scope": [
            "strategy change",
            "entry policy change",
            "hard gate relaxation",
            "exit gate change",
            "final_entry_contract change",
            "A_CLASS mode reset or enablement",
            "paper/live executor enablement",
            "canary or risk increase",
        ],
        "notes": [
            "Upper-bound review only: selected clusters may explain the current pass_allow shortfall, but do not prove any runtime threshold should change.",
            "Any fix requiring strategy, gate, final_entry_contract, A_CLASS, executor, paper/live, or risk changes requires human approval.",
        ],
    }


def build_pass_allow_60_closure_plan(
    stage_metrics,
    pass_allow_gap_audit,
    decision_no_pass_review,
    shadow_queue,
    reports,
    context_eligibility,
):
    """Turn the pass/allow shortfall into a bounded shadow-only closure plan.

    This is intentionally narrower than the general improvement queue. It
    answers: given the current target gap, which read-only reviews or
    shadow-only probes could plausibly explain or cover the missing pass/allow
    events, and what must wait for clean-window/OOS validation?
    """
    stage_counts = stage_metrics.get("stage_counts") or {}
    target_gap = pass_allow_gap_audit.get("target_gap") or {}
    raw_den = safe_int(
        target_gap.get("raw_gold_silver_denominator")
        or stage_counts.get("raw_gold_silver_denominator"),
        0,
    )
    target_60 = safe_int(target_gap.get("target_60_count"), 0)
    current_pass_allow = safe_int(target_gap.get("current_pass_allow_count"), 0)
    additional_needed = safe_int(
        target_gap.get("additional_pass_allow_events_needed_to_60"),
        max(0, target_60 - current_pass_allow),
    )

    clusters_by_name = {
        row.get("cluster"): row
        for row in (decision_no_pass_review.get("clusters") or [])
        if isinstance(row, dict) and row.get("cluster")
    }
    selected_cluster_items = []
    cumulative = 0
    for row in decision_no_pass_review.get("selected_clusters_to_cover_current_pass_allow_gap_upper_bound") or []:
        if not isinstance(row, dict):
            continue
        cluster = row.get("cluster")
        full = clusters_by_name.get(cluster) or {}
        contribution = safe_int(row.get("events_contributing_to_gap_upper_bound"), 0)
        cumulative += contribution
        selected_cluster_items.append({
            "plan_item_id": f"decision_no_pass_quality_timing:{cluster}",
            "evidence_source": "decision_no_pass_quality_timing_review",
            "cluster": cluster,
            "expected_capture_stage_improved": "pass_allow_capture",
            "decision_no_pass_event_count": row.get("decision_no_pass_event_count"),
            "events_contributing_to_current_60pct_gap_upper_bound": contribution,
            "cumulative_events_contributing_to_current_60pct_gap_upper_bound": cumulative,
            "share_of_current_pass_allow_gap_upper_bound": rate(contribution, additional_needed),
            "candidate_matched_any_rate": full.get("candidate_matched_any_rate"),
            "unique_tokens": full.get("unique_tokens"),
            "top_candidates": (full.get("top_candidates") or [])[:5],
            "top_lifecycle_source_contexts": (full.get("top_lifecycle_source_contexts") or [])[:5],
            "context_blockers": full.get("context_blockers") or [],
            "status": "PENDING_CLEAN_WINDOW_THEN_OOS",
            "next_action": row.get("suggested_shadow_only_action")
            or "track_decision_no_pass_cluster_shadow_only_then_oos",
            "human_approval_required_if_fix_requires": row.get("human_approval_required_if_fix_requires"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })

    capture_cross = reports.get("capture_cross") or {}
    clean_cross_items = []
    for row in capture_cross.get("valid_top_crosses") or []:
        if not isinstance(row, dict):
            continue
        if row.get("data_blockers") or row.get("invalid_reasons"):
            continue
        if row.get("judgment") not in {"DISCOVERY_HIT", "WATCH"}:
            continue
        pass_allow_lift = safe_float(row.get("pass_allow_lift"), 0.0)
        if pass_allow_lift <= 0:
            continue
        clean_cross_items.append({
            "plan_item_id": (
                f"clean_2d:{row.get('candidate_id')}:{row.get('dimension')}={row.get('slice_value')}"
            ),
            "evidence_source": "capture_cross_validity_24h",
            "candidate_id": row.get("candidate_id"),
            "family": row.get("family"),
            "dimension": row.get("dimension"),
            "dimension_group": row.get("dimension_group"),
            "dimension_eligibility_status": row.get("dimension_eligibility_status"),
            "slice_value": row.get("slice_value"),
            "judgment": row.get("judgment"),
            "matched_gold_silver_events": row.get("matched_gold_silver_events"),
            "slice_downstream_signal_count": row.get("slice_downstream_signal_count"),
            "downstream_lift_scope": row.get("downstream_lift_scope"),
            "downstream_slice_join_available": row.get("downstream_slice_join_available"),
            "downstream_slice_join_required_for_promotion_evidence": (
                row.get("downstream_slice_join_required_for_promotion_evidence")
            ),
            "match_recall_event": row.get("match_recall_event"),
            "match_precision_event": row.get("match_precision_event"),
            "pass_allow_capture_rate_after_match": row.get("pass_allow_capture_rate_after_match"),
            "pass_allow_lift": row.get("pass_allow_lift"),
            "decision_capture_rate_after_match": row.get("decision_capture_rate_after_match"),
            "decision_lift": row.get("decision_lift"),
            "pending_capture_rate_after_match": row.get("pending_capture_rate_after_match"),
            "pending_lift": row.get("pending_lift"),
            "final_entry_rate_after_match": row.get("final_entry_rate_after_match"),
            "final_entry_lift": row.get("final_entry_lift"),
            "mode_adjusted_final_eligibility_rate_after_match": (
                row.get("mode_adjusted_final_eligibility_rate_after_match")
            ),
            "mode_adjusted_final_eligibility_lift": row.get("mode_adjusted_final_eligibility_lift"),
            "status": "DISCOVERY_SAME_WINDOW_ONLY",
            "next_action": "freeze_same_definition_for_clean_window_oos_if_repeated",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    clean_cross_items = sorted(
        clean_cross_items,
        key=lambda row: (
            safe_float(row.get("pass_allow_lift"), 0.0),
            safe_float(row.get("matched_gold_silver_events"), 0.0),
            safe_float(row.get("match_precision_event"), 0.0),
        ),
        reverse=True,
    )[:20]

    shadow_pass_allow_items = []
    for row in shadow_queue.get("top_items") or shadow_queue.get("top_opportunities") or []:
        if not isinstance(row, dict):
            continue
        if row.get("expected_capture_stage_improved") != "pass_allow_capture":
            continue
        evidence = row.get("evidence") or {}
        impact = evidence.get("readiness_impact_upper_bound") or {}
        shadow_pass_allow_items.append({
            "plan_item_id": f"shadow_queue:{row.get('candidate_id')}",
            "evidence_source": "shadow_candidate_improvement_queue",
            "candidate_id": row.get("candidate_id"),
            "hypothesis_source": row.get("hypothesis_source"),
            "expected_capture_stage_improved": row.get("expected_capture_stage_improved"),
            "event_count": evidence.get("event_count") or evidence.get("cluster_event_count"),
            "events_contributing_to_60pct_gap_upper_bound": (
                impact.get("events_contributing_to_60pct_gap_upper_bound")
            ),
            "context_blockers": row.get("context_blockers") or [],
            "required_features": row.get("required_features") or [],
            "time_legal_status": row.get("time_legal_status"),
            "next_action": row.get("next_action"),
            "human_approval_required_if_fix_requires": row.get("human_approval_required_if_fix_requires"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    shadow_pass_allow_items = shadow_pass_allow_items[:20]

    residual_gap_after_clusters = max(0, additional_needed - cumulative)
    clean_2d_non_dedup_upper_bound = sum(
        safe_int(row.get("matched_gold_silver_events"), 0)
        for row in clean_cross_items
    )
    clean_2d_max_single_slice = max(
        [safe_int(row.get("matched_gold_silver_events"), 0) for row in clean_cross_items] or [0]
    )
    shadow_queue_non_dedup_upper_bound = sum(
        safe_int(
            row.get("events_contributing_to_60pct_gap_upper_bound")
            or row.get("event_count"),
            0,
        )
        for row in shadow_pass_allow_items
    )
    supplemental_non_dedup_upper_bound = (
        clean_2d_non_dedup_upper_bound + shadow_queue_non_dedup_upper_bound
    )
    residual_gap_supplemental_tracks = {
        "residual_gap_after_selected_clusters": residual_gap_after_clusters,
        "clean_2d_pass_allow_lift_slice_count": len(clean_cross_items),
        "clean_2d_positive_pass_allow_lift_count": len(clean_cross_items),
        "clean_2d_non_dedup_upper_bound_event_count": clean_2d_non_dedup_upper_bound,
        "clean_2d_max_single_slice_matched_gold_silver_events": clean_2d_max_single_slice,
        "clean_2d_top_items": [
            {
                "plan_item_id": row.get("plan_item_id"),
                "candidate_id": row.get("candidate_id"),
                "dimension": row.get("dimension"),
                "slice_value": row.get("slice_value"),
                "matched_gold_silver_events": row.get("matched_gold_silver_events"),
                "pass_allow_lift": row.get("pass_allow_lift"),
                "downstream_lift_scope": row.get("downstream_lift_scope"),
            }
            for row in clean_cross_items[:5]
        ],
        "shadow_queue_pass_allow_item_count": len(shadow_pass_allow_items),
        "shadow_queue_non_dedup_upper_bound_event_count": shadow_queue_non_dedup_upper_bound,
        "shadow_queue_top_items": [
            {
                "plan_item_id": row.get("plan_item_id"),
                "candidate_id": row.get("candidate_id"),
                "hypothesis_source": row.get("hypothesis_source"),
                "event_count": row.get("event_count"),
                "events_contributing_to_60pct_gap_upper_bound": (
                    row.get("events_contributing_to_60pct_gap_upper_bound")
                ),
                "next_action": row.get("next_action"),
            }
            for row in shadow_pass_allow_items[:5]
        ],
        "supplemental_non_dedup_upper_bound_event_count": supplemental_non_dedup_upper_bound,
        "supplemental_tracks_can_cover_residual_upper_bound": (
            residual_gap_after_clusters == 0
            or supplemental_non_dedup_upper_bound >= residual_gap_after_clusters
        ),
        "combined_selected_plus_supplemental_upper_bound_event_count": (
            cumulative + supplemental_non_dedup_upper_bound
        ),
        "combined_tracks_can_cover_current_gap_upper_bound": (
            cumulative + supplemental_non_dedup_upper_bound >= additional_needed
            if additional_needed
            else True
        ),
        "evidence_level": "non_deduped_same_window_upper_bound_not_promotion_evidence",
        "next_action": (
            "track_residual_pass_allow_gap_clean_2d_and_shadow_queue_until_oos"
            if residual_gap_after_clusters > 0 and supplemental_non_dedup_upper_bound
            else "continue_pass_allow_reason_decomposition"
            if residual_gap_after_clusters > 0
            else "selected_clusters_cover_current_gap_upper_bound"
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "caveat": (
            "Supplemental tracks are intentionally non-deduped upper bounds. They show where to "
            "continue shadow-only/OOS collection for the residual pass_allow gap; they do not prove "
            "runtime capture improvement and cannot justify strategy, gate, final_entry_contract, "
            "A_CLASS, executor, paper/live, canary, or risk changes."
        ),
    }

    if additional_needed <= 0:
        classification = "PASS_ALLOW_60_CLOSURE_NOT_NEEDED"
        next_action = "pass_allow_capture_target_reached_continue_downstream_gap_audit"
    elif cumulative >= additional_needed:
        classification = "PASS_ALLOW_60_CLOSURE_PLAN_READY_PENDING_CLEAN_OOS"
        next_action = "hold_selected_pass_allow_gap_clusters_until_clean_window_then_non_overlapping_oos"
    elif selected_cluster_items or clean_cross_items or shadow_pass_allow_items:
        classification = "PASS_ALLOW_60_CLOSURE_PLAN_PARTIAL_PENDING_MORE_SHADOW_EVIDENCE"
        next_action = "expand_shadow_pass_allow_reviews_using_clean_cross_and_decision_no_pass_clusters"
    else:
        classification = "PASS_ALLOW_60_CLOSURE_PLAN_EMPTY"
        next_action = "continue_pass_allow_reason_decomposition"

    return {
        "schema_version": "pass_allow_60_closure_plan.v3",
        "report_type": "pass_allow_60_closure_plan",
        "generated_at": utc_now(),
        "phase": "discovery_readiness",
        "evidence_level": "discovery_same_window",
        "usage": "read_only_shadow_gap_closure_planning",
        "classification": classification,
        "next_action": next_action,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "runtime_effect": "none",
        "target_gap": {
            "raw_gold_silver_denominator": raw_den,
            "target_capture_rate": TARGET_RATE,
            "target_60_count": target_60,
            "current_pass_allow_count": current_pass_allow,
            "current_pass_allow_rate": (stage_counts.get("pass_allow_capture") or {}).get("rate"),
            "additional_pass_allow_events_needed_to_60": additional_needed,
            "selected_cluster_upper_bound_event_count": cumulative,
            "selected_clusters_cover_current_gap_upper_bound": (
                cumulative >= additional_needed if additional_needed else True
            ),
            "residual_gap_after_selected_cluster_upper_bound": residual_gap_after_clusters,
        },
        "residual_gap_supplemental_tracks": residual_gap_supplemental_tracks,
        "closure_tracks": {
            "decision_no_pass_quality_timing_clusters": {
                "count": len(selected_cluster_items),
                "items": selected_cluster_items,
            },
            "clean_2d_pass_allow_lift_slices": {
                "count": len(clean_cross_items),
                "items": clean_cross_items,
                "caveat": (
                    "These are slice-level matched gold/silver signal_id downstream overlays when "
                    "downstream_lift_scope=slice_level_matched_gold_silver_signal_id; otherwise "
                    "they remain candidate-level proxies. All require repeated clean-window evidence "
                    "and OOS validation."
                ),
            },
            "shadow_queue_pass_allow_items": {
                "count": len(shadow_pass_allow_items),
                "items": shadow_pass_allow_items,
            },
        },
        "context_constraints": {
            "clean_dimensions": context_eligibility.get("clean_dimensions") or [],
            "blocked_dimensions": context_eligibility.get("blocked_dimensions") or [],
            "rule": "Use only clean or core metadata dimensions for same-window discovery; blocked quote/kline/Markov dimensions cannot support OOS evidence.",
        },
        "allowed_scope": [
            "read-only evaluator/report improvements",
            "shadow-only candidate/context instrumentation",
            "hypothesis registry entries for clean-window/OOS validation",
        ],
        "forbidden_scope": [
            "strategy change",
            "entry policy change",
            "hard gate relaxation",
            "exit gate change",
            "final_entry_contract change",
            "A_CLASS mode reset or enablement",
            "paper/live executor enablement",
            "canary or risk increase",
        ],
        "notes": [
            "This plan closes an audit gap, not a trading gap. It does not authorize runtime behavior changes.",
            "All event contributions are upper bounds until repeated clean-window and non-overlapping OOS validation.",
        ],
    }


def stable_fingerprint(payload):
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def build_pass_allow_60_oos_freeze_registry(pass_allow_closure_plan, previous_registry=None):
    """Freeze current pass/allow closure hypotheses for future OOS checks."""
    previous_registry = previous_registry or {}
    previous_items_by_id = {
        row.get("freeze_id"): row
        for row in previous_registry.get("items") or []
        if isinstance(row, dict) and row.get("freeze_id")
    }
    previous_fingerprints = {
        row.get("definition_fingerprint")
        for row in previous_registry.get("items") or []
        if isinstance(row, dict) and row.get("definition_fingerprint")
    }
    now = utc_now()
    tracks = pass_allow_closure_plan.get("closure_tracks") or {}
    target_gap = pass_allow_closure_plan.get("target_gap") or {}
    items = []

    def append_item(source, source_item, freeze_definition, expected_stage="pass_allow_capture"):
        if not source_item:
            return
        fingerprint = stable_fingerprint(freeze_definition)
        freeze_id = f"pass_allow_60:{source}:{fingerprint}"
        previous = previous_items_by_id.get(freeze_id) or {}
        frozen_at = previous.get("frozen_at") or previous_registry.get("definition_set_frozen_at") or previous_registry.get("generated_at") or now
        items.append({
            "freeze_id": freeze_id,
            "source": source,
            "source_plan_item_id": source_item.get("plan_item_id"),
            "expected_capture_stage_improved": expected_stage,
            "definition_fingerprint": fingerprint,
            "frozen_at": frozen_at,
            "freeze_definition": freeze_definition,
            "current_window_evidence": {
                key: source_item.get(key)
                for key in (
                    "decision_no_pass_event_count",
                    "events_contributing_to_current_60pct_gap_upper_bound",
                    "matched_gold_silver_events",
                    "slice_downstream_signal_count",
                    "downstream_lift_scope",
                    "downstream_slice_join_available",
                    "match_recall_event",
                    "match_precision_event",
                    "decision_lift",
                    "pass_allow_lift",
                    "pending_lift",
                    "final_entry_lift",
                    "mode_adjusted_final_eligibility_lift",
                    "decision_capture_rate_after_match",
                    "pass_allow_capture_rate_after_match",
                    "pending_capture_rate_after_match",
                    "final_entry_rate_after_match",
                    "mode_adjusted_final_eligibility_rate_after_match",
                    "event_count",
                    "events_contributing_to_60pct_gap_upper_bound",
                )
                if source_item.get(key) is not None
            },
            "status": "FROZEN_PENDING_CLEAN_NON_OVERLAPPING_OOS",
            "oos_requirements": {
                "definition_must_match_fingerprint": True,
                "train_window_must_not_overlap_eval_window": True,
                "overlap": False,
                "context_clean_window_required": True,
                "same_window_evidence_is_not_promotion_evidence": True,
                "human_approval_required_before_promotion": True,
            },
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })

    for row in (tracks.get("decision_no_pass_quality_timing_clusters") or {}).get("items") or []:
        if not isinstance(row, dict):
            continue
        append_item(
            "decision_no_pass_quality_timing_cluster",
            row,
            {
                "cluster": row.get("cluster"),
                "expected_capture_stage_improved": "pass_allow_capture",
                "required_match": {
                    "quality_timing_cluster": row.get("cluster"),
                    "stage": "decision_no_pass_or_allow",
                },
                "top_candidate_ids": [
                    item.get("candidate_id")
                    for item in row.get("top_candidates") or []
                    if item.get("candidate_id")
                ],
                "top_lifecycle_source_contexts": row.get("top_lifecycle_source_contexts") or [],
            },
        )

    for row in (tracks.get("clean_2d_pass_allow_lift_slices") or {}).get("items") or []:
        if not isinstance(row, dict):
            continue
        append_item(
            "clean_2d_pass_allow_lift_slice",
            row,
            {
                "candidate_id": row.get("candidate_id"),
                "dimension": row.get("dimension"),
                "dimension_group": row.get("dimension_group"),
                "dimension_eligibility_status": row.get("dimension_eligibility_status"),
                "slice_value": row.get("slice_value"),
                "judgment": row.get("judgment"),
                "expected_capture_stage_improved": "pass_allow_capture",
                "downstream_lift_scope": row.get("downstream_lift_scope"),
            },
        )

    for row in (tracks.get("shadow_queue_pass_allow_items") or {}).get("items") or []:
        if not isinstance(row, dict):
            continue
        append_item(
            "shadow_queue_pass_allow_item",
            row,
            {
                "candidate_id": row.get("candidate_id"),
                "hypothesis_source": row.get("hypothesis_source"),
                "expected_capture_stage_improved": row.get("expected_capture_stage_improved"),
                "required_features": row.get("required_features") or [],
                "time_legal_status": row.get("time_legal_status"),
                "context_blockers": row.get("context_blockers") or [],
            },
            expected_stage=row.get("expected_capture_stage_improved") or "pass_allow_capture",
        )

    source_counts = Counter(row.get("source") for row in items)
    additional_needed = safe_int(target_gap.get("additional_pass_allow_events_needed_to_60"), 0)
    if additional_needed <= 0:
        classification = "PASS_ALLOW_60_OOS_FREEZE_NOT_NEEDED"
    elif items:
        classification = "PASS_ALLOW_60_OOS_FREEZE_READY_PENDING_CLEAN_WINDOW"
    else:
        classification = "PASS_ALLOW_60_OOS_FREEZE_EMPTY"
    fingerprints = {row.get("definition_fingerprint") for row in items if row.get("definition_fingerprint")}
    previous_set_frozen_at = previous_registry.get("definition_set_frozen_at") or previous_registry.get("generated_at")
    definition_set_unchanged = bool(fingerprints and fingerprints == previous_fingerprints)
    definition_set_frozen_at = previous_set_frozen_at if definition_set_unchanged else now
    return {
        "schema_version": "pass_allow_60_oos_freeze_registry.v1",
        "report_type": "pass_allow_60_oos_freeze_registry",
        "generated_at": now,
        "definition_set_frozen_at": definition_set_frozen_at,
        "definition_set_unchanged_from_previous_registry": definition_set_unchanged,
        "phase": "discovery_readiness",
        "evidence_level": "same_window_definitions_frozen_for_future_oos",
        "usage": "read_only_oos_definition_registry",
        "classification": classification,
        "source_closure_plan_classification": pass_allow_closure_plan.get("classification"),
        "target_gap": target_gap,
        "frozen_definition_count": len(items),
        "source_counts": dict(source_counts),
        "items": items,
        "oos_requirements": {
            "train_window_must_not_overlap_eval_window": True,
            "overlap": False,
            "definition_fingerprint_required": True,
            "context_clean_window_required": True,
            "promotion_allowed": False,
            "human_approval_required_before_promotion": True,
        },
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "notes": [
            "Definitions are frozen for future validation only.",
            "A matching fingerprint in a non-overlapping clean eval window is required before any OOS claim.",
        ],
    }


def freeze_item_required_dimension_groups(item):
    definition = (item or {}).get("freeze_definition") or {}
    groups = set()
    dimension_group = definition.get("dimension_group")
    if dimension_group:
        groups.add(dimension_group)
    dimension = definition.get("dimension")
    if dimension:
        if "quote" in str(dimension):
            groups.add("quote-sensitive")
        elif "source" in str(dimension):
            groups.add("source_component")
        elif "lifecycle" in str(dimension):
            groups.add("lifecycle")
        elif "volume" in str(dimension):
            groups.add("volume")
        elif "kline" in str(dimension) or "candle" in str(dimension):
            groups.add("kline")
        elif "markov" in str(dimension):
            groups.add("Markov")
    for blocker in definition.get("context_blockers") or item.get("context_blockers") or []:
        blocker_text = str(blocker)
        if "quote" in blocker_text or "schema" in blocker_text:
            groups.add("quote-sensitive")
        elif "source_component" in blocker_text:
            groups.add("source_component")
        elif "lifecycle" in blocker_text:
            groups.add("lifecycle")
        elif "volume" in blocker_text:
            groups.add("volume")
        elif "kline" in blocker_text or "candle" in blocker_text:
            groups.add("kline")
        elif "markov" in blocker_text:
            groups.add("Markov")
    return sorted(groups)


def build_pass_allow_60_oos_readiness_monitor(pass_allow_freeze_registry, context_eligibility):
    items = pass_allow_freeze_registry.get("items") or []
    blocked_dimensions = set(context_eligibility.get("blocked_dimensions") or [])
    clean_dimensions = set(context_eligibility.get("clean_dimensions") or [])
    now_ts = int(time.time())
    freeze_at = (
        pass_allow_freeze_registry.get("definition_set_frozen_at")
        or pass_allow_freeze_registry.get("generated_at")
    )
    freeze_ts = parse_utc_ts(freeze_at)
    post_freeze_age_sec = None if freeze_ts is None else max(0, now_ts - freeze_ts)
    safety_sec = 120
    min_post_freeze_hours = 0.05
    usable_post_freeze_sec = None
    usable_post_freeze_hours = None
    if post_freeze_age_sec is not None:
        usable_post_freeze_sec = max(0, post_freeze_age_sec - safety_sec)
        usable_post_freeze_hours = round(usable_post_freeze_sec / 3600.0, 4)
    scoped_rows = []
    clean_count = 0
    blocked_count = 0
    source_counts = Counter()
    blocked_source_counts = Counter()
    clean_source_counts = Counter()
    blocker_counts = Counter()
    for item in items:
        source = item.get("source") or "unknown"
        source_counts[source] += 1
        required = freeze_item_required_dimension_groups(item)
        blockers = sorted(group for group in required if group in blocked_dimensions)
        clean = not blockers
        if clean:
            clean_count += 1
            clean_source_counts[source] += 1
        else:
            blocked_count += 1
            blocked_source_counts[source] += 1
            blocker_counts.update(blockers)
        scoped_rows.append({
            "freeze_id": item.get("freeze_id"),
            "source": source,
            "definition_fingerprint": item.get("definition_fingerprint"),
            "required_dimension_groups": required,
            "blocked_required_dimension_groups": blockers,
            "definition_context_clean": clean,
            "promotion_allowed": False,
        })
    global_blocked_not_required = sorted(
        group for group in blocked_dimensions
        if not any(group in row["required_dimension_groups"] for row in scoped_rows)
    )
    if not items:
        classification = "PASS_ALLOW_60_OOS_MONITOR_EMPTY"
        next_action = "continue_pass_allow_gap_reason_decomposition"
    elif clean_count <= 0:
        classification = "PASS_ALLOW_60_OOS_ALL_DEFINITIONS_CONTEXT_BLOCKED"
        next_action = "fix_or_exclude_blocked_context_dimensions_before_oos"
    elif usable_post_freeze_hours is None:
        classification = "PASS_ALLOW_60_OOS_FREEZE_TS_MISSING"
        next_action = "regenerate_freeze_registry_with_generated_at"
    elif usable_post_freeze_hours < min_post_freeze_hours:
        classification = "PASS_ALLOW_60_OOS_POST_FREEZE_WINDOW_TOO_YOUNG"
        next_action = "continue_collecting_post_freeze_window_before_judging_oos"
    else:
        classification = "PASS_ALLOW_60_OOS_READY_FOR_POST_FREEZE_PROBE"
        next_action = "run_pass_allow_60_post_freeze_oos_validation"
    return {
        "schema_version": "pass_allow_60_oos_readiness_monitor.v1",
        "report_type": "pass_allow_60_oos_readiness_monitor",
        "generated_at": utc_now(),
        "classification": classification,
        "next_action": next_action,
        "freeze_generated_at": pass_allow_freeze_registry.get("generated_at"),
        "definition_set_frozen_at": pass_allow_freeze_registry.get("definition_set_frozen_at"),
        "freeze_age_reference_at": freeze_at,
        "freeze_generated_ts": freeze_ts,
        "now_ts": now_ts,
        "post_freeze_age_sec": post_freeze_age_sec,
        "post_freeze_safety_sec": safety_sec,
        "usable_post_freeze_sec": usable_post_freeze_sec,
        "usable_post_freeze_hours": usable_post_freeze_hours,
        "min_post_freeze_hours_for_probe": min_post_freeze_hours,
        "post_freeze_hours_needed": (
            None if usable_post_freeze_hours is None
            else max(0.0, round(min_post_freeze_hours - usable_post_freeze_hours, 4))
        ),
        "frozen_definition_count": len(items),
        "definition_context_clean_count": clean_count,
        "definition_context_blocked_count": blocked_count,
        "source_counts": dict(source_counts),
        "clean_source_counts": dict(clean_source_counts),
        "blocked_source_counts": dict(blocked_source_counts),
        "blocked_required_dimension_counts": dict(blocker_counts),
        "global_blocked_dimensions": sorted(blocked_dimensions),
        "clean_dimensions": sorted(clean_dimensions),
        "global_blocked_dimensions_not_required_by_frozen_definitions": global_blocked_not_required,
        "items": scoped_rows[:75],
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "notes": [
            "Definition-scoped context readiness only. Global dirty dimensions do not block a frozen definition unless that definition requires them.",
            "A post-freeze probe is still discovery/readiness evidence; promotion requires non-overlapping validation and human approval.",
        ],
    }


def build_final_entry_readiness_audit(a_class, stage_metrics, pending_audit):
    return {
        "schema_version": "final_entry_readiness_audit.v1",
        "report_type": "final_entry_readiness_audit",
        "generated_at": utc_now(),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "final_entry_status": (a_class or {}).get("final_entry_status"),
        "current_capture_stage": (a_class or {}).get("current_capture_stage"),
        "reason": (a_class or {}).get("reason"),
        "human_action_required": boolish((a_class or {}).get("human_action_required")),
        "mode_status": (a_class or {}).get("A_CLASS_mode_status") or (a_class or {}).get("mode_status") or {},
        "stage2_flat_summary": (a_class or {}).get("stage2_flat_summary") or {},
        "readiness_shortfall_summary": (a_class or {}).get("readiness_shortfall_summary") or {},
        "paper_entry_proposal_readiness": (a_class or {}).get("paper_entry_proposal_readiness") or {},
        "final_entry_contract_blocker_breakdown": (a_class or {}).get("final_entry_contract_blocker_breakdown") or {},
        "mode_disabled_adjusted_final_eligibility": stage_metrics["stage_counts"].get("mode_disabled_adjusted_final_eligibility") or {},
        "pending_to_final_entry_dominant_category": (
            (pending_audit.get("pending_no_final_entry_classification") or {}).get("dominant_category")
        ),
    }


def build_strategy_memory_capture_validation(reports):
    validation = reports.get("strategy_memory_validation") or {}
    ingestion = reports.get("strategy_memory_ingestion") or {}
    rows = []
    for row in validation.get("hypotheses") or []:
        primary = row.get("primary_candidate") or {}
        current_24h = next((item for item in row.get("window_validations") or [] if safe_int(item.get("hours"), 0) == 24), {})
        rows.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "name": row.get("name"),
            "family": row.get("strategy_family"),
            "verdict": row.get("verdict"),
            "mapped_candidate_ids": row.get("mapped_existing_candidate_ids") or row.get("mapped_candidate_ids") or [],
            "missing_shadow_candidate_required": boolish(row.get("missing_shadow_candidate_handoff_required")),
            "raw_gs_recall": primary.get("raw_gold_silver_recall") if primary else current_24h.get("raw_gold_silver_recall"),
            "precision": primary.get("precision") if primary else current_24h.get("precision"),
            "decision_capture_lift": row.get("decision_capture_lift"),
            "pending_capture_lift": row.get("pending_capture_lift"),
            "final_eligibility_lift": row.get("final_eligibility_lift"),
            "mode_disabled_adjusted_final_eligibility_lift": row.get("mode_disabled_adjusted_final_eligibility_lift"),
            "decision_capture": primary.get("decision_capture") if primary else current_24h.get("decision_capture"),
            "pending_capture": primary.get("pending_capture") if primary else current_24h.get("pending_capture"),
            "final_eligibility_capture": primary.get("final_eligibility_capture") if primary else current_24h.get("final_eligibility_capture"),
            "mode_disabled_adjusted_final_eligibility": (
                primary.get("mode_disabled_adjusted_final_eligibility")
                if primary
                else current_24h.get("mode_disabled_adjusted_final_eligibility")
            ),
            "paper_capture": primary.get("paper_capture") if primary else current_24h.get("paper_capture"),
            "context_blockers": row.get("context_blockers") or row.get("blocked_contexts") or [],
            "time_legal_status": row.get("time_legal_status"),
            "future_data_rejected": boolish(row.get("future_data_rejected") or row.get("rejected_future_data")),
            "pnl_secondary_only": True,
            "promotion_allowed": False,
        })
    return {
        "schema_version": "strategy_memory_capture_validation.v1",
        "report_type": "strategy_memory_capture_validation",
        "generated_at": utc_now(),
        "promotion_allowed": False,
        "evidence_role": "historical_memory_discovery_only",
        "allowed_use": "shadow_only",
        "hypotheses_count": validation.get("hypotheses_count") or ingestion.get("strategy_memory_hypotheses_count") or len(rows),
        "status_counts": validation.get("status_counts") or {},
        "missing_shadow_candidates": ingestion.get("missing_shadow_candidates", 0),
        "rejected_future_data_hypotheses": ingestion.get("rejected_future_data_hypotheses", 0),
        "hypotheses": rows,
    }


def expected_stage_from_quality_timing_stage(stage):
    stage = str(stage or "")
    if stage == "decision_no_pass_or_allow":
        return "pass_allow_capture"
    if stage == "pass_or_allow_without_pending_entry":
        return "pending_capture"
    if stage == "pending_without_final_entry_contract":
        return "final_eligibility"
    return "mode_disabled_adjusted_final_eligibility"


def quality_timing_required_features(row):
    required = ["quality_timing_cluster", "decision_reason"]
    contexts = row.get("top_lifecycle_source_contexts") or []
    if contexts:
        required.extend(["lifecycle_profile", "source_component"])
    if row.get("top_candidates"):
        required.append("candidate_id")
    return required


def quality_timing_context_blockers(row, context_eligibility):
    blockers = []
    dimensions = context_eligibility.get("dimensions") or {}
    for dim in ("lifecycle", "source_component"):
        if dim in quality_timing_required_features(row):
            status = (dimensions.get(dim) or {}).get("status")
            if status not in {STATUS_CLEAN, STATUS_NA}:
                blockers.append(dim)
    return blockers


def improvement_queue_priority(item):
    source_priority = {
        "quality_timing_reject_cluster": 0,
        "quality_timing_candidate_probe": 1,
        "pending_to_final_stale_before_final_cluster": 2,
        "pending_momentum_decay_shadow_probe": 3,
        "pending_to_final_transition_dropoff_category": 4,
        "filtered_winner_dossier": 5,
        "strategy_memory_missing_shadow_candidate": 6,
        "clean_2d_capture_cross_slice": 7,
        "derive_context_filtered_shadow_candidate": 8,
        "refine_potential_entry_hypothesis_with_context": 9,
    }
    stage_priority = {
        "pass_allow_capture": 0,
        "pending_capture": 1,
        "final_eligibility": 2,
        "mode_disabled_adjusted_final_eligibility": 3,
        "detector_capture": 4,
        "detector_or_context_capture": 5,
    }
    evidence = item.get("evidence") or {}
    event_count = safe_float(
        evidence.get("event_count")
        or evidence.get("cluster_event_count")
        or evidence.get("matched_gold_silver_events")
        or evidence.get("candidate_cluster_match_count")
        or 0,
        0,
    )
    return (
        source_priority.get(item.get("hypothesis_source"), 50),
        stage_priority.get(item.get("expected_capture_stage_improved"), 50),
        -float(event_count or 0),
        str(item.get("candidate_id") or ""),
    )


def clean_cross_expected_capture_stage(row):
    """Pick the earliest improved capture stage for a clean 2D slice.

    The queue is discovery-only, but the expected stage still matters: a slice
    that improves pass/allow should be reviewed before a slice that only
    improves generic detector recall.
    """
    if safe_float(row.get("pass_allow_lift"), 0.0) > 0:
        return "pass_allow_capture"
    if safe_float(row.get("pending_lift"), 0.0) > 0:
        return "pending_capture"
    if (
        safe_float(row.get("final_entry_lift"), 0.0) > 0
        or safe_float(row.get("mode_adjusted_final_eligibility_lift"), 0.0) > 0
    ):
        return "final_eligibility"
    if safe_float(row.get("decision_lift"), 0.0) > 0:
        return "decision_capture"
    return "detector_capture"


def clean_cross_downstream_evidence(row):
    return {
        "matched_gold_silver_events": row.get("matched_gold_silver_events"),
        "slice_downstream_signal_count": row.get("slice_downstream_signal_count"),
        "downstream_lift_scope": row.get("downstream_lift_scope"),
        "downstream_slice_join_available": row.get("downstream_slice_join_available"),
        "decision_capture_rate_after_match": row.get("decision_capture_rate_after_match"),
        "pass_allow_capture_rate_after_match": row.get("pass_allow_capture_rate_after_match"),
        "pending_capture_rate_after_match": row.get("pending_capture_rate_after_match"),
        "final_entry_rate_after_match": row.get("final_entry_rate_after_match"),
        "mode_adjusted_final_eligibility_rate_after_match": (
            row.get("mode_adjusted_final_eligibility_rate_after_match")
        ),
        "decision_lift": row.get("decision_lift"),
        "pass_allow_lift": row.get("pass_allow_lift"),
        "pending_lift": row.get("pending_lift"),
        "final_entry_lift": row.get("final_entry_lift"),
        "mode_adjusted_final_eligibility_lift": row.get("mode_adjusted_final_eligibility_lift"),
        "paper_capture_lift": row.get("paper_capture_lift"),
    }


def build_shadow_candidate_improvement_queue(reports, context_eligibility):
    candidate_improvement = reports.get("candidate_improvement") or {}
    strategy_memory_validation = reports.get("strategy_memory_validation") or {}
    strategy_memory_ingestion = reports.get("strategy_memory_ingestion") or {}
    filtered_bridge = reports.get("strategy_memory_filtered_winner_bridge") or {}
    pending_audit = reports.get("pending_to_final_entry_audit") or {}
    quality_timing = reports.get("quality_timing_reject_research") or {}
    capture_cross = reports.get("capture_cross") or {}
    clean_dimensions = set(context_eligibility.get("clean_dimensions") or [])
    items = []
    for row in strategy_memory_ingestion.get("missing_shadow_candidate_handoffs") or []:
        items.append({
            "candidate_id": row.get("candidate_id") or row.get("hypothesis_id"),
            "hypothesis_source": "strategy_memory_missing_shadow_candidate",
            "expected_capture_stage_improved": "detector_capture",
            "required_features": row.get("required_features") or [],
            "time_legal_status": "requires_review",
            "context_blockers": row.get("blocked_contexts") or [],
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "next_action": "generate_codex_handoff_only",
        })
    for row in candidate_improvement.get("top_opportunities") or []:
        items.append({
            "candidate_id": row.get("candidate_id"),
            "hypothesis_source": row.get("opportunity_type") or "candidate_improvement_opportunity",
            "expected_capture_stage_improved": "detector_or_context_capture",
            "required_features": [row.get("dimension")] if row.get("dimension") else [],
            "time_legal_status": "unknown",
            "context_blockers": row.get("blocked_by") or [],
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "evidence": row.get("metrics") or {},
            "next_action": row.get("suggested_action"),
        })
    for row in capture_cross.get("valid_top_crosses") or []:
        if row.get("judgment") not in {"DISCOVERY_HIT", "WATCH"}:
            continue
        dimension = row.get("dimension")
        dimension_group = row.get("dimension_group") or dimension
        dimension_status = row.get("dimension_eligibility_status")
        core_allowed = dimension_status == "CORE_METADATA_ALLOWED"
        if dimension_group and dimension_group not in clean_dimensions and not core_allowed:
            continue
        expected_stage = clean_cross_expected_capture_stage(row)
        items.append({
            "candidate_id": row.get("candidate_id"),
            "hypothesis_source": "clean_2d_capture_cross_slice",
            "expected_capture_stage_improved": expected_stage,
            "required_features": [dimension] if dimension else [],
            "time_legal_status": "context_slice_time_legal_not_proven",
            "dimension_group": dimension_group,
            "dimension_eligibility_status": dimension_status,
            "context_blockers": row.get("invalid_reasons") or [],
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "evidence": {
                "recall_lift": row.get("recall_lift_vs_candidate_baseline"),
                "precision_lift": row.get("precision_lift_vs_candidate_baseline"),
                **clean_cross_downstream_evidence(row),
            },
            "next_action": "track_same_definition_in_next_clean_window_then_oos_if_repeated",
        })
    blocker_counts = filtered_bridge.get("final_blocker_counts") or {}
    if blocker_counts:
        items.append({
            "candidate_id": "filtered_winner_bridge_review",
            "hypothesis_source": "filtered_winner_dossier",
            "expected_capture_stage_improved": "pending_to_final_or_exit_capture",
            "required_features": ["filtered_winner_final_blocker_attribution"],
            "time_legal_status": "historical_memory_requires_current_validation",
            "context_blockers": [],
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "evidence": {"final_blocker_counts": blocker_counts},
            "next_action": "bridge_filtered_winners_to_current_funnel_blockers",
        })
    transition_review = pending_audit.get("largest_transition_dropoff_review") or {}
    for row in transition_review.get("selected_categories_to_cover_largest_transition_dropoff_upper_bound") or []:
        if not isinstance(row, dict):
            continue
        category = row.get("category")
        items.append({
            "candidate_id": f"pending_to_final:{category}",
            "hypothesis_source": "pending_to_final_transition_dropoff_category",
            "expected_capture_stage_improved": "final_eligibility",
            "required_features": [
                "pending_to_final_category",
                "pending_entry_ts",
                "final_entry_contract_record",
                "decision_reason",
            ],
            "time_legal_status": "read_only_transition_attribution_not_entry_rule",
            "context_blockers": [],
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "evidence": {
                "category": category,
                "event_count": row.get("event_count"),
                "events_contributing_to_largest_transition_dropoff_upper_bound": row.get(
                    "events_contributing_to_largest_transition_dropoff_upper_bound"
                ),
                "cumulative_events_contributing_to_largest_transition_dropoff_upper_bound": row.get(
                    "cumulative_events_contributing_to_largest_transition_dropoff_upper_bound"
                ),
                "adjacent_count_loss_pending_to_final": transition_review.get(
                    "adjacent_count_loss_pending_to_final"
                ),
                "pending_without_final_entry_contract": transition_review.get(
                    "pending_without_final_entry_contract"
                ),
            },
            "human_approval_required_if_fix_requires": row.get(
                "human_approval_required_if_fix_requires"
            ),
            "next_action": row.get("next_action"),
        })
    stale_review = pending_audit.get("stale_before_final_review") or {}
    for row in stale_review.get("selected_clusters_to_cover_stale_before_final_upper_bound") or []:
        if not isinstance(row, dict):
            continue
        cluster = row.get("cluster")
        items.append({
            "candidate_id": f"pending_to_final_stale:{cluster}",
            "hypothesis_source": "pending_to_final_stale_before_final_cluster",
            "expected_capture_stage_improved": "final_eligibility",
            "required_features": [
                "pending_entry_ts",
                "quality_timing_cluster",
                "decision_reason",
                "final_entry_contract_record",
            ],
            "time_legal_status": "read_only_pending_timing_attribution_not_entry_rule",
            "context_blockers": row.get("context_blockers") or [],
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "evidence": {
                "cluster": cluster,
                "event_count": row.get("event_count"),
                "events_contributing_to_stale_before_final_upper_bound": row.get(
                    "events_contributing_to_stale_before_final_upper_bound"
                ),
                "cumulative_events_contributing_to_stale_before_final_upper_bound": row.get(
                    "cumulative_events_contributing_to_stale_before_final_upper_bound"
                ),
                "stale_before_final_event_count": stale_review.get(
                    "stale_before_final_event_count"
                ),
                "adjacent_count_loss_pending_to_final": stale_review.get(
                    "adjacent_count_loss_pending_to_final"
                ),
            },
            "human_approval_required_if_fix_requires": row.get(
                "human_approval_required_if_fix_requires"
            ),
            "next_action": row.get("next_action"),
        })
    momentum_decay_review = stale_review.get("momentum_decay_review") or {}
    for row in momentum_decay_review.get("selected_shadow_probes") or []:
        if not isinstance(row, dict):
            continue
        probe_id = row.get("probe_id")
        items.append({
            "candidate_id": probe_id,
            "hypothesis_source": "pending_momentum_decay_shadow_probe",
            "expected_capture_stage_improved": row.get("expected_capture_stage_improved") or "final_eligibility",
            "required_features": row.get("required_features") or [],
            "time_legal_status": "shadow_recheck_window_time_legal_not_proven",
            "context_blockers": momentum_decay_review.get("context_blockers") or [],
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "evidence": row.get("evidence") or {},
            "human_approval_required_if_fix_requires": row.get(
                "human_approval_required_if_fix_requires"
            ),
            "next_action": row.get("next_action"),
        })
    qt_review = (quality_timing.get("shadow_only_review") or {})
    for row in (qt_review.get("top_research_opportunities") or [])[:10]:
        if not isinstance(row, dict):
            continue
        cluster = row.get("cluster")
        stage_counts = row.get("stage_counts") or []
        dominant_stage = (stage_counts[0] or {}).get("stage") if stage_counts else None
        items.append({
            "candidate_id": f"quality_timing:{cluster}",
            "hypothesis_source": "quality_timing_reject_cluster",
            "expected_capture_stage_improved": expected_stage_from_quality_timing_stage(dominant_stage),
            "required_features": quality_timing_required_features(row),
            "time_legal_status": "research_only_runtime_reject_cluster_not_entry_rule",
            "context_blockers": quality_timing_context_blockers(row, context_eligibility),
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "evidence": {
                "cluster": cluster,
                "event_count": row.get("event_count"),
                "share_of_quality_timing_rejects": row.get("share_of_quality_timing_rejects"),
                "share_of_raw_all_gold_silver": row.get("share_of_raw_all_gold_silver"),
                "unique_tokens": row.get("unique_tokens"),
                "candidate_matched_any_rate": row.get("candidate_matched_any_rate"),
                "readiness_impact_upper_bound": row.get("readiness_impact_upper_bound") or {},
                "top_candidates": (row.get("top_candidates") or [])[:5],
                "top_lifecycle_source_contexts": (row.get("top_lifecycle_source_contexts") or [])[:5],
            },
            "human_approval_required_if_fix_requires": row.get("human_approval_required_if_fix_requires"),
            "next_action": row.get("suggested_shadow_only_action")
            or "track_quality_timing_false_negative_shadow_probe",
        })
        for candidate in (row.get("top_candidates") or [])[:2]:
            candidate_id = candidate.get("candidate_id")
            if not candidate_id or candidate_id in {"current_all", "current_would_enter_all"}:
                continue
            items.append({
                "candidate_id": candidate_id,
                "hypothesis_source": "quality_timing_candidate_probe",
                "expected_capture_stage_improved": expected_stage_from_quality_timing_stage(dominant_stage),
                "required_features": ["quality_timing_cluster", "candidate_id", "decision_reason"],
                "time_legal_status": "research_only_runtime_reject_cluster_not_entry_rule",
                "context_blockers": quality_timing_context_blockers(row, context_eligibility),
                "allowed_use": "shadow_only",
                "promotion_allowed": False,
                "strategy_change_allowed": False,
                "automatic_runtime_change_allowed": False,
                "paper_enablement_allowed": False,
                "evidence": {
                    "cluster": cluster,
                    "candidate_cluster_match_count": candidate.get("count"),
                    "candidate_family": candidate.get("family"),
                    "cluster_event_count": row.get("event_count"),
                    "share_of_quality_timing_rejects": row.get("share_of_quality_timing_rejects"),
                },
                "human_approval_required_if_fix_requires": row.get("human_approval_required_if_fix_requires"),
                "next_action": "track_candidate_within_quality_timing_cluster_shadow_only",
            })
    items = sorted(items, key=improvement_queue_priority)
    status_counts = Counter(row.get("hypothesis_source") for row in items)
    top_items = items[:75]
    return {
        "schema_version": "shadow_candidate_improvement_queue.v2",
        "report_type": "shadow_candidate_improvement_queue",
        "generated_at": utc_now(),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "queue_count": len(items),
        "source_counts": dict(status_counts),
        "top_items": top_items,
        "top_opportunities": top_items,
        "strategy_memory_status_counts": strategy_memory_validation.get("status_counts") or {},
        "pending_to_final_dominant_category": (
            (pending_audit.get("pending_no_final_entry_classification") or {}).get("dominant_category")
        ),
    }


def load_oos_reports(run_dir):
    run_dir = Path(run_dir)
    reports = {}
    for name in (
        "hypothesis_validation_audit_oos_probe_0p1h.json",
        "hypothesis_validation_audit_oos_probe_0p25h.json",
        "hypothesis_validation_audit_oos_probe_0p5h.json",
        "hypothesis_validation_audit_oos_probe_1h.json",
        "matured_volume_capture_cross_audit_oos_probe_0p1h.json",
        "matured_volume_capture_cross_audit_oos_probe_0p25h.json",
        "matured_volume_capture_cross_audit_oos_probe_0p5h.json",
        "matured_volume_capture_cross_audit_oos_probe_1h.json",
        "oos_readiness_probe_refresh.json",
        "pass_allow_60_post_freeze_oos_validation.json",
    ):
        path = run_dir / name
        if path.exists():
            key = name.replace(".json", "")
            key = key.replace("hypothesis_validation_audit_oos_probe_", "hypothesis_validation_oos_probe_")
            key = key.replace("matured_volume_capture_cross_audit_oos_probe_", "matured_volume_cross_oos_probe_")
            reports[key] = load_json(path, {})
    return reports


def build_oos_summary(run_dir, reports=None):
    oos_reports = load_oos_reports(run_dir)
    input_reports = reports if reports is not None else {}
    if build_oos_readiness_summary:
        summary = build_oos_readiness_summary(oos_reports)
    else:
        summary = {
            "classification": "OOS_PROBES_MISSING" if not oos_reports else "OOS_REPORTS_AVAILABLE",
            "available_probe_count": len(oos_reports),
            "promotion_allowed": False,
            "probes": [],
        }
    summary["schema_version"] = "oos_readiness_summary.v1"
    summary["report_type"] = "oos_readiness_summary"
    summary["generated_at"] = utc_now()
    summary["promotion_allowed"] = False
    quality_timing_probe_validation = (
        (input_reports or {}).get("quality_timing_candidate_probe_validation") or {}
    )
    decision_no_pass_watch_validation = (
        (input_reports or {}).get("decision_no_pass_quality_timing_watch_validation") or {}
    )
    pass_allow_closure_plan = (
        (input_reports or {}).get("pass_allow_60_closure_plan") or {}
    )
    pass_allow_freeze_registry = (
        (input_reports or {}).get("pass_allow_60_oos_freeze_registry") or {}
    )
    pass_allow_post_freeze_validation = (
        (input_reports or {}).get("pass_allow_60_post_freeze_oos_validation")
        or oos_reports.get("pass_allow_60_post_freeze_oos_validation")
        or {}
    )
    qt_oos_queue = quality_timing_probe_validation.get("oos_readiness_queue") or {}
    qt_queue_count = safe_int(
        quality_timing_probe_validation.get("oos_readiness_queue_count")
        or qt_oos_queue.get("queue_count"),
        0,
    )
    if quality_timing_probe_validation:
        summary["quality_timing_probe_validation"] = {
            "available": True,
            "classification": quality_timing_probe_validation.get("classification"),
            "next_action": quality_timing_probe_validation.get("next_action"),
            "registered_probe_count": quality_timing_probe_validation.get("registered_probe_count"),
            "validated_probe_count": quality_timing_probe_validation.get("validated_probe_count"),
            "repeated_probe_count": quality_timing_probe_validation.get("repeated_probe_count"),
            "repeated_probe_rate": quality_timing_probe_validation.get("repeated_probe_rate"),
            "oos_readiness_queue_count": qt_queue_count,
            "oos_queue_classification": qt_oos_queue.get("classification"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "blocked_until": "context_clean_window_and_non_overlapping_eval",
        }
        summary["quality_timing_oos_queue_count"] = qt_queue_count
        if qt_queue_count:
            summary["next_quality_timing_oos_action"] = (
                "hold_repeated_quality_timing_probes_until_clean_window_then_non_overlapping_oos"
            )
    dnp_oos_queue = decision_no_pass_watch_validation.get("oos_readiness_queue") or {}
    dnp_queue_count = safe_int(dnp_oos_queue.get("queue_count"), 0)
    if decision_no_pass_watch_validation:
        summary["decision_no_pass_quality_timing_watch_validation"] = {
            "available": True,
            "classification": decision_no_pass_watch_validation.get("classification"),
            "next_action": decision_no_pass_watch_validation.get("next_action"),
            "registered_watch_count": decision_no_pass_watch_validation.get("registered_watch_count"),
            "validated_watch_count": decision_no_pass_watch_validation.get("validated_watch_count"),
            "repeated_selected_cluster_count": (
                decision_no_pass_watch_validation.get("repeated_selected_cluster_count")
            ),
            "repeated_selected_cluster_rate": (
                decision_no_pass_watch_validation.get("repeated_selected_cluster_rate")
            ),
            "oos_readiness_queue_count": dnp_queue_count,
            "oos_queue_classification": dnp_oos_queue.get("classification"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "blocked_until": "context_clean_window_and_non_overlapping_eval",
        }
        summary["decision_no_pass_quality_timing_oos_queue_count"] = dnp_queue_count
        if dnp_queue_count:
            summary["next_decision_no_pass_quality_timing_oos_action"] = (
                "hold_repeated_decision_no_pass_clusters_until_clean_window_then_non_overlapping_oos"
            )
    if pass_allow_closure_plan:
        closure_tracks = pass_allow_closure_plan.get("closure_tracks") or {}
        dnp_track = closure_tracks.get("decision_no_pass_quality_timing_clusters") or {}
        clean_cross_track = closure_tracks.get("clean_2d_pass_allow_lift_slices") or {}
        shadow_track = closure_tracks.get("shadow_queue_pass_allow_items") or {}
        selected_cluster_count = safe_int(dnp_track.get("count"), 0)
        clean_cross_count = safe_int(clean_cross_track.get("count"), 0)
        shadow_pass_allow_count = safe_int(shadow_track.get("count"), 0)
        target_gap = pass_allow_closure_plan.get("target_gap") or {}
        residual_supplemental_tracks = (
            pass_allow_closure_plan.get("residual_gap_supplemental_tracks") or {}
        )
        queue_count = selected_cluster_count + clean_cross_count + shadow_pass_allow_count
        if safe_int(target_gap.get("additional_pass_allow_events_needed_to_60"), 0) <= 0:
            closure_oos_classification = "PASS_ALLOW_60_CLOSURE_OOS_NOT_NEEDED"
            next_action = "continue_downstream_gap_oos_monitoring"
        elif queue_count:
            closure_oos_classification = "PASS_ALLOW_60_CLOSURE_OOS_QUEUE_PENDING_CLEAN_WINDOW"
            next_action = "hold_pass_allow_60_closure_plan_until_clean_non_overlapping_oos"
        else:
            closure_oos_classification = "PASS_ALLOW_60_CLOSURE_OOS_QUEUE_EMPTY"
            next_action = "continue_pass_allow_gap_reason_decomposition"
        summary["pass_allow_60_closure_plan"] = {
            "available": True,
            "classification": pass_allow_closure_plan.get("classification"),
            "next_action": pass_allow_closure_plan.get("next_action"),
            "target_gap": target_gap,
            "residual_gap_supplemental_tracks": residual_supplemental_tracks,
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        }
        summary["pass_allow_60_closure_oos_queue"] = {
            "classification": closure_oos_classification,
            "queue_count": queue_count,
            "decision_no_pass_cluster_count": selected_cluster_count,
            "clean_2d_pass_allow_lift_slice_count": clean_cross_count,
            "shadow_queue_pass_allow_item_count": shadow_pass_allow_count,
            "selected_cluster_upper_bound_event_count": target_gap.get("selected_cluster_upper_bound_event_count"),
            "selected_clusters_cover_current_gap_upper_bound": (
                target_gap.get("selected_clusters_cover_current_gap_upper_bound")
            ),
            "residual_gap_after_selected_cluster_upper_bound": (
                target_gap.get("residual_gap_after_selected_cluster_upper_bound")
            ),
            "residual_gap_supplemental_tracks": residual_supplemental_tracks,
            "blocked_until": "context_clean_window_and_non_overlapping_eval",
            "next_action": next_action,
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        }
        summary["pass_allow_60_closure_oos_queue_count"] = queue_count
        if queue_count:
            summary["next_pass_allow_60_closure_oos_action"] = next_action
    if pass_allow_freeze_registry:
        pass_allow_oos_monitor = build_pass_allow_60_oos_readiness_monitor(
            pass_allow_freeze_registry,
            (input_reports or {}).get("context_dimension_eligibility") or {},
        )
        summary["pass_allow_60_oos_freeze_registry"] = {
            "available": True,
            "classification": pass_allow_freeze_registry.get("classification"),
            "frozen_definition_count": pass_allow_freeze_registry.get("frozen_definition_count"),
            "source_counts": pass_allow_freeze_registry.get("source_counts") or {},
            "oos_requirements": pass_allow_freeze_registry.get("oos_requirements") or {},
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        }
        summary["pass_allow_60_oos_frozen_definition_count"] = safe_int(
            pass_allow_freeze_registry.get("frozen_definition_count"),
            0,
        )
        summary["pass_allow_60_oos_readiness_monitor"] = pass_allow_oos_monitor
    if pass_allow_post_freeze_validation:
        summary["pass_allow_60_post_freeze_oos_validation"] = {
            "available": True,
            "schema_version": pass_allow_post_freeze_validation.get("schema_version"),
            "classification": pass_allow_post_freeze_validation.get("classification"),
            "next_action": pass_allow_post_freeze_validation.get("next_action"),
            "raw_gold_silver_event_rows": pass_allow_post_freeze_validation.get("raw_gold_silver_event_rows"),
            "raw_gold_silver_rows_since_eval_start_unfiltered": (
                pass_allow_post_freeze_validation.get("raw_gold_silver_rows_since_eval_start_unfiltered")
            ),
            "all_raw_rows_since_eval_start": (
                pass_allow_post_freeze_validation.get("all_raw_rows_since_eval_start")
            ),
            "latest_raw_signal_age_sec": pass_allow_post_freeze_validation.get("latest_raw_signal_age_sec"),
            "latest_raw_gold_silver_age_sec": (
                pass_allow_post_freeze_validation.get("latest_raw_gold_silver_age_sec")
            ),
            "latest_raw_gold_silver_lag_sec_before_eval_start": (
                pass_allow_post_freeze_validation.get("latest_raw_gold_silver_lag_sec_before_eval_start")
            ),
            "global_pass_allow_count": pass_allow_post_freeze_validation.get("global_pass_allow_count"),
            "global_pass_allow_rate": pass_allow_post_freeze_validation.get("global_pass_allow_rate"),
            "frozen_definition_count": pass_allow_post_freeze_validation.get("frozen_definition_count"),
            "validated_definition_count": pass_allow_post_freeze_validation.get("validated_definition_count"),
            "supported_definition_count": pass_allow_post_freeze_validation.get("supported_definition_count"),
            "repeat_watch_count": pass_allow_post_freeze_validation.get("repeat_watch_count"),
            "positive_lift_count": pass_allow_post_freeze_validation.get("positive_lift_count"),
            "too_small_definition_count": pass_allow_post_freeze_validation.get("too_small_definition_count"),
            "post_freeze_usable_hours": pass_allow_post_freeze_validation.get("post_freeze_usable_hours"),
            "definition_set_frozen_at": pass_allow_post_freeze_validation.get("definition_set_frozen_at"),
            "oos_data_availability": pass_allow_post_freeze_validation.get("oos_data_availability") or {},
            "status_counts": pass_allow_post_freeze_validation.get("status_counts") or {},
            "top_repeat_watch_items": (
                pass_allow_post_freeze_validation.get("top_repeat_watch_items") or []
            )[:12],
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        }
        summary["pass_allow_60_post_freeze_oos_classification"] = (
            pass_allow_post_freeze_validation.get("classification")
        )
        summary["pass_allow_60_post_freeze_oos_repeat_watch_count"] = (
            pass_allow_post_freeze_validation.get("repeat_watch_count")
        )
    return summary


def assemble_reports(run_dir, out_dir=None):
    run_dir = Path(run_dir)
    out_dir = Path(out_dir) if out_dir else run_dir
    reports = collect_reports(run_dir)
    stage_metrics = build_capture_stage_metrics(reports.get("a_class") or {})
    context_eligibility = build_context_dimension_eligibility(reports)
    reports["context_dimension_eligibility"] = context_eligibility
    pending_audit = build_pending_to_final_entry_audit(reports.get("a_class") or {})
    pending_audit["stale_before_final_review"] = build_stale_before_final_review(
        pending_audit,
        reports,
        context_eligibility,
    )
    reports["pending_to_final_entry_audit"] = pending_audit
    gap_report = build_capture_60_gap_report(stage_metrics, context_eligibility, pending_audit)
    pass_allow_gap_audit = build_pass_allow_capture_gap_audit(
        stage_metrics,
        pending_audit,
        reports,
        context_eligibility,
    )
    decision_no_pass_review = build_decision_no_pass_quality_timing_review(
        stage_metrics,
        pass_allow_gap_audit,
        reports,
        context_eligibility,
    )
    final_entry_readiness = build_final_entry_readiness_audit(reports.get("a_class") or {}, stage_metrics, pending_audit)
    strategy_memory_capture = build_strategy_memory_capture_validation(reports)
    shadow_queue = build_shadow_candidate_improvement_queue(reports, context_eligibility)
    pass_allow_closure_plan = build_pass_allow_60_closure_plan(
        stage_metrics,
        pass_allow_gap_audit,
        decision_no_pass_review,
        shadow_queue,
        reports,
        context_eligibility,
    )
    reports["pass_allow_60_closure_plan"] = pass_allow_closure_plan
    previous_pass_allow_freeze_registry = load_json(
        run_dir / V3_OUTPUT_FILES["pass_allow_60_oos_freeze_registry"],
        {},
    )
    pass_allow_freeze_registry = build_pass_allow_60_oos_freeze_registry(
        pass_allow_closure_plan,
        previous_pass_allow_freeze_registry,
    )
    reports["pass_allow_60_oos_freeze_registry"] = pass_allow_freeze_registry
    oos_summary = build_oos_summary(run_dir, reports)
    payloads = {
        "capture_60_gap_report": gap_report,
        "capture_stage_metrics": stage_metrics,
        "context_dimension_eligibility": context_eligibility,
        "pass_allow_capture_gap_audit": pass_allow_gap_audit,
        "decision_no_pass_quality_timing_review": decision_no_pass_review,
        "pass_allow_60_closure_plan": pass_allow_closure_plan,
        "pass_allow_60_oos_freeze_registry": pass_allow_freeze_registry,
        "pending_to_final_entry_audit": pending_audit,
        "final_entry_readiness_audit": final_entry_readiness,
        "strategy_memory_capture_validation": strategy_memory_capture,
        "shadow_candidate_improvement_queue": shadow_queue,
        "oos_readiness_summary": oos_summary,
    }
    paths = {}
    for key, payload in payloads.items():
        path = out_dir / V3_OUTPUT_FILES[key]
        write_json(path, payload)
        paths[key] = str(path)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "run_dir": str(run_dir),
        "out_dir": str(out_dir),
        "promotion_allowed": False,
        "paths": paths,
        "summary": {
            "biggest_gap_stage": gap_report.get("biggest_gap_stage"),
            "additional_count_needed_to_60": gap_report.get("additional_count_needed_to_60"),
            "next_best_allowed_action": gap_report.get("next_best_allowed_action"),
            "pass_allow_gap_next_action": pass_allow_gap_audit.get("next_action"),
            "decision_no_pass_review_next_action": decision_no_pass_review.get("next_action"),
            "pass_allow_60_closure_classification": pass_allow_closure_plan.get("classification"),
            "pass_allow_60_closure_next_action": pass_allow_closure_plan.get("next_action"),
            "pass_allow_60_oos_freeze_classification": pass_allow_freeze_registry.get("classification"),
            "pass_allow_60_oos_frozen_definition_count": (
                pass_allow_freeze_registry.get("frozen_definition_count")
            ),
            "context_blocked_dimensions": context_eligibility.get("blocked_dimensions") or [],
            "strategy_memory_hypotheses_count": strategy_memory_capture.get("hypotheses_count"),
            "shadow_candidate_queue_count": shadow_queue.get("queue_count"),
            "oos_classification": oos_summary.get("classification"),
        },
    }


def create_self_test_run(root):
    run_dir = root / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    capture = {
        "report_health": {"promotion_blockers": []},
        "quote_context_coverage": {
            "source_quote_clean_present_rate": 1.0,
            "source_quote_executable_present_rate": 1.0,
        },
        "context_health": {"quote_sensitive_slices_evaluable": True},
    }
    context = {
        "quote_context_coverage": capture["quote_context_coverage"],
        "source_component_coverage": {"coverage_rate": 1.0},
        "lifecycle_profile_coverage": {"coverage_rate": 1.0},
        "volume_profile_coverage": {"coverage_rate": 1.0, "blocker": None},
        "kline_coverage": {"coverage_rate": 0.47, "blocker": "kline_coverage_below_80pct"},
        "blockers": ["kline_coverage_below_80pct"],
    }
    volume_kline = {
        "volume_context": {
            "coverage_denominator_type": "signal_context_carrier_rows",
            "field_present_rate": 1.0,
            "known_rate": 0.5,
            "unknown_rate": 0.5,
            "missing_rate": 0.0,
            "blocker": "volume_profile_coverage_below_80pct",
            "root_causes": ["volume_profile_unknown_from_insufficient_or_unclassified_kline"],
            "unknown_diagnostics": {
                "volume_profile_reason_counts": {"insufficient_kline_bars_lt_3": 2},
            },
        },
        "raw_gold_silver_kline": {
            "coverage_denominator_type": "raw_all_gold_silver",
            "raw_all_gold_silver_event_rows": 5,
            "kline_covered_rows": 2,
            "kline_uncovered_rows": 3,
            "kline_coverage_rate": 0.4,
            "kline_coverage_pct": 40.0,
            "blocker": "kline_coverage_below_80pct",
            "kline_uncovered_root_cause_counts": {"baseline_confidence_low_low_30_60s": 3},
        },
    }
    context_blocker_monitor = {
        "overall_verdict": {
            "quote_writer_fix": "VERIFIED_POST_DEPLOY",
            "rolling24_quote_status": "QUOTE_CLEAN_WINDOW_PENDING",
        },
        "task_a_post_deploy_quote_smoke_test": {"classification": "VERIFIED_POST_DEPLOY"},
        "task_b_clean_window_monitor": {"classification": "QUOTE_CLEAN_WINDOW_PENDING"},
    }
    a_class = {
        "final_entry_status": "FUNNEL_BLOCKED_EXPECTED",
        "current_capture_stage": "mode_disabled_clean_window_pending",
        "reason": "self_test",
        "human_action_required": False,
        "capture_stage_rates": {
            "denominator_raw_gold_silver_events": 5,
            "denominator_raw_signal_ids": 5,
            "detector_capture_rate": 1.0,
            "decision_record_capture_rate": 0.8,
            "pass_allow_capture_rate": 0.6,
            "pending_capture_rate": 0.4,
            "final_entry_contract_reach_rate": 0.2,
            "mode_disabled_adjusted_final_eligibility_rate": 0.2,
            "paper_capture_rate": 0.0,
            "paper_trade_intent_rate": 0.0,
            "realized_capture_rate": 0.0,
            "events": {
                "candidate_matched_any": 5,
                "decision_records": 4,
                "pass_or_allow": 3,
                "pending_entry": 2,
                "final_entry_contract": 1,
                "mode_disabled_only_final_entry": 1,
                "paper_trade_intent": 0,
                "paper_committed": 0,
                "realized": 0,
                "entered": 0,
            },
            "mode_disabled_adjusted_final_eligibility": {
                "mode_disabled_only_unique_signal_ids": 1,
                "status": "CAPTURE_READINESS_BELOW_60",
            },
            "upstream_funnel_gap": {
                "no_decision_record": 1,
                "decision_no_pass_or_allow": 1,
                "pass_or_allow_without_pending_entry": 1,
            },
            "pending_to_final_entry_gap": {
                "pending_without_final_entry_contract": 1,
                "pending_without_final_entry_category_counts": {
                    "categories": [
                        {
                            "category": "QUALITY_OR_TIMING_REJECT",
                            "count": 1,
                            "top_reasons": [
                                {
                                    "component": "smart_entry",
                                    "count": 1,
                                    "decision": "reject",
                                    "event_type": "timing_decision",
                                    "reason": "momentum_fading",
                                }
                            ],
                        }
                    ]
                },
            },
        },
        "readiness_shortfall_summary": {
            "target_count_60pct": 3,
            "shortfall_to_60_final_eligibility": 2,
        },
    }
    strategy_validation = {
        "strategy_memory_enabled": True,
        "hypotheses_count": 1,
        "status_counts": {"STRATEGY_MEMORY_DISCOVERY_WATCH": 1},
        "hypotheses": [
            {
                "hypothesis_id": "SM-TEST",
                "name": "Self test",
                "strategy_family": "test",
                "verdict": "STRATEGY_MEMORY_DISCOVERY_WATCH",
                "mapped_existing_candidate_ids": ["current_all"],
                "primary_candidate": {
                    "raw_gold_silver_recall": 0.6,
                    "precision": 0.2,
                    "decision_capture": 0.8,
                    "pending_capture": 0.4,
                    "final_eligibility_capture": 0.2,
                    "mode_disabled_adjusted_final_eligibility": 0.2,
                    "paper_capture": 0.0,
                },
                "future_data_rejected": False,
                "context_blockers": [],
            }
        ],
    }
    ingestion = {
        "strategy_memory_hypotheses_count": 1,
        "missing_shadow_candidates": 1,
        "rejected_future_data_hypotheses": 0,
        "missing_shadow_candidate_handoffs": [
            {"hypothesis_id": "SM-MISSING", "candidate_id": "shadow:missing", "required_features": ["mc_bucket"]}
        ],
    }
    candidate_improvement = {
        "top_opportunities": [
            {
                "candidate_id": "current_all",
                "opportunity_type": "track_context_slice_shadow_only",
                "dimension": "source_component",
                "blocked_by": [],
                "metrics": {"matched_gold_silver_events": 3},
                "suggested_action": "continue_shadow_tracking",
            }
        ]
    }
    quality_timing = {
        "stage_attribution": {
            "reason_counts": [
                {
                    "stage": "pending_without_final_entry_contract",
                    "component": "smart_entry",
                    "count": 1,
                    "decision": "reject",
                    "event_type": "timing_decision",
                    "reason": "momentum_fading",
                }
            ]
        },
        "shadow_only_review": {
            "top_research_opportunities": [
                {
                    "cluster": "matrix_alignment_wait",
                    "event_count": 2,
                    "share_of_quality_timing_rejects": 1.0,
                    "share_of_raw_all_gold_silver": 0.4,
                    "unique_tokens": 2,
                    "candidate_matched_any_rate": 1.0,
                    "stage_counts": [{"stage": "decision_no_pass_or_allow", "count": 2}],
                    "top_candidates": [
                        {"candidate_id": "entry_mode_registry:stage1", "family": "entry_mode_registry", "count": 2}
                    ],
                    "top_lifecycle_source_contexts": [
                        {
                            "lifecycle_profile": "ATH_SHALLOW_PULLBACK:OBSERVE",
                            "source_component": "matrix_evaluator",
                            "count": 2,
                        }
                    ],
                    "suggested_shadow_only_action": "track_matrix_alignment_false_negative_shadow_probe",
                    "human_approval_required_if_fix_requires": "changing matrix alignment thresholds",
                },
                {
                    "cluster": "momentum_fading_or_negative_trend",
                    "event_count": 1,
                    "share_of_quality_timing_rejects": 0.5,
                    "share_of_raw_all_gold_silver": 0.2,
                    "unique_tokens": 1,
                    "candidate_matched_any_rate": 1.0,
                    "time_to_sustained_peak_sec_median": 795,
                    "stage_counts": [{"stage": "pending_without_final_entry_contract", "count": 1}],
                    "top_candidates": [
                        {"candidate_id": "kline:first_bar_return_filters", "family": "kline", "count": 1},
                        {
                            "candidate_id": "entry_mode_registry:smart_entry_pullback_bounce",
                            "family": "entry_mode_registry",
                            "count": 1,
                        },
                    ],
                    "top_lifecycle_source_contexts": [
                        {
                            "lifecycle_profile": "FIRST_PUMP:PROBE",
                            "source_component": "smart_entry",
                            "count": 1,
                        }
                    ],
                    "suggested_shadow_only_action": "track_pending_momentum_decay_shadow_probe",
                    "human_approval_required_if_fix_requires": "changing momentum thresholds",
                },
            ]
        }
    }
    quality_timing_probe_validation = {
        "classification": "QUALITY_TIMING_PROBES_REPEATED_SAME_WINDOW",
        "next_action": "continue_shadow_probe_tracking_until_clean_window_then_oos",
        "registered_probe_count": 1,
        "validated_probe_count": 1,
        "repeated_probe_count": 1,
        "repeated_probe_rate": 1.0,
        "oos_readiness_queue_count": 1,
        "oos_readiness_queue": {
            "classification": "QUALITY_TIMING_OOS_QUEUE_PENDING_CLEAN_WINDOW",
            "queue_count": 1,
            "promotion_allowed": False,
        },
        "promotion_allowed": False,
    }
    decision_no_pass_watch_validation = {
        "classification": "DECISION_NO_PASS_QUALITY_TIMING_WATCH_REPEATED_SAME_WINDOW",
        "next_action": "continue_cluster_tracking_until_clean_window_then_oos",
        "registered_watch_count": 1,
        "validated_watch_count": 1,
        "repeated_selected_cluster_count": 1,
        "repeated_selected_cluster_rate": 1.0,
        "oos_readiness_queue": {
            "classification": "DECISION_NO_PASS_QUALITY_TIMING_OOS_QUEUE_PENDING_CLEAN_WINDOW",
            "queue_count": 1,
            "promotion_allowed": False,
        },
        "promotion_allowed": False,
    }
    capture_cross = {
        "valid_top_crosses": [
            {
                "candidate_id": "current_all",
                "dimension": "source_component",
                "slice_value": "matrix_evaluator",
                "judgment": "WATCH",
                "matched_gold_silver_events": 3,
                "slice_downstream_signal_count": 3,
                "downstream_lift_scope": "slice_level_matched_gold_silver_signal_id",
                "downstream_slice_join_available": True,
                "downstream_slice_join_required_for_promotion_evidence": False,
                "recall_lift_vs_candidate_baseline": 0.1,
                "precision_lift_vs_candidate_baseline": 0.05,
                "decision_lift": 0.1,
                "pass_allow_lift": 0.25,
                "pending_lift": 0.2,
                "final_entry_lift": 0.0,
                "mode_adjusted_final_eligibility_lift": 0.0,
                "decision_capture_rate_after_match": 1.0,
                "pass_allow_capture_rate_after_match": 0.75,
                "pending_capture_rate_after_match": 0.5,
                "final_entry_rate_after_match": 0.0,
                "mode_adjusted_final_eligibility_rate_after_match": 0.0,
                "match_precision_event": 0.3,
            }
        ]
    }
    markov = {
        "classification": "NON_INFORMATIVE",
        "profile_diagnostics": {},
        "non_informative_reasons": {"runtime": "insufficient"},
    }
    fixtures = {
        "capture": capture,
        "context_coverage": context,
        "volume_kline_coverage_audit": volume_kline,
        "context_blocker_monitor": context_blocker_monitor,
        "a_class": a_class,
        "strategy_memory_validation": strategy_validation,
        "strategy_memory_ingestion": ingestion,
        "candidate_improvement": candidate_improvement,
        "quality_timing_reject_research": quality_timing,
        "quality_timing_candidate_probe_validation": quality_timing_probe_validation,
        "decision_no_pass_quality_timing_watch_validation": decision_no_pass_watch_validation,
        "capture_cross": capture_cross,
        "markov_effectiveness": markov,
    }
    for key, filename in REQUIRED_FILES.items():
        write_json(run_dir / filename, fixtures.get(key, {}))
    return run_dir


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        run_dir = create_self_test_run(root)
        result = assemble_reports(run_dir)
        for key, filename in V3_OUTPUT_FILES.items():
            assert (run_dir / filename).exists(), (key, filename)
        gap = load_json(run_dir / "capture_60_gap_report.json")
        assert gap["promotion_allowed"] is False
        assert gap["raw_gold_silver_denominator"] == 5
        assert gap["target_60_count"] == 3
        assert gap["biggest_gap_stage"] == "pending_capture"
        assert gap["target_shortfall_stage"] == "pending_capture"
        assert gap["largest_transition_dropoff"]["to_stage"] in {
            "detector_capture",
            "decision_capture",
            "pass_allow_capture",
            "pending_capture",
            "final_eligibility",
            "mode_disabled_adjusted_final_eligibility",
            "paper_capture",
            "realized_capture",
        }
        assert len(gap["recommended_parallel_tracks"]) == 2
        assert all(item["promotion_allowed"] is False for item in gap["recommended_parallel_tracks"])
        assert gap["additional_count_needed_to_60"] == 1
        assert gap["next_best_allowed_action"] == (
            "audit_pass_allow_to_pending_bridge_shadow_only_with_blocked_context_dimensions_excluded"
        )
        stage_metrics = load_json(run_dir / "capture_stage_metrics.json")
        assert stage_metrics["raw_gold_silver_denominator"] == 5
        assert stage_metrics["target_60_count"] == 3
        assert stage_metrics["detector_capture_count"] == 5
        assert stage_metrics["detector_capture_rate"] == 1.0
        assert stage_metrics["decision_capture_count"] == 4
        assert stage_metrics["decision_capture_rate"] == 0.8
        assert stage_metrics["pass_allow_capture_count"] == 3
        assert stage_metrics["pass_allow_capture_rate"] == 0.6
        assert stage_metrics["pending_capture_count"] == 2
        assert stage_metrics["pending_capture_rate"] == 0.4
        assert stage_metrics["final_eligibility_count"] == 1
        assert stage_metrics["final_eligibility_capture_rate"] == 0.2
        assert stage_metrics["mode_disabled_adjusted_final_eligibility_count"] == 1
        assert stage_metrics["mode_disabled_adjusted_final_eligibility_rate"] == 0.2
        assert stage_metrics["paper_capture_count"] == 0
        assert stage_metrics["paper_capture_rate"] == 0.0
        pass_allow_gap = load_json(run_dir / "pass_allow_capture_gap_audit.json")
        assert pass_allow_gap["classification"] == "PASS_ALLOW_CAPTURE_60_REACHED_DISCOVERY_ONLY"
        assert pass_allow_gap["dominant_gap_stage"] is None
        assert pass_allow_gap["dominant_blocker"] is None
        assert pass_allow_gap["next_action"] == "pass_allow_capture_target_reached_continue_downstream_gap_audit"
        assert pass_allow_gap["promotion_allowed"] is False
        closure_plan = load_json(run_dir / "pass_allow_60_closure_plan.json")
        assert closure_plan["promotion_allowed"] is False
        assert closure_plan["schema_version"] == "pass_allow_60_closure_plan.v3"
        assert closure_plan["classification"] == "PASS_ALLOW_60_CLOSURE_NOT_NEEDED"
        assert closure_plan["target_gap"]["additional_pass_allow_events_needed_to_60"] == 0
        residual_tracks = closure_plan["residual_gap_supplemental_tracks"]
        assert residual_tracks["residual_gap_after_selected_clusters"] == 0
        assert residual_tracks["clean_2d_pass_allow_lift_slice_count"] == 1
        assert residual_tracks["clean_2d_positive_pass_allow_lift_count"] == 1
        assert residual_tracks["supplemental_tracks_can_cover_residual_upper_bound"] is True
        assert residual_tracks["promotion_allowed"] is False
        assert closure_plan["closure_tracks"]["clean_2d_pass_allow_lift_slices"]["count"] == 1
        clean_2d_item = closure_plan["closure_tracks"]["clean_2d_pass_allow_lift_slices"]["items"][0]
        assert clean_2d_item["pass_allow_lift"] == 0.25
        assert clean_2d_item["downstream_lift_scope"] == "slice_level_matched_gold_silver_signal_id"
        assert clean_2d_item["slice_downstream_signal_count"] == 3
        freeze_registry = load_json(run_dir / "pass_allow_60_oos_freeze_registry.json")
        assert freeze_registry["promotion_allowed"] is False
        assert freeze_registry["classification"] == "PASS_ALLOW_60_OOS_FREEZE_NOT_NEEDED"
        assert freeze_registry["frozen_definition_count"] >= 1
        assert freeze_registry["definition_set_frozen_at"]
        assert freeze_registry["items"][0]["frozen_at"]
        assert freeze_registry["items"][0]["definition_fingerprint"]
        assert freeze_registry["items"][0]["oos_requirements"]["overlap"] is False
        context = load_json(run_dir / "context_dimension_eligibility.json")
        assert context["dimensions"]["quote-sensitive"]["status"] == STATUS_CLEAN
        assert context["dimensions"]["quote-sensitive"]["eligible_for_capture_cross"] is True
        assert "quote-sensitive" not in context["blocked_dimensions"]
        assert (
            context["dimensions"]["quote-sensitive"]["evidence"][
                "quote_clean_window_pending_reconciled_by_current_coverage"
            ]
            is True
        )
        assert context["dimensions"]["source_component"]["status"] == STATUS_CLEAN
        assert context["dimensions"]["volume"]["status"] == STATUS_BLOCKED
        assert context["dimensions"]["volume"]["coverage_rate"] == 0.5
        assert context["dimensions"]["volume"]["evidence"]["field_present_rate"] == 1.0
        assert context["dimensions"]["volume"]["evidence"]["known_rate"] == 0.5
        assert "volume_profile_coverage_below_80pct" in context["dimensions"]["volume"]["blockers"]
        assert context["dimensions"]["kline"]["coverage_rate"] == 0.4
        assert "kline_coverage_below_80pct" in context["dimensions"]["kline"]["blockers"]
        partial_markov = build_context_dimension_eligibility({
            "markov_effectiveness": {
                "profile_diagnostics": {
                    "candidate_lifecycle": {
                        "informative_bucket_count": 4,
                        "status": "informative_discovery_only",
                    },
                    "kline": {
                        "informative_bucket_count": 0,
                        "status": "profile_over_fragmented_or_min_closed_not_met",
                    },
                },
                "non_informative_reasons": {
                    "kline": "closed_rows_exist_but_no_bucket_reached_min_closed",
                },
                "context_blockers": [],
            }
        })
        assert partial_markov["dimensions"]["Markov"]["status"] == STATUS_CLEAN
        assert partial_markov["dimensions"]["Markov"]["eligible_for_capture_cross"] is True
        assert "Markov" in partial_markov["clean_dimensions"]
        assert "Markov" not in partial_markov["blocked_dimensions"]
        assert partial_markov["dimensions"]["Markov"]["evidence"]["partial_profile_blockers"]["kline"]
        pending = load_json(run_dir / "pending_to_final_entry_audit.json")
        assert pending["dropoff_counts"]["pending_no_final_entry"] == 1
        assert pending["pending_no_final_entry_classification"]["categories"]["stale_before_final"] == 1
        transition_review = pending["largest_transition_dropoff_review"]
        assert transition_review["promotion_allowed"] is False
        assert transition_review["adjacent_count_loss_pending_to_final"] == 1
        assert transition_review["selected_categories_to_cover_largest_transition_dropoff_upper_bound"][0]["category"] == "stale_before_final"
        assert transition_review["selected_categories_to_cover_largest_transition_dropoff_upper_bound"][0]["promotion_allowed"] is False
        stale_review = pending["stale_before_final_review"]
        assert stale_review["promotion_allowed"] is False
        assert stale_review["stale_before_final_event_count"] == 1
        assert stale_review["selected_clusters_to_cover_stale_before_final_upper_bound"][0]["cluster"] == "momentum_fading_or_negative_trend"
        momentum_review = stale_review["momentum_decay_review"]
        assert momentum_review["promotion_allowed"] is False
        assert momentum_review["recheck_window_classification"] == "RECHECK_WINDOW_EXISTS_BEFORE_SUSTAINED_PEAK"
        assert len(momentum_review["selected_shadow_probes"]) == 3
        strategy = load_json(run_dir / "strategy_memory_capture_validation.json")
        assert strategy["promotion_allowed"] is False
        assert strategy["hypotheses_count"] == 1
        queue = load_json(run_dir / "shadow_candidate_improvement_queue.json")
        assert queue["promotion_allowed"] is False
        assert queue["schema_version"] == "shadow_candidate_improvement_queue.v2"
        assert queue["queue_count"] >= 3
        assert queue["source_counts"]["quality_timing_reject_cluster"] == 2
        assert queue["source_counts"]["clean_2d_capture_cross_slice"] == 1
        assert queue["source_counts"]["pending_to_final_stale_before_final_cluster"] == 1
        assert queue["source_counts"]["pending_momentum_decay_shadow_probe"] == 3
        assert queue["source_counts"]["pending_to_final_transition_dropoff_category"] == 1
        assert queue["top_opportunities"] == queue["top_items"]
        assert any(
            item.get("candidate_id") == "quality_timing:matrix_alignment_wait"
            and item.get("expected_capture_stage_improved") == "pass_allow_capture"
            for item in queue["top_items"]
        )
        assert any(
            item.get("candidate_id") == "pending_to_final:stale_before_final"
            and item.get("expected_capture_stage_improved") == "final_eligibility"
            and item.get("promotion_allowed") is False
            for item in queue["top_items"]
        )
        assert any(
            item.get("candidate_id") == "pending_to_final_stale:momentum_fading_or_negative_trend"
            and item.get("expected_capture_stage_improved") == "final_eligibility"
            and item.get("promotion_allowed") is False
            for item in queue["top_items"]
        )
        assert any(
            item.get("candidate_id") == "pending_momentum_decay:timeboxed_recheck_window"
            and item.get("hypothesis_source") == "pending_momentum_decay_shadow_probe"
            and item.get("promotion_allowed") is False
            for item in queue["top_items"]
        )
        assert any(
            item.get("candidate_id") == "current_all"
            and item.get("hypothesis_source") == "clean_2d_capture_cross_slice"
            and item.get("expected_capture_stage_improved") == "pass_allow_capture"
            and (item.get("evidence") or {}).get("downstream_lift_scope") == "slice_level_matched_gold_silver_signal_id"
            and (item.get("evidence") or {}).get("slice_downstream_signal_count") == 3
            for item in queue["top_items"]
        )
        oos = load_json(run_dir / "oos_readiness_summary.json")
        assert oos["quality_timing_probe_validation"]["oos_readiness_queue_count"] == 1
        assert oos["next_quality_timing_oos_action"] == (
            "hold_repeated_quality_timing_probes_until_clean_window_then_non_overlapping_oos"
        )
        assert oos["decision_no_pass_quality_timing_watch_validation"]["oos_readiness_queue_count"] == 1
        assert oos["next_decision_no_pass_quality_timing_oos_action"] == (
            "hold_repeated_decision_no_pass_clusters_until_clean_window_then_non_overlapping_oos"
        )
        assert oos["pass_allow_60_closure_plan"]["available"] is True
        assert oos["pass_allow_60_closure_oos_queue"]["classification"] == (
            "PASS_ALLOW_60_CLOSURE_OOS_NOT_NEEDED"
        )
        assert oos["pass_allow_60_closure_oos_queue_count"] >= 1
        assert oos["pass_allow_60_oos_freeze_registry"]["available"] is True
        assert oos["pass_allow_60_oos_frozen_definition_count"] >= 1
        monitor = oos["pass_allow_60_oos_readiness_monitor"]
        assert monitor["promotion_allowed"] is False
        assert monitor["frozen_definition_count"] >= 1
        assert monitor["definition_context_clean_count"] >= 1
        assert monitor["definition_context_blocked_count"] == 0
        assert "quote-sensitive" not in monitor["global_blocked_dimensions"]
        assert "volume" in monitor["global_blocked_dimensions_not_required_by_frozen_definitions"]
        post_freeze_summary = build_oos_summary(run_dir, reports={
            "pass_allow_60_post_freeze_oos_validation": {
                "schema_version": "pass_allow_60_post_freeze_oos_validation.v2",
                "classification": "PASS_ALLOW_60_POST_FREEZE_OOS_TOO_SMALL",
                "next_action": "continue_collecting_post_freeze_oos_window",
                "raw_gold_silver_event_rows": 0,
                "raw_gold_silver_rows_since_eval_start_unfiltered": 0,
                "all_raw_rows_since_eval_start": 4,
                "latest_raw_signal_age_sec": 90,
                "latest_raw_gold_silver_age_sec": 900,
                "latest_raw_gold_silver_lag_sec_before_eval_start": 600,
                "post_freeze_usable_hours": 0.3,
                "validated_definition_count": 45,
                "repeat_watch_count": 0,
                "promotion_allowed": False,
            }
        })
        post_freeze = post_freeze_summary["pass_allow_60_post_freeze_oos_validation"]
        assert post_freeze["schema_version"] == "pass_allow_60_post_freeze_oos_validation.v2"
        assert post_freeze["raw_gold_silver_rows_since_eval_start_unfiltered"] == 0
        assert post_freeze["all_raw_rows_since_eval_start"] == 4
        assert post_freeze["latest_raw_signal_age_sec"] == 90
        assert result["summary"]["biggest_gap_stage"] == "pending_capture"
        first_frozen_at = freeze_registry["definition_set_frozen_at"]
        second_result = assemble_reports(run_dir)
        second_freeze = load_json(run_dir / "pass_allow_60_oos_freeze_registry.json")
        assert second_result["summary"]["biggest_gap_stage"] == "pending_capture"
        assert second_freeze["definition_set_frozen_at"] == first_frozen_at
        assert second_freeze["definition_set_unchanged_from_previous_registry"] is True
    print("SELF_TEST_PASS capture_60_target_loop")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", default="/app/data/agent_runs/latest")
    parser.add_argument("--out-dir", default=None)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        self_test()
        return
    result = assemble_reports(args.run_dir, args.out_dir)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
