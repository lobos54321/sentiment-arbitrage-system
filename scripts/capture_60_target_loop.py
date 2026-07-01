#!/usr/bin/env python3
"""Assemble the Gold/Silver 60% Capture AutoLoop v3 target artifacts.

Read-only target layer. This script consumes already materialized AutoLoop
reports and writes the v3 target-driven capture artifacts. It never reads or
writes production strategy, gates, final_entry_contract, A_CLASS mode,
executors, wallets, canary settings, or risk settings.
"""

from __future__ import annotations

import argparse
import json
import math
import tempfile
import time
from collections import Counter
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
    "capture_cross": "capture_cross_validity_24h.json",
    "markov_effectiveness": "markov_effectiveness_24h.json",
    "pnl_secondary": "pnl_cross_secondary_24h.json",
    "oos_refresh": "oos_readiness_probe_refresh.json",
}

V3_OUTPUT_FILES = {
    "capture_60_gap_report": "capture_60_gap_report.json",
    "capture_stage_metrics": "capture_stage_metrics.json",
    "context_dimension_eligibility": "context_dimension_eligibility.json",
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
    return {
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
    for row in category_rows:
        count = safe_int(row.get("count"), 0)
        category = str(row.get("category") or "")
        reasons = row.get("top_reasons") or []
        text = " ".join(str(reason.get("reason") or "") for reason in reasons).lower()
        if "stale" in text:
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
        top_reason_rows.extend(reasons[:5])
    if pending_without_final and not any(required_categories.values()):
        required_categories["unknown"] = pending_without_final
    dominant = max(required_categories.items(), key=lambda item: item[1])[0] if required_categories else "unknown"
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
        "upstream_funnel_gap": upstream,
        "pending_to_final_entry_gap": pending_gap,
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
    quote_pending = (
        monitor_overall.get("rolling24_quote_status") == "QUOTE_CLEAN_WINDOW_PENDING"
        or clean_window.get("classification") == "QUOTE_CLEAN_WINDOW_PENDING"
        or (quote_writer_verified and quote_rate < 0.8)
    )
    quote_writer_bug = quote_root.get("dominant_root_cause") == "v2_writer_path_missing_quote_fields"
    field_writer_verified = field_smoke.get("classification") == "VERIFIED_POST_DEPLOY" or monitor_overall.get("context_field_writer_fix") == "VERIFIED_POST_DEPLOY"

    source_rate = coverage_rate(context_report.get("source_component_coverage") or {})
    lifecycle_rate = coverage_rate(context_report.get("lifecycle_profile_coverage") or {})
    volume_rate = coverage_rate(context_report.get("volume_profile_coverage") or {})
    kline_rate = coverage_rate(context_report.get("kline_coverage") or {})
    markov_diag = markov.get("profile_diagnostics") or {}
    markov_informative = any((row or {}).get("informative_bucket_count", 0) for row in markov_diag.values())
    markov_blocked = bool(markov.get("context_blockers") or markov.get("non_informative_reasons"))
    dimensions = {
        "quote-sensitive": {
            "status": dimension_status_from_rate(quote_rate, pending=quote_pending, writer_bug=quote_writer_bug),
            "eligible_for_capture_cross": quote_rate >= 0.8 and not quote_writer_bug,
            "coverage_rate": quote_rate,
            "blockers": sorted(blocker for blocker in blockers if "quote" in blocker or "schema" in blocker),
            "evidence": quote_cov,
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
            "blockers": sorted(blocker for blocker in blockers if "volume" in blocker),
            "evidence": context_report.get("volume_profile_coverage") or {},
        },
        "kline": {
            "status": dimension_status_from_rate(kline_rate),
            "eligible_for_capture_cross": kline_rate is not None and kline_rate >= 0.8,
            "coverage_rate": kline_rate,
            "blockers": sorted(blocker for blocker in blockers if "kline" in blocker),
            "evidence": context_report.get("kline_coverage") or {},
        },
        "Markov": {
            "status": STATUS_CLEAN if markov_informative and not markov_blocked else (STATUS_NA if not markov else STATUS_BLOCKED),
            "eligible_for_capture_cross": bool(markov_informative and not markov_blocked),
            "coverage_rate": None,
            "blockers": sorted(set(markov.get("context_blockers") or [])),
            "evidence": {
                "classification": markov.get("classification"),
                "profile_count": len(markov_diag),
                "informative_profile_count": sum(1 for row in markov_diag.values() if (row or {}).get("informative_bucket_count", 0)),
                "non_informative_reasons": markov.get("non_informative_reasons") or {},
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
        "largest_stage_dropoff": stage_metrics.get("largest_stage_dropoff") or {},
        "additional_count_needed_to_60": additional_needed,
        "next_best_allowed_action": next_best_allowed_action(biggest_gap_stage, context_eligibility, pending_audit),
        "human_approval_required_before_runtime_change": True,
        "notes": [
            "This is a target-gap report, not a promotion report.",
            "All suggested actions are constrained to evaluator, data, shadow-only, or human-approval handoff paths.",
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
        if dimension and dimension not in clean_dimensions and dimension not in {"signal_type", "mode_route"}:
            continue
        items.append({
            "candidate_id": row.get("candidate_id"),
            "hypothesis_source": "clean_2d_capture_cross_slice",
            "expected_capture_stage_improved": "detector_capture",
            "required_features": [dimension] if dimension else [],
            "time_legal_status": "context_slice_time_legal_not_proven",
            "context_blockers": row.get("invalid_reasons") or [],
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "evidence": {
                "recall_lift": row.get("recall_lift_vs_candidate_baseline"),
                "precision_lift": row.get("precision_lift_vs_candidate_baseline"),
                "matched_gold_silver_events": row.get("matched_gold_silver_events"),
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
    status_counts = Counter(row.get("hypothesis_source") for row in items)
    return {
        "schema_version": "shadow_candidate_improvement_queue.v1",
        "report_type": "shadow_candidate_improvement_queue",
        "generated_at": utc_now(),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "queue_count": len(items),
        "source_counts": dict(status_counts),
        "top_items": items[:75],
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
    ):
        path = run_dir / name
        if path.exists():
            key = name.replace(".json", "")
            key = key.replace("hypothesis_validation_audit_oos_probe_", "hypothesis_validation_oos_probe_")
            key = key.replace("matured_volume_capture_cross_audit_oos_probe_", "matured_volume_cross_oos_probe_")
            reports[key] = load_json(path, {})
    return reports


def build_oos_summary(run_dir):
    reports = load_oos_reports(run_dir)
    if build_oos_readiness_summary:
        summary = build_oos_readiness_summary(reports)
    else:
        summary = {
            "classification": "OOS_PROBES_MISSING" if not reports else "OOS_REPORTS_AVAILABLE",
            "available_probe_count": len(reports),
            "promotion_allowed": False,
            "probes": [],
        }
    summary["schema_version"] = "oos_readiness_summary.v1"
    summary["report_type"] = "oos_readiness_summary"
    summary["generated_at"] = utc_now()
    summary["promotion_allowed"] = False
    return summary


def assemble_reports(run_dir, out_dir=None):
    run_dir = Path(run_dir)
    out_dir = Path(out_dir) if out_dir else run_dir
    reports = collect_reports(run_dir)
    stage_metrics = build_capture_stage_metrics(reports.get("a_class") or {})
    context_eligibility = build_context_dimension_eligibility(reports)
    pending_audit = build_pending_to_final_entry_audit(reports.get("a_class") or {})
    reports["pending_to_final_entry_audit"] = pending_audit
    gap_report = build_capture_60_gap_report(stage_metrics, context_eligibility, pending_audit)
    final_entry_readiness = build_final_entry_readiness_audit(reports.get("a_class") or {}, stage_metrics, pending_audit)
    strategy_memory_capture = build_strategy_memory_capture_validation(reports)
    shadow_queue = build_shadow_candidate_improvement_queue(reports, context_eligibility)
    oos_summary = build_oos_summary(run_dir)
    payloads = {
        "capture_60_gap_report": gap_report,
        "capture_stage_metrics": stage_metrics,
        "context_dimension_eligibility": context_eligibility,
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
        "report_health": {"promotion_blockers": ["volume_profile_coverage_below_80pct"]},
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
        "volume_profile_coverage": {"coverage_rate": 0.5, "blocker": "volume_profile_coverage_below_80pct"},
        "kline_coverage": {"coverage_rate": 0.47, "blocker": "kline_coverage_below_80pct"},
        "blockers": ["volume_profile_coverage_below_80pct", "kline_coverage_below_80pct"],
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
                        {"category": "DATA_OR_MARKET_CONTEXT_BLOCK", "count": 1, "top_reasons": [{"reason": "quote_missing"}]}
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
                }
            ]
        }
    }
    capture_cross = {
        "valid_top_crosses": [
            {
                "candidate_id": "current_all",
                "dimension": "source_component",
                "slice_value": "matrix_evaluator",
                "judgment": "WATCH",
                "matched_gold_silver_events": 3,
                "recall_lift_vs_candidate_baseline": 0.1,
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
        "a_class": a_class,
        "strategy_memory_validation": strategy_validation,
        "strategy_memory_ingestion": ingestion,
        "candidate_improvement": candidate_improvement,
        "quality_timing_reject_research": quality_timing,
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
        assert gap["additional_count_needed_to_60"] == 1
        assert gap["next_best_allowed_action"] == (
            "audit_pass_allow_to_pending_bridge_shadow_only_with_blocked_context_dimensions_excluded"
        )
        context = load_json(run_dir / "context_dimension_eligibility.json")
        assert context["dimensions"]["source_component"]["status"] == STATUS_CLEAN
        assert context["dimensions"]["volume"]["status"] == STATUS_BLOCKED
        pending = load_json(run_dir / "pending_to_final_entry_audit.json")
        assert pending["dropoff_counts"]["pending_no_final_entry"] == 1
        assert pending["pending_no_final_entry_classification"]["categories"]["quote_missing"] == 1
        strategy = load_json(run_dir / "strategy_memory_capture_validation.json")
        assert strategy["promotion_allowed"] is False
        assert strategy["hypotheses_count"] == 1
        queue = load_json(run_dir / "shadow_candidate_improvement_queue.json")
        assert queue["promotion_allowed"] is False
        assert queue["queue_count"] >= 2
        assert queue["source_counts"]["quality_timing_reject_cluster"] == 1
        assert any(
            item.get("candidate_id") == "quality_timing:matrix_alignment_wait"
            and item.get("expected_capture_stage_improved") == "pass_allow_capture"
            for item in queue["top_items"]
        )
        assert result["summary"]["biggest_gap_stage"] == "pending_capture"
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
