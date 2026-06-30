#!/usr/bin/env python3
"""Build a reviewer verdict for the gold/silver capture discovery loop.

Read-only. This script consumes materialized discovery reports and produces a
single audit verdict. It never changes strategy, gates, executor, or risk.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "capture_discovery_reviewer_verdict.v5"
EXPECTED_CANDIDATE_COUNT = 84
EXPECTED_CONTEXT_SCHEMA_VERSION = "candidate-shadow-context-v2.no_signal_price_quote_inference"
EXPECTED_QUOTE_CLEAN_DEFINITION = "source_or_executable_quote_only_no_signal_price"

H1_CANDIDATES = {
    "kline:active_mom20_first3",
    "kline:lowvol_active20_support",
}
H2_CANDIDATES = {
    "entry_mode_registry:pullback_tiny_scout",
    "entry_mode_registry:smart_entry_pullback_bounce",
    "entry_mode_registry:source_resonance_tiny_probe",
    "entry_mode_registry:hard_gate_pass_tiny_probe",
    "entry_mode_registry:momentum_direct_entry",
}
JUDGMENT_ORDER = {
    "DISCOVERY_HIT": 4,
    "WATCH": 3,
    "TOO_SMALL": 2,
    "NO_SIGNAL": 1,
    "REJECT": 0,
}


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path):
    if not path:
        return None
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def boolish(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def top_slice_key(row):
    return (
        JUDGMENT_ORDER.get(row.get("judgment"), -1),
        as_float(row.get("recall_lift_vs_candidate_baseline")) or -999.0,
        as_float(row.get("match_recall_event")) or -1.0,
        as_float(row.get("match_precision_event")) or -1.0,
        as_int(row.get("matched_gold_silver_events")) or 0,
    )


def compact_capture_row(row):
    keys = (
        "candidate_id",
        "family",
        "dimension",
        "slice_value",
        "judgment",
        "signal_count",
        "match_count",
        "gold_silver_event_denominator",
        "gold_silver_unique_denominator",
        "matched_gold_silver_events",
        "matched_gold_silver_unique",
        "match_recall_event",
        "match_recall_unique",
        "match_precision_event",
        "match_precision_unique",
        "raw_all_gold_silver_event_denominator",
        "matched_raw_all_gold_silver_events",
        "business_match_recall_event",
        "recall_lift_vs_candidate_baseline",
        "precision_lift_vs_candidate_baseline",
    )
    return {key: row.get(key) for key in keys if key in row}


def compact_matured_volume_slice(row):
    keys = (
        "candidate_id",
        "family",
        "dimension",
        "slice_value",
        "verdict",
        "slice_signal_count",
        "slice_raw_gs_count",
        "candidate_match_count",
        "matched_gs_count",
        "match_recall_event",
        "match_precision_event",
        "candidate_baseline_recall_event",
        "candidate_baseline_precision_event",
        "recall_lift_vs_candidate_baseline",
        "precision_lift_vs_candidate_baseline",
        "promotion_allowed",
    )
    return {key: row.get(key) for key in keys if key in row}


def hypothesis_metrics(capture, name):
    slices = capture.get("context_slices") or []
    if name == "H1":
        rows = [
            row for row in slices
            if row.get("candidate_id") in H1_CANDIDATES
            and row.get("dimension") == "volume_profile"
            and str(row.get("slice_value")).lower() == "building"
        ]
        definition = {
            "name": "building_volume_active_microstructure",
            "candidate_ids": sorted(H1_CANDIDATES),
            "required_slice": "volume_profile=building",
        }
    else:
        rows = [
            row for row in slices
            if (
                row.get("candidate_id") in H2_CANDIDATES
                and row.get("dimension") == "lifecycle_profile"
                and row.get("slice_value") == "ATH_SHALLOW_PULLBACK:OBSERVE"
            )
            or (
                row.get("candidate_id") in H2_CANDIDATES
                and row.get("dimension") == "source_component"
                and "matrix_evaluator" in str(row.get("slice_value") or "")
            )
        ]
        definition = {
            "name": "shallow_pullback_matrix_evaluator",
            "candidate_ids": sorted(H2_CANDIDATES),
            "required_context": [
                "lifecycle_profile=ATH_SHALLOW_PULLBACK:OBSERVE",
                "source_component contains matrix_evaluator",
            ],
        }
    rows = sorted(rows, key=top_slice_key, reverse=True)
    return {
        "definition": definition,
        "rows_found": len(rows),
        "best_slice": compact_capture_row(rows[0]) if rows else None,
        "slices": [compact_capture_row(row) for row in rows[:10]],
        "status": "not_observed" if not rows else rows[0].get("judgment", "NO_SIGNAL"),
    }


def pnl_status(pnl):
    if not pnl:
        return {
            "available": False,
            "status": "missing",
            "evidence_role": "secondary_pnl_after_match",
        }
    counts = pnl.get("judgment_counts") or {}
    status = "secondary_only"
    if (counts.get("PROMISING") or 0) > 0:
        status = "pnl_promising_secondary_only"
    elif (counts.get("WATCH") or 0) > 0:
        status = "pnl_watch_secondary_only"
    return {
        "available": True,
        "status": status,
        "report_type": pnl.get("report_type"),
        "evidence_role": pnl.get("evidence_role"),
        "can_promote_live": boolish(pnl.get("can_promote_live")),
        "coverage": pnl.get("coverage") or {},
        "judgment_counts": counts,
    }


def markov_status(markov_reports):
    out = {
        "available": bool(markov_reports),
        "status": "missing" if not markov_reports else "discovery_only",
        "can_promote_live": False,
        "profiles": {},
    }
    green = 0
    yellow = 0
    for name, report in sorted(markov_reports.items()):
        coverage = report.get("coverage") or {}
        counts = coverage.get("bucket_counts") or {}
        green += int(counts.get("green") or 0)
        yellow += int(counts.get("yellow") or 0)
        out["profiles"][name] = {
            "profile": report.get("profile") or name,
            "coverage": coverage,
            "bucket_counts": counts,
            "schema_version": report.get("schema_version"),
        }
    if green:
        out["status"] = "green_bucket_discovery_only"
    elif yellow:
        out["status"] = "yellow_bucket_discovery_only"
    return out


def build_verdict(capture, pnl=None, markov_reports=None, *, tests=None, oos_gate_passed=False, readiness_reports=None):
    markov_reports = markov_reports or {}
    readiness_reports = readiness_reports or {}
    tests = tests or {}
    coverage = capture.get("coverage") or {}
    context = capture.get("context_health") or {}
    denominator = capture.get("raw_gold_silver_denominator") or {}
    raw_join = capture.get("raw_dog_observation_join") or {}
    signal_reconciliation = capture.get("signal_identity_reconciliation") or {}
    denominator_split = capture.get("denominator_split") or signal_reconciliation.get("denominator_split") or {}
    report_health = capture.get("report_health") or {}
    quote_context_coverage = capture.get("quote_context_coverage") or context.get("quote_context_coverage") or {}
    quote_missing_root_cause = capture.get("quote_missing_root_cause") or context.get("quote_missing_root_cause") or {}
    context_blocker_monitor = readiness_reports.get("context_blocker_monitor") or {}
    context_monitor_overall = context_blocker_monitor.get("overall_verdict") or {}
    context_monitor_quote_smoke = context_blocker_monitor.get("task_a_post_deploy_quote_smoke_test") or {}
    context_monitor_clean_window = context_blocker_monitor.get("task_b_clean_window_monitor") or {}
    context_monitor_field_audit = context_blocker_monitor.get("task_d_context_field_coverage_audit") or {}
    blockers = list(report_health.get("promotion_blockers") or [])

    candidate_expected = capture.get("candidate_count_expected") or coverage.get("candidate_count_expected")
    candidate_observed = coverage.get("candidate_count_observed")
    observation_coverage_pct = coverage.get("coverage_pct")
    raw_rows_complete = denominator.get("rows_complete_against_summary")
    raw_all_signal_id_join_rate = signal_reconciliation.get("raw_all_signal_id_join_rate")
    mesh_eligible_signal_id_join_rate = signal_reconciliation.get("mesh_eligible_signal_id_join_rate")
    signal_join_rate = mesh_eligible_signal_id_join_rate if mesh_eligible_signal_id_join_rate is not None else raw_join.get("join_rate")
    schema_counts = context.get("context_schema_version_counts") or context.get("context_schema_versions") or {}
    quote_definition = {
        "expected": context.get("expected_quote_clean_definition") or EXPECTED_QUOTE_CLEAN_DEFINITION,
        "counts": context.get("quote_clean_definition_counts") or {},
        "coverage_pct": context.get("expected_quote_clean_definition_coverage_pct"),
        "quote_sensitive_slices_evaluable": boolish(context.get("quote_sensitive_slices_evaluable")),
    }

    if candidate_expected != EXPECTED_CANDIDATE_COUNT:
        blockers.append("candidate_count_expected_not_84")
    if candidate_observed != EXPECTED_CANDIDATE_COUNT:
        blockers.append("candidate_count_observed_not_84")
    if observation_coverage_pct is None or observation_coverage_pct < 99:
        blockers.append("observation_coverage_below_99pct")
    if raw_rows_complete is not True:
        blockers.append("raw_dog_rows_incomplete")
    unknown_unjoined = int(signal_reconciliation.get("unknown_unjoined") or 0)
    raw_all_unjoined_attributed = signal_reconciliation.get("raw_all_unjoined_fully_attributed") is True
    if (signal_join_rate is None or signal_join_rate < 0.99) and unknown_unjoined > 0:
        blockers.append("signal_id_join_rate_below_99pct")
    if signal_reconciliation and not raw_all_unjoined_attributed:
        blockers.append("raw_all_unjoined_not_fully_attributed")
    if not quote_definition["quote_sensitive_slices_evaluable"]:
        blockers.append("schema_mixed_quote_sensitive_slices_blocked")
    clean_present_rate = quote_context_coverage.get("source_quote_clean_present_rate")
    executable_present_rate = quote_context_coverage.get("source_quote_executable_present_rate")
    if clean_present_rate is not None and clean_present_rate < 0.8:
        blockers.append("source_quote_clean_coverage_below_80pct")
    if executable_present_rate is not None and executable_present_rate < 0.8:
        blockers.append("source_quote_executable_coverage_below_80pct")
    if tests and not tests.get("passed", False):
        blockers.append("tests_failed")

    context_monitor_warnings = set(context_monitor_field_audit.get("warnings") or [])
    reconciled_context_warnings = []
    if (
        "lifecycle_profile_coverage_below_80pct" in blockers
        and "lifecycle_profile_rolling_below_80_mature_context_ok" in context_monitor_warnings
    ):
        blockers = [
            blocker for blocker in blockers
            if blocker != "lifecycle_profile_coverage_below_80pct"
        ]
        reconciled_context_warnings.append("lifecycle_profile_coverage_reconciled_by_mature_context")

    blockers = sorted(set(blockers))
    quote_coverage_blockers = {
        "source_quote_clean_coverage_below_80pct",
        "source_quote_executable_coverage_below_80pct",
    }
    blocked_subtype = None
    if any(blocker in quote_coverage_blockers for blocker in blockers):
        dominant_quote_missing = quote_missing_root_cause.get("dominant_root_cause")
        monitor_writer_verified = (
            context_monitor_overall.get("quote_writer_fix") == "VERIFIED_POST_DEPLOY"
            or context_monitor_quote_smoke.get("classification") == "VERIFIED_POST_DEPLOY"
        )
        monitor_clean_pending = (
            context_monitor_overall.get("rolling24_quote_status") == "QUOTE_CLEAN_WINDOW_PENDING"
            or context_monitor_clean_window.get("classification") == "QUOTE_CLEAN_WINDOW_PENDING"
        )
        if monitor_writer_verified and monitor_clean_pending:
            blocked_subtype = "CLEAN_V2_WINDOW_PENDING"
        elif dominant_quote_missing == "legacy_schema":
            blocked_subtype = "CLEAN_V2_WINDOW_PENDING"
        elif dominant_quote_missing == "v2_writer_path_missing_quote_fields":
            blocked_subtype = "NEEDS_DATA_WRITER_FIX"
        elif dominant_quote_missing == "should_be_not_applicable":
            blocked_subtype = "NEEDS_NOT_APPLICABLE_CLASSIFICATION"
        else:
            blocked_subtype = "QUOTE_CONTEXT_COVERAGE"
    candidate_integrity_ok = (
        candidate_expected == EXPECTED_CANDIDATE_COUNT
        and candidate_observed == EXPECTED_CANDIDATE_COUNT
        and observation_coverage_pct is not None
        and observation_coverage_pct >= 99
        and raw_rows_complete is True
        and (signal_join_rate is not None and signal_join_rate >= 0.99 or unknown_unjoined == 0)
    )
    tests_ok = not tests or boolish(tests.get("passed", False))
    non_quote_sensitive_capture_discovery_allowed = bool(candidate_integrity_ok and tests_ok)
    quote_sensitive_slices_blocked = (
        any(blocker in quote_coverage_blockers for blocker in blockers)
        or not quote_definition["quote_sensitive_slices_evaluable"]
    )
    data_blockers = {
        "candidate_count_expected_not_84",
        "candidate_count_observed_not_84",
        "observation_coverage_below_99pct",
        "raw_dog_rows_incomplete",
        "signal_id_join_rate_below_99pct",
        "raw_all_unjoined_not_fully_attributed",
        "tests_failed",
        "report_generation_failed",
    }
    context_blockers = {
        "source_quote_clean_coverage_below_80pct",
        "source_quote_executable_coverage_below_80pct",
        "volume_profile_coverage_below_80pct",
        "kline_coverage_below_80pct",
        "schema_mixed_quote_sensitive_slices_blocked",
        "context_schema_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
        "quote_clean_definition_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
        "markov_bucket_coverage_below_80pct",
    }
    final_entry = readiness_reports.get("a_class_fastlane_mode_audit") or {}
    volume_kline_audit = readiness_reports.get("volume_kline_coverage_audit") or {}
    matured_kline_recheck = readiness_reports.get("matured_kline_volume_recheck_audit") or {}
    matured_volume_cross = readiness_reports.get("matured_volume_capture_cross_audit") or {}
    hypothesis_validation = readiness_reports.get("hypothesis_validation_audit") or {}
    matured_volume_top_slices = [
        compact_matured_volume_slice(row)
        for row in (matured_volume_cross.get("top_slices") or [])[:10]
    ]
    matured_volume_watch_slices = [
        row for row in matured_volume_top_slices
        if row.get("verdict") == "MATURED_VOLUME_DISCOVERY_WATCH"
    ]
    low_confidence_audit = readiness_reports.get("low_confidence_research_capture_audit") or {}
    final_entry_status = str(final_entry.get("final_entry_status") or "").upper()
    capture_counts = capture.get("judgment_counts") or {}
    if any(blocker in data_blockers for blocker in blockers):
        classification = "BLOCKED_DATA"
    elif any(blocker in context_blockers for blocker in blockers):
        classification = "BLOCKED_CONTEXT_COVERAGE"
    elif final_entry_status == "FUNNEL_BLOCKED_STUCK":
        classification = "FUNNEL_BLOCKED_STUCK"
    elif final_entry_status == "FUNNEL_BLOCKED_EXPECTED":
        classification = "FUNNEL_BLOCKED_EXPECTED"
    elif (capture_counts.get("DISCOVERY_HIT") or 0) > 0:
        classification = "CAPTURE_DISCOVERY_HIT"
    elif (capture_counts.get("WATCH") or 0) > 0:
        classification = "DISCOVERY_WATCH"
    else:
        classification = "DISCOVERY_NO_SIGNAL"

    human_action_required = classification == "HUMAN_APPROVAL_REQUIRED" or bool(
        final_entry.get("human_action_required")
    )
    promotion_allowed = False
    capture_stage_rates = final_entry.get("capture_stage_rates") or {}
    upstream_gap = capture_stage_rates.get("upstream_funnel_gap") or final_entry.get("upstream_funnel_gap") or {}
    pending_to_final_gap = capture_stage_rates.get("pending_to_final_entry_gap") or final_entry.get("pending_to_final_entry_gap") or {}
    mode_adjusted_final = final_entry.get("mode_disabled_adjusted_final_eligibility") or {}
    current_capture_stage = final_entry.get("current_capture_stage")
    top_blocker = blockers[0] if blockers else (
        final_entry.get("reason") or classification
    )
    if classification == "BLOCKED_DATA":
        next_action = "resolve_data_integrity_blocker"
    elif classification == "BLOCKED_CONTEXT_COVERAGE":
        next_action = "resolve_context_coverage_blocker"
    elif classification == "FUNNEL_BLOCKED_EXPECTED":
        next_action = "wait_clean_windows_or_fix_failed_context_coverage"
    elif classification == "FUNNEL_BLOCKED_STUCK":
        next_action = "human_review_a_class_shadow_state"
    elif classification == "CAPTURE_DISCOVERY_HIT":
        next_action = "freeze_hit_for_out_of_sample_validation"
    elif classification == "DISCOVERY_WATCH":
        next_action = "continue_shadow_discovery_and_watchlist_validation"
    else:
        next_action = "continue_capture_discovery"
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "phase": "discovery_mesh",
        "current_commit": readiness_reports.get("current_commit"),
        "deployment_commit": readiness_reports.get("deployment_commit"),
        "verdict": classification,
        "classification": classification,
        "next_action": next_action,
        "blocked_subtype": blocked_subtype,
        "promotion_allowed": promotion_allowed,
        "human_action_required": human_action_required,
        "current_capture_stage": current_capture_stage,
        "detector_capture_rate": capture_stage_rates.get("detector_capture_rate"),
        "decision_record_capture_rate": capture_stage_rates.get("decision_record_capture_rate"),
        "decision_capture_rate": capture_stage_rates.get("pass_allow_capture_rate"),
        "pending_capture_rate": capture_stage_rates.get("pending_capture_rate"),
        "final_entry_contract_reach_rate": capture_stage_rates.get("final_entry_contract_reach_rate"),
        "final_eligibility_capture_rate": (
            mode_adjusted_final.get("rate")
            if "rate" in mode_adjusted_final
            else capture_stage_rates.get("mode_disabled_adjusted_final_eligibility_rate")
        ),
        "paper_capture_rate": capture_stage_rates.get("paper_capture_rate"),
        "realized_capture_rate": capture_stage_rates.get("realized_capture_rate"),
        "upstream_funnel_gap_summary": {
            "raw_signal_ids": upstream_gap.get("raw_signal_ids"),
            "decision_record_signal_ids": upstream_gap.get("decision_record_signal_ids"),
            "pass_or_allow_signal_ids": upstream_gap.get("pass_or_allow_signal_ids"),
            "pending_entry_signal_ids": upstream_gap.get("pending_entry_signal_ids"),
            "no_decision_record": upstream_gap.get("no_decision_record"),
            "decision_no_pass_or_allow": upstream_gap.get("decision_no_pass_or_allow"),
            "pass_or_allow_without_pending_entry": upstream_gap.get("pass_or_allow_without_pending_entry"),
            "total_upstream_gap": upstream_gap.get("total_upstream_gap"),
            "decision_record_capture_rate": upstream_gap.get("decision_record_capture_rate"),
            "pass_allow_capture_rate": upstream_gap.get("pass_allow_capture_rate"),
            "pending_capture_rate": upstream_gap.get("pending_capture_rate"),
            "upstream_gap_category_counts": upstream_gap.get("upstream_gap_category_counts") or {},
            "upstream_gap_priority": upstream_gap.get("upstream_gap_priority") or {},
            "automatic_runtime_change_allowed": False,
            "strategy_change_allowed": False,
            "paper_enablement_allowed": False,
        },
        "entry_funnel_gap_summary": {
            "pending_entry_signal_ids": pending_to_final_gap.get("pending_entry_signal_ids"),
            "final_entry_contract_signal_ids": pending_to_final_gap.get("final_entry_contract_signal_ids"),
            "pending_without_final_entry_contract": pending_to_final_gap.get("pending_without_final_entry_contract"),
            "pending_to_final_entry_contract_rate": pending_to_final_gap.get("pending_to_final_entry_contract_rate"),
            "pending_to_mode_adjusted_final_eligibility_rate": pending_to_final_gap.get("pending_to_mode_adjusted_final_eligibility_rate"),
            "pending_without_final_entry_category_counts": (
                pending_to_final_gap.get("pending_without_final_entry_category_counts") or {}
            ),
            "readiness_gap_priority": pending_to_final_gap.get("readiness_gap_priority") or {},
            "automatic_runtime_change_allowed": False,
            "strategy_change_allowed": False,
            "paper_enablement_allowed": False,
        },
        "top_blocker": top_blocker,
        "non_quote_sensitive_capture_discovery_allowed": non_quote_sensitive_capture_discovery_allowed,
        "quote_sensitive_slices_blocked": quote_sensitive_slices_blocked,
        "canary_increase_allowed": False,
        "strategy_change_allowed": False,
        "hard_gate_change_allowed": False,
        "exit_gate_change_allowed": False,
        "blockers": blockers,
        "candidate_count_expected": candidate_expected,
        "candidate_count_observed": candidate_observed,
        "observation_coverage_pct": observation_coverage_pct,
        "raw_dog_rows_complete": raw_rows_complete is True,
        "signal_id_join_rate": signal_join_rate,
        "raw_all_signal_id_join_rate": raw_all_signal_id_join_rate,
        "mesh_eligible_signal_id_join_rate": mesh_eligible_signal_id_join_rate,
        "signal_identity_reconciliation": signal_reconciliation,
        "denominator_split": denominator_split,
        "v4_funnel_scope_vs_autoloop_scope_reconciliation": capture.get(
            "v4_funnel_scope_vs_autoloop_scope_reconciliation"
        ) or signal_reconciliation.get("v4_funnel_scope_vs_autoloop_scope_reconciliation"),
        "context_schema_version_counts": schema_counts,
        "quote_clean_definition": quote_definition,
        "quote_context_coverage": quote_context_coverage,
        "quote_missing_root_cause": quote_missing_root_cause,
        "context_field_coverage": (
            readiness_reports.get("context_coverage") or {}
        ).get("context_field_coverage") or (capture.get("context_health") or {}).get("context_field_coverage") or {},
        "lifecycle_profile_coverage": (
            (readiness_reports.get("context_coverage") or {}).get("lifecycle_profile_coverage")
            or ((capture.get("context_health") or {}).get("context_field_coverage") or {}).get("lifecycle_profile")
            or {}
        ),
        "source_component_coverage": (
            (readiness_reports.get("context_coverage") or {}).get("source_component_coverage")
            or ((capture.get("context_health") or {}).get("context_field_coverage") or {}).get("source_component")
            or {}
        ),
        "context_blocker_monitor": {
            "available": bool(context_blocker_monitor),
            "overall_verdict": context_monitor_overall,
            "post_deploy_quote_smoke_test": context_monitor_quote_smoke,
            "clean_window_monitor": context_monitor_clean_window,
            "volume_kline_coverage_audit": context_blocker_monitor.get("task_c_volume_kline_coverage_audit") or {},
            "context_field_coverage_audit": context_monitor_field_audit,
            "reconciled_warnings": reconciled_context_warnings,
        },
        "volume_profile_coverage": readiness_reports.get("volume_profile_coverage") or {},
        "kline_coverage": readiness_reports.get("kline_coverage") or {},
        "volume_kline_root_cause_audit": {
            "available": bool(volume_kline_audit),
            "overall": volume_kline_audit.get("overall") or {},
            "volume_context": {
                key: (volume_kline_audit.get("volume_context") or {}).get(key)
                for key in (
                    "rows_scanned",
                    "field_present_rate",
                    "known_rate",
                    "missing_rate",
                    "unknown_rate",
                    "value_counts",
                    "blocker",
                    "root_causes",
                    "missing_or_unknown_breakdown",
                )
            },
            "raw_gold_silver_kline": {
                key: (volume_kline_audit.get("raw_gold_silver_kline") or {}).get(key)
                for key in (
                    "raw_all_gold_silver_event_rows",
                    "raw_all_gold_silver_unique_tokens",
                    "kline_covered_rows",
                    "kline_uncovered_rows",
                    "kline_coverage_rate",
                    "coverage_reason_counts_uncovered",
                    "kline_uncovered_root_cause_counts",
                    "baseline_confidence_counts_uncovered",
                    "same_source_path_counts_uncovered",
                    "first_bar_lag_bucket_counts_uncovered",
                    "early_15m_complete_rate",
                    "low_confidence_research_audit",
                    "primary_denominator_drop_reason_counts",
                    "blocker",
                )
            },
        },
        "matured_kline_volume_recheck_audit": {
            "available": bool(matured_kline_recheck),
            "overall": matured_kline_recheck.get("overall") or {},
            "promotion_allowed": False,
            "formal_denominator_changed": bool(matured_kline_recheck.get("formal_denominator_changed")),
            "context_rows_scanned": matured_kline_recheck.get("context_rows_scanned"),
            "unknown_or_missing_rows": matured_kline_recheck.get("unknown_or_missing_rows"),
            "kline_cache_available": matured_kline_recheck.get("kline_cache_available"),
            "recheck": {
                key: (matured_kline_recheck.get("recheck") or {}).get(key)
                for key in (
                    "rechecked_rows",
                    "recoverable_known_rows",
                    "recoverable_known_rate",
                    "still_unknown_rows",
                    "still_unknown_rate",
                    "current_volume_profile_counts",
                    "current_volume_profile_reason_counts",
                    "current_kline_bar_count_bucket_counts",
                    "signal_age_bucket_counts_now",
                )
            },
        },
        "matured_volume_capture_cross_audit": {
            "available": bool(matured_volume_cross),
            "overall": matured_volume_cross.get("overall") or {},
            "promotion_allowed": False,
            "formal_denominator_changed": bool(matured_volume_cross.get("formal_denominator_changed")),
            "candidate_count_observed": matured_volume_cross.get("candidate_count_observed"),
            "signals_scanned": matured_volume_cross.get("signals_scanned"),
            "matured_volume_context": {
                key: (matured_volume_cross.get("matured_volume_context") or {}).get(key)
                for key in (
                    "kline_cache_available",
                    "signals_with_matured_context",
                    "known_rows",
                    "unknown_rows",
                    "known_rate",
                    "profile_counts",
                    "reason_counts",
                )
            },
            "denominator": {
                "raw_all_gold_silver": (matured_volume_cross.get("denominator") or {}).get("raw_all_gold_silver"),
                "evaluable_gold_silver": (matured_volume_cross.get("denominator") or {}).get("evaluable_gold_silver"),
            },
            "h1_matured_building_volume": matured_volume_cross.get("h1_matured_building_volume") or {},
            "judgment_counts": matured_volume_cross.get("judgment_counts") or {},
            "top_slices": matured_volume_top_slices,
            "top_watch_slices": matured_volume_watch_slices,
            "watch_slice_count": len(matured_volume_watch_slices),
            "next_research_action": (
                "review_non_h1_matured_volume_watch_slices"
                if matured_volume_watch_slices
                and (matured_volume_cross.get("h1_matured_building_volume") or {}).get("status") != "MATURED_VOLUME_DISCOVERY_WATCH"
                else (matured_volume_cross.get("overall") or {}).get("next_action")
            ),
        },
        "hypothesis_validation_audit": {
            "available": bool(hypothesis_validation),
            "overall": hypothesis_validation.get("overall") or {},
            "promotion_allowed": False,
            "matured_volume_hypothesis_validation": {
                key: (hypothesis_validation.get("matured_volume_hypothesis_validation") or {}).get(key)
                for key in (
                    "registry_frozen_before_eval_window",
                    "eval_window_quality",
                    "registered_hypothesis_count",
                    "found_in_current_report_count",
                    "repeated_watch_count",
                    "oos_repeated_watch_count",
                )
            },
        },
        "low_confidence_research_capture_audit": {
            "available": bool(low_confidence_audit),
            "verdict": low_confidence_audit.get("verdict"),
            "promotion_allowed": False,
            "formal_denominator_changed": bool(low_confidence_audit.get("formal_denominator_changed")),
            "denominator": {
                "raw_all_gold_silver": (low_confidence_audit.get("denominator") or {}).get("raw_all_gold_silver"),
                "formal_evaluable_gold_silver": (low_confidence_audit.get("denominator") or {}).get("formal_evaluable_gold_silver"),
                "low_confidence_research_gold_silver": (low_confidence_audit.get("denominator") or {}).get("low_confidence_research_gold_silver"),
                "low_confidence_31_60_gold_silver": (low_confidence_audit.get("denominator") or {}).get("low_confidence_31_60_gold_silver"),
            },
            "candidate_layer": {
                key: (low_confidence_audit.get("candidate_layer") or {}).get(key)
                for key in (
                    "full_candidate_coverage_rate",
                    "candidate_match_any_events",
                    "candidate_match_any_rate",
                    "top_candidates_by_low_confidence_raw_gs_match",
                )
            },
            "decision_layer": {
                key: (low_confidence_audit.get("decision_layer") or {}).get(key)
                for key in (
                    "decision_record_rate",
                    "would_enter_rate",
                    "entered_rate",
                    "realized_rate",
                    "terminal_bucket_counts",
                )
            },
            "blockers": low_confidence_audit.get("blockers") or [],
        },
        "A_CLASS_mode_status": readiness_reports.get("a_class_fastlane_mode_audit") or {},
        "final_entry_contract_blocker_breakdown": (
            (readiness_reports.get("a_class_fastlane_mode_audit") or {}).get("final_entry_contract_blocker_breakdown")
            or {}
        ),
        "per_candidate_effectiveness_summary": readiness_reports.get("candidate_effectiveness") or {},
        "Markov_effectiveness_summary": readiness_reports.get("markov_effectiveness") or {},
        "two_d_cross_validity_summary": readiness_reports.get("capture_cross_validity") or {},
        "next_highest_priority_blocker": readiness_reports.get("next_highest_priority_blocker"),
        "denominator_audit": capture.get("denominator_audit") or {},
        "raw_dog_observation_join": raw_join,
        "raw_all_dog_observation_join": capture.get("raw_all_dog_observation_join") or {},
        "H1_capture_metrics": hypothesis_metrics(capture, "H1"),
        "H2_capture_metrics": hypothesis_metrics(capture, "H2"),
        "PnL_cross_secondary_status": pnl_status(pnl),
        "virtual_Markov_discovery_status": markov_status(markov_reports),
        "capture_judgment_counts": capture_counts,
        "tests_passed": boolish(tests.get("passed")) if tests else None,
        "tests": tests,
        "notes": [
            "Same-window discovery verdict only; no promotion without future out-of-sample validation.",
            "PnL cross and virtual Markov are secondary discovery evidence.",
        ],
    }


def self_test():
    capture = {
        "candidate_count_expected": 84,
        "coverage": {
            "candidate_count_expected": 84,
            "candidate_count_observed": 84,
            "coverage_pct": 100.0,
        },
        "raw_gold_silver_denominator": {
            "rows_complete_against_summary": True,
        },
        "raw_dog_observation_join": {
            "join_rate": 1.0,
        },
        "signal_identity_reconciliation": {
            "joined_exact_signal_id": 1,
            "joined_by_signal_alias": 0,
            "joined_by_lifecycle_id": 0,
            "joined_by_token_time_high_confidence": 0,
            "outside_candidate_observer_window": 0,
            "not_mesh_eligible": 0,
            "missing_candidate_observation": 0,
            "raw_event_duplicate": 0,
            "raw_event_derived_no_signal": 0,
            "unknown_unjoined": 0,
            "raw_all_signal_id_join_rate": 1.0,
            "mesh_eligible_signal_id_join_rate": 1.0,
            "raw_all_unjoined_fully_attributed": True,
        },
        "context_health": {
            "context_schema_version_counts": {EXPECTED_CONTEXT_SCHEMA_VERSION: 10},
            "quote_clean_definition_counts": {EXPECTED_QUOTE_CLEAN_DEFINITION: 10},
            "expected_quote_clean_definition_coverage_pct": 100.0,
            "quote_sensitive_slices_evaluable": True,
        },
        "quote_context_coverage": {
            "coverage_denominator_type": "signal_context_carrier_rows",
            "coverage_denominator_rows": 10,
            "context_carrier_candidate_ids": ["current_all"],
            "source_quote_clean_present_rate": 1.0,
            "source_quote_executable_present_rate": 1.0,
            "source_quote_clean_true_rate": 0.5,
            "source_quote_clean_false_rate": 0.5,
            "source_quote_clean_missing_rate": 0.0,
            "source_quote_clean_unknown_rate": 0.0,
            "source_quote_clean_not_applicable_rate": 0.0,
            "source_quote_executable_true_rate": 0.4,
            "source_quote_executable_false_rate": 0.6,
            "source_quote_executable_missing_rate": 0.0,
            "source_quote_executable_unknown_rate": 0.0,
            "source_quote_executable_not_applicable_rate": 0.0,
        },
        "quote_missing_root_cause": {
            "schema_version": "quote_missing_root_cause_audit.v1",
            "quote_missing_rows_total": 0,
            "missing_due_to_legacy_schema_count": 0,
            "missing_due_to_writer_path_count": 0,
            "missing_should_be_not_applicable_count": 0,
            "missing_unknown_count": 0,
            "dominant_root_cause": "none",
        },
        "judgment_counts": {"DISCOVERY_HIT": 0, "WATCH": 1, "TOO_SMALL": 0, "NO_SIGNAL": 0},
        "context_slices": [
            {
                "candidate_id": "kline:active_mom20_first3",
                "family": "kline",
                "dimension": "volume_profile",
                "slice_value": "building",
                "judgment": "WATCH",
                "match_recall_event": 0.5,
                "match_precision_event": 0.2,
                "recall_lift_vs_candidate_baseline": 0.1,
            }
        ],
        "report_health": {"promotion_blockers": []},
    }
    verdict = build_verdict(capture, tests={"passed": True})
    assert verdict["classification"] == "DISCOVERY_WATCH"
    assert verdict["promotion_allowed"] is False
    assert verdict["candidate_count_observed"] == 84
    assert verdict["H1_capture_metrics"]["rows_found"] == 1
    assert verdict["blocked_subtype"] is None
    assert verdict["non_quote_sensitive_capture_discovery_allowed"] is True
    assert verdict["quote_sensitive_slices_blocked"] is False
    assert verdict["quote_context_coverage"]["coverage_denominator_type"] == "signal_context_carrier_rows"
    assert verdict["matured_kline_volume_recheck_audit"]["available"] is False
    assert verdict["matured_volume_capture_cross_audit"]["available"] is False
    assert verdict["low_confidence_research_capture_audit"]["available"] is False
    stage_verdict = build_verdict(capture, tests={"passed": True}, readiness_reports={
        "a_class_fastlane_mode_audit": {
            "final_entry_status": "FUNNEL_BLOCKED_EXPECTED",
            "reason": "cooldown_elapsed_requires_clean_windows",
            "current_capture_stage": "mode_disabled_clean_window_pending",
            "capture_stage_rates": {
                "detector_capture_rate": 1.0,
                "decision_record_capture_rate": 0.9,
                "pass_allow_capture_rate": 0.5,
                "pending_capture_rate": 0.3,
                "final_entry_contract_reach_rate": 0.02,
                "paper_capture_rate": 0.0,
                "upstream_funnel_gap": {
                    "raw_signal_ids": 10,
                    "decision_record_signal_ids": 9,
                    "pass_or_allow_signal_ids": 5,
                    "pending_entry_signal_ids": 3,
                    "no_decision_record": 1,
                    "decision_no_pass_or_allow": 4,
                    "pass_or_allow_without_pending_entry": 2,
                    "total_upstream_gap": 7,
                    "decision_record_capture_rate": 0.9,
                    "pass_allow_capture_rate": 0.5,
                    "pending_capture_rate": 0.3,
                    "upstream_gap_category_counts": {
                        "total_classified": 7,
                        "categories": [{"category": "QUALITY_OR_TIMING_REJECT", "count": 4}],
                    },
                    "upstream_gap_priority": {
                        "current_shortfall_to_60_pending": 3,
                        "categories_ranked_by_optimistic_pending_gain": [
                            {
                                "category": "QUALITY_OR_TIMING_REJECT",
                                "optimistic_pending_capture_rate_if_all_bridged": 0.7,
                            }
                        ],
                        "promotion_allowed": False,
                    },
                },
            },
            "mode_disabled_adjusted_final_eligibility": {
                "rate": 0.01,
                "status": "CAPTURE_READINESS_BELOW_60",
            },
        }
    })
    assert stage_verdict["current_capture_stage"] == "mode_disabled_clean_window_pending"
    assert stage_verdict["detector_capture_rate"] == 1.0
    assert stage_verdict["decision_capture_rate"] == 0.5
    assert stage_verdict["pending_capture_rate"] == 0.3
    assert stage_verdict["final_eligibility_capture_rate"] == 0.01
    assert stage_verdict["paper_capture_rate"] == 0.0
    assert stage_verdict["upstream_funnel_gap_summary"]["total_upstream_gap"] == 7
    assert stage_verdict["upstream_funnel_gap_summary"]["upstream_gap_priority"]["current_shortfall_to_60_pending"] == 3
    blocked = build_verdict({**capture, "raw_gold_silver_denominator": {"rows_complete_against_summary": False}}, tests={"passed": True})
    assert blocked["classification"] == "BLOCKED_DATA"
    quote_blocked = build_verdict({
        **capture,
        "quote_context_coverage": {
            **capture["quote_context_coverage"],
            "source_quote_clean_present_rate": 0.7,
            "source_quote_executable_present_rate": 1.0,
            "source_quote_clean_false_rate": 0.3,
            "source_quote_clean_missing_rate": 0.2,
            "source_quote_clean_unknown_rate": 0.1,
            "source_quote_clean_not_applicable_rate": 0.1,
        },
        "quote_missing_root_cause": {
            "quote_missing_rows_total": 3,
            "missing_due_to_legacy_schema_count": 0,
            "missing_due_to_writer_path_count": 3,
            "missing_should_be_not_applicable_count": 0,
            "missing_unknown_count": 0,
            "dominant_root_cause": "v2_writer_path_missing_quote_fields",
        },
        "report_health": {"promotion_blockers": []},
    }, tests={"passed": True})
    assert quote_blocked["classification"] == "BLOCKED_CONTEXT_COVERAGE"
    assert quote_blocked["blocked_subtype"] == "NEEDS_DATA_WRITER_FIX"
    assert quote_blocked["non_quote_sensitive_capture_discovery_allowed"] is True
    assert quote_blocked["quote_sensitive_slices_blocked"] is True
    assert quote_blocked["quote_missing_root_cause"]["missing_due_to_writer_path_count"] == 3
    legacy_quote_blocked = build_verdict({
        **capture,
        "quote_context_coverage": {
            **capture["quote_context_coverage"],
            "source_quote_clean_present_rate": 0.6,
            "source_quote_executable_present_rate": 0.6,
        },
        "quote_missing_root_cause": {
            "quote_missing_rows_total": 4,
            "missing_due_to_legacy_schema_count": 4,
            "missing_due_to_writer_path_count": 0,
            "missing_should_be_not_applicable_count": 0,
            "missing_unknown_count": 0,
            "dominant_root_cause": "legacy_schema",
        },
        "report_health": {"promotion_blockers": []},
    }, tests={"passed": True})
    assert legacy_quote_blocked["blocked_subtype"] == "CLEAN_V2_WINDOW_PENDING"
    assert legacy_quote_blocked["classification"] == "BLOCKED_CONTEXT_COVERAGE"
    monitor_reconciled_quote_blocked = build_verdict({
        **capture,
        "quote_context_coverage": {
            **capture["quote_context_coverage"],
            "source_quote_clean_present_rate": 0.6,
            "source_quote_executable_present_rate": 0.6,
        },
        "quote_missing_root_cause": {
            "quote_missing_rows_total": 4,
            "missing_due_to_legacy_schema_count": 0,
            "missing_due_to_writer_path_count": 4,
            "missing_should_be_not_applicable_count": 0,
            "missing_unknown_count": 0,
            "dominant_root_cause": "v2_writer_path_missing_quote_fields",
        },
        "report_health": {"promotion_blockers": []},
    }, tests={"passed": True}, readiness_reports={
        "context_blocker_monitor": {
            "overall_verdict": {
                "quote_writer_fix": "VERIFIED_POST_DEPLOY",
                "rolling24_quote_status": "QUOTE_CLEAN_WINDOW_PENDING",
            },
            "task_a_post_deploy_quote_smoke_test": {
                "classification": "VERIFIED_POST_DEPLOY",
            },
            "task_b_clean_window_monitor": {
                "classification": "QUOTE_CLEAN_WINDOW_PENDING",
            },
        }
    })
    assert monitor_reconciled_quote_blocked["blocked_subtype"] == "CLEAN_V2_WINDOW_PENDING"
    assert monitor_reconciled_quote_blocked["context_blocker_monitor"]["available"] is True
    lifecycle_reconciled = build_verdict({
        **capture,
        "report_health": {"promotion_blockers": ["lifecycle_profile_coverage_below_80pct"]},
    }, tests={"passed": True}, readiness_reports={
        "context_blocker_monitor": {
            "task_d_context_field_coverage_audit": {
                "warnings": ["lifecycle_profile_rolling_below_80_mature_context_ok"],
            },
        }
    })
    assert "lifecycle_profile_coverage_below_80pct" not in lifecycle_reconciled["blockers"]
    assert "lifecycle_profile_coverage_reconciled_by_mature_context" in lifecycle_reconciled["context_blocker_monitor"]["reconciled_warnings"]
    matured_volume_verdict = build_verdict(capture, tests={"passed": True}, readiness_reports={
        "matured_volume_capture_cross_audit": {
            "overall": {
                "classification": "MATURED_VOLUME_DISCOVERY_NO_SIGNAL",
                "next_action": "keep_volume_sensitive_slices_shadow_only",
                "promotion_allowed": False,
            },
            "h1_matured_building_volume": {"status": "NO_H1_MATURED_VOLUME_HIT"},
            "judgment_counts": {"MATURED_VOLUME_DISCOVERY_WATCH": 1},
            "top_slices": [
                {
                    "candidate_id": "entry_mode_registry:ath_flat_structure_tiny_scout",
                    "family": "entry_mode_registry",
                    "dimension": "matured_volume_profile",
                    "slice_value": "building",
                    "verdict": "MATURED_VOLUME_DISCOVERY_WATCH",
                    "slice_signal_count": 126,
                    "slice_raw_gs_count": 10,
                    "candidate_match_count": 62,
                    "matched_gs_count": 5,
                    "match_recall_event": 0.5,
                    "match_precision_event": 0.080645,
                    "recall_lift_vs_candidate_baseline": 0.131579,
                    "precision_lift_vs_candidate_baseline": 0.017009,
                    "promotion_allowed": False,
                }
            ],
        }
    })
    matured_volume = matured_volume_verdict["matured_volume_capture_cross_audit"]
    assert matured_volume["top_watch_slices"][0]["candidate_id"] == "entry_mode_registry:ath_flat_structure_tiny_scout"
    assert matured_volume["next_research_action"] == "review_non_h1_matured_volume_watch_slices"
    reconciled = {
        **capture,
        "raw_dog_observation_join": {"join_rate": 0.5},
        "signal_identity_reconciliation": {
            "joined_exact_signal_id": 1,
            "missing_candidate_observation": 1,
            "unknown_unjoined": 0,
            "raw_all_signal_id_join_rate": 0.5,
            "mesh_eligible_signal_id_join_rate": 0.5,
            "raw_all_unjoined_fully_attributed": True,
        },
    }
    verdict_reconciled = build_verdict(reconciled, tests={"passed": True})
    assert "signal_id_join_rate_below_99pct" not in verdict_reconciled["blockers"]
    assert verdict_reconciled["signal_id_join_rate"] == 0.5
    assert verdict_reconciled["promotion_allowed"] is False
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "verdict.json"
        write_json(path, verdict)
        loaded = load_json(path)
        assert loaded is not None
        assert loaded["schema_version"] == SCHEMA_VERSION
    print("SELF_TEST_PASS review_agent_verdict")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", required=False)
    parser.add_argument("--pnl", default=None)
    parser.add_argument("--markov", action="append", default=[], help="profile:path")
    parser.add_argument(
        "--readiness",
        action="append",
        default=[],
        help="name:path readiness/report artifact, e.g. a_class_fastlane_mode_audit:/app/data/agent_runs/latest/a_class_fastlane_mode_audit_24h.json",
    )
    parser.add_argument("--tests", default=None)
    parser.add_argument("--out", default="data/agent_runs/latest/reviewer_verdict.json")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not args.capture:
        raise SystemExit("--capture is required unless --self-test is used")
    markov_reports = {}
    for item in args.markov:
        if ":" not in item:
            raise SystemExit(f"invalid --markov value {item!r}; expected profile:path")
        name, path = item.split(":", 1)
        markov_reports[name] = load_json(path)
    readiness_reports = {}
    for item in args.readiness:
        if ":" not in item:
            raise SystemExit(f"invalid --readiness value {item!r}; expected name:path")
        name, path = item.split(":", 1)
        readiness_reports[name] = load_json(path)
    tests = load_json(args.tests) if args.tests else {}
    verdict = build_verdict(
        load_json(args.capture),
        load_json(args.pnl) if args.pnl else None,
        markov_reports,
        tests=tests,
        readiness_reports=readiness_reports,
    )
    write_json(args.out, verdict)
    print(json.dumps({"out": args.out, "classification": verdict["classification"], "blockers": verdict["blockers"]}, sort_keys=True))


if __name__ == "__main__":
    main()
