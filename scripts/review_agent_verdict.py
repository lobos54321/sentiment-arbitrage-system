#!/usr/bin/env python3
"""Build a reviewer verdict for the gold/silver capture discovery loop.

Read-only. This script consumes materialized discovery reports and produces a
single audit verdict. It never changes strategy, gates, executor, or risk.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
SCHEMA_VERSION = "capture_discovery_reviewer_verdict.v5"
EXPECTED_CANDIDATE_COUNT = 84
EXPECTED_CONTEXT_SCHEMA_VERSION = "candidate-shadow-context-v2.no_signal_price_quote_inference"
EXPECTED_QUOTE_CLEAN_DEFINITION = "source_or_executable_quote_only_no_signal_price"
QUOTE_COVERAGE_BLOCKERS = {
    "source_quote_clean_coverage_below_80pct",
    "source_quote_executable_coverage_below_80pct",
}
BLOCKER_PRIORITY = [
    "candidate_count_expected_not_84",
    "candidate_count_observed_not_84",
    "observation_coverage_below_99pct",
    "raw_dog_rows_incomplete",
    "signal_id_join_rate_below_99pct",
    "raw_all_unjoined_not_fully_attributed",
    "tests_failed",
    "report_generation_failed",
    "runtime_signal_source_stale_fail_closed",
    "runtime_signal_source_freshness_missing",
    "runtime_paper_db_unavailable",
    "runtime_paper_db_integrity_marker_exists",
    "runtime_final_evidence_missing",
    "volume_profile_coverage_below_80pct",
    "kline_coverage_below_80pct",
    "source_quote_clean_coverage_below_80pct",
    "source_quote_executable_coverage_below_80pct",
    "source_component_coverage_below_80pct",
    "schema_mixed_quote_sensitive_slices_blocked",
    "context_schema_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
    "quote_clean_definition_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
    "markov_bucket_coverage_below_80pct",
]

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


DERIVED_READINESS_SIBLINGS = {
    "candidate_downstream_readiness": "candidate_downstream_readiness_24h.json",
    "context_coverage": "context_coverage_audit_24h.json",
    "candidate_effectiveness": "candidate_effectiveness_24h.json",
    "candidate_improvement_opportunities": "candidate_improvement_opportunities_24h.json",
    "markov_effectiveness": "markov_effectiveness_24h.json",
    "capture_cross_validity": "capture_cross_validity_24h.json",
    "volume_kline_coverage_audit": "volume_kline_coverage_audit_24h.json",
    "matured_kline_volume_recheck_audit": "matured_kline_volume_recheck_audit_24h.json",
    "matured_volume_capture_cross_audit": "matured_volume_capture_cross_audit_24h.json",
    "hypothesis_validation_audit": "hypothesis_validation_audit_24h.json",
    "low_confidence_research_capture_audit": "low_confidence_research_capture_audit_24h.json",
    "quality_timing_reject_research_audit": "quality_timing_reject_research_audit_24h.json",
    "quality_timing_candidate_probe_validation": "quality_timing_candidate_probe_validation_24h.json",
    "decision_no_pass_quality_timing_watch_validation": "decision_no_pass_quality_timing_watch_validation_24h.json",
    "pending_momentum_decay_recheck_validation": "pending_momentum_decay_recheck_validation_24h.json",
    "strategy_memory_ingestion_summary": "strategy_memory_ingestion_summary.json",
    "strategy_memory_validation": "strategy_memory_validation_24h.json",
    "strategy_memory_filtered_winner_bridge": "strategy_memory_filtered_winner_bridge.json",
    "strategy_memory_exit_shadow_summary": "strategy_memory_exit_shadow_summary.json",
    "strategy_memory_delay_replay_summary": "strategy_memory_delay_replay_summary.json",
    "shadow_decision_bridge_audit": "shadow_decision_bridge_audit_24h.json",
    "a_class_fastlane_mode_audit": "a_class_fastlane_mode_audit_24h.json",
    "runtime_health_snapshot": "runtime_health_snapshot_24h.json",
    "context_blocker_monitor": "context_blocker_monitor_24h.json",
    "hypothesis_validation_oos_probe_0p1h": "hypothesis_validation_audit_oos_probe_0p1h.json",
    "hypothesis_validation_oos_probe_0p25h": "hypothesis_validation_audit_oos_probe_0p25h.json",
    "hypothesis_validation_oos_probe_0p5h": "hypothesis_validation_audit_oos_probe_0p5h.json",
    "hypothesis_validation_oos_probe_1h": "hypothesis_validation_audit_oos_probe_1h.json",
    "matured_volume_cross_oos_probe_0p1h": "matured_volume_capture_cross_audit_oos_probe_0p1h.json",
    "matured_volume_cross_oos_probe_0p25h": "matured_volume_capture_cross_audit_oos_probe_0p25h.json",
    "matured_volume_cross_oos_probe_0p5h": "matured_volume_capture_cross_audit_oos_probe_0p5h.json",
    "matured_volume_cross_oos_probe_1h": "matured_volume_capture_cross_audit_oos_probe_1h.json",
    "oos_readiness_probe_refresh": "oos_readiness_probe_refresh.json",
    "pass_allow_60_post_freeze_oos_validation": "pass_allow_60_post_freeze_oos_validation.json",
    "capture_60_gap_report": "capture_60_gap_report.json",
    "capture_stage_metrics": "capture_stage_metrics.json",
    "context_dimension_eligibility": "context_dimension_eligibility.json",
    "kline_coverage_resolution_audit": "kline_coverage_resolution_audit_24h.json",
    "pass_allow_capture_gap_audit": "pass_allow_capture_gap_audit.json",
    "decision_no_pass_quality_timing_review": "decision_no_pass_quality_timing_review.json",
    "pass_allow_60_closure_plan": "pass_allow_60_closure_plan.json",
    "pass_allow_60_oos_freeze_registry": "pass_allow_60_oos_freeze_registry.json",
    "pass_allow_60_oos_readiness_monitor": "pass_allow_60_oos_readiness_monitor.json",
    "pending_to_final_entry_audit": "pending_to_final_entry_audit.json",
    "final_entry_readiness_audit": "final_entry_readiness_audit.json",
    "strategy_memory_capture_validation": "strategy_memory_capture_validation.json",
    "shadow_candidate_improvement_queue": "shadow_candidate_improvement_queue.json",
    "oos_readiness_summary_v3": "oos_readiness_summary.json",
}


def load_sibling_readiness_reports(capture_path, existing=None):
    reports = dict(existing or {})
    if not capture_path:
        return reports
    base = Path(capture_path).expanduser().resolve().parent
    for name, filename in DERIVED_READINESS_SIBLINGS.items():
        if name in reports:
            continue
        path = base / filename
        if not path.exists():
            continue
        try:
            reports[name] = load_json(path)
        except Exception:
            pass
    context_report = reports.get("context_coverage") or {}
    reports.setdefault("volume_profile_coverage", context_report.get("volume_profile_coverage") or {})
    reports.setdefault("kline_coverage", context_report.get("kline_coverage") or {})
    if "oos_readiness_summary_v3" in reports and "oos_readiness_summary" not in reports:
        reports["oos_readiness_summary"] = reports["oos_readiness_summary_v3"]
    if "oos_readiness_summary" in reports and "oos_readiness_summary_v3" not in reports:
        reports["oos_readiness_summary_v3"] = reports["oos_readiness_summary"]
    return reports


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


def as_int(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def boolish(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def compact_context_field_progress(field_audit, field_name, blocker_name):
    row = (field_audit or {}).get(field_name) or {}
    mature = row.get("mature_context") or {}
    return {
        "field": field_name,
        "blocker": blocker_name,
        "denominator_rows": row.get("denominator_rows"),
        "effective_present_rate": row.get("effective_present_rate"),
        "present_rate": row.get("present_rate"),
        "missing_rows": row.get("missing_rows"),
        "unknown_rows": row.get("unknown_rows"),
        "rows_needed_to_80pct": row.get("rows_needed_to_80pct"),
        "target_effective_present_rate": row.get("target_effective_present_rate"),
        "mature_context": {
            "denominator_rows": mature.get("denominator_rows"),
            "effective_present_rate": mature.get("effective_present_rate"),
            "present_rate": mature.get("present_rate"),
            "missing_rows": mature.get("missing_rows"),
            "unknown_rows": mature.get("unknown_rows"),
            "rows_needed_to_80pct": mature.get("rows_needed_to_80pct"),
        },
    }


def build_context_clean_window_progress(
    *,
    blockers,
    actionable_blockers,
    context_clean_window_pending,
    quote_clean_window_pending,
    context_field_writer_fix_status,
    quote_writer_fix_status,
    context_monitor_clean_window,
    context_monitor_field_audit,
    context_monitor_field_smoke,
    context_monitor_recent_writer_health=None,
):
    field_blockers = {
        "lifecycle_profile": "lifecycle_profile_coverage_below_80pct",
        "source_component": "source_component_coverage_below_80pct",
        "volume_profile": "volume_profile_coverage_below_80pct",
        "markov_bucket": "markov_bucket_coverage_below_80pct",
    }
    fields = {
        field: compact_context_field_progress(context_monitor_field_audit, field, blocker)
        for field, blocker in field_blockers.items()
    }
    blocker_set = set(blockers or [])
    waiting_fields = [
        field for field, blocker in field_blockers.items()
        if blocker in blocker_set
    ]
    actionable_set = set(actionable_blockers or [])
    actionable_fields = [
        field for field, blocker in field_blockers.items()
        if blocker in actionable_set
    ]
    if context_clean_window_pending:
        classification = "CONTEXT_CLEAN_WINDOW_PENDING"
    elif waiting_fields:
        classification = "CONTEXT_FIELDS_BLOCKED"
    else:
        classification = "CONTEXT_FIELDS_READY"
    return {
        "classification": classification,
        "context_clean_window_pending": bool(context_clean_window_pending),
        "quote_clean_window_pending": bool(quote_clean_window_pending),
        "waiting_fields": waiting_fields,
        "actionable_fields": actionable_fields,
        "writer_status": {
            "context_field_writer_fix_status": context_field_writer_fix_status,
            "quote_writer_fix_status": quote_writer_fix_status,
            "post_deploy_context_fields_healthy": boolish(
                (context_monitor_field_smoke or {}).get("post_deploy_context_fields_healthy")
            ),
            "recent_context_writer_health_classification": (
                (context_monitor_recent_writer_health or {}).get("classification")
            ),
            "recent_context_writer_inferred_clean_window_state": (
                (context_monitor_recent_writer_health or {}).get("inferred_clean_window_state")
            ),
            "recent_context_writer_newest_age_sec": (
                (context_monitor_recent_writer_health or {}).get("newest_age_sec")
            ),
        },
        "recent_context_writer_health": context_monitor_recent_writer_health or {},
        "quote_clean_window": {
            "classification": (context_monitor_clean_window or {}).get("classification"),
            "pre_fix_rows_remaining": (context_monitor_clean_window or {}).get("pre_fix_rows_remaining"),
            "post_fix_rows": (context_monitor_clean_window or {}).get("post_fix_rows"),
            "rolling24_rows": (context_monitor_clean_window or {}).get("rolling24_rows"),
            "estimated_clean_at_iso": (context_monitor_clean_window or {}).get("estimated_clean_at_iso"),
            "seconds_until_natural_clean_window": (
                (context_monitor_clean_window or {}).get("seconds_until_natural_clean_window")
            ),
        },
        "fields": fields,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def runtime_commit():
    for key in (
        "ZEABUR_GIT_COMMIT_SHA",
        "ZEABUR_GIT_COMMIT",
        "ZEABUR_COMMIT_SHA",
        "GIT_COMMIT",
        "COMMIT_SHA",
        "SOURCE_VERSION",
        "RAILWAY_GIT_COMMIT_SHA",
        "VERCEL_GIT_COMMIT_SHA",
        "RENDER_GIT_COMMIT",
        "GITHUB_SHA",
    ):
        value = os.environ.get(key)
        if value:
            return value
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=False,
            text=True,
            capture_output=True,
            timeout=5,
        )
        if proc.returncode == 0:
            return proc.stdout.strip() or None
    except Exception:
        return None
    return None


def deployment_commit():
    for key in (
        "ZEABUR_GIT_COMMIT_SHA",
        "ZEABUR_GIT_COMMIT",
        "ZEABUR_COMMIT_SHA",
        "COMMIT_SHA",
        "GIT_COMMIT",
        "SOURCE_VERSION",
        "GITHUB_SHA",
    ):
        value = os.environ.get(key)
        if value:
            return value
    return runtime_commit()


def first_blocker_priority(blockers):
    blocker_set = set(blockers or [])
    for blocker in BLOCKER_PRIORITY:
        if blocker in blocker_set:
            return blocker
    return sorted(blocker_set)[0] if blocker_set else None


def compact_oos_probe(name, report, cross_report=None):
    validation = (report or {}).get("matured_volume_hypothesis_validation") or {}
    quality = validation.get("eval_window_quality") or {}
    overall = (report or {}).get("overall") or {}
    cross_overall = (cross_report or {}).get("overall") or {}
    cross_context = (cross_report or {}).get("matured_volume_context") or {}
    return {
        "probe": name,
        "available": bool(report),
        "classification": overall.get("classification"),
        "next_action": overall.get("next_action"),
        "promotion_allowed": False,
        "human_action_required": bool(overall.get("human_action_required")),
        "sufficient_for_oos_judgment": bool(quality.get("sufficient_for_oos_judgment")),
        "blockers": quality.get("blockers") or [],
        "signals_scanned": quality.get("signals_scanned"),
        "evaluable_raw_gs_event_rows": quality.get("evaluable_raw_gs_event_rows"),
        "matured_volume_known_rate": quality.get("matured_volume_known_rate"),
        "min_oos_signals": quality.get("min_oos_signals"),
        "min_oos_raw_gs_events": quality.get("min_oos_raw_gs_events"),
        "min_oos_matured_volume_known_rate": quality.get("min_oos_matured_volume_known_rate"),
        "cross_classification": quality.get("cross_classification") or cross_overall.get("classification"),
        "cross_known_rate": cross_context.get("known_rate"),
        "oos_repeated_watch_count": validation.get("oos_repeated_watch_count"),
        "repeated_watch_count": validation.get("repeated_watch_count"),
        "registry_frozen_before_eval_window": validation.get("registry_frozen_before_eval_window"),
    }


def compact_oos_refresh_probe(row):
    cross = row.get("cross") or {}
    validation = row.get("validation") or {}
    return {
        "probe": row.get("probe"),
        "available": bool(cross or validation),
        "classification": validation.get("classification"),
        "next_action": validation.get("next_action"),
        "promotion_allowed": False,
        "human_action_required": False,
        "sufficient_for_oos_judgment": bool(validation.get("sufficient_for_oos_judgment")),
        "blockers": validation.get("blockers") or [],
        "signals_scanned": validation.get("signals_scanned") or cross.get("signals_scanned"),
        "evaluable_raw_gs_event_rows": (
            validation.get("evaluable_raw_gs_event_rows")
            if validation.get("evaluable_raw_gs_event_rows") is not None
            else cross.get("evaluable_raw_gs_event_rows")
        ),
        "matured_volume_known_rate": (
            validation.get("matured_volume_known_rate")
            if validation.get("matured_volume_known_rate") is not None
            else cross.get("matured_volume_known_rate")
        ),
        "min_oos_signals": validation.get("min_oos_signals"),
        "min_oos_raw_gs_events": validation.get("min_oos_raw_gs_events"),
        "min_oos_matured_volume_known_rate": validation.get("min_oos_matured_volume_known_rate"),
        "cross_classification": cross.get("classification"),
        "cross_known_rate": cross.get("matured_volume_known_rate"),
        "oos_repeated_watch_count": validation.get("oos_repeated_watch_count"),
        "repeated_watch_count": validation.get("repeated_watch_count"),
        "registry_frozen_before_eval_window": validation.get("registry_frozen_before_eval_window"),
        "source": "oos_readiness_probe_refresh",
    }


def probe_hours_value(label):
    text = str(label or "").strip()
    if not text:
        return None
    if text.endswith("h"):
        text = text[:-1]
    text = text.replace("p", ".")
    return as_float(text)


def rounded_probe_hours(value):
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number <= 0:
        return None
    if number < 1:
        return round(number, 3)
    if number < 24:
        return round(number + 0.05, 1)
    return round(number + 0.5, 0)


def build_oos_readiness_delta(probes, refresh_report=None):
    """Summarize why the current OOS probes are not yet judgeable.

    The loop already records detailed blockers per probe. This compact delta is
    deliberately read-only: it estimates the missing sample/context quantities
    and a next probe window without changing strategy or promotion state.
    """
    available = [row for row in probes if row.get("available")]
    if not available:
        return {
            "available": False,
            "reason": "no_oos_probes_available",
            "promotion_allowed": False,
        }
    post_freeze_probe = (refresh_report or {}).get("post_freeze_probe") or {}
    post_freeze_hours = as_float(post_freeze_probe.get("hours"))
    registry_updated_ts = as_int(post_freeze_probe.get("registry_updated_ts"))
    safety_sec = as_int(post_freeze_probe.get("safety_sec"), 0) or 0

    def score(row):
        signals = as_int(row.get("signals_scanned"), 0) or 0
        raw_gs = as_int(row.get("evaluable_raw_gs_event_rows"), 0) or 0
        known = as_float(row.get("matured_volume_known_rate"))
        return (
            bool(row.get("sufficient_for_oos_judgment")),
            raw_gs,
            signals,
            known if known is not None else -1,
            probe_hours_value(row.get("probe")) or 0,
        )

    best = max(available, key=score)
    min_signals = as_int(best.get("min_oos_signals"), 50) or 50
    min_raw_gs = as_int(best.get("min_oos_raw_gs_events"), 10) or 10
    min_known_rate = as_float(best.get("min_oos_matured_volume_known_rate"))
    if min_known_rate is None:
        min_known_rate = 0.8

    signals = as_int(best.get("signals_scanned"), 0) or 0
    raw_gs = as_int(best.get("evaluable_raw_gs_event_rows"), 0) or 0
    known_rate = as_float(best.get("matured_volume_known_rate"))
    hours = probe_hours_value(best.get("probe"))

    signal_deficit = max(0, min_signals - signals)
    raw_gs_deficit = max(0, min_raw_gs - raw_gs)
    known_rate_deficit = None
    if known_rate is not None:
        known_rate_deficit = max(0.0, round(min_known_rate - known_rate, 6))

    recommended_hours = None
    if hours:
        candidates = [hours]
        if signals > 0 and signal_deficit:
            candidates.append(hours * float(min_signals) / float(signals))
        if raw_gs > 0 and raw_gs_deficit:
            candidates.append(hours * float(min_raw_gs) / float(raw_gs))
        elif raw_gs_deficit:
            candidates.append(hours * 2.0)
        recommended_hours = rounded_probe_hours(max(candidates))

    non_overlapping = [
        row for row in available
        if row.get("registry_frozen_before_eval_window") is True
    ]
    best_non_overlapping = max(non_overlapping, key=score) if non_overlapping else None
    non_overlap_hours_remaining = None
    next_true_oos_eligible_ts = None
    next_true_oos_eligible_at_utc = None
    if recommended_hours is not None and post_freeze_hours is not None:
        non_overlap_hours_remaining = max(0.0, round(float(recommended_hours) - float(post_freeze_hours), 4))
    if recommended_hours is not None and registry_updated_ts is not None:
        next_true_oos_eligible_ts = int(registry_updated_ts + safety_sec + float(recommended_hours) * 3600)
        next_true_oos_eligible_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(next_true_oos_eligible_ts))
    next_action = (
        "review_repeated_oos_watch_without_promotion"
        if best.get("sufficient_for_oos_judgment") and best.get("registry_frozen_before_eval_window") is True
        else "wait_for_non_overlapping_window_before_oos_judgment"
        if not non_overlapping
        else "rerun_oos_with_recommended_probe_hours_when_new_data_arrives"
    )

    return {
        "available": True,
        "best_probe": best.get("probe"),
        "best_probe_role": "sample_volume_reference_not_true_oos"
        if best.get("registry_frozen_before_eval_window") is not True
        else "true_oos_probe",
        "best_true_oos_probe": best_non_overlapping.get("probe") if best_non_overlapping else None,
        "best_probe_sufficient": bool(best.get("sufficient_for_oos_judgment")),
        "best_probe_blockers": best.get("blockers") or [],
        "signals_scanned": signals,
        "min_oos_signals": min_signals,
        "signals_needed": signal_deficit,
        "evaluable_raw_gs_event_rows": raw_gs,
        "min_oos_raw_gs_events": min_raw_gs,
        "raw_gs_events_needed": raw_gs_deficit,
        "matured_volume_known_rate": known_rate,
        "min_oos_matured_volume_known_rate": min_known_rate,
        "matured_volume_known_rate_needed": known_rate_deficit,
        "registry_frozen_before_eval_window": best.get("registry_frozen_before_eval_window"),
        "non_overlapping_probe_count": len(non_overlapping),
        "post_freeze_window_hours_available": post_freeze_hours,
        "sample_volume_recommended_probe_hours": recommended_hours,
        "non_overlapping_window_hours_needed": recommended_hours,
        "non_overlapping_window_hours_remaining": non_overlap_hours_remaining,
        "next_true_oos_eligible_ts": next_true_oos_eligible_ts,
        "next_true_oos_eligible_at_utc": next_true_oos_eligible_at_utc,
        "non_overlap_warning": (
            "best_probe_is_not_true_oos_do_not_judge_promotion"
            if best.get("registry_frozen_before_eval_window") is not True
            else None
        ),
        "next_recommended_probe_hours": recommended_hours,
        "next_recommended_action": next_action,
        "promotion_allowed": False,
    }


def build_oos_readiness_summary(readiness_reports):
    refresh_report = readiness_reports.get("oos_readiness_probe_refresh") or {}
    refresh_has_execution_accounting = bool(refresh_report) and (
        "executed_probe_count" in refresh_report
        or "requested_probe_count" in refresh_report
        or "probe_count" in refresh_report
    )
    refresh_executed_probe_count = as_int(
        refresh_report.get("executed_probe_count", refresh_report.get("probe_count")),
        None,
    )
    refresh_probe_labels = {
        str(row.get("probe"))
        for row in (refresh_report.get("probes") or [])
        if row.get("probe")
    }
    probe_specs = (
        (
            "0p1h",
            "hypothesis_validation_oos_probe_0p1h",
            "matured_volume_cross_oos_probe_0p1h",
        ),
        (
            "0p25h",
            "hypothesis_validation_oos_probe_0p25h",
            "matured_volume_cross_oos_probe_0p25h",
        ),
        (
            "0p5h",
            "hypothesis_validation_oos_probe_0p5h",
            "matured_volume_cross_oos_probe_0p5h",
        ),
        (
            "1h",
            "hypothesis_validation_oos_probe_1h",
            "matured_volume_cross_oos_probe_1h",
        ),
    )
    fixed_labels = {label for label, _hypothesis_key, _cross_key in probe_specs}
    all_fixed_probes = [
        compact_oos_probe(
            label,
            readiness_reports.get(hypothesis_key) or {},
            readiness_reports.get(cross_key) or {},
        )
        for label, hypothesis_key, cross_key in probe_specs
    ]
    if refresh_has_execution_accounting:
        if (refresh_executed_probe_count or 0) <= 0:
            current_fixed_labels = set()
        else:
            current_fixed_labels = refresh_probe_labels & fixed_labels
    else:
        current_fixed_labels = fixed_labels
    probes = [
        row for row in all_fixed_probes
        if row.get("probe") in current_fixed_labels
    ]
    stale_fixed_probes = [
        row for row in all_fixed_probes
        if row.get("available") and row.get("probe") not in current_fixed_labels
    ]
    for row in refresh_report.get("probes") or []:
        label = row.get("probe")
        if not label or label in fixed_labels:
            continue
        probes.append(compact_oos_refresh_probe(row))
    available = [row for row in probes if row["available"]]
    sufficient = [row for row in available if row["sufficient_for_oos_judgment"]]
    repeated = [row for row in sufficient if (as_int(row.get("oos_repeated_watch_count"), 0) or 0) > 0]
    if not available:
        classification = "OOS_PROBES_MISSING"
        next_action = "run_non_overlapping_oos_probe_when_discovery_hit_exists"
    elif not sufficient:
        classification = "OOS_WINDOW_TOO_SMALL_OR_CONTEXT_BLOCKED"
        next_action = "continue_collecting_post_freeze_window_before_judging_oos"
    elif repeated:
        classification = "OOS_REPEATED_WATCH_PENDING_REVIEW"
        next_action = "review_repeated_oos_watch_without_promotion"
    else:
        classification = "OOS_NO_REPEAT_CONTINUE_WATCH"
        next_action = "continue_watchlist_validation"
    readiness_delta = build_oos_readiness_delta(probes, refresh_report)
    return {
        "available_probe_count": len(available),
        "sufficient_probe_count": len(sufficient),
        "oos_repeated_watch_probe_count": len(repeated),
        "current_oos_probe_count": len(available),
        "stale_oos_probe_files_ignored_count": len(stale_fixed_probes),
        "stale_oos_probe_files_ignored": [
            row.get("probe") for row in stale_fixed_probes
        ],
        "oos_probe_refresh_classification": refresh_report.get("classification"),
        "oos_probe_refresh_executed_probe_count": refresh_executed_probe_count,
        "oos_probe_refresh_requested_probe_count": refresh_report.get("requested_probe_count"),
        "oos_probe_refresh_probe_count": refresh_report.get("probe_count"),
        "oos_probe_refresh_post_freeze_reason": (
            (refresh_report.get("post_freeze_probe") or {}).get("reason")
        ),
        "classification": classification,
        "next_action": next_action,
        "readiness_delta": readiness_delta,
        "promotion_allowed": False,
        "human_action_required": False,
        "probes": probes,
    }


def compact_oos_probe_refresh(report):
    if not report:
        return {"available": False}
    probes = []
    for row in report.get("probes") or []:
        cross = row.get("cross") or {}
        validation = row.get("validation") or {}
        probes.append({
            "probe": row.get("probe"),
            "cross_classification": cross.get("classification"),
            "validation_classification": validation.get("classification"),
            "signals_scanned": validation.get("signals_scanned") or cross.get("signals_scanned"),
            "evaluable_raw_gs_event_rows": (
                validation.get("evaluable_raw_gs_event_rows")
                if validation.get("evaluable_raw_gs_event_rows") is not None
                else cross.get("evaluable_raw_gs_event_rows")
            ),
            "matured_volume_known_rate": (
                validation.get("matured_volume_known_rate")
                if validation.get("matured_volume_known_rate") is not None
                else cross.get("matured_volume_known_rate")
            ),
            "sufficient_for_oos_judgment": validation.get("sufficient_for_oos_judgment"),
            "blockers": validation.get("blockers") or [],
        })
    return {
        "available": True,
        "classification": report.get("classification"),
        "generated_at": report.get("generated_at"),
        "failed_command_count": report.get("failed_command_count"),
        "requested_probe_count": report.get("requested_probe_count"),
        "executed_probe_count": report.get("executed_probe_count", report.get("probe_count")),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "probe_count": len(probes),
        "probes": probes,
        "next_action": report.get("next_action"),
        "post_freeze_reason": ((report.get("post_freeze_probe") or {}).get("reason")),
    }


def volume_profile_blocker_state(blockers, matured_kline_recheck, matured_volume_cross):
    """Classify whether the volume blocker still needs a code fix or has a shadow-only matured path."""
    blockers = set(blockers or [])
    realtime_blocked = "volume_profile_coverage_below_80pct" in blockers
    recheck = (matured_kline_recheck or {}).get("recheck") or {}
    overall = (matured_volume_cross or {}).get("overall") or {}
    matured_context = (matured_volume_cross or {}).get("matured_volume_context") or {}
    signal_reconciliation = (matured_volume_cross or {}).get("signal_id_reconciliation") or {}
    recoverable_known_rate = as_float(recheck.get("recoverable_known_rate"))
    matured_known_rate = as_float(matured_context.get("known_rate"))
    kline_cache_available = bool(matured_context.get("kline_cache_available"))
    overall_classification = overall.get("classification")
    allowed_unjoined_reasons = {
        "outside_candidate_observer_window_before",
        "outside_candidate_observer_window_after",
    }
    reconciliation_scopes = {}
    scope_reconciled = bool(signal_reconciliation)
    scope_joined_99 = bool(signal_reconciliation)
    for name in ("raw_all_gold_silver", "evaluable_gold_silver"):
        recon = signal_reconciliation.get(name) or {}
        reason_counts = recon.get("unjoined_reason_counts") or {}
        reasons = set(reason_counts)
        joined_event_rate = as_float(recon.get("joined_event_rate"))
        unjoined_event_rows = as_int(recon.get("unjoined_event_rows"))
        if not recon:
            ok = False
        elif not reasons:
            ok = True
        else:
            ok = reasons <= allowed_unjoined_reasons
        joined_ok = bool(recon) and (
            (joined_event_rate is not None and joined_event_rate >= 0.99)
            or unjoined_event_rows == 0
        )
        reconciliation_scopes[name] = {
            "ok": ok,
            "joined_ok": joined_ok,
            "joined_event_rate": joined_event_rate,
            "unjoined_event_rows": unjoined_event_rows,
            "unjoined_reason_counts": reason_counts,
        }
        scope_reconciled = scope_reconciled and ok
        scope_joined_99 = scope_joined_99 and joined_ok
    shadow_available = bool(
        realtime_blocked
        and recoverable_known_rate is not None
        and recoverable_known_rate >= 0.8
        and matured_known_rate is not None
        and matured_known_rate >= 0.8
        and scope_reconciled
    )
    coverage_pending = bool(
        realtime_blocked
        and overall_classification == "BLOCKED_MATURED_VOLUME_COVERAGE"
        and kline_cache_available
        and matured_known_rate is not None
        and matured_known_rate < 0.8
        and scope_joined_99
    )
    if shadow_available:
        classification = "SHADOW_MATURED_VOLUME_PATH_AVAILABLE"
        next_action = "continue_shadow_matured_volume_validation_without_formal_volume_promotion"
    elif coverage_pending:
        classification = "MATURED_VOLUME_COVERAGE_PENDING"
        next_action = "continue_matured_volume_recheck_before_evaluating_volume_slices"
    elif realtime_blocked:
        classification = "REALTIME_VOLUME_CONTEXT_BLOCKED"
        next_action = "fix_volume_context_writer_or_kline_attribution"
    else:
        classification = "VOLUME_CONTEXT_NOT_BLOCKING"
        next_action = "continue_capture_discovery"
    return {
        "classification": classification,
        "realtime_volume_profile_blocked": realtime_blocked,
        "shadow_matured_volume_slices_evaluable": shadow_available,
        "matured_volume_coverage_pending": coverage_pending,
        "volume_data_coverage_pending": coverage_pending,
        "formal_volume_slices_evaluable": not realtime_blocked,
        "formal_denominator_changed": False,
        "promotion_allowed": False,
        "recoverable_known_rate": recoverable_known_rate,
        "matured_volume_known_rate": matured_known_rate,
        "kline_cache_available": kline_cache_available,
        "matured_volume_cross_classification": overall_classification,
        "scope_reconciled": scope_reconciled,
        "scope_joined_99": scope_joined_99,
        "reconciliation_scopes": reconciliation_scopes,
        "next_action": next_action,
    }


def context_dimension_formal_blockers(context_dimension_eligibility):
    dimensions = (context_dimension_eligibility or {}).get("dimensions") or {}
    fallback_blockers = {
        "quote-sensitive": "source_quote_clean_coverage_below_80pct",
        "volume": "volume_profile_coverage_below_80pct",
        "kline": "kline_coverage_below_80pct",
        "Markov": "markov_bucket_coverage_below_80pct",
    }
    blockers = []
    for name, row in dimensions.items():
        status = row.get("status")
        if status in {"CLEAN", "NOT_APPLICABLE"}:
            continue
        row_blockers = [blocker for blocker in (row.get("blockers") or []) if blocker]
        blockers.extend(row_blockers)
        if not row_blockers and name in fallback_blockers:
            blockers.append(fallback_blockers[name])
    return sorted(set(blockers))


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
    current_commit = readiness_reports.get("current_commit") or runtime_commit()
    deployed_commit = readiness_reports.get("deployment_commit") or deployment_commit() or current_commit
    if not current_commit:
        current_commit = deployed_commit
    if not deployed_commit:
        deployed_commit = current_commit
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
    context_monitor_field_smoke = context_blocker_monitor.get("task_e_post_deploy_context_field_smoke_test") or {}
    context_monitor_recent_writer_health = context_blocker_monitor.get("task_f_recent_context_writer_health") or {}
    runtime_health_snapshot = readiness_reports.get("runtime_health_snapshot") or {}
    runtime_health_blockers = list(runtime_health_snapshot.get("blockers") or [])
    runtime_health_warnings = list(runtime_health_snapshot.get("warnings") or [])
    context_dimension_eligibility = readiness_reports.get("context_dimension_eligibility") or {}
    context_dimension_blockers = context_dimension_formal_blockers(context_dimension_eligibility)
    quote_writer_fix_status = (
        context_monitor_overall.get("quote_writer_fix")
        or context_monitor_quote_smoke.get("classification")
    )
    quote_clean_window_status = (
        context_monitor_overall.get("rolling24_quote_status")
        or context_monitor_clean_window.get("classification")
    )
    context_field_writer_fix_status = (
        context_monitor_overall.get("context_field_writer_fix")
        or context_monitor_field_smoke.get("classification")
    )
    blockers = list(report_health.get("promotion_blockers") or [])
    blockers.extend(runtime_health_blockers)
    blockers.extend(context_dimension_blockers)

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
    if (
        "source_component_coverage_below_80pct" in blockers
        and "source_component_rolling_below_80_mature_context_ok" in context_monitor_warnings
    ):
        blockers = [
            blocker for blocker in blockers
            if blocker != "source_component_coverage_below_80pct"
        ]
        reconciled_context_warnings.append("source_component_coverage_reconciled_by_mature_context")

    blockers = sorted(set(blockers))
    final_entry = readiness_reports.get("a_class_fastlane_mode_audit") or {}
    volume_kline_audit = readiness_reports.get("volume_kline_coverage_audit") or {}
    matured_kline_recheck = readiness_reports.get("matured_kline_volume_recheck_audit") or {}
    matured_volume_cross = readiness_reports.get("matured_volume_capture_cross_audit") or {}
    volume_profile_state = volume_profile_blocker_state(
        blockers,
        matured_kline_recheck,
        matured_volume_cross,
    )
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
    quote_clean_window_pending = (
        blocked_subtype == "CLEAN_V2_WINDOW_PENDING"
        and (
            context_monitor_overall.get("quote_writer_fix") == "VERIFIED_POST_DEPLOY"
            or context_monitor_quote_smoke.get("classification") == "VERIFIED_POST_DEPLOY"
        )
        and (
            context_monitor_overall.get("rolling24_quote_status") == "QUOTE_CLEAN_WINDOW_PENDING"
            or context_monitor_clean_window.get("classification") == "QUOTE_CLEAN_WINDOW_PENDING"
        )
    )
    actionable_blockers = [
        blocker for blocker in blockers
        if not (quote_clean_window_pending and blocker in QUOTE_COVERAGE_BLOCKERS)
    ]
    volume_blocker_non_actionable = bool(
        volume_profile_state.get("shadow_matured_volume_slices_evaluable")
        or volume_profile_state.get("matured_volume_coverage_pending")
    )
    if volume_blocker_non_actionable:
        actionable_blockers = [
            blocker for blocker in actionable_blockers
            if blocker != "volume_profile_coverage_below_80pct"
        ]
    lifecycle_clean_window_pending = (
        "lifecycle_profile_coverage_below_80pct" in blockers
        and context_field_writer_fix_status == "VERIFIED_POST_DEPLOY"
    )
    if lifecycle_clean_window_pending:
        actionable_blockers = [
            blocker for blocker in actionable_blockers
            if blocker != "lifecycle_profile_coverage_below_80pct"
        ]
    source_component_clean_window_pending = (
        "source_component_coverage_below_80pct" in blockers
        and context_field_writer_fix_status == "VERIFIED_POST_DEPLOY"
    )
    if source_component_clean_window_pending:
        actionable_blockers = [
            blocker for blocker in actionable_blockers
            if blocker != "source_component_coverage_below_80pct"
        ]
    context_clean_window_pending = bool(
        quote_clean_window_pending
        or lifecycle_clean_window_pending
        or source_component_clean_window_pending
    )
    next_highest_priority_blocker = first_blocker_priority(actionable_blockers)
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
        "runtime_signal_source_stale_fail_closed",
        "runtime_signal_source_freshness_missing",
        "runtime_paper_db_unavailable",
        "runtime_paper_db_integrity_marker_exists",
        "runtime_final_evidence_missing",
    }
    context_blockers = {
        "source_quote_clean_coverage_below_80pct",
        "source_quote_executable_coverage_below_80pct",
        "lifecycle_profile_coverage_below_80pct",
        "source_component_coverage_below_80pct",
        "volume_profile_coverage_below_80pct",
        "kline_coverage_below_80pct",
        "schema_mixed_quote_sensitive_slices_blocked",
        "context_schema_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
        "quote_clean_definition_v2_coverage_below_95pct_quote_sensitive_slices_blocked",
        "markov_bucket_coverage_below_80pct",
    }
    hypothesis_validation = readiness_reports.get("hypothesis_validation_audit") or {}
    oos_readiness_summary = build_oos_readiness_summary(readiness_reports)
    oos_probe_refresh = readiness_reports.get("oos_readiness_probe_refresh") or {}
    matured_volume_top_slices = [
        compact_matured_volume_slice(row)
        for row in (matured_volume_cross.get("top_slices") or [])[:10]
    ]
    matured_volume_watch_slices = [
        row for row in matured_volume_top_slices
        if row.get("verdict") == "MATURED_VOLUME_DISCOVERY_WATCH"
    ]
    matured_volume_watch_queue_items = []
    for row in matured_volume_watch_slices[:10]:
        if not isinstance(row, dict):
            continue
        matured_volume_watch_queue_items.append({
            "candidate_id": row.get("candidate_id"),
            "candidate_family": row.get("family"),
            "dimension": row.get("dimension"),
            "slice_value": row.get("slice_value"),
            "matched_gs_count": row.get("matched_gs_count"),
            "slice_raw_gs_count": row.get("slice_raw_gs_count"),
            "slice_signal_count": row.get("slice_signal_count"),
            "candidate_match_count": row.get("candidate_match_count"),
            "match_recall_event": row.get("match_recall_event"),
            "match_precision_event": row.get("match_precision_event"),
            "recall_lift_vs_candidate_baseline": row.get("recall_lift_vs_candidate_baseline"),
            "precision_lift_vs_candidate_baseline": row.get("precision_lift_vs_candidate_baseline"),
            "status": "REVIEW_MATURED_VOLUME_DISCOVERY_WATCH",
            "next_action": "track_same_definition_in_next_clean_window_then_oos_if_repeated",
            "evidence_level": "discovery_same_window",
            "time_legal_note": (
                "matured volume is delayed research context; do not use as immediate entry evidence"
            ),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    matured_volume_watch_queue = {
        "classification": (
            "MATURED_VOLUME_WATCH_QUEUE_READY"
            if matured_volume_watch_queue_items
            else "MATURED_VOLUME_WATCH_QUEUE_EMPTY"
        ),
        "queue_count": len(matured_volume_watch_queue_items),
        "overall_classification": (matured_volume_cross.get("overall") or {}).get("classification"),
        "next_research_action": matured_volume_cross.get("next_research_action"),
        "h1_status": (matured_volume_cross.get("h1_matured_building_volume") or {}).get("status"),
        "watch_slice_count": len(matured_volume_watch_slices),
        "matured_volume_known_rate": (
            (matured_volume_cross.get("matured_volume_context") or {}).get("known_rate")
        ),
        "formal_denominator_changed": bool(matured_volume_cross.get("formal_denominator_changed")),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "items": matured_volume_watch_queue_items,
    }
    low_confidence_audit = readiness_reports.get("low_confidence_research_capture_audit") or {}
    quality_timing_audit = readiness_reports.get("quality_timing_reject_research_audit") or {}
    quality_timing_probe_validation = readiness_reports.get("quality_timing_candidate_probe_validation") or {}
    decision_no_pass_watch_validation = (
        readiness_reports.get("decision_no_pass_quality_timing_watch_validation") or {}
    )
    pending_momentum_decay_validation = readiness_reports.get("pending_momentum_decay_recheck_validation") or {}
    strategy_memory_ingestion = readiness_reports.get("strategy_memory_ingestion_summary") or {}
    strategy_memory_validation = readiness_reports.get("strategy_memory_validation") or {}
    strategy_memory_filtered_bridge = readiness_reports.get("strategy_memory_filtered_winner_bridge") or {}
    strategy_memory_exit_summary = readiness_reports.get("strategy_memory_exit_shadow_summary") or {}
    strategy_memory_delay_summary = readiness_reports.get("strategy_memory_delay_replay_summary") or {}
    capture_60_gap_report = readiness_reports.get("capture_60_gap_report") or {}
    capture_stage_metrics_v3 = readiness_reports.get("capture_stage_metrics") or {}
    pending_to_final_entry_audit_v3 = readiness_reports.get("pending_to_final_entry_audit") or {}
    final_entry_readiness_audit_v3 = readiness_reports.get("final_entry_readiness_audit") or {}
    strategy_memory_capture_validation = readiness_reports.get("strategy_memory_capture_validation") or {}
    shadow_candidate_improvement_queue = readiness_reports.get("shadow_candidate_improvement_queue") or {}
    kline_coverage_resolution = readiness_reports.get("kline_coverage_resolution_audit") or {}
    oos_readiness_summary_v3 = (
        readiness_reports.get("oos_readiness_summary_v3")
        or readiness_reports.get("oos_readiness_summary")
        or oos_readiness_summary
        or {}
    )
    final_entry_status = str(final_entry.get("final_entry_status") or "").upper()
    capture_counts = capture.get("judgment_counts") or {}
    v3_biggest_gap_stage = capture_60_gap_report.get("biggest_gap_stage")
    v3_mode_adjusted_rate = as_float(
        capture_60_gap_report.get("mode_disabled_adjusted_final_eligibility_rate")
    )
    v3_pending_no_final = as_int(
        ((pending_to_final_entry_audit_v3.get("dropoff_counts") or {}).get("pending_no_final_entry")),
        0,
    ) or 0
    if any(blocker in data_blockers for blocker in blockers):
        classification = "BLOCKED_DATA"
    elif any(blocker in context_blockers for blocker in blockers):
        classification = "BLOCKED_CONTEXT_COVERAGE"
    elif v3_mode_adjusted_rate is not None and v3_mode_adjusted_rate >= 0.6:
        classification = "CAPTURE_READINESS_60_REACHED"
    elif v3_biggest_gap_stage in {"final_eligibility", "mode_disabled_adjusted_final_eligibility"} and v3_pending_no_final > 0:
        classification = "FUNNEL_DROPOFF_PENDING_TO_FINAL"
    elif final_entry_status == "FUNNEL_BLOCKED_STUCK":
        classification = "A_CLASS_STUCK_REVIEW_REQUIRED"
    elif final_entry_status == "FUNNEL_BLOCKED_EXPECTED":
        classification = "A_CLASS_EXPECTED_SHADOW"
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
    readiness_shortfall = final_entry.get("readiness_shortfall_summary") or {}
    paper_proposal_readiness = final_entry.get("paper_entry_proposal_readiness") or {}
    shadow_decision_bridge = readiness_reports.get("shadow_decision_bridge_audit") or {}
    shadow_bridge_denominator = shadow_decision_bridge.get("denominator") or {}
    shadow_bridge_gap_count = as_int(
        shadow_bridge_denominator.get("shadow_entry_hypotheses_matched_no_decision_bridge"),
        0,
    ) or 0
    shadow_bridge_mirror_coverage = as_float(
        shadow_bridge_denominator.get("mirror_event_coverage_vs_shadow_bridge_gap")
    )
    shadow_bridge_mirror_truncated = boolish(
        shadow_bridge_denominator.get("mirror_event_truncated")
    )
    shadow_bridge_mirror_complete = bool(
        shadow_bridge_gap_count > 0
        and shadow_bridge_mirror_coverage is not None
        and shadow_bridge_mirror_coverage >= 1.0
        and not shadow_bridge_mirror_truncated
    )
    shadow_bridge_candidate_counts = (shadow_decision_bridge.get("candidate_counts") or [])[:10]
    shadow_bridge_family_counts = (shadow_decision_bridge.get("family_counts") or [])[:10]
    shadow_bridge_reason_counts = (shadow_decision_bridge.get("reason_counts") or [])[:10]
    pass_allow_gap_audit = readiness_reports.get("pass_allow_capture_gap_audit") or {}
    decision_no_pass_review = readiness_reports.get("decision_no_pass_quality_timing_review") or {}
    pass_allow_60_closure_plan = readiness_reports.get("pass_allow_60_closure_plan") or {}
    pass_allow_60_oos_freeze_registry = readiness_reports.get("pass_allow_60_oos_freeze_registry") or {}
    pass_allow_60_closure_tracks = pass_allow_60_closure_plan.get("closure_tracks") or {}
    pass_allow_60_dnp_clusters = (
        pass_allow_60_closure_tracks.get("decision_no_pass_quality_timing_clusters") or {}
    )
    pass_allow_60_clean_cross = (
        pass_allow_60_closure_tracks.get("clean_2d_pass_allow_lift_slices") or {}
    )
    pass_allow_60_shadow_queue = (
        pass_allow_60_closure_tracks.get("shadow_queue_pass_allow_items") or {}
    )
    pass_allow_60_priority_queue = (
        pass_allow_60_closure_plan.get("prioritized_closure_queue") or []
    )
    pass_allow_60_research_only_priority_queue = (
        pass_allow_60_closure_plan.get("research_only_priority_queue") or []
    )
    pass_allow_60_oos_queue = (
        oos_readiness_summary_v3.get("pass_allow_60_closure_oos_queue") or {}
    )
    pass_allow_60_freeze_top_priority_items = (
        pass_allow_60_oos_freeze_registry.get("top_priority_items")
        or pass_allow_60_oos_freeze_registry.get("items")
        or []
    )
    shadow_bridge_review_queue_items = []
    for row in shadow_bridge_candidate_counts[:10]:
        if not isinstance(row, dict):
            continue
        shadow_bridge_review_queue_items.append({
            "candidate_id": row.get("candidate_id"),
            "candidate_family": row.get("family"),
            "count": row.get("count"),
            "status": "REVIEW_SHADOW_MATCHED_NO_DECISION_BRIDGE",
            "next_action": "review_shadow_decision_bridge_instrumentation_without_entry_policy_change",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    shadow_bridge_review_queue = {
        "classification": (
            "SHADOW_DECISION_BRIDGE_REVIEW_QUEUE_READY"
            if shadow_bridge_review_queue_items
            else "SHADOW_DECISION_BRIDGE_REVIEW_QUEUE_EMPTY"
        ),
        "queue_count": len(shadow_bridge_review_queue_items),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "items": shadow_bridge_review_queue_items,
    }
    quality_timing_shadow_review = quality_timing_audit.get("shadow_only_review") or {}
    quality_timing_opportunities = (
        quality_timing_shadow_review.get("top_research_opportunities") or []
    )
    quality_timing_review_queue_items = []
    for row in quality_timing_opportunities[:10]:
        if not isinstance(row, dict):
            continue
        quality_timing_review_queue_items.append({
            "cluster": row.get("cluster"),
            "event_count": row.get("event_count"),
            "share_of_quality_timing_rejects": row.get("share_of_quality_timing_rejects"),
            "share_of_raw_all_gold_silver": row.get("share_of_raw_all_gold_silver"),
            "unique_tokens": row.get("unique_tokens"),
            "dominant_stage": (row.get("stage_counts") or [{}])[0].get("stage"),
            "suggested_shadow_only_action": row.get("suggested_shadow_only_action"),
            "top_candidates": (row.get("top_candidates") or [])[:5],
            "top_lifecycle_source_contexts": (row.get("top_lifecycle_source_contexts") or [])[:5],
            "status": "REVIEW_QUALITY_TIMING_REJECT_CLUSTER",
            "next_action": "track_shadow_only_quality_timing_probe_until_clean_window_then_oos",
            "human_approval_required_if_fix_requires": row.get("human_approval_required_if_fix_requires"),
            "evidence_level": row.get("evidence_level") or "discovery_same_window",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    quality_timing_shadow_review_queue = {
        "classification": (
            "QUALITY_TIMING_SHADOW_REVIEW_QUEUE_READY"
            if quality_timing_review_queue_items
            else "QUALITY_TIMING_SHADOW_REVIEW_QUEUE_EMPTY"
        ),
        "queue_count": len(quality_timing_review_queue_items),
        "dominant_cluster": quality_timing_shadow_review.get("dominant_cluster"),
        "dominant_stage": quality_timing_shadow_review.get("dominant_stage"),
        "quality_timing_false_negative_upper_bound": (
            quality_timing_shadow_review.get("quality_timing_false_negative_upper_bound") or {}
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "items": quality_timing_review_queue_items,
    }
    largest_upstream_gap = readiness_shortfall.get("largest_upstream_gap_category") or {}
    largest_pending_gap = readiness_shortfall.get("largest_pending_to_final_gap_category") or {}
    parallel_next_action = None
    parallel_next_action_reason = None
    if largest_upstream_gap.get("category") == "NO_DECISION_RECORD":
        top_reasons = largest_upstream_gap.get("top_reasons") or []
        if any(row.get("reason") == "shadow_entry_hypotheses_matched_no_decision_bridge" for row in top_reasons):
            if shadow_bridge_mirror_complete and largest_pending_gap.get("category") == "QUALITY_OR_TIMING_REJECT":
                parallel_next_action = "review_quality_timing_rejects_shadow_only"
                parallel_next_action_reason = (
                    "shadow_decision_bridge_mirror_complete;largest_pending_to_final_gap_is_quality_or_timing_reject"
                )
            elif shadow_bridge_mirror_complete:
                parallel_next_action = "continue_shadow_decision_bridge_monitoring"
                parallel_next_action_reason = "shadow_decision_bridge_mirror_complete"
            else:
                parallel_next_action = "audit_shadow_entry_hypotheses_matched_no_decision_bridge"
                parallel_next_action_reason = "largest_upstream_gap_is_shadow_match_without_decision_bridge"
        else:
            parallel_next_action = "audit_no_decision_record_bridge"
            parallel_next_action_reason = "largest_upstream_gap_is_no_decision_record"
    elif largest_pending_gap.get("category") == "QUALITY_OR_TIMING_REJECT":
        parallel_next_action = "review_quality_timing_rejects_shadow_only"
        parallel_next_action_reason = "largest_pending_to_final_gap_is_quality_or_timing_reject"
    stage2_flat = final_entry.get("stage2_flat_summary") or {}
    current_capture_stage = final_entry.get("current_capture_stage")
    top_formal_blocker = first_blocker_priority(blockers) if blockers else (
        final_entry.get("reason") or classification
    )
    top_actionable_blocker = next_highest_priority_blocker
    top_blocker = top_actionable_blocker or top_formal_blocker
    kline_resolution_next_action = (
        (kline_coverage_resolution.get("overall") or {}).get("next_action")
    )
    if context_clean_window_pending and not actionable_blockers:
        top_blocker = "context_clean_window_pending"
    if classification == "BLOCKED_DATA":
        next_action = "resolve_data_integrity_blocker"
    elif classification == "BLOCKED_CONTEXT_COVERAGE":
        if context_clean_window_pending and not actionable_blockers:
            next_action = "wait_for_context_clean_window_and_continue_shadow_oos_collection"
        elif top_actionable_blocker == "kline_coverage_below_80pct" and kline_resolution_next_action:
            next_action = kline_resolution_next_action
        elif (
            volume_blocker_non_actionable
            and not actionable_blockers
        ):
            next_action = volume_profile_state.get("next_action")
        else:
            next_action = "resolve_context_coverage_blocker"
    elif classification == "FUNNEL_DROPOFF_PENDING_TO_FINAL":
        next_action = "audit_pending_to_final_entry_dropoff_shadow_only"
    elif classification == "A_CLASS_EXPECTED_SHADOW":
        next_action = "wait_clean_windows_or_fix_failed_context_coverage"
    elif classification == "A_CLASS_STUCK_REVIEW_REQUIRED":
        next_action = "human_review_a_class_shadow_state"
    elif classification == "CAPTURE_READINESS_60_REACHED":
        next_action = "prepare_paper_entry_proposal_for_human_review_without_enabling_runtime"
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
        "current_commit": current_commit,
        "deployment_commit": deployed_commit,
        "verdict": classification,
        "classification": classification,
        "next_action": next_action,
        "parallel_next_action": parallel_next_action,
        "parallel_next_action_reason": parallel_next_action_reason,
        "capture_60_biggest_gap_stage": capture_60_gap_report.get("biggest_gap_stage"),
        "capture_60_target_shortfall_stage": (
            capture_60_gap_report.get("target_shortfall_stage")
            or capture_60_gap_report.get("biggest_gap_stage")
        ),
        "capture_60_additional_count_needed": (
            capture_60_gap_report.get("additional_count_needed_to_60")
        ),
        "capture_60_current_target_stage": capture_60_gap_report.get("current_target_stage"),
        "capture_60_current_target_count": capture_60_gap_report.get("current_target_count"),
        "capture_60_current_target_rate": capture_60_gap_report.get("current_target_rate"),
        "capture_60_current_target_additional_count_needed": (
            capture_60_gap_report.get("current_target_additional_count_needed_to_60")
        ),
        "capture_60_current_target_next_best_allowed_action": (
            capture_60_gap_report.get("current_target_next_best_allowed_action")
        ),
        "capture_60_next_best_allowed_action": capture_60_gap_report.get("next_best_allowed_action"),
        "capture_60_target_loop": {
            "available": bool(capture_60_gap_report),
            "target_capture_rate": capture_60_gap_report.get("target_capture_rate"),
            "raw_gold_silver_denominator": capture_60_gap_report.get("raw_gold_silver_denominator"),
            "target_60_count": capture_60_gap_report.get("target_60_count"),
            "biggest_gap_stage": capture_60_gap_report.get("biggest_gap_stage"),
            "target_shortfall_stage": capture_60_gap_report.get(
                "target_shortfall_stage"
            ) or capture_60_gap_report.get("biggest_gap_stage"),
            "largest_transition_dropoff": capture_60_gap_report.get(
                "largest_transition_dropoff"
            ) or capture_60_gap_report.get("largest_stage_dropoff") or {},
            "recommended_parallel_tracks": capture_60_gap_report.get(
                "recommended_parallel_tracks"
            ) or [],
            "gap_interpretation": capture_60_gap_report.get("gap_interpretation") or {},
            "additional_count_needed_to_60": capture_60_gap_report.get("additional_count_needed_to_60"),
            "next_best_allowed_action": capture_60_gap_report.get("next_best_allowed_action"),
            "current_target_stage": capture_60_gap_report.get("current_target_stage"),
            "current_target_count": capture_60_gap_report.get("current_target_count"),
            "current_target_rate": capture_60_gap_report.get("current_target_rate"),
            "current_target_additional_count_needed_to_60": capture_60_gap_report.get(
                "current_target_additional_count_needed_to_60"
            ),
            "current_target_reached": capture_60_gap_report.get("current_target_reached"),
            "current_target_next_best_allowed_action": capture_60_gap_report.get(
                "current_target_next_best_allowed_action"
            ),
            "detector_capture_rate": capture_60_gap_report.get("detector_capture_rate"),
            "decision_capture_rate": capture_60_gap_report.get("decision_capture_rate"),
            "pass_allow_capture_rate": capture_60_gap_report.get("pass_allow_capture_rate"),
            "pending_capture_rate": capture_60_gap_report.get("pending_capture_rate"),
            "final_eligibility_rate": capture_60_gap_report.get("final_eligibility_rate"),
            "mode_disabled_adjusted_final_eligibility_rate": capture_60_gap_report.get(
                "mode_disabled_adjusted_final_eligibility_rate"
            ),
            "paper_capture_rate": capture_60_gap_report.get("paper_capture_rate"),
            "realized_capture_rate": capture_60_gap_report.get("realized_capture_rate"),
            "promotion_allowed": False,
        },
        "pass_allow_60_closure_priority_queue": {
            "available": bool(pass_allow_60_closure_plan),
            "classification": pass_allow_60_closure_plan.get("classification"),
            "next_action": pass_allow_60_closure_plan.get("next_action"),
            "target_gap": pass_allow_60_closure_plan.get("target_gap") or {},
            "additional_count_needed_to_60": (
                pass_allow_60_closure_plan.get("additional_count_needed_to_60")
                or (pass_allow_60_closure_plan.get("target_gap") or {}).get(
                    "additional_count_needed_to_60"
                )
                or capture_60_gap_report.get("additional_count_needed_to_60")
            ),
            "priority_queue_count": (
                pass_allow_60_closure_plan.get("priority_queue_count")
                if pass_allow_60_closure_plan.get("priority_queue_count") is not None
                else len(pass_allow_60_priority_queue)
            ),
            "research_only_priority_queue_count": (
                pass_allow_60_closure_plan.get("research_only_priority_queue_count")
                if pass_allow_60_closure_plan.get("research_only_priority_queue_count") is not None
                else len(pass_allow_60_research_only_priority_queue)
            ),
            "formal_blocked_count": pass_allow_60_closure_plan.get("formal_blocked_count"),
            "top_priority_items": pass_allow_60_priority_queue[:8],
            "research_only_top_priority_items": pass_allow_60_research_only_priority_queue[:5],
            "oos_freeze_ready_count": len([
                row for row in pass_allow_60_priority_queue
                if isinstance(row, dict) and row.get("oos_freeze_ready")
            ]),
            "oos_queue_count": (
                oos_readiness_summary_v3.get("pass_allow_60_closure_oos_queue_count")
                or pass_allow_60_oos_queue.get("queue_count")
            ),
            "next_oos_action": (
                oos_readiness_summary_v3.get("next_pass_allow_60_closure_oos_action")
                or pass_allow_60_oos_queue.get("next_action")
            ),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        },
        "capture_stage_metrics_v3": {
            "available": bool(capture_stage_metrics_v3),
            "stage_counts": capture_stage_metrics_v3.get("stage_counts") or {},
            "raw_gold_silver_denominator": capture_stage_metrics_v3.get("raw_gold_silver_denominator"),
            "target_60_count": capture_stage_metrics_v3.get("target_60_count"),
            "detector_capture_count": capture_stage_metrics_v3.get("detector_capture_count"),
            "detector_capture_rate": capture_stage_metrics_v3.get("detector_capture_rate"),
            "decision_capture_count": capture_stage_metrics_v3.get("decision_capture_count"),
            "decision_capture_rate": capture_stage_metrics_v3.get("decision_capture_rate"),
            "pass_allow_capture_count": capture_stage_metrics_v3.get("pass_allow_capture_count"),
            "pass_allow_capture_rate": capture_stage_metrics_v3.get("pass_allow_capture_rate"),
            "pending_capture_count": capture_stage_metrics_v3.get("pending_capture_count"),
            "pending_capture_rate": capture_stage_metrics_v3.get("pending_capture_rate"),
            "final_eligibility_count": capture_stage_metrics_v3.get("final_eligibility_count"),
            "final_eligibility_rate": capture_stage_metrics_v3.get("final_eligibility_rate"),
            "final_eligibility_capture_count": capture_stage_metrics_v3.get("final_eligibility_capture_count"),
            "final_eligibility_capture_rate": capture_stage_metrics_v3.get("final_eligibility_capture_rate"),
            "mode_disabled_adjusted_final_eligibility_count": capture_stage_metrics_v3.get(
                "mode_disabled_adjusted_final_eligibility_count"
            ),
            "mode_disabled_adjusted_final_eligibility_rate": capture_stage_metrics_v3.get(
                "mode_disabled_adjusted_final_eligibility_rate"
            ),
            "paper_trade_intent_count": capture_stage_metrics_v3.get("paper_trade_intent_count"),
            "paper_trade_intent_rate": capture_stage_metrics_v3.get("paper_trade_intent_rate"),
            "paper_capture_count": capture_stage_metrics_v3.get("paper_capture_count"),
            "paper_capture_rate": capture_stage_metrics_v3.get("paper_capture_rate"),
            "realized_capture_count": capture_stage_metrics_v3.get("realized_capture_count"),
            "realized_capture_rate": capture_stage_metrics_v3.get("realized_capture_rate"),
            "largest_stage_dropoff": capture_stage_metrics_v3.get("largest_stage_dropoff") or {},
            "promotion_allowed": False,
        },
        "context_dimension_eligibility": {
            "available": bool(context_dimension_eligibility),
            "status_counts": context_dimension_eligibility.get("status_counts") or {},
            "clean_dimensions": context_dimension_eligibility.get("clean_dimensions") or [],
            "blocked_dimensions": context_dimension_eligibility.get("blocked_dimensions") or [],
            "dimensions": context_dimension_eligibility.get("dimensions") or {},
            "promotion_allowed": False,
        },
        "pending_to_final_entry_audit_v3": {
            "available": bool(pending_to_final_entry_audit_v3),
            "dropoff_counts": pending_to_final_entry_audit_v3.get("dropoff_counts") or {},
            "pending_no_final_entry_classification": (
                pending_to_final_entry_audit_v3.get("pending_no_final_entry_classification") or {}
            ),
            "largest_transition_dropoff_review": (
                pending_to_final_entry_audit_v3.get("largest_transition_dropoff_review") or {}
            ),
            "stale_before_final_review": (
                pending_to_final_entry_audit_v3.get("stale_before_final_review") or {}
            ),
            "promotion_allowed": False,
        },
        "final_entry_readiness_audit_v3": {
            "available": bool(final_entry_readiness_audit_v3),
            "final_entry_status": final_entry_readiness_audit_v3.get("final_entry_status"),
            "reason": final_entry_readiness_audit_v3.get("reason"),
            "human_action_required": boolish(final_entry_readiness_audit_v3.get("human_action_required")),
            "paper_entry_proposal_readiness": final_entry_readiness_audit_v3.get("paper_entry_proposal_readiness") or {},
            "promotion_allowed": False,
        },
        "strategy_memory_capture_validation": {
            "available": bool(strategy_memory_capture_validation),
            "hypotheses_count": strategy_memory_capture_validation.get("hypotheses_count", 0),
            "status_counts": strategy_memory_capture_validation.get("status_counts") or {},
            "top_hypotheses": (strategy_memory_capture_validation.get("hypotheses") or [])[:12],
            "promotion_allowed": False,
        },
        "shadow_candidate_improvement_queue": {
            "available": bool(shadow_candidate_improvement_queue),
            "queue_count": shadow_candidate_improvement_queue.get("queue_count", 0),
            "source_counts": shadow_candidate_improvement_queue.get("source_counts") or {},
            "top_items": (shadow_candidate_improvement_queue.get("top_items") or [])[:12],
            "promotion_allowed": False,
        },
        "oos_readiness_summary_v3": {
            "available": bool(oos_readiness_summary_v3),
            "classification": oos_readiness_summary_v3.get("classification"),
            "available_probe_count": oos_readiness_summary_v3.get("available_probe_count"),
            "sufficient_probe_count": oos_readiness_summary_v3.get("sufficient_probe_count"),
            "pass_allow_60_closure_oos_queue_count": (
                oos_readiness_summary_v3.get("pass_allow_60_closure_oos_queue_count")
            ),
            "pass_allow_60_oos_frozen_definition_count": (
                oos_readiness_summary_v3.get("pass_allow_60_oos_frozen_definition_count")
            ),
            "next_pass_allow_60_closure_oos_action": (
                oos_readiness_summary_v3.get("next_pass_allow_60_closure_oos_action")
            ),
            "pass_allow_60_closure_oos_queue": (
                oos_readiness_summary_v3.get("pass_allow_60_closure_oos_queue") or {}
            ),
            "pass_allow_60_oos_freeze_registry": (
                oos_readiness_summary_v3.get("pass_allow_60_oos_freeze_registry") or {}
            ),
            "pass_allow_60_oos_readiness_monitor": (
                oos_readiness_summary_v3.get("pass_allow_60_oos_readiness_monitor")
                or readiness_reports.get("pass_allow_60_oos_readiness_monitor")
                or {}
            ),
            "pass_allow_60_post_freeze_oos_validation": (
                oos_readiness_summary_v3.get("pass_allow_60_post_freeze_oos_validation") or {}
            ),
            "promotion_allowed": False,
        },
        "pass_allow_60_closure_oos_queue_count": (
            oos_readiness_summary_v3.get("pass_allow_60_closure_oos_queue_count")
            or pass_allow_60_oos_queue.get("queue_count")
        ),
        "next_pass_allow_60_closure_oos_action": (
            oos_readiness_summary_v3.get("next_pass_allow_60_closure_oos_action")
            or pass_allow_60_oos_queue.get("next_action")
        ),
        "pass_allow_60_closure_oos_queue": pass_allow_60_oos_queue,
        "pass_allow_60_oos_freeze_priority_queue": {
            "available": bool(pass_allow_60_oos_freeze_registry),
            "classification": pass_allow_60_oos_freeze_registry.get("classification"),
            "priority_queue_count": pass_allow_60_oos_freeze_registry.get("priority_queue_count"),
            "frozen_definition_count": pass_allow_60_oos_freeze_registry.get("frozen_definition_count"),
            "unique_priority_plan_item_count": (
                pass_allow_60_oos_freeze_registry.get("unique_priority_plan_item_count")
            ),
            "top_priority_items": pass_allow_60_freeze_top_priority_items[:8],
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        },
        "blocked_subtype": blocked_subtype,
        "promotion_allowed": promotion_allowed,
        "human_action_required": human_action_required,
        "current_capture_stage": current_capture_stage,
        "mode_status": final_entry.get("mode_status") or stage2_flat.get("mode_status"),
        "mode_action": final_entry.get("mode_action") or stage2_flat.get("mode_action"),
        "mode_reason": final_entry.get("mode_reason") or stage2_flat.get("mode_reason"),
        "shadow_entered_ts": final_entry.get("shadow_entered_ts") or stage2_flat.get("shadow_entered_ts"),
        "shadow_entered_ts_source": (
            final_entry.get("shadow_entered_ts_source")
            or stage2_flat.get("shadow_entered_ts_source")
        ),
        "cooldown_elapsed": final_entry.get("cooldown_elapsed") if "cooldown_elapsed" in final_entry else stage2_flat.get("cooldown_elapsed"),
        "cooldown_remaining_sec": (
            final_entry.get("cooldown_remaining_sec")
            if "cooldown_remaining_sec" in final_entry
            else stage2_flat.get("cooldown_remaining_sec")
        ),
        "clean_window_required_conditions": (
            final_entry.get("clean_window_required_conditions")
            or stage2_flat.get("clean_window_required_conditions")
            or []
        ),
        "clean_window_passed_conditions": (
            final_entry.get("clean_window_passed_conditions")
            or stage2_flat.get("clean_window_passed_conditions")
            or []
        ),
        "clean_window_failed_conditions": (
            final_entry.get("clean_window_failed_conditions")
            or stage2_flat.get("clean_window_failed_conditions")
            or []
        ),
        "raw_gs_events": final_entry.get("raw_gs_events") or stage2_flat.get("raw_gs_events"),
        "raw_gs_signal_ids": final_entry.get("raw_gs_signal_ids") or stage2_flat.get("raw_gs_signal_ids"),
        "candidate_matched_any": (
            final_entry.get("candidate_matched_any")
            if "candidate_matched_any" in final_entry
            else stage2_flat.get("candidate_matched_any")
        ),
        "has_decision_record": (
            final_entry.get("has_decision_record")
            if "has_decision_record" in final_entry
            else stage2_flat.get("has_decision_record")
        ),
        "pass_allow": final_entry.get("pass_allow") if "pass_allow" in final_entry else stage2_flat.get("pass_allow"),
        "pending_entry": (
            final_entry.get("pending_entry")
            if "pending_entry" in final_entry
            else stage2_flat.get("pending_entry")
        ),
        "reached_final_entry_contract": (
            final_entry.get("reached_final_entry_contract")
            if "reached_final_entry_contract" in final_entry
            else stage2_flat.get("reached_final_entry_contract")
        ),
        "final_entry_block_mode_disabled": (
            final_entry.get("final_entry_block_mode_disabled")
            if "final_entry_block_mode_disabled" in final_entry
            else stage2_flat.get("final_entry_block_mode_disabled")
        ),
        "final_entry_block_mode_disabled_only": (
            final_entry.get("final_entry_block_mode_disabled_only")
            if "final_entry_block_mode_disabled_only" in final_entry
            else stage2_flat.get("final_entry_block_mode_disabled_only")
        ),
        "final_entry_block_expected_rr": (
            final_entry.get("final_entry_block_expected_rr")
            if "final_entry_block_expected_rr" in final_entry
            else stage2_flat.get("final_entry_block_expected_rr")
        ),
        "final_entry_block_spread": (
            final_entry.get("final_entry_block_spread")
            if "final_entry_block_spread" in final_entry
            else stage2_flat.get("final_entry_block_spread")
        ),
        "paper_trade_intent": (
            final_entry.get("paper_trade_intent")
            if "paper_trade_intent" in final_entry
            else stage2_flat.get("paper_trade_intent")
        ),
        "paper_trade_committed": (
            final_entry.get("paper_trade_committed")
            if "paper_trade_committed" in final_entry
            else stage2_flat.get("paper_trade_committed")
        ),
        "stage2_entry_funnel_summary": stage2_flat,
        "detector_capture_rate": capture_stage_rates.get("detector_capture_rate"),
        "decision_record_capture_rate": capture_stage_rates.get("decision_record_capture_rate"),
        "decision_capture_rate": (
            capture_stage_rates.get("decision_capture_rate")
            if "decision_capture_rate" in capture_stage_rates
            else capture_stage_rates.get("decision_record_capture_rate")
        ),
        "pass_allow_capture_rate": capture_stage_rates.get("pass_allow_capture_rate"),
        "pending_capture_rate": capture_stage_rates.get("pending_capture_rate"),
        "final_entry_contract_reach_rate": capture_stage_rates.get("final_entry_contract_reach_rate"),
        "final_eligibility_capture_rate": (
            mode_adjusted_final.get("rate")
            if "rate" in mode_adjusted_final
            else capture_stage_rates.get("mode_disabled_adjusted_final_eligibility_rate")
        ),
        "mode_disabled_adjusted_final_eligibility_rate": (
            mode_adjusted_final.get("rate")
            if "rate" in mode_adjusted_final
            else capture_stage_rates.get("mode_disabled_adjusted_final_eligibility_rate")
        ),
        "paper_capture_rate": capture_stage_rates.get("paper_capture_rate"),
        "paper_trade_intent_rate": capture_stage_rates.get("paper_trade_intent_rate"),
        "realized_capture_rate": capture_stage_rates.get("realized_capture_rate"),
        "readiness_shortfall_summary": readiness_shortfall,
        "paper_entry_proposal_readiness": paper_proposal_readiness,
        "upstream_funnel_gap_summary": {
            "raw_signal_ids": upstream_gap.get("raw_signal_ids"),
            "decision_record_signal_ids": upstream_gap.get("decision_record_signal_ids"),
            "pass_or_allow_signal_ids": upstream_gap.get("pass_or_allow_signal_ids"),
            "pending_entry_signal_ids": upstream_gap.get("pending_entry_signal_ids"),
            "no_decision_record": upstream_gap.get("no_decision_record"),
            "no_decision_record_root_cause_counts": upstream_gap.get("no_decision_record_root_cause_counts") or [],
            "no_decision_record_subroot_cause_counts": upstream_gap.get("no_decision_record_subroot_cause_counts") or [],
            "shadow_no_decision_entry_hypothesis_family_counts": upstream_gap.get(
                "shadow_no_decision_entry_hypothesis_family_counts"
            )
            or [],
            "shadow_no_decision_entry_hypothesis_candidate_counts": upstream_gap.get(
                "shadow_no_decision_entry_hypothesis_candidate_counts"
            )
            or [],
            "shadow_no_decision_entry_hypothesis_reason_counts": upstream_gap.get(
                "shadow_no_decision_entry_hypothesis_reason_counts"
            )
            or [],
            "no_decision_token_time_decision_without_exact_signal_id": upstream_gap.get(
                "no_decision_token_time_decision_without_exact_signal_id"
            ),
            "no_decision_candidate_shadow_observed_no_decision_event": upstream_gap.get(
                "no_decision_candidate_shadow_observed_no_decision_event"
            ),
            "no_decision_partial_candidate_observation_no_decision_event": upstream_gap.get(
                "no_decision_partial_candidate_observation_no_decision_event"
            ),
            "no_decision_no_candidate_observation_or_decision_event": upstream_gap.get(
                "no_decision_no_candidate_observation_or_decision_event"
            ),
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
        "shadow_decision_bridge_audit_summary": {
            "available": bool(shadow_decision_bridge),
            "status": shadow_decision_bridge.get("status"),
            "shadow_bridge_gap_count": shadow_bridge_gap_count,
            "mirror_event_count": shadow_bridge_denominator.get("mirror_event_count"),
            "mirror_event_coverage_vs_shadow_bridge_gap": shadow_bridge_mirror_coverage,
            "mirror_event_truncated": shadow_bridge_mirror_truncated,
            "mirror_complete": shadow_bridge_mirror_complete,
            "top_candidate_counts": shadow_bridge_candidate_counts,
            "top_family_counts": shadow_bridge_family_counts,
            "top_reason_counts": shadow_bridge_reason_counts,
            "review_queue": shadow_bridge_review_queue,
            "promotion_allowed": False,
            "automatic_bridge_to_entry_allowed": False,
            "paper_enablement_allowed": False,
        },
        "pass_allow_capture_gap_audit": {
            "available": bool(pass_allow_gap_audit),
            "next_action": pass_allow_gap_audit.get("next_action"),
            "target_gap": pass_allow_gap_audit.get("target_gap") or {},
            "dropoff_counts": pass_allow_gap_audit.get("dropoff_counts") or {},
            "gap_source_upper_bounds": pass_allow_gap_audit.get("gap_source_upper_bounds") or {},
            "gap_explainability": pass_allow_gap_audit.get("gap_explainability") or {},
            "shadow_decision_bridge": pass_allow_gap_audit.get("shadow_decision_bridge") or {},
            "decision_no_pass_quality_timing": pass_allow_gap_audit.get("decision_no_pass_quality_timing") or {},
            "context_constraints": pass_allow_gap_audit.get("context_constraints") or {},
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        },
        "decision_no_pass_quality_timing_review": {
            "available": bool(decision_no_pass_review),
            "classification": decision_no_pass_review.get("classification"),
            "next_action": decision_no_pass_review.get("next_action"),
            "target_gap": decision_no_pass_review.get("target_gap") or {},
            "decision_no_pass_quality_timing_event_count": (
                decision_no_pass_review.get("decision_no_pass_quality_timing_event_count")
            ),
            "cluster_count": decision_no_pass_review.get("cluster_count"),
            "selected_cluster_count": decision_no_pass_review.get("selected_cluster_count"),
            "selected_upper_bound_event_count": decision_no_pass_review.get("selected_upper_bound_event_count"),
            "covers_current_pass_allow_gap_upper_bound": (
                decision_no_pass_review.get("covers_current_pass_allow_gap_upper_bound")
            ),
            "selected_clusters_to_cover_current_pass_allow_gap_upper_bound": (
                decision_no_pass_review.get("selected_clusters_to_cover_current_pass_allow_gap_upper_bound")
                or []
            ),
            "top_clusters": (decision_no_pass_review.get("clusters") or [])[:8],
            "context_constraints": decision_no_pass_review.get("context_constraints") or {},
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        },
        "pass_allow_60_closure_plan": {
            "available": bool(pass_allow_60_closure_plan),
            "classification": pass_allow_60_closure_plan.get("classification"),
            "next_action": pass_allow_60_closure_plan.get("next_action"),
            "priority_queue_count": (
                pass_allow_60_closure_plan.get("priority_queue_count")
                if pass_allow_60_closure_plan.get("priority_queue_count") is not None
                else len(pass_allow_60_priority_queue)
            ),
            "research_only_priority_queue_count": (
                pass_allow_60_closure_plan.get("research_only_priority_queue_count")
                if pass_allow_60_closure_plan.get("research_only_priority_queue_count") is not None
                else len(pass_allow_60_research_only_priority_queue)
            ),
            "formal_blocked_count": pass_allow_60_closure_plan.get("formal_blocked_count"),
            "top_priority_items": pass_allow_60_priority_queue[:8],
            "research_only_top_priority_items": pass_allow_60_research_only_priority_queue[:5],
            "target_gap": pass_allow_60_closure_plan.get("target_gap") or {},
            "residual_gap_supplemental_tracks": (
                pass_allow_60_closure_plan.get("residual_gap_supplemental_tracks") or {}
            ),
            "decision_no_pass_quality_timing_clusters": pass_allow_60_dnp_clusters,
            "clean_2d_pass_allow_lift_slices": {
                "count": pass_allow_60_clean_cross.get("count"),
                "items": (pass_allow_60_clean_cross.get("items") or [])[:12],
            },
            "shadow_queue_pass_allow_items": {
                "count": pass_allow_60_shadow_queue.get("count"),
                "items": (pass_allow_60_shadow_queue.get("items") or [])[:12],
            },
            "context_constraints": pass_allow_60_closure_plan.get("context_constraints") or {},
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        },
        "pass_allow_60_oos_freeze_registry": {
            "available": bool(pass_allow_60_oos_freeze_registry),
            "classification": pass_allow_60_oos_freeze_registry.get("classification"),
            "frozen_definition_count": pass_allow_60_oos_freeze_registry.get("frozen_definition_count"),
            "priority_queue_count": pass_allow_60_oos_freeze_registry.get("priority_queue_count"),
            "unique_priority_plan_item_count": (
                pass_allow_60_oos_freeze_registry.get("unique_priority_plan_item_count")
            ),
            "source_counts": pass_allow_60_oos_freeze_registry.get("source_counts") or {},
            "oos_requirements": pass_allow_60_oos_freeze_registry.get("oos_requirements") or {},
            "top_priority_items": pass_allow_60_freeze_top_priority_items[:8],
            "items": (pass_allow_60_oos_freeze_registry.get("items") or [])[:12],
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        },
        "quality_timing_shadow_review_queue": quality_timing_shadow_review_queue,
        "top_blocker": top_blocker,
        "top_actionable_blocker": top_actionable_blocker,
        "top_formal_blocker": top_formal_blocker,
        "non_quote_sensitive_capture_discovery_allowed": non_quote_sensitive_capture_discovery_allowed,
        "quote_sensitive_slices_blocked": quote_sensitive_slices_blocked,
        "formal_volume_sensitive_slices_blocked": "volume_profile_coverage_below_80pct" in blockers,
        "shadow_matured_volume_slices_evaluable": bool(
            volume_profile_state.get("shadow_matured_volume_slices_evaluable")
        ),
        "volume_data_coverage_pending": bool(
            volume_profile_state.get("matured_volume_coverage_pending")
        ),
        "canary_increase_allowed": False,
        "strategy_change_allowed": False,
        "hard_gate_change_allowed": False,
        "exit_gate_change_allowed": False,
        "blockers": blockers,
        "actionable_blockers": actionable_blockers,
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
            "post_deploy_context_field_smoke_test": context_monitor_field_smoke,
            "recent_context_writer_health": context_monitor_recent_writer_health,
            "reconciled_warnings": reconciled_context_warnings,
        },
        "context_clean_window_progress": build_context_clean_window_progress(
            blockers=blockers,
            actionable_blockers=actionable_blockers,
            context_clean_window_pending=context_clean_window_pending,
            quote_clean_window_pending=quote_clean_window_pending,
            context_field_writer_fix_status=context_field_writer_fix_status,
            quote_writer_fix_status=quote_writer_fix_status,
            context_monitor_clean_window=context_monitor_clean_window,
            context_monitor_field_audit=context_monitor_field_audit,
            context_monitor_field_smoke=context_monitor_field_smoke,
            context_monitor_recent_writer_health=context_monitor_recent_writer_health,
        ),
        "runtime_health_snapshot": {
            "available": bool(runtime_health_snapshot),
            "status": runtime_health_snapshot.get("status"),
            "blockers": runtime_health_blockers,
            "warnings": runtime_health_warnings,
            "signal_source_freshness": runtime_health_snapshot.get("signal_source_freshness") or {},
            "paper_review_snapshot": runtime_health_snapshot.get("paper_review_snapshot") or {},
            "paper_fast_lane_health": runtime_health_snapshot.get("paper_fast_lane_health") or {},
            "paper_db": runtime_health_snapshot.get("paper_db") or {},
            "runtime_final_evidence": runtime_health_snapshot.get("runtime_final_evidence") or {},
            "observer_logs": runtime_health_snapshot.get("observer_logs") or {},
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        },
        "runtime_health_status": runtime_health_snapshot.get("status"),
        "runtime_health_blockers": runtime_health_blockers,
        "runtime_health_warnings": runtime_health_warnings,
        "quote_writer_fix_status": quote_writer_fix_status,
        "quote_clean_window_status": quote_clean_window_status,
        "quote_clean_window_eta_iso": context_monitor_clean_window.get("estimated_clean_at_iso"),
        "quote_clean_window_seconds_remaining": context_monitor_clean_window.get("seconds_until_natural_clean_window"),
        "context_field_writer_fix_status": context_field_writer_fix_status,
        "context_clean_window_pending": context_clean_window_pending,
        "context_clean_window_eta_iso": context_monitor_clean_window.get("estimated_clean_at_iso"),
        "context_clean_window_seconds_remaining": context_monitor_clean_window.get("seconds_until_natural_clean_window"),
        "lifecycle_clean_window_pending": lifecycle_clean_window_pending,
        "source_component_clean_window_pending": source_component_clean_window_pending,
        "volume_profile_coverage": readiness_reports.get("volume_profile_coverage") or {},
        "kline_coverage": readiness_reports.get("kline_coverage") or {},
        "volume_profile_blocker_state": volume_profile_state,
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
                    "unknown_diagnostics",
                )
            } | {
                "recent_windows": volume_kline_audit.get("volume_context_recent_windows") or {},
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
            "signal_id_reconciliation": matured_volume_cross.get("signal_id_reconciliation") or {},
            "h1_matured_building_volume": matured_volume_cross.get("h1_matured_building_volume") or {},
            "judgment_counts": matured_volume_cross.get("judgment_counts") or {},
            "top_slices": matured_volume_top_slices,
            "top_watch_slices": matured_volume_watch_slices,
            "watch_slice_count": len(matured_volume_watch_slices),
            "review_queue": matured_volume_watch_queue,
            "next_research_action": (
                "review_non_h1_matured_volume_watch_slices"
                if matured_volume_watch_slices
                and (matured_volume_cross.get("h1_matured_building_volume") or {}).get("status") != "MATURED_VOLUME_DISCOVERY_WATCH"
                else (matured_volume_cross.get("overall") or {}).get("next_action")
            ),
        },
        "matured_volume_watch_queue": matured_volume_watch_queue,
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
        "oos_readiness_summary": oos_readiness_summary_v3 or oos_readiness_summary,
        "oos_probe_refresh_status": compact_oos_probe_refresh(oos_probe_refresh),
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
        "kline_coverage_resolution_audit": {
            "available": bool(kline_coverage_resolution),
            "overall": kline_coverage_resolution.get("overall") or {},
            "promotion_allowed": False,
            "formal_denominator_changed": bool(kline_coverage_resolution.get("formal_denominator_changed")),
            "formal_kline_coverage": kline_coverage_resolution.get("formal_kline_coverage") or {},
            "research_recoverability": kline_coverage_resolution.get("research_recoverability") or {},
            "low_confidence_capture_summary": (
                kline_coverage_resolution.get("low_confidence_capture_summary") or {}
            ),
            "allowed_resolution_tracks": (
                kline_coverage_resolution.get("allowed_resolution_tracks") or []
            ),
        },
        "quality_timing_reject_research_audit": {
            "available": bool(quality_timing_audit),
            "verdict": quality_timing_audit.get("verdict"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "denominator": quality_timing_audit.get("denominator") or {},
            "candidate_match_attribution": {
                key: (quality_timing_audit.get("candidate_match_attribution") or {}).get(key)
                for key in (
                    "full_candidate_coverage_rate",
                    "candidate_matched_any_events",
                    "candidate_matched_any_rate",
                    "top_candidates",
                    "top_families",
                )
            },
            "readiness_impact_upper_bound": {
                key: (quality_timing_audit.get("readiness_impact_upper_bound") or {}).get(key)
                for key in (
                    "target_final_eligibility_rate",
                    "raw_all_gold_silver_event_rows",
                    "target_final_eligibility_event_count",
                    "current_final_entry_contract_signal_count",
                    "current_final_entry_contract_rate",
                    "quality_timing_reject_event_rows",
                    "current_gap_to_60pct_event_count",
                    "quality_timing_rejects_share_of_current_60pct_gap_upper_bound",
                    "upper_bound_final_eligibility_count_if_all_quality_timing_resolved",
                    "upper_bound_final_eligibility_rate_if_all_quality_timing_resolved",
                    "residual_gap_to_60pct_after_all_quality_timing_upper_bound",
                    "would_all_quality_timing_resolution_reach_60pct_upper_bound",
                    "interpretation",
                )
            } | {
                "top_cluster_upper_bounds": (
                    ((quality_timing_audit.get("readiness_impact_upper_bound") or {}).get("cluster_upper_bounds") or [])[:8]
                ),
                "promotion_allowed": False,
                "strategy_change_allowed": False,
                "automatic_runtime_change_allowed": False,
                "paper_enablement_allowed": False,
            },
            "stage_attribution": {
                "stage_counts": ((quality_timing_audit.get("stage_attribution") or {}).get("stage_counts") or [])[:8],
                "reason_counts": ((quality_timing_audit.get("stage_attribution") or {}).get("reason_counts") or [])[:8],
            },
            "context_attribution": {
                "lifecycle_source_counts": ((quality_timing_audit.get("context_attribution") or {}).get("lifecycle_source_counts") or [])[:10],
                "markov_bucket_counts": ((quality_timing_audit.get("context_attribution") or {}).get("markov_bucket_counts") or [])[:8],
            },
            "shadow_only_review": {
                "classification": ((quality_timing_audit.get("shadow_only_review") or {}).get("classification")),
                "dominant_cluster": ((quality_timing_audit.get("shadow_only_review") or {}).get("dominant_cluster")),
                "dominant_stage": ((quality_timing_audit.get("shadow_only_review") or {}).get("dominant_stage")),
                "quality_timing_false_negative_upper_bound": (
                    (quality_timing_audit.get("shadow_only_review") or {}).get("quality_timing_false_negative_upper_bound")
                    or {}
                ),
                "research_opportunity_count": (
                    (quality_timing_audit.get("shadow_only_review") or {}).get("research_opportunity_count")
                ),
                "top_research_opportunities": (
                    ((quality_timing_audit.get("shadow_only_review") or {}).get("top_research_opportunities") or [])[:8]
                ),
                "promotion_allowed": False,
                "strategy_change_allowed": False,
                "automatic_runtime_change_allowed": False,
                "paper_enablement_allowed": False,
            },
            "review_queue": quality_timing_shadow_review_queue,
            "shadow_only_next_actions": quality_timing_audit.get("shadow_only_next_actions") or [],
            "blockers": quality_timing_audit.get("blockers") or [],
        },
        "quality_timing_candidate_probe_validation": {
            "available": bool(quality_timing_probe_validation),
            "classification": quality_timing_probe_validation.get("classification"),
            "next_action": quality_timing_probe_validation.get("next_action"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "denominator": quality_timing_probe_validation.get("denominator") or {},
            "status_counts": quality_timing_probe_validation.get("status_counts") or {},
            "oos_readiness_queue": (
                quality_timing_probe_validation.get("oos_readiness_queue") or {}
            ),
            "top_repeated_probes": (
                quality_timing_probe_validation.get("top_repeated_probes") or []
            )[:10],
        },
        "decision_no_pass_quality_timing_watch_validation": {
            "available": bool(decision_no_pass_watch_validation),
            "classification": decision_no_pass_watch_validation.get("classification"),
            "next_action": decision_no_pass_watch_validation.get("next_action"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "denominator": decision_no_pass_watch_validation.get("denominator") or {},
            "status_counts": decision_no_pass_watch_validation.get("status_counts") or {},
            "oos_readiness_queue": (
                decision_no_pass_watch_validation.get("oos_readiness_queue") or {}
            ),
            "top_repeated_clusters": (
                decision_no_pass_watch_validation.get("top_repeated_clusters") or []
            )[:10],
        },
        "pending_momentum_decay_recheck_validation": {
            "available": bool(pending_momentum_decay_validation),
            "classification": pending_momentum_decay_validation.get("classification"),
            "next_action": pending_momentum_decay_validation.get("next_action"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "denominator": pending_momentum_decay_validation.get("denominator") or {},
            "status_counts": pending_momentum_decay_validation.get("status_counts") or {},
            "current_momentum_decay_review": (
                pending_momentum_decay_validation.get("current_momentum_decay_review") or {}
            ),
            "oos_readiness_queue": (
                pending_momentum_decay_validation.get("oos_readiness_queue") or {}
            ),
            "top_repeated_probes": (
                pending_momentum_decay_validation.get("top_repeated_probes") or []
            )[:10],
        },
        "strategy_memory_ingestion_summary": {
            "available": bool(strategy_memory_ingestion.get("available")),
            "schema_version": strategy_memory_ingestion.get("schema_version"),
            "report_type": strategy_memory_ingestion.get("report_type"),
            "artifact_source_dir": strategy_memory_ingestion.get("artifact_source_dir"),
            "strategy_memory_hypotheses_count": strategy_memory_ingestion.get("strategy_memory_hypotheses_count", 0),
            "mapped_to_existing_candidates": strategy_memory_ingestion.get("mapped_to_existing_candidates", 0),
            "missing_shadow_candidates": strategy_memory_ingestion.get("missing_shadow_candidates", 0),
            "rejected_future_data_hypotheses": strategy_memory_ingestion.get("rejected_future_data_hypotheses", 0),
            "top_10_shadow_hypotheses": strategy_memory_ingestion.get("top_10_shadow_hypotheses") or [],
            "filtered_winner_count": strategy_memory_ingestion.get("filtered_winner_count", 0),
            "exit_policy_variants_tested": strategy_memory_ingestion.get("exit_policy_variants_tested", 0),
            "delay_replay_done": boolish(strategy_memory_ingestion.get("delay_replay_done")),
            "paper_trades_db_available": boolish(strategy_memory_ingestion.get("paper_trades_db_available")),
            "evidence_incomplete_hypotheses": strategy_memory_ingestion.get("evidence_incomplete_hypotheses", 0),
            "missing_shadow_candidate_handoffs": (
                strategy_memory_ingestion.get("missing_shadow_candidate_handoffs") or []
            )[:12],
            "exit_only_hypotheses": (
                strategy_memory_ingestion.get("exit_only_hypotheses") or []
            )[:12],
            "allowed_use": "shadow_only",
            "evidence_level": "historical_memory",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "candidate_catalog_change_allowed": False,
            "notes": strategy_memory_ingestion.get("notes") or [],
        },
        "strategy_memory": {
            "enabled": bool(strategy_memory_ingestion.get("available") or strategy_memory_validation.get("strategy_memory_enabled")),
            "hypotheses_count": (
                strategy_memory_validation.get("hypotheses_count")
                or strategy_memory_ingestion.get("strategy_memory_hypotheses_count", 0)
            ),
            "mapped_to_existing_candidates": strategy_memory_ingestion.get("mapped_to_existing_candidates", 0),
            "missing_shadow_candidates": strategy_memory_ingestion.get("missing_shadow_candidates", 0),
            "rejected_future_data_hypotheses": strategy_memory_ingestion.get("rejected_future_data_hypotheses", 0),
            "filtered_winner_count": (
                strategy_memory_filtered_bridge.get("filtered_winner_count")
                or strategy_memory_ingestion.get("filtered_winner_count", 0)
            ),
            "exit_policy_variants_tested": (
                strategy_memory_exit_summary.get("exit_policy_variants_tested")
                or strategy_memory_ingestion.get("exit_policy_variants_tested", 0)
            ),
            "delay_replay_done": boolish(
                strategy_memory_delay_summary.get("delay_replay_done")
                if strategy_memory_delay_summary
                else strategy_memory_ingestion.get("delay_replay_done")
            ),
            "paper_trades_db_available": boolish(strategy_memory_ingestion.get("paper_trades_db_available")),
            "evidence_role": "discovery_only",
            "allowed_use": "shadow_only",
            "evidence_level": "historical_memory",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "candidate_catalog_change_allowed": False,
            "validation_status_counts": strategy_memory_validation.get("status_counts") or {},
            "validation_window_count": len(strategy_memory_validation.get("window_validations") or []),
            "filtered_winner_bridge_available": bool(strategy_memory_filtered_bridge),
            "exit_shadow_summary_available": bool(strategy_memory_exit_summary),
            "delay_replay_summary_available": bool(strategy_memory_delay_summary),
            "top_10_shadow_hypotheses": strategy_memory_ingestion.get("top_10_shadow_hypotheses") or [],
            "missing_shadow_candidate_handoffs": (
                strategy_memory_ingestion.get("missing_shadow_candidate_handoffs") or []
            )[:12],
            "exit_only_hypotheses": (
                strategy_memory_exit_summary.get("exit_only_hypotheses")
                or strategy_memory_ingestion.get("exit_only_hypotheses")
                or []
            )[:12],
            "notes": [
                "Strategy Memory is discovery-only historical prior evidence.",
                "Historical memory cannot set promotion_allowed=true.",
                "Missing shadow candidates are handoff-only.",
            ],
        },
        "strategy_memory_validation": {
            "available": bool(strategy_memory_validation),
            "schema_version": strategy_memory_validation.get("schema_version"),
            "hypotheses_count": strategy_memory_validation.get("hypotheses_count", 0),
            "status_counts": strategy_memory_validation.get("status_counts") or {},
            "window_validations": strategy_memory_validation.get("window_validations") or [],
            "top_hypotheses": (strategy_memory_validation.get("hypotheses") or [])[:12],
            "promotion_allowed": False,
            "evidence_role": "discovery_only",
        },
        "strategy_memory_filtered_winner_bridge": {
            "available": bool(strategy_memory_filtered_bridge),
            "filtered_winner_count": strategy_memory_filtered_bridge.get("filtered_winner_count", 0),
            "final_blocker_counts": strategy_memory_filtered_bridge.get("final_blocker_counts") or {},
            "current_strategy_memory_hypothesis": (
                strategy_memory_filtered_bridge.get("current_strategy_memory_hypothesis") or {}
            ),
            "promotion_allowed": False,
            "evidence_role": "discovery_only",
        },
        "strategy_memory_exit_shadow_summary": {
            "available": bool(strategy_memory_exit_summary),
            "exit_policy_variants_tested": strategy_memory_exit_summary.get("exit_policy_variants_tested", 0),
            "sample_count": strategy_memory_exit_summary.get("sample_count"),
            "gold_silver_sample_count": strategy_memory_exit_summary.get("gold_silver_sample_count"),
            "top_variants": (strategy_memory_exit_summary.get("top_variants") or [])[:8],
            "exit_only_hypotheses": (strategy_memory_exit_summary.get("exit_only_hypotheses") or [])[:8],
            "promotion_allowed": False,
            "evidence_role": "exit_shadow_simulator_only",
        },
        "strategy_memory_delay_replay_summary": {
            "available": bool(strategy_memory_delay_summary),
            "delay_replay_done": boolish(strategy_memory_delay_summary.get("delay_replay_done")),
            "entry_delays_sec": strategy_memory_delay_summary.get("entry_delays_sec") or [],
            "execution_delay_hypotheses": (
                strategy_memory_delay_summary.get("execution_delay_hypotheses") or []
            )[:8],
            "hypotheses": (strategy_memory_delay_summary.get("hypotheses") or [])[:12],
            "promotion_allowed": False,
            "evidence_role": "delay_replay_only",
        },
        "A_CLASS_mode_status": readiness_reports.get("a_class_fastlane_mode_audit") or {},
        "final_entry_contract_blocker_breakdown": (
            (readiness_reports.get("a_class_fastlane_mode_audit") or {}).get("final_entry_contract_blocker_breakdown")
            or {}
        ),
        "per_candidate_effectiveness_summary": readiness_reports.get("candidate_effectiveness") or {},
        "candidate_improvement_opportunities_summary": readiness_reports.get("candidate_improvement_opportunities") or {},
        "Markov_effectiveness_summary": readiness_reports.get("markov_effectiveness") or {},
        "two_d_cross_validity_summary": readiness_reports.get("capture_cross_validity") or {},
        "next_highest_priority_blocker": next_highest_priority_blocker,
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
    assert verdict["quality_timing_reject_research_audit"]["available"] is False
    assert verdict["quality_timing_candidate_probe_validation"]["available"] is False
    assert verdict["strategy_memory_ingestion_summary"]["available"] is False
    assert verdict["strategy_memory_ingestion_summary"]["promotion_allowed"] is False
    env_commit_key = "ZEABUR_GIT_COMMIT_SHA"
    old_env_commit = os.environ.get(env_commit_key)
    os.environ[env_commit_key] = "env_commit_fixture"
    try:
        env_commit_verdict = build_verdict(capture, tests={"passed": True})
        assert env_commit_verdict["current_commit"] == "env_commit_fixture"
        assert env_commit_verdict["deployment_commit"] == "env_commit_fixture"
        explicit_commit_verdict = build_verdict(
            capture,
            tests={"passed": True},
            readiness_reports={
                "current_commit": "explicit_current_fixture",
                "deployment_commit": "explicit_deploy_fixture",
            },
        )
        assert explicit_commit_verdict["current_commit"] == "explicit_current_fixture"
        assert explicit_commit_verdict["deployment_commit"] == "explicit_deploy_fixture"
    finally:
        if old_env_commit is None:
            os.environ.pop(env_commit_key, None)
        else:
            os.environ[env_commit_key] = old_env_commit
    stage_verdict = build_verdict(capture, tests={"passed": True}, readiness_reports={
        "a_class_fastlane_mode_audit": {
            "final_entry_status": "FUNNEL_BLOCKED_EXPECTED",
            "reason": "cooldown_elapsed_requires_clean_windows",
            "current_capture_stage": "mode_disabled_clean_window_pending",
            "stage2_flat_summary": {
                "mode_status": "SHADOW",
                "mode_action": "SHADOW",
                "mode_reason": "cooldown_elapsed_requires_clean_windows",
                "shadow_entered_ts": 1234,
                "shadow_entered_ts_source": "last_breach_ts",
                "cooldown_elapsed": True,
                "cooldown_remaining_sec": 0,
                "clean_window_required_conditions": [
                    {"condition": "context_coverage_clean_window", "clean_windows_required": 4, "passed": False}
                ],
                "clean_window_passed_conditions": [],
                "clean_window_failed_conditions": [
                    {"condition": "source_quote_clean_coverage_below_80pct", "source": "context_coverage_audit"}
                ],
                "raw_gs_events": 10,
                "raw_gs_signal_ids": 10,
                "candidate_matched_any": 10,
                "has_decision_record": 9,
                "pass_allow": 5,
                "pending_entry": 3,
                "reached_final_entry_contract": 1,
                "final_entry_block_mode_disabled": 1,
                "final_entry_block_mode_disabled_only": 1,
                "final_entry_block_expected_rr": 0,
                "final_entry_block_spread": 0,
                "paper_trade_intent": 0,
                "paper_trade_committed": 0,
                "entered": 0,
                "realized": 0,
                "promotion_allowed": False,
                "paper_enablement_allowed": False,
                "automatic_runtime_change_allowed": False,
                "strategy_change_allowed": False,
            },
            "capture_stage_rates": {
                "detector_capture_rate": 1.0,
                "decision_capture_rate": 0.9,
                "decision_record_capture_rate": 0.9,
                "pass_allow_capture_rate": 0.5,
                "pending_capture_rate": 0.3,
                "final_entry_contract_reach_rate": 0.02,
                "paper_trade_intent_rate": 0.0,
                "paper_capture_rate": 0.0,
                "upstream_funnel_gap": {
                    "raw_signal_ids": 10,
                    "decision_record_signal_ids": 9,
                    "pass_or_allow_signal_ids": 5,
                    "pending_entry_signal_ids": 3,
                    "no_decision_record": 1,
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
            "readiness_shortfall_summary": {
                "target_count_60pct": 6,
                "current_mode_disabled_adjusted_final_eligibility_count": 1,
                "shortfall_to_60_final_eligibility": 5,
                "current_paper_trade_intent_count": 0,
                "current_paper_committed_count": 0,
            },
            "paper_entry_proposal_readiness": {
                "status": "NOT_READY_FOR_PAPER_ENTRY_PROPOSAL",
                "blocking_reasons": [
                    "mode_disabled_adjusted_final_eligibility_below_60pct",
                    "paper_trade_entry_intent_zero",
                    "paper_trade_committed_zero",
                ],
                "promotion_allowed": False,
                "paper_enablement_allowed": False,
            },
        },
        "capture_60_gap_report": {
            "target_capture_rate": 0.6,
            "raw_gold_silver_denominator": 10,
            "target_60_count": 6,
            "biggest_gap_stage": "pass_allow_capture",
            "target_shortfall_stage": "pass_allow_capture",
            "additional_count_needed_to_60": 1,
            "next_best_allowed_action": (
                "audit_decision_bridge_and_quality_timing_shadow_only_with_blocked_context_dimensions_excluded"
            ),
            "current_target_stage": "mode_disabled_adjusted_final_eligibility",
            "current_target_count": 1,
            "current_target_rate": 0.1,
            "current_target_additional_count_needed_to_60": 5,
            "current_target_reached": False,
            "current_target_next_best_allowed_action": (
                "audit_pending_to_final_entry_stale_before_final_shadow_only_with_blocked_context_dimensions_excluded"
            ),
            "mode_disabled_adjusted_final_eligibility_rate": 0.1,
        },
        "shadow_decision_bridge_audit": {
            "status": "SHADOW_DECISION_BRIDGE_MIRROR_COMPLETE",
            "denominator": {
                "shadow_entry_hypotheses_matched_no_decision_bridge": 1,
                "mirror_event_count": 1,
                "mirror_event_coverage_vs_shadow_bridge_gap": 1.0,
                "mirror_event_truncated": False,
            },
            "family_counts": [{"family": "base", "count": 1}],
            "candidate_counts": [
                {"candidate_id": "notath_quote_clean", "family": "base", "count": 1}
            ],
            "reason_counts": [
                {
                    "candidate_id": "notath_quote_clean",
                    "family": "base",
                    "reason": "runtime_source_quote_clean",
                    "count": 1,
                }
            ],
        },
    })
    assert stage_verdict["current_capture_stage"] == "mode_disabled_clean_window_pending"
    assert stage_verdict["mode_status"] == "SHADOW"
    assert stage_verdict["mode_action"] == "SHADOW"
    assert stage_verdict["mode_reason"] == "cooldown_elapsed_requires_clean_windows"
    assert stage_verdict["shadow_entered_ts"] == 1234
    assert stage_verdict["cooldown_elapsed"] is True
    assert stage_verdict["clean_window_failed_conditions"][0]["condition"] == "source_quote_clean_coverage_below_80pct"
    assert stage_verdict["raw_gs_events"] == 10
    assert stage_verdict["candidate_matched_any"] == 10
    assert stage_verdict["has_decision_record"] == 9
    assert stage_verdict["pass_allow"] == 5
    assert stage_verdict["pending_entry"] == 3
    assert stage_verdict["reached_final_entry_contract"] == 1
    assert stage_verdict["final_entry_block_mode_disabled"] == 1
    assert stage_verdict["paper_trade_intent"] == 0
    assert stage_verdict["paper_trade_committed"] == 0
    assert stage_verdict["stage2_entry_funnel_summary"]["paper_enablement_allowed"] is False
    assert stage_verdict["detector_capture_rate"] == 1.0
    assert stage_verdict["decision_capture_rate"] == 0.9
    assert stage_verdict["pass_allow_capture_rate"] == 0.5
    assert stage_verdict["pending_capture_rate"] == 0.3
    assert stage_verdict["final_eligibility_capture_rate"] == 0.01
    assert stage_verdict["paper_trade_intent_rate"] == 0.0
    assert stage_verdict["paper_capture_rate"] == 0.0
    assert stage_verdict["capture_60_biggest_gap_stage"] == "pass_allow_capture"
    assert stage_verdict["capture_60_target_shortfall_stage"] == "pass_allow_capture"
    assert stage_verdict["capture_60_additional_count_needed"] == 1
    assert stage_verdict["capture_60_current_target_stage"] == "mode_disabled_adjusted_final_eligibility"
    assert stage_verdict["capture_60_current_target_count"] == 1
    assert stage_verdict["capture_60_current_target_rate"] == 0.1
    assert stage_verdict["capture_60_current_target_additional_count_needed"] == 5
    assert stage_verdict["capture_60_next_best_allowed_action"] == (
        "audit_decision_bridge_and_quality_timing_shadow_only_with_blocked_context_dimensions_excluded"
    )
    assert stage_verdict["capture_60_target_loop"]["current_target_next_best_allowed_action"] == (
        "audit_pending_to_final_entry_stale_before_final_shadow_only_with_blocked_context_dimensions_excluded"
    )
    assert stage_verdict["readiness_shortfall_summary"]["shortfall_to_60_final_eligibility"] == 5
    assert stage_verdict["paper_entry_proposal_readiness"]["status"] == "NOT_READY_FOR_PAPER_ENTRY_PROPOSAL"
    assert stage_verdict["upstream_funnel_gap_summary"]["total_upstream_gap"] == 7
    assert stage_verdict["upstream_funnel_gap_summary"]["no_decision_candidate_shadow_observed_no_decision_event"] == 1
    assert stage_verdict["upstream_funnel_gap_summary"]["no_decision_record_subroot_cause_counts"][0]["root_cause"] == "shadow_entry_hypotheses_matched_no_decision_bridge"
    assert stage_verdict["upstream_funnel_gap_summary"]["shadow_no_decision_entry_hypothesis_candidate_counts"][0]["candidate_id"] == "notath_quote_clean"
    shadow_bridge_summary = stage_verdict["shadow_decision_bridge_audit_summary"]
    assert shadow_bridge_summary["mirror_complete"] is True
    assert shadow_bridge_summary["top_candidate_counts"][0]["candidate_id"] == "notath_quote_clean"
    assert shadow_bridge_summary["review_queue"]["classification"] == "SHADOW_DECISION_BRIDGE_REVIEW_QUEUE_READY"
    assert shadow_bridge_summary["review_queue"]["automatic_runtime_change_allowed"] is False
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
    volume_blocked_by_context_eligibility = build_verdict({
        **capture,
        "report_health": {"promotion_blockers": []},
    }, tests={"passed": True}, readiness_reports={
        "context_dimension_eligibility": {
            "dimensions": {
                "volume": {
                    "status": "BLOCKED_UNKNOWN",
                    "coverage_rate": 0.3,
                    "blockers": ["volume_profile_coverage_below_80pct"],
                    "eligible_for_capture_cross": False,
                },
                "kline": {
                    "status": "BLOCKED_UNKNOWN",
                    "coverage_rate": 0.46,
                    "blockers": ["kline_coverage_below_80pct"],
                    "eligible_for_capture_cross": False,
                },
            },
            "blocked_dimensions": ["volume", "kline"],
            "clean_dimensions": ["quote-sensitive", "source_component", "lifecycle"],
        },
    })
    assert volume_blocked_by_context_eligibility["classification"] == "BLOCKED_CONTEXT_COVERAGE"
    assert volume_blocked_by_context_eligibility["formal_volume_sensitive_slices_blocked"] is True
    assert "volume_profile_coverage_below_80pct" in volume_blocked_by_context_eligibility["blockers"]
    assert "kline_coverage_below_80pct" in volume_blocked_by_context_eligibility["blockers"]
    kline_resolution_verdict = build_verdict({
        **capture,
        "report_health": {"promotion_blockers": ["kline_coverage_below_80pct"]},
    }, tests={"passed": True}, readiness_reports={
        "kline_coverage_resolution_audit": {
            "overall": {
                "classification": "KLINE_FORMAL_BLOCKED_RESEARCH_RECOVERABLE",
                "next_action": "audit_low_confidence_baseline_lag_time_legality_without_changing_formal_denominator",
                "promotion_allowed": False,
            },
            "formal_kline_coverage": {"formal_coverage_rate": 0.46},
            "research_recoverability": {
                "confidence_adjusted_research_kline_coverage_rate": 0.83,
            },
            "allowed_resolution_tracks": [{"track": "low_confidence_time_legal_research"}],
        },
    })
    assert kline_resolution_verdict["kline_coverage_resolution_audit"]["available"] is True
    assert kline_resolution_verdict["kline_coverage_resolution_audit"]["promotion_allowed"] is False
    assert (
        kline_resolution_verdict["kline_coverage_resolution_audit"]["overall"]["classification"]
        == "KLINE_FORMAL_BLOCKED_RESEARCH_RECOVERABLE"
    )
    assert kline_resolution_verdict["classification"] == "BLOCKED_CONTEXT_COVERAGE"
    assert kline_resolution_verdict["next_action"] == (
        "audit_low_confidence_baseline_lag_time_legality_without_changing_formal_denominator"
    )
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
    assert monitor_reconciled_quote_blocked["quote_writer_fix_status"] == "VERIFIED_POST_DEPLOY"
    assert monitor_reconciled_quote_blocked["quote_clean_window_status"] == "QUOTE_CLEAN_WINDOW_PENDING"
    quote_pending_with_volume = build_verdict({
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
        "report_health": {"promotion_blockers": ["volume_profile_coverage_below_80pct"]},
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
    assert quote_pending_with_volume["blocked_subtype"] == "CLEAN_V2_WINDOW_PENDING"
    assert "source_quote_clean_coverage_below_80pct" in quote_pending_with_volume["blockers"]
    assert "source_quote_clean_coverage_below_80pct" not in quote_pending_with_volume["actionable_blockers"]
    assert quote_pending_with_volume["next_highest_priority_blocker"] == "volume_profile_coverage_below_80pct"
    quote_pending_with_matured_volume_path = build_verdict({
        **capture,
        "quote_context_coverage": {
            **capture["quote_context_coverage"],
            "source_quote_clean_present_rate": 0.6,
            "source_quote_executable_present_rate": 0.6,
        },
        "report_health": {"promotion_blockers": ["volume_profile_coverage_below_80pct"]},
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
        },
        "matured_kline_volume_recheck_audit": {
            "recheck": {
                "recoverable_known_rate": 0.98,
            },
        },
        "matured_volume_capture_cross_audit": {
            "matured_volume_context": {
                "known_rate": 0.91,
            },
            "signal_id_reconciliation": {
                "raw_all_gold_silver": {
                    "joined_event_rate": 0.32,
                    "unjoined_event_rows": 93,
                    "unjoined_reason_counts": {
                        "outside_candidate_observer_window_before": 93,
                    },
                },
                "evaluable_gold_silver": {
                    "joined_event_rate": 0.37,
                    "unjoined_event_rows": 37,
                    "unjoined_reason_counts": {
                        "outside_candidate_observer_window_before": 37,
                    },
                },
            },
        },
    })
    assert quote_pending_with_matured_volume_path["classification"] == "BLOCKED_CONTEXT_COVERAGE"
    assert "volume_profile_coverage_below_80pct" in quote_pending_with_matured_volume_path["blockers"]
    assert "volume_profile_coverage_below_80pct" not in quote_pending_with_matured_volume_path["actionable_blockers"]
    assert quote_pending_with_matured_volume_path["next_highest_priority_blocker"] is None
    assert quote_pending_with_matured_volume_path["top_actionable_blocker"] is None
    assert quote_pending_with_matured_volume_path["top_formal_blocker"] == "volume_profile_coverage_below_80pct"
    assert quote_pending_with_matured_volume_path["shadow_matured_volume_slices_evaluable"] is True
    assert quote_pending_with_matured_volume_path["formal_volume_sensitive_slices_blocked"] is True
    assert quote_pending_with_matured_volume_path["volume_profile_blocker_state"]["classification"] == "SHADOW_MATURED_VOLUME_PATH_AVAILABLE"
    assert quote_pending_with_matured_volume_path["volume_profile_blocker_state"]["promotion_allowed"] is False
    assert quote_pending_with_matured_volume_path["context_clean_window_pending"] is True
    assert quote_pending_with_matured_volume_path["next_action"] == "wait_for_context_clean_window_and_continue_shadow_oos_collection"
    matured_volume_coverage_pending = build_verdict({
        **capture,
        "report_health": {"promotion_blockers": ["volume_profile_coverage_below_80pct"]},
    }, tests={"passed": True}, readiness_reports={
        "matured_volume_capture_cross_audit": {
            "overall": {
                "classification": "BLOCKED_MATURED_VOLUME_COVERAGE",
                "next_action": "continue_matured_volume_recheck_before_evaluating_volume_slices",
                "promotion_allowed": False,
            },
            "matured_volume_context": {
                "kline_cache_available": True,
                "known_rate": 0.424337,
                "known_rows": 272,
                "unknown_rows": 369,
            },
            "signal_id_reconciliation": {
                "raw_all_gold_silver": {
                    "joined_event_rate": 1.0,
                    "unjoined_event_rows": 0,
                    "unjoined_reason_counts": {},
                },
                "evaluable_gold_silver": {
                    "joined_event_rate": 1.0,
                    "unjoined_event_rows": 0,
                    "unjoined_reason_counts": {},
                },
            },
        },
    })
    assert matured_volume_coverage_pending["classification"] == "BLOCKED_CONTEXT_COVERAGE"
    assert "volume_profile_coverage_below_80pct" in matured_volume_coverage_pending["blockers"]
    assert "volume_profile_coverage_below_80pct" not in matured_volume_coverage_pending["actionable_blockers"]
    assert matured_volume_coverage_pending["next_highest_priority_blocker"] is None
    assert matured_volume_coverage_pending["top_actionable_blocker"] is None
    assert matured_volume_coverage_pending["top_formal_blocker"] == "volume_profile_coverage_below_80pct"
    assert matured_volume_coverage_pending["shadow_matured_volume_slices_evaluable"] is False
    assert matured_volume_coverage_pending["volume_data_coverage_pending"] is True
    assert matured_volume_coverage_pending["volume_profile_blocker_state"]["classification"] == "MATURED_VOLUME_COVERAGE_PENDING"
    assert matured_volume_coverage_pending["volume_profile_blocker_state"]["scope_joined_99"] is True
    assert matured_volume_coverage_pending["next_action"] == "continue_matured_volume_recheck_before_evaluating_volume_slices"
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
    lifecycle_pending = build_verdict({
        **capture,
        "report_health": {"promotion_blockers": ["lifecycle_profile_coverage_below_80pct"]},
    }, tests={"passed": True}, readiness_reports={
        "context_blocker_monitor": {
            "overall_verdict": {
                "context_field_writer_fix": "VERIFIED_POST_DEPLOY",
            },
            "task_e_post_deploy_context_field_smoke_test": {
                "classification": "VERIFIED_POST_DEPLOY",
                "post_deploy_context_fields_healthy": True,
            },
            "task_f_recent_context_writer_health": {
                "classification": "RECENT_CONTEXT_WRITER_HEALTHY",
                "inferred_clean_window_state": "writer_healthy_old_rows_remaining",
                "newest_age_sec": 12,
                "fields": {
                    "lifecycle_profile": {"effective_present_rate": 1.0},
                    "source_component": {"effective_present_rate": 1.0},
                    "markov_bucket": {"effective_present_rate": 1.0},
                    "volume_profile": {"effective_present_rate": 0.4},
                },
            },
            "task_d_context_field_coverage_audit": {
                "lifecycle_profile": {
                    "denominator_rows": 10,
                    "effective_present_rate": 0.7,
                    "rows_needed_to_80pct": 1,
                    "mature_context": {
                        "denominator_rows": 5,
                        "effective_present_rate": 0.6,
                        "rows_needed_to_80pct": 1,
                    },
                },
                "source_component": {
                    "denominator_rows": 10,
                    "effective_present_rate": 1.0,
                    "rows_needed_to_80pct": 0,
                },
            },
        }
    })
    assert "lifecycle_profile_coverage_below_80pct" in lifecycle_pending["blockers"]
    assert "lifecycle_profile_coverage_below_80pct" not in lifecycle_pending["actionable_blockers"]
    assert lifecycle_pending["lifecycle_clean_window_pending"] is True
    assert lifecycle_pending["context_field_writer_fix_status"] == "VERIFIED_POST_DEPLOY"
    assert lifecycle_pending["top_blocker"] == "context_clean_window_pending"
    assert lifecycle_pending["top_formal_blocker"] == "lifecycle_profile_coverage_below_80pct"
    context_progress = lifecycle_pending["context_clean_window_progress"]
    assert context_progress["classification"] == "CONTEXT_CLEAN_WINDOW_PENDING"
    assert context_progress["waiting_fields"] == ["lifecycle_profile"]
    assert context_progress["fields"]["lifecycle_profile"]["rows_needed_to_80pct"] == 1
    assert context_progress["writer_status"]["recent_context_writer_health_classification"] == "RECENT_CONTEXT_WRITER_HEALTHY"
    assert context_progress["writer_status"]["recent_context_writer_inferred_clean_window_state"] == "writer_healthy_old_rows_remaining"
    assert context_progress["recent_context_writer_health"]["newest_age_sec"] == 12
    assert context_progress["automatic_runtime_change_allowed"] is False
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
    assert matured_volume["review_queue"]["classification"] == "MATURED_VOLUME_WATCH_QUEUE_READY"
    assert matured_volume["review_queue"]["items"][0]["candidate_id"] == "entry_mode_registry:ath_flat_structure_tiny_scout"
    assert matured_volume["review_queue"]["items"][0]["automatic_runtime_change_allowed"] is False
    assert matured_volume_verdict["matured_volume_watch_queue"]["queue_count"] == 1
    quality_timing_verdict = build_verdict(capture, tests={"passed": True}, readiness_reports={
        "quality_timing_reject_research_audit": {
            "verdict": "QUALITY_TIMING_REJECT_RESEARCH_READY",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "denominator": {
                "quality_timing_reject_event_rows": 4,
                "quality_timing_reject_share_of_raw_all": 0.2,
            },
            "candidate_match_attribution": {
                "full_candidate_coverage_rate": 1.0,
                "candidate_matched_any_events": 4,
                "candidate_matched_any_rate": 1.0,
                "top_candidates": [
                    {"candidate_id": "kline:active_mom20_first3", "family": "kline", "count": 3}
                ],
                "top_families": [{"family": "kline", "count": 3}],
            },
            "stage_attribution": {
                "stage_counts": [{"stage": "decision_no_pass_or_allow", "count": 4}],
                "reason_counts": [
                    {
                        "stage": "decision_no_pass_or_allow",
                        "component": "smart_entry",
                        "event_type": "quality_gate",
                        "decision": "REJECT",
                        "reason": "quality_score_low",
                        "count": 4,
                    }
                ],
            },
            "context_attribution": {
                "lifecycle_source_counts": [
                    {"lifecycle_profile": "ATH_SHALLOW_PULLBACK:OBSERVE", "source_component": "matrix_evaluator", "count": 4}
                ],
                "markov_bucket_counts": [{"markov_bucket": "insufficient", "count": 4}],
            },
            "shadow_only_review": {
                "classification": "QUALITY_TIMING_SHADOW_REVIEW_READY",
                "dominant_cluster": "score_or_quality_too_low",
                "dominant_stage": "decision_no_pass_or_allow",
                "quality_timing_false_negative_upper_bound": {
                    "event_count": 4,
                    "raw_all_gold_silver_event_rows": 20,
                    "rate": 0.2,
                },
                "research_opportunity_count": 1,
                "top_research_opportunities": [
                    {
                        "cluster": "score_or_quality_too_low",
                        "event_count": 4,
                        "share_of_quality_timing_rejects": 1.0,
                        "share_of_raw_all_gold_silver": 0.2,
                        "unique_tokens": 3,
                        "stage_counts": [{"stage": "decision_no_pass_or_allow", "count": 4}],
                        "top_candidates": [
                            {"candidate_id": "kline:active_mom20_first3", "family": "kline", "count": 3}
                        ],
                        "top_lifecycle_source_contexts": [
                            {
                                "lifecycle_profile": "ATH_SHALLOW_PULLBACK:OBSERVE",
                                "source_component": "matrix_evaluator",
                                "count": 4,
                            }
                        ],
                        "suggested_shadow_only_action": "track_score_quality_threshold_false_negative_shadow_probe",
                        "promotion_allowed": False,
                        "strategy_change_allowed": False,
                    }
                ],
            },
        }
    })
    qt = quality_timing_verdict["quality_timing_reject_research_audit"]
    assert qt["available"] is True
    assert qt["promotion_allowed"] is False
    assert qt["candidate_match_attribution"]["top_candidates"][0]["candidate_id"] == "kline:active_mom20_first3"
    assert qt["shadow_only_review"]["dominant_cluster"] == "score_or_quality_too_low"
    assert qt["shadow_only_review"]["top_research_opportunities"][0]["promotion_allowed"] is False
    assert qt["review_queue"]["classification"] == "QUALITY_TIMING_SHADOW_REVIEW_QUEUE_READY"
    assert qt["review_queue"]["items"][0]["cluster"] == "score_or_quality_too_low"
    assert qt["review_queue"]["items"][0]["automatic_runtime_change_allowed"] is False
    assert quality_timing_verdict["quality_timing_shadow_review_queue"]["queue_count"] == 1
    quality_timing_probe_verdict = build_verdict(capture, tests={"passed": True}, readiness_reports={
        "quality_timing_candidate_probe_validation": {
            "classification": "QUALITY_TIMING_PROBES_REPEATED_SAME_WINDOW",
            "next_action": "continue_shadow_probe_tracking_until_clean_window_then_oos",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "denominator": {
                "registered_probe_count": 2,
                "validated_probe_count": 2,
                "repeated_probe_count": 1,
                "repeated_probe_rate": 0.5,
            },
            "status_counts": {"REPEATED_SHADOW_PROBE": 1},
            "oos_readiness_queue": {
                "classification": "QUALITY_TIMING_OOS_QUEUE_PENDING_CLEAN_WINDOW",
                "queue_count": 1,
                "promotion_allowed": False,
                "automatic_runtime_change_allowed": False,
                "items": [
                    {
                        "hypothesis_id": "quality_timing_probe:matrix_alignment_wait:entry_mode_registry_smart_entry_pullback_bounce",
                        "status": "PENDING_CLEAN_WINDOW_THEN_OOS",
                        "promotion_allowed": False,
                    }
                ],
            },
            "top_repeated_probes": [
                {
                    "hypothesis_id": "quality_timing_probe:matrix_alignment_wait:entry_mode_registry_smart_entry_pullback_bounce",
                    "status": "REPEATED_SHADOW_PROBE",
                    "promotion_allowed": False,
                }
            ],
        }
    })
    qtp = quality_timing_probe_verdict["quality_timing_candidate_probe_validation"]
    assert qtp["available"] is True
    assert qtp["classification"] == "QUALITY_TIMING_PROBES_REPEATED_SAME_WINDOW"
    assert qtp["promotion_allowed"] is False
    assert qtp["oos_readiness_queue"]["classification"] == "QUALITY_TIMING_OOS_QUEUE_PENDING_CLEAN_WINDOW"
    assert qtp["oos_readiness_queue"]["automatic_runtime_change_allowed"] is False
    assert qtp["top_repeated_probes"][0]["status"] == "REPEATED_SHADOW_PROBE"
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
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        capture_path = root / "capture_discovery_24h.json"
        write_json(capture_path, capture)
        write_json(root / "candidate_effectiveness_24h.json", {
            "schema_version": "candidate_effectiveness_report.v1",
            "candidate_count": 84,
            "classification_counts": {"potential_entry_hypothesis": 2},
        })
        write_json(root / "candidate_improvement_opportunities_24h.json", {
            "schema_version": "candidate_improvement_opportunities.v1",
            "opportunity_count": 2,
            "promotion_allowed": False,
        })
        write_json(root / "markov_effectiveness_24h.json", {
            "schema_version": "markov_effectiveness_report.v1",
            "status": "insufficient_or_uninformative",
        })
        write_json(root / "capture_cross_validity_24h.json", {
            "schema_version": "capture_cross_validity_report.v1",
            "valid_cross_count": 3,
        })
        write_json(root / "shadow_decision_bridge_audit_24h.json", {
            "schema_version": "shadow_decision_bridge_audit.v1",
            "status": "SHADOW_DECISION_BRIDGE_MIRROR_COMPLETE",
            "denominator": {
                "shadow_entry_hypotheses_matched_no_decision_bridge": 2,
                "mirror_event_count": 2,
                "mirror_event_coverage_vs_shadow_bridge_gap": 1.0,
                "mirror_event_truncated": False,
            },
            "candidate_counts": [
                {"candidate_id": "notath_quote_clean", "family": "base", "count": 2}
            ],
        })
        write_json(root / "hypothesis_validation_audit_oos_probe_0p1h.json", {
            "schema_version": "hypothesis_validation_audit.v1",
            "overall": {
                "classification": "OOS_WINDOW_TOO_SMALL_CONTINUE_WAIT",
                "next_action": "continue_collecting_post_freeze_window_before_judging_oos_repeat",
                "promotion_allowed": False,
            },
            "matured_volume_hypothesis_validation": {
                "eval_window_quality": {
                    "sufficient_for_oos_judgment": False,
                    "signals_scanned": 12,
                    "evaluable_raw_gs_event_rows": 0,
                    "matured_volume_known_rate": 0.416667,
                    "blockers": [
                        "oos_signal_count_below_min",
                        "oos_raw_gs_event_count_below_min",
                    ],
                },
                "oos_repeated_watch_count": 0,
            },
            "promotion_allowed": False,
        })
        write_json(root / "matured_volume_capture_cross_audit_oos_probe_0p1h.json", {
            "schema_version": "matured_volume_capture_cross_audit.v1",
            "overall": {
                "classification": "BLOCKED_MATURED_VOLUME_COVERAGE",
                "promotion_allowed": False,
            },
            "matured_volume_context": {
                "known_rate": 0.416667,
            },
            "promotion_allowed": False,
        })
        write_json(root / "oos_readiness_probe_refresh.json", {
            "schema_version": "refresh_oos_readiness_probes.v1",
            "classification": "OOS_PROBES_REFRESHED",
            "generated_at": "2026-06-30T00:00:00Z",
            "failed_command_count": 0,
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "probes": [
                {
                    "probe": "0p1h",
                    "cross": {
                        "classification": "BLOCKED_MATURED_VOLUME_COVERAGE",
                        "signals_scanned": 12,
                        "evaluable_raw_gs_event_rows": 0,
                        "matured_volume_known_rate": 0.416667,
                    },
                    "validation": {
                        "classification": "OOS_WINDOW_TOO_SMALL_CONTINUE_WAIT",
                        "signals_scanned": 12,
                        "evaluable_raw_gs_event_rows": 0,
                        "matured_volume_known_rate": 0.416667,
                        "sufficient_for_oos_judgment": False,
                        "blockers": ["oos_raw_gs_event_count_below_min"],
                    },
                },
                {
                    "probe": "0p333h",
                    "cross": {
                        "classification": "BLOCKED_MATURED_VOLUME_COVERAGE",
                        "signals_scanned": 18,
                        "evaluable_raw_gs_event_rows": 0,
                        "matured_volume_known_rate": 0.5,
                    },
                    "validation": {
                        "classification": "OOS_WINDOW_TOO_SMALL_CONTINUE_WAIT",
                        "signals_scanned": 18,
                        "evaluable_raw_gs_event_rows": 0,
                        "matured_volume_known_rate": 0.5,
                        "sufficient_for_oos_judgment": False,
                        "blockers": ["oos_raw_gs_event_count_below_min"],
                        "registry_frozen_before_eval_window": True,
                    },
                }
            ],
        })
        siblings = load_sibling_readiness_reports(str(capture_path))
        assert siblings["candidate_effectiveness"]["candidate_count"] == 84
        assert siblings["candidate_improvement_opportunities"]["opportunity_count"] == 2
        assert siblings["markov_effectiveness"]["status"] == "insufficient_or_uninformative"
        assert siblings["capture_cross_validity"]["valid_cross_count"] == 3
        assert siblings["shadow_decision_bridge_audit"]["status"] == "SHADOW_DECISION_BRIDGE_MIRROR_COMPLETE"
        assert siblings["hypothesis_validation_oos_probe_0p1h"]["overall"]["promotion_allowed"] is False
        assert siblings["matured_volume_cross_oos_probe_0p1h"]["overall"]["classification"] == "BLOCKED_MATURED_VOLUME_COVERAGE"
        assert siblings["oos_readiness_probe_refresh"]["classification"] == "OOS_PROBES_REFRESHED"
        explicit = load_sibling_readiness_reports(str(capture_path), {
            "candidate_effectiveness": {"candidate_count": 1}
        })
        assert explicit["candidate_effectiveness"]["candidate_count"] == 1
        sibling_verdict = build_verdict(capture, tests={"passed": True}, readiness_reports=siblings)
        assert sibling_verdict["per_candidate_effectiveness_summary"]["candidate_count"] == 84
        assert sibling_verdict["candidate_improvement_opportunities_summary"]["opportunity_count"] == 2
        assert sibling_verdict["Markov_effectiveness_summary"]["status"] == "insufficient_or_uninformative"
        assert sibling_verdict["two_d_cross_validity_summary"]["valid_cross_count"] == 3
        assert sibling_verdict["shadow_decision_bridge_audit_summary"]["review_queue"]["classification"] == "SHADOW_DECISION_BRIDGE_REVIEW_QUEUE_READY"
        assert sibling_verdict["oos_readiness_summary"]["classification"] == "OOS_WINDOW_TOO_SMALL_OR_CONTEXT_BLOCKED"
        assert sibling_verdict["oos_readiness_summary"]["available_probe_count"] == 2
        assert sibling_verdict["oos_readiness_summary"]["sufficient_probe_count"] == 0
        assert sibling_verdict["oos_readiness_summary"]["promotion_allowed"] is False
        assert sibling_verdict["oos_readiness_summary"]["probes"][-1]["probe"] == "0p333h"
        assert sibling_verdict["oos_readiness_summary"]["probes"][-1]["source"] == "oos_readiness_probe_refresh"
        assert sibling_verdict["oos_readiness_summary_v3"]["available"] is True
        assert sibling_verdict["oos_readiness_summary_v3"]["classification"] == "OOS_WINDOW_TOO_SMALL_OR_CONTEXT_BLOCKED"
        assert sibling_verdict["oos_readiness_summary_v3"]["available_probe_count"] == 2
        assert sibling_verdict["oos_probe_refresh_status"]["available"] is True
        assert sibling_verdict["oos_probe_refresh_status"]["failed_command_count"] == 0
        assert sibling_verdict["oos_probe_refresh_status"]["probes"][0]["probe"] == "0p1h"
        write_json(root / "oos_readiness_probe_refresh.json", {
            "schema_version": "refresh_oos_readiness_probes.v1",
            "classification": "OOS_PROBES_WAITING_FOR_POST_FREEZE_WINDOW",
            "generated_at": "2026-06-30T00:05:00Z",
            "failed_command_count": 0,
            "requested_probe_count": 0,
            "executed_probe_count": 0,
            "probe_count": 0,
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "post_freeze_probe": {
                "reason": "post_freeze_window_too_young",
                "hours": 0.0,
            },
            "probes": [],
        })
        waiting_siblings = load_sibling_readiness_reports(str(capture_path))
        waiting_verdict = build_verdict(capture, tests={"passed": True}, readiness_reports=waiting_siblings)
        waiting_oos = waiting_verdict["oos_readiness_summary"]
        assert waiting_oos["available_probe_count"] == 0
        assert waiting_oos["current_oos_probe_count"] == 0
        assert waiting_oos["stale_oos_probe_files_ignored_count"] == 1
        assert waiting_oos["oos_probe_refresh_classification"] == "OOS_PROBES_WAITING_FOR_POST_FREEZE_WINDOW"
        assert waiting_oos["oos_probe_refresh_executed_probe_count"] == 0
        assert waiting_oos["oos_probe_refresh_post_freeze_reason"] == "post_freeze_window_too_young"
        assert waiting_verdict["oos_probe_refresh_status"]["classification"] == "OOS_PROBES_WAITING_FOR_POST_FREEZE_WINDOW"
        assert waiting_verdict["oos_probe_refresh_status"]["executed_probe_count"] == 0
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
    readiness_reports = load_sibling_readiness_reports(args.capture, readiness_reports)
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
