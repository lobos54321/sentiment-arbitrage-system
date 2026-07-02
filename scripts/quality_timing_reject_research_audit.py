#!/usr/bin/env python3
"""Read-only quality/timing reject research audit.

This report inspects raw gold/silver events that were blocked by quality or
timing logic before final_entry_contract. It is discovery/readiness evidence
only: it never changes strategy, gates, final_entry_contract, A_CLASS mode,
executor, wallet, canary, or risk settings.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path
from statistics import median

from a_class_fastlane_mode_readiness_audit import (
    classify_pending_gap_reason,
    classify_upstream_gap_reason,
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
    pct,
    rate,
    safe_float,
    signal_id_key,
    table_exists,
)


SCHEMA_VERSION = "quality_timing_reject_research_audit.v2"
EVIDENCE_LEVEL = "discovery_same_window"
DEFAULT_EXPECTED_CANDIDATES = 84
READINESS_TARGET_RATE = 0.6
BLOCKED_CONTEXT_DIMENSIONS = ["kline", "volume"]


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def jdump(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def decision_value(row, key, default=None):
    try:
        if isinstance(row, dict):
            return row.get(key, default)
        return row[key] if key in row.keys() else default
    except Exception:
        return default


def reason_row(row):
    if row is None:
        return {
            "component": "UNKNOWN",
            "event_type": "missing_decision_event",
            "decision": "UNKNOWN",
            "reason": "missing_decision_event",
        }
    return {
        "component": decision_value(row, "source_component")
        or decision_value(row, "component")
        or "UNKNOWN",
        "event_type": decision_value(row, "event_type") or "UNKNOWN",
        "decision": decision_value(row, "decision") or decision_value(row, "action") or "UNKNOWN",
        "reason": decision_value(row, "reason")
        or decision_value(row, "block_cause")
        or decision_value(row, "quote_failure_reason")
        or "UNKNOWN",
    }


def row_ts(row):
    return safe_float(decision_value(row, "event_ts_norm") or decision_value(row, "event_ts")) or 0


def choose_decision_no_pass_row(signal_rows):
    sorted_rows = sorted(signal_rows or [], key=row_ts)
    terminal_rows = [
        row
        for row in sorted_rows
        if str(decision_value(row, "event_type") or "").lower()
        in {"entry_block", "probe_reject", "quality_gate", "timing_decision", "entry_abort"}
        or str(decision_value(row, "decision") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
        or str(decision_value(row, "action") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
    ]
    return terminal_rows[-1] if terminal_rows else (sorted_rows[-1] if sorted_rows else None)


def choose_pass_without_pending_row(signal_rows):
    sorted_rows = sorted(signal_rows or [], key=row_ts)
    pass_ts_values = [
        row_ts(row)
        for row in sorted_rows
        if decision_would_enter(row)
    ]
    first_pass_ts = min(pass_ts_values, default=None)
    after_pass = [
        row
        for row in sorted_rows
        if first_pass_ts is None or row_ts(row) >= first_pass_ts
    ]
    terminal_rows = [
        row
        for row in after_pass
        if str(decision_value(row, "event_type") or "").lower()
        in {"entry_block", "entry_abort", "pending_reject"}
        or str(decision_value(row, "decision") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
        or str(decision_value(row, "action") or "").upper()
        in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
    ]
    return terminal_rows[0] if terminal_rows else (after_pass[-1] if after_pass else None), first_pass_ts


def choose_pending_without_final_row(signal_rows):
    sorted_rows = sorted(signal_rows or [], key=row_ts)
    pending_ts_values = [
        row_ts(row)
        for row in sorted_rows
        if str(decision_value(row, "event_type") or "").lower() == "pending_entry"
    ]
    first_pending_ts = min(pending_ts_values, default=None)
    after_pending = [
        row
        for row in sorted_rows
        if first_pending_ts is None or row_ts(row) >= first_pending_ts
    ]
    terminal_rows = [
        row
        for row in after_pending
        if str(decision_value(row, "event_type") or "").lower() == "entry_block"
        or str(decision_value(row, "decision") or "").upper() in {"BLOCK", "REJECT", "WATCH_ONLY"}
        or str(decision_value(row, "action") or "").upper() in {"BLOCK", "REJECT", "WATCH_ONLY"}
    ]
    return terminal_rows[0] if terminal_rows else (after_pending[-1] if after_pending else None), first_pending_ts


def is_final_entry_contract(row):
    return str(decision_value(row, "source_component") or decision_value(row, "component") or "") == "final_entry_contract"


def stage_quality_timing_events(raw_rows, decisions):
    raw_ids = {row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}
    decisions_by_signal = defaultdict(list)
    for row in decisions:
        key = signal_id_key(decision_value(row, "signal_id"))
        if key in raw_ids:
            decisions_by_signal[key].append(row)

    events = {}
    for raw in raw_rows:
        signal_id = raw.get("signal_id_key")
        if not signal_id:
            continue
        rows = sorted(decisions_by_signal.get(signal_id, []), key=row_ts)
        if not rows:
            continue
        pass_allow = [row for row in rows if decision_would_enter(row)]
        pending = [
            row
            for row in rows
            if str(decision_value(row, "event_type") or "").lower() == "pending_entry"
        ]
        final_contract = [row for row in rows if is_final_entry_contract(row)]

        stage = None
        chosen = None
        first_stage_ts = None
        if not pass_allow:
            stage = "decision_no_pass_or_allow"
            chosen = choose_decision_no_pass_row(rows)
            category = classify_upstream_gap_reason(reason_row(chosen), stage)
        elif not pending:
            stage = "pass_or_allow_without_pending_entry"
            chosen, first_stage_ts = choose_pass_without_pending_row(rows)
            category = classify_upstream_gap_reason(reason_row(chosen), stage)
        elif not final_contract:
            stage = "pending_without_final_entry_contract"
            chosen, first_stage_ts = choose_pending_without_final_row(rows)
            category = classify_pending_gap_reason(reason_row(chosen))
        else:
            continue

        if category != "QUALITY_OR_TIMING_REJECT":
            continue
        item = {
            "signal_id": signal_id,
            "stage": stage,
            "category": category,
            "first_stage_ts": first_stage_ts,
            "attribution": reason_row(chosen),
        }
        item["reason_key"] = (
            item["stage"],
            item["attribution"]["component"],
            item["attribution"]["event_type"],
            item["attribution"]["decision"],
            item["attribution"]["reason"],
        )
        events[signal_id] = item
    return events


def compact_counter(counter, names, limit=30):
    rows = []
    for key, count in counter.most_common(limit):
        if not isinstance(key, tuple):
            key = (key,)
        item = {names[idx]: key[idx] if idx < len(key) else None for idx in range(len(names))}
        item["count"] = count
        rows.append(item)
    return rows


def blocked_candidate_dimensions(candidate_id, family):
    candidate_text = str(candidate_id or "").lower()
    family_text = str(family or "").lower()
    dimensions = []
    if family_text == "kline" or candidate_text.startswith("kline:"):
        dimensions.append("kline")
    volume_markers = ("volume", "lowvol", "low_vol", "vol_", "_vol")
    if family_text == "volume" or any(marker in candidate_text for marker in volume_markers):
        dimensions.append("volume")
    return sorted(set(dimensions))


def is_blocked_context_candidate(candidate_id, family):
    return bool(blocked_candidate_dimensions(candidate_id, family))


def compact_candidate_counter_with_context(counter, limit=30):
    rows = []
    for (candidate_id, family), count in counter.most_common(limit):
        dimensions = blocked_candidate_dimensions(candidate_id, family)
        rows.append({
            "candidate_id": candidate_id,
            "family": family,
            "count": count,
            "blocked_context_dimensions": dimensions,
            "context_clean_for_candidate_suggestion": not bool(dimensions),
        })
    return rows


def count_raw_signals_reaching_final_entry_contract(raw_rows, decisions):
    raw_ids = {row.get("signal_id_key") for row in raw_rows if row.get("signal_id_key")}
    reached = set()
    for row in decisions:
        key = signal_id_key(decision_value(row, "signal_id"))
        if key in raw_ids and is_final_entry_contract(row):
            reached.add(key)
    return reached


def build_readiness_impact_upper_bound(raw_rows, qt_count, reached_final_signal_ids, cluster_counts):
    raw_count = len(raw_rows)
    current_final_count = len(reached_final_signal_ids or set())
    target_count = int((READINESS_TARGET_RATE * raw_count) + 0.999999) if raw_count else 0
    current_gap = max(0, target_count - current_final_count)
    potential_final_count = min(raw_count, current_final_count + int(qt_count or 0))
    residual_gap_after_all_qt = max(0, target_count - potential_final_count)
    cluster_rows = []
    for cluster, count in (cluster_counts or Counter()).most_common():
        potential_count = min(raw_count, current_final_count + int(count or 0))
        cluster_rows.append({
            "cluster": cluster,
            "event_count": count,
            "current_final_eligibility_count": current_final_count,
            "upper_bound_final_eligibility_count_if_cluster_resolved": potential_count,
            "upper_bound_final_eligibility_rate_if_cluster_resolved": rate(potential_count, raw_count),
            "events_contributing_to_60pct_gap_upper_bound": min(int(count or 0), current_gap),
            "share_of_current_60pct_gap_upper_bound": rate(min(int(count or 0), current_gap), current_gap),
            "residual_gap_to_60pct_after_cluster_upper_bound": max(0, target_count - potential_count),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    return {
        "target_final_eligibility_rate": READINESS_TARGET_RATE,
        "raw_all_gold_silver_event_rows": raw_count,
        "target_final_eligibility_event_count": target_count,
        "current_final_entry_contract_signal_count": current_final_count,
        "current_final_entry_contract_rate": rate(current_final_count, raw_count),
        "quality_timing_reject_event_rows": qt_count,
        "current_gap_to_60pct_event_count": current_gap,
        "quality_timing_rejects_share_of_current_60pct_gap_upper_bound": rate(min(int(qt_count or 0), current_gap), current_gap),
        "upper_bound_final_eligibility_count_if_all_quality_timing_resolved": potential_final_count,
        "upper_bound_final_eligibility_rate_if_all_quality_timing_resolved": rate(potential_final_count, raw_count),
        "residual_gap_to_60pct_after_all_quality_timing_upper_bound": residual_gap_after_all_qt,
        "would_all_quality_timing_resolution_reach_60pct_upper_bound": potential_final_count >= target_count if raw_count else False,
        "cluster_upper_bounds": cluster_rows,
        "interpretation": (
            "Upper bound only: assumes every quality/timing reject could safely reach final_entry_contract. "
            "It does not prove the rejects were wrong, safe, or eligible for runtime changes."
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def top_examples(rows, limit):
    rows = sorted(
        rows,
        key=lambda row: safe_float(row.get("max_sustained_peak_pct")) or 0,
        reverse=True,
    )
    return rows[:limit]


def classify_shadow_review_cluster(stage, attribution):
    component = str((attribution or {}).get("component") or "").lower()
    event_type = str((attribution or {}).get("event_type") or "").lower()
    reason = str((attribution or {}).get("reason") or "").lower()
    decision = str((attribution or {}).get("decision") or "").lower()
    text = " ".join([stage or "", component, event_type, decision, reason])
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


SHADOW_REVIEW_CLUSTER_DETAILS = {
    "matrix_alignment_wait": {
        "description": "Raw gold/silver reached a matrix wait state before pass/pending/final eligibility.",
        "suggested_shadow_only_action": "track_matrix_alignment_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing matrix alignment thresholds or treating OBSERVE/WAIT as entry-eligible",
    },
    "low_volume_observe": {
        "description": "Raw gold/silver was skipped or delayed by low-volume observation logic.",
        "suggested_shadow_only_action": "track_low_volume_gold_silver_shadow_probe",
        "human_approval_required_if_fix_requires": "relaxing low-volume, liquidity, or market-quality gates",
    },
    "newborn_pullback_timing_reject": {
        "description": "Raw gold/silver was rejected by newborn pullback timing logic.",
        "suggested_shadow_only_action": "track_newborn_pullback_timing_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing newborn timing rules or pullback rejection thresholds",
    },
    "notath_upstream_skip": {
        "description": "Raw gold/silver was skipped by upstream NOT_ATH routing or classification.",
        "suggested_shadow_only_action": "track_notath_upstream_skip_shadow_probe",
        "human_approval_required_if_fix_requires": "changing upstream ATH/NOT_ATH routing policy",
    },
    "buy_pressure_weak": {
        "description": "Raw gold/silver was rejected by weak buy-pressure or scout-quality logic.",
        "suggested_shadow_only_action": "track_buy_pressure_weak_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "relaxing scout quality or buy-pressure gates",
    },
    "chasing_top_timing_reject": {
        "description": "Raw gold/silver was rejected as chasing top.",
        "suggested_shadow_only_action": "track_chasing_top_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing anti-chase timing policy",
    },
    "score_or_quality_too_low": {
        "description": "Raw gold/silver was rejected by a score or quality threshold.",
        "suggested_shadow_only_action": "track_score_quality_threshold_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing score or quality thresholds",
    },
    "momentum_fading_or_negative_trend": {
        "description": "Raw gold/silver was rejected for fading momentum or negative trend.",
        "suggested_shadow_only_action": "track_momentum_fading_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing momentum/trend rejection policy",
    },
    "entry_timing_timeout_or_retry": {
        "description": "Raw gold/silver was delayed or timed out around entry retry/timing logic.",
        "suggested_shadow_only_action": "track_entry_timing_timeout_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "changing entry retry, timeout, or scheduling policy",
    },
    "final_entry_contract_research_block": {
        "description": "Raw gold/silver reached final-entry-related block evidence inside the quality/timing audit scope.",
        "suggested_shadow_only_action": "decompose_final_entry_hard_block_shadow_only",
        "human_approval_required_if_fix_requires": "changing final_entry_contract, A_CLASS mode, or paper/live enablement",
    },
    "holder_concentration_quality_reject": {
        "description": "Raw gold/silver was rejected by holder concentration or related quality logic.",
        "suggested_shadow_only_action": "track_holder_concentration_false_negative_shadow_probe",
        "human_approval_required_if_fix_requires": "relaxing holder concentration or quality gates",
    },
    "other_quality_timing_reject": {
        "description": "Raw gold/silver was rejected by a less frequent quality/timing reason.",
        "suggested_shadow_only_action": "continue_reason_level_shadow_review",
        "human_approval_required_if_fix_requires": "changing strategy, entry policy, gate, final_entry, or runtime behavior",
    },
    "unknown_quality_timing_reject": {
        "description": "Raw gold/silver was rejected but the reason payload was not classifiable.",
        "suggested_shadow_only_action": "improve_quality_timing_reason_instrumentation",
        "human_approval_required_if_fix_requires": "changing strategy, entry policy, gate, final_entry, or runtime behavior",
    },
}


def median_or_none(values):
    parsed = [safe_float(value) for value in values]
    parsed = [value for value in parsed if value is not None]
    return None if not parsed else round(float(median(parsed)), 6)


def max_or_none(values):
    parsed = [safe_float(value) for value in values]
    parsed = [value for value in parsed if value is not None]
    return None if not parsed else round(max(parsed), 6)


def build_shadow_only_review(
    *,
    raw_rows,
    qt_count,
    stage_counts,
    cluster_counts,
    cluster_stage_counts,
    cluster_candidate_counts,
    cluster_family_counts,
    cluster_clean_candidate_counts,
    cluster_clean_family_counts,
    cluster_blocked_candidate_counts,
    cluster_blocked_family_counts,
    cluster_context_counts,
    cluster_tokens,
    cluster_matched_any,
    cluster_clean_matched_any,
    cluster_blocked_matched_any,
    cluster_peak_pct,
    cluster_time_to_peak,
    readiness_impact_upper_bound,
    limit,
):
    cluster_impact = {
        row.get("cluster"): row
        for row in (readiness_impact_upper_bound or {}).get("cluster_upper_bounds") or []
    }
    opportunities = []
    for cluster, count in cluster_counts.most_common(limit):
        details = SHADOW_REVIEW_CLUSTER_DETAILS.get(
            cluster,
            SHADOW_REVIEW_CLUSTER_DETAILS["other_quality_timing_reject"],
        )
        opportunities.append({
            "cluster": cluster,
            "description": details["description"],
            "event_count": count,
            "share_of_quality_timing_rejects": rate(count, qt_count),
            "share_of_raw_all_gold_silver": rate(count, len(raw_rows)),
            "unique_tokens": len(cluster_tokens.get(cluster) or set()),
            "candidate_matched_any_rate": rate(cluster_matched_any.get(cluster, 0), count),
            "clean_candidate_matched_any_rate": rate(cluster_clean_matched_any.get(cluster, 0), count),
            "blocked_candidate_matched_any_rate": rate(cluster_blocked_matched_any.get(cluster, 0), count),
            "max_sustained_peak_pct_max": max_or_none(cluster_peak_pct.get(cluster) or []),
            "time_to_sustained_peak_sec_median": median_or_none(cluster_time_to_peak.get(cluster) or []),
            "readiness_impact_upper_bound": cluster_impact.get(cluster) or {},
            "stage_counts": compact_counter(
                cluster_stage_counts.get(cluster) or Counter(),
                ["stage"],
                limit,
            ),
            "top_candidates": compact_counter(
                cluster_candidate_counts.get(cluster) or Counter(),
                ["candidate_id", "family"],
                limit,
            ),
            "top_clean_candidates": compact_candidate_counter_with_context(
                cluster_clean_candidate_counts.get(cluster) or Counter(),
                limit,
            ),
            "top_blocked_candidates": compact_candidate_counter_with_context(
                cluster_blocked_candidate_counts.get(cluster) or Counter(),
                limit,
            ),
            "top_families": compact_counter(
                cluster_family_counts.get(cluster) or Counter(),
                ["family"],
                limit,
            ),
            "top_clean_families": compact_counter(
                cluster_clean_family_counts.get(cluster) or Counter(),
                ["family"],
                limit,
            ),
            "top_blocked_families": compact_counter(
                cluster_blocked_family_counts.get(cluster) or Counter(),
                ["family"],
                limit,
            ),
            "top_lifecycle_source_contexts": compact_counter(
                cluster_context_counts.get(cluster) or Counter(),
                ["lifecycle_profile", "source_component"],
                limit,
            ),
            "suggested_shadow_only_action": details["suggested_shadow_only_action"],
            "evidence_level": EVIDENCE_LEVEL,
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "human_approval_required_if_fix_requires": details["human_approval_required_if_fix_requires"],
            "next_validation": "repeat_same_cluster_in_clean_window_then_oos_if_it_generates_shadow_candidate_lift",
        })
    dominant_stage = stage_counts.most_common(1)[0][0] if stage_counts else None
    return {
        "classification": (
            "QUALITY_TIMING_SHADOW_REVIEW_READY"
            if qt_count
            else "QUALITY_TIMING_SHADOW_REVIEW_EMPTY"
        ),
        "quality_timing_false_negative_upper_bound": {
            "event_count": qt_count,
            "raw_all_gold_silver_event_rows": len(raw_rows),
            "rate": rate(qt_count, len(raw_rows)),
            "interpretation": (
                "Upper bound only: these raw gold/silver events were rejected by quality/timing logic, "
                "but this does not prove the reject was wrong or safe to trade."
            ),
        },
        "readiness_impact_upper_bound": readiness_impact_upper_bound,
        "dominant_cluster": opportunities[0]["cluster"] if opportunities else None,
        "dominant_stage": dominant_stage,
        "research_opportunity_count": len(opportunities),
        "top_research_opportunities": opportunities,
        "allowed_scope": [
            "read-only evaluator/report improvements",
            "shadow-only candidate or context instrumentation",
            "hypothesis registry entries for future clean-window/OOS validation",
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
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "notes": [
            "Use these clusters to decide what shadow-only probes to add or watch next.",
            "Prefer top_clean_candidates when generating shadow probes while kline/volume dimensions are blocked.",
            "Any change to thresholds, gates, final_entry_contract, runtime mode, executor, or risk requires human approval.",
        ],
    }


def build_blocked_context_excluded_view(
    *,
    qt_count,
    clean_candidate_counts,
    clean_family_counts,
    blocked_candidate_counts,
    blocked_family_counts,
    clean_matched_any,
    blocked_matched_any,
    clean_observation_count,
    blocked_observation_count,
    limit,
):
    return {
        "classification": (
            "CLEAN_CANDIDATE_ATTRIBUTION_READY"
            if qt_count
            else "CLEAN_CANDIDATE_ATTRIBUTION_EMPTY"
        ),
        "blocked_dimensions": BLOCKED_CONTEXT_DIMENSIONS,
        "interpretation": (
            "Read-only candidate attribution that excludes candidates depending on blocked kline/volume dimensions. "
            "Blocked candidates remain visible for diagnostics but must not drive shadow probe suggestions until the dimensions are clean."
        ),
        "quality_timing_reject_event_rows": qt_count,
        "clean_candidate_matched_any_events": clean_matched_any,
        "clean_candidate_matched_any_rate": rate(clean_matched_any, qt_count),
        "blocked_candidate_matched_any_events": blocked_matched_any,
        "blocked_candidate_matched_any_rate": rate(blocked_matched_any, qt_count),
        "clean_candidate_observation_count": clean_observation_count,
        "blocked_candidate_observation_count": blocked_observation_count,
        "top_clean_candidates": compact_candidate_counter_with_context(clean_candidate_counts, limit),
        "top_blocked_candidates": compact_candidate_counter_with_context(blocked_candidate_counts, limit),
        "top_clean_families": compact_counter(clean_family_counts, ["family"], limit),
        "top_blocked_families": compact_counter(blocked_family_counts, ["family"], limit),
        "candidate_suggestion_policy": (
            "Use top_clean_candidates for shadow-only quality/timing probe generation. "
            "Do not use top_blocked_candidates while kline/volume context coverage is blocked."
        ),
        "evidence_level": EVIDENCE_LEVEL,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def build_top_level_summary(*, verdict, blockers, denominator, readiness_impact, shadow_review):
    opportunities = (shadow_review or {}).get("top_research_opportunities") or []
    dominant = opportunities[0] if opportunities else {}
    if blockers:
        next_action = "fix_quality_timing_audit_data_blockers"
    elif dominant:
        next_action = dominant.get("suggested_shadow_only_action") or "review_shadow_candidates_for_quality_timing_rejects"
    else:
        next_action = "continue_collecting_quality_timing_reject_evidence"
    top_clusters = []
    for row in opportunities[:8]:
        top_clusters.append({
            "cluster": row.get("cluster"),
            "event_count": row.get("event_count"),
            "share_of_quality_timing_rejects": row.get("share_of_quality_timing_rejects"),
            "share_of_raw_all_gold_silver": row.get("share_of_raw_all_gold_silver"),
            "suggested_shadow_only_action": row.get("suggested_shadow_only_action"),
            "next_validation": row.get("next_validation"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    return {
        "classification": verdict,
        "next_action": next_action,
        "dominant_cluster": dominant.get("cluster"),
        "dominant_stage": (shadow_review or {}).get("dominant_stage"),
        "quality_timing_reject_event_rows": (denominator or {}).get("quality_timing_reject_event_rows"),
        "quality_timing_reject_share_of_raw_all": (denominator or {}).get("quality_timing_reject_share_of_raw_all"),
        "raw_all_gold_silver_event_rows": (denominator or {}).get("raw_all_gold_silver_event_rows"),
        "current_final_entry_contract_rate": (readiness_impact or {}).get("current_final_entry_contract_rate"),
        "current_final_entry_contract_signal_count": (
            readiness_impact or {}
        ).get("current_final_entry_contract_signal_count"),
        "target_final_eligibility_event_count": (readiness_impact or {}).get("target_final_eligibility_event_count"),
        "current_gap_to_60pct_event_count": (readiness_impact or {}).get("current_gap_to_60pct_event_count"),
        "upper_bound_final_eligibility_rate_if_all_quality_timing_resolved": (
            readiness_impact or {}
        ).get("upper_bound_final_eligibility_rate_if_all_quality_timing_resolved"),
        "residual_gap_to_60pct_after_all_quality_timing_upper_bound": (
            readiness_impact or {}
        ).get("residual_gap_to_60pct_after_all_quality_timing_upper_bound"),
        "would_all_quality_timing_resolution_reach_60pct_upper_bound": (
            readiness_impact or {}
        ).get("would_all_quality_timing_resolution_reach_60pct_upper_bound"),
        "top_quality_timing_clusters": top_clusters,
        "allowed_scope": (shadow_review or {}).get("allowed_scope") or [
            "read-only evaluator/report improvements",
            "shadow-only candidate or context instrumentation",
        ],
        "human_approval_required_if_fix_requires": (
            dominant.get("human_approval_required_if_fix_requires")
            or "changing strategy, entry policy, gate, final_entry, runtime mode, executor, or risk"
        ),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "interpretation": (
            "Top-level read-only summary for the quality/timing reject audit. "
            "It ranks shadow-only research targets and does not authorize runtime, strategy, gate, "
            "final_entry_contract, paper, executor, canary, or risk changes."
        ),
    }


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    raw_db = sqlite3.connect(args.raw_db)
    raw_db.row_factory = sqlite3.Row
    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    try:
        if not table_exists(raw_db, "raw_signal_outcomes"):
            raise SystemExit("raw_signal_outcomes table missing")
        raw_rows = load_raw_dogs(raw_db, since_ts)
        raw_signal_ids, raw_tokens, _, _ = make_raw_indexes(raw_rows)
        until_ts = max([row.get("signal_ts_norm") or since_ts for row in raw_rows] + [now_ts])
        observations, obs_meta = load_candidate_observations(paper_db, raw_signal_ids, since_ts)
        decisions = load_paper_decisions(paper_db, raw_tokens, since_ts, until_ts)
        trades = load_paper_trades(paper_db, raw_tokens, since_ts, until_ts)
        audits = attach_records(raw_rows, observations, decisions, trades, args.expected_candidates)
        audits_by_signal = {row.get("signal_id"): row for row in audits if row.get("signal_id")}
        observations_by_signal = defaultdict(list)
        for obs in observations:
            if obs.get("signal_id_key"):
                observations_by_signal[obs["signal_id_key"]].append(obs)

        qt_events = stage_quality_timing_events(raw_rows, decisions)
        reached_final_signal_ids = count_raw_signals_reaching_final_entry_contract(raw_rows, decisions)
        qt_rows = []
        stage_counts = Counter()
        reason_counts = Counter()
        candidate_counts = Counter()
        family_counts = Counter()
        clean_candidate_counts = Counter()
        clean_family_counts = Counter()
        blocked_candidate_counts = Counter()
        blocked_family_counts = Counter()
        context_counts = Counter()
        lifecycle_counts = Counter()
        source_counts = Counter()
        markov_counts = Counter()
        schema_counts = Counter()
        quote_clean_counts = Counter()
        quote_exec_counts = Counter()
        cluster_counts = Counter()
        cluster_stage_counts = defaultdict(Counter)
        cluster_candidate_counts = defaultdict(Counter)
        cluster_family_counts = defaultdict(Counter)
        cluster_clean_candidate_counts = defaultdict(Counter)
        cluster_clean_family_counts = defaultdict(Counter)
        cluster_blocked_candidate_counts = defaultdict(Counter)
        cluster_blocked_family_counts = defaultdict(Counter)
        cluster_context_counts = defaultdict(Counter)
        cluster_tokens = defaultdict(set)
        cluster_matched_any = Counter()
        cluster_clean_matched_any = Counter()
        cluster_blocked_matched_any = Counter()
        cluster_peak_pct = defaultdict(list)
        cluster_time_to_peak = defaultdict(list)
        coverage_ok = 0
        matched_any = 0
        clean_matched_any = 0
        blocked_matched_any = 0
        total_obs_rows = 0
        clean_observation_count = 0
        blocked_observation_count = 0

        for signal_id, event in qt_events.items():
            audit = audits_by_signal.get(signal_id) or {}
            signal_obs = observations_by_signal.get(signal_id, [])
            matched_obs = [obs for obs in signal_obs if obs.get("matched")]
            candidate_coverage_ok = len({obs.get("candidate_id") for obs in signal_obs}) == args.expected_candidates
            stage_counts[event["stage"]] += 1
            reason_counts[event["reason_key"]] += 1
            if candidate_coverage_ok:
                coverage_ok += 1
            if matched_obs:
                matched_any += 1
            total_obs_rows += len(signal_obs)
            lifecycle = audit.get("lifecycle_profile") or "UNKNOWN"
            source = audit.get("source_component") or "UNKNOWN"
            lifecycle_counts[lifecycle] += 1
            source_counts[source] += 1
            context_counts[(lifecycle, source)] += 1
            markov_counts[str(audit.get("markov_bucket") or "UNKNOWN")] += 1
            schema_counts[str(audit.get("context_schema_version") or "legacy_or_missing")] += 1
            quote_clean_counts[str(audit.get("source_quote_clean"))] += 1
            quote_exec_counts[str(audit.get("source_quote_executable"))] += 1
            cluster = classify_shadow_review_cluster(event["stage"], event["attribution"])
            cluster_counts[cluster] += 1
            cluster_stage_counts[cluster][event["stage"]] += 1
            cluster_context_counts[cluster][(lifecycle, source)] += 1
            if audit.get("token_ca"):
                cluster_tokens[cluster].add(audit.get("token_ca"))
            if matched_obs:
                cluster_matched_any[cluster] += 1
            cluster_peak_pct[cluster].append(audit.get("max_sustained_peak_pct"))
            cluster_time_to_peak[cluster].append(audit.get("time_to_sustained_peak_sec"))
            event_has_clean_match = False
            event_has_blocked_match = False
            for obs in matched_obs:
                candidate_id = obs.get("candidate_id") or "UNKNOWN"
                family = obs.get("family") or "UNKNOWN"
                candidate_counts[(candidate_id, family)] += 1
                family_counts[family] += 1
                cluster_candidate_counts[cluster][
                    (candidate_id, family)
                ] += 1
                cluster_family_counts[cluster][family] += 1
                if is_blocked_context_candidate(candidate_id, family):
                    blocked_candidate_counts[(candidate_id, family)] += 1
                    blocked_family_counts[family] += 1
                    cluster_blocked_candidate_counts[cluster][(candidate_id, family)] += 1
                    cluster_blocked_family_counts[cluster][family] += 1
                    blocked_observation_count += 1
                    event_has_blocked_match = True
                else:
                    clean_candidate_counts[(candidate_id, family)] += 1
                    clean_family_counts[family] += 1
                    cluster_clean_candidate_counts[cluster][(candidate_id, family)] += 1
                    cluster_clean_family_counts[cluster][family] += 1
                    clean_observation_count += 1
                    event_has_clean_match = True
            if event_has_clean_match:
                clean_matched_any += 1
                cluster_clean_matched_any[cluster] += 1
            if event_has_blocked_match:
                blocked_matched_any += 1
                cluster_blocked_matched_any[cluster] += 1
            qt_rows.append(
                {
                    "signal_id": signal_id,
                    "token_ca": audit.get("token_ca"),
                    "symbol": audit.get("symbol"),
                    "tier": audit.get("tier"),
                    "stage": event["stage"],
                    "shadow_review_cluster": cluster,
                    "attribution": event["attribution"],
                    "candidate_observation_count": len(signal_obs),
                    "candidate_coverage_ok": candidate_coverage_ok,
                    "matched_candidate_count": len(matched_obs),
                    "top_matched_candidates": [
                        {
                            "candidate_id": obs.get("candidate_id"),
                            "family": obs.get("family"),
                            "reason": obs.get("reason"),
                            "blocked_context_dimensions": blocked_candidate_dimensions(
                                obs.get("candidate_id"),
                                obs.get("family"),
                            ),
                        }
                        for obs in matched_obs[:20]
                    ],
                    "clean_matched_candidate_count": sum(
                        1
                        for obs in matched_obs
                        if not is_blocked_context_candidate(obs.get("candidate_id"), obs.get("family"))
                    ),
                    "blocked_matched_candidate_count": sum(
                        1
                        for obs in matched_obs
                        if is_blocked_context_candidate(obs.get("candidate_id"), obs.get("family"))
                    ),
                    "lifecycle_profile": lifecycle,
                    "source_component": source,
                    "markov_bucket": audit.get("markov_bucket"),
                    "source_quote_clean": audit.get("source_quote_clean"),
                    "source_quote_executable": audit.get("source_quote_executable"),
                    "context_schema_version": audit.get("context_schema_version"),
                    "max_sustained_peak_pct": audit.get("max_sustained_peak_pct"),
                    "time_to_sustained_peak_sec": audit.get("time_to_sustained_peak_sec"),
                }
            )

        qt_count = len(qt_events)
        blockers = []
        if not raw_rows:
            blockers.append("raw_gold_silver_denominator_empty")
        if qt_count and rate(coverage_ok, qt_count) is not None and rate(coverage_ok, qt_count) < 0.99:
            blockers.append("quality_timing_candidate_coverage_incomplete")
        verdict = "QUALITY_TIMING_REJECT_RESEARCH_READY"
        if not qt_count:
            verdict = "QUALITY_TIMING_REJECT_RESEARCH_EMPTY"
        elif blockers:
            verdict = "QUALITY_TIMING_REJECT_RESEARCH_BLOCKED_DATA"
        readiness_impact_upper_bound = build_readiness_impact_upper_bound(
            raw_rows,
            qt_count,
            reached_final_signal_ids,
            cluster_counts,
        )
        blocked_context_excluded_view = build_blocked_context_excluded_view(
            qt_count=qt_count,
            clean_candidate_counts=clean_candidate_counts,
            clean_family_counts=clean_family_counts,
            blocked_candidate_counts=blocked_candidate_counts,
            blocked_family_counts=blocked_family_counts,
            clean_matched_any=clean_matched_any,
            blocked_matched_any=blocked_matched_any,
            clean_observation_count=clean_observation_count,
            blocked_observation_count=blocked_observation_count,
            limit=args.limit,
        )
        shadow_only_review = build_shadow_only_review(
            raw_rows=raw_rows,
            qt_count=qt_count,
            stage_counts=stage_counts,
            cluster_counts=cluster_counts,
            cluster_stage_counts=cluster_stage_counts,
            cluster_candidate_counts=cluster_candidate_counts,
            cluster_family_counts=cluster_family_counts,
            cluster_clean_candidate_counts=cluster_clean_candidate_counts,
            cluster_clean_family_counts=cluster_clean_family_counts,
            cluster_blocked_candidate_counts=cluster_blocked_candidate_counts,
            cluster_blocked_family_counts=cluster_blocked_family_counts,
            cluster_context_counts=cluster_context_counts,
            cluster_tokens=cluster_tokens,
            cluster_matched_any=cluster_matched_any,
            cluster_clean_matched_any=cluster_clean_matched_any,
            cluster_blocked_matched_any=cluster_blocked_matched_any,
            cluster_peak_pct=cluster_peak_pct,
            cluster_time_to_peak=cluster_time_to_peak,
            readiness_impact_upper_bound=readiness_impact_upper_bound,
            limit=args.limit,
        )
        denominator = {
            "raw_all_gold_silver_event_rows": len(raw_rows),
            "raw_all_gold_silver_unique_tokens": len({row.get("token_ca") for row in raw_rows if row.get("token_ca")}),
            "quality_timing_reject_event_rows": qt_count,
            "quality_timing_reject_unique_tokens": len({
                (audits_by_signal.get(signal_id) or {}).get("token_ca")
                for signal_id in qt_events
                if (audits_by_signal.get(signal_id) or {}).get("token_ca")
            }),
            "quality_timing_reject_share_of_raw_all": rate(qt_count, len(raw_rows)),
            "quality_timing_reject_share_of_raw_all_pct": pct(qt_count, len(raw_rows)),
        }
        summary = build_top_level_summary(
            verdict=verdict,
            blockers=blockers,
            denominator=denominator,
            readiness_impact=readiness_impact_upper_bound,
            shadow_review=shadow_only_review,
        )

        return {
            "schema_version": SCHEMA_VERSION,
            "report_type": "quality_timing_reject_research_audit",
            "generated_at": utc_now(),
            "window": {"hours": args.hours, "since_ts": since_ts, "until_ts": now_ts},
            "inputs": {"paper_db": args.db, "raw_db": args.raw_db, "raw_funnel": args.raw_funnel},
            "evidence_level": EVIDENCE_LEVEL,
            "usage": "read_only_shadow_research_quality_timing_rejects",
            "promotion_allowed": False,
            "can_promote_live": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "final_entry_contract_change_allowed": False,
            "classification": summary["classification"],
            "next_action": summary["next_action"],
            "dominant_cluster": summary["dominant_cluster"],
            "dominant_stage": summary["dominant_stage"],
            "quality_timing_reject_event_rows": summary["quality_timing_reject_event_rows"],
            "quality_timing_reject_share_of_raw_all": summary["quality_timing_reject_share_of_raw_all"],
            "current_final_entry_contract_rate": summary["current_final_entry_contract_rate"],
            "upper_bound_final_eligibility_rate_if_all_quality_timing_resolved": (
                summary["upper_bound_final_eligibility_rate_if_all_quality_timing_resolved"]
            ),
            "would_all_quality_timing_resolution_reach_60pct_upper_bound": (
                summary["would_all_quality_timing_resolution_reach_60pct_upper_bound"]
            ),
            "residual_gap_to_60pct_after_all_quality_timing_upper_bound": (
                summary["residual_gap_to_60pct_after_all_quality_timing_upper_bound"]
            ),
            "top_quality_timing_clusters": summary["top_quality_timing_clusters"],
            "allowed_scope": summary["allowed_scope"],
            "human_approval_required_if_fix_requires": summary["human_approval_required_if_fix_requires"],
            "verdict": verdict,
            "blockers": blockers,
            "summary": summary,
            "denominator": denominator,
            "observation_load": obs_meta,
            "candidate_match_attribution": {
                "expected_candidates": args.expected_candidates,
                "candidate_observation_rows": total_obs_rows,
                "events_with_full_candidate_coverage": coverage_ok,
                "full_candidate_coverage_rate": rate(coverage_ok, qt_count),
                "full_candidate_coverage_pct": pct(coverage_ok, qt_count),
                "candidate_matched_any_events": matched_any,
                "candidate_matched_any_rate": rate(matched_any, qt_count),
                "candidate_matched_any_pct": pct(matched_any, qt_count),
                "top_candidates": compact_counter(candidate_counts, ["candidate_id", "family"], args.limit),
                "top_families": compact_counter(family_counts, ["family"], args.limit),
            },
            "blocked_context_dimensions_excluded_view": blocked_context_excluded_view,
            "readiness_impact_upper_bound": readiness_impact_upper_bound,
            "stage_attribution": {
                "stage_counts": compact_counter(stage_counts, ["stage"], args.limit),
                "reason_counts": compact_counter(
                    reason_counts,
                    ["stage", "component", "event_type", "decision", "reason"],
                    args.limit,
                ),
            },
            "context_attribution": {
                "lifecycle_profile_counts": compact_counter(lifecycle_counts, ["lifecycle_profile"], args.limit),
                "source_component_counts": compact_counter(source_counts, ["source_component"], args.limit),
                "lifecycle_source_counts": compact_counter(
                    context_counts,
                    ["lifecycle_profile", "source_component"],
                    args.limit,
                ),
                "markov_bucket_counts": compact_counter(markov_counts, ["markov_bucket"], args.limit),
                "context_schema_version_counts": compact_counter(schema_counts, ["context_schema_version"], args.limit),
                "source_quote_clean_counts": compact_counter(quote_clean_counts, ["source_quote_clean"], args.limit),
                "source_quote_executable_counts": compact_counter(
                    quote_exec_counts,
                    ["source_quote_executable"],
                    args.limit,
                ),
            },
            "shadow_only_review": shadow_only_review,
            "shadow_only_next_actions": [
                "review_shadow_candidates_for_quality_timing_rejects",
                "compare quality/timing reject candidate families against pending/final eligibility lifts",
                "do not relax timing or quality thresholds without human approval",
            ],
            "top_examples": top_examples(qt_rows, args.limit),
            "notes": [
                "Research-only upper-bound audit. This report explains quality/timing rejects; it does not authorize strategy, gate, final_entry_contract, A_CLASS, executor, or risk changes.",
                "promotion_allowed remains false.",
            ],
        }
    finally:
        raw_db.close()
        paper_db.close()


def compact_summary(report):
    return {
        "verdict": report.get("verdict"),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "denominator": report.get("denominator") or {},
        "candidate_match_attribution": report.get("candidate_match_attribution") or {},
        "blocked_context_dimensions_excluded_view": (
            report.get("blocked_context_dimensions_excluded_view") or {}
        ),
        "readiness_impact_upper_bound": report.get("readiness_impact_upper_bound") or {},
        "top_stage_counts": ((report.get("stage_attribution") or {}).get("stage_counts") or [])[:8],
        "top_reason_counts": ((report.get("stage_attribution") or {}).get("reason_counts") or [])[:8],
        "top_candidates": ((report.get("candidate_match_attribution") or {}).get("top_candidates") or [])[:10],
        "top_contexts": ((report.get("context_attribution") or {}).get("lifecycle_source_counts") or [])[:10],
        "shadow_only_review": report.get("shadow_only_review") or {},
        "blockers": report.get("blockers") or [],
    }


def self_test():
    now = 2_000_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        paper_path = root / "paper.db"
        raw_path = root / "raw.db"
        paper = sqlite3.connect(paper_path)
        paper.execute(
            """
            CREATE TABLE candidate_shadow_observations(
              signal_id TEXT, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT,
              matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT
            )
            """
        )
        payload = json.dumps({
            "lifecycle_profile": "ATH_SHALLOW_PULLBACK:OBSERVE",
            "source_component": "matrix_evaluator",
            "markov_bucket": "insufficient",
            "source_quote_clean": True,
            "source_quote_executable": False,
            "context_schema_version": "candidate-shadow-context-v2.no_signal_price_quote_inference",
        })
        for sig, token, cand, family, matched in [
            ("101", "QT1", "current_all", "baseline", 1),
            ("101", "QT1", "kline:active_mom20_first3", "kline", 1),
            ("102", "QT2", "current_all", "baseline", 1),
            ("102", "QT2", "entry_mode_registry:pullback_tiny_scout", "entry_mode_registry", 1),
            ("103", "OK", "current_all", "baseline", 1),
            ("103", "OK", "kline:active_mom20_first3", "kline", 1),
        ]:
            paper.execute(
                "INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?,?,?,?)",
                (sig, token, now - 100, cand, family, matched, "self_test", now - 50, payload if cand == "current_all" else "{}"),
            )
        paper.execute(
            """
            CREATE TABLE paper_decision_events(
              id INTEGER, event_ts INTEGER, signal_id TEXT, token_ca TEXT, symbol TEXT,
              lifecycle_id TEXT, component TEXT, reason TEXT, event_type TEXT, decision TEXT,
              route TEXT, data_source TEXT, lifecycle_state TEXT, payload_json TEXT
            )
            """
        )
        paper.executemany(
            "INSERT INTO paper_decision_events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                (1, now - 80, "101", "QT1", "QT1", "lc1", "smart_entry", "quality_score_low", "quality_gate", "REJECT", None, None, None, "{}"),
                (2, now - 70, "102", "QT2", "QT2", "lc2", "smart_entry", "pass", "would_enter", "PASS", None, None, None, "{}"),
                (3, now - 65, "102", "QT2", "QT2", "lc2", "scout_quality", "timing_too_late", "timing_decision", "REJECT", None, None, None, "{}"),
                (4, now - 60, "103", "OK", "OK", "lc3", "smart_entry", "pass", "would_enter", "PASS", None, None, None, "{}"),
                (5, now - 55, "103", "OK", "OK", "lc3", "entry_engine", "pending", "pending_entry", "PENDING", None, None, None, "{}"),
                (6, now - 50, "103", "OK", "OK", "lc3", "final_entry_contract", "mode_disabled", "entry_block", "BLOCK", None, None, None, "{}"),
            ],
        )
        paper.commit()
        paper.close()

        raw = sqlite3.connect(raw_path)
        raw.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
              signal_type TEXT, source TEXT, observation_status TEXT,
              kline_covered INTEGER, coverage_reason TEXT, baseline_confidence TEXT,
              same_source_path INTEGER, outlier_flag INTEGER, outlier_reason TEXT,
              sustained_evaluable INTEGER, sustained_reason TEXT, raw_sustained_tier TEXT,
              raw_primary_tier TEXT, max_sustained_peak_pct REAL, max_wick_peak_pct REAL,
              time_to_sustained_peak_sec REAL, did_enter INTEGER, entered_before_peak INTEGER,
              held_to_silver INTEGER, held_to_gold INTEGER, raw_dog_entered INTEGER,
              raw_dog_realized INTEGER, sold_before_silver INTEGER, sold_before_gold INTEGER,
              exit_reason TEXT, payload_json TEXT, source_kind TEXT, source_family TEXT
            )
            """
        )
        raw.executemany(
            "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                ("101", "QT1", "QT1", now - 100, "ATH", "src", "matured", 1, "covered", "high", 1, 0, None, 1, None, "silver", "silver", 90, 95, 100, 0, 0, 0, 0, 0, 0, 0, 0, None, "{}", "dex", "native"),
                ("102", "QT2", "QT2", now - 90, "ATH", "src", "matured", 1, "covered", "high", 1, 0, None, 1, None, "gold", "gold", 300, 320, 120, 0, 0, 0, 0, 0, 0, 0, 0, None, "{}", "dex", "native"),
                ("103", "OK", "OK", now - 80, "ATH", "src", "matured", 1, "covered", "high", 1, 0, None, 1, None, "silver", "silver", 110, 120, 100, 0, 0, 0, 0, 0, 0, 0, 0, None, "{}", "dex", "native"),
            ],
        )
        raw.commit()
        raw.close()

        args = argparse.Namespace(
            db=str(paper_path),
            raw_db=str(raw_path),
            raw_funnel=None,
            hours=1,
            expected_candidates=2,
            now_ts=now,
            limit=10,
            out=None,
            compact=False,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["strategy_change_allowed"] is False
        assert report["classification"] == "QUALITY_TIMING_REJECT_RESEARCH_READY"
        assert report["verdict"] == "QUALITY_TIMING_REJECT_RESEARCH_READY"
        assert report["next_action"] in {
            "track_score_quality_threshold_false_negative_shadow_probe",
            "track_newborn_pullback_timing_false_negative_shadow_probe",
            "continue_reason_level_shadow_review",
        }
        assert report["dominant_cluster"] is not None
        assert report["quality_timing_reject_event_rows"] == 2
        assert report["upper_bound_final_eligibility_rate_if_all_quality_timing_resolved"] == 1.0
        assert report["would_all_quality_timing_resolution_reach_60pct_upper_bound"] is True
        assert report["top_quality_timing_clusters"]
        assert report["allowed_scope"]
        assert report["summary"]["promotion_allowed"] is False
        assert report["summary"]["automatic_runtime_change_allowed"] is False
        assert report["summary"]["paper_enablement_allowed"] is False
        assert report["denominator"]["quality_timing_reject_event_rows"] == 2
        assert report["candidate_match_attribution"]["candidate_matched_any_events"] == 2
        assert report["candidate_match_attribution"]["full_candidate_coverage_rate"] == 1.0
        clean_view = report["blocked_context_dimensions_excluded_view"]
        assert clean_view["classification"] == "CLEAN_CANDIDATE_ATTRIBUTION_READY"
        assert clean_view["blocked_dimensions"] == ["kline", "volume"]
        assert clean_view["clean_candidate_matched_any_events"] == 2
        assert clean_view["blocked_candidate_matched_any_events"] == 1
        assert clean_view["clean_candidate_observation_count"] == 3
        assert clean_view["blocked_candidate_observation_count"] == 1
        assert any(
            row["candidate_id"] == "entry_mode_registry:pullback_tiny_scout"
            for row in clean_view["top_clean_candidates"]
        )
        assert all(
            not row["blocked_context_dimensions"]
            for row in clean_view["top_clean_candidates"]
        )
        assert any(
            row["candidate_id"] == "kline:active_mom20_first3"
            and "kline" in row["blocked_context_dimensions"]
            for row in clean_view["top_blocked_candidates"]
        )
        impact = report["readiness_impact_upper_bound"]
        assert impact["current_final_entry_contract_signal_count"] == 1
        assert impact["quality_timing_reject_event_rows"] == 2
        assert impact["upper_bound_final_eligibility_count_if_all_quality_timing_resolved"] == 3
        assert impact["would_all_quality_timing_resolution_reach_60pct_upper_bound"] is True
        review = report["shadow_only_review"]
        assert review["classification"] == "QUALITY_TIMING_SHADOW_REVIEW_READY"
        assert review["promotion_allowed"] is False
        assert review["strategy_change_allowed"] is False
        assert review["automatic_runtime_change_allowed"] is False
        assert review["paper_enablement_allowed"] is False
        assert review["research_opportunity_count"] >= 1
        assert review["top_research_opportunities"][0]["promotion_allowed"] is False
        assert review["top_research_opportunities"][0]["readiness_impact_upper_bound"]["promotion_allowed"] is False
        assert "top_clean_candidates" in review["top_research_opportunities"][0]
        assert "top_blocked_candidates" in review["top_research_opportunities"][0]
        stages = {row["stage"]: row["count"] for row in report["stage_attribution"]["stage_counts"]}
        assert stages["decision_no_pass_or_allow"] == 1
        assert stages["pass_or_allow_without_pending_entry"] == 1
        assert "pending_without_final_entry_contract" not in stages
        compact = compact_summary(report)
        assert compact["promotion_allowed"] is False
        assert compact["shadow_only_review"]["classification"] == "QUALITY_TIMING_SHADOW_REVIEW_READY"
        assert compact["blocked_context_dimensions_excluded_view"]["classification"] == "CLEAN_CANDIDATE_ATTRIBUTION_READY"
    print("SELF_TEST_PASS quality_timing_reject_research_audit")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--raw-funnel", default=None)
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--expected-candidates", type=int, default=DEFAULT_EXPECTED_CANDIDATES)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--out")
    parser.add_argument("--compact", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    payload = compact_summary(report) if args.compact else report
    if args.out:
        jdump(args.out, payload)
    else:
        print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
