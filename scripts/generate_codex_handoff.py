#!/usr/bin/env python3
"""Generate a Codex handoff from a capture discovery reviewer verdict.

The handoff is for data/report/evaluator fixes only. It must not recommend
strategy, entry policy, hard gate, exit gate, executor, canary, or risk changes.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "capture_discovery_codex_handoff.v6"
QUOTE_COVERAGE_BLOCKERS = {
    "source_quote_clean_coverage_below_80pct",
    "source_quote_executable_coverage_below_80pct",
}
FIXABLE_BLOCKER_HINTS = {
    "raw_dog_rows_incomplete": "Fix raw dog row materialization or report input wiring before judging capture recall.",
    "raw_gold_silver_denominator_rows_truncated": "Ensure the raw dog JSON/API includes complete event rows or use --raw-db.",
    "raw_dog_candidate_observation_join_incomplete": "Audit signal_id/token join between raw_signal_outcomes and candidate_shadow_observations.",
    "raw_all_dog_candidate_observation_join_incomplete": "Audit raw_all denominator join coverage; missing observations block business recall.",
    "signal_id_join_rate_below_99pct": "Normalize signal_id keys and inspect missing raw dog observation rows.",
    "schema_mixed_quote_sensitive_slices_blocked": "Wait for clean v2 rows or split report by context_schema_version before judging quote-sensitive slices.",
    "context_schema_v2_coverage_below_95pct_quote_sensitive_slices_blocked": "Do not judge quote-sensitive candidates until v2 schema coverage is at least 95%.",
    "quote_clean_definition_v2_coverage_below_95pct_quote_sensitive_slices_blocked": "Do not judge quote-sensitive candidates until quote_clean_definition v2 coverage is at least 95%.",
    "source_quote_clean_coverage_below_80pct": "Inspect quote context coverage audit and writer/source breakdowns; fix data/report wiring only, not entry policy.",
    "source_quote_executable_coverage_below_80pct": "Inspect executable quote context coverage audit and writer/source breakdowns; fix data/report wiring only, not gates or executor.",
    "volume_profile_coverage_below_80pct": "Inspect volume/kline root-cause audit; fix context carrier, kline-derived volume classification, or report wiring only.",
    "kline_coverage_below_80pct": "Inspect raw gold/silver kline coverage and low-confidence research split; fix data/source coverage or evaluator attribution only.",
    "candidate_count_observed_not_84": "Inspect candidate shadow observer coverage and catalog consistency.",
    "candidate_count_mismatch": "Inspect candidate shadow observer expected/observed candidate counts.",
    "observation_coverage_below_99pct": "Inspect per-signal observation coverage and missing candidate rows.",
    "per_signal_candidate_coverage_incomplete": "Inspect bad signal rows in candidate coverage and rerun candidate shadow observer if needed.",
    "tests_failed": "Fix report/evaluator tests before using the run verdict.",
}


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_text(path, text):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(target)


def quote_clean_window_pending(verdict):
    if verdict.get("blocked_subtype") != "CLEAN_V2_WINDOW_PENDING":
        return False
    writer_status = verdict.get("quote_writer_fix_status")
    window_status = verdict.get("quote_clean_window_status")
    if writer_status == "VERIFIED_POST_DEPLOY" and window_status == "QUOTE_CLEAN_WINDOW_PENDING":
        return True
    monitor = verdict.get("context_blocker_monitor") or {}
    overall = monitor.get("overall_verdict") or {}
    smoke = monitor.get("post_deploy_quote_smoke_test") or {}
    window = monitor.get("clean_window_monitor") or {}
    return (
        (overall.get("quote_writer_fix") == "VERIFIED_POST_DEPLOY" or smoke.get("classification") == "VERIFIED_POST_DEPLOY")
        and (overall.get("rolling24_quote_status") == "QUOTE_CLEAN_WINDOW_PENDING" or window.get("classification") == "QUOTE_CLEAN_WINDOW_PENDING")
    )


def actionable_blockers(verdict):
    if "actionable_blockers" in verdict:
        blockers = list(verdict.get("actionable_blockers") or [])
    else:
        blockers = list(verdict.get("blockers") or [])
    if quote_clean_window_pending(verdict):
        blockers = [blocker for blocker in blockers if blocker not in QUOTE_COVERAGE_BLOCKERS]
    return blockers


def handoff_needed(verdict):
    return any(blocker in FIXABLE_BLOCKER_HINTS for blocker in actionable_blockers(verdict))


def compact_shadow_queue_items(items, limit=5):
    compact = []
    for item in (items or [])[:limit]:
        evidence = item.get("evidence") or {}
        readiness = evidence.get("readiness_impact_upper_bound") or {}
        compact.append({
            "candidate_id": item.get("candidate_id"),
            "hypothesis_source": item.get("hypothesis_source"),
            "expected_capture_stage_improved": item.get("expected_capture_stage_improved"),
            "next_action": item.get("next_action"),
            "context_blockers": item.get("context_blockers") or [],
            "human_approval_required_if_fix_requires": (
                item.get("human_approval_required_if_fix_requires")
            ),
            "evidence": {
                "cluster": evidence.get("cluster"),
                "event_count": evidence.get("event_count"),
                "unique_tokens": evidence.get("unique_tokens"),
                "candidate_cluster_match_count": evidence.get("candidate_cluster_match_count"),
                "candidate_family": evidence.get("candidate_family"),
                "share_of_raw_all_gold_silver": evidence.get("share_of_raw_all_gold_silver"),
                "events_contributing_to_60pct_gap_upper_bound": readiness.get(
                    "events_contributing_to_60pct_gap_upper_bound"
                ),
            },
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    return compact


def compact_pass_allow_queue_items(items, limit=5):
    compact = []
    for item in (items or [])[:limit]:
        if not isinstance(item, dict):
            continue
        current_evidence = item.get("current_window_evidence") or {}
        compact.append({
            "priority_rank": item.get("priority_rank"),
            "priority_bucket": item.get("priority_bucket"),
            "plan_item_id": item.get("plan_item_id") or item.get("source_plan_item_id"),
            "freeze_id": item.get("freeze_id"),
            "candidate_id": item.get("candidate_id"),
            "cluster": item.get("cluster"),
            "dimension": item.get("dimension"),
            "slice_value": item.get("slice_value"),
            "expected_capture_stage_improved": item.get("expected_capture_stage_improved"),
            "events_contributing_to_current_60pct_gap_upper_bound": (
                item.get("events_contributing_to_current_60pct_gap_upper_bound")
                if item.get("events_contributing_to_current_60pct_gap_upper_bound") is not None
                else current_evidence.get("events_contributing_to_current_60pct_gap_upper_bound")
            ),
            "non_dedup_upper_bound_event_count": item.get("non_dedup_upper_bound_event_count"),
            "matched_gold_silver_events": item.get("matched_gold_silver_events"),
            "pass_allow_lift": item.get("pass_allow_lift"),
            "status": item.get("status"),
            "next_action": item.get("next_action"),
            "context_blockers": item.get("context_blockers") or [],
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        })
    return compact


def compact_kline_coverage_resolution(audit):
    if not isinstance(audit, dict) or not audit:
        return {}
    formal = audit.get("formal_kline_coverage") or {}
    research = audit.get("research_recoverability") or {}
    low_confidence = audit.get("low_confidence_capture_summary") or {}
    tracks = []
    for row in audit.get("allowed_resolution_tracks") or []:
        if not isinstance(row, dict):
            continue
        tracks.append({
            "track": row.get("track"),
            "allowed_use": row.get("allowed_use"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "recoverable_rows": row.get("recoverable_rows"),
            "row_count": row.get("row_count"),
            "recoverable_known_rate": row.get("recoverable_known_rate"),
        })
    return {
        "available": audit.get("available", True),
        "classification": (audit.get("overall") or {}).get("classification"),
        "next_action": (audit.get("overall") or {}).get("next_action"),
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "formal_denominator_changed": bool(audit.get("formal_denominator_changed")),
        "formal_coverage_rate": formal.get("coverage_rate"),
        "formal_covered_rows": formal.get("covered_rows"),
        "formal_target_80pct_count": formal.get("target_80pct_count"),
        "formal_additional_rows_needed_to_80pct": formal.get(
            "additional_rows_needed_to_80pct"
        ),
        "confidence_adjusted_research_coverage_rate": research.get(
            "confidence_adjusted_research_coverage_rate"
        ),
        "confidence_adjusted_research_covered_rows": research.get(
            "confidence_adjusted_research_covered_rows"
        ),
        "time_legal_recoverable_reaches_80pct": research.get(
            "time_legal_recoverable_reaches_80pct"
        ),
        "low_confidence_event_rows": low_confidence.get("event_rows"),
        "low_confidence_candidate_matched_any_rate": low_confidence.get(
            "candidate_matched_any_rate"
        ),
        "allowed_resolution_tracks": tracks,
    }


def build_handoff(verdict):
    blockers = verdict.get("blockers") or []
    actionable = actionable_blockers(verdict)
    needed = handoff_needed(verdict)
    parallel_action = verdict.get("parallel_next_action")
    parallel_reason = verdict.get("parallel_next_action_reason")
    display_next_blocker = verdict.get("next_highest_priority_blocker")
    if quote_clean_window_pending(verdict) and display_next_blocker in QUOTE_COVERAGE_BLOCKERS:
        display_next_blocker = actionable[0] if actionable else None
    lines = [
        "# Gold/Silver Capture Discovery Codex Handoff",
        "",
        f"- schema_version: `{SCHEMA_VERSION}`",
        f"- generated_at: `{utc_now()}`",
        f"- current_commit: `{verdict.get('current_commit')}`",
        f"- deployment_commit: `{verdict.get('deployment_commit')}`",
        f"- verdict: `{verdict.get('classification')}`",
        f"- blocked_subtype: `{verdict.get('blocked_subtype')}`",
        f"- next_action: `{verdict.get('next_action')}`",
        f"- parallel_next_action: `{parallel_action}`",
        f"- parallel_next_action_reason: `{parallel_reason}`",
        f"- promotion_allowed: `{str(bool(verdict.get('promotion_allowed'))).lower()}`",
        f"- human_action_required: `{str(bool(verdict.get('human_action_required'))).lower()}`",
        f"- next_highest_priority_blocker: `{display_next_blocker}`",
        f"- non_quote_sensitive_capture_discovery_allowed: `{str(bool(verdict.get('non_quote_sensitive_capture_discovery_allowed'))).lower()}`",
        f"- quote_sensitive_slices_blocked: `{str(bool(verdict.get('quote_sensitive_slices_blocked'))).lower()}`",
        f"- formal_volume_sensitive_slices_blocked: `{str(bool(verdict.get('formal_volume_sensitive_slices_blocked'))).lower()}`",
        f"- shadow_matured_volume_slices_evaluable: `{str(bool(verdict.get('shadow_matured_volume_slices_evaluable'))).lower()}`",
        f"- quote_writer_fix_status: `{verdict.get('quote_writer_fix_status')}`",
        f"- quote_clean_window_status: `{verdict.get('quote_clean_window_status')}`",
        f"- quote_clean_window_eta_iso: `{verdict.get('quote_clean_window_eta_iso')}`",
        f"- context_field_writer_fix_status: `{verdict.get('context_field_writer_fix_status')}`",
        f"- context_clean_window_pending: `{str(bool(verdict.get('context_clean_window_pending'))).lower()}`",
        f"- context_clean_window_eta_iso: `{verdict.get('context_clean_window_eta_iso')}`",
        f"- lifecycle_clean_window_pending: `{str(bool(verdict.get('lifecycle_clean_window_pending'))).lower()}`",
        f"- source_component_clean_window_pending: `{str(bool(verdict.get('source_component_clean_window_pending'))).lower()}`",
        f"- current_capture_stage: `{verdict.get('current_capture_stage')}`",
        f"- mode_status: `{verdict.get('mode_status')}`",
        f"- mode_reason: `{verdict.get('mode_reason')}`",
        f"- raw_gs_events: `{verdict.get('raw_gs_events')}`",
        f"- candidate_matched_any: `{verdict.get('candidate_matched_any')}`",
        f"- has_decision_record: `{verdict.get('has_decision_record')}`",
        f"- pass_allow: `{verdict.get('pass_allow')}`",
        f"- pending_entry: `{verdict.get('pending_entry')}`",
        f"- reached_final_entry_contract: `{verdict.get('reached_final_entry_contract')}`",
        f"- final_entry_block_mode_disabled: `{verdict.get('final_entry_block_mode_disabled')}`",
        f"- paper_trade_intent: `{verdict.get('paper_trade_intent')}`",
        f"- paper_trade_committed: `{verdict.get('paper_trade_committed')}`",
        f"- final_eligibility_capture_rate: `{verdict.get('final_eligibility_capture_rate')}`",
        f"- paper_trade_intent_rate: `{verdict.get('paper_trade_intent_rate')}`",
        f"- paper_capture_rate: `{verdict.get('paper_capture_rate')}`",
        f"- capture_60_gap_classification: `{(verdict.get('capture_60_target_loop') or {}).get('classification') or verdict.get('capture_60_gap_classification')}`",
        f"- capture_60_biggest_gap_stage: `{(verdict.get('capture_60_target_loop') or {}).get('biggest_gap_stage')}`",
        f"- capture_60_target_shortfall_stage: `{(verdict.get('capture_60_target_loop') or {}).get('target_shortfall_stage')}`",
        f"- capture_60_largest_transition_dropoff: `{json.dumps((verdict.get('capture_60_target_loop') or {}).get('largest_transition_dropoff') or {}, sort_keys=True)}`",
        f"- capture_60_additional_count_needed: `{(verdict.get('capture_60_target_loop') or {}).get('additional_count_needed_to_60')}`",
        f"- capture_60_next_best_allowed_action: `{(verdict.get('capture_60_target_loop') or {}).get('next_best_allowed_action')}`",
        f"- capture_60_current_target_stage: `{(verdict.get('capture_60_target_loop') or {}).get('current_target_stage')}`",
        f"- capture_60_current_target_count: `{(verdict.get('capture_60_target_loop') or {}).get('current_target_count')}`",
        f"- capture_60_current_target_rate: `{(verdict.get('capture_60_target_loop') or {}).get('current_target_rate')}`",
        f"- capture_60_current_target_additional_count_needed: `{(verdict.get('capture_60_target_loop') or {}).get('current_target_additional_count_needed_to_60')}`",
        f"- capture_60_current_target_next_best_allowed_action: `{(verdict.get('capture_60_target_loop') or {}).get('current_target_next_best_allowed_action')}`",
        f"- decision_capture_60_gap_classification: `{(verdict.get('decision_capture_60_gap_audit') or {}).get('classification')}`",
        f"- decision_capture_60_shadow_bridge_mirror_complete: `{(verdict.get('decision_capture_60_gap_audit') or {}).get('shadow_bridge_mirror_complete')}`",
        f"- decision_capture_60_optimistic_rate_if_shadow_gap_logged: `{(verdict.get('decision_capture_60_gap_audit') or {}).get('optimistic_decision_record_rate_if_shadow_gap_logged')}`",
        f"- handoff_needed: `{str(needed).lower()}`",
        "",
        "## Guardrails",
        "",
        "- Do not change strategy, entry policy, hard gates, exit gates, live executor, canary size, wallet config, or risk settings.",
        "- Only fix data, report, evaluator, API, or test issues needed to make the discovery verdict auditable.",
        "- Same-window discovery output cannot promote live trading.",
        "",
        "## Current Integrity Snapshot",
        "",
        f"- candidate_count_expected: `{verdict.get('candidate_count_expected')}`",
        f"- candidate_count_observed: `{verdict.get('candidate_count_observed')}`",
        f"- observation_coverage_pct: `{verdict.get('observation_coverage_pct')}`",
        f"- raw_dog_rows_complete: `{str(bool(verdict.get('raw_dog_rows_complete'))).lower()}`",
        f"- signal_id_join_rate: `{verdict.get('signal_id_join_rate')}`",
        f"- raw_all_signal_id_join_rate: `{verdict.get('raw_all_signal_id_join_rate')}`",
        f"- mesh_eligible_signal_id_join_rate: `{verdict.get('mesh_eligible_signal_id_join_rate')}`",
        "",
    ]
    if verdict.get("capture_60_target_loop"):
        shadow_queue = verdict.get("shadow_candidate_improvement_queue") or {}
        shadow_top_items = (
            shadow_queue.get("top_items")
            or shadow_queue.get("top_opportunities")
            or []
        )
        lines.extend([
            "## Capture 60 Target Loop",
            "",
            "The `target_shortfall_stage` is the first stage below 60%; `largest_transition_dropoff` is the largest adjacent funnel loss. Treat them as separate shadow-only audit tracks when they differ.",
            "",
            "```json",
            json.dumps(
                {
                    "capture_60_target_loop": verdict.get("capture_60_target_loop") or {},
                    "context_dimension_eligibility": {
                        key: (verdict.get("context_dimension_eligibility") or {}).get(key)
                        for key in (
                            "status_counts",
                            "clean_dimensions",
                            "blocked_dimensions",
                        )
                    },
                    "pending_to_final_entry_audit_v3": {
                        key: (verdict.get("pending_to_final_entry_audit_v3") or {}).get(key)
                        for key in (
                            "dropoff_counts",
                            "pending_no_final_entry_classification",
                            "largest_transition_dropoff_review",
                            "stale_before_final_review",
                            "promotion_allowed",
                        )
                    },
                    "shadow_candidate_improvement_queue": {
                        key: shadow_queue.get(key)
                        for key in (
                            "classification",
                            "evidence_level",
                            "next_action",
                            "queue_count",
                            "source_counts",
                            "top_next_actions",
                            "promotion_allowed",
                        )
                    },
                    "shadow_candidate_improvement_top_items": compact_shadow_queue_items(
                        shadow_top_items,
                        limit=5,
                    ),
                    "oos_readiness_summary_v3": verdict.get("oos_readiness_summary_v3") or {},
                },
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
        ])
    pass_allow_priority = verdict.get("pass_allow_60_closure_priority_queue") or {}
    pass_allow_freeze_priority = verdict.get("pass_allow_60_oos_freeze_priority_queue") or {}
    pass_allow_oos_queue = verdict.get("pass_allow_60_closure_oos_queue") or {}
    if pass_allow_priority or pass_allow_freeze_priority or pass_allow_oos_queue:
        lines.extend([
            "## Pass Allow 60 Closure Priority Queue",
            "",
            "This queue is shadow-only. It identifies the next evidence to collect for closing the pass_allow gap to 60%; it does not authorize strategy, gate, final_entry_contract, A_CLASS, executor, paper/live, canary, wallet, or risk changes.",
            "",
            "```json",
            json.dumps(
                {
                    "closure_priority_queue": {
                        "available": pass_allow_priority.get("available"),
                        "classification": pass_allow_priority.get("classification"),
                        "additional_count_needed_to_60": pass_allow_priority.get(
                            "additional_count_needed_to_60"
                        ),
                        "priority_queue_count": pass_allow_priority.get("priority_queue_count"),
                        "research_only_priority_queue_count": pass_allow_priority.get(
                            "research_only_priority_queue_count"
                        ),
                        "formal_blocked_count": pass_allow_priority.get("formal_blocked_count"),
                        "oos_freeze_ready_count": pass_allow_priority.get("oos_freeze_ready_count"),
                        "next_oos_action": pass_allow_priority.get("next_oos_action"),
                        "promotion_allowed": False,
                    },
                    "top_priority_items": compact_pass_allow_queue_items(
                        pass_allow_priority.get("top_priority_items") or [],
                        limit=5,
                    ),
                    "research_only_top_priority_items": compact_pass_allow_queue_items(
                        pass_allow_priority.get("research_only_top_priority_items") or [],
                        limit=5,
                    ),
                    "oos_freeze_priority_queue": {
                        "available": pass_allow_freeze_priority.get("available"),
                        "classification": pass_allow_freeze_priority.get("classification"),
                        "priority_queue_count": pass_allow_freeze_priority.get(
                            "priority_queue_count"
                        ),
                        "frozen_definition_count": pass_allow_freeze_priority.get(
                            "frozen_definition_count"
                        ),
                        "unique_priority_plan_item_count": pass_allow_freeze_priority.get(
                            "unique_priority_plan_item_count"
                        ),
                        "top_priority_items": compact_pass_allow_queue_items(
                            pass_allow_freeze_priority.get("top_priority_items") or [],
                            limit=5,
                        ),
                        "promotion_allowed": False,
                    },
                    "closure_oos_queue": {
                        "classification": pass_allow_oos_queue.get("classification"),
                        "queue_count": (
                            verdict.get("pass_allow_60_closure_oos_queue_count")
                            or pass_allow_oos_queue.get("queue_count")
                        ),
                        "next_action": (
                            verdict.get("next_pass_allow_60_closure_oos_action")
                            or pass_allow_oos_queue.get("next_action")
                        ),
                        "blocked_until": pass_allow_oos_queue.get("blocked_until"),
                        "decision_no_pass_cluster_count": pass_allow_oos_queue.get(
                            "decision_no_pass_cluster_count"
                        ),
                        "clean_2d_pass_allow_lift_slice_count": pass_allow_oos_queue.get(
                            "clean_2d_pass_allow_lift_slice_count"
                        ),
                        "promotion_allowed": False,
                    },
                },
                indent=2,
                sort_keys=True,
            )[:12000],
            "```",
            "",
        ])
    kline_resolution_summary = compact_kline_coverage_resolution(
        verdict.get("kline_coverage_resolution_audit") or {}
    )
    if kline_resolution_summary:
        lines.extend([
            "## Kline Coverage Resolution",
            "",
            "Formal kline-sensitive evidence remains blocked unless coverage reaches the required threshold. Research recovery tracks are shadow-only and do not change the formal denominator.",
            "",
            "```json",
            json.dumps(kline_resolution_summary, indent=2, sort_keys=True),
            "```",
            "",
        ])
    quote = verdict.get("quote_clean_definition") or {}
    lines.extend([
        "## Schema / Quote State",
        "",
        f"- context_schema_version_counts: `{json.dumps(verdict.get('context_schema_version_counts') or {}, sort_keys=True)}`",
        f"- quote_clean_definition: `{json.dumps(quote, sort_keys=True)}`",
        "",
    ])
    if quote_clean_window_pending(verdict):
        lines.extend([
            "## Quote Clean Window",
            "",
            "Post-deploy quote writer coverage is verified. The remaining quote blocker is from older rows still inside the rolling window, so this is not a writer-fix handoff.",
            "",
            f"- clean_window_status: `{verdict.get('quote_clean_window_status')}`",
            f"- estimated_clean_at: `{verdict.get('quote_clean_window_eta_iso')}`",
            f"- seconds_until_natural_clean_window: `{verdict.get('quote_clean_window_seconds_remaining')}`",
            "",
            "Next action: continue non-quote-sensitive discovery, wait for the clean window, then rerun AutoLoop before evaluating quote-sensitive slices.",
            "",
        ])
    if verdict.get("lifecycle_clean_window_pending") or verdict.get("source_component_clean_window_pending"):
        monitor = verdict.get("context_blocker_monitor") or {}
        field_audit = monitor.get("context_field_coverage_audit") or {}
        lines.extend([
            "## Context Field Clean Window",
            "",
            "Post-deploy lifecycle/source-component context writing is verified. The remaining lifecycle/source_component blockers are older rows still inside the rolling window, not a new writer-fix handoff.",
            "",
            f"- context_field_writer_fix_status: `{verdict.get('context_field_writer_fix_status')}`",
            f"- context_clean_window_pending: `{str(bool(verdict.get('context_clean_window_pending'))).lower()}`",
            f"- estimated_clean_at: `{verdict.get('context_clean_window_eta_iso')}`",
            f"- seconds_until_natural_clean_window: `{verdict.get('context_clean_window_seconds_remaining')}`",
            f"- lifecycle_clean_window_pending: `{str(bool(verdict.get('lifecycle_clean_window_pending'))).lower()}`",
            f"- source_component_clean_window_pending: `{str(bool(verdict.get('source_component_clean_window_pending'))).lower()}`",
            "",
            "```json",
            json.dumps(
                {
                    "context_field_status": field_audit.get("classification"),
                    "blockers": field_audit.get("blockers") or [],
                    "warnings": field_audit.get("warnings") or [],
                    "lifecycle_profile": {
                        "effective_present_rate": ((field_audit.get("lifecycle_profile") or {}).get("effective_present_rate")),
                        "rows_needed_to_80pct": ((field_audit.get("lifecycle_profile") or {}).get("rows_needed_to_80pct")),
                        "mature_effective_present_rate": (((field_audit.get("lifecycle_profile") or {}).get("mature_context") or {}).get("effective_present_rate")),
                    },
                    "source_component": {
                        "effective_present_rate": ((field_audit.get("source_component") or {}).get("effective_present_rate")),
                        "rows_needed_to_80pct": ((field_audit.get("source_component") or {}).get("rows_needed_to_80pct")),
                        "mature_effective_present_rate": (((field_audit.get("source_component") or {}).get("mature_context") or {}).get("effective_present_rate")),
                    },
                },
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
            "Next action: continue non-context-sensitive discovery, wait for the clean window, then rerun AutoLoop before evaluating lifecycle/source_component-sensitive slices.",
            "",
        ])
    if parallel_action:
        lines.extend([
            "## Parallel Read-only Action",
            "",
            f"- action: `{parallel_action}`",
            f"- reason: `{parallel_reason}`",
            "- allowed scope: read-only evaluator/report/dashboard instrumentation and shadow-only context evidence.",
            "- forbidden scope: strategy, entry policy, hard/exit gates, final_entry_contract, A_CLASS mode, executor, paper/live enablement, canary size, wallet, or risk.",
            "",
        ])
    if not needed:
        lines.extend([
            "## Next Action",
            "",
            (
                "No blocking Codex data/report/evaluator fix is required by the primary verdict. "
                "Continue collecting clean discovery data and rerun the loop."
            ),
            "",
        ])
    else:
        lines.extend(["## Required Fixes", ""])
        for blocker in actionable:
            hint = FIXABLE_BLOCKER_HINTS.get(blocker)
            if hint:
                lines.append(f"- `{blocker}`: {hint}")
        lines.append("")
    volume_state = verdict.get("volume_profile_blocker_state") or {}
    show_volume_kline = (
        any(blocker in {"volume_profile_coverage_below_80pct", "kline_coverage_below_80pct"} for blocker in actionable)
        or bool(volume_state)
    )
    if show_volume_kline:
        volume_kline = verdict.get("volume_kline_root_cause_audit") or {}
        matured_recheck = verdict.get("matured_kline_volume_recheck_audit") or {}
        matured_cross = verdict.get("matured_volume_capture_cross_audit") or {}
        volume_context = volume_kline.get("volume_context") or {}
        volume_resolution = volume_kline.get("volume_context_resolution") or {}
        raw_kline = volume_kline.get("raw_gold_silver_kline") or {}
        compact_volume_kline = {
            "overall": volume_kline.get("overall") or {},
            "volume_context": {
                key: volume_context.get(key)
                for key in (
                    "rows_scanned",
                    "field_present_rate",
                    "known_rate",
                    "missing_rate",
                    "unknown_rate",
                    "blocker",
                    "root_causes",
                    "recent_windows",
                )
            },
            "volume_context_resolution": {
                key: volume_resolution.get(key)
                for key in (
                    "classification",
                    "next_action",
                    "formal_volume_profile_known_rate",
                    "formal_volume_profile_unknown_rate",
                    "writer_field_present",
                    "primary_unknown_reason",
                    "matured_volume_shadow_recheck_recommended",
                    "allowed_use",
                    "promotion_allowed",
                )
            },
            "raw_gold_silver_kline": {
                key: raw_kline.get(key)
                for key in (
                    "raw_all_gold_silver_event_rows",
                    "kline_coverage_rate",
                    "kline_covered_rows",
                    "kline_uncovered_rows",
                    "kline_uncovered_root_cause_counts",
                    "low_confidence_research_audit",
                    "blocker",
                )
            },
            "matured_kline_volume_recheck": {
                "overall": matured_recheck.get("overall") or {},
                "context_rows_scanned": matured_recheck.get("context_rows_scanned"),
                "unknown_or_missing_rows": matured_recheck.get("unknown_or_missing_rows"),
                "recheck": matured_recheck.get("recheck") or {},
            },
            "matured_volume_capture_cross": {
                "overall": matured_cross.get("overall") or {},
                "denominator": matured_cross.get("denominator") or {},
                "signal_id_reconciliation": matured_cross.get("signal_id_reconciliation") or {},
                "matured_volume_context": matured_cross.get("matured_volume_context") or {},
                "h1_matured_building_volume": matured_cross.get("h1_matured_building_volume") or {},
                "watch_slice_count": matured_cross.get("watch_slice_count"),
                "next_research_action": matured_cross.get("next_research_action"),
            },
            "volume_profile_blocker_state": volume_state,
        }
        lines.extend([
            "## Volume / Kline Root Cause",
            "",
        ])
        if volume_resolution:
            lines.extend([
                "Volume context resolution:",
                "",
                "```json",
                json.dumps(
                    {
                        "volume_context_resolution": compact_volume_kline[
                            "volume_context_resolution"
                        ],
                    },
                    indent=2,
                    sort_keys=True,
                ),
                "```",
                "",
            ])
        lines.extend([
            "```json",
            json.dumps(compact_volume_kline, indent=2, sort_keys=True)[:12000],
            "```",
            "",
        ])
    matured_cross = verdict.get("matured_volume_capture_cross_audit") or {}
    matured_queue = verdict.get("matured_volume_watch_queue") or {}
    if matured_cross or matured_queue:
        matured_context = matured_cross.get("matured_volume_context") or {}
        h1 = matured_cross.get("h1_matured_building_volume") or {}
        matured_payload = {
            "summary": {
                "classification": (matured_cross.get("overall") or {}).get("classification"),
                "next_action": (matured_cross.get("overall") or {}).get("next_action"),
                "h1_status": h1.get("status"),
                "matured_volume_known_rate": matured_context.get("known_rate"),
                "signals_with_matured_context": matured_context.get("signals_with_matured_context"),
                "formal_denominator_changed": bool(matured_cross.get("formal_denominator_changed")),
                "watch_queue_classification": matured_queue.get("classification"),
                "watch_queue_count": matured_queue.get("queue_count"),
                "promotion_allowed": False,
                "strategy_change_allowed": False,
                "automatic_runtime_change_allowed": False,
                "paper_enablement_allowed": False,
                "allowed_use": "shadow_only_matured_volume_context",
            },
            "matured_volume_profile_counts": matured_context.get("profile_counts") or {},
            "h1_matured_building_volume": h1,
            "watch_queue_items": (matured_queue.get("items") or [])[:8],
            "interpretation": (
                "Matured-volume rows are delayed-context discovery evidence while formal volume/kline coverage is blocked. "
                "They can guide shadow-only watch/OOS collection but cannot promote runtime strategy."
            ),
        }
        lines.extend([
            "## Matured Volume Shadow Validation",
            "",
            "```json",
            json.dumps(matured_payload, indent=2, sort_keys=False)[:12000],
            "```",
            "",
        ])
    two_d_cross = verdict.get("two_d_cross_validity_summary") or {}
    if two_d_cross:
        cross_freeze = verdict.get("capture_cross_oos_freeze_registry") or {}
        compact_two_d = {
            "classification": two_d_cross.get("classification"),
            "next_action": two_d_cross.get("next_action"),
            "evidence_level": two_d_cross.get("evidence_level"),
            "valid_cross_count": two_d_cross.get("valid_cross_count"),
            "invalid_cross_count": two_d_cross.get("invalid_cross_count"),
            "shadow_matured_volume_cross_count": two_d_cross.get("shadow_matured_volume_cross_count"),
            "discovery_hit_count": two_d_cross.get("discovery_hit_count"),
            "watch_count": two_d_cross.get("watch_count"),
            "valid_cross_judgment_counts": two_d_cross.get("valid_cross_judgment_counts") or {},
            "same_window_discovery_only": two_d_cross.get("same_window_discovery_only"),
            "oos_required_before_promotion": two_d_cross.get("oos_required_before_promotion"),
            "oos_freeze_registry": {
                "available": bool(cross_freeze),
                "classification": cross_freeze.get("classification"),
                "next_action": cross_freeze.get("next_action"),
                "frozen_definition_count": cross_freeze.get("frozen_definition_count"),
                "stage_counts": cross_freeze.get("stage_counts") or {},
            },
            "promotion_allowed": False,
        }
        lines.extend([
            "## Capture-First 2D Cross",
            "",
            "```json",
            json.dumps(compact_two_d, indent=2, sort_keys=True),
            "```",
            "",
        ])
    reconciliation = verdict.get("signal_identity_reconciliation") or {}
    if reconciliation:
        lines.extend([
            "## Signal Identity Reconciliation",
            "",
            "```json",
            json.dumps(
                {
                    key: reconciliation.get(key)
                    for key in (
                        "joined_exact_signal_id",
                        "joined_by_signal_alias",
                        "joined_by_lifecycle_id",
                        "joined_by_token_time_high_confidence",
                        "outside_candidate_observer_window",
                        "not_mesh_eligible",
                        "missing_candidate_observation",
                        "raw_event_duplicate",
                        "raw_event_derived_no_signal",
                        "unknown_unjoined",
                        "raw_all_signal_id_join_rate",
                        "mesh_eligible_signal_id_join_rate",
                    )
                },
                indent=2,
                sort_keys=True,
            ),
            "```",
            "",
        ])
    quote_context = verdict.get("quote_context_coverage") or {}
    if quote_context:
        compact_quote_context = {
            key: quote_context.get(key)
            for key in (
                "coverage_denominator_type",
                "coverage_denominator_rows",
                "context_carrier_candidate_ids",
                "source_quote_clean_present_rate",
                "source_quote_executable_present_rate",
                "source_quote_clean_true_rate",
                "source_quote_clean_false_rate",
                "source_quote_clean_missing_rate",
                "source_quote_clean_unknown_rate",
                "source_quote_clean_not_applicable_rate",
                "source_quote_executable_true_rate",
                "source_quote_executable_false_rate",
                "source_quote_executable_missing_rate",
                "source_quote_executable_unknown_rate",
                "source_quote_executable_not_applicable_rate",
            )
        }
        compact_quote_context["breakdowns"] = {
            key: quote_context.get("breakdowns", {}).get(key)
            for key in (
                "by_context_schema_version",
                "by_source_component",
                "by_signal_type",
                "by_writer_path",
                "by_candidate_family",
                "by_lifecycle_profile",
                "by_context_carrier_candidate_id",
            )
        }
        lines.extend([
            "## Quote Context Coverage",
            "",
            "```json",
            json.dumps(compact_quote_context, indent=2, sort_keys=True),
            "```",
            "",
        ])
    quote_missing = verdict.get("quote_missing_root_cause") or {}
    if quote_missing:
        compact_quote_missing = {
            key: quote_missing.get(key)
            for key in (
                "quote_missing_rows_total",
                "missing_by_context_schema_version",
                "missing_by_source_component",
                "missing_by_signal_type",
                "missing_by_writer_path",
                "missing_by_lifecycle_profile",
                "missing_by_payload_key_presence",
                "missing_due_to_legacy_schema_count",
                "missing_due_to_writer_path_count",
                "missing_should_be_not_applicable_count",
                "missing_unknown_count",
                "dominant_root_cause",
            )
        }
        lines.extend([
            "## Quote Missing Root Cause",
            "",
            "```json",
            json.dumps(compact_quote_missing, indent=2, sort_keys=True),
            "```",
            "",
        ])
    hypothesis_validation = verdict.get("hypothesis_validation_audit") or {}
    if hypothesis_validation:
        compact_hypothesis_validation = {
            "available": hypothesis_validation.get("available"),
            "overall": hypothesis_validation.get("overall") or {},
            "promotion_allowed": False,
            "matured_volume_hypothesis_validation": (
                hypothesis_validation.get("matured_volume_hypothesis_validation") or {}
            ),
        }
        lines.extend([
            "## Hypothesis Validation",
            "",
            "```json",
            json.dumps(compact_hypothesis_validation, indent=2, sort_keys=True),
            "```",
            "",
        ])
    runtime_health = verdict.get("runtime_health_snapshot") or {}
    if runtime_health:
        signal_source = runtime_health.get("signal_source_freshness") or {}
        paper_review = runtime_health.get("paper_review_snapshot") or {}
        paper_fast_lane = runtime_health.get("paper_fast_lane_health") or {}
        observer_logs = runtime_health.get("observer_logs") or {}
        compact_runtime_health = {
            "available": runtime_health.get("available"),
            "status": runtime_health.get("status"),
            "warnings": runtime_health.get("warnings") or [],
            "blockers": runtime_health.get("blockers") or [],
            "signal_source_freshness": {
                "status": signal_source.get("status"),
                "age_minutes": signal_source.get("age_minutes"),
                "fail_closed": signal_source.get("fail_closed"),
                "latest_iso": signal_source.get("latest_iso"),
            },
            "paper_review_snapshot": {
                "status": paper_review.get("status"),
                "age_minutes": paper_review.get("age_minutes"),
                "max_age_minutes": paper_review.get("max_age_minutes"),
                "warnings": paper_review.get("warnings") or [],
            },
            "paper_fast_lane_health": {
                "status": paper_fast_lane.get("status"),
                "age_minutes": paper_fast_lane.get("age_minutes"),
                "max_age_minutes": paper_fast_lane.get("max_age_minutes"),
                "warnings": paper_fast_lane.get("warnings") or [],
            },
            "paper_db_status": (runtime_health.get("paper_db") or {}).get("status"),
            "observer_logs_status": observer_logs.get("status"),
            "runtime_final_evidence_status": (
                runtime_health.get("runtime_final_evidence") or {}
            ).get("status"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
        }
        lines.extend([
            "## Runtime Health Snapshot",
            "",
            "```json",
            json.dumps(compact_runtime_health, indent=2, sort_keys=True),
            "```",
            "",
        ])
    oos_readiness = verdict.get("oos_readiness_summary") or {}
    if oos_readiness:
        pass_allow_post_freeze = (
            oos_readiness.get("pass_allow_60_post_freeze_oos_validation")
            or (verdict.get("oos_readiness_summary_v3") or {}).get("pass_allow_60_post_freeze_oos_validation")
            or verdict.get("pass_allow_60_post_freeze_oos_validation")
            or {}
        )
        compact_oos = {
            "classification": oos_readiness.get("classification"),
            "available_probe_count": oos_readiness.get("available_probe_count"),
            "sufficient_probe_count": oos_readiness.get("sufficient_probe_count"),
            "oos_repeated_watch_probe_count": oos_readiness.get("oos_repeated_watch_probe_count"),
            "next_action": oos_readiness.get("next_action"),
            "readiness_delta": oos_readiness.get("readiness_delta") or {},
            "pass_allow_60_post_freeze": {
                "classification": pass_allow_post_freeze.get("classification"),
                "raw_gold_silver_event_rows": pass_allow_post_freeze.get(
                    "raw_gold_silver_event_rows"
                ),
                "post_freeze_usable_hours": pass_allow_post_freeze.get(
                    "post_freeze_usable_hours"
                ),
                "repeat_watch_count": pass_allow_post_freeze.get("repeat_watch_count"),
                "all_raw_rows_since_eval_start": pass_allow_post_freeze.get(
                    "all_raw_rows_since_eval_start"
                ),
                "global_pass_allow_count": pass_allow_post_freeze.get("global_pass_allow_count"),
                "candidate_observation_meta": pass_allow_post_freeze.get("candidate_observation_meta") or {},
                "candidate_observation_effective_status": pass_allow_post_freeze.get(
                    "candidate_observation_effective_status"
                ),
                "candidate_observation_join_blocked": pass_allow_post_freeze.get(
                    "candidate_observation_join_blocked"
                ),
                "post_freeze_oos_wait_reason": pass_allow_post_freeze.get(
                    "post_freeze_oos_wait_reason"
                ),
                "raw_signal_rows_seen_after_freeze": pass_allow_post_freeze.get(
                    "raw_signal_rows_seen_after_freeze"
                ),
                "oos_data_next_action": pass_allow_post_freeze.get("oos_data_next_action"),
                "oos_data_availability": pass_allow_post_freeze.get(
                    "oos_data_availability"
                ) or {},
                "promotion_allowed": False,
            },
            "promotion_allowed": False,
            "probes": (oos_readiness.get("probes") or [])[:4],
        }
        lines.extend([
            "## OOS Readiness",
            "",
            "```json",
            json.dumps(compact_oos, indent=2, sort_keys=True),
            "```",
            "",
        ])
    candidate_improvement = verdict.get("candidate_improvement_opportunities_summary") or {}
    if candidate_improvement:
        compact_candidate_improvement = {
            "opportunity_count": candidate_improvement.get("opportunity_count"),
            "opportunity_type_counts": candidate_improvement.get("opportunity_type_counts") or {},
            "blocked_context_opportunity_count": candidate_improvement.get("blocked_context_opportunity_count"),
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "top_opportunities": (candidate_improvement.get("top_opportunities") or [])[:5],
        }
        lines.extend([
            "## Candidate Improvement Opportunities",
            "",
            "```json",
            json.dumps(compact_candidate_improvement, indent=2, sort_keys=True),
            "```",
            "",
        ])
    markov_information = verdict.get("Markov_effectiveness_summary") or {}
    if markov_information:
        compact_markov_information = {
            "status": markov_information.get("status"),
            "next_action": markov_information.get("next_action"),
            "usage": markov_information.get("usage"),
            "promotion_allowed": False,
            "total_green_buckets": markov_information.get("total_green_buckets"),
            "total_yellow_buckets": markov_information.get("total_yellow_buckets"),
            "total_insufficient_buckets": markov_information.get("total_insufficient_buckets"),
            "non_informative_reasons": markov_information.get("non_informative_reasons") or {},
            "recommended_shadow_profiles": (markov_information.get("recommended_shadow_profiles") or [])[:8],
            "profile_diagnostics": {
                name: {
                    "status": row.get("status"),
                    "closed_virtual_rows": row.get("closed_virtual_rows"),
                    "keys_emitted": row.get("keys_emitted"),
                    "bucket_counts": row.get("bucket_counts") or {},
                    "context_blockers_affecting_profile": row.get("context_blockers_affecting_profile") or [],
                }
                for name, row in sorted((markov_information.get("profile_diagnostics") or {}).items())
            },
        }
        lines.extend([
            "## Markov Information Value",
            "",
            "```json",
            json.dumps(compact_markov_information, indent=2, sort_keys=True),
            "```",
            "",
        ])
    entry_gap = verdict.get("entry_funnel_gap_summary") or {}
    readiness_shortfall = verdict.get("readiness_shortfall_summary") or {}
    paper_proposal = verdict.get("paper_entry_proposal_readiness") or {}
    if readiness_shortfall or paper_proposal:
        lines.extend([
            "## Capture Readiness Shortfall",
            "",
            "```json",
            json.dumps(
                {
                    "readiness_shortfall_summary": readiness_shortfall,
                    "paper_entry_proposal_readiness": paper_proposal,
                },
                indent=2,
                sort_keys=True,
            )[:12000],
            "```",
            "",
        ])
    if entry_gap:
        lines.extend([
            "## Entry Funnel Gap",
            "",
            "```json",
            json.dumps(entry_gap, indent=2, sort_keys=True),
            "```",
            "",
        ])
    upstream_gap = verdict.get("upstream_funnel_gap_summary") or {}
    if upstream_gap:
        lines.extend([
            "## Upstream Funnel Gap",
            "",
            "```json",
            json.dumps(upstream_gap, indent=2, sort_keys=True),
            "```",
            "",
        ])
    quality_timing = verdict.get("quality_timing_reject_research_audit") or {}
    if quality_timing:
        quality_timing_review = quality_timing.get("shadow_only_review") or {}
        quality_timing_denominator = quality_timing.get("denominator") or {}
        quality_timing_impact = quality_timing.get("readiness_impact_upper_bound") or {}
        quality_timing_reason_breakout = quality_timing.get("reason_level_breakout") or {}
        quality_timing_opportunities = quality_timing_review.get("top_research_opportunities") or []
        quality_timing_top_clusters = quality_timing.get("top_quality_timing_clusters") or [
            {
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
            }
            for row in quality_timing_opportunities[:8]
        ]
        quality_timing_dominant = quality_timing_opportunities[0] if quality_timing_opportunities else {}
        quality_timing_next_action = (
            quality_timing.get("next_action")
            or quality_timing_dominant.get("suggested_shadow_only_action")
            or ((quality_timing.get("shadow_only_next_actions") or [None])[0])
        )
        quality_timing_payload = {
            "summary": {
                "available": quality_timing.get("available"),
                "classification": quality_timing.get("classification") or quality_timing.get("verdict"),
                "verdict": quality_timing.get("verdict"),
                "next_action": quality_timing_next_action,
                "dominant_cluster": (
                    quality_timing.get("dominant_cluster")
                    or quality_timing_review.get("dominant_cluster")
                    or quality_timing_dominant.get("cluster")
                ),
                "dominant_stage": (
                    quality_timing.get("dominant_stage")
                    or quality_timing_review.get("dominant_stage")
                ),
                "quality_timing_reject_event_rows": (
                    quality_timing.get("quality_timing_reject_event_rows")
                    or quality_timing_denominator.get("quality_timing_reject_event_rows")
                ),
                "quality_timing_reject_share_of_raw_all": (
                    quality_timing.get("quality_timing_reject_share_of_raw_all")
                    or quality_timing_denominator.get("quality_timing_reject_share_of_raw_all")
                ),
                "current_final_entry_contract_rate": (
                    quality_timing.get("current_final_entry_contract_rate")
                    or quality_timing_impact.get("current_final_entry_contract_rate")
                ),
                "upper_bound_final_eligibility_rate_if_all_quality_timing_resolved": (
                    quality_timing.get(
                        "upper_bound_final_eligibility_rate_if_all_quality_timing_resolved"
                    )
                    or quality_timing_impact.get(
                        "upper_bound_final_eligibility_rate_if_all_quality_timing_resolved"
                    )
                ),
                "residual_gap_to_60pct_after_all_quality_timing_upper_bound": (
                    quality_timing.get(
                        "residual_gap_to_60pct_after_all_quality_timing_upper_bound"
                    )
                    or quality_timing_impact.get(
                        "residual_gap_to_60pct_after_all_quality_timing_upper_bound"
                    )
                ),
                "would_all_quality_timing_resolution_reach_60pct_upper_bound": (
                    quality_timing.get(
                        "would_all_quality_timing_resolution_reach_60pct_upper_bound"
                    )
                    if quality_timing.get(
                        "would_all_quality_timing_resolution_reach_60pct_upper_bound"
                    ) is not None
                    else quality_timing_impact.get(
                        "would_all_quality_timing_resolution_reach_60pct_upper_bound"
                    )
                ),
                "human_approval_required_if_fix_requires": (
                    quality_timing.get("human_approval_required_if_fix_requires")
                    or quality_timing_dominant.get("human_approval_required_if_fix_requires")
                ),
                "promotion_allowed": False,
                "strategy_change_allowed": False,
                "automatic_runtime_change_allowed": False,
                "paper_enablement_allowed": False,
            },
            "top_quality_timing_clusters": quality_timing_top_clusters[:8],
            "reason_level_breakout": {
                "classification": quality_timing_reason_breakout.get("classification"),
                "next_action": quality_timing_reason_breakout.get("next_action"),
                "dominant_cluster": quality_timing_reason_breakout.get("dominant_cluster"),
                "dominant_cluster_top_reasons": (
                    quality_timing_reason_breakout.get("dominant_cluster_top_reasons") or []
                )[:8],
                "other_quality_timing_top_reasons": (
                    quality_timing_reason_breakout.get("other_quality_timing_top_reasons") or []
                )[:8],
                "promotion_allowed": False,
                "strategy_change_allowed": False,
                "automatic_runtime_change_allowed": False,
                "paper_enablement_allowed": False,
            },
            "denominator": quality_timing_denominator,
            "candidate_match_attribution": quality_timing.get("candidate_match_attribution") or {},
            "blocked_context_dimensions_excluded_view": (
                quality_timing.get("blocked_context_dimensions_excluded_view") or {}
            ),
            "stage_attribution": quality_timing.get("stage_attribution") or {},
            "context_attribution": quality_timing.get("context_attribution") or {},
            "shadow_only_review": quality_timing.get("shadow_only_review") or {},
            "shadow_only_next_actions": quality_timing.get("shadow_only_next_actions") or [],
        }
        lines.extend([
            "## Quality / Timing Reject Research",
            "",
            "```json",
            json.dumps(
                quality_timing_payload,
                indent=2,
                sort_keys=False,
            )[:12000],
            "```",
            "",
        ])
    quality_timing_probe_validation = verdict.get("quality_timing_candidate_probe_validation") or {}
    if quality_timing_probe_validation:
        lines.extend([
            "## Quality / Timing Candidate Probe Validation",
            "",
            "```json",
            json.dumps(
                {
                    "available": quality_timing_probe_validation.get("available"),
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
                    )[:8],
                },
                indent=2,
                sort_keys=True,
            )[:12000],
            "```",
            "",
        ])
    decision_no_pass_watch_validation = verdict.get("decision_no_pass_quality_timing_watch_validation") or {}
    if decision_no_pass_watch_validation:
        lines.extend([
            "## Decision No-Pass Quality / Timing Watch Validation",
            "",
            "```json",
            json.dumps(
                {
                    "available": decision_no_pass_watch_validation.get("available"),
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
                    )[:8],
                },
                indent=2,
                sort_keys=True,
            )[:12000],
            "```",
            "",
        ])
    pending_momentum_decay_validation = verdict.get("pending_momentum_decay_recheck_validation") or {}
    if pending_momentum_decay_validation:
        lines.extend([
            "## Pending Momentum Decay Recheck Validation",
            "",
            "```json",
            json.dumps(
                {
                    "available": pending_momentum_decay_validation.get("available"),
                    "classification": pending_momentum_decay_validation.get("classification"),
                    "next_action": pending_momentum_decay_validation.get("next_action"),
                    "promotion_allowed": False,
                    "strategy_change_allowed": False,
                    "automatic_runtime_change_allowed": False,
                    "paper_enablement_allowed": False,
                    "registered_probe_count": pending_momentum_decay_validation.get(
                        "registered_probe_count"
                    ),
                    "current_cluster_event_count": pending_momentum_decay_validation.get(
                        "current_cluster_event_count"
                    ),
                    "current_cluster_unique_tokens": pending_momentum_decay_validation.get(
                        "current_cluster_unique_tokens"
                    ),
                    "validated_probe_count": pending_momentum_decay_validation.get(
                        "validated_probe_count"
                    ),
                    "repeated_probe_count": pending_momentum_decay_validation.get(
                        "repeated_probe_count"
                    ),
                    "repeated_probe_rate": pending_momentum_decay_validation.get(
                        "repeated_probe_rate"
                    ),
                    "oos_readiness_queue_count": (
                        (pending_momentum_decay_validation.get("oos_readiness_queue") or {}).get(
                            "queue_count"
                        )
                    ),
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
                    )[:8],
                },
                indent=2,
                sort_keys=True,
            )[:12000],
            "```",
            "",
        ])
    pending_stale_before_final_validation = (
        verdict.get("pending_stale_before_final_watch_validation") or {}
    )
    if pending_stale_before_final_validation:
        lines.extend([
            "## Pending Stale-Before-Final Watch Validation",
            "",
            "```json",
            json.dumps(
                {
                    "available": pending_stale_before_final_validation.get("available"),
                    "classification": pending_stale_before_final_validation.get("classification"),
                    "next_action": pending_stale_before_final_validation.get("next_action"),
                    "promotion_allowed": False,
                    "strategy_change_allowed": False,
                    "automatic_runtime_change_allowed": False,
                    "paper_enablement_allowed": False,
                    "registered_watch_count": pending_stale_before_final_validation.get(
                        "registered_watch_count"
                    ),
                    "current_stale_before_final_event_count": pending_stale_before_final_validation.get(
                        "current_stale_before_final_event_count"
                    ),
                    "repeated_selected_cluster_count": pending_stale_before_final_validation.get(
                        "repeated_selected_cluster_count"
                    ),
                    "denominator": pending_stale_before_final_validation.get("denominator") or {},
                    "status_counts": pending_stale_before_final_validation.get("status_counts") or {},
                    "oos_readiness_queue": (
                        pending_stale_before_final_validation.get("oos_readiness_queue") or {}
                    ),
                    "top_repeated_clusters": (
                        pending_stale_before_final_validation.get("top_repeated_clusters") or []
                    )[:8],
                },
                indent=2,
                sort_keys=True,
            )[:12000],
            "```",
            "",
        ])
    strategy_memory = verdict.get("strategy_memory") or {}
    strategy_memory_ingestion = verdict.get("strategy_memory_ingestion_summary") or {}
    strategy_memory_validation = verdict.get("strategy_memory_validation") or {}
    if strategy_memory or strategy_memory_ingestion or strategy_memory_validation:
        lines.extend([
            "## Strategy Memory",
            "",
            "```json",
            json.dumps(
                {
                    "enabled": strategy_memory.get("enabled", strategy_memory_ingestion.get("available")),
                    "hypotheses_count": strategy_memory.get("hypotheses_count"),
                    "mapped_to_existing_candidates": strategy_memory.get("mapped_to_existing_candidates"),
                    "missing_shadow_candidates": strategy_memory.get("missing_shadow_candidates"),
                    "rejected_future_data_hypotheses": strategy_memory.get("rejected_future_data_hypotheses"),
                    "top_10_shadow_hypotheses": strategy_memory.get("top_10_shadow_hypotheses") or [],
                    "filtered_winner_count": strategy_memory.get("filtered_winner_count"),
                    "exit_policy_variants_tested": strategy_memory.get("exit_policy_variants_tested"),
                    "delay_replay_done": strategy_memory.get("delay_replay_done"),
                    "paper_trades_db_available": strategy_memory.get("paper_trades_db_available"),
                    "validation_status_counts": strategy_memory.get("validation_status_counts") or (
                        strategy_memory_validation.get("status_counts") or {}
                    ),
                    "validation_window_count": strategy_memory.get("validation_window_count"),
                    "evidence_level": "historical_memory",
                    "allowed_use": "shadow_only",
                    "promotion_allowed": False,
                    "candidate_catalog_change_allowed": False,
                    "strategy_change_allowed": False,
                    "automatic_runtime_change_allowed": False,
                    "paper_enablement_allowed": False,
                    "missing_shadow_candidate_handoffs": (
                        strategy_memory.get("missing_shadow_candidate_handoffs")
                        or strategy_memory_ingestion.get("missing_shadow_candidate_handoffs")
                        or []
                    )[:8],
                    "exit_only_hypotheses": (
                        strategy_memory.get("exit_only_hypotheses")
                        or strategy_memory_ingestion.get("exit_only_hypotheses")
                        or []
                    )[:8],
                },
                indent=2,
                sort_keys=True,
            )[:12000],
            "```",
            "",
        ])
    lines.extend([
        "## Readiness Summaries",
        "",
        "```json",
        json.dumps(
            {
                "volume_profile_coverage": verdict.get("volume_profile_coverage") or {},
                "kline_coverage": verdict.get("kline_coverage") or {},
                "lifecycle_clean_window_pending": verdict.get("lifecycle_clean_window_pending"),
                "source_component_coverage": verdict.get("source_component_coverage") or {},
                "source_component_clean_window_pending": verdict.get("source_component_clean_window_pending"),
                "context_clean_window_pending": verdict.get("context_clean_window_pending"),
                "context_clean_window_eta_iso": verdict.get("context_clean_window_eta_iso"),
                "context_clean_window_progress": verdict.get("context_clean_window_progress") or {},
                "context_field_writer_fix_status": verdict.get("context_field_writer_fix_status"),
                "volume_kline_root_cause_audit": verdict.get("volume_kline_root_cause_audit") or {},
                "matured_kline_volume_recheck_audit": verdict.get("matured_kline_volume_recheck_audit") or {},
                "matured_volume_capture_cross_audit": verdict.get("matured_volume_capture_cross_audit") or {},
                "matured_volume_watch_queue": verdict.get("matured_volume_watch_queue") or {},
                "hypothesis_validation_audit": verdict.get("hypothesis_validation_audit") or {},
                "low_confidence_research_capture_audit": verdict.get("low_confidence_research_capture_audit") or {},
                "kline_coverage_resolution_audit": verdict.get("kline_coverage_resolution_audit") or {},
                "quality_timing_reject_research_audit": verdict.get("quality_timing_reject_research_audit") or {},
                "quality_timing_candidate_probe_validation": verdict.get("quality_timing_candidate_probe_validation") or {},
                "decision_no_pass_quality_timing_watch_validation": verdict.get("decision_no_pass_quality_timing_watch_validation") or {},
                "pending_momentum_decay_recheck_validation": verdict.get("pending_momentum_decay_recheck_validation") or {},
                "pending_stale_before_final_watch_validation": verdict.get("pending_stale_before_final_watch_validation") or {},
                "quality_timing_shadow_review_queue": verdict.get("quality_timing_shadow_review_queue") or {},
                "shadow_decision_bridge_audit_summary": verdict.get("shadow_decision_bridge_audit_summary") or {},
                "pass_allow_capture_gap_audit": verdict.get("pass_allow_capture_gap_audit") or {},
                "decision_no_pass_quality_timing_review": verdict.get("decision_no_pass_quality_timing_review") or {},
                "pass_allow_60_closure_plan": verdict.get("pass_allow_60_closure_plan") or {},
                "pass_allow_60_oos_freeze_registry": verdict.get("pass_allow_60_oos_freeze_registry") or {},
                "capture_cross_oos_freeze_registry": verdict.get("capture_cross_oos_freeze_registry") or {},
                "A_CLASS_mode_status": verdict.get("A_CLASS_mode_status") or {},
                "final_entry_contract_blocker_breakdown": verdict.get("final_entry_contract_blocker_breakdown") or {},
                "per_candidate_effectiveness_summary": verdict.get("per_candidate_effectiveness_summary") or {},
                "candidate_improvement_opportunities_summary": verdict.get("candidate_improvement_opportunities_summary") or {},
                "Markov_effectiveness_summary": verdict.get("Markov_effectiveness_summary") or {},
                "two_d_cross_validity_summary": verdict.get("two_d_cross_validity_summary") or {},
            },
            indent=2,
            sort_keys=True,
        )[:12000],
        "```",
        "",
        "## H1 / H2",
        "",
        "### H1",
        "",
        "```json",
        json.dumps(verdict.get("H1_capture_metrics") or {}, indent=2, sort_keys=True),
        "```",
        "",
        "### H2",
        "",
        "```json",
        json.dumps(verdict.get("H2_capture_metrics") or {}, indent=2, sort_keys=True),
        "```",
        "",
        "## Secondary Evidence",
        "",
        "```json",
        json.dumps(
            {
                "PnL_cross_secondary_status": verdict.get("PnL_cross_secondary_status"),
                "virtual_Markov_discovery_status": verdict.get("virtual_Markov_discovery_status"),
            },
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
    ])
    return "\n".join(lines)


def self_test():
    verdict = {
        "classification": "BLOCKED_DATA",
        "promotion_allowed": False,
        "blockers": ["raw_dog_rows_incomplete"],
        "candidate_count_expected": 84,
        "candidate_count_observed": 84,
        "observation_coverage_pct": 100.0,
        "raw_dog_rows_complete": False,
        "signal_id_join_rate": 1.0,
        "context_schema_version_counts": {"v": 1},
        "quote_clean_definition": {"counts": {"q": 1}},
        "blocked_subtype": "QUOTE_CONTEXT_COVERAGE",
        "current_commit": "abc",
        "deployment_commit": "abc",
        "human_action_required": False,
        "next_highest_priority_blocker": "source_quote_clean_coverage_below_80pct",
        "capture_60_target_loop": {
            "classification": "CAPTURE_PASS_ALLOW_GAP_BELOW_60",
            "raw_gold_silver_denominator": 10,
            "target_60_count": 6,
            "biggest_gap_stage": "pass_allow_capture",
            "additional_count_needed_to_60": 1,
            "next_best_allowed_action": "audit_decision_bridge_and_quality_timing_shadow_only",
            "promotion_allowed": False,
        },
        "decision_capture_60_gap_audit": {
            "classification": "DECISION_CAPTURE_60_SHADOW_BRIDGE_CAN_CLOSE_GAP_SAME_WINDOW",
            "shadow_bridge_mirror_complete": True,
            "optimistic_decision_record_rate_if_shadow_gap_logged": 0.9,
            "promotion_allowed": False,
        },
        "oos_readiness_summary": {
            "classification": "OOS_WINDOW_TOO_SMALL_OR_CONTEXT_BLOCKED",
            "available_probe_count": 1,
            "sufficient_probe_count": 0,
            "oos_repeated_watch_probe_count": 0,
            "next_action": "continue_collecting_post_freeze_window_before_judging_oos",
            "pass_allow_60_post_freeze_oos_validation": {
                "classification": "PASS_ALLOW_60_POST_FREEZE_OOS_TOO_SMALL",
                "raw_gold_silver_event_rows": 0,
                "post_freeze_usable_hours": 1.25,
                "repeat_watch_count": 0,
                "candidate_observation_effective_status": "not_applicable_no_raw_signal_ids",
                "candidate_observation_join_blocked": False,
                "post_freeze_oos_wait_reason": "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER",
                "raw_signal_rows_seen_after_freeze": 49,
                "oos_data_next_action": "continue_collecting_post_freeze_raw_gold_silver_events",
                "oos_data_availability": {
                    "classification": "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER",
                    "root_causes": ["no_post_freeze_raw_gold_silver_events"],
                    "candidate_observation_effective_status": "not_applicable_no_raw_signal_ids",
                    "next_action": "continue_collecting_post_freeze_raw_gold_silver_events",
                    "promotion_allowed": False,
                },
            },
        },
        "runtime_health_snapshot": {
            "available": True,
            "status": "degraded",
            "warnings": [
                "runtime_paper_fast_lane_health_stale",
                "runtime_paper_review_snapshot_stale",
            ],
            "blockers": [],
            "signal_source_freshness": {
                "status": "ok",
                "age_minutes": 4.0,
                "fail_closed": False,
                "latest_iso": "2026-07-01T20:29:04Z",
            },
            "paper_review_snapshot": {
                "status": "stale_or_missing",
                "age_minutes": 550.0,
                "max_age_minutes": 30.0,
                "warnings": ["runtime_paper_review_snapshot_stale"],
            },
            "paper_fast_lane_health": {
                "status": "stale_or_missing",
                "age_minutes": 34296.0,
                "max_age_minutes": 30.0,
                "warnings": ["runtime_paper_fast_lane_health_stale"],
            },
            "paper_db": {"status": "ok"},
            "observer_logs": {"status": "ok"},
            "runtime_final_evidence": {"status": "ok"},
        },
        "non_quote_sensitive_capture_discovery_allowed": True,
        "quote_sensitive_slices_blocked": True,
        "volume_profile_coverage": {"coverage_rate": 1.0},
        "kline_coverage": {"coverage_rate": 1.0},
        "A_CLASS_mode_status": {"final_entry_status": "READINESS_AUDIT_ONLY"},
        "final_entry_contract_blocker_breakdown": {},
        "per_candidate_effectiveness_summary": {"candidate_count": 84},
        "candidate_improvement_opportunities_summary": {"opportunity_count": 2},
        "shadow_candidate_improvement_queue": {
            "classification": "SHADOW_CANDIDATE_IMPROVEMENT_QUEUE_READY",
            "evidence_level": "discovery_shadow_only",
            "next_action": "track_notath_upstream_skip_shadow_probe",
            "queue_count": 1,
            "source_counts": {"quality_timing_reject_cluster": 1},
            "top_next_actions": ["track_notath_upstream_skip_shadow_probe"],
            "promotion_allowed": False,
            "top_items": [
                {
                    "candidate_id": "quality_timing:notath_upstream_skip",
                    "hypothesis_source": "quality_timing_reject_cluster",
                    "expected_capture_stage_improved": "pass_allow_capture",
                    "next_action": "track_notath_upstream_skip_shadow_probe",
                    "promotion_allowed": False,
                }
            ],
        },
        "Markov_effectiveness_summary": {
            "status": "insufficient_or_uninformative",
            "next_action": "run_or_review_coarse_non_quote_non_kline_profiles_before_claiming_markov_value",
            "usage": "research_only_markov_information_value",
            "total_green_buckets": 0,
            "total_yellow_buckets": 0,
            "total_insufficient_buckets": 0,
            "non_informative_reasons": {"kline": "closed_rows_exist_but_no_bucket_reached_min_closed"},
            "recommended_shadow_profiles": [
                {
                    "profile": "candidate_source",
                    "key_dimensions": ["candidate_id", "source_component"],
                    "status": "recommended_to_run",
                    "blocked_by": [],
                }
            ],
            "profile_diagnostics": {
                "kline": {
                    "status": "profile_over_fragmented_or_min_closed_not_met",
                    "closed_virtual_rows": 10,
                    "keys_emitted": 0,
                    "bucket_counts": {},
                    "context_blockers_affecting_profile": ["kline_coverage_below_80pct"],
                }
            },
        },
        "two_d_cross_validity_summary": {
            "classification": "CAPTURE_CROSS_DISCOVERY_WATCH",
            "next_action": "track_valid_capture_crosses_in_clean_non_overlapping_oos",
            "evidence_level": "discovery_same_window",
            "valid_cross_count": 3,
            "invalid_cross_count": 5,
            "shadow_matured_volume_cross_count": 1,
            "watch_count": 3,
            "promotion_allowed": False,
        },
        "capture_cross_oos_freeze_registry": {
            "classification": "CAPTURE_CROSS_OOS_FREEZE_READY_PENDING_CLEAN_WINDOW",
            "next_action": "validate_frozen_capture_cross_definitions_in_next_clean_non_overlapping_window",
            "frozen_definition_count": 1,
            "stage_counts": {"pass_allow_capture": 1},
            "promotion_allowed": False,
        },
        "quote_context_coverage": {
            "coverage_denominator_type": "signal_context_carrier_rows",
            "coverage_denominator_rows": 10,
            "context_carrier_candidate_ids": ["current_all"],
            "source_quote_clean_present_rate": 0.7,
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
        "H1_capture_metrics": {"status": "WATCH"},
        "H2_capture_metrics": {"status": "not_observed"},
        "entry_funnel_gap_summary": {
            "pending_entry_signal_ids": 10,
            "final_entry_contract_signal_ids": 2,
            "pending_without_final_entry_contract": 8,
            "pending_without_final_entry_category_counts": {
                "total_classified": 8,
                "automatic_runtime_change_allowed": False,
                "strategy_change_allowed": False,
                "paper_enablement_allowed": False,
                "categories": [
                    {
                        "category": "QUALITY_OR_TIMING_REJECT",
                        "count": 5,
                        "automatic_allowed_scope": "shadow-only candidate/evaluator analysis",
                    }
                ],
            },
            "readiness_gap_priority": {
                "current_shortfall_to_60": 6,
                "categories_ranked_by_optimistic_readiness_gain": [
                    {
                        "category": "QUALITY_OR_TIMING_REJECT",
                        "optimistic_mode_adjusted_final_eligibility_rate_if_all_bridged": 0.5,
                        "requires_final_entry_contract_eval": True,
                    }
                ],
                "promotion_allowed": False,
            },
        },
        "upstream_funnel_gap_summary": {
            "raw_signal_ids": 20,
            "decision_record_signal_ids": 12,
            "pass_or_allow_signal_ids": 8,
            "pending_entry_signal_ids": 5,
            "no_decision_record": 8,
            "no_decision_record_root_cause_counts": [
                {
                    "root_cause": "candidate_shadow_observed_no_decision_event",
                    "description": "Candidate shadow observations exist with full candidate coverage, but no decision event was written.",
                    "count": 8,
                }
            ],
            "no_decision_record_subroot_cause_counts": [
                {
                    "root_cause": "shadow_entry_hypotheses_matched_no_decision_bridge",
                    "description": "Full candidate mesh observed the signal and one or more shadow entry hypotheses matched, but no decision event was written.",
                    "count": 8,
                }
            ],
            "shadow_no_decision_entry_hypothesis_candidate_counts": [
                {"candidate_id": "notath_quote_clean", "family": "base", "count": 8}
            ],
            "no_decision_candidate_shadow_observed_no_decision_event": 8,
            "decision_no_pass_or_allow": 4,
            "pass_or_allow_without_pending_entry": 3,
            "total_upstream_gap": 15,
            "upstream_gap_category_counts": {
                "total_classified": 15,
                "categories": [
                    {
                        "category": "NO_DECISION_RECORD",
                        "count": 8,
                        "automatic_allowed_scope": "instrumentation, join, or observer audit",
                    }
                ],
            },
            "upstream_gap_priority": {
                "current_shortfall_to_60_pending": 7,
                "categories_ranked_by_optimistic_pending_gain": [
                    {
                        "category": "NO_DECISION_RECORD",
                        "optimistic_pending_capture_rate_if_all_bridged": 0.65,
                    }
                ],
                "promotion_allowed": False,
            },
        },
        "hypothesis_validation_audit": {
            "available": True,
            "overall": {
                "classification": "SAME_WINDOW_ONLY_PENDING_NEXT_WINDOW",
                "promotion_allowed": False,
            },
            "matured_volume_hypothesis_validation": {
                "registered_hypothesis_count": 10,
                "repeated_watch_count": 10,
                "oos_repeated_watch_count": 0,
            },
        },
        "shadow_decision_bridge_audit_summary": {
            "available": True,
            "status": "SHADOW_DECISION_BRIDGE_MIRROR_COMPLETE",
            "shadow_bridge_gap_count": 8,
            "mirror_complete": True,
            "top_candidate_counts": [
                {"candidate_id": "notath_quote_clean", "family": "base", "count": 8}
            ],
            "top_family_counts": [
                {"family": "base", "count": 8}
            ],
            "top_reason_counts": [
                {
                    "candidate_id": "notath_quote_clean",
                    "family": "base",
                    "reason": "runtime_source_quote_clean",
                    "count": 8,
                }
            ],
            "review_queue": {
                "classification": "SHADOW_DECISION_BRIDGE_REVIEW_QUEUE_READY",
                "queue_count": 1,
                "promotion_allowed": False,
                "automatic_runtime_change_allowed": False,
                "items": [
                    {
                        "candidate_id": "notath_quote_clean",
                        "status": "REVIEW_SHADOW_MATCHED_NO_DECISION_BRIDGE",
                        "promotion_allowed": False,
                    }
                ],
            },
        },
        "quality_timing_reject_research_audit": {
            "available": True,
            "verdict": "QUALITY_TIMING_REJECT_RESEARCH_READY",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "denominator": {"quality_timing_reject_event_rows": 4},
            "candidate_match_attribution": {
                "top_candidates": [
                    {"candidate_id": "kline:active_mom20_first3", "family": "kline", "count": 3}
                ]
            },
            "blocked_context_dimensions_excluded_view": {
                "classification": "CLEAN_CANDIDATE_ATTRIBUTION_READY",
                "blocked_dimensions": ["kline", "volume"],
                "clean_candidate_matched_any_events": 3,
                "blocked_candidate_matched_any_events": 1,
                "top_clean_candidates": [
                    {
                        "candidate_id": "entry_mode_registry:pullback_tiny_scout",
                        "family": "entry_mode_registry",
                        "count": 2,
                        "blocked_context_dimensions": [],
                    }
                ],
                "top_blocked_candidates": [
                    {
                        "candidate_id": "kline:active_mom20_first3",
                        "family": "kline",
                        "count": 3,
                        "blocked_context_dimensions": ["kline"],
                    }
                ],
            },
            "stage_attribution": {
                "stage_counts": [{"stage": "decision_no_pass_or_allow", "count": 4}]
            },
            "context_attribution": {
                "lifecycle_source_counts": [
                    {"lifecycle_profile": "ATH_SHALLOW_PULLBACK:OBSERVE", "source_component": "matrix_evaluator", "count": 4}
                ]
            },
            "shadow_only_review": {
                "classification": "QUALITY_TIMING_SHADOW_REVIEW_READY",
                "dominant_cluster": "matrix_alignment_wait",
                "dominant_stage": "decision_no_pass_or_allow",
                "research_opportunity_count": 1,
                "top_research_opportunities": [
                    {
                        "cluster": "matrix_alignment_wait",
                        "event_count": 4,
                        "suggested_shadow_only_action": "track_matrix_alignment_false_negative_shadow_probe",
                        "promotion_allowed": False,
                    }
                ],
            },
            "reason_level_breakout": {
                "classification": "QUALITY_TIMING_REASON_LEVEL_READY",
                "next_action": "review_dominant_quality_timing_reason_breakout_shadow_only",
                "dominant_cluster": "matrix_alignment_wait",
                "dominant_cluster_top_reasons": [
                    {
                        "stage": "decision_no_pass_or_allow",
                        "component": "matrix_evaluator",
                        "event_type": "timing_decision",
                        "decision": "WAIT",
                        "reason": "matrices not yet aligned",
                        "count": 4,
                        "suggested_shadow_only_action": "track_matrix_alignment_reason_shadow_only",
                        "promotion_allowed": False,
                    }
                ],
                "other_quality_timing_top_reasons": [],
                "promotion_allowed": False,
            },
            "shadow_only_next_actions": ["review_shadow_candidates_for_quality_timing_rejects"],
        },
        "quality_timing_candidate_probe_validation": {
            "available": True,
            "classification": "QUALITY_TIMING_PROBES_REPEATED_SAME_WINDOW",
            "next_action": "continue_shadow_probe_tracking_until_clean_window_then_oos",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "denominator": {
                "registered_probe_count": 3,
                "validated_probe_count": 3,
                "repeated_probe_count": 2,
                "repeated_probe_rate": 0.666667,
            },
            "status_counts": {
                "REPEATED_SHADOW_PROBE": 2,
                "NOT_OBSERVED_CURRENT_WINDOW": 1,
            },
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
        },
        "pending_momentum_decay_recheck_validation": {
            "available": True,
            "classification": "PENDING_MOMENTUM_DECAY_PROBES_REPEATED_SAME_WINDOW",
            "next_action": "continue_shadow_probe_tracking_until_clean_window_then_oos",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "registered_probe_count": 3,
            "current_cluster_event_count": 4,
            "current_cluster_unique_tokens": 3,
            "validated_probe_count": 3,
            "repeated_probe_count": 2,
            "repeated_probe_rate": 0.666667,
            "denominator": {
                "registered_probe_count": 3,
                "validated_probe_count": 3,
                "repeated_probe_count": 2,
                "repeated_probe_rate": 0.666667,
            },
            "status_counts": {
                "REPEATED_SHADOW_PROBE": 2,
                "NOT_OBSERVED_CURRENT_WINDOW": 1,
            },
            "oos_readiness_queue": {
                "classification": "PENDING_MOMENTUM_DECAY_OOS_QUEUE_PENDING_CLEAN_WINDOW",
                "queue_count": 2,
                "promotion_allowed": False,
                "automatic_runtime_change_allowed": False,
                "items": [
                    {
                        "hypothesis_id": "pending_momentum_decay:timeboxed_recheck_window",
                        "status": "PENDING_CLEAN_WINDOW_THEN_OOS",
                        "promotion_allowed": False,
                    }
                ],
            },
            "top_repeated_probes": [
                {
                    "hypothesis_id": "pending_momentum_decay:timeboxed_recheck_window",
                    "status": "REPEATED_SHADOW_PROBE",
                    "promotion_allowed": False,
                }
            ],
        },
        "quality_timing_shadow_review_queue": {
            "classification": "QUALITY_TIMING_SHADOW_REVIEW_QUEUE_READY",
            "queue_count": 1,
            "dominant_cluster": "matrix_alignment_wait",
            "promotion_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "items": [
                {
                    "cluster": "matrix_alignment_wait",
                    "event_count": 4,
                    "suggested_shadow_only_action": "track_matrix_alignment_false_negative_shadow_probe",
                    "status": "REVIEW_QUALITY_TIMING_REJECT_CLUSTER",
                    "promotion_allowed": False,
                    "automatic_runtime_change_allowed": False,
                    "paper_enablement_allowed": False,
                }
            ],
        },
        "matured_volume_watch_queue": {
            "classification": "MATURED_VOLUME_WATCH_QUEUE_READY",
            "queue_count": 1,
            "h1_status": "NO_H1_MATURED_VOLUME_HIT",
            "promotion_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "items": [
                {
                    "candidate_id": "entry_mode_registry:ath_flat_structure_tiny_scout",
                    "candidate_family": "entry_mode_registry",
                    "slice_value": "building",
                    "status": "REVIEW_MATURED_VOLUME_DISCOVERY_WATCH",
                    "promotion_allowed": False,
                    "automatic_runtime_change_allowed": False,
                    "paper_enablement_allowed": False,
                }
            ],
        },
        "kline_coverage_resolution_audit": {
            "available": True,
            "overall": {
                "classification": "KLINE_FORMAL_BLOCKED_RESEARCH_RECOVERABLE",
                "next_action": "continue_research_only_recovery_without_formal_kline_promotion",
            },
            "formal_denominator_changed": False,
            "formal_kline_coverage": {
                "coverage_rate": 0.46,
                "covered_rows": 42,
                "target_80pct_count": 73,
                "additional_rows_needed_to_80pct": 31,
            },
            "research_recoverability": {
                "confidence_adjusted_research_coverage_rate": 0.83,
                "confidence_adjusted_research_covered_rows": 76,
                "time_legal_recoverable_reaches_80pct": True,
            },
            "low_confidence_capture_summary": {
                "event_rows": 34,
                "candidate_matched_any_rate": 1.0,
            },
            "allowed_resolution_tracks": [
                {
                    "track": "low_confidence_time_legal_research",
                    "allowed_use": "research_only",
                    "recoverable_rows": 34,
                }
            ],
            "promotion_allowed": False,
        },
        "strategy_memory_ingestion_summary": {
            "available": True,
            "strategy_memory_hypotheses_count": 2,
            "mapped_to_existing_candidates": 1,
            "missing_shadow_candidates": 1,
            "rejected_future_data_hypotheses": 2,
            "top_10_shadow_hypotheses": ["SM-TEST-ENTRY", "SM-EXIT-TEST"],
            "filtered_winner_count": 3,
            "exit_policy_variants_tested": 2,
            "delay_replay_done": True,
            "paper_trades_db_available": True,
            "promotion_allowed": False,
            "allowed_use": "shadow_only",
            "evidence_level": "historical_memory",
            "missing_shadow_candidate_handoffs": [
                {
                    "hypothesis_id": "SM-EXIT-TEST",
                    "mapping_status": "missing_exit_shadow_sim_only",
                    "allowed_action": "generate_codex_handoff_only",
                    "promotion_allowed": False,
                }
            ],
            "exit_only_hypotheses": ["SM-EXIT-TEST"],
        },
    }
    text = build_handoff(verdict)
    assert "Capture-First 2D Cross" in text
    assert "CAPTURE_CROSS_DISCOVERY_WATCH" in text
    assert "track_valid_capture_crosses_in_clean_non_overlapping_oos" in text
    assert "CAPTURE_CROSS_OOS_FREEZE_READY_PENDING_CLEAN_WINDOW" in text
    assert "validate_frozen_capture_cross_definitions_in_next_clean_non_overlapping_window" in text
    assert "handoff_needed: `true`" in text
    assert "raw_dog_rows_incomplete" in text
    assert "Quote Context Coverage" in text
    assert "Quote Missing Root Cause" in text
    assert "Hypothesis Validation" in text
    assert "Entry Funnel Gap" in text
    assert "Upstream Funnel Gap" in text
    assert "QUALITY_OR_TIMING_REJECT" in text
    assert "NO_DECISION_RECORD" in text
    assert "candidate_shadow_observed_no_decision_event" in text
    assert "shadow_entry_hypotheses_matched_no_decision_bridge" in text
    assert "SHADOW_DECISION_BRIDGE_REVIEW_QUEUE_READY" in text
    assert "notath_quote_clean" in text
    assert "readiness_gap_priority" in text
    assert "upstream_gap_priority" in text
    assert "capture_60_gap_classification: `CAPTURE_PASS_ALLOW_GAP_BELOW_60`" in text
    assert (
        "decision_capture_60_gap_classification: `DECISION_CAPTURE_60_SHADOW_BRIDGE_CAN_CLOSE_GAP_SAME_WINDOW`"
        in text
    )
    assert "SAME_WINDOW_ONLY_PENDING_NEXT_WINDOW" in text
    assert "Quality / Timing Reject Research" in text
    assert "QUALITY_TIMING_REJECT_RESEARCH_READY" in text
    assert "blocked_context_dimensions_excluded_view" in text
    assert "CLEAN_CANDIDATE_ATTRIBUTION_READY" in text
    assert "entry_mode_registry:pullback_tiny_scout" in text
    assert "matrix_alignment_wait" in text
    assert "track_matrix_alignment_false_negative_shadow_probe" in text
    assert "reason_level_breakout" in text
    assert "QUALITY_TIMING_REASON_LEVEL_READY" in text
    assert "track_matrix_alignment_reason_shadow_only" in text
    assert "review_shadow_candidates_for_quality_timing_rejects" in text
    assert "quality_timing_shadow_review_queue" in text
    assert "QUALITY_TIMING_SHADOW_REVIEW_QUEUE_READY" in text
    assert "matured_volume_watch_queue" in text
    assert "MATURED_VOLUME_WATCH_QUEUE_READY" in text
    assert "Quality / Timing Candidate Probe Validation" in text
    assert "QUALITY_TIMING_PROBES_REPEATED_SAME_WINDOW" in text
    assert "REPEATED_SHADOW_PROBE" in text
    assert "quality_timing_probe:matrix_alignment_wait" in text
    assert "QUALITY_TIMING_OOS_QUEUE_PENDING_CLEAN_WINDOW" in text
    assert "Pending Momentum Decay Recheck Validation" in text
    assert "PENDING_MOMENTUM_DECAY_PROBES_REPEATED_SAME_WINDOW" in text
    assert '"registered_probe_count": 3' in text
    assert '"current_cluster_event_count": 4' in text
    assert '"current_cluster_unique_tokens": 3' in text
    assert '"repeated_probe_count": 2' in text
    assert "PENDING_MOMENTUM_DECAY_OOS_QUEUE_PENDING_CLEAN_WINDOW" in text
    assert "pending_momentum_decay:timeboxed_recheck_window" in text
    assert "Candidate Improvement Opportunities" in text
    assert "Markov Information Value" in text
    assert "candidate_source" in text
    assert "Strategy Memory" in text
    assert "SM-EXIT-TEST" in text
    assert "generate_codex_handoff_only" in text
    assert "Readiness Summaries" in text
    assert "context_clean_window_progress" in text
    assert "candidate_improvement_opportunities_summary" in text
    assert "SHADOW_CANDIDATE_IMPROVEMENT_QUEUE_READY" in text
    assert "discovery_shadow_only" in text
    assert "track_notath_upstream_skip_shadow_probe" in text
    assert "shadow_candidate_improvement_top_items" in text
    assert "quality_timing:notath_upstream_skip" in text
    assert "Kline Coverage Resolution" in text
    assert "KLINE_FORMAL_BLOCKED_RESEARCH_RECOVERABLE" in text
    assert "low_confidence_time_legal_research" in text
    assert "OOS_DATA_WAITING_FOR_POST_FREEZE_RAW_GOLD_SILVER" in text
    assert "candidate_observation_effective_status" in text
    assert "not_applicable_no_raw_signal_ids" in text
    assert "candidate_observation_join_blocked" in text
    assert "continue_collecting_post_freeze_raw_gold_silver_events" in text
    assert "Runtime Health Snapshot" in text
    assert "runtime_paper_review_snapshot_stale" in text
    quote_pending_verdict = {
        **verdict,
        "classification": "BLOCKED_CONTEXT_COVERAGE",
        "blocked_subtype": "CLEAN_V2_WINDOW_PENDING",
        "blockers": [
            "source_quote_clean_coverage_below_80pct",
            "source_quote_executable_coverage_below_80pct",
        ],
        "quote_writer_fix_status": "VERIFIED_POST_DEPLOY",
        "quote_clean_window_status": "QUOTE_CLEAN_WINDOW_PENDING",
        "quote_clean_window_eta_iso": "2026-07-01T00:31:02Z",
        "quote_clean_window_seconds_remaining": 3600,
    }
    text = build_handoff(quote_pending_verdict)
    assert "handoff_needed: `false`" in text
    assert "Quote Clean Window" in text
    assert "not a writer-fix handoff" in text
    assert "Required Fixes" not in text
    assert "source_quote_clean_coverage_below_80pct" not in text
    quote_pending_volume_verdict = {
        **quote_pending_verdict,
        "blockers": [
            "source_quote_clean_coverage_below_80pct",
            "source_quote_executable_coverage_below_80pct",
            "volume_profile_coverage_below_80pct",
        ],
        "actionable_blockers": ["volume_profile_coverage_below_80pct"],
        "next_highest_priority_blocker": "volume_profile_coverage_below_80pct",
        "volume_kline_root_cause_audit": {
            "overall": {
                "classification": "DATA_BLOCKED_VOLUME_KLINE",
                "root_causes": ["volume_profile_unknown_from_insufficient_or_unclassified_kline"],
                "promotion_allowed": False,
            },
            "volume_context": {
                "rows_scanned": 10,
                "field_present_rate": 1.0,
                "known_rate": 0.4,
                "missing_rate": 0.0,
                "unknown_rate": 0.6,
                "blocker": "volume_profile_coverage_below_80pct",
                "root_causes": ["volume_profile_unknown_from_insufficient_or_unclassified_kline"],
                "recent_windows": {
                    "1h": {"rows_scanned": 5, "field_present_rate": 1.0, "known_rate": 0.4, "unknown_rate": 0.6}
                },
            },
            "volume_context_resolution": {
                "classification": "VOLUME_FORMAL_CONTEXT_BLOCKED_SHADOW_MATURED_RECHECK_AVAILABLE",
                "next_action": "review_matured_volume_shadow_recheck_and_kline_resolution_before_formal_volume_promotion",
                "formal_volume_profile_known_rate": 0.4,
                "formal_volume_profile_unknown_rate": 0.6,
                "writer_field_present": True,
                "primary_unknown_reason": "insufficient_kline_bars_lt_3",
                "matured_volume_shadow_recheck_recommended": True,
                "allowed_use": "shadow_only_matured_volume_recheck",
                "promotion_allowed": False,
            },
            "raw_gold_silver_kline": {
                "raw_all_gold_silver_event_rows": 8,
                "kline_coverage_rate": 0.5,
                "kline_covered_rows": 4,
                "kline_uncovered_rows": 4,
                "kline_uncovered_root_cause_counts": {"no_kline_for_token": 4},
                "blocker": "kline_coverage_below_80pct",
            },
        },
        "matured_kline_volume_recheck_audit": {
            "overall": {
                "classification": "DISCOVERY_ONLY_MATURED_KLINE_RECHECK",
                "next_action": "shadow_delayed_volume_recheck_likely_useful",
                "promotion_allowed": False,
            },
            "context_rows_scanned": 10,
            "unknown_or_missing_rows": 6,
            "recheck": {
                "recoverable_known_rows": 5,
                "recoverable_known_rate": 0.833333,
                "still_unknown_rows": 1,
            },
        },
        "matured_volume_capture_cross_audit": {
            "overall": {
                "classification": "MATURED_VOLUME_DISCOVERY_NO_SIGNAL",
                "promotion_allowed": False,
            },
            "denominator": {
                "raw_all_gold_silver": {"event_rows": 8, "joined_event_rate": 0.5}
            },
            "signal_id_reconciliation": {
                "raw_all_gold_silver": {
                    "joined_event_rate": 0.5,
                    "unjoined_reason_counts": {"missing_context_carrier_observation": 4},
                }
            },
            "matured_volume_context": {"known_rate": 0.9},
            "watch_slice_count": 1,
            "next_research_action": "review_non_h1_matured_volume_watch_slices",
        },
    }
    text = build_handoff(quote_pending_volume_verdict)
    assert "handoff_needed: `true`" in text
    assert "next_highest_priority_blocker: `volume_profile_coverage_below_80pct`" in text
    assert "Required Fixes" in text
    assert "Volume / Kline Root Cause" in text
    assert "volume_context_resolution" in text
    assert "VOLUME_FORMAL_CONTEXT_BLOCKED_SHADOW_MATURED_RECHECK_AVAILABLE" in text
    assert "matured_volume_shadow_recheck_recommended" in text
    assert "shadow_only_matured_volume_recheck" in text
    assert "volume_profile_unknown_from_insufficient_or_unclassified_kline" in text
    assert "shadow_delayed_volume_recheck_likely_useful" in text
    assert "missing_context_carrier_observation" in text
    assert "volume_profile_coverage_below_80pct" in text
    assert "`source_quote_clean_coverage_below_80pct`:" not in text
    matured_volume_path_available_verdict = {
        **quote_pending_verdict,
        "blockers": [
            "source_quote_clean_coverage_below_80pct",
            "source_quote_executable_coverage_below_80pct",
            "volume_profile_coverage_below_80pct",
        ],
        "actionable_blockers": [],
        "next_highest_priority_blocker": None,
        "formal_volume_sensitive_slices_blocked": True,
        "shadow_matured_volume_slices_evaluable": True,
        "volume_profile_blocker_state": {
            "classification": "SHADOW_MATURED_VOLUME_PATH_AVAILABLE",
            "formal_volume_slices_evaluable": False,
            "shadow_matured_volume_slices_evaluable": True,
            "recoverable_known_rate": 0.98,
            "matured_volume_known_rate": 0.91,
            "scope_reconciled": True,
            "next_action": "continue_shadow_matured_volume_validation_without_formal_volume_promotion",
            "promotion_allowed": False,
        },
        "matured_volume_capture_cross_audit": {
            "overall": {
                "classification": "MATURED_VOLUME_DISCOVERY_WATCH",
                "promotion_allowed": False,
            },
            "signal_id_reconciliation": {
                "raw_all_gold_silver": {
                    "joined_event_rate": 0.32,
                    "unjoined_reason_counts": {"outside_candidate_observer_window_before": 93},
                }
            },
            "matured_volume_context": {"known_rate": 0.91},
            "watch_slice_count": 3,
        },
    }
    text = build_handoff(matured_volume_path_available_verdict)
    assert "handoff_needed: `false`" in text
    assert "Required Fixes" not in text
    assert "Volume / Kline Root Cause" in text
    assert "Matured Volume Shadow Validation" in text
    assert "SHADOW_MATURED_VOLUME_PATH_AVAILABLE" in text
    assert "shadow_matured_volume_slices_evaluable: `true`" in text
    verdict["blockers"] = []
    text = build_handoff(verdict)
    assert "handoff_needed: `false`" in text
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "handoff.md"
        write_text(path, text)
        assert path.read_text(encoding="utf-8").startswith("# Gold/Silver")
    print("SELF_TEST_PASS generate_codex_handoff")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verdict", required=False)
    parser.add_argument("--out", default="data/agent_handoffs/latest_codex_handoff.md")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not args.verdict:
        raise SystemExit("--verdict is required unless --self-test is used")
    verdict = load_json(args.verdict)
    text = build_handoff(verdict)
    write_text(args.out, text)
    print(json.dumps({"out": args.out, "handoff_needed": handoff_needed(verdict)}, sort_keys=True))


if __name__ == "__main__":
    main()
