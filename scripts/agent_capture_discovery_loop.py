#!/usr/bin/env python3
"""Bounded autonomous gold/silver capture discovery loop.

Discovery-only. This loop writes reviewer artifacts and never changes strategy,
entry policy, hard gates, exit gates, canary size, executor, wallet, or risk.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from generate_codex_handoff import build_handoff, write_text as write_handoff_text
from review_agent_verdict import build_verdict, write_json


SCHEMA_VERSION = "agent_capture_discovery_loop.v1"
DEFAULT_OUT_ROOT = "/app/data/agent_runs"
DEFAULT_HANDOFF_DIR = "/app/data/agent_handoffs"
DEFAULT_REGISTRY = "/app/data/hypothesis_registry.json"
SHADOW_DECISION_MIRROR_EVENT_LIMIT = 200
SHADOW_DECISION_MIRROR_EXAMPLE_LIMIT = 20
REPORT_TEST_COMMANDS = (
    ("capture_self_test", ["scripts/offline_candidate_capture_discovery.py", "--self-test"]),
    ("pnl_cross_self_test", ["scripts/offline_candidate_cross_eval.py", "--self-test"]),
    ("virtual_markov_self_test", ["scripts/build_candidate_virtual_markov.py", "--self-test"]),
    ("volume_kline_audit_self_test", ["scripts/volume_kline_coverage_audit.py", "--self-test"]),
    ("matured_kline_volume_recheck_self_test", ["scripts/matured_kline_volume_recheck_audit.py", "--self-test"]),
    ("matured_volume_capture_cross_self_test", ["scripts/matured_volume_capture_cross_audit.py", "--self-test"]),
    ("hypothesis_validation_self_test", ["scripts/hypothesis_validation_audit.py", "--self-test"]),
    ("low_confidence_research_capture_self_test", ["scripts/low_confidence_research_capture_audit.py", "--self-test"]),
    ("quality_timing_reject_research_self_test", ["scripts/quality_timing_reject_research_audit.py", "--self-test"]),
    ("candidate_downstream_readiness_self_test", ["scripts/candidate_downstream_readiness_audit.py", "--self-test"]),
    ("a_class_mode_readiness_self_test", ["scripts/a_class_fastlane_mode_readiness_audit.py", "--self-test"]),
    ("runtime_health_snapshot_self_test", ["scripts/runtime_health_snapshot_audit.py", "--self-test"]),
    ("oos_probe_refresh_self_test", ["scripts/refresh_oos_readiness_probes.py", "--self-test"]),
    ("reviewer_self_test", ["scripts/review_agent_verdict.py", "--self-test"]),
    ("handoff_self_test", ["scripts/generate_codex_handoff.py", "--self-test"]),
)


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def run_id():
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_text(path, text):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(target)


def log_event(event, **fields):
    payload = {
        "schema_version": "agent_capture_discovery_loop_event.v1",
        "event": event,
        "at": utc_now(),
        **fields,
    }
    print(json.dumps(payload, sort_keys=True), flush=True)


def file_available(path):
    return bool(path) and Path(path).exists() and Path(path).is_file()


def sqlite_has_table(path, table):
    if not file_available(path):
        return False
    db = sqlite3.connect(path)
    try:
        row = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
        return bool(row)
    finally:
        db.close()


def command_result(name, args, *, timeout):
    started = time.time()
    cmd = [sys.executable, *args]
    log_event("command_start", name=name, timeout_sec=timeout, cmd=cmd)
    try:
        proc = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        result = {
            "name": name,
            "cmd": cmd,
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "duration_sec": round(time.time() - started, 3),
            "stdout_tail": proc.stdout[-4000:],
            "stderr_tail": proc.stderr[-4000:],
        }
        log_event("command_end", name=name, ok=result["ok"], returncode=proc.returncode, duration_sec=result["duration_sec"])
        return result
    except subprocess.TimeoutExpired as exc:
        result = {
            "name": name,
            "cmd": cmd,
            "ok": False,
            "returncode": None,
            "duration_sec": round(time.time() - started, 3),
            "stdout_tail": (exc.stdout or "")[-4000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-4000:] if isinstance(exc.stderr, str) else "",
            "error": f"timeout_after_{timeout}s",
        }
        log_event("command_timeout", name=name, timeout_sec=timeout, duration_sec=result["duration_sec"])
        return result


def run_self_tests(timeout):
    results = [command_result(name, args, timeout=timeout) for name, args in REPORT_TEST_COMMANDS]
    return {
        "schema_version": "agent_capture_discovery_tests.v1",
        "generated_at": utc_now(),
        "passed": all(row["ok"] for row in results),
        "results": results,
    }


def blocked_capture_report(reason, paper_db, raw_db, hours, expected_candidates):
    return {
        "schema_version": "offline_candidate_capture_discovery.v1",
        "report_type": "capture_first_candidate_discovery",
        "evidence_level": "discovery_same_window",
        "evidence_role": "primary_gold_silver_capture_discovery",
        "can_promote_live": False,
        "generated_at": utc_now(),
        "db": paper_db,
        "raw_dog_source": {
            "source": "raw_signal_outcomes_db",
            "path": raw_db,
            "available": file_available(raw_db),
        },
        "hours": hours,
        "candidate_count_expected": expected_candidates,
        "report_health": {
            "promotion_allowed": False,
            "promotion_blockers": [reason],
        },
        "coverage": {
            "candidate_count_expected": expected_candidates,
            "candidate_count_observed": 0,
            "signal_count": 0,
            "observation_rows": 0,
            "expected_observation_rows": 0,
            "coverage_pct": 0,
            "bad_signal_count": 0,
        },
        "context_health": {
            "context_schema_version_counts": {},
            "quote_clean_definition_counts": {},
            "quote_sensitive_slices_evaluable": False,
            "gaps": [reason],
        },
        "raw_gold_silver_denominator": {
            "available": False,
            "rows_complete_against_summary": False,
            "event_rows": 0,
            "unique_tokens": 0,
        },
        "denominator_audit": {},
        "raw_dog_observation_join": {"join_rate": 0, "raw_dog_event_rows": 0},
        "raw_all_dog_observation_join": {"join_rate": 0, "raw_dog_event_rows": 0},
        "candidate_baseline": [],
        "context_slices": [],
        "judgment_counts": {"DISCOVERY_HIT": 0, "WATCH": 0, "TOO_SMALL": 0, "NO_SIGNAL": 0},
        "missed_dog_attribution": [],
        "watchlist_hypotheses": [],
    }


def run_report(name, args, out_path, *, timeout):
    result = command_result(name, args, timeout=timeout)
    result["out_path"] = str(out_path)
    result["out_exists"] = Path(out_path).exists()
    return result


def git_commit():
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


def safe_rate(num, den):
    try:
        den = float(den or 0)
        if den <= 0:
            return None
        return round(float(num or 0) / den, 6)
    except Exception:
        return None


def pct_to_rate(value):
    try:
        if value is None:
            return None
        return round(float(value) / 100.0, 6)
    except Exception:
        return None


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def write_derived_report(path, payload):
    payload = {
        "generated_at": utc_now(),
        "can_promote_live": False,
        "evidence_level": "discovery_same_window",
        **payload,
    }
    write_json(path, payload)
    return path


def build_context_coverage_report(capture):
    context = capture.get("context_health") or {}
    field_coverage = context.get("field_coverage") or {}
    context_field_coverage = context.get("context_field_coverage") or {}
    denominator = capture.get("denominator_audit") or {}
    raw_all = denominator.get("raw_all_gold_silver_event_rows") or 0
    dropped_kline = (denominator.get("filter_drop_breakdown_non_exclusive") or {}).get("dropped_kline_uncovered", 0)
    kline_rate = None if not raw_all else round(max(0.0, 1.0 - float(dropped_kline or 0) / float(raw_all)), 6)
    volume_pct = (field_coverage.get("volume_profile") or {}).get("coverage_pct")
    candle_pct = (field_coverage.get("candle_pattern") or {}).get("coverage_pct")
    fbr_pct = (field_coverage.get("fbr_time_legal") or {}).get("coverage_pct")
    blockers = []
    blockers.extend(context.get("gaps") or [])
    if kline_rate is not None and kline_rate < 0.8:
        blockers.append("kline_coverage_below_80pct")
    return {
        "schema_version": "context_coverage_audit.v1",
        "report_type": "context_coverage_audit",
        "quote_context_coverage": capture.get("quote_context_coverage") or context.get("quote_context_coverage") or {},
        "quote_missing_root_cause": capture.get("quote_missing_root_cause") or context.get("quote_missing_root_cause") or {},
        "context_field_coverage": context_field_coverage,
        "lifecycle_profile_coverage": context_field_coverage.get("lifecycle_profile") or {
            "coverage_denominator_type": "signal_context_carrier_rows",
            "coverage_pct": (field_coverage.get("lifecycle_profile") or {}).get("coverage_pct"),
            "coverage_rate": pct_to_rate((field_coverage.get("lifecycle_profile") or {}).get("coverage_pct")),
            "blocker": "lifecycle_profile_coverage_below_80pct"
            if ((field_coverage.get("lifecycle_profile") or {}).get("coverage_pct") is None
                or (field_coverage.get("lifecycle_profile") or {}).get("coverage_pct") < 80)
            else None,
        },
        "source_component_coverage": context_field_coverage.get("source_component") or {
            "coverage_denominator_type": "signal_context_carrier_rows",
            "coverage_pct": (field_coverage.get("source_component") or {}).get("coverage_pct"),
            "coverage_rate": pct_to_rate((field_coverage.get("source_component") or {}).get("coverage_pct")),
            "blocker": "source_component_coverage_below_80pct"
            if ((field_coverage.get("source_component") or {}).get("coverage_pct") is None
                or (field_coverage.get("source_component") or {}).get("coverage_pct") < 80)
            else None,
        },
        "markov_bucket_coverage": context_field_coverage.get("markov_bucket") or {
            "coverage_denominator_type": "signal_context_carrier_rows",
            "coverage_pct": (field_coverage.get("markov_bucket") or {}).get("coverage_pct"),
            "coverage_rate": pct_to_rate((field_coverage.get("markov_bucket") or {}).get("coverage_pct")),
            "blocker": "markov_bucket_coverage_below_80pct"
            if ((field_coverage.get("markov_bucket") or {}).get("coverage_pct") is None
                or (field_coverage.get("markov_bucket") or {}).get("coverage_pct") < 80)
            else None,
        },
        "volume_profile_coverage": {
            "coverage_denominator_type": "signal_context_carrier_rows",
            "coverage_pct": volume_pct,
            "coverage_rate": pct_to_rate(volume_pct),
            "blocker": "volume_profile_coverage_below_80pct" if volume_pct is None or volume_pct < 80 else None,
        },
        "kline_coverage": {
            "coverage_denominator_type": "raw_all_gold_silver",
            "raw_all_gold_silver_event_rows": raw_all,
            "dropped_kline_uncovered": dropped_kline,
            "coverage_rate": kline_rate,
            "candle_pattern_coverage_pct": candle_pct,
            "fbr_time_legal_coverage_pct": fbr_pct,
            "blocker": "kline_coverage_below_80pct" if kline_rate is not None and kline_rate < 0.8 else None,
        },
        "context_schema_version_counts": context.get("context_schema_version_counts") or {},
        "quote_clean_definition_counts": context.get("quote_clean_definition_counts") or {},
        "blockers": sorted(set(blockers)),
    }


def build_candidate_effectiveness_report(capture, downstream=None):
    rows = list(capture.get("candidate_baseline") or [])
    downstream = downstream or {}
    downstream_by_candidate = {
        row.get("candidate_id"): row
        for row in (downstream.get("all_candidates") or downstream.get("top_candidates") or [])
        if row.get("candidate_id")
    }
    classified = []
    counts = {"true_detector": 0, "low_precision_broad_detector": 0, "potential_entry_hypothesis": 0, "no_signal": 0}
    for row in rows:
        recall = row.get("business_match_recall_event")
        if recall is None:
            recall = row.get("match_recall_event")
        precision = row.get("match_precision_event")
        match_count = row.get("match_count") or 0
        matched_gs = row.get("matched_raw_all_gold_silver_events")
        if matched_gs is None:
            matched_gs = row.get("matched_gold_silver_events") or 0
        if recall is not None and recall >= 0.35 and (precision or 0) < 0.05:
            label = "low_precision_broad_detector"
        elif recall is not None and recall > 0 and (precision or 0) >= 0.15 and matched_gs >= 3:
            label = "potential_entry_hypothesis"
        elif recall is not None and recall > 0 and matched_gs > 0:
            label = "true_detector"
        else:
            label = "no_signal"
        counts[label] += 1
        item = {
            "candidate_id": row.get("candidate_id"),
            "family": row.get("family"),
            "classification": label,
            "match_count": match_count,
            "matched_raw_all_gold_silver_events": matched_gs,
            "business_match_recall_event": recall,
            "match_precision_event": precision,
        }
        downstream_row = downstream_by_candidate.get(row.get("candidate_id")) or {}
        if downstream_row:
            item.update(
                {
                    "downstream_classification": downstream_row.get("classification"),
                    "decision_record_rate_after_match": downstream_row.get("decision_record_rate_after_match"),
                    "pass_allow_rate_after_match": downstream_row.get("pass_allow_rate_after_match"),
                    "pending_rate_after_match": downstream_row.get("pending_rate_after_match"),
                    "final_entry_contract_rate_after_match": downstream_row.get("final_entry_contract_rate_after_match"),
                    "mode_disabled_adjusted_final_eligibility_rate_after_match": downstream_row.get(
                        "mode_disabled_adjusted_final_eligibility_rate_after_match"
                    ),
                    "paper_trade_intent_rate_after_match": downstream_row.get("paper_trade_intent_rate_after_match"),
                    "paper_trade_committed_rate_after_match": downstream_row.get("paper_trade_committed_rate_after_match"),
                    "decision_record_count_after_match": downstream_row.get("decision_record_count_after_match"),
                    "pending_count_after_match": downstream_row.get("pending_count_after_match"),
                    "mode_disabled_only_final_entry_count_after_match": downstream_row.get(
                        "mode_disabled_only_final_entry_count_after_match"
                    ),
                    "paper_trade_intent_count_after_match": downstream_row.get("paper_trade_intent_count_after_match"),
                    "paper_trade_committed_count_after_match": downstream_row.get("paper_trade_committed_count_after_match"),
                }
            )
        classified.append(item)
    order = {
        "potential_entry_hypothesis": 3,
        "true_detector": 2,
        "low_precision_broad_detector": 1,
        "no_signal": 0,
    }
    classified.sort(
        key=lambda row: (
            order.get(row["classification"], 0),
            row.get("business_match_recall_event") or 0,
            row.get("match_precision_event") or 0,
            row.get("matched_raw_all_gold_silver_events") or 0,
        ),
        reverse=True,
    )
    return {
        "schema_version": "candidate_effectiveness_report.v2",
        "report_type": "candidate_effectiveness_24h",
        "candidate_count": len(rows),
        "downstream_readiness_loaded": bool(downstream_by_candidate),
        "downstream_stage_counts": downstream.get("stage_counts") or {},
        "downstream_classification_counts": downstream.get("classification_counts") or {},
        "classification_counts": counts,
        "top_candidates": classified[:50],
        "notes": [
            "Capture-first classifications are discovery-only and do not imply entry promotion.",
            "Downstream rates are conditional on candidate-matched raw gold/silver signal_ids.",
            "PnL is intentionally not used as the primary candidate-effectiveness criterion.",
        ],
    }


def count_for_root(rows, root_cause):
    for row in rows or []:
        if row.get("root_cause") == root_cause:
            return safe_int(row.get("count"), 0)
    return 0


def build_shadow_decision_mirror_events(signal_examples, *, limit=SHADOW_DECISION_MIRROR_EVENT_LIMIT):
    examples = []
    for row in (signal_examples or [])[:limit]:
        matched_sample = list(row.get("matched_entry_hypothesis_sample") or [])
        examples.append(
            {
                "schema_version": "shadow_decision_evidence_mirror.event.v1",
                "evidence_type": "shadow_entry_hypothesis_matched_no_decision_bridge",
                "signal_id": row.get("signal_id"),
                "token_ca": row.get("token_ca"),
                "matched_entry_hypothesis_count": row.get("matched_entry_hypothesis_count"),
                "matched_entry_hypothesis_sample": matched_sample[:12],
                "matched_candidate_ids_sample": [
                    item.get("candidate_id")
                    for item in matched_sample[:12]
                    if item.get("candidate_id")
                ],
                "source_artifact": "raw_gold_silver_funnel_audit",
                "recommended_write_target": "shadow_decision_evidence_mirror_only",
                "forbidden_write_targets": [
                    "paper_decision_events",
                    "a_class_decision_events",
                    "pending_entries",
                    "paper_trades",
                    "final_entry_contract",
                ],
                "creates_pending_entry": False,
                "creates_paper_trade": False,
                "changes_runtime_mode": False,
                "promotion_allowed": False,
            }
        )
    return examples


def build_shadow_decision_bridge_audit(raw_funnel):
    """Extract the shadow-entry match/no-decision bridge gap into a focused artifact."""
    summary = raw_funnel.get("summary") or {}
    entry_bridge = summary.get("entry_bridge_layer") or {}
    raw_bridge = entry_bridge.get("raw_signal_decision_bridge") or {}
    no_decision_subroots = raw_bridge.get("no_decision_record_subroot_cause_counts") or []
    no_decision_roots = raw_bridge.get("no_decision_record_root_cause_counts") or []
    shadow_count = count_for_root(
        no_decision_subroots,
        "shadow_entry_hypotheses_matched_no_decision_bridge",
    )
    raw_signal_ids = safe_int(raw_bridge.get("raw_signal_ids"), 0)
    no_decision_count = safe_int(raw_bridge.get("raw_signals_without_decision_record"), 0)
    current_decision_count = max(0, raw_signal_ids - no_decision_count)
    optimistic_decision_count = min(raw_signal_ids, current_decision_count + shadow_count)
    remaining_no_decision_after_shadow = max(0, no_decision_count - shadow_count)
    signal_examples = raw_bridge.get("shadow_no_decision_entry_hypothesis_signal_examples") or []
    source_signal_count = safe_int(
        raw_bridge.get("shadow_no_decision_entry_hypothesis_signal_count"),
        shadow_count,
    )
    source_signal_count = shadow_count if source_signal_count is None else source_signal_count
    source_examples_truncated = bool(
        raw_bridge.get("shadow_no_decision_entry_hypothesis_signal_examples_truncated")
    )
    mirror_events = build_shadow_decision_mirror_events(signal_examples)
    mirror_examples = mirror_events[:SHADOW_DECISION_MIRROR_EXAMPLE_LIMIT]
    mirror_truncated = bool(
        source_examples_truncated
        or len(mirror_events) < source_signal_count
        or len(mirror_events) < shadow_count
    )
    mirror_complete = bool(shadow_count > 0 and not mirror_truncated and len(mirror_events) >= shadow_count)
    status = "NO_SHADOW_DECISION_BRIDGE_GAP"
    next_action = "continue_capture_discovery"
    mirror_status = "NO_MIRROR_NEEDED"
    if shadow_count > 0 and mirror_complete:
        status = "SHADOW_DECISION_BRIDGE_MIRROR_COMPLETE"
        next_action = "continue_shadow_decision_bridge_monitoring"
        mirror_status = "READ_ONLY_EVIDENCE_MIRROR_COMPLETE"
    elif shadow_count > 0:
        status = "SHADOW_DECISION_BRIDGE_AUDIT_REQUIRED"
        next_action = "audit_shadow_entry_hypotheses_matched_no_decision_bridge"
        mirror_status = "RECOMMENDED_READ_ONLY_INSTRUMENTATION"
    return {
        "schema_version": "shadow_decision_bridge_audit.v1",
        "report_type": "shadow_decision_bridge_audit_24h",
        "status": status,
        "next_action": next_action,
        "root_cause": "shadow_entry_hypotheses_matched_no_decision_bridge",
        "bridge_expectation": {
            "candidate_shadow_observer_mode": "shadow_only_observation",
            "production_decision_event_expected_from_shadow_match": False,
            "interpretation": (
                "Shadow candidate matches are research evidence. By design they do not create pending entries, "
                "paper trades, live orders, or production decision records."
            ),
            "gap_meaning": (
                "These rows identify raw gold/silver signals where shadow entry hypotheses matched but no "
                "production decision event exists for the raw signal_id. This is a bridge/instrumentation gap, "
                "not promotion evidence and not permission to change runtime mode."
            ),
        },
        "denominator": {
            "raw_signal_ids": raw_signal_ids,
            "current_decision_record_count": current_decision_count,
            "current_decision_record_rate": safe_rate(current_decision_count, raw_signal_ids),
            "raw_signals_without_decision_record": no_decision_count,
            "shadow_entry_hypotheses_matched_no_decision_bridge": shadow_count,
            "shadow_bridge_gap_rate_vs_raw_signal_ids": safe_rate(shadow_count, raw_signal_ids),
            "shadow_bridge_gap_share_of_no_decision": safe_rate(shadow_count, no_decision_count),
            "optimistic_decision_record_count_if_shadow_gap_logged": optimistic_decision_count,
            "optimistic_decision_record_rate_if_shadow_gap_logged": safe_rate(
                optimistic_decision_count,
                raw_signal_ids,
            ),
            "remaining_no_decision_after_shadow_gap_logged": remaining_no_decision_after_shadow,
            "source_signal_examples_count": len(signal_examples),
            "source_signal_examples_total": source_signal_count,
            "source_signal_examples_limit": raw_bridge.get(
                "shadow_no_decision_entry_hypothesis_signal_example_limit"
            ),
            "source_signal_examples_truncated": source_examples_truncated,
            "mirror_event_count": len(mirror_events),
            "mirror_event_example_count": len(mirror_examples),
            "mirror_event_limit": SHADOW_DECISION_MIRROR_EVENT_LIMIT,
            "mirror_event_coverage_vs_shadow_bridge_gap": safe_rate(len(mirror_events), shadow_count),
            "mirror_event_truncated": mirror_truncated,
        },
        "read_only_evidence_mirror": {
            "schema_version": "shadow_decision_evidence_mirror.v1",
            "status": mirror_status,
            "purpose": (
                "Record that the shadow mesh saw raw gold/silver opportunities when production decision "
                "events were absent, without changing entry policy or paper/live execution."
            ),
            "recommended_write_target": "shadow_decision_evidence_mirror_only",
            "recommended_storage": [
                "agent artifact",
                "read-only evidence log",
                "separate SQLite table that is excluded from production decision/final-entry queries",
            ],
            "forbidden_write_targets": [
                "paper_decision_events",
                "a_class_decision_events",
                "pending_entries",
                "paper_trades",
                "final_entry_contract",
            ],
            "required_fields": [
                "event_ts",
                "signal_id",
                "token_ca",
                "root_cause",
                "matched_entry_hypothesis_count",
                "matched_entry_hypothesis_sample",
                "candidate_count_expected",
                "promotion_allowed=false",
                "creates_pending_entry=false",
                "creates_paper_trade=false",
                "changes_runtime_mode=false",
            ],
            "event_count": len(mirror_events),
            "event_limit": SHADOW_DECISION_MIRROR_EVENT_LIMIT,
            "event_coverage_vs_shadow_bridge_gap": safe_rate(len(mirror_events), shadow_count),
            "event_truncated": mirror_truncated,
            "runtime_effect": "none",
            "promotion_allowed": False,
            "paper_enablement_allowed": False,
            "automatic_runtime_change_allowed": False,
        },
        "mirror_events": mirror_events,
        "mirror_event_examples": mirror_examples,
        "no_decision_record_root_cause_counts": no_decision_roots,
        "no_decision_record_subroot_cause_counts": no_decision_subroots,
        "family_counts": raw_bridge.get("shadow_no_decision_entry_hypothesis_family_counts") or [],
        "candidate_counts": raw_bridge.get("shadow_no_decision_entry_hypothesis_candidate_counts") or [],
        "reason_counts": raw_bridge.get("shadow_no_decision_entry_hypothesis_reason_counts") or [],
        "signal_examples": signal_examples,
        "nearby_signal_id_mismatch_count": count_for_root(
            no_decision_subroots,
            "token_time_decision_nearby_signal_id_mismatch",
        ),
        "missing_signal_id_decision_count": count_for_root(
            no_decision_subroots,
            "token_time_decision_missing_signal_id",
        ),
        "allowed_scope": [
            "read-only decision bridge attribution audit",
            "evaluator/report/dashboard artifact improvements",
            "shadow-only candidate/context instrumentation",
        ],
        "forbidden_scope": [
            "strategy change",
            "entry policy change",
            "hard/exit gate change",
            "final_entry_contract change",
            "A_CLASS mode reset or enablement",
            "paper/live executor enablement",
            "canary/risk increase",
        ],
        "promotion_allowed": False,
        "paper_enablement_allowed": False,
        "automatic_runtime_change_allowed": False,
        "automatic_bridge_to_entry_allowed": False,
        "human_approval_required_if_next_step_requires": [
            "connecting shadow matches to production entry policy",
            "creating pending entries from shadow candidates",
            "enabling paper/live execution",
            "changing final_entry_contract or A_CLASS mode",
        ],
        "notes": [
            "This report is extracted from raw_gold_silver_funnel_audit and does not rescan or mutate trading state.",
            "A shadow match without a decision record is a bridge/instrumentation finding, not promotion evidence.",
            "The optimistic decision-record rate is an attribution upper bound only; it does not imply pass, pending, final eligibility, paper capture, or realized capture.",
        ],
    }


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def improvement_priority(row):
    recall_lift = safe_float(row.get("recall_lift_vs_candidate_baseline")) or 0.0
    precision = safe_float(row.get("match_precision_event")) or 0.0
    matched = row.get("matched_gold_silver_events") or row.get("matched_raw_all_gold_silver_events") or 0
    signals = row.get("slice_signal_count") or row.get("signal_count") or 0
    return (recall_lift, precision, matched, signals)


def build_candidate_improvement_opportunities_report(capture, candidate_effectiveness, cross_validity):
    """Convert capture evidence into shadow-only candidate improvement opportunities."""
    candidate_class = {
        row.get("candidate_id"): row
        for row in (candidate_effectiveness.get("top_candidates") or [])
    }
    valid_crosses = list(cross_validity.get("valid_top_crosses") or [])
    invalid_crosses = list(cross_validity.get("invalid_sample") or [])
    watchlist = list(capture.get("watchlist_hypotheses") or [])
    opportunities = []

    for row in valid_crosses:
        if row.get("judgment") not in {"DISCOVERY_HIT", "WATCH"}:
            continue
        candidate_id = row.get("candidate_id")
        base = candidate_class.get(candidate_id) or {}
        classification = base.get("classification")
        dimension = row.get("dimension")
        slice_value = row.get("slice_value")
        if classification == "potential_entry_hypothesis":
            opportunity_type = "refine_potential_entry_hypothesis_with_context"
        elif classification == "true_detector":
            opportunity_type = "derive_context_filtered_shadow_candidate"
        else:
            opportunity_type = "track_context_slice_shadow_only"
        opportunities.append({
            "opportunity_type": opportunity_type,
            "scope": "shadow_only_candidate_or_context_feature",
            "candidate_id": candidate_id,
            "candidate_classification": classification,
            "family": row.get("family"),
            "dimension": dimension,
            "slice_value": slice_value,
            "evidence_level": "discovery_same_window",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "suggested_action": (
                "register_shadow_context_hypothesis"
                if opportunity_type != "track_context_slice_shadow_only"
                else "continue_shadow_tracking"
            ),
            "metrics": {
                key: row.get(key)
                for key in (
                    "matched_gold_silver_events",
                    "match_recall_event",
                    "match_precision_event",
                    "recall_lift_vs_candidate_baseline",
                )
            },
            "blocked_by": [],
            "next_validation": "repeat_same_definition_in_next_clean_window_then_oos_if_repeated",
        })

    for row in invalid_crosses:
        if row.get("judgment") not in {"DISCOVERY_HIT", "WATCH"}:
            continue
        reasons = list(row.get("invalid_reasons") or [])
        opportunities.append({
            "opportunity_type": "blocked_context_slice_candidate_opportunity",
            "scope": "shadow_only_after_context_coverage_fix",
            "candidate_id": row.get("candidate_id"),
            "candidate_classification": (candidate_class.get(row.get("candidate_id")) or {}).get("classification"),
            "family": row.get("family"),
            "dimension": row.get("dimension"),
            "slice_value": row.get("slice_value"),
            "evidence_level": "blocked_discovery_same_window",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "suggested_action": "do_not_register_until_context_coverage_passes",
            "metrics": {
                key: row.get(key)
                for key in (
                    "matched_gold_silver_events",
                    "match_recall_event",
                    "match_precision_event",
                    "recall_lift_vs_candidate_baseline",
                )
            },
            "blocked_by": reasons,
            "next_validation": "rerun_after_context_coverage_clean_window",
        })

    # Include capture watchlist rows that may not appear in the top cross-validity sample.
    existing_keys = {
        (row.get("candidate_id"), row.get("dimension"), row.get("slice_value"))
        for row in opportunities
    }
    for row in watchlist[:100]:
        key = (row.get("candidate_id"), row.get("dimension"), row.get("slice_value"))
        if key in existing_keys:
            continue
        if row.get("judgment") not in {"DISCOVERY_HIT", "WATCH"}:
            continue
        opportunities.append({
            "opportunity_type": "capture_watchlist_shadow_candidate_opportunity",
            "scope": "shadow_only_candidate_or_context_feature",
            "candidate_id": row.get("candidate_id"),
            "candidate_classification": (candidate_class.get(row.get("candidate_id")) or {}).get("classification"),
            "family": row.get("family"),
            "dimension": row.get("dimension"),
            "slice_value": row.get("slice_value"),
            "evidence_level": "discovery_same_window",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "suggested_action": "register_shadow_context_hypothesis_if_context_validity_passes",
            "metrics": {
                key: row.get(key)
                for key in (
                    "matched_gold_silver_events",
                    "match_recall_event",
                    "match_precision_event",
                    "recall_lift_vs_candidate_baseline",
                    "precision_lift_vs_candidate_baseline",
                )
            },
            "blocked_by": ["context_validity_not_checked_in_top_sample"],
            "next_validation": "include_in_capture_cross_validity_or_rerun_with_larger_sample",
        })

    opportunities.sort(key=improvement_priority, reverse=True)
    top = opportunities[:50]
    return {
        "schema_version": "candidate_improvement_opportunities.v1",
        "report_type": "candidate_improvement_opportunities_24h",
        "generated_at": utc_now(),
        "evidence_level": "discovery_same_window",
        "usage": "shadow_only_research_queue",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "opportunity_count": len(opportunities),
        "top_opportunities": top,
        "opportunity_type_counts": {
            kind: sum(1 for row in opportunities if row.get("opportunity_type") == kind)
            for kind in sorted({row.get("opportunity_type") for row in opportunities})
        },
        "blocked_context_opportunity_count": sum(1 for row in opportunities if row.get("blocked_by")),
        "notes": [
            "This report is a shadow-only research queue. It never promotes candidates or changes runtime policy.",
            "Opportunities must repeat in a clean future window and pass OOS before human review.",
        ],
    }


def build_markov_effectiveness_report(markov_reports, capture):
    profiles = {}
    total_green = 0
    total_yellow = 0
    total_insufficient = 0
    profile_diagnostics = {}
    non_informative_reasons = {}
    context_blockers = [
        blocker for blocker in ((capture.get("report_health") or {}).get("promotion_blockers") or [])
        if "coverage" in str(blocker) or "schema" in str(blocker)
    ]

    def as_int(value, default=0):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def profile_context_blockers(dimensions):
        blockers = []
        dims = set(dimensions or [])
        if "source_quote_clean" in dims or "source_quote_executable" in dims:
            blockers.extend(
                blocker for blocker in context_blockers
                if "quote" in str(blocker)
            )
        if "volume_profile" in dims:
            blockers.extend(
                blocker for blocker in context_blockers
                if "volume_profile" in str(blocker)
            )
        if "candle_pattern" in dims or "fbr_time_legal" in dims or "fbr_lookahead_warning" in dims:
            blockers.extend(
                blocker for blocker in context_blockers
                if "kline" in str(blocker)
            )
        if "lifecycle_profile" in dims:
            blockers.extend(
                blocker for blocker in context_blockers
                if "lifecycle_profile" in str(blocker)
            )
        if "source_component" in dims:
            blockers.extend(
                blocker for blocker in context_blockers
                if "source_component" in str(blocker)
            )
        return sorted(set(blockers))

    def diagnose_profile(name, report):
        coverage = report.get("coverage") or {}
        counts = coverage.get("bucket_counts") or {}
        dimensions = report.get("key_dimensions") or []
        closed_virtual_rows = as_int(coverage.get("closed_virtual_rows"))
        keys_emitted = as_int(coverage.get("keys_emitted"))
        green = as_int(counts.get("green"))
        yellow = as_int(counts.get("yellow"))
        red = as_int(counts.get("red"))
        insufficient = as_int(counts.get("insufficient"))
        informative = green + yellow
        blockers = profile_context_blockers(dimensions)
        if not closed_virtual_rows:
            status = "no_closed_virtual_rows"
            reason = "no_virtual_closed_rows_in_window"
        elif keys_emitted == 0:
            status = "profile_over_fragmented_or_min_closed_not_met"
            reason = "closed_rows_exist_but_no_bucket_reached_min_closed"
        elif informative:
            status = "informative_discovery_only"
            reason = "has_yellow_or_green_research_bucket"
        elif red:
            status = "red_only_no_positive_bucket"
            reason = "all_emitted_buckets_are_red"
        elif insufficient:
            status = "insufficient_only"
            reason = "all_emitted_buckets_are_insufficient"
        else:
            status = "empty_no_markov_keys"
            reason = "no_bucket_counts_available"
        if blockers and status != "informative_discovery_only":
            reason = f"{reason}_with_context_coverage_blockers"
        return {
            "profile": report.get("profile") or name,
            "key_dimensions": dimensions,
            "closed_virtual_rows": closed_virtual_rows,
            "keys_emitted": keys_emitted,
            "bucket_counts": counts,
            "green_buckets": green,
            "yellow_buckets": yellow,
            "red_buckets": red,
            "insufficient_buckets": insufficient,
            "informative_bucket_count": informative,
            "status": status,
            "non_informative_reason": reason if not informative else None,
            "context_blockers_affecting_profile": blockers,
            "usage": report.get("usage") or "discovery_only",
            "promotion_allowed": False,
        }

    for name, report in sorted(markov_reports.items()):
        coverage = report.get("coverage") or {}
        counts = coverage.get("bucket_counts") or {}
        diagnostic = diagnose_profile(name, report)
        profile_diagnostics[name] = diagnostic
        if diagnostic["non_informative_reason"]:
            non_informative_reasons[name] = diagnostic["non_informative_reason"]
        total_green += diagnostic["green_buckets"]
        total_yellow += diagnostic["yellow_buckets"]
        total_insufficient += diagnostic["insufficient_buckets"]
        profiles[name] = {
            "schema_version": report.get("schema_version"),
            "profile": report.get("profile") or name,
            "key_dimensions": report.get("key_dimensions") or [],
            "bucket_counts": counts,
            "coverage": coverage,
            "usage": report.get("usage") or "discovery_only",
        }
    observed_profiles = set(markov_reports)
    recommended_shadow_profiles = [
        {
            "profile": "candidate_lifecycle",
            "key_dimensions": ["candidate_id", "lifecycle_profile"],
            "status": "observed" if "candidate_lifecycle" in observed_profiles else "recommended_to_run",
            "blocked_by": [
                blocker for blocker in context_blockers
                if "lifecycle_profile" in str(blocker)
            ],
        },
        {
            "profile": "candidate_source",
            "key_dimensions": ["candidate_id", "source_component"],
            "status": "observed" if "candidate_source" in observed_profiles else "recommended_to_run",
            "blocked_by": [
                blocker for blocker in context_blockers
                if "source_component" in str(blocker)
            ],
        },
        {
            "profile": "candidate_signal_type",
            "key_dimensions": ["candidate_id", "signal_type"],
            "status": "observed" if "candidate_signal_type" in observed_profiles else "recommended_to_run",
            "blocked_by": [],
        },
        {
            "profile": "candidate_lifecycle_source",
            "key_dimensions": ["candidate_id", "lifecycle_profile", "source_component"],
            "status": "observed" if "candidate_lifecycle_source" in observed_profiles else "recommended_to_run",
            "blocked_by": [
                blocker for blocker in context_blockers
                if "lifecycle_profile" in str(blocker) or "source_component" in str(blocker)
            ],
        },
        {
            "profile": "candidate_volume",
            "key_dimensions": ["candidate_id", "volume_profile"],
            "status": (
                "blocked_until_volume_profile_coverage_gte_80pct"
                if any("volume_profile" in str(blocker) for blocker in context_blockers)
                else ("observed" if "candidate_volume" in observed_profiles else "recommended_to_run")
            ),
            "blocked_by": [
                blocker for blocker in context_blockers
                if "volume_profile" in str(blocker)
            ],
        },
        {
            "profile": "candidate_quote",
            "key_dimensions": ["candidate_id", "source_quote_clean"],
            "status": (
                "blocked_until_quote_context_coverage_gte_80pct"
                if any("quote" in str(blocker) for blocker in context_blockers)
                else ("observed" if "candidate_quote" in observed_profiles else "recommended_to_run")
            ),
            "blocked_by": [
                blocker for blocker in context_blockers
                if "quote" in str(blocker)
            ],
        },
    ]
    if total_green or total_yellow:
        status = "informative_discovery_only"
        next_action = "keep_markov_discovery_only_and_require_capture_oos_before_promotion"
    elif any(row["keys_emitted"] == 0 and row["closed_virtual_rows"] for row in profile_diagnostics.values()):
        status = "profile_fragmented_or_uninformative"
        next_action = "run_or_review_coarse_non_quote_non_kline_profiles_before_claiming_markov_value"
    elif any(row["red_buckets"] for row in profile_diagnostics.values()):
        status = "red_only_non_informative"
        next_action = "do_not_use_markov_as_positive_evidence; keep as discovery_risk_context_only"
    else:
        status = "insufficient_or_uninformative"
        next_action = "collect_more_closed_virtual_rows_and_keep_markov_out_of_promotion"
    return {
        "schema_version": "markov_effectiveness_report.v2",
        "report_type": "markov_effectiveness_24h",
        "status": status,
        "next_action": next_action,
        "evidence_level": "discovery_same_window",
        "usage": "research_only_markov_information_value",
        "promotion_allowed": False,
        "markov_used_for_promotion": False,
        "total_green_buckets": total_green,
        "total_yellow_buckets": total_yellow,
        "total_insufficient_buckets": total_insufficient,
        "profiles": profiles,
        "profile_diagnostics": profile_diagnostics,
        "non_informative_reasons": non_informative_reasons,
        "recommended_shadow_profiles": recommended_shadow_profiles,
        "context_blockers": context_blockers,
        "notes": [
            "This report explains Markov information value only. It never promotes candidates or changes runtime policy.",
            "Quote, volume, and kline-sensitive profiles stay blocked while their coverage is below the configured threshold.",
        ],
    }


def build_capture_cross_validity_report(capture, context_report):
    quote_rate = ((context_report.get("quote_context_coverage") or {}).get("source_quote_clean_present_rate") or 0)
    quote_exec_rate = ((context_report.get("quote_context_coverage") or {}).get("source_quote_executable_present_rate") or 0)
    source_component_rate = ((context_report.get("source_component_coverage") or {}).get("effective_present_rate"))
    if source_component_rate is None:
        source_component_rate = ((context_report.get("source_component_coverage") or {}).get("coverage_rate") or 0)
    volume_rate = ((context_report.get("volume_profile_coverage") or {}).get("coverage_rate") or 0)
    kline_rate = ((context_report.get("kline_coverage") or {}).get("coverage_rate") or 0)
    valid = []
    invalid = []
    for row in capture.get("context_slices") or []:
        dim = row.get("dimension")
        reasons = []
        if dim in {"source_quote_clean", "source_quote_executable", "source_quote_executable_proxy"} and (quote_rate < 0.8 or quote_exec_rate < 0.8):
            reasons.append("quote_context_coverage_below_80pct")
        if dim == "source_component" and source_component_rate < 0.8:
            reasons.append("source_component_coverage_below_80pct")
        if dim == "volume_profile" and volume_rate < 0.8:
            reasons.append("volume_profile_coverage_below_80pct")
        if dim in {"candle_pattern", "fbr_time_legal", "fbr_lookahead_warning"} and kline_rate < 0.8:
            reasons.append("kline_coverage_below_80pct")
        item = {
            "candidate_id": row.get("candidate_id"),
            "family": row.get("family"),
            "dimension": dim,
            "slice_value": row.get("slice_value"),
            "judgment": row.get("judgment"),
            "matched_gold_silver_events": row.get("matched_gold_silver_events"),
            "match_recall_event": row.get("match_recall_event"),
            "match_precision_event": row.get("match_precision_event"),
            "recall_lift_vs_candidate_baseline": row.get("recall_lift_vs_candidate_baseline"),
            "valid": not reasons,
            "invalid_reasons": reasons,
        }
        (valid if not reasons else invalid).append(item)
    return {
        "schema_version": "capture_cross_validity_report.v1",
        "report_type": "capture_cross_validity_24h",
        "valid_cross_count": len(valid),
        "invalid_cross_count": len(invalid),
        "valid_top_crosses": valid[:50],
        "invalid_reason_counts": {
            reason: sum(1 for row in invalid for reason in row["invalid_reasons"])
            for reason in sorted({reason for row in invalid for reason in row["invalid_reasons"]})
        },
        "invalid_sample": invalid[:50],
        "criteria": {
            "quote_sensitive_requires_present_rate_gte": 0.8,
            "source_component_requires_present_rate_gte": 0.8,
            "volume_sensitive_requires_present_rate_gte": 0.8,
            "kline_sensitive_requires_coverage_rate_gte": 0.8,
            "pnl_is_secondary": True,
        },
    }


def build_a_class_fastlane_mode_audit(raw_funnel, context_report):
    summary = raw_funnel.get("summary") or {}
    entry_bridge = summary.get("entry_bridge_layer") or {}
    final_contract = entry_bridge.get("final_entry_contract") or {}
    hard_blockers = final_contract.get("hard_blockers") or {}
    mode_status = final_contract.get("mode_status") or {}
    context_blockers = context_report.get("blockers") or []
    mode_disabled_count = int(hard_blockers.get("mode_disabled") or 0)
    clean_windows_passed = not any(
        blocker for blocker in context_blockers
        if blocker in {
            "source_quote_clean_coverage_below_80pct",
            "source_quote_executable_coverage_below_80pct",
            "volume_profile_coverage_below_80pct",
            "kline_coverage_below_80pct",
        }
    )
    if mode_disabled_count and clean_windows_passed:
        final_entry_status = "FUNNEL_BLOCKED_STUCK"
        human_action_required = True
        reason = "final_entry_contract_mode_disabled_after_clean_windows"
    elif mode_disabled_count:
        final_entry_status = "FUNNEL_BLOCKED_EXPECTED"
        human_action_required = False
        reason = "final_entry_contract_mode_disabled_while_context_blockers_remain"
    else:
        final_entry_status = "READINESS_AUDIT_ONLY"
        human_action_required = False
        reason = "no_final_entry_mode_disabled_blocker_detected"
    return {
        "schema_version": "a_class_fastlane_mode_audit.v1",
        "report_type": "a_class_fastlane_mode_audit_24h",
        "A_CLASS_mode_status": mode_status,
        "final_entry_status": final_entry_status,
        "reason": reason,
        "human_action_required": human_action_required,
        "raw_gold_silver_entered_events": (summary.get("raw_denominator") or {}).get("entered_events"),
        "decision_layer": summary.get("decision_layer") or {},
        "entry_bridge_layer_summary": {
            "paper_trades_entry_ts_window_count": entry_bridge.get("paper_trades_entry_ts_window_count"),
            "raw_signals_with_final_entry_contract": entry_bridge.get("raw_signals_with_final_entry_contract"),
            "paper_evidence_log": entry_bridge.get("paper_evidence_log"),
        },
        "final_entry_contract_blocker_breakdown": hard_blockers,
        "final_entry_contract": final_contract,
    }


def first_blocker_priority(blockers):
    priority = [
        "candidate_count_mismatch",
        "candidate_count_observed_not_84",
        "observation_coverage_below_99pct",
        "raw_dog_rows_incomplete",
        "signal_id_join_rate_below_99pct",
        "source_quote_clean_coverage_below_80pct",
        "source_quote_executable_coverage_below_80pct",
        "volume_profile_coverage_below_80pct",
        "kline_coverage_below_80pct",
        "markov_bucket_coverage_below_80pct",
        "report_generation_failed",
    ]
    for item in priority:
        if item in blockers:
            return item
    return blockers[0] if blockers else None


def oos_probe_label(hours):
    text = ("%s" % hours).rstrip("0").rstrip(".")
    if not text:
        text = "0"
    return text.replace(".", "p") + "h"


def parse_oos_probe_hours(value):
    hours = []
    for item in str(value or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            parsed = float(item)
        except ValueError:
            continue
        if parsed > 0:
            hours.append(parsed)
    return hours


def parse_capture_hours(value, primary_hours):
    hours = {int(primary_hours)}
    for item in str(value or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            parsed = int(float(item))
        except ValueError:
            continue
        if parsed > 0:
            hours.add(parsed)
    return sorted(hours)


def run_reports(run_dir, args):
    primary_hours = int(args.hours)
    capture_path = run_dir / f"capture_discovery_{primary_hours}h.json"
    capture_hours = parse_capture_hours(getattr(args, "capture_hours", "24,48,72"), primary_hours)
    capture_paths = {
        hours: run_dir / f"capture_discovery_{hours}h.json"
        for hours in capture_hours
    }
    pnl_path = run_dir / f"pnl_cross_secondary_{primary_hours}h.json"
    raw_funnel_path = run_dir / f"raw_gold_silver_funnel_audit_{primary_hours}h.json"
    shadow_decision_bridge_path = run_dir / f"shadow_decision_bridge_audit_{primary_hours}h.json"
    candidate_downstream_path = run_dir / f"candidate_downstream_readiness_{primary_hours}h.json"
    context_coverage_path = run_dir / f"context_coverage_audit_{primary_hours}h.json"
    candidate_effectiveness_path = run_dir / f"candidate_effectiveness_{primary_hours}h.json"
    candidate_improvement_path = run_dir / f"candidate_improvement_opportunities_{primary_hours}h.json"
    markov_effectiveness_path = run_dir / f"markov_effectiveness_{primary_hours}h.json"
    capture_cross_validity_path = run_dir / f"capture_cross_validity_{primary_hours}h.json"
    a_class_fastlane_path = run_dir / f"a_class_fastlane_mode_audit_{primary_hours}h.json"
    runtime_health_snapshot_path = run_dir / f"runtime_health_snapshot_{primary_hours}h.json"
    context_blocker_monitor_path = run_dir / f"context_blocker_monitor_{primary_hours}h.json"
    volume_kline_audit_path = run_dir / f"volume_kline_coverage_audit_{primary_hours}h.json"
    matured_kline_recheck_path = run_dir / f"matured_kline_volume_recheck_audit_{primary_hours}h.json"
    matured_volume_capture_cross_path = run_dir / f"matured_volume_capture_cross_audit_{primary_hours}h.json"
    hypothesis_validation_path = run_dir / f"hypothesis_validation_audit_{primary_hours}h.json"
    low_confidence_research_path = run_dir / f"low_confidence_research_capture_audit_{primary_hours}h.json"
    quality_timing_research_path = run_dir / f"quality_timing_reject_research_audit_{primary_hours}h.json"
    markov_paths = {
        profile: run_dir / f"candidate_virtual_markov_{profile}_{primary_hours}h.json"
        for profile in args.markov_profiles.split(",")
        if profile
    }
    diagnostics = []
    readiness_paths = {}
    diagnostics.append(run_report(
        "runtime_health_snapshot",
        [
            "scripts/runtime_health_snapshot_audit.py",
            "--data-dir", args.data_dir,
            "--hours", str(primary_hours),
            "--out", str(runtime_health_snapshot_path),
        ],
        runtime_health_snapshot_path,
        timeout=args.report_timeout_sec,
    ))
    if runtime_health_snapshot_path.exists():
        readiness_paths["runtime_health_snapshot"] = runtime_health_snapshot_path

    db_ready = file_available(args.paper_db) and sqlite_has_table(args.paper_db, "candidate_shadow_observations")
    raw_ready = file_available(args.raw_db) and sqlite_has_table(args.raw_db, "raw_signal_outcomes")
    if not db_ready:
        capture = blocked_capture_report("paper_db_unavailable_or_missing_candidate_shadow_observations", args.paper_db, args.raw_db, primary_hours, args.expected_candidates)
        write_json(capture_path, capture)
        diagnostics.append({"name": "db_guard", "ok": False, "reason": "paper_db_unavailable_or_missing_candidate_shadow_observations"})
        return {"capture_primary": capture_path, "pnl": None, "markov": {}, "readiness": readiness_paths, "diagnostics": diagnostics}
    if not raw_ready:
        capture = blocked_capture_report("raw_signal_outcomes_db_unavailable", args.paper_db, args.raw_db, primary_hours, args.expected_candidates)
        write_json(capture_path, capture)
        diagnostics.append({"name": "db_guard", "ok": False, "reason": "raw_signal_outcomes_db_unavailable"})
        return {"capture_primary": capture_path, "pnl": None, "markov": {}, "readiness": readiness_paths, "diagnostics": diagnostics}

    for hours, path in capture_paths.items():
        diagnostics.append(run_report(
            f"capture_discovery_{hours}h",
            [
                "scripts/offline_candidate_capture_discovery.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--hours", str(hours),
                "--expected-candidates", str(args.expected_candidates),
                "--max-scan-rows", str(args.max_scan_rows),
                "--out", str(path),
            ],
            path,
            timeout=args.report_timeout_sec,
        ))
    if primary_hours in capture_paths:
        capture_path = capture_paths[primary_hours]

    diagnostics.append(run_report(
        "raw_gold_silver_funnel_audit",
        [
            "scripts/offline_raw_gold_silver_funnel_audit.py",
            "--db", args.paper_db,
            "--raw-db", args.raw_db,
            "--hours", str(primary_hours),
            "--expected-candidates", str(args.expected_candidates),
            "--out", str(raw_funnel_path),
        ],
        raw_funnel_path,
        timeout=args.report_timeout_sec,
    ))
    diagnostics.append(run_report(
        "pnl_cross_secondary",
        [
            "scripts/offline_candidate_cross_eval.py",
            "--db", args.paper_db,
            "--hours", str(primary_hours),
            "--max-scan-rows", str(args.max_scan_rows),
            "--out", str(pnl_path),
        ],
        pnl_path,
        timeout=args.report_timeout_sec,
    ))
    diagnostics.append(run_report(
        "candidate_downstream_readiness",
        [
            "scripts/candidate_downstream_readiness_audit.py",
            "--db", args.paper_db,
            "--raw-db", args.raw_db,
            "--hours", str(primary_hours),
            "--expected-candidates", str(args.expected_candidates),
            "--out", str(candidate_downstream_path),
        ],
        candidate_downstream_path,
        timeout=args.report_timeout_sec,
    ))
    if candidate_downstream_path.exists():
        readiness_paths["candidate_downstream_readiness"] = candidate_downstream_path
    successful_markov = {}
    for profile, path in markov_paths.items():
        diagnostics.append(run_report(
            f"virtual_markov_{profile}",
            [
                "scripts/build_candidate_virtual_markov.py",
                "--db", args.paper_db,
                "--hours", str(primary_hours),
                "--profile", profile,
                "--max-scan-rows", str(args.max_scan_rows),
                "--out", str(path),
            ],
            path,
            timeout=args.report_timeout_sec,
        ))
        if path.exists():
            successful_markov[profile] = path
    try:
        capture = load_json(capture_path)
        context_report = build_context_coverage_report(capture)
        write_derived_report(context_coverage_path, context_report)
        readiness_paths["context_coverage"] = context_coverage_path
        downstream = load_json(candidate_downstream_path) if candidate_downstream_path.exists() else {}
        candidate_effectiveness = build_candidate_effectiveness_report(capture, downstream)
        write_derived_report(candidate_effectiveness_path, candidate_effectiveness)
        readiness_paths["candidate_effectiveness"] = candidate_effectiveness_path
        markov_reports = {name: load_json(path) for name, path in successful_markov.items() if Path(path).exists()}
        write_derived_report(markov_effectiveness_path, build_markov_effectiveness_report(markov_reports, capture))
        readiness_paths["markov_effectiveness"] = markov_effectiveness_path
        capture_cross_validity = build_capture_cross_validity_report(capture, context_report)
        write_derived_report(capture_cross_validity_path, capture_cross_validity)
        readiness_paths["capture_cross_validity"] = capture_cross_validity_path
        write_derived_report(
            candidate_improvement_path,
            build_candidate_improvement_opportunities_report(capture, candidate_effectiveness, capture_cross_validity),
        )
        readiness_paths["candidate_improvement_opportunities"] = candidate_improvement_path
        if raw_funnel_path.exists():
            raw_funnel = load_json(raw_funnel_path)
        else:
            raw_funnel = {}
        write_derived_report(
            shadow_decision_bridge_path,
            build_shadow_decision_bridge_audit(raw_funnel),
        )
        readiness_paths["shadow_decision_bridge_audit"] = shadow_decision_bridge_path
        write_derived_report(a_class_fastlane_path, build_a_class_fastlane_mode_audit(raw_funnel, context_report))
        readiness_paths["a_class_fastlane_mode_audit"] = a_class_fastlane_path
        # Backward-compatible alias for older readers.
        legacy_capture = run_dir / f"candidate_capture_discovery_{primary_hours}h.json"
        if capture_path.exists() and legacy_capture != capture_path:
            shutil.copy2(capture_path, legacy_capture)
            readiness_paths["legacy_candidate_capture"] = legacy_capture
    except Exception as exc:
        diagnostics.append({"name": "derived_readiness_reports", "ok": False, "error": repr(exc), "duration_sec": None})
    for hours, path in capture_paths.items():
        if path.exists():
            readiness_paths[f"capture_{hours}h"] = path
    if raw_funnel_path.exists():
        readiness_paths["raw_gold_silver_funnel_audit"] = raw_funnel_path
    if shadow_decision_bridge_path.exists():
        readiness_paths["shadow_decision_bridge_audit"] = shadow_decision_bridge_path
    diagnostics.append(run_report(
        "volume_kline_coverage_audit",
        [
            "scripts/volume_kline_coverage_audit.py",
            "--db", args.paper_db,
            "--raw-db", args.raw_db,
            "--hours", str(primary_hours),
            "--out", str(volume_kline_audit_path),
        ],
        volume_kline_audit_path,
        timeout=args.report_timeout_sec,
    ))
    if volume_kline_audit_path.exists():
        readiness_paths["volume_kline_coverage_audit"] = volume_kline_audit_path
    diagnostics.append(run_report(
        "matured_kline_volume_recheck_audit",
        [
            "scripts/matured_kline_volume_recheck_audit.py",
            "--db", args.paper_db,
            "--kline-db", args.kline_db,
            "--hours", str(primary_hours),
            "--out", str(matured_kline_recheck_path),
        ],
        matured_kline_recheck_path,
        timeout=args.report_timeout_sec,
    ))
    if matured_kline_recheck_path.exists():
        readiness_paths["matured_kline_volume_recheck_audit"] = matured_kline_recheck_path
    diagnostics.append(run_report(
        "matured_volume_capture_cross_audit",
        [
            "scripts/matured_volume_capture_cross_audit.py",
            "--db", args.paper_db,
            "--raw-db", args.raw_db,
            "--kline-db", args.kline_db,
            "--hours", str(primary_hours),
            "--expected-candidates", str(args.expected_candidates),
            "--max-scan-rows", str(args.max_scan_rows),
            "--out", str(matured_volume_capture_cross_path),
        ],
        matured_volume_capture_cross_path,
        timeout=args.report_timeout_sec,
    ))
    if matured_volume_capture_cross_path.exists():
        readiness_paths["matured_volume_capture_cross_audit"] = matured_volume_capture_cross_path
    diagnostics.append(run_report(
        "hypothesis_validation_audit",
        [
            "scripts/hypothesis_validation_audit.py",
            "--registry", args.registry,
            "--matured-volume-cross", str(matured_volume_capture_cross_path),
            "--out", str(hypothesis_validation_path),
        ],
        hypothesis_validation_path,
        timeout=args.report_timeout_sec,
    ))
    if hypothesis_validation_path.exists():
        readiness_paths["hypothesis_validation_audit"] = hypothesis_validation_path
    for probe_hours in parse_oos_probe_hours(getattr(args, "oos_probe_hours", "")):
        label = oos_probe_label(probe_hours)
        oos_cross_path = run_dir / f"matured_volume_capture_cross_audit_oos_probe_{label}.json"
        oos_validation_path = run_dir / f"hypothesis_validation_audit_oos_probe_{label}.json"
        diagnostics.append(run_report(
            f"matured_volume_capture_cross_oos_probe_{label}",
            [
                "scripts/matured_volume_capture_cross_audit.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--kline-db", args.kline_db,
                "--hours", str(probe_hours),
                "--expected-candidates", str(args.expected_candidates),
                "--max-scan-rows", str(args.max_scan_rows),
                "--out", str(oos_cross_path),
            ],
            oos_cross_path,
            timeout=args.report_timeout_sec,
        ))
        if oos_cross_path.exists():
            readiness_paths[f"matured_volume_cross_oos_probe_{label}"] = oos_cross_path
        diagnostics.append(run_report(
            f"hypothesis_validation_oos_probe_{label}",
            [
                "scripts/hypothesis_validation_audit.py",
                "--registry", args.registry,
                "--matured-volume-cross", str(oos_cross_path),
                "--out", str(oos_validation_path),
            ],
            oos_validation_path,
            timeout=args.report_timeout_sec,
        ))
        if oos_validation_path.exists():
            readiness_paths[f"hypothesis_validation_oos_probe_{label}"] = oos_validation_path
    diagnostics.append(run_report(
        "low_confidence_research_capture_audit",
        [
            "scripts/low_confidence_research_capture_audit.py",
            "--db", args.paper_db,
            "--raw-db", args.raw_db,
            "--hours", str(primary_hours),
            "--expected-candidates", str(args.expected_candidates),
            "--out", str(low_confidence_research_path),
        ],
        low_confidence_research_path,
        timeout=args.report_timeout_sec,
    ))
    if low_confidence_research_path.exists():
        readiness_paths["low_confidence_research_capture_audit"] = low_confidence_research_path
    diagnostics.append(run_report(
        "quality_timing_reject_research_audit",
        [
            "scripts/quality_timing_reject_research_audit.py",
            "--db", args.paper_db,
            "--raw-db", args.raw_db,
            "--raw-funnel", str(raw_funnel_path),
            "--hours", str(primary_hours),
            "--expected-candidates", str(args.expected_candidates),
            "--out", str(quality_timing_research_path),
        ],
        quality_timing_research_path,
        timeout=args.report_timeout_sec,
    ))
    if quality_timing_research_path.exists():
        readiness_paths["quality_timing_reject_research_audit"] = quality_timing_research_path
    context_monitor_cmd = [
        "scripts/context_blocker_monitor.py",
        "--db", args.paper_db,
        "--raw-db", args.raw_db,
        "--hours", str(primary_hours),
        "--out", str(context_blocker_monitor_path),
    ]
    if int(args.quote_fix_deploy_ts or 0) > 0:
        context_monitor_cmd.extend(["--deploy-ts", str(int(args.quote_fix_deploy_ts))])
    diagnostics.append(run_report(
        "context_blocker_monitor",
        context_monitor_cmd,
        context_blocker_monitor_path,
        timeout=args.report_timeout_sec,
    ))
    if context_blocker_monitor_path.exists():
        readiness_paths["context_blocker_monitor"] = context_blocker_monitor_path
    diagnostics.append(run_report(
        "a_class_fastlane_mode_readiness_audit",
        [
            "scripts/a_class_fastlane_mode_readiness_audit.py",
            "--db", args.paper_db,
            "--raw-funnel", str(raw_funnel_path),
            "--context-coverage", str(context_coverage_path),
            "--volume-kline-audit", str(volume_kline_audit_path),
            "--context-blocker-monitor", str(context_blocker_monitor_path),
            "--hours", str(primary_hours),
            "--out", str(a_class_fastlane_path),
        ],
        a_class_fastlane_path,
        timeout=args.report_timeout_sec,
    ))
    if a_class_fastlane_path.exists():
        readiness_paths["a_class_fastlane_mode_audit"] = a_class_fastlane_path
    diagnostics.append(run_report(
        "runtime_health_snapshot_final",
        [
            "scripts/runtime_health_snapshot_audit.py",
            "--data-dir", args.data_dir,
            "--hours", str(primary_hours),
            "--out", str(runtime_health_snapshot_path),
        ],
        runtime_health_snapshot_path,
        timeout=args.report_timeout_sec,
    ))
    if runtime_health_snapshot_path.exists():
        readiness_paths["runtime_health_snapshot"] = runtime_health_snapshot_path
    return {
        "capture_primary": capture_path,
        "pnl": pnl_path if pnl_path.exists() else None,
        "markov": successful_markov,
        "readiness": readiness_paths,
        "diagnostics": diagnostics,
    }


def compact_hypothesis(metrics):
    best = metrics.get("best_slice") or {}
    return {
        "definition": metrics.get("definition") or {},
        "status": metrics.get("status"),
        "rows_found": metrics.get("rows_found"),
        "latest_best_slice": best,
    }


def compact_matured_volume_hypothesis(row):
    candidate_id = row.get("candidate_id")
    slice_value = row.get("slice_value")
    return {
        "hypothesis_id": f"matured_volume:{candidate_id}:{slice_value}",
        "evidence_level": "discovery_same_window",
        "scope": "shadow_only_matured_volume_context",
        "promotion_allowed": False,
        "definition": {
            "candidate_id": candidate_id,
            "dimension": row.get("dimension") or "matured_volume_profile",
            "slice_value": slice_value,
        },
        "latest_metrics": {
            key: row.get(key)
            for key in (
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
                "verdict",
            )
        },
        "next_validation": "track_same_definition_in_next_clean_window_then_oos_if_repeated",
    }


def compact_quality_timing_shadow_hypothesis(row):
    cluster = row.get("cluster")
    return {
        "hypothesis_id": f"quality_timing:{cluster}",
        "evidence_level": "discovery_same_window",
        "scope": "shadow_only_quality_timing_reject_cluster",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "definition": {
            "cluster": cluster,
            "dominant_stage_filter": [
                item.get("stage")
                for item in (row.get("stage_counts") or [])
                if item.get("stage")
            ],
            "suggested_shadow_only_action": row.get("suggested_shadow_only_action"),
        },
        "latest_metrics": {
            key: row.get(key)
            for key in (
                "event_count",
                "share_of_quality_timing_rejects",
                "share_of_raw_all_gold_silver",
                "unique_tokens",
                "candidate_matched_any_rate",
                "max_sustained_peak_pct_max",
                "time_to_sustained_peak_sec_median",
            )
        },
        "top_candidates": (row.get("top_candidates") or [])[:10],
        "top_families": (row.get("top_families") or [])[:10],
        "top_lifecycle_source_contexts": (row.get("top_lifecycle_source_contexts") or [])[:10],
        "human_approval_required_if_fix_requires": row.get("human_approval_required_if_fix_requires"),
        "next_validation": row.get("next_validation")
        or "repeat_same_cluster_in_clean_window_then_oos_if_it_generates_shadow_candidate_lift",
    }


def hypothesis_id_part(value):
    text = str(value or "unknown").strip()
    return "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in text)[:120] or "unknown"


def is_quality_timing_probe_candidate(row):
    if not isinstance(row, dict):
        return False
    candidate_id = str(row.get("candidate_id") or "")
    family = str(row.get("family") or "")
    if not candidate_id:
        return False
    if candidate_id in {"current_all", "current_would_enter_all"}:
        return False
    if family == "runtime" or candidate_id.startswith("runtime:"):
        return False
    return True


def compact_quality_timing_candidate_probe(cluster_row, candidate_row, rank):
    cluster = cluster_row.get("cluster")
    candidate_id = candidate_row.get("candidate_id")
    return {
        "hypothesis_id": (
            f"quality_timing_probe:{hypothesis_id_part(cluster)}:{hypothesis_id_part(candidate_id)}"
        ),
        "evidence_level": "discovery_same_window",
        "scope": "shadow_only_quality_timing_candidate_probe",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "definition": {
            "quality_timing_cluster": cluster,
            "candidate_id": candidate_id,
            "candidate_family": candidate_row.get("family"),
            "dominant_stage_filter": [
                item.get("stage")
                for item in (cluster_row.get("stage_counts") or [])
                if item.get("stage")
            ],
            "suggested_shadow_only_action": cluster_row.get("suggested_shadow_only_action"),
        },
        "latest_metrics": {
            "cluster_event_count": cluster_row.get("event_count"),
            "cluster_share_of_quality_timing_rejects": cluster_row.get("share_of_quality_timing_rejects"),
            "cluster_share_of_raw_all_gold_silver": cluster_row.get("share_of_raw_all_gold_silver"),
            "cluster_unique_tokens": cluster_row.get("unique_tokens"),
            "candidate_cluster_match_count": candidate_row.get("count"),
            "candidate_probe_rank_in_cluster": rank,
            "candidate_matched_any_rate": cluster_row.get("candidate_matched_any_rate"),
            "max_sustained_peak_pct_max": cluster_row.get("max_sustained_peak_pct_max"),
            "time_to_sustained_peak_sec_median": cluster_row.get("time_to_sustained_peak_sec_median"),
        },
        "top_lifecycle_source_contexts": (cluster_row.get("top_lifecycle_source_contexts") or [])[:10],
        "human_approval_required_if_fix_requires": cluster_row.get("human_approval_required_if_fix_requires"),
        "next_validation": (
            "track_candidate_within_quality_timing_cluster_in_next_clean_window_then_oos_if_repeated"
        ),
    }


def build_quality_timing_candidate_probes(opportunities, *, per_cluster_limit=3, total_limit=24):
    probes = []
    seen = set()
    for cluster_row in opportunities or []:
        if not isinstance(cluster_row, dict) or not cluster_row.get("cluster"):
            continue
        rank = 0
        for candidate_row in cluster_row.get("top_candidates") or []:
            if not is_quality_timing_probe_candidate(candidate_row):
                continue
            rank += 1
            key = (cluster_row.get("cluster"), candidate_row.get("candidate_id"))
            if key in seen:
                continue
            seen.add(key)
            probes.append(compact_quality_timing_candidate_probe(cluster_row, candidate_row, rank))
            if rank >= per_cluster_limit or len(probes) >= total_limit:
                break
        if len(probes) >= total_limit:
            break
    return probes


def build_quality_timing_candidate_probe_validation(registry, quality_timing_report):
    """Validate registered quality/timing probes against the current same-window audit.

    This is intentionally read-only discovery evidence. It tracks whether a
    shadow-only probe remains visible in the latest quality/timing reject window;
    it does not promote candidates or authorize threshold changes.
    """
    registry = registry or {}
    quality_timing_report = quality_timing_report or {}
    probes = list(registry.get("shadow_only_quality_timing_candidate_probes") or [])
    opportunities = (
        (quality_timing_report.get("shadow_only_review") or {}).get("top_research_opportunities")
        or []
    )
    clusters = {}
    current_candidates = {}
    for cluster_row in opportunities:
        if not isinstance(cluster_row, dict) or not cluster_row.get("cluster"):
            continue
        cluster = cluster_row.get("cluster")
        clusters[cluster] = cluster_row
        rank = 0
        for candidate_row in cluster_row.get("top_candidates") or []:
            if not is_quality_timing_probe_candidate(candidate_row):
                continue
            rank += 1
            current_candidates[(cluster, candidate_row.get("candidate_id"))] = {
                "candidate": candidate_row,
                "rank": rank,
                "cluster": cluster_row,
            }

    rows = []
    for probe in probes:
        definition = probe.get("definition") or {}
        cluster = definition.get("quality_timing_cluster")
        candidate_id = definition.get("candidate_id")
        current = current_candidates.get((cluster, candidate_id))
        cluster_row = clusters.get(cluster)
        current_candidate = (current or {}).get("candidate") or {}
        status = "NOT_OBSERVED_CURRENT_WINDOW"
        if current_candidate:
            status = "REPEATED_SHADOW_PROBE"
        elif cluster_row:
            status = "CLUSTER_REPEATED_CANDIDATE_NOT_TOP"
        rows.append({
            "hypothesis_id": probe.get("hypothesis_id"),
            "status": status,
            "scope": "shadow_only_quality_timing_candidate_probe",
            "evidence_level": "discovery_same_window_probe_validation",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "definition": {
                "quality_timing_cluster": cluster,
                "candidate_id": candidate_id,
                "candidate_family": definition.get("candidate_family"),
            },
            "current_window": {
                "cluster_repeated": bool(cluster_row),
                "candidate_repeated": bool(current_candidate),
                "cluster_event_count": (cluster_row or {}).get("event_count"),
                "cluster_share_of_quality_timing_rejects": (
                    (cluster_row or {}).get("share_of_quality_timing_rejects")
                ),
                "cluster_share_of_raw_all_gold_silver": (
                    (cluster_row or {}).get("share_of_raw_all_gold_silver")
                ),
                "cluster_unique_tokens": (cluster_row or {}).get("unique_tokens"),
                "candidate_cluster_match_count": current_candidate.get("count"),
                "candidate_probe_rank_in_cluster": (current or {}).get("rank"),
                "candidate_matched_any_rate": (cluster_row or {}).get("candidate_matched_any_rate"),
                "max_sustained_peak_pct_max": (cluster_row or {}).get("max_sustained_peak_pct_max"),
                "time_to_sustained_peak_sec_median": (
                    (cluster_row or {}).get("time_to_sustained_peak_sec_median")
                ),
            },
            "next_validation": (
                "continue_shadow_only_tracking_until_clean_window_then_oos_if_repeated"
            ),
        })

    status_counts = Counter(row.get("status") for row in rows)
    repeated_rows = [
        row for row in rows
        if row.get("status") == "REPEATED_SHADOW_PROBE"
    ]
    repeated_rows = sorted(
        repeated_rows,
        key=lambda row: (
            safe_int((row.get("current_window") or {}).get("cluster_event_count"), 0),
            safe_int((row.get("current_window") or {}).get("candidate_cluster_match_count"), 0),
        ),
        reverse=True,
    )
    if not probes:
        classification = "NO_REGISTERED_QUALITY_TIMING_CANDIDATE_PROBES"
        next_action = "continue_quality_timing_cluster_discovery"
    elif repeated_rows:
        classification = "QUALITY_TIMING_PROBES_REPEATED_SAME_WINDOW"
        next_action = "continue_shadow_probe_tracking_until_clean_window_then_oos"
    elif any(row.get("status") == "CLUSTER_REPEATED_CANDIDATE_NOT_TOP" for row in rows):
        classification = "QUALITY_TIMING_CLUSTERS_REPEATED_CANDIDATES_SHIFTED"
        next_action = "refresh_shadow_probe_candidates_from_current_cluster_leaders"
    else:
        classification = "QUALITY_TIMING_PROBES_NOT_REPEATED_CURRENT_WINDOW"
        next_action = "continue_monitoring_registered_shadow_probes"

    denominator = {
        "registered_probe_count": len(probes),
        "current_quality_timing_cluster_count": len(clusters),
        "validated_probe_count": len(rows),
        "repeated_probe_count": status_counts.get("REPEATED_SHADOW_PROBE", 0),
        "cluster_repeated_candidate_not_top_count": status_counts.get(
            "CLUSTER_REPEATED_CANDIDATE_NOT_TOP",
            0,
        ),
        "not_observed_current_window_count": status_counts.get(
            "NOT_OBSERVED_CURRENT_WINDOW",
            0,
        ),
        "repeated_probe_rate": safe_rate(
            status_counts.get("REPEATED_SHADOW_PROBE", 0),
            len(probes),
        ),
    }

    return {
        "schema_version": "quality_timing_candidate_probe_validation.v1",
        "report_type": "quality_timing_candidate_probe_validation_24h",
        "generated_at": utc_now(),
        "classification": classification,
        "next_action": next_action,
        "evidence_level": "discovery_same_window_probe_validation",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "registered_probe_count": denominator["registered_probe_count"],
        "current_quality_timing_cluster_count": denominator["current_quality_timing_cluster_count"],
        "validated_probe_count": denominator["validated_probe_count"],
        "repeated_probe_count": denominator["repeated_probe_count"],
        "repeated_probe_rate": denominator["repeated_probe_rate"],
        "denominator": denominator,
        "status_counts": dict(status_counts),
        "top_repeated_probes": repeated_rows[:12],
        "probe_validations": rows,
        "notes": [
            "Read-only validation of shadow-only quality/timing candidate probes.",
            "Repeated same-window probes are discovery evidence only; OOS validation and human approval are still required before any promotion.",
        ],
    }


def stable_hypothesis_signature(
    *,
    watchlist_hypotheses,
    matured_volume_watch,
    quality_timing_watch=None,
    quality_timing_candidate_probes=None,
):
    watchlist_keys = []
    for row in watchlist_hypotheses or []:
        if not isinstance(row, dict):
            continue
        watchlist_keys.append(
            row.get("hypothesis_id")
            or row.get("id")
            or row.get("name")
            or json.dumps(row.get("definition") or row, sort_keys=True)
        )
    matured_keys = []
    for row in matured_volume_watch or []:
        if not isinstance(row, dict):
            continue
        matured_keys.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "definition": row.get("definition") or {},
        })
    matured_keys = sorted(matured_keys, key=lambda item: item.get("hypothesis_id") or "")
    quality_timing_keys = []
    for row in quality_timing_watch or []:
        if not isinstance(row, dict):
            continue
        quality_timing_keys.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "definition": row.get("definition") or {},
        })
    quality_timing_keys = sorted(quality_timing_keys, key=lambda item: item.get("hypothesis_id") or "")
    quality_timing_probe_keys = []
    for row in quality_timing_candidate_probes or []:
        if not isinstance(row, dict):
            continue
        quality_timing_probe_keys.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "definition": row.get("definition") or {},
        })
    quality_timing_probe_keys = sorted(
        quality_timing_probe_keys,
        key=lambda item: item.get("hypothesis_id") or "",
    )
    return {
        "watchlist_hypothesis_keys": sorted(str(item) for item in watchlist_keys),
        "shadow_only_matured_volume_watch": matured_keys,
        "shadow_only_quality_timing_watch": quality_timing_keys,
        "shadow_only_quality_timing_candidate_probes": quality_timing_probe_keys,
    }


def update_hypothesis_registry(path, verdict, capture):
    target = Path(path)
    if target.exists():
        try:
            registry = load_json(target)
        except Exception:
            registry = {}
    else:
        registry = {}
    recent = list(registry.get("recent_runs") or [])
    recent.append({
        "generated_at": verdict.get("generated_at"),
        "classification": verdict.get("classification"),
        "blockers": verdict.get("blockers") or [],
        "capture_judgment_counts": verdict.get("capture_judgment_counts") or {},
    })
    matured_volume = verdict.get("matured_volume_capture_cross_audit") or {}
    matured_volume_slices = (
        matured_volume.get("top_watch_slices")
        or [
            row for row in (matured_volume.get("top_slices") or [])
            if row.get("verdict") == "MATURED_VOLUME_DISCOVERY_WATCH"
        ]
    )
    matured_volume_watch = [
        compact_matured_volume_hypothesis(row)
        for row in matured_volume_slices[:10]
    ]
    quality_timing = verdict.get("quality_timing_reject_research_audit") or {}
    quality_timing_review = quality_timing.get("shadow_only_review") or {}
    quality_timing_opportunities = quality_timing_review.get("top_research_opportunities") or []
    quality_timing_watch = [
        compact_quality_timing_shadow_hypothesis(row)
        for row in quality_timing_opportunities[:10]
        if isinstance(row, dict) and row.get("cluster")
    ]
    quality_timing_candidate_probes = build_quality_timing_candidate_probes(
        quality_timing_opportunities,
    )
    watchlist_hypotheses = capture.get("watchlist_hypotheses", [])[:25]
    new_signature = stable_hypothesis_signature(
        watchlist_hypotheses=watchlist_hypotheses,
        matured_volume_watch=matured_volume_watch,
        quality_timing_watch=quality_timing_watch,
        quality_timing_candidate_probes=quality_timing_candidate_probes,
    )
    previous_signature = registry.get("hypothesis_set_signature")
    previous_frozen_at = registry.get("hypothesis_frozen_at") or registry.get("updated_at")
    hypothesis_frozen_at = previous_frozen_at if previous_signature == new_signature and previous_frozen_at else utc_now()
    registry = {
        "schema_version": "hypothesis_registry.v2",
        "updated_at": utc_now(),
        "hypothesis_frozen_at": hypothesis_frozen_at,
        "hypothesis_set_signature": new_signature,
        "phase": "discovery_mesh",
        "promotion_allowed": False,
        "hypotheses": {
            "H1_building_volume_active_microstructure": compact_hypothesis(verdict.get("H1_capture_metrics") or {}),
            "H2_shallow_pullback_matrix_evaluator": compact_hypothesis(verdict.get("H2_capture_metrics") or {}),
        },
        "watchlist_hypotheses": watchlist_hypotheses,
        "shadow_only_matured_volume_watch": matured_volume_watch,
        "shadow_only_quality_timing_watch": quality_timing_watch,
        "shadow_only_quality_timing_candidate_probes": quality_timing_candidate_probes,
        "recent_runs": recent[-20:],
    }
    write_json(target, registry)
    return registry


def build_run_summary(verdict, paths, diagnostics, tests):
    lines = [
        "# Gold/Silver Capture Discovery AutoLoop Summary",
        "",
        f"- generated_at: `{utc_now()}`",
        f"- phase: `discovery_mesh`",
        f"- current_commit: `{verdict.get('current_commit')}`",
        f"- deployment_commit: `{verdict.get('deployment_commit')}`",
        f"- verdict: `{verdict.get('classification')}`",
        f"- blocked_subtype: `{verdict.get('blocked_subtype')}`",
        f"- next_action: `{verdict.get('next_action')}`",
        f"- parallel_next_action: `{verdict.get('parallel_next_action')}`",
        f"- parallel_next_action_reason: `{verdict.get('parallel_next_action_reason')}`",
        f"- promotion_allowed: `{str(bool(verdict.get('promotion_allowed'))).lower()}`",
        f"- human_action_required: `{str(bool(verdict.get('human_action_required'))).lower()}`",
        f"- strategy_change_allowed: `{str(bool(verdict.get('strategy_change_allowed'))).lower()}`",
        f"- non_quote_sensitive_capture_discovery_allowed: `{str(bool(verdict.get('non_quote_sensitive_capture_discovery_allowed'))).lower()}`",
        f"- quote_sensitive_slices_blocked: `{str(bool(verdict.get('quote_sensitive_slices_blocked'))).lower()}`",
        f"- formal_volume_sensitive_slices_blocked: `{str(bool(verdict.get('formal_volume_sensitive_slices_blocked'))).lower()}`",
        f"- shadow_matured_volume_slices_evaluable: `{str(bool(verdict.get('shadow_matured_volume_slices_evaluable'))).lower()}`",
        f"- context_field_writer_fix_status: `{verdict.get('context_field_writer_fix_status')}`",
        f"- context_clean_window_pending: `{str(bool(verdict.get('context_clean_window_pending'))).lower()}`",
        f"- context_clean_window_eta_iso: `{verdict.get('context_clean_window_eta_iso')}`",
        f"- lifecycle_clean_window_pending: `{str(bool(verdict.get('lifecycle_clean_window_pending'))).lower()}`",
        f"- source_component_clean_window_pending: `{str(bool(verdict.get('source_component_clean_window_pending'))).lower()}`",
        f"- current_capture_stage: `{verdict.get('current_capture_stage')}`",
        f"- final_eligibility_capture_rate: `{verdict.get('final_eligibility_capture_rate')}`",
        f"- paper_trade_intent_rate: `{verdict.get('paper_trade_intent_rate')}`",
        f"- paper_capture_rate: `{verdict.get('paper_capture_rate')}`",
        "",
        "## Integrity",
        "",
        f"- candidate_count_expected: `{verdict.get('candidate_count_expected')}`",
        f"- candidate_count_observed: `{verdict.get('candidate_count_observed')}`",
        f"- observation_coverage_pct: `{verdict.get('observation_coverage_pct')}`",
        f"- raw_dog_rows_complete: `{str(bool(verdict.get('raw_dog_rows_complete'))).lower()}`",
        f"- signal_id_join_rate: `{verdict.get('signal_id_join_rate')}`",
        f"- raw_all_signal_id_join_rate: `{verdict.get('raw_all_signal_id_join_rate')}`",
        f"- mesh_eligible_signal_id_join_rate: `{verdict.get('mesh_eligible_signal_id_join_rate')}`",
        f"- blockers: `{json.dumps(verdict.get('blockers') or [], sort_keys=True)}`",
        "",
        "## Signal Identity Reconciliation",
        "",
        "```json",
        json.dumps(
            {
                key: (verdict.get("signal_identity_reconciliation") or {}).get(key)
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
        "## Quote Context Coverage",
        "",
        "```json",
        json.dumps(
            {
                key: (verdict.get("quote_context_coverage") or {}).get(key)
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
            },
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## Quote Missing Root Cause",
        "",
        "```json",
        json.dumps(
            {
                key: (verdict.get("quote_missing_root_cause") or {}).get(key)
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
            },
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## Runtime Health",
        "",
        "```json",
        json.dumps(
            {
                "status": verdict.get("runtime_health_status"),
                "blockers": verdict.get("runtime_health_blockers") or [],
                "warnings": verdict.get("runtime_health_warnings") or [],
                "signal_source_freshness": (verdict.get("runtime_health_snapshot") or {}).get("signal_source_freshness") or {},
                "paper_review_snapshot": (verdict.get("runtime_health_snapshot") or {}).get("paper_review_snapshot") or {},
                "paper_fast_lane_health": (verdict.get("runtime_health_snapshot") or {}).get("paper_fast_lane_health") or {},
                "paper_db": (verdict.get("runtime_health_snapshot") or {}).get("paper_db") or {},
                "runtime_final_evidence": (verdict.get("runtime_health_snapshot") or {}).get("runtime_final_evidence") or {},
                "observer_logs": (verdict.get("runtime_health_snapshot") or {}).get("observer_logs") or {},
                "promotion_allowed": False,
            },
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## Volume / Kline Coverage",
        "",
        "```json",
        json.dumps(
            {
                "volume_profile_coverage": verdict.get("volume_profile_coverage") or {},
                "kline_coverage": verdict.get("kline_coverage") or {},
                "volume_kline_root_cause_audit": verdict.get("volume_kline_root_cause_audit") or {},
                "matured_kline_volume_recheck_audit": verdict.get("matured_kline_volume_recheck_audit") or {},
                "matured_volume_capture_cross_audit": verdict.get("matured_volume_capture_cross_audit") or {},
            },
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## A_CLASS / Final Entry",
        "",
        "```json",
        json.dumps(
            {
                "A_CLASS_mode_status": verdict.get("A_CLASS_mode_status") or {},
                "final_entry_contract_blocker_breakdown": verdict.get("final_entry_contract_blocker_breakdown") or {},
                "readiness_shortfall_summary": verdict.get("readiness_shortfall_summary") or {},
                "paper_entry_proposal_readiness": verdict.get("paper_entry_proposal_readiness") or {},
                "human_action_required": verdict.get("human_action_required"),
            },
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## Candidate / Markov / Cross Readiness",
        "",
        "```json",
        json.dumps(
            {
                "per_candidate_effectiveness_summary": verdict.get("per_candidate_effectiveness_summary") or {},
                "candidate_improvement_opportunities_summary": verdict.get("candidate_improvement_opportunities_summary") or {},
                "Markov_effectiveness_summary": verdict.get("Markov_effectiveness_summary") or {},
                "two_d_cross_validity_summary": verdict.get("two_d_cross_validity_summary") or {},
                "next_highest_priority_blocker": verdict.get("next_highest_priority_blocker"),
            },
            indent=2,
            sort_keys=True,
        )[:12000],
        "```",
        "",
        "## H1/H2",
        "",
        f"- H1 status: `{(verdict.get('H1_capture_metrics') or {}).get('status')}`",
        f"- H2 status: `{(verdict.get('H2_capture_metrics') or {}).get('status')}`",
        "",
        "## Candidate Improvement Opportunities",
        "",
        "```json",
        json.dumps(
            {
                "opportunity_count": (verdict.get("candidate_improvement_opportunities_summary") or {}).get("opportunity_count"),
                "opportunity_type_counts": (verdict.get("candidate_improvement_opportunities_summary") or {}).get("opportunity_type_counts") or {},
                "blocked_context_opportunity_count": (verdict.get("candidate_improvement_opportunities_summary") or {}).get("blocked_context_opportunity_count"),
                "promotion_allowed": False,
                "top_opportunities": ((verdict.get("candidate_improvement_opportunities_summary") or {}).get("top_opportunities") or [])[:5],
            },
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
        "## Secondary Evidence",
        "",
        f"- PnL cross: `{(verdict.get('PnL_cross_secondary_status') or {}).get('status')}`",
        f"- virtual Markov: `{(verdict.get('virtual_Markov_discovery_status') or {}).get('status')}`",
        "",
        "## Artifacts",
        "",
    ]
    for name, path in sorted(paths.items()):
        lines.append(f"- {name}: `{path}`")
    lines.extend(["", "## Tests", ""])
    lines.append(f"- passed: `{str(bool(tests.get('passed'))).lower()}`")
    for result in tests.get("results", []):
        lines.append(f"- {result.get('name')}: `{str(bool(result.get('ok'))).lower()}`")
    lines.extend(["", "## Report Commands", ""])
    for result in diagnostics:
        lines.append(f"- {result.get('name')}: ok=`{str(bool(result.get('ok'))).lower()}` duration_sec=`{result.get('duration_sec')}`")
    lines.append("")
    return "\n".join(lines)


def sync_latest(run_dir, latest_dir, handoff_dir, verdict_path, summary_path, handoff_path):
    latest_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(verdict_path, latest_dir / "reviewer_verdict.json")
    shutil.copy2(summary_path, latest_dir / "run_summary.md")
    for report in run_dir.glob("*.json"):
        if report.name != "reviewer_verdict.json":
            shutil.copy2(report, latest_dir / report.name)
    handoff_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(handoff_path, handoff_dir / "latest_codex_handoff.md")


def write_materialized_artifacts(
    args,
    *,
    rid,
    run_dir,
    latest_dir,
    handoff_dir,
    capture_path,
    pnl_path=None,
    markov_paths=None,
    readiness_paths=None,
    diagnostics=None,
    tests=None,
    state="final",
    publish_latest=True,
    refresh_oos_after_registry=False,
):
    diagnostics = diagnostics or []
    markov_paths = markov_paths or {}
    readiness_paths = readiness_paths or {}
    tests = tests or {
        "schema_version": "agent_capture_discovery_tests.v1",
        "generated_at": utc_now(),
        "passed": False,
        "status": "pending",
        "results": [],
    }
    tests_path = run_dir / "tests.json"
    write_json(tests_path, tests)

    capture = load_json(capture_path)
    pnl = load_json(pnl_path) if pnl_path and Path(pnl_path).exists() else None
    markov_reports = {name: load_json(path) for name, path in markov_paths.items() if Path(path).exists()}

    def collect_readiness_reports():
        reports = {}
        for key, path in readiness_paths.items():
            if path and Path(path).exists():
                try:
                    reports[key] = load_json(path)
                except Exception:
                    pass
        context_report = reports.get("context_coverage") or {}
        reports["current_commit"] = git_commit()
        reports["deployment_commit"] = (
            os.environ.get("ZEABUR_GIT_COMMIT_SHA")
            or os.environ.get("ZEABUR_GIT_COMMIT")
            or os.environ.get("ZEABUR_COMMIT_SHA")
            or os.environ.get("COMMIT_SHA")
            or os.environ.get("GIT_COMMIT")
            or os.environ.get("SOURCE_VERSION")
            or os.environ.get("GITHUB_SHA")
            or reports["current_commit"]
        )
        if not reports["current_commit"]:
            reports["current_commit"] = reports["deployment_commit"]
        if not reports["deployment_commit"]:
            reports["deployment_commit"] = reports["current_commit"]
        reports["volume_profile_coverage"] = context_report.get("volume_profile_coverage") or {}
        reports["kline_coverage"] = context_report.get("kline_coverage") or {}
        reports["next_highest_priority_blocker"] = first_blocker_priority(
            list((capture.get("report_health") or {}).get("promotion_blockers") or [])
            + list(context_report.get("blockers") or [])
        )
        return reports

    def build_loop_verdict():
        verdict_payload = build_verdict(
            capture,
            pnl,
            markov_reports,
            tests=tests if tests.get("status") != "pending" else {},
            readiness_reports=collect_readiness_reports(),
        )
        if any(not row.get("ok") for row in diagnostics):
            verdict_payload["blockers"] = sorted(set((verdict_payload.get("blockers") or []) + ["report_generation_failed"]))
            verdict_payload["classification"] = "BLOCKED_DATA"
            verdict_payload["promotion_allowed"] = False
        if state != "final":
            verdict_payload["blockers"] = sorted(set((verdict_payload.get("blockers") or []) + [state]))
            verdict_payload["classification"] = "BLOCKED_DATA"
            verdict_payload["promotion_allowed"] = False
        verdict_payload["loop"] = {
            "schema_version": SCHEMA_VERSION,
            "run_id": rid,
            "run_dir": str(run_dir),
            "state": state,
            "report_diagnostics": diagnostics,
        }
        return verdict_payload

    verdict = build_loop_verdict()
    verdict_path = run_dir / "reviewer_verdict.json"
    registry = update_hypothesis_registry(args.registry, verdict, capture)
    quality_timing_report = {}
    quality_timing_path = readiness_paths.get("quality_timing_reject_research_audit")
    if quality_timing_path and Path(quality_timing_path).exists():
        try:
            quality_timing_report = load_json(quality_timing_path)
        except Exception:
            quality_timing_report = {}
    quality_timing_probe_validation_path = (
        run_dir / f"quality_timing_candidate_probe_validation_{int(args.hours)}h.json"
    )
    write_json(
        quality_timing_probe_validation_path,
        build_quality_timing_candidate_probe_validation(registry, quality_timing_report),
    )
    readiness_paths["quality_timing_candidate_probe_validation"] = quality_timing_probe_validation_path
    verdict = build_loop_verdict()
    write_json(verdict_path, verdict)
    if refresh_oos_after_registry and state == "final":
        oos_refresh_path = run_dir / "oos_readiness_probe_refresh.json"
        probe_count = max(1, len(parse_oos_probe_hours(getattr(args, "oos_probe_hours", "")))) + 2
        oos_timeout = max(int(args.report_timeout_sec), int(args.report_timeout_sec) * probe_count)
        refresh_result = command_result(
            "oos_readiness_probe_refresh",
            [
                "scripts/refresh_oos_readiness_probes.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--kline-db", args.kline_db,
                "--registry", str(args.registry),
                "--run-dir", str(run_dir),
                "--probe-hours", str(getattr(args, "oos_probe_hours", "")),
                "--expected-candidates", str(args.expected_candidates),
                "--max-scan-rows", str(args.max_scan_rows),
                "--timeout-sec", str(args.report_timeout_sec),
                "--out", str(oos_refresh_path),
            ],
            timeout=oos_timeout,
        )
        diagnostics.append(refresh_result)
        if oos_refresh_path.exists():
            readiness_paths["oos_readiness_probe_refresh"] = oos_refresh_path
        verdict = build_loop_verdict()
        write_json(verdict_path, verdict)

    handoff_text = build_handoff(verdict)
    handoff_path = run_dir / "codex_handoff.md"
    write_handoff_text(handoff_path, handoff_text)

    artifact_paths = {
        "run_dir": str(run_dir),
        "latest_dir": str(latest_dir),
        "reviewer_verdict": str(verdict_path),
        "run_summary": str(run_dir / "run_summary.md"),
        "codex_handoff": str(handoff_path),
        "hypothesis_registry": str(args.registry),
        "capture_report": str(capture_path),
        "pnl_cross_report": str(pnl_path) if pnl_path else None,
        "tests": str(tests_path),
    }
    for profile, path in sorted(markov_paths.items()):
        artifact_paths[f"markov_{profile}"] = str(path)
    for name, path in sorted(readiness_paths.items()):
        artifact_paths[name] = str(path)
    summary = build_run_summary(verdict, artifact_paths, diagnostics, tests)
    summary_path = run_dir / "run_summary.md"
    write_text(summary_path, summary)
    if publish_latest:
        sync_latest(run_dir, latest_dir, handoff_dir, verdict_path, summary_path, handoff_path)
    return verdict, registry, verdict_path, summary_path, handoff_path, tests_path


def run_once(args):
    rid = run_id()
    out_root = Path(args.out_root)
    run_dir = out_root / rid
    latest_dir = out_root / "latest"
    handoff_dir = Path(args.handoff_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    log_event("run_start", run_id=rid, run_dir=str(run_dir), hours=args.hours)

    capture_path = run_dir / f"candidate_capture_discovery_{int(args.hours)}h.json"
    initial_capture = blocked_capture_report(
        "agent_run_started",
        args.paper_db,
        args.raw_db,
        int(args.hours),
        args.expected_candidates,
    )
    write_json(capture_path, initial_capture)
    write_materialized_artifacts(
        args,
        rid=rid,
        run_dir=run_dir,
        latest_dir=latest_dir,
        handoff_dir=handoff_dir,
        capture_path=capture_path,
        state="agent_run_started",
        publish_latest=False,
    )

    tests = run_self_tests(args.test_timeout_sec)
    tests_path = run_dir / "tests.json"
    write_json(tests_path, tests)
    log_event("self_tests_done", run_id=rid, passed=tests.get("passed"))

    report_bundle = run_reports(run_dir, args)
    capture_path = report_bundle["capture_primary"]
    pnl_path = report_bundle["pnl"]
    markov_paths = report_bundle["markov"]
    readiness_paths = report_bundle["readiness"]
    diagnostics = report_bundle["diagnostics"]
    verdict, registry, verdict_path, summary_path, handoff_path, _tests_path = write_materialized_artifacts(
        args,
        rid=rid,
        run_dir=run_dir,
        latest_dir=latest_dir,
        handoff_dir=handoff_dir,
        capture_path=capture_path,
        pnl_path=pnl_path,
        markov_paths=markov_paths,
        readiness_paths=readiness_paths,
        diagnostics=diagnostics,
        tests=tests,
        state="final",
        refresh_oos_after_registry=True,
    )
    log_event("run_end", run_id=rid, classification=verdict.get("classification"), blockers=verdict.get("blockers") or [])

    return {
        "run_id": rid,
        "classification": verdict.get("classification"),
        "blockers": verdict.get("blockers") or [],
        "promotion_allowed": verdict.get("promotion_allowed"),
        "latest_verdict": str(latest_dir / "reviewer_verdict.json"),
        "latest_summary": str(latest_dir / "run_summary.md"),
        "latest_handoff": str(handoff_dir / "latest_codex_handoff.md"),
        "hypothesis_registry": str(args.registry),
        "tests_passed": tests.get("passed"),
        "registry_updated_at": registry.get("updated_at"),
    }


def create_self_test_dbs(root):
    now = int(time.time())
    paper = root / "paper.db"
    raw = root / "raw.db"
    kline = root / "kline.db"
    db = sqlite3.connect(paper)
    db.executescript(
        """
        CREATE TABLE candidate_shadow_observations(
          signal_id TEXT, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT,
          matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT
        );
        CREATE TABLE candidate_shadow_virtual_trades(
          signal_id TEXT, token_ca TEXT, candidate_id TEXT, family TEXT,
          status TEXT, net_pnl_pct REAL, observed_at INTEGER
        );
        """
    )
    candidates = ["kline:active_mom20_first3", "entry_mode_registry:smart_entry_pullback_bounce"]
    for signal_id, token, is_dog in [("1", "DOG", True), ("2", "NORM", False)]:
        for candidate in candidates:
            payload = {
                "context_schema_version": "candidate-shadow-context-v2.no_signal_price_quote_inference",
                "quote_clean_definition": "source_or_executable_quote_only_no_signal_price",
                "quote_context_writer_path": "candidate_shadow_observer:inferred",
                "source_quote_clean": True,
                "source_quote_executable": True,
                "volume_profile": "building" if candidate.startswith("kline:") else "UNKNOWN",
                "lifecycle_profile": "ATH_SHALLOW_PULLBACK:OBSERVE",
                "source_component": "matrix_evaluator",
            }
            matched = 1 if is_dog else 0
            db.execute(
                "INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?,?,?,?)",
                (signal_id, token, now - 120, candidate, "test", matched, "self_test", now, json.dumps(payload)),
            )
            db.execute(
                "INSERT INTO candidate_shadow_virtual_trades VALUES (?,?,?,?,?,?,?)",
                (signal_id, token, candidate, "test", "VIRTUAL_CLOSED", 5.0 if is_dog else -1.0, now),
            )
    db.commit()
    db.close()
    raw_db = sqlite3.connect(raw)
    raw_db.executescript(
        """
        CREATE TABLE raw_signal_outcomes(
          signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts INTEGER,
          observation_status TEXT, kline_covered INTEGER, baseline_confidence TEXT,
          same_source_path INTEGER, outlier_flag INTEGER, sustained_evaluable INTEGER,
          raw_primary_tier TEXT, raw_sustained_tier TEXT,
          max_sustained_peak_pct REAL, time_to_sustained_peak_sec INTEGER,
          raw_dog_entered INTEGER, raw_dog_realized INTEGER, did_enter INTEGER,
          held_to_silver INTEGER, held_to_gold INTEGER, exit_reason TEXT
        );
        """
    )
    raw_db.execute(
        "INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("1", "DOG", "DOG", now - 120, "matured", 1, "high", 1, 0, 1, "silver", "silver", 80.0, 600, 0, 0, 0, 0, 0, None),
    )
    raw_db.commit()
    raw_db.close()
    kline_db = sqlite3.connect(kline)
    kline_db.executescript(
        """
        CREATE TABLE kline_1m(
          token_ca TEXT, pool_address TEXT DEFAULT '', timestamp INTEGER,
          open REAL, high REAL, low REAL, close REAL, volume REAL,
          PRIMARY KEY(token_ca, timestamp)
        );
        """
    )
    kline_db.executemany(
        "INSERT INTO kline_1m(token_ca,timestamp,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?)",
        [
            ("DOG", now - 120, 1, 1.1, 0.9, 1.0, 10),
            ("DOG", now - 60, 1, 1.2, 0.9, 1.1, 20),
            ("DOG", now, 1.1, 1.4, 1.0, 1.3, 40),
        ],
    )
    kline_db.commit()
    kline_db.close()
    (root / "v27_read_models").mkdir(parents=True, exist_ok=True)
    (root / "review-artifacts" / "live").mkdir(parents=True, exist_ok=True)
    write_json(root / "v27_read_models" / "signal_source_freshness.json", {
        "schema_version": "v1.signal_source_freshness_health",
        "status": "ok",
        "age_minutes": 1,
        "warn_after_minutes": 15,
        "fail_closed_after_minutes": 45,
        "fail_closed": False,
        "generated_at": now,
        "latest_ts": now - 60,
        "source": "local",
        "total": 2,
    })
    write_json(root / "review-artifacts" / "live" / "paper_review_24h.json", {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        "snapshot_id": "self_test",
        "requested_hours": 24,
        "materialized_hours": 24,
    })
    write_json(root / "paper-fast-lane-health.json", {
        "schema_version": "self_test",
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        "worker_state": "scanned",
        "paper_db_exists": True,
    })
    (root / "paper_trades.db").write_bytes(b"self-test")
    (root / "runtime_final_evidence.jsonl").write_text("{}\n", encoding="utf-8")
    return paper, raw, kline


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        paper, raw, kline = create_self_test_dbs(root)
        args = argparse.Namespace(
            paper_db=str(paper),
            raw_db=str(raw),
            kline_db=str(kline),
            data_dir=str(root),
            hours=24,
            expected_candidates=2,
            out_root=str(root / "agent_runs"),
            handoff_dir=str(root / "agent_handoffs"),
            registry=str(root / "hypothesis_registry.json"),
            markov_profiles="runtime,kline,candidate_lifecycle,candidate_source,candidate_signal_type,candidate_lifecycle_source",
            report_timeout_sec=60,
            test_timeout_sec=60,
            max_scan_rows=2_000_000,
            oos_probe_hours="0.25,0.5,1",
            capture_hours="24,48,72",
            quote_fix_deploy_ts=int(time.time()) - 3600,
        )
        result = run_once(args)
        assert Path(result["latest_verdict"]).exists()
        assert Path(result["latest_summary"]).exists()
        assert Path(result["latest_handoff"]).exists()
        assert Path(result["hypothesis_registry"]).exists()
        verdict = load_json(result["latest_verdict"])
        assert verdict["candidate_count_expected"] == 2
        assert verdict["promotion_allowed"] is False
        required_verdict_fields = [
            "current_commit",
            "deployment_commit",
            "candidate_count_expected",
            "candidate_count_observed",
            "observation_coverage_pct",
            "raw_dog_rows_complete",
            "raw_all_signal_id_join_rate",
            "mesh_eligible_signal_id_join_rate",
            "quote_context_coverage",
            "volume_profile_coverage",
            "kline_coverage",
            "volume_kline_root_cause_audit",
            "matured_kline_volume_recheck_audit",
            "matured_volume_capture_cross_audit",
            "hypothesis_validation_audit",
            "low_confidence_research_capture_audit",
            "quality_timing_reject_research_audit",
            "runtime_health_snapshot",
            "runtime_health_status",
            "runtime_health_blockers",
            "runtime_health_warnings",
            "A_CLASS_mode_status",
            "final_entry_contract_blocker_breakdown",
            "per_candidate_effectiveness_summary",
            "candidate_improvement_opportunities_summary",
            "Markov_effectiveness_summary",
            "two_d_cross_validity_summary",
            "promotion_allowed",
            "human_action_required",
            "tests_passed",
        ]
        missing = [field for field in required_verdict_fields if field not in verdict]
        assert not missing, missing
        markov_summary = verdict["Markov_effectiveness_summary"]
        assert markov_summary["usage"] == "research_only_markov_information_value"
        assert "profile_diagnostics" in markov_summary
        assert "recommended_shadow_profiles" in markov_summary
        assert markov_summary["promotion_allowed"] is False
        assert verdict["oos_probe_refresh_status"]["available"] is True
        assert verdict["oos_probe_refresh_status"]["classification"] == "OOS_PROBES_REFRESHED"
        assert verdict["oos_readiness_summary"]["available_probe_count"] >= 3
        assert verdict["oos_readiness_summary"]["promotion_allowed"] is False
        latest_dir = Path(result["latest_verdict"]).parent
        registry = load_json(result["hypothesis_registry"])
        assert registry["schema_version"] == "hypothesis_registry.v2"
        assert "shadow_only_matured_volume_watch" in registry
        assert "shadow_only_quality_timing_watch" in registry
        assert "shadow_only_quality_timing_candidate_probes" in registry
        manual_registry_path = root / "manual_hypothesis_registry.json"
        manual_registry = update_hypothesis_registry(
            manual_registry_path,
            {
                "generated_at": utc_now(),
                "classification": "BLOCKED_CONTEXT_COVERAGE",
                "blockers": ["volume_profile_coverage_below_80pct"],
                "capture_judgment_counts": {},
                "H1_capture_metrics": {},
                "H2_capture_metrics": {},
                "matured_volume_capture_cross_audit": {
                    "top_slices": [
                        {
                            "candidate_id": "entry_mode_registry:ath_flat_structure_tiny_scout",
                            "dimension": "matured_volume_profile",
                            "slice_value": "building",
                            "slice_signal_count": 126,
                            "slice_raw_gs_count": 10,
                            "candidate_match_count": 62,
                            "matched_gs_count": 5,
                            "match_recall_event": 0.5,
                            "match_precision_event": 0.080645,
                            "recall_lift_vs_candidate_baseline": 0.131579,
                            "precision_lift_vs_candidate_baseline": 0.017009,
                            "verdict": "MATURED_VOLUME_DISCOVERY_WATCH",
                        },
                        {
                            "candidate_id": "kline:active_mom20_first3",
                            "dimension": "matured_volume_profile",
                            "slice_value": "building",
                            "verdict": "NO_SIGNAL",
                        }
                    ]
                },
                "quality_timing_reject_research_audit": {
                    "shadow_only_review": {
                        "top_research_opportunities": [
                            {
                                "cluster": "matrix_alignment_wait",
                                "event_count": 7,
                                "share_of_quality_timing_rejects": 0.2,
                                "share_of_raw_all_gold_silver": 0.06,
                                "unique_tokens": 6,
                                "candidate_matched_any_rate": 1.0,
                                "max_sustained_peak_pct_max": 9520.72,
                                "time_to_sustained_peak_sec_median": 1049,
                                "stage_counts": [
                                    {"stage": "decision_no_pass_or_allow", "count": 7}
                                ],
                                "top_candidates": [
                                    {"candidate_id": "current_all", "family": "base", "count": 7},
                                    {"candidate_id": "runtime:entry_mode_registry", "family": "runtime", "count": 7},
                                    {"candidate_id": "entry_mode_registry:smart_entry_pullback_bounce", "family": "entry_mode_registry", "count": 7},
                                    {"candidate_id": "entry_mode_registry:source_resonance_tiny_probe", "family": "entry_mode_registry", "count": 6},
                                ],
                                "top_families": [
                                    {"family": "entry_mode_registry", "count": 77}
                                ],
                                "top_lifecycle_source_contexts": [
                                    {"lifecycle_profile": "ATH_SHALLOW_PULLBACK:PROBE", "source_component": "matrix_evaluator", "count": 1}
                                ],
                                "suggested_shadow_only_action": "track_matrix_alignment_false_negative_shadow_probe",
                                "human_approval_required_if_fix_requires": "changing matrix alignment thresholds",
                                "next_validation": "repeat_same_cluster_in_clean_window_then_oos_if_it_generates_shadow_candidate_lift",
                            }
                        ]
                    }
                },
            },
            {"watchlist_hypotheses": []},
        )
        assert manual_registry["shadow_only_matured_volume_watch"][0]["hypothesis_id"] == "matured_volume:entry_mode_registry:ath_flat_structure_tiny_scout:building"
        assert manual_registry["shadow_only_matured_volume_watch"][0]["promotion_allowed"] is False
        assert manual_registry["shadow_only_quality_timing_watch"][0]["hypothesis_id"] == "quality_timing:matrix_alignment_wait"
        assert manual_registry["shadow_only_quality_timing_watch"][0]["promotion_allowed"] is False
        assert manual_registry["shadow_only_quality_timing_candidate_probes"][0]["hypothesis_id"] == (
            "quality_timing_probe:matrix_alignment_wait:entry_mode_registry_smart_entry_pullback_bounce"
        )
        assert manual_registry["shadow_only_quality_timing_candidate_probes"][0]["promotion_allowed"] is False
        assert manual_registry["shadow_only_quality_timing_candidate_probes"][0]["strategy_change_allowed"] is False
        required_artifacts = [
            "capture_discovery_24h.json",
            "capture_discovery_48h.json",
            "capture_discovery_72h.json",
            "raw_gold_silver_funnel_audit_24h.json",
            "shadow_decision_bridge_audit_24h.json",
            "runtime_health_snapshot_24h.json",
            "a_class_fastlane_mode_audit_24h.json",
            "candidate_effectiveness_24h.json",
            "candidate_improvement_opportunities_24h.json",
            "markov_effectiveness_24h.json",
            "capture_cross_validity_24h.json",
            "pnl_cross_secondary_24h.json",
            "context_coverage_audit_24h.json",
            "context_blocker_monitor_24h.json",
            "volume_kline_coverage_audit_24h.json",
            "matured_kline_volume_recheck_audit_24h.json",
            "matured_volume_capture_cross_audit_24h.json",
            "hypothesis_validation_audit_24h.json",
            "matured_volume_capture_cross_audit_oos_probe_0p25h.json",
            "hypothesis_validation_audit_oos_probe_0p25h.json",
            "matured_volume_capture_cross_audit_oos_probe_0p5h.json",
            "hypothesis_validation_audit_oos_probe_0p5h.json",
            "matured_volume_capture_cross_audit_oos_probe_1h.json",
            "hypothesis_validation_audit_oos_probe_1h.json",
            "oos_readiness_probe_refresh.json",
            "low_confidence_research_capture_audit_24h.json",
            "quality_timing_reject_research_audit_24h.json",
            "quality_timing_candidate_probe_validation_24h.json",
        ]
        missing_artifacts = [name for name in required_artifacts if not (latest_dir / name).exists()]
        assert not missing_artifacts, missing_artifacts
        shadow_bridge = load_json(latest_dir / "shadow_decision_bridge_audit_24h.json")
        assert shadow_bridge["status"] in {
            "NO_SHADOW_DECISION_BRIDGE_GAP",
            "SHADOW_DECISION_BRIDGE_AUDIT_REQUIRED",
            "SHADOW_DECISION_BRIDGE_MIRROR_COMPLETE",
        }
        assert "mirror_events" in shadow_bridge
        assert "mirror_event_examples" in shadow_bridge
        assert "mirror_event_count" in shadow_bridge["denominator"]
        assert "mirror_event_coverage_vs_shadow_bridge_gap" in shadow_bridge["denominator"]
        assert shadow_bridge["read_only_evidence_mirror"]["promotion_allowed"] is False
        assert shadow_bridge["read_only_evidence_mirror"]["automatic_runtime_change_allowed"] is False
        for event in shadow_bridge.get("mirror_events") or []:
            assert event["recommended_write_target"] == "shadow_decision_evidence_mirror_only"
            assert event["creates_pending_entry"] is False
            assert event["creates_paper_trade"] is False
            assert event["changes_runtime_mode"] is False
            assert "final_entry_contract" in event["forbidden_write_targets"]
        quality_probe_validation = load_json(latest_dir / "quality_timing_candidate_probe_validation_24h.json")
        assert quality_probe_validation["promotion_allowed"] is False
        assert quality_probe_validation["strategy_change_allowed"] is False
        assert quality_probe_validation["registered_probe_count"] == (
            quality_probe_validation.get("denominator") or {}
        ).get("registered_probe_count")
        assert quality_probe_validation["repeated_probe_count"] == (
            quality_probe_validation.get("denominator") or {}
        ).get("repeated_probe_count")
        assert "repeated_probe_rate" in quality_probe_validation
        assert "probe_validations" in quality_probe_validation
    print("SELF_TEST_PASS agent_capture_discovery_loop")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument("--data-dir", default="/app/data")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument(
        "--capture-hours",
        default="24,48,72",
        help="Comma-separated capture discovery windows. Default keeps the full 24/48/72 evaluator set.",
    )
    parser.add_argument("--expected-candidates", type=int, default=84)
    parser.add_argument("--out-root", default=DEFAULT_OUT_ROOT)
    parser.add_argument("--handoff-dir", default=DEFAULT_HANDOFF_DIR)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--markov-profiles", default="runtime,kline,candidate_lifecycle,candidate_source,candidate_signal_type,candidate_lifecycle_source")
    parser.add_argument("--report-timeout-sec", type=int, default=600)
    parser.add_argument("--test-timeout-sec", type=int, default=120)
    parser.add_argument("--max-scan-rows", type=int, default=2_000_000)
    parser.add_argument(
        "--oos-probe-hours",
        default="0.25,0.5,1",
        help="Comma-separated non-overlapping probe windows for shadow-only OOS readiness checks.",
    )
    parser.add_argument("--max-runs", type=int, default=1)
    parser.add_argument("--interval-sec", type=int, default=300)
    parser.add_argument("--initial-delay-sec", type=int, default=0)
    parser.add_argument(
        "--quote-fix-deploy-ts",
        type=int,
        default=0,
        help="Unix timestamp of quote writer fix deployment; enables post-deploy quote clean-window monitor.",
    )
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    outputs = []
    runs = max(1, args.max_runs)
    initial_delay = max(0, args.initial_delay_sec)
    if initial_delay:
        log_event("initial_delay_start", delay_sec=initial_delay)
        time.sleep(initial_delay)
        log_event("initial_delay_done", delay_sec=initial_delay)
    for index in range(runs):
        outputs.append(run_once(args))
        if index + 1 < runs:
            time.sleep(max(1, args.interval_sec))
    print(json.dumps({"schema_version": SCHEMA_VERSION, "runs": outputs}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
