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
REPORT_TEST_COMMANDS = (
    ("capture_self_test", ["scripts/offline_candidate_capture_discovery.py", "--self-test"]),
    ("pnl_cross_self_test", ["scripts/offline_candidate_cross_eval.py", "--self-test"]),
    ("virtual_markov_self_test", ["scripts/build_candidate_virtual_markov.py", "--self-test"]),
    ("volume_kline_audit_self_test", ["scripts/volume_kline_coverage_audit.py", "--self-test"]),
    ("matured_kline_volume_recheck_self_test", ["scripts/matured_kline_volume_recheck_audit.py", "--self-test"]),
    ("matured_volume_capture_cross_self_test", ["scripts/matured_volume_capture_cross_audit.py", "--self-test"]),
    ("hypothesis_validation_self_test", ["scripts/hypothesis_validation_audit.py", "--self-test"]),
    ("low_confidence_research_capture_self_test", ["scripts/low_confidence_research_capture_audit.py", "--self-test"]),
    ("a_class_mode_readiness_self_test", ["scripts/a_class_fastlane_mode_readiness_audit.py", "--self-test"]),
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


def build_candidate_effectiveness_report(capture):
    rows = list(capture.get("candidate_baseline") or [])
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
        classified.append({
            "candidate_id": row.get("candidate_id"),
            "family": row.get("family"),
            "classification": label,
            "match_count": match_count,
            "matched_raw_all_gold_silver_events": matched_gs,
            "business_match_recall_event": recall,
            "match_precision_event": precision,
        })
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
        "schema_version": "candidate_effectiveness_report.v1",
        "report_type": "candidate_effectiveness_24h",
        "candidate_count": len(rows),
        "classification_counts": counts,
        "top_candidates": classified[:50],
        "notes": [
            "Capture-first classifications are discovery-only and do not imply entry promotion.",
            "PnL is intentionally not used as the primary candidate-effectiveness criterion.",
        ],
    }


def build_markov_effectiveness_report(markov_reports, capture):
    profiles = {}
    total_green = 0
    total_yellow = 0
    total_insufficient = 0
    for name, report in sorted(markov_reports.items()):
        coverage = report.get("coverage") or {}
        counts = coverage.get("bucket_counts") or {}
        total_green += int(counts.get("green") or 0)
        total_yellow += int(counts.get("yellow") or 0)
        total_insufficient += int(counts.get("insufficient") or 0)
        profiles[name] = {
            "schema_version": report.get("schema_version"),
            "profile": report.get("profile") or name,
            "bucket_counts": counts,
            "coverage": coverage,
            "usage": report.get("usage") or "discovery_only",
        }
    status = "informative_discovery_only" if total_green or total_yellow else "insufficient_or_uninformative"
    return {
        "schema_version": "markov_effectiveness_report.v1",
        "report_type": "markov_effectiveness_24h",
        "status": status,
        "markov_used_for_promotion": False,
        "total_green_buckets": total_green,
        "total_yellow_buckets": total_yellow,
        "total_insufficient_buckets": total_insufficient,
        "profiles": profiles,
        "context_blockers": [
            blocker for blocker in ((capture.get("report_health") or {}).get("promotion_blockers") or [])
            if "coverage" in str(blocker) or "schema" in str(blocker)
        ],
    }


def build_capture_cross_validity_report(capture, context_report):
    quote_rate = ((context_report.get("quote_context_coverage") or {}).get("source_quote_clean_present_rate") or 0)
    quote_exec_rate = ((context_report.get("quote_context_coverage") or {}).get("source_quote_executable_present_rate") or 0)
    volume_rate = ((context_report.get("volume_profile_coverage") or {}).get("coverage_rate") or 0)
    kline_rate = ((context_report.get("kline_coverage") or {}).get("coverage_rate") or 0)
    valid = []
    invalid = []
    for row in capture.get("context_slices") or []:
        dim = row.get("dimension")
        reasons = []
        if dim in {"source_quote_clean", "source_quote_executable", "source_quote_executable_proxy"} and (quote_rate < 0.8 or quote_exec_rate < 0.8):
            reasons.append("quote_context_coverage_below_80pct")
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


def run_reports(run_dir, args):
    primary_hours = int(args.hours)
    capture_path = run_dir / f"capture_discovery_{primary_hours}h.json"
    capture_hours = sorted({primary_hours, 24, 48, 72})
    capture_paths = {
        hours: run_dir / f"capture_discovery_{hours}h.json"
        for hours in capture_hours
    }
    pnl_path = run_dir / f"pnl_cross_secondary_{primary_hours}h.json"
    raw_funnel_path = run_dir / f"raw_gold_silver_funnel_audit_{primary_hours}h.json"
    context_coverage_path = run_dir / f"context_coverage_audit_{primary_hours}h.json"
    candidate_effectiveness_path = run_dir / f"candidate_effectiveness_{primary_hours}h.json"
    markov_effectiveness_path = run_dir / f"markov_effectiveness_{primary_hours}h.json"
    capture_cross_validity_path = run_dir / f"capture_cross_validity_{primary_hours}h.json"
    a_class_fastlane_path = run_dir / f"a_class_fastlane_mode_audit_{primary_hours}h.json"
    context_blocker_monitor_path = run_dir / f"context_blocker_monitor_{primary_hours}h.json"
    volume_kline_audit_path = run_dir / f"volume_kline_coverage_audit_{primary_hours}h.json"
    matured_kline_recheck_path = run_dir / f"matured_kline_volume_recheck_audit_{primary_hours}h.json"
    matured_volume_capture_cross_path = run_dir / f"matured_volume_capture_cross_audit_{primary_hours}h.json"
    hypothesis_validation_path = run_dir / f"hypothesis_validation_audit_{primary_hours}h.json"
    low_confidence_research_path = run_dir / f"low_confidence_research_capture_audit_{primary_hours}h.json"
    markov_paths = {
        profile: run_dir / f"candidate_virtual_markov_{profile}_{primary_hours}h.json"
        for profile in args.markov_profiles.split(",")
        if profile
    }
    diagnostics = []

    db_ready = file_available(args.paper_db) and sqlite_has_table(args.paper_db, "candidate_shadow_observations")
    raw_ready = file_available(args.raw_db) and sqlite_has_table(args.raw_db, "raw_signal_outcomes")
    if not db_ready:
        capture = blocked_capture_report("paper_db_unavailable_or_missing_candidate_shadow_observations", args.paper_db, args.raw_db, primary_hours, args.expected_candidates)
        write_json(capture_path, capture)
        diagnostics.append({"name": "db_guard", "ok": False, "reason": "paper_db_unavailable_or_missing_candidate_shadow_observations"})
        return {"capture_primary": capture_path, "pnl": None, "markov": {}, "readiness": {}, "diagnostics": diagnostics}
    if not raw_ready:
        capture = blocked_capture_report("raw_signal_outcomes_db_unavailable", args.paper_db, args.raw_db, primary_hours, args.expected_candidates)
        write_json(capture_path, capture)
        diagnostics.append({"name": "db_guard", "ok": False, "reason": "raw_signal_outcomes_db_unavailable"})
        return {"capture_primary": capture_path, "pnl": None, "markov": {}, "readiness": {}, "diagnostics": diagnostics}

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
    readiness_paths = {}
    try:
        capture = load_json(capture_path)
        context_report = build_context_coverage_report(capture)
        write_derived_report(context_coverage_path, context_report)
        readiness_paths["context_coverage"] = context_coverage_path
        write_derived_report(candidate_effectiveness_path, build_candidate_effectiveness_report(capture))
        readiness_paths["candidate_effectiveness"] = candidate_effectiveness_path
        markov_reports = {name: load_json(path) for name, path in successful_markov.items() if Path(path).exists()}
        write_derived_report(markov_effectiveness_path, build_markov_effectiveness_report(markov_reports, capture))
        readiness_paths["markov_effectiveness"] = markov_effectiveness_path
        write_derived_report(capture_cross_validity_path, build_capture_cross_validity_report(capture, context_report))
        readiness_paths["capture_cross_validity"] = capture_cross_validity_path
        if raw_funnel_path.exists():
            raw_funnel = load_json(raw_funnel_path)
        else:
            raw_funnel = {}
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
        "a_class_fastlane_mode_readiness_audit",
        [
            "scripts/a_class_fastlane_mode_readiness_audit.py",
            "--db", args.paper_db,
            "--raw-funnel", str(raw_funnel_path),
            "--context-coverage", str(context_coverage_path),
            "--volume-kline-audit", str(volume_kline_audit_path),
            "--hours", str(primary_hours),
            "--out", str(a_class_fastlane_path),
        ],
        a_class_fastlane_path,
        timeout=args.report_timeout_sec,
    ))
    if a_class_fastlane_path.exists():
        readiness_paths["a_class_fastlane_mode_audit"] = a_class_fastlane_path
    if int(args.quote_fix_deploy_ts or 0) > 0:
        diagnostics.append(run_report(
            "context_blocker_monitor",
            [
                "scripts/context_blocker_monitor.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--hours", str(primary_hours),
                "--deploy-ts", str(int(args.quote_fix_deploy_ts)),
                "--out", str(context_blocker_monitor_path),
            ],
            context_blocker_monitor_path,
            timeout=args.report_timeout_sec,
        ))
        if context_blocker_monitor_path.exists():
            readiness_paths["context_blocker_monitor"] = context_blocker_monitor_path
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
    matured_volume_watch = [
        compact_matured_volume_hypothesis(row)
        for row in (matured_volume.get("top_watch_slices") or [])[:10]
    ]
    registry = {
        "schema_version": "hypothesis_registry.v2",
        "updated_at": utc_now(),
        "phase": "discovery_mesh",
        "promotion_allowed": False,
        "hypotheses": {
            "H1_building_volume_active_microstructure": compact_hypothesis(verdict.get("H1_capture_metrics") or {}),
            "H2_shallow_pullback_matrix_evaluator": compact_hypothesis(verdict.get("H2_capture_metrics") or {}),
        },
        "watchlist_hypotheses": capture.get("watchlist_hypotheses", [])[:25],
        "shadow_only_matured_volume_watch": matured_volume_watch,
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
        f"- promotion_allowed: `{str(bool(verdict.get('promotion_allowed'))).lower()}`",
        f"- human_action_required: `{str(bool(verdict.get('human_action_required'))).lower()}`",
        f"- strategy_change_allowed: `{str(bool(verdict.get('strategy_change_allowed'))).lower()}`",
        f"- non_quote_sensitive_capture_discovery_allowed: `{str(bool(verdict.get('non_quote_sensitive_capture_discovery_allowed'))).lower()}`",
        f"- quote_sensitive_slices_blocked: `{str(bool(verdict.get('quote_sensitive_slices_blocked'))).lower()}`",
        f"- formal_volume_sensitive_slices_blocked: `{str(bool(verdict.get('formal_volume_sensitive_slices_blocked'))).lower()}`",
        f"- shadow_matured_volume_slices_evaluable: `{str(bool(verdict.get('shadow_matured_volume_slices_evaluable'))).lower()}`",
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
    readiness_reports = {}
    for key, path in readiness_paths.items():
        if path and Path(path).exists():
            try:
                readiness_reports[key] = load_json(path)
            except Exception:
                pass
    context_report = readiness_reports.get("context_coverage") or {}
    readiness_reports["current_commit"] = git_commit()
    readiness_reports["deployment_commit"] = (
        os.environ.get("ZEABUR_GIT_COMMIT_SHA")
        or os.environ.get("ZEABUR_GIT_COMMIT")
        or os.environ.get("ZEABUR_COMMIT_SHA")
        or os.environ.get("COMMIT_SHA")
        or os.environ.get("GIT_COMMIT")
        or os.environ.get("SOURCE_VERSION")
        or os.environ.get("GITHUB_SHA")
        or readiness_reports["current_commit"]
    )
    if not readiness_reports["current_commit"]:
        readiness_reports["current_commit"] = readiness_reports["deployment_commit"]
    if not readiness_reports["deployment_commit"]:
        readiness_reports["deployment_commit"] = readiness_reports["current_commit"]
    readiness_reports["volume_profile_coverage"] = context_report.get("volume_profile_coverage") or {}
    readiness_reports["kline_coverage"] = context_report.get("kline_coverage") or {}
    readiness_reports["next_highest_priority_blocker"] = first_blocker_priority(
        list((capture.get("report_health") or {}).get("promotion_blockers") or [])
        + list(context_report.get("blockers") or [])
    )
    verdict = build_verdict(
        capture,
        pnl,
        markov_reports,
        tests=tests if tests.get("status") != "pending" else {},
        readiness_reports=readiness_reports,
    )
    if any(not row.get("ok") for row in diagnostics):
        verdict["blockers"] = sorted(set((verdict.get("blockers") or []) + ["report_generation_failed"]))
        verdict["classification"] = "BLOCKED_DATA"
        verdict["promotion_allowed"] = False
    if state != "final":
        verdict["blockers"] = sorted(set((verdict.get("blockers") or []) + [state]))
        verdict["classification"] = "BLOCKED_DATA"
        verdict["promotion_allowed"] = False
    verdict["loop"] = {
        "schema_version": SCHEMA_VERSION,
        "run_id": rid,
        "run_dir": str(run_dir),
        "state": state,
        "report_diagnostics": diagnostics,
    }

    verdict_path = run_dir / "reviewer_verdict.json"
    write_json(verdict_path, verdict)
    registry = update_hypothesis_registry(args.registry, verdict, capture)

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
    return paper, raw, kline


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        paper, raw, kline = create_self_test_dbs(root)
        args = argparse.Namespace(
            paper_db=str(paper),
            raw_db=str(raw),
            kline_db=str(kline),
            hours=24,
            expected_candidates=2,
            out_root=str(root / "agent_runs"),
            handoff_dir=str(root / "agent_handoffs"),
            registry=str(root / "hypothesis_registry.json"),
            markov_profiles="runtime,kline",
            report_timeout_sec=60,
            test_timeout_sec=60,
            max_scan_rows=2_000_000,
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
            "A_CLASS_mode_status",
            "final_entry_contract_blocker_breakdown",
            "per_candidate_effectiveness_summary",
            "Markov_effectiveness_summary",
            "two_d_cross_validity_summary",
            "promotion_allowed",
            "human_action_required",
            "tests_passed",
        ]
        missing = [field for field in required_verdict_fields if field not in verdict]
        assert not missing, missing
        latest_dir = Path(result["latest_verdict"]).parent
        registry = load_json(result["hypothesis_registry"])
        assert registry["schema_version"] == "hypothesis_registry.v2"
        assert "shadow_only_matured_volume_watch" in registry
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
                    "top_watch_slices": [
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
                        }
                    ]
                },
            },
            {"watchlist_hypotheses": []},
        )
        assert manual_registry["shadow_only_matured_volume_watch"][0]["hypothesis_id"] == "matured_volume:entry_mode_registry:ath_flat_structure_tiny_scout:building"
        assert manual_registry["shadow_only_matured_volume_watch"][0]["promotion_allowed"] is False
        required_artifacts = [
            "capture_discovery_24h.json",
            "capture_discovery_48h.json",
            "capture_discovery_72h.json",
            "raw_gold_silver_funnel_audit_24h.json",
            "a_class_fastlane_mode_audit_24h.json",
            "candidate_effectiveness_24h.json",
            "markov_effectiveness_24h.json",
            "capture_cross_validity_24h.json",
            "pnl_cross_secondary_24h.json",
            "context_coverage_audit_24h.json",
            "context_blocker_monitor_24h.json",
            "volume_kline_coverage_audit_24h.json",
            "matured_kline_volume_recheck_audit_24h.json",
            "matured_volume_capture_cross_audit_24h.json",
            "hypothesis_validation_audit_24h.json",
            "low_confidence_research_capture_audit_24h.json",
        ]
        missing_artifacts = [name for name in required_artifacts if not (latest_dir / name).exists()]
        assert not missing_artifacts, missing_artifacts
    print("SELF_TEST_PASS agent_capture_discovery_loop")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--expected-candidates", type=int, default=84)
    parser.add_argument("--out-root", default=DEFAULT_OUT_ROOT)
    parser.add_argument("--handoff-dir", default=DEFAULT_HANDOFF_DIR)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--markov-profiles", default="runtime,kline")
    parser.add_argument("--report-timeout-sec", type=int, default=600)
    parser.add_argument("--test-timeout-sec", type=int, default=120)
    parser.add_argument("--max-scan-rows", type=int, default=2_000_000)
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
