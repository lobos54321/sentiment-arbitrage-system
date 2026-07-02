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


SCHEMA_VERSION = "agent_capture_discovery_loop.v2"
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
    ("kline_coverage_resolution_self_test", ["scripts/kline_coverage_resolution_audit.py", "--self-test"]),
    ("quality_timing_reject_research_self_test", ["scripts/quality_timing_reject_research_audit.py", "--self-test"]),
    ("candidate_downstream_readiness_self_test", ["scripts/candidate_downstream_readiness_audit.py", "--self-test"]),
    ("a_class_mode_readiness_self_test", ["scripts/a_class_fastlane_mode_readiness_audit.py", "--self-test"]),
    ("runtime_health_snapshot_self_test", ["scripts/runtime_health_snapshot_audit.py", "--self-test"]),
    ("strategy_memory_audit_self_test", ["scripts/offline_strategy_memory_audit.py", "--self-test"]),
    ("strategy_memory_validation_self_test", ["scripts/strategy_memory_validation.py", "--self-test"]),
    ("capture_60_target_loop_self_test", ["scripts/capture_60_target_loop.py", "--self-test"]),
    ("autoloop_lightweight_reconciliation_self_test", ["scripts/autoloop_lightweight_reconciliation.py", "--self-test"]),
    ("oos_probe_refresh_self_test", ["scripts/refresh_oos_readiness_probes.py", "--self-test"]),
    ("reviewer_self_test", ["scripts/review_agent_verdict.py", "--self-test"]),
    ("handoff_self_test", ["scripts/generate_codex_handoff.py", "--self-test"]),
)

DEFAULT_MARKOV_PROFILES = (
    "runtime,kline,candidate_lifecycle,candidate_source,candidate_signal_type,"
    "candidate_lifecycle_source,candidate_volume,candidate_quote"
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


def attach_latest_readiness_artifacts(verdict, readiness_paths):
    """Keep verdict, handoff, and summary aligned with post-finalize artifacts."""
    if not isinstance(verdict, dict):
        return verdict
    oos_path = readiness_paths.get("oos_readiness_summary")
    if oos_path and Path(oos_path).exists():
        try:
            oos_summary = load_json(oos_path)
        except Exception:
            oos_summary = None
        if isinstance(oos_summary, dict):
            verdict["oos_readiness_summary_v3"] = oos_summary
            verdict["oos_readiness_summary"] = oos_summary
    return verdict


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


def strategy_memory_artifact_dirs(args):
    dirs = []
    env_dir = os.environ.get("STRATEGY_MEMORY_ARTIFACT_DIR")
    if env_dir:
        dirs.append(Path(env_dir))
    if getattr(args, "strategy_memory_dir", None):
        dirs.append(Path(args.strategy_memory_dir))
    data_dir = Path(args.data_dir)
    dirs.extend([
        data_dir / "strategy_memory_local",
        data_dir / "strategy_memory",
        data_dir,
    ])
    seen = set()
    out = []
    for directory in dirs:
        resolved = directory.expanduser()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        out.append(resolved)
    return out


def find_strategy_memory_artifact(args, filename):
    for directory in strategy_memory_artifact_dirs(args):
        path = directory / filename
        if path.exists():
            return path
    return None


def load_optional_strategy_json(args, filename, *, required_keys=()):
    path = find_strategy_memory_artifact(args, filename)
    if not path:
        return None, None
    payload = load_json(path)
    if not isinstance(payload, dict):
        raise ValueError(f"{filename} must be a JSON object")
    missing = [key for key in required_keys if key not in payload]
    if missing:
        raise ValueError(f"{filename} missing required keys: {missing}")
    return payload, path


def strategy_memory_requires_paper_db(hypothesis):
    hid = str(hypothesis.get("id") or hypothesis.get("hypothesis_id") or "").upper()
    family = str(hypothesis.get("strategy_family") or "").lower()
    required_features = {str(item).lower() for item in (hypothesis.get("required_features") or [])}
    paper_features = {
        "candidate_matches",
        "decision_status",
        "pending_status",
        "final_entry_status",
        "entry_price",
        "peak_pct",
        "time_path_or_kline",
        "decision_ts",
    }
    return bool(
        hid.startswith("SM-EXIT")
        or "EXIT" in hid
        or "EXECUTION-DELAY" in hid
        or "FILTERED-WINNER" in hid
        or "exit" in family
        or "delay" in family
        or "filtered winner" in family
        or required_features.intersection(paper_features)
    )


def is_strategy_memory_exit_only(hypothesis, mapping_row):
    hid = str(hypothesis.get("id") or hypothesis.get("hypothesis_id") or "").upper()
    family = str(hypothesis.get("strategy_family") or "").lower()
    mapping_status = str((mapping_row or {}).get("mapping_status") or "")
    return bool(hid.startswith("SM-EXIT") or "exit" in family or mapping_status == "missing_exit_shadow_sim_only")


def is_strategy_memory_delay_replay_only(hypothesis, mapping_row):
    hid = str(hypothesis.get("id") or hypothesis.get("hypothesis_id") or "").upper()
    family = str(hypothesis.get("strategy_family") or "").lower()
    mapping_status = str((mapping_row or {}).get("mapping_status") or "")
    return bool(
        "EXECUTION-DELAY" in hid
        or "delay" in family
        or mapping_status == "missing_delay_replay_only"
    )


def compact_strategy_memory_hypothesis(hypothesis, mapping_row, *, paper_db_available):
    hid = hypothesis.get("id") or hypothesis.get("hypothesis_id")
    future_features = list(hypothesis.get("future_or_posthoc_features") or [])
    blocked_contexts = list((mapping_row or {}).get("blocked_contexts") or [])
    rejected_future_data = bool(
        future_features
        or (mapping_row or {}).get("requires_future_data_conversion")
        or "future_data_features_must_be_labels_only" in blocked_contexts
    )
    exit_only = is_strategy_memory_exit_only(hypothesis, mapping_row)
    delay_replay_only = is_strategy_memory_delay_replay_only(hypothesis, mapping_row)
    requires_paper = strategy_memory_requires_paper_db(hypothesis)
    evidence_incomplete = bool(requires_paper and not paper_db_available)
    route = "strategy_memory_shadow_context_only"
    if exit_only:
        route = "exit_policy_shadow_simulator_only"
    elif delay_replay_only:
        route = "delay_replay_only"
    return {
        "hypothesis_id": hid,
        "name": hypothesis.get("name"),
        "strategy_family": hypothesis.get("strategy_family"),
        "priority": hypothesis.get("priority"),
        "allowed_use": "shadow_only",
        "promotion_allowed": False,
        "evidence_level": "historical_memory",
        "historical_pnl_is_promotion_evidence": False,
        "same_window_discovery_is_promotion_evidence": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "candidate_catalog_change_allowed": False,
        "mapping_status": (mapping_row or {}).get("mapping_status"),
        "mapped_existing_candidate_ids": ((mapping_row or {}).get("existing_candidate_ids") or [])[:24],
        "blocked_contexts": blocked_contexts,
        "missing_shadow_candidate_handoff_required": str((mapping_row or {}).get("mapping_status") or "").startswith("missing_"),
        "rejected_future_data": rejected_future_data,
        "future_or_posthoc_features": future_features,
        "requires_paper_trades_db": requires_paper,
        "evidence_incomplete": evidence_incomplete,
        "evidence_incomplete_reason": "paper_trades_db_unavailable" if evidence_incomplete else None,
        "route": route,
        "exit_only": exit_only,
        "delay_replay_only": delay_replay_only,
        "next_validation_required": hypothesis.get("next_validation_required"),
        "entry_definition": hypothesis.get("entry_definition"),
        "exit_definition": hypothesis.get("exit_definition"),
        "required_features": hypothesis.get("required_features") or [],
        "time_legal_features": hypothesis.get("time_legal_features") or [],
        "known_risks": hypothesis.get("known_risks") or [],
    }


def build_strategy_memory_ingestion_summary(args, out_path):
    hypotheses, hypotheses_path = load_optional_strategy_json(
        args,
        "strategy_memory_hypotheses.json",
        required_keys=("hypotheses",),
    )
    paper_db_available = file_available(args.paper_db) and sqlite_has_table(args.paper_db, "candidate_shadow_observations")
    if not hypotheses_path:
        summary = {
            "schema_version": "strategy_memory_ingestion_summary.v1",
            "report_type": "strategy_memory_ingestion_summary",
            "generated_at": utc_now(),
            "available": False,
            "reason": "strategy_memory_hypotheses_json_not_present",
            "artifact_search_dirs": [str(path) for path in strategy_memory_artifact_dirs(args)],
            "paper_trades_db_available": paper_db_available,
            "promotion_allowed": False,
            "allowed_use": "shadow_only",
            "evidence_level": "historical_memory",
        }
        write_json(out_path, summary)
        return summary
    if not isinstance(hypotheses.get("hypotheses"), list):
        raise ValueError("strategy_memory_hypotheses.json field hypotheses must be a list")

    mapping, mapping_path = load_optional_strategy_json(
        args,
        "strategy_memory_candidate_mapping.json",
        required_keys=("mappings",),
    )
    if mapping is not None and not isinstance(mapping.get("mappings"), list):
        raise ValueError("strategy_memory_candidate_mapping.json field mappings must be a list")
    queue, queue_path = load_optional_strategy_json(args, "strategy_memory_prioritized_queue.json")
    dossier, dossier_path = load_optional_strategy_json(args, "filtered_winner_dossier_24h.json")
    exit_report, exit_report_path = load_optional_strategy_json(args, "exit_policy_shadow_simulator_24h.json")
    delay_report, delay_report_path = load_optional_strategy_json(args, "execution_delay_adjusted_replay_24h.json")

    mapping = mapping or {}
    mapping_by_id = {row.get("hypothesis_id"): row for row in mapping.get("mappings", []) if isinstance(row, dict)}
    rows = [
        compact_strategy_memory_hypothesis(row, mapping_by_id.get(row.get("id")), paper_db_available=paper_db_available)
        for row in hypotheses.get("hypotheses", [])
        if isinstance(row, dict)
    ]
    rows.sort(key=lambda row: safe_int(row.get("priority"), 0), reverse=True)
    missing_handoffs = [
        {
            "hypothesis_id": row.get("hypothesis_id"),
            "strategy_family": row.get("strategy_family"),
            "mapping_status": row.get("mapping_status"),
            "route": row.get("route"),
            "allowed_action": "generate_codex_handoff_only",
            "candidate_catalog_change_allowed": False,
            "promotion_allowed": False,
        }
        for row in rows
        if row.get("missing_shadow_candidate_handoff_required")
    ]
    rejected_future = [row for row in rows if row.get("rejected_future_data")]
    evidence_incomplete = [row for row in rows if row.get("evidence_incomplete")]
    top_ids = [row.get("hypothesis_id") for row in rows[:10]]
    summary = {
        "schema_version": "strategy_memory_ingestion_summary.v1",
        "report_type": "strategy_memory_ingestion_summary",
        "generated_at": utc_now(),
        "available": True,
        "artifact_source_dir": str(hypotheses_path.parent),
        "artifact_paths": {
            "strategy_memory_hypotheses": str(hypotheses_path),
            "strategy_memory_candidate_mapping": str(mapping_path) if mapping_path else None,
            "strategy_memory_prioritized_queue": str(queue_path) if queue_path else None,
            "filtered_winner_dossier": str(dossier_path) if dossier_path else None,
            "exit_policy_shadow_simulator": str(exit_report_path) if exit_report_path else None,
            "execution_delay_adjusted_replay": str(delay_report_path) if delay_report_path else None,
        },
        "strategy_memory_hypotheses_count": hypotheses.get("hypotheses_count", len(rows)),
        "mapped_to_existing_candidates": mapping.get("mapped_to_existing_candidates", 0),
        "missing_shadow_candidates": mapping.get("missing_shadow_candidates", len(missing_handoffs)),
        "rejected_future_data_hypotheses": mapping.get(
            "rejected_future_data_hypotheses",
            hypotheses.get("rejected_future_data_hypotheses_count", len(rejected_future)),
        ),
        "top_10_shadow_hypotheses": top_ids,
        "top_10_shadow_hypothesis_details": [
            {
                "hypothesis_id": row.get("hypothesis_id"),
                "name": row.get("name"),
                "strategy_family": row.get("strategy_family"),
                "priority": row.get("priority"),
                "mapping_status": row.get("mapping_status"),
                "route": row.get("route"),
                "rejected_future_data": row.get("rejected_future_data"),
                "evidence_incomplete": row.get("evidence_incomplete"),
                "promotion_allowed": False,
            }
            for row in rows[:10]
        ],
        "filtered_winner_count": (dossier or {}).get("filtered_winner_count", 0),
        "exit_policy_variants_tested": (exit_report or {}).get("exit_policy_variants_tested", 0),
        "delay_replay_done": bool((delay_report or {}).get("delay_replay_done")),
        "paper_trades_db_available": paper_db_available,
        "evidence_incomplete_hypotheses": len(evidence_incomplete),
        "missing_shadow_candidate_handoffs": missing_handoffs,
        "exit_only_hypotheses": [row.get("hypothesis_id") for row in rows if row.get("exit_only")],
        "allowed_use": "shadow_only",
        "promotion_allowed": False,
        "evidence_level": "historical_memory",
        "candidate_catalog_change_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "hypotheses": rows,
        "notes": [
            "Strategy Memory artifacts are historical-memory discovery context only.",
            "Missing shadow candidates are handoff-only; candidate catalog is not modified automatically.",
            "Future/posthoc features are labels only and are rejected as entry evidence.",
            "Exit-only hypotheses are routed to exit_policy_shadow_simulator only.",
        ],
    }
    write_json(out_path, summary)
    return summary


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
    total_red = 0
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
        total_red += diagnostic["red_buckets"]
        total_insufficient += diagnostic["insufficient_buckets"]
        profiles[name] = {
            "schema_version": report.get("schema_version"),
            "profile": report.get("profile") or name,
            "key_dimensions": report.get("key_dimensions") or [],
            "bucket_counts": counts,
            "coverage": coverage,
            "usage": report.get("usage") or "discovery_only",
        }
    total_bucket_count = total_green + total_yellow + total_red + total_insufficient
    total_informative_buckets = total_green + total_yellow
    total_closed_virtual_rows = sum(row.get("closed_virtual_rows") or 0 for row in profile_diagnostics.values())
    total_keys_emitted = sum(row.get("keys_emitted") or 0 for row in profile_diagnostics.values())
    insufficient_rate = (
        round(total_insufficient / total_bucket_count, 6)
        if total_bucket_count
        else None
    )
    informative_bucket_rate = (
        round(total_informative_buckets / total_bucket_count, 6)
        if total_bucket_count
        else None
    )
    coverage_sufficient = bool(total_closed_virtual_rows and total_keys_emitted and total_informative_buckets)
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
        classification = "MARKOV_DISCOVERY_INFORMATIVE"
        next_action = "keep_markov_discovery_only_and_require_capture_oos_before_promotion"
    elif any(row["keys_emitted"] == 0 and row["closed_virtual_rows"] for row in profile_diagnostics.values()):
        status = "profile_fragmented_or_uninformative"
        classification = "MARKOV_PROFILE_FRAGMENTED_OR_UNINFORMATIVE"
        next_action = "run_or_review_coarse_non_quote_non_kline_profiles_before_claiming_markov_value"
    elif any(row["red_buckets"] for row in profile_diagnostics.values()):
        status = "red_only_non_informative"
        classification = "MARKOV_RED_ONLY_NON_INFORMATIVE"
        next_action = "do_not_use_markov_as_positive_evidence; keep as discovery_risk_context_only"
    else:
        status = "insufficient_or_uninformative"
        classification = "MARKOV_INSUFFICIENT_OR_UNINFORMATIVE"
        next_action = "collect_more_closed_virtual_rows_and_keep_markov_out_of_promotion"
    return {
        "schema_version": "markov_effectiveness_report.v2",
        "report_type": "markov_effectiveness_24h",
        "classification": classification,
        "status": status,
        "next_action": next_action,
        "evidence_level": "discovery_same_window",
        "usage": "research_only_markov_information_value",
        "promotion_allowed": False,
        "markov_used_for_promotion": False,
        "profile_count": len(profiles),
        "closed_virtual_rows": total_closed_virtual_rows,
        "keys_emitted": total_keys_emitted,
        "total_green_buckets": total_green,
        "total_yellow_buckets": total_yellow,
        "total_red_buckets": total_red,
        "total_insufficient_buckets": total_insufficient,
        "total_bucket_count": total_bucket_count,
        "informative_bucket_count": total_informative_buckets,
        "informative_bucket_rate": informative_bucket_rate,
        "insufficient_rate": insufficient_rate,
        "coverage_sufficient": coverage_sufficient,
        "insufficient_rate_acceptable": (
            insufficient_rate is not None and insufficient_rate < 0.5
        ),
        "capture_lift_available": False,
        "coverage_on_raw_dogs": None,
        "coverage_on_raw_dogs_status": "not_joined_to_raw_gold_silver_events_in_this_report",
        "raw_dog_density_lift": None,
        "decision_capture_lift": None,
        "pending_capture_lift": None,
        "final_eligibility_lift": None,
        "mode_disabled_adjusted_final_eligibility_lift": None,
        "oos_status": "OOS_REQUIRED_BEFORE_PROMOTION",
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


def downstream_global_rates(candidate_downstream):
    candidate_downstream = candidate_downstream or {}
    denominator = safe_int(candidate_downstream.get("raw_gold_silver_signal_denominator"), 0) or 0
    stage_counts = candidate_downstream.get("stage_counts") or {}
    return {
        "decision_rate": safe_rate(stage_counts.get("decision"), denominator),
        "pass_allow_rate": safe_rate(stage_counts.get("pass_allow"), denominator),
        "pending_rate": safe_rate(stage_counts.get("pending"), denominator),
        "final_entry_rate": safe_rate(stage_counts.get("final_entry"), denominator),
        "mode_disabled_adjusted_final_eligibility_rate": safe_rate(
            stage_counts.get("final_mode_disabled_only"),
            denominator,
        ),
        "paper_committed_rate": safe_rate(stage_counts.get("paper_committed"), denominator),
    }


def candidate_downstream_index(candidate_downstream):
    rows = []
    candidate_downstream = candidate_downstream or {}
    rows.extend(candidate_downstream.get("all_candidates") or [])
    rows.extend(candidate_downstream.get("top_candidates") or [])
    out = {}
    for row in rows:
        if isinstance(row, dict) and row.get("candidate_id") and row.get("candidate_id") not in out:
            out[row.get("candidate_id")] = row
    return out


def round_lift(value, baseline):
    if value is None or baseline is None:
        return None
    return round(float(value) - float(baseline), 6)


def downstream_proxy_for_candidate(candidate_id, downstream_by_candidate, global_rates):
    row = downstream_by_candidate.get(candidate_id) or {}
    if not row:
        return {
            "downstream_lift_available": False,
            "downstream_lift_scope": "unavailable",
            "downstream_lift_unavailable_reason": "candidate_downstream_readiness_missing",
            "decision_lift": None,
            "pass_allow_lift": None,
            "pending_lift": None,
            "final_entry_lift": None,
            "mode_adjusted_final_eligibility_lift": None,
            "paper_capture_lift": None,
        }
    decision_rate = row.get("decision_record_rate_after_match")
    pass_allow_rate = row.get("pass_allow_rate_after_match")
    pending_rate = row.get("pending_rate_after_match")
    final_rate = row.get("final_entry_contract_rate_after_match")
    mode_adjusted_rate = row.get("mode_disabled_adjusted_final_eligibility_rate_after_match")
    paper_rate = row.get("paper_trade_committed_rate_after_match")
    return {
        "downstream_lift_available": True,
        "downstream_lift_scope": "candidate_level_after_match_proxy_not_slice_specific",
        "downstream_slice_join_required_for_promotion_evidence": True,
        "decision_capture_rate_after_match": decision_rate,
        "pass_allow_capture_rate_after_match": pass_allow_rate,
        "pending_capture_rate_after_match": pending_rate,
        "final_entry_rate_after_match": final_rate,
        "mode_adjusted_final_eligibility_rate_after_match": mode_adjusted_rate,
        "paper_capture_rate_after_match": paper_rate,
        "decision_lift": round_lift(decision_rate, global_rates.get("decision_rate")),
        "pass_allow_lift": round_lift(pass_allow_rate, global_rates.get("pass_allow_rate")),
        "pending_lift": round_lift(pending_rate, global_rates.get("pending_rate")),
        "final_entry_lift": round_lift(final_rate, global_rates.get("final_entry_rate")),
        "mode_adjusted_final_eligibility_lift": round_lift(
            mode_adjusted_rate,
            global_rates.get("mode_disabled_adjusted_final_eligibility_rate"),
        ),
        "paper_capture_lift": round_lift(paper_rate, global_rates.get("paper_committed_rate")),
        "candidate_downstream_classification": row.get("classification"),
        "matched_raw_gs_signals_after_match": row.get("matched_raw_gs_signals"),
    }


def downstream_stage_signal_id_sets(candidate_downstream):
    stage_signal_ids = (candidate_downstream or {}).get("stage_signal_ids") or {}
    return {
        key: {str(value) for value in values or [] if value is not None}
        for key, values in stage_signal_ids.items()
    }


def downstream_slice_for_signal_ids(signal_ids, stage_sets, global_rates):
    signal_set = {str(value) for value in signal_ids or [] if value is not None}
    denominator = len(signal_set)
    if not denominator:
        return {
            "downstream_lift_available": False,
            "downstream_lift_scope": "slice_level_unavailable",
            "downstream_lift_unavailable_reason": "slice_matched_gold_silver_signal_ids_missing",
            "downstream_slice_join_available": False,
            "downstream_slice_join_required_for_promotion_evidence": True,
            "decision_lift": None,
            "pass_allow_lift": None,
            "pending_lift": None,
            "final_entry_lift": None,
            "mode_adjusted_final_eligibility_lift": None,
            "paper_capture_lift": None,
        }
    decision_count = len(signal_set & stage_sets.get("decision", set()))
    pass_allow_count = len(signal_set & stage_sets.get("pass_allow", set()))
    pending_count = len(signal_set & stage_sets.get("pending", set()))
    final_count = len(signal_set & stage_sets.get("final_entry", set()))
    mode_adjusted_count = len(signal_set & stage_sets.get("final_mode_disabled_only", set()))
    paper_count = len(signal_set & stage_sets.get("paper_committed", set()))
    decision_rate = safe_rate(decision_count, denominator)
    pass_allow_rate = safe_rate(pass_allow_count, denominator)
    pending_rate = safe_rate(pending_count, denominator)
    final_rate = safe_rate(final_count, denominator)
    mode_adjusted_rate = safe_rate(mode_adjusted_count, denominator)
    paper_rate = safe_rate(paper_count, denominator)
    return {
        "downstream_lift_available": True,
        "downstream_lift_scope": "slice_level_matched_gold_silver_signal_id",
        "downstream_slice_join_available": True,
        "downstream_slice_join_required_for_promotion_evidence": False,
        "slice_downstream_signal_count": denominator,
        "decision_count_after_match": decision_count,
        "pass_allow_count_after_match": pass_allow_count,
        "pending_count_after_match": pending_count,
        "final_entry_count_after_match": final_count,
        "mode_adjusted_final_eligibility_count_after_match": mode_adjusted_count,
        "paper_capture_count_after_match": paper_count,
        "decision_capture_rate_after_match": decision_rate,
        "pass_allow_capture_rate_after_match": pass_allow_rate,
        "pending_capture_rate_after_match": pending_rate,
        "final_entry_rate_after_match": final_rate,
        "mode_adjusted_final_eligibility_rate_after_match": mode_adjusted_rate,
        "paper_capture_rate_after_match": paper_rate,
        "decision_lift": round_lift(decision_rate, global_rates.get("decision_rate")),
        "pass_allow_lift": round_lift(pass_allow_rate, global_rates.get("pass_allow_rate")),
        "pending_lift": round_lift(pending_rate, global_rates.get("pending_rate")),
        "final_entry_lift": round_lift(final_rate, global_rates.get("final_entry_rate")),
        "mode_adjusted_final_eligibility_lift": round_lift(
            mode_adjusted_rate,
            global_rates.get("mode_disabled_adjusted_final_eligibility_rate"),
        ),
        "paper_capture_lift": round_lift(paper_rate, global_rates.get("paper_committed_rate")),
    }


QUOTE_SENSITIVE_CROSS_DIMS = {
    "source_quote_clean",
    "source_quote_executable",
    "source_quote_executable_proxy",
}
KLINE_CROSS_DIMS = {
    "candle_pattern",
    "fbr_time_legal",
    "fbr_lookahead_warning",
}
CORE_METADATA_CROSS_DIMS = {
    "signal_type": "core_signal_metadata",
    "market_cap_bucket": "core_market_metadata",
    "mode_route": "core_route_metadata",
}


def context_field_rate(context_report, field):
    field_cov = (context_report.get("context_field_coverage") or {}).get(field) or {}
    for key in ("effective_present_rate", "coverage_rate"):
        if field_cov.get(key) is not None:
            return field_cov.get(key)
    if field_cov.get("coverage_pct") is not None:
        return pct_to_rate(field_cov.get("coverage_pct"))
    return None


def capture_cross_dimension_eligibility(dim, rates):
    """Return a stable eligibility decision for a capture-cross dimension."""
    if dim in QUOTE_SENSITIVE_CROSS_DIMS:
        ok = rates["quote_clean"] >= 0.8 and rates["quote_executable"] >= 0.8
        return {
            "dimension_group": "quote-sensitive",
            "dimension_eligibility_status": "CLEAN" if ok else "BLOCKED_CONTEXT_COVERAGE",
            "invalid_reasons": [] if ok else ["quote_context_coverage_below_80pct"],
        }
    if dim == "source_component":
        ok = rates["source_component"] is not None and rates["source_component"] >= 0.8
        return {
            "dimension_group": "source_component",
            "dimension_eligibility_status": "CLEAN" if ok else "BLOCKED_CONTEXT_COVERAGE",
            "invalid_reasons": [] if ok else ["source_component_coverage_below_80pct"],
        }
    if dim == "source_resonance_state":
        rate = rates["source_resonance_state"]
        ok = rate is not None and rate >= 0.8
        return {
            "dimension_group": "source_resonance_state",
            "dimension_eligibility_status": "CLEAN" if ok else "BLOCKED_CONTEXT_COVERAGE",
            "invalid_reasons": [] if ok else ["source_resonance_state_coverage_below_80pct"],
        }
    if dim in {"lifecycle_profile", "lifecycle_state"}:
        ok = rates["lifecycle"] is not None and rates["lifecycle"] >= 0.8
        return {
            "dimension_group": "lifecycle",
            "dimension_eligibility_status": "CLEAN" if ok else "BLOCKED_CONTEXT_COVERAGE",
            "invalid_reasons": [] if ok else ["lifecycle_profile_coverage_below_80pct"],
        }
    if dim == "volume_profile":
        ok = rates["volume"] is not None and rates["volume"] >= 0.8
        return {
            "dimension_group": "volume",
            "dimension_eligibility_status": "CLEAN" if ok else "BLOCKED_CONTEXT_COVERAGE",
            "invalid_reasons": [] if ok else ["volume_profile_coverage_below_80pct"],
        }
    if dim in KLINE_CROSS_DIMS:
        ok = rates["kline"] is not None and rates["kline"] >= 0.8
        return {
            "dimension_group": "kline",
            "dimension_eligibility_status": "CLEAN" if ok else "BLOCKED_CONTEXT_COVERAGE",
            "invalid_reasons": [] if ok else ["kline_coverage_below_80pct"],
        }
    if dim == "hard_gate_status":
        rate = rates["hard_gate_status"]
        ok = rate is not None and rate >= 0.8
        return {
            "dimension_group": "hard_gate",
            "dimension_eligibility_status": "CLEAN" if ok else "BLOCKED_CONTEXT_COVERAGE",
            "invalid_reasons": [] if ok else ["hard_gate_status_coverage_below_80pct"],
        }
    if dim == "markov_bucket":
        return {
            "dimension_group": "Markov",
            "dimension_eligibility_status": "BLOCKED_CONTEXT_COVERAGE",
            "invalid_reasons": ["markov_effectiveness_not_loaded_for_capture_cross_validity"],
        }
    if dim in CORE_METADATA_CROSS_DIMS:
        return {
            "dimension_group": CORE_METADATA_CROSS_DIMS[dim],
            "dimension_eligibility_status": "CORE_METADATA_ALLOWED",
            "invalid_reasons": [],
        }
    return {
        "dimension_group": "unregistered",
        "dimension_eligibility_status": "UNREGISTERED_DIMENSION",
        "invalid_reasons": ["unregistered_context_dimension"],
    }


def build_capture_cross_validity_report(capture, context_report, candidate_downstream=None):
    quote_rate = ((context_report.get("quote_context_coverage") or {}).get("source_quote_clean_present_rate") or 0)
    quote_exec_rate = ((context_report.get("quote_context_coverage") or {}).get("source_quote_executable_present_rate") or 0)
    source_component_rate = ((context_report.get("source_component_coverage") or {}).get("effective_present_rate"))
    if source_component_rate is None:
        source_component_rate = ((context_report.get("source_component_coverage") or {}).get("coverage_rate") or 0)
    lifecycle_rate = ((context_report.get("lifecycle_profile_coverage") or {}).get("effective_present_rate"))
    if lifecycle_rate is None:
        lifecycle_rate = ((context_report.get("lifecycle_profile_coverage") or {}).get("coverage_rate") or 0)
    volume_rate = ((context_report.get("volume_profile_coverage") or {}).get("coverage_rate") or 0)
    kline_rate = ((context_report.get("kline_coverage") or {}).get("coverage_rate") or 0)
    rates = {
        "quote_clean": quote_rate,
        "quote_executable": quote_exec_rate,
        "source_component": source_component_rate,
        "source_resonance_state": context_field_rate(context_report, "source_resonance_state"),
        "lifecycle": lifecycle_rate,
        "volume": volume_rate,
        "kline": kline_rate,
        "hard_gate_status": context_field_rate(context_report, "hard_gate_status"),
    }
    downstream_by_candidate = candidate_downstream_index(candidate_downstream)
    global_rates = downstream_global_rates(candidate_downstream)
    downstream_stage_sets = downstream_stage_signal_id_sets(candidate_downstream)
    slice_downstream_available = bool(downstream_stage_sets)
    valid = []
    invalid = []
    for row in capture.get("context_slices") or []:
        dim = row.get("dimension")
        dimension_eligibility = capture_cross_dimension_eligibility(dim, rates)
        reasons = list(dimension_eligibility["invalid_reasons"])
        item = {
            "candidate_id": row.get("candidate_id"),
            "family": row.get("family"),
            "dimension": dim,
            "dimension_group": dimension_eligibility["dimension_group"],
            "dimension_eligibility_status": dimension_eligibility["dimension_eligibility_status"],
            "slice_value": row.get("slice_value"),
            "judgment": row.get("judgment"),
            "matched_gold_silver_events": row.get("matched_gold_silver_events"),
            "match_recall_event": row.get("match_recall_event"),
            "match_precision_event": row.get("match_precision_event"),
            "recall_lift_vs_candidate_baseline": row.get("recall_lift_vs_candidate_baseline"),
            "precision_lift_vs_candidate_baseline": row.get("precision_lift_vs_candidate_baseline"),
            "valid": not reasons,
            "invalid_reasons": reasons,
            "data_blockers": reasons,
            "pnl_secondary_status": "secondary_pnl_report_required_not_used_for_capture_cross",
        }
        slice_downstream = (
            downstream_slice_for_signal_ids(
                row.get("matched_gold_silver_signal_ids"),
                downstream_stage_sets,
                global_rates,
            )
            if slice_downstream_available
            else downstream_proxy_for_candidate(row.get("candidate_id"), downstream_by_candidate, global_rates)
        )
        if (
            slice_downstream_available
            and not slice_downstream.get("downstream_lift_available")
        ):
            fallback = downstream_proxy_for_candidate(row.get("candidate_id"), downstream_by_candidate, global_rates)
            slice_downstream.update(
                {
                    "candidate_proxy_downstream_lift_scope": fallback.get("downstream_lift_scope"),
                    "candidate_proxy_decision_lift": fallback.get("decision_lift"),
                    "candidate_proxy_pass_allow_lift": fallback.get("pass_allow_lift"),
                    "candidate_proxy_pending_lift": fallback.get("pending_lift"),
                    "candidate_proxy_final_entry_lift": fallback.get("final_entry_lift"),
                    "candidate_proxy_mode_adjusted_final_eligibility_lift": fallback.get(
                        "mode_adjusted_final_eligibility_lift"
                    ),
                }
            )
        item.update(slice_downstream)
        (valid if not reasons else invalid).append(item)
    report = {
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
            "source_resonance_state_requires_present_rate_gte": 0.8,
            "lifecycle_requires_present_rate_gte": 0.8,
            "volume_sensitive_requires_present_rate_gte": 0.8,
            "kline_sensitive_requires_coverage_rate_gte": 0.8,
            "hard_gate_status_requires_present_rate_gte": 0.8,
            "unregistered_dimensions_are_invalid": True,
            "core_metadata_dimensions_allowed_without_context_coverage": sorted(CORE_METADATA_CROSS_DIMS),
            "pnl_is_secondary": True,
            "downstream_lift_scope": (
                "slice_level_matched_gold_silver_signal_id"
                if slice_downstream_available
                else "candidate_level_after_match_proxy_not_slice_specific"
            ),
            "slice_level_downstream_join_required_for_promotion_evidence": not slice_downstream_available,
        },
        "dimension_registry": {
            "quote-sensitive": sorted(QUOTE_SENSITIVE_CROSS_DIMS),
            "source_component": ["source_component"],
            "source_resonance_state": ["source_resonance_state"],
            "lifecycle": ["lifecycle_profile", "lifecycle_state"],
            "volume": ["volume_profile"],
            "kline": sorted(KLINE_CROSS_DIMS),
            "hard_gate": ["hard_gate_status"],
            "Markov": ["markov_bucket"],
            "core_metadata": sorted(CORE_METADATA_CROSS_DIMS),
        },
        "dimension_group_counts": dict(Counter(row["dimension_group"] for row in valid)),
        "invalid_dimension_group_counts": dict(Counter(row["dimension_group"] for row in invalid)),
        "dimension_status_counts": dict(Counter(row["dimension_eligibility_status"] for row in valid + invalid)),
        "downstream_global_rates": global_rates,
        "notes": [
            (
                "Downstream lifts are slice-specific matched gold/silver signal_id overlays."
                if slice_downstream_available
                else "Downstream lifts in this report are candidate-level after-match proxies attached to each slice."
            ),
            (
                "Slice-level downstream joins are available for discovery ranking; promotion still requires clean OOS validation."
                if slice_downstream_available
                else "They are useful for discovery ranking but are not slice-specific promotion evidence until a raw observation to decision join is added."
            ),
            "valid_top_crosses only includes registered dimensions with clean coverage or explicitly allowed core metadata dimensions.",
            "promotion_allowed remains false.",
        ],
    }
    apply_capture_cross_verdict(report)
    return report


def apply_capture_cross_verdict(report):
    valid = [row for row in report.get("valid_top_crosses") or [] if isinstance(row, dict)]
    invalid_count = int(report.get("invalid_cross_count") or 0)
    valid_count = int(report.get("valid_cross_count") or len(valid))
    judgment_counts = Counter(str(row.get("judgment") or "UNKNOWN") for row in valid)
    hit_count = sum(
        1
        for row in valid
        if str(row.get("judgment") or "").upper() in {"DISCOVERY_HIT", "HIT", "CAPTURE_DISCOVERY_HIT"}
    )
    watch_count = sum(
        1
        for row in valid
        if str(row.get("judgment") or "").upper() in {"WATCH", "DISCOVERY_WATCH"}
    )
    if hit_count:
        classification = "CAPTURE_CROSS_DISCOVERY_HIT_PENDING_OOS"
        next_action = "freeze_clean_capture_cross_definitions_for_next_window_oos"
    elif valid_count:
        classification = "CAPTURE_CROSS_DISCOVERY_WATCH"
        next_action = "track_valid_capture_crosses_in_clean_non_overlapping_oos"
    elif invalid_count:
        classification = "CAPTURE_CROSS_BLOCKED_CONTEXT_COVERAGE"
        next_action = "wait_for_context_clean_window_before_capture_cross_oos"
    else:
        classification = "CAPTURE_CROSS_NO_VALID_SIGNAL"
        next_action = "continue_capture_discovery_until_clean_cross_signal"
    report["classification"] = classification
    report["next_action"] = next_action
    report["evidence_level"] = report.get("evidence_level") or "discovery_same_window"
    report["valid_cross_judgment_counts"] = dict(judgment_counts)
    report["discovery_hit_count"] = hit_count
    report["watch_count"] = watch_count
    report["same_window_discovery_only"] = True
    report["oos_required_before_promotion"] = True
    report["can_promote_live"] = False
    report["promotion_allowed"] = False
    report["strategy_change_allowed"] = False
    report["automatic_runtime_change_allowed"] = False
    report["paper_enablement_allowed"] = False
    return report


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
        "autoloop_execution_timeout_partial_sync",
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


def diagnostic_is_timeout_like(row):
    text = " ".join(
        str(row.get(key) or "")
        for key in ("error", "stdout_tail", "stderr_tail", "name")
    ).lower()
    return any(
        marker in text
        for marker in (
            "timeout_after_",
            "timed out",
            "timeout",
            "524",
            "gateway timeout",
        )
    )


def autoloop_timeout_status(diagnostics):
    failed = [row for row in diagnostics or [] if not row.get("ok")]
    timeout_like = [row for row in failed if diagnostic_is_timeout_like(row)]
    if not timeout_like:
        return None
    return {
        "classification": "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC",
        "status": "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC",
        "timeout_command_count": len(timeout_like),
        "failed_command_count": len(failed),
        "all_failed_commands_timeout_like": len(timeout_like) == len(failed),
        "timeout_commands": [
            {
                "name": row.get("name"),
                "duration_sec": row.get("duration_sec"),
                "error": row.get("error"),
                "returncode": row.get("returncode"),
            }
            for row in timeout_like[:20]
        ],
        "strategy_failure_inferred": False,
        "lightweight_artifact_reconciliation_required": True,
        "preserve_previous_valid_reviewer_verdict": True,
        "handoff_only_if_timeout_repeats": True,
        "next_action": "run_lightweight_artifact_reconciliation_and_preserve_previous_valid_verdict",
        "promotion_allowed": False,
    }


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
    kline_coverage_resolution_path = run_dir / f"kline_coverage_resolution_audit_{primary_hours}h.json"
    quality_timing_research_path = run_dir / f"quality_timing_reject_research_audit_{primary_hours}h.json"
    strategy_memory_ingestion_path = run_dir / "strategy_memory_ingestion_summary.json"
    strategy_memory_validation_path = run_dir / "strategy_memory_validation_24h.json"
    strategy_memory_filtered_winner_bridge_path = run_dir / "strategy_memory_filtered_winner_bridge.json"
    strategy_memory_exit_shadow_summary_path = run_dir / "strategy_memory_exit_shadow_summary.json"
    strategy_memory_delay_replay_summary_path = run_dir / "strategy_memory_delay_replay_summary.json"
    markov_paths = {
        profile: run_dir / f"candidate_virtual_markov_{profile}_{primary_hours}h.json"
        for profile in args.markov_profiles.split(",")
        if profile
    }
    diagnostics = []
    readiness_paths = {}
    try:
        build_strategy_memory_ingestion_summary(args, strategy_memory_ingestion_path)
        readiness_paths["strategy_memory_ingestion_summary"] = strategy_memory_ingestion_path
        diagnostics.append({
            "name": "strategy_memory_ingestion_summary",
            "ok": True,
            "duration_sec": None,
            "output": str(strategy_memory_ingestion_path),
        })
    except Exception as exc:
        diagnostics.append({
            "name": "strategy_memory_ingestion_summary",
            "ok": False,
            "error": repr(exc),
            "duration_sec": None,
        })
        capture = blocked_capture_report(
            "strategy_memory_artifact_schema_invalid",
            args.paper_db,
            args.raw_db,
            primary_hours,
            args.expected_candidates,
        )
        write_json(capture_path, capture)
        return {
            "capture_primary": capture_path,
            "pnl": None,
            "markov": {},
            "readiness": readiness_paths,
            "diagnostics": diagnostics,
        }
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
        capture_cross_validity = build_capture_cross_validity_report(capture, context_report, downstream)
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
        "kline_coverage_resolution_audit",
        [
            "scripts/kline_coverage_resolution_audit.py",
            "--volume-kline-audit", str(volume_kline_audit_path),
            "--low-confidence-audit", str(low_confidence_research_path),
            "--matured-kline-recheck", str(matured_kline_recheck_path),
            "--matured-volume-cross", str(matured_volume_capture_cross_path),
            "--out", str(kline_coverage_resolution_path),
        ],
        kline_coverage_resolution_path,
        timeout=args.report_timeout_sec,
    ))
    if kline_coverage_resolution_path.exists():
        readiness_paths["kline_coverage_resolution_audit"] = kline_coverage_resolution_path
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
    diagnostics.append(run_report(
        "strategy_memory_validation",
        [
            "scripts/strategy_memory_validation.py",
            "--registry", str(args.registry),
            "--ingestion-summary", str(strategy_memory_ingestion_path),
            "--capture-24h", str(capture_paths.get(24, capture_path)),
            "--capture-48h", str(capture_paths.get(48, capture_paths.get(primary_hours, capture_path))),
            "--capture-72h", str(capture_paths.get(72, capture_paths.get(primary_hours, capture_path))),
            "--downstream", str(candidate_downstream_path),
            "--a-class", str(a_class_fastlane_path),
            "--pnl", str(pnl_path),
            "--filtered-winner", str(Path(args.data_dir) / "filtered_winner_dossier_24h.json"),
            "--exit-report", str(Path(args.data_dir) / "exit_policy_shadow_simulator_24h.json"),
            "--delay-report", str(Path(args.data_dir) / "execution_delay_adjusted_replay_24h.json"),
            "--out", str(strategy_memory_validation_path),
            "--filtered-bridge-out", str(strategy_memory_filtered_winner_bridge_path),
            "--exit-summary-out", str(strategy_memory_exit_shadow_summary_path),
            "--delay-summary-out", str(strategy_memory_delay_replay_summary_path),
        ],
        strategy_memory_validation_path,
        timeout=args.report_timeout_sec,
    ))
    if strategy_memory_validation_path.exists():
        readiness_paths["strategy_memory_validation"] = strategy_memory_validation_path
    if strategy_memory_filtered_winner_bridge_path.exists():
        readiness_paths["strategy_memory_filtered_winner_bridge"] = strategy_memory_filtered_winner_bridge_path
    if strategy_memory_exit_shadow_summary_path.exists():
        readiness_paths["strategy_memory_exit_shadow_summary"] = strategy_memory_exit_shadow_summary_path
    if strategy_memory_delay_replay_summary_path.exists():
        readiness_paths["strategy_memory_delay_replay_summary"] = strategy_memory_delay_replay_summary_path
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
        "top_clean_candidates": (row.get("top_clean_candidates") or [])[:10],
        "top_blocked_candidates": (row.get("top_blocked_candidates") or [])[:10],
        "top_families": (row.get("top_families") or [])[:10],
        "top_clean_families": (row.get("top_clean_families") or [])[:10],
        "top_blocked_families": (row.get("top_blocked_families") or [])[:10],
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
    if row.get("blocked_context_dimensions"):
        return False
    if candidate_id in {"current_all", "current_would_enter_all"}:
        return False
    if family == "runtime" or candidate_id.startswith("runtime:"):
        return False
    candidate_text = candidate_id.lower()
    family_text = family.lower()
    if family_text == "kline" or candidate_text.startswith("kline:"):
        return False
    if family_text == "volume" or any(
        marker in candidate_text
        for marker in ("volume", "lowvol", "low_vol", "vol_", "_vol")
    ):
        return False
    return True


def quality_timing_candidate_source_rows(cluster_row):
    if not isinstance(cluster_row, dict):
        return []
    return cluster_row.get("top_clean_candidates") or cluster_row.get("top_candidates") or []


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
        for candidate_row in quality_timing_candidate_source_rows(cluster_row):
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
        for candidate_row in quality_timing_candidate_source_rows(cluster_row):
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
            "quality_timing_cluster": cluster,
            "candidate_id": candidate_id,
            "candidate_family": definition.get("candidate_family"),
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
            "next_action": (
                "evaluate_probe_in_next_clean_non_overlapping_window"
                if status == "REPEATED_SHADOW_PROBE"
                else "continue_shadow_only_tracking_until_clean_window_then_oos_if_repeated"
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
    oos_queue_items = []
    for row in repeated_rows:
        definition = row.get("definition") or {}
        current_window = row.get("current_window") or {}
        oos_queue_items.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "status": "PENDING_CLEAN_WINDOW_THEN_OOS",
            "scope": "shadow_only_quality_timing_candidate_probe",
            "quality_timing_cluster": definition.get("quality_timing_cluster"),
            "candidate_id": definition.get("candidate_id"),
            "candidate_family": definition.get("candidate_family"),
            "current_window": {
                "cluster_event_count": current_window.get("cluster_event_count"),
                "cluster_unique_tokens": current_window.get("cluster_unique_tokens"),
                "cluster_share_of_quality_timing_rejects": (
                    current_window.get("cluster_share_of_quality_timing_rejects")
                ),
                "cluster_share_of_raw_all_gold_silver": (
                    current_window.get("cluster_share_of_raw_all_gold_silver")
                ),
                "candidate_cluster_match_count": (
                    current_window.get("candidate_cluster_match_count")
                ),
                "candidate_probe_rank_in_cluster": (
                    current_window.get("candidate_probe_rank_in_cluster")
                ),
                "max_sustained_peak_pct_max": current_window.get("max_sustained_peak_pct_max"),
                "time_to_sustained_peak_sec_median": (
                    current_window.get("time_to_sustained_peak_sec_median")
                ),
            },
            "readiness_gates": {
                "same_window_repeated": True,
                "context_clean_window_required": True,
                "non_overlapping_oos_required": True,
                "human_approval_required_before_promotion": True,
            },
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "next_action": "evaluate_probe_in_next_clean_non_overlapping_window",
        })
    oos_readiness_queue = {
        "classification": (
            "QUALITY_TIMING_OOS_QUEUE_PENDING_CLEAN_WINDOW"
            if oos_queue_items
            else "QUALITY_TIMING_OOS_QUEUE_EMPTY"
        ),
        "queue_count": len(oos_queue_items),
        "pending_clean_window_count": len(oos_queue_items),
        "ready_for_runtime_change_count": 0,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "items": oos_queue_items[:12],
        "notes": [
            "Queue is read-only and shadow-only.",
            "Items may only move to OOS judgment after context clean windows pass.",
            "This queue does not authorize strategy, gate, A_CLASS, executor, paper, or risk changes.",
        ],
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
        "oos_readiness_queue_count": oos_readiness_queue["queue_count"],
        "oos_readiness_queue": oos_readiness_queue,
        "top_repeated_probes": repeated_rows[:12],
        "probe_validations": rows,
        "notes": [
            "Read-only validation of shadow-only quality/timing candidate probes.",
            "Repeated same-window probes are discovery evidence only; OOS validation and human approval are still required before any promotion.",
        ],
    }


def build_decision_no_pass_quality_timing_watch_validation(registry, decision_no_pass_review):
    """Validate registered decision-no-pass timing clusters against current v3 review.

    This sits one layer above candidate-level quality/timing probes. It tracks
    whether the pass/allow gap clusters selected by ``capture_60_target_loop``
    remain visible in the latest same-window report. Repeated clusters can be
    held for clean-window/OOS review, but this artifact never authorizes
    strategy, policy, gate, A_CLASS, executor, paper, or risk changes.
    """
    registry = registry or {}
    decision_no_pass_review = decision_no_pass_review or {}
    watches = list(registry.get("shadow_only_decision_no_pass_quality_timing_watch") or [])
    current_clusters = {
        row.get("cluster"): row
        for row in (decision_no_pass_review.get("clusters") or [])
        if isinstance(row, dict) and row.get("cluster")
    }
    selected_clusters = {
        row.get("cluster"): row
        for row in (
            decision_no_pass_review.get(
                "selected_clusters_to_cover_current_pass_allow_gap_upper_bound"
            )
            or []
        )
        if isinstance(row, dict) and row.get("cluster")
    }

    rows = []
    for watch in watches:
        definition = watch.get("definition") or {}
        cluster = definition.get("cluster")
        current = current_clusters.get(cluster) or {}
        selected = selected_clusters.get(cluster) or {}
        status = "NOT_OBSERVED_CURRENT_WINDOW"
        if selected:
            status = "REPEATED_SELECTED_CLUSTER"
        elif current:
            status = "CLUSTER_REPEATED_NOT_SELECTED"
        rows.append({
            "hypothesis_id": watch.get("hypothesis_id"),
            "cluster": cluster,
            "stage": definition.get("stage") or "decision_no_pass_or_allow",
            "status": status,
            "scope": "shadow_only_decision_no_pass_quality_timing_cluster",
            "evidence_level": "discovery_same_window_cluster_validation",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "definition": {
                "cluster": cluster,
                "stage": definition.get("stage") or "decision_no_pass_or_allow",
                "suggested_shadow_only_action": definition.get("suggested_shadow_only_action"),
            },
            "current_window": {
                "cluster_repeated": bool(current),
                "cluster_selected_for_pass_allow_gap": bool(selected),
                "decision_no_pass_event_count": current.get("decision_no_pass_event_count"),
                "events_contributing_to_gap_upper_bound": (
                    selected.get("events_contributing_to_gap_upper_bound")
                ),
                "cumulative_events_contributing_to_gap_upper_bound": (
                    selected.get("cumulative_events_contributing_to_gap_upper_bound")
                ),
                "share_of_raw_gold_silver": current.get("share_of_raw_gold_silver"),
                "share_of_current_pass_allow_gap_upper_bound": (
                    current.get("share_of_current_pass_allow_gap_upper_bound")
                ),
                "unique_tokens": current.get("unique_tokens"),
                "candidate_matched_any_rate": current.get("candidate_matched_any_rate"),
                "max_sustained_peak_pct_max": current.get("max_sustained_peak_pct_max"),
                "time_to_sustained_peak_sec_median": (
                    current.get("time_to_sustained_peak_sec_median")
                ),
                "context_blockers": current.get("context_blockers") or [],
            },
            "next_validation": (
                "continue_shadow_only_cluster_tracking_until_clean_window_then_oos"
            ),
            "next_action": (
                "evaluate_cluster_in_next_clean_non_overlapping_window"
                if status == "REPEATED_SELECTED_CLUSTER"
                else "continue_shadow_only_cluster_tracking_until_clean_window_then_oos_if_repeated"
            ),
        })

    status_counts = Counter(row.get("status") for row in rows)
    repeated_rows = [
        row for row in rows
        if row.get("status") == "REPEATED_SELECTED_CLUSTER"
    ]
    shifted_rows = [
        row for row in rows
        if row.get("status") == "CLUSTER_REPEATED_NOT_SELECTED"
    ]
    repeated_rows = sorted(
        repeated_rows,
        key=lambda row: (
            safe_int((row.get("current_window") or {}).get("decision_no_pass_event_count"), 0),
            safe_int((row.get("current_window") or {}).get("unique_tokens"), 0),
        ),
        reverse=True,
    )
    if not watches:
        classification = "NO_REGISTERED_DECISION_NO_PASS_QUALITY_TIMING_WATCH"
        next_action = "register_selected_decision_no_pass_quality_timing_clusters_from_current_review"
    elif repeated_rows:
        classification = "DECISION_NO_PASS_QUALITY_TIMING_WATCH_REPEATED_SAME_WINDOW"
        next_action = "continue_cluster_tracking_until_clean_window_then_oos"
    elif shifted_rows:
        classification = "DECISION_NO_PASS_QUALITY_TIMING_CLUSTERS_REPEATED_NOT_SELECTED"
        next_action = "refresh_selected_cluster_watch_if_pass_allow_gap_shifted"
    else:
        classification = "DECISION_NO_PASS_QUALITY_TIMING_WATCH_NOT_REPEATED_CURRENT_WINDOW"
        next_action = "continue_monitoring_registered_decision_no_pass_quality_timing_watch"

    denominator = {
        "registered_watch_count": len(watches),
        "current_cluster_count": len(current_clusters),
        "current_selected_cluster_count": len(selected_clusters),
        "validated_watch_count": len(rows),
        "repeated_selected_cluster_count": status_counts.get("REPEATED_SELECTED_CLUSTER", 0),
        "cluster_repeated_not_selected_count": status_counts.get(
            "CLUSTER_REPEATED_NOT_SELECTED",
            0,
        ),
        "not_observed_current_window_count": status_counts.get(
            "NOT_OBSERVED_CURRENT_WINDOW",
            0,
        ),
        "repeated_selected_cluster_rate": safe_rate(
            status_counts.get("REPEATED_SELECTED_CLUSTER", 0),
            len(watches),
        ),
    }
    oos_items = []
    for row in repeated_rows:
        definition = row.get("definition") or {}
        current_window = row.get("current_window") or {}
        oos_items.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "status": "PENDING_CLEAN_WINDOW_THEN_OOS",
            "scope": "shadow_only_decision_no_pass_quality_timing_cluster",
            "cluster": definition.get("cluster"),
            "stage": definition.get("stage"),
            "current_window": {
                "decision_no_pass_event_count": (
                    current_window.get("decision_no_pass_event_count")
                ),
                "events_contributing_to_gap_upper_bound": (
                    current_window.get("events_contributing_to_gap_upper_bound")
                ),
                "share_of_current_pass_allow_gap_upper_bound": (
                    current_window.get("share_of_current_pass_allow_gap_upper_bound")
                ),
                "unique_tokens": current_window.get("unique_tokens"),
                "candidate_matched_any_rate": current_window.get("candidate_matched_any_rate"),
                "max_sustained_peak_pct_max": current_window.get("max_sustained_peak_pct_max"),
                "time_to_sustained_peak_sec_median": (
                    current_window.get("time_to_sustained_peak_sec_median")
                ),
                "context_blockers": current_window.get("context_blockers") or [],
            },
            "readiness_gates": {
                "same_window_repeated": True,
                "context_clean_window_required": True,
                "non_overlapping_oos_required": True,
                "human_approval_required_before_promotion": True,
            },
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "next_action": "evaluate_cluster_in_next_clean_non_overlapping_window",
        })

    return {
        "schema_version": "decision_no_pass_quality_timing_watch_validation.v1",
        "report_type": "decision_no_pass_quality_timing_watch_validation_24h",
        "generated_at": utc_now(),
        "classification": classification,
        "next_action": next_action,
        "evidence_level": "discovery_same_window_cluster_validation",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "registered_watch_count": denominator["registered_watch_count"],
        "current_cluster_count": denominator["current_cluster_count"],
        "current_selected_cluster_count": denominator["current_selected_cluster_count"],
        "validated_watch_count": denominator["validated_watch_count"],
        "repeated_selected_cluster_count": denominator["repeated_selected_cluster_count"],
        "repeated_selected_cluster_rate": denominator["repeated_selected_cluster_rate"],
        "denominator": denominator,
        "status_counts": dict(status_counts),
        "oos_readiness_queue": {
            "classification": (
                "DECISION_NO_PASS_QUALITY_TIMING_OOS_QUEUE_PENDING_CLEAN_WINDOW"
                if oos_items
                else "DECISION_NO_PASS_QUALITY_TIMING_OOS_QUEUE_EMPTY"
            ),
            "queue_count": len(oos_items),
            "pending_clean_window_count": len(oos_items),
            "ready_for_runtime_change_count": 0,
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "items": oos_items[:12],
            "notes": [
                "Queue is read-only and shadow-only.",
                "Repeated decision-no-pass clusters are pass/allow gap evidence, not promotion evidence.",
                "This queue does not authorize strategy, gate, A_CLASS, executor, paper, or risk changes.",
            ],
        },
        "top_repeated_clusters": repeated_rows[:12],
        "watch_validations": rows,
        "notes": [
            "Read-only validation of selected pass/allow gap quality/timing clusters.",
            "Repeated same-window clusters must wait for clean-window and non-overlapping OOS validation.",
        ],
    }


def compact_pending_momentum_decay_probe(row):
    probe_id = row.get("probe_id")
    return {
        "hypothesis_id": f"pending_momentum_decay_probe:{hypothesis_id_part(probe_id)}",
        "evidence_level": "discovery_same_window",
        "scope": "shadow_only_pending_momentum_decay_recheck_probe",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "definition": {
            "probe_id": probe_id,
            "cluster": ((row.get("evidence") or {}).get("cluster") or "momentum_fading_or_negative_trend"),
            "expected_capture_stage_improved": row.get("expected_capture_stage_improved"),
            "required_features": row.get("required_features") or [],
        },
        "latest_metrics": {
            "event_count": (row.get("evidence") or {}).get("event_count"),
            "time_to_sustained_peak_sec_median": (
                (row.get("evidence") or {}).get("time_to_sustained_peak_sec_median")
            ),
            "recheck_window_classification": (
                (row.get("evidence") or {}).get("recheck_window_classification")
            ),
        },
        "human_approval_required_if_fix_requires": row.get("human_approval_required_if_fix_requires"),
        "next_validation": row.get("next_action")
        or "validate_pending_momentum_decay_recheck_window_shadow_only",
    }


def build_pending_momentum_decay_probe_validation(registry, pending_to_final_audit):
    """Validate registered pending momentum decay probes against current v3 audit.

    This remains shadow-only timing attribution. A repeated recheck window can
    feed OOS readiness, but it must not authorize threshold, policy, gate,
    A_CLASS, executor, paper, or risk changes.
    """
    registry = registry or {}
    pending_to_final_audit = pending_to_final_audit or {}
    probes = list(registry.get("shadow_only_pending_momentum_decay_probes") or [])
    stale_review = pending_to_final_audit.get("stale_before_final_review") or {}
    momentum = stale_review.get("momentum_decay_review") or {}
    current_probes = {
        row.get("probe_id"): row
        for row in (momentum.get("selected_shadow_probes") or [])
        if isinstance(row, dict) and row.get("probe_id")
    }
    cluster_repeated = safe_int(momentum.get("event_count"), 0) > 0

    rows = []
    for probe in probes:
        definition = probe.get("definition") or {}
        probe_id = definition.get("probe_id")
        current = current_probes.get(probe_id) or {}
        status = "NOT_OBSERVED_CURRENT_WINDOW"
        if current:
            status = "REPEATED_SHADOW_PROBE"
        elif cluster_repeated:
            status = "CLUSTER_REPEATED_PROBE_NOT_SELECTED"
        rows.append({
            "hypothesis_id": probe.get("hypothesis_id"),
            "status": status,
            "scope": "shadow_only_pending_momentum_decay_recheck_probe",
            "evidence_level": "discovery_same_window_probe_validation",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "definition": {
                "probe_id": probe_id,
                "cluster": definition.get("cluster"),
                "expected_capture_stage_improved": definition.get("expected_capture_stage_improved"),
                "required_features": definition.get("required_features") or [],
            },
            "current_window": {
                "cluster_repeated": cluster_repeated,
                "probe_repeated": bool(current),
                "event_count": momentum.get("event_count"),
                "unique_tokens": momentum.get("unique_tokens"),
                "share_of_raw_all_gold_silver": momentum.get("share_of_raw_all_gold_silver"),
                "share_of_quality_timing_rejects": momentum.get("share_of_quality_timing_rejects"),
                "time_to_sustained_peak_sec_median": momentum.get("time_to_sustained_peak_sec_median"),
                "max_sustained_peak_pct_max": momentum.get("max_sustained_peak_pct_max"),
                "recheck_window_classification": momentum.get("recheck_window_classification"),
                "example_peak_lag_band_counts": momentum.get("example_peak_lag_band_counts") or {},
                "context_blockers": momentum.get("context_blockers") or [],
            },
            "next_validation": "continue_shadow_only_recheck_tracking_until_clean_window_then_oos",
        })

    status_counts = Counter(row.get("status") for row in rows)
    repeated_rows = [row for row in rows if row.get("status") == "REPEATED_SHADOW_PROBE"]
    if not probes:
        classification = "NO_REGISTERED_PENDING_MOMENTUM_DECAY_PROBES"
        next_action = "register_pending_momentum_decay_shadow_probes_from_current_audit"
    elif repeated_rows:
        classification = "PENDING_MOMENTUM_DECAY_PROBES_REPEATED_SAME_WINDOW"
        next_action = "continue_shadow_probe_tracking_until_clean_window_then_oos"
    elif cluster_repeated:
        classification = "PENDING_MOMENTUM_DECAY_CLUSTER_REPEATED_PROBES_SHIFTED"
        next_action = "refresh_pending_momentum_decay_shadow_probes_from_current_audit"
    else:
        classification = "PENDING_MOMENTUM_DECAY_PROBES_NOT_REPEATED_CURRENT_WINDOW"
        next_action = "continue_monitoring_pending_momentum_decay_shadow_probes"

    denominator = {
        "registered_probe_count": len(probes),
        "current_cluster_event_count": safe_int(momentum.get("event_count"), 0),
        "current_cluster_unique_tokens": safe_int(momentum.get("unique_tokens"), 0),
        "current_selected_probe_count": len(current_probes),
        "validated_probe_count": len(rows),
        "repeated_probe_count": status_counts.get("REPEATED_SHADOW_PROBE", 0),
        "cluster_repeated_probe_not_selected_count": status_counts.get(
            "CLUSTER_REPEATED_PROBE_NOT_SELECTED",
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
    oos_items = []
    for row in repeated_rows:
        current_window = row.get("current_window") or {}
        oos_items.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "status": "PENDING_CLEAN_WINDOW_THEN_OOS",
            "scope": "shadow_only_pending_momentum_decay_recheck_probe",
            "probe_id": (row.get("definition") or {}).get("probe_id"),
            "cluster": (row.get("definition") or {}).get("cluster"),
            "current_window": {
                "event_count": current_window.get("event_count"),
                "unique_tokens": current_window.get("unique_tokens"),
                "time_to_sustained_peak_sec_median": (
                    current_window.get("time_to_sustained_peak_sec_median")
                ),
                "recheck_window_classification": (
                    current_window.get("recheck_window_classification")
                ),
                "share_of_raw_all_gold_silver": (
                    current_window.get("share_of_raw_all_gold_silver")
                ),
                "share_of_quality_timing_rejects": (
                    current_window.get("share_of_quality_timing_rejects")
                ),
            },
            "readiness_gates": {
                "same_window_repeated": True,
                "context_clean_window_required": True,
                "non_overlapping_oos_required": True,
                "human_approval_required_before_promotion": True,
            },
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "next_action": "evaluate_recheck_probe_in_next_clean_non_overlapping_window",
        })

    return {
        "schema_version": "pending_momentum_decay_probe_validation.v1",
        "report_type": "pending_momentum_decay_recheck_validation_24h",
        "generated_at": utc_now(),
        "classification": classification,
        "next_action": next_action,
        "evidence_level": "discovery_same_window_probe_validation",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "registered_probe_count": denominator["registered_probe_count"],
        "current_cluster_event_count": denominator["current_cluster_event_count"],
        "current_cluster_unique_tokens": denominator["current_cluster_unique_tokens"],
        "validated_probe_count": denominator["validated_probe_count"],
        "repeated_probe_count": denominator["repeated_probe_count"],
        "repeated_probe_rate": denominator["repeated_probe_rate"],
        "denominator": denominator,
        "status_counts": dict(status_counts),
        "current_momentum_decay_review": {
            key: momentum.get(key)
            for key in (
                "event_count",
                "unique_tokens",
                "share_of_raw_all_gold_silver",
                "share_of_quality_timing_rejects",
                "time_to_sustained_peak_sec_median",
                "max_sustained_peak_pct_max",
                "recheck_window_classification",
                "next_action",
            )
        },
        "oos_readiness_queue": {
            "classification": (
                "PENDING_MOMENTUM_DECAY_OOS_QUEUE_PENDING_CLEAN_WINDOW"
                if oos_items
                else "PENDING_MOMENTUM_DECAY_OOS_QUEUE_EMPTY"
            ),
            "queue_count": len(oos_items),
            "pending_clean_window_count": len(oos_items),
            "ready_for_runtime_change_count": 0,
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "items": oos_items[:12],
            "notes": [
                "Queue is read-only and shadow-only.",
                "Repeated probes only mean the same timing pattern is still visible.",
                "This queue does not authorize threshold, gate, A_CLASS, executor, paper, or risk changes.",
            ],
        },
        "top_repeated_probes": repeated_rows[:12],
        "probe_validations": rows,
        "notes": [
            "Read-only validation of pending momentum decay recheck probes.",
            "Same-window repeat is discovery evidence only; OOS validation and human approval are required before any promotion.",
        ],
    }


def compact_pending_stale_before_final_cluster(row):
    cluster = row.get("cluster")
    return {
        "hypothesis_id": f"pending_stale_before_final:{hypothesis_id_part(cluster)}",
        "evidence_level": "discovery_same_window",
        "scope": "shadow_only_pending_stale_before_final_cluster",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "definition": {
            "cluster": cluster,
            "expected_capture_stage_improved": "final_eligibility",
            "required_features": [
                "pending_entry_ts",
                "quality_timing_cluster",
                "decision_reason",
                "final_entry_contract_record",
            ],
        },
        "latest_metrics": {
            "event_count": row.get("event_count"),
            "events_contributing_to_stale_before_final_upper_bound": row.get(
                "events_contributing_to_stale_before_final_upper_bound"
            ),
            "cumulative_events_contributing_to_stale_before_final_upper_bound": row.get(
                "cumulative_events_contributing_to_stale_before_final_upper_bound"
            ),
            "context_blockers": row.get("context_blockers") or [],
        },
        "human_approval_required_if_fix_requires": row.get(
            "human_approval_required_if_fix_requires"
        ),
        "next_validation": row.get("next_action")
        or "continue_shadow_only_pending_stale_cluster_tracking_until_clean_window_then_oos",
    }


def build_pending_stale_before_final_watch_validation(registry, pending_to_final_audit):
    """Validate all registered stale-before-final clusters against the current audit.

    This is broader than the momentum-decay probe validation: it keeps
    chasing-top, signal-stale, momentum-decay, and other stale timing clusters
    visible for OOS readiness without authorizing any runtime or policy change.
    """
    registry = registry or {}
    pending_to_final_audit = pending_to_final_audit or {}
    watches = list(registry.get("shadow_only_pending_stale_before_final_watch") or [])
    stale_review = pending_to_final_audit.get("stale_before_final_review") or {}
    current_by_cluster = {
        row.get("cluster"): row
        for row in (stale_review.get("selected_clusters_to_cover_stale_before_final_upper_bound") or [])
        if isinstance(row, dict) and row.get("cluster")
    }
    stale_event_count = safe_int(stale_review.get("stale_before_final_event_count"), 0)

    rows = []
    for watch in watches:
        definition = watch.get("definition") or {}
        cluster = definition.get("cluster")
        current = current_by_cluster.get(cluster) or {}
        if current:
            status = "REPEATED_SELECTED_STALE_CLUSTER"
        elif stale_event_count > 0:
            status = "STALE_EVENT_PRESENT_CLUSTER_NOT_SELECTED"
        else:
            status = "NOT_OBSERVED_CURRENT_WINDOW"
        rows.append({
            "hypothesis_id": watch.get("hypothesis_id"),
            "status": status,
            "scope": "shadow_only_pending_stale_before_final_cluster",
            "evidence_level": "discovery_same_window_cluster_validation",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "definition": {
                "cluster": cluster,
                "expected_capture_stage_improved": definition.get(
                    "expected_capture_stage_improved"
                ),
                "required_features": definition.get("required_features") or [],
            },
            "current_window": {
                "cluster_repeated": bool(current),
                "stale_before_final_event_count": stale_event_count,
                "pending_without_final_entry_contract": stale_review.get(
                    "pending_without_final_entry_contract"
                ),
                "adjacent_count_loss_pending_to_final": stale_review.get(
                    "adjacent_count_loss_pending_to_final"
                ),
                "event_count": current.get("event_count"),
                "events_contributing_to_stale_before_final_upper_bound": current.get(
                    "events_contributing_to_stale_before_final_upper_bound"
                ),
                "cumulative_events_contributing_to_stale_before_final_upper_bound": current.get(
                    "cumulative_events_contributing_to_stale_before_final_upper_bound"
                ),
                "context_blockers": current.get("context_blockers") or [],
            },
            "next_validation": (
                "continue_shadow_only_pending_stale_cluster_tracking_until_clean_window_then_oos"
            ),
        })

    status_counts = Counter(row.get("status") for row in rows)
    repeated_rows = [
        row for row in rows
        if row.get("status") == "REPEATED_SELECTED_STALE_CLUSTER"
    ]
    if not watches:
        classification = "NO_REGISTERED_PENDING_STALE_BEFORE_FINAL_WATCH"
        next_action = "register_pending_stale_before_final_clusters_from_current_audit"
    elif repeated_rows:
        classification = "PENDING_STALE_BEFORE_FINAL_WATCH_REPEATED_SAME_WINDOW"
        next_action = "continue_stale_cluster_tracking_until_clean_window_then_oos"
    elif stale_event_count > 0:
        classification = "PENDING_STALE_BEFORE_FINAL_EVENTS_REPEATED_WATCH_SHIFTED"
        next_action = "refresh_pending_stale_before_final_watch_from_current_audit"
    else:
        classification = "PENDING_STALE_BEFORE_FINAL_WATCH_NOT_REPEATED_CURRENT_WINDOW"
        next_action = "continue_monitoring_pending_stale_before_final_watch"

    denominator = {
        "registered_watch_count": len(watches),
        "current_stale_before_final_event_count": stale_event_count,
        "current_selected_cluster_count": len(current_by_cluster),
        "validated_watch_count": len(rows),
        "repeated_selected_cluster_count": status_counts.get(
            "REPEATED_SELECTED_STALE_CLUSTER",
            0,
        ),
        "stale_event_present_cluster_not_selected_count": status_counts.get(
            "STALE_EVENT_PRESENT_CLUSTER_NOT_SELECTED",
            0,
        ),
        "not_observed_current_window_count": status_counts.get(
            "NOT_OBSERVED_CURRENT_WINDOW",
            0,
        ),
        "repeated_selected_cluster_rate": safe_rate(
            status_counts.get("REPEATED_SELECTED_STALE_CLUSTER", 0),
            len(watches),
        ),
    }

    oos_items = []
    for row in repeated_rows:
        current_window = row.get("current_window") or {}
        definition = row.get("definition") or {}
        oos_items.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "status": "PENDING_CLEAN_WINDOW_THEN_OOS",
            "scope": "shadow_only_pending_stale_before_final_cluster",
            "cluster": definition.get("cluster"),
            "current_window": {
                "event_count": current_window.get("event_count"),
                "stale_before_final_event_count": current_window.get(
                    "stale_before_final_event_count"
                ),
                "events_contributing_to_stale_before_final_upper_bound": current_window.get(
                    "events_contributing_to_stale_before_final_upper_bound"
                ),
                "context_blockers": current_window.get("context_blockers") or [],
            },
            "readiness_gates": {
                "same_window_repeated": True,
                "context_clean_window_required": True,
                "non_overlapping_oos_required": True,
                "human_approval_required_before_promotion": True,
            },
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "next_action": "evaluate_stale_cluster_in_next_clean_non_overlapping_window",
        })

    return {
        "schema_version": "pending_stale_before_final_watch_validation.v1",
        "report_type": "pending_stale_before_final_watch_validation_24h",
        "generated_at": utc_now(),
        "classification": classification,
        "next_action": next_action,
        "evidence_level": "discovery_same_window_cluster_validation",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "registered_watch_count": denominator["registered_watch_count"],
        "current_stale_before_final_event_count": denominator[
            "current_stale_before_final_event_count"
        ],
        "current_selected_cluster_count": denominator["current_selected_cluster_count"],
        "validated_watch_count": denominator["validated_watch_count"],
        "repeated_selected_cluster_count": denominator["repeated_selected_cluster_count"],
        "repeated_selected_cluster_rate": denominator["repeated_selected_cluster_rate"],
        "denominator": denominator,
        "status_counts": dict(status_counts),
        "oos_readiness_queue": {
            "classification": (
                "PENDING_STALE_BEFORE_FINAL_OOS_QUEUE_PENDING_CLEAN_WINDOW"
                if oos_items
                else "PENDING_STALE_BEFORE_FINAL_OOS_QUEUE_EMPTY"
            ),
            "queue_count": len(oos_items),
            "pending_clean_window_count": len(oos_items),
            "ready_for_runtime_change_count": 0,
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "items": oos_items[:12],
            "notes": [
                "Queue is read-only and shadow-only.",
                "Repeated stale-before-final clusters are final-eligibility gap evidence, not promotion evidence.",
                "This queue does not authorize timing threshold, gate, final_entry_contract, A_CLASS, executor, paper, or risk changes.",
            ],
        },
        "top_repeated_clusters": repeated_rows[:12],
        "watch_validations": rows,
        "notes": [
            "Read-only validation of stale-before-final timing clusters.",
            "Same-window repeat must wait for clean-window and non-overlapping OOS validation.",
        ],
    }


def stable_hypothesis_signature(
    *,
    watchlist_hypotheses,
    matured_volume_watch,
    quality_timing_watch=None,
    quality_timing_candidate_probes=None,
    decision_no_pass_quality_timing_watch=None,
    pending_momentum_decay_probes=None,
    pending_stale_before_final_watch=None,
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
    decision_no_pass_keys = []
    for row in decision_no_pass_quality_timing_watch or []:
        if not isinstance(row, dict):
            continue
        decision_no_pass_keys.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "definition": row.get("definition") or {},
        })
    decision_no_pass_keys = sorted(
        decision_no_pass_keys,
        key=lambda item: item.get("hypothesis_id") or "",
    )
    pending_momentum_keys = []
    for row in pending_momentum_decay_probes or []:
        if not isinstance(row, dict):
            continue
        pending_momentum_keys.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "definition": row.get("definition") or {},
        })
    pending_momentum_keys = sorted(
        pending_momentum_keys,
        key=lambda item: item.get("hypothesis_id") or "",
    )
    pending_stale_keys = []
    for row in pending_stale_before_final_watch or []:
        if not isinstance(row, dict):
            continue
        pending_stale_keys.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "definition": row.get("definition") or {},
        })
    pending_stale_keys = sorted(
        pending_stale_keys,
        key=lambda item: item.get("hypothesis_id") or "",
    )
    return {
        "watchlist_hypothesis_keys": sorted(str(item) for item in watchlist_keys),
        "shadow_only_matured_volume_watch": matured_keys,
        "shadow_only_quality_timing_watch": quality_timing_keys,
        "shadow_only_quality_timing_candidate_probes": quality_timing_probe_keys,
        "shadow_only_decision_no_pass_quality_timing_watch": decision_no_pass_keys,
        "shadow_only_pending_momentum_decay_probes": pending_momentum_keys,
        "shadow_only_pending_stale_before_final_watch": pending_stale_keys,
    }


def stable_oos_hypothesis_signature(*, matured_volume_watch):
    """Signature for hypotheses currently eligible for matured-volume OOS.

    The full hypothesis registry can change for unrelated reasons such as
    Strategy Memory metadata, quality/timing review queues, or recent run
    bookkeeping. Those updates must not reset the non-overlapping OOS clock for
    the frozen matured-volume hypotheses that ``hypothesis_validation_audit``
    evaluates.
    """
    matured_keys = []
    for row in matured_volume_watch or []:
        if not isinstance(row, dict):
            continue
        matured_keys.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "definition": row.get("definition") or {},
        })
    matured_keys = sorted(matured_keys, key=lambda item: item.get("hypothesis_id") or "")
    return {
        "shadow_only_matured_volume_watch": matured_keys,
    }


def compact_decision_no_pass_quality_timing_hypothesis(row):
    cluster = row.get("cluster")
    return {
        "hypothesis_id": f"decision_no_pass_quality_timing:{hypothesis_id_part(cluster)}",
        "evidence_level": "discovery_same_window",
        "scope": "shadow_only_decision_no_pass_quality_timing_cluster",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "definition": {
            "cluster": cluster,
            "stage": "decision_no_pass_or_allow",
            "suggested_shadow_only_action": row.get("suggested_shadow_only_action"),
        },
        "latest_metrics": {
            key: row.get(key)
            for key in (
                "decision_no_pass_event_count",
                "events_contributing_to_gap_upper_bound",
                "cumulative_events_contributing_to_gap_upper_bound",
                "share_of_raw_gold_silver",
                "share_of_current_pass_allow_gap_upper_bound",
                "unique_tokens",
                "candidate_matched_any_rate",
                "max_sustained_peak_pct_max",
                "time_to_sustained_peak_sec_median",
            )
        },
        "reason_counts": (row.get("reason_counts") or [])[:10],
        "top_candidates": (row.get("top_candidates") or [])[:10],
        "top_lifecycle_source_contexts": (row.get("top_lifecycle_source_contexts") or [])[:10],
        "human_approval_required_if_fix_requires": row.get("human_approval_required_if_fix_requires"),
        "context_blockers": row.get("context_blockers") or [],
        "next_validation": (
            "track_same_decision_no_pass_cluster_in_next_clean_window_then_oos_if_repeated"
        ),
    }


def compact_strategy_memory_registry(summary):
    summary = summary or {}
    hypotheses = []
    for row in summary.get("hypotheses") or []:
        if not isinstance(row, dict):
            continue
        hypotheses.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "name": row.get("name"),
            "strategy_family": row.get("strategy_family"),
            "priority": row.get("priority"),
            "allowed_use": "shadow_only",
            "promotion_allowed": False,
            "evidence_level": "historical_memory",
            "historical_pnl_is_promotion_evidence": False,
            "same_window_discovery_is_promotion_evidence": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "candidate_catalog_change_allowed": False,
            "mapping_status": row.get("mapping_status"),
            "mapped_existing_candidate_ids": row.get("mapped_existing_candidate_ids") or [],
            "blocked_contexts": row.get("blocked_contexts") or [],
            "rejected_future_data": bool(row.get("rejected_future_data")),
            "future_or_posthoc_features": row.get("future_or_posthoc_features") or [],
            "requires_paper_trades_db": bool(row.get("requires_paper_trades_db")),
            "evidence_incomplete": bool(row.get("evidence_incomplete")),
            "evidence_incomplete_reason": row.get("evidence_incomplete_reason"),
            "route": row.get("route"),
            "exit_only": bool(row.get("exit_only")),
            "delay_replay_only": bool(row.get("delay_replay_only")),
            "missing_shadow_candidate_handoff_required": bool(row.get("missing_shadow_candidate_handoff_required")),
            "next_validation_required": row.get("next_validation_required"),
        })
    return {
        "schema_version": "strategy_memory_registry_namespace.v1",
        "updated_at": utc_now(),
        "available": bool(summary.get("available")),
        "allowed_use": "shadow_only",
        "promotion_allowed": False,
        "evidence_level": "historical_memory",
        "candidate_catalog_change_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "summary": {
            key: summary.get(key)
            for key in (
                "strategy_memory_hypotheses_count",
                "mapped_to_existing_candidates",
                "missing_shadow_candidates",
                "rejected_future_data_hypotheses",
                "top_10_shadow_hypotheses",
                "filtered_winner_count",
                "exit_policy_variants_tested",
                "delay_replay_done",
                "paper_trades_db_available",
                "evidence_incomplete_hypotheses",
            )
        } | {
            "promotion_allowed": False,
            "allowed_use": "shadow_only",
            "evidence_level": "historical_memory",
        },
        "missing_shadow_candidate_handoffs": summary.get("missing_shadow_candidate_handoffs") or [],
        "exit_only_hypotheses": summary.get("exit_only_hypotheses") or [],
        "source_artifacts": summary.get("artifact_paths") or {},
        "hypotheses": hypotheses,
    }


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


def update_hypothesis_registry(path, verdict, capture, strategy_memory_summary=None):
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
    decision_no_pass_review = verdict.get("decision_no_pass_quality_timing_review") or {}
    selected_decision_no_pass_clusters = {
        row.get("cluster")
        for row in (
            decision_no_pass_review.get("selected_clusters_to_cover_current_pass_allow_gap_upper_bound")
            or []
        )
        if isinstance(row, dict) and row.get("cluster")
    }
    selected_decision_no_pass_rows = []
    if selected_decision_no_pass_clusters:
        top_by_cluster = {
            row.get("cluster"): row
            for row in (decision_no_pass_review.get("top_clusters") or [])
            if isinstance(row, dict) and row.get("cluster")
        }
        selected_by_cluster = {
            row.get("cluster"): row
            for row in (
                decision_no_pass_review.get("selected_clusters_to_cover_current_pass_allow_gap_upper_bound")
                or []
            )
            if isinstance(row, dict) and row.get("cluster")
        }
        for cluster in sorted(selected_decision_no_pass_clusters):
            merged = dict(top_by_cluster.get(cluster) or {})
            merged.update(selected_by_cluster.get(cluster) or {})
            selected_decision_no_pass_rows.append(merged)
    decision_no_pass_quality_timing_watch = [
        compact_decision_no_pass_quality_timing_hypothesis(row)
        for row in selected_decision_no_pass_rows
        if isinstance(row, dict) and row.get("cluster")
    ]
    previous_decision_no_pass_quality_timing_watch = [
        row
        for row in (registry.get("shadow_only_decision_no_pass_quality_timing_watch") or [])
        if isinstance(row, dict) and row.get("hypothesis_id")
    ]
    decision_review_empty_or_missing = not selected_decision_no_pass_rows and (
        not decision_no_pass_review
        or decision_no_pass_review.get("classification") == "DECISION_NO_PASS_QUALITY_TIMING_REVIEW_EMPTY"
        or verdict.get("autoloop_execution_status") == "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC"
    )
    if not decision_no_pass_quality_timing_watch and previous_decision_no_pass_quality_timing_watch and decision_review_empty_or_missing:
        decision_no_pass_quality_timing_watch = previous_decision_no_pass_quality_timing_watch
    pending_audit = (
        verdict.get("pending_to_final_entry_audit_v3")
        or verdict.get("pending_to_final_entry_audit")
        or {}
    )
    momentum_decay_review = (
        (pending_audit.get("stale_before_final_review") or {}).get("momentum_decay_review")
        or {}
    )
    pending_momentum_decay_probes = [
        compact_pending_momentum_decay_probe(row)
        for row in (momentum_decay_review.get("selected_shadow_probes") or [])
        if isinstance(row, dict) and row.get("probe_id")
    ]
    previous_pending_momentum_decay_probes = [
        row
        for row in (registry.get("shadow_only_pending_momentum_decay_probes") or [])
        if isinstance(row, dict) and row.get("hypothesis_id")
    ]
    momentum_review_empty_or_missing = (
        not pending_momentum_decay_probes
        and (
            not pending_audit
            or verdict.get("autoloop_execution_status") == "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC"
        )
    )
    if (
        not pending_momentum_decay_probes
        and previous_pending_momentum_decay_probes
        and momentum_review_empty_or_missing
    ):
        pending_momentum_decay_probes = previous_pending_momentum_decay_probes
    stale_before_final_review = pending_audit.get("stale_before_final_review") or {}
    pending_stale_before_final_watch = [
        compact_pending_stale_before_final_cluster(row)
        for row in (
            stale_before_final_review.get(
                "selected_clusters_to_cover_stale_before_final_upper_bound"
            )
            or []
        )
        if isinstance(row, dict) and row.get("cluster")
    ]
    previous_pending_stale_before_final_watch = [
        row
        for row in (registry.get("shadow_only_pending_stale_before_final_watch") or [])
        if isinstance(row, dict) and row.get("hypothesis_id")
    ]
    stale_review_empty_or_missing = (
        not pending_stale_before_final_watch
        and (
            not pending_audit
            or verdict.get("autoloop_execution_status") == "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC"
        )
    )
    if (
        not pending_stale_before_final_watch
        and previous_pending_stale_before_final_watch
        and stale_review_empty_or_missing
    ):
        pending_stale_before_final_watch = previous_pending_stale_before_final_watch
    watchlist_hypotheses = capture.get("watchlist_hypotheses", [])[:25]
    new_signature = stable_hypothesis_signature(
        watchlist_hypotheses=watchlist_hypotheses,
        matured_volume_watch=matured_volume_watch,
        quality_timing_watch=quality_timing_watch,
        quality_timing_candidate_probes=quality_timing_candidate_probes,
        decision_no_pass_quality_timing_watch=decision_no_pass_quality_timing_watch,
        pending_momentum_decay_probes=pending_momentum_decay_probes,
        pending_stale_before_final_watch=pending_stale_before_final_watch,
    )
    previous_signature = registry.get("hypothesis_set_signature")
    previous_frozen_at = registry.get("hypothesis_frozen_at") or registry.get("updated_at")
    hypothesis_frozen_at = previous_frozen_at if previous_signature == new_signature and previous_frozen_at else utc_now()
    new_oos_signature = stable_oos_hypothesis_signature(
        matured_volume_watch=matured_volume_watch,
    )
    previous_oos_signature = registry.get("oos_hypothesis_set_signature")
    previous_oos_frozen_at = (
        registry.get("oos_hypothesis_frozen_at")
        or registry.get("hypothesis_frozen_at")
        or registry.get("updated_at")
    )
    oos_hypothesis_frozen_at = (
        previous_oos_frozen_at
        if previous_oos_signature in (None, new_oos_signature) and previous_oos_frozen_at
        else utc_now()
    )
    registry = {
        "schema_version": "hypothesis_registry.v2",
        "updated_at": utc_now(),
        "hypothesis_frozen_at": hypothesis_frozen_at,
        "hypothesis_set_signature": new_signature,
        "oos_hypothesis_frozen_at": oos_hypothesis_frozen_at,
        "oos_hypothesis_set_signature": new_oos_signature,
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
        "shadow_only_decision_no_pass_quality_timing_watch": decision_no_pass_quality_timing_watch,
        "shadow_only_pending_momentum_decay_probes": pending_momentum_decay_probes,
        "shadow_only_pending_stale_before_final_watch": pending_stale_before_final_watch,
        "strategy_memory": compact_strategy_memory_registry(strategy_memory_summary),
        "recent_runs": recent[-20:],
    }
    write_json(target, registry)
    return registry


def build_run_summary(verdict, paths, diagnostics, tests):
    oos_v3 = verdict.get("oos_readiness_summary_v3") or {}
    post_freeze = oos_v3.get("pass_allow_60_post_freeze_oos_validation") or {}
    oos_preliminary = oos_v3.get("oos_preliminary_signal_summary") or {}
    oos_data = post_freeze.get("oos_data_availability") or {}
    source_activity = post_freeze.get("post_freeze_source_activity") or oos_data.get("post_freeze_source_activity") or {}
    pass_allow_priority = verdict.get("pass_allow_60_closure_priority_queue") or {}
    pass_allow_freeze_priority = verdict.get("pass_allow_60_oos_freeze_priority_queue") or {}
    pass_allow_oos_queue = verdict.get("pass_allow_60_closure_oos_queue") or {}
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
        f"- capture_60_gap_classification: `{(verdict.get('capture_60_target_loop') or {}).get('classification') or verdict.get('capture_60_gap_classification')}`",
        f"- capture_60_biggest_gap_stage: `{(verdict.get('capture_60_target_loop') or {}).get('biggest_gap_stage')}`",
        f"- capture_60_target_shortfall_stage: `{(verdict.get('capture_60_target_loop') or {}).get('target_shortfall_stage')}`",
        f"- capture_60_largest_transition_dropoff: `{json.dumps((verdict.get('capture_60_target_loop') or {}).get('largest_transition_dropoff') or {}, sort_keys=True)}`",
        f"- capture_60_additional_count_needed: `{(verdict.get('capture_60_target_loop') or {}).get('additional_count_needed_to_60')}`",
        f"- capture_60_next_best_allowed_action: `{(verdict.get('capture_60_target_loop') or {}).get('next_best_allowed_action')}`",
        f"- decision_capture_60_gap_classification: `{(verdict.get('decision_capture_60_gap_audit') or {}).get('classification')}`",
        f"- decision_capture_60_shadow_bridge_mirror_complete: `{(verdict.get('decision_capture_60_gap_audit') or {}).get('shadow_bridge_mirror_complete')}`",
        f"- decision_capture_60_optimistic_rate_if_shadow_gap_logged: `{(verdict.get('decision_capture_60_gap_audit') or {}).get('optimistic_decision_record_rate_if_shadow_gap_logged')}`",
        f"- post_freeze_oos_classification: `{post_freeze.get('classification')}`",
        f"- post_freeze_raw_gold_silver_event_rows: `{post_freeze.get('raw_gold_silver_event_rows')}`",
        f"- post_freeze_min_raw_events_for_oos_judgment: `{post_freeze.get('min_raw_events_for_oos_judgment')}`",
        f"- post_freeze_usable_hours: `{post_freeze.get('post_freeze_usable_hours')}`",
        f"- post_freeze_all_raw_rows_since_eval_start: `{source_activity.get('all_raw_rows_since_eval_start')}`",
        f"- post_freeze_latest_raw_signal_age_sec: `{source_activity.get('latest_raw_signal_age_sec')}`",
        f"- post_freeze_raw_gold_silver_rows_since_eval_start_unfiltered: `{source_activity.get('raw_gold_silver_rows_since_eval_start_unfiltered')}`",
        f"- post_freeze_oos_data_next_action: `{oos_data.get('next_action')}`",
        f"- oos_preliminary_signal_classification: `{oos_preliminary.get('classification')}`",
        f"- oos_preliminary_signal_next_action: `{oos_preliminary.get('next_action')}`",
        f"- oos_best_preliminary_track: `{oos_preliminary.get('best_preliminary_track')}`",
        f"- oos_best_preliminary_repeat_watch_count: `{oos_preliminary.get('best_preliminary_repeat_watch_count')}`",
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
        "## Capture 60 Target Loop",
        "",
        "```json",
        json.dumps(
            {
                "capture_60_target_loop": verdict.get("capture_60_target_loop") or {},
                "context_dimension_eligibility": {
                    key: (verdict.get("context_dimension_eligibility") or {}).get(key)
                    for key in (
                        "available",
                        "status_counts",
                        "clean_dimensions",
                        "blocked_dimensions",
                        "promotion_allowed",
                    )
                },
                "pending_to_final_entry_audit_v3": verdict.get("pending_to_final_entry_audit_v3") or {},
                "final_entry_readiness_audit_v3": verdict.get("final_entry_readiness_audit_v3") or {},
                "shadow_candidate_improvement_queue": {
                    key: (verdict.get("shadow_candidate_improvement_queue") or {}).get(key)
                    for key in (
                        "available",
                        "queue_count",
                        "source_counts",
                        "promotion_allowed",
                    )
                },
                "oos_readiness_summary_v3": verdict.get("oos_readiness_summary_v3") or {},
            },
            indent=2,
            sort_keys=True,
        )[:12000],
        "```",
        "",
        "## Pass Allow 60 Closure Priority Queue",
        "",
        "Shadow-only queue for closing the pass_allow gap to 60%. These items are evidence collection and OOS validation targets, not permission to change strategy, gates, final_entry_contract, A_CLASS, executor, paper/live, canary, wallet, or risk.",
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
                    "priority_queue_count": pass_allow_freeze_priority.get("priority_queue_count"),
                    "frozen_definition_count": pass_allow_freeze_priority.get("frozen_definition_count"),
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
        "## Strategy Memory",
        "",
        "```json",
        json.dumps(
            {
                "strategy_memory": verdict.get("strategy_memory") or {},
                "strategy_memory_ingestion_summary": verdict.get("strategy_memory_ingestion_summary") or {},
                "strategy_memory_validation": {
                    key: (verdict.get("strategy_memory_validation") or {}).get(key)
                    for key in (
                        "available",
                        "hypotheses_count",
                        "status_counts",
                        "window_validations",
                        "promotion_allowed",
                        "evidence_role",
                    )
                },
                "strategy_memory_filtered_winner_bridge": verdict.get("strategy_memory_filtered_winner_bridge") or {},
                "strategy_memory_exit_shadow_summary": {
                    key: (verdict.get("strategy_memory_exit_shadow_summary") or {}).get(key)
                    for key in (
                        "available",
                        "exit_policy_variants_tested",
                        "sample_count",
                        "gold_silver_sample_count",
                        "promotion_allowed",
                        "evidence_role",
                    )
                },
                "strategy_memory_delay_replay_summary": {
                    key: (verdict.get("strategy_memory_delay_replay_summary") or {}).get(key)
                    for key in (
                        "available",
                        "delay_replay_done",
                        "entry_delays_sec",
                        "promotion_allowed",
                        "evidence_role",
                    )
                },
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
    shutil.copy2(handoff_path, latest_dir / "codex_handoff.md")
    shutil.copy2(handoff_path, latest_dir / "latest_codex_handoff.md")
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
        timeout_status = autoloop_timeout_status(diagnostics)
        if any(not row.get("ok") for row in diagnostics):
            if timeout_status:
                verdict_payload["blockers"] = sorted(
                    set((verdict_payload.get("blockers") or []) + ["autoloop_execution_timeout_partial_sync"])
                )
                verdict_payload["classification"] = "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC"
                verdict_payload["autoloop_execution_status"] = timeout_status["status"]
                verdict_payload["timeout_reconciliation"] = timeout_status
                verdict_payload["strategy_failure_inferred"] = False
                verdict_payload["next_action"] = timeout_status["next_action"]
            else:
                verdict_payload["blockers"] = sorted(set((verdict_payload.get("blockers") or []) + ["report_generation_failed"]))
                verdict_payload["classification"] = "BLOCKED_DATA"
                verdict_payload["autoloop_execution_status"] = "FULL_RUN_REPORT_GENERATION_FAILED"
            verdict_payload["promotion_allowed"] = False
        else:
            verdict_payload["autoloop_execution_status"] = "FULL_RUN_COMPLETED"
        if state != "final" and not timeout_status:
            verdict_payload["blockers"] = sorted(set((verdict_payload.get("blockers") or []) + [state]))
            verdict_payload["classification"] = "BLOCKED_DATA"
            verdict_payload["promotion_allowed"] = False
        elif state != "final" and timeout_status:
            verdict_payload["blockers"] = sorted(set((verdict_payload.get("blockers") or []) + [state]))
            verdict_payload["promotion_allowed"] = False
        verdict_payload["loop"] = {
            "schema_version": SCHEMA_VERSION,
            "run_id": rid,
            "run_dir": str(run_dir),
            "state": state,
            "report_diagnostics": diagnostics,
        }
        return verdict_payload

    def run_capture_60_target_artifacts():
        result = command_result(
            "capture_60_target_loop",
            [
                "scripts/capture_60_target_loop.py",
                "--run-dir", str(run_dir),
                "--out-dir", str(run_dir),
            ],
            timeout=max(60, int(args.report_timeout_sec)),
        )
        diagnostics.append(result)
        v3_outputs = {
            "capture_60_gap_report": "capture_60_gap_report.json",
            "capture_stage_metrics": "capture_stage_metrics.json",
            "context_dimension_eligibility": "context_dimension_eligibility.json",
            "decision_capture_60_gap_audit": "decision_capture_60_gap_audit.json",
            "pass_allow_capture_gap_audit": "pass_allow_capture_gap_audit.json",
            "decision_no_pass_quality_timing_review": "decision_no_pass_quality_timing_review.json",
            "pass_allow_60_closure_plan": "pass_allow_60_closure_plan.json",
            "pass_allow_60_oos_freeze_registry": "pass_allow_60_oos_freeze_registry.json",
            "pass_allow_60_oos_readiness_monitor": "pass_allow_60_oos_readiness_monitor.json",
            "pass_allow_60_post_freeze_oos_validation": "pass_allow_60_post_freeze_oos_validation.json",
            "capture_cross_oos_freeze_registry": "capture_cross_oos_freeze_registry.json",
            "capture_cross_post_freeze_oos_validation": "capture_cross_post_freeze_oos_validation.json",
            "pending_to_final_entry_audit": "pending_to_final_entry_audit.json",
            "final_entry_readiness_audit": "final_entry_readiness_audit.json",
            "strategy_memory_capture_validation": "strategy_memory_capture_validation.json",
            "shadow_candidate_improvement_queue": "shadow_candidate_improvement_queue.json",
            "oos_readiness_summary": "oos_readiness_summary.json",
        }
        for key, filename in v3_outputs.items():
            path = run_dir / filename
            if path.exists():
                readiness_paths[key] = path
        return result

    def run_pass_allow_60_post_freeze_validation():
        freeze_registry = run_dir / "pass_allow_60_oos_freeze_registry.json"
        if not freeze_registry.exists():
            return None
        out_path = run_dir / "pass_allow_60_post_freeze_oos_validation.json"
        result = command_result(
            "pass_allow_60_post_freeze_oos_validation",
            [
                "scripts/pass_allow_60_post_freeze_oos_validation.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--freeze-registry", str(freeze_registry),
                "--expected-candidates", str(args.expected_candidates),
                "--out", str(out_path),
            ],
            timeout=max(60, int(args.report_timeout_sec) * 2),
        )
        diagnostics.append(result)
        if out_path.exists():
            readiness_paths["pass_allow_60_post_freeze_oos_validation"] = out_path
        return result

    def run_capture_cross_post_freeze_validation():
        freeze_registry = run_dir / "capture_cross_oos_freeze_registry.json"
        if not freeze_registry.exists():
            return None
        out_path = run_dir / "capture_cross_post_freeze_oos_validation.json"
        result = command_result(
            "capture_cross_post_freeze_oos_validation",
            [
                "scripts/capture_cross_post_freeze_oos_validation.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--kline-db", getattr(args, "kline_db", "/app/data/kline_cache.db"),
                "--freeze-registry", str(freeze_registry),
                "--expected-candidates", str(args.expected_candidates),
                "--out", str(out_path),
            ],
            timeout=max(60, int(args.report_timeout_sec) * 2),
        )
        diagnostics.append(result)
        if out_path.exists():
            readiness_paths["capture_cross_post_freeze_oos_validation"] = out_path
        return result

    verdict = build_loop_verdict()
    verdict_path = run_dir / "reviewer_verdict.json"
    strategy_memory_summary = {}
    strategy_memory_path = readiness_paths.get("strategy_memory_ingestion_summary")
    if strategy_memory_path and Path(strategy_memory_path).exists():
        try:
            strategy_memory_summary = load_json(strategy_memory_path)
        except Exception:
            strategy_memory_summary = {}
    registry = update_hypothesis_registry(args.registry, verdict, capture, strategy_memory_summary)
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
    decision_no_pass_watch_validation_path = (
        run_dir / f"decision_no_pass_quality_timing_watch_validation_{int(args.hours)}h.json"
    )
    pending_momentum_decay_validation_path = (
        run_dir / f"pending_momentum_decay_recheck_validation_{int(args.hours)}h.json"
    )
    pending_stale_before_final_validation_path = (
        run_dir / f"pending_stale_before_final_watch_validation_{int(args.hours)}h.json"
    )

    def load_pending_to_final_report():
        pending_path = readiness_paths.get("pending_to_final_entry_audit")
        if pending_path and Path(pending_path).exists():
            try:
                return load_json(pending_path)
            except Exception:
                return {}
        return {}

    def load_decision_no_pass_review():
        decision_path = readiness_paths.get("decision_no_pass_quality_timing_review")
        if decision_path and Path(decision_path).exists():
            try:
                return load_json(decision_path)
            except Exception:
                return {}
        return {}

    write_json(
        quality_timing_probe_validation_path,
        build_quality_timing_candidate_probe_validation(registry, quality_timing_report),
    )
    readiness_paths["quality_timing_candidate_probe_validation"] = quality_timing_probe_validation_path
    write_json(
        decision_no_pass_watch_validation_path,
        build_decision_no_pass_quality_timing_watch_validation(
            registry,
            load_decision_no_pass_review(),
        ),
    )
    readiness_paths["decision_no_pass_quality_timing_watch_validation"] = (
        decision_no_pass_watch_validation_path
    )
    write_json(
        pending_momentum_decay_validation_path,
        build_pending_momentum_decay_probe_validation(registry, load_pending_to_final_report()),
    )
    readiness_paths["pending_momentum_decay_recheck_validation"] = pending_momentum_decay_validation_path
    write_json(
        pending_stale_before_final_validation_path,
        build_pending_stale_before_final_watch_validation(
            registry,
            load_pending_to_final_report(),
        ),
    )
    readiness_paths["pending_stale_before_final_watch_validation"] = (
        pending_stale_before_final_validation_path
    )
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
                "--post-freeze-probe",
                "--out", str(oos_refresh_path),
            ],
            timeout=oos_timeout,
        )
        diagnostics.append(refresh_result)
        if oos_refresh_path.exists():
            readiness_paths["oos_readiness_probe_refresh"] = oos_refresh_path
        run_capture_60_target_artifacts()
        verdict = build_loop_verdict()
        verdict = attach_latest_readiness_artifacts(verdict, readiness_paths)
        write_json(verdict_path, verdict)
    elif state == "final":
        run_capture_60_target_artifacts()
        verdict = build_loop_verdict()
        write_json(verdict_path, verdict)

    if state == "final":
        # The v3 target artifacts are materialized near the end of the run.
        # Refresh the registry after they exist so target-scoped watchlists
        # such as decision_no_pass_quality_timing are not computed from the
        # pre-v3 verdict and accidentally cleared.
        registry = update_hypothesis_registry(args.registry, verdict, capture, strategy_memory_summary)
        write_json(
            quality_timing_probe_validation_path,
            build_quality_timing_candidate_probe_validation(registry, quality_timing_report),
        )
        readiness_paths["quality_timing_candidate_probe_validation"] = quality_timing_probe_validation_path
        write_json(
            decision_no_pass_watch_validation_path,
            build_decision_no_pass_quality_timing_watch_validation(
                registry,
                load_decision_no_pass_review(),
            ),
        )
        readiness_paths["decision_no_pass_quality_timing_watch_validation"] = (
            decision_no_pass_watch_validation_path
        )
        write_json(
            pending_momentum_decay_validation_path,
            build_pending_momentum_decay_probe_validation(registry, load_pending_to_final_report()),
        )
        readiness_paths["pending_momentum_decay_recheck_validation"] = pending_momentum_decay_validation_path
        write_json(
            pending_stale_before_final_validation_path,
            build_pending_stale_before_final_watch_validation(
                registry,
                load_pending_to_final_report(),
            ),
        )
        readiness_paths["pending_stale_before_final_watch_validation"] = (
            pending_stale_before_final_validation_path
        )
        run_capture_60_target_artifacts()
        run_pass_allow_60_post_freeze_validation()
        run_capture_cross_post_freeze_validation()
        # Rebuild v3 target/OOS artifacts so oos_readiness_summary consumes the
        # freshly refreshed post-freeze validations from the current finalize.
        run_capture_60_target_artifacts()
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
    strategy_memory_dir = root / "strategy_memory_local"
    strategy_memory_dir.mkdir(parents=True, exist_ok=True)
    write_json(strategy_memory_dir / "strategy_memory_hypotheses.json", {
        "schema_version": "strategy_memory_hypotheses.v1",
        "hypotheses_count": 2,
        "promotion_allowed": False,
        "hypotheses": [
            {
                "id": "SM-TEST-ENTRY",
                "name": "Self-test entry memory",
                "strategy_family": "ATH1 early scout",
                "priority": 90,
                "entry_definition": "shadow-only self test",
                "exit_definition": "none",
                "required_features": ["ath_stage", "market_cap"],
                "time_legal_features": ["ath_stage_at_signal"],
                "future_or_posthoc_features": ["future_peak"],
                "known_risks": ["self-test future label"],
                "next_validation_required": "shadow_only_oos",
                "allowed_use": "shadow_only",
                "promotion_allowed": False,
            },
            {
                "id": "SM-EXIT-TEST",
                "name": "Self-test exit memory",
                "strategy_family": "Exit policy variants",
                "priority": 80,
                "entry_definition": "entry unchanged",
                "exit_definition": "exit simulator only",
                "required_features": ["entry_price", "peak_pct"],
                "time_legal_features": ["post_entry_kline_only_for_exit_simulation"],
                "future_or_posthoc_features": ["best_exit_after_peak"],
                "known_risks": ["exit proxy"],
                "next_validation_required": "exit_policy_shadow_simulator",
                "allowed_use": "shadow_only",
                "promotion_allowed": False,
            },
        ],
    })
    write_json(strategy_memory_dir / "strategy_memory_candidate_mapping.json", {
        "schema_version": "strategy_memory_candidate_mapping.v1",
        "hypotheses_count": 2,
        "mapped_to_existing_candidates": 1,
        "missing_shadow_candidates": 1,
        "rejected_future_data_hypotheses": 2,
        "promotion_allowed": False,
        "mappings": [
            {
                "hypothesis_id": "SM-TEST-ENTRY",
                "mapping_status": "partial_existing_context",
                "existing_candidate_ids": ["current_all"],
                "blocked_contexts": ["future_data_features_must_be_labels_only"],
                "requires_future_data_conversion": True,
                "allowed_use": "shadow_only",
                "promotion_allowed": False,
            },
            {
                "hypothesis_id": "SM-EXIT-TEST",
                "mapping_status": "missing_exit_shadow_sim_only",
                "existing_candidate_ids": [],
                "blocked_contexts": ["kline_or_exit_path_required"],
                "requires_future_data_conversion": True,
                "allowed_use": "shadow_only",
                "promotion_allowed": False,
            },
        ],
    })
    write_json(strategy_memory_dir / "strategy_memory_prioritized_queue.json", {
        "schema_version": "offline_strategy_memory_audit.v1",
        "strategy_memory_hypotheses_count": 2,
        "mapped_to_existing_candidates": 1,
        "missing_shadow_candidates": 1,
        "rejected_future_data_hypotheses": 2,
        "top_10_shadow_hypotheses": ["SM-TEST-ENTRY", "SM-EXIT-TEST"],
        "promotion_allowed": False,
    })
    write_json(strategy_memory_dir / "filtered_winner_dossier_24h.json", {
        "schema_version": "filtered_winner_dossier.v1",
        "filtered_winner_count": 3,
        "promotion_allowed": False,
        "allowed_use": "shadow_only",
    })
    write_json(strategy_memory_dir / "exit_policy_shadow_simulator_24h.json", {
        "schema_version": "exit_policy_shadow_simulator.v1",
        "exit_policy_variants_tested": 2,
        "promotion_allowed": False,
        "allowed_use": "shadow_only",
    })
    write_json(strategy_memory_dir / "execution_delay_adjusted_replay_24h.json", {
        "schema_version": "execution_delay_adjusted_replay.v1",
        "delay_replay_done": True,
        "promotion_allowed": False,
        "allowed_use": "shadow_only",
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
            strategy_memory_dir=str(root / "strategy_memory_local"),
            markov_profiles=DEFAULT_MARKOV_PROFILES,
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
        summary_text = Path(result["latest_summary"]).read_text(encoding="utf-8")
        assert "capture_60_gap_classification:" in summary_text
        assert "decision_capture_60_gap_classification:" in summary_text
        markov_summary = verdict["Markov_effectiveness_summary"]
        assert markov_summary["usage"] == "research_only_markov_information_value"
        assert markov_summary["classification"] in {
            "MARKOV_DISCOVERY_INFORMATIVE",
            "MARKOV_PROFILE_FRAGMENTED_OR_UNINFORMATIVE",
            "MARKOV_RED_ONLY_NON_INFORMATIVE",
            "MARKOV_INSUFFICIENT_OR_UNINFORMATIVE",
        }
        assert "total_red_buckets" in markov_summary
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
        assert "shadow_only_decision_no_pass_quality_timing_watch" in registry
        assert "shadow_only_pending_momentum_decay_probes" in registry
        assert "strategy_memory" in registry
        assert registry["strategy_memory"]["promotion_allowed"] is False
        assert registry["strategy_memory"]["allowed_use"] == "shadow_only"
        assert registry["strategy_memory"]["evidence_level"] == "historical_memory"
        assert registry["strategy_memory"]["summary"]["strategy_memory_hypotheses_count"] == 2
        assert len(registry["strategy_memory"]["hypotheses"]) == 2
        assert registry["strategy_memory"]["hypotheses"][0]["promotion_allowed"] is False
        assert any(
            row["route"] == "exit_policy_shadow_simulator_only"
            for row in registry["strategy_memory"]["hypotheses"]
        )
        assert registry["strategy_memory"]["missing_shadow_candidate_handoffs"][0]["allowed_action"] == "generate_codex_handoff_only"
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
                "decision_no_pass_quality_timing_review": {
                    "selected_clusters_to_cover_current_pass_allow_gap_upper_bound": [
                        {
                            "cluster": "matrix_alignment_wait",
                            "decision_no_pass_event_count": 7,
                            "events_contributing_to_gap_upper_bound": 7,
                            "cumulative_events_contributing_to_gap_upper_bound": 7,
                            "suggested_shadow_only_action": "track_matrix_alignment_false_negative_shadow_probe",
                            "human_approval_required_if_fix_requires": "changing matrix alignment thresholds",
                        }
                    ],
                    "top_clusters": [
                        {
                            "cluster": "matrix_alignment_wait",
                            "decision_no_pass_event_count": 7,
                            "share_of_raw_gold_silver": 0.06,
                            "share_of_current_pass_allow_gap_upper_bound": 0.31,
                            "unique_tokens": 6,
                            "candidate_matched_any_rate": 1.0,
                            "reason_counts": [
                                {
                                    "component": "matrix_evaluator",
                                    "event_type": "matrix_decision",
                                    "decision": "wait",
                                    "reason": "matrices not yet aligned",
                                    "count": 7,
                                }
                            ],
                            "top_candidates": [
                                {"candidate_id": "entry_mode_registry:smart_entry_pullback_bounce", "family": "entry_mode_registry", "count": 7},
                            ],
                            "top_lifecycle_source_contexts": [
                                {"lifecycle_profile": "ATH_SHALLOW_PULLBACK:PROBE", "source_component": "matrix_evaluator", "count": 1}
                            ],
                            "suggested_shadow_only_action": "track_matrix_alignment_false_negative_shadow_probe",
                            "human_approval_required_if_fix_requires": "changing matrix alignment thresholds",
                        }
                    ],
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
        assert manual_registry["shadow_only_decision_no_pass_quality_timing_watch"][0]["hypothesis_id"] == (
            "decision_no_pass_quality_timing:matrix_alignment_wait"
        )
        assert manual_registry["shadow_only_decision_no_pass_quality_timing_watch"][0]["promotion_allowed"] is False
        assert manual_registry["shadow_only_decision_no_pass_quality_timing_watch"][0]["definition"]["stage"] == (
            "decision_no_pass_or_allow"
        )
        original_oos_frozen_at = manual_registry["oos_hypothesis_frozen_at"]
        changed_non_oos_registry = update_hypothesis_registry(
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
                            "verdict": "MATURED_VOLUME_DISCOVERY_WATCH",
                        }
                    ]
                },
                "quality_timing_reject_research_audit": {
                    "shadow_only_review": {
                        "top_research_opportunities": [
                            {
                                "cluster": "low_volume_observe",
                                "suggested_shadow_only_action": "track_low_volume_gold_silver_shadow_probe",
                            }
                        ]
                    }
                },
            },
            {"watchlist_hypotheses": []},
        )
        assert changed_non_oos_registry["oos_hypothesis_frozen_at"] == original_oos_frozen_at
        preserved_registry = update_hypothesis_registry(
            manual_registry_path,
            {
                "classification": "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC",
                "autoloop_execution_status": "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC",
                "decision_no_pass_quality_timing_review": {
                    "classification": "DECISION_NO_PASS_QUALITY_TIMING_REVIEW_EMPTY",
                    "selected_clusters_to_cover_current_pass_allow_gap_upper_bound": [],
                },
            },
            {"watchlist_hypotheses": []},
        )
        assert len(preserved_registry["shadow_only_decision_no_pass_quality_timing_watch"]) == 1
        assert preserved_registry["shadow_only_decision_no_pass_quality_timing_watch"][0]["hypothesis_id"] == (
            "decision_no_pass_quality_timing:matrix_alignment_wait"
        )
        required_artifacts = [
            "capture_60_gap_report.json",
            "capture_stage_metrics.json",
            "context_dimension_eligibility.json",
            "pass_allow_capture_gap_audit.json",
            "decision_no_pass_quality_timing_review.json",
            "pending_to_final_entry_audit.json",
            "final_entry_readiness_audit.json",
            "strategy_memory_capture_validation.json",
            "shadow_candidate_improvement_queue.json",
            "oos_readiness_summary.json",
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
            "decision_no_pass_quality_timing_watch_validation_24h.json",
            "pending_momentum_decay_recheck_validation_24h.json",
            "pending_stale_before_final_watch_validation_24h.json",
            "strategy_memory_ingestion_summary.json",
            "strategy_memory_validation_24h.json",
            "strategy_memory_filtered_winner_bridge.json",
            "strategy_memory_exit_shadow_summary.json",
            "strategy_memory_delay_replay_summary.json",
            "codex_handoff.md",
            "latest_codex_handoff.md",
        ]
        missing_artifacts = [name for name in required_artifacts if not (latest_dir / name).exists()]
        assert not missing_artifacts, missing_artifacts
        assert "current_commit:" in (latest_dir / "codex_handoff.md").read_text()
        assert (latest_dir / "codex_handoff.md").read_text() == (
            latest_dir / "latest_codex_handoff.md"
        ).read_text()
        capture_cross_validity = load_json(latest_dir / "capture_cross_validity_24h.json")
        assert capture_cross_validity["classification"] in {
            "CAPTURE_CROSS_DISCOVERY_HIT_PENDING_OOS",
            "CAPTURE_CROSS_DISCOVERY_WATCH",
            "CAPTURE_CROSS_BLOCKED_CONTEXT_COVERAGE",
            "CAPTURE_CROSS_NO_VALID_SIGNAL",
        }
        assert capture_cross_validity["next_action"]
        assert capture_cross_validity["promotion_allowed"] is False
        assert capture_cross_validity["same_window_discovery_only"] is True
        assert capture_cross_validity["oos_required_before_promotion"] is True
        assert capture_cross_validity["criteria"]["downstream_lift_scope"] in {
            "slice_level_matched_gold_silver_signal_id",
            "candidate_level_after_match_proxy_not_slice_specific",
        }
        if capture_cross_validity["criteria"]["downstream_lift_scope"] == "slice_level_matched_gold_silver_signal_id":
            assert capture_cross_validity["criteria"]["slice_level_downstream_join_required_for_promotion_evidence"] is False
        else:
            assert capture_cross_validity["criteria"]["slice_level_downstream_join_required_for_promotion_evidence"] is True
        if capture_cross_validity.get("valid_top_crosses"):
            first_cross = capture_cross_validity["valid_top_crosses"][0]
            assert "decision_lift" in first_cross
            assert "pass_allow_lift" in first_cross
            assert "pending_lift" in first_cross
            assert "final_entry_lift" in first_cross
            assert "mode_adjusted_final_eligibility_lift" in first_cross
            assert first_cross["downstream_lift_scope"] in {
                "slice_level_matched_gold_silver_signal_id",
                "candidate_level_after_match_proxy_not_slice_specific",
                "slice_level_unavailable",
            }
            if first_cross["downstream_lift_scope"] == "slice_level_matched_gold_silver_signal_id":
                assert first_cross["downstream_slice_join_available"] is True
                assert first_cross["downstream_slice_join_required_for_promotion_evidence"] is False
                assert "slice_downstream_signal_count" in first_cross
            assert first_cross["pnl_secondary_status"] == "secondary_pnl_report_required_not_used_for_capture_cross"
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
        assert "oos_readiness_queue" in quality_probe_validation
        assert quality_probe_validation["oos_readiness_queue"]["promotion_allowed"] is False
        assert quality_probe_validation["oos_readiness_queue"]["automatic_runtime_change_allowed"] is False
        assert "probe_validations" in quality_probe_validation
        if quality_probe_validation["probe_validations"]:
            first_probe = quality_probe_validation["probe_validations"][0]
            assert first_probe.get("quality_timing_cluster")
            assert first_probe.get("candidate_id")
            assert first_probe.get("next_action")
        if quality_probe_validation.get("top_repeated_probes"):
            first_repeated_probe = quality_probe_validation["top_repeated_probes"][0]
            assert first_repeated_probe.get("quality_timing_cluster")
            assert first_repeated_probe.get("candidate_id")
            assert first_repeated_probe.get("next_action") == (
                "evaluate_probe_in_next_clean_non_overlapping_window"
            )
        decision_watch_validation = load_json(
            latest_dir / "decision_no_pass_quality_timing_watch_validation_24h.json"
        )
        assert decision_watch_validation["promotion_allowed"] is False
        assert decision_watch_validation["strategy_change_allowed"] is False
        assert decision_watch_validation["automatic_runtime_change_allowed"] is False
        assert decision_watch_validation["paper_enablement_allowed"] is False
        assert "denominator" in decision_watch_validation
        assert "watch_validations" in decision_watch_validation
        if decision_watch_validation["watch_validations"]:
            first_watch = decision_watch_validation["watch_validations"][0]
            assert first_watch.get("cluster")
            assert first_watch.get("stage")
            assert first_watch.get("next_action")
        if decision_watch_validation.get("top_repeated_clusters"):
            first_repeated_cluster = decision_watch_validation["top_repeated_clusters"][0]
            assert first_repeated_cluster.get("cluster")
            assert first_repeated_cluster.get("next_action") == (
                "evaluate_cluster_in_next_clean_non_overlapping_window"
            )
        assert "oos_readiness_queue" in decision_watch_validation
        assert decision_watch_validation["oos_readiness_queue"]["promotion_allowed"] is False
        pending_momentum_validation = load_json(latest_dir / "pending_momentum_decay_recheck_validation_24h.json")
        assert pending_momentum_validation["promotion_allowed"] is False
        assert pending_momentum_validation["strategy_change_allowed"] is False
        assert pending_momentum_validation["automatic_runtime_change_allowed"] is False
        assert pending_momentum_validation["paper_enablement_allowed"] is False
        assert "denominator" in pending_momentum_validation
        assert "probe_validations" in pending_momentum_validation
        assert "oos_readiness_queue" in pending_momentum_validation
        assert pending_momentum_validation["oos_readiness_queue"]["promotion_allowed"] is False
        strategy_memory = load_json(latest_dir / "strategy_memory_ingestion_summary.json")
        assert strategy_memory["promotion_allowed"] is False
        assert strategy_memory["allowed_use"] == "shadow_only"
        assert strategy_memory["evidence_level"] == "historical_memory"
        assert strategy_memory["strategy_memory_hypotheses_count"] == 2
        assert strategy_memory["mapped_to_existing_candidates"] == 1
        assert strategy_memory["missing_shadow_candidates"] == 1
        assert strategy_memory["rejected_future_data_hypotheses"] == 2
        assert strategy_memory["paper_trades_db_available"] is True
        assert strategy_memory["missing_shadow_candidate_handoffs"][0]["allowed_action"] == "generate_codex_handoff_only"
        assert any(row["route"] == "exit_policy_shadow_simulator_only" for row in strategy_memory["hypotheses"])
        strategy_validation = load_json(latest_dir / "strategy_memory_validation_24h.json")
        assert strategy_validation["promotion_allowed"] is False
        assert strategy_validation["strategy_memory_enabled"] is True
        assert strategy_validation["hypotheses_count"] == 2
        assert "STRATEGY_MEMORY_EXIT_ONLY" in strategy_validation["status_counts"]
        assert "window_validations" in strategy_validation
        assert load_json(latest_dir / "strategy_memory_filtered_winner_bridge.json")["promotion_allowed"] is False
        assert load_json(latest_dir / "strategy_memory_exit_shadow_summary.json")["promotion_allowed"] is False
        assert load_json(latest_dir / "strategy_memory_delay_replay_summary.json")["promotion_allowed"] is False
    print("SELF_TEST_PASS agent_capture_discovery_loop")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument("--data-dir", default="/app/data")
    parser.add_argument(
        "--strategy-memory-dir",
        default=None,
        help="Optional Strategy Memory artifact directory. Defaults to data-dir strategy_memory_local/strategy_memory.",
    )
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
    parser.add_argument("--markov-profiles", default=DEFAULT_MARKOV_PROFILES)
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
