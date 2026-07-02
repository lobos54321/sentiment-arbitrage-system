#!/usr/bin/env python3
"""Staged Gold/Silver Capture AutoLoop runner.

This is a read-only reliability wrapper around ``agent_capture_discovery_loop``.
It lets Zeabur exec run short resumable stages instead of one long command that
can be cut off by HTTP 524. It only produces evaluator artifacts; it never
changes strategy, entry policy, gates, final_entry_contract, A_CLASS mode,
executor, wallet, canary, or risk settings.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from agent_capture_discovery_loop import (
    DEFAULT_HANDOFF_DIR,
    DEFAULT_MARKOV_PROFILES,
    DEFAULT_OUT_ROOT,
    DEFAULT_REGISTRY,
    build_a_class_fastlane_mode_audit,
    build_candidate_effectiveness_report,
    build_candidate_improvement_opportunities_report,
    build_capture_cross_validity_report,
    build_context_coverage_report,
    build_markov_effectiveness_report,
    build_shadow_decision_bridge_audit,
    build_strategy_memory_ingestion_summary,
    create_self_test_dbs,
    load_json,
    parse_capture_hours,
    run_report,
    run_self_tests,
    sync_latest,
    utc_now,
    write_derived_report,
    write_json,
    write_materialized_artifacts,
)


SCHEMA_VERSION = "agent_autoloop_stage_runner.v1"
REQUIRED_FINAL_ARTIFACTS = (
    "capture_60_gap_report.json",
    "capture_stage_metrics.json",
    "context_dimension_eligibility.json",
    "decision_capture_60_gap_audit.json",
    "pass_allow_capture_gap_audit.json",
    "decision_no_pass_quality_timing_review.json",
    "pass_allow_60_closure_plan.json",
    "pass_allow_60_oos_freeze_registry.json",
    "pass_allow_60_oos_readiness_monitor.json",
    "capture_cross_oos_freeze_registry.json",
    "pending_to_final_entry_audit.json",
    "final_entry_readiness_audit.json",
    "strategy_memory_capture_validation.json",
    "shadow_candidate_improvement_queue.json",
    "capture_cross_validity_24h.json",
    "markov_effectiveness_24h.json",
    "quality_timing_candidate_probe_validation_24h.json",
    "decision_no_pass_quality_timing_watch_validation_24h.json",
    "pending_stale_before_final_watch_validation_24h.json",
    "oos_readiness_summary.json",
    "reviewer_verdict.json",
    "run_summary.md",
    "codex_handoff.md",
)


def write_text(path, text):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(target)


def load_json_default(path, default=None):
    try:
        return load_json(path)
    except Exception:
        return default if default is not None else {}


def stage_run_id():
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def run_dir_for(args):
    if args.run_dir:
        return Path(args.run_dir)
    return Path(args.out_root) / args.run_id


def stage_state_path(run_dir):
    return Path(run_dir) / "stage_state.json"


def diagnostics_path(run_dir):
    return Path(run_dir) / "stage_diagnostics.json"


def load_diagnostics(run_dir):
    data = load_json_default(diagnostics_path(run_dir), [])
    return data if isinstance(data, list) else []


def append_diagnostics(run_dir, rows):
    diagnostics = load_diagnostics(run_dir)
    diagnostics.extend(rows if isinstance(rows, list) else [rows])
    write_json(diagnostics_path(run_dir), diagnostics)
    return diagnostics


def update_stage_state(run_dir, stage, **fields):
    path = stage_state_path(run_dir)
    state = load_json_default(path, {})
    state.update({
        "schema_version": SCHEMA_VERSION,
        "updated_at": utc_now(),
        "run_dir": str(run_dir),
        "last_stage": stage,
        **fields,
    })
    completed = list(state.get("completed_stages") or [])
    if stage not in completed:
        completed.append(stage)
    state["completed_stages"] = completed
    write_json(path, state)
    return state


def capture_denominator(report):
    denominator = (report or {}).get("raw_gold_silver_denominator") or {}
    if isinstance(denominator, dict):
        return int(denominator.get("event_rows") or denominator.get("unique_tokens") or 0)
    try:
        return int(denominator or 0)
    except Exception:
        return 0


def final_publish_eligibility(run_dir):
    path = Path(run_dir)
    capture = load_json_default(path / "capture_discovery_24h.json", {})
    gap = load_json_default(path / "capture_60_gap_report.json", {})
    a_class = load_json_default(path / "a_class_fastlane_mode_audit_24h.json", {})
    capture_den = capture_denominator(capture)
    gap_den = int(gap.get("raw_gold_silver_denominator") or 0)
    stage2_raw = int(((a_class.get("stage2_flat_summary") or {}).get("raw_gs_events")) or 0)
    missing = [name for name in REQUIRED_FINAL_ARTIFACTS if not (path / name).exists()]
    blockers = []
    if missing:
        blockers.append("missing_required_final_artifacts")
    if capture_den > 0 and gap_den <= 0:
        blockers.append("capture_60_denominator_zero_while_capture_denominator_positive")
    if capture_den > 0 and stage2_raw <= 0:
        blockers.append("a_class_stage2_summary_missing_while_capture_denominator_positive")
    return {
        "eligible": not blockers,
        "blockers": blockers,
        "missing_required_final_artifacts": missing,
        "capture_denominator": capture_den,
        "capture_60_denominator": gap_den,
        "a_class_stage2_raw_gs_events": stage2_raw,
    }


def args_namespace(args):
    return argparse.Namespace(
        paper_db=args.paper_db,
        raw_db=args.raw_db,
        kline_db=args.kline_db,
        data_dir=args.data_dir,
        strategy_memory_dir=args.strategy_memory_dir,
        hours=args.hours,
        capture_hours=args.capture_hours,
        expected_candidates=args.expected_candidates,
        out_root=args.out_root,
        handoff_dir=args.handoff_dir,
        registry=args.registry,
        markov_profiles=args.markov_profiles,
        report_timeout_sec=args.report_timeout_sec,
        test_timeout_sec=args.test_timeout_sec,
        max_scan_rows=args.max_scan_rows,
        oos_probe_hours=args.oos_probe_hours,
        quote_fix_deploy_ts=args.quote_fix_deploy_ts,
    )


def stage_init(args, run_dir):
    if args.reset and run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    ingestion_path = run_dir / "strategy_memory_ingestion_summary.json"
    build_strategy_memory_ingestion_summary(args_namespace(args), ingestion_path)
    update_stage_state(run_dir, "init", strategy_memory_ingestion_summary=str(ingestion_path))
    return {"stage": "init", "run_dir": str(run_dir), "strategy_memory_ingestion_summary": str(ingestion_path)}


def stage_selftests(args, run_dir):
    tests = run_self_tests(int(args.test_timeout_sec))
    path = run_dir / "tests.json"
    write_json(path, tests)
    update_stage_state(run_dir, "selftests", tests_passed=tests.get("passed"))
    return {"stage": "selftests", "tests": str(path), "passed": tests.get("passed")}


def stage_capture(args, run_dir):
    rows = []
    capture_hours = parse_stage_capture_hours(args.capture_hours, int(args.hours), bool(args.capture_exact))
    capture_paths = []
    for hours in capture_hours:
        path = run_dir / f"capture_discovery_{hours}h.json"
        capture_paths.append(path)
        rows.append(run_report(
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
            timeout=int(args.report_timeout_sec),
        ))
    default_primary = run_dir / f"capture_discovery_{int(args.hours)}h.json"
    primary = default_primary if default_primary.exists() else (capture_paths[0] if capture_paths else default_primary)
    legacy = run_dir / f"candidate_capture_discovery_{int(args.hours)}h.json"
    if default_primary.exists() and legacy != default_primary:
        shutil.copy2(default_primary, legacy)
    append_diagnostics(run_dir, rows)
    update_stage_state(
        run_dir,
        "capture",
        primary_capture=str(primary),
        capture_exact=bool(args.capture_exact),
        capture_hours=[row.get("name") for row in rows],
    )
    return {"stage": "capture", "capture_exact": bool(args.capture_exact), "diagnostics": rows}


def stage_core(args, run_dir):
    hours = int(args.hours)
    commands = [
        (
            "runtime_health_snapshot",
            [
                "scripts/runtime_health_snapshot_audit.py",
                "--data-dir", args.data_dir,
                "--hours", str(hours),
                "--out", str(run_dir / f"runtime_health_snapshot_{hours}h.json"),
            ],
            run_dir / f"runtime_health_snapshot_{hours}h.json",
        ),
        (
            "raw_gold_silver_funnel_audit",
            [
                "scripts/offline_raw_gold_silver_funnel_audit.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--hours", str(hours),
                "--expected-candidates", str(args.expected_candidates),
                "--out", str(run_dir / f"raw_gold_silver_funnel_audit_{hours}h.json"),
            ],
            run_dir / f"raw_gold_silver_funnel_audit_{hours}h.json",
        ),
        (
            "pnl_cross_secondary",
            [
                "scripts/offline_candidate_cross_eval.py",
                "--db", args.paper_db,
                "--hours", str(hours),
                "--max-scan-rows", str(args.max_scan_rows),
                "--out", str(run_dir / f"pnl_cross_secondary_{hours}h.json"),
            ],
            run_dir / f"pnl_cross_secondary_{hours}h.json",
        ),
        (
            "candidate_downstream_readiness",
            [
                "scripts/candidate_downstream_readiness_audit.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--hours", str(hours),
                "--expected-candidates", str(args.expected_candidates),
                "--out", str(run_dir / f"candidate_downstream_readiness_{hours}h.json"),
            ],
            run_dir / f"candidate_downstream_readiness_{hours}h.json",
        ),
    ]
    rows = [run_report(name, cmd, out, timeout=int(args.report_timeout_sec)) for name, cmd, out in commands]
    append_diagnostics(run_dir, rows)
    update_stage_state(run_dir, "core")
    return {"stage": "core", "diagnostics": rows}


def stage_markov(args, run_dir):
    hours = int(args.hours)
    rows = []
    for profile in [item.strip() for item in str(args.markov_profiles or "").split(",") if item.strip()]:
        path = run_dir / f"candidate_virtual_markov_{profile}_{hours}h.json"
        rows.append(run_report(
            f"virtual_markov_{profile}",
            [
                "scripts/build_candidate_virtual_markov.py",
                "--db", args.paper_db,
                "--hours", str(hours),
                "--profile", profile,
                "--max-scan-rows", str(args.max_scan_rows),
                "--out", str(path),
            ],
            path,
            timeout=int(args.report_timeout_sec),
        ))
    append_diagnostics(run_dir, rows)
    update_stage_state(run_dir, "markov")
    return {"stage": "markov", "diagnostics": rows}


def stage_derived(args, run_dir):
    hours = int(args.hours)
    capture = load_json(run_dir / f"capture_discovery_{hours}h.json")
    downstream = load_json_default(run_dir / f"candidate_downstream_readiness_{hours}h.json", {})
    raw_funnel = load_json_default(run_dir / f"raw_gold_silver_funnel_audit_{hours}h.json", {})
    markov_reports = {}
    for profile in [item.strip() for item in str(args.markov_profiles or "").split(",") if item.strip()]:
        path = run_dir / f"candidate_virtual_markov_{profile}_{hours}h.json"
        if path.exists():
            markov_reports[profile] = load_json(path)
    context_report = build_context_coverage_report(capture)
    context_path = run_dir / f"context_coverage_audit_{hours}h.json"
    write_derived_report(context_path, context_report)
    candidate_effectiveness = build_candidate_effectiveness_report(capture, downstream)
    candidate_effectiveness_path = run_dir / f"candidate_effectiveness_{hours}h.json"
    write_derived_report(candidate_effectiveness_path, candidate_effectiveness)
    markov_effectiveness_path = run_dir / f"markov_effectiveness_{hours}h.json"
    write_derived_report(markov_effectiveness_path, build_markov_effectiveness_report(markov_reports, capture))
    capture_cross = build_capture_cross_validity_report(capture, context_report, downstream)
    capture_cross_path = run_dir / f"capture_cross_validity_{hours}h.json"
    write_derived_report(capture_cross_path, capture_cross)
    improvement_path = run_dir / f"candidate_improvement_opportunities_{hours}h.json"
    write_derived_report(
        improvement_path,
        build_candidate_improvement_opportunities_report(capture, candidate_effectiveness, capture_cross),
    )
    shadow_bridge_path = run_dir / f"shadow_decision_bridge_audit_{hours}h.json"
    write_derived_report(shadow_bridge_path, build_shadow_decision_bridge_audit(raw_funnel))
    a_class_light_path = run_dir / f"a_class_fastlane_mode_audit_{hours}h_derived.json"
    write_derived_report(a_class_light_path, build_a_class_fastlane_mode_audit(raw_funnel, context_report))
    update_stage_state(run_dir, "derived")
    return {
        "stage": "derived",
        "artifacts": [
            str(context_path), str(candidate_effectiveness_path), str(markov_effectiveness_path),
            str(capture_cross_path), str(improvement_path), str(shadow_bridge_path), str(a_class_light_path),
        ],
    }


def stage_context(args, run_dir):
    hours = int(args.hours)
    selected_reports = parse_context_reports(getattr(args, "context_reports", "all"))
    rows = []
    commands = [
        (
            "volume",
            "volume_kline_coverage_audit",
            [
                "scripts/volume_kline_coverage_audit.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--hours", str(hours),
                "--out", str(run_dir / f"volume_kline_coverage_audit_{hours}h.json"),
            ],
            run_dir / f"volume_kline_coverage_audit_{hours}h.json",
        ),
        (
            "matured_recheck",
            "matured_kline_volume_recheck_audit",
            [
                "scripts/matured_kline_volume_recheck_audit.py",
                "--db", args.paper_db,
                "--kline-db", args.kline_db,
                "--hours", str(hours),
                "--out", str(run_dir / f"matured_kline_volume_recheck_audit_{hours}h.json"),
            ],
            run_dir / f"matured_kline_volume_recheck_audit_{hours}h.json",
        ),
        (
            "matured_cross",
            "matured_volume_capture_cross_audit",
            [
                "scripts/matured_volume_capture_cross_audit.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--kline-db", args.kline_db,
                "--hours", str(hours),
                "--expected-candidates", str(args.expected_candidates),
                "--max-scan-rows", str(args.max_scan_rows),
                "--out", str(run_dir / f"matured_volume_capture_cross_audit_{hours}h.json"),
            ],
            run_dir / f"matured_volume_capture_cross_audit_{hours}h.json",
        ),
        (
            "low_confidence",
            "low_confidence_research_capture_audit",
            [
                "scripts/low_confidence_research_capture_audit.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--hours", str(hours),
                "--expected-candidates", str(args.expected_candidates),
                "--out", str(run_dir / f"low_confidence_research_capture_audit_{hours}h.json"),
            ],
            run_dir / f"low_confidence_research_capture_audit_{hours}h.json",
        ),
        (
            "resolution",
            "kline_coverage_resolution_audit",
            [
                "scripts/kline_coverage_resolution_audit.py",
                "--volume-kline-audit", str(run_dir / f"volume_kline_coverage_audit_{hours}h.json"),
                "--low-confidence-audit", str(run_dir / f"low_confidence_research_capture_audit_{hours}h.json"),
                "--matured-kline-recheck", str(run_dir / f"matured_kline_volume_recheck_audit_{hours}h.json"),
                "--matured-volume-cross", str(run_dir / f"matured_volume_capture_cross_audit_{hours}h.json"),
                "--out", str(run_dir / f"kline_coverage_resolution_audit_{hours}h.json"),
            ],
            run_dir / f"kline_coverage_resolution_audit_{hours}h.json",
        ),
    ]
    rows.extend(
        run_report(name, cmd, out, timeout=int(args.report_timeout_sec))
        for report_key, name, cmd, out in commands
        if report_key in selected_reports
    )
    if "hypothesis" in selected_reports:
        hypothesis_path = run_dir / f"hypothesis_validation_audit_{hours}h.json"
        rows.append(run_report(
            "hypothesis_validation_audit",
            [
                "scripts/hypothesis_validation_audit.py",
                "--registry", args.registry,
                "--matured-volume-cross", str(run_dir / f"matured_volume_capture_cross_audit_{hours}h.json"),
                "--out", str(hypothesis_path),
            ],
            hypothesis_path,
            timeout=int(args.report_timeout_sec),
        ))
    append_diagnostics(run_dir, rows)
    update_stage_state(run_dir, "context", context_reports=sorted(selected_reports))
    return {"stage": "context", "context_reports": sorted(selected_reports), "diagnostics": rows}


def stage_decision(args, run_dir):
    hours = int(args.hours)
    context_monitor_cmd = [
        "scripts/context_blocker_monitor.py",
        "--db", args.paper_db,
        "--raw-db", args.raw_db,
        "--hours", str(hours),
        "--out", str(run_dir / f"context_blocker_monitor_{hours}h.json"),
    ]
    if int(args.quote_fix_deploy_ts or 0) > 0:
        context_monitor_cmd.extend(["--deploy-ts", str(int(args.quote_fix_deploy_ts))])
    rows = [
        run_report(
            "quality_timing_reject_research_audit",
            [
                "scripts/quality_timing_reject_research_audit.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--raw-funnel", str(run_dir / f"raw_gold_silver_funnel_audit_{hours}h.json"),
                "--hours", str(hours),
                "--expected-candidates", str(args.expected_candidates),
                "--out", str(run_dir / f"quality_timing_reject_research_audit_{hours}h.json"),
            ],
            run_dir / f"quality_timing_reject_research_audit_{hours}h.json",
            timeout=int(args.report_timeout_sec),
        ),
        run_report(
            "context_blocker_monitor",
            context_monitor_cmd,
            run_dir / f"context_blocker_monitor_{hours}h.json",
            timeout=int(args.report_timeout_sec),
        ),
        run_report(
            "a_class_fastlane_mode_readiness_audit",
            [
                "scripts/a_class_fastlane_mode_readiness_audit.py",
                "--db", args.paper_db,
                "--raw-funnel", str(run_dir / f"raw_gold_silver_funnel_audit_{hours}h.json"),
                "--context-coverage", str(run_dir / f"context_coverage_audit_{hours}h.json"),
                "--volume-kline-audit", str(run_dir / f"volume_kline_coverage_audit_{hours}h.json"),
                "--context-blocker-monitor", str(run_dir / f"context_blocker_monitor_{hours}h.json"),
                "--hours", str(hours),
                "--out", str(run_dir / f"a_class_fastlane_mode_audit_{hours}h.json"),
            ],
            run_dir / f"a_class_fastlane_mode_audit_{hours}h.json",
            timeout=int(args.report_timeout_sec),
        ),
        run_report(
            "runtime_health_snapshot_final",
            [
                "scripts/runtime_health_snapshot_audit.py",
                "--data-dir", args.data_dir,
                "--hours", str(hours),
                "--out", str(run_dir / f"runtime_health_snapshot_{hours}h.json"),
            ],
            run_dir / f"runtime_health_snapshot_{hours}h.json",
            timeout=int(args.report_timeout_sec),
        ),
    ]
    append_diagnostics(run_dir, rows)
    update_stage_state(run_dir, "decision")
    return {"stage": "decision", "diagnostics": rows}


def stage_strategy(args, run_dir):
    hours = int(args.hours)
    path = run_dir / "strategy_memory_validation_24h.json"
    rows = [run_report(
        "strategy_memory_validation",
        [
            "scripts/strategy_memory_validation.py",
            "--registry", str(args.registry),
            "--ingestion-summary", str(run_dir / "strategy_memory_ingestion_summary.json"),
            "--capture-24h", str(run_dir / "capture_discovery_24h.json"),
            "--capture-48h", str(run_dir / "capture_discovery_48h.json"),
            "--capture-72h", str(run_dir / "capture_discovery_72h.json"),
            "--downstream", str(run_dir / f"candidate_downstream_readiness_{hours}h.json"),
            "--a-class", str(run_dir / f"a_class_fastlane_mode_audit_{hours}h.json"),
            "--pnl", str(run_dir / f"pnl_cross_secondary_{hours}h.json"),
            "--filtered-winner", str(Path(args.data_dir) / "filtered_winner_dossier_24h.json"),
            "--exit-report", str(Path(args.data_dir) / "exit_policy_shadow_simulator_24h.json"),
            "--delay-report", str(Path(args.data_dir) / "execution_delay_adjusted_replay_24h.json"),
            "--out", str(path),
            "--filtered-bridge-out", str(run_dir / "strategy_memory_filtered_winner_bridge.json"),
            "--exit-summary-out", str(run_dir / "strategy_memory_exit_shadow_summary.json"),
            "--delay-summary-out", str(run_dir / "strategy_memory_delay_replay_summary.json"),
        ],
        path,
        timeout=int(args.report_timeout_sec),
    )]
    rows.append(run_report(
        "clean_dimension_2d_capture_cross_audit",
        [
            "scripts/clean_dimension_2d_capture_cross_audit.py",
            "--db", args.paper_db,
            "--raw-db", args.raw_db,
            "--strategy-memory", str(path),
            "--context-eligibility", str(run_dir / "context_dimension_eligibility.json"),
            "--hours", str(hours),
            "--expected-candidates", str(args.expected_candidates),
            "--out", str(run_dir / f"clean_dimension_2d_capture_cross_{hours}h.json"),
            "--quality-out", str(run_dir / f"quality_timing_reason_cross_{hours}h.json"),
            "--strategy-out", str(run_dir / f"strategy_memory_reason_cross_{hours}h.json"),
        ],
        run_dir / f"clean_dimension_2d_capture_cross_{hours}h.json",
        timeout=max(60, int(args.report_timeout_sec) * 2),
    ))
    append_diagnostics(run_dir, rows)
    update_stage_state(run_dir, "strategy")
    return {"stage": "strategy", "diagnostics": rows}


def stage_oos(args, run_dir):
    """Refresh read-only OOS probes from the frozen hypothesis registry.

    This stage intentionally runs before finalize and uses the current registry
    as the frozen hypothesis source. It does not promote candidates or change
    runtime behavior.
    """
    freeze_registry = run_dir / "pass_allow_60_oos_freeze_registry.json"
    if not freeze_registry.exists():
        latest_registry = Path(args.out_root) / "latest" / "pass_allow_60_oos_freeze_registry.json"
        if latest_registry.exists():
            freeze_registry = latest_registry
    capture_cross_freeze_registry = run_dir / "capture_cross_oos_freeze_registry.json"
    if not capture_cross_freeze_registry.exists():
        latest_capture_cross_registry = (
            Path(args.out_root) / "latest" / "capture_cross_oos_freeze_registry.json"
        )
        if latest_capture_cross_registry.exists():
            capture_cross_freeze_registry = latest_capture_cross_registry
    rows = [
        run_report(
            "oos_readiness_probe_refresh",
            [
                "scripts/refresh_oos_readiness_probes.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--kline-db", args.kline_db,
                "--registry", args.registry,
                "--run-dir", str(run_dir),
                "--probe-hours", str(args.oos_probe_hours or ""),
                "--expected-candidates", str(args.expected_candidates),
                "--max-scan-rows", str(args.max_scan_rows),
                "--timeout-sec", str(args.report_timeout_sec),
                "--post-freeze-probe",
                "--out", str(run_dir / "oos_readiness_probe_refresh.json"),
            ],
            run_dir / "oos_readiness_probe_refresh.json",
            timeout=max(60, int(args.report_timeout_sec) * 3),
        ),
        run_report(
            "pass_allow_60_post_freeze_oos_validation",
            [
                "scripts/pass_allow_60_post_freeze_oos_validation.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--freeze-registry", str(freeze_registry),
                "--expected-candidates", str(args.expected_candidates),
                "--out", str(run_dir / "pass_allow_60_post_freeze_oos_validation.json"),
            ],
            run_dir / "pass_allow_60_post_freeze_oos_validation.json",
            timeout=max(60, int(args.report_timeout_sec) * 2),
        ),
        run_report(
            "capture_cross_post_freeze_oos_validation",
            [
                "scripts/capture_cross_post_freeze_oos_validation.py",
                "--db", args.paper_db,
                "--raw-db", args.raw_db,
                "--kline-db", args.kline_db,
                "--freeze-registry", str(capture_cross_freeze_registry),
                "--expected-candidates", str(args.expected_candidates),
                "--out", str(run_dir / "capture_cross_post_freeze_oos_validation.json"),
            ],
            run_dir / "capture_cross_post_freeze_oos_validation.json",
            timeout=max(60, int(args.report_timeout_sec) * 2),
        ),
    ]
    append_diagnostics(run_dir, rows)
    update_stage_state(run_dir, "oos")
    return {"stage": "oos", "diagnostics": rows}


def collect_paths(args, run_dir):
    hours = int(args.hours)
    markov_paths = {}
    for profile in [item.strip() for item in str(args.markov_profiles or "").split(",") if item.strip()]:
        path = run_dir / f"candidate_virtual_markov_{profile}_{hours}h.json"
        if path.exists():
            markov_paths[profile] = path
    readiness_names = {
        "runtime_health_snapshot": f"runtime_health_snapshot_{hours}h.json",
        "candidate_downstream_readiness": f"candidate_downstream_readiness_{hours}h.json",
        "context_coverage": f"context_coverage_audit_{hours}h.json",
        "candidate_effectiveness": f"candidate_effectiveness_{hours}h.json",
        "markov_effectiveness": f"markov_effectiveness_{hours}h.json",
        "capture_cross_validity": f"capture_cross_validity_{hours}h.json",
        "candidate_improvement_opportunities": f"candidate_improvement_opportunities_{hours}h.json",
        "shadow_decision_bridge_audit": f"shadow_decision_bridge_audit_{hours}h.json",
        "a_class_fastlane_mode_audit": f"a_class_fastlane_mode_audit_{hours}h.json",
        "legacy_candidate_capture": f"candidate_capture_discovery_{hours}h.json",
        "raw_gold_silver_funnel_audit": f"raw_gold_silver_funnel_audit_{hours}h.json",
        "volume_kline_coverage_audit": f"volume_kline_coverage_audit_{hours}h.json",
        "matured_kline_volume_recheck_audit": f"matured_kline_volume_recheck_audit_{hours}h.json",
        "matured_volume_capture_cross_audit": f"matured_volume_capture_cross_audit_{hours}h.json",
        "hypothesis_validation_audit": f"hypothesis_validation_audit_{hours}h.json",
        "low_confidence_research_capture_audit": f"low_confidence_research_capture_audit_{hours}h.json",
        "kline_coverage_resolution_audit": f"kline_coverage_resolution_audit_{hours}h.json",
        "quality_timing_reject_research_audit": f"quality_timing_reject_research_audit_{hours}h.json",
        "context_blocker_monitor": f"context_blocker_monitor_{hours}h.json",
        "decision_no_pass_quality_timing_watch_validation": f"decision_no_pass_quality_timing_watch_validation_{hours}h.json",
        "pending_momentum_decay_recheck_validation": f"pending_momentum_decay_recheck_validation_{hours}h.json",
        "pending_stale_before_final_watch_validation": f"pending_stale_before_final_watch_validation_{hours}h.json",
        "strategy_memory_validation": "strategy_memory_validation_24h.json",
        "strategy_memory_filtered_winner_bridge": "strategy_memory_filtered_winner_bridge.json",
        "strategy_memory_exit_shadow_summary": "strategy_memory_exit_shadow_summary.json",
        "strategy_memory_delay_replay_summary": "strategy_memory_delay_replay_summary.json",
        "strategy_memory_ingestion_summary": "strategy_memory_ingestion_summary.json",
        "clean_dimension_2d_capture_cross": f"clean_dimension_2d_capture_cross_{hours}h.json",
        "quality_timing_reason_cross": f"quality_timing_reason_cross_{hours}h.json",
        "strategy_memory_reason_cross": f"strategy_memory_reason_cross_{hours}h.json",
        "oos_readiness_probe_refresh": "oos_readiness_probe_refresh.json",
        "pass_allow_60_post_freeze_oos_validation": "pass_allow_60_post_freeze_oos_validation.json",
        "capture_cross_post_freeze_oos_validation": "capture_cross_post_freeze_oos_validation.json",
        "decision_capture_60_gap_audit": "decision_capture_60_gap_audit.json",
        "pass_allow_60_closure_plan": "pass_allow_60_closure_plan.json",
        "pass_allow_60_oos_freeze_registry": "pass_allow_60_oos_freeze_registry.json",
        "pass_allow_60_oos_readiness_monitor": "pass_allow_60_oos_readiness_monitor.json",
        "capture_cross_oos_freeze_registry": "capture_cross_oos_freeze_registry.json",
    }
    readiness = {}
    for key, filename in readiness_names.items():
        path = run_dir / filename
        if path.exists():
            readiness[key] = path
    for capture_hours in parse_capture_hours(args.capture_hours, hours):
        path = run_dir / f"capture_discovery_{capture_hours}h.json"
        if path.exists():
            readiness[f"capture_{capture_hours}h"] = path
    return markov_paths, readiness


def stage_finalize(args, run_dir):
    hours = int(args.hours)
    capture_path = run_dir / f"capture_discovery_{hours}h.json"
    pnl_path = run_dir / f"pnl_cross_secondary_{hours}h.json"
    tests_path = run_dir / "tests.json"
    tests = load_json_default(tests_path, {
        "schema_version": "agent_capture_discovery_tests.v1",
        "generated_at": utc_now(),
        "passed": False,
        "status": "missing",
        "results": [],
    })
    markov_paths, readiness_paths = collect_paths(args, run_dir)
    diagnostics = load_diagnostics(run_dir)
    verdict, registry, verdict_path, summary_path, handoff_path, _ = write_materialized_artifacts(
        args_namespace(args),
        rid=run_dir.name,
        run_dir=run_dir,
        latest_dir=Path(args.out_root) / "latest",
        handoff_dir=Path(args.handoff_dir),
        capture_path=capture_path,
        pnl_path=pnl_path if pnl_path.exists() else None,
        markov_paths=markov_paths,
        readiness_paths=readiness_paths,
        diagnostics=diagnostics,
        tests=tests,
        state="final",
        publish_latest=False,
        refresh_oos_after_registry=False,
    )
    eligibility = final_publish_eligibility(run_dir)
    published = False
    sync_skipped_same_latest = False
    latest_dir = Path(args.out_root) / "latest"
    if eligibility["eligible"]:
        if run_dir.resolve() == latest_dir.resolve():
            sync_skipped_same_latest = True
            shutil.copy2(handoff_path, latest_dir / "latest_codex_handoff.md")
            Path(args.handoff_dir).mkdir(parents=True, exist_ok=True)
            shutil.copy2(handoff_path, Path(args.handoff_dir) / "latest_codex_handoff.md")
            published = True
        else:
            sync_latest(run_dir, latest_dir, Path(args.handoff_dir), verdict_path, summary_path, handoff_path)
            published = True
    update_stage_state(
        run_dir,
        "finalize",
        classification=verdict.get("classification"),
        promotion_allowed=verdict.get("promotion_allowed"),
        publish_latest=published,
        publish_eligibility=eligibility,
        sync_skipped_same_latest=sync_skipped_same_latest,
    )
    return {
        "stage": "finalize",
        "classification": verdict.get("classification"),
        "promotion_allowed": verdict.get("promotion_allowed"),
        "publish_latest": published,
        "publish_eligibility": eligibility,
        "sync_skipped_same_latest": sync_skipped_same_latest,
        "decision_no_pass_watch_count": len(registry.get("shadow_only_decision_no_pass_quality_timing_watch") or []),
    }


STAGES = {
    "init": stage_init,
    "selftests": stage_selftests,
    "capture": stage_capture,
    "core": stage_core,
    "markov": stage_markov,
    "derived": stage_derived,
    "context": stage_context,
    "decision": stage_decision,
    "strategy": stage_strategy,
    "oos": stage_oos,
    "finalize": stage_finalize,
}

DEFAULT_SEQUENCE = (
    "init",
    "selftests",
    "capture",
    "core",
    "markov",
    "derived",
    "context",
    "decision",
    "strategy",
    "oos",
    "finalize",
)
DEFAULT_CONTEXT_REPORTS = (
    "volume",
    "matured_recheck",
    "matured_cross",
    "low_confidence",
    "resolution",
    "hypothesis",
)


def parse_stages(value):
    if value == "all":
        return list(DEFAULT_SEQUENCE)
    stages = [item.strip() for item in str(value or "").split(",") if item.strip()]
    unknown = [stage for stage in stages if stage not in STAGES]
    if unknown:
        raise SystemExit(f"unknown stage(s): {', '.join(unknown)}")
    return stages


def parse_capture_hours_exact(value, primary_hours):
    hours = []
    for item in str(value or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            parsed = int(float(item))
        except ValueError:
            continue
        if parsed > 0:
            hours.append(parsed)
    if not hours:
        hours.append(int(primary_hours))
    return sorted(set(hours))


def parse_stage_capture_hours(value, primary_hours, capture_exact=False):
    if capture_exact:
        return parse_capture_hours_exact(value, primary_hours)
    return parse_capture_hours(value, primary_hours)


def parse_context_reports(value):
    selected = [item.strip() for item in str(value or "").split(",") if item.strip()]
    if not selected or "all" in selected:
        return set(DEFAULT_CONTEXT_REPORTS)
    aliases = {
        "volume_kline": "volume",
        "volume_kline_coverage": "volume",
        "matured_kline_volume_recheck": "matured_recheck",
        "matured_volume": "matured_cross",
        "matured_volume_cross": "matured_cross",
        "low_confidence_research": "low_confidence",
        "kline_resolution": "resolution",
        "hypothesis_validation": "hypothesis",
    }
    reports = set()
    for item in selected:
        reports.add(aliases.get(item, item))
    unknown = sorted(reports - set(DEFAULT_CONTEXT_REPORTS))
    if unknown:
        raise SystemExit(f"Unknown context report(s): {','.join(unknown)}")
    return reports


def self_test():
    assert parse_stage_capture_hours("48", 24, False) == [24, 48]
    assert parse_stage_capture_hours("48", 24, True) == [48]
    assert parse_stage_capture_hours("", 24, True) == [24]
    assert parse_context_reports("") == set(DEFAULT_CONTEXT_REPORTS)
    assert parse_context_reports("all") == set(DEFAULT_CONTEXT_REPORTS)
    assert parse_context_reports("volume_kline,matured_volume,hypothesis_validation") == {
        "volume",
        "matured_cross",
        "hypothesis",
    }
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        create_self_test_dbs(root)
        run_dir = root / "agent_runs" / "staged_self_test"
        args = argparse.Namespace(
            paper_db=str(root / "paper.db"),
            raw_db=str(root / "raw.db"),
            kline_db=str(root / "kline.db"),
            data_dir=str(root),
            strategy_memory_dir=str(root / "strategy_memory_local"),
            hours=24,
            capture_hours="24",
            capture_exact=False,
            expected_candidates=2,
            out_root=str(root / "agent_runs"),
            handoff_dir=str(root / "agent_handoffs"),
            registry=str(root / "hypothesis_registry.json"),
            markov_profiles="runtime,kline",
            context_reports="all",
            report_timeout_sec=60,
            test_timeout_sec=60,
            max_scan_rows=10000,
            oos_probe_hours="",
            quote_fix_deploy_ts=0,
            run_id="staged_self_test",
            run_dir=str(run_dir),
            reset=True,
        )
        for stage in ("init", "selftests", "capture", "core", "markov", "derived"):
            STAGES[stage](args, run_dir)
        assert not (run_dir / "a_class_fastlane_mode_audit_24h.json").exists()
        assert (run_dir / "a_class_fastlane_mode_audit_24h_derived.json").exists()
        derived_only_finalize = stage_finalize(args, run_dir)
        assert derived_only_finalize["publish_latest"] is False
        assert "a_class_stage2_summary_missing_while_capture_denominator_positive" in (
            derived_only_finalize["publish_eligibility"]["blockers"]
        )
        for stage in ("context", "decision", "strategy", "oos", "finalize"):
            STAGES[stage](args, run_dir)
        latest = root / "agent_runs" / "latest"
        assert (latest / "reviewer_verdict.json").exists()
        assert (latest / "capture_60_gap_report.json").exists()
        assert (latest / "latest_codex_handoff.md").exists()
        assert (root / "agent_handoffs" / "latest_codex_handoff.md").exists()
        latest_verdict = load_json(latest / "reviewer_verdict.json")
        assert latest_verdict["promotion_allowed"] is False
        assert latest_verdict["decision_capture_60_gap_audit"]["available"] is True
        assert latest_verdict["decision_capture_60_gap_classification"] == (
            latest_verdict["decision_capture_60_gap_audit"]["classification"]
        )
        assert load_json(root / "hypothesis_registry.json")["promotion_allowed"] is False
        (latest / "latest_codex_handoff.md").write_text("STALE_LATEST_ALIAS", encoding="utf-8")
        (root / "agent_handoffs" / "latest_codex_handoff.md").write_text("STALE_GLOBAL_ALIAS", encoding="utf-8")
        same_latest_finalize = stage_finalize(args, latest)
        assert same_latest_finalize["publish_latest"] is True
        assert same_latest_finalize["sync_skipped_same_latest"] is True
        assert "STALE_LATEST_ALIAS" not in (latest / "latest_codex_handoff.md").read_text(encoding="utf-8")
        assert "STALE_GLOBAL_ALIAS" not in (
            root / "agent_handoffs" / "latest_codex_handoff.md"
        ).read_text(encoding="utf-8")
    print("SELF_TEST_PASS agent_autoloop_stage_runner")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paper-db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--kline-db", default="/app/data/kline_cache.db")
    parser.add_argument("--data-dir", default="/app/data")
    parser.add_argument("--strategy-memory-dir", default=None)
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--capture-hours", default="24")
    parser.add_argument(
        "--capture-exact",
        action="store_true",
        help=(
            "For --stage capture, run exactly --capture-hours instead of always "
            "including the primary --hours window. Read-only helper for avoiding 524s."
        ),
    )
    parser.add_argument("--expected-candidates", type=int, default=84)
    parser.add_argument("--out-root", default=DEFAULT_OUT_ROOT)
    parser.add_argument("--handoff-dir", default=DEFAULT_HANDOFF_DIR)
    parser.add_argument("--registry", default=DEFAULT_REGISTRY)
    parser.add_argument("--markov-profiles", default=DEFAULT_MARKOV_PROFILES)
    parser.add_argument(
        "--context-reports",
        default="all",
        help=(
            "Comma-separated context subreports for --stage context. "
            "Use all, volume, matured_recheck, matured_cross, low_confidence, "
            "resolution, hypothesis. This is read-only and helps avoid 524s."
        ),
    )
    parser.add_argument("--report-timeout-sec", type=int, default=600)
    parser.add_argument("--test-timeout-sec", type=int, default=120)
    parser.add_argument("--max-scan-rows", type=int, default=2_000_000)
    parser.add_argument("--oos-probe-hours", default="")
    parser.add_argument("--quote-fix-deploy-ts", type=int, default=0)
    parser.add_argument("--run-id", default="staged_current")
    parser.add_argument("--run-dir", default=None)
    parser.add_argument("--stage", default="all")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        self_test()
        return
    run_dir = run_dir_for(args)
    run_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for stage in parse_stages(args.stage):
        result = STAGES[stage](args, run_dir)
        results.append(result)
        print(json.dumps({"schema_version": SCHEMA_VERSION, **result}, sort_keys=True), flush=True)
    print(json.dumps({
        "schema_version": SCHEMA_VERSION,
        "run_dir": str(run_dir),
        "stages": [row.get("stage") for row in results],
        "last_result": results[-1] if results else None,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
