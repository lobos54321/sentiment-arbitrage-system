#!/usr/bin/env python3
"""Lightweight AutoLoop artifact reconciliation.

Use this after a remote full AutoLoop execution times out, for example through
an HTTP 524 / gateway timeout. The script is read-only with respect to trading:
it only rebuilds evaluator artifacts from existing data and never changes
strategy, gates, final_entry_contract, A_CLASS, executor, wallet, canary, or
risk.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from agent_capture_discovery_loop import (
    build_strategy_memory_ingestion_summary,
    compact_strategy_memory_registry,
    load_json,
    update_hypothesis_registry,
    write_json,
)
from generate_codex_handoff import build_handoff, write_text
from review_agent_verdict import build_verdict, load_sibling_readiness_reports
from strategy_memory_validation import (
    build_delay_replay_summary,
    build_exit_shadow_summary,
    build_filtered_winner_bridge,
    build_validation,
)


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def safe_load(path, default=None):
    try:
        return load_json(path)
    except Exception:
        return default


def run_capture_60_target_loop(run_dir, timeout_sec=180):
    """Rebuild target-loop artifacts from already materialized reports."""
    run_path = Path(run_dir)
    result = {
        "name": "capture_60_target_loop_reconciliation",
        "ok": False,
        "returncode": None,
        "duration_sec": None,
        "skipped": False,
    }
    if not (run_path / "capture_discovery_24h.json").exists():
        result["skipped"] = True
        result["reason"] = "capture_discovery_24h_missing"
        return result
    started = time.time()
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "capture_60_target_loop.py"),
        "--run-dir",
        str(run_path),
        "--out-dir",
        str(run_path),
    ]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(SCRIPT_DIR.parent),
            text=True,
            capture_output=True,
            timeout=timeout_sec,
            check=False,
        )
        result.update({
            "ok": proc.returncode == 0,
            "returncode": proc.returncode,
            "stdout_tail": (proc.stdout or "")[-4000:],
            "stderr_tail": (proc.stderr or "")[-4000:],
        })
    except subprocess.TimeoutExpired as exc:
        result.update({
            "ok": False,
            "returncode": "timeout",
            "stdout_tail": (exc.stdout or "")[-4000:] if isinstance(exc.stdout, str) else "",
            "stderr_tail": (exc.stderr or "")[-4000:] if isinstance(exc.stderr, str) else "",
            "error": "timeout_expired",
        })
    result["duration_sec"] = round(time.time() - started, 3)
    return result


def publish_latest(run_dir, latest_dir, handoff_out):
    """Publish a reconciled partial run as the latest read-only artifact set."""
    source = Path(run_dir)
    target = Path(latest_dir)
    target.mkdir(parents=True, exist_ok=True)
    for report in source.glob("*.json"):
        shutil.copy2(report, target / report.name)
    for name in ("run_summary.md", "codex_handoff.md"):
        path = source / name
        if path.exists():
            shutil.copy2(path, target / name)
    handoff_path = Path(handoff_out)
    run_handoff = source / "codex_handoff.md"
    if run_handoff.exists():
        handoff_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(run_handoff, handoff_path)
    return {
        "published": True,
        "source_run_dir": str(source),
        "latest_dir": str(target),
        "handoff_out": str(handoff_path),
    }


def update_run_summary(path, verdict):
    target = Path(path)
    old = target.read_text(encoding="utf-8") if target.exists() else "# Gold/Silver Capture AutoLoop Summary\n"
    status_section = (
        "# Gold/Silver Capture AutoLoop Reconciliation Status\n\n"
        f"- generated_at: `{utc_now()}`\n"
        f"- classification: `{verdict.get('classification')}`\n"
        f"- autoloop_execution_status: `{verdict.get('autoloop_execution_status')}`\n"
        f"- current_commit: `{verdict.get('current_commit')}`\n"
        f"- deployment_commit: `{verdict.get('deployment_commit')}`\n"
        f"- next_action: `{verdict.get('next_action')}`\n"
        f"- promotion_allowed: `{str(bool(verdict.get('promotion_allowed'))).lower()}`\n"
        f"- strategy_failure_inferred: `{str(bool(verdict.get('strategy_failure_inferred'))).lower()}`\n"
        "\n"
    )
    strategy_memory_section = {
        "strategy_memory": verdict.get("strategy_memory") or {},
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
        "autoloop_execution_status": verdict.get("autoloop_execution_status"),
        "timeout_reconciliation": verdict.get("timeout_reconciliation"),
    }
    section = (
        "\n## Strategy Memory v2.1 Validation\n\n```json\n"
        + json.dumps(strategy_memory_section, ensure_ascii=False, indent=2, sort_keys=True)[:12000]
        + "\n```\n"
    )
    marker = "\n## Strategy Memory v2.1 Validation\n"
    if marker in old:
        new_text = old.split(marker, 1)[0].rstrip() + section
    else:
        new_text = old.rstrip() + section
    status_marker = "# Gold/Silver Capture AutoLoop Reconciliation Status\n"
    if status_marker in new_text:
        remainder = new_text.split(status_marker, 1)[1]
        if "\n# Gold/Silver Capture" in remainder:
            remainder = "# Gold/Silver Capture" + remainder.split("\n# Gold/Silver Capture", 1)[1]
        else:
            remainder = ""
        new_text = status_section.rstrip() + "\n\n" + remainder.lstrip()
    else:
        new_text = status_section.rstrip() + "\n\n" + new_text.lstrip()
    write_text(target, new_text + "\n")


def build_validation_args(args):
    run_dir = Path(args.run_dir)
    data_dir = Path(args.data_dir)
    return argparse.Namespace(
        registry=args.registry,
        ingestion_summary=str(run_dir / "strategy_memory_ingestion_summary.json"),
        capture_24h=str(run_dir / "capture_discovery_24h.json"),
        capture_48h=str(run_dir / "capture_discovery_48h.json"),
        capture_72h=str(run_dir / "capture_discovery_72h.json"),
        downstream=str(run_dir / "candidate_downstream_readiness_24h.json"),
        a_class=str(run_dir / "a_class_fastlane_mode_audit_24h.json"),
        pnl=str(run_dir / "pnl_cross_secondary_24h.json"),
        filtered_winner=str(data_dir / "filtered_winner_dossier_24h.json"),
        exit_report=str(data_dir / "exit_policy_shadow_simulator_24h.json"),
        delay_report=str(data_dir / "execution_delay_adjusted_replay_24h.json"),
    )


def reconcile(args):
    run_dir = Path(args.run_dir)
    data_dir = Path(args.data_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    previous_verdict = safe_load(run_dir / "reviewer_verdict.json", {})

    ingestion_args = argparse.Namespace(
        data_dir=str(data_dir),
        strategy_memory_dir=args.strategy_memory_dir,
        paper_db=args.paper_db,
    )
    ingestion_path = run_dir / "strategy_memory_ingestion_summary.json"
    ingestion = build_strategy_memory_ingestion_summary(ingestion_args, ingestion_path)

    registry_path = Path(args.registry)
    registry = safe_load(registry_path, {}) or {}
    registry["strategy_memory"] = compact_strategy_memory_registry(ingestion)
    write_json(registry_path, registry)

    validation_args = build_validation_args(args)
    validation = build_validation(validation_args)
    validation_path = run_dir / "strategy_memory_validation_24h.json"
    write_json(validation_path, validation)
    filtered_bridge = build_filtered_winner_bridge(validation_args, validation)
    filtered_bridge_path = run_dir / "strategy_memory_filtered_winner_bridge.json"
    write_json(filtered_bridge_path, filtered_bridge)
    exit_summary = build_exit_shadow_summary(validation_args, validation)
    exit_summary_path = run_dir / "strategy_memory_exit_shadow_summary.json"
    write_json(exit_summary_path, exit_summary)
    delay_summary = build_delay_replay_summary(validation_args, validation)
    delay_summary_path = run_dir / "strategy_memory_delay_replay_summary.json"
    write_json(delay_summary_path, delay_summary)
    capture_60_reconciliation = run_capture_60_target_loop(run_dir)

    capture_path = run_dir / "capture_discovery_24h.json"
    pnl_path = run_dir / "pnl_cross_secondary_24h.json"
    markov_reports = {}
    for profile in ("runtime", "kline"):
        path = run_dir / f"candidate_virtual_markov_{profile}_24h.json"
        if path.exists():
            markov_reports[profile] = load_json(path)
    readiness_reports = load_sibling_readiness_reports(str(capture_path), {})
    verdict = build_verdict(
        load_json(capture_path),
        load_json(pnl_path) if pnl_path.exists() else None,
        markov_reports,
        tests={},
        readiness_reports=readiness_reports,
    )
    if args.timeout_partial_sync:
        preserved = {
            "classification": previous_verdict.get("classification"),
            "blockers": previous_verdict.get("blockers"),
            "next_action": previous_verdict.get("next_action"),
            "promotion_allowed": previous_verdict.get("promotion_allowed"),
            "generated_at": previous_verdict.get("generated_at"),
        }
        verdict["classification"] = "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC"
        verdict["blockers"] = sorted(set((verdict.get("blockers") or []) + ["autoloop_execution_timeout_partial_sync"]))
        verdict["autoloop_execution_status"] = "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC"
        verdict["strategy_failure_inferred"] = False
        verdict["promotion_allowed"] = False
        verdict["timeout_reconciliation"] = {
            "schema_version": "autoloop_timeout_reconciliation.v1",
            "generated_at": utc_now(),
            "status": "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC",
            "source": args.timeout_source,
            "strategy_failure_inferred": False,
            "lightweight_artifact_reconciliation_done": True,
            "preserve_previous_valid_reviewer_verdict": True,
            "preserved_reviewer_verdict": preserved,
            "handoff_only_if_timeout_repeats": True,
            "promotion_allowed": False,
        }
        verdict["next_action"] = "full_autoloop_timeout_reconciled_continue_previous_valid_plan"
    else:
        verdict["autoloop_execution_status"] = "LIGHTWEIGHT_RECONCILIATION_COMPLETED"
    verdict.setdefault("loop", {})
    verdict["loop"]["capture_60_target_reconciliation"] = capture_60_reconciliation
    verdict_path = run_dir / "reviewer_verdict.json"
    write_json(verdict_path, verdict)
    capture = load_json(capture_path) if capture_path.exists() else {}
    registry = update_hypothesis_registry(args.registry, verdict, capture, ingestion)

    handoff_path = Path(args.handoff_out)
    handoff_text = build_handoff(verdict)
    write_text(handoff_path, handoff_text)
    write_text(run_dir / "codex_handoff.md", handoff_text)
    summary_path = run_dir / "run_summary.md"
    update_run_summary(summary_path, verdict)
    publish_result = None
    if args.publish_latest:
        publish_result = publish_latest(run_dir, args.latest_dir, args.handoff_out)
    return {
        "schema_version": "autoloop_lightweight_reconciliation.v1",
        "generated_at": utc_now(),
        "run_dir": str(run_dir),
        "strategy_memory_validation_out": str(validation_path),
        "strategy_memory_filtered_winner_bridge_out": str(filtered_bridge_path),
        "strategy_memory_exit_shadow_summary_out": str(exit_summary_path),
        "strategy_memory_delay_replay_summary_out": str(delay_summary_path),
        "reviewer_verdict_out": str(verdict_path),
        "handoff_out": str(handoff_path),
        "run_summary_out": str(summary_path),
        "capture_60_target_reconciliation": capture_60_reconciliation,
        "publish_latest": publish_result,
        "classification": verdict.get("classification"),
        "autoloop_execution_status": verdict.get("autoloop_execution_status"),
        "strategy_failure_inferred": verdict.get("strategy_failure_inferred", False),
        "strategy_memory_status_counts": validation.get("status_counts") or {},
        "decision_no_pass_watch_count": len(registry.get("shadow_only_decision_no_pass_quality_timing_watch") or []),
        "promotion_allowed": False,
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        data_dir = root / "data"
        run_dir = data_dir / "agent_runs" / "latest"
        run_dir.mkdir(parents=True)
        registry = root / "hypothesis_registry.json"
        write_json(registry, {})
        strategy_dir = data_dir / "strategy_memory_local"
        strategy_dir.mkdir(parents=True)
        write_json(strategy_dir / "strategy_memory_hypotheses.json", {
            "hypotheses_count": 1,
            "hypotheses": [
                {
                    "id": "SM-EXECUTION-DELAY-SENSITIVITY",
                    "name": "delay",
                    "strategy_family": "Execution delay sensitivity tests",
                    "priority": 1,
                    "future_or_posthoc_features": ["best_delay_after_peak"],
                }
            ],
        })
        write_json(strategy_dir / "strategy_memory_candidate_mapping.json", {
            "mapped_to_existing_candidates": 1,
            "missing_shadow_candidates": 1,
            "rejected_future_data_hypotheses": 1,
            "mappings": [
                {
                    "hypothesis_id": "SM-EXECUTION-DELAY-SENSITIVITY",
                    "mapping_status": "missing_delay_replay_only",
                    "existing_candidate_ids": ["candidate:a"],
                    "blocked_contexts": ["future_data_features_must_be_labels_only"],
                }
            ],
        })
        write_json(data_dir / "execution_delay_adjusted_replay_24h.json", {
            "delay_replay_done": True,
            "entry_delays_sec": [0, 15],
            "hypotheses": [{"hypothesis_id": "SM-EXECUTION-DELAY-SENSITIVITY", "metrics_by_delay": []}],
        })
        write_json(data_dir / "exit_policy_shadow_simulator_24h.json", {"exit_policy_variants_tested": 0, "variants": []})
        write_json(data_dir / "filtered_winner_dossier_24h.json", {"filtered_winner_count": 0, "missed_winners": []})
        capture = {
            "schema_version": "offline_candidate_capture_discovery.v1",
            "raw_gold_silver_denominator": {"event_rows": 3, "unique_tokens": 3, "rows_complete_against_summary": True},
            "coverage": {"candidate_count_observed": 84, "coverage_pct": 100},
            "report_health": {"promotion_blockers": []},
            "judgment_counts": {},
            "candidate_baseline": [{"candidate_id": "candidate:a", "match_recall_event": 1.0, "match_precision_event": 0.5}],
        }
        for hours in (24, 48, 72):
            write_json(run_dir / f"capture_discovery_{hours}h.json", capture)
        write_json(run_dir / "candidate_downstream_readiness_24h.json", {"all_candidates": []})
        write_json(run_dir / "a_class_fastlane_mode_audit_24h.json", {"capture_stage_rates": {}})
        write_json(run_dir / "pnl_cross_secondary_24h.json", {"baseline": []})
        write_json(run_dir / "raw_gold_silver_funnel_audit_24h.json", {
            "raw_gold_silver_events": 3,
            "candidate_matched_any": 3,
            "has_decision_record": 2,
            "pass_allow": 1,
            "pending_entry": 1,
            "reached_final_entry_contract": 0,
            "paper_trade_intent": 0,
            "paper_trade_committed": 0,
        })
        write_json(run_dir / "reviewer_verdict.json", {"classification": "BLOCKED_CONTEXT_COVERAGE", "promotion_allowed": False})
        latest_dir = data_dir / "agent_runs" / "latest_published"
        args = argparse.Namespace(
            data_dir=str(data_dir),
            run_dir=str(run_dir),
            registry=str(registry),
            strategy_memory_dir=str(strategy_dir),
            paper_db=str(root / "paper.db"),
            handoff_out=str(data_dir / "agent_handoffs" / "latest_codex_handoff.md"),
            latest_dir=str(latest_dir),
            publish_latest=True,
            timeout_partial_sync=True,
            timeout_source="self_test_524",
        )
        result = reconcile(args)
        assert result["classification"] == "AUTOLOOP_EXEC_TIMEOUT_PARTIAL_SYNC"
        assert result["strategy_failure_inferred"] is False
        assert result["capture_60_target_reconciliation"]["ok"] is True
        assert result["publish_latest"]["published"] is True
        verdict = load_json(run_dir / "reviewer_verdict.json")
        assert verdict["timeout_reconciliation"]["lightweight_artifact_reconciliation_done"] is True
        assert (latest_dir / "reviewer_verdict.json").exists()
        assert (latest_dir / "run_summary.md").read_text(encoding="utf-8").startswith(
            "# Gold/Silver Capture AutoLoop Reconciliation Status"
        )
        validation = load_json(run_dir / "strategy_memory_validation_24h.json")
        assert validation["hypotheses"][0]["window_validation_count"] == 3
        assert load_json(registry)["strategy_memory"]["hypotheses"][0]["route"] == "delay_replay_only"
    print("SELF_TEST_PASS autoloop_lightweight_reconciliation")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="/app/data")
    parser.add_argument("--run-dir", default="/app/data/agent_runs/latest")
    parser.add_argument("--registry", default="/app/data/hypothesis_registry.json")
    parser.add_argument("--strategy-memory-dir", default=None)
    parser.add_argument("--paper-db", default="/app/data/paper_trades.db")
    parser.add_argument("--handoff-out", default="/app/data/agent_handoffs/latest_codex_handoff.md")
    parser.add_argument("--latest-dir", default="/app/data/agent_runs/latest")
    parser.add_argument("--publish-latest", action="store_true")
    parser.add_argument("--timeout-partial-sync", action="store_true")
    parser.add_argument("--timeout-source", default="remote_full_autoloop_timeout_or_524")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    print(json.dumps(reconcile(args), sort_keys=True))


if __name__ == "__main__":
    main()
