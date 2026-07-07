#!/usr/bin/env python3
"""Generate the Phase 3 goal loop and P7 paper-proposal checkpoint artifacts.

This script is deliberately read-only with respect to strategy/runtime behavior. It writes
planning artifacts only. It must not enable paper experiments, change exits, change gates, or
alter risk settings.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "phase3_goal_loop.v1"
PROPOSAL_SCHEMA_VERSION = "p7_paper_proposal_checkpoint.v1"
DEFAULT_DATA_DIR = Path("/app/data")
DEFAULT_RUN_DIR = DEFAULT_DATA_DIR / "agent_runs/latest"
P7_CHAMPION_POLICY_ID = "trail_a50_dd15_stop20"


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path: Path) -> dict:
    try:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except Exception as exc:  # pragma: no cover - defensive artifact handling
        return {"error": str(exc), "path": str(path)}


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def write_json(path: Path, payload: dict) -> None:
    write_text(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def p7_passed(p7: dict) -> bool:
    return (
        p7.get("classification") == "P7_EXIT_POLICY_OOS_PASSED_PENDING_HUMAN_REVIEW"
        and p7.get("two_windows_complete") is True
        and p7.get("two_windows_same_positive_direction") is True
        and p7.get("stop_fill_stress_champion_stable") is True
    )


def summarize_windows(p7: dict) -> list[dict]:
    rows = []
    for window in p7.get("windows") or []:
        dedupe_rows = []
        for report in window.get("dedupe_reports") or []:
            meta = report.get("dedupe_meta") or {}
            champion = report.get("champion_primary_delay_5_30") or {}
            stability = report.get("ranking_stability_delay_5_30") or {}
            dedupe_rows.append({
                "dedupe_view": meta.get("dedupe_view"),
                "input_count": meta.get("input_count"),
                "output_count": meta.get("output_count"),
                "top_policy_id_delay_5_30": report.get("top_policy_id_delay_5_30"),
                "champion_policy_id": champion.get("policy_id"),
                "champion_rank": champion.get("rank"),
                "champion_median_rolling_24h_roi_pct": champion.get("median_primary_metric_pct"),
                "champion_positive_cell_count": champion.get("positive_cell_count"),
                "ranking_stability_champion_top_rate": stability.get("champion_top_rate"),
                "ranking_stability_strict": stability.get("strictly_stable"),
                "stop_fill_stress_passed_for_champion": report.get("stop_fill_stress_passed_for_champion"),
            })
        rows.append({
            "window_index": window.get("window_index"),
            "window_start_iso": window.get("window_start_iso"),
            "window_end_iso": window.get("window_end_iso"),
            "complete": window.get("complete"),
            "primary_all_samples_direction_positive": window.get("primary_all_samples_direction_positive"),
            "stop_fill_stress_all_samples_passed_for_champion": window.get(
                "stop_fill_stress_all_samples_passed_for_champion"
            ),
            "paper_evidence": window.get("paper_evidence") or {},
            "dedupe_reports": dedupe_rows,
        })
    return rows


def task(task_id: str, title: str, status: str, *, reason: str, allowed_scope: list[str], acceptance: list[str], extra=None):
    payload = {
        "task_id": task_id,
        "title": title,
        "status": status,
        "reason": reason,
        "allowed_scope": allowed_scope,
        "forbidden_scope": [
            "production_strategy",
            "entry_policy",
            "hard_gates",
            "exit_gates",
            "final_entry_contract",
            "A_CLASS_runtime_mode",
            "live_or_paper_executor_enablement",
            "canary_size",
            "wallet",
            "risk_settings",
        ],
        "acceptance": acceptance,
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }
    if extra:
        payload.update(extra)
    return payload


def build_p7_proposal(p7: dict, reviewer: dict) -> dict:
    passed = p7_passed(p7)
    return {
        "schema_version": PROPOSAL_SCHEMA_VERSION,
        "generated_at": utc_now(),
        "classification": (
            "P7_PAPER_PROPOSAL_CHECKPOINT_OPEN" if passed else "P7_PAPER_PROPOSAL_BLOCKED_OOS_NOT_PASSED"
        ),
        "champion_policy_id": P7_CHAMPION_POLICY_ID,
        "proposal_type": "paper_level_experiment_only",
        "p7_oos": {
            "classification": p7.get("classification"),
            "generated_at": p7.get("generated_at"),
            "two_windows_complete": p7.get("two_windows_complete"),
            "two_windows_same_positive_direction": p7.get("two_windows_same_positive_direction"),
            "stop_fill_stress_champion_stable": p7.get("stop_fill_stress_champion_stable"),
            "primary_metric": p7.get("primary_metric"),
            "compound_cumulative_roi_is_reference_only": p7.get("compound_cumulative_roi_is_reference_only"),
            "paper_proposal_allowed": p7.get("paper_proposal_allowed"),
        },
        "window_summaries": summarize_windows(p7),
        "real_paper_evidence_interpretation": {
            "must_be_separate_from_replay": True,
            "production_promotion_evidence": False,
            "note": "P7 replay/OOS passed; real paper evidence remains separate and must be reviewed before any paper experiment approval.",
        },
        "reviewer_context": {
            "classification": reviewer.get("classification") or reviewer.get("verdict"),
            "top_blocker": reviewer.get("top_blocker"),
            "current_capture_stage": reviewer.get("current_capture_stage"),
            "paper_capture_rate": reviewer.get("paper_capture_rate"),
            "final_eligibility_capture_rate": reviewer.get("final_eligibility_capture_rate"),
        },
        "human_approval_required": passed,
        "paper_experiment_enablement_allowed": False,
        "production_exit_policy_change_allowed": False,
        "promotion_allowed": False,
        "next_action": "human_review_paper_proposal" if passed else "wait_for_p7_oos_pass",
    }


def build_phase3_loop(p7: dict, reviewer: dict) -> dict:
    p7_ready = p7_passed(p7)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now(),
        "phase": "phase3_real_quote_tail_validation",
        "operating_thesis": "Signal mining is not enough; Phase 3 tests real-quote tail capture, long-horizon observation, and metric predictiveness under governance.",
        "current_p7_status": {
            "classification": p7.get("classification"),
            "passed_pending_human_review": p7_ready,
            "paper_proposal_allowed": bool(p7.get("paper_proposal_allowed")),
            "promotion_allowed": False,
        },
        "current_reviewer_status": {
            "classification": reviewer.get("classification") or reviewer.get("verdict"),
            "top_blocker": reviewer.get("top_blocker"),
            "current_capture_stage": reviewer.get("current_capture_stage"),
            "next_action": reviewer.get("next_action"),
        },
        "tasks": [
            task(
                "P3.1",
                "Wide-net paper experiment proposal",
                "HUMAN_APPROVAL_REQUIRED_BEFORE_ENABLEMENT",
                reason="Only real paper quotes can test whether lottery tails are capturable after replay evidence is exhausted.",
                allowed_scope=["proposal", "dry_run_schema", "independent_paper_ledger_design", "self_tests"],
                acceptance=[
                    "human_approval_required_before_enablement_true",
                    "fixed_equal_size_0p001_sol_declared",
                    "quote_executable_required",
                    "p7_champion_exit_declared",
                    "independent_ledger_schema_declared",
                    "promotion_allowed_false",
                ],
                extra={
                    "experiment_defaults": {
                        "paper_size_sol": 0.001,
                        "entry_scope": "all_eligible_signals",
                        "exit_policy": P7_CHAMPION_POLICY_ID,
                        "hard_stop_range_pct": [-20, -10],
                        "minimum_run_days": 14,
                    },
                    "human_approval_required_before_enablement": True,
                },
            ),
            task(
                "P3.2",
                "Extend path observer horizon to 24h",
                "READY_FOR_SHADOW_IMPLEMENTATION",
                reason="Late token behavior cannot be studied if observation stops too early.",
                allowed_scope=["shadow_observer", "read_only_artifact", "storage_cap", "rate_limit", "self_tests"],
                acceptance=[
                    "observation_horizon_hours_24",
                    "storage_cap_declared",
                    "provider_rate_limit_declared",
                    "missing_bars_recorded_not_dropped",
                    "production_entry_exit_unchanged",
                ],
            ),
            task(
                "P3.3",
                "P7 paper proposal checkpoint",
                "READY_FOR_HUMAN_REVIEW" if p7_ready else "BLOCKED_WAITING_FOR_P7_OOS",
                reason="P7 completed the evidence -> shadow -> OOS part of the governance path.",
                allowed_scope=["proposal", "human_review_packet", "artifact_summary"],
                acceptance=[
                    "p7_replay_oos_separate_from_real_paper",
                    "real_paper_evidence_disclosed",
                    "paper_level_only",
                    "production_promotion_forbidden",
                ],
                extra={"human_checkpoint_required": p7_ready},
            ),
            task(
                "P3.4",
                "P9 metric predictiveness ledger",
                "READY_FOR_READ_ONLY_IMPLEMENTATION",
                reason="Metrics must prove forward predictive value before they can influence promotion evidence.",
                allowed_scope=["read_only_evaluator", "metric_ledger", "oos_statistics", "self_tests"],
                acceptance=[
                    "train_eval_no_overlap",
                    "effect_size_recorded",
                    "metric_verdict_recorded",
                    "metric_cannot_change_production_logic",
                ],
            ),
            task(
                "P3.5",
                "Influence/KOL shadow source plan",
                "READY_FOR_SHADOW_DESIGN",
                reason="Price-external influence data is the remaining untested source family for tail continuation and market-cap ceiling calibration.",
                allowed_scope=["agent_reach_x_twitter_sampling", "cached_shadow_features", "read_only_artifacts"],
                acceptance=[
                    "acquisition_backend_agent_reach_x_twitter",
                    "no_social_write_actions",
                    "production_impact_zero",
                    "runtime_does_not_depend_on_live_x_availability",
                    "features_marked_shadow_only",
                ],
                extra={
                    "acquisition_backend": "agent_reach_x_twitter",
                    "preferred_x_backends": ["twitter-cli", "opencli twitter fallback"],
                    "production_impact": "zero",
                },
            ),
        ],
        "guardrails": {
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_experiment_enablement_allowed_without_human": False,
            "production_exit_policy_change_allowed": False,
            "influence_kol_runtime_dependency_allowed": False,
        },
        "next_action": "human_review_p7_paper_proposal" if p7_ready else "wait_for_p7_oos_then_generate_proposal",
    }


def phase3_markdown(loop: dict, proposal: dict) -> str:
    lines = [
        "# Phase 3 Goal Loop",
        "",
        f"generated_at: `{loop['generated_at']}`",
        f"phase: `{loop['phase']}`",
        f"next_action: `{loop['next_action']}`",
        "",
        "## P7 Checkpoint",
        "",
        f"classification: `{proposal['classification']}`",
        f"champion_policy_id: `{proposal['champion_policy_id']}`",
        f"human_approval_required: `{proposal['human_approval_required']}`",
        f"promotion_allowed: `{proposal['promotion_allowed']}`",
        "",
        "## Tasks",
        "",
    ]
    for item in loop["tasks"]:
        lines.extend([
            f"### {item['task_id']} - {item['title']}",
            "",
            f"status: `{item['status']}`",
            f"reason: {item['reason']}",
            f"promotion_allowed: `{item['promotion_allowed']}`",
            "",
        ])
    lines.extend([
        "## Influence/KOL Note",
        "",
        "P3.5 uses agent-reach X/Twitter acquisition (`twitter-cli` preferred, `opencli twitter` fallback).",
        "All fetched social data is shadow-only cached evidence; production runtime must not depend on live X availability.",
        "",
    ])
    return "\n".join(lines)


def proposal_markdown(proposal: dict) -> str:
    p7 = proposal.get("p7_oos") or {}
    lines = [
        "# P7 Paper Proposal Checkpoint",
        "",
        f"generated_at: `{proposal['generated_at']}`",
        f"classification: `{proposal['classification']}`",
        f"champion_policy_id: `{proposal['champion_policy_id']}`",
        f"human_approval_required: `{proposal['human_approval_required']}`",
        f"promotion_allowed: `{proposal['promotion_allowed']}`",
        f"production_exit_policy_change_allowed: `{proposal['production_exit_policy_change_allowed']}`",
        "",
        "## OOS Evidence",
        "",
        f"p7_classification: `{p7.get('classification')}`",
        f"two_windows_complete: `{p7.get('two_windows_complete')}`",
        f"two_windows_same_positive_direction: `{p7.get('two_windows_same_positive_direction')}`",
        f"stop_fill_stress_champion_stable: `{p7.get('stop_fill_stress_champion_stable')}`",
        "",
        "## Interpretation",
        "",
        "P7 replay/OOS passed, but real paper trading evidence is separate and is not sufficient for production promotion.",
        "Any next step is paper-level human approval only.",
        "",
    ]
    return "\n".join(lines)


def run(args) -> dict:
    run_dir = Path(args.run_dir)
    p7 = load_json(Path(args.p7) if args.p7 else run_dir / "p7_exit_policy_oos_validation.json")
    reviewer = load_json(Path(args.reviewer) if args.reviewer else run_dir / "reviewer_verdict.json")
    loop = build_phase3_loop(p7, reviewer)
    proposal = build_p7_proposal(p7, reviewer)
    write_json(Path(args.out), loop)
    write_text(Path(args.out_md), phase3_markdown(loop, proposal))
    write_json(Path(args.proposal_out), proposal)
    write_text(Path(args.proposal_md), proposal_markdown(proposal))
    return {"loop": loop, "proposal": proposal}


def self_test() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        p7 = {
            "classification": "P7_EXIT_POLICY_OOS_PASSED_PENDING_HUMAN_REVIEW",
            "generated_at": "2026-07-05T22:22:40Z",
            "two_windows_complete": True,
            "two_windows_same_positive_direction": True,
            "stop_fill_stress_champion_stable": True,
            "paper_proposal_allowed": True,
            "primary_metric": "rolling_24h_realized_net_roi_median_pct",
            "compound_cumulative_roi_is_reference_only": True,
            "windows": [
                {"window_index": 1, "complete": True, "dedupe_reports": []},
                {"window_index": 2, "complete": True, "dedupe_reports": []},
            ],
        }
        reviewer = {
            "classification": "A_CLASS_EXPECTED_SHADOW",
            "top_blocker": "discovery_same_window_not_promotion_evidence",
            "current_capture_stage": "mode_disabled_clean_window_pending",
            "paper_capture_rate": 0.067961,
        }
        (root / "p7.json").write_text(json.dumps(p7), encoding="utf-8")
        (root / "reviewer.json").write_text(json.dumps(reviewer), encoding="utf-8")
        args = argparse.Namespace(
            run_dir=str(root),
            p7=str(root / "p7.json"),
            reviewer=str(root / "reviewer.json"),
            out=str(root / "phase3_goal_loop.json"),
            out_md=str(root / "phase3_goal_loop.md"),
            proposal_out=str(root / "p7_paper_proposal_checkpoint.json"),
            proposal_md=str(root / "p7_paper_proposal_checkpoint.md"),
        )
        result = run(args)
        loop = result["loop"]
        proposal = result["proposal"]
        assert proposal["classification"] == "P7_PAPER_PROPOSAL_CHECKPOINT_OPEN"
        assert proposal["human_approval_required"] is True
        assert proposal["promotion_allowed"] is False
        tasks = {item["task_id"]: item for item in loop["tasks"]}
        assert tasks["P3.1"]["human_approval_required_before_enablement"] is True
        assert tasks["P3.5"]["acquisition_backend"] == "agent_reach_x_twitter"
        assert loop["guardrails"]["paper_experiment_enablement_allowed_without_human"] is False
        assert (root / "phase3_goal_loop.md").exists()
        assert (root / "p7_paper_proposal_checkpoint.md").exists()
    print("SELF_TEST_PASS phase3_goal_loop")


def parse_args():
    parser = argparse.ArgumentParser(description="Generate Phase 3 goal loop artifacts.")
    parser.add_argument("--run-dir", default=str(DEFAULT_RUN_DIR))
    parser.add_argument("--p7")
    parser.add_argument("--reviewer")
    parser.add_argument("--out", default=str(DEFAULT_RUN_DIR / "phase3_goal_loop.json"))
    parser.add_argument("--out-md", default=str(DEFAULT_RUN_DIR / "phase3_goal_loop.md"))
    parser.add_argument("--proposal-out", default=str(DEFAULT_RUN_DIR / "p7_paper_proposal_checkpoint.json"))
    parser.add_argument("--proposal-md", default=str(DEFAULT_RUN_DIR / "p7_paper_proposal_checkpoint.md"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        self_test()
        return 0
    result = run(args)
    print(json.dumps({
        "schema_version": SCHEMA_VERSION,
        "generated_at": result["loop"]["generated_at"],
        "phase": result["loop"]["phase"],
        "p7_proposal_classification": result["proposal"]["classification"],
        "next_action": result["loop"]["next_action"],
        "promotion_allowed": False,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
