"""Rolling strategy goal controller actions.

The controller proposes mode actions from objective evidence.  By default those
actions are advisory; when an operator materializes a runtime overlay and
enables the runtime gate feature flag, v27_runtime_mode_gate consumes the same
actions as deterministic mode constraints.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from contextual_bandit_allocator import build_tiny_bandit_allocation
from counterfactual_replay import build_counterfactual_replay_report
from entry_mode_scorecard import build_entry_mode_scorecard
from missed_dog_blocker_ranking import build_missed_dog_blocker_ranking


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_CONTROLLER_ACTIONS_PATH = PROJECT_ROOT / "data" / "v27_read_models" / "strategy_goal_controller_actions.json"
CONTROLLER_ACTIONS_SCHEMA_VERSION = "v1.strategy_goal_controller.runtime_overlay"


def _num(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _scorecard_controller_actions(entry_mode_scorecard: dict | None) -> list[dict]:
    scorecard = entry_mode_scorecard or {}
    rows = scorecard.get("rows") if isinstance(scorecard.get("rows"), list) else []
    actions = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        mode = row.get("mode")
        if not mode:
            continue
        max_loss_pct = _num(row.get("max_loss_pct"), None)
        no_route_rate = _num(row.get("no_route_rate"), 0.0) or 0.0
        trapped_rate = _num(row.get("trapped_rate"), 0.0) or 0.0
        status = str(row.get("status") or "").upper()
        if max_loss_pct is not None and max_loss_pct < -0.20:
            actions.append({
                "mode": mode,
                "action": "DISABLE",
                "reason": "scorecard_single_trade_loss_breached",
                "max_loss_pct": max_loss_pct,
            })
            continue
        if no_route_rate > 0.10 or trapped_rate > 0.10 or status == "DISABLED":
            actions.append({
                "mode": mode,
                "action": "DISABLE",
                "reason": "scorecard_route_health_or_status_disabled",
                "no_route_rate": no_route_rate,
                "trapped_rate": trapped_rate,
                "status": status,
            })
            continue
        if status in {"SHADOW", "TINY_ONLY"}:
            actions.append({
                "mode": mode,
                "action": "TINY_CANARY",
                "size_sol": min(0.001, _num(row.get("allowed_max_size_sol"), 0.001) or 0.001),
                "reason": "scorecard_not_ready_for_live_size",
                "status": status,
            })
    return actions


def _missed_ranking_review(missed_dog_blocker_ranking: dict | None) -> dict:
    ranking = missed_dog_blocker_ranking or {}
    rows = ranking.get("rows") if isinstance(ranking.get("rows"), list) else []
    allow_count = sum(1 for row in rows if isinstance(row, dict) and row.get("recommendation") == "allow_a_class_only")
    hard_count = sum(1 for row in rows if isinstance(row, dict) and row.get("recommendation") == "keep_hard_block")
    return {
        "allow_a_class_only_count": allow_count,
        "keep_hard_block_count": hard_count,
        "recommendations": rows[:50],
    }


def _bandit_controller_actions(bandit_allocation: dict | None) -> list[dict]:
    allocation = bandit_allocation or {}
    arms = allocation.get("arms") if isinstance(allocation.get("arms"), list) else []
    actions = []
    for arm in arms:
        if not isinstance(arm, dict):
            continue
        size = _num(arm.get("recommended_max_size_sol"), 0.0) or 0.0
        if not arm.get("eligible") or size <= 0:
            continue
        actions.append({
            "mode": arm.get("arm"),
            "action": "TINY_CANARY",
            "size_sol": size,
            "reason": "bandit_advisory_positive_winsorized_reward",
            "posterior_mean_sol": arm.get("posterior_mean_sol"),
        })
    return actions


def build_strategy_goal_controller_actions(
    *,
    rolling_goal_status: dict | None = None,
    a_class_p0_discovery: dict | None = None,
    counterfactual_audit: dict | None = None,
    missed_dog_review: dict | None = None,
    entry_mode_scorecard: dict | None = None,
    missed_dog_blocker_ranking: dict | None = None,
    bandit_allocation: dict | None = None,
) -> dict:
    goal = rolling_goal_status or {}
    p0 = a_class_p0_discovery or {}
    audit = counterfactual_audit or {}
    missed = missed_dog_review or _missed_ranking_review(missed_dog_blocker_ranking)
    actions = []
    blockers = []

    if goal.get("status") in {"insufficient_sample", "evidence_unavailable"}:
        blockers.append("rolling_goal_sample_or_evidence_insufficient")
    if goal.get("max_single_trade_loss_ok") is False:
        actions.append({
            "mode": "ALL_LIVE_RISK",
            "action": "DISABLE",
            "reason": "single_trade_loss_limit_breached",
        })
    if p0.get("discovery_exit", {}).get("advisory") == "PROMOTE_TINY_CANARY" and audit.get("pass"):
        actions.append({
            "mode": "A_CLASS_FASTLANE",
            "action": "TINY_CANARY",
            "size_sol": p0.get("discovery_exit", {}).get("canary_size_sol", 0.001),
            "reason": "counterfactual_denominator_and_rr_passed",
            "requires_human_approval": True,
        })
    elif p0.get("available") is False or not audit.get("pass", False):
        actions.append({
            "mode": "A_CLASS_FASTLANE",
            "action": "SHADOW",
            "reason": "p0_discovery_or_counterfactual_audit_not_green",
        })

    no_route_rate = _num(p0.get("would_enter_no_route_rate"), 0.0) or 0.0
    trapped_rate = _num(p0.get("would_enter_trapped_rate"), 0.0) or 0.0
    if no_route_rate > 0.10 or trapped_rate > 0.10:
        actions.append({
            "mode": "A_CLASS_FASTLANE",
            "action": "DISABLE",
            "reason": "route_health_risk_above_threshold",
        })
    allow_count = int((missed.get("allow_a_class_only_count") or missed.get("extra", {}).get("allow_a_class_only_count") or 0))
    if allow_count:
        actions.append({
            "mode": "MISSED_DOG_BLOCKERS",
            "action": "ALLOW_A_CLASS_ONLY",
            "reason": "missed_dog_reviewer_found_soft_blocker_candidates",
            "candidate_blocker_count": allow_count,
        })
    actions.extend(_scorecard_controller_actions(entry_mode_scorecard))
    actions.extend(_bandit_controller_actions(bandit_allocation))

    next_safe_action = "keep_a_class_shadow"
    if any(action["action"] == "TINY_CANARY" for action in actions):
        next_safe_action = "prepare_0_001_tiny_paper_after_observability_green"
    if any(action["action"] == "DISABLE" for action in actions):
        next_safe_action = "disable_or_shadow_risky_modes"

    return {
        "schema_version": "v1.strategy_goal_controller.actions",
        "advisory_only": True,
        "can_trigger_trade": False,
        "actions": actions,
        "blockers": blockers,
        "evidence": {
            "entry_mode_scorecard_available": bool((entry_mode_scorecard or {}).get("available")),
            "missed_dog_blocker_ranking_available": bool((missed_dog_blocker_ranking or {}).get("available")),
            "bandit_allocation_available": bool(bandit_allocation),
        },
        "next_safe_action": next_safe_action,
    }


def build_strategy_goal_control_snapshot(
    db,
    *,
    hours: float = 24,
    rolling_goal_status: dict | None = None,
    a_class_p0_discovery: dict | None = None,
    counterfactual_audit: dict | None = None,
    missed_dog_review: dict | None = None,
    min_sample_to_live: int = 30,
    bandit_min_samples: int = 30,
    enforcement_enabled: bool = False,
    generated_at: float | None = None,
) -> dict:
    generated_at = float(generated_at if generated_at is not None else time.time())
    since_ts = generated_at - float(hours) * 3600.0 if hours else None
    entry_mode_scorecard = build_entry_mode_scorecard(
        db,
        since_ts=since_ts,
        min_sample_to_live=min_sample_to_live,
    )
    missed_ranking = build_missed_dog_blocker_ranking(db, since_ts=since_ts)
    counterfactual_replay = build_counterfactual_replay_report(db, since_ts=since_ts)
    bandit_allocation = build_tiny_bandit_allocation(
        entry_mode_scorecard.get("rows", []) if isinstance(entry_mode_scorecard, dict) else [],
        min_samples=bandit_min_samples,
    )
    p0_or_replay = a_class_p0_discovery or counterfactual_replay
    controller_actions = build_strategy_goal_controller_actions(
        rolling_goal_status=rolling_goal_status,
        a_class_p0_discovery=p0_or_replay,
        counterfactual_audit=counterfactual_audit,
        missed_dog_review=missed_dog_review,
        missed_dog_blocker_ranking=missed_ranking,
        entry_mode_scorecard=entry_mode_scorecard,
        bandit_allocation=bandit_allocation,
    )
    runtime_overlay = build_strategy_goal_runtime_overlay(
        controller_actions,
        enforcement_enabled=enforcement_enabled,
        generated_at=generated_at,
    )
    return {
        "schema_version": "v1.strategy_goal_controller.snapshot",
        "generated_at": generated_at,
        "generated_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(generated_at)),
        "hours": hours,
        "since_ts": since_ts,
        "entry_mode_scorecard": entry_mode_scorecard,
        "missed_dog_blocker_ranking": missed_ranking,
        "counterfactual_replay": counterfactual_replay,
        "bandit_allocation": bandit_allocation,
        "controller_actions": controller_actions,
        "runtime_overlay": runtime_overlay,
        "advisory_only": True,
        "can_trigger_trade": False,
    }


def build_strategy_goal_runtime_overlay(
    controller_actions: dict,
    *,
    enforcement_enabled: bool = False,
    generated_at: float | None = None,
) -> dict:
    """Build the file consumed by v27_runtime_mode_gate.

    `enforcement_enabled` must be explicitly true in the file *and* the runtime
    process must set STRATEGY_GOAL_CONTROLLER_RUNTIME_ENFORCEMENT_ENABLED=true
    before actions affect entries.  This double opt-in prevents accidental
    production behavior changes while still making the control plane wired.
    """
    generated_at = float(generated_at if generated_at is not None else time.time())
    controller_actions = controller_actions or {}
    actions = controller_actions.get("actions") if isinstance(controller_actions.get("actions"), list) else []
    return {
        "schema_version": CONTROLLER_ACTIONS_SCHEMA_VERSION,
        "generated_at": generated_at,
        "generated_at_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(generated_at)),
        "enforcement_enabled": bool(enforcement_enabled),
        "advisory_source_schema_version": controller_actions.get("schema_version"),
        "actions": actions,
        "blockers": controller_actions.get("blockers") or [],
        "next_safe_action": controller_actions.get("next_safe_action") or "keep_a_class_shadow",
        "can_trigger_trade": False,
        "notes": [
            "AI/controller actions never bypass final hard gates.",
            "Runtime enforcement additionally requires STRATEGY_GOAL_CONTROLLER_RUNTIME_ENFORCEMENT_ENABLED=true.",
        ],
    }


def write_strategy_goal_runtime_overlay(
    controller_actions: dict,
    *,
    path: str | os.PathLike[str] | None = None,
    enforcement_enabled: bool = False,
    generated_at: float | None = None,
) -> dict:
    overlay = build_strategy_goal_runtime_overlay(
        controller_actions,
        enforcement_enabled=enforcement_enabled,
        generated_at=generated_at,
    )
    output_path = Path(path or os.environ.get("STRATEGY_GOAL_CONTROLLER_ACTIONS_PATH") or DEFAULT_CONTROLLER_ACTIONS_PATH)
    if not output_path.is_absolute():
        output_path = PROJECT_ROOT / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(overlay, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")
    tmp_path.replace(output_path)
    return {
        "path": str(output_path),
        "overlay": overlay,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Build strategy goal controller snapshot and optional runtime overlay.")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--min-sample-to-live", type=int, default=30)
    parser.add_argument("--bandit-min-samples", type=int, default=30)
    parser.add_argument("--write-overlay", action="store_true")
    parser.add_argument("--overlay-path", default=None)
    parser.add_argument("--enforcement-enabled", action="store_true")
    args = parser.parse_args(argv)

    db = sqlite3.connect(args.db)
    db.row_factory = sqlite3.Row
    try:
        snapshot = build_strategy_goal_control_snapshot(
            db,
            hours=args.hours,
            min_sample_to_live=args.min_sample_to_live,
            bandit_min_samples=args.bandit_min_samples,
            enforcement_enabled=args.enforcement_enabled,
        )
        if args.write_overlay:
            result = write_strategy_goal_runtime_overlay(
                snapshot["controller_actions"],
                path=args.overlay_path,
                enforcement_enabled=args.enforcement_enabled,
                generated_at=snapshot["generated_at"],
            )
            snapshot["written_overlay_path"] = result["path"]
        print(json.dumps(snapshot, ensure_ascii=False, sort_keys=True, indent=2))
    finally:
        db.close()


if __name__ == "__main__":
    main()
