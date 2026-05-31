#!/usr/bin/env python3
"""Generate a one-variable strategy reflection candidate.

The runner consumes the read-only scoreboard from strategy_reflection_score.py
and writes an auditable hypothesis ledger. It does not mutate trading code,
does not promote candidates, and does not change live/paper execution.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
from pathlib import Path
from typing import Any

try:
    from strategy_reflection_score import (
        DEFAULT_DB,
        DEFAULT_GOAL,
        ROOT,
        evaluate_from_paths,
        load_simple_yaml,
        safe_float,
        utc_iso,
    )
except ImportError:
    from .strategy_reflection_score import (
        DEFAULT_DB,
        DEFAULT_GOAL,
        ROOT,
        evaluate_from_paths,
        load_simple_yaml,
        safe_float,
        utc_iso,
    )


DEFAULT_OUT_DIR = ROOT / "data" / "strategy_reflection"


def top_clean_blocker(scoreboard: dict[str, Any]) -> tuple[str, int]:
    blockers = ((scoreboard.get("missed_winners") or {}).get("top_clean_blockers") or [])
    if not blockers:
        blockers = ((scoreboard.get("missed_winners") or {}).get("top_blockers") or [])
    if not blockers:
        return ("none", 0)
    first = blockers[0]
    if isinstance(first, (list, tuple)) and len(first) >= 2:
        return (str(first[0]), int(first[1]))
    return (str(first), 1)


def focus_entry_mode(goal: dict[str, Any]) -> str:
    scope = goal.get("candidate_scope") or {}
    return str(scope.get("default_entry_mode") or "lotto_not_ath_reclaim_tiny_probe")


def build_single_variable_recommendation(scoreboard: dict[str, Any], goal: dict[str, Any]) -> dict[str, Any]:
    status = str(scoreboard.get("status") or "unknown")
    trades = scoreboard.get("trades") or {}
    missed = scoreboard.get("missed_winners") or {}
    markov = scoreboard.get("markov_position_advice") or {}
    entry_mode = focus_entry_mode(goal)
    blocker, blocker_n = top_clean_blocker(scoreboard)
    blocker_l = blocker.lower()
    closed_n = int(trades.get("closed_n") or 0)
    missed_clean_medal = int(missed.get("missed_clean_medal_unique") or 0)
    captured_medal = int(trades.get("captured_medal_unique") or 0)
    risk_breached = bool((scoreboard.get("score") or {}).get("risk_breached"))
    min_samples = int((goal.get("risk_limits") or {}).get("min_sample_n_before_promotion") or 30)
    markov_event_n = int(markov.get("event_n") or 0)

    if risk_breached:
        return {
            "action": "candidate_proposed",
            "target_entry_mode": entry_mode,
            "hypothesis": "Recent paper outcomes breached the drawdown guardrail; reduce canary size before testing more recall.",
            "one_variable_changed": "position_size_sol",
            "old_value": "current",
            "new_value": "reduce_one_step",
            "expected_effect": "lower tail loss while preserving the same entry selection evidence stream",
            "promotion_rule": "closed-trade score recovers above zero without increasing missed clean medal winners",
            "rollback_rule": "restore prior size if capture collapses while risk is no longer breached",
            "evidence": {
                "status": status,
                "closed_n": closed_n,
                "risk_breached": True,
            },
        }

    if "mc_0" in blocker_l or "mc0" in blocker_l:
        return {
            "action": "candidate_proposed",
            "target_entry_mode": entry_mode,
            "hypothesis": "Clean medal winners are still being lost to missing-market-cap rejects; gate-side mc0 fallback should be tested as one isolated variable.",
            "one_variable_changed": "mc0_gate_fallback_enabled",
            "old_value": False,
            "new_value": True,
            "expected_effect": "recover mc0 clean winners without changing stale, quote, or Markov thresholds",
            "promotion_rule": f"at least {min_samples} samples and non-negative score versus baseline",
            "rollback_rule": "disable fallback after three consecutive mc0 canary losses or any catastrophic loss",
            "evidence": {
                "top_clean_blocker": blocker,
                "top_clean_blocker_n": blocker_n,
                "missed_clean_medal_unique": missed_clean_medal,
            },
        }

    if (
        "tracking_ttl_expired" in blocker_l
        or "lotto_stale" in blocker_l
        or "stale" in blocker_l
        or missed_clean_medal > captured_medal
    ):
        variable = "markov_green_required" if markov_event_n < min_samples else "stale_reclaim_max_age_sec"
        return {
            "action": "candidate_proposed",
            "target_entry_mode": entry_mode,
            "hypothesis": "The largest remaining opportunity is clean late-forming LOTTO winners; test a stale reclaim canary guarded by Markov green rather than broadening all stale entries.",
            "one_variable_changed": variable,
            "old_value": "current",
            "new_value": "green_only" if variable == "markov_green_required" else "extend_one_step",
            "expected_effect": "increase clean gold/silver/bronze capture while keeping red-bucket losses capped",
            "promotion_rule": f"minimum {min_samples} Markov/canary samples and improved clean-medal recall without drawdown breach",
            "rollback_rule": "stop after three consecutive canary losses, risk breach, or red-bucket losses exceeding green wins",
            "evidence": {
                "top_clean_blocker": blocker,
                "top_clean_blocker_n": blocker_n,
                "missed_clean_medal_unique": missed_clean_medal,
                "captured_medal_unique": captured_medal,
                "markov_position_advice_n": markov_event_n,
            },
        }

    if closed_n < min(20, min_samples):
        return {
            "action": "collect_more_data",
            "target_entry_mode": entry_mode,
            "hypothesis": "There are not enough recent closed trades to justify changing a strategy variable.",
            "one_variable_changed": None,
            "old_value": None,
            "new_value": None,
            "expected_effect": "avoid overfitting a sparse evidence window",
            "promotion_rule": f"wait until at least {min(20, min_samples)} recent closed trades or a clean-medal missed-winner cluster appears",
            "rollback_rule": "not applicable",
            "evidence": {
                "closed_n": closed_n,
                "required_closed_n": min(20, min_samples),
                "status": status,
            },
        }

    return {
        "action": "collect_more_data",
        "target_entry_mode": entry_mode,
        "hypothesis": "Current evidence does not identify a single dominant bottleneck; keep collecting rather than changing multiple variables.",
        "one_variable_changed": None,
        "old_value": None,
        "new_value": None,
        "expected_effect": "preserve scientific-method guardrails",
        "promotion_rule": "wait for a dominant blocker, risk breach, or Markov bucket separation",
        "rollback_rule": "not applicable",
        "evidence": {
            "status": status,
            "top_clean_blocker": blocker,
            "closed_n": closed_n,
        },
    }


def validate_one_variable_change(candidate: dict[str, Any], goal: dict[str, Any]) -> None:
    variable = candidate.get("one_variable_changed")
    if candidate.get("action") == "collect_more_data":
        if variable not in {None, ""}:
            raise ValueError("collect_more_data candidates must not change a variable")
        return
    if isinstance(variable, (list, tuple, set)):
        if len(variable) != 1:
            raise ValueError("candidate changes more than one variable")
        variable = list(variable)[0]
        candidate["one_variable_changed"] = variable
    if not variable:
        raise ValueError("candidate_proposed requires one_variable_changed")
    allowed = set((goal.get("candidate_scope") or {}).get("allowed_variables") or [])
    if allowed and variable not in allowed:
        raise ValueError(f"variable not in allowed_variables: {variable}")


def candidate_signature(candidate: dict[str, Any], scoreboard: dict[str, Any]) -> str:
    basis = {
        "window": scoreboard.get("window"),
        "action": candidate.get("action"),
        "target_entry_mode": candidate.get("target_entry_mode"),
        "one_variable_changed": candidate.get("one_variable_changed"),
        "new_value": candidate.get("new_value"),
        "hypothesis": candidate.get("hypothesis"),
    }
    raw = json.dumps(basis, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def save_prior_candidate(current_path: Path, history_dir: Path, now_ts: float) -> str | None:
    if not current_path.exists():
        return None
    history_dir.mkdir(parents=True, exist_ok=True)
    stem = datetime_stem(now_ts)
    target = history_dir / f"candidate-{stem}.json"
    shutil.copy2(current_path, target)
    return str(target)


def datetime_stem(ts: float) -> str:
    return utc_iso(ts).replace(":", "").replace("-", "").split("+", 1)[0].replace(".", "_")


def run_reflection(
    *,
    db_path: str | Path = DEFAULT_DB,
    goal_path: str | Path = DEFAULT_GOAL,
    out_dir: str | Path = DEFAULT_OUT_DIR,
    hours: float = 24,
    now_ts: float | None = None,
    write: bool = True,
) -> dict[str, Any]:
    now_ts = float(now_ts or time.time())
    goal = load_simple_yaml(goal_path)
    scoreboard = evaluate_from_paths(db_path, goal_path, hours=hours, now_ts=now_ts)
    candidate = build_single_variable_recommendation(scoreboard, goal)
    validate_one_variable_change(candidate, goal)
    sig = candidate_signature(candidate, scoreboard)
    hypothesis = {
        "schema_version": "v1.strategy_reflection_hypothesis",
        "id": f"hyp_{datetime_stem(now_ts)}_{sig}",
        "created_at": utc_iso(now_ts),
        "evidence_window_hours": hours,
        "score_status": scoreboard.get("status"),
        "score": scoreboard.get("score"),
        **candidate,
        "signature": sig,
        "shadow_first": bool((goal.get("reflection") or {}).get("shadow_first", True)),
        "one_variable_only": True,
    }
    result = {
        "schema_version": "v1.strategy_reflection_run",
        "generated_at": utc_iso(now_ts),
        "scoreboard": scoreboard,
        "hypothesis": hypothesis,
        "write": write,
    }
    if write:
        out = Path(out_dir)
        write_json(out / "scoreboard-latest.json", scoreboard)
        current_path = out / "current_candidate.json"
        prior_path = save_prior_candidate(current_path, out / "history", now_ts)
        write_json(current_path, hypothesis)
        append_jsonl(out / "hypotheses.jsonl", hypothesis)
        result["written"] = {
            "scoreboard": str(out / "scoreboard-latest.json"),
            "current_candidate": str(current_path),
            "hypotheses": str(out / "hypotheses.jsonl"),
            "prior_candidate_history": prior_path,
        }
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the strategy reflection layer.")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--goal", default=str(DEFAULT_GOAL))
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--now-ts", type=float, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    result = run_reflection(
        db_path=args.db,
        goal_path=args.goal,
        out_dir=args.out_dir,
        hours=args.hours,
        now_ts=args.now_ts,
        write=not args.dry_run,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
