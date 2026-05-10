#!/usr/bin/env python3
"""
Counterfactual report for missed gold/silver/bronze dogs.

This script does not decide live trades. It classifies missed-dog evidence into
candidate recovery lanes and highlights the facts that still block confidence.
Inputs are JSON responses from:

- /api/paper/missed-recovery-summary
- /api/paper/trade-replay?loss_only=0
- /api/paper/lifecycle-summary
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DOG_TIERS = ("gold", "silver", "bronze")


def tier_from_ratio(value: Any) -> str:
    value = float(value or 0.0)
    if value >= 1.0:
        return "gold"
    if value >= 0.5:
        return "silver"
    if value >= 0.25:
        return "bronze"
    return "sub25"


def tier_from_pct(value: Any) -> str:
    return tier_from_ratio(float(value or 0.0) / 100.0)


def pct_ratio(value: Any) -> str:
    return f"{float(value or 0.0) * 100:.1f}%"


def pct_value(value: Any) -> str:
    return f"{float(value or 0.0):.1f}%"


def iso_ts(ts: Any) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat(timespec="seconds")


def _text(value: Any) -> str:
    return str(value or "").lower()


def est_trade_sol(trade: dict[str, Any]) -> float:
    return float(trade.get("position_size_sol") or 0.0) * float(trade.get("pnl_pct") or 0.0) / 100.0


def dog_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if tier_from_ratio(row.get("max_pnl")) in DOG_TIERS]


def lifecycle_index(lifecycle: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in lifecycle.get("lifecycles") or []:
        token = row.get("token_ca")
        if token:
            out[token].append(row)
    return out


def trade_index(replay: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in replay.get("trades") or []:
        token = row.get("token_ca")
        if token:
            out[token].append(row)
    return out


def caught_dogs(replay: dict[str, Any], *, by: str = "peak") -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in replay.get("trades") or []:
        tier = tier_from_pct(trade.get("peak_pnl_pct") if by == "peak" else trade.get("pnl_pct"))
        if tier in DOG_TIERS:
            grouped[tier].append(trade)
    return grouped


def _has_lifecycle_reason(lifecycles: list[dict[str, Any]], pattern: str) -> bool:
    pattern = pattern.lower()
    for row in lifecycles:
        text = " ".join(
            _text(row.get(key))
            for key in ("final_reason", "missed_reason", "final_component", "final_blocker_key")
        )
        if pattern in text:
            return True
    return False


def _has_trade(lifecycles: list[dict[str, Any]]) -> bool:
    return any(bool(row.get("has_trade")) or row.get("trade_id") for row in lifecycles)


def _post_exit_candidate(lifecycles: list[dict[str, Any]]) -> bool:
    exit_markers = (
        "phase_probe_no_follow",
        "fast_fail",
        "lotto_no_follow",
        "profit_protect",
        "trail_stop",
        "gap_crash",
        "crash_brake",
        "probe_runner_floor",
    )
    for row in lifecycles:
        reason = _text(row.get("exit_reason") or row.get("final_reason"))
        if row.get("has_trade") and any(marker in reason for marker in exit_markers):
            return True
    return False


@dataclass
class CandidateAssessment:
    symbol: str
    token_ca: str
    tier: str
    max_pnl: float
    route: str
    component: str
    reject_reason: str
    lane: str
    vulnerabilities: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    @property
    def defensible_first_entry(self) -> bool:
        hard_flags = {
            "stop_before_peak_requires_reentry",
            "gmgn_toxic_needs_bundler_only_split",
            "price_collapse_reclaim_needs_two_step_confirmation",
            "already_traded_needs_post_exit_logic",
        }
        return self.lane != "unmapped" and not hard_flags.intersection(self.vulnerabilities)


def assess_candidate(row: dict[str, Any], lifecycles: list[dict[str, Any]]) -> CandidateAssessment:
    route = str(row.get("route") or "-").upper()
    component = str(row.get("component") or "-")
    reason = str(row.get("reject_reason") or "-")
    reason_l = reason.lower()
    component_l = component.lower()

    vulnerabilities: list[str] = []
    notes: list[str] = []

    status = _text(row.get("tradability_status"))
    tradability_reason = _text(row.get("tradability_reason"))
    if "would_stop_before_peak" in status or "would_stop_before_peak" in tradability_reason:
        vulnerabilities.append("stop_before_peak_requires_reentry")
    if _has_trade(lifecycles):
        vulnerabilities.append("already_traded_needs_post_exit_logic")
    if "gmgn_toxic_bundler" in reason_l or _has_lifecycle_reason(lifecycles, "gmgn_toxic_bundler"):
        vulnerabilities.append("gmgn_toxic_needs_bundler_only_split")
    if "price_collapse" in reason_l:
        vulnerabilities.append("price_collapse_reclaim_needs_two_step_confirmation")

    if _post_exit_candidate(lifecycles):
        lane = "post_exit_runner_watch"
    elif "tracking_ttl_expired" in reason_l or _has_lifecycle_reason(lifecycles, "tracking_ttl_expired"):
        lane = "ttl_extension_watch"
    elif any(
        marker in reason_l
        for marker in (
            "upstream_realtime_liquidity_too_low",
            "discovery_liquidity_too_low",
            "scout_quality_liquidity_low",
            "liquidity_too_low",
        )
    ):
        lane = "low_liquidity_reclaim_watch"
    elif "not_ath_v17" in reason_l or "not_ath_prebuy_kline_unknown_data_blocked" in reason_l:
        lane = "not_ath_reclaim_watch"
    elif component_l == "matrix_evaluator" or any(
        marker in reason_l
        for marker in ("matrices not yet aligned", "timeout", "momentum check failed", "price_collapse")
    ):
        lane = "matrix_recovery_watch"
    elif any(
        marker in reason_l
        for marker in (
            "scout_quality_buy_pressure_weak",
            "scout_quality_negative_trend",
            "scout_quality_volume_low",
            "weak_buying_pressure",
            "trend_bearish_timeout",
        )
    ):
        lane = "quality_reclaim_watch"
    elif "gmgn_toxic_bundler" in reason_l or _has_lifecycle_reason(lifecycles, "gmgn_toxic_bundler"):
        lane = "gmgn_bundler_only_rescue_watch"
    else:
        lane = "unmapped"

    if lane == "post_exit_runner_watch":
        notes.append("token already had a trade; first-entry widening would not solve this case")
    if route == "LOTTO" and lane in {"low_liquidity_reclaim_watch", "quality_reclaim_watch"}:
        notes.append("must remain tiny and quote-executable; source also produced no-follow losses")
    if lane == "ttl_extension_watch":
        notes.append("TTL extension needs positive live activity, not just future max_pnl")

    return CandidateAssessment(
        symbol=str(row.get("symbol") or row.get("token_ca") or "?"),
        token_ca=str(row.get("token_ca") or ""),
        tier=tier_from_ratio(row.get("max_pnl")),
        max_pnl=float(row.get("max_pnl") or 0.0),
        route=route,
        component=component,
        reject_reason=reason,
        lane=lane,
        vulnerabilities=vulnerabilities,
        notes=notes,
    )


def summarize_replay_noise(replay: dict[str, Any]) -> dict[str, Any]:
    losses = [t for t in replay.get("trades") or [] if float(t.get("pnl_pct") or 0.0) < 0]
    wins = [t for t in replay.get("trades") or [] if float(t.get("pnl_pct") or 0.0) > 0]
    by_mode: dict[str, dict[str, Any]] = defaultdict(lambda: {"n": 0, "est_pnl_sol": 0.0})
    by_root: dict[str, dict[str, Any]] = defaultdict(lambda: {"n": 0, "est_pnl_sol": 0.0})
    for trade in losses:
        mode = str(trade.get("entry_mode") or "unknown")
        root = str((trade.get("loss_attribution") or {}).get("root_cause") or "unknown")
        by_mode[mode]["n"] += 1
        by_mode[mode]["est_pnl_sol"] += est_trade_sol(trade)
        by_root[root]["n"] += 1
        by_root[root]["est_pnl_sol"] += est_trade_sol(trade)
    return {
        "trades": len(replay.get("trades") or []),
        "wins": len(wins),
        "losses": len(losses),
        "net_est_pnl_sol": round(sum(est_trade_sol(t) for t in replay.get("trades") or []), 6),
        "loss_by_entry_mode": dict(sorted(by_mode.items(), key=lambda item: item[1]["est_pnl_sol"])),
        "loss_by_root": dict(sorted(by_root.items(), key=lambda item: item[1]["est_pnl_sol"])),
    }


def build_report(
    missed: dict[str, Any],
    replay: dict[str, Any],
    lifecycle: dict[str, Any],
) -> dict[str, Any]:
    lifecycles_by_token = lifecycle_index(lifecycle)
    missed_dogs = dog_rows(missed.get("top_clean_quote_dogs") or [])
    assessments = [
        assess_candidate(row, lifecycles_by_token.get(row.get("token_ca"), []))
        for row in missed_dogs
    ]
    lane_counts = Counter(item.lane for item in assessments)
    vulnerability_counts = Counter(flag for item in assessments for flag in item.vulnerabilities)
    tier_counts = Counter(item.tier for item in assessments)
    defensible = [item for item in assessments if item.defensible_first_entry]
    caught_peak = caught_dogs(replay, by="peak")
    caught_realized = caught_dogs(replay, by="realized")

    return {
        "windows": {
            "missed_since": (missed.get("filters") or {}).get("since_iso"),
            "missed_generated_at": missed.get("generated_at"),
            "replay_since": (replay.get("filters") or {}).get("since_iso"),
            "replay_generated_at": replay.get("generated_at"),
            "lifecycle_since": (lifecycle.get("filters") or {}).get("since_iso"),
            "lifecycle_generated_at": lifecycle.get("generated_at"),
        },
        "missed_summary": {
            "top_dog_rows": len(missed_dogs),
            "tier_counts": dict(tier_counts),
            "lane_counts": dict(lane_counts),
            "vulnerability_counts": dict(vulnerability_counts),
            "defensible_first_entry_rows": len(defensible),
        },
        "caught_summary": {
            "by_peak": {
                tier: {
                    "trades": len(rows),
                    "unique_tokens": len({row.get("token_ca") for row in rows}),
                    "est_realized_sol": round(sum(est_trade_sol(row) for row in rows), 6),
                }
                for tier, rows in caught_peak.items()
                if tier in DOG_TIERS
            },
            "by_realized": {
                tier: {
                    "trades": len(rows),
                    "unique_tokens": len({row.get("token_ca") for row in rows}),
                    "est_realized_sol": round(sum(est_trade_sol(row) for row in rows), 6),
                }
                for tier, rows in caught_realized.items()
                if tier in DOG_TIERS
            },
        },
        "replay_noise": summarize_replay_noise(replay),
        "assessments": [item.__dict__ | {"defensible_first_entry": item.defensible_first_entry} for item in assessments],
    }


def print_report(report: dict[str, Any]) -> None:
    print("Dog recovery counterfactual")
    print("windows:")
    for key, value in report["windows"].items():
        print(f"  {key}: {value}")

    missed = report["missed_summary"]
    print("\nmissed dogs:")
    print(f"  top_dog_rows: {missed['top_dog_rows']}")
    print(f"  tiers: {missed['tier_counts']}")
    print(f"  lanes: {missed['lane_counts']}")
    print(f"  vulnerabilities: {missed['vulnerability_counts']}")
    print(f"  defensible_first_entry_rows: {missed['defensible_first_entry_rows']}")

    print("\ncaught dogs:")
    print(f"  by_peak: {report['caught_summary']['by_peak']}")
    print(f"  by_realized: {report['caught_summary']['by_realized']}")

    noise = report["replay_noise"]
    print("\nreplay noise:")
    print(f"  trades={noise['trades']} wins={noise['wins']} losses={noise['losses']} net_est_pnl_sol={noise['net_est_pnl_sol']}")
    print(f"  loss_by_root={noise['loss_by_root']}")
    print("  worst_loss_modes:")
    for mode, stats in list(noise["loss_by_entry_mode"].items())[:8]:
        print(f"    {mode}: n={stats['n']} est_pnl_sol={stats['est_pnl_sol']:.6f}")

    print("\nmissed dog assessments:")
    for item in report["assessments"]:
        flags = ",".join(item["vulnerabilities"]) or "-"
        notes = "; ".join(item["notes"]) or "-"
        print(
            f"  {item['tier']:<6} {item['symbol'][:14]:<14} max={pct_ratio(item['max_pnl']):>9} "
            f"lane={item['lane']:<32} flags={flags} gate={item['route']}/{item['component']} {item['reject_reason']} | {notes}"
        )


def load_json(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text())


def main() -> int:
    parser = argparse.ArgumentParser(description="Classify missed dog recovery counterfactuals.")
    parser.add_argument("--missed", required=True, help="missed-recovery-summary JSON")
    parser.add_argument("--replay", required=True, help="trade-replay loss_only=0 JSON")
    parser.add_argument("--lifecycle", required=True, help="lifecycle-summary JSON")
    parser.add_argument("--json-output", help="optional path for machine-readable report")
    args = parser.parse_args()

    report = build_report(load_json(args.missed), load_json(args.replay), load_json(args.lifecycle))
    print_report(report)
    if args.json_output:
        Path(args.json_output).write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
