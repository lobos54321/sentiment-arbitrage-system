#!/usr/bin/env python3
"""Validate Strategy Memory hypotheses against current AutoLoop evidence.

Read-only and shadow-only. This script treats historical strategy memory as
prior discovery context. It never changes candidates, strategy, gates,
final_entry_contract, A_CLASS mode, executor, wallet, canary, or risk.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path


VERDICTS = {
    "REJECTED_FUTURE_DATA": "STRATEGY_MEMORY_REJECTED_FUTURE_DATA",
    "DATA_BLOCKED": "STRATEGY_MEMORY_DATA_BLOCKED",
    "EXIT_ONLY": "STRATEGY_MEMORY_EXIT_ONLY",
    "DISCOVERY_WATCH": "STRATEGY_MEMORY_DISCOVERY_WATCH",
    "EFFECTIVENESS_HIT_PENDING_OOS": "STRATEGY_MEMORY_EFFECTIVENESS_HIT_PENDING_OOS",
    "NO_CURRENT_SIGNAL": "STRATEGY_MEMORY_NO_CURRENT_SIGNAL",
}


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path, default=None):
    if not path:
        return default
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if parsed == parsed else default
    except Exception:
        return default


def safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        return default


def rate(num, den):
    den = safe_float(den, 0)
    if not den:
        return None
    return round(float(num or 0) / den, 6)


def as_list(value):
    return value if isinstance(value, list) else []


def strategy_memory_from_sources(registry, ingestion_summary):
    namespace = (registry or {}).get("strategy_memory") or {}
    if namespace.get("hypotheses"):
        return namespace, True
    summary = ingestion_summary or {}
    return {
        "available": bool(summary.get("available")),
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
        },
        "hypotheses": summary.get("hypotheses") or [],
        "missing_shadow_candidate_handoffs": summary.get("missing_shadow_candidate_handoffs") or [],
        "exit_only_hypotheses": summary.get("exit_only_hypotheses") or [],
        "allowed_use": "shadow_only",
        "evidence_level": "historical_memory",
        "promotion_allowed": False,
    }, False


def candidate_baseline_map(capture_report):
    return {
        row.get("candidate_id"): row
        for row in as_list((capture_report or {}).get("candidate_baseline"))
        if isinstance(row, dict) and row.get("candidate_id")
    }


def downstream_map(downstream_report):
    return {
        row.get("candidate_id"): row
        for row in as_list((downstream_report or {}).get("all_candidates"))
        if isinstance(row, dict) and row.get("candidate_id")
    }


def pnl_map(pnl_report):
    return {
        row.get("candidate_id"): row
        for row in as_list((pnl_report or {}).get("baseline"))
        if isinstance(row, dict) and row.get("candidate_id")
    }


def compact_candidate_validation(candidate_id, capture_by_candidate, downstream_by_candidate, pnl_by_candidate):
    capture = capture_by_candidate.get(candidate_id) or {}
    downstream = downstream_by_candidate.get(candidate_id) or {}
    pnl = pnl_by_candidate.get(candidate_id) or {}
    raw_recall = (
        downstream.get("raw_gs_recall")
        if downstream.get("raw_gs_recall") is not None
        else capture.get("match_recall_event")
    )
    precision = (
        downstream.get("match_precision")
        if downstream.get("match_precision") is not None
        else capture.get("match_precision_event")
    )
    return {
        "candidate_id": candidate_id,
        "family": downstream.get("family") or capture.get("family") or pnl.get("family"),
        "raw_gold_silver_recall": raw_recall,
        "precision": precision,
        "matched_raw_gs_signals": downstream.get("matched_raw_gs_signals"),
        "observed_raw_gs_signals": downstream.get("observed_raw_gs_signals"),
        "decision_capture": downstream.get("decision_record_rate_after_match"),
        "pass_allow_capture": downstream.get("pass_allow_rate_after_match"),
        "pending_capture": downstream.get("pending_rate_after_match"),
        "final_eligibility_capture": downstream.get("final_entry_contract_rate_after_match"),
        "mode_disabled_adjusted_final_eligibility": (
            downstream.get("mode_disabled_adjusted_final_eligibility_rate_after_match")
        ),
        "paper_capture": downstream.get("paper_trade_committed_rate_after_match"),
        "paper_trade_intent_rate": downstream.get("paper_trade_intent_rate_after_match"),
        "realized_capture": None,
        "downstream_classification": downstream.get("classification"),
        "pnl_secondary": {
            "available": bool(pnl),
            "judgment": pnl.get("judgment"),
            "closed_n": pnl.get("closed_n"),
            "unique_tokens": pnl.get("unique_tokens"),
            "win_rate_pct": pnl.get("win_rate_pct"),
            "median_net_pnl_pct": pnl.get("median_net_pnl_pct"),
            "capped_avg_net_pnl_pct": pnl.get("capped_avg_net_pnl_pct"),
            "profit_factor": pnl.get("profit_factor"),
        },
    }


def choose_primary_candidate(candidate_rows):
    if not candidate_rows:
        return None
    return sorted(
        candidate_rows,
        key=lambda row: (
            safe_float(row.get("raw_gold_silver_recall"), -1),
            safe_float(row.get("mode_disabled_adjusted_final_eligibility"), -1),
            safe_float(row.get("precision"), -1),
        ),
        reverse=True,
    )[0]


def verdict_for_hypothesis(row, candidate_rows):
    if row.get("exit_only"):
        return VERDICTS["EXIT_ONLY"]
    if row.get("rejected_future_data"):
        return VERDICTS["REJECTED_FUTURE_DATA"]
    if row.get("missing_shadow_candidate_handoff_required") or not row.get("mapped_existing_candidate_ids"):
        return VERDICTS["DATA_BLOCKED"]
    primary = choose_primary_candidate(candidate_rows)
    if not primary or safe_float(primary.get("raw_gold_silver_recall"), 0) <= 0:
        return VERDICTS["NO_CURRENT_SIGNAL"]
    if (
        safe_float(primary.get("raw_gold_silver_recall"), 0) >= 0.6
        and safe_float(primary.get("precision"), 0) > 0
        and safe_float(primary.get("decision_capture"), 0) > 0
        and safe_float(primary.get("mode_disabled_adjusted_final_eligibility"), 0) >= 0.6
    ):
        return VERDICTS["EFFECTIVENESS_HIT_PENDING_OOS"]
    return VERDICTS["DISCOVERY_WATCH"]


def summarize_windows(capture_reports):
    rows = []
    for label, report in sorted(capture_reports.items(), key=lambda item: safe_float(item[0], 0)):
        if not report:
            continue
        denom = report.get("raw_gold_silver_denominator") or {}
        rows.append({
            "hours": label,
            "raw_gold_silver_event_rows": denom.get("event_rows"),
            "raw_gold_silver_unique_tokens": denom.get("unique_tokens"),
            "rows_complete_against_summary": denom.get("rows_complete_against_summary"),
            "candidate_count_observed": ((report.get("coverage") or {}).get("candidate_count_observed")),
            "observation_coverage_pct": ((report.get("coverage") or {}).get("coverage_pct")),
            "judgment_counts": report.get("judgment_counts") or {},
        })
    return rows


def build_validation(args):
    registry = load_json(args.registry, {})
    ingestion = load_json(args.ingestion_summary, {})
    strategy_memory, namespace_loaded = strategy_memory_from_sources(registry, ingestion)
    capture_reports = {
        "24": load_json(args.capture_24h, {}),
        "48": load_json(args.capture_48h, {}),
        "72": load_json(args.capture_72h, {}),
    }
    capture_24 = capture_reports["24"] or {}
    downstream = load_json(args.downstream, {})
    a_class = load_json(args.a_class, {})
    pnl = load_json(args.pnl, {})
    capture_by_candidate = candidate_baseline_map(capture_24)
    downstream_by_candidate = downstream_map(downstream)
    pnl_by_candidate = pnl_map(pnl)
    hypotheses = as_list(strategy_memory.get("hypotheses"))
    validations = []
    for row in hypotheses:
        if not isinstance(row, dict):
            continue
        candidate_ids = as_list(row.get("mapped_existing_candidate_ids"))
        candidate_rows = [
            compact_candidate_validation(candidate_id, capture_by_candidate, downstream_by_candidate, pnl_by_candidate)
            for candidate_id in candidate_ids
            if candidate_id in capture_by_candidate or candidate_id in downstream_by_candidate or candidate_id in pnl_by_candidate
        ]
        primary = choose_primary_candidate(candidate_rows)
        verdict = verdict_for_hypothesis(row, candidate_rows)
        data_blockers = []
        if row.get("missing_shadow_candidate_handoff_required"):
            data_blockers.append("missing_shadow_candidate_handoff_only")
        if row.get("rejected_future_data"):
            data_blockers.append("future_or_posthoc_features_rejected")
        if row.get("requires_paper_trades_db") and not ((strategy_memory.get("summary") or {}).get("paper_trades_db_available")):
            data_blockers.append("paper_trades_db_unavailable")
        if candidate_ids and not candidate_rows:
            data_blockers.append("mapped_candidates_not_observed_current_window")
        validations.append({
            "hypothesis_id": row.get("hypothesis_id"),
            "name": row.get("name"),
            "family": row.get("strategy_family"),
            "mapped_candidate_ids": candidate_ids,
            "missing_shadow_candidate_required": bool(row.get("missing_shadow_candidate_handoff_required")),
            "allowed_use": "shadow_only",
            "evidence_level": "historical_memory_validated_against_current_window",
            "promotion_allowed": False,
            "strategy_change_allowed": False,
            "automatic_runtime_change_allowed": False,
            "paper_enablement_allowed": False,
            "route": row.get("route"),
            "future_data_rejected": bool(row.get("rejected_future_data")),
            "future_or_posthoc_features": row.get("future_or_posthoc_features") or [],
            "paper_db_required": bool(row.get("requires_paper_trades_db")),
            "paper_db_available": bool((strategy_memory.get("summary") or {}).get("paper_trades_db_available")),
            "exit_only": bool(row.get("exit_only")),
            "data_blockers": sorted(set(data_blockers)),
            "time_legal_status": "time_legal_features_declared" if row.get("time_legal_features") else "time_legal_not_declared",
            "raw_gold_silver_recall": (primary or {}).get("raw_gold_silver_recall"),
            "precision": (primary or {}).get("precision"),
            "decision_capture": (primary or {}).get("decision_capture"),
            "pass_allow_capture": (primary or {}).get("pass_allow_capture"),
            "pending_capture": (primary or {}).get("pending_capture"),
            "final_eligibility_capture": (primary or {}).get("final_eligibility_capture"),
            "mode_disabled_adjusted_final_eligibility": (
                (primary or {}).get("mode_disabled_adjusted_final_eligibility")
            ),
            "paper_capture": (primary or {}).get("paper_capture"),
            "realized_capture": (primary or {}).get("realized_capture"),
            "pnl_secondary_status": ((primary or {}).get("pnl_secondary") or {}).get("judgment"),
            "primary_current_candidate": primary,
            "candidate_validations": candidate_rows[:24],
            "verdict": verdict,
        })
    status_counts = {}
    for row in validations:
        status_counts[row["verdict"]] = status_counts.get(row["verdict"], 0) + 1
    return {
        "schema_version": "strategy_memory_validation.v1",
        "report_type": "strategy_memory_validation_24h",
        "generated_at": utc_now(),
        "hours": 24,
        "window_validations": summarize_windows(capture_reports),
        "strategy_memory_namespace_loaded": namespace_loaded,
        "strategy_memory_enabled": bool(strategy_memory.get("available")),
        "hypotheses_count": len(validations),
        "mapped_to_existing_candidates": (strategy_memory.get("summary") or {}).get("mapped_to_existing_candidates"),
        "missing_shadow_candidates": (strategy_memory.get("summary") or {}).get("missing_shadow_candidates"),
        "rejected_future_data_hypotheses": (strategy_memory.get("summary") or {}).get("rejected_future_data_hypotheses"),
        "paper_trades_db_available": bool((strategy_memory.get("summary") or {}).get("paper_trades_db_available")),
        "global_capture_stage_rates": a_class.get("capture_stage_rates") or {},
        "status_counts": status_counts,
        "allowed_use": "shadow_only",
        "evidence_role": "discovery_only",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "candidate_catalog_change_allowed": False,
        "hypotheses": validations,
        "notes": [
            "Historical memory is prior discovery context, not promotion evidence.",
            "Metrics are computed from currently mapped candidates and current AutoLoop artifacts.",
            "Candidate aggregation uses the best currently mapped candidate by recall/final-eligibility/precision, not a union simulator.",
            "Future/posthoc features remain rejected as entry evidence.",
        ],
    }


def build_filtered_winner_bridge(args, validation):
    dossier = load_json(args.filtered_winner, {})
    winners = as_list(dossier.get("missed_winners"))
    blocker_counts = {}
    for row in winners:
        blocker = row.get("final_blocker") or "unknown"
        blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1
    relevant = [
        row for row in validation.get("hypotheses") or []
        if row.get("hypothesis_id") == "SM-FILTERED-WINNER-RELAXATION"
    ]
    return {
        "schema_version": "strategy_memory_filtered_winner_bridge.v1",
        "report_type": "strategy_memory_filtered_winner_bridge",
        "generated_at": utc_now(),
        "filtered_winner_count": dossier.get("filtered_winner_count", len(winners)),
        "final_blocker_counts": dossier.get("final_blocker_counts") or blocker_counts,
        "current_strategy_memory_hypothesis": relevant[0] if relevant else None,
        "top_examples": winners[:20],
        "allowed_use": "shadow_only",
        "evidence_role": "discovery_only",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
        "notes": [
            "Filtered winners are missed-opportunity training data only.",
            "This bridge attributes historical filtered winners to current funnel blockers; it does not relax filters.",
        ],
    }


def build_exit_shadow_summary(args, validation):
    report = load_json(args.exit_report, {})
    variants = as_list(report.get("variants"))
    sorted_variants = sorted(
        variants,
        key=lambda row: safe_float(row.get("avg_net_pnl_pct"), -10**9),
        reverse=True,
    )
    exit_hypotheses = [
        row for row in validation.get("hypotheses") or []
        if row.get("route") == "exit_policy_shadow_simulator_only" or row.get("exit_only")
    ]
    return {
        "schema_version": "strategy_memory_exit_shadow_summary.v1",
        "report_type": "strategy_memory_exit_shadow_summary",
        "generated_at": utc_now(),
        "exit_policy_variants_tested": report.get("exit_policy_variants_tested", len(variants)),
        "sample_count": report.get("sample_count"),
        "gold_silver_sample_count": report.get("gold_silver_sample_count"),
        "top_variants": sorted_variants[:12],
        "exit_only_hypotheses": exit_hypotheses,
        "live_position_monitor_changed": False,
        "allowed_use": "shadow_only",
        "evidence_role": "exit_shadow_simulator_only",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def build_delay_replay_summary(args, validation):
    report = load_json(args.delay_report, {})
    rows = []
    for hypothesis in as_list(report.get("hypotheses")):
        metrics = as_list(hypothesis.get("metrics_by_delay"))
        best = None
        if metrics:
            best = sorted(
                metrics,
                key=lambda row: (
                    safe_float(row.get("mode_disabled_adjusted_final_eligibility_capture"), -1),
                    safe_float(row.get("recall"), -1),
                    safe_float(row.get("precision"), -1),
                ),
                reverse=True,
            )[0]
        rows.append({
            "hypothesis_id": hypothesis.get("hypothesis_id"),
            "delay_replay_done": bool(report.get("delay_replay_done")),
            "ath1_delay_replay_required": hypothesis.get("ath1_delay_replay_required"),
            "delay_adjustment_basis": hypothesis.get("delay_adjustment_basis"),
            "best_delay_metrics": best,
            "promotion_allowed": False,
        })
    execution_delay_hypotheses = [
        row for row in validation.get("hypotheses") or []
        if row.get("hypothesis_id") == "SM-EXECUTION-DELAY-SENSITIVITY"
        or row.get("route") == "delay_replay_only"
    ]
    return {
        "schema_version": "strategy_memory_delay_replay_summary.v1",
        "report_type": "strategy_memory_delay_replay_summary",
        "generated_at": utc_now(),
        "delay_replay_done": bool(report.get("delay_replay_done")),
        "entry_delays_sec": report.get("entry_delays_sec") or [],
        "hypotheses": rows,
        "execution_delay_hypotheses": execution_delay_hypotheses,
        "allowed_use": "shadow_only",
        "evidence_role": "delay_replay_only",
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "automatic_runtime_change_allowed": False,
        "paper_enablement_allowed": False,
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        registry = {
            "strategy_memory": {
                "available": True,
                "summary": {
                    "mapped_to_existing_candidates": 1,
                    "missing_shadow_candidates": 1,
                    "rejected_future_data_hypotheses": 1,
                    "paper_trades_db_available": True,
                },
                "hypotheses": [
                    {
                        "hypothesis_id": "SM-TEST",
                        "name": "test",
                        "strategy_family": "test",
                        "mapped_existing_candidate_ids": ["candidate:a"],
                        "rejected_future_data": False,
                        "promotion_allowed": False,
                        "route": "strategy_memory_shadow_context_only",
                    },
                    {
                        "hypothesis_id": "SM-EXIT-TEST",
                        "name": "exit",
                        "strategy_family": "Exit policy variants",
                        "mapped_existing_candidate_ids": [],
                        "rejected_future_data": True,
                        "exit_only": True,
                        "promotion_allowed": False,
                        "route": "exit_policy_shadow_simulator_only",
                    },
                ],
            }
        }
        capture = {
            "raw_gold_silver_denominator": {"event_rows": 10, "unique_tokens": 8, "rows_complete_against_summary": True},
            "coverage": {"candidate_count_observed": 84, "coverage_pct": 100},
            "judgment_counts": {"WATCH": 1},
            "candidate_baseline": [
                {"candidate_id": "candidate:a", "family": "test", "match_recall_event": 0.7, "match_precision_event": 0.2}
            ],
        }
        downstream = {
            "all_candidates": [
                {
                    "candidate_id": "candidate:a",
                    "family": "test",
                    "raw_gs_recall": 0.7,
                    "match_precision": 0.2,
                    "decision_record_rate_after_match": 0.6,
                    "pass_allow_rate_after_match": 0.5,
                    "pending_rate_after_match": 0.4,
                    "final_entry_contract_rate_after_match": 0.3,
                    "mode_disabled_adjusted_final_eligibility_rate_after_match": 0.3,
                    "paper_trade_committed_rate_after_match": 0.0,
                }
            ]
        }
        pnl = {"baseline": [{"candidate_id": "candidate:a", "judgment": "WATCH", "closed_n": 20}]}
        exit_report = {"exit_policy_variants_tested": 1, "variants": [{"id": "v", "avg_net_pnl_pct": 1}]}
        delay_report = {"delay_replay_done": True, "hypotheses": [{"hypothesis_id": "SM-TEST", "metrics_by_delay": [{"entry_delay_sec": 0, "recall": 0.7}]}]}
        filtered = {"filtered_winner_count": 1, "missed_winners": [{"signal_id": "1", "final_blocker": "no_candidate_match"}]}
        paths = {
            "registry": root / "registry.json",
            "capture": root / "capture.json",
            "downstream": root / "downstream.json",
            "pnl": root / "pnl.json",
            "exit": root / "exit.json",
            "delay": root / "delay.json",
            "filtered": root / "filtered.json",
        }
        for key, payload in [
            ("registry", registry),
            ("capture", capture),
            ("downstream", downstream),
            ("pnl", pnl),
            ("exit", exit_report),
            ("delay", delay_report),
            ("filtered", filtered),
        ]:
            write_json(paths[key], payload)
        args = argparse.Namespace(
            registry=str(paths["registry"]),
            ingestion_summary=None,
            capture_24h=str(paths["capture"]),
            capture_48h=str(paths["capture"]),
            capture_72h=str(paths["capture"]),
            downstream=str(paths["downstream"]),
            a_class=None,
            pnl=str(paths["pnl"]),
            filtered_winner=str(paths["filtered"]),
            exit_report=str(paths["exit"]),
            delay_report=str(paths["delay"]),
        )
        validation = build_validation(args)
        assert validation["promotion_allowed"] is False
        assert validation["hypotheses_count"] == 2
        assert validation["status_counts"][VERDICTS["DISCOVERY_WATCH"]] == 1
        assert validation["status_counts"][VERDICTS["EXIT_ONLY"]] == 1
        assert validation["hypotheses"][0]["pnl_secondary_status"] == "WATCH"
        bridge = build_filtered_winner_bridge(args, validation)
        assert bridge["filtered_winner_count"] == 1
        exit_summary = build_exit_shadow_summary(args, validation)
        assert exit_summary["exit_policy_variants_tested"] == 1
        delay_summary = build_delay_replay_summary(args, validation)
        assert delay_summary["delay_replay_done"] is True
    print("SELF_TEST_PASS strategy_memory_validation")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="/app/data/hypothesis_registry.json")
    parser.add_argument("--ingestion-summary", default="/app/data/agent_runs/latest/strategy_memory_ingestion_summary.json")
    parser.add_argument("--capture-24h", default="/app/data/agent_runs/latest/capture_discovery_24h.json")
    parser.add_argument("--capture-48h", default="/app/data/agent_runs/latest/capture_discovery_48h.json")
    parser.add_argument("--capture-72h", default="/app/data/agent_runs/latest/capture_discovery_72h.json")
    parser.add_argument("--downstream", default="/app/data/agent_runs/latest/candidate_downstream_readiness_24h.json")
    parser.add_argument("--a-class", default="/app/data/agent_runs/latest/a_class_fastlane_mode_audit_24h.json")
    parser.add_argument("--pnl", default="/app/data/agent_runs/latest/pnl_cross_secondary_24h.json")
    parser.add_argument("--filtered-winner", default="/app/data/filtered_winner_dossier_24h.json")
    parser.add_argument("--exit-report", default="/app/data/exit_policy_shadow_simulator_24h.json")
    parser.add_argument("--delay-report", default="/app/data/execution_delay_adjusted_replay_24h.json")
    parser.add_argument("--out", default="/app/data/agent_runs/latest/strategy_memory_validation_24h.json")
    parser.add_argument("--filtered-bridge-out", default="/app/data/agent_runs/latest/strategy_memory_filtered_winner_bridge.json")
    parser.add_argument("--exit-summary-out", default="/app/data/agent_runs/latest/strategy_memory_exit_shadow_summary.json")
    parser.add_argument("--delay-summary-out", default="/app/data/agent_runs/latest/strategy_memory_delay_replay_summary.json")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    validation = build_validation(args)
    write_json(args.out, validation)
    filtered_bridge = build_filtered_winner_bridge(args, validation)
    write_json(args.filtered_bridge_out, filtered_bridge)
    exit_summary = build_exit_shadow_summary(args, validation)
    write_json(args.exit_summary_out, exit_summary)
    delay_summary = build_delay_replay_summary(args, validation)
    write_json(args.delay_summary_out, delay_summary)
    print(json.dumps({
        "out": args.out,
        "strategy_memory_enabled": validation["strategy_memory_enabled"],
        "hypotheses_count": validation["hypotheses_count"],
        "status_counts": validation["status_counts"],
        "filtered_winner_bridge_out": args.filtered_bridge_out,
        "exit_summary_out": args.exit_summary_out,
        "delay_summary_out": args.delay_summary_out,
        "promotion_allowed": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
