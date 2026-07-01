#!/usr/bin/env python3
"""Summarize Strategy Memory Mining artifacts into a narrow handoff report."""

from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path

from strategy_hypothesis_registry import DEFAULT_DATA_DIR, FORBIDDEN_PRODUCTION_FILES, load_json, write_json, utc_now


def git_forbidden_changes():
    try:
        proc = subprocess.run(
            ["git", "status", "--porcelain"],
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
        )
    except Exception as exc:
        return [], {"status": "unavailable", "error": str(exc)}
    if proc.returncode != 0:
        return [], {"status": "unavailable", "error": proc.stderr.strip() or proc.stdout.strip()}
    changed = []
    for line in proc.stdout.splitlines():
        path = line[3:].strip()
        if path in FORBIDDEN_PRODUCTION_FILES:
            changed.append(path)
    return changed, {"status": "ok"}


def top_hypotheses(hypotheses, mapping, limit=10):
    by_id = {row["hypothesis_id"]: row for row in mapping.get("mappings", [])}
    rows = []
    for row in hypotheses.get("hypotheses", []):
        mapped = by_id.get(row["id"], {})
        rows.append({
            "id": row["id"],
            "name": row["name"],
            "strategy_family": row["strategy_family"],
            "priority": row.get("priority", 0),
            "mapping_status": mapped.get("mapping_status"),
            "blocked_contexts": mapped.get("blocked_contexts") or [],
            "promotion_allowed": False,
        })
    rows.sort(key=lambda item: item["priority"], reverse=True)
    return rows[:limit]


def build_report(args):
    data_dir = Path(args.data_dir)
    hypotheses = load_json(args.hypotheses or data_dir / "strategy_memory_hypotheses.json", {})
    mapping = load_json(args.mapping or data_dir / "strategy_memory_candidate_mapping.json", {})
    dossier = load_json(args.filtered_winner or data_dir / "filtered_winner_dossier_24h.json", {})
    exit_report = load_json(args.exit_report or data_dir / "exit_policy_shadow_simulator_24h.json", {})
    delay_report = load_json(args.delay_report or data_dir / "execution_delay_adjusted_replay_24h.json", {})
    forbidden, git_meta = git_forbidden_changes()
    report = {
        "schema_version": "offline_strategy_memory_audit.v1",
        "generated_at": utc_now(),
        "strategy_memory_hypotheses_count": hypotheses.get("hypotheses_count", len(hypotheses.get("hypotheses", []))),
        "mapped_to_existing_candidates": mapping.get("mapped_to_existing_candidates", 0),
        "missing_shadow_candidates": mapping.get("missing_shadow_candidates", 0),
        "rejected_future_data_hypotheses": mapping.get(
            "rejected_future_data_hypotheses",
            hypotheses.get("rejected_future_data_hypotheses_count", 0),
        ),
        "top_10_shadow_hypotheses": top_hypotheses(hypotheses, mapping, 10),
        "filtered_winner_count": dossier.get("filtered_winner_count", 0),
        "exit_policy_variants_tested": exit_report.get("exit_policy_variants_tested", 0),
        "delay_replay_done": bool(delay_report.get("delay_replay_done")),
        "forbidden_files_changed": forbidden,
        "forbidden_file_scan": git_meta,
        "promotion_allowed": False,
        "allowed_use": "shadow_only",
        "next_action": "Feed artifacts into AutoLoop as discovery-only context after clean window; do not promote or edit production strategy.",
    }
    return report


def self_test():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "strategy_memory_hypotheses.json").write_text(json.dumps({
            "hypotheses_count": 1,
            "hypotheses": [{"id": "H", "name": "h", "strategy_family": "ATH1 early scout", "priority": 10}],
            "promotion_allowed": False,
        }), encoding="utf-8")
        (root / "strategy_memory_candidate_mapping.json").write_text(json.dumps({
            "mapped_to_existing_candidates": 1,
            "missing_shadow_candidates": 0,
            "rejected_future_data_hypotheses": 1,
            "mappings": [{"hypothesis_id": "H", "mapping_status": "partial_existing_context", "blocked_contexts": []}],
        }), encoding="utf-8")
        (root / "filtered_winner_dossier_24h.json").write_text(json.dumps({"filtered_winner_count": 2}), encoding="utf-8")
        (root / "exit_policy_shadow_simulator_24h.json").write_text(json.dumps({"exit_policy_variants_tested": 10}), encoding="utf-8")
        (root / "execution_delay_adjusted_replay_24h.json").write_text(json.dumps({"delay_replay_done": True}), encoding="utf-8")
        args = argparse.Namespace(data_dir=str(root), hypotheses=None, mapping=None, filtered_winner=None, exit_report=None, delay_report=None)
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["strategy_memory_hypotheses_count"] == 1
        assert report["delay_replay_done"] is True
    print("SELF_TEST_PASS offline_strategy_memory_audit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--hypotheses", default=None)
    parser.add_argument("--mapping", default=None)
    parser.add_argument("--filtered-winner", default=None)
    parser.add_argument("--exit-report", default=None)
    parser.add_argument("--delay-report", default=None)
    parser.add_argument("--out", default=str(DEFAULT_DATA_DIR / "strategy_memory_prioritized_queue.json"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    write_json(args.out, report)
    print(json.dumps({
        "strategy_memory_hypotheses_count": report["strategy_memory_hypotheses_count"],
        "mapped_to_existing_candidates": report["mapped_to_existing_candidates"],
        "missing_shadow_candidates": report["missing_shadow_candidates"],
        "rejected_future_data_hypotheses": report["rejected_future_data_hypotheses"],
        "filtered_winner_count": report["filtered_winner_count"],
        "exit_policy_variants_tested": report["exit_policy_variants_tested"],
        "delay_replay_done": report["delay_replay_done"],
        "forbidden_files_changed": report["forbidden_files_changed"],
        "promotion_allowed": False,
        "next_action": report["next_action"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
