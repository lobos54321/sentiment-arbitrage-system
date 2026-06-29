#!/usr/bin/env python3
"""Generate a Codex handoff from a capture discovery reviewer verdict.

The handoff is for data/report/evaluator fixes only. It must not recommend
strategy, entry policy, hard gate, exit gate, executor, canary, or risk changes.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path


SCHEMA_VERSION = "capture_discovery_codex_handoff.v1"
FIXABLE_BLOCKER_HINTS = {
    "raw_dog_rows_incomplete": "Fix raw dog row materialization or report input wiring before judging capture recall.",
    "raw_gold_silver_denominator_rows_truncated": "Ensure the raw dog JSON/API includes complete event rows or use --raw-db.",
    "raw_dog_candidate_observation_join_incomplete": "Audit signal_id/token join between raw_signal_outcomes and candidate_shadow_observations.",
    "raw_all_dog_candidate_observation_join_incomplete": "Audit raw_all denominator join coverage; missing observations block business recall.",
    "signal_id_join_rate_below_99pct": "Normalize signal_id keys and inspect missing raw dog observation rows.",
    "schema_mixed_quote_sensitive_slices_blocked": "Wait for clean v2 rows or split report by context_schema_version before judging quote-sensitive slices.",
    "context_schema_v2_coverage_below_95pct_quote_sensitive_slices_blocked": "Do not judge quote-sensitive candidates until v2 schema coverage is at least 95%.",
    "quote_clean_definition_v2_coverage_below_95pct_quote_sensitive_slices_blocked": "Do not judge quote-sensitive candidates until quote_clean_definition v2 coverage is at least 95%.",
    "candidate_count_observed_not_84": "Inspect candidate shadow observer coverage and catalog consistency.",
    "candidate_count_mismatch": "Inspect candidate shadow observer expected/observed candidate counts.",
    "observation_coverage_below_99pct": "Inspect per-signal observation coverage and missing candidate rows.",
    "per_signal_candidate_coverage_incomplete": "Inspect bad signal rows in candidate coverage and rerun candidate shadow observer if needed.",
    "tests_failed": "Fix report/evaluator tests before using the run verdict.",
}


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_json(path):
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_text(path, text):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(target)


def handoff_needed(verdict):
    blockers = verdict.get("blockers") or []
    return any(blocker in FIXABLE_BLOCKER_HINTS for blocker in blockers)


def build_handoff(verdict):
    blockers = verdict.get("blockers") or []
    needed = handoff_needed(verdict)
    lines = [
        "# Gold/Silver Capture Discovery Codex Handoff",
        "",
        f"- schema_version: `{SCHEMA_VERSION}`",
        f"- generated_at: `{utc_now()}`",
        f"- verdict: `{verdict.get('classification')}`",
        f"- promotion_allowed: `{str(bool(verdict.get('promotion_allowed'))).lower()}`",
        f"- handoff_needed: `{str(needed).lower()}`",
        "",
        "## Guardrails",
        "",
        "- Do not change strategy, entry policy, hard gates, exit gates, live executor, canary size, wallet config, or risk settings.",
        "- Only fix data, report, evaluator, API, or test issues needed to make the discovery verdict auditable.",
        "- Same-window discovery output cannot promote live trading.",
        "",
        "## Current Integrity Snapshot",
        "",
        f"- candidate_count_expected: `{verdict.get('candidate_count_expected')}`",
        f"- candidate_count_observed: `{verdict.get('candidate_count_observed')}`",
        f"- observation_coverage_pct: `{verdict.get('observation_coverage_pct')}`",
        f"- raw_dog_rows_complete: `{str(bool(verdict.get('raw_dog_rows_complete'))).lower()}`",
        f"- signal_id_join_rate: `{verdict.get('signal_id_join_rate')}`",
        f"- raw_all_signal_id_join_rate: `{verdict.get('raw_all_signal_id_join_rate')}`",
        f"- mesh_eligible_signal_id_join_rate: `{verdict.get('mesh_eligible_signal_id_join_rate')}`",
        "",
    ]
    quote = verdict.get("quote_clean_definition") or {}
    lines.extend([
        "## Schema / Quote State",
        "",
        f"- context_schema_version_counts: `{json.dumps(verdict.get('context_schema_version_counts') or {}, sort_keys=True)}`",
        f"- quote_clean_definition: `{json.dumps(quote, sort_keys=True)}`",
        "",
    ])
    if not needed:
        lines.extend([
            "## Next Action",
            "",
            "No Codex data/report/evaluator fix is required by this verdict. Continue collecting clean discovery data and rerun the loop.",
            "",
        ])
    else:
        lines.extend(["## Required Fixes", ""])
        for blocker in blockers:
            hint = FIXABLE_BLOCKER_HINTS.get(blocker)
            if hint:
                lines.append(f"- `{blocker}`: {hint}")
        lines.append("")
    reconciliation = verdict.get("signal_identity_reconciliation") or {}
    if reconciliation:
        lines.extend([
            "## Signal Identity Reconciliation",
            "",
            "```json",
            json.dumps(
                {
                    key: reconciliation.get(key)
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
        ])
    lines.extend([
        "## H1 / H2",
        "",
        "### H1",
        "",
        "```json",
        json.dumps(verdict.get("H1_capture_metrics") or {}, indent=2, sort_keys=True),
        "```",
        "",
        "### H2",
        "",
        "```json",
        json.dumps(verdict.get("H2_capture_metrics") or {}, indent=2, sort_keys=True),
        "```",
        "",
        "## Secondary Evidence",
        "",
        "```json",
        json.dumps(
            {
                "PnL_cross_secondary_status": verdict.get("PnL_cross_secondary_status"),
                "virtual_Markov_discovery_status": verdict.get("virtual_Markov_discovery_status"),
            },
            indent=2,
            sort_keys=True,
        ),
        "```",
        "",
    ])
    return "\n".join(lines)


def self_test():
    verdict = {
        "classification": "BLOCKED_DATA",
        "promotion_allowed": False,
        "blockers": ["raw_dog_rows_incomplete"],
        "candidate_count_expected": 84,
        "candidate_count_observed": 84,
        "observation_coverage_pct": 100.0,
        "raw_dog_rows_complete": False,
        "signal_id_join_rate": 1.0,
        "context_schema_version_counts": {"v": 1},
        "quote_clean_definition": {"counts": {"q": 1}},
        "H1_capture_metrics": {"status": "WATCH"},
        "H2_capture_metrics": {"status": "not_observed"},
    }
    text = build_handoff(verdict)
    assert "handoff_needed: `true`" in text
    assert "raw_dog_rows_incomplete" in text
    verdict["blockers"] = []
    text = build_handoff(verdict)
    assert "handoff_needed: `false`" in text
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "handoff.md"
        write_text(path, text)
        assert path.read_text(encoding="utf-8").startswith("# Gold/Silver")
    print("SELF_TEST_PASS generate_codex_handoff")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verdict", required=False)
    parser.add_argument("--out", default="data/agent_handoffs/latest_codex_handoff.md")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not args.verdict:
        raise SystemExit("--verdict is required unless --self-test is used")
    verdict = load_json(args.verdict)
    text = build_handoff(verdict)
    write_text(args.out, text)
    print(json.dumps({"out": args.out, "handoff_needed": handoff_needed(verdict)}, sort_keys=True))


if __name__ == "__main__":
    main()
