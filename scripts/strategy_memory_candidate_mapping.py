#!/usr/bin/env python3
"""Map strategy-memory hypotheses to the current shadow candidate catalog."""

from __future__ import annotations

import argparse
import json
import tempfile
from collections import Counter
from pathlib import Path

from strategy_hypothesis_registry import (
    DEFAULT_DATA_DIR,
    build_hypothesis_registry,
    candidate_catalog_from_code,
    historical_match_to_candidates,
    write_json,
)


def build_mapping(args):
    source = Path(args.hypotheses)
    hypotheses_report = json.loads(source.read_text(encoding="utf-8")) if source.exists() else build_hypothesis_registry("", {"source_type": "missing"})
    catalog, catalog_meta = candidate_catalog_from_code(args.registry)
    rows = [
        historical_match_to_candidates(row, catalog)
        for row in hypotheses_report.get("hypotheses", [])
    ]
    counts = Counter(row["mapping_status"] for row in rows)
    blocked = Counter()
    for row in rows:
        for blocker in row.get("blocked_contexts") or []:
            blocked[blocker] += 1
    return {
        "schema_version": "strategy_memory_candidate_mapping.v1",
        "report_type": "strategy_memory_candidate_mapping",
        "source_hypotheses": str(source),
        "candidate_catalog": catalog_meta,
        "candidate_catalog_count": len(catalog),
        "hypotheses_count": len(rows),
        "mapped_to_existing_candidates": sum(1 for row in rows if row["existing_candidate_ids"]),
        "missing_shadow_candidates": sum(1 for row in rows if row["mapping_status"].startswith("missing")),
        "rejected_future_data_hypotheses": sum(1 for row in rows if row["requires_future_data_conversion"]),
        "mapping_status_counts": dict(counts),
        "blocked_context_counts": dict(blocked),
        "promotion_allowed": False,
        "allowed_use": "shadow_only",
        "mappings": rows,
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        hpath = Path(td) / "h.json"
        report = build_hypothesis_registry("ATH#3 Super 200 V5 TP1", {"source_type": "self_test"})
        hpath.write_text(json.dumps(report), encoding="utf-8")
        args = argparse.Namespace(hypotheses=str(hpath), registry="config/entry-mode-registry.json")
        out = build_mapping(args)
        assert out["promotion_allowed"] is False
        assert out["hypotheses_count"] == report["hypotheses_count"]
        assert out["mapped_to_existing_candidates"] >= 1
    print("SELF_TEST_PASS strategy_memory_candidate_mapping")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hypotheses", default=str(DEFAULT_DATA_DIR / "strategy_memory_hypotheses.json"))
    parser.add_argument("--registry", default="config/entry-mode-registry.json")
    parser.add_argument("--out", default=str(DEFAULT_DATA_DIR / "strategy_memory_candidate_mapping.json"))
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    report = build_mapping(args)
    write_json(args.out, report)
    print(json.dumps({
        "out": args.out,
        "mapped_to_existing_candidates": report["mapped_to_existing_candidates"],
        "missing_shadow_candidates": report["missing_shadow_candidates"],
        "rejected_future_data_hypotheses": report["rejected_future_data_hypotheses"],
        "promotion_allowed": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
