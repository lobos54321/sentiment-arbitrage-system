#!/usr/bin/env python3
"""Mine historical strategy notes into structured shadow-only hypotheses."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from strategy_hypothesis_registry import (
    DEFAULT_DATA_DIR,
    build_hypothesis_registry,
    load_source_text,
    write_json,
)


def build_report(args):
    text, meta = load_source_text(source_docx=args.source_docx, source_text=args.source_text)
    report = build_hypothesis_registry(text, meta, include_x_placeholder=not args.no_x_placeholder)
    report["report_type"] = "strategy_memory_hypotheses"
    report["notes"] = [
        "Historical PnL is not promotion evidence.",
        "Same-window discovery is not promotion evidence.",
        "Future features such as max_ath or later peak are labels only.",
        "All hypotheses are shadow-only.",
    ]
    return report


def self_test():
    with tempfile.TemporaryDirectory() as td:
        source = Path(td) / "notes.txt"
        out = Path(td) / "strategy_memory_hypotheses.json"
        source.write_text(
            "V17.4 ATH#1 MC$20-75K SupΔ≥15 TΔ≥1\n"
            "V18 ATH#2 TΔ≥2 MC50K-150K SecΔ<8\n"
            "ATH#3 Sec≤20 Addr≥15 DynSL 15/35/55\n"
            "SKIP 错杀 MC>=50K\n",
            encoding="utf-8",
        )
        args = argparse.Namespace(source_text=str(source), source_docx=None, no_x_placeholder=False)
        report = build_report(args)
        write_json(out, report)
        loaded = json.loads(out.read_text(encoding="utf-8"))
        assert loaded["promotion_allowed"] is False
        assert loaded["hypotheses_count"] >= 12
        assert all(row["allowed_use"] == "shadow_only" for row in loaded["hypotheses"])
    print("SELF_TEST_PASS strategy_memory_mining")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-docx", default=None)
    parser.add_argument("--source-text", default=None)
    parser.add_argument("--out", default=str(DEFAULT_DATA_DIR / "strategy_memory_hypotheses.json"))
    parser.add_argument("--no-x-placeholder", action="store_true")
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
        "out": args.out,
        "strategy_memory_hypotheses_count": report["hypotheses_count"],
        "rejected_future_data_hypotheses": report["rejected_future_data_hypotheses_count"],
        "promotion_allowed": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
