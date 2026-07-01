#!/usr/bin/env python3
"""Simulate historical exit variants in shadow only."""

from __future__ import annotations

import argparse
import json
import sqlite3
import statistics
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

from strategy_hypothesis_registry import (
    DEFAULT_DATA_DIR,
    EXIT_VARIANTS,
    is_gold_silver,
    load_raw_signal_outcomes,
    simulate_exit_variant,
    simulation_peak_pct,
    write_json,
)


def metric(values):
    nums = [v for v in values if v is not None]
    if not nums:
        return {"n": 0, "avg_net_pnl_pct": None, "median_net_pnl_pct": None, "win_rate": None}
    wins = [v for v in nums if v > 0]
    return {
        "n": len(nums),
        "avg_net_pnl_pct": round(sum(nums) / len(nums), 6),
        "median_net_pnl_pct": round(statistics.median(nums), 6),
        "win_rate": round(len(wins) / len(nums), 6),
    }


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    rows, raw_meta = load_raw_signal_outcomes(args.raw_db, hours=args.hours, now_ts=now_ts, limit=args.limit, gold_silver_only=False)
    samples = [row for row in rows if simulation_peak_pct(row) is not None]
    variant_rows = []
    for variant in EXIT_VARIANTS:
        pnls = []
        statuses = Counter()
        examples = []
        for row in samples:
            peak = simulation_peak_pct(row)
            sim = simulate_exit_variant(peak, variant)
            pnls.append(sim.get("net_pnl_pct"))
            statuses[sim.get("status")] += 1
            if len(examples) < 8:
                examples.append({
                    "signal_id": row.get("signal_id"),
                    "token_ca": row.get("token_ca"),
                    "symbol": row.get("symbol"),
                    "tier": row.get("tier"),
                    "peak_pct": peak,
                    "simulated": sim,
                })
        variant_rows.append({
            "variant_id": variant["id"],
            "variant_name": variant["name"],
            "simulation_basis": "peak_only_shadow_proxy_unless_real_kline_path_is_joined_later",
            **metric(pnls),
            "status_counts": dict(statuses),
            "examples": examples,
            "promotion_allowed": False,
        })
    variant_rows.sort(key=lambda r: (r["avg_net_pnl_pct"] is not None, r["avg_net_pnl_pct"] or -999), reverse=True)
    return {
        "schema_version": "exit_policy_shadow_simulator.v1",
        "report_type": f"exit_policy_shadow_simulator_{int(args.hours)}h",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts)),
        "hours": args.hours,
        "raw_meta": raw_meta,
        "sample_count": len(samples),
        "gold_silver_sample_count": sum(1 for row in samples if is_gold_silver(row)),
        "exit_policy_variants_tested": len(EXIT_VARIANTS),
        "promotion_allowed": False,
        "allowed_use": "shadow_only",
        "live_position_monitor_changed": False,
        "warning": "Peak-only simulation is not execution evidence; use real kline path before interpreting exit ordering.",
        "variants": variant_rows,
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        raw = Path(td) / "raw.db"
        now = int(time.time())
        db = sqlite3.connect(raw)
        db.execute("CREATE TABLE raw_signal_outcomes(signal_id INTEGER, token_ca TEXT, symbol TEXT, signal_ts INTEGER, raw_primary_tier TEXT, max_sustained_peak_pct REAL, payload_json TEXT)")
        db.execute("INSERT INTO raw_signal_outcomes VALUES (1,'CA','AAA',?,'gold',120,'{}')", (now,))
        db.execute("INSERT INTO raw_signal_outcomes VALUES (2,'CB','BBB',?,'silver',10,'{}')", (now,))
        db.commit(); db.close()
        args = argparse.Namespace(raw_db=str(raw), hours=24, now_ts=now, limit=10)
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["exit_policy_variants_tested"] >= 10
        assert report["sample_count"] == 2
    print("SELF_TEST_PASS exit_policy_shadow_simulator")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-db", default=str(DEFAULT_DATA_DIR / "raw_signal_outcomes.db"))
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--out", default=str(DEFAULT_DATA_DIR / "exit_policy_shadow_simulator_24h.json"))
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
        "exit_policy_variants_tested": report["exit_policy_variants_tested"],
        "promotion_allowed": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
