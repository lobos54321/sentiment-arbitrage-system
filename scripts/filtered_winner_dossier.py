#!/usr/bin/env python3
"""Build a shadow-only dossier for missed or filtered gold/silver winners."""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from pathlib import Path

from strategy_hypothesis_registry import (
    DEFAULT_DATA_DIR,
    ath_stage_from,
    classify_final_blocker,
    extract_index_snapshot,
    group_candidate_matches,
    is_gold_silver,
    load_candidate_observations,
    load_raw_signal_outcomes,
    load_x_context,
    market_cap_bucket,
    row_market_cap,
    simulation_peak_pct,
    stage_sets_from_paper,
    write_json,
)


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    raw_rows, raw_meta = load_raw_signal_outcomes(
        args.raw_db, hours=args.hours, now_ts=now_ts, limit=args.limit * 20, gold_silver_only=False
    )
    raw_targets = [
        row for row in raw_rows
        if is_gold_silver(row) or (simulation_peak_pct(row) is not None and simulation_peak_pct(row) >= 100)
    ]
    signal_ids = [row["signal_id_key"] for row in raw_targets if row.get("signal_id_key")]
    observations, obs_meta = load_candidate_observations(args.db, signal_ids=signal_ids, hours=args.hours, now_ts=now_ts)
    matches = group_candidate_matches(observations)
    stage_sets, stage_meta = stage_sets_from_paper(args.db, signal_ids, hours=args.hours, now_ts=now_ts)
    x_context = load_x_context(args.x_context)
    missed = []
    for row in raw_targets:
        sid = row.get("signal_id_key")
        cand = matches.get(sid) or []
        downstream = {
            "decision": sid in stage_sets.get("decision", set()),
            "pass_allow": sid in stage_sets.get("pass_allow", set()),
            "pending": sid in stage_sets.get("pending", set()),
            "final_entry": sid in stage_sets.get("final_entry", set()),
            "paper": sid in stage_sets.get("paper", set()),
            "realized": sid in stage_sets.get("realized", set()),
        }
        final_blocker = classify_final_blocker(row, cand, stage_sets, stage_meta, x_available=x_context["available"])
        if args.only_missed and final_blocker == "paper_or_realized_seen":
            continue
        payload = row.get("payload") or {}
        idx = extract_index_snapshot(payload)
        mc = row_market_cap(row)
        x_row = x_context["by_signal"].get(sid) if x_context["available"] else None
        missed.append({
            "token": row.get("symbol") or row.get("token_ca"),
            "token_ca": row.get("token_ca"),
            "signal_id": row.get("signal_id"),
            "signal_ts": row.get("signal_ts_norm"),
            "signal_ts_iso": row.get("signal_ts_iso"),
            "ath_stage": ath_stage_from(row, payload),
            "mc_bucket": market_cap_bucket(mc),
            "market_cap": mc,
            "tier": row.get("tier"),
            "peak_pct": simulation_peak_pct(row),
            "index_snapshot": idx,
            "index_deltas": {key: idx.get(key) for key in idx if key.endswith("_delta")},
            "mc_change_1m": payload.get("mc_change_1m"),
            "mc_change_5m": payload.get("mc_change_5m"),
            "mc_change_10m": payload.get("mc_change_10m"),
            "mc_change_15m": payload.get("mc_change_15m"),
            "candidate_matches": [c["candidate_id"] for c in cand[:20]],
            "candidate_match_count": len(cand),
            "downstream_status": downstream,
            "final_blocker": final_blocker,
            "x_narrative_context_available": bool(x_row),
            "x_narrative_stage": (x_row or {}).get("x_narrative_stage") or (x_row or {}).get("narrative_stage"),
        })
        if len(missed) >= args.limit:
            break
    blocker_counts = {}
    for item in missed:
        blocker_counts[item["final_blocker"]] = blocker_counts.get(item["final_blocker"], 0) + 1
    return {
        "schema_version": "filtered_winner_dossier.v1",
        "report_type": f"filtered_winner_dossier_{int(args.hours)}h",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts)),
        "hours": args.hours,
        "raw_meta": raw_meta,
        "candidate_observation_meta": obs_meta,
        "stage_meta": stage_meta,
        "target_count": len(raw_targets),
        "filtered_winner_count": len(missed),
        "final_blocker_counts": blocker_counts,
        "promotion_allowed": False,
        "allowed_use": "shadow_only",
        "historical_pnl_is_promotion_evidence": False,
        "missed_winners": missed,
    }


def self_test():
    with tempfile.TemporaryDirectory() as td:
        raw = Path(td) / "raw.db"
        paper = Path(td) / "paper.db"
        db = sqlite3.connect(raw)
        db.execute(
            "CREATE TABLE raw_signal_outcomes(signal_id INTEGER, token_ca TEXT, symbol TEXT, signal_ts INTEGER, signal_type TEXT, raw_sustained_tier TEXT, max_sustained_peak_pct REAL, payload_json TEXT)"
        )
        now = int(time.time())
        db.execute("INSERT INTO raw_signal_outcomes VALUES (1,'CA','AAA',?,'ATH','gold',120,?)", (now, json.dumps({"market_cap": 70000, "super_index": 220})))
        db.commit(); db.close()
        db = sqlite3.connect(paper)
        db.execute("CREATE TABLE candidate_shadow_observations(signal_id INTEGER, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT, matched INTEGER, reason TEXT, observed_at INTEGER, payload_json TEXT)")
        db.execute("INSERT INTO candidate_shadow_observations VALUES (1,'CA',?,'historical:smart_backtest_76wr','historical',1,'test',?,?)", (now, now, "{}"))
        db.commit(); db.close()
        args = argparse.Namespace(raw_db=str(raw), db=str(paper), hours=24, now_ts=now, limit=10, x_context=None, only_missed=False)
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["filtered_winner_count"] == 1
        assert report["missed_winners"][0]["candidate_match_count"] == 1
    print("SELF_TEST_PASS filtered_winner_dossier")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(DEFAULT_DATA_DIR / "paper_trades.db"))
    parser.add_argument("--raw-db", default=str(DEFAULT_DATA_DIR / "raw_signal_outcomes.db"))
    parser.add_argument("--x-context", default=str(DEFAULT_DATA_DIR / "x_narrative_context_24h.json"))
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--only-missed", action="store_true")
    parser.add_argument("--out", default=None)
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    if not args.out:
        args.out = str(DEFAULT_DATA_DIR / f"filtered_winner_dossier_{int(args.hours)}h.json")
    report = build_report(args)
    write_json(args.out, report)
    print(json.dumps({
        "out": args.out,
        "filtered_winner_count": report["filtered_winner_count"],
        "promotion_allowed": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
