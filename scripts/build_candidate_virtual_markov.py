#!/usr/bin/env python3
"""Build a research-only Markov bucket from candidate virtual outcomes."""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

from offline_candidate_cross_eval import DEFAULT_MAX_SCAN_ROWS, dim_value, jloads, recent_rowid_floor, stats


KEY_DIMENSIONS = (
    "candidate_id",
    "lifecycle_profile",
    "source_component",
    "source_resonance_state",
    "source_quote_clean",
    "candle_pattern",
    "volume_profile",
    "market_cap_bucket",
)
PROFILES = {
    "strict": KEY_DIMENSIONS,
    "kline": (
        "candidate_id",
        "candle_pattern",
        "volume_profile",
        "market_cap_bucket",
    ),
    "runtime": (
        "candidate_id",
        "lifecycle_profile",
        "source_component",
        "source_resonance_state",
        "source_quote_clean",
    ),
}


def bucket(row):
    n = row["closed_n"]
    unique = row["unique_tokens"]
    median = row["median_net_pnl_pct"]
    capped_avg = row["capped_avg_net_pnl_pct"]
    pf = row["profit_factor"]
    drop_max = row["drop_largest_winner_total_pct"]
    if n < 20:
        return "insufficient"
    if (
        n >= 50
        and unique >= 30
        and median is not None
        and capped_avg is not None
        and pf is not None
        and median > 0
        and capped_avg > 0
        and pf > 1.2
        and drop_max > 0
    ):
        return "green"
    if capped_avg is not None and pf is not None and capped_avg > 0 and pf > 1 and drop_max > 0:
        return "yellow"
    return "red"


def key_for(row, payload, dimensions):
    values = {"candidate_id": row["candidate_id"]}
    for name in dimensions:
        if name == "candidate_id":
            continue
        values[name] = dim_value(payload, name)
    return tuple(values[name] for name in dimensions), values


def build(db_path, hours, min_closed, limit, profile, max_scan_rows=DEFAULT_MAX_SCAN_ROWS):
    dimensions = PROFILES[profile]
    since = int(time.time()) - hours * 3600
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    v_rowid_floor = recent_rowid_floor(db, "candidate_shadow_virtual_trades", max_scan_rows)
    o_rowid_floor = recent_rowid_floor(db, "candidate_shadow_observations", max_scan_rows)
    filters = ["v.observed_at >= ?"]
    params = [since]
    if v_rowid_floor is not None:
        filters.append("v.rowid >= ?")
        params.append(v_rowid_floor)
    if o_rowid_floor is not None:
        filters.append("o.rowid >= ?")
        params.append(o_rowid_floor)
    rows = db.execute(
        f"""
        SELECT v.signal_id, v.token_ca, v.candidate_id, v.family, v.net_pnl_pct,
               v.observed_at, o.payload_json
        FROM candidate_shadow_virtual_trades v
        JOIN candidate_shadow_observations o
          ON o.signal_id = v.signal_id
         AND o.candidate_id = v.candidate_id
         AND o.observed_at = v.observed_at
        WHERE {' AND '.join(filters)}
          AND v.status = 'VIRTUAL_CLOSED'
          AND v.net_pnl_pct IS NOT NULL
        """,
        tuple(params),
    ).fetchall()
    groups = defaultdict(list)
    meta = {}
    for row in rows:
        payload = jloads(row["payload_json"])
        key, values = key_for(row, payload, dimensions)
        groups[key].append(row)
        meta[key] = values

    buckets = []
    for key, items in groups.items():
        if len(items) < min_closed:
            continue
        result = {**meta[key], **stats(items)}
        result["bucket"] = bucket(result)
        buckets.append(result)
    buckets.sort(
        key=lambda r: (
            {"green": 3, "yellow": 2, "red": 1, "insufficient": 0}[r["bucket"]],
            r["closed_n"],
            r.get("capped_avg_net_pnl_pct") if r.get("capped_avg_net_pnl_pct") is not None else -999,
        ),
        reverse=True,
    )
    return {
        "schema_version": "candidate_virtual_markov.v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "db": db_path,
        "hours": hours,
        "since_ts": since,
        "profile": profile,
        "key_dimensions": list(dimensions),
        "thresholds": {
            "min_closed_to_emit": min_closed,
            "green_closed_min": 50,
            "green_unique_min": 30,
            "yellow_closed_min": 20,
        },
        "coverage": {
            "closed_virtual_rows": len(rows),
            "keys_emitted": len(buckets),
            "bucket_counts": dict(Counter(row["bucket"] for row in buckets)),
            "scan": {
                "max_scan_rows": max_scan_rows,
                "virtual_trades_rowid_floor": v_rowid_floor,
                "observations_rowid_floor": o_rowid_floor,
            },
        },
        "buckets": buckets[:limit],
    }


def self_test():
    now = int(time.time())
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "x.db"
        db = sqlite3.connect(path)
        db.executescript(
            """
            CREATE TABLE candidate_shadow_observations(
              signal_id INTEGER, token_ca TEXT, candidate_id TEXT, observed_at INTEGER, payload_json TEXT
            );
            CREATE TABLE candidate_shadow_virtual_trades(
              signal_id INTEGER, token_ca TEXT, candidate_id TEXT, family TEXT,
              status TEXT, net_pnl_pct REAL, observed_at INTEGER
            );
            """
        )
        pnls = [5.0] * 35 + [-1.0] * 20
        for i, pnl in enumerate(pnls, 1):
            payload = {
                "lifecycle_profile": "FIRST_PUMP:PROBE",
                "source_component": "test",
                "source_resonance_state": "test:ok",
                "source_quote_clean": True,
                "candle_pattern": "green",
                "volume_profile": "building",
                "market_cap": 20_000,
            }
            db.execute("INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?)", (i, f"CA{i}", "cand", now, json.dumps(payload)))
            db.execute("INSERT INTO candidate_shadow_virtual_trades VALUES (?,?,?,?,?,?,?)", (i, f"CA{i}", "cand", "base", "VIRTUAL_CLOSED", pnl, now))
        db.commit()
        db.close()
        out = build(str(path), 1, 20, 10, "kline")
        assert out["coverage"]["closed_virtual_rows"] == len(pnls)
        assert out["buckets"][0]["bucket"] == "green"
        assert out["buckets"][0]["candidate_id"] == "cand"
        assert out["profile"] == "kline"
    print("SELF_TEST_PASS candidate_virtual_markov")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="data/paper_trades.db")
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--min-closed", type=int, default=20)
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--profile", choices=sorted(PROFILES), default="strict")
    parser.add_argument("--max-scan-rows", type=int, default=DEFAULT_MAX_SCAN_ROWS)
    parser.add_argument("--out", default="data/candidate_virtual_markov.json")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    result = build(args.db, args.hours, args.min_closed, args.limit, args.profile, args.max_scan_rows)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"out": args.out, "coverage": result["coverage"]}, sort_keys=True))


if __name__ == "__main__":
    main()
