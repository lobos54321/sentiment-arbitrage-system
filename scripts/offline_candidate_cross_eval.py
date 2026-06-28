#!/usr/bin/env python3
"""Score candidate shadow baselines and 2D slices.

Read-only. This is a report, not trading logic.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import statistics
import tempfile
import time
from collections import defaultdict
from pathlib import Path


DIMENSIONS = (
    "source_quote_clean",
    "source_quote_executable_proxy",
    "mode_route",
    "signal_type",
    "markov_bucket",
    "lifecycle_profile",
    "fbr_time_legal",
    "fbr_lookahead_warning",
    "hard_gate_status",
    "market_cap_bucket",
    "source_component",
    "source_resonance_state",
    "candle_pattern",
    "volume_profile",
)
CAPPED_LOSS_PCT = -10.0
CAPPED_WIN_PCT = 100.0
DEFAULT_MAX_SCAN_ROWS = 2_000_000


def jloads(raw):
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def recent_rowid_floor(db, table, max_scan_rows):
    if not max_scan_rows or max_scan_rows <= 0:
        return None
    try:
        row = db.execute(f"SELECT MAX(rowid) FROM {table}").fetchone()
        max_rowid = int(row[0] or 0) if row else 0
    except Exception:
        return None
    if max_rowid <= 0:
        return None
    return max(1, max_rowid - int(max_scan_rows) + 1)


def bucket_market_cap(value):
    try:
        mc = float(value)
    except (TypeError, ValueError):
        return "UNKNOWN"
    if not math.isfinite(mc) or mc <= 0:
        return "UNKNOWN"
    if mc < 5_000:
        return "lt5k"
    if mc < 10_000:
        return "5k_10k"
    if mc < 30_000:
        return "10k_30k"
    if mc < 100_000:
        return "30k_100k"
    return "gte100k"


def dim_value(payload, name):
    if name == "market_cap_bucket":
        return bucket_market_cap(payload.get("market_cap"))
    value = payload.get(name)
    if value in (None, ""):
        return "UNKNOWN"
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def stats(rows):
    pnls = [float(r["net_pnl_pct"]) for r in rows if r["net_pnl_pct"] is not None]
    tokens = {r["token_ca"] for r in rows if r["token_ca"]}
    closed = len(pnls)
    if not closed:
        return {
            "closed_n": 0,
            "unique_tokens": len(tokens),
            "win_rate_pct": None,
            "avg_net_pnl_pct": None,
            "median_net_pnl_pct": None,
            "profit_factor": None,
            "worst_loss_pct": None,
            "best_win_pct": None,
            "total_net_pnl_pct": 0.0,
            "drop_largest_winner_total_pct": 0.0,
            "capped_avg_net_pnl_pct": None,
        }
    wins = [p for p in pnls if p > 0]
    losses = [-p for p in pnls if p < 0]
    total = sum(pnls)
    largest_win = max(wins) if wins else 0.0
    capped = [max(CAPPED_LOSS_PCT, min(CAPPED_WIN_PCT, p)) for p in pnls]
    return {
        "closed_n": closed,
        "unique_tokens": len(tokens),
        "win_rate_pct": round(len(wins) / closed * 100, 2),
        "avg_net_pnl_pct": round(total / closed, 4),
        "capped_avg_net_pnl_pct": round(sum(capped) / closed, 4),
        "median_net_pnl_pct": round(statistics.median(pnls), 4),
        "profit_factor": round(sum(wins) / sum(losses), 4) if losses else None,
        "worst_loss_pct": round(min(pnls), 4),
        "best_win_pct": round(max(pnls), 4),
        "total_net_pnl_pct": round(total, 4),
        "drop_largest_winner_total_pct": round(total - largest_win, 4),
    }


def judge(row, *, is_slice):
    closed = row.get("closed_n") or 0
    unique = row.get("unique_tokens") or 0
    avg = row.get("avg_net_pnl_pct")
    capped_avg = row.get("capped_avg_net_pnl_pct")
    median = row.get("median_net_pnl_pct")
    pf = row.get("profit_factor")
    lift = row.get("avg_lift_pp", 0 if not is_slice else None)
    ex_max = row.get("drop_largest_winner_total_pct")
    if closed < 20:
        return "TOO_SMALL"
    if (
        is_slice
        and closed >= 50
        and unique >= 30
        and avg is not None
        and median is not None
        and capped_avg is not None
        and pf is not None
        and avg > 0
        and capped_avg > 0
        and median > 0
        and pf > 1.2
        and lift is not None
        and lift >= 2
        and ex_max is not None
        and ex_max > 0
    ):
        return "PROMISING"
    if (
        closed >= 20
        and avg is not None
        and capped_avg is not None
        and median is not None
        and pf is not None
        and avg > 0
        and capped_avg > 0
        and median > 0
        and pf > 1
        and (not is_slice or (lift is not None and lift > 0))
        and ex_max is not None
        and ex_max > 0
    ):
        return "WATCH"
    return "REJECT"


def summarize(db_path, hours, min_baseline_closed, min_baseline_unique, min_slice_closed, limit, max_scan_rows=DEFAULT_MAX_SCAN_ROWS):
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
        SELECT v.signal_id, v.token_ca, v.candidate_id, v.family, v.status,
               v.net_pnl_pct, v.observed_at, o.payload_json
        FROM candidate_shadow_virtual_trades v
        JOIN candidate_shadow_observations o
          ON o.signal_id = v.signal_id AND o.candidate_id = v.candidate_id
         AND o.observed_at = v.observed_at
        WHERE {' AND '.join(filters)}
          AND v.status = 'VIRTUAL_CLOSED'
          AND v.net_pnl_pct IS NOT NULL
        """,
        tuple(params),
    ).fetchall()
    by_candidate = defaultdict(list)
    for r in rows:
        by_candidate[r["candidate_id"]].append(r)

    baseline = []
    eligible = set()
    for candidate_id, items in by_candidate.items():
        row = {
            "candidate_id": candidate_id,
            "family": items[0]["family"],
            **stats(items),
        }
        row["judgment"] = judge(row, is_slice=False)
        baseline.append(row)
        if row["closed_n"] >= min_baseline_closed and row["unique_tokens"] >= min_baseline_unique:
            eligible.add(candidate_id)
    baseline.sort(key=lambda r: (r["avg_net_pnl_pct"] or -999, r["closed_n"]), reverse=True)
    baseline_by_id = {r["candidate_id"]: r for r in baseline}

    crosses = []
    for dim in DIMENSIONS:
        buckets = defaultdict(list)
        for r in rows:
            if r["candidate_id"] not in eligible:
                continue
            payload = jloads(r["payload_json"])
            buckets[(r["candidate_id"], dim, dim_value(payload, dim))].append(r)
        for (candidate_id, dimension, slice_value), items in buckets.items():
            if len(items) < min_slice_closed:
                continue
            base = baseline_by_id[candidate_id]
            row = {
                "candidate_id": candidate_id,
                "family": items[0]["family"],
                "dimension": dimension,
                "slice_value": slice_value,
                **stats(items),
            }
            if row["avg_net_pnl_pct"] is not None and base["avg_net_pnl_pct"] is not None:
                row["avg_lift_pp"] = round(row["avg_net_pnl_pct"] - base["avg_net_pnl_pct"], 4)
            else:
                row["avg_lift_pp"] = None
            if row["capped_avg_net_pnl_pct"] is not None and base["capped_avg_net_pnl_pct"] is not None:
                row["capped_avg_lift_pp"] = round(row["capped_avg_net_pnl_pct"] - base["capped_avg_net_pnl_pct"], 4)
            else:
                row["capped_avg_lift_pp"] = None
            if row["win_rate_pct"] is not None and base["win_rate_pct"] is not None:
                row["wr_lift_pp"] = round(row["win_rate_pct"] - base["win_rate_pct"], 2)
            else:
                row["wr_lift_pp"] = None
            row["pf_ratio"] = (
                round(row["profit_factor"] / base["profit_factor"], 4)
                if row["profit_factor"] is not None and base["profit_factor"]
                else None
            )
            row["baseline_avg_net_pnl_pct"] = base["avg_net_pnl_pct"]
            row["baseline_capped_avg_net_pnl_pct"] = base["capped_avg_net_pnl_pct"]
            row["baseline_median_net_pnl_pct"] = base["median_net_pnl_pct"]
            row["baseline_win_rate_pct"] = base["win_rate_pct"]
            row["judgment"] = judge(row, is_slice=True)
            crosses.append(row)
    crosses.sort(
        key=lambda r: (
            {"PROMISING": 3, "WATCH": 2, "TOO_SMALL": 1, "REJECT": 0}[r["judgment"]],
            r.get("capped_avg_lift_pp") if r.get("capped_avg_lift_pp") is not None else -999,
            r.get("median_net_pnl_pct") if r.get("median_net_pnl_pct") is not None else -999,
            r.get("capped_avg_net_pnl_pct") if r.get("capped_avg_net_pnl_pct") is not None else -999,
        ),
        reverse=True,
    )

    return {
        "schema_version": "offline_candidate_cross_eval.v1",
        "report_type": "pnl_cross_eval",
        "evidence_level": "discovery_same_window",
        "evidence_role": "secondary_pnl_after_match",
        "primary_question": "matched candidate virtual trade profitability",
        "does_not_answer": [
            "gold_silver_capture_recall",
            "gold_silver_capture_precision",
            "missed_gold_silver_denominator",
        ],
        "can_promote_live": False,
        "judgment_scope": "pnl_only",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "db": db_path,
        "hours": hours,
        "since_ts": since,
        "scan": {
            "max_scan_rows": max_scan_rows,
            "virtual_trades_rowid_floor": v_rowid_floor,
            "observations_rowid_floor": o_rowid_floor,
        },
        "thresholds": {
            "baseline_closed_min": min_baseline_closed,
            "baseline_unique_min": min_baseline_unique,
            "slice_closed_min": min_slice_closed,
            "capped_loss_pct": CAPPED_LOSS_PCT,
            "capped_win_pct": CAPPED_WIN_PCT,
        },
        "coverage": {
            "closed_virtual_rows": len(rows),
            "candidate_ids": len(by_candidate),
            "eligible_candidate_ids": len(eligible),
        },
        "baseline": baseline,
        "crosses": crosses[:limit],
        "judgment_counts": {
            name: sum(1 for r in crosses if r["judgment"] == name)
            for name in ("PROMISING", "WATCH", "TOO_SMALL", "REJECT")
        },
    }


def self_test():
    now = int(time.time())
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "x.db"
        db = sqlite3.connect(p)
        db.executescript(
            """
            CREATE TABLE candidate_shadow_observations(
              signal_id INTEGER, token_ca TEXT, candidate_id TEXT, family TEXT, observed_at INTEGER, payload_json TEXT
            );
            CREATE TABLE candidate_shadow_virtual_trades(
              signal_id INTEGER, token_ca TEXT, candidate_id TEXT, family TEXT,
              status TEXT, net_pnl_pct REAL, observed_at INTEGER
            );
            """
        )
        for i, pnl in enumerate([1, 2, -1, 3, 4, -2], 1):
            payload = {"source_quote_clean": i <= 4, "market_cap": 8000 if i <= 4 else 50000}
            db.execute("INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?)", (i, f"CA{i}", "cand", "base", now, json.dumps(payload)))
            db.execute("INSERT INTO candidate_shadow_virtual_trades VALUES (?,?,?,?,?,?,?)", (i, f"CA{i}", "cand", "base", "VIRTUAL_CLOSED", pnl, now))
        for i, pnl in enumerate([1000] + [-6.5] * 19, 101):
            payload = {"source_quote_clean": True, "market_cap": 8000}
            db.execute("INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?,?)", (i, f"TAIL{i}", "tail", "base", now, json.dumps(payload)))
            db.execute("INSERT INTO candidate_shadow_virtual_trades VALUES (?,?,?,?,?,?,?)", (i, f"TAIL{i}", "tail", "base", "VIRTUAL_CLOSED", pnl, now))
        db.commit()
        db.close()
        out = summarize(str(p), 1, 1, 1, 1, 50)
        assert out["coverage"]["candidate_ids"] == 2
        cand = next(r for r in out["baseline"] if r["candidate_id"] == "cand")
        assert cand["median_net_pnl_pct"] == 1.5
        assert cand["capped_avg_net_pnl_pct"] == 1.1667
        assert next(r for r in out["baseline"] if r["candidate_id"] == "tail")["judgment"] == "REJECT"
        assert any(r["dimension"] == "source_quote_clean" and r["slice_value"] == "true" for r in out["crosses"])
    print("SELF_TEST_PASS offline_candidate_cross_eval")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="data/paper_trades.db")
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--min-baseline-closed", type=int, default=30)
    ap.add_argument("--min-baseline-unique", type=int, default=20)
    ap.add_argument("--min-slice-closed", type=int, default=20)
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--max-scan-rows", type=int, default=DEFAULT_MAX_SCAN_ROWS)
    ap.add_argument("--out", default="data/offline_candidate_cross_eval.json")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return
    result = summarize(
        args.db,
        args.hours,
        args.min_baseline_closed,
        args.min_baseline_unique,
        args.min_slice_closed,
        args.limit,
        args.max_scan_rows,
    )
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"out": args.out, "coverage": result["coverage"], "judgment_counts": result["judgment_counts"]}, sort_keys=True))


if __name__ == "__main__":
    main()
