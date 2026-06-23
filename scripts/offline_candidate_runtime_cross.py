#!/usr/bin/env python3
"""Offline join for candidate shadow rows + runtime/source evidence.

No dashboard, no DB writes. Use this for old overnight rows.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import tempfile
import time
from collections import defaultdict
from pathlib import Path


WINDOW_SEC = 600
DIMENSIONS = (
    "markov_bucket",
    "lifecycle_profile",
    "lifecycle_state",
    "source_resonance_state",
    "source_quote_clean",
    "source_quote_executable_proxy",
)


def jloads(raw):
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def boolish(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    return str(v).lower() in {"1", "true", "yes", "y"}


def ts(v):
    if v is None:
        return None
    try:
        n = float(v)
    except Exception:
        return None
    if not math.isfinite(n) or n <= 0:
        return None
    return int(n / 1000) if n > 10_000_000_000 else int(n)


def table_exists(db, name):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def cols(db, table):
    return {r[1] for r in db.execute(f"PRAGMA table_info({table})")} if table_exists(db, table) else set()


def col(c, name, fallback="NULL"):
    return name if name in c else f"{fallback} AS {name}"


def pick(payload, *paths):
    for path in paths:
        cur = payload
        for key in path:
            if not isinstance(cur, dict) or key not in cur:
                cur = None
                break
            cur = cur[key]
        if cur not in (None, ""):
            return cur
    return None


def markov(payload):
    v = pick(
        payload,
        ("gate", "markov_bucket"),
        ("markov_reclaim_forecast", "gate", "markov_bucket"),
        ("lotto_markov_reclaim_forecast", "gate", "markov_bucket"),
        ("revival_canary", "markov_bucket"),
        ("learning_bypass", "markov_bucket"),
        ("markov_bucket",),
    )
    return str(v).lower() if v not in (None, "") else None


def nearest(index, token, signal_ts):
    rows = index.get(token) or []
    if signal_ts is None:
        return {}
    best = None
    best_gap = WINDOW_SEC + 1
    for row_ts, data in rows:
        gap = abs(row_ts - signal_ts)
        if gap < best_gap:
            best_gap, best = gap, data
    return best or {}


def load_source(db, since):
    if not table_exists(db, "source_resonance_candidates"):
        return {}
    c = cols(db, "source_resonance_candidates")
    rows = db.execute(
        f"""
        SELECT {col(c,'token_ca')}, {col(c,'signal_ts')}, {col(c,'cohort')},
               {col(c,'quote_clean_seen')}, {col(c,'two_quote_clean_snapshots')},
               {col(c,'entry_quote_success_seen')}, {col(c,'entry_quote_fail_seen')},
               {col(c,'resonance_level')}, {col(c,'resonance_score')}
        FROM source_resonance_candidates
        WHERE signal_ts >= ?
        """,
        (since - WINDOW_SEC,),
    ).fetchall()
    out = defaultdict(list)
    for r in rows:
        row_ts = ts(r["signal_ts"])
        token = r["token_ca"]
        if not token or row_ts is None:
            continue
        quote_clean = boolish(r["quote_clean_seen"]) or boolish(r["two_quote_clean_snapshots"]) or boolish(r["entry_quote_success_seen"])
        state = r["cohort"] or ("level_%s" % r["resonance_level"] if r["resonance_level"] is not None else "seen")
        out[token].append((row_ts, {
            "source_resonance_state": state,
            "source_quote_clean": "true" if quote_clean else "false",
            "source_quote_executable_proxy": "true" if quote_clean else "false",
        }))
    return out


def load_runtime(db, since):
    by_signal = {}
    by_token = defaultdict(list)
    for table in ("paper_decision_events", "paper_missed_signal_attribution"):
        if not table_exists(db, table):
            continue
        c = cols(db, table)
        ts_names = [n for n in ("event_ts", "signal_ts", "created_event_ts", "baseline_ts") if n in c]
        ts_expr = ts_names[0] if len(ts_names) == 1 else ("COALESCE(%s,0)" % ",".join(ts_names) if ts_names else "0")
        rows = db.execute(
            f"""
            SELECT {col(c,'signal_id')}, {col(c,'token_ca')}, {col(c,'lifecycle_state')},
                   {col(c,'entry_bias')}, {col(c,'vitality_score')}, {col(c,'payload_json')},
                   {ts_expr} AS runtime_ts
            FROM {table}
            WHERE {ts_expr} >= ?
            """,
            (since - WINDOW_SEC,),
        ).fetchall()
        for r in rows:
            payload = jloads(r["payload_json"])
            state = r["lifecycle_state"] or payload.get("lifecycle_state") or pick(payload, ("lifecycle", "state"), ("revival_canary", "lifecycle_state"))
            bias = r["entry_bias"] or payload.get("entry_bias") or pick(payload, ("lifecycle", "entry_bias"), ("revival_canary", "entry_bias"))
            data = {
                "markov_bucket": markov(payload) or "UNKNOWN",
                "lifecycle_state": str(state) if state not in (None, "") else "UNKNOWN",
                "lifecycle_profile": ":".join(str(x) for x in (state, bias) if x not in (None, "")) or "UNKNOWN",
            }
            if data["markov_bucket"] == "UNKNOWN" and data["lifecycle_profile"] == "UNKNOWN":
                continue
            if r["signal_id"] is not None:
                by_signal.setdefault(int(r["signal_id"]), data)
            row_ts = ts(r["runtime_ts"])
            if r["token_ca"] and row_ts is not None:
                by_token[r["token_ca"]].append((row_ts, data))
    return by_signal, by_token


def dim(features, name):
    return str(features.get(name) or "UNKNOWN")


def summarize(candidate_db, runtime_db, hours, min_closed, limit):
    since = int(time.time()) - hours * 3600
    cdb = sqlite3.connect(candidate_db)
    rdb = sqlite3.connect(runtime_db)
    cdb.row_factory = rdb.row_factory = sqlite3.Row

    obs = cdb.execute(
        """
        SELECT signal_id, token_ca, signal_ts, candidate_id, family, payload_json
        FROM candidate_shadow_observations
        WHERE observed_at >= ?
        """,
        (since,),
    ).fetchall()
    trades = cdb.execute(
        """
        SELECT signal_id, token_ca, candidate_id, family, status, entry_ts, exit_ts, net_pnl_pct
        FROM candidate_shadow_virtual_trades
        WHERE observed_at >= ?
        """,
        (since,),
    ).fetchall()
    source = load_source(rdb, since)
    runtime_by_signal, runtime_by_token = load_runtime(rdb, since)

    features = {}
    signals = {}
    for r in obs:
        signal_ts = ts(r["signal_ts"])
        f = jloads(r["payload_json"])
        f.update(nearest(source, r["token_ca"], signal_ts))
        f.update(runtime_by_signal.get(int(r["signal_id"])) or nearest(runtime_by_token, r["token_ca"], signal_ts))
        features[(int(r["signal_id"]), r["candidate_id"])] = f
        signals.setdefault(int(r["signal_id"]), f)

    out = {}
    for d in DIMENSIONS:
        buckets = {}
        for t in trades:
            f = features.get((int(t["signal_id"]), t["candidate_id"]), {})
            key = (t["candidate_id"], t["family"], dim(f, d))
            b = buckets.setdefault(key, {"tokens": set(), "n": 0, "closed": 0, "wins": 0, "sum": 0.0, "gw": 0.0, "gl": 0.0})
            b["n"] += 1
            b["tokens"].add(t["token_ca"])
            if t["status"] != "VIRTUAL_CLOSED" or t["net_pnl_pct"] is None:
                continue
            pnl = float(t["net_pnl_pct"])
            b["closed"] += 1
            b["wins"] += int(pnl > 0)
            b["sum"] += pnl
            b["gw"] += pnl if pnl > 0 else 0.0
            b["gl"] += -pnl if pnl < 0 else 0.0
        rows = []
        for (candidate_id, family, slice_value), b in buckets.items():
            if b["closed"] < min_closed:
                continue
            rows.append({
                "candidate_id": candidate_id,
                "family": family,
                "dimension": d,
                "slice_value": slice_value,
                "virtual_rows": b["n"],
                "closed_n": b["closed"],
                "unique_tokens": len(b["tokens"]),
                "win_rate_pct": round(b["wins"] / b["closed"] * 100, 2),
                "avg_net_pnl_pct": round(b["sum"] / b["closed"], 4),
                "total_net_pnl_pct": round(b["sum"], 4),
                "profit_factor": round(b["gw"] / b["gl"], 4) if b["gl"] else None,
            })
        out[d] = sorted(rows, key=lambda x: x["avg_net_pnl_pct"], reverse=True)[:limit]

    return {
        "schema_version": "offline_candidate_runtime_cross.v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "candidate_db": candidate_db,
        "runtime_db": runtime_db,
        "hours": hours,
        "since_ts": since,
        "coverage": {
            "signals": len(signals),
            "observation_rows": len(obs),
            "virtual_rows": len(trades),
            "candidate_ids": len({r["candidate_id"] for r in obs}),
            "source_seen_signals": sum(1 for f in signals.values() if f.get("source_resonance_state")),
            "markov_seen_signals": sum(1 for f in signals.values() if f.get("markov_bucket")),
            "lifecycle_seen_signals": sum(1 for f in signals.values() if f.get("lifecycle_profile") or f.get("lifecycle_state")),
        },
        "dimensions": out,
    }


def self_test():
    now = int(time.time())
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "x.db"
        db = sqlite3.connect(p)
        db.executescript("""
        CREATE TABLE candidate_shadow_observations(signal_id INTEGER, token_ca TEXT, signal_ts INTEGER, candidate_id TEXT, family TEXT, observed_at INTEGER, payload_json TEXT);
        CREATE TABLE candidate_shadow_virtual_trades(signal_id INTEGER, token_ca TEXT, candidate_id TEXT, family TEXT, status TEXT, entry_ts INTEGER, exit_ts INTEGER, net_pnl_pct REAL, observed_at INTEGER);
        CREATE TABLE source_resonance_candidates(token_ca TEXT, signal_ts INTEGER, cohort TEXT, quote_clean_seen INTEGER, two_quote_clean_snapshots INTEGER, entry_quote_success_seen INTEGER, entry_quote_fail_seen INTEGER, resonance_level INTEGER, resonance_score REAL);
        CREATE TABLE paper_decision_events(signal_id INTEGER, token_ca TEXT, event_ts INTEGER, lifecycle_state TEXT, entry_bias TEXT, vitality_score REAL, payload_json TEXT);
        """)
        db.execute("INSERT INTO candidate_shadow_observations VALUES (1,'CA',?, 'cand', 'base', ?, '{}')", (now - 60, now - 10))
        db.execute("INSERT INTO candidate_shadow_virtual_trades VALUES (1,'CA','cand','base','VIRTUAL_CLOSED',?,?,5,?)", (now - 50, now - 20, now - 10))
        db.execute("INSERT INTO source_resonance_candidates VALUES ('CA',?,'clean',1,0,0,0,1,1.0)", (now - 61,))
        db.execute("INSERT INTO paper_decision_events VALUES (1,'CA',?,'RECLAIM','PROBE',1.0,?)", (now - 59, json.dumps({"markov_reclaim_forecast": {"gate": {"markov_bucket": "green"}}})))
        db.commit()
        db.close()
        s = summarize(str(p), str(p), 1, 1, 10)
        assert s["coverage"]["source_seen_signals"] == 1
        assert s["coverage"]["markov_seen_signals"] == 1
        assert s["dimensions"]["markov_bucket"][0]["slice_value"] == "green"
        assert s["dimensions"]["lifecycle_profile"][0]["slice_value"] == "RECLAIM:PROBE"
    print("SELF_TEST_PASS offline_candidate_runtime_cross")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--candidate-db", default="data/paper_trades.db")
    ap.add_argument("--runtime-db", default=None)
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--min-closed", type=int, default=20)
    ap.add_argument("--limit", type=int, default=100)
    ap.add_argument("--out", default="data/offline_candidate_runtime_cross.json")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    if args.self_test:
        self_test()
        return
    runtime_db = args.runtime_db or args.candidate_db
    result = summarize(args.candidate_db, runtime_db, args.hours, args.min_closed, args.limit)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"out": args.out, "coverage": result["coverage"]}, sort_keys=True))


if __name__ == "__main__":
    main()
