#!/usr/bin/env python3
"""Read-only per-candidate downstream readiness audit.

This report overlays each shadow candidate that matched raw gold/silver events
with the runtime funnel stages that followed those same signal_ids. It never
changes strategy, gates, A_CLASS mode, final_entry_contract, paper/live
execution, or risk settings.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path


SCHEMA_VERSION = "candidate_downstream_readiness_audit.v1"
GS_TIERS = {"gold", "silver"}


def utc_now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def signal_id_key(value):
    if value is None or value == "":
        return None
    return str(value)


def safe_float(value, default=None):
    try:
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default
    except Exception:
        return default


def safe_int(value, default=0):
    parsed = safe_float(value)
    return default if parsed is None else int(parsed)


def truthy(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def rate(num, den):
    return None if not den else round(float(num) / float(den), 6)


def table_exists(db, table):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def columns(db, table):
    try:
        return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
    except sqlite3.Error:
        return set()


def chunks(values, size=500):
    values = list(values or [])
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def jloads(raw, default=None):
    default = {} if default is None else default
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, (dict, list)) else default
    except Exception:
        return default


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + f".{int(time.time() * 1000)}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)


def raw_tier(row):
    for key in ("raw_sustained_tier", "raw_primary_tier"):
        value = str(row.get(key) or "").lower()
        if value in GS_TIERS:
            return value
    return None


def load_raw_gold_silver(raw_db, since_ts):
    if not table_exists(raw_db, "raw_signal_outcomes"):
        return []
    cols = columns(raw_db, "raw_signal_outcomes")
    needed = (
        "signal_id",
        "token_ca",
        "symbol",
        "signal_ts",
        "signal_type",
        "raw_sustained_tier",
        "raw_primary_tier",
    )
    select = [name if name in cols else f"NULL AS {name}" for name in needed]
    tier_exprs = []
    if "raw_sustained_tier" in cols:
        tier_exprs.append("raw_sustained_tier IN ('gold', 'silver')")
    if "raw_primary_tier" in cols:
        tier_exprs.append("raw_primary_tier IN ('gold', 'silver')")
    if not tier_exprs:
        return []
    rows = raw_db.execute(
        f"""
        SELECT {", ".join(select)}
        FROM raw_signal_outcomes
        WHERE COALESCE(signal_ts, 0) >= ?
          AND ({' OR '.join(tier_exprs)})
        ORDER BY signal_ts ASC, signal_id ASC
        """,
        (since_ts,),
    ).fetchall()
    out = []
    seen = set()
    for row in rows:
        item = dict(row)
        key = signal_id_key(item.get("signal_id"))
        if not key or key in seen:
            continue
        seen.add(key)
        item["signal_id_key"] = key
        item["tier"] = raw_tier(item)
        out.append(item)
    return out


def load_candidate_observations(paper_db, raw_signal_ids, since_ts):
    if not raw_signal_ids or not table_exists(paper_db, "candidate_shadow_observations"):
        return []
    int_ids = []
    text_ids = []
    for value in raw_signal_ids:
        try:
            number = int(value)
            if str(number) == str(value):
                int_ids.append(number)
            else:
                text_ids.append(value)
        except Exception:
            text_ids.append(value)
    rows = []
    for chunk in chunks(int_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(
            paper_db.execute(
                f"""
                SELECT signal_id, candidate_id, family, matched
                FROM candidate_shadow_observations
                WHERE observed_at >= ? AND signal_id IN ({placeholders})
                """,
                [since_ts - 3600, *chunk],
            ).fetchall()
        )
    for chunk in chunks(text_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(
            paper_db.execute(
                f"""
                SELECT signal_id, candidate_id, family, matched
                FROM candidate_shadow_observations
                WHERE observed_at >= ? AND CAST(signal_id AS TEXT) IN ({placeholders})
                """,
                [since_ts - 3600, *chunk],
            ).fetchall()
        )
    out = []
    for row in rows:
        out.append(
            {
                "signal_id_key": signal_id_key(row["signal_id"]),
                "candidate_id": row["candidate_id"],
                "family": row["family"],
                "matched": truthy(row["matched"]),
            }
        )
    return out


def extract_hard_blockers(payload):
    blockers = payload.get("hard_blockers")
    if blockers is None and isinstance(payload.get("final_entry_contract"), dict):
        blockers = payload.get("final_entry_contract", {}).get("hard_blockers")
    if isinstance(blockers, str):
        decoded = jloads(blockers, None)
        blockers = decoded if isinstance(decoded, list) else [blockers]
    if isinstance(blockers, (list, tuple, set)):
        return [str(item) for item in blockers if str(item or "")]
    return []


def load_stage_sets(paper_db, raw_signal_ids, since_ts, until_ts):
    raw_set = set(raw_signal_ids or [])
    sets = {
        "decision": set(),
        "pass_allow": set(),
        "pending": set(),
        "final_entry": set(),
        "final_mode_disabled": set(),
        "final_mode_disabled_only": set(),
        "paper_intent": set(),
        "paper_committed": set(),
    }
    if not raw_set or not table_exists(paper_db, "paper_decision_events"):
        return sets
    rows = paper_db.execute(
        """
        SELECT event_ts, signal_id, component, event_type, decision, payload_json
        FROM paper_decision_events
        WHERE event_ts >= ? AND event_ts <= ? AND signal_id IS NOT NULL
        """,
        (since_ts - 60, until_ts + 900),
    ).fetchall()
    for row in rows:
        sig = signal_id_key(row["signal_id"])
        if sig not in raw_set:
            continue
        event_type = str(row["event_type"] or "").lower()
        decision = str(row["decision"] or "").upper()
        component = str(row["component"] or "")
        sets["decision"].add(sig)
        if decision in {"PASS", "ALLOW", "WOULD_ENTER", "ENTER"} or event_type in {"would_enter", "enter"}:
            sets["pass_allow"].add(sig)
        if event_type == "pending_entry":
            sets["pending"].add(sig)
        if event_type == "paper_trade_entry_intent":
            sets["paper_intent"].add(sig)
        if event_type in {"paper_trade_committed", "paper_trade_entry_committed"}:
            sets["paper_committed"].add(sig)
        if component == "final_entry_contract":
            sets["final_entry"].add(sig)
            blockers = extract_hard_blockers(jloads(row["payload_json"]))
            non_mode = [blocker for blocker in blockers if blocker != "mode_disabled"]
            if "mode_disabled" in blockers:
                sets["final_mode_disabled"].add(sig)
                if not non_mode:
                    sets["final_mode_disabled_only"].add(sig)
    return sets


def classify_candidate(row):
    recall = row["raw_gs_recall"]
    precision = row["match_precision"]
    pending = row["pending_rate_after_match"]
    final_rate = row["mode_disabled_adjusted_final_eligibility_rate_after_match"]
    matched = row["matched_raw_gs_signals"]
    if matched <= 0:
        return "NO_SIGNAL"
    if final_rate >= 0.6 and precision >= 0.05:
        return "EFFECTIVENESS_HIT_PENDING_OOS"
    if pending >= 0.6 and recall >= 0.2:
        return "PENDING_BRIDGE_WATCH"
    if row["decision_record_rate_after_match"] < 0.8:
        return "DECISION_BRIDGE_WATCH"
    if recall >= 0.35 and precision < 0.05:
        return "LOW_PRECISION_DETECTOR"
    if recall > 0 and precision >= 0.15 and matched >= 3:
        return "POTENTIAL_ENTRY_HYPOTHESIS"
    return "DETECTOR_ONLY"


def build_report(args):
    now_ts = int(args.now_ts or time.time())
    since_ts = now_ts - int(float(args.hours) * 3600)
    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    raw_db = sqlite3.connect(args.raw_db)
    raw_db.row_factory = sqlite3.Row
    try:
        raw_rows = load_raw_gold_silver(raw_db, since_ts)
        raw_signal_ids = [row["signal_id_key"] for row in raw_rows if row.get("signal_id_key")]
        observations = load_candidate_observations(paper_db, raw_signal_ids, since_ts)
        stage_sets = load_stage_sets(paper_db, raw_signal_ids, since_ts, now_ts)
    finally:
        paper_db.close()
        raw_db.close()
    raw_den = len(set(raw_signal_ids))
    by_candidate = defaultdict(lambda: {"signals": set(), "matched": set(), "family": None})
    for row in observations:
        cid = row["candidate_id"]
        by_candidate[cid]["family"] = row.get("family")
        if row.get("signal_id_key"):
            by_candidate[cid]["signals"].add(row["signal_id_key"])
            if row["matched"]:
                by_candidate[cid]["matched"].add(row["signal_id_key"])
    rows = []
    for candidate_id, bucket in by_candidate.items():
        matched = set(bucket["matched"])
        matched_n = len(matched)
        observed_n = len(bucket["signals"])
        item = {
            "candidate_id": candidate_id,
            "family": bucket.get("family"),
            "observed_raw_gs_signals": observed_n,
            "matched_raw_gs_signals": matched_n,
            "raw_gs_recall": rate(matched_n, raw_den),
            "match_precision": rate(matched_n, observed_n),
            "decision_record_count_after_match": len(matched & stage_sets["decision"]),
            "pass_allow_count_after_match": len(matched & stage_sets["pass_allow"]),
            "pending_count_after_match": len(matched & stage_sets["pending"]),
            "final_entry_contract_count_after_match": len(matched & stage_sets["final_entry"]),
            "mode_disabled_final_entry_count_after_match": len(matched & stage_sets["final_mode_disabled"]),
            "mode_disabled_only_final_entry_count_after_match": len(matched & stage_sets["final_mode_disabled_only"]),
            "paper_trade_intent_count_after_match": len(matched & stage_sets["paper_intent"]),
            "paper_trade_committed_count_after_match": len(matched & stage_sets["paper_committed"]),
        }
        item.update(
            {
                "decision_record_rate_after_match": rate(item["decision_record_count_after_match"], matched_n),
                "pass_allow_rate_after_match": rate(item["pass_allow_count_after_match"], matched_n),
                "pending_rate_after_match": rate(item["pending_count_after_match"], matched_n),
                "final_entry_contract_rate_after_match": rate(item["final_entry_contract_count_after_match"], matched_n),
                "mode_disabled_adjusted_final_eligibility_rate_after_match": rate(
                    item["mode_disabled_only_final_entry_count_after_match"],
                    matched_n,
                ),
                "paper_trade_intent_rate_after_match": rate(item["paper_trade_intent_count_after_match"], matched_n),
                "paper_trade_committed_rate_after_match": rate(item["paper_trade_committed_count_after_match"], matched_n),
            }
        )
        item["classification"] = classify_candidate(item)
        rows.append(item)
    rows.sort(
        key=lambda row: (
            row.get("mode_disabled_adjusted_final_eligibility_rate_after_match") or 0,
            row.get("pending_rate_after_match") or 0,
            row.get("pass_allow_rate_after_match") or 0,
            row.get("raw_gs_recall") or 0,
            row.get("match_precision") or 0,
        ),
        reverse=True,
    )
    counts = Counter(row["classification"] for row in rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "candidate_downstream_readiness_audit_24h",
        "generated_at": utc_now(),
        "window": {"hours": args.hours, "since_ts": since_ts, "until_ts": now_ts},
        "promotion_allowed": False,
        "strategy_change_allowed": False,
        "paper_enablement_allowed": False,
        "automatic_runtime_change_allowed": False,
        "stage_attribution_scope": "signal_id_level_not_candidate_specific",
        "raw_gold_silver_signal_denominator": raw_den,
        "candidate_count_observed": len(by_candidate),
        "candidate_count_expected": args.expected_candidates,
        "stage_counts": {key: len(value) for key, value in stage_sets.items()},
        "classification_counts": dict(counts),
        "top_candidates": rows[: args.limit],
        "all_candidates": rows,
        "notes": [
            "Read-only downstream overlay for shadow candidates that matched raw gold/silver signal_ids.",
            "Rates are conditional on candidate-matched raw gold/silver signal_ids and do not imply promotion.",
            "Downstream stages are attributed at signal_id level because current decision events are not candidate-specific.",
        ],
    }


def self_test():
    now = 2_000_000
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        paper = root / "paper.db"
        raw = root / "raw.db"
        pdb = sqlite3.connect(paper)
        pdb.execute(
            """
            CREATE TABLE candidate_shadow_observations(
              signal_id TEXT, candidate_id TEXT, family TEXT, matched INTEGER, observed_at REAL
            )
            """
        )
        for sig in ("1", "2", "3"):
            pdb.execute("INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?)", (sig, "current_all", "base", 1, now - 10))
            pdb.execute("INSERT INTO candidate_shadow_observations VALUES (?,?,?,?,?)", (sig, "candidate_a", "base", 1 if sig != "3" else 0, now - 10))
        pdb.execute(
            """
            CREATE TABLE paper_decision_events(
              event_ts REAL, signal_id TEXT, component TEXT, event_type TEXT, decision TEXT, payload_json TEXT
            )
            """
        )
        pdb.execute("INSERT INTO paper_decision_events VALUES (?,?,?,?,?,?)", (now - 5, "1", "x", "would_enter", "PASS", "{}"))
        pdb.execute("INSERT INTO paper_decision_events VALUES (?,?,?,?,?,?)", (now - 4, "1", "x", "pending_entry", "PENDING", "{}"))
        pdb.execute("INSERT INTO paper_decision_events VALUES (?,?,?,?,?,?)", (now - 3, "1", "final_entry_contract", "entry_block", "BLOCK", json.dumps({"hard_blockers": ["mode_disabled"]})))
        pdb.execute("INSERT INTO paper_decision_events VALUES (?,?,?,?,?,?)", (now - 5, "2", "x", "would_enter", "PASS", "{}"))
        pdb.commit()
        pdb.close()
        rdb = sqlite3.connect(raw)
        rdb.execute(
            """
            CREATE TABLE raw_signal_outcomes(
              signal_id TEXT, token_ca TEXT, symbol TEXT, signal_ts REAL, signal_type TEXT,
              raw_sustained_tier TEXT, raw_primary_tier TEXT
            )
            """
        )
        for sig in ("1", "2", "3"):
            rdb.execute("INSERT INTO raw_signal_outcomes VALUES (?,?,?,?,?,?,?)", (sig, f"T{sig}", f"T{sig}", now - 100, "NEW_TRENDING", "gold", None))
        rdb.commit()
        rdb.close()
        args = argparse.Namespace(
            db=str(paper),
            raw_db=str(raw),
            hours=24,
            now_ts=now,
            expected_candidates=2,
            limit=50,
            out=None,
        )
        report = build_report(args)
        assert report["promotion_allowed"] is False
        assert report["raw_gold_silver_signal_denominator"] == 3
        assert report["candidate_count_observed"] == 2
        by_id = {row["candidate_id"]: row for row in report["all_candidates"]}
        assert by_id["candidate_a"]["matched_raw_gs_signals"] == 2
        assert by_id["candidate_a"]["decision_record_rate_after_match"] == 1.0
        assert by_id["candidate_a"]["pending_rate_after_match"] == 0.5
        assert by_id["candidate_a"]["mode_disabled_adjusted_final_eligibility_rate_after_match"] == 0.5
    print("SELF_TEST_PASS candidate_downstream_readiness_audit")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="/app/data/paper_trades.db")
    parser.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--now-ts", type=int, default=None)
    parser.add_argument("--expected-candidates", type=int, default=84)
    parser.add_argument("--limit", type=int, default=84)
    parser.add_argument("--out")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    report = build_report(args)
    if args.out:
        write_json(args.out, report)
    print(json.dumps({
        "candidate_count_observed": report["candidate_count_observed"],
        "raw_gold_silver_signal_denominator": report["raw_gold_silver_signal_denominator"],
        "classification_counts": report["classification_counts"],
        "promotion_allowed": False,
    }, sort_keys=True))


if __name__ == "__main__":
    main()
