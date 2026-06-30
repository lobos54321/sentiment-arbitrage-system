#!/usr/bin/env python3
"""Event-level raw gold/silver entry funnel audit.

Read-only. This report starts from raw_signal_outcomes gold/silver rows and
only joins the corresponding candidate observations and decision records. It is
intended to answer why observed raw dogs did not enter the trading loop without
full-scanning the candidate shadow mesh.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import math
import sqlite3
import time
from collections import Counter, defaultdict
from pathlib import Path


SCHEMA_VERSION = "offline_raw_gold_silver_funnel_audit.v4"
EVIDENCE_LEVEL = "discovery_same_window"
DEFAULT_EXPECTED_CANDIDATES = 84


def jloads(raw):
    try:
        value = json.loads(raw or "{}")
        return value if isinstance(value, dict) else {}
    except Exception:
        return {}


def truthy(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "enter", "would_enter"}


def safe_float(value):
    try:
        number = float(value)
        return number if math.isfinite(number) else None
    except Exception:
        return None


def safe_int(value):
    number = safe_float(value)
    return int(number) if number is not None else None


def signal_id_key(value):
    if value is None or value == "":
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        number = float(text)
        if math.isfinite(number) and number.is_integer():
            return str(int(number))
    except Exception:
        pass
    return text


def rate(num, den):
    if not den:
        return None
    return round(float(num) / float(den), 6)


def pct(num, den):
    if not den:
        return None
    return round(float(num) / float(den) * 100.0, 4)


def table_exists(db, table):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def columns(db, table):
    if not table_exists(db, table):
        return set()
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def optional(cols, name, fallback="NULL"):
    return name if name in cols else f"{fallback} AS {name}"


def expr(cols, name, fallback="NULL"):
    return name if name in cols else fallback


def chunks(items, size=400):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def normalize_ts(value):
    ts = safe_float(value)
    if ts is None:
        return None
    return ts / 1000.0 if ts > 1_000_000_000_000 else ts


def raw_tier(row):
    return str(row.get("raw_sustained_tier") or row.get("raw_primary_tier") or "").lower()


def is_gold_silver(row):
    return raw_tier(row) in {"gold", "silver"}


def compact_grouped_counts(rows):
    return [
        {
            "component": row["component"],
            "event_type": row["event_type"],
            "decision": row["decision"],
            "reason": row["reason"],
            "count": row["n"],
        }
        for row in rows
    ]


def raw_eligibility(row):
    failures = []
    if row.get("observation_status") != "matured":
        failures.append("not_matured")
    if not truthy(row.get("kline_covered")):
        failures.append("kline_uncovered")
    if row.get("baseline_confidence") not in {"high", "medium"}:
        failures.append("low_confidence")
    if not truthy(row.get("same_source_path")):
        failures.append("not_same_source_path")
    if truthy(row.get("outlier_flag")):
        failures.append("outlier")
    if not truthy(row.get("sustained_evaluable")):
        failures.append("not_sustained_evaluable")
    return failures


def load_raw_dogs(raw_db, since_ts):
    if not table_exists(raw_db, "raw_signal_outcomes"):
        raise SystemExit("raw_signal_outcomes table missing")
    cols = columns(raw_db, "raw_signal_outcomes")
    needed = (
        "signal_id",
        "token_ca",
        "symbol",
        "signal_ts",
        "signal_type",
        "source",
        "observation_status",
        "kline_covered",
        "coverage_reason",
        "baseline_confidence",
        "same_source_path",
        "outlier_flag",
        "outlier_reason",
        "sustained_evaluable",
        "sustained_reason",
        "raw_sustained_tier",
        "raw_primary_tier",
        "max_sustained_peak_pct",
        "max_wick_peak_pct",
        "time_to_sustained_peak_sec",
        "did_enter",
        "entered_before_peak",
        "held_to_silver",
        "held_to_gold",
        "raw_dog_entered",
        "raw_dog_realized",
        "sold_before_silver",
        "sold_before_gold",
        "exit_reason",
        "payload_json",
        "source_kind",
        "source_family",
    )
    select = [name if name in cols else f"NULL AS {name}" for name in needed]
    tier_exprs = []
    if "raw_sustained_tier" in cols:
        tier_exprs.append("raw_sustained_tier IN ('gold', 'silver')")
    if "raw_primary_tier" in cols:
        tier_exprs.append("raw_primary_tier IN ('gold', 'silver')")
    if not tier_exprs:
        raise SystemExit("raw_signal_outcomes has no raw tier columns")
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
    for row in rows:
        item = dict(row)
        item["signal_id_key"] = signal_id_key(item.get("signal_id"))
        item["signal_ts_norm"] = normalize_ts(item.get("signal_ts"))
        item["tier"] = raw_tier(item)
        item["evaluable_failures"] = raw_eligibility(item)
        item["evaluable"] = len(item["evaluable_failures"]) == 0
        item["raw_dog_entered_bool"] = truthy(item.get("raw_dog_entered") or item.get("did_enter"))
        item["raw_dog_realized_bool"] = truthy(
            item.get("raw_dog_realized") or item.get("held_to_silver") or item.get("held_to_gold")
        )
        out.append(item)
    return out


def make_raw_indexes(raw_rows):
    signal_ids = sorted({row["signal_id_key"] for row in raw_rows if row.get("signal_id_key")})
    tokens = sorted({str(row["token_ca"]) for row in raw_rows if row.get("token_ca")})
    by_signal = defaultdict(list)
    by_token = defaultdict(list)
    for idx, row in enumerate(raw_rows):
        row["_event_idx"] = idx
        if row.get("signal_id_key"):
            by_signal[row["signal_id_key"]].append(idx)
        if row.get("token_ca"):
            by_token[str(row["token_ca"])].append(idx)
    return signal_ids, tokens, by_signal, by_token


def load_candidate_observations(paper_db, raw_signal_ids, since_ts):
    if not raw_signal_ids or not table_exists(paper_db, "candidate_shadow_observations"):
        return [], {"available": False, "reason": "missing_signal_ids_or_table"}
    rows = []
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
    for chunk in chunks(int_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(
            paper_db.execute(
                f"""
                SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched, reason,
                       observed_at,
                       CASE WHEN candidate_id = 'current_all' THEN payload_json ELSE NULL END AS payload_json
                FROM candidate_shadow_observations
                WHERE observed_at >= ?
                  AND signal_id IN ({placeholders})
                """,
                [since_ts - 3600, *chunk],
            ).fetchall()
        )
    for chunk in chunks(text_ids):
        placeholders = ",".join("?" for _ in chunk)
        rows.extend(
            paper_db.execute(
                f"""
                SELECT signal_id, token_ca, signal_ts, candidate_id, family, matched, reason,
                       observed_at,
                       CASE WHEN candidate_id = 'current_all' THEN payload_json ELSE NULL END AS payload_json
                FROM candidate_shadow_observations
                WHERE observed_at >= ?
                  AND CAST(signal_id AS TEXT) IN ({placeholders})
                """,
                [since_ts - 3600, *chunk],
            ).fetchall()
        )
    out = []
    for row in rows:
        # Only the per-signal baseline row is needed for context fields. Parsing
        # every candidate payload for raw_dogs x 84 rows is memory-heavy in the
        # production container.
        payload = jloads(row["payload_json"]) if row["candidate_id"] == "current_all" else {}
        out.append(
            {
                "signal_id_key": signal_id_key(row["signal_id"]),
                "token_ca": row["token_ca"],
                "signal_ts": safe_int(row["signal_ts"]),
                "candidate_id": row["candidate_id"],
                "family": row["family"],
                "matched": truthy(row["matched"]),
                "reason": row["reason"],
                "observed_at": safe_int(row["observed_at"]),
                "payload": payload,
            }
        )
    return out, {"available": True, "loaded_rows": len(out)}


def load_paper_decisions(paper_db, tokens, since_ts, until_ts):
    if not tokens:
        return []
    token_set = set(tokens)
    out = []
    if table_exists(paper_db, "paper_decision_events"):
        cols = columns(paper_db, "paper_decision_events")
        rows = paper_db.execute(
            """
            SELECT id, 'paper_decision_events' AS source_kind, event_ts, signal_id, token_ca,
                   symbol, lifecycle_id, component AS source_component, reason,
                   event_type, decision, route, data_source, lifecycle_state,
                   NULL AS payload_json, NULL AS action, NULL AS would_action,
                   NULL AS would_enter_a_class, NULL AS did_enter,
                   NULL AS quote_clean, NULL AS quote_executable, NULL AS route_available,
                   NULL AS block_cause, NULL AS hard_blockers_json,
                   NULL AS quote_failure_reason, NULL AS route_failure_reason
            FROM paper_decision_events
            WHERE event_ts >= ? AND event_ts <= ?
            """,
            [since_ts - 60, until_ts + 900],
        ).fetchall()
        out.extend(dict(row) for row in rows if str(row["token_ca"] or "") in token_set)
    if table_exists(paper_db, "a_class_decision_events"):
        cols = columns(paper_db, "a_class_decision_events")
        rows = paper_db.execute(
            f"""
            SELECT id, 'a_class_decision_events' AS source_kind, event_ts, NULL AS signal_id,
                   token_ca, symbol, {optional(cols, 'lifecycle_id')},
                   {optional(cols, 'source_component')}, {optional(cols, 'reason')},
                   NULL AS event_type, NULL AS decision, {expr(cols, 'route_bucket', 'NULL')} AS route,
                   {expr(cols, 'source_table', 'NULL')} AS data_source,
                   NULL AS lifecycle_state,
                   NULL AS payload_json,
                   {optional(cols, 'action')}, {optional(cols, 'would_action')},
                   NULL AS would_enter_a_class, NULL AS did_enter,
                   {optional(cols, 'quote_clean')}, {optional(cols, 'quote_executable')},
                   {optional(cols, 'route_available')}, {optional(cols, 'block_cause')},
                   {optional(cols, 'hard_blockers_json', "'[]'")},
                   {optional(cols, 'quote_failure_reason')}, {optional(cols, 'route_failure_reason')}
            FROM a_class_decision_events
            WHERE event_ts >= ? AND event_ts <= ?
            """,
            [since_ts - 60, until_ts + 900],
        ).fetchall()
        out.extend(dict(row) for row in rows if str(row["token_ca"] or "") in token_set)
    if table_exists(paper_db, "opportunity_events"):
        cols = columns(paper_db, "opportunity_events")
        rows = paper_db.execute(
            f"""
            SELECT id, 'opportunity_events' AS source_kind, event_ts, NULL AS signal_id,
                   token_ca, symbol, {optional(cols, 'lifecycle_id')},
                   {optional(cols, 'source_component')},
                   {expr(cols, 'quote_failure_reason', 'NULL')} AS reason,
                   NULL AS event_type, NULL AS decision,
                   {expr(cols, 'route_bucket', 'NULL')} AS route,
                   {expr(cols, 'source_type', 'NULL')} AS data_source,
                   NULL AS lifecycle_state,
                   NULL AS payload_json,
                   CASE WHEN COALESCE({expr(cols, 'did_enter', '0')}, 0) = 1 THEN 'ENTER'
                        WHEN COALESCE({expr(cols, 'would_enter_a_class', '0')}, 0) = 1 THEN 'WOULD_ENTER'
                        ELSE 'BLOCK' END AS action,
                   NULL AS would_action,
                   {optional(cols, 'would_enter_a_class', '0')},
                   {optional(cols, 'did_enter', '0')},
                   {optional(cols, 'quote_clean')}, {optional(cols, 'quote_executable')},
                   {optional(cols, 'route_available')}, {optional(cols, 'block_cause')},
                   {optional(cols, 'hard_blockers_json', "'[]'")},
                   {optional(cols, 'quote_failure_reason')}, {optional(cols, 'route_failure_reason')}
            FROM opportunity_events
            WHERE event_ts >= ? AND event_ts <= ?
            """,
            [since_ts - 60, until_ts + 900],
        ).fetchall()
        out.extend(dict(row) for row in rows if str(row["token_ca"] or "") in token_set)
    normalized = []
    for row in out:
        row = dict(row)
        row["signal_id_key"] = signal_id_key(row.get("signal_id"))
        row["event_ts_norm"] = normalize_ts(row.get("event_ts"))
        normalized.append(row)
    normalized.sort(key=lambda r: (r.get("event_ts_norm") or 0, r.get("source_kind") or "", r.get("id") or 0))
    return normalized


def load_paper_trades(paper_db, tokens, since_ts, until_ts):
    if not tokens or not table_exists(paper_db, "paper_trades"):
        return []
    out = []
    for chunk in chunks(tokens):
        placeholders = ",".join("?" for _ in chunk)
        rows = paper_db.execute(
            f"""
            SELECT id, token_ca, symbol, premium_signal_id, signal_ts, entry_ts, exit_ts,
                   exit_reason, pnl_pct, lifecycle_id, entry_mode, entry_branch,
                   execution_availability, accounting_outcome, paper_only
            FROM paper_trades
            WHERE entry_ts >= ? AND entry_ts <= ?
              AND token_ca IN ({placeholders})
            """,
            [since_ts - 60, until_ts + 900, *chunk],
        ).fetchall()
        out.extend(dict(row) for row in rows)
    for row in out:
        row["signal_id_key"] = signal_id_key(row.get("premium_signal_id"))
        row["entry_ts_norm"] = normalize_ts(row.get("entry_ts"))
    return out


ENTRY_BRIDGE_COMPONENTS = (
    "a_class_live_enqueue",
    "entry_execution_eligibility",
    "v27_runtime_mode_gate",
    "entry_decision_contract",
    "final_entry_contract",
    "execution_api",
    "paper_fast_lane",
)

ENTRY_TERMINAL_EVENT_TYPES = (
    "paper_trade_entry_intent",
    "paper_trade_entry_committed",
    "entry_quote",
    "filled_paper",
)


def _extract_hard_blockers(payload):
    blockers = payload.get("hard_blockers")
    if blockers is None and isinstance(payload.get("final_entry_contract"), dict):
        blockers = payload.get("final_entry_contract", {}).get("hard_blockers")
    if isinstance(blockers, str):
        try:
            decoded = json.loads(blockers)
            blockers = decoded
        except Exception:
            blockers = [blockers]
    if isinstance(blockers, (list, tuple, set)):
        return [str(item) for item in blockers if item is not None and str(item)]
    return []


def _mode_state(payload):
    state = payload.get("mode_state")
    if state is None and isinstance(payload.get("final_entry_contract"), dict):
        state = payload.get("final_entry_contract", {}).get("mode_state")
    return state if isinstance(state, dict) else {}


def _payload_counter_value(value):
    if value is None or value == "":
        return "UNKNOWN"
    return str(value)


def _decision_reason_key(row):
    if row is None:
        return ("UNKNOWN", "missing_decision_event", "UNKNOWN", "missing_decision_event")
    return (
        str(row["component"] or "UNKNOWN"),
        str(row["event_type"] or "UNKNOWN"),
        str(row["decision"] or "UNKNOWN"),
        str(row["reason"] or "UNKNOWN"),
    )


def _reason_rows(counter, limit=20):
    return [
        {
            "component": key[0],
            "event_type": key[1],
            "decision": key[2],
            "reason": key[3],
            "count": count,
        }
        for key, count in counter.most_common(limit)
    ]


NO_DECISION_ROOT_CAUSE_DESCRIPTIONS = {
    "token_time_decision_without_exact_signal_id": (
        "Decision-like events exist for the same token/time window, but not under the raw signal_id."
    ),
    "candidate_shadow_observed_no_decision_event": (
        "Candidate shadow observations exist with full candidate coverage, but no decision event was written."
    ),
    "partial_candidate_observation_no_decision_event": (
        "Candidate shadow observations exist but coverage is incomplete, and no decision event was written."
    ),
    "raw_event_missing_signal_id": "The raw dog row has no usable signal_id.",
    "no_candidate_observation_or_decision_event": (
        "No candidate observation and no decision-like event were found for the raw signal."
    ),
}


def _root_cause_rows(counter, descriptions, limit=20):
    return [
        {
            "root_cause": key,
            "description": descriptions.get(key, "Unclassified no-decision root cause."),
            "count": count,
        }
        for key, count in counter.most_common(limit)
    ]


def _choose_decision_no_pass_row(signal_rows):
    sorted_rows = sorted(signal_rows or [], key=lambda row: safe_float(row["event_ts"]) or 0)
    terminal_rows = [
        row
        for row in sorted_rows
        if str(row["event_type"] or "").lower()
        in {"entry_block", "probe_reject", "quality_gate", "timing_decision", "entry_abort"}
        or str(row["decision"] or "").upper() in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
    ]
    return terminal_rows[-1] if terminal_rows else (sorted_rows[-1] if sorted_rows else None)


def _choose_pass_without_pending_row(signal_rows):
    sorted_rows = sorted(signal_rows or [], key=lambda row: safe_float(row["event_ts"]) or 0)
    pass_ts_values = [
        safe_float(row["event_ts"])
        for row in sorted_rows
        if str(row["decision"] or "").upper() in {"PASS", "ALLOW", "WOULD_ENTER", "ENTER"}
        or str(row["event_type"] or "").lower() in {"would_enter", "enter"}
    ]
    first_pass_ts = min([ts for ts in pass_ts_values if ts is not None], default=None)
    after_pass = [
        row
        for row in sorted_rows
        if first_pass_ts is None or (safe_float(row["event_ts"]) or 0) >= first_pass_ts
    ]
    terminal_rows = [
        row
        for row in after_pass
        if str(row["event_type"] or "").lower() in {"entry_block", "entry_abort", "pending_reject"}
        or str(row["decision"] or "").upper() in {"BLOCK", "REJECT", "WATCH_ONLY", "WAIT", "SKIP"}
    ]
    return (
        terminal_rows[0] if terminal_rows else (after_pass[-1] if after_pass else None),
        first_pass_ts,
    )


def _raw_signal_decision_window(raw_row):
    if not raw_row:
        return None, None
    signal_ts = raw_row.get("signal_ts_norm")
    if signal_ts is None:
        signal_ts = normalize_ts(raw_row.get("signal_ts"))
    if signal_ts is None:
        return None, None
    peak_sec = safe_float(raw_row.get("time_to_sustained_peak_sec"))
    end_offset = max(60.0, min(900.0, peak_sec or 900.0))
    return signal_ts - 60, signal_ts + end_offset


def _decision_summary(row):
    if not row:
        return None
    get = row.get if isinstance(row, dict) else lambda key, default=None: row[key] if key in row.keys() else default
    return {
        "source_kind": get("source_kind"),
        "event_ts": get("event_ts"),
        "signal_id": signal_id_key(get("signal_id")),
        "token_ca": get("token_ca"),
        "source_component": get("source_component") or get("component"),
        "event_type": get("event_type"),
        "decision": get("decision") or get("action"),
        "reason": get("reason") or get("block_cause"),
        "quote_clean": get("quote_clean"),
        "quote_executable": get("quote_executable"),
        "route_available": get("route_available"),
    }


def _attribute_no_decision_records(
    missing_signal_ids,
    raw_by_signal=None,
    observations_by_signal=None,
    decisions_by_token=None,
    expected_candidates=DEFAULT_EXPECTED_CANDIDATES,
):
    raw_by_signal = raw_by_signal or {}
    observations_by_signal = observations_by_signal or {}
    decisions_by_token = decisions_by_token or {}
    root_counts = Counter()
    examples = []
    token_time_joined = 0
    candidate_shadow_observed = 0
    partial_candidate_observed = 0
    no_observation_or_decision = 0
    missing_signal_id = 0

    for signal_id in sorted(missing_signal_ids or []):
        raw_row = raw_by_signal.get(signal_id) or {}
        token = str(raw_row.get("token_ca") or "")
        observations = observations_by_signal.get(signal_id, [])
        unique_candidates = {row.get("candidate_id") for row in observations if row.get("candidate_id")}
        matched_count = sum(1 for row in observations if truthy(row.get("matched")))
        start, end = _raw_signal_decision_window(raw_row)
        token_time_decisions = []
        if token and start is not None and end is not None:
            for decision in decisions_by_token.get(token, []):
                ts = decision.get("event_ts_norm") if isinstance(decision, dict) else None
                if ts is None:
                    ts = normalize_ts(decision.get("event_ts") if isinstance(decision, dict) else None)
                if ts is None or ts < start or ts > end:
                    continue
                token_time_decisions.append(decision)
        token_time_decisions.sort(key=lambda row: row.get("event_ts_norm") or normalize_ts(row.get("event_ts")) or 0)
        if token_time_decisions:
            root = "token_time_decision_without_exact_signal_id"
            token_time_joined += 1
        elif not signal_id:
            root = "raw_event_missing_signal_id"
            missing_signal_id += 1
        elif len(unique_candidates) >= int(expected_candidates or 0):
            root = "candidate_shadow_observed_no_decision_event"
            candidate_shadow_observed += 1
        elif unique_candidates:
            root = "partial_candidate_observation_no_decision_event"
            partial_candidate_observed += 1
        else:
            root = "no_candidate_observation_or_decision_event"
            no_observation_or_decision += 1
        root_counts[root] += 1
        if len(examples) < 20:
            examples.append(
                {
                    "signal_id": signal_id,
                    "token_ca": token or raw_row.get("token_ca"),
                    "root_cause": root,
                    "description": NO_DECISION_ROOT_CAUSE_DESCRIPTIONS.get(root),
                    "candidate_observation_count": len(observations),
                    "unique_candidate_count": len(unique_candidates),
                    "matched_candidate_count": matched_count,
                    "token_time_decision_count": len(token_time_decisions),
                    "token_time_decision_sample": [
                        _decision_summary(row) for row in token_time_decisions[:3]
                    ],
                }
            )

    return {
        "no_decision_record_root_cause_counts": _root_cause_rows(
            root_counts, NO_DECISION_ROOT_CAUSE_DESCRIPTIONS
        ),
        "no_decision_record_examples": examples,
        "no_decision_token_time_decision_without_exact_signal_id": token_time_joined,
        "no_decision_candidate_shadow_observed_no_decision_event": candidate_shadow_observed,
        "no_decision_partial_candidate_observation_no_decision_event": partial_candidate_observed,
        "no_decision_no_candidate_observation_or_decision_event": no_observation_or_decision,
        "no_decision_raw_event_missing_signal_id": missing_signal_id,
    }


def load_paper_evidence_event_counts(db_path, since_ts, until_ts):
    """Summarize JSONL evidence events without loading payloads into memory."""
    log_dir = Path(db_path).parent / "paper_evidence_log"
    result = {
        "available": log_dir.exists(),
        "log_dir": str(log_dir),
        "files_checked": 0,
        "events_in_window": 0,
        "parse_errors": 0,
        "event_type_counts": {},
        "source_event_type_counts": [],
    }
    if not log_dir.exists():
        return result

    source_event_counts = Counter()
    event_counts = Counter()
    start_day = _dt.datetime.fromtimestamp(float(since_ts), tz=_dt.timezone.utc).date()
    end_day = _dt.datetime.fromtimestamp(float(until_ts), tz=_dt.timezone.utc).date()
    day = start_day
    while day <= end_day:
        path = log_dir / f"paper-events-{day.strftime('%Y%m%d')}.jsonl"
        if path.exists():
            result["files_checked"] += 1
            try:
                with path.open("r", encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            record = json.loads(line)
                        except Exception:
                            result["parse_errors"] += 1
                            continue
                        ts = safe_float(record.get("event_ts"))
                        if ts is None or ts < since_ts or ts > until_ts:
                            continue
                        event_type = str(record.get("event_type") or "unknown")
                        source = str(record.get("source") or "unknown")
                        event_counts[event_type] += 1
                        source_event_counts[(source, event_type)] += 1
                        result["events_in_window"] += 1
            except Exception:
                result["parse_errors"] += 1
        day = day + _dt.timedelta(days=1)
    result["event_type_counts"] = dict(event_counts)
    result["source_event_type_counts"] = [
        {"source": key[0], "event_type": key[1], "count": count}
        for key, count in source_event_counts.most_common(30)
    ]
    return result


def load_raw_signal_decision_bridge(
    paper_db,
    raw_signal_ids,
    since_ts,
    until_ts,
    *,
    raw_rows=None,
    observations=None,
    token_time_decisions=None,
    expected_candidates=DEFAULT_EXPECTED_CANDIDATES,
):
    result = {
        "raw_signal_ids": len(raw_signal_ids or []),
        "decision_records_by_signal_id": 0,
        "raw_signals_with_decision_record": 0,
        "raw_signals_without_decision_record": len(raw_signal_ids or []),
        "no_decision_record_root_cause_counts": [],
        "no_decision_record_examples": [],
        "no_decision_token_time_decision_without_exact_signal_id": 0,
        "no_decision_candidate_shadow_observed_no_decision_event": 0,
        "no_decision_partial_candidate_observation_no_decision_event": 0,
        "no_decision_no_candidate_observation_or_decision_event": 0,
        "no_decision_raw_event_missing_signal_id": 0,
        "raw_signals_with_pass_or_allow": 0,
        "raw_signals_with_pending_entry": 0,
        "raw_signals_with_final_entry_contract": 0,
        "raw_signals_with_final_entry_block": 0,
        "raw_signals_with_final_entry_mode_disabled": 0,
        "raw_signals_with_final_entry_mode_disabled_only": 0,
        "raw_signals_with_final_entry_mode_disabled_plus_other": 0,
        "raw_signals_with_decision_no_pass_or_allow": 0,
        "decision_no_pass_or_allow_reason_counts": [],
        "decision_no_pass_or_allow_examples": [],
        "raw_signals_pass_or_allow_without_pending_entry": 0,
        "pass_or_allow_without_pending_entry_reason_counts": [],
        "pass_or_allow_without_pending_entry_examples": [],
        "raw_signals_pending_without_final_entry_contract": 0,
        "pending_without_final_entry_reason_counts": [],
        "pending_without_final_entry_examples": [],
        "raw_scoped_final_entry_hard_blockers": {},
        "component_decision_reason_counts": [],
    }
    if not raw_signal_ids or not table_exists(paper_db, "paper_decision_events"):
        return result

    raw_id_set = set(raw_signal_ids or [])
    raw_by_signal = {
        row.get("signal_id_key"): row
        for row in (raw_rows or [])
        if row.get("signal_id_key")
    }
    observations_by_signal = defaultdict(list)
    for row in observations or []:
        if row.get("signal_id_key"):
            observations_by_signal[row["signal_id_key"]].append(row)
    decisions_by_token = defaultdict(list)
    for row in token_time_decisions or []:
        if row.get("token_ca"):
            decisions_by_token[str(row["token_ca"])].append(row)
    window_rows = paper_db.execute(
        """
        SELECT event_ts, signal_id, component, event_type, decision, reason, payload_json
        FROM paper_decision_events
        WHERE event_ts >= ? AND event_ts <= ?
          AND signal_id IS NOT NULL
        """,
        [since_ts - 60, until_ts + 900],
    ).fetchall()
    rows = [row for row in window_rows if signal_id_key(row["signal_id"]) in raw_id_set]

    by_signal = defaultdict(list)
    grouped = Counter()
    for row in rows:
        key = signal_id_key(row["signal_id"])
        if key is None:
            continue
        by_signal[key].append(row)
        grouped[(row["component"], row["event_type"], row["decision"], row["reason"])] += 1

    with_decision = set(by_signal)
    pass_allow = set()
    pending = set()
    final_contract = set()
    final_block = set()
    final_mode_disabled = set()
    final_mode_disabled_only = set()
    final_mode_disabled_plus_other = set()
    raw_scoped_final_blockers = Counter()
    for signal_id, signal_rows in by_signal.items():
        for row in signal_rows:
            event_type = str(row["event_type"] or "").lower()
            decision = str(row["decision"] or "").upper()
            component = str(row["component"] or "")
            if decision in {"PASS", "ALLOW", "WOULD_ENTER", "ENTER"} or event_type in {"would_enter", "enter"}:
                pass_allow.add(signal_id)
            if event_type == "pending_entry":
                pending.add(signal_id)
            if component == "final_entry_contract":
                final_contract.add(signal_id)
                payload = jloads(row["payload_json"])
                blockers = _extract_hard_blockers(payload)
                non_mode_blockers = [blocker for blocker in blockers if blocker != "mode_disabled"]
                for blocker in blockers:
                    raw_scoped_final_blockers[blocker] += 1
                if "mode_disabled" in blockers:
                    final_mode_disabled.add(signal_id)
                    if non_mode_blockers:
                        final_mode_disabled_plus_other.add(signal_id)
                    else:
                        final_mode_disabled_only.add(signal_id)
                if event_type == "entry_block" or decision == "BLOCK":
                    final_block.add(signal_id)

    decision_no_pass = (with_decision - pass_allow) & raw_id_set
    decision_no_pass_reasons = Counter()
    decision_no_pass_examples = []
    for signal_id in sorted(decision_no_pass):
        chosen = _choose_decision_no_pass_row(by_signal.get(signal_id, []))
        reason_key = _decision_reason_key(chosen)
        payload = jloads(chosen["payload_json"]) if chosen is not None else {}
        decision_no_pass_reasons[reason_key] += 1
        if len(decision_no_pass_examples) < 20:
            decision_no_pass_examples.append(
                {
                    "signal_id": signal_id,
                    "attribution": {
                        "component": reason_key[0],
                        "event_type": reason_key[1],
                        "decision": reason_key[2],
                        "reason": reason_key[3],
                        "hard_blockers": _extract_hard_blockers(payload),
                    },
                }
            )

    pass_without_pending = (pass_allow - pending) & raw_id_set
    pass_without_pending_reasons = Counter()
    pass_without_pending_examples = []
    for signal_id in sorted(pass_without_pending):
        chosen, first_pass_ts = _choose_pass_without_pending_row(by_signal.get(signal_id, []))
        reason_key = _decision_reason_key(chosen)
        payload = jloads(chosen["payload_json"]) if chosen is not None else {}
        pass_without_pending_reasons[reason_key] += 1
        if len(pass_without_pending_examples) < 20:
            pass_without_pending_examples.append(
                {
                    "signal_id": signal_id,
                    "first_pass_ts": first_pass_ts,
                    "attribution": {
                        "component": reason_key[0],
                        "event_type": reason_key[1],
                        "decision": reason_key[2],
                        "reason": reason_key[3],
                        "hard_blockers": _extract_hard_blockers(payload),
                    },
                }
            )

    missing_decision_ids = raw_id_set - with_decision
    no_decision_attribution = _attribute_no_decision_records(
        missing_decision_ids,
        raw_by_signal=raw_by_signal,
        observations_by_signal=observations_by_signal,
        decisions_by_token=decisions_by_token,
        expected_candidates=expected_candidates,
    )

    pending_without_final = (pending - final_contract) & raw_id_set
    pending_without_final_reasons = Counter()
    pending_without_final_examples = []
    for signal_id in sorted(pending_without_final):
        signal_rows = sorted(
            by_signal.get(signal_id, []),
            key=lambda row: safe_float(row["event_ts"]) or 0,
        )
        pending_ts_values = [
            safe_float(row["event_ts"])
            for row in signal_rows
            if str(row["event_type"] or "").lower() == "pending_entry"
        ]
        first_pending_ts = min([ts for ts in pending_ts_values if ts is not None], default=None)
        after_pending = [
            row
            for row in signal_rows
            if first_pending_ts is None or (safe_float(row["event_ts"]) or 0) >= first_pending_ts
        ]
        terminal_rows = [
            row
            for row in after_pending
            if str(row["event_type"] or "").lower() == "entry_block"
            or str(row["decision"] or "").upper() in {"BLOCK", "REJECT", "WATCH_ONLY"}
        ]
        chosen = terminal_rows[0] if terminal_rows else (after_pending[-1] if after_pending else None)
        if chosen is None:
            reason_key = ("UNKNOWN", "missing_post_pending_event", "UNKNOWN", "missing_post_pending_event")
            chosen_payload = {}
        else:
            reason_key = _decision_reason_key(chosen)
            chosen_payload = jloads(chosen["payload_json"])
        pending_without_final_reasons[reason_key] += 1
        if len(pending_without_final_examples) < 20:
            pending_without_final_examples.append(
                {
                    "signal_id": signal_id,
                    "first_pending_ts": first_pending_ts,
                    "attribution": {
                        "component": reason_key[0],
                        "event_type": reason_key[1],
                        "decision": reason_key[2],
                        "reason": reason_key[3],
                        "hard_blockers": _extract_hard_blockers(chosen_payload),
                    },
                }
            )

    result.update(
        {
            "decision_records_by_signal_id": len(rows),
            "raw_signals_with_decision_record": len(with_decision & raw_id_set),
            "raw_signals_without_decision_record": len(missing_decision_ids),
            **no_decision_attribution,
            "raw_signals_with_pass_or_allow": len(pass_allow & raw_id_set),
            "raw_signals_with_pending_entry": len(pending & raw_id_set),
            "raw_signals_with_final_entry_contract": len(final_contract & raw_id_set),
            "raw_signals_with_final_entry_block": len(final_block & raw_id_set),
            "raw_signals_with_final_entry_mode_disabled": len(final_mode_disabled & raw_id_set),
            "raw_signals_with_final_entry_mode_disabled_only": len(final_mode_disabled_only & raw_id_set),
            "raw_signals_with_final_entry_mode_disabled_plus_other": len(final_mode_disabled_plus_other & raw_id_set),
            "raw_signals_with_decision_no_pass_or_allow": len(decision_no_pass),
            "decision_no_pass_or_allow_reason_counts": _reason_rows(decision_no_pass_reasons),
            "decision_no_pass_or_allow_examples": decision_no_pass_examples,
            "raw_signals_pass_or_allow_without_pending_entry": len(pass_without_pending),
            "pass_or_allow_without_pending_entry_reason_counts": _reason_rows(pass_without_pending_reasons),
            "pass_or_allow_without_pending_entry_examples": pass_without_pending_examples,
            "raw_signals_pending_without_final_entry_contract": len(pending_without_final),
            "pending_without_final_entry_reason_counts": _reason_rows(pending_without_final_reasons),
            "pending_without_final_entry_examples": pending_without_final_examples,
            "raw_scoped_final_entry_hard_blockers": dict(raw_scoped_final_blockers.most_common()),
            "component_decision_reason_counts": [
                {
                    "component": key[0],
                    "event_type": key[1],
                    "decision": key[2],
                    "reason": key[3],
                    "count": count,
                }
                for key, count in grouped.most_common(30)
            ],
        }
    )
    return result


def load_entry_bridge_summary(
    paper_db,
    db_path,
    since_ts,
    until_ts,
    raw_signal_ids=None,
    *,
    raw_rows=None,
    observations=None,
    token_time_decisions=None,
    expected_candidates=DEFAULT_EXPECTED_CANDIDATES,
):
    """Lightweight operational entry bridge audit.

    This intentionally queries indexed fixed components over the report window
    instead of loading every decision record for every token. It answers whether
    raw dogs reached pending enqueue, execution eligibility, final contract, and
    paper-entry evidence.
    """
    summary = {
        "paper_decision_events_available": table_exists(paper_db, "paper_decision_events"),
        "raw_signal_decision_bridge": load_raw_signal_decision_bridge(
            paper_db,
            raw_signal_ids or [],
            since_ts,
            until_ts,
            raw_rows=raw_rows or [],
            observations=observations or [],
            token_time_decisions=token_time_decisions or [],
            expected_candidates=expected_candidates,
        ),
        "components": {},
        "terminal_event_type_counts_in_decision_events": {},
        "final_entry_contract": {
            "rows": 0,
            "event_type_decision_reason_counts": [],
            "hard_blockers": {},
            "normalized_modes": {},
            "mode_status": {},
            "mode_action": {},
            "mode_reason": {},
            "enforced_counts": {},
            "quote_success_counts": {},
            "sample_blocks": [],
        },
        "paper_evidence_log": load_paper_evidence_event_counts(db_path, since_ts, until_ts),
        "paper_trades_entry_ts_window_count": 0,
    }
    if table_exists(paper_db, "paper_trades"):
        try:
            summary["paper_trades_entry_ts_window_count"] = paper_db.execute(
                "SELECT COUNT(*) FROM paper_trades WHERE entry_ts >= ? AND entry_ts <= ?",
                (since_ts, until_ts),
            ).fetchone()[0]
        except Exception:
            summary["paper_trades_entry_ts_window_count"] = None

    if not summary["paper_decision_events_available"]:
        return summary

    placeholders = ",".join("?" for _ in ENTRY_BRIDGE_COMPONENTS)
    rows = paper_db.execute(
        f"""
        SELECT component, event_type, decision, reason, COUNT(*) AS n
        FROM paper_decision_events
        WHERE event_ts >= ? AND event_ts <= ?
          AND component IN ({placeholders})
        GROUP BY component, event_type, decision, reason
        ORDER BY component ASC, n DESC
        """,
        [since_ts, until_ts, *ENTRY_BRIDGE_COMPONENTS],
    ).fetchall()
    grouped_by_component = defaultdict(list)
    for row in rows:
        grouped_by_component[row["component"]].append(row)
    summary["components"] = {
        component: compact_grouped_counts(grouped_by_component.get(component, []))
        for component in ENTRY_BRIDGE_COMPONENTS
    }

    event_placeholders = ",".join("?" for _ in ENTRY_TERMINAL_EVENT_TYPES)
    event_rows = paper_db.execute(
        f"""
        SELECT event_type, decision, reason, component, COUNT(*) AS n
        FROM paper_decision_events
        WHERE event_ts >= ? AND event_ts <= ?
          AND event_type IN ({event_placeholders})
        GROUP BY event_type, decision, reason, component
        ORDER BY n DESC
        """,
        [since_ts, until_ts, *ENTRY_TERMINAL_EVENT_TYPES],
    ).fetchall()
    summary["terminal_event_type_counts_in_decision_events"] = [
        {
            "event_type": row["event_type"],
            "component": row["component"],
            "decision": row["decision"],
            "reason": row["reason"],
            "count": row["n"],
        }
        for row in event_rows
    ]

    final_rows = paper_db.execute(
        """
        SELECT event_ts, signal_id, token_ca, symbol, lifecycle_id, event_type,
               decision, reason, payload_json
        FROM paper_decision_events
        WHERE event_ts >= ? AND event_ts <= ?
          AND component = 'final_entry_contract'
        ORDER BY event_ts DESC
        """,
        (since_ts, until_ts),
    ).fetchall()
    hard_blockers = Counter()
    normalized_modes = Counter()
    mode_status = Counter()
    mode_action = Counter()
    mode_reason = Counter()
    enforced_counts = Counter()
    quote_success = Counter()
    final_group_counts = Counter()
    samples = []
    for row in final_rows:
        payload = jloads(row["payload_json"])
        final_group_counts[(row["event_type"], row["decision"], row["reason"])] += 1
        for blocker in _extract_hard_blockers(payload):
            hard_blockers[blocker] += 1
        normalized_modes[_payload_counter_value(payload.get("normalized_mode"))] += 1
        state = _mode_state(payload)
        mode_status[_payload_counter_value(state.get("status"))] += 1
        mode_action[_payload_counter_value(state.get("action"))] += 1
        mode_reason[_payload_counter_value(state.get("reason"))] += 1
        enforced_counts[_payload_counter_value(payload.get("enforced"))] += 1
        quote_detail = payload.get("quote_detail") if isinstance(payload.get("quote_detail"), dict) else {}
        quote_success[_payload_counter_value(quote_detail.get("success"))] += 1
        if len(samples) < 10 and row["event_type"] == "entry_block":
            samples.append(
                {
                    "event_ts": row["event_ts"],
                    "signal_id": signal_id_key(row["signal_id"]),
                    "token_ca": row["token_ca"],
                    "symbol": row["symbol"],
                    "lifecycle_id": row["lifecycle_id"],
                    "decision": row["decision"],
                    "reason": row["reason"],
                    "hard_blockers": _extract_hard_blockers(payload),
                    "normalized_mode": payload.get("normalized_mode"),
                    "route_bucket": payload.get("route_bucket"),
                    "expected_rr": payload.get("expected_rr"),
                    "spread_pct": payload.get("spread_pct"),
                    "mode_state": {
                        "status": state.get("status"),
                        "action": state.get("action"),
                        "reason": state.get("reason"),
                        "circuit_broken": state.get("circuit_broken"),
                    },
                    "quote_detail": {
                        "success": quote_detail.get("success"),
                        "quote_clean": quote_detail.get("quote_clean"),
                        "route_available": quote_detail.get("route_available"),
                        "hard_blockers": quote_detail.get("hard_blockers"),
                    },
                }
            )
    summary["final_entry_contract"] = {
        "rows": len(final_rows),
        "event_type_decision_reason_counts": [
            {
                "event_type": key[0],
                "decision": key[1],
                "reason": key[2],
                "count": count,
            }
            for key, count in final_group_counts.most_common(30)
        ],
        "hard_blockers": dict(hard_blockers),
        "normalized_modes": dict(normalized_modes),
        "mode_status": dict(mode_status),
        "mode_action": dict(mode_action),
        "mode_reason": dict(mode_reason),
        "enforced_counts": dict(enforced_counts),
        "quote_success_counts": dict(quote_success),
        "sample_blocks": samples,
    }
    return summary


def decision_would_enter(row):
    action = str(row.get("action") or "").upper()
    would_action = str(row.get("would_action") or "").upper()
    event_type = str(row.get("event_type") or "").lower()
    decision = str(row.get("decision") or "").upper()
    return (
        action in {"WOULD_ENTER", "ENTER"}
        or would_action in {"WOULD_ENTER", "ENTER"}
        or event_type in {"would_enter", "enter"}
        or decision in {"WOULD_ENTER", "ENTER", "ALLOW", "PASS"}
        or truthy(row.get("would_enter_a_class"))
        or truthy(row.get("did_enter"))
    )


def decision_entered(row):
    action = str(row.get("action") or "").upper()
    event_type = str(row.get("event_type") or "").lower()
    decision = str(row.get("decision") or "").upper()
    return action == "ENTER" or event_type == "enter" or decision == "ENTER" or truthy(row.get("did_enter"))


def attach_records(raw_rows, observations, decisions, trades, expected_candidates):
    obs_by_signal = defaultdict(list)
    for obs in observations:
        obs_by_signal[obs["signal_id_key"]].append(obs)
    decisions_by_token = defaultdict(list)
    decisions_by_signal = defaultdict(list)
    for row in decisions:
        if row.get("token_ca"):
            decisions_by_token[str(row["token_ca"])].append(row)
        if row.get("signal_id_key"):
            decisions_by_signal[row["signal_id_key"]].append(row)
    trades_by_token = defaultdict(list)
    trades_by_signal = defaultdict(list)
    for row in trades:
        if row.get("token_ca"):
            trades_by_token[str(row["token_ca"])].append(row)
        if row.get("signal_id_key"):
            trades_by_signal[row["signal_id_key"]].append(row)

    audits = []
    for row in raw_rows:
        signal_key = row.get("signal_id_key")
        token = str(row.get("token_ca") or "")
        signal_ts = row.get("signal_ts_norm") or 0
        peak_sec = safe_float(row.get("time_to_sustained_peak_sec"))
        end_offset = max(60.0, min(900.0, peak_sec or 900.0))
        start = signal_ts - 60
        end = signal_ts + end_offset
        dog_obs = obs_by_signal.get(signal_key, [])
        matched_obs = [obs for obs in dog_obs if obs["matched"]]
        dog_decisions = []
        seen_decision_keys = set()
        for dec in decisions_by_signal.get(signal_key, []):
            key = (dec.get("source_kind"), dec.get("id"))
            seen_decision_keys.add(key)
            dog_decisions.append(dec)
        for dec in decisions_by_token.get(token, []):
            ts = dec.get("event_ts_norm")
            if ts is None or ts < start or ts > end:
                continue
            key = (dec.get("source_kind"), dec.get("id"))
            if key in seen_decision_keys:
                continue
            seen_decision_keys.add(key)
            dog_decisions.append(dec)
        dog_trades = []
        seen_trade_ids = set()
        for trade in trades_by_signal.get(signal_key, []):
            seen_trade_ids.add(trade.get("id"))
            dog_trades.append(trade)
        for trade in trades_by_token.get(token, []):
            ts = trade.get("entry_ts_norm")
            if ts is None or ts < start or ts > end:
                continue
            if trade.get("id") in seen_trade_ids:
                continue
            seen_trade_ids.add(trade.get("id"))
            dog_trades.append(trade)

        would_enter = [dec for dec in dog_decisions if decision_would_enter(dec)]
        decision_enter = [dec for dec in dog_decisions if decision_entered(dec)]
        entered = bool(dog_trades or decision_enter or row.get("raw_dog_entered_bool"))
        if not matched_obs:
            terminal = "no_candidate_match"
        elif not dog_decisions:
            terminal = "candidate_match_no_decision_record"
        elif not would_enter:
            terminal = "decision_record_no_would_enter"
        elif not entered:
            terminal = "would_enter_not_entered"
        elif not row.get("raw_dog_realized_bool"):
            terminal = "entered_not_realized"
        else:
            terminal = "realized_gold_silver"

        context_obs = next((obs for obs in dog_obs if obs["candidate_id"] == "current_all"), dog_obs[0] if dog_obs else None)
        payload = context_obs.get("payload") if context_obs else {}
        top_candidates = [
            {
                "candidate_id": obs["candidate_id"],
                "family": obs["family"],
                "reason": obs.get("reason"),
            }
            for obs in matched_obs[:20]
        ]
        audits.append(
            {
                "raw_event_idx": row["_event_idx"],
                "signal_id": signal_key,
                "token_ca": row.get("token_ca"),
                "symbol": row.get("symbol"),
                "signal_ts": row.get("signal_ts"),
                "tier": row.get("tier"),
                "max_sustained_peak_pct": row.get("max_sustained_peak_pct"),
                "time_to_sustained_peak_sec": row.get("time_to_sustained_peak_sec"),
                "evaluable": row.get("evaluable"),
                "evaluable_failures": row.get("evaluable_failures"),
                "candidate_observation_count": len(dog_obs),
                "expected_candidate_count": expected_candidates,
                "candidate_coverage_ok": len({obs["candidate_id"] for obs in dog_obs}) == expected_candidates,
                "matched_candidate_count": len(matched_obs),
                "matched_candidate_families": dict(Counter(obs["family"] or "UNKNOWN" for obs in matched_obs)),
                "top_matched_candidates": top_candidates,
                "lifecycle_profile": payload.get("lifecycle_profile"),
                "lifecycle_state": payload.get("lifecycle_state"),
                "source_component": payload.get("source_component"),
                "source_resonance_state": payload.get("source_resonance_state"),
                "hard_gate_status": payload.get("hard_gate_status"),
                "markov_bucket": payload.get("markov_bucket"),
                "markov_sample_n": payload.get("markov_sample_n"),
                "markov_gate_reason": payload.get("markov_gate_reason"),
                "source_quote_clean": payload.get("source_quote_clean", payload.get("source_quote_clean_seen")),
                "source_quote_executable": payload.get("source_quote_executable", payload.get("source_quote_executable_proxy")),
                "context_schema_version": payload.get("context_schema_version"),
                "quote_clean_definition": payload.get("quote_clean_definition"),
                "decision_record_count": len(dog_decisions),
                "decision_sources": dict(Counter(dec.get("source_kind") or "UNKNOWN" for dec in dog_decisions)),
                "would_enter_count": len(would_enter),
                "decision_enter_count": len(decision_enter),
                "paper_trade_count": len(dog_trades),
                "entered": entered,
                "raw_dog_entered": row.get("raw_dog_entered_bool"),
                "raw_dog_realized": row.get("raw_dog_realized_bool"),
                "terminal_bucket": terminal,
                "decision_reason_sample": [
                    {
                        "source_kind": dec.get("source_kind"),
                        "event_ts": dec.get("event_ts"),
                        "source_component": dec.get("source_component"),
                        "action": dec.get("action"),
                        "would_action": dec.get("would_action"),
                        "event_type": dec.get("event_type"),
                        "decision": dec.get("decision"),
                        "reason": dec.get("reason"),
                        "block_cause": dec.get("block_cause"),
                        "quote_clean": dec.get("quote_clean"),
                        "quote_executable": dec.get("quote_executable"),
                        "route_available": dec.get("route_available"),
                        "quote_failure_reason": dec.get("quote_failure_reason"),
                        "route_failure_reason": dec.get("route_failure_reason"),
                    }
                    for dec in dog_decisions[:10]
                ],
            }
        )
    return audits


def summarize(audits, raw_rows, observations, decisions, trades, expected_candidates, entry_bridge):
    raw_all = raw_rows
    evaluable = [row for row in raw_rows if row.get("evaluable")]
    terminal_counts = Counter(row["terminal_bucket"] for row in audits)
    matched_any = sum(1 for row in audits if row["matched_candidate_count"] > 0)
    no_match = len(audits) - matched_any
    has_decision = sum(1 for row in audits if row["decision_record_count"] > 0)
    would_enter = sum(1 for row in audits if row["would_enter_count"] > 0)
    entered = sum(1 for row in audits if row["entered"])
    realized = sum(1 for row in audits if row["raw_dog_realized"])
    coverage_ok = sum(1 for row in audits if row["candidate_coverage_ok"])

    candidate_counts = Counter()
    candidate_family_counts = Counter()
    candidate_gs_counts = Counter()
    for audit in audits:
        for cand in audit["top_matched_candidates"]:
            candidate_counts[cand["candidate_id"]] += 1
            candidate_family_counts[cand.get("family") or "UNKNOWN"] += 1
            candidate_gs_counts[(cand["candidate_id"], cand.get("family") or "UNKNOWN")] += 1

    lifecycle_counts = Counter((row.get("lifecycle_profile") or "UNKNOWN", row.get("source_component") or "UNKNOWN") for row in audits)
    lifecycle_would = Counter(
        (row.get("lifecycle_profile") or "UNKNOWN", row.get("source_component") or "UNKNOWN")
        for row in audits
        if row["would_enter_count"] > 0
    )
    lifecycle_entered = Counter(
        (row.get("lifecycle_profile") or "UNKNOWN", row.get("source_component") or "UNKNOWN")
        for row in audits
        if row["entered"]
    )
    markov_counts = Counter(str(row.get("markov_bucket") or "UNKNOWN") for row in audits)
    schema_counts = Counter(str(row.get("context_schema_version") or "legacy_or_missing") for row in audits)
    quote_def_counts = Counter(str(row.get("quote_clean_definition") or "legacy_or_missing") for row in audits)
    decision_reason_counts = Counter()
    for audit in audits:
        for dec in audit.get("decision_reason_sample") or []:
            key = (
                dec.get("source_kind") or "UNKNOWN",
                dec.get("source_component") or "UNKNOWN",
                dec.get("action") or dec.get("decision") or dec.get("event_type") or "UNKNOWN",
                dec.get("reason") or dec.get("block_cause") or dec.get("quote_failure_reason") or "UNKNOWN",
            )
            decision_reason_counts[key] += 1

    return {
        "raw_denominator": {
            "raw_all_gold_silver_event_rows": len(raw_all),
            "raw_all_gold_silver_unique_tokens": len({row.get("token_ca") for row in raw_all if row.get("token_ca")}),
            "evaluable_gold_silver_event_rows": len(evaluable),
            "evaluable_gold_silver_unique_tokens": len({row.get("token_ca") for row in evaluable if row.get("token_ca")}),
            "filtered_out_event_rows": max(0, len(raw_all) - len(evaluable)),
            "filter_drop_breakdown_non_exclusive": dict(
                Counter(failure for row in raw_all for failure in row.get("evaluable_failures", []))
            ),
            "entered_events": sum(1 for row in raw_all if row.get("raw_dog_entered_bool")),
            "realized_events": sum(1 for row in raw_all if row.get("raw_dog_realized_bool")),
        },
        "candidate_layer": {
            "expected_candidates": expected_candidates,
            "events_with_full_candidate_coverage": coverage_ok,
            "full_candidate_coverage_pct": pct(coverage_ok, len(audits)),
            "candidate_matched_any_events": matched_any,
            "candidate_matched_none_events": no_match,
            "candidate_match_any_rate": rate(matched_any, len(audits)),
            "observation_rows_loaded": len(observations),
            "top_candidates_by_raw_gs_match": [
                {
                    "candidate_id": cid,
                    "family": fam,
                    "matched_raw_gs_events": count,
                    "raw_gs_event_recall": rate(count, len(audits)),
                }
                for (cid, fam), count in candidate_gs_counts.most_common(30)
            ],
            "matched_candidate_family_counts": dict(candidate_family_counts),
        },
        "context_layer": {
            "lifecycle_source_distribution": [
                {
                    "lifecycle_profile": key[0],
                    "source_component": key[1],
                    "raw_gs_events": count,
                    "would_enter_events": lifecycle_would.get(key, 0),
                    "entered_events": lifecycle_entered.get(key, 0),
                    "gs_density_within_raw_denominator": rate(count, len(audits)),
                }
                for key, count in lifecycle_counts.most_common(30)
            ],
            "markov_bucket_distribution": dict(markov_counts),
            "context_schema_version_counts": dict(schema_counts),
            "quote_clean_definition_counts": dict(quote_def_counts),
        },
        "decision_layer": {
            "decision_records_loaded": len(decisions),
            "events_with_decision_record": has_decision,
            "events_without_decision_record": len(audits) - has_decision,
            "decision_record_rate": rate(has_decision, len(audits)),
            "would_enter_events": would_enter,
            "would_enter_rate": rate(would_enter, len(audits)),
            "entered_events": entered,
            "entered_rate": rate(entered, len(audits)),
            "realized_events": realized,
            "realized_rate": rate(realized, len(audits)),
            "decision_reason_top": [
                {
                    "source_kind": key[0],
                    "source_component": key[1],
                    "action_or_decision": key[2],
                    "reason": key[3],
                    "events": count,
                }
                for key, count in decision_reason_counts.most_common(30)
            ],
        },
        "entry_layer": {
            "paper_trades_loaded": len(trades),
            "terminal_bucket_counts": dict(terminal_counts),
        },
        "entry_bridge_layer": entry_bridge,
    }


def build_report(args):
    now_ts = int(time.time())
    since_ts = now_ts - int(args.hours * 3600)
    raw_db = sqlite3.connect(args.raw_db)
    raw_db.row_factory = sqlite3.Row
    paper_db = sqlite3.connect(args.db)
    paper_db.row_factory = sqlite3.Row
    try:
        raw_rows = load_raw_dogs(raw_db, since_ts)
        signal_ids, tokens, _, _ = make_raw_indexes(raw_rows)
        observations, obs_meta = load_candidate_observations(paper_db, signal_ids, since_ts)
        until_ts = max([row.get("signal_ts_norm") or since_ts for row in raw_rows] + [now_ts])
        decisions = [] if args.skip_decisions else load_paper_decisions(paper_db, tokens, since_ts, until_ts)
        trades = [] if args.skip_trades else load_paper_trades(paper_db, tokens, since_ts, until_ts)
        entry_bridge = load_entry_bridge_summary(
            paper_db,
            args.db,
            since_ts,
            now_ts,
            signal_ids,
            raw_rows=raw_rows,
            observations=observations,
            token_time_decisions=decisions,
            expected_candidates=args.expected_candidates,
        )
        audits = attach_records(raw_rows, observations, decisions, trades, args.expected_candidates)
        summary = summarize(audits, raw_rows, observations, decisions, trades, args.expected_candidates, entry_bridge)
        blockers = []
        if not raw_rows:
            blockers.append("raw_gold_silver_denominator_empty")
        if summary["candidate_layer"]["full_candidate_coverage_pct"] is not None and summary["candidate_layer"]["full_candidate_coverage_pct"] < 99:
            blockers.append("candidate_coverage_incomplete")
        if summary["raw_denominator"]["entered_events"] == 0:
            blockers.append("raw_gold_silver_entered_zero")
        if (summary["entry_bridge_layer"].get("paper_trades_entry_ts_window_count") or 0) == 0:
            blockers.append("paper_trades_zero_for_raw_gold_silver_window")
        final_contract = summary["entry_bridge_layer"].get("final_entry_contract") or {}
        final_hard_blockers = final_contract.get("hard_blockers") or {}
        if final_hard_blockers.get("mode_disabled", 0):
            blockers.append("final_entry_contract_mode_disabled")
        if final_contract.get("rows", 0) and not final_contract.get("event_type_decision_reason_counts"):
            blockers.append("final_entry_contract_unparsed")
        evidence_events = (summary["entry_bridge_layer"].get("paper_evidence_log") or {}).get("event_type_counts") or {}
        if not evidence_events.get("paper_trade_entry_intent", 0):
            blockers.append("paper_trade_entry_intent_zero")
        if summary["context_layer"]["context_schema_version_counts"].get("legacy_or_missing", 0):
            blockers.append("mixed_or_legacy_context_schema")
        if summary["raw_denominator"]["filter_drop_breakdown_non_exclusive"].get("kline_uncovered", 0):
            blockers.append("kline_coverage_incomplete")
        verdict = "BLOCKED_DATA"
        if raw_rows and not blockers:
            verdict = "DISCOVERY_WATCH"
        elif raw_rows:
            verdict = "FUNNEL_BLOCKED"
        return {
            "schema_version": SCHEMA_VERSION,
            "evidence_level": EVIDENCE_LEVEL,
            "can_promote_live": False,
            "report_type": "raw_gold_silver_entry_funnel_audit",
            "generated_at": now_ts,
            "window": {"hours": args.hours, "since_ts": since_ts, "until_ts": now_ts},
            "inputs": {"paper_db": args.db, "raw_db": args.raw_db},
            "load_options": {
                "skip_decisions": args.skip_decisions,
                "skip_trades": args.skip_trades,
            },
            "verdict": verdict,
            "promotion_blockers": blockers,
            "observation_load": obs_meta,
            "summary": summary,
            "missed_examples": sorted(
                audits,
                key=lambda row: safe_float(row.get("max_sustained_peak_pct")) or 0,
                reverse=True,
            )[: args.limit],
        }
    finally:
        raw_db.close()
        paper_db.close()


def compact_stdout_summary(report, out_path=None):
    summary = report.get("summary") or {}
    entry_bridge = summary.get("entry_bridge_layer") or {}
    final_contract = entry_bridge.get("final_entry_contract") or {}
    raw_bridge = entry_bridge.get("raw_signal_decision_bridge") or {}
    evidence_log = entry_bridge.get("paper_evidence_log") or {}
    return {
        "out": str(out_path) if out_path else None,
        "verdict": report.get("verdict"),
        "promotion_blockers": report.get("promotion_blockers") or [],
        "raw_denominator": summary.get("raw_denominator") or {},
        "decision_layer": {
            key: (summary.get("decision_layer") or {}).get(key)
            for key in (
                "decision_record_rate",
                "events_with_decision_record",
                "events_without_decision_record",
                "would_enter_events",
                "would_enter_rate",
                "entered_events",
                "entered_rate",
                "realized_events",
                "realized_rate",
            )
        },
        "entry_bridge": {
            key: raw_bridge.get(key)
            for key in (
                "raw_signal_ids",
                "raw_signals_with_decision_record",
                "raw_signals_without_decision_record",
                "raw_signals_with_pass_or_allow",
                "raw_signals_with_pending_entry",
                "raw_signals_with_final_entry_contract",
                "raw_signals_with_final_entry_block",
                "raw_signals_with_final_entry_mode_disabled",
                "raw_signals_with_final_entry_mode_disabled_only",
                "raw_signals_with_final_entry_mode_disabled_plus_other",
                "no_decision_record_root_cause_counts",
                "no_decision_token_time_decision_without_exact_signal_id",
                "no_decision_candidate_shadow_observed_no_decision_event",
                "no_decision_partial_candidate_observation_no_decision_event",
                "no_decision_no_candidate_observation_or_decision_event",
                "raw_signals_with_decision_no_pass_or_allow",
                "raw_signals_pass_or_allow_without_pending_entry",
                "raw_scoped_final_entry_hard_blockers",
            )
        },
        "final_entry_contract": {
            "rows": final_contract.get("rows"),
            "hard_blockers": final_contract.get("hard_blockers") or {},
            "mode_status": final_contract.get("mode_status") or {},
            "mode_action": final_contract.get("mode_action") or {},
            "mode_reason": final_contract.get("mode_reason") or {},
        },
        "paper_evidence_event_counts": evidence_log.get("event_type_counts") or {},
        "paper_trades_entry_ts_window_count": entry_bridge.get("paper_trades_entry_ts_window_count"),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="/app/data/paper_trades.db")
    ap.add_argument("--raw-db", default="/app/data/raw_signal_outcomes.db")
    ap.add_argument("--hours", type=float, default=72)
    ap.add_argument("--expected-candidates", type=int, default=DEFAULT_EXPECTED_CANDIDATES)
    ap.add_argument("--limit", type=int, default=80, help="number of top missed examples to include")
    ap.add_argument("--skip-decisions", action="store_true", help="skip decision table loading")
    ap.add_argument("--skip-trades", action="store_true", help="skip paper trade table loading")
    ap.add_argument("--out")
    args = ap.parse_args()
    report = build_report(args)
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text + "\n", encoding="utf-8")
        print(json.dumps(compact_stdout_summary(report, out), ensure_ascii=False, sort_keys=True))
    else:
        print(text)


if __name__ == "__main__":
    main()
