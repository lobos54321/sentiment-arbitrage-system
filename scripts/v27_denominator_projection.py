#!/usr/bin/env python3
"""Build a v2.7 denominator seed projection from the append-only event log.

This projection is deliberately conservative. It only counts D0/D1/D2/D3a/D3b
when the mirrored decision payload carries explicit evidence for the required
contract fields. Missing evidence is reported instead of inferred.
"""

import argparse
import json
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, V27EventLogError, sha256_hex  # noqa: E402


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
MIRRORED_DECISION_EVENT_TYPE = "paper_decision_event_recorded"
MIRRORED_MISSED_EVENT_TYPE = "paper_missed_signal_attribution_recorded"
TELEGRAM_SIGNAL_EVENT_TYPE = "telegram_signal_seen"
SOURCE_DOG_LABEL_EVENT_TYPE = "source_dog_label_recorded"
DENOMINATOR_SEED_EVENT_TYPES = {MIRRORED_DECISION_EVENT_TYPE, MIRRORED_MISSED_EVENT_TYPE, TELEGRAM_SIGNAL_EVENT_TYPE, SOURCE_DOG_LABEL_EVENT_TYPE}
GOLD_SILVER_LABELS = {"gold", "silver"}
DOG_LABELS = {"gold", "silver", "copper", "bronze", "sub25", "none", "unknown"}


def _nested_get(mapping, path, default=None):
    cursor = mapping
    for key in path:
        if not isinstance(cursor, dict) or key not in cursor:
            return default
        cursor = cursor.get(key)
    return cursor


def _first_present(mapping, paths, default=None):
    for path in paths:
        value = _nested_get(mapping, path, default=None)
        if value is not None:
            return value
    return default


def _as_bool(value):
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on", "ok", "pass", "passed", "clean", "confirmed"}:
            return True
        if normalized in {"0", "false", "no", "n", "off", "fail", "failed", "dirty", "bad", "unknown"}:
            return False
    return None


def _as_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _clean_label(value):
    if value is None:
        return None
    label = str(value).strip().lower()
    if not label:
        return None
    return label


def _payload_bags(event):
    outer = event.get("payload") or {}
    legacy_payload = outer.get("payload") if isinstance(outer.get("payload"), dict) else {}
    lifecycle = outer.get("lifecycle") if isinstance(outer.get("lifecycle"), dict) else {}
    lifecycle_features = lifecycle.get("lifecycle_features") if isinstance(lifecycle.get("lifecycle_features"), dict) else {}
    return {
        "outer": outer,
        "legacy_payload": legacy_payload,
        "lifecycle": lifecycle,
        "lifecycle_features": lifecycle_features,
    }


def _extract_flag(bags, paths):
    for bag_name in ("legacy_payload", "outer", "lifecycle", "lifecycle_features"):
        bag = bags[bag_name]
        value = _first_present(bag, paths)
        parsed = _as_bool(value)
        if parsed is not None:
            return parsed
    return None


def _extract_scalar(bags, paths, default=None):
    for bag_name in ("legacy_payload", "outer", "lifecycle", "lifecycle_features"):
        value = _first_present(bags[bag_name], paths)
        if value is not None:
            return value
    return default


def _extract_source_label(bags):
    value = _extract_scalar(
        bags,
        [
            ("source_dog_label",),
            ("source_label_tier",),
            ("source_outcome_tier",),
            ("dog_label",),
            ("dog_tier",),
            ("source", "dog_label"),
            ("label", "source_dog_label"),
        ],
    )
    label = _clean_label(value)
    if label in DOG_LABELS:
        return label
    return label


def _denominator_key(fields):
    return ":".join(
        [
            str(fields.get("chain") or "unknown_chain"),
            str(fields.get("token_ca")),
            str(fields.get("canonical_pool_group") or "unknown_pool"),
            str(fields.get("lifecycle_epoch", 0)),
        ]
    )


def _extract_decision_fact(event):
    bags = _payload_bags(event)
    outer = bags["outer"]
    token_ca = outer.get("token_ca") or _extract_scalar(bags, [("token_ca",), ("token", "ca")])
    if token_ca is not None:
        token_ca = str(token_ca).strip() or None
    chain = _extract_scalar(bags, [("chain",), ("chain_id",)], default="unknown_chain")
    canonical_pool_group = _extract_scalar(
        bags,
        [
            ("canonical_pool_group",),
            ("pool_group",),
            ("selected_pool",),
            ("pool_address",),
            ("lifecycle_id",),
        ],
        default=outer.get("lifecycle_id") or "unknown_pool",
    )
    lifecycle_epoch = _as_int(_extract_scalar(bags, [("lifecycle_epoch",), ("epoch",)], default=0), default=0)
    source_label = _extract_source_label(bags)
    captured_flag = _extract_flag(
        bags,
        [
            ("captured",),
            ("paper_entered",),
            ("tiny_entered",),
            ("entry_executed",),
            ("trade", "captured"),
        ],
    )
    if captured_flag is None:
        decision = str(outer.get("decision") or "").strip().lower()
        legacy_event_type = str(outer.get("legacy_event_type") or "").strip().lower()
        captured_flag = decision in {"enter", "entered", "paper_enter", "paper_entered", "tiny_entered"} or legacy_event_type in {
            "entry",
            "paper_entry",
            "tiny_entered",
        }

    flags = {
        "telegram_seen": _extract_flag(bags, [("telegram_seen",), ("telegram_anchor",), ("telegram_lifecycle_event",)]),
        "realtime_observable": _extract_flag(bags, [("realtime_observable",), ("d1_realtime_observable",), ("low_latency_observed",)]),
        "realtime_clean": _extract_flag(bags, [("realtime_clean",), ("d2_realtime_clean",), ("quote_clean",)]),
        "entry_quote_executable": _extract_flag(bags, [("entry_quote_executable",), ("entry_quote_executable_ok",)]),
        "exit_quote_executable": _extract_flag(bags, [("exit_quote_executable",), ("exit_quote_executable_ok",)]),
        "liquidity_ok": _extract_flag(bags, [("liquidity_ok",), ("liquidity_depth_ok",)]),
        "critical_risk_ok": _extract_flag(bags, [("critical_risk_ok",), ("critical_risk_known_clean",), ("critical_risk_not_bad",)]),
        "ex_ante_feasible": _extract_flag(bags, [("ex_ante_feasible",), ("ex_ante_feasibility",)]),
        "reclaim_confirmed": _extract_flag(bags, [("reclaim_confirmed",), ("reclaim_confirmed_ok",)]),
        "not_overextended": _extract_flag(bags, [("not_overextended",), ("not_overextended_ok",)]),
        "model_pass": _extract_flag(bags, [("model_pass",), ("model_passed",), ("absorbing_peak_prob_ok",)]),
    }

    fields = {
        "event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
        "aggregate_id": event.get("aggregate_id"),
        "decision_event_id": outer.get("decision_event_id"),
        "token_ca": token_ca,
        "symbol": outer.get("symbol"),
        "chain": chain,
        "canonical_pool_group": canonical_pool_group,
        "lifecycle_epoch": lifecycle_epoch,
        "route": outer.get("route"),
        "component": outer.get("component"),
        "decision": outer.get("decision"),
        "reason": outer.get("reason"),
        "source_dog_label": source_label,
        "source_label_research_only": _extract_flag(bags, [("source_label_research_only",), ("source_dog_label_research_only",)]),
        "captured": captured_flag,
        **flags,
    }
    fields["denominator_dedup_key"] = _denominator_key(fields) if token_ca else None
    return fields


def _merge_bool(existing, incoming):
    if existing is True or incoming is True:
        return True
    if existing is False or incoming is False:
        return False
    return None


def _new_record(fact):
    return {
        "denominator_dedup_key": fact["denominator_dedup_key"],
        "token_ca": fact["token_ca"],
        "symbol": fact.get("symbol"),
        "chain": fact.get("chain"),
        "canonical_pool_group": fact.get("canonical_pool_group"),
        "lifecycle_epoch": fact.get("lifecycle_epoch"),
        "merged_decision_event_ids": [],
        "source_event_ids": [],
        "routes": [],
        "components": [],
        "dedup_reason": "same chain/token/canonical_pool_group/lifecycle_epoch",
        "primary_outcome_role": "source_dog_label" if fact.get("source_dog_label") else "unlabeled_decision_seed",
        "source_label_conflicts": [],
        "source_label_research_only": False,
        "denominator_dirty_reasons": [],
        "source_dog_label": None,
        "captured": False,
    }


DENOMINATOR_FLAGS = [
    "telegram_seen",
    "realtime_observable",
    "realtime_clean",
    "entry_quote_executable",
    "exit_quote_executable",
    "liquidity_ok",
    "critical_risk_ok",
    "ex_ante_feasible",
    "reclaim_confirmed",
    "not_overextended",
    "model_pass",
]


def _merge_fact(record, fact):
    record["merged_decision_event_ids"].append(fact.get("decision_event_id"))
    record["source_event_ids"].append(fact.get("event_id"))
    if fact.get("route") and fact.get("route") not in record["routes"]:
        record["routes"].append(fact.get("route"))
    if fact.get("component") and fact.get("component") not in record["components"]:
        record["components"].append(fact.get("component"))
    record["captured"] = bool(record["captured"] or fact.get("captured"))

    label = fact.get("source_dog_label")
    record["source_label_research_only"] = bool(record.get("source_label_research_only") or fact.get("source_label_research_only"))
    if label:
        if record["source_dog_label"] and record["source_dog_label"] != label:
            conflict = {"existing": record["source_dog_label"], "incoming": label, "event_id": fact.get("event_id")}
            record["source_label_conflicts"].append(conflict)
            if "source_label_conflict" not in record["denominator_dirty_reasons"]:
                record["denominator_dirty_reasons"].append("source_label_conflict")
        elif not record["source_dog_label"]:
            record["source_dog_label"] = label
            record["primary_outcome_role"] = "source_dog_label"

    for flag in DENOMINATOR_FLAGS:
        record[flag] = _merge_bool(record.get(flag), fact.get(flag))


def _record_missing_evidence(record):
    missing = []
    if not record.get("source_dog_label"):
        missing.append("SourceDogLabelContract")
    elif record.get("source_label_research_only"):
        missing.append("ProductionSourceDogLabelContract")
    for field, contract in [
        ("telegram_seen", "TelegramLifecycleEvent"),
        ("realtime_observable", "D1RealtimeObservability"),
        ("realtime_clean", "RealtimeCleanDetector"),
        ("entry_quote_executable", "PreTradeRoundTripQuoteContract.entry"),
        ("exit_quote_executable", "PreTradeRoundTripQuoteContract.exit"),
        ("liquidity_ok", "LiquidityDepth"),
        ("critical_risk_ok", "RiskContract"),
        ("ex_ante_feasible", "ExAnteFeasibility"),
        ("reclaim_confirmed", "ReclaimDetector"),
        ("not_overextended", "OverextensionDetector"),
        ("model_pass", "ForecastModelPass"),
    ]:
        if record.get(field) is None:
            missing.append(contract)
    record["missing_evidence"] = missing
    return missing


def _record_denominator_membership(record):
    dirty = bool(record.get("denominator_dirty_reasons"))
    source_gold_silver = record.get("source_dog_label") in GOLD_SILVER_LABELS
    d0 = (not dirty) and source_gold_silver and record.get("telegram_seen") is True
    d1 = d0 and record.get("realtime_observable") is True
    d2 = d1 and record.get("realtime_clean") is True
    d3a = (
        d2
        and record.get("entry_quote_executable") is True
        and record.get("exit_quote_executable") is True
        and record.get("liquidity_ok") is True
        and record.get("critical_risk_ok") is True
        and record.get("ex_ante_feasible") is True
    )
    d3b = d3a and record.get("reclaim_confirmed") is True and record.get("not_overextended") is True and record.get("model_pass") is True
    record["denominator_membership"] = {
        "D0_telegram_gold_silver_total": bool(d0),
        "D1_realtime_observable_gold_silver": bool(d1),
        "D2_realtime_clean_gold_silver": bool(d2),
        "D3a_externally_actionable_gold_silver": bool(d3a),
        "D3b_policy_actionable_gold_silver": bool(d3b),
    }


def build_denominator_projection(event_log_dir, *, include_records=False):
    event_log_dir = Path(event_log_dir)
    event_log = V27EventLog(event_log_dir)
    projection = {
        "projection_name": "v27_denominator_seed",
        "projection_version": "v0.1",
        "spec_version": "v2.7.0",
        "event_log_dir": str(event_log_dir),
        "event_log_verify": None,
        "event_log_error": None,
        "input_events": 0,
        "telegram_signal_seen_events": 0,
        "source_dog_label_events": 0,
        "mirrored_decision_events": 0,
        "mirrored_missed_attribution_events": 0,
        "dirty_events": [],
        "dirty_records": [],
        "metrics": {
            "denominator_seed_records": 0,
            "telegram_gold_silver_total_D0": 0,
            "telegram_realtime_observable_gold_silver_D1": 0,
            "telegram_realtime_clean_gold_silver_D2": 0,
            "telegram_externally_actionable_gold_silver_D3a": 0,
            "telegram_policy_actionable_gold_silver_D3b": 0,
            "telegram_captured_actionable_D3a": 0,
            "telegram_captured_actionable_D3b": 0,
            "telegram_capture_rate_D3a": None,
            "telegram_capture_rate_D3b": None,
        },
        "evidence_gaps": {},
        "health": {
            "event_log_ok": False,
            "projection_built": False,
            "denominator_clean": False,
            "normal_tiny_ready": False,
            "status": "not_built",
        },
        "records_hash": None,
    }

    try:
        projection["event_log_verify"] = event_log.verify()
        projection["health"]["event_log_ok"] = True
    except V27EventLogError as exc:
        projection["event_log_error"] = str(exc)
        projection["health"]["status"] = "event_log_invalid"
        return projection

    records = {}
    for event in event_log.iter_events() or []:
        projection["input_events"] += 1
        if event.get("event_type") not in DENOMINATOR_SEED_EVENT_TYPES:
            continue
        if event.get("event_type") == MIRRORED_DECISION_EVENT_TYPE:
            projection["mirrored_decision_events"] += 1
        if event.get("event_type") == MIRRORED_MISSED_EVENT_TYPE:
            projection["mirrored_missed_attribution_events"] += 1
        if event.get("event_type") == TELEGRAM_SIGNAL_EVENT_TYPE:
            projection["telegram_signal_seen_events"] += 1
        if event.get("event_type") == SOURCE_DOG_LABEL_EVENT_TYPE:
            projection["source_dog_label_events"] += 1
        fact = _extract_decision_fact(event)
        if not fact.get("token_ca"):
            projection["dirty_events"].append(
                {
                    "event_id": event.get("event_id"),
                    "decision_event_id": fact.get("decision_event_id"),
                    "reason": "missing_token_ca",
                }
            )
            projection["evidence_gaps"]["TokenIdentityContract"] = projection["evidence_gaps"].get("TokenIdentityContract", 0) + 1
            continue
        key = fact["denominator_dedup_key"]
        if key not in records:
            records[key] = _new_record(fact)
        _merge_fact(records[key], fact)

    record_list = []
    for key in sorted(records):
        record = records[key]
        missing = _record_missing_evidence(record)
        for contract in missing:
            projection["evidence_gaps"][contract] = projection["evidence_gaps"].get(contract, 0) + 1
        _record_denominator_membership(record)
        membership = record["denominator_membership"]
        metrics = projection["metrics"]
        metrics["denominator_seed_records"] += 1
        if membership["D0_telegram_gold_silver_total"]:
            metrics["telegram_gold_silver_total_D0"] += 1
        if membership["D1_realtime_observable_gold_silver"]:
            metrics["telegram_realtime_observable_gold_silver_D1"] += 1
        if membership["D2_realtime_clean_gold_silver"]:
            metrics["telegram_realtime_clean_gold_silver_D2"] += 1
        if membership["D3a_externally_actionable_gold_silver"]:
            metrics["telegram_externally_actionable_gold_silver_D3a"] += 1
            if record.get("captured"):
                metrics["telegram_captured_actionable_D3a"] += 1
        if membership["D3b_policy_actionable_gold_silver"]:
            metrics["telegram_policy_actionable_gold_silver_D3b"] += 1
            if record.get("captured"):
                metrics["telegram_captured_actionable_D3b"] += 1
        if record.get("denominator_dirty_reasons"):
            projection["dirty_records"].append(
                {
                    "denominator_dedup_key": key,
                    "reasons": record.get("denominator_dirty_reasons"),
                }
            )
        record_list.append(record)

    metrics = projection["metrics"]
    if metrics["telegram_externally_actionable_gold_silver_D3a"]:
        metrics["telegram_capture_rate_D3a"] = metrics["telegram_captured_actionable_D3a"] / metrics["telegram_externally_actionable_gold_silver_D3a"]
    if metrics["telegram_policy_actionable_gold_silver_D3b"]:
        metrics["telegram_capture_rate_D3b"] = metrics["telegram_captured_actionable_D3b"] / metrics["telegram_policy_actionable_gold_silver_D3b"]

    projection["records_hash"] = sha256_hex(record_list)
    projection["health"]["projection_built"] = True
    projection["health"]["denominator_clean"] = not projection["dirty_events"] and not projection["dirty_records"]
    if projection["dirty_events"]:
        projection["health"]["status"] = "seed_partial_dirty_events"
    elif projection["dirty_records"]:
        projection["health"]["status"] = "seed_dirty_records"
    elif metrics["telegram_gold_silver_total_D0"]:
        projection["health"]["status"] = "seed_ready"
    elif metrics["denominator_seed_records"]:
        projection["health"]["status"] = "seed_partial_missing_source_labels"
    else:
        projection["health"]["status"] = "seed_empty"
    if include_records:
        projection["records"] = record_list
    return projection


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--output")
    parser.add_argument("--include-records", action="store_true")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    projection = build_denominator_projection(args.event_log_dir, include_records=args.include_records)
    rendered = json.dumps(projection, ensure_ascii=False, sort_keys=True, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    if args.strict and (projection.get("event_log_error") or not projection["health"].get("denominator_clean")):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
