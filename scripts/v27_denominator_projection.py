#!/usr/bin/env python3
"""Build a v2.7 denominator seed projection from the append-only event log.

This projection is deliberately conservative. It only counts D0/D1/D2/D3a/D3b
when the mirrored decision payload carries explicit evidence for the required
contract fields. Missing evidence is reported instead of inferred.
"""

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_event_log import V27EventLog, V27EventLogError, sha256_hex  # noqa: E402


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DEFAULT_SPEC_MANIFEST = PROJECT_ROOT / "spec" / "telegram_dog_regime_capture" / "v2.7.0" / "spec.manifest.json"
MIRRORED_DECISION_EVENT_TYPE = "paper_decision_event_recorded"
MIRRORED_MISSED_EVENT_TYPE = "paper_missed_signal_attribution_recorded"
TELEGRAM_SIGNAL_EVENT_TYPE = "telegram_signal_seen"
SOURCE_DOG_LABEL_EVENT_TYPE = "source_dog_label_recorded"
LIFECYCLE_IDENTITY_EVENT_TYPE = "token_lifecycle_identity_resolved"
TRADE_OUTCOME_LABEL_EVENT_TYPE = "trade_outcome_label_recorded"
STANDARDIZED_STOP_EVENT_TYPE = "standardized_stop_contract_recorded"
EX_ANTE_FEASIBILITY_EVENT_TYPE = "ex_ante_feasibility_recorded"
DENOMINATOR_SEED_EVENT_TYPES = {
    MIRRORED_DECISION_EVENT_TYPE,
    MIRRORED_MISSED_EVENT_TYPE,
    TELEGRAM_SIGNAL_EVENT_TYPE,
    SOURCE_DOG_LABEL_EVENT_TYPE,
    LIFECYCLE_IDENTITY_EVENT_TYPE,
    TRADE_OUTCOME_LABEL_EVENT_TYPE,
    STANDARDIZED_STOP_EVENT_TYPE,
    EX_ANTE_FEASIBILITY_EVENT_TYPE,
}
GOLD_SILVER_LABELS = {"gold", "silver"}
DOG_LABELS = {"gold", "silver", "copper", "bronze", "sub25", "none", "unknown"}


def _utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_ts(value):
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _lag_ms(older_ts, newer_ts):
    older = _parse_iso_ts(older_ts)
    newer = _parse_iso_ts(newer_ts)
    if older is None or newer is None:
        return None
    return max(0, int((newer - older).total_seconds() * 1000))


def _load_spec_metadata(spec_manifest_path=DEFAULT_SPEC_MANIFEST):
    spec_manifest_path = Path(spec_manifest_path)
    try:
        with spec_manifest_path.open("r", encoding="utf-8") as fh:
            manifest = json.load(fh)
        catalog_path = spec_manifest_path.parent / manifest.get("contract_catalog_file", "contract-catalog.json")
        from v27_spec_validate import validate_all  # noqa: WPS433

        validation = validate_all(manifest_path=spec_manifest_path, catalog_path=catalog_path)
        return {
            "spec_id": validation["spec_id"],
            "spec_version": validation["spec_version"],
            "spec_hash": validation["spec_hash"],
            "spec_valid": True,
            "spec_manifest_path": str(spec_manifest_path),
        }
    except Exception as exc:  # pragma: no cover - exercised by integration failures.
        return {
            "spec_id": None,
            "spec_version": None,
            "spec_hash": None,
            "spec_valid": False,
            "spec_manifest_path": str(spec_manifest_path),
            "spec_error": str(exc),
        }


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


def _as_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _earlier_iso(existing, candidate):
    if not candidate:
        return existing
    if not existing:
        return candidate
    existing_parsed = _parse_iso_ts(existing)
    candidate_parsed = _parse_iso_ts(candidate)
    if existing_parsed is None:
        return candidate
    if candidate_parsed is None:
        return existing
    return candidate if candidate_parsed < existing_parsed else existing


def _later_iso(existing, candidate):
    if not candidate:
        return existing
    if not existing:
        return candidate
    existing_parsed = _parse_iso_ts(existing)
    candidate_parsed = _parse_iso_ts(candidate)
    if existing_parsed is None:
        return candidate
    if candidate_parsed is None:
        return existing
    return candidate if candidate_parsed > existing_parsed else existing


def _denominator_key(fields):
    return ":".join(
        [
            str(fields.get("chain") or "unknown_chain"),
            str(fields.get("token_ca")),
            str(fields.get("canonical_pool_group") or "unknown_pool"),
            str(fields.get("lifecycle_epoch", 0)),
        ]
    )


def _extract_reference_price_contract(event, bags):
    explicit_price = _as_float(
        _extract_scalar(
            bags,
            [
                ("source_reference_price",),
                ("reference_price",),
                ("counterfactual_entry_quote",),
                ("simulated_fill_price",),
                ("entry_quote_price",),
                ("entry_price",),
                ("baseline_price",),
            ],
        )
    )
    if explicit_price is None or not math.isfinite(explicit_price) or explicit_price <= 0:
        return None
    explicit_type = _extract_scalar(
        bags,
        [
            ("source_reference_price_type",),
            ("reference_price_type",),
            ("entry_quote_price_type",),
            ("price_type",),
        ],
    )
    if explicit_type == "missing":
        return None
    if not explicit_type:
        if _as_float(_extract_scalar(bags, [("baseline_price",)])) is not None:
            explicit_type = "legacy_baseline_price"
        elif _as_float(_extract_scalar(bags, [("entry_price",)])) is not None:
            explicit_type = "legacy_entry_price"
        elif _as_float(_extract_scalar(bags, [("simulated_fill_price",)])) is not None:
            explicit_type = "simulated_fill_price"
        elif _as_float(_extract_scalar(bags, [("counterfactual_entry_quote",), ("entry_quote_price",)])) is not None:
            explicit_type = "counterfactual_entry_quote"
        else:
            explicit_type = "legacy_reference_price"
    reference_ts = _extract_scalar(
        bags,
        [
            ("source_reference_price_ts",),
            ("reference_price_ts",),
            ("entry_quote_ts",),
            ("source_label_available_at",),
            ("signal_ts",),
            ("event_ts",),
            ("receive_ts",),
        ],
        default=event.get("available_at") or event.get("observed_at") or event.get("ingested_at"),
    )
    return {
        "reference_price_type": str(explicit_type),
        "reference_price": explicit_price,
        "reference_price_ts": reference_ts,
        "reference_quote_source": _extract_scalar(bags, [("reference_quote_source",), ("quote_source",), ("data_source",)], default=event.get("source")),
        "reference_quote_age_sec": _extract_scalar(bags, [("reference_quote_age_sec",), ("quote_age_sec",)]),
        "reference_price_available_at": _extract_scalar(
            bags,
            [("reference_price_available_at",), ("source_label_available_at",), ("receive_ts",)],
            default=event.get("available_at"),
        ),
        "reference_price_quality": _extract_scalar(
            bags,
            [("reference_price_quality",), ("source_label_quality",), ("quote_quality",)],
            default="legacy_seed",
        ),
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_trade_outcome_label_contract(event, bags):
    version = _extract_scalar(bags, [("trade_outcome_label_version",)])
    if event.get("event_type") != TRADE_OUTCOME_LABEL_EVENT_TYPE and not version:
        return None
    counterfactual_entry_ts = _extract_scalar(bags, [("counterfactual_entry_ts",), ("entry_ts",)])
    simulated_fill_price = _as_float(_extract_scalar(bags, [("simulated_fill_price",), ("entry_price",)]))
    trade_label_available_at = _extract_scalar(
        bags,
        [("trade_label_available_at",), ("exit_ts",), ("updated_at")],
        default=event.get("available_at"),
    )
    if not version and counterfactual_entry_ts is None and simulated_fill_price is None:
        return None
    missing_fields = []
    if not version:
        missing_fields.append("trade_outcome_label_version")
    if counterfactual_entry_ts is None:
        missing_fields.append("counterfactual_entry_ts")
    if simulated_fill_price is None or not math.isfinite(simulated_fill_price) or simulated_fill_price <= 0:
        missing_fields.append("simulated_fill_price")
    if trade_label_available_at is None:
        missing_fields.append("trade_label_available_at")
    return {
        "trade_outcome_label_version": version,
        "counterfactual_entry_ts": counterfactual_entry_ts,
        "counterfactual_entry_reason": _extract_scalar(bags, [("counterfactual_entry_reason",)]),
        "fill_time_anchor": _extract_scalar(bags, [("fill_time_anchor",)], default="simulated_fill_ts"),
        "simulated_fill_ts": _extract_scalar(bags, [("simulated_fill_ts",), ("entry_ts",)]),
        "simulated_fill_price": simulated_fill_price,
        "net_delayed_executable_peak_1s": _as_float(_extract_scalar(bags, [("net_delayed_executable_peak_1s",)])),
        "net_delayed_executable_peak_3s": _as_float(_extract_scalar(bags, [("net_delayed_executable_peak_3s",), ("peak_pnl",)])),
        "net_delayed_executable_peak_5s": _as_float(_extract_scalar(bags, [("net_delayed_executable_peak_5s",)])),
        "realized_pnl": _as_float(_extract_scalar(bags, [("realized_pnl",), ("pnl_pct",)])),
        "exit_capture_ratio": _as_float(_extract_scalar(bags, [("exit_capture_ratio",)])),
        "trade_label_available_at": trade_label_available_at,
        "trade_outcome_label_quality": _extract_scalar(bags, [("trade_outcome_label_quality",)], default="legacy_seed"),
        "ledger_authority_proven": bool(_extract_scalar(bags, [("ledger_authority_proven",)], default=False)),
        "paper_trade_id": _extract_scalar(bags, [("paper_trade_id",), ("id",)]),
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
        "missing_fields": missing_fields,
    }


def _extract_standardized_stop_contract(event, bags):
    version = _extract_scalar(bags, [("stop_contract_version",)])
    if event.get("event_type") != STANDARDIZED_STOP_EVENT_TYPE and not version:
        return None
    stop_threshold_pct = _as_float(_extract_scalar(bags, [("stop_threshold_pct",)]))
    stop_executable_required = _extract_scalar(bags, [("stop_executable_required",)])
    stop_available_at = _extract_scalar(
        bags,
        [("stop_available_at",), ("counterfactual_entry_ts",), ("simulated_fill_ts",), ("entry_ts")],
        default=event.get("available_at"),
    )
    stop_type = _extract_scalar(bags, [("stop_type",)])
    stop_window = _extract_scalar(bags, [("stop_window",)])
    stop_price_type = _extract_scalar(bags, [("stop_price_type",)])
    stop_friction_model_version = _extract_scalar(bags, [("stop_friction_model_version",)])
    if not version and stop_threshold_pct is None and stop_type is None:
        return None
    missing_fields = []
    if not version:
        missing_fields.append("stop_contract_version")
    if not stop_type:
        missing_fields.append("stop_type")
    if stop_threshold_pct is None or not math.isfinite(stop_threshold_pct) or stop_threshold_pct >= 0:
        missing_fields.append("stop_threshold_pct")
    if not stop_window:
        missing_fields.append("stop_window")
    if not stop_price_type:
        missing_fields.append("stop_price_type")
    if stop_executable_required is None:
        missing_fields.append("stop_executable_required")
    elif _as_bool(stop_executable_required) is not True:
        missing_fields.append("stop_executable_required_true")
    if not stop_friction_model_version:
        missing_fields.append("stop_friction_model_version")
    if stop_available_at is None:
        missing_fields.append("stop_available_at")
    return {
        "stop_contract_version": version,
        "stop_type": stop_type,
        "stop_threshold_pct": stop_threshold_pct,
        "stop_window": stop_window,
        "stop_price_type": stop_price_type,
        "stop_executable_required": _as_bool(stop_executable_required),
        "stop_friction_model_version": stop_friction_model_version,
        "stop_available_at": stop_available_at,
        "standardized_stop_quality": _extract_scalar(bags, [("standardized_stop_quality",)], default="legacy_seed"),
        "paper_trade_id": _extract_scalar(bags, [("paper_trade_id",), ("id",)]),
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
        "missing_fields": missing_fields,
    }


def _extract_ex_ante_feasibility_contract(event, bags):
    version = _extract_scalar(bags, [("feasibility_policy_version",)])
    if event.get("event_type") != EX_ANTE_FEASIBILITY_EVENT_TYPE and not version:
        return None
    ex_ante_feasible = _extract_flag(bags, [("ex_ante_feasible",)])
    feasibility_class = _extract_scalar(bags, [("feasibility_class",)])
    decision_ts = _extract_scalar(bags, [("decision_ts",), ("decision_available_at",), ("counterfactual_entry_ts",), ("entry_ts")])
    forbidden_future_fields_used = _extract_scalar(bags, [("forbidden_future_fields_used",)], default=[])
    if forbidden_future_fields_used is None:
        forbidden_future_fields_used = []
    if not isinstance(forbidden_future_fields_used, list):
        forbidden_future_fields_used = [forbidden_future_fields_used]
    used_future_peak = _extract_flag(bags, [("used_future_peak",)])
    used_future_outcome = _extract_flag(bags, [("used_future_outcome",)])
    used_posthoc_label = _extract_flag(bags, [("used_posthoc_label",)])
    used_future_peak = False if used_future_peak is None else used_future_peak
    used_future_outcome = False if used_future_outcome is None else used_future_outcome
    used_posthoc_label = False if used_posthoc_label is None else used_posthoc_label
    if not version and ex_ante_feasible is None and feasibility_class is None:
        return None
    missing_fields = []
    leakage_fields = []
    if ex_ante_feasible is None:
        missing_fields.append("ex_ante_feasible")
    if not feasibility_class:
        missing_fields.append("feasibility_class")
    if not version:
        missing_fields.append("feasibility_policy_version")
    if decision_ts is None:
        missing_fields.append("decision_ts")
    if used_future_peak:
        leakage_fields.append("used_future_peak")
    if used_future_outcome:
        leakage_fields.append("used_future_outcome")
    if used_posthoc_label:
        leakage_fields.append("used_posthoc_label")
    leakage_fields.extend(str(field) for field in forbidden_future_fields_used if field)
    return {
        "ex_ante_feasible": ex_ante_feasible,
        "feasibility_class": feasibility_class,
        "feasibility_policy_version": version,
        "decision_ts": decision_ts,
        "decision_available_at": _extract_scalar(bags, [("decision_available_at",)], default=decision_ts),
        "counterfactual_entry_ts": _extract_scalar(bags, [("counterfactual_entry_ts",), ("entry_ts",)]),
        "system_min_decision_latency_sec": _as_float(_extract_scalar(bags, [("system_min_decision_latency_sec",)])),
        "system_min_entry_latency_sec": _as_float(_extract_scalar(bags, [("system_min_entry_latency_sec",)])),
        "entry_delay_from_signal_sec": _as_float(_extract_scalar(bags, [("entry_delay_from_signal_sec",)])),
        "entry_quote_available": _extract_flag(bags, [("entry_quote_available",)]),
        "entry_quote_available_at": _extract_scalar(bags, [("entry_quote_available_at",)]),
        "current_quote_availability": _extract_flag(bags, [("current_quote_availability",)]),
        "current_pool_resolution": _extract_scalar(bags, [("current_pool_resolution",), ("canonical_pool_group",)]),
        "current_provider_health": _extract_scalar(bags, [("current_provider_health",)]),
        "current_risk_availability": _extract_scalar(bags, [("current_risk_availability",)]),
        "current_queue_delay_sec": _as_float(_extract_scalar(bags, [("current_queue_delay_sec",)])),
        "feature_max_available_at": _extract_scalar(bags, [("feature_max_available_at",)]),
        "used_future_peak": bool(used_future_peak),
        "used_future_outcome": bool(used_future_outcome),
        "used_posthoc_label": bool(used_posthoc_label),
        "forbidden_future_fields_used": forbidden_future_fields_used,
        "paper_trade_id": _extract_scalar(bags, [("paper_trade_id",), ("id",)]),
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
        "missing_fields": missing_fields,
        "leakage_fields": sorted(set(leakage_fields)),
    }


LEGACY_SOURCE_REFERENCE_PRICE_TYPES = {"legacy_entry_price", "legacy_baseline_price"}


def _reference_price_alias_group(price_type):
    if price_type in LEGACY_SOURCE_REFERENCE_PRICE_TYPES:
        return "legacy_source_reference_price"
    return None


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
        "seed_event_type": event.get("event_type"),
        "observed_at": event.get("observed_at"),
        "ingested_at": event.get("ingested_at"),
        "available_at": event.get("available_at"),
        "decision_event_id": outer.get("decision_event_id"),
        "telegram_signal_id": outer.get("telegram_signal_id") or _extract_scalar(bags, [("telegram_signal_id",), ("signal", "telegram_signal_id")]),
        "remote_signal_id": outer.get("remote_signal_id") or _extract_scalar(bags, [("remote_signal_id",)]),
        "signal_id": outer.get("signal_id") or _extract_scalar(bags, [("signal_id",)]),
        "signal_type": outer.get("signal_type") or _extract_scalar(bags, [("signal_type",)]),
        "source_message_ts": outer.get("source_message_ts") or _extract_scalar(bags, [("source_message_ts",), ("message_ts",)]),
        "receive_ts": outer.get("receive_ts") or _extract_scalar(bags, [("receive_ts",)]),
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
        "reference_price_contract": _extract_reference_price_contract(event, bags),
        "trade_outcome_label_contract": _extract_trade_outcome_label_contract(event, bags),
        "standardized_stop_contract": _extract_standardized_stop_contract(event, bags),
        "ex_ante_feasibility_contract": _extract_ex_ante_feasibility_contract(event, bags),
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
        "telegram_signal_stack": [],
        "decision_signal_ids": [],
        "routes": [],
        "components": [],
        "dedup_reason": "same chain/token/canonical_pool_group/lifecycle_epoch",
        "primary_outcome_role": "source_dog_label" if fact.get("source_dog_label") else "unlabeled_decision_seed",
        "source_label_conflicts": [],
        "reference_price_candidates": [],
        "reference_price_conflicts": [],
        "reference_price_ignored_late_candidates": [],
        "reference_price_compatible_alias_candidates": [],
        "trade_outcome_label_candidates": [],
        "standardized_stop_candidates": [],
        "ex_ante_feasibility_candidates": [],
        "source_label_research_only": False,
        "denominator_dirty_reasons": [],
        "source_dog_label": None,
        "signal_credit_assignment": None,
        "reference_price_contract": None,
        "trade_outcome_label": None,
        "standardized_stop_contract": None,
        "ex_ante_feasibility_contract": None,
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
    if fact.get("seed_event_type") == TELEGRAM_SIGNAL_EVENT_TYPE and fact.get("telegram_signal_id") is not None:
        signal_entry = {
            "telegram_signal_id": fact.get("telegram_signal_id"),
            "remote_signal_id": fact.get("remote_signal_id"),
            "signal_type": fact.get("signal_type"),
            "event_id": fact.get("event_id"),
            "global_seq": fact.get("global_seq"),
            "available_at": fact.get("available_at"),
            "source_message_ts": fact.get("source_message_ts"),
            "receive_ts": fact.get("receive_ts"),
        }
        if signal_entry not in record["telegram_signal_stack"]:
            record["telegram_signal_stack"].append(signal_entry)
    if fact.get("signal_id") is not None and fact.get("signal_id") not in record["decision_signal_ids"]:
        record["decision_signal_ids"].append(fact.get("signal_id"))
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

    reference_price = fact.get("reference_price_contract")
    if reference_price:
        record["reference_price_candidates"].append(reference_price)
    trade_outcome_label = fact.get("trade_outcome_label_contract")
    if trade_outcome_label:
        record["trade_outcome_label_candidates"].append(trade_outcome_label)
    standardized_stop = fact.get("standardized_stop_contract")
    if standardized_stop:
        record["standardized_stop_candidates"].append(standardized_stop)
    ex_ante_feasibility = fact.get("ex_ante_feasibility_contract")
    if ex_ante_feasibility:
        record["ex_ante_feasibility_candidates"].append(ex_ante_feasibility)

    for flag in DENOMINATOR_FLAGS:
        record[flag] = _merge_bool(record.get(flag), fact.get(flag))


def _finalize_signal_credit(record):
    stack = sorted(
        record.get("telegram_signal_stack") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("event_id") or "")),
    )
    record["telegram_signal_stack"] = stack
    if not stack:
        legacy_signal_ids = [signal_id for signal_id in record.get("decision_signal_ids") or [] if signal_id is not None]
        if not legacy_signal_ids:
            record["signal_credit_assignment"] = None
            return
        credited_signal_id = sorted(legacy_signal_ids, key=lambda value: str(value))[0]
        record["signal_credit_assignment"] = {
            "credited_signal_id": credited_signal_id,
            "credited_signal_event_id": None,
            "credit_assignment_reason": "legacy_embedded_signal_anchor",
            "credit_assignment_quality": "shadow_legacy_embedded",
            "signal_stack_before_entry": [],
            "signal_stack_after_entry": [],
            "credit_policy_version": "v2.7.0.signal_credit.v1",
        }
        return
    credited = stack[0]
    record["signal_credit_assignment"] = {
        "credited_signal_id": credited.get("telegram_signal_id"),
        "credited_signal_event_id": credited.get("event_id"),
        "credit_assignment_reason": "first_valid_telegram_anchor",
        "credit_assignment_quality": "telegram_lifecycle_event",
        "signal_stack_before_entry": stack,
        "signal_stack_after_entry": [],
        "credit_policy_version": "v2.7.0.signal_credit.v1",
    }


def _finalize_reference_price(record):
    candidates = sorted(
        record.get("reference_price_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["reference_price_candidates"] = candidates
    if not candidates:
        record["reference_price_contract"] = None
        return
    selected = candidates[0]
    for candidate in candidates[1:]:
        selected_type = selected.get("reference_price_type")
        candidate_type = candidate.get("reference_price_type")
        selected_alias_group = _reference_price_alias_group(selected_type)
        candidate_alias_group = _reference_price_alias_group(candidate_type)
        if (
            selected_type != candidate_type
            and selected_alias_group
            and selected_alias_group == candidate_alias_group
        ):
            record["reference_price_compatible_alias_candidates"].append(
                {
                    "selected_source_event_id": selected.get("source_event_id"),
                    "incoming_source_event_id": candidate.get("source_event_id"),
                    "selected_type": selected_type,
                    "incoming_type": candidate_type,
                    "selected_price": selected.get("reference_price"),
                    "incoming_price": candidate.get("reference_price"),
                    "compatible_alias_group": selected_alias_group,
                    "ignore_reason": "legacy_source_reference_alias_does_not_reset_reference_price",
                }
            )
        elif candidate.get("reference_price_type") != selected.get("reference_price_type"):
            record["reference_price_conflicts"].append(
                {
                    "selected_source_event_id": selected.get("source_event_id"),
                    "incoming_source_event_id": candidate.get("source_event_id"),
                    "selected_type": selected_type,
                    "incoming_type": candidate_type,
                }
            )
        elif candidate.get("reference_price") != selected.get("reference_price"):
            record["reference_price_ignored_late_candidates"].append(
                {
                    "selected_source_event_id": selected.get("source_event_id"),
                    "incoming_source_event_id": candidate.get("source_event_id"),
                    "reference_price_type": selected.get("reference_price_type"),
                    "selected_price": selected.get("reference_price"),
                    "incoming_price": candidate.get("reference_price"),
                    "ignore_reason": "same_type_late_candidate_does_not_reset_reference_price",
                }
            )
    record["reference_price_contract"] = {
        "reference_price_type": selected.get("reference_price_type"),
        "reference_price": selected.get("reference_price"),
        "reference_price_ts": selected.get("reference_price_ts"),
        "reference_quote_source": selected.get("reference_quote_source"),
        "reference_quote_age_sec": selected.get("reference_quote_age_sec"),
        "reference_price_available_at": selected.get("reference_price_available_at"),
        "reference_price_quality": selected.get("reference_price_quality"),
        "reference_price_source_event_id": selected.get("source_event_id"),
        "reference_price_contract_version": "v2.7.0.reference_price.v1",
    }


def _finalize_trade_outcome_label(record):
    candidates = sorted(
        record.get("trade_outcome_label_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["trade_outcome_label_candidates"] = candidates
    record["trade_outcome_label"] = candidates[0] if candidates else None


def _finalize_standardized_stop(record):
    candidates = sorted(
        record.get("standardized_stop_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["standardized_stop_candidates"] = candidates
    record["standardized_stop_contract"] = candidates[0] if candidates else None


def _finalize_ex_ante_feasibility(record):
    candidates = sorted(
        record.get("ex_ante_feasibility_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["ex_ante_feasibility_candidates"] = candidates
    record["ex_ante_feasibility_contract"] = candidates[0] if candidates else None
    if not candidates:
        return
    selected = candidates[0]
    valid = not selected.get("missing_fields") and not selected.get("leakage_fields")
    record["ex_ante_feasible"] = bool(valid and selected.get("ex_ante_feasible") is True)


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
    membership = record.get("denominator_membership") or {}
    if membership.get("D0_telegram_gold_silver_total"):
        if not record.get("signal_credit_assignment"):
            missing.append("SignalCreditAssignmentContract")
        if not record.get("reference_price_contract"):
            missing.append("ReferencePriceContract")
    if record.get("captured") and not record.get("trade_outcome_label"):
        missing.append("TradeOutcomeLabelContract")
    if record.get("captured") and not record.get("standardized_stop_contract"):
        missing.append("StandardizedStopContract")
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


def _metric_definitions(metrics, metrics_window):
    definitions = {}
    window_id = metrics_window.get("window_id")
    for metric_name, value in sorted(metrics.items()):
        definitions[metric_name] = {
            "metric_id": f"telegram_dog.{metric_name}",
            "metric_name": metric_name,
            "window_id": window_id,
            "window_start": metrics_window.get("window_start"),
            "window_end": metrics_window.get("window_end"),
            "metric_version": "v2.7.0.denominator_metrics.v1",
            "value": value,
        }
    return definitions


def _contract_evidence_from_records(record_list):
    d0_records = [record for record in record_list if record.get("denominator_membership", {}).get("D0_telegram_gold_silver_total")]
    signal_credit_missing = [
        record.get("denominator_dedup_key")
        for record in d0_records
        if not record.get("signal_credit_assignment")
    ]
    reference_price_missing = [
        record.get("denominator_dedup_key")
        for record in d0_records
        if not record.get("reference_price_contract")
    ]
    reference_price_conflicts = [
        {
            "denominator_dedup_key": record.get("denominator_dedup_key"),
            "conflicts": record.get("reference_price_conflicts"),
        }
        for record in d0_records
        if record.get("reference_price_conflicts")
    ]
    ignored_late_reference_price_candidates = [
        {
            "denominator_dedup_key": record.get("denominator_dedup_key"),
            "ignored_late_candidate_count": len(record.get("reference_price_ignored_late_candidates") or []),
        }
        for record in d0_records
        if record.get("reference_price_ignored_late_candidates")
    ]
    compatible_alias_reference_price_candidates = [
        {
            "denominator_dedup_key": record.get("denominator_dedup_key"),
            "compatible_alias_candidate_count": len(record.get("reference_price_compatible_alias_candidates") or []),
        }
        for record in d0_records
        if record.get("reference_price_compatible_alias_candidates")
    ]
    legacy_embedded_signal_credit = [
        record.get("denominator_dedup_key")
        for record in d0_records
        if (record.get("signal_credit_assignment") or {}).get("credit_assignment_quality") == "shadow_legacy_embedded"
    ]
    trade_label_records = [
        record
        for record in record_list
        if record.get("trade_outcome_label_candidates")
    ]
    malformed_trade_labels = []
    for record in trade_label_records:
        malformed = [
            label
            for label in record.get("trade_outcome_label_candidates") or []
            if label.get("missing_fields")
        ]
        if malformed:
            malformed_trade_labels.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for label in malformed for field in label.get("missing_fields", [])}),
                }
            )
    standardized_stop_records = [
        record
        for record in record_list
        if record.get("standardized_stop_candidates")
    ]
    malformed_standardized_stops = []
    for record in standardized_stop_records:
        malformed = [
            stop
            for stop in record.get("standardized_stop_candidates") or []
            if stop.get("missing_fields")
        ]
        if malformed:
            malformed_standardized_stops.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for stop in malformed for field in stop.get("missing_fields", [])}),
                }
            )
    ex_ante_records = [
        record
        for record in record_list
        if record.get("ex_ante_feasibility_candidates")
    ]
    malformed_ex_ante = []
    future_leakage_ex_ante = []
    for record in ex_ante_records:
        malformed = [
            feasibility
            for feasibility in record.get("ex_ante_feasibility_candidates") or []
            if feasibility.get("missing_fields")
        ]
        if malformed:
            malformed_ex_ante.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for item in malformed for field in item.get("missing_fields", [])}),
                }
            )
        leaky = [
            feasibility
            for feasibility in record.get("ex_ante_feasibility_candidates") or []
            if feasibility.get("leakage_fields")
        ]
        if leaky:
            future_leakage_ex_ante.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "future_leakage_count": len(leaky),
                    "leakage_fields": sorted({field for item in leaky for field in item.get("leakage_fields", [])}),
                }
            )
    return {
        "SignalCreditAssignmentContract": {
            "eligible_d0_records": len(d0_records),
            "missing_count": len(signal_credit_missing),
            "missing_denominator_keys": signal_credit_missing,
            "legacy_embedded_credit_count": len(legacy_embedded_signal_credit),
            "legacy_embedded_denominator_keys": legacy_embedded_signal_credit,
            "credit_policy_version": "v2.7.0.signal_credit.v1",
        },
        "ReferencePriceContract": {
            "eligible_d0_records": len(d0_records),
            "missing_count": len(reference_price_missing),
            "missing_denominator_keys": reference_price_missing,
            "conflict_count": len(reference_price_conflicts),
            "conflicts": reference_price_conflicts,
            "ignored_late_candidate_count": sum(item["ignored_late_candidate_count"] for item in ignored_late_reference_price_candidates),
            "ignored_late_candidates": ignored_late_reference_price_candidates,
            "compatible_alias_candidate_count": sum(item["compatible_alias_candidate_count"] for item in compatible_alias_reference_price_candidates),
            "compatible_alias_candidates": compatible_alias_reference_price_candidates,
            "reference_price_contract_version": "v2.7.0.reference_price.v1",
        },
        "TradeOutcomeLabelContract": {
            "eligible_trade_outcome_records": len(trade_label_records),
            "trade_outcome_label_count": sum(len(record.get("trade_outcome_label_candidates") or []) for record in trade_label_records),
            "malformed_count": sum(item["malformed_count"] for item in malformed_trade_labels),
            "malformed_labels": malformed_trade_labels,
            "trade_outcome_label_version": "legacy_paper_trade_outcome_v0.1",
        },
        "StandardizedStopContract": {
            "eligible_standardized_stop_records": len(standardized_stop_records),
            "standardized_stop_contract_count": sum(len(record.get("standardized_stop_candidates") or []) for record in standardized_stop_records),
            "malformed_count": sum(item["malformed_count"] for item in malformed_standardized_stops),
            "malformed_stops": malformed_standardized_stops,
            "stop_contract_versions": sorted(
                {
                    stop.get("stop_contract_version")
                    for record in standardized_stop_records
                    for stop in record.get("standardized_stop_candidates") or []
                    if stop.get("stop_contract_version")
                }
            ),
            "stop_contract_projection_version": "v2.7.0.standardized_stop.v1",
        },
        "ExAnteFeasibility": {
            "eligible_ex_ante_records": len(ex_ante_records),
            "ex_ante_feasibility_count": sum(len(record.get("ex_ante_feasibility_candidates") or []) for record in ex_ante_records),
            "ex_ante_feasible_count": sum(1 for record in ex_ante_records if record.get("ex_ante_feasible") is True),
            "malformed_count": sum(item["malformed_count"] for item in malformed_ex_ante),
            "malformed_feasibility": malformed_ex_ante,
            "future_leakage_count": sum(item["future_leakage_count"] for item in future_leakage_ex_ante),
            "future_leakage": future_leakage_ex_ante,
            "feasibility_policy_versions": sorted(
                {
                    feasibility.get("feasibility_policy_version")
                    for record in ex_ante_records
                    for feasibility in record.get("ex_ante_feasibility_candidates") or []
                    if feasibility.get("feasibility_policy_version")
                }
            ),
            "ex_ante_feasibility_projection_version": "v2.7.0.ex_ante_feasibility.v1",
        },
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
        "event_log_latest_seq": 0,
        "event_log_latest_at": None,
        "event_log_latest_event_id": None,
        "event_log_error": None,
        "input_events": 0,
        "telegram_signal_seen_events": 0,
        "source_dog_label_events": 0,
        "lifecycle_identity_events": 0,
        "trade_outcome_label_recorded_events": 0,
        "standardized_stop_contract_recorded_events": 0,
        "ex_ante_feasibility_recorded_events": 0,
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
        "metrics_window": None,
        "metric_definitions": {},
        "metric_definitions_hash": None,
        "contract_evidence": {
            "SignalCreditAssignmentContract": {},
            "ReferencePriceContract": {},
            "MetricsWindowContract": {},
            "TradeOutcomeLabelContract": {},
            "StandardizedStopContract": {},
            "ExAnteFeasibility": {},
        },
        "evidence_gaps": {},
        "health": {
            "event_log_ok": False,
            "projection_built": False,
            "denominator_clean": False,
            "signal_credit_assignment_ok": False,
            "reference_price_ok": False,
            "metrics_window_ok": False,
            "trade_outcome_label_ok": False,
            "standardized_stop_ok": False,
            "ex_ante_feasibility_ok": False,
            "normal_tiny_ready": False,
            "status": "not_built",
        },
        "records_hash": None,
    }

    try:
        projection["event_log_verify"] = event_log.verify()
        projection["event_log_latest_seq"] = projection["event_log_verify"]["last_global_seq"]
        projection["health"]["event_log_ok"] = True
    except V27EventLogError as exc:
        projection["event_log_error"] = str(exc)
        projection["health"]["status"] = "event_log_invalid"
        return projection

    facts = []
    resolved_pool_by_identity = {}
    window_start = None
    window_end = None
    for event in event_log.iter_events() or []:
        projection["input_events"] += 1
        projection["event_log_latest_event_id"] = event.get("event_id")
        projection["event_log_latest_at"] = event.get("ingested_at")
        event_window_ts = event.get("available_at") or event.get("ingested_at") or event.get("observed_at")
        window_start = _earlier_iso(window_start, event_window_ts)
        window_end = _later_iso(window_end, event_window_ts)
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
        if event.get("event_type") == LIFECYCLE_IDENTITY_EVENT_TYPE:
            projection["lifecycle_identity_events"] += 1
        if event.get("event_type") == TRADE_OUTCOME_LABEL_EVENT_TYPE:
            projection["trade_outcome_label_recorded_events"] += 1
        if event.get("event_type") == STANDARDIZED_STOP_EVENT_TYPE:
            projection["standardized_stop_contract_recorded_events"] += 1
        if event.get("event_type") == EX_ANTE_FEASIBILITY_EVENT_TYPE:
            projection["ex_ante_feasibility_recorded_events"] += 1
        fact = _extract_decision_fact(event)
        fact["seed_event_type"] = event.get("event_type")
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
        identity = (fact.get("chain"), fact.get("token_ca"), fact.get("lifecycle_epoch", 0))
        pool = fact.get("canonical_pool_group")
        if event.get("event_type") == LIFECYCLE_IDENTITY_EVENT_TYPE and pool and pool != "unknown_pool":
            resolved_pool_by_identity.setdefault(identity, pool)
        facts.append(fact)

    records = {}
    for fact in facts:
        identity = (fact.get("chain"), fact.get("token_ca"), fact.get("lifecycle_epoch", 0))
        if fact.get("canonical_pool_group") == "unknown_pool" and identity in resolved_pool_by_identity:
            fact["canonical_pool_group"] = resolved_pool_by_identity[identity]
            fact["denominator_dedup_key"] = _denominator_key(fact)
        key = fact["denominator_dedup_key"]
        if key not in records:
            records[key] = _new_record(fact)
        _merge_fact(records[key], fact)

    record_list = []
    for key in sorted(records):
        record = records[key]
        _finalize_signal_credit(record)
        _finalize_reference_price(record)
        _finalize_trade_outcome_label(record)
        _finalize_standardized_stop(record)
        _finalize_ex_ante_feasibility(record)
        _record_denominator_membership(record)
        missing = _record_missing_evidence(record)
        for contract in missing:
            projection["evidence_gaps"][contract] = projection["evidence_gaps"].get(contract, 0) + 1
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

    metrics_window = {
        "metric_window_schema_version": "v2.7.0.metrics_window.v1",
        "metric_id": "telegram_dog.denominator_projection",
        "window_id": f"event_log_seq_1_{projection['event_log_latest_seq']}",
        "window_start": window_start,
        "window_end": window_end,
        "window_start_seq": 1 if projection["event_log_latest_seq"] else None,
        "window_end_seq": projection["event_log_latest_seq"],
        "metric_version": "v2.7.0.denominator_metrics.v1",
    }
    projection["metrics_window"] = metrics_window
    projection["metric_definitions"] = _metric_definitions(metrics, metrics_window)
    projection["metric_definitions_hash"] = sha256_hex(projection["metric_definitions"])
    contract_evidence = _contract_evidence_from_records(record_list)
    metrics_window_valid = bool(
        metrics_window.get("window_id")
        and metrics_window.get("window_start")
        and metrics_window.get("window_end")
        and metrics_window.get("metric_version")
        and projection.get("metric_definitions_hash")
    )
    contract_evidence["MetricsWindowContract"] = {
        "metric_id": metrics_window.get("metric_id"),
        "window_id": metrics_window.get("window_id"),
        "window_start": metrics_window.get("window_start"),
        "window_end": metrics_window.get("window_end"),
        "metric_version": metrics_window.get("metric_version"),
        "metric_count": len(projection["metric_definitions"]),
        "metric_definitions_hash": projection.get("metric_definitions_hash"),
        "metrics_window_valid": metrics_window_valid,
    }
    projection["contract_evidence"] = contract_evidence
    projection["records_hash"] = sha256_hex(record_list)
    projection["health"]["projection_built"] = True
    projection["health"]["denominator_clean"] = not projection["dirty_events"] and not projection["dirty_records"]
    projection["health"]["signal_credit_assignment_ok"] = (
        contract_evidence["SignalCreditAssignmentContract"]["eligible_d0_records"] > 0
        and contract_evidence["SignalCreditAssignmentContract"]["missing_count"] == 0
    )
    projection["health"]["reference_price_ok"] = (
        contract_evidence["ReferencePriceContract"]["eligible_d0_records"] > 0
        and contract_evidence["ReferencePriceContract"]["missing_count"] == 0
        and contract_evidence["ReferencePriceContract"]["conflict_count"] == 0
    )
    projection["health"]["metrics_window_ok"] = metrics_window_valid
    projection["health"]["trade_outcome_label_ok"] = (
        contract_evidence["TradeOutcomeLabelContract"]["eligible_trade_outcome_records"] > 0
        and contract_evidence["TradeOutcomeLabelContract"]["malformed_count"] == 0
    )
    projection["health"]["standardized_stop_ok"] = (
        contract_evidence["StandardizedStopContract"]["eligible_standardized_stop_records"] > 0
        and contract_evidence["StandardizedStopContract"]["malformed_count"] == 0
    )
    projection["health"]["ex_ante_feasibility_ok"] = (
        contract_evidence["ExAnteFeasibility"]["eligible_ex_ante_records"] > 0
        and contract_evidence["ExAnteFeasibility"]["ex_ante_feasible_count"] > 0
        and contract_evidence["ExAnteFeasibility"]["malformed_count"] == 0
        and contract_evidence["ExAnteFeasibility"]["future_leakage_count"] == 0
    )
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


def build_denominator_read_model_snapshot(
    projection,
    *,
    max_allowed_lag_seq=0,
    max_allowed_lag_ms=300_000,
    read_model_seq=None,
    now_iso=None,
    spec_manifest_path=DEFAULT_SPEC_MANIFEST,
):
    """Wrap a projection with freshness, spec, and stable hash metadata."""

    now_iso = now_iso or _utc_now_iso()
    latest_seq = int(projection.get("event_log_latest_seq") or _nested_get(projection, ("event_log_verify", "last_global_seq"), default=0) or 0)
    read_model_seq = latest_seq if read_model_seq is None else int(read_model_seq)
    lag_seq = max(0, latest_seq - read_model_seq)
    lag_ms = _lag_ms(projection.get("event_log_latest_at"), now_iso)

    stale_reasons = []
    if lag_seq > max_allowed_lag_seq:
        stale_reasons.append("read_model_seq_lag")
    if lag_ms is not None and lag_ms > max_allowed_lag_ms:
        stale_reasons.append("read_model_time_lag")

    projection_hash_payload = {key: value for key, value in projection.items() if key != "event_log_dir"}
    projection_hash = sha256_hex(projection_hash_payload)
    spec_metadata = _load_spec_metadata(spec_manifest_path)

    snapshot = {
        "snapshot_schema_version": "v2.7.0.denominator_read_model.v1",
        "snapshot_id": "v27denom_"
        + sha256_hex(
            {
                "projection_hash": projection_hash,
                "read_model_seq": read_model_seq,
                "spec_hash": spec_metadata.get("spec_hash"),
            }
        )[:16],
        "generated_at": now_iso,
        "projection_name": projection.get("projection_name"),
        "projection_version": projection.get("projection_version"),
        "projection_hash": projection_hash,
        "spec": spec_metadata,
        "read_model": {
            "read_model_seq": read_model_seq,
            "event_log_latest_seq": latest_seq,
            "event_log_latest_at": projection.get("event_log_latest_at"),
            "read_model_updated_at": now_iso,
            "max_allowed_lag_seq": max_allowed_lag_seq,
            "max_allowed_lag_ms": max_allowed_lag_ms,
            "lag_seq": lag_seq,
            "lag_ms": lag_ms,
            "read_model_fresh_enough": not stale_reasons,
            "staleness_reasons": stale_reasons,
        },
        "health": {
            "projection_built": bool(_nested_get(projection, ("health", "projection_built"), default=False)),
            "event_log_ok": bool(_nested_get(projection, ("health", "event_log_ok"), default=False)),
            "denominator_clean": bool(_nested_get(projection, ("health", "denominator_clean"), default=False)),
            "read_model_fresh_enough": not stale_reasons,
            "spec_valid": bool(spec_metadata.get("spec_valid")),
            "normal_tiny_ready": False,
        },
        "projection": projection,
    }
    snapshot["health"]["status"] = (
        "snapshot_ready"
        if snapshot["health"]["projection_built"]
        and snapshot["health"]["event_log_ok"]
        and snapshot["health"]["denominator_clean"]
        and snapshot["health"]["read_model_fresh_enough"]
        and snapshot["health"]["spec_valid"]
        else "snapshot_not_ready"
    )
    snapshot["snapshot_hash"] = sha256_hex(snapshot)
    return snapshot


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--output")
    parser.add_argument("--snapshot-output")
    parser.add_argument("--include-records", action="store_true")
    parser.add_argument("--max-allowed-lag-seq", type=int, default=0)
    parser.add_argument("--max-allowed-lag-ms", type=int, default=300_000)
    parser.add_argument("--read-model-seq", type=int)
    parser.add_argument("--spec-manifest", default=str(DEFAULT_SPEC_MANIFEST))
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args()

    projection = build_denominator_projection(args.event_log_dir, include_records=args.include_records)
    snapshot = build_denominator_read_model_snapshot(
        projection,
        max_allowed_lag_seq=args.max_allowed_lag_seq,
        max_allowed_lag_ms=args.max_allowed_lag_ms,
        read_model_seq=args.read_model_seq,
        spec_manifest_path=args.spec_manifest,
    )
    rendered = json.dumps(projection, ensure_ascii=False, sort_keys=True, indent=2)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
    if args.snapshot_output:
        snapshot_output_path = Path(args.snapshot_output)
        snapshot_output_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_output_path.write_text(json.dumps(snapshot, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(rendered)
    if args.strict and (
        projection.get("event_log_error")
        or not projection["health"].get("denominator_clean")
        or not snapshot["read_model"].get("read_model_fresh_enough")
        or not snapshot["spec"].get("spec_valid")
    ):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
