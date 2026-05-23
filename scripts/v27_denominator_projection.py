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
EARLIEST_ACTIONABLE_EVENT_TYPE = "earliest_actionable_time_recorded"
REALTIME_CLEAN_EVENT_TYPE = "realtime_clean_detector_recorded"
QUOTE_INTENT_BINDING_EVENT_TYPE = "quote_intent_binding_recorded"
IDEMPOTENCY_EVENT_TYPE = "idempotency_contract_recorded"
EXECUTION_CONTROL_EVENT_TYPE = "execution_control_recorded"
PAPER_LEDGER_EVENT_TYPE = "paper_ledger_recorded"
NO_FILL_OUTCOME_EVENT_TYPE = "no_fill_outcome_recorded"
RUNTIME_RECOVERY_EVENT_TYPE = "runtime_recovery_control_recorded"
DENOMINATOR_SEED_EVENT_TYPES = {
    MIRRORED_DECISION_EVENT_TYPE,
    MIRRORED_MISSED_EVENT_TYPE,
    TELEGRAM_SIGNAL_EVENT_TYPE,
    SOURCE_DOG_LABEL_EVENT_TYPE,
    LIFECYCLE_IDENTITY_EVENT_TYPE,
    TRADE_OUTCOME_LABEL_EVENT_TYPE,
    STANDARDIZED_STOP_EVENT_TYPE,
    EX_ANTE_FEASIBILITY_EVENT_TYPE,
    EARLIEST_ACTIONABLE_EVENT_TYPE,
    REALTIME_CLEAN_EVENT_TYPE,
    QUOTE_INTENT_BINDING_EVENT_TYPE,
    IDEMPOTENCY_EVENT_TYPE,
    EXECUTION_CONTROL_EVENT_TYPE,
    PAPER_LEDGER_EVENT_TYPE,
    NO_FILL_OUTCOME_EVENT_TYPE,
    RUNTIME_RECOVERY_EVENT_TYPE,
}
SOURCE_REFERENCE_PRICE_EVENT_TYPES = {
    MIRRORED_DECISION_EVENT_TYPE,
    MIRRORED_MISSED_EVENT_TYPE,
    TELEGRAM_SIGNAL_EVENT_TYPE,
    SOURCE_DOG_LABEL_EVENT_TYPE,
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


def _timestamp_epoch_seconds(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        return ts / 1000.0 if ts > 1_000_000_000_000 else ts
    text = str(value).strip()
    if not text:
        return None
    try:
        ts = float(text)
        return ts / 1000.0 if ts > 1_000_000_000_000 else ts
    except ValueError:
        pass
    parsed = _parse_iso_ts(text)
    if parsed is None:
        return None
    return parsed.timestamp()


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
    if event.get("event_type") not in SOURCE_REFERENCE_PRICE_EVENT_TYPES:
        return None
    explicit_price = _as_float(
        _extract_scalar(
            bags,
            [
                ("source_reference_price",),
                ("reference_price",),
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


def _extract_earliest_actionable_time_contract(event, bags):
    version = _extract_scalar(bags, [("earliest_actionable_policy_version",), ("actionable_time_policy_version",)])
    if event.get("event_type") != EARLIEST_ACTIONABLE_EVENT_TYPE and not version:
        return None
    earliest_actionable_ts = _extract_scalar(bags, [("earliest_actionable_ts",)])
    required_inputs_available_at = _extract_scalar(bags, [("required_inputs_available_at",)], default={})
    peak_ts = _extract_scalar(bags, [("peak_ts",)])
    counterfactual_entry_ts = _extract_scalar(bags, [("counterfactual_entry_ts",), ("entry_ts",)])
    actionable_before_peak = _extract_flag(bags, [("actionable_before_peak",)])
    reason = _extract_scalar(bags, [("earliest_actionable_reason",)])
    if not version and earliest_actionable_ts is None and peak_ts is None and actionable_before_peak is None:
        return None
    if required_inputs_available_at is None:
        required_inputs_available_at = {}
    missing_inputs_before_ts = _extract_scalar(bags, [("missing_inputs_before_ts",)], default=[])
    if missing_inputs_before_ts is None:
        missing_inputs_before_ts = []
    if not isinstance(missing_inputs_before_ts, list):
        missing_inputs_before_ts = [missing_inputs_before_ts]
    missing_fields = []
    invariant_violations = []
    if not version:
        missing_fields.append("earliest_actionable_policy_version")
    if earliest_actionable_ts is None:
        missing_fields.append("earliest_actionable_ts")
    if not isinstance(required_inputs_available_at, dict) or not required_inputs_available_at:
        missing_fields.append("required_inputs_available_at")
    if peak_ts is None:
        missing_fields.append("peak_ts")
    if counterfactual_entry_ts is None:
        missing_fields.append("counterfactual_entry_ts")
    if actionable_before_peak is None:
        missing_fields.append("actionable_before_peak")
    if not reason:
        missing_fields.append("earliest_actionable_reason")
    earliest_sec = _timestamp_epoch_seconds(earliest_actionable_ts)
    entry_sec = _timestamp_epoch_seconds(counterfactual_entry_ts)
    peak_sec = _timestamp_epoch_seconds(peak_ts)
    if earliest_actionable_ts is not None and earliest_sec is None:
        missing_fields.append("earliest_actionable_ts_parseable")
    if counterfactual_entry_ts is not None and entry_sec is None:
        missing_fields.append("counterfactual_entry_ts_parseable")
    if peak_ts is not None and peak_sec is None:
        missing_fields.append("peak_ts_parseable")
    if earliest_sec is not None and entry_sec is not None and earliest_sec > entry_sec:
        invariant_violations.append("earliest_actionable_after_counterfactual_entry")
    if entry_sec is not None and peak_sec is not None and entry_sec > peak_sec:
        invariant_violations.append("counterfactual_entry_after_peak")
    if actionable_before_peak is False:
        invariant_violations.append("not_actionable_before_peak")
    return {
        "earliest_actionable_policy_version": version,
        "earliest_actionable_ts": earliest_actionable_ts,
        "required_inputs_available_at": required_inputs_available_at,
        "missing_inputs_before_ts": missing_inputs_before_ts,
        "peak_ts": peak_ts,
        "peak_ts_quality": _extract_scalar(bags, [("peak_ts_quality",)], default="unknown"),
        "peak_ts_source": _extract_scalar(bags, [("peak_ts_source",)]),
        "counterfactual_entry_ts": counterfactual_entry_ts,
        "actionable_before_peak": actionable_before_peak,
        "earliest_actionable_reason": reason,
        "actionability_quality": _extract_scalar(bags, [("actionability_quality",)], default="unknown"),
        "decision_ts": _extract_scalar(bags, [("decision_ts",), ("decision_available_at",)]),
        "decision_available_at": _extract_scalar(bags, [("decision_available_at",)]),
        "paper_trade_id": _extract_scalar(bags, [("paper_trade_id",), ("id",)]),
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
        "missing_fields": sorted(set(missing_fields)),
        "invariant_violations": sorted(set(invariant_violations)),
    }


def _extract_realtime_clean_contract(event, bags):
    version = _extract_scalar(
        bags,
        [
            ("clean_standard_version",),
            ("realtime_clean_detector_version",),
            ("realtime_clean_standard_version",),
        ],
    )
    if event.get("event_type") != REALTIME_CLEAN_EVENT_TYPE and not version:
        return None

    clean_observation_type = _extract_scalar(
        bags,
        [
            ("clean_observation_type",),
            ("realtime_clean_observation_type",),
            ("quote_clean_observation_type",),
        ],
    )
    quote_source = _extract_scalar(
        bags,
        [
            ("quote_source",),
            ("realtime_clean_quote_source",),
            ("entry_quote_source",),
        ],
        default=event.get("source"),
    )
    quote_age_sec = _as_float(
        _extract_scalar(
            bags,
            [
                ("quote_age_sec",),
                ("realtime_quote_age_sec",),
            ],
        )
    )
    decision_available_at = _extract_scalar(
        bags,
        [
            ("decision_available_at",),
            ("quote_available_at",),
            ("available_at",),
        ],
        default=event.get("available_at"),
    )
    entry_quote_available = _extract_flag(
        bags,
        [
            ("entry_quote_available",),
            ("entry_quote_clean_available",),
        ],
    )
    exit_quote_available = _extract_flag(
        bags,
        [
            ("exit_quote_available",),
            ("exit_quote_clean_available",),
        ],
    )
    entry_quote_available_at = _extract_scalar(
        bags,
        [
            ("entry_quote_available_at",),
            ("entry_quote_ts",),
            ("entry_quote_time",),
        ],
    )
    exit_quote_available_at = _extract_scalar(
        bags,
        [
            ("exit_quote_available_at",),
            ("exit_quote_ts",),
            ("exit_quote_time",),
        ],
    )
    missing_fields = []
    if not version:
        missing_fields.append("clean_standard_version")
    if quote_age_sec is None:
        missing_fields.append("quote_age_sec")
    if entry_quote_available is None:
        missing_fields.append("entry_quote_available")
    if exit_quote_available is None:
        missing_fields.append("exit_quote_available")
    if not decision_available_at:
        missing_fields.append("decision_available_at")
    if not quote_source:
        missing_fields.append("quote_source")
    used_future_peak = _extract_flag(bags, [("used_future_peak",)]) is True
    used_future_outcome = _extract_flag(bags, [("used_future_outcome",)]) is True
    used_posthoc_label = _extract_flag(bags, [("used_posthoc_label",)]) is True
    future_leakage_fields = []
    for field, used in (
        ("used_future_peak", used_future_peak),
        ("used_future_outcome", used_future_outcome),
        ("used_posthoc_label", used_posthoc_label),
    ):
        if used:
            future_leakage_fields.append(field)
    forbidden_future_fields_used = _extract_scalar(bags, [("forbidden_future_fields_used",)], default=[])
    if forbidden_future_fields_used is None:
        forbidden_future_fields_used = []
    if not isinstance(forbidden_future_fields_used, list):
        forbidden_future_fields_used = [forbidden_future_fields_used]
    future_leakage_fields.extend(str(field) for field in forbidden_future_fields_used if field)
    realtime_clean = (
        clean_observation_type in {"TRADABLE_CLEAN_OBSERVED", "QUOTE_CLEAN_OBSERVED"}
        and not missing_fields
        and not future_leakage_fields
        and entry_quote_available is True
        and exit_quote_available is True
    )
    if clean_observation_type is None:
        clean_observation_type = "TRADABLE_CLEAN_OBSERVED" if realtime_clean else "QUOTE_DIRTY_OBSERVED"
    intent_size = _extract_scalar(bags, [("size",), ("position_size_sol",), ("entry_size",)])
    if intent_size is not None:
        intent_size = _as_float(intent_size)
    quote_mint = _extract_scalar(
        bags,
        [
            ("quote_mint",),
            ("inputMint",),
            ("quote_asset",),
        ],
        default="SOL",
    )
    return {
        "realtime_clean_detector_version": str(version) if version else None,
        "clean_standard_version": str(version) if version else None,
        "clean_observation_type": clean_observation_type,
        "quote_source": quote_source,
        "quote_age_sec": quote_age_sec,
        "decision_available_at": decision_available_at,
        "entry_quote_available": entry_quote_available,
        "exit_quote_available": exit_quote_available,
        "entry_quote_available_at": entry_quote_available_at,
        "exit_quote_available_at": exit_quote_available_at,
        "quote_intent_id": _extract_scalar(bags, [("quote_intent_id",), ("paper_trade_id",), ("trade_id",), ("id",)]),
        "side": _extract_scalar(bags, [("side",)], default="buy"),
        "size": intent_size,
        "route": _extract_scalar(bags, [("route",), ("signal_route",), ("entry_mode",)]),
        "pool": _extract_scalar(bags, [("pool",), ("canonical_pool_group",), ("lifecycle_id",)]),
        "quote_mint": quote_mint,
        "slippage_bps": _extract_scalar(bags, [("slippage_bps",), ("entry_quote_slippage_bps",), ("exit_quote_slippage_bps",)]),
        "used_future_peak": used_future_peak,
        "used_future_outcome": used_future_outcome,
        "used_posthoc_label": used_posthoc_label,
        "forbidden_future_fields_used": forbidden_future_fields_used,
        "missing_fields": sorted(set(missing_fields)),
        "future_leakage_fields": sorted(set(future_leakage_fields)),
        "realtime_clean": bool(realtime_clean),
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_quote_intent_binding_contract(event, bags):
    version = _extract_scalar(
        bags,
        [
            ("binding_policy_version",),
            ("quote_intent_binding_version",),
        ],
    )
    if event.get("event_type") != QUOTE_INTENT_BINDING_EVENT_TYPE and not version:
        return None
    required_fields = [
        "quote_intent_id",
        "side",
        "size",
        "route",
        "pool",
        "quote_mint",
        "slippage_bps",
        "quote_ts",
    ]
    values = {
        "quote_intent_id": _extract_scalar(bags, [("quote_intent_id",), ("paper_trade_id",), ("trade_id",), ("id",)]),
        "side": _extract_scalar(bags, [("side",)], default="buy"),
        "size": _as_float(_extract_scalar(bags, [("size",), ("position_size_sol",), ("entry_size",)])),
        "route": _extract_scalar(bags, [("route",), ("signal_route",), ("entry_mode",)]),
        "pool": _extract_scalar(bags, [("pool",), ("canonical_pool_group",), ("lifecycle_id",)]),
        "quote_mint": _extract_scalar(bags, [("quote_mint",), ("inputMint",), ("quote_asset",)], default="SOL"),
        "slippage_bps": _as_float(_extract_scalar(bags, [("slippage_bps",), ("entry_quote_slippage_bps",), ("exit_quote_slippage_bps")])),
        "quote_ts": _extract_scalar(bags, [("quote_ts",), ("quoteTs",), ("quote_time",)]),
        "token_ca": _extract_scalar(bags, [("token_ca",), ("tokenCA",)]),
    }
    missing_fields = []
    for field in required_fields:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    if not values.get("token_ca"):
        missing_fields.append("token_ca")
    if values.get("size") is not None and values.get("size") <= 0:
        missing_fields.append("size_positive")
    if values.get("slippage_bps") is not None and values.get("slippage_bps") < 0:
        missing_fields.append("slippage_bps_nonnegative")
    if not version:
        missing_fields.append("binding_policy_version")
    mismatch_fields = _extract_scalar(bags, [("mismatch_fields",)], default=[])
    if mismatch_fields is None:
        mismatch_fields = []
    if not isinstance(mismatch_fields, list):
        mismatch_fields = [mismatch_fields]
    used_future_peak = _extract_flag(bags, [("used_future_peak",)]) is True
    used_future_outcome = _extract_flag(bags, [("used_future_outcome",)]) is True
    used_posthoc_label = _extract_flag(bags, [("used_posthoc_label",)]) is True
    future_leakage_fields = []
    for field, used in (
        ("used_future_peak", used_future_peak),
        ("used_future_outcome", used_future_outcome),
        ("used_posthoc_label", used_posthoc_label),
    ):
        if used:
            future_leakage_fields.append(field)
    forbidden_future_fields_used = _extract_scalar(bags, [("forbidden_future_fields_used",)], default=[])
    if forbidden_future_fields_used is None:
        forbidden_future_fields_used = []
    if not isinstance(forbidden_future_fields_used, list):
        forbidden_future_fields_used = [forbidden_future_fields_used]
    future_leakage_fields.extend(str(field) for field in forbidden_future_fields_used if field)
    quote_intent_bound = _extract_flag(bags, [("quote_intent_bound",), ("quote_bound",)])
    if quote_intent_bound is None:
        quote_intent_bound = not missing_fields and not mismatch_fields and not future_leakage_fields
    return {
        "binding_policy_version": str(version) if version else None,
        "quote_intent_binding_version": str(version) if version else None,
        **values,
        "quote_source": _extract_scalar(bags, [("quote_source",)], default=event.get("source")),
        "quote_binding_proof_level": _extract_scalar(bags, [("quote_binding_proof_level",)], default="unknown"),
        "quote_intent_binding_quality": _extract_scalar(bags, [("quote_intent_binding_quality",)], default="unknown"),
        "quote_intent_bound": bool(quote_intent_bound),
        "intent_hash": _extract_scalar(bags, [("intent_hash",)]),
        "quote_hash": _extract_scalar(bags, [("quote_hash",)]),
        "quote_binding_hash": _extract_scalar(bags, [("quote_binding_hash",)]),
        "missing_fields": sorted(set(missing_fields)),
        "mismatch_fields": sorted({str(field) for field in mismatch_fields if field}),
        "future_leakage_fields": sorted(set(future_leakage_fields)),
        "used_future_peak": used_future_peak,
        "used_future_outcome": used_future_outcome,
        "used_posthoc_label": used_posthoc_label,
        "forbidden_future_fields_used": forbidden_future_fields_used,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_idempotency_contract(event, bags):
    version = _extract_scalar(
        bags,
        [
            ("idempotency_contract_version",),
            ("contract_version",),
        ],
    )
    if event.get("event_type") != IDEMPOTENCY_EVENT_TYPE and not version:
        return None
    values = {
        "decision_id": _extract_scalar(bags, [("decision_id",)]),
        "execution_id": _extract_scalar(bags, [("execution_id",)]),
        "idempotency_key": _extract_scalar(bags, [("idempotency_key",)]),
        "token_lifecycle_key": _extract_scalar(bags, [("token_lifecycle_key",)]),
        "action": _extract_scalar(bags, [("action",)]),
        "namespace": _extract_scalar(bags, [("namespace",)]),
        "environment_id": _extract_scalar(bags, [("environment_id",)]),
        "route": _extract_scalar(bags, [("route",), ("signal_route",), ("entry_mode",)]),
        "hash_algorithm": _extract_scalar(bags, [("hash_algorithm",)]),
        "collision_policy": _extract_scalar(bags, [("collision_policy",)]),
        "idempotency_intent_hash": _extract_scalar(bags, [("idempotency_intent_hash",), ("key_material_hash",)]),
        "namespace_isolation_prefix": _extract_scalar(bags, [("namespace_isolation_prefix",)]),
        "cross_environment_isolated": _extract_flag(bags, [("cross_environment_isolated",)]),
        "idempotency_proof_level": _extract_scalar(bags, [("idempotency_proof_level",)], default="unknown"),
    }
    idempotency_required = ["decision_id", "execution_id", "idempotency_key", "token_lifecycle_key", "action"]
    namespace_required = ["namespace", "environment_id", "route", "hash_algorithm", "collision_policy"]
    missing_idempotency_fields = []
    for field in idempotency_required:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_idempotency_fields.append(field)
    missing_namespace_fields = []
    for field in namespace_required:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_namespace_fields.append(field)
    if not version:
        missing_idempotency_fields.append("idempotency_contract_version")
    if not values.get("idempotency_intent_hash"):
        missing_idempotency_fields.append("idempotency_intent_hash")
    expected_prefix = f"{values.get('environment_id')}:{values.get('namespace')}:"
    namespace_prefix_ok = bool(
        values.get("environment_id")
        and values.get("namespace")
        and values.get("idempotency_key")
        and str(values.get("idempotency_key")).startswith(expected_prefix)
    )
    if values.get("cross_environment_isolated") is None:
        values["cross_environment_isolated"] = namespace_prefix_ok
    return {
        "idempotency_contract_version": str(version) if version else None,
        **values,
        "namespace_prefix_ok": namespace_prefix_ok,
        "missing_idempotency_fields": sorted(set(missing_idempotency_fields)),
        "missing_namespace_fields": sorted(set(missing_namespace_fields)),
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_execution_control(event, bags):
    version = _extract_scalar(
        bags,
        [
            ("execution_control_version",),
            ("control_version",),
        ],
    )
    if event.get("event_type") != EXECUTION_CONTROL_EVENT_TYPE and not version:
        return None
    values = {
        "execution_control_version": str(version) if version else None,
        "decision_id": _extract_scalar(bags, [("decision_id",)]),
        "execution_id": _extract_scalar(bags, [("execution_id",)]),
        "token_lifecycle_key": _extract_scalar(bags, [("token_lifecycle_key",)]),
        "environment_id": _extract_scalar(bags, [("environment_id",)]),
        "route": _extract_scalar(bags, [("route",), ("signal_route",), ("entry_mode",)]),
        "lease_id": _extract_scalar(bags, [("lease_id",)]),
        "fencing_token": _extract_scalar(bags, [("fencing_token",)]),
        "acquired_at": _extract_scalar(bags, [("acquired_at",)]),
        "expires_at": _extract_scalar(bags, [("expires_at",)]),
        "released_at": _extract_scalar(bags, [("released_at",)]),
        "lease_status": _extract_scalar(bags, [("lease_status",)]),
        "lease_valid_at_execution": _extract_flag(bags, [("lease_valid_at_execution",)]),
        "state_version_at_decision": _as_int(_extract_scalar(bags, [("state_version_at_decision",)]), default=None),
        "state_version_at_execution": _as_int(_extract_scalar(bags, [("state_version_at_execution",)]), default=None),
        "requires_revalidation_before_fill": _extract_flag(bags, [("requires_revalidation_before_fill",)]),
        "revalidation_passed": _extract_flag(bags, [("revalidation_passed",)]),
        "state": _extract_scalar(bags, [("state",)]),
        "state_version": _as_int(_extract_scalar(bags, [("state_version",)]), default=None),
        "failure_reason": _extract_scalar(bags, [("failure_reason",)]),
        "terminal_state": _extract_flag(bags, [("terminal_state",)]),
        "execution_control_proof_level": _extract_scalar(bags, [("execution_control_proof_level",)], default="unknown"),
        "state_version_source": _extract_scalar(bags, [("state_version_source",)], default="unknown"),
    }
    lease_required = ["lease_id", "fencing_token", "acquired_at", "expires_at", "lease_status"]
    fencing_required = ["state_version_at_decision", "state_version_at_execution", "requires_revalidation_before_fill"]
    state_required = ["execution_id", "state", "state_version", "failure_reason"]
    missing_lease_fields = []
    for field in lease_required:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_lease_fields.append(field)
    missing_fencing_fields = []
    for field in fencing_required:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fencing_fields.append(field)
    missing_state_fields = []
    for field in state_required:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_state_fields.append(field)
    if not version:
        missing_lease_fields.append("execution_control_version")
    acquired_ts = _timestamp_epoch_seconds(values.get("acquired_at"))
    expires_ts = _timestamp_epoch_seconds(values.get("expires_at"))
    released_ts = _timestamp_epoch_seconds(values.get("released_at"))
    lease_time_order_ok = bool(acquired_ts is not None and expires_ts is not None and acquired_ts <= expires_ts)
    lease_terminal_ok = str(values.get("lease_status") or "").lower() in {"released", "expired", "cancelled"}
    lease_valid_at_execution = values.get("lease_valid_at_execution")
    if lease_valid_at_execution is None:
        lease_valid_at_execution = bool(
            acquired_ts is not None
            and expires_ts is not None
            and released_ts is not None
            and acquired_ts <= released_ts <= expires_ts
        )
        values["lease_valid_at_execution"] = lease_valid_at_execution
    decision_version = values.get("state_version_at_decision")
    execution_version = values.get("state_version_at_execution")
    state_version = values.get("state_version")
    state_version_order_ok = bool(
        decision_version is not None
        and execution_version is not None
        and state_version is not None
        and decision_version <= execution_version <= state_version
    )
    revalidation_ok = values.get("requires_revalidation_before_fill") is True and values.get("revalidation_passed") is True
    terminal_states = {"filled_paper", "rejected", "failed", "cancelled", "no_fill", "skipped"}
    terminal_state_ok = bool(values.get("terminal_state") is True and str(values.get("state") or "").lower() in terminal_states)
    return {
        **values,
        "missing_lease_fields": sorted(set(missing_lease_fields)),
        "missing_fencing_fields": sorted(set(missing_fencing_fields)),
        "missing_state_fields": sorted(set(missing_state_fields)),
        "lease_time_order_ok": lease_time_order_ok,
        "lease_terminal_ok": lease_terminal_ok,
        "state_version_order_ok": state_version_order_ok,
        "revalidation_ok": revalidation_ok,
        "terminal_state_ok": terminal_state_ok,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _hash_matches(material, expected_hash):
    if not material or not expected_hash:
        return bool(expected_hash)
    try:
        return sha256_hex(material) == expected_hash
    except TypeError:
        return False


def _extract_paper_ledger_contract(event, bags):
    version = _extract_scalar(
        bags,
        [
            ("paper_ledger_version",),
            ("position_ledger_version",),
            ("capital_ledger_version",),
        ],
    )
    if event.get("event_type") != PAPER_LEDGER_EVENT_TYPE and not version:
        return None
    values = {
        "paper_ledger_version": str(version) if version else None,
        "paper_trade_id": _extract_scalar(bags, [("paper_trade_id",)]),
        "decision_id": _extract_scalar(bags, [("decision_id",)]),
        "execution_id": _extract_scalar(bags, [("execution_id",)]),
        "position_id": _extract_scalar(bags, [("position_id",)]),
        "position_status": _extract_scalar(bags, [("position_status",)]),
        "entry_size_sol": _as_float(_extract_scalar(bags, [("entry_size_sol",), ("position_size_sol",)])),
        "remaining_size": _as_float(_extract_scalar(bags, [("remaining_size",)])),
        "position_ledger_hash": _extract_scalar(bags, [("position_ledger_hash",), ("ledger_hash",)]),
        "position_ledger_material": _extract_scalar(bags, [("position_ledger_material",)]),
        "capital_ledger_id": _extract_scalar(bags, [("capital_ledger_id",)]),
        "capital_basis_sol": _as_float(_extract_scalar(bags, [("capital_basis_sol",)])),
        "available_capital": _as_float(_extract_scalar(bags, [("available_capital",)])),
        "reserved_capital": _as_float(_extract_scalar(bags, [("reserved_capital",)])),
        "open_exposure": _as_float(_extract_scalar(bags, [("open_exposure",)])),
        "realized_pnl_sol": _as_float(_extract_scalar(bags, [("realized_pnl_sol",)])),
        "unrealized_pnl_sol": _as_float(_extract_scalar(bags, [("unrealized_pnl_sol",)])),
        "fees_sol": _as_float(_extract_scalar(bags, [("fees_sol",)])),
        "capital_ledger_hash": _extract_scalar(bags, [("capital_ledger_hash",)]),
        "capital_ledger_material": _extract_scalar(bags, [("capital_ledger_material",)]),
        "ledger_checkpoint_id": _extract_scalar(bags, [("ledger_checkpoint_id",)]),
        "ledger_hash": _extract_scalar(bags, [("ledger_hash",), ("double_entry_ledger_hash",)]),
        "ledger_hash_material": _extract_scalar(bags, [("ledger_hash_material",)]),
        "invariant_lhs": _as_float(_extract_scalar(bags, [("invariant_lhs",)])),
        "invariant_rhs": _as_float(_extract_scalar(bags, [("invariant_rhs",)])),
        "invariant_delta": _as_float(_extract_scalar(bags, [("invariant_delta",)])),
        "invariant_ok": _extract_flag(bags, [("invariant_ok",)]),
        "reservation_id": _extract_scalar(bags, [("reservation_id",)]),
        "reservation_status": _extract_scalar(bags, [("reservation_status",)]),
        "reservation_ttl_sec": _as_float(_extract_scalar(bags, [("reservation_ttl_sec",), ("reservation_ttl",)])),
        "release_reason": _extract_scalar(bags, [("release_reason",)]),
        "size_source": _extract_scalar(bags, [("size_source",)]),
        "ledger_scope": _extract_scalar(bags, [("ledger_scope",)]),
        "ledger_proof_level": _extract_scalar(bags, [("ledger_proof_level",)], default="unknown"),
    }
    if values.get("realized_pnl_sol") is None:
        values["realized_pnl_sol"] = 0.0
    if values.get("unrealized_pnl_sol") is None:
        values["unrealized_pnl_sol"] = 0.0
    if values.get("fees_sol") is None:
        values["fees_sol"] = 0.0
    position_required = ["position_id", "execution_id", "decision_id", "remaining_size", "position_ledger_hash"]
    capital_required = ["capital_ledger_id", "available_capital", "reserved_capital", "open_exposure", "capital_ledger_hash"]
    double_entry_required = ["ledger_checkpoint_id", "available_capital", "reserved_capital", "open_exposure", "ledger_hash"]
    reservation_required = ["reservation_id", "position_id", "reserved_capital", "reservation_ttl_sec", "release_reason"]
    missing_position_fields = []
    for field in position_required:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_position_fields.append(field)
    missing_capital_fields = []
    for field in capital_required:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_capital_fields.append(field)
    missing_double_entry_fields = []
    for field in double_entry_required:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_double_entry_fields.append(field)
    missing_reservation_fields = []
    for field in reservation_required:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_reservation_fields.append(field)
    if not version:
        missing_position_fields.append("paper_ledger_version")
        missing_capital_fields.append("paper_ledger_version")
        missing_double_entry_fields.append("paper_ledger_version")
        missing_reservation_fields.append("paper_ledger_version")
    nonnegative_fields = ["entry_size_sol", "remaining_size", "available_capital", "reserved_capital", "open_exposure", "fees_sol"]
    negative_fields = [field for field in nonnegative_fields if values.get(field) is not None and values.get(field) < 0]
    position_hash_ok = _hash_matches(values.get("position_ledger_material"), values.get("position_ledger_hash"))
    capital_hash_ok = _hash_matches(values.get("capital_ledger_material"), values.get("capital_ledger_hash"))
    ledger_hash_ok = _hash_matches(values.get("ledger_hash_material"), values.get("ledger_hash"))
    if values.get("invariant_ok") is None and values.get("invariant_delta") is not None:
        values["invariant_ok"] = abs(values.get("invariant_delta")) <= 0.000000001
    reservation_ttl_ok = values.get("reservation_ttl_sec") is not None and values.get("reservation_ttl_sec") > 0
    release_reason_ok = bool(str(values.get("release_reason") or "").strip())
    return {
        **values,
        "position_hash_ok": position_hash_ok,
        "capital_hash_ok": capital_hash_ok,
        "ledger_hash_ok": ledger_hash_ok,
        "negative_fields": negative_fields,
        "missing_position_fields": sorted(set(missing_position_fields)),
        "missing_capital_fields": sorted(set(missing_capital_fields)),
        "missing_double_entry_fields": sorted(set(missing_double_entry_fields)),
        "missing_reservation_fields": sorted(set(missing_reservation_fields)),
        "reservation_ttl_ok": reservation_ttl_ok,
        "release_reason_ok": release_reason_ok,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_no_fill_outcome_contract(event, bags):
    version = _extract_scalar(
        bags,
        [
            ("no_fill_outcome_version",),
            ("recovery_control_version",),
        ],
    )
    if event.get("event_type") != NO_FILL_OUTCOME_EVENT_TYPE and not version:
        return None
    values = {
        "no_fill_outcome_version": str(version) if version else None,
        "attempt_id": _extract_scalar(bags, [("attempt_id",)]),
        "decision_id": _extract_scalar(bags, [("decision_id",)]),
        "execution_id": _extract_scalar(bags, [("execution_id",)]),
        "token_lifecycle_key": _extract_scalar(bags, [("token_lifecycle_key",)]),
        "outcome_state": _extract_scalar(bags, [("outcome_state",), ("state",)]),
        "terminal_state": _extract_flag(bags, [("terminal_state",)]),
        "no_fill_record_required": _extract_flag(bags, [("no_fill_record_required",)]),
        "no_fill_reason": _extract_scalar(bags, [("no_fill_reason",)]),
        "missed_net_peak30": _as_float(_extract_scalar(bags, [("missed_net_peak30",)])),
        "missed_net_peak30_source": _extract_scalar(bags, [("missed_net_peak30_source",)]),
        "no_fill_cost": _as_float(_extract_scalar(bags, [("no_fill_cost",)])),
        "no_fill_saved_loss": _as_float(_extract_scalar(bags, [("no_fill_saved_loss",)])),
        "no_fill_cost_model": _extract_scalar(bags, [("no_fill_cost_model",)]),
        "no_fill_outcome_hash": _extract_scalar(bags, [("no_fill_outcome_hash",)]),
        "outcome_source": _extract_scalar(bags, [("outcome_source",)], default=event.get("source")),
        "outcome_available_at": _extract_scalar(bags, [("outcome_available_at",)], default=event.get("available_at")),
    }
    required_fields = ["no_fill_reason", "missed_net_peak30", "no_fill_cost", "no_fill_saved_loss"]
    missing_fields = []
    for field in required_fields:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    if not version:
        missing_fields.append("no_fill_outcome_version")
    if not values.get("attempt_id"):
        missing_fields.append("attempt_id")
    if not values.get("outcome_state"):
        missing_fields.append("outcome_state")
    if values.get("terminal_state") is None:
        missing_fields.append("terminal_state")
    invalid_numeric_fields = [
        field
        for field in ("no_fill_cost", "no_fill_saved_loss")
        if values.get(field) is not None and (not math.isfinite(values.get(field)) or values.get(field) < 0)
    ]
    if values.get("missed_net_peak30") is not None and not math.isfinite(values.get("missed_net_peak30")):
        invalid_numeric_fields.append("missed_net_peak30")
    if values.get("no_fill_record_required") is True and str(values.get("outcome_state") or "").lower() not in {"no_fill", "skipped", "rejected", "failed", "cancelled"}:
        invalid_numeric_fields.append("no_fill_record_required_state_mismatch")
    terminal_states = {"filled_paper", "rejected", "failed", "cancelled", "no_fill", "skipped"}
    terminal_outcome_ok = bool(
        values.get("terminal_state") is True
        and str(values.get("outcome_state") or "").lower() in terminal_states
        and not missing_fields
        and not invalid_numeric_fields
    )
    return {
        **values,
        "missing_fields": sorted(set(missing_fields)),
        "invalid_numeric_fields": sorted(set(invalid_numeric_fields)),
        "terminal_outcome_ok": terminal_outcome_ok,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _dict_status_ok(value):
    if isinstance(value, dict):
        status = str(value.get("status") or "").strip().lower()
        if status and status not in {"ok", "clean", "completed", "recovered"}:
            return False
        for field in ("event_log_ok", "resume_allowed"):
            if field in value and value.get(field) is not True:
                return False
        for field in ("orphaned_execution_count", "non_terminal_execution_count", "malformed_no_fill_count"):
            if field in value and _as_int(value.get(field), default=0) != 0:
                return False
        return True
    status = str(value or "").strip().lower()
    return status in {"ok", "clean", "completed", "recovered"}


def _extract_runtime_recovery_control(event, bags):
    version = _extract_scalar(
        bags,
        [
            ("recovery_control_version",),
            ("recovery_version",),
        ],
    )
    if event.get("event_type") != RUNTIME_RECOVERY_EVENT_TYPE and not version:
        return None
    values = {
        "recovery_control_version": str(version) if version else None,
        "recovery_id": _extract_scalar(bags, [("recovery_id",)]),
        "state": _extract_scalar(bags, [("state",)]),
        "orphan_scan_result": _extract_scalar(bags, [("orphan_scan_result",)]),
        "reconcile_result": _extract_scalar(bags, [("reconcile_result",)]),
        "drain_id": _extract_scalar(bags, [("drain_id",)]),
        "queued_candidates_revalidated": _as_int(_extract_scalar(bags, [("queued_candidates_revalidated",)]), default=None),
        "expired_candidates_emitted": _as_int(_extract_scalar(bags, [("expired_candidates_emitted",)]), default=None),
        "resume_drain_completed_at": _extract_scalar(bags, [("resume_drain_completed_at",)]),
        "drain_status": _extract_scalar(bags, [("drain_status",)]),
        "new_entries_blocked_until_drain": _extract_flag(bags, [("new_entries_blocked_until_drain",)]),
        "resume_allowed": _extract_flag(bags, [("resume_allowed",)]),
        "source_cursor": _extract_scalar(bags, [("source_cursor",)], default={}),
    }
    recovery_required = ["recovery_id", "state", "orphan_scan_result", "reconcile_result"]
    drain_required = ["drain_id", "queued_candidates_revalidated", "expired_candidates_emitted", "resume_drain_completed_at"]
    missing_recovery_fields = []
    for field in recovery_required:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_recovery_fields.append(field)
    missing_drain_fields = []
    for field in drain_required:
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_drain_fields.append(field)
    if not version:
        missing_recovery_fields.append("recovery_control_version")
    recovery_state_ok = str(values.get("state") or "").lower() in {"clean_start", "recovered", "drained_clean"}
    orphan_scan_ok = _dict_status_ok(values.get("orphan_scan_result"))
    reconcile_ok = _dict_status_ok(values.get("reconcile_result"))
    drain_counts_ok = (
        values.get("queued_candidates_revalidated") is not None
        and values.get("queued_candidates_revalidated") >= 0
        and values.get("expired_candidates_emitted") is not None
        and values.get("expired_candidates_emitted") >= 0
    )
    drain_completed_at_ok = _timestamp_epoch_seconds(values.get("resume_drain_completed_at")) is not None
    drain_status_ok = str(values.get("drain_status") or "").lower() in {"completed", "clean", "ok"}
    resume_allowed_ok = values.get("resume_allowed") is True
    return {
        **values,
        "missing_recovery_fields": sorted(set(missing_recovery_fields)),
        "missing_drain_fields": sorted(set(missing_drain_fields)),
        "recovery_state_ok": recovery_state_ok,
        "orphan_scan_ok": orphan_scan_ok,
        "reconcile_ok": reconcile_ok,
        "drain_counts_ok": drain_counts_ok,
        "drain_completed_at_ok": drain_completed_at_ok,
        "drain_status_ok": drain_status_ok,
        "resume_allowed_ok": resume_allowed_ok,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
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
        "earliest_actionable_time_contract": _extract_earliest_actionable_time_contract(event, bags),
        "realtime_clean_contract": _extract_realtime_clean_contract(event, bags),
        "quote_intent_binding_contract": _extract_quote_intent_binding_contract(event, bags),
        "idempotency_contract": _extract_idempotency_contract(event, bags),
        "execution_control": _extract_execution_control(event, bags),
        "paper_ledger_contract": _extract_paper_ledger_contract(event, bags),
        "no_fill_outcome_contract": _extract_no_fill_outcome_contract(event, bags),
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
        "earliest_actionable_time_candidates": [],
        "realtime_clean_candidates": [],
        "quote_intent_binding_candidates": [],
        "idempotency_contract_candidates": [],
        "execution_control_candidates": [],
        "paper_ledger_candidates": [],
        "no_fill_outcome_candidates": [],
        "source_label_research_only": False,
        "denominator_dirty_reasons": [],
        "source_dog_label": None,
        "signal_credit_assignment": None,
        "reference_price_contract": None,
        "trade_outcome_label": None,
        "standardized_stop_contract": None,
        "ex_ante_feasibility_contract": None,
        "earliest_actionable_time": None,
        "realtime_clean_contract": None,
        "quote_intent_binding_contract": None,
        "idempotency_contract": None,
        "execution_control": None,
        "paper_ledger_contract": None,
        "no_fill_outcome": None,
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
    earliest_actionable_time = fact.get("earliest_actionable_time_contract")
    if earliest_actionable_time:
        record["earliest_actionable_time_candidates"].append(earliest_actionable_time)
    realtime_clean = fact.get("realtime_clean_contract")
    if realtime_clean:
        record["realtime_clean_candidates"].append(realtime_clean)
    quote_intent_binding = fact.get("quote_intent_binding_contract")
    if quote_intent_binding:
        record["quote_intent_binding_candidates"].append(quote_intent_binding)
    idempotency_contract = fact.get("idempotency_contract")
    if idempotency_contract:
        record["idempotency_contract_candidates"].append(idempotency_contract)
    execution_control = fact.get("execution_control")
    if execution_control:
        record["execution_control_candidates"].append(execution_control)
    paper_ledger = fact.get("paper_ledger_contract")
    if paper_ledger:
        record["paper_ledger_candidates"].append(paper_ledger)
    no_fill_outcome = fact.get("no_fill_outcome_contract")
    if no_fill_outcome:
        record["no_fill_outcome_candidates"].append(no_fill_outcome)

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


def _finalize_earliest_actionable_time(record):
    candidates = sorted(
        record.get("earliest_actionable_time_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["earliest_actionable_time_candidates"] = candidates
    record["earliest_actionable_time"] = candidates[0] if candidates else None


def _finalize_realtime_clean(record):
    candidates = sorted(
        record.get("realtime_clean_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["realtime_clean_candidates"] = candidates
    if not candidates:
        record["realtime_clean_contract"] = None
        return
    clean_candidates = [candidate for candidate in candidates if candidate.get("realtime_clean") is True]
    selected = clean_candidates[0] if clean_candidates else candidates[0]
    record["realtime_clean_contract"] = selected
    record["realtime_clean"] = bool(selected and selected.get("realtime_clean") is True)


def _finalize_quote_intent_binding(record):
    candidates = sorted(
        record.get("quote_intent_binding_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["quote_intent_binding_candidates"] = candidates
    if not candidates:
        record["quote_intent_binding_contract"] = None
        return
    bound_candidates = [candidate for candidate in candidates if candidate.get("quote_intent_bound") is True]
    selected = bound_candidates[0] if bound_candidates else candidates[0]
    record["quote_intent_binding_contract"] = selected


def _finalize_idempotency_contract(record):
    candidates = sorted(
        record.get("idempotency_contract_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["idempotency_contract_candidates"] = candidates
    record["idempotency_contract"] = candidates[0] if candidates else None


def _finalize_execution_control(record):
    candidates = sorted(
        record.get("execution_control_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["execution_control_candidates"] = candidates
    record["execution_control"] = candidates[0] if candidates else None


def _finalize_paper_ledger(record):
    candidates = sorted(
        record.get("paper_ledger_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["paper_ledger_candidates"] = candidates
    record["paper_ledger_contract"] = candidates[-1] if candidates else None


def _finalize_no_fill_outcome(record):
    candidates = sorted(
        record.get("no_fill_outcome_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["no_fill_outcome_candidates"] = candidates
    if not candidates:
        record["no_fill_outcome"] = None
        return
    valid_candidates = [candidate for candidate in candidates if candidate.get("terminal_outcome_ok") is True]
    record["no_fill_outcome"] = valid_candidates[-1] if valid_candidates else candidates[-1]


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
    if record.get("realtime_clean_contract") and not record.get("quote_intent_binding_contract"):
        missing.append("QuoteIntentBindingContract")
    if record.get("quote_intent_binding_contract") and not record.get("idempotency_contract"):
        missing.append("IdempotencyContract")
        missing.append("IdempotencyKeyNamespaceContract")
    if record.get("idempotency_contract") and not record.get("execution_control"):
        missing.append("ExecutionLeaseContract")
        missing.append("StateVersionFencing")
        missing.append("EntryExecutionStateMachine")
    if record.get("execution_control") and not record.get("paper_ledger_contract"):
        missing.append("PaperPositionLedgerContract")
        missing.append("PaperCapitalLedgerContract")
        missing.append("DoubleEntryLedgerInvariantContract")
        missing.append("CapitalReservationPolicy")
    if record.get("execution_control") and not record.get("no_fill_outcome"):
        missing.append("NoFillOutcome")
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


def _contract_evidence_from_records(record_list, runtime_recovery_controls=None):
    runtime_recovery_controls = runtime_recovery_controls or []
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
    earliest_actionable_records = [
        record
        for record in record_list
        if record.get("earliest_actionable_time_candidates")
    ]
    malformed_earliest_actionable = []
    invariant_violations_earliest_actionable = []
    for record in earliest_actionable_records:
        malformed = [
            item
            for item in record.get("earliest_actionable_time_candidates") or []
            if item.get("missing_fields")
        ]
        if malformed:
            malformed_earliest_actionable.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for item in malformed for field in item.get("missing_fields", [])}),
                }
            )
        violated = [
            item
            for item in record.get("earliest_actionable_time_candidates") or []
            if item.get("invariant_violations")
        ]
        if violated:
            invariant_violations_earliest_actionable.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "violation_count": len(violated),
                    "invariant_violations": sorted({field for item in violated for field in item.get("invariant_violations", [])}),
                }
            )
    realtime_clean_records = [
        record
        for record in record_list
        if record.get("realtime_clean_candidates")
    ]
    malformed_realtime_clean = []
    future_leakage_realtime_clean = []
    for record in realtime_clean_records:
        malformed = [
            item
            for item in record.get("realtime_clean_candidates") or []
            if item.get("missing_fields")
        ]
        if malformed:
            malformed_realtime_clean.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for item in malformed for field in item.get("missing_fields", [])}),
                }
            )
        leaky = [
            item
            for item in record.get("realtime_clean_candidates") or []
            if item.get("future_leakage_fields")
        ]
        if leaky:
            future_leakage_realtime_clean.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "future_leakage_count": len(leaky),
                    "future_leakage_fields": sorted({field for item in leaky for field in item.get("future_leakage_fields", [])}),
                }
            )
    quote_intent_binding_records = [
        record
        for record in record_list
        if record.get("quote_intent_binding_candidates")
    ]
    malformed_quote_intent_bindings = []
    mismatched_quote_intent_bindings = []
    future_leakage_quote_intent_bindings = []
    for record in quote_intent_binding_records:
        malformed = [
            item
            for item in record.get("quote_intent_binding_candidates") or []
            if item.get("missing_fields")
        ]
        if malformed:
            malformed_quote_intent_bindings.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for item in malformed for field in item.get("missing_fields", [])}),
                }
            )
        mismatched = [
            item
            for item in record.get("quote_intent_binding_candidates") or []
            if item.get("mismatch_fields")
        ]
        if mismatched:
            mismatched_quote_intent_bindings.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "mismatch_count": len(mismatched),
                    "mismatch_fields": sorted({field for item in mismatched for field in item.get("mismatch_fields", [])}),
                }
            )
        leaky = [
            item
            for item in record.get("quote_intent_binding_candidates") or []
            if item.get("future_leakage_fields")
        ]
        if leaky:
            future_leakage_quote_intent_bindings.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "future_leakage_count": len(leaky),
                    "future_leakage_fields": sorted({field for item in leaky for field in item.get("future_leakage_fields", [])}),
                }
            )
    idempotency_records = [
        record
        for record in record_list
        if record.get("idempotency_contract_candidates")
    ]
    malformed_idempotency = []
    malformed_namespaces = []
    all_idempotency_candidates = [
        item
        for record in idempotency_records
        for item in record.get("idempotency_contract_candidates") or []
    ]
    for record in idempotency_records:
        malformed = [
            item
            for item in record.get("idempotency_contract_candidates") or []
            if item.get("missing_idempotency_fields")
        ]
        if malformed:
            malformed_idempotency.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for item in malformed for field in item.get("missing_idempotency_fields", [])}),
                }
            )
        malformed_namespace = [
            item
            for item in record.get("idempotency_contract_candidates") or []
            if item.get("missing_namespace_fields") or item.get("namespace_prefix_ok") is not True
        ]
        if malformed_namespace:
            malformed_namespaces.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed_namespace),
                    "missing_fields": sorted({field for item in malformed_namespace for field in item.get("missing_namespace_fields", [])}),
                    "namespace_prefix_violations": sum(1 for item in malformed_namespace if item.get("namespace_prefix_ok") is not True),
                }
            )

    idempotency_key_hashes = {}
    idempotency_collisions = []
    for item in all_idempotency_candidates:
        key = (item.get("environment_id"), item.get("namespace"), item.get("idempotency_key"))
        if not all(key):
            continue
        existing = idempotency_key_hashes.setdefault(key, item.get("idempotency_intent_hash"))
        if existing != item.get("idempotency_intent_hash"):
            idempotency_collisions.append(
                {
                    "environment_id": item.get("environment_id"),
                    "namespace": item.get("namespace"),
                    "idempotency_key": item.get("idempotency_key"),
                    "existing_intent_hash": existing,
                    "incoming_intent_hash": item.get("idempotency_intent_hash"),
                    "source_event_id": item.get("source_event_id"),
                }
            )

    idempotent_actions = {}
    duplicate_action_conflicts = []
    for item in all_idempotency_candidates:
        key = (item.get("environment_id"), item.get("namespace"), item.get("decision_id"), item.get("action"))
        if not all(key):
            continue
        existing = idempotent_actions.setdefault(
            key,
            {
                "execution_id": item.get("execution_id"),
                "idempotency_key": item.get("idempotency_key"),
                "token_lifecycle_key": item.get("token_lifecycle_key"),
            },
        )
        if existing.get("execution_id") != item.get("execution_id"):
            duplicate_action_conflicts.append(
                {
                    "environment_id": item.get("environment_id"),
                    "namespace": item.get("namespace"),
                    "decision_id": item.get("decision_id"),
                    "token_lifecycle_key": item.get("token_lifecycle_key"),
                    "existing_token_lifecycle_key": existing.get("token_lifecycle_key"),
                    "action": item.get("action"),
                    "existing_execution_id": existing.get("execution_id"),
                    "incoming_execution_id": item.get("execution_id"),
                    "existing_idempotency_key": existing.get("idempotency_key"),
                    "incoming_idempotency_key": item.get("idempotency_key"),
                    "source_event_id": item.get("source_event_id"),
                }
            )

    allowed_collision_policies = {"reject_same_namespace_key_with_different_intent_hash"}
    namespace_policy_violations = [
        {
            "environment_id": item.get("environment_id"),
            "namespace": item.get("namespace"),
            "collision_policy": item.get("collision_policy"),
            "hash_algorithm": item.get("hash_algorithm"),
            "source_event_id": item.get("source_event_id"),
        }
        for item in all_idempotency_candidates
        if item.get("collision_policy") not in allowed_collision_policies
        or item.get("hash_algorithm") != "sha256(canonical_json)"
        or item.get("cross_environment_isolated") is not True
    ]
    execution_control_records = [
        record
        for record in record_list
        if record.get("execution_control_candidates")
    ]
    all_execution_control_candidates = [
        item
        for record in execution_control_records
        for item in record.get("execution_control_candidates") or []
    ]
    malformed_leases = []
    malformed_fencing = []
    malformed_state_machines = []
    lease_violations = []
    fencing_violations = []
    state_machine_violations = []
    for record in execution_control_records:
        malformed = [
            item
            for item in record.get("execution_control_candidates") or []
            if item.get("missing_lease_fields")
        ]
        if malformed:
            malformed_leases.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for item in malformed for field in item.get("missing_lease_fields", [])}),
                }
            )
        bad_leases = [
            item
            for item in record.get("execution_control_candidates") or []
            if item.get("lease_time_order_ok") is not True
            or item.get("lease_terminal_ok") is not True
            or item.get("lease_valid_at_execution") is not True
        ]
        if bad_leases:
            lease_violations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "violation_count": len(bad_leases),
                    "source_event_ids": [item.get("source_event_id") for item in bad_leases],
                }
            )
        malformed_fence = [
            item
            for item in record.get("execution_control_candidates") or []
            if item.get("missing_fencing_fields")
        ]
        if malformed_fence:
            malformed_fencing.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed_fence),
                    "missing_fields": sorted({field for item in malformed_fence for field in item.get("missing_fencing_fields", [])}),
                }
            )
        bad_fencing = [
            item
            for item in record.get("execution_control_candidates") or []
            if item.get("state_version_order_ok") is not True
            or item.get("revalidation_ok") is not True
        ]
        if bad_fencing:
            fencing_violations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "violation_count": len(bad_fencing),
                    "source_event_ids": [item.get("source_event_id") for item in bad_fencing],
                }
            )
        malformed_state = [
            item
            for item in record.get("execution_control_candidates") or []
            if item.get("missing_state_fields")
        ]
        if malformed_state:
            malformed_state_machines.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed_state),
                    "missing_fields": sorted({field for item in malformed_state for field in item.get("missing_state_fields", [])}),
                }
            )
        bad_state = [
            item
            for item in record.get("execution_control_candidates") or []
            if item.get("terminal_state_ok") is not True
            or item.get("state_version_order_ok") is not True
        ]
        if bad_state:
            state_machine_violations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "violation_count": len(bad_state),
                    "source_event_ids": [item.get("source_event_id") for item in bad_state],
                }
            )
    paper_ledger_records = [
        record
        for record in record_list
        if record.get("paper_ledger_candidates")
    ]
    all_paper_ledger_candidates = [
        item
        for record in paper_ledger_records
        for item in record.get("paper_ledger_candidates") or []
    ]
    malformed_position_ledgers = []
    malformed_capital_ledgers = []
    malformed_reservations = []
    position_ledger_violations = []
    capital_ledger_violations = []
    double_entry_violations = []
    reservation_policy_violations = []
    for record in paper_ledger_records:
        malformed_position = [
            item
            for item in record.get("paper_ledger_candidates") or []
            if item.get("missing_position_fields")
        ]
        if malformed_position:
            malformed_position_ledgers.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed_position),
                    "missing_fields": sorted({field for item in malformed_position for field in item.get("missing_position_fields", [])}),
                }
            )
        bad_position = [
            item
            for item in record.get("paper_ledger_candidates") or []
            if item.get("negative_fields") or item.get("position_hash_ok") is not True
        ]
        if bad_position:
            position_ledger_violations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "violation_count": len(bad_position),
                    "source_event_ids": [item.get("source_event_id") for item in bad_position],
                    "negative_fields": sorted({field for item in bad_position for field in item.get("negative_fields", [])}),
                }
            )
        malformed_capital = [
            item
            for item in record.get("paper_ledger_candidates") or []
            if item.get("missing_capital_fields")
        ]
        if malformed_capital:
            malformed_capital_ledgers.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed_capital),
                    "missing_fields": sorted({field for item in malformed_capital for field in item.get("missing_capital_fields", [])}),
                }
            )
        bad_capital = [
            item
            for item in record.get("paper_ledger_candidates") or []
            if item.get("negative_fields") or item.get("capital_hash_ok") is not True
        ]
        if bad_capital:
            capital_ledger_violations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "violation_count": len(bad_capital),
                    "source_event_ids": [item.get("source_event_id") for item in bad_capital],
                    "negative_fields": sorted({field for item in bad_capital for field in item.get("negative_fields", [])}),
                }
            )
        malformed_double_entry = [
            item
            for item in record.get("paper_ledger_candidates") or []
            if item.get("missing_double_entry_fields")
        ]
        bad_double_entry = [
            item
            for item in record.get("paper_ledger_candidates") or []
            if item.get("missing_double_entry_fields")
            or item.get("invariant_ok") is not True
            or item.get("ledger_hash_ok") is not True
        ]
        if bad_double_entry:
            double_entry_violations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "violation_count": len(bad_double_entry),
                    "malformed_count": len(malformed_double_entry),
                    "source_event_ids": [item.get("source_event_id") for item in bad_double_entry],
                    "max_abs_invariant_delta": max(abs(item.get("invariant_delta") or 0.0) for item in bad_double_entry),
                }
            )
        malformed_reservation = [
            item
            for item in record.get("paper_ledger_candidates") or []
            if item.get("missing_reservation_fields")
        ]
        if malformed_reservation:
            malformed_reservations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed_reservation),
                    "missing_fields": sorted({field for item in malformed_reservation for field in item.get("missing_reservation_fields", [])}),
                }
            )
        bad_reservation = [
            item
            for item in record.get("paper_ledger_candidates") or []
            if item.get("missing_reservation_fields")
            or item.get("reservation_ttl_ok") is not True
            or item.get("release_reason_ok") is not True
        ]
        if bad_reservation:
            reservation_policy_violations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "violation_count": len(bad_reservation),
                    "source_event_ids": [item.get("source_event_id") for item in bad_reservation],
                }
            )
    no_fill_records = [
        record
        for record in record_list
        if record.get("no_fill_outcome_candidates")
    ]
    all_no_fill_candidates = [
        item
        for record in no_fill_records
        for item in record.get("no_fill_outcome_candidates") or []
    ]
    malformed_no_fill_outcomes = []
    no_fill_outcome_violations = []
    for record in no_fill_records:
        malformed = [
            item
            for item in record.get("no_fill_outcome_candidates") or []
            if item.get("missing_fields")
        ]
        if malformed:
            malformed_no_fill_outcomes.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for item in malformed for field in item.get("missing_fields", [])}),
                }
            )
        bad_no_fill = [
            item
            for item in record.get("no_fill_outcome_candidates") or []
            if item.get("terminal_outcome_ok") is not True or item.get("invalid_numeric_fields")
        ]
        if bad_no_fill:
            no_fill_outcome_violations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "violation_count": len(bad_no_fill),
                    "source_event_ids": [item.get("source_event_id") for item in bad_no_fill],
                    "invalid_numeric_fields": sorted({field for item in bad_no_fill for field in item.get("invalid_numeric_fields", [])}),
                }
            )

    malformed_recovery_controls = [
        item
        for item in runtime_recovery_controls
        if item.get("missing_recovery_fields")
    ]
    recovery_violations = [
        item
        for item in runtime_recovery_controls
        if item.get("missing_recovery_fields")
        or item.get("recovery_state_ok") is not True
        or item.get("orphan_scan_ok") is not True
        or item.get("reconcile_ok") is not True
    ]
    malformed_resume_drains = [
        item
        for item in runtime_recovery_controls
        if item.get("missing_drain_fields")
    ]
    resume_drain_violations = [
        item
        for item in runtime_recovery_controls
        if item.get("missing_drain_fields")
        or item.get("drain_counts_ok") is not True
        or item.get("drain_completed_at_ok") is not True
        or item.get("drain_status_ok") is not True
        or item.get("resume_allowed_ok") is not True
    ]
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
        "EarliestActionableTime": {
            "eligible_earliest_actionable_records": len(earliest_actionable_records),
            "earliest_actionable_count": sum(len(record.get("earliest_actionable_time_candidates") or []) for record in earliest_actionable_records),
            "actionable_before_peak_count": sum(
                1
                for record in earliest_actionable_records
                for item in record.get("earliest_actionable_time_candidates") or []
                if item.get("actionable_before_peak") is True
            ),
            "malformed_count": sum(item["malformed_count"] for item in malformed_earliest_actionable),
            "malformed_earliest_actionable_times": malformed_earliest_actionable,
            "invariant_violation_count": sum(item["violation_count"] for item in invariant_violations_earliest_actionable),
            "invariant_violations": invariant_violations_earliest_actionable,
            "peak_ts_qualities": sorted(
                {
                    item.get("peak_ts_quality")
                    for record in earliest_actionable_records
                    for item in record.get("earliest_actionable_time_candidates") or []
                    if item.get("peak_ts_quality")
                }
            ),
            "earliest_actionable_policy_versions": sorted(
                {
                    item.get("earliest_actionable_policy_version")
                    for record in earliest_actionable_records
                    for item in record.get("earliest_actionable_time_candidates") or []
                    if item.get("earliest_actionable_policy_version")
                }
            ),
            "earliest_actionable_projection_version": "v2.7.0.earliest_actionable_time.v1",
        },
        "RealtimeCleanDetector": {
            "eligible_realtime_clean_records": len(realtime_clean_records),
            "realtime_clean_observation_count": sum(len(record.get("realtime_clean_candidates") or []) for record in realtime_clean_records),
            "realtime_clean_observed_count": sum(1 for record in realtime_clean_records if record.get("realtime_clean") is True),
            "dirty_observed_count": sum(1 for record in realtime_clean_records if record.get("realtime_clean") is False),
            "malformed_count": sum(item["malformed_count"] for item in malformed_realtime_clean),
            "malformed_realtime_clean": malformed_realtime_clean,
            "future_leakage_count": sum(item["future_leakage_count"] for item in future_leakage_realtime_clean),
            "future_leakage_realtime_clean": future_leakage_realtime_clean,
            "clean_standard_versions": sorted(
                {
                    item.get("clean_standard_version")
                    for record in realtime_clean_records
                    for item in record.get("realtime_clean_candidates") or []
                    if item.get("clean_standard_version")
                }
            ),
            "quote_sources": sorted(
                {
                    item.get("quote_source")
                    for record in realtime_clean_records
                    for item in record.get("realtime_clean_candidates") or []
                    if item.get("quote_source")
                }
            ),
            "realtime_clean_projection_version": "v2.7.0.realtime_clean.v1",
        },
        "QuoteIntentBindingContract": {
            "eligible_quote_intent_binding_records": len(quote_intent_binding_records),
            "quote_intent_binding_observation_count": sum(len(record.get("quote_intent_binding_candidates") or []) for record in quote_intent_binding_records),
            "quote_intent_bound_count": sum(1 for record in quote_intent_binding_records if (record.get("quote_intent_binding_contract") or {}).get("quote_intent_bound") is True),
            "quote_intent_unbound_count": sum(1 for record in quote_intent_binding_records if (record.get("quote_intent_binding_contract") or {}).get("quote_intent_bound") is False),
            "malformed_count": sum(item["malformed_count"] for item in malformed_quote_intent_bindings),
            "malformed_quote_intent_bindings": malformed_quote_intent_bindings,
            "mismatch_count": sum(item["mismatch_count"] for item in mismatched_quote_intent_bindings),
            "mismatched_quote_intent_bindings": mismatched_quote_intent_bindings,
            "future_leakage_count": sum(item["future_leakage_count"] for item in future_leakage_quote_intent_bindings),
            "future_leakage_quote_intent_bindings": future_leakage_quote_intent_bindings,
            "binding_policy_versions": sorted(
                {
                    item.get("binding_policy_version")
                    for record in quote_intent_binding_records
                    for item in record.get("quote_intent_binding_candidates") or []
                    if item.get("binding_policy_version")
                }
            ),
            "quote_binding_proof_levels": sorted(
                {
                    item.get("quote_binding_proof_level")
                    for record in quote_intent_binding_records
                    for item in record.get("quote_intent_binding_candidates") or []
                    if item.get("quote_binding_proof_level")
                }
            ),
            "quote_sources": sorted(
                {
                    item.get("quote_source")
                    for record in quote_intent_binding_records
                    for item in record.get("quote_intent_binding_candidates") or []
                    if item.get("quote_source")
                }
            ),
            "quote_intent_binding_projection_version": "v2.7.0.quote_intent_binding.v1",
        },
        "IdempotencyContract": {
            "eligible_idempotency_records": len(idempotency_records),
            "idempotency_observation_count": len(all_idempotency_candidates),
            "malformed_count": sum(item["malformed_count"] for item in malformed_idempotency),
            "malformed_idempotency": malformed_idempotency,
            "idempotency_collision_count": len(idempotency_collisions),
            "idempotency_collisions": idempotency_collisions,
            "duplicate_action_conflict_key": "environment_id:namespace:decision_id:action",
            "duplicate_action_conflict_count": len(duplicate_action_conflicts),
            "duplicate_action_conflicts": duplicate_action_conflicts,
            "contract_versions": sorted(
                {
                    item.get("idempotency_contract_version")
                    for item in all_idempotency_candidates
                    if item.get("idempotency_contract_version")
                }
            ),
            "actions": sorted({item.get("action") for item in all_idempotency_candidates if item.get("action")}),
            "idempotency_proof_levels": sorted(
                {
                    item.get("idempotency_proof_level")
                    for item in all_idempotency_candidates
                    if item.get("idempotency_proof_level")
                }
            ),
            "idempotency_projection_version": "v2.7.0.idempotency.v1",
        },
        "IdempotencyKeyNamespaceContract": {
            "eligible_namespace_records": len(idempotency_records),
            "namespace_observation_count": len(all_idempotency_candidates),
            "malformed_count": sum(item["malformed_count"] for item in malformed_namespaces),
            "malformed_namespaces": malformed_namespaces,
            "namespace_policy_violation_count": len(namespace_policy_violations),
            "namespace_policy_violations": namespace_policy_violations,
            "idempotency_collision_count": len(idempotency_collisions),
            "environment_ids": sorted({item.get("environment_id") for item in all_idempotency_candidates if item.get("environment_id")}),
            "namespaces": sorted({item.get("namespace") for item in all_idempotency_candidates if item.get("namespace")}),
            "routes": sorted({item.get("route") for item in all_idempotency_candidates if item.get("route")}),
            "hash_algorithms": sorted({item.get("hash_algorithm") for item in all_idempotency_candidates if item.get("hash_algorithm")}),
            "collision_policies": sorted({item.get("collision_policy") for item in all_idempotency_candidates if item.get("collision_policy")}),
            "namespace_projection_version": "v2.7.0.idempotency_namespace.v1",
        },
        "ExecutionLeaseContract": {
            "eligible_execution_lease_records": len(execution_control_records),
            "execution_control_observation_count": len(all_execution_control_candidates),
            "malformed_count": sum(item["malformed_count"] for item in malformed_leases),
            "malformed_leases": malformed_leases,
            "lease_violation_count": sum(item["violation_count"] for item in lease_violations),
            "lease_violations": lease_violations,
            "lease_statuses": sorted({item.get("lease_status") for item in all_execution_control_candidates if item.get("lease_status")}),
            "execution_control_versions": sorted({item.get("execution_control_version") for item in all_execution_control_candidates if item.get("execution_control_version")}),
            "execution_control_proof_levels": sorted({item.get("execution_control_proof_level") for item in all_execution_control_candidates if item.get("execution_control_proof_level")}),
            "execution_lease_projection_version": "v2.7.0.execution_lease.v1",
        },
        "StateVersionFencing": {
            "eligible_state_fencing_records": len(execution_control_records),
            "state_fencing_observation_count": len(all_execution_control_candidates),
            "malformed_count": sum(item["malformed_count"] for item in malformed_fencing),
            "malformed_fencing": malformed_fencing,
            "fencing_violation_count": sum(item["violation_count"] for item in fencing_violations),
            "fencing_violations": fencing_violations,
            "state_version_sources": sorted({item.get("state_version_source") for item in all_execution_control_candidates if item.get("state_version_source")}),
            "requires_revalidation_count": sum(1 for item in all_execution_control_candidates if item.get("requires_revalidation_before_fill") is True),
            "revalidation_passed_count": sum(1 for item in all_execution_control_candidates if item.get("revalidation_passed") is True),
            "state_fencing_projection_version": "v2.7.0.state_fencing.v1",
        },
        "EntryExecutionStateMachine": {
            "eligible_entry_execution_records": len(execution_control_records),
            "entry_execution_observation_count": len(all_execution_control_candidates),
            "terminal_state_count": sum(1 for item in all_execution_control_candidates if item.get("terminal_state_ok") is True),
            "malformed_count": sum(item["malformed_count"] for item in malformed_state_machines),
            "malformed_state_machines": malformed_state_machines,
            "state_machine_violation_count": sum(item["violation_count"] for item in state_machine_violations),
            "state_machine_violations": state_machine_violations,
            "states": sorted({item.get("state") for item in all_execution_control_candidates if item.get("state")}),
            "entry_execution_projection_version": "v2.7.0.entry_execution_state_machine.v1",
        },
        "PaperPositionLedgerContract": {
            "eligible_position_ledger_records": len(paper_ledger_records),
            "position_ledger_observation_count": len(all_paper_ledger_candidates),
            "malformed_count": sum(item["malformed_count"] for item in malformed_position_ledgers),
            "malformed_position_ledgers": malformed_position_ledgers,
            "position_ledger_violation_count": sum(item["violation_count"] for item in position_ledger_violations),
            "position_ledger_violations": position_ledger_violations,
            "position_statuses": sorted({item.get("position_status") for item in all_paper_ledger_candidates if item.get("position_status")}),
            "size_sources": sorted({item.get("size_source") for item in all_paper_ledger_candidates if item.get("size_source")}),
            "ledger_proof_levels": sorted({item.get("ledger_proof_level") for item in all_paper_ledger_candidates if item.get("ledger_proof_level")}),
            "position_ledger_projection_version": "v2.7.0.paper_position_ledger.v1",
        },
        "PaperCapitalLedgerContract": {
            "eligible_capital_ledger_records": len(paper_ledger_records),
            "capital_ledger_observation_count": len(all_paper_ledger_candidates),
            "malformed_count": sum(item["malformed_count"] for item in malformed_capital_ledgers),
            "malformed_capital_ledgers": malformed_capital_ledgers,
            "capital_ledger_violation_count": sum(item["violation_count"] for item in capital_ledger_violations),
            "capital_ledger_violations": capital_ledger_violations,
            "latest_available_capital": all_paper_ledger_candidates[-1].get("available_capital") if all_paper_ledger_candidates else None,
            "latest_reserved_capital": all_paper_ledger_candidates[-1].get("reserved_capital") if all_paper_ledger_candidates else None,
            "latest_open_exposure": all_paper_ledger_candidates[-1].get("open_exposure") if all_paper_ledger_candidates else None,
            "capital_ledger_projection_version": "v2.7.0.paper_capital_ledger.v1",
        },
        "DoubleEntryLedgerInvariantContract": {
            "eligible_double_entry_records": len(paper_ledger_records),
            "double_entry_observation_count": len(all_paper_ledger_candidates),
            "invariant_violation_count": sum(item["violation_count"] for item in double_entry_violations),
            "double_entry_violations": double_entry_violations,
            "max_abs_invariant_delta": max([abs(item.get("invariant_delta") or 0.0) for item in all_paper_ledger_candidates] or [None]),
            "ledger_scopes": sorted({item.get("ledger_scope") for item in all_paper_ledger_candidates if item.get("ledger_scope")}),
            "double_entry_projection_version": "v2.7.0.double_entry_ledger.v1",
        },
        "CapitalReservationPolicy": {
            "eligible_reservation_records": len(paper_ledger_records),
            "reservation_observation_count": len(all_paper_ledger_candidates),
            "malformed_count": sum(item["malformed_count"] for item in malformed_reservations),
            "malformed_reservations": malformed_reservations,
            "reservation_policy_violation_count": sum(item["violation_count"] for item in reservation_policy_violations),
            "reservation_policy_violations": reservation_policy_violations,
            "release_reasons": sorted({item.get("release_reason") for item in all_paper_ledger_candidates if item.get("release_reason")}),
            "reservation_statuses": sorted({item.get("reservation_status") for item in all_paper_ledger_candidates if item.get("reservation_status")}),
            "capital_reservation_projection_version": "v2.7.0.capital_reservation_policy.v1",
        },
        "NoFillOutcome": {
            "eligible_no_fill_records": len(no_fill_records),
            "no_fill_outcome_observation_count": len(all_no_fill_candidates),
            "terminal_outcome_count": sum(1 for item in all_no_fill_candidates if item.get("terminal_outcome_ok") is True),
            "no_fill_terminal_count": sum(1 for item in all_no_fill_candidates if str(item.get("outcome_state") or "").lower() in {"no_fill", "skipped", "rejected", "failed", "cancelled"}),
            "malformed_count": sum(item["malformed_count"] for item in malformed_no_fill_outcomes),
            "malformed_no_fill_outcomes": malformed_no_fill_outcomes,
            "no_fill_outcome_violation_count": sum(item["violation_count"] for item in no_fill_outcome_violations),
            "no_fill_outcome_violations": no_fill_outcome_violations,
            "outcome_states": sorted({item.get("outcome_state") for item in all_no_fill_candidates if item.get("outcome_state")}),
            "no_fill_reasons": sorted({item.get("no_fill_reason") for item in all_no_fill_candidates if item.get("no_fill_reason")}),
            "no_fill_cost_models": sorted({item.get("no_fill_cost_model") for item in all_no_fill_candidates if item.get("no_fill_cost_model")}),
            "no_fill_outcome_projection_version": "v2.7.0.no_fill_outcome.v1",
        },
        "CrashRecoveryStateMachine": {
            "eligible_recovery_records": len(runtime_recovery_controls),
            "recovery_observation_count": len(runtime_recovery_controls),
            "malformed_count": len(malformed_recovery_controls),
            "malformed_recovery_controls": [
                {
                    "recovery_id": item.get("recovery_id"),
                    "missing_fields": item.get("missing_recovery_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_recovery_controls
            ],
            "recovery_violation_count": len(recovery_violations),
            "recovery_violations": [
                {
                    "recovery_id": item.get("recovery_id"),
                    "state": item.get("state"),
                    "orphan_scan_ok": item.get("orphan_scan_ok"),
                    "reconcile_ok": item.get("reconcile_ok"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in recovery_violations
            ],
            "recovery_states": sorted({item.get("state") for item in runtime_recovery_controls if item.get("state")}),
            "recovery_control_versions": sorted({item.get("recovery_control_version") for item in runtime_recovery_controls if item.get("recovery_control_version")}),
            "crash_recovery_projection_version": "v2.7.0.crash_recovery_state_machine.v1",
        },
        "ResumeDrainPolicy": {
            "eligible_resume_drain_records": len(runtime_recovery_controls),
            "resume_drain_observation_count": len(runtime_recovery_controls),
            "malformed_count": len(malformed_resume_drains),
            "malformed_resume_drains": [
                {
                    "drain_id": item.get("drain_id"),
                    "missing_fields": item.get("missing_drain_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_resume_drains
            ],
            "resume_drain_violation_count": len(resume_drain_violations),
            "resume_drain_violations": [
                {
                    "drain_id": item.get("drain_id"),
                    "drain_status": item.get("drain_status"),
                    "resume_allowed": item.get("resume_allowed"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in resume_drain_violations
            ],
            "queued_candidates_revalidated": sum(item.get("queued_candidates_revalidated") or 0 for item in runtime_recovery_controls),
            "expired_candidates_emitted": sum(item.get("expired_candidates_emitted") or 0 for item in runtime_recovery_controls),
            "drain_statuses": sorted({item.get("drain_status") for item in runtime_recovery_controls if item.get("drain_status")}),
            "resume_drain_projection_version": "v2.7.0.resume_drain_policy.v1",
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
        "earliest_actionable_time_recorded_events": 0,
        "realtime_clean_detector_recorded_events": 0,
        "quote_intent_binding_recorded_events": 0,
        "idempotency_contract_recorded_events": 0,
        "execution_control_recorded_events": 0,
        "paper_ledger_recorded_events": 0,
        "no_fill_outcome_recorded_events": 0,
        "runtime_recovery_control_recorded_events": 0,
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
            "EarliestActionableTime": {},
            "RealtimeCleanDetector": {},
            "QuoteIntentBindingContract": {},
            "IdempotencyContract": {},
            "IdempotencyKeyNamespaceContract": {},
            "ExecutionLeaseContract": {},
            "StateVersionFencing": {},
            "EntryExecutionStateMachine": {},
            "PaperPositionLedgerContract": {},
            "PaperCapitalLedgerContract": {},
            "DoubleEntryLedgerInvariantContract": {},
            "CapitalReservationPolicy": {},
            "NoFillOutcome": {},
            "CrashRecoveryStateMachine": {},
            "ResumeDrainPolicy": {},
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
            "earliest_actionable_time_ok": False,
            "realtime_clean_detector_ok": False,
            "quote_intent_binding_ok": False,
            "idempotency_contract_ok": False,
            "idempotency_key_namespace_ok": False,
            "execution_lease_ok": False,
            "state_version_fencing_ok": False,
            "entry_execution_state_machine_ok": False,
            "paper_position_ledger_ok": False,
            "paper_capital_ledger_ok": False,
            "double_entry_ledger_invariant_ok": False,
            "capital_reservation_policy_ok": False,
            "no_fill_outcome_ok": False,
            "crash_recovery_state_machine_ok": False,
            "resume_drain_policy_ok": False,
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
    runtime_recovery_controls = []
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
        if event.get("event_type") == EARLIEST_ACTIONABLE_EVENT_TYPE:
            projection["earliest_actionable_time_recorded_events"] += 1
        if event.get("event_type") == REALTIME_CLEAN_EVENT_TYPE:
            projection["realtime_clean_detector_recorded_events"] += 1
        if event.get("event_type") == QUOTE_INTENT_BINDING_EVENT_TYPE:
            projection["quote_intent_binding_recorded_events"] += 1
        if event.get("event_type") == IDEMPOTENCY_EVENT_TYPE:
            projection["idempotency_contract_recorded_events"] += 1
        if event.get("event_type") == EXECUTION_CONTROL_EVENT_TYPE:
            projection["execution_control_recorded_events"] += 1
        if event.get("event_type") == PAPER_LEDGER_EVENT_TYPE:
            projection["paper_ledger_recorded_events"] += 1
        if event.get("event_type") == NO_FILL_OUTCOME_EVENT_TYPE:
            projection["no_fill_outcome_recorded_events"] += 1
        if event.get("event_type") == RUNTIME_RECOVERY_EVENT_TYPE:
            projection["runtime_recovery_control_recorded_events"] += 1
            recovery_control = _extract_runtime_recovery_control(event, _payload_bags(event))
            if recovery_control:
                runtime_recovery_controls.append(recovery_control)
            continue
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
        _finalize_earliest_actionable_time(record)
        _finalize_realtime_clean(record)
        _finalize_quote_intent_binding(record)
        _finalize_idempotency_contract(record)
        _finalize_execution_control(record)
        _finalize_paper_ledger(record)
        _finalize_no_fill_outcome(record)
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
    contract_evidence = _contract_evidence_from_records(record_list, runtime_recovery_controls=runtime_recovery_controls)
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
    projection["health"]["earliest_actionable_time_ok"] = (
        contract_evidence["EarliestActionableTime"]["eligible_earliest_actionable_records"] > 0
        and contract_evidence["EarliestActionableTime"]["actionable_before_peak_count"] > 0
        and contract_evidence["EarliestActionableTime"]["malformed_count"] == 0
        and contract_evidence["EarliestActionableTime"]["invariant_violation_count"] == 0
    )
    projection["health"]["realtime_clean_detector_ok"] = (
        contract_evidence["RealtimeCleanDetector"]["eligible_realtime_clean_records"] > 0
        and contract_evidence["RealtimeCleanDetector"]["realtime_clean_observed_count"] > 0
        and contract_evidence["RealtimeCleanDetector"]["malformed_count"] == 0
        and contract_evidence["RealtimeCleanDetector"]["future_leakage_count"] == 0
    )
    projection["health"]["quote_intent_binding_ok"] = (
        contract_evidence["QuoteIntentBindingContract"]["eligible_quote_intent_binding_records"] > 0
        and contract_evidence["QuoteIntentBindingContract"]["quote_intent_bound_count"] > 0
        and contract_evidence["QuoteIntentBindingContract"]["malformed_count"] == 0
        and contract_evidence["QuoteIntentBindingContract"]["mismatch_count"] == 0
        and contract_evidence["QuoteIntentBindingContract"]["future_leakage_count"] == 0
    )
    projection["health"]["idempotency_contract_ok"] = (
        contract_evidence["IdempotencyContract"]["eligible_idempotency_records"] > 0
        and contract_evidence["IdempotencyContract"]["malformed_count"] == 0
        and contract_evidence["IdempotencyContract"]["idempotency_collision_count"] == 0
        and contract_evidence["IdempotencyContract"]["duplicate_action_conflict_count"] == 0
    )
    projection["health"]["idempotency_key_namespace_ok"] = (
        contract_evidence["IdempotencyKeyNamespaceContract"]["eligible_namespace_records"] > 0
        and contract_evidence["IdempotencyKeyNamespaceContract"]["malformed_count"] == 0
        and contract_evidence["IdempotencyKeyNamespaceContract"]["namespace_policy_violation_count"] == 0
        and contract_evidence["IdempotencyKeyNamespaceContract"]["idempotency_collision_count"] == 0
    )
    projection["health"]["execution_lease_ok"] = (
        contract_evidence["ExecutionLeaseContract"]["eligible_execution_lease_records"] > 0
        and contract_evidence["ExecutionLeaseContract"]["malformed_count"] == 0
        and contract_evidence["ExecutionLeaseContract"]["lease_violation_count"] == 0
    )
    projection["health"]["state_version_fencing_ok"] = (
        contract_evidence["StateVersionFencing"]["eligible_state_fencing_records"] > 0
        and contract_evidence["StateVersionFencing"]["malformed_count"] == 0
        and contract_evidence["StateVersionFencing"]["fencing_violation_count"] == 0
    )
    projection["health"]["entry_execution_state_machine_ok"] = (
        contract_evidence["EntryExecutionStateMachine"]["eligible_entry_execution_records"] > 0
        and contract_evidence["EntryExecutionStateMachine"]["terminal_state_count"] > 0
        and contract_evidence["EntryExecutionStateMachine"]["malformed_count"] == 0
        and contract_evidence["EntryExecutionStateMachine"]["state_machine_violation_count"] == 0
    )
    projection["health"]["paper_position_ledger_ok"] = (
        contract_evidence["PaperPositionLedgerContract"]["eligible_position_ledger_records"] > 0
        and contract_evidence["PaperPositionLedgerContract"]["malformed_count"] == 0
        and contract_evidence["PaperPositionLedgerContract"]["position_ledger_violation_count"] == 0
    )
    projection["health"]["paper_capital_ledger_ok"] = (
        contract_evidence["PaperCapitalLedgerContract"]["eligible_capital_ledger_records"] > 0
        and contract_evidence["PaperCapitalLedgerContract"]["malformed_count"] == 0
        and contract_evidence["PaperCapitalLedgerContract"]["capital_ledger_violation_count"] == 0
    )
    projection["health"]["double_entry_ledger_invariant_ok"] = (
        contract_evidence["DoubleEntryLedgerInvariantContract"]["eligible_double_entry_records"] > 0
        and contract_evidence["DoubleEntryLedgerInvariantContract"]["invariant_violation_count"] == 0
    )
    projection["health"]["capital_reservation_policy_ok"] = (
        contract_evidence["CapitalReservationPolicy"]["eligible_reservation_records"] > 0
        and contract_evidence["CapitalReservationPolicy"]["malformed_count"] == 0
        and contract_evidence["CapitalReservationPolicy"]["reservation_policy_violation_count"] == 0
    )
    projection["health"]["no_fill_outcome_ok"] = (
        contract_evidence["NoFillOutcome"]["eligible_no_fill_records"] > 0
        and contract_evidence["NoFillOutcome"]["terminal_outcome_count"] > 0
        and contract_evidence["NoFillOutcome"]["malformed_count"] == 0
        and contract_evidence["NoFillOutcome"]["no_fill_outcome_violation_count"] == 0
    )
    projection["health"]["crash_recovery_state_machine_ok"] = (
        contract_evidence["CrashRecoveryStateMachine"]["eligible_recovery_records"] > 0
        and contract_evidence["CrashRecoveryStateMachine"]["malformed_count"] == 0
        and contract_evidence["CrashRecoveryStateMachine"]["recovery_violation_count"] == 0
    )
    projection["health"]["resume_drain_policy_ok"] = (
        contract_evidence["ResumeDrainPolicy"]["eligible_resume_drain_records"] > 0
        and contract_evidence["ResumeDrainPolicy"]["malformed_count"] == 0
        and contract_evidence["ResumeDrainPolicy"]["resume_drain_violation_count"] == 0
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
