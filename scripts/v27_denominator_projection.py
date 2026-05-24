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

from v27_event_log import HEAVY_PAYLOAD_FIELDS, V27EventLog, V27EventLogError, sha256_hex  # noqa: E402


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
RAW_PROVIDER_EVIDENCE_EVENT_TYPE = "raw_provider_evidence_recorded"
IDEMPOTENCY_EVENT_TYPE = "idempotency_contract_recorded"
EXECUTION_CONTROL_EVENT_TYPE = "execution_control_recorded"
PAPER_LEDGER_EVENT_TYPE = "paper_ledger_recorded"
NO_FILL_OUTCOME_EVENT_TYPE = "no_fill_outcome_recorded"
RUNTIME_RECOVERY_EVENT_TYPE = "runtime_recovery_control_recorded"
RANDOMNESS_CONTROL_EVENT_TYPE = "randomness_control_recorded"
DEPLOYMENT_ROLLOUT_EVENT_TYPE = "deployment_rollout_state_recorded"
WORKER_FLEET_EVENT_TYPE = "worker_fleet_heartbeat_recorded"
BACKUP_RESTORE_DRILL_EVENT_TYPE = "backup_restore_drill_recorded"
INCIDENT_EVIDENCE_FREEZE_EVENT_TYPE = "incident_evidence_freeze_recorded"
CIRCUIT_BREAKER_RESUME_EVENT_TYPE = "circuit_breaker_resume_recorded"
QUEUE_DURABILITY_EVENT_TYPE = "queue_durability_recorded"
CANDIDATE_CANCELLATION_EVENT_TYPE = "candidate_cancellation_recorded"
RETRY_STORM_CONTROL_EVENT_TYPE = "retry_storm_control_recorded"
PROVIDER_COVERAGE_MAP_EVENT_TYPE = "provider_coverage_map_recorded"
TRAINING_SERVING_SKEW_EVENT_TYPE = "training_serving_skew_recorded"
OUTCOME_WINDOW_CLOSE_VERSION = "v2.7.0.outcome_window_close.v2"
LEGACY_OUTCOME_WINDOW_ORDER_TOLERANCE_SEC = 1.0
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
    RAW_PROVIDER_EVIDENCE_EVENT_TYPE,
    IDEMPOTENCY_EVENT_TYPE,
    EXECUTION_CONTROL_EVENT_TYPE,
    PAPER_LEDGER_EVENT_TYPE,
    NO_FILL_OUTCOME_EVENT_TYPE,
    RUNTIME_RECOVERY_EVENT_TYPE,
    RANDOMNESS_CONTROL_EVENT_TYPE,
    DEPLOYMENT_ROLLOUT_EVENT_TYPE,
    WORKER_FLEET_EVENT_TYPE,
    BACKUP_RESTORE_DRILL_EVENT_TYPE,
    INCIDENT_EVIDENCE_FREEZE_EVENT_TYPE,
    CIRCUIT_BREAKER_RESUME_EVENT_TYPE,
    QUEUE_DURABILITY_EVENT_TYPE,
    CANDIDATE_CANCELLATION_EVENT_TYPE,
    RETRY_STORM_CONTROL_EVENT_TYPE,
    PROVIDER_COVERAGE_MAP_EVENT_TYPE,
    TRAINING_SERVING_SKEW_EVENT_TYPE,
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


def _valid_sha256_hex(value):
    if not isinstance(value, str):
        return False
    if len(value) != 64:
        return False
    return all(ch in "0123456789abcdef" for ch in value.lower())


def _extract_raw_provider_evidence_contract(event, bags):
    version = _extract_scalar(
        bags,
        [
            ("raw_provider_evidence_version",),
            ("provider_evidence_version",),
        ],
    )
    if event.get("event_type") != RAW_PROVIDER_EVIDENCE_EVENT_TYPE and not version:
        return None
    values = {
        "raw_provider_evidence_version": str(version) if version else None,
        "provider": _extract_scalar(bags, [("provider",)]),
        "endpoint": _extract_scalar(bags, [("endpoint",)]),
        "request_hash": _extract_scalar(bags, [("request_hash",)]),
        "response_hash": _extract_scalar(bags, [("response_hash",)]),
        "request_id": _extract_scalar(bags, [("request_id",), ("provider_request_id",)]),
        "provider_request_id": _extract_scalar(bags, [("provider_request_id",), ("request_id",)]),
        "latency_ms": _as_float(_extract_scalar(bags, [("latency_ms",), ("provider_latency_ms",)])),
        "side": _extract_scalar(bags, [("side",)]),
        "request_metadata_hash": _extract_scalar(bags, [("request_metadata_hash",)]),
        "request_metadata": _extract_scalar(bags, [("request_metadata",)], default={}),
        "request_parameters": _extract_scalar(bags, [("request_parameters",)], default={}),
        "request_metadata_available": _extract_flag(bags, [("request_metadata_available",)]),
        "raw_response_hash": _extract_scalar(bags, [("raw_response_hash",)]),
        "raw_response_available": _extract_flag(bags, [("raw_response_available",)]),
        "response_material_type": _extract_scalar(bags, [("response_material_type",)]),
        "hash_algorithm": _extract_scalar(bags, [("hash_algorithm",)]),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)]),
        "provider_evidence_proof_level": _extract_scalar(bags, [("provider_evidence_proof_level",)], default="unknown"),
        "provider_evidence_trusted": _extract_flag(bags, [("provider_evidence_trusted",)]),
        "decision_available_at": _extract_scalar(bags, [("decision_available_at",)], default=event.get("available_at")),
        "paper_trade_id": _extract_scalar(bags, [("paper_trade_id",), ("id",)]),
    }
    missing_fields = []
    for field in ("provider", "endpoint", "request_hash", "response_hash", "request_id", "latency_ms"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    if not version:
        missing_fields.append("raw_provider_evidence_version")
    if values.get("latency_ms") is not None and (not math.isfinite(values["latency_ms"]) or values["latency_ms"] < 0):
        missing_fields.append("latency_ms_nonnegative")
    violation_fields = []
    if not _valid_sha256_hex(values.get("request_hash")):
        violation_fields.append("request_hash_sha256")
    if not _valid_sha256_hex(values.get("response_hash")):
        violation_fields.append("response_hash_sha256")
    if values.get("request_metadata_hash") and not _valid_sha256_hex(values.get("request_metadata_hash")):
        violation_fields.append("request_metadata_hash_sha256")
    if values.get("raw_response_hash") and not _valid_sha256_hex(values.get("raw_response_hash")):
        violation_fields.append("raw_response_hash_sha256")
    if values.get("hash_algorithm") != "sha256(canonical_json)":
        violation_fields.append("hash_algorithm")
    if values.get("request_metadata_available") is not True:
        violation_fields.append("request_metadata_available")
    if values.get("raw_response_available") is not True:
        violation_fields.append("raw_response_available")
    if values.get("provider_evidence_trusted") is not True:
        violation_fields.append("provider_evidence_trusted")
    if values.get("provider_request_id") is None:
        violation_fields.append("provider_request_id")
    trusted = bool(not missing_fields and not violation_fields)
    return {
        **values,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": sorted(set(violation_fields)),
        "provider_evidence_valid": trusted,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_randomness_control(event, bags):
    version = _extract_scalar(
        bags,
        [
            ("rng_version",),
            ("randomness_control_version",),
        ],
    )
    if event.get("event_type") != RANDOMNESS_CONTROL_EVENT_TYPE and not version:
        return None
    values = {
        "rng_seed": _extract_scalar(bags, [("rng_seed",)]),
        "rng_version": str(version) if version else None,
        "randomization_unit": _extract_scalar(bags, [("randomization_unit",)]),
        "assignment_id": _extract_scalar(bags, [("assignment_id",)]),
        "assignment_status": _extract_scalar(bags, [("assignment_status",), ("status",)]),
        "randomization_enabled": _extract_flag(bags, [("randomization_enabled",)]),
        "deterministic_assignment": _extract_flag(bags, [("deterministic_assignment",)]),
        "assignment_algorithm": _extract_scalar(bags, [("assignment_algorithm",)]),
        "assigned_bucket": _extract_scalar(bags, [("assigned_bucket",), ("experiment_bucket",)]),
        "assignment_hash": _extract_scalar(bags, [("assignment_hash",)]),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)], default=event.get("source")),
        "decision_available_at": _extract_scalar(bags, [("decision_available_at",)], default=event.get("available_at")),
    }
    missing_fields = []
    for field in ("rng_seed", "rng_version", "randomization_unit", "assignment_id"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    violation_fields = []
    if values.get("assignment_hash") and not _valid_sha256_hex(values.get("assignment_hash")):
        violation_fields.append("assignment_hash_sha256")
    return {
        **values,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": sorted(set(violation_fields)),
        "randomness_control_valid": not missing_fields and not violation_fields,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _latest_randomness_controls(randomness_controls):
    latest_by_assignment = {}
    passthrough = []
    for item in randomness_controls or []:
        assignment_id = item.get("assignment_id")
        if assignment_id is None or (isinstance(assignment_id, str) and not assignment_id.strip()):
            passthrough.append(item)
            continue
        current = latest_by_assignment.get(str(assignment_id))
        if current is None or int(item.get("global_seq") or 0) >= int(current.get("global_seq") or 0):
            latest_by_assignment[str(assignment_id)] = item
    return passthrough + [
        latest_by_assignment[key]
        for key in sorted(latest_by_assignment)
    ]


def _extract_deployment_rollout(event, bags):
    if event.get("event_type") != DEPLOYMENT_ROLLOUT_EVENT_TYPE:
        return None
    fleet_hash_map = _extract_scalar(bags, [("fleet_hash_map",)], default={})
    values = {
        "rollout_id": _extract_scalar(bags, [("rollout_id",)]),
        "state": _extract_scalar(bags, [("state",), ("rollout_state",)]),
        "fleet_hash_map": fleet_hash_map,
        "canary_status": _extract_scalar(bags, [("canary_status",)]),
        "build_hash": _extract_scalar(bags, [("build_hash",)]),
        "runtime_config_hash": _extract_scalar(bags, [("runtime_config_hash",)]),
        "policy_bundle_id": _extract_scalar(bags, [("policy_bundle_id",)]),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)], default=event.get("source")),
    }
    missing_fields = []
    for field in ("rollout_id", "state", "fleet_hash_map", "canary_status"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()) or (field == "fleet_hash_map" and not isinstance(value, dict)):
            missing_fields.append(field)
    violation_fields = []
    normalized_state = str(values.get("state") or "").strip().lower()
    if normalized_state not in {"completed", "rolled_out", "active", "ready"}:
        violation_fields.append("state_not_ready")
    normalized_canary = str(values.get("canary_status") or "").strip().lower()
    if normalized_canary not in {"passed", "healthy", "complete", "completed", "ok"}:
        violation_fields.append("canary_status_not_passed")
    if isinstance(fleet_hash_map, dict) and not fleet_hash_map:
        violation_fields.append("fleet_hash_map_empty")
    return {
        **values,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": sorted(set(violation_fields)),
        "deployment_rollout_valid": not missing_fields and not violation_fields,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_worker_fleet_heartbeat(event, bags):
    if event.get("event_type") != WORKER_FLEET_EVENT_TYPE:
        return None
    values = {
        "worker_id": _extract_scalar(bags, [("worker_id",)]),
        "build_hash": _extract_scalar(bags, [("build_hash",)]),
        "runtime_config_hash": _extract_scalar(bags, [("runtime_config_hash",)]),
        "policy_bundle_id": _extract_scalar(bags, [("policy_bundle_id",)]),
        "heartbeat_at": _extract_scalar(bags, [("heartbeat_at",)]),
        "role": _extract_scalar(bags, [("role",), ("worker_role",)]),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)], default=event.get("source")),
    }
    missing_fields = []
    for field in ("worker_id", "build_hash", "runtime_config_hash", "policy_bundle_id", "heartbeat_at"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    return {
        **values,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": [],
        "worker_fleet_heartbeat_valid": not missing_fields,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_backup_restore_drill(event, bags):
    if event.get("event_type") != BACKUP_RESTORE_DRILL_EVENT_TYPE:
        return None
    values = {
        "drill_id": _extract_scalar(bags, [("drill_id",)]),
        "backup_set_id": _extract_scalar(bags, [("backup_set_id",)]),
        "restored_world_hash": _extract_scalar(bags, [("restored_world_hash",)]),
        "restore_started_at": _extract_scalar(bags, [("restore_started_at",)]),
        "restore_completed_at": _extract_scalar(bags, [("restore_completed_at",)]),
        "restore_status": _extract_scalar(bags, [("restore_status",), ("status",)]),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)], default=event.get("source")),
    }
    missing_fields = []
    for field in ("drill_id", "backup_set_id", "restored_world_hash", "restore_started_at", "restore_completed_at"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    violation_fields = []
    if values.get("restored_world_hash") and not _valid_sha256_hex(values.get("restored_world_hash")):
        violation_fields.append("restored_world_hash_sha256")
    started_sec = _timestamp_epoch_seconds(values.get("restore_started_at"))
    completed_sec = _timestamp_epoch_seconds(values.get("restore_completed_at"))
    if values.get("restore_started_at") and started_sec is None:
        violation_fields.append("restore_started_at_parseable")
    if values.get("restore_completed_at") and completed_sec is None:
        violation_fields.append("restore_completed_at_parseable")
    if started_sec is not None and completed_sec is not None and completed_sec < started_sec:
        violation_fields.append("restore_completed_before_started")
    if values.get("restore_status") and str(values.get("restore_status")).strip().lower() not in {"passed", "verified", "complete", "completed", "ok"}:
        violation_fields.append("restore_status_not_passed")
    return {
        **values,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": sorted(set(violation_fields)),
        "backup_restore_drill_valid": not missing_fields and not violation_fields,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_incident_evidence_freeze(event, bags):
    if event.get("event_type") != INCIDENT_EVIDENCE_FREEZE_EVENT_TYPE:
        return None
    frozen_event_range = _extract_scalar(bags, [("frozen_event_range",)], default={})
    values = {
        "freeze_id": _extract_scalar(bags, [("freeze_id",)]),
        "incident_id": _extract_scalar(bags, [("incident_id",)]),
        "frozen_event_range": frozen_event_range,
        "frozen_config_hash": _extract_scalar(bags, [("frozen_config_hash",)]),
        "frozen_at": _extract_scalar(bags, [("frozen_at",)]),
        "freeze_status": _extract_scalar(bags, [("freeze_status",), ("status",)]),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)], default=event.get("source")),
    }
    missing_fields = []
    for field in ("freeze_id", "incident_id", "frozen_event_range", "frozen_config_hash", "frozen_at"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()) or (field == "frozen_event_range" and not isinstance(value, dict)):
            missing_fields.append(field)
    violation_fields = []
    if values.get("frozen_config_hash") and not _valid_sha256_hex(values.get("frozen_config_hash")):
        violation_fields.append("frozen_config_hash_sha256")
    frozen_at_sec = _timestamp_epoch_seconds(values.get("frozen_at"))
    if values.get("frozen_at") and frozen_at_sec is None:
        violation_fields.append("frozen_at_parseable")
    if isinstance(frozen_event_range, dict):
        start_seq = _as_int(frozen_event_range.get("start_seq"), default=-1)
        end_seq = _as_int(frozen_event_range.get("end_seq"), default=-1)
        if start_seq < 0 or end_seq < 0:
            violation_fields.append("frozen_event_range_seq_required")
        elif end_seq < start_seq:
            violation_fields.append("frozen_event_range_inverted")
    if values.get("freeze_status") and str(values.get("freeze_status")).strip().lower() not in {"frozen", "sealed", "complete", "completed", "ok"}:
        violation_fields.append("freeze_status_not_frozen")
    return {
        **values,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": sorted(set(violation_fields)),
        "incident_evidence_freeze_valid": not missing_fields and not violation_fields,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_circuit_breaker_resume(event, bags):
    if event.get("event_type") != CIRCUIT_BREAKER_RESUME_EVENT_TYPE:
        return None
    root_cause_fixed = _extract_flag(bags, [("root_cause_fixed",)])
    health_checks_passed = _extract_flag(bags, [("health_checks_passed",)])
    values = {
        "breaker_id": _extract_scalar(bags, [("breaker_id",)]),
        "root_cause_fixed": root_cause_fixed,
        "evidence_freeze_id": _extract_scalar(bags, [("evidence_freeze_id",), ("freeze_id",)]),
        "health_checks_passed": health_checks_passed,
        "resumed_at": _extract_scalar(bags, [("resumed_at",)]),
        "resume_status": _extract_scalar(bags, [("resume_status",), ("status",)]),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)], default=event.get("source")),
    }
    missing_fields = []
    for field in ("breaker_id", "root_cause_fixed", "evidence_freeze_id", "health_checks_passed", "resumed_at"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    violation_fields = []
    if root_cause_fixed is False:
        violation_fields.append("root_cause_not_fixed")
    if health_checks_passed is False:
        violation_fields.append("health_checks_not_passed")
    resumed_at_sec = _timestamp_epoch_seconds(values.get("resumed_at"))
    if values.get("resumed_at") and resumed_at_sec is None:
        violation_fields.append("resumed_at_parseable")
    if values.get("resume_status") and str(values.get("resume_status")).strip().lower() not in {"resumed", "complete", "completed", "ok"}:
        violation_fields.append("resume_status_not_resumed")
    return {
        **values,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": sorted(set(violation_fields)),
        "circuit_breaker_resume_valid": not missing_fields and not violation_fields,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_queue_durability(event, bags):
    if event.get("event_type") != QUEUE_DURABILITY_EVENT_TYPE:
        return None
    values = {
        "queue_id": _extract_scalar(bags, [("queue_id",)]),
        "task_id": _extract_scalar(bags, [("task_id",)]),
        "durable_state": _extract_scalar(bags, [("durable_state",)]),
        "ack_state": _extract_scalar(bags, [("ack_state",)]),
        "created_at": _extract_scalar(bags, [("created_at",)]),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)], default=event.get("source")),
    }
    missing_fields = []
    for field in ("queue_id", "task_id", "durable_state", "ack_state", "created_at"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    violation_fields = []
    durable_state = str(values.get("durable_state") or "").strip().lower()
    if durable_state not in {"persisted", "durable", "stored", "committed"}:
        violation_fields.append("durable_state_not_durable")
    ack_state = str(values.get("ack_state") or "").strip().lower()
    if ack_state not in {"pending", "acked", "nacked", "retrying"}:
        violation_fields.append("ack_state_invalid")
    if values.get("created_at") and _timestamp_epoch_seconds(values.get("created_at")) is None:
        violation_fields.append("created_at_parseable")
    return {
        **values,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": sorted(set(violation_fields)),
        "queue_durability_valid": not missing_fields and not violation_fields,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_candidate_cancellation(event, bags):
    if event.get("event_type") != CANDIDATE_CANCELLATION_EVENT_TYPE:
        return None
    values = {
        "candidate_id": _extract_scalar(bags, [("candidate_id",)]),
        "cancel_reason": _extract_scalar(bags, [("cancel_reason",)]),
        "cancel_event_seq": _extract_scalar(bags, [("cancel_event_seq",)]),
        "cancelled_at": _extract_scalar(bags, [("cancelled_at",)]),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)], default=event.get("source")),
    }
    missing_fields = []
    for field in ("candidate_id", "cancel_reason", "cancel_event_seq", "cancelled_at"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    violation_fields = []
    cancel_event_seq = _as_int(values.get("cancel_event_seq"), default=-1)
    if values.get("cancel_event_seq") is not None and cancel_event_seq < 0:
        violation_fields.append("cancel_event_seq_nonnegative")
    if values.get("cancelled_at") and _timestamp_epoch_seconds(values.get("cancelled_at")) is None:
        violation_fields.append("cancelled_at_parseable")
    return {
        **values,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": sorted(set(violation_fields)),
        "candidate_cancellation_valid": not missing_fields and not violation_fields,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_retry_storm_control(event, bags):
    if event.get("event_type") != RETRY_STORM_CONTROL_EVENT_TYPE:
        return None
    values = {
        "retry_family": _extract_scalar(bags, [("retry_family",)]),
        "backoff_policy": _extract_scalar(bags, [("backoff_policy",)]),
        "max_concurrent_retries": _extract_scalar(bags, [("max_concurrent_retries",)]),
        "p0_reserved_capacity": _extract_scalar(bags, [("p0_reserved_capacity",)]),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)], default=event.get("source")),
    }
    missing_fields = []
    for field in ("retry_family", "backoff_policy", "max_concurrent_retries", "p0_reserved_capacity"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    violation_fields = []
    backoff_policy = str(values.get("backoff_policy") or "").strip().lower()
    if backoff_policy not in {"exponential_jitter", "capped_exponential_jitter", "linear_jitter", "fixed_jitter"}:
        violation_fields.append("backoff_policy_not_bounded")
    max_concurrent_retries = _as_int(values.get("max_concurrent_retries"), default=-1)
    if values.get("max_concurrent_retries") is not None and max_concurrent_retries < 0:
        violation_fields.append("max_concurrent_retries_nonnegative")
    p0_reserved_capacity = _as_int(values.get("p0_reserved_capacity"), default=-1)
    if values.get("p0_reserved_capacity") is not None and p0_reserved_capacity < 0:
        violation_fields.append("p0_reserved_capacity_nonnegative")
    return {
        **values,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": sorted(set(violation_fields)),
        "retry_storm_control_valid": not missing_fields and not violation_fields,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_provider_coverage_map(event, bags):
    if event.get("event_type") != PROVIDER_COVERAGE_MAP_EVENT_TYPE:
        return None
    values = {
        "provider": _extract_scalar(bags, [("provider",)]),
        "chain": _extract_scalar(bags, [("chain",)]),
        "pool_type": _extract_scalar(bags, [("pool_type",), ("pool_kind",)]),
        "coverage_status": _extract_scalar(bags, [("coverage_status",)]),
        "unsupported_reason": _extract_scalar(bags, [("unsupported_reason",)]),
        "coverage_map_version": _extract_scalar(bags, [("coverage_map_version",)]),
        "checked_at": _extract_scalar(bags, [("checked_at",)], default=event.get("available_at")),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)], default=event.get("source")),
    }
    missing_fields = []
    for field in ("provider", "chain", "pool_type", "coverage_status", "unsupported_reason"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    violation_fields = []
    coverage_status = str(values.get("coverage_status") or "").strip().lower()
    if coverage_status not in {"supported", "unsupported", "partial", "degraded", "unknown"}:
        violation_fields.append("coverage_status_unknown")
    if values.get("checked_at") and _timestamp_epoch_seconds(values.get("checked_at")) is None:
        violation_fields.append("checked_at_parseable")
    coverage_key = ":".join(
        [
            str(values.get("provider") or "unknown_provider"),
            str(values.get("chain") or "unknown_chain"),
            str(values.get("pool_type") or "unknown_pool_type"),
        ]
    )
    return {
        **values,
        "coverage_key": coverage_key,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": sorted(set(violation_fields)),
        "provider_coverage_map_valid": not missing_fields and not violation_fields,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _extract_training_serving_skew(event, bags):
    if event.get("event_type") != TRAINING_SERVING_SKEW_EVENT_TYPE:
        return None
    values = {
        "training_feature_code_hash": _extract_scalar(bags, [("training_feature_code_hash",)]),
        "serving_feature_code_hash": _extract_scalar(bags, [("serving_feature_code_hash",)]),
        "normalization_version": _extract_scalar(bags, [("normalization_version",)]),
        "skew_check_result": _extract_scalar(bags, [("skew_check_result",)]),
        "feature_set_id": _extract_scalar(bags, [("feature_set_id",)]),
        "checked_at": _extract_scalar(bags, [("checked_at",)], default=event.get("available_at")),
        "training_artifact_id": _extract_scalar(bags, [("training_artifact_id",)]),
        "serving_artifact_id": _extract_scalar(bags, [("serving_artifact_id",)]),
        "evidence_source": _extract_scalar(bags, [("evidence_source",)], default=event.get("source")),
    }
    missing_fields = []
    for field in ("training_feature_code_hash", "serving_feature_code_hash", "normalization_version", "skew_check_result"):
        value = values.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing_fields.append(field)
    violation_fields = []
    if values.get("training_feature_code_hash") and not _valid_sha256_hex(values.get("training_feature_code_hash")):
        violation_fields.append("training_feature_code_hash_sha256")
    if values.get("serving_feature_code_hash") and not _valid_sha256_hex(values.get("serving_feature_code_hash")):
        violation_fields.append("serving_feature_code_hash_sha256")
    skew_check_result = str(values.get("skew_check_result") or "").strip().lower()
    if skew_check_result not in {"pass", "passed", "ok", "matched", "equivalent", "within_tolerance", "no_skew"}:
        violation_fields.append("skew_check_result_not_passed")
    if values.get("checked_at") and _timestamp_epoch_seconds(values.get("checked_at")) is None:
        violation_fields.append("checked_at_parseable")
    skew_key = ":".join(
        [
            str(values.get("feature_set_id") or "default_feature_set"),
            str(values.get("normalization_version") or "unknown_normalization"),
        ]
    )
    return {
        **values,
        "skew_key": skew_key,
        "missing_fields": sorted(set(missing_fields)),
        "violation_fields": sorted(set(violation_fields)),
        "training_serving_skew_valid": not missing_fields and not violation_fields,
        "source_event_id": event.get("event_id"),
        "global_seq": event.get("global_seq"),
    }


def _latest_by_key(items, key_name):
    latest = {}
    passthrough = []
    for item in items or []:
        key = item.get(key_name)
        if key is None or (isinstance(key, str) and not key.strip()):
            passthrough.append(item)
            continue
        current = latest.get(str(key))
        if current is None or int(item.get("global_seq") or 0) >= int(current.get("global_seq") or 0):
            latest[str(key)] = item
    return passthrough + [latest[key] for key in sorted(latest)]


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
        "source_label_id": outer.get("source_label_id") or _extract_scalar(bags, [("source_label_id",)]),
        "source_dog_label": source_label,
        "source_label_research_only": _extract_flag(bags, [("source_label_research_only",), ("source_dog_label_research_only",)]),
        "reference_price_contract": _extract_reference_price_contract(event, bags),
        "trade_outcome_label_contract": _extract_trade_outcome_label_contract(event, bags),
        "standardized_stop_contract": _extract_standardized_stop_contract(event, bags),
        "ex_ante_feasibility_contract": _extract_ex_ante_feasibility_contract(event, bags),
        "earliest_actionable_time_contract": _extract_earliest_actionable_time_contract(event, bags),
        "realtime_clean_contract": _extract_realtime_clean_contract(event, bags),
        "quote_intent_binding_contract": _extract_quote_intent_binding_contract(event, bags),
        "raw_provider_evidence_contract": _extract_raw_provider_evidence_contract(event, bags),
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
        "raw_provider_evidence_candidates": [],
        "idempotency_contract_candidates": [],
        "execution_control_candidates": [],
        "paper_ledger_candidates": [],
        "no_fill_outcome_candidates": [],
        "source_label_research_only": False,
        "denominator_dirty_reasons": [],
        "source_dog_label": None,
        "source_label_id": None,
        "source_label_ids": [],
        "signal_credit_assignment": None,
        "reference_price_contract": None,
        "trade_outcome_label": None,
        "label_finalization_contract": None,
        "outcome_window_close_contract": None,
        "standardized_stop_contract": None,
        "ex_ante_feasibility_contract": None,
        "earliest_actionable_time": None,
        "realtime_clean_contract": None,
        "quote_intent_binding_contract": None,
        "raw_provider_evidence_contract": None,
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
    source_label_id = fact.get("source_label_id")
    if source_label_id is not None:
        if source_label_id not in record["source_label_ids"]:
            record["source_label_ids"].append(source_label_id)
        if record.get("source_label_id") is None:
            record["source_label_id"] = source_label_id
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
    raw_provider_evidence = fact.get("raw_provider_evidence_contract")
    if raw_provider_evidence:
        record["raw_provider_evidence_candidates"].append(raw_provider_evidence)
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


def _finalize_label_finalization(record):
    trade_outcome_label = record.get("trade_outcome_label")
    if not trade_outcome_label:
        record["label_finalization_contract"] = None
        return
    paper_trade_id = trade_outcome_label.get("paper_trade_id")
    label_id = f"paper_trade:{paper_trade_id}:trade_outcome_label" if paper_trade_id is not None else None
    source_label_ids = sorted(
        {
            source_label_id
            for source_label_id in (record.get("source_label_ids") or [])
            if source_label_id is not None
        },
        key=lambda value: str(value),
    )
    supersedes_label_id = None
    if source_label_ids:
        supersedes_label_id = f"source_label:{source_label_ids[0]}"
    elif paper_trade_id is not None:
        supersedes_label_id = f"source_label:{paper_trade_id}"
    outcome_window_closed_at = trade_outcome_label.get("trade_label_available_at") or trade_outcome_label.get("exit_ts")
    label_status = "final" if outcome_window_closed_at is not None else None
    missing_fields = []
    if not label_id:
        missing_fields.append("label_id")
    if not label_status:
        missing_fields.append("label_status")
    if outcome_window_closed_at is None:
        missing_fields.append("outcome_window_closed_at")
    if not supersedes_label_id:
        missing_fields.append("supersedes_label_id")
    record["label_finalization_contract"] = {
        "label_id": label_id,
        "label_status": label_status,
        "outcome_window_closed_at": outcome_window_closed_at,
        "supersedes_label_id": supersedes_label_id,
        "label_finalization_version": "v2.7.0.label_finalization.v1",
        "label_finalization_quality": "trade_outcome_label_finalized_from_paper_trade",
        "paper_trade_id": paper_trade_id,
        "source_label_id": source_label_ids[0] if source_label_ids else record.get("source_label_id"),
        "trade_outcome_label_source_event_id": trade_outcome_label.get("source_event_id"),
        "trade_outcome_label_global_seq": trade_outcome_label.get("global_seq"),
        "missing_fields": missing_fields,
    }


def _finalize_outcome_window_close(record):
    trade_outcome_label = record.get("trade_outcome_label")
    if not trade_outcome_label:
        record["outcome_window_close_contract"] = None
        return
    paper_trade_id = trade_outcome_label.get("paper_trade_id")
    label_id = f"paper_trade:{paper_trade_id}:trade_outcome_label" if paper_trade_id is not None else None
    window_start = trade_outcome_label.get("counterfactual_entry_ts") or trade_outcome_label.get("simulated_fill_ts")
    window_end = trade_outcome_label.get("trade_label_available_at") or trade_outcome_label.get("exit_ts")
    window_closed_at = window_end
    missing_fields = []
    if not label_id:
        missing_fields.append("label_id")
    if window_start is None:
        missing_fields.append("window_start")
    if window_end is None:
        missing_fields.append("window_end")
    if window_closed_at is None:
        missing_fields.append("window_closed_at")
    window_order_ok = True
    window_order_delta_sec = None
    window_order_tolerance_applied_sec = 0.0
    try:
        if window_start is not None and window_end is not None:
            window_order_delta_sec = float(window_end) - float(window_start)
            window_order_ok = window_order_delta_sec >= 0
            if (
                not window_order_ok
                and trade_outcome_label.get("trade_outcome_label_quality") == "legacy_paper_trade_view"
                and abs(window_order_delta_sec) <= LEGACY_OUTCOME_WINDOW_ORDER_TOLERANCE_SEC
            ):
                window_order_ok = True
                window_order_tolerance_applied_sec = abs(window_order_delta_sec)
    except (TypeError, ValueError):
        window_order_ok = True
    record["outcome_window_close_contract"] = {
        "label_id": label_id,
        "window_start": window_start,
        "window_end": window_end,
        "window_closed_at": window_closed_at,
        "outcome_window_close_version": OUTCOME_WINDOW_CLOSE_VERSION,
        "outcome_window_close_quality": "trade_outcome_label_window_close_proxy",
        "paper_trade_id": paper_trade_id,
        "trade_outcome_label_source_event_id": trade_outcome_label.get("source_event_id"),
        "trade_outcome_label_global_seq": trade_outcome_label.get("global_seq"),
        "window_order_ok": window_order_ok,
        "window_order_delta_sec": window_order_delta_sec,
        "window_order_tolerance_sec": LEGACY_OUTCOME_WINDOW_ORDER_TOLERANCE_SEC,
        "window_order_tolerance_applied_sec": window_order_tolerance_applied_sec,
        "missing_fields": missing_fields,
    }


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


def _finalize_raw_provider_evidence(record):
    candidates = sorted(
        record.get("raw_provider_evidence_candidates") or [],
        key=lambda item: (item.get("global_seq") or 0, str(item.get("source_event_id") or "")),
    )
    record["raw_provider_evidence_candidates"] = candidates
    if not candidates:
        record["raw_provider_evidence_contract"] = None
        return
    trusted_candidates = [candidate for candidate in candidates if candidate.get("provider_evidence_valid") is True]
    record["raw_provider_evidence_contract"] = trusted_candidates[0] if trusted_candidates else candidates[0]


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
    if record.get("trade_outcome_label") and not record.get("label_finalization_contract"):
        missing.append("LabelFinalizationContract")
    if record.get("trade_outcome_label") and not record.get("outcome_window_close_contract"):
        missing.append("OutcomeWindowCloseContract")
    if record.get("captured") and not record.get("standardized_stop_contract"):
        missing.append("StandardizedStopContract")
    if record.get("realtime_clean_contract") and not record.get("quote_intent_binding_contract"):
        missing.append("QuoteIntentBindingContract")
    if record.get("quote_intent_binding_contract") and not record.get("raw_provider_evidence_contract"):
        missing.append("RawProviderEvidenceContract")
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


def _contract_evidence_from_records(
    record_list,
    runtime_recovery_controls=None,
    standalone_no_fill_outcomes=None,
    randomness_controls=None,
    deployment_rollouts=None,
    worker_fleet_heartbeats=None,
    backup_restore_drills=None,
    incident_evidence_freezes=None,
    circuit_breaker_resumes=None,
    queue_durability_records=None,
    candidate_cancellations=None,
    retry_storm_controls=None,
    provider_coverage_maps=None,
    training_serving_skews=None,
):
    runtime_recovery_controls = runtime_recovery_controls or []
    standalone_no_fill_outcomes = standalone_no_fill_outcomes or []
    raw_randomness_control_count = len(randomness_controls or [])
    randomness_controls = _latest_randomness_controls(randomness_controls or [])
    superseded_randomness_control_count = max(0, raw_randomness_control_count - len(randomness_controls))
    raw_deployment_rollout_count = len(deployment_rollouts or [])
    deployment_rollouts = _latest_by_key(deployment_rollouts or [], "rollout_id")
    superseded_deployment_rollout_count = max(0, raw_deployment_rollout_count - len(deployment_rollouts))
    raw_worker_fleet_count = len(worker_fleet_heartbeats or [])
    worker_fleet_heartbeats = _latest_by_key(worker_fleet_heartbeats or [], "worker_id")
    superseded_worker_fleet_count = max(0, raw_worker_fleet_count - len(worker_fleet_heartbeats))
    raw_backup_restore_drill_count = len(backup_restore_drills or [])
    backup_restore_drills = _latest_by_key(backup_restore_drills or [], "drill_id")
    superseded_backup_restore_drill_count = max(0, raw_backup_restore_drill_count - len(backup_restore_drills))
    raw_incident_evidence_freeze_count = len(incident_evidence_freezes or [])
    incident_evidence_freezes = _latest_by_key(incident_evidence_freezes or [], "freeze_id")
    superseded_incident_evidence_freeze_count = max(0, raw_incident_evidence_freeze_count - len(incident_evidence_freezes))
    raw_circuit_breaker_resume_count = len(circuit_breaker_resumes or [])
    circuit_breaker_resumes = _latest_by_key(circuit_breaker_resumes or [], "breaker_id")
    superseded_circuit_breaker_resume_count = max(0, raw_circuit_breaker_resume_count - len(circuit_breaker_resumes))
    raw_queue_durability_count = len(queue_durability_records or [])
    queue_durability_records = _latest_by_key(queue_durability_records or [], "task_id")
    superseded_queue_durability_count = max(0, raw_queue_durability_count - len(queue_durability_records))
    raw_candidate_cancellation_count = len(candidate_cancellations or [])
    candidate_cancellations = _latest_by_key(candidate_cancellations or [], "candidate_id")
    superseded_candidate_cancellation_count = max(0, raw_candidate_cancellation_count - len(candidate_cancellations))
    raw_retry_storm_control_count = len(retry_storm_controls or [])
    retry_storm_controls = _latest_by_key(retry_storm_controls or [], "retry_family")
    superseded_retry_storm_control_count = max(0, raw_retry_storm_control_count - len(retry_storm_controls))
    raw_provider_coverage_map_count = len(provider_coverage_maps or [])
    provider_coverage_maps = _latest_by_key(provider_coverage_maps or [], "coverage_key")
    superseded_provider_coverage_map_count = max(0, raw_provider_coverage_map_count - len(provider_coverage_maps))
    raw_training_serving_skew_count = len(training_serving_skews or [])
    training_serving_skews = _latest_by_key(training_serving_skews or [], "skew_key")
    superseded_training_serving_skew_count = max(0, raw_training_serving_skew_count - len(training_serving_skews))
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
    label_finalization_records = [
        record
        for record in record_list
        if record.get("label_finalization_contract")
    ]
    malformed_label_finalizations = []
    for record in label_finalization_records:
        malformed = [
            item
            for item in [record.get("label_finalization_contract")]
            if item and (item.get("missing_fields") or item.get("label_status") != "final")
        ]
        if malformed:
            malformed_label_finalizations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for item in malformed for field in item.get("missing_fields", [])}),
                    "label_statuses": sorted({item.get("label_status") for item in malformed if item.get("label_status")}),
                }
            )
    outcome_window_close_records = [
        record
        for record in record_list
        if record.get("outcome_window_close_contract")
    ]
    malformed_outcome_window_closes = []
    outcome_window_close_violations = []
    for record in outcome_window_close_records:
        malformed = [
            item
            for item in [record.get("outcome_window_close_contract")]
            if item and item.get("missing_fields")
        ]
        if malformed:
            malformed_outcome_window_closes.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for item in malformed for field in item.get("missing_fields", [])}),
                }
            )
        violated = [
            item
            for item in [record.get("outcome_window_close_contract")]
            if item and item.get("window_order_ok") is not True
        ]
        if violated:
            outcome_window_close_violations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "violation_count": len(violated),
                    "paper_trade_ids": [item.get("paper_trade_id") for item in violated],
                    "source_event_ids": [item.get("trade_outcome_label_source_event_id") for item in violated],
                    "trade_outcome_label_global_seqs": [item.get("trade_outcome_label_global_seq") for item in violated],
                    "windows": [
                        {
                            "window_start": item.get("window_start"),
                            "window_end": item.get("window_end"),
                            "window_closed_at": item.get("window_closed_at"),
                        }
                        for item in violated
                    ],
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
    raw_provider_evidence_records = [
        record
        for record in record_list
        if record.get("raw_provider_evidence_candidates")
    ]
    all_raw_provider_candidates = [
        item
        for record in raw_provider_evidence_records
        for item in record.get("raw_provider_evidence_candidates") or []
    ]
    malformed_raw_provider_evidence = []
    raw_provider_evidence_violations = []
    for record in raw_provider_evidence_records:
        malformed = [
            item
            for item in record.get("raw_provider_evidence_candidates") or []
            if item.get("missing_fields")
        ]
        if malformed:
            malformed_raw_provider_evidence.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "malformed_count": len(malformed),
                    "missing_fields": sorted({field for item in malformed for field in item.get("missing_fields", [])}),
                }
            )
        violated = [
            item
            for item in record.get("raw_provider_evidence_candidates") or []
            if item.get("violation_fields")
        ]
        if violated:
            raw_provider_evidence_violations.append(
                {
                    "denominator_dedup_key": record.get("denominator_dedup_key"),
                    "violation_count": len(violated),
                    "violation_fields": sorted({field for item in violated for field in item.get("violation_fields", [])}),
                    "source_event_ids": [item.get("source_event_id") for item in violated],
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
    no_fill_records = no_fill_records + [
        {
            "denominator_dedup_key": item.get("denominator_dedup_key"),
            "no_fill_outcome_candidates": [item],
        }
        for item in standalone_no_fill_outcomes
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
    malformed_randomness_controls = [
        item
        for item in randomness_controls
        if item.get("missing_fields")
    ]
    randomness_control_violations = [
        item
        for item in randomness_controls
        if item.get("missing_fields") or item.get("violation_fields")
    ]
    malformed_deployment_rollouts = [
        item
        for item in deployment_rollouts
        if item.get("missing_fields")
    ]
    deployment_rollout_violations = [
        item
        for item in deployment_rollouts
        if item.get("missing_fields") or item.get("violation_fields")
    ]
    malformed_worker_fleet_heartbeats = [
        item
        for item in worker_fleet_heartbeats
        if item.get("missing_fields")
    ]
    malformed_backup_restore_drills = [
        item
        for item in backup_restore_drills
        if item.get("missing_fields")
    ]
    backup_restore_drill_violations = [
        item
        for item in backup_restore_drills
        if item.get("missing_fields") or item.get("violation_fields")
    ]
    malformed_incident_evidence_freezes = [
        item
        for item in incident_evidence_freezes
        if item.get("missing_fields")
    ]
    incident_evidence_freeze_violations = [
        item
        for item in incident_evidence_freezes
        if item.get("missing_fields") or item.get("violation_fields")
    ]
    valid_freeze_ids = {
        item.get("freeze_id")
        for item in incident_evidence_freezes
        if item.get("incident_evidence_freeze_valid") is True and item.get("freeze_id")
    }
    malformed_circuit_breaker_resumes = [
        item
        for item in circuit_breaker_resumes
        if item.get("missing_fields")
    ]
    circuit_breaker_resume_violations = [
        item
        for item in circuit_breaker_resumes
        if item.get("missing_fields") or item.get("violation_fields")
    ]
    circuit_breaker_resume_reference_violations = [
        {
            "breaker_id": item.get("breaker_id"),
            "evidence_freeze_id": item.get("evidence_freeze_id"),
            "violation_fields": ["evidence_freeze_id_not_frozen"],
            "source_event_id": item.get("source_event_id"),
        }
        for item in circuit_breaker_resumes
        if item.get("evidence_freeze_id") and item.get("evidence_freeze_id") not in valid_freeze_ids
    ]
    malformed_queue_durability = [
        item
        for item in queue_durability_records
        if item.get("missing_fields")
    ]
    queue_durability_violations = [
        item
        for item in queue_durability_records
        if item.get("missing_fields") or item.get("violation_fields")
    ]
    malformed_candidate_cancellations = [
        item
        for item in candidate_cancellations
        if item.get("missing_fields")
    ]
    candidate_cancellation_violations = [
        item
        for item in candidate_cancellations
        if item.get("missing_fields") or item.get("violation_fields")
    ]
    malformed_retry_storm_controls = [
        item
        for item in retry_storm_controls
        if item.get("missing_fields")
    ]
    retry_storm_control_violations = [
        item
        for item in retry_storm_controls
        if item.get("missing_fields") or item.get("violation_fields")
    ]
    malformed_provider_coverage_maps = [
        item
        for item in provider_coverage_maps
        if item.get("missing_fields")
    ]
    provider_coverage_map_violations = [
        item
        for item in provider_coverage_maps
        if item.get("missing_fields") or item.get("violation_fields")
    ]
    malformed_training_serving_skews = [
        item
        for item in training_serving_skews
        if item.get("missing_fields")
    ]
    training_serving_skew_violations = [
        item
        for item in training_serving_skews
        if item.get("missing_fields") or item.get("violation_fields")
    ]
    worker_fleet_hashes = {
        "build_hashes": sorted({item.get("build_hash") for item in worker_fleet_heartbeats if item.get("build_hash")}),
        "runtime_config_hashes": sorted({item.get("runtime_config_hash") for item in worker_fleet_heartbeats if item.get("runtime_config_hash")}),
        "policy_bundle_ids": sorted({item.get("policy_bundle_id") for item in worker_fleet_heartbeats if item.get("policy_bundle_id")}),
    }
    worker_fleet_consistency_violations = []
    if len(worker_fleet_hashes["build_hashes"]) > 1:
        worker_fleet_consistency_violations.append("mixed_build_hash")
    if len(worker_fleet_hashes["runtime_config_hashes"]) > 1:
        worker_fleet_consistency_violations.append("mixed_runtime_config_hash")
    if len(worker_fleet_hashes["policy_bundle_ids"]) > 1:
        worker_fleet_consistency_violations.append("mixed_policy_bundle_id")
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
        "LabelFinalizationContract": {
            "eligible_label_finalization_records": len(label_finalization_records),
            "label_finalization_count": len(label_finalization_records),
            "malformed_count": sum(item["malformed_count"] for item in malformed_label_finalizations),
            "malformed_label_finalizations": malformed_label_finalizations,
            "label_statuses": sorted(
                {
                    item.get("label_status")
                    for record in label_finalization_records
                    for item in [record.get("label_finalization_contract")]
                    if item and item.get("label_status")
                }
            ),
            "supersedes_label_ids": sorted(
                {
                    item.get("supersedes_label_id")
                    for record in label_finalization_records
                    for item in [record.get("label_finalization_contract")]
                    if item and item.get("supersedes_label_id")
                }
            ),
            "label_finalization_projection_version": "v2.7.0.label_finalization.v1",
        },
        "OutcomeWindowCloseContract": {
            "eligible_outcome_window_close_records": len(outcome_window_close_records),
            "outcome_window_close_count": len(outcome_window_close_records),
            "malformed_count": sum(item["malformed_count"] for item in malformed_outcome_window_closes),
            "malformed_outcome_window_closes": malformed_outcome_window_closes,
            "window_order_violation_count": len(outcome_window_close_violations),
            "window_order_violations": outcome_window_close_violations,
            "outcome_window_close_projection_version": OUTCOME_WINDOW_CLOSE_VERSION,
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
        "RawProviderEvidenceContract": {
            "eligible_raw_provider_records": len(raw_provider_evidence_records),
            "raw_provider_evidence_observation_count": len(all_raw_provider_candidates),
            "trusted_raw_provider_evidence_count": sum(1 for item in all_raw_provider_candidates if item.get("provider_evidence_valid") is True),
            "untrusted_raw_provider_evidence_count": sum(1 for item in all_raw_provider_candidates if item.get("provider_evidence_valid") is not True),
            "malformed_count": sum(item["malformed_count"] for item in malformed_raw_provider_evidence),
            "malformed_raw_provider_evidence": malformed_raw_provider_evidence,
            "provider_evidence_violation_count": sum(item["violation_count"] for item in raw_provider_evidence_violations),
            "provider_evidence_violations": raw_provider_evidence_violations,
            "providers": sorted({item.get("provider") for item in all_raw_provider_candidates if item.get("provider")}),
            "endpoints": sorted({item.get("endpoint") for item in all_raw_provider_candidates if item.get("endpoint")}),
            "sides": sorted({item.get("side") for item in all_raw_provider_candidates if item.get("side")}),
            "evidence_sources": sorted({item.get("evidence_source") for item in all_raw_provider_candidates if item.get("evidence_source")}),
            "provider_evidence_proof_levels": sorted({item.get("provider_evidence_proof_level") for item in all_raw_provider_candidates if item.get("provider_evidence_proof_level")}),
            "response_material_types": sorted({item.get("response_material_type") for item in all_raw_provider_candidates if item.get("response_material_type")}),
            "hash_algorithms": sorted({item.get("hash_algorithm") for item in all_raw_provider_candidates if item.get("hash_algorithm")}),
            "raw_provider_evidence_versions": sorted({item.get("raw_provider_evidence_version") for item in all_raw_provider_candidates if item.get("raw_provider_evidence_version")}),
            "raw_provider_evidence_projection_version": "v2.7.0.raw_provider_evidence.v1",
        },
        "RandomnessControlContract": {
            "eligible_randomness_control_records": len(randomness_controls),
            "randomness_control_observation_count": raw_randomness_control_count,
            "current_randomness_control_count": len(randomness_controls),
            "superseded_randomness_control_event_count": superseded_randomness_control_count,
            "valid_randomness_control_count": sum(1 for item in randomness_controls if item.get("randomness_control_valid") is True),
            "malformed_count": len(malformed_randomness_controls),
            "malformed_randomness_controls": [
                {
                    "assignment_id": item.get("assignment_id"),
                    "randomization_unit": item.get("randomization_unit"),
                    "missing_fields": item.get("missing_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_randomness_controls
            ],
            "randomness_control_violation_count": len(randomness_control_violations),
            "randomness_control_violations": [
                {
                    "assignment_id": item.get("assignment_id"),
                    "randomization_unit": item.get("randomization_unit"),
                    "missing_fields": item.get("missing_fields"),
                    "violation_fields": item.get("violation_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in randomness_control_violations
            ],
            "rng_versions": sorted({item.get("rng_version") for item in randomness_controls if item.get("rng_version")}),
            "randomization_units": sorted({item.get("randomization_unit") for item in randomness_controls if item.get("randomization_unit")}),
            "assignment_statuses": sorted({item.get("assignment_status") for item in randomness_controls if item.get("assignment_status")}),
            "evidence_sources": sorted({item.get("evidence_source") for item in randomness_controls if item.get("evidence_source")}),
            "randomness_control_projection_version": "v2.7.0.randomness_control.v1",
        },
        "DeploymentRolloutStateMachine": {
            "eligible_deployment_rollout_records": len(deployment_rollouts),
            "deployment_rollout_observation_count": raw_deployment_rollout_count,
            "current_deployment_rollout_count": len(deployment_rollouts),
            "superseded_deployment_rollout_event_count": superseded_deployment_rollout_count,
            "valid_deployment_rollout_count": sum(1 for item in deployment_rollouts if item.get("deployment_rollout_valid") is True),
            "malformed_count": len(malformed_deployment_rollouts),
            "malformed_deployment_rollouts": [
                {
                    "rollout_id": item.get("rollout_id"),
                    "state": item.get("state"),
                    "canary_status": item.get("canary_status"),
                    "missing_fields": item.get("missing_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_deployment_rollouts
            ],
            "deployment_rollout_violation_count": len(deployment_rollout_violations),
            "deployment_rollout_violations": [
                {
                    "rollout_id": item.get("rollout_id"),
                    "state": item.get("state"),
                    "canary_status": item.get("canary_status"),
                    "missing_fields": item.get("missing_fields"),
                    "violation_fields": item.get("violation_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in deployment_rollout_violations
            ],
            "rollout_states": sorted({item.get("state") for item in deployment_rollouts if item.get("state")}),
            "canary_statuses": sorted({item.get("canary_status") for item in deployment_rollouts if item.get("canary_status")}),
            "evidence_sources": sorted({item.get("evidence_source") for item in deployment_rollouts if item.get("evidence_source")}),
            "deployment_rollout_projection_version": "v2.7.0.deployment_rollout.v1",
        },
        "WorkerFleetConsistencyContract": {
            "eligible_worker_fleet_records": len(worker_fleet_heartbeats),
            "worker_fleet_observation_count": raw_worker_fleet_count,
            "current_worker_fleet_count": len(worker_fleet_heartbeats),
            "superseded_worker_fleet_event_count": superseded_worker_fleet_count,
            "valid_worker_fleet_count": sum(1 for item in worker_fleet_heartbeats if item.get("worker_fleet_heartbeat_valid") is True),
            "malformed_count": len(malformed_worker_fleet_heartbeats),
            "malformed_worker_fleet_heartbeats": [
                {
                    "worker_id": item.get("worker_id"),
                    "role": item.get("role"),
                    "missing_fields": item.get("missing_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_worker_fleet_heartbeats
            ],
            "worker_fleet_violation_count": len(malformed_worker_fleet_heartbeats) + len(worker_fleet_consistency_violations),
            "worker_fleet_violations": worker_fleet_consistency_violations,
            "worker_ids": sorted({item.get("worker_id") for item in worker_fleet_heartbeats if item.get("worker_id")}),
            "roles": sorted({item.get("role") for item in worker_fleet_heartbeats if item.get("role")}),
            "build_hashes": worker_fleet_hashes["build_hashes"],
            "runtime_config_hashes": worker_fleet_hashes["runtime_config_hashes"],
            "policy_bundle_ids": worker_fleet_hashes["policy_bundle_ids"],
            "evidence_sources": sorted({item.get("evidence_source") for item in worker_fleet_heartbeats if item.get("evidence_source")}),
            "worker_fleet_projection_version": "v2.7.0.worker_fleet.v1",
        },
        "BackupRestoreDrillContract": {
            "eligible_backup_restore_drill_records": len(backup_restore_drills),
            "backup_restore_drill_observation_count": raw_backup_restore_drill_count,
            "current_backup_restore_drill_count": len(backup_restore_drills),
            "superseded_backup_restore_drill_event_count": superseded_backup_restore_drill_count,
            "valid_backup_restore_drill_count": sum(1 for item in backup_restore_drills if item.get("backup_restore_drill_valid") is True),
            "malformed_count": len(malformed_backup_restore_drills),
            "malformed_backup_restore_drills": [
                {
                    "drill_id": item.get("drill_id"),
                    "backup_set_id": item.get("backup_set_id"),
                    "missing_fields": item.get("missing_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_backup_restore_drills
            ],
            "backup_restore_drill_violation_count": len(backup_restore_drill_violations),
            "backup_restore_drill_violations": [
                {
                    "drill_id": item.get("drill_id"),
                    "backup_set_id": item.get("backup_set_id"),
                    "missing_fields": item.get("missing_fields"),
                    "violation_fields": item.get("violation_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in backup_restore_drill_violations
            ],
            "backup_set_ids": sorted({item.get("backup_set_id") for item in backup_restore_drills if item.get("backup_set_id")}),
            "restored_world_hashes": sorted({item.get("restored_world_hash") for item in backup_restore_drills if item.get("restored_world_hash")}),
            "restore_statuses": sorted({item.get("restore_status") for item in backup_restore_drills if item.get("restore_status")}),
            "evidence_sources": sorted({item.get("evidence_source") for item in backup_restore_drills if item.get("evidence_source")}),
            "backup_restore_drill_projection_version": "v2.7.0.backup_restore_drill.v1",
        },
        "IncidentEvidenceFreezeContract": {
            "eligible_incident_evidence_freeze_records": len(incident_evidence_freezes),
            "incident_evidence_freeze_observation_count": raw_incident_evidence_freeze_count,
            "current_incident_evidence_freeze_count": len(incident_evidence_freezes),
            "superseded_incident_evidence_freeze_event_count": superseded_incident_evidence_freeze_count,
            "valid_incident_evidence_freeze_count": sum(1 for item in incident_evidence_freezes if item.get("incident_evidence_freeze_valid") is True),
            "malformed_count": len(malformed_incident_evidence_freezes),
            "malformed_incident_evidence_freezes": [
                {
                    "freeze_id": item.get("freeze_id"),
                    "incident_id": item.get("incident_id"),
                    "missing_fields": item.get("missing_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_incident_evidence_freezes
            ],
            "incident_evidence_freeze_violation_count": len(incident_evidence_freeze_violations),
            "incident_evidence_freeze_violations": [
                {
                    "freeze_id": item.get("freeze_id"),
                    "incident_id": item.get("incident_id"),
                    "missing_fields": item.get("missing_fields"),
                    "violation_fields": item.get("violation_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in incident_evidence_freeze_violations
            ],
            "freeze_ids": sorted({item.get("freeze_id") for item in incident_evidence_freezes if item.get("freeze_id")}),
            "incident_ids": sorted({item.get("incident_id") for item in incident_evidence_freezes if item.get("incident_id")}),
            "freeze_statuses": sorted({item.get("freeze_status") for item in incident_evidence_freezes if item.get("freeze_status")}),
            "evidence_sources": sorted({item.get("evidence_source") for item in incident_evidence_freezes if item.get("evidence_source")}),
            "incident_evidence_freeze_projection_version": "v2.7.0.incident_evidence_freeze.v1",
        },
        "CircuitBreakerResumeContract": {
            "eligible_circuit_breaker_resume_records": len(circuit_breaker_resumes),
            "circuit_breaker_resume_observation_count": raw_circuit_breaker_resume_count,
            "current_circuit_breaker_resume_count": len(circuit_breaker_resumes),
            "superseded_circuit_breaker_resume_event_count": superseded_circuit_breaker_resume_count,
            "valid_circuit_breaker_resume_count": sum(1 for item in circuit_breaker_resumes if item.get("circuit_breaker_resume_valid") is True),
            "malformed_count": len(malformed_circuit_breaker_resumes),
            "malformed_circuit_breaker_resumes": [
                {
                    "breaker_id": item.get("breaker_id"),
                    "evidence_freeze_id": item.get("evidence_freeze_id"),
                    "missing_fields": item.get("missing_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_circuit_breaker_resumes
            ],
            "circuit_breaker_resume_violation_count": len(circuit_breaker_resume_violations) + len(circuit_breaker_resume_reference_violations),
            "circuit_breaker_resume_violations": [
                {
                    "breaker_id": item.get("breaker_id"),
                    "evidence_freeze_id": item.get("evidence_freeze_id"),
                    "missing_fields": item.get("missing_fields"),
                    "violation_fields": item.get("violation_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in circuit_breaker_resume_violations
            ]
            + circuit_breaker_resume_reference_violations,
            "breaker_ids": sorted({item.get("breaker_id") for item in circuit_breaker_resumes if item.get("breaker_id")}),
            "evidence_freeze_ids": sorted({item.get("evidence_freeze_id") for item in circuit_breaker_resumes if item.get("evidence_freeze_id")}),
            "resume_statuses": sorted({item.get("resume_status") for item in circuit_breaker_resumes if item.get("resume_status")}),
            "evidence_sources": sorted({item.get("evidence_source") for item in circuit_breaker_resumes if item.get("evidence_source")}),
            "circuit_breaker_resume_projection_version": "v2.7.0.circuit_breaker_resume.v1",
        },
        "QueueDurabilityContract": {
            "eligible_queue_durability_records": len(queue_durability_records),
            "queue_durability_observation_count": raw_queue_durability_count,
            "current_queue_durability_count": len(queue_durability_records),
            "superseded_queue_durability_event_count": superseded_queue_durability_count,
            "valid_queue_durability_count": sum(1 for item in queue_durability_records if item.get("queue_durability_valid") is True),
            "malformed_count": len(malformed_queue_durability),
            "malformed_queue_durability": [
                {
                    "queue_id": item.get("queue_id"),
                    "task_id": item.get("task_id"),
                    "missing_fields": item.get("missing_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_queue_durability
            ],
            "queue_durability_violation_count": len(queue_durability_violations),
            "queue_durability_violations": [
                {
                    "queue_id": item.get("queue_id"),
                    "task_id": item.get("task_id"),
                    "missing_fields": item.get("missing_fields"),
                    "violation_fields": item.get("violation_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in queue_durability_violations
            ],
            "queue_ids": sorted({item.get("queue_id") for item in queue_durability_records if item.get("queue_id")}),
            "durable_states": sorted({item.get("durable_state") for item in queue_durability_records if item.get("durable_state")}),
            "ack_states": sorted({item.get("ack_state") for item in queue_durability_records if item.get("ack_state")}),
            "evidence_sources": sorted({item.get("evidence_source") for item in queue_durability_records if item.get("evidence_source")}),
            "queue_durability_projection_version": "v2.7.0.queue_durability.v1",
        },
        "CandidateCancellationContract": {
            "eligible_candidate_cancellation_records": len(candidate_cancellations),
            "candidate_cancellation_observation_count": raw_candidate_cancellation_count,
            "current_candidate_cancellation_count": len(candidate_cancellations),
            "superseded_candidate_cancellation_event_count": superseded_candidate_cancellation_count,
            "valid_candidate_cancellation_count": sum(1 for item in candidate_cancellations if item.get("candidate_cancellation_valid") is True),
            "malformed_count": len(malformed_candidate_cancellations),
            "malformed_candidate_cancellations": [
                {
                    "candidate_id": item.get("candidate_id"),
                    "cancel_reason": item.get("cancel_reason"),
                    "missing_fields": item.get("missing_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_candidate_cancellations
            ],
            "candidate_cancellation_violation_count": len(candidate_cancellation_violations),
            "candidate_cancellation_violations": [
                {
                    "candidate_id": item.get("candidate_id"),
                    "cancel_reason": item.get("cancel_reason"),
                    "missing_fields": item.get("missing_fields"),
                    "violation_fields": item.get("violation_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in candidate_cancellation_violations
            ],
            "candidate_ids": sorted({item.get("candidate_id") for item in candidate_cancellations if item.get("candidate_id")}),
            "cancel_reasons": sorted({item.get("cancel_reason") for item in candidate_cancellations if item.get("cancel_reason")}),
            "evidence_sources": sorted({item.get("evidence_source") for item in candidate_cancellations if item.get("evidence_source")}),
            "candidate_cancellation_projection_version": "v2.7.0.candidate_cancellation.v1",
        },
        "RetryStormControlContract": {
            "eligible_retry_storm_control_records": len(retry_storm_controls),
            "retry_storm_control_observation_count": raw_retry_storm_control_count,
            "current_retry_storm_control_count": len(retry_storm_controls),
            "superseded_retry_storm_control_event_count": superseded_retry_storm_control_count,
            "valid_retry_storm_control_count": sum(1 for item in retry_storm_controls if item.get("retry_storm_control_valid") is True),
            "malformed_count": len(malformed_retry_storm_controls),
            "malformed_retry_storm_controls": [
                {
                    "retry_family": item.get("retry_family"),
                    "backoff_policy": item.get("backoff_policy"),
                    "missing_fields": item.get("missing_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_retry_storm_controls
            ],
            "retry_storm_control_violation_count": len(retry_storm_control_violations),
            "retry_storm_control_violations": [
                {
                    "retry_family": item.get("retry_family"),
                    "backoff_policy": item.get("backoff_policy"),
                    "missing_fields": item.get("missing_fields"),
                    "violation_fields": item.get("violation_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in retry_storm_control_violations
            ],
            "retry_families": sorted({item.get("retry_family") for item in retry_storm_controls if item.get("retry_family")}),
            "backoff_policies": sorted({item.get("backoff_policy") for item in retry_storm_controls if item.get("backoff_policy")}),
            "evidence_sources": sorted({item.get("evidence_source") for item in retry_storm_controls if item.get("evidence_source")}),
            "retry_storm_control_projection_version": "v2.7.0.retry_storm_control.v1",
        },
        "ProviderCoverageMapContract": {
            "eligible_provider_coverage_map_records": len(provider_coverage_maps),
            "provider_coverage_map_observation_count": raw_provider_coverage_map_count,
            "current_provider_coverage_map_count": len(provider_coverage_maps),
            "superseded_provider_coverage_map_event_count": superseded_provider_coverage_map_count,
            "valid_provider_coverage_map_count": sum(1 for item in provider_coverage_maps if item.get("provider_coverage_map_valid") is True),
            "malformed_count": len(malformed_provider_coverage_maps),
            "malformed_provider_coverage_maps": [
                {
                    "provider": item.get("provider"),
                    "chain": item.get("chain"),
                    "pool_type": item.get("pool_type"),
                    "missing_fields": item.get("missing_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_provider_coverage_maps
            ],
            "provider_coverage_map_violation_count": len(provider_coverage_map_violations),
            "provider_coverage_map_violations": [
                {
                    "provider": item.get("provider"),
                    "chain": item.get("chain"),
                    "pool_type": item.get("pool_type"),
                    "coverage_status": item.get("coverage_status"),
                    "missing_fields": item.get("missing_fields"),
                    "violation_fields": item.get("violation_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in provider_coverage_map_violations
            ],
            "providers": sorted({item.get("provider") for item in provider_coverage_maps if item.get("provider")}),
            "chains": sorted({item.get("chain") for item in provider_coverage_maps if item.get("chain")}),
            "pool_types": sorted({item.get("pool_type") for item in provider_coverage_maps if item.get("pool_type")}),
            "coverage_statuses": sorted({item.get("coverage_status") for item in provider_coverage_maps if item.get("coverage_status")}),
            "unsupported_reasons": sorted({item.get("unsupported_reason") for item in provider_coverage_maps if item.get("unsupported_reason")}),
            "evidence_sources": sorted({item.get("evidence_source") for item in provider_coverage_maps if item.get("evidence_source")}),
            "provider_coverage_map_projection_version": "v2.7.0.provider_coverage_map.v1",
        },
        "TrainingServingSkewContract": {
            "eligible_training_serving_skew_records": len(training_serving_skews),
            "training_serving_skew_observation_count": raw_training_serving_skew_count,
            "current_training_serving_skew_count": len(training_serving_skews),
            "superseded_training_serving_skew_event_count": superseded_training_serving_skew_count,
            "valid_training_serving_skew_count": sum(1 for item in training_serving_skews if item.get("training_serving_skew_valid") is True),
            "malformed_count": len(malformed_training_serving_skews),
            "malformed_training_serving_skews": [
                {
                    "feature_set_id": item.get("feature_set_id"),
                    "normalization_version": item.get("normalization_version"),
                    "missing_fields": item.get("missing_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in malformed_training_serving_skews
            ],
            "training_serving_skew_violation_count": len(training_serving_skew_violations),
            "training_serving_skew_violations": [
                {
                    "feature_set_id": item.get("feature_set_id"),
                    "normalization_version": item.get("normalization_version"),
                    "skew_check_result": item.get("skew_check_result"),
                    "missing_fields": item.get("missing_fields"),
                    "violation_fields": item.get("violation_fields"),
                    "source_event_id": item.get("source_event_id"),
                }
                for item in training_serving_skew_violations
            ],
            "feature_set_ids": sorted({item.get("feature_set_id") for item in training_serving_skews if item.get("feature_set_id")}),
            "normalization_versions": sorted({item.get("normalization_version") for item in training_serving_skews if item.get("normalization_version")}),
            "skew_check_results": sorted({item.get("skew_check_result") for item in training_serving_skews if item.get("skew_check_result")}),
            "evidence_sources": sorted({item.get("evidence_source") for item in training_serving_skews if item.get("evidence_source")}),
            "training_serving_skew_projection_version": "v2.7.0.training_serving_skew.v1",
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
            "standalone_no_fill_outcome_count": len(standalone_no_fill_outcomes),
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


RECORD_HASH_CONTRACT_FIELDS = {
    "reference_price": "reference_price_contract",
    "trade_outcome_label": "trade_outcome_label",
    "label_finalization": "label_finalization_contract",
    "outcome_window_close": "outcome_window_close_contract",
    "standardized_stop": "standardized_stop_contract",
    "ex_ante_feasibility": "ex_ante_feasibility_contract",
    "earliest_actionable_time": "earliest_actionable_time_contract",
    "realtime_clean": "realtime_clean_contract",
    "quote_intent_binding": "quote_intent_binding_contract",
    "raw_provider_evidence": "raw_provider_evidence_contract",
    "idempotency": "idempotency_contract",
    "execution_control": "execution_control",
    "paper_ledger": "paper_ledger_contract",
    "no_fill_outcome": "no_fill_outcome",
}


def _compact_contract_ref(value):
    if not isinstance(value, dict):
        return None
    ref = {
        key: value.get(key)
        for key in (
            "source_event_id",
            "global_seq",
            "source",
            "label_status",
            "outcome_state",
            "state",
            "contract_version",
            "no_fill_outcome_version",
        )
        if value.get(key) is not None
    }
    for key, item in value.items():
        if key.endswith("_version") and item is not None:
            ref[key] = item
    return ref


def _records_hash_material(record_list):
    material = []
    for record in record_list:
        material.append(
            {
                "denominator_dedup_key": record.get("denominator_dedup_key"),
                "token_ca": record.get("token_ca"),
                "chain": record.get("chain"),
                "canonical_pool_group": record.get("canonical_pool_group"),
                "lifecycle_epoch": record.get("lifecycle_epoch"),
                "source_dog_label": record.get("source_dog_label"),
                "captured": record.get("captured"),
                "denominator_membership": record.get("denominator_membership"),
                "denominator_dirty_reasons": record.get("denominator_dirty_reasons") or [],
                "contracts": {
                    name: _compact_contract_ref(record.get(field))
                    for name, field in RECORD_HASH_CONTRACT_FIELDS.items()
                    if _compact_contract_ref(record.get(field))
                },
            }
        )
    return material


def build_denominator_projection(
    event_log_dir,
    *,
    include_records=False,
    progress_callback=None,
    progress_interval_events=10_000,
):
    event_log_dir = Path(event_log_dir)
    event_log = V27EventLog(event_log_dir)
    projection = {
        "projection_name": "v27_denominator_seed",
        "projection_version": "v0.1",
        "spec_version": "v2.7.0",
        "event_log_dir": str(event_log_dir),
        "event_log_verify": None,
        "event_log_verify_mode": None,
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
        "raw_provider_evidence_recorded_events": 0,
        "idempotency_contract_recorded_events": 0,
        "execution_control_recorded_events": 0,
        "paper_ledger_recorded_events": 0,
        "no_fill_outcome_recorded_events": 0,
        "runtime_recovery_control_recorded_events": 0,
        "randomness_control_recorded_events": 0,
        "deployment_rollout_state_recorded_events": 0,
        "worker_fleet_heartbeat_recorded_events": 0,
        "backup_restore_drill_recorded_events": 0,
        "incident_evidence_freeze_recorded_events": 0,
        "circuit_breaker_resume_recorded_events": 0,
        "queue_durability_recorded_events": 0,
        "candidate_cancellation_recorded_events": 0,
        "retry_storm_control_recorded_events": 0,
        "provider_coverage_map_recorded_events": 0,
        "training_serving_skew_recorded_events": 0,
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
            "LabelFinalizationContract": {},
            "OutcomeWindowCloseContract": {},
            "StandardizedStopContract": {},
            "ExAnteFeasibility": {},
            "EarliestActionableTime": {},
            "RealtimeCleanDetector": {},
            "QuoteIntentBindingContract": {},
            "RawProviderEvidenceContract": {},
            "RandomnessControlContract": {},
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
            "DeploymentRolloutStateMachine": {},
            "WorkerFleetConsistencyContract": {},
            "BackupRestoreDrillContract": {},
            "IncidentEvidenceFreezeContract": {},
            "CircuitBreakerResumeContract": {},
            "QueueDurabilityContract": {},
            "CandidateCancellationContract": {},
            "RetryStormControlContract": {},
            "ProviderCoverageMapContract": {},
            "TrainingServingSkewContract": {},
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
            "label_finalization_ok": False,
            "outcome_window_close_ok": False,
            "standardized_stop_ok": False,
            "ex_ante_feasibility_ok": False,
            "earliest_actionable_time_ok": False,
            "realtime_clean_detector_ok": False,
            "quote_intent_binding_ok": False,
            "raw_provider_evidence_ok": False,
            "randomness_control_ok": False,
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
            "deployment_rollout_state_machine_ok": False,
            "worker_fleet_consistency_ok": False,
            "backup_restore_drill_ok": False,
            "incident_evidence_freeze_ok": False,
            "circuit_breaker_resume_ok": False,
            "queue_durability_ok": False,
            "candidate_cancellation_ok": False,
            "retry_storm_control_ok": False,
            "provider_coverage_map_ok": False,
            "training_serving_skew_ok": False,
            "normal_tiny_ready": False,
            "status": "not_built",
        },
        "records_hash_material_version": None,
        "records_hash": None,
    }

    try:
        event_log_summary = event_log.summary()
        projection["event_log_verify_mode"] = event_log_summary.pop("verify_mode", None)
        projection["event_log_verify"] = event_log_summary
        projection["event_log_latest_seq"] = projection["event_log_verify"]["last_global_seq"]
        projection["health"]["event_log_ok"] = True
        if progress_callback:
            progress_callback(
                {
                    "stage": "event_log_summary",
                    "event_log_latest_seq": projection["event_log_latest_seq"],
                    "event_log_verify_mode": projection.get("event_log_verify_mode"),
                }
            )
    except V27EventLogError as exc:
        projection["event_log_error"] = str(exc)
        projection["health"]["status"] = "event_log_invalid"
        return projection

    facts = []
    no_fill_facts = []
    runtime_recovery_controls = []
    randomness_controls = []
    deployment_rollouts = []
    worker_fleet_heartbeats = []
    backup_restore_drills = []
    incident_evidence_freezes = []
    circuit_breaker_resumes = []
    queue_durability_records = []
    candidate_cancellations = []
    retry_storm_controls = []
    provider_coverage_maps = []
    training_serving_skews = []
    resolved_pool_by_identity = {}
    window_start = None
    window_end = None
    for event in event_log.iter_events(prune_payload_fields=HEAVY_PAYLOAD_FIELDS) or []:
        projection["input_events"] += 1
        if (
            progress_callback
            and progress_interval_events
            and projection["input_events"] % int(progress_interval_events) == 0
        ):
            progress_callback(
                {
                    "stage": "event_replay",
                    "input_events": projection["input_events"],
                    "global_seq": event.get("global_seq"),
                    "event_type": event.get("event_type"),
                    "facts": len(facts),
                    "no_fill_facts": len(no_fill_facts),
                    "runtime_recovery_controls": len(runtime_recovery_controls),
                }
            )
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
        if event.get("event_type") == RAW_PROVIDER_EVIDENCE_EVENT_TYPE:
            projection["raw_provider_evidence_recorded_events"] += 1
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
        if event.get("event_type") == RANDOMNESS_CONTROL_EVENT_TYPE:
            projection["randomness_control_recorded_events"] += 1
            randomness_control = _extract_randomness_control(event, _payload_bags(event))
            if randomness_control:
                randomness_controls.append(randomness_control)
            continue
        if event.get("event_type") == DEPLOYMENT_ROLLOUT_EVENT_TYPE:
            projection["deployment_rollout_state_recorded_events"] += 1
            deployment_rollout = _extract_deployment_rollout(event, _payload_bags(event))
            if deployment_rollout:
                deployment_rollouts.append(deployment_rollout)
            continue
        if event.get("event_type") == WORKER_FLEET_EVENT_TYPE:
            projection["worker_fleet_heartbeat_recorded_events"] += 1
            worker_fleet_heartbeat = _extract_worker_fleet_heartbeat(event, _payload_bags(event))
            if worker_fleet_heartbeat:
                worker_fleet_heartbeats.append(worker_fleet_heartbeat)
            continue
        if event.get("event_type") == BACKUP_RESTORE_DRILL_EVENT_TYPE:
            projection["backup_restore_drill_recorded_events"] += 1
            backup_restore_drill = _extract_backup_restore_drill(event, _payload_bags(event))
            if backup_restore_drill:
                backup_restore_drills.append(backup_restore_drill)
            continue
        if event.get("event_type") == INCIDENT_EVIDENCE_FREEZE_EVENT_TYPE:
            projection["incident_evidence_freeze_recorded_events"] += 1
            incident_evidence_freeze = _extract_incident_evidence_freeze(event, _payload_bags(event))
            if incident_evidence_freeze:
                incident_evidence_freezes.append(incident_evidence_freeze)
            continue
        if event.get("event_type") == CIRCUIT_BREAKER_RESUME_EVENT_TYPE:
            projection["circuit_breaker_resume_recorded_events"] += 1
            circuit_breaker_resume = _extract_circuit_breaker_resume(event, _payload_bags(event))
            if circuit_breaker_resume:
                circuit_breaker_resumes.append(circuit_breaker_resume)
            continue
        if event.get("event_type") == QUEUE_DURABILITY_EVENT_TYPE:
            projection["queue_durability_recorded_events"] += 1
            queue_durability = _extract_queue_durability(event, _payload_bags(event))
            if queue_durability:
                queue_durability_records.append(queue_durability)
            continue
        if event.get("event_type") == CANDIDATE_CANCELLATION_EVENT_TYPE:
            projection["candidate_cancellation_recorded_events"] += 1
            candidate_cancellation = _extract_candidate_cancellation(event, _payload_bags(event))
            if candidate_cancellation:
                candidate_cancellations.append(candidate_cancellation)
            continue
        if event.get("event_type") == RETRY_STORM_CONTROL_EVENT_TYPE:
            projection["retry_storm_control_recorded_events"] += 1
            retry_storm_control = _extract_retry_storm_control(event, _payload_bags(event))
            if retry_storm_control:
                retry_storm_controls.append(retry_storm_control)
            continue
        if event.get("event_type") == PROVIDER_COVERAGE_MAP_EVENT_TYPE:
            projection["provider_coverage_map_recorded_events"] += 1
            provider_coverage_map = _extract_provider_coverage_map(event, _payload_bags(event))
            if provider_coverage_map:
                provider_coverage_maps.append(provider_coverage_map)
            continue
        if event.get("event_type") == TRAINING_SERVING_SKEW_EVENT_TYPE:
            projection["training_serving_skew_recorded_events"] += 1
            training_serving_skew = _extract_training_serving_skew(event, _payload_bags(event))
            if training_serving_skew:
                training_serving_skews.append(training_serving_skew)
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
        if event.get("event_type") == NO_FILL_OUTCOME_EVENT_TYPE:
            no_fill_facts.append(fact)
            continue
        identity = (fact.get("chain"), fact.get("token_ca"), fact.get("lifecycle_epoch", 0))
        pool = fact.get("canonical_pool_group")
        if event.get("event_type") == LIFECYCLE_IDENTITY_EVENT_TYPE and pool and pool != "unknown_pool":
            resolved_pool_by_identity.setdefault(identity, pool)
        facts.append(fact)

    if progress_callback:
        progress_callback(
            {
                "stage": "event_replay_complete",
                "input_events": projection["input_events"],
                "facts": len(facts),
                "no_fill_facts": len(no_fill_facts),
                "runtime_recovery_controls": len(runtime_recovery_controls),
                "randomness_controls": len(randomness_controls),
            }
        )

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

    standalone_no_fill_outcomes = []
    for fact in no_fill_facts:
        key = fact["denominator_dedup_key"]
        if key in records:
            _merge_fact(records[key], fact)
            continue
        no_fill_outcome = fact.get("no_fill_outcome_contract")
        if no_fill_outcome:
            standalone_no_fill_outcomes.append(
                {
                    **no_fill_outcome,
                    "denominator_dedup_key": key,
                }
            )

    if progress_callback:
        progress_callback(
            {
                "stage": "records_merged",
                "facts": len(facts),
                "no_fill_facts": len(no_fill_facts),
                "records": len(records),
                "standalone_no_fill_outcomes": len(standalone_no_fill_outcomes),
            }
        )

    record_list = []
    for index, key in enumerate(sorted(records), start=1):
        record = records[key]
        _finalize_signal_credit(record)
        _finalize_reference_price(record)
        _finalize_trade_outcome_label(record)
        _finalize_label_finalization(record)
        _finalize_outcome_window_close(record)
        _finalize_standardized_stop(record)
        _finalize_ex_ante_feasibility(record)
        _finalize_earliest_actionable_time(record)
        _finalize_realtime_clean(record)
        _finalize_quote_intent_binding(record)
        _finalize_raw_provider_evidence(record)
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
        if progress_callback and progress_interval_events and index % 1000 == 0:
            progress_callback(
                {
                    "stage": "records_finalize",
                    "records_finalized": index,
                    "records": len(records),
                    "dirty_records": len(projection["dirty_records"]),
                }
            )

    if progress_callback:
        progress_callback(
            {
                "stage": "records_finalize_complete",
                "records": len(record_list),
                "dirty_records": len(projection["dirty_records"]),
            }
        )

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
    if progress_callback:
        progress_callback(
            {
                "stage": "contract_evidence_start",
                "records": len(record_list),
                "standalone_no_fill_outcomes": len(standalone_no_fill_outcomes),
            }
        )
    contract_evidence = _contract_evidence_from_records(
        record_list,
        runtime_recovery_controls=runtime_recovery_controls,
        standalone_no_fill_outcomes=standalone_no_fill_outcomes,
        randomness_controls=randomness_controls,
        deployment_rollouts=deployment_rollouts,
        worker_fleet_heartbeats=worker_fleet_heartbeats,
        backup_restore_drills=backup_restore_drills,
        incident_evidence_freezes=incident_evidence_freezes,
        circuit_breaker_resumes=circuit_breaker_resumes,
        queue_durability_records=queue_durability_records,
        candidate_cancellations=candidate_cancellations,
        retry_storm_controls=retry_storm_controls,
        provider_coverage_maps=provider_coverage_maps,
        training_serving_skews=training_serving_skews,
    )
    if progress_callback:
        progress_callback(
            {
                "stage": "contract_evidence_complete",
                "records": len(record_list),
                "standalone_no_fill_outcomes": len(standalone_no_fill_outcomes),
            }
        )
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
    if progress_callback:
        progress_callback({"stage": "records_hash_start", "records": len(record_list)})
    projection["records_hash_material_version"] = "v2.7.0.compact_record_contract_summary.v1"
    projection["records_hash"] = sha256_hex(_records_hash_material(record_list))
    if progress_callback:
        progress_callback({"stage": "records_hash_complete", "records": len(record_list)})
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
    projection["health"]["label_finalization_ok"] = (
        contract_evidence["LabelFinalizationContract"]["eligible_label_finalization_records"] > 0
        and contract_evidence["LabelFinalizationContract"]["malformed_count"] == 0
    )
    projection["health"]["outcome_window_close_ok"] = (
        contract_evidence["OutcomeWindowCloseContract"]["eligible_outcome_window_close_records"] > 0
        and contract_evidence["OutcomeWindowCloseContract"]["malformed_count"] == 0
        and contract_evidence["OutcomeWindowCloseContract"]["window_order_violation_count"] == 0
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
    projection["health"]["raw_provider_evidence_ok"] = (
        contract_evidence["RawProviderEvidenceContract"]["eligible_raw_provider_records"] > 0
        and contract_evidence["RawProviderEvidenceContract"]["trusted_raw_provider_evidence_count"] > 0
        and contract_evidence["RawProviderEvidenceContract"]["malformed_count"] == 0
        and contract_evidence["RawProviderEvidenceContract"]["provider_evidence_violation_count"] == 0
    )
    projection["health"]["randomness_control_ok"] = (
        contract_evidence["RandomnessControlContract"]["eligible_randomness_control_records"] > 0
        and contract_evidence["RandomnessControlContract"]["valid_randomness_control_count"] > 0
        and contract_evidence["RandomnessControlContract"]["malformed_count"] == 0
        and contract_evidence["RandomnessControlContract"]["randomness_control_violation_count"] == 0
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
    projection["health"]["deployment_rollout_state_machine_ok"] = (
        contract_evidence["DeploymentRolloutStateMachine"]["eligible_deployment_rollout_records"] > 0
        and contract_evidence["DeploymentRolloutStateMachine"]["valid_deployment_rollout_count"] > 0
        and contract_evidence["DeploymentRolloutStateMachine"]["malformed_count"] == 0
        and contract_evidence["DeploymentRolloutStateMachine"]["deployment_rollout_violation_count"] == 0
    )
    projection["health"]["worker_fleet_consistency_ok"] = (
        contract_evidence["WorkerFleetConsistencyContract"]["eligible_worker_fleet_records"] > 0
        and contract_evidence["WorkerFleetConsistencyContract"]["valid_worker_fleet_count"] > 0
        and contract_evidence["WorkerFleetConsistencyContract"]["malformed_count"] == 0
        and contract_evidence["WorkerFleetConsistencyContract"]["worker_fleet_violation_count"] == 0
    )
    projection["health"]["backup_restore_drill_ok"] = (
        contract_evidence["BackupRestoreDrillContract"]["eligible_backup_restore_drill_records"] > 0
        and contract_evidence["BackupRestoreDrillContract"]["valid_backup_restore_drill_count"] > 0
        and contract_evidence["BackupRestoreDrillContract"]["malformed_count"] == 0
        and contract_evidence["BackupRestoreDrillContract"]["backup_restore_drill_violation_count"] == 0
    )
    projection["health"]["incident_evidence_freeze_ok"] = (
        contract_evidence["IncidentEvidenceFreezeContract"]["eligible_incident_evidence_freeze_records"] > 0
        and contract_evidence["IncidentEvidenceFreezeContract"]["valid_incident_evidence_freeze_count"] > 0
        and contract_evidence["IncidentEvidenceFreezeContract"]["malformed_count"] == 0
        and contract_evidence["IncidentEvidenceFreezeContract"]["incident_evidence_freeze_violation_count"] == 0
    )
    projection["health"]["circuit_breaker_resume_ok"] = (
        contract_evidence["CircuitBreakerResumeContract"]["eligible_circuit_breaker_resume_records"] > 0
        and contract_evidence["CircuitBreakerResumeContract"]["valid_circuit_breaker_resume_count"] > 0
        and contract_evidence["CircuitBreakerResumeContract"]["malformed_count"] == 0
        and contract_evidence["CircuitBreakerResumeContract"]["circuit_breaker_resume_violation_count"] == 0
    )
    projection["health"]["queue_durability_ok"] = (
        contract_evidence["QueueDurabilityContract"]["eligible_queue_durability_records"] > 0
        and contract_evidence["QueueDurabilityContract"]["valid_queue_durability_count"] > 0
        and contract_evidence["QueueDurabilityContract"]["malformed_count"] == 0
        and contract_evidence["QueueDurabilityContract"]["queue_durability_violation_count"] == 0
    )
    projection["health"]["candidate_cancellation_ok"] = (
        contract_evidence["CandidateCancellationContract"]["eligible_candidate_cancellation_records"] > 0
        and contract_evidence["CandidateCancellationContract"]["valid_candidate_cancellation_count"] > 0
        and contract_evidence["CandidateCancellationContract"]["malformed_count"] == 0
        and contract_evidence["CandidateCancellationContract"]["candidate_cancellation_violation_count"] == 0
    )
    projection["health"]["retry_storm_control_ok"] = (
        contract_evidence["RetryStormControlContract"]["eligible_retry_storm_control_records"] > 0
        and contract_evidence["RetryStormControlContract"]["valid_retry_storm_control_count"] > 0
        and contract_evidence["RetryStormControlContract"]["malformed_count"] == 0
        and contract_evidence["RetryStormControlContract"]["retry_storm_control_violation_count"] == 0
    )
    projection["health"]["provider_coverage_map_ok"] = (
        contract_evidence["ProviderCoverageMapContract"]["eligible_provider_coverage_map_records"] > 0
        and contract_evidence["ProviderCoverageMapContract"]["valid_provider_coverage_map_count"] > 0
        and contract_evidence["ProviderCoverageMapContract"]["malformed_count"] == 0
        and contract_evidence["ProviderCoverageMapContract"]["provider_coverage_map_violation_count"] == 0
    )
    projection["health"]["training_serving_skew_ok"] = (
        contract_evidence["TrainingServingSkewContract"]["eligible_training_serving_skew_records"] > 0
        and contract_evidence["TrainingServingSkewContract"]["valid_training_serving_skew_count"] > 0
        and contract_evidence["TrainingServingSkewContract"]["malformed_count"] == 0
        and contract_evidence["TrainingServingSkewContract"]["training_serving_skew_violation_count"] == 0
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
    lag_ms = 0 if lag_seq == 0 else _lag_ms(projection.get("event_log_latest_at"), now_iso)

    stale_reasons = []
    if lag_seq > max_allowed_lag_seq:
        stale_reasons.append("read_model_seq_lag")
    if lag_seq > 0 and lag_ms is not None and lag_ms > max_allowed_lag_ms:
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
