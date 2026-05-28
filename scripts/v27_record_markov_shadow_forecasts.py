#!/usr/bin/env python3
"""Record Markov lifecycle forecasts as post-hard-gate shadow evidence.

This intentionally does not create paper entries. It only turns the
Telegram-lifecycle Markov model into an auditable feature snapshot after the
candidate has already passed the non-negotiable hard evidence boundary.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from telegram_lifecycle_markov import build_lifecycle_forecast_snapshot, canonical_state  # noqa: E402
from v27_denominator_projection import build_denominator_projection  # noqa: E402
from v27_event_log import V27EventLog, sha256_hex  # noqa: E402


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
MARKOV_SHADOW_EVENT_TYPE = "markov_shadow_forecast_recorded"
SCHEMA_VERSION = "v2.7.0.markov_shadow_forecast.v1"
SOURCE = "v27_markov_shadow_forecast"
MODEL_INPUT_SCHEMA_VERSION = "v2.7.0.telegram_lifecycle_markov_shadow_features.v1"
POLICY_BUNDLE_ID = "v2.7.0_markov_shadow_feature_policy"
METRIC_IDS = ("p_absorb_peak30", "p_absorb_stop_before_peak")
THRESHOLD_IDS = ("thr_p_absorb_peak30_shadow_min", "thr_p_absorb_stop_before_peak_shadow_max")


def _payload(event: Mapping[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _nested_get(value: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = value
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _truthy(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_value(payload: Mapping[str, Any], paths: Iterable[tuple[str, ...]]) -> Any:
    for path in paths:
        value = _nested_get(payload, path)
        if value is not None:
            return value
    return None


def _token_lifecycle_fields(payload: Mapping[str, Any]) -> dict[str, Any]:
    chain = payload.get("chain") or "unknown_chain"
    token_ca = payload.get("token_ca") or payload.get("normalized_ca") or "unknown_token"
    pool = payload.get("canonical_pool_group") or payload.get("pool_address") or payload.get("pool") or "unknown_pool"
    epoch = payload.get("lifecycle_epoch", 0)
    token_lifecycle_key = payload.get("token_lifecycle_key") or f"{chain}:{token_ca}:{pool}"
    return {
        "token_lifecycle_key": str(token_lifecycle_key),
        "token_ca": token_ca,
        "chain": chain,
        "canonical_pool_group": pool,
        "lifecycle_epoch": epoch,
    }


def _derived_lifecycle_state(event: Mapping[str, Any], payload: Mapping[str, Any]) -> str | None:
    explicit_state = canonical_state(
        payload.get("lifecycle_state")
        or payload.get("state")
        or _nested_get(payload, ("lifecycle", "lifecycle_state"))
    )
    if explicit_state:
        return explicit_state

    event_type = str(event.get("event_type") or "")
    if event_type == "telegram_signal_seen":
        return "TELEGRAM_SEEN"
    if event_type == "realtime_clean_detector_recorded":
        return "TRADABLE_CLEAN" if _truthy(payload.get("realtime_clean")) is True else "DIRTY_QUOTE"
    if event_type == "ex_ante_feasibility_recorded" and _truthy(payload.get("ex_ante_feasible")) is True:
        return "TRADABLE_CLEAN"
    if event_type == "earliest_actionable_time_recorded" and _truthy(payload.get("actionable_before_peak")) is True:
        return "RECLAIM_CONFIRMED" if _truthy(payload.get("reclaim_confirmed")) is True else "TRADABLE_CLEAN"
    if event_type == "execution_control_recorded":
        state = str(payload.get("state") or "").lower()
        if state == "filled_paper":
            return "TINY_ENTERED"
        if state in {"no_fill", "skipped", "cancelled", "rejected"}:
            return "OBSERVE_ONLY"
    if event_type == "trade_outcome_label_recorded":
        if _truthy(payload.get("would_stop_before_peak")) is True:
            return "STOP_BEFORE_PEAK"
        peak = _as_float(
            payload.get("net_delayed_executable_peak_3s")
            or payload.get("net_delayed_executable_peak")
            or payload.get("executable_peak_pnl")
        )
        if peak is not None and peak >= 0.30:
            return "PEAK30"
        return "TERMINAL_DEAD"
    return None


def v27_event_log_lifecycle_events(events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    lifecycle_events: list[dict[str, Any]] = []
    for event in events:
        payload = _payload(event)
        state = _derived_lifecycle_state(event, payload)
        if not state:
            continue
        fields = _token_lifecycle_fields(payload)
        lifecycle_events.append(
            {
                **fields,
                "event_id": event.get("event_id"),
                "lifecycle_state": state,
                "monotonic_ingest_seq": event.get("global_seq"),
                "aggregate_seq": event.get("aggregate_seq"),
                "decision_available_at": (
                    payload.get("decision_available_at")
                    or payload.get("available_at")
                    or event.get("available_at")
                    or event.get("observed_at")
                ),
            }
        )
    return lifecycle_events


def _hard_gate_evidence(record: Mapping[str, Any]) -> dict[str, Any]:
    membership = record.get("denominator_membership") if isinstance(record.get("denominator_membership"), dict) else {}
    ex_ante = record.get("ex_ante_feasibility_contract") if isinstance(record.get("ex_ante_feasibility_contract"), dict) else {}
    earliest = record.get("earliest_actionable_time") if isinstance(record.get("earliest_actionable_time"), dict) else {}
    reasons: list[str] = []

    checks = {
        "d3a_externally_actionable": membership.get("D3a_externally_actionable_gold_silver") is True,
        "realtime_clean": record.get("realtime_clean") is True,
        "entry_quote_executable": record.get("entry_quote_executable") is True,
        "exit_quote_executable": record.get("exit_quote_executable") is True,
        "liquidity_ok": record.get("liquidity_ok") is True,
        "critical_risk_ok": record.get("critical_risk_ok") is True,
        "ex_ante_feasible": record.get("ex_ante_feasible") is True,
        "reclaim_confirmed": record.get("reclaim_confirmed") is True,
        "not_overextended": record.get("not_overextended") is True,
        "earliest_actionable_valid": bool(
            earliest
            and earliest.get("actionable_before_peak") is True
            and not earliest.get("missing_fields")
            and not earliest.get("invariant_violations")
        ),
        "ex_ante_contract_valid": bool(
            ex_ante
            and ex_ante.get("ex_ante_feasible") is True
            and not ex_ante.get("missing_fields")
            and not ex_ante.get("leakage_fields")
        ),
    }
    for name, passed in checks.items():
        if not passed:
            reasons.append(f"{name}_missing_or_false")

    hard_gate_seq_fields = [
        _nested_get(record, ("realtime_clean_contract", "global_seq")),
        _nested_get(record, ("ex_ante_feasibility_contract", "global_seq")),
        _nested_get(record, ("earliest_actionable_time", "global_seq")),
    ]
    hard_gate_cutoff_seq = max([int(seq) for seq in hard_gate_seq_fields if seq is not None], default=None)
    if hard_gate_cutoff_seq is None:
        reasons.append("hard_gate_cutoff_seq_missing")

    decision_available_at = (
        earliest.get("decision_available_at")
        or earliest.get("decision_ts")
        or ex_ante.get("decision_available_at")
        or ex_ante.get("decision_ts")
        or record.get("available_at")
    )
    if not decision_available_at:
        reasons.append("decision_available_at_missing")

    return {
        "passed": not reasons,
        "blocking_reasons": sorted(set(reasons)),
        "checks": checks,
        "hard_gate_cutoff_seq": hard_gate_cutoff_seq,
        "decision_available_at": decision_available_at,
        "earliest_actionable_time": earliest,
        "ex_ante_feasibility": ex_ante,
    }


def _record_identity(record: Mapping[str, Any]) -> dict[str, Any]:
    chain = record.get("chain") or "unknown_chain"
    token_ca = record.get("token_ca") or "unknown_token"
    pool = record.get("canonical_pool_group") or "unknown_pool"
    epoch = record.get("lifecycle_epoch", 0)
    token_lifecycle_key = f"{chain}:{token_ca}:{pool}"
    denominator_dedup_key = record.get("denominator_dedup_key") or f"{token_lifecycle_key}:{epoch}"
    return {
        "denominator_dedup_key": denominator_dedup_key,
        "token_lifecycle_key": token_lifecycle_key,
        "token_ca": token_ca,
        "chain": chain,
        "canonical_pool_group": pool,
        "lifecycle_epoch": epoch,
    }


def _serialized_probability(value: Any) -> str:
    try:
        return f"{float(value):.12g}"
    except (TypeError, ValueError):
        return "null"


def _feature_vector(snapshot: Mapping[str, Any], *, decision_available_at: str) -> dict[str, Any]:
    absorption = snapshot.get("absorption_forecast") if isinstance(snapshot.get("absorption_forecast"), dict) else {}
    feature_values = {
        "p_absorb_peak30": _serialized_probability(absorption.get("p_absorb_peak30")),
        "p_absorb_stop_before_peak": _serialized_probability(absorption.get("p_absorb_stop_before_peak")),
        "p_absorb_toxic_dead": _serialized_probability(absorption.get("p_absorb_toxic_dead")),
        "p_absorb_crash_dead": _serialized_probability(absorption.get("p_absorb_crash_dead")),
        "unresolved_probability_after_horizon": _serialized_probability(absorption.get("unresolved_probability_after_horizon")),
    }
    feature_names = sorted(feature_values)
    material = {
        "feature_names_ordered": feature_names,
        "feature_values_serialized": {name: feature_values[name] for name in feature_names},
        "missing_value_policy": "markov_shadow_probability_null_is_research_only",
        "normalization_version": "v2.7.0.markov_probability_decimal_string",
        "model_input_schema_version": MODEL_INPUT_SCHEMA_VERSION,
        "decision_ts": decision_available_at,
        "feature_available_at_map": {name: decision_available_at for name in feature_names},
        "source_lineage_node_ids": [
            f"node:markov_snapshot:{snapshot.get('model_snapshot_id')}",
            "node:v27_event_log:lifecycle_transition_history",
        ],
        "feature_research_only": True,
    }
    return {"feature_vector_hash": sha256_hex(material), **material}


def _payload_for_record(record: Mapping[str, Any], snapshot: Mapping[str, Any], gate: Mapping[str, Any]) -> dict[str, Any]:
    identity = _record_identity(record)
    decision_available_at = str(gate["decision_available_at"])
    feature_vector = _feature_vector(snapshot, decision_available_at=decision_available_at)
    absorption = snapshot.get("absorption_forecast") if isinstance(snapshot.get("absorption_forecast"), dict) else {}
    forecast_id = f"markov_shadow:{identity['denominator_dedup_key']}:{gate['hard_gate_cutoff_seq']}"
    decision_audit_shadow = {
        "decision_id": forecast_id,
        "policy_bundle_id": POLICY_BUNDLE_ID,
        "feature_vector_hash": feature_vector["feature_vector_hash"],
        "decision_available_at": decision_available_at,
        "feature_max_available_at": decision_available_at,
        "entry_gate_allowed": False,
        "paper_entry_action": "none",
        "failure_action": "shadow_no_entry",
        "used_future_peak": False,
        "used_future_outcome": False,
        "used_posthoc_label": False,
        "forbidden_future_fields_used": [],
    }
    decision_audit_shadow["decision_trace_bundle_hash"] = sha256_hex(decision_audit_shadow)
    return {
        "schema_version": SCHEMA_VERSION,
        "shadow_forecast_id": forecast_id,
        **identity,
        "mode": "shadow",
        "forecast_scope": "post_d3a_reclaim_earliest_actionable_shadow_only",
        "entry_gate_allowed": False,
        "paper_entry_action": "none",
        "hard_gate_passed": True,
        "hard_gate_evidence": gate,
        "model_snapshot_id": snapshot.get("model_snapshot_id"),
        "model_family": snapshot.get("model_family"),
        "state_definition_version": snapshot.get("state_definition_version"),
        "start_state": snapshot.get("start_state"),
        "matrix_build_cutoff_seq": snapshot.get("matrix_build_cutoff_seq"),
        "sample_n": snapshot.get("sample_n"),
        "p_absorb_peak30": absorption.get("p_absorb_peak30"),
        "p_absorb_stop_before_peak": absorption.get("p_absorb_stop_before_peak"),
        "p_absorb_toxic_dead": absorption.get("p_absorb_toxic_dead"),
        "p_absorb_crash_dead": absorption.get("p_absorb_crash_dead"),
        "metric_ids": list(METRIC_IDS),
        "threshold_ids": list(THRESHOLD_IDS),
        "feature_vector_snapshot": feature_vector,
        "decision_audit_shadow": decision_audit_shadow,
        "markov_snapshot": snapshot,
    }


def _already_recorded_forecast_ids(event_log: V27EventLog) -> set[str]:
    forecast_ids = set()
    for event in event_log.iter_events() or []:
        if event.get("event_type") != MARKOV_SHADOW_EVENT_TYPE:
            continue
        payload = _payload(event)
        forecast_id = payload.get("shadow_forecast_id")
        if forecast_id:
            forecast_ids.add(str(forecast_id))
    return forecast_ids


def record_markov_shadow_forecasts(
    event_log_dir: str | Path = DEFAULT_EVENT_LOG_DIR,
    *,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict[str, Any]:
    event_log = V27EventLog(event_log_dir)
    events = list(event_log.iter_events() or [])
    lifecycle_events = v27_event_log_lifecycle_events(events)
    projection = build_denominator_projection(event_log_dir, include_records=True)
    records = projection.get("records") if isinstance(projection.get("records"), list) else []
    already_recorded = _already_recorded_forecast_ids(event_log)
    pending_specs = []
    skipped = []

    for record in records:
        gate = _hard_gate_evidence(record)
        identity = _record_identity(record)
        if not gate["passed"]:
            skipped.append(
                {
                    "denominator_dedup_key": identity["denominator_dedup_key"],
                    "blocking_reasons": gate["blocking_reasons"],
                }
            )
            continue
        snapshot = build_lifecycle_forecast_snapshot(
            lifecycle_events,
            start_state="RECLAIM_CONFIRMED",
            cutoff_seq=gate["hard_gate_cutoff_seq"],
            horizons=(1, 3, 5, 15),
            max_absorption_steps=60,
            model_snapshot_id=f"telegram_lifecycle_markov_shadow_v1_seq_{gate['hard_gate_cutoff_seq']}",
        )
        payload = _payload_for_record(record, snapshot, gate)
        if payload["shadow_forecast_id"] in already_recorded:
            skipped.append(
                {
                    "denominator_dedup_key": identity["denominator_dedup_key"],
                    "blocking_reasons": ["duplicate_shadow_forecast"],
                }
            )
            continue
        pending_specs.append(
            {
                "event_type": MARKOV_SHADOW_EVENT_TYPE,
                "aggregate_id": f"markov_shadow:{identity['denominator_dedup_key']}",
                "payload": payload,
                "source": SOURCE,
                "idempotency_key": f"{MARKOV_SHADOW_EVENT_TYPE}:{payload['shadow_forecast_id']}",
                "observed_at": gate["decision_available_at"],
                "available_at": gate["decision_available_at"],
                "causal_parent_event_id": (gate.get("earliest_actionable_time") or {}).get("source_event_id"),
            }
        )
        if limit is not None and len(pending_specs) >= int(limit):
            break

    appended = duplicate = 0
    results = []
    if dry_run:
        results = [{"status": "dry_run", "event": spec} for spec in pending_specs]
    else:
        results = event_log.append_events(pending_specs)
        for result in results:
            if result.get("status") == "appended":
                appended += 1
            elif result.get("status") == "duplicate":
                duplicate += 1

    return {
        "schema_version": SCHEMA_VERSION,
        "event_log_dir": str(event_log_dir),
        "input_events": len(events),
        "lifecycle_events": len(lifecycle_events),
        "projection_records": len(records),
        "eligible_shadow_forecasts": len(pending_specs),
        "appended": appended,
        "duplicate": duplicate,
        "dry_run": bool(dry_run),
        "skipped": skipped,
        "event_log_verify": None if dry_run else event_log.verify(),
        "results": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-log-dir", default=str(DEFAULT_EVENT_LOG_DIR))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    report = record_markov_shadow_forecasts(args.event_log_dir, dry_run=args.dry_run, limit=args.limit)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
