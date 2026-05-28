#!/usr/bin/env python3
"""Record v2.7 decision-audit evidence from bound quote intent records.

This recorder closes the ultra-tiny audit trail without opening a buy gate. It
turns an already-bound quote intent into an append-only `decision_audit_recorded`
event whose feature vector and trace bundle can be verified by the denominator
projection.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from v27_denominator_projection import build_denominator_projection  # noqa: E402
from v27_event_log import V27EventLog, sha256_hex  # noqa: E402


DEFAULT_EVENT_LOG_DIR = PROJECT_ROOT / "data" / "v27_event_log"
DECISION_AUDIT_EVENT_TYPE = "decision_audit_recorded"
SCHEMA_VERSION = "v2.7.0.decision_audit.v1"
SOURCE = "v27_decision_audit_evidence"
POLICY_BUNDLE_ID = "v2.7.0_ultra_tiny_entry_policy_shadow_seed"
DECISION_BOUNDARY = "ultra_tiny_entry_decision_shadow_seed"
SPEC_HASH = sha256_hex({"spec": "v2.7.0", "contract": "DecisionAudit", "schema_version": SCHEMA_VERSION})


def _payload(event: Mapping[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


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
        "symbol": record.get("symbol"),
        "chain": chain,
        "canonical_pool_group": pool,
        "lifecycle_epoch": epoch,
    }


def _epoch_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1_000_000_000_000:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    text = str(value).strip()
    if not text:
        return None
    try:
        ts = float(text)
    except ValueError:
        try:
            datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        return text
    if ts > 1_000_000_000_000:
        ts /= 1000.0
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _event_time_by_id(events: list[Mapping[str, Any]]) -> dict[str, str]:
    times: dict[str, str] = {}
    for event in events:
        event_id = event.get("event_id")
        if not event_id:
            continue
        event_time = event.get("available_at") or event.get("observed_at") or event.get("ingested_at")
        if event_time:
            times[str(event_id)] = str(event_time)
    return times


def _decision_available_at(
    quote: Mapping[str, Any],
    *,
    source_event_times: Mapping[str, str],
) -> str | None:
    source_event_id = quote.get("source_event_id")
    return (
        _epoch_to_iso(quote.get("quote_ts"))
        or (source_event_times.get(str(source_event_id)) if source_event_id else None)
    )


def _feature_vector(identity: Mapping[str, Any], quote: Mapping[str, Any], *, decision_available_at: str) -> dict[str, Any]:
    material = {
        "feature_names_ordered": [
            "canonical_pool_group",
            "chain",
            "intent_hash",
            "quote_binding_hash",
            "quote_hash",
            "quote_intent_id",
            "quote_mint",
            "quote_source",
            "route",
            "side",
            "size",
            "slippage_bps",
            "token_ca",
        ],
        "feature_values": {
            "canonical_pool_group": identity.get("canonical_pool_group"),
            "chain": identity.get("chain"),
            "intent_hash": quote.get("intent_hash"),
            "quote_binding_hash": quote.get("quote_binding_hash"),
            "quote_hash": quote.get("quote_hash"),
            "quote_intent_id": quote.get("quote_intent_id"),
            "quote_mint": quote.get("quote_mint"),
            "quote_source": quote.get("quote_source"),
            "route": quote.get("route"),
            "side": quote.get("side"),
            "size": quote.get("size"),
            "slippage_bps": quote.get("slippage_bps"),
            "token_ca": identity.get("token_ca"),
        },
        "missing_value_policy": "decision_audit_quote_binding_required",
        "normalization_version": "v2.7.0.decision_audit.quote_binding.v1",
        "decision_ts": decision_available_at,
        "feature_available_at_map": {
            "quote_intent_binding_contract": decision_available_at,
        },
        "source_lineage_node_ids": [
            f"node:v27_event:{quote.get('source_event_id')}",
            f"node:v27_denominator:{identity.get('denominator_dedup_key')}",
        ],
    }
    return material


def _payload_for_record(
    record: Mapping[str, Any],
    quote: Mapping[str, Any],
    *,
    decision_available_at: str,
) -> dict[str, Any]:
    identity = _record_identity(record)
    quote_intent_id = quote.get("quote_intent_id")
    decision_id = f"decision_audit:{identity['denominator_dedup_key']}:{quote_intent_id}:entry_decision"
    feature_vector = _feature_vector(identity, quote, decision_available_at=decision_available_at)
    feature_vector_hash = sha256_hex(feature_vector)
    trace_bundle = {
        "policy_bundle_id": POLICY_BUNDLE_ID,
        "spec_hash": SPEC_HASH,
        "decision_boundary": DECISION_BOUNDARY,
        "decision_id": decision_id,
        "source_event_ids": [quote.get("source_event_id")],
        "feature_vector_hash": feature_vector_hash,
        "feature_max_available_at": decision_available_at,
        "decision_available_at": decision_available_at,
        "entry_gate_allowed": False,
        "paper_entry_action": "none",
        "failure_action": "entry_rejected",
        "used_future_peak": False,
        "used_future_outcome": False,
        "used_posthoc_label": False,
        "forbidden_future_fields_used": [],
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "decision_audit_version": SCHEMA_VERSION,
        **identity,
        "decision_id": decision_id,
        "policy_bundle_id": POLICY_BUNDLE_ID,
        "spec_hash": SPEC_HASH,
        "feature_vector": feature_vector,
        "feature_vector_hash": feature_vector_hash,
        "decision_trace_bundle": trace_bundle,
        "decision_trace_bundle_hash": sha256_hex(trace_bundle),
        "decision_available_at": decision_available_at,
        "feature_max_available_at": decision_available_at,
        "entry_gate_allowed": False,
        "paper_entry_action": "none",
        "failure_action": "entry_rejected",
        "used_future_peak": False,
        "used_future_outcome": False,
        "used_posthoc_label": False,
        "forbidden_future_fields_used": [],
        "quote_intent_binding_contract": quote,
    }


def _already_recorded_decision_ids(event_log: V27EventLog) -> set[str]:
    decision_ids = set()
    for event in event_log.iter_events() or []:
        if event.get("event_type") != DECISION_AUDIT_EVENT_TYPE:
            continue
        decision_id = _payload(event).get("decision_id")
        if decision_id:
            decision_ids.add(str(decision_id))
    return decision_ids


def record_decision_audit_evidence(
    event_log_dir: str | Path = DEFAULT_EVENT_LOG_DIR,
    *,
    dry_run: bool = False,
    limit: int | None = None,
) -> dict[str, Any]:
    event_log = V27EventLog(event_log_dir)
    events = list(event_log.iter_events() or [])
    projection = build_denominator_projection(event_log_dir, include_records=True)
    records = projection.get("records") if isinstance(projection.get("records"), list) else []
    source_event_times = _event_time_by_id(events)
    already_recorded = _already_recorded_decision_ids(event_log)
    pending_specs = []
    skipped = []

    for record in records:
        identity = _record_identity(record)
        quote = record.get("quote_intent_binding_contract")
        if not isinstance(quote, dict):
            skipped.append({"denominator_dedup_key": identity["denominator_dedup_key"], "blocking_reasons": ["quote_intent_binding_missing"]})
            continue
        if quote.get("quote_intent_bound") is not True:
            skipped.append({"denominator_dedup_key": identity["denominator_dedup_key"], "blocking_reasons": ["quote_intent_not_bound"]})
            continue
        if record.get("decision_audit_contract"):
            skipped.append({"denominator_dedup_key": identity["denominator_dedup_key"], "blocking_reasons": ["decision_audit_contract_already_projected"]})
            continue
        decision_available_at = _decision_available_at(quote, source_event_times=source_event_times)
        if not decision_available_at:
            skipped.append({"denominator_dedup_key": identity["denominator_dedup_key"], "blocking_reasons": ["decision_available_at_missing"]})
            continue
        payload = _payload_for_record(record, quote, decision_available_at=decision_available_at)
        if payload["decision_id"] in already_recorded:
            skipped.append({"denominator_dedup_key": identity["denominator_dedup_key"], "blocking_reasons": ["duplicate_decision_audit"]})
            continue
        pending_specs.append(
            {
                "event_type": DECISION_AUDIT_EVENT_TYPE,
                "aggregate_id": f"decision_audit:{identity['denominator_dedup_key']}",
                "payload": payload,
                "source": SOURCE,
                "idempotency_key": f"{DECISION_AUDIT_EVENT_TYPE}:{payload['decision_id']}",
                "observed_at": decision_available_at,
                "available_at": decision_available_at,
                "causal_parent_event_id": quote.get("source_event_id"),
            }
        )
        if limit is not None and len(pending_specs) >= int(limit):
            break

    appended = duplicate = 0
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
        "projection_records": len(records),
        "eligible_decision_audits": len(pending_specs),
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
    report = record_decision_audit_evidence(args.event_log_dir, dry_run=args.dry_run, limit=args.limit)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
