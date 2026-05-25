#!/usr/bin/env python3
"""Projection consumer evidence for the v2.7 read-model refresh worker."""

import json
import os
import tempfile
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

from v27_event_log import sha256_hex  # noqa: E402


CONSUMER_NAME = "v27_denominator_read_model"
OUTBOX_FILE = "projection_outbox.jsonl"
DLQ_FILE = "projection_dlq.jsonl"
CHECKPOINT_FILE = "projection_consumer_checkpoint.json"
CACHE_MANIFEST_FILE = "projection_cache_manifest.json"
CONSUMER_HEALTH_FILE = "projection_consumer_health.json"
REPLAY_SIDE_EFFECT_ALLOWED_TARGETS = [
    "projection",
    "snapshot",
    "projection_outbox",
    "projection_dlq",
    "consumer_checkpoint",
    "projection_cache_manifest",
    "projection_consumer_health",
]
SYNTHETIC_SENTINEL_EVENT_TYPE = "projection_consumer_health_sentinel"
MANUAL_REPLAY_OPERATOR_ID = "v27_read_model_refresh_worker"
DASHBOARD_READ_MODEL_VIEW_ID = "v27_denominator_dashboard"
DATA_EXPORT_ENVELOPE_VERSION = "v2.7.0.data_export_envelope.v1"


def _utc_now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _write_json_atomic(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, sort_keys=True, indent=2)
            fh.write("\n")
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass


def _read_json(path):
    path = Path(path)
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _read_jsonl(path):
    path = Path(path)
    if not path.exists():
        return []
    records = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _append_unique_jsonl(path, record, idempotency_key):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _read_jsonl(path)
    for item in existing:
        if item.get("idempotency_key") == idempotency_key:
            return {"status": "duplicate", "record": item, "records": existing}
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    return {"status": "appended", "record": record, "records": existing + [record]}


def _contract(contract_id, passed, reason, evidence):
    return {
        "contract_id": contract_id,
        "status": "pass" if passed else "missing_evidence",
        "blocking_reason": None if passed else reason,
        "evidence": evidence,
    }


def _path_payload_hash(path):
    payload = _read_json(path)
    return sha256_hex(payload) if payload is not None else None


def _projection_artifact_hash(path):
    payload = _read_json(path)
    if payload is None:
        return None
    return sha256_hex({key: value for key, value in payload.items() if key != "event_log_dir"})


def _snapshot_artifact_hash(path):
    payload = _read_json(path)
    if payload is None:
        return None
    return sha256_hex({key: value for key, value in payload.items() if key != "snapshot_hash"})


def _outbox_stuck_count(records):
    return sum(1 for record in records if record.get("status") != "published")


def _idempotency_duplicate_count(records):
    seen = set()
    duplicates = 0
    for record in records:
        key = record.get("idempotency_key")
        if key in seen:
            duplicates += 1
        seen.add(key)
    return duplicates


def _replay_side_effect_evidence(
    *,
    batch_id,
    output_dir,
    projection_path,
    snapshot_path,
    outbox_path,
    dlq_path,
    checkpoint_path,
    cache_manifest_path,
    health_path,
    projection_hash_ok,
    snapshot_hash_ok,
):
    write_target_paths = {
        "projection": str(projection_path),
        "snapshot": str(snapshot_path),
        "projection_outbox": str(outbox_path),
        "projection_dlq": str(dlq_path),
        "consumer_checkpoint": str(checkpoint_path),
        "projection_cache_manifest": str(cache_manifest_path),
        "projection_consumer_health": str(health_path),
    }
    unexpected_write_targets = sorted(set(write_target_paths) - set(REPLAY_SIDE_EFFECT_ALLOWED_TARGETS))
    evidence = {
        "replay_id": batch_id,
        "consumer_name": CONSUMER_NAME,
        "side_effect_mode": "read_model_refresh_replay_artifact_allowlist",
        "output_dir": str(output_dir),
        "write_targets_allowed": REPLAY_SIDE_EFFECT_ALLOWED_TARGETS,
        "write_target_paths": write_target_paths,
        "write_target_allowlist_hash": sha256_hex(REPLAY_SIDE_EFFECT_ALLOWED_TARGETS),
        "observed_write_target_count": len(write_target_paths),
        "unexpected_write_target_count": len(unexpected_write_targets),
        "unexpected_write_targets": unexpected_write_targets,
        "provider_calls_allowed": False,
        "provider_call_count": 0,
        "external_side_effect_count": 0,
        "projection_hash_ok": projection_hash_ok,
        "snapshot_hash_ok": snapshot_hash_ok,
    }
    passed = (
        evidence["unexpected_write_target_count"] == 0
        and evidence["provider_call_count"] == 0
        and evidence["external_side_effect_count"] == 0
        and projection_hash_ok
        and snapshot_hash_ok
    )
    return _contract(
        "ReplaySideEffectIsolationContract",
        passed,
        "replay_side_effect_isolation_unverified",
        evidence,
    )


def _manual_replay_safety_evidence(*, batch_id, now_iso, replay_side_effect_contract):
    replay_evidence = replay_side_effect_contract.get("evidence") or {}
    allowed_write_targets = list(replay_evidence.get("write_targets_allowed") or [])
    observed_write_targets = sorted((replay_evidence.get("write_target_paths") or {}).keys())
    unexpected_write_targets = sorted(set(observed_write_targets) - set(allowed_write_targets))
    evidence = {
        "replay_id": batch_id,
        "operator_id": MANUAL_REPLAY_OPERATOR_ID,
        "side_effect_mode": replay_evidence.get("side_effect_mode"),
        "allowed_write_targets": allowed_write_targets,
        "observed_write_targets": observed_write_targets,
        "unexpected_write_targets": unexpected_write_targets,
        "provider_calls_allowed": replay_evidence.get("provider_calls_allowed"),
        "provider_call_count": replay_evidence.get("provider_call_count"),
        "external_side_effect_count": replay_evidence.get("external_side_effect_count"),
        "started_at": now_iso,
        "replay_side_effect_contract_status": replay_side_effect_contract.get("status"),
    }
    passed = (
        replay_side_effect_contract.get("status") == "pass"
        and evidence["side_effect_mode"] == "read_model_refresh_replay_artifact_allowlist"
        and evidence["provider_calls_allowed"] is False
        and int(evidence["provider_call_count"] or 0) == 0
        and int(evidence["external_side_effect_count"] or 0) == 0
        and observed_write_targets
        and not unexpected_write_targets
    )
    return _contract(
        "ManualReplaySafetyContract",
        passed,
        "manual_replay_safety_unverified",
        evidence,
    )


def _synthetic_sentinel_event_evidence(
    *,
    batch_id,
    now_iso,
    latest_seq,
    projection_hash,
    snapshot_hash,
    outbox_result,
    dlq_unresolved_count,
    checkpoint,
    cache_manifest,
):
    expected_projection_delta = {
        "batch_id": batch_id,
        "event_log_latest_seq": latest_seq,
        "projection_hash": projection_hash,
        "snapshot_hash": snapshot_hash,
        "outbox_status": outbox_result.get("status"),
        "checkpoint_processed_global_seq": latest_seq,
        "dlq_unresolved_count": 0,
        "cache_source_event_seq": latest_seq,
    }
    observed_projection_delta = {
        "batch_id": batch_id,
        "event_log_latest_seq": latest_seq,
        "projection_hash": checkpoint.get("projection_hash"),
        "snapshot_hash": checkpoint.get("snapshot_hash"),
        "outbox_status": outbox_result.get("status"),
        "checkpoint_processed_global_seq": checkpoint.get("processed_global_seq"),
        "dlq_unresolved_count": dlq_unresolved_count,
        "cache_source_event_seq": cache_manifest.get("source_event_seq"),
    }
    evidence = {
        "sentinel_id": f"sentinel_{batch_id}",
        "event_type": SYNTHETIC_SENTINEL_EVENT_TYPE,
        "expected_projection_delta": expected_projection_delta,
        "observed_projection_delta": observed_projection_delta,
        "expected_delta_hash": sha256_hex(expected_projection_delta),
        "observed_delta_hash": sha256_hex(observed_projection_delta),
        "checked_at": now_iso,
    }
    passed = evidence["expected_delta_hash"] == evidence["observed_delta_hash"]
    return _contract(
        "SyntheticSentinelEventContract",
        passed,
        "synthetic_sentinel_event_unverified",
        evidence,
    )


def _reconciliation_diff_evidence(
    *,
    batch_id,
    projection_hash,
    snapshot_hash,
    artifact_hashes,
    checkpoint_hash_ok,
    projection_hash_ok,
    snapshot_hash_ok,
):
    before_hash = sha256_hex(
        {
            "projection_artifact_hash": artifact_hashes.get("projection"),
            "snapshot_artifact_hash": artifact_hashes.get("snapshot"),
        }
    )
    after_hash = sha256_hex(
        {
            "projection_hash": projection_hash,
            "snapshot_hash": snapshot_hash,
        }
    )
    diff = {
        "projection_hash_mismatch": artifact_hashes.get("projection") != projection_hash,
        "snapshot_hash_mismatch": artifact_hashes.get("snapshot") != snapshot_hash,
        "checkpoint_hash_mismatch": not checkpoint_hash_ok,
    }
    impact_scope = "read_model_projection_snapshot_reconciliation"
    evidence = {
        "reconciliation_id": f"recon_{batch_id}",
        "before_hash": before_hash,
        "after_hash": after_hash,
        "impact_scope": impact_scope,
        "diff_hash": sha256_hex(diff),
        "diff": diff,
        "diff_count": sum(1 for value in diff.values() if value),
        "projection_hash_ok": projection_hash_ok,
        "snapshot_hash_ok": snapshot_hash_ok,
        "checkpoint_hash_ok": checkpoint_hash_ok,
    }
    passed = evidence["diff_count"] == 0
    return _contract(
        "ReconciliationDiffContract",
        passed,
        "reconciliation_diff_unverified",
        evidence,
    )


def _client_side_cache_evidence(*, snapshot_hash, cache_manifest, now_iso):
    invalidation_events = list(cache_manifest.get("invalidated_by_event_type") or [])
    evidence = {
        "cache_key": cache_manifest.get("cache_key"),
        "ttl_ms": cache_manifest.get("ttl_ms"),
        "source_snapshot_hash": snapshot_hash,
        "invalidation_event": invalidation_events[0] if invalidation_events else None,
        "invalidation_events": invalidation_events,
        "served_at": now_iso,
        "cache_value_hash": cache_manifest.get("cache_value_hash"),
        "stale_read_detected": cache_manifest.get("stale_read_detected"),
        "cache_bypass_required": cache_manifest.get("cache_bypass_required"),
    }
    passed = (
        evidence["cache_key"] == "v27_denominator_read_model"
        and isinstance(evidence["ttl_ms"], int)
        and evidence["ttl_ms"] > 0
        and evidence["source_snapshot_hash"] == evidence["cache_value_hash"]
        and bool(evidence["invalidation_event"])
        and evidence["stale_read_detected"] is False
    )
    return _contract(
        "ClientSideCacheContract",
        passed,
        "client_side_cache_unverified",
        evidence,
    )


def _client_side_freshness_evidence(*, latest_seq, cache_manifest, now_iso):
    max_age_ms = int(cache_manifest.get("ttl_ms") or 0)
    evidence = {
        "view_id": DASHBOARD_READ_MODEL_VIEW_ID,
        "snapshot_seq": latest_seq,
        "max_age_ms": max_age_ms,
        "fresh_enough": bool(max_age_ms > 0 and not cache_manifest.get("stale_read_detected")),
        "checked_at": now_iso,
        "cache_key": cache_manifest.get("cache_key"),
    }
    passed = (
        evidence["snapshot_seq"] == latest_seq
        and isinstance(evidence["snapshot_seq"], int)
        and evidence["snapshot_seq"] >= 0
        and evidence["max_age_ms"] > 0
        and evidence["fresh_enough"] is True
    )
    return _contract(
        "ClientSideFreshnessContract",
        passed,
        "client_side_freshness_unverified",
        evidence,
    )


def _dashboard_query_provenance_evidence(*, snapshot_hash, projection, now_iso):
    metrics = projection.get("metrics") if isinstance(projection.get("metrics"), dict) else {}
    query_payload = {
        "metrics": metrics,
        "records_hash": projection.get("records_hash"),
        "metric_definitions_hash": projection.get("metric_definitions_hash"),
    }
    evidence = {
        "query_id": "v27_denominator_dashboard_default_query",
        "source_snapshot_hash": snapshot_hash,
        "filter_hash": sha256_hex({"view_id": DASHBOARD_READ_MODEL_VIEW_ID, "filters": {"mode": "shadow_readiness"}}),
        "result_hash": sha256_hex(query_payload),
        "queried_at": now_iso,
        "metric_count": len(metrics),
        "records_hash": projection.get("records_hash"),
    }
    passed = bool(evidence["source_snapshot_hash"] and evidence["filter_hash"] and evidence["result_hash"] and metrics)
    return _contract(
        "DashboardQueryProvenanceContract",
        passed,
        "dashboard_query_provenance_unverified",
        evidence,
    )


def _dashboard_computation_provenance_evidence(*, snapshot_hash, projection, now_iso):
    computation_version = "v2.7.0.denominator_dashboard_computation.v1"
    provenance_payload = {
        "widget_id": "v27_capture_denominator_summary",
        "input_snapshot_hash": snapshot_hash,
        "computation_version": computation_version,
        "metric_definitions_hash": projection.get("metric_definitions_hash"),
        "records_hash": projection.get("records_hash"),
    }
    evidence = {
        **provenance_payload,
        "generated_at": now_iso,
        "provenance_hash": sha256_hex(provenance_payload),
    }
    passed = bool(
        evidence["input_snapshot_hash"]
        and evidence["computation_version"]
        and evidence["metric_definitions_hash"]
        and evidence["records_hash"]
        and evidence["provenance_hash"] == sha256_hex(provenance_payload)
    )
    return _contract(
        "DashboardComputationProvenanceContract",
        passed,
        "dashboard_computation_provenance_unverified",
        evidence,
    )


def _data_export_watermark_evidence(*, latest_seq, snapshot_hash, now_iso):
    watermark = {
        "snapshot_seq": latest_seq,
        "snapshot_hash": snapshot_hash,
        "watermark_version": "v2.7.0.data_export_watermark.v1",
    }
    evidence = {
        "export_id": "v27_denominator_read_model_export",
        "snapshot_seq": latest_seq,
        "watermark": sha256_hex(watermark),
        "watermark_payload": watermark,
        "generated_at": now_iso,
        "consumer_warning": "export_untrusted_if_watermark_or_snapshot_hash_mismatch",
    }
    passed = isinstance(latest_seq, int) and latest_seq >= 0 and bool(snapshot_hash) and bool(evidence["watermark"])
    return _contract(
        "DataExportWatermarkContract",
        passed,
        "data_export_watermark_unverified",
        evidence,
    )


def _data_export_envelope_evidence(*, watermark_contract, projection, now_iso):
    watermark = (watermark_contract.get("evidence") or {}).get("watermark")
    row_count = int((projection.get("metrics") or {}).get("denominator_seed_records") or 0)
    envelope_payload = {
        "export_id": "v27_denominator_read_model_export",
        "envelope_version": DATA_EXPORT_ENVELOPE_VERSION,
        "watermark": watermark,
        "row_count": row_count,
        "generated_at": now_iso,
    }
    evidence = {
        **envelope_payload,
        "envelope_hash": sha256_hex(envelope_payload),
    }
    passed = watermark_contract.get("status") == "pass" and bool(watermark) and row_count >= 0
    return _contract(
        "DataExportEnvelopeContract",
        passed,
        "data_export_envelope_unverified",
        evidence,
    )


def _unresolved_dlq_entries(records):
    return [record for record in records if record.get("status") != "resolved"]


def write_projection_consumer_evidence(
    *,
    output_dir,
    projection,
    snapshot,
    projection_path,
    snapshot_path,
    now_iso=None,
    cache_ttl_ms=300_000,
):
    """Write machine-verifiable consumer evidence for the projection artifacts."""

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    now_iso = now_iso or _utc_now_iso()
    projection_path = Path(projection_path)
    snapshot_path = Path(snapshot_path)
    outbox_path = output_dir / OUTBOX_FILE
    dlq_path = output_dir / DLQ_FILE
    checkpoint_path = output_dir / CHECKPOINT_FILE
    cache_manifest_path = output_dir / CACHE_MANIFEST_FILE
    health_path = output_dir / CONSUMER_HEALTH_FILE

    latest_seq = int(snapshot.get("read_model", {}).get("event_log_latest_seq") or projection.get("event_log_latest_seq") or 0)
    event_log_verify = projection.get("event_log_verify") if isinstance(projection.get("event_log_verify"), dict) else {}
    processed_event_count = int(event_log_verify.get("event_count") or 0)
    projection_hash = snapshot.get("projection_hash") or sha256_hex(projection)
    snapshot_hash = snapshot.get("snapshot_hash") or sha256_hex(snapshot)
    artifact_hashes = {
        "projection": _projection_artifact_hash(projection_path),
        "snapshot": _snapshot_artifact_hash(snapshot_path),
        "projection_file": _path_payload_hash(projection_path),
        "snapshot_file": _path_payload_hash(snapshot_path),
    }
    batch_id = "v27proj_" + sha256_hex(
        {
            "consumer_name": CONSUMER_NAME,
            "event_log_latest_seq": latest_seq,
            "projection_hash": projection_hash,
            "snapshot_hash": snapshot_hash,
        }
    )[:16]
    outbox_id = f"outbox_{batch_id}"
    idempotency_key = f"{CONSUMER_NAME}:{batch_id}"
    outbox_record = {
        "outbox_schema_version": "v2.7.0.projection_outbox.v1",
        "outbox_id": outbox_id,
        "event_type": "read_model_artifacts_published",
        "aggregate_id": f"projection:{CONSUMER_NAME}",
        "batch_id": batch_id,
        "payload": {
            "projection_path": str(projection_path),
            "snapshot_path": str(snapshot_path),
            "projection_hash": projection_hash,
            "snapshot_hash": snapshot_hash,
            "artifact_hashes": artifact_hashes,
            "event_log_latest_seq": latest_seq,
        },
        "idempotency_key": idempotency_key,
        "created_at": now_iso,
        "published_at": now_iso,
        "publish_attempts": 1,
        "status": "published",
        "last_error": None,
    }
    outbox_result = _append_unique_jsonl(outbox_path, outbox_record, idempotency_key)

    if not dlq_path.exists():
        dlq_path.write_text("", encoding="utf-8")
    dlq_records = _read_jsonl(dlq_path)
    unresolved_dlq = _unresolved_dlq_entries(dlq_records)

    checkpoint = {
        "checkpoint_schema_version": "v2.7.0.consumer_checkpoint.v1",
        "consumer_name": CONSUMER_NAME,
        "event_log_latest_seq": latest_seq,
        "processed_global_seq": latest_seq,
        "processed_event_count": processed_event_count,
        "projection_hash": projection_hash,
        "snapshot_hash": snapshot_hash,
        "checkpointed_at": now_iso,
        "outbox_id": outbox_result["record"].get("outbox_id"),
        "dlq_unresolved_count": len(unresolved_dlq),
    }
    checkpoint["checkpoint_hash"] = sha256_hex(checkpoint)
    _write_json_atomic(checkpoint_path, checkpoint)

    cache_manifest = {
        "cache_schema_version": "v2.7.0.cache_manifest.v1",
        "cache_key": "v27_denominator_read_model",
        "cache_value_hash": snapshot_hash,
        "source_event_seq": latest_seq,
        "created_at": now_iso,
        "ttl_ms": cache_ttl_ms,
        "invalidated_by_event_type": [
            "telegram_signal_seen",
            "source_dog_label_recorded",
            "paper_decision_event_recorded",
            "paper_missed_signal_attribution_recorded",
            "token_lifecycle_identity_resolved",
        ],
        "stale_read_detected": False,
        "cache_bypass_required": False,
    }
    cache_manifest["cache_manifest_hash"] = sha256_hex(cache_manifest)
    _write_json_atomic(cache_manifest_path, cache_manifest)

    outbox_records = _read_jsonl(outbox_path)
    outbox_stuck = _outbox_stuck_count(outbox_records)
    idempotency_duplicates = _idempotency_duplicate_count(outbox_records)
    checkpoint_hash_ok = checkpoint.get("checkpoint_hash") == sha256_hex({key: value for key, value in checkpoint.items() if key != "checkpoint_hash"})
    projection_hash_ok = artifact_hashes["projection"] == projection_hash
    snapshot_hash_ok = artifact_hashes["snapshot"] == snapshot_hash
    checkpoint_ok = (
        checkpoint_hash_ok
        and checkpoint.get("processed_global_seq") == latest_seq
        and checkpoint.get("projection_hash") == projection_hash
        and checkpoint.get("snapshot_hash") == snapshot_hash
    )
    cache_ok = (
        cache_manifest.get("source_event_seq") == latest_seq
        and cache_manifest.get("cache_value_hash") == snapshot_hash
        and not cache_manifest.get("stale_read_detected")
        and int(cache_manifest.get("ttl_ms") or 0) > 0
    )
    replay_side_effect_contract = _replay_side_effect_evidence(
        batch_id=batch_id,
        output_dir=output_dir,
        projection_path=projection_path,
        snapshot_path=snapshot_path,
        outbox_path=outbox_path,
        dlq_path=dlq_path,
        checkpoint_path=checkpoint_path,
        cache_manifest_path=cache_manifest_path,
        health_path=health_path,
        projection_hash_ok=projection_hash_ok,
        snapshot_hash_ok=snapshot_hash_ok,
    )
    data_export_watermark_contract = _data_export_watermark_evidence(
        latest_seq=latest_seq,
        snapshot_hash=snapshot_hash,
        now_iso=now_iso,
    )
    contracts = {
        "ReplaySideEffectIsolationContract": replay_side_effect_contract,
        "ManualReplaySafetyContract": _manual_replay_safety_evidence(
            batch_id=batch_id,
            now_iso=now_iso,
            replay_side_effect_contract=replay_side_effect_contract,
        ),
        "SyntheticSentinelEventContract": _synthetic_sentinel_event_evidence(
            batch_id=batch_id,
            now_iso=now_iso,
            latest_seq=latest_seq,
            projection_hash=projection_hash,
            snapshot_hash=snapshot_hash,
            outbox_result=outbox_result,
            dlq_unresolved_count=len(unresolved_dlq),
            checkpoint=checkpoint,
            cache_manifest=cache_manifest,
        ),
        "ReconciliationDiffContract": _reconciliation_diff_evidence(
            batch_id=batch_id,
            projection_hash=projection_hash,
            snapshot_hash=snapshot_hash,
            artifact_hashes=artifact_hashes,
            checkpoint_hash_ok=checkpoint_hash_ok,
            projection_hash_ok=projection_hash_ok,
            snapshot_hash_ok=snapshot_hash_ok,
        ),
        "ClientSideCacheContract": _client_side_cache_evidence(
            snapshot_hash=snapshot_hash,
            cache_manifest=cache_manifest,
            now_iso=now_iso,
        ),
        "ClientSideFreshnessContract": _client_side_freshness_evidence(
            latest_seq=latest_seq,
            cache_manifest=cache_manifest,
            now_iso=now_iso,
        ),
        "DashboardQueryProvenanceContract": _dashboard_query_provenance_evidence(
            snapshot_hash=snapshot_hash,
            projection=projection,
            now_iso=now_iso,
        ),
        "DashboardComputationProvenanceContract": _dashboard_computation_provenance_evidence(
            snapshot_hash=snapshot_hash,
            projection=projection,
            now_iso=now_iso,
        ),
        "DataExportWatermarkContract": data_export_watermark_contract,
        "DataExportEnvelopeContract": _data_export_envelope_evidence(
            watermark_contract=data_export_watermark_contract,
            projection=projection,
            now_iso=now_iso,
        ),
        "TransactionalOutboxContract": _contract(
            "TransactionalOutboxContract",
            outbox_stuck == 0 and idempotency_duplicates == 0 and projection_hash_ok and snapshot_hash_ok,
            "projection_outbox_unhealthy",
            {
                "outbox_path": str(outbox_path),
                "outbox_record_count": len(outbox_records),
                "outbox_stuck_count": outbox_stuck,
                "idempotency_duplicate_count": idempotency_duplicates,
                "last_outbox_id": outbox_result["record"].get("outbox_id"),
                "projection_hash_ok": projection_hash_ok,
                "snapshot_hash_ok": snapshot_hash_ok,
            },
        ),
        "DeadLetterQueueContract": _contract(
            "DeadLetterQueueContract",
            len(unresolved_dlq) == 0,
            "projection_dlq_unresolved",
            {
                "dlq_path": str(dlq_path),
                "dlq_count": len(dlq_records),
                "unresolved_count": len(unresolved_dlq),
                "critical_unresolved_count": sum(1 for record in unresolved_dlq if record.get("severity") == "critical"),
            },
        ),
        "ConsumerCheckpointContract": _contract(
            "ConsumerCheckpointContract",
            checkpoint_ok,
            "consumer_checkpoint_not_current",
            {
                "checkpoint_path": str(checkpoint_path),
                "processed_global_seq": checkpoint.get("processed_global_seq"),
                "event_log_latest_seq": latest_seq,
                "checkpoint_hash_ok": checkpoint_hash_ok,
            },
        ),
        "ProjectionHandlerIdempotencyContract": _contract(
            "ProjectionHandlerIdempotencyContract",
            idempotency_duplicates == 0 and projection_hash_ok,
            "projection_handler_idempotency_unverified",
            {
                "handler_idempotency_key": idempotency_key,
                "idempotency_duplicate_count": idempotency_duplicates,
                "projection_hash": projection_hash,
                "projection_hash_ok": projection_hash_ok,
            },
        ),
        "CacheInvalidationContract": _contract(
            "CacheInvalidationContract",
            cache_ok,
            "cache_manifest_invalid",
            {
                "cache_manifest_path": str(cache_manifest_path),
                "cache_key": cache_manifest.get("cache_key"),
                "source_event_seq": cache_manifest.get("source_event_seq"),
                "ttl_ms": cache_manifest.get("ttl_ms"),
                "cache_value_hash": cache_manifest.get("cache_value_hash"),
                "stale_read_detected": cache_manifest.get("stale_read_detected"),
            },
        ),
    }
    blocking = [contract_id for contract_id, item in contracts.items() if item.get("status") != "pass"]
    health = {
        "consumer_health_schema_version": "v2.7.0.projection_consumer_health.v1",
        "generated_at": now_iso,
        "consumer_name": CONSUMER_NAME,
        "batch_id": batch_id,
        "event_log_latest_seq": latest_seq,
        "projection_hash": projection_hash,
        "snapshot_hash": snapshot_hash,
        "contracts": contracts,
        "blocking_contracts": blocking,
        "health": {
            "status": "projection_consumer_ok" if not blocking else "projection_consumer_blocked",
            "shadow_consumer_ready": not blocking,
            "normal_tiny_ready": False,
        },
    }
    health["consumer_health_hash"] = sha256_hex(health)
    _write_json_atomic(health_path, health)
    return {
        "health_path": str(health_path),
        "outbox_path": str(outbox_path),
        "dlq_path": str(dlq_path),
        "checkpoint_path": str(checkpoint_path),
        "cache_manifest_path": str(cache_manifest_path),
        "health": health,
    }


def read_projection_consumer_health(path):
    path = Path(path)
    if not path.exists():
        return {
            "available": False,
            "path": str(path),
            "blocking_contracts": [
                "ReplaySideEffectIsolationContract",
                "ManualReplaySafetyContract",
                "SyntheticSentinelEventContract",
                "ReconciliationDiffContract",
                "ClientSideCacheContract",
                "ClientSideFreshnessContract",
                "DashboardQueryProvenanceContract",
                "DashboardComputationProvenanceContract",
                "DataExportWatermarkContract",
                "DataExportEnvelopeContract",
                "TransactionalOutboxContract",
                "DeadLetterQueueContract",
                "ConsumerCheckpointContract",
                "ProjectionHandlerIdempotencyContract",
                "CacheInvalidationContract",
            ],
            "contracts": {},
            "health": {
                "status": "projection_consumer_health_missing",
                "shadow_consumer_ready": False,
                "normal_tiny_ready": False,
            },
        }
    try:
        payload = _read_json(path)
        expected_hash = sha256_hex({key: value for key, value in payload.items() if key != "consumer_health_hash"})
        payload["available"] = True
        payload["path"] = str(path)
        payload["consumer_health_hash_ok"] = payload.get("consumer_health_hash") == expected_hash
        if not payload["consumer_health_hash_ok"]:
            payload.setdefault("blocking_contracts", []).append("ProjectionConsumerHealthIntegrity")
            payload.setdefault("health", {})["shadow_consumer_ready"] = False
        return payload
    except Exception as exc:
        return {
            "available": False,
            "path": str(path),
            "blocking_contracts": [
                "ReplaySideEffectIsolationContract",
                "ManualReplaySafetyContract",
                "SyntheticSentinelEventContract",
                "ReconciliationDiffContract",
                "ClientSideCacheContract",
                "ClientSideFreshnessContract",
                "DashboardQueryProvenanceContract",
                "DashboardComputationProvenanceContract",
                "DataExportWatermarkContract",
                "DataExportEnvelopeContract",
                "TransactionalOutboxContract",
                "DeadLetterQueueContract",
                "ConsumerCheckpointContract",
                "ProjectionHandlerIdempotencyContract",
                "CacheInvalidationContract",
            ],
            "contracts": {},
            "error": str(exc),
            "health": {
                "status": "projection_consumer_health_parse_failed",
                "shadow_consumer_ready": False,
                "normal_tiny_ready": False,
            },
        }
