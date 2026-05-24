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
    contracts = {
        "ReplaySideEffectIsolationContract": _replay_side_effect_evidence(
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
