import json
import sys

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_read_model_refresh import acquire_loop_lock, refresh_denominator_read_model  # noqa: E402


def append_signal(log, token_ca="TokenA"):
    return log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id=f"telegram_signal:solana:{token_ca}:unknown_pool:0",
        idempotency_key=f"premium_signals:{token_ca}",
        payload={
            "telegram_signal_id": token_ca,
            "token_ca": token_ca,
            "symbol": token_ca[-4:],
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_refresh_writes_projection_snapshot_and_health_atomically_consumable(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    log = V27EventLog(event_log_dir)
    append_signal(log, "TokenA")

    report = refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    projection = read_json(out_dir / "denominator_projection.json")
    snapshot = read_json(out_dir / "denominator_snapshot.json")
    health = read_json(out_dir / "denominator_freshness.json")
    mode_readiness = read_json(out_dir / "mode_readiness.json")
    consumer_health = read_json(out_dir / "projection_consumer_health.json")
    checkpoint = read_json(out_dir / "projection_consumer_checkpoint.json")
    cache_manifest = read_json(out_dir / "projection_cache_manifest.json")
    assert report["health"]["status"] == "read_model_refresh_ok"
    assert report["dashboard_safe"] is True
    assert report["read_model_seq"] == 1
    assert report["event_log_latest_seq"] == 1
    assert report["projection_consumer"]["status"] == "projection_consumer_ok"
    assert report["projection_consumer"]["shadow_consumer_ready"] is True
    assert report["projection_consumer"]["blocking_contracts"] == []
    assert report["snapshot_hash"] == snapshot["snapshot_hash"]
    assert health["snapshot_hash"] == snapshot["snapshot_hash"]
    assert health["projection_hash"] == snapshot["projection_hash"]
    assert health["projection_consumer_health_path"] == str(out_dir / "projection_consumer_health.json")
    assert report["mode_readiness_path"] == str(out_dir / "mode_readiness.json")
    assert report["mode_readiness"]["normal_tiny_ready"] is False
    assert report["mode_readiness"]["observe_only_ready"] is True
    assert report["mode_readiness"]["highest_allowed_mode"] == "observe_only"
    assert report["mode_readiness"]["blocking_contracts"]["observe_only"] == []
    assert report["health"]["normal_tiny_ready"] == report["mode_readiness"]["normal_tiny_ready"]
    assert mode_readiness["matrix_schema_version"] == "v2.7.0.mode_readiness.v1"
    assert mode_readiness["modes"]["normal_tiny"]["status"] == "blocked"
    assert health["health"]["normal_tiny_ready"] == report["mode_readiness"]["normal_tiny_ready"]
    assert projection["event_log_latest_seq"] == 1
    assert snapshot["read_model"]["read_model_seq"] == 1
    assert health["verifier_report"]["blocking_reasons"] == []
    for contract_id in (
        "TransactionalOutboxContract",
        "DeadLetterQueueContract",
        "ConsumerCheckpointContract",
        "ProjectionHandlerIdempotencyContract",
        "CacheInvalidationContract",
    ):
        assert consumer_health["contracts"][contract_id]["status"] == "pass"
    assert checkpoint["processed_global_seq"] == 1
    assert checkpoint["projection_hash"] == snapshot["projection_hash"]
    assert cache_manifest["source_event_seq"] == 1
    assert cache_manifest["cache_value_hash"] == snapshot["snapshot_hash"]


def test_refresh_health_report_blocks_invalid_spec_manifest(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    bad_spec_manifest = tmp_path / "missing_spec.manifest.json"
    log = V27EventLog(event_log_dir)
    append_signal(log, "TokenA")

    report = refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        spec_manifest_path=bad_spec_manifest,
        max_snapshot_age_ms=300_000,
    )

    health = read_json(out_dir / "denominator_freshness.json")
    mode_readiness = read_json(out_dir / "mode_readiness.json")
    assert report["dashboard_safe"] is False
    assert report["health"]["status"] == "read_model_refresh_not_ready"
    assert "spec_invalid" in report["blocking_reasons"]
    assert health["dashboard_safe"] is False
    assert "spec_invalid" in health["verifier_report"]["blocking_reasons"]
    assert mode_readiness["contract_statuses"]["CanonicalSpecIntegrityContract"]["status"] == "fail"


def test_refresh_writes_fail_closed_health_for_invalid_event_log(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    log = V27EventLog(event_log_dir)
    append_signal(log, "TokenA")
    event_path = event_log_dir / "events.jsonl"
    original_line = event_path.read_text(encoding="utf-8").strip()
    event_path.write_text(original_line + "\n" + original_line + "\n", encoding="utf-8")

    report = refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    projection = read_json(out_dir / "denominator_projection.json")
    health = read_json(out_dir / "denominator_freshness.json")
    assert projection["health"]["status"] == "event_log_invalid"
    assert report["dashboard_safe"] is False
    assert report["health"]["status"] == "read_model_refresh_not_ready"
    assert "projection_status_event_log_invalid" in report["blocking_reasons"]
    assert "event_log_empty" in report["blocking_reasons"]
    assert health["verifier_report"]["projection_status"] == "event_log_invalid"


def test_refresh_projection_outbox_is_idempotent_for_same_batch(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    log = V27EventLog(event_log_dir)
    append_signal(log, "TokenA")

    first = refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )
    second = refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    outbox_records = read_jsonl(out_dir / "projection_outbox.jsonl")
    consumer_health = read_json(out_dir / "projection_consumer_health.json")
    idempotency_keys = [record["idempotency_key"] for record in outbox_records]

    assert first["projection_hash"] == second["projection_hash"]
    assert len(idempotency_keys) == len(set(idempotency_keys))
    assert all(record["status"] == "published" for record in outbox_records)
    assert consumer_health["contracts"]["TransactionalOutboxContract"]["status"] == "pass"
    assert consumer_health["contracts"]["ProjectionHandlerIdempotencyContract"]["evidence"]["idempotency_duplicate_count"] == 0


def test_refresh_loop_lock_rejects_duplicate_worker(tmp_path):
    lock_path = tmp_path / "v27_refresh.lock"
    first = acquire_loop_lock(lock_path)
    assert first is not None
    try:
        assert acquire_loop_lock(lock_path) is None
    finally:
        first.close()

    second = acquire_loop_lock(lock_path)
    assert second is not None
    second.close()
