import json
import sys
from types import SimpleNamespace

sys.path.insert(0, "scripts")

import v27_read_model_refresh as read_model_refresh  # noqa: E402
from v27_event_log import V27EventLog  # noqa: E402
from v27_read_model_refresh import (  # noqa: E402
    acquire_loop_lock,
    refresh_denominator_read_model,
    run_refresh_loop,
    run_refresh_once_with_lock,
)


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
    # This refresh test verifies faithful materialization of the authoritative
    # matrix. Policy expectations for which modes should be allowed remain in
    # test_v27_mode_readiness.py and must not be weakened here to make P0-A pass.
    assert report["mode_readiness"]["normal_tiny_ready"] is mode_readiness["health"]["normal_tiny_ready"]
    assert report["mode_readiness"]["observe_only_ready"] is mode_readiness["health"]["observe_only_ready"]
    assert report["mode_readiness"]["highest_allowed_mode"] == mode_readiness["highest_allowed_mode"]
    assert (
        report["mode_readiness"]["blocking_contracts"]["observe_only"]
        == mode_readiness["modes"]["observe_only"]["blocking_contracts"]
    )
    assert report["health"]["normal_tiny_ready"] == report["mode_readiness"]["normal_tiny_ready"]
    assert mode_readiness["matrix_schema_version"] == "v2.7.0.mode_readiness.v1"
    assert mode_readiness["modes"]["normal_tiny"]["status"] == "blocked"
    assert health["health"]["normal_tiny_ready"] == report["mode_readiness"]["normal_tiny_ready"]
    assert projection["event_log_latest_seq"] == 1
    assert snapshot["read_model"]["read_model_seq"] == 1
    assert health["verifier_report"]["blocking_reasons"] == []
    for contract_id in (
        "ReplaySideEffectIsolationContract",
        "TransactionalOutboxContract",
        "DeadLetterQueueContract",
        "ConsumerCheckpointContract",
        "ProjectionHandlerIdempotencyContract",
        "CacheInvalidationContract",
    ):
        assert consumer_health["contracts"][contract_id]["status"] == "pass"
    replay = consumer_health["contracts"]["ReplaySideEffectIsolationContract"]["evidence"]
    assert replay["provider_calls_allowed"] is False
    assert replay["provider_call_count"] == 0
    assert replay["external_side_effect_count"] == 0
    assert replay["unexpected_write_target_count"] == 0
    assert replay["projection_hash_ok"] is True
    assert replay["snapshot_hash_ok"] is True
    assert "projection_consumer_health" in replay["write_targets_allowed"]
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


def refresh_loop_args(tmp_path, event_log_dir):
    output_dir = tmp_path / "read_models"
    return SimpleNamespace(
        event_log_dir=str(event_log_dir),
        output_dir=str(output_dir),
        projection_path=None,
        snapshot_path=None,
        health_path=None,
        mode_readiness_path=None,
        status_path=str(output_dir / "v27_read_model_worker_status.json"),
        spec_manifest=str(read_model_refresh.DEFAULT_SPEC_MANIFEST),
        include_records=False,
        max_allowed_lag_seq=0,
        max_allowed_lag_ms=300_000,
        max_snapshot_age_ms=300_000,
        interval=5,
        initial_delay=0,
        max_runs=1,
        lock_file=str(tmp_path / "v27_refresh.lock"),
        progress=False,
    )


def test_refresh_loop_writes_worker_status_without_promoting_modes(tmp_path):
    event_log_dir = tmp_path / "events"
    log = V27EventLog(event_log_dir)
    append_signal(log, "TokenA")
    args = refresh_loop_args(tmp_path, event_log_dir)

    report = run_refresh_loop(args)
    status = read_json(tmp_path / "read_models" / "v27_read_model_worker_status.json")
    readiness = read_json(tmp_path / "read_models" / "mode_readiness.json")

    assert report["refresh_schema_version"] == "v2.7.0.read_model_refresh.v1"
    assert status["schema_version"] == "v2.7.0.read_model_worker_status.v1"
    assert status["running"] is False
    assert status["status"] == "stopped"
    assert status["last_refresh_status"] in {"ok", "readiness_blocked"}
    assert status["last_success_at"]
    assert status["error_count"] == 0
    assert status["normal_tiny_ready"] is readiness["health"]["normal_tiny_ready"]
    assert status["highest_allowed_mode"] == readiness["highest_allowed_mode"]
    assert status["artifact_paths"]["mode_readiness"] == str(tmp_path / "read_models" / "mode_readiness.json")
    assert readiness["health"]["normal_tiny_ready"] is False


def test_refresh_loop_records_error_without_fake_success(tmp_path, monkeypatch):
    event_log_dir = tmp_path / "events"
    args = refresh_loop_args(tmp_path, event_log_dir)

    def fail_refresh(_args):
        raise RuntimeError("unit_refresh_failure")

    monkeypatch.setattr(read_model_refresh, "run_refresh_once", fail_refresh)
    report = run_refresh_loop(args)
    status = read_json(tmp_path / "read_models" / "v27_read_model_worker_status.json")

    assert report == {"status": "stopped_before_first_refresh"}
    assert status["running"] is False
    assert status["status"] == "stopped"
    assert status["last_refresh_status"] == "refresh_error"
    assert status["last_success_at"] is None
    assert status["error_count"] == 1
    assert status["last_error_at"]
    assert status["last_error"] == "RuntimeError:unit_refresh_failure"
    assert status["blocking_reasons"] == ["read_model_refresh_exception"]


def test_one_shot_refresh_fails_closed_when_continuous_worker_holds_lock(tmp_path):
    event_log_dir = tmp_path / "events"
    args = refresh_loop_args(tmp_path, event_log_dir)
    first = acquire_loop_lock(args.lock_file)
    assert first is not None
    try:
        report = run_refresh_once_with_lock(args)
    finally:
        first.close()

    assert report["health"]["status"] == "read_model_refresh_lock_held"
    assert report["health"]["dashboard_safe"] is False
    assert report["blocking_reasons"] == ["v27_read_model_refresh_lock_held"]
    assert report["lock_file"] == args.lock_file


def test_refresh_loop_lock_rejects_duplicate_worker(tmp_path):
    lock_path = tmp_path / "v27_refresh.lock"
    first = acquire_loop_lock(lock_path)
    assert first is not None
    owner_pid = lock_path.read_text(encoding="utf-8")
    try:
        assert acquire_loop_lock(lock_path) is None
        assert lock_path.read_text(encoding="utf-8") == owner_pid
    finally:
        first.close()

    second = acquire_loop_lock(lock_path)
    assert second is not None
    second.close()
