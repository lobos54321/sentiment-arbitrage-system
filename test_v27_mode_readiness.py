import sys

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog, sha256_hex  # noqa: E402
import v27_mode_readiness as mode_readiness_module  # noqa: E402
from v27_mode_readiness import build_mode_readiness_matrix  # noqa: E402
from v27_read_model_refresh import refresh_denominator_read_model  # noqa: E402


def append_seed_events(event_log_dir):
    log = V27EventLog(event_log_dir)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenReady:unknown_pool:0",
        idempotency_key="premium_signals:TokenReady",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenReady:unknown_pool:0",
        idempotency_key="source_label:TokenReady",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "gold",
            "source_dog_label_version": "unit_seed",
            "source_label_quality": "unit_seed",
            "source_label_research_only": True,
            "source_reference_price_type": "unit_seed",
            "source_reference_price": 0.001,
            "source_label_window": "24h",
            "source_peak_type": "unit_peak",
            "source_peak_value": 1.25,
            "source_label_available_at": 1_700_000_000,
        },
    )


def append_realtime_clean_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="realtime_clean_detector_recorded",
        aggregate_id="realtime_clean:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="realtime_clean_detector:1:legacy_round_trip_quote_clean_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "quote_intent_id": 1,
            "side": "buy",
            "size": 1.0,
            "route": "unit_route",
            "pool": "unknown_pool",
            "quote_mint": "SOL",
            "slippage_bps": 25,
            "quote_source": "paper_trade_round_trip_quote",
            "quote_age_sec": 1.0,
            "decision_available_at": 1_700_000_003,
            "entry_quote_available": True,
            "entry_quote_available_at": 1_700_000_002,
            "entry_quote_price": 0.001,
            "exit_quote_available": True,
            "exit_quote_available_at": 1_700_000_004,
            "exit_quote_price": 0.0012,
            "clean_standard_version": "legacy_round_trip_quote_clean_v0.1",
            "clean_observation_type": "TRADABLE_CLEAN_OBSERVED",
            "realtime_clean": True,
            "realtime_clean_detector_version": "legacy_round_trip_quote_clean_v0.1",
            "used_future_peak": False,
            "used_future_outcome": False,
            "used_posthoc_label": False,
            "forbidden_future_fields_used": [],
        },
    )


def append_quote_intent_binding_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="quote_intent_binding_recorded",
        aggregate_id="quote_intent_binding:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="quote_intent_binding:1:legacy_paper_trade_quote_intent_binding_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "binding_policy_version": "legacy_paper_trade_quote_intent_binding_v0.1",
            "quote_intent_binding_version": "legacy_paper_trade_quote_intent_binding_v0.1",
            "quote_intent_id": 1,
            "side": "buy",
            "size": 0.01,
            "route": "unit_route",
            "pool": "unknown_pool",
            "quote_mint": "SOL",
            "slippage_bps": 25,
            "quote_ts": 1_700_000_002,
            "quote_source": "paper_trade_entry_quote_or_legacy_proxy",
            "quote_binding_proof_level": "entry_execution_audit",
            "quote_intent_binding_quality": "entry_execution_audit_bound",
            "quote_intent_bound": True,
            "intent_hash": "intent-hash",
            "quote_hash": "quote-hash",
            "quote_binding_hash": "binding-hash",
            "missing_fields": [],
            "mismatch_fields": [],
            "used_future_peak": False,
            "used_future_outcome": False,
            "used_posthoc_label": False,
            "forbidden_future_fields_used": [],
        },
    )


def append_raw_provider_evidence_event(event_log_dir):
    request_metadata = {
        "paper_trade_id": 1,
        "side": "entry",
        "provider": "jupiter_ultra",
        "endpoint": "/ultra/v1/order",
        "request_id": "provider-request-ready",
        "request_parameters": {
            "input_mint": "SOL",
            "output_mint": "TokenReady",
            "input_amount": 0.01,
            "slippage_bps": 25,
        },
    }
    raw_response = {
        "requestId": "provider-request-ready",
        "transaction": "base64-tx",
        "outAmount": "1000000",
    }
    V27EventLog(event_log_dir).append_event(
        event_type="raw_provider_evidence_recorded",
        aggregate_id="raw_provider_evidence:solana:TokenReady:unknown_pool:0:1:entry",
        idempotency_key="raw_provider_evidence:1:entry:legacy_paper_raw_provider_evidence_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "raw_provider_evidence_version": "legacy_paper_raw_provider_evidence_v0.1",
            "provider_evidence_version": "legacy_paper_raw_provider_evidence_v0.1",
            "provider": "jupiter_ultra",
            "endpoint": "/ultra/v1/order",
            "request_id": "provider-request-ready",
            "provider_request_id": "provider-request-ready",
            "side": "entry",
            "latency_ms": 123,
            "request_parameters": request_metadata["request_parameters"],
            "request_metadata": request_metadata,
            "request_metadata_available": True,
            "request_metadata_hash": sha256_hex(request_metadata),
            "request_hash": sha256_hex(request_metadata),
            "response_hash": sha256_hex(raw_response),
            "raw_response_hash": sha256_hex(raw_response),
            "raw_response_available": True,
            "response_material_type": "execution._rawOrder",
            "hash_algorithm": "sha256(canonical_json)",
            "evidence_source": "unit",
            "provider_evidence_proof_level": "provider_request_id_with_raw_response_hash",
            "provider_evidence_trusted": True,
            "decision_available_at": "2026-01-15T00:00:02Z",
        },
    )


def append_randomness_control_event(event_log_dir):
    assignment_id = "normal-tiny-policy-v1"
    V27EventLog(event_log_dir).append_event(
        event_type="randomness_control_recorded",
        aggregate_id=f"randomness_control:{assignment_id}",
        idempotency_key=f"randomness_control:{assignment_id}:v2.7.0.randomness_control.v1",
        payload={
            "rng_seed": "sha256:normal-tiny-policy-seed",
            "rng_version": "v2.7.0.randomness_control.v1",
            "randomization_unit": "normal_tiny_promotion_policy",
            "assignment_id": assignment_id,
            "assignment_status": "deterministic_policy",
            "randomization_enabled": False,
            "deterministic_assignment": True,
            "assignment_algorithm": "deterministic_no_randomized_assignment",
            "assigned_bucket": "normal_tiny_candidate",
            "assignment_hash": sha256_hex({"assignment_id": assignment_id}),
            "evidence_source": "unit",
            "decision_available_at": "2026-01-15T00:00:00Z",
        },
    )


def append_deployment_rollout_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="deployment_rollout_state_recorded",
        aggregate_id="deployment_rollout:rollout-unit-v1",
        idempotency_key="deployment_rollout:rollout-unit-v1:completed:passed",
        payload={
            "rollout_id": "rollout-unit-v1",
            "state": "completed",
            "fleet_hash_map": {
                "dashboard": "build-a",
                "v27-read-model-refresh": "build-a",
            },
            "canary_status": "passed",
            "build_hash": "build-a",
            "runtime_config_hash": "config-a",
            "policy_bundle_id": "policy-a",
            "evidence_source": "unit",
        },
    )


def append_worker_fleet_heartbeat_event(event_log_dir, worker_id="dashboard", build_hash="build-a"):
    V27EventLog(event_log_dir).append_event(
        event_type="worker_fleet_heartbeat_recorded",
        aggregate_id=f"worker_fleet:{worker_id}",
        idempotency_key=f"worker_fleet:{worker_id}:{build_hash}:config-a:policy-a",
        payload={
            "worker_id": worker_id,
            "role": worker_id,
            "build_hash": build_hash,
            "runtime_config_hash": "config-a",
            "policy_bundle_id": "policy-a",
            "heartbeat_at": "2026-01-15T00:00:00Z",
            "evidence_source": "unit",
        },
    )


def append_backup_restore_drill_event(event_log_dir):
    drill_id = "restore-drill-unit-v1"
    V27EventLog(event_log_dir).append_event(
        event_type="backup_restore_drill_recorded",
        aggregate_id=f"backup_restore_drill:{drill_id}",
        idempotency_key=f"backup_restore_drill:{drill_id}:backup-set-unit-v1",
        payload={
            "drill_id": drill_id,
            "backup_set_id": "backup-set-unit-v1",
            "restored_world_hash": sha256_hex({"world": "restored", "drill_id": drill_id}),
            "restore_started_at": "2026-01-15T00:00:00Z",
            "restore_completed_at": "2026-01-15T00:01:00Z",
            "restore_status": "passed",
            "evidence_source": "unit",
        },
    )


def append_incident_evidence_freeze_event(event_log_dir):
    freeze_id = "freeze-unit-v1"
    V27EventLog(event_log_dir).append_event(
        event_type="incident_evidence_freeze_recorded",
        aggregate_id=f"incident_evidence_freeze:{freeze_id}",
        idempotency_key=f"incident_evidence_freeze:{freeze_id}:incident-unit-v1",
        payload={
            "freeze_id": freeze_id,
            "incident_id": "incident-unit-v1",
            "frozen_event_range": {"start_seq": 1, "end_seq": 42},
            "frozen_config_hash": sha256_hex({"config": "frozen", "incident_id": "incident-unit-v1"}),
            "frozen_at": "2026-01-15T00:02:00Z",
            "freeze_status": "frozen",
            "evidence_source": "unit",
        },
    )


def append_circuit_breaker_resume_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="circuit_breaker_resume_recorded",
        aggregate_id="circuit_breaker_resume:breaker-unit-v1",
        idempotency_key="circuit_breaker_resume:breaker-unit-v1:freeze-unit-v1",
        payload={
            "breaker_id": "breaker-unit-v1",
            "root_cause_fixed": True,
            "evidence_freeze_id": "freeze-unit-v1",
            "health_checks_passed": True,
            "resumed_at": "2026-01-15T00:03:00Z",
            "resume_status": "resumed",
            "evidence_source": "unit",
        },
    )


def append_queue_durability_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="queue_durability_recorded",
        aggregate_id="queue_durability:entry-queue:task-unit-v1",
        idempotency_key="queue_durability:entry-queue:task-unit-v1:persisted:acked",
        payload={
            "queue_id": "entry-queue",
            "task_id": "task-unit-v1",
            "durable_state": "persisted",
            "ack_state": "acked",
            "created_at": "2026-01-15T00:04:00Z",
            "evidence_source": "unit",
        },
    )


def append_candidate_cancellation_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="candidate_cancellation_recorded",
        aggregate_id="candidate_cancellation:candidate-unit-v1",
        idempotency_key="candidate_cancellation:candidate-unit-v1:42",
        payload={
            "candidate_id": "candidate-unit-v1",
            "cancel_reason": "risk_revalidated",
            "cancel_event_seq": 42,
            "cancelled_at": "2026-01-15T00:05:00Z",
            "evidence_source": "unit",
        },
    )


def append_retry_storm_control_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="retry_storm_control_recorded",
        aggregate_id="retry_storm_control:provider_quote",
        idempotency_key="retry_storm_control:provider_quote:capped_exponential_jitter",
        payload={
            "retry_family": "provider_quote",
            "backoff_policy": "capped_exponential_jitter",
            "max_concurrent_retries": 2,
            "p0_reserved_capacity": 1,
            "evidence_source": "unit",
        },
    )


def append_provider_coverage_map_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="provider_coverage_map_recorded",
        aggregate_id="provider_coverage_map:jupiter_ultra:solana:raydium_amm",
        idempotency_key="provider_coverage_map:jupiter_ultra:solana:raydium_amm:supported",
        payload={
            "provider": "jupiter_ultra",
            "chain": "solana",
            "pool_type": "raydium_amm",
            "coverage_status": "supported",
            "unsupported_reason": "none",
            "coverage_map_version": "v2.7.0.provider_coverage_map.v1",
            "checked_at": "2026-01-15T00:06:00Z",
            "evidence_source": "unit",
        },
    )


def append_training_serving_skew_event(event_log_dir):
    feature_set_id = "normal-tiny-features-v1"
    normalization_version = "norm-v1"
    V27EventLog(event_log_dir).append_event(
        event_type="training_serving_skew_recorded",
        aggregate_id=f"training_serving_skew:{feature_set_id}:{normalization_version}",
        idempotency_key=f"training_serving_skew:{feature_set_id}:{normalization_version}:pass",
        payload={
            "feature_set_id": feature_set_id,
            "training_feature_code_hash": sha256_hex({"feature_code": "training", "version": "v1"}),
            "serving_feature_code_hash": sha256_hex({"feature_code": "serving", "version": "v1"}),
            "normalization_version": normalization_version,
            "skew_check_result": "pass",
            "checked_at": "2026-01-15T00:07:00Z",
            "training_artifact_id": "training-artifact-unit-v1",
            "serving_artifact_id": "serving-artifact-unit-v1",
            "evidence_source": "unit",
        },
    )


def append_idempotency_contract_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="idempotency_contract_recorded",
        aggregate_id="idempotency_contract:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="idempotency_contract:unit:1:legacy_paper_entry_idempotency_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "idempotency_contract_version": "legacy_paper_entry_idempotency_v0.1",
            "decision_id": "paper_trade:1:entry_decision",
            "execution_id": "paper_trade:1:entry_execution",
            "idempotency_key": "unit:paper_entry_execution:intent-1",
            "token_lifecycle_key": "solana:TokenReady:unknown_pool:0:1",
            "action": "paper_entry",
            "namespace": "paper_entry_execution",
            "environment_id": "unit",
            "route": "unit_route",
            "hash_algorithm": "sha256(canonical_json)",
            "collision_policy": "reject_same_namespace_key_with_different_intent_hash",
            "idempotency_intent_hash": "intent-1",
            "key_material_hash": "intent-1",
            "namespace_isolation_prefix": "unit:paper_entry_execution:",
            "cross_environment_isolated": True,
            "idempotency_proof_level": "legacy_paper_trade_entry_execution",
        },
    )


def append_execution_control_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="execution_control_recorded",
        aggregate_id="execution_control:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="execution_control:unit:1:legacy_paper_entry_execution_control_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "execution_control_version": "legacy_paper_entry_execution_control_v0.1",
            "decision_id": "paper_trade:1:entry_decision",
            "execution_id": "paper_trade:1:entry_execution",
            "token_lifecycle_key": "solana:TokenReady:unknown_pool:0:1",
            "environment_id": "unit",
            "route": "unit_route",
            "lease_id": "lease:unit:ready",
            "fencing_token": "fence-ready",
            "acquired_at": "2026-01-15T00:00:00Z",
            "expires_at": "2026-01-15T00:00:20Z",
            "released_at": "2026-01-15T00:00:01Z",
            "lease_status": "released",
            "lease_valid_at_execution": True,
            "state_version_at_decision": 1,
            "state_version_at_execution": 2,
            "requires_revalidation_before_fill": True,
            "revalidation_passed": True,
            "state": "filled_paper",
            "state_version": 2,
            "failure_reason": "none",
            "terminal_state": True,
            "execution_control_proof_level": "unit_execution_control",
            "state_version_source": "unit",
        },
    )


def append_paper_ledger_event(event_log_dir):
    position_material = {
        "position_id": "paper_trade:1:position",
        "decision_id": "paper_trade:1:entry_decision",
        "execution_id": "paper_trade:1:entry_execution",
        "entry_size_sol": "0.010000000000",
        "remaining_size": "0.000000000000",
        "position_status": "closed",
        "row_state_hash": "row-state-ready",
    }
    capital_material = {
        "capital_ledger_id": "capital_ledger:unit:paper:1",
        "capital_basis_sol": "1.000000000000",
        "available_capital": "1.001000000000",
        "reserved_capital": "0.000000000000",
        "open_exposure": "0.000000000000",
        "realized_pnl_sol": "0.001000000000",
        "fees_sol": "0.000000000000",
    }
    ledger_material = {
        **capital_material,
        "ledger_checkpoint_id": "ledger_checkpoint:unit:paper:1",
        "invariant_formula": "available_capital + reserved_capital + open_exposure - realized_pnl_sol + fees_sol == capital_basis_sol",
        "invariant_lhs": "1.000000000000",
        "invariant_rhs": "1.000000000000",
    }
    V27EventLog(event_log_dir).append_event(
        event_type="paper_ledger_recorded",
        aggregate_id="paper_ledger:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="paper_ledger:unit:1:legacy_paper_position_capital_ledger_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "paper_ledger_version": "legacy_paper_position_capital_ledger_v0.1",
            "decision_id": "paper_trade:1:entry_decision",
            "execution_id": "paper_trade:1:entry_execution",
            "token_lifecycle_key": "solana:TokenReady:unknown_pool:0:1",
            "environment_id": "unit",
            "route": "unit_route",
            "position_id": "paper_trade:1:position",
            "position_status": "closed",
            "entry_size_sol": "0.010000000000",
            "remaining_size": "0.000000000000",
            "position_realized_pnl_sol": "0.001000000000",
            "size_source": "unit",
            "position_ledger_material": position_material,
            "position_ledger_hash": sha256_hex(position_material),
            "capital_ledger_material": capital_material,
            "capital_ledger_hash": sha256_hex(capital_material),
            **capital_material,
            "ledger_checkpoint_id": ledger_material["ledger_checkpoint_id"],
            "ledger_hash_material": ledger_material,
            "ledger_hash": sha256_hex(ledger_material),
            "invariant_formula": ledger_material["invariant_formula"],
            "invariant_lhs": "1.000000000000",
            "invariant_rhs": "1.000000000000",
            "invariant_delta": "0.000000000000",
            "invariant_ok": True,
            "reservation_id": "reservation:unit:1",
            "reservation_status": "released",
            "reservation_ttl_sec": "20.000000000000",
            "release_reason": "position_closed",
            "reserved_capital_at_entry": "0.010000000000",
            "ledger_scope": "paper_global_capital_reconstruction",
            "ledger_proof_level": "unit_paper_ledger",
        },
    )


def append_no_fill_outcome_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="no_fill_outcome_recorded",
        aggregate_id="no_fill_outcome:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="no_fill_outcome:unit:1:legacy_paper_recovery_control_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "recovery_control_version": "legacy_paper_recovery_control_v0.1",
            "no_fill_outcome_version": "legacy_paper_recovery_control_v0.1",
            "attempt_id": "paper_trade:1:attempt",
            "decision_id": "paper_trade:1:entry_decision",
            "execution_id": "paper_trade:1:entry_execution",
            "token_lifecycle_key": "solana:TokenReady:unknown_pool:0:1",
            "environment_id": "unit",
            "route": "unit_route",
            "outcome_state": "filled_paper",
            "terminal_state": True,
            "no_fill_record_required": False,
            "no_fill_reason": "none_filled_paper",
            "missed_net_peak30": 0.0,
            "missed_net_peak30_source": "not_applicable_filled_paper",
            "no_fill_cost": 0.0,
            "no_fill_saved_loss": 0.0,
            "no_fill_cost_model": "unit_no_fill_cost_model",
            "outcome_source": "unit",
            "outcome_available_at": "2026-01-15T00:00:02Z",
        },
    )


def append_runtime_recovery_control_event(event_log_dir):
    V27EventLog(event_log_dir).append_event(
        event_type="runtime_recovery_control_recorded",
        aggregate_id="runtime_recovery:unit",
        idempotency_key="runtime_recovery_control:unit:legacy_paper_recovery_control_v0.1",
        payload={
            "recovery_control_version": "legacy_paper_recovery_control_v0.1",
            "recovery_id": "recovery:unit:ready",
            "state": "clean_start",
            "environment_id": "unit",
            "orphan_scan_result": {
                "status": "ok",
                "event_log_ok": True,
                "orphaned_execution_count": 0,
                "non_terminal_execution_count": 0,
            },
            "reconcile_result": {
                "status": "ok",
                "event_log_ok": True,
                "malformed_no_fill_count": 0,
            },
            "drain_id": "drain:unit:ready",
            "queued_candidates_revalidated": 1,
            "expired_candidates_emitted": 0,
            "resume_drain_completed_at": "2026-01-15T00:00:03Z",
            "drain_status": "completed",
            "new_entries_blocked_until_drain": True,
            "resume_allowed": True,
        },
    )


def test_mode_readiness_reports_passed_evidence_and_blocks_unproven_modes(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["matrix_schema_version"] == "v2.7.0.mode_readiness.v1"
    assert matrix["event_log"]["verify"]["event_count"] == 2
    assert matrix["contract_statuses"]["CanonicalSpecIntegrityContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["EventSemanticsContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["EventSequencerContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DecisionReadModelFreshnessContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DenominatorDedupContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SourceDogLabelContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SpecConsistencyLinterContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["PaperModeSafetyBoundary"]["status"] == "pass"
    assert matrix["contract_statuses"]["ChainConfigContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SourceRegistryContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["WritePathRegistryContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["InputSanitizationContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SafeDefaultContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ProjectStopLossContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["EvidenceEligibilityMatrix"]["status"] == "pass"
    assert matrix["contract_statuses"]["TopFixQueueContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SafetyCaseContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["WaiverPolicyContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["TransactionalOutboxContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ReplaySideEffectIsolationContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ReplaySideEffectIsolationContract"]["evidence"]["provider_call_count"] == 0
    assert matrix["contract_statuses"]["ReplaySideEffectIsolationContract"]["evidence"]["unexpected_write_target_count"] == 0
    assert matrix["contract_statuses"]["DeadLetterQueueContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ConsumerCheckpointContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ProjectionHandlerIdempotencyContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["CacheInvalidationContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SignalCreditAssignmentContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ReferencePriceContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["MetricsWindowContract"]["status"] == "pass"
    assert matrix["modes"]["observe_only"]["status"] == "allowed"
    assert matrix["modes"]["shadow"]["status"] == "allowed"
    assert matrix["highest_allowed_mode"] == "shadow"
    assert matrix["modes"]["shadow"]["blocking_contracts"] == []
    assert matrix["modes"]["ultra_tiny"]["status"] == "blocked"
    assert "EntryExecutionStateMachine" in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert matrix["modes"]["normal_tiny"]["status"] == "blocked"
    assert "WorkerFleetConsistencyContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "SafeDefaultContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "ProjectStopLossContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "EvidenceEligibilityMatrix" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "TopFixQueueContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "SafetyCaseContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "WaiverPolicyContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert matrix["health"]["observe_only_ready"] is True
    assert matrix["health"]["shadow_ready"] is True
    assert matrix["health"]["normal_tiny_ready"] is False


def test_mode_readiness_component_health_follows_final_normal_tiny_gate(monkeypatch, tmp_path):
    required_contracts = set()
    for mode in mode_readiness_module.MODE_ORDER:
        required_contracts.update(mode_readiness_module._expanded_requirements(mode))
    passing_contracts = {
        contract_id: {
            "contract_id": contract_id,
            "status": "pass",
            "blocking_reason": None,
            "evidence": {"source": "unit"},
        }
        for contract_id in required_contracts
    }

    monkeypatch.setattr(
        mode_readiness_module,
        "_load_json",
        lambda path: {"contracts": {contract_id: {} for contract_id in required_contracts}},
    )
    monkeypatch.setattr(mode_readiness_module, "validate_all", lambda *args, **kwargs: {"spec_valid": True})
    monkeypatch.setattr(
        mode_readiness_module,
        "build_basic_contract_readiness",
        lambda **kwargs: {
            "contracts": {},
            "blocking_contracts": [],
            "health": {
                "status": "basic_contract_readiness_ok",
                "observe_only_foundation_ready": True,
                "normal_tiny_ready": False,
            },
        },
    )
    monkeypatch.setattr(
        mode_readiness_module,
        "validate_snapshot_file",
        lambda *args, **kwargs: {
            "blocking_reasons": [],
            "health": {
                "status": "dashboard_read_model_fresh",
                "dashboard_safe": True,
                "normal_tiny_ready": False,
            },
        },
    )
    monkeypatch.setattr(mode_readiness_module, "_snapshot_payload", lambda path: {})
    monkeypatch.setattr(mode_readiness_module, "_event_log_report", lambda event_log_dir: {"ok": True})
    monkeypatch.setattr(
        mode_readiness_module,
        "read_projection_consumer_health",
        lambda path: {
            "contracts": {},
            "blocking_contracts": [],
            "health": {
                "status": "projection_consumer_ok",
                "shadow_consumer_ready": True,
                "normal_tiny_ready": False,
            },
        },
    )
    monkeypatch.setattr(mode_readiness_module, "build_contract_statuses", lambda **kwargs: dict(passing_contracts))

    matrix = mode_readiness_module.build_mode_readiness_matrix(
        event_log_dir=tmp_path / "events",
        snapshot_path=tmp_path / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["highest_allowed_mode"] == "normal_tiny"
    assert matrix["health"]["normal_tiny_ready"] is True
    assert matrix["read_model"]["health"]["normal_tiny_ready"] is True
    assert matrix["basic_readiness"]["health"]["normal_tiny_ready"] is True
    assert matrix["projection_consumer"]["health"]["normal_tiny_ready"] is True
    assert matrix["read_model"]["health"]["normal_tiny_ready_source"] == "mode_readiness_matrix"
    assert matrix["read_model"]["health"]["read_model_fresh"] is True
    assert matrix["basic_readiness"]["health"]["basic_contracts_ready"] is True
    assert matrix["projection_consumer"]["health"]["projection_consumer_ready"] is True


def test_mode_readiness_consumes_realtime_clean_detector_evidence(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_realtime_clean_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["contract_statuses"]["RealtimeCleanDetector"]["status"] == "pass"
    assert matrix["contract_statuses"]["RealtimeCleanDetector"]["evidence"]["realtime_clean_observed_count"] == 1
    assert "RealtimeCleanDetector" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_quote_intent_binding_evidence(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_realtime_clean_event(event_log_dir)
    append_quote_intent_binding_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["contract_statuses"]["RealtimeCleanDetector"]["status"] == "pass"
    assert matrix["contract_statuses"]["QuoteIntentBindingContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["QuoteIntentBindingContract"]["evidence"]["quote_intent_bound_count"] == 1
    assert "RealtimeCleanDetector" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "QuoteIntentBindingContract" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "IdempotencyContract" in matrix["modes"]["ultra_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_raw_provider_evidence_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_realtime_clean_event(event_log_dir)
    append_quote_intent_binding_event(event_log_dir)
    append_raw_provider_evidence_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["contract_statuses"]["RawProviderEvidenceContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["RawProviderEvidenceContract"]["evidence"]["trusted_raw_provider_evidence_count"] == 1
    assert "RawProviderEvidenceContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "LabelFinalizationContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "OutcomeWindowCloseContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_randomness_control_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_randomness_control_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    evidence = matrix["contract_statuses"]["RandomnessControlContract"]["evidence"]
    assert matrix["contract_statuses"]["RandomnessControlContract"]["status"] == "pass"
    assert evidence["valid_randomness_control_count"] == 1
    assert "RandomnessControlContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "RawProviderEvidenceContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_blocks_randomness_control_when_missing(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["contract_statuses"]["RandomnessControlContract"]["status"] == "missing_evidence"
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["required_contracts"]
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_deployment_and_worker_fleet_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_deployment_rollout_event(event_log_dir)
    append_worker_fleet_heartbeat_event(event_log_dir, worker_id="dashboard")
    append_worker_fleet_heartbeat_event(event_log_dir, worker_id="v27-read-model-refresh")
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    deployment = matrix["contract_statuses"]["DeploymentRolloutStateMachine"]
    fleet = matrix["contract_statuses"]["WorkerFleetConsistencyContract"]
    assert deployment["status"] == "pass"
    assert fleet["status"] == "pass"
    assert deployment["evidence"]["valid_deployment_rollout_count"] == 1
    assert fleet["evidence"]["valid_worker_fleet_count"] == 2
    assert "DeploymentRolloutStateMachine" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "WorkerFleetConsistencyContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_backup_restore_drill_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_backup_restore_drill_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    backup_restore = matrix["contract_statuses"]["BackupRestoreDrillContract"]
    assert backup_restore["status"] == "pass"
    assert backup_restore["evidence"]["valid_backup_restore_drill_count"] == 1
    assert "BackupRestoreDrillContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_incident_freeze_and_breaker_resume_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_incident_evidence_freeze_event(event_log_dir)
    append_circuit_breaker_resume_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    freeze = matrix["contract_statuses"]["IncidentEvidenceFreezeContract"]
    resume = matrix["contract_statuses"]["CircuitBreakerResumeContract"]
    assert freeze["status"] == "pass"
    assert resume["status"] == "pass"
    assert freeze["evidence"]["valid_incident_evidence_freeze_count"] == 1
    assert resume["evidence"]["valid_circuit_breaker_resume_count"] == 1
    assert "IncidentEvidenceFreezeContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "CircuitBreakerResumeContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_queue_candidate_and_retry_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_queue_durability_event(event_log_dir)
    append_candidate_cancellation_event(event_log_dir)
    append_retry_storm_control_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    queue = matrix["contract_statuses"]["QueueDurabilityContract"]
    cancellation = matrix["contract_statuses"]["CandidateCancellationContract"]
    retry = matrix["contract_statuses"]["RetryStormControlContract"]
    assert queue["status"] == "pass"
    assert cancellation["status"] == "pass"
    assert retry["status"] == "pass"
    assert queue["evidence"]["valid_queue_durability_count"] == 1
    assert cancellation["evidence"]["valid_candidate_cancellation_count"] == 1
    assert retry["evidence"]["valid_retry_storm_control_count"] == 1
    assert "QueueDurabilityContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "CandidateCancellationContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "RetryStormControlContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_provider_coverage_and_training_serving_skew_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_provider_coverage_map_event(event_log_dir)
    append_training_serving_skew_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    coverage = matrix["contract_statuses"]["ProviderCoverageMapContract"]
    skew = matrix["contract_statuses"]["TrainingServingSkewContract"]
    assert coverage["status"] == "pass"
    assert skew["status"] == "pass"
    assert coverage["evidence"]["valid_provider_coverage_map_count"] == 1
    assert skew["evidence"]["valid_training_serving_skew_count"] == 1
    assert "ProviderCoverageMapContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "TrainingServingSkewContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_idempotency_contract_evidence(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_realtime_clean_event(event_log_dir)
    append_quote_intent_binding_event(event_log_dir)
    append_idempotency_contract_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["contract_statuses"]["IdempotencyContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["IdempotencyKeyNamespaceContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["IdempotencyContract"]["evidence"]["idempotency_collision_count"] == 0
    assert "IdempotencyContract" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "IdempotencyKeyNamespaceContract" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "ExecutionLeaseContract" in matrix["modes"]["ultra_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_execution_control_evidence(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_realtime_clean_event(event_log_dir)
    append_quote_intent_binding_event(event_log_dir)
    append_idempotency_contract_event(event_log_dir)
    append_execution_control_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["contract_statuses"]["ExecutionLeaseContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["StateVersionFencing"]["status"] == "pass"
    assert matrix["contract_statuses"]["EntryExecutionStateMachine"]["status"] == "pass"
    assert matrix["contract_statuses"]["ExecutionLeaseContract"]["evidence"]["lease_violation_count"] == 0
    assert "ExecutionLeaseContract" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "StateVersionFencing" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "EntryExecutionStateMachine" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "PaperPositionLedgerContract" in matrix["modes"]["ultra_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_paper_ledger_evidence(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_realtime_clean_event(event_log_dir)
    append_quote_intent_binding_event(event_log_dir)
    append_idempotency_contract_event(event_log_dir)
    append_execution_control_event(event_log_dir)
    append_paper_ledger_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    for contract_id in (
        "PaperPositionLedgerContract",
        "PaperCapitalLedgerContract",
        "DoubleEntryLedgerInvariantContract",
        "CapitalReservationPolicy",
    ):
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "NoFillOutcome" in matrix["modes"]["ultra_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_no_fill_and_recovery_controls(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_realtime_clean_event(event_log_dir)
    append_quote_intent_binding_event(event_log_dir)
    append_idempotency_contract_event(event_log_dir)
    append_execution_control_event(event_log_dir)
    append_paper_ledger_event(event_log_dir)
    append_no_fill_outcome_event(event_log_dir)
    append_runtime_recovery_control_event(event_log_dir)
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    for contract_id in (
        "NoFillOutcome",
        "CrashRecoveryStateMachine",
        "ResumeDrainPolicy",
    ):
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_trade_outcome_label_evidence(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    V27EventLog(event_log_dir).append_event(
        event_type="trade_outcome_label_recorded",
        aggregate_id="trade_outcome:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="paper_trade_outcome_label:1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "trade_outcome_label_version": "legacy_paper_trade_outcome_v0.1",
            "counterfactual_entry_ts": 1_700_000_003,
            "fill_time_anchor": "simulated_fill_ts",
            "simulated_fill_ts": 1_700_000_003,
            "simulated_fill_price": 0.001,
            "net_delayed_executable_peak_3s": 0.75,
            "realized_pnl": 0.3,
            "trade_label_available_at": 1_700_000_120,
        },
    )
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["contract_statuses"]["TradeOutcomeLabelContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["LabelFinalizationContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["OutcomeWindowCloseContract"]["status"] == "pass"
    assert "TradeOutcomeLabelContract" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "LabelFinalizationContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "OutcomeWindowCloseContract" not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert "StandardizedStopContract" in matrix["modes"]["ultra_tiny"]["blocking_contracts"]


def test_mode_readiness_fails_outcome_window_close_order_violation(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    V27EventLog(event_log_dir).append_event(
        event_type="trade_outcome_label_recorded",
        aggregate_id="trade_outcome:solana:TokenRollback:unknown_pool:0:1",
        idempotency_key="paper_trade_outcome_label:rollback",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenRollback",
            "symbol": "ROLL",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "trade_outcome_label_version": "legacy_paper_trade_outcome_v0.1",
            "counterfactual_entry_ts": 1_700_000_120,
            "fill_time_anchor": "simulated_fill_ts",
            "simulated_fill_ts": 1_700_000_120,
            "simulated_fill_price": 0.001,
            "net_delayed_executable_peak_3s": 0.75,
            "realized_pnl": 0.3,
            "trade_label_available_at": 1_700_000_003,
        },
    )
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    outcome = matrix["contract_statuses"]["OutcomeWindowCloseContract"]
    assert matrix["contract_statuses"]["LabelFinalizationContract"]["status"] == "pass"
    assert outcome["status"] == "fail"
    assert outcome["blocking_reason"] == "outcome_window_close_malformed_or_order_violation"
    assert outcome["evidence"]["malformed_count"] == 0
    assert outcome["evidence"]["window_order_violation_count"] == 1
    assert outcome["evidence"]["window_order_violations"][0]["windows"] == [
        {
            "window_start": 1_700_000_120,
            "window_end": 1_700_000_003,
            "window_closed_at": 1_700_000_003,
        }
    ]
    assert "OutcomeWindowCloseContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_standardized_stop_evidence(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    V27EventLog(event_log_dir).append_event(
        event_type="trade_outcome_label_recorded",
        aggregate_id="trade_outcome:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="paper_trade_outcome_label:1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "trade_outcome_label_version": "legacy_paper_trade_outcome_v0.1",
            "counterfactual_entry_ts": 1_700_000_003,
            "simulated_fill_price": 0.001,
            "trade_label_available_at": 1_700_000_120,
        },
    )
    V27EventLog(event_log_dir).append_event(
        event_type="standardized_stop_contract_recorded",
        aggregate_id="standardized_stop:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="standardized_stop_contract:1:legacy_standardized_stop_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "stop_contract_version": "legacy_standardized_stop_v0.1",
            "stop_type": "standardized_counterfactual_stop",
            "stop_threshold_pct": -30.0,
            "stop_window": "60m",
            "stop_price_type": "delayed_executable_exit_quote_proxy",
            "stop_executable_required": True,
            "stop_friction_model_version": "legacy_round_trip_friction_v0.1",
            "stop_available_at": 1_700_000_003,
        },
    )
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["contract_statuses"]["StandardizedStopContract"]["status"] == "pass"
    assert "TradeOutcomeLabelContract" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "StandardizedStopContract" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "ExAnteFeasibility" in matrix["modes"]["ultra_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_ex_ante_feasibility_evidence(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    log = V27EventLog(event_log_dir)
    log.append_event(
        event_type="trade_outcome_label_recorded",
        aggregate_id="trade_outcome:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="paper_trade_outcome_label:1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "trade_outcome_label_version": "legacy_paper_trade_outcome_v0.1",
            "counterfactual_entry_ts": 1_700_000_003,
            "simulated_fill_price": 0.001,
            "trade_label_available_at": 1_700_000_120,
        },
    )
    log.append_event(
        event_type="standardized_stop_contract_recorded",
        aggregate_id="standardized_stop:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="standardized_stop_contract:1:legacy_standardized_stop_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "stop_contract_version": "legacy_standardized_stop_v0.1",
            "stop_type": "standardized_counterfactual_stop",
            "stop_threshold_pct": -30.0,
            "stop_window": "60m",
            "stop_price_type": "delayed_executable_exit_quote_proxy",
            "stop_executable_required": True,
            "stop_friction_model_version": "legacy_round_trip_friction_v0.1",
            "stop_available_at": 1_700_000_003,
        },
    )
    log.append_event(
        event_type="ex_ante_feasibility_recorded",
        aggregate_id="ex_ante_feasibility:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="ex_ante_feasibility:1:legacy_actual_paper_entry_feasibility_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "decision_ts": 1_700_000_002,
            "decision_available_at": 1_700_000_002,
            "counterfactual_entry_ts": 1_700_000_003,
            "feasibility_policy_version": "legacy_actual_paper_entry_feasibility_v0.1",
            "ex_ante_feasible": True,
            "feasibility_class": "legacy_actual_paper_entry",
            "entry_quote_available": True,
            "entry_quote_available_at": 1_700_000_002,
            "current_quote_availability": True,
            "current_pool_resolution": "unknown_pool",
            "used_future_peak": False,
            "used_future_outcome": False,
            "used_posthoc_label": False,
            "forbidden_future_fields_used": [],
        },
    )
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["contract_statuses"]["ExAnteFeasibility"]["status"] == "pass"
    assert "TradeOutcomeLabelContract" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "StandardizedStopContract" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "ExAnteFeasibility" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "EarliestActionableTime" in matrix["modes"]["ultra_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_earliest_actionable_time_evidence(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    log = V27EventLog(event_log_dir)
    log.append_event(
        event_type="trade_outcome_label_recorded",
        aggregate_id="trade_outcome:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="paper_trade_outcome_label:1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "trade_outcome_label_version": "legacy_paper_trade_outcome_v0.1",
            "counterfactual_entry_ts": 1_700_000_003,
            "simulated_fill_price": 0.001,
            "trade_label_available_at": 1_700_000_120,
        },
    )
    log.append_event(
        event_type="standardized_stop_contract_recorded",
        aggregate_id="standardized_stop:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="standardized_stop_contract:1:legacy_standardized_stop_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "stop_contract_version": "legacy_standardized_stop_v0.1",
            "stop_type": "standardized_counterfactual_stop",
            "stop_threshold_pct": -30.0,
            "stop_window": "60m",
            "stop_price_type": "delayed_executable_exit_quote_proxy",
            "stop_executable_required": True,
            "stop_friction_model_version": "legacy_round_trip_friction_v0.1",
            "stop_available_at": 1_700_000_003,
        },
    )
    log.append_event(
        event_type="ex_ante_feasibility_recorded",
        aggregate_id="ex_ante_feasibility:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="ex_ante_feasibility:1:legacy_actual_paper_entry_feasibility_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "decision_ts": 1_700_000_002,
            "decision_available_at": 1_700_000_002,
            "counterfactual_entry_ts": 1_700_000_003,
            "feasibility_policy_version": "legacy_actual_paper_entry_feasibility_v0.1",
            "ex_ante_feasible": True,
            "feasibility_class": "legacy_actual_paper_entry",
            "entry_quote_available": True,
            "entry_quote_available_at": 1_700_000_002,
            "current_quote_availability": True,
            "current_pool_resolution": "unknown_pool",
            "used_future_peak": False,
            "used_future_outcome": False,
            "used_posthoc_label": False,
            "forbidden_future_fields_used": [],
        },
    )
    log.append_event(
        event_type="earliest_actionable_time_recorded",
        aggregate_id="earliest_actionable_time:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="earliest_actionable_time:1:legacy_actual_paper_entry_actionable_time_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "earliest_actionable_policy_version": "legacy_actual_paper_entry_actionable_time_v0.1",
            "earliest_actionable_ts": 1_700_000_003,
            "required_inputs_available_at": {
                "telegram_anchor_available_at": 1_700_000_000,
                "pool_resolved_available_at": 1_700_000_002,
                "entry_quote_executable_available_at": 1_700_000_002,
                "exit_quote_executable_available_at": 1_700_000_002,
                "critical_risk_not_bad_available_at": 1_700_000_002,
                "liquidity_ok_available_at": 1_700_000_002,
                "decision_engine_available_at": 1_700_000_002,
            },
            "missing_inputs_before_ts": [],
            "peak_ts": 1_700_000_120,
            "peak_ts_quality": "legacy_outcome_window_close_proxy",
            "peak_ts_source": "paper_trade_exit_ts",
            "counterfactual_entry_ts": 1_700_000_003,
            "actionable_before_peak": True,
            "earliest_actionable_reason": "legacy_actual_paper_entry_inputs_available_by_decision",
            "actionability_quality": "legacy_actual_paper_entry_window_proof",
        },
    )
    refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    matrix = build_mode_readiness_matrix(
        event_log_dir=event_log_dir,
        snapshot_path=out_dir / "denominator_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["contract_statuses"]["EarliestActionableTime"]["status"] == "pass"
    assert "ExAnteFeasibility" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "EarliestActionableTime" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "RealtimeCleanDetector" in matrix["modes"]["ultra_tiny"]["blocking_contracts"]


def test_mode_readiness_blocks_when_snapshot_missing(tmp_path):
    matrix = build_mode_readiness_matrix(
        event_log_dir=tmp_path / "events",
        snapshot_path=tmp_path / "missing_snapshot.json",
        max_snapshot_age_ms=300_000,
    )

    assert matrix["contract_statuses"]["DecisionReadModelFreshnessContract"]["status"] == "fail"
    assert "snapshot_missing" in matrix["read_model"]["blocking_reasons"]
    assert matrix["health"]["dashboard_safe"] is False
    assert matrix["modes"]["normal_tiny"]["status"] == "blocked"
