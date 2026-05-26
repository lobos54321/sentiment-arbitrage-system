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


def append_decision_audit_event(event_log_dir):
    feature_vector = {
        "token_ca": "TokenReady",
        "route": "unit_route",
        "quote_intent_id": 1,
        "entry_quote_available_at": "2026-01-15T00:00:02Z",
    }
    trace_bundle = {
        "policy_version": "v2.7.0.unit_ultra_decision_policy.v1",
        "source_event_ids": ["unit:telegram_signal_seen", "unit:quote_intent_binding"],
        "feature_max_available_at": "2026-01-15T00:00:02Z",
        "decision_available_at": "2026-01-15T00:00:03Z",
        "used_future_peak": False,
        "used_future_outcome": False,
        "used_posthoc_label": False,
        "forbidden_future_fields_used": [],
    }
    V27EventLog(event_log_dir).append_event(
        event_type="decision_audit_recorded",
        aggregate_id="decision_audit:paper_trade:1:entry_decision",
        idempotency_key="decision_audit:paper_trade:1:entry_decision:v2.7.0.unit_ultra_decision_policy.v1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "decision_audit_version": "v2.7.0.decision_audit.v1",
            "decision_id": "paper_trade:1:entry_decision",
            "policy_bundle_id": "policy-bundle:unit-ultra:v1",
            "spec_hash": sha256_hex({"spec": "v2.7.0"}),
            "feature_vector": feature_vector,
            "feature_vector_hash": sha256_hex(feature_vector),
            "decision_trace_bundle": trace_bundle,
            "decision_trace_bundle_hash": sha256_hex(trace_bundle),
            "decision_available_at": "2026-01-15T00:00:03Z",
            "feature_max_available_at": "2026-01-15T00:00:02Z",
            "failure_action": "entry_rejected",
            "used_future_peak": False,
            "used_future_outcome": False,
            "used_posthoc_label": False,
            "forbidden_future_fields_used": [],
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


def append_provider_dependency_resource_trust_events(event_log_dir):
    log = V27EventLog(event_log_dir)
    log.append_event(
        event_type="provider_byzantine_quorum_recorded",
        aggregate_id="provider_byzantine_quorum:provider-quorum-solana-entry",
        idempotency_key="provider_byzantine_quorum:provider-quorum-solana-entry:jupiter_ultra",
        payload={
            "quorum_id": "provider-quorum-solana-entry",
            "provider_set": ["jupiter_ultra", "gmgn_quote"],
            "conflict_policy": "fail_closed_on_conflict",
            "selected_provider": "jupiter_ultra",
            "quorum_size": 2,
            "agreement_metric": "entry_quote_price_within_tolerance",
            "checked_at": "2026-01-15T00:08:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="provider_cache_poisoning_guard_recorded",
        aggregate_id="provider_cache_poisoning_guard:jupiter_ultra:quote:solana:TokenReady:unknown_pool",
        idempotency_key="provider_cache_poisoning_guard:jupiter_ultra:quote:solana:TokenReady:unknown_pool:none",
        payload={
            "cache_key": "quote:solana:TokenReady:unknown_pool",
            "provider": "jupiter_ultra",
            "poison_detected": False,
            "quarantine_action": "none",
            "cache_validation_hash": sha256_hex({"cache_key": "quote:solana:TokenReady:unknown_pool"}),
            "checked_at": "2026-01-15T00:09:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="external_dependency_health_recorded",
        aggregate_id="external_dependency:jupiter_ultra_quote",
        idempotency_key="external_dependency:jupiter_ultra_quote:healthy",
        payload={
            "dependency_name": "jupiter_ultra_quote",
            "health_status": "healthy",
            "fallback_mode": "fail_closed",
            "fail_closed_action": "block_entry",
            "checked_at": "2026-01-15T00:10:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="third_party_status_correlation_recorded",
        aggregate_id="third_party_status_correlation:jupiter_ultra_quote:jupiter_status_page:none",
        idempotency_key="third_party_status_correlation:jupiter_ultra_quote:jupiter_status_page:none:no_incident",
        payload={
            "dependency_name": "jupiter_ultra_quote",
            "status_source": "jupiter_status_page",
            "incident_id": "none",
            "correlation_result": "no_incident",
            "checked_at": "2026-01-15T00:11:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="resource_exhaustion_recorded",
        aggregate_id="resource_exhaustion:provider_quote_pool",
        idempotency_key="resource_exhaustion:provider_quote_pool:normal:observe",
        payload={
            "resource_type": "provider_quote_pool",
            "pressure_level": "normal",
            "pressure_action": "observe",
            "safety_budget_remaining": 10,
            "checked_at": "2026-01-15T00:12:00Z",
            "evidence_source": "unit",
        },
    )


def append_config_activation_retry_policy_events(event_log_dir):
    log = V27EventLog(event_log_dir)
    config_hash = sha256_hex({"config_id": "normal-tiny-config-v1", "version": "v1"})
    log.append_event(
        event_type="config_distribution_recorded",
        aggregate_id="config_distribution:normal-tiny-config-v1",
        idempotency_key="config_distribution:normal-tiny-config-v1:all_workers_before_effective_at",
        payload={
            "config_id": "normal-tiny-config-v1",
            "config_hash": config_hash,
            "target_workers": ["dashboard", "v27-read-model-refresh"],
            "effective_at": "2026-01-15T00:13:00Z",
            "ack_policy": "all_workers_before_effective_at",
            "evidence_source": "unit",
        },
    )
    for worker_id in ("dashboard", "v27-read-model-refresh"):
        log.append_event(
            event_type="config_distribution_ack_recorded",
            aggregate_id=f"config_distribution_ack:normal-tiny-config-v1:{worker_id}",
            idempotency_key=f"config_distribution_ack:normal-tiny-config-v1:{worker_id}:acked",
            payload={
                "config_id": "normal-tiny-config-v1",
                "worker_id": worker_id,
                "config_hash": config_hash,
                "ack_state": "acked",
                "acked_at": "2026-01-15T00:13:10Z",
                "evidence_source": "unit",
            },
        )
    log.append_event(
        event_type="in_flight_config_rotation_recorded",
        aggregate_id="in_flight_config_rotation:normal-tiny-rotation-v1",
        idempotency_key="in_flight_config_rotation:normal-tiny-rotation-v1:drain_then_cutover",
        payload={
            "rotation_id": "normal-tiny-rotation-v1",
            "old_config_hash": sha256_hex({"config_id": "normal-tiny-config-v0", "version": "v0"}),
            "new_config_hash": config_hash,
            "affected_workers": ["dashboard", "v27-read-model-refresh"],
            "safe_cutover_at": "2026-01-15T00:14:00Z",
            "rotation_policy": "drain_then_cutover",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="policy_activation_barrier_recorded",
        aggregate_id="policy_activation_barrier:normal-tiny-policy-v1:1",
        idempotency_key="policy_activation_barrier:normal-tiny-policy-v1:1:2",
        payload={
            "policy_bundle_id": "normal-tiny-policy-v1",
            "activation_epoch": 1,
            "required_worker_ack_count": 2,
            "observed_worker_ack_count": 2,
            "activated_at": "2026-01-15T00:15:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="retry_policy_catalog_recorded",
        aggregate_id="retry_policy_catalog:provider_quote",
        idempotency_key="retry_policy_catalog:provider_quote:capped_exponential_jitter:3",
        payload={
            "retry_family": "provider_quote",
            "backoff_policy": "capped_exponential_jitter",
            "max_attempts": 3,
            "jitter_policy": "full_jitter",
            "owner": "runtime",
            "checked_at": "2026-01-15T00:16:00Z",
            "evidence_source": "unit",
        },
    )


def append_alert_model_release_runtime_trust_events(event_log_dir):
    log = V27EventLog(event_log_dir)
    log.append_event(
        event_type="alert_noise_budget_recorded",
        aggregate_id="alert_noise_budget:provider_quote_health:alerts-2026-01-15T00",
        idempotency_key="alert_noise_budget:provider_quote_health:alerts-2026-01-15T00",
        payload={
            "alert_family": "provider_quote_health",
            "window_id": "alerts-2026-01-15T00",
            "noise_budget": 5,
            "suppression_count": 1,
            "owner": "runtime",
            "checked_at": "2026-01-15T00:17:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="alert_suppression_audit_recorded",
        aggregate_id="alert_suppression_audit:suppression-unit-v1",
        idempotency_key="alert_suppression_audit:suppression-unit-v1:audit-event-unit-v1",
        payload={
            "suppression_id": "suppression-unit-v1",
            "alert_family": "provider_quote_health",
            "suppression_reason": "deduplicated_noisy_probe",
            "expires_at": "2026-01-15T01:17:00Z",
            "audit_event_id": "audit-event-unit-v1",
            "checked_at": "2026-01-15T00:17:10Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="canary_abort_recorded",
        aggregate_id="canary_abort:canary-unit-v1",
        idempotency_key="canary_abort:canary-unit-v1:rollback_release",
        payload={
            "canary_id": "canary-unit-v1",
            "abort_threshold": 0.05,
            "observed_metric": 0.08,
            "abort_action": "rollback_release",
            "aborted_at": "2026-01-15T00:18:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="model_artifact_runtime_compatibility_recorded",
        aggregate_id="model_artifact_runtime_compatibility:model-snapshot-unit-v1:runtime-v1",
        idempotency_key="model_artifact_runtime_compatibility:model-snapshot-unit-v1:runtime-v1",
        payload={
            "model_snapshot_id": "model-snapshot-unit-v1",
            "runtime_version": "runtime-v1",
            "serialization_format": "onnx",
            "compatibility_result": "compatible",
            "checked_at": "2026-01-15T00:19:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="model_rollback_recorded",
        aggregate_id="model_rollback:model-rollback-unit-v1",
        idempotency_key="model_rollback:model-rollback-unit-v1:model-snapshot-unit-v1",
        payload={
            "rollback_id": "model-rollback-unit-v1",
            "from_model_snapshot_id": "model-snapshot-unit-v2",
            "to_model_snapshot_id": "model-snapshot-unit-v1",
            "rollback_verified_at": "2026-01-15T00:20:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="post_release_monitoring_window_recorded",
        aggregate_id="post_release_monitoring_window:release-unit-v1",
        idempotency_key="post_release_monitoring_window:release-unit-v1:monitoring_passed",
        payload={
            "release_id": "release-unit-v1",
            "window_start": "2026-01-15T00:00:00Z",
            "window_end": "2026-01-15T01:00:00Z",
            "monitored_metrics": ["error_rate", "capture_rate"],
            "exit_status": "monitoring_passed",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="training_poisoning_guard_recorded",
        aggregate_id="training_poisoning_guard:training-run-unit-v1",
        idempotency_key="training_poisoning_guard:training-run-unit-v1:0:none",
        payload={
            "training_run_id": "training-run-unit-v1",
            "dataset_hash": sha256_hex({"dataset": "unit", "version": "v1"}),
            "poison_signal_count": 0,
            "quarantine_action": "none",
            "checked_at": "2026-01-15T00:21:00Z",
            "evidence_source": "unit",
        },
    )


def append_position_feature_runtime_trust_events(event_log_dir):
    log = V27EventLog(event_log_dir)
    feature_hash = sha256_hex({"feature_set_id": "feature-set-unit-v1", "normalization_version": "norm-v1"})
    log.append_event(
        event_type="feature_store_consistency_recorded",
        aggregate_id="feature_store_consistency:feature-set-unit-v1:norm-v1",
        idempotency_key=f"feature_store_consistency:feature-set-unit-v1:norm-v1:{feature_hash}",
        payload={
            "feature_set_id": "feature-set-unit-v1",
            "offline_hash": feature_hash,
            "online_hash": feature_hash,
            "normalization_version": "norm-v1",
            "checked_at": "2026-01-15T00:22:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="dynamic_token_authority_change_recorded",
        aggregate_id="dynamic_token_authority_change:TokenReady:freeze",
        idempotency_key="dynamic_token_authority_change:TokenReady:freeze:risk_recheck",
        payload={
            "token_ca": "TokenReady",
            "authority_type": "freeze",
            "previous_authority_hash": sha256_hex({"token_ca": "TokenReady", "authority": "previous"}),
            "current_authority_hash": sha256_hex({"token_ca": "TokenReady", "authority": "current"}),
            "risk_action": "risk_recheck",
            "checked_at": "2026-01-15T00:23:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="adversarial_execution_simulation_recorded",
        aggregate_id="adversarial_execution_simulation:simulation-unit-v1",
        idempotency_key="adversarial_execution_simulation:simulation-unit-v1:blocked",
        payload={
            "simulation_id": "simulation-unit-v1",
            "execution_policy_version": "normal-tiny-execution-policy-v1",
            "attack_scenario": "quote_cache_poison_then_retry_storm",
            "safety_result": "blocked",
            "checked_at": "2026-01-15T00:24:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="open_position_valuation_recorded",
        aggregate_id="open_position_valuation:position-unit-v1",
        idempotency_key="open_position_valuation:position-unit-v1:0.001",
        payload={
            "position_id": "position-unit-v1",
            "valuation_ts": "2026-01-15T00:25:00Z",
            "quote_source": "jupiter_ultra",
            "valuation_price": 0.001,
            "valuation_hash": sha256_hex({"position_id": "position-unit-v1", "valuation_price": 0.001}),
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="exit_policy_migration_recorded",
        aggregate_id="exit_policy_migration:position-unit-v1",
        idempotency_key="exit_policy_migration:position-unit-v1:exit-policy-v1:exit-policy-v2",
        payload={
            "position_id": "position-unit-v1",
            "old_exit_policy": "exit-policy-v1",
            "new_exit_policy": "exit-policy-v2",
            "migration_reason": "tighten_dirty_quote_exit_guard",
            "migrated_at": "2026-01-15T00:26:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="open_position_policy_migration_recorded",
        aggregate_id="open_position_policy_migration:position-unit-v2",
        idempotency_key="open_position_policy_migration:position-unit-v2:exit-policy-v1:exit-policy-v2",
        payload={
            "position_id": "position-unit-v2",
            "old_exit_policy": "exit-policy-v1",
            "new_exit_policy": "exit-policy-v2",
            "migration_reason": "align_open_position_exit_policy",
            "checked_at": "2026-01-15T00:27:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="position_ownership_transfer_recorded",
        aggregate_id="position_ownership_transfer:position-unit-v1",
        idempotency_key="position_ownership_transfer:position-unit-v1:paper_executor:risk_controller",
        payload={
            "position_id": "position-unit-v1",
            "from_owner": "paper_executor",
            "to_owner": "risk_controller",
            "transfer_reason": "risk_revalidation_requires_controller",
            "transferred_at": "2026-01-15T00:28:00Z",
            "evidence_source": "unit",
        },
    )


def append_release_rollback_metric_trust_events(event_log_dir):
    log = V27EventLog(event_log_dir)
    log.append_event(
        event_type="rollback_verification_recorded",
        aggregate_id="rollback_verification:rollback-unit-v1",
        idempotency_key="rollback_verification:rollback-unit-v1:release-v2:release-v1",
        payload={
            "rollback_id": "rollback-unit-v1",
            "from_version": "release-v2",
            "to_version": "release-v1",
            "verified_at": "2026-01-15T00:29:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="partial_rollback_policy_recorded",
        aggregate_id="partial_rollback_policy:partial-rollback-unit-v1",
        idempotency_key="partial_rollback_policy:partial-rollback-unit-v1:dashboard:v27-readiness",
        payload={
            "rollback_id": "partial-rollback-unit-v1",
            "component_scope": "dashboard:v27-readiness",
            "dependency_scope": "read_model_refresh",
            "verification_plan": "health_check_and_scope_audit",
            "rolled_back_at": "2026-01-15T00:30:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="release_readiness_review_recorded",
        aggregate_id="release_readiness_review:release-review-unit-v1",
        idempotency_key="release_readiness_review:release-review-unit-v1:approved",
        payload={
            "review_id": "release-review-unit-v1",
            "release_id": "release-unit-v2",
            "required_evidence": ["health", "scope_audit", "pytest"],
            "approval_status": "approved",
            "approved_at": "2026-01-15T00:31:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="change_freeze_recorded",
        aggregate_id="change_freeze:freeze-unit-v1",
        idempotency_key="change_freeze:freeze-unit-v1:normal_tiny_runtime",
        payload={
            "freeze_id": "freeze-unit-v1",
            "scope": "normal_tiny_runtime",
            "start_at": "2026-01-15T00:32:00Z",
            "end_at": "2026-01-15T01:32:00Z",
            "exception_policy": "break_glass_only",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="notification_channel_integrity_recorded",
        aggregate_id="notification_channel_integrity:ops-alerts-unit",
        idempotency_key="notification_channel_integrity:ops-alerts-unit:verified",
        payload={
            "channel_id": "ops-alerts-unit",
            "destination_hash": sha256_hex({"channel_id": "ops-alerts-unit", "destination": "telegram_ops"}),
            "signature_required": True,
            "delivery_status": "verified",
            "checked_at": "2026-01-15T00:33:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="runbook_freshness_recorded",
        aggregate_id="runbook_freshness:normal-tiny-rollback-runbook",
        idempotency_key="runbook_freshness:normal-tiny-rollback-runbook:fresh",
        payload={
            "runbook_id": "normal-tiny-rollback-runbook",
            "owner": "runtime",
            "last_reviewed_at": "2026-01-15T00:34:00Z",
            "max_age_days": 30,
            "freshness_status": "fresh",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="metric_backfill_impact_recorded",
        aggregate_id="metric_backfill_impact:metric-backfill-unit-v1",
        idempotency_key="metric_backfill_impact:metric-backfill-unit-v1:metric_window_only",
        payload={
            "backfill_id": "metric-backfill-unit-v1",
            "metric_id": "telegram_capture_rate_D3b",
            "impact_scope": "metric_window_only",
            "impact_report_hash": sha256_hex({"backfill_id": "metric-backfill-unit-v1", "metric_id": "telegram_capture_rate_D3b"}),
            "checked_at": "2026-01-15T00:35:00Z",
            "evidence_source": "unit",
        },
    )
    log.append_event(
        event_type="selection_bias_diagnostic_recorded",
        aggregate_id="selection_bias_diagnostic:selection-bias-unit-v1",
        idempotency_key="selection_bias_diagnostic:selection-bias-unit-v1:within_tolerance",
        payload={
            "diagnostic_id": "selection-bias-unit-v1",
            "selection_policy_version": "normal-tiny-selection-v1",
            "included_count": 40,
            "excluded_count": 8,
            "bias_result": "within_tolerance",
            "checked_at": "2026-01-15T00:36:00Z",
            "evidence_source": "unit",
        },
    )


def append_final_normal_tiny_blocking_events(event_log_dir):
    log = V27EventLog(event_log_dir)
    log.append_event(
        event_type="access_review_recorded",
        aggregate_id="access_review:access-review-unit-v1",
        idempotency_key="access_review:access-review-unit-v1:operator-unit",
        payload={
            "review_id": "access-review-unit-v1",
            "operator_id": "operator-unit",
            "scope": "dashboard:admin_mutation",
            "privilege_delta": "reduced",
            "reviewed_at": "2026-01-15T00:37:00Z",
        },
    )
    log.append_event(
        event_type="approval_workflow_recorded",
        aggregate_id="approval_workflow:approval-unit-v1",
        idempotency_key="approval_workflow:approval-unit-v1:mutation-unit-v1",
        payload={
            "approval_id": "approval-unit-v1",
            "mutation_id": "mutation-unit-v1",
            "required_approvers": ["runtime-owner"],
            "approval_state": "approved",
            "approved_at": "2026-01-15T00:38:00Z",
        },
    )
    log.append_event(
        event_type="break_glass_access_recorded",
        aggregate_id="break_glass_access:break-glass-unit-v1",
        idempotency_key="break_glass_access:break-glass-unit-v1:audit-unit-v1",
        payload={
            "break_glass_id": "break-glass-unit-v1",
            "operator_id": "operator-unit",
            "reason": "restore_paper_read_model",
            "expires_at": "2026-01-15T01:39:00Z",
            "audit_event_id": "audit-unit-v1",
        },
    )
    log.append_event(
        event_type="csv_spreadsheet_injection_recorded",
        aggregate_id="csv_spreadsheet_injection:export-unit-v1:symbol",
        idempotency_key="csv_spreadsheet_injection:export-unit-v1:symbol",
        payload={
            "export_id": "export-unit-v1",
            "column_name": "symbol",
            "unsafe_prefix_detected": True,
            "sanitization_policy": "escape_formula_prefix",
            "checked_at": "2026-01-15T00:40:00Z",
        },
    )
    log.append_event(
        event_type="evidence_external_anchoring_recorded",
        aggregate_id="evidence_external_anchoring:anchor-unit-v1",
        idempotency_key="evidence_external_anchoring:anchor-unit-v1",
        payload={
            "anchor_id": "anchor-unit-v1",
            "anchored_hash": sha256_hex({"anchor_id": "anchor-unit-v1", "target": "v27_denominator_projection"}),
            "anchor_target": "v27_denominator_projection",
            "anchored_at": "2026-01-15T00:41:00Z",
        },
    )
    log.append_event(
        event_type="experiment_assignment_immutability_recorded",
        aggregate_id="experiment_assignment_immutability:assignment-unit-v1",
        idempotency_key="experiment_assignment_immutability:assignment-unit-v1",
        payload={
            "assignment_id": "assignment-unit-v1",
            "randomization_unit": "normal_tiny_promotion_policy",
            "original_assignment_hash": sha256_hex({"assignment_id": "assignment-unit-v1", "arm": "control"}),
            "attempted_change_hash": sha256_hex({"assignment_id": "assignment-unit-v1", "arm": "treatment"}),
            "detected_at": "2026-01-15T00:42:00Z",
        },
    )
    log.append_event(
        event_type="incident_postmortem_recorded",
        aggregate_id="incident_postmortem:postmortem-unit-v1",
        idempotency_key="incident_postmortem:postmortem-unit-v1:incident-unit-v1",
        payload={
            "postmortem_id": "postmortem-unit-v1",
            "incident_id": "incident-unit-v1",
            "root_cause": "read_model_refresh_regression",
            "corrective_actions": ["add_scope_audit_regression"],
            "approved_at": "2026-01-15T00:43:00Z",
        },
    )
    log.append_event(
        event_type="label_dispute_resolution_recorded",
        aggregate_id="label_dispute_resolution:label-dispute-unit-v1",
        idempotency_key="label_dispute_resolution:label-dispute-unit-v1:quarantine",
        payload={
            "dispute_id": "label-dispute-unit-v1",
            "label_id": "label-unit-v1",
            "resolution_action": "quarantine",
            "resolved_at": "2026-01-15T00:44:00Z",
        },
    )
    log.append_event(
        event_type="negative_control_recorded",
        aggregate_id="negative_control:negative-control-unit-v1",
        idempotency_key="negative_control:negative-control-unit-v1:0.002",
        payload={
            "control_id": "negative-control-unit-v1",
            "control_group": "holdout",
            "expected_no_effect_metric": 0.01,
            "observed_effect": 0.002,
            "checked_at": "2026-01-15T00:45:00Z",
        },
    )
    log.append_event(
        event_type="operator_training_certification_recorded",
        aggregate_id="operator_training_certification:operator-unit:normal_tiny_runtime_ops",
        idempotency_key="operator_training_certification:operator-unit:normal_tiny_runtime_ops",
        payload={
            "operator_id": "operator-unit",
            "training_module": "normal_tiny_runtime_ops",
            "certification_status": "certified",
            "expires_at": "2026-02-15T00:46:00Z",
            "checked_at": "2026-01-15T00:46:00Z",
        },
    )


def append_runtime_trust_governance_events(event_log_dir):
    log = V27EventLog(event_log_dir)
    log.append_event(
        event_type="runtime_spec_assertion_recorded",
        aggregate_id="runtime_spec_assertion:runtime-spec-assertion-unit-v1",
        idempotency_key="runtime_spec_assertion:runtime-spec-assertion-unit-v1:RealtimeCleanDetector",
        payload={
            "assertion_id": "runtime-spec-assertion-unit-v1",
            "contract_id": "RealtimeCleanDetector",
            "runtime_location": "scripts/paper_trade_monitor.py:realtime_clean_gate",
            "failure_action": "runtime_assert_failed",
        },
    )
    log.append_event(
        event_type="minimum_viable_trust_boundary_recorded",
        aggregate_id="minimum_viable_trust_boundary:minimum-viable-trust-unit-v1",
        idempotency_key="minimum_viable_trust_boundary:minimum-viable-trust-unit-v1",
        payload={
            "boundary_id": "minimum-viable-trust-unit-v1",
            "trusted_inputs": ["entry_quote", "exit_quote"],
            "untrusted_inputs": ["mark_only_peak", "posthoc_label"],
            "required_contracts": ["RealtimeCleanDetector", "QuoteIntentBindingContract"],
            "failure_action": "mode_blocked",
        },
    )
    log.append_event(
        event_type="evidence_conflict_recorded",
        aggregate_id="evidence_conflict:evidence-conflict-unit-v1",
        idempotency_key="evidence_conflict:evidence-conflict-unit-v1",
        payload={
            "conflict_id": "evidence-conflict-unit-v1",
            "evidence_a_hash": sha256_hex({"evidence": "a", "scope": "unit"}),
            "evidence_b_hash": sha256_hex({"evidence": "b", "scope": "unit"}),
            "resolution_policy": "quarantine_then_operator_review",
            "resolved_at": "2026-01-15T00:47:00Z",
        },
    )
    log.append_event(
        event_type="evidence_aging_recorded",
        aggregate_id="evidence_aging:evidence-aging-unit-v1",
        idempotency_key="evidence_aging:evidence-aging-unit-v1",
        payload={
            "evidence_id": "evidence-aging-unit-v1",
            "evidence_type": "quote_clean_snapshot",
            "max_age_ms": 120000,
            "age_ms": 30000,
            "expiration_action": "revalidate_before_entry",
        },
    )
    log.append_event(
        event_type="market_regime_invalidates_evidence_recorded",
        aggregate_id="market_regime_invalidates_evidence:market-regime-unit-v1:evidence-aging-unit-v1",
        idempotency_key="market_regime_invalidates_evidence:market-regime-unit-v1:evidence-aging-unit-v1",
        payload={
            "regime_id": "market-regime-unit-v1",
            "evidence_id": "evidence-aging-unit-v1",
            "invalidating_signal": "liquidity_regime_flip",
            "action": "revalidate_evidence",
            "detected_at": "2026-01-15T00:48:00Z",
        },
    )
    log.append_event(
        event_type="source_alpha_decay_exit_criteria_recorded",
        aggregate_id="source_alpha_decay_exit_criteria:premium-clean-source-unit-v1",
        idempotency_key="source_alpha_decay_exit_criteria:premium-clean-source-unit-v1:24h",
        payload={
            "source_id": "premium-clean-source-unit-v1",
            "alpha_metric": 0.12,
            "decay_window": "24h",
            "exit_threshold": 0.05,
            "action": "keep_source",
        },
    )
    log.append_event(
        event_type="false_negative_budget_recorded",
        aggregate_id="false_negative_budget:false-negative-budget-unit-v1",
        idempotency_key="false_negative_budget:false-negative-budget-unit-v1:0.08",
        payload={
            "budget_id": "false-negative-budget-unit-v1",
            "hazard_class": "missed_clean_gold_dog",
            "allowed_false_negative_rate": 0.15,
            "observed_rate": 0.08,
            "action": "continue_with_watch",
        },
    )
    log.append_event(
        event_type="small_sample_decision_recorded",
        aggregate_id="small_sample_decision:small-sample-policy-unit-v1",
        idempotency_key="small_sample_decision:small-sample-policy-unit-v1:40",
        payload={
            "policy_id": "small-sample-policy-unit-v1",
            "sample_size": 40,
            "min_sample_size": 30,
            "decision_allowed": True,
            "fallback_action": "hold_promotion",
        },
    )
    log.append_event(
        event_type="safety_vs_capture_tradeoff_recorded",
        aggregate_id="safety_vs_capture_tradeoff:safety-capture-tradeoff-unit-v1",
        idempotency_key="safety_vs_capture_tradeoff:safety-capture-tradeoff-unit-v1",
        payload={
            "tradeoff_id": "safety-capture-tradeoff-unit-v1",
            "safety_metric": 0.98,
            "capture_metric": 0.62,
            "chosen_policy": "safety_first_capture_watch",
            "approved_at": "2026-01-15T00:49:00Z",
        },
    )
    log.append_event(
        event_type="implementation_drift_monitor_recorded",
        aggregate_id="implementation_drift_monitor:implementation-drift-unit-v1",
        idempotency_key="implementation_drift_monitor:implementation-drift-unit-v1:RealtimeCleanDetector",
        payload={
            "drift_id": "implementation-drift-unit-v1",
            "spec_contract_id": "RealtimeCleanDetector",
            "runtime_location": "scripts/paper_trade_monitor.py:realtime_clean_gate",
            "drift_detected": False,
            "detected_at": "2026-01-15T00:50:00Z",
        },
    )


def append_assumption_priority_escalation_governance_events(event_log_dir):
    log = V27EventLog(event_log_dir)
    log.append_event(
        event_type="assumption_registry_recorded",
        aggregate_id="assumption_registry:assumption-unit-v1",
        idempotency_key="assumption_registry:assumption-unit-v1:normal_tiny_capture_metrics",
        payload={
            "assumption_id": "assumption-unit-v1",
            "scope": "normal_tiny_capture_metrics",
            "owner": "runtime-owner",
            "evidence_link": "v27_denominator_projection:runtime_trust",
            "expires_at": "2026-02-15T00:51:00Z",
        },
    )
    log.append_event(
        event_type="assumption_invalidation_trigger_recorded",
        aggregate_id="assumption_invalidation_trigger:assumption-unit-v1:missed_clean_gold_false_negative_rate",
        idempotency_key="assumption_invalidation_trigger:assumption-unit-v1:missed_clean_gold_false_negative_rate",
        payload={
            "assumption_id": "assumption-unit-v1",
            "trigger_metric": "missed_clean_gold_false_negative_rate",
            "threshold": 0.15,
            "observed_value": 0.21,
            "invalidated_at": "2026-01-15T00:52:00Z",
        },
    )
    log.append_event(
        event_type="contract_priority_graph_recorded",
        aggregate_id="contract_priority_graph:contract-priority-graph-unit-v1",
        idempotency_key=(
            "contract_priority_graph:contract-priority-graph-unit-v1:"
            "SafetyVsCaptureTradeoffContract:SourceAlphaDecayExitCriteria"
        ),
        payload={
            "graph_id": "contract-priority-graph-unit-v1",
            "higher_priority_contract": "SafetyVsCaptureTradeoffContract",
            "lower_priority_contract": "SourceAlphaDecayExitCriteria",
            "cycle_detected": False,
            "resolved_at": "2026-01-15T00:53:00Z",
        },
    )
    log.append_event(
        event_type="contract_conflict_resolution_recorded",
        aggregate_id="contract_conflict_resolution:contract-conflict-unit-v1",
        idempotency_key="contract_conflict_resolution:contract-conflict-unit-v1:apply_higher_priority_contract",
        payload={
            "conflict_id": "contract-conflict-unit-v1",
            "higher_priority_contract": "SafetyVsCaptureTradeoffContract",
            "lower_priority_contract": "SourceAlphaDecayExitCriteria",
            "resolution_action": "apply_higher_priority_contract",
        },
    )
    log.append_event(
        event_type="contract_failure_blast_radius_recorded",
        aggregate_id="contract_failure_blast_radius:RealtimeCleanDetector",
        idempotency_key="contract_failure_blast_radius:RealtimeCleanDetector:normal_tiny_entry_block",
        payload={
            "contract_id": "RealtimeCleanDetector",
            "blast_radius": "normal_tiny_entry_block",
            "affected_modes": ["normal_tiny"],
            "fallback_action": "block_entry_and_hold_shadow",
            "reviewed_at": "2026-01-15T00:54:00Z",
        },
    )
    log.append_event(
        event_type="dashboard_triage_workflow_recorded",
        aggregate_id="dashboard_triage_workflow:dashboard-triage-unit-v1",
        idempotency_key="dashboard_triage_workflow:dashboard-triage-unit-v1:regression_budget_exceeded",
        payload={
            "triage_id": "dashboard-triage-unit-v1",
            "blocker_code": "regression_budget_exceeded",
            "owner": "runtime-owner",
            "next_action": "open_metric_escalation",
            "due_at": "2026-01-16T00:55:00Z",
        },
    )
    log.append_event(
        event_type="issue_escalation_from_metrics_recorded",
        aggregate_id="issue_escalation_from_metrics:missed_clean_gold_false_negative_rate",
        idempotency_key="issue_escalation_from_metrics:missed_clean_gold_false_negative_rate:issue-runtime-trust-unit-v1",
        payload={
            "metric_id": "missed_clean_gold_false_negative_rate",
            "threshold": 0.15,
            "issue_id": "issue-runtime-trust-unit-v1",
            "escalation_owner": "runtime-owner",
            "created_at": "2026-01-15T00:56:00Z",
        },
    )
    log.append_event(
        event_type="promotion_evidence_package_recorded",
        aggregate_id="promotion_evidence_package:promotion-evidence-package-unit-v1",
        idempotency_key="promotion_evidence_package:promotion-evidence-package-unit-v1:approved",
        payload={
            "package_id": "promotion-evidence-package-unit-v1",
            "evidence_hash": sha256_hex({"package_id": "promotion-evidence-package-unit-v1", "scope": "normal_tiny"}),
            "generated_at": "2026-01-15T00:57:00Z",
            "approval_status": "approved",
        },
    )
    log.append_event(
        event_type="regression_budget_recorded",
        aggregate_id="regression_budget:regression-budget-unit-v1",
        idempotency_key="regression_budget:regression-budget-unit-v1:clean_dog_capture_rate",
        payload={
            "budget_id": "regression-budget-unit-v1",
            "metric_id": "clean_dog_capture_rate",
            "allowed_regression": 0.03,
            "observed_regression": 0.01,
            "action": "allow_release",
        },
    )
    log.append_event(
        event_type="root_cause_taxonomy_versioning_recorded",
        aggregate_id="root_cause_taxonomy_versioning:root-cause-taxonomy-v1:quote_clean_evidence_expired",
        idempotency_key="root_cause_taxonomy_versioning:root-cause-taxonomy-v1:quote_clean_evidence_expired",
        payload={
            "taxonomy_version": "root-cause-taxonomy-v1",
            "root_cause_code": "quote_clean_evidence_expired",
            "severity": "high",
            "migration_policy": "map_legacy_codes_before_postmortem",
            "effective_at": "2026-01-15T00:58:00Z",
        },
    )


def append_fee_provider_and_risk_events(event_log_dir):
    fee_version = "fee-v1"
    V27EventLog(event_log_dir).append_event(
        event_type="fee_schedule_recorded",
        aggregate_id=f"fee_schedule:jupiter_ultra:solana:{fee_version}",
        idempotency_key=f"fee_schedule:jupiter_ultra:solana:{fee_version}",
        payload={
            "fee_source_id": "fee-source-jupiter-ultra",
            "provider": "jupiter_ultra",
            "chain": "solana",
            "fee_version": fee_version,
            "source_hash": sha256_hex({"provider": "jupiter_ultra", "fee_version": fee_version}),
            "fee_model_hash": sha256_hex({"fee_model": "unit", "fee_version": fee_version}),
            "effective_at": "2026-01-15T00:00:00Z",
            "supersedes_version": "none",
            "checked_at": "2026-01-15T00:00:01Z",
            "evidence_source": "unit",
        },
    )
    V27EventLog(event_log_dir).append_event(
        event_type="provider_credential_scope_recorded",
        aggregate_id="provider_credential_scope:jupiter_ultra:jupiter-ultra-unit-token",
        idempotency_key="provider_credential_scope:jupiter_ultra:jupiter-ultra-unit-token",
        payload={
            "credential_id": "jupiter-ultra-unit-token",
            "provider": "jupiter_ultra",
            "allowed_endpoints": ["/ultra/v1/order", "/ultra/v1/execute"],
            "allowed_modes": ["paper", "normal_tiny"],
            "expires_at": "2099-01-01T00:00:00Z",
            "credential_status": "active",
            "checked_at": "2026-01-15T00:00:01Z",
            "evidence_source": "unit",
        },
    )
    V27EventLog(event_log_dir).append_event(
        event_type="provider_request_replay_recorded",
        aggregate_id="provider_request_replay:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="provider_request_replay:1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "request_id": "provider-request-ready",
            "provider": "jupiter_ultra",
            "request_hash": sha256_hex({"request_id": "provider-request-ready"}),
            "retry_count": 0,
            "decision_reason": "initial_attempt_no_replay_needed",
            "replay_status": "not_replayed_no_retry",
            "checked_at": "2026-01-15T00:00:02Z",
        },
    )
    V27EventLog(event_log_dir).append_event(
        event_type="provider_response_authenticity_recorded",
        aggregate_id="provider_response_authenticity:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="provider_response_authenticity:1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "response_id": "provider-response-ready",
            "provider": "jupiter_ultra",
            "signature_status": "verified",
            "transport_security": "tls_verified",
            "verified_at": "2026-01-15T00:00:03Z",
            "response_hash": sha256_hex({"response_id": "provider-response-ready"}),
        },
    )
    V27EventLog(event_log_dir).append_event(
        event_type="risk_revalidation_after_entry_recorded",
        aggregate_id="risk_revalidation_after_entry:solana:TokenReady:unknown_pool:0:1",
        idempotency_key="risk_revalidation_after_entry:1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenReady",
            "symbol": "READY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "position_id": "paper_trade:1:position",
            "risk_event_id": "risk-event-ready",
            "risk_status": "clean",
            "exit_safety_action": "hold",
            "revalidated_at": "2026-01-15T00:00:04Z",
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


def append_shadow_trade_timing_events(event_log_dir):
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


def test_mode_readiness_reports_passed_evidence_and_blocks_unproven_modes(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_shadow_trade_timing_events(event_log_dir)
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
    assert matrix["event_log"]["verify"]["event_count"] == 6
    assert matrix["contract_statuses"]["CanonicalSpecIntegrityContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["NumericPrecisionContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ModeReadinessMatrix"]["status"] == "pass"
    assert matrix["contract_statuses"]["ModeReadinessMatrix"]["evidence"]["row_violations"] == []
    assert matrix["contract_statuses"]["EventSemanticsContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["EventSequencerContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DecisionReadModelFreshnessContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DenominatorDedupContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SourceDogLabelContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SpecConsistencyLinterContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["PaperModeSafetyBoundary"]["status"] == "pass"
    assert matrix["contract_statuses"]["ChainConfigContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SourceRegistryContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["AccessControlContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["AuditLogIntegrityContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["WritePathRegistryContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DirectDatabaseMutationBan"]["status"] == "pass"
    assert matrix["contract_statuses"]["AggregateBoundaryContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ClockRollbackGuardContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["EventSchemaCompatibilityContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["EnumEvolutionContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["MutationCommandIdempotencyContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ProjectionVersionIsolationContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SnapshotCompactionInvariantContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SnapshotCompactionReadBarrier"]["status"] == "pass"
    assert matrix["contract_statuses"]["WorkerHeartbeatContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SilentWorkerDeathDetector"]["status"] == "pass"
    assert matrix["contract_statuses"]["WarmStartSafetyContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ConnectionPoolPartitionContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DBLockContentionPolicy"]["status"] == "pass"
    assert matrix["contract_statuses"]["DatabaseTransactionIsolationContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DistributedLockBackendHealthContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["BackgroundJobRegistryContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ScheduledJobModeGateContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["EntryPointInventoryContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["StaticPolicyEnforcementContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["FeatureFlagDependencyContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["FilesystemDiskPressurePolicy"]["status"] == "pass"
    assert matrix["contract_statuses"]["APIResponseContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["APIResponseEnvelopeContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ErrorTaxonomyContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["LogRedactionVerificationContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["AdminSessionSecurityContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SecretAccessAuditContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["TelegramSessionSecurityContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["QueueAckNackContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["PipelineProgressInvariant"]["status"] == "pass"
    assert matrix["contract_statuses"]["ThreadPoolIsolationContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["CICDMergeGateContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["GeneratedClientContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SpecChangeImpactAnalysisContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ServiceReadinessProbeContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DashboardActionSeparationContract"]["status"] == "pass"
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
    assert matrix["contract_statuses"]["ManualReplaySafetyContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ManualReplaySafetyContract"]["evidence"]["provider_call_count"] == 0
    assert matrix["contract_statuses"]["SyntheticSentinelEventContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SyntheticSentinelEventContract"]["evidence"]["expected_delta_hash"] == matrix["contract_statuses"]["SyntheticSentinelEventContract"]["evidence"]["observed_delta_hash"]
    assert matrix["contract_statuses"]["ReconciliationDiffContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ReconciliationDiffContract"]["evidence"]["diff_count"] == 0
    assert matrix["contract_statuses"]["DeadLetterQueueContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ConsumerCheckpointContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ProjectionHandlerIdempotencyContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["CacheInvalidationContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ClientSideCacheContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ClientSideCacheContract"]["evidence"]["source_snapshot_hash"] == matrix["contract_statuses"]["ClientSideCacheContract"]["evidence"]["cache_value_hash"]
    assert matrix["contract_statuses"]["ClientSideFreshnessContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ClientSideFreshnessContract"]["evidence"]["fresh_enough"] is True
    assert matrix["contract_statuses"]["DashboardQueryProvenanceContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DashboardQueryProvenanceContract"]["evidence"]["result_hash"]
    assert matrix["contract_statuses"]["DashboardComputationProvenanceContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DashboardComputationProvenanceContract"]["evidence"]["provenance_hash"]
    assert matrix["contract_statuses"]["DataExportWatermarkContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DataExportWatermarkContract"]["evidence"]["watermark"]
    assert matrix["contract_statuses"]["DataExportEnvelopeContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["DataExportEnvelopeContract"]["evidence"]["envelope_version"] == "v2.7.0.data_export_envelope.v1"
    assert matrix["contract_statuses"]["SignalCreditAssignmentContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ReferencePriceContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["MetricsWindowContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["TradeOutcomeLabelContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["StandardizedStopContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ExAnteFeasibility"]["status"] == "pass"
    assert matrix["contract_statuses"]["EarliestActionableTime"]["status"] == "pass"
    assert matrix["contract_statuses"]["ParserCanaryCorpusContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ParserAmbiguityContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["TelegramForwardedMessagePolicy"]["status"] == "pass"
    assert matrix["contract_statuses"]["PremiumSourceAccessHealthContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SourceAuthenticityContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ParserConfusablesContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ImageOCRSignalPolicy"]["status"] == "pass"
    assert matrix["contract_statuses"]["SourceImpersonationDetector"]["status"] == "pass"
    assert matrix["contract_statuses"]["IdentityMergeSplitContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["ReKeyingContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["SourceGapBackfillBoundary"]["status"] == "pass"
    assert matrix["contract_statuses"]["ObservationPolicyContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["CounterfactualEntryTime"]["status"] == "pass"
    assert matrix["modes"]["observe_only"]["status"] == "allowed"
    assert "NumericPrecisionContract" in matrix["modes"]["observe_only"]["required_contracts"]
    assert "CICDMergeGateContract" in matrix["modes"]["observe_only"]["required_contracts"]
    assert "GeneratedClientContract" in matrix["modes"]["observe_only"]["required_contracts"]
    assert "SpecChangeImpactAnalysisContract" in matrix["modes"]["observe_only"]["required_contracts"]
    assert matrix["modes"]["shadow"]["status"] == "allowed"
    assert "ManualReplaySafetyContract" in matrix["modes"]["shadow"]["required_contracts"]
    assert "SyntheticSentinelEventContract" in matrix["modes"]["shadow"]["required_contracts"]
    assert "ReconciliationDiffContract" in matrix["modes"]["shadow"]["required_contracts"]
    assert "ClientSideCacheContract" in matrix["modes"]["shadow"]["required_contracts"]
    assert "DataExportEnvelopeContract" in matrix["modes"]["shadow"]["required_contracts"]
    assert "TradeOutcomeLabelContract" in matrix["modes"]["shadow"]["required_contracts"]
    assert "StandardizedStopContract" in matrix["modes"]["shadow"]["required_contracts"]
    assert "ExAnteFeasibility" in matrix["modes"]["shadow"]["required_contracts"]
    assert "EarliestActionableTime" in matrix["modes"]["shadow"]["required_contracts"]
    assert "CounterfactualEntryTime" in matrix["modes"]["shadow"]["required_contracts"]
    assert "ParserCanaryCorpusContract" in matrix["modes"]["shadow"]["required_contracts"]
    assert "SourceImpersonationDetector" in matrix["modes"]["shadow"]["required_contracts"]
    assert "ObservationPolicyContract" in matrix["modes"]["shadow"]["required_contracts"]
    assert "IdentityMergeSplitContract" in matrix["modes"]["shadow"]["required_contracts"]
    assert matrix["highest_allowed_mode"] == "shadow"
    assert matrix["modes"]["shadow"]["blocking_contracts"] == []
    assert matrix["modes"]["ultra_tiny"]["status"] == "blocked"
    assert "EntryExecutionStateMachine" in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "DecisionAudit" in matrix["modes"]["ultra_tiny"]["required_contracts"]
    assert "LedgerSnapshotHashContract" in matrix["modes"]["ultra_tiny"]["required_contracts"]
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


def test_mode_readiness_consumes_provider_dependency_resource_trust_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_provider_dependency_resource_trust_events(event_log_dir)
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
        "ProviderByzantineQuorumContract",
        "ProviderCachePoisoningGuard",
        "ExternalDependencyContract",
        "ThirdPartyStatusCorrelationContract",
        "ResourceExhaustionContract",
    ):
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert (
        matrix["contract_statuses"]["ProviderByzantineQuorumContract"]["evidence"]["valid_provider_byzantine_quorum_count"]
        == 1
    )
    assert matrix["contract_statuses"]["ExternalDependencyContract"]["evidence"]["valid_external_dependency_count"] == 1
    assert matrix["contract_statuses"]["ResourceExhaustionContract"]["evidence"]["valid_resource_exhaustion_count"] == 1
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_config_activation_retry_policy_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_config_activation_retry_policy_events(event_log_dir)
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
        "ConfigDistributionContract",
        "ConfigDistributionAckContract",
        "InFlightConfigRotationPolicy",
        "PolicyActivationBarrierContract",
        "RetryPolicyCatalogContract",
    ):
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert matrix["contract_statuses"]["ConfigDistributionAckContract"]["evidence"]["valid_config_distribution_ack_count"] == 2
    assert matrix["contract_statuses"]["RetryPolicyCatalogContract"]["evidence"]["valid_retry_policy_catalog_count"] == 1
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_alert_model_release_runtime_trust_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_alert_model_release_runtime_trust_events(event_log_dir)
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
        "AlertNoiseBudgetContract",
        "AlertSuppressionAuditContract",
        "CanaryAbortContract",
        "ModelArtifactRuntimeCompatibilityContract",
        "ModelRollbackContract",
        "PostReleaseMonitoringWindow",
        "TrainingPoisoningGuard",
    ):
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert matrix["contract_statuses"]["AlertNoiseBudgetContract"]["evidence"]["valid_alert_noise_budget_count"] == 1
    assert matrix["contract_statuses"]["ModelRollbackContract"]["evidence"]["valid_model_rollback_count"] == 1
    assert matrix["contract_statuses"]["TrainingPoisoningGuard"]["evidence"]["valid_training_poisoning_guard_count"] == 1
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_position_feature_runtime_trust_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_position_feature_runtime_trust_events(event_log_dir)
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
        "FeatureStoreConsistencyContract",
        "DynamicTokenAuthorityChangeContract",
        "AdversarialExecutionSimulationContract",
        "OpenPositionValuationContract",
        "ExitPolicyMigrationContract",
        "OpenPositionPolicyMigrationContract",
        "PositionOwnershipTransferContract",
    ):
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert matrix["contract_statuses"]["FeatureStoreConsistencyContract"]["evidence"]["valid_feature_store_consistency_count"] == 1
    assert matrix["contract_statuses"]["OpenPositionValuationContract"]["evidence"]["valid_open_position_valuation_count"] == 1
    assert (
        matrix["contract_statuses"]["PositionOwnershipTransferContract"]["evidence"]["valid_position_ownership_transfer_count"]
        == 1
    )
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_release_rollback_metric_trust_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_release_rollback_metric_trust_events(event_log_dir)
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
        "RollbackVerificationContract",
        "PartialRollbackPolicy",
        "ReleaseReadinessReviewContract",
        "ChangeFreezeContract",
        "NotificationChannelIntegrityContract",
        "RunbookFreshnessContract",
        "MetricBackfillImpactContract",
        "SelectionBiasDiagnosticContract",
    ):
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert matrix["contract_statuses"]["RollbackVerificationContract"]["evidence"]["valid_rollback_verification_count"] == 1
    assert matrix["contract_statuses"]["ReleaseReadinessReviewContract"]["evidence"]["valid_release_readiness_review_count"] == 1
    assert matrix["contract_statuses"]["MetricBackfillImpactContract"]["evidence"]["valid_metric_backfill_impact_count"] == 1
    assert (
        matrix["contract_statuses"]["SelectionBiasDiagnosticContract"]["evidence"]["valid_selection_bias_diagnostic_count"]
        == 1
    )
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_final_normal_tiny_blocking_contracts(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_final_normal_tiny_blocking_events(event_log_dir)
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
        "AccessReviewContract",
        "ApprovalWorkflowContract",
        "BreakGlassAccessContract",
        "CSVSpreadsheetInjectionContract",
        "EvidenceExternalAnchoringContract",
        "ExperimentAssignmentImmutabilityContract",
        "IncidentPostmortemContract",
        "LabelDisputeResolutionContract",
        "NegativeControlContract",
        "OperatorTrainingCertificationContract",
    ):
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert matrix["contract_statuses"]["AccessReviewContract"]["evidence"]["valid_access_review_count"] == 1
    assert matrix["contract_statuses"]["ApprovalWorkflowContract"]["evidence"]["valid_approval_workflow_count"] == 1
    assert (
        matrix["contract_statuses"]["EvidenceExternalAnchoringContract"]["evidence"][
            "valid_evidence_external_anchoring_count"
        ]
        == 1
    )
    assert matrix["contract_statuses"]["NegativeControlContract"]["evidence"]["valid_negative_control_count"] == 1
    assert (
        matrix["contract_statuses"]["OperatorTrainingCertificationContract"]["evidence"][
            "valid_operator_training_certification_count"
        ]
        == 1
    )
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_runtime_trust_governance_contracts(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_runtime_trust_governance_events(event_log_dir)
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
        "RuntimeSpecAssertionContract",
        "MinimumViableTrustBoundary",
        "EvidenceConflictContract",
        "EvidenceAgingContract",
        "MarketRegimeInvalidatesEvidence",
        "SourceAlphaDecayExitCriteria",
        "FalseNegativeBudgetContract",
        "SmallSampleDecisionPolicy",
        "SafetyVsCaptureTradeoffContract",
        "ImplementationDriftMonitor",
    ):
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert matrix["contract_statuses"]["RuntimeSpecAssertionContract"]["evidence"]["valid_runtime_spec_assertion_count"] == 1
    assert (
        matrix["contract_statuses"]["MinimumViableTrustBoundary"]["evidence"][
            "valid_minimum_viable_trust_boundary_count"
        ]
        == 1
    )
    assert matrix["contract_statuses"]["EvidenceConflictContract"]["evidence"]["valid_evidence_conflict_count"] == 1
    assert matrix["contract_statuses"]["EvidenceAgingContract"]["evidence"]["valid_evidence_aging_count"] == 1
    assert (
        matrix["contract_statuses"]["MarketRegimeInvalidatesEvidence"]["evidence"][
            "valid_market_regime_invalidates_evidence_count"
        ]
        == 1
    )
    assert (
        matrix["contract_statuses"]["SourceAlphaDecayExitCriteria"]["evidence"][
            "valid_source_alpha_decay_exit_criteria_count"
        ]
        == 1
    )
    assert matrix["contract_statuses"]["FalseNegativeBudgetContract"]["evidence"]["valid_false_negative_budget_count"] == 1
    assert matrix["contract_statuses"]["SmallSampleDecisionPolicy"]["evidence"]["valid_small_sample_decision_count"] == 1
    assert (
        matrix["contract_statuses"]["SafetyVsCaptureTradeoffContract"]["evidence"][
            "valid_safety_vs_capture_tradeoff_count"
        ]
        == 1
    )
    assert (
        matrix["contract_statuses"]["ImplementationDriftMonitor"]["evidence"][
            "valid_implementation_drift_monitor_count"
        ]
        == 1
    )
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_assumption_priority_escalation_governance_contracts(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_assumption_priority_escalation_governance_events(event_log_dir)
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
        "AssumptionRegistryContract",
        "AssumptionInvalidationTrigger",
        "ContractPriorityGraph",
        "ContractConflictResolutionContract",
        "ContractFailureBlastRadius",
        "DashboardTriageWorkflowContract",
        "IssueEscalationFromMetricsContract",
        "PromotionEvidencePackageContract",
        "RegressionBudgetContract",
        "RootCauseTaxonomyVersioning",
    ):
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert matrix["contract_statuses"]["AssumptionRegistryContract"]["evidence"]["valid_assumption_registry_count"] == 1
    assert (
        matrix["contract_statuses"]["AssumptionInvalidationTrigger"]["evidence"][
            "valid_assumption_invalidation_trigger_count"
        ]
        == 1
    )
    assert matrix["contract_statuses"]["ContractPriorityGraph"]["evidence"]["valid_contract_priority_graph_count"] == 1
    assert (
        matrix["contract_statuses"]["ContractConflictResolutionContract"]["evidence"][
            "valid_contract_conflict_resolution_count"
        ]
        == 1
    )
    assert (
        matrix["contract_statuses"]["ContractFailureBlastRadius"]["evidence"][
            "valid_contract_failure_blast_radius_count"
        ]
        == 1
    )
    assert (
        matrix["contract_statuses"]["DashboardTriageWorkflowContract"]["evidence"][
            "valid_dashboard_triage_workflow_count"
        ]
        == 1
    )
    assert (
        matrix["contract_statuses"]["IssueEscalationFromMetricsContract"]["evidence"][
            "valid_issue_escalation_from_metrics_count"
        ]
        == 1
    )
    assert (
        matrix["contract_statuses"]["PromotionEvidencePackageContract"]["evidence"][
            "valid_promotion_evidence_package_count"
        ]
        == 1
    )
    assert matrix["contract_statuses"]["RegressionBudgetContract"]["evidence"]["valid_regression_budget_count"] == 1
    assert (
        matrix["contract_statuses"]["RootCauseTaxonomyVersioning"]["evidence"][
            "valid_root_cause_taxonomy_versioning_count"
        ]
        == 1
    )
    assert "RandomnessControlContract" in matrix["modes"]["normal_tiny"]["blocking_contracts"]


def test_mode_readiness_consumes_fee_provider_auth_and_risk_for_normal_tiny(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_realtime_clean_event(event_log_dir)
    append_quote_intent_binding_event(event_log_dir)
    append_raw_provider_evidence_event(event_log_dir)
    append_execution_control_event(event_log_dir)
    append_fee_provider_and_risk_events(event_log_dir)
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
        "FeeScheduleSourceContract",
        "FeeScheduleVersionContract",
        "ProviderCredentialScopeContract",
        "ProviderRequestReplayContract",
        "ProviderResponseAuthenticityContract",
        "RiskRevalidationAfterEntryContract",
    ):
        assert matrix["contract_statuses"][contract_id]["status"] == "pass"
        assert contract_id not in matrix["modes"]["normal_tiny"]["blocking_contracts"]
    assert matrix["contract_statuses"]["FeeScheduleSourceContract"]["evidence"]["valid_fee_schedule_source_count"] == 1
    assert matrix["contract_statuses"]["ProviderRequestReplayContract"]["evidence"]["valid_provider_request_replay_count"] == 1
    assert matrix["contract_statuses"]["RiskRevalidationAfterEntryContract"]["evidence"]["valid_risk_revalidation_after_entry_count"] == 1
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


def test_mode_readiness_consumes_decision_audit_and_ledger_snapshot_hash(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    append_seed_events(event_log_dir)
    append_shadow_trade_timing_events(event_log_dir)
    append_realtime_clean_event(event_log_dir)
    append_quote_intent_binding_event(event_log_dir)
    append_raw_provider_evidence_event(event_log_dir)
    append_decision_audit_event(event_log_dir)
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

    decision_audit = matrix["contract_statuses"]["DecisionAudit"]
    ledger_snapshot = matrix["contract_statuses"]["LedgerSnapshotHashContract"]
    assert decision_audit["status"] == "pass"
    assert decision_audit["evidence"]["valid_decision_audit_count"] == 1
    assert decision_audit["evidence"]["future_leakage_count"] == 0
    assert ledger_snapshot["status"] == "pass"
    assert ledger_snapshot["evidence"]["ledger_checkpoint_id"] == "ledger_checkpoint:unit:paper:1"
    assert ledger_snapshot["evidence"]["snapshot_hash_ok"] is True
    assert ledger_snapshot["evidence"]["replay_hash"]
    assert "DecisionAudit" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "LedgerSnapshotHashContract" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert matrix["modes"]["ultra_tiny"]["status"] == "allowed"
    assert matrix["highest_allowed_mode"] == "ultra_tiny"


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
