import sys

sys.path.insert(0, "scripts")

from v27_denominator_projection import build_denominator_projection, build_denominator_read_model_snapshot  # noqa: E402
from v27_event_log import V27EventLog, sha256_hex  # noqa: E402


def append_decision(
    log,
    *,
    decision_id,
    token_ca,
    source_dog_label="gold",
    captured=False,
    pool="pool-a",
    route="unit_route",
    **flags,
):
    payload = {
        "decision_event_id": decision_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:] if token_ca else None,
        "route": route,
        "component": "unit_gate",
        "legacy_event_type": "decision",
        "decision": "enter" if captured else "shadow",
        "reason": "unit",
        "payload": {
            "chain": "solana",
            "canonical_pool_group": pool,
            "lifecycle_epoch": 0,
            "source_dog_label": source_dog_label,
            "captured": captured,
            **flags,
        },
        "lifecycle": {},
    }
    return log.append_event(
        event_type="paper_decision_event_recorded",
        aggregate_id=f"paper_decision:token:{token_ca or decision_id}",
        payload=payload,
        idempotency_key=f"paper_decision_events:{decision_id}",
    )


FULL_D3B_FLAGS = {
    "telegram_seen": True,
    "realtime_observable": True,
    "realtime_clean": True,
    "entry_quote_executable": True,
    "exit_quote_executable": True,
    "liquidity_ok": True,
    "critical_risk_ok": True,
    "ex_ante_feasible": True,
    "reclaim_confirmed": True,
    "not_overextended": True,
    "model_pass": True,
}


def append_ex_ante(log, *, token_ca, paper_trade_id=1, pool="pool-a", used_future_peak=False, missing_version=False):
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "decision_ts": 1_700_000_002,
        "decision_available_at": 1_700_000_002,
        "counterfactual_entry_ts": 1_700_000_003,
        "feasibility_policy_version": None if missing_version else "legacy_actual_paper_entry_feasibility_v0.1",
        "ex_ante_feasible": True,
        "feasibility_class": "legacy_actual_paper_entry",
        "entry_quote_available": True,
        "entry_quote_available_at": 1_700_000_002,
        "current_quote_availability": True,
        "current_pool_resolution": pool,
        "current_provider_health": "legacy_not_recorded",
        "current_risk_availability": "legacy_not_recorded",
        "current_queue_delay_sec": 0,
        "feature_max_available_at": 1_700_000_002,
        "used_future_peak": used_future_peak,
        "used_future_outcome": False,
        "used_posthoc_label": False,
        "forbidden_future_fields_used": [],
    }
    return log.append_event(
        event_type="ex_ante_feasibility_recorded",
        aggregate_id=f"ex_ante_feasibility:solana:{token_ca}:{pool}:0:{paper_trade_id}",
        idempotency_key=f"ex_ante_feasibility:{paper_trade_id}",
        payload=payload,
    )


def append_earliest_actionable(log, *, token_ca, paper_trade_id=1, pool="pool-a", entry_after_peak=False, missing_version=False):
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "earliest_actionable_policy_version": None if missing_version else "legacy_actual_paper_entry_actionable_time_v0.1",
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
        "peak_ts": 1_700_000_002 if entry_after_peak else 1_700_000_120,
        "peak_ts_quality": "legacy_outcome_window_close_proxy",
        "peak_ts_source": "paper_trade_exit_ts",
        "counterfactual_entry_ts": 1_700_000_003,
        "actionable_before_peak": not entry_after_peak,
        "earliest_actionable_reason": "legacy_actual_paper_entry_inputs_available_by_decision",
        "actionability_quality": "legacy_actual_paper_entry_window_proof",
    }
    return log.append_event(
        event_type="earliest_actionable_time_recorded",
        aggregate_id=f"earliest_actionable_time:solana:{token_ca}:{pool}:0:{paper_trade_id}",
        idempotency_key=f"earliest_actionable_time:{paper_trade_id}",
        payload=payload,
    )


def append_realtime_clean(log, *, token_ca, paper_trade_id=1, pool="pool-a", clean=True, missing_version=False):
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "quote_intent_id": paper_trade_id,
        "side": "buy",
        "size": 1.0,
        "route": "unit_route",
        "pool": pool,
        "quote_mint": "SOL",
        "slippage_bps": 25,
        "quote_source": "paper_trade_round_trip_quote",
        "quote_age_sec": 1.0 if clean else 0.0,
        "decision_available_at": 1_700_000_003,
        "entry_quote_available": True if clean else False,
        "entry_quote_available_at": 1_700_000_002,
        "entry_quote_price": 0.001,
        "exit_quote_available": True if clean else False,
        "exit_quote_available_at": 1_700_000_004,
        "exit_quote_price": 0.0012,
        "clean_standard_version": None if missing_version else "legacy_round_trip_quote_clean_v0.1",
        "clean_observation_type": "TRADABLE_CLEAN_OBSERVED" if clean else "QUOTE_DIRTY_OBSERVED",
        "realtime_clean": clean,
        "realtime_clean_detector_version": None if missing_version else "legacy_round_trip_quote_clean_v0.1",
        "used_future_peak": False,
        "used_future_outcome": False,
        "used_posthoc_label": False,
        "forbidden_future_fields_used": [],
    }
    return log.append_event(
        event_type="realtime_clean_detector_recorded",
        aggregate_id=f"realtime_clean:solana:{token_ca}:{pool}:0:{paper_trade_id}",
        idempotency_key=f"realtime_clean_detector:{paper_trade_id}:legacy_round_trip_quote_clean_v0.1",
        payload=payload,
    )


def append_quote_intent_binding(log, *, token_ca, paper_trade_id=1, pool="pool-a", bound=True, missing_version=False):
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "binding_policy_version": None if missing_version else "legacy_paper_trade_quote_intent_binding_v0.1",
        "quote_intent_binding_version": None if missing_version else "legacy_paper_trade_quote_intent_binding_v0.1",
        "quote_intent_id": paper_trade_id,
        "side": "buy",
        "size": 0.01,
        "route": "unit_route",
        "pool": pool,
        "quote_mint": "SOL",
        "slippage_bps": 25,
        "quote_ts": 1_700_000_002,
        "quote_source": "paper_trade_entry_quote_or_legacy_proxy",
        "quote_binding_proof_level": "entry_execution_audit",
        "quote_intent_binding_quality": "entry_execution_audit_bound",
        "quote_intent_bound": bound,
        "intent_hash": "intent-hash",
        "quote_hash": "quote-hash",
        "quote_binding_hash": "binding-hash",
        "missing_fields": [] if not missing_version else ["binding_policy_version"],
        "mismatch_fields": [] if bound else ["size"],
        "used_future_peak": False,
        "used_future_outcome": False,
        "used_posthoc_label": False,
        "forbidden_future_fields_used": [],
    }
    return log.append_event(
        event_type="quote_intent_binding_recorded",
        aggregate_id=f"quote_intent_binding:solana:{token_ca}:{pool}:0:{paper_trade_id}",
        idempotency_key=f"quote_intent_binding:{paper_trade_id}:legacy_paper_trade_quote_intent_binding_v0.1",
        payload=payload,
    )


def append_raw_provider_evidence(
    log,
    *,
    token_ca,
    paper_trade_id=1,
    pool="pool-a",
    trusted=True,
    missing_version=False,
    payload_overrides=None,
):
    request_metadata = {
        "paper_trade_id": paper_trade_id,
        "side": "entry",
        "provider": "jupiter_ultra",
        "endpoint": "/ultra/v1/order",
        "request_id": f"provider-request-{paper_trade_id}",
        "request_parameters": {
            "input_mint": "SOL",
            "output_mint": token_ca,
            "input_amount": 0.01,
            "slippage_bps": 25,
        },
    }
    raw_response = {
        "requestId": f"provider-request-{paper_trade_id}",
        "transaction": "base64-tx",
        "outAmount": "1000000",
    }
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "raw_provider_evidence_version": None if missing_version else "legacy_paper_raw_provider_evidence_v0.1",
        "provider_evidence_version": None if missing_version else "legacy_paper_raw_provider_evidence_v0.1",
        "provider": "jupiter_ultra",
        "endpoint": "/ultra/v1/order",
        "request_id": f"provider-request-{paper_trade_id}",
        "provider_request_id": f"provider-request-{paper_trade_id}",
        "side": "entry",
        "latency_ms": 123,
        "request_parameters": request_metadata["request_parameters"],
        "request_metadata": request_metadata,
        "request_metadata_available": True,
        "request_metadata_hash": sha256_hex(request_metadata),
        "request_hash": sha256_hex(request_metadata),
        "response_hash": sha256_hex(raw_response),
        "raw_response_hash": sha256_hex(raw_response) if trusted else None,
        "raw_response_available": trusted,
        "response_material_type": "execution._rawOrder" if trusted else "execution_audit_projection",
        "hash_algorithm": "sha256(canonical_json)",
        "evidence_source": "unit",
        "provider_evidence_proof_level": "provider_request_id_with_raw_response_hash" if trusted else "legacy_execution_projection_without_raw_provider_response",
        "provider_evidence_trusted": trusted,
        "decision_available_at": "2026-01-15T00:00:02Z",
    }
    if payload_overrides:
        payload.update(payload_overrides)
    return log.append_event(
        event_type="raw_provider_evidence_recorded",
        aggregate_id=f"raw_provider_evidence:solana:{token_ca}:{pool}:0:{paper_trade_id}:entry",
        idempotency_key=f"raw_provider_evidence:{paper_trade_id}:entry:legacy_paper_raw_provider_evidence_v0.1",
        payload=payload,
    )


def append_fee_schedule(log, *, provider="jupiter_ultra", chain="solana", fee_version="fee-v1", bad_hash=False):
    payload = {
        "fee_source_id": "fee-source-jupiter-ultra",
        "provider": provider,
        "chain": chain,
        "fee_version": fee_version,
        "source_hash": "not-a-sha" if bad_hash else sha256_hex({"provider": provider, "fee_version": fee_version}),
        "fee_model_hash": sha256_hex({"fee_model": "unit", "fee_version": fee_version}),
        "effective_at": "2026-01-15T00:00:00Z",
        "supersedes_version": "none",
        "checked_at": "2026-01-15T00:00:01Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="fee_schedule_recorded",
        aggregate_id=f"fee_schedule:{provider}:{chain}:{fee_version}",
        idempotency_key=f"fee_schedule:{provider}:{chain}:{fee_version}",
        payload=payload,
    )


def append_provider_credential_scope(log, *, credential_id="jupiter-ultra-unit-token", provider="jupiter_ultra", revoked=False):
    payload = {
        "credential_id": credential_id,
        "provider": provider,
        "allowed_endpoints": ["/ultra/v1/order", "/ultra/v1/execute"],
        "allowed_modes": ["paper", "normal_tiny"],
        "expires_at": "2099-01-01T00:00:00Z",
        "credential_status": "revoked" if revoked else "active",
        "checked_at": "2026-01-15T00:00:01Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="provider_credential_scope_recorded",
        aggregate_id=f"provider_credential_scope:{provider}:{credential_id}",
        idempotency_key=f"provider_credential_scope:{provider}:{credential_id}",
        payload=payload,
    )


def append_provider_request_replay(log, *, token_ca, paper_trade_id=1, pool="pool-a", bad_hash=False):
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "request_id": f"provider-request-{paper_trade_id}",
        "provider": "jupiter_ultra",
        "request_hash": "not-a-sha" if bad_hash else sha256_hex({"request_id": f"provider-request-{paper_trade_id}"}),
        "retry_count": 0,
        "decision_reason": "initial_attempt_no_replay_needed",
        "replay_status": "not_replayed_no_retry",
        "checked_at": "2026-01-15T00:00:02Z",
    }
    return log.append_event(
        event_type="provider_request_replay_recorded",
        aggregate_id=f"provider_request_replay:solana:{token_ca}:{pool}:0:{paper_trade_id}",
        idempotency_key=f"provider_request_replay:{paper_trade_id}",
        payload=payload,
    )


def append_provider_response_authenticity(log, *, token_ca, paper_trade_id=1, pool="pool-a", signature_status="verified"):
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "response_id": f"provider-response-{paper_trade_id}",
        "provider": "jupiter_ultra",
        "signature_status": signature_status,
        "transport_security": "tls_verified",
        "verified_at": "2026-01-15T00:00:03Z",
        "response_hash": sha256_hex({"response_id": f"provider-response-{paper_trade_id}"}),
    }
    return log.append_event(
        event_type="provider_response_authenticity_recorded",
        aggregate_id=f"provider_response_authenticity:solana:{token_ca}:{pool}:0:{paper_trade_id}",
        idempotency_key=f"provider_response_authenticity:{paper_trade_id}",
        payload=payload,
    )


def append_risk_revalidation_after_entry(log, *, token_ca, paper_trade_id=1, pool="pool-a", risk_status="clean", exit_safety_action="hold"):
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "position_id": f"paper_trade:{paper_trade_id}:position",
        "risk_event_id": f"risk-event-{paper_trade_id}",
        "risk_status": risk_status,
        "exit_safety_action": exit_safety_action,
        "revalidated_at": "2026-01-15T00:00:04Z",
    }
    return log.append_event(
        event_type="risk_revalidation_after_entry_recorded",
        aggregate_id=f"risk_revalidation_after_entry:solana:{token_ca}:{pool}:0:{paper_trade_id}",
        idempotency_key=f"risk_revalidation_after_entry:{paper_trade_id}",
        payload=payload,
    )


def append_randomness_control(log, *, assignment_id="normal-tiny-policy-v1", missing_seed=False, bad_hash=False, idempotency_suffix=None):
    payload = {
        "rng_seed": None if missing_seed else "sha256:normal-tiny-policy-seed",
        "rng_version": "v2.7.0.randomness_control.v1",
        "randomization_unit": "normal_tiny_promotion_policy",
        "assignment_id": assignment_id,
        "assignment_status": "deterministic_policy",
        "randomization_enabled": False,
        "deterministic_assignment": True,
        "assignment_algorithm": "deterministic_no_randomized_assignment",
        "assigned_bucket": "normal_tiny_candidate",
        "assignment_hash": "not-a-sha" if bad_hash else sha256_hex({"assignment_id": assignment_id}),
        "evidence_source": "unit",
        "decision_available_at": "2026-01-15T00:00:00Z",
    }
    return log.append_event(
        event_type="randomness_control_recorded",
        aggregate_id=f"randomness_control:{assignment_id}",
        idempotency_key=f"randomness_control:{assignment_id}:v2.7.0.randomness_control.v1{':' + idempotency_suffix if idempotency_suffix else ''}",
        payload=payload,
    )


def append_deployment_rollout(log, *, rollout_id="rollout-unit-v1", state="completed", canary_status="passed", fleet_hash_map=None):
    payload = {
        "rollout_id": rollout_id,
        "state": state,
        "fleet_hash_map": fleet_hash_map if fleet_hash_map is not None else {
            "dashboard": "build-a",
            "v27-read-model-refresh": "build-a",
        },
        "canary_status": canary_status,
        "build_hash": "build-a",
        "runtime_config_hash": "config-a",
        "policy_bundle_id": "policy-a",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="deployment_rollout_state_recorded",
        aggregate_id=f"deployment_rollout:{rollout_id}",
        idempotency_key=f"deployment_rollout:{rollout_id}:{state}:{canary_status}",
        payload=payload,
    )


def append_worker_fleet_heartbeat(log, *, worker_id="dashboard", build_hash="build-a", runtime_config_hash="config-a", policy_bundle_id="policy-a"):
    payload = {
        "worker_id": worker_id,
        "role": worker_id,
        "build_hash": build_hash,
        "runtime_config_hash": runtime_config_hash,
        "policy_bundle_id": policy_bundle_id,
        "heartbeat_at": "2026-01-15T00:00:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="worker_fleet_heartbeat_recorded",
        aggregate_id=f"worker_fleet:{worker_id}",
        idempotency_key=f"worker_fleet:{worker_id}:{build_hash}:{runtime_config_hash}:{policy_bundle_id}",
        payload=payload,
    )


def append_backup_restore_drill(
    log,
    *,
    drill_id="restore-drill-unit-v1",
    restored_world_hash=None,
    restore_started_at="2026-01-15T00:00:00Z",
    restore_completed_at="2026-01-15T00:01:00Z",
    restore_status="passed",
):
    payload = {
        "drill_id": drill_id,
        "backup_set_id": "backup-set-unit-v1",
        "restored_world_hash": restored_world_hash or sha256_hex({"world": "restored", "drill_id": drill_id}),
        "restore_started_at": restore_started_at,
        "restore_completed_at": restore_completed_at,
        "restore_status": restore_status,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="backup_restore_drill_recorded",
        aggregate_id=f"backup_restore_drill:{drill_id}",
        idempotency_key=f"backup_restore_drill:{drill_id}:{payload['backup_set_id']}",
        payload=payload,
    )


def append_incident_evidence_freeze(
    log,
    *,
    freeze_id="freeze-unit-v1",
    incident_id="incident-unit-v1",
    frozen_event_range=None,
    frozen_config_hash=None,
    frozen_at="2026-01-15T00:02:00Z",
    freeze_status="frozen",
):
    payload = {
        "freeze_id": freeze_id,
        "incident_id": incident_id,
        "frozen_event_range": frozen_event_range if frozen_event_range is not None else {"start_seq": 1, "end_seq": 42},
        "frozen_config_hash": frozen_config_hash or sha256_hex({"config": "frozen", "incident_id": incident_id}),
        "frozen_at": frozen_at,
        "freeze_status": freeze_status,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="incident_evidence_freeze_recorded",
        aggregate_id=f"incident_evidence_freeze:{freeze_id}",
        idempotency_key=f"incident_evidence_freeze:{freeze_id}:{incident_id}",
        payload=payload,
    )


def append_circuit_breaker_resume(
    log,
    *,
    breaker_id="breaker-unit-v1",
    evidence_freeze_id="freeze-unit-v1",
    root_cause_fixed=True,
    health_checks_passed=True,
    resumed_at="2026-01-15T00:03:00Z",
    resume_status="resumed",
):
    payload = {
        "breaker_id": breaker_id,
        "root_cause_fixed": root_cause_fixed,
        "evidence_freeze_id": evidence_freeze_id,
        "health_checks_passed": health_checks_passed,
        "resumed_at": resumed_at,
        "resume_status": resume_status,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="circuit_breaker_resume_recorded",
        aggregate_id=f"circuit_breaker_resume:{breaker_id}",
        idempotency_key=f"circuit_breaker_resume:{breaker_id}:{evidence_freeze_id}",
        payload=payload,
    )


def append_queue_durability(
    log,
    *,
    queue_id="entry-queue",
    task_id="task-unit-v1",
    durable_state="persisted",
    ack_state="acked",
    created_at="2026-01-15T00:04:00Z",
):
    payload = {
        "queue_id": queue_id,
        "task_id": task_id,
        "durable_state": durable_state,
        "ack_state": ack_state,
        "created_at": created_at,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="queue_durability_recorded",
        aggregate_id=f"queue_durability:{queue_id}:{task_id}",
        idempotency_key=f"queue_durability:{queue_id}:{task_id}:{durable_state}:{ack_state}",
        payload=payload,
    )


def append_candidate_cancellation(
    log,
    *,
    candidate_id="candidate-unit-v1",
    cancel_reason="risk_revalidated",
    cancel_event_seq=42,
    cancelled_at="2026-01-15T00:05:00Z",
):
    payload = {
        "candidate_id": candidate_id,
        "cancel_reason": cancel_reason,
        "cancel_event_seq": cancel_event_seq,
        "cancelled_at": cancelled_at,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="candidate_cancellation_recorded",
        aggregate_id=f"candidate_cancellation:{candidate_id}",
        idempotency_key=f"candidate_cancellation:{candidate_id}:{cancel_event_seq}",
        payload=payload,
    )


def append_retry_storm_control(
    log,
    *,
    retry_family="provider_quote",
    backoff_policy="capped_exponential_jitter",
    max_concurrent_retries=2,
    p0_reserved_capacity=1,
):
    payload = {
        "retry_family": retry_family,
        "backoff_policy": backoff_policy,
        "max_concurrent_retries": max_concurrent_retries,
        "p0_reserved_capacity": p0_reserved_capacity,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="retry_storm_control_recorded",
        aggregate_id=f"retry_storm_control:{retry_family}",
        idempotency_key=f"retry_storm_control:{retry_family}:{backoff_policy}",
        payload=payload,
    )


def append_provider_coverage_map(
    log,
    *,
    provider="jupiter_ultra",
    chain="solana",
    pool_type="raydium_amm",
    coverage_status="supported",
    unsupported_reason="none",
    checked_at="2026-01-15T00:06:00Z",
):
    payload = {
        "provider": provider,
        "chain": chain,
        "pool_type": pool_type,
        "coverage_status": coverage_status,
        "unsupported_reason": unsupported_reason,
        "coverage_map_version": "v2.7.0.provider_coverage_map.v1",
        "checked_at": checked_at,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="provider_coverage_map_recorded",
        aggregate_id=f"provider_coverage_map:{provider}:{chain}:{pool_type}",
        idempotency_key=f"provider_coverage_map:{provider}:{chain}:{pool_type}:{coverage_status}",
        payload=payload,
    )


def append_training_serving_skew(
    log,
    *,
    feature_set_id="normal-tiny-features-v1",
    normalization_version="norm-v1",
    training_feature_code_hash=None,
    serving_feature_code_hash=None,
    skew_check_result="pass",
    checked_at="2026-01-15T00:07:00Z",
):
    training_feature_code_hash = training_feature_code_hash or sha256_hex({"feature_code": "training", "version": "v1"})
    serving_feature_code_hash = serving_feature_code_hash or sha256_hex({"feature_code": "serving", "version": "v1"})
    payload = {
        "feature_set_id": feature_set_id,
        "training_feature_code_hash": training_feature_code_hash,
        "serving_feature_code_hash": serving_feature_code_hash,
        "normalization_version": normalization_version,
        "skew_check_result": skew_check_result,
        "checked_at": checked_at,
        "training_artifact_id": "training-artifact-unit-v1",
        "serving_artifact_id": "serving-artifact-unit-v1",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="training_serving_skew_recorded",
        aggregate_id=f"training_serving_skew:{feature_set_id}:{normalization_version}",
        idempotency_key=f"training_serving_skew:{feature_set_id}:{normalization_version}:{skew_check_result}",
        payload=payload,
    )


def append_provider_byzantine_quorum(
    log,
    *,
    quorum_id="provider-quorum-solana-entry",
    provider_set=None,
    selected_provider="jupiter_ultra",
    conflict_policy="fail_closed_on_conflict",
):
    payload = {
        "quorum_id": quorum_id,
        "provider_set": provider_set if provider_set is not None else ["jupiter_ultra", "gmgn_quote"],
        "conflict_policy": conflict_policy,
        "selected_provider": selected_provider,
        "quorum_size": 2,
        "agreement_metric": "entry_quote_price_within_tolerance",
        "checked_at": "2026-01-15T00:08:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="provider_byzantine_quorum_recorded",
        aggregate_id=f"provider_byzantine_quorum:{quorum_id}",
        idempotency_key=f"provider_byzantine_quorum:{quorum_id}:{selected_provider}",
        payload=payload,
    )


def append_provider_cache_poisoning_guard(
    log,
    *,
    cache_key="quote:solana:TokenReady:pool-a",
    provider="jupiter_ultra",
    poison_detected=False,
    quarantine_action="none",
    cache_validation_hash=None,
):
    payload = {
        "cache_key": cache_key,
        "provider": provider,
        "poison_detected": poison_detected,
        "quarantine_action": quarantine_action,
        "cache_validation_hash": cache_validation_hash or sha256_hex({"cache_key": cache_key, "provider": provider}),
        "checked_at": "2026-01-15T00:09:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="provider_cache_poisoning_guard_recorded",
        aggregate_id=f"provider_cache_poisoning_guard:{provider}:{cache_key}",
        idempotency_key=f"provider_cache_poisoning_guard:{provider}:{cache_key}:{quarantine_action}",
        payload=payload,
    )


def append_external_dependency(
    log,
    *,
    dependency_name="jupiter_ultra_quote",
    health_status="healthy",
    fallback_mode="fail_closed",
    fail_closed_action="block_entry",
):
    payload = {
        "dependency_name": dependency_name,
        "health_status": health_status,
        "fallback_mode": fallback_mode,
        "fail_closed_action": fail_closed_action,
        "checked_at": "2026-01-15T00:10:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="external_dependency_health_recorded",
        aggregate_id=f"external_dependency:{dependency_name}",
        idempotency_key=f"external_dependency:{dependency_name}:{health_status}",
        payload=payload,
    )


def append_third_party_status_correlation(
    log,
    *,
    dependency_name="jupiter_ultra_quote",
    status_source="jupiter_status_page",
    incident_id="none",
    correlation_result="no_incident",
):
    payload = {
        "dependency_name": dependency_name,
        "status_source": status_source,
        "incident_id": incident_id,
        "correlation_result": correlation_result,
        "checked_at": "2026-01-15T00:11:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="third_party_status_correlation_recorded",
        aggregate_id=f"third_party_status_correlation:{dependency_name}:{status_source}:{incident_id}",
        idempotency_key=f"third_party_status_correlation:{dependency_name}:{status_source}:{incident_id}:{correlation_result}",
        payload=payload,
    )


def append_resource_exhaustion(
    log,
    *,
    resource_type="provider_quote_pool",
    pressure_level="normal",
    pressure_action="observe",
    safety_budget_remaining=10,
):
    payload = {
        "resource_type": resource_type,
        "pressure_level": pressure_level,
        "pressure_action": pressure_action,
        "safety_budget_remaining": safety_budget_remaining,
        "checked_at": "2026-01-15T00:12:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="resource_exhaustion_recorded",
        aggregate_id=f"resource_exhaustion:{resource_type}",
        idempotency_key=f"resource_exhaustion:{resource_type}:{pressure_level}:{pressure_action}",
        payload=payload,
    )


def append_config_distribution(
    log,
    *,
    config_id="config-unit-v1",
    target_workers=None,
    config_hash=None,
    ack_policy="all_workers_before_effective_at",
):
    config_hash = config_hash or sha256_hex({"config_id": config_id, "version": "v1"})
    payload = {
        "config_id": config_id,
        "config_hash": config_hash,
        "target_workers": target_workers if target_workers is not None else ["dashboard", "v27-read-model-refresh"],
        "effective_at": "2026-01-15T00:13:00Z",
        "ack_policy": ack_policy,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="config_distribution_recorded",
        aggregate_id=f"config_distribution:{config_id}",
        idempotency_key=f"config_distribution:{config_id}:{ack_policy}",
        payload=payload,
    )


def append_config_distribution_ack(
    log,
    *,
    config_id="config-unit-v1",
    worker_id="dashboard",
    config_hash=None,
    ack_state="acked",
):
    config_hash = config_hash or sha256_hex({"config_id": config_id, "version": "v1"})
    payload = {
        "config_id": config_id,
        "worker_id": worker_id,
        "config_hash": config_hash,
        "ack_state": ack_state,
        "acked_at": "2026-01-15T00:13:10Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="config_distribution_ack_recorded",
        aggregate_id=f"config_distribution_ack:{config_id}:{worker_id}",
        idempotency_key=f"config_distribution_ack:{config_id}:{worker_id}:{ack_state}",
        payload=payload,
    )


def append_in_flight_config_rotation(
    log,
    *,
    rotation_id="rotation-unit-v1",
    old_config_hash=None,
    new_config_hash=None,
    affected_workers=None,
    rotation_policy="drain_then_cutover",
):
    old_config_hash = old_config_hash or sha256_hex({"config_id": "old", "version": "v1"})
    new_config_hash = new_config_hash or sha256_hex({"config_id": "new", "version": "v2"})
    payload = {
        "rotation_id": rotation_id,
        "old_config_hash": old_config_hash,
        "new_config_hash": new_config_hash,
        "affected_workers": affected_workers if affected_workers is not None else ["dashboard", "v27-read-model-refresh"],
        "safe_cutover_at": "2026-01-15T00:14:00Z",
        "rotation_policy": rotation_policy,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="in_flight_config_rotation_recorded",
        aggregate_id=f"in_flight_config_rotation:{rotation_id}",
        idempotency_key=f"in_flight_config_rotation:{rotation_id}:{rotation_policy}",
        payload=payload,
    )


def append_policy_activation_barrier(
    log,
    *,
    policy_bundle_id="policy-unit-v1",
    activation_epoch=1,
    required_worker_ack_count=2,
    observed_worker_ack_count=2,
):
    payload = {
        "policy_bundle_id": policy_bundle_id,
        "activation_epoch": activation_epoch,
        "required_worker_ack_count": required_worker_ack_count,
        "observed_worker_ack_count": observed_worker_ack_count,
        "activated_at": "2026-01-15T00:15:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="policy_activation_barrier_recorded",
        aggregate_id=f"policy_activation_barrier:{policy_bundle_id}:{activation_epoch}",
        idempotency_key=f"policy_activation_barrier:{policy_bundle_id}:{activation_epoch}:{observed_worker_ack_count}",
        payload=payload,
    )


def append_retry_policy_catalog(
    log,
    *,
    retry_family="provider_quote",
    backoff_policy="capped_exponential_jitter",
    max_attempts=3,
    jitter_policy="full_jitter",
    owner="runtime",
):
    payload = {
        "retry_family": retry_family,
        "backoff_policy": backoff_policy,
        "max_attempts": max_attempts,
        "jitter_policy": jitter_policy,
        "owner": owner,
        "checked_at": "2026-01-15T00:16:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="retry_policy_catalog_recorded",
        aggregate_id=f"retry_policy_catalog:{retry_family}",
        idempotency_key=f"retry_policy_catalog:{retry_family}:{backoff_policy}:{max_attempts}",
        payload=payload,
    )


def append_alert_noise_budget(
    log,
    *,
    alert_family="provider_quote_health",
    window_id="alerts-2026-01-15T00",
    noise_budget=5,
    suppression_count=1,
    owner="runtime",
):
    payload = {
        "alert_family": alert_family,
        "window_id": window_id,
        "noise_budget": noise_budget,
        "suppression_count": suppression_count,
        "owner": owner,
        "checked_at": "2026-01-15T00:17:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="alert_noise_budget_recorded",
        aggregate_id=f"alert_noise_budget:{alert_family}:{window_id}",
        idempotency_key=f"alert_noise_budget:{alert_family}:{window_id}",
        payload=payload,
    )


def append_alert_suppression_audit(
    log,
    *,
    suppression_id="suppression-unit-v1",
    alert_family="provider_quote_health",
    suppression_reason="deduplicated_noisy_probe",
    expires_at="2026-01-15T01:17:00Z",
    audit_event_id="audit-event-unit-v1",
):
    payload = {
        "suppression_id": suppression_id,
        "alert_family": alert_family,
        "suppression_reason": suppression_reason,
        "expires_at": expires_at,
        "audit_event_id": audit_event_id,
        "checked_at": "2026-01-15T00:17:10Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="alert_suppression_audit_recorded",
        aggregate_id=f"alert_suppression_audit:{suppression_id}",
        idempotency_key=f"alert_suppression_audit:{suppression_id}:{audit_event_id}",
        payload=payload,
    )


def append_canary_abort(
    log,
    *,
    canary_id="canary-unit-v1",
    abort_threshold=0.05,
    observed_metric=0.08,
    abort_action="rollback_release",
):
    payload = {
        "canary_id": canary_id,
        "abort_threshold": abort_threshold,
        "observed_metric": observed_metric,
        "abort_action": abort_action,
        "aborted_at": "2026-01-15T00:18:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="canary_abort_recorded",
        aggregate_id=f"canary_abort:{canary_id}",
        idempotency_key=f"canary_abort:{canary_id}:{abort_action}",
        payload=payload,
    )


def append_model_artifact_runtime_compatibility(
    log,
    *,
    model_snapshot_id="model-snapshot-unit-v1",
    runtime_version="runtime-v1",
    serialization_format="onnx",
    compatibility_result="compatible",
):
    payload = {
        "model_snapshot_id": model_snapshot_id,
        "runtime_version": runtime_version,
        "serialization_format": serialization_format,
        "compatibility_result": compatibility_result,
        "checked_at": "2026-01-15T00:19:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="model_artifact_runtime_compatibility_recorded",
        aggregate_id=f"model_artifact_runtime_compatibility:{model_snapshot_id}:{runtime_version}",
        idempotency_key=f"model_artifact_runtime_compatibility:{model_snapshot_id}:{runtime_version}",
        payload=payload,
    )


def append_model_rollback(
    log,
    *,
    rollback_id="model-rollback-unit-v1",
    from_model_snapshot_id="model-snapshot-unit-v2",
    to_model_snapshot_id="model-snapshot-unit-v1",
):
    payload = {
        "rollback_id": rollback_id,
        "from_model_snapshot_id": from_model_snapshot_id,
        "to_model_snapshot_id": to_model_snapshot_id,
        "rollback_verified_at": "2026-01-15T00:20:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="model_rollback_recorded",
        aggregate_id=f"model_rollback:{rollback_id}",
        idempotency_key=f"model_rollback:{rollback_id}:{to_model_snapshot_id}",
        payload=payload,
    )


def append_post_release_monitoring_window(
    log,
    *,
    release_id="release-unit-v1",
    window_start="2026-01-15T00:00:00Z",
    window_end="2026-01-15T01:00:00Z",
    monitored_metrics=None,
    exit_status="monitoring_passed",
):
    payload = {
        "release_id": release_id,
        "window_start": window_start,
        "window_end": window_end,
        "monitored_metrics": monitored_metrics if monitored_metrics is not None else ["error_rate", "capture_rate"],
        "exit_status": exit_status,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="post_release_monitoring_window_recorded",
        aggregate_id=f"post_release_monitoring_window:{release_id}",
        idempotency_key=f"post_release_monitoring_window:{release_id}:{exit_status}",
        payload=payload,
    )


def append_training_poisoning_guard(
    log,
    *,
    training_run_id="training-run-unit-v1",
    dataset_hash=None,
    poison_signal_count=0,
    quarantine_action="none",
):
    dataset_hash = dataset_hash or sha256_hex({"dataset": "unit", "version": "v1"})
    payload = {
        "training_run_id": training_run_id,
        "dataset_hash": dataset_hash,
        "poison_signal_count": poison_signal_count,
        "quarantine_action": quarantine_action,
        "checked_at": "2026-01-15T00:21:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="training_poisoning_guard_recorded",
        aggregate_id=f"training_poisoning_guard:{training_run_id}",
        idempotency_key=f"training_poisoning_guard:{training_run_id}:{poison_signal_count}:{quarantine_action}",
        payload=payload,
    )


def append_feature_store_consistency(
    log,
    *,
    feature_set_id="feature-set-unit-v1",
    normalization_version="norm-v1",
    offline_hash=None,
    online_hash=None,
):
    offline_hash = offline_hash or sha256_hex({"feature_set_id": feature_set_id, "normalization_version": normalization_version})
    online_hash = online_hash or offline_hash
    payload = {
        "feature_set_id": feature_set_id,
        "offline_hash": offline_hash,
        "online_hash": online_hash,
        "normalization_version": normalization_version,
        "checked_at": "2026-01-15T00:22:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="feature_store_consistency_recorded",
        aggregate_id=f"feature_store_consistency:{feature_set_id}:{normalization_version}",
        idempotency_key=f"feature_store_consistency:{feature_set_id}:{normalization_version}:{offline_hash}:{online_hash}",
        payload=payload,
    )


def append_dynamic_token_authority_change(
    log,
    *,
    token_ca="TokenReady",
    authority_type="freeze",
    previous_authority_hash=None,
    current_authority_hash=None,
    risk_action="risk_recheck",
):
    previous_authority_hash = previous_authority_hash or sha256_hex(
        {"token_ca": token_ca, "authority_type": authority_type, "authority": "previous"}
    )
    current_authority_hash = current_authority_hash or sha256_hex(
        {"token_ca": token_ca, "authority_type": authority_type, "authority": "current"}
    )
    payload = {
        "token_ca": token_ca,
        "authority_type": authority_type,
        "previous_authority_hash": previous_authority_hash,
        "current_authority_hash": current_authority_hash,
        "risk_action": risk_action,
        "checked_at": "2026-01-15T00:23:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="dynamic_token_authority_change_recorded",
        aggregate_id=f"dynamic_token_authority_change:{token_ca}:{authority_type}",
        idempotency_key=f"dynamic_token_authority_change:{token_ca}:{authority_type}:{risk_action}",
        payload=payload,
    )


def append_adversarial_execution_simulation(
    log,
    *,
    simulation_id="simulation-unit-v1",
    safety_result="blocked",
):
    payload = {
        "simulation_id": simulation_id,
        "execution_policy_version": "normal-tiny-execution-policy-v1",
        "attack_scenario": "quote_cache_poison_then_retry_storm",
        "safety_result": safety_result,
        "checked_at": "2026-01-15T00:24:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="adversarial_execution_simulation_recorded",
        aggregate_id=f"adversarial_execution_simulation:{simulation_id}",
        idempotency_key=f"adversarial_execution_simulation:{simulation_id}:{safety_result}",
        payload=payload,
    )


def append_open_position_valuation(
    log,
    *,
    position_id="position-unit-v1",
    valuation_price=0.001,
    valuation_hash=None,
):
    valuation_hash = valuation_hash or sha256_hex({"position_id": position_id, "valuation_price": valuation_price})
    payload = {
        "position_id": position_id,
        "valuation_ts": "2026-01-15T00:25:00Z",
        "quote_source": "jupiter_ultra",
        "valuation_price": valuation_price,
        "valuation_hash": valuation_hash,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="open_position_valuation_recorded",
        aggregate_id=f"open_position_valuation:{position_id}",
        idempotency_key=f"open_position_valuation:{position_id}:{valuation_price}",
        payload=payload,
    )


def append_exit_policy_migration(
    log,
    *,
    position_id="position-unit-v1",
    old_exit_policy="exit-policy-v1",
    new_exit_policy="exit-policy-v2",
):
    payload = {
        "position_id": position_id,
        "old_exit_policy": old_exit_policy,
        "new_exit_policy": new_exit_policy,
        "migration_reason": "tighten_dirty_quote_exit_guard",
        "migrated_at": "2026-01-15T00:26:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="exit_policy_migration_recorded",
        aggregate_id=f"exit_policy_migration:{position_id}",
        idempotency_key=f"exit_policy_migration:{position_id}:{old_exit_policy}:{new_exit_policy}",
        payload=payload,
    )


def append_open_position_policy_migration(
    log,
    *,
    position_id="position-unit-v2",
    old_exit_policy="exit-policy-v1",
    new_exit_policy="exit-policy-v2",
):
    payload = {
        "position_id": position_id,
        "old_exit_policy": old_exit_policy,
        "new_exit_policy": new_exit_policy,
        "migration_reason": "align_open_position_exit_policy",
        "checked_at": "2026-01-15T00:27:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="open_position_policy_migration_recorded",
        aggregate_id=f"open_position_policy_migration:{position_id}",
        idempotency_key=f"open_position_policy_migration:{position_id}:{old_exit_policy}:{new_exit_policy}",
        payload=payload,
    )


def append_position_ownership_transfer(
    log,
    *,
    position_id="position-unit-v1",
    from_owner="paper_executor",
    to_owner="risk_controller",
):
    payload = {
        "position_id": position_id,
        "from_owner": from_owner,
        "to_owner": to_owner,
        "transfer_reason": "risk_revalidation_requires_controller",
        "transferred_at": "2026-01-15T00:28:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="position_ownership_transfer_recorded",
        aggregate_id=f"position_ownership_transfer:{position_id}",
        idempotency_key=f"position_ownership_transfer:{position_id}:{from_owner}:{to_owner}",
        payload=payload,
    )


def append_rollback_verification(
    log,
    *,
    rollback_id="rollback-unit-v1",
    from_version="release-v2",
    to_version="release-v1",
    verified_at="2026-01-15T00:29:00Z",
):
    payload = {
        "rollback_id": rollback_id,
        "from_version": from_version,
        "to_version": to_version,
        "verified_at": verified_at,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="rollback_verification_recorded",
        aggregate_id=f"rollback_verification:{rollback_id}",
        idempotency_key=f"rollback_verification:{rollback_id}:{from_version}:{to_version}",
        payload=payload,
    )


def append_partial_rollback_policy(
    log,
    *,
    rollback_id="partial-rollback-unit-v1",
    component_scope="dashboard:v27-readiness",
    dependency_scope="read_model_refresh",
    verification_plan="health_check_and_scope_audit",
    rolled_back_at="2026-01-15T00:30:00Z",
):
    payload = {
        "rollback_id": rollback_id,
        "component_scope": component_scope,
        "dependency_scope": dependency_scope,
        "verification_plan": verification_plan,
        "rolled_back_at": rolled_back_at,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="partial_rollback_policy_recorded",
        aggregate_id=f"partial_rollback_policy:{rollback_id}",
        idempotency_key=f"partial_rollback_policy:{rollback_id}:{component_scope}",
        payload=payload,
    )


def append_release_readiness_review(
    log,
    *,
    review_id="release-review-unit-v1",
    release_id="release-unit-v2",
    required_evidence=None,
    approval_status="approved",
    approved_at="2026-01-15T00:31:00Z",
):
    payload = {
        "review_id": review_id,
        "release_id": release_id,
        "required_evidence": required_evidence if required_evidence is not None else ["health", "scope_audit", "pytest"],
        "approval_status": approval_status,
        "approved_at": approved_at,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="release_readiness_review_recorded",
        aggregate_id=f"release_readiness_review:{review_id}",
        idempotency_key=f"release_readiness_review:{review_id}:{approval_status}",
        payload=payload,
    )


def append_change_freeze(
    log,
    *,
    freeze_id="freeze-unit-v1",
    scope="normal_tiny_runtime",
    start_at="2026-01-15T00:32:00Z",
    end_at="2026-01-15T01:32:00Z",
    exception_policy="break_glass_only",
):
    payload = {
        "freeze_id": freeze_id,
        "scope": scope,
        "start_at": start_at,
        "end_at": end_at,
        "exception_policy": exception_policy,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="change_freeze_recorded",
        aggregate_id=f"change_freeze:{freeze_id}",
        idempotency_key=f"change_freeze:{freeze_id}:{scope}",
        payload=payload,
    )


def append_notification_channel_integrity(
    log,
    *,
    channel_id="ops-alerts-unit",
    destination_hash=None,
    signature_required=True,
    delivery_status="verified",
):
    destination_hash = destination_hash or sha256_hex({"channel_id": channel_id, "destination": "telegram_ops"})
    payload = {
        "channel_id": channel_id,
        "destination_hash": destination_hash,
        "signature_required": signature_required,
        "delivery_status": delivery_status,
        "checked_at": "2026-01-15T00:33:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="notification_channel_integrity_recorded",
        aggregate_id=f"notification_channel_integrity:{channel_id}",
        idempotency_key=f"notification_channel_integrity:{channel_id}:{delivery_status}",
        payload=payload,
    )


def append_runbook_freshness(
    log,
    *,
    runbook_id="normal-tiny-rollback-runbook",
    owner="runtime",
    last_reviewed_at="2026-01-15T00:34:00Z",
    max_age_days=30,
    freshness_status="fresh",
):
    payload = {
        "runbook_id": runbook_id,
        "owner": owner,
        "last_reviewed_at": last_reviewed_at,
        "max_age_days": max_age_days,
        "freshness_status": freshness_status,
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="runbook_freshness_recorded",
        aggregate_id=f"runbook_freshness:{runbook_id}",
        idempotency_key=f"runbook_freshness:{runbook_id}:{freshness_status}",
        payload=payload,
    )


def append_metric_backfill_impact(
    log,
    *,
    backfill_id="metric-backfill-unit-v1",
    metric_id="telegram_capture_rate_D3b",
    impact_scope="metric_window_only",
    impact_report_hash=None,
):
    impact_report_hash = impact_report_hash or sha256_hex({"backfill_id": backfill_id, "metric_id": metric_id})
    payload = {
        "backfill_id": backfill_id,
        "metric_id": metric_id,
        "impact_scope": impact_scope,
        "impact_report_hash": impact_report_hash,
        "checked_at": "2026-01-15T00:35:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="metric_backfill_impact_recorded",
        aggregate_id=f"metric_backfill_impact:{backfill_id}",
        idempotency_key=f"metric_backfill_impact:{backfill_id}:{impact_scope}",
        payload=payload,
    )


def append_selection_bias_diagnostic(
    log,
    *,
    diagnostic_id="selection-bias-unit-v1",
    selection_policy_version="normal-tiny-selection-v1",
    included_count=40,
    excluded_count=8,
    bias_result="within_tolerance",
):
    payload = {
        "diagnostic_id": diagnostic_id,
        "selection_policy_version": selection_policy_version,
        "included_count": included_count,
        "excluded_count": excluded_count,
        "bias_result": bias_result,
        "checked_at": "2026-01-15T00:36:00Z",
        "evidence_source": "unit",
    }
    return log.append_event(
        event_type="selection_bias_diagnostic_recorded",
        aggregate_id=f"selection_bias_diagnostic:{diagnostic_id}",
        idempotency_key=f"selection_bias_diagnostic:{diagnostic_id}:{bias_result}",
        payload=payload,
    )


def append_access_review(
    log,
    *,
    review_id="access-review-unit-v1",
    operator_id="operator-unit",
    scope="dashboard:admin_mutation",
    privilege_delta="reduced",
    reviewed_at="2026-01-15T00:37:00Z",
):
    payload = {
        "review_id": review_id,
        "operator_id": operator_id,
        "scope": scope,
        "privilege_delta": privilege_delta,
        "reviewed_at": reviewed_at,
    }
    return log.append_event(
        event_type="access_review_recorded",
        aggregate_id=f"access_review:{review_id}",
        idempotency_key=f"access_review:{review_id}:{operator_id}",
        payload=payload,
    )


def append_approval_workflow(
    log,
    *,
    approval_id="approval-unit-v1",
    mutation_id="mutation-unit-v1",
    required_approvers=None,
    approval_state="approved",
    approved_at="2026-01-15T00:38:00Z",
):
    payload = {
        "approval_id": approval_id,
        "mutation_id": mutation_id,
        "required_approvers": required_approvers if required_approvers is not None else ["runtime-owner"],
        "approval_state": approval_state,
        "approved_at": approved_at,
    }
    return log.append_event(
        event_type="approval_workflow_recorded",
        aggregate_id=f"approval_workflow:{approval_id}",
        idempotency_key=f"approval_workflow:{approval_id}:{mutation_id}",
        payload=payload,
    )


def append_break_glass_access(
    log,
    *,
    break_glass_id="break-glass-unit-v1",
    operator_id="operator-unit",
    reason="restore_paper_read_model",
    expires_at="2026-01-15T01:39:00Z",
    audit_event_id="audit-unit-v1",
):
    payload = {
        "break_glass_id": break_glass_id,
        "operator_id": operator_id,
        "reason": reason,
        "expires_at": expires_at,
        "audit_event_id": audit_event_id,
    }
    return log.append_event(
        event_type="break_glass_access_recorded",
        aggregate_id=f"break_glass_access:{break_glass_id}",
        idempotency_key=f"break_glass_access:{break_glass_id}:{audit_event_id}",
        payload=payload,
    )


def append_csv_spreadsheet_injection(
    log,
    *,
    export_id="export-unit-v1",
    column_name="symbol",
    unsafe_prefix_detected=True,
    sanitization_policy="escape_formula_prefix",
    checked_at="2026-01-15T00:40:00Z",
):
    payload = {
        "export_id": export_id,
        "column_name": column_name,
        "unsafe_prefix_detected": unsafe_prefix_detected,
        "sanitization_policy": sanitization_policy,
        "checked_at": checked_at,
    }
    return log.append_event(
        event_type="csv_spreadsheet_injection_recorded",
        aggregate_id=f"csv_spreadsheet_injection:{export_id}:{column_name}",
        idempotency_key=f"csv_spreadsheet_injection:{export_id}:{column_name}",
        payload=payload,
    )


def append_evidence_external_anchoring(
    log,
    *,
    anchor_id="anchor-unit-v1",
    anchored_hash=None,
    anchor_target="v27_denominator_projection",
    anchored_at="2026-01-15T00:41:00Z",
):
    payload = {
        "anchor_id": anchor_id,
        "anchored_hash": anchored_hash or sha256_hex({"anchor_id": anchor_id, "target": anchor_target}),
        "anchor_target": anchor_target,
        "anchored_at": anchored_at,
    }
    return log.append_event(
        event_type="evidence_external_anchoring_recorded",
        aggregate_id=f"evidence_external_anchoring:{anchor_id}",
        idempotency_key=f"evidence_external_anchoring:{anchor_id}",
        payload=payload,
    )


def append_experiment_assignment_immutability(
    log,
    *,
    assignment_id="assignment-unit-v1",
    randomization_unit="normal_tiny_promotion_policy",
    original_assignment_hash=None,
    attempted_change_hash=None,
    detected_at="2026-01-15T00:42:00Z",
):
    original_assignment_hash = original_assignment_hash or sha256_hex({"assignment_id": assignment_id, "arm": "control"})
    attempted_change_hash = attempted_change_hash or sha256_hex({"assignment_id": assignment_id, "arm": "treatment"})
    payload = {
        "assignment_id": assignment_id,
        "randomization_unit": randomization_unit,
        "original_assignment_hash": original_assignment_hash,
        "attempted_change_hash": attempted_change_hash,
        "detected_at": detected_at,
    }
    return log.append_event(
        event_type="experiment_assignment_immutability_recorded",
        aggregate_id=f"experiment_assignment_immutability:{assignment_id}",
        idempotency_key=f"experiment_assignment_immutability:{assignment_id}",
        payload=payload,
    )


def append_incident_postmortem(
    log,
    *,
    postmortem_id="postmortem-unit-v1",
    incident_id="incident-unit-v1",
    root_cause="read_model_refresh_regression",
    corrective_actions=None,
    approved_at="2026-01-15T00:43:00Z",
):
    payload = {
        "postmortem_id": postmortem_id,
        "incident_id": incident_id,
        "root_cause": root_cause,
        "corrective_actions": corrective_actions if corrective_actions is not None else ["add_scope_audit_regression"],
        "approved_at": approved_at,
    }
    return log.append_event(
        event_type="incident_postmortem_recorded",
        aggregate_id=f"incident_postmortem:{postmortem_id}",
        idempotency_key=f"incident_postmortem:{postmortem_id}:{incident_id}",
        payload=payload,
    )


def append_label_dispute_resolution(
    log,
    *,
    dispute_id="label-dispute-unit-v1",
    label_id="label-unit-v1",
    resolution_action="quarantine",
    resolved_at="2026-01-15T00:44:00Z",
):
    payload = {
        "dispute_id": dispute_id,
        "label_id": label_id,
        "resolution_action": resolution_action,
        "resolved_at": resolved_at,
    }
    return log.append_event(
        event_type="label_dispute_resolution_recorded",
        aggregate_id=f"label_dispute_resolution:{dispute_id}",
        idempotency_key=f"label_dispute_resolution:{dispute_id}:{resolution_action}",
        payload=payload,
    )


def append_negative_control(
    log,
    *,
    control_id="negative-control-unit-v1",
    control_group="holdout",
    expected_no_effect_metric=0.01,
    observed_effect=0.002,
    checked_at="2026-01-15T00:45:00Z",
):
    payload = {
        "control_id": control_id,
        "control_group": control_group,
        "expected_no_effect_metric": expected_no_effect_metric,
        "observed_effect": observed_effect,
        "checked_at": checked_at,
    }
    return log.append_event(
        event_type="negative_control_recorded",
        aggregate_id=f"negative_control:{control_id}",
        idempotency_key=f"negative_control:{control_id}:{observed_effect}",
        payload=payload,
    )


def append_operator_training_certification(
    log,
    *,
    operator_id="operator-unit",
    training_module="normal_tiny_runtime_ops",
    certification_status="certified",
    expires_at="2026-02-15T00:46:00Z",
    checked_at="2026-01-15T00:46:00Z",
):
    payload = {
        "operator_id": operator_id,
        "training_module": training_module,
        "certification_status": certification_status,
        "expires_at": expires_at,
        "checked_at": checked_at,
    }
    return log.append_event(
        event_type="operator_training_certification_recorded",
        aggregate_id=f"operator_training_certification:{operator_id}:{training_module}",
        idempotency_key=f"operator_training_certification:{operator_id}:{training_module}",
        payload=payload,
    )


def append_runtime_spec_assertion(
    log,
    *,
    assertion_id="runtime-spec-assertion-unit-v1",
    contract_id="RealtimeCleanDetector",
    runtime_location="scripts/paper_trade_monitor.py:realtime_clean_gate",
    failure_action="runtime_assert_failed",
):
    payload = {
        "assertion_id": assertion_id,
        "contract_id": contract_id,
        "runtime_location": runtime_location,
        "failure_action": failure_action,
    }
    return log.append_event(
        event_type="runtime_spec_assertion_recorded",
        aggregate_id=f"runtime_spec_assertion:{assertion_id}",
        idempotency_key=f"runtime_spec_assertion:{assertion_id}:{contract_id}",
        payload=payload,
    )


def append_minimum_viable_trust_boundary(
    log,
    *,
    boundary_id="minimum-viable-trust-unit-v1",
    trusted_inputs=None,
    untrusted_inputs=None,
    required_contracts=None,
    failure_action="mode_blocked",
):
    payload = {
        "boundary_id": boundary_id,
        "trusted_inputs": trusted_inputs if trusted_inputs is not None else ["entry_quote", "exit_quote"],
        "untrusted_inputs": untrusted_inputs if untrusted_inputs is not None else ["mark_only_peak", "posthoc_label"],
        "required_contracts": required_contracts if required_contracts is not None else [
            "RealtimeCleanDetector",
            "QuoteIntentBindingContract",
        ],
        "failure_action": failure_action,
    }
    return log.append_event(
        event_type="minimum_viable_trust_boundary_recorded",
        aggregate_id=f"minimum_viable_trust_boundary:{boundary_id}",
        idempotency_key=f"minimum_viable_trust_boundary:{boundary_id}",
        payload=payload,
    )


def append_evidence_conflict(
    log,
    *,
    conflict_id="evidence-conflict-unit-v1",
    evidence_a_hash=None,
    evidence_b_hash=None,
    resolution_policy="quarantine_then_operator_review",
    resolved_at="2026-01-15T00:47:00Z",
):
    evidence_a_hash = evidence_a_hash or sha256_hex({"evidence": "a", "conflict_id": conflict_id})
    evidence_b_hash = evidence_b_hash or sha256_hex({"evidence": "b", "conflict_id": conflict_id})
    payload = {
        "conflict_id": conflict_id,
        "evidence_a_hash": evidence_a_hash,
        "evidence_b_hash": evidence_b_hash,
        "resolution_policy": resolution_policy,
        "resolved_at": resolved_at,
    }
    return log.append_event(
        event_type="evidence_conflict_recorded",
        aggregate_id=f"evidence_conflict:{conflict_id}",
        idempotency_key=f"evidence_conflict:{conflict_id}",
        payload=payload,
    )


def append_evidence_aging(
    log,
    *,
    evidence_id="evidence-aging-unit-v1",
    evidence_type="quote_clean_snapshot",
    max_age_ms=120_000,
    age_ms=30_000,
    expiration_action="revalidate_before_entry",
):
    payload = {
        "evidence_id": evidence_id,
        "evidence_type": evidence_type,
        "max_age_ms": max_age_ms,
        "age_ms": age_ms,
        "expiration_action": expiration_action,
    }
    return log.append_event(
        event_type="evidence_aging_recorded",
        aggregate_id=f"evidence_aging:{evidence_id}",
        idempotency_key=f"evidence_aging:{evidence_id}",
        payload=payload,
    )


def append_market_regime_invalidates_evidence(
    log,
    *,
    regime_id="market-regime-unit-v1",
    evidence_id="evidence-aging-unit-v1",
    invalidating_signal="liquidity_regime_flip",
    action="revalidate_evidence",
    detected_at="2026-01-15T00:48:00Z",
):
    payload = {
        "regime_id": regime_id,
        "evidence_id": evidence_id,
        "invalidating_signal": invalidating_signal,
        "action": action,
        "detected_at": detected_at,
    }
    return log.append_event(
        event_type="market_regime_invalidates_evidence_recorded",
        aggregate_id=f"market_regime_invalidates_evidence:{regime_id}:{evidence_id}",
        idempotency_key=f"market_regime_invalidates_evidence:{regime_id}:{evidence_id}",
        payload=payload,
    )


def append_source_alpha_decay_exit_criteria(
    log,
    *,
    source_id="premium-clean-source-unit-v1",
    alpha_metric=0.12,
    decay_window="24h",
    exit_threshold=0.05,
    action="keep_source",
):
    payload = {
        "source_id": source_id,
        "alpha_metric": alpha_metric,
        "decay_window": decay_window,
        "exit_threshold": exit_threshold,
        "action": action,
    }
    return log.append_event(
        event_type="source_alpha_decay_exit_criteria_recorded",
        aggregate_id=f"source_alpha_decay_exit_criteria:{source_id}",
        idempotency_key=f"source_alpha_decay_exit_criteria:{source_id}:{decay_window}",
        payload=payload,
    )


def append_false_negative_budget(
    log,
    *,
    budget_id="false-negative-budget-unit-v1",
    hazard_class="missed_clean_gold_dog",
    allowed_false_negative_rate=0.15,
    observed_rate=0.08,
    action="continue_with_watch",
):
    payload = {
        "budget_id": budget_id,
        "hazard_class": hazard_class,
        "allowed_false_negative_rate": allowed_false_negative_rate,
        "observed_rate": observed_rate,
        "action": action,
    }
    return log.append_event(
        event_type="false_negative_budget_recorded",
        aggregate_id=f"false_negative_budget:{budget_id}",
        idempotency_key=f"false_negative_budget:{budget_id}:{observed_rate}",
        payload=payload,
    )


def append_small_sample_decision(
    log,
    *,
    policy_id="small-sample-policy-unit-v1",
    sample_size=40,
    min_sample_size=30,
    decision_allowed=True,
    fallback_action="hold_promotion",
):
    payload = {
        "policy_id": policy_id,
        "sample_size": sample_size,
        "min_sample_size": min_sample_size,
        "decision_allowed": decision_allowed,
        "fallback_action": fallback_action,
    }
    return log.append_event(
        event_type="small_sample_decision_recorded",
        aggregate_id=f"small_sample_decision:{policy_id}",
        idempotency_key=f"small_sample_decision:{policy_id}:{sample_size}",
        payload=payload,
    )


def append_safety_vs_capture_tradeoff(
    log,
    *,
    tradeoff_id="safety-capture-tradeoff-unit-v1",
    safety_metric=0.98,
    capture_metric=0.62,
    chosen_policy="safety_first_capture_watch",
    approved_at="2026-01-15T00:49:00Z",
):
    payload = {
        "tradeoff_id": tradeoff_id,
        "safety_metric": safety_metric,
        "capture_metric": capture_metric,
        "chosen_policy": chosen_policy,
        "approved_at": approved_at,
    }
    return log.append_event(
        event_type="safety_vs_capture_tradeoff_recorded",
        aggregate_id=f"safety_vs_capture_tradeoff:{tradeoff_id}",
        idempotency_key=f"safety_vs_capture_tradeoff:{tradeoff_id}",
        payload=payload,
    )


def append_implementation_drift_monitor(
    log,
    *,
    drift_id="implementation-drift-unit-v1",
    spec_contract_id="RealtimeCleanDetector",
    runtime_location="scripts/paper_trade_monitor.py:realtime_clean_gate",
    drift_detected=False,
    detected_at="2026-01-15T00:50:00Z",
):
    payload = {
        "drift_id": drift_id,
        "spec_contract_id": spec_contract_id,
        "runtime_location": runtime_location,
        "drift_detected": drift_detected,
        "detected_at": detected_at,
    }
    return log.append_event(
        event_type="implementation_drift_monitor_recorded",
        aggregate_id=f"implementation_drift_monitor:{drift_id}",
        idempotency_key=f"implementation_drift_monitor:{drift_id}:{spec_contract_id}",
        payload=payload,
    )


def append_assumption_registry(
    log,
    *,
    assumption_id="assumption-unit-v1",
    scope="normal_tiny_capture_metrics",
    owner="runtime-owner",
    evidence_link="v27_denominator_projection:runtime_trust",
    expires_at="2026-02-15T00:51:00Z",
):
    payload = {
        "assumption_id": assumption_id,
        "scope": scope,
        "owner": owner,
        "evidence_link": evidence_link,
        "expires_at": expires_at,
    }
    return log.append_event(
        event_type="assumption_registry_recorded",
        aggregate_id=f"assumption_registry:{assumption_id}",
        idempotency_key=f"assumption_registry:{assumption_id}:{scope}",
        payload=payload,
    )


def append_assumption_invalidation_trigger(
    log,
    *,
    assumption_id="assumption-unit-v1",
    trigger_metric="missed_clean_gold_false_negative_rate",
    threshold=0.15,
    observed_value=0.21,
    invalidated_at="2026-01-15T00:52:00Z",
):
    payload = {
        "assumption_id": assumption_id,
        "trigger_metric": trigger_metric,
        "threshold": threshold,
        "observed_value": observed_value,
        "invalidated_at": invalidated_at,
    }
    return log.append_event(
        event_type="assumption_invalidation_trigger_recorded",
        aggregate_id=f"assumption_invalidation_trigger:{assumption_id}:{trigger_metric}",
        idempotency_key=f"assumption_invalidation_trigger:{assumption_id}:{trigger_metric}",
        payload=payload,
    )


def append_contract_priority_graph(
    log,
    *,
    graph_id="contract-priority-graph-unit-v1",
    higher_priority_contract="SafetyVsCaptureTradeoffContract",
    lower_priority_contract="SourceAlphaDecayExitCriteria",
    cycle_detected=False,
    resolved_at="2026-01-15T00:53:00Z",
):
    payload = {
        "graph_id": graph_id,
        "higher_priority_contract": higher_priority_contract,
        "lower_priority_contract": lower_priority_contract,
        "cycle_detected": cycle_detected,
        "resolved_at": resolved_at,
    }
    return log.append_event(
        event_type="contract_priority_graph_recorded",
        aggregate_id=f"contract_priority_graph:{graph_id}",
        idempotency_key=f"contract_priority_graph:{graph_id}:{higher_priority_contract}:{lower_priority_contract}",
        payload=payload,
    )


def append_contract_conflict_resolution(
    log,
    *,
    conflict_id="contract-conflict-unit-v1",
    higher_priority_contract="SafetyVsCaptureTradeoffContract",
    lower_priority_contract="SourceAlphaDecayExitCriteria",
    resolution_action="apply_higher_priority_contract",
):
    payload = {
        "conflict_id": conflict_id,
        "higher_priority_contract": higher_priority_contract,
        "lower_priority_contract": lower_priority_contract,
        "resolution_action": resolution_action,
    }
    return log.append_event(
        event_type="contract_conflict_resolution_recorded",
        aggregate_id=f"contract_conflict_resolution:{conflict_id}",
        idempotency_key=f"contract_conflict_resolution:{conflict_id}:{resolution_action}",
        payload=payload,
    )


def append_contract_failure_blast_radius(
    log,
    *,
    contract_id="RealtimeCleanDetector",
    blast_radius="normal_tiny_entry_block",
    affected_modes=None,
    fallback_action="block_entry_and_hold_shadow",
    reviewed_at="2026-01-15T00:54:00Z",
):
    payload = {
        "contract_id": contract_id,
        "blast_radius": blast_radius,
        "affected_modes": affected_modes if affected_modes is not None else ["normal_tiny"],
        "fallback_action": fallback_action,
        "reviewed_at": reviewed_at,
    }
    return log.append_event(
        event_type="contract_failure_blast_radius_recorded",
        aggregate_id=f"contract_failure_blast_radius:{contract_id}",
        idempotency_key=f"contract_failure_blast_radius:{contract_id}:{blast_radius}",
        payload=payload,
    )


def append_dashboard_triage_workflow(
    log,
    *,
    triage_id="dashboard-triage-unit-v1",
    blocker_code="regression_budget_exceeded",
    owner="runtime-owner",
    next_action="open_metric_escalation",
    due_at="2026-01-16T00:55:00Z",
):
    payload = {
        "triage_id": triage_id,
        "blocker_code": blocker_code,
        "owner": owner,
        "next_action": next_action,
        "due_at": due_at,
    }
    return log.append_event(
        event_type="dashboard_triage_workflow_recorded",
        aggregate_id=f"dashboard_triage_workflow:{triage_id}",
        idempotency_key=f"dashboard_triage_workflow:{triage_id}:{blocker_code}",
        payload=payload,
    )


def append_issue_escalation_from_metrics(
    log,
    *,
    metric_id="missed_clean_gold_false_negative_rate",
    threshold=0.15,
    issue_id="issue-runtime-trust-unit-v1",
    escalation_owner="runtime-owner",
    created_at="2026-01-15T00:56:00Z",
):
    payload = {
        "metric_id": metric_id,
        "threshold": threshold,
        "issue_id": issue_id,
        "escalation_owner": escalation_owner,
        "created_at": created_at,
    }
    return log.append_event(
        event_type="issue_escalation_from_metrics_recorded",
        aggregate_id=f"issue_escalation_from_metrics:{metric_id}",
        idempotency_key=f"issue_escalation_from_metrics:{metric_id}:{issue_id}",
        payload=payload,
    )


def append_promotion_evidence_package(
    log,
    *,
    package_id="promotion-evidence-package-unit-v1",
    evidence_hash=None,
    generated_at="2026-01-15T00:57:00Z",
    approval_status="approved",
):
    payload = {
        "package_id": package_id,
        "evidence_hash": evidence_hash or sha256_hex({"package_id": package_id, "scope": "normal_tiny"}),
        "generated_at": generated_at,
        "approval_status": approval_status,
    }
    return log.append_event(
        event_type="promotion_evidence_package_recorded",
        aggregate_id=f"promotion_evidence_package:{package_id}",
        idempotency_key=f"promotion_evidence_package:{package_id}:{approval_status}",
        payload=payload,
    )


def append_regression_budget(
    log,
    *,
    budget_id="regression-budget-unit-v1",
    metric_id="clean_dog_capture_rate",
    allowed_regression=0.03,
    observed_regression=0.01,
    action="allow_release",
):
    payload = {
        "budget_id": budget_id,
        "metric_id": metric_id,
        "allowed_regression": allowed_regression,
        "observed_regression": observed_regression,
        "action": action,
    }
    return log.append_event(
        event_type="regression_budget_recorded",
        aggregate_id=f"regression_budget:{budget_id}",
        idempotency_key=f"regression_budget:{budget_id}:{metric_id}",
        payload=payload,
    )


def append_root_cause_taxonomy_versioning(
    log,
    *,
    taxonomy_version="root-cause-taxonomy-v1",
    root_cause_code="quote_clean_evidence_expired",
    severity="high",
    migration_policy="map_legacy_codes_before_postmortem",
    effective_at="2026-01-15T00:58:00Z",
):
    payload = {
        "taxonomy_version": taxonomy_version,
        "root_cause_code": root_cause_code,
        "severity": severity,
        "migration_policy": migration_policy,
        "effective_at": effective_at,
    }
    return log.append_event(
        event_type="root_cause_taxonomy_versioning_recorded",
        aggregate_id=f"root_cause_taxonomy_versioning:{taxonomy_version}:{root_cause_code}",
        idempotency_key=f"root_cause_taxonomy_versioning:{taxonomy_version}:{root_cause_code}",
        payload=payload,
    )


def append_idempotency_contract(
    log,
    *,
    token_ca,
    paper_trade_id=1,
    pool="pool-a",
    namespace="paper_entry_execution",
    environment_id="unit",
    decision_id=None,
    execution_id=None,
    idempotency_key=None,
    intent_hash=None,
    token_lifecycle_key=None,
):
    key_material_hash = intent_hash or f"intent-{paper_trade_id}"
    decision_id = decision_id or f"paper_trade:{paper_trade_id}:entry_decision"
    execution_id = execution_id or f"paper_trade:{paper_trade_id}:entry_execution"
    idempotency_key = idempotency_key or f"{environment_id}:{namespace}:{key_material_hash}"
    token_lifecycle_key = token_lifecycle_key or f"solana:{token_ca}:{pool}:0:{paper_trade_id}"
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "idempotency_contract_version": "legacy_paper_entry_idempotency_v0.1",
        "decision_id": decision_id,
        "execution_id": execution_id,
        "idempotency_key": idempotency_key,
        "token_lifecycle_key": token_lifecycle_key,
        "action": "paper_entry",
        "namespace": namespace,
        "environment_id": environment_id,
        "route": "unit_route",
        "hash_algorithm": "sha256(canonical_json)",
        "collision_policy": "reject_same_namespace_key_with_different_intent_hash",
        "idempotency_intent_hash": key_material_hash,
        "key_material_hash": key_material_hash,
        "namespace_isolation_prefix": f"{environment_id}:{namespace}:",
        "cross_environment_isolated": True,
        "idempotency_proof_level": "legacy_paper_trade_entry_execution",
    }
    return log.append_event(
        event_type="idempotency_contract_recorded",
        aggregate_id=f"idempotency_contract:solana:{token_ca}:{pool}:0:{paper_trade_id}",
        idempotency_key=f"idempotency_contract:{environment_id}:{paper_trade_id}:legacy_paper_entry_idempotency_v0.1",
        payload=payload,
    )


def append_execution_control(log, *, token_ca, paper_trade_id=1, pool="pool-a", environment_id="unit"):
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "execution_control_version": "legacy_paper_entry_execution_control_v0.1",
        "decision_id": f"paper_trade:{paper_trade_id}:entry_decision",
        "execution_id": f"paper_trade:{paper_trade_id}:entry_execution",
        "token_lifecycle_key": f"solana:{token_ca}:{pool}:0:{paper_trade_id}",
        "environment_id": environment_id,
        "route": "unit_route",
        "lease_id": f"lease:{environment_id}:abc-{paper_trade_id}",
        "fencing_token": f"fence-{paper_trade_id}",
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
    }
    return log.append_event(
        event_type="execution_control_recorded",
        aggregate_id=f"execution_control:solana:{token_ca}:{pool}:0:{paper_trade_id}",
        idempotency_key=f"execution_control:{environment_id}:{paper_trade_id}:legacy_paper_entry_execution_control_v0.1",
        payload=payload,
    )


def append_paper_ledger(log, *, token_ca, paper_trade_id=1, pool="pool-a", environment_id="unit"):
    position_id = f"paper_trade:{paper_trade_id}:position"
    decision_id = f"paper_trade:{paper_trade_id}:entry_decision"
    execution_id = f"paper_trade:{paper_trade_id}:entry_execution"
    position_material = {
        "position_id": position_id,
        "decision_id": decision_id,
        "execution_id": execution_id,
        "entry_size_sol": "0.010000000000",
        "remaining_size": "0.000000000000",
        "position_status": "closed",
        "row_state_hash": f"row-state-{paper_trade_id}",
    }
    capital_material = {
        "capital_ledger_id": f"capital_ledger:{environment_id}:paper:{paper_trade_id}",
        "capital_basis_sol": "1.000000000000",
        "available_capital": "1.001000000000",
        "reserved_capital": "0.000000000000",
        "open_exposure": "0.000000000000",
        "realized_pnl_sol": "0.001000000000",
        "fees_sol": "0.000000000000",
    }
    ledger_material = {
        **capital_material,
        "ledger_checkpoint_id": f"ledger_checkpoint:{environment_id}:paper:{paper_trade_id}",
        "invariant_formula": "available_capital + reserved_capital + open_exposure - realized_pnl_sol + fees_sol == capital_basis_sol",
        "invariant_lhs": "1.000000000000",
        "invariant_rhs": "1.000000000000",
    }
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "paper_ledger_version": "legacy_paper_position_capital_ledger_v0.1",
        "decision_id": decision_id,
        "execution_id": execution_id,
        "token_lifecycle_key": f"solana:{token_ca}:{pool}:0:{paper_trade_id}",
        "environment_id": environment_id,
        "route": "unit_route",
        "position_id": position_id,
        "position_status": "closed",
        "entry_size_sol": "0.010000000000",
        "remaining_size": "0.000000000000",
        "position_realized_pnl_sol": "0.001000000000",
        "size_source": "paper_trades.position_size_sol",
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
        "reservation_id": f"reservation:{environment_id}:{paper_trade_id}",
        "reservation_status": "released",
        "reservation_ttl_sec": "20.000000000000",
        "release_reason": "position_closed",
        "reserved_capital_at_entry": "0.010000000000",
        "ledger_scope": "paper_global_capital_reconstruction",
        "ledger_proof_level": "unit_paper_ledger",
    }
    return log.append_event(
        event_type="paper_ledger_recorded",
        aggregate_id=f"paper_ledger:solana:{token_ca}:{pool}:0:{paper_trade_id}",
        idempotency_key=f"paper_ledger:{environment_id}:{paper_trade_id}:legacy_paper_position_capital_ledger_v0.1",
        payload=payload,
    )


def append_no_fill_outcome(log, *, token_ca, paper_trade_id=1, pool="pool-a", environment_id="unit", outcome_state="filled_paper"):
    required = outcome_state in {"no_fill", "skipped", "rejected", "failed", "cancelled"}
    payload = {
        "paper_trade_id": paper_trade_id,
        "token_ca": token_ca,
        "symbol": token_ca[-4:],
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": 0,
        "recovery_control_version": "legacy_paper_recovery_control_v0.1",
        "no_fill_outcome_version": "legacy_paper_recovery_control_v0.1",
        "attempt_id": f"paper_trade:{paper_trade_id}:attempt",
        "decision_id": f"paper_trade:{paper_trade_id}:entry_decision",
        "execution_id": f"paper_trade:{paper_trade_id}:entry_execution",
        "token_lifecycle_key": f"solana:{token_ca}:{pool}:0:{paper_trade_id}",
        "environment_id": environment_id,
        "route": "unit_route",
        "outcome_state": outcome_state,
        "terminal_state": True,
        "no_fill_record_required": required,
        "no_fill_reason": "quote_unavailable" if required else "none_filled_paper",
        "missed_net_peak30": 0.42 if required else 0.0,
        "missed_net_peak30_source": "unit_peak",
        "no_fill_cost": 0.42 if required else 0.0,
        "no_fill_saved_loss": 0.11 if required else 0.0,
        "no_fill_cost_model": "unit_no_fill_cost_model",
        "outcome_source": "unit",
        "outcome_available_at": "2026-01-15T00:00:02Z",
    }
    return log.append_event(
        event_type="no_fill_outcome_recorded",
        aggregate_id=f"no_fill_outcome:solana:{token_ca}:{pool}:0:{paper_trade_id}",
        idempotency_key=f"no_fill_outcome:{environment_id}:{paper_trade_id}:legacy_paper_recovery_control_v0.1",
        payload=payload,
    )


def append_runtime_recovery_control(log, *, environment_id="unit", state="clean_start"):
    payload = {
        "recovery_control_version": "legacy_paper_recovery_control_v0.1",
        "recovery_id": f"recovery:{environment_id}:unit",
        "state": state,
        "environment_id": environment_id,
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
        "drain_id": f"drain:{environment_id}:unit",
        "queued_candidates_revalidated": 1,
        "expired_candidates_emitted": 0,
        "resume_drain_completed_at": "2026-01-15T00:00:03Z",
        "drain_status": "completed",
        "new_entries_blocked_until_drain": True,
        "resume_allowed": True,
    }
    return log.append_event(
        event_type="runtime_recovery_control_recorded",
        aggregate_id=f"runtime_recovery:{environment_id}",
        idempotency_key=f"runtime_recovery_control:{environment_id}:legacy_paper_recovery_control_v0.1",
        payload=payload,
    )


def test_denominator_projection_counts_d_buckets_and_dedups_by_token_pool_epoch(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id=1, token_ca="TokenA", captured=True, **FULL_D3B_FLAGS)
    append_decision(log, decision_id=2, token_ca="TokenA", captured=False, **FULL_D3B_FLAGS)
    append_decision(
        log,
        decision_id=3,
        token_ca="TokenB",
        source_dog_label="silver",
        telegram_seen=True,
        realtime_observable=True,
        realtime_clean=True,
        entry_quote_executable=True,
        exit_quote_executable=False,
        liquidity_ok=True,
        critical_risk_ok=True,
        ex_ante_feasible=True,
    )
    append_decision(log, decision_id=4, token_ca="TokenC", source_dog_label="copper", **FULL_D3B_FLAGS)

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["health"]["event_log_ok"] is True
    assert projection["health"]["denominator_clean"] is True
    assert projection["metrics"]["denominator_seed_records"] == 3
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 2
    assert projection["metrics"]["telegram_realtime_observable_gold_silver_D1"] == 2
    assert projection["metrics"]["telegram_realtime_clean_gold_silver_D2"] == 2
    assert projection["metrics"]["telegram_externally_actionable_gold_silver_D3a"] == 1
    assert projection["metrics"]["telegram_policy_actionable_gold_silver_D3b"] == 1
    assert projection["metrics"]["telegram_captured_actionable_D3a"] == 1
    assert projection["metrics"]["telegram_captured_actionable_D3b"] == 1
    assert projection["metrics"]["telegram_capture_rate_D3a"] == 1.0
    assert projection["metrics"]["telegram_capture_rate_D3b"] == 1.0

    token_a = [record for record in projection["records"] if record["token_ca"] == "TokenA"][0]
    assert token_a["merged_decision_event_ids"] == [1, 2]
    assert token_a["denominator_membership"]["D3b_policy_actionable_gold_silver"] is True


def test_denominator_projection_consumes_realtime_clean_detector_contract(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags["realtime_clean"] = False
    append_decision(log, decision_id=1, token_ca="TokenClean", captured=True, **flags)
    append_realtime_clean(log, token_ca="TokenClean")

    projection = build_denominator_projection(tmp_path, include_records=True)
    evidence = projection["contract_evidence"]["RealtimeCleanDetector"]

    assert projection["realtime_clean_detector_recorded_events"] == 1
    assert projection["health"]["realtime_clean_detector_ok"] is True
    assert evidence["eligible_realtime_clean_records"] == 1
    assert evidence["realtime_clean_observed_count"] == 1
    assert evidence["dirty_observed_count"] == 0
    assert evidence["malformed_count"] == 0
    assert evidence["future_leakage_count"] == 0
    assert evidence["clean_standard_versions"] == ["legacy_round_trip_quote_clean_v0.1"]
    assert projection["metrics"]["telegram_realtime_clean_gold_silver_D2"] == 1
    assert projection["records"][0]["realtime_clean_contract"]["clean_observation_type"] == "TRADABLE_CLEAN_OBSERVED"


def test_denominator_projection_consumes_quote_intent_binding_contract(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags["realtime_clean"] = False
    append_decision(log, decision_id=1, token_ca="TokenQuote", captured=True, **flags)
    append_realtime_clean(log, token_ca="TokenQuote")
    append_quote_intent_binding(log, token_ca="TokenQuote")

    projection = build_denominator_projection(tmp_path, include_records=True)
    evidence = projection["contract_evidence"]["QuoteIntentBindingContract"]

    assert projection["quote_intent_binding_recorded_events"] == 1
    assert projection["health"]["quote_intent_binding_ok"] is True
    assert evidence["eligible_quote_intent_binding_records"] == 1
    assert evidence["quote_intent_bound_count"] == 1
    assert evidence["malformed_count"] == 0
    assert evidence["mismatch_count"] == 0
    assert evidence["future_leakage_count"] == 0
    assert evidence["binding_policy_versions"] == ["legacy_paper_trade_quote_intent_binding_v0.1"]
    assert evidence["quote_binding_proof_levels"] == ["entry_execution_audit"]
    assert projection["records"][0]["quote_intent_binding_contract"]["quote_intent_bound"] is True


def test_denominator_projection_consumes_raw_provider_evidence_contract(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags["realtime_clean"] = False
    append_decision(log, decision_id=1, token_ca="TokenProvider", captured=True, **flags)
    append_realtime_clean(log, token_ca="TokenProvider")
    append_quote_intent_binding(log, token_ca="TokenProvider")
    append_raw_provider_evidence(log, token_ca="TokenProvider")

    projection = build_denominator_projection(tmp_path, include_records=True)
    evidence = projection["contract_evidence"]["RawProviderEvidenceContract"]

    assert projection["raw_provider_evidence_recorded_events"] == 1
    assert projection["health"]["raw_provider_evidence_ok"] is True
    assert evidence["eligible_raw_provider_records"] == 1
    assert evidence["raw_provider_evidence_observation_count"] == 1
    assert evidence["trusted_raw_provider_evidence_count"] == 1
    assert evidence["untrusted_raw_provider_evidence_count"] == 0
    assert evidence["malformed_count"] == 0
    assert evidence["provider_evidence_violation_count"] == 0
    assert evidence["providers"] == ["jupiter_ultra"]
    assert evidence["endpoints"] == ["/ultra/v1/order"]
    assert evidence["provider_evidence_proof_levels"] == ["provider_request_id_with_raw_response_hash"]
    assert evidence["response_material_types"] == ["execution._rawOrder"]
    contract = projection["records"][0]["raw_provider_evidence_contract"]
    assert contract["provider_evidence_valid"] is True
    assert len(contract["raw_response_hash"]) == 64
    assert contract["hash_algorithm"] == "sha256(canonical_json)"


def test_denominator_projection_rejects_untrusted_raw_provider_evidence(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags["realtime_clean"] = False
    append_decision(log, decision_id=1, token_ca="TokenProviderBad", captured=True, **flags)
    append_raw_provider_evidence(log, token_ca="TokenProviderBad", trusted=False)

    projection = build_denominator_projection(tmp_path, include_records=True)
    evidence = projection["contract_evidence"]["RawProviderEvidenceContract"]

    assert projection["health"]["raw_provider_evidence_ok"] is False
    assert evidence["trusted_raw_provider_evidence_count"] == 0
    assert evidence["untrusted_raw_provider_evidence_count"] == 1
    assert evidence["provider_evidence_violation_count"] == 1
    assert evidence["provider_evidence_violations"][0]["violation_fields"] == [
        "provider_evidence_trusted",
        "raw_response_available",
    ]


def test_denominator_projection_rejects_legacy_response_material_even_if_flags_claim_trusted(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id=1, token_ca="TokenProviderForged", captured=True, **FULL_D3B_FLAGS)
    append_raw_provider_evidence(
        log,
        token_ca="TokenProviderForged",
        payload_overrides={
            "response_material_type": "execution_json_projection",
            "provider_evidence_proof_level": "legacy_execution_projection_without_raw_provider_response",
            "raw_response_available": True,
            "provider_evidence_trusted": True,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)
    evidence = projection["contract_evidence"]["RawProviderEvidenceContract"]
    violations = evidence["provider_evidence_violations"][0]["violation_fields"]

    assert projection["health"]["raw_provider_evidence_ok"] is False
    assert evidence["trusted_raw_provider_evidence_count"] == 0
    assert "response_material_type" in violations
    assert "provider_evidence_proof_level" in violations
    assert projection["records"][0]["raw_provider_evidence_contract"]["provider_evidence_valid"] is False


def test_denominator_projection_requires_raw_response_hash_for_trusted_provider_evidence(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id=1, token_ca="TokenProviderNoRawHash", captured=True, **FULL_D3B_FLAGS)
    append_raw_provider_evidence(
        log,
        token_ca="TokenProviderNoRawHash",
        payload_overrides={
            "raw_response_available": True,
            "provider_evidence_trusted": True,
            "raw_response_hash": None,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)
    evidence = projection["contract_evidence"]["RawProviderEvidenceContract"]

    assert projection["health"]["raw_provider_evidence_ok"] is False
    assert evidence["trusted_raw_provider_evidence_count"] == 0
    assert evidence["provider_evidence_violations"][0]["violation_fields"] == ["raw_response_hash"]


def test_denominator_projection_consumes_fee_provider_and_risk_normal_tiny_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id="d-provider-chain", token_ca="TokenProviderChain", **FULL_D3B_FLAGS)
    append_raw_provider_evidence(log, token_ca="TokenProviderChain")
    append_fee_schedule(log)
    append_provider_credential_scope(log)
    append_provider_request_replay(log, token_ca="TokenProviderChain")
    append_provider_response_authenticity(log, token_ca="TokenProviderChain")
    append_risk_revalidation_after_entry(log, token_ca="TokenProviderChain")

    projection = build_denominator_projection(tmp_path, include_records=True)

    fee_source = projection["contract_evidence"]["FeeScheduleSourceContract"]
    fee_version = projection["contract_evidence"]["FeeScheduleVersionContract"]
    credential_scope = projection["contract_evidence"]["ProviderCredentialScopeContract"]
    request_replay = projection["contract_evidence"]["ProviderRequestReplayContract"]
    response_authenticity = projection["contract_evidence"]["ProviderResponseAuthenticityContract"]
    risk_revalidation = projection["contract_evidence"]["RiskRevalidationAfterEntryContract"]
    assert projection["fee_schedule_recorded_events"] == 1
    assert projection["provider_credential_scope_recorded_events"] == 1
    assert projection["provider_request_replay_recorded_events"] == 1
    assert projection["provider_response_authenticity_recorded_events"] == 1
    assert projection["risk_revalidation_after_entry_recorded_events"] == 1
    assert projection["health"]["fee_schedule_source_ok"] is True
    assert projection["health"]["fee_schedule_version_ok"] is True
    assert projection["health"]["provider_credential_scope_ok"] is True
    assert projection["health"]["provider_request_replay_ok"] is True
    assert projection["health"]["provider_response_authenticity_ok"] is True
    assert projection["health"]["risk_revalidation_after_entry_ok"] is True
    assert fee_source["valid_fee_schedule_source_count"] == 1
    assert fee_version["valid_fee_schedule_version_count"] == 1
    assert credential_scope["valid_provider_credential_scope_count"] == 1
    assert request_replay["valid_provider_request_replay_count"] == 1
    assert response_authenticity["valid_provider_response_authenticity_count"] == 1
    assert risk_revalidation["valid_risk_revalidation_after_entry_count"] == 1
    record = projection["records"][0]
    assert record["provider_request_replay_contract"]["provider_request_replay_valid"] is True
    assert record["provider_response_authenticity_contract"]["provider_response_authenticity_valid"] is True
    assert record["risk_revalidation_after_entry_contract"]["risk_revalidation_after_entry_valid"] is True


def test_denominator_projection_rejects_malformed_fee_provider_and_risk_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id="d-provider-bad", token_ca="TokenProviderBadChain", **FULL_D3B_FLAGS)
    append_fee_schedule(log, bad_hash=True)
    append_provider_credential_scope(log, revoked=True)
    append_provider_request_replay(log, token_ca="TokenProviderBadChain", bad_hash=True)
    append_provider_response_authenticity(log, token_ca="TokenProviderBadChain", signature_status="missing")
    append_risk_revalidation_after_entry(
        log,
        token_ca="TokenProviderBadChain",
        risk_status="bad",
        exit_safety_action="hold",
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["health"]["fee_schedule_source_ok"] is False
    assert projection["health"]["provider_credential_scope_ok"] is False
    assert projection["health"]["provider_request_replay_ok"] is False
    assert projection["health"]["provider_response_authenticity_ok"] is False
    assert projection["health"]["risk_revalidation_after_entry_ok"] is False
    assert projection["contract_evidence"]["FeeScheduleSourceContract"]["fee_schedule_source_violation_count"] == 1
    assert projection["contract_evidence"]["ProviderCredentialScopeContract"]["provider_credential_scope_violation_count"] == 1
    assert projection["contract_evidence"]["ProviderRequestReplayContract"]["provider_request_replay_violation_count"] == 1
    assert projection["contract_evidence"]["ProviderResponseAuthenticityContract"]["provider_response_authenticity_violation_count"] == 1
    assert projection["contract_evidence"]["RiskRevalidationAfterEntryContract"]["risk_revalidation_after_entry_violation_count"] == 1


def test_denominator_projection_consumes_provider_dependency_resource_trust_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_provider_byzantine_quorum(log)
    append_provider_cache_poisoning_guard(log)
    append_external_dependency(log)
    append_third_party_status_correlation(log)
    append_resource_exhaustion(log)

    projection = build_denominator_projection(tmp_path)

    quorum = projection["contract_evidence"]["ProviderByzantineQuorumContract"]
    cache_guard = projection["contract_evidence"]["ProviderCachePoisoningGuard"]
    external_dependency = projection["contract_evidence"]["ExternalDependencyContract"]
    status_correlation = projection["contract_evidence"]["ThirdPartyStatusCorrelationContract"]
    resource_exhaustion = projection["contract_evidence"]["ResourceExhaustionContract"]
    assert projection["provider_byzantine_quorum_recorded_events"] == 1
    assert projection["provider_cache_poisoning_guard_recorded_events"] == 1
    assert projection["external_dependency_health_recorded_events"] == 1
    assert projection["third_party_status_correlation_recorded_events"] == 1
    assert projection["resource_exhaustion_recorded_events"] == 1
    assert projection["health"]["provider_byzantine_quorum_ok"] is True
    assert projection["health"]["provider_cache_poisoning_guard_ok"] is True
    assert projection["health"]["external_dependency_ok"] is True
    assert projection["health"]["third_party_status_correlation_ok"] is True
    assert projection["health"]["resource_exhaustion_ok"] is True
    assert quorum["valid_provider_byzantine_quorum_count"] == 1
    assert cache_guard["valid_provider_cache_poisoning_guard_count"] == 1
    assert external_dependency["valid_external_dependency_count"] == 1
    assert status_correlation["valid_third_party_status_correlation_count"] == 1
    assert resource_exhaustion["valid_resource_exhaustion_count"] == 1
    assert quorum["providers"] == ["gmgn_quote", "jupiter_ultra"]
    assert cache_guard["poison_detected_count"] == 0
    assert external_dependency["fail_closed_actions"] == ["block_entry"]


def test_denominator_projection_rejects_provider_dependency_resource_trust_violations(tmp_path):
    log = V27EventLog(tmp_path)
    append_provider_byzantine_quorum(log, provider_set=["jupiter_ultra"], selected_provider="gmgn_quote")
    append_provider_cache_poisoning_guard(log, poison_detected=True, quarantine_action="none")
    append_external_dependency(log, health_status="down", fail_closed_action="none")
    append_third_party_status_correlation(log, incident_id="incident-123", correlation_result="no_incident")
    append_resource_exhaustion(log, pressure_level="critical", pressure_action="none", safety_budget_remaining=0)

    projection = build_denominator_projection(tmp_path)

    assert projection["health"]["provider_byzantine_quorum_ok"] is False
    assert projection["health"]["provider_cache_poisoning_guard_ok"] is False
    assert projection["health"]["external_dependency_ok"] is False
    assert projection["health"]["third_party_status_correlation_ok"] is False
    assert projection["health"]["resource_exhaustion_ok"] is False
    assert projection["contract_evidence"]["ProviderByzantineQuorumContract"]["provider_byzantine_quorum_violation_count"] == 1
    assert projection["contract_evidence"]["ProviderCachePoisoningGuard"]["provider_cache_poisoning_guard_violation_count"] == 1
    assert projection["contract_evidence"]["ExternalDependencyContract"]["external_dependency_violation_count"] == 1
    assert projection["contract_evidence"]["ThirdPartyStatusCorrelationContract"]["third_party_status_correlation_violation_count"] == 1
    assert projection["contract_evidence"]["ResourceExhaustionContract"]["resource_exhaustion_violation_count"] == 1


def test_denominator_projection_consumes_config_activation_retry_policy_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    config_hash = sha256_hex({"config_id": "config-unit-v1", "version": "v1"})
    append_config_distribution(log, config_hash=config_hash)
    append_config_distribution_ack(log, worker_id="dashboard", config_hash=config_hash)
    append_config_distribution_ack(log, worker_id="v27-read-model-refresh", config_hash=config_hash)
    append_in_flight_config_rotation(log)
    append_policy_activation_barrier(log)
    append_retry_policy_catalog(log)

    projection = build_denominator_projection(tmp_path)

    config_distribution = projection["contract_evidence"]["ConfigDistributionContract"]
    config_ack = projection["contract_evidence"]["ConfigDistributionAckContract"]
    rotation = projection["contract_evidence"]["InFlightConfigRotationPolicy"]
    activation = projection["contract_evidence"]["PolicyActivationBarrierContract"]
    retry_policy = projection["contract_evidence"]["RetryPolicyCatalogContract"]
    assert projection["config_distribution_recorded_events"] == 1
    assert projection["config_distribution_ack_recorded_events"] == 2
    assert projection["in_flight_config_rotation_recorded_events"] == 1
    assert projection["policy_activation_barrier_recorded_events"] == 1
    assert projection["retry_policy_catalog_recorded_events"] == 1
    assert projection["health"]["config_distribution_ok"] is True
    assert projection["health"]["config_distribution_ack_ok"] is True
    assert projection["health"]["in_flight_config_rotation_ok"] is True
    assert projection["health"]["policy_activation_barrier_ok"] is True
    assert projection["health"]["retry_policy_catalog_ok"] is True
    assert config_distribution["valid_config_distribution_count"] == 1
    assert config_distribution["config_distribution_violation_count"] == 0
    assert config_ack["valid_config_distribution_ack_count"] == 2
    assert rotation["valid_in_flight_config_rotation_count"] == 1
    assert activation["valid_policy_activation_barrier_count"] == 1
    assert retry_policy["valid_retry_policy_catalog_count"] == 1


def test_denominator_projection_rejects_config_activation_retry_policy_violations(tmp_path):
    log = V27EventLog(tmp_path)
    config_hash = sha256_hex({"config_id": "config-unit-v1", "version": "v1"})
    bad_config_hash = sha256_hex({"config_id": "config-unit-v1", "version": "bad"})
    append_config_distribution(log, config_hash=config_hash, ack_policy="eventual_ack")
    append_config_distribution_ack(log, worker_id="dashboard", config_hash=bad_config_hash, ack_state="stale")
    old_hash = sha256_hex({"config_id": "same", "version": "v1"})
    append_in_flight_config_rotation(log, old_config_hash=old_hash, new_config_hash=old_hash, rotation_policy="instant_swap")
    append_policy_activation_barrier(log, required_worker_ack_count=2, observed_worker_ack_count=1)
    append_retry_policy_catalog(log, backoff_policy="no_backoff", max_attempts=99, jitter_policy="none")

    projection = build_denominator_projection(tmp_path)

    assert projection["health"]["config_distribution_ok"] is False
    assert projection["health"]["config_distribution_ack_ok"] is False
    assert projection["health"]["in_flight_config_rotation_ok"] is False
    assert projection["health"]["policy_activation_barrier_ok"] is False
    assert projection["health"]["retry_policy_catalog_ok"] is False
    assert projection["contract_evidence"]["ConfigDistributionContract"]["config_distribution_violation_count"] == 2
    assert projection["contract_evidence"]["ConfigDistributionAckContract"]["config_distribution_ack_violation_count"] == 1
    assert projection["contract_evidence"]["InFlightConfigRotationPolicy"]["in_flight_config_rotation_violation_count"] == 1
    assert projection["contract_evidence"]["PolicyActivationBarrierContract"]["policy_activation_barrier_violation_count"] == 1
    assert projection["contract_evidence"]["RetryPolicyCatalogContract"]["retry_policy_catalog_violation_count"] == 1


def test_denominator_projection_consumes_alert_model_release_runtime_trust_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_alert_noise_budget(log)
    append_alert_suppression_audit(log)
    append_canary_abort(log)
    append_model_artifact_runtime_compatibility(log)
    append_model_rollback(log)
    append_post_release_monitoring_window(log)
    append_training_poisoning_guard(log)

    projection = build_denominator_projection(tmp_path)

    assert projection["alert_noise_budget_recorded_events"] == 1
    assert projection["alert_suppression_audit_recorded_events"] == 1
    assert projection["canary_abort_recorded_events"] == 1
    assert projection["model_artifact_runtime_compatibility_recorded_events"] == 1
    assert projection["model_rollback_recorded_events"] == 1
    assert projection["post_release_monitoring_window_recorded_events"] == 1
    assert projection["training_poisoning_guard_recorded_events"] == 1
    assert projection["health"]["alert_noise_budget_ok"] is True
    assert projection["health"]["alert_suppression_audit_ok"] is True
    assert projection["health"]["canary_abort_ok"] is True
    assert projection["health"]["model_artifact_runtime_compatibility_ok"] is True
    assert projection["health"]["model_rollback_ok"] is True
    assert projection["health"]["post_release_monitoring_window_ok"] is True
    assert projection["health"]["training_poisoning_guard_ok"] is True
    assert projection["contract_evidence"]["AlertNoiseBudgetContract"]["valid_alert_noise_budget_count"] == 1
    assert projection["contract_evidence"]["AlertSuppressionAuditContract"]["valid_alert_suppression_audit_count"] == 1
    assert projection["contract_evidence"]["CanaryAbortContract"]["valid_canary_abort_count"] == 1
    assert (
        projection["contract_evidence"]["ModelArtifactRuntimeCompatibilityContract"][
            "valid_model_artifact_runtime_compatibility_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["ModelRollbackContract"]["valid_model_rollback_count"] == 1
    assert projection["contract_evidence"]["PostReleaseMonitoringWindow"]["valid_post_release_monitoring_window_count"] == 1
    assert projection["contract_evidence"]["TrainingPoisoningGuard"]["valid_training_poisoning_guard_count"] == 1


def test_denominator_projection_rejects_alert_model_release_runtime_trust_violations(tmp_path):
    log = V27EventLog(tmp_path)
    append_alert_noise_budget(log, noise_budget=2, suppression_count=5)
    append_alert_suppression_audit(log, expires_at="not-a-time")
    append_canary_abort(log, abort_action="notify_only")
    append_model_artifact_runtime_compatibility(log, compatibility_result="incompatible")
    append_model_rollback(log, from_model_snapshot_id="model-same", to_model_snapshot_id="model-same")
    append_post_release_monitoring_window(log, window_start="2026-01-15T02:00:00Z", window_end="2026-01-15T01:00:00Z")
    append_training_poisoning_guard(log, poison_signal_count=2, quarantine_action="none")

    projection = build_denominator_projection(tmp_path)

    assert projection["health"]["alert_noise_budget_ok"] is False
    assert projection["health"]["alert_suppression_audit_ok"] is False
    assert projection["health"]["canary_abort_ok"] is False
    assert projection["health"]["model_artifact_runtime_compatibility_ok"] is False
    assert projection["health"]["model_rollback_ok"] is False
    assert projection["health"]["post_release_monitoring_window_ok"] is False
    assert projection["health"]["training_poisoning_guard_ok"] is False
    assert projection["contract_evidence"]["AlertNoiseBudgetContract"]["alert_noise_budget_violation_count"] == 1
    assert projection["contract_evidence"]["AlertSuppressionAuditContract"]["alert_suppression_audit_violation_count"] == 1
    assert projection["contract_evidence"]["CanaryAbortContract"]["canary_abort_violation_count"] == 1
    assert (
        projection["contract_evidence"]["ModelArtifactRuntimeCompatibilityContract"][
            "model_artifact_runtime_compatibility_violation_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["ModelRollbackContract"]["model_rollback_violation_count"] == 1
    assert projection["contract_evidence"]["PostReleaseMonitoringWindow"]["post_release_monitoring_window_violation_count"] == 1
    assert projection["contract_evidence"]["TrainingPoisoningGuard"]["training_poisoning_guard_violation_count"] == 1


def test_denominator_projection_consumes_position_feature_runtime_trust_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_feature_store_consistency(log)
    append_dynamic_token_authority_change(log)
    append_adversarial_execution_simulation(log)
    append_open_position_valuation(log)
    append_exit_policy_migration(log)
    append_open_position_policy_migration(log)
    append_position_ownership_transfer(log)

    projection = build_denominator_projection(tmp_path)

    assert projection["feature_store_consistency_recorded_events"] == 1
    assert projection["dynamic_token_authority_change_recorded_events"] == 1
    assert projection["adversarial_execution_simulation_recorded_events"] == 1
    assert projection["open_position_valuation_recorded_events"] == 1
    assert projection["exit_policy_migration_recorded_events"] == 1
    assert projection["open_position_policy_migration_recorded_events"] == 1
    assert projection["position_ownership_transfer_recorded_events"] == 1
    assert projection["health"]["feature_store_consistency_ok"] is True
    assert projection["health"]["dynamic_token_authority_change_ok"] is True
    assert projection["health"]["adversarial_execution_simulation_ok"] is True
    assert projection["health"]["open_position_valuation_ok"] is True
    assert projection["health"]["exit_policy_migration_ok"] is True
    assert projection["health"]["open_position_policy_migration_ok"] is True
    assert projection["health"]["position_ownership_transfer_ok"] is True
    assert projection["contract_evidence"]["FeatureStoreConsistencyContract"]["valid_feature_store_consistency_count"] == 1
    assert projection["contract_evidence"]["DynamicTokenAuthorityChangeContract"]["valid_dynamic_token_authority_change_count"] == 1
    assert (
        projection["contract_evidence"]["AdversarialExecutionSimulationContract"]["valid_adversarial_execution_simulation_count"]
        == 1
    )
    assert projection["contract_evidence"]["OpenPositionValuationContract"]["valid_open_position_valuation_count"] == 1
    assert projection["contract_evidence"]["ExitPolicyMigrationContract"]["valid_exit_policy_migration_count"] == 1
    assert (
        projection["contract_evidence"]["OpenPositionPolicyMigrationContract"]["valid_open_position_policy_migration_count"]
        == 1
    )
    assert projection["contract_evidence"]["PositionOwnershipTransferContract"]["valid_position_ownership_transfer_count"] == 1


def test_denominator_projection_rejects_position_feature_runtime_trust_violations(tmp_path):
    log = V27EventLog(tmp_path)
    append_feature_store_consistency(
        log,
        offline_hash=sha256_hex({"feature": "offline"}),
        online_hash=sha256_hex({"feature": "online"}),
    )
    append_dynamic_token_authority_change(log, risk_action="observe")
    append_adversarial_execution_simulation(log, safety_result="bypassed")
    append_open_position_valuation(log, valuation_price=0)
    append_exit_policy_migration(log, old_exit_policy="exit-policy-v1", new_exit_policy="exit-policy-v1")
    append_open_position_policy_migration(log, old_exit_policy="exit-policy-v1", new_exit_policy="exit-policy-v1")
    append_position_ownership_transfer(log, from_owner="paper_executor", to_owner="paper_executor")

    projection = build_denominator_projection(tmp_path)

    assert projection["health"]["feature_store_consistency_ok"] is False
    assert projection["health"]["dynamic_token_authority_change_ok"] is False
    assert projection["health"]["adversarial_execution_simulation_ok"] is False
    assert projection["health"]["open_position_valuation_ok"] is False
    assert projection["health"]["exit_policy_migration_ok"] is False
    assert projection["health"]["open_position_policy_migration_ok"] is False
    assert projection["health"]["position_ownership_transfer_ok"] is False
    assert projection["contract_evidence"]["FeatureStoreConsistencyContract"]["feature_store_consistency_violation_count"] == 1
    assert (
        projection["contract_evidence"]["DynamicTokenAuthorityChangeContract"]["dynamic_token_authority_change_violation_count"]
        == 1
    )
    assert (
        projection["contract_evidence"]["AdversarialExecutionSimulationContract"]["adversarial_execution_simulation_violation_count"]
        == 1
    )
    assert projection["contract_evidence"]["OpenPositionValuationContract"]["open_position_valuation_violation_count"] == 1
    assert projection["contract_evidence"]["ExitPolicyMigrationContract"]["exit_policy_migration_violation_count"] == 1
    assert (
        projection["contract_evidence"]["OpenPositionPolicyMigrationContract"]["open_position_policy_migration_violation_count"]
        == 1
    )
    assert (
        projection["contract_evidence"]["PositionOwnershipTransferContract"]["position_ownership_transfer_violation_count"]
        == 1
    )


def test_denominator_projection_consumes_release_rollback_metric_trust_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_rollback_verification(log)
    append_partial_rollback_policy(log)
    append_release_readiness_review(log)
    append_change_freeze(log)
    append_notification_channel_integrity(log)
    append_runbook_freshness(log)
    append_metric_backfill_impact(log)
    append_selection_bias_diagnostic(log)

    projection = build_denominator_projection(tmp_path)

    assert projection["rollback_verification_recorded_events"] == 1
    assert projection["partial_rollback_policy_recorded_events"] == 1
    assert projection["release_readiness_review_recorded_events"] == 1
    assert projection["change_freeze_recorded_events"] == 1
    assert projection["notification_channel_integrity_recorded_events"] == 1
    assert projection["runbook_freshness_recorded_events"] == 1
    assert projection["metric_backfill_impact_recorded_events"] == 1
    assert projection["selection_bias_diagnostic_recorded_events"] == 1
    assert projection["health"]["rollback_verification_ok"] is True
    assert projection["health"]["partial_rollback_policy_ok"] is True
    assert projection["health"]["release_readiness_review_ok"] is True
    assert projection["health"]["change_freeze_ok"] is True
    assert projection["health"]["notification_channel_integrity_ok"] is True
    assert projection["health"]["runbook_freshness_ok"] is True
    assert projection["health"]["metric_backfill_impact_ok"] is True
    assert projection["health"]["selection_bias_diagnostic_ok"] is True
    assert projection["contract_evidence"]["RollbackVerificationContract"]["valid_rollback_verification_count"] == 1
    assert projection["contract_evidence"]["PartialRollbackPolicy"]["valid_partial_rollback_policy_count"] == 1
    assert projection["contract_evidence"]["ReleaseReadinessReviewContract"]["valid_release_readiness_review_count"] == 1
    assert projection["contract_evidence"]["ChangeFreezeContract"]["valid_change_freeze_count"] == 1
    assert (
        projection["contract_evidence"]["NotificationChannelIntegrityContract"]["valid_notification_channel_integrity_count"]
        == 1
    )
    assert projection["contract_evidence"]["RunbookFreshnessContract"]["valid_runbook_freshness_count"] == 1
    assert projection["contract_evidence"]["MetricBackfillImpactContract"]["valid_metric_backfill_impact_count"] == 1
    assert projection["contract_evidence"]["SelectionBiasDiagnosticContract"]["valid_selection_bias_diagnostic_count"] == 1


def test_denominator_projection_rejects_release_rollback_metric_trust_violations(tmp_path):
    log = V27EventLog(tmp_path)
    append_rollback_verification(log, from_version="release-same", to_version="release-same")
    append_partial_rollback_policy(log, verification_plan="none")
    append_release_readiness_review(log, approval_status="pending")
    append_change_freeze(log, start_at="2026-01-15T02:00:00Z", end_at="2026-01-15T01:00:00Z")
    append_notification_channel_integrity(log, signature_required=False, delivery_status="failed")
    append_runbook_freshness(log, max_age_days=999, freshness_status="stale")
    append_metric_backfill_impact(log, impact_scope="unbounded", impact_report_hash="not-a-hash")
    append_selection_bias_diagnostic(log, included_count=-1, bias_result="biased")

    projection = build_denominator_projection(tmp_path)

    assert projection["health"]["rollback_verification_ok"] is False
    assert projection["health"]["partial_rollback_policy_ok"] is False
    assert projection["health"]["release_readiness_review_ok"] is False
    assert projection["health"]["change_freeze_ok"] is False
    assert projection["health"]["notification_channel_integrity_ok"] is False
    assert projection["health"]["runbook_freshness_ok"] is False
    assert projection["health"]["metric_backfill_impact_ok"] is False
    assert projection["health"]["selection_bias_diagnostic_ok"] is False
    assert projection["contract_evidence"]["RollbackVerificationContract"]["rollback_verification_violation_count"] == 1
    assert projection["contract_evidence"]["PartialRollbackPolicy"]["partial_rollback_policy_violation_count"] == 1
    assert projection["contract_evidence"]["ReleaseReadinessReviewContract"]["release_readiness_review_violation_count"] == 1
    assert projection["contract_evidence"]["ChangeFreezeContract"]["change_freeze_violation_count"] == 1
    assert (
        projection["contract_evidence"]["NotificationChannelIntegrityContract"]["notification_channel_integrity_violation_count"]
        == 1
    )
    assert projection["contract_evidence"]["RunbookFreshnessContract"]["runbook_freshness_violation_count"] == 1
    assert projection["contract_evidence"]["MetricBackfillImpactContract"]["metric_backfill_impact_violation_count"] == 1
    assert projection["contract_evidence"]["SelectionBiasDiagnosticContract"]["selection_bias_diagnostic_violation_count"] == 1


def test_denominator_projection_consumes_final_normal_tiny_blocking_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_access_review(log)
    append_approval_workflow(log)
    append_break_glass_access(log)
    append_csv_spreadsheet_injection(log)
    append_evidence_external_anchoring(log)
    append_experiment_assignment_immutability(log)
    append_incident_postmortem(log)
    append_label_dispute_resolution(log)
    append_negative_control(log)
    append_operator_training_certification(log)

    projection = build_denominator_projection(tmp_path)

    assert projection["access_review_recorded_events"] == 1
    assert projection["approval_workflow_recorded_events"] == 1
    assert projection["break_glass_access_recorded_events"] == 1
    assert projection["csv_spreadsheet_injection_recorded_events"] == 1
    assert projection["evidence_external_anchoring_recorded_events"] == 1
    assert projection["experiment_assignment_immutability_recorded_events"] == 1
    assert projection["incident_postmortem_recorded_events"] == 1
    assert projection["label_dispute_resolution_recorded_events"] == 1
    assert projection["negative_control_recorded_events"] == 1
    assert projection["operator_training_certification_recorded_events"] == 1
    assert projection["health"]["access_review_ok"] is True
    assert projection["health"]["approval_workflow_ok"] is True
    assert projection["health"]["break_glass_access_ok"] is True
    assert projection["health"]["csv_spreadsheet_injection_ok"] is True
    assert projection["health"]["evidence_external_anchoring_ok"] is True
    assert projection["health"]["experiment_assignment_immutability_ok"] is True
    assert projection["health"]["incident_postmortem_ok"] is True
    assert projection["health"]["label_dispute_resolution_ok"] is True
    assert projection["health"]["negative_control_ok"] is True
    assert projection["health"]["operator_training_certification_ok"] is True
    assert projection["contract_evidence"]["AccessReviewContract"]["valid_access_review_count"] == 1
    assert projection["contract_evidence"]["ApprovalWorkflowContract"]["valid_approval_workflow_count"] == 1
    assert projection["contract_evidence"]["BreakGlassAccessContract"]["valid_break_glass_access_count"] == 1
    assert (
        projection["contract_evidence"]["CSVSpreadsheetInjectionContract"]["valid_csv_spreadsheet_injection_count"]
        == 1
    )
    assert (
        projection["contract_evidence"]["EvidenceExternalAnchoringContract"]["valid_evidence_external_anchoring_count"]
        == 1
    )
    assert (
        projection["contract_evidence"]["ExperimentAssignmentImmutabilityContract"][
            "valid_experiment_assignment_immutability_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["IncidentPostmortemContract"]["valid_incident_postmortem_count"] == 1
    assert projection["contract_evidence"]["LabelDisputeResolutionContract"]["valid_label_dispute_resolution_count"] == 1
    assert projection["contract_evidence"]["NegativeControlContract"]["valid_negative_control_count"] == 1
    assert (
        projection["contract_evidence"]["OperatorTrainingCertificationContract"][
            "valid_operator_training_certification_count"
        ]
        == 1
    )


def test_denominator_projection_rejects_final_normal_tiny_blocking_violations(tmp_path):
    log = V27EventLog(tmp_path)
    original_hash = sha256_hex({"assignment_id": "assignment-unit-v1", "arm": "control"})
    append_access_review(log, privilege_delta="unreviewed")
    append_approval_workflow(log, approval_state="pending")
    append_break_glass_access(log, reason="unknown")
    append_csv_spreadsheet_injection(log, sanitization_policy="allow_raw")
    append_evidence_external_anchoring(log, anchored_hash="not-a-hash")
    append_experiment_assignment_immutability(log, original_assignment_hash=original_hash, attempted_change_hash=original_hash)
    append_incident_postmortem(log, root_cause="unknown")
    append_label_dispute_resolution(log, resolution_action="ignore")
    append_negative_control(log, expected_no_effect_metric=0.01, observed_effect=0.5)
    append_operator_training_certification(
        log,
        certification_status="expired",
        expires_at="2026-01-15T00:00:00Z",
        checked_at="2026-01-15T00:46:00Z",
    )

    projection = build_denominator_projection(tmp_path)

    assert projection["health"]["access_review_ok"] is False
    assert projection["health"]["approval_workflow_ok"] is False
    assert projection["health"]["break_glass_access_ok"] is False
    assert projection["health"]["csv_spreadsheet_injection_ok"] is False
    assert projection["health"]["evidence_external_anchoring_ok"] is False
    assert projection["health"]["experiment_assignment_immutability_ok"] is False
    assert projection["health"]["incident_postmortem_ok"] is False
    assert projection["health"]["label_dispute_resolution_ok"] is False
    assert projection["health"]["negative_control_ok"] is False
    assert projection["health"]["operator_training_certification_ok"] is False
    assert projection["contract_evidence"]["AccessReviewContract"]["access_review_violation_count"] == 1
    assert projection["contract_evidence"]["ApprovalWorkflowContract"]["approval_workflow_violation_count"] == 1
    assert projection["contract_evidence"]["BreakGlassAccessContract"]["break_glass_access_violation_count"] == 1
    assert (
        projection["contract_evidence"]["CSVSpreadsheetInjectionContract"]["csv_spreadsheet_injection_violation_count"]
        == 1
    )
    assert (
        projection["contract_evidence"]["EvidenceExternalAnchoringContract"]["evidence_external_anchoring_violation_count"]
        == 1
    )
    assert (
        projection["contract_evidence"]["ExperimentAssignmentImmutabilityContract"][
            "experiment_assignment_immutability_violation_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["IncidentPostmortemContract"]["incident_postmortem_violation_count"] == 1
    assert projection["contract_evidence"]["LabelDisputeResolutionContract"]["label_dispute_resolution_violation_count"] == 1
    assert projection["contract_evidence"]["NegativeControlContract"]["negative_control_violation_count"] == 1
    assert (
        projection["contract_evidence"]["OperatorTrainingCertificationContract"][
            "operator_training_certification_violation_count"
        ]
        == 1
    )


def test_denominator_projection_consumes_runtime_trust_governance_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_runtime_spec_assertion(log)
    append_minimum_viable_trust_boundary(log)
    append_evidence_conflict(log)
    append_evidence_aging(log)
    append_market_regime_invalidates_evidence(log)
    append_source_alpha_decay_exit_criteria(log)
    append_false_negative_budget(log)
    append_small_sample_decision(log)
    append_safety_vs_capture_tradeoff(log)
    append_implementation_drift_monitor(log)

    projection = build_denominator_projection(tmp_path)

    assert projection["runtime_spec_assertion_recorded_events"] == 1
    assert projection["minimum_viable_trust_boundary_recorded_events"] == 1
    assert projection["evidence_conflict_recorded_events"] == 1
    assert projection["evidence_aging_recorded_events"] == 1
    assert projection["market_regime_invalidates_evidence_recorded_events"] == 1
    assert projection["source_alpha_decay_exit_criteria_recorded_events"] == 1
    assert projection["false_negative_budget_recorded_events"] == 1
    assert projection["small_sample_decision_recorded_events"] == 1
    assert projection["safety_vs_capture_tradeoff_recorded_events"] == 1
    assert projection["implementation_drift_monitor_recorded_events"] == 1
    assert projection["health"]["runtime_spec_assertion_ok"] is True
    assert projection["health"]["minimum_viable_trust_boundary_ok"] is True
    assert projection["health"]["evidence_conflict_ok"] is True
    assert projection["health"]["evidence_aging_ok"] is True
    assert projection["health"]["market_regime_invalidates_evidence_ok"] is True
    assert projection["health"]["source_alpha_decay_exit_criteria_ok"] is True
    assert projection["health"]["false_negative_budget_ok"] is True
    assert projection["health"]["small_sample_decision_ok"] is True
    assert projection["health"]["safety_vs_capture_tradeoff_ok"] is True
    assert projection["health"]["implementation_drift_monitor_ok"] is True
    assert projection["contract_evidence"]["RuntimeSpecAssertionContract"]["valid_runtime_spec_assertion_count"] == 1
    assert (
        projection["contract_evidence"]["MinimumViableTrustBoundary"][
            "valid_minimum_viable_trust_boundary_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["EvidenceConflictContract"]["valid_evidence_conflict_count"] == 1
    assert projection["contract_evidence"]["EvidenceAgingContract"]["valid_evidence_aging_count"] == 1
    assert (
        projection["contract_evidence"]["MarketRegimeInvalidatesEvidence"][
            "valid_market_regime_invalidates_evidence_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["SourceAlphaDecayExitCriteria"][
            "valid_source_alpha_decay_exit_criteria_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["FalseNegativeBudgetContract"]["valid_false_negative_budget_count"] == 1
    assert projection["contract_evidence"]["SmallSampleDecisionPolicy"]["valid_small_sample_decision_count"] == 1
    assert (
        projection["contract_evidence"]["SafetyVsCaptureTradeoffContract"][
            "valid_safety_vs_capture_tradeoff_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["ImplementationDriftMonitor"][
            "valid_implementation_drift_monitor_count"
        ]
        == 1
    )


def test_denominator_projection_rejects_runtime_trust_governance_violations(tmp_path):
    log = V27EventLog(tmp_path)
    shared_hash = sha256_hex({"evidence": "same"})
    append_runtime_spec_assertion(log, failure_action="ignore")
    append_minimum_viable_trust_boundary(
        log,
        trusted_inputs=["entry_quote", "telegram_anchor"],
        untrusted_inputs=["entry_quote", "mark_only_peak"],
        required_contracts=[],
        failure_action="none",
    )
    append_evidence_conflict(log, evidence_a_hash=shared_hash, evidence_b_hash=shared_hash)
    append_evidence_aging(log, max_age_ms=60_000, age_ms=90_000)
    append_market_regime_invalidates_evidence(log, action="ignore")
    append_source_alpha_decay_exit_criteria(log, alpha_metric=0.02, exit_threshold=0.05, action="hold")
    append_false_negative_budget(log, allowed_false_negative_rate=0.10, observed_rate=0.30)
    append_small_sample_decision(log, sample_size=12, min_sample_size=30, decision_allowed=True)
    append_safety_vs_capture_tradeoff(log, safety_metric=-0.01, chosen_policy="capture_only")
    append_implementation_drift_monitor(log, drift_detected=True)

    projection = build_denominator_projection(tmp_path)

    assert projection["health"]["runtime_spec_assertion_ok"] is False
    assert projection["health"]["minimum_viable_trust_boundary_ok"] is False
    assert projection["health"]["evidence_conflict_ok"] is False
    assert projection["health"]["evidence_aging_ok"] is False
    assert projection["health"]["market_regime_invalidates_evidence_ok"] is False
    assert projection["health"]["source_alpha_decay_exit_criteria_ok"] is False
    assert projection["health"]["false_negative_budget_ok"] is False
    assert projection["health"]["small_sample_decision_ok"] is False
    assert projection["health"]["safety_vs_capture_tradeoff_ok"] is False
    assert projection["health"]["implementation_drift_monitor_ok"] is False
    assert projection["contract_evidence"]["RuntimeSpecAssertionContract"]["runtime_spec_assertion_violation_count"] == 1
    assert (
        projection["contract_evidence"]["MinimumViableTrustBoundary"][
            "minimum_viable_trust_boundary_violation_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["EvidenceConflictContract"]["evidence_conflict_violation_count"] == 1
    assert projection["contract_evidence"]["EvidenceAgingContract"]["evidence_aging_violation_count"] == 1
    assert (
        projection["contract_evidence"]["MarketRegimeInvalidatesEvidence"][
            "market_regime_invalidates_evidence_violation_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["SourceAlphaDecayExitCriteria"][
            "source_alpha_decay_exit_criteria_violation_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["FalseNegativeBudgetContract"]["false_negative_budget_violation_count"] == 1
    assert projection["contract_evidence"]["SmallSampleDecisionPolicy"]["small_sample_decision_violation_count"] == 1
    assert (
        projection["contract_evidence"]["SafetyVsCaptureTradeoffContract"][
            "safety_vs_capture_tradeoff_violation_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["ImplementationDriftMonitor"][
            "implementation_drift_monitor_violation_count"
        ]
        == 1
    )


def test_denominator_projection_consumes_assumption_priority_escalation_governance_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_assumption_registry(log)
    append_assumption_invalidation_trigger(log)
    append_contract_priority_graph(log)
    append_contract_conflict_resolution(log)
    append_contract_failure_blast_radius(log)
    append_dashboard_triage_workflow(log)
    append_issue_escalation_from_metrics(log)
    append_promotion_evidence_package(log)
    append_regression_budget(log)
    append_root_cause_taxonomy_versioning(log)

    projection = build_denominator_projection(tmp_path)

    assert projection["assumption_registry_recorded_events"] == 1
    assert projection["assumption_invalidation_trigger_recorded_events"] == 1
    assert projection["contract_priority_graph_recorded_events"] == 1
    assert projection["contract_conflict_resolution_recorded_events"] == 1
    assert projection["contract_failure_blast_radius_recorded_events"] == 1
    assert projection["dashboard_triage_workflow_recorded_events"] == 1
    assert projection["issue_escalation_from_metrics_recorded_events"] == 1
    assert projection["promotion_evidence_package_recorded_events"] == 1
    assert projection["regression_budget_recorded_events"] == 1
    assert projection["root_cause_taxonomy_versioning_recorded_events"] == 1
    assert projection["health"]["assumption_registry_ok"] is True
    assert projection["health"]["assumption_invalidation_trigger_ok"] is True
    assert projection["health"]["contract_priority_graph_ok"] is True
    assert projection["health"]["contract_conflict_resolution_ok"] is True
    assert projection["health"]["contract_failure_blast_radius_ok"] is True
    assert projection["health"]["dashboard_triage_workflow_ok"] is True
    assert projection["health"]["issue_escalation_from_metrics_ok"] is True
    assert projection["health"]["promotion_evidence_package_ok"] is True
    assert projection["health"]["regression_budget_ok"] is True
    assert projection["health"]["root_cause_taxonomy_versioning_ok"] is True
    assert projection["contract_evidence"]["AssumptionRegistryContract"]["valid_assumption_registry_count"] == 1
    assert (
        projection["contract_evidence"]["AssumptionInvalidationTrigger"][
            "valid_assumption_invalidation_trigger_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["ContractPriorityGraph"]["valid_contract_priority_graph_count"] == 1
    assert (
        projection["contract_evidence"]["ContractConflictResolutionContract"][
            "valid_contract_conflict_resolution_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["ContractFailureBlastRadius"][
            "valid_contract_failure_blast_radius_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["DashboardTriageWorkflowContract"][
            "valid_dashboard_triage_workflow_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["IssueEscalationFromMetricsContract"][
            "valid_issue_escalation_from_metrics_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["PromotionEvidencePackageContract"][
            "valid_promotion_evidence_package_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["RegressionBudgetContract"]["valid_regression_budget_count"] == 1
    assert (
        projection["contract_evidence"]["RootCauseTaxonomyVersioning"][
            "valid_root_cause_taxonomy_versioning_count"
        ]
        == 1
    )


def test_denominator_projection_rejects_assumption_priority_escalation_governance_violations(tmp_path):
    log = V27EventLog(tmp_path)
    append_assumption_registry(log, evidence_link="none", expires_at="not-a-time")
    append_assumption_invalidation_trigger(log, threshold=0.25, observed_value=0.10)
    append_contract_priority_graph(
        log,
        higher_priority_contract="SafetyCaseContract",
        lower_priority_contract="SafetyCaseContract",
        cycle_detected=True,
    )
    append_contract_conflict_resolution(log, resolution_action="ignore")
    append_contract_failure_blast_radius(log, blast_radius="unbounded", affected_modes=[], fallback_action="warn_only")
    append_dashboard_triage_workflow(log, next_action="tbd", due_at="not-a-time")
    append_issue_escalation_from_metrics(log, threshold="not-a-number", issue_id="missing")
    append_promotion_evidence_package(log, evidence_hash="not-a-hash", approval_status="pending")
    append_regression_budget(log, allowed_regression=0.01, observed_regression=0.08)
    append_root_cause_taxonomy_versioning(log, severity="unknown", migration_policy="none", effective_at="not-a-time")

    projection = build_denominator_projection(tmp_path)

    assert projection["health"]["assumption_registry_ok"] is False
    assert projection["health"]["assumption_invalidation_trigger_ok"] is False
    assert projection["health"]["contract_priority_graph_ok"] is False
    assert projection["health"]["contract_conflict_resolution_ok"] is False
    assert projection["health"]["contract_failure_blast_radius_ok"] is False
    assert projection["health"]["dashboard_triage_workflow_ok"] is False
    assert projection["health"]["issue_escalation_from_metrics_ok"] is False
    assert projection["health"]["promotion_evidence_package_ok"] is False
    assert projection["health"]["regression_budget_ok"] is False
    assert projection["health"]["root_cause_taxonomy_versioning_ok"] is False
    assert projection["contract_evidence"]["AssumptionRegistryContract"]["assumption_registry_violation_count"] == 1
    assert (
        projection["contract_evidence"]["AssumptionInvalidationTrigger"][
            "assumption_invalidation_trigger_violation_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["ContractPriorityGraph"]["contract_priority_graph_violation_count"] == 1
    assert (
        projection["contract_evidence"]["ContractConflictResolutionContract"][
            "contract_conflict_resolution_violation_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["ContractFailureBlastRadius"][
            "contract_failure_blast_radius_violation_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["DashboardTriageWorkflowContract"][
            "dashboard_triage_workflow_violation_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["IssueEscalationFromMetricsContract"][
            "issue_escalation_from_metrics_violation_count"
        ]
        == 1
    )
    assert (
        projection["contract_evidence"]["PromotionEvidencePackageContract"][
            "promotion_evidence_package_violation_count"
        ]
        == 1
    )
    assert projection["contract_evidence"]["RegressionBudgetContract"]["regression_budget_violation_count"] == 1
    assert (
        projection["contract_evidence"]["RootCauseTaxonomyVersioning"][
            "root_cause_taxonomy_versioning_violation_count"
        ]
        == 1
    )


def test_denominator_projection_consumes_randomness_control_contract(tmp_path):
    log = V27EventLog(tmp_path)
    append_randomness_control(log)

    projection = build_denominator_projection(tmp_path)
    evidence = projection["contract_evidence"]["RandomnessControlContract"]

    assert projection["randomness_control_recorded_events"] == 1
    assert projection["health"]["randomness_control_ok"] is True
    assert evidence["eligible_randomness_control_records"] == 1
    assert evidence["valid_randomness_control_count"] == 1
    assert evidence["malformed_count"] == 0
    assert evidence["randomness_control_violation_count"] == 0
    assert evidence["rng_versions"] == ["v2.7.0.randomness_control.v1"]
    assert evidence["randomization_units"] == ["normal_tiny_promotion_policy"]


def test_denominator_projection_rejects_malformed_randomness_control_contract(tmp_path):
    log = V27EventLog(tmp_path)
    append_randomness_control(log, missing_seed=True, bad_hash=True)

    projection = build_denominator_projection(tmp_path)
    evidence = projection["contract_evidence"]["RandomnessControlContract"]

    assert projection["health"]["randomness_control_ok"] is False
    assert evidence["valid_randomness_control_count"] == 0
    assert evidence["malformed_count"] == 1
    assert evidence["randomness_control_violation_count"] == 1
    assert evidence["malformed_randomness_controls"][0]["missing_fields"] == ["rng_seed"]
    assert evidence["randomness_control_violations"][0]["violation_fields"] == ["assignment_hash_sha256"]


def test_denominator_projection_uses_latest_randomness_control_assignment(tmp_path):
    log = V27EventLog(tmp_path)
    append_randomness_control(log, assignment_id="candidate-repair", missing_seed=True, bad_hash=True, idempotency_suffix="bad")
    append_randomness_control(log, assignment_id="candidate-repair", idempotency_suffix="good")

    projection = build_denominator_projection(tmp_path)
    evidence = projection["contract_evidence"]["RandomnessControlContract"]

    assert projection["randomness_control_recorded_events"] == 2
    assert projection["health"]["randomness_control_ok"] is True
    assert evidence["randomness_control_observation_count"] == 2
    assert evidence["current_randomness_control_count"] == 1
    assert evidence["superseded_randomness_control_event_count"] == 1
    assert evidence["valid_randomness_control_count"] == 1
    assert evidence["malformed_count"] == 0
    assert evidence["randomness_control_violation_count"] == 0


def test_denominator_projection_consumes_deployment_and_worker_fleet_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_deployment_rollout(log)
    append_worker_fleet_heartbeat(log, worker_id="dashboard")
    append_worker_fleet_heartbeat(log, worker_id="v27-read-model-refresh")

    projection = build_denominator_projection(tmp_path)
    deployment = projection["contract_evidence"]["DeploymentRolloutStateMachine"]
    fleet = projection["contract_evidence"]["WorkerFleetConsistencyContract"]

    assert projection["deployment_rollout_state_recorded_events"] == 1
    assert projection["worker_fleet_heartbeat_recorded_events"] == 2
    assert projection["health"]["deployment_rollout_state_machine_ok"] is True
    assert projection["health"]["worker_fleet_consistency_ok"] is True
    assert deployment["valid_deployment_rollout_count"] == 1
    assert deployment["deployment_rollout_violation_count"] == 0
    assert fleet["valid_worker_fleet_count"] == 2
    assert fleet["worker_fleet_violation_count"] == 0
    assert fleet["build_hashes"] == ["build-a"]
    assert fleet["runtime_config_hashes"] == ["config-a"]


def test_denominator_projection_rejects_bad_rollout_and_mixed_worker_fleet(tmp_path):
    log = V27EventLog(tmp_path)
    append_deployment_rollout(log, state="deploying", canary_status="failed", fleet_hash_map={})
    append_worker_fleet_heartbeat(log, worker_id="dashboard", build_hash="build-a")
    append_worker_fleet_heartbeat(log, worker_id="v27-read-model-refresh", build_hash="build-b")

    projection = build_denominator_projection(tmp_path)
    deployment = projection["contract_evidence"]["DeploymentRolloutStateMachine"]
    fleet = projection["contract_evidence"]["WorkerFleetConsistencyContract"]

    assert projection["health"]["deployment_rollout_state_machine_ok"] is False
    assert projection["health"]["worker_fleet_consistency_ok"] is False
    assert deployment["deployment_rollout_violation_count"] == 1
    assert deployment["deployment_rollout_violations"][0]["violation_fields"] == [
        "canary_status_not_passed",
        "fleet_hash_map_empty",
        "state_not_ready",
    ]
    assert fleet["worker_fleet_violation_count"] == 1
    assert fleet["worker_fleet_violations"] == ["mixed_build_hash"]


def test_denominator_projection_consumes_backup_restore_drill_contract(tmp_path):
    log = V27EventLog(tmp_path)
    append_backup_restore_drill(log)

    projection = build_denominator_projection(tmp_path)
    evidence = projection["contract_evidence"]["BackupRestoreDrillContract"]

    assert projection["backup_restore_drill_recorded_events"] == 1
    assert projection["health"]["backup_restore_drill_ok"] is True
    assert evidence["eligible_backup_restore_drill_records"] == 1
    assert evidence["valid_backup_restore_drill_count"] == 1
    assert evidence["backup_restore_drill_violation_count"] == 0
    assert evidence["malformed_count"] == 0
    assert evidence["restore_statuses"] == ["passed"]


def test_denominator_projection_rejects_invalid_backup_restore_drill(tmp_path):
    log = V27EventLog(tmp_path)
    append_backup_restore_drill(
        log,
        restored_world_hash="not-a-sha",
        restore_started_at="2026-01-15T00:02:00Z",
        restore_completed_at="2026-01-15T00:01:00Z",
        restore_status="failed",
    )

    projection = build_denominator_projection(tmp_path)
    evidence = projection["contract_evidence"]["BackupRestoreDrillContract"]

    assert projection["health"]["backup_restore_drill_ok"] is False
    assert evidence["valid_backup_restore_drill_count"] == 0
    assert evidence["backup_restore_drill_violation_count"] == 1
    assert evidence["backup_restore_drill_violations"][0]["violation_fields"] == [
        "restore_completed_before_started",
        "restore_status_not_passed",
        "restored_world_hash_sha256",
    ]


def test_denominator_projection_consumes_incident_freeze_and_breaker_resume_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_incident_evidence_freeze(log)
    append_circuit_breaker_resume(log)

    projection = build_denominator_projection(tmp_path)
    freeze = projection["contract_evidence"]["IncidentEvidenceFreezeContract"]
    resume = projection["contract_evidence"]["CircuitBreakerResumeContract"]

    assert projection["incident_evidence_freeze_recorded_events"] == 1
    assert projection["circuit_breaker_resume_recorded_events"] == 1
    assert projection["health"]["incident_evidence_freeze_ok"] is True
    assert projection["health"]["circuit_breaker_resume_ok"] is True
    assert freeze["valid_incident_evidence_freeze_count"] == 1
    assert freeze["incident_evidence_freeze_violation_count"] == 0
    assert freeze["freeze_ids"] == ["freeze-unit-v1"]
    assert resume["valid_circuit_breaker_resume_count"] == 1
    assert resume["circuit_breaker_resume_violation_count"] == 0
    assert resume["evidence_freeze_ids"] == ["freeze-unit-v1"]


def test_denominator_projection_rejects_bad_incident_freeze_and_unfrozen_resume(tmp_path):
    log = V27EventLog(tmp_path)
    append_incident_evidence_freeze(
        log,
        frozen_event_range={"start_seq": 42, "end_seq": 1},
        frozen_config_hash="not-a-sha",
        frozen_at="not-a-time",
        freeze_status="open",
    )
    append_circuit_breaker_resume(
        log,
        evidence_freeze_id="missing-freeze",
        root_cause_fixed=False,
        health_checks_passed=False,
        resumed_at="not-a-time",
        resume_status="blocked",
    )

    projection = build_denominator_projection(tmp_path)
    freeze = projection["contract_evidence"]["IncidentEvidenceFreezeContract"]
    resume = projection["contract_evidence"]["CircuitBreakerResumeContract"]

    assert projection["health"]["incident_evidence_freeze_ok"] is False
    assert projection["health"]["circuit_breaker_resume_ok"] is False
    assert freeze["incident_evidence_freeze_violation_count"] == 1
    assert freeze["incident_evidence_freeze_violations"][0]["violation_fields"] == [
        "freeze_status_not_frozen",
        "frozen_at_parseable",
        "frozen_config_hash_sha256",
        "frozen_event_range_inverted",
    ]
    assert resume["circuit_breaker_resume_violation_count"] == 2
    assert resume["circuit_breaker_resume_violations"][0]["violation_fields"] == [
        "health_checks_not_passed",
        "resume_status_not_resumed",
        "resumed_at_parseable",
        "root_cause_not_fixed",
    ]
    assert resume["circuit_breaker_resume_violations"][1]["violation_fields"] == [
        "evidence_freeze_id_not_frozen",
    ]


def test_denominator_projection_consumes_queue_candidate_and_retry_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_queue_durability(log)
    append_candidate_cancellation(log)
    append_retry_storm_control(log)

    projection = build_denominator_projection(tmp_path)
    queue = projection["contract_evidence"]["QueueDurabilityContract"]
    cancellation = projection["contract_evidence"]["CandidateCancellationContract"]
    retry = projection["contract_evidence"]["RetryStormControlContract"]

    assert projection["queue_durability_recorded_events"] == 1
    assert projection["candidate_cancellation_recorded_events"] == 1
    assert projection["retry_storm_control_recorded_events"] == 1
    assert projection["health"]["queue_durability_ok"] is True
    assert projection["health"]["candidate_cancellation_ok"] is True
    assert projection["health"]["retry_storm_control_ok"] is True
    assert queue["valid_queue_durability_count"] == 1
    assert queue["queue_durability_violation_count"] == 0
    assert queue["queue_ids"] == ["entry-queue"]
    assert cancellation["valid_candidate_cancellation_count"] == 1
    assert cancellation["candidate_cancellation_violation_count"] == 0
    assert cancellation["cancel_reasons"] == ["risk_revalidated"]
    assert retry["valid_retry_storm_control_count"] == 1
    assert retry["retry_storm_control_violation_count"] == 0
    assert retry["backoff_policies"] == ["capped_exponential_jitter"]


def test_denominator_projection_rejects_bad_queue_candidate_and_retry_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_queue_durability(log, durable_state="memory_only", ack_state="unknown", created_at="not-a-time")
    append_candidate_cancellation(log, cancel_event_seq=-1, cancelled_at="not-a-time")
    append_retry_storm_control(log, backoff_policy="immediate", max_concurrent_retries=-1, p0_reserved_capacity=-1)

    projection = build_denominator_projection(tmp_path)
    queue = projection["contract_evidence"]["QueueDurabilityContract"]
    cancellation = projection["contract_evidence"]["CandidateCancellationContract"]
    retry = projection["contract_evidence"]["RetryStormControlContract"]

    assert projection["health"]["queue_durability_ok"] is False
    assert projection["health"]["candidate_cancellation_ok"] is False
    assert projection["health"]["retry_storm_control_ok"] is False
    assert queue["queue_durability_violation_count"] == 1
    assert queue["queue_durability_violations"][0]["violation_fields"] == [
        "ack_state_invalid",
        "created_at_parseable",
        "durable_state_not_durable",
    ]
    assert cancellation["candidate_cancellation_violation_count"] == 1
    assert cancellation["candidate_cancellation_violations"][0]["violation_fields"] == [
        "cancel_event_seq_nonnegative",
        "cancelled_at_parseable",
    ]
    assert retry["retry_storm_control_violation_count"] == 1
    assert retry["retry_storm_control_violations"][0]["violation_fields"] == [
        "backoff_policy_not_bounded",
        "max_concurrent_retries_nonnegative",
        "p0_reserved_capacity_nonnegative",
    ]


def test_denominator_projection_consumes_provider_coverage_and_training_serving_skew_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_provider_coverage_map(log)
    append_training_serving_skew(log)

    projection = build_denominator_projection(tmp_path)
    coverage = projection["contract_evidence"]["ProviderCoverageMapContract"]
    skew = projection["contract_evidence"]["TrainingServingSkewContract"]

    assert projection["provider_coverage_map_recorded_events"] == 1
    assert projection["training_serving_skew_recorded_events"] == 1
    assert projection["health"]["provider_coverage_map_ok"] is True
    assert projection["health"]["training_serving_skew_ok"] is True
    assert coverage["eligible_provider_coverage_map_records"] == 1
    assert coverage["valid_provider_coverage_map_count"] == 1
    assert coverage["provider_coverage_map_violation_count"] == 0
    assert coverage["providers"] == ["jupiter_ultra"]
    assert coverage["coverage_statuses"] == ["supported"]
    assert skew["eligible_training_serving_skew_records"] == 1
    assert skew["valid_training_serving_skew_count"] == 1
    assert skew["training_serving_skew_violation_count"] == 0
    assert skew["normalization_versions"] == ["norm-v1"]
    assert skew["skew_check_results"] == ["pass"]


def test_denominator_projection_rejects_bad_provider_coverage_and_training_serving_skew_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    append_provider_coverage_map(log, provider="", coverage_status="ambiguous", unsupported_reason="", checked_at="not-a-time")
    append_training_serving_skew(
        log,
        training_feature_code_hash="not-a-sha",
        serving_feature_code_hash="also-not-a-sha",
        skew_check_result="fail",
        checked_at="not-a-time",
    )

    projection = build_denominator_projection(tmp_path)
    coverage = projection["contract_evidence"]["ProviderCoverageMapContract"]
    skew = projection["contract_evidence"]["TrainingServingSkewContract"]

    assert projection["health"]["provider_coverage_map_ok"] is False
    assert projection["health"]["training_serving_skew_ok"] is False
    assert coverage["valid_provider_coverage_map_count"] == 0
    assert coverage["malformed_count"] == 1
    assert coverage["provider_coverage_map_violation_count"] == 1
    assert coverage["provider_coverage_map_violations"][0]["missing_fields"] == ["provider", "unsupported_reason"]
    assert coverage["provider_coverage_map_violations"][0]["violation_fields"] == [
        "checked_at_parseable",
        "coverage_status_unknown",
    ]
    assert skew["valid_training_serving_skew_count"] == 0
    assert skew["training_serving_skew_violation_count"] == 1
    assert skew["training_serving_skew_violations"][0]["violation_fields"] == [
        "checked_at_parseable",
        "serving_feature_code_hash_sha256",
        "skew_check_result_not_passed",
        "training_feature_code_hash_sha256",
    ]


def test_denominator_projection_consumes_idempotency_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags["realtime_clean"] = False
    append_decision(log, decision_id=1, token_ca="TokenIdem", captured=True, **flags)
    append_realtime_clean(log, token_ca="TokenIdem")
    append_quote_intent_binding(log, token_ca="TokenIdem")
    append_idempotency_contract(log, token_ca="TokenIdem")

    projection = build_denominator_projection(tmp_path, include_records=True)
    idempotency = projection["contract_evidence"]["IdempotencyContract"]
    namespace = projection["contract_evidence"]["IdempotencyKeyNamespaceContract"]

    assert projection["idempotency_contract_recorded_events"] == 1
    assert projection["health"]["idempotency_contract_ok"] is True
    assert projection["health"]["idempotency_key_namespace_ok"] is True
    assert idempotency["eligible_idempotency_records"] == 1
    assert idempotency["idempotency_observation_count"] == 1
    assert idempotency["malformed_count"] == 0
    assert idempotency["idempotency_collision_count"] == 0
    assert idempotency["duplicate_action_conflict_count"] == 0
    assert namespace["eligible_namespace_records"] == 1
    assert namespace["malformed_count"] == 0
    assert namespace["namespace_policy_violation_count"] == 0
    assert namespace["namespaces"] == ["paper_entry_execution"]
    assert projection["records"][0]["idempotency_contract"]["action"] == "paper_entry"


def test_idempotency_allows_distinct_decisions_on_same_lifecycle_action(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags["realtime_clean"] = False
    append_decision(log, decision_id=1, token_ca="TokenMultiEntry", captured=True, **flags)
    lifecycle_key = "solana:TokenMultiEntry:pool-a:0:premium-signal-1"
    append_idempotency_contract(
        log,
        token_ca="TokenMultiEntry",
        paper_trade_id=1,
        token_lifecycle_key=lifecycle_key,
    )
    append_idempotency_contract(
        log,
        token_ca="TokenMultiEntry",
        paper_trade_id=2,
        token_lifecycle_key=lifecycle_key,
    )

    projection = build_denominator_projection(tmp_path, include_records=True)
    idempotency = projection["contract_evidence"]["IdempotencyContract"]

    assert projection["health"]["idempotency_contract_ok"] is True
    assert idempotency["idempotency_observation_count"] == 2
    assert idempotency["duplicate_action_conflict_key"] == "environment_id:namespace:decision_id:action"
    assert idempotency["duplicate_action_conflict_count"] == 0


def test_idempotency_rejects_same_decision_action_with_multiple_executions(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags["realtime_clean"] = False
    append_decision(log, decision_id=1, token_ca="TokenDuplicateDecision", captured=True, **flags)
    decision_id = "paper_trade:shared:entry_decision"
    lifecycle_key = "solana:TokenDuplicateDecision:pool-a:0:premium-signal-1"
    append_idempotency_contract(
        log,
        token_ca="TokenDuplicateDecision",
        paper_trade_id=1,
        decision_id=decision_id,
        execution_id="paper_trade:1:entry_execution",
        token_lifecycle_key=lifecycle_key,
    )
    append_idempotency_contract(
        log,
        token_ca="TokenDuplicateDecision",
        paper_trade_id=2,
        decision_id=decision_id,
        execution_id="paper_trade:2:entry_execution",
        token_lifecycle_key=lifecycle_key,
    )

    projection = build_denominator_projection(tmp_path, include_records=True)
    idempotency = projection["contract_evidence"]["IdempotencyContract"]

    assert projection["health"]["idempotency_contract_ok"] is False
    assert idempotency["duplicate_action_conflict_count"] == 1
    assert idempotency["duplicate_action_conflicts"][0]["decision_id"] == decision_id
    assert idempotency["duplicate_action_conflicts"][0]["existing_execution_id"] == "paper_trade:1:entry_execution"
    assert idempotency["duplicate_action_conflicts"][0]["incoming_execution_id"] == "paper_trade:2:entry_execution"


def test_denominator_projection_consumes_execution_control_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags["realtime_clean"] = False
    append_decision(log, decision_id=1, token_ca="TokenLease", captured=True, **flags)
    append_realtime_clean(log, token_ca="TokenLease")
    append_quote_intent_binding(log, token_ca="TokenLease")
    append_idempotency_contract(log, token_ca="TokenLease")
    append_execution_control(log, token_ca="TokenLease")

    projection = build_denominator_projection(tmp_path, include_records=True)
    lease = projection["contract_evidence"]["ExecutionLeaseContract"]
    fencing = projection["contract_evidence"]["StateVersionFencing"]
    state_machine = projection["contract_evidence"]["EntryExecutionStateMachine"]

    assert projection["execution_control_recorded_events"] == 1
    assert projection["health"]["execution_lease_ok"] is True
    assert projection["health"]["state_version_fencing_ok"] is True
    assert projection["health"]["entry_execution_state_machine_ok"] is True
    assert lease["eligible_execution_lease_records"] == 1
    assert lease["malformed_count"] == 0
    assert lease["lease_violation_count"] == 0
    assert fencing["eligible_state_fencing_records"] == 1
    assert fencing["fencing_violation_count"] == 0
    assert fencing["requires_revalidation_count"] == 1
    assert state_machine["eligible_entry_execution_records"] == 1
    assert state_machine["terminal_state_count"] == 1
    assert state_machine["state_machine_violation_count"] == 0
    assert projection["records"][0]["execution_control"]["state"] == "filled_paper"


def test_denominator_projection_consumes_paper_ledger_contracts(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags["realtime_clean"] = False
    append_decision(log, decision_id=1, token_ca="TokenLedger", captured=True, **flags)
    append_realtime_clean(log, token_ca="TokenLedger")
    append_quote_intent_binding(log, token_ca="TokenLedger")
    append_idempotency_contract(log, token_ca="TokenLedger")
    append_execution_control(log, token_ca="TokenLedger")
    append_paper_ledger(log, token_ca="TokenLedger")

    projection = build_denominator_projection(tmp_path, include_records=True)
    position = projection["contract_evidence"]["PaperPositionLedgerContract"]
    capital = projection["contract_evidence"]["PaperCapitalLedgerContract"]
    double_entry = projection["contract_evidence"]["DoubleEntryLedgerInvariantContract"]
    reservation = projection["contract_evidence"]["CapitalReservationPolicy"]

    assert projection["paper_ledger_recorded_events"] == 1
    assert projection["health"]["paper_position_ledger_ok"] is True
    assert projection["health"]["paper_capital_ledger_ok"] is True
    assert projection["health"]["double_entry_ledger_invariant_ok"] is True
    assert projection["health"]["capital_reservation_policy_ok"] is True
    assert position["eligible_position_ledger_records"] == 1
    assert position["position_ledger_violation_count"] == 0
    assert capital["eligible_capital_ledger_records"] == 1
    assert capital["capital_ledger_violation_count"] == 0
    assert double_entry["eligible_double_entry_records"] == 1
    assert double_entry["invariant_violation_count"] == 0
    assert reservation["eligible_reservation_records"] == 1
    assert reservation["reservation_policy_violation_count"] == 0
    assert projection["records"][0]["paper_ledger_contract"]["position_status"] == "closed"


def test_denominator_projection_consumes_no_fill_and_recovery_controls(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags["realtime_clean"] = False
    append_decision(log, decision_id=1, token_ca="TokenRecovery", captured=True, **flags)
    append_execution_control(log, token_ca="TokenRecovery")
    append_no_fill_outcome(log, token_ca="TokenRecovery", outcome_state="no_fill")
    append_runtime_recovery_control(log)

    projection = build_denominator_projection(tmp_path, include_records=True)
    no_fill = projection["contract_evidence"]["NoFillOutcome"]
    recovery = projection["contract_evidence"]["CrashRecoveryStateMachine"]
    resume = projection["contract_evidence"]["ResumeDrainPolicy"]

    assert projection["no_fill_outcome_recorded_events"] == 1
    assert projection["runtime_recovery_control_recorded_events"] == 1
    assert projection["health"]["no_fill_outcome_ok"] is True
    assert projection["health"]["crash_recovery_state_machine_ok"] is True
    assert projection["health"]["resume_drain_policy_ok"] is True
    assert no_fill["eligible_no_fill_records"] == 1
    assert no_fill["no_fill_terminal_count"] == 1
    assert no_fill["no_fill_outcome_violation_count"] == 0
    assert recovery["eligible_recovery_records"] == 1
    assert recovery["recovery_violation_count"] == 0
    assert resume["eligible_resume_drain_records"] == 1
    assert resume["resume_drain_violation_count"] == 0
    assert projection["records"][0]["no_fill_outcome"]["no_fill_reason"] == "quote_unavailable"


def test_denominator_projection_keeps_standalone_no_fill_outcomes_out_of_denominator_records(tmp_path):
    log = V27EventLog(tmp_path)
    append_no_fill_outcome(log, token_ca="TokenStandalone", outcome_state="no_fill")

    projection = build_denominator_projection(tmp_path, include_records=True)

    no_fill = projection["contract_evidence"]["NoFillOutcome"]
    assert projection["no_fill_outcome_recorded_events"] == 1
    assert projection["metrics"]["denominator_seed_records"] == 0
    assert no_fill["eligible_no_fill_records"] == 1
    assert no_fill["standalone_no_fill_outcome_count"] == 1
    assert projection["records"] == []


def test_denominator_read_model_snapshot_pins_freshness_and_spec_hash(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id=1, token_ca="TokenA", captured=True, **FULL_D3B_FLAGS)

    projection = build_denominator_projection(tmp_path, include_records=True)
    snapshot = build_denominator_read_model_snapshot(
        projection,
        max_allowed_lag_seq=0,
        max_allowed_lag_ms=300_000,
    )

    assert projection["event_log_verify_mode"] == "cached_state_metadata"
    assert snapshot["snapshot_schema_version"] == "v2.7.0.denominator_read_model.v1"
    assert snapshot["snapshot_id"].startswith("v27denom_")
    assert len(snapshot["projection_hash"]) == 64
    assert len(snapshot["snapshot_hash"]) == 64
    assert snapshot["spec"]["spec_version"] == "2.7.0"
    assert len(snapshot["spec"]["spec_hash"]) == 64
    assert snapshot["read_model"]["event_log_latest_seq"] == 1
    assert snapshot["read_model"]["read_model_seq"] == 1
    assert snapshot["read_model"]["lag_seq"] == 0
    assert snapshot["read_model"]["read_model_fresh_enough"] is True
    assert snapshot["health"]["status"] == "snapshot_ready"


def test_denominator_read_model_snapshot_blocks_stale_seq(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id=1, token_ca="TokenA", captured=True, **FULL_D3B_FLAGS)
    append_decision(log, decision_id=2, token_ca="TokenB", captured=True, **FULL_D3B_FLAGS)

    projection = build_denominator_projection(tmp_path, include_records=False)
    snapshot = build_denominator_read_model_snapshot(
        projection,
        max_allowed_lag_seq=0,
        read_model_seq=1,
    )

    assert snapshot["read_model"]["event_log_latest_seq"] == 2
    assert snapshot["read_model"]["read_model_seq"] == 1
    assert snapshot["read_model"]["lag_seq"] == 1
    assert snapshot["read_model"]["read_model_fresh_enough"] is False
    assert snapshot["read_model"]["staleness_reasons"] == ["read_model_seq_lag"]
    assert snapshot["health"]["status"] == "snapshot_not_ready"


def test_denominator_read_model_snapshot_does_not_treat_quiescent_log_as_stale(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id=1, token_ca="TokenA", captured=True, **FULL_D3B_FLAGS)

    projection = build_denominator_projection(tmp_path, include_records=False)
    snapshot = build_denominator_read_model_snapshot(
        projection,
        max_allowed_lag_seq=0,
        max_allowed_lag_ms=1,
        now_iso="2099-01-01T00:00:00Z",
    )

    assert snapshot["read_model"]["event_log_latest_seq"] == 1
    assert snapshot["read_model"]["read_model_seq"] == 1
    assert snapshot["read_model"]["lag_seq"] == 0
    assert snapshot["read_model"]["lag_ms"] == 0
    assert snapshot["read_model"]["read_model_fresh_enough"] is True
    assert snapshot["read_model"]["staleness_reasons"] == []


def test_denominator_read_model_snapshot_blocks_old_unprocessed_event_lag(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id=1, token_ca="TokenA", captured=True, **FULL_D3B_FLAGS)
    append_decision(log, decision_id=2, token_ca="TokenB", captured=True, **FULL_D3B_FLAGS)

    projection = build_denominator_projection(tmp_path, include_records=False)
    snapshot = build_denominator_read_model_snapshot(
        projection,
        max_allowed_lag_seq=10,
        max_allowed_lag_ms=1,
        read_model_seq=1,
        now_iso="2099-01-01T00:00:00Z",
    )

    assert snapshot["read_model"]["event_log_latest_seq"] == 2
    assert snapshot["read_model"]["read_model_seq"] == 1
    assert snapshot["read_model"]["lag_seq"] == 1
    assert snapshot["read_model"]["lag_ms"] > 1
    assert snapshot["read_model"]["read_model_fresh_enough"] is False
    assert snapshot["read_model"]["staleness_reasons"] == ["read_model_time_lag"]


def test_denominator_projection_marks_source_label_conflict_dirty(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id=1, token_ca="TokenA", source_dog_label="gold", **FULL_D3B_FLAGS)
    append_decision(log, decision_id=2, token_ca="TokenA", source_dog_label="silver", **FULL_D3B_FLAGS)

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["health"]["denominator_clean"] is False
    assert projection["dirty_records"] == [
        {
            "denominator_dedup_key": "solana:TokenA:pool-a:0",
            "reasons": ["source_label_conflict"],
        }
    ]
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 0
    assert projection["records"][0]["source_label_conflicts"][0]["incoming"] == "silver"


def test_denominator_projection_reports_missing_token_and_evidence_gaps(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id=1, token_ca=None, source_dog_label="gold", **FULL_D3B_FLAGS)
    append_decision(log, decision_id=2, token_ca="TokenPartial", source_dog_label=None)

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["health"]["denominator_clean"] is False
    assert projection["dirty_events"][0]["reason"] == "missing_token_ca"
    assert projection["health"]["status"] == "seed_partial_dirty_events"
    assert projection["metrics"]["denominator_seed_records"] == 1
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 0
    assert projection["evidence_gaps"]["TokenIdentityContract"] == 1
    assert projection["evidence_gaps"]["SourceDogLabelContract"] == 1
    assert projection["evidence_gaps"]["RealtimeCleanDetector"] == 1
    assert "SourceDogLabelContract" in projection["records"][0]["missing_evidence"]


def test_denominator_projection_consumes_missed_attribution_seed_without_overclaiming_d2(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="paper_missed_signal_attribution_recorded",
        aggregate_id="paper_missed:token:TokenMiss",
        idempotency_key="paper_missed_signal_attribution:1",
        payload={
            "missed_attribution_id": 1,
            "decision_event_id": 10,
            "token_ca": "TokenMiss",
            "symbol": "MISS",
            "signal_id": 77,
            "signal_ts": 1_700_000_000,
            "route": "LOTTO",
            "component": "upstream_gate",
            "legacy_event_type": "missed_signal_attribution",
            "decision": "skip",
            "reason": "tracking_ttl_expired",
            "source_dog_label": "silver",
            "source_dog_label_version": "legacy_missed_attribution_seed_v0.1",
            "source_label_research_only": True,
            "telegram_seen": True,
            "realtime_observable": True,
            "baseline_price": 0.001,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["mirrored_missed_attribution_events"] == 1
    assert projection["metrics"]["denominator_seed_records"] == 1
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 1
    assert projection["metrics"]["telegram_realtime_observable_gold_silver_D1"] == 1
    assert projection["metrics"]["telegram_realtime_clean_gold_silver_D2"] == 0
    assert projection["metrics"]["telegram_externally_actionable_gold_silver_D3a"] == 0
    assert projection["evidence_gaps"]["RealtimeCleanDetector"] == 1
    assert projection["evidence_gaps"]["ExAnteFeasibility"] == 1
    assert projection["records"][0]["source_dog_label"] == "silver"


def test_denominator_projection_consumes_ex_ante_feasibility_contract(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags.pop("ex_ante_feasible")
    append_decision(log, decision_id=1, token_ca="TokenFeas", captured=True, **flags)
    append_ex_ante(log, token_ca="TokenFeas")

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["ex_ante_feasibility_recorded_events"] == 1
    assert projection["health"]["ex_ante_feasibility_ok"] is True
    assert projection["contract_evidence"]["ExAnteFeasibility"]["eligible_ex_ante_records"] == 1
    assert projection["contract_evidence"]["ExAnteFeasibility"]["ex_ante_feasible_count"] == 1
    assert projection["contract_evidence"]["ExAnteFeasibility"]["future_leakage_count"] == 0
    assert projection["metrics"]["telegram_externally_actionable_gold_silver_D3a"] == 1
    record = projection["records"][0]
    assert record["ex_ante_feasible"] is True
    assert record["ex_ante_feasibility_contract"]["feasibility_policy_version"] == "legacy_actual_paper_entry_feasibility_v0.1"


def test_denominator_projection_rejects_ex_ante_future_leakage(tmp_path):
    log = V27EventLog(tmp_path)
    flags = dict(FULL_D3B_FLAGS)
    flags.pop("ex_ante_feasible")
    append_decision(log, decision_id=1, token_ca="TokenLeak", captured=True, **flags)
    append_ex_ante(log, token_ca="TokenLeak", used_future_peak=True)

    projection = build_denominator_projection(tmp_path, include_records=True)

    evidence = projection["contract_evidence"]["ExAnteFeasibility"]
    assert projection["health"]["ex_ante_feasibility_ok"] is False
    assert evidence["future_leakage_count"] == 1
    assert evidence["future_leakage"][0]["leakage_fields"] == ["used_future_peak"]
    assert projection["records"][0]["ex_ante_feasible"] is False
    assert projection["metrics"]["telegram_externally_actionable_gold_silver_D3a"] == 0


def test_denominator_projection_consumes_earliest_actionable_time_contract(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id=1, token_ca="TokenAct", captured=True, **FULL_D3B_FLAGS)
    append_earliest_actionable(log, token_ca="TokenAct")

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["earliest_actionable_time_recorded_events"] == 1
    assert projection["health"]["earliest_actionable_time_ok"] is True
    evidence = projection["contract_evidence"]["EarliestActionableTime"]
    assert evidence["eligible_earliest_actionable_records"] == 1
    assert evidence["actionable_before_peak_count"] == 1
    assert evidence["malformed_count"] == 0
    assert evidence["invariant_violation_count"] == 0
    assert evidence["peak_ts_qualities"] == ["legacy_outcome_window_close_proxy"]
    record = projection["records"][0]
    assert record["earliest_actionable_time"]["earliest_actionable_policy_version"] == "legacy_actual_paper_entry_actionable_time_v0.1"


def test_denominator_projection_rejects_earliest_actionable_invariant_violation(tmp_path):
    log = V27EventLog(tmp_path)
    append_decision(log, decision_id=1, token_ca="TokenLate", captured=True, **FULL_D3B_FLAGS)
    append_earliest_actionable(log, token_ca="TokenLate", entry_after_peak=True)

    projection = build_denominator_projection(tmp_path, include_records=True)

    evidence = projection["contract_evidence"]["EarliestActionableTime"]
    assert projection["health"]["earliest_actionable_time_ok"] is False
    assert evidence["actionable_before_peak_count"] == 0
    assert evidence["invariant_violation_count"] == 1
    assert evidence["invariant_violations"][0]["invariant_violations"] == [
        "counterfactual_entry_after_peak",
        "not_actionable_before_peak",
    ]


def test_denominator_projection_merges_telegram_signal_anchor_with_missed_label_seed(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenMerge:unknown_pool:0",
        idempotency_key="premium_signals:1",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenMerge",
            "symbol": "MERGE",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
            "realtime_observable_quality": "realtime_seed",
            "signal_type": "NOT_ATH",
        },
    )
    log.append_event(
        event_type="paper_missed_signal_attribution_recorded",
        aggregate_id="paper_missed:token:TokenMerge",
        idempotency_key="paper_missed_signal_attribution:1",
        payload={
            "missed_attribution_id": 1,
            "token_ca": "TokenMerge",
            "symbol": "MERGE",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "gold",
            "source_dog_label_version": "legacy_missed_attribution_seed_v0.1",
            "source_label_research_only": True,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["telegram_signal_seen_events"] == 1
    assert projection["mirrored_missed_attribution_events"] == 1
    assert projection["metrics"]["denominator_seed_records"] == 1
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 1
    assert projection["metrics"]["telegram_realtime_observable_gold_silver_D1"] == 1
    assert projection["metrics"]["telegram_realtime_clean_gold_silver_D2"] == 0
    assert projection["records"][0]["merged_decision_event_ids"] == [None, None]
    assert projection["records"][0]["source_dog_label"] == "gold"


def test_denominator_projection_does_not_count_backfilled_signal_as_d1(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenBackfill:unknown_pool:0",
        idempotency_key="premium_signals:1",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenBackfill",
            "symbol": "BACK",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": False,
            "realtime_observable_quality": "backfilled",
            "source_dog_label": "silver",
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 1
    assert projection["metrics"]["telegram_realtime_observable_gold_silver_D1"] == 0
    assert projection["records"][0]["realtime_observable"] is False


def test_denominator_projection_merges_telegram_signal_with_source_label_event(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenSource:unknown_pool:0",
        idempotency_key="premium_signals:1",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenSource",
            "symbol": "SRC",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenSource:unknown_pool:0",
        idempotency_key="signal_features_source_label:1",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenSource",
            "symbol": "SRC",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "silver",
            "source_dog_label_version": "legacy_signal_features_seed_v0.1",
            "source_label_research_only": True,
            "source_reference_price_type": "legacy_entry_price",
            "source_reference_price": 0.001,
            "source_label_window": "24h",
            "source_label_available_at": "2026-01-15T00:00:00Z",
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["telegram_signal_seen_events"] == 1
    assert projection["source_dog_label_events"] == 1
    assert projection["metrics"]["denominator_seed_records"] == 1
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 1
    assert projection["metrics"]["telegram_realtime_observable_gold_silver_D1"] == 1
    assert projection["metrics"]["telegram_realtime_clean_gold_silver_D2"] == 0
    assert projection["evidence_gaps"]["ProductionSourceDogLabelContract"] == 1
    assert projection["records"][0]["source_label_research_only"] is True
    assert projection["records"][0]["signal_credit_assignment"]["credited_signal_id"] == 1
    assert projection["records"][0]["reference_price_contract"]["reference_price_type"] == "legacy_entry_price"
    assert projection["contract_evidence"]["SignalCreditAssignmentContract"]["missing_count"] == 0
    assert projection["contract_evidence"]["ReferencePriceContract"]["missing_count"] == 0
    assert projection["contract_evidence"]["MetricsWindowContract"]["metrics_window_valid"] is True
    assert projection["health"]["signal_credit_assignment_ok"] is True
    assert projection["health"]["reference_price_ok"] is True
    assert projection["health"]["metrics_window_ok"] is True


def test_denominator_projection_blocks_shadow_contracts_when_no_d0_records(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenOnlyLabel:unknown_pool:0",
        idempotency_key="signal_features_source_label:1",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenOnlyLabel",
            "symbol": "ONLY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "gold",
            "source_label_research_only": True,
            "source_reference_price_type": "legacy_entry_price",
            "source_reference_price": 0.001,
            "source_label_available_at": "2026-01-15T00:00:00Z",
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 0
    assert projection["contract_evidence"]["SignalCreditAssignmentContract"]["eligible_d0_records"] == 0
    assert projection["contract_evidence"]["ReferencePriceContract"]["eligible_d0_records"] == 0
    assert projection["health"]["signal_credit_assignment_ok"] is False
    assert projection["health"]["reference_price_ok"] is False


def test_denominator_projection_rejects_non_positive_reference_price_for_d0(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenZeroRef:unknown_pool:0",
        idempotency_key="premium_signals:1",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenZeroRef",
            "symbol": "ZERO",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenZeroRef:unknown_pool:0",
        idempotency_key="signal_features_source_label:1",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenZeroRef",
            "symbol": "ZERO",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "gold",
            "source_label_research_only": True,
            "source_reference_price_type": "legacy_entry_price",
            "source_reference_price": 0.0,
            "source_label_available_at": "2026-01-15T00:00:00Z",
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 1
    assert projection["records"][0]["signal_credit_assignment"]["credited_signal_id"] == 1
    assert projection["records"][0]["reference_price_contract"] is None
    assert projection["contract_evidence"]["ReferencePriceContract"]["missing_count"] == 1
    assert projection["health"]["signal_credit_assignment_ok"] is True
    assert projection["health"]["reference_price_ok"] is False


def test_denominator_projection_uses_legacy_embedded_signal_anchor_for_shadow_credit(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="paper_missed_signal_attribution_recorded",
        aggregate_id="paper_missed:token:TokenEmbeddedSignal",
        idempotency_key="paper_missed_signal_attribution:embedded-signal",
        payload={
            "missed_attribution_id": 1,
            "decision_event_id": 10,
            "token_ca": "TokenEmbeddedSignal",
            "symbol": "EMB",
            "signal_id": 77,
            "signal_ts": 1_700_000_000,
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "source_dog_label": "gold",
            "source_label_research_only": True,
            "telegram_seen": True,
            "realtime_observable": True,
            "baseline_price": 0.001,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    credit = projection["records"][0]["signal_credit_assignment"]
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 1
    assert credit["credited_signal_id"] == 77
    assert credit["credit_assignment_reason"] == "legacy_embedded_signal_anchor"
    assert credit["credit_assignment_quality"] == "shadow_legacy_embedded"
    assert projection["contract_evidence"]["SignalCreditAssignmentContract"]["missing_count"] == 0
    assert projection["contract_evidence"]["SignalCreditAssignmentContract"]["legacy_embedded_credit_count"] == 1
    assert projection["health"]["signal_credit_assignment_ok"] is True


def test_denominator_projection_ignores_late_same_type_reference_price_candidate(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenLateRef:unknown_pool:0",
        idempotency_key="premium_signals:late-ref",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenLateRef",
            "symbol": "LATE",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    for idx, price in enumerate((0.001, 0.002), start=1):
        log.append_event(
            event_type="source_dog_label_recorded",
            aggregate_id=f"source_label:solana:TokenLateRef:unknown_pool:0:{idx}",
            idempotency_key=f"signal_features_source_label:late-ref:{idx}",
            payload={
                "source_label_id": idx,
                "token_ca": "TokenLateRef",
                "symbol": "LATE",
                "chain": "solana",
                "canonical_pool_group": "unknown_pool",
                "lifecycle_epoch": 0,
                "source_dog_label": "gold",
                "source_label_research_only": True,
                "source_reference_price_type": "legacy_entry_price",
                "source_reference_price": price,
                "source_label_available_at": "2026-01-15T00:00:00Z",
            },
        )

    projection = build_denominator_projection(tmp_path, include_records=True)

    record = projection["records"][0]
    evidence = projection["contract_evidence"]["ReferencePriceContract"]
    assert record["reference_price_contract"]["reference_price"] == 0.001
    assert evidence["missing_count"] == 0
    assert evidence["conflict_count"] == 0
    assert evidence["ignored_late_candidate_count"] == 1
    assert record["reference_price_ignored_late_candidates"][0]["ignore_reason"] == "same_type_late_candidate_does_not_reset_reference_price"
    assert projection["health"]["reference_price_ok"] is True


def test_denominator_projection_treats_legacy_entry_and_baseline_as_shadow_aliases(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenLegacyAlias:unknown_pool:0",
        idempotency_key="premium_signals:legacy-alias",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenLegacyAlias",
            "symbol": "ALIAS",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    for idx, ref_type in enumerate(("legacy_entry_price", "legacy_baseline_price"), start=1):
        log.append_event(
            event_type="source_dog_label_recorded",
            aggregate_id=f"source_label:solana:TokenLegacyAlias:unknown_pool:0:{idx}",
            idempotency_key=f"signal_features_source_label:legacy-alias:{idx}",
            payload={
                "source_label_id": idx,
                "token_ca": "TokenLegacyAlias",
                "symbol": "ALIAS",
                "chain": "solana",
                "canonical_pool_group": "unknown_pool",
                "lifecycle_epoch": 0,
                "source_dog_label": "gold",
                "source_label_research_only": True,
                "source_reference_price_type": ref_type,
                "source_reference_price": 0.001 + (idx / 10000),
                "source_label_available_at": "2026-01-15T00:00:00Z",
            },
        )

    projection = build_denominator_projection(tmp_path, include_records=True)

    record = projection["records"][0]
    evidence = projection["contract_evidence"]["ReferencePriceContract"]
    assert record["reference_price_contract"]["reference_price_type"] == "legacy_entry_price"
    assert evidence["missing_count"] == 0
    assert evidence["conflict_count"] == 0
    assert evidence["compatible_alias_candidate_count"] == 1
    assert record["reference_price_compatible_alias_candidates"][0]["compatible_alias_group"] == "legacy_source_reference_price"
    assert projection["health"]["reference_price_ok"] is True


def test_denominator_projection_keeps_different_reference_price_type_as_conflict(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenRefConflict:unknown_pool:0",
        idempotency_key="premium_signals:ref-conflict",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenRefConflict",
            "symbol": "REF",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    for idx, ref_type in enumerate(("legacy_entry_price", "simulated_fill_price"), start=1):
        log.append_event(
            event_type="source_dog_label_recorded",
            aggregate_id=f"source_label:solana:TokenRefConflict:unknown_pool:0:{idx}",
            idempotency_key=f"signal_features_source_label:ref-conflict:{idx}",
            payload={
                "source_label_id": idx,
                "token_ca": "TokenRefConflict",
                "symbol": "REF",
                "chain": "solana",
                "canonical_pool_group": "unknown_pool",
                "lifecycle_epoch": 0,
                "source_dog_label": "gold",
                "source_label_research_only": True,
                "source_reference_price_type": ref_type,
                "source_reference_price": 0.001,
                "source_label_available_at": "2026-01-15T00:00:00Z",
            },
        )

    projection = build_denominator_projection(tmp_path, include_records=True)

    evidence = projection["contract_evidence"]["ReferencePriceContract"]
    assert evidence["missing_count"] == 0
    assert evidence["conflict_count"] == 1
    assert projection["health"]["reference_price_ok"] is False


def test_denominator_projection_consumes_trade_outcome_label_contract(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenTradeOutcome:unknown_pool:0",
        idempotency_key="premium_signals:trade-outcome",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenTradeOutcome",
            "symbol": "TOL",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenTradeOutcome:unknown_pool:0",
        idempotency_key="source_label:trade-outcome",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenTradeOutcome",
            "symbol": "TOL",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "silver",
            "source_label_research_only": True,
            "source_reference_price_type": "legacy_entry_price",
            "source_reference_price": 0.001,
            "source_label_available_at": "2026-01-15T00:00:00Z",
        },
    )
    log.append_event(
        event_type="trade_outcome_label_recorded",
        aggregate_id="trade_outcome:solana:TokenTradeOutcome:unknown_pool:0:1",
        idempotency_key="paper_trade_outcome_label:1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenTradeOutcome",
            "symbol": "TOL",
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
            "exit_capture_ratio": 0.4,
            "trade_label_available_at": 1_700_000_120,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    evidence = projection["contract_evidence"]["TradeOutcomeLabelContract"]
    label_finalization = projection["contract_evidence"]["LabelFinalizationContract"]
    outcome_window_close = projection["contract_evidence"]["OutcomeWindowCloseContract"]
    assert projection["trade_outcome_label_recorded_events"] == 1
    assert evidence["eligible_trade_outcome_records"] == 1
    assert evidence["trade_outcome_label_count"] == 1
    assert evidence["malformed_count"] == 0
    assert projection["health"]["trade_outcome_label_ok"] is True
    assert projection["health"]["label_finalization_ok"] is True
    assert projection["health"]["outcome_window_close_ok"] is True
    assert projection["health"]["reference_price_ok"] is True
    assert projection["contract_evidence"]["ReferencePriceContract"]["conflict_count"] == 0
    assert projection["records"][0]["trade_outcome_label"]["counterfactual_entry_ts"] == 1_700_000_003
    assert label_finalization["eligible_label_finalization_records"] == 1
    assert label_finalization["label_statuses"] == ["final"]
    assert label_finalization["supersedes_label_ids"] == ["source_label:1"]
    assert outcome_window_close["eligible_outcome_window_close_records"] == 1
    assert outcome_window_close["window_order_violation_count"] == 0
    assert projection["records"][0]["label_finalization_contract"]["label_status"] == "final"
    assert projection["records"][0]["label_finalization_contract"]["supersedes_label_id"] == "source_label:1"
    assert projection["records"][0]["outcome_window_close_contract"]["window_closed_at"] == 1_700_000_120


def test_denominator_projection_rejects_outcome_window_close_rollback(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenWindowRollback:unknown_pool:0",
        idempotency_key="premium_signals:window-rollback",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenWindowRollback",
            "symbol": "OWR",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenWindowRollback:unknown_pool:0",
        idempotency_key="source_label:window-rollback",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenWindowRollback",
            "symbol": "OWR",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "silver",
            "source_label_research_only": True,
            "source_reference_price_type": "legacy_entry_price",
            "source_reference_price": 0.001,
            "source_label_available_at": "2026-01-15T00:00:00Z",
        },
    )
    log.append_event(
        event_type="trade_outcome_label_recorded",
        aggregate_id="trade_outcome:solana:TokenWindowRollback:unknown_pool:0:1",
        idempotency_key="paper_trade_outcome_label:window-rollback",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenWindowRollback",
            "symbol": "OWR",
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
            "exit_capture_ratio": 0.4,
            "trade_label_available_at": 1_700_000_003,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    evidence = projection["contract_evidence"]["OutcomeWindowCloseContract"]
    assert projection["health"]["trade_outcome_label_ok"] is True
    assert projection["health"]["label_finalization_ok"] is True
    assert projection["health"]["outcome_window_close_ok"] is False
    assert evidence["eligible_outcome_window_close_records"] == 1
    assert evidence["malformed_count"] == 0
    assert evidence["window_order_violation_count"] == 1
    assert evidence["malformed_outcome_window_closes"] == []
    assert evidence["window_order_violations"][0]["windows"] == [
        {
            "window_start": 1_700_000_120,
            "window_end": 1_700_000_003,
            "window_closed_at": 1_700_000_003,
        }
    ]


def test_denominator_projection_tolerates_one_second_legacy_outcome_window_rollback(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenLegacyWindowRollback:unknown_pool:0",
        idempotency_key="premium_signals:legacy-window-rollback",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenLegacyWindowRollback",
            "symbol": "LWR",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenLegacyWindowRollback:unknown_pool:0",
        idempotency_key="source_label:legacy-window-rollback",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenLegacyWindowRollback",
            "symbol": "LWR",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "silver",
            "source_label_research_only": True,
            "source_reference_price_type": "legacy_entry_price",
            "source_reference_price": 0.001,
            "source_label_available_at": "2026-01-15T00:00:00Z",
        },
    )
    log.append_event(
        event_type="trade_outcome_label_recorded",
        aggregate_id="trade_outcome:solana:TokenLegacyWindowRollback:unknown_pool:0:1",
        idempotency_key="paper_trade_outcome_label:legacy-window-rollback",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenLegacyWindowRollback",
            "symbol": "LWR",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "trade_outcome_label_version": "legacy_paper_trade_outcome_v0.1",
            "trade_outcome_label_quality": "legacy_paper_trade_view",
            "counterfactual_entry_ts": 1_700_000_120,
            "fill_time_anchor": "simulated_fill_ts",
            "simulated_fill_ts": 1_700_000_120,
            "simulated_fill_price": 0.001,
            "net_delayed_executable_peak_3s": 0.75,
            "realized_pnl": 0.3,
            "exit_capture_ratio": 0.4,
            "trade_label_available_at": 1_700_000_119,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    evidence = projection["contract_evidence"]["OutcomeWindowCloseContract"]
    contract = projection["records"][0]["outcome_window_close_contract"]
    assert projection["health"]["outcome_window_close_ok"] is True
    assert evidence["eligible_outcome_window_close_records"] == 1
    assert evidence["malformed_count"] == 0
    assert evidence["window_order_violation_count"] == 0
    assert evidence["window_order_violations"] == []
    assert contract["window_order_ok"] is True
    assert contract["window_order_delta_sec"] == -1.0
    assert contract["window_order_tolerance_sec"] == 1.0
    assert contract["window_order_tolerance_applied_sec"] == 1.0
    assert contract["outcome_window_close_version"] == "v2.7.0.outcome_window_close.v2"


def test_denominator_projection_does_not_mix_trade_fill_with_source_reference_price(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenFillRef:unknown_pool:0",
        idempotency_key="premium_signals:fill-ref",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenFillRef",
            "symbol": "FILL",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenFillRef:unknown_pool:0",
        idempotency_key="source_label:fill-ref",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenFillRef",
            "symbol": "FILL",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "gold",
            "source_label_research_only": True,
            "source_reference_price_type": "legacy_entry_price",
            "source_reference_price": 0.001,
            "source_label_available_at": "2026-01-15T00:00:00Z",
        },
    )
    log.append_event(
        event_type="trade_outcome_label_recorded",
        aggregate_id="trade_outcome:solana:TokenFillRef:unknown_pool:0:1",
        idempotency_key="paper_trade_outcome_label:fill-ref:1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenFillRef",
            "symbol": "FILL",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "trade_outcome_label_version": "legacy_paper_trade_outcome_v0.1",
            "counterfactual_entry_ts": 1_700_000_003,
            "simulated_fill_price": 0.002,
            "trade_label_available_at": 1_700_000_120,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    evidence = projection["contract_evidence"]["ReferencePriceContract"]
    assert evidence["missing_count"] == 0
    assert evidence["conflict_count"] == 0
    assert projection["health"]["reference_price_ok"] is True
    assert projection["records"][0]["reference_price_contract"]["reference_price"] == 0.001
    assert projection["records"][0]["trade_outcome_label"]["simulated_fill_price"] == 0.002


def test_denominator_projection_does_not_mix_execution_contract_prices_with_source_reference_price(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenExecutionRef:unknown_pool:0",
        idempotency_key="premium_signals:execution-ref",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenExecutionRef",
            "symbol": "EXEC",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenExecutionRef:unknown_pool:0",
        idempotency_key="source_label:execution-ref",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenExecutionRef",
            "symbol": "EXEC",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "gold",
            "source_label_research_only": True,
            "source_reference_price_type": "legacy_entry_price",
            "source_reference_price": 0.001,
            "source_label_available_at": "2026-01-15T00:00:00Z",
        },
    )
    log.append_event(
        event_type="standardized_stop_contract_recorded",
        aggregate_id="standardized_stop:solana:TokenExecutionRef:unknown_pool:0:1",
        idempotency_key="standardized_stop_contract:execution-ref",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenExecutionRef",
            "symbol": "EXEC",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "counterfactual_entry_ts": 1_700_000_003,
            "simulated_fill_ts": 1_700_000_003,
            "simulated_fill_price": 0.004,
            "stop_contract_version": "legacy_standardized_stop_v0.1",
            "stop_type": "standardized_counterfactual_stop",
            "stop_threshold_pct": -0.3,
            "stop_window": "5m",
            "stop_price_type": "delayed_executable_exit_quote",
            "stop_executable_required": True,
            "stop_friction_model_version": "legacy_round_trip_friction_v0.1",
            "stop_available_at": 1_700_000_003,
        },
    )
    log.append_event(
        event_type="ex_ante_feasibility_recorded",
        aggregate_id="ex_ante_feasibility:solana:TokenExecutionRef:unknown_pool:0:1",
        idempotency_key="ex_ante_feasibility:execution-ref",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenExecutionRef",
            "symbol": "EXEC",
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
            "entry_quote_price": 0.004,
            "current_quote_availability": True,
            "current_pool_resolution": "unknown_pool",
            "current_provider_health": "legacy_not_recorded",
            "current_risk_availability": "legacy_not_recorded",
            "current_queue_delay_sec": 0,
            "feature_max_available_at": 1_700_000_002,
            "used_future_peak": False,
            "used_future_outcome": False,
            "used_posthoc_label": False,
            "forbidden_future_fields_used": [],
        },
    )
    log.append_event(
        event_type="earliest_actionable_time_recorded",
        aggregate_id="earliest_actionable_time:solana:TokenExecutionRef:unknown_pool:0:1",
        idempotency_key="earliest_actionable_time:execution-ref",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenExecutionRef",
            "symbol": "EXEC",
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
            "counterfactual_entry_ts": 1_700_000_003,
            "actionable_before_peak": True,
            "earliest_actionable_reason": "legacy_actual_paper_entry_inputs_available_by_decision",
            "actionability_quality": "legacy_actual_paper_entry_window_proof",
            "entry_quote_price": 0.004,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    evidence = projection["contract_evidence"]["ReferencePriceContract"]
    record = projection["records"][0]
    assert evidence["missing_count"] == 0
    assert evidence["conflict_count"] == 0
    assert evidence["ignored_late_candidate_count"] == 0
    assert evidence["compatible_alias_candidate_count"] == 0
    assert len(record["reference_price_candidates"]) == 1
    assert record["reference_price_contract"]["reference_price"] == 0.001
    assert record["reference_price_contract"]["reference_price_source_event_id"].startswith("v27evt_")
    assert projection["health"]["reference_price_ok"] is True


def test_denominator_projection_does_not_treat_source_label_legacy_trade_as_trade_outcome(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenSourceOnly:unknown_pool:0",
        idempotency_key="source_label:source-only",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenSourceOnly",
            "symbol": "SRC",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "silver",
            "source_label_research_only": True,
            "source_reference_price_type": "legacy_entry_price",
            "source_reference_price": 0.001,
            "source_label_available_at": "2026-01-15T00:00:00Z",
            "legacy_paper_trade": {
                "entry_price": 0.001,
                "entry_ts": 1_700_000_003,
                "peak_pnl": 0.75,
            },
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    evidence = projection["contract_evidence"]["TradeOutcomeLabelContract"]
    assert projection["trade_outcome_label_recorded_events"] == 0
    assert evidence["eligible_trade_outcome_records"] == 0
    assert evidence["malformed_count"] == 0
    assert projection["health"]["trade_outcome_label_ok"] is False
    assert projection["records"][0]["trade_outcome_label"] is None


def test_denominator_projection_consumes_standardized_stop_contract(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="standardized_stop_contract_recorded",
        aggregate_id="standardized_stop:solana:TokenStop:unknown_pool:0:1",
        idempotency_key="standardized_stop_contract:1:legacy_standardized_stop_v0.1",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenStop",
            "symbol": "STOP",
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

    projection = build_denominator_projection(tmp_path, include_records=True)

    evidence = projection["contract_evidence"]["StandardizedStopContract"]
    assert projection["standardized_stop_contract_recorded_events"] == 1
    assert evidence["eligible_standardized_stop_records"] == 1
    assert evidence["standardized_stop_contract_count"] == 1
    assert evidence["malformed_count"] == 0
    assert evidence["stop_contract_versions"] == ["legacy_standardized_stop_v0.1"]
    assert projection["health"]["standardized_stop_ok"] is True
    assert projection["records"][0]["standardized_stop_contract"]["stop_threshold_pct"] == -30.0


def test_denominator_projection_rejects_malformed_standardized_stop_contract(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="standardized_stop_contract_recorded",
        aggregate_id="standardized_stop:solana:TokenBadStop:unknown_pool:0:1",
        idempotency_key="standardized_stop_contract:1:bad",
        payload={
            "paper_trade_id": 1,
            "token_ca": "TokenBadStop",
            "symbol": "BADSTOP",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "stop_contract_version": "legacy_standardized_stop_v0.1",
            "stop_type": "standardized_counterfactual_stop",
            "stop_threshold_pct": 30.0,
            "stop_window": "60m",
            "stop_price_type": "delayed_executable_exit_quote_proxy",
            "stop_executable_required": False,
            "stop_friction_model_version": "legacy_round_trip_friction_v0.1",
            "stop_available_at": 1_700_000_003,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    evidence = projection["contract_evidence"]["StandardizedStopContract"]
    assert evidence["eligible_standardized_stop_records"] == 1
    assert evidence["malformed_count"] == 1
    assert evidence["malformed_stops"][0]["missing_fields"] == [
        "stop_executable_required_true",
        "stop_threshold_pct",
    ]
    assert projection["health"]["standardized_stop_ok"] is False


def test_denominator_projection_keeps_unresolved_source_label_as_gap(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenUnresolved:unknown_pool:0",
        idempotency_key="signal_features_source_label:1",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenUnresolved",
            "symbol": "UNR",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": None,
            "source_label_quality": "legacy_label_unresolved",
            "source_label_research_only": True,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["source_dog_label_events"] == 1
    assert projection["metrics"]["denominator_seed_records"] == 1
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 0
    assert projection["evidence_gaps"]["SourceDogLabelContract"] == 1
    assert projection["evidence_gaps"]["TelegramLifecycleEvent"] == 1


def test_denominator_projection_does_not_count_source_label_without_telegram_anchor_as_d0(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenOnlyLabel:unknown_pool:0",
        idempotency_key="signal_features_source_label:1",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenOnlyLabel",
            "symbol": "ONLY",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "gold",
            "source_label_research_only": True,
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["metrics"]["denominator_seed_records"] == 1
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 0
    assert projection["evidence_gaps"]["TelegramLifecycleEvent"] == 1
    assert projection["evidence_gaps"]["ProductionSourceDogLabelContract"] == 1


def test_denominator_projection_rekeys_unknown_pool_records_from_lifecycle_identity(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id="telegram_signal:solana:TokenLife:unknown_pool:0",
        idempotency_key="premium_signals:1",
        payload={
            "telegram_signal_id": 1,
            "token_ca": "TokenLife",
            "symbol": "LIFE",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )
    log.append_event(
        event_type="source_dog_label_recorded",
        aggregate_id="source_label:solana:TokenLife:unknown_pool:0",
        idempotency_key="signal_features_source_label:1",
        payload={
            "source_label_id": 1,
            "token_ca": "TokenLife",
            "symbol": "LIFE",
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "source_dog_label": "gold",
            "source_label_research_only": True,
        },
    )
    log.append_event(
        event_type="token_lifecycle_identity_resolved",
        aggregate_id="token_lifecycle:solana:TokenLife:PoolA:0",
        idempotency_key="lifecycle_tracks:1",
        payload={
            "lifecycle_track_id": 1,
            "token_ca": "TokenLife",
            "symbol": "LIFE",
            "chain": "solana",
            "canonical_pool_group": "PoolA",
            "lifecycle_epoch": 0,
            "lifecycle_id": "TokenLife:1700000000",
            "pool_address": "PoolA",
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["lifecycle_identity_events"] == 1
    assert projection["metrics"]["denominator_seed_records"] == 1
    assert projection["records"][0]["denominator_dedup_key"] == "solana:TokenLife:PoolA:0"
    assert projection["records"][0]["canonical_pool_group"] == "PoolA"
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 1
    assert projection["metrics"]["telegram_realtime_observable_gold_silver_D1"] == 1


def test_denominator_projection_lifecycle_identity_alone_does_not_prove_d0(tmp_path):
    log = V27EventLog(tmp_path)
    log.append_event(
        event_type="token_lifecycle_identity_resolved",
        aggregate_id="token_lifecycle:solana:TokenOnlyLife:PoolA:0",
        idempotency_key="lifecycle_tracks:1",
        payload={
            "lifecycle_track_id": 1,
            "token_ca": "TokenOnlyLife",
            "symbol": "ONLY",
            "chain": "solana",
            "canonical_pool_group": "PoolA",
            "lifecycle_epoch": 0,
            "pool_address": "PoolA",
        },
    )

    projection = build_denominator_projection(tmp_path, include_records=True)

    assert projection["metrics"]["denominator_seed_records"] == 1
    assert projection["metrics"]["telegram_gold_silver_total_D0"] == 0
    assert projection["evidence_gaps"]["TelegramLifecycleEvent"] == 1
    assert projection["evidence_gaps"]["SourceDogLabelContract"] == 1
