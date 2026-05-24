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


def append_raw_provider_evidence(log, *, token_ca, paper_trade_id=1, pool="pool-a", trusted=True, missing_version=False):
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
    return log.append_event(
        event_type="raw_provider_evidence_recorded",
        aggregate_id=f"raw_provider_evidence:solana:{token_ca}:{pool}:0:{paper_trade_id}:entry",
        idempotency_key=f"raw_provider_evidence:{paper_trade_id}:entry:legacy_paper_raw_provider_evidence_v0.1",
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
    assert projection["records"][0]["raw_provider_evidence_contract"]["provider_evidence_valid"] is True


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
