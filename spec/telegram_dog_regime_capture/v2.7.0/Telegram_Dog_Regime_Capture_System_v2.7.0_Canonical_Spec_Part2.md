# Telegram Dog Regime Capture System v2.7.0 Canonical Spec - Part 2

Generated from repo-local canonical JSON artifacts.

## Source Artifacts

- Manifest: `spec/telegram_dog_regime_capture/v2.7.0/spec.manifest.json`
- Contract catalog: `spec/telegram_dog_regime_capture/v2.7.0/contract-catalog.json`
- Gap register: `spec/telegram_dog_regime_capture/v2.7.0/gap-register.json`
- Catalog contracts: `282`
- Gap register contracts: `244`
- Gap contracts missing catalog records: `0`

## Release Principle

Do not rewrite the whole system at once. Freeze dangerous direct entries first, create canonical spec and traceability, mirror existing decisions into the new event log, rebuild denominators, shadow the new arbiter, then gate one tracer-bullet cohort through ultra tiny before normal tiny canary.

## Next Required Step

Implement and verify the first remaining normal_tiny runtime blocker evidence chain against production read models.

## S07 - Feature, Label, Lineage, and Training Truth

- Section mode target: `normal_tiny_blocking`
- Catalog contract count: `15`
- Gap batch count: `5`

### Catalog Contracts

#### DataLineageGraphContract

- Section: `S07`
- Mode target: `shadow_blocking`
- Failure action: `sample_not_eligible_for_training`
- Required fields: `lineage_node_id, node_type, parent_node_ids, edge_type, source_hash`

#### ExAnteVsPosthocFeasibilityContract

- Section: `S07`
- Mode target: `shadow_blocking`
- Failure action: `sample_excluded`
- Required fields: `ex_ante_feasible, posthoc_feasible, feasibility_class, used_future_peak_in_ex_ante`

#### FeatureAvailabilityContract

- Section: `S07`
- Mode target: `shadow_blocking`
- Failure action: `feature_leakage_detected`
- Required fields: `feature_window_end, feature_available_at, decision_available_at, label_available_at, null_policy_field`

#### FeatureStoreConsistencyContract

- Section: `S07`
- Mode target: `normal_tiny_blocking`
- Failure action: `model_promotion_blocked`
- Required fields: `feature_set_id, offline_hash, online_hash, normalization_version, checked_at`

#### FeatureVectorSnapshotContract

- Section: `S07`
- Mode target: `normal_tiny_blocking`
- Failure action: `replay_invalid`
- Required fields: `feature_vector_hash, feature_names_ordered, feature_values_serialized, missing_value_policy, model_input_schema_version`

#### LabelDisputeResolutionContract

- Section: `S07`
- Mode target: `normal_tiny_blocking`
- Failure action: `label_quarantined`
- Required fields: `dispute_id, label_id, resolution_action, resolved_at`

#### LabelFinalizationContract

- Section: `S07`
- Mode target: `normal_tiny_blocking`
- Failure action: `no_training_or_promotion`
- Required fields: `label_id, label_status, outcome_window_closed_at, supersedes_label_id`

#### NullValuePolicyContract

- Section: `S07`
- Mode target: `shadow_blocking`
- Failure action: `feature_invalid_or_shadow_only`
- Required fields: `field_name, null_class, allowed_in_modes, default_value_allowed, imputation_policy`

#### ObservationPolicyContract

- Section: `S07`
- Mode target: `shadow_blocking`
- Failure action: `observation_rejected`
- Required fields: `observation_id, observation_policy_version, allowed_sources, forbidden_fields, recorded_at`

#### OutcomeWindowCloseContract

- Section: `S07`
- Mode target: `normal_tiny_blocking`
- Failure action: `label_not_final`
- Required fields: `label_id, window_start, window_end, window_closed_at`

#### SelectionBiasDiagnosticContract

- Section: `S07`
- Mode target: `normal_tiny_blocking`
- Failure action: `research_only`
- Required fields: `diagnostic_id, selection_policy_version, included_count, excluded_count, bias_result`

#### TradeOutcomeLabelContract

- Section: `S07`
- Mode target: `shadow_blocking`
- Failure action: `research_only`
- Required fields: `trade_outcome_label_version, counterfactual_entry_ts, simulated_fill_price, trade_label_available_at`

#### TrainingDatasetManifestContract

- Section: `S07`
- Mode target: `normal_tiny_blocking`
- Failure action: `model_promotion_blocked`
- Required fields: `dataset_id, event_log_hash_range, included_sample_ids, label_versions, feature_versions`

#### TrainingPoisoningGuard

- Section: `S07`
- Mode target: `normal_tiny_blocking`
- Failure action: `model_promotion_blocked`
- Required fields: `training_run_id, dataset_hash, poison_signal_count, quarantine_action, checked_at`

#### TrainingServingSkewContract

- Section: `S07`
- Mode target: `normal_tiny_blocking`
- Failure action: `no_model_promotion`
- Required fields: `training_feature_code_hash, serving_feature_code_hash, normalization_version, skew_check_result`

### Gap Register Coverage

#### v2.6.13_spec_governance_confidence_fill_feasibility

- Theme: Rendered spec views, health enums, contract lifecycle, objective priority, goal confidence, fill-time anchors, and ex-ante/posthoc feasibility must be machine-checkable before shadow or normal tiny evidence can be trusted.
- Contracts: `ExAnteVsPosthocFeasibilityContract`

#### v2.6.13_training_truth_foundation

- Theme: Feature availability, null semantics, lineage, feature vector snapshots, and training dataset manifests must be machine-checkable before model promotion or normal tiny evidence.
- Contracts: `DataLineageGraphContract, FeatureAvailabilityContract, FeatureVectorSnapshotContract, NullValuePolicyContract, TrainingDatasetManifestContract`

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `LabelFinalizationContract, OutcomeWindowCloseContract, TrainingPoisoningGuard`

#### v2.6.15_queue_fleet_admin_parser_training_serving

- Theme: Queue durability, worker readiness, admin command safety, parser ambiguity, and training-serving skew.
- Contracts: `FeatureStoreConsistencyContract, ObservationPolicyContract, SelectionBiasDiagnosticContract, TrainingServingSkewContract`

#### v2.6.17_meta_governance_evidence_release_safety

- Theme: Contract conflict resolution, evidence eligibility, complexity control, release safety, waivers, and project-level stop loss.
- Contracts: `LabelDisputeResolutionContract`

## S08 - Provider Evidence, Quote Intent, Quota, and External Dependency Safety

- Section mode target: `normal_tiny_blocking`
- Catalog contract count: `13`
- Gap batch count: `4`

### Catalog Contracts

#### ChainFinalityContract

- Section: `S08`
- Mode target: `observe_only_blocking`
- Failure action: `shadow_only`
- Required fields: `chain, slot, commitment_level, rpc_consistency_check, chain_reorg_detected`

#### ExternalDependencyContract

- Section: `S08`
- Mode target: `normal_tiny_blocking`
- Failure action: `dependency_degraded`
- Required fields: `dependency_name, health_status, fallback_mode, fail_closed_action`

#### FeeScheduleSourceContract

- Section: `S08`
- Mode target: `normal_tiny_blocking`
- Failure action: `fee_source_untrusted`
- Required fields: `fee_source_id, provider, fee_version, source_hash, effective_at`

#### FeeScheduleVersionContract

- Section: `S08`
- Mode target: `normal_tiny_blocking`
- Failure action: `fee_model_invalid`
- Required fields: `fee_version, provider, chain, effective_at, supersedes_version`

#### ProviderByzantineQuorumContract

- Section: `S08`
- Mode target: `normal_tiny_blocking`
- Failure action: `provider_untrusted`
- Required fields: `quorum_id, provider_set, conflict_policy, selected_provider`

#### ProviderCachePoisoningGuard

- Section: `S08`
- Mode target: `normal_tiny_blocking`
- Failure action: `provider_cache_dirty`
- Required fields: `cache_key, provider, poison_detected, quarantine_action`

#### ProviderCoverageMapContract

- Section: `S08`
- Mode target: `normal_tiny_blocking`
- Failure action: `coverage_gap_not_no_quote`
- Required fields: `provider, chain, pool_type, coverage_status, unsupported_reason`

#### ProviderCredentialScopeContract

- Section: `S08`
- Mode target: `normal_tiny_blocking`
- Failure action: `provider_credential_rejected`
- Required fields: `credential_id, provider, allowed_endpoints, allowed_modes, expires_at`

#### ProviderRequestReplayContract

- Section: `S08`
- Mode target: `normal_tiny_blocking`
- Failure action: `replay_incomplete`
- Required fields: `request_id, provider, request_hash, retry_count, decision_reason`

#### ProviderResponseAuthenticityContract

- Section: `S08`
- Mode target: `normal_tiny_blocking`
- Failure action: `provider_untrusted`
- Required fields: `response_id, provider, signature_status, transport_security, verified_at`

#### ProviderSchemaContract

- Section: `S08`
- Mode target: `observe_only_blocking`
- Failure action: `provider_degraded`
- Required fields: `provider_name, schema_version, required_fields, canary_parse_result, schema_drift_detected`

#### QuoteIntentBindingContract

- Section: `S08`
- Mode target: `ultra_tiny_blocking`
- Failure action: `quote_invalid`
- Required fields: `quote_intent_id, side, size, route, pool, quote_mint, slippage_bps, quote_ts`

#### RawProviderEvidenceContract

- Section: `S08`
- Mode target: `normal_tiny_blocking`
- Failure action: `evidence_untrusted`
- Required fields: `provider, endpoint, request_hash, response_hash, request_id, latency_ms`

### Gap Register Coverage

#### v2.6.13_identity_unit_provider_finality

- Theme: Token identity, data unit, chain finality, and provider schema truth must be machine-checkable before denominator, label, or training evidence can be trusted.
- Contracts: `ChainFinalityContract, ProviderSchemaContract`

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `ExternalDependencyContract, ProviderByzantineQuorumContract, ProviderCachePoisoningGuard, ProviderRequestReplayContract, QuoteIntentBindingContract, RawProviderEvidenceContract`

#### v2.6.15_queue_fleet_admin_parser_training_serving

- Theme: Queue durability, worker readiness, admin command safety, parser ambiguity, and training-serving skew.
- Contracts: `FeeScheduleVersionContract, ProviderCoverageMapContract, ProviderCredentialScopeContract, ProviderResponseAuthenticityContract`

#### v2.6.16_bypass_static_resources_source_risk_evidence

- Theme: Bypass prevention, CI/CD enforcement, resource exhaustion, source authenticity, post-entry risk, and evidence anchoring.
- Contracts: `FeeScheduleSourceContract`

## S09 - Decision, Forecast, and Model Governance

- Section mode target: `shadow_first`
- Catalog contract count: `24`
- Gap batch count: `4`

### Catalog Contracts

#### AbsorbingSemiMarkovForecastContract

- Section: `S09`
- Mode target: `shadow_first`
- Failure action: `forecast_research_only`
- Required fields: `model_snapshot_id, absorbing_state_definition, state_duration_sec, absorption_probabilities, expected_time_to_absorption`

#### CensoringPolicyContract

- Section: `S09`
- Mode target: `shadow_first`
- Failure action: `forecast_research_only`
- Required fields: `censoring_policy_version, outcome_status, censoring_reason, training_weight_policy`

#### CompetingRiskForecastContract

- Section: `S09`
- Mode target: `shadow_first`
- Failure action: `forecast_research_only`
- Required fields: `model_snapshot_id, p_absorb_peak30, p_absorb_stop_before_peak, censoring_policy_version`

#### CounterfactualEntryTime

- Section: `S09`
- Mode target: `shadow_blocking`
- Failure action: `research_only`
- Required fields: `counterfactual_entry_ts, counterfactual_policy_version, counterfactual_model_snapshot_id`

#### DecisionAudit

- Section: `S09`
- Mode target: `ultra_tiny_blocking`
- Failure action: `entry_rejected`
- Required fields: `decision_id, policy_bundle_id, spec_hash, feature_vector_hash, decision_trace_bundle`

#### DetectorCalibrationContract

- Section: `S09`
- Mode target: `normal_tiny_promotion_blocking`
- Failure action: `no_model_promotion`
- Required fields: `calibration_id, detector_id, metric_id, threshold_id, sample_n`

#### EarliestActionableTime

- Section: `S09`
- Mode target: `shadow_blocking`
- Failure action: `attribute_not_missed_entry`
- Required fields: `earliest_actionable_ts, required_inputs_available_at, peak_ts, actionable_before_peak`

#### ExAnteFeasibility

- Section: `S09`
- Mode target: `shadow_blocking`
- Failure action: `no_entry`
- Required fields: `ex_ante_feasible, feasibility_class, feasibility_policy_version, decision_ts`

#### ForecastSanityGuard

- Section: `S09`
- Mode target: `normal_tiny_blocking`
- Failure action: `global_circuit_breaker`
- Required fields: `raw_forecast, sanitized_forecast, sanity_cap_reason, feature_vector_hash`

#### ForecastWalkForwardValidationContract

- Section: `S09`
- Mode target: `shadow_first`
- Failure action: `no_model_promotion`
- Required fields: `validation_id, cutoff_seq, train_window_id, no_lookahead_proof`

#### HMMResearchOnlyBoundaryContract

- Section: `S09`
- Mode target: `shadow_first`
- Failure action: `no_entry_gate`
- Required fields: `artifact_id, research_only, online_filtering_only, full_sequence_viterbi_allowed`

#### LifecycleNstepForecastContract

- Section: `S09`
- Mode target: `shadow_first`
- Failure action: `forecast_research_only`
- Required fields: `model_snapshot_id, forecast_horizon_steps, start_state, n_step_distribution, matrix_build_cutoff_seq`

#### ModelArtifactRuntimeCompatibilityContract

- Section: `S09`
- Mode target: `normal_tiny_blocking`
- Failure action: `model_unloadable`
- Required fields: `model_snapshot_id, runtime_version, serialization_format, compatibility_result`

#### ModelExpiryContract

- Section: `S09`
- Mode target: `normal_tiny_blocking`
- Failure action: `shadow_only`
- Required fields: `model_snapshot_id, trained_until, expiry_ts, expired_action_cap`

#### OverextensionDetector

- Section: `S09`
- Mode target: `shadow_blocking`
- Failure action: `cannot_be_normal_tiny_gate`
- Required fields: `detector_id, detector_version, detector_output_states, threshold_ids, feature_available_at_required`

#### PolicyBundleCompatibilityContract

- Section: `S09`
- Mode target: `normal_tiny_blocking`
- Failure action: `shadow_only`
- Required fields: `policy_bundle_id, model_snapshot_id, feature_schema_version, threshold_version, metric_registry_version`

#### RealtimeCleanDetector

- Section: `S09`
- Mode target: `ultra_tiny_blocking`
- Failure action: `no_entry`
- Required fields: `quote_source, quote_age_sec, entry_quote_available, exit_quote_available, clean_standard_version`

#### ReclaimDetector

- Section: `S09`
- Mode target: `shadow_blocking`
- Failure action: `cannot_be_normal_tiny_gate`
- Required fields: `detector_id, detector_version, detector_output_states, threshold_ids, feature_available_at_required`

#### ReferencePriceContract

- Section: `S09`
- Mode target: `shadow_blocking`
- Failure action: `label_unavailable`
- Required fields: `reference_price_type, reference_price, reference_price_ts, reference_price_quality`

#### ReplayDeterminismCheck

- Section: `S09`
- Mode target: `normal_tiny_blocking`
- Failure action: `training_and_promotion_disabled`
- Required fields: `event_log_hash, policy_manifest_hash, decision_hash, forecast_hash, ledger_hash`

#### ReproducibleBuildContract

- Section: `S09`
- Mode target: `normal_tiny_blocking`
- Failure action: `research_only`
- Required fields: `code_commit_hash, dependency_lock_hash, runtime_version, container_image_hash, build_hash`

#### StandardizedStopContract

- Section: `S09`
- Mode target: `shadow_blocking`
- Failure action: `no_model_promotion`
- Required fields: `stop_contract_version, stop_type, stop_threshold_pct, stop_price_type`

#### SupplyChainSecurityContract

- Section: `S09`
- Mode target: `normal_tiny_blocking`
- Failure action: `global_circuit_breaker`
- Required fields: `artifact_id, signature_status, SBOM_hash, vulnerability_scan_status, provenance_attestation`

#### TelegramLifecycleTransitionMatrixContract

- Section: `S09`
- Mode target: `shadow_first`
- Failure action: `forecast_research_only`
- Required fields: `model_snapshot_id, state_definition_version, transition_matrix, matrix_build_cutoff_seq, sample_n`

### Gap Register Coverage

#### v2.6.13_detector_shadow_calibration

- Theme: Reclaim and overextension detectors must exist as shadow-only calibrated evidence before they can influence entry promotion.
- Contracts: `DetectorCalibrationContract, OverextensionDetector, ReclaimDetector`

#### v2.6.13_markov_lifecycle_forecast_shadow

- Theme: Carries the v2.6.13 Markov-regime material forward as Telegram lifecycle absorbing Semi-Markov, competing-risk, censoring, no-lookahead forecast boundaries.
- Contracts: `AbsorbingSemiMarkovForecastContract, CensoringPolicyContract, CompetingRiskForecastContract, ForecastWalkForwardValidationContract, HMMResearchOnlyBoundaryContract, LifecycleNstepForecastContract, TelegramLifecycleTransitionMatrixContract`

#### v2.6.13_replay_build_model_trust

- Theme: Replay determinism, reproducible build, supply-chain provenance, compatible policy bundle, model expiry, and forecast sanity must be proven before normal tiny.
- Contracts: `ForecastSanityGuard, ModelExpiryContract, PolicyBundleCompatibilityContract, ReplayDeterminismCheck, ReproducibleBuildContract, SupplyChainSecurityContract`

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `ModelArtifactRuntimeCompatibilityContract`

## S10 - Experiment, Holdout, Randomness, and Bias Control

- Section mode target: `normal_tiny_promotion_blocking`
- Catalog contract count: `6`
- Gap batch count: `4`

### Catalog Contracts

#### BlindedHoldoutContract

- Section: `S10`
- Mode target: `normal_tiny_promotion_blocking`
- Failure action: `experiment_invalid`
- Required fields: `holdout_id, window_id, blinded, access_count, no_retune_enforced`

#### ExperimentAssignmentImmutabilityContract

- Section: `S10`
- Mode target: `normal_tiny_promotion_blocking`
- Failure action: `experiment_invalid`
- Required fields: `assignment_id, randomization_unit, original_assignment_hash, attempted_change_hash, detected_at`

#### LifecycleStateMachineContract

- Section: `S10`
- Mode target: `ultra_tiny_blocking`
- Failure action: `shadow_only`
- Required fields: `states, allowed_transitions, state_version_fencing_required, invalid_transition_action`

#### ManualOverrideContract

- Section: `S10`
- Mode target: `normal_tiny_blocking`
- Failure action: `manual_override_quarantined`
- Required fields: `override_id, operator_id, quarantine_required, promotion_evidence_allowed, training_allowed`

#### NegativeControlContract

- Section: `S10`
- Mode target: `normal_tiny_promotion_blocking`
- Failure action: `experiment_invalid`
- Required fields: `control_id, control_group, expected_no_effect_metric, observed_effect, checked_at`

#### RandomnessControlContract

- Section: `S10`
- Mode target: `normal_tiny_promotion_blocking`
- Failure action: `experiment_invalid`
- Required fields: `rng_seed, rng_version, randomization_unit, assignment_id`

### Gap Register Coverage

#### v2.6.13_execution_exit_safety

- Theme: Lifecycle, exit execution, exit policy, circuit breaker, emergency exit journal, and exit queue health must be machine-checkable before normal tiny can be trusted.
- Contracts: `LifecycleStateMachineContract`

#### v2.6.13_release_experiment_safety

- Theme: Secrets lifecycle, system SLO, no-trade root cause, release complexity, backpressure, budget reserve, holdout blinding, manual override quarantine, contract tests, and adversarial replay must be machine-checkable before normal tiny.
- Contracts: `BlindedHoldoutContract, ManualOverrideContract`

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `ExperimentAssignmentImmutabilityContract, RandomnessControlContract`

#### v2.6.15_queue_fleet_admin_parser_training_serving

- Theme: Queue durability, worker readiness, admin command safety, parser ambiguity, and training-serving skew.
- Contracts: `NegativeControlContract`

## S11 - Queue, Scheduler, Worker, and Backpressure Safety

- Section mode target: `normal_tiny_blocking`
- Catalog contract count: `28`
- Gap batch count: `5`

### Catalog Contracts

#### BackgroundJobRegistryContract

- Section: `S11`
- Mode target: `mvp_blocking`
- Failure action: `job_disabled`
- Required fields: `job_name, entry_point, allowed_modes, lease_policy, owner`

#### BackpressurePolicy

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `capacity_backpressure_blocked`
- Required fields: `component, queue_depth, max_queue_depth, drops_p0_p1_allowed, exit_safety_priority`

#### BudgetReserveContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `budget_reserve_blocked`
- Required fields: `reserve_id, budget_pool, reserved_for, hard_limit, priority_class`

#### CandidateCancellationContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `abort_candidate`
- Required fields: `candidate_id, cancel_reason, cancel_event_seq, cancelled_at`

#### CapacityPlanningContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `capacity_degraded`
- Required fields: `capacity_plan_id, component, expected_peak_qps, measured_peak_qps, headroom_pct`

#### CircuitBreakerPositionPolicy

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `global_circuit_breaker`
- Required fields: `trigger_events, new_entry_disabled, exit_safety_remains_active, open_position_policy`

#### ConnectionPoolPartitionContract

- Section: `S11`
- Mode target: `mvp_blocking`
- Failure action: `resource_degraded`
- Required fields: `pool_name, partition_key, max_connections, critical_reserved_connections, checked_at`

#### EconomicCostBudgetContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `degrade_low_priority`
- Required fields: `budget_id, budget_pool, soft_limit, hard_limit, current_usage`

#### EmergencyExitJournal

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `exit_evidence_quarantined`
- Required fields: `journal_event_id, position_id, reconciled_to_ledger, journal_append_only`

#### ExitExecutionStateMachine

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `exit_safety_degraded`
- Required fields: `states, allowed_transitions, exit_quote_required, state_revalidation_required`

#### ExitPolicyContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `exit_policy_invalid`
- Required fields: `exit_policy_version, take_profit_rules, stop_loss_rules, time_stop_rules`

#### ExitQueueHealthContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `no_normal_tiny`
- Required fields: `exit_queue_status, stuck_open_position_count, exit_quote_failure_count, exit_safety_budget_reserved`

#### LatencyAttributionContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `missed_analysis_invalid`
- Required fields: `token_lifecycle_key, latency_class, latency_ms, blocking_component, owner`

#### LoadTestReplayContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `capacity_degraded`
- Required fields: `load_test_id, event_log_hash, replay_speed_multiplier, expected_invariants, pass_fail`

#### PipelineProgressInvariant

- Section: `S11`
- Mode target: `mvp_blocking`
- Failure action: `pipeline_degraded`
- Required fields: `pipeline_id, stage_name, max_stall_ms, last_progress_at, stall_action`

#### ProviderQuotaIsolationContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `provider_degraded`
- Required fields: `provider, budget_pool, priority_order, quota_limit_per_min, exit_safety_reserved_pct`

#### QueueAckNackContract

- Section: `S11`
- Mode target: `mvp_blocking`
- Failure action: `queue_state_untrusted`
- Required fields: `queue_id, task_id, ack_state, nack_reason, recorded_at`

#### QueueDurabilityContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `no_new_entry_for_queue`
- Required fields: `queue_id, task_id, durable_state, ack_state, created_at`

#### ResourceExhaustionContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `degrade_low_priority`
- Required fields: `resource_type, pressure_level, pressure_action, safety_budget_remaining`

#### RetryPolicyCatalogContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `retry_policy_missing`
- Required fields: `retry_family, backoff_policy, max_attempts, jitter_policy, owner`

#### RetryStormControlContract

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `degrade_low_priority`
- Required fields: `retry_family, backoff_policy, max_concurrent_retries, p0_reserved_capacity`

#### ScheduledJobModeGateContract

- Section: `S11`
- Mode target: `mvp_blocking`
- Failure action: `job_disabled`
- Required fields: `job_name, mode, allowed_to_run, gate_reason, checked_at`

#### ServiceReadinessProbeContract

- Section: `S11`
- Mode target: `mvp_blocking`
- Failure action: `service_not_ready`
- Required fields: `service_name, probe_id, health_status, dependency_status, checked_at`

#### SilentWorkerDeathDetector

- Section: `S11`
- Mode target: `mvp_blocking`
- Failure action: `worker_restarted`
- Required fields: `worker_id, heartbeat_deadline, last_heartbeat_at, death_detected, action`

#### SystemSLO

- Section: `S11`
- Mode target: `normal_tiny_blocking`
- Failure action: `new_entry_shadow_only`
- Required fields: `slo_id, metric_id, threshold_id, measured_value, new_entry_action`

#### ThreadPoolIsolationContract

- Section: `S11`
- Mode target: `mvp_blocking`
- Failure action: `resource_degraded`
- Required fields: `pool_name, workload_class, max_workers, reserved_capacity, checked_at`

#### WarmStartSafetyContract

- Section: `S11`
- Mode target: `mvp_blocking`
- Failure action: `worker_restart_required`
- Required fields: `worker_id, warm_start_state_hash, required_snapshot_hash, safe_to_resume, checked_at`

#### WorkerHeartbeatContract

- Section: `S11`
- Mode target: `mvp_blocking`
- Failure action: `worker_unhealthy`
- Required fields: `worker_id, worker_role, heartbeat_at, heartbeat_deadline, build_hash`

### Gap Register Coverage

#### v2.6.13_capacity_load_latency_truth

- Theme: Capacity plans, load replay, latency attribution, provider quota isolation, and economic budgets must prove P0/P1 and exit safety survive Telegram bursts before normal tiny.
- Contracts: `CapacityPlanningContract, EconomicCostBudgetContract, LatencyAttributionContract, LoadTestReplayContract, ProviderQuotaIsolationContract`

#### v2.6.13_execution_exit_safety

- Theme: Lifecycle, exit execution, exit policy, circuit breaker, emergency exit journal, and exit queue health must be machine-checkable before normal tiny can be trusted.
- Contracts: `CircuitBreakerPositionPolicy, EmergencyExitJournal, ExitExecutionStateMachine, ExitPolicyContract, ExitQueueHealthContract`

#### v2.6.13_release_experiment_safety

- Theme: Secrets lifecycle, system SLO, no-trade root cause, release complexity, backpressure, budget reserve, holdout blinding, manual override quarantine, contract tests, and adversarial replay must be machine-checkable before normal tiny.
- Contracts: `BackpressurePolicy, BudgetReserveContract, SystemSLO`

#### v2.6.15_queue_fleet_admin_parser_training_serving

- Theme: Queue durability, worker readiness, admin command safety, parser ambiguity, and training-serving skew.
- Contracts: `BackgroundJobRegistryContract, CandidateCancellationContract, QueueAckNackContract, QueueDurabilityContract, RetryPolicyCatalogContract, RetryStormControlContract, ScheduledJobModeGateContract, ServiceReadinessProbeContract, WarmStartSafetyContract, WorkerHeartbeatContract`

#### v2.6.16_bypass_static_resources_source_risk_evidence

- Theme: Bypass prevention, CI/CD enforcement, resource exhaustion, source authenticity, post-entry risk, and evidence anchoring.
- Contracts: `ConnectionPoolPartitionContract, PipelineProgressInvariant, ResourceExhaustionContract, SilentWorkerDeathDetector, ThreadPoolIsolationContract`

## S12 - Execution, Idempotency, Entry, Fill, Exit, and Paper Realism

- Section mode target: `ultra_tiny_blocking`
- Catalog contract count: `12`
- Gap batch count: `3`

### Catalog Contracts

#### AdversarialExecutionSimulationContract

- Section: `S12`
- Mode target: `normal_tiny_blocking`
- Failure action: `execution_policy_blocked`
- Required fields: `simulation_id, execution_policy_version, attack_scenario, safety_result, checked_at`

#### DynamicTokenAuthorityChangeContract

- Section: `S12`
- Mode target: `normal_tiny_blocking`
- Failure action: `position_risk_recheck`
- Required fields: `token_ca, authority_type, previous_authority_hash, current_authority_hash, risk_action`

#### EntryExecutionStateMachine

- Section: `S12`
- Mode target: `ultra_tiny_blocking`
- Failure action: `abort_execution`
- Required fields: `execution_id, state, state_version, failure_reason`

#### ExecutionLeaseContract

- Section: `S12`
- Mode target: `ultra_tiny_blocking`
- Failure action: `abort_execution`
- Required fields: `lease_id, fencing_token, acquired_at, expires_at, lease_status`

#### ExitPolicyMigrationContract

- Section: `S12`
- Mode target: `normal_tiny_blocking`
- Failure action: `exit_policy_migration_blocked`
- Required fields: `position_id, old_exit_policy, new_exit_policy, migration_reason, migrated_at`

#### IdempotencyContract

- Section: `S12`
- Mode target: `ultra_tiny_blocking`
- Failure action: `reject_duplicate`
- Required fields: `decision_id, execution_id, idempotency_key, token_lifecycle_key, action`

#### IdempotencyKeyNamespaceContract

- Section: `S12`
- Mode target: `ultra_tiny_blocking`
- Failure action: `reject_key`
- Required fields: `namespace, environment_id, route, hash_algorithm, collision_policy`

#### NoFillOutcome

- Section: `S12`
- Mode target: `ultra_tiny_blocking`
- Failure action: `no_fill_record_required`
- Required fields: `no_fill_reason, missed_net_peak30, no_fill_cost, no_fill_saved_loss`

#### OpenPositionPolicyMigrationContract

- Section: `S12`
- Mode target: `normal_tiny_blocking`
- Failure action: `open_position_blocked`
- Required fields: `position_id, old_exit_policy, new_exit_policy, migration_reason`

#### PositionOwnershipTransferContract

- Section: `S12`
- Mode target: `normal_tiny_blocking`
- Failure action: `ownership_transfer_blocked`
- Required fields: `position_id, from_owner, to_owner, transfer_reason`

#### RiskRevalidationAfterEntryContract

- Section: `S12`
- Mode target: `normal_tiny_blocking`
- Failure action: `exit_safety_priority`
- Required fields: `position_id, risk_event_id, risk_status, exit_safety_action`

#### StateVersionFencing

- Section: `S12`
- Mode target: `ultra_tiny_blocking`
- Failure action: `abort_execution`
- Required fields: `state_version_at_decision, state_version_at_execution, requires_revalidation_before_fill`

### Gap Register Coverage

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `OpenPositionPolicyMigrationContract, PositionOwnershipTransferContract`

#### v2.6.15_queue_fleet_admin_parser_training_serving

- Theme: Queue durability, worker readiness, admin command safety, parser ambiguity, and training-serving skew.
- Contracts: `ExitPolicyMigrationContract`

#### v2.6.16_bypass_static_resources_source_risk_evidence

- Theme: Bypass prevention, CI/CD enforcement, resource exhaustion, source authenticity, post-entry risk, and evidence anchoring.
- Contracts: `AdversarialExecutionSimulationContract, DynamicTokenAuthorityChangeContract, RiskRevalidationAfterEntryContract`

## S13 - Ledger, Capital, Recovery, and Incident Evidence

- Section mode target: `mvp_and_normal_tiny_blocking`
- Catalog contract count: `14`
- Gap batch count: `3`

### Catalog Contracts

#### BackupRestoreDrillContract

- Section: `S13`
- Mode target: `normal_tiny_blocking`
- Failure action: `no_normal_tiny`
- Required fields: `drill_id, backup_set_id, restored_world_hash, restore_started_at, restore_completed_at`

#### CapitalReservationPolicy

- Section: `S13`
- Mode target: `ultra_tiny_blocking`
- Failure action: `no_new_entry`
- Required fields: `reservation_id, position_id, reserved_capital, reservation_ttl, release_reason`

#### CircuitBreakerResumeContract

- Section: `S13`
- Mode target: `normal_tiny_blocking`
- Failure action: `resume_blocked`
- Required fields: `breaker_id, root_cause_fixed, evidence_freeze_id, health_checks_passed, resumed_at`

#### CrashRecoveryStateMachine

- Section: `S13`
- Mode target: `ultra_tiny_blocking`
- Failure action: `no_new_entry`
- Required fields: `recovery_id, state, orphan_scan_result, reconcile_result`

#### DoubleEntryLedgerInvariantContract

- Section: `S13`
- Mode target: `ultra_tiny_blocking`
- Failure action: `no_new_entry`
- Required fields: `ledger_checkpoint_id, available_capital, reserved_capital, open_exposure, ledger_hash`

#### IncidentEvidenceFreezeContract

- Section: `S13`
- Mode target: `normal_tiny_blocking`
- Failure action: `repair_blocked`
- Required fields: `freeze_id, incident_id, frozen_event_range, frozen_config_hash, frozen_at`

#### IncidentPostmortemContract

- Section: `S13`
- Mode target: `normal_tiny_blocking`
- Failure action: `repair_blocked`
- Required fields: `postmortem_id, incident_id, root_cause, corrective_actions, approved_at`

#### LedgerSnapshotHashContract

- Section: `S13`
- Mode target: `ultra_tiny_blocking`
- Failure action: `dirty_paper_data`
- Required fields: `ledger_snapshot_id, ledger_checkpoint_id, snapshot_hash, replay_hash, verified_at`

#### OpenPositionValuationContract

- Section: `S13`
- Mode target: `normal_tiny_blocking`
- Failure action: `open_position_blocked`
- Required fields: `position_id, valuation_ts, quote_source, valuation_price, valuation_hash`

#### PaperCapitalLedgerContract

- Section: `S13`
- Mode target: `ultra_tiny_blocking`
- Failure action: `no_new_entry`
- Required fields: `capital_ledger_id, available_capital, reserved_capital, open_exposure, capital_ledger_hash`

#### PaperPositionLedgerContract

- Section: `S13`
- Mode target: `ultra_tiny_blocking`
- Failure action: `dirty_paper_data`
- Required fields: `position_id, execution_id, decision_id, remaining_size, ledger_hash`

#### ReconciliationDiffContract

- Section: `S13`
- Mode target: `mvp_blocking`
- Failure action: `repair_audit_missing`
- Required fields: `reconciliation_id, before_hash, after_hash, impact_scope`

#### ReconciliationPolicyContract

- Section: `S13`
- Mode target: `shadow_blocking`
- Failure action: `reconciliation_blocked`
- Required fields: `mismatch_class, repair_class, audit_required, promotion_evidence_allowed`

#### ResumeDrainPolicy

- Section: `S13`
- Mode target: `ultra_tiny_blocking`
- Failure action: `no_normal_tiny`
- Required fields: `drain_id, queued_candidates_revalidated, expired_candidates_emitted, resume_drain_completed_at`

### Gap Register Coverage

#### v2.6.13_delivery_traceability

- Theme: Reconciliation, dashboard freshness, traceability, implementation issue graph, module closure, and decommission policy must be machine-checkable before normal tiny can be trusted.
- Contracts: `ReconciliationPolicyContract`

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `BackupRestoreDrillContract, CircuitBreakerResumeContract, DoubleEntryLedgerInvariantContract, IncidentEvidenceFreezeContract, IncidentPostmortemContract, LedgerSnapshotHashContract, ReconciliationDiffContract`

#### v2.6.15_queue_fleet_admin_parser_training_serving

- Theme: Queue durability, worker readiness, admin command safety, parser ambiguity, and training-serving skew.
- Contracts: `OpenPositionValuationContract`
