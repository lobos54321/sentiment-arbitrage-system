# Telegram Dog Regime Capture System v2.7.0 Canonical Spec - Part 1

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

## S00 - Scope, Boundaries, and Release Objective

- Section mode target: `all_modes`
- Catalog contract count: `1`
- Gap batch count: `1`

### Catalog Contracts

#### RenderedSpecViewContract

- Section: `S00`
- Mode target: `observe_only_blocking`
- Failure action: `normal_tiny_disabled`
- Required fields: `source_spec_hash, rendered_doc_hash, render_validation_status, section_count`

### Gap Register Coverage

#### v2.6.13_spec_governance_confidence_fill_feasibility

- Theme: Rendered spec views, health enums, contract lifecycle, objective priority, goal confidence, fill-time anchors, and ex-ante/posthoc feasibility must be machine-checkable before shadow or normal tiny evidence can be trusted.
- Contracts: `RenderedSpecViewContract`

## S01 - Canonical Spec and Machine-Checkable Governance

- Section mode target: `all_modes`
- Catalog contract count: `5`
- Gap batch count: `2`

### Catalog Contracts

#### CanonicalSerializationContract

- Section: `S01`
- Mode target: `all_modes`
- Failure action: `spec_dirty`
- Required fields: `serialization_version, json_sort_keys, time_format, decimal_policy`

#### CanonicalSpecIntegrityContract

- Section: `S01`
- Mode target: `all_modes`
- Failure action: `global_promotion_disabled`
- Required fields: `spec_id, spec_version, spec_hash, section_id, contract_id`

#### NumericPrecisionContract

- Section: `S01`
- Mode target: `all_modes`
- Failure action: `spec_dirty`
- Required fields: `decimal_scale, rounding_mode, overflow_policy, unit`

#### SpecChangeImpactAnalysisContract

- Section: `S01`
- Mode target: `mvp_blocking`
- Failure action: `spec_change_blocked`
- Required fields: `spec_change_id, affected_contracts, affected_modes, impact_hash, approved_at`

#### SpecConsistencyLinterContract

- Section: `S01`
- Mode target: `all_modes`
- Failure action: `spec_dirty`
- Required fields: `linter_version, checked_sections, conflict_count, result`

### Gap Register Coverage

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `CanonicalSerializationContract, NumericPrecisionContract`

#### v2.6.16_bypass_static_resources_source_risk_evidence

- Theme: Bypass prevention, CI/CD enforcement, resource exhaustion, source authenticity, post-entry risk, and evidence anchoring.
- Contracts: `SpecChangeImpactAnalysisContract, SpecConsistencyLinterContract`

## S02 - Metrics, Thresholds, Windows, and Evidence Eligibility

- Section mode target: `normal_tiny_blocking`
- Catalog contract count: `5`
- Gap batch count: `3`

### Catalog Contracts

#### GoalConfidenceContract

- Section: `S02`
- Mode target: `normal_tiny_blocking`
- Failure action: `promotion_inconclusive`
- Required fields: `metric_id, numerator, denominator, wilson_lower_bound, beta_posterior_lower_bound`

#### MetricBackfillImpactContract

- Section: `S02`
- Mode target: `normal_tiny_blocking`
- Failure action: `metric_backfill_blocked`
- Required fields: `backfill_id, metric_id, impact_scope, impact_report_hash`

#### MetricDefinitionRegistry

- Section: `S02`
- Mode target: `observe_only_blocking`
- Failure action: `metric_invalid`
- Required fields: `metric_id, metric_name, formula, numerator_definition, denominator_definition, window_id, event_time_basis, metric_version, metric_hash`

#### MetricsWindowContract

- Section: `S02`
- Mode target: `normal_tiny_blocking`
- Failure action: `metric_invalid`
- Required fields: `metric_id, window_id, window_start, window_end, metric_version`

#### ThresholdCatalogContract

- Section: `S02`
- Mode target: `observe_only_blocking`
- Failure action: `policy_bundle_incompatible`
- Required fields: `threshold_id, threshold_name, threshold_value, unit, comparison_operator, scope, applies_to_metric, applies_to_mode, owner, source_spec_section_id, policy_bundle_id, effective_from, effective_to, change_reason, approval_id, threshold_hash`

### Gap Register Coverage

#### v2.6.13_metric_threshold_truth_foundation

- Theme: Metric and threshold truth sources required before any mode can trust dashboard, release, or policy evidence.
- Contracts: `MetricDefinitionRegistry, ThresholdCatalogContract`

#### v2.6.13_spec_governance_confidence_fill_feasibility

- Theme: Rendered spec views, health enums, contract lifecycle, objective priority, goal confidence, fill-time anchors, and ex-ante/posthoc feasibility must be machine-checkable before shadow or normal tiny evidence can be trusted.
- Contracts: `GoalConfidenceContract`

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `MetricBackfillImpactContract`

## S03 - Runtime, Environment, Build, and Supply Chain

- Section mode target: `normal_tiny_blocking`
- Catalog contract count: `4`
- Gap batch count: `2`

### Catalog Contracts

#### ClockRollbackGuardContract

- Section: `S03`
- Mode target: `mvp_blocking`
- Failure action: `time_dirty`
- Required fields: `clock_source, wall_clock_ts, monotonic_ts, rollback_detected, guard_action`

#### EnvironmentSeparationContract

- Section: `S03`
- Mode target: `observe_only_blocking`
- Failure action: `environment_contamination`
- Required fields: `environment_id, environment_type, allowed_event_logs, allowed_databases, allowed_provider_keys, allowed_routes, allowed_modes, write_permissions, read_permissions, environment_hash`

#### PaperModeSafetyBoundary

- Section: `S03`
- Mode target: `all_modes`
- Failure action: `global_circuit_breaker`
- Required fields: `paper_mode_required, real_order_router_disabled, live_private_key_present, network_transaction_signing_enabled`

#### RuntimeConfigDriftContract

- Section: `S03`
- Mode target: `observe_only_blocking`
- Failure action: `runtime_config_drift`
- Required fields: `runtime_config_hash, env_vars_hash, feature_flags_hash, provider_config_hash, route_registry_hash, source_registry_hash, threshold_catalog_hash, metric_registry_hash, policy_bundle_hash, expected_hash, drift_detected, drift_action`

### Gap Register Coverage

#### v2.6.13_runtime_environment_truth_foundation

- Theme: Runtime config drift and environment separation must be observe-only blockers before decisions, dashboards, backfills, or paper evidence can be trusted.
- Contracts: `EnvironmentSeparationContract, RuntimeConfigDriftContract`

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `ClockRollbackGuardContract`

## S04 - Source Registry, Telegram Ingestion, Parser, and Source Authenticity

- Section mode target: `shadow_blocking`
- Catalog contract count: `15`
- Gap batch count: `4`

### Catalog Contracts

#### ContractLifecycleContract

- Section: `S04`
- Mode target: `observe_only_blocking`
- Failure action: `shadow_only`
- Required fields: `contract_id, contract_version, status, allowed_modes, contract_tests_status`

#### HealthStateEnumContract

- Section: `S04`
- Mode target: `observe_only_blocking`
- Failure action: `health_state_invalid`
- Required fields: `health_component, health_state, blocking_modes, recovery_condition`

#### ImageOCRSignalPolicy

- Section: `S04`
- Mode target: `shadow_blocking`
- Failure action: `ocr_signal_quarantined`
- Required fields: `message_id, ocr_engine_version, image_hash, confidence, policy_action`

#### InputSanitizationContract

- Section: `S04`
- Mode target: `observe_only_blocking`
- Failure action: `shadow_only_security_alert`
- Required fields: `source, raw_value, normalized_value, payload_schema_valid, unsafe_pattern_detected`

#### ObjectivePriorityContract

- Section: `S04`
- Mode target: `observe_only_blocking`
- Failure action: `shadow_only`
- Required fields: `objective_conflict_id, conflicting_objectives, chosen_objective, reason`

#### ParserAmbiguityContract

- Section: `S04`
- Mode target: `shadow_blocking`
- Failure action: `observe_only`
- Required fields: `message_id, candidate_anchors, selected_anchor, ambiguity_reason`

#### ParserCanaryCorpusContract

- Section: `S04`
- Mode target: `shadow_blocking`
- Failure action: `parser_promotion_blocked`
- Required fields: `corpus_id, parser_version, canary_case_count, failure_count, checked_at`

#### ParserConfusablesContract

- Section: `S04`
- Mode target: `shadow_blocking`
- Failure action: `parser_output_quarantined`
- Required fields: `message_id, confusable_token, normalized_token, risk_class, policy_action`

#### PremiumSourceAccessHealthContract

- Section: `S04`
- Mode target: `shadow_blocking`
- Failure action: `source_access_degraded`
- Required fields: `source_id, access_probe_id, auth_state, last_success_at, failure_action`

#### RouteRegistryContract

- Section: `S04`
- Mode target: `observe_only_blocking`
- Failure action: `observe_only`
- Required fields: `route_id, allowed_modes, direct_entry_allowed, kill_switch`

#### SourceAuthenticityContract

- Section: `S04`
- Mode target: `shadow_blocking`
- Failure action: `observe_only`
- Required fields: `source_id, channel_id, authenticity_status, evidence_hash`

#### SourceImpersonationDetector

- Section: `S04`
- Mode target: `shadow_blocking`
- Failure action: `source_quarantined`
- Required fields: `source_id, message_id, impersonation_signal, confidence, action`

#### SourceRegistryContract

- Section: `S04`
- Mode target: `observe_only_blocking`
- Failure action: `observe_only`
- Required fields: `telegram_source_id, telegram_channel_id, allowed_modes, source_status`

#### TelegramForwardedMessagePolicy

- Section: `S04`
- Mode target: `shadow_blocking`
- Failure action: `message_quarantined`
- Required fields: `message_id, forwarded_from, source_policy, trust_level, action`

#### TelegramSessionSecurityContract

- Section: `S04`
- Mode target: `mvp_blocking`
- Failure action: `ingestion_disabled`
- Required fields: `session_id, account_id, auth_state, device_fingerprint_hash, checked_at`

### Gap Register Coverage

#### v2.6.13_spec_governance_confidence_fill_feasibility

- Theme: Rendered spec views, health enums, contract lifecycle, objective priority, goal confidence, fill-time anchors, and ex-ante/posthoc feasibility must be machine-checkable before shadow or normal tiny evidence can be trusted.
- Contracts: `ContractLifecycleContract, HealthStateEnumContract, ObjectivePriorityContract`

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `ParserCanaryCorpusContract`

#### v2.6.15_queue_fleet_admin_parser_training_serving

- Theme: Queue durability, worker readiness, admin command safety, parser ambiguity, and training-serving skew.
- Contracts: `ParserAmbiguityContract, PremiumSourceAccessHealthContract, TelegramForwardedMessagePolicy`

#### v2.6.16_bypass_static_resources_source_risk_evidence

- Theme: Bypass prevention, CI/CD enforcement, resource exhaustion, source authenticity, post-entry risk, and evidence anchoring.
- Contracts: `ImageOCRSignalPolicy, ParserConfusablesContract, SourceAuthenticityContract, SourceImpersonationDetector, TelegramSessionSecurityContract`

## S05 - Token Identity, Pool Resolution, and Denominator Truth

- Section mode target: `shadow_blocking`
- Catalog contract count: `9`
- Gap batch count: `3`

### Catalog Contracts

#### ChainConfigContract

- Section: `S05`
- Mode target: `observe_only_blocking`
- Failure action: `observe_only`
- Required fields: `chain, native_unit, quote_mint, finality_rule, address_validator`

#### DataUnitContract

- Section: `S05`
- Mode target: `observe_only_blocking`
- Failure action: `shadow_only`
- Required fields: `token_decimals, quote_decimals, price_unit, normalized_price, unit_validation_status`

#### DenominatorDedupContract

- Section: `S05`
- Mode target: `shadow_blocking`
- Failure action: `denominator_dirty`
- Required fields: `denominator_dedup_key, canonical_pool_group, lifecycle_epoch, merged_signal_ids`

#### IdentityMergeSplitContract

- Section: `S05`
- Mode target: `shadow_blocking`
- Failure action: `identity_dirty`
- Required fields: `merge_split_id, old_identity_key, new_identity_key, resolution_reason`

#### ReKeyingContract

- Section: `S05`
- Mode target: `shadow_blocking`
- Failure action: `identity_dirty`
- Required fields: `old_key, new_key, rekey_reason, supersedes_event_id`

#### SignalCreditAssignmentContract

- Section: `S05`
- Mode target: `shadow_blocking`
- Failure action: `denominator_dirty`
- Required fields: `credited_signal_id, credit_assignment_reason, credit_policy_version`

#### SourceDogLabelContract

- Section: `S05`
- Mode target: `shadow_blocking`
- Failure action: `denominator_dirty`
- Required fields: `source_dog_label_version, source_reference_price_type, source_label_window, source_label_available_at`

#### SourceGapBackfillBoundary

- Section: `S05`
- Mode target: `shadow_blocking`
- Failure action: `backfill_research_only`
- Required fields: `backfill_id, source_id, gap_window, allowed_fields, backfilled_at`

#### TokenIdentityContract

- Section: `S05`
- Mode target: `observe_only_blocking`
- Failure action: `shadow_only`
- Required fields: `chain, token_ca, normalized_ca, checksum, identity_confidence`

### Gap Register Coverage

#### v2.6.13_identity_unit_provider_finality

- Theme: Token identity, data unit, chain finality, and provider schema truth must be machine-checkable before denominator, label, or training evidence can be trusted.
- Contracts: `DataUnitContract, TokenIdentityContract`

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `IdentityMergeSplitContract, ReKeyingContract`

#### v2.6.15_queue_fleet_admin_parser_training_serving

- Theme: Queue durability, worker readiness, admin command safety, parser ambiguity, and training-serving skew.
- Contracts: `SourceGapBackfillBoundary`

## S06 - Event Log, Sequencing, Outbox, Projection, and Replay Safety

- Section mode target: `mvp_blocking`
- Catalog contract count: `23`
- Gap batch count: `4`

### Catalog Contracts

#### AggregateBoundaryContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `event_log_unhealthy`
- Required fields: `aggregate_type, aggregate_id_pattern, sequence_scope, owner_store`

#### CacheInvalidationContract

- Section: `S06`
- Mode target: `shadow_blocking`
- Failure action: `shadow_only`
- Required fields: `cache_key, source_event_seq, ttl_ms, invalidated_by_event_type`

#### ConsumerCheckpointContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `projection_unhealthy`
- Required fields: `consumer_name, aggregate_id, last_applied_seq, checkpoint_seq, checkpoint_tx_id`

#### DBLockContentionPolicy

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `storage_degraded`
- Required fields: `store, lock_name, contention_threshold_ms, retry_policy, fallback_action`

#### DatabaseTransactionIsolationContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `storage_degraded`
- Required fields: `store, isolation_level, transaction_id, deadlock_retry_policy, invariant_scope`

#### DeadLetterQueueContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `shadow_only_if_critical`
- Required fields: `event_id, consumer_name, failure_count, poison_event_class, moved_to_dlq_at`

#### DecisionReadModelFreshnessContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `direct_replay_or_shadow`
- Required fields: `read_model_seq, event_log_latest_seq, max_allowed_lag_seq, read_model_fresh_enough`

#### DistributedLockBackendHealthContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `lock_backend_unhealthy`
- Required fields: `backend_name, health_status, stale_read_detected, split_brain_detected`

#### EnumEvolutionContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `schema_change_blocked`
- Required fields: `enum_name, old_value, new_value, compatibility_policy, migration_action`

#### EventSchemaCompatibilityContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `event_rejected`
- Required fields: `event_type, schema_version, producer_version, consumer_version, compatibility_result`

#### EventSemanticsContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `event_rejected`
- Required fields: `event_id, event_type, observed_at, available_at, idempotency_key`

#### EventSequencerContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `event_log_unhealthy`
- Required fields: `monotonic_ingest_seq, aggregate_id, aggregate_seq, sequencer_epoch`

#### FillTimeAnchorContract

- Section: `S06`
- Mode target: `shadow_blocking`
- Failure action: `trade_outcome_invalid`
- Required fields: `decision_ts, decision_available_at, simulated_fill_ts, position_open_confirmed_ts`

#### ManualReplaySafetyContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `replay_aborted`
- Required fields: `replay_id, operator_id, side_effect_mode, allowed_write_targets, started_at`

#### ProjectionHandlerIdempotencyContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `projection_unhealthy`
- Required fields: `consumer_name, event_id, idempotency_key, apply_result_hash`

#### ProjectionOrderingContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `projection_rebuild_required`
- Required fields: `aggregate_id, aggregate_seq, last_applied_seq, out_of_order_detected`

#### ProjectionVersionIsolationContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `mixed_projection_blocked`
- Required fields: `projection_version, environment_id, active_dashboard_version, research_dashboard_version`

#### ReplaySideEffectIsolationContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `replay_aborted`
- Required fields: `replay_id, side_effect_mode, write_targets_allowed, provider_calls_allowed`

#### SnapshotCompactionInvariantContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `snapshot_untrusted`
- Required fields: `snapshot_id, full_replay_hash, compaction_hash, verified_at`

#### SnapshotCompactionReadBarrier

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `snapshot_untrusted`
- Required fields: `snapshot_id, compaction_seq, reader_checkpoint_seq, barrier_passed, checked_at`

#### SyntheticSentinelEventContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `projection_unhealthy`
- Required fields: `sentinel_id, event_type, expected_projection_delta, observed_projection_delta, checked_at`

#### TieBreakOrderingContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `replay_nondeterministic`
- Required fields: `monotonic_ingest_seq, source_seq, event_type_priority, event_id`

#### TransactionalOutboxContract

- Section: `S06`
- Mode target: `mvp_blocking`
- Failure action: `shadow_only_if_stuck`
- Required fields: `outbox_id, event_type, aggregate_id, idempotency_key, status`

### Gap Register Coverage

#### v2.6.13_spec_governance_confidence_fill_feasibility

- Theme: Rendered spec views, health enums, contract lifecycle, objective priority, goal confidence, fill-time anchors, and ex-ante/posthoc feasibility must be machine-checkable before shadow or normal tiny evidence can be trusted.
- Contracts: `FillTimeAnchorContract`

#### v2.6.14_evidence_finality_replay_safety_fleet

- Theme: Evidence finality, replay safety, provider proof, ledger invariants, and fleet consistency.
- Contracts: `AggregateBoundaryContract, ConsumerCheckpointContract, DistributedLockBackendHealthContract, EventSequencerContract, ManualReplaySafetyContract, ProjectionVersionIsolationContract, ReplaySideEffectIsolationContract, SnapshotCompactionInvariantContract, SyntheticSentinelEventContract`

#### v2.6.15_queue_fleet_admin_parser_training_serving

- Theme: Queue durability, worker readiness, admin command safety, parser ambiguity, and training-serving skew.
- Contracts: `EnumEvolutionContract, EventSchemaCompatibilityContract, ProjectionHandlerIdempotencyContract, SnapshotCompactionReadBarrier`

#### v2.6.16_bypass_static_resources_source_risk_evidence

- Theme: Bypass prevention, CI/CD enforcement, resource exhaustion, source authenticity, post-entry risk, and evidence anchoring.
- Contracts: `DBLockContentionPolicy, DatabaseTransactionIsolationContract`
