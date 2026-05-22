import sys

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
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
    assert matrix["contract_statuses"]["InputSanitizationContract"]["status"] == "pass"
    assert matrix["contract_statuses"]["TransactionalOutboxContract"]["status"] == "pass"
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
    assert matrix["health"]["observe_only_ready"] is True
    assert matrix["health"]["shadow_ready"] is True
    assert matrix["health"]["normal_tiny_ready"] is False


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
    assert "TradeOutcomeLabelContract" not in matrix["modes"]["ultra_tiny"]["blocking_contracts"]
    assert "StandardizedStopContract" in matrix["modes"]["ultra_tiny"]["blocking_contracts"]


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
