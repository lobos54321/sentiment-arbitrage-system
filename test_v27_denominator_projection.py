import sys

sys.path.insert(0, "scripts")

from v27_denominator_projection import build_denominator_projection  # noqa: E402
from v27_event_log import V27EventLog  # noqa: E402


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
