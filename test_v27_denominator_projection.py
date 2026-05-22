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
