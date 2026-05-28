import sys

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_record_markov_shadow_forecasts import (  # noqa: E402
    MARKOV_SHADOW_EVENT_TYPE,
    record_markov_shadow_forecasts,
)


def _ts(seq):
    return f"2026-05-27T00:{seq // 60:02d}:{seq % 60:02d}Z"


def _append(log, event_type, token, state=None, *, pool="pool-a", epoch=0, seq=1, **payload):
    base_payload = {
        "token_ca": token,
        "chain": "solana",
        "canonical_pool_group": pool,
        "lifecycle_epoch": epoch,
        "decision_available_at": _ts(seq),
    }
    if state:
        base_payload["lifecycle_state"] = state
    base_payload.update(payload)
    return log.append_event(
        event_type=event_type,
        aggregate_id=f"token_lifecycle:solana:{token}:{pool}:{epoch}",
        payload=base_payload,
        source="test",
        idempotency_key=f"test:{event_type}:{token}:{state}:{seq}",
        observed_at=_ts(seq),
        available_at=_ts(seq),
    )["event"]


def _append_state(log, token, state, *, seq, pool="pool-a"):
    return _append(log, "telegram_signal_seen", token, state, seq=seq, pool=pool)


def _append_hard_gate_candidate(log, token, *, seq_start=20, pool="pool-a", reclaim_confirmed=True):
    _append(
        log,
        "telegram_signal_seen",
        token,
        "TELEGRAM_SEEN",
        seq=seq_start,
        pool=pool,
        telegram_seen=True,
        realtime_observable=True,
        source_dog_label="gold",
    )
    _append(
        log,
        "realtime_clean_detector_recorded",
        token,
        "TRADABLE_CLEAN",
        seq=seq_start + 1,
        pool=pool,
        realtime_clean=True,
        clean_standard_version="v2.7.0.test",
        clean_observation_type="TRADABLE_CLEAN_OBSERVED",
        quote_age_sec=1.0,
        entry_quote_available=True,
        exit_quote_available=True,
        entry_quote_available_at=_ts(seq_start + 1),
        exit_quote_available_at=_ts(seq_start + 1),
        entry_quote_executable=True,
        exit_quote_executable=True,
        liquidity_ok=True,
        critical_risk_ok=True,
    )
    _append(
        log,
        "ex_ante_feasibility_recorded",
        token,
        "TRADABLE_CLEAN",
        seq=seq_start + 2,
        pool=pool,
        ex_ante_feasible=True,
        feasibility_class="round_trip_executable",
        feasibility_policy_version="v2.7.0.test",
        decision_ts=_ts(seq_start + 2),
        decision_available_at=_ts(seq_start + 2),
        used_future_peak=False,
        used_future_outcome=False,
        used_posthoc_label=False,
        forbidden_future_fields_used=[],
    )
    return _append(
        log,
        "earliest_actionable_time_recorded",
        token,
        "RECLAIM_CONFIRMED" if reclaim_confirmed else "TRADABLE_CLEAN",
        seq=seq_start + 3,
        pool=pool,
        earliest_actionable_policy_version="v2.7.0.test",
        earliest_actionable_ts=_ts(seq_start + 1),
        required_inputs_available_at={
            "realtime_clean": _ts(seq_start + 1),
            "ex_ante_feasibility": _ts(seq_start + 2),
            "entry_quote_executable": _ts(seq_start + 1),
            "exit_quote_executable": _ts(seq_start + 1),
        },
        peak_ts=_ts(seq_start + 20),
        counterfactual_entry_ts=_ts(seq_start + 3),
        actionable_before_peak=True,
        earliest_actionable_reason="all_required_inputs_before_counterfactual_entry",
        reclaim_confirmed=reclaim_confirmed,
        not_overextended=True,
    )


def _markov_payloads(log_dir):
    return [
        event["payload"]
        for event in V27EventLog(log_dir).iter_events()
        if event.get("event_type") == MARKOV_SHADOW_EVENT_TYPE
    ]


def test_markov_shadow_forecast_records_post_hard_gate_feature_snapshot(tmp_path):
    log = V27EventLog(tmp_path)
    _append_state(log, "hist-win", "TELEGRAM_SEEN", seq=1)
    _append_state(log, "hist-win", "RECLAIM_CONFIRMED", seq=2)
    _append_state(log, "hist-win", "PEAK30", seq=3)
    _append_state(log, "hist-stop", "TELEGRAM_SEEN", seq=4)
    _append_state(log, "hist-stop", "RECLAIM_CONFIRMED", seq=5)
    _append_state(log, "hist-stop", "STOP_BEFORE_PEAK", seq=6)
    _append_hard_gate_candidate(log, "candidate", seq_start=20)

    report = record_markov_shadow_forecasts(tmp_path)

    assert report["appended"] == 1
    payload = _markov_payloads(tmp_path)[0]
    assert payload["entry_gate_allowed"] is False
    assert payload["paper_entry_action"] == "none"
    assert payload["hard_gate_passed"] is True
    assert payload["hard_gate_evidence"]["checks"]["d3a_externally_actionable"] is True
    assert payload["feature_vector_snapshot"]["feature_research_only"] is True
    assert "p_absorb_peak30" in payload["feature_vector_snapshot"]["feature_values_serialized"]
    assert payload["decision_audit_shadow"]["entry_gate_allowed"] is False
    assert payload["decision_audit_shadow"]["used_future_peak"] is False


def test_markov_shadow_forecast_does_not_record_before_reclaim_gate(tmp_path):
    log = V27EventLog(tmp_path)
    _append_state(log, "hist-win", "RECLAIM_CONFIRMED", seq=1)
    _append_state(log, "hist-win", "PEAK30", seq=2)
    _append_hard_gate_candidate(log, "candidate", seq_start=20, reclaim_confirmed=False)

    report = record_markov_shadow_forecasts(tmp_path)

    assert report["appended"] == 0
    assert _markov_payloads(tmp_path) == []
    skipped_reasons = {
        reason
        for item in report["skipped"]
        for reason in item["blocking_reasons"]
    }
    assert "reclaim_confirmed_missing_or_false" in skipped_reasons


def test_markov_shadow_forecast_cutoff_excludes_future_outcome(tmp_path):
    log = V27EventLog(tmp_path)
    _append_state(log, "hist-stop", "RECLAIM_CONFIRMED", seq=1)
    _append_state(log, "hist-stop", "STOP_BEFORE_PEAK", seq=2)
    gate_event = _append_hard_gate_candidate(log, "candidate", seq_start=20)
    future_event = _append(
        log,
        "trade_outcome_label_recorded",
        "candidate",
        "PEAK30",
        seq=50,
        net_delayed_executable_peak_3s=0.72,
        would_stop_before_peak=False,
    )

    report = record_markov_shadow_forecasts(tmp_path)

    assert report["appended"] == 1
    payload = _markov_payloads(tmp_path)[0]
    assert payload["matrix_build_cutoff_seq"] == gate_event["global_seq"]
    assert payload["matrix_build_cutoff_seq"] < future_event["global_seq"]
    assert payload["p_absorb_peak30"] == 0.0
    assert payload["p_absorb_stop_before_peak"] == 1.0
