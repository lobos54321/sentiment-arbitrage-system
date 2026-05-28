import sys

sys.path.insert(0, "scripts")

from telegram_lifecycle_markov import (  # noqa: E402
    STATE_INDEX,
    build_lifecycle_forecast_snapshot,
    build_transition_counts,
    distribution_after_steps,
    normalize_transition_matrix,
)


def _event(token, seq, state, epoch=0):
    return {
        "event_id": f"{token}-{seq}-{state}",
        "token_lifecycle_key": token,
        "lifecycle_epoch": epoch,
        "monotonic_ingest_seq": seq,
        "decision_available_at": f"2026-05-27T00:00:{seq:02d}Z",
        "lifecycle_state": state,
    }


def test_absorbing_rows_are_self_loops_even_without_outgoing_counts():
    counts = [[0 for _ in STATE_INDEX] for _ in STATE_INDEX]
    matrix = normalize_transition_matrix(counts)

    peak_idx = STATE_INDEX["PEAK30"]
    stop_idx = STATE_INDEX["STOP_BEFORE_PEAK"]

    assert matrix[peak_idx][peak_idx] == 1.0
    assert sum(matrix[peak_idx]) == 1.0
    assert matrix[stop_idx][stop_idx] == 1.0
    assert sum(matrix[stop_idx]) == 1.0


def test_cutoff_seq_prevents_future_lifecycle_events_from_entering_matrix():
    events = [
        _event("winner", 1, "TELEGRAM_SEEN"),
        _event("winner", 2, "QUOTE_CLEAN"),
        _event("winner", 3, "RECLAIM_CONFIRMED"),
        _event("winner", 4, "PEAK30"),
        _event("future", 30, "TELEGRAM_SEEN"),
        _event("future", 31, "CRASH_DEAD"),
    ]

    counts = build_transition_counts(events, cutoff_seq=10)

    assert counts[STATE_INDEX["TELEGRAM_SEEN"]][STATE_INDEX["QUOTE_CLEAN"]] == 1
    assert counts[STATE_INDEX["TELEGRAM_SEEN"]][STATE_INDEX["CRASH_DEAD"]] == 0


def test_n_step_distribution_and_competing_risk_snapshot_from_lifecycle_events():
    events = [
        _event("winner", 1, "TELEGRAM_SEEN"),
        _event("winner", 2, "QUOTE_CLEAN"),
        _event("winner", 3, "RECLAIM_CONFIRMED"),
        _event("winner", 4, "PEAK30"),
        _event("loser", 5, "TELEGRAM_SEEN"),
        _event("loser", 6, "QUOTE_CLEAN"),
        _event("loser", 7, "RECLAIM_CONFIRMED"),
        _event("loser", 8, "STOP_BEFORE_PEAK"),
    ]

    snapshot = build_lifecycle_forecast_snapshot(
        events,
        start_state="RECLAIM_CONFIRMED",
        cutoff_seq=20,
        horizons=(1, 2),
        max_absorption_steps=2,
    )

    assert snapshot["sample_n"] == 6
    assert snapshot["transition_matrix"]["RECLAIM_CONFIRMED"]["PEAK30"] == 0.5
    assert snapshot["transition_matrix"]["RECLAIM_CONFIRMED"]["STOP_BEFORE_PEAK"] == 0.5
    assert snapshot["n_step_forecasts"]["1"]["PEAK30"] == 0.5
    assert snapshot["n_step_forecasts"]["1"]["STOP_BEFORE_PEAK"] == 0.5
    assert snapshot["absorption_forecast"]["p_absorb_peak30"] == 0.5
    assert snapshot["absorption_forecast"]["p_absorb_stop_before_peak"] == 0.5


def test_markov_layer_is_explicitly_not_an_entry_gate():
    snapshot = build_lifecycle_forecast_snapshot(
        [
            _event("winner", 1, "TELEGRAM_SEEN"),
            _event("winner", 2, "QUOTE_CLEAN"),
            _event("winner", 3, "PEAK30"),
        ],
        start_state="QUOTE_CLEAN",
        cutoff_seq=10,
        horizons=(1,),
    )

    boundaries = snapshot["contract_boundaries"]
    assert boundaries["mode_target"] == "shadow_first"
    assert boundaries["entry_gate_allowed"] is False
    assert boundaries["ordinary_stationary_distribution_allowed_as_entry_gate"] is False
    assert boundaries["hmm_full_sequence_viterbi_allowed"] is False


def test_distribution_after_steps_rejects_unknown_state():
    matrix = normalize_transition_matrix([[0 for _ in STATE_INDEX] for _ in STATE_INDEX])

    try:
        distribution_after_steps(matrix, "BULL", 1)
    except ValueError as exc:
        assert "unknown start_state" in str(exc)
    else:
        raise AssertionError("expected unknown state to be rejected")
