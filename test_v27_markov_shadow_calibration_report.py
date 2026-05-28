import sys

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_markov_shadow_calibration_report import build_markov_shadow_calibration_report  # noqa: E402
from v27_record_markov_shadow_forecasts import MARKOV_SHADOW_EVENT_TYPE  # noqa: E402


def _ts(seq):
    return f"2026-05-27T01:{seq // 60:02d}:{seq % 60:02d}Z"


def _append(log, event_type, payload, seq):
    return log.append_event(
        event_type=event_type,
        aggregate_id=f"{event_type}:{payload.get('denominator_dedup_key') or payload.get('token_ca')}:{seq}",
        payload=payload,
        source="test",
        idempotency_key=f"test:{event_type}:{payload.get('denominator_dedup_key') or payload.get('token_ca')}:{seq}",
        observed_at=_ts(seq),
        available_at=_ts(seq),
    )["event"]


def _forecast_payload(key, probability, cutoff_seq, *, entry_gate_allowed=False, paper_entry_action="none"):
    token = key.split(":")[1]
    return {
        "schema_version": "v2.7.0.markov_shadow_forecast.v1",
        "shadow_forecast_id": f"markov_shadow:{key}:{cutoff_seq}",
        "denominator_dedup_key": key,
        "token_ca": token,
        "chain": "solana",
        "canonical_pool_group": key.split(":")[2],
        "lifecycle_epoch": 0,
        "entry_gate_allowed": entry_gate_allowed,
        "paper_entry_action": paper_entry_action,
        "matrix_build_cutoff_seq": cutoff_seq,
        "p_absorb_peak30": probability,
        "p_absorb_stop_before_peak": 1.0 - probability,
        "feature_vector_snapshot": {"feature_vector_hash": "a" * 64},
    }


def _outcome_payload(key, peak, *, stop=False):
    token = key.split(":")[1]
    return {
        "denominator_dedup_key": key,
        "token_ca": token,
        "chain": "solana",
        "canonical_pool_group": key.split(":")[2],
        "lifecycle_epoch": 0,
        "trade_outcome_label_version": "v2.7.0.test",
        "trade_label_available_at": _ts(120),
        "net_delayed_executable_peak_3s": peak,
        "would_stop_before_peak": stop,
    }


def _seed_pair(log, index, probability, peak, *, stop=False, cutoff_seq=0):
    key = f"solana:token{index}:pool:0"
    _append(log, MARKOV_SHADOW_EVENT_TYPE, _forecast_payload(key, probability, cutoff_seq), 20 + index * 2)
    _append(log, "trade_outcome_label_recorded", _outcome_payload(key, peak, stop=stop), 21 + index * 2)


def test_markov_calibration_reports_high_bucket_lift(tmp_path):
    log = V27EventLog(tmp_path)
    for index in range(6):
        _seed_pair(log, index, 0.72, 0.45)
    for index in range(6, 12):
        _seed_pair(log, index, 0.20, 0.12, stop=True)

    report = build_markov_shadow_calibration_report(tmp_path, min_sample=4)

    assert report["paired_sample_n"] == 12
    assert report["buckets"]["high"]["peak30_before_stop_rate"] == 1.0
    assert report["buckets"]["low"]["peak30_before_stop_rate"] == 0.0
    assert report["lift_vs_overall"] == 2.0
    assert report["promotion_allowed"] is False
    assert report["health"]["status"] == "shadow_calibration_observable"


def test_markov_calibration_excludes_outcomes_at_or_before_forecast_cutoff(tmp_path):
    log = V27EventLog(tmp_path)
    key = "solana:token1:pool:0"
    _append(log, "trade_outcome_label_recorded", _outcome_payload(key, 0.60), 1)
    _append(log, MARKOV_SHADOW_EVENT_TYPE, _forecast_payload(key, 0.80, cutoff_seq=10), 20)

    report = build_markov_shadow_calibration_report(tmp_path, min_sample=1)

    assert report["paired_sample_n"] == 0
    assert report["unpaired_forecast_count"] == 1
    assert report["health"]["status"] == "shadow_calibration_insufficient"


def test_markov_calibration_flags_boundary_violating_forecasts(tmp_path):
    log = V27EventLog(tmp_path)
    key = "solana:token1:pool:0"
    _append(
        log,
        MARKOV_SHADOW_EVENT_TYPE,
        _forecast_payload(key, 0.80, cutoff_seq=10, entry_gate_allowed=True, paper_entry_action="enter"),
        20,
    )
    _append(log, "trade_outcome_label_recorded", _outcome_payload(key, 0.60), 21)

    report = build_markov_shadow_calibration_report(tmp_path, min_sample=1)

    assert report["paired_sample_n"] == 0
    assert report["contaminated_forecast_count"] == 1
    assert "markov_shadow_boundary_violation" in report["health"]["reasons"]


def test_markov_calibration_keeps_low_sample_as_insufficient(tmp_path):
    log = V27EventLog(tmp_path)
    _seed_pair(log, 1, 0.72, 0.50)

    report = build_markov_shadow_calibration_report(tmp_path, min_sample=30)

    assert report["paired_sample_n"] == 1
    assert report["health"]["sample_ready"] is False
    assert report["promotion_allowed"] is False
    assert "sample_below_minimum" in report["health"]["reasons"]
