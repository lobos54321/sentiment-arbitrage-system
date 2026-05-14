import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from paper_learning_policy import (  # noqa: E402
    build_entry_execution_latency_audit,
    capital_tier_for_entry,
    classify_rejection_hardness,
    history_discounted_size,
    is_retryable_entry_quote_failure,
    sample_governance_status,
)


def test_entry_latency_audit_records_latency_and_drift():
    base_ms = 1_700_000_000_000
    audit = build_entry_execution_latency_audit(
        {"signal_ts": base_ms, "trigger_price": 1.0},
        decision_start_ts_ms=base_ms + 500_000,
        quote_request_ts_ms=base_ms + 501_000,
        quote_response_ts_ms=base_ms + 503_000,
        entry_executed_ts_ms=base_ms + 503_000,
        quote_price=1.1,
        entry_fill_price=1.1,
        quote_spread_pct=10,
    )
    assert audit["signal_to_quote_latency_ms"] == 503_000
    assert audit["quote_latency_ms"] == 2_000
    assert round(audit["signal_to_quote_drift_pct"], 4) == 10.0


def test_capital_tier_prefers_paper_tiny_probe_modes():
    assert capital_tier_for_entry(entry_mode="hard_gate_pass_tiny_probe", size_sol=0.002) == "tiny_probe"
    assert capital_tier_for_entry(entry_mode="stage1", strategy_stage="stage1", size_sol=0.06) == "stage1_main"
    assert capital_tier_for_entry(entry_mode="lotto_fast_lane", size_sol=0.03, is_lotto=True) == "lotto_main"


def test_history_discount_and_quote_retry_classification():
    assert abs(history_discounted_size(0.003, 2) - (0.003 * 0.85 * 0.85)) < 1e-12
    assert is_retryable_entry_quote_failure("provider_rate_limited_429")
    assert is_retryable_entry_quote_failure("unknown_data_no_kline")
    assert not is_retryable_entry_quote_failure("honeypot_reject")


def test_rejection_hardness_and_governance():
    assert classify_rejection_hardness("scout_quality_buy_pressure_weak") == "soft_reject"
    assert classify_rejection_hardness("top10_too_high") == "hard_reject"
    assert sample_governance_status(7)["decision"] == "continue_sampling"
    assert sample_governance_status(31, bootstrap_lower_bound=0.01)["decision"] == "promotion_candidate"
