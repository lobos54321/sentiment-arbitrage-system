import sys

sys.path.insert(0, "scripts")

from phase_policy import (  # noqa: E402
    classify_kline_position,
    evaluate_phase_policy,
    principal_recovery_sell_pct,
)


def rising_bars():
    return [
        {"ts": 1, "close": 1.00},
        {"ts": 2, "close": 1.02},
        {"ts": 3, "close": 1.04},
        {"ts": 4, "close": 1.06},
        {"ts": 5, "close": 1.08},
        {"ts": 6, "close": 1.10},
    ]


def test_kline_classifies_above_rising_ema():
    kline = classify_kline_position(rising_bars(), current_price=1.11)
    assert kline.state == "ABOVE_RISING_EMA"
    assert kline.distance_ema10_pct is not None


def test_principal_recovery_sell_pct_matches_profit_level():
    assert round(principal_recovery_sell_pct(0.25), 4) == 0.8
    assert round(principal_recovery_sell_pct(0.50), 4) == 0.6667


def test_peak_25_to_50_recommends_recover_principal():
    decision = evaluate_phase_policy(
        current_pnl=0.30,
        peak_pnl=0.35,
        held_sec=90,
        sold_pct=0.0,
        dex_snapshot={"buys_m5": 20, "sells_m5": 10, "liquidity_usd": 15_000},
        kline_bars=rising_bars(),
        current_price=1.11,
    )
    assert decision.phase_state == "RECOVER_PRINCIPAL"
    assert decision.shadow_action == "PARTIAL_SELL"
    assert 0.70 <= decision.sell_pct <= 0.80


def test_high_rug_risk_overrides_phase():
    decision = evaluate_phase_policy(
        current_pnl=-0.05,
        peak_pnl=0.20,
        held_sec=90,
        sold_pct=0.0,
        dex_snapshot={"buys_m5": 2, "sells_m5": 20, "liquidity_usd": 3_000},
        kline_bars=[
            {"ts": 1, "close": 1.20},
            {"ts": 2, "close": 1.15},
            {"ts": 3, "close": 1.10},
            {"ts": 4, "close": 1.00},
            {"ts": 5, "close": 0.90},
        ],
        current_price=0.85,
        quote_pnl=-0.35,
    )
    assert decision.phase_state == "RUG_DEFENSE"
    assert decision.shadow_action == "EXIT"


def run_tests():
    tests = [
        test_kline_classifies_above_rising_ema,
        test_principal_recovery_sell_pct_matches_profit_level,
        test_peak_25_to_50_recommends_recover_principal,
        test_high_rug_risk_overrides_phase,
    ]
    for test in tests:
        test()
        print(f"ok - {test.__name__}")


if __name__ == "__main__":
    run_tests()
