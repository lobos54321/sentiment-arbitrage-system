import sys

sys.path.insert(0, "scripts")

from scout_quality import evaluate_scout_quality  # noqa: E402


def test_recent_failure_blocks_until_reclaim_is_strong():
    weak = evaluate_scout_quality(
        mode="ath_uncertainty_tiny_scout",
        route="ATH",
        trend={
            "liquidity_usd": 12000,
            "price_change_m5": 4,
            "vol_m5": 12000,
            "tx_m5": 100,
            "buy_sell_ratio": 1.3,
        },
        token_risk={
            "blocked": True,
            "reason": "token_quarantine_reclaim_required",
            "cooldown_expired": True,
            "severe_failure_count": 1,
            "risk_memory_count": 0,
        },
        position_size_sol=0.003,
    )

    assert weak["pass"] is False
    assert weak["reason"] == "scout_quality_recent_token_failure"
    assert weak["recent_failure_reclaim_bypass"]["reason"] == "recent_failure_reclaim_not_strong"


def test_recent_failure_allows_tiny_scout_after_strong_reclaim():
    strong = evaluate_scout_quality(
        mode="ath_uncertainty_tiny_scout",
        route="ATH",
        trend={
            "liquidity_usd": 12000,
            "price_change_m5": 18,
            "vol_m5": 16000,
            "tx_m5": 140,
            "buy_sell_ratio": 1.35,
        },
        token_risk={
            "blocked": True,
            "reason": "token_quarantine_reclaim_required",
            "cooldown_expired": True,
            "severe_failure_count": 1,
            "risk_memory_count": 0,
        },
        position_size_sol=0.003,
    )

    assert strong["pass"] is True
    assert strong["reason"] == "scout_quality_pass"
    assert strong["recent_failure_reclaim_bypass"]["reason"] == "recent_failure_reclaim_bypass"


def test_matrix_reclaim_requires_cleaner_activity_than_soft_reclaim():
    weak_matrix = evaluate_scout_quality(
        mode="matrix_reclaim_tiny_probe",
        route="ATH",
        trend={
            "liquidity_usd": 10000,
            "price_change_m5": -2,
            "vol_m5": 8000,
            "tx_m5": 80,
            "buy_sell_ratio": 1.12,
        },
        position_size_sol=0.003,
    )

    assert weak_matrix["pass"] is False
    assert weak_matrix["reason"] == "scout_quality_buy_pressure_weak"


def test_matrix_micro_momentum_probe_has_its_own_clean_activity_profile():
    quality = evaluate_scout_quality(
        mode="matrix_micro_momentum_tiny_probe",
        route="ATH",
        trend={
            "liquidity_usd": 10000,
            "price_change_m5": -2.5,
            "vol_m5": 9000,
            "tx_m5": 90,
            "buy_sell_ratio": 1.16,
            "top10_holder_pct": 40,
        },
        position_size_sol=0.003,
    )

    assert quality["pass"] is True
    assert quality["reason"] == "scout_quality_pass"


def test_matrix_micro_momentum_probe_blocks_deeper_negative_trend():
    quality = evaluate_scout_quality(
        mode="matrix_micro_momentum_tiny_probe",
        route="ATH",
        trend={
            "liquidity_usd": 10000,
            "price_change_m5": -3.5,
            "vol_m5": 9000,
            "tx_m5": 90,
            "buy_sell_ratio": 1.16,
            "top10_holder_pct": 40,
        },
        position_size_sol=0.003,
    )

    assert quality["pass"] is False
    assert quality["reason"] == "scout_quality_negative_trend"


def test_lotto_low_liquidity_reclaim_uses_quote_tx_bs_profile():
    quality = evaluate_scout_quality(
        mode="lotto_low_liquidity_reclaim_tiny_probe",
        route="LOTTO",
        trend={
            "liquidity_usd": 5000,
            "price_change_m5": 0,
            "vol_m5": 0,
            "tx_m5": 55,
            "buy_sell_ratio": 1.25,
            "top1_pct": 40,
            "top10_pct": 75,
        },
        position_size_sol=0.003,
    )

    assert quality["pass"] is True
    assert quality["reason"] == "scout_quality_pass"


def test_lotto_micro_reclaim_requires_positive_bounce_quality():
    quality = evaluate_scout_quality(
        mode="lotto_micro_reclaim_tiny_probe",
        route="LOTTO",
        trend={
            "liquidity_usd": 10000,
            "price_change_m5": -1,
            "vol_m5": 5000,
            "tx_m5": 50,
            "buy_sell_ratio": 1.3,
            "top1_pct": 40,
            "top10_pct": 70,
        },
        position_size_sol=0.003,
    )

    assert quality["pass"] is False
    assert quality["reason"] == "scout_quality_negative_trend"
