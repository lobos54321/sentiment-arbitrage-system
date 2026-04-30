import sys
import sqlite3

sys.path.insert(0, "scripts")

from paper_decision_audit import init_decision_audit  # noqa: E402
from paper_trade_monitor import (  # noqa: E402
    find_lotto_real_probe_candidates,
    should_block_lotto_falling_knife,
)


def test_blocks_newborn_low_liq_m5_down_falling_knife():
    blocked, detail = should_block_lotto_falling_knife(
        {"liquidity_usd": 9654.68},
        {
            "lifecycle_state": "NEWBORN_LAUNCH",
            "lifecycle_features": {
                "liquidity_usd": 9654.68,
                "price_change_m5": -43.04,
            },
        },
    )
    assert blocked is True
    assert detail["liquidity_usd"] == 9654.68
    assert detail["price_change_m5"] == -43.04


def test_allows_newborn_low_liq_without_m5_downtrend():
    blocked, _ = should_block_lotto_falling_knife(
        {"liquidity_usd": 9654.68},
        {
            "lifecycle_state": "NEWBORN_LAUNCH",
            "lifecycle_features": {
                "liquidity_usd": 9654.68,
                "price_change_m5": 12.0,
            },
        },
    )
    assert blocked is False


def test_allows_non_newborn_even_when_low_liq_m5_down():
    blocked, _ = should_block_lotto_falling_knife(
        {"liquidity_usd": 9654.68},
        {
            "lifecycle_state": "FIRST_PUMP",
            "lifecycle_features": {
                "liquidity_usd": 9654.68,
                "price_change_m5": -43.04,
            },
        },
    )
    assert blocked is False


def test_real_probe_requires_non_stale_15m_strength():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    rows = [
        ("BlueBuck", "BlueToken", "lotto_mc_30740", 0.4653, 0.3439, 0.4653),
        ("Saviour", "SaviourToken", "lotto_stale_1825s", 0.6854, -0.0329, 0.6854),
    ]
    for symbol, token, reason, pnl5, pnl15, max_pnl in rows:
        db.execute(
            """
            INSERT INTO paper_missed_signal_attribution
                (created_event_ts, token_ca, symbol, signal_ts, route, component,
                 decision, reject_reason, baseline_price, baseline_ts,
                 pnl_5m, pnl_15m, max_pnl_recorded, status)
            VALUES (?, ?, ?, ?, 'LOTTO', 'lotto_entry_gate',
                    'expire', ?, 1.0, ?, ?, ?, ?, 'pending')
            """,
            (1000, token, symbol, 900, reason, 900, pnl5, pnl15, max_pnl),
        )
    db.commit()
    candidates = find_lotto_real_probe_candidates(db, now_ts=1200, limit=5)
    assert [row["symbol"] for row in candidates] == ["BlueBuck"]


def run_tests():
    tests = [
        test_blocks_newborn_low_liq_m5_down_falling_knife,
        test_allows_newborn_low_liq_without_m5_downtrend,
        test_allows_non_newborn_even_when_low_liq_m5_down,
        test_real_probe_requires_non_stale_15m_strength,
    ]
    for test in tests:
        test()
        print(f"ok - {test.__name__}")


if __name__ == "__main__":
    run_tests()
