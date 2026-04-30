import sys

sys.path.insert(0, "scripts")

from paper_trade_monitor import should_block_lotto_falling_knife  # noqa: E402


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


def run_tests():
    tests = [
        test_blocks_newborn_low_liq_m5_down_falling_knife,
        test_allows_newborn_low_liq_without_m5_downtrend,
        test_allows_non_newborn_even_when_low_liq_m5_down,
    ]
    for test in tests:
        test()
        print(f"ok - {test.__name__}")


if __name__ == "__main__":
    run_tests()
