import sys

sys.path.insert(0, "scripts")

from profit_protect_policy import ath_moon_bag_floor  # noqa: E402


def test_ath_moon_bag_floor_tightens_just_crossed_50pct_peak():
    floor = ath_moon_bag_floor(0.525)

    assert floor is not None
    assert 0.27 <= floor <= 0.30


def test_ath_moon_bag_floor_keeps_wide_room_for_large_moonshot():
    floor = ath_moon_bag_floor(1.50)

    assert floor == 1.10
