import sys

sys.path.insert(0, "scripts")

from profit_protect_policy import ath_moon_bag_floor, probe_runner_floor  # noqa: E402


def test_ath_moon_bag_floor_tightens_just_crossed_50pct_peak():
    floor = ath_moon_bag_floor(0.525)

    assert floor is not None
    assert 0.27 <= floor <= 0.30


def test_ath_moon_bag_floor_keeps_wide_room_for_large_moonshot():
    floor = ath_moon_bag_floor(1.50)

    assert floor == 1.10


def test_probe_runner_floor_is_inactive_before_10pct_peak():
    assert probe_runner_floor(0.099) is None


def test_probe_runner_floor_protects_10_to_45pct_probe_peaks():
    assert round(probe_runner_floor(0.12), 4) == 0.042
    assert round(probe_runner_floor(0.25), 4) == 0.0875
    assert round(probe_runner_floor(0.45), 4) == 0.1575


def test_probe_runner_floor_tightens_after_50pct_peak_without_changing_main_moon_floor():
    assert ath_moon_bag_floor(1.50) == 1.10
    assert probe_runner_floor(0.525) > ath_moon_bag_floor(0.525)
    assert probe_runner_floor(1.50) == 1.25
