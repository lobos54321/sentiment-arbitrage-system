import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from entry_mode_registry import entry_mode_registry_entry, normalize_entry_mode, normalized_entry_mode_detail


def test_normalize_entry_mode_four_primary_buckets():
    assert normalize_entry_mode("ath_micro_reclaim_tiny_probe", route="ATH") == "RECLAIM_REVIVAL"
    assert normalize_entry_mode("lotto_low_liquidity_reclaim_tiny_probe", route="LOTTO") == "RECLAIM_REVIVAL"
    assert normalize_entry_mode("lotto_upstream_realtime_tiny_scout", route="LOTTO") == "LOTTO_TINY_SCOUT"
    assert normalize_entry_mode("source_resonance_a_class_fastlane", route="A_GRADE") == "A_CLASS_FASTLANE"
    assert normalize_entry_mode("ath_uncertainty_tiny_scout", route="ATH") == "ATH_CONTINUATION"


def test_normalized_entry_mode_detail_marks_known_mode():
    detail = normalized_entry_mode_detail("hard_gate_pass_tiny_probe", route="ATH")

    assert detail["normalized_mode"] == "ATH_CONTINUATION"
    assert detail["known_normalized_mode"] is True


def test_recent_loss_breach_modes_are_hard_shadowed():
    pullback = entry_mode_registry_entry("smart_entry_pullback_bounce")
    micro = entry_mode_registry_entry("lotto_micro_reclaim_tiny_probe")

    assert pullback["tier"] == "hard_shadow"
    assert pullback["paper_enabled"] is False
    assert "20%" in pullback["reason"]
    assert micro["tier"] == "hard_shadow"
    assert micro["paper_enabled"] is False
    assert "20%" in micro["reason"]
