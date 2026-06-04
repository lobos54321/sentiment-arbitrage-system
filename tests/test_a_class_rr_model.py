import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from a_class_fastlane import AClassCandidate
from a_class_rr_model import build_a_class_rr_model


def test_rr_model_requires_two_to_one_for_live_allowed():
    candidate = AClassCandidate(
        token_ca="TokenA",
        route_bucket="ATH",
        raw_payload={"quote_clean_peak_pnl": 0.20},
    )

    rr = build_a_class_rr_model(candidate, {"matrix_grade": "A"})

    assert rr["expected_rr"] < 2.0
    assert rr["live_allowed_by_rr"] is False
    assert "expected_rr_below_2" in rr["hard_blockers"]
    assert rr["bottom_ticket_size_sol"] == 0.0


def test_rr_model_assigns_bottom_ticket_and_plans():
    candidate = AClassCandidate(
        token_ca="TokenB",
        route_bucket="LOTTO",
        source_resonance=True,
        fresh_momentum=True,
        missed_dog_cohort_strong=True,
        raw_payload={"quote_clean_peak_pnl": 1.00},
    )

    rr = build_a_class_rr_model(candidate, {"matrix_grade": "STRONG_A"})

    assert rr["expected_rr"] >= 3.0
    assert rr["live_allowed_by_rr"] is True
    assert 0.001 <= rr["bottom_ticket_size_sol"] <= 0.003
    assert rr["principal_recovery_plan"]["no_averaging_down"] is True
    assert rr["moonbag_plan"]["keep_tail_after_moonbag"] is True
