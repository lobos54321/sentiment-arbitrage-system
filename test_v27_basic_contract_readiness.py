import sys

sys.path.insert(0, "scripts")

from v27_basic_contract_readiness import (  # noqa: E402
    build_basic_contract_readiness,
    verify_input_sanitization,
    verify_paper_mode_safety,
)


def test_basic_contract_readiness_passes_seed_foundation():
    report = build_basic_contract_readiness(env={})

    assert report["health"]["status"] == "basic_contract_readiness_ok"
    assert report["blocking_contracts"] == []
    for contract_id in (
        "SpecConsistencyLinterContract",
        "PaperModeSafetyBoundary",
        "ChainConfigContract",
        "SourceRegistryContract",
        "InputSanitizationContract",
    ):
        assert report["contracts"][contract_id]["status"] == "pass"


def test_paper_mode_safety_blocks_live_capabilities():
    report = verify_paper_mode_safety(
        env={
            "PREMIUM_LIVE_EXECUTION_ENABLED": "true",
            "TRADE_WALLET_PRIVATE_KEY": "secret",
        }
    )

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "paper_live_capability_detected"
    assert report["evidence"]["premium_live_execution_enabled"] is True
    assert report["evidence"]["live_private_key_present"] is True


def test_input_sanitization_redacts_raw_telegram_text():
    report = verify_input_sanitization()

    assert report["status"] == "pass"
    assert report["evidence"]["payload_schema_valid"] is True
    assert report["evidence"]["raw_message_hash_present"] is True
    assert report["evidence"]["legacy_raw_message_leaked"] is False
