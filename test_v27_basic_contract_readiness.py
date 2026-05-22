import sys
import json

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


def test_paper_mode_safety_consumes_clean_runtime_evidence(tmp_path):
    evidence_path = tmp_path / "paper_mode_safety.json"
    evidence_path.write_text(
        json.dumps(
            {
                "runtime_evidence_schema_version": "v2.7.0.paper_mode_safety_runtime.v1",
                "generated_at": "2026-05-22T00:00:00Z",
                "paper_mode_required": True,
                "paper_only_mode": True,
                "premium_live_execution_enabled": False,
                "live_private_key_present": False,
                "present_live_secret_names": [],
                "live_swap_endpoint_enabled": False,
                "real_order_router_enabled": False,
                "network_transaction_signing_enabled": False,
                "jupiter_executor_initialized": False,
                "live_execution_executor_initialized": False,
                "live_position_monitor_initialized": False,
            }
        ),
        encoding="utf-8",
    )

    report = verify_paper_mode_safety(env={}, runtime_evidence_path=evidence_path)

    assert report["status"] == "pass"
    assert report["evidence"]["runtime_evidence_present"] is True
    assert report["evidence"]["runtime_evidence_valid"] is True


def test_paper_mode_safety_allows_quarantined_live_secret_marker(tmp_path):
    evidence_path = tmp_path / "paper_mode_safety.json"
    evidence_path.write_text(
        json.dumps(
            {
                "runtime_evidence_schema_version": "v2.7.0.paper_mode_safety_runtime.v1",
                "generated_at": "2026-05-22T00:00:00Z",
                "paper_mode_required": True,
                "paper_only_mode": True,
                "premium_live_execution_enabled": False,
                "live_private_key_present": False,
                "present_live_secret_names": [],
                "live_secret_quarantine_applied": True,
                "live_secret_quarantine_reason": "node_preload_before_app_import",
                "quarantined_live_secret_names": ["TRADE_WALLET_PRIVATE_KEY"],
                "live_secret_quarantine_hash": "hash",
                "live_swap_endpoint_enabled": False,
                "real_order_router_enabled": False,
                "network_transaction_signing_enabled": False,
                "jupiter_executor_initialized": False,
                "live_execution_executor_initialized": False,
                "live_position_monitor_initialized": False,
            }
        ),
        encoding="utf-8",
    )

    report = verify_paper_mode_safety(
        env={
            "V27_LIVE_SECRET_QUARANTINE_APPLIED": "true",
            "V27_LIVE_SECRET_QUARANTINE_REASON": "node_preload_before_app_import",
            "V27_QUARANTINED_LIVE_SECRET_NAMES": "TRADE_WALLET_PRIVATE_KEY",
            "V27_LIVE_SECRET_QUARANTINE_HASH": "hash",
        },
        runtime_evidence_path=evidence_path,
    )

    assert report["status"] == "pass"
    assert report["evidence"]["live_private_key_present"] is False
    assert report["evidence"]["live_secret_quarantine_applied"] is True
    assert report["evidence"]["quarantined_live_secret_names"] == ["TRADE_WALLET_PRIVATE_KEY"]
    assert report["evidence"]["runtime_evidence"]["live_secret_quarantine_applied"] is True


def test_paper_mode_safety_blocks_runtime_live_component(tmp_path):
    evidence_path = tmp_path / "paper_mode_safety.json"
    evidence_path.write_text(
        json.dumps(
            {
                "runtime_evidence_schema_version": "v2.7.0.paper_mode_safety_runtime.v1",
                "generated_at": "2026-05-22T00:00:00Z",
                "paper_mode_required": True,
                "paper_only_mode": True,
                "premium_live_execution_enabled": False,
                "live_private_key_present": False,
                "present_live_secret_names": [],
                "live_swap_endpoint_enabled": False,
                "real_order_router_enabled": False,
                "network_transaction_signing_enabled": False,
                "jupiter_executor_initialized": True,
                "live_execution_executor_initialized": False,
                "live_position_monitor_initialized": False,
            }
        ),
        encoding="utf-8",
    )

    report = verify_paper_mode_safety(env={}, runtime_evidence_path=evidence_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "paper_live_capability_detected"
    assert "runtime_jupiter_executor_initialized" in report["evidence"]["violations"]


def test_input_sanitization_redacts_raw_telegram_text():
    report = verify_input_sanitization()

    assert report["status"] == "pass"
    assert report["evidence"]["payload_schema_valid"] is True
    assert report["evidence"]["raw_message_hash_present"] is True
    assert report["evidence"]["legacy_raw_message_leaked"] is False
