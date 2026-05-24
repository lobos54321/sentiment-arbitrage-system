import sys
import json

sys.path.insert(0, "scripts")

from v27_basic_contract_readiness import (  # noqa: E402
    build_basic_contract_readiness,
    verify_evidence_eligibility_matrix,
    verify_input_sanitization,
    verify_paper_mode_safety,
    verify_project_stop_loss,
    verify_safe_default,
    verify_safety_case,
    verify_top_fix_queue,
    verify_waiver_policy,
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
        "SafeDefaultContract",
        "ProjectStopLossContract",
        "EvidenceEligibilityMatrix",
        "TopFixQueueContract",
        "SafetyCaseContract",
        "WaiverPolicyContract",
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


def test_safe_default_requires_blocked_shadow_defaults(tmp_path):
    registry_path = tmp_path / "entry-mode-registry.json"
    registry_path.write_text(
        json.dumps(
            {
                "tiers": {"live": {"paper_enabled": True}},
                "modes": {
                    "unit_live": {
                        "tier": "live",
                        "paper_enabled": True,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    report = verify_safe_default(registry_path=registry_path)

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "safe_default_fail_closed_unverified"
    assert report["evidence"]["blocked_mode_count"] == 0
    assert report["evidence"]["default_action"] == "fail_closed"


def test_project_stop_loss_blocks_when_auto_kill_disabled():
    report = verify_project_stop_loss(
        env={
            "ENTRY_MODE_QUALITY_AUTO_KILL_SWITCH_ENABLED": "false",
        }
    )

    assert report["status"] == "missing_evidence"
    assert report["blocking_reason"] == "project_stop_loss_unverified_or_disabled"
    assert report["evidence"]["auto_kill_switch_enabled"] is False
    assert report["evidence"]["action"]["stop_automatic_entry"] is True


def test_project_stop_loss_passes_default_thresholds():
    report = verify_project_stop_loss(env={})

    assert report["status"] == "pass"
    assert report["evidence"]["scope"] == "entry_mode"
    assert report["evidence"]["stop_criteria"]["negative_ev_min_samples"] == 20
    assert report["evidence"]["action"]["action"] == "downgrade_to_watch_only"


def test_governance_readiness_contracts_pass_seed_artifact():
    assert verify_evidence_eligibility_matrix()["status"] == "pass"
    assert verify_top_fix_queue()["status"] == "pass"
    assert verify_safety_case()["status"] == "pass"
    assert verify_waiver_policy()["status"] == "pass"


def test_governance_readiness_blocks_incomplete_artifact(tmp_path):
    governance_path = tmp_path / "governance.json"
    governance_path.write_text(
        json.dumps(
            {
                "schema_version": "unit.bad",
                "evidence_eligibility_matrix": [
                    {
                        "evidence_use": "normal_tiny_promotion",
                        "event_truth": [],
                        "feature_truth": [],
                        "label_truth": [],
                        "replay_truth": [],
                    }
                ],
                "top_fix_queue": [
                    {
                        "fix_id": "fix-only-one",
                        "blocker_code": "RawProviderEvidenceContract",
                    }
                ],
                "safety_cases": [
                    {
                        "safety_case_id": "case-without-links",
                        "scope": "normal_tiny",
                        "core_hazards": ["hazard"],
                        "mitigations": ["mitigation"],
                        "evidence_links": [],
                    }
                ],
                "waiver_policy": [
                    {
                        "waiver_id": "expired-waiver",
                        "contract_id": "RawProviderEvidenceContract",
                        "scope": "normal_tiny",
                        "expires_at": "2020-01-01T00:00:00Z",
                        "non_waivable": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    assert verify_evidence_eligibility_matrix(governance_path)["status"] == "missing_evidence"
    assert verify_top_fix_queue(governance_path)["status"] == "missing_evidence"
    assert verify_safety_case(governance_path)["status"] == "missing_evidence"
    assert verify_waiver_policy(governance_path)["status"] == "missing_evidence"
