import sys

sys.path.insert(0, "scripts")

from v27_denominator_projection import build_denominator_projection  # noqa: E402
from v27_event_log import V27EventLog, sha256_hex  # noqa: E402
from v27_record_decision_audit_evidence import record_decision_audit_evidence  # noqa: E402


def append_quote_intent_binding(log):
    binding_material = {
        "quote_intent_id": 1,
        "side": "buy",
        "size": 0.01,
        "route": "unit_route",
        "pool": "PoolAudit",
        "quote_mint": "SOL",
        "slippage_bps": 25,
        "quote_ts": 1_700_000_002,
        "token_ca": "TokenAudit",
    }
    quote = {"request_id": "quote-audit-1", "out_amount": "1000000"}
    log.append_event(
        event_type="quote_intent_binding_recorded",
        aggregate_id="quote_intent_binding:solana:TokenAudit:PoolAudit:0:1",
        idempotency_key="quote_intent_binding:TokenAudit:1",
        payload={
            "token_ca": "TokenAudit",
            "symbol": "AUDIT",
            "chain": "solana",
            "canonical_pool_group": "PoolAudit",
            "lifecycle_epoch": 0,
            "binding_policy_version": "legacy_paper_trade_quote_intent_binding_v0.1",
            "quote_intent_binding_version": "legacy_paper_trade_quote_intent_binding_v0.1",
            **binding_material,
            "quote_source": "paper_trade_entry_quote_or_legacy_proxy",
            "quote_binding_proof_level": "entry_execution_audit",
            "quote_intent_binding_quality": "entry_execution_audit_bound",
            "quote_intent_bound": True,
            "intent_hash": sha256_hex(binding_material),
            "quote_hash": sha256_hex(quote),
            "quote_binding_hash": sha256_hex({"intent": binding_material, "quote": quote, "mismatch_fields": []}),
            "missing_fields": [],
            "mismatch_fields": [],
            "used_future_peak": False,
            "used_future_outcome": False,
            "used_posthoc_label": False,
            "forbidden_future_fields_used": [],
        },
    )


def test_record_decision_audit_evidence_from_bound_quote_intent(tmp_path):
    log = V27EventLog(tmp_path)
    append_quote_intent_binding(log)

    report = record_decision_audit_evidence(tmp_path)
    projection = build_denominator_projection(tmp_path, include_records=True)
    evidence = projection["contract_evidence"]["DecisionAudit"]

    assert report["eligible_decision_audits"] == 1
    assert report["appended"] == 1
    assert projection["decision_audit_recorded_events"] == 1
    assert projection["health"]["decision_audit_ok"] is True
    assert evidence["valid_decision_audit_count"] == 1
    assert evidence["future_leakage_count"] == 0
    assert evidence["feature_vector_hash_mismatch_count"] == 0
    assert evidence["trace_bundle_hash_mismatch_count"] == 0


def test_record_decision_audit_evidence_is_idempotent(tmp_path):
    log = V27EventLog(tmp_path)
    append_quote_intent_binding(log)

    first = record_decision_audit_evidence(tmp_path)
    second = record_decision_audit_evidence(tmp_path)

    assert first["appended"] == 1
    assert second["appended"] == 0
    assert second["eligible_decision_audits"] == 0
    assert V27EventLog(tmp_path).verify()["event_count"] == 2
