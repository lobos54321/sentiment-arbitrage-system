import sys

sys.path.insert(0, "scripts")

from v27_denominator_projection import build_denominator_projection  # noqa: E402
from v27_event_log import V27EventLog  # noqa: E402
from v27_record_raw_provider_probe_evidence import (  # noqa: E402
    build_raw_provider_probe_payload,
    record_raw_provider_probe_evidence,
)


def fake_fetcher(**_kwargs):
    return {
        "status": 200,
        "latency_ms": 42.5,
        "data": {
            "requestId": "probe-request-1",
            "outAmount": "1000000",
            "routePlan": [{"swapInfo": {"ammKey": "probe-pool"}}],
            "slippageBps": 0,
        },
    }


def test_raw_provider_probe_records_trusted_evidence_into_projection(tmp_path):
    summary = record_raw_provider_probe_evidence(
        tmp_path,
        run_id="probe-run-1",
        output_mint="TokenProbe",
        output_symbol="PROBE",
        fetcher=fake_fetcher,
    )

    events = list(V27EventLog(tmp_path).iter_events())
    projection = build_denominator_projection(tmp_path, include_records=True)
    evidence = projection["contract_evidence"]["RawProviderEvidenceContract"]

    assert summary["appended"] == 1
    assert summary["trusted_provider_evidence"] == 1
    assert summary["provider_request_id"] == "probe-request-1"
    assert events[0]["event_type"] == "raw_provider_evidence_recorded"
    assert events[0]["payload"]["response_material_type"] == "provider_probe.rawResponse"
    assert projection["health"]["raw_provider_evidence_ok"] is True
    assert evidence["trusted_raw_provider_evidence_count"] == 1
    assert evidence["response_material_types"] == ["provider_probe.rawResponse"]


def test_raw_provider_probe_payload_requires_provider_request_id():
    payload = build_raw_provider_probe_payload(
        {
            "status": 200,
            "latency_ms": 7,
            "data": {
                "outAmount": "1000000",
                "routePlan": [],
            },
        },
        run_id="probe-run-no-request",
        output_mint="TokenProbe",
        observed_at="2026-05-24T00:00:00Z",
    )

    assert payload["provider_request_id"] is None
    assert payload["raw_response_available"] is True
    assert payload["raw_response_hash"]
    assert payload["provider_evidence_trusted"] is False
    assert payload["provider_evidence_proof_level"] == "provider_probe_without_trusted_request_id"
