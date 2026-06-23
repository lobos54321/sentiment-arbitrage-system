import json
import sqlite3
import sys

sys.path.insert(0, "scripts")

from runtime_final_evidence import (  # noqa: E402
    build_runtime_final_evidence_row,
    emit_runtime_final_evidence,
    export_runtime_final_evidence,
)
from gmgn_policy import emit_gmgn_policy_evidence  # noqa: E402
from source_resonance_shadow import init_source_resonance_shadow, upsert_candidate  # noqa: E402


def test_runtime_final_evidence_writer_and_export(tmp_path):
    raw = tmp_path / "runtime-final-raw.jsonl"
    fullnet = tmp_path / "row.jsonl"
    out = tmp_path / "runtime-final-export.jsonl"
    fullnet.write_text(json.dumps({"token_ca": "T", "signal_ts": 1500}) + "\n", encoding="utf-8")

    result = emit_runtime_final_evidence(
        "source_resonance",
        {"token_ca": "T", "signal_ts": 1500, "premium_signal_id": 7},
        {
            "gmgn_first_seen_ts": 1200,
            "gmgn_last_seen_ts": 1490,
            "lead_time_sec": 300,
            "resonance_source": "telegram_gmgn",
            "resonance_score": 2.25,
            "timestamp_valid": True,
        },
        source="unit",
        evidence_ts=1510,
        path=raw,
    )

    assert result["emitted"] is True
    exported = export_runtime_final_evidence(
        raw_log=raw,
        fullnet_row=fullnet,
        window_start_ts=1000,
        window_end_ts=2000,
        out=out,
    )
    assert exported["exported"] == 1
    row = json.loads(out.read_text(encoding="utf-8").strip())
    assert row["module_group"] == "source_resonance"
    assert row["join_confidence"] == "HIGH"
    assert row["window_start_ts"] == 1000
    assert row["window_end_ts"] == 2000


def test_runtime_final_evidence_rejects_missing_fields():
    try:
        build_runtime_final_evidence_row(
            "source_resonance",
            {"token_ca": "T", "signal_ts": 1500},
            {"gmgn_first_seen_ts": 1200},
        )
    except ValueError as exc:
        assert "missing required fields" in str(exc)
    else:
        raise AssertionError("missing fields should fail")


def test_gmgn_policy_evidence_emits_only_with_identity(tmp_path):
    log = tmp_path / "runtime-final.jsonl"
    policy = {"action": "allow", "reason": "gmgn_policy_allow"}

    missing = emit_gmgn_policy_evidence(policy, {}, source="unit")
    assert missing["emitted"] is False

    result = emit_gmgn_policy_evidence(
        policy,
        {"token_ca": "T", "signal_ts": 1500, "premium_signal_id": 7},
        source="unit",
    )
    assert result["emitted"] is False  # no env/path configured

    result = emit_runtime_final_evidence(
        "gmgn_policy",
        {"token_ca": "T", "signal_ts": 1500, "premium_signal_id": 7},
        {
            "gmgn_policy_decision": "allow",
            "gmgn_policy_reason": "gmgn_policy_allow",
            "gmgn_policy_source": "unit",
            "gmgn_policy_version": "gmgn_paper_policy.v1",
        },
        path=log,
    )
    assert result["emitted"] is True
    row = json.loads(log.read_text(encoding="utf-8").strip())
    assert row["module_group"] == "gmgn_policy"
    assert row["gmgn_policy_decision"] == "allow"


def test_gmgn_policy_evidence_diagnostic_logs_skip_reason(monkeypatch, capsys):
    monkeypatch.delenv("RUNTIME_FINAL_EVIDENCE_LOG", raising=False)
    monkeypatch.setenv("RUNTIME_FINAL_EVIDENCE_DIAGNOSTIC_LOG_ENABLED", "true")

    result = emit_gmgn_policy_evidence(
        {"action": "allow", "reason": "gmgn_policy_allow"},
        {"token_ca": "TokenAddress123456789", "signal_ts": 1500, "premium_signal_id": 7},
        source="unit",
    )

    assert result["emitted"] is False
    assert result["reason"] == "runtime_final_evidence_log_not_configured"
    captured = capsys.readouterr()
    assert "gmgn_policy emit skipped" in captured.err
    assert "runtime_final_evidence_log_not_configured" in captured.err
    assert "TokenA...6789" in captured.err


def test_source_resonance_upsert_emits_runtime_final_evidence(tmp_path, monkeypatch):
    log = tmp_path / "runtime-final.jsonl"
    monkeypatch.setenv("RUNTIME_FINAL_EVIDENCE_LOG", str(log))
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_source_resonance_shadow(db)

    upsert_candidate(db, {
        "token_ca": "T",
        "symbol": "TOK",
        "signal_ts": 1500,
        "telegram_signal_id": 7,
        "signal_type": "NEW_TRENDING",
        "telegram_seen": 1,
        "telegram_ts": 1500,
        "gmgn_pre_seen": 1,
        "gmgn_first_seen_ts": 1200,
        "gmgn_last_seen_ts": 1490,
        "gmgn_lead_time_sec": 300,
        "source_count": 2,
        "resonance_level": 2,
        "resonance_score": 2.25,
        "cohort": "telegram_gmgn",
        "payload_json": "{}",
    })

    rows = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    by_module = {row["module_group"]: row for row in rows}
    assert set(by_module) == {"source_resonance", "worker_health"}
    assert by_module["source_resonance"]["token_ca"] == "T"
    assert by_module["source_resonance"]["lead_time_sec"] == 300
    assert by_module["worker_health"]["worker_name"] == "source_resonance_shadow"
    assert by_module["worker_health"]["worker_status"] == "ok"
