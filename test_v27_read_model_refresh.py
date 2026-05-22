import json
import sys

sys.path.insert(0, "scripts")

from v27_event_log import V27EventLog  # noqa: E402
from v27_read_model_refresh import refresh_denominator_read_model  # noqa: E402


def append_signal(log, token_ca="TokenA"):
    return log.append_event(
        event_type="telegram_signal_seen",
        aggregate_id=f"telegram_signal:solana:{token_ca}:unknown_pool:0",
        idempotency_key=f"premium_signals:{token_ca}",
        payload={
            "telegram_signal_id": token_ca,
            "token_ca": token_ca,
            "symbol": token_ca[-4:],
            "chain": "solana",
            "canonical_pool_group": "unknown_pool",
            "lifecycle_epoch": 0,
            "telegram_seen": True,
            "realtime_observable": True,
        },
    )


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def test_refresh_writes_projection_snapshot_and_health_atomically_consumable(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    log = V27EventLog(event_log_dir)
    append_signal(log, "TokenA")

    report = refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        max_snapshot_age_ms=300_000,
    )

    projection = read_json(out_dir / "denominator_projection.json")
    snapshot = read_json(out_dir / "denominator_snapshot.json")
    health = read_json(out_dir / "denominator_freshness.json")
    assert report["health"]["status"] == "read_model_refresh_ok"
    assert report["dashboard_safe"] is True
    assert report["read_model_seq"] == 1
    assert report["event_log_latest_seq"] == 1
    assert report["snapshot_hash"] == snapshot["snapshot_hash"]
    assert health["snapshot_hash"] == snapshot["snapshot_hash"]
    assert health["projection_hash"] == snapshot["projection_hash"]
    assert projection["event_log_latest_seq"] == 1
    assert snapshot["read_model"]["read_model_seq"] == 1
    assert health["verifier_report"]["blocking_reasons"] == []


def test_refresh_health_report_blocks_invalid_spec_manifest(tmp_path):
    event_log_dir = tmp_path / "events"
    out_dir = tmp_path / "read_models"
    bad_spec_manifest = tmp_path / "missing_spec.manifest.json"
    log = V27EventLog(event_log_dir)
    append_signal(log, "TokenA")

    report = refresh_denominator_read_model(
        event_log_dir=event_log_dir,
        projection_path=out_dir / "denominator_projection.json",
        snapshot_path=out_dir / "denominator_snapshot.json",
        health_path=out_dir / "denominator_freshness.json",
        spec_manifest_path=bad_spec_manifest,
        max_snapshot_age_ms=300_000,
    )

    health = read_json(out_dir / "denominator_freshness.json")
    assert report["dashboard_safe"] is False
    assert report["health"]["status"] == "read_model_refresh_not_ready"
    assert "spec_invalid" in report["blocking_reasons"]
    assert health["dashboard_safe"] is False
    assert "spec_invalid" in health["verifier_report"]["blocking_reasons"]
