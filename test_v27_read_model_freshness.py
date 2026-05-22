import json
import sys

sys.path.insert(0, "scripts")

from v27_denominator_projection import build_denominator_projection, build_denominator_read_model_snapshot  # noqa: E402
from v27_event_log import V27EventLog  # noqa: E402
from v27_read_model_freshness import validate_snapshot_file  # noqa: E402


def append_seed(log, token_ca="TokenA"):
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


def write_snapshot(path, snapshot):
    path.write_text(json.dumps(snapshot, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def test_read_model_freshness_accepts_valid_fresh_snapshot(tmp_path):
    log = V27EventLog(tmp_path / "events")
    append_seed(log)
    projection = build_denominator_projection(tmp_path / "events")
    snapshot = build_denominator_read_model_snapshot(projection, max_allowed_lag_seq=0, max_allowed_lag_ms=300_000)
    snapshot_path = tmp_path / "denominator_snapshot.json"
    write_snapshot(snapshot_path, snapshot)

    report = validate_snapshot_file(snapshot_path, max_snapshot_age_ms=300_000)

    assert report["snapshot_present"] is True
    assert report["snapshot_parse_ok"] is True
    assert report["snapshot_hash_ok"] is True
    assert report["projection_hash_ok"] is True
    assert report["read_model_seq"] == 1
    assert report["event_log_latest_seq"] == 1
    assert report["blocking_reasons"] == []
    assert report["health"]["dashboard_safe"] is True


def test_read_model_freshness_blocks_missing_snapshot(tmp_path):
    report = validate_snapshot_file(tmp_path / "missing_snapshot.json")

    assert report["snapshot_present"] is False
    assert report["blocking_reasons"] == ["snapshot_missing"]
    assert report["health"]["dashboard_safe"] is False
    assert report["health"]["status"] == "snapshot_missing"


def test_read_model_freshness_blocks_stale_snapshot_file(tmp_path):
    log = V27EventLog(tmp_path / "events")
    append_seed(log)
    projection = build_denominator_projection(tmp_path / "events")
    snapshot = build_denominator_read_model_snapshot(
        projection,
        now_iso="2026-05-22T00:00:00Z",
        max_allowed_lag_seq=0,
        max_allowed_lag_ms=300_000,
    )
    snapshot_path = tmp_path / "denominator_snapshot.json"
    write_snapshot(snapshot_path, snapshot)

    report = validate_snapshot_file(
        snapshot_path,
        now_iso="2026-05-22T00:10:00Z",
        max_snapshot_age_ms=1_000,
    )

    assert report["snapshot_hash_ok"] is True
    assert report["snapshot_age_ok"] is False
    assert "snapshot_file_stale" in report["blocking_reasons"]
    assert report["health"]["dashboard_safe"] is False


def test_read_model_freshness_blocks_tampered_snapshot_hash(tmp_path):
    log = V27EventLog(tmp_path / "events")
    append_seed(log)
    projection = build_denominator_projection(tmp_path / "events")
    snapshot = build_denominator_read_model_snapshot(projection, max_allowed_lag_seq=0, max_allowed_lag_ms=300_000)
    snapshot["read_model"]["read_model_seq"] = 0
    snapshot_path = tmp_path / "denominator_snapshot.json"
    write_snapshot(snapshot_path, snapshot)

    report = validate_snapshot_file(snapshot_path, max_snapshot_age_ms=300_000)

    assert report["snapshot_hash_ok"] is False
    assert "snapshot_hash_mismatch" in report["blocking_reasons"]
    assert report["health"]["dashboard_safe"] is False


def test_read_model_freshness_blocks_empty_event_log_snapshot(tmp_path):
    log = V27EventLog(tmp_path / "events")
    assert log.verify()["event_count"] == 0
    projection = build_denominator_projection(tmp_path / "events")
    snapshot = build_denominator_read_model_snapshot(projection, max_allowed_lag_seq=0, max_allowed_lag_ms=300_000)
    snapshot_path = tmp_path / "denominator_snapshot.json"
    write_snapshot(snapshot_path, snapshot)

    report = validate_snapshot_file(snapshot_path, max_snapshot_age_ms=300_000)

    assert report["projection_status"] == "seed_empty"
    assert report["event_log_latest_seq"] == 0
    assert "projection_status_seed_empty" in report["blocking_reasons"]
    assert "event_log_empty" in report["blocking_reasons"]
    assert report["health"]["dashboard_safe"] is False
