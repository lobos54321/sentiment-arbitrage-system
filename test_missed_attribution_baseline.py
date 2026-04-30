import sqlite3
import sys

sys.path.insert(0, "scripts")

from paper_decision_audit import (  # noqa: E402
    _extract_baseline_price,
    init_decision_audit,
    missed_attribution_coverage,
    record_decision_event,
    update_due_missed_attributions,
)


def new_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    return db


def test_extracts_nested_lifecycle_baseline():
    price, source = _extract_baseline_price({
        "lifecycle": {
            "lifecycle_features": {
                "signal_price": "0.000123",
            },
        },
    })
    assert price == 0.000123
    assert source == "lifecycle.lifecycle_features.signal_price"


def test_upstream_reject_records_baseline_when_payload_has_price():
    db = new_db()
    record_decision_event(
        db,
        component="upstream_gate",
        event_type="signal_skip",
        decision="skip",
        reason="not_ath_prebuy_kline_unknown_data_blocked",
        token_ca="TokenWithPrice",
        symbol="PRICE",
        route="LOTTO",
        payload={"current_price": 0.42},
        event_ts=1000,
    )
    row = db.execute(
        "SELECT baseline_price, baseline_source FROM paper_missed_signal_attribution"
    ).fetchone()
    assert row["baseline_price"] == 0.42
    assert row["baseline_source"] == "current_price"


def test_missing_baseline_becomes_explicit_and_does_not_block_fresh_rows():
    db = new_db()
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
            (created_event_ts, token_ca, symbol, component, decision, baseline_ts, status, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (1000, "OldNoPrice", "OLD", "upstream_gate", "skip", 1000, "pending", "{}"),
    )
    db.execute(
        """
        INSERT INTO paper_missed_signal_attribution
            (created_event_ts, token_ca, symbol, component, decision, baseline_ts, status, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (1990, "FreshPrice", "FRESH", "upstream_gate", "skip", 1990, "pending", "{}"),
    )
    db.commit()

    updated = update_due_missed_attributions(
        db,
        historical_price_fetcher=lambda token_ca, ts: None,
        live_price_fetcher=lambda token_ca: (1.0, "live:test", 2000) if token_ca == "FreshPrice" else None,
        now=2000,
        limit=2,
    )

    rows = {
        row["token_ca"]: row
        for row in db.execute(
            "SELECT token_ca, status, baseline_price, baseline_source FROM paper_missed_signal_attribution"
        )
    }
    assert updated == 2
    assert rows["OldNoPrice"]["status"] == "baseline_missing"
    assert rows["OldNoPrice"]["baseline_source"] == "missing:no_price_source"
    assert rows["FreshPrice"]["status"] == "pending"
    assert rows["FreshPrice"]["baseline_price"] == 1.0
    assert rows["FreshPrice"]["baseline_source"] == "live:test"

    coverage = missed_attribution_coverage(db)
    assert coverage["total"] == 2
    assert coverage["baseline_n"] == 1
    assert coverage["baseline_missing_n"] == 1


def run_tests():
    tests = [
        test_extracts_nested_lifecycle_baseline,
        test_upstream_reject_records_baseline_when_payload_has_price,
        test_missing_baseline_becomes_explicit_and_does_not_block_fresh_rows,
    ]
    for test in tests:
        test()
        print(f"ok - {test.__name__}")


if __name__ == "__main__":
    run_tests()
