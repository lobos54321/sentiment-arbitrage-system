import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from paper_trade_integrity_audit import build_paper_trade_integrity_audit


def memory_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    return db


def create_schema(db):
    db.executescript(
        """
        CREATE TABLE paper_trades (
            id INTEGER PRIMARY KEY,
            token_ca TEXT,
            symbol TEXT,
            entry_ts REAL,
            entry_price REAL,
            exit_price REAL,
            pnl_pct REAL,
            peak_pnl REAL,
            replay_source TEXT DEFAULT 'live_monitor',
            execution_availability TEXT,
            accounting_outcome TEXT,
            synthetic_close INTEGER DEFAULT 0
        );
        """
    )


def test_integrity_audit_separates_trusted_legacy_and_polluted_rows():
    db = memory_db()
    create_schema(db)
    db.executemany(
        """
        INSERT INTO paper_trades (
            token_ca, symbol, entry_ts, entry_price, exit_price, pnl_pct,
            peak_pnl, replay_source, execution_availability,
            accounting_outcome, synthetic_close
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("T1", "WIN", 1_000, 0.000001, 0.000002, 1.0, 1.2, "live_monitor", "available", "closed_real", 0),
            ("T2", "LOSS", 1_100, 0.000001, 0.0000008, -0.2, 0.0, "live_monitor", "available", "closed_real", 0),
            ("LEG", "BAD", 900, 3e-15, 0.0, 0.0, 100_000, "live_monitor", None, None, 0),
            ("SYN", "SYN", 1_200, 0.000001, 0.000001, 0.0, 0.0, "live_monitor", "unavailable", "closed_synthetic", 1),
        ],
    )
    db.commit()

    audit = build_paper_trade_integrity_audit(db)

    assert audit["total"]["rows"] == 4
    assert audit["trusted_real_available"]["n"] == 2
    assert audit["trusted_real_available"]["win_pct"] == 50.0
    assert audit["legacy_null_accounting"]["n"] == 1
    assert audit["pollution"]["polluted_peak_rows"] == 1
    assert audit["pollution"]["suspicious_tiny_entry_rows"] == 1
    assert audit["trusted_coverage"] == 0.5
