import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from materialize_candidate_shadow_runtime_cross import build_snapshot  # noqa: E402


class CandidateShadowRuntimeMaterializerTest(unittest.TestCase):
    def test_materializes_old_shadow_rows_with_runtime_and_source_evidence(self):
        now = int(time.time())
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "paper.db"
            db = sqlite3.connect(db_path)
            db.executescript(
                """
                CREATE TABLE candidate_shadow_observations (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  signal_id INTEGER NOT NULL,
                  token_ca TEXT NOT NULL,
                  signal_ts INTEGER,
                  candidate_id TEXT NOT NULL,
                  family TEXT,
                  matched INTEGER NOT NULL,
                  reason TEXT,
                  observed_at INTEGER NOT NULL,
                  payload_json TEXT NOT NULL,
                  UNIQUE(signal_id, candidate_id)
                );
                CREATE TABLE candidate_shadow_virtual_trades (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  signal_id INTEGER NOT NULL,
                  token_ca TEXT NOT NULL,
                  signal_ts INTEGER,
                  candidate_id TEXT NOT NULL,
                  family TEXT,
                  status TEXT NOT NULL,
                  entry_ts INTEGER,
                  entry_price REAL,
                  entry_reason TEXT,
                  exit_ts INTEGER,
                  exit_price REAL,
                  exit_reason TEXT,
                  peak_pct REAL,
                  gross_pnl_pct REAL,
                  friction_bps REAL,
                  net_pnl_pct REAL,
                  observed_at INTEGER NOT NULL,
                  payload_json TEXT NOT NULL,
                  UNIQUE(signal_id, candidate_id)
                );
                CREATE TABLE source_resonance_candidates (
                  token_ca TEXT,
                  signal_ts INTEGER,
                  cohort TEXT,
                  quote_clean_seen INTEGER,
                  two_quote_clean_snapshots INTEGER,
                  entry_quote_success_seen INTEGER,
                  entry_quote_fail_seen INTEGER,
                  gmgn_pre_seen INTEGER,
                  gmgn_lead_time_sec REAL,
                  resonance_level INTEGER,
                  resonance_score REAL
                );
                CREATE TABLE paper_decision_events (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  event_ts INTEGER,
                  signal_id INTEGER,
                  token_ca TEXT,
                  lifecycle_state TEXT,
                  entry_bias TEXT,
                  vitality_score REAL,
                  payload_json TEXT
                );
                """
            )
            payload = {"signal_type": "NEW_TRENDING", "hard_gate_status": "PASS"}
            db.execute(
                """
                INSERT INTO candidate_shadow_observations
                  (signal_id, token_ca, signal_ts, candidate_id, family, matched, reason, observed_at, payload_json)
                VALUES (1, 'CA1', ?, 'candidate:a', 'base', 1, 'matched', ?, ?)
                """,
                (now - 120, now - 60, json.dumps(payload)),
            )
            db.execute(
                """
                INSERT INTO candidate_shadow_virtual_trades
                  (signal_id, token_ca, signal_ts, candidate_id, family, status, entry_ts, exit_ts,
                   net_pnl_pct, observed_at, payload_json)
                VALUES (1, 'CA1', ?, 'candidate:a', 'base', 'VIRTUAL_CLOSED', ?, ?, 5.0, ?, '{}')
                """,
                (now - 120, now - 110, now - 30, now - 60),
            )
            db.execute(
                """
                INSERT INTO source_resonance_candidates
                  (token_ca, signal_ts, cohort, quote_clean_seen, two_quote_clean_snapshots,
                   entry_quote_success_seen, entry_quote_fail_seen, gmgn_pre_seen, gmgn_lead_time_sec,
                   resonance_level, resonance_score)
                VALUES ('CA1', ?, 'quote_clean_reclaim', 1, 0, 0, 0, 1, 180, 2, 0.8)
                """,
                (now - 121,),
            )
            db.execute(
                """
                INSERT INTO paper_decision_events
                  (event_ts, signal_id, token_ca, lifecycle_state, entry_bias, vitality_score, payload_json)
                VALUES (?, 1, 'CA1', 'RECLAIM_CONFIRMED', 'PROBE', 0.7, ?)
                """,
                (now - 119, json.dumps({"markov_reclaim_forecast": {"gate": {"markov_bucket": "green"}}})),
            )
            db.commit()
            db.close()

            snapshot = build_snapshot(str(db_path), hours=1, min_closed=1, limit=20)

        self.assertTrue(snapshot["available"])
        self.assertEqual(snapshot["coverage"]["signals"], 1)
        self.assertEqual(snapshot["coverage"]["runtime_signal_features"]["source_quote_clean_seen_signals"], 1)
        self.assertEqual(snapshot["coverage"]["runtime_signal_features"]["markov_bucket_seen_signals"], 1)
        self.assertEqual(snapshot["coverage"]["runtime_signal_features"]["lifecycle_profile_seen_signals"], 1)

        by_dim = snapshot["dimensions"]
        self.assertEqual(by_dim["source_quote_clean"][0]["slice_value"], "true")
        self.assertEqual(by_dim["markov_bucket"][0]["slice_value"], "green")
        self.assertEqual(by_dim["lifecycle_profile"][0]["slice_value"], "RECLAIM_CONFIRMED:PROBE")
        self.assertEqual(by_dim["source_resonance_state"][0]["slice_value"], "quote_clean_reclaim")


if __name__ == "__main__":
    unittest.main()
