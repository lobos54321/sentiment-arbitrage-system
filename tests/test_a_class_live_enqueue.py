import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from canonical_ledger import init_canonical_ledger
from fastlane_config import load_a_class_config
from paper_decision_audit import init_decision_audit
from paper_trade_monitor import (
    active_a_class_fastlane_count,
    enqueue_a_class_fastlane_tiny_candidates,
    pending_requires_quote_clean_for_final_entry,
)


class DummyWatchlist:
    def __init__(self):
        self.rows = {}
        self.next_id = 1

    def register(
        self,
        *,
        ca,
        symbol,
        signal_type,
        pool_address,
        signal_ts,
        premium_signal_id=None,
        signal_price=None,
        signal_mc=0,
        signal_super=0,
        signal_holders=0,
        signal_vol24h=0,
        signal_tx24h=0,
        signal_top10=0,
    ):
        row = {
            "id": self.next_id,
            "ca": ca,
            "token_ca": ca,
            "symbol": symbol,
            "signal_type": signal_type,
            "pool_address": pool_address,
            "signal_ts": signal_ts,
            "premium_signal_id": premium_signal_id,
            "signal_price": signal_price,
            "signal_mc": signal_mc,
            "signal_super": signal_super,
            "signal_holders": signal_holders,
            "signal_vol24h": signal_vol24h,
            "signal_tx24h": signal_tx24h,
            "signal_top10": signal_top10,
        }
        self.rows[self.next_id] = row
        self.next_id += 1
        return dict(row)

    def update_position_state(self, watchlist_id, **updates):
        self.rows[watchlist_id].update(updates)

    def get_by_id(self, watchlist_id):
        row = self.rows.get(watchlist_id)
        return dict(row) if row else None


def _db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    init_decision_audit(db)
    init_canonical_ledger(db)
    return db


def _enter_result(**overrides):
    candidate = {
        "token_ca": "LiveFastlaneToken",
        "symbol": "LIVE",
        "lifecycle_id": "life-live",
        "route_bucket": "ATH",
        "signal_ts": 900,
        "opportunity_ts": 999,
        "current_price": 0.000001,
        "market_cap": 120000,
        "liquidity_usd": 80000,
        "quote_source": "jupiter",
        "quote_age_sec": 2,
        "gmgn_pre_seen": True,
        "source_resonance": True,
        "top10_pct": 35,
        "bundler_rate": 0.01,
        "rat_trader_rate": 0.01,
        "entrapment_ratio": 0.01,
        "source_component": "ath_uncertainty_scout",
        "source_reason": "scout_quality_buy_pressure_weak",
        "raw_payload": {
            "pool": "pool-live",
            "premium_signal_id": 42,
        },
    }
    decision = {
        "score": 100,
        "grade": "A_PLUS",
        "size_sol": 0.003,
        "reason": "a_class_fastlane_pass",
        "expected_rr": 5.0,
        "expected_upside_pct": 1.0,
        "defined_risk_pct": 0.2,
    }
    candidate.update(overrides.pop("candidate", {}))
    decision.update(overrides.pop("decision", {}))
    return {
        "action": "WOULD_ENTER",
        "live_ready": True,
        "candidate": candidate,
        "decision": decision,
        "source_table": "paper_decision_events",
        "source_id": 42,
        "opportunity_key": "a-class-unit",
        **overrides,
    }


def test_a_class_live_enqueue_creates_capped_pending_entry():
    db = _db()
    pending_entries = {}

    enqueued = enqueue_a_class_fastlane_tiny_candidates(
        db,
        DummyWatchlist(),
        pending_entries,
        {},
        a_class_summary={"enter_candidates": [_enter_result()]},
        now_ts=1000,
        config=load_a_class_config({"A_CLASS_ENABLED": "true"}),
        max_positions=10,
    )

    assert enqueued == 1
    assert active_a_class_fastlane_count({}, pending_entries) == 1
    pending = next(iter(pending_entries.values()))
    assert pending["entry_mode"] == "a_class_fastlane_tiny_canary"
    assert pending["kelly_position_sol"] == 0.001
    assert pending["paper_only_scout"] is True
    assert pending["final_reclaim_quote_executable"] is True
    assert pending["expected_rr"] == 5.0
    assert pending["defined_risk_pct"] == 0.2
    assert pending_requires_quote_clean_for_final_entry(pending) is True
    event = db.execute(
        """
        SELECT decision, reason
        FROM paper_decision_events
        WHERE component = 'a_class_live_enqueue'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    assert event["decision"] == "pending"
    assert event["reason"] == "a_class_fastlane_tiny_canary_armed"


def test_a_class_live_enqueue_respects_daily_loss_budget():
    db = _db()
    db.execute(
        """
        INSERT INTO canonical_trade_ledger (
            trade_id, token_ca, entry_ts, exit_ts, realized_pnl_sol,
            is_a_class_fastlane, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("loss-1", "LossToken", 900, 995, -0.006, 1, 900, 995),
    )
    db.commit()
    pending_entries = {}

    enqueued = enqueue_a_class_fastlane_tiny_candidates(
        db,
        DummyWatchlist(),
        pending_entries,
        {},
        a_class_summary={"enter_candidates": [_enter_result()]},
        now_ts=1000,
        config=load_a_class_config(
            {
                "A_CLASS_ENABLED": "true",
                "A_CLASS_LIVE_DAILY_LOSS_BUDGET_SOL": "0.005",
            }
        ),
        max_positions=10,
    )

    assert enqueued == 0
    assert pending_entries == {}
    event = db.execute(
        """
        SELECT decision, reason
        FROM paper_decision_events
        WHERE component = 'a_class_live_enqueue'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    assert event["decision"] == "block"
    assert event["reason"] == "a_class_live_daily_loss_budget_hit"
