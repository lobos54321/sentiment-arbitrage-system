#!/usr/bin/env python3
"""
Structured decision audit trail for paper trader.

This table is intentionally append-only. It answers "why did/didn't we trade"
by recording each major decision boundary with component, decision, reason,
route, source, and the exact payload used at the time.
"""

import json
import logging
import time


log = logging.getLogger("paper_trade.audit")


CREATE_DECISION_AUDIT_SQL = """
CREATE TABLE IF NOT EXISTS paper_decision_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_ts REAL NOT NULL,
    signal_id INTEGER,
    token_ca TEXT,
    symbol TEXT,
    lifecycle_id TEXT,
    trade_id INTEGER,
    signal_ts INTEGER,
    strategy_stage TEXT,
    route TEXT,
    component TEXT NOT NULL,
    event_type TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT,
    data_source TEXT,
    payload_json TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
"""


CREATE_DECISION_AUDIT_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_pde_token ON paper_decision_events(token_ca)",
    "CREATE INDEX IF NOT EXISTS idx_pde_lifecycle ON paper_decision_events(lifecycle_id)",
    "CREATE INDEX IF NOT EXISTS idx_pde_trade ON paper_decision_events(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_pde_component ON paper_decision_events(component, decision)",
    "CREATE INDEX IF NOT EXISTS idx_pde_event_ts ON paper_decision_events(event_ts)",
]


def init_decision_audit(db):
    db.execute(CREATE_DECISION_AUDIT_SQL)
    for sql in CREATE_DECISION_AUDIT_INDEXES:
        db.execute(sql)
    db.commit()


def _json_default(value):
    try:
        return str(value)
    except Exception:
        return "<unserializable>"


def record_decision_event(
    db,
    *,
    component,
    event_type,
    decision,
    reason=None,
    token_ca=None,
    symbol=None,
    lifecycle_id=None,
    trade_id=None,
    signal_ts=None,
    signal_id=None,
    strategy_stage=None,
    route=None,
    data_source=None,
    payload=None,
    event_ts=None,
):
    """Best-effort audit write. Never break trading because audit failed."""
    try:
        db.execute(
            """
            INSERT INTO paper_decision_events
                (event_ts, signal_id, token_ca, symbol, lifecycle_id, trade_id,
                 signal_ts, strategy_stage, route, component, event_type,
                 decision, reason, data_source, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_ts or time.time(),
                signal_id,
                token_ca,
                symbol,
                lifecycle_id,
                trade_id,
                signal_ts,
                strategy_stage,
                route,
                component,
                event_type,
                decision,
                reason,
                data_source,
                json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, default=_json_default),
            ),
        )
        db.commit()
    except Exception as exc:
        log.debug("[AUDIT] decision event write failed: %s", exc)


def signal_payload(sig):
    if not sig:
        return {}
    return {
        "id": sig.get("id"),
        "token_ca": sig.get("token_ca"),
        "symbol": sig.get("symbol"),
        "timestamp": sig.get("timestamp"),
        "signal_type": sig.get("signal_type"),
        "is_ath": sig.get("is_ath"),
        "market_cap": sig.get("market_cap"),
        "holders": sig.get("holders"),
        "volume_24h": sig.get("volume_24h"),
        "top10_pct": sig.get("top10_pct"),
        "hard_gate_status": sig.get("hard_gate_status"),
    }
