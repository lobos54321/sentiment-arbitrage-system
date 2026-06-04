"""Unified opportunity-event projection helpers.

This table is a projection for audit and counterfactual replay.  It should be
written from the same decision path that writes ledger rows, not from an
independent shadow-only path.
"""

from __future__ import annotations

import json
import time
from typing import Any


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, sort_keys=True, default=str)


def _get(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "clean", "available"}
    return bool(value)


def init_opportunity_events(db):
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS opportunity_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opportunity_key TEXT UNIQUE,
            event_ts REAL NOT NULL,
            token_ca TEXT,
            symbol TEXT,
            lifecycle_id TEXT,
            source_type TEXT,
            source_component TEXT,
            source_reason TEXT,
            route_bucket TEXT,
            raw_signal_ts REAL,
            opportunity_ts REAL,
            opportunity_age_sec REAL,
            quote_available INTEGER,
            quote_executable INTEGER,
            quote_clean INTEGER,
            route_available INTEGER,
            liquidity_usd REAL,
            spread_pct REAL,
            market_cap REAL,
            source_strength_score REAL,
            execution_quality_score REAL,
            market_flow_score REAL,
            security_cleanliness_score REAL,
            freshness_lifecycle_score REAL,
            historical_ev_score REAL,
            matrix_score REAL,
            expected_rr REAL,
            defined_risk_pct REAL,
            hard_blockers_json TEXT,
            soft_notes_json TEXT,
            would_enter_a_class INTEGER,
            did_enter INTEGER,
            linked_trade_id TEXT,
            final_entry_decision_json TEXT,
            raw_payload_json TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_opportunity_events_recent
        ON opportunity_events(event_ts DESC, route_bucket, source_type)
        """
    )
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_opportunity_events_token
        ON opportunity_events(token_ca, lifecycle_id, event_ts DESC)
        """
    )
    try:
        db.commit()
    except Exception:
        pass


def record_opportunity_event(db, event: Any) -> str:
    init_opportunity_events(db)
    now_ts = float(_get(event, "updated_at", None) or time.time())
    event_ts = _safe_float(_get(event, "event_ts"), now_ts) or now_ts
    token_ca = _get(event, "token_ca") or ""
    route = _get(event, "route_bucket", _get(event, "route", ""))
    source_type = _get(event, "source_type", _get(event, "source_table", ""))
    source_id = _get(event, "source_id", _get(event, "id", ""))
    opportunity_key = _get(event, "opportunity_key") or f"{source_type}:{source_id}:{token_ca}:{int(event_ts)}"

    db.execute(
        """
        INSERT INTO opportunity_events (
            opportunity_key, event_ts, token_ca, symbol, lifecycle_id,
            source_type, source_component, source_reason, route_bucket,
            raw_signal_ts, opportunity_ts, opportunity_age_sec,
            quote_available, quote_executable, quote_clean, route_available,
            liquidity_usd, spread_pct, market_cap, source_strength_score,
            execution_quality_score, market_flow_score, security_cleanliness_score,
            freshness_lifecycle_score, historical_ev_score, matrix_score,
            expected_rr, defined_risk_pct, hard_blockers_json, soft_notes_json,
            would_enter_a_class, did_enter, linked_trade_id,
            final_entry_decision_json, raw_payload_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(opportunity_key) DO UPDATE SET
            quote_available=excluded.quote_available,
            quote_executable=excluded.quote_executable,
            quote_clean=excluded.quote_clean,
            route_available=excluded.route_available,
            liquidity_usd=excluded.liquidity_usd,
            spread_pct=excluded.spread_pct,
            matrix_score=excluded.matrix_score,
            expected_rr=excluded.expected_rr,
            defined_risk_pct=excluded.defined_risk_pct,
            hard_blockers_json=excluded.hard_blockers_json,
            soft_notes_json=excluded.soft_notes_json,
            would_enter_a_class=excluded.would_enter_a_class,
            did_enter=excluded.did_enter,
            linked_trade_id=COALESCE(excluded.linked_trade_id, linked_trade_id),
            final_entry_decision_json=excluded.final_entry_decision_json,
            raw_payload_json=excluded.raw_payload_json,
            updated_at=excluded.updated_at
        """,
        (
            opportunity_key,
            event_ts,
            token_ca or None,
            _get(event, "symbol"),
            _get(event, "lifecycle_id"),
            source_type or None,
            _get(event, "source_component"),
            _get(event, "source_reason"),
            route or None,
            _safe_float(_get(event, "raw_signal_ts", _get(event, "signal_ts")), None),
            _safe_float(_get(event, "opportunity_ts"), None),
            _safe_float(_get(event, "opportunity_age_sec"), None),
            1 if _truthy(_get(event, "quote_available", False)) else 0,
            1 if _truthy(_get(event, "quote_executable", False)) else 0,
            1 if _truthy(_get(event, "quote_clean", False)) else 0,
            1 if _truthy(_get(event, "route_available", False)) else 0,
            _safe_float(_get(event, "liquidity_usd"), None),
            _safe_float(_get(event, "spread_pct"), None),
            _safe_float(_get(event, "market_cap"), None),
            _safe_float(_get(event, "source_strength_score"), None),
            _safe_float(_get(event, "execution_quality_score"), None),
            _safe_float(_get(event, "market_flow_score"), None),
            _safe_float(_get(event, "security_cleanliness_score"), None),
            _safe_float(_get(event, "freshness_lifecycle_score"), None),
            _safe_float(_get(event, "historical_ev_score"), None),
            _safe_float(_get(event, "matrix_score"), None),
            _safe_float(_get(event, "expected_rr"), None),
            _safe_float(_get(event, "defined_risk_pct"), None),
            _json_dumps(_get(event, "hard_blockers", [])),
            _json_dumps(_get(event, "soft_notes", [])),
            1 if _truthy(_get(event, "would_enter_a_class", False)) else 0,
            1 if _truthy(_get(event, "did_enter", False)) else 0,
            _get(event, "linked_trade_id"),
            _json_dumps(_get(event, "final_entry_decision", {})),
            _json_dumps(_get(event, "raw_payload", event if isinstance(event, dict) else {})),
            now_ts,
            now_ts,
        ),
    )
    try:
        db.commit()
    except Exception:
        pass
    return str(opportunity_key)


def fetch_opportunity_events(db, *, since_ts: float | None = None, limit: int = 50) -> list[dict]:
    init_opportunity_events(db)
    params = {"limit": int(limit)}
    where = ""
    if since_ts is not None:
        where = "WHERE event_ts >= @since_ts"
        params["since_ts"] = float(since_ts)
    rows = db.execute(
        f"""
        SELECT *
        FROM opportunity_events
        {where}
        ORDER BY event_ts DESC, id DESC
        LIMIT @limit
        """,
        params,
    ).fetchall()
    return [dict(row) if hasattr(row, "keys") else dict(row) for row in rows]
