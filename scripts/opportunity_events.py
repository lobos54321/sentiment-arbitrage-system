"""Unified opportunity-event projection helpers.

This table is a projection for audit and counterfactual replay.  It should be
written from the same decision path that writes ledger rows, not from an
independent shadow-only path.
"""

from __future__ import annotations

import json
import time
from typing import Any


OPPORTUNITY_EVENT_EXTRA_COLUMNS = (
    ("quote_source", "TEXT"),
    ("quote_age_sec", "REAL"),
    ("data_confidence", "TEXT"),
    ("provider_data_state", "TEXT"),
    ("provider_reason", "TEXT"),
    ("provider_attempts_json", "TEXT"),
    ("evidence_status", "TEXT"),
    ("quote_failure_reason", "TEXT"),
    ("path_sample_count", "INTEGER DEFAULT 0"),
)


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
        return value.strip().lower() in {"1", "true", "yes", "on", "ok", "clean", "available", "executable"}
    return bool(value)


def _table_columns(db, table: str) -> set[str]:
    try:
        return {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        return set()


def _json_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except Exception:
            return [value]
        return parsed if isinstance(parsed, list) else [parsed]
    return [] if value is None else [value]


def _quote_clean_from_event(event: Any) -> bool:
    return (
        _truthy(_get(event, "quote_clean", False))
        and _truthy(_get(event, "quote_executable", False))
        and _truthy(_get(event, "route_available", False))
    )


def _no_route_from_event(event: Any) -> bool:
    if _truthy(_get(event, "no_route_flag", False)):
        return True
    route_available = _get(event, "route_available", None)
    if route_available is not None and not _truthy(route_available):
        return True
    blockers = " ".join(str(item).lower() for item in _json_list(_get(event, "hard_blockers", [])))
    reason = str(
        _get(event, "quote_failure_reason", "")
        or _get(event, "provider_reason", "")
        or _get(event, "reason", "")
        or ""
    ).lower()
    return "no_route" in blockers or "route_unavailable" in blockers or "no_route" in reason or "route_unavailable" in reason


def _evidence_status(event: Any) -> str:
    if _quote_clean_from_event(event):
        return "quote_clean_executable"
    if _no_route_from_event(event):
        return "no_route_or_route_unavailable"
    if not _truthy(_get(event, "quote_available", False)):
        return "quote_missing"
    if not _truthy(_get(event, "quote_executable", False)):
        return "quote_not_executable"
    return "partial_or_unknown"


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
    cols = _table_columns(db, "opportunity_events")
    for col_name, col_def in OPPORTUNITY_EVENT_EXTRA_COLUMNS:
        if col_name not in cols:
            try:
                db.execute(f"ALTER TABLE opportunity_events ADD COLUMN {col_name} {col_def}")
            except Exception:
                pass
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
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS opportunity_event_path_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opportunity_key TEXT NOT NULL,
            sample_ts REAL NOT NULL,
            quote_pnl_pct REAL,
            quote_clean INTEGER DEFAULT 0,
            quote_executable INTEGER DEFAULT 0,
            route_available INTEGER DEFAULT 0,
            no_route_flag INTEGER DEFAULT 0,
            trapped_flag INTEGER DEFAULT 0,
            liquidity_usd REAL,
            spread_pct REAL,
            valuation_sol REAL,
            quote_source TEXT,
            quote_age_sec REAL,
            data_confidence TEXT,
            provider_data_state TEXT,
            provider_reason TEXT,
            raw_payload_json TEXT,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(opportunity_key, sample_ts)
        )
        """
    )
    db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_opportunity_path_samples_key_ts
        ON opportunity_event_path_samples(opportunity_key, sample_ts ASC)
        """
    )
    try:
        db.commit()
    except Exception:
        pass


def record_opportunity_path_sample(db, opportunity_key: str, sample: Any) -> bool:
    init_opportunity_events(db)
    if not opportunity_key:
        return False
    now_ts = float(_get(sample, "updated_at", None) or time.time())
    sample_ts = _safe_float(_get(sample, "sample_ts", _get(sample, "event_ts", _get(sample, "ts"))), now_ts)
    if sample_ts is None:
        return False
    quote_clean = _quote_clean_from_event(sample)
    no_route = _no_route_from_event(sample)
    trapped = _truthy(_get(sample, "trapped_flag", False)) or str(_get(sample, "status", "")).lower() == "trapped"
    db.execute(
        """
        INSERT INTO opportunity_event_path_samples (
            opportunity_key, sample_ts, quote_pnl_pct, quote_clean,
            quote_executable, route_available, no_route_flag, trapped_flag,
            liquidity_usd, spread_pct, valuation_sol, quote_source,
            quote_age_sec, data_confidence, provider_data_state,
            provider_reason, raw_payload_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(opportunity_key, sample_ts) DO UPDATE SET
            quote_pnl_pct=excluded.quote_pnl_pct,
            quote_clean=excluded.quote_clean,
            quote_executable=excluded.quote_executable,
            route_available=excluded.route_available,
            no_route_flag=excluded.no_route_flag,
            trapped_flag=excluded.trapped_flag,
            liquidity_usd=excluded.liquidity_usd,
            spread_pct=excluded.spread_pct,
            valuation_sol=excluded.valuation_sol,
            quote_source=excluded.quote_source,
            quote_age_sec=excluded.quote_age_sec,
            data_confidence=excluded.data_confidence,
            provider_data_state=excluded.provider_data_state,
            provider_reason=excluded.provider_reason,
            raw_payload_json=excluded.raw_payload_json,
            updated_at=excluded.updated_at
        """,
        (
            str(opportunity_key),
            float(sample_ts),
            _safe_float(_get(sample, "quote_pnl_pct", _get(sample, "current_quote_pnl_pct")), None),
            1 if quote_clean else 0,
            1 if _truthy(_get(sample, "quote_executable", False)) else 0,
            1 if _truthy(_get(sample, "route_available", False)) else 0,
            1 if no_route else 0,
            1 if trapped else 0,
            _safe_float(_get(sample, "liquidity_usd"), None),
            _safe_float(_get(sample, "spread_pct"), None),
            _safe_float(_get(sample, "valuation_sol"), None),
            _get(sample, "quote_source"),
            _safe_float(_get(sample, "quote_age_sec"), None),
            _get(sample, "data_confidence"),
            _get(sample, "provider_data_state"),
            _get(sample, "provider_reason"),
            _json_dumps(_get(sample, "raw_payload", sample if isinstance(sample, dict) else {})),
            now_ts,
            now_ts,
        ),
    )
    db.execute(
        """
        UPDATE opportunity_events
           SET path_sample_count = (
             SELECT COUNT(*)
             FROM opportunity_event_path_samples
             WHERE opportunity_key = ?
           ),
               updated_at = ?
         WHERE opportunity_key = ?
        """,
        (str(opportunity_key), now_ts, str(opportunity_key)),
    )
    try:
        db.commit()
    except Exception:
        pass
    return True


def record_decision_time_path_sample(db, opportunity_key: str, event: Any, *, event_ts: float | None = None) -> bool:
    sample_ts = _safe_float(event_ts, _safe_float(_get(event, "event_ts"), None))
    if sample_ts is None:
        return False
    quote_clean = _quote_clean_from_event(event)
    quote_pnl = _safe_float(_get(event, "quote_pnl_pct", _get(event, "current_quote_pnl_pct")), None)
    if quote_pnl is None and quote_clean:
        quote_pnl = 0.0
    return record_opportunity_path_sample(
        db,
        opportunity_key,
        {
            "sample_ts": sample_ts,
            "quote_pnl_pct": quote_pnl,
            "quote_clean": _get(event, "quote_clean", False),
            "quote_executable": _get(event, "quote_executable", False),
            "route_available": _get(event, "route_available", False),
            "no_route_flag": _no_route_from_event(event),
            "trapped_flag": _truthy(_get(event, "trapped_flag", False)),
            "liquidity_usd": _get(event, "liquidity_usd"),
            "spread_pct": _get(event, "spread_pct"),
            "valuation_sol": _get(event, "valuation_sol", _get(event, "entry_size_sol")),
            "quote_source": _get(event, "quote_source"),
            "quote_age_sec": _get(event, "quote_age_sec"),
            "data_confidence": _get(event, "data_confidence"),
            "provider_data_state": _get(event, "provider_data_state"),
            "provider_reason": _get(event, "provider_reason"),
            "raw_payload": _get(event, "raw_payload", event if isinstance(event, dict) else {}),
        },
    )


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
            final_entry_decision_json, raw_payload_json, created_at, updated_at,
            quote_source, quote_age_sec, data_confidence, provider_data_state,
            provider_reason, provider_attempts_json, evidence_status,
            quote_failure_reason
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(opportunity_key) DO UPDATE SET
            quote_available=excluded.quote_available,
            quote_executable=excluded.quote_executable,
            quote_clean=excluded.quote_clean,
            route_available=excluded.route_available,
            liquidity_usd=excluded.liquidity_usd,
            spread_pct=excluded.spread_pct,
            quote_source=excluded.quote_source,
            quote_age_sec=excluded.quote_age_sec,
            data_confidence=excluded.data_confidence,
            provider_data_state=excluded.provider_data_state,
            provider_reason=excluded.provider_reason,
            provider_attempts_json=excluded.provider_attempts_json,
            evidence_status=excluded.evidence_status,
            quote_failure_reason=excluded.quote_failure_reason,
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
            _get(event, "quote_source"),
            _safe_float(_get(event, "quote_age_sec"), None),
            _get(event, "data_confidence"),
            _get(event, "provider_data_state"),
            _get(event, "provider_reason"),
            _json_dumps(_get(event, "provider_attempts", [])),
            _get(event, "evidence_status") or _evidence_status(event),
            _get(event, "quote_failure_reason"),
        ),
    )
    if _get(event, "record_decision_sample", True):
        record_decision_time_path_sample(db, str(opportunity_key), event, event_ts=event_ts)
    try:
        db.commit()
    except Exception:
        pass
    return str(opportunity_key)


def record_linked_trade_path_sample(db, trade_id: str, sample: Any) -> int:
    init_opportunity_events(db)
    if not trade_id:
        return 0
    rows = db.execute(
        """
        SELECT opportunity_key
        FROM opportunity_events
        WHERE linked_trade_id = ?
        ORDER BY event_ts DESC, id DESC
        LIMIT 5
        """,
        (str(trade_id),),
    ).fetchall()
    count = 0
    for row in rows:
        key = row["opportunity_key"] if hasattr(row, "keys") else row[0]
        if record_opportunity_path_sample(db, str(key), sample):
            count += 1
    return count


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
