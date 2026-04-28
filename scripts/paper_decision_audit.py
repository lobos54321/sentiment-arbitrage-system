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


CREATE_MISSED_ATTRIBUTION_SQL = """
CREATE TABLE IF NOT EXISTS paper_missed_signal_attribution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_event_id INTEGER UNIQUE,
    created_event_ts REAL NOT NULL,
    token_ca TEXT NOT NULL,
    symbol TEXT,
    lifecycle_id TEXT,
    signal_id INTEGER,
    signal_ts INTEGER,
    route TEXT,
    component TEXT NOT NULL,
    decision TEXT NOT NULL,
    reject_reason TEXT,
    baseline_price REAL,
    baseline_source TEXT,
    baseline_ts INTEGER,
    price_5m REAL,
    pnl_5m REAL,
    price_15m REAL,
    pnl_15m REAL,
    price_60m REAL,
    pnl_60m REAL,
    price_24h REAL,
    pnl_24h REAL,
    max_pnl_recorded REAL,
    min_pnl_recorded REAL,
    status TEXT DEFAULT 'pending',
    payload_json TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
"""


CREATE_MISSED_ATTRIBUTION_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_pmsa_token ON paper_missed_signal_attribution(token_ca)",
    "CREATE INDEX IF NOT EXISTS idx_pmsa_status ON paper_missed_signal_attribution(status)",
    "CREATE INDEX IF NOT EXISTS idx_pmsa_route_component ON paper_missed_signal_attribution(route, component)",
    "CREATE INDEX IF NOT EXISTS idx_pmsa_signal_ts ON paper_missed_signal_attribution(signal_ts)",
]


MISSED_HORIZONS = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "60m": 60 * 60,
    "24h": 24 * 60 * 60,
}


def init_decision_audit(db):
    db.execute(CREATE_DECISION_AUDIT_SQL)
    for sql in CREATE_DECISION_AUDIT_INDEXES:
        db.execute(sql)
    db.execute(CREATE_MISSED_ATTRIBUTION_SQL)
    for sql in CREATE_MISSED_ATTRIBUTION_INDEXES:
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
        cur = db.execute(
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
        event_id = cur.lastrowid
        try:
            _maybe_record_missed_attribution(
                db,
                decision_event_id=event_id,
                event_ts=event_ts or time.time(),
                signal_id=signal_id,
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=signal_ts,
                route=route,
                component=component,
                decision=decision,
                reason=reason,
                payload=payload or {},
            )
        except Exception as missed_exc:
            log.debug("[AUDIT] missed attribution write failed: %s", missed_exc)
        db.commit()
    except Exception as exc:
        log.debug("[AUDIT] decision event write failed: %s", exc)


def _normalize_signal_ts_seconds(value):
    if value is None:
        return None
    try:
        ts = int(value)
    except (TypeError, ValueError):
        return None
    return ts // 1000 if ts > 1_000_000_000_000 else ts


def _extract_baseline_price(payload):
    for key in (
        "signal_price",
        "current_price",
        "trigger_price",
        "momentum_final_price",
        "quote_price",
        "entry_price",
    ):
        value = (payload or {}).get(key)
        try:
            price = float(value)
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price, key
    return None, None


def _should_track_missed(*, component, event_type=None, decision=None, route=None):
    route_u = (route or "").upper()
    component = component or ""
    decision = (decision or "").lower()

    if route_u not in {"LOTTO", "MATRIX", "ATH", "NOT_ATH", "WATCHLIST"}:
        return False
    if decision not in {"reject", "skip", "abort", "remove", "expire"}:
        return False
    if component in {"signal_ingest", "trade_lifecycle"}:
        return False
    return True


def _maybe_record_missed_attribution(
    db,
    *,
    decision_event_id,
    event_ts,
    signal_id=None,
    token_ca=None,
    symbol=None,
    lifecycle_id=None,
    signal_ts=None,
    route=None,
    component=None,
    decision=None,
    reason=None,
    payload=None,
):
    if not token_ca:
        return
    if not _should_track_missed(component=component, decision=decision, route=route):
        return

    baseline_price, baseline_source = _extract_baseline_price(payload or {})
    signal_ts_sec = _normalize_signal_ts_seconds(signal_ts)
    baseline_ts = signal_ts_sec or int(event_ts)
    db.execute(
        """
        INSERT OR IGNORE INTO paper_missed_signal_attribution
            (decision_event_id, created_event_ts, token_ca, symbol, lifecycle_id,
             signal_id, signal_ts, route, component, decision, reject_reason,
             baseline_price, baseline_source, baseline_ts, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            decision_event_id,
            event_ts,
            token_ca,
            symbol,
            lifecycle_id,
            signal_id,
            signal_ts_sec,
            route,
            component,
            decision,
            reason,
            baseline_price,
            baseline_source,
            baseline_ts,
            json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, default=_json_default),
        ),
    )


def update_due_missed_attributions(
    db,
    *,
    historical_price_fetcher=None,
    live_price_fetcher=None,
    now=None,
    limit=50,
):
    """Fill missed-signal 5m/15m/60m/24h outcomes from non-shadow price data."""
    now = int(now or time.time())
    rows = db.execute(
        """
        SELECT *
        FROM paper_missed_signal_attribution
        WHERE status != 'complete'
        ORDER BY id ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    updated = 0
    for row in rows:
        token_ca = row["token_ca"]
        base_ts = row["baseline_ts"] or row["signal_ts"] or int(row["created_event_ts"])
        baseline_price = row["baseline_price"]
        baseline_source = row["baseline_source"]

        if not baseline_price and historical_price_fetcher:
            hist = historical_price_fetcher(token_ca, base_ts)
            if hist:
                baseline_price, baseline_source = hist[0], hist[1]

        if not baseline_price and live_price_fetcher:
            live = live_price_fetcher(token_ca)
            if live:
                baseline_price, baseline_source = live[0], live[1]
                base_ts = int(live[2] or now)

        if not baseline_price or baseline_price <= 0:
            continue

        changes = {
            "baseline_price": float(baseline_price),
            "baseline_source": baseline_source,
            "baseline_ts": int(base_ts),
        }
        pnl_values = []
        for name, offset in MISSED_HORIZONS.items():
            price_col = f"price_{name}"
            pnl_col = f"pnl_{name}"
            if row[price_col] is not None:
                pnl_values.append(float(row[pnl_col] or 0))
                continue
            due_ts = int(base_ts) + offset
            if due_ts > now:
                continue
            price = None
            source = None
            if historical_price_fetcher:
                hist = historical_price_fetcher(token_ca, due_ts)
                if hist:
                    price, source = hist[0], hist[1]
            if price is None and live_price_fetcher and name == "5m":
                live = live_price_fetcher(token_ca)
                if live:
                    price, source = live[0], live[1]
            if price and price > 0:
                pnl = (float(price) / float(baseline_price)) - 1.0
                changes[price_col] = float(price)
                changes[pnl_col] = pnl
                pnl_values.append(pnl)

        for name in MISSED_HORIZONS:
            if changes.get(f"pnl_{name}") is not None:
                continue
            existing = row[f"pnl_{name}"]
            if existing is not None:
                pnl_values.append(float(existing))

        if pnl_values:
            changes["max_pnl_recorded"] = max(pnl_values)
            changes["min_pnl_recorded"] = min(pnl_values)

        complete = all(
            (changes.get(f"price_{name}") is not None or row[f"price_{name}"] is not None)
            for name in MISSED_HORIZONS
        )
        changes["status"] = "complete" if complete else "pending"
        changes["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")

        set_clause = ", ".join(f"{key} = ?" for key in changes)
        db.execute(
            f"UPDATE paper_missed_signal_attribution SET {set_clause} WHERE id = ?",
            [*changes.values(), row["id"]],
        )
        updated += 1

    if updated:
        db.commit()
    return updated


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
