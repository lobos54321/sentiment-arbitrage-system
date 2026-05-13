#!/usr/bin/env python3
"""External alpha shadow state.

This module records external candidate feeds as evidence only. It does not
submit trades and does not change entry policy. Paper trader can join these
features to Telegram-driven signals for attribution and later analysis.
"""

import json
import os
import sqlite3
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_EXTERNAL_ALPHA_DB = PROJECT_ROOT / "data" / "paper_trades.db"

EXTERNAL_ALPHA_SHADOW_ENABLED = os.environ.get("EXTERNAL_ALPHA_SHADOW_ENABLED", "true").strip().lower() != "false"
GMGN_MOMENTUM_SHADOW_ENABLED = os.environ.get("GMGN_MOMENTUM_SHADOW_ENABLED", "true").strip().lower() != "false"
EXTERNAL_ALPHA_LOOKBACK_SEC = int(os.environ.get("EXTERNAL_ALPHA_LOOKBACK_SEC", "600"))
GMGN_MOMENTUM_MIN_ROUNDS = int(os.environ.get("GMGN_MOMENTUM_MIN_ROUNDS", "3"))
GMGN_MOMENTUM_MIN_GAIN_PCT = float(os.environ.get("GMGN_MOMENTUM_MIN_GAIN_PCT", "5.0"))
GMGN_MOMENTUM_BUY_DECAY_TOLERANCE = float(os.environ.get("GMGN_MOMENTUM_BUY_DECAY_TOLERANCE", "0.80"))


CREATE_EXTERNAL_ALPHA_SNAPSHOTS_SQL = """
CREATE TABLE IF NOT EXISTS external_alpha_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    captured_at INTEGER NOT NULL,
    source TEXT NOT NULL,
    category TEXT,
    chain TEXT NOT NULL,
    token_ca TEXT NOT NULL,
    symbol TEXT,
    name TEXT,
    market_cap REAL,
    liquidity REAL,
    volume REAL,
    swaps REAL,
    buys REAL,
    sells REAL,
    price_change_5m REAL,
    price_change_1h REAL,
    raw_json TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
"""


CREATE_EXTERNAL_ALPHA_STATE_SQL = """
CREATE TABLE IF NOT EXISTS external_alpha_state (
    chain TEXT NOT NULL,
    token_ca TEXT NOT NULL,
    first_seen_ts INTEGER NOT NULL,
    last_seen_ts INTEGER NOT NULL,
    seen_count INTEGER DEFAULT 0,
    changed_count INTEGER DEFAULT 0,
    source_last TEXT,
    category_last TEXT,
    symbol TEXT,
    name TEXT,
    last_market_cap REAL,
    last_liquidity REAL,
    last_volume REAL,
    last_swaps REAL,
    last_buys REAL,
    last_sells REAL,
    momentum_rounds INTEGER DEFAULT 1,
    momentum_start_mc REAL,
    momentum_gain_pct REAL DEFAULT 0,
    momentum_confirmed INTEGER DEFAULT 0,
    volume_confirmed INTEGER DEFAULT 0,
    buy_pressure REAL DEFAULT 0,
    last_snapshot_json TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chain, token_ca)
)
"""


CREATE_EXTERNAL_ALPHA_HEALTH_SQL = """
CREATE TABLE IF NOT EXISTS external_alpha_health (
    source TEXT PRIMARY KEY,
    last_run_ts INTEGER,
    last_success_ts INTEGER,
    candidate_count INTEGER DEFAULT 0,
    recorded_count INTEGER DEFAULT 0,
    momentum_confirmed_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
"""


EXTERNAL_ALPHA_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_eas_token ON external_alpha_snapshots(token_ca)",
    "CREATE INDEX IF NOT EXISTS idx_eas_seen ON external_alpha_state(token_ca, last_seen_ts)",
    "CREATE INDEX IF NOT EXISTS idx_eas_momentum ON external_alpha_state(momentum_confirmed, last_seen_ts)",
]


def _json_default(value):
    try:
        return str(value)
    except Exception:
        return "<unserializable>"


def _f(value, default=0.0):
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _i(value, default=0):
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _timestamp_sec(value, default=None):
    ts = _i(value, default)
    if ts is None:
        return None
    return ts // 1000 if ts > 1_000_000_000_000 else ts


def init_external_alpha_shadow(db):
    db.execute(CREATE_EXTERNAL_ALPHA_SNAPSHOTS_SQL)
    db.execute(CREATE_EXTERNAL_ALPHA_STATE_SQL)
    db.execute(CREATE_EXTERNAL_ALPHA_HEALTH_SQL)
    for sql in EXTERNAL_ALPHA_INDEXES:
        db.execute(sql)
    db.commit()


def connect_external_alpha_db(db_path=None):
    path = Path(db_path or os.environ.get("EXTERNAL_ALPHA_DB") or os.environ.get("PAPER_DB") or DEFAULT_EXTERNAL_ALPHA_DB)
    path.parent.mkdir(parents=True, exist_ok=True)
    timeout_sec = float(os.environ.get("EXTERNAL_ALPHA_SQLITE_TIMEOUT_SEC", "30"))
    db = sqlite3.connect(path, timeout=timeout_sec)
    db.execute(f"PRAGMA busy_timeout = {int(timeout_sec * 1000)}")
    try:
        db.execute("PRAGMA journal_mode = WAL")
    except sqlite3.OperationalError:
        pass
    db.row_factory = sqlite3.Row
    init_external_alpha_shadow(db)
    return db


def _row_to_dict(row):
    if not row:
        return None
    return {key: row[key] for key in row.keys()}


def _candidate_key(candidate):
    chain = str(candidate.get("chain") or "sol").lower()
    ca = candidate.get("ca") or candidate.get("token_ca") or candidate.get("address")
    return chain, ca


def _changed(candidate, state):
    if not state:
        return True
    return any(
        _f(candidate.get(field)) != _f(state.get(state_field))
        for field, state_field in [
            ("market_cap", "last_market_cap"),
            ("volume", "last_volume"),
            ("swaps", "last_swaps"),
            ("buys", "last_buys"),
            ("sells", "last_sells"),
        ]
    )


def compute_next_external_alpha_state(candidate, state=None, captured_at=None):
    """Return the next state row for one normalized external candidate."""
    captured_at = int(captured_at or candidate.get("captured_at") or time.time())
    state = dict(state or {})
    mc = _f(candidate.get("market_cap"))
    volume = _f(candidate.get("volume"))
    swaps = _f(candidate.get("swaps"))
    buys = _f(candidate.get("buys"))
    sells = _f(candidate.get("sells"))
    buy_pressure = buys / max(sells, 1.0) if buys > 0 else 0.0

    first_seen = _i(state.get("first_seen_ts"), captured_at)
    seen_count = _i(state.get("seen_count")) + 1
    changed_count = _i(state.get("changed_count"))
    rounds = max(1, _i(state.get("momentum_rounds"), 1))
    start_mc = _f(state.get("momentum_start_mc"), mc)
    volume_confirmed = bool(_i(state.get("volume_confirmed")))

    changed = _changed(candidate, state)
    if changed:
        changed_count += 1
        prev_mc = _f(state.get("last_market_cap"))
        prev_buys = _f(state.get("last_buys"))
        prev_swaps = _f(state.get("last_swaps"))
        if prev_mc > 0 and mc > prev_mc:
            rounds += 1
            if start_mc <= 0:
                start_mc = prev_mc
        else:
            rounds = 1
            start_mc = mc
        if prev_buys > 0 or prev_swaps > 0:
            volume_confirmed = (
                buys >= prev_buys * GMGN_MOMENTUM_BUY_DECAY_TOLERANCE
                or swaps >= prev_swaps * GMGN_MOMENTUM_BUY_DECAY_TOLERANCE
            )
        elif buys > 0 or swaps > 0:
            volume_confirmed = True

    gain_pct = ((mc - start_mc) / start_mc * 100.0) if start_mc > 0 else 0.0
    momentum_confirmed = (
        GMGN_MOMENTUM_SHADOW_ENABLED
        and rounds >= GMGN_MOMENTUM_MIN_ROUNDS
        and gain_pct >= GMGN_MOMENTUM_MIN_GAIN_PCT
        and volume_confirmed
    )

    return {
        "chain": str(candidate.get("chain") or state.get("chain") or "sol").lower(),
        "token_ca": candidate.get("ca") or candidate.get("token_ca") or candidate.get("address"),
        "first_seen_ts": first_seen,
        "last_seen_ts": captured_at,
        "seen_count": seen_count,
        "changed_count": changed_count,
        "source_last": candidate.get("source") or state.get("source_last"),
        "category_last": candidate.get("category") or state.get("category_last"),
        "symbol": candidate.get("symbol") or state.get("symbol"),
        "name": candidate.get("name") or state.get("name"),
        "last_market_cap": mc,
        "last_liquidity": _f(candidate.get("liquidity")),
        "last_volume": volume,
        "last_swaps": swaps,
        "last_buys": buys,
        "last_sells": sells,
        "momentum_rounds": rounds,
        "momentum_start_mc": start_mc,
        "momentum_gain_pct": gain_pct,
        "momentum_confirmed": int(bool(momentum_confirmed)),
        "volume_confirmed": int(bool(volume_confirmed)),
        "buy_pressure": buy_pressure,
        "last_snapshot_json": json.dumps(candidate, ensure_ascii=False, sort_keys=True, default=_json_default),
    }


def record_external_alpha_candidates(db, candidates, captured_at=None):
    if not EXTERNAL_ALPHA_SHADOW_ENABLED:
        return {"recorded": 0, "momentum_confirmed": 0}
    captured_at = int(captured_at or time.time())
    recorded = 0
    confirmed = 0
    for candidate in candidates:
        chain, ca = _candidate_key(candidate)
        if not ca:
            continue
        raw_json = json.dumps(candidate, ensure_ascii=False, sort_keys=True, default=_json_default)
        db.execute(
            """
            INSERT INTO external_alpha_snapshots
                (captured_at, source, category, chain, token_ca, symbol, name,
                 market_cap, liquidity, volume, swaps, buys, sells,
                 price_change_5m, price_change_1h, raw_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                captured_at,
                candidate.get("source") or "unknown",
                candidate.get("category"),
                chain,
                ca,
                candidate.get("symbol"),
                candidate.get("name"),
                _f(candidate.get("market_cap")),
                _f(candidate.get("liquidity")),
                _f(candidate.get("volume")),
                _f(candidate.get("swaps")),
                _f(candidate.get("buys")),
                _f(candidate.get("sells")),
                _f(candidate.get("price_change_5m")),
                _f(candidate.get("price_change_1h")),
                raw_json,
            ),
        )
        row = db.execute(
            "SELECT * FROM external_alpha_state WHERE chain = ? AND token_ca = ?",
            (chain, ca),
        ).fetchone()
        next_state = compute_next_external_alpha_state(candidate, _row_to_dict(row), captured_at=captured_at)
        db.execute(
            """
            INSERT INTO external_alpha_state
                (chain, token_ca, first_seen_ts, last_seen_ts, seen_count, changed_count,
                 source_last, category_last, symbol, name, last_market_cap, last_liquidity,
                 last_volume, last_swaps, last_buys, last_sells, momentum_rounds,
                 momentum_start_mc, momentum_gain_pct, momentum_confirmed,
                 volume_confirmed, buy_pressure, last_snapshot_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(chain, token_ca) DO UPDATE SET
                first_seen_ts = excluded.first_seen_ts,
                last_seen_ts = excluded.last_seen_ts,
                seen_count = excluded.seen_count,
                changed_count = excluded.changed_count,
                source_last = excluded.source_last,
                category_last = excluded.category_last,
                symbol = excluded.symbol,
                name = excluded.name,
                last_market_cap = excluded.last_market_cap,
                last_liquidity = excluded.last_liquidity,
                last_volume = excluded.last_volume,
                last_swaps = excluded.last_swaps,
                last_buys = excluded.last_buys,
                last_sells = excluded.last_sells,
                momentum_rounds = excluded.momentum_rounds,
                momentum_start_mc = excluded.momentum_start_mc,
                momentum_gain_pct = excluded.momentum_gain_pct,
                momentum_confirmed = excluded.momentum_confirmed,
                volume_confirmed = excluded.volume_confirmed,
                buy_pressure = excluded.buy_pressure,
                last_snapshot_json = excluded.last_snapshot_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            tuple(next_state[k] for k in [
                "chain", "token_ca", "first_seen_ts", "last_seen_ts", "seen_count", "changed_count",
                "source_last", "category_last", "symbol", "name", "last_market_cap", "last_liquidity",
                "last_volume", "last_swaps", "last_buys", "last_sells", "momentum_rounds",
                "momentum_start_mc", "momentum_gain_pct", "momentum_confirmed",
                "volume_confirmed", "buy_pressure", "last_snapshot_json",
            ]),
        )
        recorded += 1
        if next_state["momentum_confirmed"]:
            confirmed += 1
    db.commit()
    return {"recorded": recorded, "momentum_confirmed": confirmed}


def record_external_alpha_health(
    db,
    *,
    source="gmgn_candidate_scout",
    run_ts=None,
    success=False,
    candidate_count=0,
    recorded_count=0,
    momentum_confirmed_count=0,
    error=None,
):
    run_ts = int(run_ts or time.time())
    error_text = str(error)[:500] if error else None
    db.execute(
        """
        INSERT INTO external_alpha_health
            (source, last_run_ts, last_success_ts, candidate_count, recorded_count,
             momentum_confirmed_count, error_count, last_error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(source) DO UPDATE SET
            last_run_ts = excluded.last_run_ts,
            last_success_ts = CASE
                WHEN excluded.last_success_ts IS NOT NULL THEN excluded.last_success_ts
                ELSE external_alpha_health.last_success_ts
            END,
            candidate_count = excluded.candidate_count,
            recorded_count = excluded.recorded_count,
            momentum_confirmed_count = excluded.momentum_confirmed_count,
            error_count = external_alpha_health.error_count + excluded.error_count,
            last_error = excluded.last_error,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            source,
            run_ts,
            run_ts if success else None,
            int(candidate_count or 0),
            int(recorded_count or 0),
            int(momentum_confirmed_count or 0),
            0 if success else 1,
            error_text,
        ),
    )
    db.commit()
    return {
        "source": source,
        "last_run_ts": run_ts,
        "success": bool(success),
        "candidate_count": int(candidate_count or 0),
        "recorded_count": int(recorded_count or 0),
        "momentum_confirmed_count": int(momentum_confirmed_count or 0),
        "error": error_text,
    }


def lookup_external_alpha(db, token_ca, *, chain=None, now=None, signal_ts=None, lookback_sec=None):
    if not EXTERNAL_ALPHA_SHADOW_ENABLED or not token_ca:
        return {"available": False, "reason": "external_alpha_disabled_or_missing_token"}
    now = _timestamp_sec(now, int(time.time()))
    lookback_sec = EXTERNAL_ALPHA_LOOKBACK_SEC if lookback_sec is None else int(lookback_sec)
    params = [token_ca]
    chain_clause = ""
    if chain:
        chain_clause = "AND chain = ?"
        params.append(str(chain).lower())
    row = db.execute(
        f"""
        SELECT * FROM external_alpha_state
        WHERE token_ca = ?
          {chain_clause}
        ORDER BY last_seen_ts DESC
        LIMIT 1
        """,
        tuple(params),
    ).fetchone()
    if not row:
        return {"available": False, "reason": "external_alpha_not_seen"}
    state = _row_to_dict(row)
    first_seen = _timestamp_sec(state.get("first_seen_ts"))
    last_seen = _timestamp_sec(state.get("last_seen_ts"))
    signal_ts_sec = _timestamp_sec(signal_ts, now)
    age_sec = now - last_seen if last_seen is not None else None
    lead_sec = signal_ts_sec - first_seen if first_seen is not None and signal_ts_sec is not None else None
    anomaly_reasons = []
    if first_seen is None:
        anomaly_reasons.append("gmgn_first_seen_missing")
    if last_seen is None:
        anomaly_reasons.append("gmgn_last_seen_missing")
    if lead_sec is not None and lead_sec < 0:
        anomaly_reasons.append("gmgn_seen_after_signal")
    if lead_sec is not None and lead_sec > 24 * 60 * 60:
        anomaly_reasons.append("gmgn_lead_time_unreasonable")
    if age_sec is not None and age_sec < -120:
        anomaly_reasons.append("external_alpha_future_seen")
    if age_sec is None or age_sec > lookback_sec:
        return {
            "available": False,
            "reason": "external_alpha_timestamp_missing" if age_sec is None else "external_alpha_stale",
            "last_seen_age_sec": age_sec,
            "gmgn_first_seen_ts": first_seen,
            "gmgn_last_seen_ts": last_seen,
            "signal_ts_sec": signal_ts_sec,
            "gmgn_lead_time_sec": lead_sec,
            "timestamp_valid": not anomaly_reasons,
            "timestamp_anomaly_reason": ",".join(anomaly_reasons) if anomaly_reasons else None,
        }
    return {
        "available": True,
        "source": "external_alpha_shadow",
        "gmgn_pre_seen": True,
        "gmgn_momentum_confirmed": bool(_i(state.get("momentum_confirmed"))),
        "gmgn_momentum_rounds": _i(state.get("momentum_rounds")),
        "gmgn_momentum_gain_pct": _f(state.get("momentum_gain_pct")),
        "gmgn_volume_confirmed": bool(_i(state.get("volume_confirmed"))),
        "gmgn_buy_pressure": _f(state.get("buy_pressure")),
        "gmgn_seen_count": _i(state.get("seen_count")),
        "gmgn_changed_count": _i(state.get("changed_count")),
        "gmgn_first_seen_ts": first_seen,
        "gmgn_last_seen_ts": last_seen,
        "signal_ts_sec": signal_ts_sec,
        "gmgn_lead_time_sec": lead_sec,
        "last_seen_age_sec": age_sec,
        "timestamp_valid": not anomaly_reasons,
        "timestamp_anomaly_reason": ",".join(anomaly_reasons) if anomaly_reasons else None,
        "source_last": state.get("source_last"),
        "category_last": state.get("category_last"),
        "last_market_cap": _f(state.get("last_market_cap")),
        "last_liquidity": _f(state.get("last_liquidity")),
        "last_volume": _f(state.get("last_volume")),
    }
