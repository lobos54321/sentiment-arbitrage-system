#!/usr/bin/env python3
"""Source resonance shadow collector.

This process is observational only. It joins Telegram premium signals with the
external GMGN shadow state and existing quote-clean/audit evidence, then writes
a compact cohort table for later EV analysis. It does not place trades and does
not change routing.
"""

import argparse
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import time


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PAPER_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_SIGNAL_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"
SOURCE_NAME = "source_resonance_shadow"
SOURCE_RESONANCE_QUOTE_LOOKBACK_SEC = int(os.environ.get("SOURCE_RESONANCE_QUOTE_LOOKBACK_SEC", "3600"))


CREATE_SOURCE_RESONANCE_CANDIDATES_SQL = """
CREATE TABLE IF NOT EXISTS source_resonance_candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_ca TEXT NOT NULL,
    symbol TEXT,
    signal_ts INTEGER NOT NULL,
    telegram_signal_id INTEGER,
    signal_type TEXT,
    telegram_seen INTEGER DEFAULT 1,
    telegram_ts INTEGER,
    source_message_ts INTEGER,
    receive_ts INTEGER,
    signal_recorded_ts INTEGER,
    gmgn_pre_seen INTEGER DEFAULT 0,
    gmgn_first_seen_ts INTEGER,
    gmgn_last_seen_ts INTEGER,
    gmgn_lead_time_sec REAL,
    gmgn_seen_count INTEGER DEFAULT 0,
    gmgn_momentum_rounds INTEGER DEFAULT 0,
    gmgn_momentum_confirmed INTEGER DEFAULT 0,
    gmgn_volume_confirmed INTEGER DEFAULT 0,
    gmgn_buy_pressure REAL DEFAULT 0,
    gmgn_last_market_cap REAL,
    gmgn_last_liquidity REAL,
    quote_clean_seen INTEGER DEFAULT 0,
    two_quote_clean_snapshots INTEGER DEFAULT 0,
    entry_quote_success_seen INTEGER DEFAULT 0,
    entry_quote_fail_seen INTEGER DEFAULT 0,
    source_count INTEGER DEFAULT 1,
    resonance_level INTEGER DEFAULT 1,
    resonance_score REAL DEFAULT 0,
    cohort TEXT,
    payload_json TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(token_ca, signal_ts)
)
"""


CREATE_SOURCE_RESONANCE_HEALTH_SQL = """
CREATE TABLE IF NOT EXISTS source_resonance_health (
    source TEXT PRIMARY KEY,
    last_run_ts INTEGER,
    signal_count INTEGER DEFAULT 0,
    candidate_count INTEGER DEFAULT 0,
    gmgn_pre_seen_count INTEGER DEFAULT 0,
    dual_source_count INTEGER DEFAULT 0,
    quote_clean_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    last_error TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
"""


CREATE_LATENCY_AUDIT_EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS latency_audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    token_ca TEXT NOT NULL,
    symbol TEXT,
    signal_ts INTEGER NOT NULL,
    lifecycle_id TEXT,
    stage TEXT NOT NULL,
    event_ts INTEGER,
    event_ts_ms INTEGER,
    lag_from_source_ms INTEGER,
    lag_from_receive_ms INTEGER,
    payload_json TEXT,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, token_ca, signal_ts, stage)
)
"""


RESONANCE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_src_res_token ON source_resonance_candidates(token_ca, signal_ts)",
    "CREATE INDEX IF NOT EXISTS idx_src_res_cohort ON source_resonance_candidates(cohort, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_src_res_level ON source_resonance_candidates(resonance_level, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_latency_token_stage ON latency_audit_events(token_ca, signal_ts, stage)",
]


def connect_db(path, *, readonly=False):
    db_path = Path(path)
    timeout_sec = float(os.environ.get("SOURCE_RESONANCE_SQLITE_TIMEOUT_SEC", "60"))
    if readonly:
        uri = f"file:{db_path}?mode=ro"
        db = sqlite3.connect(uri, uri=True, timeout=timeout_sec)
    else:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(db_path, timeout=timeout_sec)
    db.execute(f"PRAGMA busy_timeout = {int(timeout_sec * 1000)}")
    if not readonly:
        try:
            db.execute("PRAGMA journal_mode = WAL")
        except sqlite3.OperationalError:
            pass
    db.row_factory = sqlite3.Row
    return db


def acquire_process_lock(path):
    lock_path = Path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = lock_path.open("w", encoding="utf-8")
    try:
        fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(f"source resonance shadow lock held at {lock_path}; duplicate worker idling", flush=True)
        return None
    fh.write(str(os.getpid()))
    fh.flush()
    return fh


def candidate_signal_db_paths(primary):
    paths = [
        primary,
        os.environ.get("DB_PATH"),
        os.environ.get("SENTIMENT_DB"),
        "/app/data/sentiment_arb.db",
        "/app/data/sentiment.db",
        PROJECT_ROOT / "data" / "sentiment_arb.db",
        PROJECT_ROOT / "data" / "sentiment.db",
    ]
    seen = set()
    result = []
    for path in paths:
        if not path:
            continue
        text = str(path)
        if text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def connect_signal_db(primary):
    checked = []
    for path in candidate_signal_db_paths(primary):
        checked.append(path)
        try:
            if not Path(path).exists():
                continue
            db = connect_db(path, readonly=True)
            if table_exists(db, "premium_signals"):
                return db, path
            db.close()
        except sqlite3.Error:
            continue
    raise FileNotFoundError(f"premium_signals table not found in signal DB candidates: {checked}")


def init_source_resonance_shadow(db):
    db.execute(CREATE_SOURCE_RESONANCE_CANDIDATES_SQL)
    db.execute(CREATE_SOURCE_RESONANCE_HEALTH_SQL)
    db.execute(CREATE_LATENCY_AUDIT_EVENTS_SQL)
    for sql in RESONANCE_INDEXES:
        db.execute(sql)
    db.commit()


def table_exists(db, table):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return bool(row)


def table_columns(db, table):
    if not table_exists(db, table):
        return set()
    return {row["name"] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}


def normalize_ts_sec(value):
    try:
        if value in (None, ""):
            return None
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if ts <= 0:
        return None
    return int(ts / 1000) if ts > 1_000_000_000_000 else int(ts)


def ts_ms(value):
    ts = normalize_ts_sec(value)
    if ts is None:
        return None
    try:
        raw = float(value)
        if raw > 1_000_000_000_000:
            return int(raw)
    except (TypeError, ValueError):
        pass
    return int(ts * 1000)


def parse_sqlite_datetime(value):
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ"):
        try:
            return int(dt.datetime.strptime(text[:26].replace("Z", ""), fmt.replace("Z", "")).replace(tzinfo=dt.timezone.utc).timestamp())
        except ValueError:
            continue
    try:
        return normalize_ts_sec(float(text))
    except (TypeError, ValueError):
        return None


def row_dict(row):
    return {key: row[key] for key in row.keys()} if row else None


def load_recent_signals(signal_db, *, lookback_hours=24, limit=500, now=None):
    now = int(now or time.time())
    since = now - int(float(lookback_hours) * 3600)
    if not table_exists(signal_db, "premium_signals"):
        return []
    cols = table_columns(signal_db, "premium_signals")
    wanted = [
        "id", "token_ca", "symbol", "market_cap", "holders", "volume_24h", "top10_pct",
        "timestamp", "source_message_ts", "receive_ts", "signal_type", "is_ath",
        "signal_source", "source_event_id", "gate_result", "created_at",
    ]
    select_cols = [col for col in wanted if col in cols]
    if "token_ca" not in select_cols or "timestamp" not in select_cols:
        return []
    rows = signal_db.execute(
        f"SELECT {', '.join(select_cols)} FROM premium_signals ORDER BY id DESC LIMIT ?",
        (int(limit),),
    ).fetchall()
    signals = []
    for row in rows:
        data = row_dict(row)
        signal_ts = normalize_ts_sec(data.get("timestamp"))
        receive_ts = normalize_ts_sec(data.get("receive_ts"))
        created_ts = parse_sqlite_datetime(data.get("created_at"))
        event_ts = receive_ts or signal_ts or created_ts
        if not data.get("token_ca") or not event_ts or event_ts < since:
            continue
        data["_signal_ts_sec"] = signal_ts or event_ts
        data["_receive_ts_sec"] = receive_ts
        data["_created_ts_sec"] = created_ts
        data["_source_message_ts_sec"] = normalize_ts_sec(data.get("source_message_ts"))
        signals.append(data)
    signals.sort(key=lambda item: (item.get("_signal_ts_sec") or 0, item.get("id") or 0))
    return signals


def lookup_gmgn_state(paper_db, token_ca):
    if not token_ca or not table_exists(paper_db, "external_alpha_state"):
        return None
    row = paper_db.execute(
        """
        SELECT *
        FROM external_alpha_state
        WHERE token_ca = ?
        ORDER BY last_seen_ts DESC
        LIMIT 1
        """,
        (token_ca,),
    ).fetchone()
    return row_dict(row)


def lookup_quote_shadow(paper_db, token_ca, signal_ts):
    empty = {
        "quote_clean_seen": 0,
        "two_quote_clean_snapshots": 0,
        "snapshot_pass_seen": 0,
        "quote_snapshot_count": 0,
    }
    if not token_ca or not table_exists(paper_db, "lotto_not_ath_watch_shadow_snapshots"):
        return empty
    params = {
        "token_ca": token_ca,
        "signal_ts": int(signal_ts or 0),
        "lookback_sec": int(SOURCE_RESONANCE_QUOTE_LOOKBACK_SEC),
    }
    row = paper_db.execute(
        """
        SELECT
            COUNT(*) AS snapshot_count,
            SUM(CASE WHEN quote_clean = 1 THEN 1 ELSE 0 END) AS quote_clean_count,
            MAX(CASE WHEN snapshot_pass = 1 THEN 1 ELSE 0 END) AS snapshot_pass_seen
        FROM lotto_not_ath_watch_shadow_snapshots
        WHERE token_ca = @token_ca
          AND (
            @signal_ts = 0
            OR COALESCE(signal_ts, 0) = @signal_ts
            OR ABS(COALESCE(signal_ts, 0) - @signal_ts) <= @lookback_sec
          )
        """,
        params,
    ).fetchone()
    if not row:
        return empty
    quote_clean_count = int(row["quote_clean_count"] or 0)
    return {
        "quote_clean_seen": 1 if quote_clean_count > 0 else 0,
        "two_quote_clean_snapshots": 1 if quote_clean_count >= 2 else 0,
        "snapshot_pass_seen": int(row["snapshot_pass_seen"] or 0),
        "quote_snapshot_count": int(row["snapshot_count"] or 0),
    }


def lookup_entry_quote_audit(paper_db, token_ca, signal_ts):
    empty = {"entry_quote_success_seen": 0, "entry_quote_fail_seen": 0, "first_decision_ts": None}
    if not token_ca or not table_exists(paper_db, "paper_decision_events"):
        return empty
    row = paper_db.execute(
        """
        SELECT
            MIN(event_ts) AS first_decision_ts,
            MAX(CASE
                WHEN component = 'execution_api'
                 AND event_type = 'entry_quote'
                 AND decision NOT IN ('fail', 'fallback', 'filled_synthetic_paper')
                THEN 1 ELSE 0
            END) AS entry_quote_success_seen,
            MAX(CASE WHEN component = 'execution_api' AND event_type = 'entry_quote' AND decision = 'fail' THEN 1 ELSE 0 END) AS entry_quote_fail_seen
        FROM paper_decision_events
        WHERE token_ca = ?
          AND (
            ? = 0
            OR COALESCE(signal_ts, 0) = ?
            OR ABS(COALESCE(signal_ts, 0) - ?) <= ?
          )
        """,
        (
            token_ca,
            int(signal_ts or 0),
            int(signal_ts or 0),
            int(signal_ts or 0),
            int(SOURCE_RESONANCE_QUOTE_LOOKBACK_SEC),
        ),
    ).fetchone()
    if not row:
        return empty
    return {
        "entry_quote_success_seen": int(row["entry_quote_success_seen"] or 0),
        "entry_quote_fail_seen": int(row["entry_quote_fail_seen"] or 0),
        "first_decision_ts": normalize_ts_sec(row["first_decision_ts"]),
    }


def compute_resonance(signal, gmgn_state=None, quote_shadow=None, quote_audit=None):
    quote_shadow = quote_shadow or {}
    quote_audit = quote_audit or {}
    signal_ts = int(signal.get("_signal_ts_sec") or 0)
    source_message_ts = signal.get("_source_message_ts_sec")
    receive_ts = signal.get("_receive_ts_sec")
    telegram_ts = source_message_ts or signal_ts or receive_ts
    gmgn_first_seen = normalize_ts_sec((gmgn_state or {}).get("first_seen_ts"))
    gmgn_last_seen = normalize_ts_sec((gmgn_state or {}).get("last_seen_ts"))
    gmgn_pre_seen = bool(gmgn_first_seen and telegram_ts and gmgn_first_seen <= telegram_ts)
    gmgn_lead = (telegram_ts - gmgn_first_seen) if gmgn_pre_seen else None
    quote_clean_seen = int(quote_shadow.get("quote_clean_seen") or quote_audit.get("entry_quote_success_seen") or 0)
    two_quote_clean = int(quote_shadow.get("two_quote_clean_snapshots") or 0)

    source_count = 1 + (1 if gmgn_pre_seen else 0) + (1 if quote_clean_seen else 0)
    resonance_level = min(3, source_count)
    gmgn_momentum_confirmed = int((gmgn_state or {}).get("momentum_confirmed") or 0)
    gmgn_volume_confirmed = int((gmgn_state or {}).get("volume_confirmed") or 0)
    resonance_score = (
        1.0
        + (1.25 if gmgn_pre_seen else 0.0)
        + (0.75 if gmgn_momentum_confirmed else 0.0)
        + (0.35 if gmgn_volume_confirmed else 0.0)
        + (1.0 if quote_clean_seen else 0.0)
        + (0.4 if two_quote_clean else 0.0)
    )
    if gmgn_pre_seen and quote_clean_seen:
        cohort = "telegram_gmgn_quote_clean"
    elif gmgn_pre_seen:
        cohort = "telegram_gmgn"
    elif quote_clean_seen:
        cohort = "telegram_quote_clean"
    else:
        cohort = "telegram_only"

    payload = {
        "signal": {
            "id": signal.get("id"),
            "market_cap": signal.get("market_cap"),
            "holders": signal.get("holders"),
            "volume_24h": signal.get("volume_24h"),
            "top10_pct": signal.get("top10_pct"),
            "signal_source": signal.get("signal_source"),
            "source_event_id": signal.get("source_event_id"),
        },
        "gmgn": gmgn_state or None,
        "quote_shadow": quote_shadow,
        "quote_audit": quote_audit,
    }
    return {
        "token_ca": signal.get("token_ca"),
        "symbol": signal.get("symbol"),
        "signal_ts": signal_ts,
        "telegram_signal_id": signal.get("id"),
        "signal_type": signal.get("signal_type") or ("ATH" if signal.get("is_ath") else "NEW_TRENDING"),
        "telegram_seen": 1,
        "telegram_ts": telegram_ts,
        "source_message_ts": source_message_ts,
        "receive_ts": receive_ts,
        "signal_recorded_ts": signal.get("_created_ts_sec"),
        "gmgn_pre_seen": 1 if gmgn_pre_seen else 0,
        "gmgn_first_seen_ts": gmgn_first_seen,
        "gmgn_last_seen_ts": gmgn_last_seen,
        "gmgn_lead_time_sec": gmgn_lead,
        "gmgn_seen_count": int((gmgn_state or {}).get("seen_count") or 0),
        "gmgn_momentum_rounds": int((gmgn_state or {}).get("momentum_rounds") or 0),
        "gmgn_momentum_confirmed": gmgn_momentum_confirmed,
        "gmgn_volume_confirmed": gmgn_volume_confirmed,
        "gmgn_buy_pressure": float((gmgn_state or {}).get("buy_pressure") or 0),
        "gmgn_last_market_cap": (gmgn_state or {}).get("last_market_cap"),
        "gmgn_last_liquidity": (gmgn_state or {}).get("last_liquidity"),
        "quote_clean_seen": quote_clean_seen,
        "two_quote_clean_snapshots": two_quote_clean,
        "entry_quote_success_seen": int(quote_audit.get("entry_quote_success_seen") or 0),
        "entry_quote_fail_seen": int(quote_audit.get("entry_quote_fail_seen") or 0),
        "source_count": source_count,
        "resonance_level": resonance_level,
        "resonance_score": resonance_score,
        "cohort": cohort,
        "payload_json": json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
    }


def upsert_candidate(db, candidate):
    keys = [
        "token_ca", "symbol", "signal_ts", "telegram_signal_id", "signal_type",
        "telegram_seen", "telegram_ts", "source_message_ts", "receive_ts",
        "signal_recorded_ts", "gmgn_pre_seen", "gmgn_first_seen_ts",
        "gmgn_last_seen_ts", "gmgn_lead_time_sec", "gmgn_seen_count",
        "gmgn_momentum_rounds", "gmgn_momentum_confirmed", "gmgn_volume_confirmed",
        "gmgn_buy_pressure", "gmgn_last_market_cap", "gmgn_last_liquidity",
        "quote_clean_seen", "two_quote_clean_snapshots", "entry_quote_success_seen",
        "entry_quote_fail_seen", "source_count", "resonance_level",
        "resonance_score", "cohort", "payload_json",
    ]
    update = ", ".join(f"{key} = excluded.{key}" for key in keys if key not in {"token_ca", "signal_ts"})
    db.execute(
        f"""
        INSERT INTO source_resonance_candidates ({', '.join(keys)}, updated_at)
        VALUES ({', '.join('?' for _ in keys)}, CURRENT_TIMESTAMP)
        ON CONFLICT(token_ca, signal_ts) DO UPDATE SET
            {update},
            updated_at = CURRENT_TIMESTAMP
        """,
        tuple(candidate.get(key) for key in keys),
    )


def upsert_latency_event(db, event):
    keys = [
        "source", "token_ca", "symbol", "signal_ts", "lifecycle_id", "stage",
        "event_ts", "event_ts_ms", "lag_from_source_ms", "lag_from_receive_ms",
        "payload_json",
    ]
    db.execute(
        f"""
        INSERT INTO latency_audit_events ({', '.join(keys)}, updated_at)
        VALUES ({', '.join('?' for _ in keys)}, CURRENT_TIMESTAMP)
        ON CONFLICT(source, token_ca, signal_ts, stage) DO UPDATE SET
            lifecycle_id = excluded.lifecycle_id,
            event_ts = excluded.event_ts,
            event_ts_ms = excluded.event_ts_ms,
            lag_from_source_ms = excluded.lag_from_source_ms,
            lag_from_receive_ms = excluded.lag_from_receive_ms,
            payload_json = excluded.payload_json,
            updated_at = CURRENT_TIMESTAMP
        """,
        tuple(event.get(key) for key in keys),
    )


def latency_events_for_signal(signal, quote_audit=None):
    quote_audit = quote_audit or {}
    source_ms = ts_ms(signal.get("source_message_ts")) or ts_ms(signal.get("timestamp"))
    receive_ms = ts_ms(signal.get("receive_ts"))
    signal_ts = int(signal.get("_signal_ts_sec") or 0)
    token_ca = signal.get("token_ca")
    symbol = signal.get("symbol")
    base_payload = {
        "telegram_signal_id": signal.get("id"),
        "signal_source": signal.get("signal_source"),
        "source_event_id": signal.get("source_event_id"),
    }

    stages = [
        ("source_event", source_ms),
        ("telegram_receive", receive_ms),
        ("premium_signal_recorded", (signal.get("_created_ts_sec") * 1000 if signal.get("_created_ts_sec") else None)),
    ]
    first_decision_ts = quote_audit.get("first_decision_ts")
    if first_decision_ts:
        stages.append(("paper_first_decision", int(first_decision_ts) * 1000))

    events = []
    for stage, event_ms in stages:
        if not event_ms:
            continue
        events.append({
            "source": SOURCE_NAME,
            "token_ca": token_ca,
            "symbol": symbol,
            "signal_ts": signal_ts,
            "lifecycle_id": None,
            "stage": stage,
            "event_ts": int(event_ms / 1000),
            "event_ts_ms": int(event_ms),
            "lag_from_source_ms": int(event_ms - source_ms) if source_ms else None,
            "lag_from_receive_ms": int(event_ms - receive_ms) if receive_ms else None,
            "payload_json": json.dumps(base_payload, ensure_ascii=False, sort_keys=True),
        })
    return events


def record_health(db, *, run_ts=None, signal_count=0, candidate_count=0, gmgn_pre_seen_count=0,
                  dual_source_count=0, quote_clean_count=0, error=None):
    run_ts = int(run_ts or time.time())
    db.execute(
        """
        INSERT INTO source_resonance_health
            (source, last_run_ts, signal_count, candidate_count, gmgn_pre_seen_count,
             dual_source_count, quote_clean_count, error_count, last_error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(source) DO UPDATE SET
            last_run_ts = excluded.last_run_ts,
            signal_count = excluded.signal_count,
            candidate_count = excluded.candidate_count,
            gmgn_pre_seen_count = excluded.gmgn_pre_seen_count,
            dual_source_count = excluded.dual_source_count,
            quote_clean_count = excluded.quote_clean_count,
            error_count = source_resonance_health.error_count + excluded.error_count,
            last_error = excluded.last_error,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            SOURCE_NAME,
            run_ts,
            int(signal_count or 0),
            int(candidate_count or 0),
            int(gmgn_pre_seen_count or 0),
            int(dual_source_count or 0),
            int(quote_clean_count or 0),
            1 if error else 0,
            str(error)[:500] if error else None,
        ),
    )
    db.commit()


def run_once(*, paper_db_path=None, signal_db_path=None, lookback_hours=24, limit=500, now=None):
    now = int(now or time.time())
    paper_db = connect_db(paper_db_path or os.environ.get("PAPER_DB") or DEFAULT_PAPER_DB)
    init_source_resonance_shadow(paper_db)
    signal_path = signal_db_path or os.environ.get("SENTIMENT_DB") or os.environ.get("DB_PATH") or DEFAULT_SIGNAL_DB
    signal_db = None
    try:
        signal_db, _selected_signal_path = connect_signal_db(signal_path)
        signals = load_recent_signals(signal_db, lookback_hours=lookback_hours, limit=limit, now=now)
        candidates = []
        for signal in signals:
            token_ca = signal.get("token_ca")
            signal_ts = signal.get("_signal_ts_sec")
            gmgn_state = lookup_gmgn_state(paper_db, token_ca)
            quote_shadow = lookup_quote_shadow(paper_db, token_ca, signal_ts)
            quote_audit = lookup_entry_quote_audit(paper_db, token_ca, signal_ts)
            candidate = compute_resonance(signal, gmgn_state, quote_shadow, quote_audit)
            upsert_candidate(paper_db, candidate)
            for event in latency_events_for_signal(signal, quote_audit):
                upsert_latency_event(paper_db, event)
            candidates.append(candidate)
        paper_db.commit()
        gmgn_pre_seen_count = sum(1 for item in candidates if item["gmgn_pre_seen"])
        quote_clean_count = sum(1 for item in candidates if item["quote_clean_seen"])
        dual_source_count = sum(1 for item in candidates if item["source_count"] >= 2)
        record_health(
            paper_db,
            run_ts=now,
            signal_count=len(signals),
            candidate_count=len(candidates),
            gmgn_pre_seen_count=gmgn_pre_seen_count,
            dual_source_count=dual_source_count,
            quote_clean_count=quote_clean_count,
        )
        return {
            "signals": len(signals),
            "candidates": len(candidates),
            "gmgn_pre_seen": gmgn_pre_seen_count,
            "dual_source": dual_source_count,
            "quote_clean": quote_clean_count,
        }
    except Exception as exc:
        record_health(paper_db, run_ts=now, error=exc)
        raise
    finally:
        try:
            if signal_db:
                signal_db.close()
        finally:
            paper_db.close()


def main():
    parser = argparse.ArgumentParser(description="Build source resonance shadow cohorts")
    parser.add_argument("--paper-db", default=os.environ.get("PAPER_DB") or str(DEFAULT_PAPER_DB))
    parser.add_argument("--signal-db", default=os.environ.get("SENTIMENT_DB") or str(DEFAULT_SIGNAL_DB))
    parser.add_argument("--lookback-hours", type=float, default=float(os.environ.get("SOURCE_RESONANCE_LOOKBACK_HOURS", "24")))
    parser.add_argument("--limit", type=int, default=int(os.environ.get("SOURCE_RESONANCE_LIMIT", "500")))
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=float, default=float(os.environ.get("SOURCE_RESONANCE_INTERVAL_SEC", "60")))
    parser.add_argument("--initial-delay", type=float, default=float(os.environ.get("SOURCE_RESONANCE_INITIAL_DELAY_SEC", "0")))
    parser.add_argument("--lock-file", default=os.environ.get("SOURCE_RESONANCE_LOCK_FILE", "/tmp/source_resonance_shadow.lock"))
    args = parser.parse_args()

    lock_fh = acquire_process_lock(args.lock_file)
    if lock_fh is None:
        while True:
            time.sleep(300)

    if args.loop and args.initial_delay > 0:
        print(f"source resonance shadow initial delay {args.initial_delay:.1f}s", flush=True)
        time.sleep(args.initial_delay)

    while True:
        try:
            summary = run_once(
                paper_db_path=args.paper_db,
                signal_db_path=args.signal_db,
                lookback_hours=args.lookback_hours,
                limit=args.limit,
            )
            print(f"source resonance shadow: {json.dumps(summary, sort_keys=True)}", flush=True)
        except Exception as exc:
            print(f"source resonance shadow error: {exc}", flush=True)
            if not args.loop:
                raise
        if not args.loop:
            break
        time.sleep(max(10.0, args.interval))


if __name__ == "__main__":
    main()
