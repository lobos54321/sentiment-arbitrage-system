#!/usr/bin/env python3
"""Paper fast-entry lane.

This worker is paper-only. It turns strong, already-recorded signals into a
small quote-anchored paper probe without waiting for the heavier paper monitor
main loop. The main monitor later adopts open rows from paper_trades and owns
exit lifecycle management.
"""

import argparse
import datetime as dt
import fcntl
import json
import logging
import os
from pathlib import Path
import signal
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import paper_trade_monitor as ptm
from entry_mode_quality import evaluate_entry_mode_quality
from entry_readiness_policy import (
    build_clean_dog_reclaim_eligibility,
    build_entry_execution_eligibility,
)
from sqlite_write_coordinator import SQLiteSingleWriterLock


DEFAULT_PAPER_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_SIGNAL_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"

log = logging.getLogger("paper_fast_lane")

FAST_LANE_POLICY_VERSION = os.environ.get("PAPER_FAST_LANE_POLICY_VERSION", "fast_lane_v1")
CLEAN_DOG_RECLAIM_POLICY_VERSION = os.environ.get(
    "CLEAN_DOG_RECLAIM_POLICY_VERSION",
    "clean_dog_reclaim_v2",
)
FAST_ENTRY_ENABLED = os.environ.get("PAPER_FAST_ENTRY_ENABLED", "true").lower() != "false"
FAST_ENTRY_SIZE_SOL = float(os.environ.get("FAST_ENTRY_SIZE_SOL", "0.002"))
FAST_ENTRY_DEGRADED_SIZE_SOL = float(os.environ.get("FAST_ENTRY_DEGRADED_SIZE_SOL", "0.001"))
FAST_ENTRY_MAX_QUEUE_AGE_SEC = float(os.environ.get("FAST_ENTRY_MAX_QUEUE_AGE_SEC", "120"))
FAST_ENTRY_CLAIM_TTL_SEC = float(os.environ.get("FAST_ENTRY_CLAIM_TTL_SEC", "20"))
FAST_ENTRY_QUOTE_TIMEOUT_SEC = float(os.environ.get("FAST_ENTRY_QUOTE_TIMEOUT_SEC", "5"))
FAST_ENTRY_PER_TOKEN_COOLDOWN_SEC = float(os.environ.get("FAST_ENTRY_PER_TOKEN_COOLDOWN_SEC", "120"))
FAST_ENTRY_GLOBAL_MAX_PER_MIN = int(os.environ.get("FAST_ENTRY_GLOBAL_MAX_PER_MIN", "10"))
FAST_ENTRY_MAX_OPEN_POSITIONS = int(os.environ.get("FAST_ENTRY_MAX_OPEN_POSITIONS", "50"))
FAST_ENTRY_MAX_DRIFT_PCT = float(os.environ.get("FAST_ENTRY_MAX_DRIFT_PCT", "15"))
FAST_ENTRY_HARD_DRIFT_PCT = float(os.environ.get("FAST_ENTRY_HARD_DRIFT_PCT", "40"))
FAST_ENTRY_DEGRADE_DRIFT_PCT = float(os.environ.get("FAST_ENTRY_DEGRADE_DRIFT_PCT", "8"))
FAST_ENTRY_DEGRADE_LATENCY_SEC = float(os.environ.get("FAST_ENTRY_DEGRADE_LATENCY_SEC", "10"))
FAST_ENTRY_RETRY_LATENCY_SEC = float(os.environ.get("FAST_ENTRY_RETRY_LATENCY_SEC", "30"))
FAST_ENTRY_HARD_STALE_SEC = float(os.environ.get("FAST_ENTRY_HARD_STALE_SEC", "120"))
FAST_ENTRY_SCAN_INTERVAL_SEC = float(os.environ.get("FAST_ENTRY_SCAN_INTERVAL_SEC", "0.75"))
FAST_ENTRY_QUEUE_DEDUPE_SEC = float(os.environ.get("FAST_ENTRY_QUEUE_DEDUPE_SEC", "90"))
FAST_ENTRY_MAX_QUEUE_DEPTH = int(os.environ.get("FAST_ENTRY_MAX_QUEUE_DEPTH", "80"))
FAST_ENTRY_PRESSURE_PRIORITY_CUTOFF = int(os.environ.get("FAST_ENTRY_PRESSURE_PRIORITY_CUTOFF", "15"))
FAST_ENTRY_PREMIUM_BATCH_LIMIT = int(os.environ.get("FAST_ENTRY_PREMIUM_BATCH_LIMIT", "80"))
FAST_ENTRY_SOURCE_SCAN_LIMIT = int(os.environ.get("FAST_ENTRY_SOURCE_SCAN_LIMIT", "40"))
FAST_ENTRY_SOURCE_LOOKBACK_SEC = int(os.environ.get("FAST_ENTRY_SOURCE_LOOKBACK_SEC", "30"))
FAST_ENTRY_MISSED_RESCUE_LIMIT = int(os.environ.get("FAST_ENTRY_MISSED_RESCUE_LIMIT", "30"))
FAST_ENTRY_CLEAN_DOG_RECLAIM_PRIORITY = int(os.environ.get("FAST_ENTRY_CLEAN_DOG_RECLAIM_PRIORITY", "8"))
FAST_ENTRY_CLEAN_DOG_BRONZE_PRIORITY = int(os.environ.get("FAST_ENTRY_CLEAN_DOG_BRONZE_PRIORITY", "12"))
FAST_ENTRY_HARD_GATE_DIRECT_ENABLED = os.environ.get(
    "FAST_ENTRY_HARD_GATE_DIRECT_ENABLED",
    "false",
).lower() == "true"
FAST_ENTRY_SOURCE_GMGN_ONLY_DIRECT_ENABLED = os.environ.get(
    "FAST_ENTRY_SOURCE_GMGN_ONLY_DIRECT_ENABLED",
    "false",
).lower() == "true"
FAST_ENTRY_SOURCE_QUOTE_CLEAN_ACTIVITY_REQUIRED = os.environ.get(
    "FAST_ENTRY_SOURCE_QUOTE_CLEAN_ACTIVITY_REQUIRED",
    "true",
).lower() != "false"
FAST_ENTRY_SOURCE_QUOTE_CLEAN_MAX_UPDATE_AGE_SEC = float(os.environ.get(
    "FAST_ENTRY_SOURCE_QUOTE_CLEAN_MAX_UPDATE_AGE_SEC",
    "60",
))
FAST_ENTRY_SOURCE_QUOTE_CLEAN_MAX_ORIGINAL_AGE_SEC = float(os.environ.get(
    "FAST_ENTRY_SOURCE_QUOTE_CLEAN_MAX_ORIGINAL_AGE_SEC",
    "180",
))
FAST_ENTRY_SOURCE_GMGN_MOMENTUM_CANARY_ENABLED = os.environ.get(
    "FAST_ENTRY_SOURCE_GMGN_MOMENTUM_CANARY_ENABLED",
    "true",
).lower() != "false"
FAST_ENTRY_SOURCE_GMGN_CANARY_MIN_RESONANCE_LEVEL = int(os.environ.get(
    "FAST_ENTRY_SOURCE_GMGN_CANARY_MIN_RESONANCE_LEVEL",
    "3",
))
FAST_ENTRY_SOURCE_GMGN_CANARY_QUIET_ENABLED = os.environ.get(
    "FAST_ENTRY_SOURCE_GMGN_CANARY_QUIET_ENABLED",
    "false",
).lower() == "true"
FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_ENABLED = os.environ.get(
    "FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_ENABLED",
    "true",
).lower() != "false"
FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_MAX_ORIGINAL_AGE_SEC = float(os.environ.get(
    "FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_MAX_ORIGINAL_AGE_SEC",
    "240",
))
FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_MAX_UPDATE_AGE_SEC = float(os.environ.get(
    "FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_MAX_UPDATE_AGE_SEC",
    "30",
))
FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_REQUIRE_TWO_SNAPSHOTS = os.environ.get(
    "FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_REQUIRE_TWO_SNAPSHOTS",
    "true",
).lower() != "false"
FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_QUIET_ENABLED = os.environ.get(
    "FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_QUIET_ENABLED",
    "false",
).lower() == "true"
FAST_ENTRY_KLINE_RESCUE_DIRECT_ENABLED = os.environ.get(
    "FAST_ENTRY_KLINE_RESCUE_DIRECT_ENABLED",
    "false",
).lower() == "true"
FAST_ENTRY_KLINE_RECOVERY_CANARY_ENABLED = os.environ.get(
    "FAST_ENTRY_KLINE_RECOVERY_CANARY_ENABLED",
    "true",
).lower() != "false"
FAST_ENTRY_NOT_ATH_RECLAIM_CANARY_ENABLED = os.environ.get(
    "FAST_ENTRY_NOT_ATH_RECLAIM_CANARY_ENABLED",
    "true",
).lower() != "false"
FAST_ENTRY_SMART_QUALITY_RECHECK_CANARY_ENABLED = os.environ.get(
    "FAST_ENTRY_SMART_QUALITY_RECHECK_CANARY_ENABLED",
    "true",
).lower() != "false"
FAST_ENTRY_MATRIX_TIMEOUT_CANARY_ENABLED = os.environ.get(
    "FAST_ENTRY_MATRIX_TIMEOUT_CANARY_ENABLED",
    "true",
).lower() != "false"
FAST_ENTRY_RECOVERY_MAX_TRADABLE_AGE_SEC = float(os.environ.get(
    "FAST_ENTRY_RECOVERY_MAX_TRADABLE_AGE_SEC",
    "120",
))
FAST_ENTRY_TTL_RESCUE_MAX_TRADABLE_AGE_SEC = float(os.environ.get(
    "FAST_ENTRY_TTL_RESCUE_MAX_TRADABLE_AGE_SEC",
    "300",
))
FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC = float(os.environ.get(
    "FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC",
    "300",
))
FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC = float(os.environ.get(
    "FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC",
    str(8 * 60 * 60),
))
FAST_ENTRY_BRANCH_CIRCUIT_ENABLED = os.environ.get(
    "FAST_ENTRY_BRANCH_CIRCUIT_ENABLED",
    "true",
).lower() != "false"
FAST_ENTRY_BRANCH_CIRCUIT_LOOKBACK_SEC = float(os.environ.get(
    "FAST_ENTRY_BRANCH_CIRCUIT_LOOKBACK_SEC",
    str(24 * 60 * 60),
))
FAST_ENTRY_BRANCH_CIRCUIT_MIN_CLOSED = int(os.environ.get(
    "FAST_ENTRY_BRANCH_CIRCUIT_MIN_CLOSED",
    "20",
))
FAST_ENTRY_BRANCH_CIRCUIT_AVG_PNL_FLOOR = float(os.environ.get(
    "FAST_ENTRY_BRANCH_CIRCUIT_AVG_PNL_FLOOR",
    "-0.03",
))
FAST_ENTRY_BRANCH_CIRCUIT_P10_PNL_FLOOR = float(os.environ.get(
    "FAST_ENTRY_BRANCH_CIRCUIT_P10_PNL_FLOOR",
    "-0.30",
))
FAST_ENTRY_BRANCH_CIRCUIT_MAX_LOSS_FLOOR = float(os.environ.get(
    "FAST_ENTRY_BRANCH_CIRCUIT_MAX_LOSS_FLOOR",
    "-0.80",
))
FAST_ENTRY_SOURCE_MIN_LIQUIDITY_USD = float(os.environ.get(
    "FAST_ENTRY_SOURCE_MIN_LIQUIDITY_USD",
    "5000",
))

FAST_LANE_HARD_REJECT_STATUSES = {
    "GMGN_REJECT",
    "HONEYPOT",
    "RUG",
    "ILLIQUID_JUNK",
}

FAST_LANE_PREMIUM_PASS_STATUSES = {
    "PASS",
}

FAST_LANE_RESCUE_STATUSES = {
    "NOT_ATH_PREBUY_KLINE_RETRY_EXPIRED",
    "NOT_ATH_PREBUY_KLINE_BLOCK",
    "NOT_ATH_PREBUY_KLINE_UNKNOWN_DATA_BLOCKED",
}

FAST_LANE_MISSED_REASONS = {
    "tracking_ttl_expired",
    "not_ath_v17",
    "not_ath_prebuy_kline_retry_expired",
    "not_ath_prebuy_kline_block",
    "entry_edge_spread_too_high",
    "missing_trigger_or_quote",
    "entry_edge_probe_missing_trigger_or_quote",
    "lotto_stale",
}

KLINE_RESCUE_BRANCHES = {
    "not_ath_prebuy_kline_unknown_data_blocked",
    "not_ath_prebuy_kline_block",
    "not_ath_prebuy_kline_retry_expired",
}

CLEAN_DOG_RECLAIM_BRANCHES = {
    "not_ath_reclaim_quote_clean_tiny_probe",
    "tracking_ttl_reclaim_quote_clean_tiny_probe",
    "pre_pass_stale_reclaim_quote_clean_tiny_probe",
    "smart_entry_reclaim_quote_clean_tiny_probe",
}

SMART_QUALITY_RECHECK_REASONS = {
    "weak_buying_pressure",
    "no_kline_low_volume",
    "negative_trend",
    "chasing_top",
    "scout_quality_buy_pressure_weak",
    "scout_quality_volume_low",
    "scout_quality_tx_low",
    "scout_quality_negative_trend",
}

MATRIX_TIMEOUT_RECHECK_REASONS = {
    "matrices not yet aligned",
}

DEGRADED_CANARY_BRANCHES = {
    "source_gmgn_momentum_canary",
    "source_quote_clean_refresh_tiny_probe",
    "ttl_final_reclaim_quote_clean",
    *CLEAN_DOG_RECLAIM_BRANCHES,
    "kline_recovery_quote_clean_tiny_probe",
    "smart_quality_reclaim_tiny_probe",
    "matrix_timeout_final_quote_tiny_probe",
}

SQLITE_WRITE_LOCK = SQLiteSingleWriterLock("paper_fast_lane")


def connect_db(path):
    timeout_sec = float(os.environ.get("PAPER_FAST_LANE_SQLITE_TIMEOUT_SEC", "60"))
    db = sqlite3.connect(path, timeout=timeout_sec, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute(f"PRAGMA busy_timeout = {int(timeout_sec * 1000)}")
    try:
        db.execute("PRAGMA journal_mode = WAL")
    except sqlite3.OperationalError:
        pass
    return db


def table_exists(db, name):
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def table_columns(db, name):
    try:
        return {row[1] for row in db.execute(f"PRAGMA table_info({name})").fetchall()}
    except sqlite3.OperationalError:
        return set()


def optional_col(cols, name, default="NULL"):
    return name if name in cols else f"{default} AS {name}"


def unixish_sql_expr(column):
    return (
        f"CASE WHEN COALESCE({column}, 0) > 1000000000000 "
        f"THEN CAST({column} / 1000 AS INTEGER) "
        f"ELSE CAST(COALESCE({column}, 0) AS INTEGER) END"
    )


def add_column_if_missing(db, table, column, definition):
    if column not in table_columns(db, table):
        db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def market_session_for_ts(value):
    ts = normalize_ts_sec(value)
    try:
        hour = dt.datetime.fromtimestamp(int(ts), dt.timezone.utc).hour
    except (TypeError, ValueError, OSError, OverflowError):
        hour = dt.datetime.now(dt.timezone.utc).hour
    if 0 <= hour < 8:
        return "asia"
    if 8 <= hour < 14:
        return "europe"
    if 14 <= hour < 22:
        return "us"
    return "quiet"


def status_history_append(raw, *, status, error=None, now_ts=None):
    now_ts = float(now_ts if now_ts is not None else time.time())
    try:
        history = json.loads(raw or "[]")
        if not isinstance(history, list):
            history = []
    except (TypeError, json.JSONDecodeError):
        history = []
    history.append({
        "ts": now_ts,
        "status": status,
        "error": error,
    })
    return json.dumps(history[-20:], ensure_ascii=False)


def init_fast_lane_schema(db):
    with SQLITE_WRITE_LOCK:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_fast_entry_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at REAL NOT NULL,
                source_signal_ts INTEGER,
                signal_receive_ts INTEGER,
                signal_recorded_ts INTEGER,
                token_ca TEXT NOT NULL,
                symbol TEXT,
                source_type TEXT NOT NULL,
                entry_mode_hint TEXT,
                entry_branch TEXT,
                hard_gate_status TEXT,
                source_resonance_cohort TEXT,
                trigger_price REAL,
                trigger_mc REAL,
                priority INTEGER DEFAULT 50,
                status TEXT DEFAULT 'queued',
                attempt_count INTEGER DEFAULT 0,
                last_error TEXT,
                claimed_by TEXT,
                claimed_at REAL,
                decision_deadline_ts REAL,
                quote_deadline_ts REAL,
                queue_key TEXT NOT NULL UNIQUE,
                payload_json TEXT,
                updated_at REAL NOT NULL
            )
            """
        )
        add_column_if_missing(db, "paper_fast_entry_queue", "first_error", "TEXT")
        add_column_if_missing(db, "paper_fast_entry_queue", "first_error_at", "REAL")
        add_column_if_missing(db, "paper_fast_entry_queue", "status_history_json", "TEXT")
        add_column_if_missing(db, "paper_fast_entry_queue", "market_session", "TEXT")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pfeq_status_priority ON paper_fast_entry_queue(status, priority, created_at)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pfeq_token_status ON paper_fast_entry_queue(token_ca, status)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pfeq_queue_key ON paper_fast_entry_queue(queue_key)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pfeq_first_error ON paper_fast_entry_queue(first_error)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pfeq_market_session ON paper_fast_entry_queue(market_session)")
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_fast_missed_rescue_state (
                missed_attribution_id INTEGER PRIMARY KEY,
                rescue_signature TEXT NOT NULL,
                last_status TEXT,
                last_reason TEXT,
                last_action_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        add_column_if_missing(db, "paper_fast_missed_rescue_state", "token_ca", "TEXT")
        add_column_if_missing(db, "paper_fast_missed_rescue_state", "entry_branch", "TEXT")
        add_column_if_missing(db, "paper_fast_missed_rescue_state", "entry_mode_hint", "TEXT")
        add_column_if_missing(db, "paper_fast_missed_rescue_state", "policy_version", "TEXT")
        add_column_if_missing(db, "paper_fast_missed_rescue_state", "state", "TEXT")
        add_column_if_missing(db, "paper_fast_missed_rescue_state", "blocker", "TEXT")
        add_column_if_missing(db, "paper_fast_missed_rescue_state", "first_seen_at", "REAL")
        add_column_if_missing(db, "paper_fast_missed_rescue_state", "last_clean_quote_ts", "REAL")
        add_column_if_missing(db, "paper_fast_missed_rescue_state", "last_tradable_ts", "REAL")
        add_column_if_missing(db, "paper_fast_missed_rescue_state", "eligibility_json", "TEXT")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pfmr_state_updated ON paper_fast_missed_rescue_state(updated_at)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pfmr_state_state ON paper_fast_missed_rescue_state(state, updated_at)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pfmr_state_branch ON paper_fast_missed_rescue_state(entry_branch, state)")
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_entry_locks (
                token_ca TEXT PRIMARY KEY,
                lifecycle_id TEXT,
                lock_reason TEXT,
                owner TEXT,
                expires_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        db.execute("CREATE INDEX IF NOT EXISTS idx_pel_expires ON paper_entry_locks(expires_at)")
        db.commit()


def normalize_ts_sec(value):
    normalized = ptm.normalize_signal_ts_seconds(value)
    if normalized is not None:
        return normalized
    try:
        normalized = ptm.normalize_epoch_ts(value)
    except Exception:
        normalized = None
    return normalized or int(time.time())


def parse_datetime_ts(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return normalize_ts_sec(value)
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            parsed = dt.datetime.strptime(text.replace("Z", "")[:26], fmt)
            return int(parsed.replace(tzinfo=dt.timezone.utc).timestamp())
        except ValueError:
            continue
    try:
        return normalize_ts_sec(float(text))
    except (TypeError, ValueError):
        return None


def queue_key(source_type, token_ca, signal_ts, branch):
    return f"{source_type}:{token_ca}:{int(normalize_ts_sec(signal_ts))}:{branch}"


def active_queue_depth(db):
    row = db.execute(
        """
        SELECT COUNT(*) AS c
        FROM paper_fast_entry_queue
        WHERE status IN ('queued', 'claimed', 'retry_watch')
        """
    ).fetchone()
    return int(row["c"] or 0)


def existing_recent_queue_row(db, token_ca, now_ts):
    return db.execute(
        """
        SELECT id, priority, status, entry_branch, status_history_json, first_error, first_error_at
        FROM paper_fast_entry_queue
        WHERE token_ca = ?
          AND updated_at >= ?
          AND status IN ('queued', 'claimed', 'retry_watch', 'entered', 'rejected', 'quote_failed', 'skipped', 'rate_limited', 'watch_only', 'counterfactual_only')
        ORDER BY
          CASE WHEN status IN ('queued', 'claimed', 'retry_watch') THEN 0 ELSE 1 END,
          priority ASC,
          updated_at DESC
        LIMIT 1
        """,
        (token_ca, now_ts - FAST_ENTRY_QUEUE_DEDUPE_SEC),
    ).fetchone()


def candidate_is_too_stale(receive_ts, now_ts, source_type):
    if not receive_ts:
        return False
    if any(marker in str(source_type or "") for marker in ("rescue", "reclaim", "recovery", "source_resonance")):
        return False
    return (now_ts - int(normalize_ts_sec(receive_ts))) > FAST_ENTRY_RETRY_LATENCY_SEC


def enqueue_fast_entry(db, *, source_type, token_ca, symbol=None, signal_ts=None,
                       receive_ts=None, recorded_ts=None, entry_mode_hint=None,
                       entry_branch=None, hard_gate_status=None,
                       source_resonance_cohort=None, trigger_price=None,
                       trigger_mc=None, priority=50, payload=None, now_ts=None):
    if not token_ca:
        return False
    now_ts = float(now_ts if now_ts is not None else time.time())
    signal_ts = int(normalize_ts_sec(signal_ts or now_ts))
    receive_ts = int(normalize_ts_sec(receive_ts)) if receive_ts else signal_ts
    if candidate_is_too_stale(receive_ts, now_ts, source_type):
        return False
    branch = entry_branch or source_type
    key = queue_key(source_type, token_ca, signal_ts, branch)
    session = market_session_for_ts(receive_ts or signal_ts or now_ts)
    try:
        with SQLITE_WRITE_LOCK:
            if active_queue_depth(db) >= FAST_ENTRY_MAX_QUEUE_DEPTH and int(priority) > FAST_ENTRY_PRESSURE_PRIORITY_CUTOFF:
                return False
            existing = existing_recent_queue_row(db, token_ca, now_ts)
            if existing is not None:
                if int(priority) < int(existing["priority"] or 999):
                    new_status = "queued" if existing["status"] in ("watch_only", "counterfactual_only") else existing["status"]
                    history = status_history_append(
                        existing["status_history_json"],
                        status=new_status,
                        error=None,
                        now_ts=now_ts,
                    )
                    db.execute(
                        """
                        UPDATE paper_fast_entry_queue
                        SET priority = ?, source_type = ?, entry_mode_hint = ?, entry_branch = ?,
                            hard_gate_status = COALESCE(?, hard_gate_status),
                            source_resonance_cohort = COALESCE(?, source_resonance_cohort),
                            payload_json = ?,
                            status = CASE WHEN status IN ('watch_only', 'counterfactual_only') THEN 'queued' ELSE status END,
                            last_error = CASE WHEN status IN ('watch_only', 'counterfactual_only') THEN NULL ELSE last_error END,
                            status_history_json = ?,
                            market_session = COALESCE(market_session, ?),
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            int(priority),
                            source_type,
                            entry_mode_hint,
                            branch,
                            hard_gate_status,
                            source_resonance_cohort,
                            json.dumps(payload or {}, ensure_ascii=False),
                            history,
                            session,
                            now_ts,
                            existing["id"],
                        ),
                    )
                    db.commit()
                return False
            db.execute(
                """
                INSERT OR IGNORE INTO paper_fast_entry_queue (
                    created_at, source_signal_ts, signal_receive_ts, signal_recorded_ts,
                    token_ca, symbol, source_type, entry_mode_hint, entry_branch,
                    hard_gate_status, source_resonance_cohort, trigger_price, trigger_mc,
                    priority, status, decision_deadline_ts, quote_deadline_ts, queue_key,
                    payload_json, status_history_json, market_session, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_ts,
                    signal_ts,
                    int(normalize_ts_sec(receive_ts)) if receive_ts else signal_ts,
                    int(normalize_ts_sec(recorded_ts)) if recorded_ts else None,
                    token_ca,
                    symbol,
                    source_type,
                    entry_mode_hint,
                    branch,
                    hard_gate_status,
                    source_resonance_cohort,
                    trigger_price,
                    trigger_mc,
                    int(priority),
                    now_ts + FAST_ENTRY_RETRY_LATENCY_SEC,
                    now_ts + FAST_ENTRY_QUOTE_TIMEOUT_SEC,
                    key,
                    json.dumps(payload or {}, ensure_ascii=False),
                    status_history_append(None, status="queued", now_ts=now_ts),
                    session,
                    now_ts,
                ),
            )
            inserted = db.execute("SELECT changes() AS c").fetchone()["c"] > 0
            db.commit()
        return inserted
    except sqlite3.OperationalError as exc:
        log.warning("enqueue failed token=%s source=%s: %s", token_ca, source_type, exc)
        return False


def record_fast_lane_observation(db, *, source_type, token_ca, symbol=None, signal_ts=None,
                                 receive_ts=None, recorded_ts=None, entry_mode_hint=None,
                                 entry_branch=None, hard_gate_status=None,
                                 source_resonance_cohort=None, trigger_price=None,
                                 trigger_mc=None, priority=50, payload=None,
                                 status="watch_only", reason=None, now_ts=None):
    """Persist a non-entry fast-lane observation for review without queueing it."""
    if not token_ca:
        return False
    now_ts = float(now_ts if now_ts is not None else time.time())
    signal_ts = int(normalize_ts_sec(signal_ts or now_ts))
    receive_ts = int(normalize_ts_sec(receive_ts)) if receive_ts else signal_ts
    branch = entry_branch or source_type
    key = queue_key(source_type, token_ca, signal_ts, branch)
    session = market_session_for_ts(receive_ts or signal_ts or now_ts)
    payload = {
        **(payload or {}),
        "direct_fill_status": status,
        "direct_fill_reason": reason,
        "shadow_entry": status == "watch_only",
        "counterfactual_entry": status == "counterfactual_only",
    }
    try:
        with SQLITE_WRITE_LOCK:
            db.execute(
                """
                INSERT OR IGNORE INTO paper_fast_entry_queue (
                    created_at, source_signal_ts, signal_receive_ts, signal_recorded_ts,
                    token_ca, symbol, source_type, entry_mode_hint, entry_branch,
                    hard_gate_status, source_resonance_cohort, trigger_price, trigger_mc,
                    priority, status, decision_deadline_ts, quote_deadline_ts, queue_key,
                    payload_json, last_error, first_error, first_error_at,
                    status_history_json, market_session, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_ts,
                    signal_ts,
                    receive_ts,
                    int(normalize_ts_sec(recorded_ts)) if recorded_ts else None,
                    token_ca,
                    symbol,
                    source_type,
                    entry_mode_hint,
                    branch,
                    hard_gate_status,
                    source_resonance_cohort,
                    trigger_price,
                    trigger_mc,
                    int(priority),
                    status,
                    now_ts + FAST_ENTRY_RETRY_LATENCY_SEC,
                    now_ts + FAST_ENTRY_QUOTE_TIMEOUT_SEC,
                    key,
                    json.dumps(payload, ensure_ascii=False),
                    reason,
                    reason,
                    now_ts if reason else None,
                    status_history_append(None, status=status, error=reason, now_ts=now_ts),
                    session,
                    now_ts,
                ),
            )
            inserted = db.execute("SELECT changes() AS c").fetchone()["c"] > 0
            if not inserted:
                row = db.execute(
                    "SELECT first_error, first_error_at, status_history_json FROM paper_fast_entry_queue WHERE queue_key = ?",
                    (key,),
                ).fetchone()
                first_error = (row["first_error"] if row else None) or reason
                first_error_at = (row["first_error_at"] if row else None) or (now_ts if reason else None)
                history = status_history_append(row["status_history_json"] if row else None, status=status, error=reason, now_ts=now_ts)
                db.execute(
                    """
                    UPDATE paper_fast_entry_queue
                    SET status = ?, last_error = ?, first_error = ?, first_error_at = ?,
                        payload_json = ?, status_history_json = ?, market_session = COALESCE(market_session, ?),
                        updated_at = ?
                    WHERE queue_key = ? AND status NOT IN ('entered')
                    """,
                    (status, reason, first_error, first_error_at, json.dumps(payload, ensure_ascii=False), history, session, now_ts, key),
                )
            db.commit()
        if inserted and table_exists(db, "paper_decision_events"):
            try:
                audit_decision = "shadow" if status == "watch_only" else status
                audit_event_type = (
                    "shadow_observation"
                    if status == "watch_only"
                    else "counterfactual_observation"
                    if status == "counterfactual_only"
                    else "non_entry_observation"
                )
                ptm.record_decision_event(
                    db,
                    component="paper_fast_lane",
                    event_type=audit_event_type,
                    decision=audit_decision,
                    reason=reason,
                    token_ca=token_ca,
                    symbol=symbol,
                    lifecycle_id=ptm.build_lifecycle_id(token_ca, signal_ts),
                    signal_ts=signal_ts,
                    route=source_type,
                    data_source="paper_fast_entry_queue",
                    payload={
                        "source_type": source_type,
                        "entry_branch": branch,
                        "entry_mode_hint": entry_mode_hint,
                        "source_resonance_cohort": source_resonance_cohort,
                        "status": status,
                        "reason": reason,
                        **payload,
                    },
                    event_ts=now_ts,
                )
            except Exception:
                log.debug("fast lane observation audit failed token=%s source=%s", token_ca, source_type, exc_info=True)
        return inserted
    except sqlite3.OperationalError as exc:
        log.warning("observation failed token=%s source=%s: %s", token_ca, source_type, exc)
        return False


def status_is_hard_reject(status):
    status = str(status or "").upper()
    if status in FAST_LANE_HARD_REJECT_STATUSES:
        return True
    return "RUG" in status or "HONEYPOT" in status


def row_value(row, key, default=None):
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        if isinstance(row, dict):
            return row.get(key, default)
        return default


def source_to_mode_and_stage(row):
    source_type = str(row_value(row, "source_type", "") or "")
    branch = str(row_value(row, "entry_branch", "") or source_type)
    mode_hint = row_value(row, "entry_mode_hint")
    if "hard_gate" in source_type or "hard_gate" in branch:
        return "hard_gate_pass_tiny_probe", "stage1"
    if "source_resonance" in source_type or "source_resonance" in branch:
        return "source_resonance_tiny_probe", "lotto"
    if mode_hint:
        return str(mode_hint), "lotto"
    if (
        "ttl" in source_type
        or "kline" in source_type
        or "spread" in source_type
        or "missing_quote" in source_type
        or "reclaim" in source_type
    ):
        return "pre_pass_resonance_tiny_probe", "lotto"
    return "pre_pass_resonance_tiny_probe", "lotto"


def row_payload(row):
    try:
        raw = row_value(row, "payload_json")
        return json.loads(raw or "{}") if raw else {}
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def source_activity_confirmed(payload):
    return bool(
        int(payload.get("gmgn_momentum_confirmed") or 0)
        or int(payload.get("gmgn_volume_confirmed") or 0)
        or int(payload.get("two_quote_clean_snapshots") or 0)
        or int(payload.get("resonance_level") or 0) >= 3
    )


def _payload_bool(payload, key):
    value = (payload or {}).get(key)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _payload_int(payload, key, default=0):
    try:
        return int(float((payload or {}).get(key) or default))
    except (TypeError, ValueError):
        return int(default)


def _payload_float(payload, key, default=0.0):
    try:
        return float((payload or {}).get(key) or default)
    except (TypeError, ValueError):
        return float(default)


def source_execution_eligibility_detail(payload, *, entry_mode="source_resonance_tiny_probe", route=None, now_ts=None, require_quote_clean=True):
    payload = payload or {}
    signal_ts = payload.get("original_signal_ts") or payload.get("original_receive_ts")
    liquidity_usd = _payload_float(
        payload,
        "liquidity_usd",
        _payload_float(payload, "gmgn_last_liquidity", 0.0),
    )
    quote_clean_seen = _payload_bool(payload, "quote_clean_seen") or _payload_bool(payload, "two_quote_clean_snapshots")
    eligibility = build_entry_execution_eligibility(
        entry_mode=entry_mode,
        route=route or payload.get("signal_type"),
        signal_ts=signal_ts,
        now_ts=now_ts,
        observed={
            "quote_clean_seen": quote_clean_seen,
            "quote_executable": quote_clean_seen,
            "liquidity_usd": liquidity_usd,
        },
        min_liquidity_usd=FAST_ENTRY_SOURCE_MIN_LIQUIDITY_USD,
        require_quote_clean=require_quote_clean,
        require_timing=False,
        timing_confirmed=True,
        quote_clean_seen=quote_clean_seen,
        quote_executable=quote_clean_seen,
    ).to_dict()
    return eligibility


def source_gmgn_momentum_canary_detail(payload, *, now_ts=None):
    payload = payload or {}
    if not FAST_ENTRY_SOURCE_GMGN_MOMENTUM_CANARY_ENABLED:
        return {"pass": False, "reason": "source_gmgn_momentum_canary_disabled"}
    session = str(payload.get("market_session") or market_session_for_ts(now_ts or time.time()))
    if session == "quiet" and not FAST_ENTRY_SOURCE_GMGN_CANARY_QUIET_ENABLED:
        return {"pass": False, "reason": "source_gmgn_momentum_canary_quiet_session", "market_session": session}
    resonance_level = _payload_int(payload, "resonance_level")
    if resonance_level < FAST_ENTRY_SOURCE_GMGN_CANARY_MIN_RESONANCE_LEVEL:
        return {
            "pass": False,
            "reason": "source_gmgn_momentum_canary_resonance_low",
            "resonance_level": resonance_level,
            "min_resonance_level": FAST_ENTRY_SOURCE_GMGN_CANARY_MIN_RESONANCE_LEVEL,
        }
    if not (_payload_bool(payload, "gmgn_momentum_confirmed") or _payload_bool(payload, "gmgn_volume_confirmed")):
        return {"pass": False, "reason": "source_gmgn_momentum_canary_unconfirmed"}
    if payload.get("source_updated_at") and not updated_at_is_fresh(payload.get("source_updated_at"), now_ts=now_ts):
        return {"pass": False, "reason": "source_gmgn_momentum_canary_stale_update"}
    eligibility = source_execution_eligibility_detail(payload, now_ts=now_ts, require_quote_clean=True)
    if not eligibility.get("direct_entry_ok"):
        return {
            "pass": False,
            "reason": eligibility.get("reason") or "entry_execution_not_eligible",
            "entry_execution_eligibility": eligibility,
        }
    return {
        "pass": True,
        "reason": "source_gmgn_momentum_canary",
        "market_session": session,
        "resonance_level": resonance_level,
        "entry_execution_eligibility": eligibility,
    }


def source_quote_clean_refresh_detail(payload, *, now_ts=None):
    payload = payload or {}
    if not FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_ENABLED:
        return {"pass": False, "reason": "source_quote_clean_refresh_disabled"}
    session = str(payload.get("market_session") or market_session_for_ts(now_ts or time.time()))
    if session == "quiet" and not FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_QUIET_ENABLED:
        return {"pass": False, "reason": "source_quote_clean_refresh_quiet_session", "market_session": session}
    original_age_sec = payload_ts_age_sec(
        payload,
        ("original_signal_ts", "original_receive_ts"),
        now_ts=now_ts,
    )
    if original_age_sec is None:
        return {"pass": False, "reason": "source_quote_clean_refresh_original_signal_missing"}
    if original_age_sec > FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_MAX_ORIGINAL_AGE_SEC:
        return {
            "pass": False,
            "reason": "source_quote_clean_refresh_original_signal_stale",
            "original_age_sec": original_age_sec,
            "max_original_age_sec": FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_MAX_ORIGINAL_AGE_SEC,
        }
    if not _payload_bool(payload, "quote_clean_seen"):
        return {"pass": False, "reason": "source_quote_clean_refresh_missing_quote_clean"}
    if (
        FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_REQUIRE_TWO_SNAPSHOTS
        and not _payload_bool(payload, "two_quote_clean_snapshots")
    ):
        return {"pass": False, "reason": "source_quote_clean_refresh_needs_two_quote_snapshots"}
    if payload.get("source_updated_at") and not updated_at_is_fresh(
        payload.get("source_updated_at"),
        now_ts=now_ts,
        max_age_sec=FAST_ENTRY_SOURCE_QUOTE_CLEAN_REFRESH_MAX_UPDATE_AGE_SEC,
    ):
        return {"pass": False, "reason": "source_quote_clean_refresh_stale_update"}
    source_activity_fresh = bool(
        _payload_bool(payload, "activity_confirmed")
        or _payload_bool(payload, "gmgn_momentum_confirmed")
        or _payload_bool(payload, "gmgn_volume_confirmed")
    )
    if not source_activity_fresh:
        return {"pass": False, "reason": "source_quote_clean_refresh_activity_not_confirmed"}
    eligibility = source_execution_eligibility_detail(payload, now_ts=now_ts, require_quote_clean=True)
    if not eligibility.get("direct_entry_ok"):
        return {
            "pass": False,
            "reason": eligibility.get("reason") or "entry_execution_not_eligible",
            "entry_execution_eligibility": eligibility,
        }
    return {
        "pass": True,
        "reason": "source_quote_clean_refresh_tiny_probe",
        "market_session": session,
        "original_age_sec": original_age_sec,
        "entry_execution_eligibility": eligibility,
    }


def updated_at_is_fresh(value, *, now_ts=None, max_age_sec=None):
    ts = parse_datetime_ts(value)
    if ts is None:
        return False
    now_ts = float(now_ts if now_ts is not None else time.time())
    max_age_sec = FAST_ENTRY_SOURCE_QUOTE_CLEAN_MAX_UPDATE_AGE_SEC if max_age_sec is None else max_age_sec
    return (now_ts - ts) <= float(max_age_sec)


def payload_ts_age_sec(payload, keys, *, now_ts=None):
    now_ts = float(now_ts if now_ts is not None else time.time())
    for key in keys:
        raw = payload.get(key)
        if raw is None:
            continue
        if isinstance(raw, str) and not raw.strip():
            continue
        ts = parse_datetime_ts(raw)
        if ts:
            return max(0.0, now_ts - ts)
    return None


def recovery_tradable_fresh_detail(payload, *, now_ts=None, max_age_sec=None):
    payload = payload or {}
    max_age_sec = FAST_ENTRY_RECOVERY_MAX_TRADABLE_AGE_SEC if max_age_sec is None else max_age_sec
    if not (_payload_bool(payload, "tradable_missed") or _payload_bool(payload, "recovery_quote_clean") or _payload_bool(payload, "final_reclaim_quote_executable")):
        return {"pass": False, "reason": "recovery_quote_clean_missing"}
    age_sec = payload_ts_age_sec(
        payload,
        (
            "last_tradable_ts",
            "last_clean_quote_ts",
            "missed_updated_at",
            "updated_at",
            "first_tradable_ts",
            "recovery_created_ts",
            "rescue_created_ts",
        ),
        now_ts=now_ts,
    )
    if age_sec is None:
        return {"pass": False, "reason": "recovery_tradable_timestamp_missing"}
    if age_sec > float(max_age_sec):
        return {
            "pass": False,
            "reason": "recovery_tradable_signal_stale_watch_only",
            "tradable_age_sec": age_sec,
            "max_tradable_age_sec": float(max_age_sec),
        }
    return {
        "pass": True,
        "reason": "recovery_quote_clean_fresh",
        "tradable_age_sec": age_sec,
        "max_tradable_age_sec": float(max_age_sec),
    }


def recovery_strong_signal_confirmed(payload):
    payload = payload or {}
    return bool(
        _payload_bool(payload, "strong_signal_seen")
        or _payload_bool(payload, "hard_gate_reconfirmed")
        or _payload_bool(payload, "source_momentum_confirmed")
        or _payload_bool(payload, "activity_confirmed")
        or _payload_bool(payload, "gmgn_momentum_confirmed")
        or _payload_bool(payload, "gmgn_volume_confirmed")
        or _payload_bool(payload, "two_quote_clean_snapshots")
        or _payload_int(payload, "resonance_level") >= 3
    )


def clean_dog_reclaim_eligibility_detail(branch, payload, *, now_ts=None):
    eligibility = build_clean_dog_reclaim_eligibility(
        payload,
        entry_branch=branch,
        route=(payload or {}).get("route"),
        now_ts=now_ts,
        max_tradable_age_sec=FAST_ENTRY_NOT_ATH_RECLAIM_MAX_TRADABLE_AGE_SEC,
        route_allowed=branch in CLEAN_DOG_RECLAIM_BRANCHES,
        canary_budget_ok=FAST_ENTRY_NOT_ATH_RECLAIM_CANARY_ENABLED,
    )
    data = eligibility.to_dict()
    data["tradable_age_sec"] = data.get("last_tradable_age_sec")
    return data


def kline_recovery_detail(payload, *, now_ts=None):
    detail = recovery_tradable_fresh_detail(payload, now_ts=now_ts)
    if not detail.get("pass"):
        return detail
    if not recovery_strong_signal_confirmed(payload):
        return {
            **detail,
            "pass": False,
            "reason": "recovery_strong_signal_missing",
        }
    return {
        **detail,
        "reason": "kline_recovery_fresh_quote_and_strong_signal",
    }


def branch_circuit_detail(db, branch, *, market_session=None, now_ts=None):
    if not FAST_ENTRY_BRANCH_CIRCUIT_ENABLED:
        return {"pass": True, "reason": "branch_circuit_disabled"}
    if not branch or not table_exists(db, "paper_trades"):
        return {"pass": True, "reason": "branch_circuit_no_history"}
    cols = table_columns(db, "paper_trades")
    if "entry_branch" not in cols or "pnl_pct" not in cols:
        return {"pass": True, "reason": "branch_circuit_schema_missing"}
    ts_col = next((name for name in ("entry_ts", "signal_ts", "exit_ts") if name in cols), None)
    filter_ts_expr = ts_col or "0"
    session_ts_expr = ts_col or "0"
    now_ts = float(now_ts if now_ts is not None else time.time())
    since_ts = now_ts - FAST_ENTRY_BRANCH_CIRCUIT_LOOKBACK_SEC
    session = str(market_session or "").strip().lower()
    session_clause = ""
    params = [branch, since_ts]
    if session in {"asia", "europe", "us", "quiet"} and session_ts_expr != "0":
        session_clause = f"""
          AND CASE
            WHEN CAST(strftime('%H', datetime({session_ts_expr}, 'unixepoch')) AS INTEGER) BETWEEN 0 AND 7 THEN 'asia'
            WHEN CAST(strftime('%H', datetime({session_ts_expr}, 'unixepoch')) AS INTEGER) BETWEEN 8 AND 13 THEN 'europe'
            WHEN CAST(strftime('%H', datetime({session_ts_expr}, 'unixepoch')) AS INTEGER) BETWEEN 14 AND 21 THEN 'us'
            ELSE 'quiet'
          END = ?
        """
        params.append(session)
    peak_expr = "COALESCE(peak_pnl, 0)"
    if "trusted_peak_pnl" in cols and "quote_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(trusted_peak_pnl, 0), NULLIF(quote_peak_pnl, 0), 0)"
    elif "trusted_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(trusted_peak_pnl, 0), 0)"
    elif "quote_peak_pnl" in cols:
        peak_expr = "COALESCE(NULLIF(quote_peak_pnl, 0), 0)"
    rows = db.execute(
        f"""
        SELECT COALESCE(pnl_pct, 0) AS pnl_pct,
               {peak_expr} AS trusted_peak_pnl
        FROM paper_trades
        WHERE entry_branch = ?
          AND pnl_pct IS NOT NULL
          AND {filter_ts_expr} >= ?
          {session_clause}
        ORDER BY {filter_ts_expr} DESC
        """,
        tuple(params),
    ).fetchall()
    pnls = sorted(float(row["pnl_pct"] or 0.0) for row in rows)
    closed_n = len(pnls)
    avg_pnl = (sum(pnls) / closed_n) if closed_n else 0.0
    wins = sum(1 for pnl in pnls if pnl > 0)
    dog_capture_n = sum(1 for row in rows if float(row["trusted_peak_pnl"] or 0.0) >= 0.25)

    def percentile(values, pct):
        if not values:
            return None
        if len(values) == 1:
            return values[0]
        pos = (len(values) - 1) * pct
        lo = int(pos)
        hi = min(lo + 1, len(values) - 1)
        frac = pos - lo
        return values[lo] * (1 - frac) + values[hi] * frac

    detail = {
        "pass": True,
        "reason": "branch_circuit_pass",
        "entry_branch": branch,
        "market_session": session or "all",
        "closed_n": closed_n,
        "wins": wins,
        "win_rate": (wins / closed_n) if closed_n else None,
        "avg_pnl": avg_pnl,
        "p10_pnl": percentile(pnls, 0.10),
        "p90_pnl": percentile(pnls, 0.90),
        "max_loss": min(pnls) if pnls else None,
        "trusted_dog_capture_n": dog_capture_n,
        "lookback_sec": FAST_ENTRY_BRANCH_CIRCUIT_LOOKBACK_SEC,
        "min_closed": FAST_ENTRY_BRANCH_CIRCUIT_MIN_CLOSED,
        "avg_pnl_floor": FAST_ENTRY_BRANCH_CIRCUIT_AVG_PNL_FLOOR,
        "p10_pnl_floor": FAST_ENTRY_BRANCH_CIRCUIT_P10_PNL_FLOOR,
        "max_loss_floor": FAST_ENTRY_BRANCH_CIRCUIT_MAX_LOSS_FLOOR,
        "auto_action": "allow_or_keep_watch",
    }
    if closed_n >= FAST_ENTRY_BRANCH_CIRCUIT_MIN_CLOSED and avg_pnl < FAST_ENTRY_BRANCH_CIRCUIT_AVG_PNL_FLOOR:
        detail["pass"] = False
        detail["reason"] = "branch_circuit_negative_ev"
        detail["auto_action"] = "downgrade_to_watch_only"
    elif closed_n >= FAST_ENTRY_BRANCH_CIRCUIT_MIN_CLOSED and detail["p10_pnl"] is not None and detail["p10_pnl"] < FAST_ENTRY_BRANCH_CIRCUIT_P10_PNL_FLOOR:
        detail["pass"] = False
        detail["reason"] = "branch_circuit_tail_loss"
        detail["auto_action"] = "downgrade_to_watch_only"
    elif closed_n > 0 and detail["max_loss"] is not None and detail["max_loss"] < FAST_ENTRY_BRANCH_CIRCUIT_MAX_LOSS_FLOOR:
        detail["pass"] = False
        detail["reason"] = "branch_circuit_catastrophic_loss"
        detail["auto_action"] = "downgrade_to_watch_only"
    return detail


def direct_fill_policy(row, *, now_ts=None):
    branch = str(row_value(row, "entry_branch", "") or "")
    source_type = str(row_value(row, "source_type", "") or "")
    payload = row_payload(row)
    if "hard_gate" in source_type or "hard_gate" in branch:
        if not FAST_ENTRY_HARD_GATE_DIRECT_ENABLED:
            return {
                "pass": False,
                "status": "counterfactual_only",
                "reason": "hard_gate_fast_direct_entry_disabled_counterfactual_only",
                "detail": {
                    "entry_branch": branch,
                    "source_type": source_type,
                    "auto_action": "downgrade_to_counterfactual_only",
                },
            }
    if branch == "source_gmgn_momentum_canary":
        detail = source_gmgn_momentum_canary_detail(payload, now_ts=now_ts)
        if not detail.get("pass"):
            return {"pass": False, "status": "watch_only", "reason": detail.get("reason"), "detail": detail}
        return {"pass": True, "status": "queued", "reason": detail.get("reason"), "detail": detail}
    if branch == "source_quote_clean_refresh_tiny_probe":
        detail = source_quote_clean_refresh_detail(payload, now_ts=now_ts)
        if not detail.get("pass"):
            return {"pass": False, "status": "watch_only", "reason": detail.get("reason"), "detail": detail}
        return {"pass": True, "status": "queued", "reason": detail.get("reason"), "detail": detail}
    if branch == "ttl_final_reclaim_quote_clean":
        detail = recovery_tradable_fresh_detail(
            payload,
            now_ts=now_ts,
            max_age_sec=FAST_ENTRY_TTL_RESCUE_MAX_TRADABLE_AGE_SEC,
        )
        if not detail.get("pass"):
            return {"pass": False, "status": "watch_only", "reason": f"ttl_final_reclaim_{detail.get('reason')}", "detail": detail}
        return {"pass": True, "status": "queued", "reason": "ttl_final_reclaim_quote_clean", "detail": detail}
    if branch == "kline_recovery_quote_clean_tiny_probe":
        if not FAST_ENTRY_KLINE_RECOVERY_CANARY_ENABLED:
            return {"pass": False, "status": "counterfactual_only", "reason": "kline_recovery_canary_disabled"}
        detail = kline_recovery_detail(payload, now_ts=now_ts)
        if not detail.get("pass"):
            reason = detail.get("reason") or "recovery_not_confirmed"
            if reason.startswith("recovery_"):
                reason = reason[len("recovery_"):]
            return {"pass": False, "status": "counterfactual_only", "reason": f"kline_recovery_{reason}", "detail": detail}
        return {"pass": True, "status": "queued", "reason": "kline_recovery_quote_clean_tiny_probe", "detail": detail}
    if branch in CLEAN_DOG_RECLAIM_BRANCHES:
        detail = clean_dog_reclaim_eligibility_detail(branch, payload, now_ts=now_ts)
        if not detail.get("direct_reclaim_ok"):
            return {"pass": False, "status": "watch_only", "reason": detail.get("reason"), "detail": detail}
        return {"pass": True, "status": "queued", "reason": branch, "detail": detail}
    if branch == "smart_quality_reclaim_tiny_probe":
        if not FAST_ENTRY_SMART_QUALITY_RECHECK_CANARY_ENABLED:
            return {"pass": False, "status": "watch_only", "reason": "smart_quality_recheck_canary_disabled"}
        detail = recovery_tradable_fresh_detail(payload, now_ts=now_ts)
        if not detail.get("pass"):
            return {"pass": False, "status": "watch_only", "reason": f"smart_quality_reclaim_{detail.get('reason')}", "detail": detail}
        return {"pass": True, "status": "queued", "reason": "smart_quality_reclaim_tiny_probe", "detail": detail}
    if branch == "matrix_timeout_final_quote_tiny_probe":
        if not FAST_ENTRY_MATRIX_TIMEOUT_CANARY_ENABLED:
            return {"pass": False, "status": "watch_only", "reason": "matrix_timeout_canary_disabled"}
        detail = recovery_tradable_fresh_detail(payload, now_ts=now_ts)
        if not detail.get("pass"):
            return {"pass": False, "status": "watch_only", "reason": f"matrix_timeout_reclaim_{detail.get('reason')}", "detail": detail}
        return {"pass": True, "status": "queued", "reason": "matrix_timeout_final_quote_tiny_probe", "detail": detail}
    if branch == "source_resonance_gmgn_fast" and not FAST_ENTRY_SOURCE_GMGN_ONLY_DIRECT_ENABLED:
        return {
            "pass": False,
            "status": "watch_only",
            "reason": "source_resonance_gmgn_only_watch_only",
            "detail": {
                "entry_branch": branch,
                "source_type": source_type,
                "shadow_entry": True,
                "counterfactual_entry": False,
                "auto_action": "keep_shadow_until_quote_clean_and_timing",
                "required_next_state": "telegram_gmgn_quote_clean",
            },
        }
    if branch == "source_resonance_quote_clean_fast":
        original_age_sec = payload_ts_age_sec(
            payload,
            ("original_signal_ts", "original_receive_ts"),
            now_ts=now_ts,
        )
        if original_age_sec is None or original_age_sec > FAST_ENTRY_SOURCE_QUOTE_CLEAN_MAX_ORIGINAL_AGE_SEC:
            return {
                "pass": False,
                "status": "watch_only",
                "reason": "source_quote_clean_original_signal_stale_watch_only",
            }
        source_updated_at = payload.get("source_updated_at")
        if source_updated_at and not updated_at_is_fresh(source_updated_at, now_ts=now_ts):
            return {
                "pass": False,
                "status": "watch_only",
                "reason": "source_quote_clean_stale_update_watch_only",
            }
        if FAST_ENTRY_SOURCE_QUOTE_CLEAN_ACTIVITY_REQUIRED and not source_activity_confirmed(payload):
            return {
                "pass": False,
                "status": "watch_only",
                "reason": "source_quote_clean_activity_not_confirmed",
            }
        eligibility = source_execution_eligibility_detail(payload, now_ts=now_ts, require_quote_clean=True)
        if not eligibility.get("direct_entry_ok"):
            return {
                "pass": False,
                "status": "watch_only",
                "reason": eligibility.get("reason") or "entry_execution_not_eligible",
                "detail": eligibility,
            }
    if branch == "tracking_ttl_expired":
        tradable_age_sec = payload_ts_age_sec(
            payload,
            ("first_tradable_ts", "rescue_created_ts"),
            now_ts=now_ts,
        )
        if tradable_age_sec is None or tradable_age_sec > FAST_ENTRY_TTL_RESCUE_MAX_TRADABLE_AGE_SEC:
            return {
                "pass": False,
                "status": "watch_only",
                "reason": "ttl_rescue_tradable_signal_stale_watch_only",
            }
    if (
        branch in KLINE_RESCUE_BRANCHES
        or (
            "kline" in source_type
            and "hard_gate" not in source_type
            and not FAST_ENTRY_KLINE_RESCUE_DIRECT_ENABLED
        )
    ):
        return {
            "pass": False,
            "status": "counterfactual_only",
            "reason": "kline_rescue_direct_fill_disabled",
        }
    return {"pass": True, "status": "queued", "reason": "direct_fill_allowed"}


def open_or_recent_trade_exists(db, token_ca, now_ts=None):
    now_ts = float(now_ts if now_ts is not None else time.time())
    row = db.execute(
        """
        SELECT id, exit_ts, entry_ts
        FROM paper_trades
        WHERE token_ca = ?
          AND (
            exit_reason IS NULL
            OR entry_ts >= ?
          )
        ORDER BY id DESC
        LIMIT 1
        """,
        (token_ca, int(now_ts - FAST_ENTRY_PER_TOKEN_COOLDOWN_SEC)),
    ).fetchone()
    return row is not None


def acquire_token_lock(db, token_ca, lifecycle_id, owner, now_ts=None):
    now_ts = float(now_ts if now_ts is not None else time.time())
    with SQLITE_WRITE_LOCK:
        db.execute("DELETE FROM paper_entry_locks WHERE expires_at <= ?", (now_ts,))
        try:
            db.execute(
                """
                INSERT INTO paper_entry_locks(token_ca, lifecycle_id, lock_reason, owner, expires_at, updated_at)
                VALUES (?, ?, 'fast_entry_attempt', ?, ?, ?)
                """,
                (token_ca, lifecycle_id, owner, now_ts + FAST_ENTRY_CLAIM_TTL_SEC, now_ts),
            )
            db.commit()
            return True
        except sqlite3.IntegrityError:
            return False


def release_token_lock(db, token_ca, owner=None):
    with SQLITE_WRITE_LOCK:
        if owner:
            db.execute("DELETE FROM paper_entry_locks WHERE token_ca = ? AND owner = ?", (token_ca, owner))
        else:
            db.execute("DELETE FROM paper_entry_locks WHERE token_ca = ?", (token_ca,))
        db.commit()


def claim_queue_item(db, owner):
    now_ts = time.time()
    stale_claim_before = now_ts - FAST_ENTRY_CLAIM_TTL_SEC
    with SQLITE_WRITE_LOCK:
        db.execute(
            """
            UPDATE paper_fast_entry_queue
            SET status = 'expired',
                last_error = 'fast_lane_queue_age_expired',
                first_error = COALESCE(first_error, 'fast_lane_queue_age_expired'),
                first_error_at = COALESCE(first_error_at, ?),
                claimed_by = NULL,
                claimed_at = NULL,
                updated_at = ?
            WHERE status IN ('queued', 'claimed')
              AND (? - created_at) > ?
            """,
            (now_ts, now_ts, now_ts, FAST_ENTRY_MAX_QUEUE_AGE_SEC),
        )
        db.execute(
            """
            UPDATE paper_fast_entry_queue
            SET status = 'queued', claimed_by = NULL, claimed_at = NULL, updated_at = ?
            WHERE status = 'claimed' AND COALESCE(claimed_at, 0) < ?
            """,
            (now_ts, stale_claim_before),
        )
        row = db.execute(
            """
            SELECT *
            FROM paper_fast_entry_queue
            WHERE status = 'queued'
            ORDER BY priority ASC, created_at ASC
            LIMIT 1
            """
        ).fetchone()
        if row is None:
            db.commit()
            return None
        db.execute(
            """
            UPDATE paper_fast_entry_queue
            SET status = 'claimed', claimed_by = ?, claimed_at = ?, attempt_count = attempt_count + 1, updated_at = ?
            WHERE id = ? AND status = 'queued'
            """,
            (owner, now_ts, now_ts, row["id"]),
        )
        claimed = db.execute("SELECT changes() AS c").fetchone()["c"] == 1
        db.commit()
    if not claimed:
        return None
    return db.execute("SELECT * FROM paper_fast_entry_queue WHERE id = ?", (row["id"],)).fetchone()


def mark_queue(db, row_id, status, error=None):
    with SQLITE_WRITE_LOCK:
        now_ts = time.time()
        row = db.execute(
            "SELECT first_error, first_error_at, status_history_json FROM paper_fast_entry_queue WHERE id = ?",
            (row_id,),
        ).fetchone()
        first_error = (row["first_error"] if row else None) or error
        first_error_at = (row["first_error_at"] if row else None) or (now_ts if error else None)
        history = status_history_append(row["status_history_json"] if row else None, status=status, error=error, now_ts=now_ts)
        db.execute(
            """
            UPDATE paper_fast_entry_queue
            SET status = ?, last_error = ?, first_error = ?, first_error_at = ?,
                status_history_json = ?, claimed_by = NULL, claimed_at = NULL, updated_at = ?
            WHERE id = ?
            """,
            (status, error, first_error, first_error_at, history, now_ts, row_id),
        )
        db.commit()


def refresh_retry_watch(db, *, now_ts=None):
    now_ts = float(now_ts if now_ts is not None else time.time())
    with SQLITE_WRITE_LOCK:
        db.execute(
            """
            UPDATE paper_fast_entry_queue
            SET status = 'expired',
                last_error = 'fast_lane_queue_age_expired',
                first_error = COALESCE(first_error, 'fast_lane_queue_age_expired'),
                first_error_at = COALESCE(first_error_at, ?),
                claimed_by = NULL,
                claimed_at = NULL,
                updated_at = ?
            WHERE status IN ('queued', 'claimed')
              AND (? - created_at) > ?
            """,
            (now_ts, now_ts, now_ts, FAST_ENTRY_MAX_QUEUE_AGE_SEC),
        )
        db.execute(
            """
            UPDATE paper_fast_entry_queue
            SET status = 'expired',
                last_error = 'fast_lane_retry_watch_expired',
                first_error = COALESCE(first_error, last_error, 'fast_lane_retry_watch_expired'),
                first_error_at = COALESCE(first_error_at, ?),
                updated_at = ?
            WHERE status = 'retry_watch'
              AND (? - created_at) > ?
            """,
            (now_ts, now_ts, now_ts, FAST_ENTRY_MAX_QUEUE_AGE_SEC),
        )
        db.execute(
            """
            UPDATE paper_fast_entry_queue
            SET status = 'queued', updated_at = ?
            WHERE status = 'retry_watch'
              AND (? - updated_at) >= 10
              AND (? - created_at) <= ?
            """,
            (now_ts, now_ts, now_ts, FAST_ENTRY_MAX_QUEUE_AGE_SEC),
        )
        db.commit()


def global_rate_limit_allows(db, now_ts=None):
    now_ts = float(now_ts if now_ts is not None else time.time())
    row = db.execute(
        """
        SELECT COUNT(*) AS c
        FROM paper_trades
        WHERE replay_source = 'paper_fast_lane'
          AND entry_ts >= ?
        """,
        (int(now_ts - 60),),
    ).fetchone()
    return int(row["c"] or 0) < FAST_ENTRY_GLOBAL_MAX_PER_MIN


def open_position_cap_allows(db):
    if FAST_ENTRY_MAX_OPEN_POSITIONS <= 0:
        return True
    if not table_exists(db, "paper_trades"):
        return True
    row = db.execute(
        """
        SELECT COUNT(*) AS c
        FROM paper_trades
        WHERE replay_source = 'paper_fast_lane'
          AND exit_reason IS NULL
        """
    ).fetchone()
    return int(row["c"] or 0) < FAST_ENTRY_MAX_OPEN_POSITIONS


def entry_guard_detail(row, quote_price, *, quote_request_ts_ms, quote_response_ts_ms):
    now_ts = time.time()
    source_type = str(row_value(row, "source_type", "") or "")
    branch = str(row_value(row, "entry_branch", "") or "")
    quote_anchored_reclaim = any(
        marker in source_type or marker in branch
        for marker in ("source_resonance", "rescue", "reclaim", "recovery", "missing_quote", "stale_refresh")
    )
    source_signal_ts = normalize_ts_sec(row_value(row, "source_signal_ts") or row_value(row, "created_at"))
    receive_ts = normalize_ts_sec(row_value(row, "signal_receive_ts") or source_signal_ts)
    created_ts = normalize_ts_sec(row_value(row, "created_at") or now_ts)
    original_signal_to_quote_ms = int(max(0, quote_request_ts_ms - receive_ts * 1000))
    fast_lane_sla_ms = int(max(0, quote_request_ts_ms - created_ts * 1000))
    guard_latency_ms = fast_lane_sla_ms if quote_anchored_reclaim else original_signal_to_quote_ms
    trigger_price = row_value(row, "trigger_price")
    drift_pct = 0.0
    if trigger_price and trigger_price > 0 and quote_price and quote_price > 0:
        drift_pct = ((quote_price - float(trigger_price)) / float(trigger_price)) * 100.0
    detail = {
        "pass": True,
        "reason": "fast_lane_quote_guard_pass",
        "signal_to_quote_latency_ms": guard_latency_ms,
        "original_signal_to_quote_latency_ms": original_signal_to_quote_ms,
        "fast_lane_sla_latency_ms": fast_lane_sla_ms,
        "signal_age_sec": max(0.0, now_ts - receive_ts),
        "original_signal_age_sec": max(0.0, now_ts - receive_ts),
        "fast_lane_queue_age_sec": max(0.0, now_ts - created_ts),
        "quote_drift_pct": drift_pct,
        "position_size_sol": FAST_ENTRY_SIZE_SOL,
    }
    if status_is_hard_reject(row_value(row, "hard_gate_status")):
        detail.update({"pass": False, "reason": "fast_lane_hard_reject_status"})
    elif detail["signal_age_sec"] > FAST_ENTRY_HARD_STALE_SEC and not quote_anchored_reclaim:
        detail.update({"pass": False, "reason": "fast_lane_signal_hard_stale"})
    elif abs(drift_pct) > FAST_ENTRY_HARD_DRIFT_PCT:
        detail.update({"pass": False, "reason": "fast_lane_quote_drift_hard_reject"})
    elif abs(drift_pct) > FAST_ENTRY_MAX_DRIFT_PCT:
        detail.update({"pass": False, "reason": "fast_lane_quote_drift_retry_watch"})
    elif guard_latency_ms > int(FAST_ENTRY_RETRY_LATENCY_SEC * 1000) and not quote_anchored_reclaim:
        detail.update({"pass": False, "reason": "fast_lane_latency_retry_watch"})
    elif abs(drift_pct) > FAST_ENTRY_DEGRADE_DRIFT_PCT or guard_latency_ms > int(FAST_ENTRY_DEGRADE_LATENCY_SEC * 1000):
        detail["reason"] = "fast_lane_degraded_quote_anchored"
        detail["position_size_sol"] = FAST_ENTRY_DEGRADED_SIZE_SOL
    if quote_anchored_reclaim and detail.get("pass") and detail["signal_age_sec"] > FAST_ENTRY_HARD_STALE_SEC:
        detail["reason"] = "fast_lane_reclaim_quote_anchored_stale_signal"
        detail["position_size_sol"] = FAST_ENTRY_DEGRADED_SIZE_SOL
    if detail.get("pass") and branch in DEGRADED_CANARY_BRANCHES:
        detail["position_size_sol"] = min(
            float(detail.get("position_size_sol") or FAST_ENTRY_SIZE_SOL),
            FAST_ENTRY_DEGRADED_SIZE_SOL,
        )
        detail["canary_branch"] = branch
    return detail


def insert_fast_paper_trade(db, row, execution, guard, *, quote_request_ts_ms, quote_response_ts_ms):
    token_ca = row["token_ca"]
    symbol = row["symbol"] or "UNKNOWN"
    signal_ts = int(normalize_ts_sec(row["source_signal_ts"] or row["created_at"]))
    receive_ts = int(normalize_ts_sec(row["signal_receive_ts"] or signal_ts))
    lifecycle_id = ptm.build_lifecycle_id(token_ca, signal_ts)
    entry_mode, strategy_stage = source_to_mode_and_stage(row)
    entry_branch = row["entry_branch"] or row["source_type"]
    size_sol = float(guard.get("position_size_sol") or FAST_ENTRY_SIZE_SOL)
    quote_price = float(execution.get("effectivePrice") or 0)
    token_amount_raw = execution.get("quotedOutAmountRaw")
    token_decimals = execution.get("outputDecimals") or 0
    entry_ts = int((execution.get("quoteTs") or quote_response_ts_ms) / 1000)
    trigger_price = row["trigger_price"] if row["trigger_price"] else quote_price
    latency_audit = ptm.build_entry_execution_latency_audit(
        {
            "signal_ts": signal_ts,
            "source_message_ts": row["source_signal_ts"],
            "receive_ts": row["signal_receive_ts"],
            "created_at": row["signal_recorded_ts"],
            "entry_mode": entry_mode,
        },
        decision_start_ts_ms=int(row["claimed_at"] * 1000) if row["claimed_at"] else quote_request_ts_ms,
        quote_request_ts_ms=quote_request_ts_ms,
        quote_response_ts_ms=quote_response_ts_ms,
        entry_executed_ts_ms=entry_ts * 1000,
        signal_price=trigger_price,
        decision_price=trigger_price,
        quote_price=quote_price,
        entry_fill_price=quote_price,
        quote_spread_pct=guard.get("quote_drift_pct"),
    )
    original_signal_to_quote_ms = (
        guard.get("original_signal_to_quote_latency_ms")
        if guard.get("original_signal_to_quote_latency_ms") is not None
        else latency_audit.get("signal_to_quote_latency_ms")
    )
    fast_lane_receive_to_quote_ms = guard.get("fast_lane_sla_latency_ms")
    if fast_lane_receive_to_quote_ms is None:
        fast_lane_receive_to_quote_ms = guard.get("signal_to_quote_latency_ms")
    fast_lane_queue_to_quote_ms = int(max(0, quote_request_ts_ms - float(row["created_at"] or (quote_request_ts_ms / 1000)) * 1000))
    latency_audit.update({
        "original_signal_to_quote_latency_ms": original_signal_to_quote_ms,
        "fast_lane_sla_latency_ms": fast_lane_receive_to_quote_ms,
        "fast_lane_receive_to_quote_latency_ms": fast_lane_receive_to_quote_ms,
        "fast_lane_queue_to_quote_latency_ms": fast_lane_queue_to_quote_ms,
        "fast_lane_claim_to_quote_latency_ms": int(max(0, quote_request_ts_ms - float(row["claimed_at"] or (quote_request_ts_ms / 1000)) * 1000)),
        "fast_lane_latency_basis": "receive_to_quote_for_execution_sla",
    })
    capital_tier = "tiny_probe"
    position_size_class = ptm.position_size_class(size_sol)
    monitor_state = {
        "tokenCA": token_ca,
        "symbol": symbol,
        "entryPrice": quote_price,
        "entryMode": entry_mode,
        "entryBranch": entry_branch,
        "entryTriggerPrice": trigger_price,
        "entryQuotePrice": quote_price,
        "entryPriceUnit": ptm.PRICE_UNIT_SOL_PER_TOKEN,
        "entryTriggerPriceUnit": ptm.PRICE_UNIT_SOL_PER_TOKEN,
        "entryQuotePriceUnit": ptm.PRICE_UNIT_SOL_PER_TOKEN,
        "pnlUnit": ptm.PNL_UNIT_RATIO_DECIMAL,
        "accountingUnit": ptm.AMOUNT_UNIT_SOL,
        "priceUnitContractVersion": ptm.PRICE_UNIT_CONTRACT_VERSION,
        "entrySpreadPct": guard.get("quote_drift_pct"),
        "entrySol": size_sol,
        "capitalTier": capital_tier,
        "positionSizeClass": position_size_class,
        "paperOnly": True,
        "fastLane": True,
        "quotePrimaryRequired": True,
        "entryLatencyAudit": latency_audit,
        "fastLaneGuard": guard,
        "sourceType": row["source_type"],
        "sourceResonanceCohort": row["source_resonance_cohort"],
        "hardGateStatus": row["hard_gate_status"],
        "tokenAmount": int(token_amount_raw),
        "tokenDecimals": int(token_decimals or 0),
        "entryTime": entry_ts * 1000,
    }
    audit = ptm.build_execution_audit(execution, {
        "auditVersion": 1,
        "stage": strategy_stage,
        "lifecycleId": lifecycle_id,
        "signalTs": signal_ts,
        "entryPriceSolPerToken": quote_price,
        "entryTriggerPriceSolPerToken": trigger_price,
        "entryQuotePriceSolPerToken": quote_price,
        **ptm.price_unit_contract_payload(),
        "entrySpreadPct": guard.get("quote_drift_pct"),
        "entryLatencyAudit": latency_audit,
        "positionSizeSol": size_sol,
        "capitalTier": capital_tier,
        "positionSizeClass": position_size_class,
        "policyVersion": FAST_LANE_POLICY_VERSION,
        "entryBranch": entry_branch,
        "paperOnly": True,
        "quotePrimaryRequired": True,
        "fastLane": True,
    })

    with SQLITE_WRITE_LOCK:
        db.execute(
            """
            INSERT INTO paper_trades (
                strategy_id, strategy_role, strategy_stage, stage_outcome,
                token_ca, symbol, signal_ts, entry_price, entry_ts,
                market_regime, replay_source, peak_pnl, trailing_active,
                lifecycle_id, stage_seq, trigger_ts, trigger_price,
                position_size_sol, token_amount_raw, token_decimals,
                entry_execution_json, entry_execution_audit_json, monitor_state_json,
                premium_signal_id, signal_type, signal_route, entry_mode,
                strategy_outcome, execution_availability, accounting_outcome, synthetic_close,
                capital_tier, position_size_class, paper_only, regime_tag,
                signal_to_quote_latency_ms, signal_to_quote_drift_pct, quote_spread_pct,
                policy_version, entry_branch, intervention_flags_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'entered', 'available', 'open', 0, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "paper-fast-lane-v1",
                "fast_paper_canary",
                strategy_stage,
                f"{entry_branch}_fast_lane_entered",
                token_ca,
                symbol,
                signal_ts,
                quote_price,
                entry_ts,
                "unknown",
                "paper_fast_lane",
                lifecycle_id,
                ptm.stage_seq(strategy_stage),
                entry_ts,
                trigger_price,
                size_sol,
                str(token_amount_raw),
                token_decimals or 0,
                json.dumps(execution),
                json.dumps(audit),
                json.dumps(monitor_state, ensure_ascii=False),
                (json.loads(row["payload_json"] or "{}") or {}).get("premium_signal_id"),
                (json.loads(row["payload_json"] or "{}") or {}).get("signal_type"),
                row["source_type"],
                entry_mode,
                capital_tier,
                position_size_class,
                "unknown",
                latency_audit.get("fast_lane_sla_latency_ms"),
                latency_audit.get("signal_to_quote_drift_pct"),
                latency_audit.get("quote_spread_pct"),
                FAST_LANE_POLICY_VERSION,
                entry_branch,
                json.dumps(["fast_lane", "quote_primary_required"]),
            ),
        )
        trade_id = db.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        ptm.record_decision_event(
            db,
            component="paper_fast_lane",
            event_type="entry_quote",
            decision="filled_paper",
            reason=guard.get("reason") or "fast_lane_entry",
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            trade_id=trade_id,
            signal_ts=signal_ts,
            signal_id=(json.loads(row["payload_json"] or "{}") or {}).get("premium_signal_id"),
            strategy_stage=strategy_stage,
            route=row["source_type"],
            data_source="paper_fast_entry_queue+jupiter_quote",
            payload={
                "queue_id": row["id"],
                "entry_mode": entry_mode,
                "entry_branch": entry_branch,
                "guard": guard,
                "entry_latency_audit": latency_audit,
                "quote_primary_required": True,
            },
        )
        db.commit()
    return trade_id


def process_queue_item(db, row, owner):
    if not FAST_ENTRY_ENABLED:
        mark_queue(db, row["id"], "disabled", "fast_entry_disabled")
        return
    now_ts = time.time()
    policy = direct_fill_policy(row, now_ts=now_ts)
    if not policy.get("pass"):
        mark_queue(db, row["id"], policy.get("status") or "watch_only", policy.get("reason"))
        return
    mode, stage = source_to_mode_and_stage(row)
    entry_mode_quality = evaluate_entry_mode_quality(db, mode, now_ts=now_ts)
    if entry_mode_quality.get("decision") == "shadow":
        reason = entry_mode_quality.get("reason") or "entry_mode_quality_shadow"
        mark_queue(db, row["id"], "watch_only", reason)
        try:
            token_ca = row["token_ca"]
            signal_ts = int(normalize_ts_sec(row["source_signal_ts"] or row["created_at"]))
            ptm.record_decision_event(
                db,
                component="entry_mode_quality",
                event_type="fast_lane_quality_gate",
                decision="watch_only",
                reason=reason,
                token_ca=token_ca,
                symbol=row["symbol"],
                lifecycle_id=ptm.build_lifecycle_id(token_ca, signal_ts),
                signal_ts=signal_ts,
                route=row["source_type"],
                data_source="paper_fast_entry_queue+paper_trades",
                payload=entry_mode_quality,
            )
            db.commit()
        except Exception:
            pass
        return
    branch = str(row_value(row, "entry_branch", "") or row_value(row, "source_type", "") or "")
    circuit = branch_circuit_detail(
        db,
        branch,
        market_session=row_value(row, "market_session"),
        now_ts=now_ts,
    )
    if not circuit.get("pass"):
        mark_queue(db, row["id"], "watch_only", circuit.get("reason") or "branch_circuit_block")
        try:
            token_ca = row["token_ca"]
            signal_ts = int(normalize_ts_sec(row["source_signal_ts"] or row["created_at"]))
            ptm.record_decision_event(
                db,
                component="paper_fast_lane",
                event_type="branch_circuit",
                decision="watch_only",
                reason=circuit.get("reason") or "branch_circuit_block",
                token_ca=token_ca,
                symbol=row["symbol"],
                lifecycle_id=ptm.build_lifecycle_id(token_ca, signal_ts),
                signal_ts=signal_ts,
                route=row["source_type"],
                data_source="paper_trades_branch_ev",
                payload=circuit,
            )
            db.commit()
        except Exception:
            pass
        return
    if now_ts - float(row["created_at"] or now_ts) > FAST_ENTRY_MAX_QUEUE_AGE_SEC:
        mark_queue(db, row["id"], "expired", "fast_lane_queue_age_expired")
        return
    if not global_rate_limit_allows(db, now_ts=now_ts):
        mark_queue(db, row["id"], "rate_limited", "fast_lane_global_rate_limit")
        return
    if not open_position_cap_allows(db):
        mark_queue(db, row["id"], "rate_limited", "fast_lane_open_position_cap")
        return
    token_ca = row["token_ca"]
    signal_ts = int(normalize_ts_sec(row["source_signal_ts"] or row["created_at"]))
    lifecycle_id = ptm.build_lifecycle_id(token_ca, signal_ts)
    if open_or_recent_trade_exists(db, token_ca, now_ts=now_ts):
        mark_queue(db, row["id"], "skipped", "open_or_recent_trade_exists")
        return
    if not acquire_token_lock(db, token_ca, lifecycle_id, owner, now_ts=now_ts):
        mark_queue(db, row["id"], "skipped", "token_lifecycle_lock_held")
        return
    try:
        quote_request_ts_ms = int(time.time() * 1000)
        execution = ptm.simulate_entry_execution(
            token_ca,
            FAST_ENTRY_SIZE_SOL,
            stage,
            strategy_id="paper-fast-lane-v1",
            lifecycle_id=lifecycle_id,
            timeout=FAST_ENTRY_QUOTE_TIMEOUT_SEC,
            fast_lane_timeout=True,
        )
        quote_response_ts_ms = int(time.time() * 1000)
        if not execution.get("success"):
            reason = execution.get("failureReason") or "entry_quote_failed"
            mark_queue(db, row["id"], "retry_watch" if ptm.is_retryable_entry_quote_failure(reason) else "quote_failed", reason)
            return
        quote_price = execution.get("effectivePrice")
        token_amount_raw = execution.get("quotedOutAmountRaw")
        if quote_price is None or quote_price <= 0 or not token_amount_raw:
            mark_queue(db, row["id"], "quote_failed", "invalid_entry_quote_payload")
            return
        guard = entry_guard_detail(
            row,
            float(quote_price),
            quote_request_ts_ms=quote_request_ts_ms,
            quote_response_ts_ms=quote_response_ts_ms,
        )
        if not guard.get("pass"):
            status = "retry_watch" if "retry" in str(guard.get("reason")) else "rejected"
            mark_queue(db, row["id"], status, guard.get("reason"))
            ptm.record_decision_event(
                db,
                component="paper_fast_lane",
                event_type="entry_guard",
                decision="reject",
                reason=guard.get("reason"),
                token_ca=token_ca,
                symbol=row["symbol"],
                lifecycle_id=lifecycle_id,
                signal_ts=signal_ts,
                strategy_stage=stage,
                route=row["source_type"],
                data_source="paper_fast_entry_queue+entry_guard",
                payload=guard,
            )
            db.commit()
            return
        requested_size_sol = float(guard.get("position_size_sol") or FAST_ENTRY_SIZE_SOL)
        if abs(requested_size_sol - FAST_ENTRY_SIZE_SOL) > 1e-9:
            quote_request_ts_ms = int(time.time() * 1000)
            execution = ptm.simulate_entry_execution(
                token_ca,
                requested_size_sol,
                stage,
                strategy_id="paper-fast-lane-v1",
                lifecycle_id=lifecycle_id,
                timeout=FAST_ENTRY_QUOTE_TIMEOUT_SEC,
                fast_lane_timeout=True,
            )
            quote_response_ts_ms = int(time.time() * 1000)
            if not execution.get("success"):
                reason = execution.get("failureReason") or "entry_quote_failed"
                mark_queue(db, row["id"], "retry_watch" if ptm.is_retryable_entry_quote_failure(reason) else "quote_failed", reason)
                return
            quote_price = execution.get("effectivePrice")
            token_amount_raw = execution.get("quotedOutAmountRaw")
            if quote_price is None or quote_price <= 0 or not token_amount_raw:
                mark_queue(db, row["id"], "quote_failed", "invalid_degraded_entry_quote_payload")
                return
            guard = entry_guard_detail(
                row,
                float(quote_price),
                quote_request_ts_ms=quote_request_ts_ms,
                quote_response_ts_ms=quote_response_ts_ms,
            )
            guard["position_size_sol"] = requested_size_sol
            if not guard.get("pass"):
                status = "retry_watch" if "retry" in str(guard.get("reason")) else "rejected"
                mark_queue(db, row["id"], status, guard.get("reason"))
                return
        if open_or_recent_trade_exists(db, token_ca, now_ts=time.time()):
            mark_queue(db, row["id"], "skipped", "open_or_recent_trade_exists_after_quote")
            return
        if not open_position_cap_allows(db):
            mark_queue(db, row["id"], "rate_limited", "fast_lane_open_position_cap_after_quote")
            return
        trade_id = insert_fast_paper_trade(
            db,
            row,
            execution,
            guard,
            quote_request_ts_ms=quote_request_ts_ms,
            quote_response_ts_ms=quote_response_ts_ms,
        )
        mark_queue(db, row["id"], "entered", None)
        log.info(
            "[FAST_ENTRY] entered token=%s symbol=%s mode=%s branch=%s trade_id=%s latency_ms=%s drift_pct=%s",
            token_ca,
            row["symbol"],
            mode,
            row["entry_branch"],
            trade_id,
            guard.get("signal_to_quote_latency_ms"),
            guard.get("quote_drift_pct"),
        )
    finally:
        release_token_lock(db, token_ca, owner)


def process_premium_signal_row(pdb, row, *, now_ts=None):
    now_ts = float(now_ts if now_ts is not None else time.time())
    status = str(row["hard_gate_status"] or "").upper()
    if status_is_hard_reject(status):
        return "hard_reject"
    branch = None
    priority = 50
    source_type = None
    if status in FAST_LANE_PREMIUM_PASS_STATUSES:
        source_type = "hard_gate_fast"
        branch = "hard_gate_fast_clean"
        priority = 10
    elif status in FAST_LANE_RESCUE_STATUSES:
        source_type = "kline_retry_reclaim_fast"
        branch = status.lower()
        priority = 30
    else:
        return "ignored"
    payload = {
        "premium_signal_id": row["id"],
        "signal_type": row["signal_type"],
        "description": row["description"],
    }
    receive_ts = row["receive_ts"] or row["timestamp"]
    if candidate_is_too_stale(receive_ts, now_ts, source_type):
        inserted = record_fast_lane_observation(
            pdb,
            source_type=source_type,
            token_ca=row["token_ca"],
            symbol=row["symbol"],
            signal_ts=row["timestamp"],
            receive_ts=row["receive_ts"],
            recorded_ts=row["created_at"],
            entry_mode_hint="hard_gate_pass_tiny_probe" if source_type == "hard_gate_fast" else "pre_pass_resonance_tiny_probe",
            entry_branch=branch,
            hard_gate_status=status,
            trigger_mc=row["market_cap"],
            priority=priority,
            payload=payload,
            status="watch_only",
            reason="premium_signal_stale_watch_only",
            now_ts=now_ts,
        )
        if inserted:
            log.info("[FAST_WATCH] premium source=%s token=%s status=%s reason=premium_signal_stale_watch_only", source_type, row["token_ca"], status)
        return "watch_only"
    policy_probe = {
        "entry_branch": branch,
        "source_type": source_type,
        "payload_json": json.dumps(payload),
    }
    policy = direct_fill_policy(policy_probe)
    if not policy.get("pass"):
        inserted = record_fast_lane_observation(
            pdb,
            source_type=source_type,
            token_ca=row["token_ca"],
            symbol=row["symbol"],
            signal_ts=row["timestamp"],
            receive_ts=row["receive_ts"],
            recorded_ts=row["created_at"],
            entry_mode_hint="hard_gate_pass_tiny_probe" if source_type == "hard_gate_fast" else "pre_pass_resonance_tiny_probe",
            entry_branch=branch,
            hard_gate_status=status,
            trigger_mc=row["market_cap"],
            priority=priority,
            payload=payload,
            status=policy.get("status") or "watch_only",
            reason=policy.get("reason"),
            now_ts=now_ts,
        )
        if inserted:
            log.info("[FAST_WATCH] premium source=%s token=%s status=%s reason=%s", source_type, row["token_ca"], status, policy.get("reason"))
        return "watch_only"
    inserted = enqueue_fast_entry(
        pdb,
        source_type=source_type,
        token_ca=row["token_ca"],
        symbol=row["symbol"],
        signal_ts=row["timestamp"],
        receive_ts=row["receive_ts"],
        recorded_ts=row["created_at"],
        entry_mode_hint="hard_gate_pass_tiny_probe" if source_type == "hard_gate_fast" else "pre_pass_resonance_tiny_probe",
        entry_branch=branch,
        hard_gate_status=status,
        trigger_mc=row["market_cap"],
        priority=priority,
        payload=payload,
        now_ts=now_ts,
    )
    if inserted:
        log.info("[FAST_QUEUE] premium source=%s token=%s status=%s", source_type, row["token_ca"], status)
        return "queued"
    return "deduped"


def scan_premium_once(sdb, pdb, *, last_id=0, lookback_sec=0, now_ts=None):
    """Scan new premium rows and reconcile recent status changes.

    Some premium rows are inserted before their final hard_gate_status is known.
    An id-only cursor can therefore miss a row that later turns into PASS or a
    rescue status. The recent-window reconciliation path is deliberately
    idempotent; fast-lane queue keys and token dedupe prevent duplicate entries.
    """
    init_fast_lane_schema(pdb)
    if not table_exists(sdb, "premium_signals"):
        return {"last_id": last_id, "rows": 0, "queued": 0, "watch_only": 0, "deduped": 0}
    cols = table_columns(sdb, "premium_signals")
    timestamp_expr = unixish_sql_expr("timestamp") if "timestamp" in cols else "0"
    cutoff = int((now_ts if now_ts is not None else time.time()) - max(0, int(lookback_sec or 0)))
    actionable_statuses = tuple(FAST_LANE_PREMIUM_PASS_STATUSES | FAST_LANE_RESCUE_STATUSES)
    placeholders = ",".join("?" for _ in actionable_statuses)
    rows = sdb.execute(
        f"""
        SELECT id, token_ca, symbol, timestamp, hard_gate_status,
               {optional_col(cols, 'signal_type')},
               {optional_col(cols, 'source_message_ts')},
               {optional_col(cols, 'receive_ts')},
               {optional_col(cols, 'created_at')},
               {optional_col(cols, 'market_cap')},
               {optional_col(cols, 'description', "''")}
        FROM premium_signals
        WHERE id > ?
           OR (
             {timestamp_expr} >= ?
             AND UPPER(COALESCE(hard_gate_status, '')) IN ({placeholders})
           )
        ORDER BY id ASC
        LIMIT ?
        """,
        (last_id, cutoff, *actionable_statuses, FAST_ENTRY_PREMIUM_BATCH_LIMIT * 2),
    ).fetchall()
    counts = {"last_id": last_id, "rows": len(rows), "queued": 0, "watch_only": 0, "deduped": 0, "ignored": 0, "hard_reject": 0}
    for row in rows:
        counts["last_id"] = max(int(counts["last_id"]), int(row["id"] or 0))
        result = process_premium_signal_row(pdb, row, now_ts=now_ts)
        counts[result] = counts.get(result, 0) + 1
    return counts


def premium_scan(signal_db_path, paper_db_path, stop_event, lookback_sec):
    last_id = 0
    try:
        sdb = connect_db(signal_db_path)
        if table_exists(sdb, "premium_signals"):
            cutoff = int(time.time() - max(0, int(lookback_sec or 0)))
            cols = table_columns(sdb, "premium_signals")
            timestamp_expr = unixish_sql_expr("timestamp") if "timestamp" in cols else "0"
            row = sdb.execute(
                f"SELECT COALESCE(MAX(id), 0) AS id FROM premium_signals WHERE {timestamp_expr} < ?",
                (cutoff,),
            ).fetchone()
            last_id = int(row["id"] or 0)
        sdb.close()
    except Exception:
        last_id = 0
    while not stop_event.is_set():
        try:
            sdb = connect_db(signal_db_path)
            pdb = connect_db(paper_db_path)
            init_fast_lane_schema(pdb)
            if not table_exists(sdb, "premium_signals"):
                time.sleep(FAST_ENTRY_SCAN_INTERVAL_SEC)
                continue
            result = scan_premium_once(
                sdb,
                pdb,
                last_id=last_id,
                lookback_sec=lookback_sec,
            )
            last_id = int(result.get("last_id") or last_id)
            sdb.close()
            pdb.close()
        except Exception as exc:
            log.warning("premium scan failed: %s", exc, exc_info=True)
        stop_event.wait(FAST_ENTRY_SCAN_INTERVAL_SEC)


def source_resonance_scan(paper_db_path, stop_event):
    while not stop_event.is_set():
        try:
            db = connect_db(paper_db_path)
            init_fast_lane_schema(db)
            if table_exists(db, "source_resonance_candidates"):
                source_cols = table_columns(db, "source_resonance_candidates")
                rows = db.execute(
                    f"""
                    SELECT id, token_ca, symbol, signal_ts, telegram_signal_id, signal_type,
                           receive_ts, signal_recorded_ts, gmgn_pre_seen, quote_clean_seen,
                           two_quote_clean_snapshots, gmgn_momentum_confirmed,
                           gmgn_volume_confirmed, gmgn_momentum_rounds,
                           {optional_col(source_cols, "gmgn_last_liquidity", "NULL")},
                           cohort, resonance_level, resonance_score, updated_at
                    FROM source_resonance_candidates
                    WHERE (COALESCE(quote_clean_seen, 0) = 1 OR COALESCE(gmgn_pre_seen, 0) = 1 OR cohort LIKE '%telegram_gmgn%')
                      AND updated_at >= datetime('now', ?)
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (f"-{FAST_ENTRY_SOURCE_LOOKBACK_SEC} seconds", FAST_ENTRY_SOURCE_SCAN_LIMIT),
                ).fetchall()
                for row in rows:
                    priority = 12 if int(row["quote_clean_seen"] or 0) else 18
                    branch = "source_resonance_quote_clean_fast" if int(row["quote_clean_seen"] or 0) else "source_resonance_gmgn_fast"
                    detected_ts = int(time.time())
                    original_receive_ts = row["receive_ts"] or row["signal_ts"] or row["signal_recorded_ts"] or detected_ts
                    payload = {
                        "telegram_signal_id": row["telegram_signal_id"],
                        "signal_type": row["signal_type"],
                        "original_signal_ts": row["signal_ts"],
                        "original_receive_ts": original_receive_ts,
                        "fast_lane_detected_ts": detected_ts,
                        "resonance_level": row["resonance_level"],
                        "resonance_score": row["resonance_score"],
                        "quote_clean_seen": row["quote_clean_seen"],
                        "two_quote_clean_snapshots": row["two_quote_clean_snapshots"],
                        "gmgn_pre_seen": row["gmgn_pre_seen"],
                        "gmgn_momentum_confirmed": row["gmgn_momentum_confirmed"],
                        "gmgn_volume_confirmed": row["gmgn_volume_confirmed"],
                        "gmgn_momentum_rounds": row["gmgn_momentum_rounds"],
                        "gmgn_last_liquidity": row["gmgn_last_liquidity"],
                        "liquidity_usd": row["gmgn_last_liquidity"],
                        "source_updated_at": row["updated_at"],
                        "market_session": market_session_for_ts(original_receive_ts or detected_ts),
                    }
                    if branch == "source_resonance_gmgn_fast":
                        canary = source_gmgn_momentum_canary_detail(payload, now_ts=detected_ts)
                        payload["source_gmgn_momentum_canary"] = canary
                        if canary.get("pass"):
                            branch = "source_gmgn_momentum_canary"
                            priority = 16
                    elif branch == "source_resonance_quote_clean_fast":
                        original_age_sec = payload_ts_age_sec(
                            payload,
                            ("original_signal_ts", "original_receive_ts"),
                            now_ts=detected_ts,
                        )
                        if (
                            original_age_sec is not None
                            and original_age_sec > FAST_ENTRY_SOURCE_QUOTE_CLEAN_MAX_ORIGINAL_AGE_SEC
                        ):
                            refresh = source_quote_clean_refresh_detail(payload, now_ts=detected_ts)
                            payload["source_quote_clean_refresh"] = refresh
                            if refresh.get("pass"):
                                branch = "source_quote_clean_refresh_tiny_probe"
                                priority = 14
                    policy_probe = {
                        "entry_branch": branch,
                        "source_type": "source_resonance_fast",
                        "payload_json": json.dumps(payload),
                    }
                    policy = direct_fill_policy(policy_probe)
                    if not policy.get("pass"):
                        payload["direct_fill_policy"] = policy.get("detail") or {}
                        inserted = record_fast_lane_observation(
                            db,
                            source_type="source_resonance_fast",
                            token_ca=row["token_ca"],
                            symbol=row["symbol"],
                            signal_ts=row["signal_ts"],
                            receive_ts=original_receive_ts,
                            recorded_ts=row["signal_recorded_ts"],
                            entry_mode_hint="source_resonance_tiny_probe",
                            entry_branch=branch,
                            source_resonance_cohort=row["cohort"],
                            priority=priority,
                            payload=payload,
                            status=policy.get("status") or "watch_only",
                            reason=policy.get("reason"),
                        )
                        if inserted:
                            log.info("[FAST_WATCH] source token=%s cohort=%s branch=%s reason=%s", row["token_ca"], row["cohort"], branch, policy.get("reason"))
                        continue
                    inserted = enqueue_fast_entry(
                        db,
                        source_type="source_resonance_fast",
                        token_ca=row["token_ca"],
                        symbol=row["symbol"],
                        signal_ts=row["signal_ts"],
                        receive_ts=original_receive_ts,
                        recorded_ts=row["signal_recorded_ts"],
                        entry_mode_hint="source_resonance_tiny_probe",
                        entry_branch=branch,
                        source_resonance_cohort=row["cohort"],
                        priority=priority,
                        payload=payload,
                    )
                    if inserted:
                        log.info("[FAST_QUEUE] source token=%s cohort=%s branch=%s", row["token_ca"], row["cohort"], branch)
            db.close()
        except Exception as exc:
            log.warning("source resonance scan failed: %s", exc, exc_info=True)
        stop_event.wait(FAST_ENTRY_SCAN_INTERVAL_SEC)


def missed_rescue_signature(row):
    values = [
        CLEAN_DOG_RECLAIM_POLICY_VERSION,
        row_value(row, "tradable_missed", 0),
        row_value(row, "would_stop_before_peak", 0),
        row_value(row, "first_tradable_ts"),
        row_value(row, "updated_at"),
        row_value(row, "executable_peak_pnl"),
        row_value(row, "route"),
        row_value(row, "component"),
        row_value(row, "reject_reason"),
    ]
    return "|".join("" if value is None else str(value) for value in values)


def missed_rescue_entry_mode_hint(row, reason, branch):
    route = str(row_value(row, "route", "") or "").upper()
    reason_l = str(reason or "").lower()
    branch_l = str(branch or "").lower()
    if (
        route in {"LOTTO", "NOT_ATH"}
        and (
            reason_l.startswith("tracking_ttl")
            or "not_ath_prebuy_kline" in reason_l
            or reason_l == "pre_pass_signal_too_stale"
            or reason_l.startswith("lotto_stale_")
            or branch_l.startswith("not_ath_reclaim")
            or branch_l.startswith("pre_pass_stale_reclaim")
            or branch_l.startswith("tracking_ttl_reclaim")
        )
    ):
        return "lotto_not_ath_reclaim_tiny_probe"
    if reason_l in SMART_QUALITY_RECHECK_REASONS:
        return "lotto_micro_reclaim_tiny_probe"
    return "pre_pass_resonance_tiny_probe"


def clean_dog_reclaim_state(status, reason):
    status_l = str(status or "").lower()
    reason_l = str(reason or "").lower()
    if status_l in {"entered", "filled", "filled_paper"}:
        return "entered"
    if status_l == "queued":
        return "queued"
    if "stale" in reason_l:
        return "stale"
    if "stop_before_peak" in reason_l or "toxic" in reason_l or "top_holder" in reason_l:
        return "shadow"
    if "quote_clean_missing" in reason_l or "timestamp_missing" in reason_l or "momentum_missing" in reason_l:
        return "tracking"
    if status_l == "watch_only":
        return "watch_only"
    if status_l == "counterfactual_only":
        return "counterfactual"
    if status_l == "deduped":
        return "deduped"
    return status_l or "observed"


def _payload_ts(payload, *keys):
    payload = payload or {}
    for key in keys:
        ts = parse_datetime_ts(payload.get(key))
        if ts is not None:
            return float(ts)
    return None


def mark_missed_rescue_processed(
    db,
    missed_id,
    signature,
    *,
    status=None,
    reason=None,
    now_ts=None,
    token_ca=None,
    entry_branch=None,
    entry_mode_hint=None,
    blocker=None,
    payload=None,
    eligibility=None,
):
    now_ts = float(now_ts if now_ts is not None else time.time())
    payload = payload or {}
    eligibility = eligibility if isinstance(eligibility, dict) else None
    state = clean_dog_reclaim_state(status, reason)
    last_clean_quote_ts = _payload_ts(payload, "last_clean_quote_ts", "missed_updated_at", "updated_at")
    last_tradable_ts = _payload_ts(payload, "last_tradable_ts", "first_tradable_ts", "missed_updated_at", "updated_at")
    db.execute(
        """
        INSERT INTO paper_fast_missed_rescue_state (
            missed_attribution_id, rescue_signature, last_status, last_reason,
            last_action_at, updated_at, token_ca, entry_branch, entry_mode_hint,
            policy_version, state, blocker, first_seen_at, last_clean_quote_ts,
            last_tradable_ts, eligibility_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(missed_attribution_id) DO UPDATE SET
            rescue_signature = excluded.rescue_signature,
            last_status = excluded.last_status,
            last_reason = excluded.last_reason,
            last_action_at = excluded.last_action_at,
            updated_at = excluded.updated_at,
            token_ca = COALESCE(excluded.token_ca, paper_fast_missed_rescue_state.token_ca),
            entry_branch = COALESCE(excluded.entry_branch, paper_fast_missed_rescue_state.entry_branch),
            entry_mode_hint = COALESCE(excluded.entry_mode_hint, paper_fast_missed_rescue_state.entry_mode_hint),
            policy_version = excluded.policy_version,
            state = excluded.state,
            blocker = COALESCE(excluded.blocker, paper_fast_missed_rescue_state.blocker),
            first_seen_at = COALESCE(paper_fast_missed_rescue_state.first_seen_at, excluded.first_seen_at),
            last_clean_quote_ts = COALESCE(excluded.last_clean_quote_ts, paper_fast_missed_rescue_state.last_clean_quote_ts),
            last_tradable_ts = COALESCE(excluded.last_tradable_ts, paper_fast_missed_rescue_state.last_tradable_ts),
            eligibility_json = excluded.eligibility_json
        """,
        (
            int(missed_id),
            signature,
            status,
            reason,
            now_ts,
            now_ts,
            token_ca,
            entry_branch,
            entry_mode_hint,
            CLEAN_DOG_RECLAIM_POLICY_VERSION,
            state,
            blocker,
            now_ts,
            last_clean_quote_ts,
            last_tradable_ts,
            json.dumps(eligibility or {}, ensure_ascii=False),
        ),
    )


def missed_rescue_priority(row, branch):
    peak = 0.0
    try:
        peak = float(row_value(row, "executable_peak_pnl", 0) or 0)
    except (TypeError, ValueError):
        peak = 0.0
    if branch in CLEAN_DOG_RECLAIM_BRANCHES:
        if peak >= 0.50:
            return FAST_ENTRY_CLEAN_DOG_RECLAIM_PRIORITY
        if peak >= 0.25:
            return FAST_ENTRY_CLEAN_DOG_BRONZE_PRIORITY
        return 20
    return 35


def process_missed_rescue_row(db, row, *, now_ts=None):
    now_ts = int(now_ts if now_ts is not None else time.time())
    reason = str(row["reject_reason"] or "missed_rescue")
    rescue_created_ts = now_ts
    original_signal_ts = row["signal_ts"] or row["baseline_ts"] or row["first_tradable_ts"] or rescue_created_ts
    reason_l = reason.lower()
    if reason.startswith("tracking_ttl"):
        source_type = "ttl_final_reclaim_fast"
        branch = "tracking_ttl_reclaim_quote_clean_tiny_probe"
    elif reason_l == "pre_pass_signal_too_stale":
        source_type = "pre_pass_stale_reclaim_fast"
        branch = "pre_pass_stale_reclaim_quote_clean_tiny_probe"
    elif reason_l == "not_ath_v17":
        source_type = "not_ath_reclaim_fast"
        branch = "not_ath_reclaim_quote_clean_tiny_probe"
    elif reason in KLINE_RESCUE_BRANCHES or "kline" in reason_l:
        source_type = "not_ath_reclaim_fast"
        branch = "not_ath_reclaim_quote_clean_tiny_probe"
    elif "spread" in reason:
        source_type = "spread_recovery_fast"
        branch = reason
    elif "quote" in reason:
        source_type = "missing_quote_recovery_fast"
        branch = reason
    elif reason in SMART_QUALITY_RECHECK_REASONS:
        source_type = "smart_quality_reclaim_fast"
        branch = "smart_entry_reclaim_quote_clean_tiny_probe"
    elif reason in MATRIX_TIMEOUT_RECHECK_REASONS or reason_l.startswith("timeout (") or reason_l.startswith("price_collapse"):
        source_type = "matrix_timeout_reclaim_fast"
        branch = "matrix_timeout_final_quote_tiny_probe"
    else:
        source_type = "stale_refresh_fast"
        branch = reason
    payload = {
        "missed_attribution_id": row["id"],
        "signal_id": row["signal_id"],
        "original_signal_ts": original_signal_ts,
        "original_receive_ts": original_signal_ts,
        "first_tradable_ts": row["first_tradable_ts"],
        "last_tradable_ts": row_value(row, "updated_at"),
        "last_clean_quote_ts": row_value(row, "updated_at"),
        "missed_updated_at": row_value(row, "updated_at"),
        "rescue_created_ts": rescue_created_ts,
        "recovery_created_ts": rescue_created_ts,
        "tradable_missed": row["tradable_missed"],
        "would_stop_before_peak": row_value(row, "would_stop_before_peak", 0),
        "recovery_quote_clean": bool(row["tradable_missed"]),
        "route": row["route"],
        "component": row["component"],
        "reject_reason": reason,
        "executable_peak_pnl": row["executable_peak_pnl"],
    }
    entry_mode_hint = missed_rescue_entry_mode_hint(row, reason, branch)
    priority = missed_rescue_priority(row, branch)
    policy_probe = {
        "entry_branch": branch,
        "source_type": source_type,
        "entry_mode_hint": entry_mode_hint,
        "payload_json": json.dumps(payload),
    }
    policy = direct_fill_policy(policy_probe, now_ts=now_ts)
    eligibility = policy.get("detail") if isinstance(policy.get("detail"), dict) else None
    if branch in CLEAN_DOG_RECLAIM_BRANCHES:
        payload["clean_dog_reclaim_policy_version"] = CLEAN_DOG_RECLAIM_POLICY_VERSION
        payload["clean_dog_reclaim_eligibility"] = eligibility or {}
        payload["clean_dog_reclaim_state"] = clean_dog_reclaim_state(
            "queued" if policy.get("pass") else (policy.get("status") or "watch_only"),
            policy.get("reason"),
        )
    if not policy.get("pass"):
        inserted = record_fast_lane_observation(
            db,
            source_type=source_type,
            token_ca=row["token_ca"],
            symbol=row["symbol"],
            signal_ts=original_signal_ts,
            receive_ts=original_signal_ts,
            recorded_ts=row["baseline_ts"],
            entry_mode_hint=entry_mode_hint,
            entry_branch=branch,
            trigger_price=row["baseline_price"],
            priority=priority,
            payload=payload,
            status=policy.get("status") or "counterfactual_only",
            reason=policy.get("reason"),
            now_ts=now_ts,
        )
        if inserted:
            log.info("[FAST_COUNTERFACTUAL] missed-rescue token=%s reason=%s policy=%s", row["token_ca"], reason, policy.get("reason"))
        return {
            "status": policy.get("status") or "counterfactual_only",
            "reason": policy.get("reason"),
            "inserted": inserted,
            "entry_branch": branch,
            "entry_mode_hint": entry_mode_hint,
            "blocker": reason,
            "payload": payload,
            "eligibility": eligibility,
        }
    inserted = enqueue_fast_entry(
        db,
        source_type=source_type,
        token_ca=row["token_ca"],
        symbol=row["symbol"],
        signal_ts=original_signal_ts,
        receive_ts=original_signal_ts,
        recorded_ts=row["baseline_ts"],
        entry_mode_hint=entry_mode_hint,
        entry_branch=branch,
        trigger_price=row["baseline_price"],
        priority=priority,
        payload=payload,
        now_ts=now_ts,
    )
    if inserted:
        log.info("[FAST_QUEUE] missed-rescue token=%s reason=%s", row["token_ca"], reason)
    return {
        "status": "queued" if inserted else "deduped",
        "reason": policy.get("reason"),
        "inserted": inserted,
        "entry_branch": branch,
        "entry_mode_hint": entry_mode_hint,
        "blocker": reason,
        "payload": payload,
        "eligibility": eligibility,
    }


def scan_missed_rescue_once(db, *, now_ts=None, limit=None):
    now_ts = float(now_ts if now_ts is not None else time.time())
    init_fast_lane_schema(db)
    if not table_exists(db, "paper_missed_signal_attribution"):
        return {"rows": 0, "processed": 0, "queued": 0, "watch_only": 0, "counterfactual_only": 0, "deduped": 0}
    cols = table_columns(db, "paper_missed_signal_attribution")
    cutoff = now_ts - FAST_ENTRY_MISSED_RESCUE_LOOKBACK_SEC
    updated_expr = "COALESCE(strftime('%s', m.updated_at), 0)" if "updated_at" in cols else "0"
    created_expr = "COALESCE(m.created_event_ts, m.signal_ts, m.baseline_ts, 0)"
    first_tradable_expr = "COALESCE(m.first_tradable_ts, 0)" if "first_tradable_ts" in cols else "0"
    stop_before_peak_expr = "COALESCE(m.would_stop_before_peak, 0)" if "would_stop_before_peak" in cols else "0"
    rows = db.execute(
        f"""
        SELECT m.id, m.token_ca, m.symbol, m.signal_ts, m.signal_id, m.route, m.component,
               m.reject_reason, m.baseline_price, m.baseline_ts,
               {optional_col(cols, 'first_tradable_ts')},
               {optional_col(cols, 'tradable_missed', '0')},
               {optional_col(cols, 'would_stop_before_peak', '0')},
               {optional_col(cols, 'executable_peak_pnl', '0')},
               {'m.updated_at AS updated_at' if 'updated_at' in cols else 'NULL AS updated_at'},
               s.rescue_signature AS processed_signature
        FROM paper_missed_signal_attribution m
        LEFT JOIN paper_fast_missed_rescue_state s ON s.missed_attribution_id = m.id
        WHERE COALESCE({ 'm.tradable_missed' if 'tradable_missed' in cols else '0' }, 0) = 1
          AND {stop_before_peak_expr} != 1
          AND (
            {first_tradable_expr} >= ?
            OR {created_expr} >= ?
            OR {updated_expr} >= ?
          )
          AND (
            m.reject_reason IN (
              'tracking_ttl_expired',
              'not_ath_v17',
              'not_ath_prebuy_kline_retry_expired',
              'not_ath_prebuy_kline_block',
              'entry_edge_spread_too_high',
              'missing_trigger_or_quote',
              'entry_edge_probe_missing_trigger_or_quote',
              'pre_pass_signal_too_stale',
              'weak_buying_pressure',
              'no_kline_low_volume',
              'negative_trend',
              'chasing_top',
              'trend_bearish_timeout',
              'scout_quality_buy_pressure_weak',
              'scout_quality_volume_low',
              'scout_quality_tx_low',
              'scout_quality_negative_trend',
              'matrices not yet aligned'
            )
            OR m.reject_reason LIKE 'lotto_stale_%'
            OR m.reject_reason LIKE 'timeout (%'
            OR m.reject_reason LIKE 'price_collapse%'
          )
        ORDER BY COALESCE({first_tradable_expr}, {created_expr}, 0) ASC, m.id ASC
        LIMIT ?
        """,
        (cutoff, cutoff, cutoff, limit or FAST_ENTRY_MISSED_RESCUE_LIMIT),
    ).fetchall()
    counts = {"rows": len(rows), "processed": 0, "queued": 0, "watch_only": 0, "counterfactual_only": 0, "deduped": 0}
    for row in rows:
        signature = missed_rescue_signature(row)
        if row["processed_signature"] == signature:
            continue
        result = process_missed_rescue_row(db, row, now_ts=now_ts)
        status = result.get("status") or "unknown"
        counts["processed"] += 1
        counts[status] = counts.get(status, 0) + 1
        mark_missed_rescue_processed(
            db,
            row["id"],
            signature,
            status=status,
            reason=result.get("reason"),
            now_ts=now_ts,
            token_ca=row["token_ca"],
            entry_branch=result.get("entry_branch"),
            entry_mode_hint=result.get("entry_mode_hint"),
            blocker=result.get("blocker") or row["reject_reason"],
            payload=result.get("payload"),
            eligibility=result.get("eligibility"),
        )
    if counts["processed"]:
        db.commit()
    return counts


def missed_rescue_scan(paper_db_path, stop_event):
    while not stop_event.is_set():
        try:
            db = connect_db(paper_db_path)
            scan_missed_rescue_once(db)
            db.close()
        except Exception as exc:
            log.warning("missed rescue scan failed: %s", exc, exc_info=True)
        stop_event.wait(max(2.0, FAST_ENTRY_SCAN_INTERVAL_SEC * 2))


def worker_loop(paper_db_path, worker_id, stop_event):
    owner = f"fast-worker-{worker_id}"
    db = connect_db(paper_db_path)
    init_fast_lane_schema(db)
    while not stop_event.is_set():
        try:
            if worker_id == 1:
                refresh_retry_watch(db)
            row = claim_queue_item(db, owner)
            if row is None:
                stop_event.wait(0.25)
                continue
            process_queue_item(db, row, owner)
        except Exception as exc:
            log.warning("%s failed: %s", owner, exc, exc_info=True)
            stop_event.wait(1.0)
    db.close()


def run_worker(args):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    stop_event = threading.Event()

    def _stop(signum, frame):
        log.info("stopping paper fast lane")
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    lock_fh = None
    if args.lock_file:
        lock_path = Path(args.lock_file)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_fh = lock_path.open("w", encoding="utf-8")
        try:
            fcntl.flock(lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            lock_fh.write(str(os.getpid()))
            lock_fh.flush()
        except BlockingIOError:
            log.warning("paper fast lane lock held at %s; duplicate worker idling", lock_path)
            while not stop_event.is_set():
                time.sleep(300)
            return

    schema_db = ptm.init_paper_db(args.paper_db)
    schema_db.close()
    db = connect_db(args.paper_db)
    init_fast_lane_schema(db)
    db.close()

    log.info(
        "paper fast lane started paper_db=%s signal_db=%s concurrency=%s",
        args.paper_db,
        args.signal_db,
        args.concurrency,
    )
    with ThreadPoolExecutor(max_workers=args.concurrency + 2, thread_name_prefix="paper-fast") as pool:
        pool.submit(premium_scan, args.signal_db, args.paper_db, stop_event, args.lookback_sec)
        pool.submit(source_resonance_scan, args.paper_db, stop_event)
        pool.submit(missed_rescue_scan, args.paper_db, stop_event)
        for i in range(args.concurrency):
            pool.submit(worker_loop, args.paper_db, i + 1, stop_event)
        while not stop_event.is_set():
            time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-db", default=os.environ.get("PAPER_DB", str(DEFAULT_PAPER_DB)))
    parser.add_argument("--signal-db", default=os.environ.get("SENTIMENT_DB", os.environ.get("DB_PATH", str(DEFAULT_SIGNAL_DB))))
    parser.add_argument("--concurrency", type=int, default=int(os.environ.get("FAST_ENTRY_WORKER_CONCURRENCY", "2")))
    parser.add_argument("--lookback-sec", type=int, default=int(os.environ.get("FAST_ENTRY_BOOT_LOOKBACK_SEC", "120")))
    parser.add_argument("--lock-file", default=os.environ.get("PAPER_FAST_LANE_LOCK_FILE", "/tmp/paper_fast_lane.lock"))
    args = parser.parse_args()
    run_worker(args)


if __name__ == "__main__":
    main()
