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


DEFAULT_PAPER_DB = PROJECT_ROOT / "data" / "paper_trades.db"
DEFAULT_SIGNAL_DB = PROJECT_ROOT / "data" / "sentiment_arb.db"

log = logging.getLogger("paper_fast_lane")

FAST_LANE_POLICY_VERSION = os.environ.get("PAPER_FAST_LANE_POLICY_VERSION", "fast_lane_v1")
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
FAST_ENTRY_KLINE_RESCUE_DIRECT_ENABLED = os.environ.get(
    "FAST_ENTRY_KLINE_RESCUE_DIRECT_ENABLED",
    "false",
).lower() == "true"
FAST_ENTRY_TTL_RESCUE_MAX_TRADABLE_AGE_SEC = float(os.environ.get(
    "FAST_ENTRY_TTL_RESCUE_MAX_TRADABLE_AGE_SEC",
    "300",
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
    "not_ath_prebuy_kline_retry_expired",
    "not_ath_prebuy_kline_block",
    "entry_edge_spread_too_high",
    "missing_trigger_or_quote",
    "entry_edge_probe_missing_trigger_or_quote",
    "lotto_stale",
}

SQLITE_WRITE_LOCK = threading.Lock()


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
        db.execute("CREATE INDEX IF NOT EXISTS idx_pfeq_status_priority ON paper_fast_entry_queue(status, priority, created_at)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pfeq_token_status ON paper_fast_entry_queue(token_ca, status)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_pfeq_queue_key ON paper_fast_entry_queue(queue_key)")
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
        SELECT id, priority, status, entry_branch
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
    try:
        with SQLITE_WRITE_LOCK:
            if active_queue_depth(db) >= FAST_ENTRY_MAX_QUEUE_DEPTH and int(priority) > FAST_ENTRY_PRESSURE_PRIORITY_CUTOFF:
                return False
            existing = existing_recent_queue_row(db, token_ca, now_ts)
            if existing is not None:
                if int(priority) < int(existing["priority"] or 999):
                    db.execute(
                        """
                        UPDATE paper_fast_entry_queue
                        SET priority = ?, source_type = ?, entry_mode_hint = ?, entry_branch = ?,
                            hard_gate_status = COALESCE(?, hard_gate_status),
                            source_resonance_cohort = COALESCE(?, source_resonance_cohort),
                            payload_json = ?,
                            status = CASE WHEN status IN ('watch_only', 'counterfactual_only') THEN 'queued' ELSE status END,
                            last_error = CASE WHEN status IN ('watch_only', 'counterfactual_only') THEN NULL ELSE last_error END,
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
                    payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?)
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
    payload = {
        **(payload or {}),
        "direct_fill_status": status,
        "direct_fill_reason": reason,
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
                    payload_json, last_error, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    now_ts,
                ),
            )
            inserted = db.execute("SELECT changes() AS c").fetchone()["c"] > 0
            if not inserted:
                db.execute(
                    """
                    UPDATE paper_fast_entry_queue
                    SET status = ?, last_error = ?, payload_json = ?, updated_at = ?
                    WHERE queue_key = ? AND status NOT IN ('entered')
                    """,
                    (status, reason, json.dumps(payload, ensure_ascii=False), now_ts, key),
                )
            db.commit()
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
    if "hard_gate" in source_type or "hard_gate" in branch:
        return "hard_gate_pass_tiny_probe", "stage1"
    if "source_resonance" in source_type or "source_resonance" in branch:
        return "source_resonance_tiny_probe", "lotto"
    if "ttl" in source_type or "kline" in source_type or "spread" in source_type or "missing_quote" in source_type:
        return "pre_pass_resonance_tiny_probe", "lotto"
    return row_value(row, "entry_mode_hint") or "pre_pass_resonance_tiny_probe", "lotto"


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


def direct_fill_policy(row, *, now_ts=None):
    branch = str(row_value(row, "entry_branch", "") or "")
    source_type = str(row_value(row, "source_type", "") or "")
    payload = row_payload(row)
    if branch == "source_resonance_gmgn_fast" and not FAST_ENTRY_SOURCE_GMGN_ONLY_DIRECT_ENABLED:
        return {
            "pass": False,
            "status": "watch_only",
            "reason": "source_resonance_gmgn_only_watch_only",
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
        branch in {
            "not_ath_prebuy_kline_unknown_data_blocked",
            "not_ath_prebuy_kline_block",
            "not_ath_prebuy_kline_retry_expired",
        }
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
            SET status = 'expired', last_error = 'fast_lane_queue_age_expired', claimed_by = NULL, claimed_at = NULL, updated_at = ?
            WHERE status IN ('queued', 'claimed')
              AND (? - created_at) > ?
            """,
            (now_ts, now_ts, FAST_ENTRY_MAX_QUEUE_AGE_SEC),
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
        db.execute(
            """
            UPDATE paper_fast_entry_queue
            SET status = ?, last_error = ?, claimed_by = NULL, claimed_at = NULL, updated_at = ?
            WHERE id = ?
            """,
            (status, error, time.time(), row_id),
        )
        db.commit()


def refresh_retry_watch(db, *, now_ts=None):
    now_ts = float(now_ts if now_ts is not None else time.time())
    with SQLITE_WRITE_LOCK:
        db.execute(
            """
            UPDATE paper_fast_entry_queue
            SET status = 'expired', last_error = 'fast_lane_queue_age_expired', claimed_by = NULL, claimed_at = NULL, updated_at = ?
            WHERE status IN ('queued', 'claimed')
              AND (? - created_at) > ?
            """,
            (now_ts, now_ts, FAST_ENTRY_MAX_QUEUE_AGE_SEC),
        )
        db.execute(
            """
            UPDATE paper_fast_entry_queue
            SET status = 'expired', last_error = 'fast_lane_retry_watch_expired', updated_at = ?
            WHERE status = 'retry_watch'
              AND (? - created_at) > ?
            """,
            (now_ts, now_ts, FAST_ENTRY_MAX_QUEUE_AGE_SEC),
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
        mode, stage = source_to_mode_and_stage(row)
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


def premium_scan(signal_db_path, paper_db_path, stop_event, lookback_sec):
    last_id = 0
    try:
        sdb = connect_db(signal_db_path)
        if table_exists(sdb, "premium_signals"):
            cutoff = int(time.time() - max(0, int(lookback_sec or 0)))
            row = sdb.execute(
                "SELECT COALESCE(MAX(id), 0) AS id FROM premium_signals WHERE COALESCE(timestamp, 0) < ?",
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
            cols = table_columns(sdb, "premium_signals")
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
                ORDER BY id ASC
                LIMIT ?
                """,
                (last_id, FAST_ENTRY_PREMIUM_BATCH_LIMIT),
            ).fetchall()
            for row in rows:
                last_id = max(last_id, int(row["id"] or 0))
                status = str(row["hard_gate_status"] or "").upper()
                if status_is_hard_reject(status):
                    continue
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
                    continue
                payload = {
                    "premium_signal_id": row["id"],
                    "signal_type": row["signal_type"],
                    "description": row["description"],
                }
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
                    )
                    if inserted:
                        log.info("[FAST_WATCH] premium source=%s token=%s status=%s reason=%s", source_type, row["token_ca"], status, policy.get("reason"))
                    continue
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
                )
                if inserted:
                    log.info("[FAST_QUEUE] premium source=%s token=%s status=%s", source_type, row["token_ca"], status)
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
                rows = db.execute(
                    """
                    SELECT id, token_ca, symbol, signal_ts, telegram_signal_id, signal_type,
                           receive_ts, signal_recorded_ts, gmgn_pre_seen, quote_clean_seen,
                           two_quote_clean_snapshots, gmgn_momentum_confirmed,
                           gmgn_volume_confirmed, gmgn_momentum_rounds,
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
                        "source_updated_at": row["updated_at"],
                    }
                    policy_probe = {
                        "entry_branch": branch,
                        "source_type": "source_resonance_fast",
                        "payload_json": json.dumps(payload),
                    }
                    policy = direct_fill_policy(policy_probe)
                    if not policy.get("pass"):
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


def missed_rescue_scan(paper_db_path, stop_event):
    last_seen = 0
    try:
        db = connect_db(paper_db_path)
        if table_exists(db, "paper_missed_signal_attribution"):
            cutoff = time.time() - FAST_ENTRY_MAX_QUEUE_AGE_SEC
            row = db.execute(
                "SELECT COALESCE(MAX(id), 0) AS id FROM paper_missed_signal_attribution WHERE COALESCE(created_event_ts, 0) < ?",
                (cutoff,),
            ).fetchone()
            last_seen = int(row["id"] or 0)
        db.close()
    except Exception:
        last_seen = 0
    while not stop_event.is_set():
        try:
            db = connect_db(paper_db_path)
            init_fast_lane_schema(db)
            if table_exists(db, "paper_missed_signal_attribution"):
                rows = db.execute(
                    """
                    SELECT id, token_ca, symbol, signal_ts, signal_id, route, component,
                           reject_reason, baseline_price, baseline_ts,
                           first_tradable_ts, tradable_missed, executable_peak_pnl
                    FROM paper_missed_signal_attribution
                    WHERE id > ?
                      AND COALESCE(tradable_missed, 0) = 1
                      AND (
                        reject_reason IN (
                          'tracking_ttl_expired',
                          'not_ath_prebuy_kline_retry_expired',
                          'not_ath_prebuy_kline_block',
                          'entry_edge_spread_too_high',
                          'missing_trigger_or_quote',
                          'entry_edge_probe_missing_trigger_or_quote'
                        )
                        OR reject_reason LIKE 'lotto_stale_%'
                      )
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (last_seen, FAST_ENTRY_MISSED_RESCUE_LIMIT),
                ).fetchall()
                for row in rows:
                    last_seen = max(last_seen, int(row["id"] or 0))
                    reason = str(row["reject_reason"] or "missed_rescue")
                    rescue_created_ts = int(time.time())
                    original_signal_ts = row["signal_ts"] or row["baseline_ts"] or row["first_tradable_ts"] or rescue_created_ts
                    if reason.startswith("tracking_ttl"):
                        source_type = "ttl_rescue_fast"
                    elif "kline" in reason:
                        source_type = "kline_retry_reclaim_fast"
                    elif "spread" in reason:
                        source_type = "spread_recovery_fast"
                    elif "quote" in reason:
                        source_type = "missing_quote_recovery_fast"
                    else:
                        source_type = "stale_refresh_fast"
                    payload = {
                        "missed_attribution_id": row["id"],
                        "signal_id": row["signal_id"],
                        "original_signal_ts": original_signal_ts,
                        "original_receive_ts": original_signal_ts,
                        "first_tradable_ts": row["first_tradable_ts"],
                        "rescue_created_ts": rescue_created_ts,
                        "route": row["route"],
                        "component": row["component"],
                        "reject_reason": reason,
                        "executable_peak_pnl": row["executable_peak_pnl"],
                    }
                    policy_probe = {
                        "entry_branch": reason,
                        "source_type": source_type,
                        "payload_json": json.dumps(payload),
                    }
                    policy = direct_fill_policy(policy_probe)
                    if not policy.get("pass"):
                        inserted = record_fast_lane_observation(
                            db,
                            source_type=source_type,
                            token_ca=row["token_ca"],
                            symbol=row["symbol"],
                            signal_ts=original_signal_ts,
                            receive_ts=original_signal_ts,
                            recorded_ts=row["baseline_ts"],
                            entry_mode_hint="pre_pass_resonance_tiny_probe",
                            entry_branch=reason,
                            trigger_price=row["baseline_price"],
                            priority=35,
                            payload=payload,
                            status=policy.get("status") or "counterfactual_only",
                            reason=policy.get("reason"),
                        )
                        if inserted:
                            log.info("[FAST_COUNTERFACTUAL] missed-rescue token=%s reason=%s policy=%s", row["token_ca"], reason, policy.get("reason"))
                        continue
                    inserted = enqueue_fast_entry(
                        db,
                        source_type=source_type,
                        token_ca=row["token_ca"],
                        symbol=row["symbol"],
                        signal_ts=original_signal_ts,
                        receive_ts=original_signal_ts,
                        recorded_ts=row["baseline_ts"],
                        entry_mode_hint="pre_pass_resonance_tiny_probe",
                        entry_branch=reason,
                        trigger_price=row["baseline_price"],
                        priority=35,
                        payload=payload,
                    )
                    if inserted:
                        log.info("[FAST_QUEUE] missed-rescue token=%s reason=%s", row["token_ca"], reason)
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
    with ThreadPoolExecutor(max_workers=args.concurrency + 3, thread_name_prefix="paper-fast") as pool:
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
    parser.add_argument("--concurrency", type=int, default=int(os.environ.get("FAST_ENTRY_WORKER_CONCURRENCY", "4")))
    parser.add_argument("--lookback-sec", type=int, default=int(os.environ.get("FAST_ENTRY_BOOT_LOOKBACK_SEC", "120")))
    parser.add_argument("--lock-file", default=os.environ.get("PAPER_FAST_LANE_LOCK_FILE", "/tmp/paper_fast_lane.lock"))
    args = parser.parse_args()
    run_worker(args)


if __name__ == "__main__":
    main()
