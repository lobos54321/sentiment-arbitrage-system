#!/usr/bin/env python3
# Canonical paper active path owner: this monitor orchestrates paper lifecycle state and delegates exit evaluation
# through scripts/execution_bridge.js -> src/execution/paper-live-position-monitor.js.
"""
Paper Trade Monitor — forward-test NOT_ATH signals with staged lifecycle execution.

NOT_ATH Strategy:
  - Stage 1: super > 80
  - Stage 1 exit: SL=-3%, trail@+2%/0.90, timeout=120min
  - Stage 2A: after stage1 stop-loss, wait 3 bars and re-enter on +18% rebound from post-stop rolling low
  - Stage 3: event-driven re-entry after qualifying stage1/stage2A close and later same-token awakening signal

Monitors premium_signals for new entries, enters at live price via GeckoTerminal,
tracks staged lifecycle positions, records results to paper_trades.db.

Usage:
    python3 scripts/paper_trade_monitor.py              # live monitor
    python3 scripts/paper_trade_monitor.py --dry-run    # dry run from recent signals
    python3 scripts/paper_trade_monitor.py --stats      # print daily stats
"""

import sqlite3
import json
import re
import time
import subprocess
import sys
import os
import signal
import logging
import urllib.request
import urllib.parse
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

from watchlist_store import WatchlistStore
from matrix_evaluator import MatrixEvaluator, ExitMatrixEvaluator
from entry_engine import (
    calculate_kelly_position, evaluate_smart_entry,
    fetch_dexscreener_trend_snapshot, evaluate_trend_phase,
    evaluate_entry_position, clear_dex_trend_cache,
    get_liquidity_position_cap, get_adaptive_stop_loss,
    KELLY_BASE_CAPITAL_SOL, KELLY_BASE_WIN_RATE, KELLY_BASE_ODDS, KELLY_COLD_START_ODDS,
    SMART_ENTRY_MAX_WAIT_SEC, SMART_ENTRY_POLL_INTERVAL_SEC,
)
from exit_engine import ExitGuardianThread, process_guardian_exits

try:
    import redis
except Exception:
    redis = None

# === Configuration ===
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
CONFIG_DIR = PROJECT_ROOT / 'config'
SENTIMENT_DB = os.environ.get('SENTIMENT_DB', str(DATA_DIR / 'sentiment_arb.db'))
PAPER_DB = os.environ.get('PAPER_DB', str(DATA_DIR / 'paper_trades.db'))
KLINE_DB = os.environ.get('KLINE_DB', str(DATA_DIR / 'kline_cache.db'))
REGISTRY_JSON = os.environ.get('PAPER_STRATEGY_REGISTRY', str(CONFIG_DIR / 'paper-strategy-registry.json'))
REMOTE_SIGNAL_URL = os.environ.get('REMOTE_SIGNAL_URL', '').strip()
REMOTE_SIGNAL_TOKEN = os.environ.get('REMOTE_SIGNAL_TOKEN', '').strip()
REMOTE_SIGNAL_LOOKBACK = max(50, int(os.environ.get('REMOTE_SIGNAL_LOOKBACK', '500')))
EXECUTION_BRIDGE = PROJECT_ROOT / 'scripts' / 'execution_bridge.js'

DEFAULT_STRATEGY_ID = 'notath-selective-v1'
DEFAULT_STRATEGY_ROLE = 'selective_challenger'
DEFAULT_STAGE1_EXIT = {'stopLossPct': 7.5, 'trailStartPct': 15, 'trailFactor': 0.6, 'timeoutMinutes': 120}

DEFAULT_PAPER_EXECUTION = {
    'executionMode': 'parity',
    'entryPriceSource': 'quote',
    'exitPriceSource': 'quote',
    'paperUsesQuoteOnly': True,
    'applyPaperPenalty': True,
    'quoteTimeoutMs': 10000,
    'quoteRetries': 5,
    'maxQuoteAgeSec': 180,
    'noRouteFailureThreshold': 3,
    'noRouteTrapMinutes': 15,
    'tokenNotTradableFailureThreshold': 1,
    'tokenNotTradableTrapMinutes': 1,
}

# Backward-compatible defaults for code paths that still use legacy constants.
SL_PCT = -0.03
TRAIL_START = 0.02
TRAIL_FACTOR = 0.90
TIMEOUT_MIN = 120

# Polling intervals (seconds)
SIGNAL_POLL_INTERVAL = max(1, int(os.environ.get('SIGNAL_POLL_INTERVAL_SEC', '5')))        # check for new signals
POSITION_POLL_INTERVAL = max(1, int(os.environ.get('POSITION_POLL_INTERVAL_SEC', '2')))    # update open positions
MAIN_LOOP_TICK_SEC = max(0.5, float(os.environ.get('MAIN_LOOP_TICK_SEC', '1.0')))
DAILY_REPORT_HOUR = 0           # UTC hour for daily report
HEARTBEAT_INTERVAL_SEC = 300

# P6: Global reference for SmartEntry to check active holdings count
# Set by run_monitor() to a lambda that returns holdings count
_active_holdings_count = None
PENDING_ENTRY_BAR_LOOKBACK = max(8, int(os.environ.get('PENDING_ENTRY_BAR_LOOKBACK', '30')))
PENDING_ENTRY_DEBUG_INTERVAL_SEC = max(30, int(os.environ.get('PENDING_ENTRY_DEBUG_INTERVAL_SEC', '120')))
PENDING_ENTRY_BAR_TOLERANCE_SEC = max(30, int(os.environ.get('PENDING_ENTRY_BAR_TOLERANCE_SEC', '90')))
PENDING_ENTRY_NEAREST_PAST_MAX_SEC = max(60, int(os.environ.get('PENDING_ENTRY_NEAREST_PAST_MAX_SEC', '180')))
MAX_EVALS_PER_CYCLE = max(1, int(os.environ.get('MAX_EVALS_PER_CYCLE', '8')))
NO_ROUTE_TRAP_FAILURES = max(1, int(os.environ.get('NO_ROUTE_TRAP_FAILURES', '3')))
NO_ROUTE_TRAP_MINUTES = max(1, int(os.environ.get('NO_ROUTE_TRAP_MINUTES', '15')))
TRAPPED_NO_ROUTE_PNL_PCT = float(os.environ.get('TRAPPED_NO_ROUTE_PNL_PCT', '-1.0'))
ENTRY_QUOTE_MAX_ATTEMPTS = max(1, int(os.environ.get('ENTRY_QUOTE_MAX_ATTEMPTS', '5')))
ENTRY_QUOTE_MAX_AGE_SEC = max(30, int(os.environ.get('ENTRY_QUOTE_MAX_AGE_SEC', '180')))

REDIS_URL = os.environ.get('REDIS_URL', '').strip()
REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1').strip()
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
REDIS_DB = int(os.environ.get('REDIS_DB', '0'))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', '').strip() or None
REDIS_KEY_PREFIX = os.environ.get('PRICE_REDIS_KEY_PREFIX', 'live_price:').strip() or 'live_price:'
LIVE_PRICE_MAX_AGE_MS = int(os.environ.get('LIVE_PRICE_MAX_AGE_MS', '90000'))
DEX_RATE_LIMIT_COOLDOWN_SEC = int(os.environ.get('DEX_RATE_LIMIT_COOLDOWN_SEC', '60'))
SOL_PRICE_TTL_SEC = int(os.environ.get('SOL_PRICE_TTL_SEC', '30'))
MARKET_DATA_UNIFIED_ROLLOUT = os.environ.get('MARKET_DATA_UNIFIED_ROLLOUT', 'true').lower() != 'false'
MARKET_DATA_UNIFIED_PAPER_MONITOR = os.environ.get('MARKET_DATA_UNIFIED_PAPER_MONITOR', 'true').lower() != 'false'
MARKET_DATA_SHARED_POOL_RESOLUTION = os.environ.get('MARKET_DATA_SHARED_POOL_RESOLUTION', 'true').lower() != 'false'
MARKET_DATA_SHARED_OHLCV = os.environ.get('MARKET_DATA_SHARED_OHLCV', 'true').lower() != 'false'
MARKET_DATA_SHARED_QUOTES = os.environ.get('MARKET_DATA_SHARED_QUOTES', 'true').lower() != 'false'
MARKET_DATA_SHARED_REDIS_CACHE = os.environ.get('MARKET_DATA_SHARED_REDIS_CACHE', 'false').lower() == 'true'
MARKET_DATA_PAPER_DIRECT_FALLBACK = os.environ.get('MARKET_DATA_PAPER_DIRECT_FALLBACK', 'true').lower() != 'false'

_REDIS_CLIENT = None
_REDIS_INIT_ATTEMPTED = False
_DEX_RATE_LIMIT_UNTIL = 0.0
_DEX_LAST_WARN_AT = 0.0
_SOL_PRICE_CACHE = {'price': None, 'fetched_at': 0.0}
_KLINE_DB_CONN = None
_KLINE_DB_FAILED = False
_SHARED_MARKET_DATA_RUNTIME = {}
_SHARED_POOL_CACHE = {}
_SHARED_SOL_PRICE_CACHE = {'price': None, 'fetched_at': 0.0}


def get_kline_db():
    global _KLINE_DB_CONN, _KLINE_DB_FAILED
    if _KLINE_DB_CONN is not None:
        return _KLINE_DB_CONN
    if _KLINE_DB_FAILED:
        return None
    init_fn = globals().get('init_kline_db')
    if not callable(init_fn):
        return None
    try:
        _KLINE_DB_CONN = init_fn()
        return _KLINE_DB_CONN
    except Exception as e:
        _KLINE_DB_FAILED = True
        logging.getLogger('paper_trade').warning(f"Failed to open kline cache DB: {e}")
        return None

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('paper_trade')


# === Strategy Config ===

def _deep_merge(base, override):
    result = dict(base or {})
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_active_strategy_config():
    base = {
        'strategyId': DEFAULT_STRATEGY_ID,
        'strategyRole': DEFAULT_STRATEGY_ROLE,
        'entryTimingFilters': {'minSuperIndex': 80},
        'paperRiskCaps': {'maxPositions': 5, 'positionSizeSol': 0.06},
        'paperExecution': dict(DEFAULT_PAPER_EXECUTION),
        'stageRules': {
            'stage1Exit': dict(DEFAULT_STAGE1_EXIT),
        },
    }
    try:
        with open(REGISTRY_JSON, 'r', encoding='utf-8') as f:
            registry = json.load(f)
        candidates = registry.get('candidates') or {}
        candidate_id = registry.get('activeChallengerId') or registry.get('activeBaselineId') or DEFAULT_STRATEGY_ID
        candidate = candidates.get(candidate_id) or candidates.get(DEFAULT_STRATEGY_ID)
        if not candidate:
            return base
        strategy_config = candidate.get('strategyConfig') or {}
        merged = _deep_merge(base, strategy_config)
        merged['strategyId'] = candidate.get('id') or candidate_id
        merged['strategyRole'] = 'active_challenger' if registry.get('activeChallengerId') == merged['strategyId'] else DEFAULT_STRATEGY_ROLE
        return merged
    except Exception as e:
        log.warning(f"Failed to load strategy registry {REGISTRY_JSON}: {e}")
        return base


def pct_to_decimal(value):
    try:
        return float(value) / 100.0
    except Exception:
        return 0.0


def get_paper_position_size_sol(strategy_config):
    caps = (strategy_config or {}).get('paperRiskCaps') or {}
    try:
        size = float(caps.get('positionSizeSol', 0.06))
    except Exception:
        size = 0.06
    return size if size > 0 else 0.06


# ─── Kelly Criterion Position Sizing ─────────────────────────────────────────
# Base capital: 5 SOL total
# Base win rate: 30% (empirical: ~5/17 signal-bearing coins hit 30%+ in 6h audit)
# Base odds: 5x (avg winner ~150% / avg loser ~15% stop loss)

# Kelly constants and cache moved to entry_engine.py
# Re-exported above via: from entry_engine import KELLY_BASE_CAPITAL_SOL, ...

# _get_historical_odds and calculate_kelly_position moved to entry_engine.py


def get_paper_max_positions(strategy_config):
    override = os.environ.get('PAPER_MAX_POSITIONS_OVERRIDE')
    if override is not None and str(override).strip() != '':
        try:
            return max(1, int(override))
        except Exception:
            pass
    caps = (strategy_config or {}).get('paperRiskCaps') or {}
    try:
        max_positions = int(caps.get('maxPositions', 5))
    except Exception:
        max_positions = 5
    return max(1, max_positions)


def get_paper_execution_config(strategy_config):
    execution = dict(DEFAULT_PAPER_EXECUTION)
    execution.update(((strategy_config or {}).get('paperExecution') or {}))
    execution['quoteRetries'] = max(1, _safe_int(execution.get('quoteRetries'), DEFAULT_PAPER_EXECUTION['quoteRetries']))
    execution['quoteTimeoutMs'] = max(1000, _safe_int(execution.get('quoteTimeoutMs'), DEFAULT_PAPER_EXECUTION['quoteTimeoutMs']))
    execution['maxQuoteAgeSec'] = max(1, _safe_int(execution.get('maxQuoteAgeSec'), DEFAULT_PAPER_EXECUTION['maxQuoteAgeSec']))
    execution['noRouteFailureThreshold'] = max(1, _safe_int(execution.get('noRouteFailureThreshold'), DEFAULT_PAPER_EXECUTION['noRouteFailureThreshold']))
    execution['noRouteTrapMinutes'] = max(1, _safe_int(execution.get('noRouteTrapMinutes'), DEFAULT_PAPER_EXECUTION['noRouteTrapMinutes']))
    execution['tokenNotTradableFailureThreshold'] = max(1, _safe_int(execution.get('tokenNotTradableFailureThreshold'), DEFAULT_PAPER_EXECUTION['tokenNotTradableFailureThreshold']))
    execution['tokenNotTradableTrapMinutes'] = max(1, _safe_int(execution.get('tokenNotTradableTrapMinutes'), DEFAULT_PAPER_EXECUTION['tokenNotTradableTrapMinutes']))
    execution['applyPaperPenalty'] = bool(execution.get('applyPaperPenalty', True))
    execution['paperUsesQuoteOnly'] = bool(execution.get('paperUsesQuoteOnly', True))
    execution['executionMode'] = str(execution.get('executionMode') or 'parity')
    execution['entryPriceSource'] = str(execution.get('entryPriceSource') or 'quote')
    execution['exitPriceSource'] = str(execution.get('exitPriceSource') or 'quote')
    return execution


def build_execution_audit(execution=None, extra=None):
    payload = execution if isinstance(execution, dict) else {}
    audit = {
        'mode': payload.get('mode'),
        'side': payload.get('side'),
        'success': payload.get('success'),
        'routeAvailable': payload.get('routeAvailable'),
        'requestId': payload.get('requestId'),
        'quotedOutAmount': _safe_float(payload.get('quotedOutAmount'), None),
        'quotedOutAmountRaw': payload.get('quotedOutAmountRaw'),
        'effectivePrice': _safe_float(payload.get('effectivePrice'), None),
        'slippageBps': _safe_float(payload.get('slippageBps'), None),
        'quoteTs': _safe_int(payload.get('quoteTs'), None),
        'feeEstimate': _safe_float(payload.get('feeEstimate'), None),
        'failureReason': payload.get('failureReason'),
        'txHash': payload.get('txHash'),
        'actualAmountOut': _safe_float(payload.get('actualAmountOut'), None),
        'actualAmountOutRaw': payload.get('actualAmountOutRaw'),
        'inputAmount': _safe_float(payload.get('inputAmount'), None),
        'inputAmountRaw': payload.get('inputAmountRaw'),
        'inputMint': payload.get('inputMint'),
        'outputMint': payload.get('outputMint'),
        'inputDecimals': _safe_int(payload.get('inputDecimals'), None),
        'outputDecimals': _safe_int(payload.get('outputDecimals'), None),
        'tokenCA': payload.get('tokenCA'),
        'penaltyApplied': payload.get('penaltyApplied'),
        'penaltyBps': _safe_int(payload.get('penaltyBps'), None),
        'penaltyBreakdown': payload.get('penaltyBreakdown') if isinstance(payload.get('penaltyBreakdown'), dict) else None,
        'rawQuotedOutAmount': _safe_float(payload.get('rawQuotedOutAmount'), None),
        'rawQuotedOutAmountRaw': payload.get('rawQuotedOutAmountRaw'),
        'rawEffectivePrice': _safe_float(payload.get('rawEffectivePrice'), None),
        'rawFeeEstimate': _safe_float(payload.get('rawFeeEstimate'), None),
    }
    if isinstance(extra, dict):
        for key, value in extra.items():
            audit[key] = value
    return {key: value for key, value in audit.items() if value is not None}


import threading
import urllib.request
import urllib.error
import socket
import time

def _post_json(url, json_payload, timeout_sec):
    data = json.dumps(json_payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as response:
            return response.status, json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode('utf-8'))
        except Exception:
            return e.code, {}
    except urllib.error.URLError as e:
        if isinstance(e.reason, socket.timeout):
            raise TimeoutError()
        raise e

class PersistentExecutionBridge:
    def __init__(self):
        self._proc = None
        self._lock = threading.Lock()
        
    def _start_if_needed(self):
        with self._lock:
            try:
                status, _ = _post_json("http://127.0.0.1:38942", {"_command": "ping"}, 0.1)
                if status in [200, 405, 500]:
                    return
            except Exception:
                pass
            
            if self._proc is None or self._proc.poll() is not None:
                env = os.environ.copy()
                self._proc = subprocess.Popen(
                    ['node', str(EXECUTION_BRIDGE), 'daemon'],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    cwd=str(PROJECT_ROOT),
                    env=env
                )
                
            for _ in range(50):
                try:
                    status, _ = _post_json("http://127.0.0.1:38942", {"_command": "ping"}, 0.5)
                    if status in [200, 405, 500]:
                        return
                except Exception:
                    pass
                time.sleep(0.1)
            
    def call(self, command, payload, timeout=10):
        self._start_if_needed()
        req = {"_command": command, "payload": payload}
        try:
            actual_timeout = timeout
            if command in ['quote-buy', 'quote-sell', 'simulate-buy', 'simulate-sell']:
                actual_timeout = max(timeout, 30)
                
            _, data = _post_json("http://127.0.0.1:38942", req, actual_timeout)
            return data
        except TimeoutError:
            return {'success': False, 'failureReason': 'daemon_timeout'}
        except Exception as e:
            return {'success': False, 'failureReason': f'daemon_request_failed:{e}'}

_daemon_bridge = PersistentExecutionBridge()

def call_execution_bridge(command, payload, timeout=10):
    return _daemon_bridge.call(command, payload, timeout)


def simulate_entry_execution(token_ca, amount_sol, stage_name, strategy_id=None, lifecycle_id=None):
    return call_execution_bridge('simulate-buy', {
        'mode': 'paper',
        'tokenCA': token_ca,
        'amountSol': amount_sol,
        'options': {
            'stage': stage_name,
            'strategyId': strategy_id,
            'lifecycleId': lifecycle_id,
        }
    })


def simulate_exit_execution(token_ca, token_amount_raw, token_decimals, stage_name, strategy_id=None, lifecycle_id=None):
    return call_execution_bridge('simulate-sell', {
        'mode': 'paper',
        'tokenCA': token_ca,
        'tokenAmountRaw': int(token_amount_raw),
        'options': {
            'stage': stage_name,
            'strategyId': strategy_id,
            'lifecycleId': lifecycle_id,
            'inputAmount': (int(token_amount_raw) / (10 ** int(token_decimals or 0))) if token_decimals is not None else None,
        }
    })


def evaluate_paper_exit(position_payload, mark_payload):
    return call_execution_bridge('evaluate-paper-exit', {
        'position': position_payload,
        'mark': mark_payload,
    }, timeout=20)


def parse_monitor_state(monitor_state_json):
    if isinstance(monitor_state_json, dict):
        return monitor_state_json
    if not monitor_state_json:
        return None
    try:
        return json.loads(monitor_state_json)
    except Exception:
        return None


def monitor_peak_pnl_decimal(monitor_state, fallback=0.0):
    if not isinstance(monitor_state, dict):
        return float(fallback or 0.0)
    if monitor_state.get('highPnl') is not None:
        try:
            return float(monitor_state.get('highPnl')) / 100.0
        except Exception:
            pass
    if monitor_state.get('peakPnl') is not None:
        try:
            return float(monitor_state.get('peakPnl'))
        except Exception:
            pass
    return float(fallback or 0.0)


def sync_position_from_monitor_state(pos, allow_token_amount_override=False):
    state = pos.monitor_state or {}
    pos.peak_pnl = monitor_peak_pnl_decimal(state, pos.peak_pnl)
    pos.trailing_active = bool(state.get('breakeven', state.get('trailingActive', pos.trailing_active)))
    pos.bars_held = int(state.get('barsHeld', pos.bars_held) or pos.bars_held)
    pos.last_mark_ts = int(state.get('lastMarkTs', pos.last_mark_ts) or pos.last_mark_ts)
    pos.last_bar_ts = pos.last_mark_ts or pos.last_bar_ts
    if allow_token_amount_override:
        pos.token_amount_raw = int(state.get('tokenAmount', pos.token_amount_raw) or pos.token_amount_raw)
    elif pos.token_amount_raw:
        state['tokenAmount'] = int(pos.token_amount_raw)
        state['tokenDecimals'] = int(pos.token_decimals or state.get('tokenDecimals') or 0)


def lifecycle_realized_pnl_from_state(state, fallback_position_size_sol=0.0, final_exit_sol=None, has_partial_history=False):
    monitor_state = state if isinstance(state, dict) else {}
    entry_sol = _safe_float(monitor_state.get('entrySol'), fallback_position_size_sol)
    total_sol_received = _safe_float(monitor_state.get('totalSolReceived'), None)
    final_exit_total_sol = _safe_float(final_exit_sol, None)

    if entry_sol <= 0:
        accounting_source = 'monitor_state_total_sol_received' if has_partial_history else 'final_exit_only'
        return None, total_sol_received if has_partial_history else final_exit_total_sol, entry_sol, accounting_source

    if has_partial_history:
        # BUG FIX: When partial history exists, totalSolReceived accumulates ALL previous
        # partial exit amounts across the lifecycle. Using it for pnl_pct would double-count
        # previous exits and produce wildly inflated PnL numbers (e.g. +86% on a loser).
        # The correct approach: compute pnl only from this final exit's received SOL vs entry.
        if final_exit_total_sol is not None and entry_sol > 0:
            pnl = (final_exit_total_sol - entry_sol) / entry_sol
            return pnl, final_exit_total_sol, entry_sol, 'final_exit_vs_entry_sol'
        # Fallback: if we have no final exit amount, report as unknown
        if total_sol_received is None:
            return None, total_sol_received, entry_sol, 'monitor_state_total_sol_received'
        return ((total_sol_received - entry_sol) / entry_sol), total_sol_received, entry_sol, 'monitor_state_total_sol_received'

    if final_exit_total_sol is not None:
        return ((final_exit_total_sol - entry_sol) / entry_sol), final_exit_total_sol, entry_sol, 'final_exit_only'
    if total_sol_received is not None:
        return ((total_sol_received - entry_sol) / entry_sol), total_sol_received, entry_sol, 'monitor_state_total_sol_received'
    return None, final_exit_total_sol, entry_sol, 'final_exit_only'



def compute_exit_debug_fields(exit_rules, pos, trigger_pnl):
    trigger_pct = _safe_float(trigger_pnl * 100.0, None)
    peak_pct = _safe_float(pos.peak_pnl * 100.0, None)
    stop_loss_pct = -abs(_safe_float(exit_rules.get('stopLossPct'), 0.0))
    trail_active = bool(pos.trailing_active)
    trail_floor_pct = None
    if trail_active and peak_pct is not None:
        trail_floor_pct = max(0.0, peak_pct * _safe_float(exit_rules.get('trailFactor'), 0.9))
    return {
        'trigger_pct': trigger_pct,
        'peak_pct': peak_pct,
        'stop_loss_pct': stop_loss_pct,
        'trail_active': trail_active,
        'trail_floor_pct': trail_floor_pct,
    }


# === Database Setup ===

def init_paper_db(db_path=None):
    """Create paper_trades table if not exists."""
    path = db_path or PAPER_DB
    os.makedirs(os.path.dirname(path), exist_ok=True)
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id TEXT DEFAULT 'notath-selective-v1',
            strategy_role TEXT DEFAULT 'selective_challenger',
            strategy_stage TEXT DEFAULT 'stage1',
            stage_outcome TEXT,
            token_ca TEXT NOT NULL,
            symbol TEXT,
            signal_ts INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            entry_ts INTEGER NOT NULL,
            exit_price REAL,
            exit_ts INTEGER,
            exit_reason TEXT,
            pnl_pct REAL,
            bars_held INTEGER,
            market_regime TEXT,
            replay_source TEXT DEFAULT 'live_monitor',
            peak_pnl REAL DEFAULT 0,
            trailing_active INTEGER DEFAULT 0,
            position_size_sol REAL,
            token_amount_raw TEXT,
            token_decimals INTEGER DEFAULT 0,
            entry_execution_json TEXT,
            exit_execution_json TEXT,
            monitor_state_json TEXT,
            entry_execution_audit_json TEXT,
            exit_execution_audit_json TEXT,
            exit_quote_failures INTEGER DEFAULT 0,
            last_exit_quote_failure TEXT,
            premium_signal_id INTEGER,
            signal_type TEXT,
            strategy_outcome TEXT,
            execution_availability TEXT,
            accounting_outcome TEXT,
            synthetic_close INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_pt_token ON paper_trades(token_ca)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_pt_exit ON paper_trades(exit_reason)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_pt_entry_ts ON paper_trades(entry_ts)")
    for column_sql in [
        "ALTER TABLE paper_trades ADD COLUMN strategy_id TEXT DEFAULT 'notath-selective-v1'",
        "ALTER TABLE paper_trades ADD COLUMN strategy_role TEXT DEFAULT 'selective_challenger'",
        "ALTER TABLE paper_trades ADD COLUMN strategy_stage TEXT DEFAULT 'stage1'",
        "ALTER TABLE paper_trades ADD COLUMN stage_outcome TEXT",
        "ALTER TABLE paper_trades ADD COLUMN replay_source TEXT DEFAULT 'live_monitor'",
        "ALTER TABLE paper_trades ADD COLUMN lifecycle_id TEXT",
        "ALTER TABLE paper_trades ADD COLUMN parent_trade_id INTEGER",
        "ALTER TABLE paper_trades ADD COLUMN stage_seq INTEGER",
        "ALTER TABLE paper_trades ADD COLUMN trigger_ts INTEGER",
        "ALTER TABLE paper_trades ADD COLUMN trigger_price REAL",
        "ALTER TABLE paper_trades ADD COLUMN armed_ts INTEGER",
        "ALTER TABLE paper_trades ADD COLUMN first_peak_pct REAL",
        "ALTER TABLE paper_trades ADD COLUMN rolling_low_price REAL",
        "ALTER TABLE paper_trades ADD COLUMN rolling_low_ts INTEGER",
        "ALTER TABLE paper_trades ADD COLUMN reentry_source TEXT",
        "ALTER TABLE paper_trades ADD COLUMN stage3_peak_price REAL",
        "ALTER TABLE paper_trades ADD COLUMN stage3_qualifying_exit_ts INTEGER",
        "ALTER TABLE paper_trades ADD COLUMN stage3_dormant INTEGER DEFAULT 0",
        "ALTER TABLE paper_trades ADD COLUMN stage3_blacklisted INTEGER DEFAULT 0",
        "ALTER TABLE paper_trades ADD COLUMN position_size_sol REAL",
        "ALTER TABLE paper_trades ADD COLUMN token_amount_raw TEXT",
        "ALTER TABLE paper_trades ADD COLUMN token_decimals INTEGER DEFAULT 0",
        "ALTER TABLE paper_trades ADD COLUMN entry_execution_json TEXT",
        "ALTER TABLE paper_trades ADD COLUMN exit_execution_json TEXT",
        "ALTER TABLE paper_trades ADD COLUMN monitor_state_json TEXT",
        "ALTER TABLE paper_trades ADD COLUMN entry_execution_audit_json TEXT",
        "ALTER TABLE paper_trades ADD COLUMN exit_execution_audit_json TEXT",
        "ALTER TABLE paper_trades ADD COLUMN exit_quote_failures INTEGER DEFAULT 0",
        "ALTER TABLE paper_trades ADD COLUMN last_exit_quote_failure TEXT",
        "ALTER TABLE paper_trades ADD COLUMN premium_signal_id INTEGER",
        "ALTER TABLE paper_trades ADD COLUMN signal_type TEXT",
        "ALTER TABLE paper_trades ADD COLUMN strategy_outcome TEXT",
        "ALTER TABLE paper_trades ADD COLUMN execution_availability TEXT",
        "ALTER TABLE paper_trades ADD COLUMN accounting_outcome TEXT",
        "ALTER TABLE paper_trades ADD COLUMN synthetic_close INTEGER DEFAULT 0",
    ]:
        try:
            db.execute(column_sql)
        except sqlite3.OperationalError:
            pass
    try:
        db.execute("CREATE INDEX IF NOT EXISTS idx_pt_lifecycle_stage ON paper_trades(token_ca, signal_ts, strategy_stage)")
    except sqlite3.OperationalError:
        pass
    db.commit()
    return db


# === Price Fetching ===

def _is_dexscreener_url(url):
    return 'api.dexscreener.com' in (url or '')


def _dex_rate_limited():
    return time.time() < _DEX_RATE_LIMIT_UNTIL


def _mark_dex_rate_limited(source, detail=''):
    global _DEX_RATE_LIMIT_UNTIL, _DEX_LAST_WARN_AT
    now = time.time()
    _DEX_RATE_LIMIT_UNTIL = now + DEX_RATE_LIMIT_COOLDOWN_SEC
    if now - _DEX_LAST_WARN_AT >= 15:
        suffix = f": {detail}" if detail else ''
        log.warning(f"DexScreener rate-limited; cooling down for {DEX_RATE_LIMIT_COOLDOWN_SEC}s ({source}){suffix}")
        _DEX_LAST_WARN_AT = now


def market_data_unified_enabled():
    return MARKET_DATA_UNIFIED_ROLLOUT and MARKET_DATA_UNIFIED_PAPER_MONITOR


def shared_truth_source_enabled(feature_name):
    if not market_data_unified_enabled():
        return False
    if feature_name == 'pool':
        return MARKET_DATA_SHARED_POOL_RESOLUTION
    if feature_name == 'ohlcv':
        return MARKET_DATA_SHARED_OHLCV
    if feature_name == 'quotes':
        return MARKET_DATA_SHARED_QUOTES
    if feature_name == 'redis':
        return MARKET_DATA_SHARED_REDIS_CACHE
    return False


def direct_provider_fallback_allowed():
    return (not market_data_unified_enabled()) or MARKET_DATA_PAPER_DIRECT_FALLBACK


def get_shared_market_runtime(namespace='paper-monitor:bridge'):
    global _SHARED_MARKET_DATA_RUNTIME
    runtime = _SHARED_MARKET_DATA_RUNTIME.get(namespace)
    if runtime is not None:
        return runtime
    runtime = {'namespace': namespace}
    _SHARED_MARKET_DATA_RUNTIME[namespace] = runtime
    return runtime


def call_shared_runtime(method, payload=None, timeout=8, namespace='paper-monitor:bridge'):
    runtime = get_shared_market_runtime(namespace)
    bridge_payload = {
        'mode': 'paper',
        'method': method,
        'payload': payload or {},
        'namespace': runtime['namespace'],
    }
    try:
        res = call_execution_bridge('shared-runtime', bridge_payload, timeout)
        if isinstance(res, dict) and res.get('failureReason') and not res.get('success'):
            return None
        return res
    except Exception:
        return None


def get_shared_cache_value(key, namespace='paper-monitor:bridge'):
    if not shared_truth_source_enabled('redis'):
        return None
    return call_shared_runtime('getCache', {'key': key}, namespace=namespace)


def get_shared_quote_cache_value(key):
    if not (shared_truth_source_enabled('quotes') and shared_truth_source_enabled('redis')):
        return None
    return call_shared_runtime('getCache', {'key': key}, namespace='market-data:quotes')


def get_shared_cooldown_ms(provider, namespace='paper-monitor:bridge'):
    if not shared_truth_source_enabled('redis'):
        return 0
    try:
        return int(call_shared_runtime('getSharedCooldown', {'provider': provider}, namespace=namespace) or 0)
    except Exception:
        return 0


def get_shared_pool_resolution(token_ca):
    if not shared_truth_source_enabled('pool'):
        return None
    return call_shared_runtime('resolvePool', {'tokenCA': token_ca}, timeout=12)


def get_shared_recent_ohlcv(token_ca, pool_address, options=None):
    if not shared_truth_source_enabled('ohlcv'):
        return None
    return call_shared_runtime('fetchRecentOhlcvByPool', {
        'tokenCA': token_ca,
        'poolAddress': pool_address,
        'options': options or {}
    }, timeout=20)


def get_shared_swap_quote(token_ca, amount_raw, output_mint='So11111111111111111111111111111111111111112', options=None):
    if not shared_truth_source_enabled('quotes'):
        return None
    return call_shared_runtime('getSwapQuote', {
        'inputMint': token_ca,
        'amount': amount_raw,
        'outputMint': output_mint,
        'options': options or {}
    }, timeout=15)


def set_shared_cache_value(key, value, ttl_ms, namespace='paper-monitor:bridge'):
    if not shared_truth_source_enabled('redis'):
        return False
    result = call_shared_runtime('setCache', {'key': key, 'value': value, 'ttlMs': int(ttl_ms or 0)}, namespace=namespace)
    return bool(result)


def curl_json(url, timeout=15):
    """Fetch JSON via curl."""
    if _is_dexscreener_url(url):
        if _dex_rate_limited():
            return None
        if get_shared_cooldown_ms('dexscreener', namespace='market-data:quotes') > 0:
            return None

    try:
        result = subprocess.run(
            [
                'curl', '-sS', '-L', '-m', str(timeout),
                '-H', 'Accept: application/json',
                '-H', 'User-Agent: sentiment-arbitrage-system/1.0',
                url,
            ],
            capture_output=True, text=True, timeout=timeout + 5
        )
        if result.returncode != 0:
            stderr = (result.stderr or '').strip()
            if stderr:
                log.warning(f"Fetch failed for {url}: {stderr}")
            return None

        body = (result.stdout or '').strip()
        if not body:
            log.warning(f"Empty response from {url}")
            return None

        if body[0] not in '{[':
            preview = body[:160].replace('\n', ' ')
            if _is_dexscreener_url(url) and '1015' in preview:
                _mark_dex_rate_limited(url, preview)
                return None
            log.warning(f"Non-JSON response from {url}: {preview}")
            return None

        try:
            return json.loads(body)
        except json.JSONDecodeError as e:
            preview = body[:160].replace('\n', ' ')
            if _is_dexscreener_url(url) and '1015' in preview:
                _mark_dex_rate_limited(url, preview)
                return None
            log.warning(f"JSON parse failed for {url}: {e}; preview={preview}")
            return None
    except Exception as e:
        log.warning(f"Fetch exception for {url}: {e}")
        return None


def fetch_dexscreener_price_usd(token_ca, timeout=5):
    """Fetch current USD price from DexScreener. Returns (price_usd, timestamp_sec) or (None, None)."""
    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_ca}'
    data = curl_json(url, timeout=timeout)
    if not data or not isinstance(data, dict):
        return None, None
    pairs = data.get('pairs')
    if not pairs or not isinstance(pairs, list):
        return None, None
    # Use the first pair with a valid priceUsd
    for pair in pairs[:3]:
        price_str = pair.get('priceUsd')
        if price_str:
            try:
                price = float(price_str)
                if price > 0:
                    return price, int(time.time())
            except (ValueError, TypeError):
                continue
    return None, None


def fetch_dexscreener_m5(token_ca, timeout=5):
    """Fetch 5-minute price change % from DexScreener. Returns float or None."""
    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_ca}'
    data = curl_json(url, timeout=timeout)
    if not data or not isinstance(data, dict):
        return None
    pairs = data.get('pairs')
    if not pairs or not isinstance(pairs, list):
        return None
    for pair in pairs[:3]:
        price_change = pair.get('priceChange', {})
        m5 = price_change.get('m5')
        if m5 is not None:
            try:
                return float(m5)
            except (ValueError, TypeError):
                continue
    return None


def fetch_dexscreener_volume(token_ca, timeout=5):
    """Fetch 24h volume and transaction count from DexScreener.
    Returns dict {volume_usd, txns} or None.
    """
    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_ca}'
    data = curl_json(url, timeout=timeout)
    if not data or not isinstance(data, dict):
        return None
    pairs = data.get('pairs')
    if not pairs or not isinstance(pairs, list):
        return None
    best = _select_best_dex_pair(token_ca, pairs)
    if not best:
        best = pairs[0]
    volume = best.get('volume', {})
    txns = best.get('txns', {})
    vol_h24 = volume.get('h24', 0) or 0
    buys = (txns.get('h24', {}) or {}).get('buys', 0) or 0
    sells = (txns.get('h24', {}) or {}).get('sells', 0) or 0
    total_txns = buys + sells
    try:
        return {'volume_usd': float(vol_h24), 'txns': int(total_txns)}
    except (ValueError, TypeError):
        return None


# ─── Social Signal Fetcher ────────────────────────────────────────────────────
# Calls the social_signal_service.py microservice running on Zeabur.
# Returns Twitter mention count + DexScreener boost status.
# Service URL configured via SOCIAL_SERVICE_URL env var.
# If service unavailable, returns None gracefully (never blocks trading).

SOCIAL_SERVICE_URL = os.environ.get('SOCIAL_SERVICE_URL', 'http://localhost:8765')
_social_signal_cache = {}  # {ca: (result, expire_ts)}
SOCIAL_SIGNAL_CACHE_SEC = 120  # 2-minute cache


def fetch_social_signals(token_ca, symbol='', timeout=5):
    """Fetch social propagation signals from the social_signal_service.

    Returns dict:
      {twitter_mentions, twitter_unique_authors, twitter_engagement,
       dex_has_boost, dex_boost_amount, dex_has_profile, social_score}
    Returns None if service unavailable (non-blocking).
    """
    now = time.time()
    cached = _social_signal_cache.get(token_ca)
    if cached and now < cached[1]:
        return cached[0]

    try:
        url = f"{SOCIAL_SERVICE_URL}/social?ca={token_ca}&symbol={symbol}"
        req = urllib.request.Request(url)
        resp = urllib.request.urlopen(req, timeout=timeout)
        data = json.loads(resp.read())
        _social_signal_cache[token_ca] = (data, now + SOCIAL_SIGNAL_CACHE_SEC)
        return data
    except Exception:
        # Service unavailable — fail silently, never block trading
        return None



# ─── Smart Entry Engine ──────────────────────────────────────────────────────
# Replaces the static Dip-then-Rip entry timing with a two-layer dynamic
# engine based on real-time market state.
#
# Layer 1 (Scheme B): DexScreener volume-price trend confirmation
#   → "Is this a REAL buying wave or a fake pump?"
# SmartEntry constants moved to entry_engine.py
# Re-exported above via: from entry_engine import SMART_ENTRY_MAX_WAIT_SEC, ...


# ExitGuardianThread moved to exit_engine.py
# SmartEntry functions (fetch_dexscreener_trend_snapshot, evaluate_trend_phase,
# evaluate_entry_position, evaluate_smart_entry) moved to entry_engine.py
# All re-exported above via: from entry_engine import ...; from exit_engine import ...


ENTRY_TIMING_INTERVAL_SEC = int(os.environ.get('ENTRY_TIMING_INTERVAL_SEC', '3'))
ENTRY_TIMING_MAX_ROUNDS = int(os.environ.get('ENTRY_TIMING_MAX_ROUNDS', '100'))
ENTRY_TIMING_BREAKOUT_PCT = float(os.environ.get('ENTRY_TIMING_BREAKOUT_PCT', '3.0'))
# Reject ascending_3 / breakout signals where the rally is already too far
# advanced inside the observation window — that's a parabolic blow-off top,
# not an early-momentum entry.
ENTRY_TIMING_FROM_BASE_MAX_PCT = float(os.environ.get('ENTRY_TIMING_FROM_BASE_MAX_PCT', '80.0'))
# Last-mile pre-buy recheck: between timing PASS and the buy quote we still
# pay ~15-30s of execution latency, during which the entry wick often tops.
# Refetch a real-time price right before submitting the buy and abort if it
# has already drifted more than this fraction above the timing s3 trigger.
ENTRY_PREBUY_RECHECK_MAX_PCT = float(os.environ.get('ENTRY_PREBUY_RECHECK_MAX_PCT', '8.0'))
# Max acceptable staleness for timing-engine snapshots in milliseconds.
# Must be fresh enough that our decisions track real market state, not
# DexScreener cache artifacts (observed 30%+ divergence between sources).
ENTRY_TIMING_SNAP_MAX_AGE_MS = int(os.environ.get('ENTRY_TIMING_SNAP_MAX_AGE_MS', '5000'))
# Concurrent timing evaluations: up to this many signals can be in the
# timing engine at once, so a slow 300s evaluation on one signal doesn't
# block the main loop from processing new signals.
TIMING_MAX_CONCURRENT = int(os.environ.get('TIMING_MAX_CONCURRENT', '50'))
_timing_executor = ThreadPoolExecutor(
    max_workers=TIMING_MAX_CONCURRENT, thread_name_prefix='timing'
)
# lifecycle_id -> {'future', 'ctx', 'submitted_at'}
_timing_inflight = {}
HELIUS_API_KEY = os.environ.get('HELIUS_API_KEY', '')
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# B+C: Helius volume polling cache for held positions
# trade_id → {'last_check': ts, 'last_sig': str, 'tps': float, 'tps_history': [float]}
_helius_vol_cache = {}

def poll_helius_volume(trade_id, pool_address, interval=30):
    """Poll Helius for real transaction frequency (TPS) of a held position.
    Returns smoothed TPS (average of last 3 readings) for stability.
    Cached per trade_id, re-fetches only after `interval` seconds.
    """
    now = time.time()
    cache = _helius_vol_cache.get(trade_id)

    if cache and (now - cache['last_check']) < interval:
        return cache.get('tps_smooth', cache.get('tps', 0.0))

    try:
        sigs = get_recent_signatures(pool_address, limit=30)
        if not sigs:
            raw_tps = 0.0
        elif cache and cache.get('last_sig'):
            new_count = next(
                (i for i, sig in enumerate(sigs) if sig == cache['last_sig']),
                len(sigs)
            )
            elapsed = now - cache['last_check']
            raw_tps = new_count / elapsed if elapsed > 0 else 0.0
        else:
            raw_tps = -1.0  # first call, no baseline yet

        # Multi-round smoothing: keep last 3 TPS readings, return average
        tps_history = (cache.get('tps_history') if cache else None) or []
        if raw_tps >= 0:
            tps_history.append(raw_tps)
        if len(tps_history) > 3:
            tps_history = tps_history[-3:]
        tps_smooth = sum(tps_history) / len(tps_history) if tps_history else 0.0

        _helius_vol_cache[trade_id] = {
            'last_check': now,
            'last_sig': sigs[0] if sigs else None,
            'tps': raw_tps,
            'tps_smooth': tps_smooth,
            'tps_history': tps_history,
        }
        return tps_smooth
    except Exception:
        return cache.get('tps_smooth', cache.get('tps', 0.0)) if cache else 0.0

def get_recent_signatures(token_ca, limit=100):
    """Fetch recent signatures using Helius RPC to count momentum"""
    if not HELIUS_API_KEY or 'your' in HELIUS_API_KEY.lower():
        return []
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [
            token_ca,
            {"limit": limit}
        ]
    }
    for attempt in range(3):
        try:
            # Short timeout: we only care about real-time speed. If it's slow, we skip it.
            status, res_data = _post_json(HELIUS_RPC_URL, payload, 2.5)
            if status == 200 and 'result' in res_data:
                return [item['signature'] for item in res_data['result']]
        except Exception:
            pass
        if attempt < 2:
            time.sleep(0.1)
    return []




def fetch_realtime_price(token_ca, pool_address, max_age_ms=ENTRY_TIMING_SNAP_MAX_AGE_MS, token_decimals=None):
    """
    Fetch a real-time SOL-denominated price for the timing/exit engines.

    Uses get_live_price_snapshot (Redis → shared Jupiter quote → DexScreener
    → direct) but enforces a strict freshness check: any snapshot older than
    max_age_ms is rejected. Returns (price, source, age_ms) or (None, None, None).

    Pass `token_decimals` when known (e.g. exit-monitor positions) so the
    Jupiter shared-quote source can convert SOL lamports → USD per token
    correctly. When unknown (entry timing on a brand-new signal), the helper
    falls back to assuming decimals=6 (the pump.fun default).
    """
    now_ms = int(time.time() * 1000)
    min_ts_ms = now_ms - max_age_ms
    snap = get_live_price_snapshot(token_ca, pool_address, min_timestamp_ms=min_ts_ms, token_decimals=token_decimals)
    if not snap:
        return None, None, None
    price = snap.get('price')
    ts_ms = snap.get('timestamp_ms') or 0
    age_ms = now_ms - ts_ms if ts_ms else None
    if not price or price <= 0:
        return None, None, None
    # Belt-and-suspenders: some sources (shared quote cache) don't honour
    # min_timestamp_ms inside get_live_price_snapshot — re-check here.
    if age_ms is not None and age_ms > max_age_ms:
        return None, None, age_ms
    return float(price), snap.get('source'), age_ms


def evaluate_entry_timing(token_ca, symbol='?', pool_address=None, strict_fail_open=False):
    """
    Entry timing engine v5 — Two-Phase "Dip-then-Rip" Sniper.

    Total window : 5 min  = 100 rounds × 3s each.
    Phase 1 budget: 2.5 min = 50 rounds — wait for a washout dip (>= -2%).
    Phase 2 budget: remaining rounds — wait for 3-step recovery from valley.

    Fail-Open: if Phase 1 exhausts with no dip, treat current price as valley
               and proceed to Phase 2 with the remaining 50 rounds.

    Buy triggers (Phase 2 only):
      (A) s3 > s2 > s1  — 3 ascending price steps from valley
      (B) s3 >= valley * 1.03 — 3% breakout above valley baseline
      + Helius TPS >= 2.0 confirmation

    Returns: (should_enter: bool, reason: str, detail: str, trigger_price)
    """
    interval          = ENTRY_TIMING_INTERVAL_SEC   # 3s
    max_rounds        = ENTRY_TIMING_MAX_ROUNDS      # 100
    phase1_max_rounds = max_rounds // 2              # 50 rounds = 2.5 min
    breakout_pct      = ENTRY_TIMING_BREAKOUT_PCT    # 3.0
    dip_threshold_pct = 2.0                          # require >= -2% dip

    initial_baseline = None
    valley_price     = None
    saw_dip          = False
    phase            = 1   # 1 = dip watch, 2 = rip watch

    snapshots = []
    sources   = []
    s1_sigs   = None
    s1_top_sig = None
    s1_time   = 0

    for round_i in range(max_rounds):
        if round_i > 0:
            time.sleep(interval)

        price, src, age_ms = fetch_realtime_price(token_ca, pool_address)
        if not price or price <= 0:
            if initial_baseline is None:
                return False, 'no_price', f'no fresh price (age_ms={age_ms})', None
            continue  # stale read — keep waiting

        sources.append(src or '?')

        # First good price initialises both baselines
        if initial_baseline is None:
            initial_baseline = price
            valley_price     = price
            log.info(f"  [ENTRY_TIMING] {symbol} Phase1 start baseline={initial_baseline:.10f}")

        # Always track the lowest price seen (updates valley in both phases)
        if price < valley_price:
            valley_price = price

        # ──────────────────────────────────────────────────
        # PHASE 1 — Wait for washout dip
        # ──────────────────────────────────────────────────
        if phase == 1:
            dip_pct = ((price - initial_baseline) / initial_baseline) * 100

            # Early-stop: if price craters >15% after 10+ rounds with no recovery, abort.
            # Saves ~2 min of wasted observation on dying tokens.
            if round_i >= 10 and dip_pct <= -15.0:
                log.info(
                    f"  [ENTRY_TIMING] {symbol} ❌ Early-stop: "
                    f"price cratering {dip_pct:+.1f}% after {round_i+1} rounds, aborting"
                )
                return False, 'early_stop', f'price cratering {dip_pct:+.1f}% (>{-15}%)', None

            if dip_pct <= -dip_threshold_pct:
                saw_dip = True
                log.info(
                    f"  [ENTRY_TIMING] {symbol} 🔻 Dip confirmed "
                    f"dip={dip_pct:+.2f}% price={price:.10f} "
                    f"round={round_i+1} → Phase 2"
                )
                phase = 2
                snapshots = [price]
                try:
                    s1_sigs    = get_recent_signatures(token_ca, limit=5)
                    s1_top_sig = s1_sigs[0] if s1_sigs else None
                    s1_time    = time.time()
                except Exception:
                    pass
                continue

            # Fail-Open after 2.5 min with no dip
            if round_i + 1 >= phase1_max_rounds:
                log.info(
                    f"  [ENTRY_TIMING] {symbol} ⚡ Fail-Open: "
                    f"no dip in {phase1_max_rounds} rounds, "
                    f"current={price:.10f} becomes valley"
                )
                valley_price = price
                phase        = 2
                snapshots    = [price]
                try:
                    s1_sigs    = get_recent_signatures(token_ca, limit=5)
                    s1_top_sig = s1_sigs[0] if s1_sigs else None
                    s1_time    = time.time()
                except Exception:
                    pass
                continue

            continue  # still waiting for dip

        # ──────────────────────────────────────────────────
        # PHASE 2 — Target-price crossing (valley + 5%)
        # ──────────────────────────────────────────────────
        # No ladder needed. Once Phase1 confirmed a dip, we watch for price
        # to cross valley × 1.05. The FIRST 3-second snapshot that reaches
        # the target fires immediately — catching the start of the bounce
        # (e.g. at valley+5%) not the top of it (e.g. at valley+27%).
        # Valley is updated downward in real-time so the target tracks the
        # true floor even if the coin dips further in Phase2.
        target_recovery_pct = 5.0
        phase2_target = valley_price * (1 + target_recovery_pct / 100)
        from_valley_pct = ((price - valley_price) / valley_price) * 100
        src_str = sources[-1] if sources else '?'

        # Blow-off guard — already ripped past the max, abort
        limit_pct = 80.0 if strict_fail_open else ENTRY_TIMING_FROM_BASE_MAX_PCT
        if from_valley_pct > limit_pct:
            detail = (
                f'blow_off: from_valley={from_valley_pct:+.2f}% > max {limit_pct}% '
                f'price={price:.10f} valley={valley_price:.10f} '
                f'src={src_str} round={round_i+1}'
            )
            log.info(f"  [ENTRY_TIMING] {symbol} SKIP: {detail}")
            return False, 'blow_off', detail, None

        # Not yet at target — keep watching
        if price < phase2_target:
            log.debug(
                f"  [ENTRY_TIMING] {symbol} Phase2 watching: "
                f"price={price:.10f} target={phase2_target:.10f} "
                f"({from_valley_pct:+.2f}% / needed +{target_recovery_pct}%) "
                f"round={round_i+1}"
            )
            continue

        # ✅ Target crossed: price >= valley + 5%
        buy_reason = 'valley_target_5pct' if saw_dip else 'failopen_target_5pct'

        # Helius TPS confirmation — ensure real buy pressure, not a single whale sweep
        elapsed = time.time() - s1_time
        if s1_top_sig and elapsed > 0:
            current_sigs = get_recent_signatures(token_ca, limit=100)
            new_tx_count = next(
                (i for i, sig in enumerate(current_sigs) if sig == s1_top_sig),
                len(current_sigs)
            )
            tps = new_tx_count / elapsed
            if tps < 2.0:
                detail = (
                    f"momentum_died: TPS {tps:.1f} too low "
                    f"({new_tx_count} txs in {elapsed:.1f}s, req >= 2.0) "
                    f"saw_dip={saw_dip} src={src_str} round={round_i+1}"
                )
                log.warning(f"  [ENTRY_TIMING] {symbol} BLOCKED: {detail}")
                return False, 'momentum_died', detail, None

        detail = (
            f'{buy_reason}: from_valley={from_valley_pct:+.2f}% '
            f'saw_dip={saw_dip} valley={valley_price:.10f} target={phase2_target:.10f} '
            f'price={price:.10f} src={src_str} round={round_i+1}'
        )
        log.info(f"  [ENTRY_TIMING] {symbol} ENTER: {detail}")
        return True, buy_reason, detail, price



    # ── Full 5-minute timeout ──────────────────────────────
    from_valley_pct_final = ((price - valley_price) / valley_price * 100
                              if valley_price and valley_price > 0 else 0)
    detail = (
        f'timeout: rounds={max_rounds} phase={phase} saw_dip={saw_dip} '
        f'valley={valley_price:.10f} final={from_valley_pct_final:+.2f}%'
    )
    log.info(f"  [ENTRY_TIMING] {symbol} SKIP: {detail}")
    return False, 'timeout', detail, None




def submit_timing_eval(lifecycle_id, ctx):
    """Submit a timing evaluation to the thread pool. Non-blocking."""
    if lifecycle_id in _timing_inflight:
        return False
    future = _timing_executor.submit(
        evaluate_entry_timing,
        ctx['token_ca'],
        ctx['symbol'],
        ctx['pool'],
        ctx.get('strict_fail_open', False)
    )
    _timing_inflight[lifecycle_id] = {
        'future': future,
        'ctx': ctx,
        'submitted_at': time.time(),
    }
    return True


def drain_timing_results(pending_entries, lifecycles, positions):
    """
    Poll completed timing evaluations and promote passing ones to
    pending_entries. Called from the main loop each iteration.
    """
    if not _timing_inflight:
        return
    for lifecycle_id in list(_timing_inflight.keys()):
        entry = _timing_inflight[lifecycle_id]
        future = entry['future']
        if not future.done():
            continue
        ctx = entry['ctx']
        symbol = ctx['symbol']
        del _timing_inflight[lifecycle_id]

        # Skip if this lifecycle already has a position or is already staged
        if lifecycle_id in pending_entries:
            log.debug(f"  [TIMING_DRAIN] {symbol} already staged, dropping result")
            continue
        if any(getattr(p, 'lifecycle_id', None) == lifecycle_id for p in positions.values()):
            log.debug(f"  [TIMING_DRAIN] {symbol} already in positions, dropping result")
            continue

        try:
            should_enter, reason, detail, trigger_price = future.result()
        except Exception as e:
            log.exception(f"  [TIMING_DRAIN] {symbol} eval raised: {e}")
            continue

        waited = int(time.time() - entry['submitted_at'])
        if should_enter:
            log.info(
                f"  [PREBUY_FILTER] {symbol} PASS: trend+timing OK ({reason}) "
                f"waited={waited}s inflight={len(_timing_inflight)}"
            )
            pending_entries[lifecycle_id] = {
                'token_ca': ctx['token_ca'],
                'symbol': symbol,
                'signal_ts': ctx['signal_ts'],
                'premium_signal_id': ctx['premium_signal_id'],
                'signal_type': ctx['signal_type'],
                'signal_minute_ts': ctx['signal_minute_ts'],
                'pool': ctx['pool'],
                'lifecycle_id': lifecycle_id,
                'super_idx': ctx['super_idx'],
                'trigger_price': trigger_price,
                'staged_at': time.time(),
                'attempts': 0,
                'last_debug_at': 0,
            }
            lifecycles.setdefault(
                lifecycle_id,
                build_lifecycle_state(
                    lifecycle_id, ctx['token_ca'], symbol, ctx['signal_ts'],
                    ctx['premium_signal_id'], ctx['signal_type']
                )
            )
            log.info(
                f"New signal: {symbol} lifecycle={lifecycle_id} "
                f"super={ctx['super_idx']} staged for stage1 execution"
            )
        else:
            log.info(
                f"  [PREBUY_FILTER] {symbol} BLOCKED by timing: {reason} — {detail} "
                f"waited={waited}s"
            )


def get_pool_address(token_ca):
    """Get pool address from shared/local truth sources first, then DexScreener fallback."""
    if token_ca in _SHARED_POOL_CACHE:
        return _SHARED_POOL_CACHE[token_ca]

    if shared_truth_source_enabled('pool'):
        shared_cached = get_shared_cache_value(f'pool:{token_ca}')
        if isinstance(shared_cached, dict):
            pool = str(shared_cached.get('poolAddress') or '').replace('solana_', '').strip()
            if pool:
                _SHARED_POOL_CACHE[token_ca] = pool
                return pool
        shared_resolved = get_shared_pool_resolution(token_ca)
        if isinstance(shared_resolved, dict):
            pool = str(shared_resolved.get('poolAddress') or '').replace('solana_', '').strip()
            if pool:
                _SHARED_POOL_CACHE[token_ca] = pool
                return pool
            if shared_resolved.get('rateLimited'):
                return None

    kline_db = get_kline_db()
    if kline_db is not None:
        try:
            row = kline_db.execute("SELECT pool_address FROM pool_mapping WHERE token_ca = ? AND pool_address IS NOT NULL AND TRIM(pool_address) != ''", (token_ca,)).fetchone()
            if row and row['pool_address']:
                pool = str(row['pool_address']).replace('solana_', '').strip()
                if pool:
                    _SHARED_POOL_CACHE[token_ca] = pool
                    return pool
        except Exception as e:
            log.debug(f"Pool lookup via pool_mapping failed for {token_ca}: {e}")
        try:
            row = kline_db.execute("SELECT pool_address FROM kline_1m WHERE token_ca = ? AND pool_address IS NOT NULL AND TRIM(pool_address) != '' ORDER BY fetched_at DESC, timestamp DESC LIMIT 1", (token_ca,)).fetchone()
            if row and row['pool_address']:
                pool = str(row['pool_address']).replace('solana_', '').strip()
                if pool:
                    _SHARED_POOL_CACHE[token_ca] = pool
                    return pool
        except Exception as e:
            log.debug(f"Pool lookup via kline_1m failed for {token_ca}: {e}")

    if not direct_provider_fallback_allowed():
        return None
    if get_shared_cooldown_ms('dexscreener', namespace='market-data:quotes') > 0:
        return None

    data = curl_json(f"https://api.dexscreener.com/latest/dex/tokens/{token_ca}")
    if not data:
        return None

    pairs = data.get('pairs', [])
    if not pairs:
        return None

    sol_pairs = [p for p in pairs if p.get('chainId') == 'solana']
    if not sol_pairs:
        sol_pairs = pairs

    pair = max(sol_pairs, key=lambda p: (p.get('liquidity', {}).get('usd', 0) or 0))
    pool = pair.get('pairAddress', '')
    if pool:
        _SHARED_POOL_CACHE[token_ca] = pool
        set_shared_cache_value(f'pool:{token_ca}', {'poolAddress': pool, 'provider': 'paper-monitor:dexscreener'}, 15 * 60 * 1000)
    return pool or None


def get_redis_client():
    """Return a Redis client if redis-py is installed and configured."""
    global _REDIS_CLIENT, _REDIS_INIT_ATTEMPTED
    if _REDIS_CLIENT is not None:
        return _REDIS_CLIENT
    if _REDIS_INIT_ATTEMPTED:
        return None
    _REDIS_INIT_ATTEMPTED = True
    if redis is None:
        log.info("redis package not available; live price checks will use direct fetch fallback")
        return None
    try:
        if REDIS_URL:
            client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        else:
            client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True,
            )
        client.ping()
        _REDIS_CLIENT = client
        log.info("Redis live price reader enabled")
        return _REDIS_CLIENT
    except Exception as e:
        log.warning(f"Redis unavailable, using direct fetch fallback: {e}")
        return None


def read_redis_payload(token_ca):
    """Read and parse raw live-price payload for a token from Redis."""
    client = get_redis_client()
    if not client or not token_ca:
        return None
    keys = [
        f"{REDIS_KEY_PREFIX}{token_ca}",
        f"live_price:{token_ca}",
        token_ca,
    ]
    for key in keys:
        try:
            raw = client.get(key)
        except Exception as e:
            log.warning(f"Redis read failed for {token_ca[:8]} key={key}: {e}")
            return None
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            log.warning(f"Redis payload parse failed for {token_ca[:8]} key={key}")
            continue
        if isinstance(payload, dict):
            payload['_redis_key'] = key
            return payload
    return None


def _coerce_timestamp_ms(payload):
    for key in ('timestamp_ms', 'timestamp', 'ts', 'updated_at_ms'):
        value = payload.get(key)
        if value is None:
            continue
        try:
            ts = int(float(value))
        except (TypeError, ValueError):
            continue
        if ts < 10**11:
            ts *= 1000
        return ts
    return None


def is_redis_payload_fresh(payload, max_age_ms=LIVE_PRICE_MAX_AGE_MS, min_timestamp_ms=None):
    """Validate Redis price payload freshness and monotonicity."""
    if not isinstance(payload, dict):
        return False
    try:
        price = float(payload.get('price_usd') or 0)
    except (TypeError, ValueError):
        return False
    if price <= 0:
        return False
    timestamp_ms = _coerce_timestamp_ms(payload)
    if not timestamp_ms:
        return False
    age_ms = int(time.time() * 1000) - timestamp_ms
    if age_ms < 0 or age_ms > max_age_ms:
        return False
    if min_timestamp_ms is not None and timestamp_ms < int(min_timestamp_ms):
        return False
    return True


def get_current_bar(token_ca, pool_address=None):
    """Get latest 1m bar from shared/local truth sources first, then GeckoTerminal fallback."""
    if shared_truth_source_enabled('ohlcv'):
        shared_latest_bars = get_shared_cache_value(f'ohlcv-latest:{token_ca}')
        if isinstance(shared_latest_bars, list) and shared_latest_bars:
            bar = shared_latest_bars[0]
            try:
                return {
                    'ts': int(bar['timestamp']),
                    'open': float(bar['open']),
                    'high': float(bar['high']),
                    'low': float(bar['low']),
                    'close': float(bar['close']),
                    'volume': float(bar.get('volume', 0)),
                }
            except Exception:
                pass

    kline_db = get_kline_db()
    if kline_db is not None:
        try:
            row = kline_db.execute("SELECT timestamp, open, high, low, close, volume FROM kline_1m WHERE token_ca = ? ORDER BY timestamp DESC LIMIT 1", (token_ca,)).fetchone()
            if row:
                return {
                    'ts': int(row['timestamp']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row['volume']),
                }
        except Exception:
            pass

    if not pool_address:
        pool_address = get_pool_address(token_ca)
    if not pool_address:
        return None

    if shared_truth_source_enabled('ohlcv'):
        shared_result = get_shared_recent_ohlcv(token_ca, pool_address, {
            'signalTsSec': int(time.time()),
            'bars': 2,
            'beforeTimestamps': [int(time.time()) + 60],
            'allowDexFallback': False,
        })
        if isinstance(shared_result, dict):
            bars = shared_result.get('bars') or []
            if bars:
                bar = bars[-1]
                try:
                    return {
                        'ts': int(bar['timestamp']),
                        'open': float(bar['open']),
                        'high': float(bar['high']),
                        'low': float(bar['low']),
                        'close': float(bar['close']),
                        'volume': float(bar.get('volume', 0)),
                    }
                except Exception:
                    pass
            if shared_result.get('rateLimited'):
                return None

    if not direct_provider_fallback_allowed():
        return None
    if get_shared_cooldown_ms('geckoterminal', namespace='market-data:pool-ohclv') > 0:
        return None

    url = (
        f"https://api.geckoterminal.com/api/v2/networks/solana/pools/"
        f"{pool_address}/ohlcv/minute?aggregate=1&limit=1"
    )
    data = curl_json(url)
    if not data:
        return None

    ohlcv = data.get('data', {}).get('attributes', {}).get('ohlcv_list', [])
    if not ohlcv:
        return None

    row = ohlcv[0]
    return {
        'ts': int(row[0]),
        'open': float(row[1]),
        'high': float(row[2]),
        'low': float(row[3]),
        'close': float(row[4]),
        'volume': float(row[5]),
    }


def get_current_price_direct(token_ca, pool_address=None):
    """Get latest price from GeckoTerminal (latest 1m candle close)."""
    bar = get_current_bar(token_ca, pool_address)
    if not bar:
        return None
    price = bar['close'] or bar.get('open')
    if price is None or price <= 0:
        return None
    return {
        'price': price,
        'ts': int(bar['ts']),
        'source': 'geckoterminal_direct',
        'bar': bar,
    }


def _select_best_dex_pair(token_ca, pairs):
    sol_pairs = []
    fallback_pairs = []
    for pair in pairs or []:
        base_addr = ((pair.get('baseToken') or {}).get('address') or '').strip()
        if base_addr and base_addr != token_ca:
            continue
        if pair.get('chainId') == 'solana':
            sol_pairs.append(pair)
        else:
            fallback_pairs.append(pair)
    candidates = sol_pairs or fallback_pairs
    if not candidates:
        return None
    return max(candidates, key=lambda p: (p.get('liquidity', {}).get('usd', 0) or 0))


def get_dexscreener_price_snapshot(token_ca, min_timestamp_ms=None):
    """Get latest USD price snapshot from DexScreener when shared/live quotes are unavailable."""
    if not token_ca or not direct_provider_fallback_allowed():
        return None
    if get_shared_cooldown_ms('dexscreener', namespace='market-data:quotes') > 0:
        return None

    data = curl_json(f"https://api.dexscreener.com/latest/dex/tokens/{token_ca}")
    if not data:
        return None

    best_pair = _select_best_dex_pair(token_ca, (data or {}).get('pairs') or [])
    if not best_pair:
        return None

    try:
        price = float(best_pair.get('priceUsd') or 0)
    except (TypeError, ValueError):
        return None
    if price <= 0:
        return None

    pair_created_at = best_pair.get('pairCreatedAt')
    try:
        pair_created_at = int(pair_created_at) if pair_created_at is not None else None
    except (TypeError, ValueError):
        pair_created_at = None

    timestamp_ms = int(time.time() * 1000)
    if min_timestamp_ms is not None and pair_created_at is not None and pair_created_at < int(min_timestamp_ms):
        return None

    return {
        'price': price,
        'ts': int(timestamp_ms // 1000),
        'timestamp_ms': timestamp_ms,
        'source': 'dexscreener',
        'payload': best_pair,
    }


def _shared_quote_to_sol_price(out_amount_lamports, token_decimals):
    """
    Convert a Jupiter shared-quote outAmount (SOL lamports for an input of
    1,000,000 raw token units) into SOL per token.

    For pump.fun-style tokens (decimals=6), 1,000,000 raw = 1 token, so the
    conversion is simply (lamports / 1e9). For other decimal counts we adjust
    by 10^(decimals-6).

    Returns SOL-denominated price directly — no USD conversion needed.
    All position monitoring now uses SOL pricing to eliminate phantom PnL
    from SOL/USD fluctuations during trades.
    """
    try:
        lamports = float(out_amount_lamports or 0)
    except (TypeError, ValueError):
        return None
    if lamports <= 0:
        return None
    decimals = int(token_decimals) if token_decimals is not None else 6
    # SOL per (1,000,000 raw token units) = lamports / 1e9
    # tokens per (1,000,000 raw)           = 1e6 / 10^decimals
    # SOL per token                         = above / tokens_per_million_raw
    tokens_per_million_raw = (10 ** 6) / (10 ** decimals) if decimals >= 0 else 1.0
    if tokens_per_million_raw <= 0:
        return None
    sol_per_million_raw = lamports / 1e9
    return sol_per_million_raw / tokens_per_million_raw


def get_live_price_snapshot(token_ca, pool_address=None, min_timestamp_ms=None, token_decimals=None):
    """Get live price snapshot — ALL prices returned in SOL per token.

    Primary source: Jupiter shared-quote (already in SOL lamports).
    Fallback sources (Redis/DexScreener/GeckoTerminal) return USD and are
    converted to SOL via sol_price. This eliminates phantom PnL from
    SOL/USD fluctuations during trades.
    """
    payload = read_redis_payload(token_ca)
    if payload and is_redis_payload_fresh(payload, LIVE_PRICE_MAX_AGE_MS, min_timestamp_ms=min_timestamp_ms):
        timestamp_ms = _coerce_timestamp_ms(payload)
        usd_price = float(payload['price_usd'])
        sol_usd = get_sol_price()
        if sol_usd and sol_usd > 0:
            sol_price_val = usd_price / sol_usd  # Convert USD → SOL
            return {
                'price': sol_price_val,
                'ts': int(timestamp_ms // 1000),
                'timestamp_ms': timestamp_ms,
                'source': 'redis',
                'payload': payload,
            }

    if shared_truth_source_enabled('quotes'):
        shared_quote = get_shared_quote_cache_value(f'quote:{token_ca}:So11111111111111111111111111111111111111112:1000000')
        if isinstance(shared_quote, dict):
            quote = shared_quote.get('quote') or {}
            sol_price_val = _shared_quote_to_sol_price(quote.get('outAmount'), token_decimals)
            if sol_price_val and sol_price_val > 0:
                fetched_at = shared_quote.get('fetchedAt')
                try:
                    fetched_at = int(fetched_at)
                except Exception:
                    fetched_at = int(time.time() * 1000)
                timestamp_ms = fetched_at if fetched_at > 10_000_000_000 else int(fetched_at * 1000)
                if min_timestamp_ms is None or timestamp_ms >= min_timestamp_ms:
                    return {
                        'price': sol_price_val,
                        'ts': int(timestamp_ms // 1000),
                        'timestamp_ms': timestamp_ms,
                        'source': 'shared-quote-cache',
                        'payload': shared_quote,
                    }
        shared_quote_result = get_shared_swap_quote(token_ca, 1000000)
        if isinstance(shared_quote_result, dict):
            quote = shared_quote_result.get('quote') or {}
            sol_price_val = _shared_quote_to_sol_price(quote.get('outAmount'), token_decimals)
            if sol_price_val and sol_price_val > 0:
                fetched_at = shared_quote_result.get('fetchedAt')
                try:
                    fetched_at = int(fetched_at)
                except Exception:
                    fetched_at = int(time.time() * 1000)
                timestamp_ms = fetched_at if fetched_at > 10_000_000_000 else int(fetched_at * 1000)
                return {
                    'price': sol_price_val,
                    'ts': int(timestamp_ms // 1000),
                    'timestamp_ms': timestamp_ms,
                    'source': 'shared-quote-runtime',
                    'payload': shared_quote_result,
                }
            if shared_quote_result.get('rateLimited'):
                return None

    dex_snapshot = get_dexscreener_price_snapshot(token_ca, min_timestamp_ms=min_timestamp_ms)
    if dex_snapshot:
        # DexScreener returns USD — convert to SOL
        sol_usd = get_sol_price()
        if sol_usd and sol_usd > 0:
            dex_snapshot['price'] = dex_snapshot['price'] / sol_usd
            return dex_snapshot

    direct = get_current_price_direct(token_ca, pool_address)
    if not direct:
        return None
    direct['timestamp_ms'] = int(direct['ts']) * 1000
    # GeckoTerminal returns USD — convert to SOL
    sol_usd = get_sol_price()
    if sol_usd and sol_usd > 0:
        direct['price'] = direct['price'] / sol_usd
    return direct


def get_current_price(token_ca, pool_address=None):
    """Get latest price from GeckoTerminal (latest 1m candle close)."""
    snapshot = get_current_price_direct(token_ca, pool_address)
    if not snapshot:
        return None
    return snapshot['price']


# === NOT_ATH Scoring ===

def parse_super_index(description):
    """
    Parse Super Index from NOT_ATH description.
    Supports formats:
      ✡ Super Index： 119🔮
      ✡ **Super Index**： 119🔮
      ✡ Super Index： ✡ x 82
    Returns int or None.
    """
    if not description:
        return None
    normalized = str(description).replace('**', '').replace('\r', '')
    # Try format: " 119🔮"
    m = re.search(r'Super\s+Index[：:]\s*(\d+)\s*🔮', normalized)
    if m:
        return int(m.group(1))
    # Try format: "✡ x 82"
    m = re.search(r'Super\s+Index[：:]\s*✡\s*x\s*(\d+)', normalized, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


# ─── Task 6: Sub-Index Parsing ──────────────────────────────────────────────
_SUB_INDEX_NAMES = ['ai', 'trade', 'security', 'address', 'sentiment', 'media']

def parse_sub_indices(description):
    """Extract all 6 sub-index values from signal description.
    Supports NOT_ATH format: 'AI         Index：15🔮'
    and ATH format: 'AI Index：(signal)15 --> 45 🔺200%' (takes current=45)
    Returns dict with keys: ai, trade, security, address, sentiment, media.
    """
    result = {k: 0 for k in _SUB_INDEX_NAMES}
    if not description:
        return result
    normalized = str(description).replace('**', '').replace('\\r', '')
    _pat_map = {'ai': 'AI', 'trade': 'Trade', 'security': 'Security',
                'address': 'Address', 'sentiment': 'Sentiment', 'media': 'Media'}
    for name, pat in _pat_map.items():
        # ATH format: "AI Index：(signal)15 --> 45 🔺200%"
        m = re.search(pat + r'\s+Index[：:]\s*\(signal\)\d+\s*-->\s*(\d+)', normalized, re.IGNORECASE)
        if m:
            result[name] = int(m.group(1))
            continue
        # NOT_ATH format: "AI         Index：15🔮"
        m = re.search(pat + r'\s+Index[：:]\s*(\d+)', normalized, re.IGNORECASE)
        if m:
            result[name] = int(m.group(1))
    return result


# ─── Task 7: Signal Velocity (Telegram Propagation Proxy) ────────────────────
def calculate_signal_velocity(watchlist_entry):
    """Signal velocity = signal_count / hours since registration.
    Higher velocity = token spreading rapidly across Telegram channels.
    """
    sc = int(watchlist_entry.get('signal_count') or 1)
    added_at = watchlist_entry.get('added_at') or time.time()
    hours = max((time.time() - added_at) / 3600, 0.0167)  # min 1 minute
    return round(sc / hours, 2)


def parse_top10_percent(description):
    """
    Extract Top10 holder percentage from string like 'Top10:** 24.73%'
    """
    m = re.search(r'Top10[^0-9]*([\d\.]+)\s*%', description, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def get_notath_bars(pool_address, limit=5):
    """
    Get recent 1-minute bars for NOT_ATH scoring.
    Prefer local kline cache for the pool, fall back to GeckoTerminal.
    Returns list of bars (newest first), or None.
    """
    kline_db = get_kline_db()
    if kline_db is not None:
        try:
            rows = kline_db.execute(
                "SELECT timestamp, open, high, low, close, volume FROM kline_1m WHERE pool_address = ? ORDER BY timestamp DESC LIMIT ?",
                (pool_address, limit)
            ).fetchall()
            if rows and len(rows) >= min(4, limit):
                return [{
                    'ts': int(row['timestamp']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row['volume']),
                } for row in rows]
        except Exception:
            pass

    if not direct_provider_fallback_allowed():
        return None

    url = (
        f"https://api.geckoterminal.com/api/v2/networks/solana/pools/"
        f"{pool_address}/ohlcv/minute?aggregate=1&limit={limit}"
    )
    data = curl_json(url)
    if not data:
        return None
    ohlcv_list = data.get('data', {}).get('attributes', {}).get('ohlcv_list', [])
    if not ohlcv_list or len(ohlcv_list[0]) < 6:
        return None
    bars = []
    for row in ohlcv_list:
        bars.append({
            'ts': int(row[0]),
            'open': float(row[1]),
            'high': float(row[2]),
            'low': float(row[3]),
            'close': float(row[4]),
            'volume': float(row[5]),
        })
    return bars


def check_multi_bar_trend(bars, symbol):
    """
    Evaluate trend using linear regression on ALL available 1m bars.

    Looks at the overall direction regardless of individual candle drops.
    A large red candle in the middle of an uptrend does not kill the score.

    Auto-calibrates threshold: uses normalized slope (slope / std-dev of prices),
    which scales correctly whether we have 5 bars or 100 bars.

    Returns (trend_ok: bool, reason: str, detail: str)
    """
    if not bars or len(bars) < 3:
        return True, 'insufficient_bars', 'Not enough bars for shape analysis, fail-open'

    # bars is newest-first; reverse to get chronological order
    closes = [b['close'] for b in bars]
    closes = list(reversed(closes))  # oldest → newest
    n = len(closes)

    # Validate prices
    if any(c <= 0 for c in closes):
        return True, 'insufficient_bars', 'Invalid close prices, fail-open'

    # Linear regression slope
    mean_x = (n - 1) / 2.0
    mean_y = sum(closes) / n
    numerator = sum((i - mean_x) * (closes[i] - mean_y) for i in range(n))
    denominator = sum((i - mean_x) ** 2 for i in range(n))
    slope = numerator / denominator if denominator != 0 else 0

    # Convert to % change per bar relative to mean price
    slope_pct = (slope / mean_y * 100) if mean_y > 0 else 0

    # Auto-calibrate threshold: normalize by std-dev of prices
    # This makes the threshold scale-invariant: works the same for 5 bars or 100 bars.
    # A slope of 0.15x std-dev = meaningful directional move, not just noise.
    variance = sum((c - mean_y) ** 2 for c in closes) / n
    std_pct = (variance ** 0.5 / mean_y * 100) if mean_y > 0 else 1.0
    std_pct = max(std_pct, 0.01)  # floor to avoid div-by-zero on flat price
    normalized_slope = slope_pct / std_pct

    # Thresholds (normalized, window-size invariant):
    # normalized_slope > +0.15: overall rising (slope > 15% of 1 std-dev per bar)
    # normalized_slope < -0.15: overall falling
    # between: sideways → fail-open
    if normalized_slope > 0.15:
        return True, 'passed_shape', (
            f'uptrend slope={slope_pct:+.3f}%/bar norm={normalized_slope:+.2f} n={n}'
        )
    elif normalized_slope < -0.15:
        return False, 'downtrend', (
            f'downtrend slope={slope_pct:+.3f}%/bar norm={normalized_slope:+.2f} n={n}'
        )
    else:
        return True, 'sideways', (
            f'sideways slope={slope_pct:+.3f}%/bar norm={normalized_slope:+.2f} n={n} (pass)'
        )


def compute_notath_score(bars):
    """
    Compute NOT_ATH entry score.
    bars: [current, lag1, lag2, lag3] — newest first
    Scoring: RED(+2) + lowVolume(+1) + active(+1) >= 3 to pass

    Returns {passed, score, is_red, low_volume, is_active, mom, avg_vol}
    """
    if not bars or len(bars) < 4:
        return {'passed': False, 'score': 0, 'is_red': False,
                'low_volume': False, 'is_active': False, 'mom': 0, 'avg_vol': 0}

    current = bars[0]
    prev3 = bars[1:4]  # [lag1, lag2, lag3]

    # RED bar: close < open (pullback confirmation)
    is_red = current['close'] < current['open']
    if not is_red:
        return {'passed': False, 'score': 0, 'is_red': False,
                'low_volume': False, 'is_active': False, 'mom': 0,
                'avg_vol': sum(b['volume'] for b in prev3) / 3}

    # lowVolume: current vol <= avg(prev3 vol) — accumulation not distribution
    avg_vol = sum(b['volume'] for b in prev3) / 3
    low_volume = current['volume'] <= avg_vol

    # active: |mom_from_lag1| > 30% — momentum exists
    mom = 0
    if prev3[0]['close'] > 0:
        mom = ((current['close'] - prev3[0]['close']) / prev3[0]['close']) * 100
    is_active = abs(mom) > 30

    # Score
    score = 2 + (1 if low_volume else 0) + (1 if is_active else 0)
    passed = score >= 3

    return {
        'passed': passed,
        'score': score,
        'is_red': True,
        'low_volume': low_volume,
        'is_active': is_active,
        'mom': mom,
        'avg_vol': avg_vol,
    }


def get_entry_bar_ohlcv(pool_address, token_ca=None):
    """Get the most recent completed 1-minute OHLCV bar for FBR check."""
    if shared_truth_source_enabled('ohlcv'):
        kline_db = get_kline_db()
        if kline_db is not None:
            try:
                row = kline_db.execute(
                    "SELECT timestamp, open, high, low, close, volume FROM kline_1m WHERE pool_address = ? ORDER BY timestamp DESC LIMIT 1",
                    (pool_address,)
                ).fetchone()
                if row:
                    log.info(f"  [FBR_SOURCE] kline_db hit for pool={pool_address[:12]}...")
                    return {
                        'ts': int(row['timestamp']),
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'volume': float(row['volume']),
                    }
                else:
                    log.info(f"  [FBR_SOURCE] kline_db miss for pool={pool_address[:12]}...")
            except Exception as e:
                log.warning(f"  [FBR_SOURCE] kline_db error for pool={pool_address[:12]}...: {e}")
        else:
            log.info(f"  [FBR_SOURCE] kline_db not available")
    else:
        log.info(f"  [FBR_SOURCE] shared ohlcv not enabled, using direct fallback")

    if not direct_provider_fallback_allowed():
        log.info(f"  [FBR_SOURCE] direct fallback not allowed, no bar data")
        return None

    url = (
        f"https://api.geckoterminal.com/api/v2/networks/solana/pools/"
        f"{pool_address}/ohlcv/minute?aggregate=1&limit=2"
    )
    data = curl_json(url, timeout=5)
    if not data:
        log.warning(f"  [FBR_SOURCE] GeckoTerminal returned no data for pool={pool_address[:12]}...")
        return None
    ohlcv_list = data.get('data', {}).get('attributes', {}).get('ohlcv_list', [])
    if not ohlcv_list or len(ohlcv_list[0]) < 6:
        log.warning(f"  [FBR_SOURCE] GeckoTerminal empty ohlcv_list for pool={pool_address[:12]}...")
        return None
    # Return the most recent completed bar
    row = ohlcv_list[0]
    log.info(f"  [FBR_SOURCE] GeckoTerminal hit for pool={pool_address[:12]}... o={row[1]} c={row[4]}")
    return {
        'ts': int(row[0]),
        'open': float(row[1]),
        'high': float(row[2]),
        'low': float(row[3]),
        'close': float(row[4]),
        'volume': float(row[5]),
    }


def get_sol_price():
    """Get current SOL/USD price from shared cache first, then DexScreener."""
    now = time.time()
    shared_cached_price = _SHARED_SOL_PRICE_CACHE.get('price')
    shared_fetched_at = _SHARED_SOL_PRICE_CACHE.get('fetched_at') or 0.0
    if shared_cached_price and (now - shared_fetched_at) < SOL_PRICE_TTL_SEC:
        return shared_cached_price

    if shared_truth_source_enabled('quotes'):
        shared_snapshot = get_shared_quote_cache_value('dex-pair:So11111111111111111111111111111111111111112')
        if isinstance(shared_snapshot, dict):
            pair = shared_snapshot.get('pair') or {}
            try:
                price = float(pair.get('priceUsd') or 0)
            except (TypeError, ValueError):
                price = 0
            if price > 0:
                _SHARED_SOL_PRICE_CACHE['price'] = price
                _SHARED_SOL_PRICE_CACHE['fetched_at'] = now
                return price

    cached_price = _SOL_PRICE_CACHE.get('price')
    fetched_at = _SOL_PRICE_CACHE.get('fetched_at') or 0.0
    if cached_price and (now - fetched_at) < SOL_PRICE_TTL_SEC:
        return cached_price

    # SOL/USD is infrastructure-level data — every USD-denominated comparison
    # in the paper trader (entry price, trail/SL evals, and the Jupiter
    # shared-quote SOL→USD conversion) depends on it. The unified-truth
    # `direct_provider_fallback_allowed()` flag is meant to gate token-level
    # direct hits, not this universal anchor. If we honour it here and the
    # shared SOL cache happens to be empty (e.g. right after a deploy
    # restart), get_sol_price returns None and every shared-quote price
    # conversion silently fails — producing the `no_price (age_ms=None)`
    # entry-timing storm we saw on 2026-04-07 12:xx after fb5e595. Always
    # allow the direct DexScreener call as a last-resort anchor fetch.

    data = curl_json("https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112")
    if not data:
        return cached_price
    pairs = data.get('pairs', [])
    if not pairs:
        return cached_price
    usdc_pairs = [p for p in pairs if 'USD' in (p.get('quoteToken', {}).get('symbol', '') or '').upper()]
    if usdc_pairs:
        pair = max(usdc_pairs, key=lambda p: (p.get('liquidity', {}).get('usd', 0) or 0))
    else:
        pair = pairs[0]
    try:
        price = float(pair.get('priceUsd', 0))
        if price > 0:
            _SOL_PRICE_CACHE['price'] = price
            _SOL_PRICE_CACHE['fetched_at'] = now
            return price
    except (TypeError, ValueError):
        pass
    return cached_price


def determine_market_regime(sol_price_now, sol_price_cache={}):
    """Determine market regime based on SOL daily price direction.
    Returns 'bull', 'bear', or 'neutral'."""
    now_ts = int(time.time())
    day_key = datetime.utcfromtimestamp(now_ts).strftime('%Y-%m-%d')

    # Cache SOL price at start of day
    if 'day' not in sol_price_cache or sol_price_cache['day'] != day_key:
        sol_price_cache['day'] = day_key
        sol_price_cache['open'] = sol_price_now

    if sol_price_cache.get('open') and sol_price_now:
        change = (sol_price_now - sol_price_cache['open']) / sol_price_cache['open']
        if change > 0.01:
            return 'bull'
        elif change < -0.01:
            return 'bear'
    return 'neutral'


# === Signal Monitoring ===

def _normalize_signal_rows(rows):
    normalized = []
    for idx, row in enumerate(rows, start=1):
        if isinstance(row, sqlite3.Row):
            record = dict(row)
        else:
            record = dict(row)
        record.setdefault('id', idx)
        record.setdefault('token_ca', None)
        record.setdefault('symbol', None)
        record.setdefault('timestamp', None)
        record.setdefault('description', '')
        record.setdefault('hard_gate_status', None)
        record.setdefault('market_cap', None)
        record.setdefault('holders', None)
        record.setdefault('volume_24h', None)
        record.setdefault('top10_pct', None)
        record.setdefault('is_ath', 0)
        
        # Ensure is_ath and signal_type are properly inferred from description if missing/not set
        desc = record.get('description') or ''
        if not record.get('is_ath') and ('ATH' in desc or 'All Time High' in desc):
            record['is_ath'] = 1
        if record.get('is_ath') and not record.get('signal_type'):
            record['signal_type'] = 'ATH'
        elif not record.get('signal_type') and 'New Trending' in desc:
            record['signal_type'] = 'NEW_TRENDING'
            
        normalized.append(record)
    return normalized


def _read_remote_export(limit=REMOTE_SIGNAL_LOOKBACK, before_id=None):
    if not REMOTE_SIGNAL_URL:
        return []

    params = {'limit': str(limit)}
    if before_id is not None:
        params['before_id'] = str(before_id)
    query = urllib.parse.urlencode(params)
    url = REMOTE_SIGNAL_URL
    sep = '&' if '?' in url else '?'
    request_url = f"{url}{sep}{query}"

    curl_cmd = ['curl', '-sS', '-m', '20', '-H', 'Accept: application/json']
    if REMOTE_SIGNAL_TOKEN:
        curl_cmd.extend(['-H', f'x-dashboard-token: {REMOTE_SIGNAL_TOKEN}'])
    curl_cmd.append(request_url)

    payload = None
    try:
        result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=25)
        if result.returncode == 0 and result.stdout.strip():
            payload = json.loads(result.stdout)
        elif result.stderr:
            log.warning(f"Remote export curl failed: {result.stderr.strip()}")
    except Exception as e:
        log.warning(f"Remote export curl exception: {e}")

    if payload is None:
        headers = {'Accept': 'application/json'}
        if REMOTE_SIGNAL_TOKEN:
            headers['x-dashboard-token'] = REMOTE_SIGNAL_TOKEN
        req = urllib.request.Request(request_url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as resp:
            payload = json.loads(resp.read().decode('utf-8'))

    tables = payload.get('tables', {}) if isinstance(payload, dict) else {}
    premium = tables.get('premium_signals', {}) if isinstance(tables, dict) else {}
    rows = premium.get('rows', []) if isinstance(premium, dict) else []
    return _normalize_signal_rows(rows)


def _is_paper_trade_signal(record):
    status = (record.get('hard_gate_status') or '').upper()
    description = record.get('description') or ''
    signal_type = (record.get('signal_type') or '').upper()
    
    is_ath = signal_type == 'ATH' or 'New ATH' in description or 'ATH' in description or 'All Time High' in description
    is_new_trending = signal_type == 'NEW_TRENDING' or 'New Trending' in description
    
    if is_ath:
        # For ATH, we bypass the Node.js hard_gate_status (which often rejects with V17_NOT_ATH1 or V18 blocks)
        # to allow the Python Matrix evaluator to handle the full breakout lifecycle logic.
        return True
    if is_new_trending:
        return status in {'PASS', 'RISK_BLOCKED'}
        
    return False


def _premium_signal_has_column(sdb, column_name):
    try:
        columns = sdb.execute("PRAGMA table_info(premium_signals)").fetchall()
        return any(str(row[1]) == column_name for row in columns)
    except Exception:
        return False


def _query_local_new_signals(last_signal_id):
    sdb = sqlite3.connect(SENTIMENT_DB)
    sdb.row_factory = sqlite3.Row
    has_signal_type = _premium_signal_has_column(sdb, 'signal_type')
    signal_type_expr = 'signal_type' if has_signal_type else 'NULL AS signal_type'
    rows = sdb.execute(f"""
        SELECT id, token_ca, symbol, timestamp, description, hard_gate_status, {signal_type_expr}, market_cap, holders, volume_24h, top10_pct, is_ath
        FROM premium_signals
        WHERE id > ?
          AND (
              (hard_gate_status IN ('PASS', 'RISK_BLOCKED') AND description LIKE '%New Trending%')
              OR description LIKE '%New ATH%' OR description LIKE '%ATH%' OR description LIKE '%All Time High%'
          )
        ORDER BY id ASC
    """, (last_signal_id,)).fetchall()
    sdb.close()
    return _normalize_signal_rows(rows)


def _query_local_recent_signals(limit=20):
    sdb = sqlite3.connect(SENTIMENT_DB)
    sdb.row_factory = sqlite3.Row
    has_signal_type = _premium_signal_has_column(sdb, 'signal_type')
    signal_type_expr = 'signal_type' if has_signal_type else 'NULL AS signal_type'
    rows = sdb.execute(f"""
        SELECT id, token_ca, symbol, timestamp, description, hard_gate_status, {signal_type_expr}, market_cap, holders, volume_24h, top10_pct, is_ath
        FROM premium_signals
        WHERE (hard_gate_status IN ('PASS', 'RISK_BLOCKED') AND description LIKE '%New Trending%')
           OR description LIKE '%New ATH%' OR description LIKE '%ATH%' OR description LIKE '%All Time High%'
        ORDER BY id DESC
        LIMIT ?
    """, (limit,)).fetchall()
    sdb.close()
    return list(reversed(_normalize_signal_rows(rows)))


def get_new_signals(last_signal_id):
    """Query new paper-trade candidate New Trending signals from remote export or local DB."""
    try:
        if REMOTE_SIGNAL_URL:
            rows = _read_remote_export(limit=REMOTE_SIGNAL_LOOKBACK)
            rows = [
                r for r in rows
                if (r.get('id') or 0) > last_signal_id
                and _is_paper_trade_signal(r)
            ]
            rows.sort(key=lambda r: r.get('id') or 0)
            return rows
        return _query_local_new_signals(last_signal_id)
    except Exception as e:
        log.warning(f"Failed to query signals: {e}")
        return []


def get_recent_signals(limit=20):
    """Get most recent paper-trade candidate New Trending signals for dry-run mode."""
    try:
        if REMOTE_SIGNAL_URL:
            rows = _read_remote_export(limit=max(limit, REMOTE_SIGNAL_LOOKBACK))
            rows = [r for r in rows if _is_paper_trade_signal(r)]
            rows.sort(key=lambda r: r.get('id') or 0)
            return rows[-limit:]
        return _query_local_recent_signals(limit)
    except Exception as e:
        log.warning(f"Failed to query signals: {e}")
        return []


def get_signal_freshness():
    """Return latest premium_signals timestamp metadata for health logging."""
    try:
        if REMOTE_SIGNAL_URL:
            rows = _read_remote_export(limit=min(REMOTE_SIGNAL_LOOKBACK, 200))
            if not rows:
                return {'latest_ts': None, 'age_minutes': None, 'total': 0, 'source': 'remote'}
            latest_ts = max(int(r['timestamp']) for r in rows if r.get('timestamp'))
            latest_sec = latest_ts // 1000 if latest_ts > 1e12 else latest_ts
            age_minutes = int((time.time() - latest_sec) / 60)
            return {'latest_ts': latest_sec, 'age_minutes': age_minutes, 'total': len(rows), 'source': 'remote'}

        sdb = sqlite3.connect(SENTIMENT_DB)
        sdb.row_factory = sqlite3.Row
        row = sdb.execute("SELECT MAX(timestamp) AS latest_ts, COUNT(*) AS total FROM premium_signals").fetchone()
        sdb.close()
        if not row or not row['latest_ts']:
            return {'latest_ts': None, 'age_minutes': None, 'total': 0, 'source': 'local'}

        latest_ts = int(row['latest_ts'])
        latest_sec = latest_ts // 1000 if latest_ts > 1e12 else latest_ts
        age_minutes = int((time.time() - latest_sec) / 60)
        return {'latest_ts': latest_sec, 'age_minutes': age_minutes, 'total': int(row['total'] or 0), 'source': 'local'}
    except Exception as e:
        log.warning(f"Failed to inspect signal freshness: {e}")
        return {'latest_ts': None, 'age_minutes': None, 'total': 0, 'source': 'unknown'}


def get_last_processed_id(db):
    """Get the highest signal_ts in paper_trades to avoid re-processing."""
    row = db.execute("SELECT MAX(signal_ts) as max_ts FROM paper_trades").fetchone()
    return row['max_ts'] or 0


def wait_for_local_signal_source():
    """Wait until the local sentiment DB and premium_signals table are available."""
    if REMOTE_SIGNAL_URL:
        return

    attempts = 0
    while True:
        try:
            if os.path.exists(SENTIMENT_DB):
                sdb = sqlite3.connect(SENTIMENT_DB)
                try:
                    row = sdb.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='premium_signals'"
                    ).fetchone()
                    if row:
                        return
                finally:
                    sdb.close()
        except Exception as e:
            if attempts % 12 == 0:
                log.warning(f"Waiting for local signal DB readiness: {e}")
        if attempts % 12 == 0:
            log.warning(f"Waiting for local signal DB/table: {SENTIMENT_DB} (attempt {attempts + 1})")
        attempts += 1
        time.sleep(5)


# === Position Tracking ===

def _safe_int(value, default=0):
    try:
        if value in (None, ''):
            return default
        return int(str(value))
    except (TypeError, ValueError):
        return default


def _safe_float(value, default=0.0):
    try:
        if value in (None, ''):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_entry_execution(entry_execution_json):
    if isinstance(entry_execution_json, dict):
        return entry_execution_json
    if not entry_execution_json:
        return None
    try:
        return json.loads(entry_execution_json)
    except Exception:
        return None


def normalize_monitor_state_json(monitor_state):
    return json.dumps(monitor_state or {}, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def has_partial_state_gap(token_amount_raw, entry_execution, monitor_state):
    execution = entry_execution if isinstance(entry_execution, dict) else {}
    state = monitor_state if isinstance(monitor_state, dict) else {}
    original_token_amount_raw = _safe_int(execution.get('quotedOutAmountRaw'), 0)
    remaining_token_amount_raw = _safe_int(token_amount_raw, 0)
    if original_token_amount_raw <= 0 or remaining_token_amount_raw <= 0:
        return False
    if remaining_token_amount_raw >= original_token_amount_raw:
        return False
    partial_state_fields = ('tp1', 'tp2', 'tp3', 'tp4', 'soldPct', 'lockedPnl', 'moonbag')
    return not any(state.get(field) not in (None, False, 0, 0.0, '') for field in partial_state_fields)


def sanitize_monitor_state(monitor_state, *, token_ca, symbol, entry_price, entry_ts, position_size_sol, token_amount_raw, token_decimals, peak_pnl=0.0, trailing_active=False, bars_held=0, last_mark_ts=None):
    sanitized = dict(monitor_state or {})
    sanitized['tokenCA'] = token_ca or sanitized.get('tokenCA') or None
    sanitized['symbol'] = symbol or sanitized.get('symbol') or 'UNKNOWN'
    sanitized['entryPrice'] = _safe_float(entry_price, 0.0)
    sanitized['entrySol'] = _safe_float(position_size_sol, 0.0)
    sanitized['tokenAmount'] = _safe_int(token_amount_raw, 0)
    sanitized['tokenDecimals'] = _safe_int(token_decimals, 0)
    sanitized['entryTime'] = _safe_int(entry_ts, 0) * 1000
    sanitized['highPnl'] = round(_safe_float(peak_pnl, 0.0) * 100.0, 6)
    sanitized['peakPnl'] = _safe_float(peak_pnl, 0.0)
    sanitized['breakeven'] = bool(trailing_active)
    sanitized['trailingActive'] = bool(trailing_active)
    sanitized['barsHeld'] = max(0, _safe_int(bars_held, 0))
    sanitized['lastMarkTs'] = _safe_int(last_mark_ts, _safe_int(entry_ts, 0))
    sanitized['closed'] = False
    sanitized['exitReason'] = None
    sanitized['partialSellInProgress'] = False
    sanitized['exitInProgress'] = False
    sanitized['pendingSell'] = False
    sanitized['pendingSellReason'] = None
    return sanitized


def recover_position_state(position_size_sol, token_amount_raw, token_decimals, entry_execution_json=None, raw_monitor_state=None, fallback_position_size_sol=0.06):
    execution = parse_entry_execution(entry_execution_json)
    monitor_state = raw_monitor_state if isinstance(raw_monitor_state, dict) else {}

    recovered_position_size_sol = _safe_float(position_size_sol, 0.0)
    if recovered_position_size_sol <= 0:
        recovered_position_size_sol = _safe_float(execution.get('inputAmount') if execution else None, 0.0)
    if recovered_position_size_sol <= 0:
        recovered_position_size_sol = _safe_float(fallback_position_size_sol, 0.06)

    stored_token_amount_raw = _safe_int(token_amount_raw, 0)
    stored_token_decimals = _safe_int(token_decimals, 6)
    recovered_token_amount_raw = stored_token_amount_raw
    recovered_token_decimals = stored_token_decimals
    recovery_source = 'stored'

    if recovered_token_amount_raw <= 0:
        recovered_token_amount_raw = _safe_int(monitor_state.get('tokenAmount'), 0)
        recovered_token_decimals = _safe_int(monitor_state.get('tokenDecimals'), recovered_token_decimals)
        if recovered_token_amount_raw > 0:
            recovery_source = 'monitor_state'

    if recovered_token_amount_raw <= 0 and execution:
        recovered_token_amount_raw = _safe_int(execution.get('quotedOutAmountRaw'), 0)
        recovered_token_decimals = _safe_int(execution.get('outputDecimals'), recovered_token_decimals)
        if recovered_token_amount_raw > 0:
            recovery_source = 'entry_execution'

    if recovered_token_amount_raw <= 0:
        recovery_source = 'missing'

    return {
        'position_size_sol': recovered_position_size_sol,
        'token_amount_raw': recovered_token_amount_raw,
        'token_decimals': recovered_token_decimals,
        'recovery_source': recovery_source,
    }


class Position:
    """Tracks an open paper trade position."""
    __slots__ = [
        'trade_id', 'token_ca', 'symbol', 'signal_ts', 'entry_price', 'entry_ts',
        'pool_address', 'peak_pnl', 'trailing_active', 'bars_held', 'last_bar_ts',
        'strategy_stage', 'lifecycle_id', 'exit_rules', 'position_size_sol',
        'token_amount_raw', 'token_decimals', 'exit_quote_failures', 'last_exit_quote_failure', 'last_mark_ts',
        'monitor_state', 'entry_execution_json', 'premium_signal_id', 'signal_type',
        'price_ring', 'vel_history',
        'trail_factor',  # ExitMatrix trail ratchet (in-memory, persistent)
        '_guardian_velocity', '_guardian_tick_vol',  # Written by Guardian thread
        'peak_ts', '_initial_tick_vol',  # A3 (time-decay) and A4 (flat-top) fields
    ]

    def __init__(self, trade_id, token_ca, symbol, signal_ts, entry_price, entry_ts, pool_address, strategy_stage, lifecycle_id, exit_rules, position_size_sol=0.06, token_amount_raw=0, token_decimals=0, exit_quote_failures=0, last_exit_quote_failure=None, monitor_state=None, entry_execution_json=None):
        self.trade_id = trade_id
        self.token_ca = token_ca
        self.symbol = symbol
        self.signal_ts = signal_ts
        self.entry_price = entry_price
        self.entry_ts = entry_ts
        self.pool_address = pool_address
        self.strategy_stage = strategy_stage
        self.lifecycle_id = lifecycle_id
        self.exit_rules = exit_rules or {}
        self.position_size_sol = float(position_size_sol or 0.06)
        self.token_decimals = int(token_decimals or 6)
        estimated_amount = 0
        if not token_amount_raw and self.entry_price and self.entry_price > 0:
            estimated_amount = int((self.position_size_sol / self.entry_price) * (10 ** self.token_decimals))
        self.token_amount_raw = int(token_amount_raw or estimated_amount or 0)
        self.exit_quote_failures = int(exit_quote_failures or 0)
        self.last_exit_quote_failure = last_exit_quote_failure or None
        self.peak_pnl = 0.0
        self.peak_ts = int(entry_ts)  # A3: timestamp of last peak_pnl update (for time-decay trail)
        self.trailing_active = False
        self.bars_held = 0
        self.last_bar_ts = int(entry_ts)
        self.last_mark_ts = int(entry_ts)
        self.monitor_state = monitor_state or {}
        self.entry_execution_json = entry_execution_json
        self.premium_signal_id = None
        self.signal_type = None
        # B+C velocity system: Guardian fills every 3s, 20 slots = 60s history
        self.price_ring = deque(maxlen=20)
        # Multi-round velocity smoothing: last 3 vel_30s readings → avg
        self.vel_history = deque(maxlen=3)
        # ExitMatrix velocity & trail ratchet (persist across cycles, was lost when stored on w_entry)
        self.trail_factor = 0.0
        self._guardian_velocity = 0  # Set by Guardian thread, consumed by ExitMatrix
        self._guardian_tick_vol = 0  # Set by Guardian thread


def build_lifecycle_id(token_ca, signal_ts):
    return f"{token_ca}:{int(signal_ts)}"


def stage_seq(stage_name):
    return {'stage1': 1, 'stage2A': 2, 'stage3': 3}.get(stage_name, 0)


def effective_lifecycle_id(token_ca, signal_ts, lifecycle_id=None):
    return lifecycle_id or build_lifecycle_id(token_ca, signal_ts)


def row_effective_lifecycle_id(row):
    return effective_lifecycle_id(row['token_ca'], row['signal_ts'], row['lifecycle_id'])


def normalize_epoch_ts(value):
    if value is None:
        return None
    value = int(value)
    return value // 1000 if value > 1e12 else value


def stage1_sort_key(row):
    entry_ts = normalize_epoch_ts(row['entry_ts'])
    return (entry_ts if entry_ts is not None else float('inf'), int(row['id']))


def is_valid_child_row(row, canonical_stage1, plausible_stage1_roots, rows_by_id):
    stage = row['strategy_stage'] or 'stage1'
    if stage not in ('stage2A', 'stage3') or canonical_stage1 is None:
        return False

    child_lifecycle_id = row_effective_lifecycle_id(row)
    canonical_lifecycle_id = row_effective_lifecycle_id(canonical_stage1)
    if child_lifecycle_id != canonical_lifecycle_id:
        return False

    parent = rows_by_id.get(row['parent_trade_id']) if row['parent_trade_id'] is not None else None
    if row['parent_trade_id'] == canonical_stage1['id']:
        if parent is None or (parent['strategy_stage'] or 'stage1') != 'stage1':
            return False
        if row_effective_lifecycle_id(parent) != canonical_lifecycle_id:
            return False
        if stage == 'stage2A' and (parent['exit_reason'] or '') != 'sl':
            return False
        return True

    if len(plausible_stage1_roots) != 1:
        return False

    if parent is not None:
        if (parent['strategy_stage'] or 'stage1') != 'stage1':
            return False
        if row_effective_lifecycle_id(parent) != canonical_lifecycle_id:
            return False
        if stage == 'stage2A' and (parent['exit_reason'] or '') != 'sl':
            return False
    elif stage == 'stage2A' and row['armed_ts'] is None:
        return False

    return True


def summarize_lifecycle_rows(rows):
    rows = sorted(rows, key=lambda row: (normalize_epoch_ts(row['signal_ts']) or 0, int(row['id'])))
    rows_by_id = {row['id']: row for row in rows}
    stage1_rows = [row for row in rows if (row['strategy_stage'] or 'stage1') == 'stage1']
    plausible_stage1_roots = [row for row in stage1_rows if row['parent_trade_id'] is None]
    canonical_candidates = plausible_stage1_roots or stage1_rows
    canonical_stage1 = min(canonical_candidates, key=stage1_sort_key) if canonical_candidates else None

    summary = {
        'canonical_stage1': canonical_stage1,
        'canonical_stage1_id': canonical_stage1['id'] if canonical_stage1 else None,
        'plausible_stage1_root_count': len(plausible_stage1_roots),
        'first_peak_pct': 0.0,
        'stage1_stop_ts': None,
        'valid_stage2a_row': None,
        'valid_stage3_row': None,
        'stage3_peak_price': None,
        'stage3_qualifying_exit_ts': None,
        'stage3_dormant': False,
        'stage3_blacklisted': False,
    }

    for row in stage1_rows:
        summary['first_peak_pct'] = max(
            summary['first_peak_pct'],
            float(row['peak_pnl'] or 0) * 100.0,
            float(row['first_peak_pct'] or 0) or 0.0,
        )

    if canonical_stage1 is not None and (canonical_stage1['exit_reason'] or '') == 'sl':
        summary['stage1_stop_ts'] = canonical_stage1['exit_ts']

    for row in rows:
        stage = row['strategy_stage'] or 'stage1'
        if row['stage3_peak_price'] is not None and summary['stage3_peak_price'] is None:
            summary['stage3_peak_price'] = row['stage3_peak_price']
        if row['stage3_qualifying_exit_ts'] is not None and summary['stage3_qualifying_exit_ts'] is None:
            summary['stage3_qualifying_exit_ts'] = row['stage3_qualifying_exit_ts']
        if row['stage3_blacklisted']:
            summary['stage3_blacklisted'] = True
            summary['stage3_dormant'] = False
        elif row['stage3_dormant'] and not summary['stage3_blacklisted']:
            summary['stage3_dormant'] = True
        if stage not in ('stage2A', 'stage3'):
            continue
        if not is_valid_child_row(row, canonical_stage1, plausible_stage1_roots, rows_by_id):
            continue
        key = 'valid_stage2a_row' if stage == 'stage2A' else 'valid_stage3_row'
        current = summary[key]
        if current is None or int(row['id']) < int(current['id']):
            summary[key] = row
        if stage == 'stage2A' and summary['stage1_stop_ts'] is None and row['armed_ts'] is not None:
            summary['stage1_stop_ts'] = row['armed_ts']

    if summary['valid_stage3_row'] is not None:
        summary['stage3_dormant'] = False
        summary['stage3_blacklisted'] = False

    return summary


def load_lifecycle_rows(db, lifecycle_id):
    return db.execute("""
        SELECT id, token_ca, symbol, signal_ts, strategy_stage, exit_reason, pnl_pct,
               peak_pnl, entry_ts, exit_ts, lifecycle_id, parent_trade_id, bars_held,
               first_peak_pct, rolling_low_price, rolling_low_ts, reentry_source, armed_ts,
               stage3_peak_price, stage3_qualifying_exit_ts, stage3_dormant, stage3_blacklisted
        FROM paper_trades
        WHERE COALESCE(lifecycle_id, token_ca || ':' || signal_ts) = ?
        ORDER BY signal_ts ASC, id ASC
    """, (lifecycle_id,)).fetchall()




def get_exit_rules_for_stage(strategy_config, stage_name):
    stage_rules = (strategy_config or {}).get('stageRules') or {}
    if stage_name == 'stage1':
        return dict(stage_rules.get('stage1Exit') or DEFAULT_STAGE1_EXIT)
    return dict(DEFAULT_STAGE1_EXIT)


def build_lifecycle_state(lifecycle_id, token_ca, symbol, signal_ts, premium_signal_id=None, signal_type=None):
    return {
        'lifecycle_id': lifecycle_id,
        'token_ca': token_ca,
        'symbol': symbol,
        'signal_ts': signal_ts,
        'premium_signal_id': premium_signal_id,
        'signal_type': signal_type,
        'first_peak_pct': 0.0,
        'stage1_trade_id': None,
        'stage2a_trade_id': None,
        'stage3_trade_id': None,
        'stage2a_attempted': False,
        'stage3_attempted': False,
        'stage1_stop_ts': None,
        'rolling_low_after_stop': None,
        'rolling_low_ts': None,
        'stage3_peak_price': None,
        'stage3_qualifying_exit_ts': None,
        'stage3_dormant': False,
        'stage3_blacklisted': False,
    }


def restore_lifecycles(db):
    lifecycles = {}
    rows = db.execute("""
        SELECT id, token_ca, symbol, signal_ts, strategy_stage, exit_reason, pnl_pct,
               peak_pnl, entry_ts, exit_ts, lifecycle_id, parent_trade_id, bars_held,
               first_peak_pct, rolling_low_price, rolling_low_ts, reentry_source, armed_ts,
               stage3_peak_price, stage3_qualifying_exit_ts, stage3_dormant, stage3_blacklisted,
               premium_signal_id, signal_type
        FROM paper_trades
        ORDER BY signal_ts ASC, id ASC
    """).fetchall()
    grouped_rows = defaultdict(list)
    for row in rows:
        grouped_rows[row_effective_lifecycle_id(row)].append(row)

    for lifecycle_id, lifecycle_rows in grouped_rows.items():
        first_row = lifecycle_rows[0]
        summary = summarize_lifecycle_rows(lifecycle_rows)
        valid_stage2a_row = summary['valid_stage2a_row']
        valid_stage3_row = summary['valid_stage3_row']
        item = build_lifecycle_state(lifecycle_id, first_row['token_ca'], first_row['symbol'], first_row['signal_ts'], first_row['premium_signal_id'] if 'premium_signal_id' in first_row.keys() else None, first_row['signal_type'] if 'signal_type' in first_row.keys() else None)
        item['first_peak_pct'] = summary['first_peak_pct']
        item['stage1_trade_id'] = summary['canonical_stage1_id']
        item['stage2a_trade_id'] = valid_stage2a_row['id'] if valid_stage2a_row else None
        item['stage3_trade_id'] = valid_stage3_row['id'] if valid_stage3_row else None
        item['stage2a_attempted'] = valid_stage2a_row is not None
        item['stage1_stop_ts'] = summary['stage1_stop_ts']
        item['stage3_peak_price'] = summary['stage3_peak_price']
        item['stage3_qualifying_exit_ts'] = summary['stage3_qualifying_exit_ts']
        if valid_stage2a_row is not None:
            item['rolling_low_after_stop'] = valid_stage2a_row['rolling_low_price']
            item['rolling_low_ts'] = valid_stage2a_row['rolling_low_ts']

        if valid_stage3_row is not None:
            item['stage3_attempted'] = True
            item['stage3_dormant'] = False
            item['stage3_blacklisted'] = False
        elif summary['stage3_blacklisted']:
            item['stage3_attempted'] = True
            item['stage3_dormant'] = False
            item['stage3_blacklisted'] = True
        elif summary['stage3_dormant']:
            item['stage3_attempted'] = False
            item['stage3_dormant'] = True
            item['stage3_blacklisted'] = False
        else:
            item['stage3_attempted'] = False
            item['stage3_dormant'] = False
            item['stage3_blacklisted'] = False

        lifecycles[lifecycle_id] = item

    return lifecycles




# === Daily Report ===

def summarize_rows(rows):
    if not rows:
        return None
    pnls = [r['pnl_pct'] for r in rows]
    n = len(pnls)
    ev = sum(pnls) / n
    wins = sum(1 for p in pnls if p > 0)
    wr = wins / n
    std = (sum((p - ev) ** 2 for p in pnls) / n) ** 0.5
    sharpe = ev / std if std > 0 else 0
    return {
        'n': n,
        'ev': ev,
        'wr': wr,
        'sharpe': sharpe,
        'total_pnl': sum(pnls)
    }


def print_summary_block(title, rows):
    summary = summarize_rows(rows)
    if not summary:
        log.info(f"  {title}: no trades")
        return
    log.info(
        f"  {title}: n={summary['n']}  EV={summary['ev']*100:+.2f}%  "
        f"WR={summary['wr']*100:.1f}%  Sharpe={summary['sharpe']:.3f}  "
        f"Total={summary['total_pnl']*100:+.2f}%"
    )


def print_daily_report(db, date_str=None):
    """Print daily statistics."""
    if not date_str:
        date_str = datetime.utcnow().strftime('%Y-%m-%d')

    rows = db.execute("""
        SELECT pnl_pct, exit_reason, market_regime, replay_source, bars_held,
               strategy_stage, stage_outcome, reentry_source, lifecycle_id
        FROM paper_trades
        WHERE exit_reason IS NOT NULL
          AND date(exit_ts, 'unixepoch') = ?
    """, (date_str,)).fetchall()

    if not rows:
        log.info(f"=== Daily Report {date_str}: No closed trades ===")
        return

    real_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'real_kline_replay']
    live_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'live_monitor']

    by_reason = defaultdict(list)
    by_stage = defaultdict(list)
    by_stage_outcome = defaultdict(list)
    by_reentry_source = defaultdict(list)
    by_regime = defaultdict(list)
    lifecycle_stage_counts = defaultdict(set)
    for r in rows:
        by_reason[r['exit_reason']].append(r['pnl_pct'])
        by_stage[r['strategy_stage'] or 'stage1'].append(r['pnl_pct'])
        by_stage_outcome[r['stage_outcome'] or 'unknown'].append(r['pnl_pct'])
        by_reentry_source[r['reentry_source'] or 'none'].append(r['pnl_pct'])
        by_regime[r['market_regime'] or 'unknown'].append(r['pnl_pct'])
        lifecycle_stage_counts[r['lifecycle_id'] or 'unknown'].add(r['strategy_stage'] or 'stage1')

    log.info(f"{'='*60}")
    log.info(f"  Daily Report: {date_str}")
    log.info(f"{'='*60}")
    print_summary_block('All trades', rows)
    print_summary_block('Live monitor', live_rows)
    print_summary_block('Real K-line replay', real_rows)
    log.info(f"")
    log.info(f"  By Exit Reason:")
    for reason, ps in sorted(by_reason.items()):
        r_ev = sum(ps) / len(ps)
        log.info(f"    {reason:10s}  n={len(ps):3d}  EV={r_ev*100:+.2f}%")
    log.info(f"")
    log.info(f"  By Strategy Stage:")
    for stage, ps in sorted(by_stage.items()):
        s_ev = sum(ps) / len(ps)
        log.info(f"    {stage:10s}  n={len(ps):3d}  EV={s_ev*100:+.2f}%")
    log.info(f"")
    log.info(f"  By Stage Outcome:")
    for outcome, ps in sorted(by_stage_outcome.items()):
        o_ev = sum(ps) / len(ps)
        log.info(f"    {outcome:18s}  n={len(ps):3d}  EV={o_ev*100:+.2f}%")
    log.info(f"")
    log.info(f"  By Reentry Source:")
    for source, ps in sorted(by_reentry_source.items()):
        s_ev = sum(ps) / len(ps)
        log.info(f"    {source:18s}  n={len(ps):3d}  EV={s_ev*100:+.2f}%")
    log.info(f"")
    log.info(f"  By Market Regime:")
    for regime, ps in sorted(by_regime.items()):
        r_ev = sum(ps) / len(ps)
        log.info(f"    {regime:10s}  n={len(ps):3d}  EV={r_ev*100:+.2f}%")

    by_source = defaultdict(list)
    for r in rows:
        by_source[r['replay_source'] or 'live_monitor'].append(r['pnl_pct'])

    log.info(f"")
    log.info(f"  By Replay Source:")
    for source, ps in sorted(by_source.items()):
        s_ev = sum(ps) / len(ps)
        log.info(f"    {source:16s}  n={len(ps):3d}  EV={s_ev*100:+.2f}%")

    lifecycle_counts = defaultdict(int)
    for stages in lifecycle_stage_counts.values():
        lifecycle_counts[len(stages)] += 1
    log.info(f"")
    log.info(f"  Lifecycle Stage Counts:")
    for stage_count, lifecycle_count in sorted(lifecycle_counts.items()):
        log.info(f"    {stage_count} stage(s): {lifecycle_count}")
    log.info(f"{'='*60}")


def print_all_stats(db):
    """Print cumulative stats across all dates."""
    rows = db.execute("""
        SELECT pnl_pct, exit_reason, market_regime, replay_source, bars_held,
               strategy_stage, stage_outcome, reentry_source, lifecycle_id,
               date(exit_ts, 'unixepoch') as exit_date
        FROM paper_trades
        WHERE exit_reason IS NOT NULL
        ORDER BY exit_ts
    """).fetchall()

    if not rows:
        log.info("No completed paper trades yet.")
        return

    real_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'real_kline_replay']
    live_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'live_monitor']

    log.info(f"{'='*60}")
    log.info(f"  Cumulative Paper Trade Stats")
    log.info(f"{'='*60}")
    print_summary_block('All trades', rows)
    print_summary_block('Live monitor', live_rows)
    print_summary_block('Real K-line replay', real_rows)

    # By date
    by_date = defaultdict(list)
    by_stage = defaultdict(list)
    by_stage_outcome = defaultdict(list)
    by_reentry_source = defaultdict(list)
    by_regime = defaultdict(list)
    lifecycle_stage_counts = defaultdict(set)
    for r in rows:
        by_date[r['exit_date']].append(r['pnl_pct'])
        by_stage[r['strategy_stage'] or 'stage1'].append(r['pnl_pct'])
        by_stage_outcome[r['stage_outcome'] or 'unknown'].append(r['pnl_pct'])
        by_reentry_source[r['reentry_source'] or 'none'].append(r['pnl_pct'])
        by_regime[r['market_regime'] or 'unknown'].append(r['pnl_pct'])
        lifecycle_stage_counts[r['lifecycle_id'] or 'unknown'].add(r['strategy_stage'] or 'stage1')

    log.info(f"")
    log.info(f"  By Date:")
    for date, ps in sorted(by_date.items()):
        d_ev = sum(ps) / len(ps)
        d_wr = sum(1 for p in ps if p > 0) / len(ps)
        log.info(f"    {date}  n={len(ps):3d}  EV={d_ev*100:+.2f}%  WR={d_wr*100:.1f}%")

    log.info(f"")
    log.info(f"  By Strategy Stage:")
    for stage, ps in sorted(by_stage.items()):
        s_ev = sum(ps) / len(ps)
        s_wr = sum(1 for p in ps if p > 0) / len(ps)
        log.info(f"    {stage:10s}  n={len(ps):3d}  EV={s_ev*100:+.2f}%  WR={s_wr*100:.1f}%")

    log.info(f"")
    log.info(f"  By Stage Outcome:")
    for outcome, ps in sorted(by_stage_outcome.items()):
        o_ev = sum(ps) / len(ps)
        o_wr = sum(1 for p in ps if p > 0) / len(ps)
        log.info(f"    {outcome:18s}  n={len(ps):3d}  EV={o_ev*100:+.2f}%  WR={o_wr*100:.1f}%")

    log.info(f"")
    log.info(f"  By Reentry Source:")
    for source, ps in sorted(by_reentry_source.items()):
        s_ev = sum(ps) / len(ps)
        s_wr = sum(1 for p in ps if p > 0) / len(ps)
        log.info(f"    {source:18s}  n={len(ps):3d}  EV={s_ev*100:+.2f}%  WR={s_wr*100:.1f}%")

    log.info(f"")
    log.info(f"  By Market Regime:")
    for regime, ps in sorted(by_regime.items()):
        r_ev = sum(ps) / len(ps)
        r_wr = sum(1 for p in ps if p > 0) / len(ps)
        log.info(f"    {regime:10s}  n={len(ps):3d}  EV={r_ev*100:+.2f}%  WR={r_wr*100:.1f}%")

    lifecycle_counts = defaultdict(int)
    for stages in lifecycle_stage_counts.values():
        lifecycle_counts[len(stages)] += 1
    log.info(f"")
    log.info(f"  Lifecycle Stage Counts:")
    for stage_count, lifecycle_count in sorted(lifecycle_counts.items()):
        log.info(f"    {stage_count} stage(s): {lifecycle_count}")

    # Open positions
    open_count = db.execute("SELECT COUNT(*) as c FROM paper_trades WHERE exit_reason IS NULL").fetchone()['c']
    if open_count:
        log.info(f"")
        log.info(f"  Open positions: {open_count}")

    log.info(f"{'='*60}")


# === K-line Data Access ===

def init_kline_db():
    """Open kline cache DB."""
    db = sqlite3.connect(KLINE_DB)
    db.row_factory = sqlite3.Row
    return db


def get_kline_bars(kline_db, token_ca, start_ts, limit=120):
    """Fetch K-line bars for a token from signal time onward.

    Args:
        kline_db: kline_cache.db connection
        token_ca: token contract address
        start_ts: signal timestamp in seconds
        limit: max bars to fetch (default 120 = 2h)

    Returns:
        List of dicts with {timestamp, open, high, low, close, volume}
    """
    rows = kline_db.execute("""
        SELECT timestamp, open, high, low, close, volume
        FROM kline_1m
        WHERE token_ca = ? AND timestamp >= ?
        ORDER BY timestamp ASC
        LIMIT ?
    """, (token_ca, start_ts, limit)).fetchall()

    return [dict(row) for row in rows]


# === Dry Run Mode ===

def dry_run(db):
    """Simulate staged paper trading on recent signals using real K-line data."""

    log.info("=== DRY RUN MODE (REAL K-LINE REPLAY) ===")
    strategy_config = load_active_strategy_config()
    stage_rules = strategy_config.get('stageRules') or {}
    stage1_exit = get_exit_rules_for_stage(strategy_config, 'stage1')
    min_super_index = int(((strategy_config.get('entryTimingFilters') or {}).get('minSuperIndex')) or 80)
    strategy_id = strategy_config.get('strategyId') or DEFAULT_STRATEGY_ID
    strategy_role = strategy_config.get('strategyRole') or DEFAULT_STRATEGY_ROLE
    position_size_sol = get_paper_position_size_sol(strategy_config)

    try:
        kline_db = init_kline_db()
    except Exception as e:
        log.error(f"Failed to open kline_cache.db: {e}")
        return

    kline_count = kline_db.execute("SELECT COUNT(*) FROM kline_1m").fetchone()[0]
    log.info(f"K-line DB: {kline_count:,} bars available")

    signals = get_recent_signals(limit=500)
    if not signals:
        log.warning("No recent signals found in premium_signals")
        return

    log.info(f"Found {len(signals)} recent signals")

    real_klines_used = 0
    completed = []
    stage_counts = defaultdict(int)
    duplicate_prevented = 0

    for sig in signals:
        token_ca = sig['token_ca']
        if not token_ca:
            continue
        symbol = sig['symbol'] or token_ca[:8]
        signal_ts_ms = sig['timestamp']
        signal_ts = signal_ts_ms // 1000 if signal_ts_ms > 1e12 else signal_ts_ms
        lifecycle_id = build_lifecycle_id(token_ca, signal_ts_ms)
        super_idx = parse_super_index(sig.get('description') or '')
        if super_idx is None or super_idx <= min_super_index:
            stage_counts['stage1_rejected'] += 1
            continue

        bars = get_kline_bars(kline_db, token_ca, signal_ts, limit=max(stage1_exit['timeoutMinutes'], 240))
        if len(bars) < 5:
            log.info(f"  [{symbol}] insufficient K-line bars ({len(bars)}), skipping")
            continue

        real_klines_used += 1
        existing_stages = set()
        lifecycle = build_lifecycle_state(lifecycle_id, token_ca, symbol, int(signal_ts_ms))
        stage2_result = None

        def replay_trade(stage_name, start_index, exit_rules):
            entry_bar = bars[start_index]
            entry_price = entry_bar['close'] or entry_bar['open']
            if not entry_price or entry_price <= 0:
                return None
            peak_pnl = 0.0
            trailing_active = False
            exit_reason = 'timeout'
            exit_pnl = 0.0
            exit_price = entry_price
            bars_held = 0
            for offset, bar in enumerate(bars[start_index:], start=1):
                price = bar['close'] or bar.get('open') or entry_price
                if not price or price <= 0:
                    continue
                pnl = (price - entry_price) / entry_price
                peak_pnl = max(peak_pnl, pnl)
                bars_held = offset
                if not trailing_active and peak_pnl >= pct_to_decimal(exit_rules['trailStartPct']):
                    trailing_active = True
                if pnl <= -pct_to_decimal(exit_rules['stopLossPct']):
                    exit_reason = 'sl'
                    exit_pnl = -pct_to_decimal(exit_rules['stopLossPct'])
                    exit_price = entry_price * (1 + exit_pnl)
                    break
                if trailing_active:
                    trail_level = max(0.0, peak_pnl * float(exit_rules['trailFactor']))
                    if pnl <= trail_level:
                        exit_reason = 'trail'
                        exit_pnl = max(pnl, trail_level)
                        exit_price = entry_price * (1 + exit_pnl)
                        break
                exit_pnl = pnl
                exit_price = price
                if offset >= int(exit_rules['timeoutMinutes']):
                    exit_reason = 'timeout'
                    break
            exit_bar = bars[min(start_index + max(bars_held - 1, 0), len(bars) - 1)]
            return {
                'entry_price': entry_price,
                'entry_ts': int(entry_bar['timestamp']),
                'exit_price': exit_price,
                'exit_ts': int(exit_bar['timestamp']),
                'exit_reason': exit_reason,
                'exit_pnl': exit_pnl,
                'bars_held': bars_held,
                'peak_pnl': peak_pnl,
                'trailing_active': trailing_active,
            }

        if 'stage1' in existing_stages:
            duplicate_prevented += 1
            continue
        stage1_result = replay_trade('stage1', 0, stage1_exit)
        if not stage1_result:
            continue
        lifecycle['first_peak_pct'] = max(lifecycle['first_peak_pct'], stage1_result['peak_pnl'] * 100.0)
        db.execute("""
            INSERT INTO paper_trades
                (strategy_id, strategy_role, strategy_stage, stage_outcome,
                 token_ca, symbol, signal_ts, entry_price, entry_ts,
                 exit_price, exit_ts, exit_reason, pnl_pct, bars_held,
                 market_regime, replay_source, peak_pnl, trailing_active,
                 lifecycle_id, stage_seq, trigger_ts, trigger_price, first_peak_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            strategy_id, strategy_role, 'stage1', f"stage1_{stage1_result['exit_reason']}",
            token_ca, symbol, int(signal_ts_ms), stage1_result['entry_price'], stage1_result['entry_ts'],
            stage1_result['exit_price'], stage1_result['exit_ts'], stage1_result['exit_reason'], stage1_result['exit_pnl'], stage1_result['bars_held'],
            'real_kline', 'real_kline_replay', stage1_result['peak_pnl'], int(stage1_result['trailing_active']),
            lifecycle_id, stage_seq('stage1'), stage1_result['entry_ts'], stage1_result['entry_price'], lifecycle['first_peak_pct']
        ))
        stage1_trade_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
        db.commit()
        existing_stages.add('stage1')
        lifecycle['stage1_trade_id'] = stage1_trade_id
        stage_counts['stage1_entered'] += 1
        stage_counts[f"stage1_exit_{stage1_result['exit_reason']}"] += 1
        completed.append(stage1_result['exit_pnl'])



    kline_db.close()

    log.info(f"\n=== DRY RUN COMPLETE ===")
    log.info(f"Real K-line replays: {real_klines_used}")
    log.info(f"Stage 1 entered / rejected: {stage_counts['stage1_entered']} / {stage_counts['stage1_rejected']}")
    log.info(f"Stage 1 exit breakdown: sl={stage_counts['stage1_exit_sl']} trail={stage_counts['stage1_exit_trail']} timeout={stage_counts['stage1_exit_timeout']}")
    log.info(f"Duplicate prevented: {duplicate_prevented}")

    if completed:
        n = len(completed)
        ev = sum(completed) / n
        wins = sum(1 for p in completed if p > 0)
        wr = wins / n
        total_pnl = sum(completed)
        std = (sum((p - ev) ** 2 for p in completed) / n) ** 0.5
        sharpe = ev / std if std > 0 else 0
        log.info(f"\n  Trades: {n}")
        log.info(f"  EV: {ev*100:+.3f}%")
        log.info(f"  WR: {wr*100:.1f}% ({wins} wins)")
        log.info(f"  Sharpe: {sharpe:.3f}")
        log.info(f"  Total PnL: {total_pnl*100:+.2f}%")

    log.info(f"")
    print_all_stats(db)


def get_signal_minute_ts(signal_ts):
    signal_sec = signal_ts // 1000 if signal_ts > 1e12 else signal_ts
    return int(signal_sec // 60 * 60)


def _format_ts_utc(ts):
    ts = normalize_epoch_ts(ts)
    if ts is None:
        return 'n/a'
    return datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S UTC')


def _normalize_bar_series(bars):
    normalized = []
    for bar in bars or []:
        ts = normalize_epoch_ts(bar.get('ts'))
        if ts is None:
            continue
        row = dict(bar)
        row['ts'] = ts
        normalized.append(row)
    return normalized


def select_signal_entry_bar(bars, signal_ts, signal_minute_ts):
    normalized = _normalize_bar_series(bars)
    if not normalized:
        return None, 'no_bars', {'count': 0}

    signal_sec = normalize_epoch_ts(signal_ts)
    target_minute = normalize_epoch_ts(signal_minute_ts)
    if signal_sec is None or target_minute is None:
        return None, 'invalid_signal_ts', {'count': len(normalized)}

    exact = next((bar for bar in normalized if bar['ts'] == target_minute), None)
    if exact is not None:
        return exact, 'exact_minute', {'count': len(normalized)}

    nearby = [bar for bar in normalized if abs(bar['ts'] - target_minute) <= PENDING_ENTRY_BAR_TOLERANCE_SEC]
    if nearby:
        nearby.sort(key=lambda bar: (abs(bar['ts'] - target_minute), abs(bar['ts'] - signal_sec), -bar['ts']))
        return nearby[0], 'tolerant_minute', {
            'count': len(normalized),
            'delta_sec': nearby[0]['ts'] - target_minute,
        }

    past = [bar for bar in normalized if bar['ts'] <= signal_sec]
    if past:
        past.sort(key=lambda bar: (signal_sec - bar['ts'], -bar['ts']))
        nearest_past = past[0]
        age_sec = signal_sec - nearest_past['ts']
        if age_sec <= PENDING_ENTRY_NEAREST_PAST_MAX_SEC:
            return nearest_past, 'nearest_past', {
                'count': len(normalized),
                'age_sec': age_sec,
            }

    ts_values = [bar['ts'] for bar in normalized]
    return None, 'missing_signal_bar', {
        'count': len(normalized),
        'oldest_ts': min(ts_values),
        'newest_ts': max(ts_values),
        'target_ts': target_minute,
        'signal_ts': signal_sec,
    }


def log_pending_entry_issue(pending, message, level='info', force=False):
    now = time.time()
    last_debug_at = pending.get('last_debug_at') or 0
    if not force and (now - last_debug_at) < PENDING_ENTRY_DEBUG_INTERVAL_SEC:
        return
    pending['last_debug_at'] = now
    stage_age_sec = int(max(0, now - (pending.get('staged_at') or now)))
    prefix = (
        f"  Pending {pending.get('symbol', pending.get('token_ca', 'UNKNOWN')[:8])}/stage1 "
        f"age={stage_age_sec}s attempts={pending.get('attempts', 0)}"
    )
    if level == 'warning':
        log.warning(f"{prefix} {message}")
    elif level == 'error':
        log.error(f"{prefix} {message}")
    else:
        log.info(f"{prefix} {message}")


def close_position_with_guard_reason(db, pos, lifecycle, reason, pnl_pct, decision_type='guard_close', audit_extra=None, log_prefix='GUARD'):
    exit_ts = int(time.time())
    stage_outcome = f"{pos.strategy_stage}_{reason}"
    audit_payload = {
        'auditVersion': 1,
        'stage': pos.strategy_stage,
        'lifecycleId': pos.lifecycle_id,
        'decisionType': decision_type,
        'failureReason': pos.last_exit_quote_failure,
        'triggerPnlPct': _safe_float(float(pnl_pct) * 100.0, None),
    }
    if isinstance(audit_extra, dict):
        audit_payload.update(audit_extra)
    db.execute(
        """
        UPDATE paper_trades
        SET exit_price = ?, exit_ts = ?, exit_reason = ?, pnl_pct = ?,
            bars_held = ?, stage_outcome = ?, exit_quote_failures = ?, last_exit_quote_failure = ?,
            exit_execution_audit_json = ?, strategy_outcome = ?, execution_availability = ?, accounting_outcome = ?, synthetic_close = ?
        WHERE id = ?
        """,
        (
            0,
            exit_ts,
            reason,
            float(pnl_pct),
            pos.bars_held,
            stage_outcome,
            pos.exit_quote_failures,
            pos.last_exit_quote_failure,
            json.dumps(build_execution_audit(None, audit_payload)),
            'blocked_by_infra',
            'unavailable',
            'closed_synthetic',
            1,
            pos.trade_id,
        )
    )
    db.commit()
    if lifecycle is not None:
        lifecycle['stage3_dormant'] = False
        lifecycle['stage3_blacklisted'] = True
        lifecycle['stage3_attempted'] = True
    log.warning(
        f"  {log_prefix} {pos.symbol}/{pos.strategy_stage}: reason={reason} pnl={float(pnl_pct) * 100:+.1f}% "
        f"lifecycle={pos.lifecycle_id}"
    )


def close_position_as_trapped_no_route(db, pos, lifecycle, reason='trapped_no_route', pnl_pct=TRAPPED_NO_ROUTE_PNL_PCT, failure_count_field='noRouteFailureCount'):
    close_position_with_guard_reason(
        db,
        pos,
        lifecycle,
        reason=reason,
        pnl_pct=pnl_pct,
        decision_type='trap_close',
        audit_extra={failure_count_field: pos.exit_quote_failures},
        log_prefix='TRAPPED',
    )
    db.execute(
        """
        UPDATE paper_trades
        SET strategy_outcome = ?, execution_availability = ?, accounting_outcome = ?, synthetic_close = 1
        WHERE id = ?
        """,
        ('blocked_by_infra', 'unavailable', 'closed_synthetic', pos.trade_id)
    )
    db.commit()


# === Live Monitor Loop ===

def run_monitor(db):
    """Main monitoring loop."""
    strategy_config = load_active_strategy_config()
    stage_rules = strategy_config.get('stageRules') or {}
    paper_execution = get_paper_execution_config(strategy_config)
    stage1_exit = get_exit_rules_for_stage(strategy_config, 'stage1')
    min_super_index = int(((strategy_config.get('entryTimingFilters') or {}).get('minSuperIndex')) or 80)
    strategy_id = strategy_config.get('strategyId') or DEFAULT_STRATEGY_ID
    strategy_role = strategy_config.get('strategyRole') or DEFAULT_STRATEGY_ROLE
    position_size_sol = get_paper_position_size_sol(strategy_config)
    max_positions = get_paper_max_positions(strategy_config)
    
    # Initialize observation list and matrix evaluators
    watchlist = WatchlistStore()
    matrix_evaluator = MatrixEvaluator()
    exit_matrix_evaluator = ExitMatrixEvaluator()

    log.info("=== Paper Trade Monitor Started ===")
    log.info(f"  strategy={strategy_id} role={strategy_role}")
    log.info(f"  strategy registry: {REGISTRY_JSON}")
    log.info(f"  paper execution size: {position_size_sol} SOL")
    log.info(f"  max open positions: {max_positions}")
    log.info(
        f"  paper execution: mode={paper_execution['executionMode']} entry={paper_execution['entryPriceSource']} "
        f"exit={paper_execution['exitPriceSource']} retries={paper_execution['quoteRetries']} "
        f"timeout_ms={paper_execution['quoteTimeoutMs']} penalty={paper_execution['applyPaperPenalty']}"
    )
    log.info(f"  stage1 exit: SL={stage1_exit['stopLossPct']}% Trail Start={stage1_exit['trailStartPct']}% Trail Factor={stage1_exit['trailFactor']*100:.0f}% Timeout={stage1_exit['timeoutMinutes']}min")
    log.info(f"  Signal poll: {SIGNAL_POLL_INTERVAL}s  Position poll: {POSITION_POLL_INTERVAL}s")
    if REMOTE_SIGNAL_URL:
        log.info(f"  Signal Source: remote export {REMOTE_SIGNAL_URL}")
    else:
        log.info(f"  Signal DB: {SENTIMENT_DB}")
    log.info(f"  Paper DB: {PAPER_DB}")

    freshness = get_signal_freshness()
    if freshness['latest_ts']:
        latest_iso = datetime.utcfromtimestamp(freshness['latest_ts']).strftime('%Y-%m-%d %H:%M:%S UTC')
        log.info(f"  premium_signals latest: {latest_iso} ({freshness['age_minutes']} min ago, sample={freshness['total']}, source={freshness.get('source', 'unknown')})")
        if freshness['age_minutes'] is not None and freshness['age_minutes'] > 120:
            log.warning("  premium_signals is stale; paper trade monitor may idle until upstream signal source updates")
    else:
        log.warning(f"  premium_signals has no rows from {freshness.get('source', 'unknown')} source; paper trade monitor has no upstream signals to process")

    positions = {}
    positions_lock = threading.Lock()  # P6: shared lock for Guardian thread
    guardian_exit_queue = []  # P6: Guardian pushes exit signals here
    pending_entries = {}
    lifecycles = restore_lifecycles(db)
    sanitized_monitor_states = 0

    open_rows = db.execute("""
        SELECT id, token_ca, symbol, signal_ts, entry_price, entry_ts, peak_pnl, trailing_active, bars_held,
               strategy_stage, lifecycle_id, position_size_sol, token_amount_raw, token_decimals,
               entry_execution_json, monitor_state_json, exit_quote_failures, last_exit_quote_failure,
               premium_signal_id, signal_type
        FROM paper_trades
        WHERE exit_reason IS NULL
    """).fetchall()
    for r in open_rows:
        pool = get_pool_address(r['token_ca'])
        raw_monitor_state = parse_monitor_state(r['monitor_state_json'])
        recovered = recover_position_state(
            r['position_size_sol'],
            r['token_amount_raw'],
            r['token_decimals'],
            r['entry_execution_json'],
            raw_monitor_state,
            get_paper_position_size_sol(strategy_config),
        )
        pos = Position(r['id'], r['token_ca'], r['symbol'], r['signal_ts'], r['entry_price'], r['entry_ts'], pool,
                       r['strategy_stage'] or 'stage1', r['lifecycle_id'] or build_lifecycle_id(r['token_ca'], r['signal_ts']),
                       get_exit_rules_for_stage(strategy_config, r['strategy_stage'] or 'stage1'),
                       recovered['position_size_sol'],
                       recovered['token_amount_raw'],
                       recovered['token_decimals'],
                       r['exit_quote_failures'] or 0,
                       r['last_exit_quote_failure'],
                       raw_monitor_state,
                       r['entry_execution_json'])
        pos.premium_signal_id = r['premium_signal_id'] if 'premium_signal_id' in r.keys() else None
        pos.signal_type = r['signal_type'] if 'signal_type' in r.keys() else None
        if recovered['recovery_source'] == 'entry_execution':
            db.execute(
                "UPDATE paper_trades SET position_size_sol = ?, token_amount_raw = ?, token_decimals = ? WHERE id = ?",
                (recovered['position_size_sol'], str(recovered['token_amount_raw']), recovered['token_decimals'], r['id'])
            )
            log.warning(
                f"  Recovered {pos.symbol}/{pos.strategy_stage} position size from entry_execution_json "
                f"lifecycle={pos.lifecycle_id}"
            )
        elif recovered['recovery_source'] == 'monitor_state':
            db.execute(
                "UPDATE paper_trades SET token_amount_raw = ?, token_decimals = ? WHERE id = ?",
                (str(recovered['token_amount_raw']), recovered['token_decimals'], r['id'])
            )
            log.info(
                f"  Recovered {pos.symbol}/{pos.strategy_stage} token amount from monitor_state_json "
                f"lifecycle={pos.lifecycle_id}"
            )
        elif recovered['recovery_source'] == 'missing':
            log.warning(
                f"  Restored {pos.symbol}/{pos.strategy_stage} without token_amount_raw; exit quote parity may be unreliable "
                f"lifecycle={pos.lifecycle_id}"
            )
        if has_partial_state_gap(recovered['token_amount_raw'], parse_entry_execution(r['entry_execution_json']), raw_monitor_state):
            lifecycle = lifecycles.setdefault(pos.lifecycle_id, build_lifecycle_state(pos.lifecycle_id, pos.token_ca, pos.symbol, pos.signal_ts, getattr(pos, 'premium_signal_id', None), getattr(pos, 'signal_type', None)))
            close_position_with_guard_reason(
                db,
                pos,
                lifecycle,
                reason='legacy_missing_partial_state',
                pnl_pct=0.0,
                decision_type='legacy_partial_guard',
                audit_extra={
                    'originalTokenAmountRaw': _safe_int((parse_entry_execution(r['entry_execution_json']) or {}).get('quotedOutAmountRaw'), 0),
                    'remainingTokenAmountRaw': recovered['token_amount_raw'],
                },
                log_prefix='LEGACY-GUARD',
            )
            continue
        pos.peak_pnl = r['peak_pnl'] or 0
        pos.trailing_active = bool(r['trailing_active'])
        pos.bars_held = r['bars_held'] or 0
        pos.last_bar_ts = int(r['entry_ts']) + max((r['bars_held'] or 0) - 1, 0) * 60
        pos.last_mark_ts = pos.last_bar_ts
        if pos.monitor_state:
            sync_position_from_monitor_state(pos)
            pos.peak_pnl = monitor_peak_pnl_decimal(pos.monitor_state, pos.peak_pnl)
            pos.trailing_active = bool(pos.monitor_state.get('breakeven', pos.monitor_state.get('trailingActive', pos.trailing_active)))
            pos.bars_held = int(pos.monitor_state.get('barsHeld', pos.bars_held) or pos.bars_held)
            pos.last_mark_ts = int(pos.monitor_state.get('lastMarkTs', pos.last_mark_ts) or pos.last_mark_ts)
            pos.last_bar_ts = pos.last_mark_ts or pos.last_bar_ts
        pos.monitor_state = sanitize_monitor_state(
            pos.monitor_state,
            token_ca=pos.token_ca,
            symbol=pos.symbol,
            entry_price=pos.entry_price,
            entry_ts=pos.entry_ts,
            position_size_sol=pos.position_size_sol,
            token_amount_raw=pos.token_amount_raw,
            token_decimals=pos.token_decimals,
            peak_pnl=pos.peak_pnl,
            trailing_active=pos.trailing_active,
            bars_held=pos.bars_held,
            last_mark_ts=pos.last_mark_ts,
        )
        if normalize_monitor_state_json(raw_monitor_state) != normalize_monitor_state_json(pos.monitor_state):
            db.execute(
                "UPDATE paper_trades SET monitor_state_json = ? WHERE id = ?",
                (json.dumps(pos.monitor_state, ensure_ascii=False), pos.trade_id)
            )
            sanitized_monitor_states += 1
        positions[pos.trade_id] = pos
        time.sleep(0.2)
    db.commit()

    if positions:
        log.info(f"  Restored {len(positions)} open positions")
    if sanitized_monitor_states:
        log.info(f"  Sanitized {sanitized_monitor_states} open monitor_state rows")

    last_id_row = db.execute("""
        SELECT MAX(
            CASE
                WHEN reentry_source = 'v2_event_awakening' AND trigger_ts IS NOT NULL THEN trigger_ts
                ELSE signal_ts
            END
        ) AS max_ts
        FROM paper_trades
    """).fetchone()
    last_signal_id = 0
    cursor_source = 'empty_source'

    try:
        recent_rows = get_recent_signals(limit=REMOTE_SIGNAL_LOOKBACK)
        if recent_rows:
            last_signal_id = max((r.get('id') or 0) for r in recent_rows)
            cursor_source = 'latest_source_snapshot'
    except Exception:
        recent_rows = []

    if last_signal_id <= 0 and last_id_row and last_id_row['max_ts']:
        try:
            signal_ts_cutoff = last_id_row['max_ts']
            eligible = [r for r in recent_rows if (r.get('timestamp') or 0) <= signal_ts_cutoff]
            if eligible:
                last_signal_id = max((r.get('id') or 0) for r in eligible)
                cursor_source = 'trade_timestamp_cutoff'
        except Exception:
            pass

    log.info(f"  Starting from signal ID > {last_signal_id} ({cursor_source})")

    last_signal_check = 0
    last_position_check = 0
    last_daily_report = None
    sol_price = None

    last_heartbeat = 0.0
    last_progress = time.time()
    _eval_rotation_offset = 0

    # --- P6: Start EXIT Guardian Thread ---
    exit_guardian = ExitGuardianThread(
        positions_ref=positions,
        positions_lock=positions_lock,
        watchlist_store_ref=watchlist,
        exit_queue=guardian_exit_queue,
        fetch_price_fn=fetch_realtime_price,
    )
    exit_guardian.start()

    # P6: Wire up SmartEntry's max_wait dynamic adjustment
    global _active_holdings_count
    _active_holdings_count = lambda: len(positions)

    while True:
      try:
        now = time.time()
        now_utc = datetime.utcfromtimestamp(now)

        if now - last_heartbeat >= HEARTBEAT_INTERVAL_SEC:
            freshness = get_signal_freshness()
            wl_watching = len(watchlist.get_watching())
            wl_holding = watchlist.get_active_count()
            log.info(
                f"[heartbeat] signals={freshness.get('total', 0)} source={freshness.get('source', 'unknown')} "
                f"age_min={freshness.get('age_minutes')} watching={wl_watching} holding={wl_holding} active_positions={len(positions)} pending={len(pending_entries)}"
            )
            last_heartbeat = now

        if now - last_progress >= HEARTBEAT_INTERVAL_SEC * 2:
            freshness = get_signal_freshness()
            source_age_min = freshness.get('age_minutes')
            progress_cause = 'awaiting_new_work'
            if source_age_min is None or (isinstance(source_age_min, (int, float)) and source_age_min > 10):
                progress_cause = 'source_freshness'
            elif pending_entries:
                progress_cause = 'entry_backlog_or_pool_lookup'
            elif positions:
                progress_cause = 'open_positions_waiting_exit'
            log.warning(
                f"No trading progress for {int(now - last_progress)}s; likely stalled on {progress_cause} "
                f"(source_age_min={source_age_min}, pending={len(pending_entries)}, active_positions={len(positions)})"
            )
            last_progress = now

        if now - last_signal_check >= SIGNAL_POLL_INTERVAL:
            last_signal_check = now
            try:
                new_signals = get_new_signals(last_signal_id)
                for sig in new_signals:
                    token_ca = sig['token_ca']
                    last_signal_id = sig['id']
                    signal_ts = sig['timestamp']
                    premium_signal_id = sig.get('id')
                    signal_type = sig.get('signal_type') or 'NEW_TRENDING'
                    lifecycle_id = build_lifecycle_id(token_ca, signal_ts)

                    if any(pos.lifecycle_id == lifecycle_id for pos in positions.values()) or lifecycle_id in pending_entries:
                        continue

                    if len(positions) + len(pending_entries) >= max_positions:
                        continue

                    existing = db.execute(
                        "SELECT id FROM paper_trades WHERE lifecycle_id = ? OR (token_ca = ? AND signal_ts = ? AND strategy_stage = 'stage1')",
                        (lifecycle_id, token_ca, signal_ts)
                    ).fetchone()
                    if existing:
                        continue

                    symbol = sig['symbol'] or token_ca[:8]
                    super_idx = parse_super_index(sig['description'] or '')

                    # Super Score filter:
                    # NOT_ATH: must have Super > min_super_index (config=70)
                    # ATH: no Super Score (None), skip this filter — ATH uses own pipeline
                    if not sig.get('is_ath') and (super_idx is None or super_idx <= min_super_index):
                        continue                    
                    top10_max = (strategy_config.get('signalFilters') or {}).get('top10PctPrimaryMax', 45.0)
                    # Config might have 100 as default from old JSON schema, if it's 100 we override to 45 for safety or honor it?
                    # Since user wants it active, let's strictly use 45.0 if it's 100 or missing, to enforce safety easily without JSON patching.
                    if top10_max >= 100.0:
                        top10_max = 45.0
                        
                    top10_pct = parse_top10_percent(sig['description'] or '')
                    if top10_pct is not None and top10_pct > top10_max:
                        log.info(f"  [PREBUY_FILTER] {symbol} BLOCKED: Top10 {top10_pct}% exceeds max allowed {top10_max}%, skipping")
                        continue

                    # Determine signal price immediately for reference
                    pool = get_pool_address(token_ca)
                    if not pool:
                        log.warning(f"  Could not find pool for {symbol}, skipping")
                        continue
                    time.sleep(0.1)
                    sig_price_val, _, _ = fetch_realtime_price(token_ca, pool, max_age_ms=15000)
                    sig_price = sig_price_val if sig_price_val and sig_price_val > 0 else None

                    log.info(f"  [WATCHLIST] Registering {symbol} ({signal_type}) Super={super_idx} Price={sig_price}")
                    watchlist.register(
                        ca=token_ca,
                        symbol=symbol,
                        signal_type='ATH' if sig.get('is_ath') else 'NOT_ATH',
                        pool_address=pool,
                        signal_ts=signal_ts,
                        premium_signal_id=premium_signal_id,
                        signal_price=sig_price,
                        signal_mc=sig.get('market_cap'),
                        signal_super=super_idx or 0,
                        signal_holders=sig.get('holders') or 0,
                        signal_vol24h=sig.get('volume_24h') or 0,
                        signal_tx24h=0, # not readily available in this row, evaluate_matrix gets recent bars anyway
                        signal_top10=top10_pct or 0
                    )
                    last_progress = time.time()
            except Exception as e:
                log.error(f"Signal check error: {e}")

        # --- Evaluate Watchlist Entries ---
        watching_entries = watchlist.get_watching()
        if watching_entries:
            log.info(f"  [WATCHLIST_SCAN] Scanning {len(watching_entries)} watching tokens...")
        wl_eval_count = 0
        wl_skip_cooldown = 0
        wl_skip_duplicate = 0
        for w_entry in watching_entries:
            try:
                lifecycle_id = build_lifecycle_id(w_entry['ca'], w_entry['signal_ts'])
                
                # Skip if already in pending or open positions
                if lifecycle_id in pending_entries or any(p.lifecycle_id == lifecycle_id for p in positions.values()):
                    wl_skip_duplicate += 1
                    continue
                
                # Adaptive evaluation interval:
                # Fresh entries (<5min): every 10s — need fast momentum detection
                # Mid-age (5-30min): every 30s — still active but less urgent
                # Mature (30min+): every 60s — long-term monitoring, save API quota
                entry_age_min = (time.time() - w_entry.get('added_at', time.time())) / 60
                if entry_age_min < 5:
                    eval_interval = 10.0
                elif entry_age_min < 30:
                    eval_interval = 30.0
                else:
                    eval_interval = 60.0
                if time.time() - w_entry.get('last_eval_at', 0) < eval_interval:
                    wl_skip_cooldown += 1
                    continue

                # P5: Self cooldown — respect cooldown_until on THIS entry
                # Catches the deadly "win → instant re-buy same CA → loss" pattern
                _cd_until = w_entry.get('cooldown_until', 0) or 0
                if _cd_until > time.time():
                    _cd_remain = int(_cd_until - time.time())
                    if _cd_remain > 15:  # only log if > 15s remaining to reduce spam
                        log.info(f"  [WATCHLIST] ⏳ {w_entry['symbol']} WAIT reason=post_exit_cooldown ({_cd_remain}s remaining)")
                    wl_skip_cooldown += 1
                    continue
                
                # Skip if max positions reached
                if len(positions) + len(pending_entries) >= max_positions:
                    log.info(f"  [WATCHLIST_SCAN] Max positions reached ({max_positions}), stopping scan")
                    break
                
                wl_eval_count += 1
                eval_res = matrix_evaluator.evaluate(w_entry)
                watchlist.update_scores(w_entry['id'], eval_res['scores'])
                
                # Update price bounds if we got a price
                if eval_res.get('current_price') and eval_res['current_price'] > 0:
                    watchlist.update_price_bounds(w_entry['id'], eval_res['current_price'])
                
                if eval_res['action'] == 'remove':
                    watchlist.mark_expired(w_entry['id'], eval_res['action_reason'])
                    log.info(f"  [WATCHLIST] 🗑️ Removed {w_entry['symbol']}: {eval_res['action_reason']}")
                elif eval_res['action'] == 'fire':
                    # P3: Minimum age filter — skip tokens younger than 3 minutes
                    # pump.fun's most toxic dump window is 0-3 min after launch
                    if entry_age_min < 3:
                        log.info(f"  [WATCHLIST] ⏳ {w_entry['symbol']} WAIT ({entry_age_min:.0f}min) reason=age_too_young (<3min)")
                        continue

                    # P2: Per-CA cooldown cross-check — if ANY entry for this CA is in cooldown, skip
                    _ca = w_entry['ca']
                    _any_ca_cooldown = False
                    for _other in watching_entries:
                        if (_other['ca'] == _ca and _other['id'] != w_entry['id']
                                and _other.get('cooldown_until', 0) and _other['cooldown_until'] > time.time()):
                            _remaining = int(_other['cooldown_until'] - time.time())
                            log.info(f"  [WATCHLIST] ⏳ {w_entry['symbol']} WAIT reason=same_ca_cooldown ({_remaining}s remaining from entry#{_other['id']})")
                            _any_ca_cooldown = True
                            break
                    if _any_ca_cooldown:
                        continue

                    # PRICE-GATE: For re-entries, current price must be above last entry price.
                    # Data: 100% of dead cat bounces had price below entry during dip.
                    # Genuine second waves (SOLANA +1030%, Crashout +1140%) never dipped below entry.
                    _entry_count = w_entry.get('entry_count', 0) or 0
                    _last_entry_price = w_entry.get('last_exit_price')  # stores entry_price of last trade
                    if _entry_count > 0 and _last_entry_price and _last_entry_price > 0:
                        _current_price = eval_res.get('current_price', 0) or 0
                        if _current_price > 0 and _current_price <= _last_entry_price:
                            _price_vs_entry = (_current_price - _last_entry_price) / _last_entry_price * 100
                            log.info(
                                f"  [WATCHLIST] 🚫 {w_entry['symbol']} PRICE-GATE BLOCKED: "
                                f"re-entry #{_entry_count+1}, current={_current_price:.10f} "
                                f"<= last_entry={_last_entry_price:.10f} ({_price_vs_entry:+.1f}%)"
                            )
                            continue
                        elif _current_price > _last_entry_price:
                            _price_vs_entry = (_current_price - _last_entry_price) / _last_entry_price * 100
                            log.info(
                                f"  [WATCHLIST] ✅ {w_entry['symbol']} PRICE-GATE PASS: "
                                f"re-entry #{_entry_count+1}, current={_current_price:.10f} "
                                f"> last_entry={_last_entry_price:.10f} ({_price_vs_entry:+.1f}%)"
                            )

                    # Fetch signal description for sub-index parsing (Task 6)
                    # NOTE: premium_signals lives in SENTIMENT_DB (sentiment_arb.db),
                    # not in the paper_trades db connection.
                    _sig_desc = None
                    try:
                        _psid = w_entry.get('premium_signal_id')
                        if _psid:
                            import sqlite3 as _sqlite3
                            _sdb = _sqlite3.connect(SENTIMENT_DB)
                            _sdb.row_factory = _sqlite3.Row
                            _row = _sdb.execute(
                                "SELECT description FROM premium_signals WHERE id = ?", (_psid,)
                            ).fetchone()
                            _sdb.close()
                            if _row:
                                _sig_desc = _row[0] if isinstance(_row, (tuple, list)) else _row['description']
                            if _sig_desc:
                                log.info(f"  [WATCHLIST] 📋 Loaded signal description for {w_entry['symbol']} (psid={_psid}, len={len(_sig_desc)})")
                    except Exception as _e:
                        log.warning(f"  [WATCHLIST] Failed to load signal description for psid={w_entry.get('premium_signal_id')}: {_e}")
                    pending_entries[lifecycle_id] = {
                        'token_ca': w_entry['ca'],
                        'symbol': w_entry['symbol'],
                        'signal_ts': w_entry['signal_ts'],
                        'premium_signal_id': w_entry['premium_signal_id'],
                        'signal_type': w_entry['type'],
                        'pool': w_entry['pool_address'],
                        'staged_at': time.time(),
                        # Fix 2: use momentum's final snapshot price, not matrix eval start price
                        'trigger_price': eval_res.get('momentum_final_price') or eval_res.get('current_price'),
                        'watchlist_id': w_entry['id'],
                        # Task 6+7: Kelly with sub-indices + signal velocity + Matrix crowding
                        'kelly_position_sol': calculate_kelly_position(w_entry, description=_sig_desc, matrix_scores=eval_res.get('scores')),
                        'matrix_scores': eval_res.get('scores'),  # stored for Kelly recalc after SmartEntry
                        # SmartEntry retry tracking (persisted across FIRE→REJECT→re-FIRE)
                        'smart_entry_retries': w_entry.get('_smart_entry_retries', 0),
                        # Pin the watchlist entry to this pending slot so downstream Kelly recalcs
                        # and retry-counter writes don't latch onto whichever w_entry the outer
                        # watching_entries loop happened to land on last.
                        'w_entry': w_entry,
                    }

                    # Note: Kelly always returns >= 0.03 SOL (never vetoes).
                    # Matrix+Momentum decide whether to trade; Kelly only sizes.

                    # P7: Minimum position threshold — skip if Kelly gives floor value
                    # Data: 0.03 SOL trades have terrible risk/reward (avg loss -15%, wins earn 0.002 SOL)
                    _kelly_sol = pending_entries[lifecycle_id]['kelly_position_sol']
                    if _kelly_sol <= 0.05:
                        log.info(f"  [WATCHLIST] ⛔ {w_entry['symbol']} SKIP: Kelly={_kelly_sol:.3f} SOL too small (P7 min=0.05)")
                        pending_entries.pop(lifecycle_id, None)
                        continue

                    log.info(f"  [WATCHLIST] 🚀 FIRE {w_entry['symbol']}! Scores: {eval_res['scores']} Kelly: {_kelly_sol} SOL -> Pending queue")
                    last_progress = time.time()
                else:
                    # Log the 'wait' action so user sees why it's not firing
                    age_min = int((time.time() - w_entry.get('added_at', time.time())) / 60)
                    log.info(
                        f"  [WATCHLIST] ⏳ {w_entry['symbol']} WAIT ({age_min}min) "
                        f"reason={eval_res.get('action_reason', 'unknown')}"
                    )
            except Exception as e:
                log.error(f"Watchlist evaluation error for {w_entry.get('symbol')}: {e}", exc_info=True)
        
        if watching_entries:
            log.info(
                f"  [WATCHLIST_SCAN] Done: evaluated={wl_eval_count} "
                f"skip_cooldown={wl_skip_cooldown} skip_dup={wl_skip_duplicate} "
                f"total_watching={len(watching_entries)}"
            )

        if pending_entries:
            for lifecycle_id, pending in list(pending_entries.items()):
                try:
                    pending['attempts'] = int(pending.get('attempts') or 0) + 1
                    
                    # Phase 1c: Last-mile pre-buy price recheck
                    # If this is the first execution attempt and we have a valid trigger price,
                    # make sure the price hasn't already rocketed past our acceptable entry slippage.
                    # EXCEPTION: ATH + M=100 = verified parabolic move → skip entirely, buy ASAP.
                    trigger_price = pending.get('trigger_price')
                    _scores = pending.get('matrix_scores') or {}
                    _m_score = _scores.get('momentum', 0)
                    _v_score = _scores.get('volume', 0)
                    _t_score = _scores.get('trend', 0)
                    _s_score = _scores.get('signal', 0)
                    # Read the pinned w_entry for this pending slot — NEVER the outer loop variable.
                    pending_w_entry = pending.get('w_entry')
                    _entry_count = pending_w_entry.get('entry_count', 0) if pending_w_entry else 0
                    # Fast lane requires ATH + T=100 + V=100 + S=100 + M=100 + buy_sell≥1.0
                    # P is exempt: ATH tokens naturally sit at price highs → P is always low
                    # ALL other scores must be perfect. Any weakness = go through SmartEntry.
                    # Re-entries NEVER get fast lane — must confirm pullback-bounce first.
                    _ath_dex = fetch_dexscreener_trend_snapshot(pending['token_ca']) if (
                        pending.get('signal_type') == 'ATH' and _m_score and _m_score >= 100
                        and _t_score and _t_score >= 100 and _v_score and _v_score >= 100
                        and _s_score and _s_score >= 100
                    ) else None
                    _ath_bs_ratio = (_ath_dex.get('buys_m5', 0) / max(_ath_dex.get('sells_m5', 1), 1)) if _ath_dex else 0
                    _ath_pc_m5 = _ath_dex.get('price_change_m5', 0) if _ath_dex else 0
                    _is_ath_momentum = (pending.get('signal_type') == 'ATH'
                                       and _t_score and _t_score >= 100
                                       and _v_score and _v_score >= 100
                                       and _s_score and _s_score >= 100
                                       and _m_score and _m_score >= 100
                                       and _ath_bs_ratio >= 1.0
                                       and _ath_pc_m5 > 0  # 5min price must be UP (fixes Abducted: 15s bounce in -9% crash)
                                       and _entry_count == 0)  # re-entries must go through SmartEntry

                    if trigger_price and pending['attempts'] == 1 and not _is_ath_momentum:
                        live_price, _, _ = fetch_realtime_price(pending['token_ca'], pending['pool'])
                        if live_price is not None and live_price > trigger_price * (1 + ENTRY_PREBUY_RECHECK_MAX_PCT / 100):
                            log.info(f"  [ENTRY_TIMING] {pending['symbol']} SKIP: entry_too_late "
                                     f"live={live_price:.10f} > max_allowed={trigger_price * (1 + ENTRY_PREBUY_RECHECK_MAX_PCT / 100):.10f}")
                            pending_entries.pop(lifecycle_id, None)
                            continue
                        # BUG FIX: Also reject if price DROPPED >10% — token likely crashing
                        if live_price is not None and live_price < trigger_price * 0.90:
                            _drop_pct = (live_price - trigger_price) / trigger_price * 100
                            log.info(f"  [ENTRY_TIMING] {pending['symbol']} SKIP: price_collapsed "
                                     f"live={live_price:.10f} dropped {_drop_pct:+.1f}% from trigger={trigger_price:.10f}")
                            pending_entries.pop(lifecycle_id, None)
                            continue
                    elif _is_ath_momentum:
                        # Fast lane: still do a price drop sanity check (漏洞2 fix)
                        # Skip SmartEntry but DON'T buy into a crash
                        _fl_price, _, _ = fetch_realtime_price(pending['token_ca'], pending['pool'])
                        if _fl_price is not None and trigger_price and _fl_price < trigger_price * 0.90:
                            _fl_drop = (_fl_price - trigger_price) / trigger_price * 100
                            log.info(f"  [ENTRY_TIMING] {pending['symbol']} ATH fast-lane ABORT: "
                                     f"price collapsed {_fl_drop:+.1f}% since FIRE "
                                     f"(live={_fl_price:.10f} vs trigger={trigger_price:.10f})")
                            pending_entries.pop(lifecycle_id, None)
                            continue
                        log.info(f"  [ENTRY_TIMING] {pending['symbol']} ATH+T{_t_score}+M100+V{_v_score}+BS{_ath_bs_ratio:.1f}+pc_m5={_ath_pc_m5:+.1f}% → price OK, buy ASAP")
                    elif pending.get('signal_type') == 'ATH' and _m_score and _m_score >= 100 and _v_score and _v_score >= 70:
                        # ATH but missing T≥100 or buy_sell or is re-entry or price_m5≤0 → fall through to SmartEntry
                        _block_reasons = []
                        if _t_score < 100: _block_reasons.append(f"T={_t_score}<100")
                        if _ath_bs_ratio < 1.0: _block_reasons.append(f"bs={_ath_bs_ratio:.2f}<1.0")
                        if _ath_pc_m5 <= 0: _block_reasons.append(f"pc_m5={_ath_pc_m5:+.1f}%<=0")
                        if _entry_count > 0: _block_reasons.append(f"re-entry#{_entry_count+1}")
                        log.info(f"  [ENTRY_TIMING] {pending['symbol']} ATH fast lane BLOCKED: {', '.join(_block_reasons)} → SmartEntry required")

                    # --- Smart Entry Engine (replaces Dip-then-Rip) ---
                    # Layer 1: DexScreener volume-price trend confirmation
                    # Layer 2: Price trajectory pullback-bounce detection
                    # Max wait: 15 minutes. No chase — only pullback-bounce entries.
                    # EXCEPTION: ATH + T≥100 + M=100 + first entry → skip SmartEntry (momentum_direct).
                    if not pending.get('timing_passed'):
                        pending_w_entry = pending.get('w_entry')
                        if _is_ath_momentum:
                            # Verified parabolic first entry — no pullback to wait for, just buy
                            log.info(f"  [SmartEntry] {pending['symbol']} ATH+T100+M100 first entry → SKIP pullback wait, momentum_direct")
                            pending['timing_passed'] = True
                            entry_mode = 'momentum_direct'
                            pending['kelly_position_sol'] = calculate_kelly_position(
                                pending_w_entry, entry_mode=entry_mode,
                                matrix_scores=pending.get('matrix_scores'))
                        else:
                            should_enter, timing_reason, timing_detail, timing_trigger_price = evaluate_smart_entry(
                                pending['token_ca'],
                                symbol=pending['symbol'],
                                pool_address=pending['pool'],
                                entry_count=pending_w_entry.get('entry_count', 0) if pending_w_entry else 0,
                            )
                            if not should_enter:
                                retry_count = pending.get('smart_entry_retries', 0)
                                max_retries = 3
                                if retry_count >= max_retries:
                                    log.info(f"  [SmartEntry] {pending['symbol']} REJECT (final, {retry_count}/{max_retries}): {timing_reason} {timing_detail}")
                                    pending_entries.pop(lifecycle_id, None)
                                else:
                                    # Return to watchlist for re-evaluation on next Matrix PASS
                                    log.info(
                                        f"  [SmartEntry] {pending['symbol']} REJECT → back to watchlist "
                                        f"(retry {retry_count+1}/{max_retries}): {timing_reason} {timing_detail}"
                                    )
                                    # Store retry count, remove from pending so watchlist scanner picks it up again
                                    if pending_w_entry:
                                        pending_w_entry['_smart_entry_retries'] = retry_count + 1
                                    pending_entries.pop(lifecycle_id, None)
                                continue
                            # Smart entry passed — update trigger price to the confirmed entry price
                            pending['timing_passed'] = True
                            if timing_trigger_price:
                                pending['trigger_price'] = timing_trigger_price
                            # Determine entry mode from SmartEntry result
                            entry_mode = 'momentum_direct' if 'momentum' in (timing_reason or '').lower() else 'pullback_bounce'
                            # Recalculate Kelly with entry mode + matrix scores
                            pending['kelly_position_sol'] = calculate_kelly_position(
                                pending_w_entry, entry_mode=entry_mode,
                                matrix_scores=pending.get('matrix_scores'))
                            log.info(f"  [SmartEntry] {pending['symbol']} PASS: {timing_reason} trigger={timing_trigger_price}")
                            
                    # Kelly position size: apply three-layer cap
                    # Layer 1: Kelly formula output
                    # Layer 2: MAXposition 0.5 SOL hard cap
                    # Layer 3: A1 - max 1% of pool liquidity (prevents slippage in thin pools)
                    _kelly_raw = pending.get('kelly_position_sol') or position_size_sol
                    _liq_cap = get_liquidity_position_cap(
                        pending['token_ca'],
                        sol_price_usd=sol_price,
                    )
                    actual_position_size_sol = min(
                        _kelly_raw,
                        0.5,  # hard cap
                        _liq_cap if _liq_cap is not None else 0.5,
                    )
                    if _liq_cap is not None and actual_position_size_sol < _kelly_raw:
                        log.info(f"  [ENTRY_SIZE] {pending['symbol']} kelly={_kelly_raw:.3f} → liq_cap={actual_position_size_sol:.3f} SOL (pool liquidity limit)")
                    execution = simulate_entry_execution(
                        pending['token_ca'],
                        actual_position_size_sol,
                        'stage1',
                        strategy_id=strategy_id,
                        lifecycle_id=lifecycle_id,
                    )
                    if not execution.get('success'):
                        failure_reason = execution.get('failureReason') or 'entry_quote_failed'
                        log_pending_entry_issue(
                            pending,
                            f"entry quote failed reason={failure_reason} route={execution.get('routeAvailable')}",
                            level='warning'
                        )
                        staged_age_sec = int(max(0, time.time() - (pending.get('staged_at') or time.time())))
                        if failure_reason in {'rate_limited_429', 'quote_failed', 'no_route', 'missing_taker', 'unknown'} \
                                and pending['attempts'] < paper_execution['quoteRetries'] \
                                and staged_age_sec < paper_execution['maxQuoteAgeSec']:
                            continue
                        pending_entries.pop(lifecycle_id, None)
                        continue

                    quote_price_sol = execution.get('effectivePrice')
                    token_amount_raw = execution.get('quotedOutAmountRaw')
                    token_decimals = execution.get('outputDecimals')
                    if quote_price_sol is None or quote_price_sol <= 0 or not token_amount_raw:
                        log_pending_entry_issue(
                            pending,
                            f"invalid entry execution price={quote_price_sol} out={token_amount_raw}",
                            level='warning',
                            force=True
                        )
                        pending_entries.pop(lifecycle_id, None)
                        continue

                    entry_ts = int((execution.get('quoteTs') or time.time() * 1000) / 1000)
                    if sol_price is None:
                        sol_price = get_sol_price()
                        time.sleep(0.2)
                    # SOL pricing: quote_price_sol is already SOL/token from Jupiter
                    # No USD conversion needed — all monitoring now uses SOL
                    quote_price = quote_price_sol

                    # CRITICAL FIX: Use Jupiter actual fill price as entry_price baseline.
                    # Previously used trigger_price (Matrix eval snapshot) which could be
                    # 15-30s stale for fast-lane ATH entries. When price dropped between
                    # FIRE and execution, entry_price was artificially HIGH → Guardian saw
                    # phantom -11% PnL on a position that was actually -3% → false hard_sl.
                    #
                    # quote_price_sol = what Jupiter actually priced the swap at = true cost basis.
                    # trigger_price is preserved in the DB 'trigger_price' column for analysis.
                    price = quote_price
                    trigger_price_val = pending.get('trigger_price')
                    _spread = ((quote_price - trigger_price_val) / trigger_price_val * 100) if trigger_price_val and trigger_price_val > 0 else 0
                    _trigger_str = f"{trigger_price_val:.12f}" if trigger_price_val else "N/A"
                    log.info(
                        f"  [ENTRY_PRICE] {pending['symbol']} entry_price={price:.12f} "
                        f"(quote_fill, trigger_was={_trigger_str} "
                        f"spread={_spread:+.1f}%)"
                    )
                    regime = determine_market_regime(sol_price) if sol_price else 'unknown'
                    db.execute("""
                        INSERT INTO paper_trades
                            (strategy_id, strategy_role, strategy_stage, stage_outcome,
                             token_ca, symbol, signal_ts, entry_price, entry_ts,
                             market_regime, replay_source, peak_pnl, trailing_active,
                             lifecycle_id, stage_seq, trigger_ts, trigger_price,
                             position_size_sol, token_amount_raw, token_decimals,
                             entry_execution_json, entry_execution_audit_json, monitor_state_json,
                             premium_signal_id, signal_type, strategy_outcome, execution_availability, accounting_outcome, synthetic_close)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'live_monitor', 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """, (
                        strategy_id, strategy_role, 'stage1', 'stage1_entered',
                        pending['token_ca'], pending['symbol'], pending['signal_ts'], price, entry_ts,
                        regime, lifecycle_id, stage_seq('stage1'), entry_ts, price,
                        actual_position_size_sol, str(token_amount_raw), token_decimals or 0, json.dumps(execution), json.dumps(build_execution_audit(execution, {
                            'auditVersion': 1,
                            'stage': 'stage1',
                            'lifecycleId': lifecycle_id,
                            'entryPriceUsd': price,
                            'positionSizeSol': actual_position_size_sol,
                        })), json.dumps({
                            'tokenCA': pending['token_ca'],
                            'symbol': pending['symbol'],
                            'entryPrice': price,
                            'entrySol': actual_position_size_sol,
                            'tokenAmount': int(token_amount_raw),
                            'tokenDecimals': int(token_decimals or 0),
                            'entryTime': int(entry_ts) * 1000,
                            'exitStrategy': 'NOT_ATH',
                        }),
                        pending.get('premium_signal_id'), pending.get('signal_type') or 'NEW_TRENDING', 'entered', 'available', 'open'
                    ))
                    trade_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
                    db.commit()

                    # Pop immediately after commit — prevents ghost duplicate on next loop
                    pending_entries.pop(lifecycle_id, None)

                    pos = Position(trade_id, pending['token_ca'], pending['symbol'], pending['signal_ts'], price, entry_ts, pending['pool'],
                                   'stage1', lifecycle_id, stage1_exit, actual_position_size_sol, token_amount_raw, token_decimals or 0,
                                   monitor_state={
                                       'tokenCA': pending['token_ca'],
                                       'symbol': pending['symbol'],
                                       'entryPrice': price,
                                       'entrySol': actual_position_size_sol,
                                       'tokenAmount': int(token_amount_raw),
                                       'tokenDecimals': int(token_decimals or 0),
                                       'entryTime': int(entry_ts) * 1000,
                                       'exitStrategy': 'NOT_ATH',
                                   })
                    with positions_lock:
                        positions[pos.trade_id] = pos

                    # Use setdefault to avoid KeyError if lifecycle not yet initialized
                    lc = lifecycles.setdefault(lifecycle_id,
                        build_lifecycle_state(lifecycle_id, pending['token_ca'],
                            pending['symbol'], pending['signal_ts'],
                            pending.get('premium_signal_id'),
                            pending.get('signal_type')))
                    lc['stage1_trade_id'] = trade_id

                    log.info(
                        f"  Entered {pending['symbol']}/stage1 @ {price:.10f} "
                        f"(quote_sol={quote_price_sol:.12f}, decimals={token_decimals or 0}) "
                        f"lifecycle={lifecycle_id} via quoted execution"
                    )
                    if 'watchlist_id' in pending:
                        watchlist.mark_holding(
                            pending['watchlist_id'], 
                            price, 
                            actual_position_size_sol, 
                            token_amount_raw, 
                            token_decimals or 0, 
                            trade_id,
                            initial_sl=get_adaptive_stop_loss(),  # A2: volatility-adjusted SL
                        )
                    last_progress = time.time()
                    time.sleep(0.2)
                except Exception as e:
                    # Safety net: always pop to prevent infinite retry loop
                    pending_entries.pop(lifecycle_id, None)
                    log.error(f"  Pending entry error for {pending.get('symbol', lifecycle_id)}: {e}")

        if now - last_position_check >= POSITION_POLL_INTERVAL:
            last_position_check = now

            # --- P6: Process Guardian exit signals first (highest priority) ---
            to_close = process_guardian_exits(
                exit_guardian, positions, lifecycles,
                strategy_id, build_lifecycle_state, simulate_exit_execution
            )
            try:
                new_sol = get_sol_price()
                if new_sol:
                    sol_price = new_sol
                time.sleep(0.2)
            except Exception as e:
                log.debug(f"SOL price update failed: {e}")

            with positions_lock:
                all_positions = list(positions.items())
            if len(all_positions) > MAX_EVALS_PER_CYCLE:
                start = _eval_rotation_offset % len(all_positions)
                eval_batch = all_positions[start:start + MAX_EVALS_PER_CYCLE]
                if len(eval_batch) < MAX_EVALS_PER_CYCLE:
                    eval_batch += all_positions[:MAX_EVALS_PER_CYCLE - len(eval_batch)]
                _eval_rotation_offset = (start + MAX_EVALS_PER_CYCLE) % len(all_positions)
            else:
                eval_batch = all_positions

            for trade_id, pos in eval_batch:
                try:
                    # Pre-fetch real-time price (Redis live / Jupiter quote /
                    # DexScreener fallback) with relaxed 15s freshness. Using
                    # DexScreener directly here caused ~30% divergence from
                    # real market state (see LEELOO trigger_pnl=-2.9% vs
                    # quote_pnl=+28.4%), producing false trail/stop triggers.
                    pos_pool = get_pool_address(pos.token_ca)
                    pre_price, pre_src, pre_age_ms = fetch_realtime_price(
                        pos.token_ca, pos_pool, max_age_ms=15000,
                        token_decimals=getattr(pos, 'token_decimals', None),
                    )
                    smart_exit_triggered = False
                    smart_exit_reason = None

                    if pre_price and pre_price > 0:
                        pre_ts = int(time.time() - (pre_age_ms or 0) / 1000)
                        log.info(
                            f"  [PRE_PRICE] {pos.symbol}: {pre_price:.10f} "
                            f"src={pre_src} age_ms={pre_age_ms}"
                        )
                        
                        # --- POST-ENTRY EXIT MATRIX ENGINE ---
                        w_entry = watchlist.get_by_ca(pos.token_ca)

                        # B+C: Poll Helius for real TPS (30s interval, cached)
                        if w_entry:
                            pos_pool = pos.pool_address or get_pool_address(pos.token_ca)
                            helius_tps = poll_helius_volume(pos.trade_id, pos_pool)
                            w_entry['_helius_tps'] = helius_tps
                            # Inject in-memory state that must persist across cycles
                            # (w_entry is re-fetched from DB each cycle, losing _ prefixed fields)
                            w_entry['_trail_factor'] = pos.trail_factor
                            # Guardian writes velocity to its own w_entry copy → relay via Position
                            if hasattr(pos, '_guardian_velocity'):
                                w_entry['_guardian_velocity'] = pos._guardian_velocity
                            # A3: relay peak_ts for time-decay trail calculation
                            if hasattr(pos, 'peak_ts'):
                                w_entry['_peak_ts'] = pos.peak_ts

                        if not w_entry:
                            exit_matrix = {'action': 'hold', 'reason': 'no_watchlist_entry'}
                        elif w_entry['status'] == 'moon_bag':
                            exit_matrix = exit_matrix_evaluator.evaluate_moon_bag(w_entry, pre_price)
                        else:
                            exit_matrix = exit_matrix_evaluator.evaluate_exit(w_entry, pre_price)
                        
                        # Log every exit evaluation
                        held_min = int((time.time() - pos.entry_ts) / 60)
                        pnl_pct = exit_matrix.get('current_pnl', 0) * 100
                        log.info(
                            f"  [EXIT_MATRIX] {pos.symbol}/{pos.strategy_stage} "
                            f"action={exit_matrix['action']} pnl={pnl_pct:+.1f}% "
                            f"held={held_min}min reason={exit_matrix.get('reason', '-')} "
                            f"price={pre_price:.10f} trail={exit_matrix.get('trail_floor', '-')}"
                        )
                            
                        if exit_matrix.get('action') == 'tighten_sl':
                            if exit_matrix.get('new_sl'):
                                watchlist.update_position_state(w_entry['id'], dynamic_sl=exit_matrix['new_sl'])
                                log.info(f"  [EXIT_MATRIX] {pos.symbol} SL tightened to {exit_matrix['new_sl']:.1%}")
                            exit_matrix['action'] = 'hold'
                        
                        # === BUG FIX: Persist all dynamic state back to watchlist DB ===
                        # Without this, peak_pnl stays 0 forever and Trail/Lock never fires
                        state_updates = {'last_matrix_check': time.time()}
                        
                        # Persist peak_pnl (critical for trail stop + lock profit)
                        if 'peak_pnl' in exit_matrix and exit_matrix.get('current_pnl') is not None:
                            new_peak = max(w_entry.get('peak_pnl', 0) or 0, exit_matrix['current_pnl'])
                            state_updates['peak_pnl'] = new_peak
                        elif exit_matrix.get('current_pnl') is not None:
                            new_peak = max(w_entry.get('peak_pnl', 0) or 0, exit_matrix['current_pnl'])
                            state_updates['peak_pnl'] = new_peak
                        
                        # Persist moon_peak_pnl (critical for moon trail)
                        if exit_matrix.get('moon_peak_pnl') is not None:
                            state_updates['moon_peak_pnl'] = exit_matrix['moon_peak_pnl']
                        
                        # Persist zero_vol_count (critical for volume-based SL tightening)
                        if exit_matrix.get('new_zero_vol_count') is not None:
                            state_updates['zero_vol_count'] = exit_matrix['new_zero_vol_count']
                        
                        # Persist moon_trend_zero_count (critical for moon trend death)
                        if exit_matrix.get('new_moon_trend_zero_count') is not None:
                            state_updates['moon_trend_zero_count'] = exit_matrix['new_moon_trend_zero_count']
                        
                        # P4: Persist moon_trail_factor (critical for velocity ratchet)
                        if exit_matrix.get('moon_trail_factor') is not None:
                            state_updates['moon_trail_factor'] = exit_matrix['moon_trail_factor']
                        
                        if w_entry:
                            watchlist.update_position_state(w_entry['id'], **state_updates)

                        # Read back in-memory state from ExitMatrix → Position (persist across cycles)
                        if exit_matrix.get('_trail_factor') is not None:
                            pos.trail_factor = exit_matrix['_trail_factor']

                        exit_eval = {
                            'ok': True,
                            'currentPrice': pre_price,
                            'quoteTsSec': pre_ts,
                            'realizedPnl': exit_matrix.get('current_pnl', 0.0),
                            'triggerPnl': exit_matrix.get('current_pnl', 0.0),
                            'shouldExit': False,
                            'action': 'hold',
                            'execution': {'success': True, 'effectivePrice': pre_price},
                            'markSource': 'matrix_engine'
                        }
                        
                        if exit_matrix['action'] in ('exit', 'lock_profit'):
                            sell_pct = 0.5 if exit_matrix['action'] == 'lock_profit' else 1.0
                            sell_amount_raw = int(float(pos.token_amount_raw) * sell_pct) if pos.token_amount_raw else 0
                            
                            simulate_res = simulate_exit_execution(
                                pos.token_ca,
                                str(sell_amount_raw),
                                getattr(pos, 'token_decimals', 0) or 0,
                                pos.strategy_stage,
                                strategy_id=strategy_id,
                                lifecycle_id=pos.lifecycle_id
                            )
                            
                            if simulate_res.get('success'):
                                # --- Price Sanity Gate ---
                                # shared-quote-runtime can diverge heavily from real execution
                                # prices for low-liquidity tokens (e.g. trigger=-15% but quote=-4%).
                                # Before committing to an exit, verify the trigger PNL against
                                # the actual DEX quote price.
                                sanity_override = False
                                if exit_matrix['action'] == 'exit':
                                    quote_eff_sol = simulate_res.get('effectivePrice')
                                    if (quote_eff_sol and quote_eff_sol > 0 and
                                            pos.entry_price and pos.entry_price > 0):
                                        # SOL pricing: entry_price is SOL, quote_eff_sol is SOL — compare directly
                                        quote_pnl = (quote_eff_sol - pos.entry_price) / pos.entry_price
                                        trigger_pnl = exit_matrix.get('current_pnl', 0.0)
                                        divergence = abs(quote_pnl - trigger_pnl)

                                        if divergence > 0.05:  # >5% price source disagreement
                                            reason = exit_matrix.get('reason', '')
                                            cancel = False

                                            if 'hard_sl' in reason:
                                                sl_threshold = w_entry.get('dynamic_sl', -0.075) if w_entry else -0.075
                                                if quote_pnl > sl_threshold:
                                                    cancel = True
                                            elif 'trail_stop' in reason:
                                                trail_floor = exit_matrix.get('trail_floor')
                                                if trail_floor is not None and quote_pnl >= trail_floor:
                                                    cancel = True

                                            if cancel:
                                                log.warning(
                                                    f"  [PRICE_SANITY] {pos.symbol} EXIT CANCELLED — "
                                                    f"trigger_pnl={trigger_pnl:+.1%} but quote_pnl={quote_pnl:+.1%} "
                                                    f"(divergence={divergence:.1%}, src={pre_src}). "
                                                    f"Trigger price was unreliable, holding position."
                                                )
                                                sanity_override = True
                                            else:
                                                log.info(
                                                    f"  [PRICE_SANITY] {pos.symbol} price divergence noted: "
                                                    f"trigger={trigger_pnl:+.1%} quote={quote_pnl:+.1%} "
                                                    f"(gap={divergence:.1%}) — exit confirmed by quote"
                                                )

                                if not sanity_override:
                                    exit_eval['shouldExit'] = True
                                    exit_eval['action'] = 'partial_sell' if exit_matrix['action'] == 'lock_profit' else 'exit'
                                    exit_eval['exitReason'] = exit_matrix.get('reason', 'matrix_exit')
                                    exit_eval['execution'] = simulate_res
                                    exit_eval['tpName'] = 'MOON_LOCK' if exit_matrix['action'] == 'lock_profit' else None
                                    
                                    if exit_matrix['action'] == 'lock_profit':
                                        log.info(f"  [EXIT_MATRIX] 🌙 {pos.symbol} LOCK PROFIT! Selling 50%, rest → Moon Bag")
                                    else:
                                        log.info(f"  [EXIT_MATRIX] 🔔 {pos.symbol} EXIT triggered: {exit_matrix.get('reason')}")
                                    
                                    if exit_matrix['action'] == 'lock_profit' and w_entry:
                                        watchlist.mark_moon_bag(w_entry['id'], exit_matrix.get('current_pnl', 0.0))
                                        rem_amount = int(float(pos.token_amount_raw) - sell_amount_raw)
                                        exit_eval['updatedState'] = (pos.monitor_state or {}).copy()
                                        exit_eval['updatedState']['tokenAmount'] = rem_amount
                            else:
                                exit_eval['ok'] = False
                                exit_eval['failureReason'] = simulate_res.get('failureReason', 'quote_failed')
                    else:
                        pre_ts = None
                        exit_eval = {'ok': False, 'failureReason': 'no_price'}

                    if not exit_eval.get('ok'):
                        failure_info = exit_eval.get('failureReason') or exit_eval.get('action') or 'unknown'
                        held_min = int(max(0, (time.time() - pos.entry_ts) / 60))
                        log.warning(
                            f"  [EXIT_EVAL_FAIL] {pos.symbol}/{pos.strategy_stage}: {failure_info} "
                            f"held={held_min}min success={exit_eval.get('success')} "
                            f"markSource={exit_eval.get('markSource')}"
                        )
                        # Force timeout exit if position held far beyond timeout limit
                        timeout_min = int(pos.exit_rules.get('timeoutMinutes', 120))
                        if held_min >= timeout_min * 2:
                            log.warning(
                                f"  [FORCE_TIMEOUT] {pos.symbol}/{pos.strategy_stage}: held {held_min}min "
                                f"(2x timeout={timeout_min}min), force closing as timeout"
                            )
                            to_close.append({
                                'trade_id': trade_id,
                                'reason': 'timeout',
                                'pnl': 0.0,
                                'trigger_pnl': 0.0,
                                'exit_price': pos.entry_price,
                                'exit_ts': int(time.time()),
                                'mark_source': 'force_timeout',
                                'exit_eval': {
                                    'action': 'exit',
                                    'shouldExit': True,
                                    'exitReason': 'timeout',
                                    'execution': {'success': False, 'failureReason': 'force_timeout_no_quote'},
                                    'updatedState': pos.monitor_state,
                                },
                            })
                        continue
                    mark_execution = exit_eval.get('execution') or {}
                    mark_quote_reason = exit_eval.get('quoteFailureReason')
                    mark_quote_route = exit_eval.get('routeAvailable')
                    mark_quote_price = mark_execution.get('effectivePrice')
                    mark_quote_out = exit_eval.get('quotedOutAmount')
                    price = exit_eval.get('currentPrice')
                    bar_ts = int(exit_eval.get('quoteTsSec') or 0)
                    mark_source = exit_eval.get('markSource') or 'fallback'
                    if price is None or price <= 0 or not bar_ts:
                        continue
                    action = exit_eval.get('action') or 'hold'
                    should_exit = bool(exit_eval.get('shouldExit'))
                    pos.monitor_state = exit_eval.get('updatedState') or pos.monitor_state
                    sync_position_from_monitor_state(pos, allow_token_amount_override=(action == 'partial_sell'))
                    reason = exit_eval.get('exitReason') or exit_eval.get('lifecycleReason')
                    
                    # Add 10-second protection against any SL drop right after entry
                    if (should_exit or action in ('partial_sell', 'exit')) and reason and ('sl' in reason or 'stop_loss' in reason or 'hard_sl' in reason):
                        if time.time() - pos.entry_ts <= 10:
                            log.info(f"  [PROTECTION] {pos.symbol} ignoring SL trigger within first 10s of entry (reason={reason})")
                            should_exit = False
                            action = 'hold'
                            reason = 'hold'

                    pnl = float(exit_eval.get('realizedPnl') if exit_eval.get('realizedPnl') is not None else (exit_eval.get('triggerPnl') or 0.0))
                    trigger_pnl = float(exit_eval.get('triggerPnl') or 0.0)
                    lifecycle = lifecycles.setdefault(pos.lifecycle_id, build_lifecycle_state(pos.lifecycle_id, pos.token_ca, pos.symbol, pos.signal_ts, getattr(pos, 'premium_signal_id', None), getattr(pos, 'signal_type', None)))
                    lifecycle['first_peak_pct'] = max(lifecycle.get('first_peak_pct') or 0.0, pos.peak_pnl * 100.0)
                    db.execute("""
                        UPDATE paper_trades
                        SET peak_pnl = ?, trailing_active = ?, bars_held = ?, stage_outcome = ?, first_peak_pct = ?, monitor_state_json = ?
                        WHERE id = ?
                    """, (pos.peak_pnl, int(pos.trailing_active), pos.bars_held, f"{pos.strategy_stage}_open", lifecycle['first_peak_pct'], json.dumps(pos.monitor_state), pos.trade_id))
                    db.commit()
                    if action in ('partial_sell', 'exit') or should_exit:
                        trigger_price_text = f"{price:.10f}" if price is not None else 'na'
                        quote_price_text = 'na'
                        if mark_quote_price is not None and mark_quote_price > 0:
                            quote_price_value = mark_quote_price  # SOL pricing: already SOL
                            quote_price_text = f"{quote_price_value:.10f}"
                        quote_out_text = f"{float(mark_quote_out):.6f}" if mark_quote_out is not None else 'na'
                        debug_fields = compute_exit_debug_fields(pos.exit_rules, pos, trigger_pnl)
                        trail_floor_text = (
                            f"{debug_fields['trail_floor_pct']:+.1f}%"
                            if debug_fields['trail_floor_pct'] is not None else 'na'
                        )
                        log.info(
                            f"  Exit trigger {pos.symbol}/{pos.strategy_stage}: action={action} reason={reason} "
                            f"trigger_pnl={trigger_pnl*100:+.1f}% trigger_price={trigger_price_text} "
                            f"source={mark_source} quote_route={mark_quote_route} "
                            f"quote_reason={mark_quote_reason or '-'} quote_price={quote_price_text} "
                            f"quote_out={quote_out_text} trail_active={str(debug_fields['trail_active']).lower()} "
                            f"trail_floor_pct={trail_floor_text} stop_loss_pct={debug_fields['stop_loss_pct']:+.1f}%"
                        )
                        to_close.append({
                            'trade_id': trade_id,
                            'reason': reason,
                            'pnl': pnl,
                            'trigger_pnl': trigger_pnl,
                            'exit_price': price,
                            'exit_ts': bar_ts,
                            'mark_source': mark_source,
                            'exit_eval': exit_eval,
                        })
                    time.sleep(0.05)
                except Exception as e:
                    log.error(f"  Position update error for {pos.symbol}: {e}")

            for close_event in to_close:
              try:
                trade_id = close_event['trade_id']
                reason = close_event['reason']
                pnl = close_event['pnl']
                trigger_pnl = close_event.get('trigger_pnl', pnl)
                exit_price = close_event['exit_price']
                exit_ts = close_event['exit_ts']
                mark_source = close_event['mark_source']
                exit_eval = close_event.get('exit_eval') or {}
                pos = positions.get(trade_id)
                if pos is None:
                    continue
                if exit_eval.get('action') == 'partial_sell':
                    pos.monitor_state = exit_eval.get('updatedState') or pos.monitor_state
                    sync_position_from_monitor_state(pos, allow_token_amount_override=True)
                    db.execute(
                        """
                        UPDATE paper_trades
                        SET peak_pnl = ?, trailing_active = ?, bars_held = ?, stage_outcome = ?,
                            token_amount_raw = ?, exit_execution_json = ?, exit_execution_audit_json = ?, monitor_state_json = ?
                        WHERE id = ?
                        """,
                        (
                            pos.peak_pnl,
                            int(pos.trailing_active),
                            pos.bars_held,
                            f"{pos.strategy_stage}_partial_{(exit_eval.get('tpName') or 'TP').lower()}",
                            str(pos.token_amount_raw),
                            json.dumps(exit_eval.get('execution')),
                            json.dumps(build_execution_audit(exit_eval.get('execution'), {
                                'auditVersion': 1,
                                'stage': pos.strategy_stage,
                                'lifecycleId': pos.lifecycle_id,
                                'decisionType': exit_eval.get('action'),
                                'tpName': exit_eval.get('tpName'),
                                'triggerPnlPct': _safe_float(trigger_pnl * 100.0, None),
                                'markSource': mark_source,
                            })),
                            json.dumps(pos.monitor_state),
                            pos.trade_id,
                        )
                    )
                    db.commit()
                    log.info(
                        f"  PARTIAL {pos.symbol}/{pos.strategy_stage}: {exit_eval.get('tpName')} "
                        f"trigger_pnl={trigger_pnl*100:+.1f}% remaining_raw={pos.token_amount_raw} lifecycle={pos.lifecycle_id}"
                    )
                    last_progress = time.time()
                    continue

                exit_execution = exit_eval.get('execution') or {}
                is_force_timeout = (mark_source == 'force_timeout')
                if not exit_execution.get('success') and not is_force_timeout:
                    failure_reason = exit_execution.get('failureReason') or 'exit_quote_failed'
                    pos.last_exit_quote_failure = failure_reason
                    trap_failure_reason = failure_reason if failure_reason in {'no_route', 'token_not_tradable'} else None
                    if trap_failure_reason:
                        pos.exit_quote_failures += 1
                    else:
                        pos.exit_quote_failures = 0
                    db.execute(
                        "UPDATE paper_trades SET exit_execution_json = ?, exit_execution_audit_json = ?, exit_quote_failures = ?, last_exit_quote_failure = ?, strategy_outcome = ?, execution_availability = ?, accounting_outcome = ?, synthetic_close = 0 WHERE id = ?",
                        (
                            json.dumps(exit_execution),
                            json.dumps(build_execution_audit(exit_execution, {
                                'auditVersion': 1,
                                'stage': pos.strategy_stage,
                                'lifecycleId': pos.lifecycle_id,
                                'decisionType': exit_eval.get('action'),
                                'markSource': mark_source,
                                'triggerPnlPct': _safe_float(trigger_pnl * 100.0, None),
                            })),
                            pos.exit_quote_failures,
                            pos.last_exit_quote_failure,
                            'blocked_by_infra',
                            'unavailable',
                            'open',
                            pos.trade_id,
                        )
                    )
                    db.commit()
                    if trap_failure_reason:
                        held_minutes = max(0, int((time.time() - pos.entry_ts) / 60))
                        threshold_key = 'noRouteFailureThreshold' if trap_failure_reason == 'no_route' else 'tokenNotTradableFailureThreshold'
                        trap_minutes_key = 'noRouteTrapMinutes' if trap_failure_reason == 'no_route' else 'tokenNotTradableTrapMinutes'
                        trap_reason = 'trapped_no_route' if trap_failure_reason == 'no_route' else 'trapped_token_not_tradable'
                        failure_count_field = 'noRouteFailureCount' if trap_failure_reason == 'no_route' else 'tokenNotTradableFailureCount'
                        if pos.exit_quote_failures >= paper_execution[threshold_key] or held_minutes >= paper_execution[trap_minutes_key]:
                            with positions_lock:
                                positions.pop(trade_id, None)
                            close_position_as_trapped_no_route(
                                db,
                                pos,
                                lifecycle,
                                reason=trap_reason,
                                pnl_pct=TRAPPED_NO_ROUTE_PNL_PCT,
                                failure_count_field=failure_count_field,
                            )
                            last_progress = time.time()
                            continue
                    log.warning(f"  Exit quote failed for {pos.symbol}/{pos.strategy_stage}: {failure_reason}")
                    continue

                pos.exit_quote_failures = 0
                pos.last_exit_quote_failure = None

                with positions_lock:
                    positions.pop(trade_id, None)
                lifecycle = lifecycles.setdefault(pos.lifecycle_id, build_lifecycle_state(pos.lifecycle_id, pos.token_ca, pos.symbol, pos.signal_ts, getattr(pos, 'premium_signal_id', None), getattr(pos, 'signal_type', None)))
                regime = determine_market_regime(sol_price) if sol_price else 'unknown'
                stage_outcome = f"{pos.strategy_stage}_{reason}"
                quoted_exit_price = exit_execution.get('effectivePrice')
                # SOL pricing: quoted_exit_price is already SOL/token from Jupiter
                effective_exit_price = quoted_exit_price if quoted_exit_price is not None else exit_price
                realized_pnl = pnl
                actual_out = exit_execution.get('quotedOutAmount')
                has_partial_history = not has_partial_state_gap(
                    pos.token_amount_raw,
                    parse_entry_execution(pos.entry_execution_json),
                    pos.monitor_state,
                )
                lifecycle_realized_pnl, total_realized_sol, lifecycle_entry_sol, accounting_source = lifecycle_realized_pnl_from_state(
                    pos.monitor_state,
                    fallback_position_size_sol=pos.position_size_sol,
                    final_exit_sol=actual_out,
                    has_partial_history=has_partial_history,
                )
                if lifecycle_realized_pnl is not None:
                    realized_pnl = lifecycle_realized_pnl
                elif actual_out is not None and pos.position_size_sol:
                    realized_pnl = (float(actual_out) - float(pos.position_size_sol)) / float(pos.position_size_sol)
                    accounting_source = 'final_exit_only'

                # ─── PnL Sanity Guard ────────────────────────────────────
                # quotedOutAmount from Jupiter can be wildly wrong for
                # low-liquidity tokens (e.g. returning token amount instead
                # of SOL amount). If accounting PnL diverges > 50pp from
                # the price-based trigger PnL, fall back to trigger PnL
                # which is derived from real market prices.
                if trigger_pnl is not None and realized_pnl is not None:
                    divergence = abs(realized_pnl - trigger_pnl)
                    if divergence > 0.50:  # >50 percentage points
                        log.warning(
                            f"  [PNL_SANITY] {pos.symbol} accounting PnL={realized_pnl*100:+.1f}% "
                            f"diverges from trigger PnL={trigger_pnl*100:+.1f}% "
                            f"(gap={divergence*100:.0f}pp). Using trigger PnL."
                        )
                        realized_pnl = trigger_pnl
                        accounting_source = f'trigger_pnl_override(was={accounting_source})'

                log.info(
                    f"ACCOUNTING_SOURCE trade_id={pos.trade_id} lifecycle={pos.lifecycle_id} stage={pos.strategy_stage} "
                    f"source={accounting_source} partialHistory={1 if has_partial_history else 0} "
                    f"entrySol={_safe_float(lifecycle_entry_sol, None)} finalExitSol={_safe_float(actual_out, None)} "
                    f"totalRealizedSol={_safe_float(total_realized_sol, None)} realizedPnlPct={_safe_float(realized_pnl * 100.0, None)}"
                )
                db.execute("""
                    UPDATE paper_trades
                    SET exit_price = ?, exit_ts = ?, exit_reason = ?,
                        pnl_pct = ?, bars_held = ?, market_regime = ?,
                        peak_pnl = ?, trailing_active = ?, stage_outcome = ?, first_peak_pct = ?,
                        exit_execution_json = ?, exit_execution_audit_json = ?, exit_quote_failures = 0, last_exit_quote_failure = NULL,
                        strategy_outcome = ?, execution_availability = ?, accounting_outcome = ?, synthetic_close = 0
                    WHERE id = ?
                """, (
                    effective_exit_price, exit_ts, reason, realized_pnl, pos.bars_held,
                    regime, pos.peak_pnl, int(pos.trailing_active), stage_outcome, lifecycle.get('first_peak_pct') or 0.0,
                    json.dumps(exit_execution), json.dumps(build_execution_audit(exit_execution, {
                        'auditVersion': 1,
                        'stage': pos.strategy_stage,
                        'lifecycleId': pos.lifecycle_id,
                        'decisionType': exit_eval.get('action'),
                        'actionReason': exit_eval.get('actionReason'),
                        'markSource': mark_source,
                        'triggerPnlPct': _safe_float(trigger_pnl * 100.0, None),
                        'realizedPnlPct': _safe_float(realized_pnl * 100.0, None),
                        'totalRealizedSol': _safe_float(total_realized_sol, None),
                        'lifecycleEntrySol': _safe_float(lifecycle_entry_sol, None),
                        'accountingSource': accounting_source,
                        'preExitTotalSolReceived': _safe_float(exit_eval.get('preExitTotalSolReceived', exit_execution.get('preExitTotalSolReceived')), None),
                        'exitSolReceived': _safe_float(exit_eval.get('exitSolReceived', exit_execution.get('exitSolReceived')), None),
                        'postExitTotalSolReceived': _safe_float(exit_eval.get('postExitTotalSolReceived', exit_execution.get('postExitTotalSolReceived')), None),
                        'triggerPrice': _safe_float(exit_price, None),
                        'effectiveExitPrice': _safe_float(effective_exit_price, None),
                    })),
                    'force_timeout' if is_force_timeout else reason,
                    'unavailable' if is_force_timeout else 'available',
                    'closed_force_timeout' if is_force_timeout else 'closed_real',
                    pos.trade_id,
                ))
                db.commit()
                
                # Update Watchlist Status
                if w_entry:
                    # No artificial cooldown — price-gate (current_price > last_entry_price)
                    # handles re-entry filtering. See mark_watching() docstring.
                    watchlist.mark_watching(w_entry['id'], realized_pnl)
                last_progress = time.time()
                trigger_price_text = f"{exit_price:.10f}" if exit_price is not None else 'na'
                quoted_price_text = f"{effective_exit_price:.10f}" if effective_exit_price is not None else 'na'
                quote_out_text = f"{float(actual_out):.6f}" if actual_out is not None else 'na'
                total_realized_text = f"{float(total_realized_sol):.6f}" if total_realized_sol is not None else 'na'
                log.info(
                    f"  CLOSED {pos.symbol}/{pos.strategy_stage}: {reason} pnl={realized_pnl*100:+.1f}% "
                    f"trigger_pnl={trigger_pnl*100:+.1f}% peak={pos.peak_pnl*100:+.1f}% bars={pos.bars_held} "
                    f"trigger_price={trigger_price_text} quoted_price={quoted_price_text} "
                    f"quote_out={quote_out_text} total_realized_sol={total_realized_text} source={mark_source} lifecycle={pos.lifecycle_id}"
                )
              except Exception as e:
                log.error(f"  Close event error for trade_id={close_event.get('trade_id')}: {e}", exc_info=True)


            if positions:
                log.info(f"  Open positions: {len(positions)}  [{', '.join(f'{p.symbol}/{p.strategy_stage}' for p in positions.values())}]")

        today_str = now_utc.strftime('%Y-%m-%d')
        if now_utc.hour == DAILY_REPORT_HOUR and last_daily_report != today_str:
            yesterday = (now_utc - timedelta(days=1)).strftime('%Y-%m-%d')
            print_daily_report(db, yesterday)
            last_daily_report = today_str

            # Weekly cache cleanup (runs on Mondays alongside daily report)
            if now_utc.weekday() == 0:  # Monday
                now_cache = time.time()
                expired_social = [k for k, (_, exp) in _social_signal_cache.items() if exp < now_cache]
                for k in expired_social:
                    del _social_signal_cache[k]
                stale_helius = [k for k, v in _helius_vol_cache.items() if now_cache - v.get('ts', 0) > 7200]
                for k in stale_helius:
                    del _helius_vol_cache[k]
                # Clear unbounded caches that grow with every unique token seen
                pool_count = len(_SHARED_POOL_CACHE)
                _SHARED_POOL_CACHE.clear()
                try:
                    import matrix_evaluator as _me
                    _me.MatrixEvaluator.clear_kline_cache()
                except Exception:
                    pass
                clear_dex_trend_cache()
                log.info(
                    f"  [CACHE_CLEANUP] social={len(expired_social)} helius={len(stale_helius)} "
                    f"pool={pool_count} kline+dex_trend=cleared"
                )

        time.sleep(MAIN_LOOP_TICK_SEC)
      except KeyboardInterrupt:
        raise
      except Exception as loop_err:
        log.error(f"[MAIN_LOOP] Unhandled error in main loop iteration (recovering): {loop_err}", exc_info=True)
        time.sleep(5)


# === Main ===

def main():
    # Graceful shutdown
    running = [True]

    def handle_signal(signum, frame):
        log.info("Shutting down gracefully...")
        running[0] = False
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    db = init_paper_db()

    if '--dry-run' not in sys.argv and '--stats' not in sys.argv and '--daily' not in sys.argv:
        wait_for_local_signal_source()

    if '--dry-run' in sys.argv:
        dry_run(db)
    elif '--stats' in sys.argv:
        print_all_stats(db)
    elif '--daily' in sys.argv:
        date = None
        idx = sys.argv.index('--daily')
        if idx + 1 < len(sys.argv) and not sys.argv[idx + 1].startswith('-'):
            date = sys.argv[idx + 1]
        print_daily_report(db, date)
    else:
        try:
            run_monitor(db)
        except Exception as e:
            log.error(f"CRITICAL ERROR: Paper trade monitor crashed: {e}", exc_info=True)
            sys.exit(1)

    db.close()


if __name__ == '__main__':
    main()
