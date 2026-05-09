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
    Live monitor:
      python3 scripts/paper_trade_monitor.py
    Dry run from recent signals:
      python3 scripts/paper_trade_monitor.py --dry-run
    Cumulative stats:
      python3 scripts/paper_trade_monitor.py --stats
    Filtered stats:
      python3 scripts/paper_trade_monitor.py --stats --stats-min-id 1322
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
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor

from watchlist_store import WatchlistStore
from matrix_evaluator import MatrixEvaluator, ExitMatrixEvaluator
from entry_engine import (
    calculate_kelly_position, evaluate_smart_entry,
    fetch_dexscreener_trend_snapshot, evaluate_trend_phase,
    clear_dex_trend_cache,
    get_liquidity_position_cap, get_adaptive_stop_loss,
    is_chasing_top,
    get_recent_synthetic_bars,
    KELLY_BASE_CAPITAL_SOL, KELLY_BASE_WIN_RATE, KELLY_BASE_ODDS, KELLY_COLD_START_ODDS,
    SMART_ENTRY_MAX_WAIT_SEC, SMART_ENTRY_POLL_INTERVAL_SEC,
)
from exit_engine import ExitGuardianThread, process_guardian_exits
from profit_protect_policy import probe_runner_floor
from paper_decision_audit import (
    init_decision_audit,
    missed_attribution_coverage,
    record_decision_event,
    signal_payload,
    update_due_missed_attributions,
)
from lifecycle_classifier import classify_lifecycle
from entry_decision_contract import build_entry_decision_contract
from entry_readiness_policy import PAPER_TINY_SCOUT_MODES, evaluate_entry_readiness_policy
from phase_policy import evaluate_phase_policy
from signal_router import route_signal
from gmgn_readonly import fetch_gmgn_token_enrichment, gmgn_readonly_runtime_status
from gmgn_policy import evaluate_gmgn_lotto_policy, evaluate_gmgn_tiny_scout_rescue
from scout_quality import SCOUT_QUALITY_SIZE_CAP_SOL, evaluate_scout_quality
from entry_mode_quality import evaluate_entry_mode_quality
from external_alpha_shadow import init_external_alpha_shadow, lookup_external_alpha
from lotto_engine import (
    LOTTO_POSITION_SIZE_SOL,
    LOTTO_STRATEGY_ID,
    LOTTO_MAX_CONCURRENT,
    LottoDecision,
    active_lotto_count,
    build_ath_boost_updates,
    build_lotto_pending,
    evaluate_lotto_entry,
    evaluate_lotto_exit,
    is_lotto_position,
)

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
MATRIX_SPREAD_WARN_PCT = float(os.environ.get('MATRIX_SPREAD_WARN_PCT', '2.0'))
MATRIX_SPREAD_ABORT_PCT = float(os.environ.get('MATRIX_SPREAD_ABORT_PCT', '4.5'))
ENTRY_EDGE_MATRIX_MAX_SPREAD_PCT = float(os.environ.get('ENTRY_EDGE_MATRIX_MAX_SPREAD_PCT', '2.5'))
ENTRY_EDGE_ATH_MAX_SPREAD_PCT = float(os.environ.get('ENTRY_EDGE_ATH_MAX_SPREAD_PCT', '2.5'))
ENTRY_EDGE_LOTTO_MAX_SPREAD_PCT = float(os.environ.get('ENTRY_EDGE_LOTTO_MAX_SPREAD_PCT', '3.5'))
ENTRY_EDGE_LOTTO_RISKY_MAX_SPREAD_PCT = float(os.environ.get('ENTRY_EDGE_LOTTO_RISKY_MAX_SPREAD_PCT', '2.5'))
ENTRY_EDGE_LOTTO_PROBE_MAX_SPREAD_PCT = float(os.environ.get('ENTRY_EDGE_LOTTO_PROBE_MAX_SPREAD_PCT', '2.0'))
ENTRY_EDGE_TINY_SCOUT_MAX_SPREAD_PCT = float(os.environ.get('ENTRY_EDGE_TINY_SCOUT_MAX_SPREAD_PCT', '5.0'))
ENTRY_EDGE_MIN_FOLLOW_PEAK_PCT = float(os.environ.get('ENTRY_EDGE_MIN_FOLLOW_PEAK_PCT', '5.0'))
ENTRY_SPREAD_ABORT_MEMORY_SEC = float(os.environ.get('ENTRY_SPREAD_ABORT_MEMORY_SEC', str(3 * 60)))
ENTRY_SPREAD_ABORT_RECLAIM_M5_PCT = float(os.environ.get('ENTRY_SPREAD_ABORT_RECLAIM_M5_PCT', '2.0'))
ENTRY_SPREAD_ABORT_RECLAIM_BS = float(os.environ.get('ENTRY_SPREAD_ABORT_RECLAIM_BS', '1.15'))
MATRIX_DOA_EXIT_SEC = float(os.environ.get('MATRIX_DOA_EXIT_SEC', '30'))
MATRIX_DOA_PEAK_MAX = float(os.environ.get('MATRIX_DOA_PEAK_MAX', '0.001'))
MATRIX_DOA_PNL_MAX = float(os.environ.get('MATRIX_DOA_PNL_MAX', '-0.03'))
MATRIX_ATH_FULL_MC_MAX = float(os.environ.get('MATRIX_ATH_FULL_MC_MAX', '80000'))
MATRIX_ATH_HALF_MC_MAX = float(os.environ.get('MATRIX_ATH_HALF_MC_MAX', '200000'))
MATRIX_ATH_FULL_SIZE_SOL = float(os.environ.get('MATRIX_ATH_FULL_SIZE_SOL', '0.08'))
MATRIX_ATH_HALF_SIZE_SOL = float(os.environ.get('MATRIX_ATH_HALF_SIZE_SOL', '0.04'))
PAPER_TINY_SCOUT_ENTRY_MODES = set(PAPER_TINY_SCOUT_MODES)
PAPER_TINY_SCOUT_SIZE_SOL = float(os.environ.get('PAPER_TINY_SCOUT_SIZE_SOL', '0.003'))
PRIMARY_PROVING_CAP_ENABLED = os.environ.get('PRIMARY_PROVING_CAP_ENABLED', 'true').lower() != 'false'
PRIMARY_PROVING_CAP_SIZE_SOL = float(os.environ.get('PRIMARY_PROVING_CAP_SIZE_SOL', '0.005'))
PRIMARY_PROVING_CAP_MODES = {
    item.strip()
    for item in os.environ.get('PRIMARY_PROVING_CAP_MODES', 'momentum_direct_entry').split(',')
    if item.strip()
}
SMART_PULLBACK_BOUNCE_PROVING_CAP_ENABLED = os.environ.get('SMART_PULLBACK_BOUNCE_PROVING_CAP_ENABLED', 'true').lower() != 'false'
SMART_PULLBACK_BOUNCE_PROVING_CAP_SOL = float(os.environ.get('SMART_PULLBACK_BOUNCE_PROVING_CAP_SOL', '0.005'))
SMART_PULLBACK_BOUNCE_DEGRADED_CAP_SOL = float(os.environ.get('SMART_PULLBACK_BOUNCE_DEGRADED_CAP_SOL', str(PAPER_TINY_SCOUT_SIZE_SOL)))
LOTTO_PULLBACK_SIZE_PROTECT_ENABLED = os.environ.get('LOTTO_PULLBACK_SIZE_PROTECT_ENABLED', 'true').lower() != 'false'
LOTTO_PULLBACK_STRONG_MIN_TX_M5 = float(os.environ.get('LOTTO_PULLBACK_STRONG_MIN_TX_M5', '150'))
LOTTO_PULLBACK_STRONG_MIN_VOL_M5 = float(os.environ.get('LOTTO_PULLBACK_STRONG_MIN_VOL_M5', '15000'))
LOTTO_PULLBACK_STRONG_MIN_BS = float(os.environ.get('LOTTO_PULLBACK_STRONG_MIN_BS', '1.20'))
DISCOVERY_FINAL_RECLAIM_ENABLED = os.environ.get('DISCOVERY_FINAL_RECLAIM_ENABLED', 'true').lower() != 'false'
TINY_EXIT_QUOTE_SANITY_ENABLED = os.environ.get('TINY_EXIT_QUOTE_SANITY_ENABLED', 'true').lower() != 'false'
TINY_EXIT_QUOTE_SANITY_MIN_PEAK = float(os.environ.get('TINY_EXIT_QUOTE_SANITY_MIN_PEAK', '0.06'))
TINY_EXIT_QUOTE_SANITY_MAX_PEAK = float(os.environ.get('TINY_EXIT_QUOTE_SANITY_MAX_PEAK', '0.10'))
TINY_EXIT_QUOTE_SANITY_NEG_GAP = float(os.environ.get('TINY_EXIT_QUOTE_SANITY_NEG_GAP', '0.10'))

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

SOL_MINT = 'So11111111111111111111111111111111111111112'
PRICE_UNIT_SOL_PER_TOKEN = 'SOL_PER_TOKEN'
PRICE_UNIT_UNKNOWN = 'UNKNOWN'
PNL_UNIT_RATIO_DECIMAL = 'RATIO_DECIMAL'
AMOUNT_UNIT_SOL = 'SOL'
AMOUNT_UNIT_TOKEN = 'TOKEN'
PRICE_UNIT_CONTRACT_VERSION = 'v1_quote_truth_sol_per_token'

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
LOTTO_PHASE_POLICY_LIVE_EXIT = os.environ.get('LOTTO_PHASE_POLICY_LIVE_EXIT', 'true').lower() != 'false'
LOTTO_PROBE_SHADOW_ENABLED = os.environ.get('LOTTO_PROBE_SHADOW_ENABLED', 'true').lower() != 'false'
LOTTO_PROBE_SHADOW_MIN_5M_PNL = float(os.environ.get('LOTTO_PROBE_SHADOW_MIN_5M_PNL', '0.20'))
LOTTO_PROBE_SHADOW_SIZE_SOL = float(os.environ.get('LOTTO_PROBE_SHADOW_SIZE_SOL', '0.03'))
EXPLOSIVE_CONTINUATION_SHADOW_ENABLED = os.environ.get('EXPLOSIVE_CONTINUATION_SHADOW_ENABLED', 'true').lower() != 'false'
EXPLOSIVE_CONTINUATION_SHADOW_LOOKBACK_SEC = int(os.environ.get('EXPLOSIVE_CONTINUATION_SHADOW_LOOKBACK_SEC', str(2 * 60 * 60)))
LOTTO_REAL_PROBE_ENABLED = os.environ.get('LOTTO_REAL_PROBE_ENABLED', 'true').lower() != 'false'
LOTTO_REAL_PROBE_MIN_MAX_PNL = float(os.environ.get('LOTTO_REAL_PROBE_MIN_MAX_PNL', '0.25'))
LOTTO_REAL_PROBE_MIN_15M_PNL = float(os.environ.get('LOTTO_REAL_PROBE_MIN_15M_PNL', '0.15'))
LOTTO_REAL_PROBE_SIZE_SOL = float(os.environ.get('LOTTO_REAL_PROBE_SIZE_SOL', '0.03'))
LOTTO_REAL_PROBE_MAX_AGE_SEC = int(os.environ.get('LOTTO_REAL_PROBE_MAX_AGE_SEC', str(30 * 60)))
LOTTO_REAL_PROBE_DECAY_FACTOR = float(os.environ.get('LOTTO_REAL_PROBE_DECAY_FACTOR', '0.5'))
LOTTO_UPSTREAM_MISS_TINY_SCOUT_ENABLED = os.environ.get('LOTTO_UPSTREAM_MISS_TINY_SCOUT_ENABLED', 'true').lower() != 'false'
LOTTO_UPSTREAM_MISS_TINY_SCOUT_SIZE_SOL = float(os.environ.get('LOTTO_UPSTREAM_MISS_TINY_SCOUT_SIZE_SOL', str(PAPER_TINY_SCOUT_SIZE_SOL)))
LOTTO_UPSTREAM_MISS_TINY_SCOUT_MAX_AGE_SEC = int(os.environ.get('LOTTO_UPSTREAM_MISS_TINY_SCOUT_MAX_AGE_SEC', str(45 * 60)))
LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_MAX_PNL = float(os.environ.get('LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_MAX_PNL', '0.25'))
LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_RECLAIM_PNL = float(os.environ.get('LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_RECLAIM_PNL', '0.20'))
LOTTO_UPSTREAM_MISS_TINY_SCOUT_MAX_MC = float(os.environ.get('LOTTO_UPSTREAM_MISS_TINY_SCOUT_MAX_MC', '200000'))
LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_LIQ_USD = float(os.environ.get('LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_LIQ_USD', '5000'))
LOTTO_UPSTREAM_MISS_TINY_SCOUT_MODE = 'lotto_upstream_miss_tiny_scout'
LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_ENABLED = os.environ.get('LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_ENABLED', 'true').lower() != 'false'
LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_SIZE_SOL = float(os.environ.get('LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_SIZE_SOL', str(PAPER_TINY_SCOUT_SIZE_SOL)))
LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MAX_MC = float(os.environ.get('LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MAX_MC', '200000'))
LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MIN_LIQ_USD = float(os.environ.get('LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MIN_LIQ_USD', '5000'))
LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MAX_TOP1_PCT = float(os.environ.get('LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MAX_TOP1_PCT', '50'))
LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MAX_TOP10_PCT = float(os.environ.get('LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MAX_TOP10_PCT', '70'))
LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MODE = 'lotto_upstream_realtime_tiny_scout'
LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE = 'lotto_not_ath_reclaim_tiny_probe'
LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE = 'lotto_low_liquidity_reclaim_tiny_probe'
LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE = 'lotto_micro_reclaim_tiny_probe'
LOTTO_RECOVERY_TINY_PROBES_ENABLED = os.environ.get('LOTTO_RECOVERY_TINY_PROBES_ENABLED', 'true').lower() != 'false'
LOTTO_RECLAIM_MAX_MC = float(os.environ.get('LOTTO_RECLAIM_MAX_MC', '250000'))
LOTTO_RECLAIM_MIN_LIQ_USD = float(os.environ.get('LOTTO_RECLAIM_MIN_LIQ_USD', '5000'))
LOTTO_NOT_ATH_RECLAIM_MIN_BS = float(os.environ.get('LOTTO_NOT_ATH_RECLAIM_MIN_BS', '1.15'))
LOTTO_NOT_ATH_RECLAIM_MIN_TX_M5 = float(os.environ.get('LOTTO_NOT_ATH_RECLAIM_MIN_TX_M5', '70'))
LOTTO_NOT_ATH_RECLAIM_MIN_PC_M5 = float(os.environ.get('LOTTO_NOT_ATH_RECLAIM_MIN_PC_M5', '2.0'))
LOTTO_LOW_LIQ_RECLAIM_MIN_BS = float(os.environ.get('LOTTO_LOW_LIQ_RECLAIM_MIN_BS', '1.20'))
LOTTO_LOW_LIQ_RECLAIM_MIN_VOL_M5 = float(os.environ.get('LOTTO_LOW_LIQ_RECLAIM_MIN_VOL_M5', '0'))
LOTTO_LOW_LIQ_RECLAIM_MIN_TX_M5 = float(os.environ.get('LOTTO_LOW_LIQ_RECLAIM_MIN_TX_M5', '50'))
LOTTO_LOW_LIQ_RECLAIM_MIN_PC_M5 = float(os.environ.get('LOTTO_LOW_LIQ_RECLAIM_MIN_PC_M5', '0.0'))
LOTTO_MICRO_RECLAIM_MAX_WATCH_SEC = int(os.environ.get('LOTTO_MICRO_RECLAIM_MAX_WATCH_SEC', str(10 * 60)))
LOTTO_MICRO_RECLAIM_MIN_BOUNCE_PCT = float(os.environ.get('LOTTO_MICRO_RECLAIM_MIN_BOUNCE_PCT', '6.0'))
LOTTO_MICRO_RECLAIM_MIN_BS = float(os.environ.get('LOTTO_MICRO_RECLAIM_MIN_BS', '1.20'))
LOTTO_MICRO_RECLAIM_MIN_VOL_M5 = float(os.environ.get('LOTTO_MICRO_RECLAIM_MIN_VOL_M5', '4000'))
LOTTO_MICRO_RECLAIM_MIN_TX_M5 = float(os.environ.get('LOTTO_MICRO_RECLAIM_MIN_TX_M5', '40'))
LOTTO_DYNAMIC_TTL_ENABLED = os.environ.get('LOTTO_DYNAMIC_TTL_ENABLED', 'true').lower() != 'false'
LOTTO_DYNAMIC_TTL_EXTEND_SEC = int(os.environ.get('LOTTO_DYNAMIC_TTL_EXTEND_SEC', str(15 * 60)))
LOTTO_DYNAMIC_TTL_MAX_EXTENSIONS = int(os.environ.get('LOTTO_DYNAMIC_TTL_MAX_EXTENSIONS', '2'))
LOTTO_UPSTREAM_MISS_TINY_SCOUT_REASONS = {
    'not_ath_v17',
    'not_ath_prebuy_kline_unknown_data_blocked',
    'lotto_observe_low_mc_vol',
    'tracking_ttl_expired',
    'trend_bearish_timeout',
    'upstream_realtime_liquidity_too_low',
    'discovery_liquidity_too_low',
    'liquidity_too_low',
    'scout_quality_volume_low',
    'scout_quality_negative_trend',
    'scout_quality_buy_pressure_weak',
    'scout_quality_tx_low',
    'score_too_low',
    'no_kline_low_volume',
    'lotto_timing_negative_m5',
}
ATH_UNCERTAINTY_TINY_SCOUT_ENABLED = os.environ.get('ATH_UNCERTAINTY_TINY_SCOUT_ENABLED', 'true').lower() != 'false'
ATH_UNCERTAINTY_TINY_SCOUT_SIZE_SOL = float(os.environ.get('ATH_UNCERTAINTY_TINY_SCOUT_SIZE_SOL', str(PAPER_TINY_SCOUT_SIZE_SOL)))
ATH_UNCERTAINTY_TINY_SCOUT_MAX_MC = float(os.environ.get('ATH_UNCERTAINTY_TINY_SCOUT_MAX_MC', '400000'))
ATH_UNCERTAINTY_TINY_SCOUT_RUNNER_MAX_MC = float(os.environ.get('ATH_UNCERTAINTY_TINY_SCOUT_RUNNER_MAX_MC', '1250000'))
ATH_UNCERTAINTY_TINY_SCOUT_SHADOW_MAX_MC = float(os.environ.get('ATH_UNCERTAINTY_TINY_SCOUT_SHADOW_MAX_MC', '3000000'))
ATH_UNCERTAINTY_TINY_SCOUT_MIN_LIQ_USD = float(os.environ.get('ATH_UNCERTAINTY_TINY_SCOUT_MIN_LIQ_USD', '5000'))
ATH_UNCERTAINTY_TINY_SCOUT_MODE = 'ath_uncertainty_tiny_scout'
ATH_NO_KLINE_TINY_PROBE_MODE = 'ath_no_kline_tiny_probe'
ATH_HIGH_MC_TINY_PROBE_MODE = 'ath_high_mc_tiny_probe'
ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE = 'ath_reclaim_after_failure_tiny_probe'
ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE = 'ath_matrix_dissonance_tiny_probe'
ATH_MICRO_RECLAIM_TINY_PROBE_MODE = 'ath_micro_reclaim_tiny_probe'
PAPER_TINY_SCOUT_ENTRY_MODES.update({
    ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE,
    ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
    ATH_MICRO_RECLAIM_TINY_PROBE_MODE,
    LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
    LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE,
    LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
})
ATH_HIGH_MC_TINY_PROBE_ENABLED = os.environ.get('ATH_HIGH_MC_TINY_PROBE_ENABLED', 'true').lower() != 'false'
ATH_HIGH_MC_TINY_PROBE_MAX_MC = float(os.environ.get('ATH_HIGH_MC_TINY_PROBE_MAX_MC', str(ATH_UNCERTAINTY_TINY_SCOUT_RUNNER_MAX_MC)))
ATH_RECOVERY_TINY_PROBES_ENABLED = os.environ.get('ATH_RECOVERY_TINY_PROBES_ENABLED', 'true').lower() != 'false'
ATH_RECLAIM_AFTER_FAILURE_ENABLED = os.environ.get('ATH_RECLAIM_AFTER_FAILURE_ENABLED', 'true').lower() != 'false'
ATH_MATRIX_DISSONANCE_TINY_PROBE_ENABLED = os.environ.get('ATH_MATRIX_DISSONANCE_TINY_PROBE_ENABLED', 'true').lower() != 'false'
ATH_MICRO_RECLAIM_WATCH_ENABLED = os.environ.get('ATH_MICRO_RECLAIM_WATCH_ENABLED', 'true').lower() != 'false'
ATH_DYNAMIC_TTL_ENABLED = os.environ.get('ATH_DYNAMIC_TTL_ENABLED', 'true').lower() != 'false'
ATH_RECOVERY_COOLDOWN_SEC = int(os.environ.get('ATH_RECOVERY_COOLDOWN_SEC', str(4 * 60 * 60)))
ATH_RECOVERY_HARD_LOSS_COOLDOWN_SEC = int(os.environ.get('ATH_RECOVERY_HARD_LOSS_COOLDOWN_SEC', str(6 * 60 * 60)))
ATH_RECOVERY_MAX_ATTEMPTS_PER_TOKEN = int(os.environ.get('ATH_RECOVERY_MAX_ATTEMPTS_PER_TOKEN', '1'))
ATH_RECOVERY_MIN_RECLAIM_PCT = float(os.environ.get('ATH_RECOVERY_MIN_RECLAIM_PCT', '8.0'))
ATH_RECOVERY_MIN_BS = float(os.environ.get('ATH_RECOVERY_MIN_BS', '1.25'))
ATH_RECOVERY_MIN_TX_M5 = float(os.environ.get('ATH_RECOVERY_MIN_TX_M5', '80'))
ATH_RECOVERY_MIN_T = int(os.environ.get('ATH_RECOVERY_MIN_T', '80'))
ATH_RECOVERY_MIN_P = int(os.environ.get('ATH_RECOVERY_MIN_P', '80'))
ATH_RECOVERY_MIN_S = int(os.environ.get('ATH_RECOVERY_MIN_S', '100'))
ATH_RECOVERY_HARD_LOSS_PNL = float(os.environ.get('ATH_RECOVERY_HARD_LOSS_PNL', '-0.30'))
ATH_RECOVERY_HARD_LOSS_LOW_PEAK = float(os.environ.get('ATH_RECOVERY_HARD_LOSS_LOW_PEAK', '0.05'))
ATH_RECLAIM_AFTER_FAILURE_MIN_RECLAIM_PCT = float(os.environ.get('ATH_RECLAIM_AFTER_FAILURE_MIN_RECLAIM_PCT', '12.0'))
ATH_RECLAIM_AFTER_FAILURE_MIN_BS = float(os.environ.get('ATH_RECLAIM_AFTER_FAILURE_MIN_BS', '1.35'))
ATH_RECLAIM_AFTER_FAILURE_MIN_TX_M5 = float(os.environ.get('ATH_RECLAIM_AFTER_FAILURE_MIN_TX_M5', '50'))
ATH_RECLAIM_AFTER_FAILURE_MIN_T = int(os.environ.get('ATH_RECLAIM_AFTER_FAILURE_MIN_T', '50'))
ATH_RECLAIM_AFTER_FAILURE_MIN_P = int(os.environ.get('ATH_RECLAIM_AFTER_FAILURE_MIN_P', '80'))
ATH_RECLAIM_AFTER_FAILURE_MIN_S = int(os.environ.get('ATH_RECLAIM_AFTER_FAILURE_MIN_S', '100'))
ATH_MATRIX_DISSONANCE_MIN_T = int(os.environ.get('ATH_MATRIX_DISSONANCE_MIN_T', '40'))
ATH_MATRIX_DISSONANCE_MIN_BS = float(os.environ.get('ATH_MATRIX_DISSONANCE_MIN_BS', '1.20'))
ATH_MATRIX_DISSONANCE_MIN_TX_M5 = float(os.environ.get('ATH_MATRIX_DISSONANCE_MIN_TX_M5', '80'))
ATH_MATRIX_DISSONANCE_MIN_PC_M5 = float(os.environ.get('ATH_MATRIX_DISSONANCE_MIN_PC_M5', '0.0'))
ATH_MATRIX_DISSONANCE_MIN_LIQUIDITY_USD = float(os.environ.get('ATH_MATRIX_DISSONANCE_MIN_LIQUIDITY_USD', '5000'))
ATH_MICRO_RECLAIM_MAX_WATCH_SEC = int(os.environ.get('ATH_MICRO_RECLAIM_MAX_WATCH_SEC', str(10 * 60)))
ATH_MICRO_RECLAIM_MIN_BOUNCE_PCT = float(os.environ.get('ATH_MICRO_RECLAIM_MIN_BOUNCE_PCT', '6.0'))
ATH_MICRO_RECLAIM_MIN_BS = float(os.environ.get('ATH_MICRO_RECLAIM_MIN_BS', '1.25'))
ATH_MICRO_RECLAIM_MIN_TX_M5 = float(os.environ.get('ATH_MICRO_RECLAIM_MIN_TX_M5', '80'))
ATH_DYNAMIC_TTL_EXTEND_SEC = int(os.environ.get('ATH_DYNAMIC_TTL_EXTEND_SEC', str(15 * 60)))
ATH_DYNAMIC_TTL_MAX_EXTENSIONS = int(os.environ.get('ATH_DYNAMIC_TTL_MAX_EXTENSIONS', '2'))
ATH_NO_KLINE_REENTRY_GUARD_ENABLED = os.environ.get('ATH_NO_KLINE_REENTRY_GUARD_ENABLED', 'true').lower() != 'false'
ATH_NO_KLINE_REENTRY_LOOKBACK_SEC = int(os.environ.get('ATH_NO_KLINE_REENTRY_LOOKBACK_SEC', str(4 * 60 * 60)))
ATH_NO_KLINE_REENTRY_HARD_LOSS_COOLDOWN_SEC = int(os.environ.get('ATH_NO_KLINE_REENTRY_HARD_LOSS_COOLDOWN_SEC', str(6 * 60 * 60)))
ATH_NO_KLINE_REENTRY_HARD_LOSS_PNL = float(os.environ.get('ATH_NO_KLINE_REENTRY_HARD_LOSS_PNL', '-0.20'))
ATH_NO_KLINE_REENTRY_LOW_FOLLOW_PEAK = float(os.environ.get('ATH_NO_KLINE_REENTRY_LOW_FOLLOW_PEAK', '0.10'))
ATH_NO_KLINE_REENTRY_MIN_RECOVERY_PCT = float(os.environ.get('ATH_NO_KLINE_REENTRY_MIN_RECOVERY_PCT', '8.0'))
ATH_NO_KLINE_REENTRY_MAX_RECENT_ENTRIES = int(os.environ.get('ATH_NO_KLINE_REENTRY_MAX_RECENT_ENTRIES', '3'))
ATH_NO_KLINE_REENTRY_MIN_T = int(os.environ.get('ATH_NO_KLINE_REENTRY_MIN_T', '80'))
ATH_NO_KLINE_REENTRY_MIN_P = int(os.environ.get('ATH_NO_KLINE_REENTRY_MIN_P', '80'))
ATH_NO_KLINE_REENTRY_MIN_S = int(os.environ.get('ATH_NO_KLINE_REENTRY_MIN_S', '100'))
ATH_NO_KLINE_REENTRY_WINNER_PEAK = float(os.environ.get('ATH_NO_KLINE_REENTRY_WINNER_PEAK', '0.30'))
ATH_NO_KLINE_REENTRY_HARD_SL_LOW_PEAK = float(os.environ.get('ATH_NO_KLINE_REENTRY_HARD_SL_LOW_PEAK', '0.05'))
ATH_NO_KLINE_FOLLOWTHROUGH_GUARD_ENABLED = os.environ.get('ATH_NO_KLINE_FOLLOWTHROUGH_GUARD_ENABLED', 'true').lower() != 'false'
ATH_NO_KLINE_FOLLOWTHROUGH_MIN_BS = float(os.environ.get('ATH_NO_KLINE_FOLLOWTHROUGH_MIN_BS', '1.20'))
ATH_NO_KLINE_FOLLOWTHROUGH_MIN_TX_M5 = float(os.environ.get('ATH_NO_KLINE_FOLLOWTHROUGH_MIN_TX_M5', '30'))
ATH_NO_KLINE_FOLLOWTHROUGH_MIN_PC_M5 = float(os.environ.get('ATH_NO_KLINE_FOLLOWTHROUGH_MIN_PC_M5', '0.0'))
ATH_NO_KLINE_FOLLOWTHROUGH_STRONG_PC_M5 = float(os.environ.get('ATH_NO_KLINE_FOLLOWTHROUGH_STRONG_PC_M5', '6.0'))
ATH_REENTRY_MATRIX_BLOCK_SEC = int(os.environ.get('ATH_REENTRY_MATRIX_BLOCK_SEC', '120'))
ATH_REENTRY_LOW_FOLLOW_BLOCK_SEC = int(os.environ.get('ATH_REENTRY_LOW_FOLLOW_BLOCK_SEC', '600'))
ATH_NO_KLINE_VOLUME_LOW_WARN_ENABLED = os.environ.get('ATH_NO_KLINE_VOLUME_LOW_WARN_ENABLED', 'true').lower() != 'false'
ATH_NO_KLINE_VOLUME_LOW_WARN_MIN_BS = float(os.environ.get('ATH_NO_KLINE_VOLUME_LOW_WARN_MIN_BS', '1.20'))
PULLBACK_TINY_SCOUT_FORCE_LIVE_ENABLED = os.environ.get('PULLBACK_TINY_SCOUT_FORCE_LIVE_ENABLED', 'false').lower() == 'true'
ENTRY_MODE_QUALITY_HIGH_QUALITY_TINY_OVERRIDE_ENABLED = os.environ.get('ENTRY_MODE_QUALITY_HIGH_QUALITY_TINY_OVERRIDE_ENABLED', 'true').lower() != 'false'
ENTRY_MODE_QUALITY_OVERRIDE_MIN_T = int(os.environ.get('ENTRY_MODE_QUALITY_OVERRIDE_MIN_T', '80'))
ENTRY_MODE_QUALITY_OVERRIDE_MIN_P = int(os.environ.get('ENTRY_MODE_QUALITY_OVERRIDE_MIN_P', '80'))
ENTRY_MODE_QUALITY_OVERRIDE_MIN_S = int(os.environ.get('ENTRY_MODE_QUALITY_OVERRIDE_MIN_S', '100'))
ATH_UNCERTAINTY_REASONS = (
    'matrices not yet aligned',
    'momentum check failed:',
    'momentum check waiting: flat_no_fresh_tick',
)
ATH_REAL_PROBE_ENABLED = os.environ.get('ATH_REAL_PROBE_ENABLED', 'true').lower() != 'false'
ATH_REAL_PROBE_MIN_MAX_PNL = float(os.environ.get('ATH_REAL_PROBE_MIN_MAX_PNL', '0.50'))
ATH_REAL_PROBE_MIN_RECLAIM_PNL = float(os.environ.get('ATH_REAL_PROBE_MIN_RECLAIM_PNL', '0.25'))
ATH_REAL_PROBE_SIZE_SOL = float(os.environ.get('ATH_REAL_PROBE_SIZE_SOL', str(PAPER_TINY_SCOUT_SIZE_SOL)))
ATH_REAL_PROBE_MAX_AGE_SEC = int(os.environ.get('ATH_REAL_PROBE_MAX_AGE_SEC', str(45 * 60)))
ATH_REAL_PROBE_MAX_MC = float(os.environ.get('ATH_REAL_PROBE_MAX_MC', str(MATRIX_ATH_HALF_MC_MAX)))
ATH_REAL_PROBE_MIN_LIQ_USD = float(os.environ.get('ATH_REAL_PROBE_MIN_LIQ_USD', '5000'))
SCOUT_TELEMETRY_ENABLED = os.environ.get('SCOUT_TELEMETRY_ENABLED', 'true').lower() != 'false'
SCOUT_FUNNEL_LOOKBACK_SEC = int(os.environ.get('SCOUT_FUNNEL_LOOKBACK_SEC', str(24 * 60 * 60)))
SCOUT_FUNNEL_SUMMARY_INTERVAL_SEC = int(os.environ.get('SCOUT_FUNNEL_SUMMARY_INTERVAL_SEC', '300'))
SCOUT_UPSTREAM_CHAIN_LOOKBACK_SEC = int(os.environ.get('SCOUT_UPSTREAM_CHAIN_LOOKBACK_SEC', str(24 * 60 * 60)))
SCOUT_UPSTREAM_CHAIN_LIMIT = int(os.environ.get('SCOUT_UPSTREAM_CHAIN_LIMIT', '300'))
DISCOVERY_TRACKING_ENABLED = os.environ.get('DISCOVERY_TRACKING_ENABLED', 'true').lower() != 'false'
DISCOVERY_TRACKING_POLL_SEC = max(1, int(os.environ.get('DISCOVERY_TRACKING_POLL_SEC', '10')))
DISCOVERY_TRACKING_TTL_SEC = max(30, int(os.environ.get('DISCOVERY_TRACKING_TTL_SEC', '3600')))
DISCOVERY_TRACKING_MAX_CANDIDATES = max(1, int(os.environ.get('DISCOVERY_TRACKING_MAX_CANDIDATES', '120')))
DISCOVERY_TRACKING_MAX_ARMS_PER_CYCLE = max(1, int(os.environ.get('DISCOVERY_TRACKING_MAX_ARMS_PER_CYCLE', '2')))
DISCOVERY_TRACKING_MAX_EVALS_PER_CYCLE = max(1, int(os.environ.get('DISCOVERY_TRACKING_MAX_EVALS_PER_CYCLE', '12')))
DISCOVERY_LIQUIDITY_LOW_BACKOFF_SEC = max(
    30,
    int(os.environ.get('DISCOVERY_LIQUIDITY_LOW_BACKOFF_SEC', '300')),
)
DISCOVERY_LIQUIDITY_LOW_MAX_RECHECKS = max(
    1,
    int(os.environ.get('DISCOVERY_LIQUIDITY_LOW_MAX_RECHECKS', '3')),
)
DISCOVERY_MIN_LIQUIDITY_USD = float(os.environ.get('DISCOVERY_MIN_LIQUIDITY_USD', '5000'))
DISCOVERY_LOW_LIQ_BYPASS_ENABLED = os.environ.get('DISCOVERY_LOW_LIQ_BYPASS_ENABLED', 'true').lower() != 'false'
DISCOVERY_LOTTO_HIGH_RISK_MIN_LIQUIDITY_USD = float(os.environ.get('DISCOVERY_LOTTO_HIGH_RISK_MIN_LIQUIDITY_USD', '5000'))
DISCOVERY_UNKNOWN_ACTIVITY_MAX_MC = float(os.environ.get('DISCOVERY_UNKNOWN_ACTIVITY_MAX_MC', '250000'))
DISCOVERY_LOTTO_HIGH_RISK_MAX_MC = float(os.environ.get('DISCOVERY_LOTTO_HIGH_RISK_MAX_MC', '150000'))
DISCOVERY_LOTTO_HIGH_RISK_EXTREME_TOP1_PCT = float(os.environ.get('DISCOVERY_LOTTO_HIGH_RISK_EXTREME_TOP1_PCT', '70'))
DISCOVERY_LOTTO_HIGH_RISK_EXTREME_TOP10_PCT = float(os.environ.get('DISCOVERY_LOTTO_HIGH_RISK_EXTREME_TOP10_PCT', '90'))
DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_TX_M5 = float(os.environ.get('DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_TX_M5', '200'))
DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_VOL_M5 = float(os.environ.get('DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_VOL_M5', '15000'))
DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_BS = float(os.environ.get('DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_BS', '1.2'))
DISCOVERY_LOTTO_HIGH_RISK_LIVE_MAX_TOP1_PCT = float(os.environ.get('DISCOVERY_LOTTO_HIGH_RISK_LIVE_MAX_TOP1_PCT', '50'))
DISCOVERY_LOTTO_HIGH_RISK_LIVE_MAX_TOP10_PCT = float(os.environ.get('DISCOVERY_LOTTO_HIGH_RISK_LIVE_MAX_TOP10_PCT', '75'))
OBSERVATION_PROBE_COOLDOWN_SEC = int(os.environ.get('OBSERVATION_PROBE_COOLDOWN_SEC', '90'))
OBSERVATION_PROBE_TOXIC_COOLDOWN_SEC = int(os.environ.get('OBSERVATION_PROBE_TOXIC_COOLDOWN_SEC', '600'))
ATH_SOFT_RECLAIM_TINY_SCOUT_MODE = 'ath_soft_reclaim_tiny_scout'
UNKNOWN_DATA_ACTIVITY_TINY_SCOUT_MODE = 'unknown_data_activity_tiny_scout'
MATRIX_RECLAIM_TINY_PROBE_MODE = 'matrix_reclaim_tiny_probe'
MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE = 'matrix_micro_momentum_tiny_probe'
LOTTO_HIGH_RISK_DISCOVERY_PROBE_MODE = 'lotto_high_risk_discovery_probe'
ATH_OUTCOME_GUARD_ENTRY_MODES = {
    ATH_NO_KLINE_TINY_PROBE_MODE,
    ATH_UNCERTAINTY_TINY_SCOUT_MODE,
    ATH_HIGH_MC_TINY_PROBE_MODE,
    ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE,
    ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
    ATH_MICRO_RECLAIM_TINY_PROBE_MODE,
    'ath_flat_structure_tiny_scout',
    'ath_soft_reclaim_tiny_scout',
    'newborn_momentum_tiny_scout',
    MATRIX_RECLAIM_TINY_PROBE_MODE,
    MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE,
}
PROBE_PROFIT_CAPTURE_ENABLED = os.environ.get('PROBE_PROFIT_CAPTURE_ENABLED', 'true').lower() != 'false'
PROBE_PROFIT_CAPTURE_MAX_SIZE_SOL = float(os.environ.get('PROBE_PROFIT_CAPTURE_MAX_SIZE_SOL', '0.02'))
PROBE_PROFIT_CAPTURE_LOCK_PNL = float(os.environ.get('PROBE_PROFIT_CAPTURE_LOCK_PNL', '0.10'))
PROBE_PROFIT_CAPTURE_LOCK_SELL_PCT = float(os.environ.get('PROBE_PROFIT_CAPTURE_LOCK_SELL_PCT', '0.75'))
PROBE_PROFIT_CAPTURE_START_PEAK = float(os.environ.get('PROBE_PROFIT_CAPTURE_START_PEAK', '0.08'))
PROBE_PROFIT_CAPTURE_START_FLOOR = float(os.environ.get('PROBE_PROFIT_CAPTURE_START_FLOOR', '0.00'))
PROBE_PROFIT_CAPTURE_10_PEAK_FLOOR = float(os.environ.get('PROBE_PROFIT_CAPTURE_10_PEAK_FLOOR', '0.03'))
PROBE_PROFIT_CAPTURE_15_PEAK_FLOOR = float(os.environ.get('PROBE_PROFIT_CAPTURE_15_PEAK_FLOOR', '0.08'))
DISCOVERY_LOW_LIQ_QUOTE_PROBE_ENABLED = os.environ.get('DISCOVERY_LOW_LIQ_QUOTE_PROBE_ENABLED', 'true').lower() != 'false'
DISCOVERY_LOW_LIQ_EXTREME_MIN_BS = float(os.environ.get('DISCOVERY_LOW_LIQ_EXTREME_MIN_BS', '1.20'))
DISCOVERY_LOW_LIQ_EXTREME_MIN_VOL_M5 = float(os.environ.get('DISCOVERY_LOW_LIQ_EXTREME_MIN_VOL_M5', '20000'))
DISCOVERY_LOW_LIQ_EXTREME_MIN_TX_M5 = float(os.environ.get('DISCOVERY_LOW_LIQ_EXTREME_MIN_TX_M5', '200'))
DISCOVERY_LOW_LIQ_EXTREME_MAX_NEG_M5 = float(os.environ.get('DISCOVERY_LOW_LIQ_EXTREME_MAX_NEG_M5', '0'))
EXIT_QUOTE_REPRICE_DIVERGENCE_PCT = float(os.environ.get('EXIT_QUOTE_REPRICE_DIVERGENCE_PCT', '0.20'))
LIVE_PRICE_MAX_FUTURE_MS = int(os.environ.get('LIVE_PRICE_MAX_FUTURE_MS', '1500'))
LOTTO_FALLING_KNIFE_LIQ_USD = float(os.environ.get('LOTTO_FALLING_KNIFE_LIQ_USD', '15000'))
LOTTO_FALLING_KNIFE_M5_PCT = float(os.environ.get('LOTTO_FALLING_KNIFE_M5_PCT', '-20'))
TOKEN_RISK_QUARANTINE_ENABLED = os.environ.get('TOKEN_RISK_QUARANTINE_ENABLED', 'true').lower() != 'false'
TOKEN_RISK_LOSS_THRESHOLD = float(os.environ.get('TOKEN_RISK_LOSS_THRESHOLD', '-0.08'))
TOKEN_RISK_FAILURE_WINDOW_SEC = int(os.environ.get('TOKEN_RISK_FAILURE_WINDOW_SEC', str(3 * 60 * 60)))
TOKEN_RISK_BASE_COOLDOWN_SEC = int(os.environ.get('TOKEN_RISK_BASE_COOLDOWN_SEC', str(45 * 60)))
TOKEN_RISK_REPEAT_FAILURE_COUNT = int(os.environ.get('TOKEN_RISK_REPEAT_FAILURE_COUNT', '2'))
TOKEN_RISK_REPEAT_COOLDOWN_SEC = int(os.environ.get('TOKEN_RISK_REPEAT_COOLDOWN_SEC', str(2 * 60 * 60)))
TOKEN_RISK_RECLAIM_REQUIRED = os.environ.get('TOKEN_RISK_RECLAIM_REQUIRED', 'true').lower() != 'false'
TOKEN_RISK_RECLAIM_M5_PCT = float(os.environ.get('TOKEN_RISK_RECLAIM_M5_PCT', '15'))
TOKEN_RISK_RECLAIM_BS_RATIO = float(os.environ.get('TOKEN_RISK_RECLAIM_BS_RATIO', '1.25'))
TOKEN_RISK_RECLAIM_MIN_TX_M5 = int(os.environ.get('TOKEN_RISK_RECLAIM_MIN_TX_M5', '20'))
TOKEN_RISK_RECLAIM_MIN_LIQ_USD = float(os.environ.get('TOKEN_RISK_RECLAIM_MIN_LIQ_USD', '5000'))
TOKEN_RISK_NO_FOLLOW_RECLAIM_M5_PCT = float(os.environ.get('TOKEN_RISK_NO_FOLLOW_RECLAIM_M5_PCT', '25'))
TOKEN_RISK_NO_FOLLOW_RECLAIM_BS_RATIO = float(os.environ.get('TOKEN_RISK_NO_FOLLOW_RECLAIM_BS_RATIO', '1.4'))
TOKEN_RISK_NO_FOLLOW_RECLAIM_MIN_RVOL = float(os.environ.get('TOKEN_RISK_NO_FOLLOW_RECLAIM_MIN_RVOL', '1.0'))
TOKEN_RISK_WATERFALL_RECLAIM_M5_PCT = float(os.environ.get('TOKEN_RISK_WATERFALL_RECLAIM_M5_PCT', '35'))
TOKEN_RISK_WATERFALL_RECLAIM_BS_RATIO = float(os.environ.get('TOKEN_RISK_WATERFALL_RECLAIM_BS_RATIO', '1.6'))
TOKEN_RISK_WATERFALL_RECLAIM_MIN_RVOL = float(os.environ.get('TOKEN_RISK_WATERFALL_RECLAIM_MIN_RVOL', '1.5'))
LOTTO_REAL_PROBE_MIN_RECLAIM_PNL = float(os.environ.get('LOTTO_REAL_PROBE_MIN_RECLAIM_PNL', '0.15'))
LOTTO_LIFECYCLE_BLOCK_STATES = {
    s.strip().upper()
    for s in os.environ.get(
        'LOTTO_LIFECYCLE_BLOCK_STATES',
        'ATH_DEEP_RESET,DEAD_CAT_BOUNCE,DISTRIBUTION,DEAD'
    ).split(',')
    if s.strip()
}
LOTTO_TIMING_BLOCK_M5_PCT = float(os.environ.get('LOTTO_TIMING_BLOCK_M5_PCT', '-10'))

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
_LOTTO_TIMING_RETRY_MEMORY = {}


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


def _amount_unit_for_mint(mint, token_ca=None):
    mint_s = str(mint or '')
    if mint_s == SOL_MINT:
        return AMOUNT_UNIT_SOL
    if token_ca and mint_s == str(token_ca):
        return AMOUNT_UNIT_TOKEN
    if mint_s:
        return AMOUNT_UNIT_TOKEN
    return None


def execution_unit_contract(execution=None):
    payload = execution if isinstance(execution, dict) else {}
    token_ca = payload.get('tokenCA')
    input_unit = _amount_unit_for_mint(payload.get('inputMint'), token_ca)
    output_unit = _amount_unit_for_mint(payload.get('outputMint'), token_ca)
    effective_price_unit = PRICE_UNIT_UNKNOWN
    if {input_unit, output_unit} == {AMOUNT_UNIT_SOL, AMOUNT_UNIT_TOKEN}:
        # Jupiter buy: SOL / token. Jupiter sell: SOL out / token in.
        effective_price_unit = PRICE_UNIT_SOL_PER_TOKEN
    return {
        'priceUnitContractVersion': PRICE_UNIT_CONTRACT_VERSION,
        'effectivePriceUnit': effective_price_unit,
        'inputAmountUnit': input_unit,
        'quotedOutAmountUnit': output_unit,
        'accountingUnit': AMOUNT_UNIT_SOL,
        'pnlUnit': PNL_UNIT_RATIO_DECIMAL,
    }


def price_unit_contract_payload(**overrides):
    payload = {
        'priceUnitContractVersion': PRICE_UNIT_CONTRACT_VERSION,
        'entryPriceUnit': PRICE_UNIT_SOL_PER_TOKEN,
        'entryTriggerPriceUnit': PRICE_UNIT_SOL_PER_TOKEN,
        'entryQuotePriceUnit': PRICE_UNIT_SOL_PER_TOKEN,
        'exitPriceUnit': PRICE_UNIT_SOL_PER_TOKEN,
        'trailUnit': PNL_UNIT_RATIO_DECIMAL,
        'stopUnit': PNL_UNIT_RATIO_DECIMAL,
        'pnlUnit': PNL_UNIT_RATIO_DECIMAL,
        'accountingUnit': AMOUNT_UNIT_SOL,
        'marketContextUnit': 'USD_CONTEXT_ONLY',
    }
    payload.update({key: value for key, value in overrides.items() if value is not None})
    return payload


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
    audit.update(execution_unit_contract(payload))
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
    partial_realized_sol = _safe_float(monitor_state.get('partialRealizedSol'), None)
    partial_cost_basis_sol = _safe_float(monitor_state.get('partialCostBasisSol'), None)
    total_sol_received = _safe_float(monitor_state.get('totalSolReceived'), None)
    final_exit_total_sol = _safe_float(final_exit_sol, None)

    if entry_sol <= 0:
        accounting_source = 'monitor_state_total_sol_received' if has_partial_history else 'final_exit_only'
        return None, total_sol_received if has_partial_history else final_exit_total_sol, entry_sol, accounting_source

    if has_partial_history:
        # Partial locks sell only part of the position. Final PnL must blend
        # previously realized SOL with the final sale of the remaining tokens.
        prior_realized_sol = partial_realized_sol
        if prior_realized_sol is None:
            prior_realized_sol = total_sol_received
        if prior_realized_sol is None:
            prior_realized_sol = 0.0
        if final_exit_total_sol is not None and entry_sol > 0:
            total_realized = prior_realized_sol + final_exit_total_sol
            pnl = (total_realized - entry_sol) / entry_sol
            source = 'blended_partial_plus_final_sol'
            if partial_cost_basis_sol is None:
                source = 'blended_total_sol_received_plus_final_sol'
            return pnl, total_realized, entry_sol, source
        if total_sol_received is None:
            return None, total_sol_received, entry_sol, 'monitor_state_total_sol_received'
        return ((total_sol_received - entry_sol) / entry_sol), total_sol_received, entry_sol, 'monitor_state_total_sol_received'

    if final_exit_total_sol is not None:
        return ((final_exit_total_sol - entry_sol) / entry_sol), final_exit_total_sol, entry_sol, 'final_exit_only'
    if total_sol_received is not None:
        return ((total_sol_received - entry_sol) / entry_sol), total_sol_received, entry_sol, 'monitor_state_total_sol_received'
    return None, final_exit_total_sol, entry_sol, 'final_exit_only'


def apply_partial_accounting_state(state, execution, *, entry_sol, prev_sold_pct, sell_pct_delta, trigger_pnl, peak_pnl, reason, ts, quote_pnl=None, quote_mark_gap=None, quote_sanity_status=None):
    updated = dict(state or {})
    entry_sol = _safe_float(updated.get('entrySol'), entry_sol)
    prev_sold_pct = max(0.0, min(1.0, _safe_float(prev_sold_pct, 0.0)))
    sell_pct_delta = max(0.0, min(1.0 - prev_sold_pct, _safe_float(sell_pct_delta, 0.0)))
    total_sold_pct = max(prev_sold_pct, min(1.0, _safe_float(updated.get('soldPct'), prev_sold_pct + sell_pct_delta)))
    if total_sold_pct > prev_sold_pct:
        sell_pct_delta = total_sold_pct - prev_sold_pct

    partial_out_sol = _safe_float((execution or {}).get('quotedOutAmount'), None)
    prev_realized_sol = _safe_float(updated.get('partialRealizedSol'), _safe_float(updated.get('totalSolReceived'), 0.0))
    prev_cost_basis_sol = _safe_float(updated.get('partialCostBasisSol'), entry_sol * prev_sold_pct if entry_sol else 0.0)
    partial_cost_sol = entry_sol * sell_pct_delta if entry_sol else None

    updated['soldPct'] = total_sold_pct
    updated['remainingPct'] = max(0.0, 1.0 - total_sold_pct)
    updated['partialLockCount'] = _safe_int(updated.get('partialLockCount'), 0) + 1
    updated['lastPartialLockTs'] = _safe_int(ts, int(time.time()))
    updated['lastPartialLockPnl'] = _safe_float(trigger_pnl, None)
    updated['lastPartialLockPeak'] = _safe_float(peak_pnl, None)
    updated['lastPartialQuotePnl'] = _safe_float(quote_pnl, None)
    updated['lastPartialQuoteMarkGap'] = _safe_float(quote_mark_gap, None)
    if quote_sanity_status:
        updated['lastPartialQuoteSanity'] = quote_sanity_status

    if partial_out_sol is not None and partial_cost_sol is not None:
        realized_sol = prev_realized_sol + partial_out_sol
        cost_basis_sol = prev_cost_basis_sol + partial_cost_sol
        realized_pnl_on_sold = (
            (realized_sol - cost_basis_sol) / cost_basis_sol
            if cost_basis_sol and cost_basis_sol > 0 else None
        )
        realized_pnl_contribution = (
            (realized_sol - cost_basis_sol) / entry_sol
            if entry_sol and entry_sol > 0 else None
        )
        updated['partialRealizedSol'] = realized_sol
        updated['partialCostBasisSol'] = cost_basis_sol
        updated['totalSolReceived'] = realized_sol
        updated['partialRealizedPnlOnSold'] = realized_pnl_on_sold
        updated['partialRealizedPnlContribution'] = realized_pnl_contribution
        updated['remainingCostBasisSol'] = max(0.0, entry_sol - cost_basis_sol) if entry_sol else None

    history = updated.get('partialLockHistory')
    if not isinstance(history, list):
        history = []
    history.append({
        'ts': _safe_int(ts, int(time.time())),
        'reason': reason,
        'sellPct': sell_pct_delta,
        'soldPctAfter': total_sold_pct,
        'triggerPnl': _safe_float(trigger_pnl, None),
        'peakPnl': _safe_float(peak_pnl, None),
        'quotePnl': _safe_float(quote_pnl, None),
        'quoteMarkGap': _safe_float(quote_mark_gap, None),
        'quoteSanity': quote_sanity_status,
        'outSol': partial_out_sol,
        'costBasisSol': partial_cost_sol,
    })
    updated['partialLockHistory'] = history[-12:]
    return updated


def blended_mark_pnl_from_state(state, mark_pnl):
    monitor_state = state if isinstance(state, dict) else {}
    entry_sol = _safe_float(monitor_state.get('entrySol'), 0.0)
    sold_pct = max(0.0, min(1.0, _safe_float(monitor_state.get('soldPct'), 0.0)))
    partial_realized_sol = _safe_float(
        monitor_state.get('partialRealizedSol'),
        _safe_float(monitor_state.get('totalSolReceived'), 0.0),
    )
    partial_cost_basis_sol = _safe_float(monitor_state.get('partialCostBasisSol'), entry_sol * sold_pct if entry_sol else 0.0)
    mark_pnl = _safe_float(mark_pnl, None)
    if mark_pnl is None or entry_sol <= 0:
        return None
    partial_contribution = (partial_realized_sol - partial_cost_basis_sol) / entry_sol
    remaining_contribution = max(0.0, 1.0 - sold_pct) * mark_pnl
    return partial_contribution + remaining_contribution


def blended_quote_pnl_from_state(state, final_exit_sol):
    monitor_state = state if isinstance(state, dict) else {}
    entry_sol = _safe_float(monitor_state.get('entrySol'), 0.0)
    final_exit_sol = _safe_float(final_exit_sol, None)
    if entry_sol <= 0 or final_exit_sol is None:
        return None
    partial_realized_sol = _safe_float(
        monitor_state.get('partialRealizedSol'),
        _safe_float(monitor_state.get('totalSolReceived'), 0.0),
    )
    return ((partial_realized_sol + final_exit_sol) - entry_sol) / entry_sol


def quote_pnl_from_execution(execution, entry_price):
    if not isinstance(execution, dict):
        return None
    quote_price = _safe_float(execution.get('effectivePrice'), None)
    entry_price = _safe_float(entry_price, None)
    if quote_price is None or entry_price is None or entry_price <= 0:
        return None
    return (quote_price - entry_price) / entry_price


def record_trade_path_sample(db, pos, *, sample_ts, action, reason, mark_price, mark_pnl, mark_source, quote_execution=None, peak_pnl=None, phase_policy=None):
    execution = quote_execution if isinstance(quote_execution, dict) else {}
    quote_price = _safe_float(execution.get('effectivePrice'), None)
    quote_out_sol = _safe_float(execution.get('quotedOutAmount'), None)
    quote_pnl = None
    if quote_price is not None and quote_price > 0 and pos.entry_price and pos.entry_price > 0:
        quote_pnl = (quote_price - pos.entry_price) / pos.entry_price
    state = pos.monitor_state or {}
    payload = build_execution_audit(execution) if execution else {}
    if phase_policy:
        payload['phase_policy'] = phase_policy
    db.execute(
        """
        INSERT INTO paper_trade_path_samples
            (trade_id, lifecycle_id, token_ca, symbol, strategy_stage, sample_ts,
             action, reason, mark_price, mark_pnl, quote_price, quote_pnl,
             peak_pnl, sold_pct, token_amount_raw, mark_source, quote_success,
             quote_failure_reason, quote_out_sol, partial_realized_sol,
             remaining_cost_basis_sol, blended_mark_pnl, blended_quote_pnl,
             payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            pos.trade_id,
            pos.lifecycle_id,
            pos.token_ca,
            pos.symbol,
            pos.strategy_stage,
            _safe_int(sample_ts, int(time.time())),
            action,
            reason,
            _safe_float(mark_price, None),
            _safe_float(mark_pnl, None),
            quote_price,
            quote_pnl,
            _safe_float(peak_pnl if peak_pnl is not None else pos.peak_pnl, None),
            _safe_float(state.get('soldPct'), 0.0),
            str(pos.token_amount_raw) if pos.token_amount_raw is not None else None,
            mark_source,
            1 if execution.get('success') else 0 if execution else None,
            execution.get('failureReason'),
            quote_out_sol,
            _safe_float(state.get('partialRealizedSol'), _safe_float(state.get('totalSolReceived'), None)),
            _safe_float(state.get('remainingCostBasisSol'), None),
            blended_mark_pnl_from_state(state, mark_pnl),
            blended_quote_pnl_from_state(state, quote_out_sol) if action in ('exit', 'close') else None,
            json.dumps(payload) if payload else None,
        ),
    )


def record_lotto_probe_shadow_candidates(db, *, now_ts, limit=40):
    """Record missed LOTTO rows that would qualify for a tiny second-pass probe.

    This is intentionally attribution-only. It creates audit events that let us
    compare "would probe" candidates against the existing missed-dog outcomes
    before changing entry behavior.
    """
    if not LOTTO_PROBE_SHADOW_ENABLED:
        return 0
    rows = db.execute(
        """
        SELECT
            m.id, m.token_ca, m.symbol, m.lifecycle_id, m.signal_id, m.signal_ts,
            m.route, m.component, m.reject_reason, m.baseline_price, m.pnl_5m,
            m.pnl_15m, m.pnl_60m, m.max_pnl_recorded, m.lifecycle_state,
            m.vitality_score, m.entry_bias
        FROM paper_missed_signal_attribution m
        WHERE m.route = 'LOTTO'
          AND m.baseline_price IS NOT NULL
          AND m.pnl_5m IS NOT NULL
          AND m.pnl_5m >= ?
          AND m.created_event_ts >= ?
          AND m.component IN ('upstream_gate', 'lotto_entry_gate')
          AND NOT EXISTS (
              SELECT 1
              FROM paper_decision_events e
              WHERE e.component = 'lotto_probe_shadow'
                AND e.token_ca = m.token_ca
                AND COALESCE(e.signal_ts, 0) = COALESCE(m.signal_ts, 0)
                AND e.reason = m.reject_reason
                AND e.payload_json LIKE '%"source_component": "' || m.component || '"%'
          )
        ORDER BY m.pnl_5m DESC, COALESCE(m.max_pnl_recorded, m.pnl_5m) DESC
        LIMIT ?
        """,
        (LOTTO_PROBE_SHADOW_MIN_5M_PNL, now_ts - 2 * 60 * 60, limit),
    ).fetchall()
    recorded = 0
    for row in rows:
        payload = {
            'missed_attribution_id': row['id'],
            'probe_trigger': f"missed_reaccelerated_5m_{LOTTO_PROBE_SHADOW_MIN_5M_PNL:.0%}",
            'source_component': row['component'],
            'source_reject_reason': row['reject_reason'],
            'baseline_price': row['baseline_price'],
            'pnl_5m': row['pnl_5m'],
            'pnl_15m': row['pnl_15m'],
            'pnl_60m': row['pnl_60m'],
            'max_pnl_recorded': row['max_pnl_recorded'],
            'suggested_position_size_sol': LOTTO_PROBE_SHADOW_SIZE_SOL,
            'probe_mode': 'shadow_only',
            'lifecycle': {
                'lifecycle_state': row['lifecycle_state'],
                'vitality_score': row['vitality_score'],
                'entry_bias': row['entry_bias'],
            },
        }
        record_decision_event(
            db,
            component='lotto_probe_shadow',
            event_type='probe_candidate',
            decision='PROBE_SHADOW',
            reason=row['reject_reason'],
            token_ca=row['token_ca'],
            symbol=row['symbol'],
            lifecycle_id=row['lifecycle_id'],
            signal_ts=row['signal_ts'],
            signal_id=row['signal_id'],
            route='LOTTO',
            data_source='missed_attribution',
            payload=payload,
            event_ts=now_ts,
        )
        recorded += 1
    return recorded


def record_explosive_continuation_shadow_candidates(db, *, now_ts, limit=40):
    """Shadow-track chasing_top misses without creating a live entry path."""
    if not EXPLOSIVE_CONTINUATION_SHADOW_ENABLED:
        return 0
    rows = db.execute(
        """
        SELECT
            m.id, m.token_ca, m.symbol, m.lifecycle_id, m.signal_id, m.signal_ts,
            m.route, m.component, m.reject_reason, m.baseline_price, m.pnl_5m,
            m.pnl_15m, m.pnl_60m, m.max_pnl_recorded, m.lifecycle_state,
            m.vitality_score, m.entry_bias, m.created_event_ts
        FROM paper_missed_signal_attribution m
        WHERE m.route = 'LOTTO'
          AND m.component = 'smart_entry'
          AND m.reject_reason = 'chasing_top'
          AND m.created_event_ts >= ?
          AND NOT EXISTS (
              SELECT 1
              FROM paper_decision_events e
              WHERE e.component = 'explosive_continuation_shadow'
                AND e.token_ca = m.token_ca
                AND COALESCE(e.signal_ts, 0) = COALESCE(m.signal_ts, 0)
          )
        ORDER BY COALESCE(m.max_pnl_recorded, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) DESC,
                 m.created_event_ts DESC
        LIMIT ?
        """,
        (now_ts - EXPLOSIVE_CONTINUATION_SHADOW_LOOKBACK_SEC, limit),
    ).fetchall()
    recorded = 0
    for row in rows:
        payload = {
            'missed_attribution_id': row['id'],
            'probe_trigger': 'chasing_top_explosive_continuation_shadow',
            'source_component': row['component'],
            'source_reject_reason': row['reject_reason'],
            'baseline_price': row['baseline_price'],
            'pnl_5m': row['pnl_5m'],
            'pnl_15m': row['pnl_15m'],
            'pnl_60m': row['pnl_60m'],
            'max_pnl_recorded': row['max_pnl_recorded'],
            'probe_mode': 'shadow_only',
            'live_entry_enabled': False,
            'lifecycle': {
                'lifecycle_state': row['lifecycle_state'],
                'vitality_score': row['vitality_score'],
                'entry_bias': row['entry_bias'],
            },
        }
        record_decision_event(
            db,
            component='explosive_continuation_shadow',
            event_type='shadow_candidate',
            decision='SHADOW_ONLY',
            reason='chasing_top',
            token_ca=row['token_ca'],
            symbol=row['symbol'],
            lifecycle_id=row['lifecycle_id'],
            signal_ts=row['signal_ts'],
            signal_id=row['signal_id'],
            route='LOTTO',
            data_source='missed_attribution',
            payload=payload,
            event_ts=now_ts,
        )
        recorded += 1
    return recorded


def normalize_signal_ts_seconds(value):
    if value is None:
        return None
    try:
        ts = int(float(value))
    except (TypeError, ValueError):
        return None
    if ts > 1_000_000_000_000:
        return ts // 1000
    return ts


def calculate_entry_spread_pct(trigger_price, quote_price):
    try:
        trigger = float(trigger_price or 0)
        quote = float(quote_price or 0)
    except (TypeError, ValueError):
        return None
    if trigger <= 0 or quote <= 0:
        return None
    return (quote - trigger) / trigger * 100.0


def pending_is_paper_tiny_scout(pending):
    pending = pending or {}
    lotto_state = pending.get('lotto_state') or {}
    entry_decision = lotto_state.get('entryDecision') or {}
    entry_mode = str(
        pending.get('entry_mode')
        or pending.get('scout_mode')
        or entry_decision.get('entry_mode')
        or ''
    )
    try:
        pending_size_sol = float(
            pending.get('kelly_position_sol')
            or lotto_state.get('positionSizeSol')
            or entry_decision.get('position_size_sol')
            or 0.0
        )
    except (TypeError, ValueError):
        pending_size_sol = 0.0
    return (
        entry_mode in PAPER_TINY_SCOUT_ENTRY_MODES
        or (
            bool(entry_decision.get('paper_only_scout') or pending.get('paper_only_scout'))
            and pending_size_sol <= 0.005
        )
    )


def apply_paper_tiny_scout_size_cap(pending):
    pending = pending or {}
    if not pending_is_paper_tiny_scout(pending):
        return {
            'is_tiny_scout': False,
            'requested_size_sol': None,
            'actual_size_sol': None,
            'capped': False,
            'cap_sol': min(PAPER_TINY_SCOUT_SIZE_SOL, SCOUT_QUALITY_SIZE_CAP_SOL),
        }
    lotto_state = pending.get('lotto_state') or {}
    entry_decision = lotto_state.get('entryDecision') or {}
    requested_size = _safe_float(
        pending.get('kelly_position_sol')
        or lotto_state.get('positionSizeSol')
        or entry_decision.get('position_size_sol')
        or PAPER_TINY_SCOUT_SIZE_SOL,
        PAPER_TINY_SCOUT_SIZE_SOL,
    )
    cap_sol = min(PAPER_TINY_SCOUT_SIZE_SOL, SCOUT_QUALITY_SIZE_CAP_SOL)
    actual_size = min(requested_size, cap_sol)
    pending['kelly_position_sol'] = actual_size
    pending['paper_only_scout'] = True
    if isinstance(lotto_state, dict):
        lotto_state['positionSizeSol'] = actual_size
        lotto_state['paper_only_scout'] = True
        if isinstance(entry_decision, dict):
            entry_decision['position_size_sol'] = actual_size
            entry_decision['paper_only_scout'] = True
    return {
        'is_tiny_scout': True,
        'entry_mode': pending.get('entry_mode') or pending.get('scout_mode') or entry_decision.get('entry_mode'),
        'requested_size_sol': requested_size,
        'actual_size_sol': actual_size,
        'capped': actual_size < requested_size,
        'cap_sol': cap_sol,
    }


def _score_int(scores, key, default=0):
    try:
        return int(float((scores or {}).get(key, default) or default))
    except (TypeError, ValueError):
        return default


def _paper_trade_columns(db):
    if db is None:
        return set()
    try:
        rows = db.execute("PRAGMA table_info(paper_trades)").fetchall()
    except Exception:
        return set()
    columns = set()
    for row in rows:
        try:
            columns.add(row['name'])
        except Exception:
            try:
                columns.add(row[1])
            except Exception:
                continue
    return columns


def _row_to_dict(row, keys):
    if row is None:
        return {}
    try:
        return {key: row[key] for key in keys}
    except Exception:
        return {key: value for key, value in zip(keys, row)}


def _ath_no_kline_recent_trades(db, token_ca, *, now_ts=None, lookback_sec=None, limit=20):
    if not token_ca:
        return []
    columns = _paper_trade_columns(db)
    if not {'token_ca', 'entry_mode'}.issubset(columns):
        return []
    lookback_sec = ATH_NO_KLINE_REENTRY_LOOKBACK_SEC if lookback_sec is None else lookback_sec
    now_ts = int(now_ts or time.time())
    cutoff_ts = now_ts - int(lookback_sec)
    keys = [
        'id',
        'entry_ts',
        'exit_ts',
        'exit_reason',
        'pnl_pct',
        'peak_pnl',
        'entry_price',
        'exit_price',
        'entry_mode',
    ]
    select_exprs = [f"{key} AS {key}" if key in columns else f"NULL AS {key}" for key in keys]
    timestamp_expr = "COALESCE(exit_ts, entry_ts, 0)" if 'exit_ts' in columns and 'entry_ts' in columns else "entry_ts"
    order_expr = f"{timestamp_expr} DESC"
    if 'id' in columns:
        order_expr += ", id DESC"
    try:
        placeholders = ','.join(['?'] * len(ATH_OUTCOME_GUARD_ENTRY_MODES))
        rows = db.execute(
            f"""
            SELECT {', '.join(select_exprs)}
            FROM paper_trades
            WHERE token_ca = ?
              AND entry_mode IN ({placeholders})
              AND {timestamp_expr} >= ?
            ORDER BY {order_expr}
            LIMIT ?
            """,
            (token_ca, *sorted(ATH_OUTCOME_GUARD_ENTRY_MODES), cutoff_ts, int(limit)),
        ).fetchall()
    except Exception:
        return []
    return [_row_to_dict(row, keys) for row in rows]


def _ath_no_kline_matrix_strong(scores):
    trend = _score_int(scores, 'trend')
    price = _score_int(scores, 'price')
    signal = _score_int(scores, 'signal')
    return {
        'pass': (
            trend >= ATH_NO_KLINE_REENTRY_MIN_T
            and price >= ATH_NO_KLINE_REENTRY_MIN_P
            and signal >= ATH_NO_KLINE_REENTRY_MIN_S
        ),
        'scores': {
            'trend': trend,
            'volume': _score_int(scores, 'volume'),
            'price': price,
            'signal': signal,
            'momentum': _score_int(scores, 'momentum'),
        },
        'thresholds': {
            'trend': ATH_NO_KLINE_REENTRY_MIN_T,
            'price': ATH_NO_KLINE_REENTRY_MIN_P,
            'signal': ATH_NO_KLINE_REENTRY_MIN_S,
        },
    }


def _pnl_decimal(value):
    pnl = _safe_float(value, None)
    if pnl is None:
        return None
    if abs(pnl) > 3.0:
        return pnl / 100.0
    return pnl


def _ath_no_kline_followthrough_guard(pending, dex_snapshot):
    pending = pending or {}
    entry_mode = str(pending.get('entry_mode') or pending.get('entry_trigger_mode') or pending.get('scout_mode') or '')
    if not ATH_NO_KLINE_FOLLOWTHROUGH_GUARD_ENABLED:
        return {'pass': True, 'reason': 'ath_no_kline_followthrough_guard_disabled'}
    if entry_mode != ATH_NO_KLINE_TINY_PROBE_MODE:
        return {'pass': True, 'reason': 'not_ath_no_kline_tiny_probe', 'entry_mode': entry_mode}
    trend = dex_snapshot or {}
    buys_m5 = _safe_float(trend.get('buys_m5'), None)
    sells_m5 = _safe_float(trend.get('sells_m5'), None)
    bs_ratio = _safe_float(trend.get('buy_sell_ratio'), None)
    if bs_ratio is None and buys_m5 is not None:
        bs_ratio = buys_m5 / max(sells_m5 or 0.0, 1.0)
    tx_m5 = _safe_float(trend.get('tx_m5'), None)
    if tx_m5 is None and buys_m5 is not None and sells_m5 is not None:
        tx_m5 = buys_m5 + sells_m5
    pc_m5 = _safe_float(trend.get('price_change_m5'), None)
    matrix = _ath_no_kline_matrix_strong(pending.get('matrix_scores') or {})
    observed = {
        'buy_sell_ratio': bs_ratio,
        'tx_m5': tx_m5,
        'price_change_m5': pc_m5,
        'scores': matrix.get('scores'),
    }
    thresholds = {
        'buy_sell_ratio': ATH_NO_KLINE_FOLLOWTHROUGH_MIN_BS,
        'tx_m5': ATH_NO_KLINE_FOLLOWTHROUGH_MIN_TX_M5,
        'price_change_m5': ATH_NO_KLINE_FOLLOWTHROUGH_MIN_PC_M5,
        'strong_price_change_m5': ATH_NO_KLINE_FOLLOWTHROUGH_STRONG_PC_M5,
        'matrix': matrix.get('thresholds'),
    }
    if bs_ratio is None or bs_ratio < ATH_NO_KLINE_FOLLOWTHROUGH_MIN_BS:
        return {
            'pass': False,
            'reason': 'ath_no_kline_followthrough_buy_pressure_weak',
            'entry_mode': entry_mode,
            'observed': observed,
            'thresholds': thresholds,
        }
    if pc_m5 is not None and pc_m5 < ATH_NO_KLINE_FOLLOWTHROUGH_MIN_PC_M5:
        return {
            'pass': False,
            'reason': 'ath_no_kline_followthrough_negative_m5',
            'entry_mode': entry_mode,
            'observed': observed,
            'thresholds': thresholds,
        }
    tx_ok = tx_m5 is not None and tx_m5 >= ATH_NO_KLINE_FOLLOWTHROUGH_MIN_TX_M5
    pc_strong = pc_m5 is not None and pc_m5 >= ATH_NO_KLINE_FOLLOWTHROUGH_STRONG_PC_M5
    matrix_strong = bool(matrix.get('pass'))
    if not (tx_ok or pc_strong):
        return {
            'pass': False,
            'reason': 'ath_no_kline_no_followthrough_block',
            'entry_mode': entry_mode,
            'observed': observed,
            'thresholds': thresholds,
            'checks': {'tx_ok': tx_ok, 'pc_strong': pc_strong, 'matrix_strong': matrix_strong},
        }
    return {
        'pass': True,
        'reason': 'ath_no_kline_followthrough_confirmed',
        'entry_mode': entry_mode,
        'observed': observed,
        'thresholds': thresholds,
        'checks': {'tx_ok': tx_ok, 'pc_strong': pc_strong, 'matrix_strong': matrix_strong},
    }


def _ath_no_kline_reentry_guard(db, pending, *, current_price=None, now_ts=None, recent_trades=None):
    pending = pending or {}
    entry_mode = str(pending.get('entry_mode') or pending.get('entry_trigger_mode') or pending.get('scout_mode') or '')
    if not ATH_NO_KLINE_REENTRY_GUARD_ENABLED:
        return {'pass': True, 'reason': 'ath_no_kline_reentry_guard_disabled'}
    if entry_mode not in ATH_OUTCOME_GUARD_ENTRY_MODES:
        return {'pass': True, 'reason': 'not_ath_outcome_guard_mode', 'entry_mode': entry_mode}

    now_ts = int(now_ts or time.time())
    trades = list(recent_trades) if recent_trades is not None else _ath_no_kline_recent_trades(
        db,
        pending.get('token_ca'),
        now_ts=now_ts,
    )
    matrix = _ath_no_kline_matrix_strong(pending.get('matrix_scores') or {})
    thresholds = {
        'lookback_sec': ATH_NO_KLINE_REENTRY_LOOKBACK_SEC,
        'hard_loss_cooldown_sec': ATH_NO_KLINE_REENTRY_HARD_LOSS_COOLDOWN_SEC,
        'hard_loss_pnl': ATH_NO_KLINE_REENTRY_HARD_LOSS_PNL,
        'low_follow_peak': ATH_NO_KLINE_REENTRY_LOW_FOLLOW_PEAK,
        'min_recovery_pct': ATH_NO_KLINE_REENTRY_MIN_RECOVERY_PCT,
        'max_recent_entries': ATH_NO_KLINE_REENTRY_MAX_RECENT_ENTRIES,
        'winner_peak': ATH_NO_KLINE_REENTRY_WINNER_PEAK,
        'matrix': matrix.get('thresholds'),
    }
    detail = {
        'pass': True,
        'reason': 'ath_no_kline_first_entry',
        'entry_mode': entry_mode,
        'recent_trade_count': len(trades),
        'scores': matrix.get('scores'),
        'thresholds': thresholds,
    }
    if not trades:
        return detail

    latest = trades[0] or {}
    latest_pnl = _pnl_decimal(latest.get('pnl_pct'))
    latest_peak = _pnl_decimal(latest.get('peak_pnl')) or 0.0
    latest_ts = _safe_float(latest.get('exit_ts') or latest.get('entry_ts'), 0.0)
    latest_ref_price = _safe_float(latest.get('exit_price') or latest.get('entry_price'), None)
    current_price = _safe_float(current_price or pending.get('trigger_price') or pending.get('entry_price'), None)
    detail['latest_trade'] = {
        'id': latest.get('id'),
        'entry_ts': latest.get('entry_ts'),
        'exit_ts': latest.get('exit_ts'),
        'exit_reason': latest.get('exit_reason'),
        'pnl_pct': latest_pnl,
        'peak_pnl': latest_peak,
        'entry_price': _safe_float(latest.get('entry_price'), None),
        'exit_price': _safe_float(latest.get('exit_price'), None),
        'entry_mode': latest.get('entry_mode'),
    }
    detail['observed'] = {
        'current_price': current_price,
        'reference_price': latest_ref_price,
        'recovery_pct': (
            ((current_price / latest_ref_price) - 1.0) * 100.0
            if current_price is not None and latest_ref_price and latest_ref_price > 0
            else None
        ),
    }

    if len(trades) >= ATH_NO_KLINE_REENTRY_MAX_RECENT_ENTRIES:
        detail.update({'pass': False, 'reason': 'ath_no_kline_reentry_max_recent_entries'})
        return detail

    hard_loss = (
        latest_pnl is not None
        and latest_pnl <= ATH_NO_KLINE_REENTRY_HARD_LOSS_PNL
        and (now_ts - latest_ts) < ATH_NO_KLINE_REENTRY_HARD_LOSS_COOLDOWN_SEC
    )
    hard_sl_exit = 'hard_sl' in str(latest.get('exit_reason') or '').lower()
    hard_sl_low_peak = hard_sl_exit and latest_peak < ATH_NO_KLINE_REENTRY_HARD_SL_LOW_PEAK
    if hard_loss or (hard_sl_low_peak and (now_ts - latest_ts) < ATH_NO_KLINE_REENTRY_HARD_LOSS_COOLDOWN_SEC):
        detail.update({'pass': False, 'reason': 'ath_no_kline_reentry_hard_loss_cooldown'})
        return detail

    if latest_peak < ATH_NO_KLINE_REENTRY_LOW_FOLLOW_PEAK and (latest_pnl is None or latest_pnl <= 0):
        detail.update({'pass': False, 'reason': 'ath_no_kline_reentry_low_followthrough'})
        return detail

    if len(trades) >= 2 and not (latest_peak >= ATH_NO_KLINE_REENTRY_WINNER_PEAK and latest_pnl is not None and latest_pnl > 0):
        detail.update({'pass': False, 'reason': 'ath_no_kline_reentry_needs_prior_winner'})
        return detail

    if not matrix.get('pass'):
        detail.update({'pass': False, 'reason': 'ath_no_kline_reentry_matrix_not_strong'})
        return detail

    if not latest_ref_price or latest_ref_price <= 0 or current_price is None:
        detail.update({'pass': False, 'reason': 'ath_no_kline_reentry_price_reference_missing'})
        return detail

    min_recovery = ATH_NO_KLINE_REENTRY_MIN_RECOVERY_PCT
    recovery_pct = detail['observed']['recovery_pct']
    if recovery_pct is None or recovery_pct < min_recovery:
        detail.update({'pass': False, 'reason': 'ath_no_kline_reentry_recovery_not_confirmed'})
        return detail

    detail.update({'pass': True, 'reason': 'ath_no_kline_reentry_allowed'})
    return detail


def _ath_reentry_block_cooldown_sec(reason):
    reason = str(reason or '')
    if reason == 'ath_no_kline_reentry_hard_loss_cooldown':
        return ATH_NO_KLINE_REENTRY_HARD_LOSS_COOLDOWN_SEC
    if reason == 'ath_no_kline_reentry_max_recent_entries':
        return ATH_NO_KLINE_REENTRY_LOOKBACK_SEC
    if reason in {
        'ath_no_kline_reentry_low_followthrough',
        'ath_no_kline_reentry_needs_prior_winner',
        'ath_no_kline_reentry_recovery_not_confirmed',
    }:
        return ATH_REENTRY_LOW_FOLLOW_BLOCK_SEC
    if reason in {
        'ath_no_kline_reentry_matrix_not_strong',
        'ath_no_kline_reentry_price_reference_missing',
    }:
        return ATH_REENTRY_MATRIX_BLOCK_SEC
    return ATH_REENTRY_MATRIX_BLOCK_SEC


def _defer_ath_reentry_block(watchlist, watchlist_entry, guard_detail):
    if not watchlist or not watchlist_entry or not guard_detail:
        return None
    if guard_detail.get('pass'):
        return None
    entry_id = watchlist_entry.get('id')
    if not entry_id:
        return None
    reason = guard_detail.get('reason') or 'ath_reentry_block'
    cooldown_sec = _ath_reentry_block_cooldown_sec(reason)
    try:
        until = watchlist.defer_fire(entry_id, reason, cooldown_sec=cooldown_sec)
        watchlist_entry['fire_block_until'] = until
        watchlist_entry['fire_block_reason'] = reason
        return {
            'pass': False,
            'reason': reason,
            'cooldown_sec': cooldown_sec,
            'fire_block_until': until,
            'watchlist_id': entry_id,
        }
    except Exception as exc:
        return {
            'pass': False,
            'reason': 'ath_reentry_block_defer_failed',
            'error': str(exc),
            'original_reason': reason,
            'watchlist_id': entry_id,
        }


def _pending_watchlist_fire_block_detail(watchlist, pending, *, now_ts=None):
    pending = pending or {}
    now_ts = float(now_ts or time.time())
    entry = pending.get('w_entry') or {}
    watchlist_id = pending.get('watchlist_id') or entry.get('id')
    latest = entry
    if watchlist and watchlist_id:
        try:
            latest = watchlist.get_by_id(watchlist_id) or latest
        except Exception:
            latest = entry
    block_until = _safe_float((latest or {}).get('fire_block_until'), 0.0)
    reason = str((latest or {}).get('fire_block_reason') or '')
    if block_until and block_until > now_ts:
        return {
            'pass': False,
            'reason': reason or 'watchlist_fire_block_active',
            'remaining_sec': max(0, int(block_until - now_ts)),
            'fire_block_until': block_until,
            'watchlist_id': watchlist_id,
        }
    return {
        'pass': True,
        'reason': 'no_watchlist_fire_block',
        'watchlist_id': watchlist_id,
    }


def _select_structure_stop_loss(current_sl, structure_sl, pending=None):
    current_sl = float(current_sl)
    structure_sl = float(structure_sl)
    pending = pending or {}
    if pending_is_paper_tiny_scout(pending):
        return max(current_sl, structure_sl), 'tiny_probe_tight_structure_sl'
    return min(current_sl, structure_sl), 'primary_wide_structure_sl'


def _ath_no_kline_scout_quality_soft_override(pending, scout_quality, *, route=None, scout_size=None):
    if not ATH_NO_KLINE_VOLUME_LOW_WARN_ENABLED or not isinstance(scout_quality, dict):
        return scout_quality
    if scout_quality.get('pass') or scout_quality.get('reason') != 'scout_quality_volume_low':
        return scout_quality
    pending = pending or {}
    entry_mode = str(pending.get('entry_mode') or pending.get('entry_trigger_mode') or pending.get('scout_mode') or '')
    route_label = str(route or pending.get('signal_route') or pending.get('signal_type') or '').upper()
    if entry_mode != ATH_NO_KLINE_TINY_PROBE_MODE or route_label != 'ATH':
        return scout_quality

    observed = scout_quality.get('observed') or {}
    thresholds = scout_quality.get('thresholds') or {}
    scores = pending.get('matrix_scores') or {}
    score_detail = {
        'trend': _score_int(scores, 'trend'),
        'volume': _score_int(scores, 'volume'),
        'price': _score_int(scores, 'price'),
        'signal': _score_int(scores, 'signal'),
        'momentum': _score_int(scores, 'momentum'),
    }
    matrix_strong = (
        score_detail['trend'] >= 60
        and score_detail['price'] >= ENTRY_MODE_QUALITY_OVERRIDE_MIN_P
        and score_detail['signal'] >= ENTRY_MODE_QUALITY_OVERRIDE_MIN_S
    )
    size_detail = scout_size or {}
    actual_size = _safe_float(
        size_detail.get('actual_size_sol') if isinstance(size_detail, dict) else None,
        _safe_float(pending.get('kelly_position_sol'), None),
    )
    tiny_size_ok = actual_size is not None and actual_size <= min(PAPER_TINY_SCOUT_SIZE_SOL, SCOUT_QUALITY_SIZE_CAP_SOL) + 1e-9
    min_bs = max(
        _safe_float(thresholds.get('min_buy_sell_ratio'), 0.0),
        ATH_NO_KLINE_VOLUME_LOW_WARN_MIN_BS,
    )
    bs = _safe_float(observed.get('buy_sell_ratio'), None)
    tx = _safe_float(observed.get('tx_m5'), None)
    min_tx = _safe_float(thresholds.get('min_tx_m5'), 0.0)
    price_change = _safe_float(observed.get('price_change_m5'), None)
    max_negative = _safe_float(thresholds.get('max_negative_m5'), None)
    top1 = _safe_float(observed.get('top1_pct'), None)
    max_top1 = _safe_float(thresholds.get('max_top1_pct'), None)
    top10 = _safe_float(observed.get('top10_pct'), None)
    max_top10 = _safe_float(thresholds.get('max_top10_pct'), None)
    liquidity = _safe_float(observed.get('liquidity_usd'), None)
    min_liquidity = _safe_float(thresholds.get('min_liquidity_usd'), 0.0)
    checks = {
        'matrix_strong': matrix_strong,
        'tiny_size_ok': tiny_size_ok,
        'buy_pressure_ok': bs is not None and bs >= min_bs,
        'tx_ok': tx is not None and tx >= min_tx,
        'negative_trend_ok': price_change is None or max_negative is None or price_change >= max_negative,
        'top1_ok': top1 is None or max_top1 is None or top1 <= max_top1,
        'top10_ok': top10 is None or max_top10 is None or top10 <= max_top10,
        'liquidity_ok': liquidity is None or min_liquidity <= 0 or liquidity >= min_liquidity,
    }
    if not all(checks.values()):
        override = dict(scout_quality)
        override['volume_low_soft_override'] = {
            'pass': False,
            'reason': 'ath_no_kline_volume_low_soft_override_checks_failed',
            'checks': checks,
            'scores': score_detail,
            'observed': observed,
            'thresholds': {
                'trend': 60,
                'price': ENTRY_MODE_QUALITY_OVERRIDE_MIN_P,
                'signal': ENTRY_MODE_QUALITY_OVERRIDE_MIN_S,
                'buy_sell_ratio': min_bs,
                'tx_m5': min_tx,
                'max_negative_m5': max_negative,
                'max_top1_pct': max_top1,
                'max_top10_pct': max_top10,
            },
        }
        return override

    override = dict(scout_quality)
    override.update({
        'pass': True,
        'decision': 'warn',
        'reason': 'scout_quality_volume_low_warn_ath_no_kline_override',
        'original_reason': scout_quality.get('reason'),
        'volume_low_soft_override': {
            'pass': True,
            'reason': 'ath_no_kline_volume_low_soft_warn',
            'checks': checks,
            'scores': score_detail,
            'observed': observed,
            'thresholds': {
                'trend': 60,
                'price': ENTRY_MODE_QUALITY_OVERRIDE_MIN_P,
                'signal': ENTRY_MODE_QUALITY_OVERRIDE_MIN_S,
                'buy_sell_ratio': min_bs,
                'tx_m5': min_tx,
                'max_negative_m5': max_negative,
                'max_top1_pct': max_top1,
                'max_top10_pct': max_top10,
            },
        },
    })
    return override


ATH_RECOVERY_TINY_PROBE_MODES = {
    ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE,
    ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
    ATH_MICRO_RECLAIM_TINY_PROBE_MODE,
}
ATH_MICRO_RECLAIM_SOURCE_REASONS = {
    'scout_quality_negative_trend',
    'scout_quality_buy_pressure_weak',
    'scout_quality_volume_low',
    'ath_uncertainty_mc_shadow_only',
    'ath_uncertainty_mc_gate',
    'discovery_ath_mc_shadow_only',
    'discovery_ath_mc_gate',
}


def _ath_recovery_family(entry_mode):
    mode = str(entry_mode or '')
    if mode == ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE:
        return 'recent_failure_reclaim'
    if mode == ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE:
        return 'matrix_dissonance'
    if mode == ATH_MICRO_RECLAIM_TINY_PROBE_MODE:
        return 'micro_reclaim'
    return None


def _ath_recovery_source_reason(candidate=None, source_detail=None):
    candidate = candidate or {}
    source_detail = source_detail or candidate.get('source_detail') or {}
    scout_quality = source_detail.get('scout_quality') if isinstance(source_detail.get('scout_quality'), dict) else {}
    reasons = [
        source_detail.get('ath_uncertainty_reject_reason'),
        source_detail.get('pending_entry_quality_reject_reason'),
        source_detail.get('upstream_realtime_reject_reason'),
        scout_quality.get('reason'),
        candidate.get('source_reject_reason'),
    ]
    for reason in reasons:
        text = str(reason or '').strip()
        if text:
            return text
    return ''


def _ath_recovery_parent_reason(candidate=None, source_detail=None):
    candidate = candidate or {}
    source_detail = source_detail or candidate.get('source_detail') or {}
    return str(
        source_detail.get('source_reject_reason')
        or candidate.get('source_reject_reason')
        or _ath_recovery_source_reason(candidate, source_detail)
        or ''
    )


def _ath_recovery_mode_for_reason(reason, *, parent_reason=None):
    reason = str(reason or '')
    parent_reason = str(parent_reason or '')
    if reason == 'scout_quality_recent_token_failure':
        return ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE
    if reason in ATH_MICRO_RECLAIM_SOURCE_REASONS or parent_reason in ATH_MICRO_RECLAIM_SOURCE_REASONS:
        return ATH_MICRO_RECLAIM_TINY_PROBE_MODE
    if (
        reason == 'matrices not yet aligned'
        or parent_reason == 'matrices not yet aligned'
        or _matrix_micro_momentum_reason(reason)
        or _matrix_micro_momentum_reason(parent_reason)
    ):
        return ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE
    return None


def _ath_recovery_mode_for_candidate(mode, *, route=None, source_reject_reason=None, source_detail=None):
    if str(route or '').upper() != 'ATH':
        return mode
    if mode == ATH_NO_KLINE_TINY_PROBE_MODE:
        return mode
    source_detail = source_detail or {}
    reason = _ath_recovery_source_reason(
        {'source_reject_reason': source_reject_reason, 'source_detail': source_detail},
        source_detail,
    )
    parent = source_reject_reason or _ath_recovery_parent_reason(
        {'source_reject_reason': source_reject_reason, 'source_detail': source_detail},
        source_detail,
    )
    recovery_mode = _ath_recovery_mode_for_reason(reason, parent_reason=parent)
    if recovery_mode == ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE:
        scores = _ath_recovery_scores(
            {'source_reject_reason': source_reject_reason, 'source_detail': source_detail},
            {'source_detail': source_detail},
        )
        if not _ath_reclaim_after_failure_matrix_detail(scores).get('pass'):
            return mode
    if recovery_mode == ATH_MICRO_RECLAIM_TINY_PROBE_MODE:
        scores = _ath_recovery_scores(
            {'source_reject_reason': source_reject_reason, 'source_detail': source_detail},
            {'source_detail': source_detail},
        )
        if not _ath_recovery_matrix_detail(scores).get('pass'):
            return mode
    if recovery_mode == ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE:
        scores = _ath_recovery_scores(
            {'source_reject_reason': source_reject_reason, 'source_detail': source_detail},
            {'source_detail': source_detail},
        )
        if not _ath_recovery_matrix_detail(scores, matrix_dissonance=True).get('pass'):
            return mode
    return recovery_mode or mode


def _ath_recovery_scores(candidate=None, detail=None):
    candidate = candidate or {}
    detail = detail or {}
    source_detail = detail.get('source_detail') or candidate.get('source_detail') or {}
    candidates = [
        detail.get('scores'),
        source_detail.get('scores'),
        (source_detail.get('source_detail') or {}).get('scores') if isinstance(source_detail.get('source_detail'), dict) else None,
        candidate.get('matrix_scores'),
        detail.get('matrix_scores'),
    ]
    for scores in candidates:
        if isinstance(scores, dict) and scores:
            return scores
    return {}


def _ath_recovery_matrix_detail(scores, *, min_t=None, min_p=None, min_s=None, matrix_dissonance=False):
    min_t = ATH_RECOVERY_MIN_T if min_t is None else min_t
    min_p = ATH_RECOVERY_MIN_P if min_p is None else min_p
    min_s = ATH_RECOVERY_MIN_S if min_s is None else min_s
    observed = {
        'trend': _score_int(scores, 'trend'),
        'volume': _score_int(scores, 'volume'),
        'price': _score_int(scores, 'price'),
        'signal': _score_int(scores, 'signal'),
        'momentum': _score_int(scores, 'momentum'),
    }
    if matrix_dissonance:
        passed = (
            observed['trend'] >= ATH_MATRIX_DISSONANCE_MIN_T
            and (observed['price'] >= 100 or observed['signal'] >= 100)
        )
        thresholds = {
            'trend': ATH_MATRIX_DISSONANCE_MIN_T,
            'price_or_signal': 100,
        }
    else:
        passed = (
            observed['trend'] >= min_t
            and observed['price'] >= min_p
            and observed['signal'] >= min_s
        )
        thresholds = {
            'trend': min_t,
            'price': min_p,
            'signal': min_s,
        }
    return {
        'pass': passed,
        'observed': observed,
        'thresholds': thresholds,
    }


def _ath_reclaim_after_failure_matrix_detail(scores):
    return _ath_recovery_matrix_detail(
        scores,
        min_t=ATH_RECLAIM_AFTER_FAILURE_MIN_T,
        min_p=ATH_RECLAIM_AFTER_FAILURE_MIN_P,
        min_s=ATH_RECLAIM_AFTER_FAILURE_MIN_S,
    )


def _ath_recovery_recent_trades(db, token_ca, *, now_ts=None, lookback_sec=None, limit=20):
    if not token_ca:
        return []
    columns = _paper_trade_columns(db)
    if not {'token_ca', 'entry_ts'}.issubset(columns):
        return []
    now_ts = int(now_ts or time.time())
    lookback_sec = ATH_RECOVERY_HARD_LOSS_COOLDOWN_SEC if lookback_sec is None else lookback_sec
    cutoff_ts = now_ts - int(lookback_sec)
    keys = [
        'id',
        'entry_mode',
        'entry_ts',
        'exit_ts',
        'exit_reason',
        'pnl_pct',
        'peak_pnl',
        'entry_price',
        'exit_price',
    ]
    select_exprs = [f"{key} AS {key}" if key in columns else f"NULL AS {key}" for key in keys]
    timestamp_expr = "COALESCE(exit_ts, entry_ts, 0)" if 'exit_ts' in columns else "entry_ts"
    try:
        rows = db.execute(
            f"""
            SELECT {', '.join(select_exprs)}
            FROM paper_trades
            WHERE token_ca = ?
              AND {timestamp_expr} >= ?
            ORDER BY {timestamp_expr} DESC
            LIMIT ?
            """,
            (token_ca, cutoff_ts, int(limit)),
        ).fetchall()
    except Exception:
        return []
    return [_row_to_dict(row, keys) for row in rows]


def _ath_recovery_cooldown_detail(db, token_ca, entry_mode, *, now_ts=None):
    now_ts = float(now_ts or time.time())
    trades = _ath_recovery_recent_trades(
        db,
        token_ca,
        now_ts=now_ts,
        lookback_sec=max(ATH_RECOVERY_COOLDOWN_SEC, ATH_RECOVERY_HARD_LOSS_COOLDOWN_SEC),
    )
    hard_loss = None
    attempts = 0
    latest_mode_trade = None
    for trade in trades:
        reason = str(trade.get('exit_reason') or '').lower()
        pnl = _pnl_decimal(trade.get('pnl_pct'))
        peak = _pnl_decimal(trade.get('peak_pnl')) or 0.0
        trade_mode = str(trade.get('entry_mode') or '')
        hard_exit = 'hard_sl' in reason or 'hard_floor' in reason
        no_follow_exit = 'no_follow' in reason or 'fast_fail' in reason or 'doa' in reason
        deep_loss = pnl is not None and pnl <= ATH_RECOVERY_HARD_LOSS_PNL
        low_peak_hard_exit = hard_exit and peak < ATH_RECOVERY_HARD_LOSS_LOW_PEAK
        failed_recovery_probe = trade_mode in ATH_RECOVERY_TINY_PROBE_MODES and pnl is not None and pnl < 0 and (
            hard_exit or no_follow_exit
        )
        if hard_loss is None and (deep_loss or low_peak_hard_exit or failed_recovery_probe):
            hard_loss = trade
        if str(trade.get('entry_mode') or '') == str(entry_mode or ''):
            attempts += 1
            latest_mode_trade = latest_mode_trade or trade
    if hard_loss:
        return {
            'pass': False,
            'reason': 'ath_recovery_recent_hard_loss',
            'hard_loss_trade': hard_loss,
            'attempt_count': attempts,
            'thresholds': {
                'hard_loss_cooldown_sec': ATH_RECOVERY_HARD_LOSS_COOLDOWN_SEC,
                'max_attempts_per_token': ATH_RECOVERY_MAX_ATTEMPTS_PER_TOKEN,
            },
        }
    if attempts >= ATH_RECOVERY_MAX_ATTEMPTS_PER_TOKEN:
        return {
            'pass': False,
            'reason': 'ath_recovery_token_attempt_limit',
            'latest_trade': latest_mode_trade,
            'attempt_count': attempts,
            'thresholds': {
                'cooldown_sec': ATH_RECOVERY_COOLDOWN_SEC,
                'max_attempts_per_token': ATH_RECOVERY_MAX_ATTEMPTS_PER_TOKEN,
            },
        }
    return {
        'pass': True,
        'reason': 'ath_recovery_cooldown_ok',
        'attempt_count': attempts,
        'recent_trade_count': len(trades),
    }


def _ath_recovery_reference_trade(db, token_ca, token_risk=None, *, now_ts=None):
    token_risk = token_risk or {}
    ref = {
        'id': token_risk.get('last_failure_trade_id'),
        'exit_ts': token_risk.get('last_failure_exit_ts'),
        'exit_reason': token_risk.get('last_failure_reason'),
        'pnl_pct': token_risk.get('last_failure_pnl'),
    }
    trades = _ath_recovery_recent_trades(
        db,
        token_ca,
        now_ts=now_ts,
        lookback_sec=max(ATH_RECOVERY_COOLDOWN_SEC, ATH_RECOVERY_HARD_LOSS_COOLDOWN_SEC),
        limit=1,
    )
    if trades:
        trade = trades[0]
        ref.update(trade)
    return ref if any(value is not None for value in ref.values()) else None


def _ath_recovery_current_price(dex_snapshot=None, lifecycle=None, candidate=None, detail=None):
    features = (lifecycle or {}).get('lifecycle_features') or {}
    return _first_number(
        (dex_snapshot or {}).get('price'),
        (dex_snapshot or {}).get('price_usd'),
        (dex_snapshot or {}).get('current_price'),
        features.get('current_price'),
        features.get('price'),
        (detail or {}).get('current_price'),
        (candidate or {}).get('signal_price'),
        ((candidate or {}).get('watchlist_entry') or {}).get('signal_price'),
    )


def _ath_recovery_eligibility(
    db,
    *,
    entry_mode,
    candidate=None,
    detail=None,
    route=None,
    token_risk=None,
    current_reclaim=None,
    dex_snapshot=None,
    lifecycle=None,
    activity=None,
    liquidity_usd=None,
    top1_pct=None,
    top10_pct=None,
    quote_probe=None,
    now_ts=None,
):
    """Shared ATH recovery gate. It only evaluates new recovery modes, never no-kline."""
    candidate = candidate or {}
    detail = detail or {}
    route = str(route or candidate.get('route') or '').upper()
    entry_mode = str(entry_mode or '')
    token_ca = candidate.get('token_ca') or detail.get('token_ca')
    now_ts = float(now_ts or time.time())
    source_reason = _ath_recovery_source_reason(candidate, detail.get('source_detail') or candidate.get('source_detail') or {})
    parent_reason = _ath_recovery_parent_reason(candidate, detail.get('source_detail') or candidate.get('source_detail') or {})
    activity = activity or {}
    scores = _ath_recovery_scores(candidate, detail)
    current_price = _ath_recovery_current_price(dex_snapshot, lifecycle, candidate, detail)
    reference_trade = _ath_recovery_reference_trade(db, token_ca, token_risk, now_ts=now_ts)
    reference_price = _first_number(
        (reference_trade or {}).get('exit_price'),
        (reference_trade or {}).get('entry_price'),
        ((candidate.get('watchlist_entry') or {}) if isinstance(candidate.get('watchlist_entry'), dict) else {}).get('signal_price'),
        candidate.get('signal_price'),
    )
    recovery_pct = (
        ((current_price / reference_price) - 1.0) * 100.0
        if current_price is not None and reference_price and reference_price > 0
        else None
    )
    bs = _first_float_any(activity.get('buy_sell_ratio'), default=None)
    tx_m5 = _first_float_any(activity.get('tx_m5'), default=None)
    price_change_m5 = _first_float_any(activity.get('price_change_m5'), default=None)
    bounce_pct = _first_number(
        (current_reclaim or {}).get('bounce_from_low_pct'),
        (current_reclaim or {}).get('bounce_pct'),
        price_change_m5,
    )
    quote_ok = True
    if quote_probe is not None:
        quote_ok = bool(quote_probe.get('success'))
    if entry_mode == ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE:
        matrix = _ath_reclaim_after_failure_matrix_detail(scores)
    else:
        matrix = _ath_recovery_matrix_detail(
            scores,
            matrix_dissonance=(entry_mode == ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE),
        )
    cooldown = _ath_recovery_cooldown_detail(db, token_ca, entry_mode, now_ts=now_ts)
    observed = {
        'source_reason': source_reason,
        'parent_reason': parent_reason,
        'current_price': current_price,
        'reference_price': reference_price,
        'recovery_pct': recovery_pct,
        'buy_sell_ratio': bs,
        'tx_m5': tx_m5,
        'price_change_m5': price_change_m5,
        'bounce_pct': bounce_pct,
        'liquidity_usd': liquidity_usd,
        'top1_pct': top1_pct,
        'top10_pct': top10_pct,
        'quote_probe': quote_probe,
        'token_risk': token_risk or {},
        'current_reclaim': current_reclaim or {},
        'matrix': matrix,
        'cooldown': cooldown,
    }
    thresholds = {
        'size_sol': PAPER_TINY_SCOUT_SIZE_SOL,
        'recovery_pct': ATH_RECOVERY_MIN_RECLAIM_PCT,
        'buy_sell_ratio': ATH_RECOVERY_MIN_BS,
        'tx_m5': ATH_RECOVERY_MIN_TX_M5,
        'failure_reclaim_pct': ATH_RECLAIM_AFTER_FAILURE_MIN_RECLAIM_PCT,
        'failure_buy_sell_ratio': ATH_RECLAIM_AFTER_FAILURE_MIN_BS,
        'failure_tx_m5': ATH_RECLAIM_AFTER_FAILURE_MIN_TX_M5,
        'matrix': matrix.get('thresholds'),
        'liquidity_usd': ATH_MATRIX_DISSONANCE_MIN_LIQUIDITY_USD,
        'matrix_dissonance_buy_sell_ratio': ATH_MATRIX_DISSONANCE_MIN_BS,
        'matrix_dissonance_tx_m5': ATH_MATRIX_DISSONANCE_MIN_TX_M5,
        'matrix_dissonance_price_change_m5': ATH_MATRIX_DISSONANCE_MIN_PC_M5,
        'micro_bounce_pct': ATH_MICRO_RECLAIM_MIN_BOUNCE_PCT,
        'micro_buy_sell_ratio': ATH_MICRO_RECLAIM_MIN_BS,
        'micro_tx_m5': ATH_MICRO_RECLAIM_MIN_TX_M5,
    }

    def _result(passed, reason):
        return {
            'pass': bool(passed),
            'reason': reason,
            'entry_mode': entry_mode,
            'family': _ath_recovery_family(entry_mode),
            'source_reason': source_reason,
            'parent_block_reason': parent_reason,
            'observed': observed,
            'thresholds': thresholds,
        }

    if not ATH_RECOVERY_TINY_PROBES_ENABLED:
        return _result(False, 'ath_recovery_disabled')
    if route != 'ATH':
        return _result(False, 'not_ath_route')
    if entry_mode == ATH_NO_KLINE_TINY_PROBE_MODE:
        return _result(False, 'ath_no_kline_path_not_recovery')
    if entry_mode not in ATH_RECOVERY_TINY_PROBE_MODES:
        return _result(False, 'not_ath_recovery_mode')
    if not cooldown.get('pass'):
        return _result(False, cooldown.get('reason') or 'ath_recovery_cooldown_block')
    if top1_pct is not None and top1_pct > 50:
        return _result(False, 'ath_recovery_top1_too_high')
    if top10_pct is not None and top10_pct > 45:
        return _result(False, 'ath_recovery_top10_too_high')

    if entry_mode == ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE:
        if not ATH_RECLAIM_AFTER_FAILURE_ENABLED:
            return _result(False, 'ath_reclaim_after_failure_disabled')
        if source_reason != 'scout_quality_recent_token_failure':
            return _result(False, 'ath_reclaim_after_failure_wrong_source')
        if (token_risk or {}).get('blocked') and not (token_risk or {}).get('cooldown_expired'):
            return _result(False, 'ath_reclaim_after_failure_hard_cooldown')
        if not matrix.get('pass'):
            return _result(False, 'ath_reclaim_after_failure_matrix_not_strong')
        if recovery_pct is None or recovery_pct < ATH_RECLAIM_AFTER_FAILURE_MIN_RECLAIM_PCT:
            return _result(False, 'ath_reclaim_after_failure_price_not_recovered')
        if bs is None or bs < ATH_RECLAIM_AFTER_FAILURE_MIN_BS:
            return _result(False, 'ath_reclaim_after_failure_buy_pressure_weak')
        if tx_m5 is None or tx_m5 < ATH_RECLAIM_AFTER_FAILURE_MIN_TX_M5:
            return _result(False, 'ath_reclaim_after_failure_tx_low')
        if price_change_m5 is None or price_change_m5 < 0:
            return _result(False, 'ath_reclaim_after_failure_negative_m5')
        if not quote_ok:
            return _result(False, 'ath_reclaim_after_failure_quote_not_executable')
        return _result(True, 'ath_reclaim_after_failure_pass')

    if entry_mode == ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE:
        if not ATH_MATRIX_DISSONANCE_TINY_PROBE_ENABLED:
            return _result(False, 'ath_matrix_dissonance_disabled')
        if not (
            source_reason == 'matrices not yet aligned'
            or parent_reason == 'matrices not yet aligned'
            or _matrix_micro_momentum_reason(source_reason)
            or _matrix_micro_momentum_reason(parent_reason)
        ):
            return _result(False, 'ath_matrix_dissonance_wrong_source')
        if not matrix.get('pass'):
            return _result(False, 'ath_matrix_dissonance_matrix_not_strong')
        if liquidity_usd is None or liquidity_usd < ATH_MATRIX_DISSONANCE_MIN_LIQUIDITY_USD:
            return _result(False, 'ath_matrix_dissonance_liquidity_low')
        if bs is None or bs < ATH_MATRIX_DISSONANCE_MIN_BS:
            return _result(False, 'ath_matrix_dissonance_buy_pressure_weak')
        if tx_m5 is None or tx_m5 < ATH_MATRIX_DISSONANCE_MIN_TX_M5:
            return _result(False, 'ath_matrix_dissonance_tx_low')
        if price_change_m5 is None or price_change_m5 < ATH_MATRIX_DISSONANCE_MIN_PC_M5:
            return _result(False, 'ath_matrix_dissonance_not_live_confirmed')
        if not quote_ok:
            return _result(False, 'ath_matrix_dissonance_quote_not_executable')
        if (token_risk or {}).get('blocked'):
            return _result(False, (token_risk or {}).get('reason') or 'ath_matrix_dissonance_token_risk')
        return _result(True, 'ath_matrix_dissonance_pass')

    if entry_mode == ATH_MICRO_RECLAIM_TINY_PROBE_MODE:
        if not ATH_MICRO_RECLAIM_WATCH_ENABLED:
            return _result(False, 'ath_micro_reclaim_disabled')
        if source_reason not in ATH_MICRO_RECLAIM_SOURCE_REASONS and parent_reason not in ATH_MICRO_RECLAIM_SOURCE_REASONS:
            return _result(False, 'ath_micro_reclaim_wrong_source')
        if not matrix.get('pass'):
            return _result(False, 'ath_micro_reclaim_matrix_not_strong')
        if bounce_pct is None or bounce_pct < ATH_MICRO_RECLAIM_MIN_BOUNCE_PCT:
            return _result(False, 'ath_micro_reclaim_bounce_not_confirmed')
        if bs is None or bs < ATH_MICRO_RECLAIM_MIN_BS:
            return _result(False, 'ath_micro_reclaim_buy_pressure_weak')
        if tx_m5 is None or tx_m5 < ATH_MICRO_RECLAIM_MIN_TX_M5:
            return _result(False, 'ath_micro_reclaim_tx_low')
        if price_change_m5 is not None and price_change_m5 < 0:
            return _result(False, 'ath_micro_reclaim_negative_trend_still_active')
        if not quote_ok:
            return _result(False, 'ath_micro_reclaim_quote_not_executable')
        if (token_risk or {}).get('blocked'):
            return _result(False, (token_risk or {}).get('reason') or 'ath_micro_reclaim_token_risk')
        return _result(True, 'ath_micro_reclaim_probe_pass')

    return _result(False, 'ath_recovery_unhandled_mode')


def _ath_dynamic_ttl_extension_detail(candidate, *, dex_snapshot=None, lifecycle=None, activity=None, quote_probe=None):
    if not ATH_DYNAMIC_TTL_ENABLED:
        return {'pass': False, 'reason': 'ath_dynamic_ttl_disabled'}
    candidate = candidate or {}
    if str(candidate.get('route') or '').upper() != 'ATH':
        return {'pass': False, 'reason': 'not_ath_route'}
    if int(candidate.get('ttl_extend_count') or 0) >= ATH_DYNAMIC_TTL_MAX_EXTENSIONS:
        return {'pass': False, 'reason': 'ath_dynamic_ttl_max_extensions'}
    last_wait = str(candidate.get('last_wait_reason') or '')
    if last_wait in {'scout_quality_buy_pressure_weak', 'scout_quality_negative_trend'}:
        return {'pass': False, 'reason': 'ath_dynamic_ttl_recent_quality_weak'}
    activity = activity or {}
    scores = _ath_recovery_scores(candidate, {'source_detail': candidate.get('source_detail') or {}})
    matrix = _ath_recovery_matrix_detail(scores, matrix_dissonance=True)
    bs = _first_float_any(activity.get('buy_sell_ratio'), default=None)
    pc_m5 = _first_float_any(activity.get('price_change_m5'), default=None)
    quote_ok = True if quote_probe is None else bool(quote_probe.get('success'))
    pass_detail = (
        matrix.get('pass')
        and (bs is None or bs >= ATH_MATRIX_DISSONANCE_MIN_BS)
        and (pc_m5 is None or pc_m5 >= 0)
        and quote_ok
    )
    reason = 'ath_tracking_ttl_extended' if pass_detail else 'ath_tracking_ttl_not_strong'
    return {
        'pass': bool(pass_detail),
        'reason': reason,
        'entry_mode': candidate.get('mode'),
        'ttl_extend_count': int(candidate.get('ttl_extend_count') or 0),
        'observed': {
            'matrix': matrix,
            'buy_sell_ratio': bs,
            'price_change_m5': pc_m5,
            'quote_probe': quote_probe,
            'last_wait_reason': last_wait,
            'lifecycle_state': (lifecycle or {}).get('lifecycle_state'),
            'entry_bias': (lifecycle or {}).get('entry_bias'),
        },
        'thresholds': {
            'max_extensions': ATH_DYNAMIC_TTL_MAX_EXTENSIONS,
            'extend_sec': ATH_DYNAMIC_TTL_EXTEND_SEC,
            'buy_sell_ratio': ATH_MATRIX_DISSONANCE_MIN_BS,
        },
    }


def apply_ath_recovery_no_follow_exit(pos, exit_matrix, *, now_ts=None):
    if not isinstance(exit_matrix, dict) or pos is None:
        return exit_matrix
    state = getattr(pos, 'monitor_state', None) or {}
    entry_mode = str(state.get('entryMode') or getattr(pos, 'entry_mode', '') or '')
    if entry_mode not in {ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE, ATH_MICRO_RECLAIM_TINY_PROBE_MODE}:
        return exit_matrix
    current_pnl = _safe_float(exit_matrix.get('current_pnl'), None)
    if current_pnl is None:
        return exit_matrix
    now_ts = float(now_ts or time.time())
    held_sec = max(0.0, now_ts - float(getattr(pos, 'entry_ts', now_ts) or now_ts))
    peak_pnl = max(
        _safe_float(exit_matrix.get('peak_pnl'), 0.0),
        _safe_float(getattr(pos, 'peak_pnl', 0.0), 0.0),
        current_pnl,
    )
    no_follow_sec = 120.0
    min_peak = 0.05
    if held_sec >= no_follow_sec and peak_pnl < min_peak and current_pnl <= 0:
        updated = dict(exit_matrix)
        updated.update({
            'action': 'exit',
            'reason': (
                f"ath_recovery_no_follow_exit "
                f"(held={held_sec:.0f}s peak={peak_pnl:.1%} < {min_peak:.1%})"
            ),
            'current_pnl': current_pnl,
            'peak_pnl': peak_pnl,
            'ath_recovery_no_follow_exit': {
                'entry_mode': entry_mode,
                'held_sec': held_sec,
                'peak_pnl': peak_pnl,
                'current_pnl': current_pnl,
                'thresholds': {
                    'held_sec': no_follow_sec,
                    'min_peak': min_peak,
                },
            },
        })
        return updated
    return exit_matrix


def position_is_probe_profit_capture_candidate(pos):
    if not PROBE_PROFIT_CAPTURE_ENABLED or pos is None:
        return False
    state = getattr(pos, 'monitor_state', None) or {}
    entry_mode = str(state.get('entryMode') or getattr(pos, 'entry_mode', '') or '')
    signal_route = str(state.get('signalRoute') or getattr(pos, 'signal_type', '') or '')
    try:
        size_sol = float(
            state.get('entrySol')
            or getattr(pos, 'position_size_sol', 0)
            or 0.0
        )
    except (TypeError, ValueError):
        size_sol = 0.0
    if entry_mode in PAPER_TINY_SCOUT_ENTRY_MODES:
        return True
    if entry_mode and ('scout' in entry_mode or 'probe' in entry_mode):
        return size_sol <= PROBE_PROFIT_CAPTURE_MAX_SIZE_SOL
    if signal_route == 'LOTTO' and size_sol > 0 and size_sol <= PROBE_PROFIT_CAPTURE_MAX_SIZE_SOL:
        return True
    return False


def position_is_observation_probe(pos):
    if pos is None:
        return False
    state = getattr(pos, 'monitor_state', None) or {}
    entry_mode = str(state.get('entryMode') or getattr(pos, 'entry_mode', '') or '')
    try:
        size_sol = float(
            state.get('entrySol')
            or getattr(pos, 'position_size_sol', 0)
            or 0.0
        )
    except (TypeError, ValueError):
        size_sol = 0.0
    if size_sol <= 0 or size_sol > PROBE_PROFIT_CAPTURE_MAX_SIZE_SOL:
        return False
    return entry_mode in PAPER_TINY_SCOUT_ENTRY_MODES or 'scout' in entry_mode or 'probe' in entry_mode


def apply_matrix_doa_fast_exit(pos, exit_matrix, *, now_ts=None):
    if position_is_observation_probe(pos):
        return exit_matrix
    if not isinstance(exit_matrix, dict):
        return exit_matrix
    now_ts = float(now_ts or time.time())
    _held_sec = max(0.0, now_ts - float(getattr(pos, 'entry_ts', now_ts) or now_ts))
    _current_pnl = exit_matrix.get('current_pnl')
    _peak_now = max(
        float(getattr(pos, 'peak_pnl', 0) or 0),
        float(exit_matrix.get('peak_pnl') or 0),
        float(_current_pnl or 0),
    )
    if (
        _held_sec >= MATRIX_DOA_EXIT_SEC
        and _peak_now <= MATRIX_DOA_PEAK_MAX
        and _current_pnl is not None
        and _current_pnl <= MATRIX_DOA_PNL_MAX
    ):
        return {
            'action': 'exit',
            'reason': (
                f"matrix_doa_fast_exit "
                f"(held={_held_sec:.0f}s peak={_peak_now:.1%} "
                f"pnl={_current_pnl:.1%})"
            ),
            'current_pnl': _current_pnl,
            'peak_pnl': _peak_now,
            'trail_floor': None,
        }
    return exit_matrix


def apply_probe_profit_capture(pos, w_entry, exit_matrix, *, now_ts=None):
    """Add fast profit capture for small probe/scout paper positions."""
    if not position_is_probe_profit_capture_candidate(pos):
        return exit_matrix
    if not isinstance(exit_matrix, dict):
        return exit_matrix
    current_pnl = _safe_float(exit_matrix.get('current_pnl'), None)
    if current_pnl is None:
        return exit_matrix
    state = getattr(pos, 'monitor_state', None) or {}
    peak_pnl = max(
        _safe_float(exit_matrix.get('peak_pnl'), 0.0),
        _safe_float(getattr(pos, 'peak_pnl', 0.0), 0.0),
        _safe_float((w_entry or {}).get('peak_pnl'), 0.0),
        current_pnl,
    )
    entry_mode = str(state.get('entryMode') or '')
    sold_pct = max(0.0, min(1.0, _safe_float(state.get('soldPct'), 0.0)))
    already_locked = bool((w_entry or {}).get('has_locked_profit')) or sold_pct > 0

    def _override(action, reason, *, trail_floor=None, sell_pct=None):
        updated = dict(exit_matrix)
        updated.update({
            'action': action,
            'reason': reason,
            'current_pnl': current_pnl,
            'peak_pnl': peak_pnl,
            'trail_floor': trail_floor,
            'probe_profit_capture': {
                'enabled': True,
                'entry_mode': entry_mode,
                'peak_pnl': peak_pnl,
                'current_pnl': current_pnl,
                'sold_pct': sold_pct,
                'already_locked': already_locked,
                'thresholds': {
                    'lock_pnl': PROBE_PROFIT_CAPTURE_LOCK_PNL,
                    'lock_sell_pct': PROBE_PROFIT_CAPTURE_LOCK_SELL_PCT,
                    'start_peak': PROBE_PROFIT_CAPTURE_START_PEAK,
                    'start_floor': PROBE_PROFIT_CAPTURE_START_FLOOR,
                    'peak10_floor': PROBE_PROFIT_CAPTURE_10_PEAK_FLOOR,
                    'peak15_floor': PROBE_PROFIT_CAPTURE_15_PEAK_FLOOR,
                    'runner_floor': probe_runner_floor(peak_pnl),
                },
            },
        })
        if sell_pct is not None:
            updated['sell_pct'] = sell_pct
        return updated

    if not already_locked and current_pnl >= PROBE_PROFIT_CAPTURE_LOCK_PNL:
        return _override(
            'lock_profit',
            (
                f"probe_profit_lock "
                f"(pnl={current_pnl:.1%} >= {PROBE_PROFIT_CAPTURE_LOCK_PNL:.1%}, "
                f"sell={PROBE_PROFIT_CAPTURE_LOCK_SELL_PCT:.0%})"
            ),
            sell_pct=PROBE_PROFIT_CAPTURE_LOCK_SELL_PCT,
        )
    runner_floor = probe_runner_floor(peak_pnl) if already_locked else None
    if runner_floor is not None and current_pnl <= runner_floor:
        return _override(
            'exit',
            (
                f"probe_runner_floor "
                f"(pnl={current_pnl:.1%} <= floor={runner_floor:.1%}, "
                f"peak={peak_pnl:.1%})"
            ),
            trail_floor=runner_floor,
        )
    if position_is_observation_probe(pos):
        if not already_locked and peak_pnl >= 0.10 and current_pnl > 0:
            return _override(
                'lock_profit',
                (
                    f"probe_profit_late_lock "
                    f"(pnl={current_pnl:.1%}, peak={peak_pnl:.1%}, "
                    f"sell={PROBE_PROFIT_CAPTURE_LOCK_SELL_PCT:.0%})"
                ),
                sell_pct=PROBE_PROFIT_CAPTURE_LOCK_SELL_PCT,
            )
        return exit_matrix
    if peak_pnl >= 0.15 and current_pnl <= PROBE_PROFIT_CAPTURE_15_PEAK_FLOOR:
        return _override(
            'exit',
            (
                f"probe_profit_capture_15_floor "
                f"(pnl={current_pnl:.1%} <= floor={PROBE_PROFIT_CAPTURE_15_PEAK_FLOOR:.1%}, "
                f"peak={peak_pnl:.1%})"
            ),
            trail_floor=PROBE_PROFIT_CAPTURE_15_PEAK_FLOOR,
        )
    if peak_pnl >= 0.10 and current_pnl <= PROBE_PROFIT_CAPTURE_10_PEAK_FLOOR:
        return _override(
            'exit',
            (
                f"probe_profit_capture_10_floor "
                f"(pnl={current_pnl:.1%} <= floor={PROBE_PROFIT_CAPTURE_10_PEAK_FLOOR:.1%}, "
                f"peak={peak_pnl:.1%})"
            ),
            trail_floor=PROBE_PROFIT_CAPTURE_10_PEAK_FLOOR,
        )
    if peak_pnl >= PROBE_PROFIT_CAPTURE_START_PEAK and current_pnl <= PROBE_PROFIT_CAPTURE_START_FLOOR:
        return _override(
            'exit',
            (
                f"probe_profit_capture_breakeven "
                f"(pnl={current_pnl:.1%} <= floor={PROBE_PROFIT_CAPTURE_START_FLOOR:.1%}, "
                f"peak={peak_pnl:.1%})"
            ),
            trail_floor=PROBE_PROFIT_CAPTURE_START_FLOOR,
        )
    return exit_matrix


def _signal_ts_order_value(value):
    try:
        ts = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    if ts > 1_000_000_000_000:
        ts = ts / 1000.0
    return ts


def supersede_stale_pending_for_signal(pending_entries, token_ca, signal_ts, signal_type):
    if not pending_entries or not token_ca:
        return []
    incoming_type = str(signal_type or '').upper()
    if incoming_type != 'ATH':
        return []
    incoming_ts = _signal_ts_order_value(signal_ts)
    if incoming_ts <= 0:
        return []

    superseded = []
    for lifecycle_id, pending in list(pending_entries.items()):
        if pending.get('token_ca') != token_ca:
            continue
        pending_ts = _signal_ts_order_value(pending.get('signal_ts'))
        if pending_ts <= 0 or pending_ts >= incoming_ts:
            continue
        future = pending.get('_smart_entry_future')
        if future is not None:
            try:
                future.cancel()
            except Exception:
                pass
        superseded.append((lifecycle_id, pending))
        pending_entries.pop(lifecycle_id, None)
    return superseded


def evaluate_entry_edge_budget(*, route=None, trigger_price=None, quote_price=None,
                               lifecycle=None, pending=None, token_risk=None):
    """Last-mile entry contract: the fill cannot consume the trade's edge budget."""
    lifecycle = lifecycle or {}
    pending = pending or {}
    features = lifecycle.get('lifecycle_features') or {}
    route_name = str(route or pending.get('signal_route') or pending.get('signal_type') or '').upper()
    is_lotto = route_name == 'LOTTO' or bool(pending.get('is_lotto'))
    lotto_state = pending.get('lotto_state') or {}
    is_probe = (
        bool(lotto_state.get('probe'))
        or pending.get('replay_source') == 'live_monitor_lotto_probe'
        or pending.get('replay_source') == 'live_monitor_lotto_upstream_probe'
        or pending.get('replay_source') == 'live_monitor_lotto_upstream_realtime'
        or pending.get('replay_source') == 'live_monitor_ath_uncertainty'
        or pending.get('replay_source') == 'live_monitor_discovery_probe'
        or pending.get('entry_mode') == 'lotto_real_probe_reentry_arm'
        or pending.get('entry_mode') == LOTTO_UPSTREAM_MISS_TINY_SCOUT_MODE
        or pending.get('entry_mode') == LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MODE
        or pending.get('entry_mode') in LOTTO_RECOVERY_TINY_PROBE_MODES
        or pending.get('entry_mode') == ATH_UNCERTAINTY_TINY_SCOUT_MODE
        or pending.get('entry_mode') in {
            ATH_SOFT_RECLAIM_TINY_SCOUT_MODE,
            UNKNOWN_DATA_ACTIVITY_TINY_SCOUT_MODE,
            MATRIX_RECLAIM_TINY_PROBE_MODE,
            MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE,
            LOTTO_HIGH_RISK_DISCOVERY_PROBE_MODE,
        }
    )
    entry_decision = lotto_state.get('entryDecision') or {}
    entry_mode = str(
        pending.get('entry_mode')
        or entry_decision.get('entry_mode')
        or ''
    )
    try:
        pending_size_sol = float(
            pending.get('kelly_position_sol')
            or lotto_state.get('positionSizeSol')
            or entry_decision.get('position_size_sol')
            or 0.0
        )
    except (TypeError, ValueError):
        pending_size_sol = 0.0
    is_tiny_scout = pending_is_paper_tiny_scout(pending)
    spread_pct = calculate_entry_spread_pct(trigger_price, quote_price)

    def _feature_float(name, default=None):
        value = features.get(name)
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    liquidity_unknown = bool(features.get('liquidity_unknown'))
    live_top1_pct = _feature_float('live_top1_pct')
    max_spread_pct = ENTRY_EDGE_MATRIX_MAX_SPREAD_PCT
    profile = 'matrix'
    if route_name == 'ATH':
        max_spread_pct = ENTRY_EDGE_ATH_MAX_SPREAD_PCT
        profile = 'ath'
    if is_lotto:
        max_spread_pct = ENTRY_EDGE_LOTTO_MAX_SPREAD_PCT
        profile = 'lotto'
        if is_probe:
            max_spread_pct = ENTRY_EDGE_LOTTO_PROBE_MAX_SPREAD_PCT
            profile = 'lotto_probe'
        elif liquidity_unknown or (live_top1_pct is not None and live_top1_pct >= 30.0):
            max_spread_pct = ENTRY_EDGE_LOTTO_RISKY_MAX_SPREAD_PCT
            profile = 'lotto_risky'

    risk_penalty_pct = 0.0
    if token_risk and token_risk.get('risk_profile') == 'waterfall_memory':
        risk_penalty_pct = 0.5
    gmgn_policy = {}
    if is_lotto:
        gmgn_policy = (
            (pending.get('lotto_state') or {})
            .get('entryDecision', {})
            .get('gmgn_policy')
        ) or pending.get('gmgn_policy') or {}
    gmgn_spread_penalty_pct = 0.0
    if gmgn_policy:
        try:
            gmgn_spread_penalty_pct = float(gmgn_policy.get('spread_penalty_pct') or 0.0)
        except (TypeError, ValueError):
            gmgn_spread_penalty_pct = 0.0
        if gmgn_spread_penalty_pct <= 0 and int(gmgn_policy.get('toxic_score') or 0) >= 2:
            gmgn_spread_penalty_pct = 0.5
        risk_penalty_pct += max(0.0, gmgn_spread_penalty_pct)
    readiness_policy = pending.get('entry_readiness_policy') or {}
    readiness_max_spread_pct = None
    if readiness_policy:
        try:
            readiness_max_spread_pct = float(readiness_policy.get('max_spread_pct'))
        except (TypeError, ValueError):
            readiness_max_spread_pct = None
    if readiness_max_spread_pct is not None:
        max_spread_pct = min(max_spread_pct, readiness_max_spread_pct)
    tiny_scout_spread_cap_pct = None
    if is_tiny_scout:
        tiny_scout_spread_cap_pct = ENTRY_EDGE_TINY_SCOUT_MAX_SPREAD_PCT
        if entry_mode in {
            LOTTO_UPSTREAM_MISS_TINY_SCOUT_MODE,
            LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MODE,
            UNKNOWN_DATA_ACTIVITY_TINY_SCOUT_MODE,
            LOTTO_HIGH_RISK_DISCOVERY_PROBE_MODE,
            *LOTTO_RECOVERY_TINY_PROBE_MODES,
        }:
            max_spread_pct = min(max_spread_pct, ENTRY_EDGE_LOTTO_RISKY_MAX_SPREAD_PCT)
            tiny_scout_spread_cap_pct = max_spread_pct
        elif entry_mode in {
            ATH_UNCERTAINTY_TINY_SCOUT_MODE,
            ATH_SOFT_RECLAIM_TINY_SCOUT_MODE,
            MATRIX_RECLAIM_TINY_PROBE_MODE,
            MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE,
        }:
            max_spread_pct = min(max_spread_pct, 2.0)
            tiny_scout_spread_cap_pct = max_spread_pct
        else:
            max_spread_pct = max(max_spread_pct, tiny_scout_spread_cap_pct)
    effective_max_spread_pct = max(0.0, max_spread_pct - risk_penalty_pct)

    detail = {
        'pass': True,
        'profile': profile,
        'route': route_name,
        'trigger_price': trigger_price,
        'quote_price': quote_price,
        'spread_pct': spread_pct,
        'warn_spread_pct': MATRIX_SPREAD_WARN_PCT,
        'max_spread_pct': effective_max_spread_pct,
        'base_max_spread_pct': max_spread_pct,
        'risk_penalty_pct': risk_penalty_pct,
        'gmgn_spread_penalty_pct': gmgn_spread_penalty_pct,
        'gmgn_policy_action': gmgn_policy.get('action'),
        'gmgn_policy_reason': gmgn_policy.get('reason'),
        'readiness_max_spread_pct': readiness_max_spread_pct,
        'tiny_scout_spread_cap_pct': tiny_scout_spread_cap_pct,
        'entry_mode': entry_mode or None,
        'is_tiny_scout': is_tiny_scout,
        'pending_size_sol': pending_size_sol,
        'required_follow_peak_pct': ENTRY_EDGE_MIN_FOLLOW_PEAK_PCT,
        'remaining_follow_budget_pct': (
            ENTRY_EDGE_MIN_FOLLOW_PEAK_PCT - max(spread_pct or 0.0, 0.0)
            if spread_pct is not None else None
        ),
        'liquidity_unknown': liquidity_unknown,
        'live_top1_pct': live_top1_pct,
        'is_probe': is_probe,
        'reason': 'entry_edge_budget_ok',
    }
    if spread_pct is None:
        detail['reason'] = 'entry_edge_no_trigger_or_quote'
        return detail
    if spread_pct > effective_max_spread_pct:
        detail['pass'] = False
        detail['reason'] = 'entry_edge_spread_too_high'
    return detail


def _safe_json_loads(value):
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value)
    except Exception:
        return {}


def evaluate_spread_abort_memory(db, token_ca, *, lifecycle=None, current_spread_pct=None,
                                 max_spread_pct=None, now_ts=None):
    """Persistent CA-level memory for spread aborts."""
    now_ts = float(now_ts or time.time())
    cutoff_ts = now_ts - ENTRY_SPREAD_ABORT_MEMORY_SEC
    empty = {
        'blocked': False,
        'abort_count': 0,
        'reason': 'no_spread_abort_memory',
    }
    if not db or not token_ca:
        return empty
    try:
        count_row = db.execute(
            """
            SELECT COUNT(*) AS abort_count, MAX(event_ts) AS last_abort_ts
            FROM paper_decision_events
            WHERE token_ca = ?
              AND component = 'execution_guard'
              AND event_type = 'entry_abort'
              AND reason = 'entry_edge_spread_too_high'
              AND event_ts >= ?
            """,
            (token_ca, cutoff_ts),
        ).fetchone()
    except Exception:
        return empty
    if not count_row:
        return empty

    abort_count_val = count_row['abort_count'] if hasattr(count_row, 'keys') else count_row[0]
    abort_count = int(abort_count_val or 0)
    if abort_count <= 0:
        return empty
    last_abort_val = count_row['last_abort_ts'] if hasattr(count_row, 'keys') else count_row[1]
    last_abort_ts = float(last_abort_val or now_ts)

    try:
        last_row = db.execute(
            """
            SELECT payload_json
            FROM paper_decision_events
            WHERE token_ca = ?
              AND component = 'execution_guard'
              AND event_type = 'entry_abort'
              AND reason = 'entry_edge_spread_too_high'
              AND event_ts = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (token_ca, last_abort_ts),
        ).fetchone()
        last_payload = _safe_json_loads(last_row['payload_json'] if hasattr(last_row, 'keys') else last_row[0]) if last_row else {}
    except Exception:
        last_payload = {}

    lifecycle = lifecycle or {}
    features = lifecycle.get('lifecycle_features') or {}

    def _f(value, default=0.0):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    pc_m5 = _f(features.get('price_change_m5'))
    bs = _f(features.get('buy_sell_ratio'))
    spread = _f(current_spread_pct, 999.0)
    spread_cap = _f(max_spread_pct, 0.0)
    fresh_pressure = pc_m5 >= ENTRY_SPREAD_ABORT_RECLAIM_M5_PCT and bs >= ENTRY_SPREAD_ABORT_RECLAIM_BS
    spread_repaired = (
        current_spread_pct is None
        or max_spread_pct is None
        or spread_cap <= 0.0
        or spread <= max(0.5, spread_cap * 0.5)
    )
    reclaimed = fresh_pressure and spread_repaired

    return {
        'blocked': not reclaimed,
        'abort_count': abort_count,
        'reason': 'spread_abort_memory_reclaimed' if reclaimed else 'spread_abort_memory_wait_reclaim',
        'last_abort_ts': last_abort_ts,
        'age_sec': max(0.0, now_ts - last_abort_ts),
        'memory_ttl_sec': ENTRY_SPREAD_ABORT_MEMORY_SEC,
        'last_abort_payload': last_payload,
        'price_change_m5': pc_m5,
        'buy_sell_ratio': bs,
        'current_spread_pct': current_spread_pct,
        'max_spread_pct': max_spread_pct,
        'required_price_change_m5': ENTRY_SPREAD_ABORT_RECLAIM_M5_PCT,
        'required_buy_sell_ratio': ENTRY_SPREAD_ABORT_RECLAIM_BS,
        'fresh_pressure': fresh_pressure,
        'spread_repaired': spread_repaired,
    }


def _real_probe_decayed(row, *, now_ts):
    age_sec = float(now_ts) - float(row['created_event_ts'] or 0)
    if age_sec > LOTTO_REAL_PROBE_MAX_AGE_SEC:
        return True, f'real_probe_stale_{int(age_sec)}s'
    pnl_5m = _safe_float(row['pnl_5m'], None)
    pnl_15m = _safe_float(row['pnl_15m'], None)
    if pnl_5m is not None and pnl_5m > 0 and pnl_15m is not None:
        if pnl_15m < pnl_5m * LOTTO_REAL_PROBE_DECAY_FACTOR:
            return True, 'real_probe_decay_15m_vs_5m'
    return False, 'ok'


def find_lotto_real_probe_candidates(db, *, now_ts, limit=3):
    if not LOTTO_REAL_PROBE_ENABLED:
        return []
    rows = db.execute(
        """
        WITH ranked AS (
            SELECT
                m.*,
                COALESCE(m.tradable_peak_pnl, m.max_pnl_recorded, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) AS best_pnl,
                COALESCE(m.first_tradable_pnl, m.pnl_15m, m.pnl_5m, 0) AS reclaim_pnl,
                ROW_NUMBER() OVER (
                    PARTITION BY m.token_ca
                    ORDER BY COALESCE(m.tradable_peak_pnl, m.max_pnl_recorded, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) DESC,
                             m.created_event_ts DESC
                ) AS rn
            FROM paper_missed_signal_attribution m
            WHERE m.route = 'LOTTO'
              AND m.baseline_price IS NOT NULL
              AND COALESCE(m.tradable_missed, 0) = 1
              AND COALESCE(m.would_stop_before_peak, 0) = 0
              AND m.tradability_status = 'tradable_reclaim'
              AND COALESCE(m.tradable_peak_pnl, m.max_pnl_recorded, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) >= ?
              AND COALESCE(m.first_tradable_pnl, m.pnl_15m, m.pnl_5m, 0) >= ?
              AND m.created_event_ts >= ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM paper_decision_events e
                  WHERE e.component = 'lotto_probe_live'
                    AND e.token_ca = m.token_ca
                    AND e.event_type IN ('pending_entry', 'reentry_armed')
              )
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        ORDER BY best_pnl DESC, reclaim_pnl DESC
        LIMIT ?
        """,
        (
            LOTTO_REAL_PROBE_MIN_MAX_PNL,
            LOTTO_REAL_PROBE_MIN_RECLAIM_PNL,
            now_ts - LOTTO_REAL_PROBE_MAX_AGE_SEC,
            limit,
        ),
    ).fetchall()
    return [
        row for row in rows
        if not _real_probe_decayed(row, now_ts=now_ts)[0]
    ]


def find_lotto_upstream_miss_tiny_scout_candidates(db, *, now_ts, limit=3):
    if not LOTTO_UPSTREAM_MISS_TINY_SCOUT_ENABLED:
        return []
    rows = db.execute(
        """
        WITH ranked AS (
            SELECT
                m.*,
                COALESCE(m.tradable_peak_pnl, m.max_pnl_recorded, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) AS best_pnl,
                COALESCE(m.first_tradable_pnl, m.pnl_15m, m.pnl_5m, 0) AS reclaim_pnl,
                ROW_NUMBER() OVER (
                    PARTITION BY m.token_ca
                    ORDER BY COALESCE(m.tradable_peak_pnl, m.max_pnl_recorded, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) DESC,
                             m.created_event_ts DESC
                ) AS rn
            FROM paper_missed_signal_attribution m
            WHERE m.route = 'LOTTO'
              AND m.baseline_price IS NOT NULL
              AND COALESCE(m.tradable_missed, 0) = 1
              AND COALESCE(m.would_stop_before_peak, 0) = 0
              AND m.tradability_status = 'tradable_reclaim'
              AND m.component IN ('upstream_gate', 'lotto_entry_gate', 'discovery_tracking')
              AND m.reject_reason IN (
                  'not_ath_v17',
                  'not_ath_prebuy_kline_unknown_data_blocked',
                  'lotto_observe_low_mc_vol',
                  'tracking_ttl_expired',
                  'trend_bearish_timeout',
                  'upstream_realtime_liquidity_too_low',
                  'discovery_liquidity_too_low',
                  'liquidity_too_low'
              )
              AND COALESCE(m.tradable_peak_pnl, m.max_pnl_recorded, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) >= ?
              AND COALESCE(m.first_tradable_pnl, m.pnl_15m, m.pnl_5m, 0) >= ?
              AND m.created_event_ts >= ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM paper_decision_events e
                  WHERE e.component = 'lotto_upstream_probe_live'
                    AND e.token_ca = m.token_ca
                    AND e.event_type IN ('pending_entry', 'reentry_armed')
              )
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        ORDER BY best_pnl DESC, reclaim_pnl DESC
        LIMIT ?
        """,
        (
            LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_MAX_PNL,
            LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_RECLAIM_PNL,
            now_ts - LOTTO_UPSTREAM_MISS_TINY_SCOUT_MAX_AGE_SEC,
            limit,
        ),
    ).fetchall()
    return rows


def enqueue_lotto_upstream_miss_tiny_scout_candidates(db, watchlist, pending_entries, positions, *, now_ts, limit=1, max_positions=None):
    if max_positions is not None and len(positions) + len(pending_entries) >= max_positions:
        return 0
    rows = find_lotto_upstream_miss_tiny_scout_candidates(db, now_ts=now_ts, limit=limit)
    if not rows:
        recent_scan = db.execute(
            """
            SELECT 1
            FROM paper_decision_events
            WHERE component = 'lotto_upstream_probe_live'
              AND event_type = 'scan'
              AND reason = 'no_candidates'
              AND event_ts >= ?
            LIMIT 1
            """,
            (now_ts - 300,),
        ).fetchone()
        if not recent_scan:
            record_decision_event(
                db,
                component='lotto_upstream_probe_live',
                event_type='scan',
                decision='observe',
                reason='no_candidates',
                route='LOTTO',
                payload={
                    'source_reasons': sorted(LOTTO_UPSTREAM_MISS_TINY_SCOUT_REASONS),
                    'max_age_sec': LOTTO_UPSTREAM_MISS_TINY_SCOUT_MAX_AGE_SEC,
                    'min_max_pnl': LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_MAX_PNL,
                    'min_reclaim_pnl': LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_RECLAIM_PNL,
                    'max_mc': LOTTO_UPSTREAM_MISS_TINY_SCOUT_MAX_MC,
                    'min_liquidity_usd': LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_LIQ_USD,
                },
                event_ts=now_ts,
            )
        return 0

    enqueued = 0
    for row in rows:
        token_ca = row['token_ca']
        symbol = row['symbol'] or token_ca[:8]
        lifecycle_id = build_lifecycle_id(token_ca, row['signal_ts'] or int(row['created_event_ts']))
        if max_positions is not None and len(positions) + len(pending_entries) >= max_positions:
            break
        if lifecycle_id in pending_entries or any(pos.token_ca == token_ca for pos in positions.values()):
            record_decision_event(
                db,
                component='lotto_upstream_probe_live',
                event_type='skip',
                decision='skip',
                reason='already_pending_or_holding',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='LOTTO',
                payload={'missed_attribution_id': row['id']},
                event_ts=now_ts,
            )
            continue

        recent_wait = db.execute(
            """
            SELECT 1
            FROM paper_decision_events
            WHERE component = 'lotto_upstream_probe_live'
              AND token_ca = ?
              AND event_type IN ('wait_reclaim', 'skip')
              AND event_ts >= ?
            LIMIT 1
            """,
            (token_ca, now_ts - 60),
        ).fetchone()
        if recent_wait:
            continue

        features = _json_dict(row['lifecycle_features_json'])
        payload = _json_dict(row['payload_json'])
        try:
            reclaim_dex = fetch_dexscreener_trend_snapshot(token_ca)
        except Exception:
            reclaim_dex = None

        current_mc = _first_number(
            (reclaim_dex or {}).get('market_cap'),
            (reclaim_dex or {}).get('fdv'),
            features.get('market_cap'),
            payload.get('market_cap'),
            payload.get('signal_mc'),
        )
        liquidity_usd = _first_number(
            (reclaim_dex or {}).get('liquidity_usd'),
            features.get('liquidity_usd'),
            payload.get('liquidity_usd'),
        )
        if current_mc <= 0 or current_mc >= LOTTO_UPSTREAM_MISS_TINY_SCOUT_MAX_MC:
            record_decision_event(
                db,
                component='lotto_upstream_probe_live',
                event_type='skip',
                decision='skip',
                reason='upstream_probe_mc_gate',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='LOTTO',
                data_source='dexscreener+missed_attribution',
                payload={
                    'missed_attribution_id': row['id'],
                    'current_mc': current_mc,
                    'max_mc': LOTTO_UPSTREAM_MISS_TINY_SCOUT_MAX_MC,
                    'source_reject_reason': row['reject_reason'],
                },
                event_ts=now_ts,
            )
            continue
        if liquidity_usd < LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_LIQ_USD:
            record_decision_event(
                db,
                component='lotto_upstream_probe_live',
                event_type='wait_reclaim',
                decision='wait',
                reason='upstream_probe_liquidity_too_low',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='LOTTO',
                data_source='dexscreener+missed_attribution',
                payload={
                    'missed_attribution_id': row['id'],
                    'liquidity_usd': liquidity_usd,
                    'min_liquidity_usd': LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_LIQ_USD,
                },
                event_ts=now_ts,
            )
            continue

        probe_entry = {
            'ca': token_ca,
            'symbol': symbol,
            'type': 'LOTTO',
            'signal_ts': row['signal_ts'] or int(row['created_event_ts']),
            'signal_price': row['baseline_price'],
            'signal_mc': current_mc,
            'signal_vol24h': features.get('vol_h1') or features.get('volume_24h') or 0,
            'signal_tx24h': features.get('tx_m5') or 0,
            'signal_top10': features.get('top10_pct') or 0,
            'added_at': row['created_event_ts'],
        }
        reclaim_lifecycle = lifecycle_payload_for(
            watchlist_entry=probe_entry,
            dex_snapshot=reclaim_dex,
            route='LOTTO',
            signal_ts=probe_entry['signal_ts'],
            signal_price=row['baseline_price'],
            now=now_ts,
        )
        current_reclaim = evaluate_token_reclaim(
            dex_snapshot=reclaim_dex,
            lifecycle=reclaim_lifecycle,
            route='LOTTO',
        )
        if not current_reclaim.get('reclaim_confirmed'):
            record_decision_event(
                db,
                component='lotto_upstream_probe_live',
                event_type='wait_reclaim',
                decision='wait',
                reason=current_reclaim.get('reason') or 'current_reclaim_not_confirmed',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='LOTTO',
                data_source='dexscreener+lifecycle',
                payload=with_lifecycle_payload({
                    'missed_attribution_id': row['id'],
                    'best_pnl': row['best_pnl'],
                    'reclaim_pnl': row['reclaim_pnl'],
                    'source_reject_reason': row['reject_reason'],
                    'current_reclaim': current_reclaim,
                    'current_mc': current_mc,
                    'liquidity_usd': liquidity_usd,
                }, reclaim_lifecycle),
                event_ts=now_ts,
            )
            continue

        token_risk = token_quarantine_state(db, token_ca, now_ts=now_ts, reclaim=current_reclaim)
        if token_risk.get('blocked'):
            record_decision_event(
                db,
                component='lotto_upstream_probe_live',
                event_type='wait_reclaim',
                decision='wait',
                reason=token_risk.get('reason') or 'token_quarantine',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='LOTTO',
                data_source='paper_trade_history+dexscreener',
                payload={
                    'missed_attribution_id': row['id'],
                    'token_risk': token_risk,
                    'current_reclaim': current_reclaim,
                },
                event_ts=now_ts,
            )
            continue

        scout_quality = evaluate_scout_quality(
            mode=LOTTO_UPSTREAM_MISS_TINY_SCOUT_MODE,
            route='LOTTO',
            trend=reclaim_dex,
            lifecycle=reclaim_lifecycle,
            token_risk=token_risk,
            position_size_sol=LOTTO_UPSTREAM_MISS_TINY_SCOUT_SIZE_SOL,
            current_mc=current_mc,
            liquidity_usd=liquidity_usd,
        )
        record_scout_quality_decision(
            db,
            scout_quality=scout_quality,
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=row['signal_ts'],
            signal_id=row['signal_id'],
            route='LOTTO',
            lifecycle=reclaim_lifecycle,
            scout_size={
                'entry_mode': LOTTO_UPSTREAM_MISS_TINY_SCOUT_MODE,
                'actual_size_sol': LOTTO_UPSTREAM_MISS_TINY_SCOUT_SIZE_SOL,
                'cap_sol': SCOUT_QUALITY_SIZE_CAP_SOL,
            },
            source_component=row['component'],
            source_reject_reason=row['reject_reason'],
            data_source='missed_attribution+dexscreener+lifecycle+paper_risk',
            event_ts=now_ts,
        )
        if not scout_quality.get('pass'):
            record_decision_event(
                db,
                component='lotto_upstream_probe_live',
                event_type='skip',
                decision='skip',
                reason=scout_quality.get('reason') or 'scout_quality_reject',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='LOTTO',
                data_source='dexscreener+lifecycle+paper_risk',
                payload=with_lifecycle_payload({
                    'missed_attribution_id': row['id'],
                    'source_reject_reason': row['reject_reason'],
                    'scout_quality': scout_quality,
                }, reclaim_lifecycle),
                event_ts=now_ts,
            )
            continue

        pool = get_pool_address(token_ca)
        if not pool:
            record_decision_event(
                db,
                component='lotto_upstream_probe_live',
                event_type='skip',
                decision='skip',
                reason='pool_not_found',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='LOTTO',
                payload={'missed_attribution_id': row['id']},
                event_ts=now_ts,
            )
            continue

        w_entry = watchlist.register(
            ca=token_ca,
            symbol=symbol,
            signal_type='LOTTO',
            pool_address=pool,
            signal_ts=row['signal_ts'] or int(row['created_event_ts']),
            premium_signal_id=row['signal_id'],
            signal_price=row['baseline_price'],
            signal_mc=current_mc,
            signal_super=0,
            signal_holders=0,
            signal_vol24h=probe_entry['signal_vol24h'],
            signal_tx24h=probe_entry['signal_tx24h'],
            signal_top10=probe_entry['signal_top10'],
        )
        if not w_entry:
            continue
        watchlist.update_position_state(w_entry['id'], signal_route='LOTTO')
        w_entry = watchlist.get_by_id(w_entry['id']) or w_entry
        detail = {
            'probe': True,
            'paper_only_scout': True,
            'probe_source': 'missed_attribution',
            'timing_passed': False,
            'timing_gate': 'lotto_upstream_miss_probe',
            'missed_attribution_id': row['id'],
            'source_component': row['component'],
            'source_reject_reason': row['reject_reason'],
            'best_pnl': row['best_pnl'],
            'reclaim_pnl': row['reclaim_pnl'],
            'tradability_status': row['tradability_status'],
            'tradability_reason': row['tradability_reason'],
            'first_tradable_horizon': row['first_tradable_horizon'],
            'first_tradable_pnl': row['first_tradable_pnl'],
            'tradable_peak_horizon': row['tradable_peak_horizon'],
            'tradable_peak_pnl': row['tradable_peak_pnl'],
            'current_reclaim': current_reclaim,
            'current_mc': current_mc,
            'liquidity_usd': liquidity_usd,
            'scout_quality': scout_quality,
            'position_size_sol': LOTTO_UPSTREAM_MISS_TINY_SCOUT_SIZE_SOL,
            'entry_mode': LOTTO_UPSTREAM_MISS_TINY_SCOUT_MODE,
        }
        pending_entries[lifecycle_id] = build_lotto_pending(w_entry, lifecycle_id, detail=detail)
        pending_entries[lifecycle_id]['kelly_position_sol'] = LOTTO_UPSTREAM_MISS_TINY_SCOUT_SIZE_SOL
        pending_entries[lifecycle_id]['smart_entry_retries'] = _LOTTO_TIMING_RETRY_MEMORY.get(lifecycle_id, 0)
        pending_entries[lifecycle_id]['entry_mode'] = LOTTO_UPSTREAM_MISS_TINY_SCOUT_MODE
        pending_entries[lifecycle_id]['scout_mode'] = LOTTO_UPSTREAM_MISS_TINY_SCOUT_MODE
        pending_entries[lifecycle_id]['paper_only_scout'] = True
        pending_entries[lifecycle_id]['replay_source'] = 'live_monitor_lotto_upstream_probe'
        pending_entries[lifecycle_id]['stage_outcome'] = 'lotto_upstream_miss_tiny_scout_armed'
        pending_entries[lifecycle_id]['lotto_state']['probe'] = True
        pending_entries[lifecycle_id]['lotto_state']['probeSource'] = 'upstream_miss_attribution'
        pending_entries[lifecycle_id]['lotto_state']['probeEntryMode'] = LOTTO_UPSTREAM_MISS_TINY_SCOUT_MODE
        pending_entries[lifecycle_id]['lotto_state']['paper_only_scout'] = True
        record_decision_event(
            db,
            component='lotto_upstream_probe_live',
            event_type='reentry_armed',
            decision='pending',
            reason='missed_upstream_tiny_scout_armed',
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=row['signal_ts'],
            signal_id=row['signal_id'],
            route='LOTTO',
            data_source='missed_attribution+dexscreener+lifecycle',
            payload=with_lifecycle_payload(detail, reclaim_lifecycle),
            event_ts=now_ts,
        )
        record_decision_event(
            db,
            component='lotto_upstream_probe_live',
            event_type='pending_entry',
            decision='pending',
            reason=LOTTO_UPSTREAM_MISS_TINY_SCOUT_MODE,
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=row['signal_ts'],
            signal_id=row['signal_id'],
            route='LOTTO',
            data_source='missed_attribution+dexscreener+lifecycle',
            payload=with_lifecycle_payload(detail, reclaim_lifecycle),
            event_ts=now_ts,
        )
        enqueued += 1
    return enqueued


def enqueue_lotto_real_probe_candidates(db, watchlist, pending_entries, positions, *, now_ts, limit=2, max_positions=None):
    if max_positions is not None and len(positions) + len(pending_entries) >= max_positions:
        return 0
    rows = find_lotto_real_probe_candidates(db, now_ts=now_ts, limit=limit)
    if not rows:
        recent_scan = db.execute(
            """
            SELECT 1
            FROM paper_decision_events
            WHERE component = 'lotto_probe_live'
              AND event_type = 'scan'
              AND reason = 'no_candidates'
              AND event_ts >= ?
            LIMIT 1
            """,
            (now_ts - 300,),
        ).fetchone()
        if not recent_scan:
            record_decision_event(
                db,
                component='lotto_probe_live',
                event_type='scan',
                decision='observe',
                reason='no_candidates',
                route='LOTTO',
                payload={
                    'max_age_sec': LOTTO_REAL_PROBE_MAX_AGE_SEC,
                    'min_max_pnl': LOTTO_REAL_PROBE_MIN_MAX_PNL,
                    'min_reclaim_pnl': LOTTO_REAL_PROBE_MIN_RECLAIM_PNL,
                },
                event_ts=now_ts,
            )
        return 0
    enqueued = 0
    for row in rows:
        token_ca = row['token_ca']
        symbol = row['symbol'] or token_ca[:8]
        lifecycle_id = build_lifecycle_id(token_ca, row['signal_ts'] or int(row['created_event_ts']))
        if max_positions is not None and len(positions) + len(pending_entries) >= max_positions:
            break
        if lifecycle_id in pending_entries or any(pos.token_ca == token_ca for pos in positions.values()):
            record_decision_event(
                db,
                component='lotto_probe_live',
                event_type='skip',
                decision='skip',
                reason='already_pending_or_holding',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='LOTTO',
                payload={'missed_attribution_id': row['id']},
                event_ts=now_ts,
            )
            continue

        recent_wait = db.execute(
            """
            SELECT 1
            FROM paper_decision_events
            WHERE component = 'lotto_probe_live'
              AND token_ca = ?
              AND event_type = 'wait_reclaim'
              AND event_ts >= ?
            LIMIT 1
            """,
            (token_ca, now_ts - 60),
        ).fetchone()
        if recent_wait:
            continue

        features = {}
        try:
            features = json.loads(row['lifecycle_features_json'] or '{}')
        except Exception:
            features = {}

        try:
            reclaim_dex = fetch_dexscreener_trend_snapshot(token_ca)
        except Exception:
            reclaim_dex = None
        probe_entry = {
            'ca': token_ca,
            'symbol': symbol,
            'type': 'LOTTO',
            'signal_ts': row['signal_ts'] or int(row['created_event_ts']),
            'signal_price': row['baseline_price'],
            'signal_mc': features.get('market_cap') or 0,
            'signal_vol24h': features.get('vol_h1') or 0,
            'added_at': row['created_event_ts'],
        }
        reclaim_lifecycle = lifecycle_payload_for(
            watchlist_entry=probe_entry,
            dex_snapshot=reclaim_dex,
            route='LOTTO',
            signal_ts=probe_entry['signal_ts'],
            signal_price=row['baseline_price'],
            now=now_ts,
        )
        current_reclaim = evaluate_token_reclaim(
            dex_snapshot=reclaim_dex,
            lifecycle=reclaim_lifecycle,
            route='LOTTO',
        )
        if not current_reclaim.get('reclaim_confirmed'):
            record_decision_event(
                db,
                component='lotto_probe_live',
                event_type='wait_reclaim',
                decision='wait',
                reason=current_reclaim.get('reason') or 'current_reclaim_not_confirmed',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='LOTTO',
                data_source='dexscreener+lifecycle',
                payload=with_lifecycle_payload({
                    'missed_attribution_id': row['id'],
                    'best_pnl': row['best_pnl'],
                    'reclaim_pnl': row['reclaim_pnl'],
                    'tradability_status': row['tradability_status'],
                    'current_reclaim': current_reclaim,
                }, reclaim_lifecycle),
                event_ts=now_ts,
            )
            continue

        token_risk = token_quarantine_state(db, token_ca, now_ts=now_ts, reclaim=current_reclaim)
        if token_risk.get('blocked'):
            record_decision_event(
                db,
                component='lotto_probe_live',
                event_type='wait_reclaim',
                decision='wait',
                reason=token_risk.get('reason') or 'token_quarantine',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='LOTTO',
                data_source='paper_trade_history+dexscreener',
                payload={
                    'missed_attribution_id': row['id'],
                    'token_risk': token_risk,
                    'current_reclaim': current_reclaim,
                },
                event_ts=now_ts,
            )
            continue

        pool = get_pool_address(token_ca)
        if not pool:
            record_decision_event(
                db,
                component='lotto_probe_live',
                event_type='skip',
                decision='skip',
                reason='pool_not_found',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='LOTTO',
                payload={'missed_attribution_id': row['id']},
                event_ts=now_ts,
            )
            continue

        w_entry = watchlist.register(
            ca=token_ca,
            symbol=symbol,
            signal_type='LOTTO',
            pool_address=pool,
            signal_ts=row['signal_ts'] or int(row['created_event_ts']),
            premium_signal_id=row['signal_id'],
            signal_price=row['baseline_price'],
            signal_mc=features.get('market_cap') or 0,
            signal_super=0,
            signal_holders=0,
            signal_vol24h=features.get('vol_h1') or 0,
            signal_tx24h=0,
            signal_top10=0,
        )
        if not w_entry:
            continue
        watchlist.update_position_state(w_entry['id'], signal_route='LOTTO')
        w_entry = watchlist.get_by_id(w_entry['id']) or w_entry
        detail = {
            'probe': True,
            'probe_source': 'missed_attribution',
            'timing_passed': False,
            'timing_gate': 'lotto_probe_reentry',
            'missed_attribution_id': row['id'],
            'source_component': row['component'],
            'source_reject_reason': row['reject_reason'],
            'best_pnl': row['best_pnl'],
            'reclaim_pnl': row['reclaim_pnl'],
            'tradability_status': row['tradability_status'],
            'tradability_reason': row['tradability_reason'],
            'first_tradable_horizon': row['first_tradable_horizon'],
            'first_tradable_pnl': row['first_tradable_pnl'],
            'tradable_peak_horizon': row['tradable_peak_horizon'],
            'tradable_peak_pnl': row['tradable_peak_pnl'],
            'current_reclaim': current_reclaim,
            'pnl_5m': row['pnl_5m'],
            'pnl_15m': row['pnl_15m'],
            'pnl_60m': row['pnl_60m'],
            'position_size_sol': LOTTO_REAL_PROBE_SIZE_SOL,
        }
        pending_entries[lifecycle_id] = build_lotto_pending(w_entry, lifecycle_id, detail=detail)
        pending_entries[lifecycle_id]['kelly_position_sol'] = LOTTO_REAL_PROBE_SIZE_SOL
        pending_entries[lifecycle_id]['smart_entry_retries'] = _LOTTO_TIMING_RETRY_MEMORY.get(lifecycle_id, 0)
        pending_entries[lifecycle_id]['entry_mode'] = 'lotto_real_probe_reentry_arm'
        pending_entries[lifecycle_id]['replay_source'] = 'live_monitor_lotto_probe'
        pending_entries[lifecycle_id]['stage_outcome'] = 'lotto_probe_reentry_armed'
        pending_entries[lifecycle_id]['lotto_state']['probe'] = True
        pending_entries[lifecycle_id]['lotto_state']['probeSource'] = 'missed_attribution'
        pending_entries[lifecycle_id]['lotto_state']['probeEntryMode'] = 'reentry_timing_gate'
        pending_entries[lifecycle_id]['lotto_state']['realProbe'] = {
            'missed_id': row['id'],
            'created_event_ts': row['created_event_ts'],
            'age_sec': max(0.0, float(now_ts) - float(row['created_event_ts'] or now_ts)),
            'source_component': row['component'],
            'reject_reason': row['reject_reason'],
            'best_pnl': row['best_pnl'],
            'reclaim_pnl': row['reclaim_pnl'],
            'pnl_5m': row['pnl_5m'],
            'pnl_15m': row['pnl_15m'],
            'pnl_60m': row['pnl_60m'],
            'current_reclaim': current_reclaim,
        }
        record_decision_event(
            db,
            component='lotto_probe_live',
            event_type='reentry_armed',
            decision='pending',
            reason='missed_confirmed_reentry_armed',
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=row['signal_ts'],
            signal_id=row['signal_id'],
            route='LOTTO',
            payload=detail,
            event_ts=now_ts,
        )
        enqueued += 1
    return enqueued


def _json_dict(value):
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _first_number(*values):
    for value in values:
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if number > 0:
            return number
    return 0.0


def _reason_matches_any(reason, prefixes):
    reason = str(reason or '')
    return any(reason == prefix or reason.startswith(prefix) for prefix in prefixes)


def _matrix_micro_momentum_reason(reason):
    reason = str(reason or '')
    lower = reason.lower()
    if lower.startswith('momentum check waiting: flat_no_fresh_tick'):
        return True
    if not lower.startswith('momentum check failed:'):
        return False
    if 'noise' in lower and '< 0.8%' in lower:
        return True
    match = re.search(r'declining\s+([+-]?\d+(?:\.\d+)?)%', lower)
    if match:
        try:
            return abs(float(match.group(1))) <= 0.25
        except (TypeError, ValueError):
            return False
    return False


def _entry_mode_for_ath_uncertainty_reason(reason):
    if _matrix_micro_momentum_reason(reason):
        return MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE
    return ATH_UNCERTAINTY_TINY_SCOUT_MODE


def _apply_actual_tiny_trigger_mode(pending, timing_reason):
    """Preserve parent scout mode while attributing live EV to the real trigger."""
    pending = pending or {}
    timing_reason = str(timing_reason or '')
    parent_mode = pending.get('parent_scout_mode') or pending.get('scout_mode') or pending.get('entry_mode')
    if parent_mode in PAPER_TINY_SCOUT_ENTRY_MODES:
        pending['parent_scout_mode'] = parent_mode
        pending['entry_trigger_mode'] = timing_reason
        if timing_reason in PAPER_TINY_SCOUT_ENTRY_MODES:
            pending['entry_mode'] = timing_reason
            pending['scout_mode'] = parent_mode
        else:
            pending['entry_mode'] = parent_mode
            pending['scout_mode'] = parent_mode
    else:
        pending['entry_mode'] = timing_reason
        pending['entry_trigger_mode'] = timing_reason
    return pending.get('entry_mode')


def _apply_primary_proving_cap(pending, size_sol):
    pending = pending or {}
    try:
        current_size = float(size_sol or 0.0)
    except (TypeError, ValueError):
        current_size = 0.0
    entry_mode = str(pending.get('entry_mode') or '')
    if (
        SMART_PULLBACK_BOUNCE_PROVING_CAP_ENABLED
        and not pending_is_paper_tiny_scout(pending)
        and entry_mode == 'smart_entry_pullback_bounce'
        and current_size > 0
    ):
        entry_mode_quality = pending.get('entry_mode_quality') or {}
        force_detail = pending.get('entry_mode_quality_force_live') or {}
        degraded_or_forced = (
            entry_mode_quality.get('reason') == 'entry_mode_quality_degraded'
            or entry_mode_quality.get('reason') == 'entry_mode_shadow_cooldown'
            or bool(force_detail)
        )
        cap_sol = SMART_PULLBACK_BOUNCE_DEGRADED_CAP_SOL if degraded_or_forced else SMART_PULLBACK_BOUNCE_PROVING_CAP_SOL
        capped_size = min(current_size, cap_sol)
        detail = {
            'entry_mode': entry_mode,
            'old_size_sol': current_size,
            'new_size_sol': capped_size,
            'cap_sol': cap_sol,
            'capped': capped_size < current_size,
            'reason': 'smart_pullback_bounce_proving_cap',
            'entry_mode_quality': entry_mode_quality,
            'force_live_detail': force_detail,
        }
        if capped_size < current_size:
            pending['kelly_position_sol'] = capped_size
            pending['primary_proving_cap'] = detail
        return capped_size, detail
    if (
        not PRIMARY_PROVING_CAP_ENABLED
        or pending_is_paper_tiny_scout(pending)
        or entry_mode not in PRIMARY_PROVING_CAP_MODES
        or current_size <= 0
    ):
        return current_size, None
    capped_size = min(current_size, PRIMARY_PROVING_CAP_SIZE_SOL)
    detail = {
        'entry_mode': entry_mode,
        'old_size_sol': current_size,
        'new_size_sol': capped_size,
        'cap_sol': PRIMARY_PROVING_CAP_SIZE_SOL,
        'capped': capped_size < current_size,
        'reason': 'primary_proving_cap',
    }
    if capped_size < current_size:
        pending['kelly_position_sol'] = capped_size
        pending['primary_proving_cap'] = detail
    return capped_size, detail


def arm_lotto_upstream_realtime_tiny_scout(
    db,
    watchlist,
    pending_entries,
    positions,
    *,
    sig,
    registered_entry,
    pool,
    lifecycle_id,
    signal_lifecycle,
    signal_audit_payload,
    hard_gate_status,
    now_ts,
    discovery_candidates=None,
):
    """Arm a tiny paper scout immediately when upstream uncertainty blocks LOTTO."""
    if not LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_ENABLED:
        return False
    reason = str(hard_gate_status or '').lower()
    if reason not in LOTTO_UPSTREAM_MISS_TINY_SCOUT_REASONS:
        return False
    token_ca = sig['token_ca']
    symbol = sig.get('symbol') or token_ca[:8]
    signal_ts = sig.get('timestamp')
    premium_signal_id = sig.get('id')
    if lifecycle_id in pending_entries or any(pos.token_ca == token_ca for pos in positions.values()):
        return False
    if not registered_entry or not pool:
        return False

    try:
        realtime_dex = fetch_dexscreener_trend_snapshot(token_ca)
    except Exception:
        realtime_dex = None
    try:
        gmgn_enrichment = fetch_gmgn_token_enrichment(token_ca)
    except Exception:
        gmgn_enrichment = None
    try:
        live_concentration = helius_token_concentration(token_ca)
    except Exception:
        live_concentration = None

    current_mc = _first_number(
        (realtime_dex or {}).get('market_cap'),
        (realtime_dex or {}).get('fdv'),
        sig.get('market_cap'),
        registered_entry.get('signal_mc'),
    )
    liquidity_usd = _first_number(
        (realtime_dex or {}).get('liquidity_usd'),
        sig.get('liquidity_usd'),
    )
    top10_pct = _first_number(
        (live_concentration or {}).get('top10_pct'),
        registered_entry.get('signal_top10'),
    )
    top1_pct = _first_number((live_concentration or {}).get('top1_pct'))
    detail = {
        **(signal_audit_payload or {}),
        'paper_only_scout': True,
        'probe': True,
        'probe_source': 'upstream_realtime',
        'source_component': 'upstream_gate',
        'source_reject_reason': reason,
        'entry_mode': LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MODE,
        'position_size_sol': LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_SIZE_SOL,
        'current_mc': current_mc,
        'liquidity_usd': liquidity_usd,
        'top1_pct': top1_pct,
        'top10_pct': top10_pct,
        'gmgn_readonly': gmgn_enrichment,
    }
    scout_lifecycle = lifecycle_payload_for(
        signal=sig,
        watchlist_entry=registered_entry,
        dex_snapshot=realtime_dex,
        live_concentration=live_concentration,
        route='LOTTO',
        signal_ts=signal_ts,
        signal_price=registered_entry.get('signal_price'),
        now=now_ts,
    )

    reject_reason = None
    if current_mc <= 0 or current_mc >= LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MAX_MC:
        reject_reason = 'upstream_realtime_mc_gate'
    elif liquidity_usd < LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MIN_LIQ_USD:
        reject_reason = 'upstream_realtime_liquidity_too_low'
    elif top1_pct and top1_pct > LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MAX_TOP1_PCT:
        reject_reason = 'upstream_realtime_top1_too_high'
    elif top10_pct and top10_pct > LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MAX_TOP10_PCT:
        reject_reason = 'upstream_realtime_top10_too_high'

    gmgn_policy = evaluate_gmgn_lotto_policy(
        gmgn_enrichment,
        detail,
        lifecycle=scout_lifecycle,
        entry_mode=LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MODE,
    )
    detail['gmgn_policy'] = gmgn_policy
    detail['gmgn_action'] = gmgn_policy.get('action')
    detail['gmgn_reason'] = gmgn_policy.get('reason')
    if gmgn_policy.get('action') == 'reject':
        reject_reason = gmgn_policy.get('reason') or 'gmgn_policy_reject'

    scout_quality = evaluate_scout_quality(
        mode=LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MODE,
        route='LOTTO',
        trend=realtime_dex,
        lifecycle=scout_lifecycle,
        gmgn=gmgn_policy,
        live_concentration=live_concentration,
        position_size_sol=LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_SIZE_SOL,
        current_mc=current_mc,
        liquidity_usd=liquidity_usd,
        top1_pct=top1_pct,
        top10_pct=top10_pct,
    )
    detail['scout_quality'] = scout_quality
    record_scout_quality_decision(
        db,
        scout_quality=scout_quality,
        token_ca=token_ca,
        symbol=symbol,
        lifecycle_id=lifecycle_id,
        signal_ts=signal_ts,
        signal_id=premium_signal_id,
        route='LOTTO',
        lifecycle=scout_lifecycle,
        scout_size={
            'entry_mode': LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MODE,
            'actual_size_sol': LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_SIZE_SOL,
            'cap_sol': SCOUT_QUALITY_SIZE_CAP_SOL,
        },
        source_component='upstream_gate',
        source_reject_reason=reason,
        data_source='premium_signals+dexscreener+gmgn+helius',
        event_ts=now_ts,
    )
    if not reject_reason and not scout_quality.get('pass'):
        reject_reason = scout_quality.get('reason') or 'scout_quality_reject'

    if reject_reason:
        discovery_mode = _discovery_mode_for_lotto_reason(reason) or _discovery_mode_for_lotto_reason(reject_reason)
        if discovery_mode and gmgn_policy.get('action') != 'reject':
            track_discovery_candidate(
                db,
                discovery_candidates,
                mode=discovery_mode,
                route='LOTTO',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=signal_ts,
                signal_id=premium_signal_id,
                pool=pool,
                watchlist_id=registered_entry.get('id') if registered_entry else None,
                watchlist_entry=registered_entry,
                source_component='upstream_gate',
                source_reject_reason=reason,
                source_detail={
                    'upstream_realtime_reject_reason': reject_reason,
                    'hard_gate_status': hard_gate_status,
                    'scout_quality': scout_quality,
                    'gmgn_policy': gmgn_policy,
                    'current_mc': current_mc,
                    'liquidity_usd': liquidity_usd,
                    'top1_pct': top1_pct,
                    'top10_pct': top10_pct,
                },
                lifecycle=scout_lifecycle or signal_lifecycle,
                now_ts=now_ts,
            )
        record_decision_event(
            db,
            component='lotto_upstream_realtime_scout',
            event_type='scout_reject',
            decision='reject',
            reason=reject_reason,
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=signal_ts,
            signal_id=premium_signal_id,
            route='LOTTO',
            data_source='premium_signals+dexscreener+gmgn+helius',
            payload=with_lifecycle_payload(detail, scout_lifecycle or signal_lifecycle),
            event_ts=now_ts,
        )
        return False

    pending_entries[lifecycle_id] = build_lotto_pending(registered_entry, lifecycle_id, detail=detail)
    pending_entries[lifecycle_id]['kelly_position_sol'] = LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_SIZE_SOL
    pending_entries[lifecycle_id]['smart_entry_retries'] = _LOTTO_TIMING_RETRY_MEMORY.get(lifecycle_id, 0)
    pending_entries[lifecycle_id]['entry_mode'] = LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MODE
    pending_entries[lifecycle_id]['scout_mode'] = LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MODE
    pending_entries[lifecycle_id]['paper_only_scout'] = True
    pending_entries[lifecycle_id]['replay_source'] = 'live_monitor_lotto_upstream_realtime'
    pending_entries[lifecycle_id]['stage_outcome'] = 'lotto_upstream_realtime_tiny_scout_armed'
    pending_entries[lifecycle_id]['lotto_state']['probe'] = True
    pending_entries[lifecycle_id]['lotto_state']['probeSource'] = 'upstream_realtime'
    pending_entries[lifecycle_id]['lotto_state']['probeEntryMode'] = LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MODE
    pending_entries[lifecycle_id]['lotto_state']['paper_only_scout'] = True
    record_decision_event(
        db,
        component='lotto_upstream_realtime_scout',
        event_type='pending_entry',
        decision='pending',
        reason=LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_MODE,
        token_ca=token_ca,
        symbol=symbol,
        lifecycle_id=lifecycle_id,
        signal_ts=signal_ts,
        signal_id=premium_signal_id,
        route='LOTTO',
        data_source='premium_signals+dexscreener+gmgn+helius',
        payload=with_lifecycle_payload(detail, scout_lifecycle or signal_lifecycle),
        event_ts=now_ts,
    )
    return True


def _ath_uncertainty_reason(reason):
    return _reason_matches_any(reason, ATH_UNCERTAINTY_REASONS)


def arm_ath_uncertainty_tiny_scout(
    db,
    pending_entries,
    positions,
    *,
    w_entry,
    lifecycle_id,
    eval_res,
    now_ts,
    discovery_candidates=None,
):
    """Arm an ATH tiny scout for uncertainty gates without changing main ATH gate."""
    if not ATH_UNCERTAINTY_TINY_SCOUT_ENABLED:
        return False
    if w_entry.get('type') != 'ATH':
        return False
    reason = str(eval_res.get('action_reason') or '')
    if not _ath_uncertainty_reason(reason):
        return False
    token_ca = w_entry['ca']
    symbol = w_entry['symbol']
    if lifecycle_id in pending_entries or any(pos.token_ca == token_ca for pos in positions.values()):
        return False
    pool = w_entry.get('pool_address') or get_pool_address(token_ca)
    if not pool:
        record_decision_event(
            db,
            component='ath_uncertainty_scout',
            event_type='scout_reject',
            decision='reject',
            reason='pool_not_found',
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=w_entry.get('signal_ts'),
            signal_id=w_entry.get('premium_signal_id'),
            route='ATH',
            payload={'source_reject_reason': reason},
            event_ts=now_ts,
        )
        return False
    try:
        dex_snapshot = fetch_dexscreener_trend_snapshot(token_ca)
    except Exception:
        dex_snapshot = None
    current_mc = _first_number(
        (dex_snapshot or {}).get('market_cap'),
        (dex_snapshot or {}).get('fdv'),
        w_entry.get('signal_mc'),
    )
    liquidity_usd = _first_number((dex_snapshot or {}).get('liquidity_usd'))
    top10_pct = _first_number(w_entry.get('signal_top10'))
    scout_lifecycle = lifecycle_payload_for(
        watchlist_entry=w_entry,
        dex_snapshot=dex_snapshot,
        route='ATH',
        signal_ts=w_entry.get('signal_ts'),
        signal_price=w_entry.get('signal_price'),
        now=now_ts,
    )
    scout_reclaim = evaluate_token_reclaim(
        dex_snapshot=dex_snapshot,
        lifecycle=scout_lifecycle,
        route='ATH',
    )
    try:
        token_risk = token_quarantine_state(db, token_ca, now_ts=now_ts, reclaim=scout_reclaim)
    except Exception as exc:
        token_risk = {'blocked': False, 'reason': 'token_risk_unavailable', 'error': str(exc)}
    entry_mode = _entry_mode_for_ath_uncertainty_reason(reason)
    if (
        _ath_recovery_mode_for_reason(reason) == ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE
        and _ath_recovery_matrix_detail(eval_res.get('scores') or {}, matrix_dissonance=True).get('pass')
    ):
        entry_mode = ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE
    detail = {
        'paper_only_scout': True,
        'probe': True,
        'probe_source': 'ath_uncertainty',
        'source_component': 'matrix_evaluator',
        'source_reject_reason': reason,
        'entry_mode': entry_mode,
        'parent_scout_mode': ATH_UNCERTAINTY_TINY_SCOUT_MODE,
        'entry_trigger_mode': entry_mode,
        'position_size_sol': ATH_UNCERTAINTY_TINY_SCOUT_SIZE_SOL,
        'scores': eval_res.get('scores'),
        'reasons': eval_res.get('reasons'),
        'current_mc': current_mc,
        'liquidity_usd': liquidity_usd,
        'top10_pct': top10_pct,
        'current_reclaim': scout_reclaim,
        'token_risk': token_risk,
    }
    reject_reason = None
    ath_mc_tier = _ath_uncertainty_mc_tier(current_mc)
    detail['ath_mc_tier'] = ath_mc_tier
    detail['ath_mc_thresholds'] = {
        'base_max_mc': ATH_UNCERTAINTY_TINY_SCOUT_MAX_MC,
        'runner_max_mc': ATH_UNCERTAINTY_TINY_SCOUT_RUNNER_MAX_MC,
        'shadow_max_mc': ATH_UNCERTAINTY_TINY_SCOUT_SHADOW_MAX_MC,
    }
    if ath_mc_tier in {'invalid', 'blocked'}:
        reject_reason = 'ath_uncertainty_mc_gate'
    elif ath_mc_tier == 'shadow':
        reject_reason = 'ath_uncertainty_mc_shadow_only'
    elif liquidity_usd < ATH_UNCERTAINTY_TINY_SCOUT_MIN_LIQ_USD:
        reject_reason = 'ath_uncertainty_liquidity_too_low'
    elif top10_pct and top10_pct > 45:
        reject_reason = 'ath_uncertainty_top10_too_high'
    scout_quality = evaluate_scout_quality(
        mode=entry_mode,
        route='ATH',
        trend=dex_snapshot,
        lifecycle=scout_lifecycle,
        token_risk=token_risk,
        position_size_sol=ATH_UNCERTAINTY_TINY_SCOUT_SIZE_SOL,
        current_mc=current_mc,
        liquidity_usd=liquidity_usd,
        top10_pct=top10_pct,
    )
    detail['scout_quality'] = scout_quality
    record_scout_quality_decision(
        db,
        scout_quality=scout_quality,
        token_ca=token_ca,
        symbol=symbol,
        lifecycle_id=lifecycle_id,
        signal_ts=w_entry.get('signal_ts'),
        signal_id=w_entry.get('premium_signal_id'),
        route='ATH',
        lifecycle=scout_lifecycle,
        scout_size={
            'entry_mode': entry_mode,
            'parent_scout_mode': ATH_UNCERTAINTY_TINY_SCOUT_MODE,
            'actual_size_sol': ATH_UNCERTAINTY_TINY_SCOUT_SIZE_SOL,
            'cap_sol': SCOUT_QUALITY_SIZE_CAP_SOL,
        },
        source_component='matrix_evaluator',
        source_reject_reason=reason,
        data_source='matrix_inputs+dexscreener',
        event_ts=now_ts,
    )
    if not reject_reason and not scout_quality.get('pass'):
        reject_reason = scout_quality.get('reason') or 'scout_quality_reject'
    if reject_reason:
        if _discovery_is_soft_quality_reason(reject_reason):
            discovery_mode = _discovery_mode_for_ath_reason(reason)
            track_discovery_candidate(
                db,
                discovery_candidates,
                mode=discovery_mode,
                route='ATH',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=w_entry.get('signal_ts'),
                signal_id=w_entry.get('premium_signal_id'),
                pool=pool,
                watchlist_id=w_entry.get('id'),
                watchlist_entry=w_entry,
                source_component='matrix_evaluator',
                source_reject_reason=reason,
                source_detail={
                    'ath_uncertainty_reject_reason': reject_reason,
                    'scout_quality': scout_quality,
                    'scores': eval_res.get('scores'),
                    'reasons': eval_res.get('reasons'),
                    'current_mc': current_mc,
                    'liquidity_usd': liquidity_usd,
                    'top10_pct': top10_pct,
                },
                lifecycle=scout_lifecycle,
                now_ts=now_ts,
            )
        record_decision_event(
            db,
            component='ath_uncertainty_scout',
            event_type='scout_reject',
            decision='reject',
            reason=reject_reason,
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=w_entry.get('signal_ts'),
            signal_id=w_entry.get('premium_signal_id'),
            route='ATH',
            data_source='matrix_inputs+dexscreener',
            payload=with_lifecycle_payload(detail, scout_lifecycle),
            event_ts=now_ts,
        )
        record_decision_event(
            db,
            component='ath_recovery',
            event_type='candidate_block',
            decision='block',
            reason=reject_reason,
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=w_entry.get('signal_ts'),
            signal_id=w_entry.get('premium_signal_id'),
            route='ATH',
            data_source='ath_uncertainty_scout+dexscreener+scout_quality',
            payload=with_lifecycle_payload({
                **detail,
                'parent_block_reason': reason,
                'candidate_entry_mode': entry_mode,
                'candidate_block_reason': reject_reason,
            }, scout_lifecycle),
            event_ts=now_ts,
        )
        return False

    pending_entries[lifecycle_id] = {
        'token_ca': token_ca,
        'symbol': symbol,
        'signal_ts': w_entry['signal_ts'],
        'premium_signal_id': w_entry.get('premium_signal_id'),
        'signal_type': 'ATH',
        'signal_route': 'ATH',
        'pool': pool,
        'staged_at': time.time(),
        'trigger_price': eval_res.get('current_price') or w_entry.get('signal_price'),
        'watchlist_id': w_entry.get('id'),
        'kelly_position_sol': ATH_UNCERTAINTY_TINY_SCOUT_SIZE_SOL,
        'matrix_scores': eval_res.get('scores') or {},
        'smart_entry_retries': w_entry.get('_smart_entry_retries', 0),
        'w_entry': w_entry,
        'entry_mode': entry_mode,
        'scout_mode': entry_mode,
        'parent_scout_mode': ATH_UNCERTAINTY_TINY_SCOUT_MODE,
        'entry_trigger_mode': entry_mode,
        'paper_only_scout': True,
        'replay_source': 'live_monitor_ath_uncertainty',
        'ath_uncertainty_tiny_scout': True,
        'source_reject_reason': reason,
    }
    record_decision_event(
        db,
        component='ath_uncertainty_scout',
        event_type='pending_entry',
        decision='pending',
        reason=entry_mode,
        token_ca=token_ca,
        symbol=symbol,
        lifecycle_id=lifecycle_id,
        signal_ts=w_entry.get('signal_ts'),
        signal_id=w_entry.get('premium_signal_id'),
        route='ATH',
        data_source='matrix_inputs+dexscreener',
        payload=with_lifecycle_payload(detail, scout_lifecycle),
        event_ts=now_ts,
    )
    return True


def find_ath_real_probe_candidates(db, *, now_ts, limit=3):
    """Find ATH gate misses that earned a tiny, current-reclaim re-test."""
    if not ATH_REAL_PROBE_ENABLED:
        return []
    rows = db.execute(
        """
        WITH ranked AS (
            SELECT
                m.*,
                COALESCE(m.tradable_peak_pnl, m.max_pnl_recorded, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) AS best_pnl,
                COALESCE(m.first_tradable_pnl, m.pnl_15m, m.pnl_5m, 0) AS reclaim_pnl,
                ROW_NUMBER() OVER (
                    PARTITION BY m.token_ca
                    ORDER BY COALESCE(m.tradable_peak_pnl, m.max_pnl_recorded, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) DESC,
                             m.created_event_ts DESC
                ) AS rn
            FROM paper_missed_signal_attribution m
            WHERE m.route = 'ATH'
              AND m.baseline_price IS NOT NULL
              AND COALESCE(m.tradable_missed, 0) = 1
              AND COALESCE(m.would_stop_before_peak, 0) = 0
              AND m.tradability_status = 'tradable_reclaim'
              AND COALESCE(m.tradable_peak_pnl, m.max_pnl_recorded, m.pnl_60m, m.pnl_15m, m.pnl_5m, 0) >= ?
              AND COALESCE(m.first_tradable_pnl, m.pnl_15m, m.pnl_5m, 0) >= ?
              AND m.created_event_ts >= ?
              AND m.component IN ('matrix_evaluator', 'smart_entry')
              AND (
                  m.reject_reason = 'matrices not yet aligned'
                  OR m.reject_reason LIKE 'momentum check failed:%'
                  OR m.reject_reason = 'no_kline_low_volume'
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM paper_decision_events e
                  WHERE e.component = 'ath_probe_live'
                    AND e.token_ca = m.token_ca
                    AND e.event_type IN ('pending_entry', 'reentry_armed')
              )
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        ORDER BY best_pnl DESC, reclaim_pnl DESC
        LIMIT ?
        """,
        (
            ATH_REAL_PROBE_MIN_MAX_PNL,
            ATH_REAL_PROBE_MIN_RECLAIM_PNL,
            now_ts - ATH_REAL_PROBE_MAX_AGE_SEC,
            limit,
        ),
    ).fetchall()
    return rows


def enqueue_ath_real_probe_candidates(db, watchlist, pending_entries, positions, *, now_ts, limit=1, max_positions=None):
    if max_positions is not None and len(positions) + len(pending_entries) >= max_positions:
        return 0
    rows = find_ath_real_probe_candidates(db, now_ts=now_ts, limit=limit)
    if not rows:
        recent_scan = db.execute(
            """
            SELECT 1
            FROM paper_decision_events
            WHERE component = 'ath_probe_live'
              AND event_type = 'scan'
              AND reason = 'no_candidates'
              AND event_ts >= ?
            LIMIT 1
            """,
            (now_ts - 300,),
        ).fetchone()
        if not recent_scan:
            record_decision_event(
                db,
                component='ath_probe_live',
                event_type='scan',
                decision='observe',
                reason='no_candidates',
                route='ATH',
                payload={
                    'max_age_sec': ATH_REAL_PROBE_MAX_AGE_SEC,
                    'min_max_pnl': ATH_REAL_PROBE_MIN_MAX_PNL,
                    'min_reclaim_pnl': ATH_REAL_PROBE_MIN_RECLAIM_PNL,
                    'max_mc': ATH_REAL_PROBE_MAX_MC,
                    'min_liquidity_usd': ATH_REAL_PROBE_MIN_LIQ_USD,
                },
                event_ts=now_ts,
            )
        return 0

    enqueued = 0
    for row in rows:
        token_ca = row['token_ca']
        symbol = row['symbol'] or token_ca[:8]
        lifecycle_id = build_lifecycle_id(token_ca, row['signal_ts'] or int(row['created_event_ts']))
        if max_positions is not None and len(positions) + len(pending_entries) >= max_positions:
            break
        if lifecycle_id in pending_entries or any(pos.token_ca == token_ca for pos in positions.values()):
            record_decision_event(
                db,
                component='ath_probe_live',
                event_type='skip',
                decision='skip',
                reason='already_pending_or_holding',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='ATH',
                payload={'missed_attribution_id': row['id']},
                event_ts=now_ts,
            )
            continue

        recent_wait = db.execute(
            """
            SELECT 1
            FROM paper_decision_events
            WHERE component = 'ath_probe_live'
              AND token_ca = ?
              AND event_type = 'wait_reclaim'
              AND event_ts >= ?
            LIMIT 1
            """,
            (token_ca, now_ts - 60),
        ).fetchone()
        if recent_wait:
            continue

        features = _json_dict(row['lifecycle_features_json'])
        payload = _json_dict(row['payload_json'])
        try:
            reclaim_dex = fetch_dexscreener_trend_snapshot(token_ca)
        except Exception:
            reclaim_dex = None

        current_mc = _first_number(
            (reclaim_dex or {}).get('market_cap'),
            (reclaim_dex or {}).get('fdv'),
            features.get('market_cap'),
            payload.get('market_cap'),
            payload.get('signal_mc'),
        )
        liquidity_usd = _first_number(
            (reclaim_dex or {}).get('liquidity_usd'),
            features.get('liquidity_usd'),
            payload.get('liquidity_usd'),
        )
        if current_mc <= 0 or current_mc >= ATH_REAL_PROBE_MAX_MC:
            record_decision_event(
                db,
                component='ath_probe_live',
                event_type='skip',
                decision='skip',
                reason='ath_probe_mc_gate',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='ATH',
                data_source='dexscreener+missed_attribution',
                payload={
                    'missed_attribution_id': row['id'],
                    'current_mc': current_mc,
                    'max_mc': ATH_REAL_PROBE_MAX_MC,
                    'source_component': row['component'],
                    'source_reject_reason': row['reject_reason'],
                },
                event_ts=now_ts,
            )
            continue
        if liquidity_usd < ATH_REAL_PROBE_MIN_LIQ_USD:
            record_decision_event(
                db,
                component='ath_probe_live',
                event_type='wait_reclaim',
                decision='wait',
                reason='ath_probe_liquidity_too_low',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='ATH',
                data_source='dexscreener+missed_attribution',
                payload={
                    'missed_attribution_id': row['id'],
                    'liquidity_usd': liquidity_usd,
                    'min_liquidity_usd': ATH_REAL_PROBE_MIN_LIQ_USD,
                },
                event_ts=now_ts,
            )
            continue

        probe_entry = {
            'ca': token_ca,
            'symbol': symbol,
            'type': 'ATH',
            'signal_ts': row['signal_ts'] or int(row['created_event_ts']),
            'signal_price': row['baseline_price'],
            'signal_mc': current_mc,
            'signal_vol24h': features.get('vol_h1') or features.get('volume_24h') or 0,
            'signal_tx24h': features.get('tx_m5') or 0,
            'signal_top10': features.get('top10_pct') or 0,
            'added_at': row['created_event_ts'],
        }
        reclaim_lifecycle = lifecycle_payload_for(
            watchlist_entry=probe_entry,
            dex_snapshot=reclaim_dex,
            route='ATH',
            signal_ts=probe_entry['signal_ts'],
            signal_price=row['baseline_price'],
            now=now_ts,
        )
        current_reclaim = evaluate_token_reclaim(
            dex_snapshot=reclaim_dex,
            lifecycle=reclaim_lifecycle,
            route='ATH',
        )
        if not current_reclaim.get('reclaim_confirmed'):
            record_decision_event(
                db,
                component='ath_probe_live',
                event_type='wait_reclaim',
                decision='wait',
                reason=current_reclaim.get('reason') or 'current_reclaim_not_confirmed',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='ATH',
                data_source='dexscreener+lifecycle',
                payload=with_lifecycle_payload({
                    'missed_attribution_id': row['id'],
                    'best_pnl': row['best_pnl'],
                    'reclaim_pnl': row['reclaim_pnl'],
                    'tradability_status': row['tradability_status'],
                    'source_component': row['component'],
                    'source_reject_reason': row['reject_reason'],
                    'current_reclaim': current_reclaim,
                    'current_mc': current_mc,
                    'liquidity_usd': liquidity_usd,
                }, reclaim_lifecycle),
                event_ts=now_ts,
            )
            continue

        token_risk = token_quarantine_state(db, token_ca, now_ts=now_ts, reclaim=current_reclaim)
        if token_risk.get('blocked'):
            record_decision_event(
                db,
                component='ath_probe_live',
                event_type='wait_reclaim',
                decision='wait',
                reason=token_risk.get('reason') or 'token_quarantine',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='ATH',
                data_source='paper_trade_history+dexscreener',
                payload={
                    'missed_attribution_id': row['id'],
                    'token_risk': token_risk,
                    'current_reclaim': current_reclaim,
                },
                event_ts=now_ts,
            )
            continue

        pool = get_pool_address(token_ca)
        if not pool:
            record_decision_event(
                db,
                component='ath_probe_live',
                event_type='skip',
                decision='skip',
                reason='pool_not_found',
                token_ca=token_ca,
                symbol=symbol,
                lifecycle_id=lifecycle_id,
                signal_ts=row['signal_ts'],
                signal_id=row['signal_id'],
                route='ATH',
                payload={'missed_attribution_id': row['id']},
                event_ts=now_ts,
            )
            continue

        w_entry = watchlist.register(
            ca=token_ca,
            symbol=symbol,
            signal_type='ATH',
            pool_address=pool,
            signal_ts=row['signal_ts'] or int(row['created_event_ts']),
            premium_signal_id=row['signal_id'],
            signal_price=row['baseline_price'],
            signal_mc=current_mc,
            signal_super=0,
            signal_holders=0,
            signal_vol24h=probe_entry['signal_vol24h'],
            signal_tx24h=probe_entry['signal_tx24h'],
            signal_top10=probe_entry['signal_top10'],
        )
        if not w_entry:
            continue
        watchlist.update_position_state(w_entry['id'], signal_route='ATH')
        w_entry = watchlist.get_by_id(w_entry['id']) or w_entry
        detail = {
            'probe': True,
            'probe_source': 'missed_attribution',
            'timing_passed': False,
            'timing_gate': 'ath_probe_reentry',
            'missed_attribution_id': row['id'],
            'source_component': row['component'],
            'source_reject_reason': row['reject_reason'],
            'best_pnl': row['best_pnl'],
            'reclaim_pnl': row['reclaim_pnl'],
            'tradability_status': row['tradability_status'],
            'tradability_reason': row['tradability_reason'],
            'first_tradable_horizon': row['first_tradable_horizon'],
            'first_tradable_pnl': row['first_tradable_pnl'],
            'tradable_peak_horizon': row['tradable_peak_horizon'],
            'tradable_peak_pnl': row['tradable_peak_pnl'],
            'current_reclaim': current_reclaim,
            'current_mc': current_mc,
            'liquidity_usd': liquidity_usd,
            'position_size_sol': ATH_REAL_PROBE_SIZE_SOL,
            'entry_mode': 'ath_flat_structure_tiny_scout',
            'paper_only_scout': True,
        }
        pending_entries[lifecycle_id] = {
            'token_ca': token_ca,
            'symbol': symbol,
            'signal_ts': row['signal_ts'] or int(row['created_event_ts']),
            'premium_signal_id': row['signal_id'],
            'signal_type': 'ATH',
            'signal_route': 'ATH',
            'signal_price': row['baseline_price'],
            'market_cap': current_mc,
            'pool': pool,
            'staged_at': time.time(),
            'trigger_price': None,
            'watchlist_id': w_entry['id'],
            'kelly_position_sol': ATH_REAL_PROBE_SIZE_SOL,
            'matrix_scores': {},
            'smart_entry_retries': 0,
            'w_entry': w_entry,
            'momentum_snapshots': [],
            'momentum_pct': 0,
            'first_fire_pc_m5': current_reclaim.get('price_change_m5'),
            'spread_abort_count': 0,
            'entry_mode': 'ath_flat_structure_tiny_scout',
            'scout_mode': 'ath_flat_structure_tiny_scout',
            'paper_only_scout': True,
            'ath_real_probe': detail,
            'replay_source': 'live_monitor_ath_probe',
            'stage_outcome': 'ath_probe_reentry_armed',
        }
        record_decision_event(
            db,
            component='ath_probe_live',
            event_type='reentry_armed',
            decision='pending',
            reason='missed_ath_reclaim_tiny_scout_armed',
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=row['signal_ts'],
            signal_id=row['signal_id'],
            route='ATH',
            data_source='missed_attribution+dexscreener+lifecycle',
            payload=with_lifecycle_payload(detail, reclaim_lifecycle),
            event_ts=now_ts,
        )
        record_decision_event(
            db,
            component='ath_probe_live',
            event_type='pending_entry',
            decision='pending',
            reason='ath_flat_structure_tiny_scout',
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=row['signal_ts'],
            signal_id=row['signal_id'],
            route='ATH',
            data_source='missed_attribution+dexscreener+lifecycle',
            payload=with_lifecycle_payload(detail, reclaim_lifecycle),
            event_ts=now_ts,
        )
        enqueued += 1
    return enqueued


def should_block_lotto_falling_knife(lotto_detail, lotto_lifecycle):
    features = (lotto_lifecycle or {}).get('lifecycle_features') or {}
    lifecycle_state = (lotto_lifecycle or {}).get('lifecycle_state')
    try:
        liquidity = float(features.get('liquidity_usd') if features.get('liquidity_usd') is not None else lotto_detail.get('liquidity_usd') or 0)
    except (TypeError, ValueError):
        liquidity = 0.0
    try:
        price_change_m5 = float(features.get('price_change_m5') if features.get('price_change_m5') is not None else lotto_detail.get('price_change_m5') or 0)
    except (TypeError, ValueError):
        price_change_m5 = 0.0

    if (
        lifecycle_state == 'NEWBORN_LAUNCH'
        and liquidity > 0
        and liquidity < LOTTO_FALLING_KNIFE_LIQ_USD
        and price_change_m5 <= LOTTO_FALLING_KNIFE_M5_PCT
    ):
        return True, {
            'lifecycle_state': lifecycle_state,
            'liquidity_usd': liquidity,
            'price_change_m5': price_change_m5,
            'liq_threshold': LOTTO_FALLING_KNIFE_LIQ_USD,
            'm5_threshold': LOTTO_FALLING_KNIFE_M5_PCT,
        }
    return False, {
        'lifecycle_state': lifecycle_state,
        'liquidity_usd': liquidity,
        'price_change_m5': price_change_m5,
        'liq_threshold': LOTTO_FALLING_KNIFE_LIQ_USD,
        'm5_threshold': LOTTO_FALLING_KNIFE_M5_PCT,
    }


def evaluate_token_reclaim(dex_snapshot=None, lifecycle=None, route=None, risk_profile=None):
    """Require a failed CA to prove current strength before any route can re-enter."""
    dex_snapshot = dex_snapshot or {}
    lifecycle = lifecycle or {}
    features = lifecycle.get('lifecycle_features') or {}

    def _pick_number(name, default=None):
        value = features.get(name)
        if value is None:
            value = dex_snapshot.get(name)
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    state = str(lifecycle.get('lifecycle_state') or 'UNKNOWN').upper()
    entry_bias = str(lifecycle.get('entry_bias') or 'OBSERVE').upper()
    price_change_m5 = _pick_number('price_change_m5')
    buys_m5 = _pick_number('buys_m5')
    sells_m5 = _pick_number('sells_m5')
    tx_m5 = _pick_number('tx_m5')
    if tx_m5 is None and buys_m5 is not None and sells_m5 is not None:
        tx_m5 = buys_m5 + sells_m5
    buy_sell_ratio = _pick_number('buy_sell_ratio')
    if buy_sell_ratio is None and buys_m5 is not None:
        buy_sell_ratio = buys_m5 / max(sells_m5 or 0, 1)
    liquidity_usd = _pick_number('liquidity_usd', 0.0)
    liquidity_unknown = bool(features.get('liquidity_unknown') or dex_snapshot.get('liquidity_unknown'))
    relative_volume = _pick_number('relative_volume')
    if relative_volume is None:
        relative_volume = _pick_number('rvol')
    if relative_volume is None:
        relative_volume = _pick_number('volume_accel')
    if relative_volume is None:
        vol_m5 = _pick_number('vol_m5')
        vol_h1 = _pick_number('vol_h1')
        if vol_m5 is not None and vol_h1 and vol_h1 > 0:
            relative_volume = vol_m5 / max(vol_h1 / 12.0, 1e-9)

    profile = str(risk_profile or 'base').lower()
    m5_threshold = TOKEN_RISK_RECLAIM_M5_PCT
    bs_threshold = TOKEN_RISK_RECLAIM_BS_RATIO
    rvol_threshold = None
    if profile in {'no_follow_failure', 'doa_failure', 'spread_chase_failure'}:
        m5_threshold = TOKEN_RISK_NO_FOLLOW_RECLAIM_M5_PCT
        bs_threshold = TOKEN_RISK_NO_FOLLOW_RECLAIM_BS_RATIO
        rvol_threshold = TOKEN_RISK_NO_FOLLOW_RECLAIM_MIN_RVOL
    elif profile in {'waterfall_memory', 'waterfall_failure'}:
        m5_threshold = TOKEN_RISK_WATERFALL_RECLAIM_M5_PCT
        bs_threshold = TOKEN_RISK_WATERFALL_RECLAIM_BS_RATIO
        rvol_threshold = TOKEN_RISK_WATERFALL_RECLAIM_MIN_RVOL

    detail = {
        'reclaim_confirmed': False,
        'route': route,
        'risk_profile': profile,
        'lifecycle_state': state,
        'entry_bias': entry_bias,
        'price_change_m5': price_change_m5,
        'buy_sell_ratio': buy_sell_ratio,
        'tx_m5': tx_m5,
        'liquidity_usd': liquidity_usd,
        'liquidity_unknown': liquidity_unknown,
        'relative_volume': relative_volume,
        'thresholds': {
            'price_change_m5': m5_threshold,
            'buy_sell_ratio': bs_threshold,
            'tx_m5': TOKEN_RISK_RECLAIM_MIN_TX_M5,
            'liquidity_usd': TOKEN_RISK_RECLAIM_MIN_LIQ_USD,
            'relative_volume': rvol_threshold,
        },
    }

    blocked_states = set(LOTTO_LIFECYCLE_BLOCK_STATES) | {'DEAD', 'DISTRIBUTION', 'DEAD_CAT_BOUNCE', 'ATH_DEEP_RESET'}
    if entry_bias == 'REJECT' or state in blocked_states:
        detail['reason'] = 'reclaim_lifecycle_blocked'
        return detail
    if price_change_m5 is None or price_change_m5 < m5_threshold:
        detail['reason'] = 'reclaim_m5_too_low'
        return detail
    if buy_sell_ratio is None or buy_sell_ratio < bs_threshold:
        detail['reason'] = 'reclaim_buy_sell_too_low'
        return detail
    if rvol_threshold is not None and (relative_volume is None or relative_volume < rvol_threshold):
        detail['reason'] = 'reclaim_relative_volume_too_low'
        return detail
    if tx_m5 is None or tx_m5 < TOKEN_RISK_RECLAIM_MIN_TX_M5:
        detail['reason'] = 'reclaim_tx_too_low'
        return detail
    if liquidity_usd and liquidity_usd < TOKEN_RISK_RECLAIM_MIN_LIQ_USD and not liquidity_unknown:
        detail['reason'] = 'reclaim_liquidity_too_low'
        return detail

    detail['reclaim_confirmed'] = True
    detail['reason'] = 'reclaim_confirmed'
    return detail


TOKEN_RISK_DANGER_REASON_PATTERNS = (
    'hard_sl',
    'hard_floor',
    'no_follow',
    'fast_fail',
    'gap_crash',
    'doa',
    'lotto_sl',
    'stop_loss',
)


def _row_value(row, key, default=None):
    try:
        if key in row.keys():
            return row[key]
    except Exception:
        pass
    return default


def _token_risk_row_is_observation_probe(row):
    entry_mode = str(_row_value(row, 'entry_mode') or '')
    monitor_state = parse_monitor_state(_row_value(row, 'monitor_state_json')) or {}
    if not entry_mode:
        entry_mode = str(monitor_state.get('entryMode') or '')
    size_sol = _safe_float(
        _row_value(row, 'position_size_sol', None),
        _safe_float(monitor_state.get('entrySol'), 0.0),
    )
    replay_source = str(_row_value(row, 'replay_source') or '')
    if size_sol <= 0 or size_sol > PROBE_PROFIT_CAPTURE_MAX_SIZE_SOL:
        return False
    return (
        entry_mode in PAPER_TINY_SCOUT_ENTRY_MODES
        or 'scout' in entry_mode
        or 'probe' in entry_mode
        or 'probe' in replay_source
    )


def _token_risk_probe_loss_should_poison(reason):
    reason = str(reason or '').lower()
    return any(marker in reason for marker in (
        'rug',
        'honeypot',
        'blacklist',
        'freeze',
        'mint_authority',
    ))


def classify_token_risk_exit(row):
    pnl = _safe_float(row['pnl_pct'], 0.0)
    peak = _safe_float(row['peak_pnl'], 0.0) if 'peak_pnl' in row.keys() else 0.0
    reason = (row['exit_reason'] or '').lower()
    category = None
    risk_profile = None
    counts_as_failure = True

    if pnl < 0 and _token_risk_row_is_observation_probe(row) and not _token_risk_probe_loss_should_poison(reason):
        return None

    if 'gap_crash' in reason:
        if pnl > 0:
            category = 'PROFITABLE_VOLATILITY_EXIT'
            risk_profile = 'waterfall_memory'
            counts_as_failure = False
        else:
            category = 'WATERFALL_FAILURE'
            risk_profile = 'waterfall_failure'
    elif 'no_follow' in reason and pnl < 0:
        category = 'NO_FOLLOW_FAILURE'
        risk_profile = 'no_follow_failure'
    elif ('doa' in reason or 'fast_fail' in reason) and pnl < 0:
        category = 'DOA_FAILURE'
        risk_profile = 'doa_failure'
    elif ('hard_sl' in reason or 'hard_floor' in reason or 'stop_loss' in reason or 'lotto_sl' in reason):
        if pnl <= TOKEN_RISK_LOSS_THRESHOLD or pnl < 0:
            category = 'LOSS_FAILURE'
            risk_profile = 'loss_failure'
    elif pnl <= TOKEN_RISK_LOSS_THRESHOLD:
        category = 'LOSS_FAILURE'
        risk_profile = 'loss_failure'

    if category is None:
        return None
    return {
        'row': row,
        'category': category,
        'risk_profile': risk_profile,
        'counts_as_failure': counts_as_failure,
        'pnl': pnl,
        'peak': peak,
    }


def strongest_token_risk_profile(events):
    profiles = {event.get('risk_profile') for event in events}
    if 'waterfall_failure' in profiles:
        return 'waterfall_failure'
    if 'waterfall_memory' in profiles:
        return 'waterfall_memory'
    if 'no_follow_failure' in profiles:
        return 'no_follow_failure'
    if 'doa_failure' in profiles:
        return 'doa_failure'
    if 'spread_chase_failure' in profiles:
        return 'spread_chase_failure'
    return 'loss_failure'


def reclaim_satisfies_token_risk_profile(reclaim, risk_profile):
    if not TOKEN_RISK_RECLAIM_REQUIRED:
        return True, 'reclaim_not_required'
    reclaim = reclaim or {}
    if not reclaim.get('reclaim_confirmed'):
        return False, reclaim.get('reason') or 'reclaim_not_confirmed'
    if risk_profile not in {'no_follow_failure', 'waterfall_memory', 'waterfall_failure'}:
        return True, 'reclaim_confirmed'
    profile_check = evaluate_token_reclaim(
        dex_snapshot=reclaim,
        lifecycle={'lifecycle_state': reclaim.get('lifecycle_state'), 'entry_bias': reclaim.get('entry_bias')},
        route=reclaim.get('route'),
        risk_profile=risk_profile,
    )
    if not profile_check.get('reclaim_confirmed'):
        return False, profile_check.get('reason') or 'risk_profile_reclaim_not_confirmed'
    return True, 'reclaim_confirmed'


def token_quarantine_state(db, token_ca, *, now_ts=None, reclaim=None):
    """Return a global same-CA entry block after recent severe failed exits."""
    if not TOKEN_RISK_QUARANTINE_ENABLED or not token_ca:
        return {'blocked': False, 'reason': 'disabled'}
    now_ts = float(now_ts or time.time())
    cutoff = now_ts - TOKEN_RISK_FAILURE_WINDOW_SEC
    try:
        rows = db.execute(
            """
            SELECT id, symbol, exit_ts, pnl_pct, peak_pnl, exit_reason, replay_source, signal_route,
                   position_size_sol, entry_mode, monitor_state_json
            FROM paper_trades
            WHERE token_ca = ?
              AND exit_ts IS NOT NULL
              AND exit_ts >= ?
            ORDER BY exit_ts DESC
            """,
            (token_ca, cutoff),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        if 'no such column' not in str(exc):
            raise
        rows = db.execute(
            """
            SELECT id, symbol, exit_ts, pnl_pct, peak_pnl, exit_reason, replay_source, signal_route
            FROM paper_trades
            WHERE token_ca = ?
              AND exit_ts IS NOT NULL
              AND exit_ts >= ?
            ORDER BY exit_ts DESC
            """,
            (token_ca, cutoff),
        ).fetchall()
    risk_events = []
    for row in rows:
        event = classify_token_risk_exit(row)
        if event:
            risk_events.append(event)

    if not risk_events:
        return {
            'blocked': False,
            'recent_exit_count': len(rows),
            'severe_failure_count': 0,
            'risk_memory_count': 0,
            'loss_threshold': TOKEN_RISK_LOSS_THRESHOLD,
        }

    latest_event = risk_events[0]
    latest = latest_event['row']
    latest_exit_ts = _safe_float(latest['exit_ts'], 0.0)
    failure_count = sum(1 for event in risk_events if event.get('counts_as_failure'))
    risk_memory_count = len(risk_events) - failure_count
    risk_profile = strongest_token_risk_profile(risk_events)
    cooldown_sec = (
        TOKEN_RISK_REPEAT_COOLDOWN_SEC
        if failure_count >= TOKEN_RISK_REPEAT_FAILURE_COUNT
        else TOKEN_RISK_BASE_COOLDOWN_SEC
    )
    until_ts = latest_exit_ts + cooldown_sec
    if now_ts >= until_ts:
        reclaim_ok, reclaim_reason = reclaim_satisfies_token_risk_profile(reclaim, risk_profile)
        if not reclaim_ok:
            worst_pnl = min((event.get('pnl', 0.0) for event in risk_events), default=None)
            return {
                'blocked': True,
                'reason': 'token_quarantine_reclaim_required',
                'until_ts': until_ts,
                'remaining_sec': 0.0,
                'recent_exit_count': len(rows),
                'severe_failure_count': failure_count,
                'risk_memory_count': risk_memory_count,
                'risk_profile': risk_profile,
                'last_risk_category': latest_event.get('category'),
                'cooldown_expired': True,
                'last_failure_trade_id': latest['id'],
                'last_failure_exit_ts': latest_exit_ts,
                'last_failure_age_sec': now_ts - latest_exit_ts,
                'last_failure_pnl': _safe_float(latest['pnl_pct'], None),
                'last_failure_reason': latest['exit_reason'],
                'worst_failure_pnl': worst_pnl,
                'loss_threshold': TOKEN_RISK_LOSS_THRESHOLD,
                'reclaim_required': True,
                'reclaim': reclaim or {'reclaim_confirmed': False, 'reason': 'reclaim_not_checked'},
                'reclaim_profile_reason': reclaim_reason,
            }
        return {
            'blocked': False,
            'recent_exit_count': len(rows),
            'severe_failure_count': failure_count,
            'risk_memory_count': risk_memory_count,
            'risk_profile': risk_profile,
            'cooldown_expired': True,
            'reclaim_unlocked': bool((reclaim or {}).get('reclaim_confirmed')),
            'last_failure_exit_ts': latest_exit_ts,
            'last_failure_age_sec': now_ts - latest_exit_ts,
            'reclaim': reclaim,
        }

    worst_pnl = min((event.get('pnl', 0.0) for event in risk_events), default=None)
    if risk_profile in {'waterfall_memory', 'waterfall_failure'}:
        reason = 'token_quarantine_waterfall_memory'
    else:
        reason = (
            'token_quarantine_repeat_failure'
            if failure_count >= TOKEN_RISK_REPEAT_FAILURE_COUNT
            else 'token_quarantine_recent_failure'
        )
    return {
        'blocked': True,
        'reason': reason,
        'until_ts': until_ts,
        'remaining_sec': max(0.0, until_ts - now_ts),
        'recent_exit_count': len(rows),
        'severe_failure_count': failure_count,
        'risk_memory_count': risk_memory_count,
        'risk_profile': risk_profile,
        'last_risk_category': latest_event.get('category'),
        'last_failure_trade_id': latest['id'],
        'last_failure_exit_ts': latest_exit_ts,
        'last_failure_pnl': _safe_float(latest['pnl_pct'], None),
        'last_failure_reason': latest['exit_reason'],
        'worst_failure_pnl': worst_pnl,
        'loss_threshold': TOKEN_RISK_LOSS_THRESHOLD,
        'base_cooldown_sec': TOKEN_RISK_BASE_COOLDOWN_SEC,
        'repeat_cooldown_sec': TOKEN_RISK_REPEAT_COOLDOWN_SEC,
        'reclaim_required_after_cooldown': TOKEN_RISK_RECLAIM_REQUIRED,
        'reclaim': reclaim,
    }


def should_block_lotto_lifecycle_entry(lotto_lifecycle):
    lifecycle = lotto_lifecycle or {}
    features = lifecycle.get('lifecycle_features') or {}
    state = str(lifecycle.get('lifecycle_state') or 'UNKNOWN').upper()
    entry_bias = str(lifecycle.get('entry_bias') or '').upper()
    try:
        price_change_m5 = float(features.get('price_change_m5') or 0.0)
    except (TypeError, ValueError):
        price_change_m5 = 0.0

    if entry_bias == 'REJECT':
        return True, 'lotto_lifecycle_entry_bias_reject', {
            'lifecycle_state': state,
            'entry_bias': entry_bias,
            'price_change_m5': price_change_m5,
        }
    if state in LOTTO_LIFECYCLE_BLOCK_STATES:
        return True, f'lotto_lifecycle_block_{state.lower()}', {
            'lifecycle_state': state,
            'entry_bias': entry_bias,
            'price_change_m5': price_change_m5,
            'blocked_states': sorted(LOTTO_LIFECYCLE_BLOCK_STATES),
        }
    if price_change_m5 <= LOTTO_TIMING_BLOCK_M5_PCT:
        return True, 'lotto_timing_negative_m5', {
            'lifecycle_state': state,
            'entry_bias': entry_bias,
            'price_change_m5': price_change_m5,
            'm5_threshold': LOTTO_TIMING_BLOCK_M5_PCT,
        }
    return False, 'lotto_lifecycle_timing_ok', {
        'lifecycle_state': state,
        'entry_bias': entry_bias,
        'price_change_m5': price_change_m5,
        'm5_threshold': LOTTO_TIMING_BLOCK_M5_PCT,
    }


def lifecycle_payload_for(*, signal=None, watchlist_entry=None, dex_snapshot=None, live_concentration=None,
                          route=None, signal_ts=None, signal_price=None, quote_available=None,
                          mark_quote_gap=None, current_pnl=None, peak_pnl=None, now=None):
    try:
        return classify_lifecycle(
            signal=signal,
            watchlist_entry=watchlist_entry,
            dex_snapshot=dex_snapshot,
            live_concentration=live_concentration,
            route=route,
            signal_ts=signal_ts,
            signal_price=signal_price,
            quote_available=quote_available,
            mark_quote_gap=mark_quote_gap,
            current_pnl=current_pnl,
            peak_pnl=peak_pnl,
            now=now,
        ).to_payload()
    except Exception as exc:
        return {
            'lifecycle_state': 'UNKNOWN',
            'vitality_score': None,
            'entry_bias': 'OBSERVE',
            'lifecycle_features': {'error': str(exc)},
            'lifecycle_reasons': ['classifier_error'],
        }


def with_lifecycle_payload(payload, lifecycle):
    data = dict(payload or {})
    if lifecycle:
        data['lifecycle'] = lifecycle
        data['lifecycle_state'] = lifecycle.get('lifecycle_state')
        data['vitality_score'] = lifecycle.get('vitality_score')
        data['entry_bias'] = lifecycle.get('entry_bias')
    return data


def _scout_event_key(row):
    lifecycle_id = row['lifecycle_id'] if 'lifecycle_id' in row.keys() else None
    token_ca = row['token_ca'] if 'token_ca' in row.keys() else None
    signal_ts = row['signal_ts'] if 'signal_ts' in row.keys() else None
    if lifecycle_id:
        return str(lifecycle_id)
    return f"{token_ca or ''}:{signal_ts or ''}"


def _extract_scout_mode(payload=None, reason=None):
    payload = payload if isinstance(payload, dict) else {}
    candidates = [
        payload.get('entry_mode'),
        payload.get('scout_mode'),
        payload.get('probeEntryMode'),
        reason,
    ]
    scout_quality = payload.get('scout_quality')
    if isinstance(scout_quality, dict):
        candidates.extend([scout_quality.get('mode'), scout_quality.get('entry_mode')])
    scout_size = payload.get('scout_size')
    if isinstance(scout_size, dict):
        candidates.extend([scout_size.get('entry_mode'), scout_size.get('mode')])
    entry_decision = payload.get('entryDecision') or payload.get('entry_decision')
    if isinstance(entry_decision, dict):
        candidates.extend([entry_decision.get('entry_mode'), entry_decision.get('mode')])
    lotto_state = payload.get('lottoState') or payload.get('lotto_state')
    if isinstance(lotto_state, dict):
        candidates.extend([
            lotto_state.get('probeEntryMode'),
            lotto_state.get('entry_mode'),
            lotto_state.get('scout_mode'),
        ])
        nested_decision = lotto_state.get('entryDecision')
        if isinstance(nested_decision, dict):
            candidates.extend([nested_decision.get('entry_mode'), nested_decision.get('mode')])
    for candidate in candidates:
        mode = str(candidate or '').strip()
        if mode in PAPER_TINY_SCOUT_ENTRY_MODES:
            return mode
        if mode.endswith('_ok'):
            base = mode[:-3]
            if base in PAPER_TINY_SCOUT_ENTRY_MODES:
                return base
    return None


def _pct(numerator, denominator):
    if not denominator:
        return None
    return round((float(numerator) / float(denominator)) * 100.0, 2)


def record_scout_quality_decision(
    db,
    *,
    scout_quality,
    pending=None,
    token_ca=None,
    symbol=None,
    lifecycle_id=None,
    signal_ts=None,
    signal_id=None,
    strategy_stage=None,
    route=None,
    lifecycle=None,
    scout_size=None,
    source_component=None,
    source_reject_reason=None,
    data_source='dexscreener+lifecycle+paper_risk',
    event_ts=None,
):
    """Record every tiny-scout quality gate pass/block without changing behavior."""
    if not SCOUT_TELEMETRY_ENABLED or not isinstance(scout_quality, dict):
        return False
    pending = pending or {}
    mode = (
        scout_quality.get('mode')
        or pending.get('entry_mode')
        or pending.get('scout_mode')
        or _extract_scout_mode({'scout_quality': scout_quality})
    )
    if mode not in PAPER_TINY_SCOUT_ENTRY_MODES:
        return False
    passed = bool(scout_quality.get('pass'))
    decision = 'pass' if passed else 'block'
    reason = scout_quality.get('reason') or ('scout_quality_pass' if passed else 'scout_quality_reject')
    payload = {
        'entry_mode': mode,
        'actual_entry_mode': pending.get('entry_mode') or mode,
        'parent_scout_mode': pending.get('parent_scout_mode') or pending.get('scout_mode'),
        'entry_trigger_mode': pending.get('entry_trigger_mode') or pending.get('timing_entry_mode'),
        'quality_passed': passed,
        'scout_quality': scout_quality,
        'scout_size': scout_size or {},
        'source_component': source_component,
        'source_reject_reason': source_reject_reason,
    }
    record_decision_event(
        db,
        component='scout_quality',
        event_type='quality_gate',
        decision=decision,
        reason=reason,
        token_ca=token_ca or pending.get('token_ca'),
        symbol=symbol or pending.get('symbol'),
        lifecycle_id=lifecycle_id,
        signal_ts=signal_ts if signal_ts is not None else pending.get('signal_ts'),
        signal_id=signal_id if signal_id is not None else pending.get('premium_signal_id'),
        strategy_stage=strategy_stage,
        route=route or pending.get('signal_route') or pending.get('signal_type'),
        data_source=data_source,
        payload=with_lifecycle_payload(payload, lifecycle),
        event_ts=event_ts,
    )
    return True


DISCOVERY_SOFT_QUALITY_REASONS = {
    'ath_uncertainty_liquidity_too_low',
    'ath_uncertainty_mc_shadow_only',
    'ath_uncertainty_mc_gate',
    'scout_quality_recent_token_failure',
    'scout_quality_liquidity_low',
    'scout_quality_buy_pressure_weak',
    'scout_quality_volume_low',
    'scout_quality_tx_low',
    'scout_quality_negative_trend',
}
DISCOVERY_MATRIX_RECLAIM_REASONS = {
    'matrices not yet aligned',
    'no_kline_low_volume',
}
DISCOVERY_UNKNOWN_DATA_REASONS = {
    'not_ath_prebuy_kline_unknown_data_blocked',
    'backfill_rate_limited',
    'rate_limited',
    'rate_limited_429',
}
DISCOVERY_LOTTO_HIGH_RISK_REASON_PREFIXES = (
    'lotto_observe_low_mc_vol',
    'lotto_volume_unconfirmed',
    'lotto_midcap_activity_unconfirmed',
    'lotto_liq_low_',
    'lotto_newborn_falling_knife_low_liq',
    'lotto_live_top1_',
    'lotto_live_top10_',
    'lotto_top10_',
    'upstream_realtime_liquidity_too_low',
    'upstream_realtime_top1_too_high',
    'upstream_realtime_top10_too_high',
)
LOTTO_NOT_ATH_RECLAIM_SOURCE_REASONS = {
    'not_ath_v17',
    'not_ath_prebuy_kline_unknown_data_blocked',
    'tracking_ttl_expired',
    'trend_bearish_timeout',
    'upstream_probe_mc_gate',
}
LOTTO_LOW_LIQUIDITY_RECLAIM_SOURCE_REASONS = {
    'upstream_realtime_liquidity_too_low',
    'discovery_liquidity_too_low',
    'discovery_liquidity_too_low_final',
    'discovery_lotto_recovery_liquidity_too_low',
    'discovery_lotto_recovery_liquidity_too_low_final',
    'discovery_lotto_high_risk_liquidity_too_low',
    'discovery_lotto_high_risk_liquidity_too_low_final',
    'liquidity_too_low',
    'scout_quality_liquidity_low',
    'liquidity_or_quote_not_ready',
}
LOTTO_MICRO_RECLAIM_SOURCE_REASONS = {
    'weak_buying_pressure',
    'chasing_top',
    'scout_quality_volume_low',
    'scout_quality_negative_trend',
    'scout_quality_buy_pressure_weak',
    'scout_quality_tx_low',
    'score_too_low',
    'no_kline_low_volume',
    'lotto_timing_negative_m5',
}
LOTTO_RECOVERY_TINY_PROBE_MODES = {
    LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE,
    LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE,
    LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE,
}


def _lotto_recovery_family(entry_mode):
    mode = str(entry_mode or '')
    if mode == LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE:
        return 'lotto_not_ath_reclaim'
    if mode == LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE:
        return 'lotto_low_liquidity_reclaim'
    if mode == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE:
        return 'lotto_micro_reclaim'
    return 'other'


def _lotto_recovery_thresholds(mode):
    mode = str(mode or '')
    base = {
        'max_mc': LOTTO_RECLAIM_MAX_MC,
        'min_liquidity_usd': LOTTO_RECLAIM_MIN_LIQ_USD,
        'max_top1_pct': DISCOVERY_LOTTO_HIGH_RISK_EXTREME_TOP1_PCT,
        'max_top10_pct': DISCOVERY_LOTTO_HIGH_RISK_EXTREME_TOP10_PCT,
        'requires_quote': True,
    }
    if mode == LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE:
        return {
            **base,
            'min_buy_sell_ratio': LOTTO_NOT_ATH_RECLAIM_MIN_BS,
            'min_tx_m5': LOTTO_NOT_ATH_RECLAIM_MIN_TX_M5,
            'min_price_change_m5': LOTTO_NOT_ATH_RECLAIM_MIN_PC_M5,
            'min_vol_m5': 5000.0,
        }
    if mode == LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE:
        return {
            **base,
            'min_buy_sell_ratio': LOTTO_LOW_LIQ_RECLAIM_MIN_BS,
            'min_tx_m5': LOTTO_LOW_LIQ_RECLAIM_MIN_TX_M5,
            'min_price_change_m5': LOTTO_LOW_LIQ_RECLAIM_MIN_PC_M5,
            'min_vol_m5': LOTTO_LOW_LIQ_RECLAIM_MIN_VOL_M5,
        }
    if mode == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE:
        return {
            **base,
            'min_buy_sell_ratio': LOTTO_MICRO_RECLAIM_MIN_BS,
            'min_tx_m5': LOTTO_MICRO_RECLAIM_MIN_TX_M5,
            'min_price_change_m5': LOTTO_MICRO_RECLAIM_MIN_BOUNCE_PCT,
            'min_vol_m5': LOTTO_MICRO_RECLAIM_MIN_VOL_M5,
            'max_watch_sec': LOTTO_MICRO_RECLAIM_MAX_WATCH_SEC,
        }
    return base


def _lotto_recovery_activity_gate(
    mode,
    *,
    candidate=None,
    activity=None,
    quote_probe=None,
    current_mc=None,
    liquidity_usd=None,
    top1_pct=None,
    top10_pct=None,
    gmgn_policy=None,
    now_ts=None,
    require_quote=True,
):
    mode = str(mode or '')
    activity = activity or {}
    gmgn_policy = gmgn_policy or {}
    thresholds = _lotto_recovery_thresholds(mode)
    now_ts = float(now_ts or time.time())
    first_seen_ts = float((candidate or {}).get('first_seen_ts') or now_ts)
    age_sec = max(0.0, now_ts - first_seen_ts)
    bs = _first_float_any(activity.get('buy_sell_ratio'), default=0.0) or 0.0
    vol_m5 = _first_float_any(activity.get('vol_m5'), default=0.0) or 0.0
    tx_m5 = _first_float_any(activity.get('tx_m5'), default=0.0) or 0.0
    pc_m5 = _first_float_any(activity.get('price_change_m5'), default=0.0) or 0.0
    mc = _first_number(current_mc)
    liq = _first_number(liquidity_usd)
    top1 = _first_float_any(top1_pct, default=None)
    top10 = _first_float_any(top10_pct, default=None)
    quote_ok = bool((quote_probe or {}).get('success'))
    failures = []
    if not LOTTO_RECOVERY_TINY_PROBES_ENABLED:
        failures.append('lotto_recovery_disabled')
    if mode not in LOTTO_RECOVERY_TINY_PROBE_MODES:
        failures.append('not_lotto_recovery_mode')
    if mc <= 0 or mc >= thresholds['max_mc']:
        failures.append('current_mc_gate')
    if require_quote and liq < thresholds['min_liquidity_usd'] and not quote_ok:
        failures.append('liquidity_or_quote_not_ready')
    if top1 is not None and top1 > thresholds['max_top1_pct']:
        failures.append('top1_extreme')
    if top10 is not None and top10 > thresholds['max_top10_pct']:
        failures.append('top10_extreme')
    if gmgn_policy.get('action') == 'reject':
        failures.append(gmgn_policy.get('reason') or 'gmgn_policy_reject')
    if require_quote and thresholds.get('requires_quote') and not quote_ok:
        failures.append((quote_probe or {}).get('reason') or 'quote_not_executable')
    if bs < thresholds['min_buy_sell_ratio']:
        failures.append('buy_sell_ratio_low')
    if vol_m5 < thresholds['min_vol_m5']:
        failures.append('vol_m5_low')
    if tx_m5 < thresholds['min_tx_m5']:
        failures.append('tx_m5_low')
    if pc_m5 < thresholds['min_price_change_m5']:
        failures.append('price_change_m5_not_reclaimed')
    max_watch_sec = thresholds.get('max_watch_sec')
    if max_watch_sec is not None and age_sec > max_watch_sec:
        failures.append('max_watch_sec_expired')
    return {
        'pass': not failures,
        'reason': f'{_lotto_recovery_family(mode)}_live_reclaim_pass' if not failures else 'lotto_recovery_shadow_activity_not_enough',
        'family': _lotto_recovery_family(mode),
        'failures': failures,
        'observed': {
            'age_sec': age_sec,
            'current_mc': mc,
            'liquidity_usd': liq,
            'buy_sell_ratio': bs,
            'vol_m5': vol_m5,
            'tx_m5': tx_m5,
            'price_change_m5': pc_m5,
            'top1_pct': top1,
            'top10_pct': top10,
            'gmgn_action': gmgn_policy.get('action'),
            'gmgn_reason': gmgn_policy.get('reason'),
            'quote_success': quote_ok,
        },
        'thresholds': thresholds,
        'quote_probe': quote_probe,
    }


def _lotto_dynamic_ttl_extension_detail(
    candidate,
    *,
    dex_snapshot=None,
    lifecycle=None,
    activity=None,
    quote_probe=None,
    require_quote=False,
    now_ts=None,
):
    candidate = candidate or {}
    mode = str(candidate.get('mode') or '')
    if not LOTTO_DYNAMIC_TTL_ENABLED:
        return {'pass': False, 'reason': 'lotto_dynamic_ttl_disabled'}
    if mode not in LOTTO_RECOVERY_TINY_PROBE_MODES:
        return {'pass': False, 'reason': 'not_lotto_recovery_ttl_mode', 'entry_mode': mode}
    if int(candidate.get('ttl_extend_count') or 0) >= LOTTO_DYNAMIC_TTL_MAX_EXTENSIONS:
        return {'pass': False, 'reason': 'lotto_dynamic_ttl_max_extensions', 'entry_mode': mode}
    dex_snapshot = dex_snapshot or {}
    features = (lifecycle or {}).get('lifecycle_features') or {}
    detail = _lotto_recovery_activity_gate(
        mode,
        candidate=candidate,
        activity=activity,
        quote_probe=quote_probe,
        current_mc=_first_number(dex_snapshot.get('market_cap'), dex_snapshot.get('fdv'), features.get('market_cap')),
        liquidity_usd=_first_number(dex_snapshot.get('liquidity_usd'), features.get('liquidity_usd')),
        top1_pct=_first_number(features.get('top1_pct')),
        top10_pct=_first_number(features.get('top10_pct')),
        gmgn_policy=None,
        now_ts=now_ts,
        require_quote=require_quote,
    )
    if not detail.get('pass'):
        detail['reason'] = 'lotto_dynamic_ttl_not_strong'
        return detail
    detail['reason'] = 'lotto_tracking_ttl_extended'
    return detail


def discovery_candidate_key(token_ca, signal_ts, mode):
    return f"{token_ca or ''}:{signal_ts or ''}:{mode or ''}"


def _discovery_is_soft_quality_reason(reason):
    return str(reason or '') in DISCOVERY_SOFT_QUALITY_REASONS


def _discovery_mode_for_ath_reason(reason):
    reason = str(reason or '')
    if _matrix_micro_momentum_reason(reason):
        return MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE
    if reason == 'matrices not yet aligned':
        return MATRIX_RECLAIM_TINY_PROBE_MODE
    if reason == 'no_kline_low_volume':
        return MATRIX_RECLAIM_TINY_PROBE_MODE
    if reason.startswith('momentum check failed') or reason.startswith('momentum check waiting'):
        return ATH_SOFT_RECLAIM_TINY_SCOUT_MODE
    return ATH_SOFT_RECLAIM_TINY_SCOUT_MODE


def _discovery_mode_for_lotto_reason(reason):
    reason = str(reason or '').lower()
    if reason in DISCOVERY_UNKNOWN_DATA_REASONS:
        return UNKNOWN_DATA_ACTIVITY_TINY_SCOUT_MODE
    if reason in LOTTO_NOT_ATH_RECLAIM_SOURCE_REASONS:
        return LOTTO_NOT_ATH_RECLAIM_TINY_PROBE_MODE
    if reason in LOTTO_LOW_LIQUIDITY_RECLAIM_SOURCE_REASONS:
        return LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE
    if reason in LOTTO_MICRO_RECLAIM_SOURCE_REASONS:
        return LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    if reason.startswith((
        'dead_cat_below_high_',
        'lotto_live_top1_',
        'lotto_live_top10_',
        'lotto_top10_',
    )):
        return LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE
    if reason.startswith(DISCOVERY_LOTTO_HIGH_RISK_REASON_PREFIXES):
        if reason.startswith(('lotto_liq_low_', 'upstream_realtime_liquidity_too_low')):
            return LOTTO_LOW_LIQUIDITY_RECLAIM_TINY_PROBE_MODE
        return LOTTO_HIGH_RISK_DISCOVERY_PROBE_MODE
    return None


def _lotto_recovery_mode_for_blocker(*, primary_reason=None, secondary_reason=None, current_mode=None):
    """Pick the live LOTTO recovery mode from the newest blocker, not only the first reject."""
    for reason in (secondary_reason, primary_reason):
        mapped = _discovery_mode_for_lotto_reason(reason)
        if mapped in LOTTO_RECOVERY_TINY_PROBE_MODES:
            return mapped
    if current_mode in LOTTO_RECOVERY_TINY_PROBE_MODES:
        return current_mode
    for reason in (secondary_reason, primary_reason):
        mapped = _discovery_mode_for_lotto_reason(reason)
        if mapped:
            return mapped
    return current_mode


def _retarget_discovery_candidate(
    db,
    discovery_candidates,
    key,
    candidate,
    *,
    new_mode,
    reason,
    now_ts,
    lifecycle=None,
    detail=None,
):
    old_mode = candidate.get('mode')
    if not new_mode or new_mode == old_mode:
        return key
    discovery_candidates.pop(key, None)
    token_ca = candidate.get('token_ca')
    signal_ts = candidate.get('signal_ts')
    new_key = discovery_candidate_key(token_ca, signal_ts, new_mode)
    candidate['key'] = new_key
    candidate['mode'] = new_mode
    candidate['last_check_ts'] = 0.0
    candidate['last_wait_reason'] = reason
    candidate['retarget_count'] = int(candidate.get('retarget_count') or 0) + 1
    candidate['retarget_reason'] = reason
    candidate['retarget_from_mode'] = old_mode
    candidate['source_detail'] = {
        **(candidate.get('source_detail') or {}),
        'previous_mode': old_mode,
        'retarget_reason': reason,
        'retarget_to_mode': new_mode,
        **(detail or {}),
    }
    discovery_candidates[new_key] = candidate
    record_decision_event(
        db,
        component='discovery_tracking',
        event_type='candidate_retarget',
        decision='track',
        reason=reason or 'lotto_recovery_mode_retarget',
        token_ca=token_ca,
        symbol=candidate.get('symbol'),
        lifecycle_id=candidate.get('lifecycle_id'),
        signal_ts=signal_ts,
        signal_id=candidate.get('signal_id'),
        route=candidate.get('route'),
        data_source='discovery_tracking+latest_blocker',
        payload=with_lifecycle_payload({
            'old_mode': old_mode,
            'new_mode': new_mode,
            'source_component': candidate.get('source_component'),
            'source_reject_reason': candidate.get('source_reject_reason'),
            'last_wait_reason': candidate.get('last_wait_reason'),
            'retarget_count': candidate.get('retarget_count'),
            'detail': detail or {},
        }, lifecycle or candidate.get('last_lifecycle') or {}),
        event_ts=now_ts,
    )
    return new_key


def track_discovery_candidate(
    db,
    discovery_candidates,
    *,
    mode,
    route,
    token_ca,
    symbol,
    lifecycle_id,
    signal_ts,
    signal_id=None,
    pool=None,
    watchlist_id=None,
    watchlist_entry=None,
    source_component=None,
    source_reject_reason=None,
    source_detail=None,
    lifecycle=None,
    now_ts=None,
):
    """Put a soft-blocked candidate into the 10-second discovery tracking pool."""
    if not DISCOVERY_TRACKING_ENABLED or discovery_candidates is None or not token_ca or not mode:
        return False
    mode = _ath_recovery_mode_for_candidate(
        mode,
        route=route,
        source_reject_reason=source_reject_reason,
        source_detail=source_detail,
    )
    if mode not in PAPER_TINY_SCOUT_ENTRY_MODES:
        return False
    now_ts = float(now_ts or time.time())
    signal_ts = normalize_signal_ts_seconds(signal_ts) or signal_ts or int(now_ts)
    key = discovery_candidate_key(token_ca, signal_ts, mode)
    existing = discovery_candidates.get(key)
    if existing:
        existing['mode'] = mode
        existing['last_seen_ts'] = now_ts
        existing['source_reject_reason'] = source_reject_reason or existing.get('source_reject_reason')
        existing['source_component'] = source_component or existing.get('source_component')
        existing['source_detail'] = source_detail or existing.get('source_detail') or {}
        if watchlist_entry:
            existing['watchlist_entry'] = dict(watchlist_entry)
            existing['watchlist_id'] = watchlist_entry.get('id') or watchlist_id or existing.get('watchlist_id')
        if lifecycle:
            existing['last_lifecycle'] = lifecycle
        return False

    if len(discovery_candidates) >= DISCOVERY_TRACKING_MAX_CANDIDATES:
        oldest_key = min(
            discovery_candidates,
            key=lambda k: discovery_candidates[k].get('first_seen_ts', now_ts),
        )
        dropped = discovery_candidates.pop(oldest_key, None)
        if dropped:
            record_decision_event(
                db,
                component='discovery_tracking',
                event_type='candidate_expire',
                decision='expire',
                reason='discovery_tracking_capacity_prune',
                token_ca=dropped.get('token_ca'),
                symbol=dropped.get('symbol'),
                lifecycle_id=dropped.get('lifecycle_id'),
                signal_ts=dropped.get('signal_ts'),
                signal_id=dropped.get('signal_id'),
                route=dropped.get('route'),
                payload={
                    'entry_mode': dropped.get('mode'),
                    'age_sec': max(0.0, now_ts - float(dropped.get('first_seen_ts') or now_ts)),
                },
                event_ts=now_ts,
            )

    if mode == ATH_MICRO_RECLAIM_TINY_PROBE_MODE:
        candidate_ttl_sec = ATH_MICRO_RECLAIM_MAX_WATCH_SEC
    elif mode == LOTTO_MICRO_RECLAIM_TINY_PROBE_MODE:
        candidate_ttl_sec = LOTTO_MICRO_RECLAIM_MAX_WATCH_SEC
    else:
        candidate_ttl_sec = DISCOVERY_TRACKING_TTL_SEC
    candidate = {
        'key': key,
        'mode': mode,
        'route': route,
        'token_ca': token_ca,
        'symbol': symbol or token_ca[:8],
        'lifecycle_id': lifecycle_id or build_lifecycle_id(token_ca, signal_ts),
        'signal_ts': signal_ts,
        'signal_id': signal_id,
        'pool': pool,
        'watchlist_id': watchlist_id or (watchlist_entry or {}).get('id'),
        'watchlist_entry': dict(watchlist_entry or {}) if watchlist_entry else None,
        'source_component': source_component,
        'source_reject_reason': source_reject_reason,
        'source_detail': source_detail or {},
        'first_seen_ts': now_ts,
        'last_seen_ts': now_ts,
        'last_check_ts': 0.0,
        'expires_at': now_ts + candidate_ttl_sec,
        'ttl_extend_count': 0,
        'attempts': 0,
        'last_lifecycle': lifecycle,
    }
    discovery_candidates[key] = candidate
    record_decision_event(
        db,
        component='discovery_tracking',
        event_type='candidate_tracked',
        decision='track',
        reason=source_reject_reason or mode,
        token_ca=token_ca,
        symbol=candidate['symbol'],
        lifecycle_id=candidate['lifecycle_id'],
        signal_ts=signal_ts,
        signal_id=signal_id,
        route=route,
        data_source='soft_block',
        payload=with_lifecycle_payload({
            'entry_mode': mode,
            'poll_sec': DISCOVERY_TRACKING_POLL_SEC,
            'ttl_sec': candidate_ttl_sec,
            'ath_recovery_family': _ath_recovery_family(mode),
            'lotto_recovery_family': _lotto_recovery_family(mode),
            'source_component': source_component,
            'source_reject_reason': source_reject_reason,
            'source_detail': source_detail or {},
        }, lifecycle),
        event_ts=now_ts,
    )
    return True


def _discovery_synthetic_watchlist_entry(candidate, dex_snapshot=None):
    dex_snapshot = dex_snapshot or {}
    return {
        'ca': candidate['token_ca'],
        'symbol': candidate.get('symbol') or candidate['token_ca'][:8],
        'type': candidate.get('route') or 'LOTTO',
        'pool_address': candidate.get('pool') or dex_snapshot.get('pair_address'),
        'signal_ts': candidate.get('signal_ts'),
        'premium_signal_id': candidate.get('signal_id'),
        'signal_price': candidate.get('signal_price'),
        'signal_mc': _first_number(
            candidate.get('signal_mc'),
            dex_snapshot.get('market_cap'),
            dex_snapshot.get('fdv'),
        ),
        'signal_super': 0,
        'signal_holders': 0,
        'signal_vol24h': dex_snapshot.get('vol_h1') or 0,
        'signal_tx24h': (dex_snapshot.get('buys_m5') or 0) + (dex_snapshot.get('sells_m5') or 0),
        'signal_top10': candidate.get('top10_pct') or 0,
        'added_at': candidate.get('first_seen_ts') or candidate.get('signal_ts') or time.time(),
    }


def _ath_uncertainty_mc_tier(current_mc):
    current_mc = _first_number(current_mc)
    if current_mc <= 0:
        return 'invalid'
    if current_mc <= ATH_UNCERTAINTY_TINY_SCOUT_MAX_MC:
        return 'base'
    if current_mc <= ATH_UNCERTAINTY_TINY_SCOUT_RUNNER_MAX_MC:
        return 'runner'
    if current_mc <= ATH_UNCERTAINTY_TINY_SCOUT_SHADOW_MAX_MC:
        return 'shadow'
    return 'blocked'


def _first_float_any(*values, default=None):
    for value in values:
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return default


def _discovery_activity_metrics(dex_snapshot=None, lifecycle=None):
    dex_snapshot = dex_snapshot or {}
    features = (lifecycle or {}).get('lifecycle_features') or {}
    buys_m5 = _first_float_any(dex_snapshot.get('buys_m5'), features.get('buys_m5'), default=None)
    sells_m5 = _first_float_any(dex_snapshot.get('sells_m5'), features.get('sells_m5'), default=None)
    tx_m5 = _first_float_any(dex_snapshot.get('tx_m5'), features.get('tx_m5'), default=None)
    if tx_m5 is None and buys_m5 is not None and sells_m5 is not None:
        tx_m5 = buys_m5 + sells_m5
    buy_sell_ratio = _first_float_any(
        dex_snapshot.get('buy_sell_ratio'),
        features.get('buy_sell_ratio'),
        default=None,
    )
    if buy_sell_ratio is None and buys_m5 is not None:
        buy_sell_ratio = buys_m5 / max(sells_m5 or 0.0, 1.0)
    return {
        'buy_sell_ratio': buy_sell_ratio,
        'vol_m5': _first_float_any(dex_snapshot.get('vol_m5'), features.get('vol_m5'), default=None),
        'tx_m5': tx_m5,
        'price_change_m5': _first_float_any(
            dex_snapshot.get('price_change_m5'),
            features.get('price_change_m5'),
            default=None,
        ),
        'buys_m5': buys_m5,
        'sells_m5': sells_m5,
    }


def _discovery_low_liquidity_activity_bypass(mode, *, liquidity_usd, activity=None):
    if not DISCOVERY_LOW_LIQ_BYPASS_ENABLED:
        return None
    if _first_number(liquidity_usd) >= DISCOVERY_MIN_LIQUIDITY_USD:
        return None
    activity = activity or {}
    bs = _first_float_any(activity.get('buy_sell_ratio'), default=0.0) or 0.0
    vol_m5 = _first_float_any(activity.get('vol_m5'), default=0.0) or 0.0
    tx_m5 = _first_float_any(activity.get('tx_m5'), default=0.0) or 0.0
    pc_m5 = _first_float_any(activity.get('price_change_m5'), default=0.0) or 0.0
    thresholds = {
        'min_bs': 1.10,
        'min_vol_m5': 12000.0,
        'min_tx_m5': 120.0,
        'max_negative_m5': -10.0,
    }
    if mode in {ATH_SOFT_RECLAIM_TINY_SCOUT_MODE, MATRIX_RECLAIM_TINY_PROBE_MODE}:
        thresholds = {
            'min_bs': 1.05,
            'min_vol_m5': 5000.0,
            'min_tx_m5': 50.0,
            'max_negative_m5': -5.0,
        }
    elif mode in LOTTO_RECOVERY_TINY_PROBE_MODES:
        recovery_thresholds = _lotto_recovery_thresholds(mode)
        thresholds = {
            'min_bs': recovery_thresholds['min_buy_sell_ratio'],
            'min_vol_m5': recovery_thresholds['min_vol_m5'],
            'min_tx_m5': recovery_thresholds['min_tx_m5'],
            'max_negative_m5': recovery_thresholds['min_price_change_m5'],
        }
    elif mode == LOTTO_HIGH_RISK_DISCOVERY_PROBE_MODE:
        thresholds = {
            'min_bs': 1.05,
            'min_vol_m5': 5000.0,
            'min_tx_m5': 60.0,
            'max_negative_m5': -12.0,
        }
    passed = (
        bs >= thresholds['min_bs']
        and vol_m5 >= thresholds['min_vol_m5']
        and tx_m5 >= thresholds['min_tx_m5']
        and pc_m5 >= thresholds['max_negative_m5']
    )
    extreme_thresholds = {
        'min_bs': DISCOVERY_LOW_LIQ_EXTREME_MIN_BS,
        'min_vol_m5': DISCOVERY_LOW_LIQ_EXTREME_MIN_VOL_M5,
        'min_tx_m5': DISCOVERY_LOW_LIQ_EXTREME_MIN_TX_M5,
        'max_negative_m5': DISCOVERY_LOW_LIQ_EXTREME_MAX_NEG_M5,
    }
    extreme_activity = (
        bs >= extreme_thresholds['min_bs']
        and vol_m5 >= extreme_thresholds['min_vol_m5']
        and tx_m5 >= extreme_thresholds['min_tx_m5']
        and pc_m5 >= extreme_thresholds['max_negative_m5']
    )
    return {
        'pass': passed,
        'reason': 'low_liquidity_activity_bypass' if passed else 'low_liquidity_activity_not_enough',
        'extreme_activity': extreme_activity,
        'live_eligible': bool(passed and extreme_activity),
        'quote_executable': None,
        'observed': {
            'liquidity_usd': liquidity_usd,
            'buy_sell_ratio': bs,
            'vol_m5': vol_m5,
            'tx_m5': tx_m5,
            'price_change_m5': pc_m5,
        },
        'thresholds': thresholds,
        'extreme_thresholds': extreme_thresholds,
    }


def _discovery_quote_probe(token_ca, *, lifecycle_id=None, mode=None, stage_name='discovery_quote_probe'):
    if not DISCOVERY_LOW_LIQ_QUOTE_PROBE_ENABLED:
        return {'attempted': False, 'success': False, 'reason': 'quote_probe_disabled'}
    try:
        execution = simulate_entry_execution(
            token_ca,
            PAPER_TINY_SCOUT_SIZE_SOL,
            stage_name,
            strategy_id=stage_name,
            lifecycle_id=lifecycle_id,
        )
    except Exception as exc:
        return {
            'attempted': True,
            'success': False,
            'reason': 'quote_probe_exception',
            'error': str(exc),
            'entry_mode': mode,
        }
    execution = execution or {}
    return {
        'attempted': True,
        'success': bool(execution.get('success')),
        'reason': 'quote_executable' if execution.get('success') else execution.get('failureReason') or 'quote_not_executable',
        'effective_price': execution.get('effectivePrice'),
        'quoted_out_amount_raw': execution.get('quotedOutAmountRaw'),
        'route_available': execution.get('routeAvailable'),
        'entry_mode': mode,
    }


def _discovery_low_liq_quote_probe(token_ca, *, lifecycle_id=None, mode=None):
    return _discovery_quote_probe(
        token_ca,
        lifecycle_id=lifecycle_id,
        mode=mode,
        stage_name='discovery_low_liq_quote_probe',
    )


def _discovery_unknown_data_live_gate(token_ca, *, lifecycle_id=None, mode=None, activity=None):
    activity = activity or {}
    bs = _first_float_any(activity.get('buy_sell_ratio'), default=0.0) or 0.0
    vol_m5 = _first_float_any(activity.get('vol_m5'), default=0.0) or 0.0
    tx_m5 = _first_float_any(activity.get('tx_m5'), default=0.0) or 0.0
    pc_m5 = _first_float_any(activity.get('price_change_m5'), default=0.0) or 0.0
    thresholds = {
        'min_bs': 1.10,
        'min_vol_m5': 12000.0,
        'min_tx_m5': 100.0,
        'max_negative_m5': -8.0,
    }
    extreme_thresholds = {
        'min_bs': DISCOVERY_LOW_LIQ_EXTREME_MIN_BS,
        'min_vol_m5': DISCOVERY_LOW_LIQ_EXTREME_MIN_VOL_M5,
        'min_tx_m5': DISCOVERY_LOW_LIQ_EXTREME_MIN_TX_M5,
        'max_negative_m5': DISCOVERY_LOW_LIQ_EXTREME_MAX_NEG_M5,
    }
    base_activity = (
        bs >= thresholds['min_bs']
        and vol_m5 >= thresholds['min_vol_m5']
        and tx_m5 >= thresholds['min_tx_m5']
        and pc_m5 >= thresholds['max_negative_m5']
    )
    extreme_activity = (
        bs >= extreme_thresholds['min_bs']
        and vol_m5 >= extreme_thresholds['min_vol_m5']
        and tx_m5 >= extreme_thresholds['min_tx_m5']
        and pc_m5 >= extreme_thresholds['max_negative_m5']
    )
    observed = {
        'buy_sell_ratio': bs,
        'vol_m5': vol_m5,
        'tx_m5': tx_m5,
        'price_change_m5': pc_m5,
    }
    if not base_activity:
        return {
            'pass': False,
            'live_eligible': False,
            'reason': 'unknown_data_activity_not_enough',
            'observed': observed,
            'thresholds': thresholds,
            'extreme_thresholds': extreme_thresholds,
            'quote_probe': None,
        }
    if extreme_activity:
        return {
            'pass': True,
            'live_eligible': True,
            'reason': 'unknown_data_extreme_activity',
            'observed': observed,
            'thresholds': thresholds,
            'extreme_thresholds': extreme_thresholds,
            'quote_probe': None,
        }
    quote_probe = _discovery_quote_probe(
        token_ca,
        lifecycle_id=lifecycle_id,
        mode=mode,
        stage_name='discovery_unknown_data_quote_probe',
    )
    return {
        'pass': bool(quote_probe.get('success')),
        'live_eligible': bool(quote_probe.get('success')),
        'reason': 'unknown_data_quote_executable' if quote_probe.get('success') else 'unknown_data_shadow_quote_or_activity_not_enough',
        'observed': observed,
        'thresholds': thresholds,
        'extreme_thresholds': extreme_thresholds,
        'quote_probe': quote_probe,
    }


def _discovery_lotto_high_risk_live_gate(*, activity=None, liquidity_usd=None, top1_pct=None, top10_pct=None, gmgn_policy=None, low_liquidity_bypass=None):
    activity = activity or {}
    gmgn_policy = gmgn_policy or {}
    bs = _first_float_any(activity.get('buy_sell_ratio'), default=0.0) or 0.0
    vol_m5 = _first_float_any(activity.get('vol_m5'), default=0.0) or 0.0
    tx_m5 = _first_float_any(activity.get('tx_m5'), default=0.0) or 0.0
    liq = _first_number(liquidity_usd)
    top1 = _first_float_any(top1_pct, default=None)
    top10 = _first_float_any(top10_pct, default=None)
    quote_probe_ok = liq >= DISCOVERY_LOTTO_HIGH_RISK_MIN_LIQUIDITY_USD or bool((low_liquidity_bypass or {}).get('live_eligible'))
    thresholds = {
        'min_tx_m5': DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_TX_M5,
        'min_vol_m5': DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_VOL_M5,
        'min_buy_sell_ratio': DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_BS,
        'min_liquidity_usd_or_low_liq_bypass': DISCOVERY_LOTTO_HIGH_RISK_MIN_LIQUIDITY_USD,
        'max_top1_pct': DISCOVERY_LOTTO_HIGH_RISK_LIVE_MAX_TOP1_PCT,
        'max_top10_pct': DISCOVERY_LOTTO_HIGH_RISK_LIVE_MAX_TOP10_PCT,
    }
    failures = []
    if gmgn_policy.get('action') == 'reject':
        failures.append(gmgn_policy.get('reason') or 'gmgn_policy_reject')
    if not quote_probe_ok:
        failures.append('liquidity_or_quote_probe_not_ready')
    if tx_m5 < DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_TX_M5:
        failures.append('tx_m5_low')
    if vol_m5 < DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_VOL_M5:
        failures.append('vol_m5_low')
    if bs < DISCOVERY_LOTTO_HIGH_RISK_LIVE_MIN_BS:
        failures.append('buy_sell_ratio_low')
    if top1 is not None and top1 > DISCOVERY_LOTTO_HIGH_RISK_LIVE_MAX_TOP1_PCT:
        failures.append('top1_high')
    if top10 is not None and top10 > DISCOVERY_LOTTO_HIGH_RISK_LIVE_MAX_TOP10_PCT:
        failures.append('top10_high')
    return {
        'pass': not failures,
        'reason': 'lotto_high_risk_live_activity_pass' if not failures else 'lotto_high_risk_shadow_activity_not_enough',
        'failures': failures,
        'observed': {
            'liquidity_usd': liq,
            'buy_sell_ratio': bs,
            'vol_m5': vol_m5,
            'tx_m5': tx_m5,
            'top1_pct': top1,
            'top10_pct': top10,
            'low_liquidity_bypass': low_liquidity_bypass,
            'gmgn_action': gmgn_policy.get('action'),
            'gmgn_reason': gmgn_policy.get('reason'),
        },
        'thresholds': thresholds,
    }


def _discovery_hard_block(mode, *, current_mc, liquidity_usd, top1_pct=None, top10_pct=None, gmgn_policy=None, low_liquidity_bypass=None):
    gmgn_policy = gmgn_policy or {}
    low_liq_live_eligible = bool((low_liquidity_bypass or {}).get('live_eligible'))
    if gmgn_policy.get('action') == 'reject':
        return gmgn_policy.get('reason') or 'gmgn_policy_reject'
    if liquidity_usd < DISCOVERY_MIN_LIQUIDITY_USD and not low_liq_live_eligible:
        return 'discovery_liquidity_too_low'
    if mode in {
        ATH_SOFT_RECLAIM_TINY_SCOUT_MODE,
        MATRIX_RECLAIM_TINY_PROBE_MODE,
        MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE,
        ATH_RECLAIM_AFTER_FAILURE_TINY_PROBE_MODE,
        ATH_MATRIX_DISSONANCE_TINY_PROBE_MODE,
        ATH_MICRO_RECLAIM_TINY_PROBE_MODE,
    }:
        mc_tier = _ath_uncertainty_mc_tier(current_mc)
        if mc_tier in {'invalid', 'blocked'}:
            return 'discovery_ath_mc_gate'
        if mc_tier == 'shadow':
            return 'discovery_ath_mc_shadow_only'
        if top10_pct and top10_pct > 45:
            return 'discovery_ath_top10_too_high'
    elif mode == UNKNOWN_DATA_ACTIVITY_TINY_SCOUT_MODE:
        if current_mc <= 0 or current_mc >= DISCOVERY_UNKNOWN_ACTIVITY_MAX_MC:
            return 'discovery_unknown_activity_mc_gate'
    elif mode == LOTTO_HIGH_RISK_DISCOVERY_PROBE_MODE:
        if current_mc <= 0 or current_mc >= DISCOVERY_LOTTO_HIGH_RISK_MAX_MC:
            return 'discovery_lotto_high_risk_mc_gate'
        if liquidity_usd < DISCOVERY_LOTTO_HIGH_RISK_MIN_LIQUIDITY_USD and not low_liq_live_eligible:
            return 'discovery_lotto_high_risk_liquidity_too_low'
        if top1_pct and top1_pct > DISCOVERY_LOTTO_HIGH_RISK_EXTREME_TOP1_PCT:
            return 'discovery_lotto_high_risk_top1_extreme'
        if top10_pct and top10_pct > DISCOVERY_LOTTO_HIGH_RISK_EXTREME_TOP10_PCT:
            return 'discovery_lotto_high_risk_top10_extreme'
    elif mode in LOTTO_RECOVERY_TINY_PROBE_MODES:
        if not LOTTO_RECOVERY_TINY_PROBES_ENABLED:
            return 'discovery_lotto_recovery_disabled'
        if current_mc <= 0 or current_mc >= LOTTO_RECLAIM_MAX_MC:
            return 'discovery_lotto_recovery_mc_gate'
        if liquidity_usd < LOTTO_RECLAIM_MIN_LIQ_USD and not low_liq_live_eligible:
            return 'discovery_lotto_recovery_liquidity_too_low'
        if top1_pct and top1_pct > DISCOVERY_LOTTO_HIGH_RISK_EXTREME_TOP1_PCT:
            return 'discovery_lotto_recovery_top1_extreme'
        if top10_pct and top10_pct > DISCOVERY_LOTTO_HIGH_RISK_EXTREME_TOP10_PCT:
            return 'discovery_lotto_recovery_top10_extreme'
    return None


DISCOVERY_LOW_LIQUIDITY_HARD_REASONS = {
    'discovery_liquidity_too_low',
    'discovery_lotto_high_risk_liquidity_too_low',
    'discovery_lotto_recovery_liquidity_too_low',
}


def _discovery_low_liquidity_backoff_detail(candidate, hard_reason, *, now_ts):
    if hard_reason not in DISCOVERY_LOW_LIQUIDITY_HARD_REASONS:
        return {'pass': False, 'reason': 'not_low_liquidity_hard_block'}
    attempts = int((candidate or {}).get('attempts') or 0)
    if attempts >= DISCOVERY_LIQUIDITY_LOW_MAX_RECHECKS:
        return {
            'pass': True,
            'action': 'expire',
            'reason': f'{hard_reason}_final',
            'attempts': attempts,
            'max_rechecks': DISCOVERY_LIQUIDITY_LOW_MAX_RECHECKS,
        }
    next_check_ts = float(now_ts or time.time()) + DISCOVERY_LIQUIDITY_LOW_BACKOFF_SEC
    return {
        'pass': True,
        'action': 'backoff',
        'reason': f'{hard_reason}_backoff',
        'attempts': attempts,
        'max_rechecks': DISCOVERY_LIQUIDITY_LOW_MAX_RECHECKS,
        'backoff_sec': DISCOVERY_LIQUIDITY_LOW_BACKOFF_SEC,
        'next_check_ts': next_check_ts,
    }


def _record_entry_mode_quality_decision(
    db,
    *,
    decision,
    token_ca=None,
    symbol=None,
    lifecycle_id=None,
    signal_ts=None,
    signal_id=None,
    route=None,
    event_type='live_gate',
    data_source='paper_trades',
    event_ts=None,
):
    try:
        record_decision_event(
            db,
            component='entry_mode_quality',
            event_type=event_type,
            decision=decision.get('decision') or 'allow_live',
            reason=decision.get('reason') or 'entry_mode_quality_pass',
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=signal_ts,
            signal_id=signal_id,
            route=route,
            data_source=data_source,
            payload=decision,
            event_ts=event_ts,
        )
    except Exception as exc:
        log.debug(f"  [ENTRY_MODE_QUALITY] record failed: {exc}")


def _entry_mode_quality_allows_live(db, *, entry_mode, token_ca=None, symbol=None, lifecycle_id=None,
                                    signal_ts=None, signal_id=None, route=None, event_ts=None,
                                    data_source='paper_trades', force_live=False):
    decision = evaluate_entry_mode_quality(
        db,
        entry_mode,
        now_ts=event_ts or time.time(),
        force_live=force_live,
    )
    if decision.get('decision') == 'shadow':
        _record_entry_mode_quality_decision(
            db,
            decision=decision,
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=signal_ts,
            signal_id=signal_id,
            route=route,
            event_type='live_gate',
            data_source=data_source,
            event_ts=event_ts,
        )
        log.info(
            f"  [ENTRY_MODE_QUALITY] shadow {symbol or token_ca}: "
            f"mode={entry_mode} reason={decision.get('reason')} "
            f"remaining={decision.get('remaining_sec', 0):.0f}s"
        )
        return False, decision
    if decision.get('reason') != 'entry_mode_quality_insufficient_samples':
        _record_entry_mode_quality_decision(
            db,
            decision=decision,
            token_ca=token_ca,
            symbol=symbol,
            lifecycle_id=lifecycle_id,
            signal_ts=signal_ts,
            signal_id=signal_id,
            route=route,
            event_type='live_gate',
            data_source=data_source,
            event_ts=event_ts,
        )
    return True, decision


def _entry_mode_quality_high_quality_tiny_override(pending=None, *, lifecycle=None, entry_mode=None, route=None):
    """Allow one 0.003 SOL probe through path cooldown when ATH structure is exceptional."""
    if not ENTRY_MODE_QUALITY_HIGH_QUALITY_TINY_OVERRIDE_ENABLED:
        return {'pass': False, 'reason': 'entry_mode_quality_override_disabled'}
    pending = pending or {}
    entry_mode = str(entry_mode or pending.get('entry_mode') or pending.get('scout_mode') or '')
    if not pending_is_paper_tiny_scout(pending):
        return {'pass': False, 'reason': 'not_tiny_scout'}
    route_label = str(route or pending.get('signal_route') or pending.get('signal_type') or '').upper()
    if route_label != 'ATH':
        return {'pass': False, 'reason': 'not_ath_route', 'route': route_label}

    scores = pending.get('matrix_scores') or {}
    try:
        trend = int(float(scores.get('trend', 0) or 0))
        price = int(float(scores.get('price', 0) or 0))
        signal = int(float(scores.get('signal', 0) or 0))
        volume = int(float(scores.get('volume', 0) or 0))
        momentum = int(float(scores.get('momentum', 0) or 0))
    except (TypeError, ValueError):
        trend = price = signal = volume = momentum = 0

    tiny_modes_allowed = {
        ATH_NO_KLINE_TINY_PROBE_MODE,
        ATH_HIGH_MC_TINY_PROBE_MODE,
        'ath_flat_structure_tiny_scout',
        ATH_UNCERTAINTY_TINY_SCOUT_MODE,
        ATH_SOFT_RECLAIM_TINY_SCOUT_MODE,
        MATRIX_RECLAIM_TINY_PROBE_MODE,
        MATRIX_MICRO_MOMENTUM_TINY_PROBE_MODE,
    }
    if PULLBACK_TINY_SCOUT_FORCE_LIVE_ENABLED:
        tiny_modes_allowed.add('pullback_tiny_scout')
    matrix_strong = (
        trend >= ENTRY_MODE_QUALITY_OVERRIDE_MIN_T
        and price >= ENTRY_MODE_QUALITY_OVERRIDE_MIN_P
        and signal >= ENTRY_MODE_QUALITY_OVERRIDE_MIN_S
    )
    flat_structure_strong = (
        entry_mode in {ATH_HIGH_MC_TINY_PROBE_MODE, 'ath_flat_structure_tiny_scout'}
        and trend >= 60
        and price >= ENTRY_MODE_QUALITY_OVERRIDE_MIN_P
        and signal >= ENTRY_MODE_QUALITY_OVERRIDE_MIN_S
    )
    no_kline_strong = (
        entry_mode == ATH_NO_KLINE_TINY_PROBE_MODE
        and trend >= 60
        and price >= ENTRY_MODE_QUALITY_OVERRIDE_MIN_P
        and signal >= ENTRY_MODE_QUALITY_OVERRIDE_MIN_S
    )
    if entry_mode not in tiny_modes_allowed:
        return {'pass': False, 'reason': 'mode_not_overrideable', 'entry_mode': entry_mode}
    if not (matrix_strong or flat_structure_strong or no_kline_strong):
        return {
            'pass': False,
            'reason': 'matrix_not_strong_enough',
            'entry_mode': entry_mode,
            'scores': scores,
            'thresholds': {
                'trend': ENTRY_MODE_QUALITY_OVERRIDE_MIN_T,
                'price': ENTRY_MODE_QUALITY_OVERRIDE_MIN_P,
                'signal': ENTRY_MODE_QUALITY_OVERRIDE_MIN_S,
            },
        }

    return {
        'pass': True,
        'reason': 'entry_mode_quality_high_quality_tiny_override',
        'entry_mode': entry_mode,
        'route': route_label,
        'scores': {
            'trend': trend,
            'volume': volume,
            'price': price,
            'signal': signal,
            'momentum': momentum,
        },
    }


def _lotto_pullback_has_strong_activity(lifecycle):
    features = (lifecycle or {}).get('lifecycle_features') or {}
    bs = _first_float_any(
        features.get('buy_sell_ratio_m5'),
        features.get('buy_sell_ratio'),
        features.get('bs_m5'),
        default=0.0,
    ) or 0.0
    vol_m5 = _first_float_any(
        features.get('volume_m5'),
        features.get('vol_m5'),
        features.get('volume_5m'),
        default=0.0,
    ) or 0.0
    tx_m5 = _first_float_any(
        features.get('tx_m5'),
        features.get('txns_m5'),
        default=0.0,
    ) or 0.0
    return {
        'pass': (
            bs >= LOTTO_PULLBACK_STRONG_MIN_BS
            and vol_m5 >= LOTTO_PULLBACK_STRONG_MIN_VOL_M5
            and tx_m5 >= LOTTO_PULLBACK_STRONG_MIN_TX_M5
        ),
        'observed': {
            'buy_sell_ratio': bs,
            'vol_m5': vol_m5,
            'tx_m5': tx_m5,
        },
        'thresholds': {
            'min_buy_sell_ratio': LOTTO_PULLBACK_STRONG_MIN_BS,
            'min_vol_m5': LOTTO_PULLBACK_STRONG_MIN_VOL_M5,
            'min_tx_m5': LOTTO_PULLBACK_STRONG_MIN_TX_M5,
        },
    }


def _build_discovery_pending(w_entry, candidate, lifecycle_id, mode, detail):
    route = str(candidate.get('route') or w_entry.get('type') or '').upper()
    size_sol = PAPER_TINY_SCOUT_SIZE_SOL
    recovery_family = _ath_recovery_family(mode)
    parent_block_reason = _ath_recovery_parent_reason(candidate, detail.get('source_detail') or candidate.get('source_detail') or {})
    recovery_probe_reason = (detail.get('ath_recovery_gate') or {}).get('reason')
    matrix_scores = _ath_recovery_scores(candidate, detail)
    if route == 'LOTTO':
        lotto_recovery_family = _lotto_recovery_family(mode) if mode in LOTTO_RECOVERY_TINY_PROBE_MODES else None
        lotto_probe_reason = (detail.get('lotto_recovery_gate') or {}).get('reason')
        parent_reason = (
            candidate.get('source_reject_reason')
            or (detail.get('source_detail') or {}).get('original_source_reject_reason')
            or (detail.get('source_detail') or {}).get('source_reject_reason')
        )
        lotto_detail = {
            **detail,
            'entry_mode': mode,
            'position_size_sol': size_sol,
            'paper_only_scout': True,
            'probe': True,
            'probe_source': 'discovery_tracking',
            'timing_passed': True,
        }
        pending = build_lotto_pending(w_entry, lifecycle_id, detail=lotto_detail)
        pending['kelly_position_sol'] = size_sol
        pending['entry_mode'] = mode
        pending['scout_mode'] = mode
        pending['paper_only_scout'] = True
        pending['replay_source'] = 'live_monitor_discovery_probe'
        pending['stage_outcome'] = 'discovery_probe_entered'
        pending['source_component'] = candidate.get('source_component')
        pending['source_reject_reason'] = candidate.get('source_reject_reason')
        pending['lotto_recovery_family'] = lotto_recovery_family
        pending['parent_block_reason'] = parent_reason
        pending['recovery_probe_reason'] = lotto_probe_reason
        pending['lotto_state']['probe'] = True
        pending['lotto_state']['probeSource'] = 'discovery_tracking'
        pending['lotto_state']['probeEntryMode'] = mode
        pending['lotto_state']['paper_only_scout'] = True
        if lotto_recovery_family:
            pending['lotto_state']['lottoRecoveryFamily'] = lotto_recovery_family
            pending['lotto_state']['parentBlockReason'] = parent_reason
            pending['lotto_state']['recoveryProbeReason'] = lotto_probe_reason
        pending['lotto_state']['discoveryCandidate'] = {
            'mode': mode,
            'source_component': candidate.get('source_component'),
            'source_reject_reason': candidate.get('source_reject_reason'),
            'first_seen_ts': candidate.get('first_seen_ts'),
            'attempts': candidate.get('attempts'),
            'lotto_recovery_family': lotto_recovery_family,
            'parent_block_reason': parent_reason,
            'recovery_probe_reason': lotto_probe_reason,
        }
        return pending

    return {
        'token_ca': w_entry['ca'],
        'symbol': w_entry['symbol'],
        'signal_ts': w_entry.get('signal_ts') or candidate.get('signal_ts'),
        'premium_signal_id': w_entry.get('premium_signal_id') or candidate.get('signal_id'),
        'signal_type': route or 'ATH',
        'signal_route': route or 'ATH',
        'signal_price': w_entry.get('signal_price'),
        'market_cap': detail.get('current_mc') or w_entry.get('signal_mc') or 0,
        'pool': w_entry.get('pool_address') or candidate.get('pool'),
        'staged_at': time.time(),
        'trigger_price': None,
        'watchlist_id': w_entry.get('id'),
        'kelly_position_sol': size_sol,
        'matrix_scores': matrix_scores,
        'smart_entry_retries': 0,
        'w_entry': w_entry,
        'entry_mode': mode,
        'scout_mode': mode,
        'paper_only_scout': True,
        'ath_recovery_family': recovery_family,
        'parent_block_reason': parent_block_reason,
        'recovery_probe_reason': recovery_probe_reason,
        'timing_passed': True,
        'replay_source': 'live_monitor_discovery_probe',
        'stage_outcome': 'discovery_probe_entered',
        'source_component': candidate.get('source_component'),
        'source_reject_reason': candidate.get('source_reject_reason'),
        'discovery_candidate': {
            'mode': mode,
            'source_component': candidate.get('source_component'),
            'source_reject_reason': candidate.get('source_reject_reason'),
            'first_seen_ts': candidate.get('first_seen_ts'),
            'attempts': candidate.get('attempts'),
            'ath_recovery_family': recovery_family,
            'parent_block_reason': parent_block_reason,
            'recovery_probe_reason': recovery_probe_reason,
            'detail': detail,
        },
        'momentum_snapshots': [],
        'momentum_pct': 0,
        'first_fire_pc_m5': (detail.get('current_reclaim') or {}).get('price_change_m5'),
        'spread_abort_count': 0,
    }


def process_discovery_tracking_candidates(
    db,
    watchlist,
    discovery_candidates,
    pending_entries,
    positions,
    *,
    now_ts,
    max_positions=None,
):
    """Evaluate active discovery candidates every 10 seconds and arm tiny probes on reclaim."""
    if not DISCOVERY_TRACKING_ENABLED or not discovery_candidates:
        return 0
    now_ts = float(now_ts or time.time())
    armed = 0
    evaluated = 0

    for key, candidate in list(discovery_candidates.items()):
        if armed >= DISCOVERY_TRACKING_MAX_ARMS_PER_CYCLE:
            break
        if evaluated >= DISCOVERY_TRACKING_MAX_EVALS_PER_CYCLE:
            break
        if max_positions is not None and len(positions) + len(pending_entries) >= max_positions:
            break
        token_ca = candidate.get('token_ca')
        mode = candidate.get('mode')
        route = str(candidate.get('route') or '').upper()
        lifecycle_id = candidate.get('lifecycle_id') or build_lifecycle_id(token_ca, candidate.get('signal_ts'))
        if not token_ca or not mode:
            discovery_candidates.pop(key, None)
            continue
        if lifecycle_id in pending_entries or any(pos.token_ca == token_ca for pos in positions.values()):
            candidate['last_wait_reason'] = 'already_pending_or_holding'
            record_decision_event(
                db,
                component='discovery_tracking',
                event_type='candidate_recheck',
                decision='wait',
                reason='already_pending_or_holding',
                token_ca=token_ca,
                symbol=candidate.get('symbol'),
                lifecycle_id=lifecycle_id,
                signal_ts=candidate.get('signal_ts'),
                signal_id=candidate.get('signal_id'),
                route=route,
                payload={
                    'entry_mode': mode,
                    'age_sec': max(0.0, now_ts - float(candidate.get('first_seen_ts') or now_ts)),
                    'attempts': candidate.get('attempts'),
                    'pending': lifecycle_id in pending_entries,
                    'holding': any(pos.token_ca == token_ca for pos in positions.values()),
                },
                event_ts=now_ts,
            )
            continue
        if now_ts >= float(candidate.get('expires_at') or now_ts):
            if route == 'LOTTO' and not candidate.get('lotto_ttl_retargeted'):
                retarget_mode = _lotto_recovery_mode_for_blocker(
                    primary_reason=candidate.get('source_reject_reason'),
                    secondary_reason=candidate.get('last_wait_reason') or 'tracking_ttl_expired',
                    current_mode=mode,
                )
                if retarget_mode and retarget_mode != mode:
                    candidate['lotto_ttl_retargeted'] = True
                    candidate['expires_at'] = now_ts + DISCOVERY_TRACKING_POLL_SEC
                    _retarget_discovery_candidate(
                        db,
                        discovery_candidates,
                        key,
                        candidate,
                        new_mode=retarget_mode,
                        reason='lotto_ttl_latest_blocker_retarget',
                        now_ts=now_ts,
                        detail={
                            'ttl_reason': 'tracking_ttl_expired',
                            'latest_blocker': candidate.get('last_wait_reason'),
                            'source_reject_reason': candidate.get('source_reject_reason'),
                        },
                    )
                    continue
            if route == 'ATH' and ATH_DYNAMIC_TTL_ENABLED:
                try:
                    ttl_dex_snapshot = fetch_dexscreener_trend_snapshot(token_ca) or {}
                except Exception:
                    ttl_dex_snapshot = {}
                ttl_w_entry = candidate.get('watchlist_entry')
                if not ttl_w_entry:
                    try:
                        ttl_w_entry = watchlist.get_by_ca(token_ca)
                    except Exception:
                        ttl_w_entry = None
                ttl_lifecycle = lifecycle_payload_for(
                    watchlist_entry=ttl_w_entry,
                    dex_snapshot=ttl_dex_snapshot,
                    route=route,
                    signal_ts=candidate.get('signal_ts'),
                    signal_price=candidate.get('signal_price'),
                    now=now_ts,
                )
                ttl_activity = _discovery_activity_metrics(ttl_dex_snapshot, ttl_lifecycle)
                ttl_detail = _ath_dynamic_ttl_extension_detail(
                    candidate,
                    dex_snapshot=ttl_dex_snapshot,
                    lifecycle=ttl_lifecycle,
                    activity=ttl_activity,
                )
                ttl_quote_probe = None
                if ttl_detail.get('pass'):
                    ttl_quote_probe = _discovery_quote_probe(
                        token_ca,
                        lifecycle_id=lifecycle_id,
                        mode=mode,
                        stage_name='ath_dynamic_ttl_quote_probe',
                    )
                    ttl_detail = _ath_dynamic_ttl_extension_detail(
                        candidate,
                        dex_snapshot=ttl_dex_snapshot,
                        lifecycle=ttl_lifecycle,
                        activity=ttl_activity,
                        quote_probe=ttl_quote_probe,
                    )
                candidate['last_ath_strength_snapshot'] = ttl_detail
                if ttl_detail.get('pass'):
                    candidate['ttl_extend_count'] = int(candidate.get('ttl_extend_count') or 0) + 1
                    candidate['ttl_extend_reason'] = ttl_detail.get('reason')
                    candidate['expires_at'] = now_ts + ATH_DYNAMIC_TTL_EXTEND_SEC
                    record_decision_event(
                        db,
                        component='discovery_tracking',
                        event_type='candidate_recheck',
                        decision='wait',
                        reason='ath_tracking_ttl_extended',
                        token_ca=token_ca,
                        symbol=candidate.get('symbol'),
                        lifecycle_id=lifecycle_id,
                        signal_ts=candidate.get('signal_ts'),
                        signal_id=candidate.get('signal_id'),
                        route=route,
                        data_source='dexscreener+lifecycle+quote_probe',
                        payload=with_lifecycle_payload({
                            'entry_mode': mode,
                            'age_sec': max(0.0, now_ts - float(candidate.get('first_seen_ts') or now_ts)),
                            'attempts': candidate.get('attempts'),
                            'ttl_extend_count': candidate.get('ttl_extend_count'),
                            'ttl_extend_sec': ATH_DYNAMIC_TTL_EXTEND_SEC,
                            'last_ath_strength_snapshot': ttl_detail,
                            'quote_probe': ttl_quote_probe,
                        }, ttl_lifecycle),
                        event_ts=now_ts,
                    )
                    continue
            if route == 'LOTTO' and mode in LOTTO_RECOVERY_TINY_PROBE_MODES and LOTTO_DYNAMIC_TTL_ENABLED:
                try:
                    ttl_dex_snapshot = fetch_dexscreener_trend_snapshot(token_ca) or {}
                except Exception:
                    ttl_dex_snapshot = {}
                ttl_w_entry = candidate.get('watchlist_entry')
                if not ttl_w_entry:
                    try:
                        ttl_w_entry = watchlist.get_by_ca(token_ca)
                    except Exception:
                        ttl_w_entry = None
                ttl_lifecycle = lifecycle_payload_for(
                    watchlist_entry=ttl_w_entry,
                    dex_snapshot=ttl_dex_snapshot,
                    route=route,
                    signal_ts=candidate.get('signal_ts'),
                    signal_price=candidate.get('signal_price'),
                    now=now_ts,
                )
                ttl_activity = _discovery_activity_metrics(ttl_dex_snapshot, ttl_lifecycle)
                ttl_detail = _lotto_dynamic_ttl_extension_detail(
                    candidate,
                    dex_snapshot=ttl_dex_snapshot,
                    lifecycle=ttl_lifecycle,
                    activity=ttl_activity,
                    now_ts=now_ts,
                )
                ttl_quote_probe = None
                if ttl_detail.get('pass'):
                    ttl_quote_probe = _discovery_quote_probe(
                        token_ca,
                        lifecycle_id=lifecycle_id,
                        mode=mode,
                        stage_name='lotto_dynamic_ttl_quote_probe',
                    )
                    ttl_detail = _lotto_dynamic_ttl_extension_detail(
                        candidate,
                        dex_snapshot=ttl_dex_snapshot,
                        lifecycle=ttl_lifecycle,
                        activity=ttl_activity,
                        quote_probe=ttl_quote_probe,
                        require_quote=True,
                        now_ts=now_ts,
                    )
                candidate['last_lotto_strength_snapshot'] = ttl_detail
                if ttl_detail.get('pass'):
                    candidate['ttl_extend_count'] = int(candidate.get('ttl_extend_count') or 0) + 1
                    candidate['ttl_extend_reason'] = ttl_detail.get('reason')
                    candidate['expires_at'] = now_ts + LOTTO_DYNAMIC_TTL_EXTEND_SEC
                    record_decision_event(
                        db,
                        component='discovery_tracking',
                        event_type='candidate_recheck',
                        decision='wait',
                        reason='lotto_tracking_ttl_extended',
                        token_ca=token_ca,
                        symbol=candidate.get('symbol'),
                        lifecycle_id=lifecycle_id,
                        signal_ts=candidate.get('signal_ts'),
                        signal_id=candidate.get('signal_id'),
                        route=route,
                        data_source='dexscreener+lifecycle+quote_probe',
                        payload=with_lifecycle_payload({
                            'entry_mode': mode,
                            'age_sec': max(0.0, now_ts - float(candidate.get('first_seen_ts') or now_ts)),
                            'attempts': candidate.get('attempts'),
                            'ttl_extend_count': candidate.get('ttl_extend_count'),
                            'ttl_extend_sec': LOTTO_DYNAMIC_TTL_EXTEND_SEC,
                            'last_lotto_strength_snapshot': ttl_detail,
                            'quote_probe': ttl_quote_probe,
                        }, ttl_lifecycle),
                        event_ts=now_ts,
                    )
                    continue
            if DISCOVERY_FINAL_RECLAIM_ENABLED and not candidate.get('final_reclaim_attempted'):
                candidate['final_reclaim_attempted'] = True
                candidate['expires_at'] = now_ts + DISCOVERY_TRACKING_POLL_SEC
                record_decision_event(
                    db,
                    component='discovery_tracking',
                    event_type='candidate_recheck',
                    decision='wait',
                    reason='tracking_ttl_final_reclaim_check',
                    token_ca=token_ca,
                    symbol=candidate.get('symbol'),
                    lifecycle_id=lifecycle_id,
                    signal_ts=candidate.get('signal_ts'),
                    signal_id=candidate.get('signal_id'),
                    route=route,
                    payload={
                        'entry_mode': mode,
                        'age_sec': max(0.0, now_ts - float(candidate.get('first_seen_ts') or now_ts)),
                        'attempts': candidate.get('attempts'),
                        'last_wait_reason': candidate.get('last_wait_reason'),
                        'ttl_extend_count': candidate.get('ttl_extend_count'),
                        'last_ath_strength_snapshot': candidate.get('last_ath_strength_snapshot'),
                        'last_lotto_strength_snapshot': candidate.get('last_lotto_strength_snapshot'),
                    },
                    event_ts=now_ts,
                )
            else:
                discovery_candidates.pop(key, None)
                record_decision_event(
                    db,
                    component='discovery_tracking',
                    event_type='candidate_expire',
                    decision='expire',
                    reason='tracking_ttl_expired',
                    token_ca=token_ca,
                    symbol=candidate.get('symbol'),
                    lifecycle_id=lifecycle_id,
                    signal_ts=candidate.get('signal_ts'),
                    signal_id=candidate.get('signal_id'),
                    route=route,
                    payload={
                        'entry_mode': mode,
                        'age_sec': max(0.0, now_ts - float(candidate.get('first_seen_ts') or now_ts)),
                        'attempts': candidate.get('attempts'),
                        'last_wait_reason': candidate.get('last_wait_reason'),
                        'final_reclaim_attempted': bool(candidate.get('final_reclaim_attempted')),
                        'ttl_extend_count': candidate.get('ttl_extend_count'),
                        'last_ath_strength_snapshot': candidate.get('last_ath_strength_snapshot'),
                        'last_lotto_strength_snapshot': candidate.get('last_lotto_strength_snapshot'),
                    },
                    event_ts=now_ts,
                )
                continue
        if now_ts - float(candidate.get('last_check_ts') or 0.0) < DISCOVERY_TRACKING_POLL_SEC:
            continue

        candidate['last_check_ts'] = now_ts
        candidate['attempts'] = int(candidate.get('attempts') or 0) + 1
        evaluated += 1

        try:
            dex_snapshot = fetch_dexscreener_trend_snapshot(token_ca)
        except Exception:
            dex_snapshot = None
        dex_snapshot = dex_snapshot or {}
        w_entry = None
        if candidate.get('watchlist_id'):
            try:
                w_entry = watchlist.get_by_id(candidate['watchlist_id'])
            except Exception:
                w_entry = None
        if not w_entry:
            w_entry = candidate.get('watchlist_entry') or watchlist.get_by_ca(token_ca)
        synthetic_entry = _discovery_synthetic_watchlist_entry(candidate, dex_snapshot)
        entry_for_lifecycle = w_entry or synthetic_entry
        pool = (
            candidate.get('pool')
            or (w_entry or {}).get('pool_address')
            or dex_snapshot.get('pair_address')
            or get_pool_address(token_ca)
        )
        if not pool:
            candidate['last_wait_reason'] = 'pool_not_found'
            record_decision_event(
                db,
                component='discovery_tracking',
                event_type='candidate_recheck',
                decision='wait',
                reason='pool_not_found',
                token_ca=token_ca,
                symbol=candidate.get('symbol'),
                lifecycle_id=lifecycle_id,
                signal_ts=candidate.get('signal_ts'),
                signal_id=candidate.get('signal_id'),
                route=route,
                payload={'entry_mode': mode, 'attempts': candidate['attempts']},
                event_ts=now_ts,
            )
            continue

        live_concentration = None
        gmgn_enrichment = None
        gmgn_policy = None
        if route == 'LOTTO' or mode in {UNKNOWN_DATA_ACTIVITY_TINY_SCOUT_MODE, LOTTO_HIGH_RISK_DISCOVERY_PROBE_MODE}:
            try:
                live_concentration = helius_token_concentration(token_ca)
            except Exception:
                live_concentration = None
            try:
                gmgn_enrichment = fetch_gmgn_token_enrichment(token_ca)
            except Exception:
                gmgn_enrichment = None

        lifecycle = lifecycle_payload_for(
            watchlist_entry=entry_for_lifecycle,
            dex_snapshot=dex_snapshot,
            live_concentration=live_concentration,
            route=route,
            signal_ts=candidate.get('signal_ts'),
            signal_price=(w_entry or {}).get('signal_price') or candidate.get('signal_price'),
            quote_available=None,
            now=now_ts,
        )
        current_reclaim = evaluate_token_reclaim(
            dex_snapshot=dex_snapshot,
            lifecycle=lifecycle,
            route=route,
        )
        features = lifecycle.get('lifecycle_features') or {}
        current_mc = _first_number(
            dex_snapshot.get('market_cap'),
            dex_snapshot.get('fdv'),
            features.get('market_cap'),
            (w_entry or {}).get('signal_mc'),
            candidate.get('signal_mc'),
        )
        liquidity_usd = _first_number(
            dex_snapshot.get('liquidity_usd'),
            features.get('liquidity_usd'),
        )
        top1_pct = _first_number(
            (live_concentration or {}).get('top1_pct'),
            features.get('top1_pct'),
        )
        top10_pct = _first_number(
            (live_concentration or {}).get('top10_pct'),
            (w_entry or {}).get('signal_top10'),
            features.get('top10_pct'),
        )
        activity = _discovery_activity_metrics(dex_snapshot, lifecycle)
        low_liquidity_bypass = _discovery_low_liquidity_activity_bypass(
            mode,
            liquidity_usd=liquidity_usd,
            activity=activity,
        )
        if (
            low_liquidity_bypass
            and low_liquidity_bypass.get('pass')
            and liquidity_usd < DISCOVERY_MIN_LIQUIDITY_USD
            and not low_liquidity_bypass.get('live_eligible')
        ):
            quote_probe = _discovery_low_liq_quote_probe(
                token_ca,
                lifecycle_id=lifecycle_id,
                mode=mode,
            )
            low_liquidity_bypass['quote_probe'] = quote_probe
            low_liquidity_bypass['quote_executable'] = bool(quote_probe.get('success'))
            low_liquidity_bypass['live_eligible'] = bool(quote_probe.get('success'))
        effective_liquidity_usd = (
            DISCOVERY_MIN_LIQUIDITY_USD
            if (low_liquidity_bypass or {}).get('live_eligible') and liquidity_usd < DISCOVERY_MIN_LIQUIDITY_USD
            else liquidity_usd
        )
        detail = {
            'paper_only_scout': True,
            'probe': True,
            'probe_source': 'discovery_tracking',
            'entry_mode': mode,
            'position_size_sol': PAPER_TINY_SCOUT_SIZE_SOL,
            'source_component': candidate.get('source_component'),
            'source_reject_reason': candidate.get('source_reject_reason'),
            'source_detail': candidate.get('source_detail') or {},
            'first_seen_ts': candidate.get('first_seen_ts'),
            'age_sec': max(0.0, now_ts - float(candidate.get('first_seen_ts') or now_ts)),
            'attempts': candidate.get('attempts'),
            'current_mc': current_mc,
            'liquidity_usd': liquidity_usd,
            'effective_liquidity_usd': effective_liquidity_usd,
            'low_liquidity_bypass': low_liquidity_bypass,
            'activity': activity,
            'top1_pct': top1_pct,
            'top10_pct': top10_pct,
            'current_reclaim': current_reclaim,
            'gmgn_readonly': gmgn_enrichment,
        }
        if gmgn_enrichment is not None:
            gmgn_policy = evaluate_gmgn_lotto_policy(
                gmgn_enrichment,
                detail,
                lifecycle=lifecycle,
                entry_mode=mode,
            )
            detail['gmgn_policy'] = gmgn_policy
            detail['gmgn_action'] = gmgn_policy.get('action')
            detail['gmgn_reason'] = gmgn_policy.get('reason')

        high_risk_live_gate = None
        if mode == LOTTO_HIGH_RISK_DISCOVERY_PROBE_MODE:
            high_risk_live_gate = _discovery_lotto_high_risk_live_gate(
                activity=activity,
                liquidity_usd=liquidity_usd,
                top1_pct=top1_pct,
                top10_pct=top10_pct,
                gmgn_policy=gmgn_policy,
                low_liquidity_bypass=low_liquidity_bypass,
            )
            detail['lotto_high_risk_live_gate'] = high_risk_live_gate

        try:
            token_risk = token_quarantine_state(db, token_ca, now_ts=now_ts, reclaim=current_reclaim)
        except Exception as exc:
            token_risk = {'blocked': False, 'reason': 'token_risk_unavailable', 'error': str(exc)}
        detail['token_risk'] = token_risk
        hard_reason = _discovery_hard_block(
            mode,
            current_mc=current_mc,
            liquidity_usd=liquidity_usd,
            top1_pct=top1_pct,
            top10_pct=top10_pct,
            gmgn_policy=gmgn_policy,
            low_liquidity_bypass=low_liquidity_bypass,
        )
        if hard_reason:
            if route == 'LOTTO':
                retarget_mode = _lotto_recovery_mode_for_blocker(
                    primary_reason=candidate.get('source_reject_reason'),
                    secondary_reason=hard_reason,
                    current_mode=mode,
                )
                if retarget_mode and retarget_mode != mode:
                    _retarget_discovery_candidate(
                        db,
                        discovery_candidates,
                        key,
                        candidate,
                        new_mode=retarget_mode,
                        reason='lotto_latest_hard_blocker_retarget',
                        now_ts=now_ts,
                        lifecycle=lifecycle,
                        detail={
                            'hard_reason': hard_reason,
                            'source_reject_reason': candidate.get('source_reject_reason'),
                            'old_mode': mode,
                        },
                    )
                    continue
            low_liquidity_backoff = _discovery_low_liquidity_backoff_detail(
                candidate,
                hard_reason,
                now_ts=now_ts,
            )
            if low_liquidity_backoff.get('pass'):
                detail['low_liquidity_backoff'] = low_liquidity_backoff
                candidate['last_wait_reason'] = low_liquidity_backoff.get('reason') or hard_reason
                if low_liquidity_backoff.get('action') == 'backoff':
                    next_check_ts = float(low_liquidity_backoff.get('next_check_ts') or now_ts)
                    candidate['last_check_ts'] = next_check_ts - DISCOVERY_TRACKING_POLL_SEC
                    record_decision_event(
                        db,
                        component='discovery_tracking',
                        event_type='candidate_recheck',
                        decision='wait',
                        reason=low_liquidity_backoff.get('reason') or hard_reason,
                        token_ca=token_ca,
                        symbol=candidate.get('symbol'),
                        lifecycle_id=lifecycle_id,
                        signal_ts=candidate.get('signal_ts'),
                        signal_id=candidate.get('signal_id'),
                        route=route,
                        data_source='dexscreener+gmgn+helius+lifecycle',
                        payload=with_lifecycle_payload(detail, lifecycle),
                        event_ts=now_ts,
                    )
                    continue
                if low_liquidity_backoff.get('action') == 'expire':
                    discovery_candidates.pop(key, None)
                    record_decision_event(
                        db,
                        component='discovery_tracking',
                        event_type='candidate_expire',
                        decision='expire',
                        reason=low_liquidity_backoff.get('reason') or hard_reason,
                        token_ca=token_ca,
                        symbol=candidate.get('symbol'),
                        lifecycle_id=lifecycle_id,
                        signal_ts=candidate.get('signal_ts'),
                        signal_id=candidate.get('signal_id'),
                        route=route,
                        data_source='dexscreener+gmgn+helius+lifecycle',
                        payload=with_lifecycle_payload(detail, lifecycle),
                        event_ts=now_ts,
                    )
                    continue
            candidate['last_wait_reason'] = hard_reason
            record_decision_event(
                db,
                component='discovery_tracking',
                event_type='candidate_recheck',
                decision='wait',
                reason=hard_reason,
                token_ca=token_ca,
                symbol=candidate.get('symbol'),
                lifecycle_id=lifecycle_id,
                signal_ts=candidate.get('signal_ts'),
                signal_id=candidate.get('signal_id'),
                route=route,
                data_source='dexscreener+gmgn+helius+lifecycle',
                payload=with_lifecycle_payload(detail, lifecycle),
                event_ts=now_ts,
            )
            continue
        unknown_data_live_gate = None
        if mode == UNKNOWN_DATA_ACTIVITY_TINY_SCOUT_MODE:
            unknown_data_live_gate = _discovery_unknown_data_live_gate(
                token_ca,
                lifecycle_id=lifecycle_id,
                mode=mode,
                activity=activity,
            )
            detail['unknown_data_live_gate'] = unknown_data_live_gate
            if not unknown_data_live_gate.get('pass'):
                candidate['last_wait_reason'] = unknown_data_live_gate.get('reason')
                record_decision_event(
                    db,
                    component='discovery_tracking',
                    event_type='candidate_recheck',
                    decision='shadow',
                    reason=unknown_data_live_gate.get('reason') or 'unknown_data_shadow',
                    token_ca=token_ca,
                    symbol=candidate.get('symbol'),
                    lifecycle_id=lifecycle_id,
                    signal_ts=candidate.get('signal_ts'),
                    signal_id=candidate.get('signal_id'),
                    route=route,
                    data_source='discovery_tracking+dexscreener+quote_probe',
                    payload=with_lifecycle_payload(detail, lifecycle),
                    event_ts=now_ts,
                )
                continue
        if (low_liquidity_bypass or {}).get('live_eligible') and liquidity_usd < DISCOVERY_MIN_LIQUIDITY_USD:
            record_decision_event(
                db,
                component='discovery_tracking',
                event_type='candidate_recheck',
                decision='warn',
                reason='low_liq_quote_executable_probe_candidate',
                token_ca=token_ca,
                symbol=candidate.get('symbol'),
                lifecycle_id=lifecycle_id,
                signal_ts=candidate.get('signal_ts'),
                signal_id=candidate.get('signal_id'),
                route=route,
                data_source='dexscreener+lifecycle',
                payload=with_lifecycle_payload(detail, lifecycle),
                event_ts=now_ts,
            )
        if high_risk_live_gate and not high_risk_live_gate.get('pass'):
            candidate['last_wait_reason'] = high_risk_live_gate.get('reason')
            record_decision_event(
                db,
                component='discovery_tracking',
                event_type='candidate_recheck',
                decision='shadow',
                reason=high_risk_live_gate.get('reason') or 'lotto_high_risk_shadow',
                token_ca=token_ca,
                symbol=candidate.get('symbol'),
                lifecycle_id=lifecycle_id,
                signal_ts=candidate.get('signal_ts'),
                signal_id=candidate.get('signal_id'),
                route=route,
                data_source='discovery_tracking+dexscreener+gmgn+helius+lifecycle',
                payload=with_lifecycle_payload(detail, lifecycle),
                event_ts=now_ts,
            )
            continue
        if token_risk.get('blocked') and not token_risk.get('cooldown_expired'):
            candidate['last_wait_reason'] = token_risk.get('reason') or 'token_quarantine'
            record_decision_event(
                db,
                component='discovery_tracking',
                event_type='candidate_recheck',
                decision='wait',
                reason=token_risk.get('reason') or 'token_quarantine',
                token_ca=token_ca,
                symbol=candidate.get('symbol'),
                lifecycle_id=lifecycle_id,
                signal_ts=candidate.get('signal_ts'),
                signal_id=candidate.get('signal_id'),
                route=route,
                data_source='paper_trade_history+dexscreener+lifecycle',
                payload=with_lifecycle_payload(detail, lifecycle),
                event_ts=now_ts,
            )
            continue

        lotto_recovery_gate = None
        lotto_recovery_quote_probe = None
        if mode in LOTTO_RECOVERY_TINY_PROBE_MODES:
            lotto_recovery_quote_probe = _discovery_quote_probe(
                token_ca,
                lifecycle_id=lifecycle_id,
                mode=mode,
                stage_name=f'{mode}_quote_probe',
            )
            detail['lotto_recovery_quote_probe'] = lotto_recovery_quote_probe
            lotto_recovery_gate = _lotto_recovery_activity_gate(
                mode,
                candidate=candidate,
                activity=activity,
                quote_probe=lotto_recovery_quote_probe,
                current_mc=current_mc,
                liquidity_usd=liquidity_usd,
                top1_pct=top1_pct,
                top10_pct=top10_pct,
                gmgn_policy=gmgn_policy,
                now_ts=now_ts,
                require_quote=True,
            )
            detail['lotto_recovery_gate'] = lotto_recovery_gate
            if not lotto_recovery_gate.get('pass'):
                candidate['last_wait_reason'] = lotto_recovery_gate.get('reason')
                record_decision_event(
                    db,
                    component='lotto_recovery',
                    event_type='candidate_recheck',
                    decision='wait',
                    reason=lotto_recovery_gate.get('reason') or 'lotto_recovery_wait',
                    token_ca=token_ca,
                    symbol=candidate.get('symbol'),
                    lifecycle_id=lifecycle_id,
                    signal_ts=candidate.get('signal_ts'),
                    signal_id=candidate.get('signal_id'),
                    route=route,
                    data_source='discovery_tracking+dexscreener+quote_probe+lifecycle',
                    payload=with_lifecycle_payload(detail, lifecycle),
                    event_ts=now_ts,
                )
                continue
            record_decision_event(
                db,
                component='lotto_recovery',
                event_type='candidate_recheck',
                decision='pass',
                reason=lotto_recovery_gate.get('reason') or 'lotto_recovery_pass',
                token_ca=token_ca,
                symbol=candidate.get('symbol'),
                lifecycle_id=lifecycle_id,
                signal_ts=candidate.get('signal_ts'),
                signal_id=candidate.get('signal_id'),
                route=route,
                data_source='discovery_tracking+dexscreener+quote_probe+lifecycle',
                payload=with_lifecycle_payload(detail, lifecycle),
                event_ts=now_ts,
            )

        ath_recovery_gate = None
        ath_recovery_quote_probe = None
        if mode in ATH_RECOVERY_TINY_PROBE_MODES:
            ath_recovery_quote_probe = _discovery_quote_probe(
                token_ca,
                lifecycle_id=lifecycle_id,
                mode=mode,
                stage_name=f'{mode}_quote_probe',
            )
            detail['ath_recovery_quote_probe'] = ath_recovery_quote_probe
            ath_recovery_gate = _ath_recovery_eligibility(
                db,
                entry_mode=mode,
                candidate=candidate,
                detail=detail,
                route=route,
                token_risk=token_risk,
                current_reclaim=current_reclaim,
                dex_snapshot=dex_snapshot,
                lifecycle=lifecycle,
                activity=activity,
                liquidity_usd=liquidity_usd,
                top1_pct=top1_pct,
                top10_pct=top10_pct,
                quote_probe=ath_recovery_quote_probe,
                now_ts=now_ts,
            )
            detail['ath_recovery_gate'] = ath_recovery_gate
            if not ath_recovery_gate.get('pass'):
                candidate['last_wait_reason'] = ath_recovery_gate.get('reason')
                record_decision_event(
                    db,
                    component='ath_recovery',
                    event_type='candidate_recheck',
                    decision='wait',
                    reason=ath_recovery_gate.get('reason') or 'ath_recovery_wait',
                    token_ca=token_ca,
                    symbol=candidate.get('symbol'),
                    lifecycle_id=lifecycle_id,
                    signal_ts=candidate.get('signal_ts'),
                    signal_id=candidate.get('signal_id'),
                    route=route,
                    data_source='discovery_tracking+dexscreener+lifecycle+paper_risk',
                    payload=with_lifecycle_payload(detail, lifecycle),
                    event_ts=now_ts,
                )
                continue
            record_decision_event(
                db,
                component='ath_recovery',
                event_type='candidate_recheck',
                decision='pass',
                reason=ath_recovery_gate.get('reason') or 'ath_recovery_pass',
                token_ca=token_ca,
                symbol=candidate.get('symbol'),
                lifecycle_id=lifecycle_id,
                signal_ts=candidate.get('signal_ts'),
                signal_id=candidate.get('signal_id'),
                route=route,
                data_source='discovery_tracking+dexscreener+lifecycle+paper_risk',
                payload=with_lifecycle_payload(detail, lifecycle),
                event_ts=now_ts,
            )

        scout_quality = evaluate_scout_quality(
            mode=mode,
            route=route,
            trend=dex_snapshot,
            lifecycle=lifecycle,
            gmgn=gmgn_policy,
            token_risk=token_risk,
            live_concentration=live_concentration,
            position_size_sol=PAPER_TINY_SCOUT_SIZE_SOL,
            current_mc=current_mc,
            liquidity_usd=effective_liquidity_usd,
            top1_pct=top1_pct,
            top10_pct=top10_pct,
        )
        detail['scout_quality'] = scout_quality
        record_scout_quality_decision(
            db,
            scout_quality=scout_quality,
            token_ca=token_ca,
            symbol=candidate.get('symbol'),
            lifecycle_id=lifecycle_id,
            signal_ts=candidate.get('signal_ts'),
            signal_id=candidate.get('signal_id'),
            route=route,
            lifecycle=lifecycle,
            scout_size={
                'entry_mode': mode,
                'actual_size_sol': PAPER_TINY_SCOUT_SIZE_SOL,
                'cap_sol': SCOUT_QUALITY_SIZE_CAP_SOL,
            },
            source_component=candidate.get('source_component') or 'discovery_tracking',
            source_reject_reason=candidate.get('source_reject_reason'),
            data_source='discovery_tracking+dexscreener+lifecycle+paper_risk',
            event_ts=now_ts,
        )
        if not scout_quality.get('pass'):
            wait_reason = scout_quality.get('reason') or 'scout_quality_reject'
            if route == 'LOTTO':
                retarget_mode = _lotto_recovery_mode_for_blocker(
                    primary_reason=candidate.get('source_reject_reason'),
                    secondary_reason=wait_reason,
                    current_mode=mode,
                )
                if retarget_mode and retarget_mode != mode:
                    _retarget_discovery_candidate(
                        db,
                        discovery_candidates,
                        key,
                        candidate,
                        new_mode=retarget_mode,
                        reason='lotto_latest_scout_quality_retarget',
                        now_ts=now_ts,
                        lifecycle=lifecycle,
                        detail={
                            'scout_quality_reason': wait_reason,
                            'source_reject_reason': candidate.get('source_reject_reason'),
                            'old_mode': mode,
                            'scout_quality': scout_quality,
                        },
                    )
                    continue
            candidate['last_wait_reason'] = wait_reason
            record_decision_event(
                db,
                component='discovery_tracking',
                event_type='candidate_recheck',
                decision='wait',
                reason=wait_reason,
                token_ca=token_ca,
                symbol=candidate.get('symbol'),
                lifecycle_id=lifecycle_id,
                signal_ts=candidate.get('signal_ts'),
                signal_id=candidate.get('signal_id'),
                route=route,
                data_source='discovery_tracking+dexscreener+lifecycle+paper_risk',
                payload=with_lifecycle_payload(detail, lifecycle),
                event_ts=now_ts,
            )
            continue

        live_allowed, entry_mode_quality = _entry_mode_quality_allows_live(
            db,
            entry_mode=mode,
            token_ca=token_ca,
            symbol=candidate.get('symbol'),
            lifecycle_id=lifecycle_id,
            signal_ts=candidate.get('signal_ts'),
            signal_id=candidate.get('signal_id'),
            route=route,
            event_ts=now_ts,
            data_source='discovery_tracking+paper_trades',
        )
        detail['entry_mode_quality'] = entry_mode_quality
        if not live_allowed:
            candidate['last_wait_reason'] = entry_mode_quality.get('reason') or 'entry_mode_shadow'
            record_decision_event(
                db,
                component='discovery_tracking',
                event_type='candidate_recheck',
                decision='shadow',
                reason='entry_mode_quality_shadow',
                token_ca=token_ca,
                symbol=candidate.get('symbol'),
                lifecycle_id=lifecycle_id,
                signal_ts=candidate.get('signal_ts'),
                signal_id=candidate.get('signal_id'),
                route=route,
                data_source='discovery_tracking+paper_trades',
                payload=with_lifecycle_payload(detail, lifecycle),
                event_ts=now_ts,
            )
            continue

        if not w_entry:
            w_entry = watchlist.register(
                ca=token_ca,
                symbol=candidate.get('symbol') or token_ca[:8],
                signal_type=route or synthetic_entry['type'],
                pool_address=pool,
                signal_ts=candidate.get('signal_ts') or int(now_ts),
                premium_signal_id=candidate.get('signal_id'),
                signal_price=candidate.get('signal_price'),
                signal_mc=current_mc,
                signal_super=0,
                signal_holders=0,
                signal_vol24h=dex_snapshot.get('vol_h1') or 0,
                signal_tx24h=(dex_snapshot.get('buys_m5') or 0) + (dex_snapshot.get('sells_m5') or 0),
                signal_top10=top10_pct or 0,
            )
        if not w_entry:
            continue
        try:
            watchlist.update_position_state(w_entry['id'], signal_route=route)
        except Exception:
            pass
        w_entry = watchlist.get_by_id(w_entry['id']) or w_entry
        w_entry['pool_address'] = w_entry.get('pool_address') or pool
        pending = _build_discovery_pending(w_entry, candidate, lifecycle_id, mode, detail)
        pending_entries[lifecycle_id] = pending
        discovery_candidates.pop(key, None)
        record_decision_event(
            db,
            component='discovery_tracking',
            event_type='pending_entry',
            decision='pending',
            reason=mode,
            token_ca=token_ca,
            symbol=w_entry.get('symbol') or candidate.get('symbol'),
            lifecycle_id=lifecycle_id,
            signal_ts=candidate.get('signal_ts'),
            signal_id=candidate.get('signal_id'),
            route=route,
            data_source='discovery_tracking+dexscreener+gmgn+helius+lifecycle',
            payload=with_lifecycle_payload(detail, lifecycle),
            event_ts=now_ts,
        )
        armed += 1
    return armed


def record_scout_funnel_summary(db, *, now_ts, lookback_sec=None):
    """Summarize tiny-scout candidate -> quality -> pending -> SmartEntry -> fill conversion."""
    if not SCOUT_TELEMETRY_ENABLED:
        return None
    lookback_sec = int(lookback_sec or SCOUT_FUNNEL_LOOKBACK_SEC)
    since_ts = float(now_ts) - lookback_sec
    rows = db.execute(
        """
        SELECT event_ts, token_ca, symbol, lifecycle_id, signal_ts, route,
               component, event_type, decision, reason, payload_json
        FROM paper_decision_events
        WHERE event_ts >= ?
          AND (
              event_type IN (
                  'scout_candidate', 'pending_entry', 'reentry_armed',
                  'quality_gate', 'entry_quote', 'entry_abort',
                  'entry_spread_warning', 'timing_decision', 'scout_reject',
                  'candidate_tracked', 'candidate_recheck', 'candidate_expire'
              )
              OR component IN (
                  'scout_quality', 'lotto_entry_gate', 'lotto_upstream_probe_live',
                  'lotto_upstream_realtime_scout', 'ath_uncertainty_scout',
                  'smart_entry', 'execution_api', 'execution_guard',
                  'lotto_timing_gate', 'token_risk', 'discovery_tracking',
                  'ath_recovery'
              )
          )
        ORDER BY event_ts ASC
        LIMIT 25000
        """,
        (since_ts,),
    ).fetchall()

    key_to_mode = {}
    for row in rows:
        payload = _json_dict(row['payload_json'])
        key = _scout_event_key(row)
        mode = _extract_scout_mode(payload, row['reason'])
        if mode:
            key_to_mode[key] = mode

    def new_stats():
        return {
            'candidate': set(),
            'explicit_candidate': set(),
            'quality_evaluated': set(),
            'quality_pass': set(),
            'quality_block': set(),
            'pending': set(),
            'smart_entry_pass': set(),
            'smart_entry_reject': set(),
            'quote_success': set(),
            'quote_fail': set(),
            'execution_abort': set(),
            'scout_reject': set(),
            'tokens': set(),
            'quality_block_reasons': Counter(),
            'smart_entry_reject_reasons': Counter(),
            'quote_fail_reasons': Counter(),
            'execution_abort_reasons': Counter(),
            'scout_reject_reasons': Counter(),
        }

    by_mode = defaultdict(new_stats)
    for row in rows:
        payload = _json_dict(row['payload_json'])
        key = _scout_event_key(row)
        mode = _extract_scout_mode(payload, row['reason']) or key_to_mode.get(key)
        if mode not in PAPER_TINY_SCOUT_ENTRY_MODES:
            continue
        stats = by_mode[mode]
        if row['token_ca']:
            stats['tokens'].add(row['token_ca'])
        component = row['component']
        event_type = row['event_type']
        decision = str(row['decision'] or '').lower()
        reason = row['reason'] or 'unknown'

        if event_type in {'scout_candidate', 'quality_gate', 'pending_entry', 'reentry_armed', 'candidate_tracked', 'candidate_recheck'}:
            stats['candidate'].add(key)
        if event_type in {'scout_candidate', 'candidate_tracked'}:
            stats['explicit_candidate'].add(key)
        if component == 'scout_quality' and event_type == 'quality_gate':
            stats['quality_evaluated'].add(key)
            if decision == 'pass':
                stats['quality_pass'].add(key)
            elif decision == 'block':
                stats['quality_block'].add(key)
                stats['quality_block_reasons'][reason] += 1
        elif event_type == 'pending_entry' and decision == 'pending':
            stats['pending'].add(key)
        elif event_type == 'reentry_armed' and decision == 'pending':
            stats['pending'].add(key)
        elif component == 'smart_entry' and event_type == 'timing_decision':
            if decision == 'pass':
                stats['smart_entry_pass'].add(key)
            elif decision == 'reject':
                stats['smart_entry_reject'].add(key)
                stats['smart_entry_reject_reasons'][reason] += 1
        elif component == 'execution_api' and event_type == 'entry_quote':
            if decision == 'filled_paper':
                stats['quote_success'].add(key)
            elif decision == 'fail':
                stats['quote_fail'].add(key)
                stats['quote_fail_reasons'][reason] += 1
        elif component == 'execution_guard' and event_type == 'entry_abort':
            stats['execution_abort'].add(key)
            stats['execution_abort_reasons'][reason] += 1
        elif event_type == 'scout_reject':
            stats['scout_reject'].add(key)
            stats['scout_reject_reasons'][reason] += 1

    try:
        trade_rows = db.execute(
            """
            SELECT lifecycle_id, token_ca, symbol, signal_ts, entry_mode
            FROM paper_trades
            WHERE entry_ts >= ?
              AND entry_mode IS NOT NULL
            """,
            (since_ts,),
        ).fetchall()
    except Exception:
        trade_rows = []
    for row in trade_rows:
        mode = str(row['entry_mode'] or '')
        if mode not in PAPER_TINY_SCOUT_ENTRY_MODES:
            continue
        key = row['lifecycle_id'] or f"{row['token_ca'] or ''}:{row['signal_ts'] or ''}"
        stats = by_mode[mode]
        stats['candidate'].add(key)
        stats['quote_success'].add(key)
        if row['token_ca']:
            stats['tokens'].add(row['token_ca'])

    mode_summaries = []
    totals = new_stats()
    for mode, stats in sorted(by_mode.items()):
        candidate_n = len(stats['candidate'])
        quality_eval_n = len(stats['quality_evaluated'])
        quality_pass_n = len(stats['quality_pass'])
        pending_n = len(stats['pending'])
        smart_entry_pass_n = len(stats['smart_entry_pass'])
        fill_n = len(stats['quote_success'])
        mode_summaries.append({
            'entry_mode': mode,
            'candidate_n': candidate_n,
            'explicit_candidate_n': len(stats['explicit_candidate']),
            'quality_evaluated_n': quality_eval_n,
            'quality_pass_n': quality_pass_n,
            'quality_block_n': len(stats['quality_block']),
            'pending_n': pending_n,
            'smart_entry_pass_n': smart_entry_pass_n,
            'smart_entry_reject_n': len(stats['smart_entry_reject']),
            'quote_success_n': fill_n,
            'quote_fail_n': len(stats['quote_fail']),
            'execution_abort_n': len(stats['execution_abort']),
            'scout_reject_n': len(stats['scout_reject']),
            'unique_token_n': len(stats['tokens']),
            'quality_pass_rate_pct': _pct(quality_pass_n, quality_eval_n),
            'pending_per_candidate_pct': _pct(pending_n, candidate_n),
            'smart_entry_pass_per_pending_pct': _pct(smart_entry_pass_n, pending_n),
            'fill_per_smart_entry_pass_pct': _pct(fill_n, smart_entry_pass_n),
            'fill_per_candidate_pct': _pct(fill_n, candidate_n),
            'quality_block_reasons': dict(stats['quality_block_reasons'].most_common(8)),
            'smart_entry_reject_reasons': dict(stats['smart_entry_reject_reasons'].most_common(8)),
            'quote_fail_reasons': dict(stats['quote_fail_reasons'].most_common(8)),
            'execution_abort_reasons': dict(stats['execution_abort_reasons'].most_common(8)),
            'scout_reject_reasons': dict(stats['scout_reject_reasons'].most_common(8)),
        })
        for key, value in stats.items():
            if isinstance(value, set):
                totals[key].update(value)
            elif isinstance(value, Counter):
                totals[key].update(value)

    payload = {
        'lookback_sec': lookback_sec,
        'since_ts': since_ts,
        'mode_count': len(mode_summaries),
        'by_mode': mode_summaries,
        'totals': {
            'candidate_n': len(totals['candidate']),
            'quality_evaluated_n': len(totals['quality_evaluated']),
            'quality_pass_n': len(totals['quality_pass']),
            'quality_block_n': len(totals['quality_block']),
            'pending_n': len(totals['pending']),
            'smart_entry_pass_n': len(totals['smart_entry_pass']),
            'smart_entry_reject_n': len(totals['smart_entry_reject']),
            'quote_success_n': len(totals['quote_success']),
            'quote_fail_n': len(totals['quote_fail']),
            'execution_abort_n': len(totals['execution_abort']),
            'scout_reject_n': len(totals['scout_reject']),
            'unique_token_n': len(totals['tokens']),
            'quality_pass_rate_pct': _pct(len(totals['quality_pass']), len(totals['quality_evaluated'])),
            'fill_per_candidate_pct': _pct(len(totals['quote_success']), len(totals['candidate'])),
            'quality_block_reasons': dict(totals['quality_block_reasons'].most_common(10)),
            'smart_entry_reject_reasons': dict(totals['smart_entry_reject_reasons'].most_common(10)),
            'quote_fail_reasons': dict(totals['quote_fail_reasons'].most_common(10)),
            'execution_abort_reasons': dict(totals['execution_abort_reasons'].most_common(10)),
            'scout_reject_reasons': dict(totals['scout_reject_reasons'].most_common(10)),
        },
    }
    record_decision_event(
        db,
        component='scout_telemetry',
        event_type='conversion_summary',
        decision='observe',
        reason='scout_funnel_summary',
        route='SCOUT',
        data_source='paper_decision_events+paper_trades',
        payload=payload,
        event_ts=now_ts,
    )
    if mode_summaries:
        compact = []
        for item in mode_summaries[:4]:
            compact.append(
                f"{item['entry_mode']}:cand={item['candidate_n']} "
                f"q={item['quality_pass_n']}/{item['quality_evaluated_n']} "
                f"fill={item['quote_success_n']}"
            )
        log.info(f"  [SCOUT_FUNNEL] lookback={lookback_sec}s " + " | ".join(compact))
    return payload


def _classify_upstream_miss_terminal(events):
    if not events:
        return 'no_probe_event'
    for row in reversed(events):
        if row['component'] == 'execution_api' and row['event_type'] == 'entry_quote' and row['decision'] == 'filled_paper':
            return 'filled_paper'
    for row in reversed(events):
        decision = str(row['decision'] or '').lower()
        if decision in {'block', 'reject', 'skip', 'abort', 'expire', 'fail', 'wait'}:
            return f"{row['component']}:{row['reason'] or decision}"
    if any(row['event_type'] == 'pending_entry' for row in events):
        return 'pending_no_terminal'
    if any(row['event_type'] == 'reentry_armed' for row in events):
        return 'armed_no_pending'
    if any(row['component'] == 'scout_quality' and row['decision'] == 'pass' for row in events):
        return 'quality_pass_no_pending'
    return 'downstream_observed_no_terminal'


def record_upstream_miss_chain_summary(db, *, now_ts, lookback_sec=None, limit=None):
    """Trace not_ath/upstream misses into the later probe/downstream terminal layer."""
    if not SCOUT_TELEMETRY_ENABLED:
        return None
    lookback_sec = int(lookback_sec or SCOUT_UPSTREAM_CHAIN_LOOKBACK_SEC)
    limit = int(limit or SCOUT_UPSTREAM_CHAIN_LIMIT)
    since_ts = float(now_ts) - lookback_sec
    source_rows = db.execute(
        """
        SELECT id, decision_event_id, created_event_ts, token_ca, symbol, lifecycle_id,
               signal_id, signal_ts, reject_reason, max_pnl_recorded, pnl_5m,
               pnl_15m, pnl_60m, tradability_status, tradable_missed
        FROM paper_missed_signal_attribution
        WHERE route = 'LOTTO'
          AND component = 'upstream_gate'
          AND reject_reason IN (
              'not_ath_v17',
              'not_ath_prebuy_kline_unknown_data_blocked',
              'lotto_observe_low_mc_vol'
          )
          AND created_event_ts >= ?
        ORDER BY created_event_ts DESC
        LIMIT ?
        """,
        (since_ts, limit),
    ).fetchall()
    by_source_reason = defaultdict(Counter)
    samples = []
    for source in source_rows:
        events = db.execute(
            """
            SELECT id, event_ts, component, event_type, decision, reason, payload_json
            FROM paper_decision_events
            WHERE token_ca = ?
              AND event_ts >= ?
              AND event_ts <= ?
              AND (? IS NULL OR id != ?)
              AND (
                  COALESCE(lifecycle_id, '') = COALESCE(?, '')
                  OR COALESCE(signal_ts, 0) = COALESCE(?, 0)
              )
              AND component != 'upstream_gate'
            ORDER BY event_ts ASC
            """,
            (
                source['token_ca'],
                float(source['created_event_ts'] or since_ts),
                float(now_ts),
                source['decision_event_id'],
                source['decision_event_id'],
                source['lifecycle_id'],
                source['signal_ts'],
            ),
        ).fetchall()
        terminal = _classify_upstream_miss_terminal(events)
        source_reason = source['reject_reason'] or 'unknown'
        by_source_reason[source_reason][terminal] += 1
        if len(samples) < 20 and (source_reason == 'not_ath_v17' or terminal != 'no_probe_event'):
            samples.append({
                'missed_attribution_id': source['id'],
                'token_ca': source['token_ca'],
                'symbol': source['symbol'],
                'signal_ts': source['signal_ts'],
                'source_reject_reason': source_reason,
                'terminal': terminal,
                'max_pnl_recorded': source['max_pnl_recorded'],
                'tradability_status': source['tradability_status'],
                'event_chain': [
                    f"{row['component']}:{row['event_type']}:{row['decision']}:{row['reason'] or ''}"
                    for row in events[-8:]
                ],
            })
    payload = {
        'lookback_sec': lookback_sec,
        'since_ts': since_ts,
        'source_count': len(source_rows),
        'source_reasons': {
            reason: dict(counter.most_common())
            for reason, counter in sorted(by_source_reason.items())
        },
        'samples': samples,
    }
    record_decision_event(
        db,
        component='scout_telemetry',
        event_type='upstream_miss_chain_summary',
        decision='observe',
        reason='not_ath_upstream_chain',
        route='LOTTO',
        data_source='paper_missed_signal_attribution+paper_decision_events',
        payload=payload,
        event_ts=now_ts,
    )
    if source_rows:
        not_ath = by_source_reason.get('not_ath_v17', Counter())
        top = ", ".join(f"{reason}={count}" for reason, count in not_ath.most_common(4)) or "none"
        log.info(f"  [UPSTREAM_MISS_CHAIN] not_ath_v17 {top}")
    return payload


def with_external_alpha_payload(payload, external_alpha):
    data = dict(payload or {})
    if external_alpha:
        data['external_alpha'] = external_alpha
    return data


def safe_external_alpha_lookup(db, token_ca, *, now=None, signal_ts=None, chain='sol'):
    try:
        return lookup_external_alpha(db, token_ca, chain=chain, now=now, signal_ts=signal_ts)
    except Exception as exc:
        return {"available": False, "reason": "external_alpha_lookup_error", "error": str(exc)}


def smart_entry_result_ready(pending_entries):
    for pending in (pending_entries or {}).values():
        if pending.get('timing_passed'):
            return True
        future = pending.get('_smart_entry_future')
        if future is not None and future.done():
            return True
    return False



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
    db_dir = os.path.dirname(path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    
    def _create_schema(db_conn):
        db_conn.row_factory = sqlite3.Row
        db_conn.execute("""
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
                signal_route TEXT,
                entry_mode TEXT,
                lotto_state_json TEXT,
                lifecycle_state TEXT,
                vitality_score REAL,
                entry_bias TEXT,
                lifecycle_features_json TEXT,
                ath_boost_count INTEGER DEFAULT 0,
                last_ath_ts INTEGER,
                last_ath_mc REAL,
                strategy_outcome TEXT,
                execution_availability TEXT,
                accounting_outcome TEXT,
                synthetic_close INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db_conn.execute("CREATE INDEX IF NOT EXISTS idx_pt_token ON paper_trades(token_ca)")
        db_conn.execute("CREATE INDEX IF NOT EXISTS idx_pt_exit ON paper_trades(exit_reason)")
        db_conn.execute("CREATE INDEX IF NOT EXISTS idx_pt_entry_ts ON paper_trades(entry_ts)")
        db_conn.execute("""
            CREATE TABLE IF NOT EXISTS paper_trade_path_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER NOT NULL,
                lifecycle_id TEXT,
                token_ca TEXT,
                symbol TEXT,
                strategy_stage TEXT,
                sample_ts INTEGER NOT NULL,
                action TEXT,
                reason TEXT,
                mark_price REAL,
                mark_pnl REAL,
                quote_price REAL,
                quote_pnl REAL,
                peak_pnl REAL,
                sold_pct REAL DEFAULT 0,
                token_amount_raw TEXT,
                mark_source TEXT,
                quote_success INTEGER,
                quote_failure_reason TEXT,
                quote_out_sol REAL,
                partial_realized_sol REAL,
                remaining_cost_basis_sol REAL,
                blended_mark_pnl REAL,
                blended_quote_pnl REAL,
                payload_json TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        db_conn.execute("CREATE INDEX IF NOT EXISTS idx_ptps_trade_ts ON paper_trade_path_samples(trade_id, sample_ts)")
        db_conn.execute("CREATE INDEX IF NOT EXISTS idx_ptps_token_ts ON paper_trade_path_samples(token_ca, sample_ts)")
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
            "ALTER TABLE paper_trades ADD COLUMN signal_route TEXT",
            "ALTER TABLE paper_trades ADD COLUMN entry_mode TEXT",
            "ALTER TABLE paper_trades ADD COLUMN lotto_state_json TEXT",
            "ALTER TABLE paper_trades ADD COLUMN lifecycle_state TEXT",
            "ALTER TABLE paper_trades ADD COLUMN vitality_score REAL",
            "ALTER TABLE paper_trades ADD COLUMN entry_bias TEXT",
            "ALTER TABLE paper_trades ADD COLUMN lifecycle_features_json TEXT",
            "ALTER TABLE paper_trades ADD COLUMN ath_boost_count INTEGER DEFAULT 0",
            "ALTER TABLE paper_trades ADD COLUMN last_ath_ts INTEGER",
            "ALTER TABLE paper_trades ADD COLUMN last_ath_mc REAL",
        ]:
            try:
                db_conn.execute(column_sql)
            except sqlite3.OperationalError:
                pass
        try:
            db_conn.execute("CREATE INDEX IF NOT EXISTS idx_pt_lifecycle_stage ON paper_trades(token_ca, signal_ts, strategy_stage)")
            db_conn.execute("CREATE INDEX IF NOT EXISTS idx_pt_signal_route ON paper_trades(signal_route)")
            db_conn.execute("CREATE INDEX IF NOT EXISTS idx_pt_lifecycle_state ON paper_trades(lifecycle_state)")
        except sqlite3.OperationalError:
            pass
        init_decision_audit(db_conn)
        init_external_alpha_shadow(db_conn)
        db_conn.commit()
    
    try:
        db = sqlite3.connect(path)
        _create_schema(db)
    except sqlite3.DatabaseError as e:
        if "file is not a database" in str(e).lower() or "disk image is malformed" in str(e).lower():
            logging.getLogger('paper_trade').warning(f"Paper DB corrupted ({e}), recreating {path}")
            if os.path.exists(path):
                os.remove(path)
            db = sqlite3.connect(path)
            _create_schema(db)
        else:
            raise
            
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
HELIUS_RPC_URL = os.environ.get('HELIUS_RPC_URL', '') or f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
if not HELIUS_API_KEY and HELIUS_RPC_URL:
    try:
        HELIUS_API_KEY = (urllib.parse.parse_qs(urllib.parse.urlparse(HELIUS_RPC_URL).query).get('api-key') or [''])[0]
    except Exception:
        HELIUS_API_KEY = ''

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

def helius_token_concentration(token_ca, timeout=2.5):
    """LIVE on-chain top1/top10 concentration via Helius RPC.

    Critical for LOTTO rug prevention: signal-time top10 is stale by the
    minute we evaluate. If insiders accumulated AFTER the signal (e.g. top10
    went 40% → 80%), we want to detect that BEFORE firing.

    Returns dict {'top1_pct', 'top10_pct', 'top_n_visible'} or None on failure.
    Fail-open: caller treats None as "no data, allow".
    """
    if not HELIUS_API_KEY or 'your' in HELIUS_API_KEY.lower():
        return None

    try:
        # Step 1: get top largest token accounts (max 20 per RPC call)
        largest_payload = {
            "jsonrpc": "2.0", "id": 1,
            "method": "getTokenLargestAccounts",
            "params": [token_ca, {"commitment": "confirmed"}]
        }
        l_status, l_data = _post_json(HELIUS_RPC_URL, largest_payload, timeout)
        if l_status != 200 or 'result' not in l_data:
            return None
        accounts = (l_data.get('result') or {}).get('value') or []
        if not accounts:
            return None

        # Step 2: total supply
        supply_payload = {
            "jsonrpc": "2.0", "id": 2,
            "method": "getTokenSupply",
            "params": [token_ca, {"commitment": "confirmed"}]
        }
        s_status, s_data = _post_json(HELIUS_RPC_URL, supply_payload, timeout)
        if s_status != 200 or 'result' not in s_data:
            return None
        supply_value = (s_data.get('result') or {}).get('value') or {}
        try:
            total_supply = float(supply_value.get('amount') or 0)
        except (TypeError, ValueError):
            return None
        if total_supply <= 0:
            return None

        def _amt(a):
            try:
                return float((a.get('amount') if isinstance(a, dict) else 0) or 0)
            except (TypeError, ValueError):
                return 0.0

        top1_amt = _amt(accounts[0])
        top10_amt = sum(_amt(a) for a in accounts[:10])

        return {
            'top1_pct': (top1_amt / total_supply) * 100.0,
            'top10_pct': (top10_amt / total_supply) * 100.0,
            'top_n_visible': len(accounts),
        }
    except Exception:
        return None


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




def fetch_realtime_price_snapshot(token_ca, pool_address, max_age_ms=ENTRY_TIMING_SNAP_MAX_AGE_MS, token_decimals=None):
    """
    Fetch a real-time SOL-denominated price for the timing/exit engines.

    Uses get_live_price_snapshot (Redis → shared Jupiter quote → DexScreener
    → direct) but enforces a strict freshness check: any snapshot older than
    max_age_ms is rejected. Returns a dict with price/source/age/timestamp,
    or None when no usable snapshot exists.

    Pass `token_decimals` when known (e.g. exit-monitor positions) so the
    Jupiter shared-quote source can convert SOL lamports → USD per token
    correctly. When unknown (entry timing on a brand-new signal), the helper
    falls back to assuming decimals=6 (the pump.fun default).
    """
    now_ms = int(time.time() * 1000)
    min_ts_ms = now_ms - max_age_ms
    snap = get_live_price_snapshot(token_ca, pool_address, min_timestamp_ms=min_ts_ms, token_decimals=token_decimals)
    if not snap:
        return None
    price = snap.get('price')
    ts_ms = snap.get('timestamp_ms') or 0
    age_ms, age_status = normalize_price_age_ms(now_ms, ts_ms)
    if not price or price <= 0:
        return None
    # Belt-and-suspenders: some sources (shared quote cache) don't honour
    # min_timestamp_ms inside get_live_price_snapshot — re-check here.
    if age_status == 'future_quote':
        return {
            'price': None,
            'source': 'future_quote',
            'age_ms': age_ms,
            'timestamp_ms': ts_ms,
            'age_status': age_status,
        }
    if age_ms is not None and age_ms > max_age_ms:
        return {
            'price': None,
            'source': snap.get('source'),
            'age_ms': age_ms,
            'timestamp_ms': ts_ms,
            'age_status': age_status,
        }
    return {
        'price': float(price),
        'source': snap.get('source'),
        'age_ms': age_ms,
        'timestamp_ms': ts_ms,
        'age_status': age_status,
        'payload': snap.get('payload'),
    }


def fetch_realtime_price(token_ca, pool_address, max_age_ms=ENTRY_TIMING_SNAP_MAX_AGE_MS, token_decimals=None):
    """
    Compatibility wrapper returning (price, source, age_ms).
    """
    snap = fetch_realtime_price_snapshot(
        token_ca,
        pool_address,
        max_age_ms=max_age_ms,
        token_decimals=token_decimals,
    )
    if not snap:
        return None, None, None
    return snap.get('price'), snap.get('source'), snap.get('age_ms')


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


def normalize_price_age_ms(now_ms, timestamp_ms, *, max_future_ms=LIVE_PRICE_MAX_FUTURE_MS):
    if not timestamp_ms:
        return None, 'missing_timestamp'
    age_ms = int(now_ms) - int(timestamp_ms)
    if age_ms < 0:
        if abs(age_ms) <= max_future_ms:
            return 0, 'clock_skew_clamped'
        return age_ms, 'future_quote'
    return age_ms, 'ok'


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
    age_ms, age_status = normalize_price_age_ms(int(time.time() * 1000), timestamp_ms)
    if age_status == 'future_quote' or age_ms > max_age_ms:
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
    Parse Super Index from signal description.
    Supports formats:
      ✡ Super Index： 119🔮
      ✡ **Super Index**： 119🔮
      ✡ **Super Index**： 119
      ✡ Super Index： ✡ x 82
      ✡ Super Index：(signal)87 --> 244 🔺180%
      ✡ Super Index：(signal)87 --> (current)244 🔺180%
    Returns int or None.
    """
    if not description:
        return None
    normalized = str(description).replace('**', '').replace('\r', '')
    # ATH format: take current/latest Super value, not the original signal value.
    m = re.search(
        r'Super\s+Index[：:]\s*\(signal\)\s*x?\d+\s*(?:🔮)?\s*(?:-->|->|→|—>)\s*(?:\(current\)\s*)?x?(\d+)',
        normalized,
        re.IGNORECASE,
    )
    if m:
        return int(m.group(1))
    # Current signal format no longer always includes the trailing crystal ball.
    m = re.search(r'Super\s+Index[：:]\s*(\d+)(?:\s*🔮)?', normalized)
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
            
        # V8: Extract market_cap from description if missing (especially for NEW_TRENDING)
        if not record.get('market_cap'):
            import re
            mc_match = re.search(r'🏦 \*\*MC:\*\* ([\d\.]+)([KMB]?)', desc)
            if mc_match:
                val_str, unit = mc_match.groups()
                try:
                    mc_val = float(val_str)
                    if unit == 'K': mc_val *= 1000
                    elif unit == 'M': mc_val *= 1000000
                    elif unit == 'B': mc_val *= 1000000000
                    record['market_cap'] = mc_val
                except ValueError:
                    pass
            
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


OBSERVABLE_NEW_TRENDING_STATUSES = {
    'PASS',
    'RISK_BLOCKED',
    'LOTTO_OBSERVE_LOW_MC_VOL',
    'NOT_ATH_PREBUY_KLINE_UNKNOWN_DATA_BLOCKED',
    'NOT_ATH_PREBUY_KLINE_RETRY_EXPIRED',
    'NOT_ATH_V17',
    'ILLIQUID_JUNK',
}


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
        return status in OBSERVABLE_NEW_TRENDING_STATUSES
        
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
        ORDER BY id ASC
    """, (last_signal_id,)).fetchall()
    sdb.close()
    return [r for r in _normalize_signal_rows(rows) if _is_paper_trade_signal(r)]


def _query_local_recent_signals(limit=20):
    sdb = sqlite3.connect(SENTIMENT_DB)
    sdb.row_factory = sqlite3.Row
    has_signal_type = _premium_signal_has_column(sdb, 'signal_type')
    signal_type_expr = 'signal_type' if has_signal_type else 'NULL AS signal_type'
    rows = sdb.execute(f"""
        SELECT id, token_ca, symbol, timestamp, description, hard_gate_status, {signal_type_expr}, market_cap, holders, volume_24h, top10_pct, is_ath
        FROM premium_signals
        ORDER BY id DESC
        LIMIT ?
    """, (max(limit * 5, 100),)).fetchall()
    sdb.close()
    filtered = [r for r in _normalize_signal_rows(rows) if _is_paper_trade_signal(r)]
    return list(reversed(filtered[:limit]))


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
    row = db.execute("""
        SELECT MAX(
            CASE
                WHEN signal_ts > 1000000000000 THEN CAST(signal_ts / 1000 AS INTEGER)
                ELSE signal_ts
            END
        ) as max_ts
        FROM paper_trades
    """).fetchone()
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
    partial_state_fields = ('tp1', 'tp2', 'tp3', 'tp4', 'soldPct', 'lockedPnl', 'moonbag', 'phase0PartialLocked')
    return not any(state.get(field) not in (None, False, 0, 0.0, '') for field in partial_state_fields)


def sanitize_monitor_state(monitor_state, *, token_ca, symbol, entry_price, entry_ts, position_size_sol, token_amount_raw, token_decimals, peak_pnl=0.0, trailing_active=False, bars_held=0, last_mark_ts=None):
    sanitized = dict(monitor_state or {})
    sanitized['tokenCA'] = token_ca or sanitized.get('tokenCA') or None
    sanitized['symbol'] = symbol or sanitized.get('symbol') or 'UNKNOWN'
    sanitized['entryPrice'] = _safe_float(entry_price, 0.0)
    sanitized['entryPriceUnit'] = PRICE_UNIT_SOL_PER_TOKEN
    sanitized['entryQuotePriceUnit'] = PRICE_UNIT_SOL_PER_TOKEN
    sanitized['entryTriggerPriceUnit'] = PRICE_UNIT_SOL_PER_TOKEN
    sanitized['pnlUnit'] = PNL_UNIT_RATIO_DECIMAL
    sanitized['accountingUnit'] = AMOUNT_UNIT_SOL
    sanitized['priceUnitContractVersion'] = PRICE_UNIT_CONTRACT_VERSION
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
        '_guardian_threat_tighten',  # Threat score tightening (Guardian → EXIT_MATRIX relay)
        'peak_ts', '_initial_tick_vol',  # A3 (time-decay) and A4 (flat-top) fields
        '_prev_guardian_pnl', '_phase0_confirmed', '_phase0_partial_locked',  # V7 + Phase 0 protection
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
        self._guardian_threat_tighten = 0  # Threat score tightening (Guardian → EXIT_MATRIX)


def build_lifecycle_id(token_ca, signal_ts):
    return f"{token_ca}:{int(signal_ts)}"


def stage_seq(stage_name):
    return {'stage1': 1, 'lotto': 1, 'stage2A': 2, 'stage3': 3}.get(stage_name, 0)


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


def print_all_stats(db, min_id=None):
    """Print cumulative stats across all dates."""
    where_clauses = ["exit_reason IS NOT NULL"]
    params = []
    if min_id is not None:
        where_clauses.append("id >= ?")
        params.append(min_id)

    rows = db.execute("""
        SELECT pnl_pct, exit_reason, market_regime, replay_source, bars_held,
               strategy_stage, stage_outcome, reentry_source, lifecycle_id,
               date(exit_ts, 'unixepoch') as exit_date
        FROM paper_trades
        WHERE {where_sql}
        ORDER BY exit_ts
    """.format(where_sql=" AND ".join(where_clauses)), params).fetchall()

    if not rows:
        log.info("No completed paper trades yet.")
        return

    real_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'real_kline_replay']
    live_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'live_monitor']

    log.info(f"{'='*60}")
    log.info(f"  Cumulative Paper Trade Stats")
    if min_id is not None:
        log.info(f"  Filter: id >= {min_id}")
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
    open_params = []
    open_where_clauses = ["exit_reason IS NULL"]
    if min_id is not None:
        open_where_clauses.append("id >= ?")
        open_params.append(min_id)
    open_count = db.execute(
        "SELECT COUNT(*) as c FROM paper_trades WHERE " + " AND ".join(open_where_clauses),
        open_params,
    ).fetchone()['c']
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


def fetch_kline_close_at_or_after(token_ca, target_ts, max_lag_sec=180):
    """Return a cached 1m close near target_ts for attribution; never uses shadow data."""
    try:
        target_ts = int(target_ts)
    except (TypeError, ValueError):
        return None
    try:
        with sqlite3.connect(KLINE_DB) as kdb:
            kdb.row_factory = sqlite3.Row
            row = kdb.execute(
                """
                SELECT timestamp, close, provider
                FROM kline_1m
                WHERE token_ca = ? AND timestamp >= ?
                ORDER BY timestamp ASC
                LIMIT 1
                """,
                (token_ca, target_ts),
            ).fetchone()
            if not row or not row["close"] or row["close"] <= 0:
                return None
            if row["timestamp"] and (row["timestamp"] - target_ts) > max_lag_sec:
                return None
            return float(row["close"]), f"kline_1m:{row['provider'] or 'unknown'}", int(row["timestamp"])
    except Exception:
        return None


def fetch_live_price_for_attribution(token_ca):
    """Best-effort live price fallback for very fresh missed-signal attribution."""
    try:
        pool = get_pool_address(token_ca)
        if not pool:
            return None
        price, source, age_ms = fetch_realtime_price(token_ca, pool, max_age_ms=60_000)
        if not price or price <= 0:
            return None
        ts = int(time.time() - ((age_ms or 0) / 1000.0))
        return float(price), f"live:{source or 'unknown'}", ts
    except Exception:
        return None


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
        **price_unit_contract_payload(exitPriceUnit='SYNTHETIC_CLOSE_NO_PRICE'),
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
    log.info(f"  LOTTO: size={LOTTO_POSITION_SIZE_SOL} SOL max_concurrent={LOTTO_MAX_CONCURRENT} strategy={LOTTO_STRATEGY_ID}")
    gmgn_status = gmgn_readonly_runtime_status()
    log.info(
        "  GMGN readonly: "
        f"enabled={gmgn_status['enabled']} "
        f"api_key_present={gmgn_status['api_key_present']} "
        f"api_key_prefix={gmgn_status['api_key_prefix']} "
        f"gmgn_cli={'found' if gmgn_status['gmgn_cli'] else 'missing'} "
        f"cache_sec={gmgn_status['cache_sec']} timeout_sec={gmgn_status['timeout_sec']}"
    )
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
    discovery_candidates = {}
    # SmartEntry async: each coin evaluates in its own thread, no blocking
    smart_entry_pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix='SmartEntry')
    lifecycles = restore_lifecycles(db)
    sanitized_monitor_states = 0
    last_missed_attribution_update = 0.0
    last_scout_telemetry = 0.0
    last_discovery_tracking = 0.0

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
    try:
        reconciled_watchlist = watchlist.expire_orphaned_position_states(set(positions.keys()))
        if reconciled_watchlist:
            log.warning(
                f"  [WATCHLIST_RECONCILE] expired {reconciled_watchlist} orphaned holding/moon_bag rows "
                f"with no open paper trade"
            )
    except Exception as exc:
        log.warning(f"  [WATCHLIST_RECONCILE] failed: {exc}")

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
        simulate_exit_fn=simulate_exit_execution,
    )
    exit_guardian.start()

    # P6: Wire up SmartEntry's max_wait dynamic adjustment
    global _active_holdings_count
    _active_holdings_count = lambda: len(positions)

    while True:
      try:
        now = time.time()
        now_utc = datetime.utcfromtimestamp(now)
        pending_priority = smart_entry_result_ready(pending_entries)

        if now - last_heartbeat >= HEARTBEAT_INTERVAL_SEC:
            freshness = get_signal_freshness()
            wl_watching = len(watchlist.get_watching())
            wl_holding = watchlist.get_active_count()
            # --- Memory monitor (detect OOM before SIGKILL) ---
            _mem_info = ''
            try:
                import resource as _resource
                # ru_maxrss: KB on Linux, bytes on macOS -- normalize to MB
                _raw_rss = _resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss
                _rss_mb = _raw_rss / 1024 if _raw_rss > 100000 else _raw_rss / (1024 * 1024)  # Linux vs macOS
                # Also try /proc/meminfo for container memory limit
                _container_mem = ''
                try:
                    with open('/proc/meminfo', 'r') as _f:
                        for _line in _f:
                            if _line.startswith('MemAvailable:'):
                                _avail_kb = int(_line.split()[1])
                                _container_mem = f' avail_mb={_avail_kb // 1024}'
                            elif _line.startswith('MemTotal:'):
                                _total_kb = int(_line.split()[1])
                                _container_mem += f' total_mb={_total_kb // 1024}'
                except Exception:
                    pass
                _mem_info = f' rss_mb={_rss_mb:.0f}{_container_mem}'
                if _rss_mb > 400:
                    log.warning(f'[MEMORY] RSS={_rss_mb:.0f}MB -- approaching OOM limit! Forcing gc...')
                    import gc; gc.collect()
            except Exception:
                pass
            log.info(
                f'[heartbeat] signals={freshness.get("total", 0)} source={freshness.get("source", "unknown")} '
                f'age_min={freshness.get("age_minutes")} watching={wl_watching} holding={wl_holding} '
                f'active_positions={len(positions)} pending={len(pending_entries)} discovery={len(discovery_candidates)}{_mem_info}'
            )
            last_heartbeat = now

        if now - last_discovery_tracking >= DISCOVERY_TRACKING_POLL_SEC:
            try:
                with positions_lock:
                    _discovery_armed = process_discovery_tracking_candidates(
                        db,
                        watchlist,
                        discovery_candidates,
                        pending_entries,
                        dict(positions),
                        now_ts=now,
                        max_positions=max_positions,
                    )
                if _discovery_armed:
                    log.info(
                        f"  [DISCOVERY_TRACKING] armed={_discovery_armed} "
                        f"active={len(discovery_candidates)} poll={DISCOVERY_TRACKING_POLL_SEC}s "
                        f"size={PAPER_TINY_SCOUT_SIZE_SOL:.3f}SOL"
                    )
                    last_progress = time.time()
            except Exception as _discovery_err:
                log.debug(f"  [DISCOVERY_TRACKING] scan failed: {_discovery_err}")
            last_discovery_tracking = now

        if not pending_priority and now - last_missed_attribution_update >= 60:
            try:
                _missed_updated = update_due_missed_attributions(
                    db,
                    historical_price_fetcher=fetch_kline_close_at_or_after,
                    live_price_fetcher=fetch_live_price_for_attribution,
                    now=now,
                    limit=250,
                )
                if _missed_updated:
                    log.info(f"  [MISSED_ATTRIBUTION] updated={_missed_updated}")
                    try:
                        _top_missed = db.execute(
                            """
                            SELECT symbol, route, component, reject_reason, max_pnl_recorded, pnl_5m, pnl_15m, pnl_60m, pnl_24h,
                                   tradable_missed, tradability_status, mae_before_peak_pnl, time_to_peak_sec
                            FROM paper_missed_signal_attribution
                            WHERE COALESCE(max_pnl_recorded, pnl_60m, pnl_15m, pnl_5m) >= 0.5
                            ORDER BY COALESCE(max_pnl_recorded, pnl_24h, pnl_60m, pnl_15m, pnl_5m) DESC
                            LIMIT 5
                            """
                        ).fetchall()
                        if _top_missed:
                            _parts = []
                            for _row in _top_missed:
                                _maxp = _row['max_pnl_recorded']
                                _best = _maxp if _maxp is not None else next(
                                    (v for v in (_row['pnl_24h'], _row['pnl_60m'], _row['pnl_15m'], _row['pnl_5m']) if v is not None),
                                    None,
                                )
                                _parts.append(
                                    f"{_row['symbol']} max={(_best or 0)*100:+.1f}% "
                                    f"tradable={_row['tradable_missed'] or 0} status={_row['tradability_status'] or 'n/a'} "
                                    f"{_row['route']}/{_row['component']} reason={_row['reject_reason']}"
                                )
                            log.info("  [TOP_MISSED_DOGS] " + " | ".join(_parts))
                    except Exception as _top_err:
                        log.debug(f"  [TOP_MISSED_DOGS] query failed: {_top_err}")
                try:
                    _coverage = missed_attribution_coverage(db, since_ts=now - 2 * 60 * 60)
                    _total = _coverage.get('total', 0)
                    if _total:
                        _baseline_pct = (_coverage.get('baseline_n', 0) / _total) * 100.0
                        _p5_pct = (_coverage.get('pnl_5m_n', 0) / _total) * 100.0
                        _level = log.warning if _baseline_pct < 80.0 else log.info
                        _level(
                            f"  [MISSED_ATTRIBUTION_COVERAGE] window=2h total={_total} "
                            f"baseline={_coverage.get('baseline_n', 0)}({_baseline_pct:.0f}%) "
                            f"pnl5={_coverage.get('pnl_5m_n', 0)}({_p5_pct:.0f}%) "
                            f"pnl15={_coverage.get('pnl_15m_n', 0)} "
                            f"pnl60={_coverage.get('pnl_60m_n', 0)} "
                            f"baseline_missing={_coverage.get('baseline_missing_n', 0)} "
                            f"tradable={_coverage.get('tradable_missed_n', 0)} "
                            f"stop_before_peak={_coverage.get('stop_before_peak_n', 0)}"
                        )
                except Exception as _coverage_err:
                    log.debug(f"  [MISSED_ATTRIBUTION_COVERAGE] query failed: {_coverage_err}")
                try:
                    _probe_shadow_n = record_lotto_probe_shadow_candidates(db, now_ts=now, limit=30)
                    if _probe_shadow_n:
                        log.info(
                            f"  [LOTTO_PROBE_SHADOW] candidates={_probe_shadow_n} "
                            f"min5m={LOTTO_PROBE_SHADOW_MIN_5M_PNL:.0%} "
                            f"size={LOTTO_PROBE_SHADOW_SIZE_SOL:.3f}SOL"
                        )
                except Exception as _probe_shadow_err:
                    log.debug(f"  [LOTTO_PROBE_SHADOW] scan failed: {_probe_shadow_err}")
                try:
                    _explosive_shadow_n = record_explosive_continuation_shadow_candidates(db, now_ts=now, limit=30)
                    if _explosive_shadow_n:
                        log.info(
                            f"  [EXPLOSIVE_CONTINUATION_SHADOW] candidates={_explosive_shadow_n} "
                            f"mode=shadow_only live_entry=false"
                        )
                except Exception as _explosive_shadow_err:
                    log.debug(f"  [EXPLOSIVE_CONTINUATION_SHADOW] scan failed: {_explosive_shadow_err}")
                try:
                    with positions_lock:
                        _upstream_probe_live_n = enqueue_lotto_upstream_miss_tiny_scout_candidates(
                            db,
                            watchlist,
                            pending_entries,
                            dict(positions),
                            now_ts=now,
                            limit=1,
                            max_positions=max_positions,
                        )
                    if _upstream_probe_live_n:
                        log.info(
                            f"  [LOTTO_UPSTREAM_PROBE_LIVE] pending={_upstream_probe_live_n} "
                            f"minMax={LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_MAX_PNL:.0%} "
                            f"minReclaim={LOTTO_UPSTREAM_MISS_TINY_SCOUT_MIN_RECLAIM_PNL:.0%} "
                            f"maxMc={LOTTO_UPSTREAM_MISS_TINY_SCOUT_MAX_MC:.0f} "
                            f"size={LOTTO_UPSTREAM_MISS_TINY_SCOUT_SIZE_SOL:.3f}SOL"
                        )
                except Exception as _upstream_probe_live_err:
                    log.debug(f"  [LOTTO_UPSTREAM_PROBE_LIVE] scan failed: {_upstream_probe_live_err}")
                try:
                    with positions_lock:
                        _probe_live_n = enqueue_lotto_real_probe_candidates(
                            db,
                            watchlist,
                            pending_entries,
                            dict(positions),
                            now_ts=now,
                            limit=1,
                            max_positions=max_positions,
                        )
                    if _probe_live_n:
                        log.info(
                            f"  [LOTTO_PROBE_LIVE] pending={_probe_live_n} "
                            f"minMax={LOTTO_REAL_PROBE_MIN_MAX_PNL:.0%} "
                            f"min15m={LOTTO_REAL_PROBE_MIN_15M_PNL:.0%} "
                            f"size={LOTTO_REAL_PROBE_SIZE_SOL:.3f}SOL"
                        )
                except Exception as _probe_live_err:
                    log.debug(f"  [LOTTO_PROBE_LIVE] scan failed: {_probe_live_err}")
                try:
                    with positions_lock:
                        _ath_probe_live_n = enqueue_ath_real_probe_candidates(
                            db,
                            watchlist,
                            pending_entries,
                            dict(positions),
                            now_ts=now,
                            limit=1,
                            max_positions=max_positions,
                        )
                    if _ath_probe_live_n:
                        log.info(
                            f"  [ATH_PROBE_LIVE] pending={_ath_probe_live_n} "
                            f"minMax={ATH_REAL_PROBE_MIN_MAX_PNL:.0%} "
                            f"minReclaim={ATH_REAL_PROBE_MIN_RECLAIM_PNL:.0%} "
                            f"size={ATH_REAL_PROBE_SIZE_SOL:.3f}SOL"
                        )
                except Exception as _ath_probe_live_err:
                    log.debug(f"  [ATH_PROBE_LIVE] scan failed: {_ath_probe_live_err}")
            except Exception as _missed_err:
                log.warning(f"  [MISSED_ATTRIBUTION] update failed: {_missed_err}")
            last_missed_attribution_update = now

        if not pending_priority and now - last_scout_telemetry >= SCOUT_FUNNEL_SUMMARY_INTERVAL_SEC:
            try:
                record_scout_funnel_summary(db, now_ts=now)
                record_upstream_miss_chain_summary(db, now_ts=now)
            except Exception as _scout_telemetry_err:
                log.debug(f"  [SCOUT_TELEMETRY] summary failed: {_scout_telemetry_err}")
            last_scout_telemetry = now

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

        if not pending_priority and now - last_signal_check >= SIGNAL_POLL_INTERVAL:
            last_signal_check = now
            try:
                new_signals = get_new_signals(last_signal_id)
                for sig in new_signals:
                    token_ca = sig['token_ca']
                    last_signal_id = sig['id']
                    signal_ts = sig['timestamp']
                    premium_signal_id = sig.get('id')
                    signal_type = sig.get('signal_type') or 'NEW_TRENDING'
                    hard_gate_status = (sig.get('hard_gate_status') or '').upper()
                    lifecycle_id = build_lifecycle_id(token_ca, signal_ts)
                    symbol = sig['symbol'] or token_ca[:8]
                    superseded_pending = supersede_stale_pending_for_signal(
                        pending_entries,
                        token_ca,
                        signal_ts,
                        signal_type,
                    )
                    for old_lifecycle_id, old_pending in superseded_pending:
                        record_decision_event(
                            db,
                            component='ath_anchor_refresh',
                            event_type='entry_abort',
                            decision='supersede',
                            reason='newer_ath_signal_refresh',
                            token_ca=token_ca,
                            symbol=symbol,
                            lifecycle_id=old_lifecycle_id,
                            signal_ts=old_pending.get('signal_ts'),
                            signal_id=old_pending.get('premium_signal_id'),
                            route=old_pending.get('signal_route') or old_pending.get('signal_type'),
                            data_source='premium_signals',
                            payload={
                                'old_lifecycle_id': old_lifecycle_id,
                                'old_signal_ts': old_pending.get('signal_ts'),
                                'old_premium_signal_id': old_pending.get('premium_signal_id'),
                                'new_lifecycle_id': lifecycle_id,
                                'new_signal_ts': signal_ts,
                                'new_premium_signal_id': premium_signal_id,
                            },
                        )
                        log.info(
                            f"  [ATH_REFRESH] {symbol} superseded stale pending "
                            f"{old_lifecycle_id} with newer ATH lifecycle={lifecycle_id}"
                        )
                    signal_lifecycle = lifecycle_payload_for(
                        signal=sig,
                        route=signal_type,
                        signal_ts=signal_ts,
                        now=now,
                    )
                    external_alpha = safe_external_alpha_lookup(
                        db,
                        token_ca,
                        now=now,
                        signal_ts=signal_ts,
                    )
                    signal_audit_payload = with_external_alpha_payload(signal_payload(sig), external_alpha)

                    record_decision_event(
                        db,
                        component='signal_ingest',
                        event_type='signal_received',
                        decision='received',
                        reason=signal_type,
                        token_ca=token_ca,
                        symbol=symbol,
                        lifecycle_id=lifecycle_id,
                        signal_ts=signal_ts,
                        signal_id=premium_signal_id,
                        data_source='premium_signals',
                        payload=with_lifecycle_payload(signal_audit_payload, signal_lifecycle),
                    )

                    if signal_type == 'NEW_TRENDING' and hard_gate_status in {
                        'LOTTO_OBSERVE_LOW_MC_VOL',
                        'NOT_ATH_PREBUY_KLINE_UNKNOWN_DATA_BLOCKED',
                        'NOT_ATH_PREBUY_KLINE_RETRY_EXPIRED',
                        'NOT_ATH_V17',
                        'ILLIQUID_JUNK',
                    }:
                        record_decision_event(
                            db,
                            component='upstream_gate',
                            event_type='signal_skip',
                            decision='skip',
                            reason=hard_gate_status.lower(),
                            token_ca=token_ca,
                            symbol=symbol,
                            lifecycle_id=lifecycle_id,
                            signal_ts=signal_ts,
                            signal_id=premium_signal_id,
                            route='LOTTO',
                            data_source='premium_signals',
                            payload=with_lifecycle_payload(signal_audit_payload, signal_lifecycle),
                        )
                        log.info(
                            f"  [UPSTREAM_ATTR] {symbol} tracked as missed LOTTO candidate: "
                            f"status={hard_gate_status} MC=${sig.get('market_cap') or 0:,.0f} "
                            f"Vol=${sig.get('volume_24h') or 0:,.0f}"
                        )
                        if any(pos.lifecycle_id == lifecycle_id for pos in positions.values()) or lifecycle_id in pending_entries:
                            record_decision_event(
                                db,
                                component='lotto_second_pass',
                                event_type='observe_skip',
                                decision='skip',
                                reason='lifecycle_already_active',
                                token_ca=token_ca,
                                symbol=symbol,
                                lifecycle_id=lifecycle_id,
                                signal_ts=signal_ts,
                                signal_id=premium_signal_id,
                                route='LOTTO',
                                payload=with_lifecycle_payload({'pending': lifecycle_id in pending_entries}, signal_lifecycle),
                            )
                            continue

                        existing = db.execute(
                            "SELECT id FROM paper_trades WHERE lifecycle_id = ? OR (token_ca = ? AND signal_ts = ?)",
                            (lifecycle_id, token_ca, signal_ts)
                        ).fetchone()
                        if existing:
                            record_decision_event(
                                db,
                                component='lotto_second_pass',
                                event_type='observe_skip',
                                decision='skip',
                                reason='paper_trade_exists',
                                token_ca=token_ca,
                                symbol=symbol,
                                lifecycle_id=lifecycle_id,
                                signal_ts=signal_ts,
                                signal_id=premium_signal_id,
                                route='LOTTO',
                                payload=with_lifecycle_payload({'paper_trade_id': existing['id']}, signal_lifecycle),
                            )
                            continue

                        pool = get_pool_address(token_ca)
                        if not pool:
                            record_decision_event(
                                db,
                                component='lotto_second_pass',
                                event_type='observe_reject',
                                decision='reject',
                                reason='pool_not_found',
                                token_ca=token_ca,
                                symbol=symbol,
                                lifecycle_id=lifecycle_id,
                                signal_ts=signal_ts,
                                signal_id=premium_signal_id,
                                route='LOTTO',
                                data_source='pool_lookup',
                                payload=with_lifecycle_payload(signal_audit_payload, signal_lifecycle),
                            )
                            log.info(f"  [LOTTO_OBSERVE] {symbol} not added to LOTTO watchlist: pool_not_found")
                            continue

                        time.sleep(0.1)
                        sig_price_val, _, _ = fetch_realtime_price(token_ca, pool, max_age_ms=15000)
                        sig_price = sig_price_val if sig_price_val and sig_price_val > 0 else None
                        try:
                            observe_dex = fetch_dexscreener_trend_snapshot(token_ca)
                        except Exception:
                            observe_dex = None
                        observe_lifecycle = lifecycle_payload_for(
                            signal=sig,
                            dex_snapshot=observe_dex,
                            route='LOTTO',
                            signal_ts=signal_ts,
                            signal_price=sig_price,
                            quote_available=bool(sig_price),
                            now=now,
                        )
                        top10_pct = parse_top10_percent(sig.get('description') or '')
                        super_idx = parse_super_index(sig.get('description') or '')
                        registered_entry = watchlist.register(
                            ca=token_ca,
                            symbol=symbol,
                            signal_type='LOTTO',
                            pool_address=pool,
                            signal_ts=signal_ts,
                            premium_signal_id=premium_signal_id,
                            signal_price=sig_price,
                            signal_mc=sig.get('market_cap'),
                            signal_super=super_idx or 0,
                            signal_holders=sig.get('holders') or 0,
                            signal_vol24h=sig.get('volume_24h') or 0,
                            signal_tx24h=0,
                            signal_top10=top10_pct or 0,
                        )
                        if registered_entry:
                            watchlist.update_position_state(registered_entry['id'], signal_route='LOTTO')
                        record_decision_event(
                            db,
                            component='lotto_second_pass',
                            event_type='observe_register',
                            decision='registered',
                            reason=hard_gate_status.lower(),
                            token_ca=token_ca,
                            symbol=symbol,
                            lifecycle_id=lifecycle_id,
                            signal_ts=signal_ts,
                            signal_id=premium_signal_id,
                            route='LOTTO',
                            data_source='premium_signals+realtime_price',
                            payload=with_lifecycle_payload({
                                **signal_audit_payload,
                                'watchlist_id': registered_entry.get('id') if registered_entry else None,
                                'pool': pool,
                                'signal_price': sig_price,
                                'super_idx': super_idx,
                                'top10_pct': top10_pct,
                            }, observe_lifecycle),
                        )
                        _upstream_realtime_armed = arm_lotto_upstream_realtime_tiny_scout(
                            db,
                            watchlist,
                            pending_entries,
                            dict(positions),
                            sig=sig,
                            registered_entry=watchlist.get_by_id(registered_entry['id']) if registered_entry else None,
                            pool=pool,
                            lifecycle_id=lifecycle_id,
                            signal_lifecycle=observe_lifecycle,
                            signal_audit_payload=signal_audit_payload,
                            hard_gate_status=hard_gate_status,
                            now_ts=now,
                            discovery_candidates=discovery_candidates,
                        )
                        if _upstream_realtime_armed:
                            log.info(
                                f"  [LOTTO_UPSTREAM_REALTIME_SCOUT] {symbol} pending "
                                f"status={hard_gate_status} size={LOTTO_UPSTREAM_REALTIME_TINY_SCOUT_SIZE_SOL:.3f}SOL"
                            )
                        log.info(
                            f"  [LOTTO_OBSERVE] {symbol} added to LOTTO watchlist for second-pass filtering: "
                            f"status={hard_gate_status} MC=${sig.get('market_cap') or 0:,.0f} "
                            f"Vol=${sig.get('volume_24h') or 0:,.0f}"
                        )
                        last_progress = time.time()
                        continue

                    if any(pos.lifecycle_id == lifecycle_id for pos in positions.values()) or lifecycle_id in pending_entries:
                        record_decision_event(
                            db,
                            component='dedupe',
                            event_type='signal_skip',
                            decision='skip',
                            reason='lifecycle_already_active',
                            token_ca=token_ca,
                            symbol=symbol,
                            lifecycle_id=lifecycle_id,
                            signal_ts=signal_ts,
                            signal_id=premium_signal_id,
                            payload={'pending': lifecycle_id in pending_entries},
                        )
                        continue

                    existing_w_entry = watchlist.get_by_ca(token_ca)
                    route_decision = route_signal(sig, now=time.time(), existing_entry=existing_w_entry)
                    routed_lifecycle = lifecycle_payload_for(
                        signal=sig,
                        watchlist_entry=existing_w_entry,
                        route=route_decision.route,
                        signal_ts=signal_ts,
                        now=now,
                    )
                    record_decision_event(
                        db,
                        component='signal_router',
                        event_type='route_decision',
                        decision=route_decision.route,
                        reason=route_decision.reason,
                        token_ca=token_ca,
                        symbol=symbol,
                        lifecycle_id=lifecycle_id,
                        signal_ts=signal_ts,
                        signal_id=premium_signal_id,
                        route=route_decision.route,
                        data_source='premium_signals',
                        payload=with_lifecycle_payload({
                            **signal_audit_payload,
                            'signal_age_sec': route_decision.signal_age_sec,
                            'existing_watchlist_status': existing_w_entry.get('status') if existing_w_entry else None,
                            'existing_watchlist_type': existing_w_entry.get('type') if existing_w_entry else None,
                        }, routed_lifecycle),
                    )
                    if route_decision.is_lotto_boost and existing_w_entry:
                        boost_updates = build_ath_boost_updates(
                            existing_w_entry,
                            signal_ts=signal_ts,
                            signal_market_cap=sig.get('market_cap') or 0,
                            now=time.time(),
                        )
                        watchlist.update_position_state(existing_w_entry['id'], **boost_updates)
                        with positions_lock:
                            for pos in positions.values():
                                if pos.token_ca == token_ca and is_lotto_position(pos, existing_w_entry):
                                    pos.monitor_state = pos.monitor_state or {}
                                    pos.monitor_state['athBoostCount'] = boost_updates['ath_count']
                                    pos.monitor_state['lastAthTs'] = boost_updates['last_ath_ts']
                                    pos.monitor_state['lastAthMc'] = boost_updates['last_ath_mc']
                                    pos.monitor_state['lottoTrailLockoutUntil'] = boost_updates['trail_lockout_until']
                                    db.execute(
                                        """
                                        UPDATE paper_trades
                                        SET ath_boost_count = ?, last_ath_ts = ?, last_ath_mc = ?, monitor_state_json = ?
                                        WHERE id = ?
                                        """,
                                        (
                                            boost_updates['ath_count'],
                                            boost_updates['last_ath_ts'],
                                            boost_updates['last_ath_mc'],
                                            json.dumps(pos.monitor_state),
                                            pos.trade_id,
                                        )
                                    )
                        db.commit()
                        log.info(
                            f"  [LOTTO] ATH boost {sig.get('symbol') or token_ca[:8]}: "
                            f"count={boost_updates['ath_count']} mc=${boost_updates['last_ath_mc']:,.0f} "
                            f"lockout_until={int(boost_updates['trail_lockout_until'])}"
                        )
                        record_decision_event(
                            db,
                            component='lotto_ath_feedback',
                            event_type='ath_boost',
                            decision='boost_hold',
                            reason=route_decision.reason,
                            token_ca=token_ca,
                            symbol=symbol,
                            lifecycle_id=lifecycle_id,
                            signal_ts=signal_ts,
                            signal_id=premium_signal_id,
                            route='LOTTO',
                            payload=boost_updates,
                        )
                        last_progress = time.time()
                        continue

                    if len(positions) + len(pending_entries) >= max_positions:
                        record_decision_event(
                            db,
                            component='portfolio_guard',
                            event_type='signal_skip',
                            decision='skip',
                            reason='max_positions_reached',
                            token_ca=token_ca,
                            symbol=symbol,
                            lifecycle_id=lifecycle_id,
                            signal_ts=signal_ts,
                            signal_id=premium_signal_id,
                            route=route_decision.route,
                            payload={'positions': len(positions), 'pending': len(pending_entries), 'max_positions': max_positions},
                        )
                        continue

                    existing = db.execute(
                        "SELECT id FROM paper_trades WHERE lifecycle_id = ? OR (token_ca = ? AND signal_ts = ? AND strategy_stage = 'stage1')",
                        (lifecycle_id, token_ca, signal_ts)
                    ).fetchone()
                    if existing:
                        record_decision_event(
                            db,
                            component='dedupe',
                            event_type='signal_skip',
                            decision='skip',
                            reason='paper_trade_exists',
                            token_ca=token_ca,
                            symbol=symbol,
                            lifecycle_id=lifecycle_id,
                            signal_ts=signal_ts,
                            signal_id=premium_signal_id,
                            route=route_decision.route,
                            payload={'paper_trade_id': existing['id']},
                        )
                        continue

                    super_idx = parse_super_index(sig['description'] or '')

                    # === LOTTO classification (must happen BEFORE filters that could block) ===
                    # The router sends fresh NEW_TRENDING + MC<$30K signals to LOTTO.
                    # LOTTO bypasses super_idx because early tokens often have not built
                    # complete matrix data yet.
                    _raw_mc = sig.get('market_cap') or 0
                    _signal_age_sec = route_decision.signal_age_sec
                    _is_lotto_signal = route_decision.is_lotto

                    # Super Score filter:
                    # NOT_ATH: must have Super > min_super_index (config=70)
                    # ATH: no Super Score (None), skip this filter — ATH uses own pipeline
                    # LOTTO: bypass — fresh tokens haven't built super_idx yet
                    if not sig.get('is_ath') and not _is_lotto_signal and (super_idx is None or super_idx <= min_super_index):
                        record_decision_event(
                            db,
                            component='prebuy_filter',
                            event_type='signal_reject',
                            decision='reject',
                            reason='super_idx_below_min',
                            token_ca=token_ca,
                            symbol=symbol,
                            lifecycle_id=lifecycle_id,
                            signal_ts=signal_ts,
                            signal_id=premium_signal_id,
                            route=route_decision.route,
                            payload=with_lifecycle_payload({'super_idx': super_idx, 'min_super_index': min_super_index}, routed_lifecycle),
                        )
                        continue
                    top10_max = (strategy_config.get('signalFilters') or {}).get('top10PctPrimaryMax', 45.0)
                    # Config might have 100 as default from old JSON schema, if it's 100 we override to 45 for safety or honor it?
                    # Since user wants it active, let's strictly use 45.0 if it's 100 or missing, to enforce safety easily without JSON patching.
                    if top10_max >= 100.0:
                        top10_max = 45.0

                    # LOTTO uses a relaxed 70% threshold (concentrated holders normal at <$30K MC)
                    _effective_top10_max = 70.0 if _is_lotto_signal else top10_max

                    top10_pct = parse_top10_percent(sig['description'] or '')
                    if top10_pct is not None and top10_pct > _effective_top10_max:
                        log.info(f"  [PREBUY_FILTER] {symbol} BLOCKED: Top10 {top10_pct}% exceeds max allowed {_effective_top10_max}%, skipping")
                        record_decision_event(
                            db,
                            component='prebuy_filter',
                            event_type='signal_reject',
                            decision='reject',
                            reason='top10_pct_above_max',
                            token_ca=token_ca,
                            symbol=symbol,
                            lifecycle_id=lifecycle_id,
                            signal_ts=signal_ts,
                            signal_id=premium_signal_id,
                            route=route_decision.route,
                            payload=with_lifecycle_payload({'top10_pct': top10_pct, 'max_top10_pct': _effective_top10_max}, routed_lifecycle),
                        )
                        # FIX 3: Remember blocked CAs so Watchlist won't accept them later
                        # (only blacklist on the strict 45% threshold, not the LOTTO 70%)
                        if not _is_lotto_signal:
                            if not hasattr(watchlist, '_top10_blacklist'):
                                watchlist._top10_blacklist = {}
                            watchlist._top10_blacklist[token_ca] = top10_pct
                        continue

                    # FIX 3: Also block if this CA was previously flagged by PREBUY_FILTER
                    # (exempt LOTTO — its own threshold is more permissive)
                    if not _is_lotto_signal and hasattr(watchlist, '_top10_blacklist') and token_ca in watchlist._top10_blacklist:
                        _prev_top10 = watchlist._top10_blacklist[token_ca]
                        log.info(f"  [PREBUY_FILTER] {symbol} BLOCKED: previously flagged Top10={_prev_top10}% (insider concentration memory), skipping")
                        record_decision_event(
                            db,
                            component='prebuy_filter',
                            event_type='signal_reject',
                            decision='reject',
                            reason='top10_blacklist_memory',
                            token_ca=token_ca,
                            symbol=symbol,
                            lifecycle_id=lifecycle_id,
                            signal_ts=signal_ts,
                            signal_id=premium_signal_id,
                            route=route_decision.route,
                            payload=with_lifecycle_payload({'previous_top10_pct': _prev_top10}, routed_lifecycle),
                        )
                        continue

                    # Determine signal price immediately for reference
                    pool = get_pool_address(token_ca)
                    if not pool:
                        log.warning(f"  Could not find pool for {symbol}, skipping")
                        record_decision_event(
                            db,
                            component='data_source',
                            event_type='pool_lookup',
                            decision='reject',
                            reason='pool_not_found',
                            token_ca=token_ca,
                            symbol=symbol,
                            lifecycle_id=lifecycle_id,
                            signal_ts=signal_ts,
                            signal_id=premium_signal_id,
                            route=route_decision.route,
                            data_source='pool_lookup',
                            payload=with_lifecycle_payload({'token_ca': token_ca}, routed_lifecycle),
                        )
                        continue
                    time.sleep(0.1)
                    sig_price_val, _, _ = fetch_realtime_price(token_ca, pool, max_age_ms=15000)
                    sig_price = sig_price_val if sig_price_val and sig_price_val > 0 else None

                    log.info(f"  [WATCHLIST] Registering {symbol} ({signal_type}) Super={super_idx} Price={sig_price}")

                    # SUSTAINED_ATH: Record this ATH registration for trend tracking
                    if sig.get('is_ath') and sig_price and sig_price > 0:
                        if not hasattr(watchlist, '_ath_history'):
                            watchlist._ath_history = {}
                        if token_ca not in watchlist._ath_history:
                            watchlist._ath_history[token_ca] = []
                        watchlist._ath_history[token_ca].append((time.time(), sig_price))
                        # Prune entries older than 120 minutes
                        _cutoff = time.time() - 7200
                        watchlist._ath_history[token_ca] = [
                            (ts, px) for ts, px in watchlist._ath_history[token_ca] if ts > _cutoff
                        ]
                        _ath_hist = watchlist._ath_history[token_ca]
                        if len(_ath_hist) >= 2:
                            # Check if prices are generally rising and time span is at least 15 minutes.
                            # Fix (2026-04-23): Lowered from 3 registrations + 30 min → 2 + 15 min.
                            # Audit showed SAM had 8 ATH registrations over 2h but sustained_ath
                            # barely qualified late due to the strict window. Earlier qualification
                            # unlocks relaxed m9s_cap (3.5%→6.0%) and RVol bypass.
                            _first_ts, _first_px = _ath_hist[0]
                            _last_ts, _last_px = _ath_hist[-1]
                            _time_span_min = (_last_ts - _first_ts) / 60
                            if _last_px > _first_px and _time_span_min >= 15:
                                _mult = _last_px / _first_px
                                log.info(
                                    f"  [SUSTAINED_ATH] {symbol} QUALIFIED: "
                                    f"{len(_ath_hist)} ATH registrations over {_time_span_min:.0f}min, "
                                    f"price {_first_px:.10f} → {_last_px:.10f} ({_mult:.1f}x)"
                                )

                    # LOTTO classification was determined earlier (above the super_idx filter).
                    # Reuse _is_lotto_signal computed at line ~3671.
                    _wl_type = 'ATH' if sig.get('is_ath') else ('LOTTO' if _is_lotto_signal else 'NOT_ATH')
                    if _is_lotto_signal:
                        log.info(
                            f"  [LOTTO] 🎰 {symbol} classified as LOTTO: "
                            f"MC=${_raw_mc:,.0f} age={_signal_age_sec:.0f}s "
                            f"reason={route_decision.reason} super_idx={super_idx}"
                        )

                    registered_entry = watchlist.register(
                        ca=token_ca,
                        symbol=symbol,
                        signal_type=_wl_type,
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
                    if _is_lotto_signal and registered_entry:
                        watchlist.update_position_state(registered_entry['id'], signal_route='LOTTO')
                    watch_lifecycle = lifecycle_payload_for(
                        signal=sig,
                        watchlist_entry=registered_entry,
                        route='LOTTO' if _is_lotto_signal else _wl_type,
                        signal_ts=signal_ts,
                        signal_price=sig_price,
                        quote_available=bool(sig_price),
                        now=now,
                    )
                    record_decision_event(
                        db,
                        component='watchlist',
                        event_type='register',
                        decision='registered',
                        reason=_wl_type,
                        token_ca=token_ca,
                        symbol=symbol,
                        lifecycle_id=lifecycle_id,
                        signal_ts=signal_ts,
                        signal_id=premium_signal_id,
                        route='LOTTO' if _is_lotto_signal else _wl_type,
                        data_source='realtime_price',
                        payload=with_lifecycle_payload({
                            'external_alpha': external_alpha,
                            'watchlist_id': registered_entry.get('id') if registered_entry else None,
                            'watchlist_type': _wl_type,
                            'pool': pool,
                            'signal_price': sig_price,
                            'super_idx': super_idx,
                            'top10_pct': top10_pct,
                        }, watch_lifecycle),
                    )
                    last_progress = time.time()
            except Exception as e:
                log.error(f"Signal check error: {e}")

        # --- Evaluate Watchlist Entries ---
        if pending_priority:
            watching_entries = []
            log.info("  [PENDING_PRIORITY] SmartEntry result ready; skipping housekeeping/watchlist scan for immediate execution")
        else:
            watching_entries = watchlist.get_watching()
        if watching_entries:
            log.info(f"  [WATCHLIST_SCAN] Scanning {len(watching_entries)} watching tokens...")
        wl_eval_count = 0
        wl_skip_cooldown = 0
        wl_skip_duplicate = 0
        for w_entry in watching_entries:
            try:
                if smart_entry_result_ready(pending_entries):
                    log.info("  [PENDING_PRIORITY] SmartEntry completed during scan; deferring remaining watchlist work")
                    break
                lifecycle_id = build_lifecycle_id(w_entry['ca'], w_entry['signal_ts'])
                
                # Skip if already in pending or open positions
                # But if SmartEntry is actively running, send it a data refresh
                if lifecycle_id in pending_entries or any(p.lifecycle_id == lifecycle_id for p in positions.values()):
                    # Send data refresh to running SmartEntry thread (if any)
                    _pending = pending_entries.get(lifecycle_id)
                    if _pending and _pending.get('_smart_entry_future') and not _pending['_smart_entry_future'].done():
                        # SmartEntry is actively running — push fresh data
                        _refresh_cooldown = _pending.get('_refresh_sent_at', 0)
                        if time.time() - _refresh_cooldown >= 30:  # max once per 30s
                            _refresh_scores = None
                            _refresh_trend = None
                            try:
                                _refresh_eval = matrix_evaluator.evaluate(w_entry)
                                _refresh_scores = _refresh_eval.get('scores')
                                _refresh_trend = fetch_dexscreener_trend_snapshot(w_entry['ca'])
                            except Exception:
                                pass
                            if _refresh_scores:
                                _pending['_refresh_sent_at'] = time.time()
                                # Also update matrix_scores in pending for downstream use
                                _pending['matrix_scores'] = _refresh_scores

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
                _now_for_cooldown = time.time()
                if _cd_until > _now_for_cooldown:
                    _cd_remain = int(_cd_until - _now_for_cooldown)
                    if _cd_remain > 15:  # only log if > 15s remaining to reduce spam
                        log.info(f"  [WATCHLIST] ⏳ {w_entry['symbol']} WAIT reason=post_exit_cooldown ({_cd_remain}s remaining)")
                    try:
                        watchlist.touch_eval(w_entry['id'], _now_for_cooldown)
                    except Exception:
                        pass
                    wl_skip_cooldown += 1
                    continue
                _fire_block_until = float(w_entry.get('fire_block_until') or 0)
                _now_for_fire_block = time.time()
                if _fire_block_until > _now_for_fire_block:
                    _fb_remain = int(_fire_block_until - _now_for_fire_block)
                    if _fb_remain > 15:
                        log.info(
                            f"  [WATCHLIST] ⏳ {w_entry['symbol']} WAIT reason=readiness_preflight_cooldown "
                            f"({_fb_remain}s remaining; {w_entry.get('fire_block_reason') or 'unknown'})"
                        )
                    try:
                        watchlist.touch_eval(w_entry['id'], _now_for_fire_block)
                    except Exception:
                        pass
                    wl_skip_cooldown += 1
                    continue
                
                # Skip if max positions reached
                if len(positions) + len(pending_entries) >= max_positions:
                    log.info(f"  [WATCHLIST_SCAN] Max positions reached ({max_positions}), stopping scan")
                    break
                
                wl_eval_count += 1

                # === LOTTO FAST-LANE ===
                # NEW_TRENDING + MC<$30K signals skip matrix evaluation entirely.
                # Quick defense only: holders/volume/top10/liquidity/live concentration.
                if w_entry.get('type') == 'LOTTO':
                    try:
                        _lotto_dex = fetch_dexscreener_trend_snapshot(w_entry['ca'])
                    except Exception:
                        _lotto_dex = None
                    _gmgn_enrichment = fetch_gmgn_token_enrichment(w_entry['ca'])

                    _lotto_live = helius_token_concentration(w_entry['ca'])
                    with positions_lock:
                        _current_lotto_count = active_lotto_count(positions, pending_entries)
                    _lotto_decision = evaluate_lotto_entry(
                        w_entry,
                        dex_snapshot=_lotto_dex,
                        live_concentration=_lotto_live,
                        current_lotto_count=_current_lotto_count,
                        data_health_ok=True,
                        now=time.time(),
                    )
                    _lotto_detail = _lotto_decision.detail
                    if _gmgn_enrichment:
                        _lotto_detail = {
                            **_lotto_detail,
                            'gmgn_readonly': _gmgn_enrichment,
                            'gmgn_risk_flags': _gmgn_enrichment.get('risk_flags', []),
                        }
                    _lotto_liq = _lotto_detail.get('liquidity_usd', 0) or 0
                    _lotto_top10 = _lotto_detail.get('top10_pct', w_entry.get('signal_top10', 0) or 0)
                    _lotto_age_sec = _lotto_detail.get('age_sec', time.time() - w_entry.get('added_at', time.time()))
                    _lotto_lifecycle = lifecycle_payload_for(
                        watchlist_entry=w_entry,
                        dex_snapshot=_lotto_dex,
                        live_concentration=_lotto_live,
                        route='LOTTO',
                        signal_ts=w_entry['signal_ts'],
                        now=now,
                    )
                    _external_alpha = safe_external_alpha_lookup(
                        db,
                        w_entry['ca'],
                        now=now,
                        signal_ts=w_entry['signal_ts'],
                    )
                    _lotto_detail = {
                        **_lotto_detail,
                        'external_alpha': _external_alpha,
                    }
                    _gmgn_policy = evaluate_gmgn_lotto_policy(
                        _gmgn_enrichment,
                        _lotto_detail,
                        lifecycle=_lotto_lifecycle,
                        entry_mode=_lotto_detail.get('entry_mode'),
                    )
                    _lotto_detail = {
                        **_lotto_detail,
                        'gmgn_policy': _gmgn_policy,
                        'gmgn_action': _gmgn_policy.get('action'),
                        'gmgn_reason': _gmgn_policy.get('reason'),
                    }
                    if _lotto_decision.allow and _gmgn_policy.get('action') == 'reject':
                        _lotto_decision = LottoDecision(
                            "expire",
                            _gmgn_policy.get('reason') or 'gmgn_policy_reject',
                            _lotto_detail,
                        )
                    elif _lotto_decision.allow and _gmgn_policy.get('action') == 'downsize':
                        _lotto_detail['gmgn_size_multiplier'] = _gmgn_policy.get('size_multiplier', 1.0)
                    elif _lotto_decision.allow and _gmgn_policy.get('action') == 'boost':
                        _lotto_detail['gmgn_edge_boost'] = True
                        if not _lotto_detail.get('entry_mode'):
                            _lotto_detail['entry_mode'] = 'gmgn_clean_lotto_fast_lane'
                        _lotto_decision = LottoDecision(
                            "allow",
                            _gmgn_policy.get('reason') or _lotto_decision.reason,
                            _lotto_detail,
                        )
                    elif not _lotto_decision.allow:
                        _gmgn_rescue = evaluate_gmgn_tiny_scout_rescue(
                            _lotto_decision.reason,
                            _gmgn_policy,
                            _lotto_detail,
                        )
                        _rescue_quality = (_gmgn_rescue.get("detail") or {}).get("scout_quality")
                        record_scout_quality_decision(
                            db,
                            scout_quality=_rescue_quality,
                            token_ca=w_entry['ca'],
                            symbol=w_entry['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=w_entry['signal_ts'],
                            signal_id=w_entry.get('premium_signal_id'),
                            route='LOTTO',
                            lifecycle=_lotto_lifecycle,
                            scout_size={
                                'entry_mode': _gmgn_rescue.get("entry_mode") or _extract_scout_mode({'scout_quality': _rescue_quality}),
                                'actual_size_sol': _gmgn_rescue.get("position_size_sol") or PAPER_TINY_SCOUT_SIZE_SOL,
                                'cap_sol': SCOUT_QUALITY_SIZE_CAP_SOL,
                            },
                            source_component='gmgn_tiny_scout_rescue',
                            source_reject_reason=_lotto_decision.reason,
                            data_source='dexscreener+gmgn+lifecycle',
                            event_ts=now,
                        )
                        if _gmgn_rescue.get("allow"):
                            _lotto_detail = {
                                **_lotto_detail,
                                **(_gmgn_rescue.get("detail") or {}),
                                "entry_mode": _gmgn_rescue.get("entry_mode"),
                                "position_size_sol": _gmgn_rescue.get("position_size_sol"),
                                "paper_only_scout": True,
                                "gmgn_tiny_scout": True,
                                "gmgn_tiny_scout_reason": _gmgn_rescue.get("reason"),
                            }
                            _lotto_decision = LottoDecision(
                                "allow",
                                _gmgn_rescue.get("reason") or "gmgn_tiny_scout_ok",
                                _lotto_detail,
                            )
                    _falling_knife_blocked, _falling_knife_detail = should_block_lotto_falling_knife(
                        _lotto_detail,
                        _lotto_lifecycle,
                    )
                    _lifecycle_blocked, _lifecycle_block_reason, _lifecycle_block_detail = should_block_lotto_lifecycle_entry(
                        _lotto_lifecycle,
                    )
                    _lotto_reclaim = evaluate_token_reclaim(
                        dex_snapshot=_lotto_dex,
                        lifecycle=_lotto_lifecycle,
                        route='LOTTO',
                    )
                    if _falling_knife_blocked and _lotto_decision.allow:
                        _falling_detail = {**_lotto_detail, **_falling_knife_detail}
                        _gmgn_rescue = evaluate_gmgn_tiny_scout_rescue(
                            "lotto_newborn_falling_knife_low_liq",
                            _gmgn_policy,
                            _falling_detail,
                        )
                        _rescue_quality = (_gmgn_rescue.get("detail") or {}).get("scout_quality")
                        record_scout_quality_decision(
                            db,
                            scout_quality=_rescue_quality,
                            token_ca=w_entry['ca'],
                            symbol=w_entry['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=w_entry['signal_ts'],
                            signal_id=w_entry.get('premium_signal_id'),
                            route='LOTTO',
                            lifecycle=_lotto_lifecycle,
                            scout_size={
                                'entry_mode': _gmgn_rescue.get("entry_mode") or _extract_scout_mode({'scout_quality': _rescue_quality}),
                                'actual_size_sol': _gmgn_rescue.get("position_size_sol") or PAPER_TINY_SCOUT_SIZE_SOL,
                                'cap_sol': SCOUT_QUALITY_SIZE_CAP_SOL,
                            },
                            source_component='gmgn_tiny_scout_rescue',
                            source_reject_reason='lotto_newborn_falling_knife_low_liq',
                            data_source='dexscreener+gmgn+lifecycle',
                            event_ts=now,
                        )
                        if _gmgn_rescue.get("allow"):
                            _lotto_decision = LottoDecision(
                                "allow",
                                _gmgn_rescue.get("reason") or "gmgn_unknown_data_tiny_scout_ok",
                                {
                                    **_falling_detail,
                                    **(_gmgn_rescue.get("detail") or {}),
                                    "entry_mode": _gmgn_rescue.get("entry_mode"),
                                    "position_size_sol": _gmgn_rescue.get("position_size_sol"),
                                    "paper_only_scout": True,
                                    "gmgn_tiny_scout": True,
                                    "gmgn_tiny_scout_reason": _gmgn_rescue.get("reason"),
                                },
                            )
                        else:
                            _lotto_decision = LottoDecision(
                                "expire",
                                "lotto_newborn_falling_knife_low_liq",
                                _falling_detail,
                            )
                        _lotto_detail = _lotto_decision.detail
                    elif _lifecycle_blocked and _lotto_decision.allow:
                        _lotto_decision = LottoDecision(
                            "wait",
                            _lifecycle_block_reason,
                            {**_lotto_detail, **_lifecycle_block_detail},
                        )
                        _lotto_detail = _lotto_decision.detail
                    _reclaim_watch = w_entry.get('_smart_entry_reclaim_watch') or {}
                    if (
                        _reclaim_watch
                        and _lotto_reclaim.get('reclaim_confirmed')
                        and _lotto_decision.allow
                        and _gmgn_policy.get('action') != 'reject'
                    ):
                        _lotto_detail = {
                            **_lotto_detail,
                            'entry_mode': 'smart_entry_reclaim_tiny_scout',
                            'position_size_sol': PAPER_TINY_SCOUT_SIZE_SOL,
                            'paper_only_scout': True,
                            'reclaim_watch': _reclaim_watch,
                            'reclaim': _lotto_reclaim,
                        }
                        _lotto_decision = LottoDecision(
                            "allow",
                            "smart_entry_reclaim_watch_ok",
                            _lotto_detail,
                        )
                    _token_risk = token_quarantine_state(db, w_entry['ca'], now_ts=now, reclaim=_lotto_reclaim)
                    if _token_risk.get('blocked') and _lotto_decision.allow:
                        _lotto_decision = LottoDecision(
                            "wait",
                            _token_risk.get('reason') or 'token_quarantine',
                            {**_lotto_detail, 'token_risk': _token_risk},
                        )
                        _lotto_detail = _lotto_decision.detail

                    _lotto_lc_id = build_lifecycle_id(w_entry['ca'], w_entry['signal_ts'])
                    if _lotto_lc_id not in pending_entries:
                        record_decision_event(
                            db,
                            component='lotto_entry_gate',
                            event_type='entry_gate',
                            decision=_lotto_decision.action,
                            reason=_lotto_decision.reason,
                            token_ca=w_entry['ca'],
                            symbol=w_entry['symbol'],
                            lifecycle_id=_lotto_lc_id,
                            signal_ts=w_entry['signal_ts'],
                            signal_id=w_entry.get('premium_signal_id'),
                            route='LOTTO',
                            data_source='dexscreener+helius+signal',
                            payload=with_lifecycle_payload(_lotto_detail, _lotto_lifecycle),
                        )
                        try:
                            watchlist.update_scores(w_entry['id'], {}, eval_time=time.time())
                        except Exception:
                            pass
                        if _lotto_decision.expire:
                            _discovery_mode = _discovery_mode_for_lotto_reason(_lotto_decision.reason)
                            if _discovery_mode and _gmgn_policy.get('action') != 'reject':
                                track_discovery_candidate(
                                    db,
                                    discovery_candidates,
                                    mode=_discovery_mode,
                                    route='LOTTO',
                                    token_ca=w_entry['ca'],
                                    symbol=w_entry['symbol'],
                                    lifecycle_id=_lotto_lc_id,
                                    signal_ts=w_entry['signal_ts'],
                                    signal_id=w_entry.get('premium_signal_id'),
                                    pool=w_entry.get('pool_address'),
                                    watchlist_id=w_entry.get('id'),
                                    watchlist_entry=w_entry,
                                    source_component='lotto_entry_gate',
                                    source_reject_reason=_lotto_decision.reason,
                                    source_detail={
                                        **_lotto_detail,
                                        'gmgn_policy': _gmgn_policy,
                                        'current_reclaim': _lotto_reclaim,
                                    },
                                    lifecycle=_lotto_lifecycle,
                                    now_ts=now,
                                )
                            watchlist.mark_expired(w_entry['id'], _lotto_decision.reason)
                            log.info(
                                f"  [LOTTO] ⛔ {w_entry['symbol']} SKIP: {_lotto_decision.reason} "
                                f"detail={_lotto_detail}"
                            )
                        elif not _lotto_decision.allow:
                            log.info(
                                f"  [LOTTO] {w_entry['symbol']} WAIT: {_lotto_decision.reason} "
                                f"detail={_lotto_detail}"
                            )
                        else:
                            _live_summary = (
                                f" live_top1={_lotto_live['top1_pct']:.0f}% live_top10={_lotto_live['top10_pct']:.0f}%"
                                if _lotto_live else ""
                            )
                            if _lotto_decision.reason in {
                                'lotto_concentrated_scout_ok',
                                'lotto_explosive_direct_scout_ok',
                                'gmgn_concentration_tiny_scout_ok',
                                'gmgn_midcap_near_miss_scout_ok',
                                'gmgn_unknown_data_tiny_scout_ok',
                                'gmgn_reclaim_tiny_scout_ok',
                                'smart_entry_reclaim_watch_ok',
                                'lotto_newborn_momentum_tiny_scout_ok',
                            }:
                                record_decision_event(
                                    db,
                                    component='lotto_entry_gate',
                                    event_type='scout_candidate',
                                    decision='candidate',
                                    reason=_lotto_decision.reason,
                                    token_ca=w_entry['ca'],
                                    symbol=w_entry['symbol'],
                                    lifecycle_id=_lotto_lc_id,
                                    signal_ts=w_entry['signal_ts'],
                                    signal_id=w_entry.get('premium_signal_id'),
                                    route='LOTTO',
                                    payload=with_lifecycle_payload({
                                        'position_size_sol': _lotto_detail.get('position_size_sol'),
                                        **_lotto_detail,
                                    }, _lotto_lifecycle),
                                )
                            log.info(
                                f"  [LOTTO] 🎰 FIRE {w_entry['symbol']}! "
                                f"MC=${w_entry.get('signal_mc', 0) or 0:.0f} "
                                f"liq=${_lotto_liq:.0f} top10={_lotto_top10:.0f}%{_live_summary} "
                                f"age={_lotto_age_sec:.0f}s"
                            )
                            pending_entries[_lotto_lc_id] = build_lotto_pending(
                                w_entry,
                                _lotto_lc_id,
                                detail=_lotto_detail,
                            )
                            pending_entries[_lotto_lc_id]['smart_entry_retries'] = _LOTTO_TIMING_RETRY_MEMORY.get(_lotto_lc_id, 0)
                            record_decision_event(
                                db,
                                component='lotto_entry_gate',
                                event_type='pending_entry',
                                decision='pending',
                                reason=_lotto_decision.reason,
                                token_ca=w_entry['ca'],
                                symbol=w_entry['symbol'],
                                lifecycle_id=_lotto_lc_id,
                                signal_ts=w_entry['signal_ts'],
                                signal_id=w_entry.get('premium_signal_id'),
                                route='LOTTO',
                                payload=with_lifecycle_payload({
                                    'position_size_sol': pending_entries[_lotto_lc_id].get('kelly_position_sol'),
                                    **_lotto_detail,
                                }, _lotto_lifecycle),
                            )
                            last_progress = time.time()
                    continue

                # Inject SUSTAINED_ATH flag for matrix evaluator to use (e.g. for timeout extension)
                _wl_sustained = False
                if hasattr(watchlist, '_ath_history'):
                    _wl_ca = w_entry.get('ca')
                    _wl_hist = watchlist._ath_history.get(_wl_ca, [])
                    if _wl_hist:
                        w_entry['last_ath_ts'] = _wl_hist[-1][0]
                    if len(_wl_hist) >= 3:
                        _first_ts, _first_px = _wl_hist[0]
                        _last_ts, _last_px = _wl_hist[-1]
                        if _last_px > _first_px and (_last_ts - _first_ts) >= 1800:
                            _wl_sustained = True
                w_entry['is_sustained_ath'] = _wl_sustained
                _external_alpha = safe_external_alpha_lookup(
                    db,
                    w_entry['ca'],
                    now=now,
                    signal_ts=w_entry['signal_ts'],
                )
                
                eval_res = matrix_evaluator.evaluate(w_entry)
                watchlist.update_scores(w_entry['id'], eval_res['scores'])
                record_decision_event(
                    db,
                    component='matrix_evaluator',
                    event_type='matrix_decision',
                    decision=eval_res.get('action', 'unknown'),
                    reason=eval_res.get('action_reason'),
                    token_ca=w_entry['ca'],
                    symbol=w_entry['symbol'],
                    lifecycle_id=lifecycle_id,
                    signal_ts=w_entry['signal_ts'],
                    signal_id=w_entry.get('premium_signal_id'),
                    route=w_entry.get('type'),
                    data_source='matrix_inputs',
                    payload={
                        'scores': eval_res.get('scores'),
                        'reasons': eval_res.get('reasons'),
                        'current_price': eval_res.get('current_price'),
                        'momentum_pct': eval_res.get('momentum_pct'),
                        'external_alpha': _external_alpha,
                    },
                )
                
                # Update price bounds if we got a price
                if eval_res.get('current_price') and eval_res['current_price'] > 0:
                    watchlist.update_price_bounds(w_entry['id'], eval_res['current_price'])

                # FIX 1: Track P-score history for dead-cat-bounce detection
                # If P ever hits 0, remember the timestamp — fast-lane should be blocked
                _wl_id = w_entry['id']
                if not hasattr(watchlist, '_p_zero_history'):
                    watchlist._p_zero_history = {}  # {wl_id: last_p_zero_timestamp}
                if not hasattr(watchlist, '_momentum_fail_counts'):
                    watchlist._momentum_fail_counts = {}  # {wl_id: consecutive_fail_count}
                _p_now = eval_res.get('scores', {}).get('price', 50)
                if _p_now == 0:
                    watchlist._p_zero_history[_wl_id] = time.time()

                # FIX 2: Track consecutive momentum failures
                if eval_res['action'] == 'fire':
                    watchlist._momentum_fail_counts[_wl_id] = 0  # reset on success
                elif eval_res.get('action_reason', '').startswith('momentum check failed'):
                    watchlist._momentum_fail_counts[_wl_id] = watchlist._momentum_fail_counts.get(_wl_id, 0) + 1

                if arm_ath_uncertainty_tiny_scout(
                    db,
                    pending_entries,
                    dict(positions),
                    w_entry=w_entry,
                    lifecycle_id=lifecycle_id,
                    eval_res=eval_res,
                    now_ts=now,
                    discovery_candidates=discovery_candidates,
                ):
                    log.info(
                        f"  [ATH_UNCERTAINTY_SCOUT] {w_entry['symbol']} pending "
                        f"reason={eval_res.get('action_reason')} size={ATH_UNCERTAINTY_TINY_SCOUT_SIZE_SOL:.3f}SOL"
                    )
                    last_progress = time.time()
                    continue

                if eval_res['action'] == 'remove':
                    watchlist.mark_expired(w_entry['id'], eval_res['action_reason'])
                    log.info(f"  [WATCHLIST] 🗑️ Removed {w_entry['symbol']}: {eval_res['action_reason']}")
                    # Cleanup tracking dicts
                    watchlist._p_zero_history.pop(_wl_id, None)
                    watchlist._momentum_fail_counts.pop(_wl_id, None)
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

                    # REENTRY CAP V8: Conditional re-entry (max 3 entries per token).
                    # V3 data (9/9 re-entries lost) was under old entry logic with no SmartEntry.
                    # V7→V8: raised from 2→3. SmartEntry now provides entry quality filtering
                    # that didn't exist when V3 data was collected. Memestock passed 5+ Matrix
                    # checks but was blocked by cap=2.
                    # Conditions:
                    #   1. entry_count < 3 (max 3 entries total)
                    #   2. last_exit_pnl > -8% (small shakeout OK, crash exit blocked)
                    #   3. momentum score >= 70 (reasonable, not borderline)
                    #   4. 5min cooldown (already enforced by per-CA cooldown above)
                    _entry_count = w_entry.get('entry_count', 0) or 0
                    if _entry_count >= 3:
                        log.info(
                            f"  [WATCHLIST] 🚫 {w_entry['symbol']} REENTRY_CAP: "
                            f"re-entry #{_entry_count+1} blocked (max 3 entries per token)."
                        )
                        continue
                    if _entry_count >= 1:
                        _last_exit_pnl = w_entry.get('last_exit_pnl')
                        _m_score = (eval_res.get('scores') or {}).get('momentum', 0) or 0
                        if _last_exit_pnl is not None and _last_exit_pnl < -0.08:
                            log.info(
                                f"  [WATCHLIST] 🚫 {w_entry['symbol']} REENTRY_BLOCK: "
                                f"re-entry #{_entry_count+1}, last_exit_pnl={_last_exit_pnl:.1%} < -8% "
                                f"(crash exit, too risky to re-enter)"
                            )
                            continue
                        if _m_score < 60:
                            log.info(
                                f"  [WATCHLIST] 🚫 {w_entry['symbol']} REENTRY_BLOCK: "
                                f"re-entry #{_entry_count+1}, M={_m_score} < 60 "
                                f"(momentum not strong enough for re-entry)"
                            )
                            continue
                        log.info(
                            f"  [WATCHLIST] ✅ {w_entry['symbol']} REENTRY ALLOWED: "
                            f"re-entry #{_entry_count+1}, last_pnl={_last_exit_pnl:.1%} > -8%, M={_m_score} >= 60"
                        )

                    # PRICE-GATE: For re-entries, current price must be above last entry price.
                    # Data: 100% of dead cat bounces had price below entry during dip.
                    # Genuine second waves (SOLANA +1030%, Crashout +1140%) never dipped below entry.
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

                    # REENTRY-P-GATE: re-entry #1+ must have P>=70 (token not exhausted)
                    # Data: GREKT re-entry#2 P=30 → -7.7%, FLASH re-entry#2 P=30 → trigger=-11.5%
                    if _entry_count >= 1:
                        _p_score = eval_res.get('scores', {}).get('price', 0)
                        if _p_score < 70:
                            log.info(
                                f"  [WATCHLIST] 🚫 {w_entry['symbol']} REENTRY-P-GATE: "
                                f"re-entry #{_entry_count+1} P={_p_score}<70, token exhausted"
                            )
                            continue

                    _is_ath_flat_tiny_fire = (
                        w_entry.get('type') == 'ATH'
                        and str(eval_res.get('action_reason') or '').startswith('ATH FLAT STRUCTURE TINY PASS')
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
                        # Fresh momentum data from 3×3s check (for SmartEntry to use
                        # instead of DexScreener's lagging pc_m5)
                        'momentum_snapshots': eval_res.get('momentum_snapshots', []),
                        'momentum_pct': eval_res.get('momentum_pct', 0),
                        # Track first-fire pc_m5 for trend decay detection.
                        # If this is the first FIRE, capture current pc_m5.
                        # On subsequent FIREs (after SPREAD_GUARD abort), preserve original.
                        'first_fire_pc_m5': w_entry.get('_first_fire_pc_m5'),
                        'spread_abort_count': w_entry.get('_spread_abort_count', 0),
                    }
                    if _is_ath_flat_tiny_fire:
                        pending_entries[lifecycle_id]['scout_mode'] = 'ath_flat_structure_tiny_scout'
                        pending_entries[lifecycle_id]['entry_mode'] = 'ath_flat_structure_tiny_scout'
                        pending_entries[lifecycle_id]['kelly_position_sol'] = PAPER_TINY_SCOUT_SIZE_SOL
                        pending_entries[lifecycle_id]['paper_only_scout'] = True
                        pending_entries[lifecycle_id]['ath_flat_structure_tiny_scout'] = True

                    # Note: Kelly always returns >= 0.03 SOL (never vetoes).
                    # Matrix+Momentum decide whether to trade; Kelly only sizes.

                    # P7: Minimum position threshold — skip if Kelly gives floor value
                    # Data: 0.03 SOL trades have terrible risk/reward (avg loss -15%, wins earn 0.002 SOL)
                    _kelly_sol = pending_entries[lifecycle_id]['kelly_position_sol']
                    
                    _fire_mc_for_size = float(w_entry.get('signal_mc') or 0)
                    _is_matrix_ath_fire = (w_entry.get('type') == 'ATH')
                    if _is_matrix_ath_fire and _is_ath_flat_tiny_fire:
                        log.info(
                            f"  [Kelly] {w_entry['symbol']} ATH flat-structure tiny scout: "
                            f"size={PAPER_TINY_SCOUT_SIZE_SOL:.3f} SOL"
                        )
                    elif _is_matrix_ath_fire:
                        if _fire_mc_for_size >= MATRIX_ATH_HALF_MC_MAX:
                            if ATH_HIGH_MC_TINY_PROBE_ENABLED and _fire_mc_for_size <= ATH_HIGH_MC_TINY_PROBE_MAX_MC:
                                pending_entries[lifecycle_id]['scout_mode'] = ATH_HIGH_MC_TINY_PROBE_MODE
                                pending_entries[lifecycle_id]['entry_mode'] = ATH_HIGH_MC_TINY_PROBE_MODE
                                pending_entries[lifecycle_id]['kelly_position_sol'] = PAPER_TINY_SCOUT_SIZE_SOL
                                pending_entries[lifecycle_id]['paper_only_scout'] = True
                                pending_entries[lifecycle_id]['ath_high_mc_tiny_probe'] = True
                                log.info(
                                    f"  [Kelly] {w_entry['symbol']} ATH high-MC tiny probe: "
                                    f"signal_mc=${_fire_mc_for_size:,.0f} >= ${MATRIX_ATH_HALF_MC_MAX:,.0f}, "
                                    f"size={PAPER_TINY_SCOUT_SIZE_SOL:.3f} SOL"
                                )
                                record_decision_event(
                                    db,
                                    component='matrix_ath_sizing',
                                    event_type='entry_size',
                                    decision='allow',
                                    reason='ath_high_mc_tiny_probe_allowed',
                                    token_ca=w_entry['ca'],
                                    symbol=w_entry['symbol'],
                                    lifecycle_id=lifecycle_id,
                                    signal_ts=w_entry['signal_ts'],
                                    signal_id=w_entry.get('premium_signal_id'),
                                    route='ATH',
                                    payload={
                                        'signal_mc': _fire_mc_for_size,
                                        'mc_cap': MATRIX_ATH_HALF_MC_MAX,
                                        'max_probe_mc': ATH_HIGH_MC_TINY_PROBE_MAX_MC,
                                        'entry_mode': ATH_HIGH_MC_TINY_PROBE_MODE,
                                        'position_size_sol': PAPER_TINY_SCOUT_SIZE_SOL,
                                    },
                                )
                            else:
                                log.info(
                                    f"  [Kelly] {w_entry['symbol']} ATH observe-only: "
                                    f"signal_mc=${_fire_mc_for_size:,.0f} >= ${MATRIX_ATH_HALF_MC_MAX:,.0f}"
                                )
                                record_decision_event(
                                    db,
                                    component='matrix_ath_sizing',
                                    event_type='signal_reject',
                                    decision='reject',
                                    reason='ath_high_mc_observe_only',
                                    token_ca=w_entry['ca'],
                                    symbol=w_entry['symbol'],
                                    lifecycle_id=lifecycle_id,
                                    signal_ts=w_entry['signal_ts'],
                                    signal_id=w_entry.get('premium_signal_id'),
                                    route='ATH',
                                    payload={'signal_mc': _fire_mc_for_size, 'mc_cap': MATRIX_ATH_HALF_MC_MAX},
                                )
                                pending_entries.pop(lifecycle_id, None)
                                continue
                        _ath_cap = MATRIX_ATH_HALF_SIZE_SOL if _fire_mc_for_size >= MATRIX_ATH_FULL_MC_MAX else MATRIX_ATH_FULL_SIZE_SOL
                        _old_kelly = _kelly_sol
                        _kelly_sol = _ath_cap
                        pending_entries[lifecycle_id]['kelly_position_sol'] = _kelly_sol
                        log.info(
                            f"  [Kelly] {w_entry['symbol']} ATH size tier: "
                            f"MC=${_fire_mc_for_size:,.0f} cap={_ath_cap:.3f} SOL "
                            f"Kelly {_old_kelly:.3f} → {_kelly_sol:.3f}"
                        )
                    elif _kelly_sol < 0.1:
                        # Non-ATH MATRIX keeps the old systemic floor.
                        log.info(f"  [Kelly] {w_entry['symbol']} Bumping Kelly={_kelly_sol:.3f} to systemic floor 0.1 SOL")
                        _kelly_sol = 0.1
                        pending_entries[lifecycle_id]['kelly_position_sol'] = _kelly_sol

                    # T-SCORE KELLY DISCOUNT: weaker trend = smaller position
                    # Data (8hr audit, 2026-04-24): X got 0.43 SOL at T=50 → -28% = -0.12 SOL loss.
                    # All T=50 trades lost. Reduce exposure when trend is weak.
                    _t_score_fire = eval_res.get('scores', {}).get('trend', 0)
                    if _is_matrix_ath_fire:
                        log.info(
                            f"  [Kelly] {w_entry['symbol']} ATH fixed tier size; "
                            f"skip T-score discount T={_t_score_fire}"
                        )
                    elif _t_score_fire <= 60:
                        _old_kelly = _kelly_sol
                        _kelly_sol = round(_kelly_sol * 0.5, 3)
                        _kelly_sol = max(_kelly_sol, 0.03)  # never below dust
                        pending_entries[lifecycle_id]['kelly_position_sol'] = _kelly_sol
                        log.info(f"  [Kelly] {w_entry['symbol']} T={_t_score_fire} discount: {_old_kelly:.3f} → {_kelly_sol:.3f} SOL (×0.5)")
                    elif _t_score_fire <= 80:
                        _old_kelly = _kelly_sol
                        _kelly_sol = round(_kelly_sol * 0.75, 3)
                        _kelly_sol = max(_kelly_sol, 0.03)
                        pending_entries[lifecycle_id]['kelly_position_sol'] = _kelly_sol
                        log.info(f"  [Kelly] {w_entry['symbol']} T={_t_score_fire} discount: {_old_kelly:.3f} → {_kelly_sol:.3f} SOL (×0.75)")

                    # P8: Liquidity floor — reject tokens with tiny pools
                    # Data: ROCCO/hallelujah/drone had 20%+ exit slippage due to
                    # pool < $5000, contributing 55% of overnight total losses.
                    _fire_dex = None
                    try:
                        _fire_dex = fetch_dexscreener_trend_snapshot(w_entry['ca'])
                        _fire_liq = (_fire_dex.get('liquidity_usd', 0) or 0) if _fire_dex else 0
                        
                        # Vol/MC Cross-Validation (Phase 3)
                        _fire_mc = w_entry.get('signal_mc') or 0
                        _fire_vol = _fire_dex.get('vol_m5', 0) if _fire_dex else 0
                        _vol_mc_pct = (_fire_vol / _fire_mc * 100) if _fire_mc > 0 else 0
                        if _vol_mc_pct > 0:
                            log.info(f"  [FIRE] {w_entry['symbol']} Vol/MC={_vol_mc_pct:.1f}% (vol_m5=${_fire_vol:.0f} MC=${_fire_mc:.0f})")

                        if 0 < _fire_liq < 5000:
                            log.info(f"  [WATCHLIST] ⛔ {w_entry['symbol']} SKIP: liquidity=${_fire_liq:.0f} < $5000 (exit slippage risk)")
                            pending_entries.pop(lifecycle_id, None)
                            continue

                        # === V8: MC CAP GATE ===
                        # Use signal_mc (MC at signal time, parsed from Telegram message).
                        # Simpler and more reliable than DexScreener FDV — no extra API call,
                        # no NULL/zero ambiguity from FDV fields.
                        # Fail-open: if signal had no MC data (signal_mc=0), always allow.
                        MC_CAP = 200_000  # $200K — real K-line review shows >$200K ATH/NT loses convexity
                        if _fire_mc > MC_CAP:
                            _pending_for_mc = pending_entries.get(lifecycle_id) or {}
                            if pending_is_paper_tiny_scout(_pending_for_mc):
                                log.info(
                                    f"  [WATCHLIST] 🧪 {w_entry['symbol']} ATH high-MC tiny probe allowed: "
                                    f"signal_mc=${_fire_mc:,.0f} > ${MC_CAP:,.0f}, "
                                    f"mode={_pending_for_mc.get('entry_mode')}"
                                )
                                record_decision_event(
                                    db,
                                    component='matrix_ath_sizing',
                                    event_type='entry_gate',
                                    decision='allow',
                                    reason='ath_high_mc_tiny_probe_mc_cap_bypass',
                                    token_ca=w_entry['ca'],
                                    symbol=w_entry['symbol'],
                                    lifecycle_id=lifecycle_id,
                                    signal_ts=w_entry['signal_ts'],
                                    signal_id=w_entry.get('premium_signal_id'),
                                    route='ATH',
                                    payload={
                                        'signal_mc': _fire_mc,
                                        'mc_cap': MC_CAP,
                                        'entry_mode': _pending_for_mc.get('entry_mode'),
                                    },
                                )
                            else:
                                log.info(
                                    f"  [WATCHLIST] ⛔ {w_entry['symbol']} SKIP: signal_mc=${_fire_mc:,.0f} > ${MC_CAP:,.0f} "
                                    f"(chasing top — upside capped, spread will eat profits)"
                                )
                                pending_entries.pop(lifecycle_id, None)
                                continue
                        elif _fire_mc > 0:
                            log.info(f"  [FIRE] {w_entry['symbol']} MC_OK: signal_mc=${_fire_mc:,.0f} < ${MC_CAP:,.0f}")

                    except Exception:
                        pass  # fail-open if DexScreener unavailable

                    # FIX 2: Consecutive momentum failure gate
                    # boobcoin postmortem: 7 consecutive momentum failures = extreme volatility/manipulation.
                    # After 5+ consecutive fails, require stronger buy pressure (bs>1.5) to proceed.
                    _wl_id_fire = w_entry['id']
                    _consec_m_fails = watchlist._momentum_fail_counts.get(_wl_id_fire, 0) if hasattr(watchlist, '_momentum_fail_counts') else 0
                    if _consec_m_fails >= 5:
                        # Token has been failing momentum for a long time — it's unstable
                        # Only allow if DexScreener confirms strong buy pressure right now
                        try:
                            _mf_dex = fetch_dexscreener_trend_snapshot(w_entry['ca'])
                            _mf_bs = (_mf_dex.get('buys_m5', 0) / max(_mf_dex.get('sells_m5', 1), 1)) if _mf_dex else 0
                            if _mf_bs < 1.5:
                                log.info(
                                    f"  [WATCHLIST] ⚠️ {w_entry['symbol']} MOMENTUM_INSTABILITY: "
                                    f"{_consec_m_fails} consecutive momentum fails, bs={_mf_bs:.2f}<1.5 → extra caution, skip"
                                )
                                continue
                            else:
                                log.info(
                                    f"  [WATCHLIST] ✅ {w_entry['symbol']} MOMENTUM_INSTABILITY: "
                                    f"{_consec_m_fails} fails BUT bs={_mf_bs:.2f}≥1.5 → proceed with caution"
                                )
                        except Exception:
                            pass  # fail-open

                    # FIX 3: Top10 concentration gate — block if signal had Top10 > 45%
                    _sig_top10 = w_entry.get('signal_top10', 0) or 0
                    if _sig_top10 > 45:
                        log.info(
                            f"  [WATCHLIST] 🚫 {w_entry['symbol']} TOP10_BLOCK: "
                            f"signal Top10={_sig_top10:.1f}% > 45% (insider concentration), skipping"
                        )
                        continue

                    _preflight_lifecycle = lifecycle_payload_for(
                        watchlist_entry=w_entry,
                        dex_snapshot=_fire_dex,
                        route=w_entry.get('type'),
                        signal_ts=w_entry.get('signal_ts'),
                        signal_price=w_entry.get('signal_price'),
                        now=now,
                    )
                    _preflight_policy = evaluate_entry_readiness_policy(
                        route=w_entry.get('type'),
                        lifecycle=_preflight_lifecycle,
                        pending=pending_entries.get(lifecycle_id) or {
                            'token_ca': w_entry['ca'],
                            'symbol': w_entry['symbol'],
                            'signal_ts': w_entry['signal_ts'],
                            'signal_type': w_entry['type'],
                            'signal_route': w_entry['type'],
                        },
                        now_ts=now,
                    )
                    if _preflight_policy.decision == 'EXPIRE':
                        try:
                            watchlist.mark_expired(w_entry['id'], _preflight_policy.reason)
                        except Exception:
                            pass
                        record_decision_event(
                            db,
                            component='entry_readiness',
                            event_type='watchlist_fire_expired',
                            decision='expire',
                            reason=_preflight_policy.reason,
                            token_ca=w_entry['ca'],
                            symbol=w_entry['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=w_entry['signal_ts'],
                            signal_id=w_entry.get('premium_signal_id'),
                            route=w_entry.get('type'),
                            data_source='watchlist_preflight+lifecycle+dexscreener',
                            payload=_preflight_policy.to_dict(),
                        )
                        pending_entries.pop(lifecycle_id, None)
                        log.info(
                            f"  [WATCHLIST] ⛔ {w_entry['symbol']} FIRE_EXPIRED: "
                            f"{_preflight_policy.reason} profile={_preflight_policy.lifecycle_profile}"
                        )
                        continue
                    if _preflight_policy.decision == 'WAIT':
                        _pf_cooldown = 600 if _preflight_policy.reason == 'entry_readiness_stale_ath_requires_fresh_high' else 300
                        try:
                            watchlist.defer_fire(w_entry['id'], _preflight_policy.reason, cooldown_sec=_pf_cooldown)
                        except Exception:
                            pass
                        record_decision_event(
                            db,
                            component='entry_readiness',
                            event_type='watchlist_fire_deferred',
                            decision='wait',
                            reason=_preflight_policy.reason,
                            token_ca=w_entry['ca'],
                            symbol=w_entry['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=w_entry['signal_ts'],
                            signal_id=w_entry.get('premium_signal_id'),
                            route=w_entry.get('type'),
                            data_source='watchlist_preflight+lifecycle+dexscreener',
                            payload=_preflight_policy.to_dict(),
                        )
                        pending_entries.pop(lifecycle_id, None)
                        log.info(
                            f"  [WATCHLIST] ⏳ {w_entry['symbol']} FIRE_DEFERRED: "
                            f"{_preflight_policy.reason} profile={_preflight_policy.lifecycle_profile}; "
                            f"cooldown={_pf_cooldown}s"
                        )
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
                    # === SmartEntry Unified Scoring V2 (Async) ===
                    if not pending.get('timing_passed'):
                        if not pending.get('entry_readiness_policy'):
                            try:
                                _policy_dex = fetch_dexscreener_trend_snapshot(pending['token_ca'])
                            except Exception:
                                _policy_dex = None
                            _policy_lifecycle_entry = pending_w_entry or {
                                'ca': pending['token_ca'],
                                'symbol': pending['symbol'],
                                'type': pending.get('signal_route') or pending.get('signal_type'),
                                'signal_ts': pending['signal_ts'],
                                'signal_price': pending.get('signal_price') or pending.get('entry_price'),
                                'signal_mc': pending.get('market_cap') or 0,
                                'added_at': pending.get('added_at') or pending.get('staged_at') or pending['signal_ts'],
                            }
                            _policy_lifecycle = lifecycle_payload_for(
                                watchlist_entry=_policy_lifecycle_entry,
                                dex_snapshot=_policy_dex,
                                route=pending.get('signal_route') or pending.get('signal_type'),
                                signal_ts=pending['signal_ts'],
                                signal_price=pending.get('signal_price') or pending.get('entry_price'),
                                quote_available=None,
                                now=now,
                            )
                            pending['entry_readiness_lifecycle'] = _policy_lifecycle
                            _policy = evaluate_entry_readiness_policy(
                                route=pending.get('signal_route') or pending.get('signal_type'),
                                lifecycle=_policy_lifecycle,
                                pending=pending,
                                now_ts=now,
                            )
                            _policy_dict = _policy.to_dict()
                            _pending_gmgn_policy = (
                                (pending.get('lotto_state') or {})
                                .get('entryDecision', {})
                                .get('gmgn_policy')
                            )
                            if _pending_gmgn_policy:
                                _policy_dict['gmgn_policy'] = _pending_gmgn_policy
                                _policy_dict.setdefault('detail', {})['gmgn_policy'] = _pending_gmgn_policy
                            pending['entry_readiness_policy'] = _policy_dict
                            if _policy.decision == 'EXPIRE':
                                record_decision_event(
                                    db,
                                    component='entry_readiness',
                                    event_type='entry_block',
                                    decision='expire',
                                    reason=_policy.reason,
                                    token_ca=pending['token_ca'],
                                    symbol=pending['symbol'],
                                    lifecycle_id=lifecycle_id,
                                    signal_ts=pending['signal_ts'],
                                    signal_id=pending.get('premium_signal_id'),
                                    route=pending.get('signal_route') or pending.get('signal_type'),
                                    data_source='lifecycle+dexscreener',
                                    payload=_policy.to_dict(),
                                )
                                pending_entries.pop(lifecycle_id, None)
                                continue
                            if _policy.decision == 'WAIT':
                                record_decision_event(
                                    db,
                                    component='entry_readiness',
                                    event_type='entry_block',
                                    decision='wait',
                                    reason=_policy.reason,
                                    token_ca=pending['token_ca'],
                                    symbol=pending['symbol'],
                                    lifecycle_id=lifecycle_id,
                                    signal_ts=pending['signal_ts'],
                                    signal_id=pending.get('premium_signal_id'),
                                    route=pending.get('signal_route') or pending.get('signal_type'),
                                    data_source='lifecycle+dexscreener',
                                    payload=_policy.to_dict(),
                                )
                                log.info(
                                    f"  [ENTRY_READINESS] {pending['symbol']} WAIT: "
                                    f"{_policy.reason} profile={_policy.lifecycle_profile}; back to watchlist"
                                )
                                pending_entries.pop(lifecycle_id, None)
                                continue
                            record_decision_event(
                                db,
                                component='entry_readiness',
                                event_type='entry_arm',
                                decision=_policy.decision.lower(),
                                reason=_policy.reason,
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                route=pending.get('signal_route') or pending.get('signal_type'),
                                data_source='lifecycle+dexscreener',
                                payload=_policy.to_dict(),
                            )
                        _spread_memory = evaluate_spread_abort_memory(
                            db,
                            pending.get('token_ca'),
                            lifecycle=pending.get('entry_readiness_lifecycle'),
                            now_ts=now,
                        )
                        pending['spread_abort_memory'] = _spread_memory
                        _memory_abort_count = int(_spread_memory.get('abort_count') or 0)
                        _pending_tiny_scout = pending_is_paper_tiny_scout(pending)
                        if (
                            _memory_abort_count > int(pending.get('spread_abort_count') or 0)
                            and not _pending_tiny_scout
                        ):
                            pending['spread_abort_count'] = _memory_abort_count
                        if _spread_memory.get('blocked'):
                            if _pending_tiny_scout:
                                log.info(
                                    f"  [SPREAD_MEMORY] ⚠️ {pending['symbol']} tiny scout defers "
                                    f"{_memory_abort_count} prior spread abort(s) to live quote guard"
                                )
                                record_decision_event(
                                    db,
                                    component='entry_readiness',
                                    event_type='entry_block',
                                    decision='warn',
                                    reason='spread_abort_memory_tiny_scout_deferred',
                                    token_ca=pending['token_ca'],
                                    symbol=pending['symbol'],
                                    lifecycle_id=lifecycle_id,
                                    signal_ts=pending['signal_ts'],
                                    signal_id=pending.get('premium_signal_id'),
                                    route=pending.get('signal_route') or pending.get('signal_type'),
                                    data_source='paper_decision_events',
                                    payload=_spread_memory,
                                )
                            else:
                                log.info(
                                    f"  [SPREAD_MEMORY] 🚫 {pending['symbol']} BLOCKED: "
                                    f"{_memory_abort_count} spread abort(s), age={_spread_memory.get('age_sec', 0):.0f}s; "
                                    f"waiting for reclaim"
                                )
                                record_decision_event(
                                    db,
                                    component='entry_readiness',
                                    event_type='entry_block',
                                    decision='wait',
                                    reason='spread_abort_memory_wait_reclaim',
                                    token_ca=pending['token_ca'],
                                    symbol=pending['symbol'],
                                    lifecycle_id=lifecycle_id,
                                    signal_ts=pending['signal_ts'],
                                    signal_id=pending.get('premium_signal_id'),
                                    route=pending.get('signal_route') or pending.get('signal_type'),
                                    data_source='paper_decision_events',
                                    payload=_spread_memory,
                                )
                                pending_entries.pop(lifecycle_id, None)
                                continue
                        _se_sustained = False
                        if hasattr(watchlist, '_ath_history'):
                            _se_ca = pending.get('token_ca')
                            _se_hist = watchlist._ath_history.get(_se_ca, [])
                            if len(_se_hist) >= 3:
                                _first_ts, _first_px = _se_hist[0]
                                _last_ts, _last_px = _se_hist[-1]
                                if _last_px > _first_px and (_last_ts - _first_ts) >= 1800:
                                    _se_sustained = True

                        _se_future = pending.get('_smart_entry_future')
                        if _se_future is None:
                            _se_future = smart_entry_pool.submit(
                                evaluate_smart_entry,
                                pending['token_ca'],
                                symbol=pending['symbol'],
                                pool_address=pending['pool'],
                                entry_count=_entry_count,
                                momentum_snapshots=pending.get('momentum_snapshots', []),
                                momentum_pct=pending.get('momentum_pct', 0),
                                sustained_ath=_se_sustained,
                                first_fire_pc_m5=pending.get('first_fire_pc_m5'),
                                spread_abort_count=(
                                    0 if pending_is_paper_tiny_scout(pending)
                                    else pending.get('spread_abort_count', 0)
                                ),
                                entry_readiness_policy=pending.get('entry_readiness_policy'),
                                matrix_scores=pending.get('matrix_scores'),
                            )
                            pending['_smart_entry_future'] = _se_future
                            continue

                        if not _se_future.done():
                            continue

                        try:
                            should_enter, timing_reason, timing_detail, timing_trigger_price = _se_future.result()
                        except Exception as _se_err:
                            log.error(f"  [SmartEntry] {pending['symbol']} evaluation error: {_se_err}", exc_info=True)
                            pending_entries.pop(lifecycle_id, None)
                            continue

                        if not should_enter:
                            retry_count = pending.get('smart_entry_retries', 0)
                            _reclaimable_timing_reject = (
                                timing_reason in {
                                    'negative_trend',
                                    'post_spread_abort',
                                    'kline_trend_reversed',
                                }
                                or str(timing_reason or '').startswith('dead_cat')
                                or str(timing_reason or '').startswith('lotto_dead_cat')
                            )
                            # Give negative_trend rejects extra retries: pullback may resolve
                            # within 30-60s as DexScreener data refreshes or price recovers.
                            # Standard 3 retries would drop the token too quickly.
                            max_retries = 5 if timing_reason == 'negative_trend' else 3
                            if pending.get('is_lotto'):
                                _LOTTO_TIMING_RETRY_MEMORY[lifecycle_id] = retry_count + 1
                            record_decision_event(
                                db,
                                component='smart_entry',
                                event_type='timing_decision',
                                decision='reject',
                                reason=timing_reason,
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                route=pending.get('signal_route') or pending.get('signal_type'),
                                payload={
                                    'detail': timing_detail,
                                    'retry_count': retry_count,
                                    'max_retries': max_retries,
                                    'trigger_price': timing_trigger_price,
                                    'entry_readiness_policy': pending.get('entry_readiness_policy'),
                                    'matrix_scores': pending.get('matrix_scores'),
                                },
                            )
                            if pending_w_entry and _reclaimable_timing_reject:
                                pending_w_entry['_smart_entry_reclaim_watch'] = {
                                    'armed_at': now,
                                    'reject_reason': timing_reason,
                                    'reject_detail': timing_detail,
                                    'retry_count': retry_count,
                                    'max_retries': max_retries,
                                }
                            if retry_count >= max_retries:
                                log.info(f"  [SmartEntry] {pending['symbol']} REJECT (final, {retry_count}/{max_retries}): {timing_reason} {timing_detail}")
                                _LOTTO_TIMING_RETRY_MEMORY.pop(lifecycle_id, None)
                                pending_entries.pop(lifecycle_id, None)
                            else:
                                log.info(
                                    f"  [SmartEntry] {pending['symbol']} REJECT → back to watchlist "
                                    f"(retry {retry_count+1}/{max_retries}): {timing_reason} {timing_detail}"
                                )
                                if pending_w_entry:
                                    pending_w_entry['_smart_entry_retries'] = retry_count + 1
                                pending_entries.pop(lifecycle_id, None)
                            continue
                            
                        # Smart entry passed — update trigger price to the confirmed entry price
                        pending['timing_passed'] = True
                        _LOTTO_TIMING_RETRY_MEMORY.pop(lifecycle_id, None)
                        if timing_trigger_price:
                            pending['trigger_price'] = timing_trigger_price
                        _scout_mode = pending.get('scout_mode') or pending.get('entry_mode')
                        if _scout_mode in PAPER_TINY_SCOUT_ENTRY_MODES:
                            pending['timing_entry_mode'] = timing_reason
                            _apply_actual_tiny_trigger_mode(pending, timing_reason)
                        else:
                            pending['entry_mode'] = timing_reason
                            pending['entry_trigger_mode'] = timing_reason
                        record_decision_event(
                            db,
                            component='smart_entry',
                            event_type='timing_decision',
                            decision='pass',
                            reason=timing_reason,
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            route=pending.get('signal_route') or pending.get('signal_type'),
                            payload={
                                'detail': timing_detail,
                                'trigger_price': timing_trigger_price,
                                'actual_entry_mode': pending.get('entry_mode'),
                                'parent_scout_mode': pending.get('parent_scout_mode'),
                                'entry_trigger_mode': pending.get('entry_trigger_mode'),
                                'entry_readiness_policy': pending.get('entry_readiness_policy'),
                                'matrix_scores': pending.get('matrix_scores'),
                            },
                        )
                        log.info(f"  [SmartEntry] {pending['symbol']} PASS: {timing_reason} trigger={timing_trigger_price}")
                        _ath_reentry_guard = _ath_no_kline_reentry_guard(
                            db,
                            pending,
                            current_price=timing_trigger_price or pending.get('trigger_price'),
                            now_ts=now,
                        )
                        pending['ath_no_kline_reentry_guard'] = _ath_reentry_guard
                        if not _ath_reentry_guard.get('pass'):
                            _ath_reentry_defer = _defer_ath_reentry_block(
                                watchlist,
                                pending_w_entry,
                                _ath_reentry_guard,
                            )
                            if _ath_reentry_defer:
                                _ath_reentry_guard['watchlist_fire_block'] = _ath_reentry_defer
                            record_decision_event(
                                db,
                                component='entry_reentry_guard',
                                event_type='entry_block',
                                decision='block',
                                reason=_ath_reentry_guard.get('reason') or 'ath_no_kline_reentry_block',
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                strategy_stage=pending.get('strategy_stage') or 'stage1',
                                route=pending.get('signal_route') or pending.get('signal_type'),
                                data_source='paper_trades+pending_entry',
                                payload=_ath_reentry_guard,
                            )
                            log.info(
                                f"  [ATH_REENTRY] 🚫 {pending['symbol']} BLOCKED: "
                                f"{_ath_reentry_guard.get('reason')} "
                                f"recent={_ath_reentry_guard.get('recent_trade_count')} "
                                f"scores={_ath_reentry_guard.get('scores')} "
                                f"fire_block={(_ath_reentry_defer or {}).get('cooldown_sec')}"
                            )
                            pending_entries.pop(lifecycle_id, None)
                            continue

                    _pending_strategy_id = pending.get('strategy_id') or strategy_id
                    _pending_strategy_stage = pending.get('strategy_stage') or 'stage1'
                    _pending_stage_outcome = pending.get('stage_outcome') or f"{_pending_strategy_stage}_entered"
                    _pending_replay_source = pending.get('replay_source') or 'live_monitor'
                    _pending_signal_route = pending.get('signal_route') or ('LOTTO' if pending.get('is_lotto') else None)
                    _pending_lotto_state = pending.get('lotto_state') or None
                    _pending_exit_strategy = pending.get('exit_strategy') or 'NOT_ATH'

                    try:
                        _entry_timing_dex = fetch_dexscreener_trend_snapshot(pending['token_ca'])
                    except Exception:
                        _entry_timing_dex = None
                    _ath_no_kline_followthrough = _ath_no_kline_followthrough_guard(
                        pending,
                        _entry_timing_dex,
                    )
                    pending['ath_no_kline_followthrough_guard'] = _ath_no_kline_followthrough
                    if _ath_no_kline_followthrough.get('reason') != 'not_ath_no_kline_tiny_probe':
                        record_decision_event(
                            db,
                            component='ath_no_kline_quality',
                            event_type='followthrough_gate',
                            decision='pass' if _ath_no_kline_followthrough.get('pass') else 'block',
                            reason=_ath_no_kline_followthrough.get('reason') or 'ath_no_kline_followthrough',
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            strategy_stage=_pending_strategy_stage,
                            route=_pending_signal_route or pending.get('signal_type'),
                            data_source='dexscreener+matrix+pending_entry',
                            payload=_ath_no_kline_followthrough,
                        )
                    if not _ath_no_kline_followthrough.get('pass'):
                        log.info(
                            f"  [ATH_NO_KLINE_QUALITY] 🚫 {pending['symbol']} BLOCKED: "
                            f"{_ath_no_kline_followthrough.get('reason')} "
                            f"observed={_ath_no_kline_followthrough.get('observed')}"
                        )
                        pending_entries.pop(lifecycle_id, None)
                        continue
                    _entry_lifecycle_entry = pending_w_entry or {
                        'ca': pending['token_ca'],
                        'symbol': pending['symbol'],
                        'type': _pending_signal_route or pending.get('signal_type'),
                        'signal_ts': pending['signal_ts'],
                        'signal_price': pending.get('signal_price') or pending.get('entry_price'),
                        'signal_mc': pending.get('market_cap') or 0,
                        'added_at': pending.get('added_at') or pending['signal_ts'],
                    }
                    _entry_timing_lifecycle = lifecycle_payload_for(
                        watchlist_entry=_entry_lifecycle_entry,
                        dex_snapshot=_entry_timing_dex,
                        route=_pending_signal_route or pending.get('signal_type'),
                        signal_ts=pending['signal_ts'],
                        signal_price=pending.get('signal_price') or pending.get('entry_price'),
                        quote_available=None,
                        now=now,
                    )
                    _entry_reclaim = evaluate_token_reclaim(
                        dex_snapshot=_entry_timing_dex,
                        lifecycle=_entry_timing_lifecycle,
                        route=_pending_signal_route or pending.get('signal_type'),
                    )

                    _token_risk = token_quarantine_state(
                        db,
                        pending['token_ca'],
                        now_ts=now,
                        reclaim=_entry_reclaim,
                    )
                    if _token_risk.get('blocked'):
                        _is_tiny_scout_pending = pending_is_paper_tiny_scout(pending)
                        # Tiny scouts (0.003 SOL probes) bypass quarantine when the cooldown
                        # period has expired and only reclaim is missing. Rationale: spread_abort
                        # memory already has this exemption pattern (line ~8229). A 0.003 SOL
                        # probe is specifically designed to gather data on uncertain tokens —
                        # blocking it defeats its purpose. Hard cooldown (remaining_sec > 0) still
                        # applies to tiny scouts (don't probe while still hot).
                        _quarantine_cooldown_expired = _token_risk.get('cooldown_expired', False)
                        if _is_tiny_scout_pending and _quarantine_cooldown_expired:
                            log.info(
                                f"  [TOKEN_RISK] ⚠️ {pending['symbol']} quarantine deferred for tiny scout "
                                f"({_token_risk.get('reason')}, cooldown expired, "
                                f"failures={_token_risk.get('severe_failure_count')})"
                            )
                            record_decision_event(
                                db,
                                component='token_risk',
                                event_type='entry_block',
                                decision='warn',
                                reason='token_quarantine_tiny_scout_deferred',
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                strategy_stage=_pending_strategy_stage,
                                route=_pending_signal_route or pending.get('signal_type'),
                                data_source='paper_trade_history+dexscreener+lifecycle',
                                payload=_token_risk,
                            )
                            # Allow tiny scout to proceed — do not pop or continue
                        else:
                            record_decision_event(
                                db,
                                component='token_risk',
                                event_type='entry_block',
                                decision='block',
                                reason=_token_risk.get('reason') or 'token_quarantine',
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                strategy_stage=_pending_strategy_stage,
                                route=_pending_signal_route or pending.get('signal_type'),
                                data_source='paper_trade_history+dexscreener+lifecycle',
                                payload=_token_risk,
                            )
                            log.info(
                                f"  [TOKEN_RISK] 🚫 {pending['symbol']} BLOCKED: "
                                f"{_token_risk.get('reason')} remaining={_token_risk.get('remaining_sec', 0):.0f}s "
                                f"failures={_token_risk.get('severe_failure_count')}"
                            )
                            pending_entries.pop(lifecycle_id, None)
                            continue

                    if pending_is_paper_tiny_scout(pending):
                        _scout_size_detail = apply_paper_tiny_scout_size_cap(pending)
                        if _scout_size_detail.get('capped'):
                            record_decision_event(
                                db,
                                component='scout_sizing',
                                event_type='entry_size',
                                decision='cap',
                                reason='paper_tiny_scout_size_cap',
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                strategy_stage=_pending_strategy_stage,
                                route=_pending_signal_route or pending.get('signal_type'),
                                data_source='pending_entry',
                                payload=_scout_size_detail,
                            )
                            log.info(
                                f"  [SCOUT_SIZE] {pending['symbol']} "
                                f"{_scout_size_detail.get('entry_mode')} capped "
                                f"{_scout_size_detail.get('requested_size_sol'):.3f} -> "
                                f"{_scout_size_detail.get('actual_size_sol'):.3f} SOL"
                            )
                        _scout_gmgn_policy = (
                            ((pending.get('lotto_state') or {}).get('entryDecision') or {}).get('gmgn_policy')
                            or pending.get('gmgn_policy')
                            or (pending.get('entry_readiness_policy') or {}).get('gmgn_policy')
                        )
                        _scout_quality = evaluate_scout_quality(
                            mode=pending.get('entry_mode') or pending.get('scout_mode'),
                            route=_pending_signal_route or pending.get('signal_type'),
                            trend=_entry_timing_dex,
                            lifecycle=_entry_timing_lifecycle,
                            gmgn=_scout_gmgn_policy,
                            token_risk=_token_risk,
                            spread_memory=pending.get('spread_abort_memory'),
                            position_size_sol=pending.get('kelly_position_sol'),
                        )
                        _raw_scout_quality = _scout_quality
                        _scout_quality = _ath_no_kline_scout_quality_soft_override(
                            pending,
                            _scout_quality,
                            route=_pending_signal_route or pending.get('signal_type'),
                            scout_size=_scout_size_detail,
                        )
                        pending['scout_quality'] = _scout_quality
                        record_scout_quality_decision(
                            db,
                            scout_quality=_scout_quality,
                            pending=pending,
                            lifecycle_id=lifecycle_id,
                            strategy_stage=_pending_strategy_stage,
                            route=_pending_signal_route or pending.get('signal_type'),
                            lifecycle=_entry_timing_lifecycle,
                            scout_size=_scout_size_detail,
                            source_component=pending.get('source_component') or 'pending_entry',
                            source_reject_reason=pending.get('source_reject_reason'),
                            data_source='dexscreener+lifecycle+paper_risk',
                        )
                        if _scout_quality.get('decision') == 'warn':
                            record_decision_event(
                                db,
                                component='scout_quality',
                                event_type='quality_gate',
                                decision='warn',
                                reason=_scout_quality.get('reason') or 'scout_quality_warn',
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                strategy_stage=_pending_strategy_stage,
                                route=_pending_signal_route or pending.get('signal_type'),
                                data_source='dexscreener+lifecycle+paper_risk',
                                payload=with_lifecycle_payload({
                                    'scout_quality': _scout_quality,
                                    'raw_scout_quality': _raw_scout_quality,
                                    'scout_size': _scout_size_detail,
                                }, _entry_timing_lifecycle),
                            )
                            log.info(
                                f"  [SCOUT_QUALITY] ⚠️ {pending['symbol']} WARN: "
                                f"{_scout_quality.get('reason')} original={_scout_quality.get('original_reason')}"
                            )
                        if not _scout_quality.get('pass'):
                            if _discovery_is_soft_quality_reason(_scout_quality.get('reason')):
                                _pending_route_for_tracking = _pending_signal_route or pending.get('signal_type')
                                _pending_source_reason = pending.get('source_reject_reason') or pending.get('entry_mode')
                                if str(_pending_route_for_tracking or '').upper() == 'LOTTO' or pending.get('is_lotto'):
                                    _pending_discovery_mode = (
                                        _lotto_recovery_mode_for_blocker(
                                            primary_reason=_pending_source_reason,
                                            secondary_reason=_scout_quality.get('reason'),
                                            current_mode=pending.get('entry_mode') or pending.get('scout_mode'),
                                        )
                                        or pending.get('entry_mode')
                                        or pending.get('scout_mode')
                                    )
                                else:
                                    _pending_discovery_mode = _discovery_mode_for_ath_reason(_pending_source_reason)
                                track_discovery_candidate(
                                    db,
                                    discovery_candidates,
                                    mode=_pending_discovery_mode,
                                    route=_pending_route_for_tracking,
                                    token_ca=pending['token_ca'],
                                    symbol=pending['symbol'],
                                    lifecycle_id=lifecycle_id,
                                    signal_ts=pending['signal_ts'],
                                    signal_id=pending.get('premium_signal_id'),
                                    pool=pending.get('pool'),
                                    watchlist_id=pending.get('watchlist_id'),
                                    watchlist_entry=pending_w_entry,
                                    source_component=pending.get('source_component') or 'pending_entry',
                                    source_reject_reason=(
                                        _scout_quality.get('reason')
                                        if (str(_pending_route_for_tracking or '').upper() == 'LOTTO' or pending.get('is_lotto'))
                                        else _pending_source_reason
                                    ),
                                    source_detail={
                                        'original_source_reject_reason': _pending_source_reason,
                                        'pending_entry_quality_reject_reason': _scout_quality.get('reason'),
                                        'entry_mode': pending.get('entry_mode'),
                                        'scout_quality': _scout_quality,
                                        'scout_size': _scout_size_detail,
                                    },
                                    lifecycle=_entry_timing_lifecycle,
                                    now_ts=now,
                                )
                            record_decision_event(
                                db,
                                component='scout_quality',
                                event_type='entry_block',
                                decision='block',
                                reason=_scout_quality.get('reason') or 'scout_quality_reject',
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                strategy_stage=_pending_strategy_stage,
                                route=_pending_signal_route or pending.get('signal_type'),
                                data_source='dexscreener+lifecycle+paper_risk',
                                payload=with_lifecycle_payload({
                                    'scout_quality': _scout_quality,
                                    'scout_size': _scout_size_detail,
                                }, _entry_timing_lifecycle),
                            )
                            log.info(
                                f"  [SCOUT_QUALITY] 🚫 {pending['symbol']} BLOCKED: "
                                f"{_scout_quality.get('reason')} mode={pending.get('entry_mode')}"
                            )
                            pending_entries.pop(lifecycle_id, None)
                            continue
                        _entry_mode_force_live = _entry_mode_quality_high_quality_tiny_override(
                            pending,
                            lifecycle=_entry_timing_lifecycle,
                            entry_mode=pending.get('entry_mode') or pending.get('scout_mode'),
                            route=_pending_signal_route or pending.get('signal_type'),
                        )
                        if _entry_mode_force_live.get('pass'):
                            pending['entry_mode_quality_force_live'] = _entry_mode_force_live
                            log.info(
                                f"  [ENTRY_MODE_QUALITY] force-live {pending['symbol']}: "
                                f"mode={pending.get('entry_mode')} reason={_entry_mode_force_live.get('reason')} "
                                f"scores={_entry_mode_force_live.get('scores')}"
                            )
                        _entry_mode_live_allowed, _entry_mode_quality = _entry_mode_quality_allows_live(
                            db,
                            entry_mode=pending.get('entry_mode') or pending.get('scout_mode'),
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            route=_pending_signal_route or pending.get('signal_type'),
                            event_ts=now,
                            data_source='pending_entry+paper_trades',
                            force_live=bool(_entry_mode_force_live.get('pass')),
                        )
                        if _entry_mode_force_live.get('pass'):
                            _entry_mode_quality['force_live_detail'] = _entry_mode_force_live
                        pending['entry_mode_quality'] = _entry_mode_quality
                        if not _entry_mode_live_allowed:
                            _shadow_mode = (
                                _discovery_mode_for_lotto_reason(pending.get('source_reject_reason'))
                                if pending.get('is_lotto')
                                else _discovery_mode_for_ath_reason(pending.get('source_reject_reason'))
                            ) or pending.get('entry_mode') or pending.get('scout_mode')
                            track_discovery_candidate(
                                db,
                                discovery_candidates,
                                mode=_shadow_mode,
                                route=_pending_signal_route or pending.get('signal_type'),
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                pool=pending.get('pool'),
                                watchlist_id=pending.get('watchlist_id'),
                                watchlist_entry=pending_w_entry,
                                source_component=pending.get('source_component') or 'entry_mode_quality',
                                source_reject_reason='entry_mode_quality_shadow',
                                source_detail={
                                    'entry_mode': pending.get('entry_mode'),
                                    'entry_mode_quality': _entry_mode_quality,
                                },
                                lifecycle=_entry_timing_lifecycle,
                                now_ts=now,
                            )
                            record_decision_event(
                                db,
                                component='entry_mode_quality',
                                event_type='entry_block',
                                decision='shadow',
                                reason='entry_mode_quality_shadow',
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                strategy_stage=_pending_strategy_stage,
                                route=_pending_signal_route or pending.get('signal_type'),
                                data_source='pending_entry+paper_trades',
                                payload=with_lifecycle_payload({
                                    'entry_mode_quality': _entry_mode_quality,
                                }, _entry_timing_lifecycle),
                            )
                            pending_entries.pop(lifecycle_id, None)
                            continue

                    if pending.get('is_lotto'):
                        _lotto_timing_blocked, _lotto_timing_reason, _lotto_timing_detail = should_block_lotto_lifecycle_entry(
                            _entry_timing_lifecycle,
                        )
                        if _lotto_timing_blocked:
                            if pending_is_paper_tiny_scout(pending) and _lotto_timing_reason == 'lotto_timing_negative_m5':
                                _lotto_discovery_mode = (
                                    _lotto_recovery_mode_for_blocker(
                                        primary_reason=pending.get('source_reject_reason'),
                                        secondary_reason=_lotto_timing_reason,
                                        current_mode=pending.get('entry_mode') or pending.get('scout_mode'),
                                    )
                                    or pending.get('entry_mode')
                                    or pending.get('scout_mode')
                                )
                                track_discovery_candidate(
                                    db,
                                    discovery_candidates,
                                    mode=_lotto_discovery_mode,
                                    route=_pending_signal_route or pending.get('signal_type') or 'LOTTO',
                                    token_ca=pending['token_ca'],
                                    symbol=pending['symbol'],
                                    lifecycle_id=lifecycle_id,
                                    signal_ts=pending['signal_ts'],
                                    signal_id=pending.get('premium_signal_id'),
                                    pool=pending.get('pool'),
                                    watchlist_id=pending.get('watchlist_id'),
                                    watchlist_entry=pending_w_entry,
                                    source_component=pending.get('source_component') or 'lotto_timing_gate',
                                    source_reject_reason=_lotto_timing_reason,
                                    source_detail={
                                        'original_source_reject_reason': pending.get('source_reject_reason'),
                                        'lotto_timing_reason': _lotto_timing_reason,
                                        'lotto_timing_detail': _lotto_timing_detail,
                                        'entry_mode': pending.get('entry_mode'),
                                    },
                                    lifecycle=_entry_timing_lifecycle,
                                    now_ts=now,
                                )
                            record_decision_event(
                                db,
                                component='lotto_timing_gate',
                                event_type='entry_block',
                                decision='block',
                                reason=_lotto_timing_reason,
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                strategy_stage=_pending_strategy_stage,
                                route=_pending_signal_route or pending.get('signal_type'),
                                data_source='lifecycle+dexscreener',
                                payload=with_lifecycle_payload(_lotto_timing_detail, _entry_timing_lifecycle),
                            )
                            log.info(
                                f"  [LOTTO_TIMING] 🚫 {pending['symbol']} BLOCKED: "
                                f"{_lotto_timing_reason} detail={_lotto_timing_detail}"
                            )
                            pending_entries.pop(lifecycle_id, None)
                            continue

                    if (
                        LOTTO_PULLBACK_SIZE_PROTECT_ENABLED
                        and pending.get('is_lotto')
                        and not pending_is_paper_tiny_scout(pending)
                        and (pending.get('entry_mode') == 'smart_entry_pullback_bounce')
                    ):
                        _pullback_strength = _lotto_pullback_has_strong_activity(_entry_timing_lifecycle)
                        pending['lotto_pullback_size_protect'] = _pullback_strength
                        if not _pullback_strength.get('pass'):
                            _old_size = float(pending.get('kelly_position_sol') or LOTTO_POSITION_SIZE_SOL)
                            pending['kelly_position_sol'] = min(_old_size, PAPER_TINY_SCOUT_SIZE_SOL)
                            pending['paper_only_scout'] = True
                            pending['size_protected_scout'] = True
                            if isinstance(pending.get('lotto_state'), dict):
                                pending['lotto_state']['sizeProtectedScout'] = True
                                pending['lotto_state']['sizeProtectReason'] = 'lotto_pullback_activity_not_strong'
                            record_decision_event(
                                db,
                                component='entry_sizing',
                                event_type='entry_size',
                                decision='cap',
                                reason='lotto_pullback_activity_not_strong',
                                token_ca=pending['token_ca'],
                                symbol=pending['symbol'],
                                lifecycle_id=lifecycle_id,
                                signal_ts=pending['signal_ts'],
                                signal_id=pending.get('premium_signal_id'),
                                strategy_stage=_pending_strategy_stage,
                                route=_pending_signal_route or pending.get('signal_type'),
                                data_source='lifecycle+dexscreener',
                                payload=with_lifecycle_payload({
                                    'entry_mode': pending.get('entry_mode'),
                                    'old_size_sol': _old_size,
                                    'new_size_sol': pending['kelly_position_sol'],
                                    'strength': _pullback_strength,
                                }, _entry_timing_lifecycle),
                            )
                            log.info(
                                f"  [ENTRY_SIZE] {pending['symbol']} LOTTO pullback size protected "
                                f"{_old_size:.3f} -> {pending['kelly_position_sol']:.3f} SOL "
                                f"strength={_pullback_strength.get('observed')}"
                            )

                    # Paper tiny scouts must stay at the probe budget even when they come
                    # from a LOTTO pending entry with a larger fixed-size default.
                    if pending_is_paper_tiny_scout(pending):
                        _scout_size_detail = apply_paper_tiny_scout_size_cap(pending)
                        actual_position_size_sol = float(pending.get('kelly_position_sol') or PAPER_TINY_SCOUT_SIZE_SOL)
                        log.info(
                            f"  [ENTRY_SIZE] {pending['symbol']} paper tiny scout "
                            f"mode={pending.get('entry_mode')} size={actual_position_size_sol:.3f} SOL "
                            f"cap={_scout_size_detail.get('cap_sol'):.3f}"
                        )
                    # LOTTO: fixed paper size, skip Kelly and liquidity cap
                    elif pending.get('is_lotto'):
                        actual_position_size_sol = float(pending.get('kelly_position_sol') or LOTTO_POSITION_SIZE_SOL)
                        log.info(f"  [LOTTO] {pending['symbol']} fixed size: {actual_position_size_sol} SOL")
                    else:
                        # Recalculate Kelly with entry mode + matrix scores
                        pending['kelly_position_sol'] = calculate_kelly_position(
                            pending_w_entry, entry_mode=pending.get('entry_mode', 'default'),
                            matrix_scores=pending.get('matrix_scores'))

                        # Layer 1: Kelly formula output (Sustained ATH 1.5x boost is applied inside calculate_kelly_position)
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
                    actual_position_size_sol, _primary_cap_detail = _apply_primary_proving_cap(
                        pending,
                        actual_position_size_sol,
                    )
                    if _primary_cap_detail and _primary_cap_detail.get('capped'):
                        record_decision_event(
                            db,
                            component='entry_sizing',
                            event_type='entry_size',
                            decision='cap',
                            reason=_primary_cap_detail.get('reason') or 'primary_proving_cap',
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            strategy_stage=_pending_strategy_stage,
                            route=_pending_signal_route or pending.get('signal_type'),
                            data_source='entry_mode_quality+paper_research',
                            payload=_primary_cap_detail,
                        )
                        log.info(
                            f"  [ENTRY_SIZE] {pending['symbol']} proving cap "
                            f"{_primary_cap_detail.get('old_size_sol'):.3f} -> "
                            f"{_primary_cap_detail.get('new_size_sol'):.3f} SOL "
                            f"mode={pending.get('entry_mode')}"
                        )
                    execution = simulate_entry_execution(
                        pending['token_ca'],
                        actual_position_size_sol,
                        _pending_strategy_stage,
                        strategy_id=_pending_strategy_id,
                        lifecycle_id=lifecycle_id,
                    )
                    if not execution.get('success'):
                        failure_reason = execution.get('failureReason') or 'entry_quote_failed'
                        record_decision_event(
                            db,
                            component='execution_api',
                            event_type='entry_quote',
                            decision='fail',
                            reason=failure_reason,
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            strategy_stage=_pending_strategy_stage,
                            route=_pending_signal_route or pending.get('signal_type'),
                            data_source='jupiter_quote',
                            payload=execution,
                        )
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
                        record_decision_event(
                            db,
                            component='execution_api',
                            event_type='entry_quote',
                            decision='fail',
                            reason='invalid_entry_quote_payload',
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            strategy_stage=_pending_strategy_stage,
                            route=_pending_signal_route or pending.get('signal_type'),
                            data_source='jupiter_quote',
                            payload=execution,
                        )
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

                    _entry_edge_budget = evaluate_entry_edge_budget(
                        route=_pending_signal_route or pending.get('signal_type'),
                        trigger_price=trigger_price_val,
                        quote_price=price,
                        lifecycle=_entry_timing_lifecycle,
                        pending=pending,
                        token_risk=_token_risk,
                    )
                    _SPREAD_WARN_PCT = _entry_edge_budget.get('warn_spread_pct', MATRIX_SPREAD_WARN_PCT)
                    _SPREAD_GUARD_MAX_PCT = _entry_edge_budget.get('max_spread_pct', MATRIX_SPREAD_ABORT_PCT)
                    # POST-SPREAD-ABORT GUARD (persistent CA memory + live watchlist memory)
                    # A spread abort proves the current move has no usable entry edge. Require
                    # fresh reclaim before allowing another FIRE cycle for the same CA.
                    _live_abort_count = pending_w_entry.get('_spread_abort_count', 0) if pending_w_entry else 0
                    _persistent_spread_memory = evaluate_spread_abort_memory(
                        db,
                        pending.get('token_ca'),
                        lifecycle=_entry_timing_lifecycle,
                        current_spread_pct=_spread,
                        max_spread_pct=_SPREAD_GUARD_MAX_PCT,
                        now_ts=now,
                    )
                    _total_abort_count = max(
                        int(_live_abort_count or 0),
                        int(_persistent_spread_memory.get('abort_count') or 0),
                    )
                    _tiny_scout_memory_bypass = (
                        pending_is_paper_tiny_scout(pending)
                        and bool(_entry_edge_budget.get('pass', True))
                        and _spread is not None
                        and _spread <= _SPREAD_GUARD_MAX_PCT
                    )
                    if (
                        _total_abort_count >= 1
                        and _persistent_spread_memory.get('blocked', True)
                        and not _tiny_scout_memory_bypass
                    ):
                        log.info(
                            f"  [POST_SPREAD_ABORT] 🚫 {pending['symbol']} BLOCKED: "
                            f"{_total_abort_count} spread abort(s), no fresh reclaim. "
                            f"Refusing entry."
                        )
                        record_decision_event(
                            db,
                            component='execution_guard',
                            event_type='entry_abort',
                            decision='abort',
                            reason='post_spread_abort_memory',
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            strategy_stage=_pending_strategy_stage,
                            route=_pending_signal_route or pending.get('signal_type'),
                            payload={
                                'spread_abort_count': _total_abort_count,
                                'live_spread_abort_count': _live_abort_count,
                                'spread_abort_memory': _persistent_spread_memory,
                                'entry_edge_budget': _entry_edge_budget,
                            },
                        )
                        pending_entries.pop(lifecycle_id, None)
                        continue
                    if _tiny_scout_memory_bypass:
                        log.info(
                            f"  [POST_SPREAD_ABORT] ⚠️ {pending['symbol']} tiny scout bypass: "
                            f"live spread {_spread:+.1f}% within {_SPREAD_GUARD_MAX_PCT}% cap"
                        )
                        record_decision_event(
                            db,
                            component='execution_guard',
                            event_type='entry_arm',
                            decision='warn',
                            reason='spread_abort_memory_tiny_scout_quote_repaired',
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            strategy_stage=_pending_strategy_stage,
                            route=_pending_signal_route or pending.get('signal_type'),
                            payload={
                                'spread_abort_count': _total_abort_count,
                                'live_spread_abort_count': _live_abort_count,
                                'spread_abort_memory': _persistent_spread_memory,
                                'entry_edge_budget': _entry_edge_budget,
                            },
                        )
                    if _spread > _SPREAD_WARN_PCT:
                        log.info(
                            f"  [SPREAD_GUARD] ⚠️ {pending['symbol']} WARN: "
                            f"fill spread {_spread:+.1f}% > {_SPREAD_WARN_PCT}% warn "
                            f"(budget={_entry_edge_budget.get('profile')} abort at {_SPREAD_GUARD_MAX_PCT}%)."
                        )
                        record_decision_event(
                            db,
                            component='execution_guard',
                            event_type='entry_spread_warning',
                            decision='warn',
                            reason='spread_warning',
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            strategy_stage=_pending_strategy_stage,
                            route=_pending_signal_route or pending.get('signal_type'),
                            payload={
                                'spread_pct': _spread,
                                'warn_spread_pct': _SPREAD_WARN_PCT,
                                'max_spread_pct': _SPREAD_GUARD_MAX_PCT,
                                'quote_price': price,
                                'trigger_price': trigger_price_val,
                                'entry_edge_budget': _entry_edge_budget,
                            },
                        )
                    if not _entry_edge_budget.get('pass', True):
                        log.info(
                            f"  [ENTRY_EDGE] 🚫 {pending['symbol']} ABORT: "
                            f"fill spread {_spread:+.1f}% > {_SPREAD_GUARD_MAX_PCT}% budget "
                            f"(fill={price:.12f} vs trigger={_trigger_str}). "
                            f"reason={_entry_edge_budget.get('reason')}"
                        )
                        record_decision_event(
                            db,
                            component='execution_guard',
                            event_type='entry_abort',
                            decision='abort',
                            reason=_entry_edge_budget.get('reason') or 'entry_edge_budget',
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            strategy_stage=_pending_strategy_stage,
                            route=_pending_signal_route or pending.get('signal_type'),
                            payload={
                                'spread_pct': _spread,
                                'max_spread_pct': _SPREAD_GUARD_MAX_PCT,
                                'quote_price': price,
                                'trigger_price': trigger_price_val,
                                'entry_edge_budget': _entry_edge_budget,
                            },
                        )
                        # Track budget aborts on watchlist entry so subsequent FIRE→
                        # SmartEntry cycles know the fill was past the usable edge.
                        if pending_w_entry:
                            _prev_aborts = pending_w_entry.get('_spread_abort_count', 0)
                            pending_w_entry['_spread_abort_count'] = _prev_aborts + 1
                            # Capture first-fire pc_m5 if not yet set (for trend decay detection)
                            if '_first_fire_pc_m5' not in pending_w_entry:
                                try:
                                    _ff_trend = fetch_dexscreener_trend_snapshot(pending['token_ca'])
                                    if _ff_trend:
                                        pending_w_entry['_first_fire_pc_m5'] = _ff_trend.get('price_change_m5', 0)
                                except Exception:
                                    pass
                            log.info(
                                f"  [ENTRY_EDGE] Abort #{pending_w_entry['_spread_abort_count']} for {pending['symbol']} "
                                f"(first_pc_m5={pending_w_entry.get('_first_fire_pc_m5', 'N/A')})")
                        if pending_is_paper_tiny_scout(pending):
                            try:
                                track_discovery_candidate(
                                    db,
                                    discovery_candidates,
                                    mode=pending.get('entry_mode') or pending.get('scout_mode') or ATH_SOFT_RECLAIM_TINY_SCOUT_MODE,
                                    route=_pending_signal_route or pending.get('signal_type'),
                                    token_ca=pending.get('token_ca'),
                                    symbol=pending.get('symbol'),
                                    lifecycle_id=lifecycle_id,
                                    signal_ts=pending.get('signal_ts'),
                                    signal_id=pending.get('premium_signal_id'),
                                    pool=pending.get('pool') or pending.get('pool_address') or (pending_w_entry or {}).get('pool_address'),
                                    watchlist_id=(pending_w_entry or {}).get('id'),
                                    watchlist_entry=pending_w_entry,
                                    source_component='execution_guard',
                                    source_reject_reason='entry_edge_spread_too_high',
                                    source_detail={
                                        'spread_pct': _spread,
                                        'max_spread_pct': _SPREAD_GUARD_MAX_PCT,
                                        'quote_price': price,
                                        'trigger_price': trigger_price_val,
                                        'entry_edge_budget': _entry_edge_budget,
                                        'tracker': 'spread_normalization_tracker',
                                    },
                                    lifecycle=_entry_timing_lifecycle,
                                    now_ts=now,
                                )
                                record_decision_event(
                                    db,
                                    component='execution_guard',
                                    event_type='entry_defer',
                                    decision='track',
                                    reason='spread_normalization_tracker',
                                    token_ca=pending['token_ca'],
                                    symbol=pending['symbol'],
                                    lifecycle_id=lifecycle_id,
                                    signal_ts=pending['signal_ts'],
                                    signal_id=pending.get('premium_signal_id'),
                                    strategy_stage=_pending_strategy_stage,
                                    route=_pending_signal_route or pending.get('signal_type'),
                                    payload={
                                        'spread_pct': _spread,
                                        'max_spread_pct': _SPREAD_GUARD_MAX_PCT,
                                        'entry_edge_budget': _entry_edge_budget,
                                    },
                                )
                            except Exception as _spread_track_err:
                                log.debug(f"  [ENTRY_EDGE] spread normalization track failed for {pending['symbol']}: {_spread_track_err}")
                        record_decision_event(
                            db,
                            component='spread_reject_shadow_fill',
                            event_type='phantom_entry',
                            decision='shadow',
                            reason='entry_edge_spread_too_high',
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            strategy_stage=_pending_strategy_stage,
                            route=_pending_signal_route or pending.get('signal_type'),
                            data_source='jupiter_quote+entry_edge',
                            payload=with_lifecycle_payload({
                                'entry_mode': pending.get('entry_mode'),
                                'parent_scout_mode': pending.get('parent_scout_mode') or pending.get('scout_mode'),
                                'entry_trigger_mode': pending.get('entry_trigger_mode') or pending.get('timing_entry_mode'),
                                'phantom_entry_price': price,
                                'trigger_price': trigger_price_val,
                                'spread_pct': _spread,
                                'max_spread_pct': _SPREAD_GUARD_MAX_PCT,
                                'entry_edge_budget': _entry_edge_budget,
                                'quote_executable': bool(execution.get('success')),
                                'paper_pnl_impact': 'excluded_from_live_paper_pnl',
                            }, _entry_timing_lifecycle),
                        )
                        pending_entries.pop(lifecycle_id, None)
                        continue

                    _final_fire_block = _pending_watchlist_fire_block_detail(
                        watchlist,
                        pending,
                        now_ts=now,
                    )
                    if not _final_fire_block.get('pass'):
                        record_decision_event(
                            db,
                            component='entry_reentry_guard',
                            event_type='entry_block',
                            decision='block',
                            reason=_final_fire_block.get('reason') or 'watchlist_fire_block_active',
                            token_ca=pending['token_ca'],
                            symbol=pending['symbol'],
                            lifecycle_id=lifecycle_id,
                            signal_ts=pending['signal_ts'],
                            signal_id=pending.get('premium_signal_id'),
                            strategy_stage=_pending_strategy_stage,
                            route=_pending_signal_route or pending.get('signal_type'),
                            data_source='watchlist_fire_block',
                            payload=_final_fire_block,
                        )
                        log.info(
                            f"  [ENTRY_FINAL_GUARD] 🚫 {pending['symbol']} BLOCKED: "
                            f"{_final_fire_block.get('reason')} "
                            f"remaining={_final_fire_block.get('remaining_sec')}s"
                        )
                        pending_entries.pop(lifecycle_id, None)
                        continue

                    _entry_decision_contract = build_entry_decision_contract(
                        entry_readiness_policy=pending.get('entry_readiness_policy'),
                        entry_mode=pending.get('entry_mode') or timing_reason,
                        data_confidence=1.0,
                        p_follow=None,
                        spread_cost_pct=max(_spread or 0.0, 0.0),
                        exit_cost_buffer_pct=1.5,
                        timing_confirmed=True,
                    )
                    record_decision_event(
                        db,
                        component='entry_decision_contract',
                        event_type='entry_audit',
                        decision=_entry_decision_contract.decision,
                        reason=_entry_decision_contract.reason,
                        token_ca=pending['token_ca'],
                        symbol=pending['symbol'],
                        lifecycle_id=lifecycle_id,
                        signal_ts=pending['signal_ts'],
                        signal_id=pending.get('premium_signal_id'),
                        strategy_stage=_pending_strategy_stage,
                        route=_pending_signal_route or pending.get('signal_type'),
                        data_source='entry_readiness+smart_entry+execution_guard',
                        payload=_entry_decision_contract.to_dict(),
                    )

                    try:
                        _entry_dex_snapshot = fetch_dexscreener_trend_snapshot(pending['token_ca'])
                    except Exception:
                        _entry_dex_snapshot = None
                    _entry_lifecycle = lifecycle_payload_for(
                        watchlist_entry=pending_w_entry,
                        dex_snapshot=_entry_dex_snapshot,
                        route=_pending_signal_route or pending.get('signal_type'),
                        signal_ts=pending['signal_ts'],
                        signal_price=price,
                        quote_available=True,
                        mark_quote_gap=(_spread / 100.0) if _spread is not None else None,
                        now=now,
                    )
                    regime = determine_market_regime(sol_price) if sol_price else 'unknown'
                    _monitor_state = {
                        'tokenCA': pending['token_ca'],
                        'symbol': pending['symbol'],
                        'entryPrice': price,
                        'entryMode': pending.get('entry_mode') or timing_reason,
                        'entryTriggerPrice': trigger_price_val,
                        'entryQuotePrice': price,
                        'entryPriceUnit': PRICE_UNIT_SOL_PER_TOKEN,
                        'entryTriggerPriceUnit': PRICE_UNIT_SOL_PER_TOKEN,
                        'entryQuotePriceUnit': PRICE_UNIT_SOL_PER_TOKEN,
                        'pnlUnit': PNL_UNIT_RATIO_DECIMAL,
                        'accountingUnit': AMOUNT_UNIT_SOL,
                        'priceUnitContractVersion': PRICE_UNIT_CONTRACT_VERSION,
                        'entrySpreadPct': _spread,
                        'entryEdgeBudget': _entry_edge_budget,
                        'entryReadinessPolicy': pending.get('entry_readiness_policy'),
                        'entryDecisionContract': _entry_decision_contract.to_dict(),
                        'entrySol': actual_position_size_sol,
                        'tokenAmount': int(token_amount_raw),
                        'tokenDecimals': int(token_decimals or 0),
                        'entryTime': int(entry_ts) * 1000,
                        'exitStrategy': _pending_exit_strategy,
                        'lifecycleState': _entry_lifecycle.get('lifecycle_state'),
                        'vitalityScore': _entry_lifecycle.get('vitality_score'),
                        'entryBias': _entry_lifecycle.get('entry_bias'),
                    }
                    if _pending_signal_route:
                        _monitor_state['signalRoute'] = _pending_signal_route
                    if pending.get('ath_recovery_family'):
                        _monitor_state['athRecoveryFamily'] = pending.get('ath_recovery_family')
                        _monitor_state['parentBlockReason'] = pending.get('parent_block_reason')
                        _monitor_state['recoveryProbeReason'] = pending.get('recovery_probe_reason')
                    if pending.get('lotto_recovery_family'):
                        _monitor_state['lottoRecoveryFamily'] = pending.get('lotto_recovery_family')
                        _monitor_state['parentBlockReason'] = pending.get('parent_block_reason')
                        _monitor_state['recoveryProbeReason'] = pending.get('recovery_probe_reason')
                    if _pending_lotto_state:
                        _monitor_state['lottoState'] = _pending_lotto_state
                    _signal_ts_store = normalize_signal_ts_seconds(pending.get('signal_ts')) or pending.get('signal_ts')
                    _trigger_price_store = trigger_price_val if trigger_price_val else price
                    db.execute("""
                        INSERT INTO paper_trades
                            (strategy_id, strategy_role, strategy_stage, stage_outcome,
                             token_ca, symbol, signal_ts, entry_price, entry_ts,
                             market_regime, replay_source, peak_pnl, trailing_active,
                             lifecycle_id, stage_seq, trigger_ts, trigger_price,
                             position_size_sol, token_amount_raw, token_decimals,
                             entry_execution_json, entry_execution_audit_json, monitor_state_json,
                             premium_signal_id, signal_type, signal_route, entry_mode, lotto_state_json,
                             lifecycle_state, vitality_score, entry_bias, lifecycle_features_json,
                             strategy_outcome, execution_availability, accounting_outcome, synthetic_close)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """, (
                        _pending_strategy_id, strategy_role, _pending_strategy_stage, _pending_stage_outcome,
                        pending['token_ca'], pending['symbol'], _signal_ts_store, price, entry_ts,
                        regime, _pending_replay_source, lifecycle_id, stage_seq(_pending_strategy_stage), entry_ts, _trigger_price_store,
                        actual_position_size_sol, str(token_amount_raw), token_decimals or 0, json.dumps(execution), json.dumps(build_execution_audit(execution, {
                            'auditVersion': 1,
                            'stage': _pending_strategy_stage,
                            'lifecycleId': lifecycle_id,
                            'signalTs': _signal_ts_store,
                            'rawSignalTs': pending.get('signal_ts'),
                            'entryPriceSolPerToken': price,
                            'entryTriggerPriceSolPerToken': trigger_price_val,
                            'entryQuotePriceSolPerToken': price,
                            **price_unit_contract_payload(),
                            'entrySpreadPct': _spread,
                            'entryEdgeBudget': _entry_edge_budget,
                            'entryReadinessPolicy': pending.get('entry_readiness_policy'),
                            'entryDecisionContract': _entry_decision_contract.to_dict(),
                            'positionSizeSol': actual_position_size_sol,
                        })), json.dumps(_monitor_state),
                        pending.get('premium_signal_id'), pending.get('signal_type') or 'NEW_TRENDING',
                        _pending_signal_route, pending.get('entry_mode') or timing_reason,
                        json.dumps(_pending_lotto_state) if _pending_lotto_state else None,
                        _entry_lifecycle.get('lifecycle_state'), _entry_lifecycle.get('vitality_score'),
                        _entry_lifecycle.get('entry_bias'), json.dumps(_entry_lifecycle.get('lifecycle_features') or {}),
                        'entered', 'available', 'open'
                    ))
                    trade_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
                    db.commit()
                    record_decision_event(
                        db,
                        component='execution_api',
                        event_type='entry_quote',
                        decision='filled_paper',
                        reason='entry_quote_success',
                        token_ca=pending['token_ca'],
                        symbol=pending['symbol'],
                        lifecycle_id=lifecycle_id,
                        trade_id=trade_id,
                        signal_ts=pending['signal_ts'],
                        signal_id=pending.get('premium_signal_id'),
                        strategy_stage=_pending_strategy_stage,
                        route=_pending_signal_route or pending.get('signal_type'),
                        data_source='jupiter_quote',
                        payload=with_lifecycle_payload({
                            'entry_price': price,
                            'entry_price_unit': PRICE_UNIT_SOL_PER_TOKEN,
                            'trigger_price': trigger_price_val,
                            'trigger_price_unit': PRICE_UNIT_SOL_PER_TOKEN,
                            'spread_pct': _spread,
                            'position_size_sol': actual_position_size_sol,
                            'accounting_unit': AMOUNT_UNIT_SOL,
                            'pnl_unit': PNL_UNIT_RATIO_DECIMAL,
                            'entry_edge_budget': _entry_edge_budget,
                            'entry_readiness_policy': pending.get('entry_readiness_policy'),
                            'entry_decision_contract': _entry_decision_contract.to_dict(),
                            'execution': build_execution_audit(execution),
                        }, _entry_lifecycle),
                    )

                    # Pop immediately after commit — prevents ghost duplicate on next loop
                    pending_entries.pop(lifecycle_id, None)

                    # Tighter trailing stop for SUSTAINED_ATH to lock in profits
                    _custom_stage1_exit = dict(stage1_exit)
                    if pending_is_paper_tiny_scout(pending):
                        _probe_sl_pct = 35.0 if _pending_signal_route == 'LOTTO' or pending.get('is_lotto') else 30.0
                        _custom_stage1_exit['stopLossPct'] = max(float(_custom_stage1_exit.get('stopLossPct') or 0.0), _probe_sl_pct)
                        _custom_stage1_exit['trailStartPct'] = max(float(_custom_stage1_exit.get('trailStartPct') or 0.0), 100.0)
                        _custom_stage1_exit['timeoutMinutes'] = max(int(_custom_stage1_exit.get('timeoutMinutes') or 0), 240)
                        log.info(
                            f"  [OBS_PROBE] {pending['symbol']} observation exit room: "
                            f"SL={_custom_stage1_exit['stopLossPct']:.0f}% "
                            f"trail_start={_custom_stage1_exit['trailStartPct']:.0f}%"
                        )
                    if pending_w_entry and pending_w_entry.get('is_sustained_ath') and not pending_is_paper_tiny_scout(pending):
                        _custom_stage1_exit['trailStartPct'] = 10.0
                        _custom_stage1_exit['trailFactor'] = 0.5
                        log.info(f"  [SUSTAINED_ATH] {pending['symbol']} tight trail lock (10% start, 0.5x factor)")

                    pos = Position(trade_id, pending['token_ca'], pending['symbol'], _signal_ts_store, price, entry_ts, pending['pool'],
                                   _pending_strategy_stage, lifecycle_id, _custom_stage1_exit, actual_position_size_sol, token_amount_raw, token_decimals or 0,
                                   monitor_state=_monitor_state)
                    pos.premium_signal_id = pending.get('premium_signal_id')
                    pos.signal_type = pending.get('signal_type') or 'NEW_TRENDING'
                    with positions_lock:
                        positions[pos.trade_id] = pos

                    # Use setdefault to avoid KeyError if lifecycle not yet initialized
                    lc = lifecycles.setdefault(lifecycle_id,
                        build_lifecycle_state(lifecycle_id, pending['token_ca'],
                            pending['symbol'], _signal_ts_store,
                            pending.get('premium_signal_id'),
                            pending.get('signal_type')))
                    lc['stage1_trade_id'] = trade_id

                    log.info(
                        f"  Entered {pending['symbol']}/{_pending_strategy_stage} @ {price:.10f} "
                        f"(quote_sol={quote_price_sol:.12f}, decimals={token_decimals or 0}) "
                        f"mode={pending.get('entry_mode', 'default')} lifecycle={lifecycle_id} via quoted execution"
                    )
                    if 'watchlist_id' in pending:
                        if pending.get('is_lotto'):
                            # LOTTO has its own time-validation exit and -18% normal stop.
                            _base_sl = -0.18
                        else:
                            _base_sl = get_adaptive_stop_loss()  # returns -0.15
                        if pending_is_paper_tiny_scout(pending):
                            _base_sl = -0.35 if pending.get('is_lotto') else -0.30
                        _final_sl = _base_sl
                        
                        if _spread > 0:
                            log.info(
                                f"  [SL_ADJUST] {pending['symbol']} BaseSL={_base_sl*100:.1f}%. "
                                f"(Slippage was +{_spread:.1f}%, but we no longer widen SL for slippage)"
                            )
                            
                        # K-LINE STRUCTURAL STOP LOSS (Phase 5)
                        # IMPORTANT: use native_only=True because entry price is SOL-native,
                        # but GeckoTerminal/kline_db returns USD-denominated prices.
                        from entry_engine import get_recent_synthetic_bars
                        _entry_bars = get_recent_synthetic_bars(pending['token_ca'], n_bars=3, pool_address=pending['pool'], native_only=True)
                        if _entry_bars:
                            _structure_low = min(b['low'] for b in _entry_bars)
                            # Sanity: structure_low MUST be below entry price (it's a support level)
                            # If it's above, the K-line data is bad (wrong pool, USD vs SOL, stale cache)
                            if _structure_low >= price:
                                log.warning(
                                    f"  [KLINE_SL] {pending['symbol']} SKIP: structure_low={_structure_low:.10f} "
                                    f">= price={price:.10f} (bad K-line data, using fixed SL={_final_sl*100:.1f}%)"
                                )
                            elif _structure_low <= price * 0.01:
                                # structure_low is less than 1% of price — also garbage data
                                log.warning(
                                    f"  [KLINE_SL] {pending['symbol']} SKIP: structure_low={_structure_low:.10f} "
                                    f"<< price={price:.10f} (stale/bad data, using fixed SL={_final_sl*100:.1f}%)"
                                )
                            else:
                                _structure_sl_pct = ((price - _structure_low) / price) * -100
                                # Clamp: at least -3%, at most -15% (never wider than current fixed SL)
                                _structure_sl = max(-15.0, min(-3.0, _structure_sl_pct))

                                # HIGH-SCORE WIDENING: if Matrix scores sum ≥ 400
                                # (avg 80/dimension), give 5pp extra room.
                                # Data: OG (sum=420, Score=105) got KLINE_SL=-11.3% → shaken out,
                                # then rallied 3x. With -16.3% SL, would have survived.
                                _matrix_sum = sum(v for v in (pending.get('matrix_scores') or {}).values() if v is not None)
                                if _matrix_sum >= 400:
                                    _structure_sl_widened = _structure_sl - 5.0  # widen by 5pp
                                    _structure_sl_widened = max(-20.0, _structure_sl_widened)  # never wider than -20%
                                    log.info(
                                        f"  [KLINE_SL] {pending['symbol']} HIGH_SCORE widen: "
                                        f"matrix_sum={_matrix_sum} → SL {_structure_sl:.1f}% → {_structure_sl_widened:.1f}%"
                                    )
                                    _structure_sl = _structure_sl_widened

                                log.info(
                                    f"  [KLINE_SL] {pending['symbol']} structure_low={_structure_low:.10f} "
                                    f"→ SL={_structure_sl:.1f}% (vs fixed={_final_sl*100:.1f}%)"
                                )
                                # Primary positions keep the legacy wider SL to survive meme
                                # volatility. Tiny probes are different: they are data-gathering
                                # scouts, so use the tighter structural SL and fail fast.
                                _selected_sl, _sl_reason = _select_structure_stop_loss(
                                    _final_sl,
                                    _structure_sl / 100.0,
                                    pending,
                                )
                                if _selected_sl != _final_sl:
                                    log.info(
                                        f"  [KLINE_SL] {pending['symbol']} selected "
                                        f"{_selected_sl*100:.1f}% reason={_sl_reason}"
                                    )
                                _final_sl = _selected_sl
                        
                        watchlist.mark_holding(
                            pending['watchlist_id'], 
                            price, 
                            actual_position_size_sol, 
                            token_amount_raw, 
                            token_decimals or 0, 
                            trade_id,
                            initial_sl=_final_sl,  # Private slip-adjusted SL
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
                        _dex_snap = None

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
                            # Fix: write pool liquidity to w_entry for Thin Pool factor
                            # Data chain was broken: ExitGuardian reads _dex_liquidity_usd but
                            # nobody was writing it. 48% of pools are $10-20k (small).
                            try:
                                _dex_snap = fetch_dexscreener_trend_snapshot(pos.token_ca)
                                if _dex_snap:
                                    w_entry['_dex_liquidity_usd'] = _dex_snap.get('liquidity_usd', 0) or 0
                            except Exception:
                                pass
                            # Relay Guardian's full threat score (includes FLAT-TOP)
                            # to EXIT_MATRIX so both engines use identical trail tightening
                            if hasattr(pos, '_guardian_threat_tighten'):
                                w_entry['_guardian_threat_tighten'] = pos._guardian_threat_tighten

                        if w_entry and is_lotto_position(pos, w_entry):
                            exit_matrix = evaluate_lotto_exit(pos, w_entry, pre_price, now=time.time())
                        elif not w_entry:
                            exit_matrix = {'action': 'hold', 'reason': 'no_watchlist_entry'}
                        elif w_entry['status'] == 'moon_bag':
                            exit_matrix = exit_matrix_evaluator.evaluate_moon_bag(w_entry, pre_price)
                        else:
                            exit_matrix = exit_matrix_evaluator.evaluate_exit(w_entry, pre_price)

                        if w_entry and not is_lotto_position(pos, w_entry) and w_entry.get('status') != 'moon_bag':
                            _pre_doa_peak = exit_matrix.get('peak_pnl')
                            if _pre_doa_peak is None and w_entry.get('peak_pnl') is not None:
                                exit_matrix = {**exit_matrix, 'peak_pnl': w_entry.get('peak_pnl')}
                            exit_matrix = apply_matrix_doa_fast_exit(pos, exit_matrix)
                            exit_matrix = apply_ath_recovery_no_follow_exit(pos, exit_matrix, now_ts=time.time())

                        _pre_capture_action = exit_matrix.get('action')
                        _pre_capture_reason = exit_matrix.get('reason')
                        exit_matrix = apply_probe_profit_capture(pos, w_entry, exit_matrix, now_ts=time.time())
                        if exit_matrix.get('probe_profit_capture') and (
                            exit_matrix.get('action') != _pre_capture_action
                            or exit_matrix.get('reason') != _pre_capture_reason
                        ):
                            record_decision_event(
                                db,
                                component='probe_profit_capture',
                                event_type='exit_override',
                                decision=exit_matrix.get('action', 'unknown'),
                                reason=exit_matrix.get('reason'),
                                token_ca=pos.token_ca,
                                symbol=pos.symbol,
                                lifecycle_id=pos.lifecycle_id,
                                trade_id=pos.trade_id,
                                signal_ts=pos.signal_ts,
                                signal_id=getattr(pos, 'premium_signal_id', None),
                                strategy_stage=pos.strategy_stage,
                                route=(w_entry or {}).get('signal_route') or (pos.monitor_state or {}).get('signalRoute') or getattr(pos, 'signal_type', None),
                                data_source=pre_src,
                                payload=exit_matrix.get('probe_profit_capture'),
                            )
                        
                        # Log every exit evaluation
                        held_min = int((time.time() - pos.entry_ts) / 60)
                        pnl_pct = exit_matrix.get('current_pnl', 0) * 100
                        log.info(
                            f"  [EXIT_MATRIX] {pos.symbol}/{pos.strategy_stage} "
                            f"action={exit_matrix['action']} pnl={pnl_pct:+.1f}% "
                            f"held={held_min}min reason={exit_matrix.get('reason', '-')} "
                            f"price={pre_price:.10f} trail={exit_matrix.get('trail_floor', '-')}"
                        )
                        record_decision_event(
                            db,
                            component='exit_strategy',
                            event_type='exit_decision',
                            decision=exit_matrix.get('action', 'unknown'),
                            reason=exit_matrix.get('reason'),
                            token_ca=pos.token_ca,
                            symbol=pos.symbol,
                            lifecycle_id=pos.lifecycle_id,
                            trade_id=pos.trade_id,
                            signal_ts=pos.signal_ts,
                            signal_id=getattr(pos, 'premium_signal_id', None),
                            strategy_stage=pos.strategy_stage,
                            route=(w_entry or {}).get('signal_route') or (pos.monitor_state or {}).get('signalRoute') or getattr(pos, 'signal_type', None),
                            data_source=pre_src,
                            payload={
                                'current_price': pre_price,
                                'current_pnl': exit_matrix.get('current_pnl'),
                                'peak_pnl': exit_matrix.get('peak_pnl', pos.peak_pnl),
                                'trail_floor': exit_matrix.get('trail_floor'),
                                'held_min': held_min,
                                'price_age_ms': pre_age_ms,
                            },
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
                            new_peak = max(
                                w_entry.get('peak_pnl', 0) or 0,
                                exit_matrix.get('peak_pnl') or 0,
                                exit_matrix['current_pnl'],
                            )
                            state_updates['peak_pnl'] = new_peak
                        elif exit_matrix.get('current_pnl') is not None:
                            new_peak = max(w_entry.get('peak_pnl', 0) or 0, exit_matrix['current_pnl'])
                            state_updates['peak_pnl'] = new_peak
                        if state_updates.get('peak_pnl') is not None and state_updates['peak_pnl'] > pos.peak_pnl:
                            pos.peak_pnl = state_updates['peak_pnl']
                            pos.peak_ts = int(time.time())
                        
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
                            sell_pct = float(exit_matrix.get('sell_pct', 0.5)) if exit_matrix['action'] == 'lock_profit' else 1.0
                            sell_pct = min(1.0, max(0.0, sell_pct))
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
                                quote_pnl = quote_pnl_from_execution(simulate_res, pos.entry_price)
                                trigger_pnl = exit_matrix.get('current_pnl', 0.0)
                                quote_mark_gap = (
                                    quote_pnl - trigger_pnl
                                    if quote_pnl is not None and trigger_pnl is not None else None
                                )
                                quote_sanity_status = 'quote_unavailable' if quote_pnl is None else 'quote_checked'
                                if quote_pnl is not None and trigger_pnl is not None:
                                    divergence = abs(quote_pnl - trigger_pnl)
                                    if divergence > 0.05:
                                        log.info(
                                            f"  [QUOTE_SANITY] {pos.symbol} mark/quote gap: "
                                            f"action={exit_matrix['action']} mark={trigger_pnl:+.1%} "
                                            f"quote={quote_pnl:+.1%} gap={quote_mark_gap:+.1%}"
                                        )

                                if exit_matrix['action'] == 'lock_profit':
                                    if quote_pnl is None:
                                        quote_sanity_status = 'partial_lock_quote_missing'
                                        log.warning(
                                            f"  [QUOTE_SANITY] {pos.symbol} partial lock skipped — "
                                            f"missing quote PnL for mark={trigger_pnl:+.1%}"
                                        )
                                        sanity_override = True
                                    elif quote_pnl < 0:
                                        quote_sanity_status = 'partial_lock_quote_negative'
                                        log.warning(
                                            f"  [QUOTE_SANITY] {pos.symbol} partial lock skipped — "
                                            f"mark={trigger_pnl:+.1%} but quote={quote_pnl:+.1%}; "
                                            f"not treating mark profit as lockable."
                                        )
                                        sanity_override = True

                                if exit_matrix['action'] == 'exit':
                                    if quote_pnl is not None and trigger_pnl is not None:
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
                                                elif (
                                                    TINY_EXIT_QUOTE_SANITY_ENABLED
                                                    and position_is_probe_profit_capture_candidate(pos)
                                                    and TINY_EXIT_QUOTE_SANITY_MIN_PEAK <= float(getattr(pos, 'peak_pnl', 0.0) or 0.0) <= TINY_EXIT_QUOTE_SANITY_MAX_PEAK
                                                    and quote_pnl < 0
                                                    and (trigger_pnl - quote_pnl) >= TINY_EXIT_QUOTE_SANITY_NEG_GAP
                                                ):
                                                    cancel = True
                                            elif (
                                                TINY_EXIT_QUOTE_SANITY_ENABLED
                                                and position_is_probe_profit_capture_candidate(pos)
                                                and TINY_EXIT_QUOTE_SANITY_MIN_PEAK <= float(getattr(pos, 'peak_pnl', 0.0) or 0.0) <= TINY_EXIT_QUOTE_SANITY_MAX_PEAK
                                                and quote_pnl < 0
                                                and (trigger_pnl - quote_pnl) >= TINY_EXIT_QUOTE_SANITY_NEG_GAP
                                                and (
                                                    'profit_protect' in reason
                                                    or 'trail' in reason
                                                    or 'crash_brake' in reason
                                                )
                                            ):
                                                cancel = True

                                            if cancel:
                                                log.warning(
                                                    f"  [PRICE_SANITY] {pos.symbol} EXIT CANCELLED — "
                                                    f"trigger_pnl={trigger_pnl:+.1%} but quote_pnl={quote_pnl:+.1%} "
                                                    f"(divergence={divergence:.1%}, src={pre_src}). "
                                                    f"Trigger/quote disagreement too large, holding position."
                                                )
                                                quote_sanity_status = (
                                                    'exit_cancelled_tiny_quote_gap'
                                                    if quote_pnl < trigger_pnl
                                                    else 'exit_cancelled_quote_better'
                                                )
                                                sanity_override = True
                                            else:
                                                log.info(
                                                    f"  [PRICE_SANITY] {pos.symbol} price divergence noted: "
                                                    f"trigger={trigger_pnl:+.1%} quote={quote_pnl:+.1%} "
                                                    f"(gap={divergence:.1%}) — exit confirmed by quote"
                                                )
                                                quote_sanity_status = 'exit_confirmed_quote_diverged'

                                if not sanity_override:
                                    exit_eval['shouldExit'] = True
                                    exit_eval['action'] = 'partial_sell' if exit_matrix['action'] == 'lock_profit' else 'exit'
                                    exit_eval['exitReason'] = exit_matrix.get('reason', 'matrix_exit')
                                    exit_eval['execution'] = simulate_res
                                    exit_eval['sellPct'] = sell_pct if exit_matrix['action'] == 'lock_profit' else 1.0
                                    exit_eval['tpName'] = 'MOON_LOCK' if exit_matrix['action'] == 'lock_profit' else None
                                    exit_eval['quotePnl'] = _safe_float(quote_pnl, None)
                                    exit_eval['quoteMarkGap'] = _safe_float(quote_mark_gap, None)
                                    exit_eval['quoteSanityStatus'] = quote_sanity_status
                                    
                                    if exit_matrix['action'] == 'lock_profit':
                                        log.info(f"  [EXIT_MATRIX] 🌙 {pos.symbol} LOCK PROFIT! Selling {sell_pct*100:.0f}%, rest → Moon Bag")
                                    else:
                                        log.info(f"  [EXIT_MATRIX] 🔔 {pos.symbol} EXIT triggered: {exit_matrix.get('reason')}")
                                    
                                    if exit_matrix['action'] == 'lock_profit' and w_entry:
                                        watchlist.mark_moon_bag(w_entry['id'], exit_matrix.get('current_pnl', 0.0))
                                        rem_amount = int(float(pos.token_amount_raw) - sell_amount_raw)
                                        exit_eval['updatedState'] = (pos.monitor_state or {}).copy()
                                        exit_eval['updatedState']['tokenAmount'] = rem_amount
                                        prev_sold_pct = _safe_float(exit_eval['updatedState'].get('soldPct'), 0.0)
                                        exit_eval['prevSoldPct'] = prev_sold_pct
                                        exit_eval['updatedState']['soldPct'] = min(1.0, prev_sold_pct + sell_pct)
                                        exit_eval['updatedState']['lockedPnl'] = exit_matrix.get('current_pnl', 0.0)
                                        exit_eval['updatedState']['moonbag'] = True
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
                    mark_quote_out = mark_execution.get('quotedOutAmount', exit_eval.get('quotedOutAmount'))
                    price = exit_eval.get('currentPrice')
                    bar_ts = int(exit_eval.get('quoteTsSec') or 0)
                    mark_source = exit_eval.get('markSource') or 'fallback'
                    if price is None or price <= 0 or not bar_ts:
                        continue
                    action = exit_eval.get('action') or 'hold'
                    should_exit = bool(exit_eval.get('shouldExit'))
                    prev_monitor_state_for_eval = dict(pos.monitor_state or {})
                    if action == 'partial_sell' and exit_eval.get('prevSoldPct') is None:
                        exit_eval['prevSoldPct'] = _safe_float(prev_monitor_state_for_eval.get('soldPct'), 0.0)
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
                    phase_policy_payload = None
                    try:
                        _phase_bars = get_recent_synthetic_bars(
                            pos.token_ca,
                            n_bars=25,
                            pool_address=pos.pool_address or pos_pool,
                            native_only=True,
                        )
                        if _dex_snap is None:
                            try:
                                _dex_snap = fetch_dexscreener_trend_snapshot(pos.token_ca)
                            except Exception:
                                _dex_snap = None
                        _phase_decision = evaluate_phase_policy(
                            route=(w_entry or {}).get('signal_route') or (pos.monitor_state or {}).get('signalRoute') or getattr(pos, 'signal_type', None),
                            current_pnl=trigger_pnl,
                            peak_pnl=pos.peak_pnl,
                            held_sec=max(0.0, time.time() - float(pos.entry_ts or time.time())),
                            sold_pct=_safe_float((pos.monitor_state or {}).get('soldPct'), 0.0),
                            dex_snapshot=_dex_snap,
                            kline_bars=_phase_bars,
                            current_price=price,
                            quote_pnl=exit_eval.get('quotePnl'),
                            lifecycle_state=(pos.monitor_state or {}).get('lifecycleState'),
                            vitality_score=(pos.monitor_state or {}).get('vitalityScore'),
                        )
                        phase_policy_payload = _phase_decision.to_payload()
                        record_decision_event(
                            db,
                            component='phase_policy',
                            event_type='shadow_decision',
                            decision=phase_policy_payload.get('shadow_action'),
                            reason=phase_policy_payload.get('reason'),
                            token_ca=pos.token_ca,
                            symbol=pos.symbol,
                            lifecycle_id=pos.lifecycle_id,
                            trade_id=pos.trade_id,
                            signal_ts=pos.signal_ts,
                            signal_id=getattr(pos, 'premium_signal_id', None),
                            strategy_stage=pos.strategy_stage,
                            route=(w_entry or {}).get('signal_route') or (pos.monitor_state or {}).get('signalRoute') or getattr(pos, 'signal_type', None),
                            data_source='phase_policy_shadow',
                            payload={
                                **phase_policy_payload,
                                'current_pnl': trigger_pnl,
                                'peak_pnl': pos.peak_pnl,
                                'held_sec': max(0.0, time.time() - float(pos.entry_ts or time.time())),
                            },
                        )
                    except Exception as _phase_err:
                        log.debug(f"  [PHASE_POLICY] shadow eval failed for {pos.symbol}: {_phase_err}")

                    if (
                        LOTTO_PHASE_POLICY_LIVE_EXIT
                        and phase_policy_payload
                        and action not in ('partial_sell', 'exit')
                        and not should_exit
                        and not position_is_observation_probe(pos)
                    ):
                        _policy_route = (
                            (w_entry or {}).get('signal_route')
                            or (pos.monitor_state or {}).get('signalRoute')
                            or getattr(pos, 'signal_type', None)
                        )
                        _is_lotto_policy_route = (
                            str(_policy_route or '').upper() == 'LOTTO'
                            or (w_entry is not None and is_lotto_position(pos, w_entry))
                        )
                        _phase_state = phase_policy_payload.get('phase_state')
                        _phase_action = phase_policy_payload.get('shadow_action')
                        _phase_reason = phase_policy_payload.get('reason')
                        _live_reason = None
                        if _is_lotto_policy_route and _phase_action == 'EXIT':
                            if _phase_state == 'RUG_DEFENSE':
                                _live_reason = f"phase_rug_defense_exit ({_phase_reason})"
                            elif _phase_reason == 'no_follow_fast_fail_20s':
                                _live_reason = "phase_no_follow_fast_fail_20s"

                        if _live_reason:
                            _sell_amount_raw = int(float(pos.token_amount_raw)) if pos.token_amount_raw else 0
                            _phase_sim = simulate_exit_execution(
                                pos.token_ca,
                                str(_sell_amount_raw),
                                getattr(pos, 'token_decimals', 0) or 0,
                                pos.strategy_stage,
                                strategy_id=strategy_id,
                                lifecycle_id=pos.lifecycle_id,
                            )
                            _phase_quote_pnl = quote_pnl_from_execution(_phase_sim, pos.entry_price)
                            _phase_quote_gap = (
                                _phase_quote_pnl - trigger_pnl
                                if _phase_quote_pnl is not None and trigger_pnl is not None else None
                            )
                            exit_eval.update({
                                'shouldExit': True,
                                'action': 'exit',
                                'exitReason': _live_reason,
                                'execution': _phase_sim,
                                'sellPct': 1.0,
                                'quotePnl': _safe_float(_phase_quote_pnl, None),
                                'quoteMarkGap': _safe_float(_phase_quote_gap, None),
                                'quoteSanityStatus': 'phase_policy_live_quote_checked' if _phase_quote_pnl is not None else 'phase_policy_live_quote_missing',
                                'markSource': 'phase_policy_live',
                                'actionReason': _phase_reason,
                            })
                            action = 'exit'
                            should_exit = True
                            reason = _live_reason
                            mark_source = 'phase_policy_live'
                            mark_execution = _phase_sim
                            mark_quote_reason = _phase_sim.get('failureReason')
                            mark_quote_route = _phase_sim.get('routeAvailable')
                            mark_quote_price = _phase_sim.get('effectivePrice')
                            mark_quote_out = _phase_sim.get('quotedOutAmount', exit_eval.get('quotedOutAmount'))
                            record_decision_event(
                                db,
                                component='phase_policy',
                                event_type='control_decision',
                                decision='exit',
                                reason=_live_reason,
                                token_ca=pos.token_ca,
                                symbol=pos.symbol,
                                lifecycle_id=pos.lifecycle_id,
                                trade_id=pos.trade_id,
                                signal_ts=pos.signal_ts,
                                signal_id=getattr(pos, 'premium_signal_id', None),
                                strategy_stage=pos.strategy_stage,
                                route='LOTTO',
                                data_source='phase_policy_live',
                                payload={
                                    **phase_policy_payload,
                                    'current_pnl': trigger_pnl,
                                    'peak_pnl': pos.peak_pnl,
                                    'quote_pnl': _phase_quote_pnl,
                                    'quote_mark_gap': _phase_quote_gap,
                                    'execution_success': bool(_phase_sim.get('success')),
                                },
                            )
                            log.info(
                                f"  [PHASE_POLICY_LIVE] {pos.symbol}/{pos.strategy_stage} EXIT "
                                f"reason={_live_reason} mark={trigger_pnl:+.1%} "
                                f"quote={_phase_quote_pnl:+.1%}" if _phase_quote_pnl is not None else
                                f"  [PHASE_POLICY_LIVE] {pos.symbol}/{pos.strategy_stage} EXIT "
                                f"reason={_live_reason} mark={trigger_pnl:+.1%} quote=na"
                            )
                    record_trade_path_sample(
                        db,
                        pos,
                        sample_ts=bar_ts,
                        action=action,
                        reason=reason,
                        mark_price=price,
                        mark_pnl=trigger_pnl,
                        mark_source=mark_source,
                        quote_execution=mark_execution if action in ('partial_sell', 'exit', 'close') or should_exit else None,
                        peak_pnl=pos.peak_pnl,
                        phase_policy=phase_policy_payload,
                    )
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
                if mark_source in ('exit_guardian', 'force_timeout'):
                    record_trade_path_sample(
                        db,
                        pos,
                        sample_ts=exit_ts,
                        action=exit_eval.get('action') or 'exit',
                        reason=reason,
                        mark_price=exit_price,
                        mark_pnl=trigger_pnl,
                        mark_source=mark_source,
                        quote_execution=exit_eval.get('execution'),
                        peak_pnl=pos.peak_pnl,
                    )
                if exit_eval.get('action') == 'partial_sell':
                    updated_state = exit_eval.get('updatedState') or pos.monitor_state
                    prev_sold_pct = _safe_float(exit_eval.get('prevSoldPct'), 0.0)
                    next_sold_pct = _safe_float((updated_state or {}).get('soldPct'), prev_sold_pct)
                    sell_pct_delta = max(0.0, next_sold_pct - prev_sold_pct)
                    if sell_pct_delta <= 0:
                        sell_pct_delta = _safe_float(exit_eval.get('sellPct'), 0.0)
                    partial_quote_pnl = exit_eval.get('quotePnl')
                    if partial_quote_pnl is None:
                        partial_quote_pnl = quote_pnl_from_execution(exit_eval.get('execution'), pos.entry_price)
                    partial_quote_mark_gap = exit_eval.get('quoteMarkGap')
                    if partial_quote_mark_gap is None and partial_quote_pnl is not None:
                        partial_quote_mark_gap = partial_quote_pnl - trigger_pnl
                    pos.monitor_state = apply_partial_accounting_state(
                        updated_state,
                        exit_eval.get('execution'),
                        entry_sol=pos.position_size_sol,
                        prev_sold_pct=prev_sold_pct,
                        sell_pct_delta=sell_pct_delta,
                        trigger_pnl=trigger_pnl,
                        peak_pnl=pos.peak_pnl,
                        reason=reason,
                        ts=exit_ts,
                        quote_pnl=partial_quote_pnl,
                        quote_mark_gap=partial_quote_mark_gap,
                        quote_sanity_status=exit_eval.get('quoteSanityStatus'),
                    )
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
                                **price_unit_contract_payload(),
                                'triggerPnlPct': _safe_float(trigger_pnl * 100.0, None),
                                'quotePnlPct': _safe_float(partial_quote_pnl * 100.0 if partial_quote_pnl is not None else None, None),
                                'quoteMarkGapPct': _safe_float(partial_quote_mark_gap * 100.0 if partial_quote_mark_gap is not None else None, None),
                                'quoteSanityStatus': exit_eval.get('quoteSanityStatus'),
                                'markSource': mark_source,
                            })),
                            json.dumps(pos.monitor_state),
                            pos.trade_id,
                        )
                    )
                    db.commit()
                    log.info(
                        f"  PARTIAL {pos.symbol}/{pos.strategy_stage}: {exit_eval.get('tpName')} "
                        f"trigger_pnl={trigger_pnl*100:+.1f}% sold_pct={_safe_float(pos.monitor_state.get('soldPct'), 0.0)*100:.0f}% "
                        f"partial_realized_sol={_safe_float(pos.monitor_state.get('partialRealizedSol'), None)} "
                        f"remaining_raw={pos.token_amount_raw} lifecycle={pos.lifecycle_id}"
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
                                **price_unit_contract_payload(),
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
                    record_decision_event(
                        db,
                        component='execution_api',
                        event_type='exit_quote',
                        decision='fail',
                        reason=failure_reason,
                        token_ca=pos.token_ca,
                        symbol=pos.symbol,
                        lifecycle_id=pos.lifecycle_id,
                        trade_id=pos.trade_id,
                        signal_ts=pos.signal_ts,
                        signal_id=getattr(pos, 'premium_signal_id', None),
                        strategy_stage=pos.strategy_stage,
                        route=(pos.monitor_state or {}).get('signalRoute') or getattr(pos, 'signal_type', None),
                        data_source='jupiter_quote',
                        payload=exit_execution,
                    )
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
                            # Fix: Ensure trapped tokens are also removed from the holding watchlist
                            w_entry_trap = watchlist.get_by_ca(pos.token_ca)
                            if w_entry_trap:
                                watchlist.mark_expired(w_entry_trap['id'], trap_reason)
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
                exit_quote_pnl = quote_pnl_from_execution(exit_execution, pos.entry_price)
                exit_quote_mark_gap = (
                    exit_quote_pnl - trigger_pnl
                    if exit_quote_pnl is not None and trigger_pnl is not None else None
                )
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

                if exit_quote_pnl is not None and exit_quote_mark_gap is not None:
                    if abs(exit_quote_mark_gap) >= EXIT_QUOTE_REPRICE_DIVERGENCE_PCT:
                        log.warning(
                            f"  [PNL_SANITY] {pos.symbol} trigger/quote gap={exit_quote_mark_gap*100:+.0f}pp "
                            f"trigger={trigger_pnl*100:+.1f}% quote={exit_quote_pnl*100:+.1f}%. "
                            f"Using executable quote PnL."
                        )
                        realized_pnl = exit_quote_pnl
                        accounting_source = f'quote_pnl_reprice(was={accounting_source})'
                        exit_eval['quoteSanityStatus'] = 'exit_repriced_quote_mark_divergence'

                # ─── PnL Sanity Guard ────────────────────────────────────
                # quotedOutAmount from Jupiter can be wildly wrong for
                # low-liquidity tokens (e.g. returning token amount instead
                # of SOL amount). If accounting PnL diverges > 50pp from
                # the price-based trigger PnL, fall back to trigger PnL
                # which is derived from real market prices.
                if trigger_pnl is not None and realized_pnl is not None:
                    divergence = abs(realized_pnl - trigger_pnl)
                    if (
                        divergence > 0.50
                        and not (
                            exit_quote_pnl is not None
                            and exit_quote_mark_gap is not None
                            and abs(exit_quote_mark_gap) >= EXIT_QUOTE_REPRICE_DIVERGENCE_PCT
                        )
                    ):  # >50 percentage points
                        log.warning(
                            f"  [PNL_SANITY] {pos.symbol} accounting PnL={realized_pnl*100:+.1f}% "
                            f"diverges from trigger PnL={trigger_pnl*100:+.1f}% "
                            f"(gap={divergence*100:.0f}pp). Using trigger PnL."
                        )
                        realized_pnl = trigger_pnl
                        accounting_source = f'trigger_pnl_override(was={accounting_source})'

                mark_peak_before_close = _safe_float(pos.peak_pnl, 0.0)
                close_peak_candidates = [
                    mark_peak_before_close,
                    _safe_float(trigger_pnl, None),
                    _safe_float(exit_quote_pnl, None),
                    _safe_float(realized_pnl, None),
                ]
                close_peak_candidates = [
                    value for value in close_peak_candidates
                    if value is not None and value == value
                ]
                close_peak_pnl = max(close_peak_candidates) if close_peak_candidates else mark_peak_before_close
                if close_peak_pnl > mark_peak_before_close:
                    pos.peak_pnl = close_peak_pnl
                    pos.monitor_state = dict(pos.monitor_state or {})
                    pos.monitor_state['markPeakPnlBeforeClose'] = _safe_float(mark_peak_before_close, None)
                    pos.monitor_state['peakPnlAdjustedByCloseQuote'] = True
                    pos.monitor_state['closePeakPnl'] = _safe_float(close_peak_pnl, None)

                pos.monitor_state = dict(pos.monitor_state or {})
                pos.monitor_state['closed'] = True
                pos.monitor_state['exitReason'] = reason
                pos.monitor_state['exitPriceUnit'] = PRICE_UNIT_SOL_PER_TOKEN
                pos.monitor_state['triggerPriceUnit'] = PRICE_UNIT_SOL_PER_TOKEN
                pos.monitor_state['pnlUnit'] = PNL_UNIT_RATIO_DECIMAL
                pos.monitor_state['accountingUnit'] = AMOUNT_UNIT_SOL
                pos.monitor_state['priceUnitContractVersion'] = PRICE_UNIT_CONTRACT_VERSION
                pos.monitor_state['finalExitSol'] = _safe_float(actual_out, None)
                pos.monitor_state['totalRealizedSol'] = _safe_float(total_realized_sol, None)
                pos.monitor_state['blendedRealizedPnl'] = _safe_float(realized_pnl, None)
                pos.monitor_state['accountingSource'] = accounting_source
                pos.monitor_state['exitQuotePnl'] = _safe_float(exit_quote_pnl, None)
                pos.monitor_state['exitQuoteMarkGap'] = _safe_float(exit_quote_mark_gap, None)
                pos.monitor_state['exitQuoteSanity'] = exit_eval.get('quoteSanityStatus')
                if total_realized_sol is not None:
                    pos.monitor_state['totalSolReceived'] = _safe_float(total_realized_sol, None)

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
                        strategy_outcome = ?, execution_availability = ?, accounting_outcome = ?, synthetic_close = 0,
                        monitor_state_json = ?
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
                        **price_unit_contract_payload(
                            effectiveExitPriceUnit=PRICE_UNIT_SOL_PER_TOKEN,
                            triggerPriceUnit=PRICE_UNIT_SOL_PER_TOKEN,
                        ),
                        'triggerPnlPct': _safe_float(trigger_pnl * 100.0, None),
                        'realizedPnlPct': _safe_float(realized_pnl * 100.0, None),
                        'totalRealizedSol': _safe_float(total_realized_sol, None),
                        'lifecycleEntrySol': _safe_float(lifecycle_entry_sol, None),
                        'accountingSource': accounting_source,
                        'quotePnlPct': _safe_float(exit_quote_pnl * 100.0 if exit_quote_pnl is not None else None, None),
                        'quoteMarkGapPct': _safe_float(exit_quote_mark_gap * 100.0 if exit_quote_mark_gap is not None else None, None),
                        'quoteSanityStatus': exit_eval.get('quoteSanityStatus'),
                        'partialRealizedSol': _safe_float((pos.monitor_state or {}).get('partialRealizedSol'), None),
                        'partialCostBasisSol': _safe_float((pos.monitor_state or {}).get('partialCostBasisSol'), None),
                        'partialRealizedPnlContribution': _safe_float((pos.monitor_state or {}).get('partialRealizedPnlContribution'), None),
                        'soldPct': _safe_float((pos.monitor_state or {}).get('soldPct'), None),
                        'preExitTotalSolReceived': _safe_float(exit_eval.get('preExitTotalSolReceived', exit_execution.get('preExitTotalSolReceived')), None),
                        'exitSolReceived': _safe_float(exit_eval.get('exitSolReceived', exit_execution.get('exitSolReceived')), None),
                        'postExitTotalSolReceived': _safe_float(exit_eval.get('postExitTotalSolReceived', exit_execution.get('postExitTotalSolReceived')), None),
                        'triggerPrice': _safe_float(exit_price, None),
                        'effectiveExitPrice': _safe_float(effective_exit_price, None),
                    })),
                    'force_timeout' if is_force_timeout else reason,
                    'unavailable' if is_force_timeout else 'available',
                    'closed_force_timeout' if is_force_timeout else 'closed_real',
                    json.dumps(pos.monitor_state),
                    pos.trade_id,
                ))
                db.commit()
                record_decision_event(
                    db,
                    component='trade_lifecycle',
                    event_type='position_closed',
                    decision='closed',
                    reason=reason,
                    token_ca=pos.token_ca,
                    symbol=pos.symbol,
                    lifecycle_id=pos.lifecycle_id,
                    trade_id=pos.trade_id,
                    signal_ts=pos.signal_ts,
                    signal_id=getattr(pos, 'premium_signal_id', None),
                    strategy_stage=pos.strategy_stage,
                    route=(pos.monitor_state or {}).get('signalRoute') or getattr(pos, 'signal_type', None),
                    data_source=mark_source,
                    payload={
                        'realized_pnl': realized_pnl,
                        'trigger_pnl': trigger_pnl,
                        'peak_pnl': pos.peak_pnl,
                        'bars_held': pos.bars_held,
                        'accounting_source': accounting_source,
                        'execution_availability': 'unavailable' if is_force_timeout else 'available',
                    },
                )
                
                # Update Watchlist Status
                w_entry_close = watchlist.get_by_ca(pos.token_ca)
                if w_entry_close:
                    if position_is_observation_probe(pos):
                        _toxic_probe_exit = _token_risk_probe_loss_should_poison(reason)
                        _exit_cooldown = (
                            OBSERVATION_PROBE_TOXIC_COOLDOWN_SEC
                            if _toxic_probe_exit and realized_pnl is not None and realized_pnl < 0
                            else OBSERVATION_PROBE_COOLDOWN_SEC
                        )
                        log.info(
                            f"[WL] Observation probe cooldown: {pos.symbol} "
                            f"cooldown={_exit_cooldown}s toxic={1 if _toxic_probe_exit else 0}"
                        )
                    else:
                        # Loss exits: 30-minute cooldown — avoid dying-token re-entry
                        #   Data: ASTROID re-entered 6 min after -9.1% exit → -20.4% again
                        # Win exits: 5-minute cooldown — avoid immediate double-dip
                        #   Data: ASTERWOJAK re-entered 2.5 min after +5.9% exit → -7.7%
                        if realized_pnl is not None and realized_pnl < 0:
                            _exit_cooldown = 1800  # 30 min
                        else:
                            _exit_cooldown = 300   # 5 min
                    watchlist.mark_watching(w_entry_close['id'], realized_pnl, cooldown_sec=_exit_cooldown)
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
        min_id = None
        if '--stats-min-id' in sys.argv:
            idx = sys.argv.index('--stats-min-id')
            if idx + 1 >= len(sys.argv) or sys.argv[idx + 1].startswith('-'):
                log.error("--stats-min-id requires an integer value")
                sys.exit(2)
            try:
                min_id = int(sys.argv[idx + 1])
            except ValueError:
                log.error("--stats-min-id requires an integer value")
                sys.exit(2)
        print_all_stats(db, min_id=min_id)
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
