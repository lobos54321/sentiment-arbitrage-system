#!/usr/bin/env python3
"""
Paper Trade Monitor — forward-test NOT_ATH signals with staged lifecycle execution.

NOT_ATH Strategy:
  - Stage 1: super > 80
  - Stage 1 exit: SL=-3%, trail@+2%/0.90, timeout=120min
  - Stage 2A: after stage1 stop-loss, wait 3 bars and re-enter on +18% rebound from post-stop rolling low
  - Stage 3: 30 bars after signal, if first peak >= 10%, continuation re-entry

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
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

# === Configuration ===
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
SENTIMENT_DB = os.environ.get('SENTIMENT_DB', str(DATA_DIR / 'sentiment_arb.db'))
PAPER_DB = os.environ.get('PAPER_DB', str(DATA_DIR / 'paper_trades.db'))
KLINE_DB = os.environ.get('KLINE_DB', str(DATA_DIR / 'kline_cache.db'))
REGISTRY_JSON = os.environ.get('PAPER_STRATEGY_REGISTRY', str(DATA_DIR / 'paper-strategy-registry.json'))
REMOTE_SIGNAL_URL = os.environ.get('REMOTE_SIGNAL_URL', '').strip()
REMOTE_SIGNAL_TOKEN = os.environ.get('REMOTE_SIGNAL_TOKEN', '').strip()
REMOTE_SIGNAL_LOOKBACK = max(50, int(os.environ.get('REMOTE_SIGNAL_LOOKBACK', '500')))

DEFAULT_STRATEGY_ID = 'notath-selective-v1'
DEFAULT_STRATEGY_ROLE = 'selective_challenger'
DEFAULT_STAGE1_EXIT = {'stopLossPct': 3, 'trailStartPct': 2, 'trailFactor': 0.9, 'timeoutMinutes': 120}
DEFAULT_STAGE2A = {'enabled': True, 'waitBarsAfterStop': 3, 'reboundFromRollingLowPct': 18, 'rollingLowBars': 3, 'entryPriceMode': 'close', 'stopLossPct': 4, 'trailStartPct': 3, 'trailFactor': 0.9, 'timeoutMinutes': 120}
DEFAULT_STAGE3 = {'enabled': True, 'waitBarsFromSignal': 30, 'firstPeakMinPct': 10, 'entryPriceMode': 'close', 'stopLossPct': 4, 'trailStartPct': 3, 'trailFactor': 0.9, 'timeoutMinutes': 120}

# Backward-compatible defaults for code paths that still use legacy constants.
SL_PCT = -0.03
TRAIL_START = 0.02
TRAIL_FACTOR = 0.90
TIMEOUT_MIN = 120

# Polling intervals (seconds)
SIGNAL_POLL_INTERVAL = 30       # check for new signals
POSITION_POLL_INTERVAL = 60     # update open positions
DAILY_REPORT_HOUR = 0           # UTC hour for daily report

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
        'stageRules': {
            'stage1Exit': dict(DEFAULT_STAGE1_EXIT),
            'stage2A': dict(DEFAULT_STAGE2A),
            'stage3': dict(DEFAULT_STAGE3),
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

def curl_json(url, timeout=15):
    """Fetch JSON via curl."""
    try:
        result = subprocess.run(
            ['curl', '-s', '-m', str(timeout), url],
            capture_output=True, text=True, timeout=timeout + 5
        )
        if result.returncode != 0:
            return None
        return json.loads(result.stdout)
    except Exception:
        return None


def get_pool_address(token_ca, cache={}):
    """Get pool address from DexScreener, with in-memory cache."""
    if token_ca in cache:
        return cache[token_ca]

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
        cache[token_ca] = pool
    return pool or None


def get_current_bar(token_ca, pool_address=None):
    """Get latest GeckoTerminal 1m bar."""
    if not pool_address:
        pool_address = get_pool_address(token_ca)
    if not pool_address:
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


def get_current_price(token_ca, pool_address=None):
    """Get latest price from GeckoTerminal (latest 1m candle close)."""
    bar = get_current_bar(token_ca, pool_address)
    if not bar:
        return None
    return bar['close']


# === NOT_ATH Scoring ===

def parse_super_index(description):
    """
    Parse Super Index from NOT_ATH description.
    Supports formats:
      ✡ Super Index： 119🔮
      ✡ Super Index： ✡ x 82
    Returns int or None.
    """
    if not description:
        return None
    # Try format: " 119🔮"
    m = re.search(r'Super\s+Index[：:]\s*(\d+)\s*🔮', description)
    if m:
        return int(m.group(1))
    # Try format: "✡ x 82"
    m = re.search(r'Super\s+Index[：:]\s*✡\s*x\s*(\d+)', description, re.IGNORECASE)
    if m:
        return int(m.group(1))
    return None


def get_notath_bars(pool_address, limit=5):
    """
    Get recent 1-minute bars for NOT_ATH scoring.
    Returns list of bars (newest first), or None.
    Each bar: {ts, open, high, low, close, volume}
    """
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


def get_entry_bar_ohlcv(pool_address):
    """Get the most recent completed 1-minute OHLCV bar for FBR check."""
    url = (
        f"https://api.geckoterminal.com/api/v2/networks/solana/pools/"
        f"{pool_address}/ohlcv/minute?aggregate=1&limit=2"
    )
    data = curl_json(url)
    if not data:
        return None
    ohlcv_list = data.get('data', {},).get('attributes', {}).get('ohlcv_list', [])
    if not ohlcv_list or len(ohlcv_list[0]) < 6:
        return None
    # Return the most recent completed bar
    row = ohlcv_list[0]
    return {
        'ts': int(row[0]),
        'open': float(row[1]),
        'high': float(row[2]),
        'low': float(row[3]),
        'close': float(row[4]),
        'volume': float(row[5]),
    }


def get_sol_price():
    """Get current SOL/USD price from DexScreener (SOL wrapped token)."""
    data = curl_json("https://api.dexscreener.com/latest/dex/tokens/So11111111111111111111111111111111111111112")
    if not data:
        return None
    pairs = data.get('pairs', [])
    if not pairs:
        return None
    # Pick USDC pair with highest liquidity
    usdc_pairs = [p for p in pairs if 'USD' in (p.get('quoteToken', {}).get('symbol', '') or '').upper()]
    if usdc_pairs:
        pair = max(usdc_pairs, key=lambda p: (p.get('liquidity', {}).get('usd', 0) or 0))
    else:
        pair = pairs[0]
    try:
        return float(pair.get('priceUsd', 0))
    except (TypeError, ValueError):
        return None


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
    return status in {'PASS', 'RISK_BLOCKED'} and 'New Trending' in description


def _query_local_new_signals(last_signal_id):
    sdb = sqlite3.connect(SENTIMENT_DB)
    sdb.row_factory = sqlite3.Row
    rows = sdb.execute("""
        SELECT id, token_ca, symbol, timestamp, description, hard_gate_status
        FROM premium_signals
        WHERE id > ?
          AND hard_gate_status IN ('PASS', 'RISK_BLOCKED')
          AND description LIKE '%New Trending%'
        ORDER BY id ASC
    """, (last_signal_id,)).fetchall()
    sdb.close()
    return _normalize_signal_rows(rows)


def _query_local_recent_signals(limit=20):
    sdb = sqlite3.connect(SENTIMENT_DB)
    sdb.row_factory = sqlite3.Row
    rows = sdb.execute("""
        SELECT id, token_ca, symbol, timestamp, description, hard_gate_status
        FROM premium_signals
        WHERE hard_gate_status IN ('PASS', 'RISK_BLOCKED')
          AND description LIKE '%New Trending%'
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


# === Position Tracking ===

class Position:
    """Tracks an open paper trade position."""
    __slots__ = [
        'trade_id', 'token_ca', 'symbol', 'signal_ts', 'entry_price', 'entry_ts',
        'pool_address', 'peak_pnl', 'trailing_active', 'bars_held', 'last_bar_ts',
        'strategy_stage', 'lifecycle_id', 'exit_rules',
    ]

    def __init__(self, trade_id, token_ca, symbol, signal_ts, entry_price, entry_ts, pool_address, strategy_stage, lifecycle_id, exit_rules):
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
        self.peak_pnl = 0.0
        self.trailing_active = False
        self.bars_held = 0
        self.last_bar_ts = int(entry_ts)

    def check_exit(self, current_price, bar_ts):
        """Apply exit logic on a specific 1m bar. Returns (should_exit, exit_reason, pnl_pct) or (False, None, None)."""
        if current_price is None or current_price <= 0 or bar_ts is None:
            return False, None, None

        stop_loss = -pct_to_decimal(self.exit_rules.get('stopLossPct', DEFAULT_STAGE1_EXIT['stopLossPct']))
        trail_start = pct_to_decimal(self.exit_rules.get('trailStartPct', DEFAULT_STAGE1_EXIT['trailStartPct']))
        trail_factor = float(self.exit_rules.get('trailFactor', DEFAULT_STAGE1_EXIT['trailFactor']))
        timeout_min = int(self.exit_rules.get('timeoutMinutes', DEFAULT_STAGE1_EXIT['timeoutMinutes']))

        bar_ts = int(bar_ts)
        if bar_ts <= self.last_bar_ts:
            pnl = (current_price - self.entry_price) / self.entry_price
            return False, None, pnl

        self.last_bar_ts = bar_ts
        self.bars_held = max(self.bars_held + 1, int((bar_ts - self.entry_ts) / 60) + 1)
        pnl = (current_price - self.entry_price) / self.entry_price
        self.peak_pnl = max(self.peak_pnl, pnl)

        if not self.trailing_active and self.peak_pnl >= trail_start:
            self.trailing_active = True
            log.info(f"  [{self.symbol}/{self.strategy_stage}] Trailing activated at peak={self.peak_pnl*100:+.1f}%")

        if not self.trailing_active and pnl <= stop_loss:
            return True, 'sl', stop_loss

        if self.trailing_active:
            trail_level = self.peak_pnl * trail_factor
            if pnl <= trail_level:
                exit_pnl = max(pnl, trail_level)
                return True, 'trail', exit_pnl

        elapsed_min = (bar_ts - self.entry_ts) / 60
        if elapsed_min >= timeout_min:
            return True, 'timeout', pnl

        return False, None, pnl


def build_lifecycle_id(token_ca, signal_ts):
    return f"{token_ca}:{int(signal_ts)}"


def stage_seq(stage_name):
    return {'stage1': 1, 'stage2A': 2, 'stage3': 3}.get(stage_name, 0)


def get_exit_rules_for_stage(strategy_config, stage_name):
    stage_rules = (strategy_config or {}).get('stageRules') or {}
    if stage_name == 'stage1':
        return dict(stage_rules.get('stage1Exit') or DEFAULT_STAGE1_EXIT)
    if stage_name == 'stage2A':
        return dict(stage_rules.get('stage2A') or DEFAULT_STAGE2A)
    if stage_name == 'stage3':
        return dict(stage_rules.get('stage3') or DEFAULT_STAGE3)
    return dict(DEFAULT_STAGE1_EXIT)


def restore_lifecycles(db):
    lifecycles = {}
    rows = db.execute("""
        SELECT id, token_ca, symbol, signal_ts, strategy_stage, exit_reason, pnl_pct,
               peak_pnl, entry_ts, exit_ts, lifecycle_id, parent_trade_id, bars_held,
               first_peak_pct, rolling_low_price, rolling_low_ts, reentry_source
        FROM paper_trades
        ORDER BY signal_ts ASC, id ASC
    """).fetchall()
    for row in rows:
        lifecycle_id = row['lifecycle_id'] or build_lifecycle_id(row['token_ca'], row['signal_ts'])
        item = lifecycles.setdefault(lifecycle_id, {
            'lifecycle_id': lifecycle_id,
            'token_ca': row['token_ca'],
            'symbol': row['symbol'],
            'signal_ts': row['signal_ts'],
            'first_peak_pct': 0.0,
            'stage1_trade_id': None,
            'stage2a_trade_id': None,
            'stage3_trade_id': None,
            'stage2a_attempted': False,
            'stage3_attempted': False,
            'stage1_stop_ts': None,
            'rolling_low_after_stop': None,
            'rolling_low_ts': None,
        })
        stage = row['strategy_stage'] or 'stage1'
        if stage == 'stage1':
            item['stage1_trade_id'] = row['id']
            item['first_peak_pct'] = max(item['first_peak_pct'], float(row['peak_pnl'] or 0) * 100.0, float(row['first_peak_pct'] or 0) or 0.0)
            if row['exit_reason'] == 'sl':
                item['stage1_stop_ts'] = row['exit_ts']
        elif stage == 'stage2A':
            item['stage2a_trade_id'] = row['id']
            item['stage2a_attempted'] = True
        elif stage == 'stage3':
            item['stage3_trade_id'] = row['id']
            item['stage3_attempted'] = True
        if row['rolling_low_price'] is not None:
            item['rolling_low_after_stop'] = row['rolling_low_price']
            item['rolling_low_ts'] = row['rolling_low_ts']
    return lifecycles


def count_open_positions_for_lifecycle(positions, lifecycle_id):
    return sum(1 for pos in positions.values() if pos.lifecycle_id == lifecycle_id)


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
    stage2a_rules = get_exit_rules_for_stage(strategy_config, 'stage2A')
    stage3_rules = get_exit_rules_for_stage(strategy_config, 'stage3')
    min_super_index = int(((strategy_config.get('entryTimingFilters') or {}).get('minSuperIndex')) or 80)
    strategy_id = strategy_config.get('strategyId') or DEFAULT_STRATEGY_ID
    strategy_role = strategy_config.get('strategyRole') or DEFAULT_STRATEGY_ROLE

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

        bars = get_kline_bars(kline_db, token_ca, signal_ts, limit=max(stage1_exit['timeoutMinutes'], stage3_rules.get('waitBarsFromSignal', 30) + stage3_rules.get('timeoutMinutes', 120), 240))
        if len(bars) < 5:
            log.info(f"  [{symbol}] insufficient K-line bars ({len(bars)}), skipping")
            continue

        real_klines_used += 1
        existing_stages = set()
        lifecycle = {
            'lifecycle_id': lifecycle_id,
            'token_ca': token_ca,
            'symbol': symbol,
            'signal_ts': int(signal_ts_ms),
            'first_peak_pct': 0.0,
            'stage1_trade_id': None,
            'stage2a_trade_id': None,
            'stage3_trade_id': None,
            'stage2a_attempted': False,
            'stage3_attempted': False,
            'stage1_stop_ts': None,
            'rolling_low_after_stop': None,
            'rolling_low_ts': None,
        }

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
                if not trailing_active and pnl <= -pct_to_decimal(exit_rules['stopLossPct']):
                    exit_reason = 'sl'
                    exit_pnl = -pct_to_decimal(exit_rules['stopLossPct'])
                    exit_price = entry_price * (1 + exit_pnl)
                    break
                if trailing_active:
                    trail_level = peak_pnl * float(exit_rules['trailFactor'])
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

        if stage1_result['exit_reason'] == 'sl' and stage_rules.get('stage2A', {}).get('enabled') and 'stage2A' not in existing_stages:
            lifecycle['stage1_stop_ts'] = stage1_result['exit_ts']
            wait_bars = int(stage_rules['stage2A'].get('waitBarsAfterStop', 3))
            rolling_low = None
            rolling_low_ts = None
            stage2_start_index = None
            for idx in range(stage1_result['bars_held'] + wait_bars, len(bars)):
                close_price = bars[idx]['close'] or bars[idx]['open']
                if not close_price or close_price <= 0:
                    continue
                if rolling_low is None or close_price < rolling_low:
                    rolling_low = close_price
                    rolling_low_ts = int(bars[idx]['timestamp'])
                rebound_target = rolling_low * (1 + pct_to_decimal(stage_rules['stage2A'].get('reboundFromRollingLowPct', 18)))
                if close_price >= rebound_target:
                    stage2_start_index = idx
                    lifecycle['rolling_low_after_stop'] = rolling_low
                    lifecycle['rolling_low_ts'] = rolling_low_ts
                    break
            stage_counts['stage2A_armed'] += 1
            lifecycle['stage2a_attempted'] = True
            if stage2_start_index is not None:
                stage2_result = replay_trade('stage2A', stage2_start_index, stage2a_rules)
                if stage2_result:
                    db.execute("""
                        INSERT INTO paper_trades
                            (strategy_id, strategy_role, strategy_stage, stage_outcome,
                             token_ca, symbol, signal_ts, entry_price, entry_ts,
                             exit_price, exit_ts, exit_reason, pnl_pct, bars_held,
                             market_regime, replay_source, peak_pnl, trailing_active,
                             lifecycle_id, parent_trade_id, stage_seq, trigger_ts, trigger_price,
                             armed_ts, rolling_low_price, rolling_low_ts, reentry_source)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        strategy_id, strategy_role, 'stage2A', f"stage2A_{stage2_result['exit_reason']}",
                        token_ca, symbol, int(signal_ts_ms), stage2_result['entry_price'], stage2_result['entry_ts'],
                        stage2_result['exit_price'], stage2_result['exit_ts'], stage2_result['exit_reason'], stage2_result['exit_pnl'], stage2_result['bars_held'],
                        'real_kline', 'real_kline_replay', stage2_result['peak_pnl'], int(stage2_result['trailing_active']),
                        lifecycle_id, stage1_trade_id, stage_seq('stage2A'), stage2_result['entry_ts'], stage2_result['entry_price'],
                        lifecycle['stage1_stop_ts'], lifecycle.get('rolling_low_after_stop'), lifecycle.get('rolling_low_ts'), 'stage1_sl_rebound'
                    ))
                    stage2_trade_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
                    db.commit()
                    existing_stages.add('stage2A')
                    lifecycle['stage2a_trade_id'] = stage2_trade_id
                    stage_counts['stage2A_entered'] += 1
                    stage_counts[f"stage2A_exit_{stage2_result['exit_reason']}"] += 1
                    completed.append(stage2_result['exit_pnl'])
            else:
                stage_counts['stage2A_expired'] += 1

        if stage_rules.get('stage3', {}).get('enabled') and 'stage3' not in existing_stages:
            lifecycle['stage3_attempted'] = True
            wait_bars = int(stage_rules['stage3'].get('waitBarsFromSignal', 30))
            first_peak_min = float(stage_rules['stage3'].get('firstPeakMinPct', 10))
            if lifecycle['first_peak_pct'] >= first_peak_min and len(bars) > wait_bars:
                stage3_result = replay_trade('stage3', wait_bars, stage3_rules)
                stage_counts['stage3_eligible'] += 1
                if stage3_result:
                    db.execute("""
                        INSERT INTO paper_trades
                            (strategy_id, strategy_role, strategy_stage, stage_outcome,
                             token_ca, symbol, signal_ts, entry_price, entry_ts,
                             exit_price, exit_ts, exit_reason, pnl_pct, bars_held,
                             market_regime, replay_source, peak_pnl, trailing_active,
                             lifecycle_id, parent_trade_id, stage_seq, trigger_ts, trigger_price,
                             reentry_source, first_peak_pct)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        strategy_id, strategy_role, 'stage3', f"stage3_{stage3_result['exit_reason']}",
                        token_ca, symbol, int(signal_ts_ms), stage3_result['entry_price'], stage3_result['entry_ts'],
                        stage3_result['exit_price'], stage3_result['exit_ts'], stage3_result['exit_reason'], stage3_result['exit_pnl'], stage3_result['bars_held'],
                        'real_kline', 'real_kline_replay', stage3_result['peak_pnl'], int(stage3_result['trailing_active']),
                        lifecycle_id, stage1_trade_id, stage_seq('stage3'), stage3_result['entry_ts'], stage3_result['entry_price'],
                        'continuation_reentry', lifecycle['first_peak_pct']
                    ))
                    db.commit()
                    existing_stages.add('stage3')
                    stage_counts['stage3_entered'] += 1
                    stage_counts[f"stage3_exit_{stage3_result['exit_reason']}"] += 1
                    completed.append(stage3_result['exit_pnl'])
            else:
                stage_counts['stage3_skipped'] += 1

    kline_db.close()

    log.info(f"\n=== DRY RUN COMPLETE ===")
    log.info(f"Real K-line replays: {real_klines_used}")
    log.info(f"Stage 1 entered / rejected: {stage_counts['stage1_entered']} / {stage_counts['stage1_rejected']}")
    log.info(f"Stage 1 exit breakdown: sl={stage_counts['stage1_exit_sl']} trail={stage_counts['stage1_exit_trail']} timeout={stage_counts['stage1_exit_timeout']}")
    log.info(f"Stage 2A armed / entered / expired: {stage_counts['stage2A_armed']} / {stage_counts['stage2A_entered']} / {stage_counts['stage2A_expired']}")
    log.info(f"Stage 3 eligible / entered / skipped: {stage_counts['stage3_eligible']} / {stage_counts['stage3_entered']} / {stage_counts['stage3_skipped']}")
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


# === Live Monitor Loop ===

def run_monitor(db):
    """Main monitoring loop."""
    strategy_config = load_active_strategy_config()
    stage_rules = strategy_config.get('stageRules') or {}
    stage1_exit = get_exit_rules_for_stage(strategy_config, 'stage1')
    stage2a_rules = get_exit_rules_for_stage(strategy_config, 'stage2A')
    stage3_rules = get_exit_rules_for_stage(strategy_config, 'stage3')
    min_super_index = int(((strategy_config.get('entryTimingFilters') or {}).get('minSuperIndex')) or 80)
    strategy_id = strategy_config.get('strategyId') or DEFAULT_STRATEGY_ID
    strategy_role = strategy_config.get('strategyRole') or DEFAULT_STRATEGY_ROLE

    log.info("=== Paper Trade Monitor Started ===")
    log.info(f"  strategy={strategy_id} role={strategy_role}")
    log.info(f"  stage1 exit: SL={stage1_exit['stopLossPct']}% Trail Start={stage1_exit['trailStartPct']}% Trail Factor={stage1_exit['trailFactor']*100:.0f}% Timeout={stage1_exit['timeoutMinutes']}min")
    log.info(f"  stage2A exit: SL={stage2a_rules['stopLossPct']}% Trail Start={stage2a_rules['trailStartPct']}% Timeout={stage2a_rules['timeoutMinutes']}min")
    log.info(f"  stage3 exit: SL={stage3_rules['stopLossPct']}% Trail Start={stage3_rules['trailStartPct']}% Timeout={stage3_rules['timeoutMinutes']}min")
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
    pending_entries = {}
    lifecycles = restore_lifecycles(db)

    open_rows = db.execute("""
        SELECT id, token_ca, symbol, signal_ts, entry_price, entry_ts, peak_pnl, trailing_active, bars_held,
               strategy_stage, lifecycle_id
        FROM paper_trades
        WHERE exit_reason IS NULL
    """).fetchall()
    for r in open_rows:
        pool = get_pool_address(r['token_ca'])
        pos = Position(r['id'], r['token_ca'], r['symbol'], r['signal_ts'], r['entry_price'], r['entry_ts'], pool,
                       r['strategy_stage'] or 'stage1', r['lifecycle_id'] or build_lifecycle_id(r['token_ca'], r['signal_ts']),
                       get_exit_rules_for_stage(strategy_config, r['strategy_stage'] or 'stage1'))
        pos.peak_pnl = r['peak_pnl'] or 0
        pos.trailing_active = bool(r['trailing_active'])
        pos.bars_held = r['bars_held'] or 0
        pos.last_bar_ts = int(r['entry_ts']) + max((r['bars_held'] or 0) - 1, 0) * 60
        positions[pos.trade_id] = pos
        time.sleep(0.2)

    if positions:
        log.info(f"  Restored {len(positions)} open positions")

    last_id_row = db.execute("SELECT MAX(signal_ts) as max_ts FROM paper_trades").fetchone()
    last_signal_id = 0
    if last_id_row and last_id_row['max_ts']:
        try:
            signal_ts_cutoff = last_id_row['max_ts']
            recent_rows = get_recent_signals(limit=REMOTE_SIGNAL_LOOKBACK)
            eligible = [r for r in recent_rows if (r.get('timestamp') or 0) <= signal_ts_cutoff]
            if eligible:
                last_signal_id = max((r.get('id') or 0) for r in eligible)
        except Exception:
            pass

    log.info(f"  Starting from signal ID > {last_signal_id}")

    last_position_check = 0
    last_daily_report = None
    sol_price = None

    while True:
        now = time.time()
        now_utc = datetime.utcfromtimestamp(now)

        try:
            new_signals = get_new_signals(last_signal_id)
            for sig in new_signals:
                token_ca = sig['token_ca']
                last_signal_id = sig['id']
                signal_ts = sig['timestamp']
                lifecycle_id = build_lifecycle_id(token_ca, signal_ts)

                if any(pos.lifecycle_id == lifecycle_id for pos in positions.values()) or lifecycle_id in pending_entries:
                    continue

                existing = db.execute(
                    "SELECT id FROM paper_trades WHERE lifecycle_id = ? OR (token_ca = ? AND signal_ts = ? AND strategy_stage = 'stage1')",
                    (lifecycle_id, token_ca, signal_ts)
                ).fetchone()
                if existing:
                    continue

                symbol = sig['symbol'] or token_ca[:8]
                super_idx = parse_super_index(sig['description'] or '')
                if super_idx is None or super_idx <= min_super_index:
                    continue

                pool = get_pool_address(token_ca)
                if not pool:
                    log.warning(f"  Could not find pool for {symbol}, skipping")
                    continue
                time.sleep(0.3)

                signal_minute_ts = get_signal_minute_ts(signal_ts)
                pending_entries[lifecycle_id] = {
                    'token_ca': token_ca,
                    'symbol': symbol,
                    'signal_ts': signal_ts,
                    'signal_minute_ts': signal_minute_ts,
                    'pool': pool,
                    'lifecycle_id': lifecycle_id,
                    'super_idx': super_idx,
                }
                lifecycles.setdefault(lifecycle_id, {
                    'lifecycle_id': lifecycle_id,
                    'token_ca': token_ca,
                    'symbol': symbol,
                    'signal_ts': signal_ts,
                    'first_peak_pct': 0.0,
                    'stage1_trade_id': None,
                    'stage2a_trade_id': None,
                    'stage3_trade_id': None,
                    'stage2a_attempted': False,
                    'stage3_attempted': False,
                    'stage1_stop_ts': None,
                    'rolling_low_after_stop': None,
                    'rolling_low_ts': None,
                })
                log.info(f"New signal: {symbol} lifecycle={lifecycle_id} super={super_idx} staged for stage1 close @ {signal_minute_ts}")
                time.sleep(0.2)
        except Exception as e:
            log.error(f"Signal check error: {e}")

        if pending_entries:
            for lifecycle_id, pending in list(pending_entries.items()):
                try:
                    bars = get_notath_bars(pending['pool'], limit=8)
                    if not bars:
                        continue
                    signal_bar = next((b for b in bars if int(b['ts']) == pending['signal_minute_ts']), None)
                    if signal_bar is None:
                        continue
                    price = signal_bar['close']
                    if not price or price <= 0:
                        pending_entries.pop(lifecycle_id, None)
                        continue
                    entry_ts = int(signal_bar['ts'])
                    if sol_price is None:
                        sol_price = get_sol_price()
                        time.sleep(0.2)
                    regime = determine_market_regime(sol_price) if sol_price else 'unknown'
                    db.execute("""
                        INSERT INTO paper_trades
                            (strategy_id, strategy_role, strategy_stage, stage_outcome,
                             token_ca, symbol, signal_ts, entry_price, entry_ts,
                             market_regime, replay_source, peak_pnl, trailing_active,
                             lifecycle_id, stage_seq, trigger_ts, trigger_price)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'live_monitor', 0, 0, ?, ?, ?, ?)
                    """, (
                        strategy_id, strategy_role, 'stage1', 'stage1_entered',
                        pending['token_ca'], pending['symbol'], pending['signal_ts'], price, entry_ts,
                        regime, lifecycle_id, stage_seq('stage1'), entry_ts, price
                    ))
                    trade_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
                    db.commit()
                    pos = Position(trade_id, pending['token_ca'], pending['symbol'], pending['signal_ts'], price, entry_ts, pending['pool'],
                                   'stage1', lifecycle_id, stage1_exit)
                    positions[pos.trade_id] = pos
                    lifecycles[lifecycle_id]['stage1_trade_id'] = trade_id
                    pending_entries.pop(lifecycle_id, None)
                    log.info(f"  Entered {pending['symbol']}/stage1 @ ${price:.10f} lifecycle={lifecycle_id}")
                    time.sleep(0.2)
                except Exception as e:
                    log.error(f"  Pending entry error for {pending.get('symbol', lifecycle_id)}: {e}")

        if now - last_position_check >= POSITION_POLL_INTERVAL:
            last_position_check = now
            to_close = []
            try:
                new_sol = get_sol_price()
                if new_sol:
                    sol_price = new_sol
                time.sleep(0.2)
            except Exception:
                pass

            for trade_id, pos in list(positions.items()):
                try:
                    current_bar = get_current_bar(pos.token_ca, pos.pool_address)
                    if not current_bar:
                        continue
                    bar_ts = int(current_bar['ts'])
                    price = current_bar['close'] or current_bar.get('open')
                    if price is None or price <= 0:
                        continue
                    should_exit, reason, pnl = pos.check_exit(price, bar_ts)
                    lifecycle = lifecycles.setdefault(pos.lifecycle_id, {
                        'lifecycle_id': pos.lifecycle_id,
                        'token_ca': pos.token_ca,
                        'symbol': pos.symbol,
                        'signal_ts': pos.signal_ts,
                        'first_peak_pct': 0.0,
                        'stage1_trade_id': None,
                        'stage2a_trade_id': None,
                        'stage3_trade_id': None,
                        'stage2a_attempted': False,
                        'stage3_attempted': False,
                        'stage1_stop_ts': None,
                        'rolling_low_after_stop': None,
                        'rolling_low_ts': None,
                    })
                    lifecycle['first_peak_pct'] = max(lifecycle.get('first_peak_pct') or 0.0, pos.peak_pnl * 100.0)
                    db.execute("""
                        UPDATE paper_trades
                        SET peak_pnl = ?, trailing_active = ?, bars_held = ?, stage_outcome = ?, first_peak_pct = ?
                        WHERE id = ?
                    """, (pos.peak_pnl, int(pos.trailing_active), pos.bars_held, f"{pos.strategy_stage}_open", lifecycle['first_peak_pct'], pos.trade_id))
                    db.commit()
                    if should_exit:
                        to_close.append((trade_id, reason, pnl, price, bar_ts))
                    time.sleep(0.2)
                except Exception as e:
                    log.error(f"  Position update error for {pos.symbol}: {e}")

            for trade_id, reason, pnl, exit_price, exit_ts in to_close:
                pos = positions.pop(trade_id)
                lifecycle = lifecycles[pos.lifecycle_id]
                regime = determine_market_regime(sol_price) if sol_price else 'unknown'
                stage_outcome = f"{pos.strategy_stage}_{reason}"
                db.execute("""
                    UPDATE paper_trades
                    SET exit_price = ?, exit_ts = ?, exit_reason = ?,
                        pnl_pct = ?, bars_held = ?, market_regime = ?,
                        peak_pnl = ?, trailing_active = ?, stage_outcome = ?, first_peak_pct = ?
                    WHERE id = ?
                """, (
                    exit_price, exit_ts, reason, pnl, pos.bars_held,
                    regime, pos.peak_pnl, int(pos.trailing_active), stage_outcome, lifecycle.get('first_peak_pct') or 0.0, pos.trade_id
                ))
                db.commit()
                if pos.strategy_stage == 'stage1' and reason == 'sl':
                    lifecycle['stage1_stop_ts'] = exit_ts
                    lifecycle['rolling_low_after_stop'] = None
                    lifecycle['rolling_low_ts'] = None
                log.info(f"  CLOSED {pos.symbol}/{pos.strategy_stage}: {reason} pnl={pnl*100:+.1f}% peak={pos.peak_pnl*100:+.1f}% bars={pos.bars_held} lifecycle={pos.lifecycle_id}")

            for lifecycle_id, lifecycle in list(lifecycles.items()):
                if count_open_positions_for_lifecycle(positions, lifecycle_id) > 0:
                    continue
                pool = get_pool_address(lifecycle['token_ca'])
                if not pool:
                    continue
                current_bar = get_current_bar(lifecycle['token_ca'], pool)
                if not current_bar:
                    continue
                bar_ts = int(current_bar['ts'])
                close_price = current_bar['close'] or current_bar.get('open')
                if close_price is None or close_price <= 0:
                    continue

                if stage_rules.get('stage2A', {}).get('enabled') and lifecycle.get('stage1_stop_ts') and not lifecycle.get('stage2a_attempted'):
                    wait_bars = int(stage_rules['stage2A'].get('waitBarsAfterStop', 3))
                    wait_seconds = wait_bars * 60
                    if bar_ts >= int(lifecycle['stage1_stop_ts']) + wait_seconds:
                        rolling_low = lifecycle.get('rolling_low_after_stop')
                        if rolling_low is None or close_price < rolling_low:
                            lifecycle['rolling_low_after_stop'] = close_price
                            lifecycle['rolling_low_ts'] = bar_ts
                        rolling_low = lifecycle.get('rolling_low_after_stop')
                        rebound_target = rolling_low * (1 + pct_to_decimal(stage_rules['stage2A'].get('reboundFromRollingLowPct', 18)))
                        if close_price >= rebound_target:
                            regime = determine_market_regime(sol_price) if sol_price else 'unknown'
                            db.execute("""
                                INSERT INTO paper_trades
                                    (strategy_id, strategy_role, strategy_stage, stage_outcome,
                                     token_ca, symbol, signal_ts, entry_price, entry_ts,
                                     market_regime, replay_source, peak_pnl, trailing_active,
                                     lifecycle_id, parent_trade_id, stage_seq, trigger_ts, trigger_price,
                                     armed_ts, rolling_low_price, rolling_low_ts, reentry_source)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'live_monitor', 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                strategy_id, strategy_role, 'stage2A', 'stage2A_entered',
                                lifecycle['token_ca'], lifecycle['symbol'], lifecycle['signal_ts'], close_price, bar_ts,
                                regime, lifecycle_id, lifecycle.get('stage1_trade_id'), stage_seq('stage2A'), bar_ts, close_price,
                                lifecycle.get('stage1_stop_ts'), lifecycle.get('rolling_low_after_stop'), lifecycle.get('rolling_low_ts'), 'stage1_sl_rebound'
                            ))
                            trade_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
                            db.commit()
                            pos = Position(trade_id, lifecycle['token_ca'], lifecycle['symbol'], lifecycle['signal_ts'], close_price, bar_ts, pool,
                                           'stage2A', lifecycle_id, stage2a_rules)
                            positions[pos.trade_id] = pos
                            lifecycle['stage2a_trade_id'] = trade_id
                            lifecycle['stage2a_attempted'] = True
                            log.info(f"  Entered {lifecycle['symbol']}/stage2A @ ${close_price:.10f} lifecycle={lifecycle_id}")
                            continue

                if stage_rules.get('stage3', {}).get('enabled') and not lifecycle.get('stage3_attempted'):
                    signal_ts_sec = lifecycle['signal_ts'] // 1000 if lifecycle['signal_ts'] > 1e12 else lifecycle['signal_ts']
                    wait_bars = int(stage_rules['stage3'].get('waitBarsFromSignal', 30))
                    first_peak_min = float(stage_rules['stage3'].get('firstPeakMinPct', 10))
                    if bar_ts >= int(signal_ts_sec) + wait_bars * 60 and (lifecycle.get('first_peak_pct') or 0.0) >= first_peak_min:
                        regime = determine_market_regime(sol_price) if sol_price else 'unknown'
                        db.execute("""
                            INSERT INTO paper_trades
                                (strategy_id, strategy_role, strategy_stage, stage_outcome,
                                 token_ca, symbol, signal_ts, entry_price, entry_ts,
                                 market_regime, replay_source, peak_pnl, trailing_active,
                                 lifecycle_id, parent_trade_id, stage_seq, trigger_ts, trigger_price,
                                 reentry_source, first_peak_pct)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'live_monitor', 0, 0, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            strategy_id, strategy_role, 'stage3', 'stage3_entered',
                            lifecycle['token_ca'], lifecycle['symbol'], lifecycle['signal_ts'], close_price, bar_ts,
                            regime, lifecycle_id, lifecycle.get('stage1_trade_id'), stage_seq('stage3'), bar_ts, close_price,
                            'continuation_reentry', lifecycle.get('first_peak_pct') or 0.0
                        ))
                        trade_id = db.execute('SELECT last_insert_rowid()').fetchone()[0]
                        db.commit()
                        pos = Position(trade_id, lifecycle['token_ca'], lifecycle['symbol'], lifecycle['signal_ts'], close_price, bar_ts, pool,
                                       'stage3', lifecycle_id, stage3_rules)
                        positions[pos.trade_id] = pos
                        lifecycle['stage3_trade_id'] = trade_id
                        lifecycle['stage3_attempted'] = True
                        log.info(f"  Entered {lifecycle['symbol']}/stage3 @ ${close_price:.10f} lifecycle={lifecycle_id}")

            if positions:
                log.info(f"  Open positions: {len(positions)}  [{', '.join(f'{p.symbol}/{p.strategy_stage}' for p in positions.values())}]")

        today_str = now_utc.strftime('%Y-%m-%d')
        if now_utc.hour == DAILY_REPORT_HOUR and last_daily_report != today_str:
            yesterday = (now_utc - timedelta(days=1)).strftime('%Y-%m-%d')
            print_daily_report(db, yesterday)
            last_daily_report = today_str

        time.sleep(SIGNAL_POLL_INTERVAL)


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

    if not REMOTE_SIGNAL_URL and not os.path.exists(SENTIMENT_DB):
        log.error(f"Signal DB not found: {SENTIMENT_DB}")
        log.error("Set SENTIMENT_DB or ensure data/sentiment_arb.db exists, or configure REMOTE_SIGNAL_URL.")
        sys.exit(1)

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
        run_monitor(db)

    db.close()


if __name__ == '__main__':
    main()
