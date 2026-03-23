#!/usr/bin/env python3
"""
Paper Trade Monitor — forward-test NOT_ATH signals with simulated execution.

NOT_ATH Strategy:
  - Entry: Super Index >= 80 + RED(+2) + lowVol(+1) + active(+1) >= 3
  - Exit:  SL=-3%, trail@+3%/0.90, timeout=120min

Monitors premium_signals for new entries, enters at live price via GeckoTerminal,
tracks positions with trailing-stop logic, records results to paper_trades.db.

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
REMOTE_SIGNAL_URL = os.environ.get('REMOTE_SIGNAL_URL', '').strip()
REMOTE_SIGNAL_TOKEN = os.environ.get('REMOTE_SIGNAL_TOKEN', '').strip()
REMOTE_SIGNAL_LOOKBACK = max(50, int(os.environ.get('REMOTE_SIGNAL_LOOKBACK', '500')))

# Sim parameters (Canonical v2: SL=-3%, trail@+3%/0.90, timeout=120min)
SL_PCT = -0.03
TRAIL_START = 0.03
TRAIL_FACTOR = 0.90
TIMEOUT_MIN = 120
ALLOW_SYNTHETIC_REPLAY = os.environ.get('ALLOW_SYNTHETIC_REPLAY', 'false').lower() == 'true'

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
    try:
        db.execute("ALTER TABLE paper_trades ADD COLUMN replay_source TEXT DEFAULT 'live_monitor'")
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
        'token_ca', 'symbol', 'signal_ts', 'entry_price', 'entry_ts',
        'pool_address', 'peak_pnl', 'trailing_active', 'bars_held', 'last_bar_ts',
    ]

    def __init__(self, token_ca, symbol, signal_ts, entry_price, entry_ts, pool_address):
        self.token_ca = token_ca
        self.symbol = symbol
        self.signal_ts = signal_ts
        self.entry_price = entry_price
        self.entry_ts = entry_ts
        self.pool_address = pool_address
        self.peak_pnl = 0.0
        self.trailing_active = False
        self.bars_held = 0
        self.last_bar_ts = int(entry_ts)

    def check_exit(self, current_price, bar_ts):
        """Apply exit logic on a specific 1m bar. Returns (should_exit, exit_reason, pnl_pct) or (False, None, None)."""
        if current_price is None or current_price <= 0 or bar_ts is None:
            return False, None, None

        bar_ts = int(bar_ts)
        if bar_ts <= self.last_bar_ts:
            pnl = (current_price - self.entry_price) / self.entry_price
            return False, None, pnl

        self.last_bar_ts = bar_ts
        self.bars_held = max(self.bars_held + 1, int((bar_ts - self.entry_ts) / 60) + 1)
        pnl = (current_price - self.entry_price) / self.entry_price
        self.peak_pnl = max(self.peak_pnl, pnl)

        # Check trailing activation
        if not self.trailing_active and self.peak_pnl >= TRAIL_START:
            self.trailing_active = True
            log.info(f"  [{self.symbol}] Trailing activated at peak={self.peak_pnl*100:+.1f}%")

        # Stop loss (only before trailing is active)
        if not self.trailing_active and pnl <= SL_PCT:
            return True, 'sl', SL_PCT

        # Trailing stop
        if self.trailing_active:
            trail_level = self.peak_pnl * TRAIL_FACTOR
            if pnl <= trail_level:
                exit_pnl = max(pnl, trail_level)
                return True, 'trail', exit_pnl

        # Timeout
        elapsed_min = (bar_ts - self.entry_ts) / 60
        if elapsed_min >= TIMEOUT_MIN:
            return True, 'timeout', pnl

        return False, None, pnl


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
        SELECT pnl_pct, exit_reason, market_regime, replay_source, bars_held
        FROM paper_trades
        WHERE exit_reason IS NOT NULL
          AND date(exit_ts, 'unixepoch') = ?
    """, (date_str,)).fetchall()

    if not rows:
        log.info(f"=== Daily Report {date_str}: No closed trades ===")
        return

    real_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'real_kline_replay']
    synthetic_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'synthetic_replay']
    live_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'live_monitor']

    by_reason = defaultdict(list)
    for r in rows:
        by_reason[r['exit_reason']].append(r['pnl_pct'])

    by_regime = defaultdict(list)
    for r in rows:
        by_regime[r['market_regime'] or 'unknown'].append(r['pnl_pct'])

    log.info(f"{'='*60}")
    log.info(f"  Daily Report: {date_str}")
    log.info(f"{'='*60}")
    print_summary_block('All trades', rows)
    print_summary_block('Live monitor', live_rows)
    print_summary_block('Real K-line replay', real_rows)
    print_summary_block('Synthetic replay', synthetic_rows)
    log.info(f"")
    log.info(f"  By Exit Reason:")
    for reason, ps in sorted(by_reason.items()):
        r_ev = sum(ps) / len(ps)
        log.info(f"    {reason:10s}  n={len(ps):3d}  EV={r_ev*100:+.2f}%")
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
    log.info(f"{'='*60}")


def print_all_stats(db):
    """Print cumulative stats across all dates."""
    rows = db.execute("""
        SELECT pnl_pct, exit_reason, market_regime, replay_source, bars_held,
               date(exit_ts, 'unixepoch') as exit_date
        FROM paper_trades
        WHERE exit_reason IS NOT NULL
        ORDER BY exit_ts
    """).fetchall()

    if not rows:
        log.info("No completed paper trades yet.")
        return

    real_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'real_kline_replay']
    synthetic_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'synthetic_replay']
    live_rows = [r for r in rows if (r['replay_source'] or 'live_monitor') == 'live_monitor']

    log.info(f"{'='*60}")
    log.info(f"  Cumulative Paper Trade Stats")
    log.info(f"{'='*60}")
    print_summary_block('All trades', rows)
    print_summary_block('Live monitor', live_rows)
    print_summary_block('Real K-line replay', real_rows)
    print_summary_block('Synthetic replay', synthetic_rows)

    # By date
    by_date = defaultdict(list)
    for r in rows:
        by_date[r['exit_date']].append(r['pnl_pct'])

    log.info(f"")
    log.info(f"  By Date:")
    for date, ps in sorted(by_date.items()):
        d_ev = sum(ps) / len(ps)
        d_wr = sum(1 for p in ps if p > 0) / len(ps)
        log.info(f"    {date}  n={len(ps):3d}  EV={d_ev*100:+.2f}%  WR={d_wr*100:.1f}%")

    # By regime
    by_regime = defaultdict(list)
    for r in rows:
        by_regime[r['market_regime'] or 'unknown'].append(r['pnl_pct'])

    log.info(f"")
    log.info(f"  By Market Regime:")
    for regime, ps in sorted(by_regime.items()):
        r_ev = sum(ps) / len(ps)
        r_wr = sum(1 for p in ps if p > 0) / len(ps)
        log.info(f"    {regime:10s}  n={len(ps):3d}  EV={r_ev*100:+.2f}%  WR={r_wr*100:.1f}%")

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
    """Simulate paper trading on recent signals using REAL K-line data.

    Fetches historical K-line bars from kline_cache.db and replays price
    action starting from each signal. Falls back to synthetic walk if
    no K-line data available.
    """
    import random

    log.info("=== DRY RUN MODE (REAL K-LINE REPLAY) ===")
    log.info(f"Synthetic fallback: {'ENABLED' if ALLOW_SYNTHETIC_REPLAY else 'DISABLED'}")

    # Open K-line DB
    try:
        kline_db = init_kline_db()
    except Exception as e:
        log.error(f"Failed to open kline_cache.db: {e}")
        return

    # Check K-line DB stats
    kline_count = kline_db.execute("SELECT COUNT(*) FROM kline_1m").fetchone()[0]
    log.info(f"K-line DB: {kline_count:,} bars available")

    signals = get_recent_signals(limit=500)  # Process more signals
    if not signals:
        log.warning("No recent signals found in premium_signals")
        return

    log.info(f"Found {len(signals)} recent signals")

    # Track stats
    real_klines_used = 0
    synthetic_used = 0
    completed = []

    for sig in signals:
        token_ca = sig['token_ca']
        symbol = sig['symbol'] or token_ca[:8]
        signal_ts_ms = sig['timestamp']
        # Convert ms to seconds
        signal_ts = signal_ts_ms // 1000 if signal_ts_ms > 1e12 else signal_ts_ms

        # Try to get real K-line bars
        bars = get_kline_bars(kline_db, token_ca, signal_ts, limit=TIMEOUT_MIN)

        if len(bars) >= 5:
            # REAL K-LINE REPLAY
            real_klines_used += 1
            entry_price = bars[0]['close']
            if entry_price <= 0:
                entry_price = bars[0]['open']
            if entry_price <= 0:
                continue

            peak_pnl = 0.0
            trailing_active = False
            exit_reason = 'timeout'
            exit_pnl = 0.0
            bars_held = 0
            exit_price = entry_price

            for i, bar in enumerate(bars):
                price = bar['close']
                if price <= 0:
                    price = bar.get('open', entry_price)
                if price <= 0:
                    continue

                pnl = (price - entry_price) / entry_price
                peak_pnl = max(peak_pnl, pnl)
                bars_held = i + 1

                # Trail activation
                if not trailing_active and peak_pnl >= TRAIL_START:
                    trailing_active = True

                # Stop loss
                if not trailing_active and pnl <= SL_PCT:
                    exit_reason = 'sl'
                    exit_pnl = SL_PCT
                    exit_price = entry_price * (1 + exit_pnl)
                    break

                # Trailing stop
                if trailing_active:
                    trail_level = peak_pnl * TRAIL_FACTOR
                    if pnl <= trail_level:
                        exit_reason = 'trail'
                        exit_pnl = max(pnl, trail_level)
                        exit_price = entry_price * (1 + exit_pnl)
                        break

                exit_pnl = pnl
                exit_price = price

            exit_ts = int(signal_ts + bars_held * 60)
            regime = 'real_kline'
            replay_source = 'real_kline_replay'

            log.info(
                f"  [{symbol}] REAL  bars={len(bars)}  "
                f"pnl={exit_pnl*100:+.1f}%  peak={peak_pnl*100:+.1f}%  "
                f"reason={exit_reason}  bars_held={bars_held}"
            )
        else:
            # SYNTHETIC FALLBACK (insufficient K-line data)
            if not ALLOW_SYNTHETIC_REPLAY:
                log.info(f"  [{symbol}] insufficient K-line bars ({len(bars)}), skipping synthetic fallback")
                continue

            synthetic_used += 1
            entry_price = random.uniform(0.00001, 0.01)
            entry_ts = int(signal_ts)

            log.info(f"  Signal: {symbol} ({token_ca[:12]}...) entry=${entry_price:.8f}")

            peak_pnl = 0.0
            trailing_active = False
            exit_reason = 'timeout'
            exit_pnl = 0.0
            bars_held = 0

            random.seed(hash(token_ca))  # reproducible per token
            price = entry_price

            for minute in range(1, TIMEOUT_MIN + 1):
                shock = random.gauss(0.002, 0.03)
                price = price * (1 + shock)
                if price <= 0:
                    price = entry_price * 0.01

                pnl = (price - entry_price) / entry_price
                peak_pnl = max(peak_pnl, pnl)
                bars_held = minute

                if not trailing_active and peak_pnl >= TRAIL_START:
                    trailing_active = True

                if not trailing_active and pnl <= SL_PCT:
                    exit_reason = 'sl'
                    exit_pnl = SL_PCT
                    break

                if trailing_active:
                    trail_level = peak_pnl * TRAIL_FACTOR
                    if pnl <= trail_level:
                        exit_reason = 'trail'
                        exit_pnl = max(pnl, trail_level)
                        break

                exit_pnl = pnl

            exit_price = entry_price * (1 + exit_pnl)
            exit_ts = entry_ts + bars_held * 60
            regime = 'synthetic'
            replay_source = 'synthetic_replay'

            log.info(
                f"    -> {exit_reason:7s}  pnl={exit_pnl*100:+.1f}%  "
                f"peak={peak_pnl*100:+.1f}%  bars={bars_held}  [SYNTHETIC]"
            )

        # Write to DB
        db.execute("""
            INSERT INTO paper_trades
                (token_ca, symbol, signal_ts, entry_price, entry_ts,
                 exit_price, exit_ts, exit_reason, pnl_pct, bars_held,
                 market_regime, replay_source, peak_pnl, trailing_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            token_ca, symbol, int(signal_ts_ms), entry_price, int(signal_ts),
            exit_price, exit_ts, exit_reason, exit_pnl, bars_held,
            regime, replay_source, peak_pnl, int(trailing_active),
        ))
        db.commit()

        completed.append(exit_pnl)

    kline_db.close()

    # Summary
    log.info(f"\n=== DRY RUN COMPLETE ===")
    log.info(f"Real K-line replays: {real_klines_used}")
    log.info(f"Synthetic fallbacks: {synthetic_used}")

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

    # Print from DB to verify persistence
    log.info(f"")
    print_all_stats(db)


def get_signal_minute_ts(signal_ts):
    signal_sec = signal_ts // 1000 if signal_ts > 1e12 else signal_ts
    return int(signal_sec // 60 * 60)


# === Live Monitor Loop ===

def run_monitor(db):
    """Main monitoring loop."""
    log.info("=== Paper Trade Monitor Started ===")
    log.info(f"  SL={SL_PCT*100:.0f}%  Trail Start={TRAIL_START*100:.0f}%  "
             f"Trail Factor={TRAIL_FACTOR*100:.0f}%  Timeout={TIMEOUT_MIN}min")
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

    # Track open positions
    positions = {}  # token_ca -> Position
    pending_entries = {}  # token_ca -> pending signal waiting for signal-minute close

    # Restore open positions from DB
    open_rows = db.execute("""
        SELECT token_ca, symbol, signal_ts, entry_price, entry_ts, peak_pnl, trailing_active, bars_held
        FROM paper_trades
        WHERE exit_reason IS NULL
    """).fetchall()
    for r in open_rows:
        pool = get_pool_address(r['token_ca'])
        pos = Position(r['token_ca'], r['symbol'], r['signal_ts'],
                       r['entry_price'], r['entry_ts'], pool)
        pos.peak_pnl = r['peak_pnl'] or 0
        pos.trailing_active = bool(r['trailing_active'])
        pos.bars_held = r['bars_held'] or 0
        pos.last_bar_ts = int(r['entry_ts']) + max((r['bars_held'] or 0) - 1, 0) * 60
        positions[r['token_ca']] = pos
        time.sleep(0.3)

    if positions:
        log.info(f"  Restored {len(positions)} open positions")

    # Find last processed signal ID
    last_id_row = db.execute("""
        SELECT MAX(signal_ts) as max_ts FROM paper_trades
    """).fetchone()
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

        # === 1. Check for new signals ===
        try:
            new_signals = get_new_signals(last_signal_id)
            for sig in new_signals:
                token_ca = sig['token_ca']
                last_signal_id = sig['id']

                # Skip if already tracking
                if token_ca in positions or token_ca in pending_entries:
                    continue

                # Skip if already traded
                existing = db.execute(
                    "SELECT id FROM paper_trades WHERE token_ca = ? AND signal_ts = ?",
                    (token_ca, sig['timestamp'])
                ).fetchone()
                if existing:
                    continue

                symbol = sig['symbol'] or token_ca[:8]
                log.info(f"New signal: {symbol} ({token_ca[:12]}...)")

                # Parse Super Index from description, skip if < 80
                super_idx = parse_super_index(sig['description'] or '')
                if super_idx is None:
                    log.info(f"  Super Index not found in description, skipping")
                    continue
                if super_idx < 80:
                    log.info(f"  Super Index={super_idx}<80, skipping")
                    continue

                # Score on recent bars, but stage entry until signal-minute close is available
                pool = get_pool_address(token_ca)
                if not pool:
                    log.warning(f"  Could not find pool for {symbol}, skipping")
                    continue
                time.sleep(0.4)

                bars = get_notath_bars(pool, limit=6)
                if not bars or len(bars) < 4:
                    log.warning(f"  Not enough K-line bars for {symbol}, skipping")
                    continue

                score_result = compute_notath_score(bars[:4])
                if not score_result['passed']:
                    log.info(f"  NOT_ATH score={score_result['score']} < 3 (RED={score_result['is_red']}, lowVol={score_result['low_volume']}, active={score_result['is_active']}), skipping")
                    continue

                signal_ts = sig['timestamp']
                signal_minute_ts = get_signal_minute_ts(signal_ts)
                pending_entries[token_ca] = {
                    'token_ca': token_ca,
                    'symbol': symbol,
                    'signal_ts': signal_ts,
                    'signal_minute_ts': signal_minute_ts,
                    'pool': pool,
                    'score_result': score_result,
                }
                log.info(
                    f"  NOT_ATH score={score_result['score']} ✅ (RED={score_result['is_red']}, "
                    f"lowVol={score_result['low_volume']}, active={score_result['is_active']}, "
                    f"mom={score_result['mom']:+.1f}%) | staged for signal-minute close @ {signal_minute_ts}"
                )

                time.sleep(0.5)  # rate limit between signals

        except Exception as e:
            log.error(f"Signal check error: {e}")

        # === 2. Convert staged signals into entries at signal-minute close ===
        if pending_entries:
            for token_ca, pending in list(pending_entries.items()):
                try:
                    bars = get_notath_bars(pending['pool'], limit=8)
                    if not bars:
                        continue

                    signal_bar = next((b for b in bars if int(b['ts']) == pending['signal_minute_ts']), None)
                    if signal_bar is None:
                        continue

                    price = signal_bar['close']
                    if not price or price <= 0:
                        log.warning(f"  Could not get signal-minute close for {pending['symbol']}, skipping staged entry")
                        pending_entries.pop(token_ca, None)
                        continue

                    entry_ts = int(signal_bar['ts'])
                    signal_ts = pending['signal_ts']
                    if sol_price is None:
                        sol_price = get_sol_price()
                        time.sleep(0.3)
                    regime = determine_market_regime(sol_price) if sol_price else 'unknown'

                    db.execute("""
                        INSERT INTO paper_trades
                            (token_ca, symbol, signal_ts, entry_price, entry_ts,
                             market_regime, replay_source, peak_pnl, trailing_active)
                        VALUES (?, ?, ?, ?, ?, ?, 'live_monitor', 0, 0)
                    """, (token_ca, pending['symbol'], signal_ts, price, entry_ts, regime))
                    db.commit()

                    pos = Position(token_ca, pending['symbol'], signal_ts, price, entry_ts, pending['pool'])
                    pos.last_bar_ts = entry_ts
                    positions[token_ca] = pos
                    pending_entries.pop(token_ca, None)
                    delay_min = (entry_ts - (signal_ts // 1000 if signal_ts > 1e12 else signal_ts)) / 60
                    log.info(
                        f"  Entered {pending['symbol']} @ ${price:.10f}  regime={regime}  "
                        f"signal_bar_ts={entry_ts}  delay={delay_min:+.1f}min"
                    )
                    time.sleep(0.4)
                except Exception as e:
                    log.error(f"  Pending entry error for {pending['symbol']}: {e}")

        # === 3. Update open positions ===
        if now - last_position_check >= POSITION_POLL_INTERVAL and positions:
            last_position_check = now
            to_close = []

            # Refresh SOL price for regime
            try:
                new_sol = get_sol_price()
                if new_sol:
                    sol_price = new_sol
                time.sleep(0.3)
            except Exception:
                pass

            for token_ca, pos in list(positions.items()):
                try:
                    current_bar = get_current_bar(token_ca, pos.pool_address)
                    if not current_bar:
                        log.debug(f"  Current bar fetch failed for {pos.symbol}, skipping update")
                        continue

                    bar_ts = int(current_bar['ts'])
                    price = current_bar['close']
                    if not price or price <= 0:
                        price = current_bar.get('open')
                    if price is None or price <= 0:
                        log.debug(f"  Invalid current bar for {pos.symbol}, skipping update")
                        continue

                    should_exit, reason, pnl = pos.check_exit(price, bar_ts)

                    # Update peak/trailing in DB
                    db.execute("""
                        UPDATE paper_trades
                        SET peak_pnl = ?, trailing_active = ?, bars_held = ?
                        WHERE token_ca = ? AND exit_reason IS NULL
                    """, (pos.peak_pnl, int(pos.trailing_active), pos.bars_held,
                          token_ca))
                    db.commit()

                    if should_exit:
                        to_close.append((token_ca, reason, pnl, price, bar_ts))

                    time.sleep(0.4)  # rate limit

                except Exception as e:
                    log.error(f"  Position update error for {pos.symbol}: {e}")

            # Close positions
            for token_ca, reason, pnl, exit_price, exit_ts in to_close:
                pos = positions.pop(token_ca)

                regime = determine_market_regime(sol_price) if sol_price else 'unknown'

                db.execute("""
                    UPDATE paper_trades
                    SET exit_price = ?, exit_ts = ?, exit_reason = ?,
                        pnl_pct = ?, bars_held = ?, market_regime = ?,
                        peak_pnl = ?, trailing_active = ?
                    WHERE token_ca = ? AND exit_reason IS NULL
                """, (
                    exit_price, exit_ts, reason, pnl, pos.bars_held,
                    regime, pos.peak_pnl, int(pos.trailing_active), token_ca
                ))
                db.commit()

                log.info(
                    f"  CLOSED {pos.symbol}: {reason}  pnl={pnl*100:+.1f}%  "
                    f"peak={pos.peak_pnl*100:+.1f}%  bars={pos.bars_held}  regime={regime}  exit_bar_ts={exit_ts}"
                )

            if positions:
                log.info(f"  Open positions: {len(positions)}  "
                         f"[{', '.join(p.symbol for p in positions.values())}]")

        # === 3. Daily report ===
        today_str = now_utc.strftime('%Y-%m-%d')
        if now_utc.hour == DAILY_REPORT_HOUR and last_daily_report != today_str:
            yesterday = (now_utc - timedelta(days=1)).strftime('%Y-%m-%d')
            print_daily_report(db, yesterday)
            last_daily_report = today_str

        # === 4. Sleep ===
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
