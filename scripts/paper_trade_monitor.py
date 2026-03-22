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
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

# === Configuration ===
SENTIMENT_DB = os.environ.get('SENTIMENT_DB', '/tmp/sentiment.db')
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
PAPER_DB = os.environ.get('PAPER_DB', str(DATA_DIR / 'paper_trades.db'))
KLINE_DB = os.environ.get('KLINE_DB', str(DATA_DIR / 'kline_cache.db'))

# Sim parameters (Canonical v2: SL=-3%, trail@+3%/0.90, timeout=120min)
SL_PCT = -0.03
TRAIL_START = 0.03
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
            peak_pnl REAL DEFAULT 0,
            trailing_active INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_pt_token ON paper_trades(token_ca)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_pt_exit ON paper_trades(exit_reason)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_pt_entry_ts ON paper_trades(entry_ts)")
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


def get_current_price(token_ca, pool_address=None):
    """Get latest price from GeckoTerminal (latest 1m candle close)."""
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

    # ohlcv_list: [[timestamp, open, high, low, close, volume], ...]
    return ohlcv[0][4]  # close price


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


def get_notath_bars(pool_address):
    """
    Get 5 most recent 1-minute bars for NOT_ATH scoring.
    Returns list of bars (newest first), or None.
    Each bar: {ts, open, high, low, close, volume}
    """
    url = (
        f"https://api.geckoterminal.com/api/v2/networks/solana/pools/"
        f"{pool_address}/ohlcv/minute?aggregate=1&limit=5"
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

def get_new_signals(last_signal_id):
    """Query premium_signals for signals newer than last_signal_id."""
    try:
        sdb = sqlite3.connect(SENTIMENT_DB)
        sdb.row_factory = sqlite3.Row
        rows = sdb.execute("""
            SELECT id, token_ca, symbol, timestamp, description, hard_gate_status
            FROM premium_signals
            WHERE id > ?
              AND hard_gate_status LIKE 'NOT_ATH%'
            ORDER BY id ASC
        """, (last_signal_id,)).fetchall()
        sdb.close()
        return rows
    except Exception as e:
        log.warning(f"Failed to query signals: {e}")
        return []


def get_recent_signals(limit=20):
    """Get most recent signals for dry-run mode."""
    try:
        sdb = sqlite3.connect(SENTIMENT_DB)
        sdb.row_factory = sqlite3.Row
        rows = sdb.execute("""
            SELECT id, token_ca, symbol, timestamp, description, hard_gate_status
            FROM premium_signals
            WHERE hard_gate_status LIKE 'NOT_ATH%'
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()
        sdb.close()
        return list(reversed(rows))  # oldest first
    except Exception as e:
        log.warning(f"Failed to query signals: {e}")
        return []


def get_last_processed_id(db):
    """Get the highest signal_ts in paper_trades to avoid re-processing."""
    row = db.execute("SELECT MAX(signal_ts) as max_ts FROM paper_trades").fetchone()
    return row['max_ts'] or 0


# === Position Tracking ===

class Position:
    """Tracks an open paper trade position."""
    __slots__ = [
        'token_ca', 'symbol', 'signal_ts', 'entry_price', 'entry_ts',
        'pool_address', 'peak_pnl', 'trailing_active', 'bars_held',
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

    def check_exit(self, current_price):
        """Apply exit logic. Returns (should_exit, exit_reason, pnl_pct) or (False, None, None)."""
        if current_price is None or current_price <= 0:
            return False, None, None

        self.bars_held += 1
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
        elapsed_min = (int(time.time()) - self.entry_ts) / 60
        if elapsed_min >= TIMEOUT_MIN:
            return True, 'timeout', pnl

        return False, None, pnl


# === Daily Report ===

def print_daily_report(db, date_str=None):
    """Print daily statistics."""
    if not date_str:
        date_str = datetime.utcnow().strftime('%Y-%m-%d')

    # Get trades closed today
    rows = db.execute("""
        SELECT pnl_pct, exit_reason, market_regime, bars_held
        FROM paper_trades
        WHERE exit_reason IS NOT NULL
          AND date(exit_ts, 'unixepoch') = ?
    """, (date_str,)).fetchall()

    if not rows:
        log.info(f"=== Daily Report {date_str}: No closed trades ===")
        return

    pnls = [r['pnl_pct'] for r in rows]
    n = len(pnls)
    ev = sum(pnls) / n
    wins = sum(1 for p in pnls if p > 0)
    wr = wins / n
    std = (sum((p - ev) ** 2 for p in pnls) / n) ** 0.5
    sharpe = ev / std if std > 0 else 0

    # By exit reason
    by_reason = defaultdict(list)
    for r in rows:
        by_reason[r['exit_reason']].append(r['pnl_pct'])

    # By regime
    by_regime = defaultdict(list)
    for r in rows:
        by_regime[r['market_regime'] or 'unknown'].append(r['pnl_pct'])

    log.info(f"{'='*60}")
    log.info(f"  Daily Report: {date_str}")
    log.info(f"{'='*60}")
    log.info(f"  Trades: {n}  |  EV: {ev*100:+.2f}%  |  WR: {wr*100:.1f}%  |  Sharpe: {sharpe:.3f}")
    log.info(f"  Total PnL: {sum(pnls)*100:+.2f}%")
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
    log.info(f"{'='*60}")


def print_all_stats(db):
    """Print cumulative stats across all dates."""
    rows = db.execute("""
        SELECT pnl_pct, exit_reason, market_regime, bars_held,
               date(exit_ts, 'unixepoch') as exit_date
        FROM paper_trades
        WHERE exit_reason IS NOT NULL
        ORDER BY exit_ts
    """).fetchall()

    if not rows:
        log.info("No completed paper trades yet.")
        return

    pnls = [r['pnl_pct'] for r in rows]
    n = len(pnls)
    ev = sum(pnls) / n
    wins = sum(1 for p in pnls if p > 0)
    wr = wins / n
    std = (sum((p - ev) ** 2 for p in pnls) / n) ** 0.5
    sharpe = ev / std if std > 0 else 0

    log.info(f"{'='*60}")
    log.info(f"  Cumulative Paper Trade Stats")
    log.info(f"{'='*60}")
    log.info(f"  Trades: {n}  |  EV: {ev*100:+.2f}%  |  WR: {wr*100:.1f}%  |  Sharpe: {sharpe:.3f}")
    log.info(f"  Total PnL: {sum(pnls)*100:+.2f}%")

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

            log.info(
                f"  [{symbol}] REAL  bars={len(bars)}  "
                f"pnl={exit_pnl*100:+.1f}%  peak={peak_pnl*100:+.1f}%  "
                f"reason={exit_reason}  bars_held={bars_held}"
            )
        else:
            # SYNTHETIC FALLBACK (insufficient K-line data)
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

            log.info(
                f"    -> {exit_reason:7s}  pnl={exit_pnl*100:+.1f}%  "
                f"peak={peak_pnl*100:+.1f}%  bars={bars_held}  [SYNTHETIC]"
            )

        # Write to DB
        db.execute("""
            INSERT INTO paper_trades
                (token_ca, symbol, signal_ts, entry_price, entry_ts,
                 exit_price, exit_ts, exit_reason, pnl_pct, bars_held,
                 market_regime, peak_pnl, trailing_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            token_ca, symbol, int(signal_ts_ms), entry_price, int(signal_ts),
            exit_price, exit_ts, exit_reason, exit_pnl, bars_held,
            regime, peak_pnl, int(trailing_active),
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


# === Live Monitor Loop ===

def run_monitor(db):
    """Main monitoring loop."""
    log.info("=== Paper Trade Monitor Started ===")
    log.info(f"  SL={SL_PCT*100:.0f}%  Trail Start={TRAIL_START*100:.0f}%  "
             f"Trail Factor={TRAIL_FACTOR*100:.0f}%  Timeout={TIMEOUT_MIN}min")
    log.info(f"  Signal poll: {SIGNAL_POLL_INTERVAL}s  Position poll: {POSITION_POLL_INTERVAL}s")
    log.info(f"  DB: {PAPER_DB}")

    # Track open positions
    positions = {}  # token_ca -> Position

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
        positions[r['token_ca']] = pos
        time.sleep(0.3)

    if positions:
        log.info(f"  Restored {len(positions)} open positions")

    # Find last processed signal ID
    last_id_row = db.execute("""
        SELECT MAX(signal_ts) as max_ts FROM paper_trades
    """).fetchone()
    # Get signal ID from sentiment DB
    last_signal_id = 0
    if last_id_row and last_id_row['max_ts']:
        try:
            sdb = sqlite3.connect(SENTIMENT_DB)
            row = sdb.execute(
                "SELECT MAX(id) as mid FROM premium_signals WHERE timestamp <= ?",
                (last_id_row['max_ts'],)
            ).fetchone()
            if row and row[0]:
                last_signal_id = row[0]
            sdb.close()
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
                if token_ca in positions:
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
                super_idx = parse_super_index(sig.get('description', ''))
                if super_idx is None:
                    log.info(f"  Super Index not found in description, skipping")
                    continue
                if super_idx < 80:
                    log.info(f"  Super Index={super_idx}<80, skipping")
                    continue

                # Get 4 bars for NOT_ATH scoring
                pool = get_pool_address(token_ca)
                if not pool:
                    log.warning(f"  Could not find pool for {symbol}, skipping")
                    continue
                time.sleep(0.4)

                bars = get_notath_bars(pool)
                if not bars or len(bars) < 4:
                    log.warning(f"  Not enough K-line bars for {symbol}, skipping")
                    continue

                # Compute NOT_ATH score
                score_result = compute_notath_score(bars)
                if not score_result['passed']:
                    log.info(f"  NOT_ATH score={score_result['score']} < 3 (RED={score_result['is_red']}, lowVol={score_result['low_volume']}, active={score_result['is_active']}), skipping")
                    continue

                log.info(f"  NOT_ATH score={score_result['score']} ✅ (RED={score_result['is_red']}, lowVol={score_result['low_volume']}, active={score_result['is_active']}, mom={score_result['mom']:+.1f}%)")

                # Enter at close of current bar
                price = bars[0]['close']
                if not price or price <= 0:
                    log.warning(f"  Could not get price for {symbol}, skipping")
                    continue

                entry_ts = int(time.time())
                signal_ts = sig['timestamp']

                # Determine regime
                if sol_price is None:
                    sol_price = get_sol_price()
                    time.sleep(0.3)
                regime = determine_market_regime(sol_price) if sol_price else 'unknown'

                # Insert open position
                db.execute("""
                    INSERT INTO paper_trades
                        (token_ca, symbol, signal_ts, entry_price, entry_ts,
                         market_regime, peak_pnl, trailing_active)
                    VALUES (?, ?, ?, ?, ?, ?, 0, 0)
                """, (token_ca, symbol, signal_ts, price, entry_ts, regime))
                db.commit()

                pos = Position(token_ca, symbol, signal_ts, price, entry_ts, pool)
                positions[token_ca] = pos
                log.info(f"  Entered {symbol} @ ${price:.10f}  regime={regime}")

                time.sleep(0.5)  # rate limit between signals

        except Exception as e:
            log.error(f"Signal check error: {e}")

        # === 2. Update open positions ===
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
                    price = get_current_price(token_ca, pos.pool_address)
                    if price is None:
                        log.debug(f"  Price fetch failed for {pos.symbol}, skipping update")
                        # Still check timeout
                        elapsed = (int(time.time()) - pos.entry_ts) / 60
                        if elapsed >= TIMEOUT_MIN:
                            # Force timeout exit using entry price (conservative)
                            to_close.append((token_ca, 'timeout', 0.0, pos.entry_price))
                        continue

                    should_exit, reason, pnl = pos.check_exit(price)

                    # Update peak/trailing in DB
                    db.execute("""
                        UPDATE paper_trades
                        SET peak_pnl = ?, trailing_active = ?, bars_held = ?
                        WHERE token_ca = ? AND exit_reason IS NULL
                    """, (pos.peak_pnl, int(pos.trailing_active), pos.bars_held,
                          token_ca))
                    db.commit()

                    if should_exit:
                        to_close.append((token_ca, reason, pnl, price))

                    time.sleep(0.4)  # rate limit

                except Exception as e:
                    log.error(f"  Position update error for {pos.symbol}: {e}")

            # Close positions
            for token_ca, reason, pnl, exit_price in to_close:
                pos = positions.pop(token_ca)
                exit_ts = int(time.time())

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
                    f"peak={pos.peak_pnl*100:+.1f}%  bars={pos.bars_held}  regime={regime}"
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
