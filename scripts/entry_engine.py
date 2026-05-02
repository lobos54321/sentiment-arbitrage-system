#!/usr/bin/env python3
"""
Entry Engine — SmartEntry + Kelly Position Sizing

Extracted from paper_trade_monitor.py for modularity.
All buy-side decision logic lives here.
"""

import os
import time
import logging

from watchlist_store import WatchlistStore
from entry_readiness_policy import (
    ENTRY_READINESS_MAX_WAIT_SEC,
    ENTRY_READINESS_POLL_SEC,
    entry_mode_allowed,
)

log = logging.getLogger('paper_trade_monitor')

# ─── Kelly Criterion Constants ────────────────────────────────────────────────
KELLY_BASE_CAPITAL_SOL = float(os.environ.get('KELLY_BASE_CAPITAL_SOL', '5.0'))
KELLY_BASE_WIN_RATE    = 0.50   # Adjusted: system targets ~50% win rate (was 0.45 conservative)
KELLY_BASE_ODDS        = 5.0   # ~150% win / ~15% loss (ONLY used as fallback)
KELLY_COLD_START_ODDS  = 2.0   # Based on overnight data: avg_win=16.3% / avg_loss=8%
MAX_POSITION_SOL       = 0.5   # Hard cap: protect against Kelly outliers (was uncapped → 1.0 SOL)

# Data cleanup: entry_price phantom-baseline bug was fixed at commit cce57b6
# (2026-04-18 14:33:36 +1000 = 2026-04-18 04:33:36 UTC = unix ts 1776486816).
# All trades closed before this timestamp have inflated loss rates because
# ExitGuardian used trigger_price as baseline, causing in-flight PnL to show
# artificial -X% losses the moment a trade opened. Kelly reading these poisoned
# trades gave ~28% win rate against true rate ~45-50%. We exclude them.
# Until min_trades (20) clean trades accumulate, cold start applies (b=2.0).
KELLY_CLEAN_DATA_FROM_TS = 1776486816  # 2026-04-18 04:33:36 UTC

# P3: Historical odds cache
_kelly_trade_cache = {'wins': [], 'losses': [], 'last_refresh': 0}

# A2: Adaptive stop-loss cache (refreshed every 5 minutes)
_adaptive_sl_cache = {'sl': -0.075, 'last_refresh': 0}

def get_adaptive_stop_loss():
    """A2: Stop-loss — reverted to fixed -15% matching 84% win rate period.
    
    History:
    - 84% period (c6121922): Fixed -15% → gave trades enough breathing room
    - Adaptive period: -5% to -12% based on avg peak → killed trades during
      normal meme coin volatility (±5-8% swings are noise)
    - Audit (26 trades, 2026-04-22): 22/26 losses triggered at -7% to -10%.
      Many had peaks of +3-4% before reverting — with -15% SL they might have
      survived and recovered.
    
    Returns: float (-0.15)
    """
    return -0.15

# ─── SmartEntry Constants ─────────────────────────────────────────────────────
_dex_trend_cache = {}
SMART_ENTRY_DEX_CACHE_SEC = 15       # Reuse DexScreener data for 15 seconds
SMART_ENTRY_POLL_INTERVAL_SEC = 10   # Poll price every 10 seconds
SMART_ENTRY_MAX_WAIT_SEC = 900       # 15-minute maximum wait
SMART_ENTRY_MIN_PULLBACK_PCT = 2.0   # Minimum pullback depth to qualify
SMART_ENTRY_MIN_BOUNCE_PCT = 2.0     # Minimum bounce from low to confirm
SMART_ENTRY_MIN_BOUNCE_RATIO = 0.30  # bounce/pullback default (data: 25% let through Goose -14.8%)
SMART_ENTRY_BOUNCE_RATIO_STRONG = 0.15   # strong signal: bs>=1.5 + vol>=2.0 + real_buying + below_high<10%
SMART_ENTRY_BOUNCE_RATIO_MEDIUM = 0.20   # medium signal: bs>=1.3 + vol>=1.5
SMART_ENTRY_MIN_VOL_RATIO = 1.5      # vol_ratio floor — lowered from 2.0 to 1.5 to resolve G1b/tier conflict
                                      # (2.0 blocked medium tier vol>=1.5 range, making medium tier dead code)
SMART_ENTRY_REENTRY_VOL_RATIO = 1.5  # same as MIN_VOL_RATIO
SMART_ENTRY_MIN_POINTS = 8            # Raised from 6 → 8 (data: n_points≤10 was 0W/4L, PONYROID n=6 → -18.7%)
SMART_ENTRY_MAX_BELOW_HIGH_PCT = 15.0 # Dead cat bounce filter: below_high > 15% → reject
                                      # Data: 24+ samples, below_high>15% = zero win rate
                                      # Originally commit b9bb618, reverted 8102bc8. Now restored with stronger evidence.
SMART_ENTRY_RISKY_MAX_BELOW_HIGH_PCT = float(os.environ.get("SMART_ENTRY_RISKY_MAX_BELOW_HIGH_PCT", "10.0"))
SMART_ENTRY_FAKE_PUMP_THRESHOLD = 10  # After N fake_pump rounds, require stricter entry (buy_sell>=2.0)
SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_M5_PCT = float(os.environ.get("SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_M5_PCT", "300"))
SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_VOL_M5_USD = float(os.environ.get("SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_VOL_M5_USD", "20000"))
SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_M5_TXNS = int(os.environ.get("SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_M5_TXNS", "400"))
SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_BS_RATIO = float(os.environ.get("SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_BS_RATIO", "1.05"))


def _calc_velocity(price_history, window_sec):
    """Calculate price velocity (%/min) from Jupiter real-time price history.

    Uses the same approach as Guardian's velocity calculation.
    Returns 0.0 if insufficient data.
    """
    now = time.time()
    pts = [(t, p) for t, p in price_history if now - t <= window_sec and p > 0]
    if len(pts) < 2:
        return 0.0
    dt_min = (pts[-1][0] - pts[0][0]) / 60.0
    if dt_min <= 0.01:  # avoid division by near-zero
        return 0.0
    return ((pts[-1][1] - pts[0][1]) / pts[0][1] * 100) / dt_min

def clear_dex_trend_cache():
    """Purges the in-memory DexScreener trend cache to prevent memory leaks."""
    _dex_trend_cache.clear()

# ─── Kelly ────────────────────────────────────────────────────────────────────

def _get_historical_odds(min_trades=20, default_b=None):
    """P3: Calculate b = avg_win/avg_loss from recent trade history.
    Uses rolling 50-trade window. Falls back to KELLY_COLD_START_ODDS if insufficient data."""
    if default_b is None:
        default_b = KELLY_COLD_START_ODDS
    cache = _kelly_trade_cache
    now = time.time()

    # Refresh from DB every 5 minutes
    if now - cache['last_refresh'] > 300:
        try:
            store = WatchlistStore()
            trades = store.get_recent_closed_trades(limit=50)
            # Filter: exclude trades closed before the entry_price phantom-baseline
            # bug fix. Those trades have inflated loss rates (Guardian calculated
            # PnL against trigger_price, causing fake instant -X% on open).
            # Reading them poisoned the win rate. See KELLY_CLEAN_DATA_FROM_TS.
            clean_trades = [
                t for t in trades
                if (t.get('last_exit_at') or 0) >= KELLY_CLEAN_DATA_FROM_TS
            ]
            dirty_count = len(trades) - len(clean_trades)
            cache['wins'] = [t['exit_pnl'] for t in clean_trades if t['exit_pnl'] and t['exit_pnl'] > 0]
            cache['losses'] = [abs(t['exit_pnl']) for t in clean_trades if t['exit_pnl'] and t['exit_pnl'] < 0]
            cache['last_refresh'] = now
            log.info(
                f"[Kelly] Historical data refreshed: {len(cache['wins'])} wins, "
                f"{len(cache['losses'])} losses from {len(clean_trades)} clean trades "
                f"(excluded {dirty_count} pre-fix contaminated trades)"
            )
        except Exception as e:
            log.warning(f"[Kelly] Failed to refresh historical data: {e}")
            cache['last_refresh'] = now  # avoid hammering on error

    total = len(cache['wins']) + len(cache['losses'])
    if total < min_trades:
        log.info(f"[Kelly] Cold start: only {total} trades, using default b={default_b}")
        return default_b

    # Use last 20 trades for rolling average
    recent_wins = cache['wins'][-20:] if cache['wins'] else []
    recent_losses = cache['losses'][-20:] if cache['losses'] else []

    avg_win = sum(recent_wins) / max(len(recent_wins), 1) if recent_wins else 0
    avg_loss = sum(recent_losses) / max(len(recent_losses), 1) if recent_losses else 0

    if avg_loss <= 0:
        return default_b

    b_real = max(avg_win / avg_loss, 0.1)  # floor at 0.1
    b_real = min(b_real, 3.0)  # cap at 3.0 — higher caps destroy Kelly's signal differentiation (90% hit position cap)
    log.info(
        f"[Kelly] Historical odds: avg_win={avg_win*100:.1f}% avg_loss={avg_loss*100:.1f}% "
        f"b_real={b_real:.3f} (from {total} trades, capped at 3.0)"
    )
    return b_real


def calculate_kelly_position(watchlist_entry, base_capital=None, description=None, matrix_scores=None,
                              entry_mode=None):
    """
    V8: Fixed position sizing — Kelly DISABLED.

    Reason: Historical data is poisoned (11W/39L from buggy V3-V7 period).
    Kelly computes base_f*=-0.10 (negative EV) → always returns minimum 0.1 SOL anyway.
    Fixed mode removes the DB query overhead and makes the behavior explicit.

    Re-enable Kelly once V8 accumulates 30+ clean trades with positive win rate.
    The full Kelly logic is preserved in git history (commit 609c4523).

    Returns: 0.1 SOL (fixed)
    """
    FIXED_POSITION_SOL = 0.1
    log.info(f"[Kelly] FIXED MODE: {FIXED_POSITION_SOL} SOL (Kelly disabled until V8 proves win rate)")
    return FIXED_POSITION_SOL


def get_liquidity_position_cap(token_ca, sol_price_usd, max_pool_pct=0.01):
    """A1: Compute max position size in SOL based on pool liquidity.
    Cap = pool_liquidity_usd * max_pool_pct / sol_price_usd
    This prevents taking positions that are too large relative to the AMM pool,
    which causes excessive slippage on both entry and especially exit.
    Returns None if liquidity data unavailable (no cap applied).
    """
    if not sol_price_usd or sol_price_usd <= 0:
        return None
    try:
        snap = fetch_dexscreener_trend_snapshot(token_ca)
        if not snap:
            return None
        liquidity_usd = snap.get('liquidity_usd', 0) or 0
        if liquidity_usd <= 0:
            return None
        cap = (liquidity_usd * max_pool_pct) / sol_price_usd
        # Floor at 0.03 SOL (don't block tiny test trades), cap display at 0.5 (hard cap handles upper bound)
        cap = max(0.03, cap)
        log.info(f"[Liquidity] pool=${liquidity_usd:,.0f} → {max_pool_pct*100:.0f}% cap={cap:.3f} SOL (sol=${sol_price_usd:.0f})")
        return cap
    except Exception as e:
        log.debug(f"[Liquidity] cap calc failed: {e}")
        return None


# Module-level cache for GeckoTerminal K-line bars (shared by EMA and bar functions)
_gt_bars_cache = {}  # {token_ca: (bars_list, fetch_timestamp)}
_GT_BARS_CACHE_TTL = 30  # seconds


def _fetch_gt_bars_cached(token_ca, pool_address, limit=20):
    """Fetch GeckoTerminal 1m K-lines with 30s in-memory cache.
    Returns list of bar dicts or None.
    """
    import time as _time
    cached = _gt_bars_cache.get(token_ca)
    if cached and _time.time() - cached[1] < _GT_BARS_CACHE_TTL:
        return cached[0]
    
    try:
        from paper_trade_monitor import get_notath_bars
        gt_bars = get_notath_bars(pool_address, limit=limit)
        if gt_bars:
            _gt_bars_cache[token_ca] = (gt_bars, _time.time())
            return gt_bars
    except Exception:
        pass
    return None


def calculate_ema_deviation(token_ca, current_price, pool_address=None):
    """Calculate price deviation from 20-period EMA.
    Primary: GeckoTerminal real 1m K-lines (accurate OHLC from exchange).
    Fallback: in-memory price history (synthetic, lower resolution).
    
    Returns: (deviation_pct: float, ema_price: float) or (None, None) if insufficient data.
    Deviation > 0 means price is ABOVE EMA (overextended upward).
    """
    prices = []
    
    # Primary: GeckoTerminal real K-lines (30s cached)
    if pool_address:
        gt_bars = _fetch_gt_bars_cached(token_ca, pool_address, limit=20)
        if gt_bars and len(gt_bars) >= 10:
            sorted_gt = sorted(gt_bars, key=lambda b: b['ts'])
            prices = [float(b['close']) for b in sorted_gt]
    
    # Fallback: synthetic from _price_history
    if not prices:
        from matrix_evaluator import MatrixEvaluator
        history = MatrixEvaluator._price_history.get(token_ca, [])
        if len(history) >= 10:
            prices = [p for _, p in history[-20:]]
            
    if len(prices) < 10:
        return None, None
    
    # Calculate EMA with period = len(prices)
    k = 2.0 / (len(prices) + 1)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    
    if ema <= 0:
        return None, None
    
    deviation_pct = ((current_price - ema) / ema) * 100
    return deviation_pct, ema

def get_recent_synthetic_bars(token_ca, n_bars=5, pool_address=None, native_only=False):
    """Get recent 1-minute OHLC bars.
    Primary: GeckoTerminal real 1m K-lines (accurate OHLC from exchange).
    Fallback: synthetic bars built from in-memory price history.
    
    Args:
        native_only: If True, skip GeckoTerminal and use only _price_history.
                     Use this when comparing with SOL-native entry/current prices
                     (GT returns USD-denominated prices which can't be compared directly).
    
    Returns: list of {'open', 'high', 'low', 'close', 'ts'} (newest last)
    or empty list if insufficient data.
    """
    # Primary: GeckoTerminal real K-lines (30s cached)
    # SKIP when native_only=True — GT prices are in USD, not SOL-native
    if pool_address and not native_only:
        gt_bars = _fetch_gt_bars_cached(token_ca, pool_address, limit=max(n_bars, 5))
        if gt_bars and len(gt_bars) >= 2:
            sorted_gt = sorted(gt_bars, key=lambda b: b['ts'])
            return sorted_gt[-n_bars:] if len(sorted_gt) >= n_bars else sorted_gt

    # Fallback (or native_only): synthetic bars from _price_history (SOL-native)
    from matrix_evaluator import MatrixEvaluator
    history = MatrixEvaluator._price_history.get(token_ca, [])
    if len(history) < 3:
        return []
    
    # Group by minute
    import time as _time_mod
    bars = {}
    for ts, px in history:
        minute_key = int(ts // 60) * 60
        if minute_key not in bars:
            bars[minute_key] = {'ts': minute_key, 'open': px, 'high': px, 'low': px, 'close': px}
        else:
            bars[minute_key]['high'] = max(bars[minute_key]['high'], px)
            bars[minute_key]['low'] = min(bars[minute_key]['low'], px)
            bars[minute_key]['close'] = px
    
    # Mark each bar as 'complete' (closed) or 'partial' (still in progress).
    # Root cause fix for KLINE_FLAT: the current-minute bar is always partial
    # and usually has open==close (+0.0%), making trend confirmation useless.
    # 8hr audit: 21/22 trades used partial bars → all showed +0.0% KLINE_OK.
    current_minute_key = int(_time_mod.time() // 60) * 60
    for mk, bar in bars.items():
        bar['complete'] = (mk < current_minute_key)
    
    sorted_bars = sorted(bars.values(), key=lambda b: b['ts'])
    return sorted_bars[-n_bars:] if len(sorted_bars) >= n_bars else sorted_bars


# ─── SmartEntry ───────────────────────────────────────────────────────────────

def fetch_dexscreener_trend_snapshot(token_ca, timeout=5):
    """Fetch m5+h1 volume/txns/priceChange from DexScreener with 30s caching.
    Returns dict with m5/h1 data, or None on failure.
    """
    # Lazy imports to avoid circular dependency
    from paper_trade_monitor import curl_json, _select_best_dex_pair

    now = time.time()
    cached = _dex_trend_cache.get(token_ca)
    if cached and (now - cached['fetched_at']) < SMART_ENTRY_DEX_CACHE_SEC:
        return cached['data']

    url = f'https://api.dexscreener.com/latest/dex/tokens/{token_ca}'
    data = curl_json(url, timeout=timeout)
    if not data or not isinstance(data, dict):
        return cached['data'] if cached else None
    pairs = data.get('pairs')
    if not pairs or not isinstance(pairs, list):
        return cached['data'] if cached else None

    best = _select_best_dex_pair(token_ca, pairs)
    if not best:
        best = pairs[0]

    volume = best.get('volume', {}) or {}
    txns = best.get('txns', {}) or {}
    price_change = best.get('priceChange', {}) or {}
    liquidity_raw = best.get('liquidity')
    liquidity = liquidity_raw if isinstance(liquidity_raw, dict) else {}
    liquidity_unknown = not isinstance(liquidity_raw, dict) or liquidity_raw.get('usd') in (None, '')

    result = {
        'vol_m5': float(volume.get('m5', 0) or 0),
        'vol_h1': float(volume.get('h1', 0) or 0),
        'buys_m5': int((txns.get('m5', {}) or {}).get('buys', 0) or 0),
        'sells_m5': int((txns.get('m5', {}) or {}).get('sells', 0) or 0),
        'buys_h1': int((txns.get('h1', {}) or {}).get('buys', 0) or 0),
        'sells_h1': int((txns.get('h1', {}) or {}).get('sells', 0) or 0),
        'price_change_m5': float(price_change.get('m5', 0) or 0),
        'price_change_h1': float(price_change.get('h1', 0) or 0),
        'price_usd': float(best.get('priceUsd', 0) or 0),
        # A1: Pool liquidity for position sizing — prevent oversized entries in thin pools
        'liquidity_usd': float(liquidity.get('usd', 0) or 0),
        'liquidity_unknown': liquidity_unknown,
        'dex_id': best.get('dexId') or '',
        'pair_address': best.get('pairAddress') or '',
        # V7: MC/FDV for Vol/MC ratio (escape hatch for high-volume tokens with low rvol)
        'fdv': float(best.get('fdv', 0) or 0),
        'market_cap': float(best.get('marketCap', 0) or 0),
    }

    _dex_trend_cache[token_ca] = {'data': result, 'fetched_at': now}
    return result


def evaluate_trend_phase(trend_data):
    """
    Layer 1 (Scheme B): Determine market phase from DexScreener volume-price.

    Returns: (phase: str, reason: str)
      phase: 'BULLISH' | 'FAKE_PUMP' | 'BEARISH' | 'WAIT'
    """
    if not trend_data:
        return 'WAIT', 'no_trend_data'

    pc_m5 = trend_data.get('price_change_m5', 0)
    vol_m5 = trend_data.get('vol_m5', 0)
    vol_h1 = trend_data.get('vol_h1', 0)
    buys_m5 = trend_data.get('buys_m5', 0)
    sells_m5 = trend_data.get('sells_m5', 0)

    # Average m5 volume based on h1 (1 hour = 12 x 5-min windows)
    h1_avg_m5 = vol_h1 / 12.0 if vol_h1 > 0 else 0
    vol_ratio = vol_m5 / h1_avg_m5 if h1_avg_m5 > 0 else 0
    buy_sell_ratio = buys_m5 / max(sells_m5, 1)

    # Clear downtrend
    if pc_m5 < -3.0:
        return 'BEARISH', (
            f'price_m5={pc_m5:+.1f}% vol_ratio={vol_ratio:.1f} '
            f'buys={buys_m5} sells={sells_m5}'
        )

    # Price up but volume weak or sellers dominate = fake pump
    if pc_m5 > 0:
        if vol_ratio < 0.8 or buy_sell_ratio < 0.9:
            # Exemption: buyer-dominated market override
            if buy_sell_ratio >= 3.0 and pc_m5 > 10:
                return 'BULLISH', (
                    f'buyer_dominated: price_m5={pc_m5:+.1f}% '
                    f'vol_ratio={vol_ratio:.1f} buy_sell={buy_sell_ratio:.2f} (exempted)'
                )
            return 'FAKE_PUMP', (
                f'price_up_but_weak: price_m5={pc_m5:+.1f}% '
                f'vol_ratio={vol_ratio:.1f} buy_sell={buy_sell_ratio:.2f}'
            )
        if vol_ratio >= 1.5 and buy_sell_ratio >= 1.2:
            return 'BULLISH', (
                f'real_buying: price_m5={pc_m5:+.1f}% '
                f'vol_ratio={vol_ratio:.1f} buy_sell={buy_sell_ratio:.2f}'
            )
        # Moderate — price up but buy_sell must show clear buyer advantage
        if vol_ratio >= 0.8 and buy_sell_ratio >= 1.2:
            return 'BULLISH', (
                f'moderate_buying: price_m5={pc_m5:+.1f}% '
                f'vol_ratio={vol_ratio:.1f} buy_sell={buy_sell_ratio:.2f}'
            )
        # Weak — price up but no real buyer edge, not actionable
        if vol_ratio >= 0.8 and buy_sell_ratio >= 0.9:
            return 'WAIT', (
                f'weak_buying: price_m5={pc_m5:+.1f}% '
                f'vol_ratio={vol_ratio:.1f} buy_sell={buy_sell_ratio:.2f}'
            )

    return 'WAIT', (
        f'sideways: price_m5={pc_m5:+.1f}% '
        f'vol_ratio={vol_ratio:.1f} buy_sell={buy_sell_ratio:.2f}'
    )


def is_chasing_top(trend_data):
    """Simplified chase protection — only blocks extreme FOMO (>100% in 5min).
    
    84% period had NO chasing protection at all. We keep only the hard ceiling
    to protect against truly extreme cases while allowing normal momentum entries.

    Returns: (too_late: bool, reason: str)
    """
    if not trend_data:
        return False, 'no_data'

    pc_m5 = trend_data.get('price_change_m5', 0)
    sells_m5 = max(trend_data.get('sells_m5', 1), 1)
    buys_m5 = trend_data.get('buys_m5', 0)
    bs_ratio = buys_m5 / sells_m5

    # Hard ceiling: pc_m5 > 100% AND buyers not dominant → FOMO territory
    if pc_m5 > 100.0 and bs_ratio < 2.0:
        return True, (f'extreme_chase: pc_m5={pc_m5:+.0f}% '
                      f'bs={bs_ratio:.1f}<2.0')

    return False, 'ok'


def evaluate_entry_position(price_history, current_price):
    """
    Locate a tradable pullback-bounce node from recent real-time prices.
    Returns (position, detail).
    """
    if not price_history or len(price_history) < 3:
        return 'INSUFFICIENT_DATA', {
            'reason': f'only {len(price_history) if price_history else 0} price points'
        }

    prices = [p for _, p in price_history if p and p > 0]
    if len(prices) < 3:
        return 'INSUFFICIENT_DATA', {'reason': 'not_enough_valid_prices'}

    local_high = max(prices)
    high_idx = prices.index(local_high)
    prices_after_high = prices[high_idx:]
    if len(prices_after_high) < 2:
        return 'AT_TOP', {
            'reason': 'at_or_near_peak',
            'local_high': local_high,
            'current': current_price,
            'n_points': len(prices),
        }

    local_low = min(prices_after_high)
    if local_high <= 0 or local_low <= 0:
        return 'INSUFFICIENT_DATA', {'reason': 'zero_prices'}

    pullback_depth = (local_high - local_low) / local_high * 100
    bounce_from_low = (current_price - local_low) / local_low * 100
    below_high = (local_high - current_price) / local_high * 100

    detail = {
        'local_high': local_high,
        'local_low': local_low,
        'current': current_price,
        'pullback_depth_pct': round(pullback_depth, 2),
        'bounce_from_low_pct': round(bounce_from_low, 2),
        'below_high_pct': round(below_high, 2),
        'n_points': len(prices),
    }

    if (
        pullback_depth >= SMART_ENTRY_MIN_PULLBACK_PCT
        and bounce_from_low >= SMART_ENTRY_MIN_BOUNCE_PCT
        and below_high >= 2.0
    ):
        return 'GOOD_ENTRY', detail
    if pullback_depth < 1.0:
        return 'AT_TOP', detail
    if bounce_from_low < 1.0:
        return 'STILL_FALLING', detail
    if below_high < 2.0:
        return 'AT_TOP', detail
    return 'STILL_FALLING', detail


def _policy_allows(mode, entry_readiness_policy):
    if not entry_readiness_policy:
        return True
    return entry_mode_allowed(mode, entry_readiness_policy)


def _policy_min_p_follow(entry_readiness_policy, default=0.58):
    if not entry_readiness_policy:
        return default
    try:
        if hasattr(entry_readiness_policy, 'min_p_follow'):
            return float(entry_readiness_policy.min_p_follow)
        return float(entry_readiness_policy.get('min_p_follow', default))
    except (TypeError, ValueError):
        return default


def _policy_profile(entry_readiness_policy):
    if not entry_readiness_policy:
        return ""
    try:
        if hasattr(entry_readiness_policy, "lifecycle_profile"):
            return str(entry_readiness_policy.lifecycle_profile or "")
        return str(entry_readiness_policy.get("lifecycle_profile") or "")
    except Exception:
        return ""


def _policy_route(entry_readiness_policy):
    if not entry_readiness_policy:
        return ""
    try:
        detail = entry_readiness_policy.detail if hasattr(entry_readiness_policy, "detail") else entry_readiness_policy.get("detail", {})
        return str((detail or {}).get("route") or "")
    except Exception:
        return ""


def _policy_gmgn_policy(entry_readiness_policy):
    try:
        if hasattr(entry_readiness_policy, "gmgn_policy"):
            return entry_readiness_policy.gmgn_policy or {}
        if not isinstance(entry_readiness_policy, dict):
            return {}
        if isinstance(entry_readiness_policy.get("gmgn_policy"), dict):
            return entry_readiness_policy.get("gmgn_policy") or {}
        detail = entry_readiness_policy.get("detail") or {}
        if isinstance(detail, dict) and isinstance(detail.get("gmgn_policy"), dict):
            return detail.get("gmgn_policy") or {}
    except Exception:
        return {}
    return {}


def _explosive_direct_scout_ok(entry_readiness_policy, cached_trend, bs_ratio):
    if not _policy_allows("explosive_newborn_direct_scout", entry_readiness_policy):
        return False, {}
    gmgn_policy = _policy_gmgn_policy(entry_readiness_policy)
    if gmgn_policy:
        try:
            from gmgn_policy import gmgn_policy_blocks_explosive_direct
            if gmgn_policy_blocks_explosive_direct(gmgn_policy):
                return False, {
                    "gmgn_policy": gmgn_policy,
                    "gmgn_blocked": True,
                    "gmgn_reason": gmgn_policy.get("reason"),
                }
        except Exception:
            pass
    cached_trend = cached_trend or {}
    try:
        buys_m5 = float(cached_trend.get("buys_m5", 0) or 0)
    except (TypeError, ValueError):
        buys_m5 = 0.0
    try:
        sells_m5 = float(cached_trend.get("sells_m5", 0) or 0)
    except (TypeError, ValueError):
        sells_m5 = 0.0
    tx_m5 = buys_m5 + sells_m5
    try:
        price_change_m5 = float(cached_trend.get("price_change_m5", 0) or 0)
    except (TypeError, ValueError):
        price_change_m5 = 0.0
    try:
        vol_m5 = float(cached_trend.get("vol_m5", 0) or 0)
    except (TypeError, ValueError):
        vol_m5 = 0.0
    detail = {
        "price_change_m5": price_change_m5,
        "vol_m5": vol_m5,
        "tx_m5": tx_m5,
        "buy_sell_ratio": bs_ratio,
        "thresholds": {
            "price_change_m5": SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_M5_PCT,
            "vol_m5": SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_VOL_M5_USD,
            "tx_m5": SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_M5_TXNS,
            "buy_sell_ratio": SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_BS_RATIO,
        },
    }
    ok = (
        price_change_m5 >= SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_M5_PCT
        and vol_m5 >= SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_VOL_M5_USD
        and tx_m5 >= SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_M5_TXNS
        and bs_ratio >= SMART_ENTRY_EXPLOSIVE_DIRECT_MIN_BS_RATIO
    )
    return ok, detail


def smart_entry_bounce_reject_reason(pos_detail, entry_readiness_policy=None, momentum_pct=0.0):
    """Return a hard reject reason for dead-cat bounce patterns.

    This is intentionally route/lifecycle-aware: a normal continuation can
    tolerate more distance from the local high than a newborn risky probe.
    """
    profile = _policy_profile(entry_readiness_policy)
    route = _policy_route(entry_readiness_policy).upper()
    risky_newborn = profile in {"LOTTO_NEWBORN_RISKY", "LOTTO_REAL_PROBE"}
    below_high = float((pos_detail or {}).get("below_high_pct") or 0.0)
    max_below_high = SMART_ENTRY_RISKY_MAX_BELOW_HIGH_PCT if risky_newborn else SMART_ENTRY_MAX_BELOW_HIGH_PCT
    if below_high > max_below_high:
        return f"dead_cat_below_high_{below_high:.1f}pct_gt_{max_below_high:.1f}pct"
    if risky_newborn and abs(float(momentum_pct or 0.0)) < 0.5:
        return "risky_newborn_pullback_m9s_zero"
    if route == "LOTTO" and below_high > SMART_ENTRY_MAX_BELOW_HIGH_PCT:
        return f"lotto_dead_cat_below_high_{below_high:.1f}pct"
    return None


def evaluate_smart_entry(token_ca, symbol='?', pool_address=None, entry_count=0,
                         momentum_snapshots=None, momentum_pct=0, sustained_ath=False,
                         first_fire_pc_m5=None, spread_abort_count=0,
                         entry_readiness_policy=None):
    """
    Smart Entry Engine (V6 — Unified Scoring System)
    Replaces serial rejection with a 6-dimension scoring system (Total 100+ points).
    """
    from paper_trade_monitor import fetch_realtime_price
    from entry_engine import fetch_dexscreener_trend_snapshot, is_chasing_top, evaluate_trend_phase, calculate_ema_deviation, get_recent_synthetic_bars
    import time as _time
    import logging

    log = logging.getLogger('smart_entry')

    price, src, age_ms = fetch_realtime_price(token_ca, pool_address)
    cached_trend = fetch_dexscreener_trend_snapshot(token_ca)
    
    if not price or price <= 0:
        return False, 'no_price', 'could not fetch price', None

    # Parse DexScreener data
    bs_ratio = 1.0
    pc_m5 = 0
    vol_m5 = 0
    vol_h1 = 0
    if cached_trend:
        b_m5 = cached_trend.get('buys_m5', 0)
        s_m5 = max(cached_trend.get('sells_m5', 1), 1)
        bs_ratio = b_m5 / s_m5
        pc_m5 = cached_trend.get('price_change_m5', 0)
        vol_m5 = cached_trend.get('vol_m5', 0)
        vol_h1 = cached_trend.get('vol_h1', 0)

    # 1. ABSOLUTE HARD GATES (Touch and die)
    _chasing, _chase_reason = is_chasing_top(cached_trend) if cached_trend else (False, '')
    _explosive_direct_ok, _explosive_direct_detail = _explosive_direct_scout_ok(
        entry_readiness_policy,
        cached_trend,
        bs_ratio,
    )
    if _chasing:
        if _explosive_direct_ok:
            log.info(
                f"[SmartEntry] ⚡ DIRECT_SCOUT: bypassing chasing_top for tiny explosive newborn scout "
                f"detail={_explosive_direct_detail} chase={_chase_reason}"
            )
        else:
            log.info(f"[SmartEntry] 🚫  REJECT: chasing_top - {_chase_reason}")
            return False, 'chasing_top', _chase_reason, None

    # Hard gate: BS must be >= 1.05 (buyers must exceed sellers)
    # Data: AIB entered with bs=0.94 (sellers > buyers) → -21.5% loss.
    # All winning trades had bs >= 1.0.  Require slight buyer dominance.
    if bs_ratio < 1.05:
        log.info(f"[SmartEntry] 🚫  REJECT: weak_buying_pressure - bs={bs_ratio:.2f} < 1.05")
        return False, 'weak_buying_pressure', f'bs={bs_ratio:.2f} < 1.05', None

    # Trend decay detection: if pc_m5 has dropped significantly since first FIRE,
    # it means the pump is fading and we'd be buying on the way down.
    # BILLION case: pc_m5 went 17.6% → 10.0% → 1.2% across 3 FIRE attempts.
    if first_fire_pc_m5 is not None and first_fire_pc_m5 > 3.0:
        if pc_m5 < first_fire_pc_m5 * 0.5:
            log.info(
                f"[SmartEntry] 🚫  REJECT: momentum_fading - "
                f"pc_m5 decayed {first_fire_pc_m5:+.1f}%→{pc_m5:+.1f}% "
                f"(>{50}% drop since first FIRE)")
            return False, 'momentum_fading', (
                f'pc_m5 {first_fire_pc_m5:+.1f}%→{pc_m5:+.1f}% '
                f'({((first_fire_pc_m5-pc_m5)/first_fire_pc_m5*100):.0f}% decay)'), None

    # SPREAD_GUARD abort guard: ANY prior abort means price was at the top.
    # Data (9.5h, 5 tokens): 2/2 entries after spread abort = loss
    #   casinu:  1 abort → entered 2min later → -15.4%
    #   BILLION: 2 aborts → entered on 3rd try → -16.0%
    # Tokens where SPREAD_GUARD blocked ALL attempts ($ORG 3x, TRIFECTA 7x)
    # showed pc_m5 decay 70-85%, confirming the pump was over.
    if spread_abort_count >= 1:
        log.info(
            f"[SmartEntry] 🚫  REJECT: post_spread_abort - "
            f"{spread_abort_count} spread abort(s), price likely past peak "
            f"(data: 2/2 post-abort entries lost -15% to -16%)")
        return False, 'post_spread_abort', (
            f'{spread_abort_count} spread abort(s), past peak'), None

    # TREND GUARD: reject if 5-minute price change is negative.
    # Data (8hr audit, 2026-04-24): 4 trades with pc_m5<0 → 1W/3L = 25% win rate.
    # If the trend is declining, we're buying into a falling knife.
    # Matrix T≥60 should already filter this, but SmartEntry runs later and
    # DexScreener data may have shifted. This is the second safety net.
    if pc_m5 is not None and pc_m5 < 0:
        log.info(
            f"[SmartEntry] 🚫  REJECT: negative_trend - "
            f"pc_m5={pc_m5:+.1f}% (5min price declining, refusing to buy into downtrend)")
        return False, 'negative_trend', f'pc_m5={pc_m5:+.1f}% < 0', None

    liq_usd = cached_trend.get('liquidity_usd', 0) if cached_trend else 0
    if cached_trend and 0 < liq_usd < 5000:
        log.info(f"[SmartEntry] 🚫  REJECT: low_liquidity -  < 000")
        return False, 'low_liquidity', f'liquidity= < 000', None

    _dev_pct, _ema_val = calculate_ema_deviation(token_ca, price, pool_address=pool_address)
    if _dev_pct is not None and _dev_pct > 120.0:
        log.info(f"[SmartEntry] 🚫  REJECT: ema_extreme (>{_dev_pct:.0f}%)")
        return False, 'ema_extreme', f'deviation={_dev_pct:.0f}% > 120%', None

    # 2. SCORING SYSTEM
    total_score = 0
    score_details = []

    # Dim 1: bs_ratio (Max 25)
    bs_score = 0
    if bs_ratio >= 2.0: bs_score = 25
    elif bs_ratio >= 1.5: bs_score = 20
    elif bs_ratio >= 1.0: bs_score = 15
    elif bs_ratio >= 0.7: bs_score = 8
    total_score += bs_score
    score_details.append(f"bs:{bs_score}")

    # Dim 2: RVol (Max 20)
    h1_avg = vol_h1 / 12.0 if vol_h1 > 0 else 0
    rvol = (vol_m5 / h1_avg) if h1_avg > 0 else (999.0 if vol_m5 > 0 else 0)
    rvol_score = 0
    if rvol >= 3.0: rvol_score = 20
    elif rvol >= 2.0: rvol_score = 16
    elif rvol >= 1.5: rvol_score = 12
    elif rvol >= 1.0: rvol_score = 7
    total_score += rvol_score
    score_details.append(f"rvol:{rvol_score}")

    # Dim 3: pc_m5 (Max 15)
    # V8 fix: parabolic moves (>50%) were getting 0 points. These are often
    # the strongest signals. Now: sweet spot at 10-20%, still positive above.
    pc_score = 0
    if 10 <= pc_m5 <= 20: pc_score = 15  # sweet spot
    elif 20 < pc_m5 <= 35: pc_score = 10
    elif 5 <= pc_m5 < 10: pc_score = 8
    elif 35 < pc_m5 <= 50: pc_score = 5
    elif pc_m5 > 50: pc_score = 3  # V8: was 0, parabolic still valid
    total_score += pc_score
    score_details.append(f"pc:{pc_score}")

    # Dim 4: m9s (Max 15)
    m9s_score = 0
    if 1.5 <= momentum_pct <= 3.5: m9s_score = 15
    elif 3.5 < momentum_pct <= 6.0: m9s_score = 10
    elif 0.5 <= momentum_pct < 1.5: m9s_score = 8
    elif 6.0 < momentum_pct <= 10.0: m9s_score = 5
    total_score += m9s_score
    score_details.append(f"m9s:{m9s_score}")

    # Dim 5: Trend Phase (Max 15)
    trend_phase, _ = evaluate_trend_phase(cached_trend)
    trend_score = 0
    if trend_phase == 'BULLISH': trend_score = 15
    elif trend_phase == 'WAIT': trend_score = 8
    elif trend_phase == 'BEARISH': trend_score = 2
    total_score += trend_score
    score_details.append(f"tr:{trend_score}")

    # Dim 6: EMA Deviation (Max 10)
    ema_score = 0
    if _dev_pct is not None:
        if _dev_pct <= 25: ema_score = 10
        elif _dev_pct <= 50: ema_score = 7
        elif _dev_pct <= 75: ema_score = 4
        elif _dev_pct <= 100: ema_score = 2
    else:
        ema_score = 10  # safe default if EMA unavailable
    total_score += ema_score
    score_details.append(f"ema:{ema_score}")

    # 3. BONUSES
    bonus_score = 0
    if sustained_ath:
        bonus_score += 8
        score_details.append("b_ath:8")
    
    # Pullback-bounce check
    if momentum_snapshots and len(momentum_snapshots) >= 3:
        snap_high = max(momentum_snapshots)
        snap_low = min(momentum_snapshots)
        snap_last = momentum_snapshots[-1]
        if snap_high > 0 and snap_low > 0 and snap_last > 0:
            pullback_pct = ((snap_high - snap_low) / snap_high) * 100
            bounce_pct = ((snap_last - snap_low) / snap_low) * 100
            if pullback_pct >= 2.0 and bounce_pct >= 2.0:
                bounce_ratio_val = bounce_pct / pullback_pct
                if bounce_ratio_val >= 0.25:
                    bonus_score += 10
                    score_details.append("b_pb:10")
    
    # K-line breakout
    try:
        _recent_bars = get_recent_synthetic_bars(token_ca, n_bars=3, pool_address=pool_address, native_only=True)
        if len(_recent_bars) >= 2:
            _prev_high = _recent_bars[-2]['high']
            if price > _prev_high:
                bonus_score += 5
                score_details.append("b_kline:5")
    except Exception as e:
        pass

    total_score += bonus_score
    base_score = total_score - bonus_score  # core dimensions only (no bonuses)

    # 4. PRE-BUY K-LINE TREND CONFIRMATION
    # Architecture: DexScreener drove FIRE. Now confirm the trend is STILL ALIVE
    # using the most recent COMPLETE (closed) 1-minute synthetic K-line.
    # Root cause fix: Previously used the last bar (which is always the current
    # in-progress minute bar). That bar almost always has open==close (+0.0%)
    # because it just started, making the entire check useless.
    # 8hr audit proof: 21/22 trades had +0.0% KLINE_OK from partial bars.
    # Fix: Use only COMPLETE (closed) bars. If none exist, report insufficient.
    _kline_confirmed = False  # Track whether kline gave real bullish signal
    try:
        _kline_bars = get_recent_synthetic_bars(token_ca, n_bars=5, pool_address=pool_address, native_only=True)
        # Find the last COMPLETE bar (not the current in-progress minute)
        _complete_bars = [b for b in _kline_bars if b.get('complete', False)] if _kline_bars else []
        if _complete_bars:
            _last_bar = _complete_bars[-1]
            _bar_open  = _last_bar.get('open', 0)
            _bar_close = _last_bar.get('close', 0)
            if _bar_open > 0 and _bar_close < _bar_open:
                _bar_drop = (_bar_open - _bar_close) / _bar_open * 100
                # Tiered kline rejection (V7 fix, 2026-04-26):
                # Old: ANY red candle = REJECT. Killed TOLY (-2%, later +149%).
                # New: <3% = noise (allow), 3-8% = warning (allow+penalty), >8% = REJECT.
                if _bar_drop > 8.0:
                    # Genuine reversal — hard reject
                    log.info(
                        f"[SmartEntry] 🚫  REJECT: kline_trend_reversed - "
                        f"last closed 1min bar crashed (open={_bar_open:.10f} → close={_bar_close:.10f}, "
                        f"-{_bar_drop:.1f}% > 8%). Genuine reversal.")
                    return False, 'kline_trend_reversed', f'last bar -{_bar_drop:.1f}% (crash >8%)', None
                elif _bar_drop > 3.0:
                    # Warning zone — allow but penalize score
                    total_score -= 5
                    score_details.append(f"kpen:-5")
                    log.info(
                        f"[SmartEntry] ⚠️  KLINE_WARN: last closed 1min bar bearish "
                        f"(-{_bar_drop:.1f}%, 3-8% zone). Allowing with -5 score penalty.")
                else:
                    # Noise (<3%) — normal meme coin volatility, ignore
                    log.info(
                        f"[SmartEntry] ℹ️  KLINE_NOISE: last closed 1min bar "
                        f"(-{_bar_drop:.1f}% < 3%). Normal volatility, ignoring.")
            elif _bar_open > 0 and _bar_close > _bar_open:
                _bar_gain = (_bar_close - _bar_open) / _bar_open * 100
                if _bar_gain >= 1.0:
                    _kline_confirmed = True
                    log.info(
                        f"[SmartEntry] ✅  KLINE_OK: last closed 1min bar bullish "
                        f"(+{_bar_gain:.1f}%)")
                else:
                    log.info(
                        f"[SmartEntry] ⚠️  KLINE_WEAK: last closed 1min bar "
                        f"(+{_bar_gain:.1f}% < 1.0%), no kline confirmation")
            elif _bar_open > 0 and _bar_close == _bar_open:
                log.info(
                    f"[SmartEntry] ⚠️  KLINE_FLAT: last closed 1min bar is doji "
                    f"(+0.0%), cannot confirm trend")
        else:
            # No complete bars available — token too new, kline data insufficient
            log.info(
                f"[SmartEntry] ⚠️  KLINE_INSUFFICIENT: no closed 1min bars yet "
                f"(token too new for kline confirmation)")
    except Exception:
        pass  # If K-line unavailable, don't block the trade

    # 4b. COMPOUND WEAKNESS GATE
    # If kline did NOT confirm bullish trend AND rvol is low → hard reject.
    # Backtested (22 trades): filters 4 DOA (-68.4%) at cost of 2 marginal
    # wins (+5.4%), net EV +63%. Preserves Dogcoin (rvol=2.8x) and
    # BensHouse (rvol=5.3x) which had genuine volume behind the move.
    # V7: Vol/MC escape hatch — if Vol/MC > 30%, token IS heavily traded
    # regardless of rvol. KITTENGER had rvol 0.2x but Vol/MC=60-80% (massive).
    _mc = cached_trend.get('fdv', 0) or cached_trend.get('market_cap', 0) if cached_trend else 0
    _vol_mc = (vol_m5 / _mc) if _mc > 0 else 0
    if not _kline_confirmed and rvol < 2.0 and _vol_mc < 0.30:
        log.info(
            f"[SmartEntry] 🚫  REJECT: no_kline_low_volume - "
            f"kline not confirmed + rvol={rvol:.1f}x < 2.0x + vol/mc={_vol_mc:.1%} < 30% "
            f"(compound weakness: no trend proof + no volume surge)")
        return False, 'no_kline_low_volume', f'kline_unconfirmed + rvol={rvol:.1f}x + vol/mc={_vol_mc:.1%}', None
    elif not _kline_confirmed and rvol < 2.0 and _vol_mc >= 0.30:
        log.info(
            f"[SmartEntry] ✅  VOL_MC_BYPASS: kline not confirmed + rvol={rvol:.1f}x < 2.0x "
            f"BUT vol/mc={_vol_mc:.1%} >= 30% — token is heavily traded, allowing entry")

    # 5. DECISION LOGIC
    detail_str = f"Score={total_score} (base={base_score}) [{','.join(score_details)}] bs={bs_ratio:.2f} rvol={rvol:.1f}x m9s={momentum_pct:+.1f}% pc_m5={pc_m5:+.1f}%"

    if total_score < 50:
        log.info(f"[SmartEntry] 🚫  REJECT: {detail_str}")
        return False, 'score_too_low', detail_str, None

    if _explosive_direct_ok:
        node_detail = (
            f"node=explosive_direct_scout p_follow=0.74 "
            f"pc_m5={_explosive_direct_detail.get('price_change_m5'):+.1f}% "
            f"vol_m5=${_explosive_direct_detail.get('vol_m5'):.0f} "
            f"tx_m5={_explosive_direct_detail.get('tx_m5')} "
            f"buy_sell={bs_ratio:.2f} armed=({detail_str})"
        )
        log.info(f"[SmartEntry] ⚡ ${symbol} EXPLOSIVE_DIRECT_SCOUT at ${price:.10f}: {node_detail}")
        return True, 'explosive_newborn_direct_scout', node_detail, price

    # Score only arms the candidate. Entry requires a live timing node.
    from matrix_evaluator import MatrixEvaluator

    now0 = _time.time()
    existing = MatrixEvaluator._price_history.get(token_ca, [])
    price_history = [(t, p) for t, p in existing if now0 - t <= 180 and p and p > 0]
    price_history.append((_time.time(), price))
    max_wait = max(1.0, float(ENTRY_READINESS_MAX_WAIT_SEC))
    poll_sec = max(0.25, float(ENTRY_READINESS_POLL_SEC))
    min_p_follow = _policy_min_p_follow(entry_readiness_policy)
    best_wait_detail = "waiting_for_entry_node"
    consecutive_momentum_rounds = 0
    best_trend_phase = trend_phase
    fake_pump_count = 0

    log.info(
        f"[SmartEntry] ARM ${symbol}: {detail_str} "
        f"policy={entry_readiness_policy if entry_readiness_policy else 'default'} "
        f"waiting up to {max_wait:.0f}s for momentum_direct or pullback_bounce"
    )

    while _time.time() - now0 <= max_wait:
        loop_start = _time.time()
        live_price, _, _ = fetch_realtime_price(token_ca, pool_address)
        if live_price and live_price > 0:
            price_history.append((_time.time(), live_price))
            MatrixEvaluator._price_history[token_ca] = [
                (t, p) for t, p in price_history if _time.time() - t <= 3600
            ]
        else:
            live_price = price_history[-1][1] if price_history else price

        trend_now = fetch_dexscreener_trend_snapshot(token_ca) or cached_trend
        phase_now, phase_reason = evaluate_trend_phase(trend_now)
        if phase_now == 'BEARISH' and (_time.time() - now0) > 300:
            return False, 'trend_bearish_timeout', phase_reason, None
        if phase_now == 'FAKE_PUMP':
            fake_pump_count += 1
        if phase_now == 'BULLISH' and best_trend_phase != 'BULLISH':
            best_trend_phase = 'BULLISH'

        b_now = (trend_now or {}).get('buys_m5', 0) or 0
        s_now = max((trend_now or {}).get('sells_m5', 1) or 1, 1)
        bs_now = b_now / s_now

        vel_30s = _calc_velocity(price_history, 30)
        vel_60s = _calc_velocity(price_history, 60)
        if vel_30s > 15.0 and vel_60s < 0:
            consecutive_momentum_rounds = 0
            best_wait_detail = f"dead_cat vel30={vel_30s:+.1f}%/min vel60={vel_60s:+.1f}%/min"
        elif vel_30s > 15.0 and vel_60s > 5.0 and bs_now > 1.0:
            consecutive_momentum_rounds += 1
        else:
            consecutive_momentum_rounds = 0

        elapsed = _time.time() - now0
        min_momentum_wait = 10 if (bs_now >= 5.0 and vel_30s > 30.0) else 30
        if consecutive_momentum_rounds >= 2 and elapsed >= min_momentum_wait and live_price > 0:
            p_follow = 0.74 if bs_now >= 1.2 else 0.66
            if p_follow >= min_p_follow and _policy_allows('momentum_direct_entry', entry_readiness_policy):
                node_detail = (
                    f"node=momentum_direct p_follow={p_follow:.2f} "
                    f"vel_30s={vel_30s:+.1f}%/min vel_60s={vel_60s:+.1f}%/min "
                    f"buy_sell={bs_now:.2f} consecutive={consecutive_momentum_rounds} "
                    f"waited={elapsed:.0f}s armed=({detail_str}) trend={phase_reason}"
                )
                log.info(f"[SmartEntry] 🚀 ${symbol} MOMENTUM_ENTRY at ${live_price:.10f}: {node_detail}")
                return True, 'momentum_direct_entry', node_detail, live_price

        position, pos_detail = evaluate_entry_position(price_history, live_price)
        if position == 'GOOD_ENTRY' and phase_now == 'BULLISH':
            pullback = pos_detail.get('pullback_depth_pct', 0)
            bounce = pos_detail.get('bounce_from_low_pct', 0)
            bounce_ratio = bounce / pullback if pullback > 0 else 0
            trend_downgraded = (best_trend_phase == 'BULLISH' and phase_now != 'BULLISH')
            bounce_reject = smart_entry_bounce_reject_reason(
                pos_detail,
                entry_readiness_policy=entry_readiness_policy,
                momentum_pct=momentum_pct,
            )
            if bounce_reject:
                return False, bounce_reject, (
                    f"pullback={pullback:.1f}% bounce={bounce:.1f}% "
                    f"bounce_ratio={bounce_ratio:.0%} below_high={pos_detail.get('below_high_pct', 0):.1f}% "
                    f"m9s={momentum_pct:+.1f}% profile={_policy_profile(entry_readiness_policy) or 'default'}"
                ), None
            if fake_pump_count >= SMART_ENTRY_FAKE_PUMP_THRESHOLD and bs_now < 2.0:
                best_wait_detail = f"fake_pump_history={fake_pump_count} bs={bs_now:.2f}<2.0"
            elif bounce_ratio < SMART_ENTRY_MIN_BOUNCE_RATIO:
                best_wait_detail = f"bounce_ratio={bounce_ratio:.0%} too low"
            elif trend_downgraded:
                best_wait_detail = "trend_downgraded"
            else:
                p_follow = 0.68 if bs_now >= 1.2 else 0.60
                if p_follow >= min_p_follow and _policy_allows('smart_entry_pullback_bounce', entry_readiness_policy):
                    node_detail = (
                        f"node=pullback_bounce p_follow={p_follow:.2f} "
                        f"pullback={pullback:.1f}% bounce={bounce:.1f}% "
                        f"bounce_ratio={bounce_ratio:.0%} below_high={pos_detail.get('below_high_pct', 0):.1f}% "
                        f"buy_sell={bs_now:.2f} waited={elapsed:.0f}s armed=({detail_str}) trend={phase_reason}"
                    )
                    log.info(f"[SmartEntry] ✅ ${symbol} GOOD_ENTRY at ${live_price:.10f}: {node_detail}")
                    return True, 'smart_entry_pullback_bounce', node_detail, live_price
        else:
            best_wait_detail = (
                f"{phase_now}/{position} vel30={vel_30s:+.1f}%/min "
                f"vel60={vel_60s:+.1f}%/min bs={bs_now:.2f} detail={pos_detail}"
            )

        sleep_for = poll_sec - (_time.time() - loop_start)
        if sleep_for > 0:
            _time.sleep(sleep_for)

    return False, 'entry_node_timeout', f'{best_wait_detail}; armed=({detail_str})', None
