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

log = logging.getLogger('paper_trade_monitor')

# ─── Kelly Criterion Constants ────────────────────────────────────────────────
KELLY_BASE_CAPITAL_SOL = float(os.environ.get('KELLY_BASE_CAPITAL_SOL', '5.0'))
KELLY_BASE_WIN_RATE    = 0.45   # Based on overnight data: 48% win rate (conservative)
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
    """A2: Compute adaptive stop-loss based on recent trade volatility.
    Uses avg peak_pnl from last 20 trades as a volatility proxy:
    - High avg peak (25%+) → wide SL (-12%) to avoid early washout
    - Low avg peak (6%) → tight SL (-5%) to cut losers fast
    - Default (no data): -7.5%
    Returns: float (e.g., -0.075)
    """
    cache = _adaptive_sl_cache
    now = time.time()
    if now - cache['last_refresh'] < 300:  # refresh every 5 min
        return cache['sl']

    try:
        store = WatchlistStore()
        avg_peak = store.get_recent_avg_peak_pnl(limit=20)
        if avg_peak is None:
            cache['last_refresh'] = now
            return cache['sl']  # keep previous value

        # Formula: sl = -(avg_peak * 0.35), clamped to [-0.12, -0.05]
        raw_sl = -(avg_peak * 0.35)
        adaptive = max(-0.12, min(-0.05, raw_sl))
        cache['sl'] = round(adaptive, 4)
        cache['last_refresh'] = now
        log.info(f"[AdaptiveSL] avg_peak={avg_peak*100:.1f}% → SL={adaptive*100:.1f}% (raw={raw_sl*100:.2f}%)")
        return cache['sl']
    except Exception as e:
        log.debug(f"[AdaptiveSL] failed: {e}")
        cache['last_refresh'] = now
        return cache['sl']

# ─── SmartEntry Constants ─────────────────────────────────────────────────────
_dex_trend_cache = {}
SMART_ENTRY_DEX_CACHE_SEC = 15       # Reuse DexScreener data for 15 seconds
SMART_ENTRY_POLL_INTERVAL_SEC = 10   # Poll price every 10 seconds
SMART_ENTRY_MAX_WAIT_SEC = 900       # 15-minute maximum wait
SMART_ENTRY_MIN_PULLBACK_PCT = 2.0   # Minimum pullback depth to qualify
SMART_ENTRY_MIN_BOUNCE_PCT = 2.0     # Minimum bounce from low to confirm
SMART_ENTRY_MIN_BOUNCE_RATIO = 0.30  # bounce/pullback default (data: 25% let through Goose -14.8%)
SMART_ENTRY_BOUNCE_RATIO_STRONG = 0.15   # strong signal: bs>=1.5 + vol>=2.0 + real_buying
SMART_ENTRY_BOUNCE_RATIO_MEDIUM = 0.20   # medium signal: bs>=1.3 + vol>=1.5
SMART_ENTRY_MIN_VOL_RATIO = 2.0      # vol_ratio floor (data: ALL trades with vol<2.0 lost, 0W/6L on 2026-04-19)
SMART_ENTRY_REENTRY_VOL_RATIO = 2.0  # same threshold for re-entries (data: no reason to be lenient)
SMART_ENTRY_MIN_POINTS = 6            # Minimum price data points before entry (data: GREKT 4pt → instant -7.7% SL)
SMART_ENTRY_FAKE_PUMP_THRESHOLD = 10  # After N fake_pump rounds, require stricter entry (buy_sell>=2.0)


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
    Compute position size using Kelly Criterion.

    Simplified based on 23-trade backtest (2026-04-15):
      - Only entry_mode and ATH are data-validated predictors
      - Matrix crowding, sub-indices, signal velocity, DexBoost: NOT predictive → removed
      - momentum_direct: 62% win rate → full position
      - pullback_bounce: 43% win rate → p × 0.85

    Returns position size in SOL (min 0.03, max 20% of base_capital).
    """
    if base_capital is None:
        base_capital = KELLY_BASE_CAPITAL_SOL

    p = KELLY_BASE_WIN_RATE
    b = _get_historical_odds()  # historical avg_win/avg_loss, capped at 3.0

    entry_count = int(watchlist_entry.get('entry_count') or 0)

    # ─── Entry mode adjustment ──────────────────────────────────────
    # REMOVED: pullback_bounce penalty (p *= 0.85) was based on assumed 62% vs 43% win rates.
    # Deep audit (31 trades, commit 71be6ec5) found the OPPOSITE:
    #   momentum_direct: 12% win rate (1W/7L, -42.3%)
    #   pullback_bounce: 31% win rate (4W/9L, -34.3%)
    # Penalizing pullback_bounce was making things worse.
    # With only 8+13 trades, neither sample is statistically significant (p=0.017/0.083),
    # so we neutralize entry_mode's effect on Kelly rather than reverse it.

    # ─── Matrix crowding (avg pnl validated: ≤1 perfect=+5%, 2 perfect=-1.1%) ──
    if matrix_scores:
        perfect_count = sum(1 for v in matrix_scores.values() if v == 100)
        if perfect_count >= 4:
            p *= 0.7    # heavy crowding penalty
            log.info(f"[Kelly] Matrix crowding: {perfect_count}/5 perfect → p×0.7")
        elif perfect_count >= 3:
            p *= 0.9    # mild penalty
        elif perfect_count <= 1:
            # Contrarian bonus — ONLY for first entry on fresh coins.
            # Data: Veteran② got contrarian ×1.2 on re-entry (decaying signal) → 0.775 SOL → -12.5%.
            # Low perfect_count on re-entry means signal decay, not contrarian opportunity.
            entry_age_min = (time.time() - watchlist_entry.get('added_at', time.time())) / 60
            if entry_count == 0 and entry_age_min <= 5:
                p *= 1.2    # contrarian bonus — less crowded, fresh signal
                log.info(f"[Kelly] Matrix contrarian: {perfect_count}/5 perfect → p×1.2")
            else:
                log.info(f"[Kelly] Matrix low-perfect={perfect_count}/5 but entry_count={entry_count} age={entry_age_min:.0f}min → no contrarian bonus")

    # ─── ATH confirmation (logical — new highs have momentum) ─────────
    ath_num = int(watchlist_entry.get('ath_num') or 0)
    if ath_num > 0:
        ath_boost = {1: 1.6, 2: 1.4, 3: 1.2}.get(ath_num, 1.1)
        p *= ath_boost
        log.info(f"[Kelly] ATH#{ath_num} → p×{ath_boost} → p={p:.3f}")
    elif watchlist_entry.get('has_ath') or watchlist_entry.get('type') == 'ATH':
        p *= 1.5
        b *= 1.3

    p = min(p, 0.65)  # cap probability

    # Kelly formula: f* = (p*b - q) / b
    q = 1.0 - p
    kelly_f = (p * b - q) / b if b > 0 else -1.0

    # Negative EV → use minimum position (Kelly sizes, doesn't veto — Matrix decides trades)
    if kelly_f <= 0:
        log.info(f"[Kelly] f*={kelly_f:.3f} ≤ 0 → MIN position 0.03 SOL | p={p:.3f} b={b:.2f}")
        return 0.03

    # Half-Kelly for safety
    position = base_capital * kelly_f * 0.5

    # ─── Re-entry penalty: halve position (data: re-entries 50% win vs 64% first) ──
    if entry_count > 0:
        position *= 0.5
        log.info(f"[Kelly] Re-entry #{entry_count+1} → position×0.5")

    # Hard limits: min 0.03 SOL, max 20% of capital, absolute cap MAX_POSITION_SOL
    pos = round(max(0.03, min(position, base_capital * 0.20, MAX_POSITION_SOL)), 3)
    log.info(f"[Kelly] f*={kelly_f:.3f} → {pos} SOL | p={p:.3f} b={b:.2f} mode={entry_mode or 'default'}")
    return pos


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
    liquidity = best.get('liquidity', {}) or {}

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
    """Multi-factor chase protection: decides if we're too late to enter.

    Instead of a fixed pc_m5 cutoff, looks at whether money is still flowing IN:
      - Higher pc_m5 → requires proportionally stronger buy pressure + volume
      - If buyers still dominate and volume is surging, high pc_m5 is OK
      - If buyers are fading or volume is dropping, even moderate pc_m5 is risky

    Returns: (too_late: bool, reason: str)
    """
    if not trend_data:
        return False, 'no_data'

    pc_m5 = trend_data.get('price_change_m5', 0)
    vol_m5 = trend_data.get('vol_m5', 0)
    vol_h1 = trend_data.get('vol_h1', 0)
    buys_m5 = trend_data.get('buys_m5', 0)
    sells_m5 = max(trend_data.get('sells_m5', 1), 1)

    h1_avg = vol_h1 / 12.0 if vol_h1 > 0 else 0
    if h1_avg > 0:
        vol_ratio = vol_m5 / h1_avg
    elif vol_m5 > 0:
        vol_ratio = 5.0  # has m5 volume but no h1 → brand new token, assume active
    else:
        vol_ratio = 0
    bs_ratio = buys_m5 / sells_m5

    # Hard ceiling: pc_m5 > 100% AND buyers not dominant → FOMO territory
    if pc_m5 > 100.0 and bs_ratio < 2.0:
        return True, (f'extreme_chase: pc_m5={pc_m5:+.0f}% '
                      f'bs={bs_ratio:.1f}<2.0 vol={vol_ratio:.1f}')

    # Dynamic tiers: higher price surge → stricter funding requirements
    # Tier 3 (50-100%): need strong buyer edge + volume surge
    # Data: LOCKED bs=1.42 vol=2.5 at pc_m5=52% → was blocked by bs<1.5, missed +120%
    # Adjusted: bs>=1.4 vol>=1.3 for smoother gradient from mid_chase tier
    if pc_m5 > 50.0:
        if bs_ratio < 1.4 or vol_ratio < 1.3:
            return True, (f'high_chase: pc_m5={pc_m5:+.0f}% '
                          f'needs bs>=1.4+vol>=1.3, '
                          f'got bs={bs_ratio:.1f} vol={vol_ratio:.1f}')

    # Tier 2 (15-50%): need moderate buyer edge + stable volume
    elif pc_m5 > 15.0:
        if bs_ratio < 1.2 or vol_ratio < 1.0:
            return True, (f'mid_chase: pc_m5={pc_m5:+.0f}% '
                          f'needs bs>=1.2+vol>=1.0, '
                          f'got bs={bs_ratio:.1f} vol={vol_ratio:.1f}')

    # Tier 1 (0-15%): no chase restriction — Layer 1 BULLISH is sufficient
    return False, 'ok'


def evaluate_entry_position(price_history, current_price):
    """
    Layer 2 (Scheme A): Determine if current price is a good entry position
    based on pullback-bounce pattern. No "chase" — never buys at top.

    price_history: list of (timestamp, price) tuples, sorted by time
    current_price: latest price

    Returns: (position: str, detail: dict)
      position: 'GOOD_ENTRY' | 'AT_TOP' | 'STILL_FALLING' | 'INSUFFICIENT_DATA'
    """
    if not price_history or len(price_history) < 3:
        return 'INSUFFICIENT_DATA', {
            'reason': f'only {len(price_history) if price_history else 0} price points'
        }

    prices = [p for _, p in price_history]

    # Find local high (highest point in history)
    local_high = max(prices)
    high_idx = prices.index(local_high)

    # Find local low AFTER the high (the pullback bottom)
    prices_after_high = prices[high_idx:]
    if len(prices_after_high) < 2:
        # No data after the peak — we might be AT the peak
        return 'AT_TOP', {
            'reason': 'at_or_near_peak',
            'local_high': local_high,
            'current': current_price,
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

    # Good entry: pulled back enough AND bounced confirming bottom
    if (pullback_depth >= SMART_ENTRY_MIN_PULLBACK_PCT
            and bounce_from_low >= SMART_ENTRY_MIN_BOUNCE_PCT
            and below_high >= 2.0):
        # Dead cat bounce filter: if price is still >15% below the local high,
        # the "bounce" is likely a trap within a larger downtrend.
        # Data: 6/6 overnight trades with below_high>15% lost money:
        #   drone -31%, KITTY -14.9%, GME -19.2%, Plumpshies -8%,
        #   Human -1%, ELONBOAR -4%. below_high<10% had winners (SS +10.5%, GME +7.6%)
        if below_high > 15.0:
            detail['reject_reason'] = f'dead_cat_bounce below_high={below_high:.1f}%>15%'
            return 'STILL_FALLING', detail
        return 'GOOD_ENTRY', detail

    # At or near the top — no significant pullback yet
    if pullback_depth < 1.0:
        return 'AT_TOP', detail

    # Pulled back but no bounce yet — still falling or at the bottom
    if bounce_from_low < 1.0:
        return 'STILL_FALLING', detail

    # Bounced but almost back to the high — too late, risk of double-top
    if below_high < 2.0:
        return 'AT_TOP', detail

    # Everything else: keep waiting
    return 'STILL_FALLING', detail


# Shared dict for cross-thread data refresh: main thread → SmartEntry thread.
# When a coin re-FIREs while SmartEntry is running, main thread writes here.
# SmartEntry checks this each round and uses fresh data for decisions.
# Python dict assignment is atomic (GIL), so no lock needed.
_smart_entry_signals = {}


def evaluate_smart_entry(token_ca, symbol='?', pool_address=None, entry_count=0):
    """
    Smart Entry Engine — replaces evaluate_entry_timing() (Dip-then-Rip).

    Two-layer dynamic entry decision:
      Layer 1: DexScreener volume-price trend confirmation (refreshed every 30s)
      Layer 2: Price trajectory pullback-bounce detection (sampled every 10s)

    Only enters on confirmed pullback-bounce pattern (NO chase / NO追涨).
    Maximum wait: 15 minutes. After that, rejects the entry.

    Returns: (should_enter: bool, reason: str, detail: str, trigger_price: float|None)
    """
    # Lazy import to avoid circular dependency
    from paper_trade_monitor import fetch_realtime_price
    from matrix_evaluator import MatrixEvaluator

    max_rounds = int(SMART_ENTRY_MAX_WAIT_SEC / SMART_ENTRY_POLL_INTERVAL_SEC)
    interval = SMART_ENTRY_POLL_INTERVAL_SEC  # 10s

    # Seed price_history from Matrix's accumulated observations.
    # MatrixEvaluator polls this token every 10-60s since it joined the watchlist,
    # so we typically have 5-30+ minutes of pre-existing price data.
    # This means vel_30s and vel_60s work from round 1 — no cold-start delay.
    # NOTE: 120s window validated — peak=0% root cause is quote spread, not stale data.
    now = time.time()
    existing = MatrixEvaluator._price_history.get(token_ca, [])
    price_history = [(t, p) for t, p in existing if now - t <= 120 and p > 0]
    seed_count = len(price_history)

    last_dex_check = 0
    cached_trend = None
    start_time = time.time()
    best_trend_phase = None  # Track the strongest trend phase seen so far
    consecutive_momentum_rounds = 0  # Track consecutive strong momentum rounds
    last_momentum_data = None  # Track last DexScreener data to detect stale cache
    fake_pump_count = 0  # P5: accumulate FAKE_PUMP rounds
    bearish_extended = False  # Plan #2: granted accumulation grace period once
    # Plan #2: dynamic max-wait — accumulation extension adds 600s
    max_wait_sec = SMART_ENTRY_MAX_WAIT_SEC
    pc_m5_peak = 0.0  # Track pc_m5 peak to detect momentum fading

    log.info(
        f"[SmartEntry] ${symbol} starting smart entry evaluation "
        f"(max {SMART_ENTRY_MAX_WAIT_SEC}s, poll {interval}s, "
        f"seeded {seed_count} price points from watchlist)"
    )

    round_num = 0
    while True:
        round_num += 1
        elapsed = time.time() - start_time
        if elapsed > max_wait_sec:
            break

        # --- Check for data refresh from main thread ---
        # If this coin re-FIREd while we're evaluating, use the fresh data.
        _refresh = _smart_entry_signals.pop(token_ca, None)
        if _refresh:
            log.info(
                f"[SmartEntry] ${symbol} round {round_num} DATA REFRESH from new FIRE: "
                f"scores={_refresh.get('scores', '?')} "
                f"({elapsed:.0f}s into evaluation)"
            )
            # Reset momentum tracking with fresh signal
            _new_trend = _refresh.get('trend_data')
            if _new_trend:
                cached_trend = _new_trend
                last_dex_check = time.time()

        # --- Sample price (every round, using Jupiter/Redis/Helius, NOT DexScreener) ---
        price, src, age_ms = fetch_realtime_price(token_ca, pool_address)
        if price and price > 0:
            price_history.append((time.time(), price))

        # --- Refresh DexScreener trend data (every 30s) ---
        if time.time() - last_dex_check >= SMART_ENTRY_DEX_CACHE_SEC:
            trend_data = fetch_dexscreener_trend_snapshot(token_ca)
            if trend_data:
                cached_trend = trend_data
                last_dex_check = time.time()

        # --- Layer 1: Trend Phase (Scheme B) ---
        trend_phase, trend_reason = evaluate_trend_phase(cached_trend)

        if trend_phase == 'BEARISH':
            log.info(
                f"[SmartEntry] ${symbol} round {round_num} BEARISH: {trend_reason} "
                f"({elapsed:.0f}s elapsed)"
            )
            # Don't reject immediately on bearish — it might recover.
            # But if still bearish after 5 minutes, give up.
            if elapsed > 300:
                # Plan #2: 3-state classifier — distinguish dying from accumulating
                # before killing the entry. Accumulating coins (sideways with
                # stable bs/vol) get one 600s extension; only true dying coins
                # (decisive sell pressure) are rejected outright.
                _pc_m5 = (cached_trend or {}).get('price_change_m5', 0)
                _v5 = (cached_trend or {}).get('vol_m5', 0)
                _vh1 = (cached_trend or {}).get('vol_h1', 0)
                _h1_avg = _vh1 / 12.0 if _vh1 > 0 else 0
                _v_ratio = _v5 / _h1_avg if _h1_avg > 0 else 0
                _bs = (cached_trend or {}).get('buys_m5', 0) / max((cached_trend or {}).get('sells_m5', 1), 1)

                # Dying: decisive death — reject (large dump + low volume + sellers dominate)
                _is_dying = (_pc_m5 < -5.0 and _v_ratio < 0.7 and _bs < 0.8)
                # Accumulating: shallow dip with stable vol/bs — extend, don't reject
                _is_accumulating = (
                    _pc_m5 >= -5.0
                    and 0.7 <= _v_ratio <= 1.5
                    and _bs >= 0.85
                )

                if _is_dying:
                    log.info(
                        f"[SmartEntry] ${symbol} REJECT trend_dying: "
                        f"pc_m5={_pc_m5:+.1f}% vol_ratio={_v_ratio:.2f} bs={_bs:.2f}"
                    )
                    return False, 'trend_dying', (
                        f'pc_m5={_pc_m5:+.1f}% vol_ratio={_v_ratio:.2f} bs={_bs:.2f}'
                    ), None

                if _is_accumulating and not bearish_extended:
                    bearish_extended = True
                    max_wait_sec = SMART_ENTRY_MAX_WAIT_SEC + 600
                    log.info(
                        f"[SmartEntry] ${symbol} ACCUMULATING — extending wait by 600s "
                        f"(pc_m5={_pc_m5:+.1f}% vol_ratio={_v_ratio:.2f} bs={_bs:.2f})"
                    )
                    time.sleep(interval)
                    continue

                # Neither clearly dying nor clearly accumulating, or already extended
                return False, 'trend_bearish_timeout', trend_reason, None
            time.sleep(interval)
            continue

        if trend_phase == 'FAKE_PUMP':
            fake_pump_count += 1  # P5: increment FAKE_PUMP counter
            log.info(
                f"[SmartEntry] ${symbol} round {round_num} FAKE_PUMP: {trend_reason} "
                f"(fp_count={fake_pump_count}) ({elapsed:.0f}s elapsed)"
            )
            time.sleep(interval)
            continue

        if trend_phase == 'WAIT':
            if round_num % 6 == 0:  # Log every 60s
                log.info(
                    f"[SmartEntry] ${symbol} round {round_num} WAIT: {trend_reason} "
                    f"({elapsed:.0f}s elapsed)"
                )
            time.sleep(interval)
            continue

        # --- trend_phase == 'BULLISH' → check momentum direct entry first ---

        # Track pc_m5 peak for momentum fading detection
        if cached_trend:
            _cur_pc_m5 = cached_trend.get('price_change_m5', 0)
            if _cur_pc_m5 > pc_m5_peak:
                pc_m5_peak = _cur_pc_m5

        # P5: If too many FAKE_PUMPs accumulated, require stricter validation
        if fake_pump_count >= SMART_ENTRY_FAKE_PUMP_THRESHOLD and cached_trend:
            bs_check = cached_trend.get('buys_m5', 0) / max(cached_trend.get('sells_m5', 1), 1)
            if bs_check < 2.0:
                log.info(
                    f"[SmartEntry] ${symbol} round {round_num} BLOCKED: "
                    f"fake_pump_history={fake_pump_count} requires buy_sell>=2.0, "
                    f"got {bs_check:.2f} ({elapsed:.0f}s)"
                )
                time.sleep(interval)
                continue

        # Momentum Direct Entry: for parabolic movers that never pull back
        # Uses DexScreener pc_m5 (5-min price change) — proven effective.
        # NOTE: velocity-based approach (vel_30s/vel_60s) was tested twice
        # (Apr 15 + Apr 16) and both times was too strict: only 1-2/14+ trades passed.
        # pc_m5 > 15% is the validated working threshold.
        if cached_trend:
            pc_m5 = cached_trend.get('price_change_m5', 0)
            b_m5 = cached_trend.get('buys_m5', 0)
            s_m5 = max(cached_trend.get('sells_m5', 1), 1)
            bs_ratio = b_m5 / s_m5

            # Check if data actually changed from last round (prevent stale cache counting)
            current_data_key = (round(pc_m5, 1), b_m5, s_m5)
            data_is_fresh = (current_data_key != last_momentum_data)
            last_momentum_data = current_data_key

            # Multi-factor chase protection (replaces fixed pc_m5 cutoffs)
            _chasing, _chase_reason = is_chasing_top(cached_trend)
            if _chasing:
                consecutive_momentum_rounds = 0  # reset — money leaving, don't chase
            elif pc_m5 > 15.0 and bs_ratio > 1.4 and data_is_fresh:
                # bs threshold raised from 1.0 → 1.4 based on audit (31 trades):
                # momentum_direct with bs 1.0-1.37 was 0W/5L (SOLASTER bs=1.33, FLASH bs=1.47 borderline)
                # Only MISA bs=1.37 won but was borderline. bs>=1.4 filters weak-buyer entries.
                consecutive_momentum_rounds += 1
            elif not (pc_m5 > 15.0 and bs_ratio > 1.4):
                consecutive_momentum_rounds = 0
            # If data not fresh but still bullish → don't increment, don't reset

            # Re-entries need stronger confirmation: 5 rounds instead of 3
            # Data: ETH re-entry#2 via momentum_direct → -6.5% (3 rounds wasn't enough)
            _min_momentum_rounds = 5 if entry_count >= 1 else 3
            min_momentum_wait = 10 if (bs_ratio >= 5.0 and pc_m5 > 20) else 60
            if (consecutive_momentum_rounds >= _min_momentum_rounds
                    and elapsed > min_momentum_wait
                    and price and price > 0):
                detail_str = (
                    f"price_m5={pc_m5:+.1f}% buy_sell={bs_ratio:.2f} "
                    f"consecutive={consecutive_momentum_rounds} "
                    f"waited={elapsed:.0f}s trend={trend_reason}"
                )
                log.info(
                    f"[SmartEntry] 🚀 ${symbol} MOMENTUM_ENTRY at ${price:.10f}: {detail_str}"
                )
                return True, 'momentum_direct_entry', detail_str, price

        # Track strongest trend phase seen (real_buying > moderate_buying)
        is_real = 'real_buying' in trend_reason
        if best_trend_phase is None:
            best_trend_phase = 'real_buying' if is_real else 'moderate_buying'
        elif is_real:
            best_trend_phase = 'real_buying'

        # Trend downgrade detection: was real_buying, now moderate_buying
        trend_downgraded = (best_trend_phase == 'real_buying' and not is_real)

        # --- Layer 2: Entry Position (Scheme A) ---
        if not price or price <= 0:
            time.sleep(interval)
            continue

        position, detail = evaluate_entry_position(price_history, price)

        if position == 'GOOD_ENTRY':
            # Guard -1: momentum fading — pc_m5 dropped >50% from its peak
            # Data: GREKT pc_m5 +20%→+5.8% → -7.0%, ADHD +13%→+1.7% → -4.4%
            # Momentum is dying even though snapshot says BULLISH.
            if cached_trend and pc_m5_peak >= 10.0:
                _cur_pc = cached_trend.get('price_change_m5', 0)
                if _cur_pc < pc_m5_peak * 0.5:
                    if round_num % 6 == 0:
                        log.info(
                            f"[SmartEntry] ${symbol} round {round_num} REJECTED: "
                            f"momentum_fading pc_m5 peaked at {pc_m5_peak:+.1f}%, "
                            f"now {_cur_pc:+.1f}% (dropped >{50}%) ({elapsed:.0f}s)"
                        )
                    time.sleep(interval)
                    continue

            # Guard 0: minimum data points — too few points = noise, not a real pattern
            # Data: GREKT 4pt/10s → -7.7% instant SL. All winners had 6+ points.
            if len(price_history) < SMART_ENTRY_MIN_POINTS:
                if round_num % 6 == 0:
                    log.info(
                        f"[SmartEntry] ${symbol} round {round_num} WAIT: "
                        f"n_points={len(price_history)} < {SMART_ENTRY_MIN_POINTS} "
                        f"(need more data) ({elapsed:.0f}s)"
                    )
                time.sleep(interval)
                continue

            pullback = detail.get('pullback_depth_pct', 0)
            bounce = detail.get('bounce_from_low_pct', 0)
            bounce_ratio = bounce / pullback if pullback > 0 else 0

            # Guard 1: adaptive bounce_ratio — threshold depends on signal strength
            # Strong signals (all green) get lower bar; weak signals keep strict 30%
            _br_threshold = SMART_ENTRY_MIN_BOUNCE_RATIO  # default 30%
            _br_tier = 'default'
            if cached_trend:
                _br_bs = cached_trend.get('buys_m5', 0) / max(cached_trend.get('sells_m5', 1), 1)
                _br_vol_m5 = cached_trend.get('vol_m5', 0)
                _br_vol_h1 = cached_trend.get('vol_h1', 0)
                _br_h1_avg = _br_vol_h1 / 12.0 if _br_vol_h1 > 0 else 0
                _br_vol_ratio = _br_vol_m5 / _br_h1_avg if _br_h1_avg > 0 else (5.0 if _br_vol_m5 > 0 else 0)
                _br_is_real = 'real_buying' in trend_reason

                if _br_bs >= 1.5 and _br_vol_ratio >= 2.0 and _br_is_real:
                    _br_threshold = SMART_ENTRY_BOUNCE_RATIO_STRONG  # 15%
                    _br_tier = 'strong'
                elif _br_bs >= 1.3 and _br_vol_ratio >= 1.5:
                    _br_threshold = SMART_ENTRY_BOUNCE_RATIO_MEDIUM  # 20%
                    _br_tier = 'medium'

            if bounce_ratio < _br_threshold:
                if round_num % 6 == 0:
                    log.info(
                        f"[SmartEntry] ${symbol} round {round_num} REJECTED: "
                        f"bounce_ratio={bounce_ratio:.0%} < {_br_threshold:.0%} "
                        f"(bounce={bounce:.1f}% pullback={pullback:.1f}%) "
                        f"tier={_br_tier} ({elapsed:.0f}s)"
                    )
                time.sleep(interval)
                continue
            # Log when adaptive tier actually helped (non-default)
            if _br_tier != 'default':
                log.info(
                    f"[SmartEntry] ${symbol} adaptive_bounce threshold={_br_threshold:.0%} "
                    f"tier={_br_tier} bounce_ratio={bounce_ratio:.0%} → PASS"
                )

            # Guard 1b: volume ratio — reject low-volume bounces
            # Data: Buddy vol_ratio=0.8 → instant -18.8% crash. No volume = no support.
            _cur_vol_ratio = 0
            if cached_trend:
                _v5 = cached_trend.get('vol_m5', 0)
                _vh1 = cached_trend.get('vol_h1', 0)
                _h1_avg = _vh1 / 12 if _vh1 > 0 else 0
                _cur_vol_ratio = _v5 / _h1_avg if _h1_avg > 0 else (5.0 if _v5 > 0 else 0)
            _min_vol = SMART_ENTRY_REENTRY_VOL_RATIO if entry_count > 0 else SMART_ENTRY_MIN_VOL_RATIO
            if _cur_vol_ratio < _min_vol:
                if round_num % 6 == 0:
                    log.info(
                        f"[SmartEntry] ${symbol} round {round_num} REJECTED: "
                        f"vol_ratio={_cur_vol_ratio:.1f} < {_min_vol} "
                        f"(low volume, no support) ({elapsed:.0f}s)"
                    )
                time.sleep(interval)
                continue

            # Guard 2: trend downgrade — buying pressure fading
            if trend_downgraded:
                log.info(
                    f"[SmartEntry] ${symbol} round {round_num} REJECTED: "
                    f"trend downgraded real_buying→moderate_buying, "
                    f"buying pressure fading ({elapsed:.0f}s)"
                )
                time.sleep(interval)
                continue

            # Guard 3: overextension — coin already pumped too much
            # If vel_60s > 20%/min, the "pullback bounce" is likely a crash beginning
            if len(price_history) >= 4:
                _now = time.time()
                _pts_60 = [(t, p) for t, p in price_history if _now - t <= 60 and p > 0]
                if len(_pts_60) >= 2:
                    _dt = (_pts_60[-1][0] - _pts_60[0][0]) / 60.0
                    if _dt > 0:
                        _vel_60 = ((_pts_60[-1][1] - _pts_60[0][1]) / _pts_60[0][1] * 100) / _dt
                        if _vel_60 > 20.0:
                            log.info(
                                f"[SmartEntry] ${symbol} round {round_num} REJECTED: "
                                f"overextended vel_60s={_vel_60:+.1f}%/min > 20%/min, "
                                f"pullback likely crash not bounce ({elapsed:.0f}s)"
                            )
                            time.sleep(interval)
                            continue

            trigger_price = price
            detail_str = (
                f"pullback={pullback:.1f}% "
                f"bounce={bounce:.1f}% "
                f"bounce_ratio={bounce_ratio:.0%} "
                f"below_high={detail['below_high_pct']:.1f}% "
                f"trend={trend_reason} "
                f"n_points={detail['n_points']} "
                f"waited={elapsed:.0f}s"
            )
            log.info(
                f"[SmartEntry] ✅ ${symbol} GOOD_ENTRY at ${trigger_price:.10f}: {detail_str}"
            )
            return True, 'smart_entry_pullback_bounce', detail_str, trigger_price

        # Not a good entry yet — log and keep polling
        if round_num % 6 == 0:  # Log every 60s
            log.info(
                f"[SmartEntry] ${symbol} round {round_num} BULLISH but {position}: "
                f"pullback={detail.get('pullback_depth_pct', '?')}% "
                f"bounce={detail.get('bounce_from_low_pct', '?')}% "
                f"trend={trend_reason} ({elapsed:.0f}s)"
            )

        time.sleep(interval)

    # Timeout — could not find a good entry in 15 minutes
    elapsed = time.time() - start_time
    return False, 'smart_entry_timeout', f'no_good_entry_in_{elapsed:.0f}s', None
