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

def evaluate_smart_entry(token_ca, symbol='?', pool_address=None, entry_count=0,
                         momentum_snapshots=None, momentum_pct=0):
    """
    Smart Entry Engine (V5 — 84% Period Hybrid)
    
    Restores the proven 84% win rate entry logic:
      PATH 1 (Primary): Pullback-Bounce — wait for 2% pullback, then 2% bounce
      PATH 2 (Secondary): Momentum Direct — 9s momentum with data-driven guards
    
    Key differences from V4 (momentum-only):
      - Pullback-bounce is checked FIRST (84% period's primary entry method)
      - bs threshold lowered to > 1.0 (84% original, was >= 1.2 in V4)
      - Only 2 guards for pullback path (bounce_ratio + trend_downgrade)
      - Data-driven guards (m9s cap, pc_m5 band) applied to momentum path only
    
    Parameters:
        momentum_snapshots: list of [p1, p2, p3] from the 3×3s momentum check
        momentum_pct: net % change over the 9s window (p3-p1)/p1*100

    Returns: (should_enter: bool, reason: str, detail: str, trigger_price: float|None)
    """
    from paper_trade_monitor import fetch_realtime_price
    from entry_engine import fetch_dexscreener_trend_snapshot, is_chasing_top, evaluate_trend_phase

    # 1. Gather current price and DexScreener data
    price, src, age_ms = fetch_realtime_price(token_ca, pool_address)
    cached_trend = fetch_dexscreener_trend_snapshot(token_ca)
    
    if not price or price <= 0:
        return False, 'no_price', 'could not fetch price', None

    # 2. Parse DexScreener data
    bs_ratio = 1.0
    pc_m5 = 0
    if cached_trend:
        b_m5 = cached_trend.get('buys_m5', 0)
        s_m5 = max(cached_trend.get('sells_m5', 1), 1)
        bs_ratio = b_m5 / s_m5
        pc_m5 = cached_trend.get('price_change_m5', 0)

    # 3. Check anti-chase guard (simplified — only >100% hard ceiling)
    _chasing = False
    if cached_trend:
        _chasing, _chase_reason = is_chasing_top(cached_trend)

    # 4. Layer 1: Determine trend phase
    trend_phase, trend_reason = evaluate_trend_phase(cached_trend)

    # ══════════════════════════════════════════════════════════════════
    # PATH 1 (PRIMARY): PULLBACK-BOUNCE ENTRY
    # 84% win rate period's main entry method.
    # Logic: price ran up → pulled back ≥2% → bounced ≥2% from low
    # → still below high (not chasing) → buy the confirmed dip.
    #
    # Guards (84% period had only 2):
    #   1. bounce_ratio >= 25%
    #   2. trend must be BULLISH (not downgraded)
    # ══════════════════════════════════════════════════════════════════
    if momentum_snapshots and len(momentum_snapshots) >= 3:
        snap_high = max(momentum_snapshots)
        snap_low = min(momentum_snapshots)
        snap_last = momentum_snapshots[-1]

        if snap_high > 0 and snap_low > 0 and snap_last > 0:
            pullback_pct = ((snap_high - snap_low) / snap_high) * 100
            bounce_pct = ((snap_last - snap_low) / snap_low) * 100
            below_high_pct = ((snap_high - snap_last) / snap_high) * 100
            bounce_ratio_val = bounce_pct / pullback_pct if pullback_pct > 0 else 0

            if (pullback_pct >= SMART_ENTRY_MIN_PULLBACK_PCT
                    and bounce_pct >= SMART_ENTRY_MIN_BOUNCE_PCT
                    and below_high_pct >= SMART_ENTRY_MIN_PULLBACK_PCT
                    and bounce_ratio_val >= SMART_ENTRY_MIN_BOUNCE_RATIO):

                # Guard 1: bounce_ratio must be >= 25%
                if bounce_ratio_val < 0.25:
                    detail_str = (
                        f"pullback={pullback_pct:.1f}% bounce={bounce_pct:.1f}% "
                        f"ratio={bounce_ratio_val:.0%} < 25%"
                    )
                    return False, 'bounce_ratio_weak', detail_str, None

                # Guard 2: trend must be BULLISH
                if trend_phase not in ('BULLISH',):
                    detail_str = (
                        f"pullback={pullback_pct:.1f}% bounce={bounce_pct:.1f}% "
                        f"ratio={bounce_ratio_val:.0%} but trend={trend_phase}"
                    )
                    return False, 'trend_not_bullish', detail_str, None

                # Chase check
                if _chasing:
                    return False, 'chasing_top', f'pullback-bounce but {_chase_reason}', None

                # ✅ GOOD_ENTRY — pullback-bounce confirmed
                # 1s direction confirmation before buying
                import time as _time
                _time.sleep(1.0)
                price_confirm, _, _ = fetch_realtime_price(token_ca, pool_address)
                
                if price_confirm and price_confirm > 0:
                    if price_confirm < snap_last * 0.99:
                        direction_pct = ((price_confirm - snap_last) / snap_last) * 100
                        detail_str = (
                            f"pullback-bounce confirmed but direction_1s={direction_pct:+.1f}% "
                            f"(price fell {snap_last:.10f}→{price_confirm:.10f})"
                        )
                        log.info(f"[SmartEntry] ⚠️ ${symbol} PB_REVERSAL: {detail_str}")
                        return False, 'pb_momentum_reversing', detail_str, None
                    trigger_price = price_confirm
                else:
                    trigger_price = snap_last

                detail_str = (
                    f"PULLBACK_BOUNCE: pullback={pullback_pct:.1f}% bounce={bounce_pct:.1f}% "
                    f"ratio={bounce_ratio_val:.0%} below_high={below_high_pct:.1f}% "
                    f"trend={trend_phase} bs={bs_ratio:.2f} pc_m5={pc_m5:+.1f}%"
                )
                log.info(f"[SmartEntry] 🚀 ${symbol} GOOD_ENTRY at ${trigger_price:.10f}: {detail_str}")
                return True, 'pullback_bounce_entry', detail_str, trigger_price

    # ══════════════════════════════════════════════════════════════════
    # PATH 2 (SECONDARY): MOMENTUM DIRECT ENTRY
    # When no pullback-bounce is available, use 9s momentum data.
    # bs threshold: > 1.0 (84% period original, was >= 1.2 in V4).
    # Data-driven guards from audit (m9s cap, pc_m5 band) still apply.
    # ══════════════════════════════════════════════════════════════════
    if momentum_snapshots and len(momentum_snapshots) >= 2:
        m_pct = momentum_pct
        m_last = momentum_snapshots[-1]
        
        # Base threshold: m9s >= 1.5% AND bs > 1.0 (84% period value)
        if m_pct >= 1.5 and bs_ratio > 1.0:
            if _chasing:
                return False, 'chasing_top', f'momentum +{m_pct:.1f}% but chasing top', None

            # ── DATA-DRIVEN GUARD 1: Momentum Upper Bound ──
            # Audit (26 trades): All 8 trades with m9s > 3.5% lost (0% win rate).
            _M9S_UPPER = 3.5
            if m_pct > _M9S_UPPER:
                detail_str = (
                    f"momentum_9s=+{m_pct:.1f}% EXCEEDS {_M9S_UPPER}% cap "
                    f"(violent spike, likely distribution) bs={bs_ratio:.2f} pc_m5={pc_m5:+.1f}%"
                )
                log.info(f"[SmartEntry] 🚫 ${symbol} M9S_CAP_BLOCK: {detail_str}")
                return False, 'm9s_cap_exceeded', detail_str, None

            # ── DATA-DRIVEN GUARD 2: pc_m5 Sweet-Spot Band ──
            # Audit (26 trades):
            #   pc_m5 < 10%:  9 trades, 0 wins — trend too weak
            #   pc_m5 10-20%: 8 trades, 3 wins (38%) — only profitable band
            #   pc_m5 > 20%:  8 trades, 0 wins — overheated
            _PC_M5_LOW = 10.0
            _PC_M5_HIGH = 20.0
            if pc_m5 < _PC_M5_LOW:
                detail_str = (
                    f"pc_m5={pc_m5:+.1f}% BELOW {_PC_M5_LOW}% floor "
                    f"(trend not established) m9s=+{m_pct:.1f}% bs={bs_ratio:.2f}"
                )
                log.info(f"[SmartEntry] 🚫 ${symbol} PC_M5_LOW_BLOCK: {detail_str}")
                return False, 'pc_m5_too_low', detail_str, None

            if pc_m5 > _PC_M5_HIGH:
                detail_str = (
                    f"pc_m5={pc_m5:+.1f}% EXCEEDS {_PC_M5_HIGH}% ceiling "
                    f"(overheated, distribution phase) m9s=+{m_pct:.1f}% bs={bs_ratio:.2f}"
                )
                log.info(f"[SmartEntry] 🚫 ${symbol} PC_M5_HIGH_BLOCK: {detail_str}")
                return False, 'pc_m5_overheated', detail_str, None

            # ── DIRECTION CONFIRMATION (1s) ──
            import time as _time
            _time.sleep(1.0)
            price_confirm, _, _ = fetch_realtime_price(token_ca, pool_address)
            
            if price_confirm and price_confirm > 0:
                direction_pct = ((price_confirm - m_last) / m_last) * 100
                
                if price_confirm < m_last * 0.99:
                    detail_str = (
                        f"momentum_9s=+{m_pct:.1f}% bs={bs_ratio:.2f} "
                        f"but direction_1s={direction_pct:+.1f}% (price fell from "
                        f"{m_last:.10f} to {price_confirm:.10f})"
                    )
                    log.info(f"[SmartEntry] ⚠️ ${symbol} MOMENTUM_REVERSAL: {detail_str}")
                    return False, 'momentum_reversing', detail_str, None
                
                trigger_price = price_confirm
            else:
                trigger_price = m_last

            detail_str = (
                f"momentum_9s=+{m_pct:.1f}% buy_sell={bs_ratio:.2f} "
                f"pc_m5={pc_m5:+.1f}% snaps=[{', '.join(f'{s:.10f}' for s in momentum_snapshots)}]"
            )
            log.info(f"[SmartEntry] 🚀 ${symbol} MOMENTUM_ENTRY at ${trigger_price:.10f}: {detail_str}")
            return True, 'momentum_direct_entry', detail_str, trigger_price

        else:
            detail_str = (
                f"momentum_9s=+{m_pct:.1f}% (need ≥1.5%) "
                f"bs={bs_ratio:.2f} (need >1.0)"
            )
            return False, 'momentum_weak', detail_str, None

    # Fallback: no momentum data — use DexScreener pc_m5
    # 84% period: pc_m5 > 15% + bs > 1.0
    if cached_trend and pc_m5 > 15.0 and bs_ratio > 1.0:
        if not _chasing:
            detail_str = f"price_m5={pc_m5:+.1f}% buy_sell={bs_ratio:.2f} (dex_fallback)"
            log.info(f"[SmartEntry] 🚀 ${symbol} MOMENTUM_ENTRY at ${price:.10f}: {detail_str}")
            return True, 'momentum_direct_entry', detail_str, price

    return False, 'no_entry_signal', 'no pullback-bounce pattern and no momentum signal', None


