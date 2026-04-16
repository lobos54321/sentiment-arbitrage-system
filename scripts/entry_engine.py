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

# Only use trades after this date for Kelly historical data
# (old trades used -15% SL, now we use -7.5% — old avg_loss is not representative)
KELLY_DATA_CUTOFF = '2026-04-15 12:00:00'

# P3: Historical odds cache
_kelly_trade_cache = {'wins': [], 'losses': [], 'last_refresh': 0}

# ─── SmartEntry Constants ─────────────────────────────────────────────────────
_dex_trend_cache = {}
SMART_ENTRY_DEX_CACHE_SEC = 15       # Reuse DexScreener data for 15 seconds
SMART_ENTRY_POLL_INTERVAL_SEC = 10   # Poll price every 10 seconds
SMART_ENTRY_MAX_WAIT_SEC = 900       # 15-minute maximum wait
SMART_ENTRY_MIN_PULLBACK_PCT = 2.0   # Minimum pullback depth to qualify
SMART_ENTRY_MIN_BOUNCE_PCT = 2.0     # Minimum bounce from low to confirm
SMART_ENTRY_MIN_BOUNCE_RATIO = 0.25  # bounce/pullback must be >= 25% (avoid dead cat bounce)
SMART_ENTRY_FAKE_PUMP_THRESHOLD = 10  # After N fake_pump rounds, require stricter entry (buy_sell>=2.0)

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
            # Filter: only use trades after cutoff (old -15% SL data is not representative)
            trades = [t for t in trades
                      if t.get('closed_at', '') >= KELLY_DATA_CUTOFF]
            cache['wins'] = [t['exit_pnl'] for t in trades if t['exit_pnl'] and t['exit_pnl'] > 0]
            cache['losses'] = [abs(t['exit_pnl']) for t in trades if t['exit_pnl'] and t['exit_pnl'] < 0]
            cache['last_refresh'] = now
            log.info(
                f"[Kelly] Historical data refreshed: {len(cache['wins'])} wins, "
                f"{len(cache['losses'])} losses from last 50 trades"
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

    # ─── Entry mode adjustment (data-validated: 62% vs 43% win rate) ──
    if entry_mode == 'pullback_bounce':
        p *= 0.85
        log.info(f"[Kelly] pullback_bounce entry → p×0.85 → p={p:.3f}")

    # ─── Matrix crowding (avg pnl validated: ≤1 perfect=+5%, 2 perfect=-1.1%) ──
    if matrix_scores:
        perfect_count = sum(1 for v in matrix_scores.values() if v == 100)
        if perfect_count >= 4:
            p *= 0.7    # heavy crowding penalty
            log.info(f"[Kelly] Matrix crowding: {perfect_count}/5 perfect → p×0.7")
        elif perfect_count >= 3:
            p *= 0.9    # mild penalty
        elif perfect_count <= 1:
            p *= 1.2    # contrarian bonus — less crowded
            log.info(f"[Kelly] Matrix contrarian: {perfect_count}/5 perfect → p×1.2")

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

    # Hard limits: min 0.03 SOL, max 20% of capital
    pos = round(max(0.03, min(position, base_capital * 0.20)), 3)
    log.info(f"[Kelly] f*={kelly_f:.3f} → {pos} SOL | p={p:.3f} b={b:.2f} mode={entry_mode or 'default'}")
    return pos


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


def evaluate_smart_entry(token_ca, symbol='?', pool_address=None):
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

    max_rounds = int(SMART_ENTRY_MAX_WAIT_SEC / SMART_ENTRY_POLL_INTERVAL_SEC)
    interval = SMART_ENTRY_POLL_INTERVAL_SEC  # 10s

    price_history = []  # local to this evaluation
    last_dex_check = 0
    cached_trend = None
    start_time = time.time()
    best_trend_phase = None  # Track the strongest trend phase seen so far
    consecutive_momentum_rounds = 0  # Track consecutive strong momentum rounds
    last_momentum_data = None  # Track last DexScreener data to detect stale cache
    fake_pump_count = 0  # P5: accumulate FAKE_PUMP rounds

    log.info(
        f"[SmartEntry] ${symbol} starting smart entry evaluation "
        f"(max {SMART_ENTRY_MAX_WAIT_SEC}s, poll {interval}s)"
    )

    for round_num in range(1, max_rounds + 1):
        elapsed = time.time() - start_time

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
        # If price consistently surging with buyers in control, enter directly
        # IMPORTANT: Only count when DexScreener data actually refreshed
        # (avoid counting same cached data 3 times as "3 consecutive")
        if cached_trend:
            pc_m5 = cached_trend.get('price_change_m5', 0)
            b_m5 = cached_trend.get('buys_m5', 0)
            s_m5 = max(cached_trend.get('sells_m5', 1), 1)
            bs_ratio = b_m5 / s_m5

            # Check if data actually changed from last round
            current_data_key = (round(pc_m5, 1), b_m5, s_m5)
            data_is_fresh = (current_data_key != last_momentum_data)
            last_momentum_data = current_data_key

            if pc_m5 > 15.0 and bs_ratio > 1.0 and data_is_fresh:
                consecutive_momentum_rounds += 1
            elif not (pc_m5 > 15.0 and bs_ratio > 1.0):
                consecutive_momentum_rounds = 0
            # If data not fresh but still bullish → don't increment, don't reset

            # Dynamic min wait based on signal strength:
            # Extreme (buy_sell≥5 + pc_m5>20%): 10s — buyer domination, no need to wait
            # Normal: 60s — let the momentum develop
            min_momentum_wait = 10 if (bs_ratio >= 5.0 and pc_m5 > 20) else 60
            if (consecutive_momentum_rounds >= 3
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
            pullback = detail.get('pullback_depth_pct', 0)
            bounce = detail.get('bounce_from_low_pct', 0)
            bounce_ratio = bounce / pullback if pullback > 0 else 0

            # Guard 0: real-time buy/sell confirmation — buyers must still be present
            # L1 passed BULLISH (buy_sell≥1.2) but cached data can be up to 15s old;
            # verify at execution time that buy pressure hasn't dried up (≥1.0 is lenient)
            if cached_trend:
                _bs_now = cached_trend.get('buys_m5', 0) / max(cached_trend.get('sells_m5', 1), 1)
                if _bs_now < 1.0:
                    if round_num % 6 == 0:
                        log.info(
                            f"[SmartEntry] ${symbol} round {round_num} REJECTED: "
                            f"buy_sell={_bs_now:.2f} < 1.0 at GOOD_ENTRY — buyers dried up "
                            f"({elapsed:.0f}s)"
                        )
                    time.sleep(interval)
                    continue

            # Guard 1: bounce/pullback ratio — reject dead cat bounces
            if bounce_ratio < SMART_ENTRY_MIN_BOUNCE_RATIO:
                if round_num % 6 == 0:
                    log.info(
                        f"[SmartEntry] ${symbol} round {round_num} REJECTED: "
                        f"bounce_ratio={bounce_ratio:.0%} < {SMART_ENTRY_MIN_BOUNCE_RATIO:.0%} "
                        f"(bounce={bounce:.1f}% pullback={pullback:.1f}%) ({elapsed:.0f}s)"
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
            # (Normie: vel_60s ≈ 23%/min, +45% in 5min → pullback_bounce → instant -27%)
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
