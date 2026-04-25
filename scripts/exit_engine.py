#!/usr/bin/env python3
"""
Exit Engine — Guardian Thread + Exit Processing

Extracted from paper_trade_monitor.py for modularity.
All sell-side monitoring and execution logic lives here.
"""

import time
import logging
import threading

log = logging.getLogger('paper_trade_monitor')


# ─── Plan #3: Dynamic Stop-Loss (4-factor) ────────────────────────────────────
# Adjusts the base SL per-tick based on live market state instead of using
# a fixed -7.5% threshold for every position. Designed to:
#   - widen when accumulation signals are intact (bs>=1.5, vol>=2x, mom up)
#   - tighten when sellers dominate (bs<0.7, vol<0.5x, mom down)
#   - protect realized peak gains (never give back >15pp from a peak >20%)
# Bounded to [-15%, -3%] so neither runaway widening nor over-tightening.

def compute_dynamic_sl(pos, dex_trend, base_sl=-0.15):
    """4-factor dynamic SL centered around -15% (84% win rate period baseline).
    pos:        Position object (uses .price_ring, .peak_pnl)
    dex_trend:  DexScreener trend snapshot dict (or None)
    base_sl:    starting SL (negative float, default -0.15)
    Returns float in range [-0.20, -0.10].
    """
    sl = base_sl

    # Factor 1: buy/sell ratio
    if dex_trend:
        bs = dex_trend.get('buys_m5', 0) / max(dex_trend.get('sells_m5', 1), 1)
        if bs >= 1.5:
            sl -= 0.03   # widen (more negative) — buyers dominate, give room
        elif bs < 0.7:
            sl += 0.03   # tighten — sellers dominate

        # Factor 2: volume trend (vol_m5 vs h1_avg)
        v5 = dex_trend.get('vol_m5', 0)
        vh1 = dex_trend.get('vol_h1', 0)
        h1_avg = vh1 / 12.0 if vh1 > 0 else 0
        v_ratio = v5 / h1_avg if h1_avg > 0 else 1.0
        if v_ratio >= 2.0:
            sl -= 0.02   # widen — surge in interest
        elif v_ratio < 0.5:
            sl += 0.03   # tighten — interest collapsing

    # Factor 3: 3-bar momentum direction from price_ring
    ring = list(getattr(pos, 'price_ring', []))
    if len(ring) >= 4:
        last_4 = [p for _, p in ring[-4:]]
        rises = sum(1 for i in range(1, 4) if last_4[i] > last_4[i-1])
        falls = sum(1 for i in range(1, 4) if last_4[i] < last_4[i-1])
        if rises == 3:
            sl -= 0.02   # 3 strict up bars — widen
        elif falls == 3:
            sl += 0.02   # 3 strict down bars — tighten

    # Factor 4: peak protection (sub-trail zone only)
    peak = getattr(pos, 'peak_pnl', 0) or 0
    if peak > 0.10:
        sl = max(sl, -0.08)  # tighten to -8% once we've seen +10% (protect gains)

    # Bounds: centered on -15%, range [-20%, -10%]
    # 84% period used fixed -15%. Dynamic factors adjust ±5pp around that.
    return max(-0.20, min(-0.10, sl))


# ─── EXIT Guardian Thread ─────────────────────────────────────────────────────
# Independent thread that monitors all positions every 3 seconds.
# Never blocked by SmartEntry or watchlist scanning.
# ─────────────────────────────────────────────────────────────────────────────

class ExitGuardianThread(threading.Thread):
    """Independent thread that monitors all positions every 3 seconds.
    Never blocked by SmartEntry or watchlist scanning.

    Checks:
    1. Hard stop-loss (emergency exit)
    2. Moon Bag trail floor (emergency exit)
    3. Breakeven stop for moon bags
    """

    def __init__(self, positions_ref, positions_lock, watchlist_store_ref,
                 exit_queue, fetch_price_fn, simulate_exit_fn=None):
        super().__init__(daemon=True, name='exit-guardian')
        self.positions = positions_ref      # shared dict reference
        self.lock = positions_lock          # threading.Lock
        self.store = watchlist_store_ref     # WatchlistStore instance
        self.exit_queue = exit_queue         # list to push exit signals (checked by main loop)
        self.exit_queue_lock = threading.Lock()
        self._exit_pending = set()           # trade_ids already queued (dedup)
        self.fetch_price = fetch_price_fn   # fetch_realtime_price function
        self.simulate_exit = simulate_exit_fn  # simulate_exit_execution (for instant SL quotes)
        self.interval = 3  # seconds
        self._running = True

    def run(self):
        log.info("[ExitGuardian] 🛡️ Started — monitoring positions every 3s")
        while self._running:
            try:
                self._check_all_positions()
            except Exception as e:
                log.error(f"[ExitGuardian] Error: {e}", exc_info=True)
            time.sleep(self.interval)

    def stop(self):
        self._running = False

    def _get_instant_quote(self, pos, ca):
        if not self.simulate_exit: return None
        try:
            _sell_amount = int(float(pos.token_amount_raw)) if pos.token_amount_raw else 0
            _instant_sim = self.simulate_exit(
                ca, str(_sell_amount),
                getattr(pos, 'token_decimals', 0) or 0,
                pos.strategy_stage
            )
            log.info(f"[ExitGuardian] ⚡ {pos.symbol} instant quote: {_instant_sim.get('quotedOutputSOL', '?') if _instant_sim else 'FAIL'}")
            return _instant_sim
        except Exception as _e:
            log.warning(f"[ExitGuardian] {pos.symbol} instant quote failed: {_e}")
            return None

    def get_pending_exits(self):
        """Retrieve and clear pending exit signals (called by main loop)."""
        with self.exit_queue_lock:
            exits = list(self.exit_queue)
            self.exit_queue.clear()
            self._exit_pending.clear()
            return exits

    @staticmethod
    def _calc_velocity(ring, window_sec):
        """Price velocity in %/minute over window_sec from ring buffer."""
        now = time.time()
        pts = [(t, p) for t, p in ring if now - t <= window_sec]
        if len(pts) < 2:
            return 0.0
        dt_min = (pts[-1][0] - pts[0][0]) / 60.0
        if dt_min <= 0:
            return 0.0
        return ((pts[-1][1] - pts[0][1]) / pts[0][1] * 100) / dt_min

    @staticmethod
    def _calc_tick_volatility(ring):
        """Mean absolute tick-to-tick change as volume proxy."""
        prices = [p for _, p in ring]
        if len(prices) < 3:
            return 0.0
        changes = [abs(prices[i] - prices[i-1]) / prices[i-1]
                    for i in range(1, len(prices))]
        return sum(changes) / len(changes)

    def _check_all_positions(self):
        # Take snapshot under lock
        with self.lock:
            snapshot = list(self.positions.items())

        if not snapshot:
            return

        for trade_id, pos in snapshot:
            try:
                ca = pos.token_ca
                pool = pos.pool_address
                entry_price = pos.entry_price
                if not ca or not entry_price or entry_price <= 0:
                    continue

                # Skip positions we already queued an exit for (dedup)
                if trade_id in self._exit_pending:
                    continue

                # Fetch current price
                price, src, age_ms = self.fetch_price(ca, pool)
                if not price or price <= 0:
                    continue

                pnl = (price - entry_price) / entry_price

                # --- Get watchlist entry for dynamic_sl ---
                w_entry = self.store.get_by_ca(ca)
                # Plan #3: 4-factor dynamic SL (bs/vol/momentum/peak)
                # Base SL: -15% (84% win rate period). w_entry['dynamic_sl'] overrides.
                base_sl = -0.15
                if w_entry:
                    base_sl = w_entry.get('dynamic_sl', -0.15)
                # Lazy import to avoid circular dependency
                try:
                    from entry_engine import fetch_dexscreener_trend_snapshot
                    _dex_trend = fetch_dexscreener_trend_snapshot(ca)
                except Exception:
                    _dex_trend = None
                hard_sl = compute_dynamic_sl(pos, _dex_trend, base_sl=base_sl)

                # === Hard Stop Loss (Double-Tap Confirmation) ===
                # P0 Fix: A single bad price read from Redis killed Coco (+73% → -20.8%).
                # Now we require TWO consecutive price checks to confirm SL breach.
                # If first check triggers SL, wait 1s and re-fetch. Only proceed if both agree.
                if pnl <= hard_sl:
                    first_price = price
                    first_pnl = pnl
                    first_src = src
                    log.info(
                        f"[ExitGuardian] ⚠️ {pos.symbol} SL CHECK #1: "
                        f"pnl={pnl*100:+.1f}% <= SL={hard_sl*100:.1f}% "
                        f"price={price:.10f} src={src} — confirming in 0.3s..."
                    )

                    # Second check after short delay (0.3s, not 1.0s)
                    # Data: 1.0s delay cost avg -2.1% extra slippage on SL exits.
                    # 0.3s is enough to detect Redis price glitches but reduces crash exposure.
                    time.sleep(0.3)
                    price2, src2, age_ms2 = self.fetch_price(ca, pool)

                    if not price2 or price2 <= 0:
                        log.warning(
                            f"[ExitGuardian] {pos.symbol} SL CONFIRM FAILED: "
                            f"second price fetch returned None, holding position"
                        )
                        continue

                    pnl2 = (price2 - entry_price) / entry_price

                    # Check price divergence between the two reads
                    price_divergence = abs(price2 - first_price) / max(first_price, 1e-15)

                    if pnl2 > hard_sl:
                        # Second check says NOT in SL territory → price glitch, skip exit
                        log.warning(
                            f"[ExitGuardian] 🛡️ {pos.symbol} SL CANCELLED — price glitch detected! "
                            f"Check#1: pnl={first_pnl*100:+.1f}% price={first_price:.10f} src={first_src} | "
                            f"Check#2: pnl={pnl2*100:+.1f}% price={price2:.10f} src={src2} | "
                            f"divergence={price_divergence:.1%} — holding position"
                        )
                        continue

                    # Both checks confirm SL breach → proceed with exit
                    # Use the BETTER (higher) price of the two for the trigger
                    confirmed_price = max(first_price, price2)
                    confirmed_pnl = (confirmed_price - entry_price) / entry_price

                    log.info(
                        f"[ExitGuardian] 🚨 {pos.symbol} EMERGENCY SL CONFIRMED: "
                        f"Check#1: pnl={first_pnl*100:+.1f}% src={first_src} | "
                        f"Check#2: pnl={pnl2*100:+.1f}% src={src2} | "
                        f"divergence={price_divergence:.1%} — executing exit"
                    )
                    # Instant exit quote: don't wait for main loop (was 30-94s delay!)
                    # Data: drone SL at 19:25:58 but CLOSED at 19:26:29 (31s later, -31%)
                    #       ROCCO SL at 19:51:12 but CLOSED at 19:52:46 (94s later, -39%)
                    _instant_sim = self._get_instant_quote(pos, ca)
                    with self.exit_queue_lock:
                        self.exit_queue.append({
                            'trade_id': trade_id,
                            'symbol': pos.symbol,
                            'reason': f'guardian_hard_sl ({confirmed_pnl:.1%} <= {hard_sl:.1%})',
                            'trigger_price': confirmed_price,
                            'trigger_pnl': confirmed_pnl,
                            '_instant_sim': _instant_sim,  # pre-fetched quote
                        })
                    self._exit_pending.add(trade_id)
                    continue

                # === Update peak_pnl in real-time (critical for trail accuracy) ===
                # BUG FIX: Sync peak between Guardian (pos.peak_pnl) and ExitMatrix (w_entry['peak_pnl'])
                # Without this, they diverge and Guardian uses stale peak → wrong trail floor
                if w_entry:
                    w_peak = w_entry.get('peak_pnl', 0) or 0
                    if w_peak > pos.peak_pnl:
                        pos.peak_pnl = w_peak  # read ExitMatrix's peak
                if pnl > pos.peak_pnl:
                    pos.peak_pnl = pnl
                    pos.peak_ts = time.time()  # A3: record when peak was achieved
                if w_entry and pos.peak_pnl > (w_entry.get('peak_pnl', 0) or 0):
                    w_entry['peak_pnl'] = pos.peak_pnl  # write back for ExitMatrix

                # === B+C: Record price into ring buffer for velocity calc ===
                pos.price_ring.append((time.time(), price))

                # === Compute real-time velocity + tick_volatility ===
                raw_vel_30s = self._calc_velocity(pos.price_ring, 30)
                vel_60s = self._calc_velocity(pos.price_ring, 60)
                tick_vol = self._calc_tick_volatility(pos.price_ring)

                # Multi-round velocity smoothing (3 rounds × 3s = 9s average)
                pos.vel_history.append(raw_vel_30s)
                smoothed_vel = sum(pos.vel_history) / len(pos.vel_history) if pos.vel_history else 0.0

                # Extreme crash bypass: vel < -10%/min → use raw, don't wait for avg
                # Data: ROTJAK crashed -24%/min, NOTOKEN -13%/min; normal noise is -0.3~-1%/min
                use_vel = raw_vel_30s if raw_vel_30s < -10.0 else smoothed_vel

                # Read Helius TPS from watchlist (written by main loop)
                tps_smooth = 0.0
                if w_entry:
                    tps_smooth = w_entry.get('_helius_tps', 0) or 0

                # Write velocity to Position object (shared with main loop)
                # w_entry is a separate DB copy — writing here is lost
                pos._guardian_velocity = use_vel
                pos._guardian_tick_vol = tick_vol

                # Initialize threat tighten for first-check safety.
                # If any code above threw an exception on a prior iteration, this
                # attribute would never have been written → AttributeError cascade.
                _threat_tighten = getattr(pos, '_guardian_threat_tighten', 0)

                # === COORDINATED THREAT SCORE SYSTEM ===
                # Three factors work together: each adds threat points.
                # Combined score tightens trail floor (2pp per point, max 6pp).
                # Adapted for meme coins: lower peak threshold (5% vs 10%),
                # shorter stale time (30/60s vs 120s), fewer flat ticks (4 vs 8).
                # Data: old factors triggered 0 times in 31 trades because
                # peak>=10%+120s was unreachable in fast meme markets.
                _threat_score = 0
                _threat_reasons = []
                _decay_factor = 1.0  # kept for downstream compat, no longer used independently

                # Factor 1: TIME_DECAY — peak stale (not making new highs)
                _time_since_peak = time.time() - getattr(pos, 'peak_ts', pos.entry_ts)
                if pos.peak_pnl >= 0.05:
                    if _time_since_peak > 60:
                        _threat_score += 2
                        _threat_reasons.append(f'stale_60s({_time_since_peak:.0f}s)')
                    elif _time_since_peak > 30:
                        _threat_score += 1
                        _threat_reasons.append(f'stale_30s({_time_since_peak:.0f}s)')

                # Factor 2: FLAT-TOP — price flat at high level
                if pos.peak_pnl >= 0.05 and len(pos.price_ring) >= 4:
                    _recent_prices = [p for _, p in list(pos.price_ring)[-4:]]
                    _p_min, _p_max = min(_recent_prices), max(_recent_prices)
                    _flat_range = (_p_max - _p_min) / _p_min if _p_min > 0 else 0
                    # Track initial tick_vol for decay detection
                    _initial_tvol = getattr(pos, '_initial_tick_vol', None)
                    if _initial_tvol is None and tick_vol > 0:
                        pos._initial_tick_vol = tick_vol
                        _initial_tvol = tick_vol
                    _tvol_decay = (tick_vol / _initial_tvol) if _initial_tvol and _initial_tvol > 0 else 1.0

                    if _flat_range < 0.003 and _tvol_decay < 0.50:
                        _threat_score += 2  # flat + volume dying = distribution
                        _threat_reasons.append(f'flat+vol_dead(range={_flat_range*100:.2f}%,tvol={_tvol_decay:.0%})')
                    elif _flat_range < 0.005:
                        _threat_score += 1  # just flat, volume ok
                        _threat_reasons.append(f'flat(range={_flat_range*100:.2f}%)')

                # Factor 3: THIN POOL — small liquidity pool
                _liq_usd = (w_entry.get('_dex_liquidity_usd') or 0) if w_entry else 0
                if 0 < _liq_usd < 10000:
                    _threat_score += 2
                    _threat_reasons.append(f'tiny_pool(${_liq_usd:,.0f})')
                elif 0 < _liq_usd < 20000:
                    _threat_score += 1
                    _threat_reasons.append(f'small_pool(${_liq_usd:,.0f})')

                # Combined trail tightening: 2pp per point, max 6pp
                _threat_tighten = min(_threat_score * 0.02, 0.06)
                if _threat_score > 0:
                    log.info(
                        f"[ExitGuardian] ⚠️ {pos.symbol} THREAT={_threat_score} "
                        f"tighten={_threat_tighten*100:.0f}pp "
                        f"reasons=[{', '.join(_threat_reasons)}] "
                        f"peak={pos.peak_pnl*100:+.1f}%"
                    )
                # Share full threat score with EXIT_MATRIX (includes FLAT-TOP
                # which EXIT_MATRIX can't compute because it lacks price_ring)
                pos._guardian_threat_tighten = _threat_tighten

                # === Trail Floor Check (3s, velocity+volume driven, FULL RANGE) ===
                # ATH Fast Lane: three-phase — mirrors matrix_evaluator logic
                is_moon = w_entry and w_entry.get('status') == 'moon_bag'
                _is_ath_entry = w_entry and (w_entry.get('type') == 'ATH' or w_entry.get('signal_type') == 'ATH')

                if _is_ath_entry and is_moon:
                    # === ATH Phase 3 Moon Bag: absolute -40pp trail ===
                    moon_peak = pos.peak_pnl
                    moon_floor = moon_peak - 0.40
                    if moon_floor > 0 and pnl < moon_floor:
                        log.info(
                            f"[ExitGuardian] 📉 {pos.symbol} ATH PHASE3 MOON TRAIL: "
                            f"pnl={pnl*100:+.1f}% < floor={moon_floor*100:.1f}% "
                            f"(peak={moon_peak*100:.1f}%, -40pp abs) price={price:.10f} src={src}"
                        )
                        with self.exit_queue_lock:
                            self.exit_queue.append({
                                'trade_id': trade_id,
                                'symbol': pos.symbol,
                                'reason': f'guardian_ath_phase3_moon (pnl={pnl:.1%} < floor={moon_floor:.1%}, peak={moon_peak:.1%}, -40pp)',
                                'trigger_price': price,
                                'trigger_pnl': pnl,
                            '_instant_sim': self._get_instant_quote(pos, ca),

                            })
                        self._exit_pending.add(trade_id)
                        continue

                elif _is_ath_entry and not is_moon:
                    # === Velocity factor for ALL ATH trail phases ===
                    # Tighten margins when velocity signals crash/fade
                    if raw_vel_30s < -5.0:
                        _vel_tighten = 0.70   # CRASH → 30% tighter margins
                    elif use_vel < -2.0:
                        _vel_tighten = 0.85   # fading → 15% tighter
                    else:
                        _vel_tighten = 1.0    # neutral/rising → no change

                    # === ATH Phase 2 (50-100%): absolute -20pp trail ===
                    if pos.peak_pnl >= 0.50:
                        _base_margin = 0.20 * _decay_factor * _vel_tighten
                        trail_floor = pos.peak_pnl - _base_margin + _threat_tighten
                        if pnl < trail_floor:
                            log.info(
                                f"[ExitGuardian] 📉 {pos.symbol} ATH PHASE2 TRAIL: "
                                f"pnl={pnl*100:+.1f}% < floor={trail_floor*100:.1f}% "
                                f"(peak={pos.peak_pnl*100:.1f}%, -20pp+threat{_threat_tighten*100:.0f}pp) price={price:.10f} src={src}"
                            )
                            with self.exit_queue_lock:
                                self.exit_queue.append({
                                    'trade_id': trade_id,
                                    'symbol': pos.symbol,
                                    'reason': f'guardian_ath_phase2 (pnl={pnl:.1%} < floor={trail_floor:.1%}, peak={pos.peak_pnl:.1%}, -20pp)',
                                    'trigger_price': price,
                                    'trigger_pnl': pnl,
                                '_instant_sim': self._get_instant_quote(pos, ca),

                                })
                            self._exit_pending.add(trade_id)
                            continue
                    # === ATH Phase 1 (peak < 50%): tiered trail protection ===
                    # Synced with matrix_evaluator Phase1 tiers (fixes DUCK +20.6% → -15.8% bug)
                    if pos.peak_pnl >= 0.25:
                        _base_margin = 0.15 * _decay_factor * _vel_tighten
                        trail_floor = pos.peak_pnl - _base_margin + _threat_tighten
                        if pnl < trail_floor:
                            log.info(
                                f"[ExitGuardian] 📉 {pos.symbol} ATH PHASE1 TRAIL_25: "
                                f"pnl={pnl*100:+.1f}% < floor={trail_floor*100:.1f}% "
                                f"(peak={pos.peak_pnl*100:.1f}%, -15pp+liq) price={price:.10f} src={src}"
                            )
                            with self.exit_queue_lock:
                                self.exit_queue.append({
                                    'trade_id': trade_id,
                                    'symbol': pos.symbol,
                                    'reason': f'guardian_ath_phase1_trail_25 (pnl={pnl:.1%} < floor={trail_floor:.1%}, peak={pos.peak_pnl:.1%}, -15pp)',
                                    'trigger_price': price,
                                    'trigger_pnl': pnl,
                                '_instant_sim': self._get_instant_quote(pos, ca),

                                })
                            self._exit_pending.add(trade_id)
                            continue
                    elif pos.peak_pnl >= 0.15:
                        _base_margin = 0.10 * _decay_factor * _vel_tighten
                        trail_floor = pos.peak_pnl - _base_margin + _threat_tighten
                        if pnl < trail_floor:
                            log.info(
                                f"[ExitGuardian] 📉 {pos.symbol} ATH PHASE1 TRAIL_15: "
                                f"pnl={pnl*100:+.1f}% < floor={trail_floor*100:.1f}% "
                                f"(peak={pos.peak_pnl*100:.1f}%, -10pp+liq) price={price:.10f} src={src}"
                            )
                            with self.exit_queue_lock:
                                self.exit_queue.append({
                                    'trade_id': trade_id,
                                    'symbol': pos.symbol,
                                    'reason': f'guardian_ath_phase1_trail_15 (pnl={pnl:.1%} < floor={trail_floor:.1%}, peak={pos.peak_pnl:.1%}, -10pp)',
                                    'trigger_price': price,
                                    'trigger_pnl': pnl,
                                '_instant_sim': self._get_instant_quote(pos, ca),

                                })
                            self._exit_pending.add(trade_id)
                            continue
                    # === ATH Phase 1a: peak >= 8% — trail with -8pp ===
                    # Previously only crash_brake below 15%. FLASH peak=12.7% fell through
                    # to crash_brake which requires vel < -5%/min — slow declines bypassed it.
                    # Data: FLASH +12.7%→-11.5% (24.2%回吐), UNCEROID +7.9%→-0.7% (8.6%回吐)
                    elif pos.peak_pnl >= 0.08:
                        _base_margin = 0.08 * _decay_factor * _vel_tighten
                        trail_floor = pos.peak_pnl - _base_margin + _threat_tighten
                        if pnl < trail_floor:
                            log.info(
                                f"[ExitGuardian] 📉 {pos.symbol} ATH PHASE1 TRAIL_8: "
                                f"pnl={pnl*100:+.1f}% < floor={trail_floor*100:.1f}% "
                                f"(peak={pos.peak_pnl*100:.1f}%, -8pp+liq) price={price:.10f} src={src}"
                            )
                            with self.exit_queue_lock:
                                self.exit_queue.append({
                                    'trade_id': trade_id,
                                    'symbol': pos.symbol,
                                    'reason': f'guardian_ath_phase1_trail_8 (pnl={pnl:.1%} < floor={trail_floor:.1%}, peak={pos.peak_pnl:.1%}, -8pp)',
                                    'trigger_price': price,
                                    'trigger_pnl': pnl,
                                '_instant_sim': self._get_instant_quote(pos, ca),

                                })
                            self._exit_pending.add(trade_id)
                            continue

                    # === ATH Phase 1b: peak >= 5% — velocity+volume crash brake ===
                    # Previously "free run" — no protection at all. TripleUnch lost +11.9% → -6.9%.
                    # Now: use velocity and volume signals to detect crashes early.
                    if pos.peak_pnl >= 0.05:  # only if we've seen at least +5% peak
                        _ath_crash_exit = False
                        _ath_crash_reason = ''

                        # Crash brake 1: velocity-driven — rapid decline
                        if raw_vel_30s < -5.0 and pnl < pos.peak_pnl * 0.3:
                            # Crashing >5%/min AND lost >70% of peak → dump it
                            _ath_crash_exit = True
                            _ath_crash_reason = f'vel_crash (vel={raw_vel_30s:.1f}%/min, pnl={pnl:.1%} < 30% of peak={pos.peak_pnl:.1%})'
                        elif use_vel < -3.0 and pnl <= 0:
                            # Steady decline >3%/min AND back to breakeven → cut
                            _ath_crash_exit = True
                            _ath_crash_reason = f'vel_fade (vel={use_vel:.1f}%/min, pnl={pnl:.1%} <= 0, peak was {pos.peak_pnl:.1%})'
                        # Crash brake 2: volume death — tick_vol near zero while declining
                        elif tick_vol < 0.001 and pnl < pos.peak_pnl * 0.5 and len(pos.price_ring) >= 5:
                            _ath_crash_exit = True
                            _ath_crash_reason = f'vol_death (tick_vol={tick_vol:.4f}, pnl={pnl:.1%} < 50% of peak={pos.peak_pnl:.1%})'

                        if _ath_crash_exit:
                            log.info(
                                f"[ExitGuardian] 📉 {pos.symbol} ATH PHASE1 CRASH BRAKE: "
                                f"{_ath_crash_reason} price={price:.10f} src={src}"
                            )
                            with self.exit_queue_lock:
                                self.exit_queue.append({
                                    'trade_id': trade_id,
                                    'symbol': pos.symbol,
                                    'reason': f'guardian_ath_crash_brake ({_ath_crash_reason})',
                                    'trigger_price': price,
                                    'trigger_pnl': pnl,
                                '_instant_sim': self._get_instant_quote(pos, ca),

                                })
                            self._exit_pending.add(trade_id)
                            continue

                else:
                    # === Standard (non-ATH) Trail ===
                    # RACE GUARD: if peak >= 25% and not yet profit-locked, DON'T trail_stop here.
                    # Let ExitMatrix handle it with lock_profit (sell 50% + moon bag) instead of
                    # Guardian doing a full exit. Without this, Guardian fires trail_stop at peak=25%
                    # before ExitMatrix can create the moon bag → we sell 100% instead of 50%.
                    _has_locked = w_entry.get('_profit_locked', False) if w_entry else False
                    if pos.peak_pnl >= 0.25 and not is_moon and not _has_locked:
                        # Defer to ExitMatrix lock_profit — but still fire on CRASH velocity
                        if raw_vel_30s < -8.0 and pnl <= 0:
                            log.info(
                                f"[ExitGuardian] 🚨 {pos.symbol} CRASH override lock_profit defer: "
                                f"vel={raw_vel_30s:.1f} pnl={pnl:.1%} — full exit (crash > moon bag)"
                            )
                            # Fall through to trail logic below
                        else:
                            # Normal case: defer to ExitMatrix lock_profit
                            pass  # skip trail_stop entirely
                            continue
                    if pos.peak_pnl >= 0.05:
                        # Tiered base factor by peak level (synced with matrix_evaluator)
                        if pos.peak_pnl >= 0.50:
                            base_factor = 0.70   # >= +50% — about to become moon bag
                        elif pos.peak_pnl >= 0.20:
                            base_factor = 0.60
                        elif pos.peak_pnl >= 0.10:
                            base_factor = 0.55
                        else:
                            base_factor = 0.5

                        # Velocity-driven factor
                        if raw_vel_30s < -5.0:
                            vel_factor = 0.85    # CRASH → lock hard
                        elif use_vel < 0:
                            vel_factor = 0.75    # fading → tighten
                        elif use_vel > 10.0:
                            vel_factor = base_factor  # rocketing → use base
                        else:
                            vel_factor = 0.60    # neutral

                        # Volume signals
                        if tick_vol < 0.001 and len(pos.price_ring) >= 5:
                            vel_factor = max(vel_factor, 0.60)
                        if tps_smooth >= 0 and tps_smooth < 0.5 and len(pos.price_ring) >= 5:
                            vel_factor = max(vel_factor, 0.60)

                        trail_factor = max(base_factor, vel_factor)
                        trail_floor = pos.peak_pnl * trail_factor + _threat_tighten

                        if pnl < trail_floor:
                            log.info(
                                f"[ExitGuardian] 📉 {pos.symbol} TRAIL STOP: "
                                f"pnl={pnl*100:+.1f}% < floor={trail_floor*100:.1f}% "
                                f"(peak={pos.peak_pnl*100:.1f}% factor={trail_factor:.2f}) "
                                f"vel={use_vel:.1f}%/min(raw={raw_vel_30s:.1f}) "
                                f"tick_vol={tick_vol:.4f} tps={tps_smooth:.1f} "
                                f"price={price:.10f} src={src}"
                            )
                            with self.exit_queue_lock:
                                self.exit_queue.append({
                                    'trade_id': trade_id,
                                    'symbol': pos.symbol,
                                    'reason': f'guardian_trail_stop (pnl={pnl:.1%} < floor={trail_floor:.1%}, peak={pos.peak_pnl:.1%}, vel={use_vel:.1f}, tps={tps_smooth:.1f})',
                                    'trigger_price': price,
                                    'trigger_pnl': pnl,
                                '_instant_sim': self._get_instant_quote(pos, ca),

                                })
                            self._exit_pending.add(trade_id)
                            continue


                # === Profit Lock Detection (peak >= 20%, not yet locked) ===
                # Don't execute the lock from Guardian (too complex: 50% sell + moon bag state).
                # Instead, just log it so the main loop picks it up on next eval.
                has_locked = w_entry.get('_profit_locked', False) if w_entry else False
                if not is_moon and not has_locked and pos.peak_pnl >= 0.20:
                    log.info(
                        f"[ExitGuardian] 🌙 {pos.symbol} PROFIT LOCK DETECTED: "
                        f"peak={pos.peak_pnl*100:.1f}% >= 20% — main loop will execute lock"
                    )

                # === Moon Bag Breakeven Stop ===
                if is_moon and price <= entry_price:
                    log.info(
                        f"[ExitGuardian] 🔔 {pos.symbol} MOON BREAKEVEN: "
                        f"price={price:.10f} <= entry={entry_price:.10f}"
                    )
                    with self.exit_queue_lock:
                        self.exit_queue.append({
                            'trade_id': trade_id,
                            'symbol': pos.symbol,
                            'reason': f'guardian_moon_breakeven (price <= entry)',
                            'trigger_price': price,
                            'trigger_pnl': pnl,
                        '_instant_sim': self._get_instant_quote(pos, ca),

                        })
                    self._exit_pending.add(trade_id)
                    continue

                # === Moon Bag Trail Floor ===
                if is_moon and w_entry:
                    moon_peak = max(w_entry.get('moon_peak_pnl', 0) or 0, pnl)
                    # Update peak in DB if needed
                    if pnl > (w_entry.get('moon_peak_pnl', 0) or 0):
                        self.store.update_position_state(w_entry['id'], moon_peak_pnl=pnl)

                    trail_factor = w_entry.get('moon_trail_factor', 0.2) or 0.2
                    moon_floor = moon_peak * trail_factor
                    if moon_peak > 0 and pnl < moon_floor:
                        log.info(
                            f"[ExitGuardian] 🔔 {pos.symbol} MOON TRAIL: "
                            f"pnl={pnl*100:+.1f}% < floor={moon_floor*100:.1f}% "
                            f"(peak={moon_peak*100:.1f}% factor={trail_factor})"
                        )
                        with self.exit_queue_lock:
                            self.exit_queue.append({
                                'trade_id': trade_id,
                                'symbol': pos.symbol,
                                'reason': f'guardian_moon_trail (pnl={pnl:.1%} < floor={moon_floor:.1%})',
                                'trigger_price': price,
                                'trigger_pnl': pnl,
                            '_instant_sim': self._get_instant_quote(pos, ca),

                            })
                        self._exit_pending.add(trade_id)

            except Exception as e:
                sym = getattr(pos, 'symbol', '?') if pos else '?'
                log.warning(f"[ExitGuardian] Check failed for {sym}: {e}")


def process_guardian_exits(exit_guardian, positions, lifecycles,
                          strategy_id, build_lifecycle_state_fn, simulate_exit_fn):
    """Process pending Guardian exit signals and return list of positions to close.

    This encapsulates the Guardian exit processing that was previously inline in
    the main loop — preventing bugs like the to_close=[] ordering issue.

    Args:
        exit_guardian: ExitGuardianThread instance
        positions: shared positions dict
        lifecycles: lifecycle state dict
        strategy_id: current strategy ID
        build_lifecycle_state_fn: function to build lifecycle state
        simulate_exit_fn: function to simulate exit execution

    Returns:
        list of dicts ready for the to_close processing pipeline
    """
    to_close = []
    guardian_exits = exit_guardian.get_pending_exits()

    for gx in guardian_exits:
        gx_trade_id = gx.get('trade_id')
        if gx_trade_id not in positions:
            continue

        gx_pos = positions[gx_trade_id]

        # === STALE EXIT GUARD ===
        # Guardian queues exits every 3s, but main loop may process them minutes later.
        # If price has recovered since the exit was queued, SKIP IT.
        # ROOT CAUSE: Republicans peaked +72% but a stale +4.3% trail_stop from 6 min ago
        # was still in the queue and got executed, killing a winning trade.
        if 'trail_stop' in gx.get('reason', '') and gx_pos.entry_price and gx_pos.entry_price > 0:
            current_peak = max(gx_pos.peak_pnl, 0)
            trigger_pnl = gx.get('trigger_pnl', 0)
            # If peak has moved significantly beyond the trigger, this exit is stale
            if current_peak > 0.05 and trigger_pnl < current_peak * 0.5:
                log.info(
                    f"  [GUARDIAN_EXIT] ⏭️ SKIPPING stale trail_stop for {gx['symbol']}: "
                    f"trigger_pnl={trigger_pnl*100:+.1f}% but current peak={current_peak*100:+.1f}% "
                    f"— price has recovered, exit no longer valid"
                )
                continue

        gx_lifecycle_id = gx_pos.lifecycle_id
        gx_lifecycle = lifecycles.setdefault(
            gx_lifecycle_id,
            build_lifecycle_state_fn(
                gx_lifecycle_id, gx_pos.token_ca, gx_pos.symbol,
                gx_pos.signal_ts,
                getattr(gx_pos, 'premium_signal_id', None),
                getattr(gx_pos, 'signal_type', None)
            )
        )
        log.info(
            f"  [GUARDIAN_EXIT] 🚨 Processing {gx['symbol']}: {gx['reason']} "
            f"trigger_pnl={gx.get('trigger_pnl', 0)*100:+.1f}%"
        )
        # Simulate exit execution — use instant quote if Guardian already got one
        gx_sell_amount = int(float(gx_pos.token_amount_raw)) if gx_pos.token_amount_raw else 0
        _instant_sim = gx.get('_instant_sim')
        if _instant_sim and _instant_sim.get('success'):
            # Guardian already got the quote at SL trigger time — use it (fresher!)
            gx_sim = _instant_sim
            log.info(f"  [GUARDIAN_EXIT] ⚡ Using instant quote for {gx['symbol']} (no delay)")
        else:
            # Fallback: get quote now (may be stale if main loop was blocked)
            gx_sim = simulate_exit_fn(
                gx_pos.token_ca, str(gx_sell_amount),
                getattr(gx_pos, 'token_decimals', 0) or 0,
                gx_pos.strategy_stage, strategy_id=strategy_id,
                lifecycle_id=gx_lifecycle_id
            )
        gx_trigger_pnl = gx.get('trigger_pnl', 0)
        to_close.append({
            'trade_id': gx_trade_id,
            'reason': gx['reason'],
            'pnl': gx_trigger_pnl,
            'trigger_pnl': gx_trigger_pnl,
            'exit_price': gx.get('trigger_price', gx_pos.entry_price),
            'exit_ts': int(time.time()),
            'mark_source': 'exit_guardian',
            'exit_eval': {
                'action': 'close',
                'execution': gx_sim if (gx_sim and gx_sim.get('success')) else {'success': True, 'synthetic': True},
            },
        })
        # IMPORTANT: Do NOT pop positions[gx_trade_id] here!
        # The main loop pipeline needs to get it to write DB logs.
        # It will be popped by close_position_as_... later.

    return to_close
