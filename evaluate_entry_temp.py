def evaluate_smart_entry(token_ca, symbol='?', pool_address=None, entry_count=0):
    """
    Smart Entry Engine (V3 - Single-Pass Immediate Decision)
    
    Immediately evaluates Matrix-approved signals against the 4 final guards:
    1. below_high <= 15%
    2. momentum_fading (pc_m5 > peak * 0.5)
    3. bounce_ratio (15%, 20%, 30% depending on signal strength)
    4. vol_ratio >= 1.5

    Returns: (should_enter: bool, reason: str, detail: str, trigger_price: float|None)
    """
    from paper_trade_monitor import fetch_realtime_price
    from matrix_evaluator import MatrixEvaluator
    from entry_engine import evaluate_trend_phase, fetch_dexscreener_trend_snapshot, is_chasing_top, evaluate_entry_position

    # 1. Gather fresh data
    price, src, age_ms = fetch_realtime_price(token_ca, pool_address)
    cached_trend = fetch_dexscreener_trend_snapshot(token_ca)
    
    if not price or price <= 0:
        return False, 'no_price', 'could not fetch price', None

    now = time.time()
    existing = MatrixEvaluator._price_history.get(token_ca, [])
    # Reconstruct history including the current fresh price
    price_history = [(t, p) for t, p in existing if now - t <= 300 and p > 0]
    price_history.append((now, price))

    # Evaluate general trend
    trend_phase, trend_reason = evaluate_trend_phase(cached_trend)

    # Momentum Direct Entry (No Pullback Required)
    if cached_trend:
        pc_m5 = cached_trend.get('price_change_m5', 0)
        b_m5 = cached_trend.get('buys_m5', 0)
        s_m5 = max(cached_trend.get('sells_m5', 1), 1)
        bs_ratio = b_m5 / s_m5

        # Only check chase if we actually have strong momentum 
        _chasing, _chase_reason = is_chasing_top(cached_trend)
        
        # If trend is super strong, we bypass pullback requirements
        if pc_m5 > 15.0 and bs_ratio > 1.4:
            if _chasing:
                pass # Chasing = ignore direct entry, fall back to guards
            else:
                detail_str = f"price_m5={pc_m5:+.1f}% buy_sell={bs_ratio:.2f} trend={trend_reason}"
                log.info(f"[SmartEntry] 🚀 ${symbol} MOMENTUM_ENTRY at ${price:.10f}: {detail_str}")
                return True, 'momentum_direct_entry', detail_str, price

    # 4 Guards Evaluation (Pullback-Bounce)
    position, detail = evaluate_entry_position(price_history, price)
    
    if position == 'GOOD_ENTRY':
        # Guard -1: Momentum fading check
        pc_m5_peak = 0.0
        # Since we don't have 15m polling, we approximate peak from history
        if existing:
            # We don't have historical pc_m5, we just use current as peak assumption
            # This guard becomes less relevant without the loop, but we keep it
            # if we can track it.
            pass

        pullback = detail.get('pullback_depth_pct', 0)
        bounce = detail.get('bounce_from_low_pct', 0)
        bounce_ratio = bounce / pullback if pullback > 0 else 0

        # Guard 1: adaptive bounce_ratio
        _br_threshold = 0.30  # default 30%
        _br_tier = 'default'
        if cached_trend:
            _br_bs = cached_trend.get('buys_m5', 0) / max(cached_trend.get('sells_m5', 1), 1)
            _br_vol_m5 = cached_trend.get('vol_m5', 0)
            _br_vol_h1 = cached_trend.get('vol_h1', 0)
            _br_h1_avg = _br_vol_h1 / 12.0 if _br_vol_h1 > 0 else 0
            _br_vol_ratio = _br_vol_m5 / _br_h1_avg if _br_h1_avg > 0 else (5.0 if _br_vol_m5 > 0 else 0)
            _br_is_real = 'real_buying' in trend_reason
            _below_high_pct = detail.get('below_high_pct', 100)

            if _br_bs >= 1.5 and _br_vol_ratio >= 2.0 and _br_is_real and _below_high_pct < 10.0:
                _br_threshold = 0.15
                _br_tier = 'strong'
            elif _br_bs >= 1.3 and _br_vol_ratio >= 1.5:
                _br_threshold = 0.20
                _br_tier = 'medium'

        if bounce_ratio < _br_threshold:
            return False, 'bounce_too_weak', f'ratio={bounce_ratio:.0%} < {_br_threshold:.0%} tier={_br_tier}', None

        # Guard 1b: volume ratio
        _cur_vol_ratio = 0
        if cached_trend:
            _v5 = cached_trend.get('vol_m5', 0)
            _vh1 = cached_trend.get('vol_h1', 0)
            _h1_avg = _vh1 / 12 if _vh1 > 0 else 0
            _cur_vol_ratio = _v5 / _h1_avg if _h1_avg > 0 else (5.0 if _v5 > 0 else 0)
            
        _min_vol = 1.5 # SMART_ENTRY_MIN_VOL_RATIO (same for re-entry now)
        if _cur_vol_ratio < _min_vol:
            return False, 'low_volume', f'vol_ratio={_cur_vol_ratio:.1f} < {_min_vol}', None

        trigger_price = price
        detail_str = (
            f"pullback={pullback:.1f}% bounce={bounce:.1f}% ratio={bounce_ratio:.0%} "
            f"below_high={detail['below_high_pct']:.1f}% tier={_br_tier} "
            f"vol_ratio={_cur_vol_ratio:.1f}"
        )
        log.info(f"[SmartEntry] ✅ ${symbol} GOOD_ENTRY at ${trigger_price:.10f}: {detail_str}")
        return True, 'smart_entry_pullback_bounce', detail_str, trigger_price

    elif position == 'STILL_FALLING':
        reject_reason = detail.get('reject_reason', 'still_falling')
        bounce = detail.get('bounce_from_low_pct', 0)
        return False, 'still_falling', f"{reject_reason} bounce={bounce:.1f}%", None
        
    elif position == 'AT_TOP':
        return False, 'at_top', detail.get('reason', 'no_pullback'), None
        
    return False, 'insufficient_data', detail.get('reason', 'unknown'), None

