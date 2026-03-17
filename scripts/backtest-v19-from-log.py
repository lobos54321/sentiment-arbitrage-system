#!/usr/bin/env python3
"""
回测脚本: 基于 2026-03-17 16h 运行日志 + GeckoTerminal 真实K线
对比 v18 (旧策略) vs v19 (新策略: SL-35%, 动态TP1 45-60%)

对所有14个ATH信号代币进行模拟入场回测
"""

import urllib.request
import json
import ssl
import time
import csv
import sys
from datetime import datetime, timezone

# SSL context
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

# ==================== 代币数据（从日志解析） ====================
TOKENS = [
    # v18 PASS tokens (would have entered if had balance)
    {"symbol": "Gany-1", "ca": "BgtczEGgf9mZMcGBLi4J5Pn8PtmFy6Xz5uBKkMfspump",
     "entry_time": "2026-03-17T02:50:54Z", "entry_mc": 98200, "v18_pass": True},
    {"symbol": "Gany-2", "ca": "Dz4bX3snTDxqdKyZwdUgKoDvSjyvmoA23E6j5odZpump",
     "entry_time": "2026-03-17T03:00:26Z", "entry_mc": 49900, "v18_pass": True},
    {"symbol": "唐子", "ca": "ocB3t4czHwsueZk89YGBxEbisFLZ1tzvydpwbC9pump",
     "entry_time": "2026-03-17T03:52:45Z", "entry_mc": 78100, "v18_pass": True},
    # All other ATH tokens (filtered by v18, but backtest them too for reference)
    {"symbol": "TOKEN", "ca": "8P1PRDiJjSK8xA3zvaARSo2uTTj1nRagCGL1kD9Dpump",
     "entry_time": "2026-03-17T00:41:48Z", "entry_mc": 95500, "v18_pass": False},
    {"symbol": "HOSPICE", "ca": "6uq3r5mMQL6tKkJd9JpuA3bPbrqitpFfhSQDVPCMpump",
     "entry_time": "2026-03-17T01:29:26Z", "entry_mc": 124010, "v18_pass": False},
    {"symbol": "唐子-alt", "ca": "85NLap7dACtf6APMerHcMDb4iBsYRziT7F8xecfYpump",
     "entry_time": "2026-03-17T02:35:58Z", "entry_mc": 35760, "v18_pass": False},
    {"symbol": "Replacement", "ca": "7STZgGYW7HsVpZGdaCYfikLu2FuebbpqC7gwaP3Apump",
     "entry_time": "2026-03-17T02:41:42Z", "entry_mc": 43860, "v18_pass": False},
    {"symbol": "Democrats", "ca": "Vnm731Pin6BHsvvoTkBpLWcNM78NknXYAF3oyQWpump",
     "entry_time": "2026-03-17T03:11:40Z", "entry_mc": 351950, "v18_pass": False},
    {"symbol": "ARC", "ca": "2oBe59KhZ8s7pzYGbHNWVDK5RGSA2Yx9ufAe85TGpump",
     "entry_time": "2026-03-17T03:29:00Z", "entry_mc": 13380, "v18_pass": False},
    {"symbol": "Hyojo", "ca": "HfKNrf3VFYSzZfG3jS4pQEfjkHTxAwmD4wm1FUx8pump",
     "entry_time": "2026-03-17T03:42:41Z", "entry_mc": 49470, "v18_pass": False},
    {"symbol": "MINDLESS", "ca": "AJofCoVif3wj2Uy7mpzgxbqDyHPyn7xp6WzJBy7gpump",
     "entry_time": "2026-03-17T03:53:14Z", "entry_mc": 32470, "v18_pass": False},
    {"symbol": "Clove", "ca": "EAXGeuMf8xxkNQzqpLE4PRuzmyXNKSDTPKMQvzoDpump",
     "entry_time": "2026-03-17T04:09:17Z", "entry_mc": 25230, "v18_pass": False},
    {"symbol": "TITAN", "ca": "4ay158ynQu4RfKe4oEjQFd3om2Fvyguz1UnQMm3rpump",
     "entry_time": "2026-03-17T04:24:37Z", "entry_mc": 36360, "v18_pass": False},
    {"symbol": "pvpdog", "ca": "9UT2T4XPYAtiUuSBdgkHTBbCTcfUtDVUSdSkTev1pump",
     "entry_time": "2026-03-17T04:29:34Z", "entry_mc": 37760, "v18_pass": False},
]

POSITION_SIZE_SOL = 0.06

# ==================== GeckoTerminal K线获取 ====================

def get_pool_address(token_ca, retries=3):
    """获取代币在 Solana 上的主要交易池地址"""
    url = f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{token_ca}/pools?page=1"
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            })
            resp = urllib.request.urlopen(req, timeout=15, context=CTX)
            data = json.loads(resp.read())
            pools = data.get('data', [])
            # Pick first SOL pool (most liquid)
            for p in pools:
                name = p.get('attributes', {}).get('name', '')
                if 'SOL' in name:
                    return p['attributes']['address']
            # Fallback to first pool
            if pools:
                return pools[0]['attributes']['address']
            return None
        except Exception as e:
            if '429' in str(e):
                wait = 30 * (attempt + 1)
                print(f"    ⏳ 限流，等 {wait}s...")
                time.sleep(wait)
            else:
                print(f"    ❌ Pool查询失败: {e}")
                time.sleep(5)
    return None


def get_ohlcv(pool_address, timeframe="minute", aggregate=1, before_ts=None, limit=1000, retries=3):
    """获取K线数据 (1分钟级别)"""
    url = f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool_address}/ohlcv/{timeframe}"
    params = f"?aggregate={aggregate}&limit={limit}&currency=usd"
    if before_ts:
        params += f"&before_timestamp={before_ts}"

    for attempt in range(retries):
        try:
            req = urllib.request.Request(url + params, headers={
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            })
            resp = urllib.request.urlopen(req, timeout=20, context=CTX)
            data = json.loads(resp.read())
            ohlcv_list = data.get('data', {}).get('attributes', {}).get('ohlcv_list', [])
            return ohlcv_list
        except Exception as e:
            if '429' in str(e):
                wait = 30 * (attempt + 1)
                print(f"    ⏳ K线限流，等 {wait}s...")
                time.sleep(wait)
            else:
                print(f"    ❌ K线获取失败: {e}")
                time.sleep(5)
    return []


def fetch_candles_around_entry(pool_address, entry_time_str, hours_after=2):
    """获取入场时间前后的K线数据 (1分钟K线, 覆盖入场后2小时)"""
    entry_dt = datetime.fromisoformat(entry_time_str.replace('Z', '+00:00'))
    entry_ts = int(entry_dt.timestamp())

    # Get candles: entry time + 2 hours after
    end_ts = entry_ts + hours_after * 3600

    all_candles = []
    # GeckoTerminal returns candles BEFORE the timestamp, so we request from end_ts
    candles = get_ohlcv(pool_address, timeframe="minute", aggregate=1, before_ts=end_ts, limit=1000)
    if candles:
        all_candles.extend(candles)

    # Sort by timestamp ascending
    all_candles.sort(key=lambda x: x[0])

    # Filter: only candles from entry_ts onwards (and a few before for entry price)
    start_filter = entry_ts - 120  # 2 minutes before entry
    filtered = [c for c in all_candles if c[0] >= start_filter]

    return filtered, entry_ts


# ==================== 策略模拟器 ====================

def simulate_v18(candles, entry_ts, entry_price):
    """v18 旧策略: SL-40%, TP1@50%卖60%, 15分死水, 30分大限"""
    if not candles or not entry_price:
        return None

    pos = {
        'entry_price': entry_price,
        'tp1': False,
        'sold_pct': 0,
        'high_pnl': 0,
        'realized_sol': 0,  # 已实现的SOL
        'remaining_pct': 100,
        'exit_reason': None,
        'exit_pnl': None,
        'exit_time': None,
        # TP tracking
        'tp2': False, 'tp3': False, 'tp4': False,
        'moonbag': False, 'moonbag_high_pnl': 0,
    }

    for candle in candles:
        ts, o, h, l, c, vol = candle[0], candle[1], candle[2], candle[3], candle[4], candle[5] if len(candle) > 5 else 0
        if ts < entry_ts:
            continue

        hold_sec = ts - entry_ts
        hold_min = hold_sec / 60

        # Use close price for PnL calculation
        price = c
        pnl = ((price - entry_price) / entry_price) * 100

        # Track high PnL
        high_pnl_candle = ((h - entry_price) / entry_price) * 100
        if high_pnl_candle > pos['high_pnl']:
            pos['high_pnl'] = high_pnl_candle

        current_sl = 0 if pos['tp1'] else -40

        # 1. Stop Loss
        if not pos['moonbag'] and pnl <= current_sl:
            if pos['tp1']:
                pos['exit_reason'] = f"BREAKEVEN_SL(PnL{pnl:.0f}%)"
            else:
                pos['exit_reason'] = f"HARD_SL_40(PnL{pnl:.0f}%)"
            pos['exit_pnl'] = pnl
            pos['exit_time'] = ts
            break

        # 2. TP1: ≥50% → sell 60%
        if not pos['tp1'] and pnl >= 50:
            pos['tp1'] = True
            sell_amount = 0.60 * POSITION_SIZE_SOL * (1 + pnl / 100)
            pos['realized_sol'] += sell_amount
            pos['sold_pct'] = 60
            pos['remaining_pct'] = 40
            continue

        # 3. TP2: ≥100% → sell 15%
        if pos['tp1'] and not pos['tp2'] and pnl >= 100:
            pos['tp2'] = True
            sell_amount = 0.15 * POSITION_SIZE_SOL * (1 + pnl / 100)
            pos['realized_sol'] += sell_amount
            pos['sold_pct'] += 15
            pos['remaining_pct'] -= 15
            continue

        # 4. TP3: ≥200% → sell 15%
        if pos['tp2'] and not pos['tp3'] and pnl >= 200:
            pos['tp3'] = True
            sell_amount = 0.15 * POSITION_SIZE_SOL * (1 + pnl / 100)
            pos['realized_sol'] += sell_amount
            pos['sold_pct'] += 15
            pos['remaining_pct'] -= 15
            continue

        # 5. TP4: ≥500% → sell 5%, enter moonbag
        if pos['tp3'] and not pos['tp4'] and pnl >= 500:
            pos['tp4'] = True
            sell_amount = 0.05 * POSITION_SIZE_SOL * (1 + pnl / 100)
            pos['realized_sol'] += sell_amount
            pos['sold_pct'] += 5
            pos['remaining_pct'] -= 5
            pos['moonbag'] = True
            pos['moonbag_high_pnl'] = pnl
            continue

        # 6. Moonbag: drop 35% from peak → exit
        if pos['moonbag']:
            if pnl > pos['moonbag_high_pnl']:
                pos['moonbag_high_pnl'] = pnl
            moon_peak = pos['moonbag_high_pnl']
            drop_from_peak = moon_peak - pnl
            drop_pct = (drop_from_peak / moon_peak) * 100 if moon_peak > 0 else 0
            if drop_pct >= 35:
                remaining_value = (pos['remaining_pct'] / 100) * POSITION_SIZE_SOL * (1 + pnl / 100)
                pos['realized_sol'] += remaining_value
                pos['exit_reason'] = f"MOONBAG_EXIT(peak+{moon_peak:.0f}%,PnL+{pnl:.0f}%)"
                pos['exit_pnl'] = pnl
                pos['exit_time'] = ts
                break
            continue

        # 7. Dead water: 15min, peak<50%, -15%~+15%
        if not pos['tp1'] and hold_min >= 15:
            if pos['high_pnl'] < 50 and -15 <= pnl <= 15:
                pos['exit_reason'] = f"DEAD_WATER_15M(PnL{pnl:.0f}%,peak+{pos['high_pnl']:.0f}%)"
                pos['exit_pnl'] = pnl
                pos['exit_time'] = ts
                break

        # 8. Timeout: 30min without TP1
        if not pos['tp1'] and hold_min >= 30:
            pos['exit_reason'] = f"TIMEOUT_30M(PnL{pnl:.0f}%)"
            pos['exit_pnl'] = pnl
            pos['exit_time'] = ts
            break

    # If no exit triggered, use last candle
    if not pos['exit_reason'] and candles:
        last = candles[-1]
        price = last[4]
        pnl = ((price - entry_price) / entry_price) * 100
        pos['exit_reason'] = f"DATA_END(PnL{pnl:.0f}%)"
        pos['exit_pnl'] = pnl
        pos['exit_time'] = last[0]

    # Calculate total PnL
    if pos['exit_reason'] and 'DATA_END' not in pos['exit_reason']:
        if pos['remaining_pct'] > 0 and not pos['moonbag']:
            remaining_value = (pos['remaining_pct'] / 100) * POSITION_SIZE_SOL * (1 + pos['exit_pnl'] / 100)
            pos['realized_sol'] += remaining_value

    total_return = pos['realized_sol'] - POSITION_SIZE_SOL if pos['realized_sol'] > 0 else POSITION_SIZE_SOL * (pos.get('exit_pnl', 0) / 100)
    pos['total_pnl_sol'] = total_return
    pos['total_pnl_pct'] = (total_return / POSITION_SIZE_SOL) * 100

    return pos


def simulate_v19(candles, entry_ts, entry_price):
    """v19 新策略: SL-35%, 动态TP1 45-60%区间峰值, 15分死水(peak<45%), 30分大限"""
    if not candles or not entry_price:
        return None

    pos = {
        'entry_price': entry_price,
        'tp1': False,
        'tp1_zone_peak': 0,
        'sold_pct': 0,
        'high_pnl': 0,
        'realized_sol': 0,
        'remaining_pct': 100,
        'exit_reason': None,
        'exit_pnl': None,
        'exit_time': None,
        'tp2': False, 'tp3': False, 'tp4': False,
        'moonbag': False, 'moonbag_high_pnl': 0,
    }

    for candle in candles:
        ts, o, h, l, c, vol = candle[0], candle[1], candle[2], candle[3], candle[4], candle[5] if len(candle) > 5 else 0
        if ts < entry_ts:
            continue

        hold_sec = ts - entry_ts
        hold_min = hold_sec / 60

        price = c
        pnl = ((price - entry_price) / entry_price) * 100

        high_pnl_candle = ((h - entry_price) / entry_price) * 100
        if high_pnl_candle > pos['high_pnl']:
            pos['high_pnl'] = high_pnl_candle

        current_sl = 0 if pos['tp1'] else -35  # ← 改为-35%

        # 1. Stop Loss (-35% or breakeven)
        if not pos['moonbag'] and pnl <= current_sl:
            if pos['tp1']:
                pos['exit_reason'] = f"BREAKEVEN_SL(PnL{pnl:.0f}%)"
            else:
                pos['exit_reason'] = f"HARD_SL_35(PnL{pnl:.0f}%)"
            pos['exit_pnl'] = pnl
            pos['exit_time'] = ts
            break

        # 2. Dynamic TP1: 45-60% zone tracking
        if not pos['tp1'] and pnl >= 45:
            if pnl > pos['tp1_zone_peak']:
                pos['tp1_zone_peak'] = pnl

            peak_drop = pos['tp1_zone_peak'] - pnl
            # Trigger: breach 60% OR peak drop ≥5% within zone
            if pnl >= 60 or (pos['tp1_zone_peak'] >= 45 and peak_drop >= 5):
                pos['tp1'] = True
                # Sell at current price (after peak drop)
                sell_amount = 0.60 * POSITION_SIZE_SOL * (1 + pnl / 100)
                pos['realized_sol'] += sell_amount
                pos['sold_pct'] = 60
                pos['remaining_pct'] = 40
                pos['tp1_sell_pnl'] = pnl
                continue
            # Still rising, keep tracking
            continue

        # 3-6: Same as v18 (TP2/TP3/TP4/Moonbag)
        if pos['tp1'] and not pos['tp2'] and pnl >= 100:
            pos['tp2'] = True
            sell_amount = 0.15 * POSITION_SIZE_SOL * (1 + pnl / 100)
            pos['realized_sol'] += sell_amount
            pos['sold_pct'] += 15
            pos['remaining_pct'] -= 15
            continue

        if pos['tp2'] and not pos['tp3'] and pnl >= 200:
            pos['tp3'] = True
            sell_amount = 0.15 * POSITION_SIZE_SOL * (1 + pnl / 100)
            pos['realized_sol'] += sell_amount
            pos['sold_pct'] += 15
            pos['remaining_pct'] -= 15
            continue

        if pos['tp3'] and not pos['tp4'] and pnl >= 500:
            pos['tp4'] = True
            sell_amount = 0.05 * POSITION_SIZE_SOL * (1 + pnl / 100)
            pos['realized_sol'] += sell_amount
            pos['sold_pct'] += 5
            pos['remaining_pct'] -= 5
            pos['moonbag'] = True
            pos['moonbag_high_pnl'] = pnl
            continue

        if pos['moonbag']:
            if pnl > pos['moonbag_high_pnl']:
                pos['moonbag_high_pnl'] = pnl
            moon_peak = pos['moonbag_high_pnl']
            drop_from_peak = moon_peak - pnl
            drop_pct = (drop_from_peak / moon_peak) * 100 if moon_peak > 0 else 0
            if drop_pct >= 35:
                remaining_value = (pos['remaining_pct'] / 100) * POSITION_SIZE_SOL * (1 + pnl / 100)
                pos['realized_sol'] += remaining_value
                pos['exit_reason'] = f"MOONBAG_EXIT(peak+{moon_peak:.0f}%,PnL+{pnl:.0f}%)"
                pos['exit_pnl'] = pnl
                pos['exit_time'] = ts
                break
            continue

        # 7. Dead water: 15min, peak<45% (改为45%), -15%~+15%
        if not pos['tp1'] and hold_min >= 15:
            if pos['high_pnl'] < 45 and -15 <= pnl <= 15:
                pos['exit_reason'] = f"DEAD_WATER_15M(PnL{pnl:.0f}%,peak+{pos['high_pnl']:.0f}%)"
                pos['exit_pnl'] = pnl
                pos['exit_time'] = ts
                break

        # 8. Timeout: 30min without TP1
        if not pos['tp1'] and hold_min >= 30:
            pos['exit_reason'] = f"TIMEOUT_30M(PnL{pnl:.0f}%)"
            pos['exit_pnl'] = pnl
            pos['exit_time'] = ts
            break

    if not pos['exit_reason'] and candles:
        last = candles[-1]
        price = last[4]
        pnl = ((price - entry_price) / entry_price) * 100
        pos['exit_reason'] = f"DATA_END(PnL{pnl:.0f}%)"
        pos['exit_pnl'] = pnl
        pos['exit_time'] = last[0]

    if pos['exit_reason'] and 'DATA_END' not in pos['exit_reason']:
        if pos['remaining_pct'] > 0 and not pos['moonbag']:
            remaining_value = (pos['remaining_pct'] / 100) * POSITION_SIZE_SOL * (1 + pos['exit_pnl'] / 100)
            pos['realized_sol'] += remaining_value

    total_return = pos['realized_sol'] - POSITION_SIZE_SOL if pos['realized_sol'] > 0 else POSITION_SIZE_SOL * (pos.get('exit_pnl', 0) / 100)
    pos['total_pnl_sol'] = total_return
    pos['total_pnl_pct'] = (total_return / POSITION_SIZE_SOL) * 100

    return pos


# ==================== 主流程 ====================

def main():
    print("=" * 90)
    print("🔬 v18 vs v19 回测 | 基于 2026-03-17 运行日志 + GeckoTerminal 真实K线")
    print("=" * 90)
    print(f"v18: SL-40% | TP1@50%固定 | 死水peak<50%")
    print(f"v19: SL-35% | TP1@45-60%动态峰值 | 死水peak<45%")
    print(f"仓位: {POSITION_SIZE_SOL} SOL/笔")
    print()

    results = []

    for i, token in enumerate(TOKENS):
        symbol = token['symbol']
        ca = token['ca']
        v18_pass = token['v18_pass']
        tag = "✅ v18-PASS" if v18_pass else "❌ filtered"

        print(f"\n{'─'*70}")
        print(f"[{i+1}/{len(TOKENS)}] ${symbol} | {tag} | MC=${token['entry_mc']/1000:.1f}K")
        print(f"  CA: {ca}")
        print(f"  入场时间: {token['entry_time']}")

        # 1. Get pool address
        print(f"  📡 查询交易池...")
        time.sleep(2)  # Rate limit respect
        pool = get_pool_address(ca)
        if not pool:
            print(f"  ⚠️ 未找到交易池，跳过")
            results.append({
                'symbol': symbol, 'v18_pass': v18_pass,
                'error': 'no_pool', 'v18': None, 'v19': None
            })
            continue
        print(f"  ✅ Pool: {pool[:30]}...")

        # 2. Get OHLCV data
        print(f"  📊 获取1分钟K线...")
        time.sleep(3)  # Rate limit
        candles, entry_ts = fetch_candles_around_entry(pool, token['entry_time'], hours_after=2)

        if not candles or len(candles) < 5:
            print(f"  ⚠️ K线数据不足 ({len(candles) if candles else 0}根)，跳过")
            results.append({
                'symbol': symbol, 'v18_pass': v18_pass,
                'error': 'no_candles', 'v18': None, 'v19': None
            })
            continue

        # Find entry candle (closest to entry_ts)
        entry_candle = None
        for c in candles:
            if c[0] >= entry_ts:
                entry_candle = c
                break

        if not entry_candle:
            entry_candle = candles[-1]

        entry_price = entry_candle[4]  # close of entry candle

        # Calculate price range in the data
        prices = [c[4] for c in candles if c[0] >= entry_ts]
        if prices:
            max_pnl = ((max(prices) - entry_price) / entry_price) * 100
            min_pnl = ((min(prices) - entry_price) / entry_price) * 100
        else:
            max_pnl = min_pnl = 0

        duration_min = (candles[-1][0] - entry_ts) / 60 if candles else 0

        print(f"  ✅ {len(candles)}根K线 | 入场价: ${entry_price:.8f}")
        print(f"  📈 数据范围: 峰值+{max_pnl:.1f}% / 谷值{min_pnl:.1f}% | 覆盖{duration_min:.0f}分钟")

        # 3. Run both strategies
        v18_result = simulate_v18(candles, entry_ts, entry_price)
        v19_result = simulate_v19(candles, entry_ts, entry_price)

        if v18_result and v19_result:
            print(f"\n  {'策略':<8} {'退出原因':<40} {'PnL':>8} {'SOL收益':>10}")
            print(f"  {'─'*68}")
            print(f"  {'v18':<8} {v18_result['exit_reason']:<40} {v18_result['total_pnl_pct']:>+7.1f}% {v18_result['total_pnl_sol']:>+9.4f}")
            print(f"  {'v19':<8} {v19_result['exit_reason']:<40} {v19_result['total_pnl_pct']:>+7.1f}% {v19_result['total_pnl_sol']:>+9.4f}")

            diff = v19_result['total_pnl_pct'] - v18_result['total_pnl_pct']
            arrow = "🟢" if diff > 0 else "🔴" if diff < 0 else "⚪"
            print(f"  {arrow} v19 vs v18: {diff:+.1f}%")

            # Extra info for v19 TP1 zone tracking
            if v19_result.get('tp1_sell_pnl') is not None:
                print(f"  📌 v19 TP1区间峰值: +{v19_result.get('tp1_zone_peak', 0):.1f}% → 卖出时PnL: +{v19_result['tp1_sell_pnl']:.1f}%")

        results.append({
            'symbol': symbol, 'v18_pass': v18_pass,
            'entry_price': entry_price, 'max_pnl': max_pnl, 'min_pnl': min_pnl,
            'error': None, 'v18': v18_result, 'v19': v19_result
        })

        # Rate limit between tokens
        time.sleep(3)

    # ==================== 汇总 ====================
    print(f"\n\n{'='*90}")
    print("📊 回测汇总")
    print(f"{'='*90}")

    valid = [r for r in results if r['v18'] and r['v19']]
    v18_passed = [r for r in valid if r['v18_pass']]

    # All tokens summary
    print(f"\n📋 全部代币 ({len(valid)}/{len(TOKENS)}个有效):")
    print(f"{'代币':<12} {'v18Pass':>7} {'数据峰值':>10} {'v18 PnL':>10} {'v19 PnL':>10} {'差异':>8} {'v18退出':>25} {'v19退出':>25}")
    print("─" * 120)

    total_v18 = 0
    total_v19 = 0
    v18_wins = 0
    v19_wins = 0

    for r in valid:
        v18 = r['v18']
        v19 = r['v19']
        diff = v19['total_pnl_pct'] - v18['total_pnl_pct']
        marker = "✅" if r['v18_pass'] else "  "

        print(f"  {marker}{r['symbol']:<10} {'YES' if r['v18_pass'] else 'NO':>5} {r['max_pnl']:>+9.1f}% {v18['total_pnl_pct']:>+9.1f}% {v19['total_pnl_pct']:>+9.1f}% {diff:>+7.1f}% {v18['exit_reason'][:23]:>25} {v19['exit_reason'][:23]:>25}")

        total_v18 += v18['total_pnl_sol']
        total_v19 += v19['total_pnl_sol']
        if v18['total_pnl_pct'] > 0: v18_wins += 1
        if v19['total_pnl_pct'] > 0: v19_wins += 1

    print(f"\n{'─'*90}")
    print(f"  全部代币汇总:")
    print(f"    v18: 总PnL {total_v18:+.4f} SOL | 胜率 {v18_wins}/{len(valid)} ({v18_wins/len(valid)*100:.0f}%)")
    print(f"    v19: 总PnL {total_v19:+.4f} SOL | 胜率 {v19_wins}/{len(valid)} ({v19_wins/len(valid)*100:.0f}%)")
    print(f"    差异: v19 比 v18 {'多赚' if total_v19 > total_v18 else '少赚'} {abs(total_v19 - total_v18):.4f} SOL")

    # v18-passed only
    if v18_passed:
        v18p_v18_total = sum(r['v18']['total_pnl_sol'] for r in v18_passed)
        v18p_v19_total = sum(r['v19']['total_pnl_sol'] for r in v18_passed)
        v18p_v18_wins = sum(1 for r in v18_passed if r['v18']['total_pnl_pct'] > 0)
        v18p_v19_wins = sum(1 for r in v18_passed if r['v19']['total_pnl_pct'] > 0)

        print(f"\n  仅v18-PASS代币 ({len(v18_passed)}笔, 实际会入场的):")
        print(f"    v18: 总PnL {v18p_v18_total:+.4f} SOL | 胜率 {v18p_v18_wins}/{len(v18_passed)}")
        print(f"    v19: 总PnL {v18p_v19_total:+.4f} SOL | 胜率 {v18p_v19_wins}/{len(v18_passed)}")
        print(f"    差异: v19 {'多赚' if v18p_v19_total > v18p_v18_total else '少赚'} {abs(v18p_v19_total - v18p_v18_total):.4f} SOL")

    print(f"\n{'='*90}")
    print("✅ 回测完成")


if __name__ == '__main__':
    main()
