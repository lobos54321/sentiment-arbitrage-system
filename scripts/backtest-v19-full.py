#!/usr/bin/env python3
"""
回测 v2: 11个 v18 PASS 信号 × 完整K线 (1分钟+5分钟拼接覆盖数小时)
v18 vs v19 对比
"""

import urllib.request, json, ssl, time, re, sys
from datetime import datetime, timezone

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

POSITION_SIZE_SOL = 0.06

SIGNALS = [
    {"symbol": "Gany-1", "ca": "BgtczEGgf9mZMcGBLi4J5Pn8PtmFy6Xz5uBKkMfspump",
     "entry_time": "2026-03-17T02:50:54Z", "entry_mc": 98200},
    {"symbol": "Gany-2", "ca": "Dz4bX3snTDxqdKyZwdUgKoDvSjyvmoA23E6j5odZpump",
     "entry_time": "2026-03-17T03:00:26Z", "entry_mc": 49900},
    {"symbol": "唐子", "ca": "ocB3t4czHwsueZk89YGBxEbisFLZ1tzvydpwbC9pump",
     "entry_time": "2026-03-17T03:52:45Z", "entry_mc": 78100},
    {"symbol": "pvpdog", "ca": "9UT2T4XPYAtiUuSBdgkHTBbCTcfUtDVUSdSkTev1pump",
     "entry_time": "2026-03-17T04:35:20Z", "entry_mc": 73000},
    {"symbol": "grokette", "ca": "8AGFNmf6rwB9ctLEHnC9xEkUMnmFhXT2ZJEbhvAxpump",
     "entry_time": "2026-03-17T04:45:33Z", "entry_mc": 79300},
    {"symbol": "OLAF", "ca": "CqNAvDJfSM1t77NJiE9k2JH6SFwzCmEL6L4bsM3Upump",
     "entry_time": "2026-03-17T05:21:31Z", "entry_mc": 105500},
    {"symbol": "GoonGPT", "ca": "DTs1TiHQueUCFXPdGKXfwxoEqHLhYdybepxzqcEcpump",
     "entry_time": "2026-03-17T06:08:33Z", "entry_mc": 38300},
    {"symbol": "ANGEMEDE", "ca": "HGrajWZL6ACwUQzvf9SDXBMiNzNnEEgtgYo9XCCHpump",
     "entry_time": "2026-03-17T06:26:33Z", "entry_mc": 117600},
    {"symbol": "PROSUMER", "ca": "HYi27y1QhmqoU6Mg2M5KUVLfaEH99Vn4cyka35jUpump",
     "entry_time": "2026-03-17T06:44:40Z", "entry_mc": 77700},
    {"symbol": "OKARA", "ca": "Gc3J24TWcjKVBMxYJJthEoPhJjqafYbz7uyjUmejpump",
     "entry_time": "2026-03-17T06:55:13Z", "entry_mc": 49500},
    {"symbol": "BULKING", "ca": "7RzpCDKjZUwKfrzEo9GsV3sHy433GAo22SBLLdbVpump",
     "entry_time": "2026-03-17T07:41:38Z", "entry_mc": 128100},
]


def api_get(url, retries=3):
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'
            })
            resp = urllib.request.urlopen(req, timeout=20, context=CTX)
            return json.loads(resp.read())
        except Exception as e:
            err = str(e)
            if '429' in err:
                wait = 35 * (attempt + 1)
                print(f"    ⏳ 限流等{wait}s...", flush=True)
                time.sleep(wait)
            elif '401' in err or '404' in err:
                return None
            else:
                print(f"    ⚠️ {e}", flush=True)
                time.sleep(8)
    return None


def get_pool(ca):
    url = f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{ca}/pools?page=1"
    data = api_get(url)
    if not data: return None
    pools = data.get('data', [])
    for p in pools:
        name = p.get('attributes', {}).get('name', '')
        if 'SOL' in name:
            return p['attributes']['address']
    return pools[0]['attributes']['address'] if pools else None


def get_ohlcv(pool, aggregate, before_ts, limit=1000):
    url = (f"https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool}"
           f"/ohlcv/minute?aggregate={aggregate}&limit={limit}"
           f"&before_timestamp={before_ts}&currency=usd")
    data = api_get(url)
    if not data: return []
    return data.get('data', {}).get('attributes', {}).get('ohlcv_list', [])


def fetch_full_candles(pool, entry_ts):
    """
    拉取入场后尽可能多的K线:
    1) 1分钟K线 × 1000根 (覆盖~16h)
    2) 如果不够，再用5分钟K线补充后面的
    """
    all_candles = []

    # 方法: 拉入场时间到现在的1分钟K线 (limit=1000, 从当前时间往回)
    now_ts = int(time.time())
    # 先拉从"现在"往回1000根1分钟K线
    c1 = get_ohlcv(pool, 1, now_ts, 1000)
    if c1:
        all_candles.extend(c1)
        print(f"    1分K线: {len(c1)}根", flush=True)

    time.sleep(3)

    # 如果最早的K线还没到entry_ts之前，再拉更早的
    if all_candles:
        all_candles.sort(key=lambda x: x[0])
        earliest = all_candles[0][0]
        if earliest > entry_ts:
            # 再拉一批更早的
            c2 = get_ohlcv(pool, 1, earliest, 1000)
            if c2:
                all_candles.extend(c2)
                print(f"    补充1分K线: {len(c2)}根", flush=True)
            time.sleep(3)

    # 去重 + 排序
    seen = set()
    unique = []
    for c in all_candles:
        if c[0] not in seen:
            seen.add(c[0])
            unique.append(c)
    unique.sort(key=lambda x: x[0])

    # 只保留 entry_ts - 2min 之后的
    filtered = [c for c in unique if c[0] >= entry_ts - 120]

    if filtered:
        duration_h = (filtered[-1][0] - entry_ts) / 3600
        print(f"    总计: {len(filtered)}根K线, 覆盖入场后 {duration_h:.1f}h", flush=True)

    return filtered


# ==================== 策略模拟 ====================

def simulate(candles, entry_ts, entry_price, version):
    """
    version='v18': SL-40%, TP1@50%固定, 死水peak<50%
    version='v19': SL-35%, TP1@45-60%动态, 死水peak<45%
    """
    if not candles or not entry_price:
        return None

    sl_hard = -40 if version == 'v18' else -35
    tp1_trigger = 50 if version == 'v18' else 45
    dead_water_peak = 50 if version == 'v18' else 45

    pos = {
        'entry_price': entry_price,
        'tp1': False, 'tp2': False, 'tp3': False, 'tp4': False,
        'tp1_zone_peak': 0,
        'sold_pct': 0, 'remaining_pct': 100,
        'high_pnl': 0,
        'realized_sol': 0,
        'moonbag': False, 'moonbag_high_pnl': 0,
        'exit_reason': None, 'exit_pnl': None, 'exit_time': None,
        'tp1_sell_pnl': None,
    }

    for candle in candles:
        ts = candle[0]
        o, h, l, c = candle[1], candle[2], candle[3], candle[4]
        if ts < entry_ts:
            continue

        hold_min = (ts - entry_ts) / 60
        pnl = ((c - entry_price) / entry_price) * 100
        high_pnl_c = ((h - entry_price) / entry_price) * 100
        low_pnl_c = ((l - entry_price) / entry_price) * 100

        if high_pnl_c > pos['high_pnl']:
            pos['high_pnl'] = high_pnl_c

        current_sl = 0 if pos['tp1'] else sl_hard

        # 1. SL check (use low of candle for more accuracy)
        if not pos['moonbag'] and low_pnl_c <= current_sl:
            actual_sl_pnl = max(current_sl, low_pnl_c)  # hit SL at the SL level
            if pos['tp1']:
                pos['exit_reason'] = f"BREAKEVEN_SL({actual_sl_pnl:.0f}%)"
            else:
                pos['exit_reason'] = f"HARD_SL_{abs(sl_hard)}({actual_sl_pnl:.0f}%)"
            pos['exit_pnl'] = actual_sl_pnl
            pos['exit_time'] = ts
            break

        # 2. TP1
        if not pos['tp1']:
            if version == 'v18':
                # 固定: ≥50% 直接卖
                if high_pnl_c >= 50:
                    sell_pnl = max(50, pnl)  # 至少在50%卖出
                    pos['tp1'] = True
                    pos['tp1_sell_pnl'] = sell_pnl
                    sell_val = 0.60 * POSITION_SIZE_SOL * (1 + sell_pnl / 100)
                    pos['realized_sol'] += sell_val
                    pos['sold_pct'] = 60
                    pos['remaining_pct'] = 40
                    continue
            else:
                # v19 动态: 45-60% 区间峰值追踪
                if high_pnl_c >= 45:
                    if high_pnl_c > pos['tp1_zone_peak']:
                        pos['tp1_zone_peak'] = high_pnl_c

                    # 触发条件: 超60% 或 从峰值回落5%
                    if high_pnl_c >= 60:
                        sell_pnl = min(60, high_pnl_c)
                        pos['tp1'] = True
                        pos['tp1_sell_pnl'] = sell_pnl
                        sell_val = 0.60 * POSITION_SIZE_SOL * (1 + sell_pnl / 100)
                        pos['realized_sol'] += sell_val
                        pos['sold_pct'] = 60
                        pos['remaining_pct'] = 40
                        continue
                    elif pos['tp1_zone_peak'] >= 45:
                        peak_drop = pos['tp1_zone_peak'] - pnl
                        if peak_drop >= 5:
                            sell_pnl = pnl
                            pos['tp1'] = True
                            pos['tp1_sell_pnl'] = sell_pnl
                            sell_val = 0.60 * POSITION_SIZE_SOL * (1 + sell_pnl / 100)
                            pos['realized_sol'] += sell_val
                            pos['sold_pct'] = 60
                            pos['remaining_pct'] = 40
                            continue
                    # Still rising
                    continue

        # 3. TP2 ≥100%
        if pos['tp1'] and not pos['tp2'] and high_pnl_c >= 100:
            sell_pnl = max(100, pnl)
            pos['tp2'] = True
            sell_val = 0.15 * POSITION_SIZE_SOL * (1 + sell_pnl / 100)
            pos['realized_sol'] += sell_val
            pos['sold_pct'] += 15
            pos['remaining_pct'] -= 15
            continue

        # 4. TP3 ≥200%
        if pos['tp2'] and not pos['tp3'] and high_pnl_c >= 200:
            sell_pnl = max(200, pnl)
            pos['tp3'] = True
            sell_val = 0.15 * POSITION_SIZE_SOL * (1 + sell_pnl / 100)
            pos['realized_sol'] += sell_val
            pos['sold_pct'] += 15
            pos['remaining_pct'] -= 15
            continue

        # 5. TP4 ≥500%
        if pos['tp3'] and not pos['tp4'] and high_pnl_c >= 500:
            sell_pnl = max(500, pnl)
            pos['tp4'] = True
            sell_val = 0.05 * POSITION_SIZE_SOL * (1 + sell_pnl / 100)
            pos['realized_sol'] += sell_val
            pos['sold_pct'] += 5
            pos['remaining_pct'] -= 5
            pos['moonbag'] = True
            pos['moonbag_high_pnl'] = high_pnl_c
            continue

        # 6. Moonbag: 35% drawdown from peak
        if pos['moonbag']:
            if high_pnl_c > pos['moonbag_high_pnl']:
                pos['moonbag_high_pnl'] = high_pnl_c
            mp = pos['moonbag_high_pnl']
            if mp > 0:
                drop_pct = (mp - pnl) / mp * 100
                if drop_pct >= 35:
                    rem_val = (pos['remaining_pct']/100) * POSITION_SIZE_SOL * (1 + pnl/100)
                    pos['realized_sol'] += rem_val
                    pos['exit_reason'] = f"MOONBAG(peak+{mp:.0f}%→{pnl:.0f}%)"
                    pos['exit_pnl'] = pnl
                    pos['exit_time'] = ts
                    break
            continue

        # 7. Dead water: 15min
        if not pos['tp1'] and hold_min >= 15:
            if pos['high_pnl'] < dead_water_peak and -15 <= pnl <= 15:
                pos['exit_reason'] = f"DEAD_WATER(peak+{pos['high_pnl']:.0f}%,{pnl:.0f}%)"
                pos['exit_pnl'] = pnl
                pos['exit_time'] = ts
                break

        # 8. Timeout: 30min
        if not pos['tp1'] and hold_min >= 30:
            pos['exit_reason'] = f"TIMEOUT_30M({pnl:.0f}%)"
            pos['exit_pnl'] = pnl
            pos['exit_time'] = ts
            break

    # No exit triggered → use last candle
    if not pos['exit_reason'] and candles:
        last_c = [c for c in candles if c[0] >= entry_ts]
        if last_c:
            lp = last_c[-1][4]
            pnl = ((lp - entry_price) / entry_price) * 100
            pos['exit_reason'] = f"DATA_END({pnl:.0f}%)"
            pos['exit_pnl'] = pnl
            pos['exit_time'] = last_c[-1][0]

    # Final PnL calc
    if pos['exit_reason'] and 'DATA_END' not in pos['exit_reason']:
        if pos['remaining_pct'] > 0 and not pos['moonbag']:
            rem_val = (pos['remaining_pct']/100) * POSITION_SIZE_SOL * (1 + pos['exit_pnl']/100)
            pos['realized_sol'] += rem_val

    if pos['realized_sol'] > 0:
        pos['total_pnl_sol'] = pos['realized_sol'] - POSITION_SIZE_SOL
    else:
        pos['total_pnl_sol'] = POSITION_SIZE_SOL * (pos.get('exit_pnl', 0) / 100)

    pos['total_pnl_pct'] = (pos['total_pnl_sol'] / POSITION_SIZE_SOL) * 100
    return pos


def main():
    print("=" * 90)
    print("🔬 回测 v2 | 11个 v18 PASS 信号 × 完整K线 | v18 vs v19")
    print("=" * 90)
    print(f"v18: SL-40% | TP1@50%固定      | 死水peak<50%")
    print(f"v19: SL-35% | TP1@45-60%动态峰值 | 死水peak<45%")
    print(f"信号来源: 2026-03-17 00:37 ~ 07:53 (合并两份日志)")
    print(f"仓位: {POSITION_SIZE_SOL} SOL/笔")
    print()

    results = []

    for i, sig in enumerate(SIGNALS):
        symbol = sig['symbol']
        ca = sig['ca']
        entry_time = sig['entry_time']
        entry_dt = datetime.fromisoformat(entry_time.replace('Z', '+00:00'))
        entry_ts = int(entry_dt.timestamp())

        print(f"\n{'─'*70}")
        print(f"[{i+1}/{len(SIGNALS)}] ${symbol} | MC=${sig['entry_mc']/1000:.1f}K | {entry_time[:19]}")
        print(f"  CA: {ca}")

        # 1. Get pool
        print(f"  📡 查询池子...", flush=True)
        time.sleep(3)
        pool = get_pool(ca)
        if not pool:
            print(f"  ❌ 无池子，跳过")
            results.append({'symbol': symbol, 'error': 'no_pool'})
            continue
        print(f"  ✅ Pool: {pool[:40]}...")

        # 2. Get candles
        print(f"  📊 拉取K线...", flush=True)
        time.sleep(4)
        candles = fetch_full_candles(pool, entry_ts)

        if not candles or len(candles) < 5:
            print(f"  ❌ K线不足 ({len(candles) if candles else 0}根)")
            results.append({'symbol': symbol, 'error': 'no_candles'})
            continue

        # Find entry price
        entry_candle = None
        for c in candles:
            if c[0] >= entry_ts:
                entry_candle = c
                break
        if not entry_candle:
            print(f"  ❌ 找不到入场K线")
            results.append({'symbol': symbol, 'error': 'no_entry_candle'})
            continue

        entry_price = entry_candle[4]
        post_entry = [c for c in candles if c[0] >= entry_ts]
        prices = [c[4] for c in post_entry]
        highs = [c[2] for c in post_entry]
        lows = [c[3] for c in post_entry]

        if not prices:
            print(f"  ❌ 入场后无数据")
            results.append({'symbol': symbol, 'error': 'no_data_after_entry'})
            continue

        max_pnl = ((max(highs) - entry_price) / entry_price) * 100
        min_pnl = ((min(lows) - entry_price) / entry_price) * 100
        duration_h = (post_entry[-1][0] - entry_ts) / 3600

        print(f"  入场价: ${entry_price:.10f}")
        print(f"  📈 峰值: +{max_pnl:.1f}% | 谷值: {min_pnl:.1f}% | 数据: {duration_h:.1f}h ({len(post_entry)}根)")

        # 3. Run both strategies
        v18 = simulate(candles, entry_ts, entry_price, 'v18')
        v19 = simulate(candles, entry_ts, entry_price, 'v19')

        if v18 and v19:
            hold_v18 = (v18['exit_time'] - entry_ts) / 60 if v18.get('exit_time') else 0
            hold_v19 = (v19['exit_time'] - entry_ts) / 60 if v19.get('exit_time') else 0

            print(f"\n  {'策略':<6} {'退出原因':<35} {'PnL%':>8} {'SOL':>10} {'持仓':>8}")
            print(f"  {'─'*70}")
            print(f"  {'v18':<6} {v18['exit_reason']:<35} {v18['total_pnl_pct']:>+7.1f}% {v18['total_pnl_sol']:>+9.4f} {hold_v18:>6.1f}m")
            print(f"  {'v19':<6} {v19['exit_reason']:<35} {v19['total_pnl_pct']:>+7.1f}% {v19['total_pnl_sol']:>+9.4f} {hold_v19:>6.1f}m")

            diff = v19['total_pnl_pct'] - v18['total_pnl_pct']
            arrow = "🟢" if diff > 0.5 else "🔴" if diff < -0.5 else "⚪"
            print(f"  {arrow} v19 vs v18: {diff:+.1f}%")

            if v19.get('tp1_sell_pnl') is not None:
                print(f"  📌 v19 TP1: 区间峰值+{v19.get('tp1_zone_peak', 0):.1f}% → 卖出+{v19['tp1_sell_pnl']:.1f}%")
            if v18.get('tp1_sell_pnl') is not None:
                print(f"  📌 v18 TP1: 固定卖出+{v18['tp1_sell_pnl']:.1f}%")

        results.append({
            'symbol': symbol, 'error': None,
            'entry_price': entry_price, 'max_pnl': max_pnl, 'min_pnl': min_pnl,
            'duration_h': duration_h, 'candle_count': len(post_entry),
            'v18': v18, 'v19': v19,
        })

        time.sleep(2)

    # ==================== 汇总 ====================
    print(f"\n\n{'='*90}")
    print("📊 回测汇总")
    print(f"{'='*90}")

    valid = [r for r in results if r.get('v18') and r.get('v19')]
    errors = [r for r in results if r.get('error')]

    if errors:
        print(f"\n⚠️ {len(errors)}个代币数据获取失败: {', '.join(r['symbol'] for r in errors)}")

    print(f"\n{'代币':<12} {'峰值':>8} {'K线':>6} {'v18 PnL':>10} {'v18退出':>22} {'v19 PnL':>10} {'v19退出':>22} {'差异':>8}")
    print("─" * 110)

    total_v18 = total_v19 = 0
    v18_wins = v19_wins = 0
    v19_better = v19_worse = v19_same = 0

    for r in valid:
        v18 = r['v18']
        v19 = r['v19']
        diff = v19['total_pnl_pct'] - v18['total_pnl_pct']

        print(f"  {r['symbol']:<10} {r['max_pnl']:>+7.0f}% {r['candle_count']:>5} "
              f"{v18['total_pnl_pct']:>+9.1f}% {v18['exit_reason'][:20]:>22} "
              f"{v19['total_pnl_pct']:>+9.1f}% {v19['exit_reason'][:20]:>22} "
              f"{diff:>+7.1f}%")

        total_v18 += v18['total_pnl_sol']
        total_v19 += v19['total_pnl_sol']
        if v18['total_pnl_pct'] > 0: v18_wins += 1
        if v19['total_pnl_pct'] > 0: v19_wins += 1
        if diff > 0.5: v19_better += 1
        elif diff < -0.5: v19_worse += 1
        else: v19_same += 1

    n = len(valid)
    print(f"\n{'─'*90}")
    print(f"  有效样本: {n}笔")
    print(f"\n  v18 旧策略:")
    print(f"    总PnL: {total_v18:+.4f} SOL ({total_v18/POSITION_SIZE_SOL/n*100:+.1f}% 平均)")
    print(f"    胜率:  {v18_wins}/{n} ({v18_wins/n*100:.0f}%)")
    print(f"\n  v19 新策略:")
    print(f"    总PnL: {total_v19:+.4f} SOL ({total_v19/POSITION_SIZE_SOL/n*100:+.1f}% 平均)")
    print(f"    胜率:  {v19_wins}/{n} ({v19_wins/n*100:.0f}%)")
    print(f"\n  v19 vs v18:")
    print(f"    总差异: {total_v19 - total_v18:+.4f} SOL")
    print(f"    v19更好: {v19_better}笔 | v19更差: {v19_worse}笔 | 相同: {v19_same}笔")
    print(f"\n{'='*90}")
    print("✅ 回测完成")


if __name__ == '__main__':
    main()
