#!/usr/bin/env python3
"""
NOT_ATH 棘轮策略 v4.0
======================
核心思想：「让金狗用行动证明自己，自动切换到宽追踪」

== 关键洞察（来自数据分析）==
1. 30%的币是金狗(>100%)，问题不是找到，而是拿住
2. trail20_sl10 让金狗平均只赚+15%（实际能涨200%+）
3. 500%+火箭全部 pre_mom≈0%，AI偏低，TI=0-1，MC≥15K
4. 火箭峰值中位数在34根，5根超时会错过80%

== 棘轮追踪止损（Ratchet Trail）==
不预测哪个是金狗，而是设置分阶段出场规则：
  阶段1 (0~+50%):   SL = 入场价-15%（保护本金）
  阶段2 (+50~+100%): SL = 从峰值回撤25%（锁定部分利润）
  阶段3 (+100~+200%): SL = 从峰值回撤35%（金狗，放宽）
  阶段4 (+200%+):    SL = 从峰值回撤50%（火箭，大放宽）
  超时：120根（2小时），或200根（可配置）

== 过滤调整（为抓火箭）==
  放宽 MIN_AI → 35（火箭全是AI=35）
  放宽 MIN_TRADE_INDEX → 0（火箭全是TI=0-1）
  要求 pre_mom < FLAT_THRESH（入场前平静）
  MC ≥ 15K（火箭无一在<15K，但保留小MC测试）
"""

import json, os, sys
from datetime import datetime, timezone
from collections import defaultdict

SLIP = 0.004
POS  = 0.06   # SOL per trade

# ── 棘轮参数 ──────────────────────────────────────────────────────────────
SL_INIT  = -0.15    # 阶段1：初始止损
TRAIL_2  =  0.25    # 阶段2 (峰值≥50%)：从峰值回撤比例
TRAIL_3  =  0.35    # 阶段3 (峰值≥100%)
TRAIL_4  =  0.50    # 阶段4 (峰值≥200%)

# 各阶段触发阈值
PHASE2_THRESH = 0.50   # +50% 进入阶段2
PHASE3_THRESH = 1.00   # +100% 进入阶段3
PHASE4_THRESH = 2.00   # +200% 进入阶段4

# 超时：120根（=2小时）
TIMEOUT = 120

# 死狗：N根内未涨超M%则止损
DEAD_WINDOW = 10    # 10根内
DEAD_THRESH = 0.15  # 未达+15%

# ── 过滤参数 ──────────────────────────────────────────────────────────────
MIN_MC_K  = 5
MAX_MC_K  = 30
MIN_SI    = 50       # 放宽（火箭有 SI=58）
MIN_AI    = 35       # 放宽（火箭全是 AI=35）
MAX_AI    = 130      # 放宽
MAX_TRADE = 999      # 放宽 TI 上限（不限制）
PRE_FLAT_MAX = 15.0  # 入场前3分钟涨幅上限（火箭=0%，放宽到15%）
PRE_DUMP_MIN = -10.0 # 入场前急跌过滤（保留）
SKIP_UTC_20_22 = False  # 暂关闭

# ── 可对比的配置 ──────────────────────────────────────────────────────────
CONFIGS = {
    'ratchet_base': {
        'sl_init': -0.15,
        'trails':  {0.50: 0.25, 1.00: 0.35, 2.00: 0.50},
        'timeout': 120,
        'desc':    'SL-15% → trail25%@50% → trail35%@100% → trail50%@200%'
    },
    'ratchet_tight': {
        'sl_init': -0.10,
        'trails':  {0.50: 0.20, 1.00: 0.30, 2.00: 0.45},
        'timeout': 60,
        'desc':    'SL-10% → trail20%@50% → trail30%@100% → trail45%@200%'
    },
    'ratchet_wide': {
        'sl_init': -0.20,
        'trails':  {0.50: 0.30, 1.00: 0.40, 2.00: 0.55},
        'timeout': 240,
        'desc':    'SL-20% → trail30%@50% → trail40%@100% → trail55%@200%'
    },
    'v3_tp_baseline': {
        'sl_init': -0.15,
        'tp_levels': [(0.80, 0.60), (1.00, 0.50), (2.00, 0.50), (5.00, 0.80)],
        'timeout': 15,
        'desc':    'v3 TP1=80%/60% TP2=100%/50% TP3=200%/50% TP4=500%/80%'
    },
}


# ── 工具函数 ─────────────────────────────────────────────────────────────

def gi(sig, key):
    val = (sig.get('indices') or {}).get(key) or sig.get(key) or 0
    if isinstance(val, dict): return val.get('signal', 0)
    return val or 0


def find_entry(candles, ts_ms):
    eb = (ts_ms // 1000 // 60) * 60
    return (next((c for c in candles if c['ts'] == eb), None)
         or next((c for c in candles if c['ts'] <= eb and c['ts'] > eb-120), None)
         or next((c for c in candles if c['ts'] > eb and c['ts'] <= eb+300), None))


def pre_momentum(candles, entry_ts):
    bars = [c for c in candles if c['ts'] < entry_ts][-3:]
    if len(bars) < 2: return 0.0
    return (bars[-1]['c'] - bars[0]['o']) / bars[0]['o'] * 100


def sim_ratchet(after, ep, cfg):
    """棘轮追踪止损模拟。"""
    trails  = cfg.get('trails', {})   # {thresh: trail_ratio}
    sl_init = cfg['sl_init']
    timeout = cfg.get('timeout', 120)
    sorted_thresholds = sorted(trails.keys())

    rem      = POS / ep
    sol_out  = 0.0
    peak     = ep
    sl_price = ep * (1 + sl_init)
    phase    = 1          # 当前阶段
    hold     = 0
    exit_tag = None
    max_peak_pct = 0.0

    for c in after[:timeout]:
        if rem <= 1e-10:
            break
        hold += 1
        lo = c['l']; hi = c['h']

        # 更新峰值
        if hi > peak:
            peak = hi

        peak_pct = (peak - ep) / ep

        # 计算当前应使用的追踪比例（棘轮：只升不降）
        current_trail = abs(sl_init)  # 阶段1：固定止损（按入场价）
        in_trail_mode = False
        for thr in sorted_thresholds:
            if peak_pct >= thr:
                current_trail = trails[thr]
                in_trail_mode = True

        # 计算止损价
        if in_trail_mode:
            # 追踪止损：从峰值回撤
            sl_price = peak * (1 - current_trail)
        else:
            # 固定止损：从入场价
            sl_price = ep * (1 + sl_init)

        # 检查止损
        if lo <= sl_price:
            exit_price = max(lo, sl_price * 0.995)  # 轻微滑点
            sol_out += rem * exit_price * (1 - SLIP)
            rem = 0
            exit_tag = f'TRAIL_P{phase}' if in_trail_mode else 'SL'
            break

        # 死狗过滤：前N根内未达阈值
        if hold == DEAD_WINDOW and peak_pct < DEAD_THRESH:
            sol_out += rem * c['c'] * (1 - SLIP)
            rem = 0
            exit_tag = 'DEAD'
            break

        # 更新阶段标记（仅用于记录）
        for i, thr in enumerate(sorted_thresholds, 2):
            if peak_pct >= thr:
                phase = i

        if peak_pct > max_peak_pct:
            max_peak_pct = peak_pct

    if rem > 1e-10:
        sol_out += rem * after[min(hold, len(after)-1)]['c'] * (1 - SLIP)
        exit_tag = exit_tag or 'TIMEOUT'

    return {
        'pnl':  sol_out - POS,
        'pct':  (sol_out - POS) / POS * 100,
        'peak': max_peak_pct * 100,
        'ex':   exit_tag or 'END',
        'hold': hold,
    }


def sim_tp(after, ep, cfg):
    """传统 TP 结构模拟（v3 对照组）。"""
    tp_levels = cfg.get('tp_levels', [])
    sl_init   = cfg['sl_init']
    timeout   = cfg.get('timeout', 15)

    rem      = POS / ep
    sol_out  = 0.0
    peak     = ep
    sl_price = ep * (1 + sl_init)
    tp1_hit  = False
    hold     = 0
    exit_tag = None

    tp_prices = [(ep * (1+tp), ratio) for tp, ratio in tp_levels]

    for c in after[:timeout]:
        if rem <= 1e-10: break
        hold += 1
        lo = c['l']; hi = c['h']

        cur_sl = ep if tp1_hit else sl_price
        if lo <= cur_sl:
            sol_out += rem * cur_sl * (1 - SLIP)
            rem = 0
            exit_tag = 'SL_BE' if tp1_hit else 'SL'
            break

        for tp_price, ratio in sorted(tp_prices, reverse=True):
            if rem > 1e-10 and hi >= tp_price:
                sell = rem * ratio
                sol_out += sell * tp_price * (1 - SLIP)
                rem -= sell
                exit_tag = exit_tag or f'TP{tp_price/ep-1:.0%}'
                if tp_price == tp_prices[0][0]:
                    tp1_hit = True

        if hi > peak: peak = hi

        if hold == DEAD_WINDOW and (peak - ep)/ep < DEAD_THRESH and not tp1_hit:
            sol_out += rem * c['c'] * (1 - SLIP)
            rem = 0; exit_tag = 'DEAD'; break

    if rem > 1e-10:
        sol_out += rem * after[min(hold, len(after)-1)]['c'] * (1 - SLIP)
        exit_tag = exit_tag or 'TIMEOUT'

    peak_pct = (peak - ep) / ep * 100
    return {
        'pnl':  sol_out - POS,
        'pct':  (sol_out - POS) / POS * 100,
        'peak': peak_pct,
        'ex':   exit_tag or 'END',
        'hold': hold,
    }


def simulate(after, ep, cfg):
    if 'tp_levels' in cfg:
        return sim_tp(after, ep, cfg)
    return sim_ratchet(after, ep, cfg)


# ── 数据加载 ─────────────────────────────────────────────────────────────

def load_signals():
    sigs = []
    for path in ['data/channel-history.json',
                 '/tmp/mar1316_signals_filtered.json',
                 '/tmp/mar1618_signals.json']:
        if not os.path.exists(path): continue
        d = json.load(open(path))
        raw = d.get('signals', d) if isinstance(d, dict) else d
        if isinstance(raw, dict): raw = list(raw.values())
        seen = {s.get('token_ca') for s in sigs}
        for s in raw:
            ca = s.get('token_ca') or s.get('ca','')
            if ca and ca not in seen:
                s['token_ca'] = ca; sigs.append(s); seen.add(ca)
    return sigs


def filter_sig(sig, candles, entry_ts, dt):
    mc_k = (sig.get('market_cap') or 0) / 1000
    si   = gi(sig, 'super_index')
    ai   = gi(sig, 'ai_index')
    ti   = gi(sig, 'trade_index')

    if sig.get('is_ath'): return False, 'is_ath'
    if mc_k <= 0 or mc_k > MAX_MC_K: return False, f'mc({mc_k:.0f}K)'
    if mc_k < MIN_MC_K: return False, f'mc_low({mc_k:.0f}K)'
    if si < MIN_SI: return False, f'si_low({si})'
    if ai < MIN_AI: return False, f'ai_low({ai})'
    if MAX_AI > 0 and ai > MAX_AI: return False, f'ai_high({ai})'
    if SKIP_UTC_20_22 and 20 <= dt.hour < 22: return False, 'utc_20_22'

    pre = pre_momentum(candles, entry_ts)
    if pre < PRE_DUMP_MIN: return False, f'pre_dump({pre:.0f}%)'
    if pre > PRE_FLAT_MAX: return False, f'pre_hot({pre:.0f}%)'

    return True, None


# ── 主程序 ───────────────────────────────────────────────────────────────

def main():
    print('=' * 72)
    print('NOT_ATH 棘轮策略 v4.0  —  让金狗自己证明自己')
    print(f'过滤: MC {MIN_MC_K}-{MAX_MC_K}K | SI≥{MIN_SI} | AI {MIN_AI}-{MAX_AI}')
    print(f'      pre_mom: {PRE_DUMP_MIN}%~{PRE_FLAT_MAX}%（入场前需平静）')
    print('=' * 72)

    cache   = json.load(open('data/ohlcv-cache.json'))
    signals = load_signals()

    skip_counts = {}
    entries = []   # (sig_meta, after_candles, ep)

    for sig in signals:
        ca = sig.get('token_ca','')
        if not ca: continue
        cd = cache.get(ca)
        if not cd or not cd.get('candles'):
            skip_counts['no_candles'] = skip_counts.get('no_candles',0)+1; continue
        candles = sorted(cd['candles'], key=lambda x: x['ts'])
        ec = find_entry(candles, sig['ts'])
        if not ec:
            skip_counts['no_entry'] = skip_counts.get('no_entry',0)+1; continue
        dt = datetime.fromtimestamp(sig['ts']/1000, tz=timezone.utc)
        ok, reason = filter_sig(sig, candles, ec['ts'], dt)
        if not ok:
            skip_counts[reason] = skip_counts.get(reason,0)+1; continue
        after = [c for c in candles if c['ts'] > ec['ts']]
        if not after:
            skip_counts['no_after'] = skip_counts.get('no_after',0)+1; continue
        ep = ec['c'] * (1+SLIP)
        pre = pre_momentum(candles, ec['ts'])
        entries.append({
            'sym':  sig.get('symbol', ca[:8]),
            'ca':   ca,
            'ts':   sig['ts'],
            'mc_k': (sig.get('market_cap') or 0)/1000,
            'si':   gi(sig,'super_index'),
            'ai':   gi(sig,'ai_index'),
            'ti':   gi(sig,'trade_index'),
            'mi':   gi(sig,'media_index'),
            'sent': gi(sig,'sentiment_index'),
            'addr': gi(sig,'address_index'),
            'pre':  pre,
            'after': after,
            'ep':   ep,
            'date': dt.strftime('%m/%d'),
            'hour': dt.hour,
        })

    print(f'\n总信号: {len(signals)}  →  执行样本: {len(entries)}笔')
    print(f'主要过滤: ' + ' | '.join(f'{k}:{v}' for k,v in
          sorted(skip_counts.items(), key=lambda x:-x[1])[:8]))

    if not entries:
        print('无执行样本'); sys.exit(0)

    # ── 各配置对比 ──────────────────────────────────────────────────────
    print(f'\n{"="*72}')
    print(f'📊 配置对比（{len(entries)}笔）')
    print(f'{"="*72}')
    print(f'  {"配置":<20} {"N":>4}  {"WR":>5}  {"EV%":>7}  {"AvgW":>7}  {"AvgL":>7}  {"峰均值":>7}  {"总PnL":>8}')
    print(f'  {"─"*72}')

    cfg_results = {}
    for cfg_name, cfg in CONFIGS.items():
        results = []
        for e in entries:
            r = simulate(e['after'], e['ep'], cfg)
            if r:
                results.append({**r, **{k:v for k,v in e.items() if k != 'after'}})
        cfg_results[cfg_name] = results
        if not results: continue
        wins   = [r for r in results if r['pnl'] > 0]
        losses = [r for r in results if r['pnl'] <= 0]
        wr     = len(wins)/len(results)*100
        ev     = sum(r['pct'] for r in results)/len(results)
        aw     = sum(r['pct'] for r in wins)/len(wins) if wins else 0
        al     = sum(r['pct'] for r in losses)/len(losses) if losses else 0
        pk     = sum(r['peak'] for r in results)/len(results)
        pnl    = sum(r['pnl'] for r in results)
        mark   = '✅' if ev > 5 else ('▲' if ev > 0 else '❌')
        print(f'  {mark} {cfg_name:<20} {len(results):>4}  {wr:>4.0f}%  {ev:>+7.1f}%  {aw:>+7.1f}%  {al:>+7.1f}%  {pk:>+7.0f}%  {pnl:>+7.3f}')

    # ── 金狗的捕获效率对比 ──────────────────────────────────────────────
    print(f'\n{"="*72}')
    print(f'🐕 金狗捕获效率（真正的核心问题：金狗身上平均赚多少？）')
    print(f'{"="*72}')
    print(f'  金狗定义：峰值≥100%')
    print(f'  {"配置":<20} {"金狗N":>6}  {"金狗EV":>9}  {"金狗WR":>8}  {"普通EV":>9}  {"捕获峰值%":>10}')
    print(f'  {"─"*68}')

    for cfg_name, results in cfg_results.items():
        if not results: continue
        dogs    = [r for r in results if r['peak'] >= 100]
        non_dogs = [r for r in results if r['peak'] < 100]
        if not dogs: continue
        d_ev = sum(r['pct'] for r in dogs)/len(dogs)
        d_wr = sum(1 for r in dogs if r['pnl']>0)/len(dogs)*100
        n_ev = sum(r['pct'] for r in non_dogs)/len(non_dogs) if non_dogs else 0
        # 捕获率：实际盈利 / 峰值的比例
        captured = [(r['pct'] / r['peak'] * 100) for r in dogs if r['peak'] > 0]
        cap_rate = sum(captured)/len(captured) if captured else 0
        print(f'  {cfg_name:<20} {len(dogs):>6}  {d_ev:>+9.1f}%  {d_wr:>7.0f}%  {n_ev:>+9.1f}%  {cap_rate:>9.1f}%')

    # ── 聚焦 ratchet_base 的详细分析 ───────────────────────────────────
    print(f'\n{"="*72}')
    print(f'🎯 ratchet_base 详细分析')
    print(f'{"="*72}')
    results = cfg_results.get('ratchet_base', [])
    if not results:
        print('无结果'); return

    # 峰值分布下的表现
    print(f'\n  峰值区间分析（金狗分级）:')
    print(f'  {"峰值区间":<14} {"N":>4}  {"WR":>5}  {"EV%":>7}  {"实际/峰值":>10}  {"平仓方式TOP2"}')
    print(f'  {"─"*60}')
    for lo, hi, label in [(0,50,'普通(<50%)'), (50,100,'银狗(50-100%)'),
                           (100,200,'金狗(100-200%)'), (200,500,'大金狗(200-500%)'),
                           (500,None,'火箭(500%+)')]:
        g = [r for r in results if r['peak']>=lo and (hi is None or r['peak']<hi)]
        if not g: continue
        wr  = sum(1 for r in g if r['pnl']>0)/len(g)*100
        ev  = sum(r['pct'] for r in g)/len(g)
        cap = [r['pct']/r['peak']*100 for r in g if r['peak']>0]
        cap_avg = sum(cap)/len(cap) if cap else 0
        exits   = defaultdict(int)
        for r in g: exits[r['ex']] += 1
        top2_exit = ' '.join(f"{k}({v})" for k,v in sorted(exits.items(), key=lambda x:-x[1])[:2])
        mark = '🚀' if lo>=500 else ('✅' if lo>=100 else ('▲' if lo>=50 else '  '))
        print(f'  {mark} {label:<14} {len(g):>4}  {wr:>4.0f}%  {ev:>+7.1f}%  {cap_avg:>9.1f}%  {top2_exit}')

    # 平仓方式分布
    exit_cnt = defaultdict(int)
    for r in results: exit_cnt[r['ex']] += 1
    print(f'\n  平仓分布: ' + ' | '.join(f'{k}:{v}' for k,v in
          sorted(exit_cnt.items(), key=lambda x:-x[1])))

    # 时间分析
    print(f'\n  时间段（UTC）:')
    for h in range(0, 24, 4):
        g = [r for r in results if h <= r['hour'] < h+4]
        if not g: continue
        wr  = sum(1 for r in g if r['pnl']>0)/len(g)*100
        ev  = sum(r['pct'] for r in g)/len(g)
        mark = '✅' if ev>5 else ('▲' if ev>0 else '❌')
        print(f'    {mark} UTC {h:02d}-{h+4:02d}  {len(g):>3}笔  WR={wr:.0f}%  EV={ev:+.1f}%')

    # pre_mom 分析（关键！）
    print(f'\n  入场前动量（pre_mom）分析:')
    print(f'  {"区间":<14} {"N":>4}  {"金狗率":>7}  {"EV%":>7}')
    for lo, hi in [(-99,-5), (-5,5), (5,15)]:
        g = [r for r in results if lo <= r['pre'] < hi]
        if not g: continue
        dog_r = sum(1 for r in g if r['peak']>=100)/len(g)*100
        ev    = sum(r['pct'] for r in g)/len(g)
        tag   = f'{lo}%~{hi}%'
        print(f'    {tag:<14} {len(g):>4}  {dog_r:>6.0f}%  {ev:>+7.1f}%')

    # ── 火箭专项跟踪 ────────────────────────────────────────────────────
    print(f'\n{"="*72}')
    print(f'🚀 火箭（峰值≥500%）专项跟踪')
    print(f'{"="*72}')
    rockets_r = [r for r in results if r['peak'] >= 500]
    if rockets_r:
        print(f'  {"代币":<12} {"日期":>6} {"峰值%":>8}  {"实际%":>8}  {"持仓根":>6}  {"平仓"}')
        print(f'  {"─"*55}')
        for r in sorted(rockets_r, key=lambda x: -x['peak']):
            cap = r['pct']/r['peak']*100 if r['peak']>0 else 0
            print(f'  {r["sym"]:<12} {r["date"]:>5}  +{r["peak"]:>6.0f}%  {r["pct"]:>+7.1f}%  {r["hold"]:>5}根  {r["ex"]} (捕获{cap:.0f}%)')
    else:
        print(f'  本次过滤后无火箭（请检查过滤参数，可能被 pre_hot 过滤）')

    # ── 明细表 ──────────────────────────────────────────────────────────
    print(f'\n{"="*72}')
    print(f'明细（ratchet_base，按 PnL 排序）')
    print(f'{"="*72}')
    print(f'  {"代币":<12} {"日期":>6} {"MC":>4} {"SI":>4} {"AI":>3} {"TI":>3} {"pre":>5}  {"峰值":>6}  {"PnL%":>7}  {"平仓"}')
    print(f'  {"─"*72}')
    for r in sorted(results, key=lambda x: -x['pnl']):
        flag = '✅' if r['pnl']>0 else '❌'
        print(f'  {flag} {r["sym"]:<10} {r["date"]:>5} {r["mc_k"]:>4.0f}K '
              f'{r["si"]:>4.0f} {r["ai"]:>3.0f} {r["ti"]:>3.0f} '
              f'{r["pre"]:>+4.0f}%  +{r["peak"]:>5.0f}%  '
              f'{r["pct"]:>+6.1f}%  {r["ex"]}')

    # ── 总结 ────────────────────────────────────────────────────────────
    n   = len(results)
    wins = [r for r in results if r['pnl']>0]
    losses = [r for r in results if r['pnl']<=0]
    total_pnl = sum(r['pnl'] for r in results)
    wr  = len(wins)/n*100
    aw  = sum(r['pct'] for r in wins)/len(wins) if wins else 0
    al  = sum(r['pct'] for r in losses)/len(losses) if losses else 0
    rr  = abs(aw/al) if al else 0

    days_span = (max(datetime.fromtimestamp(r['ts']//1000, tz=timezone.utc) for r in results) -
                 min(datetime.fromtimestamp(r['ts']//1000, tz=timezone.utc) for r in results))
    days = max(days_span.days, 1)

    print(f'\n{"="*72}')
    print(f'▶ ratchet_base 总结（{n}笔）')
    print(f'  WR={wr:.0f}%  EV={total_pnl/n/POS*100:+.2f}%  RR={rr:.2f}x')
    print(f'  总PnL={total_pnl:+.4f}SOL  月预估={total_pnl/days*30:+.2f}SOL（{days}天）')
    print(f'  金狗率（峰值≥100%）: {sum(1 for r in results if r["peak"]>=100)/n*100:.0f}%')
    print(f'\n  ※ 下一步优化建议:')
    print(f'    1. 若"捕获率"偏低 → 放宽 TRAIL_3/TRAIL_4')
    print(f'    2. 若普通币亏损大 → 收紧 SL_INIT 或开启 DEAD_WINDOW')
    print(f'    3. 若火箭被 pre_hot 过滤 → 调整 PRE_FLAT_MAX')
    print(f'{"="*72}')


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    main()
