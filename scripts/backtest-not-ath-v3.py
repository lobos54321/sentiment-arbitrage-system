#!/usr/bin/env python3
"""
NOT_ATH 완전 전략 v3.0
==============================
[v3 신규 추가 — 浪费的两个维度全部启用]

  新维度 1: trade_index (链上交易量确认)
    · AI低但trade高 → 被低估的机会（买方悄悄入场）
    · AI高但trade低 → 虚假热度，应跳过
    · 分析: AI×trade 交叉维度 + trade_index 区间胜率

  新维度 2: address_index (地址活跃度)
    · 聪明钱代理指标：地址数越多 → 分布越健康
    · 分析: address_index 区间胜率

  新维度 3: sentiment_index + media_index 区间分析
    · sentiment: 二值分布 (0 vs 35)
    · media: 三模态 (0 / 30 / 60)

  关于 {signal, current} 双值结构:
    · 该结构仅存在于 ATH 信号 (is_ath=true)
    · NOT_ATH 信号 indices 均为平铺整数，无 current 字段
    → 加速度代理方案: 用 velocity (candle计算) 替代

[v2 继承]
  · AI≥60 过热惩罚 · MC 5-30K · UTC20-22排除
  · 入场前3分急跌过滤 · TP1/2/3/4 + SL + DW + MH

[TP 结构] (同 v2)
  TP1: +80%   60%  → SL移至本金
  TP2: +100%  50%
  TP3: +200%  50%
  TP4: +500%  80%
  SL:  -15%
  DW:  8根  MH: 15根
"""

import json, os, sys
from datetime import datetime, timezone
from collections import defaultdict

SLIP = 0.004
POS  = 0.06

# ─── TP 参数 ─────────────────────────────────────────────────────────────
SL    = -0.25
TP1   = 0.80;  TP1s = 0.60
TP2   = 1.00;  TP2s = 0.50
TP3   = 2.00;  TP3s = 0.50
TP4   = 5.00;  TP4s = 0.80
DW    = 8
MH    = 15

# ─── 信号过滤参数 ─────────────────────────────────────────────────────────
MIN_SI          = 100
MIN_AI          = 40
MAX_AI          = 0       # 0=禁用; 本次测试 ai 40-100 不设上限
MAX_MC_K        = 30
MIN_MC_K        = 5
MIN_VEL         = 0        # 0=禁用
PRE_DUMP_THRESH = -10.0
SKIP_UTC_20_22  = True

# ─── v3 新参数 ────────────────────────────────────────────────────────────
MIN_TRADE_INDEX   = 2      # trade=1 是死区(0% WR)，跳过
MAX_ADDRESS_INDEX = 5      # addr≥6 = FOMO散户已涌入，我们入场偏晚
MIN_ADDRESS_INDEX = 0      # 下限（暂不限制）
SKIP_AI_FAKE      = False  # AI高(≥80)但trade低(≤1) → 虚假热度过滤
AI_FAKE_AI_THRESH = 80
AI_FAKE_TRADE_MAX = 1


# ─── 工具函数 ─────────────────────────────────────────────────────────────

def get_index(sig, key):
    """获取 indices 字段值，兼容 flat-int 和 {signal, current} 两种结构。
    NOT_ATH 信号为 flat-int；ATH 信号（已被过滤）为对象结构。
    始终返回 signal-time 值。"""
    idx = sig.get('indices') or {}
    val = idx.get(key) or sig.get(key) or 0
    if isinstance(val, dict):
        return val.get('signal', 0)
    return val


def get_index_current(sig, key):
    """获取 current 值（仅 ATH 信号有效，NOT_ATH 返回 None）。"""
    idx = sig.get('indices') or {}
    val = idx.get(key)
    if isinstance(val, dict):
        return val.get('current', None)
    return None  # NOT_ATH 无 current 字段


def find_entry_candle(candles, sig_ts_ms):
    eb = (sig_ts_ms // 1000 // 60) * 60
    return next((c for c in candles if c['ts'] <= eb and c['ts'] > eb - 120), None)


def calc_velocity(candles, entry_bar_ts):
    window_start = entry_bar_ts - 300
    bars = [c for c in candles if window_start <= c['ts'] < entry_bar_ts]
    if not bars:
        return None
    p0, p1 = bars[0]['o'], bars[-1]['c']
    return (p1 - p0) / p0 * 100 if p0 > 0 else None


def calc_pre_momentum(candles, entry_bar_ts):
    bars = [c for c in candles if c['ts'] < entry_bar_ts][-3:]
    if len(bars) < 2:
        return 0.0
    return (bars[-1]['c'] - bars[0]['o']) / bars[0]['o'] * 100


def simulate(candles, ec):
    after = [c for c in candles if c['ts'] > ec['ts']]
    if not after:
        return None

    ep  = ec['c'] * (1 + SLIP)
    rem = POS / ep
    sol_out = 0.0
    tp1_hit = False
    peak    = 0.0
    hold    = 0
    ex      = None

    tp1_p = ep * (1 + TP1)
    tp2_p = ep * (1 + TP2)
    tp3_p = ep * (1 + TP3)
    tp4_p = ep * (1 + TP4)
    sl_p  = ep * (1 + SL)

    for c in after:
        if rem <= 1e-10:
            break
        hold += 1
        lo = c['l']; hi = c['h']

        cur_sl = ep if tp1_hit else sl_p
        if lo <= cur_sl:
            sol_out += rem * cur_sl * (1 - SLIP)
            rem = 0
            ex = 'SL_BE' if tp1_hit else 'SL'
            break

        if rem > 1e-10 and hi >= tp4_p:
            sell = rem * TP4s
            sol_out += sell * tp4_p * (1 - SLIP)
            rem -= sell
            ex = ex or 'TP4'

        if rem > 1e-10 and hi >= tp3_p:
            sell = rem * TP3s
            sol_out += sell * tp3_p * (1 - SLIP)
            rem -= sell
            ex = ex or 'TP3'

        if rem > 1e-10 and hi >= tp2_p:
            sell = rem * TP2s
            sol_out += sell * tp2_p * (1 - SLIP)
            rem -= sell
            ex = ex or 'TP2'

        if not tp1_hit and rem > 1e-10 and hi >= tp1_p:
            sell = rem * TP1s
            sol_out += sell * tp1_p * (1 - SLIP)
            rem -= sell
            tp1_hit = True
            ex = ex or 'TP1'

        if hi > peak:
            peak = hi
        if not tp1_hit:
            peak_pct = (peak - ep) / ep if ep > 0 else 0
            if hold >= DW and peak_pct < TP1 and (c['c'] - ep) / ep <= 0.20:
                sol_out += rem * c['c'] * (1 - SLIP)
                rem = 0
                ex = ex or 'DEAD'
                break
            if hold >= MH:
                sol_out += rem * c['c'] * (1 - SLIP)
                rem = 0
                ex = ex or 'TIMEOUT'
                break

    if rem > 1e-10:
        sol_out += rem * after[-1]['c'] * (1 - SLIP)
        ex = ex or 'END'

    peak_pct = (peak - ep) / ep * 100 if ep > 0 else 0
    return {
        'pnl':  sol_out - POS,
        'pct':  (sol_out - POS) / POS * 100,
        'ex':   ex,
        'tp1':  tp1_hit,
        'peak': peak_pct,
        'hold': hold,
        'ep':   ep,
    }


def filter_signal(sig, candles, entry_bar_ts, sig_dt):
    mc_k = (sig.get('market_cap') or 0) / 1000
    si   = get_index(sig, 'super_index')
    ai   = get_index(sig, 'ai_index')
    ti   = get_index(sig, 'trade_index')
    addi = get_index(sig, 'address_index')
    hour = sig_dt.hour

    if sig.get('is_ath'):
        return False, 'is_ath'

    if not sig.get('_mc_unknown'):
        if mc_k <= 0 or mc_k > MAX_MC_K:
            return False, f'mc_range({mc_k:.1f}K)'
        if mc_k < MIN_MC_K:
            return False, f'mc_low({mc_k:.1f}K)'

    if si < MIN_SI:
        return False, f'si_low({si})'

    if ai < MIN_AI:
        return False, f'ai_low({ai})'

    if MAX_AI > 0 and ai >= MAX_AI:
        return False, f'ai_high({ai})'

    if SKIP_UTC_20_22 and 20 <= hour < 22:
        return False, 'utc_20_22'

    pre_mom = calc_pre_momentum(candles, entry_bar_ts)
    if pre_mom < PRE_DUMP_THRESH:
        return False, f'pre_dump({pre_mom:.1f}%)'

    if MIN_VEL > 0:
        vel = calc_velocity(candles, entry_bar_ts)
        if vel is None or vel < MIN_VEL:
            return False, f'vel_low({vel})'

    # ── v3 新增过滤 ──────────────────────────────────────────────────────
    if MIN_TRADE_INDEX > 0 and ti < MIN_TRADE_INDEX:
        return False, f'trade_low({ti})'

    if MIN_ADDRESS_INDEX > 0 and addi < MIN_ADDRESS_INDEX:
        return False, f'addr_low({addi})'

    if MAX_ADDRESS_INDEX > 0 and addi > MAX_ADDRESS_INDEX:
        return False, f'addr_high({addi})'

    # AI 虚假热度过滤: AI 高但链上无成交 → 跳过
    if SKIP_AI_FAKE and ai >= AI_FAKE_AI_THRESH and ti <= AI_FAKE_TRADE_MAX:
        return False, f'ai_fake(ai={ai},trade={ti})'

    return True, None


def load_signals():
    hist = json.load(open('data/channel-history.json'))
    signals = list(hist['signals'])

    aux_path = '/tmp/mar1316_signals_filtered.json'
    if os.path.exists(aux_path):
        aux = json.load(open(aux_path))
        seen = {(s.get('token_ca'), s['ts']) for s in signals}
        added = 0
        for s in aux:
            ca = s.get('token_ca') or s.get('ca', '')
            key = (ca, s['ts'])
            if key in seen:
                continue
            seen.add(key)
            if 'indices' not in s:
                s['indices'] = {
                    'super_index': s.get('super_index', 0),
                    'ai_index':    s.get('ai_index', 0),
                    'media_index': s.get('media_index', 0),
                }
            s['token_ca'] = ca
            s['_mc_unknown'] = True
            signals.append(s)
            added += 1
        print(f'  辅助数据: +{added}笔 ({aux_path})')

    return signals


# ─── 区间分析辅助 ──────────────────────────────────────────────────────────

def bucket_analysis(results, field, buckets, label):
    """给定字段和区间列表，打印胜率/EV分析。"""
    print(f'\n{"─"*70}')
    print(f'{label} 区间分析')
    print(f'{"─"*70}')
    header = f'  {"区间":<14} {"N":>4}  {"WR":>6}  {"EV%":>7}  {"AvgWin":>8}  {"AvgLoss":>9}  {"Peak":>7}'
    print(header)
    for lo, hi in buckets:
        if hi is None:
            g = [r for r in results if r[field] >= lo]
            tag = f'{lo}+'
        else:
            g = [r for r in results if lo <= r[field] < hi]
            tag = f'{lo}-{hi}'
        if not g:
            continue
        wins   = [r for r in g if r['pnl'] > 0]
        losses = [r for r in g if r['pnl'] <= 0]
        wr     = len(wins) / len(g) * 100
        ev     = sum(r['pct'] for r in g) / len(g)
        aw     = sum(r['pct'] for r in wins)  / len(wins)  if wins   else 0
        al     = sum(r['pct'] for r in losses)/ len(losses) if losses else 0
        pk     = sum(r['peak'] for r in g)    / len(g)
        mark   = '✅' if ev > 15 else ('⚠️ ' if ev > 0 else '❌')
        print(f'  {mark} {tag:<12} {len(g):>4}  {wr:>5.0f}%  {ev:>+7.1f}%  {aw:>+8.1f}%  {al:>+9.1f}%  {pk:>+7.0f}%')


def cross_analysis(results):
    """AI × trade_index 交叉维度分析 — 核心洞察"""
    print(f'\n{"─"*70}')
    print(f'AI × trade_index 交叉分析 (核心: 链上确认)')
    print(f'{"─"*70}')
    print(f'  理论: AI低+trade高=被低估机会 | AI高+trade低=虚假热度')
    print(f'  {"类型":<22} {"N":>4}  {"WR":>6}  {"EV%":>7}  {"Peak":>7}')

    cases = [
        ('AI低(<80)+trade高(≥3)',   lambda r: r['ai'] <  80 and r['ti'] >= 3),
        ('AI低(<80)+trade低(<3)',   lambda r: r['ai'] <  80 and r['ti'] <  3),
        ('AI高(≥80)+trade高(≥3)',   lambda r: r['ai'] >= 80 and r['ti'] >= 3),
        ('AI高(≥80)+trade低(<3)',   lambda r: r['ai'] >= 80 and r['ti'] <  3),  # 虚假热度?
    ]
    for name, fn in cases:
        g = [r for r in results if fn(r)]
        if not g:
            continue
        wins = [r for r in g if r['pnl'] > 0]
        wr   = len(wins) / len(g) * 100
        ev   = sum(r['pct'] for r in g) / len(g)
        pk   = sum(r['peak'] for r in g) / len(g)
        mark = '✅' if ev > 15 else ('⚠️ ' if ev > 0 else '❌')
        print(f'  {mark} {name:<22} {len(g):>4}  {wr:>5.0f}%  {ev:>+7.1f}%  {pk:>+7.0f}%')


def main():
    print('=' * 70)
    print('NOT_ATH 完全策略 v3.0  — 链上维度全激活')
    ai_range = f'ai {MIN_AI}-{MAX_AI-1}' if MAX_AI > 0 else f'ai≥{MIN_AI}'
    print(f'基础过滤: NOT_ATH | MC {MIN_MC_K}-{MAX_MC_K}K | si≥{MIN_SI} | {ai_range}')
    print(f'          UTC20-22排除={SKIP_UTC_20_22} | 前3分急跌<{PRE_DUMP_THRESH}%')
    v3_flags = []
    if MIN_TRADE_INDEX > 0:   v3_flags.append(f'trade≥{MIN_TRADE_INDEX}')
    if MIN_ADDRESS_INDEX > 0: v3_flags.append(f'addr≥{MIN_ADDRESS_INDEX}')
    if MAX_ADDRESS_INDEX > 0: v3_flags.append(f'addr≤{MAX_ADDRESS_INDEX}')
    if SKIP_AI_FAKE:          v3_flags.append(f'ai_fake过滤(ai≥{AI_FAKE_AI_THRESH}&trade≤{AI_FAKE_TRADE_MAX})')
    print(f'v3 新过滤: {", ".join(v3_flags) if v3_flags else "全部禁用 (纯分析模式)"}')
    print('=' * 70)

    cache   = json.load(open('data/ohlcv-cache.json'))
    signals = load_signals()

    skip_counts = {}
    results = []

    for sig in signals:
        ca = sig.get('token_ca', '')
        if not ca:
            continue

        cd = cache.get(ca)
        if not cd or not cd.get('candles'):
            skip_counts['no_candles'] = skip_counts.get('no_candles', 0) + 1
            continue

        candles = cd['candles']
        ec = find_entry_candle(candles, sig['ts'])
        if not ec:
            skip_counts['no_entry'] = skip_counts.get('no_entry', 0) + 1
            continue

        entry_bar_ts = ec['ts']
        sig_dt = datetime.fromtimestamp(sig['ts'] / 1000, tz=timezone.utc)

        ok, reason = filter_signal(sig, candles, entry_bar_ts, sig_dt)
        if not ok:
            skip_counts[reason] = skip_counts.get(reason, 0) + 1
            continue

        r = simulate(candles, ec)
        if not r:
            skip_counts['sim_fail'] = skip_counts.get('sim_fail', 0) + 1
            continue

        mc_k = (sig.get('market_cap') or 0) / 1000
        vel  = calc_velocity(candles, entry_bar_ts)
        pre  = calc_pre_momentum(candles, entry_bar_ts)

        # v3: 记录新维度
        ti   = get_index(sig, 'trade_index')
        addi = get_index(sig, 'address_index')
        senti= get_index(sig, 'sentiment_index')
        mi   = get_index(sig, 'media_index')

        results.append({
            **r,
            'sym':   sig.get('symbol', ca[:8]),
            'ca':    ca,
            'ts':    sig['ts'],
            'mc_k':  mc_k,
            'si':    get_index(sig, 'super_index'),
            'ai':    get_index(sig, 'ai_index'),
            'ti':    ti,
            'addi':  addi,
            'senti': senti,
            'mi':    mi,
            'vel':   vel,
            'pre':   pre,
            'hour':  sig_dt.hour,
            'date':  sig_dt.strftime('%m/%d'),
        })

    # ─── 过滤统计 ──────────────────────────────────────────────────────
    print(f'\n总信号: {len(signals)}个')
    print(f'跳过明细:')
    for k in sorted(skip_counts, key=lambda x: -skip_counts[x]):
        print(f'  {k:<35}: -{skip_counts[k]}')
    print(f'执行样本: {len(results)}笔\n')

    if not results:
        print('无执行样本，退出。')
        sys.exit(0)

    n      = len(results)
    wins   = [r for r in results if r['pnl'] > 0]
    losses = [r for r in results if r['pnl'] <= 0]

    total_pnl = sum(r['pnl'] for r in results)
    wr        = len(wins) / n * 100
    avg_win   = sum(r['pct'] for r in wins)   / len(wins)   if wins   else 0
    avg_loss  = sum(r['pct'] for r in losses) / len(losses) if losses else 0
    rr        = abs(avg_win / avg_loss) if avg_loss else 0
    big_loss  = [r for r in losses if r['pct'] < -10]

    exits = {}
    for r in results:
        exits[r['ex']] = exits.get(r['ex'], 0) + 1

    print('=' * 70)
    print(f'▶ 综合结果 ({n}笔)')
    print('=' * 70)
    print(f'  总 PnL:      {total_pnl:+.4f} SOL')
    print(f'  EV/笔:       {total_pnl/n:+.4f} SOL  ({total_pnl/n/POS*100:+.2f}%)')
    print(f'  胜率:        {wr:.1f}%  ({len(wins)}W / {len(losses)}L)')
    print(f'  平均盈利:    {avg_win:+.1f}%')
    print(f'  平均亏损:    {avg_loss:+.1f}%')
    print(f'  盈亏比:      {rr:.2f}x')
    print(f'  大亏(>10%):  {len(big_loss)}笔  ({len(big_loss)/n*100:.1f}%)')
    print(f'  TP1到达:     {sum(1 for r in results if r["tp1"])}笔  ({sum(1 for r in results if r["tp1"])/n*100:.1f}%)')
    print(f'  平仓分布:    ' + ' | '.join(f'{k}:{v}' for k, v in sorted(exits.items(), key=lambda x: -x[1])))
    vels = [r["vel"] for r in results if r.get("vel") is not None]
    print(f'  平均速度:    {sum(vels)/len(vels):.1f}%' if vels else '  平均速度:    N/A')
    print(f'  平均峰值:    {sum(r["peak"] for r in results)/n:.1f}%')

    # ─── v3 核心: trade_index × AI 交叉分析 ──────────────────────────
    cross_analysis(results)

    # ─── trade_index 区间分析 ─────────────────────────────────────────
    bucket_analysis(results, 'ti', [
        (0, 1), (1, 2), (2, 4), (4, 8), (8, None)
    ], 'trade_index')

    # ─── address_index 区间分析 ───────────────────────────────────────
    bucket_analysis(results, 'addi', [
        (0, 1), (1, 3), (3, 6), (6, 10), (10, None)
    ], 'address_index')

    # ─── sentiment_index 分析 ─────────────────────────────────────────
    bucket_analysis(results, 'senti', [
        (0, 1), (1, None)
    ], 'sentiment_index (0=无情绪 / >0=有情绪)')

    # ─── media_index 区间分析 ─────────────────────────────────────────
    bucket_analysis(results, 'mi', [
        (0, 1), (1, 30), (30, 60), (60, None)
    ], 'media_index')

    # ─── AI Index 区间 (v2 继承) ──────────────────────────────────────
    bucket_analysis(results, 'ai', [
        (60, 70), (70, 80), (80, 100), (100, 150), (150, None)
    ], 'AI Index')

    # ─── MC 区间 ──────────────────────────────────────────────────────
    bucket_analysis(results, 'mc_k', [
        (5, 10), (10, 15), (15, 20), (20, 25), (25, 30)
    ], 'Market Cap (K)')

    # ─── 时间带分析 ───────────────────────────────────────────────────
    print(f'\n{"─"*70}')
    print(f'时间带成果 (UTC)')
    print(f'{"─"*70}')
    for h in range(0, 24, 2):
        g = [r for r in results if h <= r['hour'] < h + 2]
        if not g:
            continue
        g_wr = len([r for r in g if r['pnl'] > 0]) / len(g) * 100
        g_ev = sum(r['pct'] for r in g) / len(g)
        mark = '✅' if g_ev > 15 else ('⚠️ ' if g_ev > 0 else '❌')
        print(f'  {mark} UTC {h:02d}-{h+2:02d}  {len(g):>3}笔  WR={g_wr:4.0f}%  EV={g_ev:+6.1f}%')

    # ─── 日期分析 ─────────────────────────────────────────────────────
    print(f'\n{"─"*70}')
    print(f'按日期')
    print(f'{"─"*70}')
    by_date = defaultdict(list)
    for r in results:
        by_date[r['date']].append(r)
    for d in sorted(by_date):
        g = by_date[d]
        g_wr  = len([r for r in g if r['pnl'] > 0]) / len(g) * 100
        g_pnl = sum(r['pnl'] for r in g)
        print(f'  {d}  {len(g):>3}笔  WR={g_wr:4.0f}%  PnL={g_pnl:+.4f}SOL')

    # ─── 明细表 ───────────────────────────────────────────────────────
    print(f'\n{"─"*70}')
    print(f'明细 (按 PnL 排序)')
    print(f'{"─"*70}')
    print(f'  {"代币":<12} {"日期":>6} {"UTC":>3} {"MC":>5} {"SI":>4} {"AI":>4} {"TI":>3} {"ADD":>3} {"SEN":>3} {"MI":>3} {"Vel":>5} {"Peak":>6} {"PnL%":>7} {"PnL":>8} {"平仓"}')
    print(f'  {"─"*100}')
    for r in sorted(results, key=lambda x: x['pnl'], reverse=True):
        flag    = '✅' if r['pnl'] > 0 else '❌'
        vel_str = f'{r["vel"]:>4.0f}%' if r.get("vel") is not None else '  N/A'
        print(f'  {flag} {r["sym"]:<10} {r["date"]:>5} {r["hour"]:02d}h '
              f'{r["mc_k"] or 0:>4.0f}K {r["si"]:>4.0f} {r["ai"]:>4.0f} '
              f'{r["ti"]:>3.0f} {r["addi"]:>3.0f} {r["senti"]:>3.0f} {r["mi"]:>3.0f} '
              f'{vel_str} {r["peak"]:>+6.0f}% '
              f'{r["pct"]:>+6.1f}% {r["pnl"]:>+7.4f}  {r["ex"]}')

    # ─── 结论 ─────────────────────────────────────────────────────────
    print(f'\n{"=" * 70}')
    print(f'▶ 结论 v3.0')
    print(f'  EV  = {total_pnl/n/POS*100:+.2f}% / 笔')
    print(f'  WR  = {wr:.0f}%')
    print(f'  RR  = {rr:.2f}x')
    print(f'  大亏率 = {len(big_loss)/n*100:.1f}%')

    days_span = (max(datetime.fromtimestamp(r['ts']//1000, tz=timezone.utc) for r in results) -
                 min(datetime.fromtimestamp(r['ts']//1000, tz=timezone.utc) for r in results))
    days = max(days_span.days, 1)
    print(f'  月预估 PnL = {total_pnl/days*30:+.2f} SOL  (周期={days}天 / 投入={POS*n:.2f}SOL)')
    print(f'\n  ※ v3 新维度操作建议:')
    print(f'    · 根据上方 trade_index × AI 交叉结果，设置 MIN_TRADE_INDEX')
    print(f'    · 如果 AI高+trade低 EV显著偏低 → 开启 SKIP_AI_FAKE=True')
    print(f'    · address_index 阈值: 参考区间分析后设置 MIN_ADDRESS_INDEX')
    print(f'{"=" * 70}')


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    main()
