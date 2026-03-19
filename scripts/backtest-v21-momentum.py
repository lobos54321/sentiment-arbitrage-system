#!/usr/bin/env python3
"""
v21 3根阳线动量确认策略回测
============================
核心变化：入场不再立即买入，而是等待信号后连续3根1分钟阳线确认。

过滤条件:
  ✅ NOT_ATH
  ✅ MC ≤ 30K（硬过滤，MC未知跳过）
  ✅ 信号后连续3根1分钟K线全收阳
  ✅ 3根K线累计涨幅 ≥ 20%
  可选: Trade Index ≤ 2 | AI Index 40-80

入场价 = 第3根阳线收盘价

方案A（保守）: TP=+30%, SL=-30%, Timeout=15min
方案B（激进）: TP=+60%, SL=-30%, Timeout=30min

单次全仓卖出（无多级止盈）

用法:
  cd /path/to/sentiment-arbitrage-system
  python3 scripts/backtest-v21-momentum.py [--date 03/15-03/18] [--verbose]
"""

import json, os, sys, argparse
from datetime import datetime, timezone
from collections import defaultdict, Counter

# ─── 参数 ────────────────────────────────────────────────────────────────────
SLIP = 0.004
POS  = 0.06      # SOL per trade

SL_PCT  = -0.30  # 止损
TP_A    =  0.30  # 方案A止盈
TP_B    =  0.60  # 方案B止盈
TO_A    = 15     # 方案A超时（根）
TO_B    = 30     # 方案B超时（根）

MIN_CONSEC_GAIN = 0.20  # 3根K线最低累计涨幅

CACHE_PATH   = 'data/ohlcv-cache.json'
HISTORY_PATH = 'data/channel-history.json'

# ─── 工具函数 ─────────────────────────────────────────────────────────────────

def get_idx(sig, key):
    """兼容 flat-int 和 {signal, current} 两种结构"""
    val = (sig.get('indices') or {}).get(key) or sig.get(key) or 0
    if isinstance(val, dict):
        return val.get('signal', 0)
    return val or 0


def find_entry_immediate(candles, sig_ts_ms):
    """旧策略：信号出来直接买入（信号Bar收盘）"""
    eb = (sig_ts_ms // 1000 // 60) * 60
    c = (next((c for c in candles if c['ts'] == eb), None)
         or next((c for c in candles if c['ts'] <= eb and c['ts'] > eb - 120), None)
         or next((c for c in candles if c['ts'] > eb and c['ts'] <= eb + 300), None))
    return c, None


def find_entry_momentum(candles, sig_ts_ms):
    """
    新策略：信号后等待3根连续1分钟阳线，累计涨幅≥20%。
    返回 (entry_candle_or_None, skip_reason_or_None)
    """
    sig_bar = (sig_ts_ms // 1000 // 60) * 60
    # 取信号bar之后的K线（不含信号bar本身），按时间升序排列
    after = sorted([c for c in candles if c['ts'] > sig_bar], key=lambda x: x['ts'])

    if len(after) < 3:
        return None, 'not_enough_candles'

    c1, c2, c3 = after[0], after[1], after[2]

    # 三根全收阳
    if c1['c'] <= c1['o']:
        return None, 'c1_bearish'
    if c2['c'] <= c2['o']:
        return None, 'c2_bearish'
    if c3['c'] <= c3['o']:
        return None, 'c3_bearish'

    # 累计涨幅：以第1根开盘为基准
    base = c1['o']
    if base <= 0:
        return None, 'base_zero'
    cum_gain = (c3['c'] - base) / base
    if cum_gain < MIN_CONSEC_GAIN:
        return None, f'cum={cum_gain*100:.1f}%<20%'

    return c3, None


def simulate_simple(candles, entry_candle, tp_pct, sl_pct, timeout_bars):
    """
    简单单次全卖模拟。
    timeout_bars: 最大持仓K线根数。
    SL优先（同一根K线同时触及TP/SL → 止损）。
    """
    after = sorted([c for c in candles if c['ts'] > entry_candle['ts']], key=lambda x: x['ts'])
    if not after:
        return None

    ep      = entry_candle['c'] * (1 + SLIP)
    tp_p    = ep * (1 + tp_pct)
    sl_p    = ep * (1 + sl_pct)
    sol_out = 0.0
    ex      = None
    peak    = 0.0

    for c in after[:timeout_bars]:
        lo, hi = c['l'], c['h']
        if hi > peak:
            peak = hi

        # SL先检查（保守）
        if lo <= sl_p:
            sol_out = (POS / ep) * sl_p * (1 - SLIP)
            ex = 'SL'
            break

        if hi >= tp_p:
            sol_out = (POS / ep) * tp_p * (1 - SLIP)
            ex = 'TP'
            break

    if ex is None:
        last = after[min(timeout_bars - 1, len(after) - 1)]
        sol_out = (POS / ep) * last['c'] * (1 - SLIP)
        ex = 'TIMEOUT'

    return {
        'pnl':  sol_out - POS,
        'pct':  (sol_out - POS) / POS * 100,
        'ex':   ex,
        'ep':   ep,
        'peak': (peak - ep) / ep * 100 if ep > 0 else 0,
    }

# ─── 策略定义 ─────────────────────────────────────────────────────────────────

def make_strategies():
    """
    每个策略 = (name, filter_fn, entry_fn, tp_pct, timeout_bars)
    filter_fn(sig) → (ok, reason)
    entry_fn(candles, sig_ts_ms) → (candle_or_None, reason_or_None)
    """

    def base_filter(sig):
        if sig.get('is_ath'):
            return False, 'is_ath'
        mc = sig.get('market_cap') or 0
        if mc <= 0:
            return False, 'mc_unknown'
        if mc > 30_000:
            return False, f'mc>{mc/1000:.0f}K'
        return True, None

    def filter_trade2(sig):
        ok, r = base_filter(sig)
        if not ok:
            return False, r
        ti = get_idx(sig, 'trade_index')
        if ti > 2:
            return False, f'trade={ti}>2'
        return True, None

    def filter_ai_band(sig):
        ok, r = base_filter(sig)
        if not ok:
            return False, r
        ai = get_idx(sig, 'ai_index')
        if ai < 40 or ai > 80:
            return False, f'ai={ai}∉[40,80]'
        return True, None

    def filter_trade2_ai(sig):
        ok, r = filter_trade2(sig)
        if not ok:
            return False, r
        ai = get_idx(sig, 'ai_index')
        if ai < 40 or ai > 80:
            return False, f'ai={ai}∉[40,80]'
        return True, None

    imm = find_entry_immediate
    mom = find_entry_momentum

    return [
        # label,                     filter_fn,        entry_fn,  tp,   timeout
        ('Baseline-A  即时买入',      base_filter,      imm,       TP_A, TO_A),
        ('Baseline-B  即时买入',      base_filter,      imm,       TP_B, TO_B),
        ('Mom-A       3阳确认',       base_filter,      mom,       TP_A, TO_A),
        ('Mom-B       3阳确认',       base_filter,      mom,       TP_B, TO_B),
        ('Mom-A+T2    +Trade≤2',      filter_trade2,    mom,       TP_A, TO_A),
        ('Mom-B+T2    +Trade≤2',      filter_trade2,    mom,       TP_B, TO_B),
        ('Mom-A+AI    +AI40-80',      filter_ai_band,   mom,       TP_A, TO_A),
        ('Mom-A+T2+AI +Trade≤2+AI',  filter_trade2_ai, mom,       TP_A, TO_A),
    ]

# ─── 数据加载 ─────────────────────────────────────────────────────────────────

def load_signals(date_filter=None, aux_paths=None):
    signals = []

    if os.path.exists(HISTORY_PATH):
        hist = json.load(open(HISTORY_PATH))
        raw = hist.get('signals', []) if isinstance(hist, dict) else hist
        signals.extend(raw)
        print(f'  channel-history: {len(raw)} 条')

    default_aux = [
        '/tmp/mar1316_signals_filtered.json',
        '/tmp/mar1618_signals.json',
        'data/signals_export.json',
    ]
    for path in (aux_paths or default_aux):
        if os.path.exists(path):
            try:
                aux = json.load(open(path))
                if isinstance(aux, dict):
                    aux = aux.get('signals', list(aux.values()))
                seen = {(s.get('token_ca'), s.get('ts')) for s in signals}
                added = 0
                for s in aux:
                    ca = s.get('token_ca') or s.get('ca', '')
                    key = (ca, s.get('ts'))
                    if key in seen:
                        continue
                    seen.add(key)
                    if ca:
                        s['token_ca'] = ca
                    signals.append(s)
                    added += 1
                print(f'  {path}: +{added} 条')
            except Exception as e:
                print(f'  {path}: 加载失败 ({e})')

    if date_filter:
        def sig_date(s):
            d = s.get('date', '')
            if d:
                return d
            ts = s.get('ts', 0)
            if ts > 1e12:
                ts /= 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%m/%d') if ts else ''
        signals = [s for s in signals if sig_date(s) in date_filter]
        print(f'  → 日期过滤 {date_filter}: {len(signals)} 条')

    return signals


def load_cache():
    if not os.path.exists(CACHE_PATH):
        print(f'❌ 找不到 {CACHE_PATH}')
        sys.exit(1)
    cache = json.load(open(CACHE_PATH))
    print(f'  ohlcv-cache: {len(cache)} 个token')
    return cache

# ─── 统计汇报 ─────────────────────────────────────────────────────────────────

def stats(results, label):
    if not results:
        return {'label': label, 'n': 0, 'wr': None, 'ev': None,
                'avg_win': None, 'avg_loss': None, 'exits': {}}
    n = len(results)
    wins   = [r for r in results if r['pnl'] > 0]
    losses = [r for r in results if r['pnl'] <= 0]
    wr     = len(wins) / n
    avg_w  = sum(r['pct'] for r in wins)  / len(wins)  if wins   else 0
    avg_l  = sum(r['pct'] for r in losses)/ len(losses) if losses else 0
    ev     = sum(r['pct'] for r in results) / n
    total  = sum(r['pnl'] for r in results)
    exits  = Counter(r['ex'] for r in results)
    return {
        'label':    label,
        'n':        n,
        'wr':       wr,
        'ev':       ev,
        'avg_win':  avg_w,
        'avg_loss': avg_l,
        'total':    total,
        'exits':    dict(exits),
    }


def print_table(rows):
    W = 24
    hdr = f"  {'策略':<{W}} {'n':>4}  {'胜率':>5}  {'均盈':>6}  {'均亏':>7}  {'EV/笔':>7}  {'总PnL':>8}"
    sep = '  ' + '─' * (len(hdr) - 2)
    print(sep)
    print(hdr)
    print(sep)
    for r in rows:
        if r['n'] == 0:
            print(f"  {'  ✗ ' + r['label']:<{W+4}} {'0':>4}  {'—':>5}  {'—':>6}  {'—':>7}  {'N/A':>7}  {'N/A':>8}")
            continue
        ev_s   = f"{r['ev']:+.1f}%"
        wr_s   = f"{r['wr']*100:.0f}%"
        aw_s   = f"{r['avg_win']:+.1f}%"
        al_s   = f"{r['avg_loss']:+.1f}%"
        tot_s  = f"{r['total']*1000:+.1f}m"
        mark   = '✅' if r['ev'] > 5 else ('⚠️ ' if r['ev'] > 0 else '❌')
        print(f"  {mark} {r['label']:<{W}} {r['n']:>4}  {wr_s:>5}  {aw_s:>6}  {al_s:>7}  {ev_s:>7}  {tot_s:>8}")
    print(sep)


def print_exits(label, exits, n):
    if not exits:
        return
    print(f'\n  [{label}] 退出分布 (n={n}):')
    for ex, cnt in sorted(exits.items(), key=lambda x: -x[1]):
        print(f'    {ex:<10} {cnt:>3}  ({cnt/n*100:.0f}%)')


def print_momentum_funnel(funnel, total):
    print(f'\n  动量确认漏斗 (MC≤30K 基础过后 {total} 条):')
    pass_n = total - sum(funnel.values())
    for reason, cnt in sorted(funnel.items(), key=lambda x: -x[1]):
        print(f'    {reason:<28} 跳过 {cnt:>4}  ({cnt/total*100:.0f}%)')
    print(f'    {"→ 进入交易":<28} 通过 {pass_n:>4}  ({pass_n/total*100:.0f}%)')

# ─── 主程序 ───────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', help='日期过滤，逗号分隔，如 03/15,03/16,03/17,03/18')
    ap.add_argument('--verbose', '-v', action='store_true', help='逐笔打印 Mom-A 交易详情')
    ap.add_argument('--aux', nargs='*', help='额外辅助信号文件路径')
    args = ap.parse_args()

    date_filter = args.date.split(',') if args.date else None

    print('=' * 72)
    print('v21  3根阳线动量确认策略  vs  即时买入基准')
    print(f'过滤: NOT_ATH | MC≤30K | 连续3阳 + 累计≥20%')
    print(f'方案A: TP=+30% SL=-30% Timeout=15min  |  方案B: TP=+60% SL=-30% Timeout=30min')
    if date_filter:
        print(f'日期: {date_filter}')
    print('=' * 72)

    print('\n📂 数据加载:')
    cache   = load_cache()
    signals = load_signals(date_filter, args.aux)
    print(f'  信号总计: {len(signals)}')

    strategies = make_strategies()
    results    = {s[0]: [] for s in strategies}
    # 动量漏斗（对 Mom-A 策略追踪）
    mom_funnel  = defaultdict(int)
    mom_eligible = 0   # MC≤30K 后的数量（用于动量漏斗分母）

    no_cache  = 0
    processed = set()
    # 逐笔详情（Mom-A）
    moma_label = strategies[2][0]   # 'Mom-A       3阳确认'
    moma_trades = []

    for sig in signals:
        ca = sig.get('token_ca', '')
        ts = sig.get('ts', 0)
        if not ca or not ts:
            continue
        key = (ca, ts)
        if key in processed:
            continue
        processed.add(key)

        cd = cache.get(ca)
        if not cd or not cd.get('candles'):
            no_cache += 1
            continue

        candles = cd['candles']

        for s_label, flt, entry_fn, tp, timeout in strategies:
            ok, reason = flt(sig)
            if not ok:
                continue

            # 仅对 Mom-A 跟踪动量漏斗
            is_mom_a = (s_label == moma_label)
            if is_mom_a:
                mom_eligible += 1

            ec, skip_r = entry_fn(candles, ts)
            if ec is None:
                if is_mom_a and skip_r:
                    mom_funnel[skip_r] += 1
                continue

            r = simulate_simple(candles, ec, tp, SL_PCT, timeout)
            if r is None:
                continue

            r.update({
                'ca':     ca,
                'symbol': sig.get('symbol', ca[:8]),
                'mc':     (sig.get('market_cap') or 0) / 1000,
                'si':     get_idx(sig, 'super_index'),
                'ai':     get_idx(sig, 'ai_index'),
                'ti':     get_idx(sig, 'trade_index'),
                'date':   sig.get('date', ''),
            })
            results[s_label].append(r)
            if is_mom_a:
                moma_trades.append(r)

    print(f'\n  K线缺失跳过: {no_cache}  有效信号: {len(processed)}')

    # ─── 结果表格 ──────────────────────────────────────────────────────────────
    print('\n📊 策略对比:\n')
    rows = [stats(results[s[0]], s[0]) for s in strategies]
    print_table(rows)

    # ─── 动量漏斗 ─────────────────────────────────────────────────────────────
    if mom_eligible > 0:
        print_momentum_funnel(mom_funnel, mom_eligible)

    # ─── Mom-A 逐笔详情 ───────────────────────────────────────────────────────
    moma_stats = rows[2]
    if moma_stats['n'] > 0:
        print_exits(moma_label.strip(), moma_stats['exits'], moma_stats['n'])

        if args.verbose:
            print(f'\n  [Mom-A] 逐笔明细 (n={moma_stats["n"]}):')
            hdr = f"  {'日期':>5} {'symbol':<14} {'MC':>6} {'SI':>4} {'AI':>3} {'TI':>3} {'EP':>12} {'EV%':>8} {'退出'}"
            print(hdr)
            print('  ' + '─' * (len(hdr) - 2))
            for r in sorted(moma_trades, key=lambda x: x['pct']):
                mark = '✅' if r['pnl'] > 0 else '❌'
                print(f"  {mark} {r['date']:>5} {r['symbol']:<14} {r['mc']:>5.1f}K "
                      f"{r['si']:>4} {r['ai']:>3} {r['ti']:>3} "
                      f"{r['ep']:>12.8f}  {r['pct']:>+7.1f}%  {r['ex']}")

    # ─── AI 区间分析（Mom-A 基础过滤内） ─────────────────────────────────────
    base_mom_results = results[moma_label]
    if base_mom_results:
        print('\n  AI 区间分析 (Mom-A 内):')
        print(f"  {'AI区间':<10} {'N':>4}  {'WR':>5}  {'EV%':>7}")
        for lo, hi in [(0,20),(20,40),(40,60),(60,80),(80,100),(100,9999)]:
            g = [r for r in base_mom_results if lo <= r['ai'] < hi]
            if not g: continue
            w = [r for r in g if r['pnl'] > 0]
            ev = sum(r['pct'] for r in g) / len(g)
            tag = f'{lo}-{hi-1}' if hi < 9999 else f'{lo}+'
            mark = '✅' if ev > 5 else ('⚠️ ' if ev > 0 else '❌')
            print(f"  {mark} {tag:<10} {len(g):>4}  {len(w)/len(g)*100:>4.0f}%  {ev:>+7.1f}%")

    # ─── Trade Index 区间分析（Mom-A 内） ────────────────────────────────────
    if base_mom_results:
        print('\n  Trade Index 分析 (Mom-A 内):')
        print(f"  {'TI':>4} {'N':>4}  {'WR':>5}  {'EV%':>7}")
        for ti_val in sorted(set(r['ti'] for r in base_mom_results)):
            g = [r for r in base_mom_results if r['ti'] == ti_val]
            w = [r for r in g if r['pnl'] > 0]
            ev = sum(r['pct'] for r in g) / len(g)
            mark = '✅' if ev > 5 else ('⚠️ ' if ev > 0 else '❌')
            print(f"  {mark} {ti_val:>4} {len(g):>4}  {len(w)/len(g)*100:>4.0f}%  {ev:>+7.1f}%")

    # ─── 时间分布（Mom-A） ────────────────────────────────────────────────────
    if base_mom_results:
        date_grp = defaultdict(list)
        for r in base_mom_results:
            date_grp[r['date']].append(r)
        if len(date_grp) > 1:
            print('\n  按日期分布 (Mom-A):')
            print(f"  {'日期':>5}  {'N':>4}  {'WR':>5}  {'EV%':>7}")
            for d in sorted(date_grp):
                g = date_grp[d]
                w = [r for r in g if r['pnl'] > 0]
                ev = sum(r['pct'] for r in g) / len(g)
                mark = '✅' if ev > 5 else ('⚠️ ' if ev > 0 else '❌')
                print(f"  {mark} {d:>5}  {len(g):>4}  {len(w)/len(g)*100:>4.0f}%  {ev:>+7.1f}%")

    print('\n' + '=' * 72)
    n_sig = len(processed)
    n_mc  = sum(1 for s in signals
                if not s.get('is_ath')
                and 0 < (s.get('market_cap') or 0) <= 30_000
                and (s.get('token_ca'), s.get('ts')) in processed)
    print(f'总信号={n_sig}  MC≤30K候选≈{mom_eligible}  动量确认交易={moma_stats["n"]}')
    print('注: 样本量小，数据仅供参考。请结合实盘验证。')
    print('=' * 72)


if __name__ == '__main__':
    main()
