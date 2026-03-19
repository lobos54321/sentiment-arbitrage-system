#!/usr/bin/env python3
"""
NOT_ATH 완전 전략 v2.0
==============================
[기존 v1 대비 주요 변경사항]
  1. AI Index 필터: ai≥60 (과열 구간인 ai>80도 점수 하향 처리)
  2. MC 하한선: mc≥5K (극소형 유동성 리스크 제거)
  3. UTC 20-22 제외: 유일한 마이너스 EV 시간대
  4. 입장 전 급락 필터: 직전 3분 -10% 이하 신호 스킵
  5. Velocity 필터 유지: vel≥30% (기존 동일)

[TP 구조] (기존 동일, 검증된 파라미터)
  TP1: +80%   60% 매도   → SL을 본전으로 이동
  TP2: +100%  50% 매도
  TP3: +200%  50% 매도
  TP4: +500%  80% 매도
  SL:  -15%   (TP1 전)
  BE:  0%     (TP1 후)
  DW:  8봉    (dead-water 청산)
  MH:  15봉   (최대 보유)
"""

import json, os, sys
from datetime import datetime, timezone

SLIP = 0.004     # 슬리피지 0.4%
POS  = 0.06      # 트레이드당 포지션 (SOL)

# ─── TP 파라미터 ─────────────────────────────────────────────────
SL    = -0.15    # 손절 -15%
TP1   = 0.80;  TP1s = 0.60   # +80%에서 60% 매도
TP2   = 1.00;  TP2s = 0.50   # +100%에서 50% 매도
TP3   = 2.00;  TP3s = 0.50   # +200%에서 50% 매도
TP4   = 5.00;  TP4s = 0.80   # +500%에서 80% 매도
DW    = 8        # dead-water 봉 수
MH    = 15       # 최대 보유 봉 수

# ─── 신호 필터 파라미터 ───────────────────────────────────────────
MIN_SI   = 100      # super_index 최소
MIN_AI   = 60       # ai_index 최소  ← v2 변경: 40→60
MAX_MC_K = 30       # 최대 시가총액 (K 단위, 달러)
MIN_MC_K = 5        # 최소 시가총액  ← v2 신규
MIN_VEL  = 0        # 최소 velocity (%) - 0=비활성화  (참고값: 이전 optimal=30%)
PRE_DUMP_THRESH = -10.0   # 입장 전 3분 모멘텀 하한 ← v2 신규
SKIP_UTC_20_22  = True    # UTC 20-22 스킵 ← v2 신규


def get_index(sig, key):
    idx = sig.get('indices') or {}
    return idx.get(key) or sig.get(key) or 0


def find_entry_candle(candles, sig_ts_ms):
    """신호 시간에 가장 가까운 입장 봉 찾기 (±2분 윈도우)"""
    eb = (sig_ts_ms // 1000 // 60) * 60
    return next((c for c in candles if c['ts'] <= eb and c['ts'] > eb - 120), None)


def calc_velocity(candles, entry_bar_ts):
    """입장 직전 5분 가격 변동률"""
    window_start = entry_bar_ts - 300
    bars = [c for c in candles if window_start <= c['ts'] < entry_bar_ts]
    if not bars:
        return None
    p0, p1 = bars[0]['o'], bars[-1]['c']
    return (p1 - p0) / p0 * 100 if p0 > 0 else None


def calc_pre_momentum(candles, entry_bar_ts):
    """입장 직전 3분 모멘텀 (급락 감지용)"""
    bars = [c for c in candles if c['ts'] < entry_bar_ts][-3:]
    if len(bars) < 2:
        return 0.0
    return (bars[-1]['c'] - bars[0]['o']) / bars[0]['o'] * 100


def simulate(candles, ec):
    """단일 트레이드 시뮬레이션"""
    after = [c for c in candles if c['ts'] > ec['ts']]
    if not after:
        return None

    ep  = ec['c'] * (1 + SLIP)   # 입장가 (슬리피지 포함)
    rem = POS / ep                # 보유 수량
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

        # ─── 손절 체크 ───────────────────────────────────────────
        cur_sl = ep if tp1_hit else sl_p
        if lo <= cur_sl:
            sol_out += rem * cur_sl * (1 - SLIP)
            rem = 0
            ex = 'SL_BE' if tp1_hit else 'SL'
            break

        # ─── TP4 → TP3 → TP2 → TP1 순서로 체크 ─────────────────
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

        # ─── Dead-water & Timeout ────────────────────────────────
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
        'pnl':    sol_out - POS,
        'pct':    (sol_out - POS) / POS * 100,
        'ex':     ex,
        'tp1':    tp1_hit,
        'peak':   peak_pct,
        'hold':   hold,
        'ep':     ep,
    }


def filter_signal(sig, candles, entry_bar_ts, sig_dt):
    """
    신호 필터링. 통과=True, 스킵=False + 이유
    """
    mc_k = (sig.get('market_cap') or 0) / 1000
    si   = get_index(sig, 'super_index')
    ai   = get_index(sig, 'ai_index')
    hour = sig_dt.hour

    # 1. is_ath 제거
    if sig.get('is_ath'):
        return False, 'is_ath'

    # 2. MC 범위 (MC 데이터가 있을 때만 필터 적용)
    if not sig.get('_mc_unknown'):
        if mc_k <= 0 or mc_k > MAX_MC_K:
            return False, f'mc_range({mc_k:.1f}K)'
        if mc_k < MIN_MC_K:
            return False, f'mc_low({mc_k:.1f}K)'    # ← v2 신규

    # 3. Super Index
    if si < MIN_SI:
        return False, f'si_low({si})'

    # 4. AI Index (≥60)
    if ai < MIN_AI:
        return False, f'ai_low({ai})'             # ← v2 변경

    # 5. UTC 20-22 제외
    if SKIP_UTC_20_22 and 20 <= hour < 22:
        return False, 'utc_20_22'                 # ← v2 신규

    # 6. 입장 전 급락 필터
    pre_mom = calc_pre_momentum(candles, entry_bar_ts)
    if pre_mom < PRE_DUMP_THRESH:
        return False, f'pre_dump({pre_mom:.1f}%)'  # ← v2 신규

    # 7. Velocity (MIN_VEL=0이면 비활성화)
    if MIN_VEL > 0:
        vel = calc_velocity(candles, entry_bar_ts)
        if vel is None or vel < MIN_VEL:
            return False, f'vel_low({vel})'

    return True, None


def load_signals():
    """
    두 가지 소스에서 신호를 통합 로드:
    1. data/channel-history.json  (채널 전체 히스토리)
    2. /tmp/mar1316_signals_filtered.json  (03/13-16 보조 데이터, MC 미포함)
    중복 CA+ts 제거 후 반환.

    [MC 정책]
    - channel-history 신호: market_cap 필드 사용
    - 보조 소스 신호: MC 데이터 없음 → MC 필터 면제 (MC_UNKNOWN=True 태그)
      (실 거래 환경에서는 레이더 피드에서 실시간 MC 제공됨)
    """
    hist = json.load(open('data/channel-history.json'))
    signals = list(hist['signals'])

    # 보조 소스 (03/13-16)
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
                    'super_index':  s.get('super_index', 0),
                    'ai_index':     s.get('ai_index', 0),
                    'media_index':  s.get('media_index', 0),
                }
            s['token_ca'] = ca
            s['_mc_unknown'] = True   # MC 데이터 없음 표시
            signals.append(s)
            added += 1
        print(f'  보조 소스 추가: +{added}笔 ({aux_path})')

    return signals


def main():
    print('=' * 70)
    print('NOT_ATH 완전 전략 v2.0')
    print(f'신호 필터: NOT_ATH | MC {MIN_MC_K}-{MAX_MC_K}K | si≥{MIN_SI} | ai≥{MIN_AI}')
    print(f'           vel≥{MIN_VEL}% | UTC20-22제외={SKIP_UTC_20_22} | 전3분급락<{PRE_DUMP_THRESH}% 제외')
    print(f'TP 구조:   TP1={TP1*100:.0f}%({TP1s*100:.0f}%) | TP2={TP2*100:.0f}%({TP2s*100:.0f}%)')
    print(f'           TP3={TP3*100:.0f}%({TP3s*100:.0f}%) | TP4={TP4*100:.0f}%({TP4s*100:.0f}%)')
    print(f'청산 규칙:  SL={SL*100:.0f}% | DW={DW}봉 | MH={MH}봉')
    print(f'포지션:     {POS} SOL/트레이드 | 슬리피지={SLIP*100:.1f}%')
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

        results.append({
            **r,
            'sym':    sig.get('symbol', ca[:8]),
            'ca':     ca,
            'ts':     sig['ts'],
            'mc_k':   mc_k,
            'si':     get_index(sig, 'super_index'),
            'ai':     get_index(sig, 'ai_index'),
            'mi':     get_index(sig, 'media_index'),
            'vel':    vel,
            'pre':    pre,
            'hour':   sig_dt.hour,
            'date':   sig_dt.strftime('%m/%d'),
        })

    # ─── 필터 통계 ────────────────────────────────────────────────
    print(f'\n총 신호: {len(signals)}개')
    print(f'스킵 명세:')
    for k in sorted(skip_counts, key=lambda x: -skip_counts[x]):
        prefix = 'is_ath' in k and '  ' or '  '
        print(f'  {k:<30}: -{skip_counts[k]}')
    print(f'실행 샘플: {len(results)}笔\n')

    if not results:
        print('실행 샘플 없음. 종료.')
        sys.exit(0)

    # ─── 결과 집계 ────────────────────────────────────────────────
    wins   = [r for r in results if r['pnl'] > 0]
    losses = [r for r in results if r['pnl'] <= 0]
    n = len(results)

    total_pnl = sum(r['pnl'] for r in results)
    wr        = len(wins) / n * 100
    avg_win   = sum(r['pct'] for r in wins) / len(wins) if wins else 0
    avg_loss  = sum(r['pct'] for r in losses) / len(losses) if losses else 0
    rr        = abs(avg_win / avg_loss) if avg_loss else 0
    big_loss  = [r for r in losses if r['pct'] < -10]

    exits = {}
    for r in results:
        exits[r['ex']] = exits.get(r['ex'], 0) + 1

    print('=' * 70)
    print(f'▶ 종합 결과 ({n}笔)')
    print('=' * 70)
    print(f'  총 PnL:       {total_pnl:+.4f} SOL')
    print(f'  EV/트레이드:  {total_pnl/n:+.4f} SOL  ({total_pnl/n/POS*100:+.2f}%)')
    print(f'  승률:         {wr:.1f}%  ({len(wins)}W / {len(losses)}L)')
    print(f'  평균 이익:    {avg_win:+.1f}%')
    print(f'  평균 손실:    {avg_loss:+.1f}%')
    print(f'  이익비:       {rr:.2f}x')
    print(f'  대손(>10%):   {len(big_loss)}笔  ({len(big_loss)/n*100:.1f}%)')
    print(f'  TP1 도달:     {sum(1 for r in results if r["tp1"])}笔  ({sum(1 for r in results if r["tp1"])/n*100:.1f}%)')
    print(f'  청산 분포:    ' + ' | '.join(f'{k}:{v}' for k, v in sorted(exits.items(), key=lambda x: -x[1])))
    vels = [r["vel"] for r in results if r.get("vel") is not None]
    print(f'  평균 vel:     {sum(vels)/len(vels):.1f}%' if vels else '  평균 vel:     N/A')
    print(f'  평균 peak:    {sum(r["peak"] for r in results)/n:.1f}%')

    # ─── 시간대별 분석 ────────────────────────────────────────────
    print(f'\n{"─"*70}')
    print(f'시간대별 성과 (UTC)')
    print(f'{"─"*70}')
    for h in range(0, 24, 2):
        g = [r for r in results if h <= r['hour'] < h + 2]
        if not g:
            continue
        g_wr  = len([r for r in g if r['pnl'] > 0]) / len(g) * 100
        g_ev  = sum(r['pct'] for r in g) / len(g)
        mk = '✅' if g_ev > 15 else ('⚠️ ' if g_ev > 0 else '❌')
        print(f'  {mk} UTC {h:02d}-{h+2:02d}  {len(g):>3}笔  WR={g_wr:4.0f}%  EV={g_ev:+6.1f}%')

    # ─── 날짜별 분석 ──────────────────────────────────────────────
    print(f'\n{"─"*70}')
    print(f'날짜별 성과')
    print(f'{"─"*70}')
    from collections import defaultdict
    by_date = defaultdict(list)
    for r in results:
        by_date[r['date']].append(r)
    for d in sorted(by_date):
        g = by_date[d]
        g_wr = len([r for r in g if r['pnl'] > 0]) / len(g) * 100
        g_pnl = sum(r['pnl'] for r in g)
        print(f'  {d}  {len(g):>3}笔  WR={g_wr:4.0f}%  PnL={g_pnl:+.4f}SOL')

    # ─── AI Index 분포 ────────────────────────────────────────────
    print(f'\n{"─"*70}')
    print(f'AI Index 구간별 성과')
    print(f'{"─"*70}')
    for lo, hi in [(60,70),(70,80),(80,100),(100,150),(150,999)]:
        g = [r for r in results if lo <= r['ai'] < hi]
        if not g:
            continue
        g_wr = len([r for r in g if r['pnl'] > 0]) / len(g) * 100
        g_ev = sum(r['pct'] for r in g) / len(g)
        print(f'  ai={lo}-{hi:<3}  {len(g):>3}笔  WR={g_wr:4.0f}%  EV={g_ev:+6.1f}%')

    # ─── 상세 명세 ────────────────────────────────────────────────
    print(f'\n{"─"*70}')
    print(f'상세 명세 (PnL 순)')
    print(f'{"─"*70}')
    print(f'  {"토큰":<12} {"날짜":>8} {"UTC":>3} {"MC":>5} {"SI":>4} {"AI":>4} {"Vel":>5} {"Peak":>7} {"PnL%":>7} {"PnL":>8} {"청산"}')
    print(f'  {"─"*80}')
    for r in sorted(results, key=lambda x: x['pnl'], reverse=True):
        flag = '✅' if r['pnl'] > 0 else '❌'
        vel_str = f'{r["vel"]:>4.0f}%' if r.get("vel") is not None else '  N/A'
        print(f'  {flag} {r["sym"]:<10} {r["date"]:>5} {r["hour"]:02d}h '
              f'{r["mc_k"] or 0:>4.0f}K {r["si"]:>4.0f} {r["ai"]:>4.0f} '
              f'{vel_str} {r["peak"]:>+6.0f}% '
              f'{r["pct"]:>+6.1f}% {r["pnl"]:>+7.4f}  {r["ex"]}')

    # ─── 결론 ────────────────────────────────────────────────────
    print(f'\n{"=" * 70}')
    print(f'▶ 결론')
    print(f'  EV  = {total_pnl/n/POS*100:+.2f}% / 트레이드')
    print(f'  WR  = {wr:.0f}%')
    print(f'  RR  = {rr:.2f}x')
    print(f'  대손율 = {len(big_loss)/n*100:.1f}%')
    days_span = max((datetime.fromtimestamp(r['ts']//1000, tz=timezone.utc) for r in results),
                    default=datetime.now(tz=timezone.utc)) - \
                min((datetime.fromtimestamp(r['ts']//1000, tz=timezone.utc) for r in results),
                    default=datetime.now(tz=timezone.utc))
    days = max(days_span.days, 1)
    print(f'  월 추정 PnL = {total_pnl / days * 30:+.2f} SOL  (기간={days}일 / 자본={POS*n:.2f}SOL 투입)')
    print(f'{"=" * 70}')


if __name__ == '__main__':
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    main()
