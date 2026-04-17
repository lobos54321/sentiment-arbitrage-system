#!/usr/bin/env python3
"""
两个问题的数据验证：
1. Guardian 到底在用什么价格源？各笔出场的 src 来源
2. Trail Stop 的 242pp 丢失到底是"配置问题"还是"检测频率问题"
"""
import re

with open("paper_logs2.json") as f:
    lines = f.readlines()

deploy_lines = lines[652:]

# ── Q1: 每笔出场用了什么价格源？─────────────────────────
print("=" * 80)
print("  Q1: Guardian SL 和 EXIT_MATRIX 分别用了什么价格源？")
print("=" * 80)

# Find Guardian SL triggers with price source
print("\n  --- Guardian 触发时的价格源 ---")
for line in deploy_lines:
    if "ExitGuardian" in line and ("EMERGENCY" in line or "MOON" in line):
        ts = re.search(r"(\d{2}:\d{2}:\d{2})", line)
        tok = re.search(r"] .*?(\S+) (EMERGENCY|MOON)", line)
        if ts and tok:
            print(f"    {ts.group(1)} {tok.group(1):12s} {line.strip()[-80:]}")

# Find PRE_PRICE source tags for each position
print("\n  --- EXIT_MATRIX 评估时的价格源 ---")
src_counts = {}
for line in deploy_lines:
    m = re.search(r"\[PRE_PRICE\] (\S+): \$[\d\.]+ src=(\S+) age_ms=(\S+)", line)
    if m:
        sym = m.group(1)
        src = m.group(2)
        key = f"{sym}:{src}"
        src_counts[key] = src_counts.get(key, 0) + 1

# sorted summary
by_token = {}
for key, cnt in src_counts.items():
    sym, src = key.split(":")
    by_token.setdefault(sym, []).append((src, cnt))

for sym in sorted(by_token.keys()):
    srcs = by_token[sym]
    total = sum(c for _, c in srcs)
    src_str = ", ".join(f"{s}={c}" for s, c in sorted(srcs, key=lambda x: -x[1]))
    print(f"    {sym:12s}: {total:3d} evals → {src_str}")

# ── Q2: Trail 的 242pp 丢失分析 ─────────────────────────
print("\n\n" + "=" * 80)
print("  Q2: Trail Stop 242pp 丢失的真正原因 — 评估间隔分析")
print("=" * 80)

# For each token, find the EXIT_MATRIX evaluations and measure:
# 1. The time gap between the PEAK evaluation and the TRIGGERED evaluation
# 2. How fast PnL dropped
tokens_of_interest = ["FOF", "Work", "Autoncorp", "SIZE", "Yuji", "Poong", "BELKA"]

for tok in tokens_of_interest:
    evals = []
    for line in deploy_lines:
        m = re.search(rf"\[EXIT_MATRIX\] {tok}/stage\S* action=(\S+) pnl=([+\-\d\.]+)%.*held=(\d+)min", line)
        if m:
            ts = re.search(r"(\d{2}:\d{2}:\d{2})", line)
            evals.append({
                'ts': ts.group(1) if ts else "?",
                'action': m.group(1),
                'pnl': float(m.group(2)),
                'held': int(m.group(3)),
            })
    
    if not evals:
        continue
    
    # Find peak and exit point
    peak_idx = max(range(len(evals)), key=lambda i: evals[i]['pnl'])
    peak = evals[peak_idx]
    last = evals[-1]
    
    # Time between peak eval and last eval (exit)
    peak_ts = peak['ts']
    last_ts = last['ts']
    
    # Parse time diff
    def ts_to_sec(t):
        h, m, s = map(int, t.split(":"))
        return h * 3600 + m * 60 + s
    
    dt = ts_to_sec(last_ts) - ts_to_sec(peak_ts)
    pnl_drop = peak['pnl'] - last['pnl']
    drop_rate = pnl_drop / max(dt, 1) if dt > 0 else float('inf')
    
    # Count evaluation gaps
    gaps = []
    for i in range(1, len(evals)):
        gap = ts_to_sec(evals[i]['ts']) - ts_to_sec(evals[i-1]['ts'])
        gaps.append(gap)
    
    avg_gap = sum(gaps) / len(gaps) if gaps else 0
    max_gap = max(gaps) if gaps else 0
    
    print(f"\n  {tok}: {len(evals)} evals, peak={peak['pnl']:+.1f}% @ {peak_ts}, exit={last['pnl']:+.1f}% @ {last_ts}")
    print(f"    Peak→Exit: {dt}s, drop={pnl_drop:.1f}pp, speed={drop_rate:.2f}pp/s")
    print(f"    评估间隔: avg={avg_gap:.0f}s, max={max_gap:.0f}s")
    
    # Trail floor at peak
    peak_dec = peak['pnl'] / 100.0
    if peak_dec >= 0.05:
        floor = peak_dec * 0.5 if peak_dec < 0.20 else peak_dec * 0.6
        print(f"    Trail floor = {floor*100:.1f}% (peak={peak['pnl']:.1f}% × {'50%' if peak_dec < 0.20 else '60%'})")
        print(f"    如果峰值时就出场: 拿到 {peak['pnl']:+.1f}% vs 实际 {last['pnl']:+.1f}% (差{pnl_drop:.1f}pp)")
        
        # Check: would higher evaluation frequency help?
        # If peak→exit is < 10 seconds, even 3s guardian can't save it
        if dt <= 10:
            print(f"    ⚡ 峰值到崩塌仅 {dt}s — 任何合理的检测频率都无法避免")
        elif dt <= 60:
            print(f"    ⚠️ 峰值到崩塌 {dt}s — 更高频检测(3s Guardian)可能能在 floor 处接住")
        else:
            print(f"    🐌 峰值到崩塌 {dt}s — 主循环评估间隔导致错过最佳出场点")
    
    # Evaluate: what would the exit PnL be if we had checked at each second?
    # Between peak eval and exit eval, the pnl went from peak to exit.
    # The trail floor was set at peak*0.5. The question is: at what point did
    # pnl cross below the floor? We only have discrete eval points.
    floor_pct = peak['pnl'] * 0.5 if peak['pnl'] < 20 else peak['pnl'] * 0.6
    
    crossed = None
    for i in range(peak_idx + 1, len(evals)):
        if evals[i]['pnl'] < floor_pct:
            crossed = evals[i]
            prev = evals[i-1] if i > 0 else None
            gap_to_cross = ts_to_sec(crossed['ts']) - ts_to_sec(evals[i-1]['ts'])
            print(f"    Floor crossed: #{i} pnl={crossed['pnl']:+.1f}% @ {crossed['ts']} (前一次:{evals[i-1]['pnl']:+.1f}% 间隔{gap_to_cross}s)")
            print(f"    理想出场(floor附近): ~{floor_pct:+.1f}%, 实际出场: {crossed['pnl']:+.1f}%, 差距: {floor_pct - crossed['pnl']:.1f}pp")
            break

print("\n\n" + "=" * 80)
print("  结论: 主循环评估间隔 vs Guardian 3s 间隔")
print("=" * 80)
print("  Trail stop 的触发逻辑由 EXIT_MATRIX 在主循环中执行（~60s 间隔）")
print("  而 Guardian 每 3s 检查一次，但 Guardian 只检查 hard_sl，不检查 trail floor")
print("  如果把 trail floor 检查也放到 Guardian 的 3s 循环中，")
print("  就能在价格刚跌破 floor 时立即出场，而不是等 60s 后发现已经跌了很多")
