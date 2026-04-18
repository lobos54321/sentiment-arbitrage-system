#!/usr/bin/env python3
"""
深度分析：Kelly 在我们系统上到底靠不靠谱？

核心问题：Kelly 给的仓位大小，是否和实际交易结果正相关？
如果 Kelly 给大单的信号真的赚更多，Kelly 就靠谱。
如果 Kelly 给大单但亏得更多，说明 Kelly 的信号评估有问题。
"""

# 12笔交易数据（Guardian Trail 修复后的出场）
trades = [
    # (symbol, kelly_sol, new_exit_pnl%, reason)
    ("FOF",       0.882,   +7.8,  "trail"),
    ("GoyStory",  0.918,  -15.8,  "hard_sl"),
    ("Work",      1.000,   +6.3,  "trail"),
    ("Autoncorp", 1.000,   +7.4,  "trail"),
    ("Coco",      0.628,  +47.8,  "trail"),
    ("SIZE_1",    1.000,   +8.8,  "trail"),
    ("Yuji",      0.570,   +6.2,  "trail"),
    ("bullunc",   1.000,  -15.2,  "hard_sl"),
    ("SIZE_2",    1.000,  -15.2,  "hard_sl"),
    ("Poong",     1.000,  +12.8,  "trail"),
    ("BELKA",     0.654,   +9.1,  "trail"),
    ("INUUNC",    1.000,   +3.5,  "moon_trail"),
]

# ═══ 分析1: Kelly 仓位 vs 实际结果相关性 ═══
print("=" * 70)
print("  分析1: Kelly 给的仓位大小 vs 实际交易结果")
print("=" * 70)

# 按 Kelly 分组
high_kelly = [(s, k, p) for s, k, p, _ in trades if k >= 0.9]   # 大仓（≥0.9 SOL）
low_kelly  = [(s, k, p) for s, k, p, _ in trades if k < 0.9]    # 小仓（<0.9 SOL）

print(f"\n  大仓组 (kelly >= 0.9 SOL): {len(high_kelly)} 笔")
for s, k, p in high_kelly:
    print(f"    {s:12s}  kelly={k:.3f}S  result={p:+.1f}%")
h_avg = sum(p for _, _, p in high_kelly) / len(high_kelly)
h_wins = sum(1 for _, _, p in high_kelly if p > 0)
print(f"    平均PnL: {h_avg:+.1f}%  胜率: {h_wins}/{len(high_kelly)} ({h_wins/len(high_kelly)*100:.0f}%)")

print(f"\n  小仓组 (kelly < 0.9 SOL): {len(low_kelly)} 笔")
for s, k, p in low_kelly:
    print(f"    {s:12s}  kelly={k:.3f}S  result={p:+.1f}%")
l_avg = sum(p for _, _, p in low_kelly) / len(low_kelly)
l_wins = sum(1 for _, _, p in low_kelly if p > 0)
print(f"    平均PnL: {l_avg:+.1f}%  胜率: {l_wins}/{len(low_kelly)} ({l_wins/len(low_kelly)*100:.0f}%)")

print(f"\n  → 大仓组 avg={h_avg:+.1f}%  vs  小仓组 avg={l_avg:+.1f}%")
if h_avg < l_avg:
    print(f"  ⚠️ Kelly 给大仓的信号反而表现更差！")
else:
    print(f"  ✅ Kelly 给大仓的信号表现更好")

# ═══ 分析2: Kelly 的区分度 ═══
print("\n\n" + "=" * 70)
print("  分析2: Kelly 的区分度（是否只是给所有单打了差不多的分？）")
print("=" * 70)

kelly_vals = sorted(set(k for _, k, _, _ in trades))
print(f"\n  Kelly 仓位分布:")
for kv in kelly_vals:
    count = sum(1 for _, k, _, _ in trades if k == kv)
    tokens = [s for s, k, _, _ in trades if k == kv]
    print(f"    {kv:.3f} SOL: {count}笔 → {', '.join(tokens)}")

# 统计
at_cap = sum(1 for _, k, _, _ in trades if k >= 0.99)
print(f"\n  满仓(≥0.99 SOL): {at_cap}/{len(trades)} ({at_cap/len(trades)*100:.0f}%)")
print(f"  非满仓:          {len(trades)-at_cap}/{len(trades)} ({(len(trades)-at_cap)/len(trades)*100:.0f}%)")

if at_cap / len(trades) > 0.6:
    print(f"  ⚠️ 超过60%的单都是满仓 — Kelly 区分度不足！")

# ═══ 分析3: Kelly 亏损单分析 ═══
print("\n\n" + "=" * 70)
print("  分析3: Kelly 给大仓的亏损单有多危险？")
print("=" * 70)

losses = [(s, k, p) for s, k, p, _ in trades if p < 0]
print(f"\n  亏损单:")
total_loss = 0
for s, k, p in losses:
    loss_sol = k * abs(p) / 100
    total_loss += loss_sol
    print(f"    {s:12s}  kelly={k:.3f}S  亏损={p:+.1f}%  实际亏={loss_sol:.4f} SOL")
print(f"  总亏损: {total_loss:.4f} SOL")

wins = [(s, k, p) for s, k, p, _ in trades if p > 0]
total_win = sum(k * p / 100 for _, k, p in wins)
print(f"  总盈利: {total_win:.4f} SOL")
print(f"  净结果: {total_win - total_loss:+.4f} SOL")

# 最大单笔亏损
max_loss = max(losses, key=lambda x: x[1] * abs(x[2]))
max_loss_sol = max_loss[1] * abs(max_loss[2]) / 100
print(f"\n  最大单笔亏损: {max_loss[0]} → {max_loss_sol:.4f} SOL ({max_loss[1]:.3f}S × {max_loss[2]:.1f}%)")
print(f"  如果账户有 5 SOL, 最大单笔亏 {max_loss_sol/5*100:.1f}% 的账户")

# ═══ 分析4: 为什么 Kelly 给大部分单满仓？ ═══
print("\n\n" + "=" * 70)
print("  分析4: 为什么 Kelly 区分度低？(追根溯源)")
print("=" * 70)

# Kelly 公式: f* = (p*b - q) / b, position = min(f* * bankroll * 0.5, max_cap)
# 如果 b 很大（cap=8），只要 p > 0.5，f* ≈ 0.5 → half kelly ≈ 0.25 * bankroll
# bankroll=5 SOL → position = 0.25 * 5 = 1.25, 但 max_position_sol 限制在 1.0

# 模拟不同 p 和 b 下的 kelly position
print("\n  Kelly 公式仓位计算:")
print(f"  公式: f* = (p×b - q) / b, 仓位 = min(f* × bankroll × 0.5, 1.0)")
print(f"  bankroll = 5 SOL, half-kelly, max = 1.0 SOL")
print()
print(f"  {'p':>5s} {'b':>5s} {'f*':>7s} {'half_f':>7s} {'raw_pos':>8s} {'capped':>7s}")
print(f"  {'-'*5} {'-'*5} {'-'*7} {'-'*7} {'-'*8} {'-'*7}")

for p in [0.45, 0.50, 0.55, 0.60, 0.65, 0.70]:
    for b in [3.0, 8.0]:
        q = 1 - p
        f_star = (p * b - q) / b
        half_f = f_star * 0.5
        raw_pos = half_f * 5.0
        capped = min(max(raw_pos, 0), 1.0)
        marker = " ←cap" if raw_pos > 1.0 else ""
        print(f"  {p:5.2f} {b:5.1f} {f_star:7.4f} {half_f:7.4f} {raw_pos:7.3f}S {capped:6.3f}S{marker}")

print()
print("  关键发现:")
print("  当 b=8.0, p≥0.50 时, raw_pos = 1.094+ SOL → 全部被 cap 到 1.0 SOL")
print("  只有 p<0.50 (胜率低于50%) 的信号才会得到小仓位")
print("  → b_real cap 太高反而让 Kelly 失去了区分度!")
print()

# ═══ 分析5: Kelly 在不同 b_cap 下的表现 ═══
print("=" * 70)
print("  分析5: 如果 b_cap 调低，Kelly 会怎样？")
print("=" * 70)

# 用不同 b 重算每笔交易的 Kelly position
# 每笔交易的 p 值（从日志的 kelly log 可以看）
# 简化：从 kelly_sol 反推 p 值
# f* = (p*b - q)/b, half_kelly = f*/2, pos = min(half_kelly * 5, 1.0)
# 如果 pos = 1.0 (capped), 我们不知道真实 p
# 如果 pos < 1.0, pos = f*/2 * 5 = (p*b - q)/(2*b) * 5

# 已知 kelly position 和 b=3.0 的情况下反推 p:
# pos = min((p*b - (1-p)) / b / 2 * bankroll, max_pos)
# 对于 non-capped: pos = (p*3 - (1-p)) / 3 / 2 * 5
# pos = (3p - 1 + p) / 6 * 5 = (4p - 1) / 6 * 5
# p = (pos * 6/5 + 1) / 4

b_old = 3.0
bankroll = 5.0
max_pos = 1.0

estimated_p = {}
for sym, kelly, pnl, _ in trades:
    if kelly >= 0.99:
        # Capped — p is unknown but > threshold
        # threshold: (p*3 - (1-p))/3/2*5 >= 1.0 → p >= 0.55
        estimated_p[sym] = 0.60  # conservative guess for capped
    else:
        # Not capped: kelly = (p*b_old - (1-p)) / b_old / 2 * bankroll
        # kelly = (p*3 - 1 + p) / 6 * 5 = (4p-1)/6*5
        # (4p-1) = kelly * 6/5
        # p = (kelly * 6/5 + 1) / 4
        p_est = (kelly * 6 / 5 + 1) / 4
        estimated_p[sym] = round(p_est, 3)

print(f"\n  各信号的估算 p (胜率) [从 b=3.0 反推]:")
for sym, kelly, pnl, _ in trades:
    p = estimated_p[sym]
    capped = "capped" if kelly >= 0.99 else ""
    print(f"    {sym:12s}  kelly={kelly:.3f}S  p≈{p:.3f}  result={pnl:+.1f}%  {capped}")

print(f"\n  用不同 b_cap 重算仓位和 P&L (Guardian Trail 出场):")
for b_test in [2.0, 3.0, 5.0, 8.0, 13.0]:
    total_pnl = 0
    total_invest = 0
    sizes = []
    for sym, _, pnl, _ in trades:
        p = estimated_p[sym]
        q = 1 - p
        f_star = (p * b_test - q) / b_test
        half_f = max(f_star * 0.5, 0)
        pos = min(half_f * bankroll, max_pos)
        pos = max(pos, 0.01)  # minimum
        total_pnl += pos * pnl / 100
        total_invest += pos
        sizes.append(pos)
    
    avg_size = sum(sizes) / len(sizes)
    at_cap = sum(1 for s in sizes if s >= 0.99)
    roi = total_pnl / total_invest * 100 if total_invest > 0 else 0
    print(f"    b_cap={b_test:5.1f}  avg_size={avg_size:.3f}S  at_cap={at_cap}/12  invest={total_invest:.3f}S  P&L={total_pnl:+.4f}S  ROI={roi:+.1f}%")

print()
print("=" * 70)
print("  最终结论")
print("=" * 70)
