#!/usr/bin/env python3
# 深入分析：Kelly 回测结论解读

wins   = [4.1, 6.7, 8.1, 2.7, 4.5, 3.5]
losses = [1.1, 15.8, 20.8, 7.9, 15.2, 15.2]

avg_win  = sum(wins) / len(wins)
avg_loss = sum(losses) / len(losses)
b_real   = avg_win / avg_loss

print("=" * 58)
print("  本时段实际赔率分析（12笔交易）")
print("=" * 58)
print(f"  盈利单均值 avg_win  : +{avg_win:.2f}%")
print(f"  亏损单均值 avg_loss : -{avg_loss:.2f}%")
print(f"  实际 b (赔率比)     : {b_real:.3f}x")
print()

p = 0.5
q = 1 - p
b = b_real
kf = (p * b - q) / b
verdict = "正值: 此时 Kelly 仍建议下注" if kf > 0 else "负值: Kelly 认为此情形 EV 为负!"
print(f"  Kelly f* = ({p}x{b:.3f} - {q}) / {b:.3f} = {kf:.4f}")
print(f"  --> {verdict}")
print()
print(f"  满足正期望的最低 b >= 1.0 (即 avg_win >= avg_loss)")
print(f"  本时段 b={b_real:.3f} < 1.0, 说明这批样本里 赢小亏大")
print()
print("=" * 58)
print("  历史 b vs 本时段实际 b")
print("=" * 58)
b_hist = 206.2 / 15.2
kf_hist_50wr = (0.5 * b_hist - 0.5) / b_hist
kf_hist_cap  = (0.5 * 3.0  - 0.5) / 3.0
print(f"  长期历史 b (50笔): avg_win=206%/avg_loss=15% = {b_hist:.2f}x")
print(f"    Kelly f* (真实b, 50%wr): {kf_hist_50wr:.4f} -> 仓位 {5.0*kf_hist_50wr*0.5:.3f} SOL")
print(f"    Kelly f* (cap=3.0):      {kf_hist_cap:.4f} -> 仓位 {5.0*kf_hist_cap*0.5:.3f} SOL")
print()
print(f"  本时段实际 b: {b_real:.3f}x (赢太少、亏太多)")
print()
print("=" * 58)
print("  Kelly 回测 vs Fixed 对比总结")
print("=" * 58)
print(f"  Fixed (0.06 SOL x12):")
print(f"    总投入:  0.720 SOL")
print(f"    净盈亏: -0.028 SOL  (-3.87% ROI)")
print()
print(f"  Kelly 仓位:")
print(f"    总投入: 10.652 SOL  (平均 0.888 SOL/笔)")
print(f"    净盈亏: -0.354 SOL  (-3.32% ROI)")
print()
print(f"  ROI 对比: Kelly=-3.32% vs Fixed=-3.87%")
print(f"  Kelly 每单位投入的 ROI 略优 0.55pp")
print(f"  但绝对亏损额大 12.7x (因为注入了14.8x的资金)")
print()
print("=" * 58)
print("  核心问题诊断")
print("=" * 58)
print(f"  1. Trail Stop 太灵敏: 盈利单峰值 12-22% 但只拿到 2-8%")
print(f"     (FOF 峰值+15.7% 最终以 -1.1% 出来)")
print(f"  2. 本批 SL 单穿透深: Coco -20.8%, GoyStory -15.8%")
print(f"     理论 SL=-15%, 实际被滑点打穿")
print(f"  3. Kelly 方案在胜率50%且赔率<1时，应该减少下注")
print(f"     但被 b_real cap=3.0 高估了赔率，导致仍在大注")
