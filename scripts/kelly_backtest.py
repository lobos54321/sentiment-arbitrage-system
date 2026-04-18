#!/usr/bin/env python3
import re

with open("paper_logs2.json") as f:
    lines = f.readlines()

last_start_idx = 652
deploy_lines = lines[last_start_idx:]

# Step 1: Extract Kelly size for each token from FIRE lines
kelly_map = {}
for line in deploy_lines:
    m = re.search(r"FIRE (\S+)! Scores:.*Kelly: ([\d\.]+) SOL", line)
    if m:
        sym = m.group(1)
        ks = float(m.group(2))
        kelly_map[sym] = ks  # keep last FIRE (closest to actual entry)

print("=== Kelly sizes per token ===")
for k, v in sorted(kelly_map.items()):
    print(f"  {k:14s}: {v:.3f} SOL")
print()

# Step 2: Extract closed trades
closed = []
for line in deploy_lines:
    if "CLOSED " in line and "stage" in line:
        tok = re.search(r"CLOSED (\S+)/stage", line)
        pnl = re.search(r"trigger_pnl=([+\-\d\.]+)%", line)
        qout = re.search(r"quote_out=([\d\.]+)", line)
        reason_m = re.search(r"CLOSED \S+: (\S+)", line)
        ts_m = re.search(r"(\d{2}:\d{2}:\d{2})", line)
        if tok and pnl:
            closed.append({
                "symbol": tok.group(1),
                "pnl_pct": float(pnl.group(1)) / 100.0,
                "exit_sol_actual": float(qout.group(1)) if qout else None,
                "reason": reason_m.group(1) if reason_m else "?",
                "ts": ts_m.group(1) if ts_m else "?",
            })

FIXED_SIZE = 0.06

print("=" * 75)
print(f"  Token          Kelly    PnL%     Fixed P&L   Kelly P&L  Reason")
print("=" * 75)

total_fixed_in = 0.0
total_fixed_out = 0.0
total_kelly_in = 0.0
total_kelly_out = 0.0

rows = []
for t in closed:
    sym = t["symbol"]
    pnl_pct = t["pnl_pct"]

    kelly_size = kelly_map.get(sym)
    if kelly_size is None:
        for k, v in kelly_map.items():
            if k.lower() == sym.lower():
                kelly_size = v
                break
    if kelly_size is None:
        kelly_size = FIXED_SIZE

    fixed_net = FIXED_SIZE * pnl_pct
    kelly_net = kelly_size * pnl_pct

    total_fixed_in += FIXED_SIZE
    total_fixed_out += FIXED_SIZE * (1 + pnl_pct)
    total_kelly_in += kelly_size
    total_kelly_out += kelly_size * (1 + pnl_pct)

    marker = "WIN" if pnl_pct > 0 else "LOSS"
    rows.append((sym, kelly_size, pnl_pct, fixed_net, kelly_net, t["reason"], marker))
    print(f"  [{marker:4s}] {sym:10s}  {kelly_size:5.3f}S  {pnl_pct*100:+7.1f}%   {fixed_net:+9.5f}S  {kelly_net:+9.5f}S  {t['reason']}")

print("=" * 75)
fixed_net_total = total_fixed_out - total_fixed_in
kelly_net_total = total_kelly_out - total_kelly_in
fixed_roi = fixed_net_total / total_fixed_in * 100 if total_fixed_in > 0 else 0
kelly_roi = kelly_net_total / total_kelly_in * 100 if total_kelly_in > 0 else 0

print(f"\n  === FIXED SIZE (0.06 SOL per trade) ===")
print(f"  Total invested : {total_fixed_in:.3f} SOL")
print(f"  Net P&L        : {fixed_net_total:+.5f} SOL")
print(f"  ROI on capital : {fixed_roi:+.2f}%")

print(f"\n  === KELLY SIZING ===")
print(f"  Total invested : {total_kelly_in:.3f} SOL")
print(f"  Net P&L        : {kelly_net_total:+.5f} SOL")
print(f"  ROI on capital : {kelly_roi:+.2f}%")

if fixed_net_total != 0:
    improvement = (kelly_net_total - fixed_net_total)
    pct_change = (kelly_net_total / fixed_net_total - 1) * 100 if fixed_net_total > 0 else float('inf')
    print(f"\n  Delta (Kelly - Fixed): {improvement:+.5f} SOL ({pct_change:+.1f}%)")

# Breakdown: wins vs losses
win_kelly_in = sum((kelly_map.get(t["symbol"]) or FIXED_SIZE) for t in closed if t["pnl_pct"] > 0)
win_kelly_net = sum((kelly_map.get(t["symbol"]) or FIXED_SIZE) * t["pnl_pct"] for t in closed if t["pnl_pct"] > 0)
loss_kelly_in = sum((kelly_map.get(t["symbol"]) or FIXED_SIZE) for t in closed if t["pnl_pct"] < 0)
loss_kelly_net = sum((kelly_map.get(t["symbol"]) or FIXED_SIZE) * t["pnl_pct"] for t in closed if t["pnl_pct"] < 0)

win_fixed_net = sum(FIXED_SIZE * t["pnl_pct"] for t in closed if t["pnl_pct"] > 0)
loss_fixed_net = sum(FIXED_SIZE * t["pnl_pct"] for t in closed if t["pnl_pct"] < 0)

print(f"\n  --- 盈亏归因 ---")
print(f"  盈利单 ({sum(1 for t in closed if t['pnl_pct']>0)}笔):")
print(f"    Fixed:  入场 {sum(1 for t in closed if t['pnl_pct']>0)*FIXED_SIZE:.3f} SOL   盈利 {win_fixed_net:+.5f} SOL")
print(f"    Kelly:  入场 {win_kelly_in:.3f} SOL   盈利 {win_kelly_net:+.5f} SOL")
print(f"  亏损单 ({sum(1 for t in closed if t['pnl_pct']<0)}笔):")
print(f"    Fixed:  入场 {sum(1 for t in closed if t['pnl_pct']<0)*FIXED_SIZE:.3f} SOL   亏损 {loss_fixed_net:+.5f} SOL")
print(f"    Kelly:  入场 {loss_kelly_in:.3f} SOL   亏损 {loss_kelly_net:+.5f} SOL")
