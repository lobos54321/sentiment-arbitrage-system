#!/usr/bin/env python3
"""Analyze SPREAD_GUARD patterns and pc_m5 decay across all tokens in logs."""

import re
import sys
from collections import defaultdict

LOG_FILE = '/Users/boliu/.gemini/antigravity/brain/649d38c4-18ca-49da-a4fe-5f67fefc0772/.system_generated/steps/16124/content.md'

spread_guard_events = []
smart_entry_events = []
entry_price_events = []
close_events = []

with open(LOG_FILE, 'r') as f:
    for line in f:
        m = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*\[SPREAD_GUARD\].*?(\$?\w[\w]*) ABORT.*spread ([+\-\d.]+)%.*fill=([0-9.]+).*trigger=([0-9.]+)', line)
        if m:
            symbol = m.group(2).replace('$', '')
            spread_guard_events.append({'ts': m.group(1), 'symbol': symbol, 'spread': float(m.group(3)), 'fill': float(m.group(4)), 'trigger': float(m.group(5))})
            continue

        m = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*\[SmartEntry\] .*?(FAST_LANE|SMART_ENTRY).*Score=(\d+).*?(?:base=(\d+))?.*bs=([0-9.]+).*pc_m5=([+\-0-9.]+)%', line)
        if m:
            base = int(m.group(4)) if m.group(4) else None
            smart_entry_events.append({'ts': m.group(1), 'type': m.group(2), 'score': int(m.group(3)), 'base': base, 'bs': float(m.group(5)), 'pc_m5': float(m.group(6)), 'result': 'PASS'})
            continue

        m = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*\[ENTRY_PRICE\] (\$?\w[\w]*) entry_price=([0-9.]+).*trigger_was=([0-9.]+).*spread=([+\-0-9.]+)%', line)
        if m:
            symbol = m.group(2).replace('$', '')
            entry_price_events.append({'ts': m.group(1), 'symbol': symbol, 'entry': float(m.group(3)), 'trigger': float(m.group(4)), 'spread': float(m.group(5))})
            continue

        m = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*CLOSED (\w[\w]*)/stage1.*?pnl=([+\-0-9.]+)%.*peak=([+\-0-9.]+)%', line)
        if m:
            close_events.append({'ts': m.group(1), 'symbol': m.group(2), 'pnl': float(m.group(3)), 'peak': float(m.group(4))})

# Match SmartEntry PASS to symbols via ENTRY_PRICE timestamps
for se in smart_entry_events:
    if se['result'] != 'PASS': continue
    se_parts = se['ts'].split()
    se_hms = se_parts[1].split(':')
    se_sec = int(se_hms[0])*3600 + int(se_hms[1])*60 + int(se_hms[2])
    for ep in entry_price_events:
        ep_parts = ep['ts'].split()
        if se_parts[0] != ep_parts[0]: continue
        ep_hms = ep_parts[1].split(':')
        ep_sec = int(ep_hms[0])*3600 + int(ep_hms[1])*60 + int(ep_hms[2])
        if 0 <= (ep_sec - se_sec) <= 20:
            se['symbol'] = ep['symbol']
            se['spread_at_entry'] = ep['spread']
            break

print("=" * 80)
print("ANALYSIS 1: SPREAD_GUARD abort events by token")
print("=" * 80)
sg_by_symbol = defaultdict(list)
for ev in spread_guard_events:
    sg_by_symbol[ev['symbol']].append(ev)

for sym, events in sorted(sg_by_symbol.items()):
    print(f"\n{sym}: {len(events)} abort(s)")
    for ev in events:
        print(f"  {ev['ts']}  spread={ev['spread']:+.1f}%  trigger={ev['trigger']:.10f}")

print("\n" + "=" * 80)
print("ANALYSIS 2: For tokens with SPREAD_GUARD aborts -> did they enter? Outcome?")
print("=" * 80)

for sym, sg_events in sorted(sg_by_symbol.items()):
    entries = [e for e in entry_price_events if e['symbol'] == sym]
    closes = [c for c in close_events if c['symbol'] == sym]
    
    # Find SmartEntry PASS events matched to this symbol
    se_for_sym = [se for se in smart_entry_events if se.get('symbol') == sym and se['result'] == 'PASS']
    
    print(f"\n{'─'*60}")
    print(f"  {sym}: {len(sg_events)} SPREAD_GUARD abort(s)")
    
    # Show pc_m5 at each SmartEntry attempt
    all_attempts = []
    for se in se_for_sym:
        all_attempts.append({'ts': se['ts'], 'type': 'SE_PASS', 'pc_m5': se['pc_m5'], 'score': se['score'], 'base': se.get('base')})
    for sg in sg_events:
        all_attempts.append({'ts': sg['ts'], 'type': 'SG_ABORT', 'spread': sg['spread']})
    for entry in entries:
        all_attempts.append({'ts': entry['ts'], 'type': 'ENTRY', 'spread': entry['spread'], 'price': entry['entry']})
    
    all_attempts.sort(key=lambda x: x['ts'])
    
    for a in all_attempts:
        if a['type'] == 'SE_PASS':
            base_str = f"base={a['base']}" if a['base'] else ""
            print(f"    {a['ts']}  SmartEntry PASS  Score={a['score']} {base_str} pc_m5={a['pc_m5']:+.1f}%")
        elif a['type'] == 'SG_ABORT':
            print(f"    {a['ts']}  SPREAD_GUARD ABORT spread={a['spread']:+.1f}%")
        elif a['type'] == 'ENTRY':
            print(f"    {a['ts']}  → ENTERED spread={a['spread']:+.1f}%")
    
    for close in closes:
        win = "✅ WIN" if close['pnl'] > 0 else "❌ LOSS"
        print(f"    {close['ts']}  → CLOSED pnl={close['pnl']:+.1f}%  peak={close['peak']:+.1f}%  {win}")

print("\n" + "=" * 80)
print("ANALYSIS 3: ALL entries with spread level → outcome")
print("=" * 80)

entries_with_outcome = []
for ep in entry_price_events:
    closes_after = [c for c in close_events if c['symbol'] == ep['symbol'] and c['ts'] >= ep['ts']]
    if closes_after:
        entries_with_outcome.append({**ep, 'pnl': closes_after[0]['pnl'], 'peak': closes_after[0]['peak']})

spread_bins = {'<0%': [], '0-2%': [], '2-3%': [], '3-5%': [], '>5%': []}
for e in entries_with_outcome:
    s = e['spread']
    if s < 0: spread_bins['<0%'].append(e)
    elif s < 2: spread_bins['0-2%'].append(e)
    elif s < 3: spread_bins['2-3%'].append(e)
    elif s < 5: spread_bins['3-5%'].append(e)
    else: spread_bins['>5%'].append(e)

for bin_name, trades in spread_bins.items():
    if trades:
        wins = sum(1 for t in trades if t['pnl'] > 0)
        avg_pnl = sum(t['pnl'] for t in trades) / len(trades)
        print(f"\n  Spread {bin_name}: {len(trades)} trades, {wins} wins ({100*wins/len(trades):.0f}%), avg_pnl={avg_pnl:+.1f}%")
        for t in trades:
            win_str = "✅" if t['pnl'] > 0 else "❌"
            print(f"    {t['symbol']:15s} spread={t['spread']:+.1f}%  pnl={t['pnl']:+.1f}%  peak={t['peak']:+.1f}%  {win_str}")

print("\n" + "=" * 80)
print("ANALYSIS 4: pc_m5 at SmartEntry vs outcome (all tokens with matched data)")
print("=" * 80)

se_with_outcome = []
for se in smart_entry_events:
    if se['result'] == 'PASS' and 'symbol' in se:
        sym = se['symbol']
        closes_after = [c for c in close_events if c['symbol'] == sym and c['ts'] >= se['ts']]
        if closes_after:
            se_with_outcome.append({**se, 'pnl': closes_after[0]['pnl'], 'peak': closes_after[0]['peak']})

se_with_outcome.sort(key=lambda x: x['pc_m5'])
print(f"\n  {'Symbol':15s} {'pc_m5':>8s} {'Score':>6s} {'PnL':>7s} {'Peak':>7s} Result")
print(f"  {'─'*60}")
for se in se_with_outcome:
    win_str = "✅ WIN" if se['pnl'] > 0 else "❌ LOSS"
    print(f"  {se.get('symbol','?'):15s} {se['pc_m5']:+7.1f}% {se['score']:5d}  {se['pnl']:+6.1f}%  {se['peak']:+6.1f}%  {win_str}")

print("\n" + "=" * 80)
print("SUMMARY STATS")
print("=" * 80)
print(f"  Total SPREAD_GUARD aborts: {len(spread_guard_events)}")
print(f"  Unique tokens aborted: {len(sg_by_symbol)}")
print(f"  Total entries tracked: {len(entry_price_events)}")
print(f"  Total closes tracked: {len(close_events)}")
print(f"  Entries with outcome: {len(entries_with_outcome)}")
