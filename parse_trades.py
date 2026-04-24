import re
from datetime import datetime

with open('temp_logs.txt', 'r') as f:
    lines = f.readlines()

entered = {}
closed = {}
sl_checks = {}

# Regex patterns
enter_pattern = re.compile(r'(2026-\d+-\d+ \d+:\d+:\d+) \[INFO\]\s+Entered (.*?)/stage1 @ [\d\.]+ .*? mode=(.*?) ')
close_pattern = re.compile(r'(2026-\d+-\d+ \d+:\d+:\d+) \[INFO\]\s+Closed (.*?)/stage1.*?pnl=([+\-]?[\d\.]+%)(.*)')
sl_pattern = re.compile(r'(2026-\d+-\d+ \d+:\d+:\d+) \[INFO\] \[ExitGuardian\] ⚠️ (.*?) SL CHECK #1: pnl=([+\-]?[\d\.]+%)(.*)')

for line in lines:
    m_enter = enter_pattern.search(line)
    if m_enter:
        ts, symbol, mode = m_enter.groups()
        if symbol not in entered: entered[symbol] = []
        entered[symbol].append({'ts': ts, 'mode': mode})
        continue
        
    m_close = close_pattern.search(line)
    if m_close:
        ts, symbol, pnl, reason = m_close.groups()
        if symbol not in closed: closed[symbol] = []
        closed[symbol].append({'ts': ts, 'pnl': pnl, 'reason': reason.strip()})
        continue
        
    m_sl = sl_pattern.search(line)
    if m_sl:
        ts, symbol, pnl, extra = m_sl.groups()
        if symbol not in sl_checks: sl_checks[symbol] = []
        sl_checks[symbol].append({'ts': ts, 'pnl': pnl})

print(f"Total Unique Tokens Traded: {len(entered)}")
print("-" * 50)

for symbol, entries in entered.items():
    print(f"\n🪙  Token: {symbol}")
    for i, e in enumerate(entries):
        mode = e['mode']
        enter_time = e['ts']
        print(f"   [Entry {i+1}] {enter_time} | Mode: {mode}")
        
    if symbol in sl_checks:
        for sl in sl_checks[symbol]:
            print(f"   [Guardian Alert] {sl['ts']} | Dropped to: {sl['pnl']}")
            
    if symbol in closed:
        for c in closed[symbol]:
            print(f"   [Closed] {c['ts']} | PNL: {c['pnl']} | {c['reason']}")
    else:
        print("   [Status] Currently Open / Or no exit log found before 50k line cutoff")

