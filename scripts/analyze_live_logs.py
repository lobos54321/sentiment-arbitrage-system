import sys
import re
from collections import defaultdict

log_file = "/Users/boliu/.gemini/antigravity/brain/649d38c4-18ca-49da-a4fe-5f67fefc0772/.system_generated/steps/16810/content.md"

trades = defaultdict(lambda: {
    'symbol': '', 'entry_time': '', 'entry_price': 0, 'kelly': 0, 
    'peak': 0, 'exit_pnl': 0, 'exit_time': '', 'exit_reason': '', 'scores': '', 'spread': 0
})

with open(log_file, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        
        # FIRE Scores and Kelly
        m = re.search(r'\[WATCHLIST\] 🚀 FIRE (\w+)! Scores: (\{.*?\}) Kelly: ([\d\.]+) SOL', line)
        if m:
            sym = m.group(1)
            trades[sym]['scores'] = m.group(2)
            trades[sym]['kelly'] = float(m.group(3))
            
        # Entry Price and Spread
        m = re.search(r'\[ENTRY_PRICE\] (\w+) entry_price=.*? spread=([\+\-\d\.]+)%', line)
        if m:
            sym = m.group(1)
            trades[sym]['spread'] = float(m.group(2))
            
        # Entry execution
        m = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .*?Entered (\w+)/stage1 @ ([\d\.]+)', line)
        if m:
            sym = m.group(2)
            trades[sym]['symbol'] = sym
            trades[sym]['entry_time'] = m.group(1)
            trades[sym]['entry_price'] = float(m.group(3))
            
        # Peak Tracking from EXIT_MATRIX or Guardian
        m = re.search(r'peak=([\+\-\d\.]+)%', line)
        if m:
            # try to find symbol
            sym_m = re.search(r'\[(?:ExitMatrix|ExitGuardian)\]\s+.*?(\w+)', line)
            if not sym_m:
                sym_m = re.search(r'(\w+)/stage1', line)
            if sym_m:
                sym = sym_m.group(1)
                # Ensure we have the symbol in trades to avoid creating empty ones
                if sym in trades:
                    peak_val = float(m.group(1))
                    trades[sym]['peak'] = max(trades[sym]['peak'], peak_val)
                    
        # Exits from DB update (this usually gives final PnL)
        m = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) .*?Closed position.*?trade_id=(\d+) symbol=(\w+) .*?pnl_pct=([\+\-\d\.]+).*?reason=(.*)', line)
        if m:
            sym = m.group(3)
            trades[sym]['exit_time'] = m.group(1)
            trades[sym]['exit_pnl'] = float(m.group(4)) * 100
            trades[sym]['exit_reason'] = m.group(5)

print(f"| {'Symbol':<10} | {'Entry Time':<19} | {'Scores':<70} | {'Kelly':<6} | {'Spread':<7} | {'Peak%':<8} | {'Exit PnL%':<10} | {'Exit Reason':<40} |")
print("-" * 185)
for sym, t in trades.items():
    if t['entry_time']:  # only show actually entered trades
        print(f"| {sym:<10} | {t['entry_time']:<19} | {t['scores']:<70} | {t['kelly']:<6.2f} | {t['spread']:>+6.1f}% | {t['peak']:>+7.1f}% | {t['exit_pnl']:>+9.1f}% | {t['exit_reason']:<40} |")

