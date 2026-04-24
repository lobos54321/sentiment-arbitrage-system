import re
import json

log_file = '/Users/boliu/.gemini/antigravity/brain/649d38c4-18ca-49da-a4fe-5f67fefc0772/.system_generated/steps/7534/content.md'

trades = []
rejected = {} # ca -> {symbol, reason, sig_price, timestamp}
registered = {} # symbol -> {ca, price}  (best effort mapping)

# Try to extract CA from [PREBUY_FILTER] or Matrix if possible, but logs might only have symbol.
# Let's see what we can grep.

with open(log_file, 'r') as f:
    for line in f:
        # Extract timestamp
        ts_match = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
        if not ts_match:
            continue
        ts = ts_match.group(1)
        
        # Trades
        if '🟢 BOUGHT' in line or '🔥 FIRE' in line:
            trades.append(line.strip())
            
        # Registrations to map symbol to CA if possible (Wait, log might not have CA)
        # Let's inspect block logs
        if 'BLOCKED' in line or 'block=' in line or 'GATE FAIL' in line or 'TREND GATE FAIL' in line or 'skipping' in line:
            # e.g., [Matrix] $M2M eval: T=50 V=0 P=30 S=100 ready=False block=volume=0 type=ATH
            m = re.search(r'\$([A-Za-z0-9]+)', line)
            if m:
                symbol = m.group(1)
                if symbol not in rejected:
                    rejected[symbol] = []
                rejected[symbol].append((ts, line.strip()))

print(f"Total Trades Found: {len(trades)}")
for t in trades:
    print(t)

print(f"\nUnique Rejected Symbols: {len(rejected)}")
# print a sample of rejected
for sym in list(rejected.keys())[:10]:
    print(f"{sym}: {rejected[sym][0][1]}")

