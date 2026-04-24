import re
import urllib.request

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=25000"

trades = []
rejected = {}

req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')
    
    for line in lines:
        if '🟢 BOUGHT' in line or '🔥 FIRE' in line:
            trades.append(line.strip())
            
        if 'BLOCKED' in line or 'block=' in line or 'GATE FAIL' in line or 'TREND GATE FAIL' in line:
            m = re.search(r'\$([A-Za-z0-9]+)', line)
            if m:
                symbol = m.group(1)
                # Ignore M2M Wait blocks as they might pass later, unless it's a hard block
                if 'M2M' in line: continue
                if symbol not in rejected:
                    rejected[symbol] = []
                rejected[symbol].append(line.strip())

print(f"--- TRADES ({len(trades)}) ---")
for t in trades:
    print(t)

print(f"\n--- BLOCKED SYMBOLS ({len(rejected)}) ---")
for sym in rejected:
    print(f"{sym}: {rejected[sym][0]}")

