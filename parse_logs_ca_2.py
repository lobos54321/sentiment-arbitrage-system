import re
import urllib.request

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=25000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

for i, line in enumerate(lines):
    if '[WATCHLIST] Registering' in line:
        # Check previous 20 lines for CA
        symbol = re.search(r'Registering ([A-Za-z0-9_-]+)', line)
        if symbol:
            sym = symbol.group(1)
            for j in range(max(0, i-20), i):
                if 'PREBUY_FILTER' in lines[j] and sym in lines[j]:
                    print(f"Found nearby log: {lines[j]}")
                    break
