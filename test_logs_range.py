import urllib.request
import re

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=25000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

trades = 0
first_ts = None
last_ts = None

ts_pattern = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')

for line in lines:
    m = ts_pattern.match(line)
    if m:
        if not first_ts:
            first_ts = m.group(1)
        last_ts = m.group(1)

    if '🟢 BOUGHT' in line or '🔥 FIRE' in line or '[ENTRY_ENGINE] Executing paper trade' in line or 'bought ' in line.lower() or 'fire' in line.lower():
        trades += 1
        print("Trade found:", line)

print(f"Log Range: {first_ts} to {last_ts}")
print(f"Total potential trade lines: {trades}")
