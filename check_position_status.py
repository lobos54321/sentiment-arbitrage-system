import urllib.request
import re

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=40000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

# Check if open positions are still being monitored or if system crashed
print("=== LAST 50 LINES OF LOG ===")
for line in lines[-50:]:
    if line.strip():
        print(line)

