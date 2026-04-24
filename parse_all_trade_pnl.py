import urllib.request

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=25000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

for i, line in enumerate(lines):
    if '[ENTRY_PRICE]' in line:
        print(f"\n🟢 {line}")
    if '[EXIT_MATRIX]' in line and ('action=sell' in line or 'action=exit' in line or 'action=stop_loss' in line or 'timeout' in line):
        print(f"🔴 {line}")
