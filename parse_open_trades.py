import urllib.request
url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=25000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

for sym in ['Nietzsche', 'artsteroid']:
    print(f"\n--- {sym} Exits ---")
    for line in lines:
        if '[EXIT_MATRIX]' in line and sym in line and ('exit' in line or 'sell' in line or 'stop_loss' in line):
            print(line)
