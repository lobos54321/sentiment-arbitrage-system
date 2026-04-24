import urllib.request

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=25000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

for line in lines:
    if 'XRP' in line or 'chudhouse' in line or 'Crashout' in line or 'Picante' in line:
        if 'ENTRY' in line or 'BOUGHT' in line or 'FAIL' in line or 'SLIPPAGE' in line or 'SmartEntry' in line or 'GATE' in line or 'FastLane' in line or 'FAILED' in line or 'ERROR' in line:
            print(line)
