import urllib.request
import re

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=25000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    content = response.read().decode('utf-8')
    lines = content.split('\n')

target_symbols = ['Picante', 'Nietzsche', 'artsteroid']
print("=== EXIT LOGS FOR SUCCESSFUL TRADES ===")
for line in lines:
    if '[EXIT_MATRIX]' in line and any(sym in line for sym in target_symbols):
        if 'action=sell' in line or 'action=close' in line or 'action=stop_loss' in line or 'pnl=' in line:
            print(line)
            
print("\n=== WAIT TIMES FOR MISSED DOGS ===")
# Crashout and chudhouse
for line in lines:
    if '[WATCHLIST]' in line and 'Registering Crashout' in line:
        print("[Crashout] " + line)
    if 'FIRE Crashout' in line:
         print("[Crashout] " + line)
         
    if '[WATCHLIST]' in line and 'Registering chudhouse' in line:
        print("[chudhouse] " + line)
    if 'FIRE chudhouse' in line:
         print("[chudhouse] " + line)

