import urllib.request
import re

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=40000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

# Look for ALL EXIT_MATRIX lines for artsteroid and UNCEROID -- including holds
for sym in ['artsteroid', 'UNCEROID']:
    all_exit_lines = [(i,l) for i,l in enumerate(lines) if f'[EXIT_MATRIX] {sym}' in l or f'[EXIT_MATRIX] 🔔 {sym}' in l]
    print(f"\n=== {sym} EXIT_MATRIX history ({len(all_exit_lines)} lines) ===")
    for i,l in all_exit_lines:
        print(l)

# Look for errors around or after last known exit logs
print("\n\n=== ERRORS/CRASHES around those trade times ===")
for line in lines:
    if 'ERROR' in line or 'CRITICAL' in line or 'Traceback' in line or 'Exception' in line:
        print(line)

