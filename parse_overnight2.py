import urllib.request
import re

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=40000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

# The log only covers 13:10 to 22:25 — check if 40000 lines goes back far enough
# Let's check all ENTRY_PRICE and EXIT lines more fully

print("=== ALL ENTRY_PRICE OCCURRENCES ===")
for line in lines:
    if '[ENTRY_PRICE]' in line:
        print(line)

print("\n=== ALL EXIT TRIGGERED OCCURRENCES ===")
for line in lines:
    if 'EXIT triggered' in line or ('EXIT_MATRIX' in line and 'action=exit' in line) or \
       ('EXIT_MATRIX' in line and 'action=stop_loss' in line) or \
       ('EXIT_MATRIX' in line and '🔔' in line):
        print(line)

print("\n=== OPEN POSITION STATUS (last seen) ===")
# Find last exit_matrix lines for open trades
open_syms = ['artsteroid', 'UNCEROID']
for sym in open_syms:
    last_exit_line = None
    for line in lines:
        if f'[EXIT_MATRIX] {sym}' in line:
            last_exit_line = line
    if last_exit_line:
        print(f"{sym}: {last_exit_line}")
