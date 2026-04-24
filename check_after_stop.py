import urllib.request
import re

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=40000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

# After UNCEROID last exit log (15:09), what happened? Look for next 50 meaningful lines
unceroid_last_idx = 0
for i, line in enumerate(lines):
    if 'UNCEROID' in line and 'EXIT_MATRIX' in line:
        unceroid_last_idx = i

print(f"UNCEROID last exit_matrix at line {unceroid_last_idx}")
print("\n=== Lines AFTER last UNCEROID exit (next 30 meaningful lines) ===")
count = 0
for line in lines[unceroid_last_idx+1:]:
    if line.strip():
        print(line)
        count += 1
        if count >= 30:
            break

# Also check for any 'position' or 'paper_trades' database writes
print("\n\n=== Any 'paper_trade' DB insert confirmations ===")
for line in lines:
    if 'INSERT' in line or 'paper_trades' in line.lower() or 'db_write' in line.lower() or 'saved to db' in line.lower() or 'recorded' in line.lower():
        print(line)

print("\n\n=== Any 'timeout' or 'position closed' events ===")
for line in lines:
    if 'timeout' in line.lower() or 'position closed' in line.lower() or 'closed' in line.lower() and 'UNCEROID' in line:
        print(line)

