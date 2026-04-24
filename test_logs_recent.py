import urllib.request
import json

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=1000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    data = json.loads(response.read().decode())
    
logs = data.get('logs', [])
print(f"Total lines: {len(logs)}")

entries = []
exits = []

for line in logs:
    if "[SmartEntry] 🚀" in line:
        entries.append(line)
    elif "Entered " in line and "/stage1 @" in line:
        entries.append(line)
    elif "CLOSED " in line:
        exits.append(line)
    elif "PARTIAL " in line:
        exits.append(line)

print("\n--- Recent Entries ---")
for e in entries[-10:]:
    print(e.strip())

print("\n--- Recent Exits ---")
for x in exits[-10:]:
    print(x.strip())

