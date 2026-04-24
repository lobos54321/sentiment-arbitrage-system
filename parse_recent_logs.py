import sys
import re
from datetime import datetime

log_file = "/Users/boliu/.gemini/antigravity/brain/649d38c4-18ca-49da-a4fe-5f67fefc0772/.system_generated/steps/13257/content.md"

entries = []
exits = []

with open(log_file, 'r') as f:
    lines = f.readlines()

for line in lines:
    line = line.strip()
    if not line:
        continue
    
    if "[SmartEntry] 🚀" in line and "GOOD_ENTRY" in line:
        entries.append(line)
    elif "[SmartEntry] 🚀" in line and "MOMENTUM_ENTRY" in line:
        entries.append(line)
    elif "Entered " in line and "/stage1 @" in line:
        entries.append(line)
    elif "CLOSED " in line:
        exits.append(line)
    elif "PARTIAL " in line:
        exits.append(line)

print(f"Total Entries found: {len(entries)}")
print(f"Total Exits found: {len(exits)}")
print("\n--- Recent Entries ---")
for e in entries[-10:]:
    print(e)

print("\n--- Recent Exits ---")
for x in exits[-10:]:
    print(x)
