import sys
import re

log_file = "/Users/boliu/.gemini/antigravity/brain/649d38c4-18ca-49da-a4fe-5f67fefc0772/.system_generated/steps/14348/content.md"

entries = []
exits = []

with open(log_file, "r") as f:
    for line in f:
        if "Entered" in line and "via quoted execution" in line:
            entries.append(line.strip())
        elif "Exit executed" in line or "[PNL_DEBUG] Processing exit" in line or "trade_history completed" in line:
            exits.append(line.strip())

print("=== ENTRIES ===")
for e in entries: print(e)
print("\n=== EXITS ===")
for e in exits: print(e)
