import sys

with open('scripts/entry_engine.py', 'r') as f:
    lines = f.readlines()

new_lines = []
for line in lines:
    if line.startswith("def evaluate_smart_entry"):
        break
    new_lines.append(line)

with open('evaluate_entry_temp.py', 'r') as f:
    temp_func = f.read()

with open('scripts/entry_engine.py', 'w') as f:
    f.writelines(new_lines)
    f.write(temp_func)

print("Fixed entry_engine.py")
