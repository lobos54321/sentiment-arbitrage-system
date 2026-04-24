import re
from collections import defaultdict

log_file = "/Users/boliu/.gemini/antigravity/brain/649d38c4-18ca-49da-a4fe-5f67fefc0772/.system_generated/steps/13257/content.md"

rejected_tokens = {}
token_prices = defaultdict(list)

with open(log_file, 'r') as f:
    for line in f:
        # Find prices
        # e.g., current=0.0000012740, or trigger=1.307e-06, or price=...
        # Look for $TOKEN
        m_token = re.search(r'\$([a-zA-Z0-9_]+)', line)
        if not m_token:
            # try to find "TOKEN REJECT"
            m_reject = re.search(r'([a-zA-Z0-9_]+) REJECT', line)
            if m_reject:
                token = m_reject.group(1)
                rejected_tokens[token] = True
        else:
            token = m_token.group(1)
        
        if "REJECT" in line or "BLOCK" in line:
            # find token name near $
            m = re.search(r'\$([a-zA-Z0-9_]+)', line)
            if m:
                rejected_tokens[m.group(1)] = True
                
        # find prices
        # simplest way is to look for rises=[...] or declining ... or current=...
        # let's just grab numbers that look like prices 0.0000...
        prices = re.findall(r'0\.00000[0-9]+', line)
        if prices and m_token:
            token = m_token.group(1)
            for p in prices:
                token_prices[token].append(float(p))

for t in rejected_tokens:
    if t in token_prices and len(token_prices[t]) > 0:
        prices = token_prices[t]
        first_price = prices[0]
        max_price = max(prices)
        min_price = min(prices)
        last_price = prices[-1]
        
        max_gain = (max_price - first_price) / first_price * 100
        max_drop = (min_price - first_price) / first_price * 100
        print(f"Token: {t}")
        print(f"  First seen price: {first_price:.10f}")
        print(f"  Max price after:  {max_price:.10f} ({max_gain:+.1f}%)")
        print(f"  Min price after:  {min_price:.10f} ({max_drop:+.1f}%)")
        print(f"  Last seen price:  {last_price:.10f}")
