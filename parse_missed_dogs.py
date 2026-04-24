import re
from collections import defaultdict

log_file = "/Users/boliu/.gemini/antigravity/brain/649d38c4-18ca-49da-a4fe-5f67fefc0772/.system_generated/steps/13324/content.md"

signals = {} # symbol -> {signal_price, max_price, entered}
current_prices = defaultdict(float)

with open(log_file, "r") as f:
    for line in f:
        # Registering {symbol} ({signal_type}) Super={super_idx} Price={sig_price}
        m_reg = re.search(r'Registering\s+(\S+)\s+\(.*?\)\s+Super=.*?\s+Price=([0-9\.eE+-]+)', line)
        if m_reg:
            symbol = m_reg.group(1)
            price = float(m_reg.group(2))
            if symbol not in signals:
                signals[symbol] = {'signal_price': price, 'max_price': price, 'entered': False}
        
        # Entered {symbol}/stage1 @ {price}
        m_ent = re.search(r'Entered\s+(\S+)/stage1\s+@', line)
        if m_ent:
            symbol = m_ent.group(1)
            if symbol in signals:
                signals[symbol]['entered'] = True
                
        # Look for any price mentions to track max price
        # [Matrix] $symbol eval: T=... P=... current_price=... ?
        # Or look for snaps=[0.0000001, 0.0000002...]
        m_snap = re.search(r'snaps=\[([\d\.\,\s]+)\]', line)
        if m_snap:
            # but we need the symbol
            m_sym = re.search(r'\$(\S+)\s+(momentum FAIL|MOMENTUM_ENTRY|pre-momentum)', line)
            if m_sym:
                symbol = m_sym.group(1)
                if symbol in signals:
                    prices = [float(x.strip()) for x in m_snap.group(1).split(',')]
                    signals[symbol]['max_price'] = max(signals[symbol]['max_price'], max(prices))
                    
        # Another source: momentum check failed: declining ... [prices]
        m_fail = re.search(r'\$(\S+)\s+momentum FAIL:.*\[([\d\.\,\s]+)\]', line)
        if m_fail:
            symbol = m_fail.group(1)
            if symbol in signals:
                prices = [float(x.strip()) for x in m_fail.group(2).split(',')]
                signals[symbol]['max_price'] = max(signals[symbol]['max_price'], max(prices))

missed_dogs = []
for sym, data in signals.items():
    if not data['entered'] and data['signal_price'] > 0:
        multiplier = data['max_price'] / data['signal_price']
        if multiplier > 1.5:
            missed_dogs.append((sym, multiplier, data['signal_price'], data['max_price']))

missed_dogs.sort(key=lambda x: x[1], reverse=True)
print(f"Total signals: {len(signals)}")
print("Missed Dogs (Tokens that went up >1.5x but we didn't enter):")
for sym, mult, sig_p, max_p in missed_dogs[:20]:
    print(f"{sym}: {mult:.2f}x (Signal: {sig_p:.10f} -> Max observed: {max_p:.10f})")

