import re
import urllib.request
import json
import time
from urllib.error import HTTPError

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=25000"

req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

tokens = {} # symbol -> {ca, sig_price, type, max_price}
trades = 0
for line in lines:
    if '🟢 BOUGHT' in line or '🔥 FIRE' in line:
        trades += 1
        continue
    
    # 2026-04-18 08:39:13 [INFO]   [PREBUY_FILTER] SolanaLife (4473KzXF...) PASS age=0min ATH=False
    # 2026-04-18 08:39:15 [INFO]   [WATCHLIST] Registering SolanaLife (NOT_ATH) Super=None Price=0.005118744577889369
    
    if '[WATCHLIST] Registering' in line:
        # Registering SolanaLife (NOT_ATH) Super=None Price=0.005118744577889369
        m_sym = re.search(r'Registering ([A-Za-z0-9_-]+) \(([^)]+)\)', line)
        m_px = re.search(r'Price=([0-9.]+)', line)
        if m_sym and m_px:
            sym = m_sym.group(1)
            typ = m_sym.group(2)
            px = float(m_px.group(1))
            if sym not in tokens:
                tokens[sym] = {'sym': sym, 'type': typ, 'sig_price': px, 'ca': None}
            else:
                tokens[sym]['sig_price'] = px

    if '[PREBUY_FILTER]' in line and 'PASS' in line:
        # [PREBUY_FILTER] SolanaLife (4473KzXF...) PASS
        # Warning: log might truncate CA like (4473KzXF...) or maybe the full CA? Let's check token_ca mapping if another line has it.
        # Are there any other logs with CA?
        pass

    # matrix logs
    # 2026-04-18 08:11:12 [INFO] [Matrix] $Picante eval: T=50 V=0 P=40 S=50 ready=False
    
# Let's extract CAs by finding token_ca if they exist. Or just look for any length-43/44 base58 string.
# Some logs like: "Checking cooldowns for 4473KzXF..."
# If not, it means Zeabur logs might not output the full CA. 

for line in lines:
    # try to catch base58 addresses that might be CAs
    # "ca=ABCDEF..." or "token_ca=ABCDEF..."
    match = re.search(r'\b([1-9A-HJ-NP-Za-km-z]{32,44})\b', line)
    if match:
         pass # difficult to map without surrounding text
         
print(f"TRADES: {trades}")
for t in tokens:
    print(tokens[t])
