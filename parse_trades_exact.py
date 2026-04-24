import urllib.request
import re
import json

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=25000"
try:
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as response:
        content = response.read().decode('utf-8')
        lines = content.split('\n')
        
        trades = []
        for line in lines:
            # Let's search broadly for ANY indicator of a successful trade
            if ('BOUGHT' in line 
                or 'bought' in line.lower() 
                or 'entry_price' in line 
                or 'Executing paper trade' in line
                or 'FastLane' in line
                or 'inserted into paper_trades' in line.lower()
                or '🟢' in line):
                trades.append(line)
                
        print(f"Broad trade matches found: {len(trades)}")
        for i, t in enumerate(trades[:20]):
            print(f"{i}: {t}")
except Exception as e:
    print("Error:", e)
