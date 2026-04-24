import json
import subprocess
import urllib.parse

symbols = ['MAGACOCK', 'PIGEON', 'xtok', 'Crashout', 'IRVING', 'XRP', 'chudhouse']

for sym in symbols:
    url = f"https://api.dexscreener.com/latest/dex/search?q={urllib.parse.quote(sym)}"
    result = subprocess.run(['curl', '-s', '-A', 'Mozilla/5.0', url], capture_output=True, text=True)
    try:
        data = json.loads(result.stdout)
        pairs = data.get('pairs', [])
        sol_pairs = [p for p in pairs if p.get('chainId') == 'solana']
        if not sol_pairs:
            print(f"[{sym}] No solana pairs")
            continue
            
        top_pair = sol_pairs[0]
        price = float(top_pair.get('priceUsd', 0))
        h24 = top_pair.get('priceChange', {}).get('h24', 0)
        h6 = top_pair.get('priceChange', {}).get('h6', 0)
        fdv = top_pair.get('fdv', 0)
        print(f"[{sym}] Price: ${price} | 6h: {h6}% | 24h: {h24}% | FDV: ${fdv:,.0f} | URL: {top_pair.get('url')}")
    except Exception as e:
        print(f"Error parsing {sym}")

