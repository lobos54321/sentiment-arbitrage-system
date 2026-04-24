import urllib.request

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=25000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

for i, line in enumerate(lines):
    if '[ENTRY_PRICE]' in line:
        print(f"\n--- Trade Found: {line} ---")
        # Print a few lines before and after for context
        for j in range(max(0, i-5), min(len(lines), i+10)):
            if '[WATCHLIST]' in lines[j] or 'FIRE' in lines[j] or 'SmartEntry' in lines[j] or 'Matrix' in lines[j] or 'BOUGHT' in lines[j] or 'EXIT' in lines[j]:
                print(lines[j])
