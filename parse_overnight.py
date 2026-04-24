import urllib.request
import re

url = "https://sentiment-arbitrage.zeabur.app/api/logs/paper-trader?token=mytoken54321&lines=40000"
req = urllib.request.Request(url)
with urllib.request.urlopen(req) as response:
    lines = response.read().decode('utf-8').split('\n')

# Find log range
timestamps = []
for line in lines:
    m = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
    if m:
        timestamps.append(m.group(1))

print(f"Log range: {timestamps[0]} → {timestamps[-1]}")
print(f"Total log lines: {len(lines)}")

# Extract trades
trades = []
entry_map = {}  # symbol -> entry info

for i, line in enumerate(lines):
    if '[ENTRY_PRICE]' in line:
        m = re.search(r'\[ENTRY_PRICE\] (\S+) entry_price=([0-9.e+-]+) \((.+?)\)', line)
        ts = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
        if m and ts:
            sym = m.group(1)
            ep = float(m.group(2))
            fill_info = m.group(3)
            trade = {
                'symbol': sym,
                'entry_price': ep,
                'entry_time': ts.group(1),
                'fill_info': fill_info,
                'exit_pnl': None,
                'exit_time': None,
                'exit_reason': None,
                'peak_pnl': None,
            }
            trades.append(trade)
            entry_map[sym] = len(trades) - 1

    if '[EXIT_MATRIX]' in line and ('action=exit' in line or 'action=stop_loss' in line or 'action=sell' in line):
        m_sym = re.search(r'\[EXIT_MATRIX\] (\S+)/stage', line)
        m_pnl = re.search(r'pnl=([+-]?[0-9.]+)%', line)
        m_reason = re.search(r'reason=(\S+)', line)
        ts = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
        if m_sym and m_pnl and ts:
            sym = m_sym.group(1)
            pnl = float(m_pnl.group(1))
            reason = m_reason.group(1) if m_reason else ''
            
            # Match to latest open trade of this symbol
            for t in reversed(trades):
                if t['symbol'] == sym and t['exit_pnl'] is None:
                    t['exit_pnl'] = pnl
                    t['exit_time'] = ts.group(1)
                    t['exit_reason'] = reason
                    break
    
    # Capture peak pnl from EXIT_MATRIX hold lines with 'peak='
    if '[EXIT_MATRIX]' in line and 'peak=' in line:
        m_sym = re.search(r'\[EXIT_MATRIX\] (\S+)/stage', line)
        m_peak = re.search(r'peak=([0-9.]+)%', line)
        if m_sym and m_peak:
            sym = m_sym.group(1)
            peak = float(m_peak.group(1))
            for t in reversed(trades):
                if t['symbol'] == sym and t['exit_pnl'] is None:
                    if t['peak_pnl'] is None or peak > t['peak_pnl']:
                        t['peak_pnl'] = peak
                    break

print(f"\n{'='*70}")
print(f"{'#':<4} {'Symbol':<15} {'Entry Time':<20} {'Entry Price':<20} {'Exit PnL':<10} {'Peak':<8} {'Reason'}")
print(f"{'='*70}")
total_pnl = 0
wins = 0
losses = 0
open_trades = 0
for i, t in enumerate(trades):
    status = f"{t['exit_pnl']:+.1f}%" if t['exit_pnl'] is not None else "OPEN"
    peak = f"{t['peak_pnl']:+.1f}%" if t['peak_pnl'] else "-"
    reason = t['exit_reason'] or ("-" if t['exit_pnl'] is None else "?")
    print(f"{i+1:<4} {t['symbol']:<15} {t['entry_time']:<20} {t['entry_price']:<20.12f} {status:<10} {peak:<8} {reason}")
    if t['exit_pnl'] is not None:
        total_pnl += t['exit_pnl']
        if t['exit_pnl'] > 0:
            wins += 1
        else:
            losses += 1
    else:
        open_trades += 1

closed = wins + losses
print(f"\n{'='*70}")
print(f"Total trades: {len(trades)} (Closed: {closed}, Open: {open_trades})")
if closed > 0:
    print(f"Win rate: {wins}/{closed} = {wins/closed*100:.1f}%")
    print(f"Total PnL (closed): {total_pnl:+.1f}%")
    print(f"Avg PnL per closed trade: {total_pnl/closed:+.1f}%")
