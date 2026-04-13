import sqlite3
import pandas as pd
from datetime import datetime

# Connect to databases
conn_signals = sqlite3.connect('server_sentiment_arb.db')
conn_klines = sqlite3.connect('server_kline.db')

# 1. Load Signals
signals_df = pd.read_sql("""
    SELECT token_ca, symbol, signal_type, timestamp, created_at
    FROM premium_signals
    ORDER BY timestamp ASC
""", conn_signals)

print(f"Total signals: {len(signals_df)}")

# Process Super Index from raw_message? Or does it exist in the db?
signals_df2 = pd.read_sql("""
    SELECT id, token_ca, symbol, signal_type, timestamp, raw_message, is_ath, gate_result
    FROM premium_signals
    ORDER BY timestamp ASC
""", conn_signals)

# Extract Super Index via regex
import re
def extract_super_index(msg):
    if not msg: return None
    m = re.search(r'Super Index[^\d]*(\d+)', msg)
    if m: return int(m.group(1))
    return None

signals_df2['super_index'] = signals_df2['raw_message'].apply(extract_super_index)
print("Signals by type:")
print(signals_df2['signal_type'].value_counts())

# Group by token to find ATH occurrences
token_groups = signals_df2[signals_df2['signal_type'].isin(['ATH', 'NEW_TRENDING'])].groupby('token_ca')

tokens_with_multiple_aths = []

for token, group in token_groups:
    ath_signals = group[group['signal_type'] == 'ATH'].sort_values('timestamp')
    if len(ath_signals) >= 2:
        # Get the timestamp of the 2nd ATH
        second_ath_ts = ath_signals.iloc[1]['timestamp']
        tokens_with_multiple_aths.append({
            'token_ca': token,
            'symbol': ath_signals.iloc[0]['symbol'],
            'second_ath_ts': second_ath_ts,
            'ath_count': len(ath_signals)
        })

print(f"\nTokens with >= 2 ATH signals: {len(tokens_with_multiple_aths)}")

multiple_aths_df = pd.DataFrame(tokens_with_multiple_aths)

# 2. Check subsequent price performance
# For each token with >= 2 ATHs, check klines 1, 4, 12, 24 hours after the second ATH
results = []
for idx, row in multiple_aths_df.iterrows():
    ca = row['token_ca']
    ts = row['second_ath_ts']
    
    # Get price at the time of 2nd ATH (approx, within 5 min)
    klines = pd.read_sql(f"""
        SELECT timestamp, open, high, low, close 
        FROM kline_1m
        WHERE token_ca = '{ca}' 
        ORDER BY timestamp ASC
    """, conn_klines)
    
    if len(klines) == 0:
        continue
        
    klines['timestamp'] = pd.to_numeric(klines['timestamp'])
    
    # Convert signal timestamp from milliseconds to seconds
    ts_sec = ts / 1000.0
    
    # Base price window: [ts_sec, ts_sec + 5*60]
    entry_candidates = klines[(klines['timestamp'] >= ts_sec) & (klines['timestamp'] <= ts_sec + 5*60)]
    if len(entry_candidates) == 0:
        # fallback to last close before ts_sec
        entry_candidates = klines[klines['timestamp'] <= ts_sec]
        if len(entry_candidates) == 0: continue
        entry_price = entry_candidates.iloc[-1]['close']
    else:
        entry_price = entry_candidates.iloc[0]['close']
        
    # Check max high in the next 1h, 4h, 24h
    next_1h = klines[(klines['timestamp'] > ts_sec) & (klines['timestamp'] <= ts_sec + 3600)]
    next_4h = klines[(klines['timestamp'] > ts_sec) & (klines['timestamp'] <= ts_sec + 4*3600)]
    next_24h = klines[(klines['timestamp'] > ts_sec) & (klines['timestamp'] <= ts_sec + 24*3600)]
    
    max_1h = next_1h['high'].max() if len(next_1h) > 0 else entry_price
    max_4h = next_4h['high'].max() if len(next_4h) > 0 else entry_price
    max_24h = next_24h['high'].max() if len(next_24h) > 0 else entry_price
    
    results.append({
        'symbol': row['symbol'],
        'token_ca': ca,
        'entry_price': entry_price,
        'peak_1h_pct': (max_1h - entry_price) / entry_price * 100,
        'peak_4h_pct': (max_4h - entry_price) / entry_price * 100,
        'peak_24h_pct': (max_24h - entry_price) / entry_price * 100,
        'super_index_first_ath': signals_df2[(signals_df2['token_ca'] == ca) & (signals_df2['signal_type'] == 'ATH')].iloc[0]['super_index'],
        'super_index_second_ath': signals_df2[(signals_df2['token_ca'] == ca) & (signals_df2['signal_type'] == 'ATH')].iloc[1]['super_index']
    })

results_df = pd.DataFrame(results)

if len(results_df) > 0:
    print("\nPerformance after 2nd ATH (Peak %):")
    print(results_df[['symbol', 'peak_1h_pct', 'peak_4h_pct', 'peak_24h_pct']].describe())
    
    win_1h = len(results_df[results_df['peak_1h_pct'] > 10]) / len(results_df) * 100
    win_4h = len(results_df[results_df['peak_4h_pct'] > 20]) / len(results_df) * 100
    
    print(f"\nWin Rate (>10% peak in 1h): {win_1h:.1f}%")
    print(f"Win Rate (>20% peak in 4h): {win_4h:.1f}%")
    
    # Correlate Super Index with Peak
    print("\nCorrelation with Second ATH Super Index:")
    print(results_df[['super_index_second_ath', 'peak_1h_pct', 'peak_4h_pct']].corr())
else:
    print("\nNo kline data found for >=2 ATH tokens.")

# Look at ALL NEW_TRENDING signals and see overall win rate
new_trending = signals_df2[signals_df2['signal_type'] == 'NEW_TRENDING']
print(f"\nTotal NEW_TRENDING signals: {len(new_trending)}")
