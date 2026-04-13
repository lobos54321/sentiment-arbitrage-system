import sqlite3
import pandas as pd

# Connect to databases
conn_signals = sqlite3.connect('server_sentiment_arb.db')
conn_paper = sqlite3.connect('server_paper.db')

# Load premium_signals
signals_df = pd.read_sql("""
    SELECT id, token_ca, symbol, signal_type, timestamp, is_ath
    FROM premium_signals
    WHERE signal_type IN ('ATH', 'NEW_TRENDING')
    ORDER BY timestamp ASC
""", conn_signals)

# Find tokens with multiple ATHs
tokens_with_aths = signals_df[signals_df['signal_type'] == 'ATH'].copy()
tokens_with_aths['ath_rank'] = tokens_with_aths.groupby('token_ca')['timestamp'].rank(method='first').astype(int)

# Load paper_trades
paper_df = pd.read_sql("""
    SELECT premium_signal_id, token_ca, symbol, signal_type, signal_ts,
           entry_price, exit_price, pnl_pct, peak_pnl, strategy_outcome, exit_reason
    FROM paper_trades
""", conn_paper)

# Merge paper trades with ATH rank
paper_aths = pd.merge(paper_df, tokens_with_aths[['id', 'ath_rank']], left_on='premium_signal_id', right_on='id', how='left')

# Analysis 1: Performance of 2nd ATH
second_aths = paper_aths[paper_aths['ath_rank'] == 2].copy()
if len(second_aths) > 0:
    print(f"Total trades on 2nd ATH: {len(second_aths)}")
    
    # Rising proportion: peak_pnl > 10%
    rising_10 = len(second_aths[second_aths['peak_pnl'] > 10]) / len(second_aths) * 100
    rising_20 = len(second_aths[second_aths['peak_pnl'] > 20]) / len(second_aths) * 100
    profitable_close = len(second_aths[second_aths['pnl_pct'] > 0]) / len(second_aths) * 100
    
    print(f"Percentage continuing to rise >10%: {rising_10:.1f}%")
    print(f"Percentage continuing to rise >20%: {rising_20:.1f}%")
    print(f"Percentage of profitable exits: {profitable_close:.1f}%")
    print("\nDetailed stats of 2nd ATH Peak PnL:")
    print(second_aths['peak_pnl'].describe())
else:
    print("No trades found for 2nd ATH.")

# Analysis 2: Compare with 1st ATH and NEW_TRENDING
print("\n--- Compare by Signal Type / ATH Rank ---")
def print_stats(name, df_subset):
    if len(df_subset) == 0:
        return
    rising_20 = len(df_subset[df_subset['peak_pnl'] > 20]) / len(df_subset) * 100
    prof = len(df_subset[df_subset['pnl_pct'] > 0]) / len(df_subset) * 100
    print(f"{name} ({len(df_subset)} trades):")
    print(f"  >20% Peak: {rising_20:.1f}%")
    print(f"  Win % (Exit > 0): {prof:.1f}%")
    print(f"  Avg Peak PnL: {df_subset['peak_pnl'].mean():.1f}%")
    print(f"  Avg Exit PnL: {df_subset['pnl_pct'].mean():.1f}%")

print_stats("1st ATH", paper_aths[paper_aths['ath_rank'] == 1])
print_stats("2nd ATH", paper_aths[paper_aths['ath_rank'] == 2])
print_stats("3rd+ ATH", paper_aths[paper_aths['ath_rank'] >= 3])
print_stats("NEW_TRENDING", paper_aths[paper_aths['signal_type'] == 'NEW_TRENDING'])
