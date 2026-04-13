import re

desc = """📈New ATH $ALONSHOUSE is up **7.72X** 📈
`BvHneYfnCYjfZE52iUhK4bqkcqJjYjMQUJb5FPhCpump`
🏦 MarketCap  $33.37K —> $257.77K
♨ Narrative：No data available.
"""

def parse_ath_stats(description):
    if not description: return {}
    stats = {}
    
    # Parse multiplier
    m_match = re.search(r'(?:up|is up)\s+\*?\*?([0-9\.]+)[xX]\*?\*?', description, re.IGNORECASE)
    if m_match:
        try:
            stats['ath_multiplier'] = float(m_match.group(1))
        except: pass
        
    # Parse initial MC
    mc_match = re.search(r'MarketCap\s+\$?([0-9\.]+)([KMkm]?)\s*[—\-]>', description)
    if mc_match:
        try:
            val = float(mc_match.group(1))
            unit = mc_match.group(2).upper()
            if unit == 'K': val *= 1000
            elif unit == 'M': val *= 1000000
            stats['initial_mc'] = val
        except: pass

    return dict(stats)

print(parse_ath_stats(desc))
