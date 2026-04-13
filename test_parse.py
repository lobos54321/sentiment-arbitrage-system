import re
def parse_ath_multiplier(description):
    if not description: return None
    # match "**13.59X**" or "**13.5x**" or "up 13.5X"
    match = re.search(r'(?:up|is up)\s+\*?\*?([0-9\.]+)[xX]\*?\*?', description, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except:
            return None
    return None

descs = [
    "📈New ATH $COG is up **13.59X** 📈",
    "📈New ATH $WLFI is up **5.41X** 📈",
    "📈New ATH $Trump is up 2.57x 📈"
]
for d in descs:
    print(parse_ath_multiplier(d))
