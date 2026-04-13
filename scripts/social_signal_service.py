"""
Social Signal Service
─────────────────────
Standalone HTTP microservice for social propagation signals.
Runs on Zeabur server (Python 3.10+).

Three data sources (all from DexScreener public API, no auth needed):
  1. DexScreener Pair Info: social links (twitter/website/telegram),
     trading activity (txns, volume, buyers) as community engagement proxy
  2. DexScreener Boosts: whether project paid to boost (commitment signal)
  3. DexScreener Token Lock: whether tokens are locked (credibility signal)

paper_trade_monitor.py calls:
  GET http://localhost:8765/social?ca=<address>&symbol=<symbol>

Returns JSON:
  {
    "twitter_mentions": int,        # estimated from DEX trading activity
    "dex_txns_24h": int,            # total 24h transactions
    "dex_buyers_24h": int,          # buy transactions 24h
    "dex_volume_24h": float,        # 24h volume in USD
    "dex_pairs_count": int,         # number of DEX pairs
    "dex_has_boost": bool,          # paid DexScreener boost
    "dex_boost_amount": int,        # boost credits spent
    "dex_has_profile": bool,        # has token profile (paid)
    "has_twitter_link": bool,       # project linked Twitter
    "has_website": bool,            # project has website
    "has_telegram": bool,           # project has Telegram
    "has_token_lock": bool,         # tokens locked (credibility)
    "social_score": int,            # composite 0-100
    "source": str                   # data source info
  }
"""

import json
import os
import time
import logging
import random
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import urllib.request

logging.basicConfig(level=logging.INFO, format='%(asctime)s [SocialSvc] %(message)s')
log = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
PORT = int(os.environ.get('SOCIAL_SERVICE_PORT', '8765'))
CACHE_TTL_SEC = 120       # Cache results for 2 minutes per CA
DEX_BOOST_CACHE_TTL = 60  # DexScreener boost cache
DEX_PAIR_CACHE_TTL = 120  # DexScreener pair info cache (2 min)

# ─── In-memory cache ─────────────────────────────────────────────────────────
_cache = {}  # {key: (result, expire_ts)}

def _cache_get(key):
    entry = _cache.get(key)
    if entry and time.time() < entry[1]:
        return entry[0]
    return None

def _cache_set(key, value, ttl):
    _cache[key] = (value, time.time() + ttl)


_USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
]


# ─── Method 1: DexScreener Pair Info (social links + trading activity) ───────

def _fetch_dex_pair_info(ca):
    """Get token social info AND trading activity from DexScreener pairs API.
    
    Social links show the project has online presence.
    Trading activity (txns, volume, unique buyers) is a strong proxy for
    community engagement — more reliable than Twitter scraping.
    """
    cache_key = f"dex_pair:{ca}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{ca}"
        req = urllib.request.Request(url, headers={'User-Agent': random.choice(_USER_AGENTS)})
        resp = urllib.request.urlopen(req, timeout=8)
        data = json.loads(resp.read())

        pairs = data.get('pairs', [])
        if not pairs:
            result = {
                'has_twitter': False, 'has_website': False, 'has_telegram': False,
                'has_token_lock': False, 'twitter_url': '',
                'txns_24h': 0, 'buys_24h': 0, 'sells_24h': 0,
                'volume_24h': 0, 'pairs_count': 0,
            }
            _cache_set(cache_key, result, DEX_PAIR_CACHE_TTL)
            return result

        # Use first pair (highest liquidity)
        pair = pairs[0]
        info = pair.get('info', {})
        socials = info.get('socials', [])
        websites = info.get('websites', [])

        # Social links
        twitter_url = ''
        has_twitter = False
        has_telegram = False
        has_token_lock = False
        for s in socials:
            stype = (s.get('type') or '').lower()
            if stype == 'twitter':
                has_twitter = True
                twitter_url = s.get('url', '')
            elif stype == 'telegram':
                has_telegram = True

        # Check for token lock in websites
        for w in websites:
            label = (w.get('label') or '').lower()
            url_str = (w.get('url') or '').lower()
            if 'lock' in label or 'lock' in url_str or 'streamflow' in url_str:
                has_token_lock = True

        # Trading activity (aggregate across all pairs)
        total_buys_24h = 0
        total_sells_24h = 0
        total_volume_24h = 0
        for p in pairs:
            txns = p.get('txns', {})
            h24 = txns.get('h24', {})
            total_buys_24h += int(h24.get('buys', 0) or 0)
            total_sells_24h += int(h24.get('sells', 0) or 0)
            vol = p.get('volume', {})
            total_volume_24h += float(vol.get('h24', 0) or 0)

        result = {
            'has_twitter': has_twitter,
            'has_website': bool(websites),
            'has_telegram': has_telegram,
            'has_token_lock': has_token_lock,
            'twitter_url': twitter_url,
            'txns_24h': total_buys_24h + total_sells_24h,
            'buys_24h': total_buys_24h,
            'sells_24h': total_sells_24h,
            'volume_24h': total_volume_24h,
            'pairs_count': len(pairs),
        }
        _cache_set(cache_key, result, DEX_PAIR_CACHE_TTL)
        log.info(
            f"DexPair [{ca[:8]}...]: tw={has_twitter} web={bool(websites)} "
            f"tg={has_telegram} lock={has_token_lock} "
            f"txns_24h={result['txns_24h']} vol_24h=${total_volume_24h:,.0f} "
            f"pairs={len(pairs)}"
        )
        return result

    except Exception as e:
        log.warning(f"DexPair info error: {e}")
        return {
            'has_twitter': False, 'has_website': False, 'has_telegram': False,
            'has_token_lock': False, 'twitter_url': '',
            'txns_24h': 0, 'buys_24h': 0, 'sells_24h': 0,
            'volume_24h': 0, 'pairs_count': 0,
        }


# ─── Method 2: DexScreener Boost ─────────────────────────────────────────────

def _fetch_dex_boost(ca):
    """Check DexScreener for paid boost / token profile.
    
    Boost = project paid DexScreener to promote token.
    This shows: project is willing to spend money, has some credibility.
    """
    cache_key = f"dex_boost:{ca}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        url = f"https://api.dexscreener.com/orders/v1/solana/{ca}"
        req = urllib.request.Request(url, headers={'User-Agent': random.choice(_USER_AGENTS)})
        resp = urllib.request.urlopen(req, timeout=6)
        raw = resp.read()
        data = json.loads(raw)

        if isinstance(data, list):
            orders = data
            boosts = []
        else:
            orders = data.get('orders', []) if isinstance(data, dict) else []
            boosts = data.get('boosts', []) if isinstance(data, dict) else []

        has_profile = any(
            o.get('type') == 'tokenProfile' and o.get('status') == 'approved' 
            for o in orders
        )
        
        has_boost_order = any(
            'boost' in (o.get('type', '') or '').lower() 
            for o in orders
        )
        
        boost_credits = sum(b.get('totalAmount', 0) for b in boosts) if boosts else 0
        has_active_boost = has_boost_order or (boost_credits > 0)

        result = {
            'dex_has_boost': has_active_boost,
            'dex_boost_amount': int(boost_credits),
            'dex_has_profile': has_profile,
        }
        _cache_set(cache_key, result, DEX_BOOST_CACHE_TTL)
        log.info(f"DexBoost [{ca[:8]}...]: profile={has_profile} boost={has_active_boost} amount={boost_credits}")
        return result

    except Exception as e:
        log.warning(f"DexScreener boost error: {e}")
        return {'dex_has_boost': False, 'dex_boost_amount': 0, 'dex_has_profile': False}


# ─── Social Score Computation ─────────────────────────────────────────────────

def _compute_social_score(pair_data, dex_data):
    """Compute composite social score 0-100.
    
    Trading Activity (0-50 points):
      24h transactions as community engagement proxy:
        0-50 txns    → 0   (dead / no interest)
        51-200       → 10  (low activity)
        201-500      → 20  (moderate)
        501-1000     → 30  (active)
        1001-3000    → 40  (hot)
        3000+        → 50  (viral)

    DexScreener (0-25 points):
      has_profile  → +10 (project committed to listing)
      has_boost    → +15 (project spending real money = conviction)

    Social Presence (0-25 points):
      has_twitter   → +8  (project has Twitter account)
      has_website   → +5  (project has website)
      has_telegram  → +4  (community channel)
      has_token_lock→ +8  (tokens locked = credibility)
    """
    score = 0
    source_parts = []

    # Trading activity score
    if pair_data:
        txns = pair_data.get('txns_24h', 0)
        vol = pair_data.get('volume_24h', 0)
        
        if txns >= 3000:
            score += 50
        elif txns >= 1000:
            score += 40
        elif txns >= 500:
            score += 30
        elif txns >= 200:
            score += 20
        elif txns >= 50:
            score += 10
        
        source_parts.append(f"txns={txns}")
        source_parts.append(f"vol=${vol:,.0f}")

    # DexScreener boost
    if dex_data:
        if dex_data.get('dex_has_profile'):
            score += 10
            source_parts.append("profile=✅")
        if dex_data.get('dex_has_boost'):
            score += 15
            source_parts.append(f"boost={dex_data.get('dex_boost_amount', 0)}")

    # Social presence from pair info
    if pair_data:
        if pair_data.get('has_twitter'):
            score += 8
            source_parts.append("tw=✅")
        if pair_data.get('has_website'):
            score += 5
            source_parts.append("web=✅")
        if pair_data.get('has_telegram'):
            score += 4
            source_parts.append("tg=✅")
        if pair_data.get('has_token_lock'):
            score += 8
            source_parts.append("lock=✅")

    return min(score, 100), ' '.join(source_parts)


# ─── HTTP Handler ─────────────────────────────────────────────────────────────

class SocialHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress default access logs

    def do_GET(self):
        parsed = urlparse(self.path)
        
        if parsed.path == '/health':
            self._respond(200, {'status': 'ok', 'uptime': int(time.time() - _start_time)})
            return

        params = parse_qs(parsed.query)
        ca = (params.get('ca', [''])[0] or '').strip()
        symbol = (params.get('symbol', [''])[0] or '').strip()

        if not ca:
            self._respond(400, {'error': 'ca parameter required'})
            return

        cache_key = f"full:{ca}"
        cached = _cache_get(cache_key)
        if cached:
            self._respond(200, cached)
            return

        try:
            # 1. DexScreener pair info (social links + trading activity)
            pair_data = _fetch_dex_pair_info(ca)

            # 2. DexScreener boost (paid promotion)
            dex_data = _fetch_dex_boost(ca)

            # Composite score
            social_score, source_str = _compute_social_score(pair_data, dex_data)

            # Estimate "twitter_mentions" from trading activity
            # More traders = more likely to be discussed on Twitter
            txns = (pair_data or {}).get('txns_24h', 0)
            estimated_mentions = 0
            if txns >= 3000:
                estimated_mentions = 50
            elif txns >= 1000:
                estimated_mentions = 20
            elif txns >= 500:
                estimated_mentions = 10
            elif txns >= 200:
                estimated_mentions = 5
            elif txns >= 50:
                estimated_mentions = 2

            result = {
                'ca': ca,
                'symbol': symbol,
                'twitter_mentions': estimated_mentions,
                'twitter_unique_authors': estimated_mentions,
                'twitter_engagement': 0,
                'dex_txns_24h': (pair_data or {}).get('txns_24h', 0),
                'dex_buyers_24h': (pair_data or {}).get('buys_24h', 0),
                'dex_sellers_24h': (pair_data or {}).get('sells_24h', 0),
                'dex_volume_24h': (pair_data or {}).get('volume_24h', 0),
                'dex_pairs_count': (pair_data or {}).get('pairs_count', 0),
                'dex_has_boost': (dex_data or {}).get('dex_has_boost', False),
                'dex_boost_amount': (dex_data or {}).get('dex_boost_amount', 0),
                'dex_has_profile': (dex_data or {}).get('dex_has_profile', False),
                'has_twitter_link': (pair_data or {}).get('has_twitter', False),
                'has_website': (pair_data or {}).get('has_website', False),
                'has_telegram': (pair_data or {}).get('has_telegram', False),
                'has_token_lock': (pair_data or {}).get('has_token_lock', False),
                'social_score': social_score,
                'twitter_ready': True,
                'source': source_str,
                'ts': int(time.time()),
            }
            _cache_set(cache_key, result, CACHE_TTL_SEC)
            self._respond(200, result)

        except Exception as e:
            log.error(f"Handler error: {e}")
            self._respond(500, {'error': str(e)})

    def _respond(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


# ─── Main ─────────────────────────────────────────────────────────────────────

_start_time = time.time()

def main():
    server = HTTPServer(('0.0.0.0', PORT), SocialHandler)
    log.info(f"🚀 Social Signal Service running on port {PORT}")
    log.info(f"   Community Activity: ✅ via DexScreener txns/volume (always works)")
    log.info(f"   DexScreener Boost: ✅ always available")
    log.info(f"   Social Links: ✅ twitter/website/telegram/token-lock")
    log.info(f"   Usage: GET http://localhost:{PORT}/social?ca=<address>&symbol=<symbol>")
    server.serve_forever()


if __name__ == '__main__':
    main()
