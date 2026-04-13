"""
Social Signal Service
─────────────────────
Standalone HTTP microservice for social propagation signals.
Runs on Zeabur server (Python 3.10+).

Three data sources:
  1. X/Twitter mentions: via DexScreener token social links + Google search scraping
  2. DexScreener Boosts: whether project paid to boost (commitment signal)
  3. DexScreener social links: twitter followers, website existence

paper_trade_monitor.py calls:
  GET http://localhost:8765/social?ca=<address>&symbol=<symbol>

Returns JSON:
  {
    "twitter_mentions": int,       # tweets mentioning CA in recent time
    "twitter_unique_authors": int, # unique tweeters
    "twitter_engagement": int,     # likes + retweets estimate
    "dex_has_boost": bool,         # paid DexScreener boost
    "dex_boost_amount": int,       # boost credits spent
    "dex_has_profile": bool,       # has token profile (paid)
    "social_score": int,           # composite 0-100
    "source": str                  # data source info
  }
"""

import json
import os
import re
import time
import logging
import random
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, quote_plus
import urllib.request
import ssl

logging.basicConfig(level=logging.INFO, format='%(asctime)s [SocialSvc] %(message)s')
log = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
PORT = int(os.environ.get('SOCIAL_SERVICE_PORT', '8765'))
CACHE_TTL_SEC = 120       # Cache results for 2 minutes per CA
DEX_BOOST_CACHE_TTL = 60  # DexScreener boost cache
DEX_PAIR_CACHE_TTL = 300  # DexScreener pair info cache (5 min)

# ─── In-memory cache ─────────────────────────────────────────────────────────
_cache = {}  # {key: (result, expire_ts)}

def _cache_get(key):
    entry = _cache.get(key)
    if entry and time.time() < entry[1]:
        return entry[0]
    return None

def _cache_set(key, value, ttl):
    _cache[key] = (value, time.time() + ttl)


# ─── SSL context (skip verification for Google) ─────────────────────────────
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

_USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
]


# ─── Method 1: DexScreener Pair Social Info ──────────────────────────────────

def _fetch_dex_pair_info(ca):
    """Get token social info from DexScreener pairs API.
    
    This gives us:
    - Whether token has a Twitter/X linked
    - Whether token has a website  
    - Whether token has a Telegram
    These are free signals showing the project has some online presence.
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
            result = {'has_twitter': False, 'has_website': False, 'has_telegram': False, 'twitter_url': ''}
            _cache_set(cache_key, result, DEX_PAIR_CACHE_TTL)
            return result

        # Use first pair (highest liquidity)
        pair = pairs[0]
        info = pair.get('info', {})
        socials = info.get('socials', [])
        websites = info.get('websites', [])

        twitter_url = ''
        has_twitter = False
        has_telegram = False
        for s in socials:
            stype = (s.get('type') or '').lower()
            if stype == 'twitter':
                has_twitter = True
                twitter_url = s.get('url', '')
            elif stype == 'telegram':
                has_telegram = True

        result = {
            'has_twitter': has_twitter,
            'has_website': bool(websites),
            'has_telegram': has_telegram,
            'twitter_url': twitter_url,
        }
        _cache_set(cache_key, result, DEX_PAIR_CACHE_TTL)
        return result

    except Exception as e:
        log.warning(f"DexPair info error: {e}")
        return {'has_twitter': False, 'has_website': False, 'has_telegram': False, 'twitter_url': ''}


# ─── Method 2: Google Search for X/Twitter mentions ─────────────────────────

def _google_search_mentions(ca, symbol=''):
    """Search Google for recent X/Twitter mentions of this CA address.
    
    Uses: site:x.com "<CA_ADDRESS>" 
    Counts the number of search results as a proxy for Twitter mentions.
    
    This is free, requires no API key, and works without login.
    """
    cache_key = f"google_tw:{ca}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        # Search for CA on Twitter/X via Google
        query = f'site:x.com "{ca}"'
        encoded_query = quote_plus(query)
        url = f"https://www.google.com/search?q={encoded_query}&num=50&hl=en"

        req = urllib.request.Request(url, headers={
            'User-Agent': random.choice(_USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        resp = urllib.request.urlopen(req, timeout=10, context=_ssl_ctx)
        html = resp.read().decode('utf-8', errors='ignore')

        # Count search results mentioning x.com
        # Google result links contain "x.com" or "twitter.com"
        x_links = re.findall(r'https?://(?:x\.com|twitter\.com)/\w+/status/\d+', html)
        unique_links = set(x_links)

        # Extract unique authors from URLs (the username part)
        authors = set()
        for link in unique_links:
            match = re.search(r'(?:x\.com|twitter\.com)/(\w+)/status/', link)
            if match:
                authors.add(match.group(1).lower())

        # Also check "About X results" count
        about_match = re.search(r'About\s+([\d,]+)\s+results', html)
        estimated_total = 0
        if about_match:
            estimated_total = int(about_match.group(1).replace(',', ''))

        mentions = max(len(unique_links), estimated_total)

        result = {
            'twitter_mentions': mentions,
            'twitter_unique_authors': len(authors),
            'twitter_engagement': 0,  # Can't get from Google
        }
        _cache_set(cache_key, result, CACHE_TTL_SEC)
        log.info(f"Google→X [{ca[:8]}...]: {mentions} mentions, {len(authors)} authors")
        return result

    except Exception as e:
        log.warning(f"Google search error: {e}")
        return None


# ─── Method 3: Direct X.com search (no login, public page) ──────────────────

def _x_public_search(ca, symbol=''):
    """Try to get tweet count from X's public search page.
    
    X.com has some publicly accessible search results (limited).
    Falls back gracefully if blocked.
    """
    cache_key = f"x_pub:{ca}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        # X's search is mostly behind login, but embedded/oembed can work
        # Try the search suggestions API (lightweight)
        query = quote_plus(ca[:20])  # Use first 20 chars to avoid too-long URLs
        url = f"https://api.dexscreener.com/latest/dex/search?q={ca}"
        
        req = urllib.request.Request(url, headers={
            'User-Agent': random.choice(_USER_AGENTS),
        })
        resp = urllib.request.urlopen(req, timeout=6)
        data = json.loads(resp.read())
        
        # Count how many DEX pairs exist for this token (more pairs = more visibility)
        pairs = data.get('pairs', [])
        dex_visibility = len(pairs)
        
        result = {
            'dex_pairs_count': dex_visibility,
        }
        _cache_set(cache_key, result, CACHE_TTL_SEC)
        return result

    except Exception:
        return {'dex_pairs_count': 0}


# ─── DexScreener Boost ───────────────────────────────────────────────────────

def _fetch_dex_boost(ca):
    """Check DexScreener for paid boost / token profile.
    
    Boost = project paid DexScreener to promote token.
    This shows: project is willing to spend money, has some credibility.
    NOT a community signal, but a project commitment signal.
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
        
        # The response might be a list directly or an object with 'orders'
        data = json.loads(raw)

        # Handle both formats: list of orders or {orders: [...]}
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
        
        # Check for boost-type orders
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

def _compute_social_score(twitter_data, dex_data, pair_data):
    """Compute composite social score 0-100.
    
    Twitter/Google mentions (0-60 points):
      0 mentions  → 0
      1-4         → 10
      5-19        → 25
      20-49       → 40
      50+         → 60
    
    DexScreener (0-40 points):
      has_profile → +15 (project committed to listing)
      has_boost   → +25 (project spending real money = conviction)

    Pair social info (bonus, up to +10):
      has_twitter  → +5
      has_website  → +3
      has_telegram → +2
    """
    score = 0
    source_parts = []

    # Twitter/Google mentions
    if twitter_data:
        mentions = twitter_data.get('twitter_mentions', 0)
        if mentions >= 50:
            score += 60
        elif mentions >= 20:
            score += 40
        elif mentions >= 5:
            score += 25
        elif mentions >= 1:
            score += 10
        source_parts.append(f"tw={mentions}")

    # DexScreener boost
    if dex_data:
        if dex_data.get('dex_has_profile'):
            score += 15
            source_parts.append("profile=✅")
        if dex_data.get('dex_has_boost'):
            score += 25
            source_parts.append(f"boost={dex_data.get('dex_boost_amount', 0)}")

    # Social presence from pair info
    if pair_data:
        if pair_data.get('has_twitter'):
            score += 5
            source_parts.append("tw_link=✅")
        if pair_data.get('has_website'):
            score += 3
            source_parts.append("web=✅")
        if pair_data.get('has_telegram'):
            score += 2
            source_parts.append("tg=✅")

    return min(score, 100), ' '.join(source_parts)


# ─── HTTP Handler ─────────────────────────────────────────────────────────────

class SocialHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress default access logs

    def do_GET(self):
        parsed = urlparse(self.path)
        
        # Health check endpoint
        if parsed.path == '/health':
            self._respond(200, {'status': 'ok', 'uptime': int(time.time() - _start_time)})
            return

        params = parse_qs(parsed.query)

        ca = (params.get('ca', [''])[0] or '').strip()
        symbol = (params.get('symbol', [''])[0] or '').strip()

        if not ca:
            self._respond(400, {'error': 'ca parameter required'})
            return

        # Full cache key
        cache_key = f"full:{ca}"
        cached = _cache_get(cache_key)
        if cached:
            self._respond(200, cached)
            return

        try:
            # 1. DexScreener pair info (social links)
            pair_data = _fetch_dex_pair_info(ca)

            # 2. Google search for X/Twitter mentions
            twitter_data = _google_search_mentions(ca, symbol)

            # 3. DexScreener boost (paid promotion)
            dex_data = _fetch_dex_boost(ca)

            # Composite score
            social_score, source_str = _compute_social_score(twitter_data, dex_data, pair_data)

            result = {
                'ca': ca,
                'symbol': symbol,
                'twitter_mentions': (twitter_data or {}).get('twitter_mentions', 0),
                'twitter_unique_authors': (twitter_data or {}).get('twitter_unique_authors', 0),
                'twitter_engagement': (twitter_data or {}).get('twitter_engagement', 0),
                'dex_has_boost': (dex_data or {}).get('dex_has_boost', False),
                'dex_boost_amount': (dex_data or {}).get('dex_boost_amount', 0),
                'dex_has_profile': (dex_data or {}).get('dex_has_profile', False),
                'has_twitter_link': (pair_data or {}).get('has_twitter', False),
                'has_website': (pair_data or {}).get('has_website', False),
                'has_telegram': (pair_data or {}).get('has_telegram', False),
                'social_score': social_score,
                'twitter_ready': True,  # Always ready (Google-based, no login needed)
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
    # No async needed anymore — all sync HTTP calls

    # Start HTTP server
    server = HTTPServer(('0.0.0.0', PORT), SocialHandler)
    log.info(f"🚀 Social Signal Service running on port {PORT}")
    log.info(f"   Twitter Mentions: ✅ via Google Search (no login needed)")
    log.info(f"   DexScreener Boost: ✅ always available")
    log.info(f"   DexScreener Social Links: ✅ always available")
    log.info(f"   Usage: GET http://localhost:{PORT}/social?ca=<address>&symbol=<symbol>")
    server.serve_forever()


if __name__ == '__main__':
    main()
