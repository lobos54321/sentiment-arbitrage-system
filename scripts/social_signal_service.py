"""
Social Signal Service
─────────────────────
Standalone HTTP microservice for social propagation signals.
Runs on Zeabur server (Python 3.10+).

Two data sources:
  1. Twitter: search CA address mentions in last N minutes (via twikit)
  2. DexScreener Boosts: whether project paid to boost (commitment signal)

paper_trade_monitor.py calls:
  GET http://localhost:8765/social?ca=<address>&symbol=<symbol>

Returns JSON:
  {
    "twitter_mentions": int,       # tweets mentioning CA in last 30 min
    "twitter_unique_authors": int, # unique tweeters
    "twitter_engagement": int,     # likes + retweets
    "dex_has_boost": bool,         # paid DexScreener boost
    "dex_boost_amount": int,       # boost credits spent
    "dex_has_profile": bool,       # has token profile (paid)
    "social_score": int,           # composite 0-100
    "source": str                  # data source info
  }
"""

import asyncio
import json
import os
import time
import logging
import hashlib
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import threading
import urllib.request

logging.basicConfig(level=logging.INFO, format='%(asctime)s [SocialSvc] %(message)s')
log = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
PORT = int(os.environ.get('SOCIAL_SERVICE_PORT', '8765'))
TWITTER_USERNAME = os.environ.get('TWITTER_USERNAME', '')
TWITTER_EMAIL = os.environ.get('TWITTER_EMAIL', '')
TWITTER_PASSWORD = os.environ.get('TWITTER_PASSWORD', '')
COOKIES_PATH = os.environ.get('TWITTER_COOKIES_PATH', './cache/tw_cookies.json')
CACHE_TTL_SEC = 120   # Cache results for 2 minutes per CA
DEX_BOOST_CACHE_TTL = 60  # DexScreener boost cache

# ─── In-memory cache ─────────────────────────────────────────────────────────
_cache = {}  # {key: (result, expire_ts)}

def _cache_get(key):
    entry = _cache.get(key)
    if entry and time.time() < entry[1]:
        return entry[0]
    return None

def _cache_set(key, value, ttl):
    _cache[key] = (value, time.time() + ttl)

# ─── Twitter client (singleton, async) ───────────────────────────────────────
_twitter_client = None
_twitter_ready = False
_twitter_lock = threading.Lock()

async def _init_twitter():
    """Initialize twikit client with credentials."""
    global _twitter_client, _twitter_ready
    try:
        from twikit import Client
        client = Client('en-US')

        os.makedirs(os.path.dirname(COOKIES_PATH) if os.path.dirname(COOKIES_PATH) else '.', exist_ok=True)

        # Try loading saved cookies first
        if os.path.exists(COOKIES_PATH):
            try:
                client.load_cookies(COOKIES_PATH)
                log.info("✅ Twitter: loaded existing cookies")
                _twitter_client = client
                _twitter_ready = True
                return True
            except Exception as e:
                log.warning(f"Cookies load failed: {e}, trying fresh login")

        # Fresh login
        if not TWITTER_USERNAME or not TWITTER_PASSWORD:
            log.error("❌ Twitter: no credentials configured (TWITTER_USERNAME/TWITTER_PASSWORD)")
            return False

        await client.login(
            auth_info_1=TWITTER_USERNAME,
            auth_info_2=TWITTER_EMAIL,
            password=TWITTER_PASSWORD
        )
        client.save_cookies(COOKIES_PATH)
        _twitter_client = client
        _twitter_ready = True
        log.info(f"✅ Twitter: logged in as @{TWITTER_USERNAME}")
        return True

    except Exception as e:
        log.error(f"❌ Twitter init failed: {e}")
        return False


async def _search_twitter(ca, symbol='', minutes=30):
    """Search Twitter for CA mentions in the last N minutes."""
    global _twitter_client, _twitter_ready

    if not _twitter_ready or not _twitter_client:
        return None

    cache_key = f"tw:{ca}:{minutes}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        # Build search query: CA address (most specific) OR $SYMBOL
        queries = [ca]
        if symbol:
            queries.append(f'${symbol}')

        total_mentions = 0
        unique_authors = set()
        total_engagement = 0

        for q in queries:
            tweets = await _twitter_client.search_tweet(q, product='Latest', count=50)
            tweet_list = list(tweets) if tweets else []

            now = time.time()
            cutoff = now - (minutes * 60)

            for tweet in tweet_list:
                try:
                    # Parse tweet created_at
                    import datetime
                    created = tweet.created_at_datetime
                    if created:
                        ts = created.timestamp()
                        if ts < cutoff:
                            continue
                    total_mentions += 1
                    unique_authors.add(tweet.user.id if tweet.user else 'unknown')
                    likes = getattr(tweet, 'favorite_count', 0) or 0
                    rts = getattr(tweet, 'retweet_count', 0) or 0
                    total_engagement += likes + rts
                except Exception:
                    total_mentions += 1

        result = {
            'twitter_mentions': total_mentions,
            'twitter_unique_authors': len(unique_authors),
            'twitter_engagement': total_engagement,
        }
        _cache_set(cache_key, result, CACHE_TTL_SEC)
        log.info(f"Twitter [{ca[:8]}...]: {total_mentions} mentions, {len(unique_authors)} authors, {total_engagement} engagement")
        return result

    except Exception as e:
        log.warning(f"Twitter search error: {e}")
        # If CSRF / auth error, mark as not ready to force re-init
        if 'csrf' in str(e).lower() or '401' in str(e) or '403' in str(e):
            _twitter_ready = False
        return None


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
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        resp = urllib.request.urlopen(req, timeout=6)
        data = json.loads(resp.read())

        orders = data.get('orders', [])
        boosts = data.get('boosts', [])

        has_profile = any(o.get('type') == 'tokenProfile' and o.get('status') == 'approved' for o in orders)
        boost_credits = sum(b.get('totalAmount', 0) for b in boosts)
        has_active_boost = any(b.get('active', False) for b in boosts) if boosts else boost_credits > 0

        result = {
            'dex_has_boost': has_active_boost or boost_credits > 0,
            'dex_boost_amount': int(boost_credits),
            'dex_has_profile': has_profile,
        }
        _cache_set(cache_key, result, DEX_BOOST_CACHE_TTL)
        log.info(f"DexBoost [{ca[:8]}...]: profile={has_profile} boost={boost_credits}")
        return result

    except Exception as e:
        log.warning(f"DexScreener boost error: {e}")
        return {'dex_has_boost': False, 'dex_boost_amount': 0, 'dex_has_profile': False}


# ─── Social Score Computation ─────────────────────────────────────────────────

def _compute_social_score(twitter_data, dex_data):
    """Compute composite social score 0-100.
    
    Twitter mentions: 0-60 points
      0 mentions  → 0
      1-4         → 10
      5-19        → 25
      20-49       → 40
      50+         → 60
    
    DexScreener:
      has_profile → +15 (project committed to listing)
      has_boost   → +25 (project spending real money = conviction)
    """
    score = 0
    source_parts = []

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

    if dex_data:
        if dex_data.get('dex_has_profile'):
            score += 15
            source_parts.append("profile=✅")
        if dex_data.get('dex_has_boost'):
            score += 25
            source_parts.append(f"boost={dex_data.get('dex_boost_amount', 0)}")

    return min(score, 100), ' '.join(source_parts)


# ─── HTTP Handler ─────────────────────────────────────────────────────────────

_loop = None

class SocialHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress default access logs

    def do_GET(self):
        parsed = urlparse(self.path)
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
            # Twitter (async — run in event loop)
            twitter_data = None
            if _twitter_ready and _loop:
                future = asyncio.run_coroutine_threadsafe(
                    _search_twitter(ca, symbol), _loop
                )
                try:
                    twitter_data = future.result(timeout=15)
                except Exception as e:
                    log.warning(f"Twitter timeout/error: {e}")

            # DexScreener boost (sync)
            dex_data = _fetch_dex_boost(ca)

            # Composite score
            social_score, source_str = _compute_social_score(twitter_data, dex_data)

            result = {
                'ca': ca,
                'symbol': symbol,
                'twitter_mentions': (twitter_data or {}).get('twitter_mentions', 0),
                'twitter_unique_authors': (twitter_data or {}).get('twitter_unique_authors', 0),
                'twitter_engagement': (twitter_data or {}).get('twitter_engagement', 0),
                'dex_has_boost': (dex_data or {}).get('dex_has_boost', False),
                'dex_boost_amount': (dex_data or {}).get('dex_boost_amount', 0),
                'dex_has_profile': (dex_data or {}).get('dex_has_profile', False),
                'social_score': social_score,
                'twitter_ready': _twitter_ready,
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

def _run_async_loop(loop):
    """Run asyncio event loop in background thread."""
    asyncio.set_event_loop(loop)
    loop.run_forever()


def main():
    global _loop

    # Setup asyncio loop in background thread
    _loop = asyncio.new_event_loop()
    t = threading.Thread(target=_run_async_loop, args=(_loop,), daemon=True)
    t.start()

    # Initialize Twitter in background
    future = asyncio.run_coroutine_threadsafe(_init_twitter(), _loop)
    try:
        future.result(timeout=30)
    except Exception as e:
        log.warning(f"Twitter init timeout: {e}")

    # Start HTTP server
    server = HTTPServer(('0.0.0.0', PORT), SocialHandler)
    log.info(f"🚀 Social Signal Service running on port {PORT}")
    log.info(f"   Twitter: {'✅ ready' if _twitter_ready else '❌ not available'}")
    log.info(f"   DexScreener Boost: ✅ always available")
    log.info(f"   Usage: GET http://localhost:{PORT}/social?ca=<address>&symbol=<symbol>")
    server.serve_forever()


if __name__ == '__main__':
    main()
