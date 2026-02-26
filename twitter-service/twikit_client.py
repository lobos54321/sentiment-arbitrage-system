"""
Twikit Client Wrapper for Twitter Data Collection

Provides high-level interface for searching tweets, getting user info,
and analyzing social sentiment for crypto tokens.
"""

import asyncio
import time
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import json
import os

from twikit import Client
from diskcache import Cache


class TwitterClient:
    """
    Twitter client using Twikit for data collection

    Features:
    - Token search with caching
    - Rate limiting protection
    - Multi-account rotation (planned)
    - Sentiment analysis
    """

    def __init__(self, cache_dir='./cache'):
        self.client = Client('en-US')
        self.cache = Cache(cache_dir)
        self.last_request_time = 0
        self.min_request_interval = 2.0  # seconds between requests
        self.logged_in = False

    async def login(self, username: str, email: str, password: str):
        """Login to Twitter account"""
        try:
            await self.client.login(
                auth_info_1=username,
                auth_info_2=email,
                password=password
            )
            self.logged_in = True
            print(f"✅ Logged in as @{username}")

            # Save cookies for reuse
            self.client.save_cookies('twitter_cookies.json')

        except Exception as e:
            print(f"❌ Login failed: {e}")
            raise

    async def load_cookies(self, cookie_file='twitter_cookies.json'):
        """Load saved cookies to avoid re-login"""
        if os.path.exists(cookie_file):
            self.client.load_cookies(cookie_file)
            self.logged_in = True
            print("✅ Loaded cookies from file")
            return True
        return False

    def _rate_limit_wait(self):
        """Implement rate limiting"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    async def search_token(
        self,
        queries: List[str],
        timeframe_minutes: int = 15
    ) -> Dict:
        """
        Search Twitter for token mentions

        Args:
            queries: List of search terms (token CA, $SYMBOL, name)
            timeframe_minutes: Time window to search (default 15 min)

        Returns:
            {
                'mention_count': int,
                'unique_authors': int,
                'engagement': int (likes + retweets),
                'sentiment': 'positive'|'neutral'|'negative',
                'kol_mentions': List[str],
                'top_tweets': List[Dict]
            }
        """
        # Check cache first
        cache_key = f"search:{':'.join(queries)}:{timeframe_minutes}"
        cached = self.cache.get(cache_key)
        if cached:
            print(f"📦 Cache hit for {queries}")
            return cached

        # Rate limit
        self._rate_limit_wait()

        # Search tweets
        all_tweets = []
        for query in queries:
            try:
                print(f"🔍 Searching Twitter for: {query}")
                tweets = await self.client.search_tweet(
                    query,
                    product='Latest',
                    count=20
                )

                # Filter by timeframe
                cutoff_time = datetime.now() - timedelta(minutes=timeframe_minutes)
                for tweet in tweets:
                    tweet_time = datetime.strptime(
                        tweet.created_at,
                        '%a %b %d %H:%M:%S %z %Y'
                    )
                    if tweet_time.replace(tzinfo=None) > cutoff_time:
                        all_tweets.append(tweet)

            except Exception as e:
                print(f"⚠️  Search error for '{query}': {e}")
                continue

        # Analyze results
        result = self._analyze_tweets(all_tweets)

        # Cache for 5 minutes
        self.cache.set(cache_key, result, expire=300)

        return result

    def _analyze_tweets(self, tweets: List) -> Dict:
        """Analyze collected tweets"""
        if not tweets:
            return {
                'mention_count': 0,
                'unique_authors': 0,
                'engagement': 0,
                'sentiment': 'neutral',
                'kol_mentions': [],
                'top_tweets': []
            }

        unique_authors = set()
        total_engagement = 0
        kol_threshold = 10000  # Followers count to be considered KOL
        kol_mentions = []
        top_tweets = []

        # Sentiment counters
        positive_keywords = ['moon', 'bullish', 'gem', 'lfg', 'buy', 'pump']
        negative_keywords = ['scam', 'rug', 'dump', 'sell', 'bearish']
        positive_count = 0
        negative_count = 0

        for tweet in tweets:
            # Author analysis
            author = tweet.user.screen_name
            unique_authors.add(author)

            # KOL detection
            if tweet.user.followers_count >= kol_threshold:
                kol_mentions.append(f"@{author} ({tweet.user.followers_count:,} followers)")

            # Engagement
            engagement = tweet.favorite_count + tweet.retweet_count
            total_engagement += engagement

            # Sentiment analysis (basic)
            text_lower = tweet.text.lower()
            if any(kw in text_lower for kw in positive_keywords):
                positive_count += 1
            if any(kw in text_lower for kw in negative_keywords):
                negative_count += 1

            # Store top tweets
            top_tweets.append({
                'text': tweet.text[:200],
                'author': f"@{author}",
                'engagement': engagement,
                'url': f"https://twitter.com/{author}/status/{tweet.id}"
            })

        # Sort top tweets by engagement
        top_tweets.sort(key=lambda x: x['engagement'], reverse=True)
        top_tweets = top_tweets[:5]

        # Determine sentiment
        if positive_count > negative_count * 2:
            sentiment = 'positive'
        elif negative_count > positive_count * 2:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'

        return {
            'mention_count': len(tweets),
            'unique_authors': len(unique_authors),
            'engagement': total_engagement,
            'sentiment': sentiment,
            'sentiment_score': positive_count - negative_count,
            'kol_mentions': kol_mentions,
            'top_tweets': top_tweets
        }

    async def validate_signal(
        self,
        token_ca: str,
        token_symbol: str,
        tg_mention_time: str
    ) -> Dict:
        """
        Validate Telegram signal against Twitter activity

        Returns credibility score 0-100
        """
        # Search for the token
        queries = [f"${token_symbol}", token_ca]
        result = await self.search_token(queries, timeframe_minutes=30)

        # Calculate credibility score
        credibility = 0
        reasons = []

        # Factor 1: Mention count (0-40 points)
        mentions = result['mention_count']
        if mentions >= 20:
            credibility += 40
            reasons.append(f"High Twitter activity ({mentions} mentions)")
        elif mentions >= 10:
            credibility += 30
            reasons.append(f"Moderate Twitter activity ({mentions} mentions)")
        elif mentions >= 5:
            credibility += 20
            reasons.append(f"Low Twitter activity ({mentions} mentions)")
        elif mentions > 0:
            credibility += 10
            reasons.append(f"Minimal Twitter activity ({mentions} mentions)")
        else:
            reasons.append("No Twitter mentions found")

        # Factor 2: KOL involvement (0-30 points)
        kols = len(result['kol_mentions'])
        if kols >= 3:
            credibility += 30
            reasons.append(f"Multiple KOLs mentioned ({kols})")
        elif kols >= 1:
            credibility += 20
            reasons.append(f"KOL mentioned: {result['kol_mentions'][0]}")
        else:
            reasons.append("No KOL mentions")

        # Factor 3: Engagement (0-20 points)
        engagement = result['engagement']
        if engagement >= 1000:
            credibility += 20
            reasons.append(f"High engagement ({engagement:,})")
        elif engagement >= 500:
            credibility += 15
        elif engagement >= 100:
            credibility += 10

        # Factor 4: Sentiment (0-10 points)
        if result['sentiment'] == 'positive':
            credibility += 10
            reasons.append("Positive sentiment")
        elif result['sentiment'] == 'negative':
            credibility -= 20
            reasons.append("⚠️ Negative sentiment detected")

        return {
            'credibility_score': max(0, min(100, credibility)),
            'twitter_data': result,
            'reasons': reasons,
            'verified': credibility >= 50
        }
