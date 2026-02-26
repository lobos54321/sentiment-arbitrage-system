"""
Unified Twitter Client with Health Monitoring and Auto-Failover

Combines Twikit (primary), Account Pool (rotation), and Grok (fallback)
with intelligent health monitoring and automatic switching.
"""

import asyncio
import time
import os
from typing import List, Dict, Optional
from datetime import datetime
import logging

from twikit import Client
from diskcache import Cache

from health_monitor import health_monitor, HealthStatus
from account_pool import account_pool, AccountPool
from grok_client import grok_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class UnifiedTwitterClient:
    """
    Unified Twitter client with automatic failover strategy:

    1. Primary: Twikit with account pool rotation
    2. Fallback: Grok API when Twikit fails
    3. Monitoring: Real-time health tracking
    4. Alerting: Notify when failover occurs
    """

    def __init__(self, cache_dir='./cache'):
        self.cache = Cache(cache_dir)
        self.use_grok = False  # Start with Twikit
        self.alert_callbacks = []

        # Setup health monitor alert
        health_monitor.register_alert_callback(self._on_health_alert)

    async def initialize(self, accounts: List[Dict[str, str]], grok_api_key: Optional[str] = None):
        """
        Initialize client with account pool.

        Args:
            accounts: List of {'username', 'email', 'password'} dicts
            grok_api_key: Optional Grok API key for fallback
        """
        # Initialize Twikit account pool
        for acc in accounts:
            account_pool.add_account(
                username=acc['username'],
                email=acc['email'],
                password=acc['password']
            )

        await account_pool.initialize()

        # Initialize Grok client if key provided
        if grok_api_key:
            grok_client.api_key = grok_api_key
            grok_client.logged_in = True
            logger.info("✅ Grok client initialized as fallback")

        logger.info(f"🚀 Unified Twitter client ready (Accounts: {len(accounts)}, Grok: {grok_client.logged_in})")

    def register_alert_callback(self, callback):
        """Register callback for system alerts"""
        self.alert_callbacks.append(callback)

    def _trigger_alert(self, alert_type: str, message: str):
        """Trigger alert to user"""
        timestamp = datetime.now().isoformat()
        alert = {
            'timestamp': timestamp,
            'type': alert_type,
            'message': message,
            'health_status': health_monitor.status.value,
            'using_grok': self.use_grok
        }

        logger.warning(f"🚨 ALERT: {message}")

        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                logger.error(f"Alert callback error: {e}")

    def _on_health_alert(self, alert_type: str, *args):
        """Handle health monitor alerts"""
        if alert_type == "critical_failure":
            error_type, error_message = args
            self._trigger_alert(
                "twikit_critical_failure",
                f"⛔ Twikit账号可能被封: {error_type} - {error_message}. 切换到Grok备用方案!"
            )
            self._switch_to_grok("账号被封或严重错误")

        elif alert_type == "status_change":
            old_status, new_status = args

            if new_status == HealthStatus.FAILING:
                self._trigger_alert(
                    "twikit_degraded",
                    f"⚠️ Twikit服务状态变差: {old_status.value} → {new_status.value}. 考虑账号轮换或切换Grok."
                )

                # Try account rotation first
                if not self.use_grok:
                    asyncio.create_task(account_pool.rotate_account("health_degraded"))

            elif new_status == HealthStatus.FAILED:
                self._trigger_alert(
                    "twikit_failed",
                    f"🚨 Twikit完全失效! 自动切换到Grok API."
                )
                self._switch_to_grok("健康检查失败")

    def _switch_to_grok(self, reason: str):
        """Switch to Grok API fallback"""
        if self.use_grok:
            logger.warning("Already using Grok")
            return

        if not grok_client.logged_in:
            logger.error("❌ Cannot switch to Grok - API key not configured!")
            self._trigger_alert(
                "failover_failed",
                "❌ 无法切换到Grok! API密钥未配置. 系统将继续尝试Twikit."
            )
            return

        self.use_grok = True
        logger.warning(f"🔄 Switched to Grok API (reason: {reason})")
        self._trigger_alert(
            "switched_to_grok",
            f"✅ 已切换到Grok API备用方案. 原因: {reason}"
        )

    def _switch_to_twikit(self):
        """Switch back to Twikit"""
        if not self.use_grok:
            return

        self.use_grok = False
        health_monitor.reset()
        logger.info("🔄 Switched back to Twikit")
        self._trigger_alert(
            "switched_to_twikit",
            "✅ 已恢复使用Twikit (免费方案)"
        )

    async def search_token(self, queries: List[str], timeframe_minutes: int = 15) -> Dict:
        """
        Search Twitter for token mentions (unified interface).

        Automatically uses Twikit or Grok based on health status.
        """
        # Check cache first
        cache_key = f"search:{':'.join(sorted(queries))}:{timeframe_minutes}"
        cached = self.cache.get(cache_key)
        if cached:
            logger.debug(f"Cache hit for {queries}")
            return cached

        start_time = time.time()

        try:
            if self.use_grok:
                # Use Grok API
                logger.info(f"🔍 Searching with Grok: {queries}")
                result = await grok_client.search_token(queries, timeframe_minutes)
                result['source'] = 'grok_api'

            else:
                # Use Twikit with account pool
                client = account_pool.get_current_client()
                if not client:
                    raise Exception("No available Twikit accounts")

                logger.info(f"🔍 Searching with Twikit (@{account_pool.current_account.username}): {queries}")

                # Implement search logic here (simplified from original twikit_client.py)
                all_tweets = []
                for query in queries:
                    try:
                        tweets = await client.search_tweet(query, product='Latest', count=20)

                        # Filter by timeframe
                        cutoff_time = datetime.now() - timedelta(minutes=timeframe_minutes)
                        for tweet in tweets:
                            if tweet.created_at and tweet.created_at > cutoff_time:
                                all_tweets.append(tweet)

                    except Exception as e:
                        logger.warning(f"Search query '{query}' failed: {e}")

                # Analyze tweets (same logic as original)
                result = self._analyze_tweets(all_tweets)
                result['source'] = 'twikit'

            # Record success
            response_time = time.time() - start_time
            health_monitor.record_success(response_time)

            # Cache result
            self.cache.set(cache_key, result, expire=300)  # 5 minutes

            return result

        except Exception as e:
            # Record failure
            error_type = type(e).__name__
            health_monitor.record_failure(error_type, str(e))
            account_pool.mark_current_failed()

            # Check if should failover
            if health_monitor.should_failover() and not self.use_grok:
                logger.warning("⚠️ Failing over to Grok due to repeated failures")
                self._switch_to_grok(f"连续失败: {error_type}")

                # Retry with Grok
                return await self.search_token(queries, timeframe_minutes)

            raise

    async def validate_signal(self, token_ca: str, token_symbol: str, tg_mention_time: str) -> Dict:
        """Validate Telegram signal against Twitter data"""
        try:
            if self.use_grok:
                return await grok_client.validate_signal(token_ca, token_symbol, tg_mention_time)
            else:
                # Use Twikit
                twitter_data = await self.search_token(
                    queries=[f"${token_symbol}", token_ca],
                    timeframe_minutes=15
                )

                # Calculate credibility (same as grok_client logic)
                credibility_score = 0
                reasons = []

                if twitter_data['mention_count'] >= 20:
                    credibility_score += 40
                    reasons.append(f"High Twitter activity ({twitter_data['mention_count']} mentions)")
                elif twitter_data['mention_count'] >= 10:
                    credibility_score += 25

                kol_count = len(twitter_data.get('kol_mentions', []))
                if kol_count >= 3:
                    credibility_score += 30
                elif kol_count >= 1:
                    credibility_score += 20

                if twitter_data['engagement'] >= 1000:
                    credibility_score += 20

                if twitter_data['sentiment'] == 'positive':
                    credibility_score += 10

                return {
                    'credibility_score': min(credibility_score, 100),
                    'verified': credibility_score >= 50,
                    'reasons': reasons,
                    'twitter_data': twitter_data,
                    'source': 'twikit'
                }

        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise

    def _analyze_tweets(self, tweets: List) -> Dict:
        """Analyze tweets for sentiment and engagement"""
        if not tweets:
            return {
                'mention_count': 0,
                'unique_authors': 0,
                'engagement': 0,
                'sentiment': 'neutral',
                'sentiment_score': 0,
                'kol_mentions': [],
                'top_tweets': []
            }

        # Count metrics
        mention_count = len(tweets)
        unique_authors = len(set(t.user.screen_name for t in tweets if t.user))
        total_engagement = sum((t.favorite_count or 0) + (t.retweet_count or 0) for t in tweets)

        # Sentiment analysis
        positive_keywords = ['moon', 'bullish', 'gem', 'lfg', 'buy', 'pump', '🚀', '💎', '🔥']
        negative_keywords = ['scam', 'rug', 'dump', 'sell', 'bearish', '⚠️']

        sentiment_score = 0
        for tweet in tweets:
            text = (tweet.text or '').lower()
            sentiment_score += sum(1 for kw in positive_keywords if kw in text)
            sentiment_score -= sum(1 for kw in negative_keywords if kw in text)

        sentiment = 'positive' if sentiment_score > 2 else ('negative' if sentiment_score < -2 else 'neutral')

        # KOL detection
        kol_mentions = []
        for tweet in tweets:
            if tweet.user and tweet.user.followers_count >= 10000:
                kol_mentions.append({
                    'username': f"@{tweet.user.screen_name}",
                    'followers': tweet.user.followers_count,
                    'text': tweet.text[:100] if tweet.text else ''
                })

        # Top tweets
        sorted_tweets = sorted(tweets, key=lambda t: (t.favorite_count or 0) + (t.retweet_count or 0), reverse=True)
        top_tweets = [
            {
                'text': t.text[:200] if t.text else '',
                'author': f"@{t.user.screen_name}" if t.user else 'unknown',
                'engagement': (t.favorite_count or 0) + (t.retweet_count or 0),
                'url': f"https://twitter.com/{t.user.screen_name}/status/{t.id}" if t.user and t.id else ''
            }
            for t in sorted_tweets[:5]
        ]

        return {
            'mention_count': mention_count,
            'unique_authors': unique_authors,
            'engagement': total_engagement,
            'sentiment': sentiment,
            'sentiment_score': sentiment_score,
            'kol_mentions': kol_mentions[:5],
            'top_tweets': top_tweets
        }

    def get_status(self) -> Dict:
        """Get comprehensive system status"""
        return {
            'current_provider': 'grok' if self.use_grok else 'twikit',
            'health': health_monitor.get_health_report(),
            'account_pool': account_pool.get_pool_status() if not self.use_grok else None,
            'grok_available': grok_client.logged_in
        }


# Global unified client
twitter_client = UnifiedTwitterClient()
