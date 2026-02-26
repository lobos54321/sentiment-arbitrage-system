"""
Grok API Client for Twitter Search

Fallback client when Twikit fails. Uses xAI Grok API with built-in Twitter search.
"""

import os
import asyncio
import httpx
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class GrokClient:
    """
    Grok API client for Twitter/X search.

    Uses xAI's Grok 4.1 Fast model with X Search tool.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('XAI_API_KEY')
        if not self.api_key:
            logger.warning("⚠️ No XAI_API_KEY found. Grok client will not work.")

        self.base_url = "https://api.x.ai/v1"
        self.model = "grok-4-1-fast"
        self.logged_in = bool(self.api_key)

    async def search_token(self, queries: List[str], timeframe_minutes: int = 15) -> Dict:
        """
        Search Twitter for token mentions using Grok API.

        Args:
            queries: List of search queries (token symbol, CA, etc.)
            timeframe_minutes: Timeframe to search within

        Returns:
            Same format as Twikit client for compatibility
        """
        if not self.api_key:
            raise Exception("XAI_API_KEY not configured")

        try:
            # Construct search prompt
            query_str = ' OR '.join(queries)
            prompt = f"""
Search Twitter/X for recent mentions of: {query_str}

Timeframe: Last {timeframe_minutes} minutes

Analyze the results and provide:
1. Total mention count
2. Number of unique authors
3. Total engagement (likes + retweets)
4. Overall sentiment (positive/neutral/negative)
5. KOL mentions (accounts with >10k followers)
6. Top 5 most engaged tweets

Return as JSON format:
{{
    "mention_count": <number>,
    "unique_authors": <number>,
    "engagement": <number>,
    "sentiment": "<positive/neutral/negative>",
    "sentiment_score": <-10 to +10>,
    "kol_mentions": [
        {{"username": "@user", "followers": <number>, "text": "tweet text"}}
    ],
    "top_tweets": [
        {{
            "text": "tweet text",
            "author": "@username",
            "engagement": <number>,
            "url": "https://twitter.com/..."
        }}
    ]
}}
"""

            # Call Grok API with X Search tool
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{self.base_url}/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": self.model,
                        "messages": [
                            {
                                "role": "system",
                                "content": "You are a Twitter data analyst. Use the X Search tool to find tweets and analyze them. Always return valid JSON."
                            },
                            {
                                "role": "user",
                                "content": prompt
                            }
                        ],
                        "tools": [
                            {
                                "type": "x_search"  # Use X Search tool
                            }
                        ],
                        "temperature": 0.3,  # Low temperature for consistent output
                        "max_tokens": 2000
                    }
                )

                if response.status_code != 200:
                    raise Exception(f"Grok API error: {response.status_code} - {response.text}")

                result = response.json()

                # Extract content from response
                content = result['choices'][0]['message']['content']

                # Parse JSON from response
                import json
                try:
                    # Try to extract JSON from markdown code block if present
                    if '```json' in content:
                        json_start = content.index('```json') + 7
                        json_end = content.index('```', json_start)
                        json_str = content[json_start:json_end].strip()
                    elif '```' in content:
                        json_start = content.index('```') + 3
                        json_end = content.index('```', json_start)
                        json_str = content[json_start:json_end].strip()
                    else:
                        json_str = content

                    data = json.loads(json_str)

                    logger.info(f"✅ Grok search successful: {data.get('mention_count', 0)} mentions found")
                    return data

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse Grok response: {e}")
                    logger.error(f"Response content: {content}")

                    # Return fallback response
                    return {
                        "mention_count": 0,
                        "unique_authors": 0,
                        "engagement": 0,
                        "sentiment": "neutral",
                        "sentiment_score": 0,
                        "kol_mentions": [],
                        "top_tweets": [],
                        "error": "Failed to parse Grok response"
                    }

        except Exception as e:
            logger.error(f"❌ Grok search failed: {e}")
            raise

    async def validate_signal(self, token_ca: str, token_symbol: str, tg_mention_time: str) -> Dict:
        """
        Validate Telegram signal against Twitter data.

        Compatible with Twikit client interface.
        """
        try:
            # Search Twitter
            twitter_data = await self.search_token(
                queries=[f"${token_symbol}", token_ca],
                timeframe_minutes=15
            )

            # Calculate credibility score
            credibility_score = 0
            reasons = []

            # Twitter activity score (max 40 points)
            if twitter_data['mention_count'] >= 20:
                credibility_score += 40
                reasons.append(f"High Twitter activity ({twitter_data['mention_count']} mentions)")
            elif twitter_data['mention_count'] >= 10:
                credibility_score += 25
                reasons.append(f"Moderate Twitter activity ({twitter_data['mention_count']} mentions)")
            elif twitter_data['mention_count'] >= 5:
                credibility_score += 15
                reasons.append(f"Some Twitter activity ({twitter_data['mention_count']} mentions)")

            # KOL mentions (max 30 points)
            kol_count = len(twitter_data.get('kol_mentions', []))
            if kol_count >= 3:
                credibility_score += 30
                reasons.append(f"Multiple KOL mentions ({kol_count} KOLs)")
            elif kol_count >= 1:
                credibility_score += 20
                reasons.append(f"KOL mentioned: {twitter_data['kol_mentions'][0]['username']}")

            # Engagement (max 20 points)
            if twitter_data['engagement'] >= 1000:
                credibility_score += 20
                reasons.append(f"High engagement ({twitter_data['engagement']})")
            elif twitter_data['engagement'] >= 500:
                credibility_score += 15
                reasons.append(f"Good engagement ({twitter_data['engagement']})")

            # Sentiment (max 10 points)
            if twitter_data['sentiment'] == 'positive':
                credibility_score += 10
                reasons.append("Positive sentiment")
            elif twitter_data['sentiment'] == 'neutral':
                credibility_score += 5

            verified = credibility_score >= 50

            return {
                'credibility_score': min(credibility_score, 100),
                'verified': verified,
                'reasons': reasons,
                'twitter_data': twitter_data,
                'source': 'grok_api'
            }

        except Exception as e:
            logger.error(f"Grok validation failed: {e}")
            return {
                'credibility_score': 0,
                'verified': False,
                'reasons': [f"Grok API error: {str(e)}"],
                'twitter_data': {},
                'source': 'grok_api'
            }


# Global Grok client instance
grok_client = GrokClient()
