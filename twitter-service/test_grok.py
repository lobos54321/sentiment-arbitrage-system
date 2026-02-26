"""
Simple test script for Grok API - Twitter search
"""

import asyncio
import httpx
import os
from dotenv import load_dotenv

load_dotenv()

async def test_grok_twitter_search():
    """Test Grok API with Twitter search"""

    api_key = os.getenv('XAI_API_KEY')
    if not api_key:
        print("❌ XAI_API_KEY not found in .env")
        return

    print(f"✅ API Key loaded: {api_key[:20]}...")

    # Test search for a popular token
    query = "$BONK"

    prompt = f"""
I need you to search Twitter/X for recent tweets about: {query}

Look for tweets from the last 60 minutes.

Analyze what you find and provide this information in JSON format:
{{
    "mention_count": <number of tweets mentioning {query}>,
    "unique_authors": <number of different accounts posting>,
    "engagement": <total likes + retweets across all tweets>,
    "sentiment": "<positive/neutral/negative based on tweet content>"
}}

Return ONLY the JSON, no other text.
"""

    print(f"\n🔍 Searching Twitter for: {query}")
    print("=" * 60)

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                "https://api.x.ai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "grok-4-1-fast",
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a Twitter data analyst with access to Twitter/X data. Search for recent tweets and analyze them. Always return valid JSON only, no other text."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "temperature": 0.3,
                    "max_tokens": 2000
                }
            )

            if response.status_code != 200:
                print(f"❌ API Error: {response.status_code}")
                print(response.text)
                return

            result = response.json()

            # Extract content
            content = result['choices'][0]['message']['content']
            usage = result['usage']

            print(f"\n✅ Grok API Success!")
            print(f"\nResponse:\n{content}")
            print(f"\nToken Usage:")
            print(f"  Input:  {usage['prompt_tokens']}")
            print(f"  Output: {usage['completion_tokens']}")
            print(f"  Total:  {usage['total_tokens']}")

            # Try to extract JSON
            import json
            try:
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

                print(f"\n📊 Parsed Data:")
                print(f"  Mentions: {data.get('mention_count', 0)}")
                print(f"  Authors:  {data.get('unique_authors', 0)}")
                print(f"  Engagement: {data.get('engagement', 0)}")
                print(f"  Sentiment: {data.get('sentiment', 'unknown')}")

            except Exception as e:
                print(f"\n⚠️  Could not parse JSON: {e}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(test_grok_twitter_search())
