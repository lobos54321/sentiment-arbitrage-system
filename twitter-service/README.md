# Twitter Data Service

Python microservice using **Twikit** to collect Twitter social data for crypto tokens.

## Features

- 🔍 Search tweets by token CA, symbol, or name
- 📊 Analyze sentiment, engagement, KOL mentions
- ✅ Validate Telegram signals against Twitter activity
- 💾 Built-in caching (5-minute TTL)
- 🛡️ Rate limiting protection
- 🔄 Cookie-based session persistence

## Setup

### 1. Install Dependencies

```bash
cd twitter-service
pip install -r requirements.txt
```

### 2. Configure Twitter Account

Copy `.env.example` to `.env` and fill in your Twitter credentials:

```bash
cp .env.example .env
```

Edit `.env`:
```
TWITTER_USERNAME=your_twitter_username
TWITTER_EMAIL=your_email@example.com
TWITTER_PASSWORD=your_password
```

**Important**:
- You need at least 1 Twitter account (can be new/burner account)
- Account should have some activity to avoid bot detection
- Cookies will be saved after first login for reuse

### 3. Start the Service

```bash
python main.py
```

Or with uvicorn:
```bash
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

Service runs on: `http://localhost:8001`

## API Endpoints

### `GET /`
Health check

**Response:**
```json
{
  "service": "Twitter Data Service",
  "status": "online",
  "logged_in": true
}
```

### `POST /api/search`
Search Twitter for token mentions

**Request:**
```json
{
  "queries": ["$BONK", "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263pump"],
  "timeframe_minutes": 15
}
```

**Response:**
```json
{
  "mention_count": 42,
  "unique_authors": 28,
  "engagement": 1523,
  "sentiment": "positive",
  "sentiment_score": 15,
  "kol_mentions": ["@cryptoWhale (50,000 followers)"],
  "top_tweets": [
    {
      "text": "This token is going to the moon! 🚀",
      "author": "@user123",
      "engagement": 250,
      "url": "https://twitter.com/user123/status/..."
    }
  ]
}
```

### `POST /api/validate`
Validate Telegram signal with Twitter data

**Request:**
```json
{
  "token_ca": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263pump",
  "token_symbol": "BONK",
  "tg_mention_time": "2025-12-18T22:30:00Z"
}
```

**Response:**
```json
{
  "credibility_score": 75,
  "verified": true,
  "reasons": [
    "High Twitter activity (42 mentions)",
    "KOL mentioned: @cryptoWhale (50,000 followers)",
    "High engagement (1,523)",
    "Positive sentiment"
  ],
  "twitter_data": { ... }
}
```

## How It Works

### 1. Twikit Client
- Uses Twikit library to bypass Twitter API costs
- Simulates browser behavior
- Rate-limited to 1 request per 2 seconds

### 2. Caching
- Search results cached for 5 minutes
- Uses `diskcache` for persistence
- Reduces Twitter requests and improves response time

### 3. Sentiment Analysis
- Simple keyword-based sentiment detection
- Positive keywords: moon, bullish, gem, lfg, buy, pump
- Negative keywords: scam, rug, dump, sell, bearish

### 4. KOL Detection
- Users with ≥10,000 followers considered KOLs
- Tracked separately for credibility scoring

## Integration with Main System

The Node.js main system calls this service via HTTP:

```javascript
// src/social/twitter-client.js
const response = await fetch('http://localhost:8001/api/search', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    queries: [`$${tokenSymbol}`, tokenCA],
    timeframe_minutes: 15
  })
});

const twitterData = await response.json();
// Use twitterData.mention_count, sentiment, etc.
```

## Troubleshooting

### Service won't start / Login fails
- Check Twitter credentials in `.env`
- Make sure account is active (not suspended)
- Try deleting `twitter_cookies.json` and restarting

### Rate limit errors
- Increase `min_request_interval` in `twikit_client.py`
- Implement multi-account rotation (future feature)

### No search results
- Twitter may be blocking the account temporarily
- Wait 15-30 minutes and try again
- Consider using multiple accounts

## Future Improvements

- [ ] Multi-account rotation for higher throughput
- [ ] Advanced sentiment analysis (ML model)
- [ ] Real-time streaming (Twitter Streaming API alternative)
- [ ] Historical data storage and trend analysis
- [ ] Automatic account health monitoring

## License

MIT
