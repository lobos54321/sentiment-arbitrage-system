"""
Twitter Microservice API

FastAPI server providing Twitter data collection endpoints
for the sentiment arbitrage trading system.
"""

import os
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

from twikit_client import TwitterClient

# Load environment variables
load_dotenv()

# Global Twitter client
twitter_client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize Twitter client on startup"""
    global twitter_client
    twitter_client = TwitterClient()

    # Try to load saved cookies first
    cookies_loaded = await twitter_client.load_cookies()

    if not cookies_loaded:
        # Login with credentials from env
        username = os.getenv('TWITTER_USERNAME')
        email = os.getenv('TWITTER_EMAIL')
        password = os.getenv('TWITTER_PASSWORD')

        if username and email and password:
            try:
                await twitter_client.login(username, email, password)
            except Exception as e:
                print(f"⚠️  Twitter login failed: {e}")
                print("    Service will run in limited mode")

    yield

    # Cleanup
    print("🛑 Shutting down Twitter service")


# Initialize FastAPI app
app = FastAPI(
    title="Twitter Data Service",
    description="Crypto token social data collection via Twikit",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request/Response Models
class SearchRequest(BaseModel):
    queries: List[str]
    timeframe_minutes: int = 15


class ValidationRequest(BaseModel):
    token_ca: str
    token_symbol: str
    tg_mention_time: str


# API Endpoints
@app.get("/")
async def root():
    """Health check"""
    return {
        "service": "Twitter Data Service",
        "status": "online",
        "logged_in": twitter_client.logged_in if twitter_client else False
    }


@app.post("/api/search")
async def search_token(request: SearchRequest):
    """
    Search Twitter for token mentions

    Example:
        POST /api/search
        {
            "queries": ["$BONK", "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263pump"],
            "timeframe_minutes": 15
        }

    Returns:
        {
            "mention_count": 42,
            "unique_authors": 28,
            "engagement": 1523,
            "sentiment": "positive",
            "kol_mentions": ["@cryptoWhale (50k followers)"],
            "top_tweets": [...]
        }
    """
    if not twitter_client or not twitter_client.logged_in:
        raise HTTPException(
            status_code=503,
            detail="Twitter client not available - check credentials"
        )

    try:
        result = await twitter_client.search_token(
            queries=request.queries,
            timeframe_minutes=request.timeframe_minutes
        )
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/validate")
async def validate_signal(request: ValidationRequest):
    """
    Validate Telegram signal against Twitter activity

    Returns credibility score 0-100 and verification status
    """
    if not twitter_client or not twitter_client.logged_in:
        raise HTTPException(
            status_code=503,
            detail="Twitter client not available"
        )

    try:
        result = await twitter_client.validate_signal(
            token_ca=request.token_ca,
            token_symbol=request.token_symbol,
            tg_mention_time=request.tg_mention_time
        )
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/status")
async def service_status():
    """Get service status and statistics"""
    if not twitter_client:
        return {
            "initialized": False,
            "logged_in": False
        }

    return {
        "initialized": True,
        "logged_in": twitter_client.logged_in,
        "cache_dir": "./cache",
        "rate_limit_interval": twitter_client.min_request_interval
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )
