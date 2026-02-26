"""
Twitter Microservice API v2 - With Health Monitoring and Auto-Failover

Enhanced FastAPI server with:
- Unified Twikit + Grok client
- Health monitoring
- Automatic failover
- Alert system
"""

import os
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import logging

from unified_twitter_client import twitter_client

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Alert storage (in-memory for demo, use DB in production)
alerts_history = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize Twitter client on startup"""
    logger.info("🚀 Starting Twitter Service v2...")

    # Load accounts from env
    accounts = []

    # Primary account
    username = os.getenv('TWITTER_USERNAME')
    email = os.getenv('TWITTER_EMAIL')
    password = os.getenv('TWITTER_PASSWORD')

    if username and email and password:
        accounts.append({
            'username': username,
            'email': email,
            'password': password
        })

    # Additional accounts (TWITTER_USERNAME_2, etc.)
    for i in range(2, 10):
        username = os.getenv(f'TWITTER_USERNAME_{i}')
        email = os.getenv(f'TWITTER_EMAIL_{i}')
        password = os.getenv(f'TWITTER_PASSWORD_{i}')

        if username and email and password:
            accounts.append({
                'username': username,
                'email': email,
                'password': password
            })

    if not accounts:
        logger.error("❌ No Twitter accounts configured in .env!")
        logger.info("    Add TWITTER_USERNAME, TWITTER_EMAIL, TWITTER_PASSWORD")
    else:
        # Initialize client
        grok_api_key = os.getenv('XAI_API_KEY')
        await twitter_client.initialize(accounts, grok_api_key)

        # Register alert callback
        def store_alert(alert: dict):
            alerts_history.append(alert)
            # Keep only recent 100 alerts
            if len(alerts_history) > 100:
                alerts_history.pop(0)

        twitter_client.register_alert_callback(store_alert)

        logger.info("✅ Twitter Service v2 ready!")

    yield

    # Cleanup
    logger.info("🛑 Shutting down Twitter service")


# Initialize FastAPI app
app = FastAPI(
    title="Twitter Data Service v2",
    description="Crypto token social data with health monitoring and auto-failover",
    version="2.0.0",
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
    status = twitter_client.get_status()
    return {
        "service": "Twitter Data Service v2",
        "status": "online",
        "provider": status['current_provider'],
        "health": status['health']['status']
    }


@app.post("/api/search")
async def search_token(request: SearchRequest):
    """
    Search Twitter for token mentions

    Automatically uses best available provider (Twikit or Grok)
    """
    try:
        result = await twitter_client.search_token(
            queries=request.queries,
            timeframe_minutes=request.timeframe_minutes
        )
        return result

    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/validate")
async def validate_signal(request: ValidationRequest):
    """
    Validate Telegram signal against Twitter activity
    """
    try:
        result = await twitter_client.validate_signal(
            token_ca=request.token_ca,
            token_symbol=request.token_symbol,
            tg_mention_time=request.tg_mention_time
        )
        return result

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/status")
async def service_status():
    """Get comprehensive service status"""
    return twitter_client.get_status()


@app.get("/api/alerts")
async def get_alerts(limit: int = 20):
    """
    Get recent alerts

    Returns system alerts including failover events, account issues, etc.
    """
    return {
        'total_alerts': len(alerts_history),
        'recent_alerts': alerts_history[-limit:]
    }


@app.post("/api/switch-provider")
async def switch_provider(provider: str):
    """
    Manually switch provider (twikit/grok)

    Use this to force provider switch for testing or manual control
    """
    if provider == 'grok':
        twitter_client._switch_to_grok("manual_switch")
        return {"message": "Switched to Grok API", "provider": "grok"}
    elif provider == 'twikit':
        twitter_client._switch_to_twikit()
        return {"message": "Switched to Twikit", "provider": "twikit"}
    else:
        raise HTTPException(status_code=400, detail="Invalid provider. Use 'twikit' or 'grok'")


@app.post("/api/rotate-account")
async def rotate_account():
    """
    Manually trigger account rotation (Twikit only)
    """
    if twitter_client.use_grok:
        raise HTTPException(status_code=400, detail="Currently using Grok, not Twikit")

    from account_pool import account_pool
    success = await account_pool.rotate_account("manual")

    if success:
        return {"message": "Account rotated successfully", "current_account": account_pool.current_account.username}
    else:
        raise HTTPException(status_code=500, detail="Failed to rotate account")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main_v2:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )
