# Use Node.js 20 LTS (better compatibility)
FROM node:20-slim

# Install build tools for native modules (better-sqlite3) + Python
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    make \
    g++ \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application code
COPY . .

EXPOSE 3000

CMD bash -c "mkdir -p /app/data && \
echo '[STARTUP] Starting Node.js...' && \
SENTIMENT_DB=/app/data/sentiment_arb.db \
PAPER_DB=/app/data/paper_trades.db \
LIFECYCLE_DB=/app/data/lifecycle_tracks.db \
KLINE_DB=/app/data/kline_cache.db \
SHADOW_MODE=true \
AUTO_BUY_ENABLED=false \
PYTHONUNBUFFERED=1 \
node src/index.js --premium & \
echo '[STARTUP] Starting lifecycle-tracker...' && \
SENTIMENT_DB=/app/data/sentiment_arb.db \
LIFECYCLE_DB=/app/data/lifecycle_tracks.db \
KLINE_DB=/app/data/kline_cache.db \
PYTHONUNBUFFERED=1 \
python3 scripts/lifecycle_24h_tracker.py --track >> /app/data/lifecycle.log 2>&1 & \
echo '[STARTUP] Starting paper-trader...' && \
SENTIMENT_DB=/app/data/sentiment_arb.db \
PAPER_DB=/app/data/paper_trades.db \
KLINE_DB=/app/data/kline_cache.db \
PYTHONUNBUFFERED=1 \
python3 scripts/paper_trade_monitor.py >> /app/data/paper-trader.log 2>&1 & \
wait"
