# Use Node.js 20 LTS (better compatibility)
FROM node:20-slim

# Install build tools for native modules (better-sqlite3) + Python + local Redis
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    make \
    g++ \
    curl \
    ca-certificates \
    redis-server \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production && pip3 install --no-cache-dir redis

# Copy application code
COPY . .

EXPOSE 3000

CMD bash -lc "mkdir -p /app/data /app/logs && \
echo '[STARTUP] Starting redis-server...' && \
redis-server --bind 127.0.0.1 --port 6379 --save '' --appendonly no --dir /app/data --logfile /app/logs/redis.log --daemonize no & \
REDIS_PID=$! && \
echo '[STARTUP] Waiting for Redis...' && \
until redis-cli -h 127.0.0.1 -p 6379 ping 2>/dev/null | grep -q PONG; do sleep 0.2; done && \
echo '[STARTUP] Redis ready.' && \
echo '[STARTUP] Starting Node.js...' && \
SENTIMENT_DB=/app/data/sentiment_arb.db \
LIFECYCLE_DB=/app/data/lifecycle_tracks.db \
KLINE_DB=/app/data/kline_cache.db \
SHADOW_MODE=false \
AUTO_BUY_ENABLED=true \
PYTHONUNBUFFERED=1 \
node src/index.js --premium > /app/logs/node.out.log 2> /app/logs/node.err.log & \
NODE_PID=$! && \
echo '[STARTUP] Starting lifecycle-tracker...' && \
SENTIMENT_DB=/app/data/sentiment_arb.db \
LIFECYCLE_DB=/app/data/lifecycle_tracks.db \
KLINE_DB=/app/data/kline_cache.db \
PYTHONUNBUFFERED=1 \
python3 scripts/lifecycle_24h_tracker.py --track >> /app/data/lifecycle.log 2>&1 & \
LIFECYCLE_PID=$! && \
echo '[STARTUP] Starting paper-trader (with auto-restart)...' && \
( while true; do \
    echo \"[paper-trader] $(date -u '+%Y-%m-%dT%H:%M:%SZ') starting\"; \
    PAPER_DB=/app/data/paper_trades.db \
    KLINE_DB=/app/data/kline_cache.db \
    SENTIMENT_DB=/app/data/sentiment_arb.db \
    PYTHONUNBUFFERED=1 \
    python3 scripts/paper_trade_monitor.py >> /app/data/paper-trader.log 2>&1; \
    EXIT_CODE=$$?; \
    echo \"[paper-trader] $(date -u '+%Y-%m-%dT%H:%M:%SZ') exited (code $EXIT_CODE), restarting in 15s\"; \
    sleep 15; \
  done ) & \
PAPER_PID=$! && \
echo \"[STARTUP] PIDs redis=$REDIS_PID node=$NODE_PID lifecycle=$LIFECYCLE_PID paper=$PAPER_PID\" && \
sleep 3 && \
kill -0 $REDIS_PID 2>/dev/null && kill -0 $NODE_PID 2>/dev/null && kill -0 $LIFECYCLE_PID 2>/dev/null && kill -0 $PAPER_PID 2>/dev/null && \
wait"
