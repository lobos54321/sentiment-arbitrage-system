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

# Install Node.js dependencies
RUN npm ci --only=production

# Install Python dependencies (--break-system-packages required on Debian Bookworm / PEP 668)
RUN pip3 install --no-cache-dir --break-system-packages redis

# Copy application code
COPY . .

EXPOSE 3000

CMD bash -c "set -e; mkdir -p /app/data /app/logs; shutdown() { echo '[SHUTDOWN] Forwarding termination signal...'; kill -TERM \$REDIS_PID \$NODE_PID \$LIFECYCLE_PID \$PAPER_PID \$SOCIAL_PID 2>/dev/null || true; wait || true; exit 0; }; trap shutdown TERM INT; echo '[STARTUP] Starting redis-server...'; redis-server --bind 127.0.0.1 --port 6379 --save '' --appendonly no --dir /app/data --logfile /app/logs/redis.log --daemonize no & REDIS_PID=\$!; echo '[STARTUP] Waiting for Redis...'; REDIS_READY=0; for i in \$(seq 1 30); do if redis-cli -h 127.0.0.1 -p 6379 ping 2>/dev/null | grep -q PONG; then REDIS_READY=1; echo '[STARTUP] Redis ready.'; break; fi; sleep 0.5; done; if [ \"\$REDIS_READY\" -ne 1 ]; then echo '[STARTUP] Redis failed to become ready in time.'; exit 1; fi; echo '[STARTUP] Starting Node.js...'; (SENTIMENT_DB=/app/data/sentiment_arb.db LIFECYCLE_DB=/app/data/lifecycle_tracks.db KLINE_DB=/app/data/kline_cache.db SHADOW_MODE=false AUTO_BUY_ENABLED=true PYTHONUNBUFFERED=1 node src/index.js --premium 2>&1 | tee -a /app/data/node.log) & NODE_PID=\$!; echo '[STARTUP] Starting lifecycle-tracker...'; (while true; do SENTIMENT_DB=/app/data/sentiment_arb.db LIFECYCLE_DB=/app/data/lifecycle_tracks.db KLINE_DB=/app/data/kline_cache.db PYTHONUNBUFFERED=1 python3 scripts/lifecycle_24h_tracker.py --track 2>&1 | tee -a /app/data/lifecycle.log; echo '[lifecycle-tracker] restarting in 15s'; sleep 15; done) & LIFECYCLE_PID=\$!; echo '[STARTUP] Starting paper-trader (with auto-restart)...'; (while true; do echo \"[paper-trader] \$(date -u '+%Y-%m-%dT%H:%M:%SZ') starting\" | tee -a /app/data/paper-trader.log; PAPER_DB=/app/data/paper_trades.db KLINE_DB=/app/data/kline_cache.db SENTIMENT_DB=/app/data/sentiment_arb.db PYTHONUNBUFFERED=1 python3 scripts/paper_trade_monitor.py 2>&1 | tee -a /app/data/paper-trader.log; EXIT_CODE=\$?; echo \"[paper-trader] \$(date -u '+%Y-%m-%dT%H:%M:%SZ') exited (code \$EXIT_CODE), restarting in 15s\" | tee -a /app/data/paper-trader.log; sleep 15; done) & PAPER_PID=\$!; echo '[STARTUP] Starting social-signal-service...'; (while true; do SOCIAL_SERVICE_PORT=8765 PYTHONUNBUFFERED=1 python3 scripts/social_signal_service.py 2>&1 | tee -a /app/data/social-service.log; echo \"[social-service] \$(date -u '+%Y-%m-%dT%H:%M:%SZ') restarting in 10s\" | tee -a /app/data/social-service.log; sleep 10; done) & SOCIAL_PID=\$!; echo \"[STARTUP] PIDs redis=\$REDIS_PID node=\$NODE_PID lifecycle=\$LIFECYCLE_PID paper=\$PAPER_PID social=\$SOCIAL_PID\"; sleep 3; kill -0 \$REDIS_PID 2>/dev/null || echo 'WARN: REDIS dead'; kill -0 \$NODE_PID 2>/dev/null || echo 'WARN: NODE dead'; kill -0 \$LIFECYCLE_PID 2>/dev/null || echo 'WARN: LIFECYCLE dead'; kill -0 \$PAPER_PID 2>/dev/null || echo 'WARN: PAPER dead'; wait"