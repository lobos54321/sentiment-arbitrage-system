# Use Node.js 20 LTS (better compatibility)
FROM node:20-slim

# Install build tools for native modules (better-sqlite3) + Python
RUN apt-get update && apt-get install && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
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

# NOTE: /app/data directory created by Zeabur persistent volume
# Do NOT use mkdir here - it interferes with volume mount

# Expose port (if needed for health checks)
EXPOSE 3000

# Create startup script that runs Node.js + Python monitors
RUN cat > /app/start.sh << 'SCRIPT'
#!/bin/bash
set -e

echo "[startup] Starting services..."

# Create data dir if not exists (Zeabur volume might be empty)
mkdir -p /app/data

# Set environment variables
export SENTIMENT_DB="/app/data/sentiment.db"
export PAPER_DB="/app/data/paper_trades.db"
export LIFECYCLE_DB="/app/data/lifecycle_tracks.db"
export KLINE_DB="/app/data/kline_cache.db"
export PYTHONUNBUFFERED=1

# Start Node.js application in background
echo "[startup] Starting Node.js trader..."
node src/index.js --premium &
NODE_PID=$!

# Give Node.js a moment to initialize
sleep 3

# Start lifecycle tracker in background
echo "[startup] Starting lifecycle-tracker..."
python3 scripts/lifecycle_24h_tracker.py --track &
LIFECYCLE_PID=$!

# Start paper trader in background
echo "[startup] Starting paper-trader..."
python3 scripts/paper_trade_monitor.py &
PAPER_PID=$!

echo "[startup] All services started"
echo "  Node.js PID: $NODE_PID"
echo "  lifecycle-tracker PID: $LIFECYCLE_PID"
echo "  paper-trader PID: $PAPER_PID"

# Wait for all processes
# If any process dies, restart it
while true; do
    sleep 30

    # Check Node.js
    if ! kill -0 $NODE_PID 2>/dev/null; then
        echo "[startup] Node.js died, restarting..."
        node src/index.js --premium &
        NODE_PID=$!
    fi

    # Check lifecycle tracker
    if ! kill -0 $LIFECYCLE_PID 2>/dev/null; then
        echo "[startup] lifecycle-tracker died, restarting..."
        python3 scripts/lifecycle_24h_tracker.py --track &
        LIFECYCLE_PID=$!
    fi

    # Check paper trader
    if ! kill -0 $PAPER_PID 2>/dev/null; then
        echo "[startup] paper-trader died, restarting..."
        python3 scripts/paper_trade_monitor.py &
        PAPER_PID=$!
    fi
done
SCRIPT

RUN chmod +x /app/start.sh

CMD ["/app/start.sh"]
