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
    procps \
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

# Create startup script
RUN printf '#!/bin/bash\n\
echo "[STARTUP] Initializing..."\n\
mkdir -p /app/data\n\
cd /app\n\
echo "[STARTUP] Starting Node.js..."\n\
node src/index.js --premium &\n\
NODE_PID=$!\n\
sleep 2\n\
echo "[STARTUP] Node.js PID: $NODE_PID"\n\
echo "[STARTUP] Starting lifecycle-tracker..."\n\
python3 scripts/lifecycle_24h_tracker.py --track >> /app/data/lifecycle.log 2>&1 &\n\
echo "[STARTUP] Starting paper-trader..."\n\
python3 scripts/paper_trade_monitor.py >> /app/data/paper-trader.log 2>&1 &\n\
echo "[STARTUP] All started. PID: $NODE_PID"\n\
wait $NODE_PID\n' > /app/start.sh && chmod +x /app/start.sh

CMD ["/app/start.sh"]
