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

CMD bash -c "mkdir -p /app/data && echo '[STARTUP] Starting Node.js...' && node src/index.js --premium & echo '[STARTUP] Starting lifecycle-tracker...' && python3 scripts/lifecycle_24h_tracker.py --track & echo '[STARTUP] Starting paper-trader...' && python3 scripts/paper_trade_monitor.py & wait"
