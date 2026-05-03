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

# Install GMGN read-only CLI for paper-trader enrichment/policy.
RUN npm install -g gmgn-cli

# Install Python dependencies (--break-system-packages required on Debian Bookworm / PEP 668)
RUN pip3 install --no-cache-dir --break-system-packages redis

# Copy application code
COPY . .

EXPOSE 3000

CMD ["bash", "scripts/run_zeabur_services.sh"]
