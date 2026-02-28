# Use Node.js 20 LTS (better compatibility)
FROM node:20-slim

# Install build tools for native modules (better-sqlite3)
RUN apt-get update && apt-get install -y \
    python3 \
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

# Start the application
CMD node src/index.js --premium
