# Use Node.js 20 LTS (better compatibility)
FROM node:20-slim

# Install system dependencies for Chromium and Telegram
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    xvfb \
    fonts-liberation \
    fonts-noto-cjk \
    libnss3 \
    libxss1 \
    libasound2 \
    libappindicator3-1 \
    xdg-utils \
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

# Create data directory for SQLite
RUN mkdir -p /app/data

# Set environment variables for Chromium
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium \
    DISPLAY=:99

# Expose port (if needed for health checks)
EXPOSE 3000

# Start Xvfb and the application
CMD Xvfb :99 -screen 0 1024x768x16 & npm start
