#!/bin/bash
#
# Setup Cron Jobs for Sentiment Arbitrage System
#
# This script sets up automatic weekly maintenance tasks
#
# Usage:
#   chmod +x scripts/setup-cron.sh
#   ./scripts/setup-cron.sh
#

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
NODE_PATH=$(which node)
LOG_DIR="$PROJECT_DIR/logs"

# Create logs directory if not exists
mkdir -p "$LOG_DIR"

echo "🔧 Setting up cron jobs for Sentiment Arbitrage System"
echo "   Project: $PROJECT_DIR"
echo "   Node: $NODE_PATH"
echo ""

# Create the cron entries
CRON_WEEKLY="0 0 * * 0 cd $PROJECT_DIR && $NODE_PATH scripts/weekly-maintenance.js >> $LOG_DIR/weekly-maintenance.log 2>&1"

# Check if cron job already exists
EXISTING_CRON=$(crontab -l 2>/dev/null | grep "weekly-maintenance.js" || true)

if [ -n "$EXISTING_CRON" ]; then
    echo "⚠️  Weekly maintenance cron job already exists:"
    echo "   $EXISTING_CRON"
    echo ""
    read -p "Replace with new cron job? (y/n): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Cancelled"
        exit 0
    fi
    # Remove existing
    crontab -l 2>/dev/null | grep -v "weekly-maintenance.js" | crontab -
fi

# Add new cron job
(crontab -l 2>/dev/null; echo "$CRON_WEEKLY") | crontab -

echo "✅ Cron job installed!"
echo ""
echo "📅 Schedule:"
echo "   Weekly Maintenance: Every Sunday at 00:00"
echo ""
echo "📝 Tasks performed:"
echo "   1. Update narrative weights (AI assessment)"
echo "   2. Discover new KOLs and channels"
echo "   3. Auto-add high-quality channels to monitoring"
echo "   4. Clean up old data"
echo ""
echo "📄 Logs: $LOG_DIR/weekly-maintenance.log"
echo ""
echo "To view cron jobs: crontab -l"
echo "To remove: crontab -l | grep -v 'weekly-maintenance' | crontab -"
