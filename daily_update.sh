#!/bin/bash

# Daily Update Script for Stock Dashboard
# Run this script daily to update OHLCV data

echo "======================================================================"
echo "📊 STOCK DASHBOARD DAILY UPDATE"
echo "======================================================================"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# Change to script directory
cd "$(dirname "$0")"

# Activate virtual environment if exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Update OHLCV data
echo "📈 Updating OHLCV data..."
echo "----------------------------------------------------------------------"
python3 update_daily_ohlcv.py

# Optional: Update only specific high-priority stocks more frequently
# echo ""
# echo "🔥 Updating priority stocks..."
# echo "----------------------------------------------------------------------"
# python3 update_daily_ohlcv.py --ticker VNM
# python3 update_daily_ohlcv.py --ticker VIC
# python3 update_daily_ohlcv.py --ticker VHM
# python3 update_daily_ohlcv.py --ticker MWG

echo ""
echo "✅ Daily update completed!"
echo "======================================================================"