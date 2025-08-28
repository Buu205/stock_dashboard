#!/bin/bash
#
# Auto-restart script for updating all 457 stocks
# This will keep running the update script until all stocks are cached
#

echo "=========================================="
echo "AUTO UPDATE ALL STOCKS WITH RESTART"
echo "=========================================="
echo "Start time: $(date)"
echo ""

MAX_ATTEMPTS=20
ATTEMPT=1

while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    echo ""
    echo "=== Attempt $ATTEMPT/$MAX_ATTEMPTS ==="
    
    # Check current progress
    python3 check_cache_progress.py
    echo ""
    
    # Run the update script
    python3 update_remaining_enhanced.py
    
    # Check exit code
    if [ $? -eq 0 ]; then
        # Check if all stocks are cached
        MISSING=$(python3 -c "
import sqlite3
import pandas as pd
conn = sqlite3.connect('Database/cache/ohlcv_cache.db')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(DISTINCT symbol) FROM ohlcv_data')
cached = cursor.fetchone()[0]
conn.close()
csv_df = pd.read_csv('Database/Full_database/filtered_tickers_summary.csv')
total = len(csv_df)
print(total - cached)
")
        
        if [ "$MISSING" -eq "0" ]; then
            echo ""
            echo "🎉 SUCCESS! All 457 stocks are now cached!"
            break
        fi
    fi
    
    # Wait before next attempt
    echo ""
    echo "⏳ Waiting 30 seconds before next attempt..."
    sleep 30
    
    ATTEMPT=$((ATTEMPT + 1))
done

echo ""
echo "=========================================="
echo "FINAL CACHE STATUS"
echo "=========================================="
python3 check_cache_progress.py
echo ""
echo "End time: $(date)"
echo "=========================================="