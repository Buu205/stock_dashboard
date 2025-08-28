#!/usr/bin/env python3
"""
Quick check of cache progress
"""

import pandas as pd
import sqlite3
from pathlib import Path

def main():
    # Get total tickers
    csv_path = Path("Database/Full_database/filtered_tickers_summary.csv")
    total_tickers = len(pd.read_csv(csv_path))
    
    # Get cached count
    conn = sqlite3.connect('Database/cache/ohlcv_cache.db')
    cursor = conn.cursor()
    
    cursor.execute('SELECT COUNT(DISTINCT symbol) FROM ohlcv_data')
    cached_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM ohlcv_data')
    total_records = cursor.fetchone()[0]
    
    # Get file size
    import os
    file_size = os.path.getsize('Database/cache/ohlcv_cache.db') / 1024 / 1024
    
    conn.close()
    
    # Calculate progress
    progress = (cached_count / total_tickers) * 100
    missing = total_tickers - cached_count
    
    print("📊 CACHE PROGRESS REPORT")
    print("=" * 40)
    print(f"Cached stocks: {cached_count:3d} / {total_tickers} ({progress:.1f}%)")
    print(f"Missing stocks: {missing:3d}")
    print(f"Total records: {total_records:,}")
    print(f"Database size: {file_size:.1f} MB")
    
    # Progress bar
    filled = int(progress // 2.5)  # Scale to 40 chars
    bar = "█" * filled + "░" * (40 - filled)
    print(f"Progress: [{bar}] {progress:.1f}%")

if __name__ == "__main__":
    main()