#!/usr/bin/env python3
"""
Daily OHLCV Update Script - Cập nhật dữ liệu giá hàng ngày
Chỉ cập nhật data mới từ ngày cuối cùng trong cache
"""

import pandas as pd
from pathlib import Path
import sys
import time
from datetime import datetime, timedelta
import sqlite3
import argparse

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from src.data.connectors.ohlcv_cache import OHLCVCacheManager
from src.data.connectors.vnstock_connector import VnstockDataConnector
from src.data.connectors.tcbs_connector import TCBSConnector


class DailyOHLCVUpdater:
    """Daily updater - only fetches new data since last update"""
    
    def __init__(self):
        self.vnstock = VnstockDataConnector(source='VCI')
        self.tcbs = TCBSConnector()
        self.cache = OHLCVCacheManager()
        
    def get_last_cached_date(self, ticker):
        """Get last date in cache for a ticker"""
        conn = sqlite3.connect('Database/cache/ohlcv_cache.db')
        cursor = conn.cursor()
        cursor.execute(
            'SELECT MAX(date) FROM ohlcv_data WHERE symbol = ?',
            (ticker,)
        )
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            return pd.to_datetime(result[0])
        return None
    
    def update_ticker(self, ticker):
        """Update data for a single ticker (only new data)"""
        # Get last cached date
        last_date = self.get_last_cached_date(ticker)
        
        if last_date:
            # Calculate days to update
            days_diff = (datetime.now() - last_date).days
            
            # Skip if already up to date (weekend/holiday)
            if days_diff <= 1:
                return None, "Already up-to-date"
            
            # Only fetch new data (last 30 days max to ensure overlap)
            start_date = (last_date - timedelta(days=5)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
        else:
            # No cache, fetch full 5 years
            start_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Try VnStock first
        try:
            df = self.vnstock.get_ohlcv(ticker, start_date, end_date)
            if not df.empty:
                # Filter only new data
                if last_date:
                    df = df[df.index > last_date]
                
                if not df.empty:
                    # Ensure timezone naive
                    if hasattr(df.index, 'tz') and df.index.tz is not None:
                        df.index = df.index.tz_localize(None)
                    
                    # Save to cache (will merge with existing data)
                    self.cache.save_ohlcv(ticker, df)
                    return len(df), "VnStock"
                else:
                    return 0, "No new data"
        except Exception as e:
            if "rate limit" in str(e).lower():
                # Try TCBS fallback
                try:
                    df = self.tcbs.get_ohlcv(ticker, start_date, end_date)
                    if not df.empty:
                        if last_date:
                            df = df[df.index > last_date]
                        
                        if not df.empty:
                            if hasattr(df.index, 'tz') and df.index.tz is not None:
                                df.index = df.index.tz_localize(None)
                            
                            self.cache.save_ohlcv(ticker, df)
                            return len(df), "TCBS"
                except:
                    pass
        
        return None, "Failed"
    
    def get_all_cached_tickers(self):
        """Get list of all tickers in cache"""
        conn = sqlite3.connect('Database/cache/ohlcv_cache.db')
        cursor = conn.cursor()
        cursor.execute('SELECT DISTINCT symbol FROM ohlcv_data ORDER BY symbol')
        tickers = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tickers


def main():
    parser = argparse.ArgumentParser(description='Daily OHLCV Update')
    parser.add_argument('--ticker', help='Update specific ticker only')
    parser.add_argument('--batch', type=int, default=50, help='Batch size (default: 50)')
    parser.add_argument('--full', action='store_true', help='Full update for all tickers')
    args = parser.parse_args()
    
    print("="*80)
    print("📊 DAILY OHLCV UPDATE")
    print("="*80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    updater = DailyOHLCVUpdater()
    
    # Get tickers to update
    if args.ticker:
        tickers = [args.ticker.upper()]
        print(f"Updating single ticker: {args.ticker}")
    else:
        tickers = updater.get_all_cached_tickers()
        print(f"Found {len(tickers)} tickers in cache")
    
    if not tickers:
        print("No tickers to update!")
        return
    
    # Track statistics
    stats = {
        'updated': 0,
        'up_to_date': 0,
        'failed': 0,
        'new_records': 0
    }
    
    # Process tickers
    for i, ticker in enumerate(tickers, 1):
        try:
            print(f"[{i:3d}/{len(tickers)}] {ticker:6s} ... ", end="", flush=True)
            
            records, source = updater.update_ticker(ticker)
            
            if records is None:
                if source == "Already up-to-date":
                    print("✅ Up-to-date")
                    stats['up_to_date'] += 1
                else:
                    print(f"❌ {source}")
                    stats['failed'] += 1
            elif records == 0:
                print("✅ No new data")
                stats['up_to_date'] += 1
            else:
                print(f"✅ {records:3d} new records [{source}]")
                stats['updated'] += 1
                stats['new_records'] += records
            
            # Rate limiting
            if i % args.batch == 0 and i < len(tickers):
                print(f"\n⏸️  Batch {i//args.batch} completed. Waiting 10s...")
                time.sleep(10)
                print("")
            else:
                time.sleep(0.5)  # Small delay between requests
                
        except KeyboardInterrupt:
            print("\n\n⚠️ Interrupted by user")
            break
        except Exception as e:
            print(f"❌ Error: {str(e)[:50]}")
            stats['failed'] += 1
    
    # Print summary
    print("\n" + "="*80)
    print("📊 UPDATE SUMMARY")
    print("="*80)
    
    # Get cache statistics
    conn = sqlite3.connect('Database/cache/ohlcv_cache.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT 
            COUNT(DISTINCT symbol) as symbols,
            COUNT(*) as total_records,
            MAX(date) as latest_date
        FROM ohlcv_data
    ''')
    cache_stats = cursor.fetchone()
    conn.close()
    
    print(f"\n📈 Results:")
    print(f"  • Updated: {stats['updated']} tickers")
    print(f"  • Already up-to-date: {stats['up_to_date']} tickers")
    print(f"  • Failed: {stats['failed']} tickers")
    print(f"  • New records added: {stats['new_records']:,}")
    
    print(f"\n💾 Cache Status:")
    print(f"  • Total symbols: {cache_stats[0]}")
    print(f"  • Total records: {cache_stats[1]:,}")
    print(f"  • Latest data: {cache_stats[2]}")
    
    print(f"\n⏱️  End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)


if __name__ == "__main__":
    main()