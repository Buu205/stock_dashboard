#!/usr/bin/env python3
"""
Enhanced script to update remaining stocks with dual API fallback
Uses VnStock (VCI) as primary and TCBS as fallback for rate limits
"""

import pandas as pd
from pathlib import Path
import sys
import time
from datetime import datetime
import json
import sqlite3

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from src.data.connectors import OHLCVUpdater, OHLCVCacheManager
from src.data.connectors.vnstock_connector import VnstockDataConnector
from src.data.connectors.tcbs_connector import TCBSConnector


class EnhancedOHLCVUpdater:
    """Enhanced updater with dual API support"""
    
    def __init__(self):
        self.vnstock = VnstockDataConnector(source='VCI')
        self.tcbs = TCBSConnector()
        self.cache = OHLCVCacheManager()
        
    def get_ohlcv_data(self, ticker, days_back=365*5):
        """Get OHLCV data with VnStock primary, TCBS fallback"""
        from datetime import datetime, timedelta
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        # Try VnStock first
        try:
            df = self.vnstock.get_ohlcv(ticker, start_date, end_date)
            if not df.empty:
                # Ensure index is timezone naive for consistency
                if hasattr(df.index, 'tz') and df.index.tz is not None:
                    df.index = df.index.tz_localize(None)
                return df, "VnStock"
        except Exception as e:
            if "rate limit" in str(e).lower() or "quá nhiều request" in str(e).lower():
                print(f"    VnStock rate limited, switching to TCBS...")
            else:
                print(f"    VnStock error: {str(e)[:40]}")
        
        # Fallback to TCBS
        try:
            df = self.tcbs.get_ohlcv(ticker, start_date, end_date)
            if not df.empty:
                # Ensure index is timezone naive for consistency
                if hasattr(df.index, 'tz') and df.index.tz is not None:
                    df.index = df.index.tz_localize(None)
                return df, "TCBS"
        except Exception as e:
            print(f"    TCBS error: {str(e)[:40]}")
        
        return pd.DataFrame(), "Failed"
    
    def save_to_cache(self, ticker, df, resolution='1D'):
        """Save data to cache with format validation"""
        if df.empty:
            return False
            
        # Validate format
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        if not all(col in df.columns for col in required_cols):
            print(f"    Invalid format for {ticker}")
            return False
        
        # Ensure correct data types
        for col in ['open', 'high', 'low', 'close']:
            df[col] = df[col].astype(float)
        df['volume'] = df['volume'].astype(int)
        
        # Save to cache
        try:
            self.cache.save_ohlcv(ticker, df, resolution)
            return True
        except Exception as e:
            print(f"    Cache save error: {str(e)[:40]}")
            return False


def get_missing_tickers():
    """Get list of tickers not yet in cache"""
    # Get all tickers from CSV
    csv_path = Path("Database/Full_database/filtered_tickers_summary.csv")
    all_tickers = pd.read_csv(csv_path)['ticker'].tolist()
    
    # Get tickers already in cache
    conn = sqlite3.connect('Database/cache/ohlcv_cache.db')
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT symbol FROM ohlcv_data')
    cached_tickers = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    return [t for t in all_tickers if t not in cached_tickers]


def load_progress():
    """Load progress from file"""
    progress_file = Path("Database/cache/enhanced_update_progress.json")
    if progress_file.exists():
        try:
            with open(progress_file, 'r') as f:
                return json.load(f)
        except:
            return {"completed": [], "failed": [], "vnstock_count": 0, "tcbs_count": 0}
    return {"completed": [], "failed": [], "vnstock_count": 0, "tcbs_count": 0}


def save_progress(progress):
    """Save progress to file"""
    progress_file = Path("Database/cache/enhanced_update_progress.json")
    with open(progress_file, 'w') as f:
        json.dump(progress, f, indent=2)


def main():
    """Update all remaining stocks with dual API support"""
    
    print("=" * 80)
    print("ENHANCED DUAL-API STOCK UPDATE - VnStock + TCBS Fallback")
    print("=" * 80)
    print(f"Start time: {datetime.now()}\n")
    
    # Get missing tickers
    missing_tickers = get_missing_tickers()
    
    if not missing_tickers:
        print("🎉 All stocks are already cached!")
        return
    
    # Load previous progress
    progress = load_progress()
    completed = set(progress.get("completed", []))
    failed = set(progress.get("failed", []))
    vnstock_count = progress.get("vnstock_count", 0)
    tcbs_count = progress.get("tcbs_count", 0)
    
    # Filter out already processed
    remaining = [t for t in missing_tickers if t not in completed and t not in failed]
    
    print(f"Total missing from cache: {len(missing_tickers)}")
    print(f"Remaining to process: {len(remaining)}")
    print(f"Previous stats - VnStock: {vnstock_count}, TCBS: {tcbs_count}")
    print(f"Data period: 5 years (2020-2025)\n")
    
    if not remaining:
        print("✅ All remaining stocks have been processed!")
        return
    
    # Initialize enhanced updater
    updater = EnhancedOHLCVUpdater()
    
    # Track current session
    session_successful = []
    session_failed = []
    session_vnstock = 0
    session_tcbs = 0
    
    # Process stocks with intelligent batching
    batch_size = 20  # Smaller batches for better rate limit handling
    total_processed = 0
    
    for batch_start in range(0, len(remaining), batch_size):
        batch_end = min(batch_start + batch_size, len(remaining))
        batch = remaining[batch_start:batch_end]
        
        print(f"\\n📦 Processing batch {batch_start//batch_size + 1}: stocks {batch_start+1}-{batch_end}")
        print("-" * 70)
        
        vnstock_consecutive_fails = 0  # Track consecutive VnStock failures
        
        for i, ticker in enumerate(batch):
            total_processed += 1
            try:
                print(f"[{total_processed:3d}/{len(remaining)}] {ticker}...", end=" ")
                
                # Get data with fallback
                df, source = updater.get_ohlcv_data(ticker, days_back=365*5)
                
                if not df.empty:
                    # Save to cache
                    if updater.save_to_cache(ticker, df):
                        price = df.iloc[-1]['close']
                        records = len(df)
                        years = records / 250  # Approximate years
                        
                        print(f"✅ {price:,.0f} VND ({records} records, {years:.1f}y) [{source}]")
                        
                        session_successful.append(ticker)
                        completed.add(ticker)
                        
                        # Update source counters
                        if source == "VnStock":
                            session_vnstock += 1
                            vnstock_consecutive_fails = 0
                        elif source == "TCBS":
                            session_tcbs += 1
                        
                    else:
                        print(f"❌ Cache save failed")
                        session_failed.append(ticker)
                        failed.add(ticker)
                else:
                    print(f"❌ No data from both APIs")
                    session_failed.append(ticker)
                    failed.add(ticker)
                    if source == "Failed":
                        vnstock_consecutive_fails += 1
                    
            except Exception as e:
                print(f"❌ Error: {str(e)[:40]}")
                session_failed.append(ticker)
                failed.add(ticker)
                vnstock_consecutive_fails += 1
            
            # Save progress every 5 stocks
            if total_processed % 5 == 0:
                progress["completed"] = list(completed)
                progress["failed"] = list(failed)
                progress["vnstock_count"] = vnstock_count + session_vnstock
                progress["tcbs_count"] = tcbs_count + session_tcbs
                save_progress(progress)
            
            # Adaptive delay based on consecutive failures
            if vnstock_consecutive_fails >= 3:
                print(f"    💤 Multiple VnStock fails, longer delay...")
                time.sleep(2.0)
            else:
                time.sleep(0.8)  # Standard delay
        
        # Longer delay between batches
        if batch_end < len(remaining):
            delay_time = 15 if vnstock_consecutive_fails >= 3 else 8
            print(f"\\n⏱️ Batch completed. Waiting {delay_time}s before next batch...")
            time.sleep(delay_time)
    
    # Save final progress
    progress["completed"] = list(completed)
    progress["failed"] = list(failed)
    progress["vnstock_count"] = vnstock_count + session_vnstock
    progress["tcbs_count"] = tcbs_count + session_tcbs
    save_progress(progress)
    
    # Generate final report
    print("\\n" + "=" * 80)
    print("FINAL REPORT")
    print("=" * 80)
    
    # Get updated statistics
    final_missing = get_missing_tickers()
    total_vnstock = vnstock_count + session_vnstock
    total_tcbs = tcbs_count + session_tcbs
    
    conn = sqlite3.connect('Database/cache/ohlcv_cache.db')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(DISTINCT symbol), COUNT(*) FROM ohlcv_data')
    cached_symbols, total_records = cursor.fetchone()
    conn.close()
    
    print(f"\\nSession Results:")
    print(f"  Successfully updated: {len(session_successful)} stocks")
    print(f"  Failed: {len(session_failed)} stocks")
    print(f"  Via VnStock: {session_vnstock} stocks")
    print(f"  Via TCBS: {session_tcbs} stocks")
    
    print(f"\\nOverall Progress:")
    print(f"  Total stocks in CSV: 457")
    print(f"  Now cached: {cached_symbols} stocks ({(cached_symbols/457)*100:.1f}%)")
    print(f"  Still missing: {len(final_missing)} stocks")
    print(f"  Total records: {total_records:,}")
    
    print(f"\\nAPI Usage Statistics:")
    print(f"  VnStock total: {total_vnstock} stocks ({(total_vnstock/(total_vnstock+total_tcbs))*100:.1f}%)")
    print(f"  TCBS total: {total_tcbs} stocks ({(total_tcbs/(total_vnstock+total_tcbs))*100:.1f}%)")
    
    if session_failed:
        print(f"\\n❌ Failed stocks ({len(session_failed)}):")
        for ticker in session_failed[:10]:
            print(f"  - {ticker}")
        if len(session_failed) > 10:
            print(f"  ... and {len(session_failed)-10} more")
    
    if final_missing:
        print(f"\\n⚠️ Still need to update {len(final_missing)} stocks.")
        print("   Run this script again to continue.")
    else:
        print(f"\\n🎉 ALL 457 STOCKS ARE NOW CACHED!")
    
    print(f"\\nEnd time: {datetime.now()}")
    print("=" * 80)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\\n\\n⚠️ Interrupted by user. Progress has been saved.")
    except Exception as e:
        print(f"\\n\\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()