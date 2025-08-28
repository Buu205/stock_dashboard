#!/usr/bin/env python3
"""
Script to update OHLCV data for all tickers in the database
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime, timedelta
import logging
from tqdm import tqdm
import time

# Import from same directory
from .ohlcv_connector import HybridOHLCVConnector
from .ohlcv_cache import OHLCVCacheManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OHLCVUpdater:
    """Update OHLCV data for all tickers"""
    
    def __init__(self, parquet_path: str = "Database/Full_database/Buu_clean_ver2.parquet"):
        """
        Initialize updater
        
        Args:
            parquet_path: Path to the main database
        """
        self.parquet_path = parquet_path
        self.connector = HybridOHLCVConnector(primary='vnstock')
        self.cache = OHLCVCacheManager()
        
        # Load ticker list
        self.tickers = self._load_tickers()
        logger.info(f"Loaded {len(self.tickers)} tickers from database")
    
    def _load_tickers(self) -> list:
        """Load unique tickers from parquet file"""
        try:
            df = pd.read_parquet(self.parquet_path)
            tickers = df['SECURITY_CODE'].unique().tolist()
            return sorted(tickers)
        except Exception as e:
            logger.error(f"Failed to load tickers: {e}")
            return []
    
    def update_ticker(self, 
                     symbol: str, 
                     days_back: int = 365,
                     force_update: bool = False) -> bool:
        """
        Update OHLCV data for a single ticker
        
        Args:
            symbol: Stock symbol
            days_back: Number of days to fetch
            force_update: Force update even if cache is valid
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Check cache validity
            if not force_update and self.cache.is_cache_valid(symbol, max_age_hours=24):
                logger.debug(f"Cache is valid for {symbol}, skipping update")
                return True
            
            # Calculate date range
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            # Fetch new data
            logger.info(f"Fetching data for {symbol} from {start_date} to {end_date}")
            df = self.connector.get_ohlcv(symbol, start_date, end_date, '1D')
            
            if not df.empty:
                # Save to cache
                self.cache.save_ohlcv(symbol, df, '1D')
                return True
            else:
                logger.warning(f"No data received for {symbol}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating {symbol}: {e}")
            return False
    
    def update_all(self, 
                   batch_size: int = 10,
                   delay: float = 0.5,
                   force_update: bool = False):
        """
        Update OHLCV data for all tickers
        
        Args:
            batch_size: Number of tickers to process before delay
            delay: Delay between batches (seconds)
            force_update: Force update all tickers
        """
        logger.info(f"Starting update for {len(self.tickers)} tickers")
        
        success_count = 0
        failed_tickers = []
        
        # Process tickers with progress bar
        for i, ticker in enumerate(tqdm(self.tickers, desc="Updating OHLCV")):
            if self.update_ticker(ticker, force_update=force_update):
                success_count += 1
            else:
                failed_tickers.append(ticker)
            
            # Add delay after batch
            if (i + 1) % batch_size == 0:
                time.sleep(delay)
        
        # Print summary
        logger.info(f"\n{'='*60}")
        logger.info(f"Update Summary:")
        logger.info(f"  Total tickers: {len(self.tickers)}")
        logger.info(f"  Successful: {success_count}")
        logger.info(f"  Failed: {len(failed_tickers)}")
        
        if failed_tickers:
            logger.warning(f"  Failed tickers: {failed_tickers[:10]}...")
        
        # Show cache stats
        stats = self.cache.get_cache_stats()
        logger.info(f"\nCache Statistics:")
        logger.info(f"  Cached symbols: {stats['symbol_count']}")
        logger.info(f"  Total records: {stats['total_records']:,}")
        logger.info(f"  Database size: {stats['db_size_mb']:.2f} MB")
    
    def update_selected(self, symbols: list, force_update: bool = False):
        """
        Update specific tickers
        
        Args:
            symbols: List of symbols to update
            force_update: Force update even if cache is valid
        """
        logger.info(f"Updating {len(symbols)} selected tickers")
        
        for symbol in tqdm(symbols, desc="Updating"):
            self.update_ticker(symbol, force_update=force_update)
    
    def get_ticker_data(self, symbol: str) -> pd.DataFrame:
        """
        Get OHLCV data for a ticker (from cache or fetch if needed)
        
        Args:
            symbol: Stock symbol
            
        Returns:
            DataFrame with OHLCV data
        """
        # Try cache first
        df = self.cache.get_ohlcv(symbol)
        
        if df is None or df.empty:
            # Fetch new data
            logger.info(f"No cached data for {symbol}, fetching...")
            self.update_ticker(symbol)
            df = self.cache.get_ohlcv(symbol)
        
        return df if df is not None else pd.DataFrame()


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Update OHLCV data')
    parser.add_argument('--tickers', nargs='+', help='Specific tickers to update')
    parser.add_argument('--all', action='store_true', help='Update all tickers')
    parser.add_argument('--force', action='store_true', help='Force update even if cache is valid')
    parser.add_argument('--days', type=int, default=365, help='Number of days to fetch')
    parser.add_argument('--batch-size', type=int, default=10, help='Batch size for API calls')
    
    args = parser.parse_args()
    
    # Initialize updater
    updater = OHLCVUpdater()
    
    if args.tickers:
        # Update specific tickers
        updater.update_selected(args.tickers, force_update=args.force)
    elif args.all:
        # Update all tickers
        updater.update_all(batch_size=args.batch_size, force_update=args.force)
    else:
        # Default: update top 10 tickers
        print("\n📊 OHLCV Data Updater")
        print("="*60)
        print("\nUsage examples:")
        print("  python update_ohlcv_data.py --tickers VNM HPG VIC")
        print("  python update_ohlcv_data.py --all")
        print("  python update_ohlcv_data.py --all --force")
        print("\nUpdating top 10 tickers as demo...")
        
        top_tickers = updater.tickers[:10]
        updater.update_selected(top_tickers)
    
    print("\n✅ Update completed!")


if __name__ == "__main__":
    main()