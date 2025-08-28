"""
Market Data Loader using vnstock_data
Handles OHLCV data loading, caching, and real-time updates
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timedelta
import logging
from functools import lru_cache
import time

try:
    from vnstock_data import get_stock_data
    VNSTOCK_AVAILABLE = True
except ImportError:
    VNSTOCK_AVAILABLE = False
    logging.warning("vnstock_data not available. Install with: pip install vnstock_data")

logger = logging.getLogger(__name__)


class MarketDataLoader:
    """Market data loader for OHLCV data using vnstock_data"""
    
    def __init__(self, cache_ttl: int = 300):
        """
        Initialize market data loader
        
        Args:
            cache_ttl: Cache time-to-live in seconds (default: 5 minutes)
        """
        self.cache_ttl = cache_ttl
        self._cache = {}
        self._cache_timestamps = {}
        
        if not VNSTOCK_AVAILABLE:
            raise ImportError("vnstock_data is required. Install with: pip install vnstock_data")
    
    def get_stock_ohlcv(
        self, 
        symbol: str, 
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "1D"
    ) -> pd.DataFrame:
        """
        Get OHLCV data for a specific stock
        
        Args:
            symbol: Stock symbol (e.g., 'HPG', 'VNM')
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            period: Data period ('1D', '1W', '1M')
            
        Returns:
            DataFrame with columns: [date, open, high, low, close, volume]
        """
        try:
            # Check cache first
            cache_key = f"{symbol}_{start_date}_{end_date}_{period}"
            if self._is_cache_valid(cache_key):
                logger.info(f"Using cached data for {symbol}")
                return self._cache[cache_key].copy()
            
            # Get fresh data
            logger.info(f"Fetching fresh data for {symbol}")
            data = get_stock_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                period=period
            )
            
            if data is None or data.empty:
                logger.warning(f"No data returned for {symbol}")
                return pd.DataFrame()
            
            # Standardize column names
            data = self._standardize_columns(data)
            
            # Cache the data
            self._cache_data(cache_key, data)
            
            return data.copy()
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return pd.DataFrame()
    
    def get_multiple_stocks_ohlcv(
        self, 
        symbols: List[str], 
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        period: str = "1D"
    ) -> Dict[str, pd.DataFrame]:
        """
        Get OHLCV data for multiple stocks
        
        Args:
            symbols: List of stock symbols
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            period: Data period
            
        Returns:
            Dictionary with symbol as key and DataFrame as value
        """
        results = {}
        
        for symbol in symbols:
            try:
                data = self.get_stock_ohlcv(symbol, start_date, end_date, period)
                if not data.empty:
                    results[symbol] = data
                else:
                    logger.warning(f"Empty data for {symbol}")
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
                continue
        
        return results
    
    def get_realtime_price(self, symbols: List[str]) -> pd.DataFrame:
        """
        Get real-time prices for multiple symbols
        
        Args:
            symbols: List of stock symbols
            
        Returns:
            DataFrame with current prices
        """
        try:
            # For real-time, we'll get the latest available data
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
            
            realtime_data = {}
            
            for symbol in symbols:
                data = self.get_stock_ohlcv(symbol, start_date, end_date, "1D")
                if not data.empty:
                    latest = data.iloc[-1]
                    realtime_data[symbol] = {
                        'symbol': symbol,
                        'close': latest['close'],
                        'change': latest['close'] - data.iloc[-2]['close'] if len(data) > 1 else 0,
                        'change_pct': ((latest['close'] - data.iloc[-2]['close']) / data.iloc[-2]['close'] * 100) if len(data) > 1 else 0,
                        'volume': latest['volume'],
                        'timestamp': latest['date']
                    }
            
            return pd.DataFrame(realtime_data.values())
            
        except Exception as e:
            logger.error(f"Error getting real-time prices: {str(e)}")
            return pd.DataFrame()
    
    def _standardize_columns(self, data: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names to our format"""
        column_mapping = {
            'Date': 'date',
            'Open': 'open',
            'High': 'high', 
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
            'date': 'date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        }
        
        # Rename columns if needed
        data = data.rename(columns=column_mapping)
        
        # Ensure required columns exist
        required_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return pd.DataFrame()
        
        # Convert date column to datetime
        data['date'] = pd.to_datetime(data['date'])
        
        # Sort by date
        data = data.sort_values('date').reset_index(drop=True)
        
        return data
    
    def _cache_data(self, key: str, data: pd.DataFrame):
        """Cache data with timestamp"""
        self._cache[key] = data.copy()
        self._cache_timestamps[key] = time.time()
    
    def _is_cache_valid(self, key: str) -> bool:
        """Check if cached data is still valid"""
        if key not in self._cache or key not in self._cache_timestamps:
            return False
        
        cache_age = time.time() - self._cache_timestamps[key]
        return cache_age < self.cache_ttl
    
    def clear_cache(self):
        """Clear all cached data"""
        self._cache.clear()
        self._cache_timestamps.clear()
        logger.info("Cache cleared")
    
    def get_cache_info(self) -> Dict[str, int]:
        """Get cache statistics"""
        return {
            'cached_items': len(self._cache),
            'cache_size_mb': sum(df.memory_usage(deep=True).sum() for df in self._cache.values()) / 1024 / 1024
        }


# Utility functions
def get_stock_data_simple(symbol: str, days: int = 365) -> pd.DataFrame:
    """
    Simple function to get stock data
    
    Args:
        symbol: Stock symbol
        days: Number of days to look back
        
    Returns:
        DataFrame with OHLCV data
    """
    loader = MarketDataLoader()
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    return loader.get_stock_ohlcv(symbol, start_date, end_date)


def get_market_overview(symbols: List[str], days: int = 30) -> pd.DataFrame:
    """
    Get market overview for multiple symbols
    
    Args:
        symbols: List of stock symbols
        days: Number of days to look back
        
    Returns:
        DataFrame with market overview
    """
    loader = MarketDataLoader()
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    
    return loader.get_multiple_stocks_ohlcv(symbols, start_date, end_date)