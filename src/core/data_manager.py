"""
Data Manager - Central orchestration for data loading and caching
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List
import pickle
from datetime import datetime, timedelta
import os

from .config import get_config
from .exceptions import DataLoadError, ConfigurationError


class DataManager:
    """Central data management and orchestration"""
    
    def __init__(self, config=None):
        """
        Initialize data manager
        
        Args:
            config: Configuration object
        """
        self.config = config or get_config()
        self._cache = {}
        self._cache_timestamps = {}
        
        # Get paths from config
        self.parquet_path = self.config.paths.parquet_path
        self.metadata_path = self.config.paths.metadata_path
        self.cache_dir = self.config.paths.cache_dir
        
        # Create cache directory if it doesn't exist
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Cache TTL from config
        self.cache_ttl = self.config.data.cache_ttl
        
        # Load main data
        self._main_data = None
        self._metadata = None
    
    def get_main_data(self) -> pd.DataFrame:
        """
        Get main financial data from parquet file
        
        Returns:
            DataFrame with financial data
        """
        if self._main_data is None:
            try:
                if self.parquet_path and Path(self.parquet_path).exists():
                    self._main_data = pd.read_parquet(self.parquet_path)
                else:
                    # Try relative path
                    relative_path = Path("Database/Full_database/Buu_clean_ver2.parquet")
                    if relative_path.exists():
                        self._main_data = pd.read_parquet(relative_path)
                    else:
                        raise DataLoadError(f"Data file not found: {self.parquet_path}")
            except Exception as e:
                raise DataLoadError(f"Failed to load main data: {str(e)}")
        
        return self._main_data
    
    def get_metadata(self) -> pd.DataFrame:
        """
        Get metadata from Excel file
        
        Returns:
            DataFrame with metadata
        """
        if self._metadata is None:
            try:
                if self.metadata_path and Path(self.metadata_path).exists():
                    self._metadata = pd.read_excel(self.metadata_path)
                else:
                    # Try relative path
                    relative_path = Path("Database/Full_database/CSDL.xlsx")
                    if relative_path.exists():
                        self._metadata = pd.read_excel(relative_path)
                    else:
                        # Return empty metadata if not found
                        self._metadata = pd.DataFrame()
            except Exception as e:
                print(f"Warning: Could not load metadata: {str(e)}")
                self._metadata = pd.DataFrame()
        
        return self._metadata
    
    def get_ticker_data(self, ticker: str, 
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None) -> pd.DataFrame:
        """
        Get data for a specific ticker
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date for filtering
            end_date: End date for filtering
            
        Returns:
            Filtered DataFrame for the ticker
        """
        df = self.get_main_data()
        
        # Filter by ticker
        if 'ticker' in df.columns:
            ticker_data = df[df['ticker'] == ticker.upper()].copy()
        elif 'symbol' in df.columns:
            ticker_data = df[df['symbol'] == ticker.upper()].copy()
        elif 'SECURITY_CODE' in df.columns:
            ticker_data = df[df['SECURITY_CODE'] == ticker.upper()].copy()
        else:
            raise DataLoadError("No ticker/symbol/SECURITY_CODE column found in data")
        
        # Filter by date if provided
        if 'date' in ticker_data.columns:
            ticker_data['date'] = pd.to_datetime(ticker_data['date'])
            date_col = 'date'
        elif 'REPORT_DATE' in ticker_data.columns:
            ticker_data['REPORT_DATE'] = pd.to_datetime(ticker_data['REPORT_DATE'])
            date_col = 'REPORT_DATE'
        else:
            date_col = None
            
        if date_col:
            if start_date:
                ticker_data = ticker_data[ticker_data[date_col] >= start_date]
            if end_date:
                ticker_data = ticker_data[ticker_data[date_col] <= end_date]
            
            ticker_data = ticker_data.sort_values(date_col)
        
        return ticker_data
    
    def get_available_tickers(self) -> List[str]:
        """
        Get list of available tickers in the dataset
        
        Returns:
            List of ticker symbols
        """
        df = self.get_main_data()
        
        if 'ticker' in df.columns:
            return sorted(df['ticker'].unique().tolist())
        elif 'symbol' in df.columns:
            return sorted(df['symbol'].unique().tolist())
        else:
            return []
    
    def get_metric_value(self, ticker: str, metric_code: str, 
                        period: Optional[str] = None) -> Optional[float]:
        """
        Get specific metric value for a ticker
        
        Args:
            ticker: Stock ticker
            metric_code: Metric code (e.g., 'CIS_20' for revenue)
            period: Optional period filter
            
        Returns:
            Metric value or None if not found
        """
        ticker_data = self.get_ticker_data(ticker)
        
        if metric_code in ticker_data.columns:
            if period and 'period' in ticker_data.columns:
                filtered = ticker_data[ticker_data['period'] == period]
                if not filtered.empty:
                    return filtered[metric_code].iloc[-1]
            elif not ticker_data.empty:
                return ticker_data[metric_code].iloc[-1]
        
        return None
    
    def cache_data(self, key: str, data: Any) -> None:
        """
        Cache data with TTL
        
        Args:
            key: Cache key
            data: Data to cache
        """
        self._cache[key] = data
        self._cache_timestamps[key] = datetime.now()
        
        # Also save to disk cache
        cache_file = self.cache_dir / f"{key}.pkl"
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            print(f"Warning: Could not save cache to disk: {e}")
    
    def get_cached_data(self, key: str) -> Optional[Any]:
        """
        Get cached data if not expired
        
        Args:
            key: Cache key
            
        Returns:
            Cached data or None if expired/not found
        """
        # Check memory cache first
        if key in self._cache:
            timestamp = self._cache_timestamps.get(key)
            if timestamp and (datetime.now() - timestamp).seconds < self.cache_ttl:
                return self._cache[key]
        
        # Try disk cache
        cache_file = self.cache_dir / f"{key}.pkl"
        if cache_file.exists():
            try:
                # Check file modification time
                file_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
                if file_age.seconds < self.cache_ttl:
                    with open(cache_file, 'rb') as f:
                        data = pickle.load(f)
                        # Update memory cache
                        self._cache[key] = data
                        self._cache_timestamps[key] = datetime.fromtimestamp(cache_file.stat().st_mtime)
                        return data
            except Exception as e:
                print(f"Warning: Could not load cache from disk: {e}")
        
        return None
    
    def clear_cache(self, key: Optional[str] = None) -> None:
        """
        Clear cache
        
        Args:
            key: Optional specific key to clear, otherwise clear all
        """
        if key:
            self._cache.pop(key, None)
            self._cache_timestamps.pop(key, None)
            
            cache_file = self.cache_dir / f"{key}.pkl"
            if cache_file.exists():
                cache_file.unlink()
        else:
            self._cache.clear()
            self._cache_timestamps.clear()
            
            # Clear disk cache
            for cache_file in self.cache_dir.glob("*.pkl"):
                cache_file.unlink()
    
    def get_industry_data(self, industry: str) -> pd.DataFrame:
        """
        Get data for all companies in an industry
        
        Args:
            industry: Industry name or code
            
        Returns:
            DataFrame with industry data
        """
        df = self.get_main_data()
        
        if 'industry' in df.columns:
            return df[df['industry'] == industry].copy()
        elif 'sector' in df.columns:
            return df[df['sector'] == industry].copy()
        else:
            return pd.DataFrame()