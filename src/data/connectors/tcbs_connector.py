"""
TCBS API Connector
Fetches stock price data from TCBS public API
Updated to match the standard API implementation
"""

import pandas as pd
import numpy as np
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from pathlib import Path
import pickle
import time

logger = logging.getLogger(__name__)

# Import config - using relative import
from src.core.config import get_config


class TCBSConnector:
    """
    Connector for TCBS API to fetch stock market data
    Provides historical price, volume, and technical indicators
    """
    
    BASE_URL = "https://apipubaws.tcbs.com.vn"
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize TCBS connector with configuration"""
        self.config = get_config(config_path)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        })
        self._cache = {}
        self.rate_limit_delay = 0.5  # Delay between requests in seconds
        self._last_request_time = 0
        
        logger.info("TCBS Connector initialized")
    
    def _rate_limit(self):
        """Implement rate limiting to avoid being blocked"""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self.rate_limit_delay:
            sleep_time = self.rate_limit_delay - time_since_last
            time.sleep(sleep_time)
        
        self._last_request_time = time.time()
    
    def fetch_historical_price(
        self, 
        ticker: str, 
        days: int = 365,
        start_date: str = None,
        end_date: str = None,
        resolution: str = "D",
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Fetch historical price and volume data from TCBS API
        
        Args:
            ticker: Stock ticker symbol (e.g., 'MWG', 'VNM')
            days: Number of days to fetch (default: 365)
            start_date: Optional start date in format 'YYYY-MM-DD'
            end_date: Optional end date in format 'YYYY-MM-DD' 
            resolution: Data resolution - 'D' (daily), 'W' (weekly), 'M' (monthly)
            use_cache: Whether to use cached data
            
        Returns:
            DataFrame with columns: date, open, high, low, close, volume
        """
        # Calculate timestamps
        if start_date and end_date:
            # Use provided dates
            from_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
            to_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
        else:
            # Use days parameter
            from_timestamp = int((datetime.now() - timedelta(days=days)).timestamp())
            to_timestamp = int(datetime.now().timestamp())
        
        # Check cache
        cache_key = f"{ticker}_{from_timestamp}_{to_timestamp}_{resolution}"
        if use_cache and cache_key in self._cache:
            logger.info(f"Using cached data for {ticker}")
            return self._cache[cache_key]
        
        # Rate limiting
        self._rate_limit()
        
        # API endpoint - use v2 with countBack
        url = f"{self.BASE_URL}/stock-insight/v2/stock/bars-long-term"
        
        # Parameters - v2 API requires countBack
        params = {
            "ticker": ticker.upper(),
            "type": "stock",
            "resolution": resolution,
            "from": str(from_timestamp),
            "to": str(to_timestamp),
            "countBack": days  # Required parameter for v2 API
        }
        
        try:
            logger.info(f"Fetching historical data for {ticker} ({days} days)")
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data and data['data']:
                # Convert to DataFrame
                df = pd.DataFrame(data['data'])
                
                # Process DataFrame
                df = self._process_price_data(df)
                
                # Cache the result
                if use_cache and not df.empty:
                    self._cache[cache_key] = df
                
                logger.info(f"✓ Fetched {len(df)} records for {ticker}")
                return df
            else:
                logger.warning(f"No data returned for {ticker}")
                return pd.DataFrame()
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from TCBS: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return pd.DataFrame()
    
    def _process_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and clean price data"""
        if df.empty:
            return df
            
        # Convert timestamp to datetime
        if 'tradingDate' in df.columns:
            # Check if tradingDate is already in ISO format string
            if df['tradingDate'].dtype == 'object' and len(df) > 0:
                first_value = df['tradingDate'].iloc[0]
                if isinstance(first_value, str) and 'T' in first_value:
                    df['tradingDate'] = pd.to_datetime(df['tradingDate'])
                else:
                    df['tradingDate'] = pd.to_datetime(df['tradingDate'], unit='ms')
            else:
                # Assume it's timestamp in milliseconds
                df['tradingDate'] = pd.to_datetime(df['tradingDate'], unit='ms')
        
        # Select relevant columns
        columns_to_keep = ['tradingDate', 'open', 'high', 'low', 'close', 'volume']
        df = df[[col for col in columns_to_keep if col in df.columns]]
        
        # Remove any rows with null dates
        if 'tradingDate' in df.columns:
            df = df.dropna(subset=['tradingDate'])
        
        # Sort by date
        if 'tradingDate' in df.columns:
            df = df.sort_values('tradingDate')
        
        # Reset index to ensure continuous indexing
        df = df.reset_index(drop=True)
        
        # Ensure numeric columns are numeric
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Rename tradingDate to date and set as index
        if 'tradingDate' in df.columns:
            df.rename(columns={'tradingDate': 'date'}, inplace=True)
        
        # Set date as index if available
        if 'date' in df.columns:
            df.set_index('date', inplace=True)
        
        return df
    
    def get_ohlcv(self, symbol: str, start_date: str = None, end_date: str = None, interval: str = '1D') -> pd.DataFrame:
        """
        Get OHLCV data (wrapper for compatibility with HybridOHLCVConnector)
        
        Args:
            symbol: Stock ticker
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD) 
            interval: Time interval (only '1D' supported)
            
        Returns:
            DataFrame with OHLCV data
        """
        if interval != '1D':
            logger.warning(f"TCBS only supports daily data, ignoring interval {interval}")
        
        # Calculate days from date range
        days = 365  # Default
        if start_date and end_date:
            from datetime import datetime
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            days = (end - start).days
        
        return self.fetch_historical_price(symbol, days)
    
    def fetch_intraday_price(
        self,
        ticker: str,
        resolution: str = "1"
    ) -> pd.DataFrame:
        """
        Fetch intraday price data
        
        Args:
            ticker: Stock ticker
            resolution: Time resolution in minutes ('1', '5', '15', '30', '60')
            
        Returns:
            DataFrame with intraday data
        """
        # Rate limiting
        self._rate_limit()
        
        url = f"{self.BASE_URL}/stock-insight/v1/intraday/bars"
        
        params = {
            "ticker": ticker.upper(),
            "type": "stock",
            "resolution": resolution
        }
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data:
                df = pd.DataFrame(data['data'])
                df = self._process_price_data(df)
                logger.info(f"✓ Fetched intraday data for {ticker}")
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to fetch intraday data for {ticker}: {e}")
            return pd.DataFrame()
    
    def fetch_market_overview(self) -> Dict:
        """
        Fetch market overview data (indices, market stats)
        
        Returns:
            Dictionary with market overview data
        """
        # Rate limiting
        self._rate_limit()
        
        url = f"{self.BASE_URL}/stock-insight/v1/market/overview"
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            logger.info("✓ Fetched market overview")
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch market overview: {e}")
            return {}
    
    def calculate_technical_indicators(
        self,
        df: pd.DataFrame,
        indicators: List[str] = None
    ) -> pd.DataFrame:
        """
        Calculate technical indicators on price data
        
        Args:
            df: DataFrame with OHLCV data
            indicators: List of indicators to calculate
                       ['SMA', 'EMA', 'RSI', 'MACD', 'BB']
        
        Returns:
            DataFrame with added indicator columns
        """
        if df.empty:
            return df
        
        if indicators is None:
            indicators = ['SMA', 'EMA', 'RSI']
        
        df = df.copy()
        
        # Simple Moving Average
        if 'SMA' in indicators:
            df['SMA_20'] = df['close'].rolling(window=20).mean()
            df['SMA_50'] = df['close'].rolling(window=50).mean()
            df['SMA_200'] = df['close'].rolling(window=200).mean()
        
        # Exponential Moving Average
        if 'EMA' in indicators:
            df['EMA_9'] = df['close'].ewm(span=9, adjust=False).mean()
            df['EMA_21'] = df['close'].ewm(span=21, adjust=False).mean()
            df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()
        
        # RSI (Relative Strength Index)
        if 'RSI' in indicators:
            df['RSI'] = self._calculate_rsi(df['close'])
        
        # MACD
        if 'MACD' in indicators:
            exp1 = df['close'].ewm(span=12, adjust=False).mean()
            exp2 = df['close'].ewm(span=26, adjust=False).mean()
            df['MACD'] = exp1 - exp2
            df['MACD_signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
            df['MACD_hist'] = df['MACD'] - df['MACD_signal']
        
        # Bollinger Bands
        if 'BB' in indicators:
            sma20 = df['close'].rolling(window=20).mean()
            std20 = df['close'].rolling(window=20).std()
            df['BB_upper'] = sma20 + (std20 * 2)
            df['BB_lower'] = sma20 - (std20 * 2)
            df['BB_middle'] = sma20
        
        # Volume indicators
        if 'volume' in df.columns:
            df['volume_SMA'] = df['volume'].rolling(window=20).mean()
        
        logger.info(f"✓ Calculated {len(indicators)} technical indicators")
        
        return df
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI indicator"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def fetch_multiple_tickers(
        self,
        tickers: List[str],
        days: int = 365,
        start_date: str = None,
        end_date: str = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch data for multiple tickers
        
        Args:
            tickers: List of ticker symbols
            days: Number of days to fetch
            start_date: Optional start date
            end_date: Optional end date
            
        Returns:
            Dictionary with ticker as key and DataFrame as value
        """
        result = {}
        
        for ticker in tickers:
            try:
                if start_date and end_date:
                    df = self.fetch_historical_price(
                        ticker, 
                        start_date=start_date, 
                        end_date=end_date
                    )
                else:
                    df = self.fetch_historical_price(ticker, days=days)
                    
                if not df.empty:
                    result[ticker] = df
                    logger.info(f"✓ Fetched data for {ticker}")
                else:
                    logger.warning(f"No data for {ticker}")
                    
            except Exception as e:
                logger.error(f"Failed to fetch {ticker}: {e}")
                continue
        
        logger.info(f"✓ Fetched data for {len(result)}/{len(tickers)} tickers")
        
        return result
    
    def get_price_summary(
        self,
        ticker: str,
        days: int = 365,
        start_date: str = None
    ) -> Dict:
        """
        Get price summary statistics for a ticker
        
        Args:
            ticker: Stock ticker
            days: Number of days for analysis
            start_date: Optional start date
            
        Returns:
            Dictionary with summary statistics
        """
        if start_date:
            df = self.fetch_historical_price(ticker, start_date=start_date)
        else:
            df = self.fetch_historical_price(ticker, days=days)
        
        if df.empty:
            return {}
        
        current_price = df['close'].iloc[-1]
        start_price = df['close'].iloc[0]
        
        summary = {
            'ticker': ticker,
            'current_price': current_price,
            'start_price': start_price,
            'change_pct': ((current_price - start_price) / start_price) * 100,
            'high_52w': df['high'].max(),
            'low_52w': df['low'].min(),
            'avg_volume': df['volume'].mean(),
            'volatility': df['close'].pct_change().std() * np.sqrt(252),  # Annualized
            'data_points': len(df),
            'date_range': f"{df['date'].min().date()} to {df['date'].max().date()}"
        }
        
        return summary
    
    def save_cache(self, filepath: str = None):
        """Save cache to file"""
        if filepath is None:
            filepath = self.config.get_cache_path("tcbs_cache.pkl")
        
        try:
            with open(filepath, 'wb') as f:
                pickle.dump(self._cache, f)
            logger.info(f"Cache saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
    
    def load_cache(self, filepath: str = None):
        """Load cache from file"""
        if filepath is None:
            filepath = self.config.get_cache_path("tcbs_cache.pkl")
        
        try:
            if Path(filepath).exists():
                with open(filepath, 'rb') as f:
                    self._cache = pickle.load(f)
                logger.info(f"Cache loaded from {filepath}")
        except Exception as e:
            logger.error(f"Failed to load cache: {e}")


# Convenience functions
def get_stock_price(ticker: str, days: int = 365) -> pd.DataFrame:
    """Quick function to get stock price"""
    connector = TCBSConnector()
    return connector.fetch_historical_price(ticker, days=days)


def get_market_overview() -> Dict:
    """Quick function to get market overview"""
    connector = TCBSConnector()
    return connector.fetch_market_overview()


if __name__ == "__main__":
    # Test the connector
    print("Testing TCBS Connector...")
    print("=" * 60)
    
    # Initialize connector
    connector = TCBSConnector()
    
    # Test 1: Fetch historical price for MWG
    print("\n1. Testing historical price fetch for MWG...")
    df_mwg = connector.fetch_historical_price('MWG', days=365)
    
    if not df_mwg.empty:
        print(f"✓ Fetched {len(df_mwg)} days of data")
        print("\nLast 5 days:")
        print(df_mwg[['date', 'open', 'high', 'low', 'close', 'volume']].tail())
        
        # Test 2: Calculate technical indicators
        print("\n2. Testing technical indicators...")
        df_with_indicators = connector.calculate_technical_indicators(
            df_mwg, 
            indicators=['SMA', 'EMA', 'RSI', 'MACD']
        )
        
        print("\nIndicators (last 3 days):")
        indicator_cols = ['date', 'close', 'SMA_20', 'EMA_9', 'RSI']
        available_cols = [col for col in indicator_cols if col in df_with_indicators.columns]
        print(df_with_indicators[available_cols].tail(3))
        
        # Test 3: Get price summary
        print("\n3. Testing price summary...")
        summary = connector.get_price_summary('MWG', days=365)
        
        print("\nPrice Summary for MWG:")
        for key, value in summary.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.2f}")
            else:
                print(f"  {key}: {value}")
    
    # Test 4: Fetch with different time periods
    print("\n4. Testing different time periods...")
    
    # Test 30 days
    df_30d = connector.fetch_historical_price('VNM', days=30)
    print(f"  VNM 30 days: {len(df_30d)} records")
    
    # Test 90 days
    df_90d = connector.fetch_historical_price('FPT', days=90)
    print(f"  FPT 90 days: {len(df_90d)} records")
    
    # Test with date range
    df_range = connector.fetch_historical_price(
        'MWG', 
        start_date='2024-01-01',
        end_date='2024-03-31'
    )
    print(f"  MWG Q1 2024: {len(df_range)} records")
    
    # Test 5: Multiple tickers
    print("\n5. Testing multiple tickers fetch...")
    tickers = ['MWG', 'VNM', 'FPT']
    multi_data = connector.fetch_multiple_tickers(tickers, days=30)
    
    print(f"\nFetched data for {len(multi_data)} tickers:")
    for ticker, df in multi_data.items():
        print(f"  {ticker}: {len(df)} records")
    
    print("\n" + "=" * 60)
    print("✓ All tests completed!")