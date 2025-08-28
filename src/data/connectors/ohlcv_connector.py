"""
OHLCV Data Connector - Using TCBS API only
"""

import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime, timedelta
import logging

# Import TCBS connector only
from .tcbs_connector import TCBSConnector

logger = logging.getLogger(__name__)

class OHLCVConnector:
    """Main OHLCV data connector using TCBS API"""
    
    def __init__(self):
        """Initialize TCBS connector"""
        self.connector = TCBSConnector()
        logger.info("OHLCVConnector initialized with TCBS API")
    
    def get_ohlcv(self, 
                  symbol: str, 
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  interval: str = '1D') -> pd.DataFrame:
        """
        Get OHLCV data for a symbol
        
        Args:
            symbol: Stock symbol (e.g., 'VNM', 'HPG')
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            interval: Time interval (only '1D' supported by TCBS)
            
        Returns:
            DataFrame with columns: date, open, high, low, close, volume
        """
        try:
            # Calculate days if dates provided
            days = 365  # Default
            if start_date and end_date:
                start = datetime.strptime(start_date, '%Y-%m-%d')
                end = datetime.strptime(end_date, '%Y-%m-%d')
                days = (end - start).days
            
            # Get data from TCBS
            df = self.connector.fetch_historical_price(
                ticker=symbol,
                days=days,
                start_date=start_date,
                end_date=end_date
            )
            
            if not df.empty:
                logger.info(f"Retrieved {len(df)} records for {symbol}")
            else:
                logger.warning(f"No data available for {symbol}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_batch_ohlcv(self, 
                       symbols: List[str],
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       interval: str = '1D') -> Dict[str, pd.DataFrame]:
        """
        Get OHLCV data for multiple symbols
        
        Args:
            symbols: List of stock symbols
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            interval: Time interval
            
        Returns:
            Dictionary mapping symbol to DataFrame
        """
        results = {}
        
        for symbol in symbols:
            try:
                df = self.get_ohlcv(symbol, start_date, end_date, interval)
                results[symbol] = df
            except Exception as e:
                logger.warning(f"Failed to get data for {symbol}: {e}")
                results[symbol] = pd.DataFrame()
        
        return results
    
    def get_intraday(self, symbol: str, resolution: str = '5') -> pd.DataFrame:
        """
        Get intraday data for a symbol
        
        Args:
            symbol: Stock symbol
            resolution: Time resolution in minutes ('1', '5', '15', '30', '60')
            
        Returns:
            DataFrame with intraday data
        """
        try:
            return self.connector.fetch_intraday_price(symbol, resolution)
        except Exception as e:
            logger.error(f"Error fetching intraday data for {symbol}: {e}")
            return pd.DataFrame()
    
    def calculate_indicators(self, df: pd.DataFrame, 
                           indicators: List[str] = None) -> pd.DataFrame:
        """
        Calculate technical indicators on price data
        
        Args:
            df: DataFrame with OHLCV data
            indicators: List of indicators to calculate
            
        Returns:
            DataFrame with added indicator columns
        """
        return self.connector.calculate_technical_indicators(df, indicators)
    
    def get_price_summary(self, symbol: str, days: int = 365) -> Dict:
        """
        Get price summary statistics for a ticker
        
        Args:
            symbol: Stock ticker
            days: Number of days for analysis
            
        Returns:
            Dictionary with summary statistics
        """
        return self.connector.get_price_summary(symbol, days)

# Keep the old class name for compatibility
HybridOHLCVConnector = OHLCVConnector