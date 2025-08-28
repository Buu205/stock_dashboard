"""
OHLCV Data Connector - Abstract base class and implementations
"""

from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import logging
from pathlib import Path

# Import the connectors - TCBS as primary
from .tcbs_connector import TCBSConnector as TCBSDataConnector

logger = logging.getLogger(__name__)

class OHLCVConnector(ABC):
    """Abstract base class for OHLCV data connectors"""
    
    @abstractmethod
    def get_ohlcv(self, 
                  symbol: str, 
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  resolution: str = '1D') -> pd.DataFrame:
        """
        Get OHLCV data for a symbol
        
        Args:
            symbol: Stock symbol (e.g., 'VNM', 'HPG')
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            resolution: Time resolution ('1D', '1H', '15m', etc.)
            
        Returns:
            DataFrame with columns: open, high, low, close, volume
        """
        pass
    
    @abstractmethod
    def get_batch_ohlcv(self, 
                       symbols: List[str],
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       resolution: str = '1D') -> Dict[str, pd.DataFrame]:
        """
        Get OHLCV data for multiple symbols
        
        Args:
            symbols: List of stock symbols
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            resolution: Time resolution
            
        Returns:
            Dictionary mapping symbol to DataFrame
        """
        pass
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate OHLCV data structure"""
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        return all(col in df.columns for col in required_cols)


class VnstockConnector(OHLCVConnector):
    """Connector using vnstock3 library"""
    
    def __init__(self):
        try:
            from vnstock3 import Vnstock
            self.stock = Vnstock()
            self.available = True
            logger.info("VnstockConnector initialized successfully")
        except ImportError:
            self.available = False
            logger.warning("vnstock3 not installed. Install with: pip install vnstock3")
        except Exception as e:
            self.available = False
            logger.error(f"Failed to initialize VnstockConnector: {e}")
    
    def get_ohlcv(self, 
                  symbol: str, 
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  resolution: str = '1D') -> pd.DataFrame:
        """Get OHLCV data using vnstock3"""
        if not self.available:
            raise RuntimeError("VnstockConnector not available")
        
        try:
            # Set default dates if not provided
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            
            # Map resolution to vnstock format
            resolution_map = {
                '1D': '1D',
                '1H': '1H',
                '30m': '30m',
                '15m': '15m',
                '5m': '5m',
                '1m': '1m'
            }
            
            vnstock_resolution = resolution_map.get(resolution, '1D')
            
            # Get data from vnstock
            df = self.stock.quote.history(
                symbol=symbol.upper(),
                start=start_date,
                end=end_date,
                interval=vnstock_resolution
            )
            
            # Standardize column names
            if not df.empty:
                df = df.rename(columns={
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                })
                
                # Ensure all required columns exist
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col not in df.columns:
                        df[col] = 0
                
                # Keep only OHLCV columns
                df = df[['open', 'high', 'low', 'close', 'volume']]
                
                logger.info(f"Retrieved {len(df)} records for {symbol}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_batch_ohlcv(self, 
                       symbols: List[str],
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       resolution: str = '1D') -> Dict[str, pd.DataFrame]:
        """Get OHLCV data for multiple symbols"""
        results = {}
        
        for symbol in symbols:
            try:
                df = self.get_ohlcv(symbol, start_date, end_date, resolution)
                if not df.empty:
                    results[symbol] = df
            except Exception as e:
                logger.warning(f"Failed to get data for {symbol}: {e}")
                results[symbol] = pd.DataFrame()
        
        return results


class TCBSConnector(OHLCVConnector):
    """Connector using TCBS API directly"""
    
    def __init__(self):
        import requests
        self.session = requests.Session()
        self.base_url = "https://apipubaws.tcbs.com.vn"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        self.session.headers.update(self.headers)
        logger.info("TCBSConnector initialized")
    
    def get_ohlcv(self, 
                  symbol: str, 
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  resolution: str = '1D') -> pd.DataFrame:
        """Get OHLCV data from TCBS API"""
        try:
            # Convert dates to timestamps
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            
            # Map resolution to TCBS format
            resolution_map = {
                '1D': 'D',
                '1W': 'W',
                '1M': 'M'
            }
            
            tcbs_resolution = resolution_map.get(resolution, 'D')
            
            # TCBS API endpoint
            url = f"{self.base_url}/stock-insight/v1/stock/bars-long-term"
            
            params = {
                'ticker': symbol.upper(),
                'type': tcbs_resolution,
                'from': start_date,
                'to': end_date
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                
                # Rename columns to standard format
                df = df.rename(columns={
                    'tradingDate': 'date',
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'volume': 'volume'
                })
                
                # Convert date to datetime
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                
                # Keep only OHLCV columns
                df = df[['open', 'high', 'low', 'close', 'volume']]
                
                logger.info(f"Retrieved {len(df)} records for {symbol} from TCBS")
                
                return df
            else:
                logger.warning(f"No data received for {symbol}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching TCBS data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_batch_ohlcv(self, 
                       symbols: List[str],
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       resolution: str = '1D') -> Dict[str, pd.DataFrame]:
        """Get OHLCV data for multiple symbols"""
        results = {}
        
        for symbol in symbols:
            try:
                df = self.get_ohlcv(symbol, start_date, end_date, resolution)
                if not df.empty:
                    results[symbol] = df
            except Exception as e:
                logger.warning(f"Failed to get TCBS data for {symbol}: {e}")
                results[symbol] = pd.DataFrame()
        
        return results


class HybridOHLCVConnector:
    """Hybrid connector with fallback mechanism"""
    
    def __init__(self, primary='tcbs', fallback=None, vnstock_source='VCI'):
        """
        Initialize hybrid connector
        
        Args:
            primary: Primary data source ('vnstock' or 'tcbs')
            fallback: Fallback data source
            vnstock_source: Source for vnstock_data ('VCI', 'TCBS', 'SSI', 'TVSI')
        """
        self.connectors = {}
        
        # Initialize TCBS as primary connector
        self.connectors['primary'] = TCBSDataConnector()
        
        # Optionally initialize vnstock as fallback (but it won't be used if not available)
        if fallback == 'vnstock':
            try:
                from .vnstock_connector import VnstockDataConnector
                self.connectors['fallback'] = VnstockDataConnector(source=vnstock_source)
            except Exception as e:
                logger.debug(f"Vnstock fallback not available: {e}")
                self.connectors['fallback'] = None
        else:
            self.connectors['fallback'] = None
        
        logger.info(f"HybridOHLCVConnector initialized with {primary} as primary")
    
    def get_ohlcv(self, 
                  symbol: str, 
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  resolution: str = '1D') -> pd.DataFrame:
        """Get OHLCV data with fallback mechanism"""
        
        # Try primary connector
        try:
            df = self.connectors['primary'].get_ohlcv(symbol, start_date, end_date, resolution)
            if not df.empty:
                return df
        except Exception as e:
            logger.warning(f"Primary connector failed for {symbol}: {e}")
        
        # Try fallback connector if available
        if self.connectors.get('fallback'):
            try:
                logger.info(f"Using fallback connector for {symbol}")
                df = self.connectors['fallback'].get_ohlcv(symbol, start_date, end_date, resolution)
                return df
            except Exception as e:
                logger.error(f"Both connectors failed for {symbol}: {e}")
                return pd.DataFrame()
        else:
            logger.debug(f"No fallback available for {symbol}")
            return pd.DataFrame()
    
    def get_batch_ohlcv(self, 
                       symbols: List[str],
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       resolution: str = '1D') -> Dict[str, pd.DataFrame]:
        """Get OHLCV data for multiple symbols with fallback"""
        results = {}
        
        for symbol in symbols:
            df = self.get_ohlcv(symbol, start_date, end_date, resolution)
            results[symbol] = df
        
        return results