"""
VnStock Data Connector - Using vnstock_data library
"""

import pandas as pd
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class VnstockDataConnector:
    """Connector using vnstock_data library for market data"""
    
    def __init__(self, source: str = 'VCI'):
        """
        Initialize VnStock Data connector
        
        Args:
            source: Data source ('VCI', 'TCBS', 'SSI', 'TVSI')
        """
        self.source = source
        self.available = False
        
        try:
            from vnstock_ta import DataSource
            self.DataSource = DataSource
            self.available = True
            logger.info(f"VnstockDataConnector initialized with source: {source}")
        except ImportError:
            # Silently handle missing vnstock_ta - it's optional for deployment
            self.DataSource = None
            self.available = False
            logger.debug("vnstock_ta not available - using cached data only")
        except Exception as e:
            self.DataSource = None
            self.available = False
            logger.debug(f"VnstockDataConnector initialization skipped: {e}")
    
    def get_ohlcv(self, 
                  symbol: str, 
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  interval: str = '1D') -> pd.DataFrame:
        """
        Get OHLCV data using vnstock_data
        
        Args:
            symbol: Stock symbol (e.g., 'VNM', 'HPG')
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)
            interval: Time interval ('1D', '1H', '15m', etc.)
            
        Returns:
            DataFrame with columns: open, high, low, close, volume
        """
        if not self.available:
            # Return empty DataFrame when vnstock_ta is not available
            logger.debug(f"vnstock_ta not available, returning empty data for {symbol}")
            return pd.DataFrame()
        
        try:
            # Set default dates if not provided
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            
            # Map interval to vnstock_data format
            interval_map = {
                '1m': '1m',
                '5m': '5m',
                '15m': '15m',
                '30m': '30m',
                '1H': '1H',
                '1D': '1D',
                '1W': '1W',
                '1M': '1M'
            }
            
            vnstock_interval = interval_map.get(interval, '1D')
            
            # Create DataSource and get data
            ds = self.DataSource(
                symbol=symbol.upper(),
                start=start_date,
                end=end_date,
                interval=vnstock_interval,
                source=self.source
            )
            
            df = ds.get_data()
            
            if df is not None and not df.empty:
                # Standardize column names
                column_mapping = {
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume',
                    'open': 'open',
                    'high': 'high',
                    'low': 'low',
                    'close': 'close',
                    'volume': 'volume'
                }
                
                # Rename columns
                df = df.rename(columns=column_mapping)
                
                # Ensure all required columns exist
                required_cols = ['open', 'high', 'low', 'close', 'volume']
                for col in required_cols:
                    if col not in df.columns:
                        logger.warning(f"Missing column {col} for {symbol}")
                        df[col] = 0
                
                # Select only OHLCV columns
                df = df[required_cols]
                
                # Scale prices from thousands to actual VND
                # VnStock returns prices in thousands (e.g., 75.0 = 75,000 VND)
                price_cols = ['open', 'high', 'low', 'close']
                for col in price_cols:
                    if col in df.columns:
                        df[col] = df[col] * 1000
                
                logger.info(f"Retrieved {len(df)} records for {symbol} from {self.source}")
                return df
            else:
                logger.warning(f"No data received for {symbol}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching vnstock_data for {symbol}: {e}")
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
                if not df.empty:
                    results[symbol] = df
                else:
                    logger.warning(f"Empty data for {symbol}")
                    results[symbol] = pd.DataFrame()
            except Exception as e:
                logger.error(f"Failed to get data for {symbol}: {e}")
                results[symbol] = pd.DataFrame()
        
        return results
    
    def get_intraday_data(self, 
                         symbol: str,
                         interval: str = '5m') -> pd.DataFrame:
        """
        Get intraday data for a symbol
        
        Args:
            symbol: Stock symbol
            interval: Intraday interval ('1m', '5m', '15m', '30m', '1H')
            
        Returns:
            DataFrame with intraday OHLCV data
        """
        # For intraday, get last 5 days
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        return self.get_ohlcv(symbol, start_date, end_date, interval)
    
    def get_realtime_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Get realtime quote for a symbol
        
        Args:
            symbol: Stock symbol
            
        Returns:
            Dictionary with realtime quote data
        """
        try:
            # Get latest 1-minute data
            df = self.get_intraday_data(symbol, '1m')
            
            if not df.empty:
                latest = df.iloc[-1]
                return {
                    'symbol': symbol,
                    'price': latest['close'],
                    'open': latest['open'],
                    'high': latest['high'],
                    'low': latest['low'],
                    'volume': latest['volume'],
                    'timestamp': df.index[-1] if hasattr(df.index[-1], 'strftime') else str(df.index[-1])
                }
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error getting realtime quote for {symbol}: {e}")
            return {}
    
    def test_connection(self) -> bool:
        """
        Test connection to data source
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to get data for a popular stock
            df = self.get_ohlcv('VNM', interval='1D')
            return not df.empty
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


class VnstockTechnicalAnalysis:
    """Technical Analysis using vnstock_ta library"""
    
    def __init__(self):
        """Initialize technical analysis tools"""
        self.available = False
        
        try:
            from vnstock_ta import Indicator
            self.Indicator = Indicator
            self.available = True
            logger.info("VnstockTechnicalAnalysis initialized")
        except ImportError:
            # Silently handle missing vnstock_ta - it's optional for deployment
            self.Indicator = None
            self.available = False
            logger.debug("vnstock_ta not available - technical indicators disabled")
        except Exception as e:
            self.Indicator = None
            self.available = False
            logger.debug(f"VnstockTechnicalAnalysis initialization skipped: {e}")
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate technical indicators
        
        Args:
            df: DataFrame with OHLCV data
            
        Returns:
            DataFrame with added indicator columns
        """
        if not self.available:
            logger.debug("vnstock_ta not available, returning original data")
            return df
        
        try:
            # Create indicator object
            ta = self.Indicator(df)
            
            # Calculate various indicators
            # Moving Averages
            ta.sma(length=20)  # SMA 20
            ta.sma(length=50)  # SMA 50
            ta.ema(length=9)   # EMA 9
            ta.ema(length=21)  # EMA 21
            
            # MACD
            ta.macd(fast=12, slow=26, signal=9)
            
            # RSI
            ta.rsi(length=14)
            
            # Bollinger Bands
            ta.bbands(length=20, std=2)
            
            # Volume indicators
            ta.obv()  # On Balance Volume
            
            logger.info("Technical indicators calculated successfully")
            return df
            
        except Exception as e:
            logger.error(f"Error calculating indicators: {e}")
            return df
    
    def get_signals(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get trading signals based on indicators
        
        Args:
            df: DataFrame with OHLCV and indicator data
            
        Returns:
            Dictionary with trading signals
        """
        signals = {
            'ema_cross': None,
            'macd_signal': None,
            'rsi_signal': None,
            'bb_signal': None
        }
        
        try:
            # EMA Crossover
            if 'EMA_9' in df.columns and 'EMA_21' in df.columns:
                ema9 = df['EMA_9'].iloc[-1]
                ema21 = df['EMA_21'].iloc[-1]
                ema9_prev = df['EMA_9'].iloc[-2]
                ema21_prev = df['EMA_21'].iloc[-2]
                
                if ema9 > ema21 and ema9_prev <= ema21_prev:
                    signals['ema_cross'] = 'GOLDEN_CROSS'
                elif ema9 < ema21 and ema9_prev >= ema21_prev:
                    signals['ema_cross'] = 'DEATH_CROSS'
            
            # MACD Signal
            if 'MACD' in df.columns and 'MACD_SIGNAL' in df.columns:
                macd = df['MACD'].iloc[-1]
                macd_signal = df['MACD_SIGNAL'].iloc[-1]
                
                if macd > macd_signal:
                    signals['macd_signal'] = 'BULLISH'
                else:
                    signals['macd_signal'] = 'BEARISH'
            
            # RSI Signal
            if 'RSI_14' in df.columns:
                rsi = df['RSI_14'].iloc[-1]
                
                if rsi > 70:
                    signals['rsi_signal'] = 'OVERBOUGHT'
                elif rsi < 30:
                    signals['rsi_signal'] = 'OVERSOLD'
                else:
                    signals['rsi_signal'] = 'NEUTRAL'
            
            # Bollinger Bands Signal
            if all(col in df.columns for col in ['BB_UPPER', 'BB_LOWER', 'close']):
                close = df['close'].iloc[-1]
                bb_upper = df['BB_UPPER'].iloc[-1]
                bb_lower = df['BB_LOWER'].iloc[-1]
                
                if close > bb_upper:
                    signals['bb_signal'] = 'OVERBOUGHT'
                elif close < bb_lower:
                    signals['bb_signal'] = 'OVERSOLD'
                else:
                    signals['bb_signal'] = 'NEUTRAL'
            
        except Exception as e:
            logger.error(f"Error generating signals: {e}")
        
        return signals