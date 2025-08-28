"""
Technical Indicator Analyzer
Calculates various technical indicators for stock analysis
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class TechnicalIndicatorAnalyzer:
    """Technical indicator analyzer for stock data"""
    
    def __init__(self):
        """Initialize technical indicator analyzer"""
        pass
    
    def calculate_moving_averages(
        self, 
        data: pd.DataFrame, 
        periods: List[int] = [9, 20, 50, 100]
    ) -> pd.DataFrame:
        """
        Calculate Simple Moving Averages (SMA)
        
        Args:
            data: DataFrame with OHLCV data
            periods: List of periods for MA calculation
            
        Returns:
            DataFrame with MA columns added
        """
        if data.empty:
            return data
        
        result = data.copy()
        
        for period in periods:
            if len(data) >= period:
                result[f'MA_{period}'] = data['close'].rolling(window=period).mean()
            else:
                logger.warning(f"Data length ({len(data)}) < MA period ({period})")
                result[f'MA_{period}'] = np.nan
        
        return result
    
    def calculate_exponential_moving_averages(
        self, 
        data: pd.DataFrame, 
        periods: List[int] = [9, 21]
    ) -> pd.DataFrame:
        """
        Calculate Exponential Moving Averages (EMA)
        
        Args:
            data: DataFrame with OHLCV data
            periods: List of periods for EMA calculation
            
        Returns:
            DataFrame with EMA columns added
        """
        if data.empty:
            return data
        
        result = data.copy()
        
        for period in periods:
            if len(data) >= period:
                result[f'EMA_{period}'] = data['close'].ewm(span=period).mean()
            else:
                logger.warning(f"Data length ({len(data)}) < EMA period ({period})")
                result[f'EMA_{period}'] = np.nan
        
        return result
    
    def calculate_rsi(
        self, 
        data: pd.DataFrame, 
        period: int = 14
    ) -> pd.DataFrame:
        """
        Calculate Relative Strength Index (RSI)
        
        Args:
            data: DataFrame with OHLCV data
            period: RSI period (default: 14)
            
        Returns:
            DataFrame with RSI column added
        """
        if data.empty or len(data) < period + 1:
            logger.warning(f"Insufficient data for RSI calculation. Need at least {period + 1} data points")
            return data
        
        result = data.copy()
        
        # Calculate price changes
        delta = data['close'].diff()
        
        # Separate gains and losses
        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0, 0)
        
        # Calculate average gains and losses
        avg_gains = gains.rolling(window=period).mean()
        avg_losses = losses.rolling(window=period).mean()
        
        # Calculate RS and RSI
        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))
        
        result['RSI'] = rsi
        
        return result
    
    def calculate_macd(
        self, 
        data: pd.DataFrame, 
        fast_period: int = 12, 
        slow_period: int = 26, 
        signal_period: int = 9
    ) -> pd.DataFrame:
        """
        Calculate MACD (Moving Average Convergence Divergence)
        
        Args:
            data: DataFrame with OHLCV data
            fast_period: Fast EMA period
            slow_period: Slow EMA period
            signal_period: Signal line period
            
        Returns:
            DataFrame with MACD columns added
        """
        if data.empty or len(data) < slow_period:
            logger.warning(f"Insufficient data for MACD calculation. Need at least {slow_period} data points")
            return data
        
        result = data.copy()
        
        # Calculate fast and slow EMAs
        ema_fast = data['close'].ewm(span=fast_period).mean()
        ema_slow = data['close'].ewm(span=slow_period).mean()
        
        # Calculate MACD line
        macd_line = ema_fast - ema_slow
        
        # Calculate signal line
        signal_line = macd_line.ewm(span=signal_period).mean()
        
        # Calculate histogram
        histogram = macd_line - signal_line
        
        result['MACD'] = macd_line
        result['MACD_Signal'] = signal_line
        result['MACD_Histogram'] = histogram
        
        return result
    
    def calculate_bollinger_bands(
        self, 
        data: pd.DataFrame, 
        period: int = 20, 
        std_dev: float = 2.0
    ) -> pd.DataFrame:
        """
        Calculate Bollinger Bands
        
        Args:
            data: DataFrame with OHLCV data
            period: Period for MA calculation
            std_dev: Standard deviation multiplier
            
        Returns:
            DataFrame with Bollinger Bands columns added
        """
        if data.empty or len(data) < period:
            logger.warning(f"Insufficient data for Bollinger Bands. Need at least {period} data points")
            return data
        
        result = data.copy()
        
        # Calculate middle band (SMA)
        middle_band = data['close'].rolling(window=period).mean()
        
        # Calculate standard deviation
        std = data['close'].rolling(window=period).std()
        
        # Calculate upper and lower bands
        upper_band = middle_band + (std * std_dev)
        lower_band = middle_band - (std * std_dev)
        
        result['BB_Upper'] = upper_band
        result['BB_Middle'] = middle_band
        result['BB_Lower'] = lower_band
        result['BB_Width'] = upper_band - lower_band
        result['BB_Position'] = (data['close'] - lower_band) / (upper_band - lower_band)
        
        return result
    
    def calculate_support_resistance(
        self, 
        data: pd.DataFrame, 
        window: int = 20
    ) -> pd.DataFrame:
        """
        Calculate support and resistance levels
        
        Args:
            data: DataFrame with OHLCV data
            window: Window for local min/max calculation
            
        Returns:
            DataFrame with support/resistance columns added
        """
        if data.empty or len(data) < window:
            logger.warning(f"Insufficient data for support/resistance calculation. Need at least {window} data points")
            return data
        
        result = data.copy()
        
        # Calculate local highs and lows
        result['Local_High'] = data['high'].rolling(window=window, center=True).max()
        result['Local_Low'] = data['low'].rolling(window=window, center=True).min()
        
        # Identify support and resistance levels
        result['Resistance'] = np.where(
            (data['high'] == result['Local_High']) & (data['high'] > data['high'].shift(1)) & (data['high'] > data['high'].shift(-1)),
            data['high'],
            np.nan
        )
        
        result['Support'] = np.where(
            (data['low'] == result['Local_Low']) & (data['low'] < data['low'].shift(1)) & (data['low'] < data['low'].shift(-1)),
            data['low'],
            np.nan
        )
        
        return result
    
    def calculate_volume_indicators(
        self, 
        data: pd.DataFrame, 
        period: int = 20
    ) -> pd.DataFrame:
        """
        Calculate volume-based indicators
        
        Args:
            data: DataFrame with OHLCV data
            period: Period for volume calculations
            
        Returns:
            DataFrame with volume indicator columns added
        """
        if data.empty or len(data) < period:
            logger.warning(f"Insufficient data for volume indicators. Need at least {period} data points")
            return data
        
        result = data.copy()
        
        # Volume SMA
        result['Volume_SMA'] = data['volume'].rolling(window=period).mean()
        
        # Volume ratio
        result['Volume_Ratio'] = data['volume'] / result['Volume_SMA']
        
        # On-Balance Volume (OBV)
        obv = np.zeros(len(data))
        for i in range(1, len(data)):
            if data['close'].iloc[i] > data['close'].iloc[i-1]:
                obv[i] = obv[i-1] + data['volume'].iloc[i]
            elif data['close'].iloc[i] < data['close'].iloc[i-1]:
                obv[i] = obv[i-1] - data['volume'].iloc[i]
            else:
                obv[i] = obv[i-1]
        
        result['OBV'] = obv
        
        return result
    
    def get_ma_trend_signals(
        self, 
        data: pd.DataFrame
    ) -> Dict[str, str]:
        """
        Get MA trend signals
        
        Args:
            data: DataFrame with MA columns
            
        Returns:
            Dictionary with trend signals
        """
        if data.empty:
            return {}
        
        signals = {}
        
        # Check if required columns exist
        required_columns = ['EMA_9', 'EMA_21', 'MA_50', 'MA_100']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            logger.warning(f"Missing required MA columns: {missing_columns}")
            return {}
        
        # Get latest values
        latest = data.iloc[-1]
        
        # EMA crossover signal
        if 'EMA_9' in data.columns and 'EMA_21' in data.columns:
            if latest['EMA_9'] > latest['EMA_21']:
                signals['EMA_Trend'] = 'Bullish (EMA9 > EMA21)'
            else:
                signals['EMA_Trend'] = 'Bearish (EMA9 < EMA21)'
        
        # MA support levels
        if 'MA_50' in data.columns and 'MA_100' in data.columns:
            if latest['close'] > latest['MA_50'] > latest['MA_100']:
                signals['MA_Support'] = 'Strong (Price > MA50 > MA100)'
            elif latest['close'] > latest['MA_50']:
                signals['MA_Support'] = 'Moderate (Price > MA50)'
            else:
                signals['MA_Support'] = 'Weak (Price < MA50)'
        
        return signals
    
    def calculate_all_indicators(
        self, 
        data: pd.DataFrame,
        ma_periods: List[int] = [9, 20, 50, 100],
        ema_periods: List[int] = [9, 21],
        rsi_period: int = 14
    ) -> pd.DataFrame:
        """
        Calculate all technical indicators
        
        Args:
            data: DataFrame with OHLCV data
            ma_periods: Periods for MA calculation
            ema_periods: Periods for EMA calculation
            rsi_period: Period for RSI calculation
            
        Returns:
            DataFrame with all indicators added
        """
        if data.empty:
            return data
        
        result = data.copy()
        
        # Calculate all indicators
        result = self.calculate_moving_averages(result, ma_periods)
        result = self.calculate_exponential_moving_averages(result, ema_periods)
        result = self.calculate_rsi(result, rsi_period)
        result = self.calculate_macd(result)
        result = self.calculate_bollinger_bands(result)
        result = self.calculate_support_resistance(result)
        result = self.calculate_volume_indicators(result)
        
        return result


# Utility functions
def calculate_indicators_for_stock(
    data: pd.DataFrame,
    include_all: bool = True
) -> pd.DataFrame:
    """
    Calculate indicators for a single stock
    
    Args:
        data: DataFrame with OHLCV data
        include_all: Whether to calculate all indicators
        
    Returns:
        DataFrame with indicators added
    """
    analyzer = TechnicalIndicatorAnalyzer()
    
    if include_all:
        return analyzer.calculate_all_indicators(data)
    else:
        # Basic indicators only
        result = analyzer.calculate_moving_averages(data, [20, 50])
        result = analyzer.calculate_exponential_moving_averages(result, [9, 21])
        result = analyzer.calculate_rsi(result)
        return result


def get_trend_analysis(data: pd.DataFrame) -> Dict[str, any]:
    """
    Get comprehensive trend analysis
    
    Args:
        data: DataFrame with indicators
        
    Returns:
        Dictionary with trend analysis
    """
    analyzer = TechnicalIndicatorAnalyzer()
    
    # Calculate basic indicators first
    data_with_indicators = analyzer.calculate_moving_averages(data, [9, 20, 50, 100])
    data_with_indicators = analyzer.calculate_exponential_moving_averages(data_with_indicators, [9, 21])
    
    # Get trend signals
    trend_signals = analyzer.get_ma_trend_signals(data_with_indicators)
    
    # Get latest price vs MA positions
    if not data_with_indicators.empty:
        latest = data_with_indicators.iloc[-1]
        current_price = latest['close']
        
        trend_signals.update({
            'current_price': current_price,
            'price_vs_ma20': 'Above' if current_price > latest.get('MA_20', 0) else 'Below',
            'price_vs_ma50': 'Above' if current_price > latest.get('MA_50', 0) else 'Below',
            'price_vs_ma100': 'Above' if current_price > latest.get('MA_100', 0) else 'Below'
        })
    
    return trend_signals

