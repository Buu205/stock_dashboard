"""
Financial calculation utilities
"""

import pandas as pd
import numpy as np
from typing import Optional, Union, List


def calculate_returns(prices: pd.Series, period: int = 1) -> pd.Series:
    """
    Calculate returns for a price series
    
    Args:
        prices: Series of prices
        period: Period for returns (1 = daily, 5 = weekly, etc.)
        
    Returns:
        Series of returns
    """
    return prices.pct_change(period)


def calculate_moving_average(data: pd.Series, window: int) -> pd.Series:
    """
    Calculate simple moving average
    
    Args:
        data: Input data series
        window: Window size for MA
        
    Returns:
        Moving average series
    """
    return data.rolling(window=window).mean()


def calculate_rsi(prices: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate Relative Strength Index (RSI)
    
    Args:
        prices: Price series
        period: RSI period (default 14)
        
    Returns:
        RSI values
    """
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


def calculate_macd(prices: pd.Series, 
                  fast: int = 12, 
                  slow: int = 26, 
                  signal: int = 9) -> pd.DataFrame:
    """
    Calculate MACD indicator
    
    Args:
        prices: Price series
        fast: Fast EMA period
        slow: Slow EMA period
        signal: Signal EMA period
        
    Returns:
        DataFrame with MACD, signal, and histogram
    """
    ema_fast = prices.ewm(span=fast).mean()
    ema_slow = prices.ewm(span=slow).mean()
    
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal).mean()
    histogram = macd_line - signal_line
    
    return pd.DataFrame({
        'macd': macd_line,
        'signal': signal_line,
        'histogram': histogram
    })


def calculate_volatility(returns: pd.Series, window: int = 20) -> pd.Series:
    """
    Calculate rolling volatility (standard deviation)
    
    Args:
        returns: Returns series
        window: Rolling window size
        
    Returns:
        Volatility series
    """
    return returns.rolling(window=window).std() * np.sqrt(252)  # Annualized


def calculate_sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.02) -> float:
    """
    Calculate Sharpe ratio
    
    Args:
        returns: Returns series
        risk_free_rate: Risk-free rate (annual)
        
    Returns:
        Sharpe ratio
    """
    excess_returns = returns - risk_free_rate / 252  # Daily risk-free rate
    return np.sqrt(252) * excess_returns.mean() / excess_returns.std()


def calculate_max_drawdown(prices: pd.Series) -> float:
    """
    Calculate maximum drawdown
    
    Args:
        prices: Price series
        
    Returns:
        Maximum drawdown percentage
    """
    cumulative = (1 + calculate_returns(prices)).cumprod()
    running_max = cumulative.expanding().max()
    drawdown = (cumulative - running_max) / running_max
    
    return drawdown.min() * 100