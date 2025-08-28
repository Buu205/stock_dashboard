"""
Tests for calculation utilities
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent.parent / "src"
sys.path.insert(0, str(src_path))

from utils.calculations import (
    calculate_returns,
    calculate_moving_average,
    calculate_rsi,
    calculate_macd,
    calculate_volatility,
    calculate_sharpe_ratio,
    calculate_max_drawdown
)


class TestCalculations:
    """Test calculation functions"""
    
    def test_calculate_returns(self):
        """Test return calculation"""
        prices = pd.Series([100, 110, 105, 115, 120])
        returns = calculate_returns(prices)
        
        assert len(returns) == len(prices)
        assert pd.isna(returns.iloc[0])  # First return should be NaN
        assert abs(returns.iloc[1] - 0.1) < 0.001  # 10% return
    
    def test_calculate_moving_average(self):
        """Test moving average calculation"""
        data = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        ma3 = calculate_moving_average(data, window=3)
        
        assert len(ma3) == len(data)
        assert pd.isna(ma3.iloc[0])  # First values should be NaN
        assert pd.isna(ma3.iloc[1])
        assert ma3.iloc[2] == 2.0  # (1+2+3)/3 = 2
        assert ma3.iloc[3] == 3.0  # (2+3+4)/3 = 3
    
    def test_calculate_rsi(self):
        """Test RSI calculation"""
        prices = pd.Series([100, 102, 101, 103, 105, 104, 106, 108, 107, 109])
        rsi = calculate_rsi(prices, period=5)
        
        assert len(rsi) == len(prices)
        assert all(0 <= val <= 100 for val in rsi.dropna())  # RSI should be between 0 and 100
    
    def test_calculate_macd(self):
        """Test MACD calculation"""
        prices = pd.Series(np.random.randn(100).cumsum() + 100)
        macd_result = calculate_macd(prices)
        
        assert 'macd' in macd_result.columns
        assert 'signal' in macd_result.columns
        assert 'histogram' in macd_result.columns
        assert len(macd_result) == len(prices)
    
    def test_calculate_volatility(self):
        """Test volatility calculation"""
        returns = pd.Series(np.random.randn(100) * 0.01)  # 1% daily returns
        vol = calculate_volatility(returns, window=20)
        
        assert len(vol) == len(returns)
        assert all(val >= 0 for val in vol.dropna())  # Volatility should be non-negative
    
    def test_calculate_sharpe_ratio(self):
        """Test Sharpe ratio calculation"""
        returns = pd.Series(np.random.randn(252) * 0.01 + 0.0003)  # Daily returns with positive drift
        sharpe = calculate_sharpe_ratio(returns, risk_free_rate=0.02)
        
        assert isinstance(sharpe, float)
    
    def test_calculate_max_drawdown(self):
        """Test maximum drawdown calculation"""
        prices = pd.Series([100, 110, 105, 95, 100, 90, 95, 100])
        max_dd = calculate_max_drawdown(prices)
        
        assert max_dd < 0  # Drawdown should be negative
        assert -100 <= max_dd <= 0  # Should be between -100% and 0%