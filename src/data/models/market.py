"""
Market data models
"""

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict


class PriceData(BaseModel):
    """Stock price data"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    ticker: str = Field(..., description="Stock ticker")
    date: datetime = Field(..., description="Trading date")
    open: float = Field(..., description="Opening price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    close: float = Field(..., description="Closing price")
    adjusted_close: Optional[float] = Field(None, description="Adjusted closing price")
    
    def get_daily_return(self) -> float:
        """Calculate daily return"""
        if self.open > 0:
            return ((self.close - self.open) / self.open) * 100
        return 0.0
    
    def get_intraday_range(self) -> float:
        """Calculate intraday price range"""
        return self.high - self.low
    
    def get_intraday_volatility(self) -> float:
        """Calculate intraday volatility percentage"""
        if self.low > 0:
            return ((self.high - self.low) / self.low) * 100
        return 0.0


class VolumeData(BaseModel):
    """Trading volume data"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    ticker: str = Field(..., description="Stock ticker")
    date: datetime = Field(..., description="Trading date")
    volume: int = Field(..., description="Trading volume")
    value: Optional[float] = Field(None, description="Trading value")
    trades: Optional[int] = Field(None, description="Number of trades")
    
    def get_average_trade_size(self) -> Optional[float]:
        """Calculate average trade size"""
        if self.trades and self.trades > 0:
            return self.volume / self.trades
        return None
    
    def get_average_trade_value(self) -> Optional[float]:
        """Calculate average trade value"""
        if self.trades and self.trades > 0 and self.value:
            return self.value / self.trades
        return None


class MarketData(BaseModel):
    """Complete market data for a ticker"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    ticker: str = Field(..., description="Stock ticker")
    date: datetime = Field(..., description="Data date")
    
    # Price data
    open: float = Field(..., description="Opening price")
    high: float = Field(..., description="High price")
    low: float = Field(..., description="Low price")
    close: float = Field(..., description="Closing price")
    adjusted_close: Optional[float] = Field(None, description="Adjusted close")
    
    # Volume data
    volume: int = Field(..., description="Trading volume")
    value: Optional[float] = Field(None, description="Trading value")
    
    # Change metrics
    change: Optional[float] = Field(None, description="Price change")
    change_percent: Optional[float] = Field(None, description="Price change percentage")
    
    # Technical indicators (optional)
    ma_5: Optional[float] = Field(None, description="5-day moving average")
    ma_10: Optional[float] = Field(None, description="10-day moving average")
    ma_20: Optional[float] = Field(None, description="20-day moving average")
    ma_50: Optional[float] = Field(None, description="50-day moving average")
    ma_200: Optional[float] = Field(None, description="200-day moving average")
    
    rsi: Optional[float] = Field(None, description="RSI indicator")
    macd: Optional[float] = Field(None, description="MACD value")
    macd_signal: Optional[float] = Field(None, description="MACD signal")
    macd_histogram: Optional[float] = Field(None, description="MACD histogram")
    
    # Market metrics
    market_cap: Optional[float] = Field(None, description="Market capitalization")
    shares_outstanding: Optional[float] = Field(None, description="Shares outstanding")
    
    def calculate_change(self, previous_close: float) -> None:
        """Calculate price change from previous close"""
        self.change = self.close - previous_close
        if previous_close > 0:
            self.change_percent = (self.change / previous_close) * 100
    
    def is_bullish(self) -> bool:
        """Check if day was bullish (close > open)"""
        return self.close > self.open
    
    def is_above_ma(self, ma_period: int = 20) -> Optional[bool]:
        """Check if price is above moving average"""
        ma_value = getattr(self, f"ma_{ma_period}", None)
        if ma_value:
            return self.close > ma_value
        return None