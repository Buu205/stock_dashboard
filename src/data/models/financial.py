"""
Financial data models
"""

from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum


class PeriodType(str, Enum):
    """Financial period types"""
    YEARLY = "yearly"
    QUARTERLY = "quarterly"
    TTM = "ttm"  # Trailing Twelve Months


class FinancialMetric(BaseModel):
    """Individual financial metric"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    code: str = Field(..., description="Metric code (e.g., CIS_20)")
    name: str = Field(..., description="Metric name")
    value: float = Field(..., description="Metric value")
    unit: Optional[str] = Field(None, description="Unit of measurement")
    period: str = Field(..., description="Period (e.g., 2023Q4)")
    period_type: PeriodType = Field(..., description="Period type")
    date: datetime = Field(..., description="Report date")
    

class FinancialStatement(BaseModel):
    """Financial statement data"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    ticker: str = Field(..., description="Stock ticker")
    period: str = Field(..., description="Period identifier")
    period_type: PeriodType = Field(..., description="Period type")
    report_date: datetime = Field(..., description="Report date")
    
    # Income Statement
    revenue: Optional[float] = Field(None, description="Total revenue")
    cost_of_goods_sold: Optional[float] = Field(None, description="COGS")
    gross_profit: Optional[float] = Field(None, description="Gross profit")
    operating_expenses: Optional[float] = Field(None, description="Operating expenses")
    operating_profit: Optional[float] = Field(None, description="Operating profit")
    ebit: Optional[float] = Field(None, description="EBIT")
    ebitda: Optional[float] = Field(None, description="EBITDA")
    net_income: Optional[float] = Field(None, description="Net income")
    
    # Balance Sheet
    total_assets: Optional[float] = Field(None, description="Total assets")
    current_assets: Optional[float] = Field(None, description="Current assets")
    total_liabilities: Optional[float] = Field(None, description="Total liabilities")
    current_liabilities: Optional[float] = Field(None, description="Current liabilities")
    total_equity: Optional[float] = Field(None, description="Total equity")
    
    # Cash Flow
    operating_cash_flow: Optional[float] = Field(None, description="Operating cash flow")
    investing_cash_flow: Optional[float] = Field(None, description="Investing cash flow")
    financing_cash_flow: Optional[float] = Field(None, description="Financing cash flow")
    free_cash_flow: Optional[float] = Field(None, description="Free cash flow")
    
    # Additional metrics
    metrics: Dict[str, Any] = Field(default_factory=dict, description="Additional metrics")
    
    def calculate_gross_margin(self) -> Optional[float]:
        """Calculate gross margin"""
        if self.revenue and self.gross_profit:
            return (self.gross_profit / self.revenue) * 100
        return None
    
    def calculate_operating_margin(self) -> Optional[float]:
        """Calculate operating margin"""
        if self.revenue and self.operating_profit:
            return (self.operating_profit / self.revenue) * 100
        return None
    
    def calculate_net_margin(self) -> Optional[float]:
        """Calculate net profit margin"""
        if self.revenue and self.net_income:
            return (self.net_income / self.revenue) * 100
        return None


class FinancialRatio(BaseModel):
    """Financial ratios and metrics"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    ticker: str = Field(..., description="Stock ticker")
    period: str = Field(..., description="Period")
    date: datetime = Field(..., description="Calculation date")
    
    # Valuation Ratios
    pe_ratio: Optional[float] = Field(None, description="Price to Earnings")
    pb_ratio: Optional[float] = Field(None, description="Price to Book")
    ps_ratio: Optional[float] = Field(None, description="Price to Sales")
    peg_ratio: Optional[float] = Field(None, description="PEG Ratio")
    ev_ebitda: Optional[float] = Field(None, description="EV/EBITDA")
    
    # Profitability Ratios
    roe: Optional[float] = Field(None, description="Return on Equity")
    roa: Optional[float] = Field(None, description="Return on Assets")
    roic: Optional[float] = Field(None, description="Return on Invested Capital")
    gross_margin: Optional[float] = Field(None, description="Gross Margin %")
    operating_margin: Optional[float] = Field(None, description="Operating Margin %")
    net_margin: Optional[float] = Field(None, description="Net Margin %")
    
    # Liquidity Ratios
    current_ratio: Optional[float] = Field(None, description="Current Ratio")
    quick_ratio: Optional[float] = Field(None, description="Quick Ratio")
    cash_ratio: Optional[float] = Field(None, description="Cash Ratio")
    
    # Leverage Ratios
    debt_to_equity: Optional[float] = Field(None, description="Debt to Equity")
    debt_to_assets: Optional[float] = Field(None, description="Debt to Assets")
    interest_coverage: Optional[float] = Field(None, description="Interest Coverage")
    
    # Efficiency Ratios
    asset_turnover: Optional[float] = Field(None, description="Asset Turnover")
    inventory_turnover: Optional[float] = Field(None, description="Inventory Turnover")
    receivables_turnover: Optional[float] = Field(None, description="Receivables Turnover")