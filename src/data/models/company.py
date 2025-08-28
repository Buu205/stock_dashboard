"""
Company data models
"""

from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict


class CompanyInfo(BaseModel):
    """Basic company information"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    ticker: str = Field(..., description="Stock ticker symbol")
    name: str = Field(..., description="Company name")
    name_en: Optional[str] = Field(None, description="English name")
    exchange: Optional[str] = Field(None, description="Stock exchange (HOSE, HNX, UPCOM)")
    industry: Optional[str] = Field(None, description="Industry classification")
    sector: Optional[str] = Field(None, description="Sector classification")
    
    # Additional info
    website: Optional[str] = Field(None, description="Company website")
    phone: Optional[str] = Field(None, description="Contact phone")
    address: Optional[str] = Field(None, description="Company address")
    description: Optional[str] = Field(None, description="Business description")
    
    # Listing info
    listing_date: Optional[datetime] = Field(None, description="IPO/Listing date")
    charter_capital: Optional[float] = Field(None, description="Charter capital")
    shares_outstanding: Optional[float] = Field(None, description="Number of shares outstanding")
    

class Company(BaseModel):
    """Complete company model with all data"""
    model_config = ConfigDict(str_strip_whitespace=True)
    
    # Basic info
    info: CompanyInfo
    
    # Related data (will be populated by loaders)
    financial_statements: Optional[List[dict]] = Field(default_factory=list)
    market_data: Optional[List[dict]] = Field(default_factory=list)
    ratios: Optional[dict] = Field(default_factory=dict)
    
    # Metadata
    last_updated: Optional[datetime] = Field(default_factory=datetime.now)
    data_source: Optional[str] = Field(None, description="Data source identifier")
    
    def get_latest_price(self) -> Optional[float]:
        """Get latest stock price"""
        if self.market_data and len(self.market_data) > 0:
            return self.market_data[-1].get('close')
        return None
    
    def get_market_cap(self) -> Optional[float]:
        """Calculate market capitalization"""
        latest_price = self.get_latest_price()
        if latest_price and self.info.shares_outstanding:
            return latest_price * self.info.shares_outstanding
        return None
    
    def get_pe_ratio(self) -> Optional[float]:
        """Get P/E ratio"""
        if self.ratios:
            return self.ratios.get('pe')
        return None
    
    def get_pb_ratio(self) -> Optional[float]:
        """Get P/B ratio"""
        if self.ratios:
            return self.ratios.get('pb')
        return None