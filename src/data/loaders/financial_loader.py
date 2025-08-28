"""
Financial Data Loader
Loads financial data from parquet files
"""

import pandas as pd
from typing import Optional, Dict, List
from pathlib import Path
import logging

from .base_loader import BaseLoader

logger = logging.getLogger(__name__)

class FinancialDataLoader(BaseLoader):
    """Loader for financial data from parquet files"""
    
    def load(self, ticker: Optional[str] = None) -> pd.DataFrame:
        """
        Load financial data
        
        Args:
            ticker: Optional ticker to filter for
            
        Returns:
            DataFrame with financial data
        """
        try:
            parquet_path = self.config.paths.parquet_path
            df = pd.read_parquet(parquet_path)
            
            if ticker:
                df = df[df['SECURITY_CODE'] == ticker]
            
            logger.info(f"Loaded {len(df)} records from {parquet_path}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading financial data: {e}")
            return pd.DataFrame()
    
    def get_ticker_data(self, ticker: str, 
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> pd.DataFrame:
        """
        Get financial data for a specific ticker
        
        Args:
            ticker: Stock ticker
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            DataFrame with ticker data
        """
        df = self.load(ticker)
        
        if not df.empty and 'PERIOD' in df.columns:
            if start_date:
                df = df[df['PERIOD'] >= start_date]
            if end_date:
                df = df[df['PERIOD'] <= end_date]
        
        return df
    
    def get_available_tickers(self) -> List[str]:
        """Get list of available tickers"""
        df = self.load()
        if not df.empty and 'SECURITY_CODE' in df.columns:
            return sorted(df['SECURITY_CODE'].unique().tolist())
        return []
    
    def get_metric_data(self, ticker: str, metric_code: str) -> pd.DataFrame:
        """
        Get specific metric data for a ticker
        
        Args:
            ticker: Stock ticker
            metric_code: Metric code (e.g., 'CIS_20' for revenue)
            
        Returns:
            DataFrame with metric data
        """
        df = self.get_ticker_data(ticker)
        
        if not df.empty and metric_code in df.columns:
            return df[['PERIOD', metric_code]].dropna()
        
        return pd.DataFrame()
    
    def get_metrics_summary(self, ticker: str, 
                           metrics: List[str]) -> Dict[str, pd.DataFrame]:
        """
        Get summary of multiple metrics
        
        Args:
            ticker: Stock ticker
            metrics: List of metric codes
            
        Returns:
            Dictionary mapping metric to DataFrame
        """
        result = {}
        df = self.get_ticker_data(ticker)
        
        if not df.empty:
            for metric in metrics:
                if metric in df.columns:
                    result[metric] = df[['PERIOD', metric]].dropna()
        
        return result