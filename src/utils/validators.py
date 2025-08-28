"""
Data validation utilities
"""

from typing import Optional, Tuple
from datetime import datetime, date
import re
import pandas as pd


def validate_ticker(ticker: str) -> bool:
    """
    Validate stock ticker format
    
    Args:
        ticker: Ticker symbol to validate
        
    Returns:
        True if valid ticker format
    """
    if not ticker:
        return False
    
    # Vietnamese tickers are usually 3-4 uppercase letters
    pattern = r'^[A-Z]{3,4}$'
    return bool(re.match(pattern, ticker.upper()))


def validate_date_range(start_date: Optional[datetime], 
                       end_date: Optional[datetime]) -> Tuple[bool, str]:
    """
    Validate date range
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if start_date and end_date:
        if start_date > end_date:
            return False, "Start date must be before end date"
        
        if end_date > datetime.now():
            return False, "End date cannot be in the future"
    
    return True, ""


def validate_metric_code(metric_code: str) -> bool:
    """
    Validate financial metric code format
    
    Args:
        metric_code: Metric code to validate
        
    Returns:
        True if valid metric code
    """
    if not metric_code:
        return False
    
    # Valid formats: CIS_XX, CBS_XX, CFS_XX
    pattern = r'^(CIS|CBS|CFS)_\d{1,3}$'
    return bool(re.match(pattern, metric_code.upper()))


def validate_percentage(value: float) -> bool:
    """
    Validate percentage value
    
    Args:
        value: Value to validate
        
    Returns:
        True if valid percentage (0-100)
    """
    return 0 <= value <= 100


def validate_dataframe(df: pd.DataFrame, 
                      required_columns: list) -> Tuple[bool, str]:
    """
    Validate DataFrame has required columns
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if df is None or df.empty:
        return False, "DataFrame is empty"
    
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        return False, f"Missing columns: {', '.join(missing_columns)}"
    
    return True, ""