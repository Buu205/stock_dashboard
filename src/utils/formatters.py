"""
Formatting utilities for display
"""

from typing import Union, Optional
from datetime import datetime
import pandas as pd


def format_number(value: Union[int, float], decimals: int = 0) -> str:
    """
    Format number with thousand separators
    
    Args:
        value: Number to format
        decimals: Number of decimal places
        
    Returns:
        Formatted string
    """
    if pd.isna(value):
        return "N/A"
    
    if decimals > 0:
        return f"{value:,.{decimals}f}"
    else:
        return f"{int(value):,}"


def format_currency(value: Union[int, float], 
                   currency: str = "VND",
                   decimals: int = 0) -> str:
    """
    Format value as currency
    
    Args:
        value: Value to format
        currency: Currency code
        decimals: Number of decimal places
        
    Returns:
        Formatted currency string
    """
    if pd.isna(value):
        return "N/A"
    
    formatted = format_number(value, decimals)
    
    if currency == "VND":
        return f"{formatted} VND"
    elif currency == "USD":
        return f"${formatted}"
    else:
        return f"{formatted} {currency}"


def format_percentage(value: Union[int, float], decimals: int = 2) -> str:
    """
    Format value as percentage
    
    Args:
        value: Value to format (0.1 = 10%)
        decimals: Number of decimal places
        
    Returns:
        Formatted percentage string
    """
    if pd.isna(value):
        return "N/A"
    
    return f"{value * 100:.{decimals}f}%"


def format_date(date: Union[str, datetime, pd.Timestamp], 
               format_str: str = "%Y-%m-%d") -> str:
    """
    Format date to string
    
    Args:
        date: Date to format
        format_str: Format string
        
    Returns:
        Formatted date string
    """
    if pd.isna(date):
        return "N/A"
    
    if isinstance(date, str):
        date = pd.to_datetime(date)
    
    return date.strftime(format_str)


def format_large_number(value: Union[int, float]) -> str:
    """
    Format large numbers with K, M, B suffixes
    
    Args:
        value: Number to format
        
    Returns:
        Formatted string with suffix
    """
    if pd.isna(value):
        return "N/A"
    
    abs_value = abs(value)
    sign = "-" if value < 0 else ""
    
    if abs_value >= 1e9:
        return f"{sign}{abs_value/1e9:.2f}B"
    elif abs_value >= 1e6:
        return f"{sign}{abs_value/1e6:.2f}M"
    elif abs_value >= 1e3:
        return f"{sign}{abs_value/1e3:.2f}K"
    else:
        return f"{sign}{abs_value:.2f}"