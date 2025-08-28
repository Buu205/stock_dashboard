"""
Base loader class for all data loaders
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, List
import pandas as pd
from pathlib import Path

from src.core.config import Config
from src.core.data_manager import DataManager
from src.core.exceptions import DataLoadError


class BaseLoader(ABC):
    """Abstract base class for data loaders"""
    
    def __init__(self, config: Optional[Config] = None, 
                 data_manager: Optional[DataManager] = None):
        """
        Initialize base loader
        
        Args:
            config: Configuration object
            data_manager: Data manager instance
        """
        self.config = config or Config()
        self.data_manager = data_manager or DataManager(self.config)
        self._cache = {}
    
    @abstractmethod
    def load(self, *args, **kwargs) -> Any:
        """
        Load data - must be implemented by subclasses
        
        Returns:
            Loaded data in appropriate format
        """
        pass
    
    @abstractmethod
    def validate(self, data: Any) -> bool:
        """
        Validate loaded data
        
        Args:
            data: Data to validate
            
        Returns:
            True if valid, False otherwise
        """
        pass
    
    def get_from_cache(self, key: str) -> Optional[Any]:
        """
        Get data from cache
        
        Args:
            key: Cache key
            
        Returns:
            Cached data or None
        """
        # Try data manager cache first
        if self.data_manager:
            cached = self.data_manager.get_cached_data(key)
            if cached is not None:
                return cached
        
        # Try local cache
        return self._cache.get(key)
    
    def save_to_cache(self, key: str, data: Any) -> None:
        """
        Save data to cache
        
        Args:
            key: Cache key
            data: Data to cache
        """
        # Save to data manager cache
        if self.data_manager:
            self.data_manager.cache_data(key, data)
        
        # Save to local cache
        self._cache[key] = data
    
    def load_parquet(self, file_path: str) -> pd.DataFrame:
        """
        Load data from parquet file
        
        Args:
            file_path: Path to parquet file
            
        Returns:
            DataFrame with loaded data
        """
        try:
            path = Path(file_path)
            if not path.exists():
                raise DataLoadError(f"File not found: {file_path}")
            
            return pd.read_parquet(path)
        except Exception as e:
            raise DataLoadError(f"Failed to load parquet: {str(e)}")
    
    def load_excel(self, file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """
        Load data from Excel file
        
        Args:
            file_path: Path to Excel file
            sheet_name: Optional sheet name
            
        Returns:
            DataFrame with loaded data
        """
        try:
            path = Path(file_path)
            if not path.exists():
                raise DataLoadError(f"File not found: {file_path}")
            
            return pd.read_excel(path, sheet_name=sheet_name)
        except Exception as e:
            raise DataLoadError(f"Failed to load Excel: {str(e)}")
    
    def filter_by_date_range(self, df: pd.DataFrame, 
                            start_date: Optional[pd.Timestamp] = None,
                            end_date: Optional[pd.Timestamp] = None,
                            date_column: str = 'date') -> pd.DataFrame:
        """
        Filter DataFrame by date range
        
        Args:
            df: DataFrame to filter
            start_date: Start date
            end_date: End date
            date_column: Name of date column
            
        Returns:
            Filtered DataFrame
        """
        if date_column not in df.columns:
            return df
        
        # Ensure date column is datetime
        df[date_column] = pd.to_datetime(df[date_column])
        
        # Apply filters
        if start_date:
            df = df[df[date_column] >= start_date]
        if end_date:
            df = df[df[date_column] <= end_date]
        
        return df.sort_values(date_column)
    
    def get_available_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Get list of available columns in DataFrame
        
        Args:
            df: DataFrame
            
        Returns:
            List of column names
        """
        return df.columns.tolist()
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean data by removing duplicates and handling missing values
        
        Args:
            df: DataFrame to clean
            
        Returns:
            Cleaned DataFrame
        """
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values (basic strategy - can be overridden)
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_columns:
            df[col] = df[col].fillna(0)
        
        return df