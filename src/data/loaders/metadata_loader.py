"""
Metadata loader
"""

import pandas as pd
from typing import Optional, Dict, Any

from .base_loader import BaseLoader
from src.core.exceptions import DataLoadError


class MetadataLoader(BaseLoader):
    """Loader for metadata"""
    
    def load(self) -> pd.DataFrame:
        """
        Load metadata
        
        Returns:
            DataFrame with metadata
        """
        return self.data_manager.get_metadata()
    
    def validate(self, data: Any) -> bool:
        """
        Validate metadata
        
        Args:
            data: Data to validate
            
        Returns:
            True if valid
        """
        return isinstance(data, pd.DataFrame)
    
    def get_metric_info(self, metric_code: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a specific metric
        
        Args:
            metric_code: Metric code
            
        Returns:
            Dictionary with metric information
        """
        metadata = self.load()
        
        if metadata.empty:
            return None
        
        # Look for metric in metadata
        if 'code' in metadata.columns:
            metric_row = metadata[metadata['code'] == metric_code]
            if not metric_row.empty:
                return metric_row.iloc[0].to_dict()
        
        return None