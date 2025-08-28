"""
Configuration Management Module
Handles loading and validation of application configuration
"""

import yaml
import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class PathConfig:
    """Data paths configuration"""
    parquet_path: Path
    metadata_path: Path
    cache_dir: Path
    output_dir: Path
    
    def validate(self):
        """Validate that required files exist"""
        if not self.parquet_path.exists():
            raise FileNotFoundError(f"Parquet file not found: {self.parquet_path}")
        if not self.metadata_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_path}")
        
        # Create directories if they don't exist
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"✓ All paths validated successfully")


@dataclass
class DataConfig:
    """Data loading configuration"""
    cache_ttl: int = 3600
    default_lookback_years: int = 5
    default_frequency: str = "Y"
    batch_size: int = 100


@dataclass
class MetricsMapping:
    """Metrics code mapping"""
    income_statement: Dict[str, str] = field(default_factory=dict)
    balance_sheet: Dict[str, str] = field(default_factory=dict)
    cash_flow: Dict[str, str] = field(default_factory=dict)
    
    def get_all_metrics(self) -> Dict[str, str]:
        """Get all metrics combined"""
        all_metrics = {}
        all_metrics.update(self.income_statement)
        all_metrics.update(self.balance_sheet)
        all_metrics.update(self.cash_flow)
        return all_metrics
    
    def get_metric_code(self, name: str) -> Optional[str]:
        """Get metric code by name"""
        all_metrics = self.get_all_metrics()
        return all_metrics.get(name)
    
    def get_metric_name(self, code: str) -> Optional[str]:
        """Get metric name by code (reverse lookup)"""
        all_metrics = self.get_all_metrics()
        for name, metric_code in all_metrics.items():
            if metric_code == code:
                return name
        return None


@dataclass
class CalculationConfig:
    """Calculation parameters"""
    growth_min_periods: int = 2
    growth_annualize: bool = True
    good_roe: float = 0.15
    good_roa: float = 0.10
    high_debt_equity: float = 1.0
    good_current_ratio: float = 1.5


@dataclass
class AppConfig:
    """Main application configuration"""
    # Basic info
    app_name: str
    version: str
    debug: bool
    
    # Sub-configurations
    paths: PathConfig
    data: DataConfig
    metrics: MetricsMapping
    calculations: CalculationConfig
    
    # Metadata
    loaded_at: datetime = field(default_factory=datetime.now)
    config_file: Optional[str] = None
    
    @classmethod
    def load_from_yaml(cls, config_path: str = "config.yaml") -> 'AppConfig':
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to config.yaml file
            
        Returns:
            AppConfig instance
        """
        config_path = Path(config_path)
        
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        logger.info(f"Loading configuration from: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config_dict = yaml.safe_load(f)
        
        # Parse paths
        paths = PathConfig(
            parquet_path=Path(config_dict['paths']['data']['parquet']),
            metadata_path=Path(config_dict['paths']['data']['metadata']),
            cache_dir=Path(config_dict['paths']['data']['cache_dir']),
            output_dir=Path(config_dict['paths']['data'].get('output_dir', './outputs'))
        )
        
        # Parse data config
        data_config = DataConfig(
            cache_ttl=config_dict['data']['cache_ttl'],
            default_lookback_years=config_dict['data']['default_lookback_years'],
            default_frequency=config_dict['data']['default_frequency'],
            batch_size=config_dict['data'].get('batch_size', 100)
        )
        
        # Parse metrics mapping
        metrics = MetricsMapping(
            income_statement=config_dict['analysis']['income_statement'],
            balance_sheet=config_dict['analysis']['balance_sheet'],
            cash_flow=config_dict['analysis']['cash_flow']
        )
        
        # Parse calculation config
        calc_config = config_dict['calculations']
        calculations = CalculationConfig(
            growth_min_periods=calc_config['growth']['min_periods'],
            growth_annualize=calc_config['growth']['annualize'],
            good_roe=calc_config['thresholds']['good_roe'],
            good_roa=calc_config['thresholds']['good_roa'],
            high_debt_equity=calc_config['thresholds']['high_debt_equity'],
            good_current_ratio=calc_config['thresholds']['good_current_ratio']
        )
        
        # Create config instance
        config = cls(
            app_name=config_dict['app']['name'],
            version=config_dict['app']['version'],
            debug=config_dict['app']['debug'],
            paths=paths,
            data=data_config,
            metrics=metrics,
            calculations=calculations,
            config_file=str(config_path)
        )
        
        # Validate paths
        config.validate()
        
        logger.info(f"✓ Configuration loaded successfully: {config.app_name} v{config.version}")
        
        return config
    
    def validate(self):
        """Validate entire configuration"""
        self.paths.validate()
        
        # Check if we have metrics
        if not self.metrics.get_all_metrics():
            raise ValueError("No metrics defined in configuration")
        
        logger.info(f"✓ Configuration validated: {len(self.metrics.get_all_metrics())} metrics defined")
    
    def get_cache_path(self, filename: str) -> Path:
        """Get full cache file path"""
        return self.paths.cache_dir / filename
    
    def is_cache_valid(self, cache_file: Path) -> bool:
        """Check if cache file is still valid based on TTL"""
        if not cache_file.exists():
            return False
        
        age = datetime.now().timestamp() - cache_file.stat().st_mtime
        return age < self.data.cache_ttl
    
    def __str__(self) -> str:
        """String representation"""
        return (
            f"AppConfig(\n"
            f"  app: {self.app_name} v{self.version}\n"
            f"  debug: {self.debug}\n"
            f"  parquet: {self.paths.parquet_path.name}\n"
            f"  metrics: {len(self.metrics.get_all_metrics())} defined\n"
            f"  cache_ttl: {self.data.cache_ttl}s\n"
            f")"
        )


# Singleton pattern for config
_config_instance: Optional[AppConfig] = None


def get_config(config_path: str = "config.yaml", force_reload: bool = False) -> AppConfig:
    """
    Get configuration singleton instance
    
    Args:
        config_path: Path to config file
        force_reload: Force reload configuration
        
    Returns:
        AppConfig instance
    """
    global _config_instance
    
    if _config_instance is None or force_reload:
        _config_instance = AppConfig.load_from_yaml(config_path)
    
    return _config_instance


def reset_config():
    """Reset configuration (useful for testing)"""
    global _config_instance
    _config_instance = None
    logger.info("Configuration reset")


# Convenience functions
def get_metric_code(metric_name: str) -> Optional[str]:
    """Get metric code by name"""
    config = get_config()
    return config.metrics.get_metric_code(metric_name)


def get_metric_name(metric_code: str) -> Optional[str]:
    """Get metric name by code"""
    config = get_config()
    return config.metrics.get_metric_name(metric_code)


def get_parquet_path() -> Path:
    """Get parquet file path"""
    config = get_config()
    return config.paths.parquet_path


def get_metadata_path() -> Path:
    """Get metadata file path"""
    config = get_config()
    return config.paths.metadata_path


if __name__ == "__main__":
    # Test configuration loading
    try:
        config = get_config()
        print(config)
        print(f"\nAll metrics: {list(config.metrics.get_all_metrics().keys())[:5]}...")
        print(f"Revenue code: {get_metric_code('revenue')}")
        print(f"CIS_20 name: {get_metric_name('CIS_20')}")
    except Exception as e:
        logger.error(f"Configuration error: {e}")