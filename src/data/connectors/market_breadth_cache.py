"""
Market Breadth Cache - Pre-calculate and cache market breadth statistics
"""

import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Dict, List, Optional
import concurrent.futures

logger = logging.getLogger(__name__)

class MarketBreadthCache:
    """Cache manager for market breadth calculations"""
    
    def __init__(self, cache_dir: str = "Database/cache"):
        """
        Initialize market breadth cache
        
        Args:
            cache_dir: Directory to store cache files
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.cache_file = self.cache_dir / "market_breadth_cache.json"
        
    def get_cached_stats(self, max_age_hours: int = 1) -> Optional[Dict]:
        """
        Get cached market breadth statistics if valid
        
        Args:
            max_age_hours: Maximum age of cache in hours
            
        Returns:
            Cached statistics or None if expired/not found
        """
        try:
            if not self.cache_file.exists():
                return None
                
            with open(self.cache_file, 'r') as f:
                cache = json.load(f)
            
            # Check cache age
            cached_time = datetime.fromisoformat(cache['timestamp'])
            if datetime.now() - cached_time > timedelta(hours=max_age_hours):
                logger.info("Cache expired")
                return None
                
            logger.info(f"Using cached market breadth from {cache['timestamp']}")
            return cache['stats']
            
        except Exception as e:
            logger.error(f"Error reading cache: {e}")
            return None
    
    def save_stats(self, stats: Dict):
        """
        Save market breadth statistics to cache
        
        Args:
            stats: Statistics to cache
        """
        try:
            cache = {
                'timestamp': datetime.now().isoformat(),
                'stats': stats
            }
            
            with open(self.cache_file, 'w') as f:
                json.dump(cache, f, indent=2)
                
            logger.info(f"Saved market breadth cache with {stats.get('total', 0)} stocks")
            
        except Exception as e:
            logger.error(f"Error saving cache: {e}")
    
    def pre_calculate_breadth(self, updater, symbols: List[str], batch_size: int = 20) -> Dict:
        """
        Pre-calculate market breadth for all symbols
        
        Args:
            updater: OHLCVUpdater instance
            symbols: List of symbols to analyze
            batch_size: Number of symbols to process in parallel
            
        Returns:
            Market breadth statistics
        """
        logger.info(f"Pre-calculating market breadth for {len(symbols)} symbols")
        
        stats = {
            'above_ma20': 0,
            'above_ma50': 0,
            'above_ma200': 0,
            'ema9_above_ema21': 0,
            'total': 0,
            'analyzed': [],
            'failed': [],
            'timestamp': datetime.now().isoformat()
        }
        
        def analyze_symbol(symbol):
            """Analyze a single symbol"""
            try:
                df = updater.get_ticker_data(symbol)
                
                if df.empty or len(df) < 200:
                    return None
                
                # Calculate indicators
                close = df['close'].iloc[-1]
                ma20 = df['close'].rolling(20).mean().iloc[-1]
                ma50 = df['close'].rolling(50).mean().iloc[-1]
                ma200 = df['close'].rolling(200).mean().iloc[-1]
                ema9 = df['close'].ewm(span=9, adjust=False).mean().iloc[-1]
                ema21 = df['close'].ewm(span=21, adjust=False).mean().iloc[-1]
                
                return {
                    'symbol': symbol,
                    'above_ma20': not pd.isna(ma20) and close > ma20,
                    'above_ma50': not pd.isna(ma50) and close > ma50,
                    'above_ma200': not pd.isna(ma200) and close > ma200,
                    'ema9_above_ema21': not pd.isna(ema9) and not pd.isna(ema21) and ema9 > ema21
                }
            except Exception as e:
                return {'symbol': symbol, 'error': str(e)}
        
        # Process in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = [executor.submit(analyze_symbol, symbol) for symbol in symbols]
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result(timeout=5)
                    if result and 'error' not in result:
                        stats['above_ma20'] += result['above_ma20']
                        stats['above_ma50'] += result['above_ma50']
                        stats['above_ma200'] += result['above_ma200']
                        stats['ema9_above_ema21'] += result['ema9_above_ema21']
                        stats['total'] += 1
                        stats['analyzed'].append(result['symbol'])
                    elif result:
                        stats['failed'].append(result['symbol'])
                except Exception:
                    pass
        
        # Calculate percentages
        if stats['total'] > 0:
            stats['pct_above_ma20'] = (stats['above_ma20'] / stats['total']) * 100
            stats['pct_above_ma50'] = (stats['above_ma50'] / stats['total']) * 100
            stats['pct_above_ma200'] = (stats['above_ma200'] / stats['total']) * 100
            stats['pct_ema_bullish'] = (stats['ema9_above_ema21'] / stats['total']) * 100
        
        # Save to cache
        self.save_stats(stats)
        
        return stats


def create_breadth_summary_table(stats: Dict) -> pd.DataFrame:
    """
    Create a summary table from market breadth statistics
    
    Args:
        stats: Market breadth statistics
        
    Returns:
        DataFrame with summary
    """
    if not stats or stats.get('total', 0) == 0:
        return pd.DataFrame()
    
    data = {
        'Indicator': ['MA20', 'MA50', 'MA200', 'EMA9>EMA21'],
        'Above': [
            stats['above_ma20'],
            stats['above_ma50'],
            stats['above_ma200'],
            stats['ema9_above_ema21']
        ],
        'Below': [
            stats['total'] - stats['above_ma20'],
            stats['total'] - stats['above_ma50'],
            stats['total'] - stats['above_ma200'],
            stats['total'] - stats['ema9_above_ema21']
        ],
        'Percentage': [
            f"{stats.get('pct_above_ma20', 0):.1f}%",
            f"{stats.get('pct_above_ma50', 0):.1f}%",
            f"{stats.get('pct_above_ma200', 0):.1f}%",
            f"{stats.get('pct_ema_bullish', 0):.1f}%"
        ]
    }
    
    return pd.DataFrame(data)