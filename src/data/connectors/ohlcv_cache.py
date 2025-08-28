"""
OHLCV Cache Manager - SQLite-based caching for OHLCV data
"""

import pandas as pd
import sqlite3
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
from pathlib import Path
import logging
import json

logger = logging.getLogger(__name__)

class OHLCVCacheManager:
    """Manage OHLCV data caching using SQLite"""
    
    def __init__(self, cache_dir: str = "Database/cache", db_name: str = "ohlcv_cache.db"):
        """
        Initialize cache manager
        
        Args:
            cache_dir: Directory to store cache database
            db_name: Name of the SQLite database file
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.db_path = self.cache_dir / db_name
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        
        self._init_database()
        logger.info(f"OHLCVCacheManager initialized at {self.db_path}")
    
    def _init_database(self):
        """Initialize database tables"""
        cursor = self.conn.cursor()
        
        # Create OHLCV data table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ohlcv_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                resolution TEXT DEFAULT '1D',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, date, resolution)
            )
        ''')
        
        # Create metadata table for tracking updates
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cache_metadata (
                symbol TEXT NOT NULL,
                resolution TEXT NOT NULL,
                last_update TIMESTAMP,
                start_date TEXT,
                end_date TEXT,
                record_count INTEGER,
                PRIMARY KEY (symbol, resolution)
            )
        ''')
        
        # Create indices for faster queries
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_symbol_date 
            ON ohlcv_data(symbol, date)
        ''')
        
        self.conn.commit()
    
    def save_ohlcv(self, symbol: str, df: pd.DataFrame, resolution: str = '1D'):
        """
        Save OHLCV data to cache
        
        Args:
            symbol: Stock symbol
            df: DataFrame with OHLCV data (index should be date)
            resolution: Time resolution
        """
        if df.empty:
            logger.warning(f"Empty DataFrame for {symbol}, skipping save")
            return
        
        cursor = self.conn.cursor()
        
        try:
            # Prepare data for insertion
            df_copy = df.copy()
            df_copy['symbol'] = symbol
            df_copy['resolution'] = resolution
            
            # If index is date, reset it
            if df_copy.index.name:
                df_copy['date'] = df_copy.index
            
            df_copy['date'] = pd.to_datetime(df_copy['date']).dt.strftime('%Y-%m-%d')
            
            # Insert or replace data
            for _, row in df_copy.iterrows():
                cursor.execute('''
                    INSERT OR REPLACE INTO ohlcv_data 
                    (symbol, date, open, high, low, close, volume, resolution)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    row['symbol'],
                    row['date'],
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    resolution
                ))
            
            # Update metadata
            start_date = df_copy['date'].min()
            end_date = df_copy['date'].max()
            record_count = len(df_copy)
            
            cursor.execute('''
                INSERT OR REPLACE INTO cache_metadata 
                (symbol, resolution, last_update, start_date, end_date, record_count)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                symbol,
                resolution,
                datetime.now(),
                start_date,
                end_date,
                record_count
            ))
            
            self.conn.commit()
            logger.info(f"Saved {record_count} records for {symbol} ({resolution})")
            
        except Exception as e:
            logger.error(f"Error saving data for {symbol}: {e}")
            self.conn.rollback()
    
    def get_ohlcv(self, 
                  symbol: str,
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  resolution: str = '1D') -> Optional[pd.DataFrame]:
        """
        Get OHLCV data from cache
        
        Args:
            symbol: Stock symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            resolution: Time resolution
            
        Returns:
            DataFrame with OHLCV data or None if not cached
        """
        cursor = self.conn.cursor()
        
        # Build query
        query = '''
            SELECT date, open, high, low, close, volume
            FROM ohlcv_data
            WHERE symbol = ? AND resolution = ?
        '''
        
        params = [symbol, resolution]
        
        if start_date:
            query += ' AND date >= ?'
            params.append(start_date)
        
        if end_date:
            query += ' AND date <= ?'
            params.append(end_date)
        
        query += ' ORDER BY date'
        
        try:
            df = pd.read_sql_query(query, self.conn, params=params, parse_dates=['date'])
            
            if not df.empty:
                df.set_index('date', inplace=True)
                logger.info(f"Retrieved {len(df)} cached records for {symbol}")
                return df
            else:
                logger.info(f"No cached data found for {symbol}")
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving cached data for {symbol}: {e}")
            return None
    
    def is_cache_valid(self, symbol: str, resolution: str = '1D', max_age_hours: int = 24) -> bool:
        """
        Check if cache is still valid
        
        Args:
            symbol: Stock symbol
            resolution: Time resolution
            max_age_hours: Maximum age of cache in hours
            
        Returns:
            True if cache is valid, False otherwise
        """
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT last_update FROM cache_metadata
            WHERE symbol = ? AND resolution = ?
        ''', (symbol, resolution))
        
        result = cursor.fetchone()
        
        if result:
            last_update = datetime.fromisoformat(result[0])
            age = datetime.now() - last_update
            
            is_valid = age.total_seconds() < (max_age_hours * 3600)
            logger.debug(f"Cache for {symbol} is {'valid' if is_valid else 'expired'} (age: {age})")
            
            return is_valid
        
        return False
    
    def get_cached_symbols(self, resolution: str = '1D') -> List[str]:
        """Get list of symbols in cache"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            SELECT DISTINCT symbol FROM cache_metadata
            WHERE resolution = ?
            ORDER BY symbol
        ''', (resolution,))
        
        return [row[0] for row in cursor.fetchall()]
    
    def clear_cache(self, symbol: Optional[str] = None):
        """
        Clear cache data
        
        Args:
            symbol: Specific symbol to clear, or None to clear all
        """
        cursor = self.conn.cursor()
        
        if symbol:
            cursor.execute('DELETE FROM ohlcv_data WHERE symbol = ?', (symbol,))
            cursor.execute('DELETE FROM cache_metadata WHERE symbol = ?', (symbol,))
            logger.info(f"Cleared cache for {symbol}")
        else:
            cursor.execute('DELETE FROM ohlcv_data')
            cursor.execute('DELETE FROM cache_metadata')
            logger.info("Cleared all cache")
        
        self.conn.commit()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        cursor = self.conn.cursor()
        
        # Get overall stats
        cursor.execute('SELECT COUNT(DISTINCT symbol) FROM cache_metadata')
        symbol_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM ohlcv_data')
        total_records = cursor.fetchone()[0]
        
        # Get per-symbol stats
        cursor.execute('''
            SELECT symbol, resolution, record_count, last_update
            FROM cache_metadata
            ORDER BY last_update DESC
            LIMIT 10
        ''')
        
        recent_updates = []
        for row in cursor.fetchall():
            recent_updates.append({
                'symbol': row[0],
                'resolution': row[1],
                'records': row[2],
                'last_update': row[3]
            })
        
        return {
            'symbol_count': symbol_count,
            'total_records': total_records,
            'db_size_mb': self.db_path.stat().st_size / (1024 * 1024),
            'recent_updates': recent_updates
        }
    
    def close(self):
        """Close database connection"""
        self.conn.close()
        logger.info("Cache database connection closed")
    
    def __del__(self):
        """Cleanup on deletion"""
        try:
            self.conn.close()
        except:
            pass