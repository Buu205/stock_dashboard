#!/usr/bin/env python3
"""
Calculate and save Market Breadth data to database
Tính toán tỷ lệ % cổ phiếu > MA20 và MA50 cho mỗi ngày
"""

import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add src to path
sys.path.append(str(Path(__file__).parent))
from src.data.connectors.ohlcv_cache import OHLCVCacheManager

def calculate_market_breadth_history():
    """
    Tính toán market breadth cho 1 năm qua và lưu vào database
    """
    print("=" * 60)
    print("CALCULATING MARKET BREADTH HISTORY")
    print("=" * 60)
    
    # Initialize cache manager
    cache = OHLCVCacheManager()
    
    # Get all symbols from cache
    conn = sqlite3.connect('Database/cache/ohlcv_cache.db')
    cursor = conn.cursor()
    cursor.execute('SELECT DISTINCT symbol FROM ohlcv_data ORDER BY symbol')
    all_symbols = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    print(f"Found {len(all_symbols)} symbols in cache")
    
    # Dictionary to store breadth data by date
    breadth_by_date = {}
    
    # Process each symbol
    for i, symbol in enumerate(all_symbols, 1):
        print(f"Processing {i}/{len(all_symbols)}: {symbol}", end="\r")
        
        try:
            # Get OHLCV data directly from database
            conn_cache = sqlite3.connect('Database/cache/ohlcv_cache.db')
            query = f"""
                SELECT date, open, high, low, close, volume
                FROM ohlcv_data 
                WHERE symbol = '{symbol}'
                AND date >= date('now', '-365 days')
                ORDER BY date
            """
            df = pd.read_sql_query(query, conn_cache, parse_dates=['date'], index_col='date')
            conn_cache.close()
            if df is None or df.empty or len(df) < 50:
                continue
            
            # Calculate MA20 and MA50
            df['ma20'] = df['close'].rolling(window=20, min_periods=20).mean()
            df['ma50'] = df['close'].rolling(window=50, min_periods=50).mean()
            
            # Calculate trading value (billion VND)
            df['trading_value'] = (df['close'] * df['volume']) / 1_000_000_000
            
            # For each date, check if price > MA
            for date, row in df.iterrows():
                # Skip if trading value < 3 billion VND
                if row['trading_value'] < 3:
                    continue
                    
                # Initialize date entry if not exists
                if date not in breadth_by_date:
                    breadth_by_date[date] = {
                        'total_stocks': 0,
                        'above_ma20': 0,
                        'above_ma50': 0
                    }
                
                # Count this stock
                breadth_by_date[date]['total_stocks'] += 1
                
                # Check MA20
                if not pd.isna(row['ma20']) and row['close'] > row['ma20']:
                    breadth_by_date[date]['above_ma20'] += 1
                
                # Check MA50  
                if not pd.isna(row['ma50']) and row['close'] > row['ma50']:
                    breadth_by_date[date]['above_ma50'] += 1
                    
        except Exception as e:
            print(f"\nError processing {symbol}: {e}")
            continue
    
    print("\n\nCalculating percentages...")
    
    # Convert to DataFrame with percentages
    breadth_data = []
    for date, stats in breadth_by_date.items():
        if stats['total_stocks'] >= 10:  # Need at least 10 stocks
            breadth_data.append({
                'date': date,
                'total_stocks': stats['total_stocks'],
                'above_ma20_count': stats['above_ma20'],
                'above_ma50_count': stats['above_ma50'],
                'pct_above_ma20': (stats['above_ma20'] / stats['total_stocks']) * 100,
                'pct_above_ma50': (stats['above_ma50'] / stats['total_stocks']) * 100
            })
    
    if not breadth_data:
        print("No breadth data calculated!")
        return None
    
    # Create DataFrame
    breadth_df = pd.DataFrame(breadth_data)
    breadth_df['date'] = pd.to_datetime(breadth_df['date'])
    breadth_df = breadth_df.sort_values('date')
    
    print(f"\nGenerated breadth data for {len(breadth_df)} days")
    print(f"Date range: {breadth_df['date'].min()} to {breadth_df['date'].max()}")
    
    # Save to SQLite
    conn = sqlite3.connect('Database/cache/market_breadth.db')
    
    # Create table
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_breadth (
            date TEXT PRIMARY KEY,
            total_stocks INTEGER,
            above_ma20_count INTEGER,
            above_ma50_count INTEGER,
            pct_above_ma20 REAL,
            pct_above_ma50 REAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Save data
    breadth_df.to_sql('market_breadth', conn, if_exists='replace', index=False)
    conn.commit()
    
    # Show summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total trading days: {len(breadth_df)}")
    print(f"Average stocks per day: {breadth_df['total_stocks'].mean():.0f}")
    print(f"Average % above MA20: {breadth_df['pct_above_ma20'].mean():.1f}%")
    print(f"Average % above MA50: {breadth_df['pct_above_ma50'].mean():.1f}%")
    print(f"\nCurrent (latest):")
    latest = breadth_df.iloc[-1]
    print(f"  Date: {latest['date'].strftime('%Y-%m-%d')}")
    print(f"  Stocks analyzed: {latest['total_stocks']}")
    print(f"  Above MA20: {latest['above_ma20_count']}/{latest['total_stocks']} ({latest['pct_above_ma20']:.1f}%)")
    print(f"  Above MA50: {latest['above_ma50_count']}/{latest['total_stocks']} ({latest['pct_above_ma50']:.1f}%)")
    
    print(f"\n✅ Data saved to Database/cache/market_breadth.db")
    
    conn.close()
    return breadth_df


def load_market_breadth():
    """
    Load market breadth data from database
    """
    try:
        conn = sqlite3.connect('Database/cache/market_breadth.db')
        df = pd.read_sql_query('SELECT * FROM market_breadth ORDER BY date', conn)
        df['date'] = pd.to_datetime(df['date'])
        conn.close()
        return df
    except Exception as e:
        print(f"Error loading market breadth: {e}")
        return None


if __name__ == "__main__":
    # Calculate and save market breadth
    breadth_df = calculate_market_breadth_history()
    
    if breadth_df is not None:
        # Test loading
        print("\n\nTesting load function...")
        loaded_df = load_market_breadth()
        if loaded_df is not None:
            print(f"✅ Successfully loaded {len(loaded_df)} records")