#!/usr/bin/env python3
"""
OHLCV Visualization with Candlestick Chart and EMA indicators
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

# Import from same directory
from .update_ohlcv_data import OHLCVUpdater

class OHLCVVisualizer:
    """Create interactive OHLCV charts with technical indicators"""
    
    def __init__(self):
        """Initialize visualizer"""
        self.updater = OHLCVUpdater()
        
    def calculate_ema(self, prices: pd.Series, period: int) -> pd.Series:
        """
        Calculate Exponential Moving Average
        
        Args:
            prices: Series of prices (typically close prices)
            period: EMA period
            
        Returns:
            Series with EMA values
        """
        return prices.ewm(span=period, adjust=False).mean()
    
    def calculate_sma(self, prices: pd.Series, period: int) -> pd.Series:
        """
        Calculate Simple Moving Average
        
        Args:
            prices: Series of prices
            period: SMA period
            
        Returns:
            Series with SMA values
        """
        return prices.rolling(window=period).mean()
    
    def find_ema_crossovers(self, ema_short: pd.Series, ema_long: pd.Series) -> tuple:
        """
        Find EMA crossover points
        
        Args:
            ema_short: Short period EMA
            ema_long: Long period EMA
            
        Returns:
            Tuple of (golden_crosses, death_crosses) as DataFrames
        """
        # Calculate the difference
        diff = ema_short - ema_long
        
        # Find sign changes
        sign_change = np.sign(diff).diff()
        
        # Golden cross (short crosses above long)
        golden_crosses = sign_change > 0
        
        # Death cross (short crosses below long)
        death_crosses = sign_change < 0
        
        return golden_crosses, death_crosses
    
    def create_candlestick_chart(self, 
                                symbol: str,
                                days: int = 180,
                                show_ema: bool = True,
                                show_sma: bool = False,
                                show_volume: bool = True) -> go.Figure:
        """
        Create interactive candlestick chart with indicators
        
        Args:
            symbol: Stock symbol
            days: Number of days to display
            show_ema: Show EMA 9 and EMA 21
            show_sma: Show SMA 50 and SMA 200
            show_volume: Show volume chart
            
        Returns:
            Plotly figure object
        """
        # Get OHLCV data
        df = self.updater.get_ticker_data(symbol)
        
        if df.empty:
            print(f"No data available for {symbol}")
            return None
        
        # Filter to requested days
        end_date = df.index.max()
        start_date = end_date - timedelta(days=days)
        df = df[df.index >= start_date]
        
        # Calculate indicators
        df['EMA9'] = self.calculate_ema(df['close'], 9)
        df['EMA21'] = self.calculate_ema(df['close'], 21)
        df['SMA50'] = self.calculate_sma(df['close'], 50)
        df['SMA200'] = self.calculate_sma(df['close'], 200)
        
        # Find crossovers
        golden_crosses, death_crosses = self.find_ema_crossovers(df['EMA9'], df['EMA21'])
        
        # Create subplots
        if show_volume:
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                subplot_titles=(f'{symbol} - Candlestick Chart', 'Volume'),
                row_heights=[0.7, 0.3]
            )
        else:
            fig = make_subplots(
                rows=1, cols=1,
                subplot_titles=(f'{symbol} - Candlestick Chart',)
            )
        
        # Add candlestick chart
        fig.add_trace(
            go.Candlestick(
                x=df.index,
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name='OHLC',
                increasing_line_color='green',
                decreasing_line_color='red'
            ),
            row=1, col=1
        )
        
        # Add EMA lines
        if show_ema:
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df['EMA9'],
                    name='EMA 9',
                    line=dict(color='blue', width=1),
                    opacity=0.8
                ),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df['EMA21'],
                    name='EMA 21',
                    line=dict(color='orange', width=1),
                    opacity=0.8
                ),
                row=1, col=1
            )
            
            # Add crossover markers
            golden_df = df[golden_crosses]
            if not golden_df.empty:
                fig.add_trace(
                    go.Scatter(
                        x=golden_df.index,
                        y=golden_df['EMA9'],
                        mode='markers',
                        name='Golden Cross',
                        marker=dict(
                            color='gold',
                            size=12,
                            symbol='triangle-up'
                        )
                    ),
                    row=1, col=1
                )
            
            death_df = df[death_crosses]
            if not death_df.empty:
                fig.add_trace(
                    go.Scatter(
                        x=death_df.index,
                        y=death_df['EMA9'],
                        mode='markers',
                        name='Death Cross',
                        marker=dict(
                            color='black',
                            size=12,
                            symbol='triangle-down'
                        )
                    ),
                    row=1, col=1
                )
        
        # Add SMA lines
        if show_sma:
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df['SMA50'],
                    name='SMA 50',
                    line=dict(color='purple', width=1, dash='dash'),
                    opacity=0.6
                ),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=df['SMA200'],
                    name='SMA 200',
                    line=dict(color='gray', width=1, dash='dash'),
                    opacity=0.6
                ),
                row=1, col=1
            )
        
        # Add volume chart
        if show_volume:
            colors = ['red' if row['close'] < row['open'] else 'green' 
                     for _, row in df.iterrows()]
            
            fig.add_trace(
                go.Bar(
                    x=df.index,
                    y=df['volume'],
                    name='Volume',
                    marker_color=colors,
                    opacity=0.5
                ),
                row=2, col=1
            )
        
        # Update layout
        fig.update_layout(
            title=f"{symbol} - OHLCV Chart with EMA Indicators",
            xaxis_title="Date",
            yaxis_title="Price (VND)",
            template="plotly_dark",
            height=800,
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            ),
            hovermode='x unified'
        )
        
        # Update x-axis to hide weekends
        fig.update_xaxes(
            rangeslider_visible=False,
            rangebreaks=[
                dict(bounds=["sat", "mon"])  # Hide weekends
            ]
        )
        
        # Update y-axis for volume
        if show_volume:
            fig.update_yaxes(title_text="Volume", row=2, col=1)
        
        return fig
    
    def create_multi_ticker_chart(self, symbols: list, days: int = 90) -> go.Figure:
        """
        Create comparison chart for multiple tickers
        
        Args:
            symbols: List of stock symbols
            days: Number of days to display
            
        Returns:
            Plotly figure object
        """
        fig = go.Figure()
        
        for symbol in symbols:
            df = self.updater.get_ticker_data(symbol)
            
            if df.empty:
                print(f"No data for {symbol}")
                continue
            
            # Filter to requested days
            end_date = df.index.max()
            start_date = end_date - timedelta(days=days)
            df = df[df.index >= start_date]
            
            # Normalize prices (percentage change from first day)
            normalized = (df['close'] / df['close'].iloc[0] - 1) * 100
            
            fig.add_trace(
                go.Scatter(
                    x=df.index,
                    y=normalized,
                    name=symbol,
                    mode='lines'
                )
            )
        
        fig.update_layout(
            title=f"Price Comparison - {', '.join(symbols)}",
            xaxis_title="Date",
            yaxis_title="Percentage Change (%)",
            template="plotly_white",
            height=600,
            hovermode='x unified'
        )
        
        return fig
    
    def analyze_market_breadth(self, symbols: list = None, progress_callback=None, batch_size: int = 10, min_trading_value: float = 3_000_000_000) -> dict:
        """
        Analyze market breadth based on MA positions (optimized version)
        
        Args:
            symbols: List of symbols to analyze (None for all)
            progress_callback: Optional callback for progress updates
            batch_size: Number of symbols to process in parallel
            min_trading_value: Minimum trading value in VND (default 3 billion)
            
        Returns:
            Dictionary with market breadth statistics
        """
        import concurrent.futures
        from functools import partial
        
        if symbols is None:
            symbols = self.updater.tickers  # Use all tickers if not specified
        
        stats = {
            'above_ma20': 0,
            'above_ma50': 0,
            'above_ma200': 0,
            'ema9_above_ema21': 0,
            'total': 0,
            'analyzed': [],
            'failed': []
        }
        
        def analyze_single_symbol(symbol, min_val):
            """Analyze a single symbol - can be run in parallel"""
            try:
                df = self.updater.get_ticker_data(symbol)
                
                # More flexible: require at least 20 days instead of 200
                if df.empty or len(df) < 20:
                    return None
                
                # Get latest values
                latest = df.iloc[-1]
                close = latest['close']
                volume = latest.get('volume', 0)
                
                # Calculate trading value (in VND)
                trading_value = close * volume
                
                # Filter by trading value threshold
                if trading_value < min_val:
                    return {'symbol': symbol, 'filtered_out': True, 'reason': 'low_trading_value'}
                
                # Calculate MAs more efficiently (handle insufficient data)
                close_series = df['close']
                data_len = len(df)
                
                # Calculate MAs only if we have enough data
                ma20 = close_series.rolling(20).mean().iloc[-1] if data_len >= 20 else pd.NA
                ma50 = close_series.rolling(50).mean().iloc[-1] if data_len >= 50 else pd.NA
                ma200 = close_series.rolling(200).mean().iloc[-1] if data_len >= 200 else pd.NA
                
                # Calculate EMAs (need at least 21 days for EMA21)
                ema9 = close_series.ewm(span=9, adjust=False).mean().iloc[-1] if data_len >= 9 else pd.NA
                ema21 = close_series.ewm(span=21, adjust=False).mean().iloc[-1] if data_len >= 21 else pd.NA
                
                result = {
                    'symbol': symbol,
                    'above_ma20': not pd.isna(ma20) and close > ma20,
                    'above_ma50': not pd.isna(ma50) and close > ma50,
                    'above_ma200': not pd.isna(ma200) and close > ma200,
                    'ema9_above_ema21': not pd.isna(ema9) and not pd.isna(ema21) and ema9 > ema21
                }
                return result
            except Exception as e:
                return {'symbol': symbol, 'error': str(e)}
        
        # Process symbols in batches for better performance
        total_symbols = len(symbols)
        processed = 0
        filtered_out_count = 0
        low_value_stocks = []
        
        # Use ThreadPoolExecutor for I/O-bound operations
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(batch_size, 20)) as executor:
            # Submit all tasks with min_trading_value
            future_to_symbol = {executor.submit(analyze_single_symbol, symbol, min_trading_value): symbol 
                              for symbol in symbols}
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                processed += 1
                
                # Update progress if callback provided
                if progress_callback:
                    progress_callback(processed / total_symbols)
                
                try:
                    result = future.result(timeout=5)  # 5 second timeout per symbol
                    if result:
                        if result.get('filtered_out'):
                            filtered_out_count += 1
                            if result.get('reason') == 'low_trading_value':
                                low_value_stocks.append(symbol)
                        elif 'error' not in result:
                            stats['above_ma20'] += result['above_ma20']
                            stats['above_ma50'] += result['above_ma50']
                            stats['above_ma200'] += result['above_ma200']
                            stats['ema9_above_ema21'] += result['ema9_above_ema21']
                            stats['total'] += 1
                            stats['analyzed'].append(result['symbol'])
                        else:
                            stats['failed'].append(symbol)
                except Exception as e:
                    stats['failed'].append(symbol)
        
        # Add filtered count to stats
        stats['filtered_out'] = filtered_out_count
        stats['low_value_stocks'] = low_value_stocks
        
        # Calculate percentages
        if stats['total'] > 0:
            stats['pct_above_ma20'] = (stats['above_ma20'] / stats['total']) * 100
            stats['pct_above_ma50'] = (stats['above_ma50'] / stats['total']) * 100
            stats['pct_above_ma200'] = (stats['above_ma200'] / stats['total']) * 100
            stats['pct_ema_bullish'] = (stats['ema9_above_ema21'] / stats['total']) * 100
        
        return stats
    
    def calculate_historical_breadth(self, symbols: list, days: int = 365, min_trading_value: float = 0) -> pd.DataFrame:
        """
        Calculate historical market breadth over time
        
        Args:
            symbols: List of stock symbols to analyze
            days: Number of days of history to calculate
            min_trading_value: Minimum trading value filter
            
        Returns:
            DataFrame with dates as index and breadth percentages as columns
        """
        import concurrent.futures
        from datetime import datetime, timedelta
        
        try:
            # Limit symbols to reasonable number for performance
            if len(symbols) > 100:
                symbols = symbols[:100]
            
            # Initialize result dictionary
            date_stats = {}
            
            def get_symbol_data(symbol):
                """Get historical data for a single symbol"""
                try:
                    df = self.updater.get_ticker_data(symbol, days=days)
                    if df.empty or len(df) < 20:  # Need at least 20 days for MA20
                        return None
                        
                    # Calculate moving averages
                    df['ma20'] = df['close'].rolling(window=20, min_periods=20).mean()
                    df['ma50'] = df['close'].rolling(window=50, min_periods=50).mean()
                    
                    # Calculate trading value
                    df['trading_value'] = df['close'] * df['volume']
                    
                    return {'symbol': symbol, 'data': df}
                except Exception as e:
                    print(f"Error processing {symbol}: {e}")
                    return None
            
            # Fetch all symbol data in parallel with smaller batch
            symbol_data = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(get_symbol_data, symbol): symbol for symbol in symbols[:50]}  # Limit to 50 symbols
                for future in concurrent.futures.as_completed(futures):
                    try:
                        result = future.result(timeout=5)
                        if result:
                            symbol_data.append(result)
                    except:
                        continue
            
            if not symbol_data:
                print(f"No valid symbol data found")
                return pd.DataFrame()
            
            print(f"Processing {len(symbol_data)} symbols for historical breadth")
            
            # Get date range from last year
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Create date range
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # Calculate breadth for each date
            for date in date_range:
                above_ma20 = 0
                above_ma50 = 0
                total_valid = 0
                
                for item in symbol_data:
                    df = item['data']
                    # Find closest date in data
                    if len(df) > 0:
                        try:
                            # Get the closest date
                            closest_idx = df.index.get_indexer([date], method='nearest')[0]
                            if closest_idx >= 0 and closest_idx < len(df):
                                row = df.iloc[closest_idx]
                                
                                # Check if date is close enough (within 3 days)
                                date_diff = abs((df.index[closest_idx] - date).days)
                                if date_diff > 3:
                                    continue
                                
                                # Check trading value filter
                                if min_trading_value > 0 and row['trading_value'] < min_trading_value:
                                    continue
                                
                                # Check MA conditions
                                if not pd.isna(row.get('ma20', np.nan)):
                                    total_valid += 1
                                    if row['close'] > row['ma20']:
                                        above_ma20 += 1
                                    if not pd.isna(row.get('ma50', np.nan)) and row['close'] > row['ma50']:
                                        above_ma50 += 1
                        except:
                            continue
                
                if total_valid >= 10:  # Need at least 10 valid stocks
                    date_stats[date] = {
                        'pct_above_ma20': float((above_ma20 / total_valid) * 100),
                        'pct_above_ma50': float((above_ma50 / total_valid) * 100),
                        'total_stocks': int(total_valid)
                    }
            
            # Convert to DataFrame
            if date_stats:
                df_breadth = pd.DataFrame.from_dict(date_stats, orient='index')
                df_breadth.index = pd.to_datetime(df_breadth.index)
                df_breadth = df_breadth.sort_index()
                
                # Smooth the data with 5-day moving average
                df_breadth['pct_above_ma20'] = df_breadth['pct_above_ma20'].rolling(window=5, min_periods=1).mean()
                df_breadth['pct_above_ma50'] = df_breadth['pct_above_ma50'].rolling(window=5, min_periods=1).mean()
                
                print(f"Generated breadth data with {len(df_breadth)} days")
                return df_breadth
            
            print("No valid date stats generated")
            return pd.DataFrame()
            
        except Exception as e:
            print(f"Error in calculate_historical_breadth: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()


def main():
    """Main function for testing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Visualize OHLCV data')
    parser.add_argument('symbol', help='Stock symbol to visualize')
    parser.add_argument('--days', type=int, default=180, help='Number of days to display')
    parser.add_argument('--compare', nargs='+', help='Compare with other symbols')
    parser.add_argument('--breadth', action='store_true', help='Analyze market breadth')
    
    args = parser.parse_args()
    
    # Initialize visualizer
    viz = OHLCVVisualizer()
    
    if args.breadth:
        # Analyze market breadth
        print("\n📊 Market Breadth Analysis")
        print("="*60)
        
        stats = viz.analyze_market_breadth()
        
        print(f"\nAnalyzed {stats['total']} stocks:")
        print(f"  Above MA20: {stats['above_ma20']} ({stats.get('pct_above_ma20', 0):.1f}%)")
        print(f"  Above MA50: {stats['above_ma50']} ({stats.get('pct_above_ma50', 0):.1f}%)")
        print(f"  Above MA200: {stats['above_ma200']} ({stats.get('pct_above_ma200', 0):.1f}%)")
        print(f"  EMA9 > EMA21: {stats['ema9_above_ema21']} ({stats.get('pct_ema_bullish', 0):.1f}%)")
    
    elif args.compare:
        # Create comparison chart
        symbols = [args.symbol] + args.compare
        fig = viz.create_multi_ticker_chart(symbols, args.days)
        if fig:
            fig.show()
    
    else:
        # Create single ticker chart
        fig = viz.create_candlestick_chart(
            args.symbol,
            days=args.days,
            show_ema=True,
            show_sma=True,
            show_volume=True
        )
        
        if fig:
            fig.show()
            print(f"\n✅ Chart created for {args.symbol}")
        else:
            print(f"\n❌ Failed to create chart for {args.symbol}")


if __name__ == "__main__":
    main()