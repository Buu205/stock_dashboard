"""
Market Overview Page with OHLCV Charts
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from typing import List, Dict, Any

# Add parent directory to path for imports
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

# Import OHLCV components
from src.data.connectors import OHLCVVisualizer, OHLCVUpdater, MarketBreadthCache
from src.utils.formatters import format_number, format_percentage
import csv


def load_tickers_from_csv():
    """Load all tickers from filtered_tickers_summary.csv"""
    csv_path = Path(__file__).parent.parent / "Database" / "Full_database" / "filtered_tickers_summary.csv"
    tickers = []
    ticker_info = {}
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ticker = row['ticker']
                tickers.append(ticker)
                ticker_info[ticker] = {
                    'sector': row['sector'],
                    'record_count': row['record_count'],
                    'years': row['years'],
                    'quarters': row['quarters']
                }
    except Exception as e:
        st.error(f"Error loading tickers from CSV: {e}")
        return [], {}
    
    return tickers, ticker_info


def main():
    st.set_page_config(
        page_title="Market Overview",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Market Overview")
    st.caption("Real-time OHLCV data with technical indicators")
    
    # Initialize components
    viz = OHLCVVisualizer()
    updater = OHLCVUpdater()
    
    # Load all tickers from CSV
    all_tickers, ticker_info = load_tickers_from_csv()
    if not all_tickers:
        st.warning("No tickers loaded. Using default ticker list.")
        all_tickers = updater.tickers
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Settings")
        
        # Time period selection
        days_options = {
            "1 Week": 7,
            "1 Month": 30,
            "3 Months": 90,
            "6 Months": 180,
            "1 Year": 365,
            "2 Years": 730,
            "5 Years": 1825
        }
        selected_period = st.selectbox(
            "Time Period",
            options=list(days_options.keys()),
            index=2  # Default to 3 months
        )
        days = days_options[selected_period]
        
        # Display options
        st.subheader("📈 Chart Options")
        show_ema = st.checkbox("Show EMA 9/21", value=True)
        show_sma = st.checkbox("Show SMA 50/200", value=False)
        show_volume = st.checkbox("Show Volume", value=True)
        
        # Update data button
        st.divider()
        if st.button("🔄 Update Data", use_container_width=True):
            with st.spinner("Updating OHLCV data..."):
                # Update top tickers
                top_tickers = all_tickers[:10] if all_tickers else updater.tickers[:10]
                updater.update_selected(top_tickers)
                st.success("Data updated successfully!")
    
    # Main content area
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Individual Charts", 
        "📈 Comparison", 
        "🌍 Market Breadth",
        "🔝 Top Movers"
    ])
    
    with tab1:
        st.header("Individual Stock Charts")
        
        # Add search functionality
        search_col, filter_col = st.columns([2, 1])
        with search_col:
            search_term = st.text_input("🔍 Search ticker (e.g., VNM, HPG, VIC)", "")
        
        with filter_col:
            selected_sector = "All"
            if ticker_info:
                sectors = ["All"] + sorted(list(set(info['sector'] for info in ticker_info.values())))
                selected_sector = st.selectbox("Filter by sector", sectors)
        
        # Filter tickers based on search and sector
        available_tickers = all_tickers if all_tickers else updater.tickers
        filtered_tickers = available_tickers
        
        if search_term:
            filtered_tickers = [t for t in filtered_tickers if search_term.upper() in t.upper()]
        
        if ticker_info and selected_sector != "All":
            filtered_tickers = [t for t in filtered_tickers if ticker_info.get(t, {}).get('sector') == selected_sector]
        
        # Stock selector
        col1, col2 = st.columns([3, 1])
        with col1:
            # Add sector info to ticker display if available
            if ticker_info and filtered_tickers:
                ticker_display = [f"{t} - {ticker_info.get(t, {}).get('sector', 'N/A')}" for t in filtered_tickers]
                selected_index = st.selectbox(
                    f"Select Stock ({len(filtered_tickers)} of {len(available_tickers)} available)",
                    options=range(len(filtered_tickers)),
                    format_func=lambda x: ticker_display[x] if x < len(ticker_display) else "",
                    index=0 if filtered_tickers else None
                )
                selected_ticker = filtered_tickers[selected_index] if filtered_tickers and selected_index is not None else None
            else:
                selected_ticker = st.selectbox(
                    f"Select Stock ({len(filtered_tickers)} of {len(available_tickers)} available)",
                    options=filtered_tickers,
                    index=0 if filtered_tickers else None
                )
        
        with col2:
            if st.button("📊 Generate Chart", use_container_width=True):
                st.session_state['generate_chart'] = True
        
        # Display chart
        if selected_ticker and st.session_state.get('generate_chart', False):
            with st.spinner(f"Loading chart for {selected_ticker}..."):
                fig = viz.create_candlestick_chart(
                    selected_ticker,
                    days=days,
                    show_ema=show_ema,
                    show_sma=show_sma,
                    show_volume=show_volume
                )
                
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Display latest statistics
                    df = updater.get_ticker_data(selected_ticker)
                    if not df.empty:
                        latest = df.iloc[-1]
                        prev = df.iloc[-2] if len(df) > 1 else latest
                        
                        col1, col2, col3, col4, col5 = st.columns(5)
                        
                        with col1:
                            st.metric(
                                "Close",
                                f"{latest['close']:,.0f}",
                                f"{((latest['close'] - prev['close']) / prev['close'] * 100):.2f}%"
                            )
                        
                        with col2:
                            st.metric(
                                "Volume",
                                format_number(latest['volume']),
                                f"{((latest['volume'] - prev['volume']) / prev['volume'] * 100):.1f}%"
                            )
                        
                        with col3:
                            st.metric(
                                "High",
                                f"{latest['high']:,.0f}"
                            )
                        
                        with col4:
                            st.metric(
                                "Low",
                                f"{latest['low']:,.0f}"
                            )
                        
                        with col5:
                            daily_change = (latest['close'] - latest['open']) / latest['open'] * 100
                            st.metric(
                                "Daily Change",
                                f"{daily_change:.2f}%"
                            )
                else:
                    st.error(f"No data available for {selected_ticker}")
    
    with tab2:
        st.header("Stock Comparison")
        
        # Multi-select for comparison
        col1, col2 = st.columns([3, 1])
        with col1:
            # Use all tickers for comparison
            comparison_tickers = all_tickers if all_tickers else updater.tickers
            default_tickers = comparison_tickers[:3] if len(comparison_tickers) >= 3 else comparison_tickers
            
            compare_tickers = st.multiselect(
                f"Select stocks to compare (max 5, {len(comparison_tickers)} available)",
                options=comparison_tickers,
                default=default_tickers,
                max_selections=5
            )
        
        with col2:
            if st.button("📊 Compare", use_container_width=True):
                st.session_state['compare_stocks'] = True
        
        if compare_tickers and st.session_state.get('compare_stocks', False):
            with st.spinner("Creating comparison chart..."):
                fig = viz.create_multi_ticker_chart(compare_tickers, days)
                
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Performance table
                    st.subheader("Performance Comparison")
                    
                    perf_data = []
                    for ticker in compare_tickers:
                        df = updater.get_ticker_data(ticker)
                        if not df.empty and len(df) > days:
                            # Filter to requested days
                            end_date = df.index.max()
                            start_date = end_date - timedelta(days=days)
                            df_period = df[df.index >= start_date]
                            
                            if not df_period.empty:
                                start_price = df_period.iloc[0]['close']
                                end_price = df_period.iloc[-1]['close']
                                change = (end_price - start_price) / start_price * 100
                                
                                perf_data.append({
                                    'Ticker': ticker,
                                    'Start Price': f"{start_price:,.0f}",
                                    'Current Price': f"{end_price:,.0f}",
                                    'Change (%)': f"{change:.2f}%"
                                })
                    
                    if perf_data:
                        perf_df = pd.DataFrame(perf_data)
                        st.dataframe(perf_df, use_container_width=True, hide_index=True)
                else:
                    st.error("Failed to create comparison chart")
    
    with tab3:
        st.header("Market Breadth Analysis")
        
        col1, col2 = st.columns([2, 1])
        with col2:
            # Get total number of tickers
            total_tickers = len(all_tickers) if all_tickers else len(updater.tickers)
            
            # Options including "All" for all tickers
            analyze_options = [20, 50, 100, 200, "All"]
            analyze_count = st.selectbox(
                "Number of stocks to analyze",
                options=analyze_options,
                index=len(analyze_options) - 1  # Default to "All"
            )
            
            # Add trading value filter option
            st.divider()
            st.caption("📊 Filter Settings")
            min_trading_value = st.select_slider(
                "Min Trading Value (Billion VND)",
                options=[0, 1, 3, 5, 10, 20],
                value=3,
                help="Only analyze stocks with daily trading value above this threshold"
            )
            
            if st.button("🔍 Analyze Market", use_container_width=True):
                st.session_state['analyze_market'] = True
                st.session_state['min_trading_value'] = min_trading_value
        
        if st.session_state.get('analyze_market', False):
            # Determine actual count
            available_for_analysis = all_tickers if all_tickers else updater.tickers
            if analyze_count == "All":
                symbols = available_for_analysis
                count_text = f"all {len(symbols)}"
            else:
                symbols = available_for_analysis[:analyze_count]
                count_text = f"top {analyze_count}"
            
            # Check cache first
            cache = MarketBreadthCache()
            stats = None
            
            # Try to use cached data for "All" option
            if analyze_count == "All":
                with st.spinner("Checking cache..."):
                    stats = cache.get_cached_stats(max_age_hours=1)
                    if stats:
                        st.success("Using cached data (updated within last hour)")
            
            # If no cache or not using "All", calculate fresh
            if not stats:
                # Use progress bar for all analysis
                progress_bar = st.progress(0)
                progress_text = st.empty()
                
                def update_progress(value):
                    progress_bar.progress(value)
                    progress_text.text(f"Analyzing {count_text} stocks... {int(value * 100)}%")
                
                # Use optimized parallel processing
                progress_text.text(f"Starting analysis of {count_text} stocks...")
                
                # Get min trading value from session state
                min_value = st.session_state.get('min_trading_value', 3) * 1_000_000_000
                
                # Increase batch size for larger datasets
                batch_size = 20 if len(symbols) > 100 else 10
                stats = viz.analyze_market_breadth(
                    symbols, 
                    progress_callback=update_progress if len(symbols) > 50 else None,
                    batch_size=batch_size,
                    min_trading_value=min_value
                )
                
                # Cache the results if analyzing all stocks
                if analyze_count == "All" and stats:
                    cache.save_stats(stats)
                
                progress_bar.empty()
                progress_text.empty()
            
            if stats and stats['total'] > 0:
                # Show detailed summary
                filtered_count = stats.get('filtered_out', 0)
                failed_count = len(stats.get('failed', []))
                analyzed_count = stats['total']
                
                summary_msg = f"📊 Analyzed {analyzed_count} stocks out of {len(symbols)} requested\n"
                if filtered_count > 0:
                    summary_msg += f"🔸 {filtered_count} stocks filtered (trading value < 3B VND)\n"
                if failed_count > 0:
                    summary_msg += f"🔸 {failed_count} stocks failed to load"
                
                st.info(summary_msg)
                
                # Display breadth metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    pct_ma20 = stats.get('pct_above_ma20', 0)
                    st.metric(
                        "Above MA20",
                        f"{stats['above_ma20']}/{stats['total']}",
                        f"{pct_ma20:.1f}%"
                    )
                    st.progress(pct_ma20 / 100)
                
                with col2:
                    pct_ma50 = stats.get('pct_above_ma50', 0)
                    st.metric(
                        "Above MA50",
                        f"{stats['above_ma50']}/{stats['total']}",
                        f"{pct_ma50:.1f}%"
                    )
                    st.progress(pct_ma50 / 100)
                
                with col3:
                    pct_ma200 = stats.get('pct_above_ma200', 0)
                    st.metric(
                        "Above MA200",
                        f"{stats['above_ma200']}/{stats['total']}",
                        f"{pct_ma200:.1f}%"
                    )
                    st.progress(pct_ma200 / 100)
                
                with col4:
                    pct_ema_bullish = stats.get('pct_ema_bullish', 0)
                    st.metric(
                        "EMA9 > EMA21",
                        f"{stats['ema9_above_ema21']}/{stats['total']}",
                        f"{pct_ema_bullish:.1f}%"
                    )
                    st.progress(pct_ema_bullish / 100)
                
                # Market sentiment indicator
                st.divider()
                st.subheader("Market Sentiment")
                
                avg_breadth = (pct_ma20 + pct_ma50 + pct_ma200 + pct_ema_bullish) / 4
                
                if avg_breadth >= 70:
                    sentiment = "🟢 **Bullish**"
                    sentiment_color = "green"
                elif avg_breadth >= 50:
                    sentiment = "🟡 **Neutral**"
                    sentiment_color = "yellow"
                elif avg_breadth >= 30:
                    sentiment = "🟠 **Cautious**"
                    sentiment_color = "orange"
                else:
                    sentiment = "🔴 **Bearish**"
                    sentiment_color = "red"
                
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.markdown(f"### {sentiment}")
                with col2:
                    st.progress(avg_breadth / 100)
                    st.caption(f"Average Breadth: {avg_breadth:.1f}%")
                
                # Historical Market Breadth Chart  
                st.divider()
                st.subheader("📈 Historical Market Breadth (1 Year)")
                
                # Load pre-calculated market breadth data
                with st.spinner("Loading historical market breadth..."):
                    try:
                        import sqlite3
                        conn = sqlite3.connect('Database/cache/market_breadth.db')
                        historical_breadth = pd.read_sql_query(
                            'SELECT * FROM market_breadth ORDER BY date', 
                            conn
                        )
                        conn.close()
                        
                        if not historical_breadth.empty:
                            historical_breadth['date'] = pd.to_datetime(historical_breadth['date'])
                            historical_breadth.set_index('date', inplace=True)
                        else:
                            historical_breadth = None
                    except Exception as e:
                        st.error(f"Error loading market breadth data: {e}")
                        historical_breadth = None
                    
                    if historical_breadth is not None:
                        # Create line chart
                        fig_breadth = go.Figure()
                        
                        # Add MA20 line
                        fig_breadth.add_trace(go.Scatter(
                            x=historical_breadth.index,
                            y=historical_breadth['pct_above_ma20'],
                            mode='lines',
                            name='% Above MA20',
                            line=dict(color='blue', width=2),
                            hovertemplate='MA20: %{y:.1f}%<br>Date: %{x|%Y-%m-%d}<extra></extra>'
                        ))
                        
                        # Add MA50 line
                        fig_breadth.add_trace(go.Scatter(
                            x=historical_breadth.index,
                            y=historical_breadth['pct_above_ma50'],
                            mode='lines',
                            name='% Above MA50',
                            line=dict(color='orange', width=2),
                            hovertemplate='MA50: %{y:.1f}%<br>Date: %{x|%Y-%m-%d}<extra></extra>'
                        ))
                        
                        # Add reference lines
                        fig_breadth.add_hline(y=70, line_dash="dash", line_color="green", 
                                            annotation_text="Bullish (70%)", annotation_position="left")
                        fig_breadth.add_hline(y=50, line_dash="dash", line_color="gray", 
                                            annotation_text="Neutral (50%)", annotation_position="left")
                        fig_breadth.add_hline(y=30, line_dash="dash", line_color="red", 
                                            annotation_text="Bearish (30%)", annotation_position="left")
                        
                        # Update layout
                        fig_breadth.update_layout(
                            title=f"Market Breadth Trend (Filtered: {len(stats['analyzed'])} stocks, Min value: {st.session_state.get('min_trading_value', 3)}B VND)",
                            xaxis_title="Date",
                            yaxis_title="Percentage (%)",
                            height=400,
                            hovermode='x unified',
                            showlegend=True,
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1
                            ),
                            yaxis=dict(range=[0, 100])
                        )
                        
                        st.plotly_chart(fig_breadth, use_container_width=True)
                        
                        # Show statistics
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            current_ma20 = historical_breadth['pct_above_ma20'].iloc[-1]
                            avg_ma20 = historical_breadth['pct_above_ma20'].mean()
                            st.metric("Current MA20", f"{current_ma20:.1f}%", 
                                    f"{current_ma20 - avg_ma20:.1f}% vs avg")
                        with col2:
                            current_ma50 = historical_breadth['pct_above_ma50'].iloc[-1]
                            avg_ma50 = historical_breadth['pct_above_ma50'].mean()
                            st.metric("Current MA50", f"{current_ma50:.1f}%",
                                    f"{current_ma50 - avg_ma50:.1f}% vs avg")
                        with col3:
                            max_ma20 = historical_breadth['pct_above_ma20'].max()
                            st.metric("Max MA20 (1Y)", f"{max_ma20:.1f}%")
                        with col4:
                            min_ma20 = historical_breadth['pct_above_ma20'].min()
                            st.metric("Min MA20 (1Y)", f"{min_ma20:.1f}%")
                    else:
                        st.info("Not enough data to calculate historical breadth. Need at least 5 stocks with sufficient history.")
                
                # Analyzed stocks list
                with st.expander("📋 Stock Details"):
                    tab_analyzed, tab_filtered = st.tabs(["✅ Analyzed", "❌ Filtered Out"])
                    
                    with tab_analyzed:
                        if stats['analyzed']:
                            analyzed_str = ", ".join(sorted(stats['analyzed']))
                            st.text(f"Total: {len(stats['analyzed'])} stocks")
                            st.text_area("Symbols:", analyzed_str, height=100)
                        else:
                            st.info("No stocks analyzed")
                    
                    with tab_filtered:
                        low_value = stats.get('low_value_stocks', [])
                        if low_value:
                            st.text(f"Filtered by low trading value: {len(low_value)} stocks")
                            st.text_area("Low value stocks:", ", ".join(sorted(low_value)), height=100)
                        else:
                            st.info("No stocks filtered out")
            else:
                st.warning("No data available for market breadth analysis")
    
    with tab4:
        st.header("Top Movers")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🚀 Top Gainers")
            
            # Calculate top gainers
            gainers = []
            # Use more tickers for comprehensive analysis
            movers_tickers = all_tickers[:100] if all_tickers else updater.tickers[:50]
            for ticker in movers_tickers:
                df = updater.get_ticker_data(ticker)
                if not df.empty and len(df) > 1:
                    latest = df.iloc[-1]
                    prev = df.iloc[-2]
                    change = (latest['close'] - prev['close']) / prev['close'] * 100
                    gainers.append({
                        'Ticker': ticker,
                        'Price': f"{latest['close']:,.0f}",
                        'Change': f"{change:.2f}%",
                        'Volume': format_number(latest['volume'])
                    })
            
            # Sort and display top 5 gainers
            gainers_sorted = sorted(gainers, key=lambda x: float(x['Change'].strip('%')), reverse=True)[:5]
            if gainers_sorted:
                gainers_df = pd.DataFrame(gainers_sorted)
                st.dataframe(gainers_df, use_container_width=True, hide_index=True)
            else:
                st.info("No gainers data available")
        
        with col2:
            st.subheader("📉 Top Losers")
            
            # Sort and display top 5 losers
            losers_sorted = sorted(gainers, key=lambda x: float(x['Change'].strip('%')))[:5]
            if losers_sorted:
                losers_df = pd.DataFrame(losers_sorted)
                st.dataframe(losers_df, use_container_width=True, hide_index=True)
            else:
                st.info("No losers data available")
        
        # Volume leaders
        st.divider()
        st.subheader("📊 Volume Leaders")
        
        volume_leaders = []
        # Use same tickers as gainers/losers for consistency
        for ticker in movers_tickers:
            df = updater.get_ticker_data(ticker)
            if not df.empty:
                latest = df.iloc[-1]
                avg_volume = df['volume'].rolling(20).mean().iloc[-1] if len(df) > 20 else df['volume'].mean()
                volume_ratio = latest['volume'] / avg_volume if avg_volume > 0 else 0
                
                volume_leaders.append({
                    'Ticker': ticker,
                    'Price': f"{latest['close']:,.0f}",
                    'Volume': format_number(latest['volume']),
                    'Avg Volume': format_number(avg_volume),
                    'Volume Ratio': f"{volume_ratio:.1f}x"
                })
        
        # Sort by volume ratio
        volume_sorted = sorted(volume_leaders, key=lambda x: float(x['Volume Ratio'].strip('x')), reverse=True)[:10]
        if volume_sorted:
            volume_df = pd.DataFrame(volume_sorted)
            st.dataframe(volume_df, use_container_width=True, hide_index=True)
        else:
            st.info("No volume data available")


if __name__ == "__main__":
    main()