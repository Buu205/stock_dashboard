# -*- coding: utf-8 -*-
"""
Technical Analysis Dashboard
Interactive charts and technical indicators for stock analysis
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Add src to path
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from data.loaders.market_loader import MarketDataLoader
    from analysis.technical.indicator_analyzer import TechnicalIndicatorAnalyzer
    from analysis.technical.market_breadth import MarketBreadthAnalyzer
    from visualization.technical_charts import TechnicalChartCreator
    MODULES_AVAILABLE = True
except ImportError as e:
    st.error(f"❌ Error importing modules: {str(e)}")
    st.info("Please install required dependencies: pip install -r requirements.txt")
    MODULES_AVAILABLE = False

# Page config
st.set_page_config(
    page_title="Technical Analysis",
    page_icon="📊",
    layout="wide"
)

# Page header
st.title("📊 Technical Analysis Dashboard")
st.markdown("Interactive charts and technical indicators for stock analysis")

if not MODULES_AVAILABLE:
    st.stop()

# Initialize components
@st.cache_resource
def get_market_loader():
    """Get market data loader"""
    return MarketDataLoader()

@st.cache_resource
def get_technical_analyzer():
    """Get technical indicator analyzer"""
    return TechnicalIndicatorAnalyzer()

@st.cache_resource
def get_market_breadth_analyzer():
    """Get market breadth analyzer"""
    return MarketBreadthAnalyzer()

@st.cache_resource
def get_chart_creator():
    """Get chart creator"""
    return TechnicalChartCreator()

# Sidebar
st.sidebar.header("⚙️ Settings")

# Stock selection
stock_symbol = st.sidebar.text_input(
    "Stock Symbol",
    value="HPG",
    help="Enter stock symbol (e.g., HPG, VNM, VCB)"
).upper().strip()

# Time period selection
time_period = st.sidebar.selectbox(
    "Time Period",
    options=["30 days", "60 days", "90 days", "180 days", "1 year"],
    index=0
)

# Convert time period to days
period_days = {
    "30 days": 30,
    "60 days": 60,
    "90 days": 90,
    "180 days": 180,
    "1 year": 365
}

days = period_days[time_period]

# Chart options
show_volume = st.sidebar.checkbox("Show Volume", value=True)
show_indicators = st.sidebar.checkbox("Show Technical Indicators", value=True)

# MA periods
ma_periods = st.sidebar.multiselect(
    "Moving Average Periods",
    options=[9, 20, 50, 100, 200],
    default=[20, 50],
    help="Select MA periods to display"
)

# EMA periods
ema_periods = st.sidebar.multiselect(
    "EMA Periods",
    options=[9, 12, 21, 26],
    default=[9, 21],
    help="Select EMA periods to display"
)

# Main content
if stock_symbol:
    try:
        # Get market data
        with st.spinner(f"📊 Fetching data for {stock_symbol}..."):
            market_loader = get_market_loader()
            
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            data = market_loader.get_stock_ohlcv(stock_symbol, start_date, end_date)
        
        if not data.empty:
            st.success(f"✅ Data loaded successfully for {stock_symbol}")
            
            # Calculate technical indicators
            with st.spinner("🔧 Calculating technical indicators..."):
                technical_analyzer = get_technical_analyzer()
                
                # Calculate MAs
                if ma_periods:
                    data = technical_analyzer.calculate_moving_averages(data, ma_periods)
                
                # Calculate EMAs
                if ema_periods:
                    data = technical_analyzer.calculate_exponential_moving_averages(data, ema_periods)
                
                # Calculate RSI
                data = technical_analyzer.calculate_rsi(data)
                
                # Calculate MACD
                data = technical_analyzer.calculate_macd(data)
                
                # Get trend signals
                trend_signals = technical_analyzer.get_ma_trend_signals(data)
            
            # Display data summary
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                latest_price = data.iloc[-1]['close']
                st.metric("Current Price", f"{latest_price:,.0f} VND")
            
            with col2:
                if len(data) > 1:
                    price_change = ((latest_price - data.iloc[-2]['close']) / data.iloc[-2]['close']) * 100
                    st.metric("Price Change", f"{price_change:+.2f}%")
                else:
                    st.metric("Price Change", "N/A")
            
            with col3:
                if 'RSI' in data.columns:
                    latest_rsi = data.iloc[-1]['RSI']
                    st.metric("RSI", f"{latest_rsi:.1f}")
                else:
                    st.metric("RSI", "N/A")
            
            with col4:
                if 'MA_20' in data.columns:
                    ma20 = data.iloc[-1]['MA_20']
                    ma_signal = "Above MA20" if latest_price > ma20 else "Below MA20"
                    st.metric("MA20 Signal", ma_signal)
                else:
                    st.metric("MA20 Signal", "N/A")
            
            # Create charts
            chart_creator = get_chart_creator()
            
            # Main candlestick chart
            st.subheader("📈 Price Chart with Technical Indicators")
            
            candlestick_fig = chart_creator.create_candlestick_chart(
                data,
                title=f"{stock_symbol} - Technical Analysis",
                show_volume=show_volume,
                show_indicators=show_indicators,
                ma_periods=ma_periods,
                ema_periods=ema_periods
            )
            
            st.plotly_chart(candlestick_fig)
            
            # Technical summary
            if trend_signals:
                st.subheader("🔍 Technical Analysis Summary")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Trend Signals:**")
                    for signal, value in trend_signals.items():
                        st.write(f"• {signal}: {value}")
                
                with col2:
                    st.write("**Current Status:**")
                    if 'EMA_Trend' in trend_signals:
                        ema_trend = trend_signals['EMA_Trend']
                        if 'Bullish' in ema_trend:
                            st.success("🟢 Bullish Trend")
                        else:
                            st.error("🔴 Bearish Trend")
                    
                    if 'MA_Support' in trend_signals:
                        st.info(f"📊 {trend_signals['MA_Support']}")
            
            # Additional charts
            st.subheader("📊 Additional Analysis")
            
            tab1, tab2, tab3 = st.tabs(["Technical Summary", "Volume Analysis", "Data Table"])
            
            with tab1:
                if trend_signals:
                    summary_fig = chart_creator.create_technical_summary_chart(
                        data, trend_signals, f"Technical Summary - {stock_symbol}"
                    )
                    st.plotly_chart(summary_fig)
            
            with tab2:
                if 'volume' in data.columns:
                    # Volume analysis
                    volume_fig = go.Figure()
                    
                    # Volume bars
                    colors = ['#26A69A' if close >= open else '#EF5350' 
                             for close, open in zip(data['close'], data['open'])]
                    
                    volume_fig.add_trace(go.Bar(
                        x=data['date'],
                        y=data['volume'],
                        name="Volume",
                        marker_color=colors,
                        opacity=0.7
                    ))
                    
                    # Volume MA
                    if 'MA_20' in data.columns:
                        volume_ma = data['volume'].rolling(window=20).mean()
                        volume_fig.add_trace(go.Scatter(
                            x=data['date'],
                            y=volume_ma,
                            mode='lines',
                            name="Volume MA20",
                            line=dict(color='orange', width=2)
                        ))
                    
                    volume_fig.update_layout(
                        title=f"Volume Analysis - {stock_symbol}",
                        xaxis_title="Date",
                        yaxis_title="Volume",
                        height=400
                    )
                    
                    st.plotly_chart(volume_fig)
            
            with tab3:
                # Display data table
                st.write("**Latest Data:**")
                display_data = data.tail(10).copy()
                display_data['date'] = display_data['date'].dt.strftime('%Y-%m-%d')
                
                # Format numeric columns
                numeric_columns = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_columns:
                    if col in display_data.columns:
                        display_data[col] = display_data[col].round(2)
                
                st.dataframe(display_data)
                
                # Download button
                csv = data.to_csv(index=False)
                st.download_button(
                    label="📥 Download Data as CSV",
                    data=csv,
                    file_name=f"{stock_symbol}_technical_data.csv",
                    mime="text/csv"
                )
        
        else:
            st.error(f"❌ No data available for {stock_symbol}")
            st.info("Please check the stock symbol and try again")
    
    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")
        st.info("Please check your internet connection and try again")

# Market Breadth Section
st.markdown("---")
st.subheader("🌐 Market Breadth Analysis")

# Market breadth options
col1, col2 = st.columns(2)

with col1:
    analyze_market = st.checkbox("Enable Market Breadth Analysis", value=False)
    
    if analyze_market:
        # Symbol list for market breadth
        symbol_input = st.text_area(
            "Stock Symbols (one per line)",
            value="HPG\nVNM\nVCB\nTCB\nBID\nCTG\nMBB\nACB\nVPB\nSTB",
            help="Enter stock symbols, one per line"
        )
        
        symbols = [s.strip().upper() for s in symbol_input.split('\n') if s.strip()]
        
        breadth_days = st.selectbox(
            "Analysis Period",
            options=[30, 60, 90],
            index=0
        )

with col2:
    if analyze_market and st.button("🚀 Analyze Market Breadth"):
        try:
            with st.spinner("📊 Analyzing market breadth..."):
                market_breadth_analyzer = get_market_breadth_analyzer()
                market_summary = market_breadth_analyzer.get_market_summary(symbols, breadth_days)
            
            if market_summary:
                st.success("✅ Market breadth analysis completed!")
                
                # Display market sentiment
                sentiment = market_summary.get('sentiment', 'Unknown')
                st.info(f"**Market Sentiment:** {sentiment}")
                
                # Display breadth statistics
                breadth_stats = market_summary.get('breadth_stats', {})
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**MA20 Statistics:**")
                    if 'MA_20' in breadth_stats:
                        ma20_stats = breadth_stats['MA_20']
                        st.metric("Above MA20", f"{ma20_stats.get('above_ma', 0)}")
                        st.metric("Below MA20", f"{ma20_stats.get('below_ma', 0)}")
                        st.metric("Above %", f"{ma20_stats.get('above_pct', 0):.1f}%")
                
                with col2:
                    st.write("**MA50 Statistics:**")
                    if 'MA_50' in breadth_stats:
                        ma50_stats = breadth_stats['MA_50']
                        st.metric("Above MA50", f"{ma50_stats.get('above_ma', 0)}")
                        st.metric("Below MA50", f"{ma50_stats.get('below_ma', 0)}")
                        st.metric("Above %", f"{ma50_stats.get('above_pct', 0):.1f}%")
                
                # Create market breadth chart
                chart_creator = get_chart_creator()
                breadth_fig = chart_creator.create_market_breadth_chart(
                    breadth_stats, "Market Breadth Analysis"
                )
                
                st.plotly_chart(breadth_fig)
                
                # Performance comparison
                st.subheader("🏆 Top vs Worst Performers")
                
                top_performers = market_summary.get('top_performers', pd.DataFrame())
                worst_performers = market_summary.get('worst_performers', pd.DataFrame())
                
                if not top_performers.empty or not worst_performers.empty:
                    performance_fig = chart_creator.create_performance_comparison_chart(
                        top_performers, worst_performers, "Performance Comparison"
                    )
                    st.plotly_chart(performance_fig)
                
                # Market health indicator
                st.subheader("💚 Market Health Indicator")
                
                ma20_above = breadth_stats.get('MA_20', {}).get('above_pct', 50)
                ma50_above = breadth_stats.get('MA_50', {}).get('above_pct', 50)
                
                health_score = (ma20_above + ma50_above) / 2
                
                if health_score >= 70:
                    health_status = "🟢 Excellent"
                    health_color = "success"
                elif health_score >= 60:
                    health_status = "🟡 Good"
                    health_color = "warning"
                elif health_score >= 50:
                    health_status = "🟠 Fair"
                    health_color = "warning"
                elif health_score >= 40:
                    health_status = "🔴 Poor"
                    health_color = "error"
                else:
                    health_status = "💀 Critical"
                    health_color = "error"
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Health Score", f"{health_score:.1f}/100")
                
                with col2:
                    st.metric("Health Status", health_status)
                
                with col3:
                    st.metric("Symbols Analyzed", market_summary.get('symbols_analyzed', 0))
                
                # Progress bar for health score
                st.progress(health_score / 100)
                
        except Exception as e:
            st.error(f"❌ Error in market breadth analysis: {str(e)}")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    <p>Technical Analysis Dashboard | Powered by vnstock_data & Plotly</p>
</div>
""", unsafe_allow_html=True)

