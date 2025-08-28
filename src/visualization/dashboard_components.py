"""
Streamlit dashboard components
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from ..core.data_manager import DataManager
from ..utils.formatters import format_number, format_currency, format_percentage
from ..utils.calculations import calculate_returns, calculate_moving_average


class DashboardComponents:
    """Main dashboard components for Streamlit app"""
    
    def __init__(self, data_manager: DataManager):
        """
        Initialize dashboard components
        
        Args:
            data_manager: Data manager instance
        """
        self.data_manager = data_manager
    
    def render_price_chart(self, ticker: str, days: int = 365) -> None:
        """
        Render price chart for a ticker
        
        Args:
            ticker: Stock ticker
            days: Number of days to show
        """
        try:
            # Get ticker data
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            data = self.data_manager.get_ticker_data(
                ticker, start_date, end_date
            )
            
            if data.empty:
                st.warning(f"No data available for {ticker}")
                return
            
            # Create candlestick chart
            fig = go.Figure()
            
            # Add candlestick
            if all(col in data.columns for col in ['open', 'high', 'low', 'close']):
                fig.add_trace(go.Candlestick(
                    x=data['date'] if 'date' in data.columns else data.index,
                    open=data['open'],
                    high=data['high'],
                    low=data['low'],
                    close=data['close'],
                    name='Price'
                ))
            elif 'close' in data.columns:
                fig.add_trace(go.Scatter(
                    x=data['date'] if 'date' in data.columns else data.index,
                    y=data['close'],
                    mode='lines',
                    name='Close Price',
                    line=dict(color='blue', width=2)
                ))
            
            # Add volume
            if 'volume' in data.columns:
                fig.add_trace(go.Bar(
                    x=data['date'] if 'date' in data.columns else data.index,
                    y=data['volume'],
                    name='Volume',
                    yaxis='y2',
                    opacity=0.3
                ))
            
            # Update layout
            fig.update_layout(
                title=f"{ticker} - Price Chart",
                xaxis_title="Date",
                yaxis_title="Price (VND)",
                yaxis2=dict(
                    title="Volume",
                    overlaying='y',
                    side='right'
                ),
                hovermode='x unified',
                height=500
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
        except Exception as e:
            st.error(f"Error rendering price chart: {str(e)}")
    
    def render_technical_indicators(self, ticker: str) -> None:
        """
        Render technical indicators
        
        Args:
            ticker: Stock ticker
        """
        try:
            data = self.data_manager.get_ticker_data(ticker)
            
            if data.empty or 'close' not in data.columns:
                st.warning(f"No price data available for {ticker}")
                return
            
            # Calculate indicators
            data['MA20'] = calculate_moving_average(data['close'], 20)
            data['MA50'] = calculate_moving_average(data['close'], 50)
            data['Returns'] = calculate_returns(data['close'])
            
            # Create subplots
            fig = go.Figure()
            
            # Price with MAs
            fig.add_trace(go.Scatter(
                x=data['date'] if 'date' in data.columns else data.index,
                y=data['close'],
                name='Close',
                line=dict(color='blue')
            ))
            
            fig.add_trace(go.Scatter(
                x=data['date'] if 'date' in data.columns else data.index,
                y=data['MA20'],
                name='MA20',
                line=dict(color='orange')
            ))
            
            fig.add_trace(go.Scatter(
                x=data['date'] if 'date' in data.columns else data.index,
                y=data['MA50'],
                name='MA50',
                line=dict(color='red')
            ))
            
            fig.update_layout(
                title=f"{ticker} - Technical Indicators",
                xaxis_title="Date",
                yaxis_title="Price (VND)",
                hovermode='x unified',
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                current_price = data['close'].iloc[-1] if not data.empty else 0
                st.metric("Current Price", format_currency(current_price))
            
            with col2:
                daily_return = data['Returns'].iloc[-1] if 'Returns' in data.columns else 0
                st.metric("Daily Return", format_percentage(daily_return))
            
            with col3:
                ma20 = data['MA20'].iloc[-1] if 'MA20' in data.columns else 0
                st.metric("MA20", format_currency(ma20))
            
            with col4:
                ma50 = data['MA50'].iloc[-1] if 'MA50' in data.columns else 0
                st.metric("MA50", format_currency(ma50))
            
        except Exception as e:
            st.error(f"Error rendering technical indicators: {str(e)}")
    
    def render_valuation_chart(self, ticker: str, metrics: List[str]) -> None:
        """
        Render valuation metrics chart
        
        Args:
            ticker: Stock ticker
            metrics: List of valuation metrics to display
        """
        try:
            st.info(f"Valuation chart for {ticker} - Metrics: {', '.join(metrics)}")
            # Placeholder for valuation implementation
            # This would fetch and display valuation metrics
            
        except Exception as e:
            st.error(f"Error rendering valuation chart: {str(e)}")
    
    def render_margin_chart(self, ticker: str, metrics: List[str]) -> None:
        """
        Render margin analysis chart
        
        Args:
            ticker: Stock ticker
            metrics: List of margin metrics to display
        """
        try:
            st.info(f"Margin analysis for {ticker} - Metrics: {', '.join(metrics)}")
            # Placeholder for margin analysis implementation
            # This would fetch and display margin metrics
            
        except Exception as e:
            st.error(f"Error rendering margin chart: {str(e)}")
    
    def render_key_metrics(self, ticker: str) -> None:
        """
        Render key financial metrics
        
        Args:
            ticker: Stock ticker
        """
        try:
            # Get ticker data
            data = self.data_manager.get_ticker_data(ticker)
            
            if data.empty:
                st.warning(f"No data available for {ticker}")
                return
            
            # Display in columns
            col1, col2, col3, col4 = st.columns(4)
            
            # Placeholder metrics - would be replaced with actual data
            with col1:
                st.metric("Market Cap", "N/A")
            with col2:
                st.metric("P/E Ratio", "N/A")
            with col3:
                st.metric("P/B Ratio", "N/A")
            with col4:
                st.metric("ROE", "N/A")
            
        except Exception as e:
            st.error(f"Error rendering key metrics: {str(e)}")
    
    def render_comparison_table(self, tickers: List[str]) -> None:
        """
        Render comparison table for multiple tickers
        
        Args:
            tickers: List of tickers to compare
        """
        try:
            if not tickers:
                st.warning("Please select tickers to compare")
                return
            
            # Create comparison data
            comparison_data = []
            
            for ticker in tickers:
                data = self.data_manager.get_ticker_data(ticker)
                if not data.empty:
                    row = {
                        'Ticker': ticker,
                        'Last Price': data['close'].iloc[-1] if 'close' in data.columns else 0,
                        'Volume': data['volume'].iloc[-1] if 'volume' in data.columns else 0
                    }
                    comparison_data.append(row)
            
            if comparison_data:
                df = pd.DataFrame(comparison_data)
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No data available for comparison")
            
        except Exception as e:
            st.error(f"Error rendering comparison table: {str(e)}")
    
    def render_sidebar_filters(self) -> Dict[str, Any]:
        """
        Render sidebar filters and return selected values
        
        Returns:
            Dictionary of selected filter values
        """
        filters = {}
        
        # Ticker selection
        available_tickers = self.data_manager.get_available_tickers()
        filters['tickers'] = st.sidebar.multiselect(
            "Select Tickers",
            options=available_tickers[:50],  # Limit to first 50
            default=available_tickers[:3] if available_tickers else []
        )
        
        # Date range
        filters['date_range'] = st.sidebar.date_input(
            "Date Range",
            value=(datetime.now() - timedelta(days=365), datetime.now())
        )
        
        # Analysis type
        filters['analysis_type'] = st.sidebar.selectbox(
            "Analysis Type",
            options=["Technical", "Fundamental", "Both"]
        )
        
        return filters