"""
Technical Charts Module
Creates interactive charts for technical analysis
"""

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class TechnicalChartCreator:
    """Creates technical analysis charts using Plotly"""
    
    def __init__(self):
        """Initialize technical chart creator"""
        pass
    
    def create_candlestick_chart(
        self, 
        data: pd.DataFrame,
        title: str = "Stock Price Chart",
        show_volume: bool = True,
        show_indicators: bool = True,
        ma_periods: List[int] = [20, 50],
        ema_periods: List[int] = [9, 21]
    ) -> go.Figure:
        """
        Create interactive candlestick chart with technical indicators
        
        Args:
            data: DataFrame with OHLCV data and indicators
            title: Chart title
            show_volume: Whether to show volume
            show_indicators: Whether to show technical indicators
            ma_periods: MA periods to display
            ema_periods: EMA periods to display
            
        Returns:
            Plotly figure object
        """
        if data.empty:
            logger.warning("Empty data provided for chart creation")
            return go.Figure()
        
        # Determine subplot layout
        if show_volume and show_indicators:
            subplot_titles = [title, "Volume", "RSI"]
            specs = [[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": False}]]
            rows = 3
        elif show_volume:
            subplot_titles = [title, "Volume"]
            specs = [[{"secondary_y": True}], [{"secondary_y": False}]]
            rows = 2
        elif show_indicators:
            subplot_titles = [title, "RSI"]
            specs = [[{"secondary_y": True}], [{"secondary_y": False}]]
            rows = 2
        else:
            subplot_titles = [title]
            specs = [[{"secondary_y": True}]]
            rows = 1
        
        # Create subplots
        fig = make_subplots(
            rows=rows, cols=1,
            subplot_titles=subplot_titles,
            specs=specs,
            shared_xaxes=True,
            vertical_spacing=0.05
        )
        
        # Add candlestick chart
        fig.add_trace(
            go.Candlestick(
                x=data['date'],
                open=data['open'],
                high=data['high'],
                low=data['low'],
                close=data['close'],
                name="OHLC",
                increasing_line_color='#26A69A',
                decreasing_line_color='#EF5350'
            ),
            row=1, col=1
        )
        
        # Add moving averages
        if show_indicators:
            for period in ma_periods:
                ma_col = f'MA_{period}'
                if ma_col in data.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=data['date'],
                            y=data[ma_col],
                            mode='lines',
                            name=f'MA {period}',
                            line=dict(width=1.5, color=f'rgba(255, 165, 0, 0.8)'),
                            opacity=0.8
                        ),
                        row=1, col=1
                    )
            
            # Add EMAs
            for period in ema_periods:
                ema_col = f'EMA_{period}'
                if ema_col in data.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=data['date'],
                            y=data[ema_col],
                            mode='lines',
                            name=f'EMA {period}',
                            line=dict(width=1.5, color=f'rgba(138, 43, 226, 0.8)'),
                            opacity=0.8
                        ),
                        row=1, col=1
                    )
        
        # Add volume
        if show_volume:
            colors = ['#26A69A' if close >= open else '#EF5350' 
                     for close, open in zip(data['close'], data['open'])]
            
            fig.add_trace(
                go.Bar(
                    x=data['date'],
                    y=data['volume'],
                    name="Volume",
                    marker_color=colors,
                    opacity=0.7
                ),
                row=2 if show_indicators else 1, col=1
            )
        
        # Add RSI
        if show_indicators and 'RSI' in data.columns:
            fig.add_trace(
                go.Scatter(
                    x=data['date'],
                    y=data['RSI'],
                    mode='lines',
                    name="RSI",
                    line=dict(width=1.5, color='purple')
                ),
                row=3 if show_volume else 2, col=1
            )
            
            # Add RSI overbought/oversold lines
            fig.add_hline(y=70, line_dash="dash", line_color="red", 
                         row=3 if show_volume else 2, col=1)
            fig.add_hline(y=30, line_dash="dash", line_color="green", 
                         row=3 if show_volume else 2, col=1)
            fig.add_hline(y=50, line_dash="dot", line_color="gray", 
                         row=3 if show_volume else 2, col=1)
        
        # Update layout
        fig.update_layout(
            title=title,
            xaxis_rangeslider_visible=False,
            height=800 if rows > 1 else 600,
            showlegend=True,
            template="plotly_white"
        )
        
        # Update axes
        fig.update_xaxes(title_text="Date", row=rows, col=1)
        fig.update_yaxes(title_text="Price (VND)", row=1, col=1)
        
        if show_volume:
            volume_row = 2 if show_indicators else 1
            fig.update_yaxes(title_text="Volume", row=volume_row, col=1)
        
        if show_indicators and 'RSI' in data.columns:
            rsi_row = 3 if show_volume else 2
            fig.update_yaxes(title_text="RSI", row=rsi_row, col=1)
            fig.update_yaxes(range=[0, 100], row=rsi_row, col=1)
        
        return fig
    
    def create_market_breadth_chart(
        self, 
        breadth_stats: Dict[str, any],
        title: str = "Market Breadth Analysis"
    ) -> go.Figure:
        """
        Create market breadth visualization
        
        Args:
            breadth_stats: Market breadth statistics
            title: Chart title
            
        Returns:
            Plotly figure object
        """
        if not breadth_stats:
            logger.warning("No breadth stats provided for chart creation")
            return go.Figure()
        
        # Extract data for visualization
        ma_periods = []
        above_counts = []
        below_counts = []
        above_pcts = []
        
        for key, value in breadth_stats.items():
            if key.startswith('MA_') and isinstance(value, dict):
                period = key.replace('MA_', '')
                ma_periods.append(f'MA{period}')
                above_counts.append(value.get('above_ma', 0))
                below_counts.append(value.get('below_ma', 0))
                above_pcts.append(value.get('above_pct', 0))
        
        if not ma_periods:
            logger.warning("No MA data found in breadth stats")
            return go.Figure()
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                "Stocks Above/Below MA",
                "Percentage Above MA",
                "Market Sentiment",
                "Advance/Decline Ratio"
            ],
            specs=[[{"type": "bar"}, {"type": "bar"}],
                   [{"type": "indicator"}, {"type": "bar"}]]
        )
        
        # Chart 1: Stocks Above/Below MA
        fig.add_trace(
            go.Bar(
                x=ma_periods,
                y=above_counts,
                name="Above MA",
                marker_color='green',
                opacity=0.8
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Bar(
                x=ma_periods,
                y=below_counts,
                name="Below MA",
                marker_color='red',
                opacity=0.8
            ),
            row=1, col=1
        )
        
        # Chart 2: Percentage Above MA
        fig.add_trace(
            go.Bar(
                x=ma_periods,
                y=above_pcts,
                name="% Above MA",
                marker_color='blue',
                opacity=0.8
            ),
            row=1, col=2
        )
        
        # Add 50% reference line
        fig.add_hline(y=50, line_dash="dash", line_color="gray", row=1, col=2)
        
        # Chart 3: Market Sentiment Indicator
        overall = breadth_stats.get('overall', {})
        advancers = overall.get('advancers', 0)
        decliners = overall.get('decliners', 0)
        
        fig.add_trace(
            go.Indicator(
                mode="gauge+number+delta",
                value=advancers,
                domain={'x': [0, 1], 'y': [0, 1]},
                title={'text': "Advancers"},
                delta={'reference': decliners},
                gauge={
                    'axis': {'range': [None, max(advancers, decliners) * 1.2]},
                    'bar': {'color': "green"},
                    'steps': [
                        {'range': [0, decliners], 'color': "lightgray"},
                        {'range': [decliners, advancers + decliners], 'color': "lightgreen"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': advancers + decliners
                    }
                }
            ),
            row=2, col=1
        )
        
        # Chart 4: Advance/Decline Ratio
        adv_decline_ratio = overall.get('advance_decline_ratio', 1.0)
        if adv_decline_ratio == float('inf'):
            adv_decline_ratio = 10  # Cap for visualization
        
        fig.add_trace(
            go.Bar(
                x=['Advance/Decline'],
                y=[adv_decline_ratio],
                name="A/D Ratio",
                marker_color='orange' if adv_decline_ratio > 1 else 'red',
                opacity=0.8
            ),
            row=2, col=2
        )
        
        # Add 1.0 reference line
        fig.add_hline(y=1.0, line_dash="dash", line_color="gray", row=2, col=2)
        
        # Update layout
        fig.update_layout(
            title=title,
            height=800,
            showlegend=True,
            template="plotly_white"
        )
        
        # Update axes
        fig.update_xaxes(title_text="Moving Average Periods", row=1, col=1)
        fig.update_yaxes(title_text="Number of Stocks", row=1, col=1)
        
        fig.update_xaxes(title_text="Moving Average Periods", row=1, col=2)
        fig.update_yaxes(title_text="Percentage (%)", row=1, col=2)
        
        fig.update_xaxes(title_text="", row=2, col=2)
        fig.update_yaxes(title_text="Ratio", row=2, col=2)
        
        return fig
    
    def create_performance_comparison_chart(
        self, 
        top_performers: pd.DataFrame,
        worst_performers: pd.DataFrame,
        title: str = "Top vs Worst Performers"
    ) -> go.Figure:
        """
        Create performance comparison chart
        
        Args:
            top_performers: DataFrame with top performers
            worst_performers: DataFrame with worst performers
            title: Chart title
            
        Returns:
            Plotly figure object
        """
        if top_performers.empty and worst_performers.empty:
            logger.warning("No performance data provided for chart creation")
            return go.Figure()
        
        # Create subplots
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=["Top Performers", "Worst Performers"],
            specs=[[{"type": "bar"}, {"type": "bar"}]]
        )
        
        # Top performers chart
        if not top_performers.empty:
            fig.add_trace(
                go.Bar(
                    x=top_performers['symbol'],
                    y=top_performers['price_change'],
                    name="Top Performers",
                    marker_color='green',
                    opacity=0.8
                ),
                row=1, col=1
            )
        
        # Worst performers chart
        if not worst_performers.empty:
            fig.add_trace(
                go.Bar(
                    x=worst_performers['symbol'],
                    y=worst_performers['price_change'],
                    name="Worst Performers",
                    marker_color='red',
                    opacity=0.8
                ),
                row=1, col=2
            )
        
        # Update layout
        fig.update_layout(
            title=title,
            height=600,
            showlegend=True,
            template="plotly_white"
        )
        
        # Update axes
        fig.update_xaxes(title_text="Symbol", row=1, col=1)
        fig.update_yaxes(title_text="Price Change (%)", row=1, col=1)
        
        fig.update_xaxes(title_text="Symbol", row=1, col=2)
        fig.update_yaxes(title_text="Price Change (%)", row=1, col=2)
        
        return fig
    
    def create_technical_summary_chart(
        self, 
        data: pd.DataFrame,
        trend_signals: Dict[str, str],
        title: str = "Technical Analysis Summary"
    ) -> go.Figure:
        """
        Create technical analysis summary chart
        
        Args:
            data: DataFrame with OHLCV and indicators
            trend_signals: Dictionary with trend signals
            title: Chart title
            
        Returns:
            Plotly figure object
        """
        if data.empty:
            logger.warning("Empty data provided for technical summary chart")
            return go.Figure()
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=[
                "Price vs Moving Averages",
                "RSI Analysis",
                "Volume Analysis",
                "Trend Signals"
            ],
            specs=[[{"type": "scatter"}, {"type": "scatter"}],
                   [{"type": "bar"}, {"type": "indicator"}]]
        )
        
        # Chart 1: Price vs Moving Averages
        fig.add_trace(
            go.Scatter(
                x=data['date'],
                y=data['close'],
                mode='lines',
                name="Close Price",
                line=dict(width=2, color='black')
            ),
            row=1, col=1
        )
        
        # Add MAs if available
        for period in [20, 50]:
            ma_col = f'MA_{period}'
            if ma_col in data.columns:
                fig.add_trace(
                    go.Scatter(
                        x=data['date'],
                        y=data[ma_col],
                        mode='lines',
                        name=f'MA {period}',
                        line=dict(width=1.5, color=f'rgba(255, 165, 0, 0.8)'),
                        opacity=0.8
                    ),
                    row=1, col=1
                )
        
        # Chart 2: RSI Analysis
        if 'RSI' in data.columns:
            fig.add_trace(
                go.Scatter(
                    x=data['date'],
                    y=data['RSI'],
                    mode='lines',
                    name="RSI",
                    line=dict(width=2, color='purple')
                ),
                row=1, col=2
            )
            
            # Add RSI levels
            fig.add_hline(y=70, line_dash="dash", line_color="red", row=1, col=2)
            fig.add_hline(y=30, line_dash="dash", line_color="green", row=1, col=2)
            fig.add_hline(y=50, line_dash="dot", line_color="gray", row=1, col=2)
        
        # Chart 3: Volume Analysis
        if 'volume' in data.columns:
            colors = ['#26A69A' if close >= open else '#EF5350' 
                     for close, open in zip(data['close'], data['open'])]
            
            fig.add_trace(
                go.Bar(
                    x=data['date'],
                    y=data['volume'],
                    name="Volume",
                    marker_color=colors,
                    opacity=0.7
                ),
                row=2, col=1
            )
        
        # Chart 4: Trend Signals
        if trend_signals:
            # Create a simple indicator showing trend strength
            trend_strength = 0
            if 'EMA_Trend' in trend_signals:
                if 'Bullish' in trend_signals['EMA_Trend']:
                    trend_strength = 75
                else:
                    trend_strength = 25
            
            fig.add_trace(
                go.Indicator(
                    mode="gauge+number",
                    value=trend_strength,
                    domain={'x': [0, 1], 'y': [0, 1]},
                    title={'text': "Trend Strength"},
                    gauge={
                        'axis': {'range': [None, 100]},
                        'bar': {'color': "lightblue"},
                        'steps': [
                            {'range': [0, 25], 'color': "red"},
                            {'range': [25, 50], 'color': "orange"},
                            {'range': [50, 75], 'color': "yellow"},
                            {'range': [75, 100], 'color': "green"}
                        ],
                        'threshold': {
                            'line': {'color': "red", 'width': 4},
                            'thickness': 0.75,
                            'value': trend_strength
                        }
                    }
                ),
                row=2, col=2
            )
        
        # Update layout
        fig.update_layout(
            title=title,
            height=800,
            showlegend=True,
            template="plotly_white"
        )
        
        # Update axes
        fig.update_xaxes(title_text="Date", row=1, col=1)
        fig.update_yaxes(title_text="Price (VND)", row=1, col=1)
        
        fig.update_xaxes(title_text="Date", row=1, col=2)
        fig.update_yaxes(title_text="RSI", row=1, col=2)
        fig.update_yaxes(range=[0, 100], row=1, col=2)
        
        fig.update_xaxes(title_text="Date", row=2, col=1)
        fig.update_yaxes(title_text="Volume", row=2, col=1)
        
        return fig


# Utility functions
def create_stock_chart(
    data: pd.DataFrame,
    title: str = "Stock Analysis",
    show_volume: bool = True,
    show_indicators: bool = True
) -> go.Figure:
    """
    Create stock chart with all indicators
    
    Args:
        data: DataFrame with OHLCV data
        title: Chart title
        show_volume: Whether to show volume
        show_indicators: Whether to show indicators
        
    Returns:
        Plotly figure object
    """
    creator = TechnicalChartCreator()
    return creator.create_candlestick_chart(
        data, title, show_volume, show_indicators
    )


def create_market_overview_chart(
    breadth_stats: Dict[str, any],
    title: str = "Market Overview"
) -> go.Figure:
    """
    Create market overview chart
    
    Args:
        breadth_stats: Market breadth statistics
        title: Chart title
        
    Returns:
        Plotly figure object
    """
    creator = TechnicalChartCreator()
    return creator.create_market_breadth_chart(breadth_stats, title)

