"""
Market Breadth Analyzer
Analyzes market breadth indicators and statistics
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging
from datetime import datetime, timedelta

from ..technical.indicator_analyzer import TechnicalIndicatorAnalyzer
from ...data.loaders.market_loader import MarketDataLoader

logger = logging.getLogger(__name__)


class MarketBreadthAnalyzer:
    """Market breadth analyzer for VN market"""
    
    def __init__(self, market_loader: Optional[MarketDataLoader] = None):
        """
        Initialize market breadth analyzer
        
        Args:
            market_loader: Market data loader instance
        """
        self.market_loader = market_loader or MarketDataLoader()
        self.technical_analyzer = TechnicalIndicatorAnalyzer()
        
        # Default VN market symbols (top 100 by market cap)
        self.default_symbols = [
            'HPG', 'VNM', 'VCB', 'TCB', 'BID', 'CTG', 'MBB', 'ACB', 'VPB', 'STB',
            'TPB', 'EIB', 'SHB', 'MSN', 'MWG', 'FPT', 'VIC', 'VHM', 'POW', 'GAS',
            'PLX', 'BSR', 'PVD', 'PVS', 'PVT', 'POW', 'GAS', 'PLX', 'BSR', 'PVD',
            'SAB', 'VNM', 'BHN', 'DPM', 'DGC', 'DCM', 'TCH', 'TCT', 'TCL', 'TCM',
            'TCO', 'TCR', 'TDC', 'TDG', 'TDH', 'TDM', 'TDP', 'TDR', 'TDS', 'TDT',
            'TDV', 'TDW', 'TDX', 'TDY', 'TDZ', 'TEA', 'TEB', 'TEC', 'TED', 'TEE',
            'TEF', 'TEG', 'TEH', 'TEI', 'TEJ', 'TEK', 'TEL', 'TEM', 'TEN', 'TEO',
            'TEP', 'TEQ', 'TER', 'TES', 'TET', 'TEU', 'TEV', 'TEW', 'TEX', 'TEY',
            'TEZ', 'TFA', 'TFB', 'TFC', 'TFD', 'TFE', 'TFF', 'TFG', 'TFH', 'TFI',
            'TFJ', 'TFK', 'TFL', 'TFM', 'TFN', 'TFO', 'TFP', 'TFQ', 'TFR', 'TFS'
        ]
    
    def calculate_market_breadth(
        self, 
        symbols: Optional[List[str]] = None,
        days: int = 30,
        ma_periods: List[int] = [20, 50]
    ) -> Dict[str, any]:
        """
        Calculate market breadth indicators
        
        Args:
            symbols: List of stock symbols to analyze
            days: Number of days to look back
            ma_periods: MA periods to analyze
            
        Returns:
            Dictionary with market breadth statistics
        """
        symbols = symbols or self.default_symbols
        
        logger.info(f"Calculating market breadth for {len(symbols)} symbols")
        
        # Get market data for all symbols
        market_data = self.market_loader.get_multiple_stocks_ohlcv(
            symbols=symbols,
            start_date=(datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d"),
            end_date=datetime.now().strftime("%Y-%m-%d")
        )
        
        if not market_data:
            logger.warning("No market data available for breadth analysis")
            return {}
        
        # Calculate breadth statistics
        breadth_stats = {}
        
        for ma_period in ma_periods:
            ma_stats = self._calculate_ma_breadth(market_data, ma_period)
            breadth_stats[f'MA_{ma_period}'] = ma_stats
        
        # Calculate overall market statistics
        breadth_stats['overall'] = self._calculate_overall_stats(market_data)
        
        return breadth_stats
    
    def _calculate_ma_breadth(
        self, 
        market_data: Dict[str, pd.DataFrame], 
        ma_period: int
    ) -> Dict[str, any]:
        """
        Calculate breadth for a specific MA period
        
        Args:
            market_data: Dictionary of market data by symbol
            ma_period: MA period to analyze
            
        Returns:
            Dictionary with MA breadth statistics
        """
        above_ma = 0
        below_ma = 0
        total_symbols = len(market_data)
        
        ma_data = {}
        
        for symbol, data in market_data.items():
            if data.empty or len(data) < ma_period:
                continue
            
            # Calculate MA
            data_with_ma = self.technical_analyzer.calculate_moving_averages(
                data, [ma_period]
            )
            
            if f'MA_{ma_period}' not in data_with_ma.columns:
                continue
            
            latest = data_with_ma.iloc[-1]
            current_price = latest['close']
            ma_value = latest[f'MA_{ma_period}']
            
            if pd.isna(ma_value):
                continue
            
            # Check if price is above or below MA
            if current_price > ma_value:
                above_ma += 1
                ma_data[symbol] = {
                    'price': current_price,
                    'ma': ma_value,
                    'position': 'above',
                    'distance': ((current_price - ma_value) / ma_value) * 100
                }
            else:
                below_ma += 1
                ma_data[symbol] = {
                    'price': current_price,
                    'ma': ma_value,
                    'position': 'below',
                    'distance': ((current_price - ma_value) / ma_value) * 100
                }
        
        # Calculate percentages
        total_valid = above_ma + below_ma
        if total_valid > 0:
            above_pct = (above_ma / total_valid) * 100
            below_pct = (below_ma / total_valid) * 100
        else:
            above_pct = below_pct = 0
        
        return {
            'above_ma': above_ma,
            'below_ma': below_ma,
            'total_valid': total_valid,
            'above_pct': above_pct,
            'below_pct': below_pct,
            'symbol_details': ma_data
        }
    
    def _calculate_overall_stats(
        self, 
        market_data: Dict[str, pd.DataFrame]
    ) -> Dict[str, any]:
        """
        Calculate overall market statistics
        
        Args:
            market_data: Dictionary of market data by symbol
            
        Returns:
            Dictionary with overall statistics
        """
        total_symbols = len(market_data)
        valid_symbols = 0
        price_changes = []
        volume_changes = []
        
        for symbol, data in market_data.items():
            if data.empty or len(data) < 2:
                continue
            
            valid_symbols += 1
            
            # Calculate price change
            latest_price = data.iloc[-1]['close']
            prev_price = data.iloc[-2]['close']
            price_change = ((latest_price - prev_price) / prev_price) * 100
            price_changes.append(price_change)
            
            # Calculate volume change
            latest_volume = data.iloc[-1]['volume']
            prev_volume = data.iloc[-2]['volume']
            if prev_volume > 0:
                volume_change = ((latest_volume - prev_volume) / prev_volume) * 100
                volume_changes.append(volume_change)
        
        # Calculate statistics
        if price_changes:
            avg_price_change = np.mean(price_changes)
            price_change_std = np.std(price_changes)
            advancers = len([x for x in price_changes if x > 0])
            decliners = len([x for x in price_changes if x < 0])
            unchanged = len([x for x in price_changes if x == 0])
        else:
            avg_price_change = price_change_std = 0
            advancers = decliners = unchanged = 0
        
        if volume_changes:
            avg_volume_change = np.mean(volume_changes)
        else:
            avg_volume_change = 0
        
        return {
            'total_symbols': total_symbols,
            'valid_symbols': valid_symbols,
            'avg_price_change': avg_price_change,
            'price_change_std': price_change_std,
            'advancers': advancers,
            'decliners': decliners,
            'unchanged': unchanged,
            'advance_decline_ratio': advancers / decliners if decliners > 0 else float('inf'),
            'avg_volume_change': avg_volume_change
        }
    
    def get_market_sentiment(self, breadth_stats: Dict[str, any]) -> str:
        """
        Determine market sentiment based on breadth statistics
        
        Args:
            breadth_stats: Market breadth statistics
            
        Returns:
            Market sentiment string
        """
        if not breadth_stats:
            return "Neutral (Insufficient Data)"
        
        # Check MA20 breadth
        ma20_stats = breadth_stats.get('MA_20', {})
        ma20_above_pct = ma20_stats.get('above_pct', 50)
        
        # Check MA50 breadth
        ma50_stats = breadth_stats.get('MA_50', {})
        ma50_above_pct = ma50_stats.get('above_pct', 50)
        
        # Check overall stats
        overall = breadth_stats.get('overall', {})
        adv_decline_ratio = overall.get('advance_decline_ratio', 1.0)
        
        # Determine sentiment
        if ma20_above_pct > 70 and ma50_above_pct > 60 and adv_decline_ratio > 1.5:
            return "Strongly Bullish 🚀"
        elif ma20_above_pct > 60 and ma50_above_pct > 50 and adv_decline_ratio > 1.2:
            return "Bullish 📈"
        elif ma20_above_pct > 45 and ma50_above_pct > 40 and adv_decline_ratio > 0.8:
            return "Slightly Bullish 📊"
        elif ma20_above_pct < 30 and ma50_above_pct < 40 and adv_decline_ratio < 0.5:
            return "Strongly Bearish 🔻"
        elif ma20_above_pct < 40 and ma50_above_pct < 50 and adv_decline_ratio < 0.8:
            return "Bearish 📉"
        elif ma20_above_pct < 55 and ma50_above_pct < 60 and adv_decline_ratio < 1.2:
            return "Slightly Bearish 📊"
        else:
            return "Neutral ↔️"
    
    def get_top_performers(
        self, 
        market_data: Dict[str, pd.DataFrame],
        top_n: int = 10
    ) -> pd.DataFrame:
        """
        Get top performing stocks
        
        Args:
            market_data: Dictionary of market data by symbol
            top_n: Number of top performers to return
            
        Returns:
            DataFrame with top performers
        """
        performers = []
        
        for symbol, data in market_data.items():
            if data.empty or len(data) < 2:
                continue
            
            latest_price = data.iloc[-1]['close']
            prev_price = data.iloc[-2]['close']
            
            if prev_price > 0:
                price_change = ((latest_price - prev_price) / prev_price) * 100
                performers.append({
                    'symbol': symbol,
                    'current_price': latest_price,
                    'price_change': price_change,
                    'volume': data.iloc[-1]['volume']
                })
        
        if not performers:
            return pd.DataFrame()
        
        # Sort by price change and return top N
        performers_df = pd.DataFrame(performers)
        performers_df = performers_df.sort_values('price_change', ascending=False)
        
        return performers_df.head(top_n)
    
    def get_market_summary(
        self, 
        symbols: Optional[List[str]] = None,
        days: int = 30
    ) -> Dict[str, any]:
        """
        Get comprehensive market summary
        
        Args:
            symbols: List of stock symbols to analyze
            days: Number of days to look back
            
        Returns:
            Dictionary with market summary
        """
        # Calculate breadth statistics
        breadth_stats = self.calculate_market_breadth(symbols, days)
        
        # Get market sentiment
        sentiment = self.get_market_sentiment(breadth_stats)
        
        # Get market data for top performers
        market_data = self.market_loader.get_multiple_stocks_ohlcv(
            symbols=symbols or self.default_symbols,
            start_date=(datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d"),
            end_date=datetime.now().strftime("%Y-%m-%d")
        )
        
        top_performers = self.get_top_performers(market_data)
        worst_performers = self.get_top_performers(market_data, top_n=10)
        worst_performers = worst_performers.sort_values('price_change').head(10)
        
        return {
            'breadth_stats': breadth_stats,
            'sentiment': sentiment,
            'top_performers': top_performers,
            'worst_performers': worst_performers,
            'analysis_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'symbols_analyzed': len(market_data) if market_data else 0
        }


# Utility functions
def get_market_breadth_summary(
    symbols: Optional[List[str]] = None,
    days: int = 30
) -> Dict[str, any]:
    """
    Get market breadth summary
    
    Args:
        symbols: List of stock symbols to analyze
        days: Number of days to look back
        
    Returns:
        Dictionary with market breadth summary
    """
    analyzer = MarketBreadthAnalyzer()
    return analyzer.get_market_summary(symbols, days)


def analyze_market_health(
    symbols: Optional[List[str]] = None,
    days: int = 30
) -> Dict[str, any]:
    """
    Analyze overall market health
    
    Args:
        symbols: List of stock symbols to analyze
        days: Number of days to look back
        
    Returns:
        Dictionary with market health indicators
    """
    analyzer = MarketBreadthAnalyzer()
    breadth_stats = analyzer.calculate_market_breadth(symbols, days)
    
    # Calculate health score
    ma20_above = breadth_stats.get('MA_20', {}).get('above_pct', 50)
    ma50_above = breadth_stats.get('MA_50', {}).get('above_pct', 50)
    
    health_score = (ma20_above + ma50_above) / 2
    
    if health_score >= 70:
        health_status = "Excellent"
    elif health_score >= 60:
        health_status = "Good"
    elif health_score >= 50:
        health_status = "Fair"
    elif health_score >= 40:
        health_status = "Poor"
    else:
        health_status = "Critical"
    
    return {
        'health_score': health_score,
        'health_status': health_status,
        'ma20_strength': ma20_above,
        'ma50_strength': ma50_above,
        'breadth_stats': breadth_stats
    }

