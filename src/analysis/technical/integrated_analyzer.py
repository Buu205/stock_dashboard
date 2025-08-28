"""
Integrated Analyzer
Combines fundamental data from financial statements with technical data from market prices
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)

# Import modules
import sys
sys.path.append(str(Path(__file__).parent.parent.parent))
from core.config import get_config, get_metric_code
from data.loaders.financial_loader import FinancialDataLoader
from data.connectors.tcbs_connector import TCBSConnector


class IntegratedAnalyzer:
    """
    Combines fundamental and technical analysis for comprehensive stock evaluation
    """
    
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize with both data sources"""
        self.config = get_config(config_path)
        self.financial_loader = FinancialDataLoader(config_path)
        self.tcbs_connector = TCBSConnector(config_path)
        
        # Load financial data
        self.financial_loader.load_data()
        
        logger.info("IntegratedAnalyzer initialized")
    
    def analyze_stock(
        self,
        ticker: str,
        start_date: str = None,
        include_technicals: bool = True
    ) -> Dict:
        """
        Perform comprehensive analysis of a stock
        
        Args:
            ticker: Stock ticker symbol
            start_date: Start date for analysis
            include_technicals: Whether to include technical analysis
            
        Returns:
            Dictionary with complete analysis
        """
        analysis = {
            'ticker': ticker,
            'timestamp': datetime.now().isoformat(),
            'fundamental': {},
            'technical': {},
            'valuation': {},
            'signals': []
        }
        
        # 1. Fundamental Analysis
        logger.info(f"Analyzing fundamentals for {ticker}...")
        fundamental = self._analyze_fundamentals(ticker)
        analysis['fundamental'] = fundamental
        
        # 2. Technical Analysis
        if include_technicals:
            logger.info(f"Analyzing technicals for {ticker}...")
            technical = self._analyze_technicals(ticker, start_date)
            analysis['technical'] = technical
        
        # 3. Valuation Analysis
        logger.info(f"Calculating valuation for {ticker}...")
        valuation = self._calculate_valuation(ticker, fundamental, technical)
        analysis['valuation'] = valuation
        
        # 4. Generate Signals
        signals = self._generate_signals(fundamental, technical, valuation)
        analysis['signals'] = signals
        
        # 5. Calculate Score
        analysis['score'] = self._calculate_score(fundamental, technical, valuation)
        
        return analysis
    
    def _analyze_fundamentals(self, ticker: str) -> Dict:
        """Analyze fundamental metrics"""
        try:
            # Get financial data
            financial_data = self.financial_loader.calculate_financial_ratios(ticker, 'Y')
            
            if financial_data.empty:
                return {}
            
            # Get latest year data
            latest = financial_data.iloc[-1]
            
            # Calculate growth metrics
            growth = self.financial_loader.get_growth_analysis(ticker, periods=3)
            
            fundamental = {
                'latest_year': int(latest.get('YEAR', 0)),
                'revenue': latest.get(get_metric_code('revenue'), 0),
                'net_profit': latest.get(get_metric_code('net_profit'), 0),
                'total_assets': latest.get(get_metric_code('total_assets'), 0),
                'equity': latest.get(get_metric_code('equity'), 0),
                'ratios': {
                    'roe': latest.get('ROE', 0),
                    'roa': latest.get('ROA', 0),
                    'net_margin': latest.get('NET_MARGIN', 0),
                    'debt_to_equity': latest.get('DEBT_TO_EQUITY', 0),
                    'asset_turnover': latest.get('ASSET_TURNOVER', 0)
                },
                'growth': {
                    'revenue_cagr': growth.get('revenue_cagr', 0),
                    'profit_cagr': growth.get('net_profit_cagr', 0),
                    'equity_cagr': growth.get('equity_cagr', 0)
                }
            }
            
            # Add quality score
            fundamental['quality_score'] = self._calculate_quality_score(fundamental)
            
            return fundamental
            
        except Exception as e:
            logger.error(f"Fundamental analysis failed for {ticker}: {e}")
            return {}
    
    def _analyze_technicals(self, ticker: str, start_date: str = None) -> Dict:
        """Analyze technical indicators"""
        try:
            # Default to 1 year of data
            if start_date is None:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            
            # Fetch price data
            price_data = self.tcbs_connector.fetch_historical_price(ticker, start_date)
            
            if price_data.empty:
                return {}
            
            # Calculate indicators
            price_data = self.tcbs_connector.calculate_technical_indicators(
                price_data,
                indicators=['SMA', 'EMA', 'RSI', 'MACD', 'BB']
            )
            
            # Get latest values
            latest = price_data.iloc[-1]
            
            technical = {
                'current_price': latest['close'],
                'volume': latest['volume'],
                'price_change_1d': price_data['close'].pct_change().iloc[-1],
                'price_change_1w': (latest['close'] / price_data['close'].iloc[-5] - 1) if len(price_data) > 5 else 0,
                'price_change_1m': (latest['close'] / price_data['close'].iloc[-20] - 1) if len(price_data) > 20 else 0,
                'price_change_ytd': (latest['close'] / price_data['close'].iloc[0] - 1),
                'indicators': {
                    'rsi': latest.get('RSI', 50),
                    'macd': latest.get('MACD', 0),
                    'macd_signal': latest.get('MACD_signal', 0),
                    'sma_20': latest.get('SMA_20', 0),
                    'sma_50': latest.get('SMA_50', 0),
                    'ema_9': latest.get('EMA_9', 0),
                    'ema_21': latest.get('EMA_21', 0)
                },
                'support_resistance': {
                    'support': price_data['low'].rolling(20).min().iloc[-1],
                    'resistance': price_data['high'].rolling(20).max().iloc[-1],
                    '52w_high': price_data['high'].max(),
                    '52w_low': price_data['low'].min()
                },
                'volume_analysis': {
                    'avg_volume': price_data['volume'].mean(),
                    'volume_ratio': latest['volume'] / price_data['volume'].mean(),
                    'volume_trend': 'high' if latest['volume'] > price_data['volume'].mean() * 1.5 else 'normal'
                }
            }
            
            # Add trend analysis
            technical['trend'] = self._analyze_trend(price_data)
            
            return technical
            
        except Exception as e:
            logger.error(f"Technical analysis failed for {ticker}: {e}")
            return {}
    
    def _calculate_valuation(
        self, 
        ticker: str,
        fundamental: Dict,
        technical: Dict
    ) -> Dict:
        """Calculate valuation metrics"""
        try:
            valuation = {}
            
            if fundamental and technical:
                current_price = technical.get('current_price', 0)
                
                # Calculate P/E ratio
                if fundamental.get('net_profit', 0) > 0:
                    # Need to get shares outstanding (simplified calculation)
                    # Assuming market cap = price * shares
                    # This is simplified - in real implementation, get actual shares data
                    pe_ratio = current_price / (fundamental['net_profit'] / 1000000)  # Simplified
                    valuation['pe_ratio'] = pe_ratio
                    valuation['pe_status'] = self._evaluate_pe(pe_ratio)
                
                # Calculate P/B ratio
                if fundamental.get('equity', 0) > 0:
                    pb_ratio = current_price / (fundamental['equity'] / 1000000)  # Simplified
                    valuation['pb_ratio'] = pb_ratio
                    valuation['pb_status'] = self._evaluate_pb(pb_ratio)
                
                # Graham Number (simplified)
                if fundamental['ratios'].get('roe', 0) > 0:
                    eps = fundamental.get('net_profit', 0) / 1000000  # Simplified
                    bvps = fundamental.get('equity', 0) / 1000000  # Simplified
                    
                    if eps > 0 and bvps > 0:
                        graham_number = np.sqrt(22.5 * eps * bvps)
                        valuation['graham_number'] = graham_number
                        valuation['graham_ratio'] = current_price / graham_number if graham_number > 0 else 0
                
                # Overall valuation assessment
                valuation['assessment'] = self._assess_valuation(valuation)
            
            return valuation
            
        except Exception as e:
            logger.error(f"Valuation calculation failed: {e}")
            return {}
    
    def _generate_signals(
        self,
        fundamental: Dict,
        technical: Dict,
        valuation: Dict
    ) -> List[Dict]:
        """Generate trading signals based on analysis"""
        signals = []
        
        # Fundamental signals
        if fundamental:
            # ROE signal
            roe = fundamental.get('ratios', {}).get('roe', 0)
            if roe > 0.15:
                signals.append({
                    'type': 'fundamental',
                    'indicator': 'ROE',
                    'value': roe,
                    'signal': 'positive',
                    'message': f'High ROE: {roe*100:.1f}% (>15%)'
                })
            elif roe < 0.08:
                signals.append({
                    'type': 'fundamental',
                    'indicator': 'ROE',
                    'value': roe,
                    'signal': 'negative',
                    'message': f'Low ROE: {roe*100:.1f}% (<8%)'
                })
            
            # Debt signal
            de_ratio = fundamental.get('ratios', {}).get('debt_to_equity', 0)
            if de_ratio > 2:
                signals.append({
                    'type': 'fundamental',
                    'indicator': 'D/E',
                    'value': de_ratio,
                    'signal': 'warning',
                    'message': f'High leverage: D/E = {de_ratio:.2f}'
                })
        
        # Technical signals
        if technical:
            # RSI signal
            rsi = technical.get('indicators', {}).get('rsi', 50)
            if rsi < 30:
                signals.append({
                    'type': 'technical',
                    'indicator': 'RSI',
                    'value': rsi,
                    'signal': 'buy',
                    'message': f'Oversold: RSI = {rsi:.1f}'
                })
            elif rsi > 70:
                signals.append({
                    'type': 'technical',
                    'indicator': 'RSI',
                    'value': rsi,
                    'signal': 'sell',
                    'message': f'Overbought: RSI = {rsi:.1f}'
                })
            
            # MACD signal
            macd = technical.get('indicators', {}).get('macd', 0)
            macd_signal = technical.get('indicators', {}).get('macd_signal', 0)
            
            if macd > macd_signal and macd > 0:
                signals.append({
                    'type': 'technical',
                    'indicator': 'MACD',
                    'value': macd,
                    'signal': 'buy',
                    'message': 'MACD bullish crossover'
                })
            elif macd < macd_signal and macd < 0:
                signals.append({
                    'type': 'technical',
                    'indicator': 'MACD',
                    'value': macd,
                    'signal': 'sell',
                    'message': 'MACD bearish crossover'
                })
            
            # Price vs MA signal
            price = technical.get('current_price', 0)
            sma_50 = technical.get('indicators', {}).get('sma_50', 0)
            
            if price > sma_50 * 1.05:
                signals.append({
                    'type': 'technical',
                    'indicator': 'MA',
                    'value': price/sma_50,
                    'signal': 'neutral',
                    'message': f'Price above SMA50: {(price/sma_50-1)*100:.1f}%'
                })
        
        # Valuation signals
        if valuation:
            if valuation.get('pe_status') == 'undervalued':
                signals.append({
                    'type': 'valuation',
                    'indicator': 'P/E',
                    'value': valuation.get('pe_ratio', 0),
                    'signal': 'buy',
                    'message': 'Stock appears undervalued by P/E'
                })
            
            graham_ratio = valuation.get('graham_ratio', 0)
            if 0 < graham_ratio < 0.8:
                signals.append({
                    'type': 'valuation',
                    'indicator': 'Graham',
                    'value': graham_ratio,
                    'signal': 'buy',
                    'message': f'Below Graham Number: {graham_ratio:.2f}'
                })
        
        return signals
    
    def _calculate_quality_score(self, fundamental: Dict) -> float:
        """Calculate quality score based on fundamentals"""
        score = 50  # Base score
        
        # ROE contribution
        roe = fundamental.get('ratios', {}).get('roe', 0)
        if roe > 0.20:
            score += 20
        elif roe > 0.15:
            score += 10
        elif roe < 0.05:
            score -= 10
        
        # Debt contribution
        de = fundamental.get('ratios', {}).get('debt_to_equity', 0)
        if de < 0.5:
            score += 10
        elif de > 2:
            score -= 10
        
        # Growth contribution
        revenue_growth = fundamental.get('growth', {}).get('revenue_cagr', 0)
        if revenue_growth > 20:
            score += 15
        elif revenue_growth > 10:
            score += 5
        elif revenue_growth < 0:
            score -= 10
        
        # Profitability
        margin = fundamental.get('ratios', {}).get('net_margin', 0)
        if margin > 0.15:
            score += 5
        elif margin < 0.05:
            score -= 5
        
        return max(0, min(100, score))
    
    def _analyze_trend(self, price_data: pd.DataFrame) -> str:
        """Analyze price trend"""
        if len(price_data) < 20:
            return 'insufficient_data'
        
        # Get moving averages
        sma_20 = price_data['close'].rolling(20).mean().iloc[-1]
        sma_50 = price_data['close'].rolling(50).mean().iloc[-1] if len(price_data) > 50 else sma_20
        current = price_data['close'].iloc[-1]
        
        # Determine trend
        if current > sma_20 > sma_50:
            return 'strong_uptrend'
        elif current > sma_20:
            return 'uptrend'
        elif current < sma_20 < sma_50:
            return 'strong_downtrend'
        elif current < sma_20:
            return 'downtrend'
        else:
            return 'sideways'
    
    def _evaluate_pe(self, pe_ratio: float) -> str:
        """Evaluate P/E ratio"""
        if pe_ratio < 0:
            return 'negative_earnings'
        elif pe_ratio < 10:
            return 'undervalued'
        elif pe_ratio < 20:
            return 'fair_value'
        elif pe_ratio < 30:
            return 'overvalued'
        else:
            return 'highly_overvalued'
    
    def _evaluate_pb(self, pb_ratio: float) -> str:
        """Evaluate P/B ratio"""
        if pb_ratio < 1:
            return 'undervalued'
        elif pb_ratio < 3:
            return 'fair_value'
        else:
            return 'overvalued'
    
    def _assess_valuation(self, valuation: Dict) -> str:
        """Overall valuation assessment"""
        pe_status = valuation.get('pe_status', '')
        pb_status = valuation.get('pb_status', '')
        graham_ratio = valuation.get('graham_ratio', 1)
        
        undervalued_count = 0
        if pe_status == 'undervalued':
            undervalued_count += 1
        if pb_status == 'undervalued':
            undervalued_count += 1
        if 0 < graham_ratio < 0.8:
            undervalued_count += 1
        
        if undervalued_count >= 2:
            return 'undervalued'
        elif undervalued_count == 1:
            return 'fairly_valued'
        else:
            return 'overvalued'
    
    def _calculate_score(
        self,
        fundamental: Dict,
        technical: Dict,
        valuation: Dict
    ) -> Dict:
        """Calculate overall investment score"""
        scores = {
            'fundamental': fundamental.get('quality_score', 50),
            'technical': 50,  # Base score
            'valuation': 50,   # Base score
            'overall': 0
        }
        
        # Technical score adjustments
        if technical:
            trend = technical.get('trend', '')
            if 'uptrend' in trend:
                scores['technical'] += 20
            elif 'downtrend' in trend:
                scores['technical'] -= 20
            
            rsi = technical.get('indicators', {}).get('rsi', 50)
            if 30 < rsi < 70:
                scores['technical'] += 10
        
        # Valuation score adjustments
        if valuation:
            assessment = valuation.get('assessment', '')
            if assessment == 'undervalued':
                scores['valuation'] += 30
            elif assessment == 'overvalued':
                scores['valuation'] -= 20
        
        # Calculate overall score (weighted average)
        scores['overall'] = (
            scores['fundamental'] * 0.4 +
            scores['technical'] * 0.3 +
            scores['valuation'] * 0.3
        )
        
        return scores
    
    def screen_stocks(
        self,
        tickers: List[str],
        criteria: Dict = None
    ) -> pd.DataFrame:
        """
        Screen multiple stocks based on criteria
        
        Args:
            tickers: List of tickers to screen
            criteria: Screening criteria
            
        Returns:
            DataFrame with screening results
        """
        if criteria is None:
            criteria = {
                'min_roe': 0.10,
                'max_de': 2.0,
                'min_revenue_growth': 0,
                'rsi_range': (30, 70)
            }
        
        results = []
        
        for ticker in tickers:
            try:
                analysis = self.analyze_stock(ticker)
                
                # Extract key metrics
                result = {
                    'ticker': ticker,
                    'roe': analysis['fundamental'].get('ratios', {}).get('roe', 0),
                    'debt_to_equity': analysis['fundamental'].get('ratios', {}).get('debt_to_equity', 0),
                    'revenue_growth': analysis['fundamental'].get('growth', {}).get('revenue_cagr', 0),
                    'current_price': analysis['technical'].get('current_price', 0),
                    'rsi': analysis['technical'].get('indicators', {}).get('rsi', 50),
                    'trend': analysis['technical'].get('trend', 'unknown'),
                    'pe_ratio': analysis['valuation'].get('pe_ratio', 0),
                    'overall_score': analysis['score'].get('overall', 0),
                    'signals': len(analysis.get('signals', []))
                }
                
                # Check criteria
                meets_criteria = True
                if result['roe'] < criteria.get('min_roe', 0):
                    meets_criteria = False
                if result['debt_to_equity'] > criteria.get('max_de', 999):
                    meets_criteria = False
                if result['revenue_growth'] < criteria.get('min_revenue_growth', -999):
                    meets_criteria = False
                
                rsi_range = criteria.get('rsi_range', (0, 100))
                if not (rsi_range[0] <= result['rsi'] <= rsi_range[1]):
                    meets_criteria = False
                
                result['meets_criteria'] = meets_criteria
                results.append(result)
                
                logger.info(f"✓ Screened {ticker}: Score={result['overall_score']:.1f}")
                
            except Exception as e:
                logger.error(f"Failed to screen {ticker}: {e}")
                continue
        
        # Create DataFrame and sort by score
        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values('overall_score', ascending=False)
        
        return df


if __name__ == "__main__":
    # Test the integrated analyzer
    print("Testing Integrated Analyzer...")
    print("=" * 70)
    
    # Initialize analyzer
    analyzer = IntegratedAnalyzer()
    
    # Test with MWG
    ticker = 'MWG'
    print(f"\nAnalyzing {ticker}...")
    
    analysis = analyzer.analyze_stock(ticker, start_date='2024-01-01')
    
    # Display results
    print(f"\n{'='*70}")
    print(f"ANALYSIS RESULTS FOR {ticker}")
    print(f"{'='*70}")
    
    # Fundamental Analysis
    print("\n1. FUNDAMENTAL ANALYSIS:")
    fundamental = analysis.get('fundamental', {})
    if fundamental:
        print(f"   Latest Year: {fundamental.get('latest_year')}")
        print(f"   Revenue: {fundamental.get('revenue'):,.0f}")
        print(f"   Net Profit: {fundamental.get('net_profit'):,.0f}")
        
        print("\n   Key Ratios:")
        for key, value in fundamental.get('ratios', {}).items():
            print(f"   - {key}: {value*100:.2f}%" if 'margin' in key or key in ['roe', 'roa'] else f"   - {key}: {value:.2f}")
        
        print("\n   Growth Metrics:")
        for key, value in fundamental.get('growth', {}).items():
            print(f"   - {key}: {value:.2f}%")
        
        print(f"\n   Quality Score: {fundamental.get('quality_score', 0):.1f}/100")
    
    # Technical Analysis
    print("\n2. TECHNICAL ANALYSIS:")
    technical = analysis.get('technical', {})
    if technical:
        print(f"   Current Price: {technical.get('current_price', 0):,.0f}")
        print(f"   Trend: {technical.get('trend', 'unknown')}")
        print(f"   YTD Return: {technical.get('price_change_ytd', 0)*100:.2f}%")
        
        print("\n   Key Indicators:")
        indicators = technical.get('indicators', {})
        print(f"   - RSI: {indicators.get('rsi', 0):.1f}")
        print(f"   - MACD: {indicators.get('macd', 0):.2f}")
        
        print("\n   Support/Resistance:")
        sr = technical.get('support_resistance', {})
        print(f"   - 52W High: {sr.get('52w_high', 0):,.0f}")
        print(f"   - 52W Low: {sr.get('52w_low', 0):,.0f}")
    
    # Valuation
    print("\n3. VALUATION:")
    valuation = analysis.get('valuation', {})
    if valuation:
        print(f"   P/E Ratio: {valuation.get('pe_ratio', 0):.2f}")
        print(f"   P/B Ratio: {valuation.get('pb_ratio', 0):.2f}")
        print(f"   Assessment: {valuation.get('assessment', 'unknown')}")
    
    # Signals
    print("\n4. TRADING SIGNALS:")
    signals = analysis.get('signals', [])
    if signals:
        for signal in signals[:5]:  # Show first 5 signals
            print(f"   [{signal['signal'].upper()}] {signal['message']}")
    else:
        print("   No signals generated")
    
    # Overall Score
    print("\n5. INVESTMENT SCORE:")
    scores = analysis.get('score', {})
    print(f"   Fundamental: {scores.get('fundamental', 0):.1f}/100")
    print(f"   Technical: {scores.get('technical', 0):.1f}/100")
    print(f"   Valuation: {scores.get('valuation', 0):.1f}/100")
    print(f"   OVERALL: {scores.get('overall', 0):.1f}/100")
    
    # Test screening
    print(f"\n{'='*70}")
    print("STOCK SCREENING TEST")
    print(f"{'='*70}")
    
    tickers = ['MWG', 'VNM', 'FPT']
    print(f"\nScreening stocks: {tickers}")
    
    screening_results = analyzer.screen_stocks(
        tickers,
        criteria={
            'min_roe': 0.10,
            'max_de': 2.0,
            'min_revenue_growth': 0
        }
    )
    
    if not screening_results.empty:
        print("\nScreening Results:")
        print(screening_results[['ticker', 'roe', 'debt_to_equity', 'trend', 'overall_score', 'meets_criteria']])
    
    print("\n" + "=" * 70)
    print("✓ Integrated analysis completed successfully!")