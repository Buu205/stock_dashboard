#!/usr/bin/env python3
"""
Complete Integration Test
Tests all components: Financial Data + Market Data + Analysis
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# Add parent directory to path
current_dir = Path(__file__).parent
parent_path = current_dir.parent.parent
sys.path.insert(0, str(parent_path))

# Import all modules
from src.core.config import get_config
from src.data.loaders.financial_loader import FinancialDataLoader
from src.data.connectors.tcbs_connector import TCBSConnector
from src.analysis.technical.integrated_analyzer import IntegratedAnalyzer


def print_header(title: str):
    """Print formatted header"""
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def print_section(title: str):
    """Print formatted section"""
    print(f"\n{'-'*40}")
    print(f" {title}")
    print(f"{'-'*40}")


def test_financial_data():
    """Test financial data loading"""
    print_header("1. TESTING FINANCIAL DATA LOADER")
    
    try:
        loader = FinancialDataLoader()
        data = loader.load_data()
        
        print(f"✓ Financial data loaded: {len(data):,} records")
        print(f"  Companies: {data['SECURITY_CODE'].nunique()}")
        print(f"  Years: {int(data['YEAR'].min())}-{int(data['YEAR'].max())}")
        
        # Test with MWG
        mwg_profit = loader.calculate_operating_profit('MWG', 'Y')
        if not mwg_profit.empty:
            print(f"\n✓ MWG Operating Profit calculated:")
            print(f"  Years analyzed: {len(mwg_profit)}")
            print(f"  Latest revenue: {mwg_profit['REVENUE'].iloc[-1]:,.0f}")
            print(f"  Latest OP margin: {mwg_profit['OPERATING_MARGIN'].iloc[-1]*100:.1f}%")
            return True, loader
        
        return False, None
        
    except Exception as e:
        print(f"✗ Financial data test failed: {e}")
        return False, None


def test_market_data():
    """Test market data from TCBS"""
    print_header("2. TESTING TCBS MARKET DATA CONNECTOR")
    
    try:
        connector = TCBSConnector()
        
        # Test 1: Fetch price data
        print("\nFetching price data for MWG...")
        price_data = connector.fetch_historical_price(
            'MWG', 
            start_date='2024-01-01'
        )
        
        if not price_data.empty:
            print(f"✓ Price data fetched: {len(price_data)} days")
            print(f"  Date range: {price_data['date'].min().date()} to {price_data['date'].max().date()}")
            print(f"  Latest price: {price_data['close'].iloc[-1]:,.0f}")
            
            # Test 2: Calculate indicators
            print("\nCalculating technical indicators...")
            with_indicators = connector.calculate_technical_indicators(price_data)
            
            if 'RSI' in with_indicators.columns:
                print(f"✓ Technical indicators calculated")
                print(f"  RSI: {with_indicators['RSI'].iloc[-1]:.1f}")
                print(f"  SMA20: {with_indicators['SMA_20'].iloc[-1]:,.0f}")
                
                return True, connector
        
        return False, None
        
    except Exception as e:
        print(f"✗ Market data test failed: {e}")
        import traceback
        traceback.print_exc()
        return False, None


def test_integrated_analysis():
    """Test integrated analysis"""
    print_header("3. TESTING INTEGRATED ANALYZER")
    
    try:
        analyzer = IntegratedAnalyzer()
        
        # Analyze MWG
        print("\nPerforming comprehensive analysis for MWG...")
        analysis = analyzer.analyze_stock('MWG', start_date='2024-01-01')
        
        if analysis:
            # Check fundamental analysis
            fundamental = analysis.get('fundamental', {})
            if fundamental:
                print(f"\n✓ Fundamental Analysis:")
                print(f"  Quality Score: {fundamental.get('quality_score', 0):.1f}/100")
                print(f"  ROE: {fundamental.get('ratios', {}).get('roe', 0)*100:.1f}%")
                print(f"  Revenue CAGR: {fundamental.get('growth', {}).get('revenue_cagr', 0):.1f}%")
            
            # Check technical analysis
            technical = analysis.get('technical', {})
            if technical:
                print(f"\n✓ Technical Analysis:")
                print(f"  Current Price: {technical.get('current_price', 0):,.0f}")
                print(f"  Trend: {technical.get('trend', 'unknown')}")
                print(f"  RSI: {technical.get('indicators', {}).get('rsi', 0):.1f}")
            
            # Check valuation
            valuation = analysis.get('valuation', {})
            if valuation:
                print(f"\n✓ Valuation Analysis:")
                print(f"  Assessment: {valuation.get('assessment', 'unknown')}")
            
            # Check signals
            signals = analysis.get('signals', [])
            print(f"\n✓ Trading Signals: {len(signals)} generated")
            if signals:
                for signal in signals[:3]:
                    print(f"  - [{signal['signal']}] {signal['message']}")
            
            # Check overall score
            score = analysis.get('score', {})
            print(f"\n✓ Investment Score:")
            print(f"  Overall: {score.get('overall', 0):.1f}/100")
            
            return True, analyzer
        
        return False, None
        
    except Exception as e:
        print(f"✗ Integrated analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return False, None


def test_stock_screening():
    """Test stock screening functionality"""
    print_header("4. TESTING STOCK SCREENING")
    
    try:
        analyzer = IntegratedAnalyzer()
        
        # Test with a few stocks
        tickers = ['MWG', 'VNM', 'FPT', 'VIC', 'HPG']
        print(f"\nScreening {len(tickers)} stocks: {tickers}")
        
        criteria = {
            'min_roe': 0.10,
            'max_de': 2.0,
            'min_revenue_growth': 0,
            'rsi_range': (20, 80)
        }
        
        print("\nScreening criteria:")
        for key, value in criteria.items():
            print(f"  {key}: {value}")
        
        print("\nRunning screening...")
        results = analyzer.screen_stocks(tickers[:3], criteria)  # Test with first 3
        
        if not results.empty:
            print(f"\n✓ Screening completed:")
            print(f"  Stocks analyzed: {len(results)}")
            print(f"  Stocks meeting criteria: {results['meets_criteria'].sum()}")
            
            print("\nTop stocks by score:")
            display_cols = ['ticker', 'overall_score', 'roe', 'trend', 'meets_criteria']
            print(results[display_cols].head())
            
            return True
        
        return False
        
    except Exception as e:
        print(f"✗ Stock screening failed: {e}")
        return False


def test_performance():
    """Test system performance"""
    print_header("5. TESTING SYSTEM PERFORMANCE")
    
    try:
        # Test data loading speed
        print("\nTesting data loading performance...")
        
        # Financial data
        start = datetime.now()
        loader = FinancialDataLoader()
        loader.load_data(force_reload=True)  # Force reload
        financial_time = (datetime.now() - start).total_seconds()
        print(f"  Financial data load time: {financial_time:.2f}s")
        
        # From cache
        start = datetime.now()
        loader.load_data()  # From cache
        cache_time = (datetime.now() - start).total_seconds()
        print(f"  Cache load time: {cache_time:.2f}s")
        print(f"  Speed improvement: {financial_time/cache_time:.1f}x")
        
        # Market data
        print("\nTesting market data fetch...")
        connector = TCBSConnector()
        
        start = datetime.now()
        price_data = connector.fetch_historical_price('MWG', start_date='2024-01-01')
        market_time = (datetime.now() - start).total_seconds()
        print(f"  Market data fetch time: {market_time:.2f}s")
        
        # Full analysis
        print("\nTesting full analysis speed...")
        analyzer = IntegratedAnalyzer()
        
        start = datetime.now()
        analysis = analyzer.analyze_stock('MWG')
        analysis_time = (datetime.now() - start).total_seconds()
        print(f"  Complete analysis time: {analysis_time:.2f}s")
        
        print(f"\n✓ Performance test completed")
        print(f"  Total test time: {financial_time + market_time + analysis_time:.2f}s")
        
        return True
        
    except Exception as e:
        print(f"✗ Performance test failed: {e}")
        return False


def run_complete_test():
    """Run all tests"""
    print("\n" + "=" * 80)
    print(" COMPLETE INTEGRATION TEST SUITE")
    print(" Testing Financial Data + Market Data + Analysis")
    print("=" * 80)
    
    start_time = datetime.now()
    tests_passed = 0
    tests_total = 5
    
    # Test 1: Financial Data
    financial_ok, loader = test_financial_data()
    if financial_ok:
        tests_passed += 1
    
    # Test 2: Market Data
    market_ok, connector = test_market_data()
    if market_ok:
        tests_passed += 1
    
    # Test 3: Integrated Analysis
    analysis_ok, analyzer = test_integrated_analysis()
    if analysis_ok:
        tests_passed += 1
    
    # Test 4: Stock Screening
    if test_stock_screening():
        tests_passed += 1
    
    # Test 5: Performance
    if test_performance():
        tests_passed += 1
    
    # Summary
    print_header("TEST SUMMARY")
    
    total_time = (datetime.now() - start_time).total_seconds()
    
    print(f"\nTests Passed: {tests_passed}/{tests_total}")
    print(f"Success Rate: {tests_passed/tests_total*100:.1f}%")
    print(f"Total Time: {total_time:.2f} seconds")
    
    if tests_passed == tests_total:
        print("\n✅ ALL TESTS PASSED!")
        print("\nSystem Status:")
        print("  • Financial data loader: ✓ Ready")
        print("  • Market data connector: ✓ Ready")
        print("  • Integrated analyzer: ✓ Ready")
        print("  • Stock screening: ✓ Ready")
        print("  • Performance: ✓ Optimized")
        
        print("\n📊 You can now:")
        print("  1. Build Streamlit dashboard: streamlit run app.py")
        print("  2. Analyze any stock: analyzer.analyze_stock('TICKER')")
        print("  3. Screen multiple stocks: analyzer.screen_stocks(['TICKER1', 'TICKER2'])")
        
    else:
        print(f"\n⚠️ {tests_total - tests_passed} test(s) failed.")
        print("Please check the errors above and fix them.")
    
    print("\n" + "=" * 80)


def quick_analysis_demo():
    """Quick demo of analysis capabilities"""
    print_header("QUICK ANALYSIS DEMO")
    
    try:
        analyzer = IntegratedAnalyzer()
        
        ticker = 'MWG'
        print(f"\nQuick analysis for {ticker}:")
        
        analysis = analyzer.analyze_stock(ticker, start_date='2024-01-01')
        
        # Create summary report
        print(f"\n{'='*60}")
        print(f" {ticker} - INVESTMENT SUMMARY")
        print(f"{'='*60}")
        
        # Score
        score = analysis['score']['overall']
        print(f"\n📊 Overall Score: {score:.1f}/100", end="")
        if score >= 70:
            print(" 🟢 STRONG BUY")
        elif score >= 60:
            print(" 🟡 BUY")
        elif score >= 40:
            print(" 🟠 HOLD")
        else:
            print(" 🔴 SELL")
        
        # Key metrics
        print("\n📈 Key Metrics:")
        print(f"  • ROE: {analysis['fundamental']['ratios']['roe']*100:.1f}%")
        print(f"  • Revenue Growth: {analysis['fundamental']['growth']['revenue_cagr']:.1f}%")
        print(f"  • Current Price: {analysis['technical']['current_price']:,.0f}")
        print(f"  • YTD Return: {analysis['technical']['price_change_ytd']*100:.1f}%")
        print(f"  • Trend: {analysis['technical']['trend']}")
        
        # Top signals
        print("\n🚦 Top Signals:")
        for signal in analysis['signals'][:3]:
            emoji = "🟢" if signal['signal'] in ['buy', 'positive'] else "🔴"
            print(f"  {emoji} {signal['message']}")
        
        print(f"\n{'='*60}")
        
    except Exception as e:
        print(f"Demo failed: {e}")


if __name__ == "__main__":
    # Run complete test
    run_complete_test()
    
    # Run quick demo if all tests pass
    print("\n")
    response = input("Run quick analysis demo? (y/n): ")
    if response.lower() == 'y':
        quick_analysis_demo()