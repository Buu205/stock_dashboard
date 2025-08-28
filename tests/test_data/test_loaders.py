#!/usr/bin/env python3
"""
Test script for Financial Data Loader
Run this to verify everything is working correctly
"""

import sys
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# Add src to path
current_dir = Path(__file__).parent
src_path = (current_dir / "../../src").resolve()
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Import our modules
from core.config import Config
from data.loaders.financial_loader import FinancialDataLoader


def print_section(title: str):
    """Print a formatted section header"""
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def test_config():
    """Test configuration loading"""
    print_section("1. TESTING CONFIGURATION")
    
    try:
        # Load config
        config = get_config()
        print(f"✓ Config loaded: {config.app_name} v{config.version}")
        print(f"  Debug mode: {config.debug}")
        
        # Check paths
        print(f"\n✓ Paths validated:")
        print(f"  Parquet: {config.paths.parquet_path.name}")
        print(f"  Metadata: {config.paths.metadata_path.name}")
        print(f"  Cache dir: {config.paths.cache_dir}")
        
        # Check metrics
        all_metrics = config.metrics.get_all_metrics()
        print(f"\n✓ Metrics loaded: {len(all_metrics)} total")
        print(f"  Sample metrics:")
        for i, (name, code) in enumerate(list(all_metrics.items())[:5]):
            print(f"    {name}: {code}")
        
        # Test metric lookups
        print(f"\n✓ Metric lookups:")
        print(f"  get_metric_code('revenue'): {get_metric_code('revenue')}")
        print(f"  get_metric_name('CIS_20'): {get_metric_name('CIS_20')}")
        
        return True
        
    except Exception as e:
        print(f"✗ Config test failed: {e}")
        return False


def test_data_loading():
    """Test data loading functionality"""
    print_section("2. TESTING DATA LOADING")
    
    try:
        # Initialize loader
        loader = FinancialDataLoader()
        print("✓ Loader initialized")
        
        # Load data (first time - from file)
        print("\nLoading data from file...")
        start = datetime.now()
        data = loader.load_data(force_reload=True)
        load_time = (datetime.now() - start).total_seconds()
        
        print(f"✓ Data loaded in {load_time:.2f} seconds")
        print(f"  Records: {len(data):,}")
        print(f"  Companies: {data['SECURITY_CODE'].nunique()}")
        print(f"  Metrics: {data['METRIC_CODE'].nunique()}")
        print(f"  Years: {int(data['YEAR'].min())}-{int(data['YEAR'].max())}")
        
        # Test cache loading
        print("\nTesting cache...")
        start = datetime.now()
        data2 = loader.load_data()  # Should load from cache
        cache_time = (datetime.now() - start).total_seconds()
        
        print(f"✓ Cache loaded in {cache_time:.2f} seconds")
        print(f"  Speed improvement: {load_time/cache_time:.1f}x faster")
        
        # Get companies list
        companies = loader.get_companies_list()
        print(f"\n✓ Companies list: {len(companies)} total")
        print(f"  First 10: {companies[:10]}")
        
        return loader, companies
        
    except Exception as e:
        print(f"✗ Data loading failed: {e}")
        import traceback
        traceback.print_exc()
        return None, None


def test_company_analysis(loader: FinancialDataLoader, ticker: str = 'MWG'):
    """Test company-specific analysis"""
    print_section(f"3. TESTING COMPANY ANALYSIS ({ticker})")
    
    try:
        # Get raw company data
        print(f"\nGetting data for {ticker}...")
        company_data = loader.get_company_data(ticker, freq='Y')
        print(f"✓ Raw data: {len(company_data)} records")
        
        # Show available metrics for this company
        available_metrics = company_data['METRIC_CODE'].unique()
        print(f"  Available metrics: {len(available_metrics)}")
        print(f"  Sample: {list(available_metrics[:10])}")
        
        # Get financials in wide format
        print(f"\nConverting to wide format...")
        wide_data = loader.get_company_financials(ticker, freq='Y')
        print(f"✓ Wide format: {wide_data.shape[0]} years × {wide_data.shape[1]} columns")
        print(f"  Years: {list(wide_data['YEAR'].values)}")
        
        # Calculate operating profit
        print(f"\n{'='*50}")
        print(f"Operating Profit Analysis for {ticker}:")
        print(f"{'='*50}")
        
        op_data = loader.calculate_operating_profit(ticker, 'Y')
        
        if not op_data.empty:
            # Display table
            display_cols = ['YEAR', 'REVENUE', 'SELLING_EXPENSE', 'ADMIN_EXPENSE', 
                          'OPERATING_PROFIT', 'OPERATING_MARGIN']
            available_cols = [col for col in display_cols if col in op_data.columns]
            
            print("\nYearly Operating Performance:")
            print(op_data[available_cols].to_string(index=False))
            
            # Show growth rates
            print("\nGrowth Rates:")
            growth_cols = ['YEAR', 'REVENUE_GROWTH', 'OPERATING_PROFIT_GROWTH']
            available_growth = [col for col in growth_cols if col in op_data.columns]
            
            growth_df = op_data[available_growth].copy()
            for col in ['REVENUE_GROWTH', 'OPERATING_PROFIT_GROWTH']:
                if col in growth_df.columns:
                    growth_df[col] = (growth_df[col] * 100).round(1).astype(str) + '%'
            
            print(growth_df.to_string(index=False))
        
        # Calculate financial ratios
        print(f"\n{'='*50}")
        print(f"Financial Ratios for {ticker}:")
        print(f"{'='*50}")
        
        ratios = loader.calculate_financial_ratios(ticker, 'Y')
        
        if not ratios.empty:
            # Display key ratios
            ratio_cols = ['YEAR', 'ROE', 'ROA', 'NET_MARGIN', 'DEBT_TO_EQUITY']
            available_ratios = [col for col in ratio_cols if col in ratios.columns]
            
            if len(available_ratios) > 1:
                display_ratios = ratios[available_ratios].tail(5).copy()
                
                # Format as percentages
                for col in ['ROE', 'ROA', 'NET_MARGIN']:
                    if col in display_ratios.columns:
                        display_ratios[col] = (display_ratios[col] * 100).round(1).astype(str) + '%'
                
                print("\nKey Financial Ratios (Last 5 Years):")
                print(display_ratios.to_string(index=False))
        
        # Growth analysis
        print(f"\n{'='*50}")
        print(f"Growth Analysis for {ticker} (5-Year CAGR):")
        print(f"{'='*50}")
        
        growth = loader.get_growth_analysis(ticker, periods=5)
        
        if growth:
            for key, value in growth.items():
                if 'cagr' in key:
                    metric = key.replace('_cagr', '').replace('_', ' ').title()
                    print(f"  {metric}: {value}%")
        
        return True
        
    except Exception as e:
        print(f"✗ Company analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_comparison(loader: FinancialDataLoader, companies: list):
    """Test company comparison functionality"""
    print_section("4. TESTING COMPANY COMPARISON")
    
    try:
        # Select companies to compare
        tickers = companies[:5] if len(companies) >= 5 else companies
        print(f"Comparing companies: {tickers}")
        
        # Select metrics
        metrics = [
            get_metric_code('revenue'),
            get_metric_code('net_profit'),
            get_metric_code('total_assets'),
            get_metric_code('equity')
        ]
        
        # Get comparison for latest year
        comparison = loader.compare_companies(tickers, metrics, year=2023)
        
        if not comparison.empty:
            print("\n2023 Comparison:")
            
            # Format large numbers
            for col in comparison.columns:
                if col not in ['TICKER', 'YEAR', 'QUARTER']:
                    if comparison[col].dtype in ['float64', 'int64']:
                        comparison[col] = comparison[col].apply(
                            lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A"
                        )
            
            print(comparison.to_string(index=False))
            
            return True
        else:
            print("No comparison data available for 2023")
            return False
            
    except Exception as e:
        print(f"✗ Comparison test failed: {e}")
        return False


def test_quarterly_data(loader: FinancialDataLoader, ticker: str = 'MWG'):
    """Test quarterly data analysis"""
    print_section(f"5. TESTING QUARTERLY DATA ({ticker})")
    
    try:
        # Get quarterly data
        print(f"\nGetting quarterly data for {ticker}...")
        quarterly_data = loader.get_company_financials(
            ticker, 
            freq='Q',
            years=[2022, 2023, 2024]
        )
        
        if not quarterly_data.empty:
            print(f"✓ Quarterly data: {len(quarterly_data)} quarters")
            
            # Show recent quarters
            print("\nRecent Quarters:")
            display_df = quarterly_data[['YEAR', 'QUARTER']].tail(8).copy()
            display_df['Period'] = display_df['YEAR'].astype(str) + 'Q' + display_df['QUARTER'].astype(str)
            
            # Add revenue if available
            revenue_code = get_metric_code('revenue')
            if revenue_code in quarterly_data.columns:
                display_df['Revenue'] = quarterly_data[revenue_code].tail(8).apply(
                    lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A"
                )
            
            print(display_df[['Period', 'Revenue']].to_string(index=False))
            
            return True
        else:
            print(f"No quarterly data available for {ticker}")
            return False
            
    except Exception as e:
        print(f"✗ Quarterly data test failed: {e}")
        return False


def main():
    """Main test function"""
    print("\n" + "=" * 70)
    print(" FINANCIAL DATA LOADER TEST SUITE")
    print(" Testing all components...")
    print("=" * 70)
    
    start_time = datetime.now()
    tests_passed = 0
    tests_total = 5
    
    # Test 1: Configuration
    if test_config():
        tests_passed += 1
    
    # Test 2: Data Loading
    loader, companies = test_data_loading()
    if loader and companies:
        tests_passed += 1
        
        # Test 3: Company Analysis
        test_ticker = 'MWG' if 'MWG' in companies else companies[0]
        if test_company_analysis(loader, test_ticker):
            tests_passed += 1
        
        # Test 4: Company Comparison
        if test_comparison(loader, companies):
            tests_passed += 1
        
        # Test 5: Quarterly Data
        if test_quarterly_data(loader, test_ticker):
            tests_passed += 1
    
    # Summary
    print_section("TEST SUMMARY")
    
    total_time = (datetime.now() - start_time).total_seconds()
    print(f"Tests Passed: {tests_passed}/{tests_total}")
    print(f"Success Rate: {tests_passed/tests_total*100:.1f}%")
    print(f"Total Time: {total_time:.2f} seconds")
    
    if tests_passed == tests_total:
        print("\n✅ ALL TESTS PASSED! System is ready to use.")
    else:
        print(f"\n⚠️ {tests_total - tests_passed} test(s) failed. Please check the errors above.")
    
    print("\n" + "=" * 70)
    print(" Next steps:")
    print(" 1. If all tests passed, you can start building the Streamlit app")
    print(" 2. Run: streamlit run app.py")
    print("=" * 70)


if __name__ == "__main__":
    main()