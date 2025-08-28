# test_growth_analysis.py
"""
Demo script để test Growth Analysis với dữ liệu mẫu
Sử dụng cho việc develop và debug
"""

import pandas as pd
import numpy as np
import os
import sys

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

try:
    from analysis.fundamental.growth_analyzer import GrowthAnalyzer, analyze_company_growth
except ImportError:
    print("⚠️  Warning: Could not import GrowthAnalyzer. Creating mock class for testing.")
    
    # Mock class for testing if import fails
    class GrowthAnalyzer:
        def __init__(self, data_path):
            self.data_path = data_path
            
        def generate_growth_analysis(self, security_code):
            return {
                'quarterly_data': pd.DataFrame(),
                'annual_data': pd.DataFrame(),
                'ttm_growth_data': pd.DataFrame(),
                'quarterly_margins': pd.DataFrame(),
                'annual_margins': pd.DataFrame()
            }
    
    def analyze_company_growth(data_path, security_code):
        return {
            'quarterly_data': pd.DataFrame(),
            'annual_data': pd.DataFrame(),
            'ttm_growth_data': pd.DataFrame(),
            'quarterly_margins': pd.DataFrame(),
            'annual_margins': pd.DataFrame()
        }

def create_sample_data():
    """
    Tạo dữ liệu mẫu để test nếu chưa có file parquet
    """
    # Tạo dữ liệu mẫu dựa trên structure của file CSV
    np.random.seed(42)  # Để có kết quả reproducible
    
    years = [2019, 2020, 2021, 2022, 2023, 2024]
    quarters = [1, 2, 3, 4]
    security_codes = ['MWG', 'FPT', 'VNM', 'VIC', 'GAS']
    
    # Các METRIC_CODE chính
    metric_codes = [
        'CIS_10',  # Revenue
        'CIS_11',  # COGS
        'CIS_20',  # Gross profit
        'CIS_21',  # Financial Income
        'CIS_22',  # Financial Expense
        'CIS_25',  # Selling expense
        'CIS_26',  # Admin expense
        'CIS_50',  # PBT
        'CIS_62',  # Net Profit after minorities interest
        'CBS_270',  # Total Assets
        'CBS_400',  # Total Equity
    ]
    
    data = []
    
    for security_code in security_codes:
        # Tạo base values cho từng công ty
        base_revenue = np.random.uniform(10000, 50000)  # VND billions
        
        for year in years:
            # Growth factor theo năm
            growth_factor = (1 + (year - 2019) * 0.1 + np.random.uniform(-0.05, 0.1))
            
            for quarter in quarters:
                # Seasonal factor
                seasonal_factor = {1: 0.9, 2: 0.95, 3: 1.05, 4: 1.1}[quarter]
                
                # Quarterly data
                revenue = base_revenue * growth_factor * seasonal_factor
                cogs = revenue * np.random.uniform(0.65, 0.75)
                gross_profit = revenue - cogs
                selling_exp = revenue * np.random.uniform(0.08, 0.12)
                admin_exp = revenue * np.random.uniform(0.05, 0.08)
                fin_income = revenue * np.random.uniform(0.01, 0.03)
                fin_expense = revenue * np.random.uniform(0.01, 0.02)
                operating_profit = gross_profit - selling_exp - admin_exp
                pbt = operating_profit + fin_income - fin_expense
                net_profit = pbt * np.random.uniform(0.8, 0.9)  # After tax
                
                total_assets = revenue * np.random.uniform(1.2, 2.0)
                total_equity = total_assets * np.random.uniform(0.3, 0.5)
                
                # Tạo records cho từng metric (đã sửa theo đúng mapping)
                metrics_values = {
                    'CIS_10': revenue,  # Doanh thu
                    'CIS_11': cogs,  # Giá vốn hàng bán
                    'CIS_20': gross_profit,  # Lợi nhuận gộp
                    'CIS_21': fin_income,  # Thu nhập tài chính
                    'CIS_22': -fin_expense,  # Chi phí tài chính (âm)
                    'CIS_25': -selling_exp,  # Chi phí bán hàng (âm)
                    'CIS_26': -admin_exp,  # Chi phí quản lý DN (âm)
                    'CIS_50': pbt,  # Lợi nhuận trước thuế
                    'CIS_62': net_profit,  # Lợi nhuận sau thuế công ty mẹ
                    'CBS_270': total_assets,  # Tổng tài sản
                    'CBS_400': total_equity,  # Vốn chủ sở hữu
                }
                
                for metric_code in metric_codes:
                    data.append({
                        'REPORT_DATE': f'{year}-{quarter:02d}-01',
                        'YEAR': year,
                        'QUARTER': quarter,
                        'FREQ_CODE': 'Q',
                        'SECURITY_CODE': security_code,
                        'AUDITED': 'Y',
                        'METRIC_CODE': metric_code,
                        'Mô tả': f'Description for {metric_code}',
                        'METRIC_VALUE': metrics_values[metric_code],
                        'ICB_L2': 'Sample Industry'
                    })
                
                # Annual data (chỉ tạo cho Q4)
                if quarter == 4:
                    annual_revenue = revenue * 4 * np.random.uniform(0.95, 1.05)
                    annual_cogs = annual_revenue * np.random.uniform(0.65, 0.75)
                    annual_gross_profit = annual_revenue - annual_cogs
                    annual_selling_exp = annual_revenue * np.random.uniform(0.08, 0.12)
                    annual_admin_exp = annual_revenue * np.random.uniform(0.05, 0.08)
                    annual_fin_income = annual_revenue * np.random.uniform(0.01, 0.03)
                    annual_fin_expense = annual_revenue * np.random.uniform(0.01, 0.02)
                    annual_operating_profit = annual_gross_profit - annual_selling_exp - annual_admin_exp
                    annual_pbt = annual_operating_profit + annual_fin_income - annual_fin_expense
                    annual_net_profit = annual_pbt * np.random.uniform(0.8, 0.9)
                    
                    annual_metrics_values = {
                        'CIS_10': annual_revenue,  # Doanh thu
                        'CIS_11': annual_cogs,  # Giá vốn hàng bán  
                        'CIS_20': annual_gross_profit,  # Lợi nhuận gộp
                        'CIS_21': annual_fin_income,  # Thu nhập tài chính
                        'CIS_22': -annual_fin_expense,  # Chi phí tài chính (âm)
                        'CIS_25': -annual_selling_exp,  # Chi phí bán hàng (âm)
                        'CIS_26': -annual_admin_exp,  # Chi phí quản lý DN (âm)
                        'CIS_50': annual_pbt,  # Lợi nhuận trước thuế
                        'CIS_62': annual_net_profit,  # Lợi nhuận sau thuế
                        'CBS_270': total_assets,  # Tổng tài sản
                        'CBS_400': total_equity,  # Vốn chủ sở hữu
                    }
                    
                    for metric_code in metric_codes:
                        data.append({
                            'REPORT_DATE': f'{year}-12-31',
                            'YEAR': year,
                            'QUARTER': 4,
                            'FREQ_CODE': 'Y',
                            'SECURITY_CODE': security_code,
                            'AUDITED': 'Y',
                            'METRIC_CODE': metric_code,
                            'Mô tả': f'Annual Description for {metric_code}',
                            'METRIC_VALUE': annual_metrics_values[metric_code],
                            'ICB_L2': 'Sample Industry'
                        })
    
    df = pd.DataFrame(data)
    return df

def test_with_sample_data():
    """
    Test Growth Analysis với dữ liệu mẫu
    """
    print("🔧 Creating sample data for testing...")
    
    # Tạo dữ liệu mẫu
    sample_df = create_sample_data()
    
    # Lưu thành file parquet tạm
    sample_file = "sample_financial_data.parquet"
    sample_df.to_parquet(sample_file, engine="pyarrow")
    print(f"✅ Sample data created: {len(sample_df)} records")
    
    # Test với mã MWG
    print("\n🚀 Testing Growth Analysis with MWG...")
    
    try:
        analyzer = GrowthAnalyzer(sample_file)
        results = analyzer.generate_growth_analysis("MWG")
        
        print("✅ Analysis completed successfully!")
        
        # Hiển thị kết quả mẫu
        print("\n📊 Sample Results:")
        print("1. Quarterly Data Shape:", results['quarterly_data'].shape)
        print("2. Annual Data Shape:", results['annual_data'].shape)
        print("3. TTM Growth Data Shape:", results['ttm_growth_data'].shape)
        
        # Hiển thị dữ liệu mẫu
        if not results['quarterly_data'].empty:
            print("\n📅 Latest Quarterly Data:")
            print(results['quarterly_data'].tail(2).to_string())
            
        if not results['annual_data'].empty:
            print("\n📅 Latest Annual Data:")
            print(results['annual_data'].tail(2).to_string())
            
        if not results['ttm_growth_data'].empty:
            print("\n📈 Latest TTM Growth:")
            cols_to_show = ['Year', 'Quarter', 'Revenue_TTM_Growth', 'Operating_Profit_TTM_Growth', 'Net_Profit_TTM_Growth']
            available_cols = [col for col in cols_to_show if col in results['ttm_growth_data'].columns]
            print(results['ttm_growth_data'][available_cols].tail(2).to_string())
        
        # Test xuất CSV (chỉ khi có dữ liệu)
        print("\n💾 Exporting to CSV files...")
        
        if not results['quarterly_data'].empty:
            results['quarterly_data'].to_csv('test_quarterly_data.csv', index=False)
            print("✅ Quarterly data exported")
            
        if not results['annual_data'].empty:
            results['annual_data'].to_csv('test_annual_data.csv', index=False)
            print("✅ Annual data exported")
            
        if not results['ttm_growth_data'].empty:
            results['ttm_growth_data'].to_csv('test_ttm_growth.csv', index=False)
            print("✅ TTM growth data exported")
            
        if not results['quarterly_margins'].empty:
            results['quarterly_margins'].to_csv('test_quarterly_margins.csv', index=False)
            print("✅ Quarterly margins exported")
            
        if not results['annual_margins'].empty:
            results['annual_margins'].to_csv('test_annual_margins.csv', index=False)
            print("✅ Annual margins exported")
        
        print("✅ CSV export completed!")
        
        # Clean up
        if os.path.exists(sample_file):
            os.remove(sample_file)
            
    except Exception as e:
        print(f"❌ Error during testing: {str(e)}")
        import traceback
        traceback.print_exc()

def test_with_real_data(parquet_file_path, security_code="MWG"):
    """
    Test với dữ liệu thật từ file parquet
    """
    if not os.path.exists(parquet_file_path):
        print(f"❌ File not found: {parquet_file_path}")
        return
        
    print(f"🚀 Testing with real data: {parquet_file_path}")
    print(f"📈 Analyzing: {security_code}")
    
    try:
        results = analyze_company_growth(parquet_file_path, security_code)
        
        # Hiển thị thông tin cơ bản
        print(f"\n📊 Analysis Results for {security_code}:")
        for key, df in results.items():
            if isinstance(df, pd.DataFrame):
                print(f"  - {key}: {df.shape[0]} rows, {df.shape[1]} columns")
        
        print("✅ Real data analysis completed!")
        
    except Exception as e:
        print(f"❌ Error with real data: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🧪 Growth Analysis Testing Suite")
    print("=" * 50)
    
    try:
        # Test 1: Với dữ liệu mẫu
        print("\n1️⃣  Testing with Sample Data")
        test_with_sample_data()
        
        # Test 2: Với dữ liệu thật (nếu có)
        real_data_path = "./Database/Buu_clean_ver2.parquet"
        
        if os.path.exists(real_data_path):
            print("\n2️⃣  Testing with Real Data")
            test_with_real_data(real_data_path, "MWG")
        else:
            print(f"\n2️⃣  Real data not found at: {real_data_path}")
            print("     Make sure the Database folder contains Buu_clean_ver2.parquet")
            
    except Exception as e:
        print(f"\n❌ Test suite failed: {str(e)}")
        import traceback
        traceback.print_exc()
    
    print("\n🎉 Testing completed!")