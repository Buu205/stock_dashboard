"""
Script phân tích tăng trưởng cho TẤT CẢ các mã cổ phiếu trong database
Sử dụng dữ liệu từ Database/Buu_clean_ver2.parquet
Export kết quả ra files CSV cho từng mã
"""

import pandas as pd
import numpy as np
import os
import sys
from datetime import datetime
import time
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from analysis.fundamental.growth_analyzer import GrowthAnalyzer

def get_all_tickers(parquet_path):
    """Lấy danh sách tất cả các mã cổ phiếu từ file parquet"""
    try:
        df = pd.read_parquet(parquet_path)
        tickers = df['SECURITY_CODE'].unique()
        return sorted(tickers)
    except Exception as e:
        print(f"❌ Error loading tickers: {e}")
        return []

def create_output_directory(base_dir="growth_analysis_results"):
    """Tạo thư mục output với timestamp"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(
        os.path.dirname(__file__), 
        f"{base_dir}_{timestamp}"
    )
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def analyze_single_ticker(analyzer, ticker, output_dir):
    """Phân tích một mã cổ phiếu"""
    try:
        # Phân tích
        results = analyzer.generate_growth_analysis(ticker)
        
        # Tạo thư mục cho ticker
        ticker_dir = os.path.join(output_dir, ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        
        # Lưu kết quả
        saved_files = []
        for key, df in results.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Thêm cột STOCK
                if 'STOCK' not in df.columns:
                    df['STOCK'] = ticker
                    
                # Lưu file
                filename = os.path.join(ticker_dir, f"{ticker}_{key}.csv")
                df.to_csv(filename, index=False, encoding='utf-8-sig')
                saved_files.append(key)
        
        return True, saved_files
    
    except Exception as e:
        return False, str(e)

def create_summary_report(results_summary, output_dir):
    """Tạo báo cáo tổng hợp"""
    summary_file = os.path.join(output_dir, "analysis_summary.txt")
    
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("GROWTH ANALYSIS SUMMARY REPORT\n")
        f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        
        # Thống kê tổng quan
        total = len(results_summary)
        successful = sum(1 for r in results_summary if r['success'])
        failed = total - successful
        
        f.write(f"Total tickers processed: {total}\n")
        f.write(f"Successful: {successful}\n")
        f.write(f"Failed: {failed}\n")
        f.write(f"Success rate: {successful/total*100:.1f}%\n\n")
        
        # Chi tiết thành công
        f.write("SUCCESSFUL ANALYSIS:\n")
        f.write("-" * 40 + "\n")
        for result in results_summary:
            if result['success']:
                f.write(f"✅ {result['ticker']}: {', '.join(result['saved_files'])}\n")
        
        # Chi tiết thất bại
        if failed > 0:
            f.write("\nFAILED ANALYSIS:\n")
            f.write("-" * 40 + "\n")
            for result in results_summary:
                if not result['success']:
                    f.write(f"❌ {result['ticker']}: {result['error']}\n")
    
    print(f"\n📊 Summary report saved to: {summary_file}")

def create_consolidated_files(output_dir):
    """Tạo file tổng hợp cho tất cả các ticker"""
    consolidated_dir = os.path.join(output_dir, "_consolidated")
    os.makedirs(consolidated_dir, exist_ok=True)
    
    data_types = [
        'quarterly_data', 'annual_data', 'ttm_growth_data',
        'quarterly_margins', 'annual_margins'
    ]
    
    for data_type in data_types:
        all_data = []
        
        # Duyệt qua tất cả các ticker directories
        for ticker in os.listdir(output_dir):
            if ticker.startswith('_'):
                continue
                
            ticker_dir = os.path.join(output_dir, ticker)
            if os.path.isdir(ticker_dir):
                file_path = os.path.join(ticker_dir, f"{ticker}_{data_type}.csv")
                
                if os.path.exists(file_path):
                    try:
                        df = pd.read_csv(file_path)
                        all_data.append(df)
                    except:
                        pass
        
        # Gộp và lưu file tổng hợp
        if all_data:
            consolidated_df = pd.concat(all_data, ignore_index=True)
            consolidated_file = os.path.join(consolidated_dir, f"all_tickers_{data_type}.csv")
            consolidated_df.to_csv(consolidated_file, index=False, encoding='utf-8-sig')
            print(f"✅ Consolidated {data_type}: {consolidated_df.shape[0]} rows from {len(all_data)} tickers")

def main():
    """Main function để chạy phân tích cho tất cả ticker"""
    print("🚀 BATCH GROWTH ANALYSIS FOR ALL TICKERS")
    print("=" * 60)
    
    # Đường dẫn data
    parquet_path = "../../Database/Buu_clean_ver2.parquet"
    
    if not os.path.exists(parquet_path):
        print(f"❌ Data file not found: {parquet_path}")
        return
    
    # Lấy danh sách ticker
    print("\n📋 Loading ticker list...")
    tickers = get_all_tickers(parquet_path)
    
    if not tickers:
        print("❌ No tickers found in database")
        return
    
    print(f"✅ Found {len(tickers)} tickers")
    print(f"📊 Sample tickers: {', '.join(tickers[:10])}")
    
    # Tạo output directory
    output_dir = create_output_directory()
    print(f"\n📁 Output directory: {output_dir}")
    
    # Khởi tạo analyzer
    print("\n🔧 Initializing analyzer...")
    analyzer = GrowthAnalyzer(parquet_path)
    
    # Phân tích từng ticker
    print(f"\n🏃 Starting analysis for {len(tickers)} tickers...")
    results_summary = []
    
    # Sử dụng tqdm để hiển thị progress bar
    try:
        from tqdm import tqdm
        ticker_list = tqdm(tickers, desc="Processing tickers")
    except ImportError:
        print("Note: Install tqdm for progress bar (pip install tqdm)")
        ticker_list = tickers
    
    start_time = time.time()
    
    for ticker in ticker_list:
        if hasattr(ticker_list, 'set_description'):
            ticker_list.set_description(f"Processing {ticker}")
        
        success, result = analyze_single_ticker(analyzer, ticker, output_dir)
        
        results_summary.append({
            'ticker': ticker,
            'success': success,
            'saved_files': result if success else [],
            'error': result if not success else None
        })
        
        # Optional: Add small delay to avoid overwhelming the system
        # time.sleep(0.1)
    
    elapsed_time = time.time() - start_time
    
    # Tạo báo cáo tổng hợp
    print("\n📝 Creating summary report...")
    create_summary_report(results_summary, output_dir)
    
    # Tạo files tổng hợp
    print("\n📦 Creating consolidated files...")
    create_consolidated_files(output_dir)
    
    # Thống kê cuối cùng
    total = len(results_summary)
    successful = sum(1 for r in results_summary if r['success'])
    failed = total - successful
    
    print("\n" + "=" * 60)
    print("✨ ANALYSIS COMPLETED!")
    print(f"⏱️  Total time: {elapsed_time:.2f} seconds")
    print(f"📊 Total processed: {total}")
    print(f"✅ Successful: {successful} ({successful/total*100:.1f}%)")
    print(f"❌ Failed: {failed} ({failed/total*100:.1f}%)")
    print(f"📁 Results saved to: {output_dir}")
    print("=" * 60)

if __name__ == "__main__":
    main()