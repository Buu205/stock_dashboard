#!/usr/bin/env python3
"""
Phân loại mã cổ phiếu thành 2 nhóm:
1. Mã chuẩn: Doanh nghiệp thông thường (không phải tài chính/ngân hàng)
2. Mã đặc biệt: Ngân hàng và toàn bộ công ty dịch vụ tài chính

Sau đó tách dữ liệu thành 2 file parquet riêng biệt.
"""

import pandas as pd
import json
import os
from datetime import datetime

def classify_and_split_data():
    """Phân loại mã và tách dữ liệu thành 2 file parquet"""
    
    print("📊 PHÂN LOẠI VÀ TÁCH DỮ LIỆU CỔ PHIẾU")
    print("="*70)
    
    # Load data
    print("\n1. Đang load dữ liệu...")
    df = pd.read_parquet('Database/Buu_clean_ver2.parquet')
    
    # Get unique tickers with their industry
    ticker_industry = df.groupby('SECURITY_CODE')['ICB_L2'].first().reset_index()
    ticker_industry.columns = ['Ticker', 'Industry_ICB_L2']
    
    print(f"   • Tổng số mã: {len(ticker_industry)}")
    
    # Separate by industry first
    # All financial services and banks go to special list
    financial_industries = ['Ngân hàng', 'Dịch vụ tài chính']
    
    # Get financial tickers
    special_df = ticker_industry[ticker_industry['Industry_ICB_L2'].isin(financial_industries)]
    standard_df = ticker_industry[~ticker_industry['Industry_ICB_L2'].isin(financial_industries)]
    
    # Sort by ticker
    standard_df = standard_df.sort_values('Ticker').reset_index(drop=True)
    special_df = special_df.sort_values('Ticker').reset_index(drop=True)
    
    print("\n2. Kết quả phân loại:")
    print(f"   ✅ Mã chuẩn (không phải tài chính): {len(standard_df)} mã")
    print(f"   🏦 Mã đặc biệt (ngân hàng + dịch vụ tài chính): {len(special_df)} mã")
    
    # Display industry breakdown
    print("\n3. Phân bố theo ngành:")
    print("\n   📊 Mã chuẩn:")
    standard_industries = standard_df['Industry_ICB_L2'].value_counts()
    for industry, count in standard_industries.head(10).items():
        print(f"      • {industry}: {count} mã")
    if len(standard_industries) > 10:
        print(f"      ... và {len(standard_industries)-10} ngành khác")
    
    print("\n   🏦 Mã đặc biệt:")
    special_industries = special_df['Industry_ICB_L2'].value_counts()
    for industry, count in special_industries.items():
        print(f"      • {industry}: {count} mã")
    
    # Show all financial service companies
    print("\n   Danh sách mã dịch vụ tài chính:")
    financial_services = special_df[special_df['Industry_ICB_L2'] == 'Dịch vụ tài chính']['Ticker'].tolist()
    for i in range(0, len(financial_services), 10):
        group = financial_services[i:i+10]
        print(f"      {', '.join(group)}")
    
    # Save CSV files
    print("\n4. Lưu file CSV...")
    standard_df.to_csv('Database/standard_tickers_with_industry.csv', index=False, encoding='utf-8')
    print(f"   ✅ Database/standard_tickers_with_industry.csv ({len(standard_df)} mã)")
    
    special_df.to_csv('Database/special_tickers_with_industry.csv', index=False, encoding='utf-8')
    print(f"   ✅ Database/special_tickers_with_industry.csv ({len(special_df)} mã)")
    
    # Split parquet data
    print("\n5. Tách dữ liệu parquet...")
    
    standard_tickers = standard_df['Ticker'].tolist()
    special_tickers = special_df['Ticker'].tolist()
    
    # Filter data
    standard_data = df[df['SECURITY_CODE'].isin(standard_tickers)]
    special_data = df[df['SECURITY_CODE'].isin(special_tickers)]
    
    print(f"   • Dữ liệu mã chuẩn: {len(standard_data):,} dòng")
    print(f"   • Dữ liệu mã đặc biệt: {len(special_data):,} dòng")
    
    # Verify coverage
    total_rows = len(df)
    covered_rows = len(standard_data) + len(special_data)
    coverage = (covered_rows / total_rows) * 100
    print(f"   • Độ phủ: {coverage:.2f}% ({covered_rows:,}/{total_rows:,} dòng)")
    
    # Save parquet files
    print("\n6. Lưu file parquet...")
    
    standard_data.to_parquet('Database/standard_data.parquet', index=False, compression='snappy')
    standard_size = os.path.getsize('Database/standard_data.parquet') / 1024 / 1024
    print(f"   ✅ Database/standard_data.parquet ({standard_size:.2f} MB)")
    
    special_data.to_parquet('Database/special_data.parquet', index=False, compression='snappy')
    special_size = os.path.getsize('Database/special_data.parquet') / 1024 / 1024
    print(f"   ✅ Database/special_data.parquet ({special_size:.2f} MB)")
    
    # Analyze metric types
    print("\n7. Phân tích metric codes:")
    
    # Standard metrics
    standard_metrics = standard_data['METRIC_CODE'].value_counts()
    standard_metric_types = set()
    for metric in standard_metrics.index[:100]:  # Check top 100
        if pd.notna(metric) and '_' in metric:
            standard_metric_types.add(metric.split('_')[0])
    
    print(f"\n   📊 Mã chuẩn sử dụng metrics: {', '.join(sorted(standard_metric_types))}")
    
    # Special metrics
    special_metrics = special_data['METRIC_CODE'].value_counts()
    special_metric_types = set()
    for metric in special_metrics.index[:100]:  # Check top 100
        if pd.notna(metric) and '_' in metric:
            special_metric_types.add(metric.split('_')[0])
    
    print(f"   🏦 Mã đặc biệt sử dụng metrics: {', '.join(sorted(special_metric_types))}")
    
    # Create summary JSON
    summary = {
        'classification_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'standard': {
            'count': len(standard_df),
            'data_rows': len(standard_data),
            'size_mb': round(standard_size, 2),
            'metric_types': sorted(list(standard_metric_types)),
            'industries': standard_df['Industry_ICB_L2'].value_counts().to_dict()
        },
        'special': {
            'count': len(special_df),
            'data_rows': len(special_data),
            'size_mb': round(special_size, 2),
            'metric_types': sorted(list(special_metric_types)),
            'industries': special_df['Industry_ICB_L2'].value_counts().to_dict(),
            'tickers': special_df.to_dict('records')
        },
        'coverage': {
            'total_rows': total_rows,
            'covered_rows': covered_rows,
            'percentage': round(coverage, 2)
        }
    }
    
    with open('Database/ticker_classification_summary.json', 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"\n   ✅ Database/ticker_classification_summary.json")
    
    print("\n" + "="*70)
    print("📌 TÓM TẮT:")
    print("="*70)
    print(f"• Mã chuẩn: {len(standard_df)} mã (doanh nghiệp thông thường)")
    print(f"• Mã đặc biệt: {len(special_df)} mã (ngân hàng + dịch vụ tài chính)")
    print(f"• File dữ liệu:")
    print(f"  - standard_data.parquet: {standard_size:.2f} MB")
    print(f"  - special_data.parquet: {special_size:.2f} MB")
    print("\n✅ HOÀN THÀNH!")
    
    return standard_df, special_df

if __name__ == "__main__":
    standard_df, special_df = classify_and_split_data()