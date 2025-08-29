#!/usr/bin/env python3
"""
Script convert Excel BSC metrics thành YAML mapping - CHỈ CHẠY 1 LẦN
Sau khi convert xong, có thể xóa script này và giữ nguyên file YAML

Author: Stock Dashboard System
Date: 2024-08-29
"""

import pandas as pd
import yaml
import os
from pathlib import Path

def convert_bsc_metrics():
    """Convert Excel BSC metrics thành YAML mapping - CHỈ CHẠY 1 LẦN"""
    
    # Đường dẫn file
    excel_path = "Database/BSC_Forecast/metric_definition.xlsx"
    output_path = "Database/BSC_Forecast/metric_mapping.yaml"
    
    # Kiểm tra file Excel có tồn tại không
    if not os.path.exists(excel_path):
        print(f"❌ Không tìm thấy file Excel: {excel_path}")
        print("💡 Hãy upload file metric_definition.xlsx vào thư mục Database/BSC_Forecast/")
        return
    
    try:
        # Load Excel file
        print(f"📊 Đang load file Excel: {excel_path}")
        excel_file = pd.ExcelFile(excel_path)
        
        print(f"📋 Tìm thấy {len(excel_file.sheet_names)} tabs:")
        for tab in excel_file.sheet_names:
            print(f"   - {tab}")
        
        # Process từng tab
        all_metrics = {}
        total_metrics = 0
        
        for tab_name in excel_file.sheet_names:
            print(f"\n📝 Đang xử lý tab: {tab_name}")
            
            # Load tab data
            df = pd.read_excel(excel_path, sheet_name=tab_name)
            print(f"   📊 Số dòng: {len(df)}")
            print(f"   📋 Cột: {list(df.columns)}")
            
            # Xác định cấu trúc cột
            column_mapping = identify_columns(df.columns)
            print(f"   🔍 Mapping cột: {column_mapping}")
            
            # Convert metrics
            tab_metrics = {}
            for idx, row in df.iterrows():
                metric_info = extract_metric(row, column_mapping)
                if metric_info:
                    tab_metrics[metric_info['name']] = metric_info['code']
                    total_metrics += 1
            
            all_metrics[tab_name] = tab_metrics
            print(f"   ✅ Đã convert {len(tab_metrics)} metrics")
        
        # Save YAML
        print(f"\n💾 Đang lưu {total_metrics} metrics vào {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(all_metrics, f, default_flow_style=False, 
                     allow_unicode=True, sort_keys=False)
        
        print(f"\n🎉 HOÀN THÀNH!")
        print(f"📁 File mapping: {output_path}")
        print(f"📊 Tổng số metrics: {total_metrics}")
        print(f"📋 Số tabs: {len(all_metrics)}")
        
        print(f"\n🚀 BƯỚC TIẾP THEO:")
        print(f"1. Kiểm tra file {output_path}")
        print(f"2. Copy mapping vào config.yaml nếu cần")
        print(f"3. Có thể xóa script này và file Excel gốc")
        print(f"4. Giữ nguyên file {output_path} để sử dụng")
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        print("💡 Hãy kiểm tra cấu trúc file Excel và thử lại")

def identify_columns(columns):
    """Xác định cấu trúc cột của Excel"""
    column_mapping = {}
    
    # Mapping chuẩn
    standard_mappings = {
        'code': ['Code', 'Mã', 'Metric Code', 'METRIC_CODE', 'Mã số'],
        'name': ['Name', 'Tên', 'Metric Name', 'METRIC_NAME', 'Tên chỉ tiêu'],
        'description': ['Description', 'Mô tả', 'Mô tả chi tiết', 'DESCRIPTION', 'Ghi chú']
    }
    
    # Tìm mapping phù hợp
    for standard_key, possible_names in standard_mappings.items():
        for col_name in columns:
            if any(name.lower() in col_name.lower() for name in possible_names):
                column_mapping[standard_key] = col_name
                break
    
    # Nếu không tìm thấy, sử dụng cột đầu tiên làm code
    if 'code' not in column_mapping and len(columns) > 0:
        column_mapping['code'] = columns[0]
    
    # Nếu không tìm thấy name, sử dụng cột thứ 2
    if 'name' not in column_mapping and len(columns) > 1:
        column_mapping['name'] = columns[1]
    
    return column_mapping

def extract_metric(row, column_mapping):
    """Trích xuất thông tin metric từ một dòng"""
    
    # Lấy các giá trị cơ bản
    code = str(row.get(column_mapping.get('code', ''))).strip()
    name = str(row.get(column_mapping.get('name', ''))).strip()
    
    # Bỏ qua nếu không có code hoặc name
    if not code or not name or code.lower() in ['nan', 'none', '']:
        return None
    
    return {
        'code': code,
        'name': name
    }

def create_sample_excel():
    """Tạo file Excel mẫu để test"""
    sample_data = {
        'Code': ['CIS_10', 'CIS_20', 'CBS_270', 'CBS_400'],
        'Name': ['Doanh thu thuần', 'Lợi nhuận gộp', 'Tổng tài sản', 'Vốn chủ sở hữu'],
        'Description': ['Doanh thu từ hoạt động kinh doanh', 'Lợi nhuận trước chi phí', 'Tổng giá trị tài sản', 'Vốn của cổ đông']
    }
    
    df = pd.DataFrame(sample_data)
    output_path = "Database/BSC_Forecast/sample_metric_definition.xlsx"
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Financial_Metrics', index=False)
    
    print(f"📝 Đã tạo file Excel mẫu: {output_path}")
    print("💡 Bạn có thể sử dụng file này để test script convert")

if __name__ == "__main__":
    print("🚀 BSC Metrics Converter - CHỈ CHẠY 1 LẦN")
    print("=" * 50)
    
    # Kiểm tra file Excel có tồn tại không
    excel_path = "Database/BSC_Forecast/metric_definition.xlsx"
    
    if not os.path.exists(excel_path):
        print(f"⚠️ Không tìm thấy file: {excel_path}")
        print("💡 Tạo file Excel mẫu để test? (y/n)")
        
        choice = input().lower().strip()
        if choice in ['y', 'yes', 'có']:
            create_sample_excel()
            print(f"\n✅ Bây giờ hãy:")
            print(f"1. Thay thế file {excel_path} bằng file Excel thực tế từ BSC")
            print(f"2. Chạy lại script này")
        else:
            print("💡 Hãy upload file metric_definition.xlsx từ BSC vào Database/BSC_Forecast/")
    else:
        # Chạy convert
        convert_bsc_metrics()