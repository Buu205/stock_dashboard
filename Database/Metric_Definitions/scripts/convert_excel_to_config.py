#!/usr/bin/env python3
"""
Script convert Excel metric definition sang config files
Chuyển đổi file Excel với nhiều tab thành các file config YAML/JSON

Author: Stock Dashboard System
Date: 2024-08-29
"""

import pandas as pd
import yaml
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MetricDefinitionConverter:
    """Class xử lý convert Excel metric definition sang config files"""
    
    def __init__(self, excel_path: str, output_dir: str = None):
        self.excel_path = Path(excel_path)
        self.output_dir = Path(output_dir) if output_dir else Path("Database/Metric_Definitions")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Cấu trúc mapping chuẩn
        self.standard_categories = {
            'income_statement': ['revenue', 'expense', 'profit'],
            'balance_sheet': ['asset', 'liability', 'equity'],
            'cash_flow': ['operating', 'investing', 'financing'],
            'bank_ratio': ['capital', 'liquidity', 'profitability'],
            'ratio': ['profitability', 'liquidity', 'leverage', 'efficiency']
        }
        
    def convert_excel_to_configs(self) -> Dict[str, Any]:
        """Convert Excel với nhiều tab thành các file config"""
        
        if not self.excel_path.exists():
            raise FileNotFoundError(f"Không tìm thấy file Excel: {self.excel_path}")
        
        logger.info(f"🔄 Bắt đầu convert file: {self.excel_path.name}")
        
        # Load Excel file
        try:
            excel_file = pd.ExcelFile(self.excel_path)
            logger.info(f"📊 Tìm thấy {len(excel_file.sheet_names)} tabs")
        except Exception as e:
            logger.error(f"❌ Lỗi khi load Excel file: {e}")
            raise
        
        # Process từng tab
        all_metrics = {}
        metadata = {}
        validation_results = []
        
        for tab_name in excel_file.sheet_names:
            logger.info(f"📋 Đang xử lý tab: {tab_name}")
            
            try:
                # Load tab data
                tab_data = pd.read_excel(self.excel_path, sheet_name=tab_name)
                
                # Convert tab thành metrics
                tab_metrics, tab_validation = self._process_tab(tab_name, tab_data)
                
                if tab_metrics:
                    all_metrics[tab_name.lower()] = tab_metrics
                    validation_results.extend(tab_validation)
                
                # Collect metadata
                metadata[tab_name] = {
                    'row_count': len(tab_data),
                    'metric_count': len(tab_metrics) if tab_metrics else 0,
                    'columns': tab_data.columns.tolist(),
                    'processed_at': datetime.now().isoformat()
                }
                
            except Exception as e:
                logger.error(f"❌ Lỗi khi xử lý tab {tab_name}: {e}")
                continue
        
        # Save các file config
        self._save_configs(all_metrics, metadata, validation_results)
        
        logger.info(f"✅ Hoàn thành convert! Đã xử lý {len(all_metrics)} tabs")
        return all_metrics
    
    def _process_tab(self, tab_name: str, tab_data: pd.DataFrame) -> tuple:
        """Xử lý từng tab Excel"""
        metrics = {}
        validation_results = []
        
        # Xác định cấu trúc cột
        column_mapping = self._identify_column_structure(tab_data.columns)
        
        logger.info(f"  📝 Cấu trúc cột: {column_mapping}")
        
        # Process từng dòng
        for idx, row in tab_data.iterrows():
            try:
                metric_info = self._extract_metric_info(row, column_mapping, tab_name)
                if metric_info:
                    metric_code = metric_info['code']
                    metrics[metric_code] = metric_info
                    
                    # Validate metric
                    validation_result = self._validate_metric(metric_info)
                    if validation_result:
                        validation_results.append(validation_result)
                        
            except Exception as e:
                logger.warning(f"  ⚠️ Lỗi ở dòng {idx + 2}: {e}")
                continue
        
        logger.info(f"  ✅ Đã xử lý {len(metrics)} metrics từ tab {tab_name}")
        return metrics, validation_results
    
    def _identify_column_structure(self, columns: List[str]) -> Dict[str, str]:
        """Xác định cấu trúc cột của tab"""
        column_mapping = {}
        
        # Mapping chuẩn
        standard_mappings = {
            'code': ['Code', 'Mã', 'Metric Code', 'METRIC_CODE'],
            'name': ['Name', 'Tên', 'Metric Name', 'METRIC_NAME'],
            'description': ['Description', 'Mô tả', 'Mô tả chi tiết', 'DESCRIPTION'],
            'category': ['Category', 'Phân loại', 'Loại', 'CATEGORY'],
            'subcategory': ['Subcategory', 'Phân loại con', 'Sub Category', 'SUBCATEGORY'],
            'unit': ['Unit', 'Đơn vị', 'Unit of Measure', 'UNIT'],
            'data_type': ['Data Type', 'Kiểu dữ liệu', 'Type', 'DATA_TYPE'],
            'source': ['Source', 'Nguồn', 'Nguồn dữ liệu', 'SOURCE']
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
        
        return column_mapping
    
    def _extract_metric_info(self, row: pd.Series, column_mapping: Dict[str, str], tab_name: str) -> Optional[Dict[str, Any]]:
        """Trích xuất thông tin metric từ một dòng"""
        
        # Lấy các giá trị cơ bản
        metric_code = str(row.get(column_mapping.get('code', ''))).strip()
        metric_name = str(row.get(column_mapping.get('name', ''))).strip()
        
        # Bỏ qua nếu không có code hoặc name
        if not metric_code or not metric_name or metric_code.lower() in ['nan', 'none', '']:
            return None
        
        # Xác định category dựa trên tab name
        category = self._determine_category_from_tab(tab_name)
        
        # Tạo metric info
        metric_info = {
            'code': metric_code,
            'name': metric_name,
            'description': str(row.get(column_mapping.get('description', ''))).strip(),
            'category': category,
            'subcategory': str(row.get(column_mapping.get('subcategory', ''))).strip(),
            'unit': str(row.get(column_mapping.get('unit', ''))).strip() or 'VND',
            'data_type': str(row.get(column_mapping.get('data_type', ''))).strip() or 'numeric',
            'source': str(row.get(column_mapping.get('source', ''))).strip() or 'BSC',
            'tab_source': tab_name,
            'is_active': True,
            'is_required': False,
            'validation_rules': self._generate_validation_rules(metric_code, category)
        }
        
        return metric_info
    
    def _determine_category_from_tab(self, tab_name: str) -> str:
        """Xác định category dựa trên tên tab"""
        tab_lower = tab_name.lower()
        
        if any(keyword in tab_lower for keyword in ['income', 'revenue', 'profit', 'loss']):
            return 'income_statement'
        elif any(keyword in tab_lower for keyword in ['balance', 'asset', 'liability', 'equity']):
            return 'balance_sheet'
        elif any(keyword in tab_lower for keyword in ['cash', 'flow', 'cf']):
            return 'cash_flow'
        elif any(keyword in tab_lower for keyword in ['bank', 'ratio', 'car', 'ldr', 'nim']):
            return 'bank_ratio'
        elif any(keyword in tab_lower for keyword in ['ratio', 'roe', 'roa', 'debt']):
            return 'ratio'
        else:
            return 'other'
    
    def _generate_validation_rules(self, metric_code: str, category: str) -> Dict[str, Any]:
        """Tạo validation rules cho metric"""
        rules = {
            'data_type': 'numeric'
        }
        
        # Thêm rules theo category
        if category == 'income_statement':
            if 'revenue' in metric_code.lower() or 'profit' in metric_code.lower():
                rules['min_value'] = 0
        elif category == 'balance_sheet':
            if 'asset' in metric_code.lower():
                rules['min_value'] = 0
        elif category in ['bank_ratio', 'ratio']:
            if 'ratio' in metric_code.lower() or 'percentage' in metric_code.lower():
                rules['min_value'] = 0
                rules['max_value'] = 1000  # Có thể là tỷ lệ lớn
        
        return rules
    
    def _validate_metric(self, metric_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Validate metric info"""
        issues = []
        
        # Kiểm tra code format
        if not metric_info['code'] or len(metric_info['code']) > 20:
            issues.append("Metric code không hợp lệ hoặc quá dài")
        
        # Kiểm tra category
        if metric_info['category'] not in self.standard_categories:
            issues.append(f"Category '{metric_info['category']}' không chuẩn")
        
        # Kiểm tra unit
        valid_units = ['VND', '%', 'ratio', 'number', 'text', 'date']
        if metric_info['unit'] not in valid_units:
            issues.append(f"Unit '{metric_info['unit']}' không chuẩn")
        
        if issues:
            return {
                'metric_code': metric_info['code'],
                'issues': issues,
                'severity': 'warning'
            }
        
        return None
    
    def _save_configs(self, all_metrics: Dict[str, Any], metadata: Dict[str, Any], validation_results: List[Dict[str, Any]]):
        """Lưu các file config"""
        
        # 1. Save YAML mapping chính
        yaml_path = self.output_dir / "configs" / "metric_mapping.yaml"
        yaml_path.parent.mkdir(exist_ok=True)
        
        with open(yaml_path, 'w', encoding='utf-8') as f:
            yaml.dump(all_metrics, f, default_flow_style=False, 
                     allow_unicode=True, sort_keys=False)
        
        logger.info(f"💾 Đã lưu metric mapping: {yaml_path}")
        
        # 2. Save metadata JSON
        json_path = self.output_dir / "configs" / "metric_metadata.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        logger.info(f"💾 Đã lưu metadata: {json_path}")
        
        # 3. Save validation results
        if validation_results:
            validation_path = self.output_dir / "configs" / "validation_results.json"
            with open(validation_path, 'w', encoding='utf-8') as f:
                json.dump(validation_results, f, indent=2, ensure_ascii=False)
            
            logger.info(f"⚠️ Đã lưu validation results: {validation_path}")
        
        # 4. Save processed data dạng parquet
        try:
            # Convert dict thành DataFrame
            processed_data = []
            for category, metrics in all_metrics.items():
                for code, info in metrics.items():
                    processed_data.append({
                        'category': category,
                        **info
                    })
            
            if processed_data:
                df = pd.DataFrame(processed_data)
                parquet_path = self.output_dir / "processed" / "metric_definition.parquet"
                parquet_path.parent.mkdir(exist_ok=True)
                df.to_parquet(parquet_path, index=False)
                
                logger.info(f"💾 Đã lưu processed data: {parquet_path}")
                
        except Exception as e:
            logger.warning(f"⚠️ Không thể lưu parquet: {e}")
        
        # 5. Tạo summary report
        self._create_summary_report(all_metrics, metadata, validation_results)
    
    def _create_summary_report(self, all_metrics: Dict[str, Any], metadata: Dict[str, Any], validation_results: List[Dict[str, Any]]):
        """Tạo báo cáo tổng hợp"""
        
        report_path = self.output_dir / "configs" / "conversion_summary.md"
        
        total_metrics = sum(len(metrics) for metrics in all_metrics.values())
        total_issues = len(validation_results)
        
        report_content = f"""# 📊 Metric Definition Conversion Summary

## 📈 Thống kê tổng quan:
- **Tổng số tabs xử lý**: {len(all_metrics)}
- **Tổng số metrics**: {total_metrics}
- **Số vấn đề validation**: {total_issues}

## 📋 Chi tiết theo tab:
"""
        
        for tab_name, tab_info in metadata.items():
            report_content += f"""
### {tab_name}
- **Số dòng**: {tab_info['row_count']}
- **Số metrics**: {tab_info['metric_count']}
- **Cột**: {', '.join(tab_info['columns'])}
"""
        
        if validation_results:
            report_content += f"""
## ⚠️ Validation Issues:
"""
            for issue in validation_results:
                report_content += f"""
- **{issue['metric_code']}**: {', '.join(issue['issues'])}
"""
        
        report_content += f"""
## 📅 Thông tin:
- **Ngày convert**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **File nguồn**: {self.excel_path.name}
- **Output directory**: {self.output_dir}

## 🚀 Bước tiếp theo:
1. Kiểm tra file `metric_mapping.yaml`
2. Review validation results
3. Update config.yaml chính
4. Test integration
"""
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"📋 Đã tạo summary report: {report_path}")


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert Excel metric definition sang config files')
    parser.add_argument('excel_path', help='Đường dẫn đến file Excel metric definition')
    parser.add_argument('--output-dir', help='Thư mục output (mặc định: Database/Metric_Definitions)')
    
    args = parser.parse_args()
    
    try:
        converter = MetricDefinitionConverter(args.excel_path, args.output_dir)
        converter.convert_excel_to_configs()
        
        print("\n🎉 Convert thành công!")
        print(f"📁 Kiểm tra kết quả tại: {converter.output_dir}")
        
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()