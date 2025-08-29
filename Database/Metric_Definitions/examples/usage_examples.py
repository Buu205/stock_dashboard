#!/usr/bin/env python3
"""
Ví dụ sử dụng Metric Definitions
Cách load và sử dụng metric definitions trong code

Author: Stock Dashboard System
Date: 2024-08-29
"""

import sys
import os
from pathlib import Path

# Add parent directory to path
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

import yaml
import json
import pandas as pd
from typing import Dict, List, Any, Optional

class MetricDefinitionManager:
    """Class quản lý và sử dụng metric definitions"""
    
    def __init__(self, config_dir: str = "Database/Metric_Definitions/configs"):
        self.config_dir = Path(config_dir)
        self.metric_mapping = self._load_metric_mapping()
        self.metadata = self._load_metadata()
        
    def _load_metric_mapping(self) -> Dict[str, Any]:
        """Load metric mapping từ YAML"""
        yaml_path = self.config_dir / "metric_mapping.yaml"
        if yaml_path.exists():
            with open(yaml_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        return {}
    
    def _load_metadata(self) -> Dict[str, Any]:
        """Load metadata từ JSON"""
        json_path = self.config_dir / "metric_metadata.json"
        if json_path.exists():
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def get_metric_info(self, metric_code: str) -> Optional[Dict[str, Any]]:
        """Lấy thông tin chi tiết của metric"""
        for category, metrics in self.metric_mapping.items():
            if metric_code in metrics:
                return metrics[metric_code]
        return None
    
    def get_metrics_by_category(self, category: str) -> Dict[str, Any]:
        """Lấy tất cả metrics theo category"""
        return self.metric_mapping.get(category.lower(), {})
    
    def search_metrics(self, keyword: str) -> List[Dict[str, Any]]:
        """Tìm kiếm metrics theo keyword"""
        results = []
        for category, metrics in self.metric_mapping.items():
            for code, info in metrics.items():
                if (keyword.lower() in info.get('name', '').lower() or 
                    keyword.lower() in info.get('description', '').lower()):
                    results.append({
                        'code': code,
                        'category': category,
                        **info
                    })
        return results
    
    def get_metric_code_by_name(self, name: str) -> Optional[str]:
        """Lấy metric code theo tên"""
        for category, metrics in self.metric_mapping.items():
            for code, info in metrics.items():
                if info.get('name', '').lower() == name.lower():
                    return code
        return None
    
    def get_metrics_for_sector(self, sector: str) -> List[Dict[str, Any]]:
        """Lấy metrics phù hợp cho một ngành cụ thể"""
        sector_metrics = []
        
        # Mapping ngành
        sector_mapping = {
            'bank': ['bank_ratio', 'income_statement', 'balance_sheet'],
            'insurance': ['income_statement', 'balance_sheet', 'ratio'],
            'real_estate': ['income_statement', 'balance_sheet', 'cash_flow', 'ratio'],
            'manufacturing': ['income_statement', 'balance_sheet', 'cash_flow', 'ratio']
        }
        
        target_categories = sector_mapping.get(sector.lower(), [])
        
        for category in target_categories:
            if category in self.metric_mapping:
                for code, info in self.metric_mapping[category].items():
                    sector_metrics.append({
                        'code': code,
                        'category': category,
                        **info
                    })
        
        return sector_metrics
    
    def validate_metric_data(self, metric_code: str, value: Any) -> Dict[str, Any]:
        """Validate dữ liệu theo metric rules"""
        metric_info = self.get_metric_info(metric_code)
        if not metric_info:
            return {'valid': False, 'error': 'Metric code không tồn tại'}
        
        validation_rules = metric_info.get('validation_rules', {})
        issues = []
        
        # Kiểm tra data type
        expected_type = validation_rules.get('data_type', 'numeric')
        if expected_type == 'numeric' and not isinstance(value, (int, float)):
            issues.append(f"Giá trị phải là số, nhận được: {type(value).__name__}")
        
        # Kiểm tra min value
        if 'min_value' in validation_rules and value < validation_rules['min_value']:
            issues.append(f"Giá trị phải >= {validation_rules['min_value']}")
        
        # Kiểm tra max value
        if 'max_value' in validation_rules and value > validation_rules['max_value']:
            issues.append(f"Giá trị phải <= {validation_rules['max_value']}")
        
        return {
            'valid': len(issues) == 0,
            'issues': issues,
            'metric_info': metric_info
        }
    
    def get_metric_summary(self) -> Dict[str, Any]:
        """Lấy tổng quan về metrics"""
        total_metrics = sum(len(metrics) for metrics in self.metric_mapping.values())
        
        category_counts = {}
        for category, metrics in self.metric_mapping.items():
            category_counts[category] = len(metrics)
        
        return {
            'total_metrics': total_metrics,
            'categories': category_counts,
            'source': 'BSC',
            'last_updated': self.metadata.get('processed_at', 'Unknown')
        }


def example_basic_usage():
    """Ví dụ sử dụng cơ bản"""
    print("🔍 Ví dụ sử dụng cơ bản:")
    print("=" * 50)
    
    # Khởi tạo manager
    metric_manager = MetricDefinitionManager()
    
    # Lấy thông tin metric
    revenue_info = metric_manager.get_metric_info("CIS_10")
    if revenue_info:
        print(f"📊 Revenue metric: {revenue_info['name']}")
        print(f"   Mô tả: {revenue_info['description']}")
        print(f"   Category: {revenue_info['category']}")
        print(f"   Unit: {revenue_info['unit']}")
    
    # Tìm kiếm metrics
    print(f"\n🔍 Tìm kiếm metrics chứa 'profit':")
    profit_metrics = metric_manager.search_metrics("profit")
    for metric in profit_metrics[:3]:  # Chỉ hiển thị 3 kết quả đầu
        print(f"   {metric['code']}: {metric['name']}")
    
    # Lấy metrics theo category
    print(f"\n📋 Metrics theo category 'income_statement':")
    income_metrics = metric_manager.get_metrics_by_category("income_statement")
    for code, info in income_metrics.items():
        print(f"   {code}: {info['name']}")


def example_sector_analysis():
    """Ví dụ phân tích theo ngành"""
    print("\n\n🏦 Ví dụ phân tích theo ngành:")
    print("=" * 50)
    
    metric_manager = MetricDefinitionManager()
    
    # Lấy metrics cho ngành bank
    bank_metrics = metric_manager.get_metrics_for_sector("bank")
    print(f"📊 Số metrics cho ngành bank: {len(bank_metrics)}")
    
    # Hiển thị một số metrics quan trọng
    important_bank_metrics = [m for m in bank_metrics if m.get('is_required', False)]
    print(f"🔑 Metrics bắt buộc cho bank: {len(important_bank_metrics)}")
    
    for metric in important_bank_metrics[:5]:
        print(f"   {metric['code']}: {metric['name']}")


def example_validation():
    """Ví dụ validate dữ liệu"""
    print("\n\n✅ Ví dụ validate dữ liệu:")
    print("=" * 50)
    
    metric_manager = MetricDefinitionManager()
    
    # Test validate revenue
    test_values = [1000000, -500000, "invalid", 0]
    
    for value in test_values:
        validation_result = metric_manager.validate_metric_data("CIS_10", value)
        status = "✅" if validation_result['valid'] else "❌"
        print(f"{status} Revenue = {value}: {validation_result['valid']}")
        
        if not validation_result['valid']:
            for issue in validation_result['issues']:
                print(f"   ⚠️ {issue}")


def example_integration_with_existing_config():
    """Ví dụ tích hợp với config hiện tại"""
    print("\n\n🔗 Ví dụ tích hợp với config hiện tại:")
    print("=" * 50)
    
    # Load config hiện tại
    config_path = Path("config.yaml")
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            existing_config = yaml.safe_load(f)
        
        print("📋 Config hiện tại có các metrics:")
        if 'analysis' in existing_config:
            for category, metrics in existing_config['analysis'].items():
                print(f"   {category}: {len(metrics)} metrics")
    
    # So sánh với metric definitions mới
    metric_manager = MetricDefinitionManager()
    new_summary = metric_manager.get_metric_summary()
    
    print(f"\n📊 Metric definitions mới:")
    print(f"   Tổng số: {new_summary['total_metrics']}")
    print(f"   Categories: {new_summary['categories']}")


def main():
    """Main function để chạy các ví dụ"""
    print("🚀 Metric Definitions Usage Examples")
    print("=" * 60)
    
    try:
        # Chạy các ví dụ
        example_basic_usage()
        example_sector_analysis()
        example_validation()
        example_integration_with_existing_config()
        
        print("\n🎉 Hoàn thành tất cả ví dụ!")
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        print("💡 Hãy đảm bảo đã chạy script convert Excel trước")


if __name__ == "__main__":
    main()