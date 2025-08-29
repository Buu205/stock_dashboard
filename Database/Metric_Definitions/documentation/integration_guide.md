# 🔗 Hướng dẫn tích hợp Metric Definitions

Hướng dẫn chi tiết cách tích hợp Metric Definitions vào hệ thống Stock Dashboard hiện tại.

## 📋 Mục lục

1. [Tổng quan](#tổng-quan)
2. [Cài đặt ban đầu](#cài-đặt-ban-đầu)
3. [Convert Excel sang Config](#convert-excel-sang-config)
4. [Tích hợp vào hệ thống](#tích-hợp-vào-hệ-thống)
5. [Sử dụng trong code](#sử-dụng-trong-code)
6. [Testing và validation](#testing-và-validation)
7. [Troubleshooting](#troubleshooting)

## 🎯 Tổng quan

Metric Definitions là hệ thống quản lý tập trung tất cả các metric codes, định nghĩa và mapping trong hệ thống. Nó giúp:

- **Thống nhất metric codes** giữa các nguồn dữ liệu
- **Quản lý metadata** của metrics (mô tả, đơn vị, validation rules)
- **Phân loại metrics** theo ngành và category
- **Tích hợp dễ dàng** với BSC Forecast và các nguồn dữ liệu khác

## 🚀 Cài đặt ban đầu

### Bước 1: Tạo cấu trúc thư mục

```bash
# Đã được tạo tự động
Database/Metric_Definitions/
├── raw/                    # Upload file Excel vào đây
├── processed/              # File đã xử lý
├── configs/                # File cấu hình
├── schemas/                # Database schemas
├── scripts/                # Script xử lý
├── examples/               # Ví dụ sử dụng
└── documentation/          # Tài liệu
```

### Bước 2: Cài đặt dependencies

```bash
pip install pandas pyyaml openpyxl
```

## 📊 Convert Excel sang Config

### Bước 1: Upload file Excel

Đặt file Excel `metric_definition.xlsx` vào thư mục `raw/`

### Bước 2: Chạy script convert

```bash
cd Database/Metric_Definitions/scripts

# Convert với đường dẫn mặc định
python convert_excel_to_config.py ../raw/metric_definition.xlsx

# Hoặc chỉ định output directory
python convert_excel_to_config.py ../raw/metric_definition.xlsx --output-dir ../processed
```

### Bước 3: Kiểm tra kết quả

Script sẽ tạo ra các file:

- `configs/metric_mapping.yaml` - Mapping chính
- `configs/metric_metadata.json` - Metadata
- `configs/validation_results.json` - Kết quả validation
- `processed/metric_definition.parquet` - Data đã xử lý
- `configs/conversion_summary.md` - Báo cáo tổng hợp

## 🔗 Tích hợp vào hệ thống

### Bước 1: Update config.yaml chính

Thêm reference đến metric definitions mới:

```yaml
# config.yaml
paths:
  data:
    metric_definitions: "Database/Metric_Definitions/configs"

# Thêm metric definitions
metric_definitions:
  enabled: true
  source: "BSC"
  mapping_file: "metric_mapping.yaml"
  metadata_file: "metric_metadata.json"
```

### Bước 2: Tạo MetricManager class

```python
# src/core/metric_manager.py
from pathlib import Path
import yaml
import json

class MetricManager:
    def __init__(self, config_dir: str = None):
        if config_dir is None:
            from src.core.config import get_config
            config = get_config()
            config_dir = config.paths.metric_definitions
        
        self.config_dir = Path(config_dir)
        self.metric_mapping = self._load_metric_mapping()
    
    def _load_metric_mapping(self):
        yaml_path = self.config_dir / "metric_mapping.yaml"
        if yaml_path.exists():
            with open(yaml_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        return {}
    
    def get_metric_info(self, metric_code: str):
        """Lấy thông tin metric theo code"""
        for category, metrics in self.metric_mapping.items():
            if metric_code in metrics:
                return metrics[metric_code]
        return None
    
    def get_metrics_by_category(self, category: str):
        """Lấy metrics theo category"""
        return self.metric_mapping.get(category.lower(), {})
```

### Bước 3: Update existing code

```python
# Thay thế các hardcoded metric codes
# Trước:
revenue_code = "CIS_10"

# Sau:
from src.core.metric_manager import MetricManager
metric_manager = MetricManager()
revenue_code = metric_manager.get_metric_code_by_name("revenue")
```

## 💻 Sử dụng trong code

### Ví dụ 1: Load metric definitions

```python
from src.core.metric_manager import MetricManager

# Khởi tạo
metric_manager = MetricManager()

# Lấy thông tin metric
revenue_info = metric_manager.get_metric_info("CIS_10")
print(f"Revenue: {revenue_info['description']}")

# Tìm kiếm metrics
profit_metrics = metric_manager.search_metrics("profit")
for metric in profit_metrics:
    print(f"{metric['code']}: {metric['name']}")
```

### Ví dụ 2: Validate dữ liệu

```python
# Validate dữ liệu theo metric rules
validation_result = metric_manager.validate_metric_data("CIS_10", 1000000)
if validation_result['valid']:
    print("✅ Dữ liệu hợp lệ")
else:
    print("❌ Dữ liệu không hợp lệ:")
    for issue in validation_result['issues']:
        print(f"  - {issue}")
```

### Ví dụ 3: Phân tích theo ngành

```python
# Lấy metrics cho ngành bank
bank_metrics = metric_manager.get_metrics_for_sector("bank")
print(f"Số metrics cho bank: {len(bank_metrics)}")

# Lấy metrics bắt buộc
required_metrics = [m for m in bank_metrics if m.get('is_required', False)]
```

## 🧪 Testing và validation

### Bước 1: Chạy examples

```bash
cd Database/Metric_Definitions/examples
python usage_examples.py
```

### Bước 2: Test integration

```python
# Test file
import pytest
from src.core.metric_manager import MetricManager

def test_metric_manager():
    manager = MetricManager()
    
    # Test get metric info
    revenue_info = manager.get_metric_info("CIS_10")
    assert revenue_info is not None
    assert revenue_info['name'] == 'revenue'
    
    # Test search
    results = manager.search_metrics("profit")
    assert len(results) > 0
```

### Bước 3: Validate data

```python
# Validate tất cả metrics
manager = MetricManager()
all_metrics = manager.get_metric_summary()

print(f"Tổng số metrics: {all_metrics['total_metrics']}")
print(f"Categories: {all_metrics['categories']}")
```

## 🔧 Troubleshooting

### Lỗi thường gặp

#### 1. File không tìm thấy

```
FileNotFoundError: Không tìm thấy file Excel
```

**Giải pháp:**
- Kiểm tra đường dẫn file Excel
- Đảm bảo file có quyền đọc
- Kiểm tra tên file chính xác

#### 2. Lỗi encoding

```
UnicodeDecodeError: 'utf-8' codec can't decode byte
```

**Giải pháp:**
- Kiểm tra encoding của file Excel
- Sử dụng encoding phù hợp (utf-8, cp1252, etc.)

#### 3. Lỗi validation

```
ValidationError: Metric code không hợp lệ
```

**Giải pháp:**
- Kiểm tra format metric code
- Review validation rules
- Update metric definitions

### Debug tips

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Check file paths
from pathlib import Path
config_dir = Path("Database/Metric_Definitions/configs")
print(f"Config dir exists: {config_dir.exists()}")
print(f"Files in config dir: {list(config_dir.glob('*'))}")

# Validate YAML syntax
import yaml
with open("metric_mapping.yaml", 'r') as f:
    try:
        data = yaml.safe_load(f)
        print("✅ YAML syntax OK")
    except yaml.YAMLError as e:
        print(f"❌ YAML syntax error: {e}")
```

## 📚 Tài liệu tham khảo

- [Metric Definitions README](../README.md)
- [Database Schema](../schemas/sqlite_schema.sql)
- [Usage Examples](../examples/usage_examples.py)
- [Conversion Script](../scripts/convert_excel_to_config.py)

## 🆘 Hỗ trợ

Nếu gặp vấn đề:

1. **Kiểm tra logs** trong terminal
2. **Review validation results** trong `validation_results.json`
3. **Kiểm tra file paths** và permissions
4. **Test với examples** trước khi tích hợp
5. **Liên hệ team** nếu cần hỗ trợ thêm

---

**Lưu ý:** Luôn backup dữ liệu trước khi thay đổi cấu hình hệ thống!