# 📊 Metric Definitions Directory

Thư mục chứa định nghĩa và mapping của tất cả các metric codes trong hệ thống.

## 🗂️ Cấu trúc thư mục:

```
Metric_Definitions/
├── raw/                    # File Excel gốc từ BSC
│   └── metric_definition.xlsx
├── processed/              # File đã xử lý (parquet, csv)
│   ├── metric_definition.parquet
│   └── metric_definition.csv
├── configs/                # File cấu hình (yaml, json)
│   ├── metric_mapping.yaml
│   ├── metric_categories.yaml
│   └── metric_metadata.json
├── schemas/                # Database schemas
│   ├── sqlite_schema.sql
│   └── postgres_schema.sql
├── scripts/                # Script xử lý và convert
│   ├── convert_excel_to_config.py
│   ├── validate_metrics.py
│   └── update_metric_mapping.py
├── examples/               # Ví dụ sử dụng
│   ├── sample_metrics.json
│   └── usage_examples.py
└── documentation/          # Tài liệu chi tiết
    ├── metric_standards.md
    └── integration_guide.md
```

## 🔄 Quy trình xử lý:

1. **Upload file Excel** vào `raw/`
2. **Chạy script convert** để xử lý Excel → Config
3. **Validate metrics** theo schema chuẩn
4. **Update mapping** vào hệ thống
5. **Test integration** với code hiện tại

## 📝 Các loại metric:

- **Income Statement**: Doanh thu, lợi nhuận, chi phí
- **Balance Sheet**: Tài sản, nợ, vốn chủ sở hữu
- **Cash Flow**: Dòng tiền từ các hoạt động
- **Bank-specific**: Tỷ lệ an toàn vốn, LDR, NIM
- **Ratios**: ROE, ROA, Debt-to-Equity

## 🚀 Sử dụng:

```python
from src.core.metric_manager import MetricManager

# Load metric definitions
metric_manager = MetricManager()

# Lấy thông tin metric
metric_info = metric_manager.get_metric_info("CIS_10")

# Tìm kiếm metrics
results = metric_manager.search_metrics("revenue")
```

## 📅 Cập nhật:

- **Version**: 1.0.0
- **Last Updated**: 2024-08-29
- **Maintainer**: Stock Dashboard Team