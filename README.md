# 🚀 Stock Dashboard - Setup Guide

## 📋 Cấu trúc thư mục cần tạo

```bash
stock_dashboard/
├── config.yaml           # File 1: Copy từ artifact
├── test_loader.py        # File 4: Copy từ artifact  
├── requirements.txt      # File 5: Copy từ artifact
├── Database/
│   └── cache/           # Tạo thư mục rỗng
├── src/
│   ├── __init__.py      # Tạo file rỗng
│   ├── core/
│   │   ├── __init__.py  # Tạo file rỗng
│   │   └── config.py    # File 2: Copy từ artifact
│   └── data/
│       ├── __init__.py  # Tạo file rỗng
│       └── loaders/
│           ├── __init__.py        # Tạo file rỗng
│           └── financial_loader.py # File 3: Copy từ artifact
```

## 🔧 Bước 1: Tạo cấu trúc thư mục

```bash
# Tạo thư mục dự án
mkdir stock_dashboard
cd stock_dashboard

# Tạo cấu trúc thư mục
mkdir -p Database/cache
mkdir -p src/core
mkdir -p src/data/loaders

# Tạo các file __init__.py rỗng
touch src/__init__.py
touch src/core/__init__.py
touch src/data/__init__.py
touch src/data/loaders/__init__.py
```

## 📝 Bước 2: Copy các file từ artifacts

1. Copy `config.yaml` vào thư mục root
2. Copy `src/core/config.py` vào `src/core/`
3. Copy `src/data/loaders/financial_loader.py` vào `src/data/loaders/`
4. Copy `test_loader.py` vào thư mục root
5. Copy `requirements.txt` vào thư mục root

## 🔨 Bước 3: Cài đặt dependencies

```bash
# Tạo virtual environment (khuyên dùng)
python -m venv venv

# Activate virtual environment
# MacOS/Linux:
source venv/bin/activate
# Windows:
venv\Scripts\activate

# Cài đặt packages
pip install -r requirements.txt
```

## ⚙️ Bước 4: Cập nhật đường dẫn trong config.yaml

Mở file `config.yaml` và sửa các đường dẫn cho phù hợp với máy của bạn:

```yaml
paths:
  data:
    parquet: "/path/to/your/Buu_clean_ver2.parquet"  # Sửa đường dẫn này
    metadata: "/path/to/your/CSDL.xlsx"              # Sửa đường dẫn này
```

## 🧪 Bước 5: Test hệ thống

```bash
# Chạy test script
python test_loader.py
```

### Kết quả mong đợi:

```
======================================================================
 FINANCIAL DATA LOADER TEST SUITE
 Testing all components...
======================================================================

======================================================================
 1. TESTING CONFIGURATION
======================================================================
✓ Config loaded: Vietnam Stock Dashboard v1.0.0
✓ Paths validated
✓ Metrics loaded: 30+ total
...

======================================================================
 TEST SUMMARY
======================================================================
Tests Passed: 5/5
Success Rate: 100.0%
✅ ALL TESTS PASSED! System is ready to use.
```

## 🐛 Troubleshooting

### Lỗi: "FileNotFoundError: Parquet file not found"
- **Nguyên nhân**: Đường dẫn file parquet không đúng
- **Giải pháp**: Kiểm tra và sửa đường dẫn trong `config.yaml`

### Lỗi: "ModuleNotFoundError"
- **Nguyên nhân**: Chưa cài đặt dependencies
- **Giải pháp**: Chạy `pip install -r requirements.txt`

### Lỗi: "No module named 'core'"
- **Nguyên nhân**: Cấu trúc thư mục không đúng
- **Giải pháp**: Kiểm tra lại cấu trúc thư mục và các file `__init__.py`

## 📊 Sử dụng cơ bản

### 1. Load data trong Python:

```python
from src.data.loaders.financial_loader import FinancialDataLoader

# Initialize loader
loader = FinancialDataLoader()

# Load data
data = loader.load_data()

# Get operating profit for MWG
mwg_profit = loader.calculate_operating_profit('MWG')
print(mwg_profit)
```

### 2. Phân tích công ty:

```python
# Get financial ratios
ratios = loader.calculate_financial_ratios('MWG', 'Y')

# Growth analysis
growth = loader.get_growth_analysis('MWG', periods=5)

# Compare companies
comparison = loader.compare_companies(
    ['MWG', 'VNM', 'FPT'],
    ['CIS_20', 'CIS_30'],  # Revenue, Net Profit
    year=2023
)
```

## 📈 Bước tiếp theo

Sau khi test thành công, bạn có thể:

1. **Tạo Streamlit app**: Tạo file `app.py` với Streamlit UI
2. **Thêm analyzers**: Xây dựng thêm các module phân tích
3. **Tích hợp vnstock**: Thêm data real-time từ vnstock
4. **Tạo visualizations**: Thêm charts với Plotly

## 💡 Tips

- Data được cache tự động trong 1 giờ (3600s)
- Để reload data mới: `loader.load_data(force_reload=True)`
- File cache nằm trong `Database/cache/`
- Log được hiển thị trong console để debug

## 📞 Support

Nếu gặp vấn đề, kiểm tra:
1. Console output cho error messages
2. File paths trong config.yaml
3. Python version (cần >= 3.8)
4. Pandas version (cần >= 2.0)

---
*Happy coding! 🎉*