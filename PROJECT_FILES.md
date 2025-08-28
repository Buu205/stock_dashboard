# 📁 Cấu trúc File Dự án Stock Dashboard

## 🎯 File Python Chính (Root)

### Core Application
- **`app.py`** - Ứng dụng Streamlit chính (multi-page app)

### Utility Scripts
- **`update_daily_ohlcv.py`** - Script cập nhật OHLCV hàng ngày (chỉ data mới)
- **`update_remaining_enhanced.py`** - Script cập nhật các mã chưa có trong cache
- **`check_cache_progress.py`** - Kiểm tra tiến độ cache
- **`run_individual_pages.py`** - Chạy từng page trên port riêng

### Shell Scripts
- **`run_pages.sh`** - Bash script chạy các pages
- **`daily_update.sh`** - Script cập nhật hàng ngày
- **`setup_cron.md`** - Hướng dẫn thiết lập cron job

## 📊 Pages (Multi-page Apps)
```
pages/
├── 1_Company_Dashboard.py     - Dashboard phân tích công ty
├── 2_Market_Overview.py        - Tổng quan thị trường  
├── 2_Technical_Analysis.py     - Phân tích kỹ thuật
├── 3_🔍_Stock_Screener.py     - Lọc cổ phiếu
└── 4_⚙️_Settings.py           - Cài đặt
```

## 🔧 Source Code
```
src/
├── analysis/
│   ├── fundamental/           - Phân tích cơ bản
│   │   ├── growth_analyzer.py (đã convert sang tỷ đồng)
│   │   └── valuation_analyzer.py
│   └── technical/             - Phân tích kỹ thuật
│       ├── indicator_analyzer.py
│       └── market_breadth.py
│
├── data/
│   ├── connectors/           - Kết nối data sources
│   │   ├── ohlcv_cache.py   - Quản lý cache SQLite
│   │   ├── vnstock_connector.py
│   │   └── tcbs_connector.py
│   └── loaders/              - Load data từ files
│
├── core/
│   ├── config.py            - Configuration management
│   └── data_manager.py      - Data orchestration
│
└── visualization/            - Charts và components
```

## 💾 Database & Cache
```
Database/
├── Full_database/
│   ├── Buu_clean_ver2.parquet  - Data tài chính chính
│   └── CSDL.xlsx               - Metadata
│
└── cache/
    ├── ohlcv_cache.db          - SQLite OHLCV cache (58MB)
    ├── ssi_cache.pkl           - SSI API cache
    └── vnstock_cache.pkl       - VnStock API cache
```

## 🚀 Cách sử dụng

### 1. Chạy ứng dụng chính
```bash
streamlit run app.py
```

### 2. Chạy từng page riêng
```bash
python3 run_individual_pages.py
```

### 3. Cập nhật dữ liệu OHLCV
```bash
# Cập nhật hàng ngày (chỉ data mới)
python3 update_daily_ohlcv.py

# Cập nhật một mã cụ thể  
python3 update_daily_ohlcv.py --ticker MWG

# Cập nhật các mã chưa có
python3 update_remaining_enhanced.py
```

### 4. Kiểm tra cache
```bash
python3 check_cache_progress.py
```

## 📝 Ghi chú quan trọng
- Data đã được convert sang **tỷ đồng** trong `growth_analyzer.py`
- OHLCV cache trong SQLite: 427 symbols, 507k records
- Sử dụng dual API: VnStock (primary) + TCBS (fallback)