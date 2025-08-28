# Quá Trình Lọc Ticker Cổ Phiếu Và Mapping Phân Ngành

## Tổng Quan
Script này thực hiện việc lọc ticker cổ phiếu theo điều kiện khối lượng giao dịch và mapping thông tin phân ngành ICB L2 vào dữ liệu tài chính.

## Flow Xử Lý

### 1. Lọc Ticker Theo Điều Kiện
- **Input**: File `Database/data_fiinx.xlsx` (1,653 ticker)
- **Điều kiện**: Giá trị trung bình 1 tháng > 5 tỷ đồng
- **Output**: 304 ticker thỏa mãn điều kiện

### 2. Mapping Phân Ngành ICB L2
- Trích xuất cột `Phân ngành - ICB L2` từ file fiinx
- Chuẩn hóa ticker (UPPER + strip)
- Tạo mapping ticker ↔ phân ngành

### 3. Join Dữ Liệu
- **Input**: File `Database/buu_clean_data.parquet` (21,977,231 records)
- **Join**: Inner join với danh sách ticker đã lọc
- **Output**: 6,646,303 records (chỉ giữ ticker thỏa mãn điều kiện)

### 4. Tạo File Mới
- **Output**: `Database/Buu_clean_ver2.parquet`
- **Cấu trúc**: 10 cột (thêm cột `ICB_L2`)

## Kết Quả

### Thống Kê Tổng Quan
- **Tổng số record ban đầu**: 21,977,231
- **Số record sau khi lọc**: 6,646,303 (giảm ~70%)
- **Số ticker duy nhất**: 302
- **Số phân ngành ICB L2**: 19

### Các Phân Ngành Chính
1. **Ngân hàng** - Chiếm phần lớn dữ liệu
2. **Bất động sản**
3. **Thực phẩm & Đồ uống**
4. **Dầu khí**
5. **Thép**
6. **Và các ngành khác...**

### Thông Tin Bổ Sung
- **Thời gian**: Dữ liệu từ 2018-2024
- **Tần suất**: Quý (Q) và Năm (Y)
- **Trạng thái**: Đã kiểm toán (Y) và chưa kiểm toán (N)
- **Metrics**: Các chỉ số tài chính cơ bản

## Files Đã Tạo

1. **`filter_tickers.py`** - Script chính thực hiện flow
2. **`analyze_results.py`** - Script phân tích kết quả
3. **`Database/Buu_clean_ver2.parquet`** - File dữ liệu đã lọc
4. **`Database/filtered_tickers_summary.csv`** - Danh sách ticker đã lọc

## Cách Sử Dụng

### Chạy Script Lọc
```bash
python3 filter_tickers.py
```

### Phân Tích Kết Quả
```bash
python3 analyze_results.py
```

### Xem Dữ Liệu Đã Lọc
```python
import pandas as pd
df = pd.read_parquet('Database/Buu_clean_ver2.parquet')
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")
```

## Lưu Ý

- Script sử dụng **inner join** nên chỉ giữ lại ticker có trong cả 2 nguồn dữ liệu
- Dữ liệu được chuẩn hóa (UPPER + strip) để tránh lỗi matching
- File output có kích thước nhỏ hơn đáng kể so với file gốc
- Tất cả ticker đều có thông tin phân ngành ICB L2

## Cải Tiến Có Thể Thực Hiện

1. **Thêm validation** cho dữ liệu đầu vào
2. **Tối ưu hóa memory** cho file lớn
3. **Thêm logging** chi tiết hơn
4. **Tạo config file** cho các tham số
5. **Thêm unit tests** cho các function chính
