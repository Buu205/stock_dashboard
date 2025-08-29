# 🏦 BSC Forecast - Đơn giản

Thư mục chứa dữ liệu forecast từ BSC và mapping metrics.

## 📁 Cấu trúc:

```
BSC_Forecast/
├── metric_definition.xlsx       # File Excel gốc từ BSC (UPLOAD VÀO ĐÂY)
├── metric_mapping.yaml          # File mapping đã convert (FINAL - GIỮ LẠI)
├── one_time_convert.py          # Script convert 1 lần (XÓA SAU KHI XONG)
└── README.md                    # File này
```

## 🚀 Sử dụng:

### Bước 1: Upload file Excel
Đặt file `metric_definition.xlsx` từ BSC vào thư mục này

### Bước 2: Chạy convert
```bash
cd Database/BSC_Forecast
python one_time_convert.py
```

### Bước 3: Kiểm tra kết quả
- File `metric_mapping.yaml` sẽ được tạo
- Kiểm tra mapping có đúng không

### Bước 4: Dọn dẹp
- Xóa script `one_time_convert.py`
- Xóa file Excel gốc `metric_definition.xlsx`
- **GIỮ LẠI** file `metric_mapping.yaml`

## 📊 Kết quả:

File `metric_mapping.yaml` sẽ có cấu trúc:
```yaml
Financial_Metrics:
  "Doanh thu thuần": "CIS_10"
  "Lợi nhuận gộp": "CIS_20"
  "Tổng tài sản": "CBS_270"
  "Vốn chủ sở hữu": "CBS_400"
```

## 💡 Lưu ý:

- **CHỈ CHẠY 1 LẦN** để convert Excel → YAML
- Sau đó chỉ cần sử dụng file YAML
- Không cần maintain script phức tạp
- Dễ dàng tích hợp vào code hiện tại

## 🔗 Tích hợp:

```python
# Trong code, chỉ cần:
import yaml

def load_bsc_metrics():
    with open("Database/BSC_Forecast/metric_mapping.yaml", 'r') as f:
        return yaml.safe_load(f)

# Sử dụng
metrics = load_bsc_metrics()
revenue_code = metrics.get("Doanh thu thuần", "CIS_10")
```

---

**Đơn giản, hiệu quả, một lần!** 🎯