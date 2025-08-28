# Hướng dẫn thiết lập Cron Job cho cập nhật tự động

## 1. Cập nhật OHLCV hàng ngày

### Cách 1: Sử dụng crontab (Linux/macOS)

1. Mở crontab editor:
```bash
crontab -e
```

2. Thêm dòng sau để chạy lúc 6:00 PM mỗi ngày (sau giờ đóng cửa thị trường):
```bash
0 18 * * * cd /Users/buu_os/Documents/GitHub/stock_dashboard && /usr/bin/python3 update_daily_ohlcv.py >> logs/daily_update.log 2>&1
```

Hoặc chạy vào buổi sáng trước giờ mở cửa:
```bash
0 8 * * * cd /Users/buu_os/Documents/GitHub/stock_dashboard && /usr/bin/python3 update_daily_ohlcv.py >> logs/daily_update.log 2>&1
```

### Cách 2: Sử dụng launchd (macOS)

Tạo file `com.stockdashboard.daily.plist` trong `~/Library/LaunchAgents/`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.stockdashboard.daily</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/python3</string>
        <string>/Users/buu_os/Documents/GitHub/stock_dashboard/update_daily_ohlcv.py</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>18</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>/Users/buu_os/Documents/GitHub/stock_dashboard/logs/daily_update.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/buu_os/Documents/GitHub/stock_dashboard/logs/daily_update_error.log</string>
</dict>
</plist>
```

Sau đó load service:
```bash
launchctl load ~/Library/LaunchAgents/com.stockdashboard.daily.plist
```

## 2. Các lệnh cập nhật thủ công

### Cập nhật tất cả stocks
```bash
python3 update_daily_ohlcv.py
```

### Cập nhật một mã cụ thể
```bash
python3 update_daily_ohlcv.py --ticker MWG
```

### Cập nhật các mã chưa có trong cache
```bash
python3 update_remaining_enhanced.py
```

## 3. Kiểm tra logs

Tạo thư mục logs:
```bash
mkdir -p logs
```

Xem logs:
```bash
tail -f logs/daily_update.log
```

## 4. Kiểm tra cache status

```bash
python3 -c "
import sqlite3
conn = sqlite3.connect('Database/cache/ohlcv_cache.db')
cursor = conn.cursor()
cursor.execute('SELECT COUNT(DISTINCT symbol), COUNT(*), MAX(date) FROM ohlcv_data')
symbols, records, latest = cursor.fetchone()
print(f'Cached: {symbols} symbols, {records:,} records, Latest: {latest}')
conn.close()
"
```

## Notes:
- Data được lưu trong SQLite database: `Database/cache/ohlcv_cache.db`
- Script chỉ cập nhật data mới từ ngày cuối cùng trong cache
- Tự động fallback sang TCBS API nếu VnStock bị rate limit
- Delay tự động giữa các requests để tránh rate limit