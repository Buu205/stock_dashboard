-- SQLite Schema cho Metric Definitions Database
-- Tạo bảng lưu trữ định nghĩa và mapping của tất cả metrics

-- Bảng chính chứa thông tin metrics
CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_code VARCHAR(20) UNIQUE NOT NULL,           -- Mã metric (VD: CIS_10, CBS_270)
    metric_name VARCHAR(100) NOT NULL,                 -- Tên metric (VD: revenue, total_assets)
    description TEXT,                                   -- Mô tả chi tiết
    category VARCHAR(50) NOT NULL,                     -- Phân loại (income_statement, balance_sheet, cash_flow, bank_ratio)
    subcategory VARCHAR(50),                            -- Phân loại con (VD: revenue, expense, asset, liability)
    unit VARCHAR(20),                                   -- Đơn vị (VND, %, ratio, number)
    data_type VARCHAR(20) DEFAULT 'numeric',           -- Kiểu dữ liệu (numeric, text, date, boolean)
    source VARCHAR(50) DEFAULT 'BSC',                  -- Nguồn định nghĩa (BSC, VNM, TCBS)
    tab_source VARCHAR(100),                           -- Tab Excel gốc
    is_active BOOLEAN DEFAULT 1,                       -- Có đang sử dụng không
    is_required BOOLEAN DEFAULT 0,                     -- Có bắt buộc không
    validation_rules TEXT,                             -- Quy tắc validate (JSON)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bảng mapping giữa metric code và tên tiếng Việt
CREATE TABLE IF NOT EXISTS metric_translations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_code VARCHAR(20) NOT NULL,
    language VARCHAR(10) NOT NULL,                     -- vi, en, zh
    translation TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (metric_code) REFERENCES metrics(metric_code),
    UNIQUE(metric_code, language)
);

-- Bảng phân loại metrics theo ngành
CREATE TABLE IF NOT EXISTS sector_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_code VARCHAR(20) NOT NULL,
    sector VARCHAR(50) NOT NULL,                       -- bank, insurance, real_estate, manufacturing
    is_applicable BOOLEAN DEFAULT 1,                   -- Có áp dụng cho ngành này không
    priority INTEGER DEFAULT 0,                        -- Độ ưu tiên hiển thị
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (metric_code) REFERENCES metrics(metric_code),
    UNIQUE(metric_code, sector)
);

-- Bảng lưu trữ các giá trị mẫu cho metrics
CREATE TABLE IF NOT EXISTS metric_examples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_code VARCHAR(20) NOT NULL,
    example_value TEXT,                                -- Giá trị mẫu
    example_description TEXT,                          -- Mô tả ví dụ
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (metric_code) REFERENCES metrics(metric_code)
);

-- Bảng lưu trữ lịch sử thay đổi metrics
CREATE TABLE IF NOT EXISTS metric_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_code VARCHAR(20) NOT NULL,
    change_type VARCHAR(20) NOT NULL,                  -- created, updated, deleted
    old_value TEXT,                                    -- Giá trị cũ
    new_value TEXT,                                    -- Giá trị mới
    changed_by VARCHAR(50),                            -- Người thay đổi
    change_reason TEXT,                                -- Lý do thay đổi
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (metric_code) REFERENCES metrics(metric_code)
);

-- Bảng mapping giữa metric code cũ và mới (cho migration)
CREATE TABLE IF NOT EXISTS metric_migrations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    old_metric_code VARCHAR(20) NOT NULL,
    new_metric_code VARCHAR(20) NOT NULL,
    migration_date DATE NOT NULL,
    migration_reason TEXT,
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes để tối ưu performance
CREATE INDEX IF NOT EXISTS idx_metrics_category ON metrics(category);
CREATE INDEX IF NOT EXISTS idx_metrics_active ON metrics(is_active);
CREATE INDEX IF NOT EXISTS idx_metrics_source ON metrics(source);
CREATE INDEX IF NOT EXISTS idx_sector_metrics_sector ON sector_metrics(sector);
CREATE INDEX IF NOT EXISTS idx_metric_translations_lang ON metric_translations(language);

-- Trigger để tự động update updated_at
CREATE TRIGGER IF NOT EXISTS update_metrics_timestamp 
    AFTER UPDATE ON metrics
    FOR EACH ROW
BEGIN
    UPDATE metrics SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Insert dữ liệu mẫu cho metrics chính
INSERT OR IGNORE INTO metrics (metric_code, metric_name, description, category, subcategory, unit, source) VALUES
('CIS_10', 'revenue', 'Doanh thu thuần', 'income_statement', 'revenue', 'VND', 'BSC'),
('CIS_20', 'gross_profit', 'Lợi nhuận gộp', 'income_statement', 'profit', 'VND', 'BSC'),
('CIS_61', 'net_profit', 'Lợi nhuận sau thuế', 'income_statement', 'profit', 'VND', 'BSC'),
('CBS_270', 'total_assets', 'Tổng tài sản', 'balance_sheet', 'asset', 'VND', 'BSC'),
('CBS_400', 'equity', 'Vốn chủ sở hữu', 'balance_sheet', 'equity', 'VND', 'BSC'),
('CFS_20', 'operating_cf', 'Dòng tiền từ hoạt động kinh doanh', 'cash_flow', 'operating', 'VND', 'BSC');

-- Insert dữ liệu mẫu cho sector metrics
INSERT OR IGNORE INTO sector_metrics (metric_code, sector, is_applicable, priority) VALUES
('CIS_10', 'bank', 1, 1),
('CIS_10', 'insurance', 1, 1),
('CIS_10', 'real_estate', 1, 1),
('CIS_10', 'manufacturing', 1, 1);

-- Insert dữ liệu mẫu cho translations
INSERT OR IGNORE INTO metric_translations (metric_code, language, translation) VALUES
('CIS_10', 'vi', 'Doanh thu thuần'),
('CIS_10', 'en', 'Net Revenue'),
('CIS_20', 'vi', 'Lợi nhuận gộp'),
('CIS_20', 'en', 'Gross Profit');