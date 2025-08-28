#!/usr/bin/env python3
"""
Script cập nhật dữ liệu từ final_processed_data.parquet sang Buu_clean_ver2.parquet
Giữ nguyên danh sách ticker hiện có trong Buu_clean_ver2.parquet

Author: Stock Dashboard System
Date: 2024
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class DataUpdater:
    """Class xử lý cập nhật dữ liệu từ final_processed_data sang Buu_clean_ver2"""
    
    def __init__(self):
        # Đường dẫn file cố định
        self.base_path = Path("/Users/buu_os/Documents/GitHub/stock_dashboard/Database/Full_database")
        self.input_file = self.base_path / "final_processed_data.parquet"
        self.output_file = self.base_path / "Buu_clean_ver2.parquet"
        self.summary_file = self.base_path / "filtered_tickers_summary.csv"
        
    def validate_files(self) -> bool:
        """Kiểm tra các file cần thiết có tồn tại không"""
        if not self.input_file.exists():
            logger.error(f"Không tìm thấy file input: {self.input_file}")
            return False
        if not self.output_file.exists():
            logger.warning(f"File {self.output_file} chưa tồn tại, sẽ tạo mới")
        return True
    
    def get_existing_tickers_and_mapping(self) -> tuple:
        """Lấy danh sách ticker và mapping phân ngành từ file hiện tại"""
        if not self.output_file.exists():
            logger.warning("File Buu_clean_ver2.parquet chưa tồn tại")
            return [], {}
        
        logger.info(f"Đang load ticker từ {self.output_file.name}")
        existing_df = pd.read_parquet(self.output_file)
        
        # Lấy danh sách ticker unique
        tickers = existing_df['SECURITY_CODE'].unique().tolist()
        logger.info(f"  • Tìm thấy {len(tickers)} ticker hiện có")
        
        # Lấy mapping ticker -> ICB_L2 (cách tối ưu)
        mapping = existing_df.groupby('SECURITY_CODE')['ICB_L2'].first().to_dict()
        
        # Loại bỏ giá trị NaN từ mapping
        mapping = {k: v for k, v in mapping.items() if pd.notna(v)}
        
        logger.info(f"  • Tạo mapping phân ngành cho {len(mapping)} ticker")
        
        return tickers, mapping
    
    def process_data(self) -> pd.DataFrame:
        """Xử lý dữ liệu chính"""
        logger.info("\n" + "="*70)
        logger.info("BẮT ĐẦU CẬP NHẬT DỮ LIỆU")
        logger.info("="*70)
        
        # 1. Lấy danh sách ticker và mapping từ file hiện có
        logger.info("\n1. LẤY DANH SÁCH TICKER HIỆN CÓ")
        valid_tickers, sector_mapping = self.get_existing_tickers_and_mapping()
        
        if not valid_tickers:
            logger.error("Không tìm thấy ticker nào trong file hiện tại!")
            raise ValueError("File Buu_clean_ver2.parquet không tồn tại hoặc không có dữ liệu")
        
        logger.info(f"  • Sử dụng {len(valid_tickers)} ticker từ file hiện có")
        logger.info(f"  • Mapping phân ngành: {len(sector_mapping)} ticker")
        
        # 2. Load dữ liệu mới từ final_processed_data
        logger.info("\n2. LOAD DỮ LIỆU MỚI")
        logger.info(f"Đang load {self.input_file.name}...")
        main_df = pd.read_parquet(self.input_file)
        logger.info(f"  • Loaded {len(main_df):,} records")
        logger.info(f"  • Columns: {main_df.columns.tolist()}")
        
        # 3. Lọc theo danh sách ticker hiện có
        logger.info("\n3. LỌC DỮ LIỆU THEO TICKER HIỆN CÓ")
        initial_count = len(main_df)
        
        # Chuẩn hóa ticker trong main_df
        main_df['SECURITY_CODE'] = main_df['SECURITY_CODE'].str.upper().str.strip()
        
        # Lọc chỉ giữ ticker hiện có
        filtered_df = main_df[main_df['SECURITY_CODE'].isin(valid_tickers)].copy()
        final_count = len(filtered_df)
        
        logger.info(f"  • Lọc từ {initial_count:,} xuống {final_count:,} records")
        logger.info(f"  • Tỷ lệ giữ lại: {(final_count/initial_count)*100:.1f}%")
        
        # Kiểm tra ticker nào không có dữ liệu mới
        tickers_with_data = set(filtered_df['SECURITY_CODE'].unique())
        missing_tickers = set(valid_tickers) - tickers_with_data
        if missing_tickers:
            logger.warning(f"  • {len(missing_tickers)} ticker không có trong dữ liệu mới: {sorted(list(missing_tickers))[:10]}")
        
        # 4. Thêm phân ngành ICB_L2 từ mapping hiện có
        logger.info("\n4. GIỮ NGUYÊN THÔNG TIN PHÂN NGÀNH")
        filtered_df['ICB_L2'] = filtered_df['SECURITY_CODE'].map(sector_mapping)
        
        # Kiểm tra ticker không có phân ngành (không nên xảy ra vì dùng mapping cũ)
        missing_sector = filtered_df[filtered_df['ICB_L2'].isna()]['SECURITY_CODE'].unique()
        if len(missing_sector) > 0:
            logger.warning(f"  • {len(missing_sector)} ticker không có thông tin phân ngành")
            logger.warning(f"    {list(missing_sector[:10])}{'...' if len(missing_sector) > 10 else ''}")
            # Điền giá trị mặc định
            filtered_df['ICB_L2'].fillna('Không xác định', inplace=True)
        else:
            logger.info("  • Tất cả ticker đều có thông tin phân ngành")
        
        # 5. Đảm bảo thứ tự cột giống file gốc (ICB_L2 ở cuối)
        expected_columns = ['REPORT_DATE', 'YEAR', 'QUARTER', 'REPORTED_DATE', 'FREQ_CODE', 
                          'SECURITY_CODE', 'AUDITED', 'METRIC_CODE', 'Mô tả', 'METRIC_VALUE', 'ICB_L2']
        
        # Chỉ giữ các cột có trong filtered_df
        final_columns = [col for col in expected_columns if col in filtered_df.columns]
        filtered_df = filtered_df[final_columns]
        
        logger.info(f"  • Cột cuối cùng: {filtered_df.columns.tolist()}")
        
        return filtered_df
    
    def save_output(self, df: pd.DataFrame):
        """Lưu file output và tạo summary"""
        # Lưu file parquet
        logger.info("\n5. LƯU FILE KẾT QUẢ")
        df.to_parquet(self.output_file, index=False, compression='snappy')
        file_size = self.output_file.stat().st_size / (1024 * 1024)
        logger.info(f"  ✅ Đã lưu: {self.output_file.name} ({file_size:.2f} MB)")
        
        # Tạo summary
        summary_data = {
            'ticker': [],
            'sector': [],
            'record_count': [],
            'years': [],
            'quarters': []
        }
        
        for ticker in sorted(df['SECURITY_CODE'].unique()):
            ticker_data = df[df['SECURITY_CODE'] == ticker]
            summary_data['ticker'].append(ticker)
            summary_data['sector'].append(ticker_data['ICB_L2'].iloc[0])
            summary_data['record_count'].append(len(ticker_data))
            summary_data['years'].append(f"{ticker_data['YEAR'].min()}-{ticker_data['YEAR'].max()}")
            
            quarters = ticker_data[ticker_data['FREQ_CODE'] == 'Q']['QUARTER'].unique()
            summary_data['quarters'].append(len(quarters))
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(self.summary_file, index=False)
        logger.info(f"  ✅ Đã lưu summary: {self.summary_file.name}")
    
    def print_statistics(self, df: pd.DataFrame):
        """In thống kê chi tiết"""
        logger.info("\n" + "="*70)
        logger.info("THỐNG KÊ CHI TIẾT")
        logger.info("="*70)
        
        # Thống kê cơ bản
        logger.info("\n📊 THỐNG KÊ CƠ BẢN:")
        logger.info(f"  • Tổng records: {len(df):,}")
        logger.info(f"  • Số ticker: {df['SECURITY_CODE'].nunique()}")
        logger.info(f"  • Số phân ngành: {df['ICB_L2'].nunique()}")
        
        # Thời gian
        logger.info("\n📅 PHẠM VI THỜI GIAN:")
        years = sorted(df['YEAR'].unique())
        logger.info(f"  • Năm: {min(years)} - {max(years)}")
        
        # Tính số quý có dữ liệu
        quarterly_data = df[df['FREQ_CODE'] == 'Q']
        if not quarterly_data.empty:
            quarters_by_year = quarterly_data.groupby('YEAR')['QUARTER'].nunique()
            logger.info(f"  • Dữ liệu theo quý:")
            for year, q_count in quarters_by_year.items():
                logger.info(f"    - {year}: {q_count} quý")
        
        # Top ngành
        logger.info("\n🏢 TOP NGÀNH (theo số ticker):")
        sector_counts = df.groupby('ICB_L2')['SECURITY_CODE'].nunique().sort_values(ascending=False)
        for sector, count in sector_counts.head(10).items():
            logger.info(f"  • {sector}: {count} ticker")
        
        # Top ticker (theo số records)
        logger.info("\n📈 TOP TICKER (theo số records):")
        ticker_counts = df['SECURITY_CODE'].value_counts()
        for ticker, count in ticker_counts.head(10).items():
            sector = df[df['SECURITY_CODE'] == ticker]['ICB_L2'].iloc[0]
            logger.info(f"  • {ticker}: {count} records ({sector})")
    
    def run(self):
        """Chạy toàn bộ quy trình"""
        try:
            # Kiểm tra file
            if not self.validate_files():
                return False
            
            # Xử lý dữ liệu
            processed_df = self.process_data()
            
            # Lưu kết quả
            self.save_output(processed_df)
            
            # In thống kê
            self.print_statistics(processed_df)
            
            logger.info("\n" + "="*70)
            logger.info("✅ CẬP NHẬT THÀNH CÔNG!")
            logger.info("="*70)
            logger.info(f"\nFile output: {self.output_file}")
            logger.info(f"Sử dụng lệnh sau để kiểm tra:")
            logger.info(f"  python3 -c \"import pandas as pd; df=pd.read_parquet('{self.output_file}'); print(df.info())\"")
            
            return True
            
        except Exception as e:
            logger.error(f"\n❌ LỖI: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

def main():
    """Main function"""
    print("\n" + "🚀 "*20)
    print("SCRIPT CẬP NHẬT DỮ LIỆU BUU_CLEAN_VER2")
    print("🚀 "*20)
    
    updater = DataUpdater()
    success = updater.run()
    
    if success:
        print("\n✨ Hoàn thành! Dữ liệu đã được cập nhật.")
    else:
        print("\n❌ Có lỗi xảy ra. Vui lòng kiểm tra log.")
    
    return success

if __name__ == "__main__":
    main()