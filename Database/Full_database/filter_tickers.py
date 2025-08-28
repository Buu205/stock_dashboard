"""
Script lọc ticker cổ phiếu theo điều kiện và mapping phân ngành ICB L2
Flow:
1. Lọc ticker có giá trị trung bình 1 tháng > 1 tỷ đồng
2. Lấy danh sách ticker đã lọc và thêm cột phân ngành ICB L2
3. Mapping phân ngành vào file Buu_clean_ver2.parquet
4. Loại bỏ ticker không có trong danh sách đã lọc
5. Tạo file mới Buu_clean_ver2.parquet
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Thiết lập logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_fiinx_data(file_path: str) -> pd.DataFrame:
    """Load dữ liệu từ file Excel data_fiinx.xlsx"""
    try:
        logger.info(f"Loading data from {file_path}")
        df = pd.read_excel(file_path)
        logger.info(f"Loaded {len(df)} records with columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        raise

def filter_tickers_by_volume(df: pd.DataFrame, min_volume: float = 1e9) -> pd.DataFrame:
    """
    Lọc ticker có giá trị trung bình 1 tháng > min_volume (mặc định 1 tỷ đồng)
    
    Args:
        df: DataFrame từ file fiinx
        min_volume: Giá trị tối thiểu (đơn vị: đồng)
    
    Returns:
        DataFrame đã lọc
    """
    # Tìm cột chứa "Giá trị trung bình 1 tháng"
    volume_col = None
    for col in df.columns:
        if "Giá trị trung bình 1 tháng" in col:
            volume_col = col
            break
    
    if volume_col is None:
        raise ValueError("Không tìm thấy cột 'Giá trị trung bình 1 tháng'")
    
    logger.info(f"Using volume column: {volume_col}")
    
    # Lọc theo điều kiện
    filtered_df = df[df[volume_col] > min_volume].copy()
    
    logger.info(f"Filtered from {len(df)} to {len(filtered_df)} tickers")
    logger.info(f"Volume threshold: {min_volume:,.0f} đồng")
    
    return filtered_df

def extract_ticker_sector_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trích xuất mapping ticker - phân ngành ICB L2
    
    Args:
        df: DataFrame đã lọc từ fiinx
    
    Returns:
        DataFrame với cột TICKER và ICB_L2
    """
    # Chuẩn hóa tên cột
    ticker_col = 'Mã'
    sector_col = 'Phân ngành - ICB L2'
    
    if ticker_col not in df.columns:
        raise ValueError(f"Không tìm thấy cột '{ticker_col}'")
    if sector_col not in df.columns:
        raise ValueError(f"Không tìm thấy cột '{sector_col}'")
    
    # Tạo mapping
    mapping_df = df[[ticker_col, sector_col]].copy()
    mapping_df.columns = ['TICKER', 'ICB_L2']
    
    # Chuẩn hóa ticker (UPPER + strip)
    mapping_df['TICKER'] = mapping_df['TICKER'].str.upper().str.strip()
    
    # Loại bỏ duplicate và null
    mapping_df = mapping_df.dropna().drop_duplicates()
    
    logger.info(f"Created ticker-sector mapping for {len(mapping_df)} tickers")
    
    return mapping_df

def load_buu_clean_data(file_path: str) -> pd.DataFrame:
    """Load dữ liệu từ file buu_clean_data.parquet"""
    try:
        logger.info(f"Loading buu_clean_data from {file_path}")
        df = pd.read_parquet(file_path)
        logger.info(f"Loaded {len(df)} records with columns: {df.columns.tolist()}")
        return df
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        raise

def join_sector_data(buu_df: pd.DataFrame, sector_mapping: pd.DataFrame) -> pd.DataFrame:
    """
    Join dữ liệu phân ngành vào buu_clean_data
    
    Args:
        buu_df: DataFrame từ buu_clean_data.parquet
        sector_mapping: DataFrame mapping ticker-sector
    
    Returns:
        DataFrame đã join
    """
    # Chuẩn hóa SECURITY_CODE trong buu_df
    buu_df = buu_df.copy()
    buu_df['SECURITY_CODE_CLEAN'] = buu_df['SECURITY_CODE'].str.upper().str.strip()
    
    # Join với sector mapping
    merged_df = buu_df.merge(
        sector_mapping, 
        left_on='SECURITY_CODE_CLEAN', 
        right_on='TICKER', 
        how='inner'
    )
    
    # Loại bỏ cột tạm
    merged_df = merged_df.drop(['SECURITY_CODE_CLEAN', 'TICKER'], axis=1)
    
    logger.info(f"Joined sector data: {len(merged_df)} records (from {len(buu_df)} original)")
    
    return merged_df

def save_filtered_data(df: pd.DataFrame, output_path: str):
    """Lưu dữ liệu đã lọc vào file parquet mới"""
    try:
        logger.info(f"Saving filtered data to {output_path}")
        df.to_parquet(output_path, index=False)
        logger.info(f"Successfully saved {len(df)} records to {output_path}")
    except Exception as e:
        logger.error(f"Error saving to {output_path}: {e}")
        raise

def main():
    """Main function thực hiện toàn bộ flow"""
    try:
        # Đường dẫn file
        fiinx_path = "Database/data_fiinx.xlsx"
        buu_clean_path = "Database/buu_clean_data.parquet"
        output_path = "Database/Buu_clean_ver2.parquet"
        
        # Kiểm tra file tồn tại
        if not Path(fiinx_path).exists():
            raise FileNotFoundError(f"Không tìm thấy file {fiinx_path}")
        if not Path(buu_clean_path).exists():
            raise FileNotFoundError(f"Không tìm thấy file {buu_clean_path}")
        
        logger.info("=== BẮT ĐẦU XỬ LÝ ===")
        
        # Bước 1: Load và lọc dữ liệu fiinx
        fiinx_df = load_fiinx_data(fiinx_path)
        filtered_fiinx = filter_tickers_by_volume(fiinx_df, min_volume=1e9)
        
        # Bước 2: Trích xuất mapping ticker-sector
        sector_mapping = extract_ticker_sector_mapping(filtered_fiinx)
        
        # Bước 3: Load buu_clean_data
        buu_df = load_buu_clean_data(buu_clean_path)
        
        # Bước 4: Join sector data và lọc ticker
        final_df = join_sector_data(buu_df, sector_mapping)
        
        # Bước 5: Lưu file mới
        save_filtered_data(final_df, output_path)
        
        # Thống kê kết quả
        logger.info("=== THỐNG KÊ KẾT QUẢ ===")
        logger.info(f"Tổng số ticker ban đầu: {len(fiinx_df)}")
        logger.info(f"Số ticker sau khi lọc (>1 tỷ): {len(filtered_fiinx)}")
        logger.info(f"Số record trong buu_clean_data: {len(buu_df)}")
        logger.info(f"Số record sau khi join và lọc: {len(final_df)}")
        logger.info(f"File output: {output_path}")
        
        # Hiển thị danh sách ticker đã lọc
        tickers_filtered = sector_mapping['TICKER'].tolist()
        logger.info(f"Danh sách ticker đã lọc ({len(tickers_filtered)}): {tickers_filtered[:10]}{'...' if len(tickers_filtered) > 10 else ''}")
        
        logger.info("=== HOÀN THÀNH XỬ LÝ ===")
        
    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý: {e}")
        raise

if __name__ == "__main__":
    main()
