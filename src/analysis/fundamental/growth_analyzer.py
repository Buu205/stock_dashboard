"""
Growth Analysis Module for Financial Data
Phân tích tăng trưởng doanh thu, lợi nhuận và các chỉ số tài chính
"""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class GrowthAnalyzer:
    """
    Phân tích tăng trưởng tài chính của công ty
    """
    
    def __init__(self, data_path: str):
        """
        Khởi tạo Growth Analyzer
        
        Args:
            data_path: Đường dẫn đến file dữ liệu (parquet/csv)
        """
        self.data_path = data_path
        self.data = None
        self._load_data()
    
    def _load_data(self):
        """Load dữ liệu từ file"""
        try:
            if self.data_path.endswith('.parquet'):
                self.data = pd.read_parquet(self.data_path)
            elif self.data_path.endswith('.csv'):
                self.data = pd.read_csv(self.data_path)
            else:
                raise ValueError("Unsupported file format. Use .parquet or .csv")
                
            logger.info(f"Data loaded successfully: {len(self.data)} records")
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            self.data = pd.DataFrame()
    
    def generate_growth_analysis(self, security_code: str) -> Dict[str, pd.DataFrame]:
        """
        Tạo phân tích tăng trưởng cho một mã chứng khoán
        
        Args:
            security_code: Mã chứng khoán (VD: MWG, VNM)
            
        Returns:
            Dictionary chứa các DataFrame phân tích
        """
        if self.data.empty:
            logger.warning("No data available for analysis")
            return self._empty_results()
        
        try:
            # Filter data cho security_code
            company_data = self.data[
                self.data['SECURITY_CODE'] == security_code.upper()
            ].copy()
            
            if company_data.empty:
                logger.warning(f"No data found for {security_code}")
                return self._empty_results()
            
            # Tạo các phân tích
            results = {
                'quarterly_data': self._prepare_quarterly_data(company_data),
                'annual_data': self._prepare_annual_data(company_data),
                'ttm_growth_data': self._calculate_ttm_growth(company_data),
                'quarterly_margins': self._calculate_quarterly_margins(company_data),
                'annual_margins': self._calculate_annual_margins(company_data)
            }
            
            logger.info(f"Growth analysis completed for {security_code}")
            return results
            
        except Exception as e:
            logger.error(f"Error in growth analysis: {e}")
            return self._empty_results()
    
    def _prepare_quarterly_data(self, company_data: pd.DataFrame) -> pd.DataFrame:
        """Chuẩn bị dữ liệu quý"""
        try:
            quarterly = company_data[
                company_data['FREQ_CODE'] == 'Q'
            ].copy()
            
            if quarterly.empty:
                return pd.DataFrame()
            
            # Convert from VND to billion VND (tỷ đồng)
            quarterly['METRIC_VALUE'] = quarterly['METRIC_VALUE'] / 1e9
            
            # Pivot để có cột riêng cho từng metric
            quarterly_pivot = quarterly.pivot_table(
                index=['REPORT_DATE', 'YEAR', 'QUARTER'],
            columns='METRIC_CODE',
            values='METRIC_VALUE',
            aggfunc='first'
        ).reset_index()
        
            return quarterly_pivot
            
        except Exception as e:
            logger.error(f"Error preparing quarterly data: {e}")
            return pd.DataFrame()
    
    def _prepare_annual_data(self, company_data: pd.DataFrame) -> pd.DataFrame:
        """Chuẩn bị dữ liệu năm"""
        try:
            annual = company_data[
                company_data['FREQ_CODE'] == 'Y'
            ].copy()
            
            if annual.empty:
                return pd.DataFrame()
            
            # Convert from VND to billion VND (tỷ đồng)
            annual['METRIC_VALUE'] = annual['METRIC_VALUE'] / 1e9
            
            # Pivot để có cột riêng cho từng metric
            annual_pivot = annual.pivot_table(
                index=['REPORT_DATE', 'YEAR'],
                columns='METRIC_CODE',
                values='METRIC_VALUE',
                aggfunc='first'
            ).reset_index()
            
            return annual_pivot
            
        except Exception as e:
            logger.error(f"Error preparing annual data: {e}")
            return pd.DataFrame()
    
    def _calculate_ttm_growth(self, company_data: pd.DataFrame) -> pd.DataFrame:
        """Tính tăng trưởng TTM (Trailing Twelve Months)"""
        try:
            quarterly = company_data[
                company_data['FREQ_CODE'] == 'Q'
            ].copy()
            
            if quarterly.empty:
                return pd.DataFrame()
            
            # Convert from VND to billion VND (tỷ đồng)
            quarterly['METRIC_VALUE'] = quarterly['METRIC_VALUE'] / 1e9
            
            # Tính TTM cho các metric chính
            metrics_to_analyze = ['CIS_10', 'CIS_20', 'CIS_61']  # Revenue, Gross Profit, Net Profit
            
            ttm_data = []
            
            for metric in metrics_to_analyze:
                metric_data = quarterly[
                    quarterly['METRIC_CODE'] == metric
                ].sort_values(['YEAR', 'QUARTER'])
                
                if len(metric_data) >= 4:
                    # Tính TTM (4 quý gần nhất)
                    for i in range(3, len(metric_data)):
                        current_ttm = metric_data.iloc[i-3:i+1]['METRIC_VALUE'].sum()
                        previous_ttm = metric_data.iloc[i-4:i]['METRIC_VALUE'].sum()
                        
                        if previous_ttm > 0:
                            growth_rate = ((current_ttm - previous_ttm) / previous_ttm) * 100
                        else:
                            growth_rate = 0
                        
                        ttm_data.append({
                            'Year': metric_data.iloc[i]['YEAR'],
                            'Quarter': metric_data.iloc[i]['QUARTER'],
                            'Metric': metric,
                            'TTM_Value': current_ttm,
                            'TTM_Growth_Rate': growth_rate
                        })
            
            if ttm_data:
                ttm_df = pd.DataFrame(ttm_data)
                # Pivot để có cột riêng cho từng metric
                ttm_pivot = ttm_df.pivot_table(
                    index=['Year', 'Quarter'],
                    columns='Metric',
                    values='TTM_Growth_Rate',
                    aggfunc='first'
                ).reset_index()
                
                # Rename columns
                ttm_pivot.columns = ['Year', 'Quarter'] + [
                    f'{col}_TTM_Growth' for col in ttm_pivot.columns[2:]
                ]
                
                return ttm_pivot
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error calculating TTM growth: {e}")
            return pd.DataFrame()
    
    def _infer_sign(self, series: pd.Series) -> int:
        """Suy luận dấu của dữ liệu dựa trên median"""
        med = pd.to_numeric(series, errors='coerce').median(skipna=True)
        # nếu median < 0 => dữ liệu lưu âm
        return -1 if pd.notna(med) and med < 0 else 1
    
    def _calculate_quarterly_margins(self, company_data: pd.DataFrame) -> pd.DataFrame:
        """Tính margins theo quý"""
        try:
            quarterly = company_data[
                company_data['FREQ_CODE'] == 'Q'
            ].copy()
            
            if quarterly.empty:
                return pd.DataFrame()
            
            # Convert from VND to billion VND (tỷ đồng)
            quarterly['METRIC_VALUE'] = quarterly['METRIC_VALUE'] / 1e9
            
            # Pivot để có cột riêng cho từng metric
            quarterly_pivot = quarterly.pivot_table(
                index=['REPORT_DATE', 'YEAR', 'QUARTER'],
                columns='METRIC_CODE',
                values='METRIC_VALUE',
                aggfunc='first'
            ).reset_index()
            
            # Tính margins nếu có đủ dữ liệu
            if 'CIS_10' in quarterly_pivot.columns and 'CIS_20' in quarterly_pivot.columns:
                quarterly_pivot['Gross_Margin'] = np.where(
                    quarterly_pivot['CIS_10'] != 0,
                    (quarterly_pivot['CIS_20'] / quarterly_pivot['CIS_10']) * 100,
                    np.nan
                )
            
            # Operating Margin = (CIS_20 + sign25*CIS_25 + sign26*CIS_26) / CIS_10
            if all(col in quarterly_pivot.columns for col in ['CIS_10', 'CIS_20', 'CIS_25', 'CIS_26']):
                # Xác định dấu cho chi phí
                sign25 = self._infer_sign(quarterly_pivot['CIS_25'])
                sign26 = self._infer_sign(quarterly_pivot['CIS_26'])
                
                # Tính operating profit với dấu đúng (Định nghĩa metric code: OPERATING_PROFIT)
                operating_profit = quarterly_pivot['CIS_20'] + sign25*quarterly_pivot['CIS_25'] + sign26*quarterly_pivot['CIS_26']
                quarterly_pivot['OPERATING_PROFIT'] = operating_profit  # Lưu operating profit với metric code cố định
                
                quarterly_pivot['Operating_Margin'] = np.where(
                    quarterly_pivot['CIS_10'] != 0,
                    (operating_profit / quarterly_pivot['CIS_10']) * 100,
                    np.nan
                )
            
            # Tính EBITDA = Operating Profit + CCFI_2 (Khấu hao)
            # Định nghĩa metric code: EBITDA
            if 'OPERATING_PROFIT' in quarterly_pivot.columns and 'CCFI_2' in quarterly_pivot.columns:
                quarterly_pivot['EBITDA'] = quarterly_pivot['OPERATING_PROFIT'] + quarterly_pivot['CCFI_2']
            elif all(col in quarterly_pivot.columns for col in ['CIS_20', 'CIS_25', 'CIS_26', 'CCFI_2']):
                # Nếu chưa có OPERATING_PROFIT, tính trực tiếp
                sign25 = self._infer_sign(quarterly_pivot['CIS_25'])
                sign26 = self._infer_sign(quarterly_pivot['CIS_26'])
                operating_profit = quarterly_pivot['CIS_20'] + sign25*quarterly_pivot['CIS_25'] + sign26*quarterly_pivot['CIS_26']
                quarterly_pivot['OPERATING_PROFIT'] = operating_profit
                quarterly_pivot['EBITDA'] = operating_profit + quarterly_pivot['CCFI_2']
            
            # Tính EBITDA Margin
            if 'EBITDA' in quarterly_pivot.columns and 'CIS_10' in quarterly_pivot.columns:
                quarterly_pivot['EBITDA_Margin'] = np.where(
                    quarterly_pivot['CIS_10'] != 0,
                    (quarterly_pivot['EBITDA'] / quarterly_pivot['CIS_10']) * 100,
                    np.nan
                )
            
            # Net Margin = CIS_61 / CIS_10
            if 'CIS_61' in quarterly_pivot.columns and 'CIS_10' in quarterly_pivot.columns:
                quarterly_pivot['Net_Margin'] = np.where(
                    quarterly_pivot['CIS_10'] != 0,
                    (quarterly_pivot['CIS_61'] / quarterly_pivot['CIS_10']) * 100,
                    np.nan
                )
            
            return quarterly_pivot
            
        except Exception as e:
            logger.error(f"Error calculating quarterly margins: {e}")
            return pd.DataFrame()
    
    def _calculate_annual_margins(self, company_data: pd.DataFrame) -> pd.DataFrame:
        """Tính margins theo năm"""
        try:
            annual = company_data[
                company_data['FREQ_CODE'] == 'Y'
            ].copy()
            
            if annual.empty:
                return pd.DataFrame()
            
            # Convert from VND to billion VND (tỷ đồng)
            annual['METRIC_VALUE'] = annual['METRIC_VALUE'] / 1e9
            
            # Pivot để có cột riêng cho từng metric
            annual_pivot = annual.pivot_table(
                index=['REPORT_DATE', 'YEAR'],
                columns='METRIC_CODE',
                values='METRIC_VALUE',
                aggfunc='first'
            ).reset_index()
            
            # Tính margins nếu có đủ dữ liệu
            if 'CIS_10' in annual_pivot.columns and 'CIS_20' in annual_pivot.columns:
                annual_pivot['Gross_Margin'] = np.where(
                    annual_pivot['CIS_10'] != 0,
                    (annual_pivot['CIS_20'] / annual_pivot['CIS_10']) * 100,
                    np.nan
                )
            
            # Operating Margin = (CIS_20 + sign25*CIS_25 + sign26*CIS_26) / CIS_10
            if all(col in annual_pivot.columns for col in ['CIS_10', 'CIS_20', 'CIS_25', 'CIS_26']):
                # Xác định dấu cho chi phí
                sign25 = self._infer_sign(annual_pivot['CIS_25'])
                sign26 = self._infer_sign(annual_pivot['CIS_26'])
                
                # Tính operating profit với dấu đúng (Định nghĩa metric code: OPERATING_PROFIT)
                operating_profit = annual_pivot['CIS_20'] + sign25*annual_pivot['CIS_25'] + sign26*annual_pivot['CIS_26']
                annual_pivot['OPERATING_PROFIT'] = operating_profit  # Lưu operating profit với metric code cố định
                
                annual_pivot['Operating_Margin'] = np.where(
                    annual_pivot['CIS_10'] != 0,
                    (operating_profit / annual_pivot['CIS_10']) * 100,
                    np.nan
                )
            
            # Tính EBITDA = Operating Profit + CCFI_2 (Khấu hao)
            # Định nghĩa metric code: EBITDA
            if 'OPERATING_PROFIT' in annual_pivot.columns and 'CCFI_2' in annual_pivot.columns:
                annual_pivot['EBITDA'] = annual_pivot['OPERATING_PROFIT'] + annual_pivot['CCFI_2']
            elif all(col in annual_pivot.columns for col in ['CIS_20', 'CIS_25', 'CIS_26', 'CCFI_2']):
                # Nếu chưa có OPERATING_PROFIT, tính trực tiếp
                sign25 = self._infer_sign(annual_pivot['CIS_25'])
                sign26 = self._infer_sign(annual_pivot['CIS_26'])
                operating_profit = annual_pivot['CIS_20'] + sign25*annual_pivot['CIS_25'] + sign26*annual_pivot['CIS_26']
                annual_pivot['OPERATING_PROFIT'] = operating_profit
                annual_pivot['EBITDA'] = operating_profit + annual_pivot['CCFI_2']
            
            # Tính EBITDA Margin
            if 'EBITDA' in annual_pivot.columns and 'CIS_10' in annual_pivot.columns:
                annual_pivot['EBITDA_Margin'] = np.where(
                    annual_pivot['CIS_10'] != 0,
                    (annual_pivot['EBITDA'] / annual_pivot['CIS_10']) * 100,
                    np.nan
                )
            
            # Net Margin = CIS_61 / CIS_10
            if 'CIS_61' in annual_pivot.columns and 'CIS_10' in annual_pivot.columns:
                annual_pivot['Net_Margin'] = np.where(
                    annual_pivot['CIS_10'] != 0,
                    (annual_pivot['CIS_61'] / annual_pivot['CIS_10']) * 100,
                    np.nan
                )
            
            return annual_pivot
            
        except Exception as e:
            logger.error(f"Error calculating annual margins: {e}")
            return pd.DataFrame()
    
    def _empty_results(self) -> Dict[str, pd.DataFrame]:
        """Trả về kết quả rỗng khi có lỗi"""
        return {
            'quarterly_data': pd.DataFrame(),
            'annual_data': pd.DataFrame(),
            'ttm_growth_data': pd.DataFrame(),
            'quarterly_margins': pd.DataFrame(),
            'annual_margins': pd.DataFrame()
        }


def analyze_company_growth(data_path: str, security_code: str) -> Dict[str, pd.DataFrame]:
    """
    Hàm tiện ích để phân tích tăng trưởng nhanh
    
    Args:
        data_path: Đường dẫn đến file dữ liệu
        security_code: Mã chứng khoán
    
    Returns:
        Dictionary chứa kết quả phân tích
    """
    analyzer = GrowthAnalyzer(data_path)
    return analyzer.generate_growth_analysis(security_code)