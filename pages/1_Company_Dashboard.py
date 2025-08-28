# -*- coding: utf-8 -*-
"""
Company Dashboard - Financial Analysis
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime
import os
import sys
import numpy as np

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from analysis.fundamental.growth_analyzer import GrowthAnalyzer

# Page config
st.set_page_config(
    page_title="Company Dashboard",
    page_icon="📈",
    layout="wide"
)

# Title
st.title("📈 Company Financial Dashboard")
st.markdown("Phân tích chi tiết các chỉ số tài chính, biên lợi nhuận và tốc độ tăng trưởng")

# Sidebar - Ticker selection
st.sidebar.header("🔍 Chọn mã cổ phiếu")

@st.cache_data
def load_ticker_list():
    """Load danh sách ticker từ database"""
    parquet_path = "Database/Full_database/Buu_clean_ver2.parquet"
    if os.path.exists(parquet_path):
        df = pd.read_parquet(parquet_path)
        return sorted(df['SECURITY_CODE'].unique())
    return []

# Load tickers
tickers = load_ticker_list()

if not tickers:
    st.error("❌ Không thể load dữ liệu. Vui lòng kiểm tra file Database/Full_database/Buu_clean_ver2.parquet")
    st.stop()

# Ticker selection
selected_ticker = st.sidebar.selectbox(
    "Chọn mã cổ phiếu:",
    tickers,
    index=tickers.index("MWG") if "MWG" in tickers else 0
)

# Period type selection (Quarterly or Annual)
st.sidebar.markdown("---")
period_type = st.sidebar.radio(
    "📊 Loại chu kỳ hiển thị:",
    ["Theo Quý", "Theo Năm"],
    index=0
)

# Load and analyze data
@st.cache_data
def analyze_ticker(ticker):
    """Phân tích dữ liệu cho ticker"""
    try:
        analyzer = GrowthAnalyzer("Database/Full_database/Buu_clean_ver2.parquet")
        return analyzer.generate_growth_analysis(ticker)
    except Exception as e:
        st.error(f"Lỗi khi phân tích {ticker}: {str(e)}")
        return None

def calculate_ma4_growth(data, value_column):
    """
    Tính MA4 growth: (Sum của 4 quý gần nhất) / (Sum của 4 quý trước đó) - 1
    """
    ma4_growth = []
    values = data[value_column].values
    
    for i in range(len(values)):
        if i < 7:  # Cần ít nhất 8 quý để tính
            ma4_growth.append(np.nan)
        else:
            current_4q = values[i-3:i+1].sum()  # 4 quý gần nhất
            previous_4q = values[i-7:i-3].sum()  # 4 quý trước đó
            
            if previous_4q != 0:
                growth = (current_4q / previous_4q - 1) * 100
                ma4_growth.append(growth)
            else:
                ma4_growth.append(np.nan)
    
    return ma4_growth

def calculate_yoy_growth(data, value_column):
    """
    Tính Year-over-Year growth cho dữ liệu năm
    """
    yoy_growth = []
    values = data[value_column].values
    
    for i in range(len(values)):
        if i < 1:  # Cần ít nhất 2 năm để tính
            yoy_growth.append(np.nan)
        else:
            if values[i-1] != 0:
                growth = ((values[i] / values[i-1]) - 1) * 100
                yoy_growth.append(growth)
            else:
                yoy_growth.append(np.nan)
    
    return yoy_growth

def prepare_annual_data(quarterly_data, metric_codes):
    """
    Chuyển đổi dữ liệu quý sang năm bằng cách sum 4 quý
    """
    if quarterly_data.empty:
        return pd.DataFrame()
    
    # Ensure EBITDA and OPERATING_PROFIT are in metric_codes if they exist in quarterly_data
    if 'EBITDA' in quarterly_data.columns and 'EBITDA' not in metric_codes:
        metric_codes = metric_codes + ['EBITDA']
    if 'OPERATING_PROFIT' in quarterly_data.columns and 'OPERATING_PROFIT' not in metric_codes:
        metric_codes = metric_codes + ['OPERATING_PROFIT']
    
    # Group by year and sum
    annual_data = quarterly_data.groupby('YEAR').agg({
        col: 'sum' for col in metric_codes if col in quarterly_data.columns
    }).reset_index()
    
    # Sort by year
    annual_data = annual_data.sort_values('YEAR')
    
    return annual_data

def create_metric_chart(data, metric_code, metric_name, color='blue', is_quarterly=True):
    """
    Tạo biểu đồ cho một metric với cột và đường growth (MA4 cho quý, YoY cho năm)
    """
    if is_quarterly:
        # Filter data từ 2018
        chart_data = data[data['YEAR'] >= 2018].copy()
        chart_data = chart_data.sort_values(['YEAR', 'QUARTER'])
        chart_data['Period'] = chart_data['QUARTER'].astype(str) + 'Q' + chart_data['YEAR'].astype(str)
        
        # Calculate MA4 growth
        chart_data['Growth'] = calculate_ma4_growth(chart_data, metric_code)
        growth_label = 'MA4 Growth (%)'
        x_label = "Quý"
    else:
        # Annual data
        chart_data = data[data['YEAR'] >= 2018].copy()
        chart_data = chart_data.sort_values('YEAR')
        chart_data['Period'] = chart_data['YEAR'].astype(str)
        
        # Calculate YoY growth
        chart_data['Growth'] = calculate_yoy_growth(chart_data, metric_code)
        growth_label = 'YoY Growth (%)'
        x_label = "Năm"
    
    # Create figure with secondary y-axis
    fig = make_subplots(
        rows=1, cols=1,
        specs=[[{"secondary_y": True}]]
    )
    
    # Add bar chart for metric values
    fig.add_trace(
        go.Bar(
            x=chart_data['Period'],
            y=chart_data[metric_code],
            name=metric_name,
            marker_color=color,
            opacity=0.7,
            text=[f'{v:,.0f}' if pd.notna(v) else '' for v in chart_data[metric_code]],
            textposition='outside',
            textfont=dict(size=9)
        ),
        secondary_y=False
    )
    
    # Add growth line
    fig.add_trace(
        go.Scatter(
            x=chart_data['Period'],
            y=chart_data['Growth'],
            mode='lines+markers',
            name=growth_label,
            line=dict(color='red', width=2),
            marker=dict(size=6),
            yaxis='y2'
        ),
        secondary_y=True
    )
    
    # Update layout
    fig.update_xaxes(title_text=x_label, tickangle=45)
    fig.update_yaxes(title_text=f"{metric_name} (Tỷ VNĐ)", secondary_y=False)
    fig.update_yaxes(title_text=growth_label, secondary_y=True)
    
    fig.update_layout(
        title=f"{metric_name} - {selected_ticker}",
        hovermode='x unified',
        height=500,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Add zero line for growth
    fig.add_hline(y=0, line_dash="dash", line_color="gray", secondary_y=True)
    
    return fig

def prepare_display_data(data, data_type, is_quarterly=True):
    """
    Chuẩn bị dữ liệu để hiển thị trong tab chi tiết với metrics làm hàng và thời gian làm cột
    """
    if data.empty:
        return pd.DataFrame()
    
    # Define metric mappings with ordered lists to preserve sequence
    income_metrics_ordered = [
        ('CIS_10', 'Doanh thu thuần'),
        ('CIS_11', 'Giá vốn hàng bán'),
        ('CIS_20', 'Lợi nhuận gộp'),
        ('CIS_25', 'Chi phí QLDN'),
        ('CIS_26', 'Chi phí bán hàng'),
        ('CIS_21', 'Thu nhập tài chính'),
        ('CIS_22', 'Chi phí tài chính'),
        ('CIS_31', 'Thu nhập khác'),
        ('CIS_32', 'Chi phí khác'),
        ('CIS_50', 'Lợi nhuận trước thuế'),
        ('CIS_61', 'Lợi nhuận ròng'),
        ('OPERATING_PROFIT', 'Lợi nhuận hoạt động'),
        ('EBITDA', 'EBITDA')
    ]
    
    balance_metrics_ordered = [
        ('CBS_270', 'Tổng tài sản'),
        ('CBS_100', 'Tài sản ngắn hạn'),
        ('CBS_110', 'Tiền và tương đương'),
        ('CBS_112', 'Đầu tư ngắn hạn'),
        ('CBS_130', 'Phải thu'),
        ('CBS_140', 'Hàng tồn kho'),
        ('CBS_220', 'Tài sản cố định'),
        ('CBS_300', 'Tổng nợ'),
        ('CBS_320', 'Nợ ngắn hạn'),
        ('CBS_338', 'Nợ dài hạn'),
        ('CBS_400', 'Vốn chủ sở hữu')
    ]
    
    cashflow_metrics_ordered = [
        ('CFS_20', 'Dòng tiền HĐKD'),
        ('CFS_30', 'Dòng tiền đầu tư'),
        ('CFS_40', 'Dòng tiền tài chính'),
        ('CFS_50', 'Lưu chuyển tiền thuần'),
        ('CCFI_2', 'Khấu hao')
    ]
    
    margin_columns_ordered = [
        ('Gross_Margin', 'Gross Margin (%)'),
        ('Operating_Margin', 'Operating Margin (%)'),
        ('EBITDA_Margin', 'EBITDA Margin (%)'),
        ('Net_Margin', 'Net Margin (%)')
    ]
    
    display_df = data.copy()
    
    # Filter from 2018
    if 'YEAR' in display_df.columns:
        display_df = display_df[display_df['YEAR'] >= 2018]
    
    # Remove REPORT_DATE column if exists
    if 'REPORT_DATE' in display_df.columns:
        display_df = display_df.drop(columns=['REPORT_DATE'])
    
    # Create Period column and sort
    if is_quarterly and 'YEAR' in display_df.columns and 'QUARTER' in display_df.columns:
        display_df = display_df.sort_values(['YEAR', 'QUARTER'])
        # Format: 1Q2018 instead of 2018-Q1
        display_df['Period'] = display_df['QUARTER'].astype(str) + 'Q' + display_df['YEAR'].astype(str)
        # Drop YEAR and QUARTER columns after creating Period
        display_df = display_df.drop(columns=['YEAR', 'QUARTER'], errors='ignore')
    elif 'YEAR' in display_df.columns:
        display_df = display_df.sort_values('YEAR')
        display_df['Period'] = display_df['YEAR'].astype(str)
        display_df = display_df.drop(columns=['YEAR'], errors='ignore')
    else:
        # If no Period column can be created, return empty
        return pd.DataFrame()
    
    # Select metrics based on data type
    if data_type == "Financial Data":
        metrics_ordered = income_metrics_ordered + balance_metrics_ordered + cashflow_metrics_ordered
    elif data_type == "Income Statement":
        metrics_ordered = income_metrics_ordered
    elif data_type == "Balance Sheet":
        metrics_ordered = balance_metrics_ordered
    elif data_type == "Cash Flow":
        metrics_ordered = cashflow_metrics_ordered
    elif data_type == "Margins":
        metrics_ordered = margin_columns_ordered
    else:
        return pd.DataFrame()
    
    # Create result dataframe
    result_data = []
    
    # Process each metric in order
    for metric_code, metric_name in metrics_ordered:
        if metric_code in display_df.columns:
            # Create a row for this metric
            row_data = {'Chỉ tiêu': metric_name}
            
            # Add values for each period
            for _, period_row in display_df.iterrows():
                period = period_row['Period']
                value = period_row.get(metric_code, np.nan)
                row_data[period] = value
            
            result_data.append(row_data)
    
    if not result_data:
        return pd.DataFrame()
    
    # Create final dataframe
    transposed_df = pd.DataFrame(result_data)
    
    # Ensure Period columns are sorted properly
    period_cols = [col for col in transposed_df.columns if col != 'Chỉ tiêu']
    
    # Sort periods correctly (extract year and quarter for proper sorting)
    def sort_period_key(period_str):
        try:
            # Format: 1Q2018, 2Q2018, etc.
            if 'Q' in period_str:
                quarter, year = period_str.split('Q')
                return (int(year), int(quarter))
            else:
                # Format: 2018, 2019, etc. (annual)
                return (int(period_str), 0)
        except:
            return (9999, 0)  # Put any unparseable values at the end
    
    period_cols_sorted = sorted(period_cols, key=sort_period_key)
    
    # Reorder columns: 'Chỉ tiêu' first, then sorted periods
    final_columns = ['Chỉ tiêu'] + period_cols_sorted
    transposed_df = transposed_df[final_columns]
    
    return transposed_df

def create_margin_chart(data, margin_columns, title, is_quarterly=True):
    """
    Tạo biểu đồ margins
    """
    # Filter data từ 2018
    chart_data = data[data['YEAR'] >= 2018].copy()
    
    if is_quarterly:
        chart_data = chart_data.sort_values(['YEAR', 'QUARTER'])
        chart_data['Period'] = chart_data['QUARTER'].astype(str) + 'Q' + chart_data['YEAR'].astype(str)
        x_label = "Quý"
    else:
        chart_data = chart_data.sort_values('YEAR')
        chart_data['Period'] = chart_data['YEAR'].astype(str)
        x_label = "Năm"
    
    fig = go.Figure()
    
    colors = {
        'Gross_Margin': 'green',
        'Operating_Margin': 'blue',
        'EBITDA_Margin': 'purple',
        'Net_Margin': 'red'
    }
    
    names = {
        'Gross_Margin': 'Gross Profit Margin',
        'Operating_Margin': 'Operating Margin',
        'EBITDA_Margin': 'EBITDA Margin',
        'Net_Margin': 'Net Profit Margin'
    }
    
    for col in margin_columns:
        if col in chart_data.columns:
            fig.add_trace(go.Scatter(
                x=chart_data['Period'],
                y=chart_data[col],
                mode='lines+markers',
                name=names.get(col, col),
                line=dict(color=colors.get(col, 'gray'), width=2),
                marker=dict(size=6)
            ))
    
    fig.update_layout(
        title=title,
        xaxis_title=x_label,
        yaxis_title="Margin (%)",
        hovermode='x unified',
        height=500,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    fig.update_xaxes(tickangle=45)
    
    return fig

# Analyze selected ticker
with st.spinner(f"Đang phân tích {selected_ticker}..."):
    analysis_results = analyze_ticker(selected_ticker)

if analysis_results is None:
    st.error("Không thể phân tích dữ liệu")
    st.stop()

# Extract data
quarterly_data = analysis_results.get('quarterly_data', pd.DataFrame())
annual_data = analysis_results.get('annual_data', pd.DataFrame())
ttm_growth = analysis_results.get('ttm_growth_data', pd.DataFrame())
quarterly_margins = analysis_results.get('quarterly_margins', pd.DataFrame())
annual_margins = analysis_results.get('annual_margins', pd.DataFrame())

# Ensure EBITDA is calculated in quarterly_data
if not quarterly_data.empty:
    if 'EBITDA' not in quarterly_data.columns:
        if 'OPERATING_PROFIT' in quarterly_data.columns and 'CCFI_2' in quarterly_data.columns:
            quarterly_data['EBITDA'] = quarterly_data['OPERATING_PROFIT'] + quarterly_data['CCFI_2']
        elif all(col in quarterly_data.columns for col in ['CIS_20', 'CIS_25', 'CIS_26', 'CCFI_2']):
            # Calculate operating profit first, then EBITDA
            quarterly_data['OPERATING_PROFIT'] = quarterly_data['CIS_20'] + quarterly_data['CIS_25'] + quarterly_data['CIS_26']
            quarterly_data['EBITDA'] = quarterly_data['OPERATING_PROFIT'] + quarterly_data['CCFI_2']

# Ensure EBITDA is calculated in annual_data
if not annual_data.empty:
    if 'EBITDA' not in annual_data.columns:
        if 'OPERATING_PROFIT' in annual_data.columns and 'CCFI_2' in annual_data.columns:
            annual_data['EBITDA'] = annual_data['OPERATING_PROFIT'] + annual_data['CCFI_2']
        elif all(col in annual_data.columns for col in ['CIS_20', 'CIS_25', 'CIS_26', 'CCFI_2']):
            # Calculate operating profit first, then EBITDA
            annual_data['OPERATING_PROFIT'] = annual_data['CIS_20'] + annual_data['CIS_25'] + annual_data['CIS_26']
            annual_data['EBITDA'] = annual_data['OPERATING_PROFIT'] + annual_data['CCFI_2']

# Main dashboard layout with updated tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Tổng quan", 
    "📈 Chỉ số tài chính", 
    "💰 Biên lợi nhuận",
    "📋 Dữ liệu chi tiết"
])

with tab1:
    st.header("📊 Tổng quan tài chính")
    
    # Key metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    
    # Get latest data
    if not quarterly_data.empty:
        latest_quarter = quarterly_data.iloc[-1]
        
        # Revenue
        if 'CIS_10' in quarterly_data.columns:
            with col1:
                revenue = latest_quarter.get('CIS_10', 0)
                st.metric(
                    "Doanh thu (Quý gần nhất)",
                    f"{revenue:,.0f} tỷ"
                )
        
        # Gross Profit
        if 'CIS_20' in quarterly_data.columns:
            with col2:
                gross_profit = latest_quarter.get('CIS_20', 0)
                st.metric(
                    "Lợi nhuận gộp",
                    f"{gross_profit:,.0f} tỷ"
                )
        
        # EBITDA
        if 'EBITDA' in quarterly_data.columns:
            with col3:
                ebitda = latest_quarter.get('EBITDA', 0)
                st.metric(
                    "EBITDA",
                    f"{ebitda:,.0f} tỷ"
                )
        elif 'OPERATING_PROFIT' in quarterly_data.columns and 'CCFI_2' in quarterly_data.columns:
            with col3:
                ebitda = latest_quarter.get('OPERATING_PROFIT', 0) + latest_quarter.get('CCFI_2', 0)
                st.metric(
                    "EBITDA",
                    f"{ebitda:,.0f} tỷ"
                )
        
        # Net Profit
        if 'CIS_61' in quarterly_data.columns:
            with col4:
                net_profit = latest_quarter.get('CIS_61', 0)
                st.metric(
                    "Lợi nhuận ròng",
                    f"{net_profit:,.0f} tỷ"
                )
        
        # SG&A
        if 'CIS_25' in quarterly_data.columns and 'CIS_26' in quarterly_data.columns:
            with col5:
                sga = abs(latest_quarter.get('CIS_25', 0)) + abs(latest_quarter.get('CIS_26', 0))
                st.metric(
                    "Chi phí SG&A",
                    f"{sga:,.0f} tỷ"
                )
    
    # Summary table for last 4 quarters
    st.subheader("📊 Tổng quan 4 quý gần nhất")
    if not quarterly_data.empty:
        summary_data = quarterly_data.tail(4).copy()
        summary_data['Period'] = summary_data['QUARTER'].astype(str) + 'Q' + summary_data['YEAR'].astype(str)
        
        # Select key columns
        display_cols = ['Period']
        col_mapping = {
            'CIS_10': 'Doanh thu',
            'CIS_20': 'LN gộp',
            'EBITDA': 'EBITDA',
            'CIS_61': 'LN ròng'
        }
        
        for old_col, new_col in col_mapping.items():
            if old_col in summary_data.columns:
                summary_data[new_col] = summary_data[old_col]
                display_cols.append(new_col)
        
        # Calculate SG&A if available
        if 'CIS_25' in summary_data.columns and 'CIS_26' in summary_data.columns:
            summary_data['Chi phí SG&A'] = abs(summary_data['CIS_25']) + abs(summary_data['CIS_26'])
            display_cols.append('Chi phí SG&A')
        
        st.dataframe(
            summary_data[display_cols].style.format({
                col: '{:,.0f}' for col in display_cols if col != 'Period'
            }),
            use_container_width=True,
            hide_index=True
        )

with tab2:
    is_quarterly = (period_type == "Theo Quý")
    
    if is_quarterly:
        st.header("📈 Các chỉ số tài chính theo Quý (2018 - Q2/2025)")
        display_data = quarterly_data
    else:
        st.header("📈 Các chỉ số tài chính theo Năm (2018 - 2025)")
        # Prepare annual data
        metrics_to_aggregate = ['CIS_10', 'CIS_20', 'CIS_61', 'CIS_25', 'CIS_26', 
                                'OPERATING_PROFIT', 'EBITDA', 'CCFI_2']
        display_data = prepare_annual_data(quarterly_data, metrics_to_aggregate)
        if 'OPERATING_PROFIT' in display_data.columns and 'CCFI_2' in display_data.columns:
            display_data['EBITDA'] = display_data['OPERATING_PROFIT'] + display_data['CCFI_2']
    
    if not display_data.empty:
        # Row 1: Revenue and Gross Profit
        col1, col2 = st.columns(2)
        
        with col1:
            if 'CIS_10' in display_data.columns:
                fig_revenue = create_metric_chart(display_data, 'CIS_10', 'Doanh thu thuần', 'blue', is_quarterly)
                st.plotly_chart(fig_revenue, use_container_width=True)
        
        with col2:
            if 'CIS_20' in display_data.columns:
                fig_gross = create_metric_chart(display_data, 'CIS_20', 'Lợi nhuận gộp', 'green', is_quarterly)
                st.plotly_chart(fig_gross, use_container_width=True)
        
        # Row 2: EBITDA and Net Profit
        col3, col4 = st.columns(2)
        
        with col3:
            # Always try to show EBITDA
            if 'EBITDA' in display_data.columns:
                fig_ebitda = create_metric_chart(display_data, 'EBITDA', 'EBITDA', 'purple', is_quarterly)
                st.plotly_chart(fig_ebitda, use_container_width=True)
            else:
                # Try to calculate EBITDA if not available
                ebitda_calculated = False
                
                # Method 1: OPERATING_PROFIT + CCFI_2
                if 'OPERATING_PROFIT' in display_data.columns and 'CCFI_2' in display_data.columns:
                    display_data['EBITDA'] = display_data['OPERATING_PROFIT'] + display_data['CCFI_2']
                    ebitda_calculated = True
                # Method 2: CIS_20 + CIS_25 + CIS_26 + CCFI_2
                elif all(col in display_data.columns for col in ['CIS_20', 'CIS_25', 'CIS_26', 'CCFI_2']):
                    display_data['EBITDA'] = display_data['CIS_20'] + display_data['CIS_25'] + display_data['CIS_26'] + display_data['CCFI_2']
                    ebitda_calculated = True
                
                if ebitda_calculated:
                    fig_ebitda = create_metric_chart(display_data, 'EBITDA', 'EBITDA', 'purple', is_quarterly)
                    st.plotly_chart(fig_ebitda, use_container_width=True)
                else:
                    st.info("EBITDA: Không đủ dữ liệu để tính toán")
        
        with col4:
            if 'CIS_61' in display_data.columns:
                fig_net = create_metric_chart(display_data, 'CIS_61', 'Lợi nhuận ròng', 'red', is_quarterly)
                st.plotly_chart(fig_net, use_container_width=True)
        
        # Row 3: SG&A (full width)
        if 'CIS_25' in display_data.columns and 'CIS_26' in display_data.columns:
            # Calculate SG&A
            display_data['SGA'] = abs(display_data['CIS_25']) + abs(display_data['CIS_26'])
            fig_sga = create_metric_chart(display_data, 'SGA', 'Chi phí bán hàng & quản lý (SG&A)', 'orange', is_quarterly)
            st.plotly_chart(fig_sga, use_container_width=True)
    else:
        st.info("Không có dữ liệu theo quý")

with tab3:
    is_quarterly = (period_type == "Theo Quý")
    
    if is_quarterly:
        st.header("💰 Biên lợi nhuận theo Quý (2018 - Q2/2025)")
        margin_data = quarterly_margins
    else:
        st.header("💰 Biên lợi nhuận theo Năm (2018 - 2025)")
        margin_data = annual_margins
    
    if not margin_data.empty:
        # Calculate EBITDA Margin if not available (for both quarterly and annual)
        if 'EBITDA_Margin' not in margin_data.columns:
            # For quarterly data
            if is_quarterly:
                # First ensure EBITDA exists in quarterly_data
                if 'EBITDA' not in quarterly_data.columns:
                    if 'OPERATING_PROFIT' in quarterly_data.columns and 'CCFI_2' in quarterly_data.columns:
                        quarterly_data['EBITDA'] = quarterly_data['OPERATING_PROFIT'] + quarterly_data['CCFI_2']
                
                # Now merge to calculate EBITDA_Margin
                if 'EBITDA' in quarterly_data.columns and 'CIS_10' in quarterly_data.columns:
                    # Get EBITDA and CIS_10 values
                    ebitda_data = quarterly_data[['YEAR', 'QUARTER', 'EBITDA', 'CIS_10']].copy()
                    
                    # Merge with margin_data
                    margin_data = margin_data.merge(
                        ebitda_data,
                        on=['YEAR', 'QUARTER'],
                        how='left',
                        suffixes=('', '_new')
                    )
                    
                    # Calculate EBITDA_Margin
                    if 'CIS_10_new' in margin_data.columns:
                        margin_data['EBITDA_Margin'] = np.where(
                            margin_data['CIS_10_new'] != 0,
                            (margin_data['EBITDA'] / margin_data['CIS_10_new']) * 100,
                            np.nan
                        )
                        # Clean up columns
                        margin_data = margin_data.drop(columns=['CIS_10_new'])
                    elif 'CIS_10' in margin_data.columns:
                        margin_data['EBITDA_Margin'] = np.where(
                            margin_data['CIS_10'] != 0,
                            (margin_data['EBITDA'] / margin_data['CIS_10']) * 100,
                            np.nan
                        )
            else:
                # For annual data
                if 'EBITDA' not in annual_data.columns:
                    if 'OPERATING_PROFIT' in annual_data.columns and 'CCFI_2' in annual_data.columns:
                        annual_data['EBITDA'] = annual_data['OPERATING_PROFIT'] + annual_data['CCFI_2']
                
                if 'EBITDA' in annual_data.columns and 'CIS_10' in annual_data.columns:
                    margin_data = margin_data.merge(
                        annual_data[['YEAR', 'EBITDA', 'CIS_10']],
                        on=['YEAR'],
                        how='left',
                        suffixes=('', '_new')
                    )
                    
                    if 'CIS_10_new' in margin_data.columns:
                        margin_data['EBITDA_Margin'] = np.where(
                            margin_data['CIS_10_new'] != 0,
                            (margin_data['EBITDA'] / margin_data['CIS_10_new']) * 100,
                            np.nan
                        )
                        margin_data = margin_data.drop(columns=['CIS_10_new'])
                    elif 'CIS_10' in margin_data.columns:
                        margin_data['EBITDA_Margin'] = np.where(
                            margin_data['CIS_10'] != 0,
                            (margin_data['EBITDA'] / margin_data['CIS_10']) * 100,
                            np.nan
                        )
        
        # Row 1: All margins in one chart
        margins_to_show = ['Gross_Margin', 'Operating_Margin', 'EBITDA_Margin', 'Net_Margin']
        fig_all_margins = create_margin_chart(
            margin_data, 
            margins_to_show,
            f"Tất cả Margins - {selected_ticker}",
            is_quarterly
        )
        st.plotly_chart(fig_all_margins, use_container_width=True)
        
        # Row 2: Individual margin charts
        col1, col2 = st.columns(2)
        
        with col1:
            if 'Gross_Margin' in margin_data.columns:
                fig_gross_margin = create_margin_chart(
                    margin_data,
                    ['Gross_Margin'],
                    f"Gross Profit Margin - {selected_ticker}",
                    is_quarterly
                )
                st.plotly_chart(fig_gross_margin, use_container_width=True)
        
        with col2:
            if 'Operating_Margin' in margin_data.columns:
                fig_op_margin = create_margin_chart(
                    margin_data,
                    ['Operating_Margin'],
                    f"Operating Margin - {selected_ticker}",
                    is_quarterly
                )
                st.plotly_chart(fig_op_margin, use_container_width=True)
        
        # Row 3: EBITDA and Net Margin
        col3, col4 = st.columns(2)
        
        with col3:
            if 'EBITDA_Margin' in margin_data.columns:
                fig_ebitda_margin = create_margin_chart(
                    margin_data,
                    ['EBITDA_Margin'],
                    f"EBITDA Margin - {selected_ticker}",
                    is_quarterly
                )
                st.plotly_chart(fig_ebitda_margin, use_container_width=True)
        
        with col4:
            if 'Net_Margin' in margin_data.columns:
                fig_net_margin = create_margin_chart(
                    margin_data,
                    ['Net_Margin'],
                    f"Net Profit Margin - {selected_ticker}",
                    is_quarterly
                )
                st.plotly_chart(fig_net_margin, use_container_width=True)
        
        # Latest margins summary
        period_label = "Quý gần nhất" if is_quarterly else "Năm gần nhất"
        st.subheader(f"📊 Margins hiện tại ({period_label})")
        latest_margins = margin_data.iloc[-1] if not margin_data.empty else {}
        
        margin_cols = st.columns(4)
        
        with margin_cols[0]:
            gross_margin = latest_margins.get('Gross_Margin', 0)
            st.metric("Gross Margin", f"{gross_margin:.2f}%")
        
        with margin_cols[1]:
            op_margin = latest_margins.get('Operating_Margin', 0)
            st.metric("Operating Margin", f"{op_margin:.2f}%")
        
        with margin_cols[2]:
            ebitda_margin = latest_margins.get('EBITDA_Margin', 0)
            st.metric("EBITDA Margin", f"{ebitda_margin:.2f}%")
        
        with margin_cols[3]:
            net_margin = latest_margins.get('Net_Margin', 0)
            st.metric("Net Margin", f"{net_margin:.2f}%")
    else:
        st.info("Không có dữ liệu margins")

with tab4:
    st.header("📋 Dữ liệu chi tiết")
    
    is_quarterly = (period_type == "Theo Quý")
    
    # Data type selection
    col1, col2 = st.columns([3, 1])
    
    with col1:
        if is_quarterly:
            data_type = st.selectbox(
                "Chọn loại dữ liệu:",
                ["Income Statement", "Balance Sheet", "Cash Flow", "Margins", "Financial Data"]
            )
        else:
            data_type = st.selectbox(
                "Chọn loại dữ liệu:",
                ["Income Statement", "Balance Sheet", "Cash Flow", "Margins", "Financial Data"]
            )
    
    # Prepare data based on selection
    if data_type in ["Income Statement", "Balance Sheet", "Cash Flow", "Financial Data"]:
        if is_quarterly:
            base_data = quarterly_data
        else:
            base_data = annual_data
    elif data_type == "Margins":
        if is_quarterly:
            base_data = quarterly_margins
        else:
            base_data = annual_margins
    else:
        base_data = pd.DataFrame()
    
    # Prepare display data
    if not base_data.empty:
        display_data = prepare_display_data(base_data, data_type, is_quarterly)
        
        if not display_data.empty:
            # Show data info
            st.info(f"📊 Hiển thị: {data_type} - {'Theo Quý' if is_quarterly else 'Theo Năm'} (từ 2018)")
            
            # Create formatted dataframe for display
            formatted_df = display_data.copy()
            
            # Get period columns (all columns except 'Chỉ tiêu')
            period_columns = [col for col in formatted_df.columns if col != 'Chỉ tiêu']
            
            # Format based on data type
            if data_type == "Margins":
                # Format margin columns as percentages
                for col in period_columns:
                    formatted_df[col] = formatted_df[col].apply(
                        lambda x: f'{x:.2f}%' if pd.notna(x) and isinstance(x, (int, float)) else ''
                    )
            else:
                # Format financial data with thousand separators
                for col in period_columns:
                    formatted_df[col] = formatted_df[col].apply(
                        lambda x: f'{x:,.0f}' if pd.notna(x) and isinstance(x, (int, float)) else ''
                    )
            
            # Create column config for better display
            column_config = {
                'Chỉ tiêu': st.column_config.TextColumn(
                    'Chỉ tiêu',
                    width='medium',
                    help='Tên chỉ tiêu tài chính'
                )
            }
            
            # Add config for period columns with smaller width
            for col in period_columns:
                column_config[col] = st.column_config.TextColumn(
                    col,
                    width='small'
                )
            
            # Display dataframe with custom styling
            st.dataframe(
                formatted_df,
                use_container_width=True,
                hide_index=True,
                height=500,
                column_config=column_config
            )
            
            # Summary statistics
            if data_type != "Margins":
                st.subheader("📊 Thống kê tóm tắt")
                
                # Select key metrics for summary
                if data_type == "Income Statement" or data_type == "Financial Data":
                    summary_metrics = ['Doanh thu thuần', 'Lợi nhuận gộp', 'EBITDA', 'Lợi nhuận ròng']
                elif data_type == "Balance Sheet":
                    summary_metrics = ['Tổng tài sản', 'Tổng nợ', 'Vốn chủ sở hữu']
                elif data_type == "Cash Flow":
                    summary_metrics = ['Dòng tiền HĐKD', 'Lưu chuyển tiền thuần']
                else:
                    summary_metrics = []
                
                # Filter to get only the metrics we want
                summary_data = display_data[display_data['Chỉ tiêu'].isin(summary_metrics)]
                
                if not summary_data.empty:
                    # Calculate statistics for each metric
                    stats_list = []
                    for _, row in summary_data.iterrows():
                        metric_name = row['Chỉ tiêu']
                        # Get numeric values from period columns
                        values = []
                        for col in period_columns:
                            val = row[col]
                            if pd.notna(val) and isinstance(val, (int, float)):
                                values.append(val)
                        
                        if values:
                            stats_list.append({
                                'Chỉ tiêu': metric_name,
                                'Trung bình': np.mean(values),
                                'Tối thiểu': np.min(values),
                                'Tối đa': np.max(values),
                                'Độ lệch chuẩn': np.std(values)
                            })
                    
                    if stats_list:
                        stats_df = pd.DataFrame(stats_list)
                        # Format numbers
                        for col in ['Trung bình', 'Tối thiểu', 'Tối đa', 'Độ lệch chuẩn']:
                            stats_df[col] = stats_df[col].apply(lambda x: f'{x:,.0f}')
                        
                        st.dataframe(stats_df, use_container_width=True, hide_index=True)
            
            # Download button
            csv = display_data.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"{selected_ticker}_{data_type.lower().replace(' ', '_')}_{period_type.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.warning(f"Không có dữ liệu {data_type} để hiển thị")
    else:
        st.info(f"Không có dữ liệu cho {selected_ticker}")

# Footer
st.markdown("---")
st.caption(f"Dữ liệu được cập nhật từ Database/Buu_clean_ver2.parquet | {datetime.now().strftime('%Y-%m-%d %H:%M')}")