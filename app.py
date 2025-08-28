"""
VN Finance Dashboard - Main Application Entry Point
Multi-page Streamlit application for Vietnamese stock market analysis
"""

import streamlit as st
import os
import sys

# Add src to path for module imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Page configuration
st.set_page_config(
    page_title="VN Finance Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': None,
        'Report a bug': None,
        'About': """
        ## VN Finance Dashboard
        
        Ứng dụng phân tích thị trường chứng khoán Việt Nam
        
        **Tính năng chính:**
        - 📈 Phân tích kỹ thuật và cơ bản
        - 💰 Định giá và so sánh cổ phiếu  
        - 📊 Biểu đồ tương tác
        - 🔍 Bộ lọc và tìm kiếm thông minh
        
        **Nguồn dữ liệu:** vnstock3, TCBS, SSI
        """
    }
)

def main():
    """Main application function"""
    
    # Main page header
    st.title("🏠 VN Finance Dashboard")
    st.markdown("### Ứng dụng phân tích thị trường chứng khoán Việt Nam")
    
    # Welcome message
    st.markdown("""
    Chào mừng bạn đến với **VN Finance Dashboard** - Công cụ phân tích chứng khoán toàn diện cho nhà đầu tư Việt Nam.
    
    ### 📱 Các tính năng chính:
    """)
    
    # Feature cards in columns
    col1, col2 = st.columns(2)
    
    with col1:
        st.info("""
        **📈 Company Dashboard**
        - Phân tích chi tiết từng mã cổ phiếu
        - Chỉ số tài chính và biên lợi nhuận
        - Biểu đồ tăng trưởng theo thời gian
        - So sánh với trung bình ngành
        """)
        
        st.success("""
        **📊 Market Overview**
        - Tổng quan thị trường
        - Top movers và volume leaders
        - Heatmap các ngành
        - Chỉ số market breadth
        """)
    
    with col2:
        st.warning("""
        **🔍 Stock Screener**
        - Lọc cổ phiếu theo tiêu chí
        - Phân tích đa chiều
        - So sánh nhiều mã
        - Xuất báo cáo
        """)
        
        st.error("""
        **⚙️ Settings**
        - Cấu hình nguồn dữ liệu
        - Tùy chỉnh giao diện
        - Quản lý danh mục
        - Cài đặt cảnh báo
        """)
    
    # Navigation guide
    st.markdown("---")
    st.markdown("""
    ### 🧭 Hướng dẫn sử dụng:
    
    👈 Sử dụng **menu bên trái** để điều hướng giữa các trang:
    
    1. **Company Dashboard**: Phân tích chi tiết từng mã cổ phiếu
    2. **Market Overview**: Tổng quan thị trường và các chỉ số
    3. **Stock Screener**: Bộ lọc và tìm kiếm cổ phiếu
    4. **Settings**: Cấu hình ứng dụng
    
    ### 📊 Dữ liệu:
    - **Nguồn**: vnstock3, TCBS, SSI APIs
    - **Cập nhật**: Real-time cho giá, EOD cho báo cáo tài chính
    - **Lịch sử**: 5 năm dữ liệu tài chính, 1 năm dữ liệu giá
    """)
    
    # Quick stats
    st.markdown("---")
    st.markdown("### 📈 Thống kê nhanh")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Số mã theo dõi",
            value="1,700+",
            delta="Toàn thị trường"
        )
    
    with col2:
        st.metric(
            label="Dữ liệu lịch sử",
            value="5 năm",
            delta="2019-2024"
        )
    
    with col3:
        st.metric(
            label="Chỉ số phân tích",
            value="50+",
            delta="Fundamental & Technical"
        )
    
    with col4:
        st.metric(
            label="Cập nhật",
            value="Real-time",
            delta="Giá & khối lượng"
        )
    
    # Footer
    st.markdown("---")
    st.caption("© 2024 VN Finance Dashboard | Powered by Streamlit & vnstock3")

if __name__ == "__main__":
    main()