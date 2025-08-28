import sys
from pathlib import Path
import warnings

warnings.filterwarnings("ignore")

# Đảm bảo src/ có trong sys.path để import module local
current_dir = Path(__file__).parent.resolve()
src_path = (current_dir / "../../src").resolve()
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

try:
    from data.connectors.tcbs_connector import TCBSConnector
except ImportError as e:
    raise ImportError(
        f"Không thể import TCBSConnector. Kiểm tra lại đường dẫn src/data/connectors/tcbs_connector.py. Lỗi: {e}"
    )

def test_tcbs_connector():
    """Test TCBSConnector fetch + indicators + summary"""
    connector = TCBSConnector()
    ticker = "MWG"
    days_hist = 30
    days_summary = 365

    # Test fetch_historical_price
    try:
        df = connector.fetch_historical_price(ticker, days=days_hist)
        assert not df.empty, "Dữ liệu trả về rỗng"
        print(f"✓ Fetched historical price data ({days_hist} days):")
        print(df.head(5))
    except Exception as e:
        print(f"❌ Lỗi khi fetch_historical_price: {e}")
        return

    # Test calculate_technical_indicators
    try:
        df_with_ind = connector.calculate_technical_indicators(df)
        assert "MA_20" in df_with_ind.columns, "Thiếu cột MA_20"
        print("\n✓ Data with technical indicators:")
        print(df_with_ind.head(5))
    except Exception as e:
        print(f"❌ Lỗi khi calculate_technical_indicators: {e}")
        return

    # Test get_price_summary
    try:
        summary = connector.get_price_summary(ticker, days=days_summary)
        assert summary is not None, "Summary trả về None"
        print(f"\n✓ Price summary ({days_summary} days):")
        print(summary)
    except Exception as e:
        print(f"❌ Lỗi khi get_price_summary: {e}")
        return

if __name__ == "__main__":
    test_tcbs_connector()