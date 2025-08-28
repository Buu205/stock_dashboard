from vnstock_ta import DataSource, Indicator, Plotter

# Lấy dữ liệu giá từ DataSource
data = DataSource(symbol='MWG',
                  start='2023-01-02', end=None,
                  interval='1D', source='VCI').get_data()

print(data)