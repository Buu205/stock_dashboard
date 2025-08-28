# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Running the Application
```bash
# Main dashboard application
streamlit run app.py

# Individual pages (multi-page Streamlit app)
streamlit run pages/1_📈_Company_Dashboard.py
```

### Testing
```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov

# Run specific test module
pytest tests/test_analysis/test_fundamental.py
```

### Code Quality
```bash
# Format code with Black
black src/ tests/

# Lint with Ruff
ruff check src/ tests/

# Auto-fix linting issues
ruff check --fix src/ tests/
```

### Dependencies
```bash
# Install all dependencies
pip install -r requirements.txt

# Install development dependencies
pip install pytest pytest-cov ruff black
```

## Architecture Overview

### Multi-Layer Architecture
The codebase follows a modular, layered architecture optimized for financial data analysis:

1. **Data Layer** (`src/data/`)
   - **Connectors**: External API integrations (vnstock3, SSI, TCBS)
   - **Loaders**: Data loading and transformation from Parquet/Excel files
   - **Models**: Pydantic models for data validation

2. **Analysis Engine** (`src/analysis/`)
   - **Fundamental Analysis**: Financial metrics, ratios, margins
   - **Technical Analysis**: Price indicators, MA, RSI, MACD
   - **Integrated Analysis**: Combined fundamental + technical signals

3. **Core Services** (`src/core/`)
   - **Config**: YAML-based configuration management with environment override
   - **DataManager**: Central data orchestration and caching
   - **Exceptions**: Custom exception hierarchy

4. **Visualization** (`src/visualization/`)
   - **Dashboard Components**: Pre-built Streamlit components
   - **Chart Builders**: Plotly-based interactive charts
   - **Comparison Charts**: Multi-ticker comparison visualizations

### Key Data Flow
1. **Data Sources**: 
   - Primary: `Database/Full_database/Buu_clean_ver2.parquet` (historical financial data)
   - Metadata: `Database/Full_database/CSDL.xlsx` (metric definitions)
   - Cache: `Database/cache/` (SSI/vnstock API responses)

2. **Configuration**: 
   - `config.yaml` defines all paths, API settings, and metric mappings
   - Uses Vietnamese Accounting Standards (VAS) codes (e.g., CIS_20 for revenue)

3. **Multi-Page App Structure**:
   - `app.py`: Main entry point
   - `pages/`: Individual dashboard pages (Company, Market, Screener, Settings)

### Important Patterns
- **Metric Mapping**: All financial metrics use VAS codes mapped in config.yaml
- **Caching Strategy**: TTL-based caching for API calls (default 1 hour)
- **Error Handling**: Custom exceptions in `src/core/exceptions.py`
- **Data Validation**: Pydantic models for all data structures

## Working with Financial Data

### Metric Codes
The system uses Vietnamese Accounting Standards codes:
- Income Statement: `CIS_*` (e.g., CIS_20=Revenue, CIS_30=Net Profit)
- Balance Sheet: `CBS_*` (e.g., CBS_10=Total Assets, CBS_30=Equity)

### Data Processing
- Parquet files are the primary data source for performance
- Excel metadata provides human-readable metric descriptions
- All data processing respects the `default_lookback_years` config (default: 5 years)