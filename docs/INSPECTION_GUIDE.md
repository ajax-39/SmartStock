# Dataset Inspection Guide

## Overview

The dataset inspection tool (`src/ingestion/inspect_dataset.py`) provides comprehensive analysis of stock market datasets without requiring PySpark or cloud resources. It discovers schema, validates data quality, and generates detailed reports.

## Features

✅ **Automatic Discovery**
- Recursively finds CSV and Parquet files
- Identifies timestamp, symbol, OHLCV columns by pattern matching
- Detects technical indicators automatically

✅ **Data Quality Analysis**
- Missing values and null percentages
- Duplicate record detection
- Invalid price validation (OHLC relationships)
- Statistical summaries for numeric columns

✅ **Comprehensive Reporting**
- Human-readable console output
- Machine-readable JSON report
- File structure analysis (combined vs. per-stock)
- Date range coverage
- Stock symbol enumeration

## Usage

### Option 1: Use Default Path (from config)

After running the download script:

```powershell
python src/ingestion/inspect_dataset.py
```

This uses `data/raw` from `config/config.yaml`.

### Option 2: Inspect Kagglehub Cache Directly

First, find where kagglehub downloaded the data:

```powershell
python scripts/find_kagglehub_cache.py
```

This will output the cache path. Then inspect it:

```powershell
# Windows example
python src/ingestion/inspect_dataset.py --path "C:\Users\YourName\.cache\kagglehub\datasets\debashis74017\stock-market-data-nifty-50-stocks-1-min-data\versions\1"

# Or use the path from find_kagglehub_cache.py output
```

### Option 3: Inspect Custom Directory

```powershell
python src/ingestion/inspect_dataset.py --path Z:\data\stocks
```

### Option 4: Save Report to Custom Location

```powershell
python src/ingestion/inspect_dataset.py --output my_report.json
```

## Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--path` | Path to dataset directory or file | From config (`data/raw`) |
| `--output` | Path to save JSON report | `data/cache/dataset_inspection_detailed.json` |

## Output

### Console Output

The tool prints a comprehensive summary including:

1. **File Discovery**: Lists all found data files with sizes
2. **Analysis Progress**: Shows progress for each file
3. **Detailed Summary**: 
   - Dataset overview (files, rows, size)
   - Stock symbols discovered
   - Per-file details:
     - Identified columns (timestamp, symbol, OHLCV)
     - Date ranges
     - Data quality metrics
     - Technical indicators found

### JSON Report

A detailed JSON file is saved containing:

```json
{
  "inspection_time": "2024-10-11T14:30:00",
  "dataset_path": "/path/to/data",
  "files": {
    "stock_data.csv": {
      "file_type": "csv",
      "rows_total": 1000000,
      "columns": ["DateTime", "Symbol", "Open", ...],
      "identified_columns": {
        "timestamp": ["DateTime"],
        "symbol": ["Symbol"],
        "open": ["Open"],
        ...
      },
      "timestamp_analysis": {
        "min": "2020-01-01T09:15:00",
        "max": "2024-12-31T15:30:00"
      },
      "symbol_analysis": {
        "unique_count": 50,
        "symbols": ["RELIANCE", "TCS", ...]
      },
      "ohlcv_completeness": {
        "open": true,
        "high": true,
        "low": true,
        "close": true,
        "volume": true
      },
      "price_validation": {
        "invalid_count": 42,
        "invalid_percentage": 0.0042
      },
      "duplicates": {
        "count": 15,
        "percentage": 0.0015
      },
      "technical_indicator_count": 55,
      "technical_indicator_types": ["RSI", "MACD", "Moving Average", ...]
    }
  },
  "summary": {
    "total_files": 1,
    "total_rows": 1000000,
    "total_size_mb": 250.5,
    "total_symbols": 50,
    "all_symbols": ["RELIANCE", "TCS", ...],
    "file_structure": "combined",
    "has_technical_indicators": true,
    "technical_indicator_count": 55
  }
}
```

## Column Identification

The tool identifies columns using pattern matching:

### Standard Columns

| Column Type | Patterns Matched |
|-------------|------------------|
| **Timestamp** | date, time, datetime, timestamp, dt |
| **Symbol** | symbol, stock, ticker, code, name |
| **Open** | open, opening |
| **High** | high, highest |
| **Low** | low, lowest |
| **Close** | close, closing |
| **Volume** | volume, vol, quantity, qty |

### Technical Indicators

The tool recognizes 14 indicator categories:

- RSI (Relative Strength Index)
- MACD (Moving Average Convergence Divergence)
- Moving Average (SMA, EMA)
- Bollinger Bands
- ATR (Average True Range)
- ADX (Average Directional Index)
- Stochastic Oscillator
- OBV (On-Balance Volume)
- CCI (Commodity Channel Index)
- Williams %R
- ROC (Rate of Change)
- Momentum
- Volatility indicators
- Support/Resistance levels

## Data Quality Checks

### 1. Missing Values
- Counts null/NaN values per column
- Calculates null percentage
- Reports in both console and JSON

### 2. Duplicate Records
- Detects exact duplicate rows
- Reports count and percentage

### 3. Price Validation
For files with complete OHLCV data, validates:
- High ≥ Low
- High ≥ Open
- High ≥ Close
- Low ≤ Open
- Low ≤ Close
- All prices > 0

### 4. Timestamp Validation
- Parses datetime columns
- Reports min/max dates
- Counts invalid/unparseable timestamps

## File Structure Detection

The tool determines if the dataset is:

- **Combined**: All stocks in one file (multiple unique symbols)
- **Separate per stock**: Each file contains one stock (single symbol per file)

## Examples

### Example 1: Quick Inspection

```powershell
# Find the kagglehub cache
python scripts/find_kagglehub_cache.py

# Inspect it
python src/ingestion/inspect_dataset.py --path "<path from above>"
```

### Example 2: Inspect After Download

```powershell
# Download to project
python scripts/download_kaggle_data.py

# Inspect
python src/ingestion/inspect_dataset.py
```

### Example 3: Custom Output Location

```powershell
python src/ingestion/inspect_dataset.py --path data/raw --output reports/inspection_$(date +%Y%m%d).json
```

## Interpreting Results

### Stock Selection

Use the symbol list to select test stocks:

```yaml
# In config/config.yaml
data:
  test_stocks: ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"]
```

Choose stocks that:
- Have sufficient data points
- Cover the full date range
- Represent different sectors

### Date Range Configuration

Based on discovered date range, set train/test split:

```yaml
data:
  train_end_date: "2023-12-31"  # Train on data up to this date
  test_start_date: "2024-01-01"  # Test on data from this date
```

### Handling Quality Issues

**High null percentage (>5%)**:
- Consider dropping columns with excessive nulls
- Or implement imputation strategy

**Many duplicates (>1%)**:
- Plan to deduplicate in preprocessing
- Investigate cause (data collection issue?)

**Invalid prices**:
- Mark for filtering in cleaning phase
- Could indicate data quality problems

## Performance

- **Small datasets (<100MB)**: Processes in seconds
- **Medium datasets (100MB-1GB)**: Processes in 1-5 minutes
- **Large datasets (>1GB)**: May take 10-30 minutes for full scan

The tool samples data intelligently to provide quick results even for large files.

## Troubleshooting

### Error: Path does not exist

```
Solution: Verify the path is correct. Use find_kagglehub_cache.py to locate downloaded data.
```

### Error: No supported data files found

```
Solution: Ensure directory contains CSV or Parquet files. Check file extensions.
```

### Error: Cannot parse CSV

```
Solution: File may be corrupted or have encoding issues. Try opening in a text editor to verify format.
```

### Memory Issues

```
Solution: The tool samples large files. If memory errors persist:
1. Process files individually: python src/ingestion/inspect_dataset.py --path path/to/file.csv
2. Reduce sample size in code (sample_size parameter)
```

## Next Steps After Inspection

1. ✅ Review JSON report in detail
2. ✅ Note the discovered schema (columns, types)
3. ✅ Identify required cleaning steps
4. ✅ Select test stocks from symbol list
5. ✅ Configure date ranges in config.yaml
6. ✅ Plan feature engineering based on existing indicators
7. ➡️ Proceed to Phase 2: Data Cleaning and Transformation

## Testing

Run the test suite:

```powershell
pytest tests/test_dataset_inspector.py -v
```

Tests cover:
- Path validation
- Column identification
- File discovery
- Error handling
- Sample data inspection

## Additional Resources

- See `PHASE1_COMPLETE.md` for overall project status
- See `ARCHITECTURE.md` for system design
- See config/config.yaml for configuration options
