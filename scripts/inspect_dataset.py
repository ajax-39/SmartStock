"""
Inspect the downloaded Kaggle dataset to discover its structure.

This script examines the raw data files to determine:
- File formats and structure
- Column schema
- Number of stocks/symbols
- Date range coverage
- Data quality issues
- Technical indicators present (if any)
"""

import os
import sys
from pathlib import Path
import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Any

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config_loader import get_config


def get_file_info(file_path: Path) -> Dict[str, Any]:
    """Get basic information about a file."""
    stat = file_path.stat()
    return {
        'name': file_path.name,
        'size_mb': stat.st_size / (1024 * 1024),
        'extension': file_path.suffix,
        'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
    }


def inspect_csv_file(file_path: Path, sample_size: int = 1000) -> Dict[str, Any]:
    """
    Inspect a CSV file and extract schema information.
    
    Args:
        file_path: Path to CSV file
        sample_size: Number of rows to sample for analysis
        
    Returns:
        Dictionary with file analysis
    """
    print(f"\n  Analyzing: {file_path.name}")
    
    try:
        # Read a sample to infer schema quickly
        sample_df = pd.read_csv(file_path, nrows=sample_size)
        
        # Get full row count (more efficient than loading entire file)
        with open(file_path, 'r', encoding='utf-8') as f:
            row_count = sum(1 for line in f) - 1  # Subtract header
        
        # Analyze columns
        columns_info = {}
        for col in sample_df.columns:
            col_data = sample_df[col]
            columns_info[col] = {
                'dtype': str(col_data.dtype),
                'non_null': int(col_data.notna().sum()),
                'null_count': int(col_data.isna().sum()),
                'unique_values': int(col_data.nunique()),
                'sample_values': col_data.head(3).tolist()
            }
        
        # Try to identify date columns
        date_columns = []
        for col in sample_df.columns:
            if any(keyword in col.lower() for keyword in ['date', 'time', 'datetime', 'timestamp']):
                date_columns.append(col)
        
        # Try to identify symbol/stock column
        symbol_columns = []
        for col in sample_df.columns:
            if any(keyword in col.lower() for keyword in ['symbol', 'stock', 'ticker', 'code']):
                symbol_columns.append(col)
        
        analysis = {
            'rows_total': row_count,
            'rows_sampled': len(sample_df),
            'columns': list(sample_df.columns),
            'column_count': len(sample_df.columns),
            'columns_info': columns_info,
            'date_columns': date_columns,
            'symbol_columns': symbol_columns,
            'memory_mb': sample_df.memory_usage(deep=True).sum() / (1024 * 1024),
            'sample_head': sample_df.head(3).to_dict('records')
        }
        
        # If symbol column found, get unique symbols
        if symbol_columns:
            symbol_col = symbol_columns[0]
            # Read full column to get all unique symbols
            symbols_df = pd.read_csv(file_path, usecols=[symbol_col])
            unique_symbols = symbols_df[symbol_col].unique().tolist()
            analysis['unique_symbols'] = sorted(unique_symbols)
            analysis['symbol_count'] = len(unique_symbols)
        
        # If date column found, get date range
        if date_columns:
            date_col = date_columns[0]
            dates_df = pd.read_csv(file_path, usecols=[date_col])
            try:
                dates_df[date_col] = pd.to_datetime(dates_df[date_col])
                analysis['date_range'] = {
                    'min': dates_df[date_col].min().strftime('%Y-%m-%d %H:%M:%S'),
                    'max': dates_df[date_col].max().strftime('%Y-%m-%d %H:%M:%S')
                }
            except:
                analysis['date_range'] = 'Could not parse dates'
        
        return analysis
        
    except Exception as e:
        return {'error': str(e)}


def inspect_dataset():
    """Main function to inspect the downloaded dataset."""
    
    config = get_config()
    raw_data_path = Path(config.get('paths.raw_data'))
    
    print(f"{'='*80}")
    print(f"SmartStock - Dataset Inspection")
    print(f"{'='*80}")
    print(f"\nInspecting: {raw_data_path}")
    
    if not raw_data_path.exists():
        print(f"\n✗ Error: Data directory not found: {raw_data_path}")
        print(f"  Please run: python scripts/download_kaggle_data.py")
        return False
    
    # Find all data files
    data_files = []
    for ext in ['*.csv', '*.parquet', '*.json']:
        data_files.extend(raw_data_path.rglob(ext))
    
    if not data_files:
        print(f"\n✗ Error: No data files found in {raw_data_path}")
        return False
    
    print(f"\nFound {len(data_files)} data file(s)")
    print(f"\n{'='*80}")
    
    # Inspect each file
    results = {}
    
    for data_file in data_files:
        print(f"\n[File {len(results) + 1}/{len(data_files)}]")
        
        # Get basic file info
        file_info = get_file_info(data_file)
        print(f"  Name: {file_info['name']}")
        print(f"  Size: {file_info['size_mb']:.2f} MB")
        print(f"  Modified: {file_info['modified']}")
        
        # Detailed inspection based on file type
        if data_file.suffix.lower() == '.csv':
            analysis = inspect_csv_file(data_file)
            results[data_file.name] = {**file_info, **analysis}
        else:
            results[data_file.name] = file_info
            print(f"  (Detailed inspection not implemented for {data_file.suffix} files)")
    
    # Save inspection results
    output_dir = project_root / "data" / "cache"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "dataset_inspection.json"
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n{'='*80}")
    print(f"Inspection Summary")
    print(f"{'='*80}")
    
    # Print summary for each file
    for filename, info in results.items():
        print(f"\n📁 {filename}")
        
        if 'error' in info:
            print(f"   ✗ Error: {info['error']}")
            continue
        
        if 'columns' in info:
            print(f"   Rows: {info['rows_total']:,}")
            print(f"   Columns: {info['column_count']}")
            print(f"   Column names: {', '.join(info['columns'])}")
            
            if 'symbol_count' in info:
                print(f"   Unique stocks: {info['symbol_count']}")
                print(f"   Stock symbols: {', '.join(info['unique_symbols'][:10])}" + 
                      (f" ... (+{info['symbol_count'] - 10} more)" if info['symbol_count'] > 10 else ""))
            
            if 'date_range' in info and isinstance(info['date_range'], dict):
                print(f"   Date range: {info['date_range']['min']} to {info['date_range']['max']}")
            
            # Identify potential technical indicators
            tech_indicators = []
            for col in info['columns']:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in 
                       ['rsi', 'macd', 'ema', 'sma', 'bollinger', 'atr', 'adx', 'obv']):
                    tech_indicators.append(col)
            
            if tech_indicators:
                print(f"   Technical indicators present: {', '.join(tech_indicators)}")
            
            print(f"\n   Sample data (first row):")
            if 'sample_head' in info and info['sample_head']:
                for key, value in list(info['sample_head'][0].items())[:5]:
                    print(f"     {key}: {value}")
    
    print(f"\n{'='*80}")
    print(f"✓ Inspection complete!")
    print(f"\nDetailed results saved to: {output_file}")
    print(f"\nNext steps:")
    print(f"  1. Review the discovered schema above")
    print(f"  2. Update config/config.yaml with test stocks if needed")
    print(f"  3. Proceed to data cleaning and transformation phase")
    print(f"{'='*80}")
    
    return True


if __name__ == "__main__":
    success = inspect_dataset()
    sys.exit(0 if success else 1)
