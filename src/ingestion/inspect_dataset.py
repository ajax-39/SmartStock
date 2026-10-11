"""
Comprehensive dataset inspection utility for SmartStock.

This script inspects stock market data downloaded from Kaggle (or any directory)
and produces detailed reports about structure, quality, and content.

Usage:
    # Use default path from config
    python src/ingestion/inspect_dataset.py
    
    # Specify custom path
    python src/ingestion/inspect_dataset.py --path /path/to/dataset
    
    # Specify kagglehub cache path
    python src/ingestion/inspect_dataset.py --path "C:/Users/Username/.cache/kagglehub/datasets/..."
    
    # Save report to custom location
    python src/ingestion/inspect_dataset.py --output custom_report.json
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pandas as pd
import numpy as np

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.config_loader import get_config


class DatasetInspector:
    """Inspect and analyze stock market datasets."""
    
    # Known column name patterns for identification
    TIMESTAMP_PATTERNS = ['date', 'time', 'datetime', 'timestamp', 'dt']
    SYMBOL_PATTERNS = ['symbol', 'stock', 'ticker', 'code', 'name']
    
    # OHLCV patterns
    OPEN_PATTERNS = ['open', 'opening']
    HIGH_PATTERNS = ['high', 'highest']
    LOW_PATTERNS = ['low', 'lowest']
    CLOSE_PATTERNS = ['close', 'closing']
    VOLUME_PATTERNS = ['volume', 'vol', 'quantity', 'qty']
    
    # Technical indicator patterns
    INDICATOR_PATTERNS = {
        'RSI': ['rsi'],
        'MACD': ['macd'],
        'Moving Average': ['sma', 'ema', 'ma', 'moving', 'average'],
        'Bollinger': ['bollinger', 'bb'],
        'ATR': ['atr', 'average_true_range'],
        'ADX': ['adx', 'directional'],
        'Stochastic': ['stochastic', 'stoch'],
        'OBV': ['obv', 'on_balance'],
        'CCI': ['cci', 'commodity'],
        'Williams': ['williams', 'willr'],
        'ROC': ['roc', 'rate_of_change'],
        'Momentum': ['momentum', 'mom'],
        'Volatility': ['volatility', 'vol_'],
        'Support/Resistance': ['support', 'resistance', 'pivot']
    }
    
    def __init__(self, dataset_path: Path, output_path: Optional[Path] = None):
        """
        Initialize dataset inspector.
        
        Args:
            dataset_path: Path to dataset directory or file
            output_path: Path to save JSON report
        """
        self.dataset_path = Path(dataset_path)
        self.output_path = output_path
        self.results = {
            'inspection_time': datetime.now().isoformat(),
            'dataset_path': str(self.dataset_path),
            'files': {},
            'summary': {}
        }
    
    def _match_column_pattern(self, column: str, patterns: List[str]) -> bool:
        """Check if column name matches any pattern."""
        col_lower = column.lower().strip()
        return any(pattern in col_lower for pattern in patterns)
    
    def _identify_column_types(self, columns: List[str]) -> Dict[str, List[str]]:
        """Identify column types based on naming patterns."""
        identified = {
            'timestamp': [],
            'symbol': [],
            'open': [],
            'high': [],
            'low': [],
            'close': [],
            'volume': [],
            'technical_indicators': {},
            'unknown': []
        }
        
        for col in columns:
            col_identified = False
            
            # Check for timestamp
            if self._match_column_pattern(col, self.TIMESTAMP_PATTERNS):
                identified['timestamp'].append(col)
                col_identified = True
            
            # Check for symbol
            if self._match_column_pattern(col, self.SYMBOL_PATTERNS):
                identified['symbol'].append(col)
                col_identified = True
            
            # Check for OHLCV
            if self._match_column_pattern(col, self.OPEN_PATTERNS):
                identified['open'].append(col)
                col_identified = True
            elif self._match_column_pattern(col, self.HIGH_PATTERNS):
                identified['high'].append(col)
                col_identified = True
            elif self._match_column_pattern(col, self.LOW_PATTERNS):
                identified['low'].append(col)
                col_identified = True
            elif self._match_column_pattern(col, self.CLOSE_PATTERNS):
                identified['close'].append(col)
                col_identified = True
            elif self._match_column_pattern(col, self.VOLUME_PATTERNS):
                identified['volume'].append(col)
                col_identified = True
            
            # Check for technical indicators
            for indicator_name, patterns in self.INDICATOR_PATTERNS.items():
                if self._match_column_pattern(col, patterns):
                    if indicator_name not in identified['technical_indicators']:
                        identified['technical_indicators'][indicator_name] = []
                    identified['technical_indicators'][indicator_name].append(col)
                    col_identified = True
            
            # Mark as unknown if not identified
            if not col_identified:
                identified['unknown'].append(col)
        
        return identified
    
    def _discover_files(self) -> List[Path]:
        """Discover all supported data files in the dataset path."""
        supported_extensions = {'.csv', '.parquet', '.pq', '.snappy'}
        discovered_files = []
        
        if self.dataset_path.is_file():
            if self.dataset_path.suffix.lower() in supported_extensions:
                discovered_files.append(self.dataset_path)
        elif self.dataset_path.is_dir():
            for ext in supported_extensions:
                discovered_files.extend(self.dataset_path.rglob(f'*{ext}'))
        
        return sorted(discovered_files)
    
    def _inspect_csv_file(self, file_path: Path, sample_size: int = 10000) -> Dict[str, Any]:
        """
        Inspect a CSV file comprehensively.
        
        Args:
            file_path: Path to CSV file
            sample_size: Number of rows to sample for detailed analysis
        """
        analysis = {
            'file_type': 'csv',
            'file_size_mb': file_path.stat().st_size / (1024 * 1024),
            'file_size_bytes': file_path.stat().st_size,
        }
        
        try:
            # First pass: get basic structure
            sample_df = pd.read_csv(file_path, nrows=100)
            columns = sample_df.columns.tolist()
            
            # Count total rows efficiently
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                row_count = sum(1 for _ in f) - 1  # Subtract header
            
            analysis['rows_total'] = row_count
            analysis['columns'] = columns
            analysis['column_count'] = len(columns)
            
            # Identify column types
            identified_cols = self._identify_column_types(columns)
            analysis['identified_columns'] = identified_cols
            
            # Read a larger sample for detailed analysis
            read_rows = min(sample_size, row_count)
            df = pd.read_csv(file_path, nrows=read_rows)
            
            # Column-level analysis
            columns_info = {}
            for col in df.columns:
                col_data = df[col]
                columns_info[col] = {
                    'dtype': str(col_data.dtype),
                    'non_null_count': int(col_data.notna().sum()),
                    'null_count': int(col_data.isna().sum()),
                    'null_percentage': float(col_data.isna().sum() / len(col_data) * 100),
                    'unique_values': int(col_data.nunique()),
                    'sample_values': col_data.dropna().head(3).tolist() if col_data.notna().any() else []
                }
                
                # For numeric columns, add statistics
                if pd.api.types.is_numeric_dtype(col_data):
                    numeric_data = col_data.dropna()
                    if len(numeric_data) > 0:
                        columns_info[col]['statistics'] = {
                            'min': float(numeric_data.min()),
                            'max': float(numeric_data.max()),
                            'mean': float(numeric_data.mean()),
                            'median': float(numeric_data.median()),
                            'std': float(numeric_data.std()) if len(numeric_data) > 1 else 0.0
                        }
            
            analysis['columns_info'] = columns_info
            
            # Analyze timestamps
            if identified_cols['timestamp']:
                timestamp_col = identified_cols['timestamp'][0]
                try:
                    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors='coerce')
                    valid_timestamps = df[timestamp_col].dropna()
                    if len(valid_timestamps) > 0:
                        analysis['timestamp_analysis'] = {
                            'column': timestamp_col,
                            'min': valid_timestamps.min().isoformat(),
                            'max': valid_timestamps.max().isoformat(),
                            'invalid_count': int(df[timestamp_col].isna().sum())
                        }
                except Exception as e:
                    analysis['timestamp_analysis'] = {'error': str(e)}
            
            # Analyze symbols
            if identified_cols['symbol']:
                symbol_col = identified_cols['symbol'][0]
                unique_symbols = df[symbol_col].dropna().unique().tolist()
                analysis['symbol_analysis'] = {
                    'column': symbol_col,
                    'unique_count': len(unique_symbols),
                    'symbols': sorted([str(s) for s in unique_symbols])
                }
                
                # Check if separate files per stock or combined
                if len(unique_symbols) > 1:
                    analysis['file_structure'] = 'combined'
                else:
                    analysis['file_structure'] = 'separate_per_stock'
            
            # Analyze OHLCV completeness
            ohlcv_present = {
                'open': len(identified_cols['open']) > 0,
                'high': len(identified_cols['high']) > 0,
                'low': len(identified_cols['low']) > 0,
                'close': len(identified_cols['close']) > 0,
                'volume': len(identified_cols['volume']) > 0
            }
            analysis['ohlcv_completeness'] = ohlcv_present
            analysis['has_complete_ohlcv'] = all(ohlcv_present.values())
            
            # Validate price data (OHLC relationships)
            if analysis['has_complete_ohlcv']:
                open_col = identified_cols['open'][0]
                high_col = identified_cols['high'][0]
                low_col = identified_cols['low'][0]
                close_col = identified_cols['close'][0]
                
                # Check for invalid price relationships
                invalid_prices = (
                    (df[high_col] < df[low_col]) |
                    (df[high_col] < df[open_col]) |
                    (df[high_col] < df[close_col]) |
                    (df[low_col] > df[open_col]) |
                    (df[low_col] > df[close_col]) |
                    (df[open_col] <= 0) |
                    (df[high_col] <= 0) |
                    (df[low_col] <= 0) |
                    (df[close_col] <= 0)
                )
                
                analysis['price_validation'] = {
                    'invalid_count': int(invalid_prices.sum()),
                    'invalid_percentage': float(invalid_prices.sum() / len(df) * 100)
                }
            
            # Check for duplicates
            duplicate_count = df.duplicated().sum()
            analysis['duplicates'] = {
                'count': int(duplicate_count),
                'percentage': float(duplicate_count / len(df) * 100)
            }
            
            # Count technical indicators
            indicator_count = sum(len(indicators) for indicators in identified_cols['technical_indicators'].values())
            analysis['technical_indicator_count'] = indicator_count
            analysis['technical_indicator_types'] = list(identified_cols['technical_indicators'].keys())
            
            # Sample records
            analysis['sample_records'] = df.head(3).to_dict('records')
            
        except Exception as e:
            analysis['error'] = str(e)
            analysis['error_type'] = type(e).__name__
        
        return analysis
    
    def _inspect_parquet_file(self, file_path: Path) -> Dict[str, Any]:
        """Inspect a Parquet file."""
        analysis = {
            'file_type': 'parquet',
            'file_size_mb': file_path.stat().st_size / (1024 * 1024),
            'file_size_bytes': file_path.stat().st_size,
        }
        
        try:
            df = pd.read_parquet(file_path)
            
            analysis['rows_total'] = len(df)
            analysis['columns'] = df.columns.tolist()
            analysis['column_count'] = len(df.columns)
            
            # Identify column types
            identified_cols = self._identify_column_types(df.columns.tolist())
            analysis['identified_columns'] = identified_cols
            
            # Basic column info
            columns_info = {}
            for col in df.columns:
                col_data = df[col]
                columns_info[col] = {
                    'dtype': str(col_data.dtype),
                    'non_null_count': int(col_data.notna().sum()),
                    'null_count': int(col_data.isna().sum()),
                    'null_percentage': float(col_data.isna().sum() / len(col_data) * 100),
                    'unique_values': int(col_data.nunique())
                }
            
            analysis['columns_info'] = columns_info
            
            # Symbol analysis
            if identified_cols['symbol']:
                symbol_col = identified_cols['symbol'][0]
                unique_symbols = df[symbol_col].dropna().unique().tolist()
                analysis['symbol_analysis'] = {
                    'column': symbol_col,
                    'unique_count': len(unique_symbols),
                    'symbols': sorted([str(s) for s in unique_symbols])
                }
            
        except Exception as e:
            analysis['error'] = str(e)
            analysis['error_type'] = type(e).__name__
        
        return analysis
    
    def inspect(self) -> Dict[str, Any]:
        """Run comprehensive inspection of the dataset."""
        print("="*80)
        print("SmartStock - Dataset Inspection")
        print("="*80)
        print(f"\nDataset path: {self.dataset_path}")
        
        # Validate path
        if not self.dataset_path.exists():
            error_msg = f"Path does not exist: {self.dataset_path}"
            print(f"\n✗ Error: {error_msg}")
            self.results['error'] = error_msg
            return self.results
        
        # Discover files
        print("\n[1/3] Discovering data files...")
        files = self._discover_files()
        
        if not files:
            error_msg = f"No supported data files found in: {self.dataset_path}"
            print(f"\n✗ Error: {error_msg}")
            print("Supported formats: CSV, Parquet (.csv, .parquet, .pq, .snappy)")
            self.results['error'] = error_msg
            return self.results
        
        print(f"✓ Found {len(files)} file(s)")
        for f in files:
            print(f"  - {f.name} ({f.stat().st_size / (1024*1024):.2f} MB)")
        
        # Inspect each file
        print(f"\n[2/3] Inspecting files...")
        for idx, file_path in enumerate(files, 1):
            print(f"\n  [{idx}/{len(files)}] Analyzing: {file_path.name}")
            
            if file_path.suffix.lower() == '.csv':
                file_results = self._inspect_csv_file(file_path)
            elif file_path.suffix.lower() in {'.parquet', '.pq', '.snappy'}:
                file_results = self._inspect_parquet_file(file_path)
            else:
                file_results = {'error': 'Unsupported file type'}
            
            self.results['files'][file_path.name] = file_results
            
            if 'error' not in file_results:
                print(f"      ✓ Rows: {file_results.get('rows_total', 0):,}")
                print(f"      ✓ Columns: {file_results.get('column_count', 0)}")
        
        # Generate summary
        print(f"\n[3/3] Generating summary...")
        self._generate_summary()
        
        # Save results
        if self.output_path:
            output_path = Path(self.output_path)
        else:
            output_dir = project_root / "data" / "cache"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / "dataset_inspection_detailed.json"
        
        with open(output_path, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        print(f"✓ Report saved to: {output_path}")
        
        # Print readable summary
        self._print_summary()
        
        return self.results
    
    def _generate_summary(self):
        """Generate overall summary statistics."""
        summary = {
            'total_files': len(self.results['files']),
            'total_rows': 0,
            'total_size_mb': 0,
            'all_symbols': set(),
            'file_structure': None,
            'has_technical_indicators': False,
            'technical_indicator_count': 0,
            'errors': []
        }
        
        for filename, file_data in self.results['files'].items():
            if 'error' in file_data:
                summary['errors'].append(f"{filename}: {file_data['error']}")
                continue
            
            summary['total_rows'] += file_data.get('rows_total', 0)
            summary['total_size_mb'] += file_data.get('file_size_mb', 0)
            
            if 'symbol_analysis' in file_data:
                summary['all_symbols'].update(file_data['symbol_analysis'].get('symbols', []))
            
            if file_data.get('technical_indicator_count', 0) > 0:
                summary['has_technical_indicators'] = True
                summary['technical_indicator_count'] = max(
                    summary['technical_indicator_count'],
                    file_data['technical_indicator_count']
                )
            
            if 'file_structure' in file_data and not summary['file_structure']:
                summary['file_structure'] = file_data['file_structure']
        
        summary['all_symbols'] = sorted(list(summary['all_symbols']))
        summary['total_symbols'] = len(summary['all_symbols'])
        
        self.results['summary'] = summary
    
    def _print_summary(self):
        """Print human-readable summary."""
        print("\n" + "="*80)
        print("INSPECTION SUMMARY")
        print("="*80)
        
        summary = self.results['summary']
        
        print(f"\n📊 Dataset Overview:")
        print(f"   Total files: {summary['total_files']}")
        print(f"   Total rows: {summary['total_rows']:,}")
        print(f"   Total size: {summary['total_size_mb']:.2f} MB")
        print(f"   File structure: {summary['file_structure'] or 'Unknown'}")
        
        if summary['total_symbols'] > 0:
            print(f"\n📈 Stock Symbols:")
            print(f"   Unique symbols: {summary['total_symbols']}")
            symbols_to_show = summary['all_symbols'][:15]
            print(f"   Symbols: {', '.join(symbols_to_show)}", end='')
            if summary['total_symbols'] > 15:
                print(f" ... (+{summary['total_symbols'] - 15} more)")
            else:
                print()
        
        # Detailed file information
        print(f"\n📁 File Details:")
        for filename, file_data in self.results['files'].items():
            if 'error' in file_data:
                print(f"\n   ✗ {filename}: ERROR - {file_data['error']}")
                continue
            
            print(f"\n   ✓ {filename}")
            print(f"      Rows: {file_data['rows_total']:,}")
            print(f"      Columns: {file_data['column_count']}")
            print(f"      Size: {file_data['file_size_mb']:.2f} MB")
            
            # Show identified columns
            identified = file_data.get('identified_columns', {})
            print(f"      Identified columns:")
            if identified.get('timestamp'):
                print(f"         Timestamp: {', '.join(identified['timestamp'])}")
            if identified.get('symbol'):
                print(f"         Symbol: {', '.join(identified['symbol'])}")
            if identified.get('open'):
                print(f"         OHLCV: O={identified['open'][0] if identified['open'] else 'N/A'}, "
                      f"H={identified['high'][0] if identified['high'] else 'N/A'}, "
                      f"L={identified['low'][0] if identified['low'] else 'N/A'}, "
                      f"C={identified['close'][0] if identified['close'] else 'N/A'}, "
                      f"V={identified['volume'][0] if identified['volume'] else 'N/A'}")
            
            # Timestamp range
            if 'timestamp_analysis' in file_data and 'min' in file_data['timestamp_analysis']:
                ts = file_data['timestamp_analysis']
                print(f"      Date range: {ts['min']} to {ts['max']}")
            
            # Data quality
            if 'duplicates' in file_data:
                dup = file_data['duplicates']
                print(f"      Duplicates: {dup['count']:,} ({dup['percentage']:.2f}%)")
            
            if 'price_validation' in file_data:
                pv = file_data['price_validation']
                print(f"      Invalid prices: {pv['invalid_count']:,} ({pv['invalid_percentage']:.2f}%)")
            
            # Technical indicators
            if file_data.get('technical_indicator_count', 0) > 0:
                print(f"      Technical indicators: {file_data['technical_indicator_count']} columns")
                indicator_types = file_data.get('technical_indicator_types', [])
                if indicator_types:
                    print(f"         Types: {', '.join(indicator_types[:10])}")
        
        if summary['errors']:
            print(f"\n⚠️  Errors encountered:")
            for error in summary['errors']:
                print(f"   - {error}")
        
        print("\n" + "="*80)
        print("Next steps:")
        print("  1. Review the JSON report for complete details")
        print("  2. Update config/config.yaml based on discovered schema")
        print("  3. Select test stocks from the symbol list")
        print("  4. Proceed to data cleaning and transformation")
        print("="*80)


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description='Inspect stock market dataset structure and quality',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        '--path',
        type=str,
        help='Path to dataset directory or file (default: from config)'
    )
    
    parser.add_argument(
        '--output',
        type=str,
        help='Path to save JSON report (default: data/cache/dataset_inspection_detailed.json)'
    )
    
    args = parser.parse_args()
    
    # Determine dataset path
    if args.path:
        dataset_path = Path(args.path)
    else:
        # Try to get from config
        try:
            config = get_config()
            dataset_path = Path(config.get('paths.raw_data'))
            
            # If raw_data is empty, try to read kagglehub cache location
            if not dataset_path.exists() or not any(dataset_path.iterdir()):
                print(f"\n⚠️  Warning: Config path empty or doesn't exist: {dataset_path}")
                print("Please specify --path argument with your dataset location.")
                print("\nExample paths:")
                print("  - Project data: --path data/raw")
                print("  - Kagglehub cache: --path \"C:/Users/YourName/.cache/kagglehub/datasets/...\"")
                sys.exit(1)
        except Exception as e:
            print(f"✗ Error loading config: {e}")
            print("Please specify --path argument with your dataset location.")
            sys.exit(1)
    
    # Run inspection
    output_path = Path(args.output) if args.output else None
    inspector = DatasetInspector(dataset_path, output_path)
    
    try:
        results = inspector.inspect()
        
        if 'error' in results:
            sys.exit(1)
        else:
            sys.exit(0)
    
    except KeyboardInterrupt:
        print("\n\nInspection interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
