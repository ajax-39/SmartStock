"""
Historical data ingestion for SmartStock.

This module loads historical stock market data from local files or Kaggle cache,
maps columns to a canonical schema, validates data quality, and writes the output
into partitioned Parquet format using streaming PyArrow processing.

Key features:
- Constant memory consumption: streams chunk-by-chunk without loading full dataset into Pandas.
- Automatic column mapping to canonical schema: timestamp, symbol, open, high, low, close, volume, indicators.
- Symbol resolution: discovers symbol from data columns or derives from filename with provenance metadata.
- Strict data validation: drops invalid timestamps or malformed OHLC prices; never manufactures fake data.
- Dual format support: handles CSV and Parquet inputs.
- Partitioned Parquet output: structured by symbol (symbol=<SYMBOL>/part-*.parquet) for fast PySpark queries.
- Detailed ingestion metadata and data-quality summary tracking.

Usage:
    python src/ingestion/load_historical_data.py
    python src/ingestion/load_historical_data.py --path /path/to/dataset
    python src/ingestion/load_historical_data.py --path /path/to/dataset --output-dir data/processed
    python src/ingestion/load_historical_data.py --max-files 5
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from tqdm import tqdm

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Ensure robust cross-platform encoding on Windows cp1252 consoles
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
if hasattr(sys.stderr, 'reconfigure'):
    try:
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

from src.config import Config, get_config


class ColumnMapper:
    """Maps source columns to canonical schema."""

    TIMESTAMP_PATTERNS = ['date', 'time', 'datetime', 'timestamp', 'dt']
    SYMBOL_PATTERNS = ['symbol', 'stock', 'ticker', 'code', 'name']
    OPEN_PATTERNS = ['open', 'opening']
    HIGH_PATTERNS = ['high', 'highest']
    LOW_PATTERNS = ['low', 'lowest']
    CLOSE_PATTERNS = ['close', 'closing']
    VOLUME_PATTERNS = ['volume', 'vol', 'quantity', 'qty']

    # Known technical indicator prefixes / patterns
    INDICATOR_PATTERNS = [
        'rsi', 'macd', 'sma', 'ema', 'wma', 'bb_', 'bollinger', 'atr', 'adx',
        'obv', 'cci', 'stoch', 'roc', 'mom', 'volatility', 'vwap'
    ]

    # File stems that are generic and should not be treated as stock symbols
    GENERIC_STEMS = {
        'data', 'dataset', 'sample', 'stock_data', 'combined', 'stocks',
        'historical', 'test', 'train', 'raw', 'market_data'
    }

    def __init__(self, source_columns: List[str], config: Config):
        """
        Initialize column mapper.

        Args:
            source_columns: List of column names from source data
            config: Configuration object
        """
        self.source_columns = source_columns
        self.config = config
        self.mapping: Dict[str, str] = {}
        self.technical_indicators: List[str] = []
        self.symbol_provenance: str = "none"

    def _match_pattern(self, column: str, patterns: List[str]) -> bool:
        """Check if column name matches any pattern."""
        col_lower = column.lower().strip()
        # Direct exact or prefix/substring match
        return any(pattern in col_lower for pattern in patterns)

    def auto_detect(self) -> Dict[str, str]:
        """
        Automatically detect column mappings.

        Returns:
            Dictionary mapping canonical names to source column names
        """
        mapping: Dict[str, str] = {}
        used_columns: Set[str] = set()

        for col in self.source_columns:
            if col in used_columns:
                continue

            # Timestamp
            if 'timestamp' not in mapping and self._match_pattern(col, self.TIMESTAMP_PATTERNS):
                mapping['timestamp'] = col
                used_columns.add(col)
            # Symbol
            elif 'symbol' not in mapping and self._match_pattern(col, self.SYMBOL_PATTERNS):
                mapping['symbol'] = col
                used_columns.add(col)
                self.symbol_provenance = "column"
            # OHLCV
            elif 'open' not in mapping and self._match_pattern(col, self.OPEN_PATTERNS):
                mapping['open'] = col
                used_columns.add(col)
            elif 'high' not in mapping and self._match_pattern(col, self.HIGH_PATTERNS):
                mapping['high'] = col
                used_columns.add(col)
            elif 'low' not in mapping and self._match_pattern(col, self.LOW_PATTERNS):
                mapping['low'] = col
                used_columns.add(col)
            elif 'close' not in mapping and self._match_pattern(col, self.CLOSE_PATTERNS):
                mapping['close'] = col
                used_columns.add(col)
            elif 'volume' not in mapping and self._match_pattern(col, self.VOLUME_PATTERNS):
                mapping['volume'] = col
                used_columns.add(col)

        # Identify technical indicators or extra feature columns
        self.technical_indicators = [
            col for col in self.source_columns if col not in used_columns
        ]

        self.mapping = mapping
        return mapping

    @classmethod
    def extract_symbol_from_path(cls, file_path: Path) -> Optional[str]:
        """
        Extract stock ticker symbol from file name.
        Example: 'ABB_minute.csv' -> 'ABB', 'TCS.parquet' -> 'TCS'.
        """
        stem = file_path.stem
        # Clean off common frequency/descriptor suffixes
        cleaned = re.sub(
            r'(_minute|_1min|_1m|_data|_daily|_hourly|_historical)$',
            '',
            stem,
            flags=re.IGNORECASE
        )
        cleaned = cleaned.strip().upper()

        if cleaned.lower() in cls.GENERIC_STEMS or not cleaned:
            return None

        # Basic check for realistic ticker symbol (alphanumeric, max 20 chars)
        if re.match(r'^[A-Z0-9_\-\.]{1,20}$', cleaned):
            return cleaned
        return None

    def validate_mapping(self, file_path: Optional[Path] = None) -> Tuple[bool, List[str]]:
        """
        Validate that required columns are mapped.

        Required: timestamp, open, high, low, close.
        Symbol: if file_path is provided, symbol must be mapped or derived from filename.
        """
        required = ['timestamp', 'open', 'high', 'low', 'close']
        missing = [col for col in required if col not in self.mapping]

        # Only validate symbol if a specific file_path is provided for validation
        if file_path is not None and 'symbol' not in self.mapping:
            derived_symbol = self.extract_symbol_from_path(file_path)
            if derived_symbol:
                self.symbol_provenance = "filename"
            else:
                missing.append('symbol')

        return len(missing) == 0, missing

    def get_renamed_mapping(self) -> Dict[str, str]:
        """
        Get mapping for pandas rename() function.

        Returns:
            Dictionary mapping source column names to canonical names
        """
        return {v: k for k, v in self.mapping.items()}


class DataValidator:
    """Validates data quality and tracks issues."""

    def __init__(self, config: Config):
        self.config = config
        self.issues: List[str] = []

    def validate_nulls(self, df: pd.DataFrame, column: str) -> Tuple[bool, float]:
        """Check null percentage for a column."""
        if len(df) == 0:
            return True, 0.0
        null_count = df[column].isna().sum()
        null_pct = (null_count / len(df)) * 100.0
        is_valid = null_pct <= self.config.ingestion.max_null_percentage

        if not is_valid:
            self.issues.append(
                f"Column '{column}' has {null_pct:.2f}% nulls "
                f"(threshold: {self.config.ingestion.max_null_percentage}%)"
            )

        return is_valid, float(null_pct)

    def validate_prices(self, df: pd.DataFrame) -> Tuple[bool, int]:
        """Validate OHLC price relationships."""
        if not all(col in df.columns for col in ['open', 'high', 'low', 'close']):
            return True, 0

        if len(df) == 0:
            return True, 0

        invalid_mask = (
            (df['high'] < df['low']) |
            (df['high'] < df['open']) |
            (df['high'] < df['close']) |
            (df['low'] > df['open']) |
            (df['low'] > df['close']) |
            (df['open'] <= 0) |
            (df['high'] <= 0) |
            (df['low'] <= 0) |
            (df['close'] <= 0)
        )

        invalid_count = int(invalid_mask.sum())
        if invalid_count > 0:
            invalid_pct = (invalid_count / len(df)) * 100.0
            self.issues.append(
                f"Found {invalid_count} ({invalid_pct:.2f}%) invalid price records"
            )

        return invalid_count == 0, invalid_count

    def check_duplicates(self, df: pd.DataFrame) -> Tuple[int, float]:
        """Check for duplicate records."""
        if len(df) == 0:
            return 0, 0.0
        dup_count = int(df.duplicated().sum())
        dup_pct = (dup_count / len(df)) * 100.0

        if dup_pct > self.config.ingestion.max_duplicate_percentage:
            self.issues.append(
                f"Found {dup_count} ({dup_pct:.2f}%) duplicate records "
                f"(threshold: {self.config.ingestion.max_duplicate_percentage}%)"
            )

        return dup_count, float(dup_pct)


class HistoricalDataLoader:
    """
    Loads and processes historical stock market data with constant memory consumption.
    Streams input files chunk-by-chunk directly into partitioned Parquet format.
    """

    # Canonical PyArrow Schema definitions
    CANONICAL_PYARROW_FIELDS = [
        pa.field("timestamp", pa.timestamp("us")),
        pa.field("symbol", pa.string()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.float64()),
    ]

    def __init__(self, config: Config):
        """Initialize data loader with configuration."""
        self.config = config
        self.metadata: Dict[str, Any] = {
            'ingestion_time': datetime.now().isoformat(),
            'source_path': str(config.ingestion.source_path),
            'files_processed': [],
            'total_raw_records': 0,
            'total_records': 0,
            'total_dropped_records': 0,
            'canonical_schema': {
                'required': ['timestamp', 'symbol', 'open', 'high', 'low', 'close'],
                'optional': ['volume'],
                'indicators': []
            },
            'column_mapping': {},
            'symbol_provenance': {},
            'data_quality': {},
            'symbols': [],
            'date_range': {}
        }
        self.unique_symbols: Set[str] = set()
        self.global_min_timestamp: Optional[datetime] = None
        self.global_max_timestamp: Optional[datetime] = None

    def discover_files(self) -> List[Path]:
        """
        Discover data files in source path.
        Supports single files or directories with CSV and Parquet files.
        """
        source_str = self.config.ingestion.source_path
        if not source_str:
            raise FileNotFoundError(
                "Dataset source path is not configured.\n"
                "Please specify via --path argument, SMARTSTOCK_DATASET_PATH env var, "
                "or run scripts/download_kaggle_data.py to download."
            )

        source_path = Path(source_str)
        if not source_path.exists():
            raise FileNotFoundError(
                f"Dataset path does not exist: {source_path}\n"
                f"Please verify the path with: --path <your-dataset-path>\n"
                f"Or download via: python scripts/download_kaggle_data.py"
            )

        files: List[Path] = []
        if source_path.is_file():
            if source_path.suffix.lower() in ['.csv', '.parquet', '.pq']:
                files = [source_path]
        else:
            for ext in ['*.csv', '*.parquet', '*.pq']:
                files.extend(source_path.rglob(ext))

        if not files:
            raise FileNotFoundError(
                f"No CSV or Parquet files found in: {source_path}\n"
                f"Supported formats: .csv, .parquet, .pq"
            )

        return sorted(files)

    def _read_chunks(self, file_path: Path) -> Generator[pd.DataFrame, None, None]:
        """
        Yield data chunks from CSV or Parquet without loading full file into memory.
        """
        chunk_size = self.config.ingestion.chunk_size

        if file_path.suffix.lower() == '.csv':
            # Low memory chunked CSV reader
            reader = pd.read_csv(
                file_path,
                chunksize=chunk_size,
                low_memory=True
            )
            for chunk in reader:
                yield chunk
        else:
            # Chunked Parquet reader using PyArrow record batches
            parquet_file = pq.ParquetFile(file_path)
            for batch in parquet_file.iter_batches(batch_size=chunk_size):
                yield batch.to_pandas()

    def _process_chunk(
        self,
        chunk: pd.DataFrame,
        mapper: ColumnMapper,
        file_path: Path,
        derived_symbol: Optional[str]
    ) -> Tuple[pd.DataFrame, int]:
        """
        Clean, validate, and convert a single chunk to the canonical schema.
        Returns: (processed_df, dropped_rows_count)
        """
        raw_count = len(chunk)
        if raw_count == 0:
            return chunk, 0

        # Rename mapped columns
        rename_map = mapper.get_renamed_mapping()
        df = chunk.rename(columns=rename_map)

        # Assign derived symbol if not in data columns
        if 'symbol' not in df.columns:
            if derived_symbol:
                df['symbol'] = derived_symbol
            else:
                raise ValueError(
                    f"Symbol could not be determined for file {file_path.name}. "
                    f"Neither a 'symbol' column exists nor can a ticker be derived from filename."
                )

        # Parse timestamp strictly and ensure microsecond precision for big data Parquet compatibility
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        valid_ts_mask = df['timestamp'].notna()
        df = df[valid_ts_mask]
        if len(df) > 0:
            df['timestamp'] = df['timestamp'].astype('datetime64[us]')

        # Ensure volume exists as float64
        if 'volume' not in df.columns:
            df['volume'] = 0.0

        # Coerce numeric columns
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Drop nulls in critical columns
        critical_cols = ['timestamp', 'symbol', 'open', 'high', 'low', 'close']
        df = df.dropna(subset=critical_cols)

        # Validate prices
        if self.config.ingestion.validate_prices and len(df) > 0:
            valid_price_mask = (
                (df['high'] >= df['low']) &
                (df['high'] >= df['open']) &
                (df['high'] >= df['close']) &
                (df['low'] <= df['open']) &
                (df['low'] <= df['close']) &
                (df['open'] > 0) &
                (df['high'] > 0) &
                (df['low'] > 0) &
                (df['close'] > 0)
            )
            df = df[valid_price_mask]

        # Check and remove duplicates within chunk
        if self.config.ingestion.remove_duplicates and len(df) > 0:
            df = df.drop_duplicates(subset=['symbol', 'timestamp'])

        # Enforce canonical column order: canonical fields first, then indicators
        canonical_base = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']
        indicator_cols = [c for c in mapper.technical_indicators if c in df.columns]
        for ind in indicator_cols:
            df[ind] = pd.to_numeric(df[ind], errors='coerce')

        final_cols = canonical_base + indicator_cols
        df = df[[c for c in final_cols if c in df.columns]].copy()

        # Ensure string type on symbol
        df['symbol'] = df['symbol'].astype(str)

        dropped_count = raw_count - len(df)
        return df, dropped_count

    def load_and_map_columns(self, file_path: Path) -> Tuple[pd.DataFrame, ColumnMapper]:
        """
        Inspect file schema and load sample (or small file) for tests and introspection.
        """
        # Read header sample
        if file_path.suffix.lower() == '.csv':
            sample_df = pd.read_csv(file_path, nrows=100)
        else:
            sample_df = pd.read_parquet(file_path)
            if len(sample_df) > 100:
                sample_df = sample_df.head(100)

        mapper = ColumnMapper(sample_df.columns.tolist(), self.config)
        mapper.auto_detect()

        is_valid, missing = mapper.validate_mapping(file_path)
        if not is_valid:
            raise ValueError(
                f"Missing required columns in {file_path.name}: {missing}\n"
                f"Available columns: {sample_df.columns.tolist()}\n"
                f"Detected mapping: {mapper.mapping}"
            )

        derived_symbol = ColumnMapper.extract_symbol_from_path(file_path)
        processed_sample, _ = self._process_chunk(sample_df, mapper, file_path, derived_symbol)
        return processed_sample, mapper

    def extract_metadata(self, df: pd.DataFrame):
        """Update loader metadata with statistics from a DataFrame chunk."""
        if len(df) == 0:
            return

        if 'symbol' in df.columns:
            chunk_symbols = set(df['symbol'].unique())
            self.unique_symbols.update(chunk_symbols)

        if 'timestamp' in df.columns and len(df['timestamp']) > 0:
            chunk_min = df['timestamp'].min()
            chunk_max = df['timestamp'].max()

            if pd.notna(chunk_min):
                if self.global_min_timestamp is None or chunk_min < self.global_min_timestamp:
                    self.global_min_timestamp = chunk_min
            if pd.notna(chunk_max):
                if self.global_max_timestamp is None or chunk_max > self.global_max_timestamp:
                    self.global_max_timestamp = chunk_max

        self.metadata['total_records'] += len(df)
        self.metadata['symbols'] = sorted(list(self.unique_symbols))
        if self.global_min_timestamp and self.global_max_timestamp:
            self.metadata['date_range'] = {
                'min': self.global_min_timestamp.isoformat(),
                'max': self.global_max_timestamp.isoformat()
            }

    def _write_table_to_parquet(
        self,
        table: pa.Table,
        output_dir: Path,
        file_idx: int,
        chunk_idx: int
    ):
        """
        Write PyArrow table directly to partitioned or flat Parquet files.
        """
        if self.config.ingestion.partition_by_symbol:
            hive_partitioning = ds.partitioning(
                pa.schema([("symbol", pa.string())]),
                flavor="hive"
            )
            ds.write_dataset(
                data=table,
                base_dir=str(output_dir / "stock_data"),
                format="parquet",
                partitioning=hive_partitioning,
                existing_data_behavior="overwrite_or_ignore",
                basename_template=f"part_f{file_idx}_c{chunk_idx}_{{i}}.parquet"
            )
        else:
            out_file = output_dir / f"stock_data_part_f{file_idx}_c{chunk_idx}.parquet"
            pq.write_table(table, out_file, compression='snappy')

    def ingest(
        self,
        output_dir: Optional[Path] = None,
        max_files: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute streaming ingestion across all discovered files.

        Args:
            output_dir: Target output directory (defaults to processed_data path)
            max_files: Optional cap on files to process (useful for tests/quick runs)

        Returns:
            Ingestion metadata dictionary
        """
        print("=" * 80)
        print("SmartStock - Streaming Historical Data Ingestion")
        print("=" * 80)
        print(f"Source: {self.config.ingestion.source_path}")

        if output_dir is None:
            output_dir = Path(self.config.paths.processed_data)
        else:
            output_dir = Path(output_dir)

        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Output: {output_dir}")
        print(f"Chunk size: {self.config.ingestion.chunk_size:,} rows")
        print(f"Partition by symbol: {self.config.ingestion.partition_by_symbol}")

        # Discover input files
        print("\n[1/3] Discovering source data files...")
        files = self.discover_files()
        total_discovered = len(files)
        print(f"[OK] Found {total_discovered} source file(s)")

        effective_max = max_files or self.config.ingestion.max_files
        if effective_max and effective_max < total_discovered:
            files = files[:effective_max]
            print(f"  (Processing first {len(files)} file(s) as requested)")

        validator = DataValidator(self.config)

        print("\n[2/3] Streaming and transforming records...")
        file_pbar = tqdm(enumerate(files), total=len(files), desc="Processing files", unit="file")

        for file_idx, file_path in file_pbar:
            file_name = file_path.name
            file_pbar.set_postfix(file=file_name[:20])

            # Determine column mapping on header
            try:
                if file_path.suffix.lower() == '.csv':
                    sample_df = pd.read_csv(file_path, nrows=50)
                else:
                    sample_df = pd.read_parquet(file_path)
                    if len(sample_df) > 50:
                        sample_df = sample_df.head(50)

                mapper = ColumnMapper(sample_df.columns.tolist(), self.config)
                mapper.auto_detect()

                is_valid, missing = mapper.validate_mapping(file_path)
                if not is_valid:
                    raise ValueError(
                        f"Validation failed for {file_name}: missing required columns {missing}. "
                        f"Found columns: {sample_df.columns.tolist()}"
                    )

                derived_symbol = ColumnMapper.extract_symbol_from_path(file_path)

                # Store primary mapping in metadata
                if not self.metadata['column_mapping']:
                    self.metadata['column_mapping'] = mapper.mapping
                    self.metadata['canonical_schema']['indicators'] = mapper.technical_indicators

                self.metadata['symbol_provenance'][file_name] = mapper.symbol_provenance

            except Exception as e:
                print(f"\n✗ Error inspecting schema of {file_name}: {e}")
                raise

            # Stream file in chunks
            file_raw_records = 0
            file_valid_records = 0
            file_dropped_records = 0

            for chunk_idx, chunk_df in enumerate(self._read_chunks(file_path)):
                chunk_raw_len = len(chunk_df)
                file_raw_records += chunk_raw_len

                # Process chunk
                clean_df, dropped = self._process_chunk(
                    chunk_df, mapper, file_path, derived_symbol
                )
                file_valid_records += len(clean_df)
                file_dropped_records += dropped

                if len(clean_df) > 0:
                    # Update streaming statistics
                    self.extract_metadata(clean_df)

                    # Convert to PyArrow table
                    arrow_table = pa.Table.from_pandas(clean_df, preserve_index=False)

                    # Stream write to Parquet
                    self._write_table_to_parquet(
                        arrow_table, output_dir, file_idx, chunk_idx
                    )

            # Record file quality summary
            self.metadata['total_raw_records'] += file_raw_records
            self.metadata['total_dropped_records'] += file_dropped_records
            self.metadata['files_processed'].append(file_name)
            self.metadata['data_quality'][file_name] = {
                'raw_records': file_raw_records,
                'valid_records': file_valid_records,
                'dropped_records': file_dropped_records,
                'dropped_percentage': (
                    (file_dropped_records / file_raw_records * 100.0)
                    if file_raw_records > 0 else 0.0
                ),
                'symbol_provenance': mapper.symbol_provenance
            }

        # Consolidate metadata
        self.metadata['symbols'] = sorted(list(self.unique_symbols))
        if self.global_min_timestamp and self.global_max_timestamp:
            self.metadata['date_range'] = {
                'min': self.global_min_timestamp.isoformat(),
                'max': self.global_max_timestamp.isoformat()
            }

        # Save metadata and quality summary
        print("\n[3/3] Saving metadata and quality reports...")
        self.save_metadata(output_dir)
        self.save_data_quality_summary(output_dir)

        self.print_summary(output_dir)
        return self.metadata

    def save_metadata(self, output_dir: Path):
        """Save ingestion metadata JSON."""
        meta_path = output_dir / "ingestion_metadata.json"
        with open(meta_path, 'w') as f:
            json.dump(self.metadata, f, indent=2, default=str)
        print(f"[OK] Metadata saved: {meta_path}")

    def save_data_quality_summary(self, output_dir: Path):
        """Save separate concise data quality summary JSON."""
        dq_path = output_dir / "data_quality_summary.json"
        total_raw = self.metadata['total_raw_records']
        total_valid = self.metadata['total_records']
        total_dropped = self.metadata['total_dropped_records']

        dq_summary = {
            'generated_at': datetime.now().isoformat(),
            'total_files': len(self.metadata['files_processed']),
            'total_raw_records': total_raw,
            'total_valid_records': total_valid,
            'total_dropped_records': total_dropped,
            'dropped_percentage': (
                (total_dropped / total_raw * 100.0) if total_raw > 0 else 0.0
            ),
            'unique_symbols_count': len(self.metadata['symbols']),
            'date_range': self.metadata['date_range'],
            'per_file_quality': self.metadata['data_quality']
        }
        with open(dq_path, 'w') as f:
            json.dump(dq_summary, f, indent=2, default=str)
        print(f"[OK] Data quality summary saved: {dq_path}")

    def print_summary(self, output_dir: Path):
        """Print ingestion summary."""
        print("\n" + "=" * 80)
        print("INGESTION SUMMARY")
        print("=" * 80)
        print(f"Files processed:       {len(self.metadata['files_processed'])}")
        print(f"Total raw records:     {self.metadata['total_raw_records']:,}")
        print(f"Valid saved records:   {self.metadata['total_records']:,}")
        print(f"Dropped records:       {self.metadata['total_dropped_records']:,}")
        print(f"Unique stock symbols:  {len(self.metadata['symbols'])}")

        if self.metadata['symbols']:
            sample = self.metadata['symbols'][:10]
            extra = len(self.metadata['symbols']) - len(sample)
            display = ", ".join(sample)
            if extra > 0:
                display += f" ... (+{extra} more)"
            print(f"Stocks:                {display}")

        if self.metadata['date_range']:
            print(f"Date range:            {self.metadata['date_range']['min']} -> {self.metadata['date_range']['max']}")

        print(f"Output directory:      {output_dir}")
        print("=" * 80)
        print("[OK] Ingestion complete!\n")


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='SmartStock - Ingest historical stock data into canonical Parquet',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--path',
        type=str,
        default=None,
        help='Path to dataset directory or file (default: from config, SMARTSTOCK_DATASET_PATH env, or Kaggle cache)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='Output directory for processed Parquet (default: data/processed)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default=None,
        help='Path to config YAML (default: config/config.yaml)'
    )
    parser.add_argument(
        '--max-files',
        type=int,
        default=None,
        help='Limit number of files to ingest (ideal for small tests or sampling)'
    )

    args = parser.parse_args()

    try:
        config = get_config(
            config_path=args.config,
            override_source_path=args.path
        )
        loader = HistoricalDataLoader(config)
        loader.ingest(
            output_dir=Path(args.output_dir) if args.output_dir else None,
            max_files=args.max_files
        )
        sys.exit(0)

    except KeyboardInterrupt:
        print("\nIngestion interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
