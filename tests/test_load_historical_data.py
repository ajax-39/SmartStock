"""Tests for historical data loader."""

import pytest
from pathlib import Path
import sys
import tempfile
import pandas as pd
import os

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.config import get_config
from src.ingestion.load_historical_data import ColumnMapper, DataValidator, HistoricalDataLoader


class TestColumnMapper:
    """Test suite for ColumnMapper."""
    
    def test_pattern_matching(self):
        """Test column pattern matching."""
        config = get_config(force_reload=True)
        columns = ["DateTime", "Symbol", "Open", "High", "Low", "Close", "Volume"]
        mapper = ColumnMapper(columns, config)
        
        assert mapper._match_pattern("DateTime", mapper.TIMESTAMP_PATTERNS)
        assert mapper._match_pattern("Symbol", mapper.SYMBOL_PATTERNS)
        assert not mapper._match_pattern("Unknown", mapper.TIMESTAMP_PATTERNS)
    
    def test_auto_detect(self):
        """Test automatic column detection."""
        config = get_config(force_reload=True)
        columns = [
            "DateTime", "Symbol", "Open", "High", "Low", "Close", "Volume",
            "RSI", "MACD", "SMA_20"
        ]
        mapper = ColumnMapper(columns, config)
        mapping = mapper.auto_detect()
        
        assert 'timestamp' in mapping
        assert 'symbol' in mapping
        assert 'open' in mapping
        assert 'high' in mapping
        assert 'low' in mapping
        assert 'close' in mapping
        assert 'volume' in mapping
        assert len(mapper.technical_indicators) == 3
    
    def test_validate_mapping(self):
        """Test mapping validation."""
        config = get_config(force_reload=True)
        
        # Valid mapping
        columns = ["DateTime", "Open", "High", "Low", "Close"]
        mapper = ColumnMapper(columns, config)
        mapper.auto_detect()
        is_valid, missing = mapper.validate_mapping()
        assert is_valid
        assert len(missing) == 0
        
        # Invalid mapping (missing required columns)
        columns = ["DateTime", "SomeColumn"]
        mapper = ColumnMapper(columns, config)
        mapper.auto_detect()
        is_valid, missing = mapper.validate_mapping()
        assert not is_valid
        assert len(missing) > 0
    
    def test_get_renamed_mapping(self):
        """Test getting rename mapping."""
        config = get_config(force_reload=True)
        columns = ["DateTime", "Symbol", "Open", "High", "Low", "Close"]
        mapper = ColumnMapper(columns, config)
        mapper.auto_detect()
        
        rename_map = mapper.get_renamed_mapping()
        assert "DateTime" in rename_map
        assert rename_map["DateTime"] == "timestamp"


class TestDataValidator:
    """Test suite for DataValidator."""
    
    def test_validate_nulls(self):
        """Test null validation."""
        config = get_config(force_reload=True)
        validator = DataValidator(config)
        
        # Good data
        df = pd.DataFrame({'col1': [1, 2, 3, 4, 5]})
        is_valid, null_pct = validator.validate_nulls(df, 'col1')
        assert is_valid
        assert null_pct == 0.0
        
        # Too many nulls
        df = pd.DataFrame({'col1': [1, None, None, None, None]})
        is_valid, null_pct = validator.validate_nulls(df, 'col1')
        assert not is_valid
        assert null_pct == 80.0
    
    def test_validate_prices(self):
        """Test price validation."""
        config = get_config(force_reload=True)
        validator = DataValidator(config)
        
        # Valid prices
        df = pd.DataFrame({
            'open': [100, 101, 102],
            'high': [105, 106, 107],
            'low': [95, 96, 97],
            'close': [103, 104, 105]
        })
        is_valid, invalid_count = validator.validate_prices(df)
        assert is_valid
        assert invalid_count == 0
        
        # Invalid prices (high < low)
        df = pd.DataFrame({
            'open': [100, 101, 102],
            'high': [90, 91, 92],  # Invalid: high < low
            'low': [95, 96, 97],
            'close': [93, 94, 95]
        })
        is_valid, invalid_count = validator.validate_prices(df)
        assert not is_valid
        assert invalid_count == 3
    
    def test_check_duplicates(self):
        """Test duplicate detection."""
        config = get_config(force_reload=True)
        validator = DataValidator(config)
        
        # No duplicates
        df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
        dup_count, dup_pct = validator.check_duplicates(df)
        assert dup_count == 0
        assert dup_pct == 0.0
        
        # With duplicates
        df = pd.DataFrame({'col1': [1, 2, 1], 'col2': ['a', 'b', 'a']})
        dup_count, dup_pct = validator.check_duplicates(df)
        assert dup_count == 1


class TestHistoricalDataLoader:
    """Test suite for HistoricalDataLoader."""
    
    def test_discover_files_nonexistent_path(self):
        """Test error handling for nonexistent path."""
        config = get_config(override_source_path="/nonexistent/path", force_reload=True)
        loader = HistoricalDataLoader(config)
        
        with pytest.raises(FileNotFoundError):
            loader.discover_files()
    
    def test_discover_files_empty_directory(self):
        """Test error handling for empty directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = get_config(override_source_path=tmpdir, force_reload=True)
            loader = HistoricalDataLoader(config)
            
            with pytest.raises(FileNotFoundError):
                loader.discover_files()
    
    def test_discover_files_success(self):
        """Test successful file discovery."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            
            # Create test CSV file
            csv_path = tmppath / "test.csv"
            df = pd.DataFrame({
                'DateTime': pd.date_range('2024-01-01', periods=10, freq='1min'),
                'Symbol': ['TEST'] * 10,
                'Open': [100] * 10,
                'High': [105] * 10,
                'Low': [95] * 10,
                'Close': [102] * 10,
                'Volume': [1000] * 10
            })
            df.to_csv(csv_path, index=False)
            
            config = get_config(override_source_path=str(tmppath), force_reload=True)
            loader = HistoricalDataLoader(config)
            files = loader.discover_files()
            
            assert len(files) == 1
            assert files[0].name == "test.csv"
    
    def test_load_and_map_columns(self):
        """Test loading and column mapping."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            
            # Create test CSV file
            csv_path = tmppath / "test.csv"
            df = pd.DataFrame({
                'DateTime': pd.date_range('2024-01-01', periods=100, freq='1min'),
                'Symbol': ['TEST'] * 100,
                'Open': [100.0 + i for i in range(100)],
                'High': [105.0 + i for i in range(100)],
                'Low': [95.0 + i for i in range(100)],
                'Close': [102.0 + i for i in range(100)],
                'Volume': [1000 + i for i in range(100)]
            })
            df.to_csv(csv_path, index=False)
            
            config = get_config(override_source_path=str(tmppath), force_reload=True)
            loader = HistoricalDataLoader(config)
            
            loaded_df, mapper = loader.load_and_map_columns(csv_path)
            
            # Check mapping was successful
            assert 'timestamp' in loaded_df.columns
            assert 'symbol' in loaded_df.columns
            assert 'open' in loaded_df.columns
            assert len(loaded_df) == 100
    
    def test_extract_metadata(self):
        """Test metadata extraction."""
        config = get_config(force_reload=True)
        loader = HistoricalDataLoader(config)
        
        df = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=100, freq='1min'),
            'symbol': ['STOCK1'] * 50 + ['STOCK2'] * 50,
            'open': [100] * 100,
            'high': [105] * 100,
            'low': [95] * 100,
            'close': [102] * 100
        })
        
        loader.extract_metadata(df)
        
        assert len(loader.metadata['symbols']) == 2
        assert 'STOCK1' in loader.metadata['symbols']
        assert 'STOCK2' in loader.metadata['symbols']
        assert 'min' in loader.metadata['date_range']
        assert 'max' in loader.metadata['date_range']

    def test_streaming_ingest_with_partitioning(self):
        """Test end-to-end streaming ingestion with symbol partitioning."""
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as out_dir:
            src_path = Path(src_dir)
            out_path = Path(out_dir)

            # Create two stock CSV files
            for symbol in ['RELIANCE', 'TCS']:
                df = pd.DataFrame({
                    'date': pd.date_range('2024-01-01 09:15', periods=50, freq='1min'),
                    'open': [2500.0] * 50,
                    'high': [2510.0] * 50,
                    'low': [2490.0] * 50,
                    'close': [2505.0] * 50,
                    'volume': [1000] * 50
                })
                # Named with _minute suffix like actual dataset
                df.to_csv(src_path / f"{symbol}_minute.csv", index=False)

            config = get_config(override_source_path=str(src_path), force_reload=True)
            config.ingestion.chunk_size = 20  # Test small chunk streaming
            config.ingestion.partition_by_symbol = True

            loader = HistoricalDataLoader(config)
            meta = loader.ingest(output_dir=out_path)

            # Validate metadata
            assert meta['total_records'] == 100
            assert set(meta['symbols']) == {'RELIANCE', 'TCS'}
            assert len(meta['files_processed']) == 2
            assert (out_path / "ingestion_metadata.json").exists()
            assert (out_path / "data_quality_summary.json").exists()

            # Validate partitioned Parquet structure
            stock_data_dir = out_path / "stock_data"
            assert stock_data_dir.exists()
            rel_part = stock_data_dir / "symbol=RELIANCE"
            tcs_part = stock_data_dir / "symbol=TCS"
            assert rel_part.exists()
            assert tcs_part.exists()

    def test_parquet_input_ingestion(self):
        """Test streaming ingestion from Parquet files."""
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as out_dir:
            src_path = Path(src_dir)
            out_path = Path(out_dir)

            df = pd.DataFrame({
                'timestamp': pd.date_range('2024-01-01 09:15', periods=30, freq='1min'),
                'symbol': ['INFY'] * 30,
                'open': [1500.0] * 30,
                'high': [1510.0] * 30,
                'low': [1490.0] * 30,
                'close': [1505.0] * 30,
                'volume': [500] * 30
            })
            pq_file = src_path / "INFY.parquet"
            df.to_parquet(pq_file, index=False)

            config = get_config(override_source_path=str(src_path), force_reload=True)
            loader = HistoricalDataLoader(config)
            meta = loader.ingest(output_dir=out_path)

            assert meta['total_records'] == 30
            assert meta['symbols'] == ['INFY']

    def test_strict_failure_on_missing_required_columns(self):
        """Test strict failure when required OHLC columns are missing (Requirement 8)."""
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as out_dir:
            src_path = Path(src_dir)
            out_path = Path(out_dir)

            # Missing open, high, low
            df = pd.DataFrame({
                'date': pd.date_range('2024-01-01', periods=10, freq='1min'),
                'close': [100.0] * 10
            })
            df.to_csv(src_path / "BAD_minute.csv", index=False)

            config = get_config(override_source_path=str(src_path), force_reload=True)
            loader = HistoricalDataLoader(config)
            with pytest.raises(ValueError, match="missing required columns"):
                loader.ingest(output_dir=out_path)

    def test_strict_failure_on_unextractable_symbol(self):
        """Test strict failure when symbol cannot be extracted from data or filename (Requirement 8)."""
        with tempfile.TemporaryDirectory() as src_dir, tempfile.TemporaryDirectory() as out_dir:
            src_path = Path(src_dir)
            out_path = Path(out_dir)

            # Generic filename 'data.csv' without symbol column
            df = pd.DataFrame({
                'date': pd.date_range('2024-01-01', periods=10, freq='1min'),
                'open': [100.0] * 10,
                'high': [105.0] * 10,
                'low': [95.0] * 10,
                'close': [102.0] * 10,
                'volume': [1000] * 10
            })
            df.to_csv(src_path / "data.csv", index=False)

            config = get_config(override_source_path=str(src_path), force_reload=True)
            loader = HistoricalDataLoader(config)
            with pytest.raises(ValueError, match=r"missing required columns.*'symbol'"):
                loader.ingest(output_dir=out_path)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
