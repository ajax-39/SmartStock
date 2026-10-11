"""Tests for dataset inspector."""

import pytest
from pathlib import Path
import sys
import tempfile
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.ingestion.inspect_dataset import DatasetInspector


class TestDatasetInspector:
    """Test suite for DatasetInspector."""
    
    def test_initialization(self):
        """Test inspector initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            inspector = DatasetInspector(Path(tmpdir))
            assert inspector.dataset_path == Path(tmpdir)
            assert 'inspection_time' in inspector.results
    
    def test_nonexistent_path(self):
        """Test handling of nonexistent path."""
        inspector = DatasetInspector(Path("/nonexistent/path"))
        results = inspector.inspect()
        assert 'error' in results
        assert 'does not exist' in results['error'].lower()
    
    def test_empty_directory(self):
        """Test handling of directory with no data files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            inspector = DatasetInspector(Path(tmpdir))
            results = inspector.inspect()
            assert 'error' in results
            assert 'no supported data files' in results['error'].lower()
    
    def test_column_pattern_matching(self):
        """Test column name pattern matching."""
        inspector = DatasetInspector(Path("."))
        
        # Test timestamp patterns
        assert inspector._match_column_pattern("DateTime", inspector.TIMESTAMP_PATTERNS)
        assert inspector._match_column_pattern("timestamp", inspector.TIMESTAMP_PATTERNS)
        assert not inspector._match_column_pattern("price", inspector.TIMESTAMP_PATTERNS)
        
        # Test symbol patterns
        assert inspector._match_column_pattern("Symbol", inspector.SYMBOL_PATTERNS)
        assert inspector._match_column_pattern("TICKER", inspector.SYMBOL_PATTERNS)
        assert not inspector._match_column_pattern("price", inspector.SYMBOL_PATTERNS)
    
    def test_identify_column_types(self):
        """Test column type identification."""
        inspector = DatasetInspector(Path("."))
        
        columns = [
            "DateTime",
            "Symbol",
            "Open",
            "High",
            "Low",
            "Close",
            "Volume",
            "RSI",
            "MACD",
            "SMA_20",
            "UnknownColumn"
        ]
        
        identified = inspector._identify_column_types(columns)
        
        assert "DateTime" in identified['timestamp']
        assert "Symbol" in identified['symbol']
        assert "Open" in identified['open']
        assert "High" in identified['high']
        assert "Low" in identified['low']
        assert "Close" in identified['close']
        assert "Volume" in identified['volume']
        assert 'RSI' in identified['technical_indicators']
        assert 'MACD' in identified['technical_indicators']
        assert "UnknownColumn" in identified['unknown']
    
    def test_csv_file_inspection(self):
        """Test CSV file inspection with sample data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create sample CSV
            csv_path = Path(tmpdir) / "sample.csv"
            df = pd.DataFrame({
                'DateTime': pd.date_range('2024-01-01', periods=100, freq='1min'),
                'Symbol': ['RELIANCE'] * 100,
                'Open': [2400.0 + i for i in range(100)],
                'High': [2410.0 + i for i in range(100)],
                'Low': [2390.0 + i for i in range(100)],
                'Close': [2405.0 + i for i in range(100)],
                'Volume': [100000 + i*1000 for i in range(100)]
            })
            df.to_csv(csv_path, index=False)
            
            # Inspect
            inspector = DatasetInspector(csv_path)
            results = inspector.inspect()
            
            # Verify results
            assert 'files' in results
            assert 'sample.csv' in results['files']
            
            file_data = results['files']['sample.csv']
            assert file_data['rows_total'] == 100
            assert file_data['column_count'] == 7
            assert file_data['has_complete_ohlcv'] == True
            assert 'symbol_analysis' in file_data
            assert file_data['symbol_analysis']['unique_count'] == 1
    
    def test_discover_files(self):
        """Test file discovery."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            
            # Create test files
            (tmppath / "data1.csv").write_text("col1,col2\n1,2\n")
            (tmppath / "data2.csv").write_text("col1,col2\n3,4\n")
            (tmppath / "readme.txt").write_text("not a data file")
            
            # Create subdirectory with file
            subdir = tmppath / "subdir"
            subdir.mkdir()
            (subdir / "data3.csv").write_text("col1,col2\n5,6\n")
            
            inspector = DatasetInspector(tmppath)
            files = inspector._discover_files()
            
            assert len(files) == 3
            assert all(f.suffix == '.csv' for f in files)


def test_path_validation():
    """Test path validation scenarios."""
    # Nonexistent path
    inspector = DatasetInspector(Path("/nonexistent"))
    results = inspector.inspect()
    assert 'error' in results
    
    # Empty directory
    with tempfile.TemporaryDirectory() as tmpdir:
        inspector = DatasetInspector(Path(tmpdir))
        results = inspector.inspect()
        assert 'error' in results


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
