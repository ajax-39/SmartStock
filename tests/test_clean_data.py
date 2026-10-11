"""
Tests for PySpark distributed preprocessing and cleaning pipeline.
"""

import os
import sys
import tempfile
from pathlib import Path
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure Hadoop on Windows for native Spark local filesystem operations
hadoop_dir = project_root / "hadoop"
if hadoop_dir.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_dir.resolve())
    hadoop_bin = str((hadoop_dir / "bin").resolve())
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

# Ensure Windows worker subprocesses use the active virtual environment Python
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.preprocessing.clean_data import DataCleaner, SparkSessionManager
from src.config import get_config


@pytest.fixture(scope="module")
def spark():
    """Create a test SparkSession fixture."""
    session = SparkSessionManager.get_or_create(
        app_name="TestSmartStockCleaning",
        master="local[1]",
        driver_memory="1g",
        executor_memory="1g"
    )
    yield session
    session.stop()


class TestDataCleaner:
    """Test suite for PySpark DataCleaner."""

    def test_duplicate_handling(self, spark):
        """Test that duplicate candles by (symbol, timestamp) are eliminated."""
        cleaner = DataCleaner(spark)

        # 3 rows, with 2 having identical (symbol, timestamp)
        data = [
            ("2024-01-01 09:15:00", "RELIANCE", 2500.0, 2510.0, 2490.0, 2505.0, 100.0),
            ("2024-01-01 09:15:00", "RELIANCE", 2500.0, 2510.0, 2490.0, 2505.0, 100.0), # duplicate
            ("2024-01-01 09:16:00", "RELIANCE", 2505.0, 2515.0, 2500.0, 2510.0, 200.0),
        ]
        columns = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        df = spark.createDataFrame(data, columns)

        cleaned_df = cleaner.clean(df)
        assert cleaned_df.count() == 2

    def test_invalid_ohlc_filtering(self, spark):
        """Test that invalid OHLC relationships and non-positive prices are rejected."""
        cleaner = DataCleaner(spark)

        data = [
            # Valid record
            ("2024-01-01 09:15:00", "TCS", 3500.0, 3520.0, 3490.0, 3510.0, 100.0),
            # Invalid: high < low
            ("2024-01-01 09:16:00", "TCS", 3500.0, 3480.0, 3510.0, 3500.0, 100.0),
            # Invalid: high < open
            ("2024-01-01 09:17:00", "TCS", 3520.0, 3510.0, 3490.0, 3500.0, 100.0),
            # Invalid: low > close
            ("2024-01-01 09:18:00", "TCS", 3500.0, 3510.0, 3490.0, 3480.0, 100.0),
            # Invalid: zero or negative price
            ("2024-01-01 09:19:00", "TCS", 0.0, 3510.0, 3490.0, 3500.0, 100.0),
            ("2024-01-01 09:20:00", "TCS", -100.0, 3510.0, 3490.0, 3500.0, 100.0),
        ]
        columns = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        df = spark.createDataFrame(data, columns)

        cleaned_df = cleaner.clean(df)
        assert cleaned_df.count() == 1
        row = cleaned_df.first()
        assert row.symbol == "TCS"
        assert row.open == 3500.0

    def test_missing_values_and_zero_volume_preservation(self, spark):
        """Test handling of nulls and preservation of genuine zero volume."""
        cleaner = DataCleaner(spark)

        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", DoubleType(), True),
        ])

        data = [
            # Valid with genuine 0.0 volume
            ("2024-01-01 09:15:00", "INFY", 1500.0, 1510.0, 1490.0, 1505.0, 0.0),
            # Null volume -> should impute to 0.0
            ("2024-01-01 09:16:00", "INFY", 1505.0, 1515.0, 1500.0, 1510.0, None),
            # Null open -> should be dropped
            ("2024-01-01 09:17:00", "INFY", None, 1515.0, 1500.0, 1510.0, 100.0),
            # Null timestamp -> should be dropped
            (None, "INFY", 1505.0, 1515.0, 1500.0, 1510.0, 100.0),
        ]
        df = spark.createDataFrame(data, schema)

        cleaned_df = cleaner.clean(df)
        assert cleaned_df.count() == 2

        volumes = [r.volume for r in cleaned_df.collect()]
        assert volumes == [0.0, 0.0]

    def test_timestamp_normalization(self, spark):
        """Test timestamp parsing and normalization to TimestampType from strings."""
        cleaner = DataCleaner(spark)

        data = [
            ("2024-01-01 09:15:00", "HDFC", 1600.0, 1610.0, 1590.0, 1605.0, 50.0),
        ]
        columns = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        df = spark.createDataFrame(data, columns)

        cleaned_df = cleaner.clean(df)
        assert isinstance(cleaned_df.schema["timestamp"].dataType, TimestampType)

    def test_nanosecond_epoch_timestamp_normalization(self, spark):
        """Test nanosecond epoch integer timestamp normalization to TimestampType."""
        cleaner = DataCleaner(spark)

        # 1704100500000000000 ns = 2024-01-01 09:15:00 UTC
        data = [
            (1704100500000000000, "WIPRO", 450.0, 455.0, 448.0, 452.0, 100.0),
        ]
        columns = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        df = spark.createDataFrame(data, columns)

        cleaned_df = cleaner.clean(df)
        assert isinstance(cleaned_df.schema["timestamp"].dataType, TimestampType)
        assert cleaned_df.count() == 1

    def test_end_to_end_pipeline(self, spark):
        """Test end-to-end cleaning pipeline writing partitioned Parquet with metadata."""
        with tempfile.TemporaryDirectory() as in_dir, tempfile.TemporaryDirectory() as out_dir:
            # Create sample source parquet file
            data = [
                ("2024-01-01 09:15:00", "SBIN", 600.0, 605.0, 595.0, 602.0, 100.0),
                ("2024-01-01 09:16:00", "SBIN", 602.0, 608.0, 600.0, 606.0, 200.0),
                ("2024-01-01 09:15:00", "ITC", 400.0, 405.0, 395.0, 402.0, 500.0),
            ]
            columns = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
            raw_df = spark.createDataFrame(data, columns)
            raw_df.write.partitionBy("symbol").parquet(str(Path(in_dir) / "stock_data"))

            cleaner = DataCleaner(spark)
            metadata = cleaner.run_pipeline(
                input_path=str(Path(in_dir) / "stock_data"),
                output_path=str(Path(out_dir) / "stock_data"),
                overwrite=True
            )

            assert metadata["summary"]["raw_records"] == 3
            assert metadata["summary"]["cleaned_records"] == 3
            assert metadata["summary"]["distinct_stocks"] == 2

            # Check that partitioned output folders exist
            out_stock_data = Path(out_dir) / "stock_data"
            assert (out_stock_data / "symbol=SBIN").exists()
            assert (out_stock_data / "symbol=ITC").exists()

            # Check metadata files
            assert (Path(out_dir) / "cleaning_metadata.json").exists()
            assert (Path(out_dir) / "cleaning_quality_summary.json").exists()
