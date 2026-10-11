"""
Unit tests for Target Label Generation pipeline.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import pytest

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Configure Hadoop on Windows
hadoop_dir = project_root / "hadoop"
if hadoop_dir.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_dir.resolve())
    hadoop_bin = str((hadoop_dir / "bin").resolve())
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.labels.create_labels import LabelGenerator
from src.preprocessing.clean_data import SparkSessionManager


@pytest.fixture(scope="module")
def spark():
    """Create test SparkSession."""
    session = SparkSessionManager.get_or_create(
        app_name="TestSmartStockLabels",
        master="local[1]",
        driver_memory="1g",
        executor_memory="1g"
    )
    yield session
    session.stop()


class TestLabelGenerator:
    """Test suite for target label generation."""

    def test_entry_price_execution_timing(self, spark):
        """
        Verify entry price is the open of candle t+1 (realistic execution).
        """
        data = [
            ("2024-01-01 09:15:00", "STOCKA", 100.0, 105.0, 99.0, 102.0, 50.0),
            ("2024-01-01 09:16:00", "STOCKA", 103.0, 106.0, 102.0, 104.0, 60.0),
            ("2024-01-01 09:17:00", "STOCKA", 105.0, 108.0, 104.0, 107.0, 70.0),
        ]
        cols = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        df = spark.createDataFrame(data, cols).withColumn("timestamp", F.to_timestamp("timestamp"))

        generator = LabelGenerator(spark)
        labels = generator.create_labels(df).collect()

        # Row 0 entry price must be row 1 open (103.0)
        assert labels[0]["entry_price"] == 103.0
        # Row 1 entry price must be row 2 open (105.0)
        assert labels[1]["entry_price"] == 105.0
        # Row 2 (terminal) has no next bar -> entry_price is null
        assert labels[2]["entry_price"] is None

    def test_hurdle_rate_label_assignment(self, spark):
        """
        Verify favorable label = 1 when forward return exceeds hurdle, 0 otherwise.
        """
        # Create a series where return is +1.0% in row 0 and -0.5% in row 1
        rows = []
        for i in range(70):
            ts = f"2024-01-01 09:{15+i:02d}:00" if i < 45 else f"2024-01-01 10:{i-45:02d}:00"
            open_p = 100.0
            close_p = 100.0
            rows.append((ts, "STOCKB", open_p, open_p + 1, open_p - 1, close_p, 100.0))

        # At row 61 (which is exit for row 0), set close to 100.50 (+0.5% return vs entry 100.0)
        rows[61] = ("2024-01-01 10:16:00", "STOCKB", 100.0, 101.0, 99.0, 100.50, 100.0)

        cols = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        df = spark.createDataFrame(rows, cols).withColumn("timestamp", F.to_timestamp("timestamp"))

        # Hurdle = 0.0020 (+0.20%)
        generator = LabelGenerator(spark, hurdle_60m=0.0020)
        labels = generator.create_labels(df).collect()

        # Row 0: entry = 100.0, exit = 100.50, ret = +0.5% > +0.2% hurdle -> label = 1
        assert pytest.approx(labels[0]["forward_return_60m"], rel=1e-4) == 0.0050
        assert labels[0]["target_favorable_60m"] == 1
        assert labels[0]["has_valid_outcome_60m"] is True

    def test_20d_session_based_calculation(self, spark):
        """
        Verify that 20 trading days are calculated using 20 completed daily sessions,
        not 20 minute rows.
        """
        # Generate 25 distinct trading days with 5 minutes each
        base_date = datetime(2024, 1, 1)
        rows = []
        for d in range(25):
            cur_date = base_date + timedelta(days=d)
            date_str = cur_date.strftime("%Y-%m-%d")
            # Daily price trends upwards: day d close is 100 + d * 2
            for m in range(5):
                ts_str = f"{date_str} 09:{15+m:02d}:00"
                open_p = 100.0 + d * 2.0
                close_p = open_p + 1.0  # Session close will be 100 + d*2 + 1
                rows.append((ts_str, "SESSIONSTOCK", open_p, open_p + 2, open_p - 1, close_p, 100.0))

        cols = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        df = spark.createDataFrame(rows, cols).withColumn("timestamp", F.to_timestamp("timestamp"))

        generator = LabelGenerator(spark, hurdle_20d=0.0150)
        labeled_df = generator.create_labels(df)

        # Inspect day 0, first candle
        day0_candle = labeled_df.filter(F.col("timestamp") == "2024-01-01 09:15:00").first()
        # Day 20 session close: d = 20 -> close_p = 100 + 20*2 + 1 = 141.0
        assert day0_candle["exit_price_20d"] == 141.0
        assert day0_candle["has_valid_outcome_20d"] is True
        assert day0_candle["target_favorable_20d"] == 1

        # Day 24 is the last day (d=24). 20 days forward does not exist -> exit_price_20d must be null!
        day24_candle = labeled_df.filter(F.col("timestamp") == "2024-01-25 09:15:00").first()
        assert day24_candle["exit_price_20d"] is None
        assert day24_candle["has_valid_outcome_20d"] is False
        assert day24_candle["target_favorable_20d"] is None

    def test_target_separation_from_features(self, spark):
        """
        Verify that target-label DataFrame contains only key and target columns,
        ensuring strict physical separation from feature sets.
        """
        data = [
            ("2024-01-01 09:15:00", "SEPARATE", 100.0, 105.0, 99.0, 102.0, 50.0),
        ]
        cols = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        df = spark.createDataFrame(data, cols).withColumn("timestamp", F.to_timestamp("timestamp"))

        generator = LabelGenerator(spark)
        labeled_df = generator.create_labels(df)

        # Columns should be keys + targets, NOT feature columns like rsi, sma, etc.
        assert "timestamp" in labeled_df.columns
        assert "symbol" in labeled_df.columns
        assert "target_favorable_60m" in labeled_df.columns
        assert "target_favorable_20d" in labeled_df.columns
        assert "rsi_14" not in labeled_df.columns
        assert "sma_15" not in labeled_df.columns
