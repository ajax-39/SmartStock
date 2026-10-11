"""
Unit tests for Feature Engineering pipeline.
"""

import os
import sys
import tempfile
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
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from src.features.feature_engineering import FeatureEngineer
from src.preprocessing.clean_data import SparkSessionManager


@pytest.fixture(scope="module")
def spark():
    """Create test SparkSession."""
    session = SparkSessionManager.get_or_create(
        app_name="TestSmartStockFeatures",
        master="local[1]",
        driver_memory="1g",
        executor_memory="1g"
    )
    yield session
    session.stop()


class TestFeatureEngineer:
    """Test suite for feature engineering."""

    @pytest.fixture
    def sample_data(self, spark):
        """Create a synthetic sequence of 70 1-minute bars for a single stock."""
        rows = []
        base_price = 1000.0
        for i in range(70):
            # 09:15 to 10:24
            hour = 9 + (15 + i) // 60
            minute = (15 + i) % 60
            ts_str = f"2024-01-01 {hour:02d}:{minute:02d}:00"
            open_p = base_price + i * 2.0
            high_p = open_p + 3.0
            low_p = open_p - 2.0
            close_p = open_p + 1.0
            vol = 100.0 + i * 10.0
            rows.append((ts_str, "TESTSTOCK", open_p, high_p, low_p, close_p, vol))

        cols = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        df = spark.createDataFrame(rows, cols)
        df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
        return df

    def test_causal_integrity_no_future_leakage(self, spark, sample_data):
        """
        Verify that features at timestamp t depend strictly on data up to t.
        Altering future candles (rows > t) must NOT affect features at candle t.
        """
        engineer = FeatureEngineer(spark)
        feat_df1 = engineer.compute_features(sample_data).persist()

        # Modify future row 60 (close price changed significantly)
        modified_data = sample_data.withColumn(
            "close",
            F.when(F.col("timestamp") == "2024-01-01 10:15:00", 9999.0).otherwise(F.col("close"))
        )
        feat_df2 = engineer.compute_features(modified_data).persist()

        # Inspect row 30 (which is in the past relative to the changed row 60)
        row1 = feat_df1.filter(F.col("timestamp") == "2024-01-01 09:45:00").first().asDict()
        row2 = feat_df2.filter(F.col("timestamp") == "2024-01-01 09:45:00").first().asDict()

        feat_df1.unpersist()
        feat_df2.unpersist()

        # Every feature at row 30 must be completely identical
        for col_name in engineer.FEATURE_COLUMNS:
            v1 = row1[col_name]
            v2 = row2[col_name]
            if v1 is None:
                assert v2 is None, f"Feature {col_name} differed (None vs not None)"
            else:
                assert pytest.approx(v1, rel=1e-5) == v2, f"Feature {col_name} leaked future data!"

    def test_lagged_returns_calculation(self, spark, sample_data):
        """Test that lagged returns match expected mathematical formulation."""
        engineer = FeatureEngineer(spark)
        feat_df = engineer.compute_features(sample_data)

        # Row 0 has close = 1001.0; Row 1 has close = 1003.0; Row 5 has close = 1011.0
        rows = feat_df.collect()

        # Row 0: lag1 is null
        assert rows[0]["return_1m"] is None
        assert rows[0]["return_5m"] is None

        # Row 1: return_1m = (1003.0 / 1001.0) - 1.0
        expected_ret_1m = (1003.0 / 1001.0) - 1.0
        assert pytest.approx(rows[1]["return_1m"], rel=1e-5) == expected_ret_1m

        # Row 5: return_5m = (close_5 / close_0) - 1.0
        expected_ret_5m = (rows[5]["close"] / rows[0]["close"]) - 1.0
        assert pytest.approx(rows[5]["return_5m"], rel=1e-5) == expected_ret_5m

    def test_moving_averages_and_ratios(self, spark, sample_data):
        """Test rolling moving averages use exact row window bounds."""
        engineer = FeatureEngineer(spark)
        feat_df = engineer.compute_features(sample_data)

        rows = feat_df.collect()

        # sma_5 at index 4 (5th bar) should be mean of closes at indices 0, 1, 2, 3, 4
        expected_sma_5 = sum(rows[i]["close"] for i in range(5)) / 5.0
        assert pytest.approx(rows[4]["sma_5"], rel=1e-5) == expected_sma_5

        # close_to_sma_15 = (close / sma_15) - 1.0
        sma_15_val = rows[14]["sma_15"]
        expected_ratio = (rows[14]["close"] / sma_15_val) - 1.0
        assert pytest.approx(rows[14]["close_to_sma_15"], rel=1e-5) == expected_ratio

    def test_momentum_rsi_and_macd(self, spark, sample_data):
        """Test RSI stays in [0, 100] and MACD is computed without errors."""
        engineer = FeatureEngineer(spark)
        feat_df = engineer.compute_features(sample_data)

        rows = feat_df.filter(F.col("rsi_14").isNotNull()).collect()
        for r in rows:
            assert 0.0 <= r["rsi_14"] <= 100.0

        # MACD histogram = macd_line - macd_signal
        macd_rows = feat_df.filter(F.col("macd_signal").isNotNull()).collect()
        assert len(macd_rows) > 0
        for r in macd_rows:
            assert pytest.approx(r["macd_hist"], abs=1e-5) == (r["macd_line"] - r["macd_signal"])

    def test_volatility_and_atr(self, spark, sample_data):
        """Test that volatility, ATR, and normalized ATR are non-negative."""
        engineer = FeatureEngineer(spark)
        feat_df = engineer.compute_features(sample_data)

        valid_atr = feat_df.filter(F.col("atr_14").isNotNull()).collect()
        assert len(valid_atr) > 0
        for r in valid_atr:
            assert r["atr_14"] > 0.0
            assert r["natr_14"] > 0.0
            assert r["hl_spread"] > 0.0

    def test_temporal_and_session_features(self, spark, sample_data):
        """Test time-of-day, day-of-week, and session boundary flags."""
        engineer = FeatureEngineer(spark)
        feat_df = engineer.compute_features(sample_data)

        # First row is 09:15:00
        first_row = feat_df.first()
        assert first_row["hour_of_day"] == 9
        assert first_row["minute_of_day"] == 555
        assert first_row["is_opening_session"] == 1
        assert pytest.approx(first_row["session_progress"], abs=1e-4) == 0.0
