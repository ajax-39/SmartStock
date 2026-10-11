"""
Distributed Feature Engineering for SmartStock using PySpark.

This module computes backward-looking technical, statistical, and temporal features
from cleaned 1-minute stock market OHLCV data.

Design & Look-Ahead Prevention:
- Strictly causal: All features are calculated using historical and contemporaneous
  information available up to the close of candle t (rowsBetween(-N+1, 0) and lag(k)).
- No forward-looking data or future leakage is ever accessible to feature calculations.
- Missing values in early warmup windows (e.g. first 14 or 60 bars of a series)
  are naturally handled without synthetic back-filling.
- Zero driver bottlenecks: aggregations and metrics are computed via distributed Spark SQL.
- Outputs are saved in partitioned Parquet format (partitioned by symbol).

Feature Catalog:
1. Lagged Returns: return_1m, return_5m, return_15m, return_30m, return_60m, log_return_1m
2. Moving Averages & Ratios: sma_5, sma_15, sma_60, sma_375, close_to_sma_15, close_to_sma_60, sma_5_to_sma_15, sma_15_to_sma_60
3. Oscillators & Momentum: rsi_14, macd_line, macd_signal, macd_hist, macd_norm
4. Volatility & Range: volatility_15m, volatility_60m, atr_14, natr_14, hl_spread
5. Volume Dynamics: vma_15, vma_60, volume_ratio_15, volume_ratio_60, volume_change_1m, price_volume_trend
6. Calendar & Session: hour_of_day, minute_of_day, session_progress, day_of_week, is_opening_session, is_closing_session
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent.parent
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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, TimestampType

from src.config import Config, get_config
from src.preprocessing.clean_data import SparkSessionManager


class FeatureEngineer:
    """
    Distributed feature engineering pipeline for stock market time series.
    """

    # Core identification and price columns
    IDENTIFIER_COLUMNS = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]

    # Explicit list of engineered features
    FEATURE_COLUMNS = [
        # Lagged Returns
        "return_1m",
        "return_5m",
        "return_15m",
        "return_30m",
        "return_60m",
        "log_return_1m",
        # Moving Averages & Ratios
        "sma_5",
        "sma_15",
        "sma_60",
        "sma_375",
        "close_to_sma_15",
        "close_to_sma_60",
        "sma_5_to_sma_15",
        "sma_15_to_sma_60",
        # Momentum & Oscillators
        "rsi_14",
        "macd_line",
        "macd_signal",
        "macd_hist",
        "macd_norm",
        # Volatility & Range
        "volatility_15m",
        "volatility_60m",
        "atr_14",
        "natr_14",
        "hl_spread",
        # Volume Dynamics
        "vma_15",
        "vma_60",
        "volume_ratio_15",
        "volume_ratio_60",
        "volume_change_1m",
        "price_volume_trend",
        # Calendar & Session Timing
        "hour_of_day",
        "minute_of_day",
        "session_progress",
        "day_of_week",
        "is_opening_session",
        "is_closing_session"
    ]

    def __init__(self, spark: SparkSession, config: Optional[Config] = None):
        """
        Initialize FeatureEngineer.

        Args:
            spark: Active SparkSession
            config: SmartStock configuration instance
        """
        self.spark = spark
        self.config = config or get_config()

    def read_input(
        self,
        input_path: str,
        subset_stocks: Optional[int] = None
    ) -> DataFrame:
        """
        Read cleaned Parquet dataset.

        Args:
            input_path: Path to cleaned dataset directory or partitioned Parquet
            subset_stocks: Optional limit on stock symbols to process

        Returns:
            Spark DataFrame
        """
        clean_input = input_path.strip().replace("\\", "/")
        df = self.spark.read.parquet(clean_input)

        # If symbol was in partition path and not in dataframe schema, extract from path
        if "symbol" not in df.columns:
            df = df.withColumn(
                "symbol",
                F.regexp_extract(
                    F.input_file_name(),
                    r"symbol=([^/]+)",
                    1
                )
            )

        # Standardize symbol to uppercase
        df = df.withColumn("symbol", F.trim(F.upper(F.col("symbol"))))

        # Filter to a subset of stocks if requested
        if subset_stocks and subset_stocks > 0:
            distinct_symbols = [
                row.symbol for row in df.select("symbol").distinct().limit(subset_stocks).collect()
            ]
            df = df.filter(F.col("symbol").isin(distinct_symbols))

        return df

    def compute_features(self, df: DataFrame) -> DataFrame:
        """
        Compute full catalog of strictly causal technical and temporal features.

        Args:
            df: Cleaned stock DataFrame with canonical schema

        Returns:
            DataFrame with identifiers and all engineered feature columns
        """
        # Ensure timestamp ordering window per stock symbol
        w = Window.partitionBy("symbol").orderBy("timestamp")

        # Rolling row-bounded windows (strictly backward-looking: current row and N-1 preceding rows)
        w_5 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-4, 0)
        w_9 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-8, 0)
        w_12 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-11, 0)
        w_14 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-13, 0)
        w_15 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-14, 0)
        w_26 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-25, 0)
        w_60 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-59, 0)
        w_375 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-374, 0)

        # -------------------------------------------------------------
        # 1. Lagged Returns
        # -------------------------------------------------------------
        lag1_close = F.lag("close", 1).over(w)
        lag5_close = F.lag("close", 5).over(w)
        lag15_close = F.lag("close", 15).over(w)
        lag30_close = F.lag("close", 30).over(w)
        lag60_close = F.lag("close", 60).over(w)

        feat_df = (
            df
            .withColumn("return_1m", (F.col("close") / lag1_close) - 1.0)
            .withColumn("return_5m", (F.col("close") / lag5_close) - 1.0)
            .withColumn("return_15m", (F.col("close") / lag15_close) - 1.0)
            .withColumn("return_30m", (F.col("close") / lag30_close) - 1.0)
            .withColumn("return_60m", (F.col("close") / lag60_close) - 1.0)
            .withColumn(
                "log_return_1m",
                F.when(lag1_close > 0.0, F.log(F.col("close") / lag1_close)).otherwise(None)
            )
        )

        # -------------------------------------------------------------
        # 2. Moving Averages & Moving Average Ratios
        # -------------------------------------------------------------
        feat_df = (
            feat_df
            .withColumn("sma_5", F.avg("close").over(w_5))
            .withColumn("sma_15", F.avg("close").over(w_15))
            .withColumn("sma_60", F.avg("close").over(w_60))
            .withColumn("sma_375", F.avg("close").over(w_375))
            .withColumn("close_to_sma_15", (F.col("close") / F.col("sma_15")) - 1.0)
            .withColumn("close_to_sma_60", (F.col("close") / F.col("sma_60")) - 1.0)
            .withColumn("sma_5_to_sma_15", (F.col("sma_5") / F.col("sma_15")) - 1.0)
            .withColumn("sma_15_to_sma_60", (F.col("sma_15") / F.col("sma_60")) - 1.0)
        )

        # -------------------------------------------------------------
        # 3. Momentum & Oscillators (RSI and MACD)
        # -------------------------------------------------------------
        # 14-period RSI
        price_delta = F.col("close") - lag1_close
        gain = F.when(price_delta > 0.0, price_delta).otherwise(0.0)
        loss = F.when(price_delta < 0.0, -price_delta).otherwise(0.0)

        feat_df = (
            feat_df
            .withColumn("_gain", gain)
            .withColumn("_loss", loss)
            .withColumn("_avg_gain_14", F.avg("_gain").over(w_14))
            .withColumn("_avg_loss_14", F.avg("_loss").over(w_14))
            .withColumn(
                "rsi_14",
                F.when(F.col("_avg_loss_14") == 0.0, 100.0)
                 .otherwise(
                     100.0 - (100.0 / (1.0 + (F.col("_avg_gain_14") / F.col("_avg_loss_14"))))
                 )
            )
            .drop("_gain", "_loss", "_avg_gain_14", "_avg_loss_14")
        )

        # MACD (12, 26, 9)
        macd_fast = F.avg("close").over(w_12)
        macd_slow = F.avg("close").over(w_26)

        feat_df = (
            feat_df
            .withColumn("macd_line", macd_fast - macd_slow)
            .withColumn("macd_signal", F.avg("macd_line").over(w_9))
            .withColumn("macd_hist", F.col("macd_line") - F.col("macd_signal"))
            .withColumn("macd_norm", F.col("macd_line") / F.col("close"))
        )

        # -------------------------------------------------------------
        # 4. Volatility & Range (Realized Volatility, True Range, ATR)
        # -------------------------------------------------------------
        tr_hl = F.col("high") - F.col("low")
        tr_hc = F.abs(F.col("high") - lag1_close)
        tr_lc = F.abs(F.col("low") - lag1_close)
        tr = F.greatest(tr_hl, F.coalesce(tr_hc, tr_hl), F.coalesce(tr_lc, tr_hl))

        feat_df = (
            feat_df
            .withColumn("volatility_15m", F.stddev("return_1m").over(w_15))
            .withColumn("volatility_60m", F.stddev("return_1m").over(w_60))
            .withColumn("_tr", tr)
            .withColumn("atr_14", F.avg("_tr").over(w_14))
            .withColumn("natr_14", F.col("atr_14") / F.col("close"))
            .withColumn("hl_spread", (F.col("high") - F.col("low")) / F.col("close"))
            .drop("_tr")
        )

        # -------------------------------------------------------------
        # 5. Relative Trading Volume & Volume Changes
        # -------------------------------------------------------------
        lag1_vol = F.lag("volume", 1).over(w)

        feat_df = (
            feat_df
            .withColumn("vma_15", F.avg("volume").over(w_15))
            .withColumn("vma_60", F.avg("volume").over(w_60))
            .withColumn(
                "volume_ratio_15",
                F.when(F.col("vma_15") == 0.0, 1.0).otherwise(F.col("volume") / F.col("vma_15"))
            )
            .withColumn(
                "volume_ratio_60",
                F.when(F.col("vma_60") == 0.0, 1.0).otherwise(F.col("volume") / F.col("vma_60"))
            )
            .withColumn(
                "volume_change_1m",
                F.when(
                    (lag1_vol.isNull()) | (lag1_vol == 0.0),
                    0.0
                ).otherwise((F.col("volume") - lag1_vol) / lag1_vol)
            )
            .withColumn("price_volume_trend", F.col("return_1m") * F.col("volume_ratio_15"))
        )

        # -------------------------------------------------------------
        # 6. Time-of-Day & Calendar Features
        # -------------------------------------------------------------
        hour_col = F.hour("timestamp")
        min_col = F.minute("timestamp")
        min_of_day = hour_col * 60 + min_col

        feat_df = (
            feat_df
            .withColumn("hour_of_day", hour_col.cast(IntegerType()))
            .withColumn("minute_of_day", min_of_day.cast(IntegerType()))
            # NSE trading session runs 09:15 (555 min) to 15:30 (930 min) = 375 min
            .withColumn(
                "session_progress",
                F.when(min_of_day < 555, 0.0)
                 .when(min_of_day > 930, 1.0)
                 .otherwise((min_of_day - 555) / 375.0)
            )
            .withColumn("day_of_week", F.dayofweek("timestamp").cast(IntegerType()))
            # Opening auction / first 30 mins (09:15 to 09:45 = min 555 to 585)
            .withColumn("is_opening_session", F.when(min_of_day <= 585, 1).otherwise(0))
            # Closing auction / last 30 mins (15:00 to 15:30 = min 900 to 930)
            .withColumn("is_closing_session", F.when(min_of_day >= 900, 1).otherwise(0))
        )

        # Select columns in canonical order: Identifiers first, then Feature columns
        ordered_cols = self.IDENTIFIER_COLUMNS + [
            c for c in self.FEATURE_COLUMNS if c in feat_df.columns
        ]
        return feat_df.select(*ordered_cols)

    def compute_feature_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Compute distributed feature quality metrics without collecting full data.

        Returns:
            Dictionary of record counts, distinct stocks, and feature null counts
        """
        total_rows = df.count()
        if total_rows == 0:
            return {
                "total_records": 0,
                "distinct_symbols": 0,
                "feature_count": len(self.FEATURE_COLUMNS),
                "null_counts": {},
                "warmup_nulls_percentage": 0.0
            }

        agg_exprs = [
            F.countDistinct("symbol").alias("distinct_symbols"),
            F.min("timestamp").alias("min_timestamp"),
            F.max("timestamp").alias("max_timestamp"),
        ]

        # Aggregate null counts across all feature columns
        for col_name in self.FEATURE_COLUMNS:
            if col_name in df.columns:
                agg_exprs.append(
                    F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"null_{col_name}")
                )

        stats_row = df.select(*agg_exprs).first()
        stats_dict = stats_row.asDict()

        null_counts = {
            col_name: stats_dict.get(f"null_{col_name}", 0) or 0
            for col_name in self.FEATURE_COLUMNS
            if col_name in df.columns
        }

        # Warmup nulls in longest window (sma_375 requires 374 prior rows)
        max_warmup_nulls = max(null_counts.values()) if null_counts else 0

        return {
            "total_records": total_rows,
            "distinct_symbols": stats_dict.get("distinct_symbols", 0),
            "feature_count": len(self.FEATURE_COLUMNS),
            "date_range": {
                "min": str(stats_dict.get("min_timestamp")),
                "max": str(stats_dict.get("max_timestamp"))
            },
            "null_counts": null_counts,
            "max_warmup_nulls_per_feature": max_warmup_nulls,
            "warmup_nulls_percentage": round((max_warmup_nulls / total_rows) * 100.0, 4) if total_rows > 0 else 0.0
        }

    def write_features(
        self,
        df: DataFrame,
        output_path: str,
        overwrite: bool = False
    ):
        """
        Write feature DataFrame partitioned by symbol to Parquet format.
        """
        writer = (
            df.repartition("symbol")
            .write.mode("overwrite" if overwrite else "errorIfExists")
            .partitionBy("symbol")
        )
        writer.parquet(output_path.strip().replace("\\", "/"))

    def run_pipeline(
        self,
        input_path: str,
        output_path: str,
        subset_stocks: Optional[int] = None,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Execute end-to-end feature engineering pipeline.
        """
        start_time = time.time()
        print("=" * 80)
        print("SmartStock - Distributed PySpark Feature Engineering")
        print("=" * 80)
        print(f"Input path:   {input_path}")
        print(f"Output path:  {output_path}")
        print(f"Subset limit: {subset_stocks or 'All available stocks'}")
        print(f"Overwrite:    {overwrite}")

        norm_in = Path(input_path).resolve() if not input_path.startswith("gs://") else input_path
        norm_out = Path(output_path).resolve() if not output_path.startswith("gs://") else output_path
        if norm_in == norm_out:
            raise ValueError(f"Input and output paths cannot be identical: {input_path}")

        print("\n[1/3] Reading cleaned input dataset...")
        df = self.read_input(input_path, subset_stocks=subset_stocks)
        symbols = [row.symbol for row in df.select("symbol").distinct().collect()]
        if subset_stocks and subset_stocks > 0:
            symbols = sorted(symbols)[:subset_stocks]
        else:
            symbols = sorted(symbols)

        clean_out = output_path.strip().replace("\\", "/")
        print(f"[OK] Discovered {len(symbols)} symbol(s) to process.")

        print(f"\n[2/3] Computing and writing features stock-by-stock...")
        for i, sym in enumerate(symbols):
            sym_t0 = time.time()
            stock_df = df.filter(F.col("symbol") == sym)
            stock_feat = self.compute_features(stock_df)
            mode = "overwrite" if (i == 0 and overwrite) else "append"
            stock_feat.write.mode(mode).partitionBy("symbol").parquet(clean_out)
            print(f"  [{i+1}/{len(symbols)}] Processed {sym} in {time.time() - sym_t0:.2f}s")

        print("\n[3/3] Computing distributed feature quality metrics...")
        saved_feat_df = self.spark.read.parquet(clean_out)
        metrics = self.compute_feature_metrics(saved_feat_df)

        duration = time.time() - start_time
        metrics["execution_duration_seconds"] = round(duration, 2)
        metrics["feature_columns"] = self.FEATURE_COLUMNS

        # Save metadata JSON files alongside output
        if not output_path.startswith("gs://"):
            out_dir = Path(output_path)
            meta_path = out_dir.parent / "feature_metadata.json"
            summary_path = out_dir.parent / "feature_quality_summary.json"

            with open(meta_path, "w") as f:
                json.dump(metrics, f, indent=2)
            with open(summary_path, "w") as f:
                json.dump({
                    "total_records": metrics["total_records"],
                    "distinct_stocks": metrics["distinct_symbols"],
                    "feature_count": metrics["feature_count"],
                    "warmup_nulls_pct": metrics["warmup_nulls_percentage"],
                    "execution_seconds": metrics["execution_duration_seconds"]
                }, f, indent=2)

            print(f"[OK] Metadata saved: {meta_path}")
            print(f"[OK] Summary saved:  {summary_path}")

        print("\n" + "=" * 80)
        print("FEATURE ENGINEERING PIPELINE SUMMARY")
        print("=" * 80)
        print(f"Duration:               {duration:.2f} seconds")
        print(f"Total Records:          {metrics['total_records']:,}")
        print(f"Distinct Stocks:        {metrics['distinct_symbols']}")
        print(f"Feature Columns Count:  {metrics['feature_count']}")
        print(f"Max Warmup Nulls:       {metrics['max_warmup_nulls_per_feature']:,} ({metrics['warmup_nulls_percentage']}%)")
        print("=" * 80)

        return metrics


def main():
    parser = argparse.ArgumentParser(
        description="SmartStock - Distributed Feature Engineering using PySpark"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default="data/cleaned/stock_data",
        help="Input directory of cleaned stock Parquet data"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/features/stock_features",
        help="Output directory for engineered feature Parquet dataset"
    )
    parser.add_argument(
        "--subset-stocks",
        type=int,
        default=None,
        help="Optional limit on stock symbols to process"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite destination directory if it exists"
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="SmartStock-FeatureEngineering",
        help="Spark application name"
    )
    parser.add_argument(
        "--master",
        type=str,
        default=None,
        help="Spark master URL (default: local[*])"
    )

    args = parser.parse_args()

    spark = SparkSessionManager.get_or_create(
        app_name=args.app_name,
        master=args.master
    )

    try:
        engineer = FeatureEngineer(spark)
        engineer.run_pipeline(
            input_path=args.input_dir,
            output_path=args.output_dir,
            subset_stocks=args.subset_stocks,
            overwrite=args.overwrite
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
