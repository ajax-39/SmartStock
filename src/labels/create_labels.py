"""
Target Label Generation for SmartStock using PySpark.

This module creates forward-looking target labels for supervised learning models,
strictly separating label targets from model features to eliminate data leakage.

Key Design Principles:
1. Realistic Execution Timing:
   Decision is made at the close of candle t. Order entry executes at candle t+1 open:
   entry_price = lead(open, 1).
2. Dual Horizons:
   - 60-Minute Intraday Horizon: exit at candle t+61 close (60 minutes post-execution).
   - 20-Trading-Day Horizon: exit at the closing price of trading session D+20
     (20 completed trading sessions, avoiding 20-row minute distortion).
3. Transaction Cost Hurdles:
   - 60-minute hurdle: 0.0020 (+0.20% round-trip friction, STT, brokerage, slippage).
   - 20-day hurdle: 0.0150 (+1.50% multi-week risk-free + cost hurdle).
4. Boundary Safety:
   Observations near the end of the historical series where future outcomes are
   not yet realized are marked as null and flagged (has_valid_outcome_* = False).
5. Output Separation:
   Labels are written to an isolated table partitioned by symbol.
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
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, StringType, TimestampType

from src.config import Config, get_config
from src.preprocessing.clean_data import SparkSessionManager


class LabelGenerator:
    """
    Distributed target-label generator for stock market predictions.
    """

    # Identifiers linking labels back to candles/features
    KEY_COLUMNS = ["timestamp", "symbol"]

    # Target-specific columns generated
    TARGET_COLUMNS = [
        "entry_price",
        # 60-Minute Horizon
        "exit_price_60m",
        "forward_return_60m",
        "target_favorable_60m",
        "has_valid_outcome_60m",
        # 20-Trading-Day Horizon
        "exit_price_20d",
        "forward_return_20d",
        "target_favorable_20d",
        "has_valid_outcome_20d"
    ]

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[Config] = None,
        hurdle_60m: float = 0.0020,
        hurdle_20d: float = 0.0150
    ):
        """
        Initialize LabelGenerator.

        Args:
            spark: Active SparkSession
            config: SmartStock configuration instance
            hurdle_60m: Return threshold for 60-minute favorable label (default: 0.20%)
            hurdle_20d: Return threshold for 20-day favorable label (default: 1.50%)
        """
        self.spark = spark
        self.config = config or get_config()
        self.hurdle_60m = hurdle_60m
        self.hurdle_20d = hurdle_20d

    def read_input(
        self,
        input_path: str,
        subset_stocks: Optional[int] = None
    ) -> DataFrame:
        """
        Read input stock dataset from cleaned Parquet or feature Parquet.
        """
        clean_input = input_path.strip().replace("\\", "/")
        df = self.spark.read.parquet(clean_input)

        if "symbol" not in df.columns:
            df = df.withColumn(
                "symbol",
                F.regexp_extract(
                    F.input_file_name(),
                    r"symbol=([^/]+)",
                    1
                )
            )

        df = df.withColumn("symbol", F.trim(F.upper(F.col("symbol"))))

        if subset_stocks and subset_stocks > 0:
            distinct_symbols = [
                row.symbol for row in df.select("symbol").distinct().limit(subset_stocks).collect()
            ]
            df = df.filter(F.col("symbol").isin(distinct_symbols))

        return df

    def create_labels(self, df: DataFrame) -> DataFrame:
        """
        Compute forward returns and binary favorable-return target labels.

        Args:
            df: DataFrame containing at least [timestamp, symbol, open, close]

        Returns:
            DataFrame containing KEY_COLUMNS and TARGET_COLUMNS
        """
        w_symbol_time = Window.partitionBy("symbol").orderBy("timestamp")

        # 1. Entry price: Realistic trade fill at the open of the candle immediately
        # following the decision candle (bar t+1 open).
        entry_price = F.lead("open", 1).over(w_symbol_time)

        # -----------------------------------------------------------------
        # 2. Intraday 60-Minute Horizon
        # Exit at candle t+61 close (60 minutes following execution at t+1 open)
        # -----------------------------------------------------------------
        exit_price_60m = F.lead("close", 61).over(w_symbol_time)
        fwd_ret_60m = (exit_price_60m - entry_price) / entry_price

        valid_outcome_60m = (entry_price.isNotNull()) & (exit_price_60m.isNotNull())

        target_60m = F.when(
            valid_outcome_60m,
            F.when(fwd_ret_60m > self.hurdle_60m, 1).otherwise(0)
        ).otherwise(None)

        labeled_df = (
            df
            .withColumn("entry_price", entry_price)
            .withColumn("exit_price_60m", exit_price_60m)
            .withColumn("forward_return_60m", F.when(valid_outcome_60m, fwd_ret_60m).otherwise(None))
            .withColumn("target_favorable_60m", target_60m.cast(IntegerType()))
            .withColumn("has_valid_outcome_60m", valid_outcome_60m.cast(BooleanType()))
        )

        # -----------------------------------------------------------------
        # 3. 20-Trading-Day Horizon (Completed Trading Sessions)
        # -----------------------------------------------------------------
        # Compute trading date for each candle
        labeled_df = labeled_df.withColumn("_trading_date", F.to_date("timestamp"))

        # Find the last closing price of each distinct trading session per symbol
        w_daily_last = (
            Window
            .partitionBy("symbol", "_trading_date")
            .orderBy("timestamp")
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

        daily_sessions = (
            labeled_df
            .select("symbol", "_trading_date", "close", "timestamp")
            .withColumn("_session_close", F.last("close").over(w_daily_last))
            .select("symbol", "_trading_date", "_session_close")
            .distinct()
        )

        # Shift forward by 20 completed trading sessions
        w_session_order = Window.partitionBy("symbol").orderBy("_trading_date")
        daily_sessions_shifted = (
            daily_sessions
            .withColumn("exit_date_20d", F.lead("_trading_date", 20).over(w_session_order))
            .withColumn("exit_price_20d", F.lead("_session_close", 20).over(w_session_order))
            .select("symbol", "_trading_date", "exit_price_20d")
        )

        # Join the 20-day session exit price back to the minute bars
        labeled_df = labeled_df.join(
            daily_sessions_shifted,
            on=["symbol", "_trading_date"],
            how="left"
        )

        fwd_ret_20d = (F.col("exit_price_20d") - F.col("entry_price")) / F.col("entry_price")
        valid_outcome_20d = (
            F.col("entry_price").isNotNull() &
            F.col("exit_price_20d").isNotNull()
        )

        target_20d = F.when(
            valid_outcome_20d,
            F.when(fwd_ret_20d > self.hurdle_20d, 1).otherwise(0)
        ).otherwise(None)

        labeled_df = (
            labeled_df
            .withColumn("forward_return_20d", F.when(valid_outcome_20d, fwd_ret_20d).otherwise(None))
            .withColumn("target_favorable_20d", target_20d.cast(IntegerType()))
            .withColumn("has_valid_outcome_20d", valid_outcome_20d.cast(BooleanType()))
            .drop("_trading_date")
        )

        # Select only canonical identifiers + target label columns
        ordered_cols = self.KEY_COLUMNS + self.TARGET_COLUMNS
        return labeled_df.select(*ordered_cols)

    def compute_label_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Compute distributed label distribution statistics and outcome coverage.
        """
        total_rows = df.count()
        if total_rows == 0:
            return {
                "total_records": 0,
                "horizons": {}
            }

        agg_exprs = [
            F.countDistinct("symbol").alias("distinct_symbols"),
            F.min("timestamp").alias("min_timestamp"),
            F.max("timestamp").alias("max_timestamp"),
            # 60m Metrics
            F.sum(F.when(F.col("has_valid_outcome_60m"), 1).otherwise(0)).alias("valid_count_60m"),
            F.sum(F.when(F.col("target_favorable_60m") == 1, 1).otherwise(0)).alias("positive_count_60m"),
            F.sum(F.when(F.col("target_favorable_60m") == 0, 1).otherwise(0)).alias("negative_count_60m"),
            F.avg(F.col("forward_return_60m")).alias("mean_return_60m"),
            # 20d Metrics
            F.sum(F.when(F.col("has_valid_outcome_20d"), 1).otherwise(0)).alias("valid_count_20d"),
            F.sum(F.when(F.col("target_favorable_20d") == 1, 1).otherwise(0)).alias("positive_count_20d"),
            F.sum(F.when(F.col("target_favorable_20d") == 0, 1).otherwise(0)).alias("negative_count_20d"),
            F.avg(F.col("forward_return_20d")).alias("mean_return_20d"),
        ]

        row = df.select(*agg_exprs).first().asDict()

        valid_60m = row["valid_count_60m"] or 0
        pos_60m = row["positive_count_60m"] or 0
        neg_60m = row["negative_count_60m"] or 0

        valid_20d = row["valid_count_20d"] or 0
        pos_20d = row["positive_count_20d"] or 0
        neg_20d = row["negative_count_20d"] or 0

        return {
            "total_records": total_rows,
            "distinct_symbols": row["distinct_symbols"],
            "date_range": {
                "min": str(row["min_timestamp"]),
                "max": str(row["max_timestamp"])
            },
            "horizons": {
                "60_minute": {
                    "hurdle_threshold": self.hurdle_60m,
                    "valid_outcomes_count": valid_60m,
                    "unrealized_terminal_count": total_rows - valid_60m,
                    "coverage_pct": round((valid_60m / total_rows * 100.0), 2) if total_rows > 0 else 0.0,
                    "positive_labels": pos_60m,
                    "negative_labels": neg_60m,
                    "positive_class_percentage": round((pos_60m / valid_60m * 100.0), 2) if valid_60m > 0 else 0.0,
                    "mean_forward_return": round(float(row["mean_return_60m"] or 0.0), 6)
                },
                "20_trading_days": {
                    "hurdle_threshold": self.hurdle_20d,
                    "valid_outcomes_count": valid_20d,
                    "unrealized_terminal_count": total_rows - valid_20d,
                    "coverage_pct": round((valid_20d / total_rows * 100.0), 2) if total_rows > 0 else 0.0,
                    "positive_labels": pos_20d,
                    "negative_labels": neg_20d,
                    "positive_class_percentage": round((pos_20d / valid_20d * 100.0), 2) if valid_20d > 0 else 0.0,
                    "mean_forward_return": round(float(row["mean_return_20d"] or 0.0), 6)
                }
            }
        }

    def write_labels(
        self,
        df: DataFrame,
        output_path: str,
        overwrite: bool = False
    ):
        """
        Write target label DataFrame partitioned by symbol to Parquet format.
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
        Execute end-to-end target labeling pipeline.
        """
        start_time = time.time()
        print("=" * 80)
        print("SmartStock - Distributed PySpark Target Label Generation")
        print("=" * 80)
        print(f"Input path:        {input_path}")
        print(f"Output path:       {output_path}")
        print(f"Hurdle (60m):      {self.hurdle_60m * 100.0:.2f}%")
        print(f"Hurdle (20d):      {self.hurdle_20d * 100.0:.2f}%")
        print(f"Subset limit:      {subset_stocks or 'All available stocks'}")
        print(f"Overwrite:         {overwrite}")

        norm_in = Path(input_path).resolve() if not input_path.startswith("gs://") else input_path
        norm_out = Path(output_path).resolve() if not output_path.startswith("gs://") else output_path
        if norm_in == norm_out:
            raise ValueError(f"Input and output paths cannot be identical: {input_path}")

        print("\n[1/3] Reading cleaned dataset...")
        df = self.read_input(input_path, subset_stocks=subset_stocks)
        symbols = [row.symbol for row in df.select("symbol").distinct().collect()]
        if subset_stocks and subset_stocks > 0:
            symbols = sorted(symbols)[:subset_stocks]
        else:
            symbols = sorted(symbols)

        clean_out = output_path.strip().replace("\\", "/")
        print(f"[OK] Discovered {len(symbols)} symbol(s) to process.")

        print(f"\n[2/3] Generating multi-horizon forward returns and hurdle target labels stock-by-stock...")
        for i, sym in enumerate(symbols):
            sym_t0 = time.time()
            stock_df = df.filter(F.col("symbol") == sym)
            stock_labels = self.create_labels(stock_df)
            mode = "overwrite" if (i == 0 and overwrite) else "append"
            stock_labels.write.mode(mode).partitionBy("symbol").parquet(clean_out)
            print(f"  [{i+1}/{len(symbols)}] Generated labels for {sym} in {time.time() - sym_t0:.2f}s")

        print("\n[3/3] Computing distributed label distribution metrics...")
        saved_labels_df = self.spark.read.parquet(clean_out)
        metrics = self.compute_label_metrics(saved_labels_df)

        duration = time.time() - start_time
        metrics["execution_duration_seconds"] = round(duration, 2)
        metrics["target_columns"] = self.TARGET_COLUMNS

        # Save metadata JSON files alongside output
        if not output_path.startswith("gs://"):
            out_dir = Path(output_path)
            meta_path = out_dir.parent / "label_metadata.json"
            summary_path = out_dir.parent / "label_quality_summary.json"

            with open(meta_path, "w") as f:
                json.dump(metrics, f, indent=2)
            with open(summary_path, "w") as f:
                json.dump({
                    "total_records": metrics["total_records"],
                    "distinct_stocks": metrics["distinct_symbols"],
                    "horizons": metrics["horizons"],
                    "execution_seconds": metrics["execution_duration_seconds"]
                }, f, indent=2)

            print(f"[OK] Metadata saved: {meta_path}")
            print(f"[OK] Summary saved:  {summary_path}")

        print("\n" + "=" * 80)
        print("TARGET LABEL GENERATION SUMMARY")
        print("=" * 80)
        print(f"Duration:               {duration:.2f} seconds")
        print(f"Total Records:          {metrics['total_records']:,}")
        print(f"Distinct Stocks:        {metrics['distinct_symbols']}")
        h60 = metrics["horizons"]["60_minute"]
        h20 = metrics["horizons"]["20_trading_days"]
        print(f"60m Horizon:            {h60['valid_outcomes_count']:,} valid ({h60['coverage_pct']}%) | Positives: {h60['positive_labels']:,} ({h60['positive_class_percentage']}%)")
        print(f"20d Horizon:            {h20['valid_outcomes_count']:,} valid ({h20['coverage_pct']}%) | Positives: {h20['positive_labels']:,} ({h20['positive_class_percentage']}%)")
        print("=" * 80)

        return metrics


def main():
    parser = argparse.ArgumentParser(
        description="SmartStock - Distributed Target Label Generation using PySpark"
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
        default="data/labels/stock_labels",
        help="Output directory for target label Parquet dataset"
    )
    parser.add_argument(
        "--hurdle-60m",
        type=float,
        default=0.0020,
        help="Hurdle rate for 60-minute favorable return (default: 0.002 = 0.2%)"
    )
    parser.add_argument(
        "--hurdle-20d",
        type=float,
        default=0.0150,
        help="Hurdle rate for 20-trading-day favorable return (default: 0.015 = 1.5%)"
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
        default="SmartStock-LabelGeneration",
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
        generator = LabelGenerator(
            spark,
            hurdle_60m=args.hurdle_60m,
            hurdle_20d=args.hurdle_20d
        )
        generator.run_pipeline(
            input_path=args.input_dir,
            output_path=args.output_dir,
            subset_stocks=args.subset_stocks,
            overwrite=args.overwrite
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
