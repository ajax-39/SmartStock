"""
Distributed Data Cleaning & Preprocessing for SmartStock using PySpark.

This module cleans and standardizes stock market time-series data using Apache Spark,
supporting local execution and cloud deployment (Google Cloud Storage / Dataproc).

Features:
- Configurable SparkSession with optimized local/cluster memory and shuffle partitions.
- Dual format support: Ingested partitioned Parquet (default) or raw CSVs with auto-schema detection.
- Normalization: Enforces canonical schema (timestamp, symbol, open, high, low, close, volume).
- Deduplication: Eliminates duplicate candles by (symbol, timestamp).
- Strict Validation: Enforces OHLC geometry (high >= low, open, close) and strictly positive prices.
- Preserves Reality: Keeps genuine zero-volume candles distinct from missing records; no blind forward-filling.
- Zero Driver Bottlenecks: Computes distributed data quality metrics without .collect() or .toPandas().
- Hive Partitioning: Writes cleaned records partitioned by symbol (symbol=<SYMBOL>/part-*.parquet).
- Safety & Idempotency: Guards against accidental source overwriting; supports --overwrite.

Usage:
    python src/preprocessing/clean_data.py
    python src/preprocessing/clean_data.py --input-dir data/processed/stock_data --output-dir data/cleaned/stock_data
    python src/preprocessing/clean_data.py --subset-stocks 5 --overwrite
"""

import argparse
import json
import os
import re
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
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.config import Config, get_config


class SparkSessionManager:
    """Manages creation and lifecycle of SparkSession for SmartStock."""

    @staticmethod
    def get_or_create(
        app_name: str = "SmartStock-Preprocessing",
        master: Optional[str] = None,
        driver_memory: str = "4g",
        executor_memory: str = "4g",
        is_cloud: bool = False,
        log_level: str = "WARN"
    ) -> SparkSession:
        """
        Create or get an active SparkSession with optimized configurations.

        Args:
            app_name: Name of Spark application
            master: Spark master URL (defaults to local[*] for local execution)
            driver_memory: Driver memory allocation
            executor_memory: Executor memory allocation
            is_cloud: Whether running against cloud storage (GCS/Dataproc)
            log_level: Logging verbosity (WARN, ERROR, INFO)

        Returns:
            Configured SparkSession instance
        """
        builder = SparkSession.builder.appName(app_name)

        if master:
            builder = builder.master(master)
        else:
            builder = builder.master("local[*]")

        # Performance optimizations for time-series workloads
        builder = (
            builder
            .config("spark.driver.memory", driver_memory)
            .config("spark.executor.memory", executor_memory)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.parquet.enableDictionary", "false")
            .config("parquet.enable.dictionary", "false")
            .config("spark.sql.legacy.parquet.nanosAsLong", "true")
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
            .config("spark.ui.enabled", "false")
        )

        # Configure Google Cloud Storage connector if executing in cloud
        if is_cloud:
            builder = (
                builder
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
                .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            )

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(log_level)
        return spark


class DataCleaner:
    """
    Distributed data cleaning pipeline for historical stock market data.
    """

    # Canonical columns expected for feature engineering & modeling
    CANONICAL_COLUMNS = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]

    def __init__(self, spark: SparkSession, config: Optional[Config] = None):
        """
        Initialize cleaner with SparkSession and application configuration.

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
    ) -> Tuple[DataFrame, str]:
        """
        Read dataset from partitioned Parquet or raw CSV files.

        Args:
            input_path: Directory or file path / cloud URI
            subset_stocks: Optional limit on stock symbols to process

        Returns:
            Tuple of (DataFrame, format_detected)
        """
        clean_input = input_path.strip().replace("\\", "/")
        is_cloud = clean_input.startswith("gs://") or clean_input.startswith("s3://")

        # Check local path existence if not cloud URI
        if not is_cloud:
            local_p = Path(clean_input)
            if not local_p.exists():
                raise FileNotFoundError(f"Input path does not exist: {clean_input}")

        # Detect format: check for parquet part files or directory
        has_parquet = False
        has_csv = False

        if not is_cloud:
            local_p = Path(clean_input)
            if local_p.is_file():
                has_parquet = local_p.suffix.lower() in ['.parquet', '.pq']
                has_csv = local_p.suffix.lower() == '.csv'
            else:
                has_parquet = any(local_p.rglob("*.parquet")) or any(local_p.rglob("*.pq"))
                has_csv = any(local_p.rglob("*.csv"))
        else:
            has_parquet = "parquet" in clean_input.lower()
            has_csv = "csv" in clean_input.lower()

        if has_parquet or (not has_csv):
            fmt = "parquet"
            df = self.spark.read.parquet(clean_input)
        else:
            fmt = "csv"
            df = self.spark.read.option("header", "true").option("inferSchema", "true").csv(clean_input)

        # Standardize column names to lower case
        for col_name in df.columns:
            clean_name = col_name.strip().lower()
            if clean_name != col_name:
                df = df.withColumnRenamed(col_name, clean_name)

        # Align column names to canonical names
        col_mappings = {
            "date": "timestamp",
            "datetime": "timestamp",
            "time": "timestamp",
            "dt": "timestamp",
            "ticker": "symbol",
            "stock": "symbol",
            "vol": "volume",
            "qty": "volume"
        }
        for old_col, new_col in col_mappings.items():
            if old_col in df.columns and new_col not in df.columns:
                df = df.withColumnRenamed(old_col, new_col)

        # Extract symbol from filename if missing from dataframe columns
        if "symbol" not in df.columns:
            # Use Spark input_file_name() to derive symbol
            df = df.withColumn(
                "symbol",
                F.regexp_extract(
                    F.input_file_name(),
                    r"([A-Za-z0-9_-]+)(?:_minute|_1min|_1m|_data)?\.[a-zA-Z]+$",
                    1
                )
            )

        # Filter to a subset of stocks if requested
        if subset_stocks and subset_stocks > 0:
            distinct_symbols = [
                row.symbol for row in df.select("symbol").distinct().limit(subset_stocks).collect()
            ]
            df = df.filter(F.col("symbol").isin(distinct_symbols))

        return df, fmt

    def compute_data_quality_metrics(self, df: DataFrame) -> Dict[str, Any]:
        """
        Compute distributed data quality metrics using Spark SQL aggregations
        without bringing full dataset to driver.
        """
        total_rows = df.count()
        if total_rows == 0:
            return {
                "total_records": 0,
                "distinct_symbols": 0,
                "null_counts": {},
                "invalid_ohlc_records": 0,
                "negative_prices": 0,
                "zero_volume_records": 0,
                "date_range": None
            }

        # Select basic aggregations
        agg_exprs = [
            F.countDistinct("symbol").alias("distinct_symbols"),
            F.min("timestamp").alias("min_timestamp"),
            F.max("timestamp").alias("max_timestamp"),
            F.sum(F.when(F.col("volume") == 0.0, 1).otherwise(0)).alias("zero_volume_records"),
            F.sum(
                F.when(
                    (F.col("high") < F.col("low")) |
                    (F.col("high") < F.col("open")) |
                    (F.col("high") < F.col("close")) |
                    (F.col("low") > F.col("open")) |
                    (F.col("low") > F.col("close")),
                    1
                ).otherwise(0)
            ).alias("invalid_ohlc_records"),
            F.sum(
                F.when(
                    (F.col("open") <= 0) |
                    (F.col("high") <= 0) |
                    (F.col("low") <= 0) |
                    (F.col("close") <= 0),
                    1
                ).otherwise(0)
            ).alias("negative_or_zero_prices")
        ]

        # Add null counts for each column
        for col_name in df.columns:
            agg_exprs.append(
                F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"null_{col_name}")
            )

        stats_row = df.select(*agg_exprs).first()
        stats_dict = stats_row.asDict()

        null_counts = {
            col_name: stats_dict.get(f"null_{col_name}", 0) or 0
            for col_name in df.columns
        }

        min_ts = stats_dict.get("min_timestamp")
        max_ts = stats_dict.get("max_timestamp")

        return {
            "total_records": total_rows,
            "distinct_symbols": stats_dict.get("distinct_symbols", 0),
            "invalid_ohlc_records": stats_dict.get("invalid_ohlc_records", 0) or 0,
            "negative_or_zero_prices": stats_dict.get("negative_or_zero_prices", 0) or 0,
            "zero_volume_records": stats_dict.get("zero_volume_records", 0) or 0,
            "null_counts": null_counts,
            "date_range": {
                "min": str(min_ts) if min_ts else None,
                "max": str(max_ts) if max_ts else None
            }
        }

    def normalize_schema(self, df: DataFrame) -> DataFrame:
        """
        Normalize column names, types, timestamp, and symbol.

        - Standardizes column names to lowercase.
        - Robustly parses timestamp from StringType, TimestampType,
          or numeric epoch (nanoseconds, milliseconds, seconds).
        - Standardizes stock ticker symbols to uppercase trimmed strings.
        - Casts numeric price and volume columns to DoubleType.
        - Keeps extra technical indicators as DoubleType.
        """
        # Standardize column names to lowercase
        for col_name in df.columns:
            if col_name != col_name.lower():
                df = df.withColumnRenamed(col_name, col_name.lower())

        # Normalize timestamp
        if "timestamp" in df.columns:
            ts_field = [f for f in df.schema.fields if f.name == "timestamp"][0]
            if isinstance(ts_field.dataType, (LongType, IntegerType, DoubleType)):
                ts_col = F.col("timestamp")
                df = df.withColumn(
                    "timestamp",
                    F.when(ts_col > 1e14, (ts_col / 1e9).cast(TimestampType()))
                     .when(ts_col > 1e11, (ts_col / 1e3).cast(TimestampType()))
                     .when(ts_col > 1e8, ts_col.cast(TimestampType()))
                     .otherwise(F.to_timestamp(ts_col))
                )
            elif not isinstance(ts_field.dataType, TimestampType):
                df = df.withColumn("timestamp", F.to_timestamp(F.col("timestamp")))

        # Normalize symbol
        if "symbol" in df.columns:
            df = df.withColumn("symbol", F.trim(F.upper(F.col("symbol"))))

        # Cast numeric price and volume columns to DoubleType
        numeric_cols = ["open", "high", "low", "close", "volume"]
        for col_name in numeric_cols:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        # Keep extra technical indicator columns as DoubleType
        extra_cols = [c for c in df.columns if c not in self.CANONICAL_COLUMNS]
        for col_name in extra_cols:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

        return df

    def clean(self, df: DataFrame) -> DataFrame:
        """
        Execute deterministic distributed data cleaning pipeline.

        Cleaning stages:
        1. Parse & normalize schema, timestamp, and stock symbols
        2. Preserve genuine zero-volume records, impute null volume to 0.0
        3. Drop records with null values in critical columns
        4. Validate OHLC price consistency & positive prices
        5. Deduplicate on (symbol, timestamp)
        6. Sort deterministically within partitions
        """
        # Step 1: Normalize schema, types, timestamps, and symbols
        cleaned = self.normalize_schema(df)

        # Step 2: Handle volume
        if "volume" in cleaned.columns:
            # Preserve genuine 0.0 volume, impute nulls to 0.0
            cleaned = cleaned.withColumn("volume", F.coalesce(F.col("volume"), F.lit(0.0)))
        else:
            cleaned = cleaned.withColumn("volume", F.lit(0.0))

        # Step 3: Drop rows missing critical identifiers or OHLC
        critical_cols = ["timestamp", "symbol", "open", "high", "low", "close"]
        cleaned = cleaned.dropna(subset=critical_cols)

        # Step 4: Validate OHLC relationships & strictly positive prices
        price_valid_condition = (
            (F.col("high") >= F.col("low")) &
            (F.col("high") >= F.col("open")) &
            (F.col("high") >= F.col("close")) &
            (F.col("low") <= F.col("open")) &
            (F.col("low") <= F.col("close")) &
            (F.col("open") > 0.0) &
            (F.col("high") > 0.0) &
            (F.col("low") > 0.0) &
            (F.col("close") > 0.0)
        )
        cleaned = cleaned.filter(price_valid_condition)

        # Step 5: Deduplicate by (symbol, timestamp)
        cleaned = cleaned.dropDuplicates(["symbol", "timestamp"])

        # Step 6: Order columns: canonical first, then indicators
        extra_cols = [c for c in cleaned.columns if c not in self.CANONICAL_COLUMNS]
        ordered_cols = [c for c in self.CANONICAL_COLUMNS if c in cleaned.columns] + extra_cols
        cleaned = cleaned.select(*ordered_cols)

        # Step 7: Sort deterministically within partition by symbol & timestamp
        cleaned = cleaned.sortWithinPartitions("symbol", "timestamp")

        return cleaned

    def write_output(
        self,
        df: DataFrame,
        output_path: str,
        overwrite: bool = False
    ):
        """
        Write cleaned records in Parquet format, partitioned by symbol.

        Args:
            df: Cleaned DataFrame
            output_path: Target directory path or gs:// URI
            overwrite: Whether to overwrite existing destination
        """
        clean_out = output_path.strip().replace("\\", "/")
        is_cloud = clean_out.startswith("gs://") or clean_out.startswith("s3://")

        if not is_cloud:
            local_p = Path(clean_out)
            if local_p.exists() and not overwrite:
                raise FileExistsError(
                    f"Output destination already exists: {clean_out}\n"
                    f"Use --overwrite to allow replacing the existing dataset."
                )
            local_p.mkdir(parents=True, exist_ok=True)

        mode = "overwrite" if overwrite else "errorifexists"

        # Write partitioned Parquet dataset
        (
            df.write
            .mode(mode)
            .partitionBy("symbol")
            .option("compression", "snappy")
            .parquet(clean_out)
        )

    def run_pipeline(
        self,
        input_path: str,
        output_path: str,
        subset_stocks: Optional[int] = None,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Execute full cleaning pipeline with pre/post metrics tracking.

        Args:
            input_path: Source data path or GCS URI
            output_path: Target directory path or GCS URI
            subset_stocks: Optional count limit on stocks to process
            overwrite: Whether to overwrite target

        Returns:
            Quality and pipeline execution summary
        """
        start_time = time.time()
        print("=" * 80)
        print("SmartStock - Distributed PySpark Preprocessing")
        print("=" * 80)
        print(f"Input path:   {input_path}")
        print(f"Output path:  {output_path}")
        print(f"Subset limit: {subset_stocks or 'All available stocks'}")
        print(f"Overwrite:    {overwrite}")

        # Safety check: Prevent writing output directly into the input directory
        norm_in = Path(input_path).resolve() if not input_path.startswith("gs://") else input_path
        norm_out = Path(output_path).resolve() if not output_path.startswith("gs://") else output_path
        if norm_in == norm_out:
            raise ValueError(
                f"Input and output paths cannot be identical to prevent overwriting source data: {input_path}"
            )

        print("\n[1/4] Reading input files...")
        raw_df, input_format = self.read_input(input_path, subset_stocks=subset_stocks)
        print(f"[OK] Format detected: {input_format.upper()}")

        # Ensure normalized types for accurate pre-metrics
        norm_raw_df = self.normalize_schema(raw_df)

        print("\n[2/4] Computing pre-cleaning quality metrics...")
        pre_metrics = self.compute_data_quality_metrics(norm_raw_df)
        print(f"  Raw total records:     {pre_metrics['total_records']:,}")
        print(f"  Distinct stocks:       {pre_metrics['distinct_symbols']}")
        print(f"  Invalid OHLC records:  {pre_metrics['invalid_ohlc_records']:,}")
        print(f"  Negative/zero prices:  {pre_metrics['negative_or_zero_prices']:,}")
        print(f"  Zero-volume records:   {pre_metrics['zero_volume_records']:,}")

        print("\n[3/4] Cleaning, normalizing, and deduplicating records...")
        cleaned_df = self.clean(norm_raw_df)

        # Cache cleaned dataframe for post-metrics computation and write
        cleaned_df = cleaned_df.persist()

        print("\n[4/4] Writing cleaned Parquet partitioned dataset...")
        self.write_output(cleaned_df, output_path, overwrite=overwrite)
        print(f"[OK] Saved partitioned Parquet to: {output_path}")

        post_metrics = self.compute_data_quality_metrics(cleaned_df)
        cleaned_df.unpersist()

        duration = time.time() - start_time
        dropped_records = pre_metrics['total_records'] - post_metrics['total_records']
        dropped_pct = (
            (dropped_records / pre_metrics['total_records'] * 100.0)
            if pre_metrics['total_records'] > 0 else 0.0
        )

        metadata = {
            "pipeline_name": "SmartStock-PySpark-Cleaning",
            "executed_at": datetime.now().isoformat(),
            "execution_duration_seconds": round(duration, 2),
            "input_path": input_path,
            "output_path": output_path,
            "input_format": input_format,
            "pre_cleaning_metrics": pre_metrics,
            "post_cleaning_metrics": post_metrics,
            "summary": {
                "raw_records": pre_metrics['total_records'],
                "cleaned_records": post_metrics['total_records'],
                "dropped_records": dropped_records,
                "dropped_percentage": round(dropped_pct, 4),
                "distinct_stocks": post_metrics['distinct_symbols'],
                "date_range": post_metrics['date_range'],
                "zero_volume_preserved": post_metrics['zero_volume_records']
            }
        }

        # Save metadata JSON locally or to disk
        if not output_path.startswith("gs://"):
            meta_dir = Path(output_path).parent if Path(output_path).name == "stock_data" else Path(output_path)
            meta_file = meta_dir / "cleaning_metadata.json"
            dq_file = meta_dir / "cleaning_quality_summary.json"
            with open(meta_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            with open(dq_file, 'w') as f:
                json.dump(metadata['summary'], f, indent=2)
            print(f"[OK] Metadata saved: {meta_file}")
            print(f"[OK] Quality summary saved: {dq_file}")

        self._print_summary(metadata)
        return metadata

    def _print_summary(self, metadata: Dict[str, Any]):
        """Print concise user-facing summary report."""
        summary = metadata["summary"]
        print("\n" + "=" * 80)
        print("PREPROCESSING PIPELINE SUMMARY")
        print("=" * 80)
        print(f"Duration:              {metadata['execution_duration_seconds']} seconds")
        print(f"Raw Input Records:     {summary['raw_records']:,}")
        print(f"Cleaned Records:       {summary['cleaned_records']:,}")
        print(f"Dropped Records:       {summary['dropped_records']:,} ({summary['dropped_percentage']}%)")
        print(f"Distinct Stocks:       {summary['distinct_stocks']}")
        print(f"Zero-Volume Preserved: {summary['zero_volume_preserved']:,}")
        if summary['date_range']:
            print(f"Date Range:            {summary['date_range']['min']} -> {summary['date_range']['max']}")
        print("=" * 80)
        print("[OK] Distributed preprocessing complete!\n")


def main():
    """Main CLI entry point for distributed preprocessing."""
    parser = argparse.ArgumentParser(
        description="SmartStock - Distributed PySpark Preprocessing and Cleaning",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=None,
        help="Input dataset path (default: data/processed/stock_data or from config)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output dataset path (default: data/cleaned/stock_data or GCS URI)"
    )
    parser.add_argument(
        "--subset-stocks",
        type=int,
        default=None,
        help="Process a subset of stock symbols for rapid testing"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting target directory if it already exists"
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="SmartStock-Preprocessing",
        help="Spark application name"
    )
    parser.add_argument(
        "--master",
        type=str,
        default=None,
        help="Spark master URL (default: local[*])"
    )

    args = parser.parse_args()

    config = get_config()

    # Determine input path
    input_path = args.input_dir
    if not input_path:
        default_ingested = Path(config.paths.processed_data) / "stock_data"
        if default_ingested.exists():
            input_path = str(default_ingested)
        else:
            input_path = config.paths.raw_data

    # Determine output path
    output_path = args.output_dir
    if not output_path:
        output_path = str(config.project_root / "data" / "cleaned" / "stock_data")

    is_cloud = input_path.startswith("gs://") or output_path.startswith("gs://")

    spark = SparkSessionManager.get_or_create(
        app_name=args.app_name,
        master=args.master,
        is_cloud=is_cloud
    )

    try:
        cleaner = DataCleaner(spark, config)
        cleaner.run_pipeline(
            input_path=input_path,
            output_path=output_path,
            subset_stocks=args.subset_stocks,
            overwrite=args.overwrite
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
