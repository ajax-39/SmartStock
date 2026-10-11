"""
Data Splitting Module for SmartStock.

Implements two complementary evaluation designs:
Design A: Unseen-Stock Evaluation
- 4-5 eligible stock symbols held out for the final test set prior to model fitting.
- Excluded from model fitting, feature scaling, threshold selection, and hyperparameter tuning.

Design B: Future-Period Evaluation (Chronological with Purging)
- Divide historical observations chronologically into Training, Validation, and Final Testing.
- Earlier periods for training, intermediate for validation, latest for testing.
- Uses an appropriate purge buffer at split boundaries matching the label horizon to prevent
  overlapping future return leakage across boundaries.
- No random row splitting.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import Config, get_config
from src.features.feature_engineering import FeatureEngineer
from src.labels.create_labels import LabelGenerator
from src.preprocessing.clean_data import SparkSessionManager


class DataSplitter:
    """
    Splits stock time-series dataset according to Unseen-Stock (Design A)
    and Purged Chronological Future-Period (Design B) designs.
    """

    # Confirmed development symbols in dataset
    DEFAULT_DEV_SYMBOLS = [
        "ABB", "ADANIENSOL", "ADANIENT", "ADANIPORTS", "ASIANPAINT", "AXISBANK"
    ]

    # Confirmed holdout unseen test symbols
    DEFAULT_UNSEEN_TEST_SYMBOLS = [
        "AMBUJACEM", "APOLLOHOSP", "BAJAJ-AUTO", "BAJAJFINSV"
    ]

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[Config] = None,
        dev_symbols: Optional[List[str]] = None,
        unseen_test_symbols: Optional[List[str]] = None,
        train_end_date: str = "2023-01-01 00:00:00",
        val_end_date: str = "2025-01-01 00:00:00",
        purge_minutes: int = 60
    ):
        """
        Initialize DataSplitter.

        Args:
            spark: Active SparkSession
            config: SmartStock configuration
            dev_symbols: List of symbols used for development (Train/Val)
            unseen_test_symbols: List of holdout symbols used exclusively for Unseen Test
            train_end_date: Cutoff boundary between Train and Validation
            val_end_date: Cutoff boundary between Validation and Future Test
            purge_minutes: Purge gap in minutes before each boundary to prevent label overlap
        """
        self.spark = spark
        self.config = config or get_config()
        self.dev_symbols = dev_symbols or self.DEFAULT_DEV_SYMBOLS
        self.unseen_test_symbols = unseen_test_symbols or self.DEFAULT_UNSEEN_TEST_SYMBOLS
        self.train_end_date = train_end_date
        self.val_end_date = val_end_date
        self.purge_minutes = purge_minutes

    def load_joined_dataset(
        self,
        features_path: str,
        labels_path: str,
        target_col: str = "target_favorable_60m"
    ) -> Tuple[DataFrame, List[str]]:
        """
        Load and join feature and label datasets on (symbol, timestamp).
        Filters out rows where target_col is null (terminal boundary or missing outcomes).

        Returns:
            Tuple of (joined_df, list_of_feature_column_names)
        """
        clean_feat_path = features_path.strip().replace("\\", "/")
        clean_lbl_path = labels_path.strip().replace("\\", "/")

        feat_df = self.spark.read.parquet(clean_feat_path)
        lbl_df = self.spark.read.parquet(clean_lbl_path)

        # Identify feature columns
        ignore_cols = set(FeatureEngineer.IDENTIFIER_COLUMNS)
        feature_cols = [c for c in feat_df.columns if c not in ignore_cols and c != "symbol"]

        # Select only the target label and key columns from the label table to prevent leakage
        lbl_sub = lbl_df.select("symbol", "timestamp", target_col)

        # Inner join on symbol and timestamp
        joined_df = feat_df.join(
            lbl_sub,
            on=["symbol", "timestamp"],
            how="inner"
        )

        # Filter out rows with null target (unrealized outcomes near dataset end)
        joined_df = joined_df.filter(F.col(target_col).isNotNull())

        # Also rename target_col to canonical 'label' column for MLlib
        joined_df = joined_df.withColumn("label", F.col(target_col).cast("double"))

        return joined_df, feature_cols

    def split(
        self,
        df: DataFrame
    ) -> Dict[str, DataFrame]:
        """
        Generate datasets for both evaluation designs with boundary purging.

        Returns:
            Dictionary containing 'train', 'validation', 'future_test', and 'unseen_test' DataFrames.
        """
        # Parse boundary timestamps and compute purged cutoff timestamps
        train_end_dt = datetime.fromisoformat(self.train_end_date.replace(" ", "T"))
        val_end_dt = datetime.fromisoformat(self.val_end_date.replace(" ", "T"))

        train_purge_dt = train_end_dt - timedelta(minutes=self.purge_minutes)
        val_purge_dt = val_end_dt - timedelta(minutes=self.purge_minutes)

        train_purge_str = train_purge_dt.strftime("%Y-%m-%d %H:%M:%S")
        val_purge_str = val_purge_dt.strftime("%Y-%m-%d %H:%M:%S")

        # -------------------------------------------------------------
        # Design A: Unseen-Stock Evaluation
        # -------------------------------------------------------------
        unseen_test_df = df.filter(F.col("symbol").isin(self.unseen_test_symbols))
        dev_df = df.filter(F.col("symbol").isin(self.dev_symbols))

        # -------------------------------------------------------------
        # Design B: Purged Chronological Future-Period Evaluation
        # -------------------------------------------------------------
        # Train: Earlier period, minus purge buffer at the end
        train_df = dev_df.filter(F.col("timestamp") < train_purge_str)

        # Validation: Intermediate period, starts exactly at train_end_date, minus purge buffer at the end
        val_df = dev_df.filter(
            (F.col("timestamp") >= self.train_end_date) &
            (F.col("timestamp") < val_purge_str)
        )

        # Future Test: Latest chronological period, starts at val_end_date
        future_test_df = dev_df.filter(F.col("timestamp") >= self.val_end_date)

        return {
            "train": train_df,
            "validation": val_df,
            "future_test": future_test_df,
            "unseen_test": unseen_test_df
        }

    def summarize_splits(
        self,
        splits: Dict[str, DataFrame]
    ) -> Dict[str, Any]:
        """
        Compute distributed summary statistics for each split partition.
        """
        summary = {
            "evaluation_design_a": {
                "dev_symbols": self.dev_symbols,
                "unseen_test_symbols": self.unseen_test_symbols,
            },
            "evaluation_design_b": {
                "train_end_date": self.train_end_date,
                "val_end_date": self.val_end_date,
                "purge_minutes": self.purge_minutes
            },
            "splits": {}
        }

        for split_name, split_df in splits.items():
            row = split_df.agg(
                F.count("*").alias("count"),
                F.countDistinct("symbol").alias("symbols_count"),
                F.min("timestamp").alias("min_ts"),
                F.max("timestamp").alias("max_ts"),
                F.sum(F.when(F.col("label") == 1.0, 1).otherwise(0)).alias("pos_count"),
                F.sum(F.when(F.col("label") == 0.0, 1).otherwise(0)).alias("neg_count")
            ).first().asDict()

            total_c = row["count"] or 0
            pos_c = row["pos_count"] or 0
            neg_c = row["neg_count"] or 0
            pos_pct = round((pos_c / total_c * 100.0), 2) if total_c > 0 else 0.0

            summary["splits"][split_name] = {
                "total_records": total_c,
                "symbols_count": row["symbols_count"] or 0,
                "min_timestamp": str(row["min_ts"]),
                "max_timestamp": str(row["max_ts"]),
                "positive_records": pos_c,
                "negative_records": neg_c,
                "positive_percentage": pos_pct
            }

        return summary


def main():
    parser = argparse.ArgumentParser(description="SmartStock - Dataset Splitting")
    parser.add_argument("--features-dir", type=str, default="data/features/stock_features")
    parser.add_argument("--labels-dir", type=str, default="data/labels/stock_labels")
    parser.add_argument("--target-col", type=str, default="target_favorable_60m")
    parser.add_argument("--purge-minutes", type=int, default=60)
    args = parser.parse_args()

    spark = SparkSessionManager.get_or_create(app_name="SmartStock-Splitter")
    try:
        splitter = DataSplitter(spark, purge_minutes=args.purge_minutes)
        joined_df, feature_cols = splitter.load_joined_dataset(
            args.features_dir, args.labels_dir, target_col=args.target_col
        )
        splits = splitter.split(joined_df)
        summary = splitter.summarize_splits(splits)
        print(json.dumps(summary, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
