"""
Model Training Pipeline for SmartStock using Spark MLlib.

Trains and compares:
1. Logistic Regression (Linear baseline with regularization)
2. Random Forest Classifier (Tree ensemble)
3. Gradient-Boosted Trees Classifier (Sequential boosting)

Methodology:
- Trained strictly on chronological Training data (2015-2022).
- Hyperparameters and probability decision threshold tuned on Validation data (2023-2024).
- Final models evaluated on Future Test (2025-2026) and Unseen Holdout Stocks.
- Saves the selected best model pipeline, feature schema, tuned threshold, and full evaluation metadata.
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

# Configure Hadoop on Windows
hadoop_dir = project_root / "hadoop"
if hadoop_dir.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_dir.resolve())
    hadoop_bin = str((hadoop_dir / "bin").resolve())
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import Config, get_config
from src.preprocessing.clean_data import SparkSessionManager
from src.training.evaluate_models import ModelEvaluator
from src.training.split_data import DataSplitter


class ModelTrainer:
    """
    Orchestrates Spark MLlib training, hyperparameter tuning, threshold selection, and saving.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[Config] = None,
        target_col: str = "target_favorable_60m"
    ):
        """
        Initialize ModelTrainer.

        Args:
            spark: Active SparkSession
            config: SmartStock configuration
            target_col: Target column to predict (e.g. 'target_favorable_60m')
        """
        self.spark = spark
        self.config = config or get_config()
        self.target_col = target_col

    def build_pipeline(
        self,
        classifier: Any,
        feature_cols: List[str]
    ) -> Pipeline:
        """
        Build an end-to-end Spark MLlib Pipeline with feature assembly and scaling.
        """
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="raw_features",
            handleInvalid="skip"
        )
        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withMean=True,
            withStd=True
        )
        return Pipeline(stages=[assembler, scaler, classifier])

    def get_candidate_models(self) -> Dict[str, Any]:
        """
        Instantiate candidate classification models.
        """
        models = {
            "logistic_regression": LogisticRegression(
                featuresCol="features",
                labelCol="label",
                maxIter=50,
                regParam=0.01,
                elasticNetParam=0.2
            ),
            "random_forest": RandomForestClassifier(
                featuresCol="features",
                labelCol="label",
                numTrees=30,
                maxDepth=6,
                seed=42
            ),
            "gradient_boosted_trees": GBTClassifier(
                featuresCol="features",
                labelCol="label",
                maxIter=25,
                maxDepth=5,
                stepSize=0.1,
                seed=42
            )
        }
        return models

    def train_and_tune(
        self,
        train_df: DataFrame,
        val_df: DataFrame,
        feature_cols: List[str],
        models_to_run: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Train each candidate model on Train, evaluate and tune threshold on Validation.

        Returns:
            Dictionary containing fitted pipelines, tuned thresholds, and validation metrics.
        """
        candidate_models = self.get_candidate_models()
        if models_to_run:
            candidate_models = {k: v for k, v in candidate_models.items() if k in models_to_run}

        results = {}

        for model_name, classifier in candidate_models.items():
            print(f"\n--- Training {model_name} ---")
            t0 = time.time()

            pipeline = self.build_pipeline(classifier, feature_cols)
            fitted_model = pipeline.fit(train_df)
            fit_time = time.time() - t0
            print(f"  [OK] Fitted in {fit_time:.2f}s")

            # Predict on Validation
            val_preds = fitted_model.transform(val_df)

            # Tune optimal decision threshold on Validation set
            tuned_threshold, val_metrics = ModelEvaluator.tune_threshold(
                val_preds, label_col="label"
            )

            print(f"  [OK] Tuned Threshold: {tuned_threshold:.2f} | Val F1: {val_metrics['f1_score']:.4f} | Val ROC-AUC: {val_metrics['roc_auc']:.4f}")

            results[model_name] = {
                "pipeline_model": fitted_model,
                "tuned_threshold": tuned_threshold,
                "fit_time_seconds": round(fit_time, 2),
                "validation_metrics": val_metrics
            }

        return results

    def select_best_model(self, model_results: Dict[str, Any]) -> str:
        """
        Select best model based on validation ROC-AUC and F1-score.
        """
        # Score = ROC-AUC + F1
        best_name = None
        best_score = -1.0

        for name, data in model_results.items():
            val_m = data["validation_metrics"]
            combined = val_m.get("roc_auc", 0.0) + val_m.get("f1_score", 0.0)
            if combined > best_score:
                best_score = combined
                best_name = name

        return best_name

    def run_training_pipeline(
        self,
        features_dir: str,
        labels_dir: str,
        output_model_dir: str = "models/saved/best_model",
        models_to_run: Optional[List[str]] = None,
        sample_fraction: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Execute complete distributed training, tuning, selection, and persistence pipeline.
        """
        overall_start = time.time()
        print("=" * 80)
        print("SmartStock - Distributed PySpark MLlib Model Training")
        print("=" * 80)
        print(f"Features path:    {features_dir}")
        print(f"Labels path:      {labels_dir}")
        print(f"Target Column:    {self.target_col}")
        print(f"Output Model Dir: {output_model_dir}")

        # 1. Load and Split
        print("\n[1/5] Loading joined dataset and applying evaluation splits...")
        splitter = DataSplitter(self.spark)
        joined_df, feature_cols = splitter.load_joined_dataset(
            features_dir, labels_dir, target_col=self.target_col
        )

        if sample_fraction and 0.0 < sample_fraction < 1.0:
            print(f"  [Sampling] Sampling {sample_fraction * 100:.1f}% for training speed...")
            joined_df = joined_df.sample(withReplacement=False, fraction=sample_fraction, seed=42)

        splits = splitter.split(joined_df)
        split_summary = splitter.summarize_splits(splits)

        train_df = splits["train"]
        val_df = splits["validation"]
        future_test_df = splits["future_test"]
        unseen_test_df = splits["unseen_test"]

        print(f"  Train Records:        {split_summary['splits']['train']['total_records']:,}")
        print(f"  Validation Records:   {split_summary['splits']['validation']['total_records']:,}")
        print(f"  Future Test Records:  {split_summary['splits']['future_test']['total_records']:,}")
        print(f"  Unseen Test Records:  {split_summary['splits']['unseen_test']['total_records']:,}")

        # 2. Train and Tune Candidate Models
        print("\n[2/5] Training candidate models and tuning decision thresholds...")
        model_results = self.train_and_tune(
            train_df, val_df, feature_cols, models_to_run=models_to_run
        )

        # 3. Model Selection
        best_model_name = self.select_best_model(model_results)
        best_data = model_results[best_model_name]
        best_model = best_data["pipeline_model"]
        tuned_threshold = best_data["tuned_threshold"]
        print(f"\n[3/5] Model Selection Result: Chosen Best Model is '{best_model_name}' (Threshold: {tuned_threshold:.2f})")

        # 4. Final Evaluations (Future Test & Unseen Stocks)
        print("\n[4/5] Evaluating selected model on Final Evaluation Sets...")
        future_test_preds = best_model.transform(future_test_df)
        future_test_metrics = ModelEvaluator.evaluate(
            future_test_preds, label_col="label", threshold=tuned_threshold
        )

        unseen_test_preds = best_model.transform(unseen_test_df)
        unseen_test_metrics = ModelEvaluator.evaluate(
            unseen_test_preds, label_col="label", threshold=tuned_threshold
        )

        # Baseline comparisons on Future Test set
        baseline_metrics = ModelEvaluator.evaluate_baselines(future_test_df, label_col="label")

        print("\n" + "-" * 40)
        print("FUTURE-PERIOD TEST (Design B):")
        print(f"  ROC-AUC:   {future_test_metrics['roc_auc']:.4f}")
        print(f"  PR-AUC:    {future_test_metrics['pr_auc']:.4f}")
        print(f"  Precision: {future_test_metrics['precision']:.4f}")
        print(f"  Recall:    {future_test_metrics['recall']:.4f}")
        print(f"  F1-Score:  {future_test_metrics['f1_score']:.4f}")

        print("\nUNSEEN-STOCK TEST (Design A):")
        print(f"  ROC-AUC:   {unseen_test_metrics['roc_auc']:.4f}")
        print(f"  PR-AUC:    {unseen_test_metrics['pr_auc']:.4f}")
        print(f"  Precision: {unseen_test_metrics['precision']:.4f}")
        print(f"  Recall:    {unseen_test_metrics['recall']:.4f}")
        print(f"  F1-Score:  {unseen_test_metrics['f1_score']:.4f}")
        print("-" * 40)

        # 5. Persist Best Model and Metadata
        print("\n[5/5] Saving model pipeline and metadata to disk...")
        clean_out_dir = str(Path(output_model_dir).resolve())
        best_model.write().overwrite().save(clean_out_dir)

        duration = time.time() - overall_start

        # Prepare metadata
        comparison_table = {}
        for name, data in model_results.items():
            vm = data["validation_metrics"]
            comparison_table[name] = {
                "fit_time_seconds": data["fit_time_seconds"],
                "tuned_threshold": data["tuned_threshold"],
                "val_roc_auc": vm["roc_auc"],
                "val_pr_auc": vm["pr_auc"],
                "val_f1_score": vm["f1_score"],
                "val_precision": vm["precision"],
                "val_recall": vm["recall"]
            }

        metadata = {
            "model_training_timestamp": datetime.now().isoformat(),
            "selected_best_model": best_model_name,
            "tuned_threshold": tuned_threshold,
            "target_column": self.target_col,
            "feature_columns_count": len(feature_cols),
            "feature_columns": feature_cols,
            "split_summary": split_summary,
            "model_comparison": comparison_table,
            "final_evaluation": {
                "future_period_test": future_test_metrics,
                "unseen_stock_test": unseen_test_metrics,
                "baselines": baseline_metrics
            },
            "training_duration_seconds": round(duration, 2)
        }

        meta_path = Path(clean_out_dir).parent / "model_metadata.json"
        with open(meta_path, "w") as f:
            json.dump(metadata, f, indent=2)

        print(f"[OK] Saved PipelineModel to: {clean_out_dir}")
        print(f"[OK] Saved Metadata to:      {meta_path}")

        print("\n" + "=" * 80)
        print("TRAINING PIPELINE COMPLETE")
        print("=" * 80)
        return metadata


def main():
    parser = argparse.ArgumentParser(description="SmartStock - Model Training with Spark MLlib")
    parser.add_argument("--features-dir", type=str, default="data/features/stock_features")
    parser.add_argument("--labels-dir", type=str, default="data/labels/stock_labels")
    parser.add_argument("--output-model-dir", type=str, default="models/saved/best_model")
    parser.add_argument("--target-col", type=str, default="target_favorable_60m")
    parser.add_argument("--models", nargs="+", default=["logistic_regression", "random_forest", "gradient_boosted_trees"])
    parser.add_argument("--sample-fraction", type=float, default=None)
    args = parser.parse_args()

    spark = SparkSessionManager.get_or_create(app_name="SmartStock-ModelTraining")
    try:
        trainer = ModelTrainer(spark, target_col=args.target_col)
        trainer.run_training_pipeline(
            features_dir=args.features_dir,
            labels_dir=args.labels_dir,
            output_model_dir=args.output_model_dir,
            models_to_run=args.models,
            sample_fraction=args.sample_fraction
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
