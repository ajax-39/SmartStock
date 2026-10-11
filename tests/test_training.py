"""
Unit tests for Model Training, Splitting, and Inference Pipeline.
"""

import os
import sys
import tempfile
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

from src.preprocessing.clean_data import SparkSessionManager
from src.training.split_data import DataSplitter
from src.training.evaluate_models import ModelEvaluator, ModelPredictor
from src.training.train_models import ModelTrainer


@pytest.fixture(scope="module")
def spark():
    """Create test SparkSession."""
    session = SparkSessionManager.get_or_create(
        app_name="TestSmartStockTraining",
        master="local[1]",
        driver_memory="1g",
        executor_memory="1g"
    )
    yield session
    session.stop()


class TestDataSplitter:
    """Test suite for data splitting designs."""

    def test_unseen_stock_isolation(self, spark):
        """
        Verify Design A holds out unseen stocks completely from development splits.
        """
        data = [
            ("2022-01-01 09:15:00", "ABB", 0.01, 1.0),
            ("2022-01-01 09:15:00", "AMBUJACEM", 0.02, 0.0),
            ("2024-01-01 09:15:00", "ABB", 0.03, 1.0),
            ("2024-01-01 09:15:00", "AMBUJACEM", 0.01, 1.0),
        ]
        cols = ["timestamp", "symbol", "feature1", "label"]
        df = spark.createDataFrame(data, cols).withColumn("timestamp", F.to_timestamp("timestamp"))

        splitter = DataSplitter(
            spark,
            dev_symbols=["ABB"],
            unseen_test_symbols=["AMBUJACEM"],
            train_end_date="2023-01-01 00:00:00",
            val_end_date="2025-01-01 00:00:00"
        )
        splits = splitter.split(df)

        # Unseen test must contain only AMBUJACEM
        unseen_syms = [r.symbol for r in splits["unseen_test"].select("symbol").distinct().collect()]
        assert unseen_syms == ["AMBUJACEM"]

        # Train and Validation must contain ONLY ABB (zero AMBUJACEM)
        train_syms = [r.symbol for r in splits["train"].select("symbol").distinct().collect()]
        val_syms = [r.symbol for r in splits["validation"].select("symbol").distinct().collect()]
        assert "AMBUJACEM" not in train_syms
        assert "AMBUJACEM" not in val_syms

    def test_purge_gap_boundary_handling(self, spark):
        """
        Verify that observations within the purge window before split boundaries
        are dropped from the preceding set to prevent label horizon overlap.
        """
        # Split boundary is 2023-01-01 00:00:00 with a 60-minute purge gap (cutoff is 2022-12-31 23:00:00)
        data = [
            # Included in train
            ("2022-12-31 22:50:00", "DEV1", 0.1, 1.0),
            # In the 60-minute purge buffer (should be PURGED from train!)
            ("2022-12-31 23:30:00", "DEV1", 0.2, 1.0),
            # Exactly on/after boundary (included in validation)
            ("2023-01-01 09:15:00", "DEV1", 0.3, 0.0),
        ]
        cols = ["timestamp", "symbol", "feature1", "label"]
        df = spark.createDataFrame(data, cols).withColumn("timestamp", F.to_timestamp("timestamp"))

        splitter = DataSplitter(
            spark,
            dev_symbols=["DEV1"],
            unseen_test_symbols=["UNSEEN1"],
            train_end_date="2023-01-01 00:00:00",
            val_end_date="2025-01-01 00:00:00",
            purge_minutes=60
        )
        splits = splitter.split(df)

        train_ts = [str(r.timestamp) for r in splits["train"].collect()]
        assert "2022-12-31 22:50:00" in train_ts
        # Purged row must NOT be in train!
        assert "2022-12-31 23:30:00" not in train_ts
        # Validation row is present in validation
        val_ts = [str(r.timestamp) for r in splits["validation"].collect()]
        assert "2023-01-01 09:15:00" in val_ts


class TestModelTrainingAndInference:
    """Test suite for training pipeline, evaluation metrics, and standalone inference loading."""

    @pytest.fixture
    def synthetic_dataset(self, spark):
        """Create a synthetic dataset with features and binary label."""
        rows = []
        for i in range(100):
            f1 = float(i) / 100.0
            f2 = float(100 - i) / 100.0
            label = 1.0 if f1 > 0.4 else 0.0
            rows.append((f1, f2, label))
        cols = ["feat_a", "feat_b", "label"]
        return spark.createDataFrame(rows, cols)

    def test_pipeline_fit_and_threshold_tuning(self, spark, synthetic_dataset):
        """
        Verify MLlib pipeline fits correctly and threshold tuner finds optimal threshold.
        """
        trainer = ModelTrainer(spark)
        models = trainer.get_candidate_models()
        lr = models["logistic_regression"]

        feature_cols = ["feat_a", "feat_b"]
        pipeline = trainer.build_pipeline(lr, feature_cols)
        fitted = pipeline.fit(synthetic_dataset)

        preds = fitted.transform(synthetic_dataset)
        tuned_th, metrics = ModelEvaluator.tune_threshold(preds, label_col="label")

        assert 0.0 < tuned_th < 1.0
        assert metrics["f1_score"] > 0.80
        assert metrics["roc_auc"] > 0.90

    def test_model_persistence_and_standalone_inference(self, spark, synthetic_dataset):
        """
        Verify that a saved pipeline model can be loaded by ModelPredictor and run
        predictions without any retraining.
        """
        feature_cols = ["feat_a", "feat_b"]
        trainer = ModelTrainer(spark)
        models = trainer.get_candidate_models()
        rf = models["random_forest"]

        pipeline = trainer.build_pipeline(rf, feature_cols)
        fitted = pipeline.fit(synthetic_dataset)

        with tempfile.TemporaryDirectory() as tmp_dir:
            model_save_dir = str(Path(tmp_dir) / "model_bundle" / "model")
            meta_save_path = Path(tmp_dir) / "model_bundle" / "model_metadata.json"
            Path(meta_save_path).parent.mkdir(parents=True, exist_ok=True)

            # Save model and write metadata
            fitted.write().overwrite().save(model_save_dir)
            with open(meta_save_path, "w") as f:
                import json
                json.dump({
                    "model_type": "RandomForest",
                    "tuned_threshold": 0.45,
                    "feature_columns": feature_cols
                }, f)

            # Test standalone predictor loading without retraining
            predictor = ModelPredictor(model_save_dir)
            assert predictor.threshold == 0.45
            assert predictor.feature_columns == feature_cols

            # Run inference on new data
            test_data = spark.createDataFrame([(0.8, 0.2), (0.1, 0.9)], ["feat_a", "feat_b"])
            infer_preds = predictor.predict(test_data).collect()

            assert len(infer_preds) == 2
            assert "prob_positive" in infer_preds[0]
            assert "recommended_action" in infer_preds[0]
            # Higher f1 (0.8) should yield higher positive probability than lower f1 (0.1)
            assert infer_preds[0]["prob_positive"] > infer_preds[1]["prob_positive"]
