"""
Evaluation and Inference Module for SmartStock MLlib Models.

Provides comprehensive metric calculation (Precision, Recall, F1, ROC-AUC, PR-AUC),
threshold tuning on validation data, baseline comparisons, and a standalone
ModelPredictor class for loading models and generating inference predictions without retraining.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


class ModelEvaluator:
    """
    Computes classification performance metrics and evaluates baseline models.
    """

    @staticmethod
    def extract_probability(df: DataFrame, prob_col: str = "probability") -> DataFrame:
        """
        Extract positive class probability P(y=1) from vector probability column.
        """
        # Vector probability [P(y=0), P(y=1)]
        from pyspark.ml.functions import vector_to_array
        if "prob_positive" not in df.columns:
            df = df.withColumn("prob_positive", vector_to_array(F.col(prob_col))[1])
        return df

    @classmethod
    def evaluate(
        cls,
        predictions_df: DataFrame,
        label_col: str = "label",
        threshold: float = 0.50
    ) -> Dict[str, Any]:
        """
        Compute comprehensive classification metrics at a given decision threshold.
        """
        pred_df = cls.extract_probability(predictions_df)

        # Apply decision threshold to get binary prediction
        pred_df = pred_df.withColumn(
            "pred_binary",
            F.when(F.col("prob_positive") >= threshold, 1.0).otherwise(0.0)
        )

        total_count = pred_df.count()
        if total_count == 0:
            return {}

        # Confusion matrix elements
        agg_res = pred_df.agg(
            F.sum(F.when((F.col("pred_binary") == 1.0) & (F.col(label_col) == 1.0), 1).otherwise(0)).alias("tp"),
            F.sum(F.when((F.col("pred_binary") == 1.0) & (F.col(label_col) == 0.0), 1).otherwise(0)).alias("fp"),
            F.sum(F.when((F.col("pred_binary") == 0.0) & (F.col(label_col) == 0.0), 1).otherwise(0)).alias("tn"),
            F.sum(F.when((F.col("pred_binary") == 0.0) & (F.col(label_col) == 1.0), 1).otherwise(0)).alias("fn"),
            F.sum(F.when(F.col(label_col) == 1.0, 1).otherwise(0)).alias("actual_pos"),
            F.sum(F.when(F.col(label_col) == 0.0, 1).otherwise(0)).alias("actual_neg"),
            F.avg(F.col("prob_positive")).alias("mean_prob")
        ).first().asDict()

        tp = agg_res["tp"] or 0
        fp = agg_res["fp"] or 0
        tn = agg_res["tn"] or 0
        fn = agg_res["fn"] or 0
        actual_pos = agg_res["actual_pos"] or 0
        actual_neg = agg_res["actual_neg"] or 0

        accuracy = (tp + tn) / total_count if total_count > 0 else 0.0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2.0 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0

        # Spark MLlib Evaluators for ROC-AUC and PR-AUC
        roc_eval = BinaryClassificationEvaluator(
            rawPredictionCol="prob_positive",
            labelCol=label_col,
            metricName="areaUnderROC"
        )
        pr_eval = BinaryClassificationEvaluator(
            rawPredictionCol="prob_positive",
            labelCol=label_col,
            metricName="areaUnderPR"
        )

        try:
            roc_auc = float(roc_eval.evaluate(pred_df))
        except Exception:
            roc_auc = 0.50

        try:
            pr_auc = float(pr_eval.evaluate(pred_df))
        except Exception:
            pr_auc = actual_pos / total_count if total_count > 0 else 0.0

        return {
            "threshold": round(threshold, 4),
            "total_samples": total_count,
            "accuracy": round(accuracy, 4),
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "f1_score": round(f1, 4),
            "roc_auc": round(roc_auc, 4),
            "pr_auc": round(pr_auc, 4),
            "confusion_matrix": {
                "tp": tp,
                "fp": fp,
                "tn": tn,
                "fn": fn
            },
            "class_distribution": {
                "actual_positive": actual_pos,
                "actual_negative": actual_neg,
                "positive_rate": round(actual_pos / total_count * 100.0, 2)
            },
            "mean_predicted_probability": round(float(agg_res["mean_prob"] or 0.0), 4)
        }

    @classmethod
    def evaluate_baselines(
        cls,
        df: DataFrame,
        label_col: str = "label"
    ) -> Dict[str, Any]:
        """
        Compare model against simple baseline predictions:
        1. Always Predict Negative (Majority class in 60m horizon)
        2. Always Predict Positive
        3. Momentum Return Heuristic (Predict positive if return_1m > 0)
        """
        total = df.count()
        if total == 0:
            return {}

        pos_count = df.filter(F.col(label_col) == 1.0).count()
        neg_count = total - pos_count

        # 1. Majority Class Baseline (Always Negative)
        maj_acc = neg_count / total

        # 2. Momentum Baseline (predict 1 if return_1m > 0)
        has_ret = "return_1m" in df.columns
        if has_ret:
            mom_df = df.withColumn("mom_pred", F.when(F.col("return_1m") > 0.0, 1.0).otherwise(0.0))
            mom_tp = mom_df.filter((F.col("mom_pred") == 1.0) & (F.col(label_col) == 1.0)).count()
            mom_fp = mom_df.filter((F.col("mom_pred") == 1.0) & (F.col(label_col) == 0.0)).count()
            mom_acc = mom_df.filter(F.col("mom_pred") == F.col(label_col)).count() / total
            mom_prec = mom_tp / (mom_tp + mom_fp) if (mom_tp + mom_fp) > 0 else 0.0
            mom_rec = mom_tp / pos_count if pos_count > 0 else 0.0
            mom_f1 = (2 * mom_prec * mom_rec) / (mom_prec + mom_rec) if (mom_prec + mom_rec) > 0 else 0.0
        else:
            mom_acc = mom_prec = mom_rec = mom_f1 = 0.0

        return {
            "majority_class_baseline": {
                "description": "Always predicts the majority class (Negative)",
                "accuracy": round(maj_acc, 4),
                "precision": 0.0,
                "recall": 0.0,
                "f1_score": 0.0
            },
            "momentum_heuristic_baseline": {
                "description": "Predicts favorable if 1-minute return is positive (return_1m > 0)",
                "accuracy": round(mom_acc, 4),
                "precision": round(mom_prec, 4),
                "recall": round(mom_rec, 4),
                "f1_score": round(mom_f1, 4)
            }
        }

    @classmethod
    def tune_threshold(
        cls,
        val_predictions: DataFrame,
        label_col: str = "label",
        threshold_candidates: Optional[List[float]] = None
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Scan probability thresholds on validation set to find threshold that maximizes F1 score.
        """
        candidates = threshold_candidates or [
            round(x * 0.05, 2) for x in range(4, 17) # 0.20 to 0.80
        ]
        pred_df = cls.extract_probability(val_predictions).persist()

        best_th = 0.50
        best_f1 = -1.0
        best_metrics = {}

        for th in candidates:
            m = cls.evaluate(pred_df, label_col=label_col, threshold=th)
            if m.get("f1_score", 0.0) > best_f1:
                best_f1 = m["f1_score"]
                best_th = th
                best_metrics = m

        pred_df.unpersist()
        return best_th, best_metrics


class ModelPredictor:
    """
    Standalone inference runner for loading saved models and generating predictions without retraining.
    """

    def __init__(self, model_dir: str):
        """
        Load saved PipelineModel and metadata from disk.
        """
        clean_dir = str(Path(model_dir).resolve())
        self.pipeline_model = PipelineModel.load(clean_dir)

        meta_path = Path(clean_dir).parent / "model_metadata.json"
        if meta_path.exists():
            with open(meta_path, "r") as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {}

        self.threshold = self.metadata.get("tuned_threshold", 0.50)
        self.feature_columns = self.metadata.get("feature_columns", [])

    def predict(self, df: DataFrame) -> DataFrame:
        """
        Run inference using the loaded model pipeline and apply the tuned decision threshold.

        Returns:
            DataFrame with original columns + 'prob_positive' and 'recommended_action' (1 = Buy, 0 = Hold/Skip)
        """
        preds = self.pipeline_model.transform(df)
        preds = ModelEvaluator.extract_probability(preds)

        preds = preds.withColumn(
            "recommended_action",
            F.when(F.col("prob_positive") >= self.threshold, 1).otherwise(0)
        )
        return preds
