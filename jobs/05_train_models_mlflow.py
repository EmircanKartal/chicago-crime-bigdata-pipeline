# Owner: Berfin
# Branch: feature/step5-step6-ml
# Purpose: Train 5 classification models and track all runs in MLflow
# Input:  delta/gold/ml_features/
# Output:
#   - MLflow runs
#   - reports/ml_model_metrics.csv
#   - reports/confusion_matrix_best_model.csv
#   - reports/feature_importance_best_model.csv
#
# Target:
#   Arrest prediction, binary classification

import csv
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier,
    GBTClassifier,
    NaiveBayes,
)
from pyspark.ml.evaluation import (
    MulticlassClassificationEvaluator,
    BinaryClassificationEvaluator,
)
from pyspark.ml.feature import StringIndexer, VectorAssembler

import mlflow
import mlflow.spark


spark = (
    SparkSession.builder
    .appName("TrainModelsMLflow")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

FEATURE_PATH = "delta/gold/ml_features"
REPORTS_DIR = Path("reports")

MLFLOW_TRACKING_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EXPERIMENT_NAME = "chicago_crime_arrest_classification"


def evaluate_model(predictions):
    """
    Calculates classification metrics required in the project:
    accuracy, f1-score, precision, recall and AUC-ROC.
    """
    metrics = {}

    evaluators = {
        "accuracy": MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="accuracy"
        ),
        "f1": MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="f1"
        ),
        "precision": MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
        ),
        "recall": MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName="weightedRecall"
        ),
    }

    for metric_name, evaluator in evaluators.items():
        metrics[metric_name] = evaluator.evaluate(predictions)

    auc_evaluator = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC",
    )
    metrics["auc_roc"] = auc_evaluator.evaluate(predictions)

    return metrics


def get_confusion_matrix(predictions):
    """
    Returns confusion matrix rows as dictionaries.
    """
    rows = (
        predictions
        .groupBy("label", "prediction")
        .count()
        .orderBy("label", "prediction")
        .collect()
    )

    return [
        {
            "label": float(row["label"]),
            "prediction": float(row["prediction"]),
            "count": int(row["count"]),
        }
        for row in rows
    ]


def save_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def extract_feature_importance(pipeline_model, feature_cols):
    """
    Extracts feature importance or coefficient-based importance where possible.
    Tree models expose featureImportances.
    Logistic Regression exposes coefficients.
    Naive Bayes does not provide direct feature importance in the same way.
    """
    model_stage = pipeline_model.stages[-1]
    model_name = model_stage.__class__.__name__

    importances = []

    if hasattr(model_stage, "featureImportances"):
        values = model_stage.featureImportances.toArray().tolist()
        importances = [
            {"feature": feature_cols[i], "importance": float(values[i])}
            for i in range(len(feature_cols))
        ]

    elif hasattr(model_stage, "coefficients"):
        values = model_stage.coefficients.toArray().tolist()
        importances = [
            {"feature": feature_cols[i], "importance": float(abs(values[i]))}
            for i in range(len(feature_cols))
        ]

    else:
        importances = [
            {"feature": feature, "importance": 0.0}
            for feature in feature_cols
        ]

    importances = sorted(importances, key=lambda x: x["importance"], reverse=True)
    return model_name, importances


def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Reading ML features from: {FEATURE_PATH}")
    df = spark.read.format("delta").load(FEATURE_PATH)

    print("ML feature schema:")
    df.printSchema()

    df = df.dropna(subset=["label"])

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    print(f"Train count: {train_df.count()}")
    print(f"Test count : {test_df.count()}")

    categorical_cols = [
        "primary_type_group",
        "location_group",
        "district_group",
    ]

    numeric_cols = [
        "hour",
        "day_of_week",
        "month",
        "is_weekend",
        "is_night",
        "domestic_numeric",
        "lat_grid",
        "lon_grid_abs",
        "geo_available",
    ]

    indexers = [
        StringIndexer(
            inputCol=c,
            outputCol=f"{c}_idx",
            handleInvalid="keep"
        )
        for c in categorical_cols
    ]

    indexed_cols = [f"{c}_idx" for c in categorical_cols]

    feature_cols = numeric_cols + indexed_cols

    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="keep",
    )

    models = {
        "LogisticRegression": LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=30,
            regParam=0.01,
        ),
        "DecisionTreeClassifier": DecisionTreeClassifier(
            featuresCol="features",
            labelCol="label",
            maxDepth=8,
            seed=42,
        ),
        "RandomForestClassifier": RandomForestClassifier(
            featuresCol="features",
            labelCol="label",
            numTrees=50,
            maxDepth=8,
            seed=42,
        ),
        "GBTClassifier": GBTClassifier(
            featuresCol="features",
            labelCol="label",
            maxIter=30,
            maxDepth=5,
            seed=42,
        ),
        "NaiveBayes": NaiveBayes(
            featuresCol="features",
            labelCol="label",
            smoothing=1.0,
            modelType="multinomial",
        ),
    }

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    all_metrics = []
    best_model_name = None
    best_auc = -1.0
    best_predictions = None
    best_pipeline_model = None

    for model_name, model in models.items():
        print(f"\nTraining model: {model_name}")

        pipeline = Pipeline(stages=indexers + [assembler, model])

        with mlflow.start_run(run_name=model_name):
            pipeline_model = pipeline.fit(train_df)
            predictions = pipeline_model.transform(test_df)

            metrics = evaluate_model(predictions)

            params = {}
            try:
                params = model.extractParamMap()
                params = {
                    param.name: value
                    for param, value in params.items()
                    if isinstance(value, (int, float, str, bool))
                }
            except Exception:
                params = {}

            mlflow.log_param("model_name", model_name)
            mlflow.log_param("target", "arrest")
            mlflow.log_param("feature_count", len(feature_cols))
            mlflow.log_param("train_count", train_df.count())
            mlflow.log_param("test_count", test_df.count())

            for key, value in params.items():
                mlflow.log_param(key, value)

            for metric_name, metric_value in metrics.items():
                mlflow.log_metric(metric_name, metric_value)

            mlflow.spark.log_model(pipeline_model, artifact_path=f"{model_name}_spark_model")

            row = {"model": model_name}
            row.update(metrics)
            all_metrics.append(row)

            print(f"{model_name} metrics:")
            for metric_name, metric_value in metrics.items():
                print(f"  {metric_name}: {metric_value:.4f}")

            if metrics["auc_roc"] > best_auc:
                best_auc = metrics["auc_roc"]
                best_model_name = model_name
                best_predictions = predictions
                best_pipeline_model = pipeline_model

    metrics_path = REPORTS_DIR / "ml_model_metrics.csv"
    save_csv(
        metrics_path,
        all_metrics,
        ["model", "accuracy", "f1", "precision", "recall", "auc_roc"],
    )

    print(f"\nModel metrics saved to: {metrics_path}")

    if best_predictions is not None and best_pipeline_model is not None:
        confusion_rows = get_confusion_matrix(best_predictions)
        confusion_path = REPORTS_DIR / "confusion_matrix_best_model.csv"
        save_csv(
            confusion_path,
            confusion_rows,
            ["label", "prediction", "count"],
        )

        extracted_model_name, importance_rows = extract_feature_importance(
            best_pipeline_model,
            feature_cols,
        )

        importance_path = REPORTS_DIR / "feature_importance_best_model.csv"
        save_csv(
            importance_path,
            importance_rows,
            ["feature", "importance"],
        )

        print(f"Best model: {best_model_name}")
        print(f"Best model internal stage: {extracted_model_name}")
        print(f"Best AUC-ROC: {best_auc:.4f}")
        print(f"Confusion matrix saved to: {confusion_path}")
        print(f"Feature importance saved to: {importance_path}")

    print("\nML training and MLflow logging completed successfully.")


if __name__ == "__main__":
    main()