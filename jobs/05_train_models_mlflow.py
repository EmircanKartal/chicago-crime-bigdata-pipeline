# Owner: Berfin
# Purpose: Train 5 classifiers, track in MLflow
# Input:  delta/gold/ml_features  (arrest binary target)
# Output: reports/ml_model_metrics.csv
#         reports/confusion_matrix_best_model.csv
#         reports/feature_importance_best_model.csv

import csv
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier,
    GBTClassifier,
    NaiveBayes,
    OneVsRest,
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
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

FEATURE_PATH = "/app/delta/gold/ml_features"
MLFLOW_URI   = os.environ.get("MLFLOW_TRACKING_URI", "file:///app/mlruns")

# Experiment versioning — set EXP_ID to keep runs separate, e.g.:
#   EXP_ID=exp01_arrest_2m     → reports/exp01_arrest_2m/
#   EXP_ID=exp02_crime_group   → reports/exp02_crime_group/
#   EXP_ID=exp03_domestic      → reports/exp03_domestic/
EXP_ID       = os.environ.get("EXP_ID", "exp01_arrest_2m")
REPORTS_DIR  = Path(f"/app/reports/{EXP_ID}")

# Target variable (arrest | crime_group | domestic)
PREDICT_TARGET = os.environ.get("PREDICT_TARGET", "arrest").strip().lower()

EXPERIMENT_MAP = {
    "arrest":      f"exp01_chicago_arrest_classification",
    "crime_group": f"exp02_chicago_crime_group",
    "domestic":    f"exp03_chicago_domestic_prediction",
}
EXPERIMENT_NAME = EXPERIMENT_MAP.get(PREDICT_TARGET, f"{EXP_ID}_{PREDICT_TARGET}")


# ── Helpers ───────────────────────────────────────────────────────────────────

def evaluate_model(predictions, is_binary=True):
    metrics = {}
    for key, mn in [
        ("accuracy",  "accuracy"),
        ("f1",        "f1"),
        ("precision", "weightedPrecision"),
        ("recall",    "weightedRecall"),
    ]:
        metrics[key] = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName=mn
        ).evaluate(predictions)

    if is_binary:
        try:
            metrics["auc_roc"] = BinaryClassificationEvaluator(
                labelCol="label", rawPredictionCol="rawPrediction",
                metricName="areaUnderROC"
            ).evaluate(predictions)
        except Exception:
            metrics["auc_roc"] = 0.0
        tp = predictions.filter((col("label")==1.0)&(col("prediction")==1.0)).count()
        fn = predictions.filter((col("label")==1.0)&(col("prediction")==0.0)).count()
        metrics["recall_arrested"] = tp/(tp+fn) if (tp+fn) > 0 else 0.0
    else:
        metrics["auc_roc"] = 0.0        # not defined for multiclass
        metrics["recall_arrested"] = 0.0

    return metrics


def get_confusion_matrix(predictions):
    rows = (
        predictions.groupBy("label", "prediction")
        .count().orderBy("label", "prediction").collect()
    )
    return [{"label": float(r["label"]),
             "prediction": float(r["prediction"]),
             "count": int(r["count"])} for r in rows]


def save_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def extract_feature_importance(pipeline_model, feature_cols):
    stage = pipeline_model.stages[-1]
    name  = stage.__class__.__name__
    if hasattr(stage, "featureImportances"):
        vals = stage.featureImportances.toArray().tolist()
        imp  = [{"feature": feature_cols[i], "importance": float(vals[i])}
                for i in range(len(feature_cols))]
    elif hasattr(stage, "coefficients"):
        vals = stage.coefficients.toArray().tolist()
        imp  = [{"feature": feature_cols[i], "importance": float(abs(vals[i]))}
                for i in range(len(feature_cols))]
    else:
        imp = [{"feature": f, "importance": 0.0} for f in feature_cols]
    return name, sorted(imp, key=lambda x: x["importance"], reverse=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Reading ML features : {FEATURE_PATH}")
    print(f"Predict target      : {PREDICT_TARGET}")
    print(f"Experiment ID       : {EXP_ID}")
    print(f"MLflow experiment   : {EXPERIMENT_NAME}")
    print(f"Reports dir         : {REPORTS_DIR}")

    df = (
        spark.read.format("delta").load(FEATURE_PATH)
        .dropna(subset=["label"])
        # Fill any remaining NaN in numeric cols — lat/lon null for ~1.4% of records
        .fillna(0.0, subset=["lat_grid", "lon_grid_abs", "district", "beat", "community_area"])
    )

    # ── Build label based on target ───────────────────────────────────────
    if PREDICT_TARGET == "crime_group":
        # Drop existing 'label' (arrest) before creating crime_group label
        df = df.drop("label")
        label_indexer = StringIndexer(
            inputCol="crime_group", outputCol="label",
            handleInvalid="keep", stringOrderType="frequencyDesc"
        )
        df = label_indexer.fit(df).transform(df)
        is_binary = False
        print("Crime group label mapping:")
        df.groupBy("crime_group", "label").count().orderBy("label").show()
    elif PREDICT_TARGET == "domestic":
        df = df.drop("label")
        df = df.withColumn("label", col("domestic_numeric").cast("double"))
        is_binary = True
    else:  # arrest (default) — label column already exists
        is_binary = True

    total = df.count()
    pos   = df.filter(col("label") == 1.0).count()
    neg   = total - pos
    print(f"Rows: {total:,}  |  Positive: {pos:,} ({100*pos/total:.1f}%)  |  Negative: {neg:,}")

    # Class-weight balancing (fixes low recall on arrested=minority class)
    weight_pos = neg / total
    weight_neg = pos / total
    df = df.withColumn(
        "classWeight",
        when(col("label") == 1.0, lit(float(weight_pos)))
        .otherwise(lit(float(weight_neg)))
    )

    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
    print(f"Train: {train_df.count():,}  |  Test: {test_df.count():,}")

    # ── Feature columns ───────────────────────────────────────────────────
    categorical_cols = [
        "primary_type_group",   # strongest predictor (narcotics vs theft arrest rate)
        "crime_group",          # VIOLENT/PROPERTY/OTHER
        "location_group",       # STREET/RESIDENCE/etc
    ]
    numeric_cols = [
        "hour", "day_of_week", "month", "is_weekend", "is_night",
        "domestic_numeric",
        "district", "beat", "community_area",
        "lat_grid", "lon_grid_abs",
    ]

    indexers     = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
                    for c in categorical_cols]
    indexed_cols = [f"{c}_idx" for c in categorical_cols]
    feature_cols = numeric_cols + indexed_cols
    assembler    = VectorAssembler(inputCols=feature_cols, outputCol="features",
                                   handleInvalid="keep")

    print(f"Features ({len(feature_cols)}): {feature_cols}")

    # ── Models ────────────────────────────────────────────────────────────
    # GBT only supports binary — wrap in OneVsRest for multiclass targets
    gbt_base = GBTClassifier(
        featuresCol="features", labelCol="label",
        maxIter=30, maxDepth=5, stepSize=0.1, seed=42,
    )
    gbt_model = (gbt_base if is_binary
                 else OneVsRest(classifier=gbt_base, labelCol="label",
                                featuresCol="features"))

    models = {
        "LogisticRegression": LogisticRegression(
            featuresCol="features", labelCol="label",
            maxIter=100, regParam=0.01, elasticNetParam=0.1,
            **({"weightCol": "classWeight"} if is_binary else {}),
        ),
        "DecisionTreeClassifier": DecisionTreeClassifier(
            featuresCol="features", labelCol="label", maxDepth=10, seed=42,
            **({"weightCol": "classWeight"} if is_binary else {}),
        ),
        "RandomForestClassifier": RandomForestClassifier(
            featuresCol="features", labelCol="label",
            numTrees=50, maxDepth=8, seed=42,
            **({"weightCol": "classWeight"} if is_binary else {}),
        ),
        "GBTClassifier": gbt_model,
        "NaiveBayes": NaiveBayes(
            featuresCol="features", labelCol="label",
            smoothing=1.0, modelType="multinomial",
        ),
    }

    # ── MLflow loop ───────────────────────────────────────────────────────
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    all_metrics      = []
    best_model_name  = None
    best_auc         = -1.0
    best_predictions = None
    best_pipeline    = None

    for model_name, model in models.items():
        print(f"\n── Training: {model_name}")
        pipeline = Pipeline(stages=indexers + [assembler, model])

        with mlflow.start_run(run_name=model_name):
            try:
                pipe_model  = pipeline.fit(train_df)
                predictions = pipe_model.transform(test_df)
                metrics     = evaluate_model(predictions, is_binary=is_binary)
            except Exception as e:
                print(f"   FAILED: {e}")
                mlflow.log_param("error", str(e)[:500])
                continue

            # Log to MLflow
            mlflow.log_param("model_name",       model_name)
            mlflow.log_param("target",           "arrest")
            mlflow.log_param("feature_count",    len(feature_cols))
            mlflow.log_param("train_rows",       train_df.count())
            mlflow.log_param("test_rows",        test_df.count())
            mlflow.log_param("class_balanced",   True)
            mlflow.log_param("weight_arrested",  round(weight_pos, 4))
            for k, v in metrics.items():
                mlflow.log_metric(k, v)
            mlflow.spark.log_model(pipe_model, artifact_path=f"{model_name}_model")

            row = {"model": model_name}
            row.update(metrics)
            all_metrics.append(row)

            print(f"   accuracy={metrics['accuracy']:.4f}  "
                  f"f1={metrics['f1']:.4f}  "
                  f"auc_roc={metrics['auc_roc']:.4f}  "
                  f"recall_arrested={metrics['recall_arrested']:.4f}")

            if metrics["auc_roc"] > best_auc:
                best_auc        = metrics["auc_roc"]
                best_model_name = model_name
                best_predictions= predictions
                best_pipeline   = pipe_model

    # ── Save reports ──────────────────────────────────────────────────────
    fieldnames = ["model", "accuracy", "f1", "precision", "recall",
                  "auc_roc", "recall_arrested"]
    save_csv(REPORTS_DIR / "ml_model_metrics.csv", all_metrics, fieldnames)

    if best_predictions is not None:
        save_csv(REPORTS_DIR / "confusion_matrix_best_model.csv",
                 get_confusion_matrix(best_predictions),
                 ["label", "prediction", "count"])

        _, imp_rows = extract_feature_importance(best_pipeline, feature_cols)
        save_csv(REPORTS_DIR / "feature_importance_best_model.csv",
                 imp_rows, ["feature", "importance"])

    print(f"\n✓ Best model     : {best_model_name}  AUC={best_auc:.4f}")
    print(f"✓ Reports        : {REPORTS_DIR}")
    print(f"✓ MLflow exp     : {EXPERIMENT_NAME}")
    print("ML training and MLflow logging completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
