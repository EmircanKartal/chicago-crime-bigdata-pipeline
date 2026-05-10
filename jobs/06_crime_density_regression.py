# Experiment 2 — Crime Density Regression + Patrol Heatmap
#
# CONCEPT: predict HOW MANY crimes will happen in each geographic grid cell
# for a given hour/day combination → output drives a patrol heatmap.
#
# Real-world use: given tomorrow is Saturday night, which Chicago grid cells
# need the most police units? This model answers that directly.
#
# Input:  delta/silver/chicago_crimes_clean  (2M rows)
# Output: reports/exp02_density/regression_metrics.csv
#         reports/exp02_density/feature_importance.csv
#         dashboard/figures/exp02_density/crime_heatmap_*.png
#         MLflow experiment: exp02_crime_density_regression

import os
import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, hour, dayofweek, month,
    when, lit, round as spark_round,
    abs as spark_abs, coalesce, avg,
)
from pyspark.ml import Pipeline
from pyspark.ml.regression import (
    LinearRegression,
    DecisionTreeRegressor,
    RandomForestRegressor,
    GBTRegressor,
    GeneralizedLinearRegression,
)
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler

import mlflow
import mlflow.spark

spark = (
    SparkSession.builder
    .appName("CrimeDensityRegression")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

SILVER_PATH     = "/app/delta/silver/chicago_crimes_clean"
REPORTS_DIR     = Path("/app/reports/exp02_density")
FIGURES_DIR_STR = "/app/reports/exp02_density/figures"
MLFLOW_URI      = os.environ.get("MLFLOW_TRACKING_URI", "file:///app/mlruns")
EXPERIMENT_NAME = "exp02_crime_density_regression"


def save_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    Path(FIGURES_DIR_STR).mkdir(parents=True, exist_ok=True)

    print(f"Reading Silver: {SILVER_PATH}")
    df = spark.read.format("delta").load(SILVER_PATH)

    # ── Aggregate: count crimes per (lat_grid, lon_grid_abs, hour, day_of_week, month)
    # This creates the regression dataset:
    #   "how many crimes happened in this grid cell at this time?"
    print("Aggregating crime counts per grid cell × time window...")

    crime_density = (
        df
        .withColumn("lat_grid",
            coalesce(spark_round(col("latitude").cast("double"), 2), lit(0.0)))
        .withColumn("lon_grid_abs",
            spark_abs(coalesce(spark_round(col("longitude").cast("double"), 2), lit(0.0))))
        .withColumn("hour_of_day", hour(col("crime_timestamp")))
        .withColumn("day_of_week", dayofweek(col("crime_timestamp")))
        .withColumn("month_num",   month(col("crime_timestamp")))
        .withColumn("is_weekend",
            when(col("day_of_week").isin([1, 7]), lit(1)).otherwise(lit(0)))
        .withColumn("is_night",
            when((col("hour_of_day") >= 22) | (col("hour_of_day") <= 5),
                 lit(1)).otherwise(lit(0)))
        .filter(col("lat_grid") != 0.0)  # remove unknown coords
        .filter(col("crime_timestamp").isNotNull())
        .groupBy("lat_grid", "lon_grid_abs", "hour_of_day",
                 "day_of_week", "month_num", "is_weekend", "is_night")
        .agg(count("*").alias("crime_count"))
        .filter(col("crime_count") >= 1)
    )

    total_cells = crime_density.count()
    print(f"Grid cells × time windows: {total_cells:,}")
    crime_density.describe("crime_count").show()

    # ── Features & target ─────────────────────────────────────────────────
    feature_cols = [
        "lat_grid", "lon_grid_abs",
        "hour_of_day", "day_of_week", "month_num",
        "is_weekend", "is_night",
    ]

    assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="features", handleInvalid="keep"
    )
    crime_density = assembler.transform(crime_density).cache()

    train_df, test_df = crime_density.randomSplit([0.8, 0.2], seed=42)
    print(f"Train: {train_df.count():,}  |  Test: {test_df.count():,}")

    # ── 5 Regression models ───────────────────────────────────────────────
    models = {
        "LinearRegression": LinearRegression(
            featuresCol="features", labelCol="crime_count",
            maxIter=100, regParam=0.01,
        ),
        "DecisionTreeRegressor": DecisionTreeRegressor(
            featuresCol="features", labelCol="crime_count",
            maxDepth=8, seed=42,
        ),
        "RandomForestRegressor": RandomForestRegressor(
            featuresCol="features", labelCol="crime_count",
            numTrees=50, maxDepth=8, seed=42,
        ),
        "GBTRegressor": GBTRegressor(
            featuresCol="features", labelCol="crime_count",
            maxIter=30, maxDepth=5, stepSize=0.1, seed=42,
        ),
        "GeneralizedLinearRegression": GeneralizedLinearRegression(
            featuresCol="features", labelCol="crime_count",
            family="poisson", link="log", maxIter=50, regParam=0.01,
        ),
    }

    evaluators = {
        "rmse": RegressionEvaluator(labelCol="crime_count",
                                    predictionCol="prediction",
                                    metricName="rmse"),
        "mae":  RegressionEvaluator(labelCol="crime_count",
                                    predictionCol="prediction",
                                    metricName="mae"),
        "r2":   RegressionEvaluator(labelCol="crime_count",
                                    predictionCol="prediction",
                                    metricName="r2"),
    }

    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    all_metrics      = []
    best_model_name  = None
    best_r2          = -9999.0
    best_pipeline    = None
    best_predictions = None

    for model_name, model in models.items():
        print(f"\n── Training: {model_name}")
        pipeline = Pipeline(stages=[model])

        with mlflow.start_run(run_name=model_name):
            try:
                pipe_model  = pipeline.fit(train_df)
                predictions = pipe_model.transform(test_df)

                metrics = {k: ev.evaluate(predictions)
                           for k, ev in evaluators.items()}

                mlflow.log_param("model_name",    model_name)
                mlflow.log_param("target",        "crime_count")
                mlflow.log_param("feature_count", len(feature_cols))
                mlflow.log_param("train_cells",   train_df.count())
                mlflow.log_param("test_cells",    test_df.count())
                for k, v in metrics.items():
                    mlflow.log_metric(k, v)
                mlflow.spark.log_model(pipe_model,
                                       artifact_path=f"{model_name}_model")

                row = {"model": model_name}; row.update(metrics)
                all_metrics.append(row)

                print(f"   RMSE={metrics['rmse']:.4f}  "
                      f"MAE={metrics['mae']:.4f}  "
                      f"R²={metrics['r2']:.4f}")

                if metrics["r2"] > best_r2:
                    best_r2          = metrics["r2"]
                    best_model_name  = model_name
                    best_pipeline    = pipe_model
                    best_predictions = predictions

            except Exception as e:
                print(f"   FAILED: {e}")
                mlflow.log_param("error", str(e)[:500])

    # ── Save regression metrics ───────────────────────────────────────────
    save_csv(REPORTS_DIR / "regression_metrics.csv",
             all_metrics, ["model", "rmse", "mae", "r2"])

    # Feature importance for best tree model
    if best_pipeline:
        stage = best_pipeline.stages[-1]
        if hasattr(stage, "featureImportances"):
            imp = [{"feature": feature_cols[i],
                    "importance": float(stage.featureImportances.toArray()[i])}
                   for i in range(len(feature_cols))]
            imp.sort(key=lambda x: x["importance"], reverse=True)
            save_csv(REPORTS_DIR / "feature_importance.csv",
                     imp, ["feature", "importance"])

    print(f"\n✓ Best model : {best_model_name}  R²={best_r2:.4f}")
    print(f"✓ Reports    : {REPORTS_DIR}")

    # ── Generate crime heatmap (total crimes per grid cell) ───────────────
    print("\nGenerating patrol heatmap data...")
    heatmap_data = (
        crime_density
        .groupBy("lat_grid", "lon_grid_abs")
        .agg(count("*").alias("time_windows"),
             avg("crime_count").alias("avg_crimes"))
        .toPandas()
    )
    heatmap_data.to_csv(str(REPORTS_DIR / "heatmap_data.csv"), index=False)
    print(f"Heatmap CSV: {REPORTS_DIR}/heatmap_data.csv  ({len(heatmap_data):,} cells)")
    print("Run notebook 06_dashboard_figures.ipynb to generate the heatmap PNG.")

    print("\nCrime density regression completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
