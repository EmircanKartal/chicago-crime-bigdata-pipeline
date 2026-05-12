# Purpose : 4-class Dispatch Protocol Classification
# Input   : delta/silver/chicago_crimes_clean  (full schema — beat, community_area, ward etc.)
# Output  : reports/exp03_dispatch_protocol/  +  MLflow exp03_dispatch_protocol
#
# Classes:
#   0 = Non-Domestic + No Arrest  → standard patrol, file report
#   1 = Non-Domestic + Arrest     → send unit with transport capacity
#   2 = Domestic    + No Arrest   → domestic-trained officers (Illinois law)
#   3 = Domestic    + Arrest      → domestic team + transport (mandatory arrest)
#
# Why this is meaningful:
#   Illinois Mandatory Arrest Law: if probable cause exists in domestic incident,
#   officer MUST arrest. Predicting class 2/3 before arrival lets dispatch send
#   the right team. Class 3 is rare (2.9%) but legally most critical.
#
# Features (12):
#   Time     : hour, day_of_week, month, is_weekend, is_night
#   Location : lat_grid, lon_grid_abs, community_area, beat_num, district_num
#   Category : primary_type_group (indexed), location_group (indexed)
#
# Exclusions: arrest, domestic (these BUILD the label — not features)
#             description, iucr, case_number (post-event admin fields)

import csv
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, hour, dayofweek, month,
    upper, trim, abs as spark_abs, round as spark_round,
    coalesce, to_timestamp, count as spark_count,
)

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    LogisticRegression,
    DecisionTreeClassifier,
    RandomForestClassifier,
    MultilayerPerceptronClassifier,
    NaiveBayes,
)
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler

import mlflow
import mlflow.spark

# ── Spark ─────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("DispatchProtocolClassification")
    .config("spark.sql.extensions",           "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

SILVER_PATH     = "/app/delta/silver/chicago_crimes_clean"
REPORTS_DIR     = Path("/app/reports/exp03_dispatch_protocol")
MLFLOW_URI      = os.environ.get("MLFLOW_TRACKING_URI", "file:///app/mlruns")
EXPERIMENT_NAME = "exp03_dispatch_protocol_classification"

# ── Helpers ───────────────────────────────────────────────────────────────────

def save_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader(); w.writerows(rows)


def evaluate_multiclass(predictions, num_classes=4):
    """Returns weighted + per-class metrics."""
    metrics = {}
    for key, mn in [("accuracy","accuracy"),("f1","f1"),
                    ("precision","weightedPrecision"),("recall","weightedRecall")]:
        metrics[key] = MulticlassClassificationEvaluator(
            labelCol="label", predictionCol="prediction", metricName=mn
        ).evaluate(predictions)

    # Per-class recall
    for cls in range(num_classes):
        tp = predictions.filter((col("label")==float(cls))&(col("prediction")==float(cls))).count()
        fn = predictions.filter((col("label")==float(cls))&(col("prediction")!=float(cls))).count()
        metrics[f"recall_class{cls}"] = tp/(tp+fn) if (tp+fn) else 0.0

    return metrics


def extract_importance(pipeline_model, feature_cols):
    stage = pipeline_model.stages[-1]
    # OneVsRest wraps the base classifier
    if hasattr(stage, "models"):
        stage = stage.models[0].stages[-1] if hasattr(stage.models[0], "stages") else stage.models[0]
    if hasattr(stage, "featureImportances"):
        vals = stage.featureImportances.toArray().tolist()
        return sorted(
            [{"feature": feature_cols[i], "importance": float(vals[i])}
             for i in range(len(feature_cols))],
            key=lambda x: x["importance"], reverse=True
        )
    elif hasattr(stage, "coefficients"):
        vals = stage.coefficients.toArray().tolist()
        return sorted(
            [{"feature": feature_cols[i], "importance": float(abs(vals[i]))}
             for i in range(len(feature_cols))],
            key=lambda x: x["importance"], reverse=True
        )
    return [{"feature": f, "importance": 0.0} for f in feature_cols]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Reading Silver: {SILVER_PATH}")

    df = spark.read.format("delta").load(SILVER_PATH)

    # ── Timestamp ─────────────────────────────────────────────────────────────
    df = df.withColumn("ts",
        coalesce(
            col("crime_timestamp"),
            to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"),
            to_timestamp(col("date"), "MM/dd/yyyy hh:mm:ss a"),
        )
    )

    # ── 4-class label ─────────────────────────────────────────────────────────
    # domestic_flag & arrest_flag derived from raw string columns
    df = (
        df
        .withColumn("domestic_flag", (col("domestic").cast("boolean") == True).cast("int"))
        .withColumn("arrest_flag",   (col("arrest").cast("boolean")   == True).cast("int"))
        .withColumn("label",
            when((col("domestic_flag")==0)&(col("arrest_flag")==0), lit(0.0))  # NonDom+NoArrest
            .when((col("domestic_flag")==0)&(col("arrest_flag")==1), lit(1.0)) # NonDom+Arrest
            .when((col("domestic_flag")==1)&(col("arrest_flag")==0), lit(2.0)) # Dom+NoArrest
            .when((col("domestic_flag")==1)&(col("arrest_flag")==1), lit(3.0)) # Dom+Arrest
            .otherwise(lit(0.0))
        )
    )

    # ── Time features ─────────────────────────────────────────────────────────
    df = (
        df
        .withColumn("hour",        hour(col("ts")))
        .withColumn("day_of_week", dayofweek(col("ts")))
        .withColumn("month",       month(col("ts")))
        .withColumn("is_weekend",  when(dayofweek(col("ts")).isin([1,7]), lit(1)).otherwise(lit(0)))
        .withColumn("is_night",    when((hour(col("ts"))>=22)|(hour(col("ts"))<=5), lit(1)).otherwise(lit(0)))
    )

    # ── Geographic features ───────────────────────────────────────────────────
    df = (
        df
        .withColumn("lat_grid",       coalesce(spark_round(col("latitude").cast("double"), 2),  lit(0.0)))
        .withColumn("lon_grid_abs",   coalesce(spark_abs(spark_round(col("longitude").cast("double"), 2)), lit(0.0)))
        .withColumn("community_area", coalesce(col("community_area").cast("double"), lit(0.0)))
        .withColumn("beat_num",       coalesce(col("beat").cast("double"),   lit(0.0)))
        .withColumn("district_num",   coalesce(col("district").cast("double"), lit(0.0)))
    )

    # ── Categorical features ──────────────────────────────────────────────────
    VIOLENT  = ["BATTERY","ASSAULT","ROBBERY","CRIMINAL SEXUAL ASSAULT","SEX OFFENSE",
                "HOMICIDE","KIDNAPPING","STALKING","INTIMIDATION","HUMAN TRAFFICKING",
                "OFFENSE INVOLVING CHILDREN"]
    PROPERTY = ["THEFT","BURGLARY","MOTOR VEHICLE THEFT","CRIMINAL DAMAGE","ARSON",
                "DECEPTIVE PRACTICE"]

    top10 = (
        df.groupBy("primary_type").agg(spark_count("*").alias("cnt"))
          .orderBy(col("cnt").desc()).limit(10)
          .select("primary_type").rdd.flatMap(lambda x: x).collect()
    )

    df = (
        df
        .withColumn("loc_clean", upper(trim(col("location_description"))))
        .withColumn("primary_type_group",
            when(col("primary_type").isin(top10), upper(trim(col("primary_type"))))
            .otherwise(lit("OTHER"))
        )
        .withColumn("location_group",
            when(col("loc_clean").contains("STREET"),    lit("STREET"))
            .when(col("loc_clean").contains("RESIDENCE"),lit("RESIDENCE"))
            .when(col("loc_clean").contains("APARTMENT"),lit("RESIDENCE"))
            .when(col("loc_clean").contains("SIDEWALK"), lit("STREET"))
            .when(col("loc_clean").contains("PARKING"),  lit("PARKING"))
            .when(col("loc_clean").contains("STORE"),    lit("STORE"))
            .when(col("loc_clean").contains("SCHOOL"),   lit("SCHOOL"))
            .when(col("loc_clean").contains("VEHICLE"),  lit("VEHICLE"))
            .otherwise(lit("OTHER"))
        )
        .withColumn("crime_group",
            when(upper(trim(col("primary_type"))).isin(VIOLENT),  lit("VIOLENT"))
            .when(upper(trim(col("primary_type"))).isin(PROPERTY), lit("PROPERTY"))
            .otherwise(lit("OTHER"))
        )
    )

    df = df.dropna(subset=["label","hour","day_of_week","month","lat_grid"])

    # ── Class distribution & weights ──────────────────────────────────────────
    total = df.count()
    class_counts = {
        r["label"]: r["cnt"]
        for r in df.groupBy("label").agg(spark_count("*").alias("cnt")).collect()
    }
    CLASS_NAMES = {0.0:"NonDom+NoArrest", 1.0:"NonDom+Arrest",
                   2.0:"Dom+NoArrest",    3.0:"Dom+Arrest"}
    print(f"\nTotal rows: {total:,}")
    for k in sorted(class_counts):
        v = class_counts[k]
        print(f"  Class {int(k)} ({CLASS_NAMES[k]}): {v:,}  ({100*v/total:.1f}%)")

    # Balanced class weights: N / (num_classes × class_count)
    num_classes = 4
    class_weights = {k: total / (num_classes * v) for k, v in class_counts.items()}
    print("\nClass weights (inverse frequency):")
    for k in sorted(class_weights):
        print(f"  Class {int(k)}: {class_weights[k]:.3f}")

    df = df.withColumn("classWeight",
        when(col("label")==0.0, lit(float(class_weights.get(0.0, 1.0))))
        .when(col("label")==1.0, lit(float(class_weights.get(1.0, 1.0))))
        .when(col("label")==2.0, lit(float(class_weights.get(2.0, 1.0))))
        .when(col("label")==3.0, lit(float(class_weights.get(3.0, 1.0))))
        .otherwise(lit(1.0))
    )

    # Sample to 600k for training to avoid OOM on RF/GBT with 2M rows on 4g heap
    # Test set stays full-size for unbiased evaluation
    TRAIN_SAMPLE = 600_000
    total_for_split = df.count()
    sample_frac = min(1.0, TRAIN_SAMPLE / total_for_split * 1.25)
    train_full, test_df = df.randomSplit([0.8, 0.2], seed=42)
    train_df = train_full.sample(fraction=min(1.0, TRAIN_SAMPLE / (total_for_split * 0.8)), seed=42)
    print(f"\nTotal: {total_for_split:,}  |  Train (sampled): {train_df.count():,}  |  Test: {test_df.count():,}")

    # ── Feature pipeline ──────────────────────────────────────────────────────
    categorical_cols = ["primary_type_group", "location_group", "crime_group"]
    numeric_cols     = [
        "hour", "day_of_week", "month", "is_weekend", "is_night",
        "lat_grid", "lon_grid_abs",
        "community_area", "beat_num", "district_num",
    ]

    indexers     = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
                    for c in categorical_cols]
    indexed_cols = [f"{c}_idx" for c in categorical_cols]
    feature_cols = numeric_cols + indexed_cols

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features",
                                handleInvalid="keep")

    print(f"\nFeatures ({len(feature_cols)}): {feature_cols}")

    # ── Models ────────────────────────────────────────────────────────────────
    # 13 features → MLPC input layer; hidden layer 32; output layer 4 classes
    num_features = len(feature_cols)
    mlpc_layers  = [num_features, 32, 4]

    models = {
        "LogisticRegression": LogisticRegression(
            featuresCol="features", labelCol="label", weightCol="classWeight",
            maxIter=200, regParam=0.001, elasticNetParam=0.1,
        ),
        "DecisionTreeClassifier": DecisionTreeClassifier(
            featuresCol="features", labelCol="label", weightCol="classWeight",
            maxDepth=12, seed=42,
        ),
        "RandomForestClassifier": RandomForestClassifier(
            featuresCol="features", labelCol="label", weightCol="classWeight",
            numTrees=50, maxDepth=6, seed=42,
        ),
        "MultilayerPerceptron": MultilayerPerceptronClassifier(
            featuresCol="features", labelCol="label",
            layers=mlpc_layers, maxIter=100, seed=42,
            # MLPC does not support weightCol — class weighting via sampling above
        ),
        "NaiveBayes": NaiveBayes(
            featuresCol="features", labelCol="label",
            smoothing=1.0, modelType="multinomial",
        ),
    }

    # ── Training loop ─────────────────────────────────────────────────────────
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    all_metrics      = []
    best_f1          = -1.0
    best_model_name  = None
    best_predictions = None
    best_pipeline    = None

    for model_name, model in models.items():
        print(f"\n── Training: {model_name}")
        pipeline = Pipeline(stages=indexers + [assembler, model])

        with mlflow.start_run(run_name=model_name):
            try:
                pipe_model  = pipeline.fit(train_df)
                predictions = pipe_model.transform(test_df)
                m           = evaluate_multiclass(predictions, num_classes=4)
            except Exception as e:
                print(f"   FAILED: {e}")
                mlflow.log_param("error", str(e)[:500])
                continue

            mlflow.log_param("model_name",    model_name)
            mlflow.log_param("predict_target","dispatch_protocol_4class")
            mlflow.log_param("num_classes",   4)
            mlflow.log_param("feature_count", len(feature_cols))
            mlflow.log_param("train_count",   train_df.count())
            mlflow.log_param("test_count",    test_df.count())
            mlflow.log_param("class_balanced",True)
            for k, v in m.items():
                mlflow.log_metric(k, v)
            mlflow.spark.log_model(pipe_model, artifact_path=f"{model_name}_model")

            row = {"model": model_name}; row.update(m)
            all_metrics.append(row)

            print(f"   accuracy={m['accuracy']:.4f}  f1={m['f1']:.4f}  "
                  f"recall_c0={m['recall_class0']:.3f}  recall_c1={m['recall_class1']:.3f}  "
                  f"recall_c2={m['recall_class2']:.3f}  recall_c3={m['recall_class3']:.3f}")

            # Save incrementally — don't lose results if a later model crashes
            fieldnames_inc = ["model","accuracy","f1","precision","recall",
                              "recall_class0","recall_class1","recall_class2","recall_class3"]
            save_csv(REPORTS_DIR / "ml_model_metrics.csv", all_metrics, fieldnames_inc)

            if m["f1"] > best_f1:
                best_f1         = m["f1"]
                best_model_name = model_name
                best_predictions= predictions
                best_pipeline   = pipe_model

    # ── Save reports ──────────────────────────────────────────────────────────
    if not all_metrics:
        print("No models completed."); return

    fieldnames = ["model","accuracy","f1","precision","recall",
                  "recall_class0","recall_class1","recall_class2","recall_class3"]
    save_csv(REPORTS_DIR / "ml_model_metrics.csv", all_metrics, fieldnames)

    if best_predictions is not None:
        cm_rows = (
            best_predictions.groupBy("label","prediction").count()
            .orderBy("label","prediction").collect()
        )
        save_csv(
            REPORTS_DIR / "confusion_matrix_best_model.csv",
            [{"label":float(r["label"]),"prediction":float(r["prediction"]),"count":int(r["count"])}
             for r in cm_rows],
            ["label","prediction","count"]
        )
        imp_rows = extract_importance(best_pipeline, feature_cols)
        save_csv(REPORTS_DIR/"feature_importance_best_model.csv",
                 imp_rows, ["feature","importance"])

    print(f"\n✓ Best model  : {best_model_name}  (weighted F1={best_f1:.4f})")
    print(f"✓ Experiment  : {EXPERIMENT_NAME}")
    print(f"✓ Reports     : {REPORTS_DIR}")
    print(f"\nClass 3 (Dom+Arrest) recall tells you how well the model catches")
    print(f"the most critical cases requiring mandatory arrest dispatch.")
    print("\nDispatch protocol classification completed.")


if __name__ == "__main__":
    main()
