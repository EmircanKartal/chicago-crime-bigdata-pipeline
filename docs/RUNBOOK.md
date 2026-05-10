# Chicago Crime Pipeline — Runbook

Step-by-step guide to build, run, and operate the full pipeline from scratch.  
Every command you need is here — no guessing.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [First-Time Setup](#2-first-time-setup)
3. [Step 1 — Build Docker Images](#3-step-1--build-docker-images)
4. [Step 2 — Start All Services](#4-step-2--start-all-services)
5. [Step 3 — Stream Data into Kafka (Producer)](#5-step-3--stream-data-into-kafka-producer)
6. [Step 4 — Kafka → Bronze Delta](#6-step-4--kafka--bronze-delta)
7. [Step 5 — Bronze → Silver Delta](#7-step-5--bronze--silver-delta)
8. [Step 6 — Silver → Gold Delta](#8-step-6--silver--gold-delta)
9. [Step 7 — Feature Engineering → ML Features Delta](#9-step-7--feature-engineering--ml-features-delta)
10. [Step 8 — Train 5 ML Models + MLflow](#10-step-8--train-5-ml-models--mlflow)
11. [Step 9 — Open MLflow UI](#11-step-9--open-mlflow-ui)
12. [Running Notebooks](#12-running-notebooks)
13. [Full Pipeline — One Block](#13-full-pipeline--one-block)
14. [Re-running After Changes](#14-re-running-after-changes)
15. [Verify Row Counts](#15-verify-row-counts)
16. [Troubleshooting](#16-troubleshooting)
17. [Service URLs](#17-service-urls)

---

## 1. Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Docker Desktop | ≥ 4.x | https://www.docker.com/products/docker-desktop |
| Docker Compose | ≥ 2.x | bundled with Docker Desktop |
| Python | 3.9+ | for local notebooks only |

> **Mac with Apple Silicon (M1/M2/M3):** all images work on `aarch64` — no extra config needed.

---

## 2. First-Time Setup

```bash
# Clone the repo
git clone <repo-url>
cd chicago-crime-bigdata-pipeline

# Create the virtual env for local notebooks (not required for Docker jobs)
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark==4.0.0 delta-spark==4.0.1 jupyterlab pandas matplotlib seaborn
```

---

## 3. Step 1 — Build Docker Images

> **Run this once, and again whenever `services/spark/Dockerfile` changes.**

```bash
docker compose build --no-cache spark-master spark-worker
```

**What gets built into the image:**
- Python packages: pandas, numpy, matplotlib, scikit-learn, mlflow, delta-spark
- Delta Lake JARs: `delta-spark_2.12-3.1.0.jar`, `delta-storage-3.1.0.jar`
- Kafka connector JARs: `spark-sql-kafka-0-10_2.12-3.5.1.jar`, `spark-token-provider-kafka-0-10_2.12-3.5.1.jar`, `kafka-clients-3.4.1.jar`, `commons-pool2-2.12.0.jar`

**Expected output (last few lines):**
```
=> exporting to image
=> => writing image sha256:...
=> => naming to docker.io/library/chicago-crime-bigdata-pipeline-spark-master
```

---

## 4. Step 2 — Start All Services

```bash
docker compose up -d
```

**Verify all containers are running:**
```bash
docker compose ps
```

**Expected — all should show `running`:**
```
chicago_zookeeper      running
chicago_kafka          running
chicago_spark_master   running
chicago_spark_worker   running
chicago_producer       running
chicago_mlflow         running
```

**If a container is not running:**
```bash
docker compose logs <container-name>
# Example:
docker compose logs chicago_kafka
```

---

## 5. Step 3 — Stream Data into Kafka (Producer)

Reads `data/raw/chicago_crimes_sample.csv` (100,000 rows) and sends each row as a JSON message to the Kafka topic `chicago_crimes_raw` at 500 msg/sec.

```bash
docker compose exec producer python /app/app/producer.py
```

**Expected output:**
```
[INFO] Kafka Producer starting...
[INFO] Max messages: 100000
[INFO] Produce rate: 500 msg/sec
[INFO] 100 messages sent to topic 'chicago_crimes_raw'
...
[INFO] 100000 messages sent to topic 'chicago_crimes_raw'
[SUCCESS] Producer finished. Total messages sent: 100000
```

**Duration:** ~3–4 minutes at 500 msg/sec.

> **Note:** If you need to re-run the producer, the old Kafka messages stay in the topic.  
> To start fresh, wipe Delta data first (see [Re-running After Changes](#14-re-running-after-changes)).

---

## 6. Step 4 — Kafka → Bronze Delta

Reads all messages from the Kafka topic (from offset 0) and writes raw JSON payloads to the Bronze Delta table.

```bash
docker compose exec spark-master spark-submit /app/jobs/01_stream_kafka_to_bronze.py
```

**Expected output (last lines):**
```
[SUCCESS] Bronze Delta written to: /app/delta/bronze/chicago_crimes_raw
```

**Output location:** `delta/bronze/chicago_crimes_raw/`  
**Expected rows:** ~100,000

---

## 7. Step 5 — Bronze → Silver Delta

Parses JSON, type-casts all columns, parses `crime_timestamp`, drops nulls on key fields, deduplicates on `crime_id`.

```bash
docker compose exec spark-master spark-submit /app/jobs/02_bronze_to_silver.py
```

**Expected output:**
```
[INFO] Before cleaning row count: 100000
[INFO] After null cleaning row count: ~100000
[INFO] After duplicate cleaning row count: ~100000
[SUCCESS] Silver Delta written to: /app/delta/silver/chicago_crimes_clean
```

**Output location:** `delta/silver/chicago_crimes_clean/`

---

## 8. Step 6 — Silver → Gold Delta

Adds engineered time columns (`crime_hour`, `crime_day_of_week`, `crime_month`, `is_weekend`, `is_night`) and integer flags (`arrest_int`, `domestic_int`).

```bash
docker compose exec spark-master spark-submit /app/jobs/03_silver_to_gold.py
```

**Expected output:**
```
[INFO] Gold row count: ~100000
[SUCCESS] Gold Delta written to: /app/delta/gold/chicago_crimes_features
```

**Output location:** `delta/gold/chicago_crimes_features/`

---

## 9. Step 7 — Feature Engineering → ML Features Delta

Reads from Silver Delta, builds 12 ML features + label (`arrest` → 0/1), writes to `ml_features` Delta table.

```bash
docker compose exec spark-master spark-submit /app/jobs/04_feature_engineering.py
```

**Expected output:**
```
Reading silver Delta table from: /app/delta/silver/chicago_crimes_clean
Top 10 primary crime types: [BATTERY, THEFT, ...]
Feature row count: ~98000
Feature engineering completed successfully.
```

**Output location:** `delta/gold/ml_features/`  
**Features produced:** `hour`, `day_of_week`, `month`, `is_weekend`, `is_night`, `domestic_numeric`, `lat_grid`, `lon_grid_abs`, `geo_available`, `primary_type_group`, `location_group`, `district_group`  
**Label:** `label` = 1 if arrested, 0 otherwise

---

## 10. Step 8 — Train 5 ML Models + MLflow

Trains 5 classifiers, evaluates with AUC-ROC / Accuracy / F1 / Precision / Recall, logs everything to MLflow, saves reports.

```bash
docker compose exec spark-master spark-submit /app/jobs/05_train_models_mlflow.py
```

**Expected output:**
```
Train count: ~78000
Test count : ~20000

Training model: LogisticRegression
  accuracy: 0.xxxx  f1: 0.xxxx  auc_roc: 0.xxxx
Training model: DecisionTreeClassifier ...
Training model: RandomForestClassifier ...
Training model: GBTClassifier ...
Training model: NaiveBayes ...

Best model: <ModelName>
Best AUC-ROC: 0.xxxx
ML training and MLflow logging completed successfully.
```

**Duration:** ~5–15 minutes (5 models × training + evaluation).

**Output files:**
| File | Content |
|------|---------|
| `reports/ml_model_metrics.csv` | All 5 models with all metrics |
| `reports/confusion_matrix_best_model.csv` | Best model confusion matrix |
| `reports/feature_importance_best_model.csv` | Feature importances |
| `mlruns/` | MLflow experiment artifacts |

---

## 11. Step 9 — Open MLflow UI

```bash
open http://localhost:5001
```

Or navigate to `http://localhost:5001` in your browser.

You will see the experiment `chicago_crime_arrest_classification` with 5 runs (one per model), comparing all metrics side by side.

---

## 12. Running Notebooks

Notebooks run **locally** with the `.venv` Python (not inside Docker).  
They use `local[*]` PySpark, so Spark runs on your machine.

```bash
# Activate venv and start Jupyter
source .venv/bin/activate
cd notebooks
jupyter notebook
```

**Notebook order:**

| Notebook | Purpose | Data source |
|----------|---------|-------------|
| `01_data_understanding.ipynb` | Schema exploration, first look | CSV |
| `02_streaming_delta_pipeline.ipynb` | Pipeline architecture overview | — |
| `03_eda.ipynb` | EDA — stats, charts, distributions | Delta Gold |
| `04_feature_engineering.ipynb` | Feature logic with business explanations | CSV (100k) |
| `05_ml_models_mlflow.ipynb` | Model training, comparison | Delta ml_features |
| `06_dashboard_figures.ipynb` | Dashboard charts | Delta Gold + reports |

**Important for `03_eda.ipynb` and Delta notebooks:**

```
Kernel → Restart Kernel → Run All Cells
```

Delta notebooks need the JARs loaded before PySpark starts — always do a full restart before running.

**For `03_eda.ipynb` set `RUN_DATA_SOURCE`:**
- `'gold'` → reads `delta/gold/chicago_crimes_features` (pipeline data)
- `'csv'` → reads `data/raw/chicago_crimes_sample.csv` (full 100k, timestamps work)

---

## 13. Full Pipeline — One Block

Copy-paste this to run the entire pipeline end to end:

```bash
# Build images (only needed once or after Dockerfile changes)
docker compose build --no-cache spark-master spark-worker

# Start services
docker compose up -d

# Wait ~10 seconds for Kafka to be ready, then:

# Stream 100k rows into Kafka
docker compose exec producer python /app/app/producer.py

# Run pipeline jobs in order
docker compose exec spark-master spark-submit /app/jobs/01_stream_kafka_to_bronze.py
docker compose exec spark-master spark-submit /app/jobs/02_bronze_to_silver.py
docker compose exec spark-master spark-submit /app/jobs/03_silver_to_gold.py
docker compose exec spark-master spark-submit /app/jobs/04_feature_engineering.py
docker compose exec spark-master spark-submit /app/jobs/05_train_models_mlflow.py

# Open MLflow UI
open http://localhost:5001
```

---

## 14. Re-running After Changes

### Wipe Delta data and start pipeline fresh

Use this when you want to rebuild Delta tables from scratch (e.g., after fixing a bug in a job):

```bash
# Delete all Delta tables and streaming checkpoints
docker compose exec spark-master bash -c \
  "rm -rf /app/delta/bronze /app/delta/silver /app/delta/gold /app/delta/checkpoints"

# Then re-run from producer onwards (Step 3 → Step 8)
docker compose exec producer python /app/app/producer.py
docker compose exec spark-master spark-submit /app/jobs/01_stream_kafka_to_bronze.py
docker compose exec spark-master spark-submit /app/jobs/02_bronze_to_silver.py
docker compose exec spark-master spark-submit /app/jobs/03_silver_to_gold.py
docker compose exec spark-master spark-submit /app/jobs/04_feature_engineering.py
docker compose exec spark-master spark-submit /app/jobs/05_train_models_mlflow.py
```

### Re-run only from Silver onwards (Bronze is fine)

```bash
docker compose exec spark-master spark-submit /app/jobs/02_bronze_to_silver.py
docker compose exec spark-master spark-submit /app/jobs/03_silver_to_gold.py
docker compose exec spark-master spark-submit /app/jobs/04_feature_engineering.py
docker compose exec spark-master spark-submit /app/jobs/05_train_models_mlflow.py
```

### Re-run only ML (features are fine)

```bash
docker compose exec spark-master spark-submit /app/jobs/05_train_models_mlflow.py
```

### Stop everything

```bash
docker compose down
```

### Stop and wipe all Docker volumes + images

```bash
docker compose down --volumes --rmi local
```

---

## 15. Verify Row Counts

Check how many rows are in each Delta layer:

```bash
docker compose exec spark-master python3 - << 'EOF'
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("verify")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

tables = {
    "Bronze":       "/app/delta/bronze/chicago_crimes_raw",
    "Silver":       "/app/delta/silver/chicago_crimes_clean",
    "Gold":         "/app/delta/gold/chicago_crimes_features",
    "ML Features":  "/app/delta/gold/ml_features",
}

for name, path in tables.items():
    try:
        n = spark.read.format("delta").load(path).count()
        print(f"  {name:15}: {n:,} rows")
    except Exception as e:
        print(f"  {name:15}: NOT FOUND ({e})")

spark.stop()
EOF
```

**Expected output after full pipeline:**
```
  Bronze         : 100,000 rows
  Silver         : ~100,000 rows
  Gold           : ~100,000 rows
  ML Features    : ~98,000 rows
```

---

## 16. Troubleshooting

### `Failed to find data source: kafka`

**Cause:** Spark image was not rebuilt after Kafka JARs were added to the Dockerfile.

```bash
docker compose down
docker compose build --no-cache spark-master spark-worker
docker compose up -d
```

Verify JARs exist:
```bash
docker compose exec spark-master ls /opt/spark/jars/ | grep kafka
# Should show: spark-sql-kafka-0-10_2.12-3.5.1.jar and 3 others
```

---

### `Failed to find data source: delta`

**Cause:** Delta JARs missing or `DeltaSparkSessionExtension` not configured.

```bash
# Check Delta JARs exist
docker compose exec spark-master ls /opt/spark/jars/ | grep delta
# Should show: delta-spark_2.12-3.1.0.jar  delta-storage-3.1.0.jar
```

If missing → rebuild images.

---

### `PATH_NOT_FOUND: /app/delta/bronze/...`

**Cause:** Job 01 (Kafka → Bronze) has not been run yet, or it failed.

```bash
# Check if bronze data exists
docker compose exec spark-master ls /app/delta/bronze/ 2>/dev/null || echo "Bronze not found"

# Re-run job 01 first
docker compose exec spark-master spark-submit /app/jobs/01_stream_kafka_to_bronze.py
```

---

### `java.io.FileNotFoundException: /home/spark/.ivy2/cache/...`

**Cause:** Old image without writable ivy cache directory.

```bash
docker compose build --no-cache spark-master spark-worker
docker compose up -d
```

---

### `NoClassDefFoundError: SupportsNonDeterministicExpression`

**Cause:** Delta version is too new for the Spark version (Delta 3.2+ needs Spark 3.5.2+).  
The Dockerfile is pinned to Delta 3.1.0 for Spark 3.5.1 — if you see this, the image needs rebuilding:

```bash
docker compose build --no-cache spark-master spark-worker
```

---

### `DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG`

**Cause:** A job's `SparkSession` is missing Delta configs.

All jobs must have:
```python
SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
```

---

### MLflow UI not reachable at `localhost:5001`

```bash
# Check if mlflow container is running
docker compose ps chicago_mlflow

# Check logs
docker compose logs chicago_mlflow

# Restart it
docker compose restart chicago_mlflow
```

---

### Bronze has more rows than expected (duplicates from multiple producer runs)

When the producer is run multiple times, Kafka accumulates all messages. Bronze will have them all but Silver deduplicates on `crime_id`, so Silver/Gold/ML Features will still have the correct unique count.

To get a completely clean Bronze, wipe everything and re-run:
```bash
docker compose exec spark-master bash -c \
  "rm -rf /app/delta/bronze /app/delta/silver /app/delta/gold /app/delta/checkpoints"
docker compose exec producer python /app/app/producer.py
docker compose exec spark-master spark-submit /app/jobs/01_stream_kafka_to_bronze.py
```

---

## 17. Service URLs

| Service | URL | Notes |
|---------|-----|-------|
| Spark Master UI | http://localhost:8080 | Job status, workers |
| Spark Worker UI | http://localhost:8081 | Worker metrics |
| MLflow UI | http://localhost:5001 | Experiment runs, metrics |
| Kafka | localhost:29092 | External port for local tools |
| Zookeeper | localhost:2181 | Internal coordination |

---

## Pipeline Architecture Summary

```
data/raw/chicago_crimes_sample.csv   (100,000 rows)
        │
        ▼
[Producer]  → Kafka topic: chicago_crimes_raw  (100,000 messages)
        │
        ▼
[Job 01]  spark-submit 01_stream_kafka_to_bronze.py
        → delta/bronze/chicago_crimes_raw          (~100,000 rows)
        │
        ▼
[Job 02]  spark-submit 02_bronze_to_silver.py
        → delta/silver/chicago_crimes_clean        (~100,000 rows, deduped)
        │
        ▼
[Job 03]  spark-submit 03_silver_to_gold.py
        → delta/gold/chicago_crimes_features       (~100,000 rows + time cols)
        │
        ▼
[Job 04]  spark-submit 04_feature_engineering.py
        → delta/gold/ml_features                   (~98,000 rows, 12 features)
        │
        ▼
[Job 05]  spark-submit 05_train_models_mlflow.py
        → reports/ml_model_metrics.csv
        → reports/confusion_matrix_best_model.csv
        → reports/feature_importance_best_model.csv
        → mlruns/  (MLflow artifacts)
```
