# Chicago Crime Pipeline — Complete Runbook

**One document. Every command. From zero to presentation.**

---

## Table of Contents

1. [Prerequisites & First-Time Setup](#1-prerequisites--first-time-setup)
2. [Download 2M Rows from Chicago Open Data](#2-download-2m-rows-from-chicago-open-data)
3. [Build Docker Images](#3-build-docker-images)
4. [Start All Services](#4-start-all-services)
5. [Stream Data through Kafka → Bronze → Silver → Gold](#5-stream-data-through-kafka--bronze--silver--gold)
6. [Feature Engineering → Delta ml_features](#6-feature-engineering--delta-ml_features)
7. [ML Experiments](#7-ml-experiments)
8. [Open MLflow UI](#8-open-mlflow-ui)
9. [Open Streamlit Dashboard](#9-open-streamlit-dashboard)
10. [Generate All Dashboard Figures](#10-generate-all-dashboard-figures)
11. [Verify Everything is Working](#11-verify-everything-is-working)
12. [Presentation Day — Quick Start](#12-presentation-day--quick-start)
13. [Troubleshooting](#13-troubleshooting)
14. [Service URLs Reference](#14-service-urls-reference)

---

## 1. Prerequisites & First-Time Setup

### Requirements
| Tool | Version | Check |
|---|---|---|
| Docker Desktop | ≥ 4.x | `docker --version` |
| Python | 3.9+ | `python3 --version` |

### Clone and setup virtual environment
```bash
git clone <repo-url>
cd chicago-crime-bigdata-pipeline

# Create Python venv (for local notebooks + Streamlit dashboard)
python3 -m venv .venv
source .venv/bin/activate

pip install \
  pyspark==4.0.0 \
  "delta-spark==4.0.1" \
  jupyterlab \
  pandas numpy matplotlib seaborn \
  streamlit plotly folium contextily \
  requests
```

---

## 2. Download 2M Rows from Chicago Open Data

Downloads from the free Chicago Open Data SODA API. No account needed.

```bash
source .venv/bin/activate

python3 scripts/download_chicago_data.py \
  --limit 2000000 \
  --output data/raw/chicago_crimes_2m.csv \
  --batch-size 50000
```

**Duration:** ~5–10 min (40 batches × 50k rows)

**Verify:**
```bash
wc -l data/raw/chicago_crimes_2m.csv
# Expected: 2,000,001 (header + 2M rows)
```

---

## 3. Build Docker Images

> Run this once, and again only if `services/spark/Dockerfile` changes.
> This bakes Delta Lake + Kafka JARs into the image so no runtime downloads are needed.

```bash
docker compose build --no-cache spark-master spark-worker
```

**Duration:** ~5 min (downloads 6 JARs during build)

**What's baked in:**
- Delta Lake 3.1.0 JARs (compatible with Spark 3.5.1)
- Spark-Kafka connector JARs (spark-sql-kafka-0-10, kafka-clients, etc.)
- Python packages: pandas, numpy, mlflow, delta-spark, scikit-learn

---

## 4. Start All Services

```bash
docker compose up -d
```

**Verify all 6 containers are running:**
```bash
docker compose ps
```

Expected output:
```
chicago_zookeeper      running
chicago_kafka          running
chicago_spark_master   running
chicago_spark_worker   running
chicago_producer       running
chicago_mlflow         running
```

**If any container is not running:**
```bash
docker compose logs <container-name>
# e.g.: docker compose logs chicago_kafka
```

---

## 5. Stream Data through Kafka → Bronze → Silver → Gold

Run these **in order**. Each step depends on the previous.

### Step 5a — Producer: Stream 2M rows into Kafka
```bash
docker compose exec producer python /app/app/producer.py
```
- Sends 100,000–2,000,000 rows at 2,000 msg/sec
- **Duration:** ~17 min for 2M rows
- Expected final line: `[SUCCESS] Producer finished. Total messages sent: 2000000`

### Step 5b — Kafka → Bronze Delta
```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/01_stream_kafka_to_bronze.py
```
- Reads all Kafka offsets from earliest, writes to `delta/bronze/`
- Expected: `[SUCCESS] Bronze Delta written to: /app/delta/bronze/chicago_crimes_raw`

### Step 5c — Bronze → Silver Delta (clean + dedup)
```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/02_bronze_to_silver.py
```
- Type-casts, parses timestamps, deduplicates on `crime_id`
- Expected: `[SUCCESS] Silver Delta written to: /app/delta/silver/chicago_crimes_clean`

### Step 5d — Silver → Gold Delta (time feature engineering)
```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/03_silver_to_gold.py
```
- Adds `crime_hour`, `crime_day_of_week`, `crime_month`, `is_weekend`, `is_night`
- Expected: `[SUCCESS] Gold Delta written to: /app/delta/gold/chicago_crimes_features`

---

## 6. Feature Engineering → Delta ml_features

Builds the ML-ready feature table from Silver Delta.

```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/04_feature_engineering.py
```

**Output:** `delta/gold/ml_features`

**Features produced (14):**
| Group | Features |
|---|---|
| Time | hour, day_of_week, month, is_weekend, is_night |
| Behavioural | domestic_numeric |
| Geographic | lat_grid, lon_grid_abs |
| Categorical | location_group, district_group, primary_type_group, crime_group |
| Targets stored | crime_type_str, crime_group, district_str, arrest_label |

---

## 7. ML Experiments

### Experiment 1 — Arrest Prediction (Binary Classification)
```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/05_train_models_mlflow.py
```
- **MLflow experiment:** `exp01_chicago_arrest_classification`
- **Reports saved to:** `reports/exp01_arrest_2m/`
- **Best result:** GBTClassifier AUC-ROC=0.859, Accuracy=89.5%
- **Duration:** ~15–20 min (5 models × 2M rows)

### Experiment 2 — Crime Density Regression
```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/06_crime_density_regression.py
```
- **MLflow experiment:** `exp02_crime_density_regression`
- **Reports saved to:** `reports/exp02_density/`
- **Best result:** GBTRegressor R²=0.445, RMSE=1.72
- **Output:** `heatmap_data.csv` → 737 grid cells with predicted crime density
- **Duration:** ~10 min

### Experiment 3 — Dispatch Protocol (4-class Classification)
```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/07_dispatch_protocol.py
```
- **MLflow experiment:** `exp03_dispatch_protocol_classification`
- **Reports saved to:** `reports/exp03_dispatch_protocol/`
- **Classes:** NonDom+NoArrest / NonDom+Arrest / Dom+NoArrest / Dom+Arrest
- **Best result:** DecisionTree F1=0.671, recall_class3 (mandatory arrest)=0.612
- **Duration:** ~20 min (4-class with class balancing)

---

## 8. Open MLflow UI

MLflow tracks all experiments, parameters, metrics and model artifacts.

```bash
# MLflow starts automatically with docker compose up -d
# Open in browser:
open http://localhost:5001
```

Or manually navigate to: **`http://localhost:5001`**

**What you'll see:**

| Experiment | Models | Best Metric |
|---|---|---|
| `exp01_chicago_arrest_classification` | LR, DT, RF, GBT, NaiveBayes | GBT AUC-ROC=0.859 |
| `exp02_crime_density_regression` | LR, DT, RF, GBT, GLR | GBT R²=0.445 |
| `exp03_dispatch_protocol_classification` | LR, DT, RF, GBT, NaiveBayes | DT F1=0.671 |

**MLflow navigation tips:**
- Click experiment name → see all runs
- Click a run → see parameters, metrics, artifacts
- Select multiple runs → click **Compare** → side-by-side metric charts
- Filter runs: type `metrics.auc_roc > 0.8` in the search bar

**Take a screenshot for your report:**
- `Cmd+Shift+4` (Mac) → select the MLflow window → saves to Desktop

---

## 9. Open Streamlit Dashboard

The interactive dashboard with all required visualizations.

```bash
# Activate virtual environment first
source .venv/bin/activate

# Run from project root
streamlit run dashboard/streamlit_app.py
```

Opens automatically at: **`http://localhost:8501`**

**If port is already in use:**
```bash
streamlit run dashboard/streamlit_app.py --server.port 8502
```

**Dashboard tabs:**
| Tab | Content |
|---|---|
| 📊 EDA | Hourly/daily trends, crime types, arrest rates, heatmap, pie charts |
| 🤖 ML — Sınıflandırma | 5-model grouped bar, feature importance, confusion matrix, ROC curve |
| 📈 ML — Regresyon | 5-regressor R²/RMSE/MAE comparison, interpretation |
| 🗺️ Patrol Heatmap | Interactive Plotly scatter_mapbox, top-20 hotspots, risk filter slider |

**Stop Streamlit:** `Ctrl+C` in terminal

---

## 10. Generate All Dashboard Figures

Figures are saved to `dashboard/figures/` with versioned subdirectories.
Old figures are **never overwritten** — each experiment has its own folder.

```bash
source .venv/bin/activate
cd notebooks
jupyter notebook 06_dashboard_figures.ipynb
```

In Jupyter: **Kernel → Restart Kernel → Run All Cells**

**Figure output directories:**
```
dashboard/figures/
  dashboard/          ← fig1–fig10 (ML comparison, EDA, ROC, confusion matrix)
  exp01_arrest_2m/    ← arrest experiment figures
  exp02_density/      ← crime density heatmap + top-20 hotspots
  exp03_dispatch/     ← dispatch protocol figures (generated by notebook)
```

**To generate figures for exp03 separately:**
```bash
source .venv/bin/activate
python3 - << 'EOF'
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

OUT = Path("dashboard/figures/exp03_dispatch")
OUT.mkdir(parents=True, exist_ok=True)
REP = Path("reports/exp03_dispatch_protocol")

if (REP / "ml_model_metrics.csv").exists():
    df = pd.read_csv(REP / "ml_model_metrics.csv")
    # Per-class recall chart
    classes = ["recall_class0","recall_class1","recall_class2","recall_class3"]
    labels  = ["NonDom\nNoArrest","NonDom\nArrest","Dom\nNoArrest","Dom\nArrest (Critical)"]
    colors  = ["#3498db","#f39c12","#9b59b6","#e74c3c"]
    fig, axes = plt.subplots(1, len(df), figsize=(14, 5))
    for ax, (_, row) in zip(axes, df.iterrows()):
        vals = [row.get(c, 0) for c in classes]
        ax.bar(labels, vals, color=colors, alpha=0.85)
        ax.set_title(row["model"].replace("Classifier",""), fontsize=9, fontweight="bold")
        ax.set_ylim(0, 1.05)
        ax.set_ylabel("Recall")
        for i, v in enumerate(vals):
            ax.text(i, v+0.02, f"{v:.2f}", ha="center", fontsize=8)
    plt.suptitle("Exp03: Dispatch Protocol — Per-Class Recall\n(Class 3 = Mandatory Arrest)", 
                 fontsize=12, fontweight="bold")
    plt.tight_layout()
    plt.savefig(str(OUT / "per_class_recall.png"), bbox_inches="tight", dpi=130)
    plt.show()
    print(f"Saved: {OUT}/per_class_recall.png")
else:
    print("Run job 07 first to generate reports.")
EOF
```

---

## 11. Verify Everything is Working

### Check Delta table row counts
```bash
docker compose exec spark-master python3 - << 'EOF'
from pyspark.sql import SparkSession
spark = (
    SparkSession.builder.appName("verify")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
tables = {
    "Bronze":      "/app/delta/bronze/chicago_crimes_raw",
    "Silver":      "/app/delta/silver/chicago_crimes_clean",
    "Gold":        "/app/delta/gold/chicago_crimes_features",
    "ML Features": "/app/delta/gold/ml_features",
}
for name, path in tables.items():
    try:
        n = spark.read.format("delta").load(path).count()
        print(f"  {name:15}: {n:,} rows  ✓")
    except Exception as e:
        print(f"  {name:15}: NOT FOUND — {e}")
spark.stop()
EOF
```

**Expected:**
```
  Bronze         : ~2,000,000 rows  ✓
  Silver         : ~2,000,000 rows  ✓
  Gold           : ~2,000,000 rows  ✓
  ML Features    : ~1,999,997 rows  ✓
```

### Check MLflow experiments exist
```bash
ls mlruns/
# Should show 4 experiment ID directories
for d in mlruns/*/; do
  [ -f "$d/meta.yaml" ] && grep "^name:" "$d/meta.yaml"
done
```

### Check reports are saved
```bash
echo "=== Experiment Reports ===" && \
ls reports/exp01_arrest_2m/ && echo "---" && \
ls reports/exp02_density/ && echo "---" && \
ls reports/exp03_dispatch_protocol/ 2>/dev/null || echo "exp03: run job 07 first"
```

### Check Streamlit dependencies
```bash
source .venv/bin/activate
python3 -c "import streamlit, plotly, folium, contextily; print('All dashboard deps OK ✓')"
```

---

## 12. Presentation Day — Quick Start

If Docker was stopped, run these commands in order:

```bash
# 1. Start all services (30 sec)
docker compose up -d

# 2. Wait ~10 sec for Kafka to be ready, then verify
docker compose ps

# 3. Open MLflow UI in browser
open http://localhost:5001

# 4. Open Streamlit dashboard
source .venv/bin/activate
streamlit run dashboard/streamlit_app.py
# → opens http://localhost:8501
```

**If you need to rebuild Delta tables from scratch** (e.g., after `docker compose down -v`):
```bash
# Wipe old data
docker compose exec spark-master bash -c \
  "rm -rf /app/delta/bronze /app/delta/silver /app/delta/gold /app/delta/checkpoints"

# Re-run pipeline
docker compose exec producer python /app/app/producer.py
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/01_stream_kafka_to_bronze.py
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/02_bronze_to_silver.py
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/03_silver_to_gold.py
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/04_feature_engineering.py
```

---

## 13. Troubleshooting

### `Failed to find data source: kafka`
Kafka JARs not in image. Rebuild:
```bash
docker compose down
docker compose build --no-cache spark-master spark-worker
docker compose up -d
```
Verify: `docker compose exec spark-master ls /opt/spark/jars/ | grep kafka`

### `Failed to find data source: delta`
Delta JARs missing or session not configured:
```bash
docker compose exec spark-master ls /opt/spark/jars/ | grep delta
# Should show: delta-spark_2.12-3.1.0.jar  delta-storage-3.1.0.jar
```
If missing → rebuild images.

### `PATH_NOT_FOUND: /app/delta/bronze/...`
Bronze hasn't been written yet. Run job 01 first.

### `OutOfMemoryError: Java heap space`
Always use `--driver-memory 4g` with spark-submit:
```bash
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/XX.py
```

### `PermissionError: /app/reports` or `/app/dashboard`
Directory not mounted. Check docker-compose.yml volumes include:
```yaml
- ./reports:/app/reports
- ./dashboard:/app/dashboard
- ./mlruns:/app/mlruns
```
Then: `docker compose down && docker compose up -d`

### `DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG`
SparkSession missing Delta config. All jobs already have this — if you see it, the job file wasn't saved correctly.

### MLflow shows no experiments at `localhost:5001`
MLflow container needs `/app/mlruns` mount. Check docker-compose:
```yaml
mlflow:
  volumes:
    - ./mlruns:/app/mlruns
  command: mlflow ui --host 0.0.0.0 --port 5000 --backend-store-uri /app/mlruns
```
Then: `docker compose restart mlflow`

### Streamlit not found
```bash
source .venv/bin/activate
pip install streamlit plotly folium contextily
```

### `NoClassDefFoundError: SupportsNonDeterministicExpression`
Wrong Delta version for Spark version. The Dockerfile pins Delta 3.1.0 for Spark 3.5.1:
```bash
docker compose build --no-cache spark-master spark-worker
```

---

## 14. Service URLs Reference

| Service | URL | Username | Notes |
|---|---|---|---|
| **MLflow UI** | http://localhost:5001 | — | Experiment tracking |
| **Streamlit Dashboard** | http://localhost:8501 | — | Interactive dashboard |
| **Spark Master UI** | http://localhost:8080 | — | Job status, workers |
| **Spark Worker UI** | http://localhost:8081 | — | Worker metrics |
| Kafka | localhost:29092 | — | External port |

---

## Architecture Summary

```
data/raw/chicago_crimes_2m.csv   ← 2,000,000 rows downloaded from Chicago Open Data
        │
        ▼
[Producer]  →  Kafka topic: chicago_crimes_raw  (2M messages at 2000 msg/sec)
        │
        ▼
[Job 01]  01_stream_kafka_to_bronze.py
        →  delta/bronze/chicago_crimes_raw          (~2M rows)
        │
        ▼
[Job 02]  02_bronze_to_silver.py
        →  delta/silver/chicago_crimes_clean        (~2M rows, deduped + typed)
        │
        ▼
[Job 03]  03_silver_to_gold.py
        →  delta/gold/chicago_crimes_features       (~2M rows + time features)
        │
        ▼
[Job 04]  04_feature_engineering.py
        →  delta/gold/ml_features                   (~2M rows, 14 ML features)
        │
        ├──▶ [Job 05]  05_train_models_mlflow.py
        │           →  exp01: Arrest Prediction (GBT AUC=0.859)
        │           →  reports/exp01_arrest_2m/
        │
        ├──▶ [Job 06]  06_crime_density_regression.py
        │           →  exp02: Crime Density Regression (GBT R²=0.445)
        │           →  reports/exp02_density/  +  heatmap_data.csv
        │
        └──▶ [Job 07]  07_dispatch_protocol.py
                    →  exp03: Dispatch Protocol 4-class (DT F1=0.671)
                    →  reports/exp03_dispatch_protocol/

All experiments → MLflow (http://localhost:5001)
All figures     → dashboard/figures/{exp01,exp02,exp03,dashboard}/
Dashboard       → streamlit run dashboard/streamlit_app.py  (http://localhost:8501)
```

---

## Experiment Results Summary

| Experiment | Problem | Best Model | Key Metric |
|---|---|---|---|
| **exp01** | Arrest prediction (binary) | GBTClassifier | AUC-ROC = **0.859** |
| **exp02** | Crime density (regression) | GBTRegressor | R² = **0.445** |
| **exp03** | Dispatch protocol (4-class) | DecisionTree | F1 = **0.671**, recall_class3 = **0.612** |

**exp03 class meaning for presentation:**
> *"Class 3 is the critical case — domestic violence where Illinois Mandatory Arrest Law requires the officer to make an arrest. Our model recalls 61.2% of these cases before dispatch, meaning 6 out of 10 mandatory-arrest domestic incidents are correctly flagged for the specialized response team."*
