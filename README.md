# Chicago Crime Big Data Streaming & ML Pipeline

<div align="center">

```mermaid
flowchart LR
    A(["Chicago\nOpen Data"]):::source
    B(["Download\nScript"]):::step
    C(["Kafka\nProducer"]):::kafka
    D(["Kafka\nBroker"]):::kafka
    E(["Spark\nStreaming"]):::spark
    F(["Bronze\nDelta Layer"]):::bronze
    G(["Silver\nDelta Layer"]):::silver
    H(["Gold\nDelta Layer"]):::gold
    I(["ML Models\n+ MLflow"]):::ml
    J(["Streamlit\nDashboard"]):::output

    A --> B --> C --> D --> E --> F --> G --> H --> I --> J

    classDef source fill:#1e3a5f,stroke:#4a9eff,color:#e8f4ff
    classDef step fill:#1a1a2e,stroke:#666,color:#ccc
    classDef kafka fill:#231f20,stroke:#f5a623,color:#f5d78e
    classDef spark fill:#3d1a00,stroke:#e25a1c,color:#ffd4b8
    classDef bronze fill:#3b2200,stroke:#cd7f32,color:#f5d5a0
    classDef silver fill:#1a1a2e,stroke:#a8a9ad,color:#e0e0e0
    classDef gold fill:#2d2000,stroke:#ffd700,color:#fff5b8
    classDef ml fill:#1a0030,stroke:#9b59b6,color:#e8d5ff
    classDef output fill:#002d1a,stroke:#27ae60,color:#d5ffe8
```

![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Confluent_7.4-231F20?style=flat-square&logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5.1-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1.0-00ADD8?style=flat-square)
![MLflow](https://img.shields.io/badge/MLflow-tracked-0194E2?style=flat-square&logo=mlflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)

**Büyük Veri Analizine Giriş — dönem projesi · end-to-end streaming analytics pipeline**

</div>

---

## 1. Project Overview

End-to-end big data pipeline on **2,000,000 Chicago Crime records** covering the full modern data engineering and data science stack — from real-time Kafka streaming through Delta Lake lakehouse layers to three MLflow-tracked ML experiments and an interactive Streamlit dashboard.

| Component | Technology | Details |
|---|---|---|
| Containerization | Docker / Docker Compose | 6 services, fully reproducible |
| Message Streaming | Apache Kafka + Python Producer | 2M records at 2,000 msg/sec |
| Stream Processing | Spark Structured Streaming | Bronze → Silver → Gold Delta layers |
| Storage | Delta Lake (ACID, versioned) | ~2M rows per layer |
| Feature Engineering | Spark MLlib | 14 ML-ready features, no data leakage |
| ML Experiment 1 | Spark MLlib + MLflow | **Arrest prediction** — GBT AUC-ROC = 0.859 |
| ML Experiment 2 | Spark MLlib + MLflow | **Crime density regression** — GBT R² = 0.445 |
| ML Experiment 3 | Spark MLlib + MLflow | **Dispatch protocol 4-class** — DT F1 = 0.671 |
| Dashboard | Streamlit + Plotly | 4-tab interactive dashboard with patrol heatmap |

---

## 2. Architecture

<img width="1536" height="1024" alt="Project Architecture" src="https://github.com/user-attachments/assets/064e8dd6-f6cf-45d3-ae96-658a0e997795" />

---

## 3. Dataset

| Property | Details |
|---|---|
| **Source** | [Chicago Data Portal — Crimes 2001 to Present](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2) |
| **Volume used** | 2,000,000 records (downloaded via SODA API) |
| **Full dataset** | ~7.9M records |
| **API** | Socrata SODA — paginated via `$limit` / `$offset` |

**Key columns:**
```
id · date · primary_type · description · location_description
district · ward · community_area · beat · latitude · longitude
domestic · arrest · fbi_code
```

> **Privacy:** Addresses are block-level only. `synthetic_user_id` in Kafka messages is a deterministic MD5 hash — no real user data.

**Excluded from ML features** (data leakage):
- `iucr`, `description` — directly encode crime type
- `arrest`, `domestic` — used only as labels, never as features

---

## 4. How to Run

### Prerequisites
- Docker Desktop ≥ 4.x with ≥ 8 GB RAM allocated
- Python 3.9+

### Quick start (3 commands)

```bash
# 1. Download 2M rows
python3 scripts/download_chicago_data.py --limit 2000000 --output data/raw/chicago_crimes_2m.csv

# 2. Build images + start services
docker compose build --no-cache spark-master spark-worker && docker compose up -d

# 3. Run the full pipeline
docker compose exec producer python /app/app/producer.py
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/01_stream_kafka_to_bronze.py
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/02_bronze_to_silver.py
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/03_silver_to_gold.py
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/04_feature_engineering.py
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/05_train_models_mlflow.py
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/06_crime_density_regression.py
docker compose exec spark-master spark-submit --driver-memory 4g /app/jobs/07_dispatch_protocol.py
```

### Open the dashboards

```bash
# MLflow — experiment tracking
open http://localhost:5001

# Streamlit — interactive dashboard
source .venv/bin/activate
streamlit run dashboard/streamlit_app.py
# → http://localhost:8501
```

> 📖 **For every command, troubleshooting, service URLs and presentation-day instructions see the complete runbook:**
> **[`docs/RUNBOOK.md`](docs/RUNBOOK.md)**

---

## 5. Delta Lake Pipeline

### Bronze — `delta/bronze/chicago_crimes_raw`
Raw preservation. Every Kafka message stored as-is with `kafka_key`, `kafka_timestamp`, `json_value`, `bronze_loaded_at`.

### Silver — `delta/silver/chicago_crimes_clean`
Cleaned and typed. Type-casts all columns, parses `crime_timestamp` from ISO format, deduplicates on `crime_id`, drops nulls on key fields.

### Gold — `delta/gold/chicago_crimes_features`
Analytics-ready. Adds derived time columns: `crime_hour`, `crime_day_of_week`, `crime_month`, `is_weekend`, `is_night`, `arrest_int`, `domestic_int`.

### ML Features — `delta/gold/ml_features`
14 ML-ready features with no leakage:

| Group | Features |
|---|---|
| Time (5) | `hour`, `day_of_week`, `month`, `is_weekend`, `is_night` |
| Behavioural (1) | `domestic_numeric` |
| Geographic (2) | `lat_grid`, `lon_grid_abs` |
| Categorical (3) | `location_group`, `district_group`, `primary_type_group` |
| Stored targets | `crime_type_str`, `crime_group`, `district_str`, `arrest_label` |

---

## 6. Kafka Producer

`services/producer/app/producer.py` — reads CSV, sends JSON to `chicago_crimes_raw`.

**Message format:**
```json
{
  "ingest_ts": "2026-05-10T01:00:00Z",
  "synthetic_user_id": "user_a3f9d12b",
  "event_type": "BATTERY",
  "primary_type": "BATTERY",
  "crime_id": "14183823",
  "date": "2026-05-01T00:00:00.000",
  "location_description": "STREET",
  "district": "001",
  "community_area": "32",
  "latitude": 41.884,
  "longitude": -87.632,
  "domestic": false,
  "arrest": false
}
```

**Config (docker-compose env):**
```
CSV_PATH=data/raw/chicago_crimes_2m.csv
PRODUCE_RATE_PER_SEC=2000
MAX_MESSAGES=2000000
```

---

## 7. EDA — Key Findings

Notebooks: `notebooks/03_eda.ipynb`

- **Crime volume:** THEFT (22%) and BATTERY (18%) dominate; 30 unique crime types
- **Time patterns:** Crime peaks at noon and midnight; Friday highest daily volume
- **Arrest rate:** Only 15.4% of crimes result in arrest — severe class imbalance
- **Domestic crimes:** 18% of all incidents; carry different legal implications (Illinois Mandatory Arrest)
- **Geography:** 41.88°N–41.90°N corridor (downtown) has highest density; Districts 6, 8, 11 top volume
- **Missing data:** Only lat/lon have 1.4% null — all other features complete

Figures: `dashboard/figures/from-csv/` and `dashboard/figures/from-delta-lake/gold/`

---

## 8. Feature Engineering

`jobs/04_feature_engineering.py` + `notebooks/04_feature_engineering.ipynb`

All features exclude `arrest`, `domestic`, `iucr`, `description` to prevent leakage.
Geographic coordinates are rounded to 0.01° (~1 km grid) to reduce overfitting.
Missing GPS coordinates (1.4%) filled with 0.0 sentinel value.

---

## 9. Machine Learning & MLflow

Three separate experiments tracked in MLflow (`http://localhost:5001`):

### Experiment 1 — Arrest Prediction (Binary Classification)
**Target:** Will this crime result in an arrest?

| Model | Accuracy | F1 | AUC-ROC |
|---|---|---|---|
| **GBTClassifier** 🏆 | **89.5%** | **0.879** | **0.859** |
| RandomForest | 78.4% | 0.807 | 0.854 |
| DecisionTree | 79.2% | 0.813 | 0.582 |
| LogisticRegression | 72.1% | 0.755 | 0.793 |
| NaiveBayes | 57.7% | 0.634 | 0.450 |

Class-weight balancing applied (arrested class = 15% of data).

---

### Experiment 2 — Crime Density Regression
**Target:** How many crimes per 1km² grid cell in a given time window?

| Model | RMSE | MAE | R² |
|---|---|---|---|
| **GBTRegressor** 🏆 | **1.72** | **1.17** | **0.445** |
| DecisionTreeRegressor | 1.77 | 1.19 | 0.415 |
| RandomForestRegressor | 1.80 | 1.23 | 0.395 |
| LinearRegression | 2.27 | 1.48 | 0.039 |
| GeneralizedLinearRegression | 2.29 | 1.50 | 0.020 |

Output: `reports/exp02_density/heatmap_data.csv` — 737 grid cells with predicted crime density → patrol heatmap.

---

### Experiment 3 — Dispatch Protocol (4-class Classification)
**Target:** Which of 4 response protocols is required?

| Class | Meaning | Frequency |
|---|---|---|
| 0 | Non-Domestic, No Arrest — standard report | 67.8% |
| 1 | Non-Domestic, Arrest — send transport unit | 12.6% |
| 2 | Domestic, No Arrest — domestic-trained officers | 16.6% |
| **3** | **Domestic, Arrest — mandatory arrest team** | **2.9%** |

**Real-world motivation:** Illinois Mandatory Arrest Law requires arrest when probable cause exists in domestic incidents. Predicting Class 3 before dispatch ensures the right team arrives first.

| Model | F1 | Recall Class 3 |
|---|---|---|
| **DecisionTree** 🏆 | **0.671** | **0.612** |
| LogisticRegression | 0.617 | 0.630 |

Class weights applied: Class 3 receives **8.5× weight** to improve recall on the rarest, most critical case.

---

## 10. Dashboard

Interactive Streamlit dashboard — `dashboard/streamlit_app.py`

```bash
source .venv/bin/activate
streamlit run dashboard/streamlit_app.py
# → http://localhost:8501
```

| Tab | Content |
|---|---|
| 📊 EDA | Hourly/daily time series, top-10 crime types, arrest rate pie, day×hour heatmap |
| 🤖 ML — Sınıflandırma | 5-model grouped bar, feature importance, confusion matrix, ROC curve |
| 📈 ML — Regresyon | R²/RMSE/MAE comparison for 5 regressors |
| 🗺️ Patrol Heatmap | Interactive Plotly scatter_mapbox, top-20 hotspot bars, risk threshold slider |

Static HTML alternative: `open dashboard/index.html`

---

## 11. Challenges

| Challenge | Solution |
|---|---|
| Spark GBTClassifier only supports binary | Wrapped in `OneVsRest` for multiclass experiments |
| Class imbalance (15% arrests, 2.9% dom+arrest) | Inverse-frequency class weights per experiment |
| `spark.driver.memory` ignored inside Python code | Always pass `--driver-memory 4g` to spark-submit CLI |
| Delta JAR / Ivy cache permission error | Baked JARs into Dockerfile; `mkdir /home/spark/.ivy2` with correct ownership |
| `SupportsNonDeterministicExpression` at runtime | Pinned Delta 3.1.0 (compatible with Spark 3.5.1, not 3.5.2+) |
| MLflow "Invalid Host header" 403 | Switched from HTTP tracking URI to `file:///app/mlruns` with shared volume |
| NaN in feature vectors (1.4% missing GPS) | `coalesce(lat_grid, 0.0)` in feature engineering + `.fillna(0)` in ML job |
| `dashboard/` not mounted in Spark container | Added `- ./dashboard:/app/dashboard` to docker-compose volumes |

---

## 12. Team

<div align="center">
<table width="100%">
<tr>
  <td width="33%" align="center">
    <a href="https://github.com/EmircanKartal">
      <img src="https://github.com/EmircanKartal.png" width="80" style="border-radius:50%"/>
    </a>
  </td>
  <td width="33%" align="center">
    <a href="https://github.com/berfinm">
      <img src="https://github.com/berfinm.png" width="80" style="border-radius:50%"/>
    </a>
  </td>
  <td width="33%" align="center">
    <a href="https://github.com/kagangur">
      <img src="https://github.com/kagangur.png" width="80" style="border-radius:50%"/>
    </a>
  </td>
</tr>
<tr>
  <td width="33%" align="center"><strong><a href="https://github.com/EmircanKartal">Emircan Kartal</a></strong></td>
  <td width="33%" align="center"><strong><a href="https://github.com/berfinm">Meryem Berfin Kenar</a></strong></td>
  <td width="33%" align="center"><strong><a href="https://github.com/kagangur">Kağan Gür</a></strong></td>
</tr>
</table>
</div>

---

<div align="center">

**Büyük Veri Analizine Giriş — 2025-2026 Bahar Dönemi**

Chicago Crime Dataset · [Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)

📖 Full commands & troubleshooting → [`docs/RUNBOOK.md`](docs/RUNBOOK.md)

</div>
