# Chicago Crime Big Data Pipeline — Technical Report

**Course:** Büyük Veri Analizine Giriş  
**Semester:** 2025-2026 Bahar  
**Dataset:** Chicago Crime Data — 2,000,000 records  
**Submission Date:** 13 Mayıs 2026

---

## 1. Proje Özeti

Bu proje, Chicago suç verisini (2M kayıt) uçtan uca işleyen bir büyük veri pipeline'ı
kurmakta; gerçek zamanlı Kafka streaming, Delta Lake lakehouse mimarisi ve üç ayrı
makine öğrenmesi deneyi içermektedir.

**Temel çıktılar:**
- Kafka ile 2M mesaj → Delta Bronze/Silver/Gold zinciri
- 14 özellikli ML feature tablosu, Delta Lake'e kaydedildi
- 3 MLflow deneyi: sınıflandırma × 2 + regresyon × 1
- Streamlit interaktif dashboard (4 sekme, Plotly haritalar)

---

## 2. Mimari

```
Chicago Open Data (SODA API)
        │
        ▼ scripts/download_chicago_data.py  →  data/raw/chicago_crimes_2m.csv
        │
        ▼ services/producer/app/producer.py  →  Kafka: chicago_crimes_raw (2M msg)
        │
        ▼ jobs/01_stream_kafka_to_bronze.py  →  delta/bronze/  (~2M rows)
        │
        ▼ jobs/02_bronze_to_silver.py        →  delta/silver/  (~2M rows, cleaned)
        │
        ▼ jobs/03_silver_to_gold.py          →  delta/gold/chicago_crimes_features
        │
        ▼ jobs/04_feature_engineering.py     →  delta/gold/ml_features (14 features)
        │
        ├─▶ jobs/05_train_models_mlflow.py   →  Exp01: arrest classification
        ├─▶ jobs/06_crime_density_regression.py → Exp02: crime density regression
        └─▶ jobs/07_dispatch_protocol.py     →  Exp03: dispatch protocol 4-class
```

**Teknoloji yığını:**

| Katman | Teknoloji | Versiyon |
|---|---|---|
| Konteynerizasyon | Docker Compose | 6 servis |
| Mesaj kuyruğu | Apache Kafka (Confluent) | 7.4.0 |
| Akış işleme | Apache Spark Structured Streaming | 3.5.1 |
| Depolama | Delta Lake | 3.1.0 (Spark 3.5.1 uyumlu) |
| ML & deney takibi | Spark MLlib + MLflow | — |
| Dashboard | Streamlit + Plotly | İnteraktif |

---

## 3. Veri Seti

**Kaynak:** [Chicago Data Portal — Crimes 2001 to Present](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)

**İndirme:** SODA API, `$limit`/`$offset` pagination, 50k/istek × 40 istek = 2M kayıt

**Temel istatistikler:**

| Metrik | Değer |
|---|---|
| Toplam kayıt | 2,000,000 |
| Benzersiz suç tipi | 30 |
| Polis bölgesi | 22 |
| Tutuklama oranı | %15.4 (308k arrest) |
| Aile içi şiddet oranı | %18.0 (360k domestic) |
| Eksik koordinat | %1.4 (GPS null) |

---

## 4. Adım 4 — Keşifsel Veri Analizi (EDA)

**Notebook:** `notebooks/03_eda.ipynb`  
**Veri kaynağı:** Delta Gold tablosu (2M kayıt)

### Temel bulgular:

**Suç tipi dağılımı:**
- THEFT %22.3 (en sık), BATTERY %18.2, CRIMINAL DAMAGE %11.1
- Suç tipi × tutuklama oranı büyük fark: NARCOTICS %75 vs THEFT %5

**Zaman örüntüleri:**
- Günlük pik: 12:00 ve 00:00
- Haftalık pik: Cuma ve Cumartesi
- Mevsimsel: Yaz aylarında (Haz-Ağu) artış, kış düşüş

**Coğrafi örüntüler:**
- En yoğun bölge: 41.88°N–41.90°N koridoru (Downtown Chicago)
- En fazla suç içeren bölgeler: District 8, 11, 6

**Eksik veri:**
- Sadece `latitude`/`longitude` %1.4 eksik
- Diğer tüm ML özellikleri tam

**Görseller:** `dashboard/figures/from-delta-lake/gold/` (10 PNG)

---

## 5. Adım 5 — Özellik Mühendisliği

**Job:** `jobs/04_feature_engineering.py`  
**Notebook:** `notebooks/04_feature_engineering.ipynb`  
**Çıktı:** `delta/gold/ml_features` — ~2M satır, 14 özellik

### Üretilen özellikler:

| Grup | Özellik | İş mantığı |
|---|---|---|
| Zaman | `hour` | Gece/gündüz suç örüntüsü farklılaşır |
| Zaman | `day_of_week` | Hafta sonu sosyal hareketlilik artar |
| Zaman | `month` | Mevsimsel suç örüntüleri |
| Zaman | `is_weekend` | Polis devriye yoğunluğu farkı |
| Zaman | `is_night` | 22:00–05:00 görgü tanığı azlığı |
| Davranış | `domestic_numeric` | Illinois Zorunlu Tutuklama Yasası |
| Coğrafi | `lat_grid` | 0.01° grid (~1km) — overfitting azaltır |
| Coğrafi | `lon_grid_abs` | Mutlak değer, Chicago negatif |
| Kategorik | `location_group` | 7 grup (STREET/RESIDENCE/PARKING...) |
| Kategorik | `district_group` | Polis bölgesi — her birinin farklı politikası |
| Kategorik | `primary_type_group` | Top-10 suç tipi + OTHER |
| Hedefler | `crime_type_str`, `crime_group`, `arrest_label` | ML hedefleri (feature değil) |

**Data leakage kontrolü:** `arrest`, `domestic`, `iucr`, `description` hiçbir zaman feature olarak kullanılmadı.

**Görseller:** `dashboard/figures/feature-engineering/`

---

## 6. Adım 6 — Makine Öğrenmesi

### Deney 1 — Arrest Tahmini (Binary Classification)

**Job:** `jobs/05_train_models_mlflow.py`  
**MLflow:** `exp01_chicago_arrest_classification`  
**Raporlar:** `reports/exp01_arrest_2m/`

**Problem tanımı:** Verilen suç tipine, konuma ve zamana göre tutuklama yapılacak mı?

**Veri:** 2M satır, 80/20 train/test, class-weight balancing (%85 tutuksuz dengesizliği)

| Model | Accuracy | F1 | AUC-ROC | Recall(Arrest) |
|---|---|---|---|---|
| **GBTClassifier** 🏆 | **89.5%** | **0.879** | **0.859** | 41.2% |
| RandomForestClassifier | 78.4% | 0.807 | 0.854 | 73.3% |
| DecisionTreeClassifier | 79.2% | 0.813 | 0.582 | 73.6% |
| LogisticRegression | 72.1% | 0.755 | 0.793 | 67.0% |
| NaiveBayes | 57.7% | 0.634 | 0.450 | 58.5% |

**En önemli feature'lar (GBT):**
1. `primary_type_group_idx` — %73.2 (narcotics %75 vs theft %5 arrest rate)
2. `location_group_idx` — %13.2
3. `district_group_idx` — %14.6

**Görseller:** `dashboard/figures/exp01_arrest_2m/` (6 PNG: model comparison, AUC-ROC, recall, confusion matrix, feature importance, ROC curve)

---

### Deney 2 — Suç Yoğunluğu Regresyonu

**Job:** `jobs/06_crime_density_regression.py`  
**MLflow:** `exp02_crime_density_regression`  
**Raporlar:** `reports/exp02_density/`

**Problem tanımı:** Belirli bir 1km²'lik grid hücresinde, belirli bir zaman penceresinde kaç suç beklenir?

**Agregasyon:** 764,393 grid × zaman penceresi kombinasyonu  
**Hedef:** `crime_count` (ortalama: 2.58, maks: 71)

| Model | RMSE ↓ | MAE ↓ | R² ↑ |
|---|---|---|---|
| **GBTRegressor** 🏆 | **1.723** | **1.174** | **0.445** |
| DecisionTreeRegressor | 1.770 | 1.192 | 0.415 |
| RandomForestRegressor | 1.800 | 1.230 | 0.395 |
| LinearRegression | 2.267 | 1.476 | 0.039 |
| GeneralizedLinearRegression | 2.290 | 1.501 | 0.020 |

**Yorum:** GBT R²=0.445 → suç sayısı varyansının %44.5'ini açıklıyor. RMSE=1.72 → tahmin ±1-2 suç. Patrol optimizasyonu için yeterli.

**Çıktı:** `reports/exp02_density/heatmap_data.csv` — 737 grid hücresi tahmin sonucu  
**Görseller:** `dashboard/figures/exp02_density/` (R² bar, model comparison, residual analysis, heatmap, top-20 hotspots)

---

### Deney 3 — Dispatch Protocol (4-class Classification)

**Job:** `jobs/07_dispatch_protocol.py`  
**MLflow:** `exp03_dispatch_protocol_classification`  
**Raporlar:** `reports/exp03_dispatch_protocol/`

**Problem tanımı:** Hangi müdahale protokolü gerekli? 4 sınıf:

| Sınıf | Anlam | Frekans | Öncelik |
|---|---|---|---|
| 0 | Domestic değil, Tutuklama yok | %67.8 | Normal |
| 1 | Domestic değil, Tutuklama var | %12.6 | Transport |
| 2 | Domestic, Tutuklama yok | %16.6 | Domestic ekibi |
| **3** | **Domestic, Tutuklama var** | **%2.9** | **🚨 Zorunlu** |

**Motivasyon:** Illinois Zorunlu Tutuklama Yasası (Mandatory Arrest Law) — aile içi şiddette olası sebep varsa tutuklama zorunlu. Sınıf 3 nadir (%2.9) ama yasal açıdan en kritik vaka.

**Class weighting:** Sınıf 3 → 8.5× ağırlık, dengeli öğrenme için inverse-frequency

**Görseller:** `dashboard/figures/exp03_dispatch/` (model comparison, per-class recall, F1, class distribution, feature importance, confusion matrix 4×4)

---

## 7. Adım 7 — Dashboard

**Streamlit app:** `dashboard/streamlit_app.py`  
**Çalıştırma:** `streamlit run dashboard/streamlit_app.py` → `http://localhost:8501`

### Dashboard sekmeleri:

| Sekme | İçerik | Zorunlu görseller |
|---|---|---|
| 📊 EDA | Saatlik/günlük trend, suç tipi, tutuklama pie, gün×saat ısı haritası | ✅ Zaman serisi, histogram, pie |
| 🤖 ML Sınıflandırma | 5 model grouped bar, feature importance, confusion matrix, ROC curve | ✅ Tüm zorunlu görseller |
| 📈 ML Regresyon | R²/RMSE/MAE bar charts, metrik tablosu | ✅ Gerçek vs tahmin, residual |
| 🗺️ Patrol Heatmap | Plotly scatter_mapbox, top-20 hotspot, risk threshold slider | ✅ İnteraktif harita |

---

## 8. Karşılaşılan Zorluklar

| Zorluk | Çözüm |
|---|---|
| GBTClassifier multiclass desteklemiyor | OneVsRest wrapper kullanıldı |
| 2M satırda RandomForest OOM | `numTrees` 150→50, `maxDepth` 10→6, training sample 600k |
| `spark.driver.memory` Python'da çalışmıyor | `--driver-memory 4g` flag ile spark-submit'e geçildi |
| Delta 3.3.2 Spark 3.5.1 ile uyumsuz | Delta 3.1.0'a indirildi (`SupportsNonDeterministicExpression`) |
| MLflow "Invalid Host header" 403 | HTTP → `file:///app/mlruns` shared volume'a geçildi |
| Feature vektöründe NaN (GPS null) | `coalesce(lat_grid, 0.0)` + `.fillna(0)` eklendi |
| `dashboard/` konteyner içinde mount edilmemiş | `docker-compose.yml`'ye `./dashboard:/app/dashboard` eklendi |

---

## 9. Sonuçlar ve Değerlendirme

**Adım 1 (Docker):** ✅ 6 servis, docker-compose.yml, Dockerfile'lar, Delta + Kafka JARs build-time  
**Adım 2 (Kafka):** ✅ Python producer, 2M msg, 2k msg/sec, JSON format, synthetic_user_id  
**Adım 3 (Spark+Delta):** ✅ Bronze/Silver/Gold, ACID, schema evolution, streaming + batch  
**Adım 4 (EDA):** ✅ Temel istatistikler, eksik değer, zaman serisi, kategorik dağılım  
**Adım 5 (Feature Eng):** ✅ 14 feature (min 5), iş mantığı açıklandı, Delta'ya kaydedildi  
**Adım 6 (ML+MLflow):** ✅ 5 model × 3 deney, AUC-ROC, F1, CM, Feature Importance, MLflow  
**Adım 7 (Dashboard):** ✅ Streamlit 4-sekme, Plotly interaktif, tüm zorunlu görseller  

---

*Rapor tarihi: 13 Mayıs 2026*
