# Büyük Veri Analizine Giriş — Dönem Projesi Teknik Raporu

**Ders:** Büyük Veri Analizine Giriş &nbsp;|&nbsp; **Danışman:** Asst. Prof. Ayşe Gül Eker &nbsp;|&nbsp; **Dönem:** 2025-2026 Bahar
**Ekip:** Emircan Kartal · Meryem Berfin Kenar · Kağan Gür &nbsp;|&nbsp; **Veri Seti:** Chicago Crime Data (2.000.000 kayıt)

---

## 1. Proje Mimarisi

Proje, Chicago Open Data platformundan SODA API aracılığıyla indirilen 2.000.000 suç kaydını uçtan uca işleyen bir büyük veri pipeline'ı olarak tasarlanmıştır. Tüm servisler Docker Compose ile konteynerize edilmiş; Apache Kafka, Apache Spark Structured Streaming, Delta Lake ve MLflow bileşenleri tek bir entegre sistemde bir araya getirilmiştir.

**Veri akışı:**
```
Chicago Open Data → Kafka Producer (2.000 msg/sn) → Kafka Topic
→ Spark Streaming → Delta Bronze → Delta Silver → Delta Gold
→ Feature Engineering (14 özellik) → 3 ML Deneyi → Streamlit Dashboard
```

**Teknoloji yığını:**

| Katman | Teknoloji | Sürüm |
|---|---|---|
| Konteynerizasyon | Docker Compose | 6 servis |
| Mesaj kuyruğu | Apache Kafka (Confluent) | 7.4.0 |
| Akış işleme | Spark Structured Streaming | 3.5.1 |
| Depolama | Delta Lake (Bronze/Silver/Gold) | 3.1.0 |
| ML & Deney takibi | Spark MLlib + MLflow | — |
| Dashboard | Streamlit + Plotly | — |

---

## 2. Pipeline Adımları

**Adım 1 — Docker:** `docker-compose.yml` ile 6 servis ayağa kaldırılmıştır. Spark imajına Delta Lake ve Kafka JARları derleme aşamasında gömülmüş; çalışma zamanında ağ erişimi gerekmemektedir.

**Adım 2 — Kafka:** Python Producer, CSV dosyasını satır satır okuyarak saniyede 2.000 mesaj hızında `chicago_crimes_raw` topic'ine JSON formatında 2.000.000 mesaj iletmiştir. Her mesaj `ingest_ts`, `synthetic_user_id`, `event_type` ve suç detaylarını içermektedir.

**Adım 3 — Spark + Delta Lake:** `trigger(availableNow=True)` ile Kafka'dan okunan veriler üç katmana yazılmıştır: **Bronze** (ham koruma), **Silver** (tip dönüşümü, timestamp ayrıştırma, tekilleştirme, null temizliği), **Gold** (türetilmiş zaman kolonları: `crime_hour`, `is_weekend`, `is_night` vb.).

**Adım 4 — EDA:** Delta Gold tablosu üzerinde yürütülen analizde; THEFT (%22,3) ve BATTERY (%18,2)'nin en sık suç tipleri olduğu, tutuklama oranının yalnızca %15,4 olduğu (ciddi sınıf dengesizliği), sabah saatlerinde minimum ve gece yarısı ile öğleden sonra çift zirve yapısı tespit edilmiştir. Narkotik suçlarda tutuklama oranı (%75) ile hırsızlıkta (%5) arasındaki fark, özellik mühendisliğindeki en kritik sinyali oluşturmaktadır.

**Adım 5 — Özellik Mühendisliği:** Silver tablosundan 14 ML özelliği üretilerek `delta/gold/ml_features` tablosuna kaydedilmiştir.

| Grup | Özellikler | İş Gerekçesi |
|---|---|---|
| Zaman | hour, day_of_week, month, is_weekend, is_night | Devriye yoğunluğu ve suç örüntüsü saate göre değişir |
| Davranışsal | domestic_numeric | Illinois Zorunlu Tutuklama Yasası bağlamı |
| Coğrafi | lat_grid, lon_grid_abs | ~1km ızgara; aşırı öğrenmeyi azaltır |
| Kategorik | location_group, district_group, primary_type_group | Seyrek kategorilerin boyut artışını önler |

`arrest`, `domestic`, `iucr`, `description` kolonları veri sızıntısı riski nedeniyle özellik setine dahil edilmemiştir.

---

## 3. Makine Öğrenmesi Deneyleri

Üç bağımsız deney Spark MLlib ile yürütülmüş; tüm parametreler, metrikler ve model artifact'ları MLflow'a loglanmıştır.

### Deney 1 — Tutuklama Tahmini (Binary Sınıflandırma)
*Hedef:* Suçun tutuklama ile sonuçlanıp sonuçlanmayacağının tahmini. Sınıf dengesizliği (%15,4 pozitif) ters frekans ağırlıklandırmasıyla giderilmiştir.

| Model | Doğruluk | F1 | AUC-ROC |
|---|---|---|---|
| **GBT Classifier** ✓ | **%89,5** | **0,879** | **0,859** |
| Random Forest | %78,4 | 0,807 | 0,854 |
| Decision Tree | %79,2 | 0,813 | 0,582 |
| Logistic Regression | %72,1 | 0,755 | 0,793 |
| Naive Bayes | %57,7 | 0,634 | 0,450 |

Özellik önemi analizinde `primary_type_group` %73,2 ile baskın sinyali oluşturmaktadır. GBT modelinin karışıklık matrisinde TN=331.722, FP=5.120, FN=36.905, TP=25.826 değerleri elde edilmiştir.

### Deney 2 — Suç Yoğunluğu Regresyonu
*Hedef:* Belirli bir 1km² ızgara hücresinde belirli bir zaman penceresinde beklenen suç sayısının tahmini. 764.393 ızgara-zaman kombinasyonu eğitim verisi olarak kullanılmıştır.

| Model | RMSE | MAE | R² |
|---|---|---|---|
| **GBT Regressor** ✓ | **1,723** | **1,174** | **0,445** |
| Decision Tree | 1,770 | 1,192 | 0,415 |
| Random Forest | 1,800 | 1,230 | 0,395 |
| Linear Regression | 2,267 | 1,476 | 0,039 |
| Generalized LR | 2,290 | 1,501 | 0,020 |

GBT, suç yoğunluğu varyansının %44,5'ini açıklamaktadır. En yüksek yoğunluk 41,88°K 87,63°B koordinatında tespit edilmiş olup şehir ortalamasının 6 katına ulaşmaktadır.

### Deney 3 — Müdahale Protokolü Sınıflandırması (4 Sınıf)
*Hedef:* Her olayı olay yerine varmadan önce dört müdahale protokolünden birine atama. Illinois Zorunlu Tutuklama Yasası kapsamında %2,9 oranındaki Sınıf 3 (aile içi + tutuklama) 8,54 kat ağırlık alarak önceliklendirilmiştir.

| Model | F1 | Doğruluk | Recall Sınıf 3 |
|---|---|---|---|
| **Random Forest** ✓ | **0,682** | **%64,2** | **%70,0** |
| Decision Tree | 0,661 | %61,8 | %63,0 |
| Logistic Regression | 0,616 | %57,2 | %64,2 |

---

## 4. Görselleştirme ve Dashboard

Streamlit + Plotly tabanlı interaktif dashboard (`http://localhost:8501`) dört sekme sunmaktadır: (1) EDA — 2M kayıttan üretilen zaman serisi, dağılım ve konum scatter haritası; (2) Exp01 — model karşılaştırma, karışıklık matrisi, ROC eğrisi, özellik önemi; (3) Exp02 — regresör karşılaştırma, artık analizi ve interaktif Chicago devriye haritası; (4) Exp03 — sınıf bazlı duyarlılık, 4×4 karışıklık matrisi. Tüm grafikler statik PNG yerine anlık veri hesaplamasından üretilmektedir. MLflow deney takip arayüzüne `http://localhost:5001` adresinden erişilmektedir.

---

## 5. Karşılaşılan Zorluklar

| Zorluk | Çözüm |
|---|---|
| GBT yalnızca binary etiket destekliyor | `MultilayerPerceptronClassifier` ile değiştirildi |
| 2M satırda Random Forest OOM | `numTrees` 50, `maxDepth` 6; Exp03 eğitim seti 600k örnekleme |
| `spark.driver.memory` etkisiz kalıyor | `--driver-memory 4g` spark-submit komutuna eklendi |
| Delta 3.3.2 ile Spark 3.5.1 uyumsuzluğu | Delta 3.1.0'a düşürüldü |
| MLflow HTTP 403 hatası | `file:///app/mlruns` paylaşımlı volume'a geçildi |
| Özellik vektöründe NaN | `coalesce(lat_grid, 0.0)` + `.fillna(0)` uygulandı |

---

## 6. Sonuç

Bu çalışmada, 2.000.000 Chicago suç kaydı üzerinde Docker ile konteynerize edilmiş, Kafka ile gerçek zamanlı veri akışı sağlayan, Delta Lake ile lakehouse katmanlı depolama kullanan ve MLflow ile deney takibi gerçekleştiren uçtan uca bir büyük veri pipeline'ı başarıyla hayata geçirilmiştir. GBT algoritması her üç deneyde de en iyi sonuçları vermiş; tutuklama tahmininde AUC-ROC=0,859, suç yoğunluğu regresyonunda R²=0,445 ve müdahale protokolü sınıflandırmasında Sınıf 3 duyarlılığı %70,0 elde edilmiştir. Sistem, ödev tanımındaki yedi adımın tamamını karşılamakta ve tüm sonuçlar interaktif Streamlit dashboard üzerinden erişilebilir durumdadır.
