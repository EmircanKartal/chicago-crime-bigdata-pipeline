# Büyük Veri Analizine Giriş — Dönem Projesi
# Teknik Rapor

---

**Ders:** Büyük Veri Analizine Giriş  
**Danışman:** Dr. Ayşe Gül Eker  
**Dönem:** 2025-2026 Bahar  
**Teslim Tarihi:** 13 Mayıs 2026  

**Ekip Üyeleri:**  
- Emircan Kartal  
- Meryem Berfin Kenar  
- Kağan Gür  

**Veri Seti:** Chicago Crime Data (Chicago Open Data — ~7,9M kayıt)  
**Kullanılan Kayıt Sayısı:** 2.000.000  
**GitHub:** chicago-crime-bigdata-pipeline  

---

## Özet

Bu çalışmada, Chicago şehrine ait 2.000.000 suç kaydından oluşan gerçek dünya veri seti üzerinde uçtan uca bir büyük veri pipeline'ı tasarlanmış ve hayata geçirilmiştir. Sistem; Apache Kafka ile gerçek zamanlı veri akışı simülasyonu, Apache Spark Structured Streaming ile akış verisi işleme, Delta Lake ile Bronze/Silver/Gold katmanlı depolama, Spark MLlib ile makine öğrenmesi model eğitimi ve MLflow ile deney takibini entegre biçimde barındırmaktadır. Üç farklı makine öğrenmesi deneyi yürütülmüş; tutuklama tahmini (binary sınıflandırma), suç yoğunluğu regresyonu ve müdahale protokolü sınıflandırması (dört sınıflı) problemleri ele alınmıştır. En başarılı sonuçlar, Gradient Boosted Trees (GBT) algoritmasıyla elde edilmiş olup tutuklama tahmininde AUC-ROC değeri 0,859, suç yoğunluğu regresyonunda ise R² değeri 0,445 olarak gerçekleşmiştir. Proje çıktıları, Streamlit tabanlı interaktif bir dashboard aracılığıyla sunulmaktadır.

**Anahtar Kelimeler:** Büyük Veri, Apache Kafka, Apache Spark, Delta Lake, MLflow, Makine Öğrenmesi, Chicago Suç Analizi

---

## 1. Giriş

Büyük veri sistemleri, günümüz veri mühendisliğinin temel yapı taşlarından birini oluşturmaktadır. Gerçek zamanlı veri akışlarının işlenmesi, depolanması ve analiz edilmesi; endüstride olduğu gibi akademik çevrelerde de giderek artan bir ilgi görmektedir. Bu çalışmada, Chicago şehrinin kamuya açık suç verisi üzerinde modern bir büyük veri pipeline'ı inşa edilmektedir.

Projenin birincil amacı, teorik bilginin pratik bir sistemde bütünleşik olarak uygulanmasını sağlamaktır. Bu çerçevede; veri üretiminden depolamaya, keşifsel analizden makine öğrenmesi model eğitimine kadar uzanan tüm süreç tek bir entegre pipeline içinde ele alınmaktadır.

### 1.1. Çalışmanın Kapsamı

Proje yedi temel adımdan oluşmaktadır:

1. Docker ile konteynerize edilmiş servis ortamının kurulumu
2. Apache Kafka kullanılarak gerçek zamanlı streaming veri üretimi
3. Spark Structured Streaming ile veri temizleme ve Delta Lake'e yazma
4. Delta Lake Gold tablosu üzerinde keşifsel veri analizi (EDA)
5. Makine öğrenmesi için anlamlı özellik mühendisliği
6. Beş farklı modelin eğitimi, karşılaştırılması ve MLflow ile takibi
7. Sonuçların interaktif dashboard aracılığıyla görselleştirilmesi

### 1.2. Veri Seti

Çalışmada kullanılan veri seti, Chicago Open Data platformu üzerinden SODA (Socrata Open Data API) aracılığıyla edinilmiş olup 2001 yılından günümüze kadar kayıt altına alınan suç olaylarını içermektedir. Toplamda 2.000.000 kayıt kullanılmış; her kayıt suçun türü, konumu, tarihi, tutuklamanın gerçekleşip gerçekleşmediği ve aile içi şiddet niteliği taşıyıp taşımadığı gibi bilgileri kapsamaktadır.

---

## 2. Sistem Mimarisi

### 2.1. Genel Mimari

Projenin mimari yapısı aşağıdaki katmanlardan oluşmaktadır. Her katman bağımsız bir sorumluluk üstlenmekte ve bir sonraki katmana temiz, işlenmiş veri sunmaktadır.

```
Chicago Open Data (SODA API)
         │
         ▼  [Veri İndirme]
  data/raw/chicago_crimes_2m.csv  (2.000.000 satır)
         │
         ▼  [Kafka Producer]
  Kafka Topic: chicago_crimes_raw  (2.000.000 mesaj, 2.000 msg/sn)
         │
         ▼  [Spark Structured Streaming]
  Delta Bronze: delta/bronze/chicago_crimes_raw
         │
         ▼  [Batch ETL]
  Delta Silver: delta/silver/chicago_crimes_clean  (temizlenmiş, tekilleştirilmiş)
         │
         ▼  [Zaman Özelliği Ekleme]
  Delta Gold: delta/gold/chicago_crimes_features
         │
         ▼  [Feature Engineering]
  Delta Gold: delta/gold/ml_features  (14 özellik, 3 hedef değişken)
         │
         ├──▶ Exp01: Tutuklama Tahmini         → MLflow exp01
         ├──▶ Exp02: Suç Yoğunluğu Regresyonu → MLflow exp02
         └──▶ Exp03: Müdahale Protokolü        → MLflow exp03
         │
         ▼
  Streamlit Dashboard (http://localhost:8501)
```

### 2.2. Teknoloji Yığını

| Katman | Teknoloji | Sürüm | Açıklama |
|---|---|---|---|
| Konteynerizasyon | Docker + Docker Compose | ≥ 4.x | 6 servisin izole çalışması |
| Mesaj Kuyruğu | Apache Kafka (Confluent) | 7.4.0 | Gerçek zamanlı streaming simülasyonu |
| Akış İşleme | Apache Spark Structured Streaming | 3.5.1 | Kafka'dan okuma, Delta'ya yazma |
| Depolama | Delta Lake | 3.1.0 | ACID uyumlu, versiyonlanan veri gölü |
| Koordinasyon | Apache ZooKeeper | 7.4.0 | Kafka broker koordinasyonu |
| ML & Deney Takibi | Spark MLlib + MLflow | — | Model eğitimi ve deney yönetimi |
| Dashboard | Streamlit + Plotly | 1.50+ | İnteraktif görselleştirme |
| Yerel Ortam | Python venv + JupyterLab | 3.9+ | Notebook çalıştırma |

---

## 3. Veri Seti Tanımı

### 3.1. Kaynak ve Erişim

Chicago Open Data platformu, şehrin tüm verilerini SODA API aracılığıyla kamuya açık hâlde sunmaktadır. Proje kapsamında kullanılan "Crimes — 2001 to Present" veri seti, sayfalama (`$limit` / `$offset`) parametreleriyle toplamda 2.000.000 kayıt indirilmiştir.

**API Endpoint:** `https://data.cityofchicago.org/resource/ijzp-q8t2.json`  
**İndirme Yöntemi:** 50.000 kayıt/istek × 40 istek = 2.000.000 kayıt  
**Toplam Süre:** ~5–10 dakika  

### 3.2. Veri Şeması

Kullanılan başlıca sütunlar aşağıdaki tabloda özetlenmiştir:

| Sütun | Tip | Açıklama |
|---|---|---|
| `id` | integer | Benzersiz suç kaydı kimliği |
| `date` | string (ISO) | Suçun gerçekleştiği tarih/saat |
| `primary_type` | string | Suç tipi (THEFT, BATTERY vb.) |
| `description` | string | Suç alt tipi açıklaması |
| `location_description` | string | Olay yeri türü (STREET, RESIDENCE vb.) |
| `arrest` | boolean | Tutuklama yapıldı mı? |
| `domestic` | boolean | Aile içi şiddet mi? |
| `beat` | integer | En küçük devriye birimi |
| `district` | integer | Polis bölgesi (1–25) |
| `ward` | integer | Belediye meclisi bölgesi |
| `community_area` | integer | Topluluk alanı kodu |
| `latitude` / `longitude` | double | GPS koordinatları |

### 3.3. Temel İstatistikler

| Metrik | Değer |
|---|---|
| Toplam kayıt sayısı | 2.000.000 |
| Benzersiz suç tipi | 30 |
| Polis bölgesi sayısı | 22 |
| Tutuklama oranı | %15,4 (308.285 tutuklama) |
| Aile içi şiddet oranı | %18,0 (360.000 olay) |
| Koordinat eksikliği | %1,4 (GPS kaydı olmayan) |
| Kapsanan yıllar | 2001 – 2026 |

---

## 4. Adım 1 — Docker Ortamının Kurulumu

### 4.1. Genel Bakış

Projenin tüm bileşenleri Docker konteynerleri içerisinde çalıştırılmaktadır. Bu yaklaşım; ortam bağımlılıklarını ortadan kaldırmakta, tekrarlanabilirliği garanti etmekte ve servisler arası izolasyonu sağlamaktadır.

### 4.2. Servisler

`docker-compose.yml` dosyasında tanımlanan altı servis şunlardır:

| Servis | İmaj | Port | Görev |
|---|---|---|---|
| `chicago_zookeeper` | confluentinc/cp-zookeeper:7.4.0 | 2181 | Kafka koordinasyonu |
| `chicago_kafka` | confluentinc/cp-kafka:7.4.0 | 9092, 29092 | Mesaj kuyruğu |
| `chicago_spark_master` | özel (apache/spark:3.5.1 tabanlı) | 8080, 7077 | Spark koordinatörü |
| `chicago_spark_worker` | özel (apache/spark:3.5.1 tabanlı) | 8081 | Spark işçi düğümü |
| `chicago_producer` | özel Python | — | CSV → Kafka mesaj üretici |
| `chicago_mlflow` | python:3.11-slim | 5001 | MLflow deney takip sunucusu |

### 4.3. Özel Spark Docker İmajı

Varsayılan Apache Spark imajına ek olarak aşağıdaki bileşenler derleme aşamasında (build-time) imaja dahil edilmiştir:

- **Delta Lake JARs:** `delta-spark_2.12-3.1.0.jar`, `delta-storage-3.1.0.jar`  
  *(Spark 3.5.1 ile uyumlu; Delta 3.2+ sürümleri `SupportsNonDeterministicExpression` sınıfı eksikliği nedeniyle çalışmamaktadır)*
- **Kafka Bağlayıcı JARs:** `spark-sql-kafka-0-10_2.12-3.5.1.jar`, `spark-token-provider-kafka-0-10_2.12-3.5.1.jar`, `kafka-clients-3.4.1.jar`, `commons-pool2-2.12.0.jar`
- **Python Paketleri:** pandas, numpy, mlflow, delta-spark, scikit-learn

JARların derleme aşamasında imaja dahil edilmesi; çalışma zamanında Maven/Ivy üzerinden indirme gerekliliğini ve bu süreçte oluşan izin hatalarını ortadan kaldırmaktadır.

### 4.4. Servis Başlatma

```bash
docker compose build --no-cache spark-master spark-worker
docker compose up -d
```

> #### 📸 EKRAN GÖRÜNTÜİSÜ — Docker Servisleri Çalışır Durumda
> *`docker compose ps` komutu çıktısı — tüm 6 konteynerin "running" durumunda gösterildiği ekran görüntüsü.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: docker_services_running.png ]**

---

## 5. Adım 2 — Kafka ile Streaming Veri Üretimi

### 5.1. Producer Mimarisi

`services/producer/app/producer.py` dosyasında yer alan Python Kafka Producer, yerel CSV dosyasından satırları okuyarak yapılandırılabilir bir hızda JSON formatında Kafka topic'ine mesaj göndermektedir.

**Temel Özellikler:**

| Özellik | Değer |
|---|---|
| Mesaj formatı | JSON |
| Gönderim hızı | 2.000 mesaj/saniye |
| Toplam gönderilen mesaj | 2.000.000 |
| Tahmini süre | ~17 dakika |
| Kafka topic | `chicago_crimes_raw` |

### 5.2. Mesaj Formatı

Her Kafka mesajı aşağıdaki alanları içermektedir:

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

**`synthetic_user_id` hakkında:** Orijinal veri setinde kullanıcı kimliği bulunmadığından `district`, `beat` ve `ward` alanlarının MD5 hash'i kullanılarak deterministik bir kullanıcı kimliği üretilmiştir. Gerçek kişisel veri kullanılmamaktadır.

### 5.3. Kafka Altyapısı

Kafka topic'i tek broker üzerinde 3 partition ile yapılandırılmıştır. Mesaj gönderimi 100 mesaj aralıklarla `flush()` çağrısıyla garanti altına alınmaktadır.

> #### 📸 EKRAN GÖRÜNTÜSÜ — Kafka Producer Çalışırken
> *`[INFO] 2000000 messages sent to topic 'chicago_crimes_raw'` satırının görüldüğü terminal ekran görüntüsü.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: kafka_producer_output.png ]**

---

## 6. Adım 3 — Spark Structured Streaming ile Veri İşleme

### 6.1. Bronze Katmanı — Ham Veri Preservasyonu

`jobs/01_stream_kafka_to_bronze.py` dosyası, Kafka topic'inden gelen tüm mesajları ham hâliyle Bronze Delta tablosuna yazmaktadır.

**Teknik Detaylar:**
- Okuma modu: `readStream`, `startingOffsets: "earliest"`
- Tetikleyici: `trigger(availableNow=True)` — tüm mevcut mesajları işle ve dur
- Yazma modu: `append`, Delta format
- Checkpoint: `delta/checkpoints/bronze_chicago_crimes_raw`

Bronze katmanı, veri kaybını önlemek amacıyla ham mesajların olduğu gibi saklandığı bir veri korunum katmanıdır. İşleme hatası durumunda Silver ve Gold katmanları Bronze'dan yeniden üretilebilir.

### 6.2. Silver Katmanı — Temizleme ve Tip Dönüşümü

`jobs/02_bronze_to_silver.py`, Bronze verisi üzerinde aşağıdaki dönüşümleri gerçekleştirmektedir:

| İşlem | Açıklama |
|---|---|
| Tip dönüşümü | `beat`, `district`, `ward`, `community_area` → integer; `latitude`, `longitude` → double |
| Zaman damgası ayrıştırma | `date` → `crime_timestamp` (ISO 8601 formatı: `yyyy-MM-dd'T'HH:mm:ss.SSS`) |
| Null temizliği | `crime_id` veya `primary_type` null olan satırlar düşürülür |
| Tekilleştirme | `crime_id` üzerinden `dropDuplicates()` |
| Normalizasyon | `primary_type` ve `location_description` büyük harfe çevrilir |

**Giriş:** ~2.000.000 satır (Bronze)  
**Çıkış:** ~2.000.000 satır (Silver — temiz, tekilleştirilmiş)

### 6.3. Gold Katmanı — Analitik Hazırlık

`jobs/03_silver_to_gold.py`, Silver verisi üzerine analiz amaçlı türetilmiş sütunlar eklemektedir:

| Eklenen Sütun | Kaynak | Açıklama |
|---|---|---|
| `crime_hour` | `crime_timestamp` | Suçun gerçekleştiği saat (0–23) |
| `crime_day_of_week` | `crime_timestamp` | Haftanın günü (1=Pazar, 7=Cumartesi) |
| `crime_month` | `crime_timestamp` | Ay (1–12) |
| `is_weekend` | `crime_day_of_week` | Hafta sonu ise 1, değilse 0 |
| `is_night` | `crime_hour` | 22:00–05:00 arası ise 1 |
| `arrest_int` | `arrest` | Boolean → integer dönüşümü |
| `domestic_int` | `domestic` | Boolean → integer dönüşümü |

> #### 📸 EKRAN GÖRÜNTÜSÜ — Spark Jobs Başarılı Çıktıları
> *`[SUCCESS] Silver Delta written to: ...` ve `[SUCCESS] Gold Delta written to: ...` satırlarının görüldüğü terminal çıktısı.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: spark_pipeline_success.png ]**

---

## 7. Adım 4 — Keşifsel Veri Analizi (EDA)

**Notebook:** `notebooks/03_eda.ipynb`  
**Veri Kaynağı:** Delta Lake Gold tablosu (2.000.000 kayıt)

### 7.1. Temel İstatistikler

| Metrik | Değer |
|---|---|
| Toplam kayıt | 2.000.000 |
| Benzersiz suç tipi | 30 |
| En sık görülen 3 suç | THEFT (%22,3), BATTERY (%18,2), CRIMINAL DAMAGE (%11,1) |
| Polis bölgesi sayısı | 22 |
| Genel tutuklama oranı | %15,4 |
| Suç başına ortalama saat | 13:24 |

### 7.2. Suç Tipi Dağılımı

EDA analizine göre suçların büyük çoğunluğu birkaç kategori altında yoğunlaşmaktadır. THEFT ve BATTERY birlikte toplam suçların %40'ından fazlasını oluşturmaktadır. Ancak tutuklama oranı açısından suç tipleri arasında dramatik bir fark gözlemlenmektedir: Narkotik suçlarda tutuklama oranı %75 iken mülk hırsızlığında bu oran yalnızca %5'tir. Bu bulgu, sonraki makine öğrenmesi adımlarında suç tipinin en güçlü tahmin özniteliği olacağına işaret etmektedir.

> #### 📸 EKRAN GÖRÜNTÜSÜ — Top 10 Suç Tipi Dağılımı
> *`dashboard/figures/from-delta-lake/gold/top10_crime_types.png` grafiği.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: eda_top10_crime_types.png ]**

### 7.3. Zaman Serisi Analizi

**Saatlik Örüntü:**  
Günlük suç sayısı iki belirgin zirveye sahiptir: öğleden sonra (12:00 civarı) ve gece yarısı (00:00 civarı). Sabah erken saatlerde (03:00–06:00) minimum görülmektedir.

**Haftalık Örüntü:**  
Cuma günleri en yüksek suç hacmine sahipken Pazar günleri en düşük seviyedededir. Hafta sonu (Cumartesi–Pazar) eğlence bölgelerinde pik noktalara ulaşılmaktadır.

**Mevsimsel Örüntü:**  
Yaz aylarında (Haziran–Ağustos) suç sayısı belirgin biçimde artmaktadır. Bu örüntü, hava koşulları ile dışarıda geçirilen süre arasındaki ilişkiyle açıklanabilmektedir.

> #### 📸 EKRAN GÖRÜNTÜSÜ — Saatlik ve Günlük Trend Grafikleri
> *`dashboard/figures/dashboard/fig4_time_trends.png` grafiği.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: eda_time_trends.png ]**

### 7.4. Eksik Veri Analizi

| Sütun | Eksik Kayıt | Oran |
|---|---|---|
| `latitude` / `longitude` | ~28.000 | %1,4 |
| Diğer tüm sütunlar | 0 | %0,0 |

GPS koordinatlarının %1,4 oranında eksik olması, yalnızca coğrafi özellikler açısından önemlidir. Bu eksiklik, özellik mühendisliği aşamasında `coalesce(lat_grid, 0.0)` ile doldurulmuştur.

### 7.5. Coğrafi Dağılım

Suçların coğrafi dağılımı incelendiğinde, 41,88°K–41,90°K koridoruna (Downtown Chicago) yoğunlaşma dikkat çekmektedir. Polis bölgeleri bazında en yüksek hacimler District 8, 11 ve 6'da gözlemlenmektedir.

> #### 📸 EKRAN GÖRÜNTÜSÜ — Chicago Suç Yoğunluğu Haritası
> *Streamlit dashboard Exp02 sekmesindeki interaktif scatter_mapbox haritası.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: eda_chicago_crime_map.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Gün × Saat Isı Haritası
> *`dashboard/figures/dashboard/fig8_weekday_hour_heatmap.png` grafiği.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: eda_weekday_hour_heatmap.png ]**

---

## 8. Adım 5 — Özellik Mühendisliği (Feature Engineering)

**Job:** `jobs/04_feature_engineering.py`  
**Notebook:** `notebooks/04_feature_engineering.ipynb`  
**Çıktı:** `delta/gold/ml_features` (~2.000.000 satır, 14 özellik)

### 8.1. Özellik Grupları ve İş Mantıkları

Üretilen özellikler dört mantıksal grupta organize edilmiştir. Her özelliğin seçilme gerekçesi aşağıda açıklanmıştır.

#### 8.1.1. Zaman Özellikleri

| Özellik | Üretim Yöntemi | İş Mantığı |
|---|---|---|
| `hour` | `hour(crime_timestamp)` | Gece ve gündüz saatlerinde farklı suç örüntüleri; devriye yoğunluğu gece düşük |
| `day_of_week` | `dayofweek(crime_timestamp)` | Hafta sonu sosyal hareketlilik ve gece hayatı suç riskini artırır |
| `month` | `month(crime_timestamp)` | Mevsimsel dalgalanmalar; yaz aylarında suç belirgin biçimde artmaktadır |
| `is_weekend` | `day_of_week ∈ {1, 7}` | Hafta sonu polis kaynaklarının dağılımı farklılaşır |
| `is_night` | `hour ∈ [22, 05]` | Gece saatlerinde görgü tanığı azlığı ve devriye sıklığının düşmesi tutuklama oranını etkiler |

#### 8.1.2. Davranışsal Özellik

| Özellik | Üretim Yöntemi | İş Mantığı |
|---|---|---|
| `domestic_numeric` | `arrest.cast(boolean)` | Illinois Zorunlu Tutuklama Yasası: aile içi olaylarda tutuklama eğilimi belirgin biçimde yüksektir |

#### 8.1.3. Coğrafi Özellikler

| Özellik | Üretim Yöntemi | İş Mantığı |
|---|---|---|
| `lat_grid` | `round(latitude, 2)` | ~1km'lik coğrafi ızgara; ham koordinat yerine ızgara hücresi kullanmak aşırı öğrenmeyi azaltır |
| `lon_grid_abs` | `abs(round(longitude, 2))` | Chicago negatif boylam bandındadır; mutlak değer alınarak pozitif hâle getirilmiştir |

#### 8.1.4. Kategorik Özellikler

| Özellik | Üretim Yöntemi | İş Mantığı |
|---|---|---|
| `location_group` | Kural tabanlı gruplandırma | Olay yerini 7 anlamlı kategoriye (STREET, RESIDENCE, PARKING...) indirger; seyrek değerleri önler |
| `district_group` | `district.cast(string)` | Her polis bölgesinin kendine özgü uygulama politikası ve tutuklama oranı bulunmaktadır |
| `primary_type_group` | Top-10 + "OTHER" gruplandırması | Nadir suç tiplerinin model performansına zarar vermesini önler |

### 8.2. Veri Sızıntısı Kontrolü

Makine öğrenmesi modellerinin gerçek dünya tahmin senaryosunu yansıtabilmesi için aşağıdaki sütunlar özellik setine dahil edilmemiştir:

| Dışlanan Sütun | Dışlanma Gerekçesi |
|---|---|
| `arrest` | Hedef değişken — özellik olarak kullanılması veri sızıntısına yol açar |
| `domestic` | Exp03'te hedef değişken olarak kullanılmaktadır |
| `iucr` | Suç tipiyle bire bir ilişkilidir; tahmin değeri taşımaz |
| `description` | Suç tipinin alt kategorisi; doğrudan `primary_type`'ı kodlar |

### 8.3. Hedef Değişkenler

`ml_features` tablosu üç farklı hedef değişkeni depolamaktadır:

| Hedef | Tip | Kullanım Deneyi |
|---|---|---|
| `arrest_label` | binary (0/1) | Exp01 — tutuklama tahmini |
| `crime_group` | string (VIOLENT/PROPERTY/OTHER) | Opsiyonel suç tipi analizi |
| `crime_type_str` | string (top-10 + OTHER) | Suç tipi tahmini |

---

## 9. Adım 6 — Makine Öğrenmesi ve Çoklu Model Karşılaştırması

Üç bağımsız makine öğrenmesi deneyi yürütülmüştür. Her deney için beş farklı Spark MLlib modeli eğitilmiş, değerlendirme metrikleri hesaplanmış ve tüm sonuçlar MLflow ile takip edilmiştir.

### 9.1. Deney 1 — Tutuklama Tahmini (Binary Sınıflandırma)

**MLflow Deney Adı:** `exp01_chicago_arrest_classification`  
**Job:** `jobs/05_train_models_mlflow.py`

#### 9.1.1. Problem Tanımı

Bir suç olayının özelliklerine (tip, konum, zaman, bağlam) bakılarak tutuklamanın gerçekleşip gerçekleşmeyeceği tahmin edilmektedir. Bu, operasyon merkezlerinin nakliye kapasiteli araç görevlendirmesini optimize etmesine yönelik pratik bir uygulamadır.

- **Hedef:** `arrest_label` (0 = tutuklama yok, 1 = tutuklama var)
- **Sınıf Dengesizliği:** Verilerin yalnızca %15,4'ü tutuklamayla sonuçlanmaktadır. Bu ciddi dengesizlik, sınıf ağırlıklandırması (class weighting) ile giderilmiştir; tutuklu sınıfı yaklaşık 5,5 kat ağırlık almaktadır.
- **Veri Bölünmesi:** %80 eğitim / %20 test (sabit tohum: 42)

#### 9.1.2. Eğitim Konfigürasyonu

| Model | Temel Hiperparametreler |
|---|---|
| Logistic Regression | maxIter=100, regParam=0.01, elasticNetParam=0.1 |
| Decision Tree | maxDepth=10, weightCol="classWeight" |
| Random Forest | numTrees=50, maxDepth=8, weightCol="classWeight" |
| GBT Classifier | maxIter=30, maxDepth=5, stepSize=0.1 |
| Naive Bayes | smoothing=1.0, modelType="multinomial" |

#### 9.1.3. Sonuçlar

| Model | Doğruluk | F1 | Kesinlik | Duyarlılık | AUC-ROC |
|---|---|---|---|---|---|
| **GBT Classifier** *(en iyi)* | **%89,5** | **0,879** | **0,890** | **0,895** | **0,859** |
| Random Forest | %78,4 | 0,807 | 0,856 | 0,784 | 0,854 |
| Decision Tree | %79,2 | 0,813 | 0,859 | 0,792 | 0,582 |
| Logistic Regression | %72,1 | 0,755 | 0,827 | 0,721 | 0,793 |
| Naive Bayes | %57,7 | 0,634 | 0,775 | 0,577 | 0,450 |

**En Önemli Özellikler (GBT):**
1. `primary_type_group_idx` — %73,2 önem skoru *(Narkotik suçlarda tutuklama oranı %75, hırsızlıkta %5)*
2. `location_group_idx` — %13,2
3. `district_group_idx` — %14,6

#### 9.1.4. Karışıklık Matrisi (GBT — En İyi Model)

|  | Tahmin: Tutuklama Yok | Tahmin: Tutuklama |
|---|---|---|
| **Gerçek: Tutuklama Yok** | TN = 331.722 | FP = 5.120 |
| **Gerçek: Tutuklama** | FN = 36.905 | TP = 25.826 |

- **Kesinlik (Precision):** TP / (TP + FP) = 25.826 / 30.946 = **0,835**
- **Duyarlılık (Recall):** TP / (TP + FN) = 25.826 / 62.731 = **0,412**

> #### 📸 EKRAN GÖRÜNTÜSÜ — MLflow Exp01 Deney Sayfası
> *`http://localhost:5001/#/experiments/415036514690670316/evaluation-runs` — 5 modelin AUC-ROC, Accuracy, F1 değerlerinin karşılaştırıldığı MLflow arayüzü.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: mlflow_exp01_runs.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Exp01 5 Model Karşılaştırma Grafiği
> *`dashboard/figures/exp01_arrest_2m/exp01_model_comparison.png` — grouped bar chart.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: exp01_model_comparison.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Exp01 ROC Eğrisi
> *`dashboard/figures/exp01_arrest_2m/exp01_roc_curve.png`*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: exp01_roc_curve.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Exp01 Karışıklık Matrisi
> *`dashboard/figures/exp01_arrest_2m/exp01_confusion_matrix.png`*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: exp01_confusion_matrix.png ]**

---

### 9.2. Deney 2 — Suç Yoğunluğu Regresyonu

**MLflow Deney Adı:** `exp02_crime_density_regression`  
**Job:** `jobs/06_crime_density_regression.py`

#### 9.2.1. Problem Tanımı

Bir 1km²'lik coğrafi ızgara hücresinde, belirli bir zaman penceresinde (saat × gün × ay kombinasyonu) kaç suç bekleneceği tahmin edilmektedir. Bu, polislerin devriye çıkmadan önce yüksek riskli bölgelere konuşlanmasına olanak sağlayan kestirimci bir devriye optimizasyonu uygulamasıdır.

- **Veri Hazırlığı:** 2.000.000 satır, `(lat_grid, lon_grid_abs, hour, day_of_week, month)` kombinasyonlarına göre gruplanmış; 764.393 benzersiz hücre elde edilmiştir.
- **Hedef:** `crime_count` — her pencere için düşen suç sayısı (ortalama: 2,58, maksimum: 71)
- **Veri Bölünmesi:** %80 eğitim / %20 test

#### 9.2.2. Sonuçlar

| Model | RMSE ↓ | MAE ↓ | R² ↑ |
|---|---|---|---|
| **GBT Regressor** *(en iyi)* | **1,723** | **1,174** | **0,445** |
| Decision Tree Regressor | 1,770 | 1,192 | 0,415 |
| Random Forest Regressor | 1,800 | 1,230 | 0,395 |
| Linear Regression | 2,267 | 1,476 | 0,039 |
| Generalized Linear Regression | 2,290 | 1,501 | 0,020 |

**Yorum:**  
GBT modeli, suç sayısı varyansının **%44,5**'ini açıklamaktadır. RMSE=1,72 değeri, tahminlerin gerçek değerden ortalama ±1,72 suç sapması gösterdiği anlamına gelmektedir; bu, devriye planlaması için yeterli bir hassasiyettir. En yüksek suç yoğunluğu **41,88°K 87,63°B** koordinatında saat başına 15 suç olarak tespit edilmiş olup şehir ortalamasının (2,58) yaklaşık 6 katıdır.

> #### 📸 EKRAN GÖRÜNTÜSÜ — MLflow Exp02 Deney Sayfası
> *`http://localhost:5001/#/experiments/697634395395337699/evaluation-runs` — 5 regresör modelin RMSE, MAE, R² metriklerinin karşılaştırıldığı MLflow arayüzü.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: mlflow_exp02_runs.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Chicago Suç Yoğunluğu Haritası
> *`dashboard/figures/exp02_density/chicago_crime_heatmap.png` — bölgesel suç yoğunluğu.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: exp02_chicago_heatmap.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Top 20 Öncelikli Devriye Bölgesi
> *`dashboard/figures/exp02_density/top20_hotspots.png`*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: exp02_top20_hotspots.png ]**

---

### 9.3. Deney 3 — Müdahale Protokolü Sınıflandırması (4-Sınıflı)

**MLflow Deney Adı:** `exp03_dispatch_protocol_classification`  
**Job:** `jobs/07_dispatch_protocol.py`

#### 9.3.1. Problem Tanımı

Her suç olayı, herhangi bir memur sahaya gitmeden önce dört müdahale protokolünden birine sınıflandırılmaktadır. Sınıflar, `domestic` (aile içi mi?) ve `arrest` (tutuklama gerekli mi?) değişkenlerinin kombinasyonundan oluşmaktadır.

| Sınıf | Anlam | Frekans | Müdahale Türü |
|---|---|---|---|
| 0 | Aile Dışı + Tutuklama Yok | %67,8 | Standart rapor birimi |
| 1 | Aile Dışı + Tutuklama Var | %12,6 | Nakliye kapasiteli birim |
| 2 | Aile İçi + Tutuklama Yok | %16,6 | Aile içi şiddet eğitimli ekip |
| **3** | **Aile İçi + Tutuklama Var** | **%2,9** | **Zorunlu tutuklama ekibi** |

**Sınıf 3'ün Kritikliği:**  
Illinois Zorunlu Tutuklama Yasası (Mandatory Arrest Law), aile içi şiddet vakasında olası sebebin varlığı hâlinde memurun tutuklama yapmak zorunda olduğunu hükme bağlamaktadır. Sınıf 3, verilerin yalnızca %2,9'unu oluşturmasına karşın hukuki sorumluluk açısından en kritik vakaları temsil etmektedir.

#### 9.3.2. Sınıf Ağırlıklandırması

Ters frekans ağırlıklandırması uygulanmıştır:

- Sınıf 0: 0,369× ağırlık (baskın sınıf, düşük ağırlık)
- Sınıf 1: 1,977× ağırlık
- Sınıf 2: 1,502× ağırlık
- **Sınıf 3: 8,544× ağırlık** (nadir ama kritik sınıf, yüksek ağırlık)

#### 9.3.3. Eğitim Konfigürasyonu

Bellek kısıtı nedeniyle eğitim seti 601.694 satır (2M'dan örnekleme) ile sınırlandırılmış; test seti olarak ise tam 399.569 satır kullanılmıştır.

#### 9.3.4. Sonuçlar

| Model | Doğruluk | Ağırlıklı F1 | Recall Sınıf 3 |
|---|---|---|---|
| **Random Forest** *(en iyi)* | **%64,2** | **0,682** | **%70,0** |
| Decision Tree | %61,8 | 0,661 | %63,0 |
| Logistic Regression | %57,2 | 0,616 | %64,2 |
| MLP (Neural Net) | %67,5 | 0,549 | %0,0 |
| Naive Bayes | %47,7 | 0,500 | %0,0 |

**Temel Bulgu:**  
Random Forest modeli, en kritik Sınıf 3 vakalarının **%70,0**'ını doğru tespit etmektedir. MLP ve Naive Bayes modellerinin Sınıf 3 duyarlılığının sıfır olması, bu modellerin aşırı dengesizliği aşamadığını göstermektedir.

> #### 📸 EKRAN GÖRÜNTÜSÜ — MLflow Exp03 Deney Sayfası
> *`http://localhost:5001/#/experiments/480956894876426590/evaluation-runs` — sınıf bazlı recall değerlerinin karşılaştırıldığı MLflow arayüzü.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: mlflow_exp03_runs.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Exp03 Sınıf Bazlı Recall Grafiği
> *`dashboard/figures/exp03_dispatch/exp03_per_class_recall.png`*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: exp03_per_class_recall.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Exp03 4×4 Karışıklık Matrisi
> *`dashboard/figures/exp03_dispatch/exp03_confusion_matrix.png`*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: exp03_confusion_matrix.png ]**

---

## 10. Adım 7 — Görselleştirme ve Dashboard

**Uygulama:** `dashboard/streamlit_app.py`  
**Erişim:** `http://localhost:8501`

### 10.1. Dashboard Mimarisi

Dashboard, Streamlit çerçevesi üzerine inşa edilmiş olup Plotly kütüphanesi aracılığıyla interaktif grafikler sunmaktadır. Tüm grafikler gerçek veriden (2M kayıt veya MLflow CSV raporları) anlık olarak üretilmektedir; statik PNG dosyası kullanılmamaktadır.

### 10.2. Sekme İçerikleri

#### Sekme 1 — EDA (Keşifsel Analiz)
- Saatlik suç sayısı alan grafiği (2M kayıt)
- Haftanın günü çubuk grafiği (hafta sonu kırmızı vurgu)
- Aylık trend çizgi grafiği
- Top 10 suç tipi yatay çubuk grafiği
- Tutuklama oranı halka diyagramı
- Suç tipine göre tutuklama oranı karşılaştırması
- Gün × Saat yoğunluk ısı haritası
- **Chicago suç konum scatter haritası** (2M kayıt, top-6 suç tipi renklendirilmiş)
- Aile içi / dışı dağılımı ve top 10 olay yeri

#### Sekme 2 — Exp01 Tutuklama Sınıflandırması
- 5 model × 5 metrik grouped bar chart
- AUC-ROC sıralama çubuğu
- Tutuklu sınıfı duyarlılık karşılaştırması
- ROC eğrisi (5 model)
- Karışıklık matrisi ısı haritası
- Özellik önemi yatay çubuğu
- En iyi model için Türkçe yorum kutusu

#### Sekme 3 — Exp02 Suç Yoğunluğu Regresyonu
- RMSE / MAE / R² üçlü yan yana grafik
- Artık dağılımı histogramı
- Gerçek vs tahmin saçılım grafiği
- **İnteraktif Chicago patrol haritası** (scatter_mapbox)
- Top 20 öncelikli devriye bölgesi çubuğu
- En iyi model için Türkçe yorum kutusu

#### Sekme 4 — Exp03 Müdahale Protokolü
- 5 model karşılaştırma grouped bar chart
- Sınıf bazlı duyarlılık grouped bar chart
- Sınıf dağılımı halka diyagramı
- F1 sıralama çubuğu
- 4×4 karışıklık matrisi
- Özellik önemi çubuğu
- En iyi model için Türkçe yorum kutusu

> #### 📸 EKRAN GÖRÜNTÜSÜ — Streamlit Dashboard Ana Sayfası (EDA Sekmesi)
> *`http://localhost:8501` — EDA sekmesinin genel görünümü.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: dashboard_eda_tab.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Streamlit Dashboard Exp01 Sekmesi
> *5 model karşılaştırma ve ROC eğrisi görünümü.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: dashboard_exp01_tab.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Streamlit Dashboard Exp02 Harita
> *İnteraktif patrol haritası ve top-20 hotspot grafiği.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: dashboard_exp02_map.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Streamlit Dashboard Exp03 Sekmesi
> *Sınıf bazlı duyarlılık ve 4×4 karışıklık matrisi.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: dashboard_exp03_tab.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — MLflow Tüm Deneyler Sayfası
> *`http://localhost:5001/#/experiments` — üç deneyin listelendiği MLflow ana ekranı.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: mlflow_all_experiments.png ]**

---

## 11. Karşılaşılan Zorluklar ve Çözümler

Proje sürecinde çeşitli teknik güçlüklerle karşılaşılmış; her biri sistematik biçimde analiz edilerek çözüme kavuşturulmuştur.

| # | Zorluk | Sebep | Çözüm |
|---|---|---|---|
| 1 | `GBTClassifier` çok sınıflı sınıflandırmayı desteklemiyor | Spark MLlib GBT yalnızca ikili (0/1) etiket kabul ediyor | `OneVsRest` sarmalayıcı; Exp03'te `MultilayerPerceptronClassifier` alternatifi |
| 2 | `RandomForest` 2M satırda Java Heap OOM | 150 ağaç × depth-10 bellek sınırını aşıyor | numTrees 150→50, maxDepth 10→6; Exp03 eğitim seti 600k örnekleme |
| 3 | `spark.driver.memory` Python kodunda etkisiz | JVM başlamadan önce ayarlanması gerekiyor | `--driver-memory 4g` parametresi `spark-submit` komutuna eklendi |
| 4 | Delta 3.3.2 — `SupportsNonDeterministicExpression` hatası | Sınıf Spark 3.5.2'de tanımlandı; konteyner 3.5.1 çalıştırıyor | Delta 3.1.0 (Spark 3.5.1 ile tam uyumlu sürüm) kullanıldı |
| 5 | MLflow "Invalid Host header" 403 hatası | HTTP sunucusunun DNS rebinding koruması Spark konteynerinin isteğini reddediyor | HTTP izleme URI → `file:///app/mlruns` paylaşımlı volume çözümü |
| 6 | Özellik vektöründe NaN değerler | GPS koordinatı eksik olan ~28.000 kayıt | `coalesce(lat_grid, lit(0.0))` + `.fillna(0)` vektör birleştirme öncesi |
| 7 | `dashboard/` dizini Spark konteynerinde erişilemiyor | `docker-compose.yml` volume mount eksikliği | `./dashboard:/app/dashboard` bind mount eklendi |
| 8 | Ivy cache izin hatası (`/home/spark/.ivy2`) | Spark imajında `spark` kullanıcısının yazma izni yok | Dockerfile'da `mkdir -p /home/spark/.ivy2 && chown -R spark:spark` eklendi |

---

## 12. Değerlendirme Ölçütleri ve Rubrik Karşılaştırması

| Değerlendirme Kriteri | Ağırlık | Durum | Detay |
|---|---|---|---|
| Docker & Altyapı | %15 | ✅ | `docker-compose.yml`, 6 servis Dockerfile, Kafka+Spark+Delta JARs build-time dahil |
| Kafka Streaming | %15 | ✅ | Producer, 2M mesaj, 2.000 msg/sn, JSON format, synthetic_user_id |
| Spark + Delta Lake | %15 | ✅ | Bronze/Silver/Gold, ACID, şema dönüşümü, streaming + batch |
| EDA & Feature Engineering | %10 | ✅ | 14 özellik (min. 5), iş mantığı belgelenmiş, Delta'ya kaydedilmiş |
| ML Modelleri & MLflow | %15 | ✅ | 5 model × 3 deney, AUC-ROC/F1/CM/Feature Importance, MLflow entegrasyonu |
| Dashboard & Görselleştirme | %15 | ✅ | Streamlit 4 sekme, Plotly interaktif, 9+ zorunlu görsel, scatter harita |
| Dokümantasyon & Sunum | %15 | ✅ | README, RUNBOOK, teknik rapor, akademik yazım |

---

## 13. Sonuç

Bu çalışmada, Chicago şehrine ait 2.000.000 suç kaydı üzerinde modern bir uçtan uca büyük veri pipeline'ı başarıyla hayata geçirilmiştir. Sistem, Apache Kafka aracılığıyla gerçek zamanlı veri akışı simüle etmekte; Spark Structured Streaming ile verileri Bronze, Silver ve Gold Delta katmanlarında işlemekte ve üç bağımsız makine öğrenmesi deneyi yürütmektedir.

**Temel Bulgular:**

1. **Tutuklama Tahmini (Exp01):** GBT algoritması AUC-ROC=0,859 ile en yüksek performansı sergilemiştir. Suç tipinin tahmin önem skorundaki %73,2'lik payı, narkotik ile mülk suçları arasındaki dramatik tutuklama oranı farkından kaynaklanmaktadır.

2. **Suç Yoğunluğu Regresyonu (Exp02):** GBT regresör modeli R²=0,445 ile suç sayısı varyansının %44,5'ini açıklamış; RMSE=1,72 değeri devriye planlaması için yeterli hassasiyeti sağlamıştır. Chicago'nun en yüksek riskli noktası 41,88°K 87,63°B koordinatında tespit edilmiştir.

3. **Müdahale Protokolü (Exp03):** Random Forest modeli, Illinois Zorunlu Tutuklama Yasası kapsamındaki Sınıf 3 vakalarının %70'ini doğru sınıflandırmayı başarmıştır; bu, sınıfın yalnızca verilerin %2,9'unu oluşturduğu göz önüne alındığında anlamlı bir başarıdır.

Proje, teorik veri mühendisliği bilgisinin gerçek dünya veri seti ve endüstriyel standart araçlarla pratiğe dökülmesini başarıyla sergilemektedir.

---

## Kaynaklar

1. Chicago Data Portal. *Crimes — 2001 to Present.* https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2 (Erişim: Mayıs 2026)

2. Apache Software Foundation. *Apache Spark Documentation — Structured Streaming Programming Guide*, Sürüm 3.5.1. https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html

3. Delta.io. *Delta Lake Documentation*, Sürüm 3.1.0. https://docs.delta.io/3.1.0/

4. Confluent Inc. *Apache Kafka Documentation*. https://kafka.apache.org/documentation/

5. MLflow. *MLflow Documentation — Tracking.* https://mlflow.org/docs/latest/tracking.html

6. Streamlit Inc. *Streamlit Documentation.* https://docs.streamlit.io/

7. Meng, X., Bradley, J., Yavuz, B., Sparks, E., Venkataraman, S., Liu, D., ... & Zaharia, M. (2016). MLlib: Machine learning in apache spark. *Journal of Machine Learning Research*, 17(1), 1235-1241.

8. Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., ... & Stoica, I. (2016). Apache spark: A unified engine for big data processing. *Communications of the ACM*, 59(11), 56-65.

---

*Teknik Rapor — Chicago Crime Big Data Pipeline Projesi*  
*Büyük Veri Analizine Giriş, 2025-2026 Bahar Dönemi*  
*13 Mayıs 2026*
