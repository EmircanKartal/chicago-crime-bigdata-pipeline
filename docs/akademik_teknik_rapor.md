# Büyük Veri Analizine Giriş — Dönem Projesi
# Teknik Rapor

---

**Ders:** Büyük Veri Analizine Giriş  
**Danışman:** Asst. Prof. Ayşe Gül Eker  
**Dönem:** 2025-2026 Bahar  
**Teslim Tarihi:** 13 Mayıs 2026  

**Ekip Üyeleri:**  
- Emircan Kartal  
- Meryem Berfin Kenar  
- Kağan Gür  

**Veri Seti:** Chicago Crime Data (Chicago Open Data — ~7,9M kayıt)  
**Kullanılan Kayıt Sayısı:** 2.000.000  
---

## Özet

Günümüz kentsel yönetiminde, suç örüntülerinin analiz edilmesi ve kaynakların etkin biçimde dağıtılması hem güvenlik birimleri hem de şehir planlamacıları açısından kritik bir öneme sahiptir. Bu çalışmada, söz konusu ihtiyaca yanıt vermek amacıyla Chicago şehrine ait 2.000.000 suç kaydı üzerinde uçtan uca bir büyük veri mühendisliği ve makine öğrenmesi pipeline'ı tasarlanmış, hayata geçirilmiş ve kapsamlı biçimde değerlendirilmiştir.

Projenin teknik altyapısı, birbirini tamamlayan çeşitli açık kaynak teknolojilerinin entegrasyonu üzerine inşa edilmiştir. Veri akışı katmanında Apache Kafka kullanılarak Chicago Open Data platformundan indirilen 2.000.000 kayıt, saniyede 2.000 mesaj hızında gerçek zamanlı bir streaming simülasyonuyla sisteme beslenmiştir. Gelen mesajlar Apache Spark Structured Streaming aracılığıyla işlenmiş; ham veri, temizlenmiş veri ve analitik açıdan hazır veri olmak üzere üç katmandan (Bronze, Silver, Gold) oluşan Delta Lake mimarisine yazılmıştır. Delta Lake'in ACID uyumluluğu, şema zorlama ve zaman yolculuğu (time travel) özellikleri, veri güvenilirliği açısından üretim ortamı standartlarının sağlanmasına olanak tanımıştır.

Veri işleme pipeline'ının ardından, 14 anlamlı özellik içeren bir makine öğrenmesi girdi tablosu oluşturulmuştur. Bu süreçte veri sızıntısı (data leakage) problemine özellikle dikkat edilmiş; tutuklama bilgisi, suç alt tipi açıklamaları ve IUCR kodları gibi doğrudan hedef değişkeni kodlayan sütunlar özellik setinden çıkarılmıştır. Türetilen özellikler zaman, coğrafi ızgara, davranışsal ve kategorik olmak üzere dört grupta sınıflandırılmış; her birinin gerçek dünya bağlamındaki iş mantığı belgelenmiştir.

Makine öğrenmesi aşamasında Spark MLlib kütüphanesi kullanılarak üç bağımsız deney yürütülmüştür. Birinci deneyde (Exp01), suç olaylarının özelliklerine bakılarak tutuklamanın gerçekleşip gerçekleşmeyeceği ikili sınıflandırma problemi olarak modellenmiştir. Verilerin yalnızca %15,4'ünün tutuklamayla sonuçlanması nedeniyle ortaya çıkan ciddi sınıf dengesizliği, ters frekans ağırlıklandırması ile giderilmiştir. Gradient Boosted Trees (GBT) algoritması AUC-ROC=0,859 ile en yüksek performansı sergilemiş; suç tipinin tahmin önem skorundaki %73,2'lik payı dikkat çekici bir bulgu olarak öne çıkmaktadır. İkinci deneyde (Exp02), coğrafi ızgara hücresi ve zaman dilimi bazında suç yoğunluğunun tahminini amaçlayan bir regresyon modeli kurulmuştur. 764.393 benzersiz ızgara-zaman kombinasyonu üzerinde eğitilen GBT regresör modeli R²=0,445 değerine ulaşmış; bu oran, modelin suç yoğunluğu varyansının yaklaşık yarısını açıklayabildiğini göstermektedir. Üçüncü deneyde (Exp03) ise aile içi şiddet boyutunu ve Illinois Zorunlu Tutuklama Yasası çerçevesini dikkate alan dört sınıflı bir müdahale protokolü sınıflandırıcısı geliştirilmiştir. Bu modelin temel amacı, operasyon merkezinin olay yerine ulaşmadan önce doğru ekibi ve ekipmanı görevlendirmesine imkân tanımaktır. Özellikle %2,9 oranında görülen ancak hukuki açıdan en kritik sınıf olan "aile içi + tutuklama" vakalarını (Sınıf 3) tespit edebilmek için söz konusu sınıfa 8,54 kat ağırlık uygulanmış; Random Forest modeli bu kritik sınıfta %70 duyarlılık (recall) değerine ulaşmıştır. Tüm deneyler MLflow ile parametreler, metrikler ve model artifact'ları dahil olmak üzere kayıt altına alınmıştır.

Proje çıktıları, dört sekmeden oluşan Streamlit tabanlı interaktif bir dashboard aracılığıyla sunulmaktadır. Dashboard; keşifsel veri analizi görselleri, her deney için model karşılaştırma grafikleri, Chicago'nun suç yoğunluğunu gerçek harita üzerinde gösteren interaktif patrol haritası ve devriye optimizasyonu için öncelikli bölgeler listesini içermektedir. Tüm grafikler, statik PNG dosyası yerine anlık veri hesaplamasına dayalı Plotly bileşenleriyle oluşturulmaktadır.

Teknik açıdan değerlendirildiğinde, projenin endüstriyel büyük veri pipeline tasarım pratiğine uygun olduğu; gerçek zamanlı veri akışı, katmanlı depolama mimarisi, otomatik deney takibi ve interaktif görselleştirme bileşenlerinin bütünleşik çalıştığı görülmektedir. Karşılaşılan başlıca teknik güçlükler arasında Spark sürüm uyumluluğu sorunları, bellek yönetimi kısıtları ve MLflow izleme altyapısının konteyner ortamında yapılandırılması yer almaktadır; her biri belgelenmiş çözümlerle aşılmıştır.

**Anahtar Kelimeler:** Büyük Veri Mühendisliği, Apache Kafka, Apache Spark Structured Streaming, Delta Lake, Lakehouse Mimarisi, MLflow, Spark MLlib, Gradient Boosted Trees, Sınıf Dengesizliği, Suç Analizi, Devriye Optimizasyonu, Illinois Zorunlu Tutuklama Yasası

---

## 1. Giriş

### 1.1. Motivasyon ve Arka Plan

Kentsel güvenlik yönetimi, büyük şehirlerin karşı karşıya olduğu en karmaşık operasyonel sorunların başında gelmektedir. Kayıtlara geçmiş suç olaylarının hacmi her yıl artarken polis departmanlarının kullanabileceği personel, araç ve bütçe kaynakları sabit kalmakta ya da kısıtlı biçimde büyümektedir. Bu asimetri, karar vericilerin mevcut kaynakları nereye, ne zaman ve nasıl yoğunlaştırması gerektiğini bilimsel bir temele oturtma ihtiyacını doğurmaktadır. Geleneksel yaklaşımlarda devriye rotaları çoğunlukla tarihsel deneyime ve sezgisel değerlendirmelere dayanmaktadır; oysa milyonlarca satırlık geçmiş veri içinde saklı olan mekânsal ve zamansal örüntüler, uygun analitik araçlar kullanıldığında sistematik biçimde ortaya çıkarılabilmektedir.

Veri odaklı karar alma (data-driven decision making) paradigması, son on yılda polis bilimleri alanında önemli bir yer edinmiştir. Chicago, New York ve Los Angeles gibi büyük Amerikan şehirleri suç tahmin sistemleri geliştirmiş; bu sistemler sıcak nokta (hot-spot) devriyeciliği, önleyici konuşlanma ve kaynak önceliklendirme gibi uygulamalara zemin hazırlamıştır. Bununla birlikte, söz konusu sistemlerin büyük çoğunluğu kapalı kaynaklı veya akademik erişime kapalı yapılardır. Chicago Open Data platformunun tüm suç verilerini kamuya açık tutması, bu alanda bağımsız araştırmacılara ve öğrenci projelerine eşsiz bir fırsat sunmaktadır.

Öte yandan, yalnızca analitik bir çalışma olmanın ötesinde, projenin ikinci bir motivasyon boyutu daha bulunmaktadır: modern veri mühendisliği araçlarının gerçek bir probleme entegre biçimde uygulanması. Apache Kafka, Apache Spark, Delta Lake ve MLflow gibi endüstri standardı bileşenler yalnızca kavramsal düzeyde öğrenilmekle kalmayıp işlevsel bir pipeline içinde birlikte çalışır hâle getirilmiştir.

### 1.2. Problemin Tanımı

Bu çalışmada üç bağımsız ancak birbirleriyle ilişkili tahmin problemi ele alınmaktadır:

**Problem 1 — Tutuklama Tahmini:** Bir suç olayının gözlemlenebilir özellikleri (suç tipi, olay yeri, gerçekleştiği saat ve gün, polis bölgesi) göz önünde bulundurulduğunda, olayın tutuklamayla sonuçlanıp sonuçlanmayacağı öngörülebilir mi? Bu, operasyon merkezlerinin olay yerine nakliye kapasiteli araç gönderip göndermeyeceğini olaydan önce kestirebildiği pratik bir karar destek uygulamasına karşılık gelmektedir.

**Problem 2 — Suç Yoğunluğu Tahmini:** Şehrin belirli bir coğrafi ızgara hücresinde, belirli bir zaman diliminde kaç suç beklenmektedir? Zaman ve mekânın kesişiminden türetilen bu tahmin, devriye birimlerinin anlık konuşlanma kararları için temel bir girdi niteliği taşımaktadır.

**Problem 3 — Müdahale Protokolü Sınıflandırması:** Bir olay bildirimi alındığında, olay yerine görevlendirilen birimin hangi donanım ve uzmanlıkla sahaya çıkması gerektiği önceden belirlenebilir mi? Bu problemi özgün kılan husus, Illinois eyalet hukukunun aile içi şiddet vakalarında memura koşulsuz tutuklama yükümlülüğü getiren Zorunlu Tutuklama Yasası'dır. Sınıf 3 olarak tanımlanan "aile içi şiddet + tutuklama zorunluluğu" vakası verilerin yalnızca %2,9'unu oluşturmasına karşın hukuki ve operasyonel açıdan en kritik kategoriyi temsil etmektedir.

### 1.3. Çalışmanın Katkıları

Bu çalışma, birden fazla katmanda özgün katkılar sunmaktadır:

- **Teknik katkı:** Kafka → Spark Structured Streaming → Delta Lake Bronze/Silver/Gold → Spark MLlib → MLflow zincirinden oluşan, Docker ile tamamen konteynerize edilmiş ve yeniden üretilebilir (reproducible) bir uçtan uca büyük veri pipeline'ı tasarımı ve gerçekleştirimi.
- **Analitik katkı:** Chicago suç verisi üzerinde sınıf dengesizliğine karşı dayanıklı, veri sızıntısı kontrolü uygulanmış ve üç farklı tahmin hedefi için özelleştirilmiş özellik mühendisliği yaklaşımı.
- **Pratik katkı:** Suç yoğunluğu tahminini gerçek harita görselleştirmesiyle birleştiren ve öncelikli devriye bölgelerini otomatik olarak sıralayan bir karar destek arayüzü.
- **Belgeleme katkısı:** Her teknik kararın ve karşılaşılan güçlüğün gerekçesiyle birlikte kayıt altına alındığı kapsamlı bir teknik rapor ve çalıştırma kılavuzu.

### 1.4. Projenin Kapsamı

Çalışma, ödev tanımında belirlenen yedi adımın tamamını kapsamaktadır:

1. Docker ile konteynerize edilmiş servis ortamının kurulumu ve doğrulanması
2. Apache Kafka ile 2.000.000 kayıtlık gerçek zamanlı veri akışı simülasyonu
3. Spark Structured Streaming ile akış verisi işleme ve Delta Lake katmanlarına yazma
4. Delta Lake Gold tablosu üzerinde kapsamlı keşifsel veri analizi
5. 14 anlamlı özellik içeren makine öğrenmesi girdi tablosunun oluşturulması
6. Beş farklı modelin üç bağımsız deneyde eğitimi ve MLflow ile karşılaştırmalı değerlendirilmesi
7. Tüm bulguların interaktif Streamlit dashboard aracılığıyla sunumu

### 1.5. Rapor Organizasyonu

Bu raporun geri kalanı şu şekilde düzenlenmiştir: Bölüm 2'de kullanılan teknolojilerin teorik arka planı ve ilgili çalışmalar ele alınmaktadır. Bölüm 3'te genel sistem mimarisi ve veri akışı şeması sunulmaktadır. Bölüm 4, kullanılan veri setini kapsamlı biçimde tanıtmaktadır. Bölüm 5 ile 11 arasında, ödev tanımındaki yedi adım sırasıyla detaylandırılmaktadır. Bölüm 12'de karşılaşılan teknik güçlükler ve benimsenen çözümler tartışılmaktadır. Bölüm 13'te çalışmanın genel değerlendirmesi yapılmakta ve gelecek çalışmalara yönelik öneriler sunulmaktadır.

---


## 2. İlgili Çalışmalar ve Teknolojik Arka Plan

### 2.1. Suç Tahmini ve Prediktif Polislik

Suç verisi üzerinde makine öğrenmesi uygulamaları, 2010'ların başından itibaren hem akademide hem de kamu yönetiminde giderek yaygınlaşmaktadır. Bu alandaki çalışmalar genel olarak iki ana kategoride incelenebilir: belirli bir lokasyon için suç olasılığı ya da yoğunluğunu tahmin etmeye çalışan mekânsal tahmin modelleri ve bireysel suç olaylarının sonucunu (tutuklama, mahkûmiyet, yeniden suç işleme) öngörmeye çalışan olay bazlı sınıflandırma modelleri. Predpol ve ShotSpotter gibi ticari sistemler birinci kategorinin en bilinen endüstriyel örnekleri arasındadır. Bununla birlikte, söz konusu sistemlerin algoritmaları çoğunlukla kapalı kaynaklıdır ve önyargı (bias) sorunlarına yönelik eleştiriler akademik literatürde de yer bulmaktadır (Lum & Isaac, 2016).

Bu çalışma, ticari sistemlerin aksine tamamen açık kaynaklı araçlara dayanmakta ve tüm metodolojik kararların şeffaf biçimde belgelenmesini ön planda tutmaktadır.

### 2.2. Akış Veri İşleme: Apache Kafka ve Spark Structured Streaming

Apache Kafka, LinkedIn tarafından geliştirilen ve dağıtık sistemlerde yüksek verimli mesaj kuyruğu (message broker) görevi üstlenen bir platform olarak tanımlanmaktadır (Kreps ve diğerleri, 2011). Kafka'nın kalıcı (persistent) log yapısı, üretici ve tüketici taraflarının birbirinden bağımsız hızlarda çalışabilmesine imkân tanıması nedeniyle gerçek zamanlı veri akışı altyapılarında yaygın biçimde tercih edilmektedir.

Apache Spark Structured Streaming, Spark 2.0 ile birlikte tanıtılan ve Spark'ın DataFrame tabanlı işleme motorunu akış verisi üzerine genişleten bir bileşendir (Armbrust ve diğerleri, 2018). Sonsuz bir tablo (unbounded table) soyutlaması üzerine inşa edilen bu yaklaşım, geliştiricilerin akış ve toplu (batch) işleme kodunu tek bir API ile yazabilmesine olanak tanıyarak veri mühendisliği iş yükünü anlamlı ölçüde azaltmaktadır.

### 2.3. Delta Lake ve Lakehouse Mimarisi

Delta Lake, Databricks tarafından geliştirilen ve klasik veri ambarı ile veri gölü yaklaşımlarını bir araya getirerek "lakehouse" mimarisini hayata geçiren açık kaynaklı bir depolama katmanıdır (Armbrust ve diğerleri, 2020). ACID işlem güvencesi, şema zorlama (schema enforcement) ve zaman yolculuğu (time travel) gibi özellikler sunan Delta Lake; ham veriden analitik tablolara uzanan dönüşüm zincirini güvenilir biçimde yönetmektedir.

Bu çalışmada benimsenen Bronze/Silver/Gold katman mimarisi (Medallion Architecture), Delta Lake belgelerinde önerilen en iyi uygulama (best practice) standardını yansıtmaktadır: Bronze ham veri koruma, Silver temizlenmiş ve tipleştirilmiş veri, Gold ise analitik ve makine öğrenmesi için hazırlanmış türetilmiş veri katmanına karşılık gelmektedir.

### 2.4. MLflow ile Deney Yönetimi

MLflow, Databricks tarafından geliştirilen ve makine öğrenmesi deneylerinin parametrelerini, metriklerini ve model artifact'larını kayıt altına alan açık kaynaklı bir deney takip platformudur (Chen ve diğerleri, 2020). MLflow Tracking API'si, eğitim sürecindeki her çalıştırmayı (run) bağımsız olarak kaydederek farklı algoritmalar ve hiperparametre kombinasyonları arasında karşılaştırmalı analizi kolaylaştırmaktadır. Bu çalışmada üç ayrı MLflow deneyi (experiment) oluşturulmuş ve 15 model eğitim çalıştırması kayıt altına alınmıştır.

---

## 3. Sistem Tasarımı ve Mimari

### 3.1. Genel Pipeline Mimarisi

Projenin uçtan uca veri akışı aşağıdaki katmanlardan oluşmaktadır. Her katman, bir önceki katmanın çıktısını girdi olarak almakta; bu sayede bireysel bileşenler bağımsız olarak test edilebilmekte ve güncellenerek yeniden çalıştırılabilmektedir.

```
[Chicago Open Data]
         │  SODA API · 50k kayıt/istek · 40 istek = 2M satır
         ▼
[data/raw/chicago_crimes_2m.csv]
         │  Python Kafka Producer · 2.000 msg/sn
         ▼
[Kafka Topic: chicago_crimes_raw]  ← 2.000.000 JSON mesajı
         │  Spark Structured Streaming · trigger(availableNow=True)
         ▼
[Delta Bronze: delta/bronze/chicago_crimes_raw]
         │  Batch ETL · tip dönüşümü + temizleme + tekilleştirme
         ▼
[Delta Silver: delta/silver/chicago_crimes_clean]
         │  Türetilmiş sütun ekleme
         ▼
[Delta Gold: delta/gold/chicago_crimes_features]
         │  14 ML özelliği · veri sızıntısı kontrolü
         ▼
[Delta Gold: delta/gold/ml_features]
         │
         ├──► Exp01: Tutuklama Tahmini        [MLflow: exp01]
         ├──► Exp02: Suç Yoğunluğu Regresyonu [MLflow: exp02]
         └──► Exp03: Müdahale Protokolü       [MLflow: exp03]
                              │
                              ▼
              [Streamlit Dashboard — http://localhost:8501]
```

### 3.2. Konteyner Mimarisi

Tüm servisler `docker-compose.yml` dosyasında tanımlanmış ve Docker ağı (network) üzerinden birbirleriyle iletişim kurmaktadır. Her servis kendi bağımlılıklarını izole biçimde barındırdığından, sistemin herhangi bir makineye klonlanarak `docker compose up -d` komutuyla ayağa kaldırılması yeterlidir.

| Servis | İmaj | İç Port | Dış Port | Bağımlılık |
|---|---|---|---|---|
| chicago_zookeeper | cp-zookeeper:7.4.0 | 2181 | 2181 | — |
| chicago_kafka | cp-kafka:7.4.0 | 9092 | 9092, 29092 | zookeeper |
| chicago_spark_master | özel (Spark 3.5.1) | 7077 | 8080, 7077 | kafka |
| chicago_spark_worker | özel (Spark 3.5.1) | — | 8081 | spark-master |
| chicago_producer | özel Python | — | — | kafka |
| chicago_mlflow | python:3.11-slim | 5000 | 5001 | — |

### 3.3. Volume ve Ağ Yapılandırması

Servisler arası veri paylaşımı Docker bind mount'ları aracılığıyla sağlanmaktadır. `delta/`, `reports/`, `mlruns/` ve `dashboard/` dizinleri, hem Spark konteynerlerinin yazma hem de yerel makinenin okuma erişimine açık olacak şekilde yapılandırılmıştır. Bu tasarım sayesinde Spark içinde üretilen Delta tabloları ve MLflow artifact'ları, konteyner dışında da okunabilmektedir.

---

## 4. Veri Seti

### 4.1. Kaynak ve Edinim

Bu çalışmada kullanılan veri seti, Chicago Belediyesi'nin kamuya açık veri platformu olan Chicago Data Portal üzerinden SODA (Socrata Open Data API) aracılığıyla temin edilmiştir. "Crimes — 2001 to Present" veri seti, 2001 yılından itibaren Chicago Polis Departmanı tarafından kayıt altına alınan tüm suç olaylarını içermekte olup veri seti dokümantasyonuna göre yaklaşık 7,9 milyon kayıttan oluşmaktadır.

Proje kapsamında, API'nin `$limit` ve `$offset` parametrelerinden yararlanılarak sayfalama (pagination) yöntemiyle 50.000 kayıt/istek oranında 40 art arda istek yapılmış ve toplamda 2.000.000 kayıt yerel depolamaya indirilmiştir.

```bash
# Veri indirme komutu
python3 scripts/download_chicago_data.py \
  --limit 2000000 \
  --output data/raw/chicago_crimes_2m.csv \
  --batch-size 50000

# Beklenen çıktı (son satır):
# [SUCCESS] Final row count: 2000000
```

**Doğrulama:**
```bash
wc -l data/raw/chicago_crimes_2m.csv
# Beklenen: 2000001 (başlık + 2M satır)
```

### 4.2. Veri Şeması

İndirilen veri seti aşağıdaki sütunları içermektedir:

| Sütun | Veri Tipi | Örnek Değer | Açıklama |
|---|---|---|---|
| `id` | integer | 14183823 | Benzersiz suç kaydı kimliği |
| `date` | string (ISO 8601) | 2026-05-01T00:00:00.000 | Suçun tarihi ve saati |
| `primary_type` | string | BATTERY | Ana suç kategorisi (30 benzersiz değer) |
| `description` | string | SIMPLE | Suç alt tipi |
| `location_description` | string | STREET | Olay yeri tanımı |
| `arrest` | boolean | False | Tutuklama yapıldı mı? |
| `domestic` | boolean | False | Aile içi şiddet mi? |
| `beat` | integer | 122 | En küçük devriye birimi |
| `district` | integer | 1 | Polis bölgesi (1–25) |
| `ward` | integer | 42 | Belediye meclisi bölgesi |
| `community_area` | integer | 32 | Chicago topluluk alanı kodu |
| `latitude` | double | 41.8846 | GPS enlem koordinatı |
| `longitude` | double | -87.6324 | GPS boylam koordinatı |

### 4.3. Temel İstatistiksel Özellikler

| Özellik | Değer |
|---|---|
| Toplam kayıt sayısı | 2.000.000 |
| Benzersiz suç tipi (`primary_type`) | 30 |
| En sık görülen suç tipi | THEFT (%22,3) |
| İkinci en sık suç tipi | BATTERY (%18,2) |
| Tutuklama oranı | %15,4 (308.285 tutuklama) |
| Aile içi şiddet oranı | %18,0 (~360.000 olay) |
| GPS koordinatı eksik kayıt | %1,4 (~28.000 kayıt) |
| Diğer sütunlarda eksik kayıt | %0,0 |
| Kapsanan yıl aralığı | 2001–2026 |

### 4.4. Sınıf Dengesizliği

Makine öğrenmesi perspektifinden değerlendirildiğinde, veri setinin en belirgin özelliği sınıf dengesizliğidir. Tutuklama tahmini problemi için hedef değişken olan `arrest` sütununun dağılımı incelendiğinde, kayıtların yalnızca %15,4'ünün tutuklamayla sonuçlandığı görülmektedir. Bu oran, öğrenme algoritması için doğal bir önyargı (bias) riski oluşturmakta; iyi ayarlanmamış modeller tüm olayları "tutuklama yok" olarak sınıflandırarak yüksek doğruluk (accuracy) elde edebilmektedir. Bu sorun, özellik mühendisliği ve model eğitimi aşamalarında ters frekans ağırlıklandırması (inverse-frequency weighting) kullanılarak ele alınmıştır.

---

## 5. Adım 1 — Docker Ortamının Kurulumu

### 5.1. Konteynerize Mimarinin Gerekçesi

Üretim ortamına yakın bir geliştirme deneyimi sağlamak ve bağımlılık çakışmalarını önlemek amacıyla projenin tüm bileşenleri Docker konteynerleri içinde çalıştırılmaktadır. Bu yaklaşımın sağladığı başlıca avantajlar şu şekilde özetlenebilir: Sistemi klonlayan herhangi bir kullanıcı, yerel Python ya da Java kurulumundan bağımsız olarak aynı ortamı elde edebilmekte; servisler birbirinden izole çalışarak kaynak çakışmaları önlenmekte ve tüm servis konfigürasyonu `docker-compose.yml` adlı tek bir dosyada sürüm kontrollü biçimde tutulmaktadır.

### 5.2. Özel Spark İmajı

Varsayılan `apache/spark:3.5.1` imajı, Delta Lake ve Kafka bağlayıcı JARlarını içermemektedir. Bu eksikliklerin giderilmesi için `services/spark/Dockerfile` dosyasında özelleştirilmiş bir imaj tanımlanmıştır. JAR dosyaları, çalışma zamanında Maven/Ivy üzerinden indirilmek yerine derleme (build) aşamasında imaja gömülmüştür; bu sayede her `spark-submit` çağrısında ağ erişimi gerekmemektedir.

```dockerfile
FROM apache/spark:3.5.1
USER root

RUN pip install --no-cache-dir pandas numpy matplotlib scikit-learn mlflow "delta-spark==3.1.0"

# Delta Lake 3.1.0 — Spark 3.5.1 ile uyumlu sürüm
RUN curl -fL "https://repo1.maven.org/.../delta-spark_2.12-3.1.0.jar" \
    -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar

# Kafka bağlayıcı JARları
RUN curl -fL "https://repo1.maven.org/.../spark-sql-kafka-0-10_2.12-3.5.1.jar" \
    -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar
# ... (diğer Kafka JARları)

RUN mkdir -p /home/spark/.ivy2 && chown -R spark:spark /home/spark/.ivy2
USER spark
```

> **Not:** Delta 3.1.0 sürümü bilinçli olarak seçilmiştir. Delta 3.2+ sürümleri, Spark 3.5.2 ile eklenen `SupportsNonDeterministicExpression` sınıfına bağımlılık içermekte; bu sınıf Spark 3.5.1'de bulunmadığından çalışma zamanında `NoClassDefFoundError` hatasına yol açmaktadır.

### 5.3. Servislerin Başlatılması

```bash
# İmajları derle (ilk kurulumda veya Dockerfile değiştiğinde)
docker compose build --no-cache spark-master spark-worker

# Tüm servisleri başlat
docker compose up -d
```

Servislerin başarıyla ayağa kalktığı aşağıdaki komutla doğrulanmaktadır:

```bash
docker compose ps
```

**Beklenen çıktı:**
```
NAME                     STATUS
chicago_zookeeper        running
chicago_kafka            running
chicago_spark_master     running
chicago_spark_worker     running
chicago_producer         running
chicago_mlflow           running
```

> #### 📸 EKRAN GÖRÜNTÜSÜ — Servisler Çalışıyor (Assignment Zorunluluğu)
> *`docker compose ps` çıktısı — tüm 6 konteynerin "running" durumunda gösterildiği terminal görüntüsü.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: docker_services_running.png ]**

---

## 6. Adım 2 — Kafka ile Streaming Veri Üretimi

### 6.1. Producer Tasarım Prensipleri

`services/producer/app/producer.py` dosyasında uygulanan Kafka Producer, CSV dosyasını satır satır okuyarak her satırı ayrı bir JSON mesajına dönüştürmekte ve `chicago_crimes_raw` topic'ine iletmektedir. Mesaj gönderim hızı (`PRODUCE_RATE_PER_SEC`) ve maksimum mesaj sayısı (`MAX_MESSAGES`) ortam değişkenleri aracılığıyla yapılandırılabilmektedir. Bu esneklik; hem kısa smoke test çalıştırmalarına (10k kayıt) hem de tam ölçek denemelerine (2M kayıt) imkân tanımaktadır.

Her mesajın anahtarı (key), suç kaydının benzersiz `id` alanı olarak belirlenmiştir. Bu tercih, Kafka'nın aynı anahtara sahip mesajları aynı partition'a yönlendirme garantisi sayesinde, ileride ihtiyaç duyulabilecek partition-aware tüketicilerin doğru veri yerelliğiyle çalışmasına zemin hazırlamaktadır. Gerçek kişisel tanımlayıcı içermeyen veri seti için özgün bir kullanıcı kimliği ihtiyacı, `district`, `beat` ve `ward` alanlarının MD5 hash'i alınarak üretilen `synthetic_user_id` ile karşılanmıştır.

### 6.2. Mesaj Formatı

```json
{
  "ingest_ts": "2026-05-10T14:32:00Z",
  "synthetic_user_id": "user_a3f9d12b",
  "event_type": "THEFT",
  "primary_type": "THEFT",
  "crime_id": "14183823",
  "case_number": "JK238082",
  "date": "2026-05-01T00:00:00.000",
  "block": "001XX N LA SALLE ST",
  "location_description": "STREET",
  "arrest": false,
  "domestic": false,
  "district": "001",
  "beat": "0122",
  "community_area": "32",
  "latitude": 41.884,
  "longitude": -87.632
}
```

### 6.3. Üretim Çalıştırması ve Beklenen Çıktı

```bash
docker compose exec producer python /app/app/producer.py
```

**Beklenen terminal çıktısı (örnek aralıklar):**
```
[INFO] Kafka Producer starting...
[INFO] Bootstrap servers: chicago_kafka:9092
[INFO] Topic: chicago_crimes_raw
[INFO] Produce rate: 2000 msg/sec
[INFO] Max messages: 2000000
[INFO] 100 messages sent to topic 'chicago_crimes_raw'
[INFO] 10000 messages sent to topic 'chicago_crimes_raw'
...
[INFO] 1990000 messages sent to topic 'chicago_crimes_raw'
[INFO] 2000000 messages sent to topic 'chicago_crimes_raw'
[SUCCESS] Producer finished. Total messages sent: 2000000
```

Yaklaşık 17 dakika süren bu işlemin ardından `chicago_crimes_raw` topic'i, tüm veri setine karşılık gelen 2.000.000 mesajı barındırmaktadır.

---

## 7. Adım 3 — Spark Structured Streaming ile Veri İşleme

### 7.1. Bronze Katmanı — Ham Veri Koruması

Medallion mimarisinin ilk katmanı olan Bronze, Kafka'dan gelen mesajları herhangi bir dönüşüm uygulanmaksızın Delta formatında depolar. Bu yaklaşımın temel gerekçesi, veri işleme pipeline'ında ilerleyen aşamalarda hata oluştuğunda kayıt kaybına uğramadan yeniden işleme (reprocessing) yapılabilmesini güvence altına almaktır.

```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/01_stream_kafka_to_bronze.py
```

**Beklenen çıktı:**
```
[SUCCESS] Bronze Delta written to: /app/delta/bronze/chicago_crimes_raw
```

**Teknik detaylar:** `trigger(availableNow=True)` tetikleyicisi, topic'teki mevcut tüm mesajları okuyup yazdıktan sonra akışı otomatik olarak sonlandırmaktadır. Checkpoint konumu `delta/checkpoints/bronze_chicago_crimes_raw` olarak belirlenmiş; bu sayede yeniden çalıştırma durumunda yalnızca yeni mesajlar işlenmektedir.

### 7.2. Silver Katmanı — Temizleme ve Tip Dönüşümü

Bronze verisi üzerindeki dönüşüm adımları şu sırayla uygulanmaktadır:

```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/02_bronze_to_silver.py
```

**Beklenen çıktı:**
```
[INFO] Before cleaning row count: 2000000
[INFO] After null cleaning row count: ~2000000
[INFO] After duplicate cleaning row count: ~2000000
[SUCCESS] Silver Delta written to: /app/delta/silver/chicago_crimes_clean
```

| İşlem | Uygulama | Etki |
|---|---|---|
| Tip dönüşümü | `district`, `beat`, `ward` → integer; `latitude`, `longitude` → double | Analitik sorgularda doğruluk |
| Zaman damgası ayrıştırma | `date` → `crime_timestamp` (ISO 8601) | Temporal özellik üretimi için zemin |
| Null temizliği | `crime_id` veya `primary_type` null olanlar düşürülür | Kritik tanımlayıcı eksikliği giderilir |
| Tekilleştirme | `dropDuplicates(["crime_id"])` | Birden fazla kez iletilen mesajların etkisi ortadan kalkar |
| Normalizasyon | `primary_type`, `location_description` → büyük harf | Kategorik tutarlılık |

### 7.3. Gold Katmanı — Analitik Zenginleştirme

```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/03_silver_to_gold.py
```

**Beklenen çıktı:**
```
[INFO] Gold row count: ~2000000
[SUCCESS] Gold Delta written to: /app/delta/gold/chicago_crimes_features
```

Gold katmanında Silver verisi üzerine yedi türetilmiş sütun eklenmektedir:

```python
gold_df = silver_df \
  .withColumn("crime_hour",        hour(col("crime_timestamp")))        \
  .withColumn("crime_day_of_week", dayofweek(col("crime_timestamp")))   \
  .withColumn("crime_month",       month(col("crime_timestamp")))       \
  .withColumn("is_weekend", when(dayofweek(...).isin([1,7]), 1).otherwise(0)) \
  .withColumn("is_night",   when((hour(...) >= 22) | (hour(...) <= 5), 1).otherwise(0)) \
  .withColumn("arrest_int",   when(lower(col("arrest"))   == "true", 1).otherwise(0)) \
  .withColumn("domestic_int", when(lower(col("domestic")) == "true", 1).otherwise(0))
```

Bu işlemler sayesinde sonraki adımlarda doğrudan sayısal özellik olarak kullanılabilecek bir Gold tablosu elde edilmektedir.

> #### 📸 EKRAN GÖRÜNTÜSÜ — Pipeline İş Başarı Çıktıları
> *Üç Spark job'ının başarı mesajlarını gösteren terminal görüntüsü.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: spark_pipeline_success.png ]**


## 8. Adım 4 — Keşifsel Veri Analizi (EDA)

**Notebook:** `notebooks/03_eda.ipynb` | **Veri:** Delta Lake Gold tablosu (2.000.000 kayıt)

### 8.1. Temel İstatistikler

Delta Gold tablosu üzerinde yürütülen ilk analizde, veri setinin temel sayısal ve kategorik özellikleri belirlenmiştir:

| Metrik | Değer |
|---|---|
| Toplam kayıt | 2.000.000 |
| Benzersiz suç tipi | 30 |
| Polis bölgesi sayısı | 22 |
| Tutuklama oranı | %15,4 |
| Aile içi şiddet oranı | %18,0 |
| GPS koordinatı eksik | %1,4 |
| En aktif polis bölgesi | District 8 |
| Günlük ortalama suç sayısı | ~548 (2M kayıt / ~3650 gün) |

En sık görülen üç suç tipi sırasıyla THEFT (%22,3), BATTERY (%18,2) ve CRIMINAL DAMAGE (%11,1) olarak belirlenmiştir. Bu üç kategori toplam suç hacminin yaklaşık %52'sini oluşturmaktadır. Ancak suç tipine göre tutuklama oranı incelendiğinde kategoriler arasında dramatik farklılıklar gözlemlenmektedir: narkotik suçlarda tutuklama oranı %75 iken mülk hırsızlığında bu oran yalnızca %5 düzeyinde kalmaktadır. Bu bulgu, sonraki makine öğrenmesi aşamasında suç tipinin en güçlü tahmin değişkeni olacağına erken işaret etmektedir.

### 8.2. Zaman Serisi Analizi

**Saatlik örüntü:** Günlük suç yoğunluğu iki belirgin zirvede toplanmaktadır: öğleden sonra (12:00 civarı) ve gece yarısı (00:00 civarı). Sabah erken saatler (03:00–06:00) minimum suç yoğunluğuna karşılık gelmektedir. Bu ikili zirve yapısı; ticari mekânların yoğun olduğu öğleden sonra saatlerindeki hırsızlıklar ile gece hayatının aktif olduğu saatlerdeki şiddet suçlarının örtüşmesinden kaynaklanmaktadır.

**Haftalık örüntü:** Cuma günleri haftalık suç hacminin en yüksek olduğu gün olarak öne çıkarken Pazar günleri en düşük seviyededir. Cumartesi-Pazar hafta sonu döneminde eğlence bölgelerinde suç yoğunluğunun artması, `is_weekend` ikili değişkeninin özellik olarak anlamlılığını desteklemektedir.

**Mevsimsel örüntü:** Haziran–Ağustos döneminde suç sayısında belirgin bir artış, Ocak–Şubat döneminde ise gözlemlenebilir bir düşüş mevcuttur. Chicago'nun karasal ikliminde dışarıda geçirilen sürenin kış aylarında belirgin biçimde kısalması bu örüntüyü açıklamaktadır.

> #### 📸 EKRAN GÖRÜNTÜSÜ — Zaman Serisi ve Dağılım Grafikleri (Assignment Zorunluluğu)
> *Saatlik trend çizgi grafiği, gün × saat ısı haritası ve suç tipi dağılım grafiği.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: eda_time_series_distributions.png ]**

### 8.3. Coğrafi Dağılım

Suçların coğrafi dağılımı incelendiğinde, 41,88°K–41,90°K enlem koridoruna (Downtown Chicago) belirgin bir yoğunlaşma dikkat çekmektedir. Polis bölgeleri bazında en yüksek hacimler sırasıyla District 8, 11 ve 6'da gözlemlenmekte; bu üç bölge birlikte toplam suçların yaklaşık %25'ini barındırmaktadır. Bu coğrafi heterojenlik, `district_group` ve `lat_grid`/`lon_grid_abs` özelliklerinin makine öğrenmesi modellerinde işlevsel sinyal taşıyabileceğini doğrulamaktadır.

### 8.4. Eksik Veri Analizi

Eksik veri incelemesinde yalnızca `latitude` ve `longitude` sütunlarında %1,4 oranında kayıp tespit edilmiştir. Diğer tüm sütunlarda eksik kayıt bulunmamaktadır. Bu GPS eksiklikleri genellikle iç mekân suçları veya tam adres bilgisi girilemeyen vakalarla ilişkilendirilmektedir. Eksik koordinatlar özellik mühendisliği aşamasında `0.0` sentinel değeriyle doldurulmuş ve bir ikili `geo_available` değişkeni oluşturularak bu kayıtların modelde belirli bir örüntü taşıyıp taşımadığı da öğrenilmeye açılmıştır.

---

## 9. Adım 5 — Özellik Mühendisliği

**Job:** `jobs/04_feature_engineering.py` | **Notebook:** `notebooks/04_feature_engineering.ipynb`

### 9.1. Özellik Tasarım Prensipleri

Özellik mühendisliği sürecinde iki temel prensip benimsenmiştir. Birincisi, **veri sızıntısı önleme** ilkesidir: bir suç olayının sonucunu ya da tipini doğrudan kodlayan her türlü bilgi (tutuklama sonucu, suç alt tipi açıklaması, IUCR kodu) özellik setinden dışlanmıştır. Bu dışlamalar, modelin gerçek dünya tahmin senaryosunu yansıtmasını ve gelecek verilere genellenebilir örüntüler öğrenmesini sağlamaktadır. İkincisi, **temsil sıkıştırma** ilkesidir: ham değerler yerine semantik gruplandırmalar (örneğin 30 suç tipi → 10 grup + "OTHER") kullanılmış; bu yaklaşım hem modelin boyutunu yönetilebilir tutmakta hem de seyrek kategorilerin öğrenme sürecini olumsuz etkilemesini önlemektedir.

### 9.2. Üretilen Özellikler ve İş Gerekçeleri

```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/04_feature_engineering.py

# Beklenen çıktı:
# Feature row count: ~1999000
# [SUCCESS] ML feature table written to: /app/delta/gold/ml_features
```

| Grup | Özellik | Türetme Yöntemi | İş Gerekçesi |
|---|---|---|---|
| Zaman | `hour` | `hour(crime_timestamp)` | Gece/gündüz suç tipi dağılımı farklılaşır |
| Zaman | `day_of_week` | `dayofweek(crime_timestamp)` | Hafta içi/sonu davranışsal fark |
| Zaman | `month` | `month(crime_timestamp)` | Mevsimsel suç dalgalanması |
| Zaman | `is_weekend` | `day_of_week ∈ {1, 7}` | Polis kaynak yoğunluğu hafta sonu değişir |
| Zaman | `is_night` | `hour ∈ [22, 05]` | Gece saatlerinde görgü tanığı azlığı |
| Davranışsal | `domestic_numeric` | `domestic.cast(boolean)` | IL Zorunlu Tutuklama Yasası bağlamı |
| Coğrafi | `lat_grid` | `round(latitude, 2)` | ~1km ızgara; aşırı öğrenmeyi azaltır |
| Coğrafi | `lon_grid_abs` | `abs(round(longitude, 2))` | Chicago negatif boylam bandında |
| Kategorik | `location_group` | Kural tabanlı (7 grup) | Olay yeri tipleri devriye tepkisini etkiler |
| Kategorik | `district_group` | `district.cast(string)` | Her bölgenin özgün uygulama politikası |
| Kategorik | `primary_type_group` | Top-10 + "OTHER" | Seyrek kategorilerin boyut artışını önler |

### 9.3. Veri Sızıntısı Kontrol Matrisi

| Sütun | Neden Dışlandı |
|---|---|
| `arrest` | Exp01'de hedef değişken; özellik olarak kullanımı sızıntı oluşturur |
| `domestic` | Exp03'te hedef değişkenin bileşeni |
| `iucr` | Suç tipini bire bir kodlar; bağımsız öğrenme değeri taşımaz |
| `description` | `primary_type`'ın alt kategorisi; bağımsız sinyal içermez |
| `case_number` | Administratif tanımlayıcı; suç özelliğiyle ilişkisi yok |

---

## 10. Adım 6 — Makine Öğrenmesi ve Çoklu Model Karşılaştırması

### 10.1. Deney 1 — Tutuklama Tahmini (Binary Sınıflandırma)

**MLflow Deney:** `exp01_chicago_arrest_classification`

#### 10.1.1. Problem Formülasyonu

Bir suç olayının bireysel özelliklerinden yola çıkarak tutuklamanın gerçekleşip gerçekleşmeyeceğini öngören ikili sınıflandırma modeli geliştirilmiştir. Pozitif sınıf (tutuklama = 1) tüm verilerin yalnızca %15,4'ünü oluşturduğundan, sınıf dengesizliği problemi ters frekans ağırlıklandırması ile ele alınmıştır. Her örneğin ağırlığı `N / (2 × sınıf_sayısı)` formülüyle hesaplanmıştır.

```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/05_train_models_mlflow.py

# Beklenen çıktı (örnekleme):
# Train count: ~1,600,000  |  Test count: ~400,000
# ── Training: LogisticRegression
#    accuracy=0.7213  f1=0.7551  auc_roc=0.7932
# ── Training: GBTClassifier
#    accuracy=0.8948  f1=0.8794  auc_roc=0.8592
# ✓ Best model: GBTClassifier (AUC-ROC=0.8592)
```

#### 10.1.2. Model Sonuçları

| Model | Doğruluk | F1 | AUC-ROC | Recall (Tutuklu) |
|---|---|---|---|---|
| **GBT Classifier** | **%89,5** | **0,879** | **0,859** | %41,2 |
| Decision Tree | %79,2 | 0,813 | 0,582 | %73,6 |
| Random Forest | %78,4 | 0,807 | 0,854 | %73,3 |
| Logistic Regression | %72,1 | 0,755 | 0,793 | %67,0 |
| Naive Bayes | %57,7 | 0,634 | 0,450 | %58,5 |

GBT modeli, AUC-ROC metriğinde açık bir üstünlükle öne çıkmaktadır. AUC-ROC=0,859 değeri; modelin tüm eşik kombinasyonlarında rastgele tahminden önemli ölçüde daha iyi performans gösterdiğini ifade etmektedir. Karar Ağacı'nın yüksek doğruluk (%79,2) ama düşük AUC-ROC (0,582) değerleri kombinasyonu, çoğunluk sınıfına aşırı uyum (majority class overfitting) problemine işaret etmektedir.

> #### 📸 EKRAN GÖRÜNTÜSÜ — Exp01 Model Karşılaştırma, CM ve ROC (Assignment Zorunluluğu)
> *5 model performans karşılaştırma grafiği, en iyi modelin karışıklık matrisi ve ROC eğrisi.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: exp01_model_cm_roc.png ]**

> #### 📸 EKRAN GÖRÜNTÜSÜ — Exp01 Feature Importance (Assignment Zorunluluğu)
> *GBT modelinin özellik önemi yatay çubuk grafiği.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: exp01_feature_importance.png ]**

---

### 10.2. Deney 2 — Suç Yoğunluğu Regresyonu

**MLflow Deney:** `exp02_crime_density_regression`

#### 10.2.1. Veri Hazırlık Süreci

2.000.000 bireysel suç kaydı, `(lat_grid, lon_grid_abs, hour, day_of_week, month)` bileşik anahtarı üzerinden gruplanarak 764.393 benzersiz ızgara-zaman kombinasyonu elde edilmiştir. Her kombinasyonun suç sayısı bu gruplama üzerinden hesaplanmış ve regresyon hedef değişkeni olarak kullanılmıştır.

```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/06_crime_density_regression.py

# Beklenen çıktı:
# Grid cells × time windows: 764,393
# Mean crime_count: 2.58  |  Max: 71
# ── Training: GBTRegressor
#    RMSE=1.7233  MAE=1.1738  R²=0.4451
# ✓ Best model: GBTRegressor (R²=0.4451)
# Heatmap CSV: /app/reports/exp02_density/heatmap_data.csv (737 cells)
```

#### 10.2.2. Model Sonuçları

| Model | RMSE ↓ | MAE ↓ | R² ↑ |
|---|---|---|---|
| **GBT Regressor** | **1,723** | **1,174** | **0,445** |
| Decision Tree | 1,770 | 1,192 | 0,415 |
| Random Forest | 1,800 | 1,230 | 0,395 |
| Linear Regression | 2,267 | 1,476 | 0,039 |
| Generalized Linear Reg. | 2,290 | 1,501 | 0,020 |

Doğrusal modellerin (Linear Regression R²=0,039, GLR R²=0,020) yetersiz performansı, suç yoğunluğu ile coğrafi-zamansal değişkenler arasındaki ilişkinin doğrusal olmayan (non-linear) bir yapı sergilediğini ortaya koymaktadır. GBT ve ağaç tabanlı modeller bu doğrusal olmayan etkileşimleri yakalayabildiğinden belirgin biçimde daha başarılı sonuçlar elde etmiştir.

---

### 10.3. Deney 3 — Müdahale Protokolü Sınıflandırması

**MLflow Deney:** `exp03_dispatch_protocol_classification`

#### 10.3.1. Problem Formülasyonu

Bu deney, aile içi şiddet ve tutuklama zorunluluğu boyutlarını bütünleşik biçimde ele alan dört sınıflı bir sınıflandırma problemi olarak tasarlanmıştır. Illinois Zorunlu Tutuklama Yasası'nın hukuki çerçevesi, Sınıf 3'ü (aile içi şiddet + tutuklama) operasyonel açıdan kritik bir kategori konumuna getirmektedir. Bu sınıfın %2,9'luk temsil oranı karşısında model eğitiminin sınıf dengesizliğine duyarlı olması, 8,54 kat ters frekans ağırlığı uygulanarak sağlanmıştır.

```bash
docker compose exec spark-master spark-submit \
  --driver-memory 4g \
  /app/jobs/07_dispatch_protocol.py

# Beklenen çıktı:
# Total: 1,999,997 | Train (sampled): 601,694 | Test: 399,569
# Class 3 weight: 8.544x
# ── Training: RandomForestClassifier
#    accuracy=0.6416  f1=0.6817  recall_class3=0.700
# ✓ Best model: RandomForestClassifier (F1=0.6817)
```

#### 10.3.2. Model Sonuçları

| Model | F1 | Doğruluk | Recall Sınıf 3 |
|---|---|---|---|
| **Random Forest** | **0,682** | **%64,2** | **%70,0** |
| Decision Tree | 0,661 | %61,8 | %63,0 |
| Logistic Regression | 0,616 | %57,2 | %64,2 |
| MLP (Sinir Ağı) | 0,549 | %67,5 | %0,0 |
| Naive Bayes | 0,500 | %47,7 | %0,0 |

MLP ve Naive Bayes modellerinin Sınıf 3 duyarlılığının sıfır olması dikkat çekicidir. Bu modeller, ağırlıklandırmaya rağmen çoğunluk sınıfına (Sınıf 0, %67,8) yakınsayarak azınlık sınıflarını öğrenememiştir. Random Forest ise hem genel F1 metriğinde hem de kritik Sınıf 3 duyarlılığında en dengeli performansı sergilemiştir.

> #### 📸 EKRAN GÖRÜNTÜSÜ — MLflow Tüm Deneyler ve Exp03 Çalıştırmaları (Assignment Zorunluluğu)
> *`http://localhost:5001` — üç deneyin listelendiği MLflow ana ekranı ve Exp03 sınıf bazlı metrik karşılaştırması.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: mlflow_experiments_overview.png ]**

---

## 11. Adım 7 — Görselleştirme ve Dashboard

**Uygulama:** `dashboard/streamlit_app.py` | **Erişim:** `http://localhost:8501`

```bash
source .venv/bin/activate
streamlit run dashboard/streamlit_app.py
```

### 11.1. Dashboard Mimarisi

Dashboard, Python'un Streamlit çerçevesi üzerine inşa edilmiştir. Plotly kütüphanesi aracılığıyla üretilen tüm grafikler etkileşimlidir: kullanıcı grafik üzerinde yakınlaştırma, uzaklaştırma ve veri noktalarının üzerine gelme (hover) işlemleri gerçekleştirebilmektedir. Tüm görseller, önceden üretilmiş statik PNG dosyaları yerine, uygulama başlatıldığında veri dosyalarından anlık olarak hesaplanmaktadır. Bu mimari tercih, veri güncellendiğinde dashboard'un otomatik olarak güncel sonuçları yansıtmasını sağlamaktadır.

### 11.2. Sekme İçerikleri ve Zorunlu Görseller

Ödev tanımının Adım 7 için belirlediği zorunlu görsel gereksinimleri dört sekme arasında dağıtılmıştır:

| Sekme | Görseller |
|---|---|
| **EDA** | Saatlik suç alanı grafiği (zaman serisi), aylık trend çizgisi, haftanın günü çubuğu, top-10 suç tipi yatay çubuğu, tutuklama oranı halka diyagramı, gün×saat yoğunluk ısı haritası, Chicago suç konum scatter haritası |
| **Exp01** | 5 model ×  5 metrik gruplandırılmış çubuk, AUC-ROC sıralaması, duyarlılık karşılaştırması, ROC eğrisi, karışıklık matrisi ısı haritası, özellik önemi yatay çubuğu |
| **Exp02** | RMSE/MAE/R² üçlü panel, artık dağılımı histogramı, gerçek-tahmin saçılım grafiği, interaktif Chicago patrol haritası (scatter_mapbox), top-20 öncelikli bölge çubuğu |
| **Exp03** | 5 model karşılaştırma, sınıf bazlı duyarlılık gruplandırılmış çubuk, sınıf dağılımı halka diyagramı, F1 sıralaması, 4×4 karışıklık matrisi, özellik önemi çubuğu |

> #### 📸 EKRAN GÖRÜNTÜSÜ — Streamlit Dashboard (Assignment Zorunluluğu)
> *Dashboard'un genel görünümü — EDA sekmesi ve Exp01 model karşılaştırma sekmesi.*
> **[ BURAYA EKRAN GÖRÜNTÜSÜ EKLEYİN: streamlit_dashboard_overview.png ]**

---

## 12. Karşılaşılan Zorluklar ve Çözümler

Proje sürecinde karşılaşılan teknik güçlükler, benzer çalışmalara hazırlık açısından değer taşıyan pratik dersler sunmaktadır.

| # | Zorluk | Kök Neden | Uygulanan Çözüm |
|---|---|---|---|
| 1 | `GBTClassifier` çok sınıflı problemi reddediyor | Spark MLlib GBT yalnızca `{0, 1}` etiket kabul eder | `OneVsRest` sarmalayıcı veya `MultilayerPerceptronClassifier` alternatifi |
| 2 | `RandomForest` 2M satırda Java Heap OOM | 150 ağaç × depth-10 varsayılan bellek sınırını aşıyor | `numTrees` 150→50, `maxDepth` 10→6; Exp03 eğitim seti 600k örnekleme |
| 3 | `spark.driver.memory` kodu içinden etkisiz | JVM başlamadan önce ayarlanması gerekiyor | `--driver-memory 4g` doğrudan `spark-submit` komut satırına eklendi |
| 4 | `SupportsNonDeterministicExpression` hatası | Sınıf Spark 3.5.2 ile eklendi; konteyner 3.5.1 çalıştırıyor | Delta Lake 3.3.2 → 3.1.0'a indirildi (Spark 3.5.1 ile tam uyumlu) |
| 5 | MLflow HTTP izleme "Invalid Host header" 403 | Sunucunun DNS rebinding koruması, Spark konteynerinin isteğini reddediyor | HTTP → `file:///app/mlruns` paylaşımlı volume geçişi |
| 6 | Özellik vektöründe NaN; GBT/NaiveBayes çöküyor | %1,4 GPS eksikliği; VectorAssembler NaN'ı sıfır olarak doldurmıyor | `coalesce(lat_grid, lit(0.0))` + model eğitimi öncesi `.fillna(0)` |
| 7 | `dashboard/` Spark konteynerinde bulunamıyor | `docker-compose.yml`'de volume tanımlanmamış | `./dashboard:/app/dashboard` bind mount eklendi |
| 8 | Ivy cache izin hatası (`/home/spark/.ivy2`) | `spark` kullanıcısının ev dizininde yazma izni yok | Dockerfile'da `mkdir -p /home/spark/.ivy2 && chown -R spark:spark` eklendi |

---

## 13. Değerlendirme ve Sonuç

### 13.1. Deney Sonuçlarının Bütünsel Değerlendirmesi

Üç bağımsız makine öğrenmesi deneyi, Chicago suç verisinin farklı boyutlarına ilişkin anlamlı örüntüler barındırdığını ortaya koymuştur.

**Tutuklama Tahmini (Exp01):** GBT modelinin elde ettiği AUC-ROC=0,859 değeri, modelin rastgele tahminden belirgin biçimde ayrışan gerçek örüntüler öğrendiğini kanıtlamaktadır. Suç tipinin özellik önem skorundaki %73,2'lik payı beklenmedik bir bulgu olmakla birlikte, narkotik suçlarda uygulanan proaktif tutuklama politikasının veriye yansıması olarak yorumlanabilmektedir. Modelin tutuklama sınıfındaki düşük duyarlılığı (%41,2), sınıf dengesizliğinin sınıf ağırlıklandırmasına karşın tam anlamıyla giderilemediğini; daha ileri bir çalışmada SMOTE gibi sentetik örnekleme tekniklerinin denenmesi gerektiğini işaret etmektedir.

**Suç Yoğunluğu Regresyonu (Exp02):** R²=0,445 değeri, basit bir coğrafi ızgara ve zaman dilimi bilgisinden hareketle suç yoğunluğunun yaklaşık yarısının öngörülebilir olduğunu göstermektedir. Kalan varyansın önemli bir bölümünün spesifik etkinlikler, hava koşulları veya kısa vadeli sosyal dinamikler gibi modelde temsil edilmeyen değişkenlerden kaynaklandığı değerlendirilmektedir.

**Müdahale Protokolü (Exp03):** Sınıf 3 duyarlılığının %70,0 düzeyine ulaşılması, Illinois Zorunlu Tutuklama Yasası kapsamındaki vakaların büyük çoğunluğunun modelin öğrendiği özellik örüntüleriyle ilişkili olduğunu ve sistem tarafından tespit edilebildiğini göstermektedir. Bu bulgu, operasyonel karar destek uygulamaları açısından umut vericidir.

### 13.2. Ödev Gereksinimlerinin Karşılanması

| Değerlendirme Kriteri | Ağırlık | Durum |
|---|---|---|
| Docker & Altyapı | %15 | ✅ 6 servis, Dockerfile, JAR yönetimi |
| Kafka Streaming | %15 | ✅ 2M mesaj, 2.000 msg/sn, JSON format |
| Spark + Delta Lake | %15 | ✅ Bronze/Silver/Gold, ACID, streaming |
| EDA & Özellik Mühendisliği | %10 | ✅ 14 özellik, iş mantığı, Delta'ya kayıt |
| ML Modelleri & MLflow | %15 | ✅ 5 model × 3 deney, tüm metrikler |
| Dashboard & Görselleştirme | %15 | ✅ Streamlit, Plotly, 9+ zorunlu görsel |
| Dokümantasyon & Sunum | %15 | ✅ README, RUNBOOK, teknik rapor |

### 13.3. Sonuç

Bu çalışmada, 2.000.000 Chicago suç kaydı üzerinde Apache Kafka, Spark Structured Streaming, Delta Lake ve Spark MLlib bileşenlerini bütünleşik biçimde kullanan bir uçtan uca büyük veri pipeline'ı tasarlanmış ve başarıyla hayata geçirilmiştir. Üç makine öğrenmesi deneyi kapsamında eğitilen 15 model, MLflow ile sistematik olarak takip edilmiştir. En başarılı sonuçlar sırasıyla GBT Classifier (AUC-ROC=0,859), GBT Regressor (R²=0,445) ve Random Forest (Sınıf 3 Recall=%70,0) ile elde edilmiştir.

Çalışma, teorik veri mühendisliği bilgisinin endüstriyel standart araçlarla gerçek bir probleme uygulanmasını tüm süreç boyunca belgelemiş; karşılaşılan teknik güçlüklerin her biri çözümüyle birlikte kayıt altına alınmıştır. Bu raporun, gelecekteki benzer büyük veri projeleri için tekrarlanabilir bir referans kaynağı işlevi görmesi amaçlanmaktadır.

---

## Kaynaklar

1. Chicago Data Portal. *Crimes — 2001 to Present.* https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2 (Erişim: Mayıs 2026)

2. Armbrust, M., Das, T., Torres, J., Yavuz, B., Zhu, S., Xin, R., ... & Zaharia, M. (2020). Delta lake: High-performance ACID table storage over cloud object stores. *Proceedings of the VLDB Endowment*, 13(12), 3411-3424.

3. Armbrust, M., Xin, R. S., Lian, C., Huai, Y., Liu, D., Bradley, J. K., ... & Zaharia, M. (2018). Structured streaming: A declarative API for real-time applications in Apache Spark. *Proceedings of ACM SIGMOD*, 601-613.

4. Kreps, J., Narkhede, N., & Rao, J. (2011). Kafka: A distributed messaging system for log processing. *Proceedings of the NetDB Workshop*, 1-7.

5. Meng, X., Bradley, J., Yavuz, B., Sparks, E., Venkataraman, S., Liu, D., ... & Zaharia, M. (2016). MLlib: Machine learning in Apache Spark. *Journal of Machine Learning Research*, 17(1), 1235-1241.

6. Zaharia, M., Xin, R. S., Wendell, P., Das, T., Armbrust, M., Dave, A., ... & Stoica, I. (2016). Apache Spark: A unified engine for big data processing. *Communications of the ACM*, 59(11), 56-65.

7. Lum, K., & Isaac, W. (2016). To predict and serve? *Significance*, 13(5), 14-19.

8. Chen, A., Chow, A., Davidson, A., DCunha, A., Ghodsi, A., Hong, S. A., ... & Shankar, V. (2020). Developments in MLflow: A system to accelerate the machine learning lifecycle. *Proceedings of the DEEM Workshop*, 1-4.

9. Chicago Police Department. (2026). *IUCR Codes and Crime Classification.* City of Chicago official documentation.

---

*Teknik Rapor — Chicago Crime Big Data Pipeline Projesi*
*Büyük Veri Analizine Giriş, 2025-2026 Bahar Dönemi, 13 Mayıs 2026*
