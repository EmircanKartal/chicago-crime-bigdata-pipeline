# Contributing

## Team

| Üye | GitHub |
|-----|--------|
| Emircan Kartal | [@EmircanKartal](https://github.com/EmircanKartal) |
| Meryem Berfin Kenar | [@berfinm](https://github.com/berfinm) |
| Kağan Gür | [@kagangur](https://github.com/kagangur) |

---

## Branch Strategy

Her özellik ayrı bir branch üzerinde geliştirilir; tamamlandığında `main` branch'e PR açılır.
`main` branch'e doğrudan commit yapılmaz.

```
main                    ← korumalı, yalnızca merge edilen PR'lar
feature/<konu>          ← yeni özellik geliştirme
fix/<konu>              ← hata düzeltme
docs/<konu>             ← dokümantasyon güncellemesi
```

---

## Commit Mesajı Formatı

```
type(scope): kısa açıklama
```

### Tip Listesi

| Tip | Ne Zaman Kullanılır |
|-----|---------------------|
| `feat` | Çalışan yeni bir özellik veya bileşen |
| `fix` | Hata düzeltme |
| `wip` | Tamamlanmamış, devam eden çalışma |
| `docs` | README, rapor, mimari notları |
| `chore` | Konfigürasyon, bağımlılıklar, klasör yapısı |
| `refactor` | Davranış değişmeden yapısal iyileştirme |
| `test` | Test ekleme veya düzenleme |

### Örnekler

```
feat(kafka): saniyede 2000 mesaj kapasiteli producer eklendi
feat(spark): kafka akışından bronze delta yazımı tamamlandı
feat(delta): silver temizleme job'ı — null ve duplike temizliği
feat(ml): 5 model × 3 deney MLflow entegrasyonuyla tamamlandı
fix(delta): silver job timestamp ayrıştırma formatı düzeltildi
fix(spark): feature vektöründe NaN değer sorunu giderildi
docs(readme): mimari diyagram ve runbook bağlantısı eklendi
chore(docker): Kafka ve Delta JAR'ları build-time imaja dahil edildi
```

### PR Mesajı Şablonu

```
## Yapılanlar
- Kısa madde listesi

## Test
- Nasıl test edildiği

## Ekran Görüntüsü (varsa)
```

---

## Git'e Eklenmeyecekler

Aşağıdaki dizin ve dosyalar `.gitignore` kapsamındadır; `git add -f` ile kesinlikle eklenmez:

```
data/          CSV dosyaları, ham indirmeler
delta/         Delta Lake tablo dosyaları
mlruns/        MLflow deney artifact'ları
.env           Ortam değişkenleri ve gizli bilgiler
__pycache__/
.ipynb_checkpoints/
*.pyc
.DS_Store
```

---

## PR Açmadan Önce Kontrol Listesi

- [ ] Kod yerel ortamda veya Docker içinde hatasız çalışıyor
- [ ] Önemli çıktılar için ekran görüntüsü alındı
- [ ] Her dosyanın başında ne yaptığını açıklayan yorum mevcut
- [ ] Branch üzerinde uygun formatta commit mesajıyla commit yapıldı
- [ ] PR açıklaması dolduruldu
