# Crypto Workspace & Trade Replay Dashboard

Bu proje, Binance Spot ve Futures piyasalarından yüksek frekanslı `aggTrades` (gerçekleşen işlemler) verilerini çekip, bunları 1 saniyelik OHLCV mumlarına dönüştüren ve tıpkı bir video oynatıcı gibi saniye saniye izlemenizi sağlayan gelişmiş bir analiz aracıdır.

Sistem tamamen lokalinizde çalışan şık bir **Web Dashboard** üzerinden yönetilir.

## Özellikler

- **Merkezi Web Paneli (Dashboard):** Tüm işlemleri komut satırına kod yazmadan, tarayıcınız üzerinden görsel bir arayüzle yönetin.
- **FastAPI & DuckDB Altyapısı:** Ultra hızlı Parquet dosya okuma işlemleri ve bellek içi veri yönetimi sayesinde devasa tik verilerinde bile tarayıcıyı dondurmadan yüksek hızda işlem yapabilme.
- **Yüksek Hızlı Multi-Thread Veri Çekimi:** `ThreadPoolExecutor` kullanılarak zaman aralıkları parçalara (chunk) bölünür ve veriler CPU çekirdek sayınıza göre dinamik olarak paralel indirilir. Binance API limitleri otomatik gözetilir.
- **Kusursuz Hassasiyet:** Çekilen veriler mükerrer kayıtlardan arındırılır. Grafikler, yan paneller ve arka plan raporlamaları yüksek fiyat hassasiyetini destekler.
- **Trade Replay (İşlem Tekrarı):** Piyasada geçmişte yaşanmış devasa fiyat çöküşlerini veya ani yükselişleri saniye saniye canlı grafik, akan emir geçmişi ve alt kısımda senkronize **Hacim (Volume) Histogramı** eşliğinde izleyin.
  - O anki barın detaylı **OHLCV, Toplam İşlem Sayısı (Trades) ve Kümülatif Hacim Deltası (CVD)** verilerini görmek için mouse'unuzu grafikteki mumların üzerine getirmeniz yeterlidir.
  - Alt paneldeki menüden **CVD (Kümülatif Hacim Deltası)** göstergesi isteğe bağlı olarak tek tıkla açılıp kapatılabilir.
  - Gelişmiş tasarım sayesinde sert fiyat hareketlerinde fiyat barları ile hacim barları hiçbir zaman birbirinin içine geçmez, daima okunaklı kalır.
  - Video oynatıcı benzeri **İleri / Geri Sarma** butonları ve slider ile dilediğiniz zamana anında atlama.
  - Ayarlanabilir **Oynatma Hızı** (0.1x'den 1000x'e kadar) ve **Grafik FPS Kontrolü**.
- **Akıllı Veri Yönetimi:** İndirilen veriler "Oynatmaya Hazır Veriler" listesinde otomatik olarak belirir. İstediğiniz zaman geçmiş bir aralığı tek tıkla açabilirsiniz.

## Kurulum

Projeyi çalıştırmak için sisteminizde **Python 3.8+** kurulu olmalıdır.

1. Projeyi bilgisayarınıza indirin veya klonlayın.
2. Terminal veya Komut İstemini (CMD) açarak proje klasörünün içine girin.
3. Gerekli kütüphaneleri yüklemek için aşağıdaki komutu çalıştırın:

```bash
pip install -r requirements.txt
```

## Nasıl Çalıştırılır?

Tüm sistemi ayağa kaldırmak için tek yapmanız gereken ana sunucu dosyasını çalıştırmaktır:

```bash
python app.py
```

Bu komutu girdiğinizde arka planda lokal bir sunucu başlar ve varsayılan tarayıcınızda otomatik olarak **`http://localhost:8080`** adresi açılır.

### Arayüz Kullanımı

1. **Yeni Veri Çekme (Sol Panel):**
   - **İşlem Paritesi:** `BTCUSDT`, `ETHUSDT` gibi bir parite girin.
   - **Piyasa:** Spot veya Futures piyasasını seçin.
   - **Zaman Aralığı:** Takvimden UTC saatine göre bir başlangıç ve bitiş zamanı seçin.
   - *Verileri Çek* butonuna basın ve yükleme çubuğunun (progress bar) dolmasını bekleyin. Binance API'si limitlerine takılmamak için script otomatik olarak beklemeler (rate-limit handling) yapar.

2. **Verileri Oynatma (Sağ Panel):**
   - İndirme tamamlandığında sağ taraftaki **Oynatmaya Hazır Veriler** listesi güncellenir.
   - İzlemek istediğiniz paritenin yanındaki tam genişlikteki **Replay** butonuna tıklayarak işlem tekrarı sayfasını (Live Chart, OHLCV ve emir geçmişi ile birlikte) açabilirsiniz.

## Dosya Yapısı

- `app.py`: Ana FastAPI sunucusu, DuckDB veritabanı entegrasyonu ve API Rotaları.
- `dashboard.html`: Ana kontrol panelinin arayüz kodları.
- `replay_template.html`: Trade Replay oynatıcısının arayüzü, gelişmiş charting (LightweightCharts) ve animasyon mantığı.
- `fetch_spot_aggtrades.py` & `fetch_futures_aggtrades.py`: Binance API'sinden paralel (multithreading) veri çeken arka plan işleyicileri.

## Yasal Uyarı

Bu proje eğitim ve geçmiş piyasa hareketlerini analiz etme amacıyla geliştirilmiştir. Herhangi bir finansal tavsiye içermez. Binance API limitlerine dikkat ederek kullanılması önerilir.
