# TikTok LIVE API

## Overview
TikTok CANLI yayınına bağlanır; hediye, sohbet, beğeni, takip, paylaşım, izleyici girişi gibi olayları toplar ve şunları sunar:
- **REST API**: bağlantı yönetimi, istatistikler, son olaylar, hediye özeti
- **WebSocket**: olayları gerçek zamanlı yayınlar (ws://host:3000/ws)
- **SSE**: tarayıcıdan kolay dinleme (/api/streams/:user/sse)
- **Webhook**: olayları kendi sunucuna POST eder (opsiyonel)

## Quick Start

### Option 1: Double-click .bat (En Kolay)
`baslat.b` dosyasına çift tıklayın veya çalıştırın.
- Node.js kontrol edilir, paketler kuruluyor (ilk kez)
- Sunucu başlatılır (varsayılanly port 3000)
- Tarayıcı otomatik olarak açılır (`http://localhost:3000`)
- Panelde kullanıcı adı girerek canlı yayına bağlanabilirsiniz

### Option 2: Manual Node.js Execution
```bash
npm install
node server.js
# veya
npm start
```

### Option 3: Via CLI
```bash
node cli.js [kullanıcı_adı]
```

## .exe Oluşturma (İsteğe bağlı)
Önceki `pkg` araçları Node.js v22 ile uyumsuzluk due to `node:sqlite` modülünden dolayı çalışmıyor. Eğer .exe istiyorsanız:

1. Node.js v18 veya v14'a düş grade
2. Ya da `nexe` için `--build` flag'i ile kaynak derlemen gerekir
   ```
   nexe --build -t node18-win-x64 -o tiktok-live-api.exe server.js
   ```

## Panel Kullanımı
- **Bağlan**: Kullanıcı adı girerek canlıYANA başlat
- **Favoriler**: Sağ panelden favori kaydet/çıkar
- **Hedef**: Elmas hedefi belirle, ulaşıldığında bildirim gönder
- **OBS Overlay**: `/overlay/:username` endpoints'i ile tarayıcı overlay
- **CSV İndirme**: Arşiv sayfaysından olay/özet/gifter CSV'leri

## API Endpoints (Özet)
| Endpoint | Açıklama |
|---|---|
| `GET /` | Panel sayfası |
| `GET /api` | API dokumentasyonu |
| `POST /api/connect` | Bağlantı başlat |
| `DELETE /api/connect/:username` | Bağlantıyı kapat |
| `GET /api/streams` | Bağlı yayınlar |
| `GET /api/streams/:username` | Durum + istatistik |
| `GET /api/streams/:username/events` | Son olaylar |
| `GET /api/streams/:username/sse` | Server-Sent Events |
| `WS /ws` | WebSocket akışı |

## Ortam Değişkenleri (.env)
| Değişken | Varsayılan | Açıklama |
|---|---|---|
| `PORT` | 3000 | HTTP + WebSocket portu |
| `HOST` | 127.0.0.1 | Dinleyici host (0.0.0.0 = dış erişim) |
| `API_KEY` | otomatik | API kimlik doğrulama anahtarı |
| `MAX_EVENTS` | 500 | Yayın başına olay sayısı |
| `TIKTOK_SESSIONID` | — | Oturum çerezi (bazı yayınlar için) |
| `EULER_API_KEY` | — | İmza servisi anahtarı |
| `WEBHOOK_URL` | — | Olay POST'u adresi |
| `DISCORD_WEBHOOK_URL` | — | Elmas hedefi Discord bildirimi |
| `TELEGRAM_BOT_TOKEN` | — | Telegram bildirimi |
| `TELEGRAM_CHAT_ID` | — | Telegram chat kimliği |
| `AUTO_CONNECT` | 1 | Açılışta favorilere otomatik bağlan |
| `ARCHIVE` | 1 | Olay/örnek kalıcı kaydetme |
| `DATA_DIR` | data | SQLite/JSONL arşiv klasörü |
| `CSV_SEP` | ; | CSV ayırıcı |
| `GIFT_CATALOG` | 0 | Hediye kataloğu önceden çekme |

## GitHub Dağıtımı
- `baslat.bat` ve `tiktok-live-api.exe` (isteğe bağlı) GitHub Releases'a yüklenebilir
- İndirenler `.bat` çalıştırabilir, tarayıcı otomatik açılır
- Ekstra Node.js kurulumu gerekmez

## Lisans
ISC