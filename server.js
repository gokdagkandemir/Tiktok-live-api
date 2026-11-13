/**
 * TikTok LIVE API
 * ----------------------------------------------------------------------------
 * Bir TikTok canlı yayınına bağlanır; hediye, sohbet, beğeni, takip, paylaşım,
 * izleyici girişi gibi olayları toplar ve şunları sunar:
 *   - REST API  : bağlantı yönetimi, istatistikler, son olaylar, hediye özeti
 *   - WebSocket : olayları gerçek zamanlı yayınlar  (ws://host:3000/ws)
 *   - SSE       : tarayıcıdan kolay dinleme        (/api/streams/:user/sse)
 *   - Webhook   : olayları kendi sunucuna POST eder (opsiyonel)
 *
 * Çalıştırma:  npm install && npm start
 */

const http = require('http');
const path = require('path');
const fs = require('fs');
const os = require('os');
const crypto = require('crypto');
const express = require('express');
const { WebSocketServer } = require('ws');
const { TikTokLiveConnection, WebcastEvent, ControlEvent } = require('tiktok-live-connector');

// Kalıcılık: olay arşivi (SQLite/JSONL), favoriler, elmas hedefleri
const store = require('./store');

// Proje klasöründeki .env dosyası (varsa) ortam değişkenlerine yüklenir; gerçek ortam değişkeni önceliklidir.
try {
  for (const line of fs.readFileSync(path.join(__dirname, '.env'), 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
    if (m && !(m[1] in process.env)) process.env[m[1]] = m[2].trim().replace(/^['"](.*)['"]$/, '$1');
  }
} catch { /* .env yok, sorun değil */ }

// ----------------------------------------------------------------------------
// Ayarlar (.env dosyasından ya da ortam değişkenlerinden)
// ----------------------------------------------------------------------------
const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '127.0.0.1';              // domain/LAN erişimi: HOST=0.0.0.0 (API_KEY ile birlikte önerilir)
// API_KEY tanımlı değilse açılışta rastgele üretilir ve data/apikey.json'a kaydedilir;
// böylece sunucu her yeniden başladığında aynı anahtar geçerli olur. .env'de değer varsa o önceliklidir.
const API_KEY_FROM_ENV = !!process.env.API_KEY;
const API_KEY = process.env.API_KEY || (() => {
  const f = path.join(store.DATA_DIR, 'apikey.json');
  try {
    const j = JSON.parse(fs.readFileSync(f, 'utf8'));
    if (j && typeof j.key === 'string' && j.key) return j.key;
  } catch { /* yok ya da bozuk: yenisi üretilir */ }
  const k = crypto.randomBytes(32).toString('base64url');
  try {
    fs.mkdirSync(store.DATA_DIR, { recursive: true });
    fs.writeFileSync(f, JSON.stringify({ key: k, createdAt: new Date().toISOString() }, null, 2));
  } catch (e) { console.error('API anahtarı kaydedilemedi:', e.message); }
  return k;
})();
const API_KEY_AUTO = !API_KEY_FROM_ENV;
const MAX_EVENTS = Number(process.env.MAX_EVENTS || 500);  // yayın başına bellekte tutulan olay sayısı
const SESSION_ID = process.env.TIKTOK_SESSIONID || '';     // opsiyonel (bazı yayınlar için gerekebilir)
const TT_TARGET_IDC = process.env.TIKTOK_TT_TARGET_IDC || '';
const EULER_API_KEY = process.env.EULER_API_KEY || '';     // opsiyonel (imza servisi anahtarı)
const WEBHOOK_URL = process.env.WEBHOOK_URL || '';         // opsiyonel: olaylar buraya POST edilir
const DISCORD_WEBHOOK_URL = process.env.DISCORD_WEBHOOK_URL || ''; // hedefe ulaşınca Discord bildirimi
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || '';  // hedefe ulaşınca Telegram bildirimi
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const ARCHIVE = process.env.ARCHIVE !== '0';               // olay ve örnekleri diske yaz (store.js)

// Takip edilecek olay türleri: API adı -> kütüphane olayı
const TRACKED = {
  chat: WebcastEvent.CHAT,
  gift: WebcastEvent.GIFT,
  like: WebcastEvent.LIKE,
  member: WebcastEvent.MEMBER,        // izleyici odaya girdi
  follow: WebcastEvent.FOLLOW,
  share: WebcastEvent.SHARE,
  roomUser: WebcastEvent.ROOM_USER,   // anlık izleyici sayısı
  emote: WebcastEvent.EMOTE,
  envelope: WebcastEvent.ENVELOPE,
  subscribe: WebcastEvent.SUB_NOTIFY,
  question: WebcastEvent.QUESTION_NEW,
  streamEnd: WebcastEvent.STREAM_END,
};

// ----------------------------------------------------------------------------
// Yardımcılar
// ----------------------------------------------------------------------------
/** "kullanici", "@kullanici" ya da "https://www.tiktok.com/@kullanici/live" girdisinden kullanıcı adını çıkarır. */
const clean = (u) => {
  let s = String(u || '').trim();
  const m = s.match(/@([A-Za-z0-9._]+)/);
  if (m) s = m[1];
  return s.replace(/^@/, '').replace(/[/?#\s].*$/, '').toLowerCase();
};

const toNum = (v) => {
  if (v === undefined || v === null) return 0;
  const n = typeof v === 'object' && typeof v.toString === 'function' ? Number(v.toString()) : Number(v);
  return Number.isFinite(n) ? n : 0;
};

const firstUrl = (img) => (img && Array.isArray(img.urlList) && img.urlList[0]) || null;

/** Protobuf kullanıcı nesnesini sade bir JSON'a çevirir. */
function normUser(u) {
  if (!u) return null;
  return {
    userId: String(u.id ?? u.userId ?? ''),
    uniqueId: u.displayId || u.uniqueId || '',
    nickname: u.nickname || '',
    secUid: u.secUid || '',
    profilePicture: firstUrl(u.avatarThumb) || firstUrl(u.avatarMedium) || null,
    verified: !!u.verified,
    isFollower: toNum(u.followInfo?.followStatus) > 0,
  };
}

/** Ham olayı API'nin kullandığı sade biçime çevirir. */
function normalize(type, d) {
  const base = { type, ts: Date.now() };
  switch (type) {
    case 'chat':
      return { ...base, user: normUser(d.user), comment: d.content || '' };

    case 'gift': {
      const g = d.gift || {};
      const info = d.extendedGiftInfo || {};
      const diamondEach = toNum(g.diamondCount ?? info.diamond_count);
      const repeatCount = toNum(d.repeatCount) || 1;
      const streakable = toNum(g.type ?? info.type) === 1; // seri gönderilebilen hediye
      const repeatEnd = !!d.repeatEnd;
      // Seri hediyelerde elmas yalnızca seri bittiğinde sayılır (çift saymayı önler).
      const countable = !streakable || repeatEnd;
      return {
        ...base,
        user: normUser(d.user),
        toUser: normUser(d.toUser),
        giftId: toNum(d.giftId),
        giftName: g.name || info.name || `gift_${toNum(d.giftId)}`,
        giftImage: firstUrl(g.image) || firstUrl(g.icon) || info.icon?.url_list?.[0] || null,
        diamondEach,
        repeatCount,
        repeatEnd,
        streakable,
        countable,
        diamonds: countable ? diamondEach * repeatCount : 0,
      };
    }

    case 'like':
      return { ...base, user: normUser(d.user), likeCount: toNum(d.count), totalLikes: toNum(d.total) };

    case 'member':
      return { ...base, user: normUser(d.user), memberCount: toNum(d.memberCount) };

    case 'follow':
    case 'share':
      return { ...base, user: normUser(d.user), action: toNum(d.action), shareType: toNum(d.shareType) };

    case 'roomUser':
      return { ...base, viewerCount: toNum(d.totalUser) || toNum(d.total), popularity: toNum(d.popularity) };

    case 'emote':
      return { ...base, user: normUser(d.user), emotes: d.emotes ?? d.emote ?? null };

    case 'envelope':
      return { ...base, raw: safeJson(d) };

    case 'subscribe':
      return { ...base, user: normUser(d.user), raw: safeJson(d) };

    case 'question':
      return { ...base, user: normUser(d.user), question: d.content || d.questionText || '' };

    case 'streamEnd':
      return { ...base };

    default:
      return { ...base, raw: safeJson(d) };
  }
}

/** Kütüphane hatasını anlaşılır Türkçe bir metne çevirir; fatal=true ise yeniden denemenin anlamı yoktur. */
function explainError(raw) {
  const m = String(raw);
  if (/business plan|requires a .*plan/i.test(m)) {
    return { fatal: true, text: 'İmza servisi (Euler Stream) bu işlem için ücretli plan / API anahtarı istiyor. README\'deki "İmza servisi" bölümüne bak.' };
  }
  if (/isn't online|useroffline|not.*live/i.test(m)) {
    return { fatal: false, text: 'Yayıncı şu an canlı yayında değil.' };
  }
  if (/room id/i.test(m)) {
    return { fatal: false, text: 'Yayın bulunamadı: kullanıcı adı yanlış olabilir ya da TikTok isteği engelledi.' };
  }
  return { fatal: false, text: m };
}

/** BigInt / döngüsel yapı içerebilecek nesneyi güvenle JSON'a çevirir. */
function safeJson(obj) {
  try {
    return JSON.parse(JSON.stringify(obj, (_, v) => (typeof v === 'bigint' ? v.toString() : v)));
  } catch {
    return null;
  }
}

// ----------------------------------------------------------------------------
// Yayın (stream) yönetimi
// ----------------------------------------------------------------------------
/** @type {Map<string, Stream>} */
const streams = new Map();
const subscribers = new Set(); // { kind:'ws'|'sse', send(fn), username, types:Set|null }

class Stream {
  constructor(username) {
    this.username = username;
    this.status = 'connecting'; // connecting | connected | disconnected | error | ended
    this.error = null;
    this.roomId = null;
    this.startedAt = Date.now();
    this.events = [];
    this.stats = {
      viewerCount: 0,
      totalLikes: 0,
      chatCount: 0,
      giftEvents: 0,
      totalDiamonds: 0,
      followCount: 0,
      shareCount: 0,
      joinCount: 0,
    };
    this.gifts = new Map();   // giftName -> { giftName, giftId, count, diamonds, image }
    this.gifters = new Map(); // userId -> { user, diamonds, giftCount }
    this.retry = 0;
    this.manualStop = false;
    this.conn = null;
    this.history = [];      // her 5 saniyede bir örneklenmiş zaman serisi (/history)
    this.sampler = null;
    this.avatar = null;     // yayıncı profil fotoğrafı (oda sahibinden ya da sayfadan)
    this.nickname = null;
    this.avatarBusy = false;
    this.goal = store.goals.get(username) || null; // elmas hedefi (data/goals.json)
    this.fatal = false;     // true: yeniden denemek işe yaramaz (örn. ücretli plan gerekiyor)
    this.seen = new Set();  // daha önce işlenen mesaj kimlikleri (bağlanırken gelen eski mesajlar tekrar gelir)
  }

  push(evt) {
    this.events.push(evt);
    if (this.events.length > MAX_EVENTS) this.events.shift();
    this.account(evt);
    if (ARCHIVE) store.addEvent(this.username, evt);
    broadcast(this.username, evt);
    postWebhook(this.username, evt);
  }

  account(e) {
    const s = this.stats;
    switch (e.type) {
      case 'chat': s.chatCount++; break;
      case 'like': s.totalLikes = e.totalLikes || s.totalLikes + e.likeCount; break;
      case 'follow': s.followCount++; break;
      case 'share': s.shareCount++; break;
      case 'member': s.joinCount++; break;
      case 'roomUser': s.viewerCount = e.viewerCount; break;
      case 'gift': {
        s.giftEvents++;
        if (!e.countable) break;
        s.totalDiamonds += e.diamonds;
        this.checkGoal();
        const gk = e.giftName;
        const row = this.gifts.get(gk) || { giftName: gk, giftId: e.giftId, image: e.giftImage, count: 0, diamonds: 0 };
        row.count += e.repeatCount;
        row.diamonds += e.diamonds;
        this.gifts.set(gk, row);
        if (e.user) {
          const uk = e.user.userId || e.user.uniqueId;
          const g = this.gifters.get(uk) || { user: e.user, diamonds: 0, giftCount: 0 };
          g.diamonds += e.diamonds;
          g.giftCount += e.repeatCount;
          this.gifters.set(uk, g);
        }
        break;
      }
    }
  }

  /** Aynı mesaj ikinci kez gelirse true döner (başlangıç verisi ile WebSocket aynı mesajları tekrarlar). */
  isDuplicate(name, data) {
    const id = data?.common?.msgId;
    if (id === undefined || id === null || String(id) === '0') return false;
    // Seri hediyelerde aynı kimlikle gelen güncellemeler farklı sayaç değeri taşıyabilir, onları ayırt et.
    let key = `${name}:${String(id)}`;
    if (name === 'gift') key += `:${toNum(data.repeatCount)}:${!!data.repeatEnd}`;
    if (this.seen.has(key)) return true;
    this.seen.add(key);
    if (this.seen.size > 3000) this.seen.delete(this.seen.values().next().value);
    return false;
  }

  sample() {
    const s = this.stats, ts = Date.now();
    this.history.push({ ts, viewers: s.viewerCount, likes: s.totalLikes, diamonds: s.totalDiamonds, chat: s.chatCount });
    if (this.history.length > 720) this.history.shift(); // en çok 1 saatlik geçmiş
    if (ARCHIVE) store.addSample(this.username, { ts, viewers: s.viewerCount, likes: s.totalLikes, diamonds: s.totalDiamonds, chat: s.chatCount });
  }

  startSampler() {
    if (!this.sampler) this.sampler = setInterval(() => this.sample(), 5000);
  }

  stopTimers() {
    clearInterval(this.sampler); this.sampler = null;
  }

  /** Bağlantının oda bilgisinden yayıncının adını ve profil fotoğrafını alır. */
  grabOwner(conn) {
    try {
      const owner = conn.roomInfo?.owner;
      if (!owner) return;
      this.avatar = firstUrl(owner.avatarThumb) || firstUrl(owner.avatarMedium) || this.avatar;
      this.nickname = owner.nickname || this.nickname;
    } catch { /* yoksay */ }
  }

  /** Oda bilgisinde fotoğraf yoksa yayıncı sayfasından avatar çeker (sessizce, en fazla tek seferde bir). */
  async tryFetchAvatar() {
    if (this.avatar || this.avatarBusy) return;
    this.avatarBusy = true;
    try {
      const r = await fetch(`https://www.tiktok.com/@${this.username}`, {
        headers: { 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36', 'accept-language': 'en-US,en;q=0.9' },
      });
      const html = await r.text();
      const m = html.match(/"avatarThumb":\s*\{\s*"urlList":\s*\[\s*"([^"]+)"/);
      if (m) {
        this.avatar = m[1].replace(/\\u002[fF]/g, '/');
        if (!this.nickname) this.nickname = html.match(/"nickname":"([^"]+)"/)?.[1] || this.nickname;
      }
    } catch { /* yoksay */ }
    this.avatarBusy = false;
  }

  summary() {
    return {
      username: this.username,
      status: this.status,
      error: this.error,
      fatal: this.fatal,
      roomId: this.roomId,
      startedAt: this.startedAt,
      uptimeSec: Math.round((Date.now() - this.startedAt) / 1000),
      avatar: this.avatar,
      nickname: this.nickname,
      goal: this.goal ? { target: this.goal.target, base: this.goal.base, reached: !!this.goal.reachedTs, total: this.stats.totalDiamonds } : null,
      stats: this.stats,
      bufferedEvents: this.events.length,
    };
  }

  async start() {
    this.startSampler();
    const options = {
      processInitialData: true,
      // Hediye listesini önceden çekmek imzalı (ücretli plan isteyen) bir istek gerektirir ve başarısız olursa
      // bağlantıyı tamamen iptal eder. Hediye adı/elmas bilgisi mesajın kendisinden okunur, bu yüzden kapalı.
      enableExtendedGiftInfo: process.env.GIFT_CATALOG === '1',
      fetchRoomInfoOnConnect: true,
    };
    if (SESSION_ID) {
      options.sessionId = SESSION_ID;
      if (TT_TARGET_IDC) options.ttTargetIdc = TT_TARGET_IDC;
    }
    if (EULER_API_KEY) options.signApiKey = EULER_API_KEY;

    const conn = new TikTokLiveConnection(this.username, options);
    this.conn = conn;

    for (const [name, evName] of Object.entries(TRACKED)) {
      conn.on(evName, (data) => {
        try {
          if (this.isDuplicate(name, data)) return;
          this.push(normalize(name, data));
        } catch (err) {
          console.error(`[${this.username}] ${name} işlenemedi:`, err.message);
        }
      });
    }

    conn.on(ControlEvent.CONNECTED, (state) => {
      this.status = 'connected';
      this.error = null;
      this.retry = 0;
      this.roomId = state?.roomId ?? conn.roomId ?? null;
      this.grabOwner(conn);
      if (!this.avatar) this.tryFetchAvatar();
      console.log(`[${this.username}] bağlandı (roomId=${this.roomId})`);
      broadcast(this.username, { type: 'status', status: 'connected', ts: Date.now() });
    });

    conn.on(ControlEvent.DISCONNECTED, () => {
      if (this.manualStop) return;
      this.status = 'disconnected';
      broadcast(this.username, { type: 'status', status: 'disconnected', ts: Date.now() });
      this.scheduleRetry();
    });

    conn.on(ControlEvent.ERROR, (err) => {
      console.error(`[${this.username}] hata:`, err?.info || err?.message || String(err));
    });

    conn.on(WebcastEvent.STREAM_END, () => {
      this.status = 'ended';
      this.manualStop = true; // yayın bitti, yeniden denemeye gerek yok
      this.stopTimers();
      broadcast(this.username, { type: 'status', status: 'ended', ts: Date.now() });
    });

    try {
      await conn.connect();
    } catch (err) {
      const raw = err?.message || String(err);
      const { text, fatal } = explainError(raw);
      this.status = 'error';
      this.error = text;
      this.fatal = fatal;
      console.error(`[${this.username}] bağlanılamadı (ham hata): ${raw}`);
      broadcast(this.username, { type: 'status', status: 'error', error: this.error, ts: Date.now() });
      if (!fatal) this.scheduleRetry();
    }
  }

  /** Elmas hedefine ulaşılırsa bir kez işaretler, olay yayınlar ve bildirim gönderir. */
  checkGoal() {
    const g = this.goal;
    if (!g || g.reachedTs) return;
    if (this.stats.totalDiamonds >= g.target) {
      g.reachedTs = Date.now();
      store.goals.set(this.username, g);
      this.push({ type: 'goal', ts: Date.now(), target: g.target, base: g.base, diamonds: this.stats.totalDiamonds });
      notifyGoal(this.username, g).catch(() => { /* yoksay */ });
    }
  }

  scheduleRetry() {
    // Yayıncı henüz canlı değilse sınırsız dener; bekleme süresi 3sn → 60sn arası artar.
    if (this.manualStop) return;
    const wait = Math.min(60000, 3000 * 2 ** Math.min(this.retry, 5));
    this.retry++;
    setTimeout(() => {
      if (!this.manualStop && streams.get(this.username) === this) this.start();
    }, wait);
  }

  stop() {
    this.manualStop = true;
    this.status = 'disconnected';
    this.stopTimers();
    try { this.conn?.disconnect(); } catch { /* yoksay */ }
  }
}

// ----------------------------------------------------------------------------
// Dağıtım: WebSocket / SSE / Webhook
// ----------------------------------------------------------------------------
function broadcast(username, evt) {
  for (const sub of subscribers) {
    if (sub.username && sub.username !== username) continue;
    if (sub.types && evt.type !== 'status' && !sub.types.has(evt.type)) continue;
    const full = { username, ...evt };
    try { sub.send(sub.fields ? pickFields(full, sub.fields) : full, evt.type); } catch { /* yoksay */ }
  }
}

async function postWebhook(username, evt) {
  if (!WEBHOOK_URL) return;
  try {
    await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json', ...(API_KEY ? { 'x-api-key': API_KEY } : {}) },
      body: JSON.stringify({ username, ...evt }),
    });
  } catch (err) {
    console.error('webhook hatası:', err.message);
  }
}

/** Elmas hedefine ulaşımı Discord/Telegram'a bildirir (tanımlıysa). */
async function notifyGoal(username, g) {
  const msg = `@${username} elmas hedefine ulaştı: ${g.target}`;
  const jobs = [];
  if (DISCORD_WEBHOOK_URL) {
    jobs.push(fetch(DISCORD_WEBHOOK_URL, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ content: msg }) }).catch(() => { /* yoksay */ }));
  }
  if (TELEGRAM_BOT_TOKEN && TELEGRAM_CHAT_ID) {
    jobs.push(fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: msg }) }).catch(() => { /* yoksay */ }));
  }
  await Promise.all(jobs);
}

/** "user.uniqueId,comment" biçimindeki alan listesini diziye çevirir. */
function parseFields(q) {
  if (!q) return null;
  const arr = String(q).split(',').map((s) => s.trim()).filter(Boolean);
  return arr.length ? arr : null;
}

/** Nesneden yalnızca istenen alanları (iç içe yollar dahil) alır: pickFields(e, ['user.uniqueId','comment']) */
function pickFields(obj, fields) {
  const out = {};
  for (const f of fields) {
    const parts = f.split('.');
    let src = obj, ok = true;
    for (const p of parts) {
      if (src && typeof src === 'object' && p in src) src = src[p]; else { ok = false; break; }
    }
    if (!ok) continue;
    let dst = out;
    parts.forEach((p, i) => { if (i === parts.length - 1) dst[p] = src; else dst = dst[p] = dst[p] || {}; });
  }
  return out;
}

const parseTypes = (q) => {
  if (!q) return null;
  const arr = String(q).split(',').map((s) => s.trim()).filter(Boolean);
  return arr.length ? new Set(arr) : null;
};

// ----------------------------------------------------------------------------
// Express
// ----------------------------------------------------------------------------
const app = express();
app.use(express.json());
app.use((req, res, next) => { // basit CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Headers', 'content-type,x-api-key');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,DELETE,OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// Yönetim paneli: index.html, "public" klasöründe ya da server.js ile aynı klasörde olabilir.
// Sayfanın kendisi açıktır, verileri API anahtarıyla çeker.
function findPanelFile() {
  return [path.join(__dirname, 'public', 'index.html'), path.join(__dirname, 'index.html')].find((f) => fs.existsSync(f)) || null;
}

app.get(['/', '/index.html'], (_req, res) => {
  const file = findPanelFile();
  if (file) return res.sendFile(file);
  res.status(404).type('text/plain; charset=utf-8').send(
    'Panel dosyası (index.html) bulunamadı.\n\n' +
    'index.html dosyasını server.js ile aynı klasöre ya da onun yanındaki "public" klasörüne koy,\n' +
    'sonra bu sayfayı yenile. (API çalışıyor: /api)'
  );
});

// OBS tarayıcı kaynağı için overlay sayfası (şeffaf arka plan; kullanıcı adı URL'den okunur)
app.get(['/overlay', '/overlay/:username'], (_req, res) => {
  const f = path.join(__dirname, 'overlay.html');
  if (fs.existsSync(f)) return res.sendFile(f);
  res.status(404).type('text/plain; charset=utf-8').send('overlay.html bulunamadı.');
});

const authOk = (req) => !API_KEY || req.get('x-api-key') === API_KEY || req.query.key === API_KEY;
app.use('/api', (req, res, next) => (authOk(req) ? next() : res.status(401).json({ error: 'Geçersiz veya eksik API anahtarı' })));

app.get('/api', (_req, res) => res.json({
  name: 'TikTok LIVE API',
  endpoints: {
    'POST /api/connect': '{ "username": "kullanici" }  yayına bağlan',
    'DELETE /api/connect/:username': 'bağlantıyı kapat',
    'GET /api/streams': 'bağlı yayınlar',
    'GET /api/streams/:username': 'durum + istatistik',
    'GET /api/streams/:username/events?type=gift,chat&limit=50&since=<ms>': 'son olaylar',
    'GET /api/streams/:username/gifts': 'hediye özeti',
    'GET /api/streams/:username/top-gifters?limit=10': 'en çok hediye gönderenler',
    'GET /api/favorites': 'favori yayınlar (sunucu açılışında otomatik bağlanır)',
    'POST /api/favorites': '{ "username": "kullanici" } favori ekle',
    'DELETE /api/favorites/:username': 'favoriden çıkar',
    'POST /api/goals/:username': '{ "target": 1000 } elmas hedefi (DELETE ile temizlenir)',
    'GET /api/archive/:username/events.csv': 'kayıtlı olaylar CSV (?from&to&type&limit)',
    'GET /api/archive/:username/summary.csv': 'gün gün özet CSV',
    'GET /api/archive/:username/gifters.csv': 'gifter toplamları CSV',
    'GET /api/streams/:username/sse?types=gift,chat': 'Server-Sent Events akışı',
    'WS  /ws?username=kullanici&types=gift,chat&key=API_KEY': 'WebSocket akışı',
    'GET /overlay/:username': 'OBS tarayıcı kaynağı overlay',
  },
  eventTypes: Object.keys(TRACKED),
}));

/** Bir yayına bağlanır (zaten bağlıysa mevcut olanı döner). Hem API hem komut ekranı kullanır. */
async function connectStream(rawUsername) {
  const username = clean(rawUsername);
  if (!username) throw new Error('username gerekli');
  let s = streams.get(username);
  if (s && !s.manualStop) return { stream: s, already: true };
  s = new Stream(username);
  streams.set(username, s);
  await s.start(); // ilk bağlantı sonucu beklenir
  return { stream: s, already: false };
}

app.post('/api/connect', async (req, res) => {
  try {
    const { stream, already } = await connectStream(req.body?.username);
    res.status(stream.status === 'error' ? 502 : 200).json({ ok: stream.status !== 'error', already, stream: stream.summary() });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

app.get('/api/connect/:username', async (req, res) => { // tarayıcıdan kolay test
  try {
    const { stream, already } = await connectStream(req.params.username);
    res.status(stream.status === 'error' ? 502 : 200).json({ ok: stream.status !== 'error', already, stream: stream.summary() });
  } catch (err) {
    res.status(400).json({ ok: false, error: err.message });
  }
});

app.delete('/api/connect/:username', (req, res) => {
  const s = streams.get(clean(req.params.username));
  if (!s) return res.status(404).json({ error: 'Yayın bulunamadı' });
  s.stop();
  streams.delete(s.username);
  res.json({ ok: true });
});

app.get('/api/streams', (_req, res) => res.json([...streams.values()].map((s) => s.summary())));

const getStream = (req, res) => {
  const s = streams.get(clean(req.params.username));
  if (!s) { res.status(404).json({ error: 'Yayın bulunamadı, önce /api/connect çağırın' }); return null; }
  return s;
};

app.get('/api/streams/:username', (req, res) => {
  const s = getStream(req, res); if (!s) return;
  const fields = parseFields(req.query.fields); // örn. ?fields=stats.totalDiamonds,status
  res.json(fields ? pickFields(s.summary(), fields) : s.summary());
});

app.get('/api/streams/:username/history', (req, res) => {
  const s = getStream(req, res); if (s) res.json(s.history);
});

app.get('/api/streams/:username/events', (req, res) => {
  const s = getStream(req, res); if (!s) return;
  const types = parseTypes(req.query.type);
  const since = Number(req.query.since || 0);
  const limit = Math.min(Number(req.query.limit || 50), MAX_EVENTS);
  let list = s.events;
  if (types) list = list.filter((e) => types.has(e.type));
  if (since) list = list.filter((e) => e.ts > since);
  const out = list.slice(-limit);
  const fields = parseFields(req.query.fields); // örn. ?fields=user.uniqueId,comment → yalnızca bu alanlar döner
  res.json(fields ? out.map((e) => pickFields(e, fields)) : out);
});

app.get('/api/streams/:username/gifts', (req, res) => {
  const s = getStream(req, res); if (!s) return;
  const gifts = [...s.gifts.values()].sort((a, b) => b.diamonds - a.diamonds);
  res.json({ totalDiamonds: s.stats.totalDiamonds, gifts });
});

app.get('/api/streams/:username/top-gifters', (req, res) => {
  const s = getStream(req, res); if (!s) return;
  const limit = Math.min(Number(req.query.limit || 10), 100);
  res.json([...s.gifters.values()].sort((a, b) => b.diamonds - a.diamonds).slice(0, limit));
});

// ---------------------------------------------------------------- favoriler (açılışta otomatik bağlanılır)
app.get('/api/favorites', (_req, res) => res.json(store.favorites.list()));

app.post('/api/favorites', (req, res) => {
  const u = clean(req.body?.username);
  if (!u) return res.status(400).json({ error: 'username gerekli' });
  store.favorites.add(u);
  res.json({ ok: true, favorites: store.favorites.list() });
});

app.delete('/api/favorites/:username', (req, res) => {
  store.favorites.remove(clean(req.params.username));
  res.json({ ok: true, favorites: store.favorites.list() });
});

// ---------------------------------------------------------------- elmas hedefi
const goalOut = (g, s) => ({ target: g.target, base: g.base, reached: !!g.reachedTs, total: s ? s.stats.totalDiamonds : g.base });

app.get('/api/goals/:username', (req, res) => {
  const u = clean(req.params.username);
  const g = store.goals.get(u);
  res.json(g ? goalOut(g, streams.get(u)) : null);
});

app.post('/api/goals/:username', (req, res) => {
  const u = clean(req.params.username);
  if (!u) return res.status(400).json({ error: 'username gerekli' });
  const s = streams.get(u);
  const base = s ? s.stats.totalDiamonds : 0;
  const target = Number(req.body?.target);
  if (!Number.isFinite(target) || Math.round(target) <= base) {
    return res.status(400).json({ error: `hedef, mevcut toplamın (${base}) üstünde bir sayı olmalı` });
  }
  const g = store.goals.set(u, { target: Math.round(target), base, reachedTs: null });
  if (s) s.goal = g;
  res.json({ ok: true, goal: goalOut(g, s) });
});

app.delete('/api/goals/:username', (req, res) => {
  const u = clean(req.params.username);
  store.goals.clear(u);
  const s = streams.get(u);
  if (s) s.goal = null;
  res.json({ ok: true });
});

// ---------------------------------------------------------------- CSV arşivi (SQLite/JSONL üzerinden)
const CSV_SEP = process.env.CSV_SEP || ';';
const csvCell = (v) => {
  const s = String(v ?? '');
  return (s.includes('"') || s.includes('\n') || s.includes('\r') || s.includes(CSV_SEP)) ? '"' + s.replace(/"/g, '""') + '"' : s;
};
const csvRow = (cells) => cells.map(csvCell).join(CSV_SEP) + '\r\n';
const fmtTs = (ts) => new Date(ts).toLocaleString('sv-SE').replace('T', ' ');
const download = (res, name, text) => {
  res.setHeader('content-type', 'text/csv; charset=utf-8');
  res.setHeader('content-disposition', `attachment; filename="${name}"`);
  res.send('\uFEFF' + text);
};

app.get('/api/archive/:username/events.csv', (req, res) => {
  const u = clean(req.params.username);
  const types = parseTypes(req.query.type);
  const rows = [csvRow(['saat', 'tur', 'kullanici', 'nick', 'metin', 'hediye', 'adet', 'elmas', 'gorsel'])];
  for (const e of store.readEvents(u, {
    from: Number(req.query.from || 0),
    to: Number(req.query.to || 0) || undefined,
    types: types ? [...types] : null,
    limit: Math.min(Number(req.query.limit || 50000), 200000),
  })) {
    rows.push(csvRow([fmtTs(e.ts), e.type, e.user?.uniqueId || '', e.user?.nickname || '', e.comment || '', e.giftName || '', e.repeatCount || '', e.diamonds || '', e.image || e.giftImage || '']));
  }
  download(res, `${u}-olaylar.csv`, rows.join(''));
});

app.get('/api/archive/:username/summary.csv', (req, res) => {
  const u = clean(req.params.username);
  const from = Number(req.query.from || 0);
  const to = Number(req.query.to || 0) || undefined;
  const days = new Map();
  const dayOf = (ts) => new Date(ts).toLocaleDateString('sv-SE');
  const get = (d) => {
    let o = days.get(d);
    if (!o) { o = { date: d, maxViewers: 0, endViewers: 0, likes: 0, diamonds: 0, giftCount: 0, chat: 0, follow: 0, share: 0, join: 0 }; days.set(d, o); }
    return o;
  };
  let prevLikes = null;
  for (const s of store.readSamples(u, { from, to })) {
    const o = get(dayOf(s.ts));
    o.maxViewers = Math.max(o.maxViewers, s.viewers || 0);
    o.endViewers = s.viewers || 0;
    if (prevLikes !== null && s.likes > prevLikes) o.likes += s.likes - prevLikes;
    prevLikes = s.likes || 0;
  }
  for (const e of store.readEvents(u, { from, to })) {
    const o = get(dayOf(e.ts));
    if (e.type === 'gift') { if (e.countable) { o.diamonds += e.diamonds || 0; o.giftCount += e.repeatCount || 1; } }
    else if (e.type === 'chat') o.chat++;
    else if (e.type === 'follow') o.follow++;
    else if (e.type === 'share') o.share++;
    else if (e.type === 'member') o.join++;
  }
  const rows = [csvRow(['tarih', 'max_izleyici', 'son_izleyici', 'elmas', 'hediye_adedi', 'mesaj', 'begeni', 'takip', 'paylasim', 'giris'])];
  [...days.values()].sort((a, b) => a.date.localeCompare(b.date))
    .forEach((o) => rows.push(csvRow([o.date, o.maxViewers, o.endViewers, o.diamonds, o.giftCount, o.chat, o.likes, o.follow, o.share, o.join])));
  download(res, `${u}-ozet.csv`, rows.join(''));
});

app.get('/api/archive/:username/gifters.csv', (req, res) => {
  const u = clean(req.params.username);
  const map = new Map();
  for (const e of store.readEvents(u, { types: ['gift'] })) {
    if (!e.countable) continue;
    const id = e.user?.userId || e.user?.uniqueId || '?';
    const g = map.get(id) || { user: e.user?.uniqueId || id, nickname: '', diamonds: 0, count: 0, last: 0 };
    g.nickname = e.user?.nickname || g.nickname;
    g.diamonds += e.diamonds || 0;
    g.count += e.repeatCount || 1;
    g.last = Math.max(g.last, e.ts);
    map.set(id, g);
  }
  const rows = [csvRow(['kullanici', 'nick', 'elmas', 'hediye_adedi', 'son_hediye'])];
  [...map.values()].sort((a, b) => b.diamonds - a.diamonds)
    .forEach((g) => rows.push(csvRow([g.user, g.nickname, g.diamonds, g.count, g.last ? fmtTs(g.last) : ''])));
  download(res, `${u}-gifterlar.csv`, rows.join(''));
});

app.get('/api/streams/:username/sse', (req, res) => {
  const s = getStream(req, res); if (!s) return;
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });
  res.write(`event: hello\ndata: ${JSON.stringify(s.summary())}\n\n`);
  const sub = {
    username: s.username,
    types: parseTypes(req.query.types),
    fields: parseFields(req.query.fields),
    send: (out, type) => res.write(`event: ${type}\ndata: ${JSON.stringify(out)}\n\n`),
  };
  subscribers.add(sub);
  const ping = setInterval(() => res.write(': ping\n\n'), 25000);
  req.on('close', () => { clearInterval(ping); subscribers.delete(sub); });
});

// ----------------------------------------------------------------------------
// HTTP + WebSocket sunucusu
// ----------------------------------------------------------------------------
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
// ws, http sunucusunun hatalarını (örn. port dolu) buraya da yansıtır; dinleyici yoksa program çöker.
// Asıl hata start() içinde ele alınır.
wss.on('error', () => {});

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const key = url.searchParams.get('key') || req.headers['x-api-key'];
  if (API_KEY && key !== API_KEY) {
    ws.close(4401, 'Geçersiz API anahtarı');
    return;
  }
  const username = clean(url.searchParams.get('username')) || null;
  const sub = {
    username,
    types: parseTypes(url.searchParams.get('types')),
    fields: parseFields(url.searchParams.get('fields')),
    send: (out) => ws.readyState === ws.OPEN && ws.send(JSON.stringify(out)),
  };
  subscribers.add(sub);
  ws.send(JSON.stringify({ type: 'hello', watching: username || 'all', streams: [...streams.keys()] }));

  ws.on('message', (raw) => { // istemci filtreyi sonradan değiştirebilir: {"types":["gift"]}
    try {
      const msg = JSON.parse(raw.toString());
      if (Array.isArray(msg.types)) sub.types = msg.types.length ? new Set(msg.types) : null;
      if (Array.isArray(msg.fields)) sub.fields = msg.fields.length ? msg.fields : null;
      if (typeof msg.username === 'string') sub.username = clean(msg.username) || null;
    } catch { /* yoksay */ }
  });
  ws.on('close', () => subscribers.delete(sub));
});

/** Sunucuyu açar. Port doluysa (örn. eski bir pencere açık kaldıysa) sıradaki boş portu dener; gerçek portu döner. */
function start() {
  return new Promise((resolve, reject) => {
    let port = PORT, tries = 0;
    const attempt = () => {
      const onListening = () => {
        server.off('error', onError);
        resolve(port);
        // Favorilere açılışta otomatik bağlan (AUTO_CONNECT=0 ile kapatılır)
        if (process.env.AUTO_CONNECT !== '0') {
          for (const u of store.favorites.list()) connectStream(u).catch(() => { /* Stream kendi yeniden dener */ });
        }
      };
      const onError = (err) => {
        server.off('listening', onListening);
        if (err.code === 'EADDRINUSE' && tries < 10) { tries++; port++; attempt(); }
        else reject(err);
      };
      server.once('listening', onListening);
      server.once('error', onError);
      server.listen(port, HOST);
    };
    attempt();
  });
}

// Doğrudan "node server.js" ile çalıştırılırsa (komut ekranı olmadan)
const localUrls = (port) => {
  const list = [`http://localhost:${port}`];
  if (HOST === '0.0.0.0') {
    for (const addrs of Object.values(os.networkInterfaces()))
      for (const a of addrs) if (a.family === 'IPv4' && !a.internal) list.push(`http://${a.address}:${port}`);
  }
  return list;
};

if (require.main === module) {
  start()
    .then((port) => {
      console.log(`TikTok LIVE API hazır → ${localUrls(port).join('  |  ')}  (panel: ilk adres, WebSocket: /ws)`);
      console.log(`API anahtarı: ${API_KEY}${API_KEY_AUTO ? '   (otomatik üretildi → data/apikey.json; .env\'e API_KEY=... yazarsan onu kullanır)' : '   (.env / ortam değişkeninden)'}`);
      if (HOST === '0.0.0.0') console.log('Sunucu tüm ağ arayüzlerinde dinliyor; domain/reverse proxy ile dışarıya açılabilir (bkz. .env.ornek).');
    })
    .catch((err) => { console.error('Sunucu başlatılamadı:', err.message); process.exit(1); });
}

module.exports = { start, connectStream, streams, subscribers, clean, Stream, findPanelFile, PORT, HOST, API_KEY, API_KEY_AUTO };

process.on('SIGINT', () => { for (const s of streams.values()) s.stop(); process.exit(0); });
process.on('unhandledRejection', (e) => console.error('unhandledRejection:', e?.message || e));