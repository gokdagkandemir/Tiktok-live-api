/**
 * Kalıcılık katmanı: olay arşivi, favoriler ve elmas hedefleri.
 * - Olay arşivi için Node.js'in gömülü "node:sqlite" modülü kullanılır (ek bağımlılık yok).
 *   Kullanılamazsa (eski Node) veriler data/archive/ altında JSONL dosyalarına yazılır.
 * - Favoriler data/favorites.json, hedefler data/goals.json dosyasında saklanır.
 * - ARCHIVE=0 ile arşiv tamamen kapatılabilir; DATA_DIR ile klasör değiştirilebilir.
 */
const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
fs.mkdirSync(DATA_DIR, { recursive: true });

// Arşive yazılacak olay türleri (roomUser hariç; örnekleyici zaten istatistiği tutar)
const ARCHIVE_TYPES = new Set(['chat', 'gift', 'like', 'member', 'follow', 'share', 'emote', 'envelope', 'subscribe', 'question', 'streamEnd', 'goal']);

let db = null, insEvent = null, insSample = null;
try {
  if (process.env.ARCHIVE !== '0') {
    const { DatabaseSync } = require('node:sqlite');
    db = new DatabaseSync(path.join(DATA_DIR, 'archive.db'));
    db.exec('PRAGMA journal_mode = WAL;');
    db.exec(`CREATE TABLE IF NOT EXISTS events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL, ts INTEGER NOT NULL, type TEXT NOT NULL,
      user_id TEXT, unique_id TEXT, nickname TEXT, comment TEXT,
      gift_id INTEGER, gift_name TEXT, repeat_count INTEGER, diamonds INTEGER, image TEXT,
      extra TEXT
    )`);
    db.exec('CREATE INDEX IF NOT EXISTS ix_events_u_ts ON events(username, ts)');
    db.exec(`CREATE TABLE IF NOT EXISTS samples (
      username TEXT NOT NULL, ts INTEGER NOT NULL,
      viewers INTEGER, likes INTEGER, diamonds INTEGER, chat INTEGER,
      PRIMARY KEY (username, ts)
    )`);
    insEvent = db.prepare('INSERT INTO events (username, ts, type, user_id, unique_id, nickname, comment, gift_id, gift_name, repeat_count, diamonds, image, extra) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)');
    insSample = db.prepare('INSERT OR REPLACE INTO samples (username, ts, viewers, likes, diamonds, chat) VALUES (?, ?, ?, ?, ?, ?)');
  }
} catch (err) {
  db = null;
  console.warn(`Arşiv (SQLite) açılamadı: ${err.message} — JSONL moduna geçiliyor.`);
}

let lastErr = 0;
const quiet = (err) => {
  const now = Date.now();
  if (now - lastErr > 60000) { lastErr = now; console.error('arşiv yazma hatası:', err.message); }
};

const jsonlEvents = (u) => path.join(DATA_DIR, 'archive', `${u}.jsonl`);
const jsonlSamples = (u) => path.join(DATA_DIR, 'archive', `${u}.samples.jsonl`);

function addEvent(username, evt) {
  if (!ARCHIVE_TYPES.has(evt.type)) return;
  try {
    const u = evt.user || {};
    if (db) {
      insEvent.run(
        username, evt.ts, evt.type,
        u.userId || null, u.uniqueId || null, u.nickname || null,
        evt.comment ?? evt.question ?? null,
        evt.giftId ?? null, evt.giftName ?? null, evt.repeatCount ?? null, evt.diamonds ?? null, evt.giftImage ?? null,
        JSON.stringify({ ...evt, user: u.uniqueId || null }),
      );
    } else {
      fs.mkdirSync(path.join(DATA_DIR, 'archive'), { recursive: true });
      fs.appendFileSync(jsonlEvents(username), JSON.stringify(evt) + '\n');
    }
  } catch (err) { quiet(err); }
}

function addSample(username, s) {
  try {
    if (db) {
      insSample.run(username, s.ts, s.viewers || 0, s.likes || 0, s.diamonds || 0, s.chat || 0);
    } else {
      fs.mkdirSync(path.join(DATA_DIR, 'archive'), { recursive: true });
      fs.appendFileSync(jsonlSamples(username), JSON.stringify(s) + '\n');
    }
  } catch (err) { quiet(err); }
}

function rowToEvent(r) {
  let extra = {};
  try { extra = r.extra ? JSON.parse(r.extra) : {}; } catch { /* yoksay */ }
  return {
    ts: r.ts, type: r.type,
    user: { userId: r.user_id || '', uniqueId: r.unique_id || '', nickname: r.nickname || '' },
    comment: r.comment || '', giftId: r.gift_id || 0, giftName: r.gift_name || '',
    repeatCount: r.repeat_count || 1, diamonds: r.diamonds || 0, image: r.image || '',
    countable: extra.countable !== false, repeatEnd: extra.repeatEnd,
  };
}

function readEvents(username, opt = {}) {
  const from = opt.from || 0;
  const to = opt.to || Number.MAX_SAFE_INTEGER;
  const limit = Math.min(opt.limit || 50000, 200000);
  if (db) {
    let sql = 'SELECT * FROM events WHERE username = ? AND ts >= ? AND ts <= ?';
    const params = [username, from, to];
    if (opt.types && opt.types.length) {
      sql += ` AND type IN (${opt.types.map(() => '?').join(',')})`;
      params.push(...opt.types);
    }
    sql += ' ORDER BY ts ASC LIMIT ?';
    params.push(limit);
    return db.prepare(sql).all(...params).map(rowToEvent);
  }
  try {
    return fs.readFileSync(jsonlEvents(username), 'utf8').split('\n').filter(Boolean)
      .map((l) => { try { return JSON.parse(l); } catch { return null; } })
      .filter(Boolean)
      .filter((e) => e.ts >= from && e.ts <= to && (!opt.types || opt.types.includes(e.type)))
      .slice(-limit);
  } catch { return []; }
}

function readSamples(username, opt = {}) {
  const from = opt.from || 0;
  const to = opt.to || Number.MAX_SAFE_INTEGER;
  const limit = Math.min(opt.limit || 100000, 500000);
  if (db) {
    return db.prepare('SELECT ts, viewers, likes, diamonds, chat FROM samples WHERE username = ? AND ts >= ? AND ts <= ? ORDER BY ts ASC LIMIT ?')
      .all(username, from, to, limit);
  }
  try {
    return fs.readFileSync(jsonlSamples(username), 'utf8').split('\n').filter(Boolean)
      .map((l) => { try { return JSON.parse(l); } catch { return null; } })
      .filter(Boolean)
      .filter((s) => s.ts >= from && s.ts <= to)
      .slice(-limit);
  } catch { return []; }
}

// ---------------------------------------------------------------- favoriler
const readJson = (f, d) => { try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch { return d; } };
const favFile = path.join(DATA_DIR, 'favorites.json');
let favorites = readJson(favFile, []);
if (!Array.isArray(favorites)) favorites = [];
const saveFav = () => { try { fs.writeFileSync(favFile, JSON.stringify(favorites, null, 2)); } catch (err) { quiet(err); } };

// ---------------------------------------------------------------- hedefler
const goalFile = path.join(DATA_DIR, 'goals.json');
let goals = readJson(goalFile, {});
if (!goals || typeof goals !== 'object' || Array.isArray(goals)) goals = {};
const saveGoals = () => { try { fs.writeFileSync(goalFile, JSON.stringify(goals, null, 2)); } catch (err) { quiet(err); } };

module.exports = {
  DATA_DIR,
  addEvent, addSample, readEvents, readSamples,
  favorites: {
    list: () => [...favorites],
    add: (u) => { if (!favorites.includes(u)) { favorites.push(u); saveFav(); } },
    remove: (u) => { favorites = favorites.filter((x) => x !== u); saveFav(); },
  },
  goals: {
    get: (u) => goals[u] || null,
    set: (u, g) => { goals[u] = g; saveGoals(); return g; },
    clear: (u) => { delete goals[u]; saveGoals(); },
  },
};
