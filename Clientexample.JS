// Örnek istemci: WebSocket ile canlı olayları dinler.
// Kullanım:  node client-example.js kullanici_adi
const WebSocket = require('ws');

const username = process.argv[2] || 'tiktok';
const BASE = process.env.API_URL || 'http://localhost:3000';
const KEY = process.env.API_KEY || '';
const headers = { 'content-type': 'application/json', ...(KEY ? { 'x-api-key': KEY } : {}) };

(async () => {
  // 1) Yayına bağlan
  const r = await fetch(`${BASE}/api/connect`, { method: 'POST', headers, body: JSON.stringify({ username }) });
  console.log('connect:', JSON.stringify(await r.json()));

  // 2) Olayları canlı dinle
  const ws = new WebSocket(
    `${BASE.replace('http', 'ws')}/ws?username=${username}&types=gift,chat,like,follow,share${KEY ? `&key=${KEY}` : ''}`
  );

  ws.on('message', (raw) => {
    const e = JSON.parse(raw);
    const who = e.user?.uniqueId ? `@${e.user.uniqueId}` : '';
    switch (e.type) {
      case 'gift':
        if (e.countable) console.log(`🎁 ${who} → ${e.giftName} x${e.repeatCount} (${e.diamonds} elmas)`);
        break;
      case 'chat':   console.log(`💬 ${who}: ${e.comment}`); break;
      case 'like':   console.log(`❤️  ${who} +${e.likeCount} (toplam ${e.totalLikes})`); break;
      case 'follow': console.log(`➕ ${who} takip etti`); break;
      case 'share':  console.log(`🔁 ${who} paylaştı`); break;
      case 'status': console.log(`ℹ️  durum: ${e.status}`); break;
    }
  });

  ws.on('close', () => console.log('WebSocket kapandı'));
})();