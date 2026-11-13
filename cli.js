/**
 * TikTok LIVE API — komut ekranı başlatıcısı
 * Çalıştırınca "Sistem başlatılıyor..." yazar, sunucuyu açar ve web panelini (ana sayfa) tarayıcıda açar;
 * kullanıcı adı ya da link panelin ana sayfasında istenir (istenirse burada da yazılabilir).
 * yayına bağlanır ve gelen HER veriyi (yorum, hediye, beğeni, giriş, takip, paylaşım...) satır satır listeler.
 * API'ye istek atıldığında ise yalnızca istenen veri döner (type / fields parametreleri).
 */
const readline = require('readline');
const { exec } = require('child_process');
const app = require('./server');

const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

// ---------------------------------------------------------------- Biçimlendirme
const useColor = process.stdout.isTTY && !process.env.NO_COLOR;
const C = { reset: '\x1b[0m', dim: '\x1b[90m', cyan: '\x1b[36m', yellow: '\x1b[33m', green: '\x1b[32m', red: '\x1b[31m', magenta: '\x1b[35m', blue: '\x1b[34m', bold: '\x1b[1m' };
const paint = (color, s) => (useColor ? `${C[color]}${s}${C.reset}` : s);

const time = () => new Date().toLocaleTimeString('tr-TR');
const fmt = (n) => Number(n || 0).toLocaleString('tr-TR');
const who = (u) => {
  if (!u) return '';
  const id = u.uniqueId || u.nickname || '?';
  return `@${id}` + (u.nickname && u.uniqueId && u.nickname !== u.uniqueId ? ` (${u.nickname})` : '');
};

/** Her olayı tek satırda yazdırır: saat | yayıncı | tür | kullanıcı: içerik */
function logEvent(e) {
  let tag, color = 'reset', text = '';
  switch (e.type) {
    case 'chat':      tag = 'MESAJ';    color = 'reset';   text = `${who(e.user)}: ${e.comment}`; break;
    case 'gift':
      tag = 'HEDİYE'; color = 'yellow';
      text = `${who(e.user)} → ${e.giftName} x${e.repeatCount}` +
             (e.countable ? ` (${fmt(e.diamonds)} elmas)` : ' (seri devam ediyor)');
      break;
    case 'like':      tag = 'BEĞENİ';   color = 'magenta'; text = `${who(e.user)} +${e.likeCount} (toplam ${fmt(e.totalLikes)})`; break;
    case 'member':    tag = 'GİRİŞ';    color = 'cyan';    text = `${who(e.user)} odaya girdi`; break;
    case 'follow':    tag = 'TAKİP';    color = 'green';   text = `${who(e.user)} takip etti`; break;
    case 'share':     tag = 'PAYLAŞIM'; color = 'blue';    text = `${who(e.user)} yayını paylaştı`; break;
    case 'roomUser':  tag = 'İZLEYİCİ'; color = 'dim';     text = `${fmt(e.viewerCount)} kişi izliyor`; break;
    case 'emote':     tag = 'EMOJİ';    color = 'cyan';    text = `${who(e.user)} emoji gönderdi`; break;
    case 'subscribe': tag = 'ABONE';    color = 'green';   text = `${who(e.user)} abone oldu`; break;
    case 'question':  tag = 'SORU';     color = 'cyan';    text = `${who(e.user)}: ${e.question}`; break;
    case 'envelope':  tag = 'ZARF';     color = 'yellow';  text = 'hediye zarfı / sandık'; break;
    case 'streamEnd': tag = 'YAYIN';    color = 'red';     text = 'yayın sona erdi'; break;
    case 'goal':      tag = 'HEDEF';    color = 'green';   text = `${fmt(e.target)} elmas hedefine ulaşıldı (${fmt(e.diamonds)} toplam)`; break;
    case 'status': {
      const names = { connected: 'bağlandı', disconnected: 'bağlantı koptu (yeniden denenecek)', error: 'hata', ended: 'yayın bitti' };
      tag = 'DURUM'; color = e.status === 'connected' ? 'green' : 'red';
      text = (names[e.status] || e.status) + (e.error ? `: ${e.error}` : '');
      break;
    }
    default: tag = e.type.toUpperCase(); color = 'dim'; text = '';
  }
  console.log(`${paint('dim', time())} ${paint('dim', '@' + e.username)} ${paint(color, `[${tag}]`.padEnd(11))} ${paint(color, text)}`);
}

function openBrowser(url) {
  if (process.env.NO_BROWSER) return;
  const cmd = process.platform === 'win32' ? `start "" "${url}"`
    : process.platform === 'darwin' ? `open "${url}"` : `xdg-open "${url}"`;
  exec(cmd, () => { /* tarayıcı açılamazsa sorun değil, adres ekranda yazıyor */ });
}

function shutdown() {
  console.log('\nKapatılıyor...');
  process.emit('SIGINT'); // server.js tüm bağlantıları kapatıp çıkar
}

// ---------------------------------------------------------------- Yayın ekleme
async function addStream(raw) {
  const username = app.clean(raw);
  if (!username) return;
  console.log(`\n@${username} yayınına bağlanılıyor...`);
  try {
    const { stream, already } = await app.connectStream(username);
    if (already) console.log(`@${username} zaten bağlı.`);
    else if (stream.status === 'connected') console.log(paint('green', `\nBağlandı! Canlı veriler aşağıda akıyor.\n`));
    else if (stream.fatal) console.log(paint('red', `\nBağlantı kurulamadı. Yukarıdaki nedeni çözmeden yeniden denemek işe yaramaz.\n`));
    else console.log(paint('yellow', `\nŞu an bağlanılamadı. Yayıncı canlı değilse sorun değil, canlı olunca otomatik bağlanacağım.\n`));
  } catch (err) {
    console.log(paint('red', 'Hata: ' + err.message));
  }
}

// ---------------------------------------------------------------- Ana akış
let username = '';
rl.on('SIGINT', shutdown);

(async () => {
  console.log(paint('cyan', '=========================================='));
  console.log(paint('bold', '        TikTok LIVE API'));
  console.log(paint('cyan', '=========================================='));
  console.log('');

  // İsteğe bağlı: "baslat.bat kullanici" ya da "node cli.js https://www.tiktok.com/@kullanici" ile doğrudan başlatılabilir.
  username = app.clean(process.argv[2] || '');

  console.log('\nSistem başlatılıyor...\n');
  let port;
  try {
    port = await app.start();
  } catch (err) {
    if (err.code === 'EADDRINUSE') console.log(paint('red', `Port ${app.PORT} zaten kullanımda. Başka bir kopya açık olabilir; onu kapatın ya da farklı port deneyin (örn. set PORT=3001).`));
    else console.log(paint('red', 'Sunucu başlatılamadı: ' + err.message));
    process.exit(1);
  }

  const url = `http://localhost:${port}`;
  console.log(paint('green', 'Sistem hazır.'));
  if (port !== app.PORT) {
    console.log(paint('yellow', `Not: ${app.PORT} portu başka bir pencerede kullanımda (eski bir TikTok LIVE API penceresi açık kalmış olabilir), bu yüzden ${port} portu kullanılıyor.`));
    console.log(paint('yellow', 'Eski pencereyi kapatmanı öneririm; aynı yayına iki kopya bağlanması gereksiz kota harcar.'));
  }
  if (!app.findPanelFile()) {
    console.log(paint('red', 'UYARI: index.html bulunamadı, web paneli açılmayacak. index.html dosyasını bu klasöre (cli.js ile aynı yere) koy.'));
  }
  console.log(`Yönetim paneli : ${url}`);
  console.log(`API adresi     : ${url}/api`);
  if (app.HOST === '0.0.0.0') {
    const ips = [];
    for (const list of Object.values(require('os').networkInterfaces()))
      for (const ni of list) if (ni.family === 'IPv4' && !ni.internal) ips.push(`http://${ni.address}:${port}`);
    if (ips.length) console.log(`Ağ erişimi     : ${ips.join('  |  ')}`);
    console.log(paint('dim', 'Domain üzerinden kullanım: .env dosyasına HOST=0.0.0.0 ve API_KEY yaz (bkz. .env.ornek); ters proxy örneği panelde "Domain erişimi" bölümünde.'));
  }
  if (app.API_KEY) console.log(paint(app.API_KEY_AUTO ? 'cyan' : 'dim', `API anahtarı   : ${app.API_KEY}${app.API_KEY_AUTO ? '  (otomatik üretildi → data/apikey.json)' : '  (.env / ortam değişkeninden)'}`));
  console.log(paint('dim', 'Komutlar: kullanıcı adı ya da link yaz = yayına bağlan | panel = paneli aç | çıkış = kapat'));

  // Her olayı komut ekranında listele (API cevaplarından bağımsız; API yalnızca istenen veriyi döner)
  app.subscribers.add({ username: null, types: null, fields: null, send: logEvent });

  openBrowser(url); // ana sayfa açılır; kullanıcı adı / link orada istenir
  if (username) await addStream(username);
  else console.log(paint('cyan', '\nTarayıcıdaki ana sayfada kullanıcı adını ya da yayın linkini gir (istersen buraya da yazabilirsin).\nCanlı veriler bağlandıktan sonra aşağıda akacak.\n'));

  rl.on('line', (line) => {
    const cmd = line.trim();
    if (!cmd) return;
    const low = cmd.toLocaleLowerCase('tr-TR');
    if (['çıkış', 'cikis', 'exit', 'quit', 'q'].includes(low)) return shutdown();
    if (low === 'panel') return openBrowser(url);
    addStream(cmd);
  });
})();