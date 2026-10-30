/**
 * cfmux 数据面 —— 自研 VLESS-over-WebSocket 内核
 *
 * 设计要点（相对 cfnew / edgetunnel 的差异）
 *  1. 只用 connect() 直连目标，不依赖 ProxyIP 中转（本账号实测 sockets 可用）
 *  2. 单连接内做「首包聚合」：VLESS 头可能跨多个 WS 帧，先攒够再解析，避免半包错误
 *  3. connect 带超时竞速，避免死 IP 把连接拖到 20s+
 *  4. 双向背压：WS <-> socket 都用 pipe，不整包缓存（省内存、降 CPU）
 *  5. 配置读取零 KV：全部来自 env，isolate 内缓存，单请求 CPU 极低
 *  6. /healthz 与 /diag 自带，便于控制面做健康检查与自动化监控
 */

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// ---------- 小工具 ----------
const enc = new TextEncoder();
const dec = new TextDecoder();

function hex2buf(s) {
  const out = new Uint8Array(s.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(s.substr(i * 2, 2), 16);
  return out;
}

function b64d(str) {
  try {
    const bin = atob(str.replace(/-/g, '+').replace(/_/g, '/'));
    const out = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
    return out;
  } catch { return null; }
}

function concat(chunks, total) {
  if (chunks.length === 1) return chunks[0];
  const out = new Uint8Array(total);
  let o = 0;
  for (const c of chunks) { out.set(c, o); o += c.length; }
  return out;
}

// ---------- VLESS 头解析 ----------
// version(1) uuid(16) addonLen(1) addons(n) cmd(1) port(2) atyp(1) addr(...)
function parseVless(buf, uuidHex) {
  if (buf.length < 18) return { need: 18 };
  if (buf[0] !== 0) return { error: 'bad version' };
  const id = buf.subarray(1, 17);
  const uuid = [...id].map((b) => b.toString(16).padStart(2, '0')).join('');
  if (uuid !== uuidHex) return { error: 'uuid mismatch' };
  const addonLen = buf[17];
  let p = 18 + addonLen;
  if (buf.length < p + 3) return { need: p + 3 };
  const cmd = buf[p]; p += 1;
  const port = (buf[p] << 8) | buf[p + 1]; p += 2;
  const atyp = buf[p]; p += 1;
  let host = '';
  if (atyp === 1) {
    if (buf.length < p + 4) return { need: p + 4 };
    host = [...buf.subarray(p, p + 4)].join('.'); p += 4;
  } else if (atyp === 2) {
    if (buf.length < p + 1) return { need: p + 1 };
    const len = buf[p]; p += 1;
    if (buf.length < p + len) return { need: p + len };
    host = dec.decode(buf.subarray(p, p + len)); p += len;
  } else if (atyp === 3) {
    if (buf.length < p + 16) return { need: p + 16 };
    const g = [];
    for (let i = 0; i < 16; i += 2) g.push(((buf[p + i] << 8) | buf[p + i + 1]).toString(16));
    host = g.join(':'); p += 16;
  } else return { error: 'bad atyp ' + atyp };
  return { cmd, host, port, offset: p };
}

// ---------- 目标可达性预检（CF 自家 IP / 保留段直接拒绝，省 20s 等待）----------
function isBannedTarget(host) {
  const m = host.match(/^(\d+)\.(\d+)\.(\d+)\.(\d+)$/);
  if (!m) return false;
  const a = +m[1], b = +m[2];
  if (a === 1 && b === 1) return true;                 // 1.1.1.0/24 CF
  if (a === 0 || a === 127 || a >= 224) return true;   // 保留/组播/回环
  if (a === 10 || a === 192 && b === 168) return true; // 私网
  if (a === 172 && b >= 16 && b <= 31) return true;
  if (a === 169 && b === 254) return true;
  if (a === 203 && b === 0 && +m[3] === 113) return true; // TEST-NET-3
  if (a === 198 && (b === 18 || b === 19)) return true;   // benchmark
  return false;
}

function withTimeout(promise, ms, onTimeout) {
  let t;
  const timeout = new Promise((_, rej) => { t = setTimeout(() => rej(new Error('timeout')), ms); });
  return Promise.race([promise.finally(() => clearTimeout(t)), timeout]).catch((e) => {
    if (onTimeout) onTimeout();
    throw e;
  });
}

// ---------- 流量计数（近似：isolate 内存累计，最多 5 分钟落一次 KV）----------
let TRAFFIC = {};
let LAST_FLUSH = Date.now();
function trafficBump(kind) {
  const d = new Date().toISOString().slice(0, 10);
  const row = (TRAFFIC[d] ||= { ws: 0, sub: 0, other: 0 });
  row[kind] = (row[kind] || 0) + 1;
}
async function trafficFlush(env, force) {
  if (!env.KV) return;
  const pendingTotal = Object.values(TRAFFIC).reduce((a, r) => a + (r.ws || 0) + (r.sub || 0) + (r.other || 0), 0);
  // 攒够 20 次或满 5 分钟才落盘，避免 KV 写入超限（免费版 1000 次/天）
  if (!force && pendingTotal < 20 && Date.now() - LAST_FLUSH < 300000) return;
  LAST_FLUSH = Date.now();
  const pending = TRAFFIC;
  TRAFFIC = {};
  try {
    let cur = {};
    try { cur = JSON.parse((await env.KV.get('traffic')) || '{}'); } catch {}
    for (const [d, row] of Object.entries(pending)) {
      const t = (cur[d] ||= { ws: 0, sub: 0, other: 0 });
      for (const k of ['ws', 'sub', 'other']) t[k] = (t[k] || 0) + (row[k] || 0);
    }
    // 只保留最近 30 天
    const keys = Object.keys(cur).sort().slice(-30);
    const trim = {}; for (const k of keys) trim[k] = cur[k];
    await env.KV.put('traffic', JSON.stringify(trim));
  } catch (e) { TRAFFIC = pending; }
}

// ---------- 主入口 ----------
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const uuidHex = (env.uuid || env.u || '').trim().toLowerCase().replace(/-/g, '');
    const pathKey = (env.path || '/').trim();

    // 运维端点
    if (url.pathname === '/healthz') {
      return json({ ok: true, v: VERSION, colo: request.cf?.colo, ts: Date.now() });
    }
    if (url.pathname === '/diag') { const d = await diag(request); const o = await d.json(); o.uuidLen = uuidHex.length; o.uuidSet = !!uuidHex; o.pathKey = pathKey; return json(o); }
    if (url.pathname === '/sub') { trafficBump('sub'); ctx.waitUntil(trafficFlush(env)); return subscription(request, env, url); }
    if (url.pathname === '/panel') return panelPage(request, env, url);
    if (url.pathname === '/api/entries') return apiEntries(request, env, url);
    if (url.pathname === '/api/quality') return apiQuality(request, env, url);
    if (url.pathname === '/api/state') return apiState(request, env, url);
    if (url.pathname === '/api/action') return apiAction(request, env, url);
    if (url.pathname === '/api/import') return apiImport(request, env, url);
    if (url.pathname === '/api/trigger') return apiTrigger(request, env, url);
    if (url.pathname === '/api/kernel') return apiKernel(request, env, url);
    if (url.pathname === '/api/traffic') return apiTraffic(request, env, url);
    if (url.pathname === '/api/stats') return apiStats(request, env, url);
    if (url.pathname === '/api/quality/report') return apiQualityReport(request, env, url);
    if (url.pathname === '/api/shortlink') return apiShortLink(request, env, url);
    if (url.pathname === '/api/shortlinks') return apiShortLinks(request, env, url);
    if (url.pathname === '/api/notify/test') return apiNotifyTest(request, env, url);
    if (url.pathname.startsWith('/s/')) return shortLinkRedirect(request, env, url);
    if (url.pathname === '/api/quality/entries') return apiQualityEntries(request, env, url);
    if (url.pathname === '/admin') return adminPage(request, env, url);
    if (url.pathname === '/admin/login') return adminLogin(request, env, url);
    if (url.pathname === '/admin/logout') return adminLogout(request, env, url);
    if (url.pathname === '/admin/save') return adminSave(request, env, url);

    // 代理入口：任意路径都可，按需校验自定义路径
    if (!uuidHex || uuidHex.length !== 32) return new Response('cfmux: uuid not configured', { status: 503 });
    if (pathKey !== '/' && url.pathname !== pathKey) return new Response('Not Found', { status: 404 });

    const upgrade = (request.headers.get('Upgrade') || '').toLowerCase();
    if (upgrade !== 'websocket') {
      return new Response(VERSION + '\nhello', { headers: { 'content-type': 'text/plain; charset=utf-8' } });
    }
    trafficBump('ws'); ctx.waitUntil(trafficFlush(env));
    return proxyWebSocket(request, env, uuidHex);
  },
};

const VERSION = 'cfmux/1.0';

function json(o, status = 200, extra) {
  const h = { 'content-type': 'application/json', 'cache-control': 'no-store' };
  if (extra) Object.assign(h, extra);
  return new Response(JSON.stringify(o), { status, headers: h });
}

async function diag(request) {
  const out = { v: VERSION, colo: request.cf?.colo, ts: Date.now(), sockets: null, fetchOut: null };
  try {
    const { connect } = await import('cloudflare:sockets');
    const t0 = Date.now();
    const s = connect({ hostname: '151.101.1.140', port: 443 });
    await withTimeout(s.opened, 5000);
    out.sockets = { ok: true, ms: Date.now() - t0 };
    try { await s.close(); } catch {}
  } catch (e) { out.sockets = { ok: false, err: String(e).slice(0, 120) }; }
  try {
    const t0 = Date.now();
    const r = await fetch('https://www.cloudflare.com/cdn-cgi/trace', { cf: { cacheTtl: 0 } });
    await r.text();
    out.fetchOut = { ok: r.ok, ms: Date.now() - t0 };
  } catch (e) { out.fetchOut = { ok: false, err: String(e).slice(0, 120) }; }
  return json(out);
}

// ---------- WS <-> TCP ----------
async function proxyWebSocket(request, env, uuidHex) {
  const pair = new WebSocketPair();
  const [client, server] = Object.values(pair);
  server.accept();

  // 早期数据：仅当子协议解 base64 后「像 VLESS 头」才采用
  const subproto = request.headers.get('Sec-WebSocket-Protocol') || '';
  let chunks = [];
  let bufLen = 0;
  if (subproto) {
    const e = b64d(subproto);
    if (e && e.length >= 18 && e[0] === 0) { chunks.push(e); bufLen += e.length; }
  }

  let state = 'header';           // header -> connecting -> open -> closed
  let socket = null, writer = null;
  let queued = [];                // connecting 期间到达的数据
  const connectTimeoutMs = +(env.connect_timeout_ms || 8000);

  const closeAll = (code = 1000, reason = '') => {
    if (state === 'closed') return;
    state = 'closed';
    try { writer && writer.close().catch(() => {}); } catch {}
    try { socket && socket.close(); } catch {}
    try { server.close(code, String(reason).slice(0, 100)); } catch {}
  };

  // 只解析一次头；连接完成后把缓冲与新数据顺序写出
  const tryStart = async () => {
    if (state !== 'header') return;
    const buf = concat(chunks, bufLen);
    const r = parseVless(buf, uuidHex);
    if (r.error) { closeAll(1008, r.error); return; }
    if (r.need) return;                       // 头还不完整
    if (r.cmd !== 1) { closeAll(1003, 'cmd=' + r.cmd); return; }
    if (isBannedTarget(r.host) || !r.port) { closeAll(1003, 'target ' + r.host + ':' + r.port); return; }

    const rest = buf.subarray(r.offset);
    chunks = []; bufLen = 0;
    state = 'connecting';

    try {
      const { connect } = await import('cloudflare:sockets');
      socket = connect({ hostname: r.host, port: r.port, allowHalfOpen: false });
      await withTimeout(socket.opened, connectTimeoutMs, () => { try { socket.close(); } catch {} });
    } catch (e) { closeAll(1011, 'connect fail'); return; }
    if (state === 'closed') { try { socket.close(); } catch {} return; }

    try { server.send(new Uint8Array([0, 0])); } catch { closeAll(1011); return; }

    socket.readable.pipeTo(new WritableStream({
      write(chunk) { if (state !== 'open') return; try { server.send(chunk); } catch { closeAll(1011); } },
      close() { closeAll(1000); },
      abort() { closeAll(1011); },
    })).catch(() => closeAll(1011));

    writer = socket.writable.getWriter();
    state = 'open';
    try {
      if (rest.length) await writer.write(rest);
      for (const c of queued) await writer.write(c);
      queued = [];
    } catch { closeAll(1011); }
  };

  server.addEventListener('message', (ev) => {
    if (state === 'closed') return;
    const d = ev.data;
    const chunk = typeof d === 'string' ? enc.encode(d) : new Uint8Array(d);
    if (state === 'open') { writer.write(chunk).catch(() => closeAll(1011)); return; }
    if (state === 'connecting') { queued.push(chunk); return; }
    chunks.push(chunk); bufLen += chunk.length;
    tryStart();
  });
  server.addEventListener('close', () => closeAll(1000));
  server.addEventListener('error', () => closeAll(1011));

  if (bufLen) queueMicrotask(() => tryStart());

  const respHeaders = {};
  if (subproto) respHeaders['Sec-WebSocket-Protocol'] = subproto;   // 必须回显
  return new Response(null, { status: 101, webSocket: client, headers: respHeaders });
}


// ---------- 质量层：只放行实测达标的节点 ----------
async function getQuality(env) {
  try { if (env.KV && env.KV.get) return JSON.parse((await env.KV.get('quality')) || '{}'); } catch {}
  return {};
}
async function getEntries(env) {
  try {
    if (env.KV && env.KV.get) {
      const raw = await env.KV.get('entries');
      if (raw) { const a = JSON.parse(raw); if (Array.isArray(a) && a.length) return a; }
    }
  } catch {}
  const st = await getSettings(env);
  if (st.entries && Array.isArray(st.entries) && st.entries.length) return st.entries;
  return [{ name: 'default', host: '', uuid: '' }];   // host 为空时用当前域名
}
async function getKernelConfig(env, entry, force) {
  if (!entry || !entry.host || !entry.uuid) return null;
  const key = `kcfg:${entry.host}`;
  try {
    if (env.KV && !force) {
      const raw = await env.KV.get(key);
      if (raw) { const o = JSON.parse(raw); if (o && o.cfg) return o.cfg; }   // 面板写入的权威副本
    }
  } catch {}
  try {
    const r = await fetch(`https://${entry.host}/${entry.uuid}/api/config`, { cf: { cacheTtl: 0, timeoutMs: 4000 } });
    if (r.ok) {
      const cfg = await r.json();
      try { if (env.KV) await env.KV.put(key, JSON.stringify({ ts: Date.now(), cfg })); } catch {}
      return cfg;
    }
  } catch {}
  return null;
}

async function getThreshold(env) {
  const st = await getSettings(env);
  return {
    minScore: st.minScore ?? 0,
    minTput: st.minTput ?? 0,
    requireQuality: st.requireQuality !== false,
    maxAgeHours: st.maxAgeHours ?? 24,
    softExpireDays: st.softExpireDays ?? 30,
  };
}

// ---------- 控制面：订阅 / 面板 / 优选引擎 ----------
const DEFAULT_PREFERRED = ['104.16.1.1','104.16.2.1','104.16.3.1','104.17.1.1','104.17.2.1','188.114.96.1','188.114.97.1'];
// 内置的优选域名源（可在 env.sources 追加，逗号分隔 URL）
const DEFAULT_SOURCES = ['https://bestcf.fxxk.dedyn.io/cf_domains.txt'];

const REGION_NAMES = {
  HKG: '香港', SJC: '美国-圣何塞', LAX: '美国-洛杉矶', NRT: '日本-东京', KIX: '日本-大阪',
  SIN: '新加坡', ICN: '韩国-首尔', TPE: '台湾-台北', FRA: '德国-法兰克福', LHR: '英国-伦敦',
  AMS: '荷兰-阿姆斯特丹', SYD: '澳洲-悉尼', MUM: '印度-孟买', DXB: '阿联酋-迪拜',
};

async function loadSources(env, request) {
  // 合并：KV 实测优选(IP) + 外部优选域名/IP 列表（带 10 分钟边缘缓存）
  const st = await getSettings(env);
  const urls = [].concat(
    (st.sources || env.sources || '').split(',').map((x) => x.trim()).filter(Boolean),
    DEFAULT_SOURCES
  );
  const out = [];
  for (const u of urls) {
    try {
      const cache = caches.default;
      const key = new Request(u, { method: 'GET' });
      let res = await cache.match(key);
      if (!res) {
        res = await fetch(u, { cf: { cacheTtl: 600, cacheEverything: true, timeoutMs: 4000 } });
        if (res.ok) await cache.put(key, res.clone());
      }
      if (!res || !res.ok) continue;
      const txt = await res.text();
      for (const line of txt.split('\n')) {
        const v = line.trim().replace(/#.*$/, '');
        if (!v || v.startsWith('<')) continue;
        if (/^[a-z0-9.*-]+(\.[a-z0-9-]+)+$/i.test(v) || /^(\d{1,3}\.){3}\d{1,3}$/.test(v)) out.push(v);
      }
    } catch {}
  }
  return [...new Set(out)];
}

async function resolveEntries(request, env, url) {
  const st = await getSettings(env);
  const limitRaw = url.searchParams.get('limit');
  const limit = Math.min(limitRaw === null ? (st.limit || 0) : +limitRaw, 300);
  const candRaw = url.searchParams.get('cand');          // 单独控制候选数量（默认全量）
  const candCap = candRaw ? Math.min(+candRaw, 2000) : 2000;
  const wantRegion = (url.searchParams.get('region') || '').trim().toUpperCase();
  const like = (url.searchParams.get('like') || '').trim();   // 域名/IP 关键字过滤

  let ips = [];
  try {
    if (env.KV && typeof env.KV.get === 'function') {
      const raw = await env.KV.get('preferred');
      if (raw) ips = (JSON.parse(raw) || []).map((x) => (typeof x === 'string' ? x : x.ip)).filter(Boolean);
    }
  } catch {}
  if (!ips.length) ips = DEFAULT_PREFERRED.slice();
  // 外部优选（域名+IP）追加在后面，可用 like 精确挑选
  const external = await loadSources(env, request);

  let colos = {};
  try { if (env.KV && env.KV.get) colos = JSON.parse(await env.KV.get('colos') || '{}'); } catch {}

  const merged = [];
  const seen = new Set();
  const push = (v, kind) => {
    const k = String(v).toLowerCase();
    if (!k || seen.has(k)) return;
    seen.add(k);
    merged.push({ value: v, kind, colo: colos[v] || null });
  };
  ips.forEach((v) => push(v, 'ip'));
  external.forEach((v) => push(v, /^\d+\./.test(v) ? 'ip' : 'domain'));

  let entries = merged;
  if (like) entries = entries.filter((e) => e.value.toLowerCase().includes(like.toLowerCase()));
  if (wantRegion) entries = entries.filter((e) => (e.colo || '').toUpperCase() === wantRegion);
  return { entries: entries.slice(0, candCap), total: merged.length, limit, wantRegion };
}

function regionLabel(q, e, colos) {
  // 优先：质量引擎实测的真实出口国家 → 其次 colo 映射 → 最后类型标记
  if (q && q.cc) return CC_NAME[q.cc] || q.country || q.cc;
  if (q && q.country) return q.country;
  const c = colos && colos[e.value];
  if (c) return REGION_NAMES[c] || CC_NAME[c] || c;
  return e.kind === 'domain' ? '域名' : 'IP';
}
const CC_NAME = {
  HK: '香港', TW: '台湾', JP: '日本', KR: '韩国', SG: '新加坡', MY: '马来西亚', TH: '泰国', VN: '越南',
  PH: '菲律宾', ID: '印尼', IN: '印度', US: '美国', CA: '加拿大', GB: '英国', DE: '德国', FR: '法国',
  NL: '荷兰', RU: '俄罗斯', AU: '澳洲', BR: '巴西', TR: '土耳其', AE: '阿联酋', CN: '中国', MO: '澳门',
  SE: '瑞典', ES: '西班牙', IT: '意大利', CH: '瑞士', AT: '奥地利', BE: '比利时', PL: '波兰', CZ: '捷克',
  UA: '乌克兰', IL: '以色列', SA: '沙特', EG: '埃及', ZA: '南非', NG: '尼日利亚', KE: '肯尼亚',
  AR: '阿根廷', CL: '智利', CO: '哥伦比亚', PE: '秘鲁', MX: '墨西哥', NZ: '新西兰', IE: '爱尔兰',
  PT: '葡萄牙', GR: '希腊', FI: '芬兰', NO: '挪威', DK: '丹麦', HU: '匈牙利', RO: '罗马尼亚',
  BG: '保加利亚', HR: '克罗地亚', SK: '斯洛伐克', LT: '立陶宛', LV: '拉脱维亚', EE: '爱沙尼亚',
  PK: '巴基斯坦', BD: '孟加拉', LK: '斯里兰卡', NP: '尼泊尔', KH: '柬埔寨', MM: '缅甸', LA: '老挝',
  MN: '蒙古', KZ: '哈萨克', UZ: '乌兹别克', QA: '卡塔尔', KW: '科威特', BH: '巴林', OM: '阿曼',
  JO: '约旦', LB: '黎巴嫩', IQ: '伊拉克', IR: '伊朗', MA: '摩洛哥', TN: '突尼斯', DZ: '阿尔及利亚',
};

function quickHash(str) {
  let h = 5381;
  for (let i = 0; i < str.length; i++) h = ((h * 33) ^ str.charCodeAt(i)) >>> 0;
  return h.toString(36);
}
function lgKey(url) {
  const u = new URL(url.toString());
  u.searchParams.delete('nocache');
  u.searchParams.delete('t');
  return 'lg:' + quickHash(u.pathname + '?' + u.searchParams.toString());
}

async function subscription(request, env, url) {
  // 边缘缓存 60s：同一订阅 URL 命中缓存直接返回，省 KV 读与 CPU
  // ?nocache=1 可绕过缓存（改完配置立刻生效）
  if (url.searchParams.get('nocache')) {
    const fresh = await buildSubscription(request, env, url);
    if (fresh.status === 200) {
      try { await saveLastGood(env, url, fresh.clone()); } catch {}
      return fresh;
    }
    const fallback = await getLastGood(env, url);
    if (fallback) return fallback;
    return fresh;
  }
  const cache = caches.default;
  const ckey = new Request(url.toString(), { method: 'GET' });
  try {
    const hit = await cache.match(ckey);
    if (hit) return hit;
  } catch {}
  const resp = await buildSubscription(request, env, url);
  if (resp.status === 200) {
    try { await cache.put(ckey, resp.clone()); } catch {}
    try { await saveLastGood(env, url, resp.clone()); } catch {}
    return resp;
  }
  // 兜底：KV 边缘一致性抖动 / 数据暂时为空时，返回最近一次成功结果
  const fallback = await getLastGood(env, url);
  if (fallback) return fallback;
  return resp;
}

async function saveLastGood(env, url, resp) {
  if (!env.KV) return;
  const body = await resp.text();
  if (!body || body.length > 900000) return;
  const ct = resp.headers.get('content-type') || 'text/plain; charset=utf-8';
  await env.KV.put(lgKey(url), JSON.stringify({ ts: Date.now(), body, ct }));
}
async function getLastGood(env, url) {
  if (!env.KV) return null;
  try {
    const raw = await env.KV.get(lgKey(url));
    if (!raw) return null;
    const o = JSON.parse(raw);
    if (!o || !o.body || Date.now() - (o.ts || 0) > 86400000) return null;   // 只在 24 小时内兜底
    return new Response(o.body, {
      status: 200,
      headers: { 'content-type': o.ct || 'text/plain; charset=utf-8', 'cache-control': 'no-store', 'x-cfmux-fallback': 'last-good', 'x-cfmux-fallback-age': String(Math.round((Date.now() - o.ts) / 60000)) + 'm' },
    });
  } catch { return null; }
}

async function buildSubscription(request, env, url) {
  const target = (url.searchParams.get('target') || 'base64').toLowerCase();
  const wsPath = url.searchParams.get('path') || '/';
  const base = url.searchParams.get('name') || 'cfmux';
  const { entries: candRaw, total } = await resolveEntries(request, env, url);
  const kindFilter = (url.searchParams.get('kind') || '').toLowerCase();
  const cand = kindFilter ? candRaw.filter((c) => c.kind === kindFilter) : candRaw;
  const entries = await getEntries(env);
  const quality = await getQuality(env);
  const th = await getThreshold(env);
  let colosMap = {}, banned = {};
  try {
    if (env.KV) {
      colosMap = JSON.parse((await env.KV.get('colos')) || '{}');
      banned = JSON.parse((await env.KV.get('banned')) || '{}');
    }
  } catch {}
  const hostOverride = url.searchParams.get('host') || '';

  // 组装候选：entry × preferred，质量优先
  let nodes = [];
  const forceKernel = url.searchParams.get('refreshKernel') === '1';
  for (const e of entries) {
    const host = e.host || url.host;
    const uuid = e.uuid || env.uuid;
    if (!uuid) continue;
    // 读取该数据面的内核配置：决定协议开关 + WS 路径（保证订阅与内核一致）
    const kc = await getKernelConfig(env, e, forceKernel);
    const vlessOn = !kc || kc.ev !== 'no';
    const trojanOn = !!(kc && kc.et === 'yes' && String(kc.tp || '').trim());
    const ePath = (kc && String(kc.d || '').trim()) ? String(kc.d).trim() : wsPath;
    if (!vlessOn && !trojanOn) continue;   // 两种协议都关 → 跳过该入口
    for (const c of cand) {
      if (banned[c.value] && banned[c.value].until > Date.now()) continue;   // 已被淘汰（封禁中）
      const q = quality[`${host}|${c.value}`];
      // 有质量数据 → 必须达标；无质量数据 → 在 requireQuality=false 时才放行
      let stale = false;
      if (q) {
        if (!q.ok) continue;
        if (q.cooldownUntil && q.cooldownUntil > Date.now()) continue;   // 熔断中
        const ageH = (Date.now() - (q.ts || 0)) / 3600000;
        const fresh = th.maxAgeHours <= 0 || ageH <= th.maxAgeHours;
        const hardDays = th.softExpireDays ?? 30;
        if (!fresh) {
          // 陈旧数据：不超过硬上限就保留，但降权并排到后面（避免测速机停机导致订阅空掉）
          if (hardDays > 0 && ageH > hardDays * 24) continue;
          stale = true;
        }
        const effScore = stale ? Math.max(0, (q.score ?? 0) - 15) : (q.score ?? 0);
        if (effScore < th.minScore) continue;
        if ((q.tput ?? 0) < th.minTput) continue;
      } else if (th.requireQuality) {
        // 该数据面完全没有测速数据时，可用 ?unmeasured=1 放行（排最后）
        if (url.searchParams.get('unmeasured') !== '1') continue;
        const hasAny = Object.keys(quality).some((k) => k.startsWith(host + '|'));
        if (hasAny) continue;
      }
      const proto = vlessOn ? 'vless' : 'trojan';
      nodes.push({
        entry: e.name || 'core', proto, path: ePath, stale,
        score: q ? (stale ? Math.max(0, (q.score ?? 0) - 15) : q.score) : null,
        tpass: trojanOn ? (kc.tp || '').trim() : '',
        value: c.value, uuid, host,
        name: `${regionLabel(q, c, colosMap)}-${e.name || 'core'}-${String(nodes.length + 1).padStart(2, '0')}[${(trojanOn && !vlessOn) ? 'T' : 'V'}${kc && kc.ech === 'yes' ? '+E' : ''}]${q && q.tier ? '-' + q.tier : ''}${q && q.lat ? '-' + q.lat + 'ms' : ''}`,
        rawScore: q ? q.score : null, tier: q ? q.tier : null, tput: q ? q.tput : null, lat: q ? q.lat : null,
        cc: q ? q.cc : null, country: q ? q.country : null, exitIp: q ? q.exitIp : null,
        dedupKey: (q && (q.exitIp || q.resolved)) || c.value,
      });
    }
  }
  // 置顶节点（settings.pinned: ["host|value", ...]）排最前
  const pinned = new Set((await getSettings(env)).pinned || []);
  if (pinned.size) {
    nodes.sort((a, b) => {
      const pa = pinned.has(`${a.host}|${a.value}`) ? 1 : 0;
      const pb = pinned.has(`${b.host}|${b.value}`) ? 1 : 0;
      return pb - pa;   // 置顶在前，其余保持
    });
  }

  // 按国家过滤（cc=SG,JP；也可用 country=新加坡）
  const ccList = (url.searchParams.get('cc') || '').toUpperCase().split(',').map(x=>x.trim()).filter(Boolean);
  const countryLike = (url.searchParams.get('country') || '').trim();
  if (ccList.length) nodes = nodes.filter((n) => n.cc && ccList.includes(String(n.cc).toUpperCase()));
  if (countryLike) nodes = nodes.filter((n) => (n.country || '').includes(countryLike) || (CC_NAME[n.cc] || '').includes(countryLike));

  // 每入口择优：先按 entry 分组各取 topK（避免某个入口霸榜），再全局排序
  const perEntryRaw = +(url.searchParams.get('perEntry') ?? (await getSettings(env)).perEntry ?? 0);
  const perEntryK = perEntryRaw > 0 ? perEntryRaw : 9999;   // 0 = 每入口全量
  const byEntry = new Map();
  for (const n of nodes) {
    const k = n.entry || 'core';
    if (!byEntry.has(k)) byEntry.set(k, []);
    byEntry.get(k).push(n);
  }
  const picked = [];
  for (const list of byEntry.values()) {
    list.sort((a, b) => (b.score ?? -1) - (a.score ?? -1) || (b.tput ?? 0) - (a.tput ?? 0));
    picked.push(...list.slice(0, perEntryK));
  }
  // 质量排序（有分的在前，同分按吞吐）
  picked.sort((a, b) => (Number(a.stale) - Number(b.stale)) || ((b.score ?? -1) - (a.score ?? -1)) || ((b.tput ?? 0) - (a.tput ?? 0)));
  // 去重：同一真实出口 IP（或同一解析 IP）只保留评分最高的一条
  let afterDedup = picked;
  if (url.searchParams.get('dedup') !== '0') {
    const seen = new Set();
    afterDedup = picked.filter((n) => { const k = n.dedupKey; if (seen.has(k)) return false; seen.add(k); return true; });
  }
  const rawLimit = +(url.searchParams.get('limit') ?? (await getSettings(env)).limit ?? 0);
  const cap = rawLimit > 0 ? Math.min(rawLimit, 300) : 300;   // limit=0/缺省 → 全部（上限 300）
  const limited = afterDedup.slice(0, cap);
  if (!limited.length) return json({ ok: false, msg: '没有达标节点（检查质量阈值 / 阈值过严）', total, threshold: th }, 404);

  const hdr = {
    'content-type': 'text/plain; charset=utf-8',
    'cache-control': 'no-store',
    'x-cfmux-total': String(total),
    'x-cfmux-nodes': String(limited.length),
    'x-cfmux-pass': String(nodes.length),
    'x-cfmux-dedup': String(picked.length - afterDedup.length),
    'x-cfmux-minscore': String(th.minScore),
    'x-cfmux-stale': String(limited.filter((n) => n.stale).length),
  };
  const sn = limited.map((n) => ({ value: n.value, name: n.name, host: n.host, uuid: n.uuid, proto: n.proto || 'vless', path: n.path || wsPath, tpass: n.tpass || '' }));
  if (target === 'singbox' || target === 'sing-box') return new Response(singboxConfig(sn, url, wsPath), { headers: hdr });
  if (target === 'clash' || target === 'stash' || target === 'mihomo' || target === 'meta') return new Response(clashConfig(sn, url, wsPath), { headers: hdr });
  if (target === 'plain' || target === 'text' || target === 'uri') return new Response(v2rayLinks(sn, url, wsPath), { headers: hdr });
  if (target === 'json') return json({ nodes: sn.map((n) => ({ name: n.name, host: n.host, uuid: n.uuid, server: n.value, port: 443, sni: n.host, path: wsPath, ws: true, tls: true })) }, 200, hdr);
  if (target === 'qx' || target === 'quantumult') return new Response(qxConfig(sn, wsPath), { headers: hdr });
  if (target === 'loon') return new Response(loonConfig(sn, wsPath), { headers: hdr });
  if (target === 'surge') return new Response(surgeConfig(sn, wsPath), { headers: hdr });
  return new Response(b64(v2rayLinks(sn, url, wsPath)), { headers: hdr });
}

async function apiState(request, env, url) {
  if (!(await isAdmin(request, env))) return json({ ok: false, msg: '未登录' }, 401);
  const [quality, entries, colos] = await Promise.all([getQuality(env), getEntries(env), (async () => {
    try { if (env.KV) return JSON.parse((await env.KV.get('colos')) || '{}'); } catch {} return {};
  })()]);
  const settings = await getSettings(env);
  let preferred = '[]', history = [];
  try {
    if (env.KV) {
      preferred = (await env.KV.get('preferred')) || '[]';
      history = JSON.parse((await env.KV.get('history')) || '[]');
    }
  } catch {}
  const entryStats = {};
  for (const e of entries) {
    const host = e.host || url.host;
    const keys = Object.keys(quality).filter((k) => k.startsWith(host + '|'));
    const okN = keys.filter((k) => quality[k] && quality[k].ok).length;
    entryStats[e.name || host] = { host, measured: keys.length, ok: okN };
  }
  return json({
    base: `https://${url.host}`, settings, entries, quality, colos, preferred, history, entryStats,
    ccNames: CC_NAME, regionNames: REGION_NAMES,
  });
}

async function apiAction(request, env, url) {
  if (!(await isAdmin(request, env))) return json({ ok: false, msg: '未登录' }, 401);
  if (!env.KV) return json({ ok: false, msg: 'KV 未绑定' }, 503);
  const body = await request.json().catch(() => ({}));
  const type = String(body.type || '');
  const key = String(body.key || '');
  let q = {};
  try { q = JSON.parse((await env.KV.get('quality')) || '{}'); } catch {}
  if (type === 'ban') {
    if (!q[key]) return json({ ok: false, msg: '节点不存在' }, 404);
    q[key].ok = false; q[key].cooldownUntil = Date.now() + 24 * 3600 * 1000;
    q[key].cooldownReason = '手动熔断';
  } else if (type === 'pin' || type === 'unpin') {
    const st = await getSettings(env);
    const arr = new Set(st.pinned || []);
    if (type === 'pin') arr.add(key); else arr.delete(key);
    st.pinned = [...arr];
    await env.KV.put('settings', JSON.stringify(st));
    return json({ ok: true, pinned: st.pinned.length });
  } else if (type === 'unban') {
    if (!q[key]) return json({ ok: false, msg: '节点不存在' }, 404);
    delete q[key].cooldownUntil; delete q[key].cooldownReason; q[key].fails = 0;
  } else {
    return json({ ok: false, msg: 'unknown action' }, 400);
  }
  await env.KV.put('quality', JSON.stringify(q));
  return json({ ok: true });
}

async function apiImport(request, env, url) {
  if (!(await isAdmin(request, env))) return json({ ok: false, msg: '未登录' }, 401);
  if (!env.KV) return json({ ok: false, msg: 'KV 未绑定' }, 503);
  const body = await request.json().catch(() => ({}));
  const src = String(body.url || '').trim();
  if (!src) return json({ ok: false, msg: '缺少 url' }, 400);
  let text = '';
  try {
    const r = await fetch(src, { cf: { cacheTtl: 0, timeoutMs: 8000 } });
    if (!r.ok) return json({ ok: false, msg: `拉取失败 HTTP ${r.status}` }, 400);
    text = await r.text();
  } catch (e) { return json({ ok: false, msg: '拉取异常: ' + String(e).slice(0, 80) }, 400); }

  const lines = text.split('\n').map((x) => x.trim().replace(/#.*$/, '')).filter((x) =>
    x && !x.startsWith('<') && (/^[a-z0-9.*-]+(\.[a-z0-9-]+)+$/i.test(x) || /^(\d{1,3}\.){3}\d{1,3}$/.test(x)));
  if (!lines.length) return json({ ok: false, msg: '未解析到有效 IP/域名' }, 400);

  let cur = [];
  try { cur = JSON.parse((await env.KV.get('preferred')) || '[]'); } catch {}
  const curVals = new Set(cur.map((x) => (typeof x === 'string' ? x : x.ip)));
  let added = 0;
  for (const v of lines) if (!curVals.has(v)) { cur.push(v); curVals.add(v); added++; }
  if (body.mode === 'replace') { cur = [...new Set(lines)]; added = cur.length; }
  await env.KV.put('preferred', JSON.stringify(cur));
  return json({ ok: true, added, total: cur.length, parsed: lines.length });
}

async function apiTrigger(request, env, url) {
  if (!(await isAdmin(request, env))) return json({ ok: false, msg: '未登录' }, 401);
  const tok = env.gh_token, repo = env.gh_repo, wf = env.gh_workflow || 'refresh-quality.yml', ref = env.gh_ref || 'main';
  if (!tok || !repo) {
    return json({ ok: false, configured: false, msg: '未配置 gh_token / gh_repo（变量：gh_token, gh_repo=owner/repo）',
      hint: '也可在本机运行：KV_ID=<id> ENTRIES_FILE=... PREF_FILE=... ./tools/refresh.sh' }, 200);
  }
  try {
    const r = await fetch(`https://api.github.com/repos/${repo}/actions/workflows/${wf}/dispatches`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${tok}`, Accept: 'application/vnd.github+json', 'User-Agent': 'cfmux' },
      body: JSON.stringify({ ref, inputs: {} }),
    });
    if (r.status === 204) return json({ ok: true, configured: true, msg: '已触发 GitHub Actions 刷新' });
    const t = await r.text();
    return json({ ok: false, configured: true, msg: `GitHub 返回 ${r.status}: ${t.slice(0, 120)}` }, 200);
  } catch (e) { return json({ ok: false, msg: String(e).slice(0, 100) }, 200); }
}

async function apiKernel(request, env, url) {
  if (!(await isAdmin(request, env))) return json({ ok: false, msg: '未登录' }, 401);
  if (request.method === 'GET') {
    const host = url.searchParams.get('host'), uuid = url.searchParams.get('uuid');
    if (!host || !uuid) return json({ ok: false, msg: '缺少 host/uuid' }, 400);
    try {
      const r = await fetch(`https://${host}/${uuid}/api/config`, { cf: { cacheTtl: 0, timeoutMs: 8000 } });
      if (!r.ok) return json({ ok: false, msg: `数据面返回 HTTP ${r.status}` }, 200);
      return json({ ok: true, host, config: await r.json() });
    } catch (e) { return json({ ok: false, msg: '拉取失败: ' + String(e).slice(0, 80) }, 200); }
  }
  // POST 保存
  const body = await request.json().catch(() => ({}));
  const host = String(body.host || ''), uuid = String(body.uuid || '');
  if (!host || !uuid || !body.config) return json({ ok: false, msg: '缺少 host/uuid/config' }, 400);
  try {
    const r = await fetch(`https://${host}/${uuid}/api/config`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body.config), cf: { cacheTtl: 0, timeoutMs: 8000 },
    });
    const t = await r.text();
    let j = null; try { j = JSON.parse(t); } catch {}
    const ok = r.ok && (!j || j.success !== false);
    if (ok && env.KV) {
      try {
        let base = {};
        const cached = await env.KV.get('kcfg:' + host);
        if (cached) base = JSON.parse(cached).cfg || {};
        else { const g = await fetch(`https://${host}/${uuid}/api/config`, { cf: { cacheTtl: 0, timeoutMs: 4000 } }); if (g.ok) base = await g.json(); }
        // 以「我们提交的补丁」为准（内核 isolate 缓存可能返回旧值）
        const merged = { ...base, ...(j && j.config ? j.config : {}), ...(body.config || {}) };
        await env.KV.put('kcfg:' + host, JSON.stringify({ ts: Date.now(), cfg: merged }));
      } catch {}
    }
    return json({ ok, msg: (j && j.message) || t.slice(0, 120), config: j && j.config });
  } catch (e) { return json({ ok: false, msg: '保存失败: ' + String(e).slice(0, 80) }, 200); }
}

async function apiTraffic(request, env, url) {
  if (!(await isAdmin(request, env))) return json({ ok: false, msg: '未登录' }, 401);
  let t = {};
  try { if (env.KV) t = JSON.parse((await env.KV.get('traffic')) || '{}'); } catch {}
  // 把内存里未落盘的也算上
  const live = JSON.parse(JSON.stringify(t));
  for (const [d, row] of Object.entries(TRAFFIC)) {
    const x = (live[d] ||= { ws: 0, sub: 0, other: 0 });
    for (const k of ['ws', 'sub', 'other']) x[k] = (x[k] || 0) + (row[k] || 0);
  }
  const quota = +(await getSettings(env)).dailyQuota || 100000;
  return json({ ok: true, traffic: live, quota, entries: (await getEntries(env)).length });
}

// 供 CI/其他机器上报质量数据：只需上报口令，不需要 Cloudflare Token
function reportOk(request, env) {
  const tok = env.report_token;
  if (!tok) return false;
  const got = request.headers.get('x-report-token') || '';
  if (got.length !== tok.length) return false;
  let diff = 0;
  for (let i = 0; i < tok.length; i++) diff |= tok.charCodeAt(i) ^ got.charCodeAt(i);
  return diff === 0;
}

async function apiQualityEntries(request, env, url) {
  if (!reportOk(request, env)) return json({ ok: false, msg: '上报口令无效' }, 401);
  const entries = await getEntries(env);
  const preferred = await (async () => {
    try { if (env.KV) return JSON.parse((await env.KV.get('preferred')) || '[]'); } catch {}
    return [];
  })();
  return json({ ok: true, entries: entries.map((e) => ({ name: e.name, host: e.host, uuid: e.uuid, path: e.path || '/' })), preferred });
}

async function apiShortLink(request, env, url) {
  if (!(await isAdmin(request, env))) return json({ ok: false, msg: '未登录' }, 401);
  if (!env.KV) return json({ ok: false, msg: 'KV 未绑定' }, 503);
  const body = await request.json().catch(() => ({}));
  const query = String(body.query || '').replace(/^\?/, '');
  if (!query) return json({ ok: false, msg: '缺少 query' }, 400);
  const tok = [...crypto.getRandomValues(new Uint8Array(6))].map((b) => b.toString(36).padStart(2, '0')).join('').slice(0, 10);
  await env.KV.put('short:' + tok, JSON.stringify({ query, ts: Date.now(), note: String(body.note || '').slice(0, 60) }));
  return json({ ok: true, path: '/s/' + tok, url: `https://${url.host}/s/${tok}`, query });
}

async function notify(env, text, force) {
  const st = await getSettings(env);
  const tg = st.tg_token, chat = st.tg_chat, hook = st.webhook_url;
  if (!tg && !hook) return { ok: false, msg: '未配置通知渠道' };
  if (!force) {
    let last = 0;
    try { last = +(await env.KV.get('notify_last') || 0); } catch {}
    if (Date.now() - last < 600000) return { ok: false, msg: '10 分钟内已通知过（节流）' };
  }
  try { await env.KV.put('notify_last', String(Date.now())); } catch {}
  const results = [];
  if (tg && chat) {
    try {
      const r = await fetch(`https://api.telegram.org/bot${tg}/sendMessage`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ chat_id: chat, text, disable_web_page_preview: true }),
      });
      results.push('telegram:' + (r.ok ? 'ok' : r.status));
    } catch (e) { results.push('telegram:err'); }
  }
  if (hook) {
    try {
      const r = await fetch(hook, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text }) });
      results.push('webhook:' + (r.ok ? 'ok' : r.status));
    } catch (e) { results.push('webhook:err'); }
  }
  return { ok: true, results };
}

async function apiNotifyTest(request, env, url) {
  if (!(await isAdmin(request, env))) return json({ ok: false, msg: '未登录' }, 401);
  const r = await notify(env, `cfmux 测试通知\n时间：${new Date().toLocaleString()}\n如果你看到这条，说明通知通道正常。`, true);
  return json(r);
}

async function apiShortLinks(request, env, url) {
  if (!(await isAdmin(request, env))) return json({ ok: false, msg: '未登录' }, 401);
  if (!env.KV) return json({ ok: false, msg: 'KV 未绑定' }, 503);
  if (request.method === 'DELETE' || url.searchParams.get('del')) {
    const tok = url.searchParams.get('del') || ((await request.json().catch(() => ({}))).token);
    if (!tok) return json({ ok: false, msg: '缺少 token' }, 400);
    await env.KV.delete('short:' + tok);
    return json({ ok: true, deleted: tok });
  }
  const out = [];
  try {
    let cursor = undefined;
    for (let i = 0; i < 5; i++) {
      const page = await env.KV.list({ prefix: 'short:', cursor });
      for (const k of page.keys) {
        const tok = k.name.slice(6);
        let rec = null;
        try { rec = JSON.parse((await env.KV.get(k.name)) || 'null'); } catch {}
        if (rec) out.push({ token: tok, query: rec.query, ts: rec.ts, note: rec.note || '', url: `https://${url.host}/s/${tok}` });
      }
      if (page.list_complete) break;
      cursor = page.cursor;
    }
  } catch (e) { return json({ ok: false, msg: String(e).slice(0, 100) }, 200); }
  out.sort((a, b) => (b.ts || 0) - (a.ts || 0));
  return json({ ok: true, links: out, total: out.length });
}

async function shortLinkRedirect(request, env, url) {
  const tok = url.pathname.slice(3);
  let rec = null;
  try { if (env.KV) rec = JSON.parse((await env.KV.get('short:' + tok)) || 'null'); } catch {}
  if (!rec) return new Response('短链不存在或已失效', { status: 404, headers: { 'content-type': 'text/plain; charset=utf-8' } });
  return Response.redirect(`https://${url.host}/sub?${rec.query}`, 302);
}

async function apiQualityReport(request, env, url) {
  if (!reportOk(request, env)) return json({ ok: false, msg: '上报口令无效' }, 401);
  if (!env.KV) return json({ ok: false, msg: 'KV 未绑定' }, 503);
  let body;
  try { body = await request.json(); } catch { return json({ ok: false, msg: 'bad json' }, 400); }
  const incoming = body && body.quality;
  if (!incoming || typeof incoming !== 'object') return json({ ok: false, msg: '缺少 quality' }, 400);
  let cur = {};
  try { cur = JSON.parse((await env.KV.get('quality')) || '{}'); } catch {}
  let n = 0;
  for (const [k, v] of Object.entries(incoming)) {
    if (!v || typeof v !== 'object' || !v.ts) continue;
    cur[k] = v; n++;
  }
  await env.KV.put('quality', JSON.stringify(cur));

  // 自动晋升：把本轮高分入口写入 preferred（去重、上限 200），实现“自动加入优选”
  let promoted = 0, pruned = 0;
  try {
    const st = await getSettings(env);
    if (st.autoPromote !== false) {
      const minScore = st.promoteMinScore ?? 55;
      const top = Object.entries(cur)
        .filter(([, v]) => v && v.ok && (v.score ?? 0) >= minScore && !v.cooldownUntil)
        .sort((a, b) => (b[1].score ?? 0) - (a[1].score ?? 0))
        .slice(0, 60)
        .map(([k]) => k.split('|')[1])
        .filter(Boolean);
      let pref = [];
      try { pref = JSON.parse((await env.KV.get('preferred')) || '[]'); } catch {}
      const set = new Set(pref.map((x) => (typeof x === 'string' ? x : x.ip)));
      for (const v of top) if (!set.has(v)) { pref.push(v); set.add(v); promoted++; }
      if (promoted) await env.KV.put('preferred', JSON.stringify(pref.slice(-200)));

      // 自动淘汰（保守版）：候选在本轮取「各数据面最低分」，落在底部 25% 才记一次
      // 连续 pruneStrikes 轮（默认 3）且可用候选数 > pruneFloor（默认 60）才移出 preferred
      const alive = Object.entries(cur).filter(([, v]) => v && v.ok && !v.cooldownUntil);
      const byVal = new Map();
      for (const [k, v] of alive) {
        const val = k.split('|')[1]; if (!val) continue;
        const sc = v.score ?? 0;
        byVal.set(val, Math.min(byVal.has(val) ? byVal.get(val) : 1e9, sc));
      }
      const vals = [...byVal.entries()].sort((a, b) => a[1] - b[1]);
      const median = vals.length ? vals[Math.floor(vals.length / 2)][1] : 0;
      const bottomN = Math.max(1, Math.floor(vals.length * 0.25));
      const bottom = new Set(vals.slice(0, bottomN).map(([v]) => v));
      let strikes = {};
      try { strikes = JSON.parse((await env.KV.get('strikes')) || '{}'); } catch {}
      for (const [v] of vals) strikes[v] = bottom.has(v) ? (strikes[v] || 0) + 1 : 0;
      const needStrikes = st.pruneStrikes ?? 3;
      const floor = st.pruneFloor ?? 60;
      const eligible = vals.length > floor;
      const drop = new Set(eligible ? vals.filter(([v]) => (strikes[v] || 0) >= needStrikes).map(([v]) => v) : []);
      if (drop.size) {
        let p2 = [];
        try { p2 = JSON.parse((await env.KV.get('preferred')) || '[]'); } catch {}
        const before = p2.length;
        p2 = p2.filter((x) => !drop.has(typeof x === 'string' ? x : x.ip));
        await env.KV.put('preferred', JSON.stringify(p2));
        let ban = {};
        try { ban = JSON.parse((await env.KV.get('banned')) || '{}'); } catch {}
        const banHours = st.banHours ?? 168;
        for (const v of drop) { ban[v] = { until: Date.now() + banHours * 3600000, ts: Date.now() }; delete strikes[v]; }
        await env.KV.put('banned', JSON.stringify(ban));
        pruned = before - p2.length;
      }
      await env.KV.put('strikes', JSON.stringify(strikes));

      // 解封：本轮分数回到中位数以上且不在底部 25%，解除封禁
      let ban2 = {};
      try { ban2 = JSON.parse((await env.KV.get('banned')) || '{}'); } catch {}
      let unbanned = 0;
      for (const v of Object.keys(ban2)) {
        if (bottom.has(v)) continue;
        const sc = byVal.get(v);
        if (sc !== undefined && sc >= median) { delete ban2[v]; unbanned++; }
      }
      if (unbanned) await env.KV.put('banned', JSON.stringify(ban2));
    }
  } catch {}

  // 记录上报来源与时间
  try { await env.KV.put('report_meta', JSON.stringify({ ts: Date.now(), source: String(body.source || 'ci').slice(0, 40), count: n })); } catch {}
  // 异常告警：本轮上报里失败占比过高，或淘汰量过大
  let alerted = false;
  try {
    const rep = Object.values(incoming).filter((v) => v && typeof v === 'object');
    const dead = rep.filter((v) => v.ok === false).length;
    const ratio = rep.length ? dead / rep.length : 0;
    if (ratio >= 0.4 && rep.length >= 10) {
      const r = await notify(env, `⚠️ cfmux 告警
本轮上报 ${rep.length} 个组合，失败 ${dead} 个（${Math.round(ratio*100)}%）
来源：${body.source || 'unknown'}
时间：${new Date().toLocaleString()}`);
      alerted = r.ok;
    }
  } catch {}
  return json({ ok: true, merged: n, total: Object.keys(cur).length, promoted, pruned, unbanned: typeof unbanned !== 'undefined' ? unbanned : 0, alerted });
}

async function apiStats(request, env, url) {
  const subUrl = new URL(url.toString());
  subUrl.pathname = '/sub';
  subUrl.search = '?target=plain&limit=0&nocache=1';
  const r = await buildSubscription(new Request(subUrl.toString()), env, subUrl);
  const h = r.headers;
  return json({
    ok: true,
    candidates: +(h.get('x-cfmux-total') || 0),
    passed: +(h.get('x-cfmux-pass') || 0),
    deduped: +(h.get('x-cfmux-dedup') || 0),
    nodes: +(h.get('x-cfmux-nodes') || 0),
    minScore: +(h.get('x-cfmux-minscore') || 0),
  });
}

async function apiQuality(request, env, url) {
  const q = await getQuality(env);
  const th = await getThreshold(env);
  const list = Object.entries(q).map(([k, v]) => {
    const [host, ip] = k.split('|');
    return { host, ip, ...v, stale: th.maxAgeHours > 0 && (Date.now() - (v.ts || 0)) / 3600000 > th.maxAgeHours };
  }).sort((a, b) => (b.score ?? -1) - (a.score ?? -1));
  return json({ threshold: th, total: list.length, top: list.slice(0, +(url.searchParams.get('n') || 50)) });
}

async function apiEntries(request, env, url) {
  const { entries, total, wantRegion } = await resolveEntries(request, env, url);
  const byColo = {};
  for (const e of entries) { const k = e.colo || 'UNKNOWN'; (byColo[k] ||= []).push(e.value); }
  return json({ total, shown: entries.length, region: wantRegion || 'ALL', byColo });
}

function jsonText(body, status = 200) {
  return new Response(body, { status, headers: { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' } });
}
function b64(s) { return btoa(unescape(encodeURIComponent(s))); }

function vlessLink(n, url, wsPath) {
  const uuid = n.uuid, host = n.host;
  const p = (n.path || wsPath) + ((n.path || wsPath).includes('?') ? '&' : '?') + 'ed=2048';
  if (n.proto === 'trojan') {
    const q = new URLSearchParams({ security: 'tls', sni: host, type: 'ws', host, path: p });
    return `trojan://${encodeURIComponent(n.tpass)}@${n.value}:443?${q.toString()}#${encodeURIComponent(n.name)}`;
  }
  const q = new URLSearchParams({ encryption: 'none', security: 'tls', sni: host, fp: 'chrome', type: 'ws', host, path: p });
  return `vless://${uuid}@${n.value}:443?${q.toString()}#${encodeURIComponent(n.name)}`;
}
function v2rayLinks(nodes, url, wsPath) { return nodes.map((n) => vlessLink(n, url, wsPath)).join('\n'); }

function qxConfig(nodes, wsPath) {
  // Quantumult X 格式
  return nodes.map((n) => {
    const p = wsPath + (wsPath.includes('?') ? '&' : '?') + 'ed=2048';
    return `vless=${n.value}:443, method=none, password=${n.uuid}, obfs=wss, obfs-host=${n.host}, obfs-uri=${p}, tls-verify=false, fast-open=false, udp-relay=false, tag=${n.name}`;
  }).join('\n');
}
function loonConfig(nodes, wsPath) {
  // Loon 格式
  return nodes.map((n) => {
    const p = wsPath + (wsPath.includes('?') ? '&' : '?') + 'ed=2048';
    return `${n.name} = VLESS,${n.value},443,"${n.uuid}",transport=ws,path=${p},host=${n.host},tls=true,sni=${n.host},skip-cert-verify=true`;
  }).join('\n');
}
function surgeConfig(nodes, wsPath) {
  // Surge 不支持 VLESS；用 Trojan-WS 需要内核开启 et=yes。这里给出可直接用的 Clash 提示
  return '# Surge 不支持 VLESS。请使用 target=clash / singbox / qx / loon。\n# 如需 Surge，可在数据面内核开启 Trojan-WS(et=yes)，并改用 Trojan 订阅。\n';
}
function clashConfig(nodes, url, wsPath) {
  const host = nodes[0]?.host;
  const proxies = nodes.map((n) => `  - {name: "${n.name}", type: vless, server: ${n.value}, port: 443, uuid: ${n.uuid}, tls: true, servername: ${n.host}, network: ws, ws-opts: {path: "${wsPath}?ed=2048", headers: {Host: ${host}}}, udp: false}`);
  const names = nodes.map((n) => `"${n.name}"`).join(', ');
  return ['mixed-port: 7890','allow-lan: false','mode: rule','log-level: warn','proxies:', ...proxies,
    'proxy-groups:',
    `  - {name: "自动优选", type: url-test, proxies: [${names}], url: "http://www.gstatic.com/generate_204", interval: 300, tolerance: 50}`,
    `  - {name: "故障切换", type: fallback, proxies: ["自动优选", ${names}], url: "http://www.gstatic.com/generate_204", interval: 120}`,
    'rules:', '  - MATCH,故障切换'].join('\n');
}

function singboxConfig(nodes, url, wsPath) {
  const host = nodes[0]?.host;
  const outs = nodes.map((n) => ({ type: n.proto === 'trojan' ? 'trojan' : 'vless', tag: n.name, server: n.value, server_port: 443, uuid: n.uuid, password: n.tpass || undefined,
    tls: { enabled: true, server_name: n.host, utls: { enabled: true, fingerprint: 'chrome' } },
    transport: { type: 'ws', path: (n.path || wsPath) + ((n.path || wsPath).includes('?') ? '&' : '?') + 'ed=2048', headers: { Host: n.host } } }));
  return JSON.stringify({
    log: { level: 'warn' },
    inbounds: [{ type: 'mixed', listen: '0.0.0.0', listen_port: 7890 }],
    outbounds: [
      { type: 'urltest', tag: 'auto', outbounds: nodes.map((n) => n.name), url: 'http://www.gstatic.com/generate_204', interval: '5m', tolerance: 50 },
      ...outs, { type: 'direct', tag: 'direct' }],
    route: { final: 'auto' },
  }, null, 1);
}

// ---------- 面板页（Pages 上的可视化控制台）----------
async function panelPage(request, env, url) {
  const base = `https://${url.host}`;
  const host = env.panel_host || url.host;
  const html = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>cfmux 面板</title>
<style>
:root{--bg:#0b1020;--card:#141b2f;--fg:#e6edf7;--dim:#8b9bb4;--acc:#4ade80;--warn:#fbbf24}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.6 -apple-system,"PingFang SC",Segoe UI,sans-serif}
.wrap{max-width:960px;margin:0 auto;padding:24px}
h1{font-size:20px;margin:0 0 4px}.sub{color:var(--dim);margin-bottom:20px}
.card{background:var(--card);border:1px solid #22304d;border-radius:12px;padding:16px;margin-bottom:14px}
.card h2{font-size:14px;margin:0 0 12px;color:var(--dim);font-weight:600;letter-spacing:.5px}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
input,select{background:#0f1526;border:1px solid #22304d;color:var(--fg);border-radius:8px;padding:8px 10px;font-size:13px}
input{flex:1;min-width:180px}
a.btn,button{background:#1d4ed8;border:0;color:#fff;border-radius:8px;padding:8px 14px;font-size:13px;cursor:pointer;text-decoration:none;display:inline-block}
a.btn.alt,button.alt{background:#1f2a44}
code{background:#0f1526;border:1px solid #22304d;border-radius:6px;padding:2px 6px;font-size:12px;word-break:break-all}
table{width:100%;border-collapse:collapse;font-size:13px}td,th{padding:6px 8px;border-bottom:1px solid #1b253c;text-align:left}
th{color:var(--dim);font-weight:500}.ok{color:var(--acc)}.dim{color:var(--dim)}
</style></head><body><div class="wrap">
<h1>cfmux <span class="dim" style="font-size:12px">自研 VLESS-over-WS 边缘代理</span></h1>
<div class="sub">数据面：<code>${host}</code> · 控制面：优选 IP/域名 → 订阅</div>

<div class="card"><h2>订阅链接</h2>
<div class="row"><span class="dim" style="width:70px">通用</span><code style="flex:1">${base}/sub</code><a class="btn alt" target="_blank" href="${base}/sub">打开</a></div>
<div class="row" style="margin-top:6px"><span class="dim" style="width:70px">Clash</span><code style="flex:1">${base}/sub?target=clash</code><a class="btn alt" target="_blank" href="${base}/sub?target=clash">打开</a></div>
<div class="row" style="margin-top:6px"><span class="dim" style="width:70px">sing-box</span><code style="flex:1">${base}/sub?target=singbox</code><a class="btn alt" target="_blank" href="${base}/sub?target=singbox">打开</a></div>
<div class="dim" style="margin-top:8px">参数：<code>limit</code> 节点数(≤300) · <code>region</code> 区域(如 HKG/SIN/NRT) · <code>like</code> 关键字过滤 · <code>host</code> SNI</div>
</div>

<div class="card"><h2>优选资源</h2>
<div class="row"><input id="kw" placeholder="关键字过滤，例如 hk / skk.moe / 104.17"><button onclick="load()">查询</button></div>
<div id="res" class="dim" style="margin-top:10px">加载中…</div>
</div>

<div class="card"><h2>使用说明</h2>
<div class="dim" style="line-height:1.9;font-size:13px">
<b class="ok">第 1 步</b>：复制下面任一订阅地址，导入客户端（Shadowrocket / Clash / sing-box）。<br>
<b class="ok">第 2 步</b>：客户端里开启 <code>url-test</code>（自动测速）或手动选国家/延迟最低的节点。<br>
<b class="ok">第 3 步</b>：连不上时先换节点（节点名里带国家、等级、延迟）。<br><br>
<b>订阅地址参数</b><br>
<code>target</code>：<code>base64</code>(默认) / <code>clash</code> / <code>singbox</code> —— 客户端格式<br>
<code>limit</code>：节点数，<code>0</code>=全部（默认全部，上限 300）<br>
<code>kind</code>：<code>ip</code> 只要优选 IP / <code>domain</code> 只要优选域名<br>
<code>like</code>：关键字筛选，如 <code>like=美国</code>、<code>like=HK</code><br>
<code>perEntry</code>：每个数据面取几个（0=不限）<br>
<code>dedup</code>：<code>0</code> 关闭去重（默认按真实出口 IP 去重）<br>
<code>nocache</code>：<code>1</code> 跳过 60 秒缓存，改配置后立刻生效<br><br>
<b>常见问题</b><br>
· <b>节点少？</b> 只有通过质量门槛（评分/吞吐/新鲜度）的节点才会出现，宁缺毋滥；后台可调门槛。<br>
· <b>延迟高？</b> 名称里是实测延迟；优先选同国家、延迟低的；也可用 <code>like=</code> 只订某国。<br>
· <b>某节点突然连不上？</b> 质量引擎会连续失败 3 次自动熔断并从订阅移除，恢复后自动回归。<br>
</div></div>
<div class="card"><h2>诊断</h2><div class="row"><a class="btn alt" target="_blank" href="${base}/healthz">/healthz</a><a class="btn alt" target="_blank" href="${base}/diag">/diag</a></div>
<div id="diag" class="dim" style="margin-top:8px"></div></div>
</div>
<script>
async function load(){
  const kw=document.getElementById('kw').value.trim();
  const r=await fetch('/api/entries?limit=300'+(kw?'&like='+encodeURIComponent(kw):''));
  const d=await r.json(); const el=document.getElementById('res');
  const rows=Object.entries(d.byColo||{}).map(([k,v])=>'<tr><td class="ok">'+k+'</td><td>'+v.length+'</td><td class="dim">'+v.slice(0,4).join(', ')+'</td></tr>').join('');
  el.innerHTML='<div class="dim" style="margin-bottom:8px">共 '+d.total+' 条，显示 '+d.shown+'</div><table><tr><th>区域</th><th>数量</th><th>样例</th></tr>'+rows+'</table>';
}
async function diag(){ try{ const d=await (await fetch('/diag')).json();
  document.getElementById('diag').innerHTML='colo=<b>'+d.colo+'</b> · sockets=<b class="'+(d.sockets&&d.sockets.ok?'ok':'')+'">'+(d.sockets&&d.sockets.ok?'OK':'FAIL')+'</b> · uuidSet=<b>'+(d.uuidSet?'yes':'no')+'</b>';
}catch(e){ document.getElementById('diag').textContent='诊断失败'; } }
load(); diag();
</script></body></html>`;
  return new Response(html, { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' } });
}

// ---------- 后台管理 ----------
const ADMIN_COOKIE = 'cfmux_admin';
const SEVEN_DAYS = 7 * 24 * 3600;

async function sha256hex(str) {
  const buf = await crypto.subtle.digest('SHA-256', enc.encode(str));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, '0')).join('');
}
function getCookie(req, name) {
  const raw = req.headers.get('Cookie') || '';
  for (const part of raw.split(';')) {
    const i = part.indexOf('=');
    if (i < 0) continue;
    if (part.slice(0, i).trim() === name) return part.slice(i + 1).trim();
  }
  return '';
}
async function adminToken(env) {
  if (!env.admin_password) return '';
  return sha256hex('cfmux|' + env.admin_password);
}
async function isAdmin(req, env) {
  const want = await adminToken(env);
  if (!want) return false;
  const got = getCookie(req, ADMIN_COOKIE);
  if (!got || got.length !== want.length) return false;
  let diff = 0;
  for (let i = 0; i < want.length; i++) diff |= want.charCodeAt(i) ^ got.charCodeAt(i);
  return diff === 0;
}
async function getSettings(env) {
  const base = { sources: '', limit: 0, perEntry: 0, minScore: 0, minTput: 0, maxAgeHours: 168, requireQuality: true, host: '' };
  try {
    if (env.KV && env.KV.get) {
      const raw = await env.KV.get('settings');
      if (raw) Object.assign(base, JSON.parse(raw));
    }
  } catch {}
  return base;
}

function loginHtml(needSetup) {
  const msg = needSetup
    ? `<div class="warn">⚠️ 未设置 <code>admin_password</code> 环境变量，后台不可用。<br>请在 Cloudflare 控制台 → Pages 项目 → 设置 → 变量 中添加 <code>admin_password</code>，然后重新部署。</div>`
    : '';
  return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>cfmux 后台登录</title><style>
body{margin:0;background:#0b1020;color:#e6edf7;font:14px/1.6 -apple-system,"PingFang SC",sans-serif;display:flex;min-height:100vh;align-items:center;justify-content:center}
.box{background:#141b2f;border:1px solid #22304d;border-radius:14px;padding:28px;width:320px}
h1{font-size:17px;margin:0 0 18px}input{width:100%;background:#0f1526;border:1px solid #22304d;color:#e6edf7;border-radius:8px;padding:10px;font-size:14px;box-sizing:border-box}
button{width:100%;margin-top:12px;background:#1d4ed8;border:0;color:#fff;border-radius:8px;padding:10px;font-size:14px;cursor:pointer}
.warn{background:#3a2a12;border:1px solid #6b4a14;color:#fbbf24;border-radius:8px;padding:10px;font-size:12px;margin-bottom:14px}
code{background:#0f1526;padding:1px 4px;border-radius:4px}</style></head><body><div class="box"><h1>cfmux 后台</h1>${msg}
<form method="POST" action="/admin/login"><input type="password" name="password" placeholder="管理员密码" autofocus><button>登录</button></form>
</div></body></html>`;
}

async function adminPage(request, env, url) {
  if (!env.admin_password) return new Response(loginHtml(true), { headers: { 'content-type': 'text/html; charset=utf-8' } });
  if (!(await isAdmin(request, env))) return new Response(loginHtml(false), { headers: { 'content-type': 'text/html; charset=utf-8' } });
  return new Response(DASHBOARD_HTML, { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' } });
}

const DASHBOARD_HTML = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>cfmux 管理后台</title><style>
*{box-sizing:border-box}body{margin:0;background:#080d19;color:#e6edf7;font:14px/1.6 -apple-system,"PingFang SC",Segoe UI,sans-serif}
.app{display:flex;min-height:100vh}
aside{width:190px;background:#0d1424;border-right:1px solid #1b253c;padding:18px 0;flex-shrink:0}
aside h1{font-size:15px;margin:0 18px 18px;letter-spacing:.5px}
aside h1 span{display:block;font-size:11px;color:#5f6b80}
nav a{display:block;padding:9px 18px;color:#8b9bb4;text-decoration:none;font-size:13px;border-left:2px solid transparent}
nav a:hover{color:#e6edf7;background:#111a2e}nav a.on{color:#4ade80;border-left-color:#4ade80;background:#0f1a2e}
main{flex:1;padding:22px 26px;min-width:0}
h2{font-size:16px;margin:0 0 14px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:18px}
.card{background:#0f1729;border:1px solid #1b253c;border-radius:12px;padding:14px}
.card .k{font-size:11px;color:#8b9bb4;letter-spacing:.5px}.card .v{font-size:22px;font-weight:600;margin-top:4px}
.card .v small{font-size:12px;color:#8b9bb4;font-weight:400}
table{width:100%;border-collapse:collapse;font-size:12.5px;background:#0f1729;border-radius:10px;overflow:hidden}
th{color:#8b9bb4;font-weight:500;text-align:left;padding:9px 10px;background:#111a2e;position:sticky;top:0}
td{padding:8px 10px;border-top:1px solid #16203a}
tr:hover td{background:#111c31}
.badge{display:inline-block;padding:1px 7px;border-radius:99px;font-size:11px;border:1px solid}
.S{color:#f0abfc;border-color:#7e22ce;background:#2a1035}.A{color:#4ade80;border-color:#166534;background:#0d2417}
.B{color:#fbbf24;border-color:#78350f;background:#2a1f08}.C{color:#f87171;border-color:#7f1d1d;background:#2a1010}
.ok{color:#4ade80}.bad{color:#f87171}.dim{color:#8b9bb4}
.btn{background:#1d4ed8;border:0;color:#fff;border-radius:7px;padding:6px 12px;font-size:12px;cursor:pointer}
.btn.alt{background:#1c2740}.btn.danger{background:#7f1d1d}.btn:hover{filter:brightness(1.15)}
input,select,textarea{background:#0b1220;border:1px solid #1b253c;color:#e6edf7;border-radius:7px;padding:7px 9px;font-size:12.5px}
textarea{width:100%;min-height:120px;font:12px/1.5 ui-monospace,Menlo,monospace}
.toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:12px}
.toolbar input{flex:1;min-width:160px}
#toast{position:fixed;left:50%;top:26px;transform:translateX(-50%) translateY(-12px);background:#14532d;border:1px solid #22c55e;color:#dcfce7;padding:12px 20px;border-radius:10px;font-size:14px;font-weight:500;opacity:0;transition:.25s;pointer-events:none;z-index:99;box-shadow:0 8px 30px rgba(0,0,0,.5)}
#toast.on{opacity:1;transform:translateX(-50%) translateY(0)}
#toast.err{background:#450a0a;border-color:#ef4444;color:#fee2e2}
.status{font-size:12px;margin-left:8px}
.status.ok{color:#4ade80}.status.err{color:#f87171}
.btn:disabled{opacity:.55;cursor:default}
code{background:#0b1220;padding:1px 5px;border-radius:4px;font-size:11.5px;word-break:break-all}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:900px){.grid2{grid-template-columns:1fr}aside{width:60px}aside h1 span,aside nav a{padding-left:4px;font-size:0}
aside h1{font-size:0}}
</style></head><body><div class="app">
<aside><h1>cfmux<span>管理后台</span></h1>
<nav>
<a href="#" data-tab="overview" class="on">📊 概览</a>
<a href="#" data-tab="nodes">🛰 节点质量</a>
<a href="#" data-tab="subs">📥 订阅</a>
<a href="#" data-tab="preferred">⭐ 优选</a>
<a href="#" data-tab="kernel">🧠 内核</a>
<a href="#" data-tab="traffic">📈 流量</a>
<a href="#" data-tab="entries">🧩 数据面</a>
<a href="#" data-tab="settings">⚙️ 设置</a>
<a href="/panel" target="_blank">🌐 公开状态页</a>
<a href="/admin/logout">🚪 退出</a>
</nav></aside>
<main>
<section id="tab-overview">
  <h2>概览</h2>
  <div class="cards" id="cards"></div>
  <div class="cards" id="funnel" style="margin-top:-6px"></div>
  <div class="card"><div class="k">最近质量趋势（评分）</div><div id="spark" style="margin-top:8px"></div></div>
  <h2 style="margin-top:20px">Top 节点</h2>
  <div id="topNodes"></div>
</section>

<section id="tab-nodes" hidden>
  <h2>节点质量</h2>
  <div class="toolbar">
    <input id="q-filter" placeholder="筛选：国家 / IP / 数据面">
    <select id="q-tier"><option value="">全部等级</option><option>S</option><option>A</option><option>B</option><option>C</option></select>
    <select id="q-state"><option value="">全部状态</option><option value="ok">正常</option><option value="dead">失败</option><option value="cool">熔断中</option></select>
    <button class="btn alt" onclick="renderNodes()">刷新</button>
  </div>
  <div id="nodeTable"></div>
</section>

<section id="tab-subs" hidden>
  <h2>订阅</h2>
  <div class="toolbar"><span class="dim">点“复制”后粘贴到客户端；带 ?nocache=1 可绕过 60 秒缓存</span></div>
  <div class="grid2" id="sub-list"></div>
  <div class="card" style="margin-top:12px"><div class="k">订阅短链（可随时失效）</div>
    <div class="toolbar" style="margin-top:8px">
      <input id="sl-query" placeholder="参数，如 target=clash&amp;limit=50&amp;cc=SG">
      <button class="btn" onclick="makeShort(this)">生成短链</button>
    </div>
    <div id="sl-out" class="dim" style="font-size:12px;margin-top:8px">生成后可分发，删除即失效</div>
    <div id="sl-list" style="margin-top:10px"></div>
  </div>
  <div class="card" style="margin-top:12px"><div class="k">二维码（手机扫一扫）</div><div id="qr" style="margin-top:10px"></div>
    <div class="dim" style="font-size:11px;margin-top:6px">通用订阅，客户端自动识别</div></div>
</section>

<section id="tab-preferred" hidden>
  <h2>优选列表</h2>
  <div class="toolbar">
    <input id="imp-url" placeholder="粘贴一个「一行一个」的 IP/域名 列表地址，例如 https://bestcf.fxxk.dedyn.io/cf_domains.txt">
    <button class="btn" onclick="importPreferred(this)">从网址导入</button>
    <button class="btn alt" onclick="triggerRun(this)">手动触发测速</button>
  </div>
  <div class="dim" id="prefHint" style="margin-bottom:8px"></div>
  <textarea id="pref-text" style="min-height:300px"></textarea>
  <div style="margin-top:10px"><button class="btn" onclick="savePreferred(this)">保存优选列表</button><span id="st-pref" class="status"></span>
    <button class="btn alt" onclick="loadPreferred(this)">重新读取</button></div>
</section>

<section id="tab-kernel" hidden>
  <h2>内核配置</h2>
  <div class="toolbar">
    <select id="k-plane"></select>
    <button class="btn" onclick="loadKernel(this)">读取配置</button>
    <button class="btn" onclick="saveKernel(this)">保存到数据面</button>
    <span class="dim" id="k-hint"></span>
  </div>
  <div class="grid2">
    <div class="card"><div class="k">协议开关</div>
      <div class="toolbar" style="flex-direction:column;align-items:stretch;margin-top:8px">
        <label class="dim" style="font-size:12px">VLESS-WS (ev)<select id="k-ev"><option>yes</option><option>no</option></select></label>
        <label class="dim" style="font-size:12px">Trojan-WS (et)<select id="k-et"><option>yes</option><option>no</option></select></label>
        <label class="dim" style="font-size:12px">VLESS-XHTTP (ex)<select id="k-ex"><option>yes</option><option>no</option></select></label>
        <label class="dim" style="font-size:12px">ECH (ech)<select id="k-ech"><option>yes</option><option>no</option></select></label>
      </div>
    </div>
    <div class="card"><div class="k">参数</div>
      <div class="toolbar" style="flex-direction:column;align-items:stretch;margin-top:8px">
        <label class="dim" style="font-size:12px">自定义路径 (d)<input id="k-d" placeholder="留空=默认"></label>
        <label class="dim" style="font-size:12px">ProxyIP (p)<input id="k-p" placeholder="如 1.2.3.4 或域名"></label>
        <label class="dim" style="font-size:12px">Trojan 密码 (tp)<input id="k-tp"></label>
        <label class="dim" style="font-size:12px">自定义 DNS<input id="k-dns"></label>
        <label class="dim" style="font-size:12px">ECH 域名<input id="k-echdom"></label>
        <label class="dim" style="font-size:12px">伪装首页 (homepage)<input id="k-home"></label>
        <label class="dim" style="font-size:12px">订阅转换后端 (s)<input id="k-s"></label>
      </div>
    </div>
  </div>
  <div class="card"><div class="k">原始配置 JSON（高级：可直接改任何字段）</div>
    <textarea id="k-raw" style="min-height:220px"></textarea></div>
</section>

<section id="tab-traffic" hidden>
  <h2>流量统计</h2>
  <div class="cards" id="t-cards"></div>
  <div class="card"><div class="k">近 7 天请求量（本机计数，近似）</div><div id="t-chart" style="margin-top:10px"></div></div>
  <div class="card"><div class="k">明细</div><div id="t-table"></div></div>
</section>

<section id="tab-entries" hidden>
  <h2>数据面</h2>
  <div class="toolbar"><button class="btn" onclick="addEntry()">+ 添加数据面</button><button class="btn alt" onclick="saveEntries(this)">保存</button><span id="st-entries" class="status"></span><span class="dim" id="entryHint"></span></div>
  <div id="entryTable"></div>
</section>

<section id="tab-settings" hidden>
  <h2>设置</h2>
  <div class="grid2">
    <div class="card"><div class="k">质量门槛</div>
      <div class="toolbar" style="margin-top:10px;flex-direction:column;align-items:stretch">
        <label class="dim" style="font-size:12px">最低评分 minScore<input id="f-minScore" type="number"></label>
        <label class="dim" style="font-size:12px">最低吞吐 KB/s minTput<input id="f-minTput" type="number"></label>
        <label class="dim" style="font-size:12px">数据有效期(小时) maxAgeHours<input id="f-maxAgeHours" type="number"></label>
        <label class="dim" style="font-size:12px">每入口取几个 perEntry<input id="f-perEntry" type="number"></label>
        <label class="dim" style="font-size:12px">必须有实测 requireQuality<select id="f-requireQuality"><option>true</option><option>false</option></select></label>
        <label class="dim" style="font-size:12px">陈旧数据保留天数 softExpireDays<input id="f-softExpireDays" type="number"></label>
        <label class="dim" style="font-size:12px">淘汰所需轮次 pruneStrikes<input id="f-pruneStrikes" type="number"></label>
        <label class="dim" style="font-size:12px">淘汰下限 pruneFloor（少于此数不淘汰）<input id="f-pruneFloor" type="number"></label>
        <label class="dim" style="font-size:12px">淘汰封禁小时 banHours<input id="f-banHours" type="number"></label>
        <label class="dim" style="font-size:12px">自动晋升分数 promoteMinScore<input id="f-promoteMinScore" type="number"></label>
        <label class="dim" style="font-size:12px">默认节点数 limit<input id="f-limit" type="number"></label>
      </div>
    </div>
    <div class="card"><div class="k">通知（Telegram / Webhook）</div>
      <div class="toolbar" style="flex-direction:column;align-items:stretch;margin-top:10px">
        <label class="dim" style="font-size:12px">Telegram Bot Token<input id="f-tg_token" placeholder="123456:ABC..."></label>
        <label class="dim" style="font-size:12px">Telegram Chat ID<input id="f-tg_chat" placeholder="-1001234567890"></label>
        <label class="dim" style="font-size:12px">Webhook URL<input id="f-webhook_url" placeholder="https://..."></label>
      </div>
      <div style="margin-top:10px"><button class="btn alt" onclick="testNotify(this)">发送测试通知</button><span id="st-notify" class="status"></span></div>
    </div>
    <div class="card"><div class="k">优选源（逗号分隔 URL）</div><textarea id="f-sources" style="margin-top:10px"></textarea>
      <div class="k" style="margin-top:10px">区域映射 colos（JSON）</div><textarea id="f-colos"></textarea>
    </div>
  </div>
  <div style="margin-top:12px"><button class="btn" onclick="saveSettings(this)">保存设置</button><span id="st-settings" class="status"></span></div>
</section>
</main></div><div id="toast"></div>
<script>
let STATE = null;
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s == null ? '' : s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
function toast(msg, isErr){ const t=$('toast'); t.textContent=(isErr?'✗ ':'✓ ')+msg; t.className='on'+(isErr?' err':''); clearTimeout(window.__tt); window.__tt=setTimeout(()=>{t.className='';},3000); }
function status(el, msg, ok){ if(!el) return; el.textContent = msg; el.className = 'status ' + (ok?'ok':'err'); }
function busy(btn, label, fn){
  const old = btn ? btn.textContent : '';
  if (btn) { btn.disabled = true; btn.textContent = label || '处理中…'; }
  return Promise.resolve().then(fn).finally(()=>{ if (btn) { btn.disabled=false; btn.textContent = old; } });
}
function copyById(el){ copy(el.dataset.copy || el.dataset.copytext); }
async function makeShort(btn){
  const q = ($('sl-query').value || '').trim();
  return busy(btn, '生成中…', async () => {
    const d = await post('/api/shortlink', { query: q || 'target=base64' });
    if (!d.ok) { toast('生成失败：' + (d.msg||''), true); return; }
    $('sl-out').innerHTML = '<code style="display:block;padding:8px;margin-bottom:8px">' + d.url + '</code>'
      + '<button class="btn alt" id="sl-copy">复制短链</button>';
    $('sl-copy').onclick = function(){ copyText(d.url); toast('短链已复制'); };
    toast('短链已生成');
    loadShortLinks();
  });
}
async function loadShortLinks(){
  const el = $('sl-list'); if (!el) return;
  const d = await (await fetch('/api/shortlinks')).json();
  if (!d.ok) { el.innerHTML = ''; return; }
  if (!d.links.length) { el.innerHTML = '<div class="dim" style="font-size:12px">还没有短链</div>'; return; }
  el.innerHTML = '<table><tr><th>短链</th><th>参数</th><th>创建时间</th><th>操作</th></tr>'
    + d.links.map((x,i) => '<tr><td><a href="'+x.url+'" target="_blank">/s/'+x.token+'</a></td><td class="dim">'
      + esc(x.query) + '</td><td class="dim">' + new Date(x.ts).toLocaleString() + '</td><td>'
      + '<button class="btn alt" data-copytext="'+x.url+'" onclick="copyById(this)">复制</button> '
      + '<button class="btn danger" data-tok="'+x.token+'" onclick="delShort(this)">删除</button></td></tr>').join('')
    + '</table>';
}
async function delShort(btn){
  const tok = btn.dataset.tok;
  const d = await post('/api/shortlinks?del=' + encodeURIComponent(tok), {});
  toast(d.ok ? '已删除' : ('删除失败：' + (d.msg||'')), !d.ok);
  if (d.ok) loadShortLinks();
}
function copy(id){
  const txt = $(id).textContent;
  const done = () => toast('已复制到剪贴板');
  const fail = () => toast('复制失败，请手动选中复制', true);
  try {
    if (navigator.clipboard && navigator.clipboard.writeText) { navigator.clipboard.writeText(txt).then(done).catch(()=>{legacy();}); return; }
  } catch(e) {}
  legacy();
  function legacy(){
    try {
      const ta = document.createElement('textarea'); ta.value = txt; ta.style.position='fixed'; ta.style.left='-9999px';
      document.body.appendChild(ta); ta.select(); const ok = document.execCommand('copy'); document.body.removeChild(ta);
      ok ? done() : fail();
    } catch(e) { fail(); }
  }
}
document.querySelectorAll('nav a[data-tab]').forEach(a=>a.addEventListener('click',(e)=>{
  e.preventDefault();
  document.querySelectorAll('nav a').forEach(x=>x.classList.remove('on')); a.classList.add('on');
  ['overview','nodes','subs','preferred','kernel','traffic','entries','settings'].forEach(t=>$('tab-'+t).hidden = (t!==a.dataset.tab));
  if (a.dataset.tab==='traffic') loadTraffic().catch(()=>{});
  if (a.dataset.tab==='kernel') { renderPlanes(); if (!$('k-raw').value) loadKernel().catch(()=>{}); }
}));

async function load(){
  const r = await fetch('/api/state'); STATE = await r.json();
  EDIT.entries = JSON.parse(JSON.stringify(STATE.entries||[]));   // 只在加载时初始化
  renderCards(); renderSpark(); renderTop(); renderNodes(); renderSubs(); renderEntries(); fillSettings(); renderPreferred();
  renderPlanes(); loadTraffic().catch(()=>{}); renderFunnel().catch(()=>{}); loadShortLinks().catch(()=>{});
}
async function renderFunnel(){
  try {
    const d = await (await fetch('/api/stats')).json();
    if (!d.ok) return;
    $('funnel').innerHTML = '<div class="card" style="grid-column:1/-1"><div class="k">节点生成漏斗（为什么“达标”多、“订阅”少）</div>'
      + '<div style="margin-top:8px;font-size:13px;line-height:2">'
      + '候选 <b>' + d.candidates + '</b> （数据面 × 优选入口）'
      + ' → 通过质量门槛 <b class="ok">' + d.passed + '</b>'
      + ' → 去重(同出口IP) <b class="bad">-' + d.deduped + '</b>'
      + ' → 最终输出 <b class="ok">' + d.nodes + '</b></div>'
      + '<div class="dim" style="font-size:12px;margin-top:4px">去重后同一出口 IP 只保留评分最高的一条；这是“订阅比达标数少”的主要原因。</div></div>';
  } catch(e) {}
}
function renderCards(){
  const q = STATE.quality || {}; const arr = Object.values(q);
  const ok = arr.filter(x=>x.ok);
  const cool = arr.filter(x=>x.cooldownUntil && x.cooldownUntil>Date.now());
  const avg = ok.length ? (ok.reduce((s,x)=>s+(x.score||0),0)/ok.length).toFixed(1) : '-';
  const best = ok.length ? Math.max(...ok.map(x=>x.score||0)).toFixed(1) : '-';
  const tput = ok.length ? Math.round(ok.reduce((s,x)=>s+(x.tput||0),0)/ok.length) : 0;
  const cards = [
    ['达标节点', ok.length + ' <small>/ ' + arr.length + '</small>'],
    ['平均评分', avg], ['最高评分', best], ['平均吞吐', tput + ' <small>KB/s</small>'],
    ['熔断中', cool.length], ['数据面', (STATE.entries||[]).length],
  ];
  const cc = {};
  arr.filter(x=>x.ok && x.cc).forEach(x=>{ cc[x.cc]=(cc[x.cc]||0)+1; });
  const chips = Object.entries(cc).sort((a,b)=>b[1]-a[1]).map(([k,v])=>'<span class="badge A">'+(STATE.ccNames[k]||k)+' '+v+'</span>').join(' ');
  $('cards').innerHTML = cards.map(([k,v])=>'<div class="card"><div class="k">'+k+'</div><div class="v">'+v+'</div></div>').join('')
    + '<div class="card" style="grid-column:1/-1"><div class="k">国家分布（实测）</div><div style="margin-top:6px;line-height:2">'+(chips||'<span class="dim">暂无</span>')+'</div></div>';
}
function renderSpark(){
  const h = STATE.history || [];
  if (h.length < 2) { $('spark').innerHTML = '<span class="dim">暂无趋势数据（每次刷新会追加一个点）</span>'; return; }
  const w=560,ht=80,pad=4;
  const vals=h.map(x=>x.avg||0);
  const hi=Math.max(...vals), lo=Math.min(...vals);
  const padv=Math.max(2,(hi-lo)*0.25); const max=hi+padv, min=Math.max(0,lo-padv);
  const pts=vals.map((v,i)=>[pad+i*(w-2*pad)/(vals.length-1), ht-pad-((v-min)/(max-min||1))*(ht-2*pad)]);
  const d=pts.map((p,i)=>(i?'L':'M')+p[0].toFixed(1)+' '+p[1].toFixed(1)).join(' ');
  $('spark').innerHTML='<svg width="'+w+'" height="'+ht+'" style="max-width:100%"><path d="'+d+'" fill="none" stroke="#4ade80" stroke-width="1.6"/>'+
    pts.map(p=>'<circle cx="'+p[0].toFixed(1)+'" cy="'+p[1].toFixed(1)+'" r="2" fill="#4ade80"/>').join('')+
    '<text x="4" y="10" fill="#5f6b80" font-size="10">max '+max.toFixed(0)+'</text></svg>';
}
function nodeRows(){
  const q = STATE.quality || {};
  return Object.entries(q).map(([k,v])=>{
    const [host, ip] = k.split('|');
    const cool = v.cooldownUntil && v.cooldownUntil > Date.now();
    return { key:k, host, ip, ...v, cool };
  });
}
function stateOf(n){ if (n.cool) return '<span class="bad">熔断</span>'; return n.ok ? '<span class="ok">正常</span>' : '<span class="bad">失败</span>'; }
function renderTop(){
  const arr = nodeRows().filter(n=>n.ok && !n.cool).sort((a,b)=>(b.score||0)-(a.score||0)).slice(0,6);
  $('topNodes').innerHTML = table(arr, ['国家','数据面','入口 IP','等级','评分','延迟','吞吐'], (n)=>[
    n.cc ? (STATE.ccNames[n.cc]||n.cc) : (n.country||'-'),
    n.host.replace('.pages.dev',''), n.ip,
    '<span class="badge '+(n.tier||'C')+'">'+(n.tier||'-')+'</span>',
    (n.score||0).toFixed(1), (n.lat||'-')+'ms', (n.tput||0)+' KB/s'
  ]);
}
function renderNodes(){
  const kw = ($('q-filter')?.value||'').toLowerCase();
  const tier = $('q-tier')?.value||''; const st = $('q-state')?.value||'';
  let arr = nodeRows().sort((a,b)=>(b.score||0)-(a.score||0));
  if (kw) arr = arr.filter(n=>((n.ip+' '+n.host+' '+(n.cc||'')+' '+(n.country||'')+' '+((STATE.ccNames||{})[n.cc]||'')).toLowerCase().includes(kw)));
  if (tier) arr = arr.filter(n=>n.tier===tier);
  if (st==='ok') arr=arr.filter(n=>n.ok&&!n.cool); if (st==='dead') arr=arr.filter(n=>!n.ok); if (st==='cool') arr=arr.filter(n=>n.cool);
  const html = table(arr, ['国家','数据面','入口','等级','评分','延迟','抖动','吞吐','失败','状态','操作'], (n)=>[
    n.cc ? (STATE.ccNames[n.cc]||n.cc) : (n.country||'-'),
    n.host.replace('.pages.dev',''), n.ip,
    '<span class="badge '+(n.tier||'C')+'">'+(n.tier||'-')+'</span>',
    (n.score||0).toFixed(1), (n.lat||'-')+'ms', (n.jitter||0)+'ms', (n.tput||0)+' KB/s',
    (n.fails||0), stateOf(n),
    '<button class="btn alt" data-act="pin" data-key="'+n.key+'" onclick="doAct(this)">置顶</button> ' +
    (n.cool ? '<button class="btn alt" data-act="unban" data-key="'+n.key+'" onclick="doAct(this)">解除</button>'
            : '<button class="btn danger" data-act="ban" data-key="'+n.key+'" onclick="doAct(this)">熔断</button>')
  ]);
  $('nodeTable').innerHTML = html;
}
function table(arr, cols, map){
  if (!arr.length) return '<div class="card dim">无数据</div>';
  return '<div style="max-height:60vh;overflow:auto"><table><tr>'+cols.map(c=>'<th>'+c+'</th>').join('')+'</tr>'+
    arr.map(n=>'<tr>'+map(n).map(c=>'<td>'+c+'</td>').join('')+'</tr>').join('')+'</table></div>';
}
const SUB_FORMATS = [
  ['通用 Base64（V2Ray / Shadowrocket / v2rayN）', '/sub'],
  ['Clash / Clash Meta / Stash / Mihomo', '/sub?target=clash'],
  ['sing-box', '/sub?target=singbox'],
  ['原始 URI 列表（纯文本）', '/sub?target=plain'],
  ['JSON 节点列表（自建客户端用）', '/sub?target=json'],
  ['Quantumult X', '/sub?target=qx'],
  ['Loon', '/sub?target=loon'],
];
function renderSubs(){
  const base = STATE.base;
  $('sub-list').innerHTML = SUB_FORMATS.map(([label, path], i) =>
    '<div class="card"><div class="k">'+label+'</div>'+
    '<code id="sl-'+i+'" style="display:block;margin-top:8px;word-break:break-all">'+base+path+'</code>'+
    '<div style="margin-top:8px"><button class="btn alt" data-copy="sl-'+i+'" onclick="copyById(this)">复制</button> '+
    '<a class="btn alt" target="_blank" href="'+base+path+'" style="text-decoration:none">打开</a></div></div>'
  ).join('');
  const url = base + '/sub';
  const box = $('qr');
  box.innerHTML = '<div id="qr-local" data-url="'+url+'" style="width:200px;height:200px;background:#fff;border-radius:8px;padding:10px;display:flex;align-items:center;justify-content:center"><style>#qr-local svg{width:100%;height:100%}</style></div>';
  const fallback = () => { box.innerHTML = '<img alt="qr" style="width:180px;height:180px;background:#fff;padding:6px;border-radius:8px" src="https://api.qrserver.com/v1/create-qr-code/?size=180x180&data=' + encodeURIComponent(url) + '">'; };
  if (window.qrcode) {
    try { const qr = qrcode(0, 'M'); qr.addData(url); qr.make(); $('qr-local').innerHTML = qr.createSvgTag({ cellSize: 6, margin: 2 }); } catch (e) { fallback(); }
  } else {
    const sc = document.createElement('script');
    sc.src = 'https://cdn.jsdelivr.net/npm/qrcode-generator@1.4.4/qrcode.js';
    sc.onload = () => { try { const qr = qrcode(0, 'M'); qr.addData(url); qr.make(); $('qr-local').innerHTML = qr.createSvgTag({ cellSize: 6, margin: 2 }); } catch (e) { fallback(); } };
    sc.onerror = fallback;
    document.head.appendChild(sc);
  }
}
function renderEntries(){
  const e = EDIT.entries || [];
  $('entryTable').innerHTML = e.length ? '<table><tr><th>名称</th><th>域名</th><th>UUID</th><th>路径</th><th>测速</th><th></th></tr>'+
    e.map((x,i)=>'<tr><td><input value="'+esc(x.name)+'" oninput="EDIT.entries['+i+'].name=this.value" style="width:90px"></td>'+
    '<td><input value="'+esc(x.host)+'" oninput="EDIT.entries['+i+'].host=this.value" style="width:230px"></td>'+
    '<td><input value="'+esc(x.uuid)+'" oninput="EDIT.entries['+i+'].uuid=this.value" style="width:290px"></td>'+
    '<td><input value="'+esc(x.path||'/')+'" oninput="EDIT.entries['+i+'].path=this.value" style="width:50px"></td>'+
    (function(){ const st=(STATE.entryStats||{})[x.name]||(STATE.entryStats||{})[x.host]; return '<td>'+(st? (st.ok>0?'<span class="ok">'+st.ok+'/'+st.measured+'</span>':'<span class="bad">0/'+st.measured+'</span>') : '<span class="bad">未测速</span>')+'</td>'; })()+
    '<td><button class="btn danger" onclick="delEntry('+i+')">删</button></td></tr>').join('')+'</table>'
    : '<div class="card dim">还没有数据面（点上面的「+ 添加数据面」）</div>';
}
function addEntry(){ EDIT.entries.push({name:'core'+((EDIT.entries||[]).length+1),host:'',uuid:'',path:'/'}); renderEntries(); }
function delEntry(i){ EDIT.entries.splice(i,1); renderEntries(); }
const EDIT = { entries: [] };
function fillSettings(){
  const s = STATE.settings||{};
  $('f-minScore').value = s.minScore ?? 0; $('f-minTput').value = s.minTput ?? 0;
  $('f-maxAgeHours').value = s.maxAgeHours ?? 168; $('f-perEntry').value = s.perEntry ?? 0;
  $('f-requireQuality').value = (s.requireQuality===false)?'false':'true'; $('f-limit').value = s.limit ?? 0;
  $('f-softExpireDays').value = s.softExpireDays ?? 30; $('f-pruneStrikes').value = s.pruneStrikes ?? 4;
  $('f-pruneFloor').value = s.pruneFloor ?? 120; $('f-banHours').value = s.banHours ?? 72;
  $('f-promoteMinScore').value = s.promoteMinScore ?? 55;
  $('f-tg_token').value = s.tg_token||''; $('f-tg_chat').value = s.tg_chat||''; $('f-webhook_url').value = s.webhook_url||'';
  $('f-sources').value = s.sources||''; $('f-colos').value = JSON.stringify(STATE.colos||{}, null, 1);
}
async function post(url, body){ const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}); return r.json(); }
async function saveSettings(btn){
  return busy(btn, '保存中…', async () => {
    const body = { minScore:+$('f-minScore').value, minTput:+$('f-minTput').value, maxAgeHours:+$('f-maxAgeHours').value,
      perEntry:+$('f-perEntry').value, requireQuality:$('f-requireQuality').value, limit:+$('f-limit').value,
      sources:$('f-sources').value, colos:$('f-colos').value,
      softExpireDays:+$('f-softExpireDays').value, pruneStrikes:+$('f-pruneStrikes').value,
      pruneFloor:+$('f-pruneFloor').value, banHours:+$('f-banHours').value,
      promoteMinScore:+$('f-promoteMinScore').value,
      tg_token:$('f-tg_token').value, tg_chat:$('f-tg_chat').value, webhook_url:$('f-webhook_url').value };
    const el = $('st-settings'); status(el, '保存中…', true);
    const d = await post('/admin/save', body);
    if (d.ok) { status(el, '已保存 ' + new Date().toLocaleTimeString(), true); toast('设置已保存并生效'); await load(); }
    else { status(el, '保存失败：' + (d.msg||'未知错误'), false); toast('保存失败：' + (d.msg||'未知错误'), true); }
  });
}
async function saveEntries(btn){
  return busy(btn, '保存中…', async () => {
    const el = $('st-entries'); status(el, '保存中…', true);
    const d = await post('/admin/save', { entries: JSON.stringify(EDIT.entries) });
    if (d.ok) { status(el, '已保存 ' + EDIT.entries.length + ' 个数据面 ' + new Date().toLocaleTimeString(), true); toast('数据面已保存'); await load(); }
    else { status(el, '失败：' + (d.msg||''), false); toast('保存失败：' + (d.msg||''), true); }
  });
}
function renderPreferred(){
  const p = STATE.preferred || [];
  let arr = []; try { arr = typeof p === 'string' ? JSON.parse(p) : p; } catch (e) { arr = []; }
  const txt = arr.map(x => typeof x === 'string' ? x : (x.ip || JSON.stringify(x))).join('\\n');
  $('pref-text').value = txt;
  $('prefHint').textContent = '当前 ' + arr.length + ' 条；测速只对这里的条目 + sources 里的域名进行';
}
async function loadPreferred(){ await load(); toast('已重新读取'); }
async function savePreferred(btn){
  return busy(btn, '保存中…', async () => {
    const lines = $('pref-text').value.split('\\n').map(x=>x.trim()).filter(Boolean);
    const el = $('st-pref'); status(el, '保存中…', true);
    const d = await post('/admin/save', { preferred: JSON.stringify(lines) });
    if (d.ok) { status(el, '已保存 ' + lines.length + ' 条 ' + new Date().toLocaleTimeString(), true); toast('优选列表已保存 ' + lines.length + ' 条'); await load(); }
    else { status(el, '失败：' + (d.msg||''), false); toast('保存失败：' + (d.msg||''), true); }
  });
}
async function importPreferred(btn){
  const u = $('imp-url').value.trim();
  if (!u) { toast('请先填写列表网址', true); return; }
  return busy(btn, '导入中…', async () => {
    const el = $('st-pref'); status(el, '导入中…', true);
    const d = await post('/api/import', { url: u });
    if (d.ok) { status(el, '导入新增 ' + d.added + ' 条，共 ' + d.total + ' 条', true); toast('导入成功：新增 ' + d.added + ' 条'); await load(); }
    else { status(el, '导入失败：' + (d.msg||''), false); toast('导入失败：' + (d.msg||''), true); }
  });
}
async function triggerRun(btn){
  return busy(btn, '触发中…', async () => {
    const d = await post('/api/trigger', {});
    if (d.ok) { toast(d.msg || '已触发测速'); return; }
    if (d.configured === false) { openTriggerHelp(); return; }
    toast('触发失败：' + (d.msg||''), true);
  });
}
function lastRefresh(){
  const h = STATE.history || [];
  if (!h.length) return '暂无记录';
  const t = new Date(h[h.length-1].ts);
  return t.toLocaleString();
}
// ---------- 内核配置 ----------
function renderPlanes(){
  const sel = $('k-plane'); if (!sel) return;
  const es = STATE.entries || [];
  sel.innerHTML = es.map((e,i)=>'<option value="'+i+'">'+esc(e.name||('plane'+i))+' — '+esc(e.host||'')+'</option>').join('')
    || '<option value="">无数据面</option>';
}
function planeOf(){ const es=STATE.entries||[]; const i=+$('k-plane').value||0; return es[i]||null; }
async function loadKernel(btn){
  const e = planeOf(); if (!e) return toast('没有数据面');
  if (btn) { btn.disabled = true; btn.textContent = '读取中…'; }
  $('k-hint').textContent = '读取中…';
  const r = await fetch('/api/kernel?host='+encodeURIComponent(e.host)+'&uuid='+encodeURIComponent(e.uuid));
  const d = await r.json();
  if (btn) { btn.disabled = false; btn.textContent = '读取配置'; }
  if (!d.ok) { $('k-hint').textContent='失败: '+(d.msg||''); return toast('读取失败', true); }
  const c = d.config;
  $('k-ev').value = c.ev||'no'; $('k-et').value = c.et||'no'; $('k-ex').value = c.ex||'no'; $('k-ech').value = c.ech||'no';
  $('k-d').value = c.d||''; $('k-p').value = c.p||''; $('k-tp').value = c.tp||'';
  $('k-dns').value = c.customDNS||''; $('k-echdom').value = c.customECHDomain||'';
  $('k-home').value = c.homepage||''; $('k-s').value = c.s||'';
  $('k-raw').value = JSON.stringify(c, null, 1);
  window.__kbase = c;
  $('k-hint').textContent = '已读取 ' + e.host + ' · ' + new Date().toLocaleTimeString();
  toast('内核配置已读取');
}
async function saveKernel(btn){
  const e = planeOf(); if (!e) return toast('没有数据面');
  let cfg;
  try { cfg = JSON.parse($('k-raw').value); } catch (err) { return toast('原始 JSON 格式错误', true); }
  cfg.ev=$('k-ev').value; cfg.et=$('k-et').value; cfg.ex=$('k-ex').value; cfg.ech=$('k-ech').value;
  cfg.d=$('k-d').value; cfg.p=$('k-p').value; cfg.tp=$('k-tp').value;
  cfg.customDNS=$('k-dns').value; cfg.customECHDomain=$('k-echdom').value;
  cfg.homepage=$('k-home').value; cfg.s=$('k-s').value;
  const base = window.__kbase || {};
  const diff = {};
  for (const k of Object.keys(cfg)) if (JSON.stringify(cfg[k]) !== JSON.stringify(base[k])) diff[k] = cfg[k];
  if (!Object.keys(diff).length) { toast('没有修改任何字段'); return; }
  if (btn) { btn.disabled = true; btn.textContent = '保存中…'; }
  const d = await post('/api/kernel', { host: e.host, uuid: e.uuid, config: diff });
  if (btn) { btn.disabled = false; btn.textContent = '保存到数据面'; }
  $('k-hint').textContent = (d.ok ? '已保存到 ' + e.host : '失败：' + (d.msg||'')) + ' · ' + new Date().toLocaleTimeString();
  toast(d.ok ? ('内核配置已保存：' + (d.msg||'')) : ('内核保存失败：' + (d.msg||'')), !d.ok);
  if (d.ok) { window.__kbase = Object.assign({}, base, diff); }
}

// ---------- 流量统计 ----------
async function loadTraffic(){
  const d = await (await fetch('/api/traffic')).json();
  if (!d.ok) return;
  const t = d.traffic || {};
  const days = Object.keys(t).sort().slice(-7);
  const today = new Date().toISOString().slice(0,10);
  const sumOf = (r) => (r.ws||0)+(r.sub||0)+(r.other||0);
  const todayN = t[today] ? sumOf(t[today]) : 0;
  const weekN = days.reduce((a,k)=>a+sumOf(t[k]),0);
  const quota = d.quota || 100000;
  const pct = Math.min(100, Math.round(todayN/quota*1000)/10);
  const cards = [['今日请求', todayN.toLocaleString()], ['每日配额', quota.toLocaleString()], ['今日使用率', pct + ' %'], ['近 7 天', weekN.toLocaleString()]];
  $('t-cards').innerHTML = cards.map(([k,v])=>'<div class="card"><div class="k">'+k+'</div><div class="v">'+v+'</div></div>').join('')
    + '<div class="card" style="grid-column:1/-1"><div class="k">说明</div><div class="dim" style="font-size:12px;margin-top:6px">'
    + '本机计数为近似值（多 isolate 各自累计，5 分钟落一次 KV）；真实账单以 Cloudflare 控制台为准。</div></div>';
  const max = Math.max(1, ...days.map(k=>sumOf(t[k])));
  const w = 620, h = 140, bw = days.length ? (w-20)/days.length-8 : 40;
  const bars = days.map((k,i)=>{
    const v = sumOf(t[k]); const bh = Math.max(2, v/max*(h-28));
    const x = 10 + i*((w-20)/Math.max(1,days.length));
    return '<rect x="'+x.toFixed(1)+'" y="'+(h-20-bh).toFixed(1)+'" width="'+bw.toFixed(1)+'" height="'+bh.toFixed(1)+'" rx="3" fill="#4ade80" opacity="0.8"/>'
      + '<text x="'+(x+bw/2).toFixed(1)+'" y="'+(h-6)+'" fill="#8b9bb4" font-size="9" text-anchor="middle">'+k.slice(5)+'</text>'
      + '<text x="'+(x+bw/2).toFixed(1)+'" y="'+(h-24-bh).toFixed(1)+'" fill="#e6edf7" font-size="9" text-anchor="middle">'+(v>999?(v/1000).toFixed(1)+'k':v)+'</text>';
  }).join('');
  $('t-chart').innerHTML = '<svg width="'+w+'" height="'+h+'" style="max-width:100%">'+bars+'</svg>';
  const rows = days.slice().reverse().map(k=>{
    const r = t[k];
    return '<tr><td>'+k+'</td><td>'+(r.ws||0)+'</td><td>'+(r.sub||0)+'</td><td>'+(r.other||0)+'</td><td>'+sumOf(r)+'</td></tr>';
  }).join('');
  $('t-table').innerHTML = days.length
    ? '<table><tr><th>日期</th><th>代理(WS)</th><th>订阅</th><th>其它</th><th>合计</th></tr>'+rows+'</table>'
    : '<div class="dim">暂无数据（产生请求后 5 分钟内会出现）</div>';
}

function openTriggerHelp(){
  const cmd = 'cd ~/cfmux && ./tools/refresh.sh';
  const wrap = document.createElement('div');
  wrap.style.cssText = 'position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:100;display:flex;align-items:center;justify-content:center';
  const card = document.createElement('div');
  card.className = 'card';
  card.style.cssText = 'width:640px;max-width:92vw;background:#0f1729';
  card.innerHTML = '<div class="k">手动测速说明</div>'
    + '<div class="dim" style="font-size:13px;line-height:1.9;margin-top:10px">'
    + '测速必须在<b>你自己的网络</b>里跑（Worker 看不到你到 Cloudflare 的链路），两种方式：<br><br>'
    + '<b class="ok">方式一（推荐，零配置）</b>：在本机终端执行下面命令，跑完自动写回云端：'
    + '</div>'
    + '<code style="display:block;padding:10px;margin:8px 0">' + cmd + '</code>'
    + '<div class="dim" style="font-size:13px">上次刷新：<b>' + lastRefresh() + '</b></div>'
    + '<div class="dim" style="font-size:13px;line-height:1.9;margin-top:12px">'
    + '<b class="ok">方式二（一键触发）</b>：配置 GitHub Token 后按钮可直接触发 Actions。<br>'
    + '需要环境变量 <code>gh_token</code>（PAT，权限 actions:write）与 <code>gh_repo</code>=moneyfly006/cfmux。'
    + '</div>'
    + '<div style="margin-top:16px;display:flex;gap:8px">'
    + '<button class="btn" id="trg-copy">复制命令</button>'
    + '<button class="btn alt" id="trg-close">关闭</button></div>';
  wrap.appendChild(card);
  document.body.appendChild(wrap);
  card.querySelector('#trg-copy').onclick = function(){ copyText(cmd); toast('命令已复制'); };
  card.querySelector('#trg-close').onclick = function(){ wrap.remove(); };
  wrap.onclick = function(ev){ if (ev.target === wrap) wrap.remove(); };
}

function doAct(el){ act(el.dataset.act, el.dataset.key); }
async function testNotify(btn){
  return busy(btn, '发送中…', async () => {
    const d = await post('/api/notify/test', {});
    const el = $('st-notify');
    if (d.ok) { status(el, '已发送：' + (d.results||[]).join(' '), true); toast('测试通知已发送'); }
    else { status(el, '失败：' + (d.msg||''), false); toast('发送失败：' + (d.msg||''), true); }
  });
}
async function act(type, key){
  const label = {pin:'置顶',unpin:'取消置顶',ban:'熔断',unban:'解除'}[type] || type;
  const d = await post('/api/action', {type, key});
  if (d.ok) { toast(label + '成功'); await load(); } else { toast(label + '失败：' + (d.msg||''), true); }
}
load();
</script></body></html>`;

async function adminLogin(request, env) {
  if (!env.admin_password) return new Response(loginHtml(true), { status: 503, headers: { 'content-type': 'text/html; charset=utf-8' } });
  const form = new URLSearchParams(await request.text());
  const pass = form.get('password') || '';
  if (pass !== env.admin_password) {
    return new Response(loginHtml(false), { status: 401, headers: { 'content-type': 'text/html; charset=utf-8' } });
  }
  const tok = await adminToken(env);
  return new Response(null, {
    status: 302,
    headers: { Location: '/admin', 'Set-Cookie': `${ADMIN_COOKIE}=${tok}; Path=/; Max-Age=${SEVEN_DAYS}; HttpOnly; Secure; SameSite=Lax` },
  });
}

async function adminLogout(request, env) {
  return new Response(null, { status: 302, headers: { Location: '/admin', 'Set-Cookie': `${ADMIN_COOKIE}=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax` } });
}

async function adminSave(request, env, url) {
  if (!(await isAdmin(request, env))) return json({ ok: false, msg: '未登录' }, 401);
  let body;
  try { body = await request.json(); } catch { return json({ ok: false, msg: 'bad json' }, 400); }
  if (!env.KV || typeof env.KV.put !== 'function') return json({ ok: false, msg: 'KV 未绑定，无法保存' }, 503);
  try {
    const st = await getSettings(env);
    if (body.sources !== undefined) st.sources = String(body.sources || '').trim();
    if (body.limit !== undefined) { const n = +body.limit; st.limit = Number.isFinite(n) ? Math.max(0, Math.min(n, 300)) : 12; }
    if (body.perEntry !== undefined) { const n = +body.perEntry; st.perEntry = Number.isFinite(n) ? Math.max(0, Math.min(n, 300)) : 0; }
    if (body.preferred !== undefined) {
      const arr = JSON.parse(body.preferred || '[]');
      if (!Array.isArray(arr)) throw new Error('preferred 必须是数组');
      await env.KV.put('preferred', JSON.stringify(arr));
    }
    if (body.entries !== undefined) {
      const arr = JSON.parse(body.entries || '[]');
      if (!Array.isArray(arr)) throw new Error('entries 必须是数组');
      await env.KV.put('entries', JSON.stringify(arr));
    }
    if (body.minScore !== undefined) st.minScore = Math.max(0, Math.min(+body.minScore || 0, 100));
    if (body.minTput !== undefined) st.minTput = Math.max(0, +body.minTput || 0);
    if (body.maxAgeHours !== undefined) st.maxAgeHours = Math.max(0, +body.maxAgeHours || 0);
    if (body.softExpireDays !== undefined) st.softExpireDays = Math.max(0, +body.softExpireDays || 0);
    if (body.pruneStrikes !== undefined) st.pruneStrikes = Math.max(1, +body.pruneStrikes || 4);
    if (body.pruneFloor !== undefined) st.pruneFloor = Math.max(0, +body.pruneFloor || 120);
    if (body.banHours !== undefined) st.banHours = Math.max(1, +body.banHours || 72);
    if (body.promoteMinScore !== undefined) st.promoteMinScore = Math.max(0, Math.min(+body.promoteMinScore || 55, 100));
    if (body.autoPromote !== undefined) st.autoPromote = String(body.autoPromote).toLowerCase() !== 'false';
    if (body.tg_token !== undefined) st.tg_token = String(body.tg_token || '').trim();
    if (body.tg_chat !== undefined) st.tg_chat = String(body.tg_chat || '').trim();
    if (body.webhook_url !== undefined) st.webhook_url = String(body.webhook_url || '').trim();
    if (body.requireQuality !== undefined) st.requireQuality = String(body.requireQuality).toLowerCase() !== 'false';
    if (body.colos !== undefined) {
      const obj = JSON.parse(body.colos || '{}');
      if (typeof obj !== 'object' || Array.isArray(obj)) throw new Error('colos 必须是对象');
      await env.KV.put('colos', JSON.stringify(obj));
    }
    await env.KV.put('settings', JSON.stringify(st));
    return json({ ok: true });
  } catch (e) {
    return json({ ok: false, msg: String(e).slice(0, 120) }, 400);
  }
}
