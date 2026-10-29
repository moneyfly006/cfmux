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
    if (url.pathname === '/sub') return subscription(request, env, url);
    if (url.pathname === '/panel') return panelPage(request, env, url);
    if (url.pathname === '/api/entries') return apiEntries(request, env, url);
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
    return proxyWebSocket(request, env, uuidHex);
  },
};

const VERSION = 'cfmux/1.0';

function json(o, status = 200) {
  return new Response(JSON.stringify(o), { status, headers: { 'content-type': 'application/json', 'cache-control': 'no-store' } });
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
async function getThreshold(env) {
  const st = await getSettings(env);
  return { minScore: st.minScore ?? 0, minTput: st.minTput ?? 0, requireQuality: st.requireQuality !== false };
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
  const limit = Math.min(+(url.searchParams.get('limit') || st.limit || 12), 300);
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
  return { entries: entries.slice(0, limit), total: merged.length, limit, wantRegion };
}

function nodeName(base, e, i) {
  const r = e.colo ? `${e.colo}` : (e.kind === 'domain' ? 'DOM' : 'IP');
  return `${base}-${r}-${String(i + 1).padStart(3, '0')}`;
}

async function subscription(request, env, url) {
  const target = (url.searchParams.get('target') || 'base64').toLowerCase();
  const wsPath = url.searchParams.get('path') || '/';
  const base = url.searchParams.get('name') || 'cfmux';
  const { entries: cand, total } = await resolveEntries(request, env, url);
  const entries = await getEntries(env);
  const quality = await getQuality(env);
  const th = await getThreshold(env);
  const hostOverride = url.searchParams.get('host') || '';

  // 组装候选：entry × preferred，质量优先
  const nodes = [];
  for (const e of entries) {
    const host = e.host || url.host;
    const uuid = e.uuid || env.uuid;
    if (!uuid) continue;
    for (const c of cand) {
      const q = quality[`${host}|${c.value}`];
      // 有质量数据 → 必须达标；无质量数据 → 在 requireQuality=false 时才放行
      if (q) {
        if (!q.ok) continue;
        if ((q.score ?? 0) < th.minScore) continue;
        if ((q.tput ?? 0) < th.minTput) continue;
      } else if (th.requireQuality) {
        continue;
      }
      nodes.push({
        value: c.value, uuid, host,
        name: `${base}-${(e.name || 'core')}-${q ? q.tier + '-' : ''}${String(nodes.length + 1).padStart(3, '0')}`,
        score: q ? q.score : null, tier: q ? q.tier : null, tput: q ? q.tput : null, lat: q ? q.lat : null,
      });
    }
  }
  // 质量排序（有分的在前，同分按吞吐）
  nodes.sort((a, b) => (b.score ?? -1) - (a.score ?? -1) || (b.tput ?? 0) - (a.tput ?? 0));
  const limited = nodes.slice(0, Math.min(+(url.searchParams.get('limit') || (await getSettings(env)).limit || 12), 300));
  if (!limited.length) return json({ ok: false, msg: '没有达标节点（检查质量阈值 / 阈值过严）', total, threshold: th }, 404);

  const hdr = {
    'content-type': 'text/plain; charset=utf-8',
    'cache-control': 'no-store',
    'x-cfmux-total': String(total),
    'x-cfmux-nodes': String(limited.length),
    'x-cfmux-pass': String(nodes.length),
    'x-cfmux-minscore': String(th.minScore),
  };
  const sn = limited.map((n) => ({ value: n.value, name: n.name, host: n.host, uuid: n.uuid }));
  if (target === 'singbox' || target === 'sing-box') return new Response(singboxConfig(sn, url, wsPath), { headers: hdr });
  if (target === 'clash' || target === 'stash' || target === 'mihomo') return new Response(clashConfig(sn, url, wsPath), { headers: hdr });
  return new Response(b64(v2rayLinks(sn, url, wsPath)), { headers: hdr });
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
  const q = new URLSearchParams({
    encryption: 'none', security: 'tls', sni: host, fp: 'chrome',
    type: 'ws', host, path: wsPath + (wsPath.includes('?') ? '&' : '?') + 'ed=2048',
  });
  return `vless://${uuid}@${n.value}:443?${q.toString()}#${encodeURIComponent(n.name)}`;
}
function v2rayLinks(nodes, url, wsPath) { return nodes.map((n) => vlessLink(n, url, wsPath)).join('\n'); }

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
  const outs = nodes.map((n) => ({ type: 'vless', tag: n.name, server: n.value, server_port: 443, uuid,
    tls: { enabled: true, server_name: n.host, utls: { enabled: true, fingerprint: 'chrome' } },
    transport: { type: 'ws', path: wsPath + (wsPath.includes('?') ? '&' : '?') + 'ed=2048', headers: { Host: n.host } } }));
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
  const base = { sources: '', limit: 12, host: '' };
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

  const st = await getSettings(env);
  let preferred = '[]', colos = '{}', entriesRaw = '[]', quality = {};
  try {
    if (env.KV) {
      preferred = (await env.KV.get('preferred')) || '[]';
      colos = (await env.KV.get('colos')) || '{}';
      entriesRaw = (await env.KV.get('entries')) || '[]';
      quality = JSON.parse((await env.KV.get('quality')) || '{}');
    }
  } catch {}
  const qRows = Object.entries(quality)
    .sort((a, b) => (b[1].score ?? -1) - (a[1].score ?? -1))
    .slice(0, 20)
    .map(([k, v]) => `<tr><td>${esc(k.split('|')[1] || '')}</td><td>${esc((k.split('|')[0] || '').replace(/\.pages\.dev$/, ''))}</td><td class="${v.ok ? 'ok' : 'bad'}">${v.ok ? '✓' : '✗'}</td><td>${v.tier || '-'}</td><td>${v.score ?? 0}</td><td>${v.lat ?? '-'}</td><td>${v.jitter ?? 0}</td><td>${v.tput ?? 0}</td></tr>`)
    .join('');
  const qualityTable = qRows
    ? `<table><tr><th>入口</th><th>数据面</th><th>状态</th><th>等级</th><th>评分</th><th>延迟ms</th><th>抖动</th><th>KB/s</th></tr>${qRows}</table>`
    : '<div style="color:#8b9bb4;font-size:12px">暂无质量数据，先跑 tools/quality.mjs 并写入 KV 的 quality 键</div>';
  const base = `https://${url.host}`;
  const esc = (x) => String(x).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const html = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>cfmux 后台</title><style>
body{margin:0;background:#0b1020;color:#e6edf7;font:14px/1.6 -apple-system,"PingFang SC",sans-serif}
.wrap{max-width:980px;margin:0 auto;padding:22px}h1{font-size:18px;margin:0 0 14px}
.card{background:#141b2f;border:1px solid #22304d;border-radius:12px;padding:16px;margin-bottom:14px}
.card h2{font-size:13px;color:#8b9bb4;margin:0 0 10px;letter-spacing:.5px}
textarea,input{width:100%;box-sizing:border-box;background:#0f1526;border:1px solid #22304d;color:#e6edf7;border-radius:8px;padding:9px;font:12px/1.5 ui-monospace,Menlo,monospace}
textarea{min-height:110px;resize:vertical}
button{background:#1d4ed8;border:0;color:#fff;border-radius:8px;padding:9px 16px;cursor:pointer;font-size:13px}
button.alt{background:#1f2a44}a{color:#63e2b7}code{background:#0f1526;padding:1px 5px;border-radius:4px;font-size:12px}
.row{display:flex;gap:10px;align-items:center;margin-bottom:8px}.row label{width:110px;color:#8b9bb4;font-size:12px}
#msg{margin-top:10px;font-size:13px}table{width:100%;border-collapse:collapse;font-size:12px}td,th{padding:5px 7px;border-bottom:1px solid #1b253c;text-align:left}th{color:#8b9bb4;font-weight:500}.ok{color:#4ade80}.bad{color:#f87171}
</style></head><body><div class="wrap">
<h1>cfmux 后台 <span style="font-size:12px;color:#8b9bb4">已登录</span></h1>
<div class="card"><h2>订阅</h2>
<div><code>${base}/sub</code> · <code>${base}/sub?target=clash</code> · <code>${base}/sub?target=singbox</code></div>
<div style="margin-top:6px;font-size:12px;color:#8b9bb4">front: <a href="/panel">/panel</a> · <a href="/diag">/diag</a> · <a href="/admin/logout">退出登录</a></div>
</div>
<form id="f">
<div class="card"><h2>基本设置</h2>
<div class="row"><label>优选源(sources)</label><input name="sources" value="${esc(st.sources)}" placeholder="逗号分隔，留空则用内置 BestCF"></div>
<div class="row"><label>默认节点数</label><input name="limit" value="${esc(st.limit)}" placeholder="12"></div>
</div>
<div class="card"><h2>数据面条目 entries（JSON：name/host/uuid/path）</h2><textarea name="entries">${esc(entriesRaw)}</textarea></div>
<div class="card"><h2>质量门槛</h2>
<div class="row"><label>最低评分</label><input name="minScore" value="${esc(st.minScore ?? 0)}" placeholder="0-100，建议 45"></div>
<div class="row"><label>最低吞吐 KB/s</label><input name="minTput" value="${esc(st.minTput ?? 0)}" placeholder="0，建议 80"></div>
<div class="row"><label>必须有实测</label><input name="requireQuality" value="${st.requireQuality === false ? 'false' : 'true'}" placeholder="true/false"></div>
</div>
<div class="card"><h2>质量表（按评分排序，前 20）</h2>${qualityTable}</div>
<div class="card"><h2>优选表 preferred（JSON 数组）</h2><textarea name="preferred">${esc(preferred)}</textarea></div>
<div class="card"><h2>区域映射 colos（JSON 对象 ip→colo）</h2><textarea name="colos">${esc(colos)}</textarea></div>
<div><button type="submit">保存到 KV</button> <span id="msg"></span></div>
</form>
</div><script>
document.getElementById('f').addEventListener('submit', async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const body = { sources: fd.get('sources'), limit: fd.get('limit'), preferred: fd.get('preferred'), colos: fd.get('colos') };
  const r = await fetch('/admin/save', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) });
  const d = await r.json().catch(()=>({}));
  document.getElementById('msg').textContent = d.ok ? '✅ 已保存' : ('❌ ' + (d.msg || '保存失败'));
});
</script></body></html>`;
  return new Response(html, { headers: { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' } });
}

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
    if (body.limit !== undefined) st.limit = Math.max(1, Math.min(+body.limit || 12, 300));
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
