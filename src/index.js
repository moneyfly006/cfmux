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

// ---------- 控制面：订阅生成（智能优选 + 健康过滤）----------
// 设计：优选列表由 tools/measure.mjs 在真实网络里测出来，写入 KV；
// 订阅按「评分排序」输出，死节点（近 N 次失败）直接不出现。
const DEFAULT_PREFERRED = [
  '104.16.1.1', '104.16.2.1', '104.16.3.1', '104.16.4.1', '104.16.5.1',
  '104.17.1.1', '104.17.2.1', '104.17.3.1', '104.18.1.1', '104.19.1.1',
  '172.64.1.1', '172.66.1.1', '172.67.1.1', '172.68.1.1',
  '162.159.1.1', '162.159.2.1', '188.114.96.1', '188.114.97.1'
];

async function subscription(request, env, url) {
  const uuid = (env.uuid || '').trim();
  if (!uuid) return json({ ok: false, msg: 'uuid not configured' }, 503);

  const target = (url.searchParams.get('target') || 'base64').toLowerCase();
  const limit = Math.min(+(url.searchParams.get('limit') || 12), 60);
  const host = url.searchParams.get('host') || url.host;          // 代理入口域名（SNI/Host）
  const wsPath = url.searchParams.get('path') || '/';
  const name = url.searchParams.get('name') || 'cfmux';

  // 优选列表：KV 优先（由测速工具写入），否则内置默认
  let preferred = DEFAULT_PREFERRED;
  try {
    if (env.KV && typeof env.KV.get === 'function') {
      const raw = await env.KV.get('preferred');
      if (raw) {
        const arr = JSON.parse(raw);
        if (Array.isArray(arr) && arr.length) {
          preferred = arr.map((x) => (typeof x === 'string' ? x : x.ip)).filter(Boolean);
        }
      }
    }
  } catch {}

  const entries = preferred.slice(0, limit);
  const nodes = entries.map((ip, i) => ({
    ip, name: `${name}-${String(i + 1).padStart(2, '0')}`,
  }));

  if (target === 'singbox' || target === 'sing-box') return jsonText(singboxConfig(nodes, uuid, host, wsPath));
  if (target === 'clash') return jsonText(clashConfig(nodes, uuid, host, wsPath));
  return jsonText(b64(v2rayLinks(nodes, uuid, host, wsPath)));
}

function jsonText(body, status = 200) {
  return new Response(body, { status, headers: { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' } });
}
function b64(s) { return btoa(unescape(encodeURIComponent(s))); }

function vlessLink(n, uuid, host, wsPath) {
  const q = new URLSearchParams({
    encryption: 'none', security: 'tls', sni: host, fp: 'chrome',
    type: 'ws', host, path: wsPath + (wsPath.includes('?') ? '&' : '?') + 'ed=2048',
  });
  return `vless://${uuid}@${n.ip}:443?${q.toString()}#${encodeURIComponent(n.name)}`;
}
function v2rayLinks(nodes, uuid, host, wsPath) { return nodes.map((n) => vlessLink(n, uuid, host, wsPath)).join('\n'); }

function clashConfig(nodes, uuid, host, wsPath) {
  const proxies = nodes.map((n) => `  - {name: "${n.name}", type: vless, server: ${n.ip}, port: 443, uuid: ${uuid}, tls: true, servername: ${host}, network: ws, ws-opts: {path: "${wsPath}?ed=2048", headers: {Host: ${host}}}, udp: false, skip-cert-verify: false}`);
  const names = nodes.map((n) => `"${n.name}"`).join(', ');
  return [
    'mixed-port: 7890', 'allow-lan: false', 'mode: rule', 'log-level: warn',
    'proxies:', ...proxies,
    'proxy-groups:',
    `  - {name: "自动优选", type: url-test, proxies: [${names}], url: "http://www.gstatic.com/generate_204", interval: 300, tolerance: 50}`,
    `  - {name: "故障切换", type: fallback, proxies: ["自动优选", ${names}], url: "http://www.gstatic.com/generate_204", interval: 120}`,
    'rules:', '  - MATCH,故障切换',
  ].join('\n');
}

function singboxConfig(nodes, uuid, host, wsPath) {
  const outs = nodes.map((n) => ({
    type: 'vless', tag: n.name, server: n.ip, server_port: 443, uuid,
    tls: { enabled: true, server_name: host, utls: { enabled: true, fingerprint: 'chrome' } },
    transport: { type: 'ws', path: wsPath + (wsPath.includes('?') ? '&' : '?') + 'ed=2048', headers: { Host: host } },
  }));
  return JSON.stringify({
    log: { level: 'warn' },
    inbounds: [{ type: 'mixed', listen: '127.0.0.1', listen_port: 7890 }],
    outbounds: [
      { type: 'urltest', tag: 'auto', outbounds: nodes.map((n) => n.name), url: 'http://www.gstatic.com/generate_204', interval: '5m', tolerance: 50 },
      ...outs,
      { type: 'direct', tag: 'direct' },
    ],
    route: { final: 'auto' },
  }, null, 1);
}
