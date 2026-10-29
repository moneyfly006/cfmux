#!/usr/bin/env node
/**
 * cfmux 质量引擎
 * 对「数据面 × 优选入口」的每个组合做三层实测，产出质量分与分级：
 *   1) 握手：TCP/TLS/WS 三段 RTT
 *   2) 可用性：VLESS 请求 204
 *   3) 吞吐：经隧道下载 ~512KB，算 KB/s
 *   4) 抖动：连续 3 次 204 的 max-min
 * 输出 quality.json： { "host|entry": {lat,ws,tput,jitter,ok,score,tier,ts} }
 *
 * 用法:
 *   node tools/quality.mjs --entries entries.json --preferred preferred.txt --out quality.json
 *   entries.json: [{"name":"core","host":"mux-core.pages.dev","uuid":"...","path":"/"}]
 */
import tls from 'node:tls';
import crypto from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, arr) => (v.startsWith('--') ? [...a, [v.slice(2), arr[i + 1]]] : a), []));
const OUT = args.out || 'quality.json';
const TMO = +(args.timeout || 8000);
const DL = +(args.bytes || 512 * 1024);
const ENTRIES = JSON.parse(readFileSync(args.entries, 'utf8'));
const PREFERRED = readFileSync(args.preferred, 'utf8').split('\n').map((s) => s.trim()).filter(Boolean);

const wsFrame = (p, op = 2) => {
  const len = p.length; let h;
  if (len < 126) h = Buffer.from([0x80 | op, 0x80 | len]);
  else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 0x80 | 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 0x80 | 127; h.writeBigUInt64BE(BigInt(len), 2); }
  const m = crypto.randomBytes(4); const b = Buffer.from(p);
  for (let i = 0; i < b.length; i++) b[i] ^= m[i & 3];
  return Buffer.concat([h, m, b]);
};
class R {
  constructor() { this.b = Buffer.alloc(0); this.q = []; this.w = null; }
  push(d) {
    this.b = Buffer.concat([this.b, d]);
    for (;;) {
      if (this.b.length < 2) return;
      let len = this.b[1] & 0x7f, off = 2;
      if (len === 126) { if (this.b.length < 4) return; len = this.b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.b.length < 10) return; len = Number(this.b.readBigUInt64BE(2)); off = 10; }
      if ((this.b[1] & 0x80) !== 0) off += 4;
      if (this.b.length < off + len) return;
      const op = this.b[0] & 0x0f; const pl = this.b.subarray(off, off + len);
      this.b = this.b.subarray(off + len);
      if (op === 8) { this.w && this.w(null); return; }
      if (op === 2 || op === 0) { if (this.w) { const w = this.w; this.w = null; w(pl); } else this.q.push(pl); }
    }
  }
  next(ms) { if (this.q.length) return Promise.resolve(this.q.shift()); return new Promise((r) => { this.w = r; setTimeout(() => { if (this.w === r) { this.w = null; r(null); } }, ms); }); }
};

function vlessHdr(uuidHex, host, port) {
  const ub = Buffer.from(String(uuidHex).replace(/-/g, ''), 'hex'), hb = Buffer.from(host);
  return Buffer.concat([Buffer.from([0]), ub, Buffer.from([0, 1]), Buffer.from([port >> 8, port & 0xff]), Buffer.from([2, hb.length]), hb]);
}

// 一次完整会话：返回 {lat, ws, ok}
function session(entry, ip, targetHost, targetPort, reqText, readN, deadlineMs) {
  return new Promise((resolve) => {
    const res = { ok: false, lat: null, ws: null, bytes: 0, err: null };
    const t0 = Date.now(); let done = false;
    const fin = (e) => { if (done) return; done = true; res.err = e || null; res.totalMs = Date.now() - t0; clearTimeout(tm); try { sock.destroy(); } catch {} resolve(res); };
    const tm = setTimeout(() => fin('timeout'), deadlineMs);
    let sock;
    try { sock = tls.connect({ host: ip, port: 443, servername: entry.host, ALPNProtocols: ['http/1.1'], rejectUnauthorized: false }); }
    catch (e) { return fin('tls ' + e.message); }
    const r = new R(); let hs = Buffer.alloc(0), up = false;
    sock.once('secureConnect', () => {
      const key = crypto.randomBytes(16).toString('base64');
      sock.write(['GET ' + (entry.path || '/') + '?ed=2048 HTTP/1.1', `Host: ${entry.host}`, 'Upgrade: websocket',
        'Connection: Upgrade', 'Sec-WebSocket-Version: 13', `Sec-WebSocket-Key: ${key}`, '', ''].join('\r\n'));
    });
    sock.on('data', (d) => {
      if (!up) {
        hs = Buffer.concat([hs, d]); const i = hs.indexOf('\r\n\r\n');
        if (i < 0) return;
        if (!/^HTTP\/1\.1 101/.test(hs.subarray(0, i).toString())) return fin('no101');
        up = true; res.ws = Date.now() - t0;
        sock.write(wsFrame(Buffer.concat([vlessHdr(entry.uuid, targetHost, targetPort), Buffer.from(reqText)])));
        const rest = hs.subarray(i + 4); if (rest.length) r.push(rest);
        (async () => {
          let acc = 0, first = true, got = 0;
          while (acc < readN) {
            const c = await r.next(TMO);
            if (!c) break;
            if (first && c.length <= 2) { first = false; continue; }
            first = false; acc += c.length;
            if (got === 0) { res.lat = Date.now() - t0; got = 1; }
          }
          res.bytes = acc; res.ok = acc > 0;
          fin(res.ok ? null : 'nodata');
        })();
        return;
      }
      r.push(d);
    });
    sock.on('error', (e) => fin(e.code || 'err'));
  });
}

function scoreOf(q) {
  if (!q.ok) return 0;
  const lat = q.lat || 9999, tput = q.tput || 0, jit = q.jitter || 0;
  // 延迟 50% + 吞吐 35% + 抖动 15%
  const sLat = Math.max(0, 100 - lat / 60);
  const sTput = Math.min(100, tput / 2);
  const sJit = Math.max(0, 100 - jit / 5);
  return Math.round((sLat * 0.5 + sTput * 0.35 + sJit * 0.15) * 100) / 100;
}
const tierOf = (s) => (s >= 70 ? 'S' : s >= 45 ? 'A' : s >= 20 ? 'B' : 'C');

const out = {};
for (const e of ENTRIES) {
  const uuidHex = (e.uuid || '').replace(/-/g, '');
  for (const ip of PREFERRED) {
    // 3 次 204 取抖动 + 1 次大文件取吞吐
    const lat = []; let lastErr = null;
    for (let i = 0; i < 3; i++) {
      const r = await session(e, ip, 'www.gstatic.com', 80,
        'GET /generate_204 HTTP/1.0\r\nHost: www.gstatic.com\r\n\r\n', 64, TMO);
      if (r.ok && r.lat) lat.push(r.lat); else lastErr = r.err;
    }
    let q = { ok: lat.length > 0, lat: lat.length ? Math.min(...lat) : null, jitter: lat.length > 1 ? Math.max(...lat) - Math.min(...lat) : 0, tput: 0, ws: null, ts: Date.now() };
    if (q.ok) {
      const big = await session(e, ip, 'cachefly.cachefly.net', 80, 'GET /1mb.test HTTP/1.0\r\nHost: cachefly.cachefly.net\r\n\r\n', DL, TMO + 15000);
      if (big.ok && big.totalMs > 0) {
        q.ws = big.ws;
        q.tput = Math.round((big.bytes / 1024) / (big.totalMs / 1000));   // KB/s
        q.dlMs = big.totalMs; q.dlBytes = big.bytes;
      }
    }
    if (!q.ok) q.err = lastErr;
    q.score = scoreOf(q); q.tier = tierOf(q.score);
    out[`${e.host}|${ip}`] = q;
    console.log(`${q.ok ? '✅' : '❌'} ${e.host} @ ${ip.padEnd(16)} lat=${String(q.lat).padStart(5)} jit=${String(q.jitter).padStart(4)} tput=${String(q.tput).padStart(5)}KB/s score=${String(q.score).padStart(6)} ${q.tier} ${q.err || ''}`);
  }
}
writeFileSync(OUT, JSON.stringify(out, null, 1));
const okN = Object.values(out).filter((x) => x.ok).length;
console.log(`\n可用 ${okN}/${Object.keys(out).length}，已写入 ${OUT}`);
