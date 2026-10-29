#!/usr/bin/env node
/**
 * cfmux 优选测速器
 * 在「真实客户端网络」里对候选 CF 边缘 IP 做三层测量：
 *   1) TLS 握手 RTT（SNI = 你的 Worker 域名）
 *   2) WebSocket 升级 RTT
 *   3) 端到端代理：VLESS 请求 http://www.gstatic.com/generate_204，验证真的能出网
 * 评分 = 端到端成功(权重最高) + 延迟，输出排序 JSON，可写入 KV 供订阅使用。
 *
 * 用法：
 *   node tools/measure.mjs --host cfmux.pages.dev --uuid <UUID> \
 *        --ips 104.16.1.1,104.16.2.1 --out preferred.json [--json]
 */
import tls from 'node:tls';
import net from 'node:net';
import crypto from 'node:crypto';
import { writeFileSync } from 'node:fs';

const args = Object.fromEntries(
  process.argv.slice(2).reduce((a, v, i, arr) => (v.startsWith('--') ? [...a, [v.slice(2), arr[i + 1]?.startsWith('--') ? true : arr[i + 1]]] : a), [])
);
const HOST = args.host;
const UUID = (args.uuid || '').replace(/-/g, '');
const OUT = args.out || 'preferred.json';
const TIMEOUT = +(args.timeout || 6000);
const DL_BYTES = +(args.bytes || 512 * 1024);
if (!HOST || !UUID) { console.error('缺 --host 或 --uuid'); process.exit(1); }

const IPS = (args.ips ? String(args.ips) : '104.16.1.1,104.16.2.1,104.16.3.1,104.16.4.1,104.17.1.1,104.17.2.1,104.17.3.1,104.18.1.1,104.19.1.1,172.64.1.1,172.66.1.1,172.67.1.1,162.159.1.1,188.114.96.1,188.114.97.1')
  .split(',').map((s) => s.trim()).filter(Boolean);

// ---------- WS 帧编解码（客户端必须 mask）----------
function wsFrame(payload, opcode = 0x2) {
  const len = payload.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, 0x80 | len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 0x80 | 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 0x80 | 127; head.writeBigUInt64BE(BigInt(len), 2); }
  const mask = crypto.randomBytes(4);
  const body = Buffer.from(payload);
  for (let i = 0; i < body.length; i++) body[i] ^= mask[i & 3];
  return Buffer.concat([head, mask, body]);
}

class Reader {
  constructor() { this.buf = Buffer.alloc(0); this.msgs = []; this.waiter = null; }
  push(d) {
    this.buf = Buffer.concat([this.buf, d]);
    this._drain();
  }
  _drain() {
    while (true) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      const op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (masked) off += 4;
      if (this.buf.length < off + len) return;
      let payload = this.buf.subarray(off, off + len);
      if (masked) { const m = this.buf.subarray(off - 4, off); payload = Buffer.from(payload); for (let i = 0; i < payload.length; i++) payload[i] ^= m[i & 3]; }
      this.buf = this.buf.subarray(off + len);
      if (op === 0x8) { this.waiter && this.waiter(null); return; }
      if (op === 0x2 || op === 0x0) this.msgs.push(payload);
      if (this.waiter && this.msgs.length) { const w = this.waiter; this.waiter = null; w(this.msgs.shift()); }
    }
  }
  next(ms) {
    if (this.msgs.length) return Promise.resolve(this.msgs.shift());
    return new Promise((res) => { this.waiter = res; setTimeout(() => { if (this.waiter === res) { this.waiter = null; res(null); } }, ms); });
  }
}

function vlessHeader(host, port) {
  const ub = Buffer.from(UUID, 'hex');
  const hb = Buffer.from(host);
  return Buffer.concat([Buffer.from([0]), ub, Buffer.from([0, 1]), Buffer.from([port >> 8, port & 0xff]), Buffer.from([2, hb.length]), hb]);
}

function measure(ip) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const res = { ip, tlsMs: null, wsMs: null, e2e: false, e2eMs: null, tcpMs: null, err: null, score: 0 };
    let done = false;
    const finish = (err) => {
      if (done) return; done = true;
      if (err) res.err = String(err).slice(0, 80);
      clearTimeout(timer);
      try { sock && sock.destroy(); } catch {}
      resolve(res);
    };
    const timer = setTimeout(() => finish('timeout'), TIMEOUT);
    const sock = tls.connect({ host: ip, port: 443, servername: HOST, ALPNProtocols: ['http/1.1'], rejectUnauthorized: false });
    sock.once('connect', () => { res.tcpMs = Date.now() - t0; });
    sock.once('secureConnect', () => {
      res.tlsMs = Date.now() - t0;
      const key = crypto.randomBytes(16).toString('base64');
      const req = [
        'GET /?ed=2048 HTTP/1.1', `Host: ${HOST}`, 'Upgrade: websocket', 'Connection: Upgrade',
        'Sec-WebSocket-Version: 13', `Sec-WebSocket-Key: ${key}`, `User-Agent: cfmux-measure/1.0`, '', '',
      ].join('\r\n');
      sock.write(req);
      const reader = new Reader();
      let hsBuf = Buffer.alloc(0), upgraded = false;
      sock.on('data', (d) => {
        if (!upgraded) {
          hsBuf = Buffer.concat([hsBuf, d]);
          const idx = hsBuf.indexOf('\r\n\r\n');
          if (idx < 0) return;
          const head = hsBuf.subarray(0, idx).toString();
          if (!/^HTTP\/1\.1 101/.test(head)) return finish('no 101: ' + head.split('\r\n')[0]);
          upgraded = true;
          res.wsMs = Date.now() - t0;
          const rest = hsBuf.subarray(idx + 4);
          // VLESS 请求：目标 www.gstatic.com:80 取 204
          const body = Buffer.concat([vlessHeader('www.gstatic.com', 80), Buffer.from('GET /generate_204 HTTP/1.0\r\nHost: www.gstatic.com\r\n\r\n')]);
          sock.write(wsFrame(body));
          if (rest.length) reader.push(rest);
          const deadline = setTimeout(() => finish(res.e2e ? null : 'e2e timeout'), TIMEOUT);
          (async () => {
            let acc = Buffer.alloc(0), firstChunk = true;
            while (true) {
              const chunk = await reader.next(TIMEOUT);
              if (!chunk) { clearTimeout(deadline); return finish(acc.length ? 'bad response' : 'e2e no data'); }
              // 首个 WS 帧只含 2 字节 VLESS 应答头
              const payload = firstChunk && chunk.length <= 2 ? Buffer.alloc(0)
                : (firstChunk ? chunk.subarray(2) : chunk);
              firstChunk = false;
              acc = Buffer.concat([acc, payload]);
              if (/HTTP\/1\.[01] 20[04]/.test(acc.toString('latin1'))) {
                res.e2e = true; res.e2eMs = Date.now() - t0; res.bytes = acc.length;
                clearTimeout(deadline); return finish(null);
              }
              if (acc.length > 4096) { clearTimeout(deadline); return finish('bad response'); }
            }
          })();
          return;
        }
        reader.push(d);
      });
    });
    sock.once('error', (e) => finish(e.code || e.message));
  });
}

function score(r) {
  if (!r.e2e) return 0;
  const lat = r.e2eMs || 9999;
  return Math.round((10000 / (lat + 100)) * 100) / 100;   // 延迟越低分越高
}

const results = [];
for (const ip of IPS) {
  const r = await measure(ip);
  r.score = score(r);
  results.push(r);
  if (!args.json) console.log(
    `${r.e2e ? '✅' : '❌'} ${ip.padEnd(15)} tcp=${String(r.tcpMs ?? '-').padStart(4)}ms tls=${String(r.tlsMs ?? '-').padStart(4)}ms ws=${String(r.wsMs ?? '-').padStart(4)}ms e2e=${String(r.e2eMs ?? '-').padStart(5)}ms score=${r.score}${r.err ? '  ' + r.err : ''}`
  );
}
results.sort((a, b) => b.score - a.score || (a.e2eMs || 1e9) - (b.e2eMs || 1e9));
const alive = results.filter((r) => r.e2e);
writeFileSync(OUT, JSON.stringify(alive, null, 1));
if (!args.json) {
  console.log(`\n可用 ${alive.length}/${results.length}，已写入 ${OUT}`);
  console.log('Top5:', alive.slice(0, 5).map((r) => `${r.ip}(${r.e2eMs}ms)`).join(' '));
} else {
  console.log(JSON.stringify(alive));
}
