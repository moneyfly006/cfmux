#!/usr/bin/env node
/**
 * 区域探测器：对每个优选条目做 TLS 握手 + GET /healthz，
 * 读回 Worker 实际落地的 colo（数据中心），生成 ip/domain -> colo 映射。
 * 用法: node tools/colos.mjs --host cfmux.pages.dev --entries ips.txt --out colos.json
 */
import tls from 'node:tls';
import { readFileSync, writeFileSync } from 'node:fs';

const args = Object.fromEntries(process.argv.slice(2).reduce((a, v, i, arr) => (v.startsWith('--') ? [...a, [v.slice(2), arr[i + 1]]] : a), []));
const HOST = args.host, OUT = args.out || 'colos.json', TIMEOUT = +(args.timeout || 5000);
const entries = readFileSync(args.entries, 'utf8').split('\n').map((s) => s.trim()).filter(Boolean);

function probe(entry) {
  return new Promise((resolve) => {
    let done = false;
    const fin = (colo) => { if (done) return; done = true; try { s.destroy(); } catch {} resolve({ entry, colo }); };
    const t = setTimeout(() => fin(null), TIMEOUT);
    const s = tls.connect({ host: entry, port: 443, servername: HOST, ALPNProtocols: ['http/1.1'], rejectUnauthorized: false });
    let buf = '';
    s.once('secureConnect', () => s.write(`GET /healthz HTTP/1.1\r\nHost: ${HOST}\r\nConnection: close\r\n\r\n`));
    s.on('data', (d) => {
      buf += d.toString();
      const i = buf.indexOf('\r\n\r\n');
      if (i >= 0 && buf.length > i + 4) {
        try { const j = JSON.parse(buf.slice(i + 4)); clearTimeout(t); fin(j.colo || null); } catch { clearTimeout(t); fin(null); }
      }
    });
    s.on('error', () => { clearTimeout(t); fin(null); });
    s.on('close', () => { clearTimeout(t); if (!done) fin(null); });
  });
}

const map = {};
for (const e of entries) {
  const r = await probe(e);
  if (r.colo) { map[e] = r.colo; console.log(`✅ ${e.padEnd(28)} ${r.colo}`); }
  else console.log(`❌ ${e}`);
}
writeFileSync(OUT, JSON.stringify(map, null, 1));
const byColo = {};
for (const [k, v] of Object.entries(map)) (byColo[v] ||= []).push(k);
console.log('\n区域分布:', Object.entries(byColo).map(([k, v]) => `${k}:${v.length}`).join(' '));
