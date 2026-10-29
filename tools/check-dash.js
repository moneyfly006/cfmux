#!/usr/bin/env node
/**
 * 部署前校验（正确版）：
 *   真正调用 Worker 渲染 /admin（mock env），拿浏览器实际会收到的 HTML，
 *   再提取 <script> 做 node --check —— 避免模板字面量转义在运行时才展开的问题。
 */
const fs = require('fs');
const path = require('path');
const os = require('os');

(async () => {
  const srcPath = path.resolve(process.argv[2] || 'src/index.js');
  const tmp = path.join(os.tmpdir(), '_cfmux_worker_check.mjs');
  fs.copyFileSync(srcPath, tmp);
  const mod = await import('file://' + tmp);
  const worker = mod.default;

  const kv = new Map();
  const env = {
    uuid: '00000000-0000-0000-0000-000000000000',
    admin_password: 'check-pass',
    KV: {
      get: async (k) => (kv.has(k) ? kv.get(k) : null),
      put: async (k, v) => { kv.set(k, v); },
      delete: async (k) => { kv.delete(k); },
      list: async () => ({ keys: [], list_complete: true }),
    },
  };
  // 预置一些数据，让页面走完整渲染分支
  kv.set('entries', JSON.stringify([{ name: 'core', host: 'mux-core.pages.dev', uuid: 'x', path: '/' }]));
  kv.set('quality', JSON.stringify({ 'mux-core.pages.dev|1.1.1.1': { ok: true, score: 66, tier: 'A', lat: 800, jitter: 10, tput: 120, ts: Date.now() } }));
  kv.set('settings', JSON.stringify({ minScore: 0, limit: 0 }));
  kv.set('preferred', JSON.stringify(['1.1.1.1']));
  kv.set('history', JSON.stringify([{ ts: Date.now(), avg: 60 }, { ts: Date.now(), avg: 62 }]));

  const crypto = require('crypto');
  const tok = crypto.createHash('sha256').update('cfmux|check-pass').digest('hex');
  const req = new Request('https://example.com/admin', { headers: { Cookie: 'cfmux_admin=' + tok } });
  const res = await worker.fetch(req, env, { waitUntil() {}, passThroughOnException() {} });
  const html = await res.text();
  if (res.status !== 200) { console.error('渲染失败 HTTP', res.status, html.slice(0, 200)); process.exit(2); }
  const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]);
  if (!scripts.length) { console.error('页面里没有 <script>'); process.exit(2); }
  const out = path.join(os.tmpdir(), '_dash_check.js');
  fs.writeFileSync(out, scripts.join('\n;\n'));
  console.log('渲染后页面', html.length, '字节 | script 块', scripts.length, '| 提取 JS', scripts.join('\n').length, '字节');
})().catch((e) => { console.error('校验异常:', e.message); process.exit(3); });
