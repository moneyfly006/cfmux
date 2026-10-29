#!/usr/bin/env node
// 部署前校验：把 DASHBOARD_HTML 模板渲染出来，提取 <script> 做语法检查
const fs = require('fs');
const src = fs.readFileSync(process.argv[2] || 'src/index.js', 'utf8');
const m = src.match(/const DASHBOARD_HTML = `([\s\S]*?)`;\n/);
if (!m) { console.error('未找到 DASHBOARD_HTML'); process.exit(2); }
const js = (m[1].match(/<script>([\s\S]*?)<\/script>/) || [])[1];
if (!js) { console.error('未找到 <script>'); process.exit(2); }
fs.writeFileSync('/tmp/_dash_check.js', js);
console.log('提取浏览器 JS:', js.length, '字节');
