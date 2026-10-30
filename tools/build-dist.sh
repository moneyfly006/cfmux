#!/usr/bin/env bash
# 构建可直接上传 Cloudflare Pages 的发布包
set -euo pipefail
cd "$(dirname "$0")/.."
KERNEL_LOCAL="${KERNEL_LOCAL:-/tmp/kernel.js}"
KERNEL_URL="https://raw.githubusercontent.com/byJoey/cfnew/core-only/_worker.js"
KERNEL_MIRROR="https://github.090227.xyz/raw.githubusercontent.com/byJoey/cfnew/core-only/_worker.js"

rm -rf dist && mkdir -p dist/control dist/dataplane
cp src/index.js dist/control/_worker.js
cp README_DATA_PLANE.txt dist/dataplane/README.txt 2>/dev/null || true

if [ -s "$KERNEL_LOCAL" ]; then cp "$KERNEL_LOCAL" dist/dataplane/_worker.js
else
  curl -sL --max-time 60 "$KERNEL_URL" -o dist/dataplane/_worker.js 2>/dev/null || \
  curl -sL --max-time 60 "$KERNEL_MIRROR" -o dist/dataplane/_worker.js
fi
[ -s dist/dataplane/_worker.js ] || { echo "内核下载失败"; exit 1; }

cp README_CONTROL.txt dist/control/README.txt 2>/dev/null || true

# 语法校验（ESM）
cp dist/control/_worker.js /tmp/_c.mjs && node --check /tmp/_c.mjs
cp dist/dataplane/_worker.js /tmp/_d.mjs && node --check /tmp/_d.mjs

(cd dist/control && zip -qr ../cfmux-control.zip .)
(cd dist/dataplane && zip -qr ../cfmux-dataplane.zip .)
(cd dist && shasum -a 256 *.zip > SHA256SUMS.txt)
echo "== 产物 =="; ls -la dist/*.zip; cat dist/SHA256SUMS.txt
