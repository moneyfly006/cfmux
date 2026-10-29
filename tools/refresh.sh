#!/usr/bin/env bash
# 本地定时刷新：用你自己的网络测质量 → 写回 KV
# 用法: ./tools/refresh.sh
# 依赖: ~/cf-deploy/.cf.env (CLOUDFLARE_API_TOKEN / CLOUDFLARE_ACCOUNT_ID)
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . "${ENV_FILE:-$HOME/cf-deploy/.cf.env}"; set +a
KV_ID="${KV_ID:?请设置 KV_ID（cfmux 项目的 KV 命名空间 id）}"
PREF="${PREF_FILE:-/tmp/pref.txt}"
ENT="${ENTRIES_FILE:?请设置 ENTRIES_FILE（entries.json 路径）}"
OUT="${OUT_FILE:-/tmp/quality.json}"

[ -f "$PREF" ] || { echo "缺少优选列表 $PREF"; exit 1; }

node tools/quality.mjs --entries "$ENT" --preferred "$PREF" --out "$OUT" --timeout 9000 --bytes 262144

echo "[*] 写入 KV …"
curl -sS --max-time 60 -X PUT \
  "https://api.cloudflare.com/client/v4/accounts/$CLOUDFLARE_ACCOUNT_ID/storage/kv/namespaces/$KV_ID/values/quality" \
  -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" -H "Content-Type: application/json" \
  --data-binary @"$OUT" | head -c 120; echo
echo "[+] 完成：$(date '+%F %T')"
