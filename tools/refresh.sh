#!/usr/bin/env bash
# 本地定时刷新：用你自己的网络测质量 → 写回 KV
# 用法: ./tools/refresh.sh
# 依赖: ~/cf-deploy/.cf.env (CLOUDFLARE_API_TOKEN / CLOUDFLARE_ACCOUNT_ID)
set -euo pipefail
cd "$(dirname "$0")/.."
set -a
. "${ENV_FILE:-$HOME/cf-deploy/.cf.env}"
[ -f "$HOME/cfmux/.refresh.env" ] && . "$HOME/cfmux/.refresh.env"
set +a
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
# 追加历史点（供后台趋势图）
python3 - "$OUT" <<'PY' || true
import json, os, sys, subprocess, time
out = json.load(open(sys.argv[1]))
ok = [v for v in out.values() if v.get("ok")]
pt = {"ts": int(time.time()*1000), "ok": len(ok), "total": len(out),
      "avg": round(sum(v.get("score",0) for v in ok)/len(ok),1) if ok else 0,
      "best": round(max((v.get("score",0) for v in ok), default=0),1),
      "dead": len(out)-len(ok)}
acc = os.environ["CLOUDFLARE_ACCOUNT_ID"]; tok = os.environ["CLOUDFLARE_API_TOKEN"]; ns = os.environ["KV_ID"]
base = f"https://api.cloudflare.com/client/v4/accounts/{acc}/storage/kv/namespaces/{ns}/values/history"
cur = subprocess.run(["curl","-sS","--max-time","20",base,"-H",f"Authorization: Bearer {tok}"],capture_output=True,text=True).stdout.strip()
try: hist = json.loads(cur) if cur.startswith("[") else []
except Exception: hist = []
hist = (hist + [pt])[-48:]
subprocess.run(["curl","-sS","--max-time","20","-X","PUT",base,"-H",f"Authorization: Bearer {tok}",
                "-H","Content-Type: application/json","--data-binary",json.dumps(hist)],capture_output=True)
print("[*] history 已追加:", pt)
PY
echo "[+] 完成：$(date '+%F %T')"
