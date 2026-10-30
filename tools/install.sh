#!/usr/bin/env bash
#
# cfmux 一键安装脚本
#
# 前置条件：
#   1. 已安装 node 18+ 与 wrangler（npm i -g wrangler）
#   2. 一个 Cloudflare API Token（权限：Cloudflare Pages:Edit、Workers KV Storage:Edit、Account Settings:Read）
#   3. 该 Token 对应的账号下没有同名项目（或你接受覆盖）
#
# 用法：
#   export CLOUDFLARE_API_TOKEN=xxx
#   ./tools/install.sh
#
# 可选环境变量：
#   CF_ACCOUNT_ID    账号 ID（不填自动探测）
#   ADMIN_PASSWORD   后台密码（不填随机生成）
#   DATA_PLANES      数据面数量，默认 1（建议 2~3）
#   PROJECT_PREFIX   项目名前缀，默认 cfmux（数据面为 ${PREFIX}-core-1 …）
#   KERNEL_URL       内核地址，默认 cfnew 官方最新
#
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

say()  { printf "\033[36m[*]\033[0m %s\n" "$*"; }
ok()   { printf "\033[32m[+]\033[0m %s\n" "$*"; }
warn() { printf "\033[33m[!]\033[0m %s\n" "$*"; }
die()  { printf "\033[31m[x]\033[0m %s\n" "$*" >&2; exit 1; }

[ -n "${CLOUDFLARE_API_TOKEN:-}" ] || die "请先 export CLOUDFLARE_API_TOKEN=..."
command -v node >/dev/null || die "缺少 node"
command -v wrangler >/dev/null || die "缺少 wrangler（npm i -g wrangler）"

API="https://api.cloudflare.com/client/v4"
AUTH=(-H "Authorization: Bearer $CLOUDFLARE_API_TOKEN" -H "Content-Type: application/json")
PREFIX="${PROJECT_PREFIX:-cfmux}"
PLANES="${DATA_PLANES:-1}"
KERNEL_URL="${KERNEL_URL:-https://raw.githubusercontent.com/byJoey/cfnew/core-only/_worker.js}"
KERNEL_MIRROR="https://github.090227.xyz/raw.githubusercontent.com/byJoey/cfnew/core-only/_worker.js"

jqf() { python3 -c "import sys,json;d=json.load(sys.stdin);print($1)" 2>/dev/null; }

# ── 0. 账号与连通性 ─────────────────────────────────────────
say "校验 Token 与账号…"
ACC="${CF_ACCOUNT_ID:-}"
if [ -z "$ACC" ]; then
  ACC=$(curl -sS --max-time 30 "$API/accounts" "${AUTH[@]}" | jqf "d['result'][0]['id']")
  [ -n "$ACC" ] || die "无法获取账号 ID，请设置 CF_ACCOUNT_ID"
fi
ok "账号：$ACC"

ADMIN_PASSWORD="${ADMIN_PASSWORD:-$(openssl rand -base64 12 | tr -d '/+=' | head -c 14)}"
REPORT_TOKEN="$(openssl rand -hex 24)"
CONTROL="${PREFIX}"
CONTROL_UUID="$(uuidgen | tr 'A-Z' 'a-z')"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

kv_create() { # $1=title → id
  curl -sS --max-time 40 -X POST "$API/accounts/$ACC/storage/kv/namespaces" "${AUTH[@]}" -d "{\"title\":\"$1\"}" | jqf "d['result']['id']"
}
page_create() { # $1=name $2=env json $3=kv json
  curl -sS --max-time 40 -X POST "$API/accounts/$ACC/pages/projects" "${AUTH[@]}" -d "{
    \"name\":\"$1\",\"production_branch\":\"main\",
    \"deployment_configs\":{\"production\":{
      \"compatibility_date\":\"2024-09-23\",
      \"env_vars\":$2,\"kv_namespaces\":$3}}}" >/dev/null
}
page_patch() { # 先建后改（某些账号创建时不允许带 deployment_configs）
  curl -sS --max-time 40 -X PATCH "$API/accounts/$ACC/pages/projects/$1" "${AUTH[@]}" -d "{
    \"deployment_configs\":{\"production\":{
      \"compatibility_date\":\"2024-09-23\",
      \"env_vars\":$2,\"kv_namespaces\":$3}}}" >/dev/null
}

# ── 1. 数据面 ───────────────────────────────────────────────
say "准备内核…"
if ! curl -sSfL --max-time 60 "$KERNEL_URL" -o "$WORK/kernel.js" 2>/dev/null || [ ! -s "$WORK/kernel.js" ]; then
  warn "官方地址不可达，改用镜像"
  curl -sSfL --max-time 60 "$KERNEL_MIRROR" -o "$WORK/kernel.js" || die "内核下载失败"
fi
grep -q "export default\|addEventListener" "$WORK/kernel.js" || die "内核文件异常（不是 _worker.js）"
ok "内核 $(wc -c < "$WORK/kernel.js") 字节"

PLANE_INFO=()
for i in $(seq 1 "$PLANES"); do
  NAME="$CONTROL-core-$i"
  UUID="$(uuidgen | tr 'A-Z' 'a-z')"
  say "创建数据面 ${i}：$NAME"
  NS=$(kv_create "${NAME}_kv"); [ -n "$NS" ] || die "KV 创建失败"
  page_create "$NAME" \
    "{\"u\":{\"type\":\"plain_text\",\"value\":\"$UUID\"}}" \
    "{\"C\":{\"namespace_id\":\"$NS\"}}" || true
  page_patch "$NAME" \
    "{\"u\":{\"type\":\"plain_text\",\"value\":\"$UUID\"}}" \
    "{\"C\":{\"namespace_id\":\"$NS\"}}"
  mkdir -p "$WORK/d$i" && cp "$WORK/kernel.js" "$WORK/d$i/_worker.js"
  ( cd "$WORK/d$i" && wrangler pages deploy . --project-name "$NAME" --commit-dirty=true >/dev/null 2>&1 ) \
    || die "数据面部署失败：$NAME"
  ok "数据面 $i 部署完成：https://$NAME.pages.dev"
  PLANE_INFO+=("$NAME|$UUID")
done

# ── 2. 控制面 ───────────────────────────────────────────────
say "创建控制面：$CONTROL"
CNS=$(kv_create "${CONTROL}_kv"); [ -n "$CNS" ] || die "KV 创建失败"
CENV="{\"uuid\":{\"type\":\"plain_text\",\"value\":\"$CONTROL_UUID\"},
      \"admin_password\":{\"type\":\"plain_text\",\"value\":\"$ADMIN_PASSWORD\"},
      \"report_token\":{\"type\":\"plain_text\",\"value\":\"$REPORT_TOKEN\"}}"
CKV="{\"KV\":{\"namespace_id\":\"$CNS\"}}"
page_create "$CONTROL" "$CENV" "$CKV" || true
page_patch "$CONTROL" "$CENV" "$CKV"
mkdir -p "$WORK/c" && cp src/index.js "$WORK/c/_worker.js"
( cd "$WORK/c" && wrangler pages deploy . --project-name "$CONTROL" --commit-dirty=true >/dev/null 2>&1 ) \
  || die "控制面部署失败"
ok "控制面部署完成：https://$CONTROL.pages.dev"

# ── 3. 初始化：写入数据面 + 导入优选源 ──────────────────────
say "等待部署生效…"
BASE="https://$CONTROL.pages.dev"
for i in $(seq 1 30); do
  code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "$BASE/healthz" || true)
  [ "$code" = "200" ] && break
  sleep 4
done
[ "$code" = "200" ] || die "控制面未就绪（$BASE/healthz = ${code}）"

CJ="$WORK/cookie.txt"
login_code=$(curl -s -o /dev/null -w "%{http_code}" -c "$CJ" --max-time 20 -X POST "$BASE/admin/login" -d "password=$ADMIN_PASSWORD")
[ "$login_code" = "302" ] || die "后台登录失败（HTTP ${login_code}），请检查 ADMIN_PASSWORD"

ENTRIES="["
first=1
for info in "${PLANE_INFO[@]}"; do
  n="${info%%|*}"; u="${info##*|}"
  [ $first -eq 0 ] && ENTRIES+=","
  ENTRIES+="{\"name\":\"$n\",\"host\":\"$n.pages.dev\",\"uuid\":\"$u\",\"path\":\"/\"}"
  first=0
done
ENTRIES+="]"
python3 - "$BASE" "$CJ" "$ENTRIES" <<'PY'
import json, subprocess, sys
base, cj, entries = sys.argv[1], sys.argv[2], sys.argv[3]
r = subprocess.run(["curl","-sS","--max-time","30","-b",cj,"-X","POST",base+"/admin/save",
                    "-H","Content-Type: application/json","-d",json.dumps({"entries":entries})],
                   capture_output=True, text=True)
print("  写入数据面:", r.stdout.strip()[:100])
r = subprocess.run(["curl","-sS","--max-time","60","-b",cj,"-X","POST",base+"/api/import",
                    "-H","Content-Type: application/json",
                    "-d",json.dumps({"url":"https://bestcf.fxxk.dedyn.io/cf_domains.txt"})],
                   capture_output=True, text=True)
print("  导入优选域名:", r.stdout.strip()[:120])
PY

# ── 4. 汇总 ─────────────────────────────────────────────────
cat <<EOF

────────────────────────────────────────────────────────────
🎉 安装完成

  控制面后台   $BASE/admin
  后台密码     $ADMIN_PASSWORD
  上报口令     $REPORT_TOKEN
  面板状态页   $BASE/panel
  订阅地址     $BASE/sub          （Base64）
               $BASE/sub?target=clash
               $BASE/sub?target=singbox

  数据面：
$(for info in "${PLANE_INFO[@]}"; do echo "    https://${info%%|*}.pages.dev"; done)

  下一步（重要）：
    1. 打开后台 → 数据面，确认 host/uuid 正确
    2. 跑一次测速（三选一）：
       a) 本机：cp .refresh.env.example .refresh.env 后执行 ./tools/refresh.sh
       b) 云端：仓库 Settings → Secrets 加 CFMUX_REPORT_TOKEN=$REPORT_TOKEN
                Variables 加 CFMUX_BASE=$BASE
       c) 后台「优选」→「手动触发测速」
    3. 测速完成前，订阅会是空的（没有质量数据 = 没有达标节点）

  建议立即修改后台密码：Cloudflare 控制台 → $CONTROL → 设置 → 变量 → admin_password
────────────────────────────────────────────────────────────
EOF
