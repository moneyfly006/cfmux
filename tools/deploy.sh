#!/usr/bin/env bash
# 安全部署：先做双重语法校验，再发布
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
set -a; . "${ENV_FILE:-$HOME/cf-deploy/.cf.env}"; set +a
node --check src/index.js
node tools/check-dash.js >/dev/null
node --check /tmp/_dash_check.js
mkdir -p /tmp/cfmux_deploy && cp src/index.js /tmp/cfmux_deploy/_worker.js
wrangler pages deploy /tmp/cfmux_deploy --project-name cfmux 2>&1 | tail -2
