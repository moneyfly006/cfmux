cfmux 数据面（cfnew 内核，Cloudflare Pages 直接上传包）

【部署步骤】
1. Cloudflare 控制台 → Workers 和 Pages → 创建 → Pages → 上传资产
2. 上传本目录（或 cfmux-dataplane.zip）
3. 项目 → 设置 → 变量和机密：添加  u = <你的 UUID>（uuidgen 生成）
4. 项目 → 设置 → 函数 → KV 命名空间绑定：变量名 C → 新建命名空间
5. 重新部署
6. 访问 https://<项目>.pages.dev/ 应返回 hello
   https://<项目>.pages.dev/<UUID>/api/config 应返回 JSON

【说明】
内核来自 cfnew（VLESS / Trojan / XHTTP / ECH / ProxyIP 全功能），本项目未修改。
