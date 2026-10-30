cfmux 控制面（Cloudflare Pages 直接上传包）

【部署步骤】
1. Cloudflare 控制台 → Workers 和 Pages → 创建 → Pages → 上传资产
2. 上传本目录（或 cfmux-control.zip）
3. 项目 → 设置 → 变量和机密，添加：
     uuid           = 数据面 UUID（可先随便填，后台可改）
     admin_password = 你的后台密码
     report_token   = 一串随机字符（测速上报用）
4. 项目 → 设置 → 函数 → KV 命名空间绑定：变量名 KV → 新建/选择命名空间
5. 重新部署（Deploy）让绑定生效
6. 打开 https://<项目>.pages.dev/admin 登录

【注意】
- 必须先部署数据面（cfmux-dataplane.zip），拿到它的 UUID
- 首次进入后台后：数据面 → 填 host/uuid → 优选 → 从网址导入
- 没测速前订阅是空的，这是正常的
