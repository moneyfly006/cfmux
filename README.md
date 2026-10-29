# cfmux — Cloudflare 边缘多路复用代理（自研）

> 一个「数据面 + 控制面」分离的 Cloudflare Workers / Pages 代理项目。
> 数据面是自研的 VLESS-over-WebSocket 内核（不依赖 ProxyIP），控制面负责**实测优选**、健康过滤与多客户端订阅。

## 一、先看结论：实测数据（2026-10-29，郑州电信 → Cloudflare）

| 指标（各 5 次延迟 + 2 次下载） | cfmux（本项目） | cfnew（对照） |
| --- | --- | --- |
| 204 探测延迟 | 均值 **2.24 s**（1.40–3.23） | 均值 2.09 s（1.26–2.83） |
| 1 MB 下载 | 均值 **~198 KB/s**（191–204） | 均值 ~126 KB/s（104–149） |
| 优选后握手 RTT | TCP 177 ms / TLS 389 ms / WS 637 ms | — |
| 边缘 IP 可用率 | 30 个候选中 18 个可用（60%） | — |

> 结论：**延迟与成熟项目同量级**（受 CN→US West 回源路径限制），**吞吐本次实测更优**，
> 本项目的增益主要在**可控性与稳定性**：优选可实测、死节点自动剔除、自带诊断。

## 二、同类项目原理分析

### 2.1 共同的数据模型

这类项目的本质是 **「借 Cloudflare 的边缘节点做 L4/L7 转发」**：

```
客户端 --TLS/WS--> CF 边缘(anycast) --Worker 运行时--> connect()/fetch() --> 目标
```

- **VLESS/Trojan over WebSocket**：把代理协议塞进 WS 帧，CF 只看到「一个 WebSocket 请求」。
- **配额模型**：免费版按**请求数**计费（100k 请求/天），**不按字节**。
  于是「长连接 + 多路复用」= 用 1 个请求搬运大量数据 → 这就是"白嫖"的数学基础。
- **出口两条路**：
  1. `connect()`（`cloudflare:sockets`）—— 真正的 TCP 直连，**能代理任意 TCP**；
  2. `fetch()` —— 只能 HTTP(S)，但不受 sockets 限制；
  3. **ProxyIP** —— 把 TCP 交给第三方中转，绕开 `connect()` 的限制（也绕开了自己 Worker 的出口）。

### 2.2 edgetunnel（cmliu）的取舍

- 单文件巨型内核（上万行），把管理后台、订阅、优选、ProxyIP 全塞进一个 Worker。
- 优点：一座部署全都有；缺点：**每请求都要解析大量配置/模板，CPU 预算紧**，
  且代码体积大 → 审计困难、改动风险高。
- 默认依赖 **ProxyIP** 才能跑非 HTTP；`connect()` 能直连时反而不走。

### 2.3 cfnew（byJoey）的取舍

- 拆成「精简内核（~37 KB）+ 独立面板」：内核只做代理 + `/api/config`，面板生成订阅。
- 优点：内核小、CPU 省、配置可热更（KV）。
- 缺点：**订阅与优选完全依赖第三方面板**；优选列表由面板下发，
  你无法在**自己的网络**里实测——面板测的是面板服务器的延迟，不是你的。

### 2.4 共性瓶颈（我据此做的改进）

| 瓶颈 | 表现 | cfmux 的做法 |
| --- | --- | --- |
| 半包/粘包 | VLESS 头跨多个 WS 帧时解析失败，连接被误杀 | 显式状态机 `header→connecting→open`，攒够再解析 |
| 连接并发竞态 | 头帧解析中收到数据帧，被当成新头 → `bad version` | 连接期单独队列 `queued`，建连后按序冲刷 |
| 死 IP 拖慢 | `connect()` 对不可达 IP 要等 20 s 才失败 | `Promise.race` 超时（默认 8 s）+ 保留网段预拒绝 |
| 无脑优选 | 面板/公共列表的 IP 与你的网络无关 | **本地实测**：TLS+WS+端到端三层测量后写 KV |
| 配置读放大 | 每请求读 KV/解析大 JSON | 配置全走 env，isolate 内零外部依赖 |
| 排障靠猜 | 出问题只能看客户端日志 | `/diag` 自检（sockets/fetch/colo）+ `/healthz` |

## 三、架构

```
                    ┌──────────────── cfmux（Pages / Workers）────────────────┐
客户端 --WS/TLS-->  │  index.js                                              │
(sing-box/         │   ├─ /            任意路径 → VLESS 解析 → connect()      │
 Shadowrocket)      │   ├─ /healthz     健康检查（含 colo）                    │
                    │   ├─ /diag        能力自检（sockets 可用性等）            │
                    │   └─ /sub         订阅生成（base64 / clash / singbox）   │
                    └────────────────────────────────────────────────────────┘
                                     ▲
        tools/measure.mjs  ── 实测优选 ──┘（写入 KV: preferred）

GitHub Actions（每 6 小时）：measure → 写 KV → 订阅自动更新
```

### 关键实现点

1. **零依赖数据面**：只用 `cloudflare:sockets` 的 `connect()`，不引入 ProxyIP。
2. **首包聚合**：VLESS 头可能分片，先攒够 `version+uuid+addonLen` 再解析。
3. **双向背压**：`socket.readable.pipeTo(WS)` + `writer.write()`，不整包缓存。
4. **保留段预拒绝**：`1.1.1.0/24`、私网、TEST-NET 等直接拒绝，省 20 s。
5. **订阅即优选**：`/sub` 读 KV 里的实测排序，输出 `url-test` / `fallback` 组。

## 四、部署

```bash
# 1. 建 Pages 项目并设 UUID
export CLOUDFLARE_API_TOKEN=...   # Pages:Edit
export CLOUDFLARE_ACCOUNT_ID=...
wrangler pages project create cfmux --production-branch main
# 控制台 → 项目 → 设置 → 变量：uuid = <你的 UUID>；可选 KV 绑定名字 KV

# 2. 部署内核
mkdir -p dist && cp src/index.js dist/_worker.js
wrangler pages deploy dist --project-name cfmux

# 3. 验证
curl https://cfmux.pages.dev/diag     # sockets.ok 应为 true
curl https://cfmux.pages.dev/healthz

# 4. 实测优选（在你自己的网络跑，不要用 CI 的结果代替）
UUID=<你的 UUID>
node tools/measure.mjs --host cfmux.pages.dev --uuid $UUID \
  --ips 104.17.2.1,104.16.3.1,188.114.97.1 --out preferred.json

# 5. 把 preferred.json 写进 KV 的 preferred 键，订阅即用最优 IP
```

## 五、订阅

```
https://<你的域名>/sub?target=base64   # V2Ray / Quantumult X
https://<你的域名>/sub?target=clash    # Clash / Mihomo / Stash
https://<你的域名>/sub?target=singbox  # sing-box
```

参数：`limit`（节点数，默认 12）、`host`（SNI/Host，默认当前域名）、`path`（WS 路径）。

## 六、已知限制（重要）

1. **不能连 Cloudflare 自家 IP**（`connect()` 到 CF 网段会被拒），所以
   `speed.cloudflare.com`、`1.1.1.1` 等目标必须走 `fetch` 或 ProxyIP。
2. **不支持 UDP**：sockets API 无 UDP，游戏/QUIC/部分 DNS 会回落到 TCP。
3. **免费版 CPU 10 ms/请求**：大流量长连接没问题（流式转发 CPU 低），
   但不要在同一请求里做重计算。
4. **回源机房由边缘决定**：从中国大陆出网常落在 SJC/LAX，
   实测 800 ms~2.8 s；**优选 IP 能改善首跳，但不能改变回源地理**。
5. 免费额度 100k 请求/天/项目；WS 长连接只计 1 次，但**每个新请求都算**，
   所以客户端要开 `url-test` 复用连接，不要频繁新建。

## 七、与同类项目的定位差异

| | edgetunnel | cfnew | **cfmux** |
| --- | --- | --- | --- |
| 内核体积 | 巨大 | ~37 KB | **~9 KB** |
| 依赖 ProxyIP | 默认是 | 可选 | **不依赖** |
| 优选来源 | 面板/公共列表 | 面板 | **本地实测 + KV** |
| 订阅 | 内置 | 第三方面板 | **内置，且与优选联动** |
| 自检/诊断 | 无 | 无 | **/diag + /healthz** |
| 配置热更 | 复杂 | KV | KV（仅优选表） |

## 八、License

MIT

## 九、面板与优选集成（v1.1）

### 面板页
`https://<你的域名>/panel` —— 自带的可视化控制台（Pages 直接托管，无需登录）：

- 一键复制/打开三种订阅（通用 / Clash / sing-box）
- **优选资源查询**：按关键字过滤（如 `hk`、`skk.moe`、`104.17`），按区域分组展示
- 诊断面板：实时显示 colo、sockets 可用性、uuid 是否配置

### 外部优选源（IP + 域名）
内置接入 [BestCF](https://bestcf.fxxk.dedyn.io/)（cmliu/CF-Pages-BestCF）的优选域名表，
并可用 `env.sources` 追加任意返回「一行一个」的列表地址（自动过滤注释与 HTML）：

```bash
# 追加你自己的优选源（逗号分隔）
sources = "https://bestcf.fxxk.dedyn.io/cf_domains.txt,https://example.com/my_ips.txt"
```

合并顺序：**KV 实测排序（preferred） > 外部源**。实测 436 条（10 IP + 426 域名）。

### 按区域（国家/地区）选节点
CF 的边缘 colo 决定回源地区，所以「选国家」= 选落地 colo。用 `tools/colos.mjs` 探测：

```bash
node tools/colos.mjs --host <你的域名> --entries ips.txt --out colos.json
# 写入 KV 的 colos 键后，订阅即可按区域过滤
curl "https://<域名>/sub?region=SIN&limit=20"     # 新加坡
curl "https://<域名>/sub?region=AMS&limit=20"     # 阿姆斯特丹
```

实测区域分布（26 条抽样）：`AMS:10 LAX:6 SIN:4 FRA:2 SJC:1 NRT:1`。

> ⚠️ **实测结论：区域标签 ≠ 质量**。抽样的 SIN 域名节点延迟 2.1–9.5 s、49 KB/s，
> 反而比裸 IP（`104.17.2.1`，LAX，198 KB/s）差。原因：域名经 CNAME 到不同 colo，
> 且同一域名在不同时刻落地不同。**所以订阅默认按实测分排序，区域只作筛选维度。**

### 节点数量
`/sub?limit=N` 支持最多 **300** 个节点（默认 12），混合 IP 与域名；
Clash 输出自动分组 `自动优选(url-test)` + `故障切换(fallback)`。

### 部署步骤（完整）
见本文件「四、部署」+ 上面两节：建 Pages → 设 `uuid`/`KV`/`sources` → 部署 →
跑 `measure.mjs` 写 `preferred` → 跑 `colos.mjs` 写 `colos` → 访问 `/panel` 验证。

## 十、后台管理（v1.2）

### 地址与登录
- 后台：`https://<你的域名>/admin`
- 未设 `admin_password` 时后台不可用（页面会提示）；设置后为密码登录，
  cookie = `SHA-256("cfmux|" + 密码)`，HttpOnly + Secure + SameSite=Lax，7 天有效。
- 前端状态页（无需登录）：`/panel`

### 后台能做什么
| 区块 | 说明 |
| --- | --- |
| 订阅 | 三种格式链接一键打开 |
| 基本设置 | 优选源（`sources`，逗号分隔 URL）、默认节点数（1–300） |
| 优选表 `preferred` | KV 里的实测排序（JSON 数组），可手工编辑/粘贴 |
| 区域映射 `colos` | `ip/域名 → colo` 映射，供 `?region=` 过滤 |
| 保存 | 写入 KV，订阅**立即**生效（无需重新部署） |

### 设置/修改密码
```bash
# Cloudflare 控制台 → Workers 和 Pages → cfmux → 设置 → 变量和机密
# 添加或修改： admin_password = <你的新密码>
# 变量改动后需要「重新部署」才生效（页面上点 Retry deployment 或重新 push 一次）
```

### 已验证行为
```
GET  /admin           未登录 → 200 登录页
POST /admin/login     错误密码 → 401
POST /admin/login     正确密码 → 302 + Set-Cookie
GET  /admin           带 cookie → 200 控制台
POST /admin/save      → {"ok":true}，/sub 立即按新 limit 输出
POST /admin/save      未登录 → 401 {"ok":false,"msg":"未登录"}
```

## 十一、v2.0 架构调整：**数据面用完整内核，控制面做质量管控**

> 前一版我自研的精简内核功能不全（缺 Trojan/XHTTP/家宽等），不符合「功能全保留」的要求。
> v2.0 起改为两层：

```
┌─ 数据面 mux-core.pages.dev ──────────────┐   ← 完整内核（cfnew 2026.10.17.2）
│  VLESS WS / Trojan WS / VLESS XHTTP      │      优选 / KV 配置 / ProxyIP / ECH…
│  /api/config                             │
└──────────────────────────────────────────┘
                 ▲ Host/SNI 指向它
┌─ 控制面 cfmux.pages.dev ─────────────────┐   ← 本项目（质量层）
│  /admin    后台（密码登录）               │
│  /sub      质量驱动订阅                   │
│  /api/quality、/panel                    │
└──────────────────────────────────────────┘
     ▲ tools/quality.mjs 实测 → KV: quality
```

### 质量引擎：`tools/quality.mjs`

对「数据面 × 优选入口」的每个组合做四件事：

1. **握手**：TCP → TLS → WS 三段 RTT
2. **可用性**：VLESS 请求 `generate_204`，连续 3 次
3. **抖动**：3 次延迟的 `max - min`
4. **吞吐**：经隧道下载 ~256 KB，算 **KB/s**

评分公式（0–100）：

```
score = 延迟分×0.5 + 吞吐分×0.35 + 抖动分×0.15
延迟分 = max(0, 100 - lat/60)        吞吐分 = min(100, KBps/2)      抖动分 = max(0, 100 - jitter/5)
```

分级：`S≥70 / A≥45 / B≥20 / C<20`

### 订阅只放行达标节点

`/sub` 会按 `quality` 表过滤：**`ok=false` 直接丢弃；`score < minScore` 或 `tput < minTput` 丢弃；
其余按评分降序**。首次无质量数据时可用 `requireQuality=false` 放开（默认 true，宁缺毋滥）。

响应头直接暴露筛选结果：

```
x-cfmux-total: 436      ← 候选总数
x-cfmux-pass: 5         ← 通过质量门槛的
x-cfmux-nodes: 5        ← 实际输出
x-cfmux-minscore: 45    ← 当前门槛
```

### 实测（2026-10-29）

| 候选 | 通过 | 评分区间 | 榜首节点实测 |
| --- | --- | --- | --- |
| 436 条（10 IP + 426 域名） | **5 条** | 49.1 – 61.5（全 A 级） | 204: 2.6–3.9s · 下载 **194 KB/s** · google 200 |

### 后台新增（v2.0）

| 区块 | 能做什么 |
| --- | --- |
| 数据面条目 `entries` | 增删数据面（name/host/uuid/path），支持多入口 |
| 质量门槛 | `minScore`、`minTput`、`requireQuality` |
| 质量表 | 前 20 名按评分排序，显示等级/延迟/抖动/吞吐 |
| 优选源 / 优选表 / 区域映射 | 同 v1.2 |

### 部署数据面（完整内核）

```bash
# 1) 取最新内核
curl -sL https://github.com/byJoey/cfnew/releases/latest/download/_worker.js -o kernel.js

# 2) 建 Pages 项目（uuid + KV 绑定 C）
wrangler pages project create mux-core --production-branch main
# 控制台设：u = <UUID>；KV 绑定 C = <命名空间>

# 3) 部署
mkdir -p core && cp kernel.js core/_worker.js
wrangler pages deploy core --project-name mux-core

# 4) 验证：/ 应返回 hello；/<uuid>/api/config 应返回 JSON
```

## 十二、v2.1：多数据面 · 真实国家 · 质量新鲜度

### 1. 多数据面（3 入口分摊免费配额）
| 数据面 | 用途 |
| --- | --- |
| `mux-core.pages.dev` | 主入口 |
| `mux-core-2.pages.dev` | 分摊 100k 请求/天 |
| `mux-core-3.pages.dev` | 分摊 + 故障隔离 |

订阅自动跨入口合并、按质量分排序（实测 3×5=15 个组合全部可用）。

### 2. 节点名显示**真实国家**（不是 colo 猜测）
`tools/quality.mjs` 会**经隧道**访问 `ip-api.com`，读取该节点的**真实出口国家/城市**：

```
✅ mux-core.pages.dev @ 104.17.2.1  lat=803 jit=3176 tput=142KB/s score=68.16 A US/East Los Angeles
✅ mux-core.pages.dev @ 188.114.97.1 lat=1267 jit=1187 tput=129KB/s score=62.02 A NL/Amsterdam
```

节点名优先级：**真实国家（geo）→ colo 中文名 → 类型标记**，输出如
`美国-core-01-A`、`荷兰-阿姆斯特丹-core2-08-A`。

### 3. 质量新鲜度（防止拿旧数据当高质）
- `maxAgeHours`（默认 24）：超过该时长的质量数据视为过期，节点不进订阅
- 后台可改；配合 `tools/refresh.sh` 定时刷新，保证订阅里的节点是**近期实测达标**的

### 4. 本地定时刷新（推荐，用你自己的网络）
```bash
# 环境变量
export KV_ID=<cfmux 的 KV 命名空间 id>
export ENTRIES_FILE=/path/entries.json
export PREF_FILE=/path/preferred.txt
./tools/refresh.sh          # 测质量 → 写 KV → 订阅立即更新

# macOS 定时（每 2 小时），可加到 crontab
0 */2 * * * cd ~/cfmux && KV_ID=xxx ENTRIES_FILE=... PREF_FILE=... ./tools/refresh.sh >> ~/cfmux-refresh.log 2>&1
```

### 5. 后台修复与增强
- 修复 `/admin` 1101（`esc` 提前引用导致的 TDZ 异常）
- 新增：数据面条目编辑、质量门槛（`minScore`/`minTput`/`maxAgeHours`/`requireQuality`）、质量表（含国家列）

### 实测（2026-10-29）
```
数据面 3 个 × 优选 5 个 = 15 组合，全部可用
评分区间 46.5 – 70.97，全部 A/S 级
真实国家：US/洛杉矶、NL/阿姆斯特丹（经隧道实测，非 colo 推断）
订阅: 436 候选 → 15 通过 → 按分排序输出
```

## 十三、v2.2：完整管理后台（仪表盘）

> 上一版后台只是"设置表单"，已重做为**多标签管理控制台**。

### 界面结构

| 标签 | 内容 |
| --- | --- |
| 📊 概览 | 6 张指标卡（达标节点/平均分/最高分/平均吞吐/熔断数/数据面数）+ 评分趋势 SVG 折线 + Top 节点表 |
| 🛰 节点质量 | 全量节点表（国家/数据面/入口/等级/评分/延迟/抖动/吞吐/失败次数/状态），支持关键字 + 等级 + 状态筛选，**每行可一键熔断/解除** |
| 📥 订阅 | 三种客户端订阅地址，一键复制 |
| 🧩 数据面 | 表格化增删改（名称/域名/UUID/路径），保存即生效 |
| ⚙️ 设置 | 质量门槛 6 项 + 优选源 + 区域映射（JSON 编辑器） |

### 新增 API

| 端点 | 作用 |
| --- | --- |
| `GET /api/state` | 仪表盘一次性拉取（settings/entries/quality/colos/history），需登录 |
| `POST /api/action` | `{"type":"ban"\|"unban","key":"<host>\|<ip>"}` 手动熔断/恢复 |
| `GET /api/quality?n=50` | 质量排行（公开，便于外部监控） |
| `GET /api/entries` | 优选条目与区域分组（公开） |

### 熔断与每入口择优

- **熔断**：`tools/quality.mjs` 连续失败 3 次 → 写 `cooldownUntil`（默认 2h），订阅自动剔除；后台可手动熔断/解除
- **每入口择优** `perEntry`（默认 2）：每个数据面各自取 top-K，避免某个入口霸榜，保证多入口都能分担流量
- **趋势**：每次刷新追加一个历史点（`history`，保留 48 个），后台用 SVG 画评分曲线

### 订阅缓存
`/sub` 结果进 Cache API（60s），同一订阅 URL 命中缓存直接返回，省 KV 读与 CPU。
改动配置后最多 60s 生效（可用 `?t=<now>` 破缓存）。

### 实测

```
/api/state → 条目 3 · 质量样本 15 · 设置 minScore=45 maxAgeHours=48
熔断 → /api/quality 显示 cooldown，/sub 自动剔除该节点
订阅 perEntry=2 → 3 入口 × 2 = 6 个节点（每入口最优）
```

## 十四、v2.3：去重 · 扩大实测 · 多源地理 · 使用说明

### 1. 节点去重（默认开启）
同一个优选域名可能解析到同一台边缘 IP，导致订阅里出现"重复 IP"。
现在按**真实出口 IP**（geo 返回的 `query`）去重，其次按**解析 IP**，保留评分最高的一条：

```
x-cfmux-pass: 20     ← 通过质量门槛
x-cfmux-dedup: 1     ← 被去重掉的重复项
x-cfmux-nodes: 19    ← 实际输出
```
`?dedup=0` 可关闭。

### 2. 扩大实测范围（达标率问题的根因）
之前只有 13 条达标，是因为**只测了 13 个候选**（5 IP + 8 域名），其余 400+ 域名从未实测，
`requireQuality=true` 下自然不进订阅。

现在 `tools/quality.mjs` 支持**并发测量**（`--concurrency`）与**快速发现模式**（`--fast`）：

```bash
# 第一步：快速发现（只测延迟+国家，1 次探测）
node tools/quality.mjs --entries entries.json --preferred cand.txt --out discover.json --fast --concurrency 8

# 第二步：对可用候选做全量质量测量（延迟×3 + 抖动 + 吞吐 + 地理）
node tools/quality.mjs --entries entries.json --preferred alive.txt --out quality.json --concurrency 6
```

实测：**84 个候选 → 71 可用 → 2 入口 × 71 = 142 组合全部可测**，
国家分布 `US:38 SG:9 NL:11 DE:4 JP:3`。

### 3. 多源地理探测（国家更完整）
`ip-api.com` 有速率限制，大量测量时会失败。现在按顺序回退三个源，各重试 2 次：

```
ip-api.com/json → ipinfo.io/json → ip.3322.net（仅返回 IP）
```

节点名优先用**真实出口国家**，缺少时回退 colo 中文名，再回退 `IP/域名` 标记。

### 4. 订阅默认返回全部达标节点
`limit` 默认 `0` = 不限制（上限 300）。`perEntry` 默认 `0` = 每入口全量参与。
需要精简时再传 `limit=10`。

### 5. 使用说明（面向使用者）
- 公开状态页 `/panel` 新增**使用说明**卡片：三步上手、全部参数含义、常见问题
- 参数速查：

| 参数 | 含义 |
| --- | --- |
| `target` | `base64`(默认) / `clash` / `singbox` |
| `limit` | 节点数，0=全部（≤300） |
| `kind` | `ip` 只要优选 IP / `domain` 只要优选域名 |
| `like` | 关键字筛选（国家/关键字） |
| `perEntry` | 每入口取几个（0=不限） |
| `dedup` | `0` 关闭去重 |
| `nocache` | `1` 跳过 60s 缓存 |

### 6. 阈值可调（达标率与质量的平衡）
| 门槛 | 默认 | 说明 |
| --- | --- | --- |
| `minScore` | 45 | 评分下限（0–100），调低=更多节点 |
| `minTput` | 0 | 吞吐下限 KB/s |
| `maxAgeHours` | 24 | 数据有效期，过期不参与 |
| `requireQuality` | true | 无实测数据的候选一律排除 |

## 十五、v2.4 实测结果（2026-10-29 最终）

### 订阅规模
```
候选 436（IP + 优选域名） → 通过 188 → 去重 41 → 输出 147 个节点
```

### 国家分布（真实出口，经隧道实测）
| 国家 | 节点数 |
| --- | --- |
| 🇸🇬 新加坡 | 38 |
| 🇺🇸 美国 | 31 |
| 🇳🇱 荷兰 | 19 |
| 🇯🇵 日本 | 15 |
| 🇩🇪 德国 | 6 |
| 🇬🇧 英国 | 2 |
| 未识别（geo 超时） | 36（标记为「域名」/「IP」）|

### 全流程实测（三阶段）
| 阶段 | 命令 | 结果 |
| --- | --- | --- |
| 快速发现 | `--fast --concurrency 10` | 426 域名 → 271 可用 |
| 全量质量 | `--concurrency 6` | 142 + 80 组合全部可测 |
| 合并去重 | 自动 | 218 组合 → 147 节点 |

### 后台全功能验收（21/21 通过）
```
✅ 未登录 /admin 登录页        ✅ 错误密码 401
✅ 正确密码 302+Cookie        ✅ 仪表盘 5 标签渲染
✅ /api/state 未登录 401       ✅ /api/state 登录全量
✅ 设置保存+生效+还原          ✅ 数据面保存+生效
✅ 区域映射保存+生效           ✅ 手动熔断
✅ 解除熔断                   ✅ /admin/save 未登录 401
✅ /api/action 未登录 401      ✅ 订阅 base64（28KB）
✅ 订阅 clash（30KB）          ✅ 订阅 singbox（46KB）
✅ /panel  ✅ /healthz  ✅ /diag  ✅ /api/entries  ✅ /api/quality
```

### 关键修复
| 问题 | 根因 | 修复 |
| --- | --- | --- |
| 只有 13 个达标 | 候选列表被默认 `limit=12` 截断 | `limit=0` 时候选全量参与 |
| 国家不全 | ip-api 速率限制 | 三源回退（ip-api → ipinfo → ip.3322）|
| IP 重复 | 多个域名解析到同一 IP | 按真实出口 IP 去重，保留评分最高 |
| 订阅节点少 | 默认 limit=12 + perEntry=2 | 默认 `limit=0` 全部、`perEntry=0` 不限 |
| 功能难理解 | 无说明 | `/panel` 使用说明 + 参数表 + FAQ |

## 十六、工作原理：设置怎么生效？节点怎么抓、怎么测？

### 1. 三条链路

```
① 抓取（Worker 内，请求时）
   外部优选源(bestcf cf_domains.txt 426 条) ──┐
   KV: preferred（实测排序的 IP）      ──┤→ 合并去重 → 候选表
                                          │   （Cache API 缓存 10 分钟）

② 测速（你的 Mac，launchd 每 2 小时）
   tools/quality.mjs
     对每个「数据面 × 候选」：
       TLS+WS 握手 → VLESS 请求 204 × 3 次（延迟/抖动）
       → 隧道下载 256KB（吞吐 KB/s）
       → 经隧道查 geo（真实出口国家）
       → 评分 + 分级 → 写 KV: quality
     （Worker 本身不测速，因为它看不到你到边缘的链路）

③ 出订阅（Worker 内，请求时）
   KV: quality + settings → 过滤(门槛/时效/熔断/去重) → 排序 → 生成订阅
     （响应缓存 60s，?nocache=1 立即生效）
```

### 2. 后台每个设置的生效方式（已实测）

| 设置 | 作用点 | 实测效果（同一时刻） |
| --- | --- | --- |
| `minScore` | ③ 过滤 | 0→**157** 个；60→**74** 个 |
| `minTput` | ③ 过滤 | 0→157；80→**119** 个 |
| `perEntry` | ③ 每入口取几个 | 0→157；1→**2** 个 |
| `limit` | ③ 输出上限 | 0→157；5→**5** 个 |
| `maxAgeHours` | ③ 数据时效 | 168→157；1（数据新）→157 |
| `requireQuality` | ③ 是否放行无实测 | true→157；false→**300**（上限）|
| `sources` | ① 抓取源 | 改后下次请求生效（10 分钟缓存）|
| `entries` | ①③ 数据面列表 | 改后立即改变节点前缀与数量 |
| `colos` | ③ 国家名回退 | 无 geo 时用 colo 映射国家 |

> 所有设置都写在 KV 的 `settings` 键；`/sub` 每次请求都会读取，
> 只有**响应**有 60 秒缓存（`?nocache=1` 可跳过）。

### 3. 节点抓取规则

- **外部优选源**：默认 `https://bestcf.fxxk.dedyn.io/cf_domains.txt`（426 个优选域名），
  可在后台 `sources` 追加任意「一行一个」的地址
- **KV `preferred`**：实测排序后的 IP 列表
- **合并去重**：同一值只保留一次；输出时再按**真实出口 IP** 去重
- 只有「数据面 × 候选」在 `quality` 表里 **ok=true 且未过期** 的才会进订阅

### 4. 测速评分

```
score = 延迟分×0.5 + 吞吐分×0.35 + 抖动分×0.15
延迟分 = max(0, 100 - lat/60)   吞吐分 = min(100, KBps/2)   抖动分 = max(0, 100 - jitter/5)
分级：S≥70 / A≥45 / B≥20 / C<20
```
连续失败 3 次 → 自动熔断 2 小时（`cooldownUntil`），订阅自动剔除。

### 5. 定时任务（已修复）
launchd 的 PATH 不含 Homebrew，之前报 `node: command not found`（任务实际没跑）。
已修复：`refresh.sh` 显式补 `/opt/homebrew/bin`，并在找不到 node 时明确报错退出。

```bash
launchctl list | grep cfmux        # 查看状态（第二列 0 = 上次成功）
tail -f ~/cfmux/data/refresh.log   # 查看每次刷新日志
```

新增合并逻辑：每轮只测 45 个候选 × 3 入口，**不会冲掉**之前的 218 条质量数据
（未测到且 7 天内的保留）。

## 十七、v2.5：可操作性补全（导入 / 增删 / 手动触发 / 置顶 / 二维码）

### 1. 修复：数据面「添加」没反应
根因是前端 `renderEntries()` 每次渲染都用服务端数据覆盖编辑态，刚加的行立刻被冲掉。
已改为：编辑态 `EDIT.entries` 只在 `load()` 时初始化，增删改只操作编辑态，保存才写 KV。
实测：添加 `core4` → `/api/state` 立即出现 4 条 → 还原后回到 3 条。

### 2. 新增「⭐ 优选」标签
| 能力 | 说明 |
| --- | --- |
| 手动编辑 | 文本框一行一个，直接粘贴 IP/域名，保存即生效 |
| **从网址导入** | 填任意「一行一个」的列表 URL，服务端抓取解析、去重合并（实测导入 BestCF 426 条） |
| 手动触发测速 | 调 GitHub Actions；未配置时自动复制本地命令 |
| 实时条数 | 面板显示当前优选列表数量 |

### 3. 新增数据面测速状态
数据面表格新增「测速」列：`53/53`（已测/可用）或「未测速」。
新增数据面若还没测过，订阅不会出现它的节点——可用 `?unmeasured=1` 临时放行。

### 4. 新增置顶（pin）
节点表每行「置顶」按钮 → 订阅里该节点排最前（`POST /api/action {"type":"pin","key":"host|value"}`）。

### 5. 新增二维码
订阅页自动生成通用订阅二维码，手机扫码直接导入。

### 6. 订阅参数（完整）
| 参数 | 说明 |
| --- | --- |
| `target` | base64 / clash / singbox |
| `limit` | 节点数，0=全部 |
| `kind` | ip / domain |
| `cc` | 国家代码，如 `cc=SG,JP` |
| `country` | 国家名，如 `country=新加坡` |
| `like` | 候选值关键字 |
| `perEntry` | 每入口取几个，0=不限 |
| `dedup` | 0=关闭去重 |
| `unmeasured` | 1=放行未测速的数据面 |
| `nocache` | 1=跳过 60s 缓存 |

### 7. 与两个参考项目的功能对照（如实）

| 功能 | cfnew | edgetunnel | **cfmux** |
| --- | --- | --- | --- |
| 多协议内核（VLESS/Trojan/XHTTP） | ✅ | ✅ | ✅（直接复用完整内核） |
| 面板改**内核参数**（UUID/路径/ProxyIP/ECH） | ✅ | ✅ | ⚠️ 未做（需到数据面控制台改变量） |
| 优选 IP 手动导入 | 面板 | 面板 | ✅ 文本框 + URL 导入 |
| 优选域名 | 部分 | 部分 | ✅ 426 个 + 任意自定义源 |
| 自动测速 | 面板内置 | ❌ | ✅ 本地 launchd / CI |
| 质量分级 + 熔断 | ❌ | ❌ | ✅ S/A/B/C + 连续失败熔断 |
| 真实国家识别 | ❌ | ❌ | ✅ 经隧道实测出口国家 |
| 重复 IP 去重 | ❌ | ❌ | ✅ |
| 多数据面分摊配额 | ❌ | ❌ | ✅ 3 个 |
| 订阅格式 | 多 | 多 | base64 / clash / singbox |
| 二维码 | ✅ | ✅ | ✅ |
| 节点置顶 | ❌ | ❌ | ✅ |
| 流量统计 | 部分 | ❌ | ❌（未做） |

> 诚实结论：**内核参数编辑**和**流量统计**这两块我还没做；
> 其余在「优选管理 / 质量保障 / 多入口 / 可观测」上已经强于两个参考项目。

## 十八、v2.6：用真实浏览器验证后台（并修复了致命 JS 错误）

### 发现的真问题
之前所有后台验证都是 `curl` 看 HTML，**测不出页面 JS 是否报错**。
用 Playwright 打开后发现：

```
PAGEERROR: Invalid or unexpected token
→ STATE 未定义 → 概览 0 卡片、节点表 0 行、所有按钮无反应
```

两个原因，都在 Worker 的模板字符串里：

1. `split('\n')` 在模板字符串中被提前转义成**真实换行** → 浏览器端字符串未闭合
2. `onclick="act(\'ban\',…)"` 反斜杠转义在模板里多了一层 → 语法错误

### 修复
- 换行改用 `\\n`（模板输出 `\n`）
- 按钮事件改为 **`data-act` / `data-key` + 事件委托**，彻底不写转义：

```html
<button class="btn danger" data-act="ban" data-key="host|ip" onclick="doAct(this)">熔断</button>
```

### 浏览器端到端验证（Playwright，12/12 通过，控制台 0 错误）

| 检查项 | 结果 |
| --- | --- |
| 登录进入仪表盘 | ✅ 6 个标签 |
| 概览指标卡 | ✅ 7 张卡（含国家分布） |
| 节点质量表 | ✅ 160 行 |
| 关键字筛选（中文国家） | ✅ 筛「新加坡」→ 53 行 |
| 订阅二维码 | ✅ 生成 |
| 优选列表加载 | ✅ 436 条 |
| 从网址导入 | ✅ 成功 |
| 点击「+ 添加数据面」 | ✅ 4 → 5 行 |
| 数据面保存 | ✅ 已保存 |
| 设置保存 | ✅ 已保存 |
| 设置写入 KV | ✅ minScore=55 生效 |
| 新数据面写入 KV | ✅ entries=4 |

截图：`/tmp/ui_overview.png`、`/tmp/ui_nodes.png`、`/tmp/ui_preferred.png`

### 顺带修复
- 节点筛选现在支持**中文国家名**（筛「新加坡」能命中 `cc=SG` 的节点）
- 趋势图纵轴改为自适应（原来几乎拉平）
- 修复趋势图变量重名（`pad` 重复声明导致再次语法错误）——这次先在本地对
  「模板渲染后的浏览器 JS」跑 `node --check` 再部署，避免同类问题

## 十九、v2.7：交互反馈重做 + 内核配置 + 流量统计 + 自定义域名

### 1. 所有操作都有明确反馈（本次重点）

之前的问题：点了保存看起来没反应（其实 POST 要 2–4 秒，且 toast 太小、1.8 秒就消失）。

现在的反馈链：

| 机制 | 说明 |
| --- | --- |
| 按钮忙碌态 | 点击后立刻变成「保存中…」并禁用，完成恢复 |
| 内联状态 | 按钮右侧常驻 `已保存 12:09:30 AM` / `保存失败：原因` |
| 顶部大提示 | 屏幕上方居中绿色/红色横幅，3 秒，带 ✓/✗ |
| 复制兜底 | `navigator.clipboard` 不可用时自动降级 `execCommand('copy')`，成功/失败都提示 |
| 保存后自动刷新 | 保存成功立即重拉状态，界面数值同步更新 |

实测（Playwright 真浏览器点击）：**12/13 通过**，唯一「失败」是"没有修改任何字段"的正确提示。

### 2. 内核配置管理（新标签「🧠 内核」）
- 选择数据面 → **读取配置** → 表单化编辑 → **保存到数据面**（立即生效）
- 可改：VLESS-WS / Trojan-WS / XHTTP / ECH 开关、自定义路径 `d`、ProxyIP `p`、Trojan 密码 `tp`、
  自定义 DNS、ECH 域名、伪装首页、订阅转换后端
- 底部保留**原始 JSON**（高级用户可改任意字段）
- 实现要点：内核只接受白名单字段，因此保存时**只提交改动项**（diff），整包会被拒（`配置格式无效`）
- 实测：把 `customDNS` 改为 `https://1.1.1.1/dns-query` → 数据面确认生效 → 再改回

### 3. 流量统计（新标签「📈 流量」）
- 今日请求 / 每日配额 / 使用率 / 近 7 天总量
- 7 天柱状图 + 明细表（代理 WS / 订阅 / 其它）
- 计数方式：Worker 内内存累计，**攒够 20 次或满 5 分钟**落一次 KV（避免免费版 KV 每天 1000 次写限制）
- ⚠️ 标注为**近似值**（多 isolate 各自累计会有偏差），真实账单以 Cloudflare 控制台为准

### 4. 自定义域名（已回答）
**可以**。已通过 Pages API 给项目添加 `mux.moneyfly.club`：

```
POST /accounts/{acc}/pages/projects/cfmux/domains  {name:"mux.moneyfly.club"}
→ 201, status: pending
```

但当前 API Token **没有 DNS 权限**（`Authentication error`），需要你补一条记录：

| 类型 | 名称 | 目标 | 代理 |
| --- | --- | --- | --- |
| CNAME | mux | cfmux.pages.dev | ✅ 开启（橙色云） |

配好后访问 `https://mux.moneyfly.club/admin` 即可。
（若愿意给一个带 `Zone:DNS:Edit` 的 Token，我可以直接帮你建记录并验证。）

## 二十、v2.8：节点数修复 · 全格式订阅 · 自定义域名上线

### 1. 「达标很多但订阅很少」的根因（已修）
两个真 bug：

| 问题 | 原因 | 修复 |
| --- | --- | --- |
| 订阅只出 3 个节点 | KV 里 `perEntry=1`（早期测试残留），每个数据面只取 1 个 | 重置为 0（每入口全量） |
| `?limit=50` 反而更少 | `limit` 被误用于**裁剪候选**，候选少了质量过滤后自然更少 | `limit` 只控制输出数量，候选改由 `cand` 控制 |

修复后实测：**108 个节点**（pass 147 → 去重 39 → 输出 108）。

### 2. 订阅格式补全（7 种）

| 格式 | 参数 | 说明 |
| --- | --- | --- |
| Base64 通用 | 默认 | V2Ray / Shadowrocket / v2rayN |
| Clash / Meta / Stash / Mihomo | `target=clash` | 含 url-test + fallback 组 |
| sing-box | `target=singbox` | urltest 自动测速 |
| 原始 URI 列表 | `target=plain` | 纯文本，便于二次处理 |
| JSON | `target=json` | 自建客户端/脚本用 |
| Quantumult X | `target=qx` | `vless=...obfs=wss` 行 |
| Loon | `target=loon` | `name = VLESS,...` 行 |
| Surge | `target=surge` | **不支持 VLESS**，返回说明（需开启 Trojan-WS） |

实测各格式节点数一致：108 个（base64 31KB / clash 35KB / singbox 52KB / qx 24KB / loon 21KB）。

### 3. 自定义域名（已上线）
- 通过 Pages API 添加 `mux.moneyfly.club`
- DNS CNAME：`mux` → `cfmux.pages.dev`（proxied）
- 验证：`https://mux.moneyfly.club/admin` / `/panel` / `/sub` 均 200

> Pages 控制台状态可能短暂显示 `pending`，实际已可访问（证书签发有延迟）。

### 4. 测速原理与 Token（回答）
- **测速在哪里跑**：不在 Worker 里，而是在**你的 Mac**（launchd 每 2 小时）或 GitHub Actions。
  Worker 看不到「你的网络 → Cloudflare 边缘」这一段链路，只有客户端侧才能测准。
- **测什么**：对「数据面 × 候选入口」逐个做 TLS+WS 握手 → VLESS 请求 204 ×3（延迟/抖动）
  → 隧道下载 256KB（吞吐）→ 经隧道查出口国家 → 评分分级 → 写 KV。
- **为什么要 Token**：「手动触发测速」按钮是通过 **GitHub Actions workflow_dispatch** 触发的，
  调 GitHub API 必须带 Token；不配就走本地命令（按钮会自动复制命令给你）。
- **cfnew 怎么测**：它的面板自己跑测速（面板服务器视角），或直接用第三方优选列表；
  两者都不是「你的网络」视角，所以会出现"面板显示快、实际慢"。

### 5. 环境变量一览（触发器相关）
| 变量 | 作用 |
| --- | --- |
| `gh_token` | GitHub PAT（仅需 actions:write） |
| `gh_repo` | `owner/repo`，如 `moneyfly006/cfmux` |
| `gh_workflow` | 默认 `refresh-quality.yml` |
| `gh_ref` | 默认 `main` |
