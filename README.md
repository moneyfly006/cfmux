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
