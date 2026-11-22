# 链路追踪尾部采样演练台（Tail-Based Sampling Workbench）

一个**零第三方依赖**（仅用 Python 3.11 标准库：`http.server` + `sqlite3` + 原生 JS）的本地 Web
演练台，用来演示分布式追踪中「尾部采样（tail-based sampling）」的完整决策过程。

## 它解决什么问题

值班员可以：

1. 手工（或通过演示按钮）提交带时间戳的 trace / span；
2. 配置**收集窗口**、**慢请求阈值**和**每个服务的保留配额**；
3. 观察系统在窗口结束后按**完整链路**（而不是单个 span）决定保留还是丢弃；
4. 观察**错误 / 超时链路优先**、慢请求其次、普通链路最后的排序；
5. 验证跨多个服务的链路必须**同时满足所有相关服务配额**——任一服务配额耗尽，
   整条链路（全部 span）一起丢弃，**绝不部分导出**；
6. 在**宽限期**内投递迟到 span，观察尚未导出的决策如何被翻转；已导出的链路
   **不会重复导出**，最终保留结果也不会突破任何配额；
7. 页面逐条展示每条链路的**保留 / 丢弃 / 等待原因**以及各服务**配额占用**；
8. 所有状态持久化在 SQLite，**重启后可继续未完成的窗口**；
9. 从历史页**清理已关闭窗口的 span 明细**以释放 SQLite 存储，清理前或清理进行中可
   **保全**排障要用的链路，清理**按整条链路分批**进行，批次之间当前窗口仍能接收 span。

## 目录结构

```
.
├── app/
│   ├── __init__.py
│   ├── server.py            # HTTP 路由、窗口生命周期、API、静态文件服务
│   ├── sampler.py           # 纯函数：链路聚合 + 优先级 + 配额分配决策
│   ├── storage.py           # SQLite 持久层（窗口/配额/span/导出记录）
│   ├── demo.py              # 四个一键演示场景
│   └── static/              # 单页前端（index.html / app.js / styles.css）
├── tests/
│   ├── test_sampler.py      # 24 项端到端单测（假时钟，确定性）
│   └── smoke_http.py        # 对真实运行服务的 49 项 HTTP 冒烟检查
├── run.py                   # 入口（自带 sys.path 处理，支持 python -I）
├── Dockerfile               # python:3.11-slim，无需 pip install
├── docker-compose.yml       # 一条命令起服务，SQLite 挂在命名卷
└── requirements.txt         # 故意为空：运行期零第三方依赖
```

## 用 Docker Compose 启动

```bash
docker compose up --build
# 打开浏览器
#   http://localhost:8080
```

数据写入命名卷 `tracing-data`（容器内 `/data/tracing.db`）。停止再启动
（`docker compose down` 后重新 `up`，只要不删卷）后，未关闭的窗口、已收
span、配额和导出记录都会恢复。

不使用 Docker 直接本地运行：

```bash
DB_PATH=./data/tracing.db PORT=8080 python3 -I run.py
```

## 采样规则（与代码一一对应）

| 规则 | 实现位置 |
| --- | --- |
| 按 `trace_id` 把乱序 span 聚合成完整链路，延迟按时间戳重算 | `sampler.build_traces` / `Trace.latency_ms` |
| 优先级：错误/超时 tier0 → 慢请求 tier1 → 普通 tier2，同 tier 按开始时间 | `sampler.evaluate_window` 排序键 |
| 每条保留链路占用其**每个**经过服务的 1 个配额槽 | 同上，接受时原子扣减全部相关服务 |
| 任一服务槽位不足 → 整条链路 `drop_quota`，不导出任何 span | `blocking` 分支 |
| 配额是硬上限：错误链路在配额满时一样丢弃，原因里明确说明 | 同上（高优先丢弃时附加说明文案） |
| 宽限期内迟到 span 可改变**暂定**决策；关闭后才最终落库 | `tentative_evaluation` + `_finalize_locked` |
| 已导出链路幂等：重新结束窗口不产生重复导出，且仍占用配额 | `exports` 表主键 `(window_id, trace_id)` + 已导出槽位恢复 |
| 重启恢复未完成窗口 | 全部状态在 SQLite；`AppState` 重建后 `advance()` 自动补关闭 |
| 关闭时快照决策，清理后历史仍完整 | `_finalize_locked` 写 `trace_records` + `quota_usage` |
| 清理按整条链路分批、每批单独提交 | `_run_purge_job` → `Storage.purge_trace_batch` |
| 保全优先，已清理不可再保全 | `preserve` 校验 `purged_at_ms`；删除 SQL 排除 preserved |
| 中断可重试、已提交批次继续生效 | `purge_jobs` 持久化；重启把 `running` 标为 `interrupted` |
| 明细删除后重复 span_id 仍判重 | 去重查 `span_registry`（不随明细清理） |

窗口三个阶段（页面顶部有进度条）：

- `collecting` 收集中：正常接收 span，展示「暂定」保留/丢弃；
- `grace` 宽限期：仍可接收时间戳合法的迟到 span，决策可能被翻转；
- `closed` 已关闭并导出：拒绝新 span，导出记录固定，不可重复导出。

## 四个一键演示场景

| 按钮 | 演练点 |
| --- | --- |
| 乱序 span | s3→s1→s2 乱序投递，窗口结束后按时间戳重建完整链路并重算延迟 |
| 宽限期迟到错误 | 普通链路在宽限期内收到一个迟到的错误 span，优先级翻转并抢到配额 |
| 配额耗尽 | payment/orders 配额仅 1，错误链路抢占唯一保留位，其余整条丢弃 |
| 跨服务配额联动 | 链路经过配额为 0 的 inventory，整条丢弃；不经过它的链路正常保留 |

页面上还有「立即结束并导出」「模拟进程重启（从 SQLite 恢复）」「清空全部数据」。

## 明细保全与清理

历史页/当前页点击任一链路行的「保全」，即可把该链路标记为排障保留；清理任务
会跳过它并保留全部 span 明细。对已关闭窗口点击「清理 span 明细（按链路分批）」：

1. 清理**按整条链路**分批删除，每批在一个事务里提交（span 明细 + `purged_at_ms`
   标记同时生效），任务进度持久化在 `purge_jobs`；
2. 批次之间不持有数据库锁、并按 `pause_ms` 间隔，因此**当前窗口仍可接收新 span**；
3. 「模拟清理中断」或进程重启会把运行中的任务标为 `interrupted`，**已提交的批次
   继续生效**，再次点击清理会从未清理的链路继续；
4. 明细已清理的链路仍显示清理前的**采样决定、原因、原始 span 数、服务配额用量和
   导出记录**（来自 `trace_records` / `quota_usage`），但**无法再保全**（提示原因）；
5. 历史页用「明细已清理」标出这些链路；
6. 去重改为查独立的 `span_registry`，所以**即使原明细已删除，相同 span_id 仍算重复上报**。
   只有「清空全部数据」会连注册表一起清掉。

## 自测

```bash
# 24 项确定性单测（假时钟驱动 collecting→grace→closed 全流程，含保全/清理）
python3 -I tests/test_sampler.py

# HTTP 冒烟（先起服务）
DB_PATH=/tmp/smoke.db PORT=8091 python3 -I run.py &
python3 -I tests/smoke_http.py http://127.0.0.1:8091
```

覆盖：乱序 span、迟到错误翻转、配额硬上限、跨服务原子性、重启恢复、
幂等导出、关闭后拒 span、重复 span 幂等、默认配额、慢请求优先、运行中
调配额、输入校验、演示场景装载、**明细分批清理、保全跳过清理、清理后历史与
配额可见、已清理链路的保全被拒、中断后重试、清理期间接收新 span、清理后重复上报判重**。

## 主要 API

```
POST /api/windows                     创建窗口（collect_ms/grace_ms/slow_ms/default_quota/quotas）
POST /api/windows/<id>/quotas         运行中调整某服务配额
POST /api/windows/<id>/finalize       立即结束窗口并导出（幂等）
POST /api/spans                       提交 span（时间戳由调用方给）
GET  /api/window/current              当前/最近窗口的完整快照（含每条链路原因与配额占用）
GET  /api/windows/<id>                任意窗口详情（含已关闭、已清理窗口）
GET  /api/windows                     历史窗口（含明细是否已清理）
POST /api/windows/<id>/preserve       保全一条链路 {"trace_id":"..."}
POST /api/windows/<id>/purge          启动/重试分批清理 {"batch_size":5,"pause_ms":0}
GET  /api/windows/<id>/purge/status   清理任务进度与剩余链路/span
POST /api/windows/<id>/purge/interrupt 模拟清理中断（已提交批次保留）
POST /api/demo/<name>                 演示场景：out-of-order | late-error | quota-exhausted | multi-service
POST /api/simulate-restart            关闭并重开 Storage，模拟进程重启
POST /api/reset                       清空全部数据
```
