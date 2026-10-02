# CLAUDE.md — bifrost-trade-api

> Legacy `bifrost-trader-engine` 已按 spine **D8**（2026-06-29）归档移出工作区。工作区事实基线见 `../AGENT_FACTS.md`。

与本项目用户对话一律使用中文回复（无论用户用何种语言提问）；UI 字符串与代码标识符使用 English。

## 工作区定位（2026-10-01）

| 项 | 值 |
|---|---|
| 域 / 载荷 | Trade (OLTP) · Satellite 执行载荷 · 4 个进程（monitor / account / market / research），网关上 8 个 `/api/<domain>` 前缀 |
| 运行位置 | K3s `bifrost-{dev,stg,prod}`；DEV inner loop 的 API 是 `192.168.10.73:30882`（STG `:30880`，PROD `:30881`） |
| 发布链 | GitHub main → `bifrost-deliver-{stg,prod}`；Argo `bifrost-stg/prod` 手动同步；PROD 清单只走 git + Argo |
| 仓库可见性 | GitHub **PUBLIC**（12 个 repo 全部公开）—— `.env`、Secret YAML、dump、kubeconfig、账户内容永不入库 |
| 硬边界 | D10 交易执行冻结（BLOCKED）· D13 三域边界 · 平台/业务解耦（Flywheel A/B） |
| 事实基线 | `../AGENT_FACTS.md`（§8c 运行时与安全事实）· 规则 `../CLAUDE.md`（§8 Claude Code 运行配置） |

会话请在工作区根 `/stocks` 启动（加载治理层 hooks / auto mode / 共享记忆）；运行时与安全事实以 `../AGENT_FACTS.md` §8c 为准。
版本号见 `pyproject.toml`；依赖的 core 版本下限也在那里。

## 部署形态：4 个进程

一个 deployment 一个进程，入口都是 `python scripts/run_server.py <domain>`（domain 由环境变量 `API_DOMAIN` 给出，
见 `bifrost-trade-infra/k8s/base/apis/manifest.yaml`）。端口取自配置 `server.*_port`。Traefik 剥掉 `/api/<prefix>`
后转发，所以浏览器的 `/api/strategy/strategies/plans` 到 account 进程时是 `/strategies/plans`。

| Deployment / 镜像 | 端口 | `run_server.py` domain（别名） | 网关前缀（Service） | 挂载的 router |
|------|------|--------|--------|--------|
| `api-monitor` / `bifrost-api-monitor` | 8765 `monitor_port` | `monitor`（`docs`、`ops` 也起它） | `/api/monitor`；`/api/docs`、`/api/ops`（Service `api-docs` / `api-ops` 选的是 monitor 的 Pod，IngressRoute 只放行 `/api/docs/health`、`/api/docs/research/docs/*`、`/api/ops/health`、`/api/ops/ops/*`） | `monitor/routers`（core · messages · status · daemon · config）+ `docs_api.attach_docs_routes` + `ops.wire_ops_control_plane`（workers · market_ingest） |
| `api-account` / `bifrost-api-account` | 8769 `trading_port` | `account`（`trading`、`strategy`、`portfolio`） | `/api/trading`、`/api/strategy`、`/api/portfolio`（三个 Service 都选 account 的 Pod） | `trading/routers/executions` · `portfolio/routers`（model · config · short_legs） · `strategy/routers`（strategies · plans · saved_searches · reviews） |
| `api-market` / `bifrost-api-market` | 8772 `market_port` | `market` | `/api/market` | `market/routers`（market_data · quotes · watchlist） |
| `api-research` / `bifrost-api-research` | 8773 `research_port` | `research` | `/api/research` | `research/routers`（option_discovery · screener · greeks · data_readiness · feedback） |

- `trading/`、`strategy/`、`portfolio/` 只是 router 包，**没有自己的 app**：各自的 `app.py` 工厂已在 9759867 删除，
  `tests/contract/test_account_serves_every_domain_router.py` 遍历这三个包的 router 模块，要求每条路由都挂在 `create_account_app` 上。新 router 加进
  `account/app.py`，不要再建独立 app。
- `ops/app.py` 只剩 `wire_ops_control_plane`（monitor 调用）；`docs_api/app.py` 的 `create_docs_app` 也由 monitor 挂载。
- 配置里的 `docs_port` / `ops_port` / `strategy_port` / `portfolio_port` 没有进程监听，API 也不再读它们：monitor `/health` 只报 `monitor_port` / `trading_port` / `market_port` / `research_port`，`/ops/health` 与 `/research/docs/health` 不报 `port`（TD-64）。
- 镜像：CI 用 `bifrost-trade-infra/k8s/cicd/docker/Dockerfile.api-stg`（与 core 一起从 Gitea 克隆构建）；本 repo 的
  `Dockerfile` 供本地构建，两者都执行 `scripts/run_server.py "${API_DOMAIN}"`。
- db-init Job 也用 `bifrost-api-monitor` 镜像，跑 `scripts/run_db_refresh_schema.py`（core 的 `_ensure_tables()` +
  Golden Source `raw_broker` DDL + FDW）。

## 谁写什么

所有 API 进程都读 PostgreSQL；写入按进程分：

| 进程 | 写入 |
|------|------|
| monitor | per-env `settings`（`POST /config/ib`、`POST /config/active-strategy`）；daemon 控制命令写 per-env Redis 控制流（`/control/*`）；IB Operator 客户端的连接管理（`/control/monitor_*`、`/control/refresh_accounts`）；ops 控制面：market-ingest 的 K8s 操作，审计发往 platform-api |
| account | `strategy_template` / `_structure` / `_opportunity` / `_allocation` / `_instance`、`gate_safety_strategy`（strategies）；`strategy_plan`（plans）；`trade_review`（reviews）；`preference_saved_search`（saved_searches）；`preference_position_categories` / `_tags` / `preference_market_streams_symbol_order`、`preference_instrument_class`（portfolio config）；Golden Source `raw_broker.executions_raw_*` / `commissions` 的手工成交与策略归属，以及 per-env 桥表 `account_execution_instance_allocation` / `account_execution_option_stock_link`（executions、`PATCH /executions/strategy-attribution`） |
| market | `watchlist`；实时报价的按需登记 / 清理写 Redis（`/quotes/refresh-options`、`/quotes/cleanup`）。bars / holidays / indices 只读：回补、删除、EOD 刷新、指数刷新与假日写入路由已删（TD-40，0 调用方 0 流量），采集归 Market Data Plugin |
| research | Golden Source `ops_feedback.*`（feedback，见 core `docs/DATABASE.md`）。`/research/data/readiness/*` 只剩读：回补、gap-ack、snapshot 与各 gaps 路由已删（TD-40） |

Research **不写** `strategy_opportunity`。表结构与列见 `bifrost-trade-core/docs/DATABASE.md`。

**D10**：monitor 的 `/control/*` 只把命令写进 Redis 控制流，daemon 消费；不得用它武装实盘交易（`../CLAUDE.md` §3）。
`strategy_plan` 是建议性数据，没有执行消费者。

## 路由约定

- 每个进程都有 `GET /health`。没有 `*/shutdown`：进程生命周期归 K8s（TD-64 删除）。
- 各 app 的 `GET <prefix>/auth/capabilities`（monitor `/api/server`、`/ops`、`/research/docs`；account `/account`、`/trading`、`/portfolio`、`/strategy`；market `/market`；research 无前缀）都由
  `bifrost_api/common/service_endpoints.mount_auth_capabilities` 挂载，读的配置与 write guard 相同。新 app 用它，不要再抄一份。
- Research router 取 DB 配置用 `research/deps.db_config`；strategy router 的 503（无 Postgres 写配置）用 `strategy/deps.write_config` / `db_not_configured`。
- `GET /status`、`GET /operations`、`POST /control/{action}` **只在 monitor**（daemon 状态与控制），不是各进程的通用模式。
- SSE：`GET /quotes/stream`（market）、`GET /api/messages/stream`（monitor）、ops market-ingest 的流。响应自己带
  `X-Accel-Buffering: no` 和 `Cache-Control: no-cache`；新增 SSE 端点照此设置。

### 响应信封（TD-16 / TD-17，Owner 决策 B：先加后删）

`bifrost_api/common/envelopes.py`，新路由和改动到的路由都用它：

- **失败**：`error_response(status, message, legacy=None)` → 真实状态码 + `{"detail": message, "ok": false, "error": message, ...legacy}`。
  客户端只读 `detail`。状态码：400 输入不对 · 404 不存在 / 没匹配到 · 409 冲突 / 被占用 · 503 依赖没配或连不上
  （PostgreSQL、IB Gateway）· 500 意外失败（helper 会记日志）。不再用 200 `{"ok": false}` 报失败。
- **列表**：`list_body(items, legacy_keys=None, total=None, **extra)` → `{"items": [...], "count": len(items), "total"?, ...}`。
  成功的单个对象保持原形状，不包一层。
- **过渡一个版本**：`ok` / `error` 和路由原来的列表键（`executions`、`attributions`、`transactions` …，与 `items` 同一个列表）
  这一版照发，让发布前打开的标签页还能用；**下一个版本删掉**。已转换：portfolio config、trading executions、
  market watchlist、monitor config、`/strategies` 列表（0.2.2，batch 3b-1）。
- core 的旧写函数常把「没有这行」和「数据库报错」合成一个 False / 0：调用方能走到的是前者时答 404（消息照旧写两种可能），
  只可能是写失败时答 500。PATCH 与 DELETE 已改用 core 0.33.0 的 TD-15 写函数（见下节），不再猜。

### 写语义（TD-15，0.3.0，决策 B 两步走）

- **PATCH = merge**：请求模型继承 `common/write_errors.PatchBody`（`extra="forbid"`、字段全可选、strict 类型），
  `body.patch_fields()`（= `model_dump(exclude_unset=True)`）原样交给 core 的 `patch_*`：只改发来的字段，显式 `null`
  清空可空列；空 body / 未知字段 → 422；成功答 200 + 该资源 GET-by-id 形状的行（老调用方读 `ok` 的路由再带一版 `ok: true`）。
  执行归属走 `PATCH /executions/{id}/attribution`（只改归属，成交列仍归 PUT）；watchlist 单项是 `PATCH /watchlist/{contract_key}`。
- **DELETE = strict**：调 core 的 `*_strict`，答 `{"deleted": "hard"|"soft", <id>, …, "ok": true}`（`ok` 下一版删）；
  不存在 → 404（不再 200），被占用 → 409 带原因。
- **错误映射只有一处**：`common/write_errors.install_write_errors`（account、market 两个 app）把 core 的 `Write*` 经
  `error_response` 答出：`WriteNotFound` 404 · `WriteConflict` 409 · `WriteInvalid` 400 · `WriteFailed` 503（`unavailable`：
  没配 / 连不上）否则 500。路由里不要再 `try/except` 这些；没配 Postgres 用 `write_target(request, what)`（同一个 503）。
- **老 PUT**：这一版行为不变，在 `deprecations.REPLACED_ROUTES` 里登记「被谁取代」——响应带 `Deprecation: true` 与
  `Link: <successor>; rel="successor-version"`，日志 `replaced route hit … use <successor>`。与 `DEPRECATED_ROUTES`（没人调用、
  准备删的路由）互斥。下一版 PUT 变真正的整体替换。新增 merge 式写入一律做 PATCH，不要再加 merge 式 PUT。

### 请求与响应模型（TD-24，0.3.1，决策 B 先加后收）

- **POST / PUT body 一律是模型**，不再收 `Dict[str, Any]`：继承 `common/request_bodies.LenientBody`（嵌套对象用
  `LenientItem`），放在各域 `*/schemas/requests.py`。类型 strict（`"5"` 不是数、`1` 不是 `true`）→ 类型错是 422、什么都不写；
  字段在模型里都可选，「必填」仍由路由 / core 答 400（消息不变）。路由只读声明过的字段：交给 core 用
  `body.declared(exclude_unset=True)`。**未知字段这一版接受并忽略**，每个请求记一行
  `unknown request fields: <METHOD> <route template> ignored [<names>]`（只有字段名，不记值；嵌套写 `legs[].x`），
  靠 `install_request_field_log`（account、market 两个 app）拿到路由模板。**下一版改 `extra="forbid"`**，与 PATCH 一致。
  core 自带的 body（opportunity / allocation / instance / plan / review）与 PATCH 的 `PatchBody` 不在此列。
- **响应模型**：allocations、opportunities、gate-safety、instances、plans 的列表 / 单个 GET / PATCH 用
  `strategy/schemas/responses.py`（`response_model=…`，`response_model_exclude_unset=True`）。`ResponseRow` 先跑
  `jsonable_encoder`，线上形状与没模型时一字不差（时间戳是 `isoformat()` 的 `+00:00` 字符串）；`extra="allow"` 这一版保留
  reader 新加的字段。没有默认值的字段 = reader 一定会发；reader 少发一个就是 500——改 core reader 的 SELECT 时同步改模型和
  `tests/strategy_rows.py`（它用真 reader 跑 SQL 形状的行）。字段清单给前端 zod 用。

## `bifrost_api.research` 的实际内容

- `routers/` — 五个 router（见上表）
- `sepa/` — `financials_data.py`（经 Plugin 读基本面）；`readiness_snapshot.py` 随 TD-40 删除
- `sepa_engine/stock_option_pcr.py` — put/call 比与按到期日的链汇总
- 顶层模块：`market_data_client.py`（Plugin HTTP）、`analytics_reader.py`（Research API 代理）、`feedback_store.py`、
  `contract_key_bridge.py`、`iv_atm.py`、`market_pg.py` 等
- `indicators/`、`screener/`、`schemas/`、`sepa_engine/sepa/` 只有空的 `__init__.py`

SEPA 筛选与回测在 `bifrost-research`（Research 域），不在本 repo。

## 依赖

```
bifrost-core  ← 数据模型、DB 读取层、配置（下限见 pyproject.toml）
fastapi / uvicorn
```

本 repo 不依赖 `bifrost-trade-worker`，也不依赖已归档的 `bifrost-trade-socket`（`make install-dev` 目前仍引用这两个 repo，待修）。

## 命令

```bash
pip install -e ../bifrost-trade-core -e ".[dev]"

python scripts/run_server.py monitor     # 起单个进程：monitor | account | market | research
python scripts/run_server.py account     # trading / strategy / portfolio 是 account 的别名

make test                                # pytest -m 'not ib and not db'
make lint                                # ruff check .
```
