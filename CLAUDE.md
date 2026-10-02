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
- 配置里的 `docs_port` / `ops_port` / `strategy_port` / `portfolio_port` 仍被读入（monitor 的 `app.state`、docs / ops 的健康输出），但没有进程监听这些端口。
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
| market | `watchlist`；实时报价的按需登记 / 清理写 Redis（`/quotes/refresh-options`、`/quotes/cleanup`）；bars 类 POST 转发给 Market Data Plugin |
| research | Golden Source `ops_feedback.*`（feedback，见 core `docs/DATABASE.md`）；`/research/data/readiness/*` 的 POST 是转发给 Market Data Plugin 的回补 / 确认请求 |

Research **不写** `strategy_opportunity`。表结构与列见 `bifrost-trade-core/docs/DATABASE.md`。

**D10**：monitor 的 `/control/*` 只把命令写进 Redis 控制流，daemon 消费；不得用它武装实盘交易（`../CLAUDE.md` §3）。
`strategy_plan` 是建议性数据，没有执行消费者。

## 路由约定

- 每个进程都有 `GET /health`。
- `GET /status`、`GET /operations`、`POST /control/{action}` **只在 monitor**（daemon 状态与控制），不是各进程的通用模式。
- SSE：`GET /quotes/stream`（market）、`GET /api/messages/stream`（monitor）、ops market-ingest 的流。响应自己带
  `X-Accel-Buffering: no` 和 `Cache-Control: no-cache`；新增 SSE 端点照此设置。

## `bifrost_api.research` 的实际内容

- `routers/` — 五个 router（见上表）
- `sepa/` — `financials_data.py`（经 Plugin 读基本面）、`readiness_snapshot.py`
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
