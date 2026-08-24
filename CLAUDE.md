# CLAUDE.md — bifrost-trade-api

> 本项目是 bifrost-trader-engine 重构的一部分。迁移进度见 `bifrost-trade-infra/docs/MIGRATION_TRACKING.md`。

与本项目用户对话一律使用中文回复（无论用户用何种语言提问）；UI 字符串与代码标识符使用 English。

## 职责范围

本 repo 包含 Phase B 后为 **4 个进程 Pod**（HTTP 路径前缀仍保留 `/api/{domain}/`）：

| 进程 | 端口 | 吸收域 |
|------|------|--------|
| monitor (operations) | 8765 | monitor + ops + docs |
| account | 8769 | trading + portfolio + strategy |
| market | 8772 | market |
| research | 8773 | research — Stock Data Readiness routes are thin passthrough to Market Data Plugin (`/market/readiness/*`) |

Trade Celery 已移除；`ops/` 仅保留认证、审计、market-ingest Kubernetes 控制。

历史 9 域表（路径别名仍可用）：

| 域 | 模块 | 端口 | 主要职责 |
|----|------|------|----------|
| monitor | `bifrost_api.monitor` | 8765 | Daemon 状态、控制命令、心跳 |
| ~~massive~~ | ~~`bifrost_api.massive`~~ | ~~8766~~ | ~~Polygon 数据查询、期权链~~ — **retired (P7)**: 由 Market Data Plugin `:8790` 替代 |
| docs | `bifrost_api.docs_api` | 8767 | OpenAPI schema、API 覆盖率 |
| ops | `bifrost_api.ops` | 8768 | **merged into monitor** — 认证、审计、market-ingest K8s 控制（Celery 队列已退役 Wave 5） |
| trading | `bifrost_api.trading` | 8769 | 订单、持仓、交易历史 |
| strategy | `bifrost_api.strategy` | 8770 | 结构模板、Gate 配置、机会发现 |
| portfolio | `bifrost_api.portfolio` | 8771 | 多账户、Greeks 聚合 |
| market | `bifrost_api.market` | 8772 | 实时行情 SSE、采集状态 |
| research | `bifrost_api.research` | 8773 | SEPA 四阶段筛选引擎 + 回测 + 历史 Greeks（完整业务逻辑） |

`bifrost_api.research` 包含以下子模块：
- `research/sepa/` — Phase 1–4 筛选流水线（基本面 → 技术面 → 期权结构 → 综合评分）
- `research/screener/` — 股票筛选器、SEPA 评分
- `research/indicators/` — 技术指标（均线、IV Cone、波动率）
- `research/routers/` — HTTP 路由（触发流水线、查询结果）
- `research/schemas/` — Pydantic 模型

## 依赖

```
bifrost-core  ← 数据模型、DB 读取层、配置
fastapi
uvicorn
```

## 命令

```bash
pip install -e ".[dev]"

python scripts/run_server.py monitor     # 启动单个服务
python scripts/run_server.py trading

pytest                                   # 所有 API 测试
```

## 路由规范

每个 FastAPI app 遵循统一模式：
- `GET /status` — 服务健康
- `GET /operations` — 操作记录
- `POST /control/{action}` — 控制指令（daemon）
- `GET /*/stream` — SSE 推送（market 域为主）

除 `research` 域（SEPA 流水线写入 `strategy_opportunity` 表）外，其余服务**只读** PostgreSQL。写操作由 daemon 和 workers 负责。

## SSE 注意事项

SSE 端点（`/quotes/stream` 等）需要 Nginx 关闭缓冲：
```nginx
proxy_buffering off;
proxy_cache off;
proxy_http_version 1.1;
```
已在 `bifrost-trade-infra/nginx/nginx.conf` 中配置。
