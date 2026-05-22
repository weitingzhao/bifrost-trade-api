# CLAUDE.md — bifrost-trade-api

与本项目用户的所有对话一律使用中文。

## 职责范围

本 repo 包含 9 个独立的 FastAPI 服务，每个服务对应一个业务域：

| 域 | 模块 | 端口 | 主要职责 |
|----|------|------|----------|
| monitor | `bifrost_api.monitor` | 8765 | Daemon 状态、控制命令、心跳 |
| massive | `bifrost_api.massive` | 8766 | Polygon 数据查询、期权链 |
| docs | `bifrost_api.docs_api` | 8767 | OpenAPI schema、API 覆盖率 |
| ops | `bifrost_api.ops` | 8768 | Celery 队列管理、Worker 健康 |
| trading | `bifrost_api.trading` | 8769 | 订单、持仓、交易历史 |
| strategy | `bifrost_api.strategy` | 8770 | 结构模板、Gate 配置、机会发现 |
| portfolio | `bifrost_api.portfolio` | 8771 | 多账户、Greeks 聚合 |
| market | `bifrost_api.market` | 8772 | 实时行情 SSE、采集状态 |
| research | `bifrost_api.research` | 8773 | 回测、历史 Greeks 分析 |

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

所有服务**只读** PostgreSQL，不写。写操作由 daemon 和 workers 负责。

## SSE 注意事项

SSE 端点（`/quotes/stream` 等）需要 Nginx 关闭缓冲：
```nginx
proxy_buffering off;
proxy_cache off;
proxy_http_version 1.1;
```
已在 `bifrost-trade-infra/nginx/nginx.conf` 中配置。
