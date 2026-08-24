#!/usr/bin/env python3
"""Run bifrost_core DDL from api-monitor image (core is pip-installed, not copied at /build/)."""

from __future__ import annotations

import os
import sys


def main() -> int:
    config_path = os.environ.get("BIFROST_CONFIG", "/app/config/config.stg.yaml")
    if not os.path.isfile(config_path):
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 1

    try:
        import yaml
        import psycopg2
        from bifrost_core.persistence.postgres.brokerage_ddl import (
            ensure_brokerage_schema,
            setup_fdw_foreign_tables,
            setup_fdw_market_tables,
        )
        from bifrost_core.persistence.postgres.connection import (
            _get_conn_params,
            _get_golden_source_conn_params,
        )
        from bifrost_core.persistence.postgres.ddl import _ensure_tables
    except ImportError as exc:
        print(f"Missing dependency: {exc}", file=sys.stderr)
        return 1

    with open(config_path, encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}

    params = _get_conn_params(config)
    params["connect_timeout"] = 10
    print(f"Trade DB: {params['user']}@{params['host']}:{params['port']}/{params['dbname']}")

    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '20s'")
            cur.execute("SET statement_timeout = '120s'")
        conn.commit()
        _ensure_tables(conn)
        conn.commit()
        print("public schema refresh complete.")
    except Exception as exc:
        print(f"Schema refresh failed: {exc}", file=sys.stderr)
        return 1

    gs_cfg = config.get("golden_source") or {}
    if not gs_cfg and not os.environ.get("GOLDEN_SOURCE_HOST"):
        conn.close()
        return 0

    gs_params = _get_golden_source_conn_params(config)
    gs_params["connect_timeout"] = 15
    print(
        f"Golden Source: {gs_params['user']}@{gs_params['host']}:"
        f"{gs_params['port']}/{gs_params['dbname']}"
    )
    gs_conn = psycopg2.connect(**gs_params)
    try:
        ensure_brokerage_schema(gs_conn, log=lambda m: print(f"brokerage {m}"))
    finally:
        gs_conn.close()

    fdw_params = dict(gs_params)
    fdw_params["user"] = gs_cfg.get("fdw_user") or "brokerage_reader"
    fdw_params["password"] = (
        gs_cfg.get("fdw_password") or gs_cfg.get("password") or fdw_params.get("password") or ""
    )
    try:
        setup_fdw_foreign_tables(
            conn,
            fdw_params,
            local_user=str(params["user"]),
            log=lambda m: print(f"fdw {m}"),
        )
        setup_fdw_market_tables(
            conn,
            local_user=str(params["user"]),
            log=lambda m: print(f"fdw market {m}"),
        )
        print("FDW foreign tables ready.")
    except Exception as exc:
        print(f"FDW setup skipped: {exc}", file=sys.stderr)
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
