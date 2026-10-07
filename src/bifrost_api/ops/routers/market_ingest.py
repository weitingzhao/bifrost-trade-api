"""Market data ingest: the services table (workload state, control lease, display state).

Read only. ``POST /ops/market-ingest/control`` (start / stop / restart / reset, the only
path that scaled a workload, the daemon included) and ``POST
/ops/market-ingest/clear-conflict-leases`` had no caller and no traffic in the release
they were marked deprecated, and are gone (TD-40, api 0.8.0)."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Request

from bifrost_api.ops.market_ingest_config import (
    IB_GATEWAY_DEPLOYMENT,
    market_ingest_services_from_config,
)
from bifrost_api.ops.market_ingest_control_env import (
    clear_control_env,
    meta_redis_url_from_ops_config,
    normalize_control_profile,
    read_control_env,
    read_control_host,
    read_control_updated_at,
)
from bifrost_api.ops.market_ingest_display import (
    derive_ingest_display_state,
    platform_gateway_managed_for_service,
    process_active_from_replica_counts,
)
from bifrost_api.ops.market_ingest_health_clear import (
    ingest_redis_health_looks_live,
    ingest_redis_health_writer_recent,
    read_health_stack_profile,
    read_health_updated_at,
)
from bifrost_api.ops.services.executor_kubernetes import KubernetesExecutor
from bifrost_core.core.redis_url import ib_redis_url_from_config

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ops-market-ingest"])

_RECENT_CONTROL_WRITE_GRACE_SEC = 120.0


def _process_counts_as_running(active: str) -> bool:
    """True when systemd / docker compose still reports the unit up (incl. restart window)."""
    a = (active or "").lower().strip()
    return a in ("active", "activating")


def _executor(request: Request):
    return request.app.state.executor


def _config(request: Request) -> dict:
    return getattr(request.app.state, "bifrost_config", {}) or {}


def _ops_control_profile(request: Request) -> Optional[str]:
    raw = getattr(request.app.state, "bifrost_config_profile", None)
    return normalize_control_profile(raw if isinstance(raw, str) else None)


def _effective_ops_control_profile(request: Request) -> Optional[str]:
    """dev|prod for Redis lease + 409 guard: filename profile, then ``ops.control_profile`` YAML, then env."""
    p = _ops_control_profile(request)
    if p:
        return p
    cfg = _config(request)
    ops_cfg = cfg.get("ops") if isinstance(cfg.get("ops"), dict) else {}
    raw = ops_cfg.get("control_profile")
    if isinstance(raw, str):
        n = normalize_control_profile(raw)
        if n:
            return n
    return normalize_control_profile(os.environ.get("BIFROST_OPS_CONTROL_PROFILE"))


@router.get("/ops/market-ingest/services")
async def market_ingest_services(request: Request) -> Dict[str, Any]:
    """List configured ingest services with current systemd ``is-active`` state."""
    cfg = _config(request)
    rows = market_ingest_services_from_config(cfg)
    exc = _executor(request)
    rurl = meta_redis_url_from_ops_config(cfg)
    ib_rurl = ib_redis_url_from_config(cfg)
    ops_profile = _effective_ops_control_profile(request)
    out: List[Dict[str, Any]] = []
    for row in rows:
        unit = str(row.get("systemd_unit") or "")
        k8s_dep = str(row.get("k8s_deployment") or "")
        k8s_ns = str(row.get("k8s_namespace") or "")
        k8s_replicas: Optional[int] = None
        k8s_ready: Optional[int] = None
        if isinstance(exc, KubernetesExecutor) and k8s_dep:
            k8s_replicas, k8s_ready = await exc.deployment_replica_counts(
                k8s_dep, namespace=k8s_ns or None
            )
            active = process_active_from_replica_counts(k8s_replicas, k8s_ready)
        else:
            try:
                active = await exc.systemctl_is_active(unit) if unit else "inactive"
            except Exception as e:
                active = "unknown"
                logger.debug("systemctl_is_active %s: %s", unit, e)
        meta_key = (row.get("redis_meta_key") or "").strip()
        redis_control_env: Optional[str] = None
        redis_control_host: Optional[str] = None
        redis_control_updated_at: Optional[float] = None
        row_sid = (row.get("id") or "").strip()
        if rurl:
            # Dev/Prod HOST is stored on the service health hash so Prod can use the same
            # Redis node it already updates (bifrost:health:*), avoiding bifrost:ops:lease:*.
            lk = meta_key
            if lk:
                redis_control_env = await asyncio.to_thread(read_control_env, rurl, lk)
                redis_control_host = await asyncio.to_thread(read_control_host, rurl, lk)
                redis_control_updated_at = await asyncio.to_thread(read_control_updated_at, rurl, lk)
            # Orphan detection: lease present but health gone → service died, clear stale lease.
            # Requires meta_key to check health hash; skip if meta_key empty.
            if redis_control_env is not None and row_sid != "trading_engine" and meta_key:
                is_live = await asyncio.to_thread(
                    ingest_redis_health_looks_live, rurl, meta_key, row_sid
                )
                control_age = (
                    time.time() - redis_control_updated_at
                    if redis_control_updated_at is not None
                    else None
                )
                recent_control_write = (
                    control_age is not None and control_age <= _RECENT_CONTROL_WRITE_GRACE_SEC
                )
                if (
                    not is_live
                    and not recent_control_write
                    and not _process_counts_as_running(active)
                ):
                    try:
                        await asyncio.to_thread(clear_control_env, rurl, lk)
                    except Exception as _ce:
                        logger.debug("GET /services: clear orphaned lease %s: %s", row_sid, _ce)
                    redis_control_env = None
                    redis_control_host = None
                    redis_control_updated_at = None
        runtime_externally_managed = False
        if rurl and meta_key and not _process_counts_as_running(active):
            looks_live = await asyncio.to_thread(
                ingest_redis_health_looks_live, rurl, meta_key, row_sid
            )
            writer_recent = await asyncio.to_thread(
                ingest_redis_health_writer_recent, rurl, meta_key
            )
            stack = await asyncio.to_thread(read_health_stack_profile, rurl, meta_key)
            if not stack:
                ops_cfg = cfg.get("ops") if isinstance(cfg.get("ops"), dict) else {}
                stack = normalize_control_profile(ops_cfg.get("control_profile"))
            externally_ok = looks_live or (stack == "stg" and writer_recent)
            if externally_ok and stack:
                runtime_externally_managed = True
                redis_control_env = stack
                if not redis_control_host:
                    redis_control_host = "k8s"
                if redis_control_updated_at is None:
                    redis_control_updated_at = time.time()
        platform_gateway_managed = await asyncio.to_thread(
            platform_gateway_managed_for_service,
            ib_rurl,
            rurl,
            meta_key,
            row_sid,
        )
        if platform_gateway_managed:
            runtime_externally_managed = True
            if not redis_control_host:
                redis_control_host = "platform-ib-gateway"
        # The gateway heartbeat is ``updated_at`` on redis-ib. The ops-control
        # field is not written there, and redis-live does not hold these hashes.
        if k8s_dep == IB_GATEWAY_DEPLOYMENT and redis_control_updated_at is None:
            health_url = ib_rurl or rurl
            if health_url and meta_key:
                redis_control_updated_at = await asyncio.to_thread(
                    read_health_updated_at, health_url, meta_key
                )

        runtime_kind = "kubernetes"
        display = derive_ingest_display_state(
            service_id=row_sid,
            process_active=active,
            redis_url=rurl,
            ib_redis_url=ib_rurl,
            meta_key=meta_key,
            runtime_externally_managed=runtime_externally_managed,
            platform_gateway_managed=platform_gateway_managed,
            ops_control_profile=ops_profile,
            runtime_kind=runtime_kind,
        )
        item: Dict[str, Any] = {
            **row,
            "process_active": active,
            "redis_control_env": redis_control_env,
            "redis_control_host": redis_control_host,
            "redis_control_updated_at": redis_control_updated_at,
            "runtime_externally_managed": runtime_externally_managed,
            "platform_gateway_managed": platform_gateway_managed,
            "runtime_kind": runtime_kind,
            **display,
        }
        if platform_gateway_managed:
            item["transport"] = "platform_gateway"
        if isinstance(exc, KubernetesExecutor):
            dep = k8s_dep or (exc.deployment_for_unit(unit) if unit else "")
            ns = k8s_ns
            if dep:
                item["k8s_deployment"] = dep
                if ns:
                    item["k8s_namespace"] = ns
                if k8s_replicas is None and k8s_ready is None:
                    k8s_replicas, k8s_ready = await exc.deployment_replica_counts(
                        dep, namespace=ns or None
                    )
                if k8s_replicas is not None:
                    item["k8s_replicas"] = k8s_replicas
                if k8s_ready is not None:
                    item["k8s_ready"] = k8s_ready
                guard = exc.scale_guard_for_deployment(dep)
                if guard is not None:
                    item["k8s_scale_guard"] = guard
        out.append(item)
    return {"ok": True, "services": out}
