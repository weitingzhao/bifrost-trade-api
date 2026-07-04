"""Status endpoints: run status, operations, risk summary."""

import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, Request

from bifrost_core.monitor.reader import get_job_bars_backfill_last_updated
from bifrost_core.monitor.reader.reference_indices_merge import (
    augment_reference_indices_with_caret_symbols,
    merge_reference_indices,
)
from bifrost_core.monitor.reader.ib_config_public import (
    ib_client_for_api,
    ib_client_public_defaults,
    ib_flex_for_status_api,
    ib_flex_public_defaults,
)
from bifrost_core.monitor.self_check import derive_daemon_self_check, derive_health_roll_up
from bifrost_core.core.realtime.redis_keys import SUBSCRIBE_CHANNEL_DEFAULT
from bifrost_core.core.redis_health_keys import (
    hgetall_account_sync_daemon_health,
    hgetall_ib_account_agent_health,
    redis_hash_field_truthy,
    hgetall_ib_ingestor_health,
    hgetall_massive_ws_status,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["status"])

_status_cache_lock = threading.Lock()
_status_cache: Dict[str, Any] = {}
_status_cache_ts: float = 0.0
_STATUS_CACHE_TTL = 2.0

# GET /status polls often; Celery control.inspect waits the full timeout when no worker replies.
# Ops uses longer CELERY_INSPECT_TIMEOUT_SEC in celery_app (e.g. 15s) for worker snapshots.
_STATUS_CELERY_INSPECT_TIMEOUT_SEC = float(
    os.environ.get("BIFROST_STATUS_CELERY_INSPECT_TIMEOUT_SEC", "2.0")
)

STATUS_SCHEMA_VERSION = 9

# PG heartbeat can refresh when Redis writes fail (e.g. suspended loop); require Redis health when configured.
_ACCOUNT_SYNC_REDIS_HEALTH_MAX_AGE_SEC = 45.0


def _account_sync_redis_reports_alive(r: Any, *, now_ts: float) -> bool:
    """True when ``bifrost:health:daemon_account_sync`` exists, alive is truthy, and updated_at is recent."""
    try:
        _h = hgetall_account_sync_daemon_health(r)
    except Exception:
        return False
    if not _h:
        return False
    if not redis_hash_field_truthy(_h, "alive"):
        return False
    _ua = _h.get("updated_at")
    if _ua is None or str(_ua).strip() == "":
        return True
    try:
        return (now_ts - float(_ua)) < _ACCOUNT_SYNC_REDIS_HEALTH_MAX_AGE_SEC
    except (TypeError, ValueError):
        return True


def _strategy_status_block(
    *,
    active_structure_id: Any,
    active_structure_name: Any,
    active_gate_id: Any,
    active_gate_name: Any,
    active_alloc_id: Any,
    active_alloc_name: Any,
) -> Dict[str, Any]:
    return {
        "active": {
            "structure": {"id": active_structure_id, "name": active_structure_name},
            "gate_safety": {"id": active_gate_id, "name": active_gate_name},
            "allocation": {"id": active_alloc_id, "name": active_alloc_name},
        }
    }


def _status_error_payload() -> Dict[str, Any]:
    """Minimal schema v8 body when status read fails (200 + blocked)."""
    ib_c = ib_client_public_defaults()
    flex_b = ib_flex_public_defaults()
    return {
        "status_schema_version": STATUS_SCHEMA_VERSION,
        "health": {
            "self_check": "blocked",
            "block_reasons": ["status_read_error"],
            "status_lamp": "red",
        },
        "lamps": {"system_lamp": "red"},
        "daemon": {
            "heartbeat": None,
            "self_check": "blocked",
            "lamp": "red",
            "block_reasons": ["status_read_error"],
            "trading": {"auto_status": None, "trading_suspended": False},
        },
        "monitor": {
            "enabled": False,
            "health": "ok",
            "self_check": "blocked",
            "lamp": "red",
            "block_reasons": ["status_read_error"],
        },
        "portfolio": {
            "accounts": None,
            "accounts_fetched_at": None,
            "open_orders": [],
        },
        "config": {
            "ib_client": ib_c,
            "ib_flex": flex_b,
            "redis": {"subscribe_channel": SUBSCRIBE_CHANNEL_DEFAULT},
        },
        "strategy": _strategy_status_block(
            active_structure_id=None,
            active_structure_name=None,
            active_gate_id=None,
            active_gate_name=None,
            active_alloc_id=None,
            active_alloc_name=None,
        ),
        "market_data": {"quotes_redis_reader_ok": False},
        "socket": {
            "massive": None,
            "ib_ingestor": None,
            "ib_account_agent": None,
            "ib_operator": None,
        },
        "celery": {
            "broker_connected": False,
            "workers": [],
            "worker_ib_connected": False,
            "worker_ib_client_id": None,
            "worker_last_updated_ts": None,
        },
        "live_ui": {"subscribed_tickers": [], "reference_indices": []},
    }


def _assemble_status_v3(
    *,
    health_self_check: str,
    health_block_reasons: List[str],
    health_status_lamp: str,
    trading_suspended: bool,
    daemon_heartbeat: Optional[Dict[str, Any]],
    daemon_self_check: str,
    daemon_lamp: str,
    daemon_block_reasons: List[str],
    auto_status: Any,
    subscribed_tickers: List[str],
    reference_indices: Any,
    accounts: Any,
    accounts_fetched_at: Any,
    ib_config: Dict[str, Any],
    flex_config: Any,
    redis_subscribe_channel: str,
    open_orders: Any,
    active_structure_id: Any,
    active_structure_name: Any,
    active_gate_id: Any,
    active_gate_name: Any,
    active_alloc_id: Any,
    active_alloc_name: Any,
    monitor_ib_status: Any,
    monitor_enabled: bool,
    monitor_health: str,
    monitor_self_check: str,
    monitor_lamp: str,
    monitor_block_reasons: List[str],
    quotes_redis_reader_ok: bool,
    celery_broker_connected: bool,
    celery_workers: List[str],
    celery_worker_ib_connected: bool,
    celery_worker_ib_client_id: Any,
    celery_worker_last_updated_ts: Any,
    massive: Any,
    ib_ingestor: Any,
    ib_account_agent: Any,
) -> Dict[str, Any]:
    sl = (health_status_lamp or "red").strip().lower()
    if sl == "red":
        system_lamp = "red"
    elif sl == "yellow":
        system_lamp = "yellow"
    else:
        system_lamp = "green"
    return {
        "status_schema_version": STATUS_SCHEMA_VERSION,
        "health": {
            "self_check": health_self_check,
            "block_reasons": health_block_reasons,
            "status_lamp": health_status_lamp,
        },
        "lamps": {"system_lamp": system_lamp},
        "daemon": {
            "heartbeat": daemon_heartbeat,
            "self_check": daemon_self_check,
            "lamp": daemon_lamp,
            "block_reasons": daemon_block_reasons,
            "trading": {
                "auto_status": auto_status,
                "trading_suspended": trading_suspended,
            },
        },
        "monitor": {
            "enabled": monitor_enabled,
            "health": monitor_health,
            "self_check": monitor_self_check,
            "lamp": monitor_lamp,
            "block_reasons": monitor_block_reasons,
        },
        "portfolio": {
            "accounts": accounts,
            "accounts_fetched_at": accounts_fetched_at,
            "open_orders": open_orders,
        },
        "config": {
            "ib_client": ib_client_for_api(ib_config),
            "ib_flex": ib_flex_for_status_api(ib_config, flex_config),
            "redis": {"subscribe_channel": redis_subscribe_channel},
        },
        "strategy": _strategy_status_block(
            active_structure_id=active_structure_id,
            active_structure_name=active_structure_name,
            active_gate_id=active_gate_id,
            active_gate_name=active_gate_name,
            active_alloc_id=active_alloc_id,
            active_alloc_name=active_alloc_name,
        ),
        "market_data": {"quotes_redis_reader_ok": quotes_redis_reader_ok},
        "socket": {
            "massive": massive,
            "ib_ingestor": ib_ingestor,
            "ib_account_agent": ib_account_agent,
            "ib_operator": monitor_ib_status,
        },
        "celery": {
            "broker_connected": celery_broker_connected,
            "workers": celery_workers,
            "worker_ib_connected": celery_worker_ib_connected,
            "worker_ib_client_id": celery_worker_ib_client_id,
            "worker_last_updated_ts": celery_worker_last_updated_ts,
        },
        "live_ui": {
            "subscribed_tickers": subscribed_tickers,
            "reference_indices": reference_indices,
        },
    }


@router.get("/status")
def get_status(request: Request) -> Dict[str, Any]:
    """Return current run status (schema v8 nested JSON). R-M1b, R-M2, R-M3. Never returns 5xx."""
    global _status_cache, _status_cache_ts
    now_mono = time.monotonic()
    with _status_cache_lock:
        if _status_cache and (now_mono - _status_cache_ts) < _STATUS_CACHE_TTL:
            return _status_cache
    app = request.app
    reader = app.state.reader
    control_via_db = app.state.control_via_db
    data_lag_threshold_ms = app.state.data_lag_threshold_ms
    try:
        status_current_row = reader.get_status_current()
        run_suspended = reader.get_run_status()
        ts_flag = run_suspended if run_suspended is not None else False
        hb = reader.get_daemon_heartbeat()
        daemon_heartbeat: Optional[Dict[str, Any]] = None
        if hb is not None:
            now_ts = time.time()
            last_ts = hb.get("last_ts")
            daemon_heartbeat = {
                "last_ts": last_ts,
                "hedge_running": hb.get("hedge_running", False),
                "daemon_alive": (last_ts is not None and (now_ts - last_ts) < 35),
                "ib_connected": hb.get("ib_connected", False),
                "ib_client_id": hb.get("ib_client_id"),
                "next_retry_ts": hb.get("next_retry_ts"),
                "seconds_until_retry": hb.get("seconds_until_retry"),
                "graceful_shutdown_at": hb.get("graceful_shutdown_at"),
                "heartbeat_interval_sec": hb.get("heartbeat_interval_sec"),
                "redis_quotes_connected": hb.get("redis_quotes_connected", False),
                "last_control_message": hb.get("last_control_message"),
            }
            dsc = derive_daemon_self_check(
                daemon_heartbeat,
                auto_status_row=status_current_row,
                data_lag_threshold_ms=data_lag_threshold_ms,
                trading_suspended=ts_flag,
            )
        else:
            dsc = derive_daemon_self_check(None)
        daemon_self_check = dsc["daemon_self_check"]
        daemon_lamp = dsc["daemon_lamp"]
        daemon_block_reasons = dsc["daemon_block_reasons"]

        symbols_set: set = set()
        if status_current_row and status_current_row.get("symbol"):
            symbols_set.add(str(status_current_row.get("symbol", "") or "").strip())
        for w in reader.get_watchlist():
            st = (w.get("sec_type") or "").strip().upper()
            sym = (w.get("symbol") or "").strip()
            if sym and (st == "STK" or not st):
                symbols_set.add(sym)
        if hb is not None and hb.get("subscribed_tickers") is not None and isinstance(
            hb["subscribed_tickers"], list
        ):
            subscribed_tickers = sorted(
                s for s in hb["subscribed_tickers"] if s and str(s).strip()
            )
        else:
            subscribed_tickers = sorted(s for s in symbols_set if s)

        merged_cfg = getattr(app.state, "bifrost_merged_config", None) or {}
        reference_indices = merge_reference_indices(
            (control_via_db or {}).get("reference_indices"),
            merged_cfg.get("reference_indices"),
        )
        try:
            caret_syms = reader.get_distinct_caret_bar_symbols()
        except Exception:
            caret_syms = []
        reference_indices = augment_reference_indices_with_caret_symbols(reference_indices, caret_syms)
        accounts = reader.get_accounts_from_tables()
        if accounts is None:
            accounts = []
        accounts_fetched_at = reader.get_accounts_fetched_at()
        ib_config = reader.get_ib_config() or {}
        flex_config = reader.get_flex_config()
        open_orders = reader.get_open_orders()

        active_strategy_structure_id = reader.get_active_strategy_structure_id()
        active_gate_safety_strategy_id = reader.get_active_gate_safety_strategy_id()
        active_strategy_allocation_id = reader.get_active_strategy_allocation_id()
        active_strategy_structure_name = None
        try:
            sid = active_strategy_structure_id
            srow = reader.get_structure_by_id(sid) if sid is not None else None
            active_strategy_structure_name = srow.get("name") if srow else None
        except Exception:
            pass
        active_gate_safety_strategy_name = None
        try:
            gid = active_gate_safety_strategy_id
            active_gate_safety_strategy_name = (
                reader.get_gate_safety_name(gid) if gid is not None else None
            )
        except Exception:
            pass
        active_strategy_allocation_name = None
        try:
            aid = active_strategy_allocation_id
            arow = reader.get_allocation_by_id(aid) if aid is not None else None
            active_strategy_allocation_name = arow.get("name") if arow else None
        except Exception:
            pass

        monitor_ib_status = None
        try:
            from bifrost_core.ib_operator.client import build_monitor_ib_status

            gw_status = build_monitor_ib_status(
                reader._config, ib_config if isinstance(ib_config, dict) else None
            )
            if gw_status is not None:
                monitor_ib_status = gw_status
        except Exception:
            pass

        monitor_enabled = bool(getattr(app.state, "monitor_enabled", True))
        monitor_health = "ok"
        monitor_block_reasons: list = []
        monitor_status_obj = monitor_ib_status or {}
        acc_status = monitor_status_obj.get("account") or {}
        host_status = monitor_status_obj.get("host") or {}
        sec_status = monitor_status_obj.get("secondary") or {}
        mkt_status = monitor_status_obj.get("market") or {}
        if not monitor_enabled:
            monitor_block_reasons.append("monitor_stopped")
        if (
            acc_status.get("last_error")
            or host_status.get("last_error")
            or sec_status.get("last_error")
            or mkt_status.get("last_error")
        ):
            monitor_block_reasons.append("monitor_ib_error")
        if not monitor_enabled:
            monitor_self_check = "blocked"
            monitor_lamp = "red"
        elif "monitor_ib_error" in monitor_block_reasons:
            monitor_self_check = "degraded"
            monitor_lamp = "yellow"
        else:
            monitor_self_check = "ok"
            pri_conn = bool(acc_status.get("connected")) or bool(host_status.get("connected"))
            sec_conn = bool(sec_status.get("connected"))
            mkt_conn = bool(mkt_status.get("connected"))
            need_secondary = "secondary" in monitor_status_obj
            if need_secondary and not (pri_conn and sec_conn and mkt_conn):
                monitor_lamp = "yellow" if (pri_conn or sec_conn or mkt_conn) else "red"
            elif not pri_conn and not mkt_conn:
                monitor_lamp = "yellow"
            else:
                monitor_lamp = "green"

        rq = getattr(app.state, "redis_quotes", None)
        quotes_redis_reader_ok = bool(rq and getattr(rq, "available", False))

        celery_broker_connected = False
        celery_workers: List[str] = []
        celery_worker_ib_connected = False
        celery_worker_ib_client_id = None
        try:
            from bifrost_worker.celery.celery_app import (
                get_celery_broker_connected,
                get_worker_ib_status,
                get_celery_workers_ping,
            )

            celery_broker_connected = get_celery_broker_connected()
            celery_workers = get_celery_workers_ping(
                timeout=_STATUS_CELERY_INSPECT_TIMEOUT_SEC
            )
            worker_ib = get_worker_ib_status()
            celery_worker_ib_connected = bool(
                worker_ib and worker_ib.get("connected") and len(celery_workers) > 0
            )
            celery_worker_ib_client_id = worker_ib.get("client_id") if worker_ib else None
        except Exception:
            pass

        celery_worker_last_updated_ts = (
            get_job_bars_backfill_last_updated(control_via_db) if control_via_db else None
        )

        massive = None
        ib_ingestor = None
        ib_account_agent = None
        _rurl: Optional[str] = None
        _r: Any = None
        try:
            from bifrost_core.config.startup import get_effective_ib_config
            from bifrost_core.monitor.integrations.ib_socket_status import build_ib_socket_status
            from bifrost_worker.data.massive.vendor.config import get_massive_settings
            from bifrost_worker.data.massive.vendor.reader import count_pending_massive_jobs
            from bifrost_core.monitor.redis_url import ib_redis_url_from_config, redis_url_from_config
            import redis as redis_mod

            _ib_eff_status = get_effective_ib_config(reader._config)
            _probe_stale_mult = float(_ib_eff_status.get("ib_probe_stale_multiplier") or 2.5)
            _status_now = time.time()

            _ms = get_massive_settings(reader._config)
            _pending_m = count_pending_massive_jobs(control_via_db) if control_via_db else 0
            massive_info: Dict[str, Any] = {
                "configured": bool(_ms.get("api_key")),
                "tier": _ms.get("tier"),
                "pending_jobs": _pending_m,
                "last_snapshot_age_s": None,
            }
            _rurl = redis_url_from_config(reader._config)
            _ib_rurl = ib_redis_url_from_config(reader._config)
            if _rurl:
                _r = redis_mod.from_url(_rurl, decode_responses=True)
                _mh = hgetall_massive_ws_status(_r)
                if _mh:
                    _now = time.time()
                    massive_info["ws_connected"] = redis_hash_field_truthy(_mh, "connected")
                    _wm = (_mh.get("ws_mode") or "").strip()
                    if _wm:
                        massive_info["ws_mode"] = _wm
                    _lm = _mh.get("last_msg_ts")
                    if _lm is not None:
                        try:
                            massive_info["last_msg_age_s"] = max(
                                0.0, _now - float(_lm)
                            )
                        except (TypeError, ValueError):
                            massive_info["last_msg_age_s"] = None
                    else:
                        massive_info["last_msg_age_s"] = None
                    _ua = _mh.get("updated_at")
                    if _ua is not None:
                        try:
                            massive_info["health_updated_age_s"] = max(
                                0.0, _now - float(_ua)
                            )
                        except (TypeError, ValueError):
                            massive_info["health_updated_age_s"] = None
                    else:
                        massive_info["health_updated_age_s"] = None
                    try:
                        _sh_iv = float(_mh.get("service_heartbeat_interval_sec") or 0)
                    except (TypeError, ValueError):
                        _sh_iv = 0.0
                    if _sh_iv > 0:
                        massive_info["service_heartbeat_interval_sec"] = _sh_iv
                        try:
                            _sh_last = float(_mh.get("last_service_heartbeat_at") or 0)
                        except (TypeError, ValueError):
                            _sh_last = 0.0
                        if _sh_last > 0:
                            massive_info["last_service_heartbeat_at"] = _sh_last
                            massive_info["next_service_heartbeat_in_s"] = max(
                                0.0, _sh_last + _sh_iv - _now
                            )
                    try:
                        massive_info["ws_reconnects"] = int(_mh.get("reconnects") or 0)
                    except (TypeError, ValueError):
                        massive_info["ws_reconnects"] = int(_mh.get("reconnects") or 0)
                else:
                    massive_info["ws_connected"] = False
                    massive_info["last_msg_age_s"] = None
            else:
                massive_info["ws_connected"] = None
                massive_info["last_msg_age_s"] = None
            massive = massive_info

            _ib_cfg = _ib_eff_status if isinstance(_ib_eff_status, dict) else {}
            _aa_unreachable = (
                "IB Account Agent unreachable "
                "(Platform IB Gateway @ redis-ib — check data/ib-gateway)"
            )
            _ib_r = _r
            if _ib_rurl and _ib_rurl != _rurl:
                try:
                    _ib_r = redis_mod.from_url(_ib_rurl, decode_responses=True)
                except Exception:
                    _ib_r = _r
            elif rq is not None and getattr(rq, "ib_redis_client", None) is not None:
                _ib_r = rq.ib_redis_client
            if _ib_rurl or _rurl:
                _ih = hgetall_ib_ingestor_health(_ib_r) or {}
                ib_ingestor = build_ib_socket_status(
                    "ib_ingestor",
                    _ih,
                    _ib_cfg,
                    now=_status_now,
                    stale_mult=_probe_stale_mult,
                )
                _ah = hgetall_ib_account_agent_health(_ib_r) or {}
                ib_account_agent = build_ib_socket_status(
                    "ib_account_agent",
                    _ah,
                    _ib_cfg,
                    now=_status_now,
                    stale_mult=_probe_stale_mult,
                    unreachable=_aa_unreachable,
                )
            else:
                ib_ingestor = build_ib_socket_status(
                    "ib_ingestor",
                    None,
                    _ib_cfg,
                    now=_status_now,
                    stale_mult=_probe_stale_mult,
                )
                ib_account_agent = build_ib_socket_status(
                    "ib_account_agent",
                    None,
                    _ib_cfg,
                    now=_status_now,
                    stale_mult=_probe_stale_mult,
                    unreachable=_aa_unreachable,
                )
        except Exception:
            massive = None
            ib_ingestor = None
            ib_account_agent = None

        hc = derive_health_roll_up(
            daemon_lamp=daemon_lamp,
            daemon_block_reasons=daemon_block_reasons,
            monitor_lamp=monitor_lamp,
            monitor_block_reasons=monitor_block_reasons,
            massive=massive,
            ib_ingestor=ib_ingestor,
            quotes_redis_reader_ok=quotes_redis_reader_ok,
            celery_broker_connected=celery_broker_connected,
            celery_workers=celery_workers,
            ib_account_agent=ib_account_agent,
        )

        _rcfg = (getattr(reader, "_config", None) or {}).get("redis") or {}
        _sub_ch_raw = _rcfg.get("subscribe_channel")
        redis_subscribe_channel = (
            str(_sub_ch_raw).strip()
            if _sub_ch_raw is not None and str(_sub_ch_raw).strip() != ""
            else SUBSCRIBE_CHANNEL_DEFAULT
        )

        payload = _assemble_status_v3(
            health_self_check=hc["self_check"],
            health_block_reasons=hc["block_reasons"],
            health_status_lamp=hc["status_lamp"],
            trading_suspended=ts_flag,
            daemon_heartbeat=daemon_heartbeat,
            daemon_self_check=daemon_self_check,
            daemon_lamp=daemon_lamp,
            daemon_block_reasons=daemon_block_reasons,
            auto_status=status_current_row,
            subscribed_tickers=subscribed_tickers,
            reference_indices=reference_indices,
            accounts=accounts,
            accounts_fetched_at=accounts_fetched_at,
            ib_config=ib_config,
            flex_config=flex_config,
            redis_subscribe_channel=redis_subscribe_channel,
            open_orders=open_orders,
            active_structure_id=active_strategy_structure_id,
            active_structure_name=active_strategy_structure_name,
            active_gate_id=active_gate_safety_strategy_id,
            active_gate_name=active_gate_safety_strategy_name,
            active_alloc_id=active_strategy_allocation_id,
            active_alloc_name=active_strategy_allocation_name,
            monitor_ib_status=monitor_ib_status,
            monitor_enabled=monitor_enabled,
            monitor_health=monitor_health,
            monitor_self_check=monitor_self_check,
            monitor_lamp=monitor_lamp,
            monitor_block_reasons=monitor_block_reasons,
            quotes_redis_reader_ok=quotes_redis_reader_ok,
            celery_broker_connected=celery_broker_connected,
            celery_workers=celery_workers,
            celery_worker_ib_connected=celery_worker_ib_connected,
            celery_worker_ib_client_id=celery_worker_ib_client_id,
            celery_worker_last_updated_ts=celery_worker_last_updated_ts,
            massive=massive,
            ib_ingestor=ib_ingestor,
            ib_account_agent=ib_account_agent,
        )
        account_sync_hb = reader.get_account_sync_heartbeat()
        account_sync_block: Optional[Dict[str, Any]] = None
        if account_sync_hb is not None:
            _as_last_ts = account_sync_hb.get("last_ts")
            _now_ts = time.time()
            _as_alive = _as_last_ts is not None and (_now_ts - float(_as_last_ts)) < 35
            if _rurl:
                if _r is None or not _account_sync_redis_reports_alive(_r, now_ts=_now_ts):
                    _as_alive = False
            account_sync_block = {
                "heartbeat": {
                    "last_ts": _as_last_ts,
                    "daemon_alive": _as_alive,
                    "heartbeat_interval_sec": float(
                        account_sync_hb.get("heartbeat_interval_sec") or 5.0,
                    ),
                    "last_sync_version": account_sync_hb.get("last_sync_version", 0),
                    "accounts_synced": account_sync_hb.get("accounts_synced", 0),
                    "positions_synced": account_sync_hb.get("positions_synced", 0),
                    "executions_synced": account_sync_hb.get("executions_synced", 0),
                    "open_orders_synced": account_sync_hb.get("open_orders_synced", 0),
                    "stream_lag": account_sync_hb.get("stream_lag", 0),
                },
            }
        else:
            try:
                if _rurl:
                    _asd_h = hgetall_account_sync_daemon_health(_r)
                    if _asd_h:
                        _asd_alive = redis_hash_field_truthy(_asd_h, "alive")
                        account_sync_block = {
                            "heartbeat": {
                                "last_ts": None,
                                "daemon_alive": _asd_alive,
                                "heartbeat_interval_sec": 5.0,
                                "last_sync_version": 0,
                                "stream_lag": int(_asd_h.get("stream_lag") or 0),
                            },
                        }
            except Exception:
                pass
        payload["account_sync_daemon"] = account_sync_block

        with _status_cache_lock:
            _status_cache = payload
            _status_cache_ts = time.monotonic()
        return payload
    except Exception as e:
        logger.warning("get_status failed: %s", e)
        return _status_error_payload()


@router.get("/open-orders")
def get_open_orders(request: Request) -> Dict[str, Any]:
    """R-A5: Return current open/unfilled orders (symbol, side, qty, limit price, status, filled/remaining)."""
    reader = request.app.state.reader
    items: List[Any] = reader.get_open_orders()
    return {"open_orders": items}


@router.get("/operations")
def get_operations(
    request: Request,
    since_ts: Optional[float] = Query(None, description="Filter operations with ts >= this"),
    until_ts: Optional[float] = Query(None, description="Filter operations with ts <= this"),
    operation_type: Optional[str] = Query(
        None, alias="type", description="Filter by type (hedge_intent, order_sent, fill, reject, cancel)"
    ),
    limit: int = Query(100, ge=1, le=1000),
) -> Dict[str, Any]:
    """Return operations list with optional filters (R-M4b)."""
    reader = request.app.state.reader
    items = reader.get_operations(
        since_ts=since_ts, until_ts=until_ts, type_filter=operation_type, limit=limit
    )
    return {"operations": items}


@router.get("/risk_summary")
def get_risk_summary(request: Request) -> Dict[str, Any]:
    """Return risk/post-mortem summary for replay & risk page (R-M7)."""
    reader = request.app.state.reader
    return reader.get_risk_summary()
