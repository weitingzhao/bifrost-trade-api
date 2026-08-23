"""Aggregate Celery inspect + broker metrics into standard worker status."""

from __future__ import annotations

import concurrent.futures
import json
import logging
import re
import time
import threading
from typing import Any, Dict, List, Optional, Tuple

from bifrost_api.ops.models.schemas import WorkerDetail, WorkerStatus, WorkerSummary
from bifrost_worker.celery.celery_app import (
    CELERY_INSPECT_TIMEOUT_SEC,
    OPS_WORKER_PRESENCE_KEY_PREFIX,
)
from bifrost_worker.celery.celery_queue_names import (
    CANONICAL_BROKER_QUEUE_NAMES,
    build_broker_queue_labels,
    load_canonical_broker_queue_names,
)

logger = logging.getLogger(__name__)

# Backward compat: default tuple when no config (prefer :func:`load_canonical_broker_queue_names`).
SUPPORTED_CELERY_QUEUES: Tuple[str, ...] = CANONICAL_BROKER_QUEUE_NAMES

# Redis list keys that look like Celery broker queues (SCAN discovery; excludes streams / result keys).
_CELERY_QUEUE_LIST_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,63}$")


class WorkerStateService:

    def __init__(self, celery_app: Any, broker_url: str, config: Optional[Dict[str, Any]] = None) -> None:
        self._celery = celery_app
        self._broker_url = broker_url
        self._config = config or {}
        ops_cfg = (self._config.get("ops") or {}) if isinstance(self._config, dict) else {}
        self._inspect_timeout = float(
            ops_cfg.get("celery_inspect_timeout_sec", CELERY_INSPECT_TIMEOUT_SEC)
        )
        wall_cfg = float(ops_cfg.get("celery_inspect_wall_sec", 12.0))
        # list_workers uses a thread pool + fut.result(timeout=wall). _inspect_for_worker_list runs
        # sequential inspect RPCs to the broker (each round waits up to timeout for all workers).
        # Flower feels faster because it uses a long-lived UI + fewer blocking snapshot semantics.
        _list_rounds = 2.0  # stats + active_queues (no separate ping — stats implies worker replied)
        self._inspect_wall_sec = max(
            wall_cfg,
            _list_rounds * self._inspect_timeout + 5.0,
        )
        self._worker_list_mode = str(
            ops_cfg.get("worker_list_mode", "redis_presence")
        ).strip().lower()
        self._worker_list_fallback_inspect = bool(
            ops_cfg.get("worker_list_fallback_inspect", True)
        )
        self._worker_list_inspect_cache_ttl_sec = float(
            ops_cfg.get("worker_list_inspect_cache_ttl_sec", 8.0)
        )
        self._worker_list_inspect_quick_timeout_sec = float(
            ops_cfg.get("worker_list_inspect_quick_timeout_sec", 2.0)
        )
        celery_ops = ops_cfg.get("celery") if isinstance(ops_cfg.get("celery"), dict) else {}
        self._prod_worker_hostnames = self._normalize_worker_hostnames(
            celery_ops.get("prod_worker_hostnames")
        )
        self._dev_worker_hostnames = self._normalize_worker_hostnames(
            celery_ops.get("dev_worker_hostnames")
        )
        self._inspect_cache_lock = threading.Lock()
        self._inspect_cache_workers: List[WorkerSummary] = []
        self._inspect_cache_ts = 0.0
        self._inspect_refreshing = False

    def _canonical_celery_queues(self) -> Tuple[str, ...]:
        cfg = self._config if isinstance(self._config, dict) else None
        return load_canonical_broker_queue_names(cfg)

    @staticmethod
    def _normalize_worker_hostnames(raw: object) -> frozenset[str]:
        if not isinstance(raw, list):
            return frozenset()
        out: set[str] = set()
        for item in raw:
            if isinstance(item, str) and item.strip():
                out.add(item.strip().lower())
        return frozenset(out)

    def _worker_hostname(self, worker_id: str) -> Optional[str]:
        if "@" not in worker_id:
            return None
        host = worker_id.rsplit("@", 1)[-1].strip().lower()
        return host or None

    def _infer_worker_config_profile(self, worker_id: str) -> Optional[str]:
        """Fallback when Redis presence omits config_profile (legacy prod workers on Linux host)."""
        host = self._worker_hostname(worker_id)
        if not host:
            return None
        if host in self._dev_worker_hostnames:
            return "dev"
        if host in self._prod_worker_hostnames:
            return "prod"
        # Docker dev Celery containers use a 12-char hex hostname.
        if re.fullmatch(r"[0-9a-f]{12}", host):
            return "dev"
        return None

    def _apply_inferred_worker_profiles(self, workers: List[WorkerSummary]) -> List[WorkerSummary]:
        if not self._prod_worker_hostnames and not self._dev_worker_hostnames:
            return workers
        out: List[WorkerSummary] = []
        for w in workers:
            if w.worker_config_profile:
                out.append(w)
                continue
            inferred = self._infer_worker_config_profile(w.worker_id)
            if inferred:
                out.append(w.model_copy(update={"worker_config_profile": inferred}))
            else:
                out.append(w)
        return out

    def _finalize_worker_list(
        self,
        workers: List[WorkerSummary],
        presence: Optional[List[WorkerSummary]] = None,
    ) -> List[WorkerSummary]:
        merged = (
            self._merge_worker_profiles_from_presence(workers, presence)
            if presence
            else workers
        )
        return self._apply_inferred_worker_profiles(merged)

    # ── internal helpers ──────────────────────────────────────────────────────

    def _inspect(
        self, timeout: Optional[float] = None,
    ) -> Tuple[dict, dict, dict, dict, dict]:
        """Run full Celery inspect (5 rounds). Used by get_worker / queue_summaries."""
        if timeout is None:
            timeout = self._inspect_timeout
        empty: Tuple[dict, dict, dict, dict, dict] = ({}, {}, {}, {}, {})
        try:
            i = self._celery.control.inspect(timeout=timeout)
            ping = i.ping() or {}
            stats = i.stats() or {}
            active = i.active() or {}
            reserved = i.reserved() or {}
            active_queues = i.active_queues() or {}
            return ping, stats, active, reserved, active_queues
        except Exception as e:
            logger.warning("Celery inspect failed: %s", e)
            return empty

    def _inspect_for_worker_list(
        self, timeout: Optional[float] = None,
    ) -> Tuple[dict, dict, dict, dict, dict]:
        """Lighter inspect for Runtime Snapshot: stats + active_queues (2 broker rounds).

        Skips a separate ``ping()`` — workers that reply to ``stats`` are treated as reachable.
        Omits active/reserved task lists; those counts stay 0 here (see get_worker for full inspect).
        """
        if timeout is None:
            timeout = self._inspect_timeout
        empty: Tuple[dict, dict, dict, dict, dict] = ({}, {}, {}, {}, {})
        try:
            i = self._celery.control.inspect(timeout=timeout)
            stats = i.stats() or {}
            active_queues = i.active_queues() or {}
            names = set(stats.keys()) | set(active_queues.keys())
            ping = {n: {"ok": "pong"} for n in names}
            return ping, stats, {}, {}, active_queues
        except Exception as e:
            logger.warning("Celery inspect (worker list) failed: %s", e)
            return empty

    def _broker_connected(self) -> bool:
        try:
            import redis as _redis

            r = _redis.from_url(
                self._broker_url,
                socket_connect_timeout=3,
                socket_timeout=3,
            )
            r.ping()
            return True
        except Exception:
            return False

    @staticmethod
    def _derive_status(
        worker_name: str,
        ping: dict,
        stats: dict,
    ) -> WorkerStatus:
        if worker_name not in ping:
            return WorkerStatus.STOPPED
        if worker_name not in stats:
            return WorkerStatus.RUNNING_DEGRADED
        return WorkerStatus.RUNNING_HEALTHY

    @staticmethod
    def _queues_for(name: str, active_queues: dict) -> List[str]:
        return [
            q.get("name", "")
            for q in active_queues.get(name, [])
            if q.get("name")
        ]

    @staticmethod
    def _concurrency_from_stats(s: dict) -> int:
        pool = s.get("pool")
        if isinstance(pool, dict):
            return pool.get("max-concurrency", 0)
        return 0

    # ── public API ────────────────────────────────────────────────────────────

    def _list_workers_impl(self) -> List[WorkerSummary]:
        ping, stats, active, reserved, active_queues = self._inspect_for_worker_list()
        all_names = sorted(set(stats.keys()) | set(active_queues.keys()))
        if not all_names:
            return []
        results: List[WorkerSummary] = []
        for name in all_names:
            s = stats.get(name, {})
            status = self._derive_status(name, ping, stats)
            last_hb = time.time() if name in ping else None
            results.append(
                WorkerSummary(
                    worker_id=name,
                    status=status,
                    queues=self._queues_for(name, active_queues),
                    concurrency=self._concurrency_from_stats(s),
                    active_tasks=len(active.get(name, [])),
                    reserved_tasks=len(reserved.get(name, [])),
                    last_heartbeat=last_hb,
                )
            )
        return results

    def _list_workers_from_redis_presence(self) -> List[WorkerSummary]:
        """Fast path: SCAN ``bifrost:ops:worker_presence:*`` written by workers (no control.inspect)."""
        try:
            import redis

            r = redis.from_url(
                self._broker_url,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=3,
            )
            cur = 0
            results: List[WorkerSummary] = []
            seen: set[str] = set()
            match = f"{OPS_WORKER_PRESENCE_KEY_PREFIX}*"
            while True:
                cur, batch = r.scan(cursor=cur, match=match, count=256)
                for key in batch:
                    raw = r.get(key)
                    if not raw:
                        continue
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue
                    wid = data.get("worker_id")
                    if not wid and isinstance(key, str) and key.startswith(
                        OPS_WORKER_PRESENCE_KEY_PREFIX
                    ):
                        wid = key[len(OPS_WORKER_PRESENCE_KEY_PREFIX) :]
                    if not wid or not isinstance(wid, str) or wid in seen:
                        continue
                    seen.add(wid)
                    queues_raw = data.get("queues")
                    queues = (
                        [str(x) for x in queues_raw]
                        if isinstance(queues_raw, list)
                        else []
                    )
                    cp = data.get("config_profile")
                    wcp = (
                        str(cp).lower().strip()
                        if isinstance(cp, str) and str(cp).lower().strip() in ("dev", "prod")
                        else None
                    )
                    results.append(
                        WorkerSummary(
                            worker_id=wid,
                            status=WorkerStatus.RUNNING_HEALTHY,
                            queues=queues,
                            concurrency=0,
                            active_tasks=0,
                            reserved_tasks=0,
                            last_heartbeat=time.time(),
                            worker_config_profile=wcp,
                        )
                    )
                if cur == 0:
                    break
            return sorted(results, key=lambda x: x.worker_id)
        except Exception as e:
            logger.warning("list_workers_from_redis_presence failed: %s", e)
            return []

    @staticmethod
    def _merge_worker_profiles_from_presence(
        workers: List[WorkerSummary], presence: List[WorkerSummary],
    ) -> List[WorkerSummary]:
        """Attach ``worker_config_profile`` from Redis presence onto inspect-only rows."""
        pmap = {
            p.worker_id: p.worker_config_profile
            for p in presence
            if p.worker_config_profile
        }
        if not pmap:
            return workers
        out: List[WorkerSummary] = []
        for w in workers:
            prof = pmap.get(w.worker_id)
            if prof and not w.worker_config_profile:
                out.append(w.model_copy(update={"worker_config_profile": prof}))
            else:
                out.append(w)
        return out

    def _list_workers_inspect_bounded(self) -> List[WorkerSummary]:
        """Celery control.inspect — bounded wall time."""
        wall = self._inspect_wall_sec
        if wall <= 0:
            return self._list_workers_impl()
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            fut = pool.submit(self._list_workers_impl)
            try:
                return fut.result(timeout=wall)
            except concurrent.futures.TimeoutError:
                logger.warning(
                    "list_workers timed out after %ss (celery inspect); returning empty",
                    wall,
                )
                return []
        finally:
            pool.shutdown(wait=False)

    def _refresh_inspect_cache_async(self) -> None:
        """Refresh inspect cache in background, so /ops/workers can return quickly."""
        def _run() -> None:
            try:
                fresh = self._list_workers_inspect_bounded()
                with self._inspect_cache_lock:
                    self._inspect_cache_workers = fresh
                    self._inspect_cache_ts = time.time()
            finally:
                with self._inspect_cache_lock:
                    self._inspect_refreshing = False

        t = threading.Thread(target=_run, daemon=True, name="ops-worker-inspect-cache-refresh")
        t.start()

    def _get_inspect_cache_for_presence(self, presence_ids: set[str]) -> List[WorkerSummary]:
        """Return inspect cache snapshot; trigger async refresh when stale/incomplete."""
        now = time.time()
        with self._inspect_cache_lock:
            cached = list(self._inspect_cache_workers)
            cache_age_sec = now - self._inspect_cache_ts if self._inspect_cache_ts > 0 else 1e9
            cached_ids = {w.worker_id for w in cached}
            missing_in_cache = sorted([wid for wid in presence_ids if wid not in cached_ids])
            stale = cache_age_sec > max(1.0, self._worker_list_inspect_cache_ttl_sec)
            need_refresh = stale or (len(cached) == 0) or (len(missing_in_cache) > 0)
            will_start_refresh = need_refresh and (not self._inspect_refreshing)
            if will_start_refresh:
                self._inspect_refreshing = True
        # Cold cache: run one quick sync inspect for correctness, then keep async refresh behavior.
        if len(cached) == 0:
            quick_timeout = max(0.5, min(self._inspect_timeout, self._worker_list_inspect_quick_timeout_sec))
            quick_ping, quick_stats, _a, _r, quick_active_queues = self._inspect_for_worker_list(timeout=quick_timeout)
            quick_names = sorted(set(quick_stats.keys()) | set(quick_active_queues.keys()) | set(quick_ping.keys()))
            if quick_names:
                quick_workers: List[WorkerSummary] = [
                    WorkerSummary(
                        worker_id=name,
                        status=WorkerStatus.RUNNING_HEALTHY,
                        queues=self._queues_for(name, quick_active_queues),
                        concurrency=self._concurrency_from_stats(quick_stats.get(name, {})),
                        active_tasks=0,
                        reserved_tasks=0,
                        last_heartbeat=time.time(),
                    )
                    for name in quick_names
                ]
                with self._inspect_cache_lock:
                    self._inspect_cache_workers = quick_workers
                    self._inspect_cache_ts = time.time()
                cached = quick_workers
        if will_start_refresh:
            self._refresh_inspect_cache_async()
        return cached

    def list_workers(self, force_refresh: bool = False) -> List[WorkerSummary]:
        """Worker list: per ``ops.worker_list_mode``.

        ``redis_presence`` (default): scan ``bifrost:ops:worker_presence:*`` then merge with
        Celery inspect when ``worker_list_fallback_inspect`` is true, so incomplete presence
        keys do not hide live workers.

        ``force_refresh``: re-scan Redis presence keys only (same as ``redis_only``); no Celery
        inspect — fast path for UI refresh after scale/remove without broker RPC cost.
        """
        if force_refresh:
            return self._finalize_worker_list(self._list_workers_from_redis_presence())
        mode = self._worker_list_mode or "redis_presence"
        if mode == "inspect":
            inspected = self._list_workers_inspect_bounded()
            presence = self._list_workers_from_redis_presence()
            return self._finalize_worker_list(inspected, presence)
        if mode == "redis_only":
            return self._finalize_worker_list(self._list_workers_from_redis_presence())
        # redis_presence (default): SCAN presence keys (fast) + merge with Celery inspect.
        # Presence alone can be incomplete (not every worker may write bifrost:ops:worker_presence:*);
        # serve quickly from inspect cache and refresh inspect in background.
        presence = self._list_workers_from_redis_presence()
        if not self._worker_list_fallback_inspect:
            return self._finalize_worker_list(presence)
        presence_ids = {x.worker_id for x in presence}
        inspected = self._get_inspect_cache_for_presence(presence_ids)
        if not presence:
            return self._finalize_worker_list(inspected)
        by_id: Dict[str, WorkerSummary] = {w.worker_id: w for w in inspected}
        for p in presence:
            if p.worker_id not in by_id:
                by_id[p.worker_id] = p
        merged = sorted(by_id.values(), key=lambda x: x.worker_id)
        return self._finalize_worker_list(merged, presence)

    def get_worker(self, worker_id: str) -> Optional[WorkerDetail]:
        ping, stats, active, reserved, active_queues = self._inspect()
        if worker_id not in ping and worker_id not in stats:
            return None
        s = stats.get(worker_id, {})
        pool_info = s.get("pool") if isinstance(s.get("pool"), dict) else {}
        status = self._derive_status(worker_id, ping, stats)
        last_hb = time.time() if worker_id in ping else None
        active_list = active.get(worker_id, [])
        reserved_list = reserved.get(worker_id, [])
        return WorkerDetail(
            worker_id=worker_id,
            status=status,
            queues=self._queues_for(worker_id, active_queues),
            concurrency=self._concurrency_from_stats(s),
            active_tasks=len(active_list),
            reserved_tasks=len(reserved_list),
            last_heartbeat=last_hb,
            pool_type=pool_info.get("implementation", "unknown") if pool_info else "unknown",
            prefetch_count=s.get("prefetch_count"),
            active_task_list=active_list,
            reserved_task_list=reserved_list,
            stats=s,
        )

    @staticmethod
    def _task_routing_key(task: Dict[str, Any]) -> str:
        di = task.get("delivery_info")
        if isinstance(di, dict):
            rk = di.get("routing_key")
            if rk:
                return str(rk).strip()
        name = str(task.get("name") or "")
        del name
        return ""

    def _count_celery_tasks_for_queue(
        self,
        queue_name: str,
        active: Optional[dict] = None,
        reserved: Optional[dict] = None,
    ) -> int:
        if active is None or reserved is None:
            _p, _s, active, reserved, _aq = self._inspect()
        n = 0
        for bucket in (active or {}, reserved or {}):
            for _wid, tasks in bucket.items():
                for t in tasks or []:
                    if not isinstance(t, dict):
                        continue
                    if self._task_routing_key(t) == queue_name:
                        n += 1
        return n

    def _broker_queue_depth(self, queue_name: str) -> int:
        try:
            import redis as _redis

            r = _redis.from_url(
                self._broker_url,
                socket_connect_timeout=3,
                socket_timeout=3,
            )
            r.ping()
            return int(r.llen(queue_name))
        except Exception as e:
            logger.debug("broker_queue_depth(%s): %s", queue_name, e)
        return 0

    def _pg_status_counts(self) -> Optional[Dict[str, int]]:
        """Trade Celery job_bars_backfill retired — no PG queue counts."""
        return None

    def _queues_from_worker_profiles(self) -> set[str]:
        try:
            from bifrost_api.ops.worker_profiles import WorkerProfileRegistry

            reg = WorkerProfileRegistry.from_config(self._config)
            out: set[str] = set()
            for p in reg.profiles.values():
                out.update(p.queues)
            return out
        except Exception as e:
            logger.debug("worker_profiles queues: %s", e)
            return set()

    @classmethod
    def _redis_key_excluded_from_queue_discovery(cls, key: str) -> bool:
        kl = key.lower()
        if key.startswith("bifrost:"):
            return True
        if kl.startswith("celery-task-meta-"):
            return True
        if "unacked" in kl:
            return True
        if kl.startswith("_kombu"):
            return True
        return False

    def _redis_discovered_queue_names_scan(self, r: Any) -> set[str]:
        """LIST keys that look like Celery broker queues (bounded SCAN)."""
        out: set[str] = set()
        scanned = 0
        max_keys = 4096
        try:
            for key in r.scan_iter(count=256):
                scanned += 1
                if scanned > max_keys:
                    break
                if not isinstance(key, str):
                    continue
                if not _CELERY_QUEUE_LIST_NAME_RE.match(key):
                    continue
                if self._redis_key_excluded_from_queue_discovery(key):
                    continue
                try:
                    t = r.type(key)
                    ts = t if isinstance(t, str) else (t.decode() if isinstance(t, bytes) else str(t))
                except Exception:
                    continue
                if ts != "list":
                    continue
                out.add(key)
        except Exception as e:
            logger.debug("redis queue name scan: %s", e)
        return out

    def queue_summaries(self) -> Dict[str, Any]:
        """Per-queue Redis LLEN plus PostgreSQL job totals for known pipelines.

        Rows include all configured canonical queues (``ops.celery.canonical_queue_order``), then any extra names from
        ``ops.worker_profiles`` and Redis LIST keys discovered via SCAN (custom
        Celery queues not listed in config still show up when present on the broker).
        """
        bars_db = self._pg_status_counts()
        profile_queues = self._queues_from_worker_profiles()
        supported = self._canonical_celery_queues()
        canonical = set(supported)
        extras: set[str] = set(profile_queues) - canonical
        ordered: List[str] = []
        llens: Dict[str, Optional[int]] = {}

        try:
            import redis as _redis

            r = _redis.from_url(
                self._broker_url,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=4,
            )
            r.ping()
            discovered = self._redis_discovered_queue_names_scan(r)
            extras |= discovered - canonical
            ordered = list(supported) + sorted(extras)
            for q in ordered:
                try:
                    llens[q] = int(r.llen(q))
                except Exception:
                    llens[q] = None
        except Exception as e:
            logger.debug("queue_summaries redis: %s", e)
            ordered = list(supported) + sorted(extras)
            for q in ordered:
                llens[q] = None

        labels = build_broker_queue_labels(self._config if isinstance(self._config, dict) else {})
        rows: List[Dict[str, Any]] = []
        for q in ordered:
            pending_broker = llens.get(q)
            display_name = labels.get(q) or q
            row = {
                "name": q,
                "display_name": display_name,
                "pending_broker": pending_broker,
                "running_celery": None,
                "done_db": None,
                "failed_db": None,
            }
            rows.append(row)
        return {
            "queues": rows,
            "db_connected": bars_db is not None,
        }

    def broker_status(self) -> Dict[str, Any]:
        connected = self._broker_connected()
        url_safe = (
            self._broker_url.split("@")[-1]
            if "@" in self._broker_url
            else self._broker_url
        )
        result: Dict[str, Any] = {"connected": connected, "url_masked": url_safe}
        if not connected:
            return result
        try:
            import redis as _redis

            r = _redis.from_url(
                self._broker_url,
                socket_connect_timeout=3,
                socket_timeout=3,
            )
            mem = r.info(section="memory")
            result["used_memory_human"] = mem.get("used_memory_human", "N/A")
            clients = r.info(section="clients")
            result["connected_clients"] = clients.get("connected_clients", 0)
            # Query only canonical queue names to avoid scanning celery-task-meta-* result keys.
            # r.keys("celery*") can return thousands of result keys (result_expires=86400) and
            # calling r.type() for each one blocks the event loop when invoked from async handlers.
            queues: Dict[str, int] = {}
            for q in self._canonical_celery_queues():
                try:
                    depth = r.llen(q)
                    if depth is not None:
                        queues[q] = int(depth)
                except Exception:
                    pass
            if queues:
                result["queues"] = queues
        except Exception as e:
            logger.debug("broker_status detail collection failed: %s", e)
        return result
