"""Worker profile registry and instance-ID allocator.

Profiles live in YAML under ``ops.worker_profiles``.  Each key maps to a label
and a list of Celery queues the worker consumes.

Instance IDs follow ``{profile_key}-{seq}`` (e.g. ``stocks_ib-1``, ``options_massive-3``).
Sequence numbers are allocated atomically via Redis INCR; when Redis is
unavailable the allocator falls back to max(existing) + 1.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from bifrost_worker.celery.celery_queue_names import load_canonical_broker_queue_names, parse_broker_queue_display_names

logger = logging.getLogger(__name__)

_PROFILE_KEY_RE = re.compile(r"^[a-zA-Z0-9_]+$")
INSTANCE_ID_RE = re.compile(r"^(?P<profile>[a-zA-Z0-9_]+)-(?P<seq>\d+)$")
_REDIS_SEQ_PREFIX = "bifrost:ops:worker_instance_seq:"
_MAX_WORKER_INSTANCES_CAP = 64


def _coerce_max_worker_instances(raw: object, default: int) -> int:
    """Clamp configured max systemd/Celery worker processes per profile (Ops scale)."""
    if raw is None:
        n = default
    else:
        try:
            n = int(raw)
        except (TypeError, ValueError):
            n = default
    return max(1, min(n, _MAX_WORKER_INSTANCES_CAP))


@dataclass
class WorkerProfile:
    key: str
    label: str
    queues: List[str]
    #: Target number of worker *units* on this Ops host (``bifrost-celery-worker@``); see ``max_worker_instances`` in YAML.
    max_worker_instances: int = 1


@dataclass
class WorkerProfileRegistry:
    profiles: Dict[str, WorkerProfile] = field(default_factory=dict)
    #: Profile keys in UI / Add-all order (``ops.celery.canonical_queue_order`` first, then remaining keys).
    profile_keys_ordered: Tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_config(cls, config: dict) -> "WorkerProfileRegistry":
        ops = config.get("ops") or {}
        celery_ops = ops.get("celery") if isinstance(ops.get("celery"), dict) else {}
        default_max = _coerce_max_worker_instances(
            celery_ops.get("default_max_worker_instances") if isinstance(celery_ops, dict) else None,
            1,
        )
        display = parse_broker_queue_display_names(config)
        raw = ops.get("worker_profiles") or {}
        profiles: Dict[str, WorkerProfile] = {}
        queue_first_owner: Dict[str, str] = {}
        for key, entry in raw.items():
            if not _PROFILE_KEY_RE.match(key):
                logger.warning("Ignoring worker_profile with invalid key %r", key)
                continue
            if not isinstance(entry, dict):
                continue
            label_in = str(entry.get("label", "")).strip()
            queues = entry.get("queues") or []
            if isinstance(queues, str):
                queues = [queues]
            queues = [str(q).strip() for q in queues if str(q).strip()]
            if not queues:
                logger.warning("worker_profile %r has no queues; skipping", key)
                continue
            for q in queues:
                prev = queue_first_owner.get(q)
                if prev is None:
                    queue_first_owner[q] = key
                elif prev != key:
                    logger.warning(
                        "ops.worker_profiles: broker queue %r is listed under both %r and %r. "
                        "Scaled instances will differ by profile key, but the Queue column label may look the same.",
                        q,
                        prev,
                        key,
                    )
            if len(queues) == 1:
                q0 = queues[0]
                label = display.get(q0, label_in or key)
            else:
                label = label_in or key
            max_w = _coerce_max_worker_instances(
                entry.get("max_worker_instances") if isinstance(entry, dict) else None,
                default_max,
            )
            profiles[key] = WorkerProfile(
                key=key,
                label=label,
                queues=queues,
                max_worker_instances=max_w,
            )

        canon = load_canonical_broker_queue_names(config if isinstance(config, dict) else None)
        queue_to_profile: Dict[str, str] = {}
        for pk, prof in profiles.items():
            for q in prof.queues:
                if q not in queue_to_profile:
                    queue_to_profile[q] = pk
        ordered_keys: List[str] = []
        for q in canon:
            pk = queue_to_profile.get(q)
            if pk and pk not in ordered_keys:
                ordered_keys.append(pk)
        for pk in profiles.keys():
            if pk not in ordered_keys:
                ordered_keys.append(pk)
        return cls(profiles=profiles, profile_keys_ordered=tuple(ordered_keys))

    def get(self, key: str) -> Optional[WorkerProfile]:
        return self.profiles.get(key)

    def list_profiles(self) -> List[Dict[str, object]]:
        keys = self.profile_keys_ordered if self.profile_keys_ordered else tuple(self.profiles.keys())
        out: List[Dict[str, object]] = []
        seen: set[str] = set()
        for k in keys:
            if k in seen or k not in self.profiles:
                continue
            seen.add(k)
            p = self.profiles[k]
            out.append(
                {
                    "key": p.key,
                    "label": p.label,
                    "queues": list(p.queues),
                    "max_worker_instances": p.max_worker_instances,
                }
            )
        return out


def allocate_instance_id(
    profile_key: str,
    broker_url: str,
    existing_units: Optional[List[str]] = None,
) -> str:
    """Return the next ``{profile_key}-{seq}`` ID.

    Primary: Redis INCR on ``bifrost:ops:worker_instance_seq:{profile_key}``.
    Fallback: parse *existing_units* for the highest seq with the same prefix.
    """
    seq = _allocate_via_redis(profile_key, broker_url)
    if seq is not None:
        return f"{profile_key}-{seq}"

    return _allocate_via_scan(profile_key, existing_units or [])


def _allocate_via_redis(profile_key: str, broker_url: str) -> Optional[int]:
    try:
        import redis as _redis

        r = _redis.from_url(broker_url, socket_connect_timeout=3, socket_timeout=3)
        seq = r.incr(f"{_REDIS_SEQ_PREFIX}{profile_key}")
        return int(seq)
    except Exception as exc:
        logger.warning("Redis INCR for worker seq failed (%s); falling back to scan", exc)
        return None


def _allocate_via_scan(profile_key: str, existing_units: List[str]) -> str:
    prefix = f"{profile_key}-"
    max_seq = 0
    for u in existing_units:
        # unit string may be "bifrost-celery-worker@bars-2.service"
        at = u.find("@")
        dot = u.rfind(".service")
        if at >= 0:
            iid = u[at + 1 : dot if dot > at else len(u)]
        else:
            iid = u
        if iid.startswith(prefix):
            try:
                seq = int(iid[len(prefix) :])
                max_seq = max(max_seq, seq)
            except ValueError:
                pass
    return f"{profile_key}-{max_seq + 1}"
