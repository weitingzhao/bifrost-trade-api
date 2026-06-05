"""Persistent audit store for the Ops control plane.

Phase 1: in-memory list (same as before).
Phase 4: PostgreSQL-backed with full persistence.
Falls back to in-memory if DB is unavailable.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional

from bifrost_api.ops.models.schemas import AuditEntry

logger = logging.getLogger(__name__)

_MAX_MEMORY_ENTRIES = 2000


class AuditStore:
    """Dual-mode audit store: PostgreSQL when available, in-memory fallback."""

    def __init__(
        self,
        dsn: Optional[str] = None,
        table: str = "ops_audit_log",
    ) -> None:
        self._memory: List[AuditEntry] = []
        self._lock = threading.Lock()
        self._dsn = dsn
        self._table = table
        self._db_available = False
        if dsn:
            self._try_init_db()

    @classmethod
    def from_config(cls, config: dict) -> "AuditStore":
        pg = config.get("postgres") or {}
        ops = config.get("ops") or {}
        audit_cfg = ops.get("audit") or {}

        if not audit_cfg.get("persist", False):
            logger.info("Ops audit: in-memory mode (persist=false or not set)")
            return cls(dsn=None)

        host = pg.get("host", "127.0.0.1")
        port = pg.get("port", 5432)
        db = pg.get("database", "options_db")
        user = pg.get("user", "bifrost")
        password = pg.get("password", "")
        dsn = f"host={host} port={port} dbname={db} user={user} password={password}"
        return cls(dsn=dsn)

    def _try_init_db(self) -> None:
        try:
            import psycopg2
            conn = psycopg2.connect(self._dsn, connect_timeout=5)
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self._table} (
                    id          BIGSERIAL PRIMARY KEY,
                    timestamp   DOUBLE PRECISION NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()),
                    operator    TEXT NOT NULL DEFAULT 'unknown',
                    source_ip   TEXT,
                    action      TEXT NOT NULL,
                    target      TEXT NOT NULL,
                    command_id  TEXT,
                    outcome     TEXT NOT NULL,
                    detail      TEXT,
                    request_id  TEXT
                )
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self._table}_ts
                ON {self._table} (timestamp DESC)
            """)
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_{self._table}_outcome
                ON {self._table} (outcome)
            """)
            conn.commit()
            cur.close()
            conn.close()
            self._db_available = True
            logger.info("Ops audit: PostgreSQL persistence enabled (table=%s)", self._table)
        except Exception as e:
            logger.warning("Ops audit: DB init failed, falling back to memory: %s", e)
            self._db_available = False

    def append(self, entry: AuditEntry) -> None:
        with self._lock:
            self._memory.append(entry)
            if len(self._memory) > _MAX_MEMORY_ENTRIES:
                self._memory = self._memory[-_MAX_MEMORY_ENTRIES:]

        if self._db_available:
            self._persist(entry)

    def _persist(self, entry: AuditEntry) -> None:
        try:
            import psycopg2
            conn = psycopg2.connect(self._dsn, connect_timeout=3)
            cur = conn.cursor()
            cur.execute(
                f"""INSERT INTO {self._table}
                    (timestamp, operator, source_ip, action, target, command_id, outcome, detail)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    entry.timestamp,
                    entry.operator,
                    entry.source_ip,
                    entry.action,
                    entry.target,
                    entry.command_id,
                    entry.outcome,
                    entry.detail,
                ),
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.debug("Ops audit: DB persist failed: %s", e)

    def list_recent(self, limit: int = 100) -> List[AuditEntry]:
        if self._db_available:
            try:
                return self._list_from_db(limit)
            except Exception as e:
                logger.debug("Ops audit: DB read failed, using memory: %s", e)

        with self._lock:
            entries = sorted(self._memory, key=lambda e: e.timestamp, reverse=True)
            return entries[:limit]

    def _list_from_db(self, limit: int) -> List[AuditEntry]:
        import psycopg2
        conn = psycopg2.connect(self._dsn, connect_timeout=3)
        cur = conn.cursor()
        cur.execute(
            f"SELECT timestamp, operator, source_ip, action, target, command_id, outcome, detail "
            f"FROM {self._table} ORDER BY timestamp DESC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [
            AuditEntry(
                timestamp=r[0],
                operator=r[1],
                source_ip=r[2],
                action=r[3],
                target=r[4],
                command_id=r[5],
                outcome=r[6],
                detail=r[7],
            )
            for r in rows
        ]

    @property
    def db_available(self) -> bool:
        return self._db_available

    def stats(self) -> Dict[str, Any]:
        return {
            "mode": "postgresql" if self._db_available else "memory",
            "memory_entries": len(self._memory),
        }
