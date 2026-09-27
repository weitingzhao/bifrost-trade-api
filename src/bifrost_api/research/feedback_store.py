"""ops_feedback.* — the in-system feedback loop's store (K5, D-Journal-Stores).

Installation-keyed (Spec §20.5: reports are the system's — one stream across
dev/stg/prod), living in Golden Source beside ops_jobs.* and owned by this
side over the analytics_reader connection. Research never writes here.

The design's contract (Rev .96/.97): four kinds, `blocks trading` chosen by
the reporter at submit, six statuses, replies land on the row and reading the
row clears the dot — in-system only, nothing leaves.

DDL is idempotent and ensured lazily on first use: this service has no
migration job against Golden Source, and two CREATE IF NOT EXISTS on a warm
path cost one round-trip once per process.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import threading
from typing import Any, Dict, List, Optional

from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

KINDS = ("bug", "data", "idea", "howto")
STATUSES = ("new", "triaged", "progress", "fixed", "answered", "wontfix")
OPEN_STATUSES = ("new", "triaged", "progress")
MAX_IMAGES = 4
MAX_IMAGE_BYTES = 2_000_000
MAX_TITLE = 200
MAX_BODY = 20_000

_DDL = """
CREATE SCHEMA IF NOT EXISTS ops_feedback;
CREATE TABLE IF NOT EXISTS ops_feedback.report (
    report_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind           text NOT NULL,
    title          text NOT NULL,
    body_md        text NOT NULL DEFAULT '',
    page_route     text NOT NULL DEFAULT '',
    page_label     text NOT NULL DEFAULT '',
    blocks_trading boolean NOT NULL DEFAULT false,
    context        jsonb NOT NULL DEFAULT '{}'::jsonb,
    status         text NOT NULL DEFAULT 'new',
    reply_md       text,
    replied_at     timestamptz,
    unread_reply   boolean NOT NULL DEFAULT false,
    created_at     timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS ops_feedback.report_image (
    report_image_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    report_id       bigint NOT NULL REFERENCES ops_feedback.report(report_id) ON DELETE CASCADE,
    seq             smallint NOT NULL,
    mime            text NOT NULL,
    bytes           bytea NOT NULL,
    UNIQUE (report_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_feedback_report_status
    ON ops_feedback.report (status, created_at DESC);
"""

_ddl_done = False
_ddl_lock = threading.Lock()


def ensure_schema(conn: Any) -> None:
    global _ddl_done
    if _ddl_done:
        return
    with _ddl_lock:
        if _ddl_done:
            return
        with conn.cursor() as cur:
            cur.execute(_DDL)
        conn.commit()
        _ddl_done = True


def public_id(report_id: int) -> str:
    """The design's word for a report — FB-0143."""
    return f"FB-{int(report_id):04d}"


def parse_public_id(raw: str) -> Optional[int]:
    s = (raw or "").strip().upper()
    if s.startswith("FB-"):
        s = s[3:]
    try:
        n = int(s)
    except ValueError:
        return None
    return n if n > 0 else None


class FeedbackValidationError(ValueError):
    """A submit the contract refuses — the message is user-facing."""


def decode_images(raw: Any) -> List[Dict[str, Any]]:
    """[{mime, data_b64}] → [{mime, bytes}] under the design's caps (≤4, image/*)."""
    items = raw if isinstance(raw, list) else []
    if len(items) > MAX_IMAGES:
        raise FeedbackValidationError(f"at most {MAX_IMAGES} images")
    out: List[Dict[str, Any]] = []
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            raise FeedbackValidationError("images must be objects")
        mime = str(item.get("mime") or "").strip().lower()
        if not mime.startswith("image/"):
            raise FeedbackValidationError(f"image {i + 1}: mime must be image/*")
        try:
            data = base64.b64decode(str(item.get("data_b64") or ""), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise FeedbackValidationError(f"image {i + 1}: bad base64") from exc
        if not data:
            raise FeedbackValidationError(f"image {i + 1}: empty")
        if len(data) > MAX_IMAGE_BYTES:
            raise FeedbackValidationError(
                f"image {i + 1}: over {MAX_IMAGE_BYTES // 1_000_000} MB"
            )
        out.append({"mime": mime, "bytes": data})
    return out


def _row(r: Dict[str, Any]) -> Dict[str, Any]:
    ctx = r.get("context")
    return {
        "id": public_id(r["report_id"]),
        "report_id": r["report_id"],
        "kind": r["kind"],
        "title": r["title"],
        "body_md": r.get("body_md") or "",
        "page_route": r.get("page_route") or "",
        "page_label": r.get("page_label") or "",
        "blocks_trading": bool(r.get("blocks_trading")),
        "context": ctx if isinstance(ctx, dict) else {},
        "status": r["status"],
        "reply_md": r.get("reply_md"),
        "replied_at": r["replied_at"].isoformat() if r.get("replied_at") else None,
        "unread_reply": bool(r.get("unread_reply")),
        "images": int(r.get("image_count") or 0),
        "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
        "updated_at": r["updated_at"].isoformat() if r.get("updated_at") else None,
    }


_SELECT = """
SELECT r.*, (SELECT count(*) FROM ops_feedback.report_image i
             WHERE i.report_id = r.report_id) AS image_count
FROM ops_feedback.report r
"""


def insert_report(
    conn: Any,
    *,
    kind: str,
    title: str,
    body_md: str = "",
    page_route: str = "",
    page_label: str = "",
    blocks_trading: bool = False,
    context: Optional[Dict[str, Any]] = None,
    images: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    if kind not in KINDS:
        raise FeedbackValidationError(f"kind must be one of {KINDS}")
    title = (title or "").strip()
    if not title:
        raise FeedbackValidationError("title is required")
    if len(title) > MAX_TITLE:
        raise FeedbackValidationError(f"title over {MAX_TITLE} chars")
    if len(body_md or "") > MAX_BODY:
        raise FeedbackValidationError(f"body over {MAX_BODY} chars")
    ensure_schema(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            INSERT INTO ops_feedback.report
                (kind, title, body_md, page_route, page_label, blocks_trading, context)
            VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
            RETURNING *, 0 AS image_count
            """,
            (
                kind,
                title,
                body_md or "",
                (page_route or "")[:300],
                (page_label or "")[:120],
                bool(blocks_trading),
                json.dumps(context or {}),
            ),
        )
        row = dict(cur.fetchone())
        for seq, img in enumerate(images or []):
            cur.execute(
                """
                INSERT INTO ops_feedback.report_image (report_id, seq, mime, bytes)
                VALUES (%s, %s, %s, %s)
                """,
                (row["report_id"], seq, img["mime"], img["bytes"]),
            )
        row["image_count"] = len(images or [])
    conn.commit()
    return _row(row)


def list_reports(conn: Any, *, scope: str = "all", limit: int = 200) -> List[Dict[str, Any]]:
    """Triage order — blocking first, then newest (the design's sort)."""
    ensure_schema(conn)
    where = ""
    if scope == "open":
        where = "WHERE r.status IN ('new','triaged','progress')"
    elif scope == "closed":
        where = "WHERE r.status NOT IN ('new','triaged','progress')"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            {_SELECT}
            {where}
            ORDER BY (r.status IN ('new','triaged','progress')) DESC,
                     r.blocks_trading DESC, r.created_at DESC
            LIMIT %s
            """,
            (max(1, min(int(limit), 500)),),
        )
        rows = cur.fetchall() or []
    return [_row(dict(r)) for r in rows]


def get_image(conn: Any, report_id: int, seq: int) -> Optional[Dict[str, Any]]:
    ensure_schema(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT mime, bytes FROM ops_feedback.report_image WHERE report_id = %s AND seq = %s",
            (report_id, seq),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def _touch(cur: Any, report_id: int, sets: str, params: List[Any]) -> Optional[Dict[str, Any]]:
    cur.execute(
        f"""
        UPDATE ops_feedback.report SET {sets}, updated_at = now()
        WHERE report_id = %s
        RETURNING *, (SELECT count(*) FROM ops_feedback.report_image i
                      WHERE i.report_id = ops_feedback.report.report_id) AS image_count
        """,
        [*params, report_id],
    )
    row = cur.fetchone()
    return dict(row) if row else None


def set_status(conn: Any, report_id: int, status: str) -> Optional[Dict[str, Any]]:
    if status not in STATUSES:
        raise FeedbackValidationError(f"status must be one of {STATUSES}")
    ensure_schema(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        # A status move is news to the reporter the same way a reply is.
        row = _touch(cur, report_id, "status = %s, unread_reply = true", [status])
    conn.commit()
    return _row(row) if row else None


def set_reply(conn: Any, report_id: int, reply_md: str) -> Optional[Dict[str, Any]]:
    text = (reply_md or "").strip()
    if not text:
        raise FeedbackValidationError("reply is empty")
    ensure_schema(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        row = _touch(
            cur,
            report_id,
            "reply_md = %s, replied_at = now(), unread_reply = true",
            [text],
        )
    conn.commit()
    return _row(row) if row else None


def mark_read(conn: Any, report_id: int) -> Optional[Dict[str, Any]]:
    """Seen is read — the My reports pane on screen clears the dot."""
    ensure_schema(conn)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        row = _touch(cur, report_id, "unread_reply = false", [])
    conn.commit()
    return _row(row) if row else None


def summary(conn: Any) -> Dict[str, int]:
    """The top-bar button's numbers: open · unread replies · waiting · blocking."""
    ensure_schema(conn)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              count(*) FILTER (WHERE status IN ('new','triaged','progress')) AS open,
              count(*) FILTER (WHERE unread_reply) AS unread,
              count(*) FILTER (WHERE status = 'new') AS waiting,
              count(*) FILTER (WHERE status IN ('new','triaged','progress')
                               AND blocks_trading) AS blocking
            FROM ops_feedback.report
            """
        )
        open_n, unread, waiting, blocking = cur.fetchone()
    return {
        "open": int(open_n),
        "unread": int(unread),
        "waiting": int(waiting),
        "blocking": int(blocking),
    }
