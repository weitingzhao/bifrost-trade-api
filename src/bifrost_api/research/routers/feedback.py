"""Feedback API (K5 — design Rev .96/.97; store ops_feedback.*, D-Journal-Stores).

The loop closes in-system: the shell submits here with the page's context
attached, Settings › My reports reads the reporter's side (seen = read), and
/system/feedback is the triage face — status moves and replies land back on
the row as the unread dot. Nothing leaves the system.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from fastapi import APIRouter, Body, Response

from bifrost_api.common.envelopes import error_response
from bifrost_api.research import feedback_store as store
from bifrost_api.research.feedback_store import get_conn

logger = logging.getLogger(__name__)

router = APIRouter(tags=["research-feedback"])


def _err(status: int, msg: str) -> Any:
    """A refusal with its status and ``{"detail"}`` (TD-16; was HTTP 200 ``{ok: false, error}``)."""
    return error_response(status, msg)


NOT_MIGRATED_DETAIL = "feedback store not migrated: ops_feedback is missing in Golden Source — run db-init"


def _store_down(exc: Exception) -> Any:
    """503 for a store that is unreachable, or was never created (db-init owns the DDL, TD-77)."""
    if store.not_migrated(exc):
        return _err(503, NOT_MIGRATED_DETAIL)
    return _err(503, f"feedback store error: {exc}")


@router.post("/research/feedback/reports")
def post_report(body: Dict[str, Any] = Body(default={})) -> Any:
    try:
        images = store.decode_images(body.get("images"))
        with get_conn() as conn:
            report = store.insert_report(
                conn,
                kind=str(body.get("kind") or ""),
                title=str(body.get("title") or ""),
                body_md=str(body.get("body_md") or ""),
                page_route=str(body.get("page_route") or ""),
                page_label=str(body.get("page_label") or ""),
                blocks_trading=bool(body.get("blocks_trading")),
                context=body.get("context") if isinstance(body.get("context"), dict) else {},
                images=images,
            )
        return {"ok": True, "report": report}
    except store.FeedbackValidationError as exc:
        return _err(400, str(exc))
    except Exception as exc:  # noqa: BLE001 — the store may be unreachable
        logger.warning("feedback insert failed: %s", exc)
        return _store_down(exc)


@router.get("/research/feedback/reports")
def get_reports(scope: str = "all", limit: int = 200) -> Any:
    if scope not in ("all", "open", "closed"):
        return _err(400, "scope must be all|open|closed")
    try:
        with get_conn() as conn:
            rows = store.list_reports(conn, scope=scope, limit=limit)
        return {"ok": True, "reports": rows, "count": len(rows)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("feedback list failed: %s", exc)
        return _store_down(exc)


@router.get("/research/feedback/summary")
def get_summary() -> Any:
    try:
        with get_conn() as conn:
            return {"ok": True, **store.summary(conn)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("feedback summary failed: %s", exc)
        return _store_down(exc)


def _by_public(raw_id: str) -> int | None:
    return store.parse_public_id(raw_id)


@router.post("/research/feedback/reports/{report_id}/read")
def post_read(report_id: str) -> Any:
    rid = _by_public(report_id)
    if rid is None:
        return _err(400, "bad report id")
    try:
        with get_conn() as conn:
            row = store.mark_read(conn, rid)
        return {"ok": True, "report": row} if row else _err(404, "report not found")
    except Exception as exc:  # noqa: BLE001
        return _store_down(exc)


@router.post("/research/feedback/reports/{report_id}/status")
def post_status(report_id: str, body: Dict[str, Any] = Body(default={})) -> Any:
    rid = _by_public(report_id)
    if rid is None:
        return _err(400, "bad report id")
    try:
        with get_conn() as conn:
            row = store.set_status(conn, rid, str(body.get("status") or ""))
        return {"ok": True, "report": row} if row else _err(404, "report not found")
    except store.FeedbackValidationError as exc:
        return _err(400, str(exc))
    except Exception as exc:  # noqa: BLE001
        return _store_down(exc)


@router.post("/research/feedback/reports/{report_id}/reply")
def post_reply(report_id: str, body: Dict[str, Any] = Body(default={})) -> Any:
    rid = _by_public(report_id)
    if rid is None:
        return _err(400, "bad report id")
    try:
        with get_conn() as conn:
            row = store.set_reply(conn, rid, str(body.get("reply_md") or ""))
        return {"ok": True, "report": row} if row else _err(404, "report not found")
    except store.FeedbackValidationError as exc:
        return _err(400, str(exc))
    except Exception as exc:  # noqa: BLE001
        return _store_down(exc)


@router.get("/research/feedback/reports/{report_id}/images/{seq}")
def get_report_image(report_id: str, seq: int) -> Response:
    rid = _by_public(report_id)
    if rid is None:
        return Response(status_code=404)
    try:
        with get_conn() as conn:
            img = store.get_image(conn, rid, seq)
    except Exception:  # noqa: BLE001
        return Response(status_code=503)
    if not img:
        return Response(status_code=404)
    return Response(content=bytes(img["bytes"]), media_type=img["mime"])
