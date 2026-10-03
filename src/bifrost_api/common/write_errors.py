"""Write outcomes to HTTP, one place for every PATCH and strict DELETE (TD-15, batch 3b-2).

Core's TD-15 writers (core 0.33.0) return what they wrote or raise one of the
``Write*`` outcomes, each with a ``reason`` the UI can show as it is. Every app
that serves those writers installs :func:`install_write_errors`, so a route just
calls the writer and the outcome becomes:

    WriteNotFound  404   the row the write names does not exist
    WriteConflict  409   in use, or the row's state refuses (``RuleInUseError``,
                         ``PlanRuleError`` are conflicts)
    WriteInvalid   400   the input is wrong (empty, unknown field, NULL on a
                         required column, a bad value)
    WriteFailed    503   Postgres / Golden Source not configured or unreachable
                         (``unavailable``)
                   500   a statement or read-back failed

The body is :func:`bifrost_api.common.envelopes.error_response`'s:
``{"detail": reason}``. A request body that does
not parse (wrong type, unknown key, empty PATCH) is still FastAPI's 422.
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, model_validator

from bifrost_api.common.envelopes import error_response
from bifrost_core.monitor.reader.errors import (
    WriteConflict,
    WriteError,
    WriteFailed,
    WriteInvalid,
    WriteNotFound,
)


def write_error_status(exc: WriteError) -> int:
    """The status a write outcome answers."""
    if isinstance(exc, WriteNotFound):
        return 404
    if isinstance(exc, WriteConflict):
        return 409
    if isinstance(exc, WriteInvalid):
        return 400
    if isinstance(exc, WriteFailed) and getattr(exc, "unavailable", False):
        return 503
    return 500


def write_error_response(exc: WriteError) -> JSONResponse:
    """The failure envelope for one write outcome."""
    return error_response(write_error_status(exc), exc.reason)


def install_write_errors(app: Any) -> None:
    """Answer every ``WriteError`` a route lets through with its status and reason."""

    async def _write_error(_request: Request, exc: WriteError) -> JSONResponse:
        return write_error_response(exc)

    app.add_exception_handler(WriteError, _write_error)


def write_target(request: Request, what: str) -> Dict[str, Any]:
    """The status config a TD-15 writer takes, or the 503 outcome when Postgres is not configured.

    Raised here rather than left to core because three writers (instance delete,
    execution patch and delete) refuse a missing config as a programming error.
    """
    config = getattr(request.app.state, "control_via_db", None)
    if not config:
        raise WriteFailed(f"Cannot write {what}: Postgres is not configured.", unavailable=True)
    return config


def deleted_body(result: Dict[str, Any]) -> Dict[str, Any]:
    """A strict delete's answer: core's ``{"deleted": "hard"|"soft", <id>, ...}`` plus ``ok``.

    ``ok: true`` is the old body's key, kept for one release (decision B) so a tab
    opened before the release still reads success.
    """
    return {**result, "ok": True}


class PatchBody(BaseModel):
    """A PATCH body: only the fields sent change, an explicit null clears.

    Unknown fields are refused (422) and so is an empty body: a PATCH that names
    nothing is a client bug, not a no-op to answer 200.
    """

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def _names_a_field(self) -> "PatchBody":
        if not self.model_fields_set:
            raise ValueError(
                "Send at least one field to change: " + ", ".join(type(self).model_fields) + "."
            )
        return self

    def patch_fields(self) -> Dict[str, Any]:
        """Exactly what the client sent, explicit nulls included."""
        return self.model_dump(exclude_unset=True)
