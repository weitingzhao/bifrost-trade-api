"""Typed request bodies for the POST / PUT writes (TD-24, batch 3c-1; Owner decision B).

Before 0.3.1 these routes took ``Dict[str, Any]`` and coerced by hand, and a value
that did not parse became ``None`` -- which on ``PUT /position-categories/tag``
means "delete the tag", so a malformed ``category_id`` deleted the user's tag and
answered ok. Every such body is now a model:

- **Types are strict now.** ``"5"`` is not a number, ``1`` is not ``true``, ``3.0``
  is not an integer (an integer is a number). A wrong type is FastAPI's 422 and
  nothing is written. Presence rules the route already answered (``name is
  required.`` …) stay the route's 400 with the same message.
- **Unknown fields are accepted and ignored this release** (``extra="allow"``):
  the route reads only the declared fields (:meth:`LenientBody.declared`), and the
  request is logged once as ``unknown request fields: <METHOD> <route template>
  ignored [<names>]`` -- field names only, never values; a nested item's field is
  named ``legs[].sort_order``. **Next release these models become
  ``extra="forbid"``** like the PATCH bodies (``write_errors.PatchBody``), so a
  name that shows up in that log is a caller to fix before then.

The log needs the route, which a model does not know: :func:`install_request_field_log`
keeps the request's ASGI scope in a context variable for the validator to read.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, model_validator
from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)

_REQUEST_SCOPE: ContextVar[Optional[Scope]] = ContextVar("bifrost_request_scope", default=None)


class LenientItem(BaseModel):
    """An object inside a request body (a leg, a batch): strict types, unknown keys
    kept out of what the route reads. Reported by the enclosing :class:`LenientBody`."""

    model_config = ConfigDict(extra="allow")


class LenientBody(LenientItem):
    """A POST / PUT body: strict types, unknown fields ignored and logged (this release).

    Next release: ``extra="forbid"`` -- an unknown field becomes a 422, as on PATCH.
    """

    @model_validator(mode="after")
    def _log_unknown_fields(self) -> "LenientBody":
        names = unknown_field_names(self)
        if names:
            scope = _REQUEST_SCOPE.get()
            method = scope.get("method", "?") if scope else "?"
            route = _route_template(scope)
            logger.warning("unknown request fields: %s %s ignored %s", method, route, names)
        return self

    def declared(self, *, exclude_unset: bool = False) -> Dict[str, Any]:
        """The declared fields as plain data, unknown ones dropped at every level.

        ``exclude_unset=True`` keeps only the fields the client sent (an explicit
        null included), for the writers that change "what is in the body"."""
        return _declared(self, exclude_unset)


def unknown_field_names(model: BaseModel, prefix: str = "") -> List[str]:
    """The unknown field names in a body and its items, sorted (``legs[].sort_order``)."""
    names = [f"{prefix}{k}" for k in (model.model_extra or {})]
    for name in type(model).model_fields:
        value = getattr(model, name)
        if isinstance(value, BaseModel):
            names.extend(unknown_field_names(value, f"{prefix}{name}."))
        elif isinstance(value, list):
            seen = set()
            for item in value:
                if isinstance(item, BaseModel):
                    for n in unknown_field_names(item, f"{prefix}{name}[]."):
                        if n not in seen:
                            seen.add(n)
                            names.append(n)
    return sorted(set(names))


def _declared(value: Any, exclude_unset: bool) -> Any:
    if isinstance(value, BaseModel):
        return {
            name: _declared(getattr(value, name), exclude_unset)
            for name in type(value).model_fields
            if not exclude_unset or name in value.model_fields_set
        }
    if isinstance(value, list):
        return [_declared(v, exclude_unset) for v in value]
    if isinstance(value, dict):
        return {k: _declared(v, exclude_unset) for k, v in value.items()}
    return value


def _route_template(scope: Optional[Scope]) -> str:
    if not scope:
        return "?"
    route = scope.get("route")
    template = getattr(route, "path", None)
    return template or scope.get("path", "?")


class _RequestScope:
    """Keep each HTTP request's scope where a body model's validator can read it."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        token = _REQUEST_SCOPE.set(scope)
        try:
            await self.app(scope, receive, send)
        finally:
            _REQUEST_SCOPE.reset(token)


def install_request_field_log(app: Any) -> None:
    """Let :class:`LenientBody` name the method and route template in its log line."""
    app.add_middleware(_RequestScope)
