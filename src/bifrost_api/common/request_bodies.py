"""Typed request bodies for the POST / PUT writes (TD-24, batch 3c-1; Owner decision B).

Before 0.3.1 these routes took ``Dict[str, Any]`` and coerced by hand, and a value
that did not parse became ``None`` -- which on ``PUT /position-categories/tag``
means "delete the tag", so a malformed ``category_id`` deleted the user's tag and
answered ok. Every such body is now a model:

- **Types are strict.** ``"5"`` is not a number, ``1`` is not ``true``, ``3.0``
  is not an integer (an integer is a number). A wrong type is FastAPI's 422 and
  nothing is written. Presence rules the route already answered (``name is
  required.`` …) stay the route's 400 with the same message.
- **Unknown fields are refused** (``extra="forbid"``, api 0.9.0), like the PATCH
  bodies (``write_errors.PatchBody``): a field the model does not declare, at the top
  or inside an item (``legs[0].leg_uid``), is a 422 of type ``extra_forbidden`` that
  names it, and nothing is written. From 0.3.1 to 0.8.x such fields were ignored and
  each request was logged as ``unknown request fields: <METHOD> <route> ignored
  [<names>]``; the switch waited for a week of PROD, STG and DEV logs with no such line
  (Owner rule, gate 2026-10-09).
"""

from __future__ import annotations

from typing import Any, Dict

from pydantic import BaseModel, ConfigDict


class StrictItem(BaseModel):
    """An object inside a request body (a leg, a batch): strict types, no undeclared keys."""

    model_config = ConfigDict(extra="forbid")


class StrictBody(StrictItem):
    """A POST / PUT body: strict types, no undeclared fields (422 ``extra_forbidden``)."""

    def declared(self, *, exclude_unset: bool = False) -> Dict[str, Any]:
        """The fields as plain data, nested items included.

        ``exclude_unset=True`` keeps only the fields the client sent (an explicit
        null included), for the writers that change "what is in the body"."""
        return _declared(self, exclude_unset)


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
