"""`/gate-sets` -- the gate set under its own name (naming program R1, api 0.7.0; D5-A, D6-A).

A gate set is one set of limit parameters a daemon allocation runs under. Its table keeps
the legacy name ``gate_safety_strategy`` (D6-A: renaming it would touch the daemon's
start-up read during the D10 freeze), so the id keeps the table's name too:
``gate_safety_strategy_id``.

    GET    /gate-sets                              every set
    GET    /gate-sets/defaults                     core's default gates (TD-72)
    GET    /gate-sets/{gate_safety_strategy_id}    one set with gates + earnings_dates (404)
    POST   /gate-sets                              create
    PUT    /gate-sets/{gate_safety_strategy_id}    replace
    PATCH  /gate-sets/{gate_safety_strategy_id}    change the fields sent
    DELETE /gate-sets/{gate_safety_strategy_id}    strict delete (409 while in use)

The same functions answer ``/strategies/gate-safety…`` until R4; those routes are marked
replaced (``bifrost_api.deprecations.REPLACED_ROUTES``) with a ``Link`` to these.
"""

from __future__ import annotations

from fastapi import APIRouter

from bifrost_api.strategy.routers import strategies as s
from bifrost_api.strategy.schemas.responses import GateSetDetail, GateSetList

router = APIRouter(tags=["gate-sets"])

_ID = "/gate-sets/{gate_safety_strategy_id:int}"

router.add_api_route(
    "/gate-sets", s.list_gate_safety, methods=["GET"], response_model=GateSetList, response_model_exclude_unset=True
)
# Before the id route: "defaults" is not an id.
router.add_api_route("/gate-sets/defaults", s.get_gate_safety_defaults, methods=["GET"])
router.add_api_route(
    _ID, s.get_gate_safety_by_id, methods=["GET"], response_model=GateSetDetail, response_model_exclude_unset=True
)
router.add_api_route("/gate-sets", s.create_gate_safety_endpoint, methods=["POST"])
router.add_api_route(_ID, s.update_gate_safety_endpoint, methods=["PUT"])
router.add_api_route(
    _ID, s.patch_gate_safety_endpoint, methods=["PATCH"], response_model=GateSetDetail, response_model_exclude_unset=True
)
router.add_api_route(_ID, s.delete_gate_safety_endpoint, methods=["DELETE"])
