"""`/preferences/saved-searches` -- a page's scope kept under a name (naming program R1, D5-A).

The rows are ``preference_saved_search``: a preference of the one operator, not part of
the rule chain, so they leave ``/strategies``. Same functions and answers as
``bifrost_api.strategy.routers.saved_searches``, whose ``/strategies/saved-searches``
routes are marked replaced until R4.

    GET    /preferences/saved-searches                               every saved search
    POST   /preferences/saved-searches                               {route, label, state_json}
    DELETE /preferences/saved-searches/{preference_saved_search_id}  forget one (404)
"""

from __future__ import annotations

from fastapi import APIRouter

from bifrost_api.strategy.routers import saved_searches as ss

router = APIRouter(tags=["saved-searches"])

router.add_api_route("/preferences/saved-searches", ss.list_saved_searches_endpoint, methods=["GET"])
router.add_api_route("/preferences/saved-searches", ss.create_saved_search_endpoint, methods=["POST"])
router.add_api_route(
    "/preferences/saved-searches/{preference_saved_search_id:int}", ss.delete_saved_search_endpoint, methods=["DELETE"]
)
