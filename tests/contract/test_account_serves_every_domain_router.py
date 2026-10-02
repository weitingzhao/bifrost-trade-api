"""Every strategy / trading / portfolio router is served by the app that is deployed.

api-trading, api-strategy and api-portfolio are Services on the api-account pods
(infra k8s/base/apis/manifest.yaml), so `create_account_app` is the only place a
router in those packages can be reached. The per-domain factories that used to
sit beside it were never deployed and were deleted (debt TD-29); their parity
tests had stayed green while a router mounted only there shipped 404s (16fc849).
This test walks the router modules themselves, so a new file is covered the day
it is added.
"""

from __future__ import annotations

import importlib
import pkgutil
from unittest.mock import MagicMock

import pytest
from fastapi import APIRouter

from bifrost_api.account.app import create_account_app
from tests.contract.helpers import full_server_config
from tests.route_listing import served_routes

PACKAGES = ("bifrost_api.strategy.routers", "bifrost_api.trading.routers", "bifrost_api.portfolio.routers")


def _served() -> set:
    reader = MagicMock()
    reader._config = full_server_config()
    app = create_account_app(reader=reader, control_via_db=None, merged_config=reader._config)
    return served_routes(app)


def _domain_routers():
    for pkg_name in PACKAGES:
        pkg = importlib.import_module(pkg_name)
        for info in pkgutil.iter_modules(pkg.__path__):
            mod = importlib.import_module(f"{pkg_name}.{info.name}")
            router = getattr(mod, "router", None)
            if isinstance(router, APIRouter):
                yield f"{pkg_name}.{info.name}", router


ROUTERS = list(_domain_routers())


def test_found_the_routers() -> None:
    assert len(ROUTERS) >= 8


@pytest.mark.parametrize("name, router", ROUTERS, ids=[n for n, _ in ROUTERS])
def test_router_is_mounted_on_the_account_app(name: str, router: APIRouter) -> None:
    served = _served()
    wanted = {(m, r.path) for r in router.routes for m in (getattr(r, "methods", None) or ())}
    missing = sorted(wanted - served)
    assert not missing, f"{name}: not served by create_account_app: {missing}"
