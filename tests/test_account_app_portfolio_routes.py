"""The account app is what serves /api/portfolio — every portfolio route must be on it.

Phase B merged the portfolio domain into the account service, so
`create_account_app` is the factory behind `/api/portfolio/*` in DEV, STG and
PROD; `create_portfolio_app` is not deployed anywhere. A router registered only
on the latter passes its own unit tests, ships, and 404s in every environment.
That happened once. This is the guard.
"""

from __future__ import annotations

from fastapi import FastAPI

from bifrost_api.portfolio.routers import portfolio_short_legs_router


def _paths(*routers) -> set:
    app = FastAPI()
    for r in routers:
        app.include_router(r)
    return {route.path for route in app.routes}


def test_every_portfolio_router_is_mounted_on_the_account_app() -> None:
    from bifrost_api.account import app as account_app_module

    source = account_app_module.__file__
    with open(source, encoding="utf-8") as fh:
        text = fh.read()

    for name in (
        "portfolio_model_router",
        "portfolio_config_router",
        "portfolio_short_legs_router",
    ):
        assert f"app.include_router({name})" in text, (
            f"{name} is not mounted on create_account_app — it would 404 in every environment"
        )


def test_short_legs_is_reachable_at_the_path_the_gateway_strips_to() -> None:
    """The gateway strips `/api/portfolio`, so the app must own `/portfolio/short-legs`."""
    assert "/portfolio/short-legs" in _paths(portfolio_short_legs_router)
