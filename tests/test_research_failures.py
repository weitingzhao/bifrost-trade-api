"""TD-16 (api 0.5.0): the Trade research app answers a failure with its status and ``{"detail"}``.

Before 0.5.0 these routers answered HTTP 200 ``{"ok": false, "error": ...}``, which a
client could not tell from a success without reading the body.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.test_research_app import _make_client

ROUTERS = Path(__file__).resolve().parents[1] / "src" / "bifrost_api" / "research" / "routers"


def _ok_false_returns(path: Path) -> list[int]:
    tree = ast.parse(path.read_text())
    return [
        n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.Dict)
        and any(
            isinstance(k, ast.Constant) and k.value == "ok" and isinstance(v, ast.Constant) and v.value is False
            for k, v in zip(n.keys, n.values)
        )
    ]


@pytest.mark.parametrize("path", sorted(ROUTERS.glob("*.py")), ids=lambda p: p.name)
def test_no_router_answers_ok_false(path: Path) -> None:
    assert _ok_false_returns(path) == []


@pytest.mark.parametrize(
    "method,url,status",
    [
        ("GET", "/research/feedback/reports?scope=bogus", 400),
        ("GET", "/research/data/readiness/tier-stats?tier=bogus", 400),
        ("GET", "/research/data/readiness/tier-filter?tier=bogus", 400),
        ("GET", "/research/data/readiness/fundamental-distribution/symbols?conditions_passed=9", 400),
        ("GET", "/research/data/readiness/symbol-statements?symbol=", 400),
    ],
)
def test_a_refusal_has_its_status_and_detail_only(method: str, url: str, status: int) -> None:
    client = _make_client(merged_config={"server": {}})
    r = client.request(method, url)
    assert r.status_code == status, r.text
    assert set(r.json()) == {"detail"}, r.json()


@pytest.mark.parametrize(
    "path",
    [
        "/research/option-expirations",
        "/research/option-oi",
        "/research/option-trades",
        "/research/iv-term-structure",
        "/research/data/readiness/momentum-distribution",
    ],
)
def test_the_deprecated_research_routes_are_gone(path: str) -> None:
    assert _make_client(merged_config={"server": {}}).get(path).status_code == 404
