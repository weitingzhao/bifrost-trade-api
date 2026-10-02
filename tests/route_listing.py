"""The routes an app serves, the same on every FastAPI version the images may install.

From FastAPI 0.14x an included router sits in `app.routes` as one `_IncludedRouter` wrapper
with no `.path`; its routes, prefix applied, are in `effective_candidates`. The images install
whatever `fastapi>=0.100.0` resolves to (0.142.2 on 2026-10-02), so a test that reads `r.path`
off `app.routes` fails there while passing on an older local install. Unlike the OpenAPI path
table, this also lists routes kept out of the schema (docs pages, /metrics) and keeps path
converters such as `{category_id:int}` as written.
"""

from __future__ import annotations

from typing import Any, Iterable, Iterator, List, Set, Tuple


def _walk(routes: Iterable[Any]) -> Iterator[Any]:
    for r in routes:
        if getattr(r, "path", None) is not None:
            yield r
            continue
        for attr in ("effective_candidates", "effective_low_priority_routes"):
            sub = getattr(r, attr, None)
            if sub is not None:
                yield from _walk(sub() if callable(sub) else sub)


def served_routes(app: Any) -> Set[Tuple[str, str]]:
    """Every (METHOD, path) the app answers."""
    return {(m, r.path) for r in _walk(app.routes) for m in (getattr(r, "methods", None) or ())}


def route_paths(app: Any) -> List[str]:
    """Route paths in matching order (an earlier path wins a request both could match)."""
    return [r.path for r in _walk(app.routes)]
