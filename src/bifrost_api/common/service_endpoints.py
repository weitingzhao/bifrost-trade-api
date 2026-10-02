"""The per-app auth capabilities endpoints, from one body (TD-64).

Each app answered ``GET <prefix>/auth/capabilities`` from its own copy of the same
three lines, and each also had a ``POST <prefix>/shutdown`` that exited the
process. The shutdowns are gone -- process lifecycle belongs to Kubernetes, and
none was called in 25 days -- and the capabilities paths stay where clients find
them, all served by the function below.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Optional

from fastapi import FastAPI, Request

from bifrost_api.ops.auth import AuthConfig, OpsAuth


def mount_auth_capabilities(
    app: FastAPI,
    paths: Iterable[str],
    config: Callable[[], Optional[Dict[str, Any]]],
) -> None:
    """Serve ``GET`` capabilities on each of ``paths`` of ``app``.

    ``config`` returns the merged YAML -- pass the same one the app's write guard
    reads, so the role a caller is told is the role the guard enforces. Tokens
    come from YAML and env at startup and do not change while the process runs,
    so the auth config is built once, on the first request.
    """
    holder: Dict[str, OpsAuth] = {}

    def auth_capabilities(request: Request) -> Dict[str, Any]:
        """Who the caller is and what their role may do (shared ops.auth tokens)."""
        if "auth" not in holder:
            holder["auth"] = OpsAuth(AuthConfig.from_config(config() or {}))
        return holder["auth"].capabilities(request)

    for path in paths:
        app.add_api_route(path, auth_capabilities, methods=["GET"], tags=["auth"])
