"""Token-based authentication for the Ops control plane.

Tokens are configured in YAML under ``ops.auth.tokens``.
Each entry maps a bearer token to a role (viewer/operator/admin) and a display name.

If no tokens are configured, the ``default_role`` (from config or "viewer") applies
to all unauthenticated requests — useful for local development.
"""

from __future__ import annotations

import hashlib
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from fastapi import Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

ROLE_HIERARCHY: Dict[str, int] = {"viewer": 0, "operator": 1, "admin": 2}
VALID_ROLES = frozenset(ROLE_HIERARCHY.keys())


@dataclass
class Identity:
    name: str = "anonymous"
    role: str = "viewer"
    authenticated: bool = False

    @property
    def role_level(self) -> int:
        return ROLE_HIERARCHY.get(self.role, 0)


@dataclass
class AuthConfig:
    tokens: Dict[str, Identity] = field(default_factory=dict)
    default_role: str = "viewer"
    allow_unauthenticated_reads: bool = True

    @classmethod
    def from_config(cls, config: dict) -> "AuthConfig":
        ops = config.get("ops") or {}
        auth_cfg = ops.get("auth") or {}

        tokens: Dict[str, Identity] = {}
        raw_tokens: list = auth_cfg.get("tokens") or []
        for entry in raw_tokens:
            if not isinstance(entry, dict):
                continue
            tok = str(entry.get("token") or "").strip()
            if not tok:
                continue
            role = str(entry.get("role", "viewer")).strip().lower()
            if role not in VALID_ROLES:
                logger.warning("Ignoring auth token with invalid role %r", role)
                continue
            name = str(entry.get("name", "user")).strip()
            token_hash = _hash_token(tok)
            tokens[token_hash] = Identity(name=name, role=role, authenticated=True)

        env_op_token = os.environ.get("OPS_OPERATOR_TOKEN", "").strip()
        if env_op_token:
            h = _hash_token(env_op_token)
            if h not in tokens:
                tokens[h] = Identity(name="env-operator", role="operator", authenticated=True)

        env_admin_token = os.environ.get("OPS_ADMIN_TOKEN", "").strip()
        if env_admin_token:
            h = _hash_token(env_admin_token)
            if h not in tokens:
                tokens[h] = Identity(name="env-admin", role="admin", authenticated=True)

        default_role = str(auth_cfg.get("default_role", "viewer")).strip().lower()
        if default_role not in VALID_ROLES:
            default_role = "viewer"

        allow_unauth = bool(auth_cfg.get("allow_unauthenticated_reads", True))

        if tokens:
            logger.info(
                "Ops auth: %d token(s) configured, default_role=%s",
                len(tokens), default_role,
            )
        else:
            logger.info(
                "Ops auth: no tokens configured — all requests get role=%s",
                default_role,
            )

        return cls(
            tokens=tokens,
            default_role=default_role,
            allow_unauthenticated_reads=allow_unauth,
        )


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class OpsAuth:
    """Resolve identity from ``Authorization: Bearer`` or query ``token`` / ``access_token``."""

    def __init__(self, auth_config: AuthConfig) -> None:
        self._cfg = auth_config

    def resolve(self, request: Request) -> Identity:
        raw_token = ""
        auth_header = request.headers.get("Authorization", "").strip()
        if auth_header.lower().startswith("bearer "):
            raw_token = auth_header[7:].strip()
        if not raw_token:
            raw_token = (
                request.query_params.get("token")
                or request.query_params.get("access_token")
                or ""
            ).strip()
        if raw_token:
            token_hash = _hash_token(raw_token)
            ident = self._cfg.tokens.get(token_hash)
            if ident is not None:
                return ident
            return Identity(name="invalid-token", role="viewer", authenticated=False)

        return Identity(
            name="anonymous",
            role=self._cfg.default_role,
            authenticated=False,
        )

    def require_role(
        self, request: Request, minimum: str,
    ) -> tuple[Identity, Optional[JSONResponse]]:
        ident = self.resolve(request)
        min_level = ROLE_HIERARCHY.get(minimum, 99)
        if ident.role_level < min_level:
            return ident, JSONResponse(
                status_code=403,
                content={
                    "ok": False,
                    "error": (
                        f"Insufficient permissions; {minimum} role required "
                        f"(current: {ident.role})."
                    ),
                    "authenticated": ident.authenticated,
                    "current_role": ident.role,
                    "required_role": minimum,
                },
            )
        return ident, None

    @property
    def has_tokens(self) -> bool:
        return bool(self._cfg.tokens)

    def capabilities(self, request: Request) -> Dict[str, Any]:
        ident = self.resolve(request)
        return {
            "ok": True,
            "identity": {
                "name": ident.name,
                "role": ident.role,
                "authenticated": ident.authenticated,
            },
            "capabilities": {
                "can_view": True,
                "can_operate": ident.role_level >= ROLE_HIERARCHY["operator"],
                "can_admin": ident.role_level >= ROLE_HIERARCHY["admin"],
            },
            "auth_required": self.has_tokens,
        }
