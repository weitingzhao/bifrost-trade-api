"""Which bifrost-core this process runs, for every ``/health`` (TD-37).

The delivery pipelines clone core beside api and build both into one image, so
the pip version alone cannot say which core commit shipped: one version string
has named several commits. The STG/PROD Dockerfiles bake the cloned commit into
``BIFROST_CORE_SHA``; images built any other way (local compose, the repo's own
Dockerfile, tests) leave it unset and report ``core_sha: null``.
"""

from __future__ import annotations

import os
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
from typing import Dict, Optional

CORE_DISTRIBUTION = "bifrost-core"
CORE_SHA_ENV = "BIFROST_CORE_SHA"


@lru_cache(maxsize=1)
def core_version() -> Optional[str]:
    """Installed bifrost-core version, or None when the distribution is absent."""
    try:
        return version(CORE_DISTRIBUTION)
    except PackageNotFoundError:
        return None


def core_sha() -> Optional[str]:
    """Core commit the image was built from (env ``BIFROST_CORE_SHA``), or None if unset."""
    value = (os.environ.get(CORE_SHA_ENV) or "").strip()
    return value or None


def core_build_info() -> Dict[str, Optional[str]]:
    """The two keys every ``/health`` payload carries."""
    return {"core_version": core_version(), "core_sha": core_sha()}
