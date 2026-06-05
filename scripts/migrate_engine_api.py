#!/usr/bin/env python3
"""Migrate engine backend/* + research into bifrost_api."""

from __future__ import annotations

import re
from pathlib import Path

ENGINE = Path(__file__).resolve().parents[2] / "bifrost-trader-engine"
API = Path(__file__).resolve().parents[1] / "src" / "bifrost_api"

DOMAIN_MAP = {
    "backend/monitor": "monitor",
    "backend/massive": "massive",
    "backend/docs": "docs_api",
    "backend/ops": "ops",
    "backend/trading": "trading",
    "backend/strategy": "strategy",
    "backend/portfolio": "portfolio",
    "backend/market": "market",
    "backend/research": "research",
}

REPLS = [
    (r"\bfrom backend\.monitor\b", "from bifrost_api.monitor"),
    (r"\bfrom backend\.massive\b", "from bifrost_api.massive"),
    (r"\bfrom backend\.docs\b", "from bifrost_api.docs_api"),
    (r"\bfrom backend\.ops\b", "from bifrost_api.ops"),
    (r"\bfrom backend\.trading\b", "from bifrost_api.trading"),
    (r"\bfrom backend\.strategy\b", "from bifrost_api.strategy"),
    (r"\bfrom backend\.portfolio\b", "from bifrost_api.portfolio"),
    (r"\bfrom backend\.market\b", "from bifrost_api.market"),
    (r"\bfrom backend\.research\b", "from bifrost_api.research"),
    (r"\bfrom src\.research\b", "from bifrost_api.research"),
    (r"\bfrom src\.config\b", "from bifrost_core.config"),
    (r"\bfrom src\.core\b", "from bifrost_core.core"),
    (r"\bfrom src\.persistence\b", "from bifrost_core.persistence"),
    (r"\bfrom src\.portfolio\b", "from bifrost_core.portfolio"),
    (r"\bfrom src\.monitor\b", "from bifrost_core.monitor"),
    (r"\bfrom src\.ib_operator\b", "from bifrost_core.ib_operator"),
    (r"\bfrom src\.app\.config\b", "from bifrost_core.config.startup"),
    (r"\bfrom src\.massive\b", "from bifrost_worker.data.massive"),
    (r"\bfrom src\.vendor\.massive\b", "from bifrost_worker.data.massive.vendor"),
]


def rewrite(text: str) -> str:
    for pat, rep in REPLS:
        text = re.sub(pat, rep, text)
    return text


def copy_tree(src_rel: str, dst_rel: str) -> None:
    src_root = ENGINE / src_rel
    if not src_root.is_dir():
        print("skip", src_rel)
        return
    dst_root = API / dst_rel
    for path in src_root.rglob("*.py"):
        rel = path.relative_to(src_root)
        dst = dst_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_text(rewrite(path.read_text(encoding="utf-8")), encoding="utf-8")
    print(f"OK {dst_rel}/")


def main() -> None:
    for eng, api in DOMAIN_MAP.items():
        copy_tree(eng, api)
    copy_tree("src/research", "research/sepa_engine")
    print("Done.")


if __name__ == "__main__":
    main()
