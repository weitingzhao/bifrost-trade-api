#!/usr/bin/env python3
"""Copy engine API tests into bifrost-trade-api/tests."""

from __future__ import annotations

import re
import shutil
from pathlib import Path

ENGINE = Path(__file__).resolve().parents[2] / "bifrost-trader-engine" / "tests"
DEST = Path(__file__).resolve().parents[1] / "tests"

TESTS = [
    "test_docs_app.py",
    "test_massive_app.py",
    "test_research_app.py",
    "test_monitor_status_v2.py",
    "test_ib_config_shape.py",
]

REPLS = [
    (r"\bfrom backend\.monitor\b", "from bifrost_api.monitor"),
    (r"\bfrom backend\.docs\b", "from bifrost_api.docs_api"),
    (r"\bfrom backend\.research\b", "from bifrost_api.research"),
    (r"\bfrom src\.app\.config\b", "from bifrost_core.config.startup"),
    (r"\bfrom src\.monitor\b", "from bifrost_core.monitor"),
]


def main() -> None:
    DEST.mkdir(exist_ok=True)
    shutil.copytree(ENGINE / "research", DEST / "research", dirs_exist_ok=True)
    for name in TESTS:
        src = ENGINE / name
        if not src.is_file():
            continue
        text = src.read_text(encoding="utf-8")
        for pat, rep in REPLS:
            text = re.sub(pat, rep, text)
        (DEST / name).write_text(text, encoding="utf-8")
        print("ok", name)
    for path in (DEST / "research").rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        new = text
        for pat, rep in REPLS:
            new = re.sub(pat, rep, new)
        if new != text:
            path.write_text(new, encoding="utf-8")


if __name__ == "__main__":
    main()
