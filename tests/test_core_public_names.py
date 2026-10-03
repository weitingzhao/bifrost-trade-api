"""TD-20: the api uses core's public names only.

core 0.34.0 published get_conn_params / get_golden_source_conn_params / ensure_tables and a
read-only StatusReader.config for what the api imported or read under an underscore. The
private names stay in core as aliases until no downstream uses them; this test keeps the
api from going back to them.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
SOURCES = sorted([*(ROOT / "src").rglob("*.py"), *(ROOT / "scripts").rglob("*.py")])


def _private_core_imports(path: Path) -> List[str]:
    out = []
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("bifrost_core"):
            out += [f"{node.module}.{a.name}" for a in node.names if a.name.startswith("_")]
    return out


def test_no_private_core_name_is_imported() -> None:
    found = {str(p.relative_to(ROOT)): names for p in SOURCES if (names := _private_core_imports(p))}
    assert found == {}


def test_the_reader_is_read_through_its_public_config() -> None:
    private = re.compile(r"\breader\._[a-z]")
    found = [f"{p.relative_to(ROOT)}:{i}" for p in SOURCES for i, line in enumerate(p.read_text().splitlines(), 1) if private.search(line)]
    assert found == []
