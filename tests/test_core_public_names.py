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


def test_the_desk_controls_import_core_canonical_modules() -> None:
    """TD-80 C1-a (with TD-40, api 0.7.6): monitor/routers/daemon.py names the modules that define
    what it calls, so core can drop the ``monitor.reader`` / ``config.startup`` re-exports. The
    functions are the same objects, so the desk's routes behave as before."""
    import bifrost_core.config.startup as startup
    import bifrost_core.config.yaml_config as yaml_config
    import bifrost_core.monitor.reader as reader_pkg
    import bifrost_core.monitor.reader.status as status
    import bifrost_core.portfolio.reader.accounts as accounts

    path = ROOT / "src" / "bifrost_api" / "monitor" / "routers" / "daemon.py"
    modules = {node.module for node in ast.walk(ast.parse(path.read_text())) if isinstance(node, ast.ImportFrom)}
    assert not modules & {"bifrost_core.monitor.reader", "bifrost_core.config.startup"}
    assert reader_pkg.write_control_command is status.write_control_command
    assert reader_pkg.write_run_status is status.write_run_status
    assert reader_pkg.sync_accounts_snapshot_to_db is accounts.sync_accounts_snapshot_to_db
    assert startup.get_effective_ib_config is yaml_config.get_effective_ib_config
