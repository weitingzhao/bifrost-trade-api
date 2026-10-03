"""TD-24: the request bodies' field lists, committed as contracts/request_bodies.json.

The frontend mirrors these models by hand in src/types/requestBodies.ts and keeps a copy
of this file (src/types/requestBodies.fields.json) that its own test checks the TS types
against. So a field added, renamed or removed here fails this test until the snapshot is
regenerated -- and the snapshot change is the signal to update the frontend copy and types.

Regenerate:  PYTHONPATH=src python tests/test_request_body_fields.py
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Dict, List

from bifrost_api.common.request_bodies import LenientItem
from bifrost_api.market.schemas import requests as market
from bifrost_api.portfolio.schemas import requests as portfolio
from bifrost_api.strategy.schemas import requests as strategy
from bifrost_api.trading.schemas import requests as trading

SNAPSHOT = Path(__file__).resolve().parents[1] / "contracts" / "request_bodies.json"


def current() -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for module in (market, portfolio, strategy, trading):
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if name.startswith("_") or cls.__module__ != module.__name__ or not issubclass(cls, LenientItem):
                continue
            if cls.__name__ != name:
                continue  # an old name kept as an alias (GateSafetyBody = GateSetBody, naming R1)
            out[name] = sorted((f.alias or n) for n, f in cls.model_fields.items())
    return dict(sorted(out.items()))


def test_the_snapshot_is_the_models() -> None:
    assert json.loads(SNAPSHOT.read_text()) == current(), (
        "request body fields changed: regenerate contracts/request_bodies.json "
        "(PYTHONPATH=src python tests/test_request_body_fields.py) and update the frontend's "
        "src/types/requestBodies.ts and requestBodies.fields.json to match"
    )


def test_every_body_and_item_is_listed() -> None:
    names = set(current())
    assert {"StructureBody", "InstrumentClassBody", "ExecutionUpdateBody", "WatchlistBody", "StructureLegItem"} <= names
    assert len(names) >= 20


if __name__ == "__main__":
    SNAPSHOT.write_text(json.dumps(current(), indent=2) + "\n")
    print(f"wrote {SNAPSHOT} ({len(current())} models)")
