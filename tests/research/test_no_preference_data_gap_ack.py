"""Assert Trade research API no longer owns preference_data_gap_ack."""

from __future__ import annotations

from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "bifrost_api"


def test_no_preference_data_gap_ack_in_source() -> None:
    hits: list[str] = []
    for path in SRC.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "preference_data_gap_ack" in text:
            hits.append(str(path.relative_to(SRC)))
    assert hits == [], f"preference_data_gap_ack still referenced in: {hits}"
