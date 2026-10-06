"""No bare reader mocks in the suite (TD-154 ratchet).

A ``MagicMock()`` standing in for the core ``StatusReader`` answers to any attribute, so an
assertion like ``reader.create_position_category.assert_not_called()`` kept passing after
core 0.49.0 removed that method. Every reader stand-in goes through ``tests.reader_mock.reader_mock``
(an autospec of ``StatusReader``) or passes ``spec=`` / ``spec_set=`` itself.

The scan flags a ``Mock(...)`` / ``MagicMock(...)`` without a spec that is assigned to a name or
attribute ending in ``reader`` (``reader = MagicMock()``, ``app.state.reader = ...``,
``reader = reader or MagicMock()``) or passed as a ``reader=`` keyword.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Iterator, List

import pytest

from tests.reader_mock import reader_mock

TESTS = Path(__file__).resolve().parent
MOCK_NAMES = {"Mock", "MagicMock", "NonCallableMock", "NonCallableMagicMock"}


def _is_bare_mock(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    f = node.func
    name = f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else ""
    if name not in MOCK_NAMES:
        return False
    return not node.args and not any(k.arg in ("spec", "spec_set") for k in node.keywords)


def _bare_mocks_in(value: ast.AST) -> Iterator[ast.AST]:
    """The value itself, or either side of ``a or b`` / ``a if c else b``."""
    if _is_bare_mock(value):
        yield value
    elif isinstance(value, ast.BoolOp):
        for v in value.values:
            yield from _bare_mocks_in(v)
    elif isinstance(value, ast.IfExp):
        yield from _bare_mocks_in(value.body)
        yield from _bare_mocks_in(value.orelse)


def _target_name(t: ast.AST) -> str:
    if isinstance(t, ast.Name):
        return t.id
    if isinstance(t, ast.Attribute):
        return t.attr
    return ""


def bare_reader_mocks(source: str, filename: str) -> List[str]:
    hits: List[str] = []
    for node in ast.walk(ast.parse(source, filename)):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(_target_name(t).lower().endswith("reader") for t in targets):
                hits += [f"{filename}:{m.lineno}" for m in _bare_mocks_in(node.value)]
        elif isinstance(node, ast.Call):
            for k in node.keywords:
                if k.arg and k.arg.lower().endswith("reader"):
                    hits += [f"{filename}:{m.lineno}" for m in _bare_mocks_in(k.value)]
    return hits


def test_reader_mocks_have_a_spec() -> None:
    hits: List[str] = []
    for path in sorted(TESTS.rglob("*.py")):
        hits += bare_reader_mocks(path.read_text(), str(path.relative_to(TESTS.parent)))
    assert not hits, (
        "bare reader mocks answer to removed facade names; use tests.reader_mock.reader_mock() "
        "or pass spec=:\n  " + "\n  ".join(hits)
    )


def test_the_scan_catches_each_spelling() -> None:
    src = "\n".join([
        "reader = MagicMock()",
        "app.state.reader = mock.MagicMock()",
        "reader = reader or MagicMock()",
        "make_app(reader=Mock())",
        "ok_reader = MagicMock(spec=object)",
        "ok = make_app(reader=reader_mock())",
        "other = MagicMock()",
    ])
    assert bare_reader_mocks(src, "x.py") == ["x.py:1", "x.py:2", "x.py:3", "x.py:4"]


def test_reader_mock_refuses_a_name_the_facade_lacks() -> None:
    reader = reader_mock()
    reader.get_option_stock_links_bulk.assert_not_called()  # a real StatusReader method
    with pytest.raises(AttributeError):
        reader.create_position_category.assert_not_called()  # removed in core 0.49.0
