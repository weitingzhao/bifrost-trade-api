"""A StatusReader stand-in that only answers to StatusReader's names (TD-154).

A bare ``MagicMock()`` answers to every attribute, so ``reader.removed_method.assert_not_called()``
passes whatever the route does -- core 0.49.0 removed the facade writes and such an assertion
went on passing for a method that no longer existed. ``reader_mock()`` is
``create_autospec(StatusReader, instance=True)``: reading or asserting on a name the facade
does not have raises ``AttributeError``, and a call with the wrong arguments raises ``TypeError``.
Setting attributes (``reader.config = ...``) still works.

``tests/test_reader_mocks_have_spec.py`` keeps bare reader mocks out of the suite.
"""

from __future__ import annotations

from unittest.mock import MagicMock, create_autospec

from bifrost_core.monitor.reader.common import StatusReader


def reader_mock() -> MagicMock:
    return create_autospec(StatusReader, instance=True)
