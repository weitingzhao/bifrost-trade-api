"""DEPRECATED: Pattern indicators moved to _deprecated/.

This stub re-exports for backward compatibility when SEPA_USE_ANALYTICS=false.
"""

import importlib
import sys

_mod = importlib.import_module("bifrost_api.research.sepa._deprecated.pattern_indicators")
sys.modules[__name__] = _mod
