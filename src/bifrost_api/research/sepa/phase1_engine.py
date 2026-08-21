"""DEPRECATED: Phase1 engine moved to _deprecated/.

This stub re-exports for backward compatibility when SEPA_USE_ANALYTICS=false.
"""

import importlib
import sys

_mod = importlib.import_module("bifrost_api.research.sepa._deprecated.phase1_engine")
sys.modules[__name__] = _mod
