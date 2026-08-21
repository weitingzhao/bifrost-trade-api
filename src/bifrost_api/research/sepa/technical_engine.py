"""DEPRECATED: Technical engine moved to _deprecated/.

This stub re-exports for backward compatibility when SEPA_USE_ANALYTICS=false.
"""

import importlib
import sys

_mod = importlib.import_module("bifrost_api.research.sepa._deprecated.technical_engine")
sys.modules[__name__] = _mod
