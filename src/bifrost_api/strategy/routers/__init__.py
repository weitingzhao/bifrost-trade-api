from bifrost_api.strategy.routers.plans import router as plans_router
from bifrost_api.strategy.routers.saved_searches import router as saved_searches_router
from bifrost_api.strategy.routers.reviews import router as reviews_router
from bifrost_api.strategy.routers.strategies import router as strategies_router
from bifrost_api.strategy.routers.trades import router as trades_router
from bifrost_api.strategy.routers.gate_sets import router as gate_sets_router
from bifrost_api.strategy.routers.preferences import router as preferences_router

__all__ = [
    "gate_sets_router",
    "plans_router",
    "preferences_router",
    "reviews_router",
    "saved_searches_router",
    "strategies_router",
    "trades_router",
]
