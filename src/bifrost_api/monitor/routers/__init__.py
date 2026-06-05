"""Domain-based API routers for the monitor FastAPI app (core 5)."""

from bifrost_api.monitor.routers.core import router as core_router
from bifrost_api.monitor.routers.logs import router as logs_router
from bifrost_api.monitor.routers.messages import router as messages_router
from bifrost_api.monitor.routers.status import router as status_router
from bifrost_api.monitor.routers.daemon import router as daemon_router
from bifrost_api.monitor.routers.config import router as config_router

__all__ = [
    "core_router",
    "logs_router",
    "messages_router",
    "status_router",
    "daemon_router",
    "config_router",
]
