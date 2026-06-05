"""Shared helpers for routers (e.g. Redis URL for log streams)."""

import logging

logger = logging.getLogger(__name__)

MASSIVE_LOG_STREAM_KEY = "bifrost:massive_console"
MASSIVE_WS_LOG_STREAM_KEY = "bifrost:console:ws_massive_option"
IB_OPERATOR_LOG_STREAM_KEY = "bifrost:console:ws_ib_operator"
IB_INGESTOR_LOG_STREAM_KEY = "bifrost:console:ws_ib_ingestor"
IB_ACCOUNT_AGENT_LOG_STREAM_KEY = "bifrost:console:ws_ib_account_agent"
ACCOUNT_SYNC_DAEMON_LOG_STREAM_KEY = "bifrost:console:account_sync_daemon"


def daemon_log_redis_url() -> str:
    """Build Redis URL for daemon/server console stream from config/env. Falls back to local Redis."""
    try:
        from bifrost_core.config.startup import read_config
        from bifrost_core.core.redis_url import effective_redis_dict, format_redis_url

        config, _ = read_config()
    except Exception as e:
        logger.warning("read_config for daemon console failed: %s; using default Redis URL", e)
        config = {}
    return format_redis_url(effective_redis_dict(config, default_db=0))
