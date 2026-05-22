"""Entry point: start a single API domain service.

Usage:
    python scripts/run_server.py <domain>

Domains: monitor | massive | docs | ops | trading | strategy | portfolio | market | research
"""
import sys
import uvicorn

DOMAIN_MAP = {
    "monitor":   ("bifrost_api.monitor.app:app",   8765),
    "massive":   ("bifrost_api.massive.app:app",   8766),
    "docs":      ("bifrost_api.docs_api.app:app",  8767),
    "ops":       ("bifrost_api.ops.app:app",        8768),
    "trading":   ("bifrost_api.trading.app:app",   8769),
    "strategy":  ("bifrost_api.strategy.app:app",  8770),
    "portfolio": ("bifrost_api.portfolio.app:app", 8771),
    "market":    ("bifrost_api.market.app:app",    8772),
    "research":  ("bifrost_api.research.app:app",  8773),
}


def main():
    domain = sys.argv[1] if len(sys.argv) > 1 else "monitor"
    if domain not in DOMAIN_MAP:
        print(f"Unknown domain: {domain}. Choose from: {list(DOMAIN_MAP)}")
        sys.exit(1)
    app_str, port = DOMAIN_MAP[domain]
    uvicorn.run(app_str, host="0.0.0.0", port=port, reload=False)


if __name__ == "__main__":
    main()
