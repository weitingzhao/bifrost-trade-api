FROM python:3.11-slim

WORKDIR /app

ARG BIFROST_CORE_REF=main
ARG GITHUB_ORG=YOUR_ORG
# Which process this image runs: monitor | account | market | research
# (aliases: docs, ops -> monitor; trading, strategy, portfolio -> account).
# Deployments set API_DOMAIN at runtime too (bifrost-trade-infra k8s/base/apis/manifest.yaml).
ARG API_DOMAIN=monitor

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

# bifrost-trade-socket is archived and nothing here imports it (Wave 14G-F).
RUN pip install --no-cache-dir \
    "bifrost-core @ git+https://github.com/${GITHUB_ORG}/bifrost-trade-core.git@${BIFROST_CORE_REF}"

COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e "."

COPY scripts/ scripts/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    API_DOMAIN=${API_DOMAIN}

# The port comes from config server.<domain>_port: monitor 8765, account 8769, market 8772, research 8773.
EXPOSE 8765 8769 8772 8773

CMD ["sh", "-c", "exec python scripts/run_server.py \"${API_DOMAIN}\""]
