FROM python:3.11-slim

WORKDIR /app

ARG BIFROST_CORE_REF=main
ARG BIFROST_WORKER_REF=main
ARG BIFROST_SOCKET_REF=main
ARG GITHUB_ORG=YOUR_ORG
ARG API_DOMAIN=monitor
ARG API_PORT=8765

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    "bifrost-core @ git+https://github.com/${GITHUB_ORG}/bifrost-trade-core.git@${BIFROST_CORE_REF}" \
    "bifrost-worker @ git+https://github.com/${GITHUB_ORG}/bifrost-trade-worker.git@${BIFROST_WORKER_REF}" \
    "bifrost-socket @ git+https://github.com/${GITHUB_ORG}/bifrost-trade-socket.git@${BIFROST_SOCKET_REF}"

COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir -e "."

COPY scripts/ scripts/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    API_DOMAIN=${API_DOMAIN} \
    API_PORT=${API_PORT}

EXPOSE ${API_PORT}

CMD ["python", "scripts/run_server.py"]
