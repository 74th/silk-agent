# silk-agent Slack bot image (k8s/build-image.sh builds and pushes it).
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.9.26 /uv /uvx /bin/

WORKDIR /src/silk-agent
COPY templates /app/sandbox-templates
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY apps ./apps
COPY policies ./policies
COPY config/mcp.json ./config/mcp.json
ENV UV_CACHE_DIR=/tmp/uv-cache UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
RUN uv sync --frozen --no-dev --no-editable && rm -rf /tmp/uv-cache

RUN useradd --uid 1000 --create-home --home-dir /home/app app
ENV PATH=/src/silk-agent/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    HOME=/home/app \
    SILK_AGENT_CONFIG=/config/silk-agent.yaml \
    SILK_AGENT_WORKDIR=/work
USER 1000:1000
ENTRYPOINT ["python", "apps/slack_bot.py"]
