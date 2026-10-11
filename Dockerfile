# silk-agent Slack bot base image (k8s/build-image.sh builds and pushes it).
# It holds no use-case files (prompts, policies, MCP settings, Skills): a use case extends it with
# `FROM` + `uv pip install <its package>` (see usecases/template/bot.Dockerfile). uv stays in the
# image for that.
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.9.26 /uv /uvx /bin/

WORKDIR /src/silk-agent
COPY templates /app/sandbox-templates
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY apps ./apps
ENV UV_CACHE_DIR=/tmp/uv-cache UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
RUN uv sync --frozen --no-dev --no-editable && rm -rf /tmp/uv-cache

RUN useradd --uid 1000 --create-home --home-dir /home/app app
ENV PATH=/src/silk-agent/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    HOME=/home/app \
    SILK_AGENT_CONFIG=/config/silk-agent.yaml \
    SILK_AGENT_WORKDIR=/work
USER 1000:1000
ENTRYPOINT ["silk-agent", "slack"]
