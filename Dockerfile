# silk-agent Slack bot image.
#
# gke-session-sandbox is a path dependency (../test-google-cloud-agent-platform/...), so the
# library checkout is passed as a named build context and placed at the same relative path:
#
#   docker build --build-context gke-session-sandbox=<library checkout> -t <image> .
#
# k8s/build-image.sh does this and pushes the image.
FROM python:3.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.9.26 /uv /uvx /bin/

ARG LIB_DIR=/src/test-google-cloud-agent-platform/20261010-gke-agent-sandbox-with-sandboxd
COPY --from=gke-session-sandbox pyproject.toml uv.lock README.md ${LIB_DIR}/
COPY --from=gke-session-sandbox src ${LIB_DIR}/src
COPY --from=gke-session-sandbox templates /app/sandbox-templates

WORKDIR /src/silk-agent
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
