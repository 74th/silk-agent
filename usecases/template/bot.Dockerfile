# Bot image of this use case: the silk-agent Bot base image plus this package and its files.
# Build with ./build.sh from this directory (set SILK_AGENT_IMAGE to the base image; use a
# digest in production). The silk-agent package is already in the base image, so only this
# package is installed (--no-deps).
ARG SILK_AGENT_IMAGE=silk-agent:latest
FROM ${SILK_AGENT_IMAGE}

USER root
WORKDIR /app/usecase
COPY pyproject.toml ./
COPY src ./src
RUN uv pip install --python /src/silk-agent/.venv/bin/python --no-deps . \
    && rm -rf /tmp/uv-cache build
# Referenced by absolute path from config/silk-agent.yaml.
COPY prompts ./prompts
COPY policies ./policies
COPY skills ./skills

USER 1000:1000
WORKDIR /
# ENTRYPOINT (silk-agent slack) and SILK_AGENT_CONFIG (/config/silk-agent.yaml, from the
# ConfigMap) are inherited from the base image.
