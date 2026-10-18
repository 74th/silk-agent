# Sandbox Pod base image: sandboxd + the Python command tracker (supervisor).
# Built by scripts/build-session-runtime-image.sh from the agent-sandbox source tree.
#
# A use case extends it (see usecases/template/sandbox/Dockerfile):
#
#   FROM <this image>@sha256:...
#   USER root
#   RUN apt-get update && apt-get install -y --no-install-recommends git \
#       && rm -rf /var/lib/apt/lists/*
#   COPY startup.sh /opt/session-runtime/startup.sh   # run before sandboxd on every start
#   USER 1000:1000
#
# Keep the ENTRYPOINT and do not remove /opt/runtime, /usr/local/bin/sandboxd or the
# `gke_session_sandbox` Python package. Commands run through `bash -lc`, so do not unset
# environment variables in /etc/profile or /etc/profile.d (AGENT_USER_EMAIL is a container
# environment variable and must reach the agent's commands).
FROM --platform=$BUILDPLATFORM golang:1.27.1 AS builder
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY packages/sandboxd/ ./packages/sandboxd/
COPY internal/ ./internal/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" \
      -o /sandboxd ./packages/sandboxd/cmd/sandboxd

FROM python:3.12-slim-bookworm
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates procps \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1000 --no-create-home runtime \
    && mkdir -p /workspace/.session-runtime /opt/runtime /opt/session-runtime \
    && chown -R 1000:1000 /workspace
COPY --from=builder /sandboxd /usr/local/bin/sandboxd
COPY runtime/python/gke_session_sandbox /opt/runtime/gke_session_sandbox
# Versions come from silk-agent's uv.lock (the generated sandboxd code was built for them).
COPY runtime/constraints.txt /tmp/constraints.txt
RUN pip install --no-cache-dir -c /tmp/constraints.txt aiohttp grpcio protobuf \
    && rm /tmp/constraints.txt
# The runtime user has no home directory; startup.sh writes git settings to /workspace/.gitconfig.
ENV PYTHONPATH=/opt/runtime PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    GIT_CONFIG_GLOBAL=/workspace/.gitconfig
WORKDIR /workspace
USER 1000:1000
EXPOSE 8080 8081 9090
ENTRYPOINT ["python3", "-m", "gke_session_sandbox.supervisor"]
