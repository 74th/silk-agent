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
    && mkdir -p /workspace/.session-runtime /opt/runtime \
    && chown -R 1000:1000 /workspace
COPY --from=builder /sandboxd /usr/local/bin/sandboxd
COPY runtime/python/gke_session_sandbox /opt/runtime/gke_session_sandbox
RUN pip install --no-cache-dir 'aiohttp>=3.14,<4' 'grpcio>=1.84,<2' 'protobuf>=7.35.1,<8'
ENV PYTHONPATH=/opt/runtime PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /workspace
USER 1000:1000
EXPOSE 8080 8081 9090
ENTRYPOINT ["python3", "-m", "gke_session_sandbox.supervisor"]
