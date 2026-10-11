# Sandbox Pod image of this use case: the silk-agent base image plus the tools the agent needs
# and startup.sh. Build with ../build.sh (set SESSION_RUNTIME_IMAGE to the base image; use a
# digest in production). The result goes into config/sandbox-config.yaml (runtime.image).
ARG SESSION_RUNTIME_IMAGE=session-runtime:v1.0.6-session-runtime-0.1.0
FROM ${SESSION_RUNTIME_IMAGE}

USER root
# Tools for the agent's commands (the base image has Python 3.12 and procps only).
RUN apt-get update \
    && apt-get install -y --no-install-recommends git jq curl \
    && rm -rf /var/lib/apt/lists/*

# Run by the supervisor on every start (create and resume), before sandboxd starts.
COPY --chmod=0755 startup.sh /opt/session-runtime/startup.sh

# Back to the unprivileged user; keep the base image's ENTRYPOINT.
USER 1000:1000
