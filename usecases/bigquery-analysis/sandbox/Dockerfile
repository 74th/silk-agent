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

# The fixed analysis script uses ADC through the official BigQuery Python client.
RUN python -m pip install --no-cache-dir google-cloud-bigquery==3.46.1

# The Bot loads the Skill from its own image, while the fixed query runs inside the Sandbox.
# Include the script and SQL at the path referenced by SKILL.md.
RUN mkdir -p /app/usecase/skills/google-trends/scripts \
    && chown -R 1000:1000 /app/usecase
COPY --chown=1000:1000 skills/google-trends/scripts/ /app/usecase/skills/google-trends/scripts/

# Run by the supervisor on every start (create and resume), before sandboxd starts.
COPY --chmod=0755 sandbox/startup.sh /opt/session-runtime/startup.sh

# Back to the unprivileged user; keep the base image's ENTRYPOINT.
USER 1000:1000
