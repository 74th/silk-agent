#!/bin/sh
# Runs before sandboxd on every start of the Sandbox (create and resume), as the container's
# user, in /workspace, with the container's environment:
#   AGENT_USER_EMAIL           e-mail of the Slack user who started the conversation
#   SANDBOX_STARTUP_FIRST_RUN  1 on the first start of the Sandbox, 0 on later starts (resume)
# It runs again on every resume: write it so that running it twice is harmless.
# Exit non-zero to fail the start (the Pod restarts); see SANDBOX_STARTUP_ON_ERROR.
# Stdout / stderr go to the container log. Outbound traffic is limited to
# `network.allowed_fqdns` in config/sandbox-config.yaml.
set -eu

echo "startup: user=${AGENT_USER_EMAIL:-unknown} first_run=${SANDBOX_STARTUP_FIRST_RUN:-?}"

# Every start (idempotent): commits made by the agent are attributed to the conversation's user.
if [ -n "${AGENT_USER_EMAIL:-}" ]; then
  git config --file /workspace/.gitconfig user.email "${AGENT_USER_EMAIL}"
  git config --file /workspace/.gitconfig user.name "${AGENT_USER_EMAIL%@*}"
fi

# First start only: fetch what the agent needs in /workspace.
if [ "${SANDBOX_STARTUP_FIRST_RUN:-0}" = "1" ]; then
  mkdir -p /workspace/data
  # curl -fsSL https://example.com/data.csv -o /workspace/data/data.csv   # add the host to allowed_fqdns
  echo "startup: prepared /workspace/data"
fi
