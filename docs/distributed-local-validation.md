# Distributed mode local validation

These checks run without Slack credentials, a live SDK, a GKE cluster, or production
Firestore. The Firestore tests pass an explicit emulator host to the client factory;
they do not infer an emulator endpoint from ambient ADC settings.

## Start the local Firestore emulator

In terminal 1:

```sh
gcloud emulators firestore start \
  --host=127.0.0.1 \
  --port=8787 \
  --database-mode=firestore-native \
  --database-edition=standard
```

Leave it running while the tests execute. Stop it with Ctrl+C afterward.

## Run the distributed persistence tests

In terminal 2, from the repository root:

```sh
SILK_AGENT_FIRESTORE_EMULATOR_HOST=127.0.0.1:8787 uv run pytest -q -m integration \
  tests/integration/test_firestore_inbox_emulator.py \
  tests/integration/test_firestore_repository_emulator.py
```

This includes duplicate Gateway event receipts, concurrent allocator decisions,
ownership fencing, recovery, command monitor claims, and competing file/Sandbox
repository operations. The direct local ingress test uses two Gateway handlers,
fake ACK callbacks, two allocators, and two Worker actor factories against one
emulator. A second conversation is assigned to the other Worker, so the test checks
per-conversation single ownership and cross-conversation distribution. It then races
two Monitor loops for one command claim, verifies one terminal completion input, and
breaks/reconnects the watch before confirming a later durable input is recovered.
The actors, ACK callbacks, command status reader, and watch transport are fakes; no
Slack, live SDK, or Sandbox command is started.

The STOP barrier emulator test accepts A, an authorized STOP control, and B in one
conversation. It verifies the control advances `stop_seq`, a late dispatch attempt
for A is fenced, the paged cancellation marks A terminal, and B remains pending.
This validates the durable barrier and inbox behavior; a separate emulator test holds
A's guard, processes the STOP control concurrently, then releases the guard and
verifies A's late dispatch is rejected while B dispatches. Another test composes
Firestore, `DurableInputDelivery`, and `AgentSession` with a fake engine across
append/interrupt/queue modes, and checks completion states and one engine per session.
These tests do not exercise a live SDK, Sandbox command, or stop during Sandbox ensure
execution. The reset emulator case validates generation advance and retention of
post-reset input, and the running-tool reset case checks that reset waits for the fake
turn to stop.
Another emulator test connects `DurableInputDelivery` to Firestore and verifies an
append refusal restores the same input to pending before a later append succeeds
with a new dispatch attempt ID.

The global drain emulator case keeps an accepted input pending while allocation is
disabled, then releases the global barrier and verifies that the same input remains
available after ownership is allocated again.

The completion-after-STOP emulator case creates a terminal background command and its
completion input first, then authorizes STOP. It verifies the completion is cancelled,
the command notification becomes suppressed, and completion delivery never calls the
session.

Background delivery is covered without waiting 20 wall-clock seconds: the tool test
asserts that the default foreground window passed to `BackgroundCommands.wait` is
exactly 20 seconds, the explicit `run_in_background` test returns immediately, and a
short injected foreground window verifies that a still-running command detaches and
later notifies. The command monitor state tests cover completion-before-detach and
detach-before-completion, and ensure one delivery path wins. Conversation tests verify
completion starts a turn while idle and appends to a running turn. A released owner's
completion remains pending when the stale token is rejected, then the replacement
Worker delivers the same input. Monitor lease takeover and the emulator harness verify
restart recovery and one durable completion input. These are fake-backed/local checks;
they do not run a live Sandbox command or Slack delivery.

Failure-boundary checks also cover ACK only after Inbox persistence, leaving failed
events unacknowledged for receipt-deduplicated redelivery, dispatching input recovery
as uncertain, idempotent lookup after a lost Sandbox start response, and an Outbox
post accepted by Slack whose response is lost becoming uncertain without retry. A
Worker that cannot renew ownership closes its local Actor and does not release the
lease as if shutdown had been confirmed.

Drain and crash recovery use explicit fakes and emulator state rather than killing a
process: a Worker releases assignments only after its actor reports stopped; expired
ownership stays blocked when stop evidence times out; after matching SDK-closed
evidence, the same background `command_id` and Sandbox generation remain running, and
the old owner token cannot dispatch. This covers the SIGKILL outcome at the recovery
boundary, while an actual OS-level SIGKILL run remains unperformed.

## Run all local unit and fake-backed tests

```sh
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run pyrefly check
```

The default pytest config excludes tests marked `integration` or `gke`. The
Firestore emulator suite above is the local integration subset; live Agent Platform,
Model Armor, and GKE acceptance tests need their own explicit environment and are
not implied by a passing local run.

## Direct Sandbox Service API tests

The in-process HTTP API can be exercised without Slack or a cluster:

```sh
uv run pytest -q tests/test_sandbox_service.py tests/test_sandbox_remote.py
```

These tests use aiohttp test clients and fake TokenReview/manager implementations.
They validate request authentication, session and command request handling, and the
remote adapter contract. The deployed Service smoke check is separately recorded in
[`distribution-plan.md`](distribution-plan.md).

## Coverage boundary

The local tests compose ingress, Firestore deduplication, allocation, Worker
activation with a fake actor, watch reconnection, and Monitor claim/completion with a
fake status reader. They also compose the real `AgentSession` and durable delivery
state with a fake engine for append/interrupt/queue behavior, append-window rejection,
STOP during tool wait, and reset while a tool is waiting. A separate session test
stops while the sandbox preparation callback is blocked and verifies no engine query
starts afterward. These tests do not use a live SDK, Sandbox command execution, or
real Socket Mode transport. STOP during a live Sandbox ensure and reset against a live
SDK turn remain open, along with drain and process-kill scenarios in OpenSpec task 14.5.
