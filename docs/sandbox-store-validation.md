# Sandbox distributed-store validation

Validated locally on 2026-10-14 for `distribute-agent-workers` tasks 4.1–4.4.

The Manager now accepts `SessionRepository`. Its local factory constructs
SQLite, while a caller can inject `FirestoreSessionRepository`. Firestore
stores the SessionRecord without dropping resource UIDs, commands, notices,
environment fingerprints, or lifecycle timestamps. Every mutation reads the
resource lease in the same transaction as the record. Reacquiring even under
the same holder identity advances the epoch, so an old token cannot revive.

File operations have independent IDs, executor identities, generations,
deadlines, and states. Their creation/completion and the aggregate activity
count commit together. Duplicate completion does not decrement twice.
A restart or an advisory deadline does not prove that remote work stopped.
Unconfirmed operations remain counted, and a cancelled request is recorded
as `unknown`. The executor must confirm completion before the count is
removed. Startup no longer clears another process's file activity, including
legacy counters that have no operation ID.

## Evidence

- The full local regression run passed: **570 tests**, 28 deselected by the
  existing integration/GKE markers. It ran before concurrent Service edits
  were complete.
- The Firestore emulator suites passed: **15 tests** across
  `test_firestore_inbox_emulator.py` and
  `test_firestore_repository_emulator.py`. Tests use an explicit local host;
  they do not connect to a production database.
- Sandbox source, Sandbox tests, and the repository emulator suite passed
  Ruff lint and format checks.
- Pyrefly passed after the Repository/file-operation changes. A subsequent
  whole-project check encountered two errors in the concurrently added
  `sandbox/service.py`; those edits are outside this verification result.
- The whole-project format check also reported a pre-existing formatting
  difference in the BigQuery `google-trends/scripts/query.py` file.

The emulator tests cover SQLite-to-Firestore round trips, CAS conflicts,
expired/reacquired lease fencing, same-holder reacquisition, oversized-record
rejection, and competing Managers during ensure and suspend. Explicit
barriers hold backend operations while the second Manager attempts them.
They also cover another Manager's startup during file activity and retention
of an unknown file operation after request cancellation.

One wider Sandbox run exposed the existing subprocess ordering assumption in
`test_without_a_script_the_children_just_start` (`tracker` versus `sandboxd`).
The focused supervisor rerun and the full regression run both passed; no
supervisor code or test expectation was changed.

## Reproduce

Start a local Firestore emulator, then run:

```sh
SILK_AGENT_FIRESTORE_EMULATOR_HOST=127.0.0.1:8787 uv run pytest -q -m integration \
  tests/integration/test_firestore_inbox_emulator.py \
  tests/integration/test_firestore_repository_emulator.py
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run pyrefly check
```

No deployment or replica increase was performed in this validation.

## Creation-recovery follow-up

A `creating` record currently contains a conversation hash and an environment
fingerprint. These values alone cannot reconstruct the original SessionKey
or environment for an independent reconciler after restart. The existing
ensure path can resume when it receives that context again. Selecting how
the independent reconciler obtains the key and environment references remains
a design decision; a fingerprint must not be treated as the environment.
