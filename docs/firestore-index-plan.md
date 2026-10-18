# Firestore query and retention plan

This is the index contract for the distributed store. The Terraform resources
in task 12.4 should materialize these declarations after the target database
and edition are confirmed. No index or TTL policy is deployed by this document.

## Query to index mapping

| Store operation | Query | Required index / field policy |
| --- | --- | --- |
| `FirestoreInboxStore.list_unfinished` | A conversation's `inputs` where `seq > after_seq` and `delivery_state in (pending, dispatching, queued, accepted)`, ordered by `seq` | Collection-scope composite index: `delivery_state ASC`, then `seq ASC`. The document name uses Firestore's default final index component. |
| `FirestoreInboxStore` record reads and writes | Deterministic document ID lookups for input, control, conversation and receipt records | No composite index. |
| `FirestoreConversationStore` record reads and writes | Deterministic conversation and ownership document lookups | No composite index. |
| `FirestoreServiceOperationStore.list_for_stop` | Accepted operations for one conversation and generation, where `origin_seq <= stop_seq`, ordered by `origin_seq` | Collection-scope composite index: `conversation_key ASC`, `conversation_generation ASC`, `state ASC`, `origin_seq ASC`; Firestore adds document name as the final component. |
| `FirestoreCommandMonitorStore.list_due` | Nonterminal commands where `state in (intent, running, cancel_requested)` and `next_attempt_at <= now`, ordered by `next_attempt_at` | Collection-scope composite index: `state ASC`, then `next_attempt_at ASC`; Firestore adds document name as the final component. |
| `FirestoreCommandMonitorStore.list_for_stop` | One conversation and generation, active command states, and `origin_seq <= stop_seq`, ordered by `origin_seq` | Collection-scope composite index: `conversation_key ASC`, `conversation_generation ASC`, `state ASC`, `origin_seq ASC`; Firestore adds document name as the final component. |
| `FirestoreOutboxStore.claim_next` | One conversation's pending/dispatching/uncertain output records, ordered by `sequence` | Collection-scope composite index: `conversation_key ASC`, `status ASC`, `sequence ASC`; Firestore adds document name as the final component. |
| `FirestoreInboxStore.list_authorized_controls` | One conversation's `controls` where `authorization_state == authorized` (sorted by `seq` in memory; a conversation has few open controls) | Single-field equality; no composite index. |
| `FirestoreOutboxStore.list_pending_page` (`list_conversations_with_pending` is its first page) | `outbox` where `status in (pending, dispatching)`, ordered by document name, `start_after` the previous page's last document, bounded by `limit` | Single-field `in` ordered by document name; no composite index. The Sender keeps the cursor and starts over at the end, so a conversation with a stuck head output cannot fill every page. |
| `FirestoreOwnershipStore.list_expired` | `ownership` where `phase in (assigned, active, recovering)`; lease expiry is filtered in memory. Released documents are never read, and the set is bounded by Worker capacity | Single-field `in`; no composite index. |
| `FirestoreQuiesceMailbox.list_requested` | `quiesce_requests` where `worker_incarnation == X` and `state == requested` | Equality-only on two fields; served by single-field index merging. |
| `FirestoreInboxStore.list_sender_enrichment_due` | Collection-group `inputs` where `next_sender_lookup_at <= now`; pending state is checked in the bounded result page | Firestore's collection-group single-field index on `next_sender_lookup_at`. |

The query is deliberately scoped to one conversation's `inputs` subcollection.
It does not use a collection-group scan. New queries must be added to this
table alongside their index before they are enabled.

## Field index exemptions

Apply single-field exemptions by collection group so nested records inherit
the policy consistently:

| Collection group | Field | Policy |
| --- | --- | --- |
| `inputs` | `payload` | Disable ascending, descending, and array indexing. |
| `controls` | `payload` | Disable ascending, descending, and array indexing. |

Payloads can contain large bodies and are never filter or ordering fields.
Extend the exemption list when command, outbox, notice, or sandbox payload
fields are introduced; do not exempt fields used by a listed query.

## TTL and child document cleanup

Processed records carry `expire_at` (a Firestore Timestamp, written by the same transaction
that ends the record; `stores/firestore/retention.py` decides which states qualify). The field
is `expire_at` everywhere. `expires_at` means an ownership, lease or Sandbox retention deadline
and never has a TTL policy.

| Collection | `expire_at` is written when | Counted from | Retention |
|---|---|---|---|
| `inputs` (group) | `completed`, `blocked`, `cancelled`, `suppressed` | that time (`completed_at` is written too) | `terminal_days` (30) |
| `controls` (group), `control_queue` | `handled`, `filtered`, `stale` | that time | `terminal_days` |
| `outbox` | `delivered`, `suppressed`, `failed` | that time | `terminal_days` |
| `commands`, `sandbox_operations` | terminal command whose notice is `none`, `delivered` or `suppressed`; the operation gets the same value in the same transaction | the later of the end and the notice | `terminal_days` |
| `quiesce_requests` | `answered` | answer time | `terminal_days` |
| `slack_receipts` | created (`received_at` is written too) | reception | `receipt_days` (7) |
| `workers` | register and every heartbeat | last heartbeat | `worker_days` (7) |

No value is written for `pending`, `dispatching`, `queued`, `accepted`, `held` or `uncertain`
inputs, `pending` / `dispatching` / `uncertain` outputs, running commands, or commands whose
notice is `pending` or `held`; a transition back out of a terminal-looking state removes it. In
particular, uncertain delivery remains available for operator recovery. The TTL field has its
single-field index disabled (`index_config {}`). `workers/*/incarnations/*` is only the parent
path of `reservations` documents, so it has no TTL.

A command's notice becomes `delivered` when its `command_finished` input completes, or
`suppressed` when that input is cancelled, blocked or suppressed (a STOP, a reset).

Existing records are given the same `expire_at` by `silk-agent retention backfill`
(`--apply` writes; the default only counts). See the operations runbook, section 11.

Conversation documents and their ownership, Sandbox and lease records are not deleted by TTL.
Do not TTL-delete the conversation parent as a way to remove its children: Firestore TTL and
deleting a parent document do not recursively remove subcollection documents. A future purge
operation must page through and delete terminal `inputs` and `controls` documents first, then
receipts and other children, and delete the parent last. It must keep pending and uncertain
records. Batch work must be checkpointed so it can resume after interruption.

### Reads of TTL-eligible collections (audit, 2026-10-18, `add-data-retention` 1.2)

Every get / query of a collection that gets `expire_at` was checked for a read that
needs a terminal record. None does; the reads that can see a deleted record degrade safely.

| Collection | Reads | Terminal record needed? |
|---|---|---|
| `inputs` | `list_unfinished`, `list_held`, `has_waiting_input`, ownership recovery queries: only non-terminal states. `advance_input` / `record_query_start` / `recover_dispatching`: by ID, expect a non-terminal state. `accept_*`: by ID for idempotent redelivery. `list_sender_enrichment_due`: `next_sender_lookup_at`, pending only | No. A redelivery after the record was deleted needs the Slack receipt (7 days) to be gone too, so it creates a new input only after the replay window |
| `controls`, `control_queue` | `list_pending_controls`, `list_authorized_controls`, `list_sender_enrichment_due`: pending / authorized only | No |
| `outbox` | `list_pending_page`, `claim_next`: pending / dispatching / uncertain only (`failed` is in neither). `mark_delivery` and `revalidate_claim`: by ID on a dispatching record | No |
| `commands` | `list_due`, `list_for_stop`, `claim`: non-terminal only. `get_command` (Monitor, `BackgroundCommands.status`, `command_output`): by command ID | A status or output request for a command older than the retention returns `unknown` (the archived output in GCS has the same 30-day retention) |
| `sandbox_operations` | `list_for_stop`: `accepted` only, and `is_dispatch_allowed` (new operations). `get_operation` (Sandbox Service): idempotency check by request ID, and command status / cancel / output | A request ID is unique per turn; a status request for an operation older than the retention returns 404 `unknown_command` |
| `quiesce_requests` | `list_requested`: `requested` only. `get`: polled by the requester until `answered` (seconds) | No |
| `slack_receipts` | `accept_received`: by ID, inside the receive transaction | The marker must outlive the Slack replay window (hours); 7 days |
| `workers` | `list_available`, `list_workers`, `get_worker` (also by the Control recovery loop, once per blocked ownership, to see whether a newer incarnation replaced the old one), CLI `status-all`: filtered by `heartbeat_at` or listed | A Worker that stopped heartbeating more than 7 days ago disappears from `status-all` |

`workers/*/incarnations/*` is not a document: it exists only as the parent path of
`reservations` documents, so it gets no TTL. Reservations left by a crashed Worker are
outside the retention (released by recovery).

Terraform materializes these index, exemption, and TTL declarations. The
release record below captures the first live apply and verification.

## Terraform release record

On 2026-10-14, Terraform created the dedicated `silk-agent` Standard Native
database in `nnyn-dev/us-central1`, enabled the Firestore API, configured the
unfinished-input composite index, payload index exemptions, and `expire_at`
TTL policy. Database deletion protection is enabled and Terraform uses the
`ABANDON` deletion policy. The named database is isolated from the existing
`(default)` and `claude-agent-chat` databases; named databases do not qualify
for the Firestore free tier, so usage is billed.

`firestore_database_mode` supports:

- `create`: create the ID in `firestore_database_id` (the default release uses
  `silk-agent`).
- `use-existing`: skip database management and apply indexes/field policies to
  the configured existing database ID.
- `import-existing`: import the existing database into
  `google_firestore_database.silk_agent[0]` before planning, using
  `terraform -chdir=terraform import 'google_firestore_database.silk_agent[0]'
  'projects/<project-id>/databases/<database-id>'`. Review the import plan for
  location, edition, and delete protection before applying.

The current GKE Workload Identity principal
`session-sandbox-system/silk-agent` has `roles/datastore.user` through
Terraform. A one-off Job using the built image digest
`sha256:14e7e2a0b1fef8fcb6a025804b6d73425c620a69fc26fa54d6c415496f857ec8`
completed a live database test: two concurrent input accepts received ordered
sequences, `list_unfinished` returned both records, and a capacity-one worker
reservation/ownership transaction succeeded. The Job, inbox/receipt documents,
worker documents, reservation, and control lease were removed after the test.
The image remains in Artifact Registry as
`silk-agent:distributed-smoke-20261014` for reproducibility.

This release validates the distributed Firestore data path from GKE. It does
not switch the existing Slack deployment to distributed roles; the remaining
OpenSpec runtime and Kubernetes tasks must be completed before that rollout.

On 2026-10-14, a follow-up Terraform apply added the `silk-agent_commands`
composite index for `state` and `next_attempt_at`, plus the
`nnyn-dev-silk-agent-command-output` bucket with 30-day object retention and
`roles/storage.objectAdmin` for the `session-sandbox-system/silk-agent` GKE
Workload Identity principal. The apply completed with 3 additions, 0 changes,
and 0 destructions. The index reached `READY` and a subsequent Terraform plan
reported no changes.

A live GCS smoke test uploaded a temporary command output object, read it in
two cursor pages (0→12→35 bytes), and deleted the object. This verifies the
bucket and worker IAM path; it does not yet exercise the full distributed
command lifecycle from a running Worker.

On 2026-10-14, the stop-barrier command query index for
`conversation_key`, `conversation_generation`, `state`, and `origin_seq` was
created in `nnyn-dev` / `silk-agent` and reached `READY`. The provider remained
in its create wait after GCP reported `READY`; the resource was imported into
Terraform state and a subsequent plan reported no changes, with no other
resource changes.

On 2026-10-14, the Sender ordering index for `silk-agent_outbox` fields
`conversation_key`, `status`, and `sequence` reached `READY`. Terraform's
provider remained in its create wait; the READY index was imported into state
and a follow-up plan reported no changes, with no other resource changes.
