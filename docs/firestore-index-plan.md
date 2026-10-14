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

`inputs.expire_at` is eligible for Firestore TTL only after a record reaches
`completed`, `blocked`, `cancelled`, or `suppressed`. The default retention is
30 days from its terminal timestamp and the helper allows configuration. No
TTL value is written for `pending`, `dispatching`, `queued`, `accepted`, or `uncertain`.
In particular, uncertain delivery remains available for operator recovery.
The TTL field itself should have its single-field index disabled except for
the TTL policy.

Slack receipt documents can use a separate configurable expiry because they
are deduplication markers; their retention must cover the supported replay
window. Do not TTL-delete the conversation parent as a way to remove its
children. Firestore TTL and deleting a parent document do not recursively
remove subcollection documents. A future purge operation must page through
and delete terminal `inputs` and `controls` documents first, then receipts and
other children, and delete the parent last. It must keep pending and uncertain
records. Batch work must be checkpointed so it can resume after interruption.

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
