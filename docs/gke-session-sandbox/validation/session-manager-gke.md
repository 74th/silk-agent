# Session manager GKE validation

Validated on 2026-10-10 against the dedicated cluster recorded in
[`gke-cluster.md`](gke-cluster.md).

| Property | Result |
| --- | --- |
| Project / context | `nnyn-dev` / `gke_nnyn-dev_us-central1_research-gke-agent-sandbox-with-sandboxd` |
| Cluster / region | `research-gke-agent-sandbox-with-sandboxd` / `us-central1` |
| Runtime image | `session-runtime@sha256:11eaa6651ef23b9a7e19a1138277a8e6b30b2400df461530a8016e043bc1baf3` |
| Validation run | `5d0a4542fc74` |
| Temporary namespace | `session-sandboxes-5d0a4542fc74` (deleted after the run) |
| Primary Sandbox UID / generation | `ac05948b-4e5f-452b-be2d-05fd7ab8ff78` / `1` |

The run used a unique namespace labeled with its run ID and a unique tenant/session key. The script checked the active gcloud project and exact kubectl context before creating resources. It removed its temporary bypass Job, terminated the owned session, and deleted the namespace only after checking the namespace UID. After completion, no validation namespaces or Sandboxes remained. The shared `sandboxd-smoke` fixture was not changed.

The run verified concurrent `ensure` deduplication; gVisor runtime readiness and network probes; tracker health, command ID deduplication, output and cursor behavior, stdin EOF, PTY resize, signal, timeout, cancellation, descendant cleanup, and gate behavior; tracker restart recovery; the Python client command path; policy denial of direct tracker gRPC access; suspend/resume; and reconnecting through a fresh manager after `close()` without deleting the remote session. A one-turn Agent fixture remained Ready while its tracked command was active. A separate run (`cf83742286cc`) measured production idle behavior: command finish at `14:53:35.118 UTC`, with suspend completed at `14:58:35.497 UTC`, about 300.38 seconds later.

The same namespace also created a disposable RWO claim with `session-workspace-rwo`. A writer Pod mounted it at `/var/lib/session-sandbox`, verified the mount, and wrote a SQLite record. After that Pod was deleted, a reader Pod mounted the same claim and read the record successfully (`sqlite-pvc-write-ok`, `sqlite-pvc-reopen-ok`). The claim and its Delete-reclaim PV were removed with the namespace.

The run also created a second session, wrote a marker into its workspace, and deleted only that run-owned Sandbox with a UID precondition. After controller cleanup removed its owned PVC and PV, the reconciler recorded `workspace_lost`; `ensure` created generation 2 with a new PVC and the marker was absent. The event remained pending across manager close/reconnect, then was acknowledged exactly once. A disposable retention probe used `retention_seconds=60`: after suspension at `22:09:45.438450 UTC`, the test waited until its expiry at `22:10:45.438450 UTC` and observed at `22:10:45.473304 UTC` (60.034854 seconds later). `ensure` then removed the expired workspace and created generation 2 with a `workspace_cleared` event and an empty workspace. A prior 10-second run (`fddcf0db81d4`) also passed. The 10-second run exposed and led to fixing a race where the controller could delete the Sandbox immediately before the manager's DELETE call. Explicit termination also returned a `workspace_cleared` event. The new generations and primary session were terminated before namespace deletion.

For this integration harness, the manager ran locally and used a temporary local SQLite database (`require_mount=False`) while calling the real GKE API. The separate writer/reader Pods validate an RWO GKE disk mount and SQLite data recovery across Pod replacement, but use `session-workspace-rwo` (Delete reclaim) and Python's standard SQLite module. They do **not** validate the production API Deployment mounting its `session-manager-state` claim with `session-manager-state-rwo` (Retain reclaim), or restarting that API process with the configured `SQLiteSessionRepository`. Repository reopen/CAS and mount-required behavior are covered by unit tests; deployment restart on the production claim remains an acceptance item.

The validation runner exposes idle, retention, reconcile interval, and one-turn duration as command-line parameters. The main session uses a separate `retention_seconds=3600`; only the disposable retention probe uses the requested test value. Real 10- and 60-second retention intervals passed on GKE. The production 604800-second (seven-day) interval remains covered by clock-injected unit tests, not a seven-day wall-clock run. Worker shutdown while a suspended Sandbox awaits controller expiry is also not covered by this run.

Run it with the dedicated cluster explicitly selected:

```sh
GKE_PROJECT_ID=nnyn-dev \
GKE_LOCATION=us-central1 \
GKE_CLUSTER_NAME=research-gke-agent-sandbox-with-sandboxd \
uv run python -u scripts/validate-session-manager-gke.py --timeout 1200 --idle-timeout-seconds 5 --retention-seconds 60
```

To measure the full 300-second idle boundary, allow extra time for the one-turn runner and subsequent idle wait:

```sh
GKE_PROJECT_ID=nnyn-dev \
GKE_LOCATION=us-central1 \
GKE_CLUSTER_NAME=research-gke-agent-sandbox-with-sandboxd \
uv run python -u scripts/validate-session-manager-gke.py --timeout 1200 --idle-timeout-seconds 300 --retention-seconds 10
```

The script creates a fresh `session-sandboxes-<run-id>` namespace and deletes it during cleanup. Do not run `scripts/validate-sandbox-lifecycle-gke.sh` as part of this test: that separate script operates on and deletes the shared `sandboxd-smoke` fixture.

On failure, the runner writes `failure.txt`, namespace and resource YAML, Pod JSON, ConfigMap identifying metadata, and current/previous container logs under `<artifact-dir>/<run-id>` before cleanup deletes the namespace. ConfigMap contents are omitted because they can contain credentials. To verify this failure path on the dedicated cluster, pass `--verify-failure-artifacts`; the runner creates a labeled probe ConfigMap, intentionally exits with an error, saves the artifacts, and deletes the run namespace using its UID precondition. This mode is expected to exit nonzero.

On 2026-10-11, `--verify-failure-artifacts` exited as expected for run `3ef736392e95`. `configmaps.txt` recorded the probe name and UID without its data, and `failure.txt` recorded the intentional exception and namespace UID; the namespace was absent after cleanup. An invocation with `GKE_CLUSTER_NAME=deliberately-wrong` was rejected before namespace creation because the active context did not match.

The cluster-internal pytest runner is `scripts/run-gke-pytest.py`. Build its digest-pinned image with `scripts/build-gke-pytest-image.sh`, then pass `GKE_PYTEST_IMAGE=<repository>@sha256:<digest>`. It creates a dedicated run namespace, a Role / RoleBinding there, and a unique ServiceAccount / Job in the control namespace. The test identity has permissions only in its run namespace. The runner waits for pytest, collects target-resource diagnostics and Job logs before failure cleanup, then deletes its Job and ServiceAccount by UID and the run namespace by UID. Its integration test verifies the in-cluster identity and refuses cluster-wide namespace creation.

The runner accepts `--retention-seconds` (default `30`) to set the temporary expiry used by its controller-fallback test. This is separate from the main validation script's retention probe option.

On 2026-10-11, image `gke-pytest@sha256:92bb4a2366e17548f627f108543ead2d0743ac304cb13c26b3ad51fc2f3eb324` passed in Job run `9a29229094e8` (3 integration tests). Along with identity, runtime image, two-session workspace/file/command, `USER_EMAIL`, and creation-env checks, the suite set an 8 KiB command output cap, generated 20 KiB, confirmed an explicit `CommandOutputLimitExceeded`, and reread the saved partial output by cursor with `output_complete=false`. The two runtime Pods were on different nodes. A Kubernetes Pod-exec probe from one runtime attempted TCP access to the other runtime Pod IP and observed a connection denial; the management runner in `session-sandbox-system` successfully used the allowed file and tracker APIs as the positive control. The retention test used `--retention-seconds 15` without starting `LifecycleReconciler`: the controller deleted the suspended Sandbox and owned PVC at expiry; a reopened manager created generation 2 with an empty workspace and acknowledged the `workspace_cleared` event exactly once. The Job removed the run namespace after completion. The preceding cross-session run `8e423168c420` also passed before output-limit coverage was added. Earlier runs `b39a0715d2b0` and `835e269aece9` passed prior versions of the suite. Runs `8f7f5e2441de` and `ea219463562c` also passed earlier suite versions. `--verify-failure-artifacts` intentionally failed run `271369c3399b`; its artifact directory [`/tmp/gke-pytest-artifacts/271369c3399b`](</tmp/gke-pytest-artifacts/271369c3399b>) recorded pytest output and namespace UID, and the namespace was removed. Setting `GKE_CLUSTER_NAME=deliberately-wrong` was rejected before creating a namespace. Existing DNS/FQDN allow, denied HTTPS, management REST allow, and direct tracker gRPC deny results are recorded in [`python-sandbox-backend.md`](python-sandbox-backend.md). See [`acceptance-matrix.md`](acceptance-matrix.md) for per-scenario coverage and remaining GKE cases.

On 2026-10-11, the broader manager validation passed as run `6571b530a327` with `--idle-timeout-seconds 5 --retention-seconds 15`. It verified the full tracker command controls, descendant termination, gate close/reopen, tracker container restart recovery (`generation=2`), workspace-loss recovery, and controller retention expiry. The namespace was removed after completion.

The cluster pytest suite was extended and passed as run `5fb43f0a7774` with image `gke-pytest@sha256:6c0d74d1c3ee9488e7b0a85c96fe8a6492818263f790aaa539e4e93b2ad26537` (3 tests, 160.83 seconds). New assertions verified four concurrent `ensure` calls return one Sandbox, suspend/resume keeps the Sandbox generation and workspace PVC UID while replacing the runtime Pod, and the workspace file plus `USER_EMAIL` survive that replacement. The test also made the runtime output directory read-only, then verified the command reports `output_storage_error` and `output_complete=false`; it restored the directory permissions before cleanup. All test-owned namespaces, Pods, Sandboxes, and PVCs were removed.

The suite then passed again as run `9044c7fc039f` with image `gke-pytest@sha256:de8ec93068422abe0b645f876ea1dd86d974ee948d5a5ec5da2e1634f5598889` (3 tests, 156.67 seconds). It extended the runtime case to start a silent command, close the Python client, verify suspend remains blocked while the command runs, and reconnect by command ID for its final output. A separate command produced 6 KiB while its caller was disconnected; the new client replayed all bytes in 1 KiB cursor pages. The cluster pytest Job and run namespace were removed after success.

The next suite passed as run `85408779eb65` with image `gke-pytest@sha256:a642db69819b817b422b6cd947f73af4af7f4f4249b5bb45fe27f8357aded0a9` (4 tests, 171.53 seconds). Its new recovery probe started a silent command, removed that command's process identity from the durable journal, killed the runtime container supervisor, and waited for Kubernetes to restart the container. The restarted tracker returned `unknown`; `commands.wait()` raised `ExecutionOutcomeUnknown`, and `suspend()` remained blocked. This confirms the conservative recovery path when process identity is unavailable. The Job removed its run namespace after success.

Run `9bdefb0cf306` passed the same four GKE tests with image `gke-pytest@sha256:2e1d163d3da7a8f1e04f2eaaeb9dcfed58f3a262fe39c1a5d65b5c7ad0640df8` in 288.84 seconds. The runtime test started two commands concurrently, ran `LifecycleReconciler.reconcile_once()` against the live GKE session, verified the session stayed Ready, and confirmed suspend remained blocked until both commands completed. It also streamed 12 MiB to the workspace-backed journal after disconnect and replayed all bytes in 256 KiB cursor pages. With `command_output_limit_bytes=16 MiB`, a 17 MiB producer returned an explicit output-limit error and retained partial output. The run namespace was removed after success.

Run `acf4242b0ed0` passed all four GKE tests in 260.94 seconds with image `gke-pytest@sha256:a377430d660f3a063b7e5c91a0478ef5b84bb74f7dd2478fcbbcb0480f03dde5`. The suspend/resume check confirmed the Sandbox resource retained generation and suspension/expiry annotations while its runtime Pod was deleted, then the lifecycle reconciler projected `state=ready` and the replacement Pod UID after resume. The run namespace was removed after success.

Run `e84561fce20d` passed all four GKE tests in 266.74 seconds with image `gke-pytest@sha256:fd00d50163b330203276a7897dcaaacd680bb3192635e4aa9dac524dfe215bb9`. The runtime test launched command admission and `manager.suspend()` concurrently and verified the result was one of the two safe outcomes: an accepted command makes suspend return `SandboxBusyError`, or a completed suspend makes command admission fail with a known `ResourceOwnershipError`. The test passed; its run namespace was removed after success.

The first run of the suspend/resume extension (`5528d87d8d8b`) exposed a five-second timeout when reopening the tracker gate immediately after the replacement Pod became Ready. Gate updates now retry transient connection errors and HTTP 5xx responses for up to 30 seconds; the retry behavior passed a unit test and the GKE run above passed. The initial run's failure artifacts are under `artifacts/gke-pytest/5528d87d8d8b` (git-ignored).

## Timing parameters

The GKE runner accepts the timing values directly so the same scenarios can use a quick test interval or the production interval:

| Option | Default | Applies to |
| --- | ---: | --- |
| `--idle-timeout-seconds` | `5` | Primary session idle suspend; set to `300` for the production boundary |
| `--retention-seconds` | `30` | Disposable retention-probe session only; the run really waits this long after suspension |
| `--reconcile-interval-seconds` | `60` | Lifecycle manager configuration used by the run |
| `--agent-turn-wait-seconds` | `10` | How long the one-turn fixture stays active while reconciliation checks it |
| `--timeout` | `300` | Kubernetes operation and cleanup timeout, not an overall run deadline |
| `--artifact-dir` | `artifacts/gke-session-manager` | Directory for per-run diagnostics on failure; defaults to a git-ignored path |

For example, `--retention-seconds 15` performs a 15-second retention test. `--retention-seconds 604800` tests the full seven-day interval and therefore keeps the validation run waiting for seven days. The main session keeps a separate one-hour retention setting so a short disposable probe cannot expire other scenarios.
