# SDK response recovery witness V1

This synthetic corpus is pinned to Inngest source
`8ebc428587cc11220e86d6fbbd5ab7ffc3bf5fef`. It pairs raw HTTP generator
responses with literal outbound-request and persisted-state projections.
[`TestRecoveryReplaysPersistedSDKResponse`](../../recovery_replay_test.go)
consumes it through the real HTTP driver and executor.

These are authored protocol examples, **not captured SDK executions**. The SDK
header uses the repository's JS test dependency pin,
`inngest-js:v3.11.1-pr-411.4`; it does not establish that SDK's behavior or a
supported version range. The step IDs are synthetic opaque protocol IDs, not
claims about an SDK's hash calculation.

Production contract sources at the pinned revision:

- `pkg/execution/driver/driver.go`: `MarshalV1` loads persisted steps and stack,
  emits the stored request version, and wraps the original event for HTTP.
- `pkg/execution/driver/httpdriver/config.go` and `httpdriver.go`:
  `Config.NewDriver`, actual HTTP, request-version headers, and generator parsing.
- `pkg/execution/state/opcode.go`: `StepRun` data is persisted inside `data`.
- `pkg/execution/executor/executor.go`: schedule/load, response-version
  negotiation, `handleGeneratorStep`, discovery enqueue, and always-retry errors.
- `pkg/execution/state/redis_state/redis_state.go` and its Lua scripts:
  checkpoint output/stack storage; queue shard and reader files in the same
  package own the durable jobs and leases.
- `pkg/execution/queue/process.go`: actual callback error handling, durable
  requeue without consuming an always-retry attempt, and dequeue on success.
- `pkg/db/sqlite/migrations.go`: `Persist: true, ForTest: true, Directory: ...`
  opens a fresh handle to a file database rather than the default singleton.

## Portable fixtures

- `checkpoint.http`: HTTP 206, request version 2, SDK header, and literal
  `StepRun` result for `checkpoint-output`.
- `replay.http`: HTTP 206 and literal `Sleep` for `next-sleep`, lasting one hour.
- `initial.request.json`: selected fields of the first actual HTTP request,
  including unknown version -1, original event/run identity, no saved steps,
  and the SDK maximum of one attempt (function retries: 0).
- `replay.request.json`: the same request projection after recovery, with
  negotiated version 2, the saved `data` wrapper, and one stack entry.
- `checkpoint.state.json`: literal version, internal event ID, saved output,
  one stack entry, and no pending steps. Compared at the intercepted enqueue,
  through B's freshly opened state adapter, and through a third reader after B
  has stopped.

The `.http` files use LF line endings and EOF-delimited bodies. The harness
parses their status/headers with `http.ReadResponse`, then serves their body
bytes unchanged through `httptest`. Header-name case and JSON object-key order
are not contractual here. JSON projections are compared structurally; array
order is preserved. Only a nil pending list is normalized to `[]` in the state
projection. The outbound stack is already `[]` when empty because the production
`FunctionStack.MarshalJSON` does that conversion.

Expectations are literals authored from the pinned contracts and formatted
with an independent JSON writer. They are not generated from `MarshalV1`,
`GeneratorOpcode.Output`, or a fixture-update mode. Request IDs, generation IDs,
queue references, trace/group identifiers, and unselected context fields are
excluded from the request projection. The original durable job ID is read
before dispatch and independently compared with both queue callbacks and each
actual HTTP body's `ctx.job_id`. Full request byte canonicalization remains
owned by `pkg/execution/driver/testdata/sdk-request-v1`.

## Scenario and independent observations

One Valkey 8.0.3 testcontainer and one SQLite temporary directory survive. A
registers the function once in SQLite and schedules the run with fixed IDs,
`At`, and `IdempotencyKey`. Queue processors, queue shards/producers, and
executors receive public clock options. Separate fake clocks start at
2026-10-26 12:00:00 UTC; scanner ticks are advanced in 10 ms increments. A's
retry is fixed at 12:10:00 UTC, beyond the bounded polling window.

The SDK endpoint branches exclusively on whether the **actual HTTP request**
contains `steps.checkpoint-output`. Without it, every call increments an
external Valkey counter with plain `INCR`, then returns `checkpoint.http`.
There is no effect dedupe key, state-service access, or callback-count branch.
With it, the endpoint returns `replay.http`. That endpoint is a protocol
fixture, not an implementation of the executor's persistence/recovery logic.

A wraps its real queue, intercepts the first `KindEdge` discovery enqueue with
outgoing `checkpoint-output`, reads the actual persisted checkpoint at that
boundary, and returns a sentinel **before forwarding**. The normal worker
callback returns the actual `Execute` error. The test requires its always-retry
classification, then reads `RunJobs` until the original job has the controlled
retry timestamp, attempt 0, and no item lease. A callback signal alone does not
satisfy that condition.

A is canceled and awaited; lifecycle listeners are closed, HTTP idle
connections are closed, SQLite is closed, and both Redis clients are closed.
B constructs new clients, Redis adapters, pause manager, queue shard/registry,
queue processor/producer/readers, SQLite handle/adapter/function loader, HTTP
client/driver, executor, tracer provider, and clock. Only store connection
information and fixed configuration cross the boundary. B neither re-registers
the function nor manually enqueues or executes a captured job. Advancing B's
clock lets the normal scanner recover the persisted original job.

The final fresh readers require unchanged checkpoint output/stack, no pending
steps, the original retry removed, exactly one future `next-sleep` scheduled
about one hour after B's recovery, and external effect count 1. No sibling
completion order is assumed. The queue's original `KindStart` item retains its
production system retry budget (`MaxRetries + 1`); the executor's loaded
function and actual SDK requests use the configured one attempt. The test
does not claim exhaustion of that original queue budget. Always-retry identity
and preserved attempt 0 are checked directly. `AlwaysRetryError` does not
unwrap its embedded sentinel, so the test checks error text and retry marker.

## Test authoring gate

This protects response bytes -> durable checkpoint -> recovered outbound
request at a transport and owner-replacement boundary. Credible failures are
omitted replay data, enqueue before checkpoint save, and loss of system retry
classification. Existing `sleep_retry_test.go`, `finalize_duplicate_test.go`,
and `pkg/execution/executor/handle_generator_step_test.go` retain ownership of
sleep completion, finalization, and duplicate/coalesced generator scenarios.
They do not replace runtime owners against surviving stores or inspect actual
HTTP replay. No runtime changes, test-only exports, or new production hooks
are needed. The fixture supplies responses, never the checkpoint receipt that
the production path must write.

## Validation evidence (2026-10-26)

Go 1.26.4 on Linux amd64, Docker 28.3.2. The existing
`StartValkey`/`NewValkeyClient` helper is reused. Pulled
`valkey/valkey:8.0.3`, digest
`sha256:4e78d8ccab4d8978afca140a3dd75b4e45d683d1e51602ee71e976bcc2bd939e`.
Docker group access was provided with `sudo -n -u ashutosh -g docker`.
`GOTMPDIR` and `TMPDIR` were set to
`/home/ashutosh/.cache/inngest-recovery-gotmp` because `/tmp` was nearly full.

Commands use `/home/ashutosh/.cache/go1.26.4/go/bin/go`:

```sh
# Run separately with EXPERIMENTAL_KEY_QUEUES_ENABLE=false and true.
go test ./tests/execution/executor -run '^TestRecoveryReplaysPersistedSDKResponse$' -count=1 -v -timeout=60s
go test -race ./tests/execution/executor -run '^TestRecoveryReplaysPersistedSDKResponse$' -count=3 -timeout=120s

go test ./tests/execution/executor ./pkg/execution/executor -run '^(TestSleepCompletionSaveRetries|TestFinalizePublishesFnFinishedOnce|TestFinalizeRetriesFnFinishedAfterPublishFailure|TestFinalizeDeleteFailureDoesNotLoseFinishEffects|TestMaybeEnqueueDiscoveryStepCoalesces|TestHandleGeneratorStep_ConcurrentCompletionsCoalesce|TestHandleGeneratorStepPlanned_StampsCoalesceKey|TestHandleGeneratorStepEnqueuesDiscoveryOnDuplicate|TestDiscoveryEnqueueFailureAlwaysRetries)$' -count=1 -timeout=120s
```

Initial non-race runs passed in both queue modes. After final cleanup changes
and restoration of mutation sources, all six race repetitions passed: false
mode 7.878 s, true mode 8.003 s. The selected existing regressions also passed:
integration executor package 0.459 s, production executor package 0.035 s.
No full repository suite, SDK E2E, Rust comparison, or performance benchmark
is claimed.

Three temporary production mutations were run separately with key queues false.
Each failed the new test for the intended reason, not a compilation failure:

1. Removed the stacked-step assignment to `req.Actions` in `MarshalV1`:
   `replay.request.json` failed because actual `steps` was empty.
2. Deferred `SaveStep` in `handleGeneratorStep` until after the attempted
   discovery enqueue: `checkpoint.state.json` failed at the intercepted
   boundary because both output and stack were empty.
3. Replaced `retryableEnqueueError`'s `AlwaysRetryError` with plain `fmt.Errorf`:
   the actual `Execute` result failed the always-retry classification assertion.

Both production files were restored byte-for-byte after each experiment.
Restored SHA-256: `pkg/execution/driver/driver.go` =
`336067a0bcd17675a31b2abf9493bf46af4a889d5e1d49001de1d24ae6a4e56d`;
`pkg/execution/executor/executor.go` =
`f9a53b9fee1eda4534b6e3814b0f9f7c0126e4b402c732db16a4ec7d09338e88`.

## Layer 3 integration validation (2026-10-26)

The recovery branch fast-forwarded to accepted layer 3
`240c75432dcf6e895399287a0414cbfd0ab43ef2` without changing the reviewed
recovery test or runtime sources. The original corpus provenance remains
pinned above. The targeted recovery test passed once in each key-queue mode
(false: 2.285 s; true: 2.185 s), and the new queue/run-state compatibility
tests passed (0.027 s), using Go 1.26.4 on Linux amd64:

```sh
for mode in false true; do
  sudo -n -u ashutosh -g docker env GOTMPDIR=/home/ashutosh/.cache/inngest-recovery-gotmp TMPDIR=/home/ashutosh/.cache/inngest-recovery-gotmp GOMAXPROCS=2 GOTOOLCHAIN=local EXPERIMENTAL_KEY_QUEUES_ENABLE="$mode" /home/ashutosh/.cache/go1.26.4/go/bin/go test -p=2 ./tests/execution/executor -run '^TestRecoveryReplaysPersistedSDKResponse$' -count=1 -v -timeout=60s
done
env GOTMPDIR=/home/ashutosh/.cache/inngest-recovery-gotmp GOMAXPROCS=2 GOTOOLCHAIN=local /home/ashutosh/.cache/go1.26.4/go/bin/go test -p=2 ./pkg/execution/state/redis_state -run '^(TestQueueItemPersistenceCompatibility|TestRunStatePersistenceCompatibility)$' -count=1 -timeout=120s
```

Independent Luna-max review found no blockers before integration. Prior race
and mutation evidence above was accepted and was not repeated for this base
advance. Formatting, fixture JSON, relative Markdown links, staged whitespace,
and unchanged runtime sources were checked. The repository plan-status script
still reports the pre-existing invalid `Implemented — pending staging review`
status in `docs/plans/run-ai-debugging.md`; that unrelated plan is unchanged.

## Limits

This witnesses replacement of drained runtime owners against surviving stores
and one synthetic effect remaining once. It does not test an OS process
restart, a crash with an active lease, Valkey restart/disk durability, arbitrary
exactly-once behavior, concurrent owners, SDK conformance, supported SDK policy,
or broader lifecycle/queue persistence parity. SQLite persists function
registration; Valkey retains execution state and queued work. The future sleep
is inspected, not waited out. This is one reference witness for the migration,
not completion of its recovery or SDK-response baseline.
