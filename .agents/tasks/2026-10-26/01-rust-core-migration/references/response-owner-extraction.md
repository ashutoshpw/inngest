# Bounded Go response-owner extraction (accepted; PR delivery in progress)

Worktree `/home/ashutosh/.t3/worktrees/inngest/t3-ca8735b9`, branch
`t3/extract-go-response-owner`. `git worktree list` confirmed this checkout owns
the branch. It had zero unique commits before fast-forward from `a4592cde4` to
`ab4e65192e22fe1f9787eae7903a6b8349420247`, the accepted PR #9 head on
`t3/stage-batch-reference-corpus`. `origin/t3/stage-batch-reference-corpus`
matched that base; the response-owner branch was absent from origin before its
first push. This T3 child thread remains bound to
`rust-core/01-compatibility-baseline`; all repository commands for this task
explicitly used the designated checkout above.

## Read-only discovery and scope

The [accepted staged corpus](staged-batch-reference.md), prior framed/direct
screening, source owners, RunContext/checkpoint callers, and recovery witness
were inspected before edits. Two tiny bridge screens did not establish a useful
crossing. The accepted next hypothesis is a Rust-owned response continuation
with Go effect ports, not a faithful whole pure planner that does not exist.

This increment factors response staging, concurrent group joins/error policy,
and successful/permanent-failure save continuations into a private Go runner.
Go effects keep output/AI/tracing preparation, writes, lifecycle notifications,
capacity release and discovery queue recipes. Retryable StepError, planned and
excluded-opcode sequencing remain existing Go handlers. These are explicitly
remaining ports/continuations, not a completed whole-response extraction.

Future candidate batches contain only None, Step, StepRun, StepError,
StepFailed and StepPlanned. Sleep, wait/invoke/gateway/defer/completion,
unknown/excluded/invalid descriptors keep the **entire** batch in Go, including
mixed batches. No routing/capability selector is introduced by this increment.

## Test authoring gate

Reuse real HandleGeneratorResponse literal corpus, grouping/hash oracle,
retry/coalescing, checkpoint lifecycle and surviving-store replay witnesses.
No fixture expectation changes or private runner tests are needed. A narrow
extension to the existing save-authority table may guard original error identity
and wrapped-error/pending results at that same production boundary: credible
typed-result regression is to classify/reconstruct an error or discard pending
when an idempotent save returns an error. Existing sentinel-only cases assert
presence, not exact returned error identity. No production test seam is needed.

Two additional real-boundary cases guard extraction-specific risks. The metadata
case observes the actual SaveStep context after real metadata emission: dropping
the prepared context or swapping before tracing loses bytes. Existing metadata
owner tests cover size limits/accumulation but do not reach this save port. The
discovery-mode case supplies conflicting opcode and RunContext parallel modes:
sharing the success/failure recipe predicate would enqueue/suppress incorrectly.
Existing tests cover planned race options and the generic predicate, not this
asymmetry at the response boundary. A selected-overflow case also checks its
original error wins over both response retry preferences; the existing discovery
always-retry and planned/sleep preference cases retain their precedence
coverage. The latter exercise handler-local policy, not ordinary joined-group
NoRetry precedence; the review follow-up below closes that distinct gap. These cases reuse the recording store/queue and
literal expectations, with no new production hooks or alternative algorithm.

## Verification and remaining gates

Go 1.26.4 Linux/amd64, binary `/home/ashutosh/.cache/go1.26.4/go/bin/go`.
Every Go invocation below uses `GOTOOLCHAIN=local`, `GOMAXPROCS=2`,
`GOTMPDIR=/home/ashutosh/.cache/response-owner/tmp`, and the same path for TMPDIR.
All use `-p=2`; no full E2E/destructive `tests.sh`, benchmark, Rust build or
full-repository suite was run. No fixture answers were changed.

Exact argument tails (`go` means the absolute binary above):

```text
# Before production edits; exit 0, executor 0.036s.
go test -p=2 ./pkg/execution/executor -run '^(TestStagedBatch|TestOpcodePlan|TestDiscoveryEnqueueFailureAlwaysRetries|TestHandleGeneratorStep|TestMaybeEnqueueDiscoveryStep|TestHandleGeneratorResponse_CoalesceKey)' -count=1 -timeout=60s

# First extraction, before test extensions; exit 0, executor 0.038s.
# Same command as above, log focused-first.log.

# Final affected owner packages; all exit 0.
# Prefix: sudo -n -u ashutosh -g docker env <the four overrides above>
go test -p=2 ./pkg/execution/executor ./pkg/execution/state ./pkg/execution/driver/httpdriver ./pkg/execution/checkpoint -count=1 -timeout=120s
# executor 5.209s; state 0.054s; httpdriver 1.073s; checkpoint 0.192s.

# Focused race, including the accepted sibling barrier and direct checkpoint witnesses.
go test -p=2 -race ./pkg/execution/executor ./pkg/execution/checkpoint -run '^(TestStagedBatch|TestOpcodePlan|TestDiscoveryEnqueueFailureAlwaysRetries|TestHandleGeneratorStep|TestMaybeEnqueueDiscoveryStep|TestHandleGeneratorResponse_CoalesceKey|TestCheckpointAsyncSteps_NotifiesRunStepRunFinishedLifecycle|TestCheckpointSyncSteps_NotifiesRunStepRunFinishedLifecycle)' -count=1 -timeout=120s
# Exit 0: executor 1.227s; checkpoint 1.194s.

# New assertions also pass on the ORIGINAL production entry/handlers.
go test -p=2 -overlay=/home/ashutosh/.cache/response-owner/baseline-overlay.json ./pkg/execution/executor -run '^(TestStagedBatchOutputAndSaveAuthority|TestStagedBatchMetadataDeltaReachesSave|TestStagedBatchDiscoveryParallelModeAuthority|TestStagedBatchStateOverflowPrecedesRetryPreferences)$' -count=1 -timeout=60s
# Exit 0, executor 0.035s. Overlay maps only executor.go to exact git-show HEAD
# (ab4e65192) bytes in cache/baseline-executor.go. Repository files are untouched;
# the new private declarations compile but the original production entry calls
# the original owners. This establishes existing behavior, not a bug-fix claim.
```

Additional development-only runs: original-entry save-table overlay alone passed
(0.041s); the three initial extraction-risk tests before adding overflow passed
(0.054s). No failed Go invocation occurred in that initial validation batch. The first Docker access probe failed
on the socket's group permissions; the existing supported subprocess prefix
`sudo -n -u ashutosh -g docker` succeeded, with no account/group configuration
change. The same uid owns test/cache artifacts. Docker server reports 28.3.2.

Integration runs use the same four overrides plus `TEST_MODE=true`,
`TEST_DATABASE=sqlite`, and separately `EXPERIMENTAL_KEY_QUEUES_ENABLE=false`
and `true`. Each has the subprocess prefix above. Exact selections:

```text
go test -p=2 ./tests/execution/executor -run '^(TestRecoveryReplaysPersistedSDKResponse|TestDeferFinalize|TestDeferAdd|TestDeferAbort|TestFinalizePublishesFnFinishedOnce|TestFinalizeRetriesFnFinishedAfterPublishFailure|TestFinalizeDeleteFailureDoesNotLoseFinishEffects|TestParallelPauseBackedOpsCoalesceDiscovery|TestSleepCompletionSaveRetries|TestInvokeRetrySucceedsIfPauseAlreadyCreated|TestAsyncOpQueuedAtNotInheritedFromRequest|TestParallelOpcodesShareQueuedAt|TestExecutionSpanKeepsItemEnqueuedAt)$' -count=1 -v -timeout=180s
# Both exit 0, 13 top-level passes/no skips each: false 4.731s; true 4.620s.

go test -p=2 -race ./tests/execution/executor -run '^TestRecoveryReplaysPersistedSDKResponse$' -count=1 -v -timeout=120s
# Both exit 0: false 3.733s; true 3.752s, with each mode logged by the witness.
```

Logs and JSON exact argv/status records are local cache evidence under
`/home/ashutosh/.cache/response-owner/`: `focused-first.log`,
`extraction-risk-first.log`, `baseline-save-table.log`,
`baseline-extraction-risk.log`, `owner-packages.log`, `focused-race.log`,
`integration-{false,true}.log`, `integration-results.json`,
`recovery-race-{false,true}.log`, `recovery-race-results.json`.
These are temporary artifacts, not durable CI logs or performance evidence.

Static source comparison confirmed byte-identical function bodies for Execute,
HandleResponse, HandleGenerator, handleGeneratorOp, handleStepError,
handleGeneratorStepPlanned and handleGeneratorSleep, plus the moved goroutine
dispatch loop and failure-discovery recipe. Grouping/hash helpers, state/queue
implementations, checkpoint package and fixtures have no diff. Gofmt and
`git diff --check` passes. The known unrelated
`bash scripts/check-plan-status.sh` failure is the pre-existing
`run-ai-debugging.md` status `Implemented — pending staging review`; that file
is untouched. Broad Phase 1 stays partial, Phase 2 open, bridge choice unset.

## Bounded review follow-up: ordinary group-error preference precedence

Independent Sol effects review reported no blockers. Independent Luna staging
review reported no production correctness blockers and one P2 coverage gap at
`response_runner.go`'s NoRetry branch: overflow and always-retry tests bypass it,
and planned/sleep tests apply preferences within their separate handlers.

Added only `TestStagedBatchGroupErrorNoRetryPrecedesRetryAt` to the existing
staged corpus, reusing its recording store/queue. A StepRun SaveStep returns an
ordinary `save unavailable` error with both response NoRetry and RetryAt set.
The actual HandleGeneratorResponse result must reject retry at attempt 0 of 3,
have no retry-at wrapper, and forward the literal original error message. The
queue's nonRetryable wrapper embeds error without Unwrap, so error identity via
errors.Is is not its contract. The test also observes only the save event and no
queue continuation. The error is neither a swallowed idempotent/duplicate error,
state overflow, nor always-retryable. No private predicate, source-shape assertion,
fixture-answer change, new production seam or production repair was added.
This protects the existing policy against branch reversal, not a product bug fix.

Authoring gate: the observable contract is NoRetry over RetryAt on an ordinary
joined-group error; reversing those branches is the credible regression; earlier
cases bypass or resolve that policy in another handler; the real production entry
and existing recorder suffice without a seam. Your explicit bounded implementation
request authorizes this test edit after read-only discovery.

All commands use the Go 1.26.4 binary and four cache/parallelism environment
overrides specified above, with `-p=2`. Selection `P` below is exactly:

```text
^(TestStagedBatchGroupErrorNoRetryPrecedesRetryAt|TestStagedBatchStateOverflowPrecedesRetryPreferences|TestStagedBatchPlannedSleepEnqueueFailurePreferences|TestDiscoveryEnqueueFailureAlwaysRetries)$
```

```text
go test -p=2 ./pkg/execution/executor -run P -count=1 -timeout=60s
# Exit 0, 0.041s; preference-fix-focused.log.
go test -p=2 -overlay=/home/ashutosh/.cache/response-owner/baseline-overlay.json ./pkg/execution/executor -run P -count=1 -timeout=60s
# Exit 0, 0.036s; preference-fix-baseline.log.
go test -p=2 -race ./pkg/execution/executor -run P -count=1 -timeout=60s
# Exit 0, 1.178s; preference-fix-race.log.
```

The overlay's baseline executor bytes were verified equal to
`git show ab4e65192e22fe1f9787eae7903a6b8349420247:pkg/execution/executor/executor.go`
before testing. It replaces only executor.go, preserving the original entry and
handlers, as in the initial overlay proof above.

Temporarily reversed only the responseRunner.run NoRetry/RetryAt branches.
`go test -p=2 ./pkg/execution/executor -run '^TestStagedBatchGroupErrorNoRetryPrecedesRetryAt$' -count=1 -timeout=60s`
exited 1 (0.037s), specifically at the ShouldRetry false assertion with message
`response NoRetry wins over RetryAt for an ordinary group error`.
A finally block restored exact original bytes; response_runner.go SHA-256 is
`86933644cd421fecf122260625ba62176a60cf9e0a93ba1b0311eddbab3fc064`.
The pre-follow-up hashes of executor.go, response_runner.go and response_effects.go
were also verified unchanged. Mutation log/result and initial production hashes
are `preference-fix-mutation.log`, `preference-fix-mutation-result.json` and
`preference-fix-production-sha256.json` under the same local cache directory.
No mutation is retained. The focused selection was rerun after restoration with the same normal command
and selection P: exit 0, 0.032s, `preference-fix-restored.log`. Gofmt and git diff --check also pass.

No broad integration rerun, benchmark, or additional delegation was made for the
P2 test repair. Independent Luna-max round two reviewed the bounded fix, passed
normal and race validation, confirmed no blockers, and confirmed the three
production files were byte-identical across the test-only repair:

| Production file | SHA-256 before and after repair |
| --- | --- |
| `executor.go` | `820c6b0f5ebb17865877cca5628883e664316ce7693dd8a1c0820aa5d9a99fb7` |
| `response_runner.go` | `86933644cd421fecf122260625ba62176a60cf9e0a93ba1b0311eddbab3fc064` |
| `response_effects.go` | `4b09e810895f29c07e396b9ba1966642c18ccf742b20757b67732bb0039dc87a` |

The parent accepted the bounded extraction. This acceptance covers only the Go
staging/join and accepted-save continuation in this increment. Retryable/planned
sequencing, finer queue ports, lifecycle/host drain contracts, recovery, Rust
activation and the broader phase gates remain open.

## Changed ownership and invariants

- `HandleGeneratorResponse` calls `responseRunner.run`; the prior staging/join
  algorithm is removed from `executor.go`. The private runner proposes metadata,
  waits for the Go write, checks the step limit, groups original descriptors,
  binds the shared handling clock, computes the existing duplicate-preserving
  coalesce hash, and waits for the pending replacement before dispatch.
- `responseMetadataUpdate` is a proposal only. UpdateMetadata precedes the limit
  and never refreshes the loaded snapshot. Stored request version governs
  coalescing unless it is -1, in which case the response version must be >=2.
  The existing grouping/hash helpers and SavePending implementation are reused.
- `goResponseEffects.runGroup` retains the original goroutine loop, copied
  runInstance/item/opcode, original metadata/accounting references and per-opcode
  history UUID activation. Its plain errgroup joins all siblings, without
  cancellation or severity ranking. The runner applies overflow, always-retry,
  response NoRetry, then RetryAt to the returned racing first error. A group
  failure prevents later groups, after all siblings in that group have finished.
- `handleGeneratorStep`/`handleStepFailed` bind the actual RunContext, opcode and
  edge to `responseRunner.step`/`failedStep`. Preparation, AI extraction, option
  decode, serialization and trace metadata stay Go. Trace emission and
  SwapMetadataSizeDelta precede the single SaveStep effect. The typed request
  holds run ID, step ID and prepared bytes; the result retains the actual bool
  and original error object, without classification/reconstruction.
- Success accepts duplicate/idempotent saves and still uses their returned
  pending flag. Permanent failure accepts idempotent but returns duplicate.
  Rejected saves return before lifecycle/release/discovery. Accepted saves run
  finished/failed lifecycle, release capacity, then the existing Go discovery
  recipe. Release errors are logged and do not suppress discovery.
- Success discovery still consults opcode parallel mode; permanent failure
  still consults RunContext parallel mode for pending suppression, while storing
  opcode mode on the queue item. JobID recipes, reset/tracing/enqueue options,
  duplicate suppression and callbacks are preserved. Suppressed enqueues still
  allocate their original UUIDs, read their original clocks and notify callbacks.
- Retryable StepError remains the original handler: no save, Errored lifecycle
  at old attempt, increment the branch copy, schedule at new attempt, return
  ErrHandledStepError. SDK/user NoRetry and exhausted attempts still convert to
  StepFailed and enter the extracted failure continuation. Planned enqueue
  errors still reach callbacks; duplicate enqueues still suppress them.
- Shared handling time is sampled after metadata/limits. Every later clock and
  generated ID remains at its original Go activation site. Sleep and its
  absolute-clock gap are untouched. No eager batch projection/sampling, second
  algorithm, speculative effects, post-effect fallback/replay or durable state.

The public `execution.Executor` interface, `HandleGenerator` dispatcher,
checkpoint/direct RunContext behavior and all excluded handlers stay intact.
All-empty None redrive remains before the runner in HandleResponse. No new
validation changes the existing behavior of invalid internal descriptors.
This adds two private files in the existing executor package, not a new package
or exported extension surface. The concrete Go effects are deliberately coarse:
discovery recipes still contain conditional queue/tracing/callback sequencing.
This is a real save-result continuation seam, not the completed Rust boundary.

## Concrete next integration prerequisites

1. Separately extract retryable StepError and planned-step sequencing plus
   discovery reset/enqueue/result/callback ports at their existing activation
   sites. Preserve the differing success/failure mode and duplicate policies.
   Keep output preparation, AI extraction, options and tracing serialization Go.
2. Define one execution-local Rust continuation for an eligible **whole batch**,
   suspended at actual metadata/pending/save/effect results and sibling joins.
   It must encode the algorithm once, including per-save result branches and
   nondeterministic error selection, without a per-opcode bridge crossing.
   This Go layer contains no candidate selector, ABI, Rust host or activation.
   Sleep/wait/invoke/gateway/defer/completion/unknown/invalid descriptors must
   keep the entire batch on the original Go runner; never split mixed batches.
3. Specify context, Go error/value handles, byte ownership and continuation
   lifetime through every sibling and callback; original error identity and
   pending authority remain Go observations. No post-effect fallback to replay.
   No resource/lifecycle abstraction is added just to anticipate this host.
4. Measure the actual extracted owner and projection/validation/materialization
   costs on matched workloads before another bridge choice. Existing component
   samples cannot be summed into those missing costs. No benchmark is needed to
   claim the bounded Go refactor's functional checks, and none was run here.
5. Establish host shutdown/drain and recovery contracts before adding host
   resources. `svc.Stop` currently closes legacy lifecycle listeners **before**
   `s.wg.Wait`; `CloseLifecycleListeners` joins listener Close calls only.
   `Execute` invokes response handling inside `util.CritT` with
   `context.WithoutCancel` and MaxFunctionTimeout+5s, not request cancellation.
   A timed-out CritT body can outlive its caller; queue-return accounting alone
   cannot prove it has stopped accessing a future host. Direct checkpoint paths
   also need their own admission/drain accounting. A safe host lifetime may
   require a separately scoped Execute/shutdown change; that portion stops here.
   This increment owns no new resources and preserves current ordering.
6. Retain the surviving-store recovery witness (both queue modes), which covers
   drained Go owners, not active leases, process crashes, persisted Rust
   continuations, general exactly-once behavior or host failure mid-effect.
   Future recovery must preserve the existing retry/checkpoint/idempotency path
   without inventing new durable state in this extraction.

## Accepted scope and delivery

- Production: `pkg/execution/executor/executor.go`, `response_runner.go` (new),
  `response_effects.go` (new). Production diff: 415 added/341 removed lines,
  net +74, including comments and factoring existing bodies.
- Tests/support: `pkg/execution/executor/staged_batch_reference_test.go`,
  83 added/2 removed lines; no fixture changes or production test-only seams.
- Durable records: this reference (new), `plan.md`, `status.md`, and a forward
  link/current-candidate clarification in `references/staged-batch-reference.md`.
- The eight-file scope is limited to the three production files, one existing
  executor corpus test, and those four task records. No cache, binary, overlay,
  or unrelated file is part of the change.
- PR number, URL, opening commit, review state, and opening checks will be added
  here after creation. The PR targets `t3/stage-batch-reference-corpus` (PR #9).
- No delegation, Rust implementation, ABI, routing, activation or new resource
  was added. The accepted change is not the whole response continuation.
- Review risks: this intentionally leaves coarse queue effects and retry/planned
  sequencing Go-owned; no claim of full extraction, Rust feasibility or speedup.
  Host shutdown, late CritT work, callback lifetime, active-lease/crash recovery,
  full projection/validation/materialization cost and native arm64 remain gates.
