# Real Go effects and test-gated response continuation integration

State: bounded implementation accepted after independent native and Go reviews;
production activation remains gated. Updated 2026-10-27. Base
`6a8cd4acac68ad4a3f9d5be3d9ee3ed352b51db9`, clean/no unique commits before
fast-forward in T3-bound worktree `/home/ashutosh/.t3/worktrees/inngest/t3-5e6f1d5e`,
branch `t3/response-continuation-effects`. No other checkout is edited.

## Authorized scope

Real private Go ports shared by production handlers and an isolated, test-only
six-tag Rust continuation driver. FFI is a functional experiment only; no
production selector, option, export, activation or performance claim. Broad
Phase 1 remains partial, Phase 2 open, bridge choice unset. Existing model
acceptance covers 40 literals, not this integration.

## Test authoring gate

1. Protect actual Go preparation/effect parity across the native continuation,
   original descriptor/context/error identity, authoritative pending, concurrent
   sibling progress, transport correlation, and resource lifetime through issued
   work and separately drained callbacks.
2. Credible failures: using fixture prepared bytes, rebuilding a descriptor from
   the lossy Rust projection, accepting a stale/duplicate completion, losing a
   consumed Resume on cancellation, save/enqueue replay after uncertain ack,
   cancelling siblings on first error, or releasing bindings before callbacks.
3. Reuse all 40 literals and their existing assertions; the native boundary and
   real Valkey persisted-state witness cover risks the isolated model and fake
   state adapter cannot reach. Keep existing Go owner tests as their primary
   contracts. Add distinct transport/lifetime observations, not implementation
   call-shape or source-string tests.
4. Production helpers must be private and called by existing Go handlers.
   State-port constructors and the integration driver stay in test files;
   native adapter files require linux && response_continuation_spike && cgo.
   No production-only test seam or opaque whole-handler callback is authorized.

## Open activation gates

Service-wide checkpoint admission, late CritT body draining, Stop listener-before-
queue ordering, ownership fencing and uncertain/lost-ack recovery remain open.
The isolated harness explicitly drains its own effects and callbacks. The older
surviving-store witness replaces drained owners; it is no Rust host crash proof.
Native arm64 execution/final link and process comparison remain future gates.

## Implementation and validation

### Go production scope

`response_effects.go` exposes private activation, retry span and explicit-time
lifecycle effects. Existing dispatcher/StepError/accepted-save handlers use them.
Activation remains after edge/history binding and before preparation; it counts
None and sets the incoming StepError span status before permanent conversion.
The existing runGroup recovery still terminates a panicking branch with nil and
joins siblings. No service/shutdown/queue algorithm is changed.

`response_queue_effects.go` separates materialization, discovery/planned tracing,
enqueue, span finish, async launch and explicit-time sync listeners. Its private
responseQueueBinding owns the actual context, queue.Item/carriers, enqueue time
and DroppableSpan. Existing discovery, permanent failure, planned and retry
handlers call the helpers. Production branches/predicates, duplicate acceptance,
logging, callbacks, reset and clock sites retain their original sequencing.
Planned checkpoint handling still reads its fallback handling clock **after**
discovery-span creation. Explicit-time helpers never read that clock again.
Async closures retain the binding through delivery; tracking/draining callbacks
is entirely test-owned, with no production wait API or export.

Production scope is only executor.go, response_effects.go and the new queue file
(net fewer production Go lines). No unused port framework, new registry, generic
handler interface or opaque algorithm callback is introduced. response_runner.go
keeps its existing production Go ownership. The experimental driver exists only
in response_continuation_integration_test.go.

### Rust model and native ABI

The model adds ActivateBranch after history UUID binding (including None), and a
key-correlated BranchPanicked observation. A recovered Go effect panic consumes
that branch's pending key, completes it with nil, and joins remaining siblings;
a response/history-allocation panic is not admitted as a recovered branch panic.
Serde derives serialize owned model values directly; no opcode bridge frame or
opcode-specific protocol was reused.

The new response-continuation-bridge crate exports four experimental v1 C ABI
calls: start, resume, output_release, close. Start/Resume receive copied JSON
input and return a fully initialized NativeOutput scalar struct with integer
session/output handles, a read-only Rust allocation pointer and byte length.
The envelope has ready requests, an explicit done flag (distinguishing success
from incomplete), and an optional disposition. Handles increase without reuse;
active generations cannot collide. Bounds are 16 MiB per input/output call,
64 native sessions, 64 returned allocations, the existing 65,536 descriptor
admission cap and 1,000-step execution limit. These are call/count bounds, **not
an allocator/RSS guarantee**; copied recipe/pending data adds native memory.

Input buffers are borrowed only during a synchronous call and deserialized to
owned Rust values. Rust owns sessions and returned allocations, retains no Go
pointer, and performs no Go callback or application I/O. Registry/state locks
cover only owned state/serialization; external effects run after native return.
Raw output is written with ptr::write, never a Rust slice over uninitialized
writable caller memory. Pointer validity/liveness and exclusive writable ranges
remain the C caller's unsafe contract; null, bounds, overflow, alignment and
input/output overlap are checked. Panic containment also contains a panicking
payload destructor. Allocator aborts and process crashes are not recoverable
panic claims.

Go's internal/responsecontinuationspike adapter requires
`linux && response_continuation_spike && cgo`; only gated test files import it.
It copies output via C.GoBytes before releasing the Rust allocation and checks
release status. There is no production selector, option, exported executor API
or app dependency on the bridge. Native amd64 linking uses the GNU static archive
at rust/target/x86_64-unknown-linux-gnu/release/libresponse_continuation_bridge.a.
Its locally tested SHA-256 is
`562139fab7fec3ed44cdcb596276c1241d038ed95c0d34a165fa7d6d0f28ed7d`.
Native arm64 execution/final Cgo link remains a later gate, not an amd64 blocker.
This ABI is an experiment, not a selected or stable production protocol.
Native calls are synchronous and cannot be interrupted; Go effect cancellation
is cooperative, and close waits until issued effects actually return.

### Test driver ownership and decisions

The test-private driver starts one eligible whole response. Rust rejects any
excluded/nil/invalid member before returning a ready request; no split-batch
routing or Go fallback exists. Rust owns metadata proposals, limit ordering,
stale snapshot/coalesce/hash/pending decisions, retry/local conversion, save
acceptance, discovery suppression/reset/recipes, span/callback decisions and
joins. Go effects do not invoke whole opcode handlers.

Original Go descriptors, runInstance branch copies, prepared contexts/save
requests, materialized queue items/maps, spans and original errors stay in
branch-indexed Go storage. Preparation uses the original descriptor (including
Timing, Metadata, Userland, full options and error object); only the requested
local opcode tag can change. Clock requests bind actual Go time.Time values;
lifecycle, enqueue and callback effects use those values without another read,
preserving timezone/monotonic information. Metadata StartedAt preserves both
zero and epoch instants through i128 projection and the original Go binding.
The harness restricts active clocks/recipe/RetryAt observations to signed Unix
nanoseconds; broader time-range transport is not established.

SaveStep reports its authoritative hasPending plus errors.Is/queue-classification
observations and an opaque handle to the exact Go error. Dispositions resolve
that handle and use existing wrapping/queue constructors. Message copies never
stand in for error identity. Output preparation, size/AI/trace/shared metadata,
state/queue calls, lifecycle, capacity-release port and listener effects are real
Go functions. The 40-literal lane still uses the original recording state/queue
ports to provide independently selected errors/pending outcomes; those are fake
storage, not the real storage witness below. Fixture Prepared bytes are never
read by the integrated lane. The fixture file is unchanged.

Only the run loop resumes native state, in channel-observed completion order,
with generation + original branch index + sequence. Ready sibling effects run
concurrently: one can reach enqueue while another save remains blocked. There is
no all-saves barrier, first-error cancellation or error-priority ranking.
A run-state guard rejects repeated driver execution before it can redispatch
cached initial requests. A shared admission mutex coordinates effect entry and
shutdown; the precise ordering and watcher contract are recorded below.

Successful Resume returns its consumed key's progress exactly once: the adapter
has no post-call context-cancellation discard. Cancellation belongs to the actual
Go effect context. Cancel, close or protocol/native poison stops new dispatch and
joins issued effects, then a separate drain joins the context watcher and async
deliveries before native close. Ordinary response errors do not cancel listeners.
Async launch is only acknowledgement of launch; a blocked callback can outlive
response completion.
The test harness wraps the existing listener interface solely to count delivery;
production has no callback counter/test drain seam. Sync panic recovery uses the
existing SafelyInvokeSyncListeners; async panics retain Go's process-fatal behavior.
A protocol/native error poisons the adapter and cannot trigger fallback, replay
or a second effect invocation. Cleanup drops a prepared but unfinished span.

#### Close and effect-admission contract

The test driver uses one `admissionMu` for the close gate and `enterEffect`.
Dispatch reserves each outstanding request before launching its goroutine. The
goroutine must pass `enterEffect` under that same mutex before it calls a Go
effect. If close wins first, the reserved goroutine reports cancellation and
never calls its Go port. If effect admission wins first, the reservation remains
outstanding until the effect returns; shutdown drains it even if the goroutine
does not get scheduled to execute the Go port until after `Close` begins. No Go
port, callback, native call, or other I/O runs while `admissionMu` is held.

`Close` closes the gate under the mutex, releases the lock, then cancels the
effect context. Parent-context cancellation closes the same gate when the
watcher or dispatcher observes cancellation; this ordering is not atomic with
an arbitrary caller invoking its `CancelFunc`. The watcher exits on parent
cancellation or driver completion. `drain` joins the run (including all
reserved effects), then the watcher and separately tracked async callbacks,
before it closes native state. The callback tracking and drain are test-owned;
production still has no callback wait API.

The close/dispatch regression pauses at a test-only barrier **before** effect
reservation, then observes the real StepError activation span for explicit
close and parent cancellation. It proves shutdown that wins before reservation
does not invoke the Go port. The later reservation-to-`enterEffect` scheduler
interleaving is supported by synchronization inspection, not a forced test; the
test does not claim to force that schedule.

### Distinct witnesses

- The same 40 independent literals and exact original assertions run through
  native-driven real Go preparation/effects; fixture answers were not relaxed.
- Rust/ABI/Cgo tests cover stale generation/session/sequence, duplicate consumed
  keys, invalid result kind, copied inputs/output lifetime, independent session
  and output bounds, valid oversized input, post-consumption publish poisoning,
  close, and repeated allocation/release beyond registry capacity.
- Integration witnesses cover mixed rejection before metadata/effects, blocked
  sibling progress and join, cancelling/closing an outstanding actual-context
  save, and separately draining a blocked async listener with an actual carrier.
- Recovered Go branch panic joins its sibling with nil; native/protocol poison
  after a committed save cancels and drains a still-issued blocked sibling and
  emits no lifecycle/queue continuation. Async process panic is observed in an
  isolated test subprocess; sync panic recovers and later listeners still run.
- Retained descriptors preserve actual custom metadata delta (literal 8 bytes),
  Timing/Userland tracing, epoch StartedAt and original bound Go times. Real
  activation metrics count None/StepError exactly once, and the incoming span
  remains Error even when preparation locally converts to permanent failure.
- Native release ordering reuses the existing RunContext observation wrapper:
  accepted save -> lifecycle -> release -> enqueue; rejected saves do not release.
  It does not claim a real remote Constraint API lease-release witness.
- Test-private storage constructors use the production Redis state adapter and
  actual queue producer/shard against Valkey in both key-queue modes. Independent
  connections/adapters read persisted wrapped output, pending steps, metadata and
  raw queued item storage, including a nonempty actual prepared tracing carrier.
- Real SaveStep and Enqueue wrappers commit then return the original Go error.
  The SAME session observes that handle and invokes each affected port once;
  restarting the driver or resuming closed native state cannot replay it. This
  is not distributed exactly-once behavior or lost-ack recovery.

### Validation and environment

#### Accepted Go P2: scheduled callback argument capture

Sol identified a production regression in the extracted `launchScheduled`:
its goroutine dereferenced `*step.runCtx.Metadata()` after launch, whereas the
base Go `go l.OnStepScheduled(...)` evaluated the receiver and arguments before
launch. Checkpoint reuses one RunContext across sequential HandleGenerator
calls. A later retryable StepError's actual metadata emission could therefore
change the first callback's snapshot and race its struct copy.

The repair is confined to `response_queue_effects.go`: bind the listener method
value, context, metadata value and queue.Item value before launch; the helper's
name pointer is already a per-call parameter. This audits every original
callback input, including the function/receiver, rather than only metadata.
Copies retain the original shallow depth: maps, slices, pointers and the name
pointee are not cloned. A deferred `runtime.KeepAlive(b)` retains the binding
through callback return without rereading its fields. Launch still acknowledges
only launch; there is no callback wait, sibling barrier, changed sync-listener
ordering or change to the test-owned separate callback drain. No Rust model,
ABI, session or other lifetime code was edited for this repair.

The retained primary regression is
`TestHandleGeneratorSnapshotsScheduledArguments` in
`pkg/execution/executor/response_queue_effects_test.go`. Its authoring gate:

1. Protect each scheduled callback's launch-time metadata, queue-item attempt,
   name and context group across sequential checkpoint HandleGenerator calls.
2. The accepted delayed-copy regression changes the first metadata size from
   literal 7 to 11 and races the second handler's real metadata update.
3. Existing 40-case callbacks use independent response branches, and existing
   metadata tests do not keep an earlier callback outstanding while reusing a
   checkpoint RunContext. This is one regression at the real Go owner, not a
   duplicate helper-call or native-transport test.
4. No production seam: reuse the existing RunContext fixture/recording tracer,
   invoke real HandleGenerator twice with retryable StepErrors, and put a real
   scoped metadata update only on the second opcode. A test listener observes
   the callback values without supplying their contents or metadata mutation.

A channel holds deliveries until both handlers have returned; `synctest.Wait`
joins delivery before assertions, and results are keyed by name rather than
arrival order. Deferred idempotent release also handles an early assertion
failure. There are no sleeps, scheduler yields or production barriers. The
gate does not control when the old goroutine evaluates arguments: the normal
negative control observed the stale snapshot; the race negative control also
detects the unordered copy when that copy happens to run before the mutation.
Correct-code assertions and completion do not depend on callback scheduling.

Local final-source checks use the cached Go 1.26.4 and bounded environment below.
Artifacts are under `/home/ashutosh/.cache/response-effects/snapshot-fix/`:

- `final-prefix-normal.log`: exit 1, expected 7 / actual 11 at the first
  callback snapshot assertion. `prefix-overlay.json` restores only the saved
  pre-fix helper, leaving the retained regression unchanged.
- `final-prefix-race.log`: exit 1, delayed copy at old queue-helper line 43
  races `RunMetrics.TryAddMetadataSize`, both the metadata counter and lazy
  mutex-pointer initialization. These are reads from the same delayed struct
  copy; no independent additional race was observed or masked.
- `final-baseline-{normal,race}.log`: exit 0 using the supplied
  `sol-review-scfkgfvc/baseline-overlay.json`. Its executor/effects source hashes
  exactly match base `6a8cd4acac68ad4a3f9d5be3d9ee3ed352b51db9`; it removes
  the extracted queue helper. Both the retained owner test and the original
  cache-only Sol repro pass, normal and race.
- `final-fixed-{normal,race}.log`: exit 0 for both regressions using the supplied
  original `sol-review-scfkgfvc/overlay.json` (it adds only the cached repro).
  The retained test also passed directly without an overlay in
  `fixed-{normal,race}.log`.
- `matrix-{normal,race}.json` records exact commands/statuses for the three
  final-source variants. `prefix-response_queue_effects.go` retains the exact
  failing helper; `matrix.py` reproduces these bounded checks.

The focused regression commands use `-p=2 [-race] ./pkg/execution/executor
-run '^TestHandleGeneratorSnapshotsScheduledArguments$' -count=1 -timeout=90s`,
with the overlays above; baseline/fixed overlay runs additionally select
`^TestReviewScheduledMetadataSnapshot$` for the supplied original repro.

Sol's fresh Go review independently reran the fixed regression and exact-base
overlay 20 times each in normal and race modes, and confirmed that restoring
only the pre-fix helper reproduces both the `11` versus `7` failure and the
actual metadata-copy race. The normal negative-control assertion is
scheduling-sensitive; the race control detects the unordered copy even when it
reads before the mutation. Fixed-code assertions and completion do not depend
on callback scheduling. The review found no other Go-helper blocker.

The affected-owner/native checks both exit 0, with no skips or additional races:

```text
go test -p=2 [-race] -tags=response_continuation_spike \
  ./pkg/execution/executor ./pkg/execution/checkpoint \
  -run '^TestResponseContinuationIntegrated(Contract|SiblingProgressAndJoin|CloseDrainsCallbacksSeparately|AsyncPanicSemantics)$|^TestResponseContinuationContract$|^TestStagedBatch|^TestHandleGenerator|^TestCheckpointSyncSteps|^TestSyncStepMetadata$' \
  -count=1 -timeout=180s -v
```

`owners-native-normal.log`: executor 0.501s, checkpoint 0.110s.
`owners-native-race.log`: executor 2.288s, checkpoint 1.668s. Each run passes all
40 native literals and all 40 Go literals, plus the selected owner, sibling,
blocked-callback separate-drain and async-panic witnesses. The existing amd64
archive above is reused unchanged; this is not a fresh Rust/ABI review. Storage
semantics are unchanged, so the full Valkey/recovery campaign was not repeated.
Formatting and `git diff --check` pass. These owner checks predate the final
close/dispatch driver repair below; acceptance includes the later fresh checks.

#### Accepted close/dispatch admission repair

Luna's initial native review found that a ready activation could pass the
runner's context check, pause before dispatch, then call its Go port after
shutdown had begun. The repaired driver shares `admissionMu` between
`enterEffect` and gate closure. Sol's round-two Go review accepted the repair
with no remaining blockers. A request reserved by dispatch remains counted
until its goroutine reports completion. Shutdown winning before `enterEffect`
prevents the Go port; admission winning first means shutdown drains that work,
including if goroutine scheduling delays the Go call until after `Close` starts.
No I/O occurs under the mutex.

The watcher closes admission when parent cancellation is observed by the watcher
or dispatcher; this is not atomic with an arbitrary external `CancelFunc`.
`drain` waits for run completion and all reserved effects, then joins the
watcher and separately tracked callbacks before native close. The regression's
barrier precedes reservation, so it directly proves the shutdown-wins path; the
post-reservation scheduling interleaving is supported by synchronization
inspection, not a forced test.

Sol's fresh review ran the selected contract, lifetime, and FFI tests 20 times
each in normal and race modes with cached Go 1.26.4, `GOMAXPROCS=2`, and `-p=2`:

```text
go test -p=2 [-race] -tags=response_continuation_spike \
  ./pkg/execution/executor ./internal/responsecontinuationspike \
  -run '^TestResponseContinuationIntegrated(Contract|RejectsWholeBatch|SiblingProgressAndJoin|CloseOutstandingSave|CloseDrainsCallbacksSeparately|RecoveredPanicJoinsSibling|PoisonAfterCommittedSave|RetainsFullDescriptor|Activation|AsyncPanicSemantics|ReleasePort|CloseDispatchAdmission|CloseBeforeDispatch)$|^TestResponseContinuationContract$|^TestFFISessionCopiesOwnsReleasesAndPoisons$' \
  -count=20 -timeout=180s
```

Both normal and race runs passed: executor 9.860s / 52.331s, FFI adapter
0.044s / 1.207s. The close-admission regression is included. The broad test
pattern attempted during this review stopped on Docker permission before it
reached Valkey; it is a failed command and is not reported as a pass. The
explicit non-Valkey selection above passed. Sol's earlier independent review
had passed all six real Valkey storage/commit-error cases in both key-queue
modes and the persisted-response replay in both modes. Those storage and replay
checks were not rerun for the final driver-only admission fix.

#### Prior bounded integration validation

The following validation predates the final close/dispatch driver repair; it is
not a claim that the later admission-fix run exercised Valkey or persisted
replay. Local checks listed here exited 0, with no skips in the selected checks.
Go 1.26.4 `/home/ashutosh/.cache/go1.26.4/go/bin/go`, Linux/amd64,
GCC 11.4.0, Rust 1.94.0 (4a4ef493e), x86_64-unknown-linux-gnu.
Every Go command below uses GOTOOLCHAIN=local, GOMAXPROCS=2, -p=2, and
GOTMPDIR/TMPDIR=/home/ashutosh/.cache/response-effects/tmp. Docker checks use
the already authorized `sudo -n -u ashutosh -g docker env ...` command.
Sol's earlier independent review also passed the untagged executor path and
`CGO_ENABLED=0` with the spike tag, confirming the Cgo adapter remains excluded
without Cgo. These initial checks predate the final Go-only admission repair;
they were not repeated as part of the fresh selected review.

```text
# Functional native library, built before integrated effects ran:
CARGO_TARGET_DIR=$PWD/rust/target TMPDIR=/home/ashutosh/.cache/response-effects/tmp \
cargo +1.94.0 build --manifest-path rust/Cargo.toml -p response-continuation-bridge --release --target x86_64-unknown-linux-gnu --offline

# Normal production Go executor package, no bridge tag (5.278s):
go test -p=2 ./pkg/execution/executor -count=1 -timeout=180s

# Focused native/Go owners + actual Valkey; normal and -race both pass:
go test -p=2 [-race] -tags=response_continuation_spike ./pkg/execution/executor ./internal/responsecontinuationspike \
  -run '^TestResponseContinuation|^TestStagedBatch|^TestHandleGeneratorStep|^TestFFISession' -count=1 -timeout=180s
# Final normal: executor 2.831s, internal 0.004s.
# Final race: executor 5.757s, internal 1.021s.

# Existing surviving-store replay: both separate commands pass (2.315s/2.272s).
EXPERIMENTAL_KEY_QUEUES_ENABLE=false go test -p=2 ./tests/execution/executor -run '^TestRecoveryReplaysPersistedSDKResponse$' -count=1 -timeout=180s
EXPERIMENTAL_KEY_QUEUES_ENABLE=true go test -p=2 ./tests/execution/executor -run '^TestRecoveryReplaysPersistedSDKResponse$' -count=1 -timeout=180s

# CARGO_BUILD_JOBS=2; TMPDIR as above; target outside repo:
# CARGO_TARGET_DIR=/home/ashutosh/.cache/response-effects/target
cargo +1.94.0 test --manifest-path rust/Cargo.toml --workspace --locked --offline
# 27 Rust tests pass; model contract retains 40 literals in both ready orders.
cargo +1.94.0 clippy --manifest-path rust/Cargo.toml --workspace --all-targets --locked --offline -- -D warnings
cargo +1.94.0 fmt --manifest-path rust/Cargo.toml --all -- --check
```

The helper ran `valkey/valkey:8.0.3` with linux/amd64 image selection. Local image
ID: `sha256:88251207ac13df6d4bd4d3a77e6953ca3b90e0e5cc6acb6b3ece0fc2f9091289`;
repository digest: `valkey/valkey@sha256:4e78d8ccab4d8978afca140a3dd75b4e45d683d1e51602ee71e976bcc2bd939e`.
Only helper-owned test containers were created/stopped; unrelated services were
not touched. Tests never claim this surviving-store witness is a Rust host crash.

Logs remain local artifacts under /home/ashutosh/.cache/response-effects:
ffi-build.log, go-normal-final.log, go-native-normal-final.log,
go-race-final.log, recovery-false-final.log, recovery-true-final.log,
rust-workspace-tests.log and clippy-workspace.log. These are not CI evidence.
Initial compile/adapter attempts failed on enum/type imports and then exposed
error-identity loss from joining a nil close error and UTC reconstruction; fixed.
The callback ownership refactor's first race run caught misplaced callback drain
accounting, and the poisoned two-sibling test needed its legitimate initial
metadata event in its new expectation. Those repairs pass in the final suites;
no existing literal expectation was changed and no mutation-proof claim is made.

The repository plan-status check was also run. It exited 1 solely on the
pre-existing invalid status `Implemented — pending staging review` in
`docs/plans/run-ai-debugging.md`; that file is unchanged. Its log is plan-status.log.
Formatting and `git diff --check` pass. Passed focused checks are distinguished
from the broad Docker-permission failure and from Valkey/replay results not
rerun after the last driver repair.

### Review handoff and limits

The parent accepted this bounded integration after Luna's native review and
Sol's Go review, including fresh review of the callback snapshot and
close/dispatch repairs. Only the test-gated driver exercises Rust decisions;
production remains on the existing Go path. The FFI is a functional experiment,
not a selected bridge. Process comparison and matched full-boundary performance
remain open. Native arm64 execution/final Cgo linking are not validated.
Service-wide checkpoint admission, late CritT draining, listener-before-queue
Stop ordering, ownership fencing and uncertain/lost-ack recovery remain open.
The surviving-store witness is not host crash recovery. Broad Phase 1 stays
partial, Phase 2 open, and bridge choice unset.

Delivered in [PR #12](https://github.com/ashutoshpw/inngest/pull/12), stacked on
[PR #11](https://github.com/ashutoshpw/inngest/pull/11). PR #12 targets
`t3/response-continuation-model` from `t3/response-continuation-effects`; the
opening head was `808fbb415e179cd79dc3f3dc305b1278117bec4a`, open, non-draft,
merge state `CLEAN`, with no reviews or checks reported at that snapshot. The
task plan/status records both thread links. This documentation follow-up
advances the PR head; final HEAD is reported in the delivery handoff.
