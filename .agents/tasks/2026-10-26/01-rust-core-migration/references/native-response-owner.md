# Constructed native response owner (2026-10-27)

**Current scoped review state:** the [readiness handoff](response-owner-oneshot-readiness.md#current-scoped-review-handoff)
records accepted bounded clock semantics, standalone precedence, Go
artifact/comparator, controls, and resource-contract reviews. The later
[pilot05 audit](response-owner-capability-pilot-05-audit.md) accepted retained
evidence only; capability and publisher transition remain incomplete, memory
INCONCLUSIVE, and performance/production verdicts absent. Prior unchanged-owner
claims apply only to their original source prefix.


**Acceptance qualification (2026-10-27):** the original acceptance below covered
an explicit single-clock reduced domain. It did not establish parity with the
separate Go discovery, producer, Redis enqueue and partition-pointer clocks.
The separately authorized [bounded correction](response-owner-clock-correction.md)
amends those native binding sites while retaining one owner invocation and the
standalone explicit-clock API/fixtures. Later review acceptance is limited to
the corrected declared cases; it does not establish full capability or
production/capture readiness. The historical evidence below remains unchanged.


State: parent accepted after independent Sol interoperability review 01 and Luna
semantics review 01 found no blockers; implementation and validation are delivered
in [PR #21](https://github.com/ashutoshpw/inngest/pull/21), a test-only stacked PR.
Internal experiment only. FFI_NO_GO and SCREEN_NO_GO remain; Phase 1 partial,
Phase 2 open, bridge unset.

## Amendment, binding and scope

The parent authorizes **one native invocation** to own an ordinary single
StepRun response in a constructed domain without observer delivery, active trace
or carrier generation, live capacity lease, concurrent metadata writer,
SDK/experiment/AI metadata generation, or another response owner. This is reduced
domain ownership and fresh-Go interoperability, not production eligibility,
observer equivalence or full-handler parity. Go metrics, noop-span operations and
Go clock operations are explicitly excluded. Existing Go operations at those
sites remain unchanged; the experiment does not suppress them on a live runtime.

Assigned worktree `/home/ashutosh/.t3/worktrees/inngest/t3-1c26d6dc`, branch
`t3/native-response-owner`. Initial HEAD
`a4592cde49cab4c4a1afc5488cf39345514d5ed7` was clean with no unique commits
relative to the requested base. `git merge --ff-only
70e6536f206b2dc9253a09e6d1ffb37bd8d035eb` advanced that checkout to PR20 /
`t3/test-step-run-input-binding`. PR20 is the base, not a PR edited by this work.
The live T3 thread binding was left unchanged; no handoff or branch switch was
performed. All 11 pre-existing changed files in the assigned checkout were
preserved and delivered from that path. No delegation, benchmark, traffic,
production selector, transport selection, SDK change, new queue consumer, fencing
or activation.

Read root AGENTS, agent-tasks and audit-test-cases, migration plan/status,
[native SaveStep](native-save.md), [native enqueue](native-enqueue.md),
[bounded preparation memory](bounded-preparation-memory.md),
[StepRun input binding](step-run-input-binding.md),
[continuation](response-continuation.md) and retained replay evidence before
editing. Owners inspected include `response_runner.go`, `response_effects.go`,
`response_queue_effects.go`, `run_context.go`, opcode/metadata/extractor owners,
Redis SaveStep and Lua, native preparation/runtime/enqueue/helper and existing
store readers/socket relays/recovery setup. Queue constructor inspection confirms
that `queue.New` constructs ports; its scanner/worker lifecycle starts with Run.

## Changed files

- `rust/crates/native-step-state/src/response.rs` — gated one-invocation owner.
- `rust/crates/native-step-state/src/lib.rs` and `src/helper.rs` — feature-gated module and one helper operation.
- `pkg/execution/executor/response_native_owner_test.go` — actual-source constructed-domain admission and consumption witness.
- `pkg/execution/state/redis_state/native_response_integration_test.go` — combined real-store/fault/policy/admission proof.
- `pkg/execution/state/redis_state/native_step_socket_test.go` — reuse relay with queue-only fault targeting.
- `tests/execution/executor/recovery_native_response_test.go` — native producer to fresh ordinary Go replay.
- `tests/execution/executor/recovery_native_enqueue_test.go` — nested helper outcome decoding only.
- This reference, migration `plan.md` and `status.md` — amendment, accepted evidence, limits and delivery record.

## Actual constructed-domain proof

`response_native_owner_test.go` constructs a new private, unstarted source owner;
it never removes listeners or reconfigures tracing on an existing runtime.
Before projection it checks actual executor listener collections, the concrete
noop provider type, trace reader/context/spans/run trace fields/item carrier,
capacity manager and item lease, loaded version/force-plan, function/run/item
constraints and original generator. Arbitrary state-limit resolvers reject
without invocation. The source owner never publishes its RunContext or starts a
worker; metadata and response locks remain held throughout the one helper call.
Contended locks reject before projection. The native input takes ownership of its
counter snapshot; no shared Go pointer or subsequent Go metric swap participates.
This proves the constructed harness, not arbitrary production ownership.

The successful source witness uses an explicit response version 3/NoRetry false
against RunContext version 99/NoRetry true, changed incoming edge, priority 19,
max attempts 5 and owned metadata counters. Original raw generator bytes go to
Rust. Fresh Go readers assert literal output, accounting and actual queue source
bindings. Negative source controls include registered async/sync observers,
active tracing configuration, carrier, lease, competing owners, loaded -1,
force-plan, arbitrary resolver and metadata/experiment/AI-bearing source input.

The storage integration constructs only initial store ports and synchronous
readers; it has no executor, observer registration, tracer, worker, capacity lease
or metadata generator. Pre-existing accounting facts are 17 current/6 loaded;
the durable precreated counter is explicitly 6, and the native save must consume
11, not a caller delta. Zero/negative differences still advance the consumed
counter while persisting zero delta. Facts and fault setup end before invocation.

Replay A reuses persistent DB/Valkey setup without constructing its executor or
starting its processor. Its executor/cancel/done handles are nil. Its source is
loaded v2 metadata plus original generator bytes from real HTTP, with response
version read from the actual header. No Go response handler or per-effect
substitution runs in A. Only the native helper writes response state/discovery.
After A closes, B opens fresh ordinary Go owners and loads the existing SQLite
registration; its normal scanner finds the durable discovery. B receives no
captured/manual job injection. B's ordinary tracing/lifecycle behavior is outside
the reduced native invocation.

## Native input and one-call ownership

`response.rs` is compiled only under the existing `test-helper` feature. The
unpublished crate's default library has no response owner. Helper operation
`response` consumes a strict descriptor; old operations and production FFI are
unchanged. No continuation Session/resume or Go per-effect bridge is driven.

Initial facts are original raw generator, explicit response version/NoRetry/
RetryAt separately from loaded version/force-plan/state bytes/both counters,
run/function/tenant/event/idempotency identity, incoming edge, effective item and
identifier priority, max attempts, static state-limit option, fixed store and
queue config, SaveStep retry mode and call budget. Absent static limit disables
state validation; configured zero uses 32 MiB. Response version does not refresh
the loaded snapshot or create a proposal in this loaded-v2 domain.

There is no initial prepared output, completed save, pending answer, enqueue
permission, fresh group, generated carrier, clock or completed recipe. Native
admission independently requires zero observers, absent tracing/capacity/metadata
generation and one metadata writer/response owner. Unknown fields fail decoding.
Single event, ordinary StepRun, loaded version 2, ForceStepPlan false, ordinary
parallel mode and existing endpoint/layout/prefix/routing constraints remain.
Batch/replay/concurrency/semaphore/singleton/throttle/overrides/normalization/
promotion/recipes and all other opcodes remain excluded. Loaded -1 and force-plan
reject before effects. The one-op/no-AI/v2 domain needs no metadata proposal,
SavePending or force-plan reset; none is implemented.

Within one invocation Rust; Go does not sequence effects between the save and
enqueue writes:

1. Admits facts and reuses bounded preparation. Failed preparation has no socket,
   consumed counter, group or generated time. Existing per-buffer limits and
   AI/size/state ordering remain; no input/parser/process-memory cap is claimed.
2. At the save boundary, computes positive `current - loaded` itself and advances
   the owned loaded counter even for a nonpositive difference or save failure.
3. Calls existing native SaveStep with its unchanged retry heuristic. None,
   idempotent and duplicate errors are accepted; acceptance does not require
   newly written/Confirmed. Returned pending is authoritative. Failed save has no
   discovery generation or enqueue; its uncertainty is retained.
4. Generates UUID v4 and wall time **after accepted save**, including when pending
   suppresses enqueue. Inspected locked dependencies contain no UUID/random crate;
   Rust std File/Read plus Linux `/dev/urandom` suffices without a dependency or new
   stack. A failed OS generation/clock operation returns an explicit failure and
   retains the accepted save; there is no fallback or rollback. The old enqueue
   constructor still validates generated clocks/scores after save; an invalid
   generated clock/construction fails with the save retained, never fabricated
   success. Static unsupported capabilities are rejected before sockets/bindings.
5. If pending is false, constructs discovery and invokes the existing enqueue
   adapter once (only acknowledged NOSCRIPT allows exact EVAL fallback). Pending
   is success with enqueue absent, not unsupported post-write rejection.
6. Finalizes disposition after these results: state overflow bypasses response
   preferences; duplicate enqueue is success; every other enqueue result is
   AlwaysRetry despite NoRetry/RetryAt; other errors use NoRetry before RetryAt.
   NoRetry does not disable the separately configured existing SaveStep retries.

Uncertainty is never promoted into certainty. Lost ACKs expose no invented result;
relay observations stay independent. No atomic save/enqueue or reconciliation
protocol is claimed. UUID/time are post-effect observations only, never fed back
into the initial native invocation.

## Independent test contracts and retained owners

Authoring gate: protect the missing single-native-invocation ordering/accounting/
disposition contract, actual source admission, and native-producer-to-fresh-Go
replay. Credible regressions are early generated bindings, accepting an observer
setup, caller-supplied delta/pending/recipe, rejecting duplicate saves, ignoring
pending, adding a hidden queue retry or honoring NoRetry after a queue failure.
Existing per-adapter tests cannot establish this composite ownership boundary;
existing Go-only/two-adapter replay still does not prove it. No new production Go
seam, copied script, generated fixture answer or redundant decision oracle.

Reuse the seven preparation literals, SaveStep/store/queue corpus, actual Go
store/queue owners, independent cache-disabled readers and existing RESP relays.
The relay's only extension targets the queue script by its real 22-key command
for post-save faults; it never synthesizes success or invokes a Go effect.
The existing replay helper only gains nested outcome decoding. Twelve prior
baseline files, every fixture and production Go/Lua/FFI stay byte-identical.

`TestNativeRustResponseOwner` covers fresh/idempotent/duplicate/pending,
idempotent-pending, failed save, real save ACK loss (with and without native
SaveStep retry), nonzero/zero/negative accounting, static absent/zero limits,
preparation failure, default/NoRetry/RetryAt dispositions and post-save queue
precommit/ACK/server faults in both queue modes. Fresh full-store equality uses
the real Go store and queue owners plus the existing literal item; only observed
post-save UUID/time bind their generated fields. Genuine commits are independently
read while the native caller and context are live before ACK loss. Separate
explicit native invocations load a fresh snapshot, then prove save/enqueue dedupe.
These explicit repeated native test invocations are not worker retries,
autonomous worker recovery, crash/lease recovery or fencing proof.

`TestNativeRustResponseAdmission` has an artifact success preflight and exercises
strict descriptor/capability rejections with zero actual sockets including AUTH,
no group/time and no save. `TestRecoveryReplaysNativeResponseOwner` covers normal
and committed-ACK-loss production by A, literal saved checkpoint/HTTP/identity/
stack, ordinary fresh B scanning, cleanup/dedupe/indexes and deliberately
non-deduplicated external effect count 1. Existing two-adapter, Go-only native-step
and Sleep replay tests remain independently selected in both queue modes.

## Validation and failures

Local raw evidence: `/home/ashutosh/.cache/native-response-owner/`. Commands use
cached Go 1.26.4, `GOTOOLCHAIN=local`, cache TMPDIR/GOTMPDIR, GOMAXPROCS=2 and -p=2.
Rust is explicitly 1.94.0 with two build jobs. Go race instruments Go, not the
separate Rust helper. Authenticated AOF Valkey 8.0.3 is started only through the
existing authorized helper; unrelated containers are preserved. No benchmark.

Retained setup failures: `initial-owner.jsonl` failed to compile due to importing
queue under a name colliding with the Redis-state package's private queue type;
fixed with its conventional osqueue alias. `owner-02.jsonl` failed because Go
Create resets metadata to zero; setup now explicitly seeds the pre-existing
counter before ownership. `owner-03.jsonl` replay failed at expected state_size
139 versus 149 because an extra raw `user:{}` added ten event bytes; corrected
the precreated source event, not the fixture. `focused-04.log` replay failed at
persisted die `0` versus `false`; precreation now establishes the accepted v2
representation before native ownership. No native metadata proposal was added.
`focused-05.log` passes 83 test/subtest records; `focused-06.log` passes 91 after
additional accounting/retry cases. Those development passes are separate from
final validation below, and are not extra unique cases.

Five controls are retained in `controls.json`, `controls-run.log`, per-control
JSONL/build logs and immutable mutant helpers. All exit 1 for intended assertions:
early bindings expose a UUID where preparation failure requires null; zero delta
sends 0 instead of 11; ignored pending sends one queue request instead of zero;
queue preferences return NeverRetry instead of AlwaysRetry; an overlay removing
the source observer guard projects a registered observer and returns nil error.
All four temporary Rust mutations restore original source before Go invocation;
the source-guard control uses Go overlay only. No production owner is mutated.
Exact argv and binary hashes are retained. These are controls, not product bugs.

## Completed author validation

All eight final Go invocations exit 0: the broad owner selection records 472
Redis-state + 201 executor passes in normal mode (82.002 / 5.208s package times)
and the same counts under race (106.462 / 7.829s). Four replay commands each
record 12 passes: false normal/race 16.175 / 19.525s, true normal/race
16.409 / 19.844s. The final source/control refinement uses genuine validated SDK
metadata, actual flat experiment metadata, Go's `step.ai.wrap` type and a real
AI-output extraction candidate; it rejects raw AI markers before projection.
Its normal/race reruns pass 74 Redis-state + 16 executor records each (4.377 /
2.173s normal, 5.870 / 3.391s race). This also validates the final explicit-retry
input loaded from actual fresh Go metadata, including state size and both counters.
The source-refinement commands use key queues=true; primary owner tables and
retained primitive owners independently exercise both modes.

Combined final selections: **1,574 test/subtest pass records, zero failures,
zero skips, no race reports**. Repeated selections are not unique-case counts.
Broad tests precede the final test-only negative-source refinement; the focused
normal/race reruns validate all refined tests. No Rust implementation or replay
source changed after their final checks. Six Rust tests pass; fmt, strict Clippy,
helper build and all four target checks pass. Only Linux amd64 GNU executed.
The original helper and final immutable helper hashes are identical.

`*.command.json` records exact argv/cwd/exit/wall duration/log hash for each
final command; `validation-summary.json` adds package times and counts.
`early-commands.json` reconstructs the three initial shell commands and records
failure counts/hashes; their early helper binaries were not archived. Initial
build failure has no test records; owner-02 has 24 pass/23 fail records;
owner-03 has 47 pass/3 fail. focused-04 has the two failing replay cases plus
parent; its exact argv/hash/counts are in the command/summary records. These
failures are setup history, not final passing evidence.

Exact final Go commands (cwd is the assigned worktree):

```sh
sudo -n -u ashutosh -g docker env GOTOOLCHAIN=local GOMAXPROCS=2 GOTMPDIR=/home/ashutosh/.cache/native-response-owner/tmp TMPDIR=/home/ashutosh/.cache/native-response-owner/tmp NATIVE_STEP_TEST_BINARY=/home/ashutosh/.cache/native-response-owner/final-native-step-helper EXPERIMENTAL_KEY_QUEUES_ENABLE=false /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -p=2 -tags=native_step_integration ./pkg/execution/state/redis_state ./pkg/execution/executor -run '^(TestNative(Rust|Step|Response)|TestResponseContinuationContract$|TestStepRunInputBinding|TestStagedBatch|TestQueueItemPersistenceCompatibility$|TestRunStatePersistenceCompatibility$)' -count=1 -timeout=180s
sudo -n -u ashutosh -g docker env GOTOOLCHAIN=local GOMAXPROCS=2 GOTMPDIR=/home/ashutosh/.cache/native-response-owner/tmp TMPDIR=/home/ashutosh/.cache/native-response-owner/tmp NATIVE_STEP_TEST_BINARY=/home/ashutosh/.cache/native-response-owner/final-native-step-helper EXPERIMENTAL_KEY_QUEUES_ENABLE=false /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -race -p=2 -tags=native_step_integration ./pkg/execution/state/redis_state ./pkg/execution/executor -run '^(TestNative(Rust|Step|Response)|TestResponseContinuationContract$|TestStepRunInputBinding|TestStagedBatch|TestQueueItemPersistenceCompatibility$|TestRunStatePersistenceCompatibility$)' -count=1 -timeout=180s
sudo -n -u ashutosh -g docker env GOTOOLCHAIN=local GOMAXPROCS=2 GOTMPDIR=/home/ashutosh/.cache/native-response-owner/tmp TMPDIR=/home/ashutosh/.cache/native-response-owner/tmp NATIVE_STEP_TEST_BINARY=/home/ashutosh/.cache/native-response-owner/final-native-step-helper EXPERIMENTAL_KEY_QUEUES_ENABLE=false /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -p=2 -tags=native_step_integration ./tests/execution/executor -run '^TestRecoveryReplays(NativeResponseOwner|NativeEnqueue|NativeStepBaseline|PersistedSDKResponse)$' -count=1 -timeout=180s
sudo -n -u ashutosh -g docker env GOTOOLCHAIN=local GOMAXPROCS=2 GOTMPDIR=/home/ashutosh/.cache/native-response-owner/tmp TMPDIR=/home/ashutosh/.cache/native-response-owner/tmp NATIVE_STEP_TEST_BINARY=/home/ashutosh/.cache/native-response-owner/final-native-step-helper EXPERIMENTAL_KEY_QUEUES_ENABLE=false /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -race -p=2 -tags=native_step_integration ./tests/execution/executor -run '^TestRecoveryReplays(NativeResponseOwner|NativeEnqueue|NativeStepBaseline|PersistedSDKResponse)$' -count=1 -timeout=180s
sudo -n -u ashutosh -g docker env GOTOOLCHAIN=local GOMAXPROCS=2 GOTMPDIR=/home/ashutosh/.cache/native-response-owner/tmp TMPDIR=/home/ashutosh/.cache/native-response-owner/tmp NATIVE_STEP_TEST_BINARY=/home/ashutosh/.cache/native-response-owner/final-native-step-helper EXPERIMENTAL_KEY_QUEUES_ENABLE=true /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -p=2 -tags=native_step_integration ./tests/execution/executor -run '^TestRecoveryReplays(NativeResponseOwner|NativeEnqueue|NativeStepBaseline|PersistedSDKResponse)$' -count=1 -timeout=180s
sudo -n -u ashutosh -g docker env GOTOOLCHAIN=local GOMAXPROCS=2 GOTMPDIR=/home/ashutosh/.cache/native-response-owner/tmp TMPDIR=/home/ashutosh/.cache/native-response-owner/tmp NATIVE_STEP_TEST_BINARY=/home/ashutosh/.cache/native-response-owner/final-native-step-helper EXPERIMENTAL_KEY_QUEUES_ENABLE=true /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -race -p=2 -tags=native_step_integration ./tests/execution/executor -run '^TestRecoveryReplays(NativeResponseOwner|NativeEnqueue|NativeStepBaseline|PersistedSDKResponse)$' -count=1 -timeout=180s
sudo -n -u ashutosh -g docker env GOTOOLCHAIN=local GOMAXPROCS=2 GOTMPDIR=/home/ashutosh/.cache/native-response-owner/tmp TMPDIR=/home/ashutosh/.cache/native-response-owner/tmp EXPERIMENTAL_KEY_QUEUES_ENABLE=true NATIVE_STEP_TEST_BINARY=/home/ashutosh/.cache/native-response-owner/final-native-step-helper /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -p=2 -tags=native_step_integration ./pkg/execution/state/redis_state ./pkg/execution/executor -run '^TestNativeRustResponse|^TestNativeResponseConstructedSources$' -count=1 -timeout=120s
sudo -n -u ashutosh -g docker env GOTOOLCHAIN=local GOMAXPROCS=2 GOTMPDIR=/home/ashutosh/.cache/native-response-owner/tmp TMPDIR=/home/ashutosh/.cache/native-response-owner/tmp EXPERIMENTAL_KEY_QUEUES_ENABLE=true NATIVE_STEP_TEST_BINARY=/home/ashutosh/.cache/native-response-owner/final-native-step-helper /home/ashutosh/.cache/go1.26.4/go/bin/go test -json -race -p=2 -tags=native_step_integration ./pkg/execution/state/redis_state ./pkg/execution/executor -run '^TestNativeRustResponse|^TestNativeResponseConstructedSources$' -count=1 -timeout=120s
```

Rust commands use cwd `rust/` and prefix
`env TMPDIR=/home/ashutosh/.cache/native-response-owner/tmp CARGO_BUILD_JOBS=2`:

```sh
cargo +1.94.0 fmt --all --check
cargo +1.94.0 clippy --locked -p native-step-state --all-targets --features test-helper -- -D warnings
cargo +1.94.0 test --locked -p native-step-state --all-targets --features test-helper
cargo +1.94.0 build --locked -p native-step-state --features test-helper --bin native-step-test-helper
cargo +1.94.0 check --locked -p native-step-state --all-targets --features test-helper --target x86_64-unknown-linux-gnu
cargo +1.94.0 check --locked -p native-step-state --all-targets --features test-helper --target x86_64-unknown-linux-musl
cargo +1.94.0 check --locked -p native-step-state --all-targets --features test-helper --target aarch64-unknown-linux-gnu
cargo +1.94.0 check --locked -p native-step-state --all-targets --features test-helper --target aarch64-unknown-linux-musl
```

Five control commands use the same Go environment as owner validation, their
immutable helper paths in `controls.json`, package Redis-state, timeout 60s and
these exact selections: `^TestNativeRustResponseOwner$/^keys=false$/^overflow$`,
`^TestNativeRustResponseOwner$/^keys=false$/^fresh$`,
`^TestNativeRustResponseOwner$/^keys=false$/^pending$`, and
`^TestNativeRustResponseOwner$/^keys=false$/^queue-before$`. The fifth uses the
executor package and `^TestNativeResponseConstructedSources$/^observer$` with
`-overlay=/home/ashutosh/.cache/native-response-owner/source-observers-overlay.json`.
The source observer overlay was regenerated from final source and rerun as
`final-source-observers` with `final-source-observers-overlay.json`; it fails
exactly the intended missing-error assertion (two failure records including
parent), exit 1. Every control helper was built under Rust 1.94 with the same
locked build command above, using temporary response.rs mutations restored before
Go invocation. Mutant helper hashes and full argv remain in `controls.json`.

Valkey image ID is
`sha256:88251207ac13df6d4bd4d3a77e6953ca3b90e0e5cc6acb6b3ece0fc2f9091289`,
digest `valkey/valkey@sha256:4e78d8ccab4d8978afca140a3dd75b4e45d683d1e51602ee71e976bcc2bd939e`.
All fourteen originally present unrelated container IDs remain running; helper
containers use existing testcontainers cleanup. No global cleanup was invoked.

`scripts/check-plan-status.sh` exits 1 only for unchanged
`docs/plans/run-ai-debugging.md` status `Implemented — pending staging review`.
Formatting and `git diff --check` pass. Twelve baseline files are SHA256-checked
both against their preserved prior manifest and exact PR20 base. Production Go,
Lua, all tracked fixtures, dependencies, FFI and continuation crates are unchanged:

```sh
sha256sum -c /home/ashutosh/.cache/native-response-owner/preserved-baseline-sha256.txt
git diff --check
git diff --exit-code 70e6536f206b2dc9253a09e6d1ffb37bd8d035eb -- '*.go' ':!**/*_test.go' '*.lua' go.mod go.sum rust/Cargo.lock rust/Cargo.toml ':(glob)**/testdata/**' internal/responsecontinuationspike rust/crates/response-continuation rust/crates/response-continuation-bridge
```

Local SHA256 evidence (logs are local artifacts, not portable oracle fixtures):

```text
31c5cdbec5c152a77261e3c3d67a2d6646f009b69d7c5ef65376df5592685aa1  final-owners-normal.log
a44b5e911cc328e81ba93e671355620f09c80163185792fc6db1e1ed416197e0  final-owners-race.log
e027781e062cc83cfa879ae4b177ba5187538ca8904caa06ebbf115770a3940f  final-replay-false-normal.log
cd9d1eac1fdda01f46522fd0c47669f29c0825ae16970a31b920fa0503dd6c92  final-replay-false-race.log
cc9f17f889bdc4ae0a919060baa7951766a61613c8807ebac8def0aabe2d6809  final-replay-true-normal.log
90d09105c85b111459c6aead71d6809d0595a8715491dbef499a2ef910e067fc  final-replay-true-race.log
f804d4db8d169ec4719fd20803847b5901f2e15717b83291703ea5957ece980a  final-source-refinement-normal.log
271ad4b5f69ba70a335456d6ceaab62d9363961d0320b56809b0a35b8fcb33a0  final-source-refinement-race.log
e48b66424fa7d591e910148873f6525d58e5965a01bf22bcba0544c80ceb7df4  final-source-observers.log
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  final-rust-fmt.log
ed53431c59e4f8181f409f5a9f5b5e3bda35a590357d633abc9ec88e34f65ec9  final-rust-clippy.log
3939acc87fac0f4cd88691892e99adf2cea671a883068401bc3346cb8c076b04  final-rust-tests.log
a13905646e2aed93ec6ea9ed6ac91490a4e01f91af0215c3965279a910612149  final-rust-helper.log
bd0b221fb4469d8015d6f441e7104df387dfffc94c15dc0d4c8a64fdc118ec0e  final-target-x86_64-unknown-linux-gnu.log
824e338b4fc3271d9391d01b28859b6b52983dd8704552119048cfdb341adc79  final-target-x86_64-unknown-linux-musl.log
4e62bf3a1b2b26e715a4af7acfb8cbb08e49b23810c73886e065d0754a474647  final-target-aarch64-unknown-linux-gnu.log
808f5f9fd9c1525b542c4d9a108740789c93efc680c83df365c4aa4502cffd98  final-target-aarch64-unknown-linux-musl.log
1a1df73e3eeb8ec631c1e30965185c50f87565f15a3e9c3eb19e17f5dc2231c5  final-plan-status.log
33377d624ae77e0350f21006f0d2e3f2ff4827ffd231992fafd9a0b8c0847446  validation-summary.json
b4073cbfc78891839de59c5d4e6556d1d037816c0203130190a0376edc1ab90c  source-sha256.json
eec4be926ab98b8681f8ca34d634ba5bdfeb53ee163f200ce86d62a2309a4c7c  early-commands.json
73cd4cad788b35dcb3023c6e3ecb7085df4113330ee59942b0a9842a09a2b4bb  controls.json
f1bb74855723a911ceb5424e92503281c30ad1428cfb48e75e07ff597baf4192  preserved-baseline-sha256.txt
2a12f5d4723b4cbd09cc5f60f255a244acc34b86cdf953d1af7007d7b9c613ba  evidence-sha256.txt
18376b9f40071311e7bc6c37338862640e074f569533e9083fd33ab7a56fa39f  final-native-step-helper
```

Changed code/helper hashes from `source-sha256.json`:

```text
0f0882ccbe66b461f9e6607cad3129ccb23b9b9c6bcbbb58fb2093f871b0aa47  pkg/execution/executor/response_native_owner_test.go
37cad66105243e3e303c427a4e4c91def496112a68e235697358514db93b7a74  pkg/execution/state/redis_state/native_response_integration_test.go
bbadc43343125a2f3ed3f4e267f471fddecb30be417b34c889ca32d3ad58a727  pkg/execution/state/redis_state/native_step_socket_test.go
0d458483dfb955d9806cfa2de69957d2fb2478699254eeefa7e34697410b2526  rust/crates/native-step-state/src/helper.rs
f97a47e44a18550fba63490c97393574b5fc2fd6f8b0ba263d97f3374e24f7a8  rust/crates/native-step-state/src/lib.rs
44053f6a164fd44117878d16f121222d4decb7883bbb7b196184e0422becc36f  rust/crates/native-step-state/src/response.rs
a97bf402a73332736bbb9929ac572a860dd4211aff368f2c64bdd3fda2998306  tests/execution/executor/recovery_native_enqueue_test.go
aeae19f941344719a3c5dbe9288250444e0490feadaf89f6bf955dc258584332  tests/execution/executor/recovery_native_response_test.go
18376b9f40071311e7bc6c37338862640e074f569533e9083fd33ab7a56fa39f  final-native-step-helper
```


## Independent review and parent acceptance (2026-10-27)

The parent accepted the bounded constructed-domain owner after independent Sol
interoperability review 01 and Luna semantics review 01 found no blockers.
Acceptance applies to the test-only constructed domain described above.

- Sol's independent Go lane recorded 254 passing test/subtest records with no
  failures or skips: 103 constructed-owner/source records in normal mode with
  key queues disabled, 103 in race mode with key queues enabled, and 12 replay
  records for each of the four normal/race × queue-mode combinations. Five
  negative controls failed on their intended assertions. The missing-helper
  preflight rejects before entering test subtests. Evidence and its manifest are
  `/home/ashutosh/.cache/sol-native-response-review-x2441y7l/summary.json`,
  `controls-summary.json`, and `evidence-sha256.txt`; all manifest entries were
  verified. The helper was rebuilt and its recorded hash checked.
- Luna independently passed six Rust tests, formatting, Clippy, and the helper
  build, then ran targeted constructed-source, native-owner and admission Go
  selections in normal and race modes with both queue modes. Luna did not run
  replay, cross-target runtime, or the full Sol race/replay matrix; Go race does
  not instrument the Rust helper process.
- Author validation remains separate: 1,574 Go pass records across the focused
  normal/race owner and replay selections, six Rust tests, formatting, strict
  Clippy, helper build and four target compile checks. The helper rebuild and
  source/evidence hashes were inspected. These are prior validation records, not
  tests rerun for this delivery.

The accepted sequence is one invocation from original raw response source through
bounded preparation, native counter-delta calculation/consumption, SaveStep
acceptance (including idempotent and duplicate results) and authoritative pending
handling, then post-acceptance UUID/time generation, conditional enqueue, and
final disposition. The source projection is admitted from a private unstarted
owner with no registered observers, tracing/carrier generation, live capacity
lease, competing metadata writer, or other response owner. It does not imply
production admission or equivalence for observers or other Go runtime effects.

There is no production eligibility, observer equivalence, full-handler parity,
atomic save/enqueue, crash or worker recovery, fencing, or performance claim.
FFI_NO_GO and SCREEN_NO_GO and all prior negative evidence remain; Phase 1 partial,
Phase 2 open, bridge unset. Runtime is Linux amd64 GNU only; cross-target
compile is not linking or runtime evidence. Native arm64/musl, macOS/Windows,
cloud parity, broader SDK/operator/lifecycle/observer/trace ownership, concurrent
metadata ownership, production input memory limits, active leases/fencing/crash
recovery, durability across store restart/disk loss, performance calibration,
transport choice, production selector/activation, rollout and rollback remain open.
Save and enqueue are distinct commits. Socket timeouts remain inactivity bounds,
not dribble-proof total deadlines; cancellation remains cooperative. Root SSPL
and workspace Apache licensing are unresolved; no distribution rights are claimed.

## Delivery record (2026-10-27)

[PR #21](https://github.com/ashutoshpw/inngest/pull/21) is open from
`t3/native-response-owner` into PR20's `t3/test-step-run-input-binding`, based on
`70e6536f206b2dc9253a09e6d1ffb37bd8d035eb`. It opened at
`8571eebcfb31b2fa072344689b39b30fd785e54d`. The opening snapshot was open,
non-draft and mergeable, with no reviews or check runs reported. Its required PR
sections are Description, Release note (`None`) and Migration note (`None`). T3
links the PR to the delivery thread and parent thread
`c37dccc0-74a6-4b56-bc19-0ffcf321cc19`; both links were verified. No merge, watch,
polling or force push was performed.

The two implementation/evidence commits are `0bc1d0572` and `8571eebcf`. A
documentation follow-up records the PR opening snapshot and advances the branch
head; its final SHA is reported in the delivery handoff. For this delivery, Go
formatting, Rust formatting, JSON parsing, the Sol evidence hash manifest, local
reference links and Git whitespace were checked. No broad test suite or benchmark
was rerun.
