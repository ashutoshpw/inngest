# Isolated response-continuation model — accepted bounded model and contract

**State:** the parent accepted the isolated Rust model and independent 40-literal
shared Go/Rust contract after combined Luna-max round two found no blockers,
following Sol owner/oracle and Luna Rust-state reviews. All three P2 coverage
gaps are repaired below. Delivery is [PR #11](https://github.com/ashutoshpw/inngest/pull/11),
stacked on accepted Go extraction [PR #10](https://github.com/ashutoshpw/inngest/pull/10).
Acceptance covers only this library model and contract, not host integration,
bridge choice, activation, performance, or completion of the broader migration
phases. Broad Phase 1 remains partial, Phase 2 open, and bridge choice unset.

## Workspace and authorized boundary

T3 reports this thread attached to
`/home/ashutosh/.t3/worktrees/inngest/t3-d38721c8`, branch `t3/response-continuation-model`.
Before edits, `git status --short` was empty, `git log 327acb5f..HEAD` was empty,
and HEAD `a4592cde49cab4c4a1afc5488cf39345514d5ed7` was an ancestor of the requested
base. Fast-forwarded this same bound worktree to
`327acb5f22e4499cdfc4363b4465ce5ebb58d88d`, matching local
`t3/extract-go-response-owner`. No other checkout was edited. No delegation,
commit, push, PR, production Go edit, routing, native bridge, process host,
transport choice, activation, benchmark, or performance claim belongs to this
package. The parent performed the combined independent review after the three
test-coverage repairs below and accepted this isolated model/contract. Host
integration is a separate future gate; no future integration behavior is
established here.

Read root AGENTS.md, agent-tasks and audit-test-cases skills, existing plan/status,
[Go extraction](response-owner-extraction.md), [staged reference](staged-batch-reference.md),
framed/direct screening failures, and the real owners/recorders before edits.

## Test authoring gate

1. Protect an independently authored cross-language decision/effect contract,
   plus suspended-session correlation, branch progress and complete joins.
2. Credible regressions include accepting permanent duplicate saves, dropping
   pending after idempotent saves, refreshing loaded metadata, sharing discovery
   mode predicates, mutating the original attempt, suppressing planned callbacks
   on ordinary enqueue failure, sampling clocks early, imposing an all-saves
   barrier, ranking racing errors, or consuming mismatched/stale results.
3. Keep the accepted Go staged corpus as primary preparation/save/metadata owner
   coverage, and opcode-plan-v1 as primary grouping/hash oracle. The new shared
   contract guards a distinct language boundary, not a second Go algorithm.
   New real-Go gap witnesses reuse the existing recorders: two retry siblings,
   reset failure after accepted save/lifecycle/release, both competing-error
   completion orders, and success/failure/planned clock and callback differences.
4. No production seam, export, interface, setter or test-only hook is added.
   HandleGeneratorResponse is the real reference entry. Only the existing
   RunContext wrapper can observe capacity release directly; that separate
   HandleGenerator witness explains its narrower boundary in source.

The user's explicit implementation request authorizes edits after this gate;
no additional skill approval was inferred. No bug-fix claim or baseline mutation
claim is made. Expected fixture answers are never recorded during test runs.

## Ownership and API

The workspace registers `rust/crates/response-continuation`, an isolated library
using pinned xxhash-rust 0.8.15 and the existing opcode tag/bounds definitions.
There is no transport/serialization API or dependency from production Go.

`Session::start(Input)` owns exactly one response and returns Session plus
Progress. `resume(Key, Observation)` consumes a single outstanding request and
returns newly ready requests plus an optional completion. The key is session
**generation + original index (or Response owner) + effect sequence**, never a
step ID. The future host must assign generations uniquely, serialize Resume
calls in observed completion order, and retain its error/context tables for the
whole session. Duplicate step IDs remain legal and distinguishable.

There is at most one outstanding request per branch, with a separate staging
slot that does not overlap branch execution. Counts are bounded by the admitted
response; the reused descriptor bound is 65,536 and the Go step limit is 1,000.
Unsupported/nil descriptors, unknown tags, missing StepError user error, missing
edge, invalid generation/error bindings, and unrepresentable attempt/count bounds
reject before any request. Preparation/output/size errors are later Go effect
observations, not permission for fallback after effects. This model has no wire
parser; opaque output/options bytes do not become a second Rust SDK validator.

All request/result data is owned. Rust holds no Go pointer, borrowed Go lifetime,
I/O facility or callback. Raw data/options and user-error decision observations
are owned input. Preparation supplies owned output bytes or an original error
handle. Outstanding requests own their payloads across Resume. Go must retain
full original opcode/prepared context, tracing carriers, shared metadata-delta
accounting, and callback bindings in branch-indexed storage; those are not Rust
preparation work. Original queue group strings (including empty/non-UUID values)
remain distinct from externally bound fresh history UUIDs.

Effects are concrete metadata/pending writes, preparation, SaveStep, accepted
existing-output logging, lifecycle notification, release, reset, queue/span
preparation, enqueue, span send/drop, asynchronous listener launch and synchronous
listener notification. There is no opaque whole-handler or conditional recipe
callback. The model is not a generic VM or priority scheduler.

## Decisions implemented in Rust

- All six eligible tags (None, Step, StepRun, StepError, StepFailed, StepPlanned)
  are non-lazy and OtherGroup. None still counts toward parallel history and
  pending/hash input. Every excluded tag, including Sleep in a mixed batch,
  rejects the entire model input before staging. The existing seven-tag Go
  mixed witness, including Sleep, is retained unchanged.
- Metadata proposals preserve loaded StartedAt, use response request version,
  enable force-plan for multiple non-lazy ops and apply observed HasAI exactly
  as the Go proposal does. Accept metadata before checking the step limit.
  Persisting a proposal never refreshes this response's loaded snapshot.
- Coalescing needs a planned opcode, multiple non-lazy IDs, and loaded version
  >=2 (response version is used only when loaded version is -1). IDs retain
  original order for SavePending; sorted UTF-8 IDs retain duplicates in xxHash64
  input. Pending acceptance is a barrier before any sibling launch/effect.
- Successful Step/StepRun accepts duplicate or idempotent save errors; permanent
  failure accepts only idempotent. Authoritative SaveStep pending is retained
  alongside classifications/original handle. Rejected saves stop that branch
  before lifecycle/release/discovery. Accepted saves notify lifecycle, release
  capacity, then bind discovery UUID and clock, even when enqueue is suppressed.
- Retryable StepError uses opcode/user/response policies and attempt budget.
  It prepares tracing, reports Errored at the old attempt, schedules the branch
  copy at attempt+1 and completes with the bound ErrHandledStepError handle.
  Exhaustion changes only the local preparation opcode to StepFailed; the
  original response/attempt is unchanged. The model also preserves None/Wait/Race
  in recipes rather than reducing every non-race mode to one enum value.
- Successful discovery's suppression/coalesced-job predicate uses opcode mode;
  failed discovery suppression uses RunContext mode, while its recipe still
  carries opcode mode and its `-failure` job ID. Reset eligibility consults the
  loaded version/force flag and only applies when discovery will enqueue. A
  reset failure after lifecycle/release blocks queue preparation and callbacks.
- Planned recipes preserve original outgoing/incoming edges, use display name
  for the edge and SDK name for callbacks, use `-plan` jobs, carry the batch
  coalesce key, omit step semaphores, and schedule at the newly bound clock
  minus 1ms. Ordinary discovery/failure recipes copy semaphore observations.
  Go still materializes Identifier, concurrency values, bytes/options/size
  observations, AI/tracing/shared metadata, storage and lifecycle implementations.
- Enqueue duplicate drops the droppable span and suppresses callbacks. Ordinary
  discovery enqueue error drops it and returns the always-retry discovery
  disposition before callbacks. Planned ordinary failure still sends its span
  and launches/notifies callbacks before the original error completes.
- Every sibling joins. The first observed branch error is retained without
  ranking or cancellation; response preferences are applied only after the join.
  Visible state overflow and existing always-retry errors return unchanged;
  then response NoRetry wins over RetryAt. Go's generated discovery alwaysRetry
  wrapper embeds error without Unwrap, so an underlying overflow becomes hidden
  from errors.Is and still gets the discovery always-retry disposition. Original
  error handles/classifications themselves are never rewritten to simulate that
  wrapper. ErrorContext records metadata/pending/reset/discovery wrapping, and
  RetryPolicy records any additional policy, for the adapter to apply to the
  retained original Go error object/value rather than reconstructing messages.

Clock/UUID requests occur at activation sites: handling after accepted metadata
and limit; history UUID before each branch starts; discovery UUID/recipe clock
only after accepted save/lifecycle/release, including suppression; planned UUID
then recipe clock; asynchronous listener launch before the later synchronous
callback clock. A sibling reaching save/enqueue does not wait for unrelated saves.

## Shared literals and independent Go reference

`pkg/execution/executor/testdata/compatibility/response-continuation-v1.json`
contains **40 manually authored literal cases** (the original 39 plus one
preparation-failure case) with source provenance at the
accepted Go base. No runtime capture/golden update path exists. The Go test reads
it independently, constructs real response/RunContext inputs, supplies recorded
store/queue outcomes and external clocks, and invokes actual
HandleGeneratorResponse. Rust reads the same file directly, supplies only typed
external observations, and asserts model requests/dispositions against its
literal bytes, recipes and ordering constraints. The Rust observation adapter
cannot choose retry/save/discovery/callback decisions.

Sibling events and recipes are compared as multisets. Explicit `before` pairs
assert partial orders. The fixture includes a separate six-tag eligible case,
two retrying siblings with unchanged original attempt, both save classifications,
reset failure, clock recipes/callback differences, duplicate callback suppression,
mode authority, metadata/pending barriers/version fallback, duplicate-preserving
coalescing, error identity/wrapping and retry preferences. Existing
opcode-plan-v1 grouping/hash vectors are independently consumed rather than
copied; all accepted grouping cases and hash vectors pass in Rust.

Fresh Go UUIDs are not injected. The Go adapter observes queue/context group
identity, history-group uniqueness and retry callbacks retaining their branch
history group; suppressed discovery callbacks also expose a new group binding.
Rust uses external UUID observations and original indices. Clock assertions use
explicit UTC-relative nanoseconds, including the planned queued-at attribute
after metadata and later callback clocks. Tracing export observes planned send
before callbacks. Dropped spans are filtered by Go's execution processor, so Go
asserts **export suppression**, not interception of its private Drop method;
Rust separately asserts the concrete drop request.

API-only tests reject stale generations, duplicate/completed keys, wrong
owners/sequences, wrong observation kinds and zero error handles without
consuming state; they then resume the original request successfully. Duplicate-ID
siblings reach SaveStep separately, one can finish enqueue while the other's
save remains outstanding, and both racing-error completion orders retain their
observed winner while completing the join. These are library protocol witnesses,
not real-server, crash, lease or recovery evidence.

## Validation

Rust 1.94.0 on Linux amd64, explicitly selected via `cargo +1.94.0`.
`CARGO_TARGET_DIR=/home/ashutosh/.cache/response-continuation/target`;
`TMPDIR=/home/ashutosh/.cache/response-continuation/tmp`. Offline dependency
resolution uses the cached, pinned crate; Cargo.lock is updated. Checks:

```text
cargo +1.94.0 fmt --manifest-path rust/Cargo.toml --all -- --check
cargo +1.94.0 clippy --manifest-path rust/Cargo.toml --workspace --all-targets --locked --offline -- -D warnings
cargo +1.94.0 test --manifest-path rust/Cargo.toml --workspace --locked --offline
```

All pass: 15 existing bridge tests, 4 kernel tests and 6 new model/contract tests
(the shared test runs every literal case in both forward and reverse ready-set
orders). Existing Rust bridge/kernel sources have no diff. Development failures
were limited to an initial borrow/type/serde spelling repair, Clippy suggestions,
recorder pending-event vocabulary, and adapting span-export/Go value-wrapper
identity observations; no production repair or fixture capture was performed.

Go binary `/home/ashutosh/.cache/go1.26.4/go/bin/go`, `GOTOOLCHAIN=local`,
`GOMAXPROCS=2`, both `GOTMPDIR` and `TMPDIR` under the same cache `tmp` directory;
all commands use `-p=2`. The exact selection P is:

```text
^(TestResponseContinuation|TestStagedBatch|TestOpcodePlan|TestDiscoveryEnqueueFailureAlwaysRetries|TestHandleGeneratorStep|TestMaybeEnqueueDiscoveryStep|TestHandleGeneratorResponse_CoalesceKey|TestCheckpointAsyncSteps_NotifiesRunStepRunFinishedLifecycle|TestCheckpointSyncSteps_NotifiesRunStepRunFinishedLifecycle)
```

```text
go test -p=2 ./pkg/execution/executor ./pkg/execution/checkpoint -run P -count=1 -timeout=120s
# exit 0: executor 0.047s; checkpoint 0.038s.
go test -p=2 -race ./pkg/execution/executor ./pkg/execution/checkpoint -run P -count=1 -timeout=120s
# exit 0: executor 1.256s; checkpoint 1.186s.
```

Final race argv/status and logs are cached at
`/home/ashutosh/.cache/response-continuation/validation.json` and
`go-owner-race.log`; Rust logs are beside them. These are local artifacts, not
published CI evidence. Gofmt and `git diff --check` pass. The repository's
`bash scripts/check-plan-status.sh` still exits 1 for the pre-existing invalid
`run-ai-debugging.md` status `Implemented — pending staging review`; that file is
untouched. No full Go suite, benchmark campaign, whole E2E, native arm64, host
shutdown, active-lease or process-recovery check was run or claimed.

## Before any host integration

These remain separate gates and do not activate this model:

- Extract the remaining retry/planned queue ports, preserving the current Go
  preparation/tracing behavior and all original branch descriptor data.
- Define context, prepared-save accounting, span, original-error handle and
  asynchronous callback lifetimes; preserve Go branch panic/listener behavior.
  This library does not provide a host lifetime protocol or panic transport.
- Drain direct checkpoint work and late CritT bodies, then preserve service Stop
  listener ordering. No unjoined callback may outlive a released host binding.
- Resolve uncertain-ack SaveStep/enqueue recovery and ownership fencing. Never
  use Go fallback after an external effect has run.
- Validate native arm64 execution for the eventual host/bridge path.
- Treat the accepted library model as a starting contract only. The host must
  separately prove the gates above before integration or activation. No
  transport is selected and no performance advantage follows from correctness
  of this library model.


## Accepted P2 contract-coverage repairs (2026-10-26)

Scope: test adapters, shared literals, and task documentation only. The independent
Sol owner/oracle review found the actual-Go reference and original 39 literal
cases honest and reported no model correctness defect. Its two accepted P2s were
missing async delivery/payload assertions and UUID consumption without downstream
binding proof. Luna's Rust-state review found no correctness blocker and identified
the third P2 preparation-coverage gap repaired in the next section. Combined
rereview of all three repairs subsequently found no blockers, and the parent
accepted only the isolated model plus shared contract.

At repair start, the designated `t3-d38721c8` directory was already on branch
`t3/response-continuation-model`, HEAD
`327acb5f22e4499cdfc4363b4465ce5ebb58d88d`. The checkout/branch and pre-existing
edits were preserved. No other worktree, production Go, Rust `src/lib.rs`,
workspace manifest or lockfile was changed by this repair.

The audit-test-cases authoring gate was applied before editing:

1. Protect shared async listener delivery, exact projected callback payload and
   suppression, plus UUID-site activation order and downstream group identity.
2. Credible regressions: omit async launch, change only its name/item, reuse a
   constant recipe group, substitute a sibling/history allocation, or bind the
   wrong group during preparation or callback delivery.
3. Existing sync event strings and enqueue recipes could stay green under these
   mutations; Rust discarded AsyncScheduled and did not inspect UUID sites or
   recipe/preparation/callback groups. Extend the existing shared contract owner
   rather than adding a duplicate test suite.
4. No production seam, global UUID generator, timed absence window, or extra
   production export is required. The explicit repair request authorizes edits.

The same 39 literals now supply `scheduled` callback projections, recipe
`branch`/`group`, and per-original-index `uuidSites`. Callback projections include
nil versus empty names, original retry versus recipe items, attempt, job/edge,
mode/coalescing, max attempts and priority. Enqueue time remains a separate recipe
assertion because it is not a callback argument. All original error, policy,
clock, recipe payload/time, metadata, writes, pending and partial-order literals
are preserved. Three duplicate-ID planned cases now use distinct SDK names
`duplicate-0`/`duplicate-1` and an empty DisplayName, preserving the original
recipe display names, IDs and coalescing hashes while exposing callback branch
identity. Their scheduled event names change accordingly. No expected value is
captured from either implementation or produced during a test run.

Go installs a separate async lifecycle recorder and uses `testing/synctest` to
drain the real response-owner's goroutines before checking both listener paths.
Expected absence fails by the observed callback multiset, without waiting for an
expected callback count. Fresh UUID values are validated and normalized by
relationships between lifecycle/history, queued item/context, async and sync
callbacks. Retry callbacks retain their branch history group and incoming context;
new recipe callbacks bind item/context to the same fresh allocation, distinct
across observed branches/sites. Go does not expose every allocation (for example,
unused history groups for None/planned branches or discovery before reset failure),
so allocation-site counts are asserted in Rust, not claimed as Go observations.

Rust records supplied UUIDs by original branch index and site, checks history use
in save/retry preparation, recipe group use in queue preparation/enqueue, and both
callback group/payload projections including suppressed-enqueue deliveries. Site
lists retain per-branch order; local assertions require handling before UUIDs,
history before branch preparation, discovery after save/lifecycle/release, and
recipe UUID before recipe clock. Async launch must be observed before a callback
clock request is emitted. `Observation::Done` acknowledges launch, not callback
completion. Neither adapter imposes async callback completion before that clock.
Sibling multisets and the original partial orders remain nondeterministic.

Focused repair validation (all exit 0):

```text
GOTOOLCHAIN=local GOMAXPROCS=2 \
GOTMPDIR=/home/ashutosh/.cache/response-continuation/tmp \
TMPDIR=/home/ashutosh/.cache/response-continuation/tmp \
/home/ashutosh/.cache/go1.26.4/go/bin/go test -p=2 ./pkg/execution/executor -run '^TestResponseContinuation' -count=1 -timeout=120s
# executor 0.086s; same command with -race: executor 1.217s.

# CARGO_TARGET_DIR=/home/ashutosh/.cache/response-continuation/target
# TMPDIR=/home/ashutosh/.cache/response-continuation/tmp
cargo +1.94.0 test --manifest-path rust/Cargo.toml -p response-continuation --locked --offline
# 6 tests; shared literal contract exercises every case in both ready-set orders.
cargo +1.94.0 fmt --manifest-path rust/Cargo.toml --all -- --check
cargo +1.94.0 clippy --manifest-path rust/Cargo.toml -p response-continuation --all-targets --locked --offline -- -D warnings
```

Six temporary mutations in a separate cached Rust workspace each compiled and
failed the shared contract at runtime (exit 101): omitted async launch (launch
must precede callback clock request); wrong async name; wrong async attempt/item;
constant recipe group `1` (observed history UUID instead of planned allocation);
wrong preparation history binding; and wrong async callback group. The latter
payload/group mutations fail the shared async literal assertion, not a separate
save/error guard. Each overlay mutation was exactly restored; the restored shared
contract exits 0. The owned `src/lib.rs` was never mutated, and its before/after
SHA-256 is `b8931f22283882f2c2942b461fb2ce7d9c60696a27c59ccb199feda424e15db9`.
Local evidence is under `/home/ashutosh/.cache/response-continuation/`:
`p2-go-normal.log`, `p2-go-race.log`, and `p2-mutations/results.json` plus each
mutation log and `restored.log`. These are local artifacts, not CI evidence.
Gofmt, JSON parsing, the original-literal preservation check, and whitespace pass.

No production/model correctness repair was needed. The combined review and parent
acceptance occurred after these repairs; delivery and its opening snapshot are
recorded below. No benchmark, full integration, host lifetime/recovery, activation,
native arm64, or performance proof was performed. Broad Phase 1 stays partial,
Phase 2 open, bridge choice unset, and all prior host/performance/activation gates
remain.


## Preparation descriptor and failure P2 repair (2026-10-26)

At the start of this repair, both initial reviews were complete and the fresh
combined Luna-max rereview of all three contract gaps was still pending. Luna
found no model correctness blocker; its remaining
P2 was that the Rust contract adapter ignored the `PrepareSave` opcode and always
returned successful prepared bytes by branch index. The first two Sol repairs
(async delivery/payload/suppression and UUID binding/order) remain intact.

The authoring gate protects the independent Go-to-Rust preparation contract:
wrong descriptor bytes/tag or continuing after failed preparation could previously
pass. Existing Go staged tests remain the primary output/preparation owner tests;
this shared case adds the missing language-boundary error continuation, and the
existing RunContext wrapper adds the distinct capacity-release observation.
No production seam or new handler implementation is introduced.

Each preparation-bearing fixture opcode now carries a literal `prepareOp`.
Rust compares the entire requested descriptor against fixture fields: tag, ID,
SDK/display names, data/options bytes, user-error fields, mode and AI observation.
The expected tag is supplied independently, not computed from retry policy or
copied from the effect. This applies to both save and retry-span preparation.
For example, `attempt-exhaustion` retains original input `StepError` while requiring
`StepFailed` only in local save preparation; retryable cases require `StepError`.
Nonempty opaque options are also exercised by the new case. The original 39 cases,
including every expected callback/UUID/recipe/error/clock literal, are preserved
apart from adding preparation-tag observations.

The fortieth shared literal, `preparation-state-size-failure-retains-original`,
uses a valid StepRun/null output (13 wrapped bytes) and the existing 12-byte
state-size policy. The actual Go `HandleGeneratorResponse` calls
`goResponseEffects.prepareStep`, which calls `validateStateSize`; no store failure
or fake preparation handler stands in for it. Loaded request version -1 and an
AI opcode legitimately write metadata before preparation fails. That write and
the handling-clock read remain expected. There is no step span, save, lifecycle,
queue recipe or either scheduled callback afterward. A separate direct
`HandleGenerator` call through the existing `stagedReleaseContext` proves that
capacity release also remains absent at the observable real RunContext boundary.

Go must return the actual standard overflow wrapper directly, preserving the
original `state.ErrStateOverflowed` sentinel, literal public error code/message,
and original retry disposition despite both response NoRetry and RetryAt.
Rust receives `Prepared(Err(...))` with its own retained original handle and
classifications, checks Original error context/policy and a branch history ending
at preparation, and rejects any later release/effects. This is a real size-policy
failure, not a new SDK-validation rule. Missing/invalid user-error objects remain
excluded model input; no semantics for them were added.

Focused checks all pass (same Go 1.26.4/cache/tmp/GOMAXPROCS=2 and Rust 1.94.0
configuration as above):

```text
go test -p=2 ./pkg/execution/executor -run '^TestResponseContinuation' -count=1 -timeout=120s
# executor 0.066s
go test -p=2 -race ./pkg/execution/executor -run '^TestResponseContinuation' -count=1 -timeout=120s
# executor 1.238s
cargo +1.94.0 test --manifest-path rust/Cargo.toml -p response-continuation --locked --offline
# 6 tests pass; all 40 literals run in both ready-set orders.
cargo +1.94.0 fmt --manifest-path rust/Cargo.toml --all -- --check
cargo +1.94.0 clippy --manifest-path rust/Cargo.toml -p response-continuation --all-targets --locked --offline -- -D warnings
```

Four compiled, isolated Rust mutations fail the shared contract at runtime
(exit 101): changed preparation data; omitted local StepFailed conversion;
retained the preparation error but continued into SaveStep/lifecycle/enqueue;
replaced only the original preparation error handle. The continuation mutant
fails on extra save/lifecycle/queue/callback events while the metadata write
remains present. The restored overlay passes. Each mutation restores exact bytes
in a finally block; owned `src/lib.rs` was never modified and remains SHA-256
`b8931f22283882f2c2942b461fb2ce7d9c60696a27c59ccb199feda424e15db9`.
All production executor Go hashes match the pre-repair snapshot. Local logs,
`mutation-results.json`, mutation script, and preservation/hash evidence are in
`/home/ashutosh/.cache/response-continuation/p2-preparation/`. The earlier six
mutation logs and repair assertions are retained. These are local evidence,
not published CI results.

Only the Go/Rust test adapters, shared fixture and this reference/status were
changed by this remaining repair. Existing workspace registration/lockfile and
plan edits are preserved. No production/model fix, delegation, benchmark, full
integration, host/performance or activation work was performed. The subsequent
combined review and parent acceptance are recorded in the delivery note below.

## Parent acceptance and PR delivery (2026-10-26)

The parent accepted the isolated continuation model and independent shared
40-literal Go/Rust contract after the combined Luna-max round-two rereview found
no blockers. This followed the Sol owner/oracle review and Luna Rust-state review;
all three P2 test-coverage gaps above were repaired before the combined review.
Acceptance covers only this model and contract.

The model is delivered by [PR #11](https://github.com/ashutoshpw/inngest/pull/11),
from `t3/response-continuation-model` into `t3/extract-go-response-owner`, stacked
on [PR #10](https://github.com/ashutoshpw/inngest/pull/10). Its Conventional
Commit title is `feat(execution): add isolated Rust response continuation model`.
It opened at `112e3b0e9401873cade2a948d3d5dd381258e534`; the opening snapshot was
open, non-draft, merge state `CLEAN`, with no reviews and no check runs reported.
The PR is linked to this delivery child and parent thread
`c37dccc0-74a6-4b56-bc19-0ffcf321cc19`; both thread listings confirmed the link.
The task status follow-up records this PR metadata. The final branch SHA is
reported at handoff. No merge, watch, or polling was performed.

Before PR opening, the fresh focused Go normal and race runs passed, along with
Rust package tests (all 40 literal cases in both ready-set orders), Rust
formatting, and strict package Clippy. Six async/UUID and four preparation
mutations failed their intended contract assertions; exact restoration and the
restored contract passed. Production Go hashes remained unchanged, and Rust
`src/lib.rs` was unchanged by the test repairs. These results are local evidence,
not CI results. The documentation follow-up did not rerun tests or benchmarks.

The package does not implement or prove host integration, bridge selection,
execution activation, or performance. Remaining host gates include queue ports;
context, prepared-save, checkpoint/error/span, and callback lifetimes; direct
checkpoint and late CritT draining; Stop listener ordering; uncertain-ack
recovery and ownership fencing; no fallback after an external effect; and native
arm64 validation. Phase 1 remains partial and Phase 2 open; no bridge is selected.
